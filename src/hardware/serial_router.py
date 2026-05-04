"""
serial_router.py — Fix: Single Serial Reader with Dual-Queue Fan-Out

THE PROBLEM THIS SOLVES
───────────────────────
Before this fix, both PrinterCommunicator and TelemetryEngine called
SerialConnection.read_line() directly. Because readline() consumes bytes
from the OS buffer, they raced for the same bytes:

  Thread A (QueueProcessor):  calls read_line() → gets "ok"     ✓
  Thread B (TelemetryEngine): calls read_line() → gets "T:205"  ✓
  ...but sometimes:
  Thread B (TelemetryEngine): calls read_line() → STEALS "ok"   ✗
    → CommandEngine times out, marks command FAILED
    → printer is still moving, system thinks it errored

THE FIX
───────
SerialRouter owns the ONE background thread that calls read_line().
It fans every decoded line out to two separate queues:

  ack_queue      → PrinterCommunicator reads from this (replaces read_line())
  telemetry_queue → TelemetryEngine reads from this (already queue-based)

No other code calls SerialConnection.read_line() after this is wired in.

WIRING CHANGES REQUIRED (documented in-file):
  1. serial_connection.py — call router.start() after connect()
  2. printer_communicator.py — replace _read_until_ok() with ack_queue reads
  3. main.py — pass telemetry_queue to TelemetryEngine
"""

import logging
import queue
import threading
from typing import Optional

from config.settings import (
    SERIAL_ACK_QUEUE_SIZE,
    SERIAL_ROUTER_STOP_TIMEOUT_SEC,
    SERIAL_TELEMETRY_QUEUE_SIZE,
)
from src.hardware.serial_connection import SerialConnection

logger = logging.getLogger(__name__)

# Bounded queues — prevents memory growth if a consumer falls behind
_ACK_QUEUE_SIZE = SERIAL_ACK_QUEUE_SIZE
_TELEMETRY_QUEUE_SIZE = SERIAL_TELEMETRY_QUEUE_SIZE


class SerialRouter:
    """
    Single-threaded serial reader that fans each decoded line to two queues.

    Usage (in main.py or connection setup):

        connection = SerialConnection()
        connection.connect()

        router = SerialRouter(connection)
        router.start()

        # Pass router.ack_queue to PrinterCommunicator
        comm = PrinterCommunicator(connection, ack_queue=router.ack_queue)

        # Pass router.telemetry_queue to TelemetryEngine
        telemetry = TelemetryEngine(
            line_queue=router.telemetry_queue,
            state_manager=state_manager,
        )

        # On shutdown:
        router.stop()
    """

    def __init__(self, connection: SerialConnection) -> None:
        self._conn              = connection
        self.ack_queue:       queue.Queue = queue.Queue(maxsize=_ACK_QUEUE_SIZE)
        self.telemetry_queue: queue.Queue = queue.Queue(maxsize=_TELEMETRY_QUEUE_SIZE)

        self._stop_event = threading.Event()
        self._thread:    Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background reader thread. Call after connect()."""
        if self._thread and self._thread.is_alive():
            logger.warning("[SerialRouter] Already running.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._read_loop,
            name="SerialRouter",
            daemon=True,
        )
        self._thread.start()
        logger.info("[SerialRouter] Started — single reader, dual-queue fan-out.")

    def stop(self, timeout: float = SERIAL_ROUTER_STOP_TIMEOUT_SEC) -> None:
        """Stop the reader thread gracefully."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("[SerialRouter] Stopped.")

    def reset_queues(self) -> None:
        """
        Flush both queues — call after reconnect() to discard stale data.
        """
        for q in (self.ack_queue, self.telemetry_queue):
            while not q.empty():
                try:
                    q.get_nowait()
                except queue.Empty:
                    break
        logger.debug("[SerialRouter] Queues flushed after reconnect.")

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ------------------------------------------------------------------
    # Internal — the single read loop
    # ------------------------------------------------------------------

    def _read_loop(self) -> None:
        logger.info("[SerialRouter] Reader loop active.")
        while not self._stop_event.is_set():
            if not self._conn.is_connected:
                # Wait for reconnect without spinning
                self._stop_event.wait(timeout=0.5)
                continue

            line = self._conn.read_line()   # ← THE ONLY CALL TO read_line()
            if line is None:
                # read_line() timed out (READ_TIMEOUT_SEC = 5s) — normal, loop again
                continue

            logger.debug(f"[SerialRouter] << {line!r}")

            # ── Fan out to both consumers ──────────────────────────────
            self._put_nowait(self.ack_queue,       line, "ack")
            self._put_nowait(self.telemetry_queue, line, "telemetry")

        logger.info("[SerialRouter] Reader loop exited.")

    @staticmethod
    def _put_nowait(q: queue.Queue, line: str, name: str) -> None:
        """
        Non-blocking put. Drops the OLDEST item if full (not the new one).
        This preserves real-time behaviour — stale data is less valuable
        than fresh data.
        """
        try:
            q.put_nowait(line)
        except queue.Full:
            # Drop oldest, make room for newest
            try:
                q.get_nowait()
            except queue.Empty:
                pass
            try:
                q.put_nowait(line)
            except queue.Full:
                pass
            logger.warning(
                f"[SerialRouter] {name}_queue full — oldest line dropped."
            )
