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

  ack_queue       → PrinterCommunicator (command responses only)
  telemetry_queue → TelemetryEngine (ALL lines for state parsing)

ACK FILTERING
─────────────
ack_queue receives only command-response lines that the command engine
cares about: ok, error, busy, and multi-line responses triggered by
commands (temperatures from M105, position from M114, firmware from M115).

Unsolicited telemetry lines (autoreport temps, SD progress, action
commands, Wi-Fi noise, echo: lines) go to telemetry_queue only.
This prevents the ack reader from blocking on lines it never asked for.

Lines routed to BOTH queues:
  - ok
  - error / !! (Marlin error prefix)
  - echo:busy / echo:busy: processing
  - T:* lines  (temp — M105 response AND autoreport; ack reader skips
                 unless it's waiting for M105, telemetry always wants it)
  - X:* Y:* Z:* (position — M114 response)
  - FIRMWARE_NAME:* / Cap:* (M115 response)

Lines routed to TELEMETRY QUEUE ONLY (ack_queue skipped):
  - SD printing byte * / Done printing file
  - // action:*
  - wifi: / echo:wifi*
  - echo:* (informational, not command responses)
  - Marlin banner lines (start, Marlin x.x.x, echo: External Reset)

No other code calls SerialConnection.read_line() after this is wired in.
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

_ACK_QUEUE_SIZE       = SERIAL_ACK_QUEUE_SIZE
_TELEMETRY_QUEUE_SIZE = SERIAL_TELEMETRY_QUEUE_SIZE


def _is_ack_line(line: str) -> bool:
    """
    Return True if this line is a command-response that the ack reader needs.

    The ack reader (PrinterCommunicator / QueueProcessor) waits for these
    to know a command completed. Letting unsolicited lines through causes
    the ack reader to block waiting for an "ok" that already passed by.

    Keep this tight — when in doubt, exclude from ack_queue.
    """
    s = line.strip()
    sl = s.lower()

    # Primary ack tokens
    if sl == "ok":
        return True
    if sl.startswith("ok "):          # ok T:200 ... (some Marlin builds)
        return True

    # Error tokens
    if sl.startswith("error"):
        return True
    if s.startswith("!!"):            # !! prefix = Marlin fatal error
        return True

    # Busy — command engine must handle these to avoid timeout
    if sl.startswith("echo:busy"):
        return True

    # Temperature lines — needed as ack for M105 / M109 / M190
    # Also produced by autoreport, but telemetry_queue handles that path.
    if s.startswith("T:") or s.startswith("T0:"):
        return True

    # Position — ack for M114
    if s.startswith("X:") and "Y:" in s and "Z:" in s:
        return True

    # Firmware info — ack for M115
    if s.startswith("FIRMWARE_NAME:"):
        return True
    if s.startswith("Cap:"):
        return True

    return False


class SerialRouter:
    """
    Single-threaded serial reader that fans each decoded line to two queues.

    ack_queue       → command-response lines only (ok, error, busy, T:, X:, Cap:)
    telemetry_queue → ALL lines (full picture for state manager)

    Usage:
        connection = SerialConnection()
        connection.connect()

        router = SerialRouter(connection)
        router.start()

        comm = PrinterCommunicator(connection, ack_queue=router.ack_queue)
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
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("[SerialRouter] Stopped.")

    def reset_queues(self) -> None:
        """Flush both queues — call after reconnect() to discard stale data."""
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
                self._stop_event.wait(timeout=0.5)
                continue

            line = self._conn.read_line()   # THE ONLY CALL TO read_line()
            if line is None:
                continue

            logger.debug(f"[SerialRouter] << {line!r}")

            # telemetry_queue gets EVERY line — full state picture
            self._put_nowait(self.telemetry_queue, line, "telemetry")

            # ack_queue gets only command-response lines
            if _is_ack_line(line):
                self._put_nowait(self.ack_queue, line, "ack")
            else:
                logger.debug(f"[SerialRouter] ack_queue skip (telemetry-only): {line!r}")

        logger.info("[SerialRouter] Reader loop exited.")

    @staticmethod
    def _put_nowait(q: queue.Queue, line: str, name: str) -> None:
        """
        Non-blocking put. Drops the OLDEST item if full.
        Preserves real-time behaviour — stale data is less valuable than fresh.
        """
        try:
            q.put_nowait(line)
        except queue.Full:
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
