"""
queue_processor.py — Layer 2: Command Queue Engine
The heart of the Command Execution Layer.

Runs a dedicated background thread that:
  1. Pulls CommandEntry objects from a thread-safe queue
  2. Sends each command to PrinterCommunicator (Layer 1)
  3. Waits for "ok" acknowledgment
  4. Updates command status and engine state
  5. Logs every transition for full traceability
  6. Handles timeouts and printer errors without halting

This is the ONLY place allowed to call PrinterCommunicator.send_command().
Nothing else in the system may write to the serial port.

Engine State transitions:
  IDLE → BUSY → WAITING_FOR_OK → IDLE   (success)
  IDLE → BUSY → WAITING_FOR_OK → ERROR → IDLE  (failure + recovery)
"""

import logging
import queue
import threading
import time
from datetime import datetime

from src.engine.command import (
    CommandEntry,
    CommandStatus,
    EngineState,
)
from src.hardware.printer_communicator import (
    PrinterCommunicator,
    PrinterUnresponsiveError,
)
from src.hardware.serial_connection import SerialConnectionError

logger = logging.getLogger(__name__)

# ── Tuning constants ──────────────────────────────────────────────────────
QUEUE_POLL_INTERVAL_SEC = 0.05    # How often idle loop checks the queue
ERROR_RECOVERY_DELAY    = 2.0     # Pause after a command failure before resuming
MAX_QUEUE_SIZE          = 256     # Hard cap; prevents unbounded memory growth


class QueueProcessor:
    """
    Background thread that owns the command execution lifecycle.

    Instantiate once. Call start() after the serial connection is ready.
    Call stop() for graceful shutdown — waits for current command to finish.
    """

    def __init__(self, communicator: PrinterCommunicator):
        self._comm         = communicator
        self._queue: queue.Queue[CommandEntry] = queue.Queue(maxsize=MAX_QUEUE_SIZE)

        self._state        = EngineState.IDLE
        self._state_lock   = threading.Lock()

        self._current_cmd: CommandEntry | None = None
        self._shutdown_event = threading.Event()
        self._thread: threading.Thread | None  = None

        # History of completed entries (bounded to last 500)
        self._history: list[CommandEntry] = []
        self._history_lock = threading.Lock()
        self._HISTORY_LIMIT = 500

    # ------------------------------------------------------------------
    # Public control API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background processor thread."""
        if self._thread and self._thread.is_alive():
            logger.warning("[Processor] Already running.")
            return

        self._shutdown_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="QueueProcessor",
            daemon=True,
        )
        self._thread.start()
        logger.info("[Processor] Started.")

    def stop(self, timeout_sec: float = 10.0) -> None:
        """
        Signal shutdown and wait for the worker thread to finish.

        - No new commands are accepted after this call.
        - The current command (if any) completes before thread exits.
        - Remaining queued commands are drained and marked REJECTED.
        """
        logger.info("[Processor] Shutdown requested …")
        self._shutdown_event.set()
        self._set_state(EngineState.SHUTDOWN)

        if self._thread:
            self._thread.join(timeout=timeout_sec)
            if self._thread.is_alive():
                logger.error("[Processor] Worker thread did not stop in time.")
            else:
                logger.info("[Processor] Worker thread stopped cleanly.")

        self._drain_queue_on_shutdown()

    def enqueue(self, entry: CommandEntry) -> bool:
        """
        Add a validated CommandEntry to the execution queue.

        Args:
            entry: Pre-built CommandEntry (from CommandEngine public API).

        Returns:
            bool: True if enqueued, False if queue full or shutting down.
        """
        if self._state == EngineState.SHUTDOWN:
            entry.mark_rejected("System is shutting down.")
            logger.warning(f"[Processor] Rejected (shutdown): {entry.gcode!r}")
            return False

        try:
            self._queue.put_nowait(entry)
            logger.info(
                f"[Processor] Enqueued [{entry.command_id}] {entry.gcode!r}  "
                f"(queue depth: {self._queue.qsize()})"
            )
            return True
        except queue.Full:
            entry.mark_rejected("Command queue is full.")
            logger.error(
                f"[Processor] Queue full — rejected [{entry.command_id}] {entry.gcode!r}"
            )
            return False

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @property
    def state(self) -> EngineState:
        with self._state_lock:
            return self._state

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def current_command(self) -> CommandEntry | None:
        return self._current_cmd

    def get_history(self, last_n: int = 50) -> list[CommandEntry]:
        """Return the most recent completed command entries."""
        with self._history_lock:
            return list(self._history[-last_n:])

    # ------------------------------------------------------------------
    # Internal: main worker loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """
        Continuous processor loop.

        Runs on the QueueProcessor background thread.
        Polls the queue and drives commands through their lifecycle.
        """
        logger.info("[Processor] Worker loop started.")

        while not self._shutdown_event.is_set():
            try:
                # Block briefly, then loop to check shutdown flag
                entry: CommandEntry = self._queue.get(
                    timeout=QUEUE_POLL_INTERVAL_SEC
                )
            except queue.Empty:
                self._set_state(EngineState.IDLE)
                continue

            self._execute_entry(entry)

        logger.info("[Processor] Worker loop exited.")

    def _execute_entry(self, entry: CommandEntry) -> None:
        """
        Drive a single CommandEntry through its full lifecycle.

        Transitions:  PENDING → BUSY → WAITING_FOR_OK → OK | FAILED
        """
        self._current_cmd = entry
        self._set_state(EngineState.BUSY)

        logger.info(
            f"[Processor] ▶ Executing [{entry.command_id}] {entry.gcode!r}"
        )

        # ── Transition: BUSY → WAITING_FOR_OK ────────────────────────
        self._set_state(EngineState.WAITING_FOR_OK)
        entry.mark_sent()

        try:
            responses = self._comm.send_command(entry.gcode)
            entry.mark_ok(responses)
            self._set_state(EngineState.IDLE)

            logger.info(
                f"[Processor] ✓ OK [{entry.command_id}] {entry.gcode!r}  "
                f"({entry.elapsed_ms:.1f} ms,  "
                f"{len(responses)} response line(s))"
            )

        except PrinterUnresponsiveError as exc:
            entry.mark_failed(str(exc))
            self._set_state(EngineState.ERROR)

            logger.error(
                f"[Processor] ✗ TIMEOUT [{entry.command_id}] {entry.gcode!r} — {exc}"
            )
            time.sleep(ERROR_RECOVERY_DELAY)
            self._set_state(EngineState.IDLE)

        except SerialConnectionError as exc:
            entry.mark_failed(f"Serial disconnected: {exc}")
            self._set_state(EngineState.ERROR)

            logger.error(
                f"[Processor] ✗ SERIAL ERROR [{entry.command_id}] — {exc}"
            )
            # Attempt reconnect; drain remaining queue if it fails
            recovered = self._comm._conn.reconnect()
            if not recovered:
                logger.critical(
                    "[Processor] Could not reconnect. Draining queue."
                )
                self._drain_queue_on_error("Serial connection lost.")
            self._set_state(EngineState.IDLE)

        except Exception as exc:
            entry.mark_failed(f"Unexpected error: {exc}")
            self._set_state(EngineState.ERROR)
            logger.exception(
                f"[Processor] ✗ UNEXPECTED [{entry.command_id}] — {exc}"
            )
            self._set_state(EngineState.IDLE)

        finally:
            self._record_history(entry)
            self._current_cmd = None
            self._queue.task_done()
            # Signal any caller blocked in send() / send_batch()
            done_event = getattr(entry, '_done_event', None)
            if done_event is not None:
                done_event.set()

    # ------------------------------------------------------------------
    # Internal: helpers
    # ------------------------------------------------------------------

    def _set_state(self, new_state: EngineState) -> None:
        with self._state_lock:
            old = self._state
            self._state = new_state
        if old != new_state:
            logger.debug(
                f"[Processor] State: {old.name} → {new_state.name}"
            )

    def _record_history(self, entry: CommandEntry) -> None:
        with self._history_lock:
            self._history.append(entry)
            if len(self._history) > self._HISTORY_LIMIT:
                self._history = self._history[-self._HISTORY_LIMIT:]

    def _drain_queue_on_shutdown(self) -> None:
        """Mark all remaining queued commands as REJECTED (shutdown path)."""
        drained = 0
        while not self._queue.empty():
            try:
                entry = self._queue.get_nowait()
                entry.mark_rejected("System shutdown.")
                self._record_history(entry)
                drained += 1
            except queue.Empty:
                break
        if drained:
            logger.info(f"[Processor] Drained {drained} command(s) on shutdown.")

    def _drain_queue_on_error(self, reason: str) -> None:
        """Mark all remaining queued commands as FAILED (error path)."""
        drained = 0
        while not self._queue.empty():
            try:
                entry = self._queue.get_nowait()
                entry.mark_failed(reason)
                self._record_history(entry)
                drained += 1
            except queue.Empty:
                break
        if drained:
            logger.warning(
                f"[Processor] Drained {drained} command(s) due to error: {reason}"
            )