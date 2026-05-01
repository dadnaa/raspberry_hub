"""
command_engine.py — Layer 2: Command Queue Engine
The PUBLIC interface for the entire command execution system.

This is the ONLY class that external modules (Cloud Bridge, Telemetry,
future AI layer) are allowed to call. No module outside this file
knows about QueueProcessor, SerialConnection, or PrinterCommunicator.

Usage:
    engine = CommandEngine(connection)
    engine.start()

    result  = engine.send("M105")
    results = engine.send_batch(["G28", "M104 S200", "M105"])

    engine.stop()
"""

import logging
import threading
from typing import Callable

from src.engine.command import (
    CommandEntry,
    CommandResult,
    CommandStatus,
    EngineState,
)
from src.engine.queue_processor import QueueProcessor
from src.engine.validator import validate_command, validate_batch, ValidationError
from src.hardware.printer_communicator import PrinterCommunicator
from src.hardware.serial_connection import SerialConnection

logger = logging.getLogger(__name__)

# How long send() blocks waiting for a result before timing out
SEND_RESULT_TIMEOUT_SEC = 30.0


class CommandEngine:
    """
    Sprint 2 public API — deterministic command scheduler for the printer.

    Responsibilities:
    - Accept G-code from any caller
    - Validate before queuing
    - Guarantee sequential execution with ok-sync
    - Return structured CommandResult to callers
    - Expose engine status for monitoring

    Thread-safe: send() and send_batch() can be called from any thread.
    """

    def __init__(self, connection: SerialConnection):
        self._comm      = PrinterCommunicator(connection)
        self._processor = QueueProcessor(self._comm)
        self._lock      = threading.Lock()

        # Optional callback: called with CommandResult after each command
        self._on_complete: Callable[[CommandResult], None] | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the queue processor background thread."""
        self._processor.start()
        logger.info("[CommandEngine] Started.")

    def stop(self) -> None:
        """
        Gracefully shut down the engine.

        - Current command finishes executing.
        - Remaining queued commands are marked REJECTED.
        - Background thread exits cleanly.
        """
        logger.info("[CommandEngine] Stopping …")
        self._processor.stop()
        logger.info("[CommandEngine] Stopped.")

    # ------------------------------------------------------------------
    # Command API (public — callable from any module)
    # ------------------------------------------------------------------

    def send(self, gcode: str) -> CommandResult:
        """
        Validate, queue, and synchronously wait for a single G-code command.

        Blocks the calling thread until the command reaches a terminal
        state (OK, FAILED, or REJECTED).

        Args:
            gcode: G-code string (e.g. "M105", "G28", "M104 S200").

        Returns:
            CommandResult: Immutable result with status, responses, timing.
        """
        entry = self._build_entry(gcode)
        if entry is None:
            # Validation failed; build a rejected result
            return CommandResult(
                command_id    = "rejected",
                gcode         = gcode,
                status        = CommandStatus.REJECTED,
                responses     = (),
                error_message = f"Validation failed: {gcode!r}",
                elapsed_ms    = None,
            )

        return self._enqueue_and_wait(entry)

    def send_batch(self, commands: list[str]) -> list[CommandResult]:
        """
        Validate and queue multiple G-code commands.

        Commands are executed in strict order.
        Each command waits for "ok" before the next begins.
        Invalid commands in the batch are skipped (returned as REJECTED).

        Args:
            commands: Ordered list of G-code strings.

        Returns:
            list[CommandResult]: One result per input command, in order.
        """
        valid_cmds, errors = validate_batch(commands)
        results: list[CommandResult] = []

        # Pre-populate rejected results for invalid commands
        error_iter = iter(errors)
        valid_iter = iter(valid_cmds)
        valid_set  = set(valid_cmds)

        # Build an ordered entry list, preserving original positions
        entries: list[CommandEntry | None] = []
        for raw in commands:
            try:
                cleaned = validate_command(raw)
                entries.append(CommandEntry(gcode=cleaned))
            except ValidationError as exc:
                entries.append(None)
                logger.warning(f"[CommandEngine] Batch skip: {exc}")

        # Enqueue all valid entries, then collect results in order
        done_events: list[threading.Event | None] = []
        for entry in entries:
            if entry is None:
                done_events.append(None)
            else:
                ev = threading.Event()
                entry._done_event = ev          # attach event for wait-on-result
                enqueued = self._processor.enqueue(entry)
                done_events.append(ev if enqueued else None)

        # Wait for each entry to complete
        for i, (entry, ev) in enumerate(zip(entries, done_events)):
            if entry is None:
                results.append(CommandResult(
                    command_id    = "rejected",
                    gcode         = commands[i],
                    status        = CommandStatus.REJECTED,
                    responses     = (),
                    error_message = "Validation failed.",
                    elapsed_ms    = None,
                ))
            elif ev is None:
                results.append(CommandResult.from_entry(entry))
            else:
                ev.wait(timeout=SEND_RESULT_TIMEOUT_SEC)
                results.append(CommandResult.from_entry(entry))

        return results

    def send_fire_and_forget(self, gcode: str) -> bool:
        """
        Queue a command without waiting for its result.

        Use for non-critical commands where the caller doesn't need
        to block (e.g. status queries in telemetry loop).

        Returns:
            bool: True if successfully enqueued.
        """
        entry = self._build_entry(gcode)
        if entry is None:
            return False
        return self._processor.enqueue(entry)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @property
    def state(self) -> EngineState:
        """Current engine state (IDLE / BUSY / WAITING_FOR_OK / ERROR / SHUTDOWN)."""
        return self._processor.state

    @property
    def queue_depth(self) -> int:
        """Number of commands currently waiting in the queue."""
        return self._processor.queue_depth

    @property
    def current_command(self) -> CommandEntry | None:
        """The command currently being executed, or None."""
        return self._processor.current_command

    def get_history(self, last_n: int = 20) -> list[CommandResult]:
        """Return the last N completed command results."""
        return [
            CommandResult.from_entry(e)
            for e in self._processor.get_history(last_n)
        ]

    def set_on_complete_callback(
        self, callback: Callable[[CommandResult], None]
    ) -> None:
        """
        Register a callback invoked after every command completes.

        Used by Sprint 4 MQTT bridge to publish command state updates.
        """
        self._on_complete = callback

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_entry(self, gcode: str) -> CommandEntry | None:
        """Validate gcode and construct a CommandEntry. Returns None on failure."""
        try:
            cleaned = validate_command(gcode)
            entry   = CommandEntry(gcode=cleaned)
            entry._done_event = threading.Event()
            return entry
        except ValidationError as exc:
            logger.warning(f"[CommandEngine] Validation failed: {exc}")
            return None

    def _enqueue_and_wait(self, entry: CommandEntry) -> CommandResult:
        """Enqueue an entry and block until it reaches a terminal state."""
        enqueued = self._processor.enqueue(entry)

        if not enqueued:
            entry.mark_rejected("Queue full or system shutting down.")
            return CommandResult.from_entry(entry)

        # Wait for the processor to signal completion
        done_event: threading.Event = entry._done_event
        done_event.wait(timeout=SEND_RESULT_TIMEOUT_SEC)

        result = CommandResult.from_entry(entry)

        if self._on_complete:
            try:
                self._on_complete(result)
            except Exception as exc:
                logger.warning(f"[CommandEngine] on_complete callback error: {exc}")

        return result