"""
telemetry_engine.py — Sprint 3: Continuous Telemetry Engine

Runs in a dedicated daemon thread.
Reads all serial output independently of the command queue.
Parses lines and updates StateManager.
Publishes TelemetryEvents to registered subscribers.

SEPARATION OF PLANES:
  Command Queue  → controls printer   (src/engine/command_engine.py)
  TelemetryEngine → observes printer  (this file)

They share the SerialConnection object but NEVER block each other:
  - Command engine writes commands and reads ack lines.
  - Telemetry engine reads ALL lines via a shared line queue that
    the SerialConnection feeds into.

STATUS TRANSITION — UNKNOWN / REBOOTING → IDLE
───────────────────────────────────────────────
When the hub starts (or USB is hot-plugged) the printer status begins as
UNKNOWN. The hub's main.py polling loop sends periodic M105 commands.
The first temperature telemetry line received here transitions the status
from UNKNOWN or REBOOTING → IDLE, confirming the printer is alive and
responsive. Without this, the hub stays stuck in UNKNOWN indefinitely.
"""

import logging
import threading
import queue
import time
from typing import Callable, Optional

from config.settings import (
    TELEMETRY_IDLE_TIMEOUT_SEC,
    TELEMETRY_POLL_COMMANDS,
    TELEMETRY_POLL_INTERVAL_SEC,
    TELEMETRY_STOP_TIMEOUT_SEC,
)
from src.telemetry.printer_state import PrinterStatus
from src.telemetry.state_manager  import StateManager
from src.telemetry.telemetry_event import TelemetryEvent
from src.telemetry import telemetry_parser as parser

logger = logging.getLogger(__name__)

_IDLE_TIMEOUT_SEC       = TELEMETRY_IDLE_TIMEOUT_SEC
_REBOOT_PROBE_DELAY_SEC = 3.0
_PROBE_SETTLE_SEC       = 0.5
_PROBE_BOOT_SEC         = 0.3
_PROBE_CMD_GAP_SEC      = 0.1
_POLL_INTERVAL_SEC      = TELEMETRY_POLL_INTERVAL_SEC
_POLL_COMMANDS          = tuple(TELEMETRY_POLL_COMMANDS)


class TelemetryEngine:
    """
    Background telemetry engine.

    Usage:
        engine = TelemetryEngine(line_queue, state_manager, write_fn=serial.write)
        engine.start()
        engine.probe_connection()   # call once after serial port opens
        # ... runs indefinitely ...
        engine.stop()

    Args:
        line_queue    – thread-safe queue.Queue fed by SerialRouter
                        with decoded string lines from the printer.
        state_manager – shared StateManager instance.
        on_event      – optional callback(TelemetryEvent) for Sprint 4 MQTT bridge.
        write_fn      – callable that writes a raw string to the serial port.
                        Required for active probing on hot-plug; if None,
                        only passive parsing is performed.
    """

    def __init__(
        self,
        line_queue:    queue.Queue,
        state_manager: StateManager,
        on_event:      Optional[Callable[[TelemetryEvent], None]] = None,
        command_sender: Optional[Callable[[str], bool]] = None,
        write_fn:      Optional[Callable[[str], None]] = None,
    ) -> None:
        self._q             = line_queue
        self._state         = state_manager
        self._on_event      = on_event
        self._command_sender = command_sender
        self._write_fn      = write_fn
        self._stop_event    = threading.Event()
        self._thread:       Optional[threading.Thread] = None
        self._poll_thread:  Optional[threading.Thread] = None
        self._last_line_ts: float = 0.0
        self._suppress_printing_until: float = 0.0

        self._state.register_listener(self._on_state_change)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.warning("[Telemetry] Already running.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="TelemetryEngine",
            daemon=True,
        )
        self._thread.start()
        self._start_polling()
        logger.info("[Telemetry] Engine started.")

    def stop(self, timeout: float = TELEMETRY_STOP_TIMEOUT_SEC) -> None:
        self._stop_event.set()
        if self._poll_thread:
            self._poll_thread.join(timeout=timeout)
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("[Telemetry] Engine stopped.")

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def set_command_sender(self, command_sender: Optional[Callable[[str], bool]]) -> None:
        """Set or replace the queued command sender used for telemetry polling."""
        self._command_sender = command_sender

    # ------------------------------------------------------------------
    # Active telemetry polling
    # ------------------------------------------------------------------

    def _start_polling(self) -> None:
        if not self._command_sender:
            logger.info("[Telemetry] Active polling disabled (no command_sender).")
            return
        if self._poll_thread and self._poll_thread.is_alive():
            return

        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            name="TelemetryPoller",
            daemon=True,
        )
        self._poll_thread.start()
        logger.info(
            "[Telemetry] Active polling started: commands=%s interval=%.1fs",
            ",".join(_POLL_COMMANDS),
            _POLL_INTERVAL_SEC,
        )

    def _poll_loop(self) -> None:
        while not self._stop_event.wait(_POLL_INTERVAL_SEC):
            sender = self._command_sender
            if not sender:
                continue

            for command in _POLL_COMMANDS:
                if self._stop_event.is_set():
                    return
                try:
                    queued = sender(command)
                    if not queued:
                        logger.debug("[Telemetry] Poll command rejected: %s", command)
                except Exception:
                    logger.exception("[Telemetry] Poll command failed: %s", command)
                self._stop_event.wait(_PROBE_CMD_GAP_SEC)

    # ------------------------------------------------------------------
    # Connection probing (call once after serial port opens)
    # ------------------------------------------------------------------

    def probe_connection(self) -> None:
        """
        Handle both USB connection scenarios:

        Case 1 — Hot-plug (printer already running):
            The printer sends nothing. We actively send M115/M105/M114
            to pull current state. The M105 response will trigger the
            UNKNOWN → IDLE transition in _process_line.

        Case 2 — Cold boot / power-on:
            The printer sends its full boot sequence. The main _run loop
            parses those lines automatically (reboot → REBOOTING, then
            first temp line → IDLE). Probes are still sent after settle
            to ensure we have fresh position and temp data.

        Runs in a short-lived daemon thread — does not block the caller.
        """
        if not self._write_fn:
            logger.warning(
                "[Telemetry] probe_connection() called but no write_fn provided. "
                "Skipping active probe — passive parsing only."
            )
            return
        threading.Thread(
            target=self._probe_worker,
            name="TelemetryProbe",
            daemon=True,
        ).start()

    def _probe_worker(self) -> None:
        logger.info("[Telemetry] probe_connection: starting probe sequence.")
        time.sleep(_PROBE_SETTLE_SEC)   # let port settle
        time.sleep(_PROBE_BOOT_SEC)     # let cold-boot lines queue
        self._send_probe_commands()

    def _send_probe_commands(self) -> None:
        if not self._write_fn:
            return
        try:
            logger.info("[Telemetry] Probe: M115 (firmware info)")
            self._write_fn("M115\n")
            time.sleep(_PROBE_CMD_GAP_SEC)

            logger.info("[Telemetry] Probe: M105 (temperatures)")
            self._write_fn("M105\n")
            time.sleep(_PROBE_CMD_GAP_SEC)

            logger.info("[Telemetry] Probe: M114 (position)")
            self._write_fn("M114\n")
        except Exception:
            logger.exception("[Telemetry] Error during probe_connection.")

    def _do_reboot_probe(self) -> None:
        """Re-probe ~3 s after reboot detected — Marlin is ready by then."""
        logger.info("[Telemetry] Re-probing after reboot (boot settle complete).")
        self._send_probe_commands()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run(self) -> None:
        logger.info("[Telemetry] Reader loop active.")
        while not self._stop_event.is_set():
            try:
                line: str = self._q.get(timeout=0.2)
            except queue.Empty:
                self._check_idle_timeout()
                continue

            self._last_line_ts = time.monotonic()
            line = line.strip()
            if not line:
                continue

            logger.debug(f"[Telemetry] RAW: {line!r}")
            self._process_line(line)

        logger.info("[Telemetry] Reader loop exiting.")

    # ------------------------------------------------------------------
    # Line processing
    # ------------------------------------------------------------------

    def _process_line(self, line: str) -> None:
        updates = {}

        # ----------------------------------------------------------------
        # Reboot detection
        # Covers cold-boot and M112 reset. Schedules a re-probe after
        # _REBOOT_PROBE_DELAY_SEC so Marlin is fully initialised first.
        # ----------------------------------------------------------------
        if parser.is_reboot(line):
            updates["status"] = PrinterStatus.REBOOTING
            logger.info("[Telemetry] Printer reboot detected.")
            if self._write_fn:
                threading.Timer(
                    _REBOOT_PROBE_DELAY_SEC,
                    self._do_reboot_probe,
                ).start()

        # ----------------------------------------------------------------
        # Temperature
        # KEY FIX: receiving any temperature line means the printer is
        # alive and communicating. If we were UNKNOWN (hub just started /
        # hot-plug, no boot sequence seen) or REBOOTING (boot finished),
        # transition to IDLE so the system knows the printer is online.
        # This is the primary signal from the main.py M105 polling loop.
        # ----------------------------------------------------------------
        temp = parser.parse_temperature(line)
        if temp:
            if temp.nozzle        is not None: updates["nozzle_temp"]   = temp.nozzle
            if temp.nozzle_target is not None: updates["nozzle_target"] = temp.nozzle_target
            if temp.bed           is not None: updates["bed_temp"]      = temp.bed
            if temp.bed_target    is not None: updates["bed_target"]    = temp.bed_target

            current_status = self._state.get_snapshot().status
            if current_status in (PrinterStatus.UNKNOWN, PrinterStatus.REBOOTING):
                updates["status"] = PrinterStatus.IDLE
                logger.info(
                    f"[Telemetry] Temperature received while status={current_status.name} "
                    f"→ transitioning to IDLE (printer is online)."
                )

        # ----------------------------------------------------------------
        # Position (M114 response)
        # ----------------------------------------------------------------
        pos = parser.parse_position(line)
        if pos:
            updates["position_x"] = pos.x
            updates["position_y"] = pos.y
            updates["position_z"] = pos.z

        # ----------------------------------------------------------------
        # SD print progress
        # ----------------------------------------------------------------
        prog = parser.parse_sd_progress(line)
        if prog:
            updates["progress_pct"] = prog.percent
            if prog.percent > 0:
                import time as _time
                if _time.time() >= self._suppress_printing_until:
                    updates["status"] = PrinterStatus.PRINTING

        if parser.is_sd_done(line):
            updates["progress_pct"] = 100.0
            updates["status"] = PrinterStatus.IDLE
            logger.info("[Telemetry] Print finished.")

        # ----------------------------------------------------------------
        # Pause / resume
        # ----------------------------------------------------------------
        if parser.is_paused(line):
            import time as _time
            if _time.time() >= self._suppress_printing_until:
                updates["status"] = PrinterStatus.PAUSED
                logger.info("[Telemetry] Printer paused.")

        if parser.is_resumed(line):
            import time as _time
            if _time.time() >= self._suppress_printing_until:
                updates["status"] = PrinterStatus.PRINTING
                logger.info("[Telemetry] Printer resumed.")

        if updates:
            self._state.update(**updates)

    # ------------------------------------------------------------------
    # Idle detection
    # ------------------------------------------------------------------

    def _check_idle_timeout(self) -> None:
        if self._last_line_ts == 0:
            return
        elapsed = time.monotonic() - self._last_line_ts
        if elapsed >= _IDLE_TIMEOUT_SEC:
            snap = self._state.get_snapshot()
            if snap.status not in (
                PrinterStatus.IDLE,
                PrinterStatus.ERROR,
                PrinterStatus.UNKNOWN,
            ):
                self._state.update(status=PrinterStatus.IDLE)
                logger.debug("[Telemetry] Idle timeout — status set to IDLE.")
            self._last_line_ts = time.monotonic()

    # ------------------------------------------------------------------
    # State change → TelemetryEvent (Sprint 4 MQTT bridge)
    # ------------------------------------------------------------------

    def _on_state_change(self, snapshot, changed_fields: dict) -> None:
        if not self._on_event:
            return
        event = TelemetryEvent.build(snapshot=snapshot, changed_fields=changed_fields)
        try:
            self._on_event(event)
        except Exception:
            logger.exception("[Telemetry] on_event callback raised.")

    def suppress_printing(self, duration_sec: float) -> None:
        """Temporarily ignore telemetry-driven transitions to PRINTING.

        Used after a cancel to prevent telemetry from immediately marking
        the printer as PRINTING while the safe-stop sequence completes.
        """
        import time as _time
        self._suppress_printing_until = _time.time() + float(duration_sec)
