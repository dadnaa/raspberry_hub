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
"""

import logging
import threading
import queue
import time
from typing import Callable, List, Optional

from config.settings import TELEMETRY_IDLE_TIMEOUT_SEC, TELEMETRY_STOP_TIMEOUT_SEC
from src.telemetry.printer_state import PrinterStatus
from src.telemetry.state_manager  import StateManager
from src.telemetry.telemetry_event import TelemetryEvent
from src.telemetry import telemetry_parser as parser

logger = logging.getLogger(__name__)

# How many seconds of silence before we consider the printer IDLE
_IDLE_TIMEOUT_SEC = TELEMETRY_IDLE_TIMEOUT_SEC


class TelemetryEngine:
    """
    Background telemetry engine.

    Usage:
        engine = TelemetryEngine(line_queue, state_manager)
        engine.start()
        # ... runs indefinitely ...
        engine.stop()

    Args:
        line_queue    – thread-safe queue.Queue fed by SerialConnection
                        with decoded string lines from the printer.
        state_manager – shared StateManager instance.
        on_event      – optional callback(TelemetryEvent) for Sprint 4 MQTT bridge.
    """

    def __init__(
        self,
        line_queue:    queue.Queue,
        state_manager: StateManager,
        on_event:      Optional[Callable[[TelemetryEvent], None]] = None,
    ) -> None:
        self._q             = line_queue
        self._state         = state_manager
        self._on_event      = on_event
        self._stop_event    = threading.Event()
        self._thread:       Optional[threading.Thread] = None
        self._last_line_ts: float = 0.0

        # Register internal event publisher with state manager
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
        logger.info("[Telemetry] Engine started.")

    def stop(self, timeout: float = TELEMETRY_STOP_TIMEOUT_SEC) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("[Telemetry] Engine stopped.")

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

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

        # --- Reboot detection ---
        if parser.is_reboot(line):
            updates["status"] = PrinterStatus.REBOOTING
            logger.info("[Telemetry] Printer reboot detected.")

        # --- Temperature ---
        temp = parser.parse_temperature(line)
        if temp:
            if temp.nozzle        is not None: updates["nozzle_temp"]   = temp.nozzle
            if temp.nozzle_target is not None: updates["nozzle_target"] = temp.nozzle_target
            if temp.bed           is not None: updates["bed_temp"]      = temp.bed
            if temp.bed_target    is not None: updates["bed_target"]    = temp.bed_target

        # --- Position ---
        pos = parser.parse_position(line)
        if pos:
            updates["position_x"] = pos.x
            updates["position_y"] = pos.y
            updates["position_z"] = pos.z

        # --- SD print progress ---
        prog = parser.parse_sd_progress(line)
        if prog:
            updates["progress_pct"] = prog.percent
            if prog.percent > 0:
                updates["status"] = PrinterStatus.PRINTING

        if parser.is_sd_done(line):
            updates["progress_pct"] = 100.0
            updates["status"] = PrinterStatus.IDLE
            logger.info("[Telemetry] Print finished.")

        # --- Pause / resume ---
        if parser.is_paused(line):
            updates["status"] = PrinterStatus.PAUSED
            logger.info("[Telemetry] Printer paused.")

        if parser.is_resumed(line):
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
            self._last_line_ts = time.monotonic()  # reset so we don't spam

    # ------------------------------------------------------------------
    # State change → TelemetryEvent (internal pub for Sprint 4 bridge)
    # ------------------------------------------------------------------

    def _on_state_change(self, snapshot, changed_fields: dict) -> None:
        if not self._on_event:
            return
        event = TelemetryEvent.build(snapshot=snapshot, changed_fields=changed_fields)
        try:
            self._on_event(event)
        except Exception:
            logger.exception("[Telemetry] on_event callback raised.")
