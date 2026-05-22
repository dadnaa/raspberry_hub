"""Printer gateway abstractions and OctoPrint implementation."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import Optional, Protocol

from config.settings import (
    OCTOPRINT_WEBSOCKET_ENABLED_ENV,
    OCTOPRINT_POLL_INTERVAL_SEC,
    OCTOPRINT_STATUS_CONNECTING,
    OCTOPRINT_STATUS_ERROR,
    OCTOPRINT_STATUS_OPERATIONAL,
    OCTOPRINT_STATUS_PAUSED,
    OCTOPRINT_STATUS_PAUSING,
    OCTOPRINT_STATUS_PRINTING,
)
import os
from src.core.printer_state import PrinterStatus
from src.core.state_manager import StateManager
from src.infrastructure.octoprint.client import OctoPrintClient
from src.infrastructure.octoprint.event_stream import OctoPrintEventStream

logger = logging.getLogger(__name__)


class GatewayCommandStatus(Enum):
    PENDING = auto()
    SENT = auto()
    OK = auto()
    FAILED = auto()
    REJECTED = auto()


@dataclass(frozen=True)
class GatewayCommandResult:
    command_id: str
    gcode: str
    status: GatewayCommandStatus
    responses: tuple[str, ...] = ()
    error_message: Optional[str] = None
    elapsed_ms: Optional[float] = None

    @property
    def succeeded(self) -> bool:
        return self.status == GatewayCommandStatus.OK

    @property
    def failed(self) -> bool:
        return self.status in (
            GatewayCommandStatus.FAILED,
            GatewayCommandStatus.REJECTED,
        )


class IPrinterGateway(Protocol):
    """Single printer-facing API used by jobs, MQTT, vision, and runtime wiring."""

    def start(self) -> None: ...
    def stop(self) -> None: ...
    def send(self, gcode: str) -> GatewayCommandResult: ...
    def send_gcode(self, gcode: str) -> GatewayCommandResult: ...
    def pause(self) -> bool: ...
    def resume(self) -> bool: ...
    def cancel(self) -> bool: ...


class OctoPrintGateway:
    """OctoPrint-backed gateway plus telemetry-to-StateManager adapter."""

    def __init__(
        self,
        client: OctoPrintClient,
        state_manager: StateManager,
        poll_interval_sec: float = OCTOPRINT_POLL_INTERVAL_SEC,
    ) -> None:
        self._client = client
        self._state = state_manager
        self._poll_interval_sec = poll_interval_sec
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._event_stream: Optional[OctoPrintEventStream] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="OctoPrintTelemetry",
            daemon=True,
        )
        self._thread.start()
        if _websocket_enabled():
            self._event_stream = OctoPrintEventStream(
                base_url=self._client.base_url,
                api_key=self._client.api_key,
                on_message=self._handle_event,
            )
            self._event_stream.start()
        logger.info("[OctoPrintGateway] Started telemetry polling.")

    def stop(self) -> None:
        self._stop_event.set()
        if self._event_stream:
            self._event_stream.stop()
        if self._thread:
            self._thread.join(timeout=self._poll_interval_sec + 2)
        logger.info("[OctoPrintGateway] Stopped.")

    def send(self, gcode: str) -> GatewayCommandResult:
        return self.send_gcode(gcode)

    def send_gcode(self, gcode: str) -> GatewayCommandResult:
        started = datetime.utcnow()
        command_id = str(uuid.uuid4())[:8]
        try:
            self._client.send_gcode(gcode)
            elapsed = (datetime.utcnow() - started).total_seconds() * 1000
            return GatewayCommandResult(
                command_id=command_id,
                gcode=gcode,
                status=GatewayCommandStatus.OK,
                responses=("accepted by OctoPrint",),
                elapsed_ms=elapsed,
            )
        except Exception as exc:
            elapsed = (datetime.utcnow() - started).total_seconds() * 1000
            logger.warning("[OctoPrintGateway] G-code rejected: %s", exc)
            return GatewayCommandResult(
                command_id=command_id,
                gcode=gcode,
                status=GatewayCommandStatus.FAILED,
                error_message=str(exc),
                elapsed_ms=elapsed,
            )

    def pause(self) -> bool:
        return self._job_control(self._client.pause_job, "pause")

    def resume(self) -> bool:
        return self._job_control(self._client.resume_job, "resume")

    def cancel(self) -> bool:
        return self._job_control(self._client.cancel_job, "cancel")

    def _job_control(self, fn, name: str) -> bool:
        try:
            fn()
            return True
        except Exception:
            logger.exception("[OctoPrintGateway] Job control failed: %s", name)
            return False

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            self.poll_once()
            self._stop_event.wait(self._poll_interval_sec)

    def poll_once(self) -> None:
        updates = {}
        try:
            printer = self._client.get_printer()
            job = self._client.get_job()
        except Exception:
            logger.exception("[OctoPrintGateway] Telemetry poll failed.")
            self._state.update(status=PrinterStatus.ERROR)
            return

        state_text = (
            printer.get("state", {}).get("text")
            or job.get("state")
            or ""
        )
        updates["status"] = _map_status(state_text)

        temps = printer.get("temperature", {})
        tool0 = temps.get("tool0", {})
        bed = temps.get("bed", {})
        if "actual" in tool0:
            updates["nozzle_temp"] = _num(tool0.get("actual"))
        if "target" in tool0:
            updates["nozzle_target"] = _num(tool0.get("target"))
        if "actual" in bed:
            updates["bed_temp"] = _num(bed.get("actual"))
        if "target" in bed:
            updates["bed_target"] = _num(bed.get("target"))

        completion = job.get("progress", {}).get("completion")
        if completion is not None:
            updates["progress_pct"] = round(float(completion), 2)

        self._state.update(**updates)

    def _handle_event(self, message: dict) -> None:
        current = message.get("current")
        if isinstance(current, dict):
            self._apply_current_payload(current)

    def _apply_current_payload(self, current: dict) -> None:
        updates = {}
        state_text = current.get("state", {}).get("text") or current.get("state")
        if state_text:
            updates["status"] = _map_status(str(state_text))

        temps = current.get("temps") or []
        if temps:
            latest = temps[-1]
            tool0 = latest.get("tool0", {})
            bed = latest.get("bed", {})
            if "actual" in tool0:
                updates["nozzle_temp"] = _num(tool0.get("actual"))
            if "target" in tool0:
                updates["nozzle_target"] = _num(tool0.get("target"))
            if "actual" in bed:
                updates["bed_temp"] = _num(bed.get("actual"))
            if "target" in bed:
                updates["bed_target"] = _num(bed.get("target"))

        progress = current.get("progress") or {}
        completion = progress.get("completion")
        if completion is not None:
            updates["progress_pct"] = round(float(completion), 2)

        self._state.update(**updates)


class MockGateway:
    """In-memory gateway for simulator tests and local development."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.started = False
        self.paused = False
        self.cancelled = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.started = False

    def send(self, gcode: str) -> GatewayCommandResult:
        return self.send_gcode(gcode)

    def send_gcode(self, gcode: str) -> GatewayCommandResult:
        self.commands.append(gcode)
        return GatewayCommandResult(
            command_id=str(uuid.uuid4())[:8],
            gcode=gcode,
            status=GatewayCommandStatus.OK,
            responses=("mock ok",),
            elapsed_ms=0.0,
        )

    def pause(self) -> bool:
        self.paused = True
        return True

    def resume(self) -> bool:
        self.paused = False
        return True

    def cancel(self) -> bool:
        self.cancelled = True
        return True


def _map_status(status_text: str) -> PrinterStatus:
    normalized = status_text.strip().lower()
    if normalized in OCTOPRINT_STATUS_PRINTING:
        return PrinterStatus.PRINTING
    if normalized in OCTOPRINT_STATUS_PAUSED or normalized in OCTOPRINT_STATUS_PAUSING:
        return PrinterStatus.PAUSED
    if normalized in OCTOPRINT_STATUS_ERROR:
        return PrinterStatus.ERROR
    if normalized in OCTOPRINT_STATUS_CONNECTING:
        return PrinterStatus.REBOOTING
    if normalized in OCTOPRINT_STATUS_OPERATIONAL:
        return PrinterStatus.IDLE
    return PrinterStatus.UNKNOWN


def _num(value) -> Optional[float]:
    if value is None:
        return None
    return float(value)


def _websocket_enabled() -> bool:
    raw = os.environ.get(OCTOPRINT_WEBSOCKET_ENABLED_ENV, "0")
    return raw.strip().lower() in {"1", "true", "yes", "on"}
