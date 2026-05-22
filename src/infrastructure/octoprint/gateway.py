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
    OCTOPRINT_PRINT_WAIT_SEC,
    OCTOPRINT_PRINT_START_WAIT_SEC,
    OCTOPRINT_JOB_CONTROL_VERIFY_SEC,
    OCTOPRINT_NONOP_WARN_BACKOFF_SEC,
)
import os
from src.core.printer_state import PrinterStatus
from src.core.state_manager import StateManager
from src.infrastructure.octoprint.client import OctoPrintClient, OctoPrintError
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
    def upload_file(self, source_url: str, target_name: Optional[str] = None) -> str: ...
    def print_file(self, filename: str) -> bool: ...
    def get_job(self) -> dict: ...


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
        # Backoff control for repeated non-operational warnings
        self._last_nonop_warning: float = 0.0
        self._nonop_backoff_sec: float = OCTOPRINT_NONOP_WARN_BACKOFF_SEC

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()

        # If websocket mode is enabled, attempt to use the event stream.
        # Perform one initial REST poll to prime state, then start the
        # websocket. If the websocket client is not available, fall back
        # to the REST poll loop.
        if _websocket_enabled():
            self._event_stream = OctoPrintEventStream(
                base_url=self._client.base_url,
                api_key=self._client.api_key,
                on_message=self._handle_event,
            )
            ws_started = self._event_stream.start()
            try:
                self.poll_once()
            except Exception:
                logger.exception("[OctoPrintGateway] Initial telemetry poll failed.")

            if not ws_started:
                # websocket requested but not available; use REST polling
                self._thread = threading.Thread(
                    target=self._poll_loop,
                    name="OctoPrintTelemetry",
                    daemon=True,
                )
                self._thread.start()
                logger.info("[OctoPrintGateway] Websocket unavailable; started REST telemetry polling.")
            else:
                logger.info("[OctoPrintGateway] Started telemetry via websocket.")
            return

        # Default: REST polling
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="OctoPrintTelemetry",
            daemon=True,
        )
        self._thread.start()
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
        # Ask OctoPrint to pause via REST, then verify via /api/job that the
        # state moved to paused; otherwise fall back to sending M25.
        try:
            resp = self._client.pause_job()
            logger.debug("[OctoPrintGateway] pause_job response: %r", resp)
        except Exception:
            logger.exception("[OctoPrintGateway] pause_job request failed")
        # Verify
        start = time.time()
        while time.time() - start < OCTOPRINT_JOB_CONTROL_VERIFY_SEC:
            try:
                job = self._client.get_job()
            except Exception:
                job = {}
            state = (job.get("state") or "").lower()
            if state in (s.lower() for s in OCTOPRINT_STATUS_PAUSED) or state in (s.lower() for s in OCTOPRINT_STATUS_PAUSING):
                return True
            time.sleep(0.2)
        logger.warning("[OctoPrintGateway] pause not observed via telemetry; falling back to M25")
        try:
            res = self.send("M25")
            return res.succeeded
        except Exception:
            logger.exception("[OctoPrintGateway] fallback M25 failed")
            return False

    def resume(self) -> bool:
        try:
            resp = self._client.resume_job()
            logger.debug("[OctoPrintGateway] resume_job response: %r", resp)
        except Exception:
            logger.exception("[OctoPrintGateway] resume_job request failed")
        start = time.time()
        while time.time() - start < OCTOPRINT_JOB_CONTROL_VERIFY_SEC:
            try:
                job = self._client.get_job()
            except Exception:
                job = {}
            state = (job.get("state") or "").lower()
            if state in (s.lower() for s in OCTOPRINT_STATUS_PRINTING):
                return True
            time.sleep(0.2)
        logger.warning("[OctoPrintGateway] resume not observed via telemetry; falling back to M24")
        try:
            res = self.send("M24")
            return res.succeeded
        except Exception:
            logger.exception("[OctoPrintGateway] fallback M24 failed")
            return False

    def cancel(self) -> bool:
        return self._job_control(self._client.cancel_job, "cancel")

    def _job_control(self, fn, name: str) -> bool:
        try:
            resp = fn()
            logger.debug("[OctoPrintGateway] Job control %s response: %r", name, resp)
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
        except OctoPrintError as e:
            msg = str(e)
            # OctoPrint may return 409 with a JSON error when the printer
            # is not operational. Treat that as ERROR but rate-limit warnings
            # to avoid log spam during extended offline periods.
            if "409" in msg or "not operational" in msg.lower():
                now = time.time()
                if now - self._last_nonop_warning >= self._nonop_backoff_sec:
                    logger.warning("[OctoPrintGateway] Printer not operational: %s", msg)
                    self._last_nonop_warning = now
                else:
                    logger.debug("[OctoPrintGateway] Printer not operational (suppressed warn): %s", msg)
                self._state.update(status=PrinterStatus.ERROR)
                return
            logger.exception("[OctoPrintGateway] Telemetry poll failed.")
            self._state.update(status=PrinterStatus.ERROR)
            return
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

    # ------------------------------------------------------------------
    # Convenience passthroughs to OctoPrintClient
    # ------------------------------------------------------------------
    def upload_file(self, source_url: str, target_name: Optional[str] = None) -> str:
        """Download and upload a file to OctoPrint local storage via client."""
        try:
            return self._client.upload_file(source_url, target_name)
        except Exception:
            logger.exception("[OctoPrintGateway] upload_file failed.")
            raise

    def print_file(self, filename: str) -> bool:
        """Select and start printing a file already uploaded to OctoPrint."""
        try:
            self._client.print_file(filename)
            return True
        except Exception as exc:
            msg = str(exc)
            # If OctoPrint reports that a job is already printing, wait until
            # it finishes (bounded by OCTOPRINT_PRINT_WAIT_SEC) and retry.
            if "409" in msg or "already printing" in msg.lower():
                logger.warning("[OctoPrintGateway] print_file conflict for %s: %s — attempting to cancel current job and start new file", filename, msg)

                # Try to cancel the running job first.
                try:
                    cancel_resp = self._client.cancel_job()
                    logger.info("[OctoPrintGateway] cancel_job response: %r", cancel_resp)
                except Exception:
                    logger.exception("[OctoPrintGateway] cancel_job failed; will still attempt to wait for idle state")

                # Wait until printer is no longer in PRINTING state, up to timeout.
                wait_start = time.time()
                while time.time() - wait_start < OCTOPRINT_PRINT_WAIT_SEC:
                    try:
                        job = self._client.get_job()
                    except Exception:
                        job = {}
                    state = (job.get("state") or "").lower()
                    if state not in (s.lower() for s in OCTOPRINT_STATUS_PRINTING):
                        # Printer is idle or otherwise not printing; attempt to select and print the file.
                        try:
                            self._client.print_file(filename)
                            return True
                        except Exception as exc2:
                            logger.warning("[OctoPrintGateway] Retry select/print failed after cancel: %s", exc2)
                            return False
                    time.sleep(self._poll_interval_sec)

                logger.error("[OctoPrintGateway] print_file timed out waiting for printer to become idle after cancel attempt")
                return False
            logger.exception("[OctoPrintGateway] print_file failed.")
            return False

    def get_job(self) -> dict:
        """Return OctoPrint /api/job payload (or empty dict on failure)."""
        try:
            return self._client.get_job()
        except Exception:
            logger.exception("[OctoPrintGateway] get_job failed.")
            return {}


class MockGateway:
    """In-memory gateway for simulator tests and local development."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.started = False
        self.paused = False
        self.cancelled = False
        self.uploads: list[str] = []
        self._progress: float = 0.0

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

    # Upload/print API for tests and local dev
    def upload_file(self, source_url: str, target_name: Optional[str] = None) -> str:
        if target_name:
            filename = target_name
        else:
            filename = source_url.split("/")[-1] or "upload.gcode"
        self.uploads.append(filename)
        return filename

    def print_file(self, filename: str) -> bool:
        self.started = True
        # reset progress for the file
        self._progress = 0.0
        return True

    def get_job(self) -> dict:
        return {"progress": {"completion": self._progress}}


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
