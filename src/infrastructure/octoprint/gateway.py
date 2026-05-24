"""Printer gateway abstractions and OctoPrint implementation."""

from __future__ import annotations

import json
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
    def send_gcode_and_wait_response(
        self,
        gcode: str,
        timeout_sec: float = 4.0,
        poll_interval: float = 0.25,
    ) -> GatewayCommandResult: ...
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
        # Last textual response from a gateway command (for MQTT response field)
        self._last_command_response: Optional[str] = None
        # Pending commands awaiting printer response: list of (cmd_log_id, ts, gcode)
        self._pending_commands: list[tuple[str, float, str]] = []
        # Command result listeners: callbacks taking (commandLogId, response_text, is_error)
        self._command_listeners: list[callable] = []
        # Recent terminal responses (ts, line, is_error)
        self._recent_terminal: list[tuple[float, str, bool]] = []
        # One-shot recv listeners for awaiting command responses
        self._recv_listeners: dict[str, dict] = {}
        self._recv_lock = threading.Lock()

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
                responses=(),
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

    def send_gcode_and_wait_response(
        self,
        gcode: str,
        timeout_sec: float = 5.0,
        poll_interval: float = 0.3,
    ) -> GatewayCommandResult:
        """Send gcode and wait for the printer's Recv: response via event stream or REST fallback."""
        started = datetime.utcnow()
        command_id = str(uuid.uuid4())[:8]
        gcode_clean = (gcode or "").strip()

        # One-shot event used to wake the waiting thread when a Recv: arrives
        response_event = threading.Event()
        result_holder: dict = {}

        def on_recv_line(line: str, is_error: bool) -> None:
            result_holder["line"] = line
            result_holder["is_error"] = is_error
            response_event.set()

        listener_id = self._register_recv_listener(gcode_clean, on_recv_line)

        try:
            self._client.send_gcode(gcode_clean)
        except Exception as exc:
            self._unregister_recv_listener(listener_id)
            elapsed = (datetime.utcnow() - started).total_seconds() * 1000
            return GatewayCommandResult(
                command_id=command_id,
                gcode=gcode_clean,
                status=GatewayCommandStatus.FAILED,
                error_message=str(exc),
                elapsed_ms=elapsed,
            )

        got = response_event.wait(timeout=timeout_sec)
        self._unregister_recv_listener(listener_id)
        elapsed = (datetime.utcnow() - started).total_seconds() * 1000

        if not got:
            logger.warning(
                "[OctoPrintGateway] send_gcode_and_wait_response timed out for %r",
                gcode_clean,
            )
            return GatewayCommandResult(
                command_id=command_id,
                gcode=gcode_clean,
                status=GatewayCommandStatus.OK,
                responses=("timeout: no terminal confirmation",),
                elapsed_ms=elapsed,
            )

        line = result_holder.get("line")
        is_err = bool(result_holder.get("is_error"))
        logger.debug("[OctoPrintGateway] Received response for %r: %r (err=%s)", gcode_clean, line, is_err)
        return GatewayCommandResult(
            command_id=command_id,
            gcode=gcode_clean,
            status=GatewayCommandStatus.FAILED if is_err else GatewayCommandStatus.OK,
            responses=(line.strip() if isinstance(line, str) else str(line),),
            error_message=(line.strip() if is_err and isinstance(line, str) else None),
            elapsed_ms=elapsed,
        )

    def pause(self) -> bool:
        # Ask OctoPrint to pause via REST, then verify via /api/job that the
        # state moved to paused; otherwise fall back to sending M25.
        try:
            resp = self._client.pause_job()
            logger.debug("[OctoPrintGateway] pause_job response: %r", resp)
            # store a textual representation for upstream MQTT responders
            try:
                if isinstance(resp, dict):
                    # prefer message fields when present
                    self._last_command_response = json.dumps(resp)
                else:
                    self._last_command_response = str(resp)
            except Exception:
                self._last_command_response = str(resp)
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
            # capture response tuple from send
            if getattr(res, "responses", None):
                self._last_command_response = res.responses[-1] if len(res.responses) else None
            return res.succeeded
        except Exception:
            logger.exception("[OctoPrintGateway] fallback M25 failed")
            return False

    def resume(self) -> bool:
        try:
            resp = self._client.resume_job()
            logger.debug("[OctoPrintGateway] resume_job response: %r", resp)
            try:
                if isinstance(resp, dict):
                    self._last_command_response = json.dumps(resp)
                else:
                    self._last_command_response = str(resp)
            except Exception:
                self._last_command_response = str(resp)
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
            if getattr(res, "responses", None):
                self._last_command_response = res.responses[-1] if len(res.responses) else None
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
            try:
                if isinstance(resp, dict):
                    self._last_command_response = json.dumps(resp)
                else:
                    self._last_command_response = str(resp)
            except Exception:
                self._last_command_response = str(resp)
            return True
        except Exception:
            logger.exception("[OctoPrintGateway] Job control failed: %s", name)
            return False

    def _notify_pending_command(self, text: str, is_error: bool) -> None:
        if not text:
            return
        now = time.time()
        window = 5.0
        candidates = [p for p in self._pending_commands if now - p[1] <= window]
        if not candidates:
            return
        cmd_log_id, ts, gcode = candidates[-1]
        self._pending_commands = [p for p in self._pending_commands if p[0] != cmd_log_id]
        for cb in list(self._command_listeners):
            try:
                cb(cmd_log_id, text, is_error)
            except Exception:
                logger.exception("[OctoPrintGateway] command listener raised")

    def _extract_terminal_lines(self, message: dict) -> list[str]:
        lines: list[str] = []

        def add_line(item) -> None:
            if isinstance(item, str):
                lines.append(item)
                return
            if isinstance(item, dict):
                for key in ("line", "message", "text"):
                    text_val = item.get(key)
                    if isinstance(text_val, str):
                        lines.append(text_val)
                        return

        def walk(obj) -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key in ("terminal", "logs", "lines", "history", "messages"):
                        if isinstance(value, list):
                            for entry in value:
                                add_line(entry)
                    else:
                        walk(value)
            elif isinstance(obj, list):
                for entry in obj:
                    walk(entry)

        walk(message)
        return lines

    def _parse_terminal_lines(self, terminal_response: dict) -> list[str]:
        """Extract ordered lines from whatever structure get_terminal() returns."""
        lines: list[str] = []
        for key in ("logs", "history", "lines", "messages"):
            entries = terminal_response.get(key)
            if isinstance(entries, list):
                for entry in entries:
                    if isinstance(entry, str):
                        lines.append(entry)
                    elif isinstance(entry, dict):
                        for item_key in ("line", "message", "text"):
                            text_val = entry.get(item_key)
                            if isinstance(text_val, str):
                                lines.append(text_val)
                                break
                if lines:
                    return lines
        return lines

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

        # No REST terminal fallback: terminal endpoint is not reliable.

    def _handle_event(self, message: dict) -> None:
        # Quick diagnostic: log message shape and small sample of logs (temporary)
        keys = list(message.keys())
        if "current" in message:
            current = message["current"]
            logs = current.get("logs") or []
            logger.debug("[GW] WS current: keys=%s logs=%r", list(current.keys()), logs[:3])
        else:
            logger.debug("[GW] WS message keys: %s", keys)

        # Primary path: OctoPrint may include logs in the `current` payload.
        current = message.get("current")
        if isinstance(current, dict):
            try:
                for line in (current.get("logs") or []):
                    if isinstance(line, str):
                        try:
                            self._dispatch_recv_line(line)
                        except Exception:
                            logger.exception("[OctoPrintGateway] dispatch_recv_line failed for current.logs")
            except Exception:
                logger.exception("[OctoPrintGateway] Failed processing current.logs")
            self._apply_current_payload(current)

        # Secondary path: walk the full message for other terminal-like shapes
        for line in self._extract_terminal_lines(message):
            try:
                self._dispatch_recv_line(line)
            except Exception:
                logger.exception("[OctoPrintGateway] dispatch_recv_line failed for extracted lines")

    def _is_error_line(self, text: str) -> bool:
        lowered = text.lower()
        error_indicators = ("unknown command", "unknown gcode", "error", "invalid", "not supported")
        return any(ind in lowered for ind in error_indicators)

    def _is_ok_line(self, text: str) -> bool:
        lowered = text.lower()
        return "ok" in lowered

    def _record_terminal_response(self, text: str, is_error: bool) -> None:
        now = time.time()
        self._recent_terminal.append((now, text, is_error))
        # Keep only a small, recent window to avoid unbounded growth
        cutoff = now - 5.0
        self._recent_terminal = [t for t in self._recent_terminal if t[0] >= cutoff]

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

    def register_command_listener(self, callback: callable) -> None:
        self._command_listeners.append(callback)

    def track_pending_command(self, command_log_id: str, gcode: str) -> None:
        # record with current monotonic timestamp
        self._pending_commands.append((command_log_id, time.time(), gcode))
        # If a terminal response already arrived, resolve immediately.
        if self._recent_terminal:
            ts, line, is_error = self._recent_terminal[-1]
            if time.time() - ts <= 5.0:
                self._notify_pending_command(line, is_error)

    def _scan_for_printer_texts(self, message: dict) -> None:
        """Search message dict for textual printer responses and notify listeners.

        This is heuristic: look for substrings that indicate errors and map them
        to the most recent pending command sent within the last few seconds.
        """
        text_fragments: list[str] = []

        def collect_texts(obj):
            if isinstance(obj, str):
                text_fragments.append(obj)
            elif isinstance(obj, dict):
                for v in obj.values():
                    collect_texts(v)
            elif isinstance(obj, list) or isinstance(obj, tuple):
                for v in obj:
                    collect_texts(v)

        collect_texts(message)
        if not text_fragments:
            return

        # Emit debug logs so we can inspect raw event payload text fragments
        try:
            logger.debug("[OctoPrintGateway] collected text fragments: %r", text_fragments)
        except Exception:
            pass

        joined = " ".join(text_fragments).lower()
        try:
            logger.debug("[OctoPrintGateway] joined event text: %s", joined)
        except Exception:
            pass
        # simple error indicators
        error_indicators = ("unknown command", "unknown gcode", "error", "invalid", "not supported")
        is_error = any(ind in joined for ind in error_indicators)
        if not is_error:
            return

        self._notify_pending_command(joined, True)

    # ------------------------------------------------------------------
    # Recv-listener registration and dispatch
    # ------------------------------------------------------------------
    def _register_recv_listener(self, gcode: str, callback) -> str:
        lid = str(uuid.uuid4())[:8]
        entry = {
            "gcode": (gcode or "").strip().upper(),
            "callback": callback,
            "registered_at": time.time(),
        }
        with self._recv_lock:
            self._recv_listeners[lid] = entry
        return lid

    def _unregister_recv_listener(self, lid: str) -> None:
        with self._recv_lock:
            self._recv_listeners.pop(lid, None)

    def _dispatch_recv_line(self, line: str) -> None:
        """Dispatch an extracted terminal line to pending listeners and record it."""
        if not isinstance(line, str):
            return
        line_text = line.strip()
        line_lower = line_text.lower()

        # Only care about Recv: lines
        if not line_lower.startswith("recv:") and not line_lower.startswith("ok"):
            # still record non-recv lines for pending heuristics
            is_err = self._is_error_line(line_text)
            self._record_terminal_response(line_text, is_err)
            self._notify_pending_command(line_text, is_err)
            return

        is_err = self._is_error_line(line_text)
        self._record_terminal_response(line_text, is_err)

        # Fire the oldest waiting listener (commands are sequential)
        with self._recv_lock:
            if not self._recv_listeners:
                return
            # pick the oldest listener by registered_at
            oldest = min(self._recv_listeners.items(), key=lambda kv: kv[1].get("registered_at", 0))
            lid, entry = oldest
            try:
                entry["callback"](line_text, is_err)
            except Exception:
                logger.exception("[OctoPrintGateway] recv listener raised")
            # remove it (one-shot)
            try:
                self._recv_listeners.pop(lid, None)
            except Exception:
                pass

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

    def send_gcode_and_wait_response(
        self,
        gcode: str,
        timeout_sec: float = 5.0,
        poll_interval: float = 0.3,
    ) -> GatewayCommandResult:
        return self.send_gcode(gcode)

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
