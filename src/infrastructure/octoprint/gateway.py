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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()

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
                logger.info(
                    "[OctoPrintGateway] Websocket unavailable; started REST telemetry polling."
                )
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

    # ------------------------------------------------------------------
    # G-code sending
    # ------------------------------------------------------------------

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
        """Send a G-code command and wait for the printer's terminal response.

        The response is captured from the persistent OctoPrint WebSocket stream
        (the same connection used for telemetry).  A one-shot listener is
        registered *before* the command is sent so that the "Recv: ok" (or any
        other printer reply) that arrives on the main stream is reliably matched
        back to this call.

        If the persistent stream is not available the call falls back to a
        fire-and-forget send via send_gcode().
        """
        started = datetime.utcnow()
        command_id = str(uuid.uuid4())[:8]
        gcode_clean = (gcode or "").strip()

        # ----------------------------------------------------------------
        # Path A: persistent WebSocket stream is running — use it.
        # ----------------------------------------------------------------
        if self._event_stream is not None:
            result_holder: dict[str, object] = {}
            done_event = threading.Event()

            def _on_recv(line: str, is_error: bool) -> None:
                result_holder["line"] = line
                result_holder["is_error"] = is_error
                done_event.set()

            # Register the listener BEFORE sending so we cannot miss a fast reply.
            listener_id = self._register_recv_listener(gcode_clean, _on_recv)

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

            got_response = done_event.wait(timeout=timeout_sec)
            self._unregister_recv_listener(listener_id)
            elapsed = (datetime.utcnow() - started).total_seconds() * 1000

            if got_response:
                line = str(result_holder.get("line", ""))
                is_err = bool(result_holder.get("is_error"))
                logger.info(
                    "[OctoPrintGateway] Got response for %r: %r", gcode_clean, line
                )
                return GatewayCommandResult(
                    command_id=command_id,
                    gcode=gcode_clean,
                    status=GatewayCommandStatus.FAILED if is_err else GatewayCommandStatus.OK,
                    responses=(line.strip(),),
                    error_message=(line.strip() if is_err else None),
                    elapsed_ms=elapsed,
                )

            # Timed out — no printer reply arrived on the stream within the window.
            logger.warning(
                "[OctoPrintGateway] send_gcode_and_wait_response timed out for %r",
                gcode_clean,
            )
            return GatewayCommandResult(
                command_id=command_id,
                gcode=gcode_clean,
                status=GatewayCommandStatus.FAILED,
                responses=("timeout: no terminal confirmation",),
                error_message=f"Timeout: no printer response for '{gcode_clean}'",
                elapsed_ms=elapsed,
            )

        # ----------------------------------------------------------------
        # Path B: no persistent stream — fire-and-forget.
        # ----------------------------------------------------------------
        logger.warning(
            "[OctoPrintGateway] No persistent WebSocket stream; "
            "falling back to fire-and-forget send for %r",
            gcode_clean,
        )
        return self.send_gcode(gcode_clean)

    # ------------------------------------------------------------------
    # Job control
    # ------------------------------------------------------------------

    def pause(self) -> bool:
        try:
            resp = self._client.pause_job()
            logger.debug("[OctoPrintGateway] pause_job response: %r", resp)
            try:
                self._last_command_response = (
                    json.dumps(resp) if isinstance(resp, dict) else str(resp)
                )
            except Exception:
                self._last_command_response = str(resp)
        except Exception:
            logger.exception("[OctoPrintGateway] pause_job request failed")

        start = time.time()
        while time.time() - start < OCTOPRINT_JOB_CONTROL_VERIFY_SEC:
            try:
                job = self._client.get_job()
            except Exception:
                job = {}
            state = (job.get("state") or "").lower()
            if state in (s.lower() for s in OCTOPRINT_STATUS_PAUSED) or state in (
                s.lower() for s in OCTOPRINT_STATUS_PAUSING
            ):
                return True
            time.sleep(0.2)

        logger.warning(
            "[OctoPrintGateway] pause not observed via telemetry; falling back to M25"
        )
        try:
            res = self.send("M25")
            if getattr(res, "responses", None):
                self._last_command_response = (
                    res.responses[-1] if res.responses else None
                )
            return res.succeeded
        except Exception:
            logger.exception("[OctoPrintGateway] fallback M25 failed")
            return False

    def resume(self) -> bool:
        try:
            resp = self._client.resume_job()
            logger.debug("[OctoPrintGateway] resume_job response: %r", resp)
            try:
                self._last_command_response = (
                    json.dumps(resp) if isinstance(resp, dict) else str(resp)
                )
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

        logger.warning(
            "[OctoPrintGateway] resume not observed via telemetry; falling back to M24"
        )
        try:
            res = self.send("M24")
            if getattr(res, "responses", None):
                self._last_command_response = (
                    res.responses[-1] if res.responses else None
                )
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
                self._last_command_response = (
                    json.dumps(resp) if isinstance(resp, dict) else str(resp)
                )
            except Exception:
                self._last_command_response = str(resp)
            return True
        except Exception:
            logger.exception("[OctoPrintGateway] Job control failed: %s", name)
            return False

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

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
            if "409" in msg or "not operational" in msg.lower():
                now = time.time()
                if now - self._last_nonop_warning >= self._nonop_backoff_sec:
                    logger.warning(
                        "[OctoPrintGateway] Printer not operational: %s", msg
                    )
                    self._last_nonop_warning = now
                else:
                    logger.debug(
                        "[OctoPrintGateway] Printer not operational (suppressed warn): %s",
                        msg,
                    )
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
            printer.get("state", {}).get("text") or job.get("state") or ""
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
        logger.debug("[GW] _handle_event keys=%s", list(message.keys()))

        # Primary path: extract terminal lines from the `current` payload and
        # dispatch to any waiting recv listeners.
        #
        # OctoPrint sends two overlapping arrays in each `current` frame:
        #   - `logs`     — lines prefixed with "Send:" / "Recv:" / "Comm:"
        #   - `messages` — the bare printer text (no prefix), e.g. "ok", "T:21.3 ..."
        #
        # In practice, **real-time** `current` frames only populate `messages`
        # (logs is empty) while the initial `history` snapshot populates both.
        # We therefore read both arrays here and synthesise a "Recv:"-prefixed
        # line from each `messages` entry so that _dispatch_recv_line can
        # filter noise uniformly.
        #
        # `history` is intentionally skipped for dispatch to avoid replaying
        # old lines into freshly-registered listeners.
        current = message.get("current")
        if isinstance(current, dict):
            # Collect all terminal lines, preferring the prefixed `logs` array
            # and falling back to synthesising from `messages`.
            terminal_lines: list[str] = []

            logs = current.get("logs") or []
            logger.debug("[GW] current.logs count=%d sample=%r", len(logs), logs[:3])
            for line in logs:
                if isinstance(line, str):
                    terminal_lines.append(line)

            # Also read `messages` — this is the only place M105 responses
            # appear in real-time frames (logs is empty for temperature pings).
            msgs = current.get("messages") or []
            logger.debug("[GW] current.messages count=%d sample=%r", len(msgs), msgs[:3])
            for msg in msgs:
                if isinstance(msg, str):
                    # Synthesise a "Recv:" prefix so _dispatch_recv_line
                    # can process it uniformly alongside real log lines.
                    terminal_lines.append(f"Recv: {msg}")

            for line in terminal_lines:
                try:
                    self._dispatch_recv_line(line)
                except Exception:
                    logger.exception(
                        "[OctoPrintGateway] dispatch_recv_line failed for %r", line
                    )

            self._apply_current_payload(current)

        # Apply telemetry from history snapshots (temperatures, state),
        # but do NOT dispatch their log lines to recv listeners.
        history = message.get("history")
        if isinstance(history, dict):
            self._apply_current_payload(history)

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
    # Recv-listener registration and dispatch
    # ------------------------------------------------------------------

    def _register_recv_listener(self, gcode: str, callback) -> str:
        """Register a one-shot listener that fires on the next meaningful Recv: line."""
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
        """Dispatch a single log line to the oldest waiting recv listener.

        Only lines that start with "Recv:" and carry a meaningful printer
        response are forwarded.  Noise lines ("wait", "Not SD printing") are
        silently dropped so they do not prematurely resolve a listener.
        """
        if not isinstance(line, str):
            return
        line = line.strip()
        logger.debug("[GW] _dispatch_recv_line: %r", line[:80])

        # Must be a terminal receive line
        if not line.lower().startswith("recv:"):
            return

        # Strip the "Recv:" prefix and check for noise
        content = line[5:].strip()
        if content.lower() in ("wait", "not sd printing", ""):
            return

        is_error = self._is_error_line(content)
        self._record_terminal_response(line, is_error)

        now = time.time()
        with self._recv_lock:
            if not self._recv_listeners:
                return

            # Always dispatch to the oldest registered listener so that
            # concurrent callers are served in FIFO order.
            oldest_lid, oldest_entry = min(
                self._recv_listeners.items(),
                key=lambda kv: kv[1]["registered_at"],
            )

            # Drop stale listeners (should not happen in normal flow, but
            # guards against leaked listeners if a thread was interrupted).
            if now - oldest_entry["registered_at"] > 10.0:
                self._recv_listeners.pop(oldest_lid, None)
                logger.warning(
                    "[GW] Dropped stale recv listener for %r",
                    oldest_entry["gcode"],
                )
                return

            logger.info(
                "[GW] Dispatching %r to listener for gcode=%r",
                content,
                oldest_entry["gcode"],
            )
            try:
                oldest_entry["callback"](line, is_error)
            except Exception:
                logger.exception("[GW] recv listener callback raised")
            finally:
                self._recv_listeners.pop(oldest_lid, None)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_error_line(self, text: str) -> bool:
        lowered = text.lower()
        error_indicators = (
            "unknown command",
            "unknown gcode",
            "error",
            "invalid",
            "not supported",
        )
        return any(ind in lowered for ind in error_indicators)

    def _is_ok_line(self, text: str) -> bool:
        return "ok" in text.lower()

    def _record_terminal_response(self, text: str, is_error: bool) -> None:
        now = time.time()
        self._recent_terminal.append((now, text, is_error))
        cutoff = now - 5.0
        self._recent_terminal = [t for t in self._recent_terminal if t[0] >= cutoff]

    def _notify_pending_command(self, text: str, is_error: bool) -> None:
        if not text:
            return
        now = time.time()
        window = 5.0
        candidates = [p for p in self._pending_commands if now - p[1] <= window]
        if not candidates:
            return
        cmd_log_id, ts, gcode = candidates[-1]
        self._pending_commands = [
            p for p in self._pending_commands if p[0] != cmd_log_id
        ]
        for cb in list(self._command_listeners):
            try:
                cb(cmd_log_id, text, is_error)
            except Exception:
                logger.exception("[OctoPrintGateway] command listener raised")

    def register_command_listener(self, callback: callable) -> None:
        self._command_listeners.append(callback)

    def track_pending_command(self, command_log_id: str, gcode: str) -> None:
        self._pending_commands.append((command_log_id, time.time(), gcode))
        if self._recent_terminal:
            ts, line, is_error = self._recent_terminal[-1]
            if time.time() - ts <= 5.0:
                self._notify_pending_command(line, is_error)

    # ------------------------------------------------------------------
    # File management passthroughs
    # ------------------------------------------------------------------

    def upload_file(self, source_url: str, target_name: Optional[str] = None) -> str:
        try:
            return self._client.upload_file(source_url, target_name)
        except Exception:
            logger.exception("[OctoPrintGateway] upload_file failed.")
            raise

    def print_file(self, filename: str) -> bool:
        try:
            self._client.print_file(filename)
            return True
        except Exception as exc:
            msg = str(exc)
            if "409" in msg or "already printing" in msg.lower():
                logger.warning(
                    "[OctoPrintGateway] print_file conflict for %s: %s — "
                    "attempting to cancel current job and start new file",
                    filename,
                    msg,
                )
                try:
                    cancel_resp = self._client.cancel_job()
                    logger.info(
                        "[OctoPrintGateway] cancel_job response: %r", cancel_resp
                    )
                except Exception:
                    logger.exception(
                        "[OctoPrintGateway] cancel_job failed; "
                        "will still attempt to wait for idle state"
                    )

                wait_start = time.time()
                while time.time() - wait_start < OCTOPRINT_PRINT_WAIT_SEC:
                    try:
                        job = self._client.get_job()
                    except Exception:
                        job = {}
                    state = (job.get("state") or "").lower()
                    if state not in (s.lower() for s in OCTOPRINT_STATUS_PRINTING):
                        try:
                            self._client.print_file(filename)
                            return True
                        except Exception as exc2:
                            logger.warning(
                                "[OctoPrintGateway] Retry select/print failed after cancel: %s",
                                exc2,
                            )
                            return False
                    time.sleep(self._poll_interval_sec)

                logger.error(
                    "[OctoPrintGateway] print_file timed out waiting for printer "
                    "to become idle after cancel attempt"
                )
                return False
            logger.exception("[OctoPrintGateway] print_file failed.")
            return False

    def get_job(self) -> dict:
        try:
            return self._client.get_job()
        except Exception:
            logger.exception("[OctoPrintGateway] get_job failed.")
            return {}


# ---------------------------------------------------------------------------
# Mock gateway for tests / local dev
# ---------------------------------------------------------------------------

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

    def upload_file(self, source_url: str, target_name: Optional[str] = None) -> str:
        filename = target_name or source_url.split("/")[-1] or "upload.gcode"
        self.uploads.append(filename)
        return filename

    def print_file(self, filename: str) -> bool:
        self.started = True
        self._progress = 0.0
        return True

    def get_job(self) -> dict:
        return {"progress": {"completion": self._progress}}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

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