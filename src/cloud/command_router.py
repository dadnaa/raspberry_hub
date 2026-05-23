"""Incoming MQTT command router.

Validated MQTT commands are sent through the printer gateway abstraction and
reported back to the cloud as QUEUED, EXECUTING, SUCCESS, or ERROR.
"""

import logging
import threading
import time

from src.cloud.message_validator import MessageValidator, ValidationError
from src.cloud.mqtt_publisher import MQTTPublisher
from src.cloud.mqtt_topics import MQTTTopics
from src.core.models import CommandMessage, CommandResponseMessage

logger = logging.getLogger(__name__)


class CommandRouter:
    """Routes cloud commands into the printer gateway with MQTT feedback."""

    def __init__(
        self,
        topics: MQTTTopics,
        publisher: MQTTPublisher,
        validator: MessageValidator,
        printer_gateway=None,
        command_engine=None,
        job_manager=None,
    ) -> None:
        gateway = printer_gateway if printer_gateway is not None else command_engine
        if gateway is None:
            raise ValueError("CommandRouter requires printer_gateway.")

        self._topics = topics
        self._pub = publisher
        self._val = validator
        self._printer_gateway = gateway
        self._job_manager = job_manager
        # pending command results: commandLogId -> {'printer_id', 'command_name', 'gcode'}
        self._pending_results: dict[str, dict] = {}
        # register for gateway command notifications when supported
        try:
            if hasattr(self._printer_gateway, "register_command_listener"):
                self._printer_gateway.register_command_listener(self._on_command_result)
        except Exception:
            logger.exception("[Router] Failed to register command listener on gateway")

    def handle_command(self, payload: str) -> None:
        """Handle printer/{id}/command messages."""
        try:
            cmd = self._val.parse_command(payload)
        except ValidationError as exc:
            logger.warning("[Router] Rejected command: %s", exc)
            return

        logger.info("[Router] Accepted command: %s -> %r", cmd.commandName, cmd.gcode)
        threading.Thread(
            target=self._execute_command,
            args=(cmd,),
            daemon=True,
        ).start()

    def handle_start_job(self, payload: str) -> None:
        """Legacy Sprint 4 hook. Full job dispatch is owned by MQTTBridge."""
        try:
            job = self._val.parse_start_job(payload)
        except ValidationError as exc:
            logger.warning("[Router] Rejected start-job: %s", exc)
            return
        logger.info(
            "[Router] start-job received: jobId=%r url=%r",
            job.jobId,
            job.fileUrl,
        )

    def _execute_command(self, cmd: CommandMessage) -> None:
        printer_id = cmd.printerId
        command_name = cmd.commandName
        gcode = cmd.gcode

        def _track_terminal_response() -> None:
            self._pending_results[cmd.commandLogId] = {
                "printer_id": printer_id,
                "command_name": command_name,
                "gcode": gcode,
            }

        self._pub.command_response(CommandResponseMessage(
            printerId=printer_id,
            commandName=command_name,
            gcode=gcode,
            status="QUEUED",
            commandLogId=cmd.commandLogId,
        ))

        self._pub.command_response(CommandResponseMessage(
            printerId=printer_id,
            commandName=command_name,
            gcode=gcode,
            status="EXECUTING",
            commandLogId=cmd.commandLogId,
        ))

        try:
            # Detect common pause/resume G-code and use job-control APIs where possible.
            g_upper = (gcode or "").strip().upper()
            if g_upper.startswith("M25"):
                ok = False
                try:
                    if hasattr(self._printer_gateway, "pause"):
                        ok = self._printer_gateway.pause()
                except Exception as exc:
                    logger.exception("[Router] Exception calling gateway.pause(): %s", exc)

                if ok:
                    # If we have a JobManager, mark the active job paused.
                    try:
                        if hasattr(self, "_job_manager") and self._job_manager:
                            active = self._job_manager.active_job
                            if active:
                                self._job_manager.pause(active.job_id)
                    except Exception:
                        logger.exception("[Router] Failed to pause active job in JobManager.")
                    try:
                        if hasattr(self._printer_gateway, "track_pending_command"):
                            self._printer_gateway.track_pending_command(cmd.commandLogId, gcode)
                    except Exception:
                        logger.exception("[Router] Failed to track pending command on gateway")
                    _track_terminal_response()
                else:
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="ERROR",
                        reason="Gateway pause failed",
                        commandLogId=cmd.commandLogId,
                        response=getattr(self._printer_gateway, "_last_command_response", None),
                    ))
                    logger.warning("[Router] FAILED: %s - gateway pause failed", command_name)

            elif g_upper.startswith("M24"):
                ok = False
                try:
                    if hasattr(self._printer_gateway, "resume"):
                        ok = self._printer_gateway.resume()
                except Exception as exc:
                    logger.exception("[Router] Exception calling gateway.resume(): %s", exc)

                if ok:
                    # If we have a JobManager, mark the active job resumed.
                    try:
                        if hasattr(self, "_job_manager") and self._job_manager:
                            active = self._job_manager.active_job
                            if active:
                                self._job_manager.resume(active.job_id)
                    except Exception:
                        logger.exception("[Router] Failed to resume active job in JobManager.")
                    try:
                        if hasattr(self._printer_gateway, "track_pending_command"):
                            self._printer_gateway.track_pending_command(cmd.commandLogId, gcode)
                    except Exception:
                        logger.exception("[Router] Failed to track pending command on gateway")
                    _track_terminal_response()
                else:
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="ERROR",
                        reason="Gateway resume failed",
                        commandLogId=cmd.commandLogId,
                        response=getattr(self._printer_gateway, "_last_command_response", None),
                    ))
                    logger.warning("[Router] FAILED: %s - gateway resume failed", command_name)

            elif (
                g_upper.startswith("M112")
                or g_upper.startswith("M0")
                or g_upper.startswith("M18")
                or g_upper.startswith("M410")
                or command_name.strip().lower() in ("cancel", "stop", "cancel-job")
            ):
                ok = False
                try:
                    if hasattr(self._printer_gateway, "cancel"):
                        ok = self._printer_gateway.cancel()
                except Exception as exc:
                    logger.exception("[Router] Exception calling gateway.cancel(): %s", exc)

                if ok:
                    try:
                        if hasattr(self, "_job_manager") and self._job_manager:
                            active = self._job_manager.active_job
                            if active:
                                self._job_manager.cancel(active.job_id)
                    except Exception:
                        logger.exception("[Router] Failed to cancel active job in JobManager.")
                    try:
                        if hasattr(self._printer_gateway, "track_pending_command"):
                            self._printer_gateway.track_pending_command(cmd.commandLogId, gcode)
                    except Exception:
                        logger.exception("[Router] Failed to track pending command on gateway")
                    _track_terminal_response()
                else:
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="ERROR",
                        reason="Gateway cancel failed",
                        commandLogId=cmd.commandLogId,
                        response=getattr(self._printer_gateway, "_last_command_response", None),
                    ))
                    logger.warning("[Router] FAILED: %s - gateway cancel failed", command_name)

            else:
                result = self._printer_gateway.send(gcode)
                try:
                    if hasattr(self._printer_gateway, "track_pending_command"):
                        self._printer_gateway.track_pending_command(cmd.commandLogId, gcode)
                except Exception:
                    logger.exception("[Router] Failed to track pending command on gateway")
                _track_terminal_response()

        except Exception as exc:
            reason = str(exc)
            self._pub.command_response(CommandResponseMessage(
                printerId=printer_id,
                commandName=command_name,
                gcode=gcode,
                status="ERROR",
                reason=reason,
                commandLogId=cmd.commandLogId,
            ))
            logger.exception("[Router] Exception executing %s: %s", command_name, exc)

    def _on_command_result(self, command_log_id: str, resp_text: str, is_error: bool) -> None:
        """Called by gateway when a printer response correlated to a pending
        command is discovered. Marks the pending waiter so the router thread
        can publish an ERROR state immediately.
        """
        info = self._pending_results.pop(command_log_id, None)
        if not info:
            return
        status = "ERROR" if is_error else "SUCCESS"
        self._pub.command_response(CommandResponseMessage(
            printerId=info.get("printer_id"),
            commandName=info.get("command_name"),
            gcode=info.get("gcode"),
            status=status,
            commandLogId=command_log_id,
            response=resp_text,
            reason=resp_text if is_error else None,
        ))
        logger.info("[Router] %s: %s", status, command_log_id)
