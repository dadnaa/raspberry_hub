"""Incoming MQTT command router.

Validated MQTT commands are sent through the printer gateway abstraction and
reported back to the cloud as QUEUED, EXECUTING, SUCCESS, or ERROR.
"""

import logging
import threading

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
    ) -> None:
        gateway = printer_gateway if printer_gateway is not None else command_engine
        if gateway is None:
            raise ValueError("CommandRouter requires printer_gateway.")

        self._topics = topics
        self._pub = publisher
        self._val = validator
        self._printer_gateway = gateway

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

        self._pub.command_response(CommandResponseMessage(
            printerId=printer_id,
            commandName=command_name,
            gcode=gcode,
            status="QUEUED",
        ))

        self._pub.command_response(CommandResponseMessage(
            printerId=printer_id,
            commandName=command_name,
            gcode=gcode,
            status="EXECUTING",
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
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="SUCCESS",
                    ))
                    logger.info("[Router] SUCCESS: %s (mapped M25 -> pause)", command_name)
                else:
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="ERROR",
                        reason="Gateway pause failed",
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
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="SUCCESS",
                    ))
                    logger.info("[Router] SUCCESS: %s (mapped M24 -> resume)", command_name)
                else:
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="ERROR",
                        reason="Gateway resume failed",
                    ))
                    logger.warning("[Router] FAILED: %s - gateway resume failed", command_name)

            else:
                result = self._printer_gateway.send(gcode)

                if result.succeeded:
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="SUCCESS",
                    ))
                    logger.info(
                        "[Router] SUCCESS: %s in %.1fms",
                        command_name,
                        result.elapsed_ms or 0.0,
                    )
                else:
                    reason = f"Gateway status: {result.status.name}"
                    self._pub.command_response(CommandResponseMessage(
                        printerId=printer_id,
                        commandName=command_name,
                        gcode=gcode,
                        status="ERROR",
                        reason=reason,
                    ))
                    logger.warning("[Router] FAILED: %s - %s", command_name, reason)

        except Exception as exc:
            reason = str(exc)
            self._pub.command_response(CommandResponseMessage(
                printerId=printer_id,
                commandName=command_name,
                gcode=gcode,
                status="ERROR",
                reason=reason,
            ))
            logger.exception("[Router] Exception executing %s: %s", command_name, exc)
