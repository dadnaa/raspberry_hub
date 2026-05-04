"""
command_router.py — Incoming Command Router + Feedback Publisher

Receives validated MQTT messages and routes them into the Command Queue.
Publishes CommandResponseMessage at every lifecycle stage.

Pipeline per incoming command:
  1. Parse + validate payload          (MessageValidator)
  2. Publish QUEUED response            (MQTTPublisher)
  3. Inject into CommandEngine queue    (CommandEngine.send / send_batch)
  4. Publish EXECUTING response
  5. Publish SUCCESS or ERROR response

CRITICAL: No direct serial writes happen here.
          The CommandEngine is the only thing that touches the printer.
"""

import logging
import threading
from typing import Optional

from src.core.models import (
    CommandMessage,
    StartJobMessage,
    CommandResponseMessage,
)
from src.mqtt.mqtt_topics      import MQTTTopics
from src.mqtt.mqtt_publisher   import MQTTPublisher
from src.mqtt.message_validator import MessageValidator, ValidationError

logger = logging.getLogger(__name__)


class CommandRouter:
    """
    Routes cloud commands into the local command engine with full
    lifecycle feedback published back over MQTT.

    Args:
        topics      – MQTTTopics for this printer
        publisher   – MQTTPublisher (upstream messages)
        validator   – MessageValidator
        command_engine – Sprint 2 CommandEngine instance
    """

    def __init__(
        self,
        topics:         MQTTTopics,
        publisher:      MQTTPublisher,
        validator:      MessageValidator,
        command_engine,                   # CommandEngine (avoid circular import)
    ) -> None:
        self._topics  = topics
        self._pub     = publisher
        self._val     = validator
        self._engine  = command_engine

    # ------------------------------------------------------------------
    # Entry points — called by MQTTBridge on message receipt
    # ------------------------------------------------------------------

    def handle_command(self, payload: str) -> None:
        """Handle printer/{id}/command message."""
        try:
            cmd = self._val.parse_command(payload)
        except ValidationError as exc:
            logger.warning(f"[Router] Rejected command: {exc}")
            return

        logger.info(f"[Router] Accepted command: {cmd.commandName} -> {cmd.gcode!r}")
        threading.Thread(
            target=self._execute_command,
            args=(cmd,),
            daemon=True,
        ).start()

    def handle_start_job(self, payload: str) -> None:
        """Handle printer/{id}/start-job message."""
        try:
            job = self._val.parse_start_job(payload)
        except ValidationError as exc:
            logger.warning(f"[Router] Rejected start-job: {exc}")
            return

        # Sprint 4 scope: acknowledge receipt; full job execution is Sprint 5
        logger.info(f"[Router] start-job received: jobId={job.jobId!r} url={job.fileUrl!r}")

    # ------------------------------------------------------------------
    # Command execution pipeline (runs in its own thread per command)
    # ------------------------------------------------------------------

    def _execute_command(self, cmd: CommandMessage) -> None:
        printer_id   = cmd.printerId
        command_name = cmd.commandName
        gcode        = cmd.gcode

        # Stage 1: QUEUED
        self._pub.command_response(CommandResponseMessage(
            printerId=printer_id,
            commandName=command_name,
            gcode=gcode,
            status="QUEUED",
        ))

        # Stage 2: EXECUTING
        self._pub.command_response(CommandResponseMessage(
            printerId=printer_id,
            commandName=command_name,
            gcode=gcode,
            status="EXECUTING",
        ))

        # Stage 3: Send through CommandEngine and wait for result
        try:
            result = self._engine.send(gcode)

            if result.succeeded:
                self._pub.command_response(CommandResponseMessage(
                    printerId=printer_id,
                    commandName=command_name,
                    gcode=gcode,
                    status="SUCCESS",
                ))
                logger.info(f"[Router] SUCCESS: {command_name} in {result.elapsed_ms:.1f}ms")
            else:
                reason = f"Engine status: {result.status.name}"
                self._pub.command_response(CommandResponseMessage(
                    printerId=printer_id,
                    commandName=command_name,
                    gcode=gcode,
                    status="ERROR",
                    reason=reason,
                ))
                logger.warning(f"[Router] FAILED: {command_name} — {reason}")

        except Exception as exc:
            reason = str(exc)
            self._pub.command_response(CommandResponseMessage(
                printerId=printer_id,
                commandName=command_name,
                gcode=gcode,
                status="ERROR",
                reason=reason,
            ))
            logger.exception(f"[Router] Exception executing {command_name}: {exc}")