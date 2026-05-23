"""
message_validator.py — Incoming Message Validation Layer

All downstream messages (Cloud -> Pi) are validated here before
anything else processes them.

Rules:
  - Payload must be valid JSON
  - Required fields must be present
  - printerId must match this device
  - gcode must be on the command whitelist (for CommandMessage)
  - Unknown or malformed messages are rejected with a logged reason
"""

import json
import logging
from typing import Optional, Tuple

from config.settings import (
    MQTT_ALLOWED_GCODE_PREFIXES,
    MQTT_REQUIRED_COMMAND_FIELDS,
    MQTT_REQUIRED_START_JOB_FIELDS,
)
from src.core.models import CommandMessage, StartJobMessage

logger = logging.getLogger(__name__)

# G-code prefix whitelist — extend as your use case grows
_ALLOWED_GCODE_PREFIXES = MQTT_ALLOWED_GCODE_PREFIXES

_REQUIRED_COMMAND_FIELDS = MQTT_REQUIRED_COMMAND_FIELDS
_REQUIRED_START_JOB_FIELDS = MQTT_REQUIRED_START_JOB_FIELDS


class ValidationError(Exception):
    pass


class MessageValidator:
    """
    Validates incoming MQTT payloads before they touch the command queue.

    Usage:
        validator = MessageValidator(printer_id="printer-001")
        cmd = validator.parse_command(raw_payload)   # raises ValidationError on failure
        job = validator.parse_start_job(raw_payload)
    """

    def __init__(self, printer_id: str) -> None:
        self._printer_id = printer_id

    # ------------------------------------------------------------------
    # Public parse methods
    # ------------------------------------------------------------------

    def parse_command(self, raw: str) -> CommandMessage:
        """Parse and validate a CommandMessage payload."""
        data = self._parse_json(raw)
        self._require_fields(data, _REQUIRED_COMMAND_FIELDS, "CommandMessage")
        self._verify_printer_id(data)
        self._verify_gcode(data["gcode"])
        return CommandMessage(
            printerId=data["printerId"],
            commandName=data["commandName"],
            gcode=data["gcode"],
            commandLogId=data["commandLogId"],
            reason=data.get("reason"),
        )

    def parse_start_job(self, raw: str) -> StartJobMessage:
        """Parse and validate a StartJobMessage payload."""
        data = self._parse_json(raw)
        self._require_fields(data, _REQUIRED_START_JOB_FIELDS, "StartJobMessage")
        self._verify_printer_id(data)
        return StartJobMessage(
            printerId=data["printerId"],
            commandName=data["commandName"],
            jobId=data["jobId"],
            fileUrl=data["fileUrl"],
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_json(self, raw: str) -> dict:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"Invalid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ValidationError("Payload must be a JSON object.")
        return data

    def _require_fields(self, data: dict, fields: set, model_name: str) -> None:
        missing = fields - data.keys()
        if missing:
            raise ValidationError(f"{model_name} missing fields: {missing}")

    def _verify_printer_id(self, data: dict) -> None:
        pid = data.get("printerId")
        if pid != self._printer_id:
            raise ValidationError(
                f"printerId mismatch: got {pid!r}, expected {self._printer_id!r}"
            )

    def _verify_gcode(self, gcode: str) -> None:
        if not gcode or not isinstance(gcode, str):
            raise ValidationError("gcode field is empty or not a string.")
        cmd = gcode.strip().upper()
        if not any(cmd.startswith(prefix) for prefix in _ALLOWED_GCODE_PREFIXES):
            raise ValidationError(
                f"G-code not on whitelist: {gcode!r}. "
                f"Allowed prefixes: {_ALLOWED_GCODE_PREFIXES}"
            )
