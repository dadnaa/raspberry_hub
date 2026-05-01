"""
telemetry_event.py — Structured Telemetry Event

Defines the event envelope published on every state change.
Sprint 4 will forward these to HiveMQ/MQTT.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict

from src.telemetry.printer_state import PrinterStateSnapshot


@dataclass
class TelemetryEvent:
    """
    Immutable event fired whenever printer state changes.

    Fields:
        timestamp     – UTC ISO-8601 string
        changed_fields – dict of only the fields that changed this tick
        snapshot      – full state at the moment of the event
    """
    timestamp:      str
    changed_fields: Dict[str, Any]
    snapshot:       PrinterStateSnapshot

    @staticmethod
    def build(
        snapshot: PrinterStateSnapshot,
        changed_fields: Dict[str, Any],
    ) -> "TelemetryEvent":
        return TelemetryEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            changed_fields=changed_fields,
            snapshot=snapshot,
        )

    def to_dict(self) -> dict:
        d = {
            "timestamp":      self.timestamp,
            "changed_fields": {
                k: (v.name if hasattr(v, "name") else v)
                for k, v in self.changed_fields.items()
            },
            "snapshot": self.snapshot.to_dict(),
        }
        return d