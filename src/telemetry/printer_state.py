"""
printer_state.py — Central State Model

Defines the immutable snapshot and the mutable internal state
for the Reactive Edge Hub telemetry layer.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum, auto
from typing import Optional


class PrinterStatus(Enum):
    UNKNOWN  = auto()
    IDLE     = auto()
    PRINTING = auto()
    PAUSED   = auto()
    ERROR    = auto()
    REBOOTING = auto()


@dataclass
class PrinterStateSnapshot:
    """
    Read-only snapshot of printer state.
    Handed out by StateManager.get_snapshot().
    Callers must never mutate this object.
    """
    status:           PrinterStatus        = PrinterStatus.UNKNOWN
    nozzle_temp:      Optional[float]      = None
    nozzle_target:    Optional[float]      = None
    bed_temp:         Optional[float]      = None
    bed_target:       Optional[float]      = None
    progress_pct:     float                = 0.0
    position_x:       Optional[float]      = None
    position_y:       Optional[float]      = None
    position_z:       Optional[float]      = None
    last_updated:     Optional[datetime]   = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.name
        d["last_updated"] = (
            self.last_updated.isoformat() if self.last_updated else None
        )
        return d