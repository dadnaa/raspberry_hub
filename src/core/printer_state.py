"""Domain printer state model used by MQTT and printer gateways."""

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum, auto
from typing import Optional


class PrinterStatus(Enum):
    UNKNOWN = auto()
    IDLE = auto()
    PRINTING = auto()
    PAUSED = auto()
    ERROR = auto()
    REBOOTING = auto()


@dataclass
class PrinterStateSnapshot:
    status: PrinterStatus = PrinterStatus.UNKNOWN
    nozzle_temp: Optional[float] = None
    nozzle_target: Optional[float] = None
    bed_temp: Optional[float] = None
    bed_target: Optional[float] = None
    progress_pct: float = 0.0
    position_x: Optional[float] = None
    position_y: Optional[float] = None
    position_z: Optional[float] = None
    last_updated: Optional[datetime] = None

    def to_dict(self) -> dict:
        data = asdict(self)
        data["status"] = self.status.name
        data["last_updated"] = (
            self.last_updated.isoformat() if self.last_updated else None
        )
        return data
