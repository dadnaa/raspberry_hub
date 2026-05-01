"""
telemetry_parser.py — Serial Line Parsers

Stateless pure functions that extract structured data
from raw printer serial output lines.
"""

import re
from dataclasses import dataclass
from typing import Optional


# ── Compiled patterns ────────────────────────────────────────────────

# Temperature: "T:205.3 /210.0 B:60.1 /60.0"
# or           "ok T:205.3 B:60.1"
_RE_TEMP = re.compile(
    r"T:(?P<nozzle>[\d.]+)"
    r"(?:\s*/(?P<nozzle_target>[\d.]+))?"
    r".*?"
    r"(?:B:(?P<bed>[\d.]+)"
    r"(?:\s*/(?P<bed_target>[\d.]+))?)?",
    re.IGNORECASE,
)

# Position: "X:10.00 Y:20.50 Z:5.00 E:0.00"
_RE_POS = re.compile(
    r"X:(?P<x>[-\d.]+)"
    r".*?Y:(?P<y>[-\d.]+)"
    r".*?Z:(?P<z>[-\d.]+)",
    re.IGNORECASE,
)

# SD progress: "SD printing byte 1234/56789"  or  "Done printing file"
_RE_SD_PROGRESS = re.compile(
    r"SD printing byte\s+(?P<done>\d+)/(?P<total>\d+)",
    re.IGNORECASE,
)
_RE_SD_DONE = re.compile(r"Done printing file", re.IGNORECASE)

# Pause
_RE_PAUSE = re.compile(r"// action:pause|M25", re.IGNORECASE)

# Resume
_RE_RESUME = re.compile(r"// action:resume|M24", re.IGNORECASE)

# Reboot/start markers
_RE_START = re.compile(r"^start$|Marlin|FIRMWARE_INFO", re.IGNORECASE)


# ── Result dataclasses ────────────────────────────────────────────────

@dataclass
class TemperatureReading:
    nozzle:        Optional[float] = None
    nozzle_target: Optional[float] = None
    bed:           Optional[float] = None
    bed_target:    Optional[float] = None


@dataclass
class PositionReading:
    x: float
    y: float
    z: float


@dataclass
class ProgressReading:
    done:  int
    total: int

    @property
    def percent(self) -> float:
        if self.total <= 0:
            return 0.0
        return round(min(100.0, (self.done / self.total) * 100), 1)


# ── Parser functions ──────────────────────────────────────────────────

def parse_temperature(line: str) -> Optional[TemperatureReading]:
    """Extract nozzle and bed temperatures from a serial line."""
    m = _RE_TEMP.search(line)
    if not m:
        return None
    nozzle = m.group("nozzle")
    if nozzle is None:
        return None
    return TemperatureReading(
        nozzle        = float(nozzle),
        nozzle_target = float(m.group("nozzle_target")) if m.group("nozzle_target") else None,
        bed           = float(m.group("bed"))           if m.group("bed")           else None,
        bed_target    = float(m.group("bed_target"))    if m.group("bed_target")    else None,
    )


def parse_position(line: str) -> Optional[PositionReading]:
    """Extract X/Y/Z head position from a serial line."""
    m = _RE_POS.search(line)
    if not m:
        return None
    return PositionReading(
        x=float(m.group("x")),
        y=float(m.group("y")),
        z=float(m.group("z")),
    )


def parse_sd_progress(line: str) -> Optional[ProgressReading]:
    """Extract SD card print progress bytes from a serial line."""
    m = _RE_SD_PROGRESS.search(line)
    if m:
        return ProgressReading(done=int(m.group("done")), total=int(m.group("total")))
    return None


def is_sd_done(line: str) -> bool:
    return bool(_RE_SD_DONE.search(line))


def is_paused(line: str) -> bool:
    return bool(_RE_PAUSE.search(line))


def is_resumed(line: str) -> bool:
    return bool(_RE_RESUME.search(line))


def is_reboot(line: str) -> bool:
    return bool(_RE_START.search(line))