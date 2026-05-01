"""
telemetry_parser.py — Shared Utility
Parses Marlin firmware response lines for temperature and position data.

Used by both:
  - PrinterCommunicator (Sprint 1) — inline parsing during command responses
  - TelemetryEngine (Sprint 3)    — continuous background parsing

All functions are pure (no side effects) and return None on parse failure.
"""

import re
import logging

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# Regex patterns for Marlin firmware output
# -----------------------------------------------------------------------

# Temperature line examples:
#   ok T:210.5 /210.0 B:60.3 /60.0 T0:210.5 /210.0 @:127 B@:0
#   T:25.1 /0.0 B:24.0 /0.0
TEMP_PATTERN = re.compile(
    r"T:(?P<nozzle>[\d.]+)"      # Nozzle current temp
)

BED_PATTERN = re.compile(
    r"\bB:(?P<bed>[\d.]+)"       # Bed current temp (standalone B:, not B@:)
)

# Position line examples:
#   X:0.00 Y:0.00 Z:0.00 E:0.00 Count X:0 Y:0 Z:0
POSITION_PATTERN = re.compile(
    r"X:(?P<x>-?[\d.]+)\s+"
    r"Y:(?P<y>-?[\d.]+)\s+"
    r"Z:(?P<z>-?[\d.]+)"
)


def parse_temperature_line(line: str) -> dict | None:
    """
    Extract nozzle and bed temperatures from a Marlin response line.

    Args:
        line: Raw response line from the printer.

    Returns:
        dict with keys 'nozzle' and/or 'bed' as floats, or None if not found.

    Examples:
        >>> parse_temperature_line("T:210.5 /210.0 B:60.3 /60.0")
        {'nozzle': 210.5, 'bed': 60.3}
        >>> parse_temperature_line("ok")
        None
    """
    if "T:" not in line:
        return None

    nozzle_match = TEMP_PATTERN.search(line)
    if not nozzle_match:
        return None

    result = {}
    try:
        result["nozzle"] = float(nozzle_match.group("nozzle"))

        bed_match = BED_PATTERN.search(line)
        if bed_match:
            result["bed"] = float(bed_match.group("bed"))

    except (ValueError, TypeError) as exc:
        logger.debug(f"[Parser] Temperature parse error on '{line}': {exc}")
        return None

    return result if result else None


def parse_position_line(line: str) -> dict | None:
    """
    Extract X/Y/Z position from a Marlin M114 response line.

    Args:
        line: Raw response line from the printer.

    Returns:
        dict with keys 'x', 'y', 'z' as floats, or None if not found.

    Examples:
        >>> parse_position_line("X:0.00 Y:0.00 Z:5.20 E:0.00 Count X:0 Y:0 Z:0")
        {'x': 0.0, 'y': 0.0, 'z': 5.2}
    """
    if "X:" not in line:
        return None

    match = POSITION_PATTERN.search(line)
    if not match:
        return None

    try:
        return {
            "x": float(match.group("x")),
            "y": float(match.group("y")),
            "z": float(match.group("z")),
        }
    except (ValueError, TypeError) as exc:
        logger.debug(f"[Parser] Position parse error on '{line}': {exc}")
        return None


def is_ok_response(line: str) -> bool:
    """
    Check if a line contains an "ok" acknowledgment from the printer.

    Detection is case-insensitive and matches anywhere in the line,
    so both "ok" and "ok T:210.5 ..." are valid.

    Args:
        line: Raw response line.

    Returns:
        bool: True if "ok" is present.
    """
    return "ok" in line.lower()


def is_error_response(line: str) -> bool:
    """
    Check if a line indicates a printer error.

    Args:
        line: Raw response line.

    Returns:
        bool: True if the line is an error message.
    """
    lower = line.lower()
    return lower.startswith("error") or "!! " in lower