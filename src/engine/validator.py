"""
validator.py — Layer 2: Command Queue Engine
Validates G-code commands before they enter the queue.

Rules enforced:
  - Command must be a non-empty string
  - Command must start with a recognized G/M/T prefix
  - System is in a state that accepts new commands

Validation is intentionally strict: unknown prefixes are rejected.
This prevents the printer from receiving garbage during edge cases.
"""

import re
import logging

logger = logging.getLogger(__name__)

# ── Allowed G-code prefixes ───────────────────────────────────────────────
# Covers all standard Marlin commands used by Creality printers.
# Extend this list as new commands are needed in later sprints.

ALLOWED_PREFIXES = (
    "G",    # Motion commands  (G0, G1, G28, G29, G90, G91, G92 …)
    "M",    # Machine commands (M104, M105, M109, M114, M115, M140 …)
    "T",    # Tool select      (T0, T1)
    ";"  ,  # Comment line     (some slicers emit these; safe to pass)
)

# A minimal pattern: one allowed prefix letter, then digits, optional params
GCODE_PATTERN = re.compile(
    r"^[GMT;]\d*(\s.*)?$",
    re.IGNORECASE,
)

MAX_COMMAND_LENGTH = 256    # Sane upper bound; Marlin buffers are small


class ValidationError(Exception):
    """Raised when a command fails validation and must be rejected."""


def validate_command(gcode: str) -> str:
    """
    Validate and normalise a single G-code command string.

    Args:
        gcode: Raw G-code string from any caller.

    Returns:
        str: Cleaned, upper-cased command ready for queuing.

    Raises:
        ValidationError: With a human-readable reason on failure.
    """
    if not isinstance(gcode, str):
        raise ValidationError(
            f"Command must be a string, got {type(gcode).__name__!r}."
        )

    cleaned = gcode.strip()

    if not cleaned:
        raise ValidationError("Empty command string rejected.")

    if len(cleaned) > MAX_COMMAND_LENGTH:
        raise ValidationError(
            f"Command too long ({len(cleaned)} chars, max {MAX_COMMAND_LENGTH})."
        )

    upper = cleaned.upper()

    if not upper.startswith(ALLOWED_PREFIXES):
        raise ValidationError(
            f"Unrecognised G-code prefix in: {cleaned!r}. "
            f"Allowed prefixes: {ALLOWED_PREFIXES}"
        )

    if not GCODE_PATTERN.match(upper):
        raise ValidationError(
            f"Malformed G-code syntax: {cleaned!r}"
        )

    logger.debug(f"[Validator] ✓ {cleaned!r}")
    return cleaned


def validate_batch(commands: list[str]) -> tuple[list[str], list[str]]:
    """
    Validate a list of G-code commands.

    Args:
        commands: Raw list of G-code strings.

    Returns:
        tuple:
          - valid_commands: List of cleaned commands that passed.
          - errors: List of error strings for rejected commands.
    """
    valid   = []
    errors  = []

    for raw in commands:
        try:
            valid.append(validate_command(raw))
        except ValidationError as exc:
            errors.append(str(exc))
            logger.warning(f"[Validator] ✗ {raw!r} → {exc}")

    logger.info(
        f"[Validator] Batch validated: {len(valid)} accepted, "
        f"{len(errors)} rejected."
    )
    return valid, errors