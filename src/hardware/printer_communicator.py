"""
printer_communicator.py — Layer 1: Hardware Interface
Implements the strict request → response → "ok" communication protocol.

This module is the gateway between upper layers and raw serial.
It enforces:
  - One command at a time
  - "ok" synchronization before next command
  - Timeout + retry on no response
  - Telemetry parsing from incoming lines
  - Structured logging of every event
"""
"""
printer_communicator.py — Fixed: reads from ack_queue instead of read_line()

CHANGE FROM SPRINT 2
────────────────────
Old _read_until_ok():
    while deadline not reached:
        line = self._conn.read_line()   ← called serial directly, raced with telemetry

New _read_until_ok():
    while deadline not reached:
        line = self._ack_queue.get(timeout=...)  ← reads from SerialRouter.ack_queue

Every other method is IDENTICAL to Sprint 2.
The constructor now accepts an optional ack_queue parameter.
If not provided (backwards-compat), falls back to direct read_line() with a warning.
"""

import logging
import queue
import time
from typing import Optional

from config.settings import (
    PRINTER_ACK_POLL_SEC,
    PRINTER_COMMAND_MAX_RETRIES,
    PRINTER_COMMAND_RETRY_DELAY_SEC,
    PRINTER_COMMAND_TIMEOUT_SEC,
)
from src.hardware.serial_connection import SerialConnection, SerialConnectionError
from src.utils.telemetry_parser import parse_temperature_line, parse_position_line

logger = logging.getLogger(__name__)

COMMAND_TIMEOUT_SEC = PRINTER_COMMAND_TIMEOUT_SEC
MAX_RETRIES = PRINTER_COMMAND_MAX_RETRIES
RETRY_DELAY_SEC = PRINTER_COMMAND_RETRY_DELAY_SEC
_ACK_POLL_SEC = PRINTER_ACK_POLL_SEC


class PrinterUnresponsiveError(Exception):
    """Raised when the printer fails to acknowledge a command after retries."""


class PrinterCommunicator:
    """
    High-level communication interface for the Creality printer.

    Args:
        connection: SerialConnection instance.
        ack_queue:  queue.Queue fed by SerialRouter (the correct way).
                    If None, falls back to direct read_line() (Sprint 2 compat,
                    but will race with TelemetryEngine — use only in tests).
    """

    def __init__(
        self,
        connection: SerialConnection,
        ack_queue:  Optional[queue.Queue] = None,
    ) -> None:
        self._conn      = connection
        self._ack_queue = ack_queue
        self._last_temperatures: dict = {}
        self._last_position:     dict = {}

        if ack_queue is None:
            logger.warning(
                "[Communicator] No ack_queue provided — falling back to direct "
                "read_line(). This WILL race with TelemetryEngine. "
                "Pass SerialRouter.ack_queue to fix this."
            )

    # ------------------------------------------------------------------
    # Public API (unchanged)
    # ------------------------------------------------------------------

    def send_command(self, command: str) -> list[str]:
        """
        Send a G-code command and wait for 'ok' acknowledgment.

        Retries up to MAX_RETRIES times on timeout.

        Args:
            command: G-code string (e.g. "M105", "G28").

        Returns:
            list[str]: All response lines received before "ok".

        Raises:
            PrinterUnresponsiveError: If "ok" never received after retries.
            SerialConnectionError:    If the serial port is not connected.
        """
        if not self._conn.is_connected:
            raise SerialConnectionError("Serial port is not connected.")

        for attempt in range(1, MAX_RETRIES + 1):
            logger.info(f"[Communicator] >> {command}  (attempt {attempt})")

            success = self._conn.write_line(command)
            if not success:
                logger.warning(f"[Communicator] Write failed for: {command}")
                continue

            responses = self._read_until_ok()

            if responses is not None:
                logger.info(
                    f"[Communicator] 'ok' received for: {command} "
                    f"({len(responses)} response line(s))"
                )
                return responses

            logger.warning(
                f"[Communicator] Timeout waiting for 'ok' "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SEC)

        raise PrinterUnresponsiveError(
            f"Printer did not acknowledge '{command}' after {MAX_RETRIES} attempts."
        )

    def send_sequence(self, commands: list[str]) -> dict[str, list[str]]:
        """Execute a list of G-code commands sequentially."""
        results = {}
        for cmd in commands:
            try:
                responses = self.send_command(cmd)
                results[cmd] = responses
            except PrinterUnresponsiveError as exc:
                logger.error(f"[Communicator] Sequence aborted: {exc}")
                results[cmd] = []
                break
        return results

    @property
    def last_temperatures(self) -> dict:
        return dict(self._last_temperatures)

    @property
    def last_position(self) -> dict:
        return dict(self._last_position)

    # ------------------------------------------------------------------
    # Internal — THE KEY CHANGE
    # ------------------------------------------------------------------

    def _read_until_ok(self) -> list[str] | None:
        """
        Collect response lines until 'ok' is received or timeout.

        Reads from ack_queue (fed by SerialRouter) instead of calling
        read_line() directly. This eliminates the race with TelemetryEngine.

        Returns:
            list[str] of response lines on success, None on timeout.
        """
        responses: list[str] = []
        deadline = time.monotonic() + COMMAND_TIMEOUT_SEC

        while time.monotonic() < deadline:

            # ── Queue-based read (normal operation) ───────────────────
            if self._ack_queue is not None:
                try:
                    line = self._ack_queue.get(timeout=_ACK_POLL_SEC)
                except queue.Empty:
                    continue   # no line yet, keep waiting until deadline

            # ── Direct read (fallback / legacy) ───────────────────────
            else:
                line = self._conn.read_line()
                if line is None:
                    continue

            if not line:
                continue

            logger.debug(f"[Communicator] << {line}")
            responses.append(line)
            self._try_parse_telemetry(line)

            if "ok" in line.lower():
                logger.debug("[Communicator] 'ok' detected.")
                return responses

        return None   # Timed out

    def _try_parse_telemetry(self, line: str) -> None:
        """Parse temperature/position side-data from response lines."""
        temp = parse_temperature_line(line)
        if temp:
            self._last_temperatures.update(temp)

        pos = parse_position_line(line)
        if pos:
            self._last_position.update(pos)
