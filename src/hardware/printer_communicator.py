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

import logging
import time

from src.hardware.serial_connection import SerialConnection, SerialConnectionError
from src.utils.telemetry_parser import parse_temperature_line, parse_position_line

logger = logging.getLogger(__name__)

# Protocol timing
COMMAND_TIMEOUT_SEC = 5.0    # Max wait for "ok" per attempt
MAX_RETRIES         = 2      # Retry count before marking unresponsive
RETRY_DELAY_SEC     = 1.0    # Pause between retries


class PrinterUnresponsiveError(Exception):
    """Raised when the printer fails to acknowledge a command after retries."""


class PrinterCommunicator:
    """
    High-level communication interface for the Creality printer.

    Upper layers (Command Engine, Telemetry) call this class.
    Direct serial port access is hidden inside SerialConnection.
    """

    def __init__(self, connection: SerialConnection):
        self._conn = connection
        self._last_temperatures: dict = {}
        self._last_position: dict = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_command(self, command: str) -> list[str]:
        """
        Send a G-code command and wait for "ok" acknowledgment.

        Retries up to MAX_RETRIES times on timeout.

        Args:
            command: G-code string (e.g. "M105", "G28").

        Returns:
            list[str]: All response lines received before "ok".

        Raises:
            PrinterUnresponsiveError: If "ok" never received after retries.
            SerialConnectionError: If the serial port is not connected.
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

            # Timeout on this attempt
            logger.warning(
                f"[Communicator] Timeout waiting for 'ok' "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SEC)

        raise PrinterUnresponsiveError(
            f"Printer did not acknowledge '{command}' after "
            f"{MAX_RETRIES} attempts."
        )

    def send_sequence(self, commands: list[str]) -> dict[str, list[str]]:
        """
        Execute a list of G-code commands sequentially.

        Each command waits for "ok" before the next is sent.

        Args:
            commands: Ordered list of G-code strings.

        Returns:
            dict: Mapping of command → response lines.
        """
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
        """Most recently parsed temperature readings."""
        return dict(self._last_temperatures)

    @property
    def last_position(self) -> dict:
        """Most recently parsed position readings."""
        return dict(self._last_position)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_until_ok(self) -> list[str] | None:
        """
        Read lines from serial until "ok" is received or timeout.

        Side-effect: updates internal temperature / position state
        from any telemetry lines encountered along the way.

        Returns:
            list[str]: Response lines collected before "ok".
            None: if timeout expired without receiving "ok".
        """
        responses: list[str] = []
        deadline = time.monotonic() + COMMAND_TIMEOUT_SEC

        while time.monotonic() < deadline:
            line = self._conn.read_line()

            if line is None:
                # readline() timed out at the serial level; keep trying
                continue

            logger.debug(f"[Communicator] << {line}")
            responses.append(line)

            # Parse any telemetry embedded in the response stream
            self._try_parse_telemetry(line)

            # "ok" detection — case-insensitive, anywhere in the line
            if "ok" in line.lower():
                logger.debug("[Communicator] 'ok' detected.")
                return responses

        return None   # Timed out

    def _try_parse_telemetry(self, line: str) -> None:
        """
        Attempt to extract temperature or position data from a response line.
        Updates internal state caches without blocking or raising.
        """
        temp = parse_temperature_line(line)
        if temp:
            self._last_temperatures.update(temp)
            logger.debug(f"[Communicator] Temp parsed: {temp}")

        pos = parse_position_line(line)
        if pos:
            self._last_position.update(pos)
            logger.debug(f"[Communicator] Position parsed: {pos}")