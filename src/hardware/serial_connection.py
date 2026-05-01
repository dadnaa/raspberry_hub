"""
serial_connection.py — Layer 1: Hardware Interface
Manages the physical USB serial connection to the Creality printer.

Responsibilities:
- Open / close the serial port
- Write raw bytes to the printer
- Read raw lines from the printer
- Detect disconnection and trigger reconnect
- Never expose serial port to upper layers directly
"""

import logging
import time
import serial
import serial.serialutil

from src.hardware.port_discovery import get_printer_port, CREALITY_BAUD_RATES

logger = logging.getLogger(__name__)

# Timing constants
STARTUP_STABILIZATION_DELAY = 3.0   # seconds — wait for printer reset after connect
READ_TIMEOUT_SEC             = 5.0   # seconds — serial read line timeout
RECONNECT_DELAY_SEC          = 5.0   # seconds — pause before reconnect attempt
MAX_RECONNECT_ATTEMPTS       = 5


class SerialConnectionError(Exception):
    """Raised when a serial connection cannot be established or is lost."""


class SerialConnection:
    """
    Low-level serial connection wrapper for the Creality printer.

    This class is the ONLY place in the system allowed to touch
    the pyserial port object directly.
    """

    def __init__(self):
        self._port: serial.Serial | None = None
        self._port_path: str | None = None
        self._baud_rate: int | None = None
        self._connected: bool = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected and self._port is not None and self._port.is_open

    @property
    def port_path(self) -> str | None:
        return self._port_path

    @property
    def baud_rate(self) -> int | None:
        return self._baud_rate

    def connect(self) -> bool:
        """
        Discover port and attempt connection at each supported baud rate.

        Returns:
            bool: True if connection succeeded.

        Raises:
            SerialConnectionError: If no port is found or all baud rates fail.
        """
        port_path, all_ports = get_printer_port()

        if not port_path:
            raise SerialConnectionError(
                "No serial port detected. Ensure the USB cable is connected "
                "and the printer is powered on."
            )

        for baud in CREALITY_BAUD_RATES:
            logger.info(f"[Serial] Trying {port_path} @ {baud} baud …")
            try:
                self._port = serial.Serial(
                    port=port_path,
                    baudrate=baud,
                    timeout=READ_TIMEOUT_SEC,
                    write_timeout=2.0,
                )
                self._port_path = port_path
                self._baud_rate = baud
                self._connected = True

                logger.info(
                    f"[Serial] Connected → {port_path} @ {baud} baud"
                )
                self._stabilize_after_connect()
                return True

            except serial.SerialException as exc:
                logger.warning(f"[Serial] Failed @ {baud} baud: {exc}")
                self._port = None

        raise SerialConnectionError(
            f"Could not connect to {port_path} at any supported baud rate: "
            f"{CREALITY_BAUD_RATES}"
        )

    def disconnect(self) -> None:
        """Safely close the serial port."""
        if self._port and self._port.is_open:
            try:
                self._port.close()
                logger.info("[Serial] Port closed.")
            except Exception as exc:
                logger.error(f"[Serial] Error closing port: {exc}")
        self._connected = False
        self._port = None

    def reconnect(self) -> bool:
        """
        Attempt to reconnect after a disconnection event.

        Returns:
            bool: True if reconnect succeeded within max attempts.
        """
        logger.warning("[Serial] Attempting reconnect …")
        self.disconnect()

        for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
            logger.info(f"[Serial] Reconnect attempt {attempt}/{MAX_RECONNECT_ATTEMPTS}")
            time.sleep(RECONNECT_DELAY_SEC)
            try:
                return self.connect()
            except SerialConnectionError as exc:
                logger.warning(f"[Serial] Reconnect failed: {exc}")

        logger.error("[Serial] All reconnect attempts exhausted.")
        return False

    def write_line(self, command: str) -> bool:
        """
        Write a single G-code command to the printer.

        The command is automatically terminated with \\n and flushed.

        Args:
            command: G-code string (e.g. "M105").

        Returns:
            bool: True if write succeeded.
        """
        if not self.is_connected:
            logger.error("[Serial] Write attempted on closed port.")
            return False

        line = command.strip() + "\n"
        try:
            self._port.write(line.encode("ascii", errors="replace"))
            self._port.flush()
            logger.debug(f"[Serial] >> {command.strip()}")
            return True

        except serial.SerialException as exc:
            logger.error(f"[Serial] Write error: {exc}")
            self._connected = False
            return False

    def read_line(self) -> str | None:
        """
        Read one line from the serial port.

        Cleans non-ASCII bytes before returning.

        Returns:
            str | None: Decoded line (stripped), or None on error/timeout.
        """
        if not self.is_connected:
            return None

        try:
            raw = self._port.readline()
            if not raw:
                return None  # Timeout (no data within READ_TIMEOUT_SEC)

            # Decode, replacing any garbage bytes
            line = raw.decode("ascii", errors="replace").strip()
            return line if line else None

        except serial.SerialException as exc:
            logger.error(f"[Serial] Read error: {exc}")
            self._connected = False
            return None

    def flush_buffers(self) -> None:
        """Clear input and output buffers (used after connect / on error)."""
        if self._port and self._port.is_open:
            try:
                self._port.reset_input_buffer()
                self._port.reset_output_buffer()
                logger.debug("[Serial] Buffers flushed.")
            except serial.SerialException as exc:
                logger.warning(f"[Serial] Buffer flush failed: {exc}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stabilize_after_connect(self) -> None:
        """
        Wait for the printer to finish its reset/boot sequence.

        During this window we flush buffers and drain startup noise
        so upper layers never see firmware boot messages.
        """
        logger.info(
            f"[Serial] Stabilizing … ({STARTUP_STABILIZATION_DELAY}s)"
        )
        time.sleep(STARTUP_STABILIZATION_DELAY)
        self.flush_buffers()

        # Drain any remaining startup lines
        drained = 0
        self._port.timeout = 0.2          # Short timeout for drain loop
        while True:
            raw = self._port.readline()
            if not raw:
                break
            drained += 1
            logger.debug(
                f"[Serial] Boot noise discarded: "
                f"{raw.decode('ascii', errors='replace').strip()}"
            )

        self._port.timeout = READ_TIMEOUT_SEC   # Restore normal timeout
        logger.info(
            f"[Serial] Stabilization complete. "
            f"Discarded {drained} boot line(s)."
        )