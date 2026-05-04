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
"""
serial_connection.py — Fixed: Added reconnect hook for SerialRouter

CHANGES FROM SPRINT 2
─────────────────────
1. Added `on_reconnect` callback parameter to __init__
   SerialRouter registers itself here so it can flush its queues
   and restart its read loop after a reconnect event.

2. reconnect() now calls on_reconnect() if registered.

3. Everything else is IDENTICAL to Sprint 2.

WHY
───
When the serial port drops and reconnects, the OS serial buffer is fresh.
Any lines still sitting in SerialRouter's queues are from the old connection
and could cause:
  - a stale "ok" being matched to the wrong command
  - stale telemetry updating state with old data

The on_reconnect hook lets SerialRouter flush both queues atomically
before the new connection starts feeding data.
"""

import logging
import time
from typing import Callable, Optional

import serial
import serial.serialutil

from config.settings import (
    SERIAL_MAX_RECONNECT_ATTEMPTS,
    SERIAL_READ_TIMEOUT_SEC,
    SERIAL_RECONNECT_DELAY_SEC,
    SERIAL_STARTUP_STABILIZATION_DELAY_SEC,
)
from src.hardware.port_discovery import get_printer_port, CREALITY_BAUD_RATES

logger = logging.getLogger(__name__)

STARTUP_STABILIZATION_DELAY = SERIAL_STARTUP_STABILIZATION_DELAY_SEC
READ_TIMEOUT_SEC = SERIAL_READ_TIMEOUT_SEC
RECONNECT_DELAY_SEC = SERIAL_RECONNECT_DELAY_SEC
MAX_RECONNECT_ATTEMPTS = SERIAL_MAX_RECONNECT_ATTEMPTS


class SerialConnectionError(Exception):
    """Raised when a serial connection cannot be established or is lost."""


class SerialConnection:
    """
    Low-level serial connection wrapper for the Creality printer.

    Args:
        on_reconnect: Optional callback invoked after a successful reconnect.
                      SerialRouter uses this to flush stale queue data.
    """

    def __init__(
        self,
        on_reconnect: Optional[Callable[[], None]] = None,
    ) -> None:
        self._port:         serial.Serial | None = None
        self._port_path:    str | None = None
        self._baud_rate:    int | None = None
        self._connected:    bool = False
        self._on_reconnect: Optional[Callable[[], None]] = on_reconnect

    # ------------------------------------------------------------------
    # Public API (unchanged except reconnect())
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
                logger.info(f"[Serial] Connected → {port_path} @ {baud} baud")
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

        After a successful reconnect, calls on_reconnect() if registered
        so SerialRouter can flush its stale queues before new data arrives.
        """
        logger.warning("[Serial] Attempting reconnect …")
        self.disconnect()

        for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
            logger.info(f"[Serial] Reconnect attempt {attempt}/{MAX_RECONNECT_ATTEMPTS}")
            time.sleep(RECONNECT_DELAY_SEC)
            try:
                result = self.connect()
                if result and self._on_reconnect:
                    # Flush stale queue data before new serial data arrives
                    try:
                        self._on_reconnect()
                    except Exception:
                        logger.exception("[Serial] on_reconnect callback failed.")
                return result
            except SerialConnectionError as exc:
                logger.warning(f"[Serial] Reconnect failed: {exc}")

        logger.error("[Serial] All reconnect attempts exhausted.")
        return False

    def write_line(self, command: str) -> bool:
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

        NOTE: After SerialRouter is wired in, this should ONLY be called
        by SerialRouter._read_loop(). No other code should call this.
        """
        if not self.is_connected:
            return None
        try:
            raw = self._port.readline()
            if not raw:
                return None
            line = raw.decode("ascii", errors="replace").strip()
            return line if line else None
        except serial.SerialException as exc:
            logger.error(f"[Serial] Read error: {exc}")
            self._connected = False
            return None

    def flush_buffers(self) -> None:
        if self._port and self._port.is_open:
            try:
                self._port.reset_input_buffer()
                self._port.reset_output_buffer()
                logger.debug("[Serial] Buffers flushed.")
            except serial.SerialException as exc:
                logger.warning(f"[Serial] Buffer flush failed: {exc}")

    # ------------------------------------------------------------------
    # Internal helpers (unchanged)
    # ------------------------------------------------------------------

    def _stabilize_after_connect(self) -> None:
        logger.info(f"[Serial] Stabilizing … ({STARTUP_STABILIZATION_DELAY}s)")
        time.sleep(STARTUP_STABILIZATION_DELAY)
        self.flush_buffers()

        drained = 0
        self._port.timeout = 0.2
        while True:
            raw = self._port.readline()
            if not raw:
                break
            drained += 1
            logger.debug(
                f"[Serial] Boot noise discarded: "
                f"{raw.decode('ascii', errors='replace').strip()}"
            )

        self._port.timeout = READ_TIMEOUT_SEC
        logger.info(f"[Serial] Stabilization complete. Discarded {drained} boot line(s).")
