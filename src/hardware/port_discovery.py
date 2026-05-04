"""
port_discovery.py — Layer 1: Hardware Interface
Scans and identifies the correct USB serial port for the Creality printer.
No port is ever hardcoded.
"""

import glob
import logging

from config.settings import SERIAL_BAUD_RATES, SERIAL_PORT_PATTERNS

logger = logging.getLogger(__name__)

# Known USB serial port patterns on Linux (Raspberry Pi)
PORT_PATTERNS = SERIAL_PORT_PATTERNS

# Standard baud rates for Creality printers (priority order)
CREALITY_BAUD_RATES = SERIAL_BAUD_RATES


def discover_ports() -> list[str]:
    """
    Scan all known serial port patterns and return a list of
    available device paths.

    Returns:
        list[str]: Discovered port paths (e.g. ['/dev/ttyUSB0'])
    """
    found = []
    for pattern in PORT_PATTERNS:
        matches = glob.glob(pattern)
        found.extend(matches)

    if found:
        logger.info(f"[PortDiscovery] Found {len(found)} port(s): {found}")
    else:
        logger.warning("[PortDiscovery] No serial ports found. Check USB connection.")

    return found


def select_port(ports: list[str]) -> str | None:
    """
    Select the most likely printer port from the discovered list.
    Strategy: prefer /dev/ttyUSB0, then /dev/ttyACM0, then first available.

    Args:
        ports: List of discovered port paths.

    Returns:
        str | None: Selected port path, or None if list is empty.
    """
    if not ports:
        return None

    # Prefer USB over ACM, prefer index 0
    for preferred in ["/dev/ttyUSB0", "/dev/ttyACM0"]:
        if preferred in ports:
            logger.info(f"[PortDiscovery] Selected preferred port: {preferred}")
            return preferred

    selected = ports[0]
    logger.info(f"[PortDiscovery] Selected first available port: {selected}")
    return selected


def get_printer_port() -> tuple[str | None, list[str]]:
    """
    Full port discovery flow: scan → select.

    Returns:
        tuple: (selected_port, all_discovered_ports)
    """
    ports = discover_ports()
    selected = select_port(ports)
    return selected, ports
