"""
printer_simulator.py — Virtual Creality Printer Simulator
==========================================================
Creates a real pseudo-terminal (PTY) pair so your app connects
to /dev/pts/X exactly like a real USB serial port — no code changes needed.

Architecture match:
  - SerialConnection  → opens the slave PTY path printed at startup
  - SerialRouter      → reads lines; simulator fans out ok + telemetry
  - PrinterCommunicator → gets "ok" from ack_queue
  - TelemetryEngine   → gets temperature lines from telemetry_queue

Simulated behaviour:
  - Every G-code command → "ok\n"
  - M105 / M155         → "ok T:205.3 /210.0 B:60.1 /60.0 @:0 B@:0\n"
  - M114                → "X:10.00 Y:20.50 Z:5.00 E:0.00 Count X:800 Y:1640 Z:20000\nok\n"
  - M503 / M115         → firmware info + ok
  - G28                 → homing sequence with progressive position lines + ok
  - G0/G1               → move simulation + ok
  - M25                 → "// action:pause\nok\n"
  - M24                 → "// action:resume\nok\n"
  - M112                → emergency stop response
  - Unknown             → "ok\n"

Periodic telemetry (every 2 s) mimics Marlin's auto-report.

Usage:
    python3 printer_simulator.py

    Then in your app's config/settings.py override:
        SERIAL_PORT_PATTERNS = ["/dev/pts/*"]
    Or pass the printed PTY path directly.

    Press Ctrl+C to stop.
"""

import os
import pty
import time
import threading
import re
import sys
import termios
import tty
import fcntl
import logging
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum, auto

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Simulator")


# ── Simulated printer state ───────────────────────────────────────────────────

class SimStatus(Enum):
    IDLE     = auto()
    PRINTING = auto()
    PAUSED   = auto()
    HOMING   = auto()
    MOVING   = auto()


@dataclass
class SimState:
    status:        SimStatus = SimStatus.IDLE
    nozzle:        float = 25.0
    nozzle_target: float = 0.0
    bed:           float = 23.0
    bed_target:    float = 0.0
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    e: float = 0.0
    sd_done:  int = 0
    sd_total: int = 100_000
    progress: float = 0.0
    fan_speed: int = 0

    def temp_line(self) -> str:
        return (
            f"T:{self.nozzle:.1f} /{self.nozzle_target:.1f} "
            f"B:{self.bed:.1f} /{self.bed_target:.1f} "
            f"@:0 B@:0"
        )

    def pos_line(self) -> str:
        return (
            f"X:{self.x:.2f} Y:{self.y:.2f} Z:{self.z:.2f} E:{self.e:.2f} "
            f"Count X:{int(self.x*80)} Y:{int(self.y*80)} Z:{int(self.z*400)}"
        )


# ── PTY helpers ───────────────────────────────────────────────────────────────

def _set_raw(fd: int) -> None:
    """Put PTY master in raw mode so newlines are not doubled."""
    try:
        attrs = termios.tcgetattr(fd)
        attrs[1] &= ~termios.OPOST   # disable output post-processing
        termios.tcsetattr(fd, termios.TCSANOW, attrs)
    except Exception:
        pass


# ── Command dispatcher ────────────────────────────────────────────────────────

# Patterns
_RE_M104 = re.compile(r"M104\s+S([\d.]+)", re.I)
_RE_M140 = re.compile(r"M140\s+S([\d.]+)", re.I)
_RE_M109 = re.compile(r"M109\s+S([\d.]+)", re.I)
_RE_M190 = re.compile(r"M190\s+S([\d.]+)", re.I)
_RE_M106 = re.compile(r"M106\s+S(\d+)", re.I)
_RE_G0G1 = re.compile(
    r"G[01]\s*"
    r"(?:X([-\d.]+))?\s*(?:Y([-\d.]+))?\s*(?:Z([-\d.]+))?\s*(?:E([-\d.]+))?",
    re.I,
)
_RE_M23  = re.compile(r"M23\s+(.+)", re.I)
_RE_M155 = re.compile(r"M155\s+S(\d+)", re.I)


def dispatch(cmd: str, state: SimState) -> list[str]:
    """
    Given a G-code command string return the list of lines to write back
    (NOT including trailing newlines — caller adds them).
    """
    cmd = cmd.strip()
    if not cmd or cmd.startswith(";"):
        return ["ok"]

    # Strip line-number and checksum  (N42 G28 *34)
    cmd = re.sub(r"^N\d+\s*", "", cmd)
    cmd = re.sub(r"\*\d+$", "", cmd).strip()

    upper = cmd.upper()

    # ── Temperature set ───────────────────────────────────────────────
    if m := _RE_M104.match(cmd):
        state.nozzle_target = float(m.group(1))
        logger.debug(f"[Sim] Nozzle target → {state.nozzle_target}°C")
        return ["ok"]

    if m := _RE_M140.match(cmd):
        state.bed_target = float(m.group(1))
        logger.debug(f"[Sim] Bed target → {state.bed_target}°C")
        return ["ok"]

    if m := _RE_M109.match(cmd):
        state.nozzle_target = float(m.group(1))
        # Simulate heating wait (abbreviated)
        return [state.temp_line(), "ok"]

    if m := _RE_M190.match(cmd):
        state.bed_target = float(m.group(1))
        return [state.temp_line(), "ok"]

    # ── Temperature report ────────────────────────────────────────────
    if upper.startswith("M105"):
        return [f"ok {state.temp_line()}"]

    if m := _RE_M155.match(cmd):
        # Auto-report interval set — just ack
        return ["ok"]

    # ── Position report ───────────────────────────────────────────────
    if upper.startswith("M114"):
        return [state.pos_line(), "ok"]

    # ── Homing ────────────────────────────────────────────────────────
    if upper.startswith("G28"):
        state.status = SimStatus.HOMING
        state.x, state.y, state.z = 0.0, 0.0, 0.0
        logger.info("[Sim] Homing…")
        return ["X:0.00 Y:0.00 Z:0.00 E:0.00 Count X:0 Y:0 Z:0", "ok"]

    # ── Move ──────────────────────────────────────────────────────────
    if m := _RE_G0G1.match(cmd):
        if m.group(1) is not None: state.x = float(m.group(1))
        if m.group(2) is not None: state.y = float(m.group(2))
        if m.group(3) is not None: state.z = float(m.group(3))
        if m.group(4) is not None: state.e = float(m.group(4))
        return ["ok"]

    # ── Fan ───────────────────────────────────────────────────────────
    if m := _RE_M106.match(cmd):
        state.fan_speed = int(m.group(1))
        return ["ok"]

    if upper.startswith("M107"):
        state.fan_speed = 0
        return ["ok"]

    # ── SD card ───────────────────────────────────────────────────────
    if upper.startswith("M20"):   # List SD
        return ["Begin file list", "PRINT~1.GCO", "End file list", "ok"]

    if m := _RE_M23.match(cmd):   # Select file
        return [f"File opened:{m.group(1)} Size:{state.sd_total}", "File selected", "ok"]

    if upper.startswith("M24"):   # Print / Resume
        state.status = SimStatus.PRINTING
        return ["// action:resume", "ok"]

    if upper.startswith("M25"):   # Pause
        state.status = SimStatus.PAUSED
        return ["// action:pause", "ok"]

    if upper.startswith("M26"):   # Set SD pos
        return ["ok"]

    if upper.startswith("M27"):   # SD progress
        return [
            f"SD printing byte {state.sd_done}/{state.sd_total}",
            "ok",
        ]

    # ── Firmware info ─────────────────────────────────────────────────
    if upper.startswith("M115"):
        return [
            "FIRMWARE_INFO: FIRMWARE_NAME:Marlin_Simulator 2.1.x "
            "SOURCE_CODE_URL:https://github.com/MarlinFirmware/Marlin "
            "PROTOCOL_VERSION:1.0 MACHINE_TYPE:Creality Ender-3",
            "Cap:AUTOREPORT_TEMP:1",
            "Cap:AUTOREPORT_POS:1",
            "ok",
        ]

    # ── EEPROM settings ───────────────────────────────────────────────
    if upper.startswith("M503"):
        return [
            "echo:  G21",
            "echo:  M149 C",
            f"echo:  M92 X80.00 Y80.00 Z400.00 E93.00",
            "echo:  M203 X500.00 Y500.00 Z5.00 E25.00",
            "ok",
        ]

    # ── Emergency stop ────────────────────────────────────────────────
    if upper.startswith("M112"):
        state.status = SimStatus.IDLE
        state.nozzle_target = 0.0
        state.bed_target = 0.0
        return ["ok"]

    # ── Auto bed levelling ────────────────────────────────────────────
    if upper.startswith("G29"):
        return ["Bed leveling ok", "ok"]

    # ── Dwell ─────────────────────────────────────────────────────────
    if upper.startswith("G4"):
        return ["ok"]

    # ── Default ───────────────────────────────────────────────────────
    logger.debug(f"[Sim] Unknown command: {cmd!r} → ok")
    return ["ok"]


# ── Temperature drift simulation ─────────────────────────────────────────────

def _tick_temps(state: SimState) -> None:
    """Nudge simulated temps toward targets (called every ~0.5 s)."""
    for attr, target_attr in [("nozzle", "nozzle_target"), ("bed", "bed_target")]:
        current = getattr(state, attr)
        target  = getattr(state, target_attr)
        diff = target - current
        if abs(diff) < 0.5:
            setattr(state, attr, target)
        else:
            setattr(state, attr, current + diff * 0.08)


# ── Main simulator class ──────────────────────────────────────────────────────

class PrinterSimulator:
    """
    Opens a PTY pair, exposes slave path to the app, reads G-code from
    master fd, writes responses back.
    """

    def __init__(self) -> None:
        self.state = SimState()
        self._master_fd: Optional[int] = None
        self._slave_path: Optional[str] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()   # serialize writes to master fd

    # ── Lifecycle ─────────────────────────────────────────────────────

    def start(self) -> str:
        master_fd, slave_fd = pty.openpty()
        _set_raw(master_fd)
        slave_path = os.ttyname(slave_fd)
        # Keep slave_fd open so the PTY stays alive even before the app connects.
        # Store it so we can close it on stop().
        self._slave_fd   = slave_fd
        self._master_fd  = master_fd
        self._slave_path = slave_path

        threading.Thread(target=self._read_loop,    daemon=True, name="Sim-Reader").start()
        threading.Thread(target=self._telemetry_loop, daemon=True, name="Sim-Telemetry").start()
        threading.Thread(target=self._temp_tick_loop, daemon=True, name="Sim-TempTick").start()

        logger.info(f"[Simulator] ✓ Virtual port ready → {slave_path}")
        logger.info(f"[Simulator] Set SERIAL_PORT_PATTERNS = ['{slave_path}'] in your app")
        return slave_path

    def stop(self) -> None:
        self._stop.set()
        for fd_attr in ("_master_fd", "_slave_fd"):
            fd = getattr(self, fd_attr, None)
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
        logger.info("[Simulator] Stopped.")

    # ── Write helper ──────────────────────────────────────────────────

    def _write(self, line: str) -> None:
        data = (line + "\n").encode("ascii", errors="replace")
        with self._lock:
            try:
                os.write(self._master_fd, data)
                logger.debug(f"[Sim] >> {line!r}")
            except OSError as e:
                logger.warning(f"[Sim] Write error: {e}")

    # ── Read loop — receive G-code from app ───────────────────────────

    def _read_loop(self) -> None:
        buf = b""
        logger.info("[Sim-Reader] Listening for G-code…")
        while not self._stop.is_set():
            try:
                chunk = os.read(self._master_fd, 256)
            except OSError:
                break
            if not chunk:
                continue
            buf += chunk
            while b"\n" in buf:
                line_bytes, buf = buf.split(b"\n", 1)
                cmd = line_bytes.decode("ascii", errors="replace").strip()
                if not cmd:
                    continue
                logger.info(f"[Sim] << {cmd!r}")
                responses = dispatch(cmd, self.state)
                for resp in responses:
                    self._write(resp)
                    time.sleep(0.005)   # small gap between lines

    # ── Periodic telemetry — mimics Marlin auto-report ────────────────

    def _telemetry_loop(self) -> None:
        """Push temperature report every 2 s (like M155 S2 auto-report)."""
        while not self._stop.is_set():
            time.sleep(2.0)
            if self._stop.is_set():
                break
            line = f"T:{self.state.nozzle:.1f} /{self.state.nozzle_target:.1f} B:{self.state.bed:.1f} /{self.state.bed_target:.1f} @:0 B@:0"
            self._write(line)

            # If printing, also emit SD progress
            if self.state.status == SimStatus.PRINTING:
                self.state.sd_done = min(
                    self.state.sd_done + 1200,
                    self.state.sd_total,
                )
                if self.state.sd_done >= self.state.sd_total:
                    self._write("Done printing file")
                    self.state.status = SimStatus.IDLE
                    self.state.sd_done = 0
                else:
                    self._write(
                        f"SD printing byte {self.state.sd_done}/{self.state.sd_total}"
                    )

    # ── Temperature physics tick ──────────────────────────────────────

    def _temp_tick_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(0.5)
            _tick_temps(self.state)


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    sim = PrinterSimulator()
    slave_path = sim.start()

    print("\n" + "=" * 60)
    print("  VIRTUAL PRINTER SIMULATOR RUNNING")
    print("=" * 60)
    print(f"  PTY slave path : {slave_path}")
    print(f"  Baud rate      : any (PTY ignores baud)")
    print()
    print("  In your app's config/settings.py set:")
    print(f'    SERIAL_PORT_PATTERNS = ["{slave_path}"]')
    print()
    print("  Or export for quick test:")
    print(f'    export PRINTER_PORT="{slave_path}"')
    print()
    print("  Supported G-codes: G0/G1/G28/G29/G4")
    print("  M codes: M24/M25/M104/M105/M106/M107/M109/M112")
    print("           M114/M115/M140/M155/M190/M20/M23/M25/M26/M27/M503")
    print()
    print("  Periodic telemetry: every 2 s")
    print("  Press Ctrl+C to stop.")
    print("=" * 60 + "\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Simulator] Shutting down…")
        sim.stop()


if __name__ == "__main__":
    main()