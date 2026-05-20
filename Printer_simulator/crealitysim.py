"""
sim_printer.py — Creality CR-10 Smart Simulator

Accurately simulates the serial behavior of a Creality CR-10 Smart running
Marlin 2.x firmware, including:

  - Exact boot sequence (start token, firmware info, EEPROM dump, capabilities)
  - CH340 USB chip behavior (no DTR reset on hot-plug → silent on reconnect)
  - Auto temperature reporting (Cap:AUTOREPORT_TEMP:1, M155 support)
  - BLTouch / mesh bed leveling output
  - Wi-Fi module noise lines
  - SD print progress simulation
  - Pause / resume action commands
  - M114 position reporting
  - M503 EEPROM settings dump
  - M115 firmware info (re-queryable at any time)
  - Correct "ok" acknowledgment flow

CHANGE: telemetry_loop thread removed.
  The hub is responsible for polling M105 periodically (main.py polling loop).
  The simulator responds to M105 on demand — handler kept.
  Auto-reporting via M155 is still supported for hub-initiated autoreport.
"""

import time
import serial
import threading
import random

PORT = "/tmp/ttyV1"
BAUD = 115200

# -----------------------------------------------------------------------------
# Shared printer state
# -----------------------------------------------------------------------------
state = {
    "nozzle":        25.0,
    "bed":           25.0,
    "target_nozzle": 0.0,
    "target_bed":    0.0,
    "x": 0.0,
    "y": 0.0,
    "z": 0.0,
    "e": 0.0,
    "printing":  False,
    "paused":    False,
    "sd_done":   0,
    "sd_total":  10000,
    # Set by M155 S<n> from hub. 0 = disabled (default — hub must poll M105).
    "autoreport_interval": 0,
    "leveling_active": False,
}

state_lock = threading.Lock()


# -----------------------------------------------------------------------------
# Helper
# -----------------------------------------------------------------------------
def send(ser, msg: str):
    print(f"[sim] >> {msg.strip()}")
    ser.write(msg.encode("ascii", errors="replace"))
    ser.flush()


# -----------------------------------------------------------------------------
# BOOT SEQUENCE
# -----------------------------------------------------------------------------
def boot_sequence(ser):
    """
    Fires on power-on or reset (M112). NOT on USB hot-plug (CH340 behaviour).
    """
    time.sleep(0.8)

    send(ser, "start\n")
    time.sleep(0.05)
    send(ser, "echo: External Reset\n")
    time.sleep(0.05)

    send(ser, "Marlin 2.0.9.3 (Oct 10 2022 15:32:17)\n")
    send(ser, "echo: Last Updated: 2022-10-10 | Author: Creality\n")
    send(ser, "echo: Compiled: Oct 10 2022\n")
    send(ser, "echo:  Free Memory: 3028  PlannerBufferBytes: 1424\n")
    time.sleep(0.1)

    send(ser, "echo:Stored settings retrieved\n")
    send(ser, "echo:  G21    ; (mm)\n")
    send(ser, "echo:  M149 C ; Units in Celsius\n")
    send(ser, "echo:  M200 S0 D1.75\n")
    send(ser, "echo:  M92 X80.00 Y80.00 Z400.00 E415.00\n")
    send(ser, "echo:  M203 X500.00 Y500.00 Z15.00 E25.00\n")
    send(ser, "echo:  M201 X500.00 Y500.00 Z100.00 E5000.00\n")
    send(ser, "echo:  M204 P500.00 R500.00 T500.00\n")
    send(ser, "echo:  M205 B20000.00 S0.00 T0.00 J0.01\n")
    send(ser, "echo:  M206 X0.00 Y0.00 Z0.00\n")
    send(ser, "echo:  M145 S0 H200 B60 F0\n")
    send(ser, "echo:  M145 S1 H240 B70 F0\n")
    send(ser, "echo:  M301 P28.72 I2.03 D101.61\n")
    send(ser, "echo:  M304 P462.10 I85.47 D624.59\n")
    send(ser, "echo:  M420 S0 Z0.00\n")
    send(ser, "echo:  M851 X-44.00 Y-14.00 Z-2.35\n")
    time.sleep(0.1)

    send(ser,
         "FIRMWARE_NAME:Marlin 2.0.9.3 "
         "SOURCE_CODE_URL:github.com/MarlinFirmware/Marlin "
         "PROTOCOL_VERSION:1.0 "
         "MACHINE_TYPE:CR-10 Smart "
         "EXTRUDER_COUNT:1 "
         "UUID:cede949f-0000-0000-0000-000000000000\n"
    )
    for cap in [
        "Cap:SERIAL_XON_XOFF:0", "Cap:BINARY_FILE_TRANSFER:0",
        "Cap:EEPROM:1", "Cap:VOLUMETRIC:1", "Cap:AUTOREPORT_TEMP:1",
        "Cap:PROGRESS:0", "Cap:PRINT_JOB:1", "Cap:AUTOLEVEL:1",
        "Cap:RUNOUT:1", "Cap:Z_PROBE:1", "Cap:LEVELING_DATA:1",
        "Cap:BUILD_PERCENT:0", "Cap:SOFTWARE_POWER:0",
        "Cap:TOGGLE_LIGHTS:0", "Cap:CASE_LIGHT_BRIGHTNESS:0",
        "Cap:EMERGENCY_PARSER:1", "Cap:HOST_ACTION_COMMANDS:1",
        "Cap:PROMPT_SUPPORT:0", "Cap:SDCARD:1", "Cap:MULTI_VOLUME:0",
        "Cap:REPEAT:0", "Cap:SD_WRITE:1", "Cap:AUTOREPORT_SD_STATUS:0",
        "Cap:THERMAL_PROTECTION:1", "Cap:MOTION_MODES:0",
        "Cap:CHAMBER_TEMPERATURE:0",
    ]:
        send(ser, cap + "\n")
    time.sleep(0.05)

    # CR-10 Smart Wi-Fi noise
    send(ser, "wifi:\n")
    send(ser, "echo:wifi connecting...\n")
    time.sleep(0.3)
    send(ser, "echo:wifi connected\n")

    send(ser, _temp_line() + "\n")
    send(ser, "ok\n")
    print("[sim] Boot sequence complete.")


# -----------------------------------------------------------------------------
# Temperature helpers
# -----------------------------------------------------------------------------
def _temp_line() -> str:
    with state_lock:
        if state["target_nozzle"] > 0:
            state["nozzle"] = min(
                state["target_nozzle"],
                state["nozzle"] + random.uniform(1.5, 3.5)
            )
        if state["target_bed"] > 0:
            state["bed"] = min(
                state["target_bed"],
                state["bed"] + random.uniform(0.5, 1.5)
            )
        return (
            f"T:{state['nozzle']:.1f} /{state['target_nozzle']:.1f} "
            f"B:{state['bed']:.1f} /{state['target_bed']:.1f} "
            f"@:0 B@:0"
        )


# -----------------------------------------------------------------------------
# Auto temperature reporting (M155 Sn)
# Idle by default — only active after hub sends M155 S<n>.
# The unconditional telemetry_loop has been REMOVED; hub polls via M105.
# -----------------------------------------------------------------------------
def autoreport_loop(ser):
    while True:
        with state_lock:
            interval = state["autoreport_interval"]
        if interval > 0:
            send(ser, _temp_line() + "\n")
            time.sleep(interval)
        else:
            time.sleep(0.5)


# -----------------------------------------------------------------------------
# Wi-Fi noise (CR-10 Smart specific)
# -----------------------------------------------------------------------------
def wifi_noise_loop(ser):
    noise_lines = ["wifi:\n", "echo:wifi connected\n", "echo:wifi heartbeat\n"]
    while True:
        time.sleep(random.uniform(25, 45))
        send(ser, random.choice(noise_lines))


# -----------------------------------------------------------------------------
# SD print simulation
# -----------------------------------------------------------------------------
def simulate_print(ser):
    with state_lock:
        state["sd_done"]  = 0
        state["printing"] = True
        state["paused"]   = False

    send(ser, "File opened: CE3PRO_test.gcode Size: 10000\n")
    send(ser, "File selected\n")
    send(ser, "ok\n")

    while True:
        with state_lock:
            if not state["printing"]:
                break
            if not state["paused"]:
                state["sd_done"] = min(
                    state["sd_done"] + random.randint(800, 1400),
                    state["sd_total"]
                )
            done  = state["sd_done"]
            total = state["sd_total"]

        send(ser, f"SD printing byte {done}/{total}\n")

        if done >= total:
            time.sleep(0.5)
            send(ser, "Done printing file\n")
            send(ser, "echo:enqueueing \"M84\"\n")
            with state_lock:
                state["printing"] = False
                state["sd_done"]  = 0
            break

        time.sleep(1.5)


# -----------------------------------------------------------------------------
# M114 position report
# -----------------------------------------------------------------------------
def _position_line() -> str:
    with state_lock:
        return (
            f"X:{state['x']:.2f} Y:{state['y']:.2f} "
            f"Z:{state['z']:.2f} E:{state['e']:.2f} "
            f"Count X:{int(state['x']*80)} "
            f"Y:{int(state['y']*80)} "
            f"Z:{int(state['z']*400)}"
        )


# -----------------------------------------------------------------------------
# M503 EEPROM dump
# -----------------------------------------------------------------------------
def send_m503(ser):
    lines = [
        "echo:  G21    ; (mm)", "echo:  M149 C ; Units in Celsius",
        "echo:  M200 S0 D1.75", "echo:  M92 X80.00 Y80.00 Z400.00 E415.00",
        "echo:  M203 X500.00 Y500.00 Z15.00 E25.00",
        "echo:  M201 X500.00 Y500.00 Z100.00 E5000.00",
        "echo:  M204 P500.00 R500.00 T500.00",
        "echo:  M205 B20000.00 S0.00 T0.00 J0.01",
        "echo:  M206 X0.00 Y0.00 Z0.00", "echo:  M145 S0 H200 B60 F0",
        "echo:  M145 S1 H240 B70 F0", "echo:  M301 P28.72 I2.03 D101.61",
        "echo:  M304 P462.10 I85.47 D624.59",
        "echo:  M420 S0 Z0.00", "echo:  M851 X-44.00 Y-14.00 Z-2.35",
    ]
    for l in lines:
        send(ser, l + "\n")
    send(ser, "ok\n")


# -----------------------------------------------------------------------------
# M115 firmware info
# -----------------------------------------------------------------------------
def send_m115(ser):
    send(ser,
         "FIRMWARE_NAME:Marlin 2.0.9.3 "
         "SOURCE_CODE_URL:github.com/MarlinFirmware/Marlin "
         "PROTOCOL_VERSION:1.0 MACHINE_TYPE:CR-10 Smart "
         "EXTRUDER_COUNT:1 UUID:cede949f-0000-0000-0000-000000000000\n"
    )
    for cap in [
        "Cap:SERIAL_XON_XOFF:0", "Cap:EEPROM:1", "Cap:AUTOREPORT_TEMP:1",
        "Cap:AUTOLEVEL:1", "Cap:Z_PROBE:1", "Cap:LEVELING_DATA:1",
        "Cap:EMERGENCY_PARSER:1", "Cap:HOST_ACTION_COMMANDS:1",
        "Cap:THERMAL_PROTECTION:1", "Cap:SDCARD:1",
    ]:
        send(ser, cap + "\n")
    send(ser, "ok\n")


# -----------------------------------------------------------------------------
# G29 BLTouch mesh leveling
# -----------------------------------------------------------------------------
def simulate_g29(ser):
    send(ser, "echo:Bed Leveling ON\n")
    send(ser, "echo:Fade Height 0.0\n")
    for x, y in [(0,0),(100,0),(200,0),(0,100),(100,100),(200,100),(0,200),(100,200),(200,200)]:
        z = round(random.uniform(-0.15, 0.15), 3)
        send(ser, f"Bed X: {x:.2f} Y: {y:.2f} Z: {z:.3f}\n")
        time.sleep(0.3)
    send(ser, "Bilinear Leveling Grid:\n")
    send(ser, "      0      1      2\n")
    send(ser, " 0 +0.023 -0.012 +0.045\n")
    send(ser, " 1 -0.008 +0.001 -0.034\n")
    send(ser, " 2 +0.056 +0.011 -0.022\n")
    send(ser, "ok\n")


# -----------------------------------------------------------------------------
# Command dispatcher
# -----------------------------------------------------------------------------
def handle_command(ser, cmd: str):
    u = cmd.upper().strip()
    print(f"[sim] << {cmd}")

    if u.startswith("M115"):
        send_m115(ser)

    # M105 — temperature on demand (hub polling lands here)
    elif u.startswith("M105"):
        send(ser, _temp_line() + "\n")
        send(ser, "ok\n")

    elif u.startswith("M114"):
        send(ser, _position_line() + "\n")
        send(ser, "ok\n")

    elif u.startswith("M155"):
        try:
            interval = 0
            for p in u.split()[1:]:
                if p.startswith("S"):
                    interval = int(p[1:])
            with state_lock:
                state["autoreport_interval"] = interval
            print(f"[sim] Autoreport {'enabled every ' + str(interval) + 's' if interval else 'disabled'}")
        except (ValueError, IndexError):
            pass
        send(ser, "ok\n")

    elif u.startswith("M503"):
        send_m503(ser)

    elif u.startswith("M104"):
        try:
            with state_lock:
                state["target_nozzle"] = float(u.split("S")[1].split()[0])
        except (IndexError, ValueError):
            pass
        send(ser, "ok\n")

    elif u.startswith("M140"):
        try:
            with state_lock:
                state["target_bed"] = float(u.split("S")[1].split()[0])
        except (IndexError, ValueError):
            pass
        send(ser, "ok\n")

    elif u.startswith("M109"):
        try:
            with state_lock:
                state["target_nozzle"] = float(u.split("S")[1].split()[0])
        except (IndexError, ValueError):
            pass
        for _ in range(3):
            send(ser, _temp_line() + "\n")
            time.sleep(0.4)
        send(ser, "ok\n")

    elif u.startswith("M190"):
        try:
            with state_lock:
                state["target_bed"] = float(u.split("S")[1].split()[0])
        except (IndexError, ValueError):
            pass
        for _ in range(3):
            send(ser, _temp_line() + "\n")
            time.sleep(0.4)
        send(ser, "ok\n")

    elif u.startswith("M24"):
        with state_lock:
            already_printing = state["printing"]
            paused = state["paused"]
        if paused:
            with state_lock:
                state["paused"] = False
            send(ser, "// action:resume\n")
            send(ser, "ok\n")
        elif not already_printing:
            send(ser, "// action:resume\n")
            threading.Thread(target=simulate_print, args=(ser,), daemon=True).start()
        else:
            send(ser, "ok\n")

    elif u.startswith("M25"):
        with state_lock:
            state["paused"] = True
        send(ser, "// action:pause\n")
        send(ser, "ok\n")

    elif u.startswith("M27"):
        with state_lock:
            printing, done, total = state["printing"], state["sd_done"], state["sd_total"]
        if printing:
            send(ser, f"SD printing byte {done}/{total}\n")
        else:
            send(ser, "Not SD printing\n")
        send(ser, "ok\n")

    elif u.startswith("G28"):
        send(ser, "echo:busy: processing\n")
        time.sleep(1.5)
        with state_lock:
            state["x"] = state["y"] = state["z"] = 0.0
        send(ser, "ok\n")

    elif u.startswith("G29"):
        threading.Thread(target=simulate_g29, args=(ser,), daemon=True).start()

    elif u.startswith("M420"):
        send(ser, "echo:Bed Leveling ON\n")
        send(ser, "echo:Fade Height 0.0\n")
        send(ser, "ok\n")

    elif u.startswith("M112"):
        with state_lock:
            state["printing"] = state["paused"] = False
            state["target_nozzle"] = state["target_bed"] = 0.0
        send(ser, "echo:EMERGENCY STOP\n")
        threading.Thread(target=boot_sequence, args=(ser,), daemon=True).start()

    elif u.startswith("M84") or u.startswith("M18"):
        send(ser, "ok\n")

    elif u.startswith("G0") or u.startswith("G1"):
        try:
            for part in u.split()[1:]:
                with state_lock:
                    if part.startswith("X"):   state["x"] = float(part[1:])
                    elif part.startswith("Y"): state["y"] = float(part[1:])
                    elif part.startswith("Z"): state["z"] = float(part[1:])
                    elif part.startswith("E"): state["e"] = float(part[1:])
        except (ValueError, IndexError):
            pass
        send(ser, "ok\n")

    else:
        send(ser, "ok\n")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    ser = serial.Serial(PORT, BAUD, timeout=0.1, write_timeout=0.5)
    print(f"[sim] CR-10 Smart simulator on {PORT} @ {BAUD}")

    boot_sequence(ser)

    # Auto temp reporting — stays idle until hub sends M155 S<n>
    threading.Thread(target=autoreport_loop, args=(ser,), daemon=True).start()

    # NOTE: telemetry_loop REMOVED.
    # Temperature data now comes from:
    #   (a) hub periodic M105 polling (main.py polling loop), OR
    #   (b) hub-initiated autoreport via M155 S<n>

    # CR-10 Smart Wi-Fi noise
    threading.Thread(target=wifi_noise_loop, args=(ser,), daemon=True).start()

    print("[sim] Ready — waiting for G-code commands.")

    buf = b""
    while True:
        b = ser.read(1)
        if not b:
            continue
        if b == b"\n":
            cmd = buf.decode("ascii", errors="replace").strip()
            buf = b""
            if cmd:
                handle_command(ser, cmd)
        else:
            buf += b


if __name__ == "__main__":
    main()
