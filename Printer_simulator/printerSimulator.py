import time
import serial
import threading

PORT = "/tmp/ttyV1"
BAUD = 115200

state = {
    "nozzle": 200.0,
    "bed": 60.0,
    "target_nozzle": 200.0,
    "target_bed": 60.0,
    "printing": False,
    "sd_done": 0,
    "sd_total": 10000,
}

# -----------------------------
# helper
# -----------------------------
def send(ser, msg: str):
    print(f"[sim_printer] >> {msg.strip()}")
    ser.write(msg.encode("ascii", errors="replace"))
    ser.flush()


# -----------------------------
# STARTUP (BOOT SEQUENCE)
# -----------------------------
def startup_sequence(ser):
    time.sleep(0.5)

    # Boot / firmware detection (→ REBOOTING in your system)
    send(ser, "start\n")
    send(ser, "Marlin 2.1.2\n")
    send(ser, "FIRMWARE_INFO: Marlin_SIM\n")

    # First temperature snapshot (still NOT enough alone for ONLINE)
    send(ser,
         f"T:{state['nozzle']:.1f} /{state['target_nozzle']:.1f} "
         f"B:{state['bed']:.1f} /{state['target_bed']:.1f}\n"
    )

    # IMPORTANT: printer acknowledges boot (classic Marlin behavior)
    send(ser, "ok\n")


# -----------------------------
# PERIODIC TELEMETRY
# -----------------------------
def telemetry_loop(ser):
    while True:
        line = (
            f"T:{state['nozzle']:.1f} /{state['target_nozzle']:.1f} "
            f"B:{state['bed']:.1f} /{state['target_bed']:.1f}\n"
        )
        send(ser, line)
        time.sleep(1.0)


# -----------------------------
# PRINT SIMULATION
# -----------------------------
def simulate_print_progress(ser):
    """
    Simulates SD printing progress.
    Triggers PRINTING state correctly.
    """
    state["printing"] = True
    state["sd_done"] = 0

    while state["printing"] and state["sd_done"] < state["sd_total"]:
        state["sd_done"] += 1200

        send(
            ser,
            f"SD printing byte {state['sd_done']}/{state['sd_total']}\n"
        )

        time.sleep(1.5)

    if state["sd_done"] >= state["sd_total"]:
        send(ser, "Done printing file\n")
        state["printing"] = False
        state["sd_done"] = 0


# -----------------------------
# MAIN
# -----------------------------
def main():
    ser = serial.Serial(PORT, BAUD, timeout=0.1, write_timeout=0.5)

    # Boot sequence
    startup_sequence(ser)

    # Telemetry thread
    threading.Thread(target=telemetry_loop, args=(ser,), daemon=True).start()

    print(f"[sim_printer] Listening on {PORT} @ {BAUD}")

    buf = b""

    while True:
        b = ser.read(1)
        if not b:
            continue

        if b == b"\n":
            cmd = buf.decode("ascii", errors="replace").strip()
            buf = b""

            if not cmd:
                continue

            print(f"[sim_printer] << {cmd}")
            u = cmd.upper()

            # -------------------------
            # temperature request
            # -------------------------
            if u.startswith("M105"):
                send(ser,
                     f"T:{state['nozzle']:.1f} /{state['target_nozzle']:.1f} "
                     f"B:{state['bed']:.1f} /{state['target_bed']:.1f}\n"
                )
                send(ser, "ok\n")

            # -------------------------
            # START PRINT (M24)
            # -------------------------
            elif u.startswith("M24"):
                send(ser, "// action:resume\n")
                send(ser, "ok\n")

                # start print thread
                threading.Thread(
                    target=simulate_print_progress,
                    args=(ser,),
                    daemon=True
                ).start()

            # -------------------------
            # PAUSE (M25)
            # -------------------------
            elif u.startswith("M25"):
                state["printing"] = False
                send(ser, "// action:pause\n")
                send(ser, "ok\n")

            # -------------------------
            # SD STATUS REQUEST (M27)
            # -------------------------
            elif u.startswith("M27"):
                if state["printing"]:
                    send(
                        ser,
                        f"SD printing byte {state['sd_done']}/{state['sd_total']}\n"
                    )
                else:
                    send(ser, "Not printing\n")

                send(ser, "ok\n")

            # -------------------------
            # DEFAULT
            # -------------------------
            else:
                send(ser, "ok\n")

        else:
            buf += b


if __name__ == "__main__":
    main()
