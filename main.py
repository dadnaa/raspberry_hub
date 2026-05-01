"""
main.py — Sprint 3 Entry Point

Reactive Edge Hub: Telemetry + State Intelligence Layer

Run:
    python main.py

What's new in Sprint 3:
  - TelemetryEngine runs in a parallel daemon thread
  - StateManager holds live printer state
  - Command queue + telemetry operate independently (no mutual blocking)
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

from src.utils.logger_setup        import setup_logging
from src.hardware.serial_connection import SerialConnection, SerialConnectionError
from src.engine.command_engine      import CommandEngine
from src.telemetry                  import StateManager, TelemetryEngine
import logging

setup_logging(session_name="sprint3")
logger = logging.getLogger(__name__)

# Commands to exercise command queue while telemetry runs
BATCH_COMMANDS = ["M115", "M105", "M114", "M105", "M114"]

# How long to let both systems run together (seconds)
PARALLEL_RUN_SEC = 10


def on_telemetry_event(event):
    """Simple console subscriber — Sprint 4 will replace this with MQTT publish."""
    logger.info(f"[Event] {event.timestamp} changed={list(event.changed_fields.keys())}")


def run_sprint3():
    logger.info("=" * 60)
    logger.info("  Reactive Edge Hub — Sprint 3: Telemetry + State Layer")
    logger.info("=" * 60)

    # ── 1. Serial connection ──────────────────────────────────────────
    connection = SerialConnection()
    try:
        connection.connect()
        logger.info(f"[Main] Connected: {connection.port_path} @ {connection.baud_rate} baud")
    except SerialConnectionError as exc:
        logger.critical(f"[Main] Connection failed: {exc}")
        sys.exit(1)

    # ── 2. State manager (shared single source of truth) ─────────────
    state_manager = StateManager()

    # ── 3. Telemetry engine (reads serial output in background) ───────
    # The SerialConnection must expose a `line_queue` (queue.Queue)
    # that it pushes all decoded lines into.  See notes below.
    telemetry = TelemetryEngine(
        line_queue=connection.line_queue,
        state_manager=state_manager,
        on_event=on_telemetry_event,
    )
    telemetry.start()

    # ── 4. Command engine (unchanged from Sprint 2) ───────────────────
    engine = CommandEngine(connection)
    engine.start()

    try:
        logger.info(f"\n[Main] Running {len(BATCH_COMMANDS)} commands while telemetry streams...")
        results = engine.send_batch(BATCH_COMMANDS)

        for r in results:
            icon = "✓" if r.succeeded else "✗"
            ms   = f"{round(r.elapsed_ms)} ms" if r.elapsed_ms else "N/A"
            logger.info(f"[Main] {icon} {r.gcode:20s} {r.status.name:10s} {ms:>8s}")

        # Let telemetry run freely for a while
        logger.info(f"\n[Main] Letting telemetry run for {PARALLEL_RUN_SEC}s...")
        for i in range(PARALLEL_RUN_SEC):
            time.sleep(1)
            snap = state_manager.get_snapshot()
            logger.info(
                f"[State] t+{i+1:02d}s  status={snap.status.name}"
                f"  T={snap.nozzle_temp}°C  B={snap.bed_temp}°C"
                f"  pos=({snap.position_x}, {snap.position_y}, {snap.position_z})"
                f"  progress={snap.progress_pct}%"
            )

    finally:
        engine.stop()
        telemetry.stop()
        connection.disconnect()

    logger.info("\n[Main] Sprint 3 complete.")
    logger.info("[Main] Final state snapshot:")
    final = state_manager.get_snapshot()
    for k, v in final.to_dict().items():
        logger.info(f"  {k:20s}: {v}")


if __name__ == "__main__":
    run_sprint3()