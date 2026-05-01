"""
main.py — Sprint 2 Entry Point
Reactive Edge Hub: Command Queue Engine

Run:
    python main.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from src.utils.logger_setup import setup_logging
from src.hardware.serial_connection import SerialConnection, SerialConnectionError
from src.engine.command_engine import CommandEngine

import logging

setup_logging(session_name="sprint2")
logger = logging.getLogger(__name__)

SINGLE_COMMAND = "M105"
BATCH_COMMANDS = ["M115", "M105", "M114", "G28", "M105"]


def run_sprint2():
    logger.info("=" * 60)
    logger.info("  Reactive Edge Hub — Sprint 2: Command Queue Engine")
    logger.info("=" * 60)

    connection = SerialConnection()
    try:
        connection.connect()
        logger.info(f"[Main] Connected: {connection.port_path} @ {connection.baud_rate} baud")
    except SerialConnectionError as exc:
        logger.critical(f"[Main] Connection failed: {exc}")
        sys.exit(1)

    engine = CommandEngine(connection)
    engine.start()

    try:
        logger.info(f"\n[Main] Single command: {SINGLE_COMMAND}")
        result = engine.send(SINGLE_COMMAND)
        logger.info(f"[Main]   Status : {result.status.name}")
        logger.info(f"[Main]   Elapsed: {result.elapsed_ms:.1f} ms")

        logger.info(f"\n[Main] Batch: {len(BATCH_COMMANDS)} commands")
        results = engine.send_batch(BATCH_COMMANDS)
        for r in results:
            icon = "✓" if r.succeeded else "✗"
            ms = f"{round(r.elapsed_ms)} ms" if r.elapsed_ms else "N/A"
            logger.info(f"[Main]   {icon} {r.gcode:20s}  {r.status.name:10s}  {ms:>8s}")

        logger.info(f"\n[Main] Engine state : {engine.state.name}")
        logger.info(f"[Main] Queue depth  : {engine.queue_depth}")

    finally:
        engine.stop()
        connection.disconnect()
        logger.info("\n[Main] Sprint 2 complete.")


if __name__ == "__main__":
    run_sprint2()