"""
main.py — Fixed Entry Point (applies to Sprint 4 onwards)

KEY WIRING CHANGES vs previous main.py versions
─────────────────────────────────────────────────
1. SerialRouter is created after connect() and started immediately.
   It owns the ONE background thread that calls read_line().

2. CommandEngine receives the router so PrinterCommunicator
   reads from router.ack_queue instead of calling read_line() directly.

3. TelemetryEngine receives router.telemetry_queue — unchanged API,
   but now fed correctly by SerialRouter instead of nothing.

4. SerialConnection receives router.reset_queues as on_reconnect hook
   so stale queue data is flushed atomically on reconnect.

5. VisionController is wired via bridge.job_manager (public property)
   and bridge.mqtt_client (public property) — no more private attribute access.
"""

import os
import sys
import signal
import threading
import logging
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))

from config.settings import (
    MQTT_DEFAULT_PRINTER_ID,
    MQTT_PRINTER_ID_ENV,
    PRINTER_MODEL,
    PRINTER_NAME,
    PRINTER_NOZZLE_DIAMETER,
    VISION_CAMERA_URL_ENV,
    VISION_CONFIDENCE_MIN,
    VISION_COOLDOWN_SEC,
    VISION_DEFAULT_CAMERA_URL,
    VISION_FAILURE_THRESHOLD,
)
from src.utils.logger_setup          import setup_logging
from src.hardware.serial_connection   import SerialConnection, SerialConnectionError
from src.hardware.serial_router       import SerialRouter
from src.engine.command_engine        import CommandEngine
from src.telemetry                    import StateManager, TelemetryEngine
from src.cloud.mqtt_bridge             import MQTTBridge, PrinterConfig
from src.vision.ai_client             import AIClient
from src.vision.vision_monitor        import VisionMonitor
from src.vision.vision_controller     import VisionController
from src.vision.vision_event_publisher import VisionEventPublisher

setup_logging(session_name="rasp-arch")
logger = logging.getLogger(__name__)

PRINTER_CONFIG = PrinterConfig(
    name=PRINTER_NAME,
    model=PRINTER_MODEL,
    nozzle_diameter=PRINTER_NOZZLE_DIAMETER,
)


def main():
    logger.info("=" * 60)
    logger.info("  Reactive Edge Hub — Full Stack (Sprints 1-6)")
    logger.info("=" * 60)
    # Load environment from .env in project root if present
    load_dotenv()

    camera_url = os.environ.get(VISION_CAMERA_URL_ENV, VISION_DEFAULT_CAMERA_URL)
    printer_id = os.environ.get(MQTT_PRINTER_ID_ENV, MQTT_DEFAULT_PRINTER_ID)

    # ── 1. Serial connection (no on_reconnect yet — wired after router) ──
    connection = SerialConnection()
    try:
        connection.connect()
        logger.info(f"[Main] Serial: {connection.port_path} @ {connection.baud_rate}")
    except SerialConnectionError as exc:
        logger.critical(f"[Main] Serial failed: {exc}")
        sys.exit(1)

    # ── 2. Serial router — THE FIX ────────────────────────────────────
    #    Must be created BEFORE CommandEngine and TelemetryEngine.
    #    Owns the single read_line() call loop.
    router = SerialRouter(connection)

    # Wire reconnect hook: when serial reconnects, flush stale queue data
    connection._on_reconnect = router.reset_queues

    router.start()

    # ── 3. State manager ──────────────────────────────────────────────
    state_manager = StateManager()

    # ── 4. Telemetry engine — receives router.telemetry_queue ─────────
    #    Every line the router reads is copied here automatically.
    # ── 5. Command engine — receives router.ack_queue ─────────────────
    #    PrinterCommunicator reads acks from queue, never from read_line().
    command_engine = CommandEngine(connection, router=router)
    telemetry = TelemetryEngine(
        line_queue=router.telemetry_queue,
        state_manager=state_manager,
        command_sender=command_engine.send_fire_and_forget,
    )
    #
    # NOTE: CommandEngine.__init__ must be updated to accept `router`:
    #
    #   def __init__(self, connection: SerialConnection, router: SerialRouter):
    #       self._comm = PrinterCommunicator(connection, ack_queue=router.ack_queue)
    #       self._processor = QueueProcessor(self._comm)
    #       self._lock = threading.Lock()
    #       self._on_complete = None

    # ── 6. MQTT bridge ────────────────────────────────────────────────
    bridge = MQTTBridge(
        command_engine=command_engine,
        state_manager=state_manager,
        printer_config=PRINTER_CONFIG,
        telemetry_engine=telemetry,
    )

    # ── 7. Vision layer ───────────────────────────────────────────────
    ai_client  = AIClient()
    vision_pub = VisionEventPublisher(
        mqtt_client=bridge.mqtt_client,    # ← public property, not bridge._mqtt
        printer_id=printer_id,
    )
    monitor = VisionMonitor(
        stream_url=camera_url,
        ai_client=ai_client,
        job_manager=bridge.job_manager,    # ← public property, not bridge._jobs
        event_publisher=vision_pub,
        guard_config={
            "failure_threshold": VISION_FAILURE_THRESHOLD,
            "confidence_min":    VISION_CONFIDENCE_MIN,
            "cooldown_sec":      VISION_COOLDOWN_SEC,
        },
    )
    controller = VisionController(monitor)
    bridge.job_manager.set_state_listener(controller.on_job_state_change)

    # ── 8. Start all layers in dependency order ───────────────────────
    # router already started (step 2)
    command_engine.start()
    telemetry.start()
    bridge.start()
    # Vision activates automatically when a job transitions to PRINTING

    logger.info("[Main] All systems running.")
    logger.info("[Main]   SerialRouter   → single reader, dual-queue fan-out")
    logger.info("[Main]   TelemetryEngine→ reading telemetry_queue")
    logger.info("[Main]   CommandEngine  → reading ack_queue")
    logger.info("[Main]   MQTTBridge     → connected to HiveMQ")
    logger.info("[Main]   VisionMonitor  → activates on PRINTING jobs")

    # ── 9. Graceful shutdown on SIGINT / SIGTERM ──────────────────────
    stop = threading.Event()

    def _shutdown(sig, frame):
        logger.info(f"[Main] Signal {sig} — shutting down.")
        stop.set()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    stop.wait()

    logger.info("[Main] Stopping all layers...")
    controller.shutdown()
    bridge.stop()
    command_engine.stop()
    telemetry.stop()
    router.stop()
    connection.disconnect()
    logger.info("[Main] Clean shutdown complete.")


if __name__ == "__main__":
    main()
