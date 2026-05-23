"""Production entry point for the OctoPrint-backed edge hub.

The domain layers still own cloud control, local job state, persistence, and
vision safety. Direct serial command execution and raw telemetry parsing are
replaced by `OctoPrintGateway`.
"""

import logging
import os
import signal
import sys
import threading
try:
    # Prefer to load .env automatically during local development if python-dotenv is installed
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # If python-dotenv isn't installed, continue silently — env must be provided by the runtime.
    pass

sys.path.insert(0, os.path.dirname(__file__))

from config.settings import (
    MQTT_DEFAULT_PRINTER_ID,
    MQTT_PRINTER_ID_ENV,
    OCTOPRINT_API_KEY_ENV,
    OCTOPRINT_BASE_URL_ENV,
    OCTOPRINT_DEFAULT_BASE_URL,
    OCTOPRINT_REQUEST_TIMEOUT_SEC,
    PRINTER_MODEL,
    PRINTER_NAME,
    PRINTER_NOZZLE_DIAMETER,
    VISION_CAMERA_URL_ENV,
    VISION_CONFIDENCE_MIN,
    VISION_COOLDOWN_SEC,
    VISION_DEFAULT_CAMERA_URL,
    VISION_FAILURE_THRESHOLD,
)
from src.cloud.mqtt_bridge import MQTTBridge, PrinterConfig
from src.core.state_manager import StateManager
from src.infrastructure.octoprint import OctoPrintClient, OctoPrintGateway
from src.utils.logger_setup import setup_logging
from src.vision.ai_client import AIClient
from src.vision.vision_controller import VisionController
from src.vision.vision_event_publisher import VisionEventPublisher
from src.vision.vision_monitor import VisionMonitor

setup_logging(session_name="octoprint-edge")
logger = logging.getLogger(__name__)

PRINTER_CONFIG = PrinterConfig(
    name=PRINTER_NAME,
    model=PRINTER_MODEL,
    nozzle_diameter=PRINTER_NOZZLE_DIAMETER,
)


def main() -> None:
    logger.info("=" * 60)
    logger.info("  Reactive Edge Hub - OctoPrint Adapter Runtime")
    logger.info("=" * 60)

    camera_url = os.environ.get(VISION_CAMERA_URL_ENV, VISION_DEFAULT_CAMERA_URL)
    printer_id = os.environ.get(MQTT_PRINTER_ID_ENV, MQTT_DEFAULT_PRINTER_ID)
    octoprint_url = os.environ.get(OCTOPRINT_BASE_URL_ENV, OCTOPRINT_DEFAULT_BASE_URL)
    octoprint_key = os.environ.get(OCTOPRINT_API_KEY_ENV)

    if not octoprint_key:
        logger.critical("[Main] Missing %s environment variable.", OCTOPRINT_API_KEY_ENV)
        sys.exit(1)

    state_manager = StateManager()
    octoprint_client = OctoPrintClient(
        base_url=octoprint_url,
        api_key=octoprint_key,
        timeout_sec=OCTOPRINT_REQUEST_TIMEOUT_SEC,
    )
    printer_gateway = OctoPrintGateway(
        client=octoprint_client,
        state_manager=state_manager,
    )

    bridge = MQTTBridge(
        printer_gateway=printer_gateway,
        state_manager=state_manager,
        printer_config=PRINTER_CONFIG,
    )

    ai_client = AIClient()
    vision_pub = VisionEventPublisher(
        mqtt_client=bridge.mqtt_client,
        printer_id=printer_id,
    )
    monitor = VisionMonitor(
        stream_url=camera_url,
        ai_client=ai_client,
        job_manager=bridge.job_manager,
        event_publisher=vision_pub,
        guard_config={
            "failure_threshold": VISION_FAILURE_THRESHOLD,
            "confidence_min": VISION_CONFIDENCE_MIN,
            "cooldown_sec": VISION_COOLDOWN_SEC,
        },
    )
    controller = VisionController(monitor)
    bridge.job_manager.set_state_listener(controller.on_job_state_change)

    printer_gateway.start()
    bridge.start()

    logger.info("[Main] All systems running.")
    logger.info("[Main]   OctoPrintGateway -> REST commands + state polling")
    logger.info("[Main]   MQTTBridge       -> cloud command/job control")
    logger.info("[Main]   VisionMonitor    -> activates on PRINTING jobs")

    stop = threading.Event()

    def _shutdown(sig, frame):
        logger.info("[Main] Signal %s - shutting down.", sig)
        # Attempt to cancel any active job immediately so it is not resumed
        # when the system restarts.
        try:
            if bridge and hasattr(bridge, "job_manager"):
                try:
                    active = bridge.job_manager.active_job
                    if active and not active.is_terminal:
                        logger.info(f"[Main] Shutdown: cancelling active job {active.job_id}")
                        bridge.job_manager.cancel(active.job_id)
                except Exception:
                    logger.exception("[Main] Error cancelling active job during shutdown.")
        except Exception:
            logger.exception("[Main] Error during shutdown job-cancel check.")
        stop.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    stop.wait()

    logger.info("[Main] Stopping all layers...")
    controller.shutdown()
    bridge.stop()
    printer_gateway.stop()
    logger.info("[Main] Clean shutdown complete.")


if __name__ == "__main__":
    main()
