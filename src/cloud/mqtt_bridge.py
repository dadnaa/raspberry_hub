"""
mqtt_bridge.py — Sprint 5 Updated MQTT Bridge

Extends Sprint 4 bridge with:
    - Job control dispatch: start-job, pause-job, resume-job, cancel-job
  - JobManager integration
  - Startup recovery via JobManager.recover()
"""

import json
import logging
import threading
import time
from typing import Optional

from config.settings import MQTT_STATE_PUBLISH_INTERVAL_SEC
from src.cloud.mqtt_client       import MQTTClient
from src.cloud.mqtt_topics       import MQTTTopics
from src.cloud.mqtt_publisher    import MQTTPublisher
from src.cloud.message_validator import MessageValidator
from src.cloud.command_router    import CommandRouter
from src.jobs.job_manager       import JobManager
from src.core.models import (
    HandshakeMessage,
    PrinterStateMessage,
    StartJobMessage,
    PauseJobMessage,
    ResumeJobMessage,
    CancelJobMessage,
)
from src.telemetry.printer_state import PrinterStatus

logger = logging.getLogger(__name__)

_STATE_PUBLISH_INTERVAL_SEC = MQTT_STATE_PUBLISH_INTERVAL_SEC


class PrinterConfig:
    def __init__(self, name: str, model: str, nozzle_diameter: float) -> None:
        self.name            = name
        self.model           = model
        self.nozzle_diameter = nozzle_diameter


class MQTTBridge:
    """
    Sprint 5 orchestrator. Wires together:
      MQTTClient, MQTTTopics, MQTTPublisher, MessageValidator,
      CommandRouter, JobManager, StateManager.
    """

    def __init__(
        self,
        command_engine,
        state_manager,
        printer_config: PrinterConfig,
        job_manager:    Optional[JobManager] = None,
        telemetry_engine: Optional[object] = None,
    ) -> None:
        self._engine  = command_engine
        self._state   = state_manager
        self._config  = printer_config

        self._mqtt   = MQTTClient()
        self._topics = MQTTTopics(self._mqtt.printer_id)
        self._pub    = MQTTPublisher(self._mqtt, self._topics)
        self._val    = MessageValidator(self._mqtt.printer_id)
        self._router = CommandRouter(
            topics=self._topics,
            publisher=self._pub,
            validator=self._val,
            command_engine=command_engine,
        )

        # JobManager — created here if not injected
        self._jobs = job_manager or JobManager(
            command_engine=command_engine,
            publish_state=self._pub.job_state,
            printer_id=self._mqtt.printer_id,
        )
        # Optional TelemetryEngine instance used for suppressing immediate
        # telemetry-driven PRINTING transitions after a cancel.
        self._telemetry = telemetry_engine

        self._stop_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None

        self._mqtt.set_message_handler(self._dispatch_message)
        self._state.register_listener(self._on_state_change)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        for topic in self._topics.all_subscriptions:
            self._mqtt.subscribe(topic)

        self._mqtt.start()
        threading.Thread(target=self._wait_and_handshake, daemon=True).start()

        # Recover interrupted jobs from previous run
        recovered = self._jobs.recover()
        if recovered:
            logger.info(f"[Bridge] Recovered {recovered} job(s) from disk.")

        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="MQTTHeartbeat", daemon=True
        )
        self._heartbeat_thread.start()
        logger.info("[Bridge] MQTT bridge started (Sprint 5).")

    def stop(self) -> None:
        self._stop_event.set()
        self._mqtt.stop()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)
        logger.info("[Bridge] MQTT bridge stopped.")

    # ------------------------------------------------------------------
    # Incoming message dispatch
    # ------------------------------------------------------------------

    def _dispatch_message(self, topic: str, payload: str) -> None:
        logger.debug(f"[Bridge] Dispatch: {topic}")

        if topic == self._topics.command:
            self._router.handle_command(payload)

        elif topic == self._topics.start_job:
            self._handle_start_job(payload)

        elif topic == self._topics.pause_job:
            self._handle_pause_job(payload)

        elif topic == self._topics.resume_job:
            self._handle_resume_job(payload)

        elif topic == self._topics.cancel_job:
            self._handle_cancel_job(payload)

        else:
            logger.warning(f"[Bridge] Unknown topic (ignored): {topic!r}")

    # ------------------------------------------------------------------
    # Job control handlers
    # ------------------------------------------------------------------

    def _handle_start_job(self, payload: str) -> None:
        try:
            msg = StartJobMessage.from_json(payload)
        except Exception as exc:
            logger.warning(f"[Bridge] Invalid start-job payload: {exc}")
            return
        if not self._is_for_this_printer(msg, "start-job"):
            return

        threading.Thread(
            target=self._submit_job_async,
            args=(msg,),
            daemon=True,
        ).start()

    def _submit_job_async(self, msg: StartJobMessage) -> None:
        try:
            job = self._jobs.submit(msg)
            logger.info(f"[Bridge] Job submitted: {job.job_id}")
        except Exception as exc:
            logger.error(f"[Bridge] Job submission failed: {exc}")

    def _handle_pause_job(self, payload: str) -> None:
        try:
            msg = PauseJobMessage.from_json(payload)
        except Exception as exc:
            logger.warning(f"[Bridge] Invalid pause-job: {exc}")
            return
        if not self._is_for_this_printer(msg, "pause-job"):
            return
        ok = self._jobs.pause(msg.jobId)
        logger.info(f"[Bridge] pause({msg.jobId}) -> {'ok' if ok else 'not found'}")

    def _handle_resume_job(self, payload: str) -> None:
        try:
            msg = ResumeJobMessage.from_json(payload)
        except Exception as exc:
            logger.warning(f"[Bridge] Invalid resume-job: {exc}")
            return
        if not self._is_for_this_printer(msg, "resume-job"):
            return
        ok = self._jobs.resume(msg.jobId)
        logger.info(f"[Bridge] resume({msg.jobId}) -> {'ok' if ok else 'not found'}")

    def _handle_cancel_job(self, payload: str) -> None:
        try:
            msg = CancelJobMessage.from_json(payload)
        except Exception as exc:
            logger.warning(f"[Bridge] Invalid cancel-job: {exc}")
            return
        if not self._is_for_this_printer(msg, "cancel-job"):
            return
        ok = self._jobs.cancel(msg.jobId)
        logger.info(f"[Bridge] cancel({msg.jobId}) -> {'ok' if ok else 'not found'}")
        # If cancel succeeded and there is no active or queued job, ensure
        # printer state is reported as IDLE (not PRINTING/PAUSED).
        try:
            if ok and not self._jobs.active_job and self._jobs.queue_length == 0:
                from src.telemetry.printer_state import PrinterStatus as _PS
                # Update StateManager; listeners (including this bridge) will publish state.
                self._state.update(status=_PS.IDLE)
                # Also ask telemetry engine to suppress immediate PRINTING
                # transitions for a short window while the safe-stop completes.
                try:
                    if self._telemetry:
                        self._telemetry.suppress_printing(2.0)
                except Exception:
                    logger.exception("[Bridge] telemetry suppress failed")
        except Exception:
            logger.exception("[Bridge] Could not publish idle state after cancel.")

    def _is_for_this_printer(self, msg, message_type: str) -> bool:
        if msg.printerId == self._mqtt.printer_id:
            return True
        logger.warning(
            f"[Bridge] {message_type} printerId mismatch: {msg.printerId!r}"
        )
        return False

    # ------------------------------------------------------------------
    # Upstream publishing
    # ------------------------------------------------------------------

    def _wait_and_handshake(self) -> None:
        while not self._mqtt.is_connected:
            time.sleep(0.5)
        self._send_handshake()

    def _send_handshake(self) -> None:
        msg = HandshakeMessage(
            printerId=self._mqtt.printer_id,
            name=self._config.name,
            model=self._config.model,
            nozzleDiameter=self._config.nozzle_diameter,
        )
        self._pub.handshake(msg)

    def _on_state_change(self, snapshot, changed_fields: dict) -> None:
        self._publish_printer_state(snapshot)

    def _publish_printer_state(self, snapshot=None) -> None:
        if snapshot is None:
            snapshot = self._state.get_snapshot()
        status_map = {
            PrinterStatus.IDLE:      "IDLE",
            PrinterStatus.PRINTING:  "PRINTING",
            PrinterStatus.PAUSED:    "PAUSED",
            PrinterStatus.ERROR:     "OFFLINE",
            PrinterStatus.REBOOTING: "OFFLINE",
            PrinterStatus.UNKNOWN:   "OFFLINE",
        }
        msg = PrinterStateMessage(
            printerId=self._mqtt.printer_id,
            name=self._config.name,
            model=self._config.model,
            status=status_map.get(snapshot.status, "OFFLINE"),
            nozzleDiameter=self._config.nozzle_diameter,
            nozzleTemp=snapshot.nozzle_temp or 0.0,
            bedTemp=snapshot.bed_temp or 0.0,
        )
        self._pub.printer_state(msg)

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=_STATE_PUBLISH_INTERVAL_SEC)
            if self._stop_event.is_set():
                break
            if self._mqtt.is_connected:
                self._publish_printer_state()
    @property
    def job_manager(self) -> JobManager:
       """The JobManager instance owned by this bridge."""
       return self._jobs

    @property
    def mqtt_client(self) -> MQTTClient:
        """The MQTTClient instance owned by this bridge."""
        return self._mqtt
