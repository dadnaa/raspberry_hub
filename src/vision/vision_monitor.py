"""
vision_monitor.py — Sprint 6: Top-Level Vision Monitor

Orchestrates the full vision safety pipeline:

  StreamReader  →  FrameSampler  →  AIClient  →  FailureGuard
                                                       │
                                              VisionDecision.PAUSE
                                                       │
                                               JobManager.pause()
                                                       │
                                          VisionEventPublisher (MQTT)

Activation is fully job-state-driven:
  - start_monitoring(job)  when job transitions to PRINTING
  - stop_monitoring()      when job leaves PRINTING state

Now uses FULL lifecycle control:
  - StreamReader is created per job
  - StreamReader is destroyed on stop
  - No persistent camera connection
"""

import logging
import threading
from typing import Optional

from config.settings import (
    VISION_CONFIDENCE_MIN,
    VISION_COOLDOWN_SEC,
    VISION_FAILURE_THRESHOLD,
    VISION_INTERVAL_NORMAL_SEC,
    VISION_INTERVAL_RISK_SEC,
    VISION_INTERVAL_STABLE_SEC,
    VISION_OK_STREAK_FOR_SLOW,
)

from src.vision.stream_reader import StreamReader
from src.vision.frame_sampler import FrameSampler
from src.vision.ai_client import AIClient, AIInferenceResult
from src.vision.failure_guard import FailureGuard, Action
from src.vision.vision_event_publisher import VisionEventPublisher
from src.jobs.job_model import Job

logger = logging.getLogger(__name__)


_INTERVAL_RISK_SEC = VISION_INTERVAL_RISK_SEC
_INTERVAL_NORMAL_SEC = VISION_INTERVAL_NORMAL_SEC
_INTERVAL_STABLE_SEC = VISION_INTERVAL_STABLE_SEC
_OK_STREAK_FOR_SLOW = VISION_OK_STREAK_FOR_SLOW


class VisionMonitor:
    """
    State-driven vision safety monitor.
    """

    def __init__(
        self,
        stream_url: str,
        ai_client: AIClient,
        job_manager,
        event_publisher: VisionEventPublisher,
        camera_id: str = "cam-0",
        guard_config: dict = None,
    ) -> None:
        self._url = stream_url
        self._ai = ai_client
        self._jobs = job_manager
        self._pub = event_publisher
        self._camera_id = camera_id

        cfg = guard_config or {}
        self._guard = FailureGuard(
            failure_threshold=cfg.get("failure_threshold", VISION_FAILURE_THRESHOLD),
            confidence_min=cfg.get("confidence_min", VISION_CONFIDENCE_MIN),
            cooldown_sec=cfg.get("cooldown_sec", VISION_COOLDOWN_SEC),
        )

        self._reader: Optional[StreamReader] = None
        self._sampler: Optional[FrameSampler] = None
        self._active_job: Optional[Job] = None
        self._lock = threading.Lock()
        self._ok_streak = 0

    # ------------------------------------------------------------------
    # Lifecycle control
    # ------------------------------------------------------------------

    def start_monitoring(self, job: Job) -> None:
        """
        Start full vision pipeline for a PRINTING job.
        Always creates a fresh stream connection.
        """
        with self._lock:
            if self._sampler is not None:
                logger.warning("[Vision] Already monitoring a job.")
                return

            logger.info(f"[Vision] Starting monitoring for job {job.job_id!r}")

            self._active_job = job
            self._guard.reset()
            self._ok_streak = 0

            # ALWAYS create fresh stream per job
            self._reader = StreamReader(self._url)
            self._reader.start()

            self._sampler = FrameSampler(
                reader=self._reader,
                on_frame=self._on_frame,
                interval=_INTERVAL_NORMAL_SEC,
                job_id=job.job_id,
                printer_id=job.printer_id,
                camera_id=self._camera_id,
            )
            self._sampler.start()

    def stop_monitoring(self) -> None:
        """
        Fully stop vision pipeline and release stream.
        Next job will reconnect from scratch.
        """
        with self._lock:
            logger.info("[Vision] Stopping monitoring.")

            if self._sampler is not None:
                self._sampler.stop()
                self._sampler = None

            if self._reader is not None:
                self._reader.stop()
                self._reader = None

            self._active_job = None
            self._ok_streak = 0

    def shutdown(self) -> None:
        """Full system shutdown."""
        self.stop_monitoring()
        logger.info("[Vision] Shutdown complete.")

    @property
    def is_active(self) -> bool:
        return self._sampler is not None

    # ------------------------------------------------------------------
    # Frame pipeline
    # ------------------------------------------------------------------

    def _on_frame(self, jpeg_bytes: bytes, metadata: dict) -> None:
        job = self._active_job
        if job is None:
            return

        if job.status != "PRINTING":
            return

        result: AIInferenceResult = self._ai.infer(jpeg_bytes, metadata)
        decision = self._guard.evaluate(result)

        self._adapt_interval(decision)

        self._pub.publish(
            decision=decision,
            job_id=job.job_id,
            timestamp=metadata.get("timestamp"),
        )

        if decision.action == Action.PAUSE:
            logger.warning(f"[Vision] FAILURE detected → failed job {job.job_id!r}")

            try:
                failed = self._jobs.fail(
                    job.job_id,
                    reason=f"Vision failure detected at confidence {decision.confidence:.2f}",)
                if failed:
                    logger.warning(f"[Vision] Job {job.job_id!r} failed due to vision detection.")
                else:
                    logger.warning(f"[Vision] fail() returned False for job {job.job_id!r}.")

            except Exception:
                logger.exception("[Vision] JobManager pause failed.")

            # IMPORTANT: stop synchronously (no thread race)
            self.stop_monitoring()

    # ------------------------------------------------------------------
    # Adaptive sampling
    # ------------------------------------------------------------------

    def _adapt_interval(self, decision) -> None:
        if self._sampler is None:
            return

        if decision.classification == "OK":
            self._ok_streak += 1
            if self._ok_streak >= _OK_STREAK_FOR_SLOW:
                self._sampler.set_interval(_INTERVAL_STABLE_SEC)

        elif decision.classification == "FAILURE":
            self._ok_streak = 0
            self._sampler.set_interval(_INTERVAL_RISK_SEC)

        else:
            self._ok_streak = 0
            self._sampler.set_interval(_INTERVAL_NORMAL_SEC)
