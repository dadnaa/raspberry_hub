"""
vision_monitor.py — Sprint 6: Top-Level Vision Monitor

Orchestrates the full vision safety pipeline:

  StreamReader  →  FrameSampler  →  AIClient  →  FailureGuard
                                                       │
                                              VisionDecision.PAUSE
                                                       │
                                               JobManager.pause()   ← Sprint 5
                                                       │
                                          VisionEventPublisher (MQTT)

Activation is fully job-state-driven:
  - start_monitoring(job)  when job transitions to PRINTING
  - stop_monitoring()      when job leaves PRINTING state

The monitor is a non-invasive observer. It NEVER touches serial or G-code.
"""

import logging
import threading
from typing import Optional

from src.vision.stream_reader          import StreamReader
from src.vision.frame_sampler          import FrameSampler, DEFAULT_INTERVAL_SEC
from src.vision.ai_client              import AIClient, AIInferenceResult
from src.vision.failure_guard          import FailureGuard, Action
from src.vision.vision_event_publisher import VisionEventPublisher
from src.jobs.job_model                import Job

logger = logging.getLogger(__name__)

# Adaptive interval adjustments
_INTERVAL_RISK_SEC    = 1.5   # faster sampling after a detected failure
_INTERVAL_NORMAL_SEC  = 3.0   # default
_INTERVAL_STABLE_SEC  = 5.0   # slow down after long OK streak
_OK_STREAK_FOR_SLOW   = 10    # consecutive OKs before relaxing interval


class VisionMonitor:
    """
    State-driven vision safety monitor.

    Args:
        stream_url      – IP camera RTSP / HTTP / MJPEG URL
        ai_client       – AIClient instance
        job_manager     – Sprint 5 JobManager
        event_publisher – VisionEventPublisher
        camera_id       – identifier for the camera (for metadata)
        guard_config    – dict with optional FailureGuard overrides:
                          {failure_threshold, confidence_min, cooldown_sec}
    """

    def __init__(
        self,
        stream_url:      str,
        ai_client:       AIClient,
        job_manager,
        event_publisher: VisionEventPublisher,
        camera_id:       str  = "cam-0",
        guard_config:    dict = None,
    ) -> None:
        self._url         = stream_url
        self._ai          = ai_client
        self._jobs        = job_manager
        self._pub         = event_publisher
        self._camera_id   = camera_id

        cfg = guard_config or {}
        self._guard = FailureGuard(
            failure_threshold = cfg.get("failure_threshold", 3),
            confidence_min    = cfg.get("confidence_min",    0.75),
            cooldown_sec      = cfg.get("cooldown_sec",      30.0),
        )

        self._reader:  Optional[StreamReader]  = None
        self._sampler: Optional[FrameSampler]  = None
        self._active_job: Optional[Job]        = None
        self._lock       = threading.Lock()
        self._ok_streak  = 0

    # ------------------------------------------------------------------
    # State-driven activation
    # ------------------------------------------------------------------

    def start_monitoring(self, job: Job) -> None:
        """
        Activate vision pipeline for a PRINTING job.
        Safe to call multiple times — idempotent.
        """
        with self._lock:
            if self._sampler is not None:
                logger.warning(f"[Vision] Already monitoring job {self._active_job.job_id!r}")
                return

            logger.info(f"[Vision] Starting monitoring for job {job.job_id!r}")
            self._active_job = job
            self._guard.reset()
            self._ok_streak  = 0

            # Lazy-create stream reader (keep across job changes if URL unchanged)
            if self._reader is None:
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
        Deactivate frame sampling. Keeps stream connection alive
        so reconnect is not needed when the next job starts.
        """
        with self._lock:
            if self._sampler is None:
                return
            logger.info("[Vision] Stopping monitoring.")
            self._sampler.stop()
            self._sampler    = None
            self._active_job = None

    def shutdown(self) -> None:
        """Full teardown — call on system exit."""
        self.stop_monitoring()
        with self._lock:
            if self._reader:
                self._reader.stop()
                self._reader = None
        logger.info("[Vision] Full shutdown complete.")

    @property
    def is_active(self) -> bool:
        return self._sampler is not None

    # ------------------------------------------------------------------
    # Frame processing pipeline
    # ------------------------------------------------------------------

    def _on_frame(self, jpeg_bytes: bytes, metadata: dict) -> None:
        """Called by FrameSampler for every sampled frame."""
        job = self._active_job
        if job is None:
            return

        # Guard: only process if job is still PRINTING
        if job.status != "PRINTING":
            logger.debug(f"[Vision] Job no longer PRINTING (status={job.status}) — skipping frame.")
            return

        # ── AI inference ──────────────────────────────────────────────
        result: AIInferenceResult = self._ai.infer(jpeg_bytes, metadata)

        # ── False-positive guard ──────────────────────────────────────
        decision = self._guard.evaluate(result)

        # ── Adaptive interval ─────────────────────────────────────────
        self._adapt_interval(decision)

        # ── MQTT publish ──────────────────────────────────────────────
        self._pub.publish(
            decision=decision,
            job_id=job.job_id,
            timestamp=metadata.get("timestamp"),
        )

        # ── Intervention ─────────────────────────────────────────────
        if decision.action == Action.PAUSE:
            logger.warning(
                f"[Vision] FAILURE detected — pausing job {job.job_id!r} "
                f"via JobManager."
            )
            try:
                paused = self._jobs.pause(job.job_id)
                if paused:
                    logger.info(f"[Vision] Job {job.job_id!r} paused successfully.")
                else:
                    logger.warning(f"[Vision] pause() returned False for job {job.job_id!r}.")
            except Exception:
                logger.exception("[Vision] Exception calling job_manager.pause().")
            # Stop sampling after intervention (job is now PAUSED)
            threading.Thread(target=self.stop_monitoring, daemon=True).start()

    # ------------------------------------------------------------------
    # Adaptive sampling rate
    # ------------------------------------------------------------------

    def _adapt_interval(self, decision) -> None:
        sampler = self._sampler
        if sampler is None:
            return

        if decision.classification == "OK":
            self._ok_streak += 1
            if self._ok_streak >= _OK_STREAK_FOR_SLOW:
                sampler.set_interval(_INTERVAL_STABLE_SEC)
        elif decision.classification == "FAILURE":
            self._ok_streak = 0
            sampler.set_interval(_INTERVAL_RISK_SEC)   # ramp up after first failure signal
        else:
            self._ok_streak = 0
            sampler.set_interval(_INTERVAL_NORMAL_SEC)