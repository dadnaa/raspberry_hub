"""
vision_controller.py — Sprint 6: Vision Activation Controller

Hooks into JobManager's job lifecycle events and automatically
starts/stops the VisionMonitor based on job state transitions.

This is the glue between Sprint 5 (job lifecycle) and Sprint 6 (vision).

Activation rules:
  job.status == PRINTING  →  start_monitoring(job)
  job.status != PRINTING  →  stop_monitoring()

The controller registers itself as a job state listener via
a callback passed to JobManager.
"""

import logging
from typing import Optional

from src.vision.vision_monitor import VisionMonitor
from src.jobs.job_model        import Job
from src.core.models           import JobStateMessage

logger = logging.getLogger(__name__)


class VisionController:
    """
    Bridges JobManager state changes to VisionMonitor activation.

    Usage:
        controller = VisionController(vision_monitor)
        # Register as listener:
        job_manager.set_state_listener(controller.on_job_state_change)
    """

    def __init__(self, monitor: VisionMonitor) -> None:
        self._monitor      = monitor
        self._current_job: Optional[Job] = None

    def on_job_state_change(self, job: Job) -> None:
        """
        Called by JobExecutor on every state transition.
        This is the sole activation/deactivation hook.
        """
        if job.status == "PRINTING":
            if self._current_job is None or self._current_job.job_id != job.job_id:
                logger.info(f"[VisionCtrl] Job PRINTING — activating vision: {job.job_id!r}")
                self._current_job = job
                self._monitor.start_monitoring(job)

        elif job.status in ("PAUSED", "COMPLETED", "FAILED", "CANCELLED"):
            if self._current_job and self._current_job.job_id == job.job_id:
                logger.info(
                    f"[VisionCtrl] Job {job.status} — deactivating vision: {job.job_id!r}"
                )
                self._monitor.stop_monitoring()
                self._current_job = None

    def shutdown(self) -> None:
        self._monitor.shutdown()
