"""
job_manager.py — Sprint 5: Job Queue + Lifecycle Manager

Single entry point for all job operations:
  - submit()  — accept a new job (FIFO queue)
  - pause()   — pause the active job
  - resume()  — resume a paused job
  - cancel()  — cancel the active job
  - recover() — reload and resume interrupted jobs after restart

Enforces:
  - Only ONE active job per printer at a time
  - FIFO queue for waiting jobs
  - Clean executor handoff when a job finishes
  - Persistence via JobStore after every state change

Thread-safety: all public methods lock before touching shared state.
"""

import logging
import threading
from collections import deque
from typing import Callable, Deque, Dict, Optional

from src.jobs.job_model    import Job
from src.jobs.job_executor import JobExecutor
from src.jobs.job_store    import JobStore
from src.jobs.gcode_pipeline import load
from src.core.models       import JobStateMessage, StartJobMessage

logger = logging.getLogger(__name__)


class JobManager:
    """
    Manages the full job lifecycle for one printer.

    Args:
        command_engine – Sprint 2 CommandEngine
        publish_state  – MQTTPublisher.job_state callable
        printer_id     – MQTT printer identifier
        store          – JobStore (optional; defaults to new instance)
    """

    def __init__(
        self,
        command_engine,
        publish_state:  Callable[[JobStateMessage], None],
        printer_id:     str,
        store:          Optional[JobStore] = None,
    ) -> None:
        self._engine      = command_engine
        self._publish     = publish_state
        self._printer_id  = printer_id
        self._store       = store or JobStore()

        self._lock:     threading.Lock           = threading.Lock()
        self._queue:    Deque[Job]               = deque()
        self._active:   Optional[Job]            = None
        self._executor: Optional[JobExecutor]    = None
        self._state_listener: Optional[Callable[[Job], None]] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_state_listener(self, callback: Optional[Callable[[Job], None]]) -> None:
        """
        Register a callback invoked by JobExecutor on job state changes.

        Used by Sprint 6 VisionController to activate/deactivate monitoring
        without polling.
        """
        with self._lock:
            self._state_listener = callback

    def submit(self, msg: StartJobMessage) -> Job:
        """
        Accept a start-job MQTT message, load G-code, and enqueue the job.

        If no job is currently active, execution begins immediately.
        Otherwise the job is queued and starts when the active job finishes.

        Returns the created Job.
        Raises RuntimeError if G-code loading fails.
        """
        logger.info(f"[JobManager] Submitting job {msg.jobId!r} from {msg.fileUrl!r}")

        # Load + parse G-code (may raise)
        try:
            gcode_lines = load(msg.fileUrl)
        except Exception as exc:
            raise RuntimeError(f"Failed to load G-code: {exc}") from exc

        job = Job.create(
            printer_id=msg.printerId,
            file_url=msg.fileUrl,
            gcode_lines=gcode_lines,
            job_id=msg.jobId,
        )

        with self._lock:
            self._store.save(job)
            if self._active is None:
                self._start_job(job)
            else:
                logger.info(f"[JobManager] Job queued (active job running): {job.job_id}")
                self._queue.append(job)

        return job

    def pause(self, job_id: str) -> bool:
        """Pause the active job. Returns True if successful."""
        with self._lock:
            if not self._active or self._active.job_id != job_id:
                logger.warning(f"[JobManager] pause({job_id!r}) — not the active job.")
                return False
            if self._executor:
                self._executor.pause()
                return True
        return False

    def resume(self, job_id: str) -> bool:
        """Resume a paused job. Returns True if successful."""
        with self._lock:
            if not self._active or self._active.job_id != job_id:
                logger.warning(f"[JobManager] resume({job_id!r}) — not the active job.")
                return False
            if self._executor:
                self._executor.resume()
                return True
        return False

    def cancel(self, job_id: str) -> bool:
        """
        Cancel a job by ID.
        If it is the active job, stops execution immediately.
        If it is queued, removes it from the queue.
        Returns True if found and cancelled.
        """
        with self._lock:
            # Active job
            if self._active and self._active.job_id == job_id:
                if self._executor:
                    self._executor.cancel()
                logger.info(f"[JobManager] Active job cancelled: {job_id}")
                return True

            # Queued job
            for job in list(self._queue):
                if job.job_id == job_id:
                    self._queue.remove(job)
                    job.mark_cancelled()
                    self._store.save(job)
                    self._publish_state(job)
                    logger.info(f"[JobManager] Queued job cancelled: {job_id}")
                    return True

        logger.warning(f"[JobManager] cancel({job_id!r}) — job not found.")
        return False
    
    def fail(self, job_id: str, reason: str) -> bool:
        """Fail the active job. Returns True if successful."""
        with self._lock:
            if not self._active or self._active.job_id != job_id:
                logger.warning(f"[JobManager] fail({job_id!r}) - not the active job.")
                return False
            if self._executor:
                self._executor.fail(reason)
                logger.warning(f"[JobManager] Active job failed: {job_id} - {reason}")
                return True
        return False

    def recover(self) -> int:
        """Called at startup. Loads all persisted jobs and re-queues resumable ones.
        Uses persisted gcode_lines directly — never re-fetches from URL."""
        all_jobs = self._store.load_all()
        resumable = [
            j for j in all_jobs
            if j.status in ("PRINTING", "PAUSED", "QUEUED", "LOADING")
        ]
        resumable.sort(key=lambda j: j.created_at or "")

        recovered = 0
        with self._lock:
            for job in resumable:
                if not job.gcode_lines:
                    # No persisted G-code — try re-fetching as fallback
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        f"[JobManager] Job {job.job_id!r} has no persisted G-code. "
                        f"Attempting re-fetch from {job.file_url!r}."
                    )
                    try:
                        from src.jobs.gcode_pipeline import load
                        job.gcode_lines = load(job.file_url)
                        job.total_lines = len(job.gcode_lines)
                    except Exception as exc:
                        logger.error(
                            f"[JobManager] Cannot recover job {job.job_id!r} — "
                            f"G-code unavailable: {exc}"
                        )
                        job.mark_failed(f"Recovery failed: G-code unavailable: {exc}")
                        self._store.save(job)
                        continue

                # Reset to QUEUED — executor will transition to PRINTING
                job.status = "QUEUED"
                if self._active is None:
                    self._start_job(job)
                else:
                    self._queue.append(job)
                recovered += 1

        import logging
        logging.getLogger(__name__).info(
            f"[JobManager] Recovery complete. {recovered} job(s) recovered."
        )
        return recovered
 

    @property
    def active_job(self) -> Optional[Job]:
        return self._active

    @property
    def queue_length(self) -> int:
        return len(self._queue)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _start_job(self, job: Job) -> None:
        """Must be called with self._lock held."""
        self._active = job
        self._executor = JobExecutor(
            job=job,
            command_engine=self._engine,
            store=self._store,
            publish_state=self._publish,
            printer_id=self._printer_id,
            on_finished=self._on_job_finished,
            state_listener=self._state_listener,
        )
        self._executor.start()
        logger.info(f"[JobManager] Execution started: {job.job_id}")

    def _on_job_finished(self, job: Job) -> None:
        """Called by JobExecutor when a job reaches a terminal state."""
        logger.info(f"[JobManager] Job finished: {job.job_id} status={job.status}")
        with self._lock:
            self._active  = None
            self._executor = None
            # Dequeue and start next job if available
            if self._queue:
                next_job = self._queue.popleft()
                logger.info(f"[JobManager] Dequeuing next job: {next_job.job_id}")
                self._start_job(next_job)

    def _publish_state(self, job: Job) -> None:
        msg = JobStateMessage(
            jobId=job.job_id,
            printerId=self._printer_id,
            fileUrl=job.file_url,
            status=job.status,
            progress=job.progress,
            startedAt=job.started_at,
            finishedAt=job.finished_at,
            estimatedTime=job.estimated_remaining_display,
            reason=job.failure_reason,
        )
        try:
            self._publish(msg)
        except Exception:
            logger.exception("[JobManager] Failed to publish job state.")
