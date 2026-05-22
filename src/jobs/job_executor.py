"""
job_executor.py — Sprint 6 Updated: Job Execution Engine

Sprint 5 logic unchanged.
Sprint 6 addition: optional state_listener callback so VisionController
reacts to every job state transition without polling.

Callback signature:  state_listener(job: Job) -> None
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from config.settings import JOB_PAUSE_POLL_INTERVAL_SEC
from config.settings import JOB_PAUSE_POLL_INTERVAL_SEC, OCTOPRINT_PRINT_START_WAIT_SEC
from src.jobs.job_model  import Job
from src.jobs.job_store  import JobStore
from src.core.models     import JobStateMessage

logger = logging.getLogger(__name__)

_PAUSE_POLL_INTERVAL = JOB_PAUSE_POLL_INTERVAL_SEC
_COMMAND_YIELD_SEC = 0.01


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobExecutor:
    def __init__(
        self,
        job:             Job,
        printer_gateway=None,
        store:           Optional[JobStore] = None,
        publish_state:   Optional[Callable[[JobStateMessage], None]] = None,
        printer_id:      Optional[str] = None,
        on_finished:     Optional[Callable[[Job], None]] = None,
        state_listener:  Optional[Callable[[Job], None]] = None,
        command_engine=None,
    ) -> None:
        gateway = printer_gateway if printer_gateway is not None else command_engine
        if gateway is None:
            raise ValueError("JobExecutor requires printer_gateway.")
        if store is None:
            raise ValueError("JobExecutor requires store.")
        if publish_state is None:
            raise ValueError("JobExecutor requires publish_state.")
        if printer_id is None:
            raise ValueError("JobExecutor requires printer_id.")

        self._job            = job
        self._printer_gateway = gateway
        self._store          = store
        self._publish        = publish_state
        self._printer_id     = printer_id
        self._on_finished    = on_finished
        self._state_listener = state_listener

        self._pause_event  = threading.Event()
        self._cancel_event = threading.Event()
        self._fail_event   = threading.Event()
        self._fail_reason: Optional[str] = None
        self._thread:      Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._cancel_event.clear()
        self._pause_event.clear()
        self._fail_event.clear()
        self._fail_reason = None
        self._thread = threading.Thread(
            target=self._stream_loop,
            name=f"JobExec-{self._job.job_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    def pause(self) -> None:
        if self._job.status == "PRINTING":
            self._pause_event.set()
            self._job.mark_paused()
            self._persist_and_publish()

    def resume(self) -> None:
        if self._job.status == "PAUSED":
            self._pause_event.clear()
            self._job.mark_printing()
            self._persist_and_publish()

    def cancel(self) -> None:
        self._cancel_event.set()
        self._pause_event.clear()
        self._fail_event.clear()
        if not self._job.is_terminal:
            self._job.mark_cancelled()
            self._persist_and_publish()

    def fail(self, reason: str) -> None:
        self._fail_reason = reason
        self._fail_event.set()
        self._pause_event.clear()

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def _stream_loop(self) -> None:
        job = self._job
        if self._fail_event.is_set():
            self._fail_job()
            return

        if self._cancel_event.is_set() or job.status == "CANCELLED":
            job.mark_cancelled()
            self._persist_and_publish(); self._fire_finished(); return

        # If the gateway exposes file upload/print methods, use the higher-level flow
        upload_fn = getattr(self._printer_gateway, "upload_file", None)
        print_fn = getattr(self._printer_gateway, "print_file", None)
        get_job_fn = getattr(self._printer_gateway, "get_job", None)
        if upload_fn and print_fn and get_job_fn:
            job.mark_loading()
            self._persist_and_publish()
            try:
                filename = upload_fn(job.file_url)
                ok = print_fn(filename)
                if not ok:
                    raise RuntimeError("print_file request failed")
            except Exception as exc:
                job.mark_failed(reason=str(exc))
                self._persist_and_publish(); self._fire_finished(); return

            # Wait until OctoPrint reports the newly-selected file as active
            # before marking the job as PRINTING. This avoids premature
            # activation of vision/other systems when OctoPrint did not yet
            # switch files or the cancellation of a previous job is still
            # settling on the server.
            start_wait = time.time()
            selected = False
            while time.time() - start_wait < OCTOPRINT_PRINT_START_WAIT_SEC:
                try:
                    status = get_job_fn()
                except Exception:
                    status = {}
                # OctoPrint /api/job includes the selected file under
                # the `job.file.name` path in many setups. Be defensive.
                try:
                    job_file = (
                        (status.get("job") or {}).get("file") or {}
                    ).get("name")
                except Exception:
                    job_file = None
                if job_file and job_file == filename:
                    selected = True
                    break
                # if state is not printing and no file yet, keep waiting
                time.sleep(0.2)

            if not selected:
                logger.warning("[Executor] print_file reported ok but OctoPrint did not report %s as selected within %s seconds", filename, OCTOPRINT_PRINT_START_WAIT_SEC)

            job.mark_printing()
            self._persist_and_publish()

            try:
                # Poll OctoPrint job state for progress/completion
                while True:
                    if self._fail_event.is_set():
                        self._fail_job()
                        return

                    if self._cancel_event.is_set():
                        self._safe_stop(); job.mark_cancelled()
                        self._persist_and_publish(); self._fire_finished(); return

                    try:
                        status = get_job_fn()
                    except Exception:
                        status = {}

                    completion = None
                    try:
                        completion = status.get("progress", {}).get("completion")
                    except Exception:
                        completion = None

                    if completion is not None:
                        try:
                            pct = float(completion)
                        except Exception:
                            pct = 0.0
                        job.progress = round(min(100.0, pct), 2)
                        # approximate current line index from completion
                        if job.total_lines > 0:
                            job.current_line_index = int(round((job.progress / 100.0) * job.total_lines))
                        self._persist_and_publish()

                    if completion is not None and float(completion) >= 100.0:
                        if not self._cancel_event.is_set() and not self._fail_event.is_set():
                            job.mark_completed()
                            self._persist_and_publish(); self._fire_finished()
                        return

                    time.sleep(_COMMAND_YIELD_SEC)

            except Exception as exc:
                logger.exception(f"[Executor] Unexpected during file-print: {exc}")
                job.mark_failed(reason=str(exc))
                self._persist_and_publish(); self._fire_finished()
            return

        # Legacy streaming removed — require upload/print-capable gateway.
        job.mark_failed(reason="Printer gateway does not support file upload/print")
        self._persist_and_publish()
        self._fire_finished()

    def _do_pause(self) -> None:
        job = self._job
        try: self._pause_printer()
        except Exception: pass
        job.mark_paused()
        self._persist_and_publish()
        while (
            self._pause_event.is_set()
            and not self._cancel_event.is_set()
            and not self._fail_event.is_set()
        ):
            time.sleep(_PAUSE_POLL_INTERVAL)
        if not self._cancel_event.is_set() and not self._fail_event.is_set():
            try: self._resume_printer()
            except Exception: pass
            job.mark_printing()
            self._persist_and_publish()

    def _fail_job(self) -> None:
        if self._job.is_terminal:
            return

        self._safe_stop()
        self._job.mark_failed(reason=self._fail_reason or "Job failed")
        self._persist_and_publish()
        self._fire_finished()

    def _safe_stop(self) -> None:
        if hasattr(self._printer_gateway, "cancel"):
            try:
                if self._printer_gateway.cancel():
                    return
            except Exception:
                pass
        for cmd in ("M25", "M104 S0", "M140 S0", "M84"):
            try: self._printer_gateway.send(cmd)
            except Exception: pass

    def _pause_printer(self) -> None:
        if hasattr(self._printer_gateway, "pause") and self._printer_gateway.pause():
            return
        self._printer_gateway.send("M25")

    def _resume_printer(self) -> None:
        if hasattr(self._printer_gateway, "resume") and self._printer_gateway.resume():
            return
        self._printer_gateway.send("M24")

    def _persist_and_publish(self) -> None:
        job = self._job
        try: self._store.save(job)
        except Exception: logger.exception("[Executor] Persist failed.")

        msg = JobStateMessage(
            jobId=job.job_id, printerId=self._printer_id,
            fileUrl=job.file_url, status=job.mqtt_status, progress=job.progress,
            startedAt=job.started_at, finishedAt=job.finished_at,
            estimatedTime=job.estimated_remaining_display,
            reason=job.failure_reason,
        )
        try: self._publish(msg)
        except Exception: logger.exception("[Executor] Publish failed.")

        if self._state_listener:
            try: self._state_listener(job)
            except Exception: logger.exception("[Executor] state_listener raised.")

    def _fire_finished(self) -> None:
        if self._on_finished:
            try: self._on_finished(self._job)
            except Exception: logger.exception("[Executor] on_finished raised.")
