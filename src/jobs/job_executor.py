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

from src.jobs.job_model  import Job
from src.jobs.job_store  import JobStore
from src.core.models     import JobStateMessage

logger = logging.getLogger(__name__)

_PAUSE_POLL_INTERVAL = 0.25


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobExecutor:
    def __init__(
        self,
        job:             Job,
        command_engine,
        store:           JobStore,
        publish_state:   Callable[[JobStateMessage], None],
        printer_id:      str,
        on_finished:     Optional[Callable[[Job], None]] = None,
        state_listener:  Optional[Callable[[Job], None]] = None,
    ) -> None:
        self._job            = job
        self._engine         = command_engine
        self._store          = store
        self._publish        = publish_state
        self._printer_id     = printer_id
        self._on_finished    = on_finished
        self._state_listener = state_listener

        self._pause_event  = threading.Event()
        self._cancel_event = threading.Event()
        self._thread:      Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._cancel_event.clear()
        self._pause_event.clear()
        self._thread = threading.Thread(
            target=self._stream_loop,
            name=f"JobExec-{self._job.job_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    def pause(self) -> None:
        if self._job.status == "PRINTING":
            self._pause_event.set()

    def resume(self) -> None:
        if self._job.status == "PAUSED":
            self._pause_event.clear()

    def cancel(self) -> None:
        self._cancel_event.set()
        self._pause_event.clear()

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def _stream_loop(self) -> None:
        job = self._job
        job.mark_printing()
        self._persist_and_publish()

        try:
            while job.current_line_index < job.total_lines:
                if self._cancel_event.is_set():
                    self._safe_stop(); job.mark_cancelled()
                    self._persist_and_publish(); self._fire_finished(); return

                if self._pause_event.is_set():
                    self._do_pause()
                    if self._cancel_event.is_set():
                        self._safe_stop(); job.mark_cancelled()
                        self._persist_and_publish(); self._fire_finished(); return
                    continue

                gcode = job.next_line
                if not gcode:
                    break

                try:
                    result = self._engine.send(gcode)
                except Exception as exc:
                    job.mark_failed(reason=str(exc))
                    self._persist_and_publish(); self._fire_finished(); return

                if not result.succeeded:
                    job.mark_failed(reason=f"Rejected {gcode!r}: {result.status.name}")
                    self._persist_and_publish(); self._fire_finished(); return

                job.current_line_index += 1
                job.update_progress()
                self._persist_and_publish()

            if not self._cancel_event.is_set():
                job.mark_completed()
                self._persist_and_publish(); self._fire_finished()

        except Exception as exc:
            logger.exception(f"[Executor] Unexpected: {exc}")
            job.mark_failed(reason=str(exc))
            self._persist_and_publish(); self._fire_finished()

    def _do_pause(self) -> None:
        job = self._job
        try: self._engine.send("M25")
        except Exception: pass
        job.mark_paused()
        self._persist_and_publish()
        while self._pause_event.is_set() and not self._cancel_event.is_set():
            time.sleep(_PAUSE_POLL_INTERVAL)
        if not self._cancel_event.is_set():
            try: self._engine.send("M24")
            except Exception: pass
            job.mark_printing()
            self._persist_and_publish()

    def _safe_stop(self) -> None:
        for cmd in ("M25", "M104 S0", "M140 S0", "M84"):
            try: self._engine.send(cmd)
            except Exception: pass

    def _persist_and_publish(self) -> None:
        job = self._job
        try: self._store.save(job)
        except Exception: logger.exception("[Executor] Persist failed.")

        msg = JobStateMessage(
            jobId=job.job_id, printerId=self._printer_id,
            fileUrl=job.file_url, status=job.status, progress=job.progress,
            startedAt=job.started_at, finishedAt=job.finished_at, estimatedTime=0,
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