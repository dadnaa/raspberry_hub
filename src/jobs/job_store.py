"""
job_store.py — Sprint 5: Persistent Job State Store

Persists job state to disk as JSON files so the system can
recover from crashes or restarts without losing execution position.

Storage layout:
  data/jobs/<job_id>.json    — one file per job

Thread-safety: all public methods use a single lock.
"""

import json
import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional

from src.jobs.job_model import Job

logger = logging.getLogger(__name__)

_DEFAULT_DIR = Path("data/jobs")


class JobStore:
    """
    Persistent key-value store for Job objects.

    Usage:
        store = JobStore()
        store.save(job)
        job = store.load(job_id)
        store.delete(job_id)
        all_jobs = store.load_all()
    """

    def __init__(self, storage_dir: Path = _DEFAULT_DIR) -> None:
        self._dir  = storage_dir
        self._lock = threading.Lock()
        self._dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[JobStore] Storage directory: {self._dir.resolve()}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, job: Job) -> None:
        """Atomically persist a Job to disk."""
        path = self._path(job.job_id)
        tmp  = path.with_suffix(".tmp")
        with self._lock:
            try:
                tmp.write_text(job.to_json(), encoding="utf-8")
                tmp.replace(path)       # atomic on POSIX (rename)
            except Exception:
                logger.exception(f"[JobStore] Failed to save job {job.job_id}")
                raise

    def load(self, job_id: str) -> Optional[Job]:
        """Load a Job by ID. Returns None if not found."""
        path = self._path(job_id)
        with self._lock:
            if not path.exists():
                return None
            try:
                return Job.from_json(path.read_text(encoding="utf-8"))
            except Exception:
                logger.exception(f"[JobStore] Corrupt job file: {path}")
                return None

    def load_all(self) -> List[Job]:
        """Load all persisted jobs. Skips corrupt files with a warning."""
        jobs = []
        with self._lock:
            for p in sorted(self._dir.glob("*.json")):
                try:
                    jobs.append(Job.from_json(p.read_text(encoding="utf-8")))
                except Exception:
                    logger.warning(f"[JobStore] Skipping corrupt file: {p}")
        return jobs

    def delete(self, job_id: str) -> None:
        """Remove a job from disk (used for cleanup, not cancellation)."""
        path = self._path(job_id)
        with self._lock:
            if path.exists():
                path.unlink()
                logger.info(f"[JobStore] Deleted job {job_id}")

    def exists(self, job_id: str) -> bool:
        return self._path(job_id).exists()

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _path(self, job_id: str) -> Path:
        # Sanitise job_id to prevent path traversal
        safe = "".join(c for c in job_id if c.isalnum() or c in "-_")
        return self._dir / f"{safe}.json"