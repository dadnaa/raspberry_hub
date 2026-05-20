"""
job_model.py — Sprint 5: Core Job Entity

The Job dataclass is the single source of truth for a print job.
It is serializable to/from JSON for persistence and MQTT publishing.

Status lifecycle:
  QUEUED -> LOADING -> PRINTING -> COMPLETED
                               -> PAUSED -> PRINTING (resume)
                               -> FAILED
                               -> CANCELLED
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import List, Optional

from src.core.models import JobStatus


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


@dataclass
class Job:
    """
    Complete representation of a print job.

    Fields set at creation:
        job_id, printer_id, file_url, gcode_lines, total_lines, created_at

    Fields mutated during execution:
        status, current_line_index, progress, started_at, paused_at,
        finished_at, failure_reason
    """
    job_id:             str
    printer_id:         str
    file_url:           str
    gcode_lines:        List[str]           # cleaned, executable lines only
    total_lines:        int
    status:             JobStatus           = "QUEUED"
    current_line_index: int                 = 0
    progress:           float               = 0.0
    created_at:         Optional[str]       = None
    started_at:         Optional[str]       = None
    paused_at:          Optional[str]       = None
    finished_at:        Optional[str]       = None
    failure_reason:     Optional[str]       = None

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @staticmethod
    def create(
        printer_id:  str,
        file_url:    str,
        gcode_lines: List[str],
        job_id:      Optional[str] = None,
    ) -> "Job":
        return Job(
            job_id=job_id or _new_id(),
            printer_id=printer_id,
            file_url=file_url,
            gcode_lines=gcode_lines,
            total_lines=len(gcode_lines),
            created_at=_utc_now(),
        )

    # ------------------------------------------------------------------
    # State transitions (mutate in place, return self for chaining)
    # ------------------------------------------------------------------

    def mark_loading(self) -> "Job":
        self.status = "LOADING"
        return self

    def mark_printing(self) -> "Job":
        self.status = "PRINTING"
        if not self.started_at:
            self.started_at = _utc_now()
        self.paused_at = None
        return self

    def mark_paused(self) -> "Job":
        self.status = "PAUSED"
        self.paused_at = _utc_now()
        return self

    def mark_completed(self) -> "Job":
        self.status     = "COMPLETED"
        self.progress   = 100.0
        self.finished_at = _utc_now()
        return self

    def mark_failed(self, reason: str) -> "Job":
        self.status         = "FAILED"
        self.failure_reason = reason
        self.finished_at    = _utc_now()
        return self

    def mark_cancelled(self) -> "Job":
        self.status     = "CANCELLED"
        self.finished_at = _utc_now()
        return self

    def update_progress(self) -> None:
        """Recalculate progress from current_line_index."""
        if self.total_lines > 0:
            self.progress = round(
                min(100.0, (self.current_line_index / self.total_lines) * 100), 2
            )

    @property
    def estimated_remaining_seconds(self) -> int:
        """Estimate remaining job time from elapsed runtime and completed lines."""
        if (
            self.status not in ("PRINTING", "PAUSED")
            or not self.started_at
            or self.current_line_index <= 0
            or self.total_lines <= 0
            or self.current_line_index >= self.total_lines
        ):
            return 0

        try:
            started = datetime.fromisoformat(self.started_at)
        except ValueError:
            return 0

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        if elapsed <= 0:
            return 0

        progress_ratio = self.current_line_index / self.total_lines
        if progress_ratio <= 0:
            return 0

        estimated_total = elapsed / progress_ratio
        remaining = max(0.0, estimated_total - elapsed)
        return int(round(remaining))

    @property
    def estimated_remaining_display(self) -> str:
        """Format estimated remaining time for MQTT job-state payloads."""
        seconds = self.estimated_remaining_seconds
        if seconds <= 0:
            return "0h 00m"

        total_minutes = (seconds + 59) // 60
        hours = total_minutes // 60
        minutes = total_minutes % 60
        return f"{hours}h {minutes:02d}m"

    @property
    def mqtt_status(self) -> str:
        """Status value used in MQTT job-state payloads."""
        if self.status == "COMPLETED":
            return "DONE"
        return self.status

    # ------------------------------------------------------------------
    # Serialization (persistence + MQTT)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @staticmethod
    def from_dict(d: dict) -> "Job":
        return Job(**d)

    @staticmethod
    def from_json(raw: str) -> "Job":
        return Job.from_dict(json.loads(raw))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def is_active(self) -> bool:
        return self.status in ("PRINTING", "PAUSED", "LOADING")

    @property
    def is_terminal(self) -> bool:
        return self.status in ("COMPLETED", "FAILED", "CANCELLED")

    @property
    def next_line(self) -> Optional[str]:
        """Return next G-code line to execute, or None if done."""
        if self.current_line_index < self.total_lines:
            return self.gcode_lines[self.current_line_index]
        return None

    def __repr__(self) -> str:
        return (
            f"Job(id={self.job_id[:8]}.. status={self.status} "
            f"progress={self.progress:.1f}% [{self.current_line_index}/{self.total_lines}])"
        )

    @property
    def has_gcode(self) -> bool:
       """True if gcode_lines is already loaded (no re-fetch needed)."""
       return bool(self.gcode_lines)
 
