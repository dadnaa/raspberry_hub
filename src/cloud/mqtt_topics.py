"""
mqtt_topics.py — Sprint 5 Updated Topic Contract

Upstream   (Pi → Cloud): handshake, printer-state, job-state, command-state
Downstream (Cloud → Pi): command, start-job, pause-job, resume-job, cancel-job
"""


class MQTTTopics:
    def __init__(self, printer_id: str) -> None:
        self._id = printer_id

    # ── Upstream (publish) ────────────────────────────────────────────

    @property
    def handshake(self) -> str:
        return f"printers/{self._id}/handshake"

    @property
    def printer_state(self) -> str:
        return f"printers/{self._id}/printer-state"

    @property
    def job_state(self) -> str:
        return f"printers/{self._id}/jobs/job-state"

    @property
    def command_state(self) -> str:
        return f"printers/{self._id}/command-state"

    # ── Downstream (subscribe) ────────────────────────────────────────

    @property
    def command(self) -> str:
        return f"printers/{self._id}/command"

    @property
    def start_job(self) -> str:
        return f"printers/{self._id}/start-job"

    @property
    def pause_job(self) -> str:
        return f"printers/{self._id}/pause-job"

    @property
    def resume_job(self) -> str:
        return f"printers/{self._id}/resume-job"

    @property
    def stop_job(self) -> str:
        # Deprecated alias kept for compatibility - prefer `cancel_job`
        return self.cancel_job

    @property
    def cancel_job(self) -> str:
        return f"printers/{self._id}/cancel-job"

    # ── Helpers ───────────────────────────────────────────────────────

    @property
    def all_subscriptions(self) -> list[str]:
        return [
            self.command,
            self.start_job,
            self.pause_job,
            self.resume_job,
            self.cancel_job,
        ]

    def __repr__(self) -> str:
        return f"MQTTTopics(printer_id={self._id!r})"
    
