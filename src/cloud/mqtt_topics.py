"""
mqtt_topics.py — Sprint 5 Updated Topic Contract

Upstream   (Pi → Cloud): handshake, printer-state, job-state, command-state
Downstream (Cloud → Pi): command, start-job, pause-job, resume-job, stop-job
"""


class MQTTTopics:
    def __init__(self, printer_id: str) -> None:
        self._id = printer_id

    # ── Upstream (publish) ────────────────────────────────────────────

    @property
    def handshake(self) -> str:
        return f"printer/{self._id}/handshake"

    @property
    def printer_state(self) -> str:
        return f"printer/{self._id}/printer-state"

    @property
    def job_state(self) -> str:
        return f"printer/{self._id}/job-state"

    @property
    def command_state(self) -> str:
        return f"printer/{self._id}/command-state"

    # ── Downstream (subscribe) ────────────────────────────────────────

    @property
    def command(self) -> str:
        return f"printer/{self._id}/command"

    @property
    def start_job(self) -> str:
        return f"printer/{self._id}/start-job"

    @property
    def pause_job(self) -> str:
        return f"printer/{self._id}/pause-job"

    @property
    def resume_job(self) -> str:
        return f"printer/{self._id}/resume-job"

    @property
    def stop_job(self) -> str:
        return f"printer/{self._id}/stop-job"

    # ── Helpers ───────────────────────────────────────────────────────

    @property
    def all_subscriptions(self) -> list[str]:
        return [
            self.command,
            self.start_job,
            self.pause_job,
            self.resume_job,
            self.stop_job,
        ]

    def __repr__(self) -> str:
        return f"MQTTTopics(printer_id={self._id!r})"
    @property
    def job_manager(self) -> JobManager:
       """The JobManager instance owned by this bridge."""
       return self._jobs

    @property
    def mqtt_client(self) -> MQTTClient:
        """The MQTTClient instance owned by this bridge."""
        return self._mqtt