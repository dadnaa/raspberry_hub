"""
mqtt_publisher.py — Structured Upstream Publisher

Handles all Pi -> Cloud messages.
Every method takes a model instance and publishes to the correct topic.
Timestamps are injected here, not in the model constructors.

Topics published:
  printers/{id}/handshake
  printers/{id}/printer-state
  printers/{id}/jobs/job-state
  printers/{id}/command-state
"""

import logging
from datetime import datetime, timezone

from src.cloud.mqtt_client  import MQTTClient
from src.cloud.mqtt_topics  import MQTTTopics
from src.core.models import (
    HandshakeMessage,
    PrinterStateMessage,
    JobStateMessage,
    CommandResponseMessage,
)

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MQTTPublisher:
    """
    Thin publish facade.  All outgoing model serialization lives here.

    Usage:
        publisher = MQTTPublisher(mqtt_client, topics)
        publisher.handshake(msg)
        publisher.printer_state(msg)
        publisher.command_response(resp)
    """

    def __init__(self, client: MQTTClient, topics: MQTTTopics) -> None:
        self._client = client
        self._topics = topics
        # Cache last published job-state per jobId to avoid duplicate MQTT messages
        self._last_job_state: dict[str, tuple] = {}

    def handshake(self, msg: HandshakeMessage) -> None:
        payload = msg.to_json()
        self._client.publish(self._topics.handshake, payload, retain=True)
        logger.info(f"[Publisher] Handshake sent for printer={msg.printerId}")

    def printer_state(self, msg: PrinterStateMessage) -> None:
        payload = msg.to_json()
        self._client.publish(self._topics.printer_state, payload)
        logger.debug(f"[Publisher] printer-state: {msg.status}")

    def job_state(self, msg: JobStateMessage) -> None:
        # Build a stable key for deduplication
        key = (
            msg.jobId,
            msg.status,
            float(msg.progress or 0.0),
            msg.startedAt,
            msg.finishedAt,
            msg.estimatedTime,
            msg.reason,
        )
        last = self._last_job_state.get(msg.jobId)
        if last == key:
            logger.debug("[Publisher] job-state unchanged for %s; skipping publish", msg.jobId)
            return
        self._last_job_state[msg.jobId] = key

        payload = msg.to_json()
        self._client.publish(self._topics.job_state, payload)
        logger.debug(f"[Publisher] job-state: {msg.status} ({msg.progress:.1f}%)")

    def command_response(self, msg: CommandResponseMessage) -> None:
        msg.timestamp = _utc_now()
        payload = msg.to_json()
        self._client.publish(self._topics.command_state, payload)
        logger.info(
            f"[Publisher] command-state: {msg.commandName} -> {msg.status}"
            + (f" ({msg.reason})" if msg.reason else "")
        )
