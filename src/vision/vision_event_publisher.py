"""
vision_event_publisher.py — Sprint 6: MQTT Vision Event Publisher

Publishes structured vision classification events to:
  fleet/{printerId}/vision/events

Payload:
{
  "jobId":          "...",
  "printerId":      "...",
  "timestamp":      "...",
  "classification": "OK | FAILURE | UNCERTAIN",
  "confidence":     0.0,
  "action":         "NONE | PAUSED"
}

This module is REPORTING ONLY — it never triggers any action.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from src.vision.failure_guard import VisionDecision, Action

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class VisionEventPublisher:
    """
    Thin MQTT publish wrapper for vision events.

    Args:
        mqtt_client – MQTTClient instance (Sprint 4)
        printer_id  – device identifier
    """

    def __init__(self, mqtt_client, printer_id: str) -> None:
        self._client     = mqtt_client
        self._printer_id = printer_id

    def publish(
        self,
        decision:   VisionDecision,
        job_id:     str,
        timestamp:  Optional[str] = None,
    ) -> None:
        topic = f"fleet/{self._printer_id}/vision/events"
        action_str = "PAUSED" if decision.action == Action.PAUSE else "NONE"

        payload = json.dumps({
            "jobId":          job_id,
            "printerId":      self._printer_id,
            "timestamp":      timestamp or _utc_now(),
            "classification": decision.classification,
            "confidence":     round(decision.confidence, 4),
            "action":         action_str,
        })

        self._client.publish(topic, payload, qos=1)
        logger.info(
            f"[VisionEvent] {decision.classification} "
            f"(conf={decision.confidence:.2f}) action={action_str}"
        )