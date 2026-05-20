"""
ai_client.py — Sprint 6: AI Inference Integration Layer

Sends JPEG frames + metadata to an external AI inference service
and returns a structured AIInferenceResult.

Protocol:
  POST {AI_ENDPOINT}
  Content-Type: multipart/form-data
    - image:    JPEG bytes
    - metadata: JSON string

Response JSON:
  {
    "classification": "OK" | "FAILURE" | "UNCERTAIN",
    "confidence":     0.0–1.0
  }

All calls are async (run in the caller's thread).
Timeouts are enforced — never blocks the vision pipeline indefinitely.
"""

import json
import logging
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Optional

from config.settings import (
    VISION_AI_ENDPOINT_ENV,
    VISION_AI_TIMEOUT_SEC,
    VISION_DEFAULT_AI_ENDPOINT,
)

logger = logging.getLogger(__name__)

# Environment variable for endpoint URL
_AI_ENDPOINT_ENV = VISION_AI_ENDPOINT_ENV
_DEFAULT_ENDPOINT = VISION_DEFAULT_AI_ENDPOINT
_TIMEOUT_SEC = VISION_AI_TIMEOUT_SEC

Classification = str   # "OK" | "FAILURE" | "UNCERTAIN"


@dataclass
class AIInferenceResult:
    """Structured result from the AI inference service."""
    classification: Classification   # "OK" | "FAILURE" | "UNCERTAIN"
    confidence:     float            # 0.0 – 1.0
    raw_response:   Optional[dict]   = None

    @property
    def is_failure(self) -> bool:
        return self.classification == "FAILURE"

    @property
    def is_ok(self) -> bool:
        return self.classification == "OK"

    @property
    def is_uncertain(self) -> bool:
        return self.classification == "UNCERTAIN"

    @staticmethod
    def timeout_result() -> "AIInferenceResult":
        return AIInferenceResult(classification="UNCERTAIN", confidence=0.0)

    @staticmethod
    def error_result(reason: str = "") -> "AIInferenceResult":
        logger.warning(f"[AIClient] Error result: {reason}")
        return AIInferenceResult(classification="UNCERTAIN", confidence=0.0)


def _build_multipart(jpeg_bytes: bytes, metadata: dict) -> tuple[bytes, str]:
    boundary = b"VisionBoundary7f3a9b"          # no leading dashes
    delimiter = b"--" + boundary                # standard: "--" prefix in body

    meta_str = json.dumps(metadata).encode()

    parts = [
        delimiter + b"\r\n",
        b'Content-Disposition: form-data; name="metadata"\r\n',
        b"Content-Type: application/json\r\n\r\n",
        meta_str + b"\r\n",
        delimiter + b"\r\n",
        b'Content-Disposition: form-data; name="image"; filename="frame.jpg"\r\n',
        b"Content-Type: image/jpeg\r\n\r\n",
        jpeg_bytes + b"\r\n",
        delimiter + b"--\r\n",                  # closing delimiter
    ]
    body = b"".join(parts)
    content_type = f"multipart/form-data; boundary={boundary.decode()}"
    return body, content_type

class AIClient:
    """
    HTTP client for the external AI inference service.

    Usage:
        client = AIClient()
        result = client.infer(jpeg_bytes, metadata)
    """

    def __init__(self, endpoint: Optional[str] = None) -> None:
        self._endpoint = endpoint or os.environ.get(_AI_ENDPOINT_ENV, _DEFAULT_ENDPOINT)
        logger.info(f"[AIClient] Endpoint: {self._endpoint}")

    def infer(self, jpeg_bytes: bytes, metadata: dict) -> AIInferenceResult:
        """
        Send frame to AI service and return classification result.
        Always returns an AIInferenceResult — never raises.
        """
        try:
            body, content_type = _build_multipart(jpeg_bytes, metadata)
            req = urllib.request.Request(
                self._endpoint,
                data=body,
                headers={
                    "Content-Type":   content_type,
                    "Content-Length": str(len(body)),
                    "User-Agent":     "rasp-arch-vision/1.0",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=_TIMEOUT_SEC) as resp:
                raw = json.loads(resp.read().decode("utf-8"))

            classification = raw.get("classification", "UNCERTAIN").upper()
            if classification not in ("OK", "FAILURE", "UNCERTAIN"):
                classification = "UNCERTAIN"
            confidence = float(raw.get("confidence", 0.0))

            result = AIInferenceResult(
                classification=classification,
                confidence=confidence,
                raw_response=raw,
            )
            logger.debug(f"[AIClient] {result.classification} ({result.confidence:.2f})")
            return result

        except TimeoutError:
            logger.warning("[AIClient] Request timed out.")
            return AIInferenceResult.timeout_result()
        except urllib.error.URLError as exc:
            return AIInferenceResult.error_result(str(exc))
        except Exception as exc:
            return AIInferenceResult.error_result(str(exc))
