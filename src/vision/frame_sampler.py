"""
frame_sampler.py — Sprint 6: Frame Sampling Engine

Pulls the latest frame from StreamReader on a configurable interval,
preprocesses it (resize + JPEG encode), and hands it to a callback.

Rules:
  - Always takes the MOST RECENT frame — no queue buildup
  - Skips the tick if previous processing is still running (non-overlapping)
  - Sampling interval is adaptive: can be tightened or relaxed at runtime
"""

import io
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional, Tuple

import cv2
import numpy as np

from config.settings import (
    VISION_DEFAULT_SAMPLE_INTERVAL_SEC,
    VISION_JPEG_QUALITY,
    VISION_MAX_SAMPLE_INTERVAL_SEC,
    VISION_MIN_SAMPLE_INTERVAL_SEC,
    VISION_TARGET_HEIGHT,
    VISION_TARGET_WIDTH,
)
from src.vision.stream_reader import StreamReader

logger = logging.getLogger(__name__)

# Default sampling parameters
DEFAULT_INTERVAL_SEC = VISION_DEFAULT_SAMPLE_INTERVAL_SEC
MIN_INTERVAL_SEC = VISION_MIN_SAMPLE_INTERVAL_SEC
MAX_INTERVAL_SEC = VISION_MAX_SAMPLE_INTERVAL_SEC
TARGET_WIDTH = VISION_TARGET_WIDTH
TARGET_HEIGHT = VISION_TARGET_HEIGHT
JPEG_QUALITY = VISION_JPEG_QUALITY


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FrameSampler:
    """
    Timed frame sampler.

    Usage:
        sampler = FrameSampler(stream_reader, on_frame=my_callback)
        sampler.start()
        sampler.set_interval(2.0)   # speed up
        sampler.stop()

    Callback signature:
        on_frame(jpeg_bytes: bytes, metadata: dict) -> None
    """

    def __init__(
        self,
        reader:      StreamReader,
        on_frame:    Callable[[bytes, dict], None],
        interval:    float = DEFAULT_INTERVAL_SEC,
        job_id:      str   = "",
        printer_id:  str   = "",
        camera_id:   str   = "cam-0",
    ) -> None:
        self._reader     = reader
        self._on_frame   = on_frame
        self._interval   = interval
        self._job_id     = job_id
        self._printer_id = printer_id
        self._camera_id  = camera_id

        self._stop_event   = threading.Event()
        self._wake_event   = threading.Event()
        self._busy         = threading.Event()   # set while callback running
        self._thread:      Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._sample_loop,
            name="FrameSampler",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"[Sampler] Started (interval={self._interval}s)")

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread:
            self._thread.join(timeout=self._interval + 2)
        logger.info("[Sampler] Stopped.")

    def set_interval(self, seconds: float) -> None:
        self._interval = max(MIN_INTERVAL_SEC, min(MAX_INTERVAL_SEC, seconds))
        self._wake_event.set()
        logger.debug(f"[Sampler] Interval updated to {self._interval}s")

    def update_context(self, job_id: str = "", printer_id: str = "") -> None:
        """Update metadata injected into frame payloads."""
        self._job_id     = job_id
        self._printer_id = printer_id

    # ------------------------------------------------------------------
    # Sample loop
    # ------------------------------------------------------------------

    def _sample_loop(self) -> None:
        while not self._stop_event.is_set():
            self._wake_event.wait(timeout=self._interval)
            self._wake_event.clear()
            if self._stop_event.is_set():
                break

            # Skip tick if callback is still processing previous frame
            if self._busy.is_set():
                logger.debug("[Sampler] Previous frame still processing — skipping tick.")
                continue

            frame = self._reader.latest_frame
            if frame is None:
                logger.debug("[Sampler] No frame available yet — skipping.")
                continue

            try:
                jpeg_bytes = self._preprocess(frame)
            except Exception:
                logger.exception("[Sampler] Frame preprocessing failed.")
                continue

            metadata = {
                "jobId":     self._job_id,
                "printerId": self._printer_id,
                "cameraId":  self._camera_id,
                "timestamp": _utc_now(),
            }

            # Dispatch to callback in a thread to avoid blocking the sample loop
            self._busy.set()
            threading.Thread(
                target=self._dispatch,
                args=(jpeg_bytes, metadata),
                daemon=True,
            ).start()

    def _dispatch(self, jpeg_bytes: bytes, metadata: dict) -> None:
        try:
            self._on_frame(jpeg_bytes, metadata)
        except Exception:
            logger.exception("[Sampler] on_frame callback raised.")
        finally:
            self._busy.clear()

    # ------------------------------------------------------------------
    # Preprocessing
    # ------------------------------------------------------------------

    def _preprocess(self, frame: np.ndarray) -> bytes:
        """Resize frame and encode to JPEG bytes."""
        resized = cv2.resize(frame, (TARGET_WIDTH, TARGET_HEIGHT))
        ok, buf = cv2.imencode(
            ".jpg",
            resized,
            [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY],
        )
        if not ok:
            raise RuntimeError("JPEG encoding failed.")
        return buf.tobytes()
