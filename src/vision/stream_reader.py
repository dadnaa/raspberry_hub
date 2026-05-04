"""
stream_reader.py — Sprint 6: IP Camera Stream Reader

Connects to an IP camera via RTSP, HTTP-MJPEG, or any OpenCV-compatible URL.
Maintains a single-slot frame buffer — always the most recent frame.
Auto-reconnects on stream drop.

Key rules:
  - NEVER stores video or accumulates frames
  - Always gives caller the LATEST available frame
  - Reconnect loop runs in a daemon thread
  - Caller thread is never blocked by network I/O
"""

import logging
import threading
import time
from typing import Optional

import cv2
import numpy as np

logger = logging.getLogger(__name__)

_RECONNECT_DELAY_SEC = 3.0
_READ_TIMEOUT_SEC    = 5.0    # seconds before we declare stream dead


class StreamReader:
    """
    Background stream reader for an IP camera URL.

    Usage:
        reader = StreamReader("rtsp://192.168.1.50:554/stream")
        reader.start()
        frame = reader.latest_frame   # numpy array or None
        reader.stop()
    """

    def __init__(self, url: str) -> None:
        self._url             = url
        self._lock            = threading.Lock()
        self._latest:         Optional[np.ndarray] = None
        self._cap:            Optional[cv2.VideoCapture] = None
        self._stop_event      = threading.Event()
        self._connected_event = threading.Event()
        self._thread:         Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._read_loop,
            name="StreamReader",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"[StreamReader] Started for URL: {self._url}")

    def stop(self) -> None:
        self._stop_event.set()
        self._connected_event.clear()
        if self._thread:
            self._thread.join(timeout=5)
        self._release_cap()
        logger.info("[StreamReader] Stopped.")

    @property
    def latest_frame(self) -> Optional[np.ndarray]:
        """Return the most recent decoded frame, or None if not yet available."""
        with self._lock:
            return self._latest.copy() if self._latest is not None else None

    @property
    def is_connected(self) -> bool:
        return self._connected_event.is_set()

    def wait_for_frame(self, timeout: float = 10.0) -> bool:
        """Block until first frame arrives or timeout. Returns True if frame ready."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.latest_frame is not None:
                return True
            time.sleep(0.1)
        return False

    # ------------------------------------------------------------------
    # Background read loop
    # ------------------------------------------------------------------

    def _read_loop(self) -> None:
        while not self._stop_event.is_set():
            self._connect()
            if self._stop_event.is_set():
                break

            last_frame_ts = time.monotonic()
            while not self._stop_event.is_set():
                ret, frame = self._cap.read()
                if not ret or frame is None:
                    elapsed = time.monotonic() - last_frame_ts
                    if elapsed > _READ_TIMEOUT_SEC:
                        logger.warning("[StreamReader] Read timeout — reconnecting.")
                        break
                    time.sleep(0.01)
                    continue

                with self._lock:
                    self._latest = frame
                last_frame_ts = time.monotonic()

            self._connected_event.clear()
            self._release_cap()
            if not self._stop_event.is_set():
                logger.info(f"[StreamReader] Reconnecting in {_RECONNECT_DELAY_SEC}s...")
                self._stop_event.wait(timeout=_RECONNECT_DELAY_SEC)

    def _connect(self) -> None:
        logger.info(f"[StreamReader] Connecting to {self._url!r} ...")
        while not self._stop_event.is_set():
            cap = cv2.VideoCapture(self._url)
            # For RTSP: disable buffering so we always get the latest frame
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            if cap.isOpened():
                self._cap = cap
                self._connected_event.set()
                logger.info("[StreamReader] Stream connected.")
                return
            cap.release()
            logger.warning(f"[StreamReader] Could not open stream — retry in {_RECONNECT_DELAY_SEC}s.")
            self._stop_event.wait(timeout=_RECONNECT_DELAY_SEC)

    def _release_cap(self) -> None:
        if self._cap:
            self._cap.release()
            self._cap = None