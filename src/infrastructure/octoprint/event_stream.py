"""Optional OctoPrint WebSocket event stream.

OctoPrint exposes its browser event feed over SockJS. This wrapper keeps the
dependency optional at import time so unit tests and REST-only deployments keep
working even when `websocket-client` is not installed.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
import string
from typing import Callable, Optional
import urllib.request
import urllib.parse
from urllib.parse import urlparse, urlunparse

from config.settings import OCTOPRINT_WEBSOCKET_RECONNECT_SEC

logger = logging.getLogger(__name__)


class OctoPrintEventStream:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        on_message: Callable[[dict], None],
        reconnect_sec: float = OCTOPRINT_WEBSOCKET_RECONNECT_SEC,
    ) -> None:
        self._base_url = base_url
        self._api_key = api_key
        self._on_message = on_message
        self._reconnect_sec = reconnect_sec
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        try:
            import websocket  # noqa: F401
        except ImportError:
            logger.warning(
                "[OctoPrintEventStream] websocket-client is not installed; "
                "using REST polling only."
            )
            return False

        if self._thread and self._thread.is_alive():
            return True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="OctoPrintEventStream",
            daemon=True,
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._reconnect_sec + 2)

    def _run(self) -> None:
        import websocket
        while not self._stop_event.is_set():
            ws = None
            try:
                url = _websocket_url(self._base_url)
                logger.info("[OctoPrintEventStream] Connecting to %s", url)

                name, session = _passive_login(self._base_url, self._api_key)

                ws = websocket.create_connection(
                    url,
                    header=[f"X-Api-Key: {self._api_key}"],
                    timeout=10,
                )
                logger.info("[OctoPrintEventStream] Connected: %s", url)
                # Ensure recv() blocks indefinitely rather than timing out.
                try:
                    ws.settimeout(None)
                except Exception:
                    pass

                if name and session:
                    # Step 1: auth (SockJS-framed)
                    try:
                        _sockjs_send(ws, {"auth": f"{name}:{session}"})
                        logger.info("[OctoPrintEventStream] Sent auth for user=%r", name)
                    except Exception:
                        logger.exception("[OctoPrintEventStream] Failed to send auth message.")

                    # Step 2: throttle — triggers current payload with logs
                    try:
                        _sockjs_send(ws, {"throttle": 1})
                        logger.info("[OctoPrintEventStream] Sent throttle=1")
                    except Exception:
                        logger.exception("[OctoPrintEventStream] Failed to send throttle message.")
                else:
                    logger.warning(
                        "[OctoPrintEventStream] No auth credentials — OctoPrint will not push current payloads"
                    )

                while not self._stop_event.is_set():
                    raw = ws.recv()
                    logger.debug("[OctoPrintEventStream] Raw frame: %r", (raw[:120] if raw else raw))
                    for message in _decode_sockjs(raw):
                        self._on_message(message)
            except Exception:
                if not self._stop_event.is_set():
                    logger.exception("[OctoPrintEventStream] Stream error.")
                    self._stop_event.wait(self._reconnect_sec)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass


def _websocket_url(base_url: str) -> str:
    server_id = str(random.randint(0, 999)).zfill(3)
    session_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    path = f"/sockjs/{server_id}/{session_id}/websocket"
    return urlunparse((scheme, parsed.netloc, path, "", "", ""))


def _passive_login(base_url: str, api_key: str) -> tuple[Optional[str], Optional[str]]:
    login_url = urllib.parse.urljoin(
        base_url,
        f"/api/login?passive=true&apikey={urllib.parse.quote(api_key)}",
    )
    try:
        req = urllib.request.Request(login_url, data=b"", method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            if not raw:
                return None, None
            data = json.loads(raw.decode("utf-8"))
            name = data.get("name")
            session = data.get("session")
            logger.info(
                "[OctoPrintEventStream] Passive login (POST): name=%r session=%r",
                name,
                (session[:8] + "...") if session else None,
            )
            return name, session
    except Exception:
        logger.exception("[OctoPrintEventStream] Passive login failed.")
        return None, None


def _decode_sockjs(raw: str) -> list[dict]:
    if not raw or raw == "o" or raw == "h":
        return []
    if raw.startswith("a"):
        try:
            frames = json.loads(raw[1:])
        except json.JSONDecodeError:
            return []
        decoded = []
        for frame in frames:
            try:
                decoded.append(json.loads(frame))
            except (TypeError, json.JSONDecodeError):
                continue
        return decoded
    try:
        return [json.loads(raw)]
    except json.JSONDecodeError:
        return []


def _sockjs_frame(payload: dict) -> str:
    """Return a SockJS-framed string for sending: a["{...}"]

    SockJS message frames are prefixed with a single-letter channel
    indicator. For typical application frames we use 'a' followed by a
    JSON array containing the stringified JSON payload.
    """
    inner = json.dumps(payload)
    return "a" + json.dumps([inner])


def _sockjs_send(ws, payload: dict) -> None:
    try:
        frame = _sockjs_frame(payload)
        ws.send(frame)
    except Exception:
        logger.exception("[OctoPrintEventStream] Failed to send SockJS framed message.")
