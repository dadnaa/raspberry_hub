"""Optional OctoPrint WebSocket event stream.

OctoPrint exposes its browser event feed over SockJS. This wrapper keeps the
dependency optional at import time so unit tests and REST-only deployments keep
working even when `websocket-client` is not installed.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Optional
import urllib.request
import urllib.parse
import urllib.error
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
        self._url = _websocket_url(base_url)
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
        headers = [f"X-Api-Key: {self._api_key}"]
        while not self._stop_event.is_set():
            ws = None
            try:
                # First perform passive login to obtain SockJS session credentials.
                login_url = urllib.parse.urljoin(
                    self._base_url,
                    f"/api/login?passive=true&apikey={urllib.parse.quote(self._api_key)}",
                )
                name = None
                session = None
                try:
                    req = urllib.request.Request(login_url, method="GET")
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        raw = resp.read()
                        if raw:
                            data = json.loads(raw.decode("utf-8"))
                            name = data.get("name")
                            session = data.get("session")
                except urllib.error.HTTPError as exc:
                    # Some OctoPrint versions reject GET for /api/login. Fall back to
                    # POST before giving up so we don't log noisy tracebacks every loop.
                    if exc.code == 405:
                        try:
                            req = urllib.request.Request(login_url, data=b"", method="POST")
                            with urllib.request.urlopen(req, timeout=10) as resp:
                                raw = resp.read()
                                if raw:
                                    data = json.loads(raw.decode("utf-8"))
                                    name = data.get("name")
                                    session = data.get("session")
                        except Exception:
                            logger.debug("[OctoPrintEventStream] Passive login unavailable (GET/POST not accepted).")
                    else:
                        logger.debug("[OctoPrintEventStream] Passive login failed with HTTP %s.", exc.code)
                except Exception:
                    logger.debug("[OctoPrintEventStream] Passive login failed.", exc_info=True)

                ws = websocket.create_connection(self._url, header=headers, timeout=10)
                logger.info("[OctoPrintEventStream] Connected: %s", self._url)
                # If we obtained credentials, send SockJS auth message.
                if name and session:
                    try:
                        auth_obj = {"auth": f"{name}:{session}"}
                        # SockJS expects an array of stringified messages.
                        msg = json.dumps([json.dumps(auth_obj)])
                        ws.send(msg)
                    except Exception:
                        logger.exception("[OctoPrintEventStream] Failed to send SockJS auth message.")
                while not self._stop_event.is_set():
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        # No frame received before socket timeout; keep connection open.
                        continue
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
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, "/sockjs/websocket", "", "", ""))


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
