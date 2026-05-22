"""Small OctoPrint REST client.

The rest of the application should not import this module directly. Use
`OctoPrintGateway` so jobs, MQTT, and vision stay independent of OctoPrint's
wire format.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)


class OctoPrintError(RuntimeError):
    """Raised when OctoPrint rejects or fails a request."""


class OctoPrintClient:
    """Thin synchronous wrapper around OctoPrint's REST API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout_sec: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout_sec = timeout_sec

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def api_key(self) -> str:
        return self._api_key

    def get_printer(self) -> dict[str, Any]:
        return self._request("GET", "/api/printer")

    def get_job(self) -> dict[str, Any]:
        return self._request("GET", "/api/job")

    def send_gcode(self, command: str) -> None:
        self._request("POST", "/api/printer/command", {"command": command})

    def pause_job(self) -> None:
        self._request("POST", "/api/job", {"command": "pause", "action": "pause"})

    def resume_job(self) -> None:
        self._request("POST", "/api/job", {"command": "pause", "action": "resume"})

    def cancel_job(self) -> None:
        self._request("POST", "/api/job", {"command": "cancel"})

    def connect(self, port: Optional[str] = None, baudrate: Optional[int] = None) -> None:
        payload: dict[str, Any] = {"command": "connect"}
        if port:
            payload["port"] = port
        if baudrate:
            payload["baudrate"] = baudrate
        self._request("POST", "/api/connection", payload)

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        url = urllib.parse.urljoin(f"{self._base_url}/", path.lstrip("/"))
        body = None
        headers = {
            "X-Api-Key": self._api_key,
            "Accept": "application/json",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
                raw = response.read()
                if not raw:
                    return {}
                content_type = response.headers.get("Content-Type", "")
                if "json" not in content_type:
                    return {}
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise OctoPrintError(f"OctoPrint HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise OctoPrintError(f"OctoPrint request failed: {exc.reason}") from exc
        except TimeoutError as exc:
            raise OctoPrintError("OctoPrint request timed out") from exc
