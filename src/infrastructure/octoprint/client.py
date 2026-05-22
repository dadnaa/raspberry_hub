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
        # OctoPrint expects an array of commands under the `commands` key.
        self._request("POST", "/api/printer/command", {"commands": [command]})

    def pause_job(self) -> None:
        resp = self._request("POST", "/api/job", {"command": "pause", "action": "pause"})
        logger.debug("[OctoPrintClient] pause_job response: %r", resp)
        return resp

    def resume_job(self) -> None:
        resp = self._request("POST", "/api/job", {"command": "pause", "action": "resume"})
        logger.debug("[OctoPrintClient] resume_job response: %r", resp)
        return resp

    def cancel_job(self) -> None:
        resp = self._request("POST", "/api/job", {"command": "cancel"})
        logger.debug("[OctoPrintClient] cancel_job response: %r", resp)
        return resp

    def connect(self, port: Optional[str] = None, baudrate: Optional[int] = None) -> None:
        payload: dict[str, Any] = {"command": "connect"}
        if port:
            payload["port"] = port
        if baudrate:
            payload["baudrate"] = baudrate
        self._request("POST", "/api/connection", payload)

    def upload_file(self, source_url: str, target_name: Optional[str] = None) -> str:
        """Download a G-code file from `source_url` and upload it to OctoPrint's local files.

        Returns the filename stored in OctoPrint (basename used when `target_name` not provided).
        Raises OctoPrintError on failure.
        """
        # Download source
        try:
            with urllib.request.urlopen(source_url, timeout=self._timeout_sec) as resp:
                content = resp.read()
        except Exception as exc:
            raise OctoPrintError(f"Failed to fetch source file: {exc}") from exc

        # Determine filename
        if target_name:
            filename = target_name
        else:
            parsed = urllib.parse.urlparse(source_url)
            filename = urllib.parse.unquote(parsed.path.split("/")[-1] or "upload.gcode")

        # Ensure the filename has a recognized G-code extension; OctoPrint
        # rejects unknown file types based on extension (HTTP 415 / invalid_file).
        lower = filename.lower()
        if not lower.endswith((".gcode", ".g", ".gco", ".gc", ".gcode.gz", ".gco.gz")):
            logger.warning("[OctoPrintClient] Filename %s has unrecognized extension; appending .gcode", filename)
            filename = filename + ".gcode"

        import uuid

        # Build a standards-compliant multipart/form-data body.
        # Use a boundary without leading dashes and include Content-Transfer-Encoding.
        url = urllib.parse.urljoin(f"{self._base_url}/", "/api/files/local")

        attempts = 3
        for attempt in range(attempts):
            # If first attempt, try original filename; otherwise append unique suffix
            if attempt == 0:
                current_name = filename
            else:
                import os
                base, ext = os.path.splitext(filename)
                current_name = f"{base}-{uuid.uuid4().hex}{ext}"

            boundary = uuid.uuid4().hex
            boundary_bytes = boundary.encode("utf-8")
            crlf = b"\r\n"

            head_lines = []
            head_lines.append(b"--" + boundary_bytes)
            head_lines.append(f'Content-Disposition: form-data; name="file"; filename="{current_name}"'.encode("utf-8"))
            head_lines.append(b"Content-Type: application/octet-stream")
            head_lines.append(b"Content-Transfer-Encoding: binary")
            head = crlf.join(head_lines) + crlf + crlf

            tail = crlf + b"--" + boundary_bytes + b"--" + crlf

            body = head + content + tail

            headers = {
                "X-Api-Key": self._api_key,
                "Accept": "application/json",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            }

            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=self._timeout_sec) as response:
                    raw = response.read()
                    if not raw:
                        return current_name
                    content_type = response.headers.get("Content-Type", "")
                    if "json" not in content_type:
                        return current_name
                    data = json.loads(raw.decode("utf-8"))
                    return current_name
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                # If file is currently being printed, OctoPrint returns 409 CONFLICT.
                # Retry with a unique filename to avoid overwrite conflict.
                if exc.code == 409 and attempt < attempts - 1:
                    logger.warning("[OctoPrintClient] Upload conflict for %s: %s — retrying with new name", current_name, detail)
                    continue
                raise OctoPrintError(f"OctoPrint HTTP {exc.code}: {detail}") from exc
            except urllib.error.URLError as exc:
                raise OctoPrintError(f"OctoPrint request failed: {exc.reason}") from exc
            except TimeoutError as exc:
                raise OctoPrintError("OctoPrint request timed out") from exc

    def print_file(self, filename: str) -> dict[str, Any]:
        """Select and start printing a file already uploaded to OctoPrint local storage.

        `filename` should be the basename under OctoPrint's local files storage.
        Returns the OctoPrint response as a dict.
        """
        path = "/api/files/local/" + urllib.parse.quote(filename, safe="")
        payload = {"command": "select", "print": True}
        return self._request("POST", path, payload)

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
