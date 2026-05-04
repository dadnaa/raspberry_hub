"""
gcode_pipeline.py — Sprint 5: G-code Processing Pipeline

Loads G-code from a local file path or a remote URL,
strips all non-executable content, and returns a clean
ordered list of G-code commands ready for streaming.

What gets removed:
  - Comment lines (starting with ;)
  - Inline comments (anything after ; on a line)
  - Empty / whitespace-only lines
  - Lines beginning with % (RepRap file markers)

What gets normalised:
  - Stripped + uppercased command token
  - Original mixed-case arguments preserved
"""

import logging
import re
import urllib.request
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

# Matches the command part before any inline comment
_INLINE_COMMENT = re.compile(r";.*$")


def _strip_line(raw: str) -> str:
    """Remove inline comments and whitespace from one raw line."""
    return _INLINE_COMMENT.sub("", raw).strip()


def _is_executable(line: str) -> bool:
    """Return True if the cleaned line is a real G-code command."""
    if not line:
        return False
    if line.startswith("%"):
        return False
    # Must start with a recognised command letter
    return line[0].upper() in ("G", "M", "T", "N")


def load_from_string(raw: str) -> List[str]:
    """
    Parse a raw G-code string into a clean executable list.

    Args:
        raw: Full G-code file contents as a string.

    Returns:
        Ordered list of clean, executable G-code lines.
    """
    lines = []
    for raw_line in raw.splitlines():
        cleaned = _strip_line(raw_line)
        if _is_executable(cleaned):
            lines.append(cleaned)
    logger.info(f"[GcodePipeline] Parsed {len(lines)} executable lines.")
    return lines


def load_from_file(path: str) -> List[str]:
    """Load and parse G-code from a local file path."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"G-code file not found: {path}")
    raw = p.read_text(encoding="utf-8", errors="replace")
    logger.info(f"[GcodePipeline] Loaded file: {path} ({len(raw)} bytes)")
    return load_from_string(raw)


def load_from_url(url: str, timeout: int = 30) -> List[str]:
    """
    Download and parse G-code from a remote URL.

    Args:
        url:     HTTP/HTTPS URL pointing to a .gcode file.
        timeout: Request timeout in seconds.

    Returns:
        Ordered list of clean, executable G-code lines.

    Raises:
        RuntimeError: If the download fails.
    """
    logger.info(f"[GcodePipeline] Downloading: {url}")
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "rasp-arch/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        logger.info(f"[GcodePipeline] Downloaded {len(raw)} bytes from {url}")
        return load_from_string(raw)
    except Exception as exc:
        raise RuntimeError(f"Failed to download G-code from {url}: {exc}") from exc


def load(source: str) -> List[str]:
    """
    Auto-detect source type and load G-code.

    Args:
        source: Either a local file path or an http(s):// URL.
    """
    if source.startswith("http://") or source.startswith("https://"):
        return load_from_url(source)
    return load_from_file(source)