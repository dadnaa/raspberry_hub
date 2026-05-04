"""
logger_setup.py — Shared Utility
Configures system-wide logging with both console and rotating file handlers.

Call setup_logging() once at application startup before any other imports.
"""

import logging
import logging.handlers
import os
from datetime import datetime

from config.settings import (
    LOG_BACKUP_COUNT,
    LOG_DIR,
    LOG_LEVEL,
    LOG_MAX_BYTES,
)


def setup_logging(session_name: str = "session") -> logging.Logger:
    """
    Configure root logger with:
      - Console handler (INFO level, colored-friendly format)
      - Rotating file handler (DEBUG level, detailed format)

    Args:
        session_name: Label embedded in the log filename.

    Returns:
        logging.Logger: Root logger instance.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file    = os.path.join(str(LOG_DIR), f"{session_name}_{timestamp}.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)

    # Avoid duplicate handlers if called multiple times
    if root_logger.handlers:
        return root_logger

    # ── Console handler ────────────────────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    ))

    # ── File handler ───────────────────────────────────────────────────
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(name)-40s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    root_logger.info(f"[Logger] Logging initialized → {log_file}")
    return root_logger
