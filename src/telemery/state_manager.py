"""
state_manager.py — Thread-Safe Central State Manager

Single source of truth for all printer state.
All telemetry updates flow through here.
All reads return immutable snapshots.
"""

import threading
import copy
import logging
from datetime import datetime, timezone
from typing import Callable, List, Optional

from src.telemetry.printer_state import PrinterStateSnapshot, PrinterStatus

logger = logging.getLogger(__name__)


class StateManager:
    """
    Thread-safe store for printer state.

    Usage:
        manager = StateManager()
        manager.update(nozzle_temp=205.3, status=PrinterStatus.PRINTING)
        snap = manager.get_snapshot()  # returns a deep copy — safe to read anywhere
    """

    def __init__(self) -> None:
        self._lock    = threading.Lock()
        self._state   = PrinterStateSnapshot()
        self._listeners: List[Callable[[PrinterStateSnapshot, dict], None]] = []

    # ------------------------------------------------------------------
    # Public write API  (telemetry engine is the only writer)
    # ------------------------------------------------------------------

    def update(self, **fields) -> None:
        """
        Atomically update one or more fields.
        Fires registered listeners after every update.

        Args:
            **fields: Any subset of PrinterStateSnapshot fields.
        """
        if not fields:
            return

        changed = {}
        with self._lock:
            for key, value in fields.items():
                if not hasattr(self._state, key):
                    logger.warning(f"[StateManager] Unknown field ignored: {key!r}")
                    continue
                old = getattr(self._state, key)
                if old != value:
                    setattr(self._state, key, value)
                    changed[key] = value

            self._state.last_updated = datetime.now(timezone.utc)
            snapshot = self._snapshot_unsafe()

        if changed:
            self._fire_listeners(snapshot, changed)

    # ------------------------------------------------------------------
    # Public read API
    # ------------------------------------------------------------------

    def get_snapshot(self) -> PrinterStateSnapshot:
        """Return a deep-copied, read-only snapshot of current state."""
        with self._lock:
            return self._snapshot_unsafe()

    # ------------------------------------------------------------------
    # Listener / event hooks (for telemetry event publisher)
    # ------------------------------------------------------------------

    def register_listener(
        self, callback: Callable[[PrinterStateSnapshot, dict], None]
    ) -> None:
        """
        Register a callback invoked on every state change.

        Signature: callback(snapshot: PrinterStateSnapshot, changed_fields: dict)
        """
        with self._lock:
            self._listeners.append(callback)

    def unregister_listener(
        self, callback: Callable[[PrinterStateSnapshot, dict], None]
    ) -> None:
        with self._lock:
            self._listeners = [l for l in self._listeners if l is not callback]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _snapshot_unsafe(self) -> PrinterStateSnapshot:
        """Must be called while _lock is held."""
        return copy.deepcopy(self._state)

    def _fire_listeners(
        self, snapshot: PrinterStateSnapshot, changed: dict
    ) -> None:
        for cb in list(self._listeners):
            try:
                cb(snapshot, changed)
            except Exception:
                logger.exception("[StateManager] Listener raised an exception")