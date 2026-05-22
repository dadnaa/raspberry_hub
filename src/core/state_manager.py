"""Thread-safe central printer state store."""

import copy
import logging
import threading
from datetime import datetime, timezone
from typing import Callable, List

from src.core.printer_state import PrinterStateSnapshot

logger = logging.getLogger(__name__)


class StateManager:
    """Single source of truth for translated printer state."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = PrinterStateSnapshot()
        self._listeners: List[Callable[[PrinterStateSnapshot, dict], None]] = []

    def update(self, **fields) -> None:
        if not fields:
            return

        changed = {}
        with self._lock:
            for key, value in fields.items():
                if not hasattr(self._state, key):
                    logger.warning("[StateManager] Unknown field ignored: %r", key)
                    continue
                old = getattr(self._state, key)
                if old != value:
                    setattr(self._state, key, value)
                    changed[key] = value

            self._state.last_updated = datetime.now(timezone.utc)
            snapshot = self._snapshot_unsafe()

        if changed:
            self._fire_listeners(snapshot, changed)

    def get_snapshot(self) -> PrinterStateSnapshot:
        with self._lock:
            return self._snapshot_unsafe()

    def register_listener(
        self, callback: Callable[[PrinterStateSnapshot, dict], None]
    ) -> None:
        with self._lock:
            self._listeners.append(callback)

    def unregister_listener(
        self, callback: Callable[[PrinterStateSnapshot, dict], None]
    ) -> None:
        with self._lock:
            self._listeners = [item for item in self._listeners if item is not callback]

    def _snapshot_unsafe(self) -> PrinterStateSnapshot:
        return copy.deepcopy(self._state)

    def _fire_listeners(self, snapshot: PrinterStateSnapshot, changed: dict) -> None:
        for callback in list(self._listeners):
            try:
                callback(snapshot, changed)
            except Exception:
                logger.exception("[StateManager] Listener raised an exception")
