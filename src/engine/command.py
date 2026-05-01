"""
command.py — Layer 2: Command Queue Engine
Data models for command lifecycle tracking.

Defines:
  - CommandStatus  → all states a command can be in
  - CommandEntry   → a single queued command with full audit trail
  - CommandResult  → immutable result returned to callers
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any


# ── Execution State Machine ───────────────────────────────────────────────

class EngineState(Enum):
    """
    State of the Command Queue Engine itself.

    Transitions:
      IDLE → BUSY (command dequeued)
      BUSY → WAITING_FOR_OK (command sent to serial)
      WAITING_FOR_OK → IDLE (ok received)
      WAITING_FOR_OK → ERROR (timeout after retries)
      ERROR → IDLE (recovery complete)
      Any → SHUTDOWN (stop() called)
    """
    IDLE           = auto()   # Queue empty, engine waiting
    BUSY           = auto()   # Command dequeued, about to send
    WAITING_FOR_OK = auto()   # Command sent, waiting for printer ack
    ERROR          = auto()   # Last command failed, recovering
    SHUTDOWN       = auto()   # Engine stopping, no new commands accepted


# ── Per-Command Status ────────────────────────────────────────────────────

class CommandStatus(Enum):
    """
    Lifecycle status of a single command entry.

    Transitions:
      PENDING → SENT → OK      (happy path)
      PENDING → SENT → TIMEOUT → FAILED  (retry exhausted)
      PENDING → REJECTED                  (invalid or shutdown)
    """
    PENDING   = auto()   # In queue, not yet sent
    SENT      = auto()   # Written to serial, waiting for ok
    OK        = auto()   # Acknowledged by printer
    TIMEOUT   = auto()   # No ok received within timeout window
    FAILED    = auto()   # Permanently failed (retries exhausted)
    REJECTED  = auto()   # Refused before queuing (validation / shutdown)


# ── Command Entry (internal queue item) ───────────────────────────────────

@dataclass
class CommandEntry:
    """
    A single G-code command as it lives inside the queue.

    Created by the public API, consumed by the queue processor.
    Not exposed outside the engine layer.
    """
    gcode: str
    command_id: str           = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status: CommandStatus     = field(default=CommandStatus.PENDING)
    queued_at: datetime       = field(default_factory=datetime.utcnow)
    sent_at: datetime | None  = field(default=None)
    done_at: datetime | None  = field(default=None)
    responses: list[str]      = field(default_factory=list)
    error_message: str | None = field(default=None)
    attempt_count: int        = field(default=0)

    def mark_sent(self) -> None:
        self.status  = CommandStatus.SENT
        self.sent_at = datetime.utcnow()
        self.attempt_count += 1

    def mark_ok(self, responses: list[str]) -> None:
        self.status    = CommandStatus.OK
        self.responses = responses
        self.done_at   = datetime.utcnow()

    def mark_timeout(self) -> None:
        self.status = CommandStatus.TIMEOUT

    def mark_failed(self, reason: str) -> None:
        self.status        = CommandStatus.FAILED
        self.error_message = reason
        self.done_at       = datetime.utcnow()

    def mark_rejected(self, reason: str) -> None:
        self.status        = CommandStatus.REJECTED
        self.error_message = reason
        self.done_at       = datetime.utcnow()

    @property
    def is_terminal(self) -> bool:
        """True when command has reached a final state."""
        return self.status in (
            CommandStatus.OK,
            CommandStatus.FAILED,
            CommandStatus.REJECTED,
        )

    @property
    def elapsed_ms(self) -> float | None:
        """Round-trip time in milliseconds (sent → done), or None."""
        if self.sent_at and self.done_at:
            delta = self.done_at - self.sent_at
            return delta.total_seconds() * 1000
        return None

    def __str__(self) -> str:
        return (
            f"[{self.command_id}] {self.gcode!r:20s}  "
            f"status={self.status.name:10s}  "
            f"attempts={self.attempt_count}"
        )


# ── Public Result (returned to callers) ───────────────────────────────────

@dataclass(frozen=True)
class CommandResult:
    """
    Immutable result object returned to the caller after execution.

    Frozen so callers cannot mutate past results.
    """
    command_id: str
    gcode: str
    status: CommandStatus
    responses: tuple[str, ...]
    error_message: str | None
    elapsed_ms: float | None

    @classmethod
    def from_entry(cls, entry: CommandEntry) -> CommandResult:
        return cls(
            command_id    = entry.command_id,
            gcode         = entry.gcode,
            status        = entry.status,
            responses     = tuple(entry.responses),
            error_message = entry.error_message,
            elapsed_ms    = entry.elapsed_ms,
        )

    @property
    def succeeded(self) -> bool:
        return self.status == CommandStatus.OK

    @property
    def failed(self) -> bool:
        return self.status in (CommandStatus.FAILED, CommandStatus.REJECTED)