"""
core/models.py — Sprint 5: Updated shared dataclasses.

Adds:
  - JobStatus extended with LOADING
    - New MQTT downstream topics: pause-job, resume-job, cancel-job
    - PauseJobMessage, ResumeJobMessage, CancelJobMessage
  - JobStateMessage gets to_json() for consistency
"""

from __future__ import annotations
from dataclasses import dataclass, asdict, field
from typing import Optional, Literal, Union
import json

# ── Type aliases ──────────────────────────────────────────────────────

PrinterStatus      = Literal["IDLE", "PRINTING", "PAUSED", "OFFLINE"]
JobStatus          = Literal["QUEUED", "LOADING", "PRINTING", "PAUSED", "COMPLETED", "DONE", "FAILED", "CANCELLED"]
AIEvent            = Literal["NORMAL", "SPAGHETTI", "LAYER_SHIFT"]
CommandStateStatus = Literal["QUEUED", "EXECUTING", "SUCCESS", "ERROR"]

# ── Existing models ───────────────────────────────────────────────────

@dataclass
class FramePacket:
    cam_id:     str
    frame_data: bytes
    timestamp:  float = 0.0
    printerId:  Optional[str] = None

@dataclass
class StartJobMessage:
    """Mapped to MQTT: printers/{id}/start-job"""
    printerId:    str
    commandName:  str
    jobId:        str
    fileUrl:      str

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "StartJobMessage":
        return StartJobMessage(**json.loads(raw))

@dataclass
class CommandMessage:
    """Mapped to MQTT: printers/{id}/command"""
    printerId:   str
    commandName: str
    gcode:       str
    commandLogId: str
    reason:      Optional[str] = None

    def to_json(self) -> str:
        return json.dumps({k: v for k, v in asdict(self).items() if v is not None})

    @staticmethod
    def from_json(raw: str) -> "CommandMessage":
        return CommandMessage(**json.loads(raw))

@dataclass
class PrinterStateMessage:
    """Mapped to MQTT: printers/{id}/printer-state"""
    printerId:      str
    name:           str
    model:          str
    status:         PrinterStatus
    nozzleDiameter: float
    nozzleTemp:     float
    bedTemp:        float

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "PrinterStateMessage":
        return PrinterStateMessage(**json.loads(raw))

@dataclass
class JobStateMessage:
    """Mapped to MQTT: printers/{id}/jobs/job-state"""
    jobId:         str
    printerId:     str
    fileUrl:       str
    status:        JobStatus
    progress:      float
    startedAt:     Optional[str]
    finishedAt:    Optional[str]
    estimatedTime: Union[int, str]
    reason:        Optional[str] = None

    def to_json(self) -> str:
        data = asdict(self)
        if data.get("reason") is None:
            data.pop("reason", None)
        return json.dumps(data)

    @staticmethod
    def from_json(raw: str) -> "JobStateMessage":
        return JobStateMessage(**json.loads(raw))

@dataclass
class HandshakeMessage:
    """Mapped to MQTT: printers/{id}/handshake"""
    printerId:      str
    name:           str
    model:          str
    nozzleDiameter: float

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "HandshakeMessage":
        return HandshakeMessage(**json.loads(raw))

@dataclass
class CommandResponseMessage:
    """Mapped to MQTT: printers/{id}/command-state"""
    printerId:   str
    commandName: str
    gcode:       str
    status:      CommandStateStatus
    commandLogId: str
    reason:      Optional[str] = None
    response:    Optional[str] = None
    timestamp:   Optional[str] = None

    def to_json(self) -> str:
        return json.dumps({k: v for k, v in asdict(self).items() if v is not None})

    @staticmethod
    def from_json(raw: str) -> "CommandResponseMessage":
        return CommandResponseMessage(**json.loads(raw))

# ── Sprint 5: Job control messages ───────────────────────────────────

@dataclass
class PauseJobMessage:
    """Mapped to MQTT: printers/{id}/pause-job"""
    printerId: str
    jobId:     str

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "PauseJobMessage":
        return PauseJobMessage(**json.loads(raw))

@dataclass
class ResumeJobMessage:
    """Mapped to MQTT: printers/{id}/resume-job"""
    printerId: str
    jobId:     str

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "ResumeJobMessage":
        return ResumeJobMessage(**json.loads(raw))

@dataclass
class CancelJobMessage:
    """Mapped to MQTT: printers/{id}/cancel-job"""
    printerId: str
    jobId:     str
    reason:    Optional[str] = None

    def to_json(self) -> str:
        return json.dumps({k: v for k, v in asdict(self).items() if v is not None})

    @staticmethod
    def from_json(raw: str) -> "CancelJobMessage":
        return CancelJobMessage(**json.loads(raw))

# ── AI / failure detection models (unchanged) ─────────────────────────

@dataclass
class AIResult:
    cameraId:   str
    event:      AIEvent
    confidence: float

    @staticmethod
    def from_dict(d: dict) -> "AIResult":
        return AIResult(
            cameraId=d["cameraId"],
            event=d.get("event", "NORMAL"),
            confidence=float(d.get("confidence", 0.0)),
        )

@dataclass
class FailureDetectionState:
    cameraId:     str
    printerId:    Optional[str]
    errorCounter: int = 0

    def to_dict(self) -> dict:
        return asdict(self)
