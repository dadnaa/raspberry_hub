# Sprint 5 — Job Management + G-code Streaming Engine

## New Files

```
src/
├── core/
│   └── models.py                  # +PauseJobMessage, ResumeJobMessage, StopJobMessage
│                                  # +JobStatus: LOADING added
└── jobs/
    ├── __init__.py
    ├── job_model.py               # Job entity, state transitions, serialization
    ├── gcode_pipeline.py          # Load from file/URL, parse, clean, normalize
    ├── job_store.py               # Persist jobs to data/jobs/<id>.json
    ├── job_executor.py            # Stream G-code through CommandEngine, pause/resume/cancel
    └── job_manager.py             # FIFO queue, single-active-job, recovery

src/mqtt/
    ├── mqtt_topics.py             # +pause-job, resume-job, stop-job
    └── mqtt_bridge.py             # +job control dispatch, recovery on start

data/jobs/                         # Persistent job state (JSON files)
tests/
    test_sprint5_jobs.py           # Full test suite
```

---

## Architecture

```
MQTT Cloud
    │
    │  start-job / pause-job / resume-job / stop-job
    ▼
MQTTBridge  (mqtt_bridge.py)
    │
    ▼
JobManager  (job_manager.py)
    │  FIFO queue — only 1 active job per printer
    │
    ▼
JobExecutor  (job_executor.py)
    │  streams line-by-line
    │  pause() / resume() / cancel() from any thread
    ▼
CommandEngine  (Sprint 2)
    │  .send(gcode) → waits for "ok"
    ▼
SerialConnection → Printer
```

---

## Job Lifecycle

```
QUEUED
  │
  └─► LOADING (G-code being fetched/parsed)
         │
         └─► PRINTING ──────────────────┐
                │                       │
                │◄──── pause() ─────────┤
                ▼                       │
              PAUSED                    │
                │                       │
                └──── resume() ─────────┘
                │
                ├─► COMPLETED  (all lines executed)
                ├─► FAILED     (printer rejected a line / exception)
                └─► CANCELLED  (stop-job received)
```

---

## New MQTT Topics (Sprint 5)

| Direction | Topic | Model |
|-----------|-------|-------|
| Cloud → Pi | `printer/{id}/pause-job`  | `PauseJobMessage`  |
| Cloud → Pi | `printer/{id}/resume-job` | `ResumeJobMessage` |
| Cloud → Pi | `printer/{id}/stop-job`   | `StopJobMessage`   |

All existing Sprint 4 topics unchanged.

---

## Crash Recovery

On startup, `MQTTBridge.start()` calls `JobManager.recover()`:

1. Loads all JSON files from `data/jobs/`
2. Finds jobs in `PRINTING`, `PAUSED`, `QUEUED`, or `LOADING` state
3. Re-queues them FIFO by `created_at`
4. Resumes execution from `current_line_index` — no lines are re-sent

---

## Safety Rules (Enforced)

- `JobExecutor` is the **only** entity that calls `CommandEngine.send()`
  for job G-code — never the bridge or manager directly
- One `JobExecutor` per printer at all times
- Cancel sends `M25, M104 S0, M140 S0, M84` (safe stop sequence)
- `JobStore` uses atomic rename (`tmp → final`) on every write

---

## Sprint 6 Preview

Vision-Based Failure Detection:
- AI monitors camera feed during `PRINTING`
- On `SPAGHETTI` or `LAYER_SHIFT` detection → `job_manager.cancel()`
- `FailureDetectionState` + `AIResult` models already in `models.py`