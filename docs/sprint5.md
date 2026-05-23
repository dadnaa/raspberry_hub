# Sprint 5 - Job Management and G-code Streaming

Sprint 5 adds persistent print jobs, G-code loading, FIFO scheduling, and job control through MQTT.

## Current Implementation

Jobs are managed by `JobManager`, which is owned by `MQTTBridge` unless injected for tests. The manager accepts `StartJobMessage`, loads the referenced G-code, creates a persistable `Job`, and starts it immediately if no job is active. Otherwise it queues the job FIFO.

`JobExecutor` streams executable G-code lines through `CommandEngine.send()`. After each line it updates progress, persists the job, publishes job state, and notifies the optional state listener used by vision.

## Files

```text
src/jobs/
  gcode_pipeline.py
  job_model.py
  job_store.py
  job_executor.py
  job_manager.py

src/cloud/mqtt_bridge.py
src/cloud/mqtt_topics.py
src/core/models.py
tests/test_sprint5.py
```

## Job Flow

```text
printers/{id}/start-job
  |
  v
MQTTBridge
  |
  v
JobManager.submit()
  |
  v
gcode_pipeline.load(fileUrl)
  |
  v
JobStore.save(job)
  |
  v
JobExecutor
  |
  v
CommandEngine.send(each line)
```

## Job Lifecycle

```text
QUEUED -> PRINTING -> COMPLETED
                 |-> PAUSED -> PRINTING
                 |-> FAILED
                 |-> CANCELLED
```

The internal completed state is `COMPLETED`; MQTT job-state publishes it as `DONE`.

## Job Controls

| MQTT topic | Manager method | Behavior |
| --- | --- | --- |
| `printers/{id}/start-job` | `submit()` | Load G-code and start or queue the job. |
| `printers/{id}/pause-job` | `pause()` | Pause the active job and send `M25`. |
| `printers/{id}/resume-job` | `resume()` | Resume the active job and send `M24`. |
| `printers/{id}/cancel-job` | `cancel()` | Cancel active or queued job. Active cancel sends safe stop commands. |

`JobManager.fail(job_id, reason)` is also available internally and is used by the vision system.

## Persistence and Recovery

`JobStore` persists jobs as JSON under `data/jobs`. `MQTTBridge.start()` calls `JobManager.recover()`, which reloads jobs in `PRINTING`, `PAUSED`, `QUEUED`, or `LOADING`, resets them to `QUEUED`, and restarts execution in FIFO order. Persisted G-code is reused when available; otherwise recovery attempts to reload from the original file URL.

## G-code Pipeline

`gcode_pipeline.py` supports local paths and `http://` or `https://` URLs. It strips empty lines, full-line comments, inline comments, and `%` markers, then returns executable lines starting with `G`, `M`, `T`, or `N`.

## Useful Commands

```bash
python -m pytest tests/test_sprint5.py -v
```
