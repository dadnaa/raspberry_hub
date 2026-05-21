# Sprint 6 - Vision-Based Failure Detection

Sprint 6 adds job-state-driven vision monitoring. Vision is inactive unless a job is currently printing.

## Current Implementation

`main.py` creates `AIClient`, `VisionEventPublisher`, `VisionMonitor`, and `VisionController`. It registers `VisionController.on_job_state_change` with `bridge.job_manager.set_state_listener()`.

When a job transitions to `PRINTING`, the controller starts the monitor. When the job transitions to `PAUSED`, `COMPLETED`, `FAILED`, or `CANCELLED`, the controller stops the monitor.

## Files

```text
src/vision/
  stream_reader.py
  frame_sampler.py
  ai_client.py
  failure_guard.py
  vision_event_publisher.py
  vision_monitor.py
  vision_controller.py

src/jobs/job_executor.py
src/jobs/job_manager.py
tests/test_sprint6.py
```

## Vision Flow

```text
Job state PRINTING
  |
  v
VisionController
  |
  v
VisionMonitor
  |
  v
StreamReader -> FrameSampler -> AIClient -> FailureGuard
                                      |
                                      v
                         publish vision event over MQTT
                                      |
                                      v
                       on intervention: JobManager.fail()
```

## Components

- `StreamReader` opens the configured camera stream and stores the latest frame.
- `FrameSampler` samples frames on a timer, encodes JPEG, and attaches metadata.
- `AIClient` posts each sampled frame to the configured inference endpoint.
- `FailureGuard` applies consecutive-failure threshold, minimum confidence, and cooldown.
- `VisionEventPublisher` publishes the decision result.
- `VisionMonitor` adapts sampling speed and acts on failure decisions.
- `VisionController` connects job lifecycle changes to monitor start/stop.

## Current Intervention Behavior

The current `VisionMonitor` calls `JobManager.fail(job_id, reason)` when `FailureGuard` returns `Action.PAUSE`. This marks the active job failed through the executor and runs the executor failure path, including safe stop commands. Older docs that say vision only pauses the job are no longer accurate.

## Sampling Behavior

| Condition | Interval |
| --- | --- |
| Normal | `VISION_INTERVAL_NORMAL_SEC` |
| Failure signal | `VISION_INTERVAL_RISK_SEC` |
| Stable OK streak | `VISION_INTERVAL_STABLE_SEC` after `VISION_OK_STREAK_FOR_SLOW` OK results |

## Environment Variables

```text
VISION_CAMERA_URL
VISION_AI_ENDPOINT
```

Defaults and thresholds are defined in `config/settings.py`.

## Useful Commands

```bash
python -m pytest tests/test_sprint6.py -v
```
