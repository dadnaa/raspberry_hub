# Sprint 6 — Vision-Based Failure Detection System

## New Files

```
src/vision/
├── __init__.py
├── stream_reader.py          # IP camera (RTSP/HTTP/MJPEG) → single-slot frame buffer
├── frame_sampler.py          # Timed frame extraction, preprocessing (JPEG 640x480)
├── ai_client.py              # HTTP multipart POST to AI inference endpoint
├── failure_guard.py          # False-positive filter: threshold + confidence + cooldown
├── vision_event_publisher.py # MQTT publish to fleet/{printerId}/vision/events
├── vision_monitor.py         # Pipeline orchestrator + adaptive rate control
└── vision_controller.py      # Job-state-driven activation/deactivation hook

src/jobs/
├── job_executor.py           # +state_listener callback (Sprint 6 hook — 1 line change)
└── job_manager.py            # +set_state_listener() method
```

---

## Architecture

```
IP Camera (RTSP/HTTP/MJPEG)
        │
        ▼
 StreamReader            background thread, single-slot latest frame
        │  latest_frame
        ▼
 FrameSampler            ticks every 1.5–5s (adaptive)
        │  (jpeg_bytes, metadata)
        ▼
 AIClient                POST multipart/form-data → AI service
        │  AIInferenceResult {classification, confidence}
        ▼
 FailureGuard            N consecutive FAILUREs + confidence ≥ threshold + cooldown
        │  VisionDecision {action: NONE | PAUSE}
        ▼
 VisionMonitor
   ├── PAUSE  →  JobManager.pause(job_id)        ← Sprint 5 only
   └── event  →  VisionEventPublisher  →  MQTT
                  fleet/{printerId}/vision/events
```

---

## Activation (Zero idle processing)

```
JobExecutor._persist_and_publish()
       │  calls state_listener(job)
       ▼
VisionController.on_job_state_change(job)
       │
       ├── job.status == "PRINTING"  →  VisionMonitor.start_monitoring(job)
       └── job.status in terminal    →  VisionMonitor.stop_monitoring()
```

Vision is **off** unless a job is actively PRINTING. No frames sampled, no AI calls made.

---

## False Positive Protection

| Parameter | Default | Meaning |
|---|---|---|
| `failure_threshold` | 3 | Consecutive FAILUREs required |
| `confidence_min` | 0.75 | Minimum confidence to count a FAILURE |
| `cooldown_sec` | 30.0 | Seconds between interventions |

A single low-confidence FAILURE or one-off glitch never triggers a pause.

---

## Adaptive Sampling Rate

| Condition | Interval |
|---|---|
| Normal (default) | 3.0 s |
| After any FAILURE signal | 1.5 s (heightened watch) |
| After 10 consecutive OKs | 5.0 s (stable print) |

---

## MQTT Vision Event

Topic: `fleet/{printerId}/vision/events`

```json
{
  "jobId":          "abc-123",
  "printerId":      "printer-001",
  "timestamp":      "2026-05-01T12:00:00+00:00",
  "classification": "FAILURE",
  "confidence":     0.91,
  "action":         "PAUSED"
}
```

This is **reporting only** — the cloud never acts on it directly.

---

## Environment Variables

| Variable | Purpose |
|---|---|
| `VISION_CAMERA_URL` | IP camera stream URL (RTSP / HTTP / MJPEG) |
| `VISION_AI_ENDPOINT` | External AI inference service URL |

---

## Sprint 7 Preview

State persistence & crash recovery hardening:
- Full system restore after unexpected restart
- Job resume from exact execution position
- Vision state sync after recovery