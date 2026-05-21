# Reactive Edge Hub Architecture

This project is a Raspberry Pi edge hub for controlling and monitoring a Creality-style Marlin 3D printer. It connects to the printer over USB serial, routes all G-code through a single command engine, continuously parses telemetry, exposes cloud control over MQTT, streams full G-code jobs with persistence, and activates a vision failure monitor while a job is printing.

## Runtime Entry Point

`main.py` wires the production stack in dependency order:

1. `SerialConnection` opens the USB serial port.
2. `SerialRouter` starts the only background reader that calls `SerialConnection.read_line()`.
3. `StateManager` becomes the shared printer state store.
4. `CommandEngine` receives `SerialRouter.ack_queue` through `PrinterCommunicator`.
5. `TelemetryEngine` receives `SerialRouter.telemetry_queue` and periodically queues `M105` and `M114`.
6. `MQTTBridge` starts cloud publishing, subscriptions, command routing, job control, and recovery.
7. `VisionMonitor` and `VisionController` are attached to `JobManager` state changes.

Shutdown is also centralized in `main.py`: vision, MQTT, command processing, telemetry, router, and serial are stopped in reverse dependency order.

## Project Layout

```text
config/
  settings.py                    Central constants for logging, serial, commands, telemetry, MQTT, jobs, and vision.

src/core/
  models.py                      Shared dataclasses and literal status types for MQTT, jobs, commands, and AI results.

src/hardware/
  port_discovery.py              Finds candidate printer serial ports.
  serial_connection.py           Opens, reads, writes, reconnects, and flushes the raw serial port.
  serial_router.py               Single serial reader; fans lines into ack and telemetry queues.
  printer_communicator.py        Sends one G-code command and waits for ok/error/busy response lines.

src/engine/
  command.py                     Command IDs, statuses, entries, results, and engine state.
  validator.py                   Local G-code validation before commands enter the queue.
  queue_processor.py             Background single-consumer command execution loop.
  command_engine.py              Public command API used by MQTT, jobs, and telemetry polling.

src/telemetry/
  printer_state.py               Printer state snapshot and status enum.
  state_manager.py               Thread-safe single source of truth for printer state.
  telemetry_parser.py            Parsers for Marlin temperature, position, progress, pause, resume, and reboot lines.
  telemetry_engine.py            Background parser and active telemetry poller.
  telemetry_event.py             Event envelope for state changes.
  __init__.py                    Public telemetry exports.

src/cloud/
  mqtt_client.py                 HiveMQ MQTT client with TLS, reconnect, subscriptions, and offline outbox.
  mqtt_topics.py                 Topic contract for this printer ID.
  mqtt_publisher.py              Typed Pi-to-cloud publishing facade.
  message_validator.py           JSON, printer ID, required-field, and G-code whitelist validation.
  command_router.py              Cloud command lifecycle: QUEUED, EXECUTING, SUCCESS or ERROR.
  mqtt_bridge.py                 Cloud orchestration plus job manager ownership and state heartbeat.

src/jobs/
  gcode_pipeline.py              Loads G-code from a file or URL and strips comments/non-executable lines.
  job_model.py                   Persistable job entity and status transitions.
  job_store.py                   JSON job persistence under `data/jobs`.
  job_executor.py                Streams job lines through `CommandEngine`; handles pause, resume, cancel, fail.
  job_manager.py                 FIFO job queue, one active job, recovery, and vision state callback hook.

src/vision/
  stream_reader.py               Camera stream reader with latest-frame storage.
  frame_sampler.py               Timed JPEG frame sampling and metadata injection.
  ai_client.py                   HTTP inference client for the external vision service.
  failure_guard.py               Confidence, threshold, and cooldown decision logic.
  vision_event_publisher.py      MQTT vision event publisher.
  vision_monitor.py              End-to-end frame -> AI -> guard -> job fail pipeline.
  vision_controller.py           Starts/stops vision based on job status changes.

src/utils/
  logger_setup.py                Console and rotating-file logging setup.
  telemetry_parser.py            Legacy/simple parser helpers used by the communicator.

Printer_simulator/
  printerSimulator.py            Basic Marlin-like simulator.
  crealitysim.py                 Rich CR-10 Smart simulator with boot, telemetry, Wi-Fi noise, SD progress, and commands.
  testSimulator.py               Simulator test/helper entry point.

tests/
  test_sprint1.py ... test_sprint6.py
```

## Main Data Flow

```text
USB serial printer
    |
    v
SerialConnection
    |
    v
SerialRouter
    |                         |
    | ack_queue               | telemetry_queue
    v                         v
PrinterCommunicator      TelemetryEngine
    |                         |
    v                         v
QueueProcessor           StateManager
    |                         |
    v                         v
CommandEngine        MQTTBridge heartbeat/state publishing
    |
    +--> direct cloud commands
    +--> telemetry polling commands
    +--> job G-code streaming
```

The most important architectural rule is that `SerialRouter` is the only component that reads from the serial port. This prevents command acknowledgements from being consumed by telemetry parsing and prevents telemetry lines from blocking command execution.

## Serial and Command Architecture

`SerialConnection` owns the physical port. It autodetects likely printer ports, tries supported baud rates, waits for the printer to stabilize, writes ASCII command lines, reads decoded lines, and attempts reconnects. On reconnect, it calls a hook used by `SerialRouter` to flush stale queue data.

`SerialRouter` owns one daemon read loop. Every line goes to `telemetry_queue`; only command-response lines go to `ack_queue`. Ack lines include `ok`, `error`, `!!`, `echo:busy`, temperature responses, position responses, firmware info, and capability lines. Informational lines, SD progress, Wi-Fi noise, and Marlin banner lines remain telemetry-only.

`PrinterCommunicator` writes commands through `SerialConnection.write_line()` and waits on `ack_queue` until it sees `ok`, an error path, or a timeout. It is the hardware-facing executor used by the command layer.

`QueueProcessor` is the only class that calls `PrinterCommunicator.send_command()`. It runs a single background worker, so all printer writes are serialized. `CommandEngine` is the public API above it, with:

- `send(gcode)` for synchronous command execution.
- `send_batch(commands)` for ordered batch execution.
- `send_fire_and_forget(gcode)` for telemetry polling and other non-blocking producers.
- history, state, queue depth, and current command accessors.

## Telemetry Architecture

`TelemetryEngine` observes all serial lines through `SerialRouter.telemetry_queue`. It parses:

- temperature and target temperature from `T:` and `B:` lines;
- position from `M114` style `X: Y: Z:` lines;
- SD progress from `SD printing byte x/y`;
- pause and resume host action lines;
- reboot/startup markers.

All parsed state updates go through `StateManager`, which stores an immutable snapshot copy for readers and notifies listeners after state changes. `MQTTBridge` registers as a listener and publishes printer state on changes. It also publishes a periodic heartbeat every `MQTT_STATE_PUBLISH_INTERVAL_SEC`.

Telemetry polling is active in the current implementation. `TelemetryEngine` periodically queues `M105` and `M114` through `CommandEngine.send_fire_and_forget()`, so polling does not bypass command serialization.

## MQTT Cloud Architecture

Cloud code lives under `src/cloud`.

`MQTTClient` connects to HiveMQ using environment variables:

- `MQTT_HOST`
- `MQTT_PORT`
- `MQTT_USERNAME`
- `MQTT_PASSWORD`
- `MQTT_PRINTER_ID`

It uses TLS, MQTT v5, reconnect backoff, an offline outbox, and background threads for connect/drain work.

`MQTTTopics` defines the actual topic contract:

| Direction | Topic |
| --- | --- |
| Pi to cloud | `printers/{id}/handshake` |
| Pi to cloud | `printers/{id}/printer-state` |
| Pi to cloud | `printers/{id}/jobs/job-state` |
| Pi to cloud | `printers/{id}/command-state` |
| Cloud to Pi | `printers/{id}/command` |
| Cloud to Pi | `printers/{id}/start-job` |
| Cloud to Pi | `printers/{id}/pause-job` |
| Cloud to Pi | `printers/{id}/resume-job` |
| Cloud to Pi | `printers/{id}/stop-job` |

`MessageValidator` validates incoming JSON, required fields, the target printer ID, and the command G-code whitelist. `CommandRouter` handles direct command messages by publishing `QUEUED`, `EXECUTING`, then `SUCCESS` or `ERROR` based on the `CommandEngine` result.

`MQTTBridge` owns the MQTT client, topics, publisher, validator, router, and job manager. It subscribes to downstream topics, sends a retained handshake after connecting, publishes printer state, dispatches job controls, and calls `JobManager.recover()` on startup.

## Job Architecture

Jobs are accepted through `printers/{id}/start-job`. `JobManager.submit()` loads the G-code with `gcode_pipeline.load()`, creates a `Job`, persists it, and either starts it immediately or queues it behind the active job.

The job subsystem enforces one active job per printer. Waiting jobs are FIFO.

`JobExecutor` streams cleaned G-code one line at a time through `CommandEngine.send()`. It updates progress after each successful line, persists state through `JobStore`, publishes `JobStateMessage`, and calls an optional state listener used by vision.

Job controls:

- `pause(job_id)` pauses the active executor, sends `M25`, and marks the job `PAUSED`.
- `resume(job_id)` resumes the active executor, sends `M24`, and marks the job `PRINTING`.
- `cancel(job_id)` cancels active or queued jobs. Active cancellation sends a safe stop sequence: `M25`, `M104 S0`, `M140 S0`, `M84`.
- `fail(job_id, reason)` marks the active job failed through the executor.
- `recover()` reloads persisted jobs in `PRINTING`, `PAUSED`, `QUEUED`, or `LOADING`, resets them to `QUEUED`, and resumes from persisted G-code/current line data where available.

Completed jobs publish MQTT status `DONE` even though the internal terminal status is `COMPLETED`.

## Vision Architecture

Vision is job-state driven. `JobManager` exposes `set_state_listener()`, and `main.py` registers `VisionController.on_job_state_change`.

When a job enters `PRINTING`, `VisionController` starts `VisionMonitor`. When the job leaves `PRINTING`, the monitor stops.

The vision pipeline is:

```text
StreamReader -> FrameSampler -> AIClient -> FailureGuard -> VisionMonitor
                                                        |
                                                        v
                                            JobManager.fail(job_id, reason)
                                                        |
                                                        v
                                            VisionEventPublisher -> MQTT
```

`StreamReader` opens the configured camera URL only while a job is being monitored. `FrameSampler` encodes frames as JPEG and attaches job, printer, camera, and timestamp metadata. `AIClient` posts the image to the external inference endpoint. `FailureGuard` requires enough consecutive confident failures and observes a cooldown before returning an intervention decision.

The current `VisionMonitor` fails the job through `JobManager.fail()` when the guard returns `Action.PAUSE`; it does not merely pause the job. It also publishes a vision event for each sampled frame decision.

Vision environment variables:

- `VISION_CAMERA_URL`
- `VISION_AI_ENDPOINT`

## Configuration

All tunables are centralized in `config/settings.py`, including:

- log directory, level, rotation size, and backup count;
- serial port patterns, baud rates, read timeout, reconnect timing, and router queue sizes;
- printer command timeout, retries, queue size, command length, and command prefixes;
- telemetry idle timeout and polling commands;
- MQTT credentials/env keys, default port, reconnect backoff, outbox size, keepalive, state interval, topic validation fields, and allowed G-code prefixes;
- job storage location and pause polling interval;
- vision stream, frame, inference, confidence, threshold, cooldown, and sampling-rate settings;
- default printer metadata.

## Tests

The test suite is organized by sprint:

```bash
python -m pytest tests/test_sprint1.py -v
python -m pytest tests/test_sprint2.py -v
python -m pytest tests/test_sprint3.py -v
python -m pytest tests/test_sprint4.py -v
python -m pytest tests/test_sprint5.py -v
python -m pytest tests/test_sprint6.py -v
```

Or run all tests:

```bash
python -m pytest tests -v
```

The tests are designed around mocks and local units, so they do not require the physical printer or cloud service for normal unit coverage.
