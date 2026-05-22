# OctoPrint-Backed Edge Hub Architecture

This project is now a Raspberry Pi edge hub that keeps the application domain
logic locally, while delegating direct printer communication to OctoPrint.

The app owns:

- MQTT cloud command and job control.
- Local job queue, persistence, and progress publishing.
- Vision monitoring and failure handling.
- Domain printer state exposed to MQTT.

OctoPrint owns:

- Serial port discovery and connection.
- G-code execution and printer safety checks.
- Printer telemetry collection.
- Job/printer event stream.

## Runtime Entry Point

`main.py` wires the production stack:

1. `StateManager` stores translated printer state.
2. `OctoPrintClient` talks to OctoPrint over REST.
3. `OctoPrintGateway` exposes the printer-facing gateway API.
4. `MQTTBridge` routes cloud commands and job controls.
5. `JobManager` streams local jobs through the gateway.
6. `VisionMonitor` starts and stops from job state changes.

Shutdown happens in reverse: vision, MQTT, then the OctoPrint gateway.

## Project Layout

```text
config/
  settings.py                         Runtime settings for MQTT, jobs, vision, and OctoPrint.

src/core/
  models.py                           MQTT/job/AI dataclasses.
  printer_state.py                    Domain printer state snapshot and status enum.
  state_manager.py                    Thread-safe state store.

src/infrastructure/octoprint/
  client.py                           OctoPrint REST client.
  event_stream.py                     Optional OctoPrint SockJS/WebSocket event stream.
  gateway.py                          IPrinterGateway, OctoPrintGateway, MockGateway.

src/cloud/
  mqtt_client.py                      HiveMQ client.
  mqtt_topics.py                      Topic contract.
  mqtt_publisher.py                   Typed publishing facade.
  message_validator.py                Incoming MQTT validation.
  command_router.py                   Routes direct G-code commands to the printer gateway.
  mqtt_bridge.py                      MQTT orchestration and job-control dispatch.

src/jobs/
  gcode_pipeline.py                   Loads and cleans G-code.
  job_model.py                        Persistable job entity.
  job_store.py                        JSON job persistence.
  job_executor.py                     Streams job lines through IPrinterGateway.
  job_manager.py                      FIFO queue, active job ownership, recovery, controls.

src/vision/
  stream_reader.py                    Camera stream reader.
  frame_sampler.py                    Timed JPEG sampling.
  ai_client.py                        External vision inference client.
  failure_guard.py                    Confidence/cooldown decision logic.
  vision_event_publisher.py           MQTT vision events.
  vision_monitor.py                   Frame -> AI -> guard -> job fail pipeline.
  vision_controller.py                Starts/stops vision from job status.
```

## Main Data Flow

```text
MQTT / Jobs / Vision
        |
        v
IPrinterGateway
        |
        v
OctoPrintGateway
        |
        v
OctoPrint REST / optional WebSocket
        |
        v
/tmp/ttyV0 simulator or real printer
```

## Printer Gateway

The domain code talks only to the gateway surface:

- `send(gcode)`
- `send_gcode(gcode)`
- `pause()`
- `resume()`
- `cancel()`
- `start()`
- `stop()`

`OctoPrintGateway` implements this against OctoPrint. `MockGateway` supports
local tests and development without OctoPrint.

## Telemetry

Raw serial telemetry parsing has been removed from the active runtime.

`OctoPrintGateway` polls OctoPrint REST endpoints and translates printer/job
payloads into the domain `StateManager`. Optional WebSocket support can be
enabled with:

```bash
OCTOPRINT_WEBSOCKET_ENABLED=1
```

REST polling remains enabled as the default fallback.

## Configuration

Required for production:

```bash
OCTOPRINT_BASE_URL=http://127.0.0.1:5000
OCTOPRINT_API_KEY=...
```

MQTT and vision settings remain in `config/settings.py`.

## Simulator Use

For `/tmp/ttyV0` simulator use, connect OctoPrint to `/tmp/ttyV0`. The app then
talks to OctoPrint, not directly to the virtual serial port.
