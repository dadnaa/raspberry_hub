# Raspberry Hub

Raspberry Hub is a Python-based edge controller for a Raspberry Pi connected to a Creality-style Marlin 3D printer. It provides a single runtime for printer control, telemetry, cloud commands, job execution, and camera-based print-failure monitoring.

## Features

- USB serial connection to a Marlin-compatible 3D printer
- Centralized G-code command queue and validation
- Serial response routing for acknowledgements and telemetry
- Printer temperature, position, progress, pause, resume, and reboot tracking
- MQTT cloud integration for printer state, commands, and job lifecycle events
- G-code job loading, persistence, FIFO queuing, pause, resume, cancellation, and recovery
- Vision monitoring that activates while a job is printing
- Local printer simulator for development and testing
- Sprint-organized test suite with hardware-independent unit tests

## Architecture

The production entry point is [`main.py`](main.py). The runtime is organized into the following layers:

```text
USB serial printer
        |
        v
SerialConnection -> SerialRouter
                         |              |
                         v              v
                 CommandEngine   TelemetryEngine
                         |              |
                         v              v
                    JobManager    StateManager
                         |
                         v
                    MQTTBridge
                         |
                         v
                 VisionController
```

The `SerialRouter` is the only component that reads from the serial port. It distributes printer responses to the command and telemetry layers so that acknowledgements are not consumed by the wrong subsystem.

For a detailed component map and data-flow explanation, see [`docs/architecture.md`](docs/architecture.md).

## Requirements

- Python 3.10 or newer recommended
- Raspberry Pi or another Linux host for physical printer operation
- USB-connected Marlin-compatible 3D printer for hardware integration
- MQTT broker, such as HiveMQ, for cloud connectivity
- Camera stream and compatible inference endpoint for vision monitoring

Install the project dependencies with:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install python-dotenv pytest
```

On Windows, activate the virtual environment with:

```powershell
.venv\Scripts\Activate.ps1
```

## Configuration

Runtime values are centralized in [`config/settings.py`](config/settings.py). Secrets and deployment-specific values should be provided through environment variables rather than committed to the repository.

Create a `.env` file in the project root when running locally:

```dotenv
MQTT_HOST=your-mqtt-host
MQTT_PORT=8883
MQTT_USERNAME=your-mqtt-username
MQTT_PASSWORD=your-mqtt-password
MQTT_PRINTER_ID=printer-001
VISION_CAMERA_URL=http://camera-host:5000/video
VISION_AI_ENDPOINT=http://ai-host:2000/predict
```

The important environment variables are:

| Variable | Purpose |
| --- | --- |
| `MQTT_HOST` | MQTT broker hostname |
| `MQTT_PORT` | MQTT broker port; TLS commonly uses `8883` |
| `MQTT_USERNAME` | MQTT username |
| `MQTT_PASSWORD` | MQTT password |
| `MQTT_PRINTER_ID` | Identifier used in MQTT topics and messages |
| `VISION_CAMERA_URL` | Camera stream URL used during printing |
| `VISION_AI_ENDPOINT` | HTTP endpoint used for frame inference |

`.env`, logs, generated job data, and Python cache files are excluded by `.gitignore`.

## Running the Hub

From the repository root:

```bash
python main.py
```

The application will:

1. Connect to the configured serial printer.
2. Start the serial router, command engine, and telemetry engine.
3. Connect the MQTT bridge and publish printer state.
4. Recover persisted jobs from `data/jobs`.
5. Start vision monitoring automatically when a job enters the `PRINTING` state.

Stop the application with `Ctrl+C`. The runtime handles `SIGINT` and `SIGTERM` and shuts down its components in dependency order.

> Physical printer operation requires a working serial connection. Use the simulator and tests for development when printer hardware is unavailable.

## Running Tests

Run the complete test suite:

```bash
python -m pytest tests -v
```

Run tests for an individual sprint:

```bash
python -m pytest tests/test_sprint1.py -v
python -m pytest tests/test_sprint2.py -v
python -m pytest tests/test_sprint3.py -v
python -m pytest tests/test_sprint4.py -v
python -m pytest tests/test_sprint5.py -v
python -m pytest tests/test_sprint6.py -v
```

The tests use mocks and local components, so normal unit testing does not require a physical printer or a live MQTT broker.

## Printer Simulator

The [`Printer_simulator`](Printer_simulator) directory contains Marlin-like printer simulators for local development:

```bash
python Printer_simulator/printerSimulator.py
```

The richer simulator is available in [`Printer_simulator/crealitysim.py`](Printer_simulator/crealitysim.py). Refer to the simulator source and tests for supported behavior and commands.

## Project Layout

```text
config/              Central runtime configuration
src/core/            Shared models and types
src/hardware/        Serial connection, routing, and printer communication
src/engine/          G-code validation and command processing
src/telemetry/       Printer state and telemetry parsing
src/cloud/           MQTT client, topics, validation, and bridge
src/jobs/            G-code jobs, persistence, execution, and recovery
src/vision/          Camera, inference, failure detection, and events
src/utils/           Logging and shared utility functions
Printer_simulator/   Local printer simulators
tests/               Sprint-based test suite
docs/                Architecture and sprint documentation
main.py              Production runtime entry point
```

## Documentation

- [Architecture](docs/architecture.md)
- [Sprint 1](docs/sprint1.md)
- [Sprint 2](docs/sprint2.md)
- [Sprint 3](docs/sprint3.md)
- [Sprint 4](docs/sprint4.md)
- [Sprint 5](docs/sprint5.md)
- [Sprint 6: Vision-based failure detection](docs/sprint6.md)

## Safety

This project sends commands to physical 3D-printer hardware. Test with a simulator first, verify serial-port and printer settings, and keep emergency power or stop controls available when deploying to a real printer.
