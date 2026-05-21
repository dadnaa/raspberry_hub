# Sprint 1 - USB Serial Foundation

Sprint 1 provides the physical printer communication layer. In the current project it is implemented by `src/hardware/port_discovery.py`, `src/hardware/serial_connection.py`, and `src/hardware/printer_communicator.py`.

## Current Implementation

`SerialConnection` owns the raw USB serial port. It detects likely printer ports, tries the configured Creality baud rates, stabilizes after opening the port, writes ASCII command lines, reads decoded serial lines, flushes buffers, disconnects, and reconnects when needed.

`PrinterCommunicator` is the command-level wrapper around the serial connection. It writes one G-code command and waits for an `ok` acknowledgement, retrying on timeout according to `config/settings.py`.

In the full current stack, `PrinterCommunicator` receives `SerialRouter.ack_queue` and does not read directly from the serial port. Direct reading is still available as a compatibility fallback for isolated tests, but production wiring uses the router to avoid races with telemetry.

## Files

```text
src/hardware/
  port_discovery.py
  serial_connection.py
  printer_communicator.py
  serial_router.py            Added later, but now required by production wiring.

src/utils/
  telemetry_parser.py         Simple helper parsers used by PrinterCommunicator.
  logger_setup.py

config/settings.py
tests/test_sprint1.py
```

## Key Runtime Rules

- Upper layers do not import or use `pyserial` directly.
- Raw writes go through `SerialConnection.write_line()`.
- Production reads are centralized by `SerialRouter`, not by multiple consumers.
- Reconnects flush stale router queues through the `on_reconnect` hook.
- Command acknowledgement handling is timeout and retry based.

## Useful Commands

```bash
pip install -r requirements.txt
python -m pytest tests/test_sprint1.py -v
```

Running `python main.py` starts the full current stack, not a Sprint 1-only demo.
