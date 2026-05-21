# Sprint 3 - Telemetry and State

Sprint 3 adds continuous printer state observation. The current implementation lives in `src/telemetry` and is wired through `SerialRouter.telemetry_queue`.

## Current Implementation

`SerialRouter` sends every decoded serial line to `telemetry_queue`. `TelemetryEngine` consumes that queue, parses Marlin output, and updates `StateManager`.

`StateManager` is the thread-safe single source of truth for printer state. It stores a `PrinterStateSnapshot`, returns deep-copied snapshots to readers, and notifies registered listeners when fields change.

Telemetry also actively polls the printer. `TelemetryEngine` periodically queues configured polling commands, currently `M105` and `M114`, using `CommandEngine.send_fire_and_forget()`. This keeps polling inside the normal command queue instead of writing directly to serial.

## Files

```text
src/telemetry/
  __init__.py
  printer_state.py
  state_manager.py
  telemetry_parser.py
  telemetry_engine.py
  telemetry_event.py

tests/test_sprint3.py
```

## Parsed Signals

| Signal | Source |
| --- | --- |
| Nozzle and bed temperature | `T:` / `B:` lines |
| Temperature targets | Marlin target fragments such as `/200.0` |
| XYZ position | `M114` style `X:... Y:... Z:...` lines |
| Print progress | `SD printing byte x/y` |
| Print complete | `Done printing file` |
| Pause/resume | `// action:pause` and `// action:resume` |
| Reboot | Marlin boot/startup markers |

## State Flow

```text
SerialRouter.telemetry_queue
  |
  v
TelemetryEngine
  |
  v
StateManager.update(...)
  |
  v
registered listeners, including MQTTBridge
```

## Current Design Notes

- Telemetry does not read directly from the serial port.
- Receiving a temperature line while status is `UNKNOWN` or `REBOOTING` moves the printer to `IDLE`.
- Idle timeout can return active statuses to `IDLE` when no more progress or action lines arrive.
- The optional `on_event` callback still exists for event-style integrations, but MQTT printer-state publishing currently uses `StateManager.register_listener()`.

## Useful Commands

```bash
python -m pytest tests/test_sprint3.py -v
```
