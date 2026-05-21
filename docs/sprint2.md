# Sprint 2 - Command Queue Engine

Sprint 2 adds deterministic command scheduling on top of the serial layer. The current implementation lives in `src/engine`.

## Current Implementation

`CommandEngine` is the public command API for the rest of the project. MQTT command routing, telemetry polling, and job streaming all send printer commands through it.

`QueueProcessor` owns the single worker thread that calls `PrinterCommunicator.send_command()`. This preserves Marlin's one-command-at-a-time `send -> response -> ok` protocol.

`validator.py` rejects invalid local G-code before commands enter the queue. `command.py` defines command entries, results, statuses, and engine states.

## Files

```text
src/engine/
  command.py
  validator.py
  queue_processor.py
  command_engine.py

tests/test_sprint2.py
```

## Public API

- `CommandEngine.send(gcode)` validates, queues, and waits for a terminal result.
- `CommandEngine.send_batch(commands)` validates and executes commands in order.
- `CommandEngine.send_fire_and_forget(gcode)` queues without waiting; telemetry polling uses this.
- `state`, `queue_depth`, `current_command`, and `get_history()` expose operational state.

## Execution Flow

```text
Producer
  |
  v
CommandEngine
  |
  v
QueueProcessor
  |
  v
PrinterCommunicator
  |
  v
SerialConnection.write_line()
  |
  v
SerialRouter.ack_queue -> ok/error/busy response
```

## Current Design Notes

- `CommandEngine.__init__` requires both a `SerialConnection` and a `SerialRouter`.
- The communicator reads acknowledgements from `router.ack_queue`.
- Queue capacity, timeouts, retries, history size, and command length limits are configured in `config/settings.py`.
- Failed serial commands can trigger reconnect through the lower hardware layer.

## Useful Commands

```bash
python -m pytest tests/test_sprint2.py -v
```
