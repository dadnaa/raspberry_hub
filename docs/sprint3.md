# Sprint 3 — Telemetry + State Intelligence Layer

## New Files

```
src/telemetry/
├── __init__.py            # Public exports
├── printer_state.py       # PrinterStateSnapshot + PrinterStatus enum
├── state_manager.py       # Thread-safe single source of truth
├── telemetry_parser.py    # Pure regex parsers (temp, pos, progress, status)
├── telemetry_engine.py    # Background thread reader + line dispatcher
└── telemetry_event.py     # TelemetryEvent envelope (for Sprint 4 MQTT)

tests/
└── test_sprint3_telemetry.py   # Full unit test suite
```

---

## Architecture: Two Planes

```
SerialConnection
      │
      ├──► line_queue (Queue) ──► TelemetryEngine  ──► StateManager
      │                            (observes)           (state)
      │
      └──► CommandEngine
            (controls)
```

The `SerialConnection` must push every decoded line into `line_queue`.
Command engine reads ACKs via its own mechanism.
These two never block each other.

---

## One Integration Step Required

`SerialConnection` (Sprint 2) needs a `line_queue: queue.Queue` attribute.
Its reader loop should do:

```python
import queue

class SerialConnection:
    def __init__(self):
        self.line_queue = queue.Queue()

    def _reader_loop(self):
        while ...:
            line = self._serial.readline().decode("utf-8", errors="replace")
            self.line_queue.put(line)   # ← add this
            # existing command-ack logic continues unchanged
```

This is the **only change** needed to Sprint 2 code.

---

## State Fields

| Field            | Type              | Source                     |
|------------------|-------------------|----------------------------|
| status           | PrinterStatus     | Pattern detection          |
| nozzle_temp      | float             | `T:` in serial lines       |
| nozzle_target    | float             | `T:xxx /yyy`               |
| bed_temp         | float             | `B:` in serial lines       |
| bed_target       | float             | `B:xxx /yyy`               |
| progress_pct     | float 0-100       | `SD printing byte x/y`     |
| position_x/y/z   | float             | `X:n Y:n Z:n`              |
| last_updated     | datetime (UTC)    | Every state change         |

---

## Sprint 4 Readiness

`TelemetryEngine` accepts an `on_event` callback:

```python
def mqtt_publish(event: TelemetryEvent):
    client.publish("printer/telemetry", json.dumps(event.to_dict()))

engine = TelemetryEngine(line_queue, state_manager, on_event=mqtt_publish)
```

No other changes needed to connect to HiveMQ.