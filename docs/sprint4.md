# Sprint 4 — MQTT Cloud Bridge

## New Files

```
src/
├── core/
│   └── models.py               # Updated: +CommandResponseMessage, +CommandStateStatus
└── mqtt/
    ├── __init__.py
    ├── mqtt_topics.py           # Single source of truth for all topic strings
    ├── mqtt_client.py           # HiveMQ connection, reconnect, buffered publish
    ├── mqtt_publisher.py        # Typed upstream publish facade
    ├── message_validator.py     # Payload validation + gcode whitelist
    ├── command_router.py        # Lifecycle routing + feedback publisher
    └── mqtt_bridge.py           # Top-level orchestrator

config/
    .env.example                 # Credential template

tests/
    test_sprint4_mqtt.py         # Full unit test suite
requirements.txt                 # paho-mqtt, python-dotenv
```

---

## Architecture

```
HiveMQ Cloud
    │  TLS/8883
    │
┌───▼──────────────────────────────────────────────────┐
│  MQTTClient  (mqtt_client.py)                        │
│  • reconnect loop + exponential backoff              │
│  • outbox buffer (256 msgs) while offline            │
│  • drain on reconnect                                │
└───┬──────────────────────────────────────────────────┘
    │
┌───▼──────────────────────────────────────────────────┐
│  MQTTBridge  (mqtt_bridge.py)                        │
│  • dispatches incoming to CommandRouter              │
│  • sends handshake on (re)connect                    │
│  • heartbeat: re-publishes printer-state every 10s   │
│  • hooks into StateManager.register_listener()       │
└───┬──────────────────────────┬───────────────────────┘
    │ publish                  │ subscribe
    │                          │
┌───▼───────────┐    ┌─────────▼──────────────────────┐
│ MQTTPublisher │    │  CommandRouter                 │
│ • handshake   │    │  1. MessageValidator            │
│ • printer-state│   │  2. Publish QUEUED              │
│ • job-state   │    │  3. Publish EXECUTING           │
│ • command-state│   │  4. CommandEngine.send(gcode)   │
└───────────────┘    │  5. Publish SUCCESS / ERROR     │
                     └─────────┬──────────────────────┘
                               │ .send(gcode)
                     ┌─────────▼──────────────────────┐
                     │  CommandEngine  (Sprint 2)      │
                     │  → SerialConnection             │
                     │  → Printer (physical)           │
                     └────────────────────────────────┘
```

---

## Topic Contract

| Direction | Topic | Model |
|-----------|-------|-------|
| Pi → Cloud | `printer/{id}/handshake` | `HandshakeMessage` |
| Pi → Cloud | `printer/{id}/printer-state` | `PrinterStateMessage` |
| Pi → Cloud | `printer/{id}/job-state` | `JobStateMessage` |
| Pi → Cloud | `printer/{id}/command-state` | `CommandResponseMessage` |
| Cloud → Pi | `printer/{id}/command` | `CommandMessage` |
| Cloud → Pi | `printer/{id}/start-job` | `StartJobMessage` |

---

## Command Lifecycle

Every cloud command produces exactly **3 publishes** to `command-state`:

```
Cloud sends:  { commandName: "SetTemp", gcode: "M104 S210", ... }

Pi publishes: { status: "QUEUED",    gcode: "M104 S210", timestamp: "..." }
Pi publishes: { status: "EXECUTING", gcode: "M104 S210", timestamp: "..." }
Pi publishes: { status: "SUCCESS",   gcode: "M104 S210", timestamp: "..." }
          or: { status: "ERROR",     gcode: "M104 S210", reason: "...", timestamp: "..." }
```

---

## Network Resilience

- Printer control never depends on MQTT (Sprint 2 command engine is unaffected)
- Outbox buffers up to 256 messages while offline
- Drained automatically on reconnect
- Reconnect uses exponential backoff: 2s → 4s → 8s … → 60s cap

---

## Setup

```bash
pip install -r requirements.txt
cp config/.env.example config/.env
# Edit config/.env with your HiveMQ credentials

# Load env vars (or use python-dotenv in main.py)
export $(cat config/.env | xargs)
python main.py
```

---

## Sprint 5 Preview

- Job file download + G-code execution pipeline
- `JobStateMessage` publishing throughout job lifecycle
- `start-job` handler fully implemented