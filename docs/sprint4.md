# Sprint 4 - MQTT Cloud Bridge

Sprint 4 adds cloud communication. In the current project the cloud package is `src/cloud`.

## Current Implementation

`MQTTClient` manages the HiveMQ connection using TLS, MQTT v5, reconnect backoff, subscriptions, and an offline outbox. `MQTTBridge` owns the client, topic contract, publisher, validator, command router, and job manager.

Incoming direct commands are validated by `MessageValidator`, then routed by `CommandRouter` into `CommandEngine`. The router publishes command lifecycle messages before and after execution.

Printer state publishing is driven by `StateManager` listener callbacks and a periodic heartbeat in `MQTTBridge`.

## Files

```text
src/cloud/
  mqtt_client.py
  mqtt_topics.py
  mqtt_publisher.py
  message_validator.py
  command_router.py
  mqtt_bridge.py

src/core/models.py
tests/test_sprint4.py
```

## Actual Topic Contract

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

## Direct Command Lifecycle

```text
Cloud publishes printers/{id}/command
  |
  v
MQTTClient -> MQTTBridge
  |
  v
CommandRouter
  |
  +--> publish command-state QUEUED
  +--> publish command-state EXECUTING
  +--> CommandEngine.send(gcode)
  +--> publish command-state SUCCESS or ERROR
```

## Environment Variables

```text
MQTT_HOST
MQTT_PORT
MQTT_USERNAME
MQTT_PASSWORD
MQTT_PRINTER_ID
```

## Current Design Notes

- MQTT never writes to serial directly.
- `MessageValidator` enforces printer ID matching and a G-code whitelist for direct commands.
- `MQTTBridge.start()` subscribes to all downstream topics and sends a retained handshake after connection.
- The bridge owns `JobManager`, so Sprint 5 job controls are now part of the same MQTT bridge.

## Useful Commands

```bash
python -m pytest tests/test_sprint4.py -v
```
