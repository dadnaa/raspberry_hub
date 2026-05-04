
---

```markdown
# Sprint 2 — Command Queue Engine

## Overview

Sprint 2 establishes **Layer 2 of the Reactive Edge Hub: the control layer**.

It introduces a **reliable command execution pipeline** on top of the raw
serial communication implemented in Sprint 1.

Instead of sending commands directly, all instructions are now routed through a
**thread-safe command queue**, ensuring:

- Sequential execution (one command at a time)
- Strict enforcement of the **send → wait → ok** protocol
- Retry handling on failure or timeout
- Safe and predictable printer control

This layer transforms the system from:

> “We can communicate with the printer”

into:

> “We can reliably control the printer”

---

## Folder Structure (Post Sprint 2)

```

reactive-edge-hub/
├── main.py                          ← Updated entry point (engine integrated)
├── requirements.txt
├── config/
│   └── settings.py                  ← Timeouts, retries, engine config
├── logs/
├── docs/
│   ├── sprint1.md
│   └── sprint2.md                   ← This file
├── tests/
│   ├── test_sprint1.py
│   └── test_sprint2.py              ← Engine + queue tests
└── src/
├── hardware/                    ← Layer 1 (Sprint 1)
│   ├── port_discovery.py
│   ├── serial_connection.py
│   └── printer_communicator.py
│
├── engine/                      ← Layer 2 (Sprint 2)
│   ├── command.py               ← Command model
│   ├── command_queue.py         ← Thread-safe queue
│   └── command_engine.py        ← Execution loop + retry logic
│
├── telemetry/                   ← Layer 3 (Sprint 3)
├── cloud/                       ← Layer 4 (Sprint 4)
└── utils/
├── telemetry_parser.py
└── logger_setup.py

````

---

## How to Run

### 1. Install dependencies

```bash
pip install pyserial
````

### 2. Connect your printer

```bash
ls /dev/ttyUSB* /dev/ttyACM*
```

### 3. Run Sprint 2

```bash
python main.py
```

The system now starts the **Command Engine thread** and sends test commands
through the queue instead of direct serial calls.

Logs are written to:

```
logs/sprint2_<timestamp>.log
```

---

## How to Run Tests (No Hardware Required)

```bash
pip install pytest
python -m pytest tests/test_sprint2.py -v
```

All tests use a **mock communicator**, so no physical printer is required.

---

## Key Design Decisions

| Decision                               | Rationale                                         |
| -------------------------------------- | ------------------------------------------------- |
| Command abstraction (`Command`)        | Decouples raw G-code from execution logic         |
| Thread-safe queue                      | Allows multiple producers (UI, jobs, cloud later) |
| Single consumer engine                 | Guarantees strict sequential execution            |
| Blocking ACK handling                  | Ensures printer stays in sync                     |
| Retry-on-failure (configurable)        | Handles transient printer/serial issues           |
| Timeout-based recovery                 | Prevents deadlocks if printer stops responding    |
| Engine runs in background thread       | Non-blocking architecture for future layers       |
| No direct serial access outside engine | Enforces single control authority                 |

---

## Execution Flow

```
Producer (main / future API / jobs)
        ↓
   CommandQueue
        ↓
   CommandEngine (thread)
        ↓
PrinterCommunicator (Sprint 1)
        ↓
     Serial Port
        ↓
   Printer Response ("ok" / error)
        ↓
   CommandEngine (retry / next)
```

---

## Engine Behavior

### Normal Flow

1. Command is enqueued
2. Engine dequeues command
3. Sends command via `PrinterCommunicator`
4. Waits for `"ok"`
5. Moves to next command

---

### Retry Logic

* On timeout or `"error"`:

  * Retry up to `MAX_RETRIES`
* If retries exhausted:

  * Command is marked as failed
  * Error is logged

---

### Graceful Shutdown

* Engine stops after current command completes
* Queue is preserved (future extension: persistence in Sprint 5)

---

## ⚠️ Edge Cases Handled

* Printer not responding (timeout)
* Temporary serial noise
* Partial responses before `"ok"`
* Engine stop during execution
* Rapid command enqueueing (queue buffering)

---

## 🚫 Out of Scope (Handled in Later Sprints)

* Continuous telemetry parsing → Sprint 3
* Remote control via MQTT → Sprint 4
* G-code job execution → Sprint 5
* Vision-based failure detection → Sprint 6

---

## Sprint 2 → Sprint 3 Handoff

Sprint 3 introduces **Layer 3: Telemetry System**.

Key evolution:

* Continuous reading from serial (not just blocking per command)
* Parsing temperature, position, and status data
* Maintaining a real-time printer state

The Command Engine will coexist with a **Telemetry Engine**, both
sharing the serial stream safely.

---

## Outcome

By the end of Sprint 2:

* The system has a **fully controlled execution pipeline**
* Commands are **safe, ordered, and recoverable**
* The foundation is ready for **real-time monitoring (Sprint 3)**

---

```

---

