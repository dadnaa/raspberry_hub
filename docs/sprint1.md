# Sprint 1 — USB Serial Foundation

## Overview

Sprint 1 establishes Layer 1 of the Reactive Edge Hub: the hardware interface.

It implements the complete USB serial communication stack between the Raspberry Pi
and the Creality 3D printer, enforcing the strict **request → response → "ok"** protocol.

---

## Folder Structure (Post Sprint 1)

```
reactive-edge-hub/
├── main.py                          ← Sprint 1 entry point
├── requirements.txt
├── config/
│   └── settings.py                  ← Global constants
├── logs/                            ← Auto-generated session logs
├── docs/
│   └── sprint1.md                   ← This file
├── tests/
│   └── test_sprint1.py              ← Unit tests (no hardware needed)
└── src/
    ├── hardware/                    ← Layer 1 (Sprint 1)
    │   ├── port_discovery.py        ← Auto-detect USB port
    │   ├── serial_connection.py     ← Raw serial open/read/write
    │   └── printer_communicator.py  ← ok-sync command protocol
    ├── engine/                      ← Layer 2 (Sprint 2)
    ├── telemetry/                   ← Layer 3 (Sprint 3)
    ├── cloud/                       ← Layer 4 (Sprint 4)
    └── utils/
        ├── telemetry_parser.py      ← Pure parsing functions
        └── logger_setup.py          ← Logging config
```

---

## How to Run

### 1. Install dependencies

```bash
pip install pyserial
```

### 2. Check your printer is connected

```bash
ls /dev/ttyUSB* /dev/ttyACM*
```

### 3. Run Sprint 1

```bash
python main.py
```

Logs appear both on console and in `logs/sprint1_<timestamp>.log`.

---

## How to Run Tests (No Hardware Required)

```bash
pip install pytest
python -m pytest tests/test_sprint1.py -v
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| No hardcoded port | Portability across machines |
| Baud rate auto-detection | Handles both 115200 and 250000 Creality variants |
| 3s stabilization delay | Printer resets on USB connect; avoids reading boot noise |
| "ok" detected anywhere in line | Marlin sometimes sends `ok T:210 …` combined lines |
| Retry-on-timeout (×2) | Transient serial noise shouldn't abort the session |
| Pure parser functions in `utils/` | Reusable across Sprint 1 (inline) and Sprint 3 (continuous) |
| SerialConnection hides pyserial | Upper layers never import pyserial directly |

---

## Sprint 1 → Sprint 2 Handoff

Sprint 2 will introduce the **Command Queue Engine** (Layer 2).

The `PrinterCommunicator.send_command()` method becomes the
internal executor called by the queue — no changes to Layer 1 required.