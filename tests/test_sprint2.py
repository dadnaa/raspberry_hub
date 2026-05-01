"""
test_sprint2.py — Sprint 2 Unit Tests
Tests for: Validator, CommandEntry state machine, CommandEngine queue behaviour.

No hardware required — PrinterCommunicator is mocked.

Run:
    python -m pytest tests/test_sprint2.py -v
"""

import sys
import os
import threading
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import MagicMock, patch

from src.engine.command import (
    CommandEntry,
    CommandResult,
    CommandStatus,
    EngineState,
)
from src.engine.validator import validate_command, validate_batch, ValidationError
from src.engine.queue_processor import QueueProcessor
from src.engine.command_engine import CommandEngine


# ── Validator Tests ──────────────────────────────────────────────────────

class TestValidator:

    def test_accepts_standard_gcode(self):
        assert validate_command("G28") == "G28"
        assert validate_command("M105") == "M105"
        assert validate_command("M104 S200") == "M104 S200"
        assert validate_command("G1 X10 Y10 F3000") == "G1 X10 Y10 F3000"

    def test_strips_whitespace(self):
        assert validate_command("  M105  ") == "M105"

    def test_rejects_empty_string(self):
        with pytest.raises(ValidationError, match="Empty"):
            validate_command("")

    def test_rejects_whitespace_only(self):
        with pytest.raises(ValidationError, match="Empty"):
            validate_command("   ")

    def test_rejects_unknown_prefix(self):
        with pytest.raises(ValidationError, match="prefix"):
            validate_command("X100")

    def test_rejects_non_string(self):
        with pytest.raises(ValidationError, match="string"):
            validate_command(42)

    def test_rejects_oversized_command(self):
        with pytest.raises(ValidationError, match="long"):
            validate_command("G1 " + "X10 " * 100)

    def test_batch_returns_valid_and_errors(self):
        cmds = ["M105", "BADCMD", "G28", "", "M114"]
        valid, errors = validate_batch(cmds)
        assert "M105" in valid
        assert "G28"  in valid
        assert "M114" in valid
        assert len(errors) == 2   # BADCMD and empty


# ── CommandEntry State Machine Tests ────────────────────────────────────

class TestCommandEntry:

    def test_initial_state(self):
        entry = CommandEntry(gcode="M105")
        assert entry.status == CommandStatus.PENDING
        assert entry.attempt_count == 0
        assert not entry.is_terminal

    def test_mark_sent(self):
        entry = CommandEntry(gcode="M105")
        entry.mark_sent()
        assert entry.status == CommandStatus.SENT
        assert entry.attempt_count == 1
        assert entry.sent_at is not None

    def test_mark_ok(self):
        entry = CommandEntry(gcode="M105")
        entry.mark_sent()
        entry.mark_ok(["ok T:200 B:60"])
        assert entry.status == CommandStatus.OK
        assert entry.is_terminal
        assert entry.responses == ["ok T:200 B:60"]

    def test_mark_failed(self):
        entry = CommandEntry(gcode="M105")
        entry.mark_sent()
        entry.mark_failed("Timeout after 2 attempts.")
        assert entry.status == CommandStatus.FAILED
        assert entry.is_terminal
        assert "Timeout" in entry.error_message

    def test_mark_rejected(self):
        entry = CommandEntry(gcode="INVALID")
        entry.mark_rejected("Bad prefix.")
        assert entry.status == CommandStatus.REJECTED
        assert entry.is_terminal

    def test_elapsed_ms_calculated(self):
        entry = CommandEntry(gcode="M105")
        entry.mark_sent()
        time.sleep(0.05)
        entry.mark_ok(["ok"])
        assert entry.elapsed_ms is not None
        assert entry.elapsed_ms >= 40   # at least 40 ms

    def test_command_result_from_entry(self):
        entry = CommandEntry(gcode="G28")
        entry.mark_sent()
        entry.mark_ok(["ok"])
        result = CommandResult.from_entry(entry)
        assert result.succeeded
        assert result.gcode == "G28"
        assert isinstance(result.responses, tuple)

    def test_command_result_is_frozen(self):
        entry = CommandEntry(gcode="G28")
        entry.mark_ok(["ok"])
        result = CommandResult.from_entry(entry)
        with pytest.raises((AttributeError, TypeError)):
            result.gcode = "M105"   # type: ignore


# ── QueueProcessor Tests (mocked communicator) ────────────────────────────

def make_mock_communicator(responses=None, raises=None):
    """Build a PrinterCommunicator mock with configurable behaviour."""
    comm = MagicMock()
    conn = MagicMock()
    conn.is_connected = True
    conn.reconnect.return_value = True
    comm._conn = conn

    if raises:
        comm.send_command.side_effect = raises
    else:
        comm.send_command.return_value = responses or ["ok"]

    return comm


class TestQueueProcessor:

    def _make_processor(self, **kwargs):
        comm = make_mock_communicator(**kwargs)
        return QueueProcessor(comm), comm

    def test_starts_in_idle(self):
        proc, _ = self._make_processor()
        assert proc.state == EngineState.IDLE

    def test_executes_single_command(self):
        proc, comm = self._make_processor()
        proc.start()

        entry = CommandEntry(gcode="M105")
        entry._done_event = threading.Event()
        proc.enqueue(entry)
        entry._done_event.wait(timeout=5)

        assert entry.status == CommandStatus.OK
        comm.send_command.assert_called_once_with("M105")
        proc.stop()

    def test_executes_batch_in_order(self):
        call_order = []

        def fake_send(gcode):
            call_order.append(gcode)
            return ["ok"]

        comm = make_mock_communicator()
        comm.send_command.side_effect = fake_send
        proc = QueueProcessor(comm)
        proc.start()

        cmds = ["G28", "M104 S200", "M105", "M114", "M115"]
        events = []
        for g in cmds:
            e = CommandEntry(gcode=g)
            e._done_event = threading.Event()
            proc.enqueue(e)
            events.append(e._done_event)

        for ev in events:
            ev.wait(timeout=5)

        assert call_order == cmds
        proc.stop()

    def test_command_marked_ok_on_success(self):
        proc, _ = self._make_processor(responses=["T:200 B:60", "ok"])
        proc.start()

        entry = CommandEntry(gcode="M105")
        entry._done_event = threading.Event()
        proc.enqueue(entry)
        entry._done_event.wait(timeout=5)

        assert entry.status == CommandStatus.OK
        assert entry.responses == ["T:200 B:60", "ok"]
        proc.stop()

    def test_command_marked_failed_on_unresponsive(self):
        from src.hardware.printer_communicator import PrinterUnresponsiveError
        proc, _ = self._make_processor(
            raises=PrinterUnresponsiveError("No ok received.")
        )
        proc.start()

        entry = CommandEntry(gcode="G28")
        entry._done_event = threading.Event()
        proc.enqueue(entry)
        entry._done_event.wait(timeout=10)

        assert entry.status == CommandStatus.FAILED
        proc.stop()

    def test_queue_depth_tracked(self):
        # Slow communicator — holds entries in queue
        barrier = threading.Barrier(2)

        def slow_send(gcode):
            if gcode == "M115":
                barrier.wait(timeout=5)
            return ["ok"]

        comm = make_mock_communicator()
        comm.send_command.side_effect = slow_send
        proc = QueueProcessor(comm)
        proc.start()

        # First command will block at barrier
        e1 = CommandEntry(gcode="M115")
        e1._done_event = threading.Event()
        proc.enqueue(e1)

        time.sleep(0.1)   # Let processor pick up e1

        # Add more while e1 is in-flight
        for g in ["M105", "M114"]:
            e = CommandEntry(gcode=g)
            e._done_event = threading.Event()
            proc.enqueue(e)

        assert proc.queue_depth >= 1
        barrier.wait(timeout=5)     # Release e1
        proc.stop()

    def test_history_records_completed_commands(self):
        proc, _ = self._make_processor()
        proc.start()

        events = []
        for g in ["M105", "M114", "M115"]:
            e = CommandEntry(gcode=g)
            e._done_event = threading.Event()
            proc.enqueue(e)
            events.append(e._done_event)

        for ev in events:
            ev.wait(timeout=5)

        history = proc.get_history()
        assert len(history) >= 3
        proc.stop()

    def test_rejected_on_shutdown(self):
        proc, _ = self._make_processor()
        proc.stop()  # Never started — state is SHUTDOWN after stop()

        entry = CommandEntry(gcode="M105")
        entry._done_event = threading.Event()
        result = proc.enqueue(entry)
        assert result is False
        assert entry.status == CommandStatus.REJECTED

    def test_safe_shutdown_finishes_current_command(self):
        started = threading.Event()
        finished = threading.Event()

        def slow_send(gcode):
            started.set()
            time.sleep(0.2)
            finished.set()
            return ["ok"]

        comm = make_mock_communicator()
        comm.send_command.side_effect = slow_send
        proc = QueueProcessor(comm)
        proc.start()

        e = CommandEntry(gcode="G28")
        e._done_event = threading.Event()
        proc.enqueue(e)

        started.wait(timeout=5)
        proc.stop(timeout_sec=5)

        assert finished.is_set()


# ── CommandEngine Integration Tests ───────────────────────────────────────

class TestCommandEngine:

    def _make_engine(self, **kwargs):
        conn = MagicMock()
        conn.is_connected = True
        conn.reconnect.return_value = True
        conn.write_line.return_value = True
        conn.readline = MagicMock(return_value=b"ok\n")

        with patch(
            "src.engine.command_engine.PrinterCommunicator"
        ) as MockComm:
            instance = MockComm.return_value
            instance._conn = conn
            if kwargs.get("raises"):
                instance.send_command.side_effect = kwargs["raises"]
            else:
                instance.send_command.return_value = kwargs.get("responses", ["ok"])

            engine = CommandEngine.__new__(CommandEngine)
            engine._comm      = instance
            engine._processor = QueueProcessor(instance)
            engine._lock      = __import__("threading").Lock()
            engine._on_complete = None
            engine.start()
            return engine, instance

    def test_send_returns_result(self):
        engine, comm = self._make_engine()
        result = engine.send("M105")
        assert result.succeeded
        assert result.gcode == "M105"
        engine.stop()

    def test_send_invalid_command_returns_rejected(self):
        engine, _ = self._make_engine()
        result = engine.send("INVALID_CMD")
        assert result.status == CommandStatus.REJECTED
        engine.stop()

    def test_send_batch_preserves_order(self):
        order = []

        def capture(gcode):
            order.append(gcode)
            return ["ok"]

        engine, comm = self._make_engine()
        comm.send_command.side_effect = capture

        cmds = ["G28", "M104 S200", "M105"]
        results = engine.send_batch(cmds)

        assert order == cmds
        assert all(r.succeeded for r in results)
        engine.stop()

    def test_on_complete_callback_fires(self):
        received = []
        engine, _ = self._make_engine()
        engine.set_on_complete_callback(received.append)

        engine.send("M105")
        assert len(received) == 1
        assert received[0].gcode == "M105"
        engine.stop()

    def test_engine_state_observable(self):
        engine, _ = self._make_engine()
        assert engine.state in list(EngineState)
        engine.stop()
        # After full shutdown the processor has drained; state is SHUTDOWN or IDLE
        assert engine.state in (EngineState.SHUTDOWN, EngineState.IDLE)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])