"""
tests/test_sprint3_telemetry.py — Sprint 3 Unit Tests

Tests:
  - telemetry_parser: all line pattern recognition
  - StateManager: thread safety, snapshots, listeners
  - TelemetryEngine: line processing, status transitions
"""

import queue
import threading
import time
import unittest
from datetime import datetime

from src.telemetry.telemetry_parser import (
    parse_temperature,
    parse_position,
    parse_sd_progress,
    is_sd_done,
    is_paused,
    is_resumed,
    is_reboot,
)
from src.telemetry.state_manager import StateManager
from src.telemetry.printer_state import PrinterStatus
from src.telemetry.telemetry_engine import TelemetryEngine


# ─────────────────────────────────────────────────────────────────────
# Parser tests
# ─────────────────────────────────────────────────────────────────────

class TestTemperatureParser(unittest.TestCase):

    def test_basic_temp(self):
        r = parse_temperature("ok T:205.3 B:60.1")
        self.assertIsNotNone(r)
        self.assertAlmostEqual(r.nozzle, 205.3)
        self.assertAlmostEqual(r.bed, 60.1)
        self.assertIsNone(r.nozzle_target)

    def test_temp_with_targets(self):
        r = parse_temperature("T:205.3 /210.0 B:60.1 /60.0")
        self.assertAlmostEqual(r.nozzle_target, 210.0)
        self.assertAlmostEqual(r.bed_target, 60.0)

    def test_no_temp(self):
        self.assertIsNone(parse_temperature("ok"))
        self.assertIsNone(parse_temperature(""))

    def test_nozzle_only(self):
        r = parse_temperature("T:180.0")
        self.assertAlmostEqual(r.nozzle, 180.0)
        self.assertIsNone(r.bed)


class TestPositionParser(unittest.TestCase):

    def test_basic_position(self):
        r = parse_position("X:10.00 Y:20.50 Z:5.00 E:0.00")
        self.assertAlmostEqual(r.x, 10.0)
        self.assertAlmostEqual(r.y, 20.5)
        self.assertAlmostEqual(r.z, 5.0)

    def test_negative_position(self):
        r = parse_position("X:-1.00 Y:0.00 Z:0.20")
        self.assertAlmostEqual(r.x, -1.0)

    def test_no_position(self):
        self.assertIsNone(parse_position("ok T:200"))


class TestProgressParser(unittest.TestCase):

    def test_sd_progress(self):
        r = parse_sd_progress("SD printing byte 12300/56789")
        self.assertAlmostEqual(r.percent, round(12300 / 56789 * 100, 1))

    def test_sd_done(self):
        self.assertTrue(is_sd_done("Done printing file"))
        self.assertFalse(is_sd_done("still printing"))

    def test_no_progress(self):
        self.assertIsNone(parse_sd_progress("ok T:200"))


class TestStatusDetectors(unittest.TestCase):

    def test_paused(self):
        self.assertTrue(is_paused("// action:pause"))
        self.assertTrue(is_paused("M25"))

    def test_resumed(self):
        self.assertTrue(is_resumed("// action:resume"))
        self.assertTrue(is_resumed("M24"))

    def test_reboot(self):
        self.assertTrue(is_reboot("start"))
        self.assertTrue(is_reboot("Marlin 2.0"))


# ─────────────────────────────────────────────────────────────────────
# StateManager tests
# ─────────────────────────────────────────────────────────────────────

class TestStateManager(unittest.TestCase):

    def setUp(self):
        self.mgr = StateManager()

    def test_initial_state(self):
        s = self.mgr.get_snapshot()
        self.assertEqual(s.status, PrinterStatus.UNKNOWN)
        self.assertIsNone(s.nozzle_temp)

    def test_update_single_field(self):
        self.mgr.update(nozzle_temp=200.0)
        s = self.mgr.get_snapshot()
        self.assertAlmostEqual(s.nozzle_temp, 200.0)

    def test_snapshot_is_copy(self):
        self.mgr.update(nozzle_temp=200.0)
        s1 = self.mgr.get_snapshot()
        self.mgr.update(nozzle_temp=210.0)
        # s1 must not change
        self.assertAlmostEqual(s1.nozzle_temp, 200.0)

    def test_listener_called_on_change(self):
        events = []
        self.mgr.register_listener(lambda snap, ch: events.append(ch))
        self.mgr.update(nozzle_temp=195.0)
        self.assertEqual(len(events), 1)
        self.assertIn("nozzle_temp", events[0])

    def test_listener_not_called_if_unchanged(self):
        self.mgr.update(nozzle_temp=200.0)
        events = []
        self.mgr.register_listener(lambda snap, ch: events.append(ch))
        self.mgr.update(nozzle_temp=200.0)  # same value
        self.assertEqual(len(events), 0)

    def test_thread_safety(self):
        errors = []

        def writer():
            for i in range(100):
                try:
                    self.mgr.update(nozzle_temp=float(i))
                except Exception as e:
                    errors.append(e)

        def reader():
            for _ in range(100):
                try:
                    self.mgr.get_snapshot()
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=writer) for _ in range(3)]
        threads += [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Thread safety errors: {errors}")

    def test_unknown_field_ignored(self):
        # Should not raise
        self.mgr.update(nonexistent_field=42)


# ─────────────────────────────────────────────────────────────────────
# TelemetryEngine integration tests (no real serial needed)
# ─────────────────────────────────────────────────────────────────────

class TestTelemetryEngine(unittest.TestCase):

    def _make_engine(self):
        q   = queue.Queue()
        mgr = StateManager()
        eng = TelemetryEngine(line_queue=q, state_manager=mgr)
        return q, mgr, eng

    def _push_and_wait(self, q, lines, wait=0.15):
        for line in lines:
            q.put(line)
        time.sleep(wait)

    def test_temperature_updates_state(self):
        q, mgr, eng = self._make_engine()
        eng.start()
        self._push_and_wait(q, ["ok T:205.3 B:60.1"])
        eng.stop()
        s = mgr.get_snapshot()
        self.assertAlmostEqual(s.nozzle_temp, 205.3)
        self.assertAlmostEqual(s.bed_temp, 60.1)

    def test_position_updates_state(self):
        q, mgr, eng = self._make_engine()
        eng.start()
        self._push_and_wait(q, ["X:10.00 Y:20.50 Z:5.00 E:0.00"])
        eng.stop()
        s = mgr.get_snapshot()
        self.assertAlmostEqual(s.position_x, 10.0)

    def test_pause_sets_status(self):
        q, mgr, eng = self._make_engine()
        eng.start()
        self._push_and_wait(q, ["// action:pause"])
        eng.stop()
        self.assertEqual(mgr.get_snapshot().status, PrinterStatus.PAUSED)

    def test_sd_done_sets_idle(self):
        q, mgr, eng = self._make_engine()
        eng.start()
        self._push_and_wait(q, ["Done printing file"])
        eng.stop()
        s = mgr.get_snapshot()
        self.assertEqual(s.status, PrinterStatus.IDLE)
        self.assertAlmostEqual(s.progress_pct, 100.0)

    def test_event_callback_fires(self):
        events = []
        q   = queue.Queue()
        mgr = StateManager()
        eng = TelemetryEngine(line_queue=q, state_manager=mgr, on_event=events.append)
        eng.start()
        self._push_and_wait(q, ["ok T:200.0 B:55.0"])
        eng.stop()
        self.assertGreater(len(events), 0)

    def test_noisy_lines_do_not_crash(self):
        q, mgr, eng = self._make_engine()
        eng.start()
        noise = ["", "   ", "!!garbage!!", "ok", "echo: busy: processing"]
        self._push_and_wait(q, noise)
        eng.stop()  # must not raise


if __name__ == "__main__":
    unittest.main()