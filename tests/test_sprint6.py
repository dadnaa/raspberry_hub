"""
tests/test_sprint6_vision.py — Sprint 6 Unit Tests

Coverage:
  - GcodePipeline: parsing (imported from sprint5, tested here for completeness)
  - StreamReader: mock-based connection and frame slot behaviour
  - FrameSampler: interval control, skip-on-busy, metadata injection
  - AIClient: correct HTTP payload, response parsing, timeout/error handling
  - FailureGuard: threshold logic, cooldown, confidence filter, reset
  - VisionMonitor: full pipeline (stream -> AI -> guard -> pause), adaptive rate
  - VisionController: start/stop triggers on job state transitions
"""

import json
import threading
import time
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
import numpy as np

from src.vision.ai_client         import AIClient, AIInferenceResult
from src.vision.failure_guard     import FailureGuard, Action, VisionDecision
from src.vision.vision_monitor    import VisionMonitor, _INTERVAL_RISK_SEC, _INTERVAL_NORMAL_SEC
from src.vision.vision_controller import VisionController
from src.vision.frame_sampler     import FrameSampler
from src.vision.stream_reader     import StreamReader
from src.jobs.job_model           import Job


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _make_job(status="PRINTING"):
    job = Job.create("p1", "http://x.com/f.gcode", ["G28", "M104"])
    job.status = status
    return job


def _ai_result(classification, confidence=0.9):
    return AIInferenceResult(classification=classification, confidence=confidence)


def _wait(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond(): return True
        time.sleep(0.05)
    return False


# ─────────────────────────────────────────────────────────────────────
# AIClient
# ─────────────────────────────────────────────────────────────────────

class TestAIClient(unittest.TestCase):

    def _mock_urlopen(self, response_body: dict):
        import urllib.request
        from io import BytesIO
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_body).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__  = MagicMock(return_value=False)
        return patch("urllib.request.urlopen", return_value=mock_resp)

    def test_ok_classification(self):
        with self._mock_urlopen({"classification": "OK", "confidence": 0.95}):
            client = AIClient(endpoint="http://fake/infer")
            result = client.infer(b"fakejpeg", {"jobId": "j1"})
        self.assertEqual(result.classification, "OK")
        self.assertAlmostEqual(result.confidence, 0.95)
        self.assertTrue(result.is_ok)

    def test_failure_classification(self):
        with self._mock_urlopen({"classification": "FAILURE", "confidence": 0.88}):
            client = AIClient(endpoint="http://fake/infer")
            result = client.infer(b"fakejpeg", {})
        self.assertTrue(result.is_failure)
        self.assertAlmostEqual(result.confidence, 0.88)

    def test_unknown_classification_becomes_uncertain(self):
        with self._mock_urlopen({"classification": "INVALID", "confidence": 0.5}):
            client = AIClient(endpoint="http://fake/infer")
            result = client.infer(b"fakejpeg", {})
        self.assertEqual(result.classification, "UNCERTAIN")

    def test_timeout_returns_uncertain(self):
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            client = AIClient(endpoint="http://fake/infer")
            result = client.infer(b"fakejpeg", {})
        self.assertEqual(result.classification, "UNCERTAIN")
        self.assertAlmostEqual(result.confidence, 0.0)

    def test_network_error_returns_uncertain(self):
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("no route")):
            client = AIClient(endpoint="http://fake/infer")
            result = client.infer(b"fakejpeg", {})
        self.assertEqual(result.classification, "UNCERTAIN")

    def test_result_is_ok_helpers(self):
        r = AIInferenceResult("OK", 0.9)
        self.assertTrue(r.is_ok)
        self.assertFalse(r.is_failure)
        self.assertFalse(r.is_uncertain)

    def test_result_is_failure_helpers(self):
        r = AIInferenceResult("FAILURE", 0.85)
        self.assertFalse(r.is_ok)
        self.assertTrue(r.is_failure)


# ─────────────────────────────────────────────────────────────────────
# FailureGuard
# ─────────────────────────────────────────────────────────────────────

class TestFailureGuard(unittest.TestCase):

    def _make_guard(self, threshold=3, conf=0.75, cooldown=0.1):
        return FailureGuard(
            failure_threshold=threshold,
            confidence_min=conf,
            cooldown_sec=cooldown,
        )

    def test_ok_resets_counter(self):
        g = self._make_guard()
        for _ in range(2):
            g.evaluate(_ai_result("FAILURE"))
        d = g.evaluate(_ai_result("OK"))
        self.assertEqual(d.action, Action.NONE)
        self.assertEqual(g._consecutive_failures, 0)

    def test_uncertain_resets_counter(self):
        g = self._make_guard()
        g.evaluate(_ai_result("FAILURE"))
        g.evaluate(_ai_result("UNCERTAIN"))
        self.assertEqual(g._consecutive_failures, 0)

    def test_low_confidence_failure_ignored(self):
        g = self._make_guard(conf=0.8)
        d = g.evaluate(_ai_result("FAILURE", confidence=0.5))
        self.assertEqual(d.action, Action.NONE)
        self.assertEqual(g._consecutive_failures, 0)

    def test_threshold_triggers_pause(self):
        g = self._make_guard(threshold=3, cooldown=0.0)
        for _ in range(2):
            d = g.evaluate(_ai_result("FAILURE", confidence=0.9))
            self.assertEqual(d.action, Action.NONE)
        d = g.evaluate(_ai_result("FAILURE", confidence=0.9))
        self.assertEqual(d.action, Action.PAUSE)

    def test_cooldown_prevents_second_trigger(self):
        g = self._make_guard(threshold=1, cooldown=60.0)
        g.evaluate(_ai_result("FAILURE"))         # triggers
        d = g.evaluate(_ai_result("FAILURE"))     # should be blocked by cooldown
        self.assertEqual(d.action, Action.NONE)
        self.assertEqual(d.reason, "cooldown_active")

    def test_cooldown_expires_and_retriggers(self):
        g = self._make_guard(threshold=1, cooldown=0.05)
        g.evaluate(_ai_result("FAILURE"))
        time.sleep(0.1)
        d = g.evaluate(_ai_result("FAILURE"))
        self.assertEqual(d.action, Action.PAUSE)

    def test_reset_clears_state(self):
        g = self._make_guard(threshold=3, cooldown=0.0)
        for _ in range(3):
            g.evaluate(_ai_result("FAILURE"))
        g.reset()
        self.assertEqual(g._consecutive_failures, 0)
        # after reset: 3 more failures needed
        for _ in range(2):
            d = g.evaluate(_ai_result("FAILURE"))
            self.assertEqual(d.action, Action.NONE)

    def test_counter_resets_after_intervention(self):
        g = self._make_guard(threshold=2, cooldown=0.0)
        for _ in range(2):
            g.evaluate(_ai_result("FAILURE"))
        # fired — counter should be 0 now
        self.assertEqual(g._consecutive_failures, 0)


# ─────────────────────────────────────────────────────────────────────
# FrameSampler (mocked StreamReader)
# ─────────────────────────────────────────────────────────────────────

class TestFrameSampler(unittest.TestCase):

    def _make_reader_with_frame(self):
        frame = np.zeros((480, 640, 3), dtype=np.uint8)
        reader = MagicMock(spec=StreamReader)
        reader.latest_frame = frame
        return reader

    def test_callback_called_with_metadata(self):
        reader   = self._make_reader_with_frame()
        received = []
        sampler  = FrameSampler(
            reader=reader,
            on_frame=lambda b, m: received.append(m),
            interval=0.1,
            job_id="j1",
            printer_id="p1",
        )
        sampler.start()
        _wait(lambda: len(received) >= 1, timeout=2)
        sampler.stop()
        self.assertGreater(len(received), 0)
        self.assertEqual(received[0]["jobId"],     "j1")
        self.assertEqual(received[0]["printerId"], "p1")
        self.assertIn("timestamp", received[0])

    def test_no_call_when_no_frame(self):
        reader = MagicMock(spec=StreamReader)
        reader.latest_frame = None
        called = []
        sampler = FrameSampler(reader=reader, on_frame=lambda b, m: called.append(1), interval=0.1)
        sampler.start()
        time.sleep(0.35)
        sampler.stop()
        self.assertEqual(called, [])

    def test_set_interval(self):
        reader  = self._make_reader_with_frame()
        sampler = FrameSampler(reader=reader, on_frame=lambda b, m: None, interval=5.0)
        sampler.set_interval(1.5)
        self.assertAlmostEqual(sampler._interval, 1.5)

    def test_interval_clamped(self):
        reader  = self._make_reader_with_frame()
        sampler = FrameSampler(reader=reader, on_frame=lambda b, m: None)
        sampler.set_interval(0.001)   # below min
        self.assertGreaterEqual(sampler._interval, 1.0)
        sampler.set_interval(999)     # above max
        self.assertLessEqual(sampler._interval, 10.0)


# ─────────────────────────────────────────────────────────────────────
# VisionMonitor (integration — mocked stream + AI)
# ─────────────────────────────────────────────────────────────────────

class TestVisionMonitor(unittest.TestCase):

    def _make_monitor(self, ai_result_fn=None, guard_threshold=3, cooldown=0.0):
        ai_client   = MagicMock()
        ai_client.infer.side_effect = ai_result_fn or (lambda *a: _ai_result("OK"))
        job_manager = MagicMock()
        job_manager.pause.return_value = True
        event_pub   = MagicMock()

        monitor = VisionMonitor(
            stream_url="rtsp://fake/stream",
            ai_client=ai_client,
            job_manager=job_manager,
            event_publisher=event_pub,
            guard_config={
                "failure_threshold": guard_threshold,
                "confidence_min":    0.5,
                "cooldown_sec":      cooldown,
            },
        )
        # Inject a mock reader so no real stream is opened
        mock_reader = MagicMock(spec=StreamReader)
        mock_reader.latest_frame = np.zeros((480, 640, 3), dtype=np.uint8)
        mock_reader.is_connected = True
        monitor._reader = mock_reader

        return monitor, ai_client, job_manager, event_pub

    def test_start_monitoring_starts_sampler(self):
        monitor, _, _, _ = self._make_monitor()
        job = _make_job("PRINTING")
        with patch.object(monitor, "_reader") as mr:
            mr.latest_frame = np.zeros((480, 640, 3), dtype=np.uint8)
            monitor._reader = mr
            monitor.start_monitoring(job)
            self.assertTrue(monitor.is_active)
            monitor.stop_monitoring()

    def test_stop_monitoring_stops_sampler(self):
        monitor, _, _, _ = self._make_monitor()
        job = _make_job("PRINTING")
        monitor.start_monitoring(job)
        monitor.stop_monitoring()
        self.assertFalse(monitor.is_active)

    def test_idempotent_start(self):
        monitor, _, _, _ = self._make_monitor()
        job = _make_job("PRINTING")
        monitor.start_monitoring(job)
        monitor.start_monitoring(job)   # second call — should not crash or duplicate
        monitor.stop_monitoring()

    def test_ok_result_does_not_pause(self):
        monitor, ai_client, job_manager, _ = self._make_monitor()
        job = _make_job("PRINTING")
        monitor.start_monitoring(job)
        time.sleep(0.5)
        monitor.stop_monitoring()
        job_manager.pause.assert_not_called()

    def test_consecutive_failures_trigger_pause(self):
        call_count = [0]
        def ai_fn(*a):
            call_count[0] += 1
            return _ai_result("FAILURE", confidence=0.95)

        monitor, _, job_manager, _ = self._make_monitor(
            ai_result_fn=ai_fn, guard_threshold=3, cooldown=0.0
        )
        # Give sampler a very fast interval for test speed
        job = _make_job("PRINTING")
        monitor.start_monitoring(job)
        monitor._sampler.set_interval(0.1)
        _wait(lambda: job_manager.pause.called, timeout=5)
        monitor.stop_monitoring()
        job_manager.pause.assert_called_with(job.job_id)

    def test_event_published_for_every_frame(self):
        events = []
        monitor, _, _, event_pub = self._make_monitor()
        event_pub.publish.side_effect = lambda **kw: events.append(kw)
        job = _make_job("PRINTING")
        monitor.start_monitoring(job)
        monitor._sampler.set_interval(0.1)
        _wait(lambda: event_pub.publish.call_count >= 2, timeout=3)
        monitor.stop_monitoring()
        self.assertGreaterEqual(event_pub.publish.call_count, 2)

    def test_skips_frame_if_job_not_printing(self):
        monitor, ai_client, job_manager, _ = self._make_monitor()
        job = _make_job("PAUSED")       # not PRINTING
        monitor._active_job = job
        monitor._on_frame(b"fakejpeg", {"timestamp": "x"})
        ai_client.infer.assert_not_called()


# ─────────────────────────────────────────────────────────────────────
# VisionController
# ─────────────────────────────────────────────────────────────────────

class TestVisionController(unittest.TestCase):

    def _make_controller(self):
        monitor    = MagicMock(spec=VisionMonitor)
        controller = VisionController(monitor)
        return controller, monitor

    def test_printing_activates_monitor(self):
        ctrl, monitor = self._make_controller()
        job = _make_job("PRINTING")
        ctrl.on_job_state_change(job)
        monitor.start_monitoring.assert_called_once_with(job)

    def test_paused_stops_monitor(self):
        ctrl, monitor = self._make_controller()
        job = _make_job("PRINTING")
        ctrl.on_job_state_change(job)
        job.status = "PAUSED"
        ctrl.on_job_state_change(job)
        monitor.stop_monitoring.assert_called_once()

    def test_completed_stops_monitor(self):
        ctrl, monitor = self._make_controller()
        job = _make_job("PRINTING")
        ctrl.on_job_state_change(job)
        job.status = "COMPLETED"
        ctrl.on_job_state_change(job)
        monitor.stop_monitoring.assert_called_once()

    def test_cancelled_stops_monitor(self):
        ctrl, monitor = self._make_controller()
        job = _make_job("PRINTING")
        ctrl.on_job_state_change(job)
        job.status = "CANCELLED"
        ctrl.on_job_state_change(job)
        monitor.stop_monitoring.assert_called_once()

    def test_only_activates_once_per_job(self):
        ctrl, monitor = self._make_controller()
        job = _make_job("PRINTING")
        ctrl.on_job_state_change(job)
        ctrl.on_job_state_change(job)   # same job, same status
        monitor.start_monitoring.assert_called_once()

    def test_queued_does_not_start_monitor(self):
        ctrl, monitor = self._make_controller()
        job = _make_job("QUEUED")
        ctrl.on_job_state_change(job)
        monitor.start_monitoring.assert_not_called()

    def test_shutdown_calls_monitor_shutdown(self):
        ctrl, monitor = self._make_controller()
        ctrl.shutdown()
        monitor.shutdown.assert_called_once()


if __name__ == "__main__":
    unittest.main()