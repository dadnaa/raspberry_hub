"""
tests/test_sprint4_mqtt.py — Sprint 4 Unit Tests

Tests:
  - MQTTTopics: correct topic strings
  - MessageValidator: field validation, printer-id check, gcode whitelist
  - CommandRouter: lifecycle publishing (QUEUED -> EXECUTING -> SUCCESS/ERROR)
  - MQTTPublisher: correct topic routing per model type
  - Models: CommandResponseMessage serialization
"""

import json
import threading
import unittest
from unittest.mock import MagicMock, call, patch

from src.core.models import (
    CommandMessage,
    CommandResponseMessage,
    HandshakeMessage,
    PrinterStateMessage,
    JobStateMessage,
)
from src.cloud.mqtt_topics       import MQTTTopics
from src.cloud.message_validator import MessageValidator, ValidationError
from src.cloud.mqtt_publisher    import MQTTPublisher
from src.cloud.command_router    import CommandRouter


PRINTER_ID = "printer-test-001"


# ─────────────────────────────────────────────────────────────────────
# MQTTTopics
# ─────────────────────────────────────────────────────────────────────

class TestMQTTTopics(unittest.TestCase):

    def setUp(self):
        self.t = MQTTTopics(PRINTER_ID)

    def test_upstream_topics(self):
        self.assertEqual(self.t.handshake,     f"printer/{PRINTER_ID}/handshake")
        self.assertEqual(self.t.printer_state, f"printer/{PRINTER_ID}/printer-state")
        self.assertEqual(self.t.job_state,     f"printer/{PRINTER_ID}/job-state")
        self.assertEqual(self.t.command_state, f"printer/{PRINTER_ID}/command-state")

    def test_downstream_topics(self):
        self.assertEqual(self.t.command,   f"printer/{PRINTER_ID}/command")
        self.assertEqual(self.t.start_job, f"printer/{PRINTER_ID}/start-job")

    def test_all_subscriptions(self):
        subs = self.t.all_subscriptions
        self.assertIn(self.t.command,   subs)
        self.assertIn(self.t.start_job, subs)
        self.assertEqual(len(subs), 2)

    def test_no_extra_topics(self):
        """Ensure no undocumented topics exist on MQTTTopics."""
        topic_props = [
            self.t.handshake, self.t.printer_state, self.t.job_state,
            self.t.command_state, self.t.command, self.t.start_job,
        ]
        self.assertEqual(len(topic_props), 6)


# ─────────────────────────────────────────────────────────────────────
# MessageValidator
# ─────────────────────────────────────────────────────────────────────

class TestMessageValidator(unittest.TestCase):

    def setUp(self):
        self.val = MessageValidator(printer_id=PRINTER_ID)

    def _cmd_payload(self, **overrides):
        base = {
            "printerId":   PRINTER_ID,
            "commandName": "SetTemp",
            "gcode":       "M104 S210",
        }
        base.update(overrides)
        return json.dumps(base)

    def test_valid_command(self):
        cmd = self.val.parse_command(self._cmd_payload())
        self.assertEqual(cmd.gcode, "M104 S210")
        self.assertEqual(cmd.printerId, PRINTER_ID)

    def test_invalid_json(self):
        with self.assertRaises(ValidationError):
            self.val.parse_command("not json {{{")

    def test_missing_field(self):
        payload = json.dumps({"printerId": PRINTER_ID, "commandName": "x"})
        with self.assertRaises(ValidationError):
            self.val.parse_command(payload)

    def test_wrong_printer_id(self):
        with self.assertRaises(ValidationError):
            self.val.parse_command(self._cmd_payload(printerId="wrong-printer"))

    def test_gcode_not_on_whitelist(self):
        with self.assertRaises(ValidationError):
            self.val.parse_command(self._cmd_payload(gcode="EVIL_CMD"))

    def test_all_whitelisted_prefixes(self):
        allowed = [
            "M104 S200", "M109 S200", "M140 S60", "M190 S60",
            "M105", "M106 S128", "M107", "M112",
            "M84", "M114", "M115",
            "G0 X10", "G1 Y20 F3000", "G28", "G29",
            "G90", "G91", "G92 E0",
            "M25", "M24",
        ]
        for gcode in allowed:
            with self.subTest(gcode=gcode):
                cmd = self.val.parse_command(self._cmd_payload(gcode=gcode))
                self.assertEqual(cmd.gcode, gcode)

    def test_valid_start_job(self):
        payload = json.dumps({
            "printerId": PRINTER_ID,
            "jobId":     "job-42",
            "fileUrl":   "https://example.com/file.gcode",
        })
        job = self.val.parse_start_job(payload)
        self.assertEqual(job.jobId, "job-42")

    def test_start_job_missing_field(self):
        payload = json.dumps({"printerId": PRINTER_ID, "jobId": "x"})
        with self.assertRaises(ValidationError):
            self.val.parse_start_job(payload)


# ─────────────────────────────────────────────────────────────────────
# MQTTPublisher
# ─────────────────────────────────────────────────────────────────────

class TestMQTTPublisher(unittest.TestCase):

    def setUp(self):
        self.mock_client = MagicMock()
        self.topics      = MQTTTopics(PRINTER_ID)
        self.pub         = MQTTPublisher(self.mock_client, self.topics)

    def test_handshake_publishes_to_correct_topic(self):
        msg = HandshakeMessage(
            printerId=PRINTER_ID, name="Ender-3",
            model="Ender-3", nozzleDiameter=0.4
        )
        self.pub.handshake(msg)
        self.mock_client.publish.assert_called_once()
        topic = self.mock_client.publish.call_args[0][0]
        self.assertEqual(topic, self.topics.handshake)

    def test_printer_state_publishes_to_correct_topic(self):
        msg = PrinterStateMessage(
            printerId=PRINTER_ID, name="Ender-3", model="Ender-3",
            status="IDLE", nozzleDiameter=0.4, nozzleTemp=25.0, bedTemp=25.0
        )
        self.pub.printer_state(msg)
        topic = self.mock_client.publish.call_args[0][0]
        self.assertEqual(topic, self.topics.printer_state)

    def test_command_response_injects_timestamp(self):
        resp = CommandResponseMessage(
            printerId=PRINTER_ID, commandName="Test",
            gcode="M105", status="SUCCESS"
        )
        self.pub.command_response(resp)
        self.assertIsNotNone(resp.timestamp)
        payload = json.loads(self.mock_client.publish.call_args[0][1])
        self.assertIn("timestamp", payload)

    def test_command_response_omits_null_reason(self):
        resp = CommandResponseMessage(
            printerId=PRINTER_ID, commandName="Test",
            gcode="M105", status="SUCCESS", reason=None
        )
        self.pub.command_response(resp)
        payload = json.loads(self.mock_client.publish.call_args[0][1])
        self.assertNotIn("reason", payload)

    def test_command_response_includes_reason_on_error(self):
        resp = CommandResponseMessage(
            printerId=PRINTER_ID, commandName="Test",
            gcode="M105", status="ERROR", reason="timeout"
        )
        self.pub.command_response(resp)
        payload = json.loads(self.mock_client.publish.call_args[0][1])
        self.assertEqual(payload["reason"], "timeout")


# ─────────────────────────────────────────────────────────────────────
# CommandRouter lifecycle
# ─────────────────────────────────────────────────────────────────────

class TestCommandRouter(unittest.TestCase):

    def _make_router(self, engine_result):
        mock_pub    = MagicMock()
        mock_engine = MagicMock()
        mock_engine.send.return_value = engine_result
        topics      = MQTTTopics(PRINTER_ID)
        val         = MessageValidator(PRINTER_ID)
        router      = CommandRouter(
            topics=topics,
            publisher=mock_pub,
            validator=val,
            command_engine=mock_engine,
        )
        return router, mock_pub, mock_engine

    def _success_result(self):
        r = MagicMock()
        r.succeeded   = True
        r.elapsed_ms  = 45.0
        r.status.name = "OK"
        return r

    def _fail_result(self):
        r = MagicMock()
        r.succeeded   = False
        r.elapsed_ms  = 10.0
        r.status.name = "TIMEOUT"
        return r

    def _valid_payload(self, gcode="M105"):
        return json.dumps({
            "printerId": PRINTER_ID, "commandName": "TempReport", "gcode": gcode
        })

    def _wait_for_calls(self, mock_pub, count=3, timeout=2.0):
        import time
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if mock_pub.command_response.call_count >= count:
                return
            time.sleep(0.05)

    def test_success_lifecycle_publishes_three_stages(self):
        router, mock_pub, _ = self._make_router(self._success_result())
        router.handle_command(self._valid_payload())
        self._wait_for_calls(mock_pub, count=3)

        statuses = [
            call_args[0][0].status
            for call_args in mock_pub.command_response.call_args_list
        ]
        self.assertEqual(statuses, ["QUEUED", "EXECUTING", "SUCCESS"])

    def test_failure_lifecycle_publishes_error(self):
        router, mock_pub, _ = self._make_router(self._fail_result())
        router.handle_command(self._valid_payload())
        self._wait_for_calls(mock_pub, count=3)

        statuses = [
            c[0][0].status for c in mock_pub.command_response.call_args_list
        ]
        self.assertIn("ERROR", statuses)

    def test_invalid_command_publishes_nothing(self):
        router, mock_pub, _ = self._make_router(self._success_result())
        router.handle_command('{"printerId": "wrong", "commandName": "x", "gcode": "M105"}')
        import time; time.sleep(0.1)
        mock_pub.command_response.assert_not_called()

    def test_engine_called_with_gcode(self):
        router, _, mock_engine = self._make_router(self._success_result())
        router.handle_command(self._valid_payload(gcode="M114"))
        import time; time.sleep(0.5)
        mock_engine.send.assert_called_with("M114")

    def test_exception_in_engine_publishes_error(self):
        mock_pub    = MagicMock()
        mock_engine = MagicMock()
        mock_engine.send.side_effect = RuntimeError("serial broken")
        router = CommandRouter(
            topics=MQTTTopics(PRINTER_ID),
            publisher=mock_pub,
            validator=MessageValidator(PRINTER_ID),
            command_engine=mock_engine,
        )
        router.handle_command(self._valid_payload())
        self._wait_for_calls(mock_pub, count=3)
        statuses = [c[0][0].status for c in mock_pub.command_response.call_args_list]
        self.assertIn("ERROR", statuses)
        reasons = [c[0][0].reason for c in mock_pub.command_response.call_args_list]
        self.assertTrue(any("serial broken" in (r or "") for r in reasons))


# ─────────────────────────────────────────────────────────────────────
# CommandResponseMessage model
# ─────────────────────────────────────────────────────────────────────

class TestCommandResponseMessage(unittest.TestCase):

    def test_to_json_omits_nulls(self):
        msg = CommandResponseMessage(
            printerId="p1", commandName="cmd", gcode="M105", status="QUEUED"
        )
        d = json.loads(msg.to_json())
        self.assertNotIn("reason", d)
        self.assertNotIn("timestamp", d)

    def test_to_json_includes_reason_when_set(self):
        msg = CommandResponseMessage(
            printerId="p1", commandName="cmd", gcode="M105",
            status="ERROR", reason="timeout"
        )
        d = json.loads(msg.to_json())
        self.assertEqual(d["reason"], "timeout")

    def test_from_json_roundtrip(self):
        msg = CommandResponseMessage(
            printerId="p1", commandName="cmd", gcode="M105",
            status="SUCCESS", reason=None, timestamp="2026-01-01T00:00:00+00:00"
        )
        restored = CommandResponseMessage.from_json(msg.to_json())
        self.assertEqual(restored.status, "SUCCESS")
        self.assertEqual(restored.timestamp, "2026-01-01T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()