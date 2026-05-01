"""
test_sprint1.py — Sprint 1 Unit Tests
Tests for telemetry parsing and "ok" detection logic.

Run:
    python -m pytest tests/test_sprint1.py -v

No hardware required — all tests use mock data.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from src.utils.telemetry_parser import (
    parse_temperature_line,
    parse_position_line,
    is_ok_response,
    is_error_response,
)


# ── Temperature Parser Tests ─────────────────────────────────────────────

class TestTemperatureParser:

    def test_parses_nozzle_and_bed(self):
        line = "ok T:210.5 /210.0 B:60.3 /60.0 @:127 B@:0"
        result = parse_temperature_line(line)
        assert result is not None
        assert result["nozzle"] == pytest.approx(210.5)
        assert result["bed"] == pytest.approx(60.3)

    def test_parses_nozzle_only(self):
        line = "T:25.1 /0.0"
        result = parse_temperature_line(line)
        assert result is not None
        assert result["nozzle"] == pytest.approx(25.1)
        assert "bed" not in result

    def test_returns_none_for_ok_line(self):
        assert parse_temperature_line("ok") is None

    def test_returns_none_for_position_line(self):
        line = "X:0.00 Y:0.00 Z:0.00 E:0.00"
        assert parse_temperature_line(line) is None

    def test_returns_none_for_empty_string(self):
        assert parse_temperature_line("") is None

    def test_parses_cold_temperatures(self):
        line = "T:20.0 /0.0 B:19.5 /0.0"
        result = parse_temperature_line(line)
        assert result["nozzle"] == pytest.approx(20.0)
        assert result["bed"] == pytest.approx(19.5)

    def test_parses_temperature_embedded_in_ok_line(self):
        # Marlin sometimes sends "ok T:210 /210 B:60 /60"
        line = "ok T:200.0 /200.0 B:55.0 /55.0"
        result = parse_temperature_line(line)
        assert result is not None
        assert result["nozzle"] == pytest.approx(200.0)


# ── Position Parser Tests ────────────────────────────────────────────────

class TestPositionParser:

    def test_parses_home_position(self):
        line = "X:0.00 Y:0.00 Z:0.00 E:0.00 Count X:0 Y:0 Z:0"
        result = parse_position_line(line)
        assert result == {"x": 0.0, "y": 0.0, "z": 0.0}

    def test_parses_mid_print_position(self):
        line = "X:125.30 Y:87.10 Z:5.20 E:1240.5"
        result = parse_position_line(line)
        assert result["x"] == pytest.approx(125.30)
        assert result["y"] == pytest.approx(87.10)
        assert result["z"] == pytest.approx(5.20)

    def test_parses_negative_position(self):
        line = "X:-5.00 Y:-2.50 Z:0.20 E:0.00"
        result = parse_position_line(line)
        assert result["x"] == pytest.approx(-5.0)
        assert result["y"] == pytest.approx(-2.5)

    def test_returns_none_for_temperature_line(self):
        assert parse_position_line("T:210.5 B:60.0") is None

    def test_returns_none_for_ok_line(self):
        assert parse_position_line("ok") is None


# ── OK Detection Tests ───────────────────────────────────────────────────

class TestOkDetection:

    def test_simple_ok(self):
        assert is_ok_response("ok") is True

    def test_ok_with_temperature(self):
        assert is_ok_response("ok T:210.5 /210.0 B:60.3 /60.0") is True

    def test_ok_case_insensitive(self):
        assert is_ok_response("OK") is True
        assert is_ok_response("Ok") is True

    def test_non_ok_line(self):
        assert is_ok_response("T:210.5 B:60.0") is False

    def test_empty_line(self):
        assert is_ok_response("") is False

    def test_error_line_is_not_ok(self):
        assert is_ok_response("Error: checksum mismatch") is False


# ── Error Detection Tests ────────────────────────────────────────────────

class TestErrorDetection:

    def test_detects_error_prefix(self):
        assert is_error_response("Error: checksum mismatch") is True

    def test_detects_double_bang(self):
        assert is_error_response("!! Something went wrong") is True

    def test_ok_is_not_error(self):
        assert is_error_response("ok") is False

    def test_temperature_line_is_not_error(self):
        assert is_error_response("T:210.0 B:60.0") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])