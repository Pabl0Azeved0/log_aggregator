from __future__ import annotations

import json

from log_aggregator.domain.models import LogEvent, parse_line


def test_json_line_parses_onto_schema():
    line = json.dumps({"service": "auth", "level": "warn", "message": "token expiring", "attrs": {"u": 1}})
    e = parse_line(line)
    assert (e.service, e.level, e.message) == ("auth", "WARNING", "token expiring")
    assert e.attrs == {"u": 1}


def test_plain_line_detects_level_and_keeps_text():
    e = parse_line("2026-07-03 ERROR payment gateway timeout", default_service="payments")
    assert e.level == "ERROR"
    assert e.service == "payments"
    assert "payment gateway timeout" in e.message


def test_garbage_line_never_raises():
    e = parse_line("{{{ not json ]][")
    assert e.level == "INFO"
    assert e.message == "{{{ not json ]]["


def test_unknown_level_normalizes_to_info():
    assert LogEvent(message="x", level="verbose").level == "INFO"
