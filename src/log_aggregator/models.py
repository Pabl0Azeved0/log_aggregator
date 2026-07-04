from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator

_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_ALIASES = {"WARN": "WARNING", "FATAL": "CRITICAL", "ERR": "ERROR"}
_LEVEL_RE = re.compile(r"\b(DEBUG|INFO|WARNING|WARN|ERROR|ERR|CRITICAL|FATAL)\b", re.I)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LogEvent(BaseModel):
    """One structured log event — the pipeline's unit of work."""

    timestamp: datetime = Field(default_factory=_utcnow)
    level: str = "INFO"
    service: str = "unknown"
    message: str
    attrs: dict[str, Any] = {}

    @field_validator("level")
    @classmethod
    def _normalize_level(cls, v: str) -> str:
        up = (v or "INFO").strip().upper()
        up = _ALIASES.get(up, up)
        return up if up in _LEVELS else "INFO"


def parse_line(raw: str, default_service: str = "unknown") -> LogEvent:
    """Tolerantly parse one raw log line into a LogEvent.

    JSON lines map onto the schema directly; plain-text lines get level detection by
    keyword and the whole line preserved as the message. Never raises on content — a
    garbage line still becomes an INFO event with the text kept.
    """
    text = raw.strip()
    try:
        data = json.loads(text)
        if isinstance(data, dict) and data.get("message"):
            data.setdefault("service", default_service)
            return LogEvent(**{k: v for k, v in data.items() if k in LogEvent.model_fields})
    except Exception:
        pass
    m = _LEVEL_RE.search(text)
    level = m.group(1) if m else "INFO"
    return LogEvent(level=level, service=default_service, message=text)
