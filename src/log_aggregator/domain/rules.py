"""Pure alerting domain: threshold rules and a deterministic sliding-window engine. No I/O,
no framework — fully unit-testable with injected time."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Rule:
    name: str
    threshold: int
    window_s: float
    cooldown_s: float = 300.0
    level: str | None = None
    service: str | None = None


def load_rules(raw: str) -> list[Rule]:
    """Parse ALERT_RULES (a JSON array of rule objects) into Rule instances."""
    if not raw.strip():
        return []
    return [
        Rule(
            name=r["name"],
            threshold=int(r["threshold"]),
            window_s=float(r["window_s"]),
            cooldown_s=float(r.get("cooldown_s", 300)),
            level=r.get("level"),
            service=r.get("service"),
        )
        for r in json.loads(raw)
    ]


class RuleEngine:
    """Sliding-window threshold detection, evaluated and bounded per (rule, tenant). Time is
    injected (`now`) so it is fully deterministic to test. A rule fires when at least
    `threshold` matching events fall inside `window_s`, at most once per `cooldown_s`."""

    def __init__(self, rules: list[Rule]) -> None:
        self.rules = rules
        self._hits: dict[tuple[str, str], deque[float]] = {}
        self._fired_at: dict[tuple[str, str], float] = {}

    def observe(self, event: dict, now: float) -> list[dict]:
        alerts: list[dict] = []
        tenant = event.get("tenant", "default")
        for rule in self.rules:
            if rule.level and event.get("level") != rule.level:
                continue
            if rule.service and event.get("service") != rule.service:
                continue
            key = (rule.name, tenant)
            hits = self._hits.setdefault(key, deque())
            hits.append(now)
            cutoff = now - rule.window_s
            while hits and hits[0] < cutoff:
                hits.popleft()
            while len(hits) > rule.threshold:  # threshold is enough to decide — bound memory
                hits.popleft()
            if len(hits) >= rule.threshold and now - self._fired_at.get(key, float("-inf")) >= rule.cooldown_s:
                self._fired_at[key] = now
                alerts.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "tenant": tenant,
                    "rule": rule.name,
                    "level": rule.level,
                    "service": rule.service,
                    "count": len(hits),
                    "window_s": rule.window_s,
                    "message": f"{rule.name}: {len(hits)} matching events within {rule.window_s:g}s",
                })
        return alerts
