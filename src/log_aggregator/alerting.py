"""Alerting worker: consume the logs stream on its own consumer group, evaluate threshold
rules over sliding windows per (tenant, rule), and notify + persist when one fires.

Run: python -m log_aggregator.alerting
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from log_aggregator.buffer import Buffer, make_buffer
from log_aggregator.composition import make_store
from log_aggregator.config import Settings, get_settings
from log_aggregator.ports.store import Store

log = logging.getLogger("alerting")


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


def make_notifier(settings: Settings):
    webhook = settings.alert_webhook

    async def notify(alert: dict) -> None:
        text = f":rotating_light: {alert['message']} (tenant={alert['tenant']})"
        if webhook:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.post(webhook, json={"text": text})
            except Exception as exc:  # noqa: BLE001 — a flaky webhook must not kill the worker
                log.warning("webhook post failed: %s", exc)
        else:
            log.warning("ALERT %s", text)

    return notify


async def run(buffer: Buffer, engine: RuleEngine, notify, store: Store, once: bool = False) -> int:
    fired = 0
    while True:
        batch = await buffer.get_batch(500, 1.0)
        if batch:
            now = time.time()
            for event in batch:
                for alert in engine.observe(event, now):
                    fired += 1
                    await notify(alert)
                    await store.record_alert(alert)
            await buffer.commit()
        elif once:
            return fired


async def _serve(buffer: Buffer, engine: RuleEngine, notify, store: Store) -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(run(buffer, engine, notify, store))
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        log.info("alerting shutting down")
    finally:
        await buffer.close()
        await store.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = get_settings()
    engine = RuleEngine(load_rules(settings.alert_rules))
    buffer = make_buffer(settings)
    store = make_store(settings)
    notify = make_notifier(settings)
    log.info("alerting starting: %d rule(s), group=%s", len(engine.rules), settings.kafka_group)
    asyncio.run(_serve(buffer, engine, notify, store))


if __name__ == "__main__":
    main()
