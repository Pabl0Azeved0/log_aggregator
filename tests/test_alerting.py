"""Alerting: sliding-window rule evaluation (deterministic via injected time), per-tenant
state, rule parsing, and the consume→notify→persist worker loop."""

from __future__ import annotations

import asyncio

from log_aggregator import alerting
from log_aggregator.adapters.memory_buffer import MemoryBuffer
from log_aggregator.adapters.memory_store import MemoryStore
from log_aggregator.domain.rules import Rule, RuleEngine, load_rules
from log_aggregator.models import LogEvent


def _err(tenant="default", service="checkout"):
    return {"level": "ERROR", "service": service, "tenant": tenant}


def test_load_rules():
    assert load_rules("") == []
    rules = load_rules('[{"name":"x","level":"ERROR","threshold":5,"window_s":10}]')
    assert rules[0].name == "x" and rules[0].threshold == 5 and rules[0].cooldown_s == 300.0


def test_fires_at_threshold_then_cooldown_then_again():
    engine = RuleEngine([Rule("r", threshold=3, window_s=10, cooldown_s=100, level="ERROR")])
    assert engine.observe(_err(), 0) == []
    assert engine.observe(_err(), 1) == []
    fired = engine.observe(_err(), 2)               # 3 within 10s → fire
    assert len(fired) == 1 and fired[0]["count"] == 3 and fired[0]["rule"] == "r"
    assert engine.observe(_err(), 3) == []          # still in cooldown
    assert engine.observe(_err(), 200) == []        # old hits aged out; only 1 in window
    assert engine.observe(_err(), 201) == []
    assert engine.observe(_err(), 202) != []        # 3 again + cooldown elapsed → fire


def test_ignores_non_matching_events():
    engine = RuleEngine([Rule("r", threshold=2, window_s=10, cooldown_s=100, level="ERROR", service="checkout")])
    assert engine.observe({"level": "INFO", "service": "checkout", "tenant": "default"}, 0) == []
    assert engine.observe(_err(service="auth"), 1) == []      # wrong service
    assert engine.observe(_err(), 2) == []                    # 1st match
    assert engine.observe(_err(), 3) != []                    # 2nd match → fire


def test_rule_state_is_per_tenant():
    engine = RuleEngine([Rule("r", threshold=2, window_s=10, cooldown_s=100, level="ERROR")])
    assert engine.observe(_err("acme"), 0) == []
    assert engine.observe(_err("globex"), 0) == []            # separate counter
    assert engine.observe(_err("acme"), 1)[0]["tenant"] == "acme"
    assert engine.observe(_err("globex"), 1)[0]["tenant"] == "globex"


def test_worker_records_and_notifies():
    async def flow():
        buf = MemoryBuffer(maxsize=100)
        store = MemoryStore()
        notified: list = []

        async def notify(alert):
            notified.append(alert)

        engine = RuleEngine([Rule("burst", threshold=2, window_s=60, cooldown_s=60, level="ERROR")])
        await buf.publish([
            LogEvent(level="ERROR", message="a").model_dump(mode="json"),
            LogEvent(level="ERROR", message="b").model_dump(mode="json"),
        ])
        fired = await alerting.run(buf, engine, notify, store, once=True)
        assert fired == 1 and len(notified) == 1
        alerts = await store.recent_alerts("default")
        assert len(alerts) == 1 and alerts[0]["rule"] == "burst"

    asyncio.run(flow())
