"""Concurrency regression: the lazy Kafka producer must be created and started exactly
once under a burst of concurrent publishes, and never used before start() completes."""

from __future__ import annotations

import asyncio

import aiokafka

from log_aggregator.buffer import KafkaBuffer


def test_producer_started_once_and_never_used_before_start(monkeypatch):
    started = 0

    class FakeProducer:
        def __init__(self, **_kw):
            pass

        async def start(self):
            nonlocal started
            await asyncio.sleep(0.01)  # yield so concurrent callers can interleave
            started += 1

        async def send(self, _topic, _value):
            assert started >= 1, "send() before producer start() — the race is back"

        async def stop(self):
            pass

    monkeypatch.setattr(aiokafka, "AIOKafkaProducer", FakeProducer)

    async def flow():
        buf = KafkaBuffer("broker:9092", "logs")
        await asyncio.gather(*[buf.publish([{"m": i}]) for i in range(10)])

    asyncio.run(flow())
    assert started == 1
