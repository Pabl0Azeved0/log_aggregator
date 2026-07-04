from __future__ import annotations

import asyncio
import json
from typing import Protocol

from log_aggregator.config import Settings


class BufferFull(Exception):
    """Raised when the buffer cannot accept more events (backpressure signal)."""


class Buffer(Protocol):
    async def publish(self, events: list[dict]) -> None: ...
    async def get_batch(self, max_items: int, timeout_s: float) -> list[dict]: ...
    async def close(self) -> None: ...


class MemoryBuffer:
    """In-process asyncio.Queue buffer — offline tests and the `make smoke` path only.
    Bounded: publishing past capacity raises BufferFull (the ingest API turns that
    into a 429), mirroring how the real pipeline signals backpressure."""

    def __init__(self, maxsize: int) -> None:
        self._q: asyncio.Queue[dict] = asyncio.Queue(maxsize=maxsize)

    async def publish(self, events: list[dict]) -> None:
        for e in events:
            try:
                self._q.put_nowait(e)
            except asyncio.QueueFull as exc:
                raise BufferFull("memory buffer at capacity") from exc

    async def get_batch(self, max_items: int, timeout_s: float) -> list[dict]:
        batch: list[dict] = []
        try:
            batch.append(await asyncio.wait_for(self._q.get(), timeout=timeout_s))
        except asyncio.TimeoutError:
            return batch
        while len(batch) < max_items:
            try:
                batch.append(self._q.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def close(self) -> None:
        return None


class KafkaBuffer:
    """Kafka-backed buffer (the real path). Producer/consumer are created lazily so
    importing this module never requires a running broker. Consumer uses a consumer
    group with auto-commit — at-least-once delivery (documented in the README)."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        self._bootstrap = bootstrap
        self._topic = topic
        self._producer = None
        self._consumer = None

    async def publish(self, events: list[dict]) -> None:
        if self._producer is None:
            from aiokafka import AIOKafkaProducer

            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap,
                value_serializer=lambda v: json.dumps(v).encode(),
                linger_ms=5,
            )
            await self._producer.start()
        for e in events:
            await self._producer.send(self._topic, e)

    async def get_batch(self, max_items: int, timeout_s: float) -> list[dict]:
        if self._consumer is None:
            from aiokafka import AIOKafkaConsumer

            self._consumer = AIOKafkaConsumer(
                self._topic,
                bootstrap_servers=self._bootstrap,
                group_id="indexer",
                value_deserializer=lambda b: json.loads(b.decode()),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            await self._consumer.start()
        polled = await self._consumer.getmany(
            timeout_ms=int(timeout_s * 1000), max_records=max_items
        )
        return [msg.value for records in polled.values() for msg in records]

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
        if self._consumer is not None:
            await self._consumer.stop()


def make_buffer(settings: Settings):
    if settings.buffer_backend == "memory":
        return MemoryBuffer(settings.memory_queue_max)
    return KafkaBuffer(settings.kafka_bootstrap, settings.kafka_topic)
