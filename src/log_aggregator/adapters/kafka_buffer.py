from __future__ import annotations

import asyncio
import json


class KafkaBuffer:
    """Kafka-backed buffer (the real path). Producer/consumer are created lazily so importing
    this module never requires a running broker. The consumer uses a configurable group and
    manual offset commit (`commit()` after a batch is indexed) — effectively-once with the
    store's idempotent ids."""

    def __init__(self, bootstrap: str, topic: str, group: str = "indexer") -> None:
        self._bootstrap = bootstrap
        self._topic = topic
        self._group = group
        self._producer = None
        self._consumer = None
        self._producer_lock = asyncio.Lock()

    async def _get_producer(self):
        # double-checked lock: concurrent first publishes must not each build a producer,
        # nor send on one that another coroutine is still awaiting start() on. _producer is
        # published only after start() completes, so a non-None value is always ready.
        if self._producer is None:
            async with self._producer_lock:
                if self._producer is None:
                    from aiokafka import AIOKafkaProducer

                    producer = AIOKafkaProducer(
                        bootstrap_servers=self._bootstrap,
                        value_serializer=lambda v: json.dumps(v).encode(),
                        linger_ms=5,
                    )
                    await producer.start()
                    self._producer = producer
        return self._producer

    async def publish(self, events: list[dict]) -> None:
        producer = await self._get_producer()
        for e in events:
            await producer.send(self._topic, e)

    async def get_batch(self, max_items: int, timeout_s: float) -> list[dict]:
        if self._consumer is None:
            from aiokafka import AIOKafkaConsumer

            self._consumer = AIOKafkaConsumer(
                self._topic,
                bootstrap_servers=self._bootstrap,
                group_id=self._group,
                value_deserializer=lambda b: json.loads(b.decode()),
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # offsets committed only after a batch is indexed
            )
            await self._consumer.start()
        polled = await self._consumer.getmany(
            timeout_ms=int(timeout_s * 1000), max_records=max_items
        )
        return [msg.value for records in polled.values() for msg in records]

    async def commit(self) -> None:
        if self._consumer is not None:
            await self._consumer.commit()

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
        if self._consumer is not None:
            await self._consumer.stop()
