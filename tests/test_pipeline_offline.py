"""Offline end-to-end: MemoryBuffer + MemoryStore through the REAL indexer loop and the
REAL APIs (via TestClient) — no Kafka, no OpenSearch, no network."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from log_aggregator.workers import indexer
from log_aggregator.adapters.memory_buffer import MemoryBuffer
from log_aggregator.domain.errors import BufferFull
from log_aggregator.config import Settings
from log_aggregator.api.ingest import create_app as create_ingest_app
from log_aggregator.domain.models import LogEvent
from log_aggregator.api.query import create_app as create_query_app
from log_aggregator.adapters.memory_store import MemoryStore


def _settings() -> Settings:
    s = Settings()
    s.batch_size = 100
    s.batch_timeout_s = 0.05
    return s


def test_ingest_index_search_roundtrip(tmp_path):
    async def flow():
        buf = MemoryBuffer(maxsize=1000)
        store = MemoryStore()
        events = [
            LogEvent(service="api", level="ERROR", message="boom in checkout").model_dump(mode="json"),
            LogEvent(service="api", level="INFO", message="ok").model_dump(mode="json"),
            LogEvent(service="worker", level="ERROR", message="boom in worker").model_dump(mode="json"),
        ]
        await buf.publish(events)
        settings = _settings()
        settings.dead_letter_path = str(tmp_path / "dl.jsonl")
        indexed = await indexer.run(buf, store, settings, once=True)
        assert indexed == 3
        hits = await store.search(q="boom", level="ERROR")
        assert len(hits) == 2
        stats = await store.stats()
        assert stats["by_level"]["ERROR"] == 2
        assert stats["by_service"]["api"] == 2
    asyncio.run(flow())


def test_ingest_returns_429_on_backpressure():
    buf = MemoryBuffer(maxsize=1)  # a 2-event batch overflows it
    ingest = TestClient(create_ingest_app(buffer=buf))
    r = ingest.post("/logs", json=[{"message": "a"}, {"message": "b"}])
    assert r.status_code == 429


def test_backpressure_raises_buffer_full():
    async def flow():
        buf = MemoryBuffer(maxsize=2)
        with pytest.raises(BufferFull):
            await buf.publish([{"m": i} for i in range(3)])
    asyncio.run(flow())


def test_retention_drops_old_events():
    async def flow():
        store = MemoryStore(retention_days=7)
        old = LogEvent(message="ancient").model_dump(mode="json")
        old["timestamp"] = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        fresh = LogEvent(message="fresh").model_dump(mode="json")
        await store.index([old, fresh])
        removed = await store.apply_retention()
        assert removed == 1
        assert await store.count() == 1
    asyncio.run(flow())


def test_dead_letter_on_persistent_index_failure(tmp_path, monkeypatch):
    class BrokenStore(MemoryStore):
        async def index(self, events):
            raise RuntimeError("store down")

    async def flow():
        monkeypatch.setattr(indexer.asyncio, "sleep", _instant_sleep)
        buf = MemoryBuffer(maxsize=10)
        await buf.publish([LogEvent(message="doomed").model_dump(mode="json")])
        settings = _settings()
        settings.dead_letter_path = str(tmp_path / "dl.jsonl")
        indexed = await indexer.run(buf, BrokenStore(), settings, once=True)
        assert indexed == 0
        assert (tmp_path / "dl.jsonl").read_text().count("doomed") == 1
    asyncio.run(flow())


def test_redelivered_event_is_indexed_once():
    async def flow():
        store = MemoryStore()
        e = LogEvent(service="api", level="ERROR", message="boom").model_dump(mode="json")
        assert await store.index([e]) == 1
        assert await store.index([e]) == 0  # redelivery: same _id, no new doc
        assert await store.count() == 1
    asyncio.run(flow())


def test_indexer_commits_after_batch(tmp_path):
    class CountingBuffer(MemoryBuffer):
        def __init__(self, maxsize):
            super().__init__(maxsize)
            self.commits = 0

        async def commit(self):
            self.commits += 1

    async def flow():
        buf = CountingBuffer(maxsize=10)
        await buf.publish([LogEvent(message="x").model_dump(mode="json")])
        settings = _settings()
        settings.dead_letter_path = str(tmp_path / "dl.jsonl")
        await indexer.run(buf, MemoryStore(), settings, once=True)
        assert buf.commits == 1  # one batch handled → one commit
    asyncio.run(flow())


def test_partial_index_failure_dead_letters_only_rejected(tmp_path):
    from log_aggregator.domain.errors import PartialIndexError

    class PartialStore(MemoryStore):
        async def index(self, events):
            await super().index(events[1:])  # all but the first succeed
            raise PartialIndexError(len(events) - 1, [events[0]])

    async def flow():
        buf = MemoryBuffer(maxsize=10)
        await buf.publish([LogEvent(message=f"msg{i}").model_dump(mode="json") for i in range(3)])
        settings = _settings()
        settings.dead_letter_path = str(tmp_path / "dl.jsonl")
        indexed = await indexer.run(buf, PartialStore(), settings, once=True)
        assert indexed == 2
        dl = (tmp_path / "dl.jsonl").read_text()
        assert dl.count("msg0") == 1 and dl.count("msg1") == 0 and dl.count("msg2") == 0
    asyncio.run(flow())


async def _instant_sleep(_secs):
    return None


def test_apis_end_to_end_with_memory_backends():
    buf = MemoryBuffer(maxsize=100)
    store = MemoryStore()
    ingest = TestClient(create_ingest_app(buffer=buf))
    query = TestClient(create_query_app(store=store))

    r = ingest.post("/logs", json={"service": "web", "level": "error", "message": "500 on /pay"})
    assert r.status_code == 202 and r.json() == {"accepted": 1}
    r = ingest.post("/logs", json=[{"message": "a"}, {"message": "b"}])
    assert r.status_code == 202 and r.json() == {"accepted": 2}

    r = ingest.post("/logs/raw?service=legacy", content="WARN disk almost full\nplain info line")
    assert r.status_code == 202 and r.json() == {"accepted": 2}

    async def drain():
        settings = _settings()
        await indexer.run(buf, store, settings, once=True)
    asyncio.run(drain())

    hits = query.get("/search", params={"level": "ERROR"}).json()
    assert len(hits) == 1 and hits[0]["message"] == "500 on /pay"
    hits = query.get("/search", params={"service": "legacy", "level": "WARNING"}).json()
    assert len(hits) == 1
    stats = query.get("/stats").json()
    assert stats["total"] == 5
    assert query.get("/healthz").json() == {"status": "ok"}
