"""Unit tests for OpenSearchStore query construction and caching — no live cluster.
A fake client is injected (with the template flag pre-set) to bypass _get_client."""

from __future__ import annotations

import asyncio
import gzip

from log_aggregator.store import ArchiveConfig, OpenSearchStore, _build_search_body


class _FakeIndices:
    def __init__(self, names, log):
        self._names = names
        self.log = log

    async def get(self, index=None, ignore_unavailable=True):
        return {n: {} for n in self._names}

    async def delete(self, index=None):
        self.log.append(("delete", index))


class _FakeIdxClient:
    def __init__(self, names, log):
        self.indices = _FakeIndices(names, log)


def _archive_cfg(enabled):
    return ArchiveConfig(enabled=enabled, bucket="b", endpoint="http://e", access_key="k", secret_key="s")


class _FakeClient:
    def __init__(self):
        self.search_calls = 0
        self.last_body = None

    async def search(self, index=None, body=None, ignore_unavailable=True):
        self.search_calls += 1
        self.last_body = body
        return {
            "hits": {"total": {"value": 5}, "hits": []},
            "aggregations": {"by_level": {"buckets": []}, "by_service": {"buckets": []}},
        }


def _store_with(fake):
    s = OpenSearchStore("http://opensearch:9200", 7)
    s._client = fake
    s._template_done = True
    return s


def test_search_body_uses_phrase_prefix_and_terms():
    body = _build_search_body("payment failed", "ERROR", "checkout", 50)
    must = body["query"]["bool"]["must"]
    assert {"match_phrase_prefix": {"message": "payment failed"}} in must
    assert {"term": {"level": "ERROR"}} in must
    assert {"term": {"service": "checkout"}} in must
    assert body["size"] == 50
    assert body["sort"] == [{"timestamp": "desc"}]


def test_search_body_empty_query_is_match_all():
    assert _build_search_body("", "", "", 100)["query"] == {"match_all": {}}


def _retention_store(enabled, log):
    class RecordingStore(OpenSearchStore):
        async def _archive_index(self, name):
            log.append(("archive", name))

    # one expired index (2000) + one far-future index (2999, kept)
    store = RecordingStore("http://opensearch:9200", 7, _archive_cfg(enabled))
    store._template_done = True
    store._client = _FakeIdxClient(["logs-2000.01.01", "logs-2999.01.01"], log)
    return store


def test_retention_archives_then_deletes_when_enabled():
    log: list = []
    store = _retention_store(True, log)

    async def flow():
        removed = await store.apply_retention()
        assert removed == 1
        assert log == [("archive", "logs-2000.01.01"), ("delete", "logs-2000.01.01")]

    asyncio.run(flow())


def test_retention_delete_only_when_disabled():
    log: list = []
    store = _retention_store(False, log)

    async def flow():
        removed = await store.apply_retention()
        assert removed == 1
        assert log == [("delete", "logs-2000.01.01")]  # no archive step

    asyncio.run(flow())


def test_restore_reindexes_from_archive():
    line = b'{"timestamp":"2020-01-01T00:00:00Z","service":"a","level":"INFO","message":"m","attrs":{}}\n'
    reindexed: list = []

    class RestoreStore(OpenSearchStore):
        def _fetch(self, key):
            assert key == "logs-2020.01.01.jsonl.gz"
            return gzip.compress(line)

        async def index(self, events):
            reindexed.extend(events)
            return len(events)

    store = RestoreStore("http://opensearch:9200", 7, _archive_cfg(True))

    async def flow():
        n = await store.restore("logs-2020.01.01")
        assert n == 1 and reindexed[0]["message"] == "m"

    asyncio.run(flow())


def test_stats_is_cached_within_ttl():
    fake = _FakeClient()
    store = _store_with(fake)

    async def flow():
        a = await store.stats()
        b = await store.stats()
        assert a["total"] == 5 and b["total"] == 5
        assert fake.search_calls == 1  # second call served from cache

    asyncio.run(flow())
