"""Unit tests for OpenSearchStore query construction and caching — no live cluster.
A fake client is injected (with the template flag pre-set) to bypass _get_client."""

from __future__ import annotations

import asyncio

from log_aggregator.store import OpenSearchStore


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


def test_stats_is_cached_within_ttl():
    fake = _FakeClient()
    store = _store_with(fake)

    async def flow():
        a = await store.stats()
        b = await store.stats()
        assert a["total"] == 5 and b["total"] == 5
        assert fake.search_calls == 1  # second call served from cache

    asyncio.run(flow())
