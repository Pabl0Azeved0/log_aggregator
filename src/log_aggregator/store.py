from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Protocol

from log_aggregator.config import Settings

_STATS_TTL_S = 1.0  # dashboards poll /stats every 1.5s; collapse duplicate aggregations

_TEMPLATE = {
    "index_patterns": ["logs-*"],
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "message": {"type": "text"},
                "attrs": {"type": "object", "enabled": True},
            }
        },
    },
}


class PartialIndexError(Exception):
    """Some documents in a batch were rejected by the store (a per-document error, not a
    transport failure). Carries the count successfully indexed and the rejected events so
    the caller can dead-letter only the failures instead of the whole batch."""

    def __init__(self, indexed: int, failed: list[dict]) -> None:
        super().__init__(f"{len(failed)} of {indexed + len(failed)} documents rejected")
        self.indexed = indexed
        self.failed = failed


class Store(Protocol):
    async def index(self, events: list[dict]) -> int: ...
    async def search(self, q: str = "", level: str = "", service: str = "", limit: int = 100) -> list[dict]: ...
    async def count(self) -> int: ...
    async def stats(self) -> dict: ...
    async def apply_retention(self) -> int: ...
    async def close(self) -> None: ...


def _build_search_body(q: str, level: str, service: str, limit: int) -> dict:
    must: list[dict] = []
    if q:
        # Phrase-prefix, not a loose `match`: the typed text must appear as a contiguous
        # phrase (the final word may be a prefix, so type-ahead still works). A plain
        # `match` OR-tokenises the input, so pasting a log line returned every doc sharing
        # any single common word.
        must.append({"match_phrase_prefix": {"message": q}})
    if level:
        must.append({"term": {"level": level}})
    if service:
        must.append({"term": {"service": service}})
    return {
        "query": {"bool": {"must": must}} if must else {"match_all": {}},
        "sort": [{"timestamp": "desc"}],
        "size": limit,
    }


async def _aenumerate(aiter, start: int = 0):
    i = start
    async for item in aiter:
        yield i, item
        i += 1


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


class MemoryStore:
    """In-memory store — offline tests only. Mirrors the Store surface exactly so the
    offline pipeline test exercises the same indexer code as production."""

    def __init__(self, retention_days: int = 7) -> None:
        self._events: list[dict] = []
        self._retention_days = retention_days

    async def index(self, events: list[dict]) -> int:
        self._events.extend(events)
        return len(events)

    async def search(self, q: str = "", level: str = "", service: str = "", limit: int = 100) -> list[dict]:
        out = []
        for e in reversed(self._events):
            if q and q.lower() not in str(e.get("message", "")).lower():
                continue
            if level and e.get("level") != level:
                continue
            if service and e.get("service") != service:
                continue
            out.append(e)
            if len(out) >= limit:
                break
        return out

    async def count(self) -> int:
        return len(self._events)

    async def stats(self) -> dict:
        by_level: dict[str, int] = {}
        by_service: dict[str, int] = {}
        for e in self._events:
            by_level[e.get("level", "INFO")] = by_level.get(e.get("level", "INFO"), 0) + 1
            by_service[e.get("service", "unknown")] = by_service.get(e.get("service", "unknown"), 0) + 1
        return {"total": len(self._events), "by_level": by_level, "by_service": by_service}

    async def apply_retention(self) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
        before = len(self._events)
        self._events = [e for e in self._events if _parse_ts(e["timestamp"]) >= cutoff]
        return before - len(self._events)

    async def close(self) -> None:
        return None


class OpenSearchStore:
    """OpenSearch-backed store: one index per day (logs-YYYY.MM.DD), an index template
    with typed mappings applied lazily once, bulk indexing, and delete-old-indices
    retention."""

    def __init__(self, url: str, retention_days: int) -> None:
        self._url = url
        self._retention_days = retention_days
        self._client = None
        self._template_done = False
        self._stats_cache: dict | None = None
        self._stats_at = 0.0

    async def _get_client(self):
        if self._client is None:
            from opensearchpy import AsyncOpenSearch

            self._client = AsyncOpenSearch(hosts=[self._url], verify_certs=False)
        if not self._template_done:
            await self._client.indices.put_index_template(name="logs", body=_TEMPLATE)
            self._template_done = True
        return self._client

    @staticmethod
    def _index_for(event: dict) -> str:
        return "logs-" + _parse_ts(event["timestamp"]).strftime("%Y.%m.%d")

    async def index(self, events: list[dict]) -> int:
        from opensearchpy.helpers import async_streaming_bulk

        client = await self._get_client()
        actions = [{"_index": self._index_for(e), "_source": e} for e in events]
        ok = 0
        failed: list[dict] = []
        # streaming_bulk yields one result per action, in order. raise_on_error=False lets
        # per-document rejections surface as (False, info) instead of taking the whole
        # batch down; transport/connection errors still raise (retried by the caller).
        async for i, (succeeded, _info) in _aenumerate(
            async_streaming_bulk(client, actions, raise_on_error=False)
        ):
            if succeeded:
                ok += 1
            else:
                failed.append(events[i])
        if failed:
            raise PartialIndexError(ok, failed)
        return ok

    async def search(self, q: str = "", level: str = "", service: str = "", limit: int = 100) -> list[dict]:
        client = await self._get_client()
        body = _build_search_body(q, level, service, limit)
        res = await client.search(index="logs-*", body=body, ignore_unavailable=True)
        return [h["_source"] for h in res["hits"]["hits"]]

    async def count(self) -> int:
        client = await self._get_client()
        res = await client.count(index="logs-*", ignore_unavailable=True)
        return res.get("count", 0)

    async def stats(self) -> dict:
        now = time.monotonic()
        if self._stats_cache is not None and now - self._stats_at < _STATS_TTL_S:
            return self._stats_cache
        client = await self._get_client()
        body = {
            "size": 0,
            "track_total_hits": True,  # else hits.total.value caps at 10000
            "aggs": {
                "by_level": {"terms": {"field": "level"}},
                "by_service": {"terms": {"field": "service"}},
            },
        }
        res = await client.search(index="logs-*", body=body, ignore_unavailable=True)
        aggs = res.get("aggregations", {})
        result = {
            "total": res["hits"]["total"]["value"],
            "by_level": {b["key"]: b["doc_count"] for b in aggs.get("by_level", {}).get("buckets", [])},
            "by_service": {b["key"]: b["doc_count"] for b in aggs.get("by_service", {}).get("buckets", [])},
        }
        self._stats_cache = result
        self._stats_at = now
        return result

    async def apply_retention(self) -> int:
        client = await self._get_client()
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
        indices = await client.indices.get(index="logs-*", ignore_unavailable=True)
        deleted = 0
        for name in list(indices):
            try:
                day = datetime.strptime(name.removeprefix("logs-"), "%Y.%m.%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if day < cutoff:
                await client.indices.delete(index=name)
                deleted += 1
        return deleted

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()


def make_store(settings: Settings) -> Store:
    if settings.store_backend == "memory":
        return MemoryStore(settings.retention_days)
    return OpenSearchStore(settings.opensearch_url, settings.retention_days)
