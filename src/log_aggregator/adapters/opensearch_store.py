from __future__ import annotations

import asyncio
import gzip
import io
import json
import time
from datetime import datetime, timedelta, timezone

from log_aggregator.adapters.archive import ArchiveConfig, S3Archive
from log_aggregator.domain.errors import PartialIndexError
from log_aggregator.domain.ids import doc_id, parse_ts
from log_aggregator.ports.store import DEFAULT_TENANT

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

_ALERTS_TEMPLATE = {
    "index_patterns": ["alerts"],
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "tenant": {"type": "keyword"},
                "rule": {"type": "keyword"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "count": {"type": "integer"},
                "message": {"type": "text"},
            }
        },
    },
}


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


class OpenSearchStore:
    """OpenSearch-backed store: one index per tenant-day (logs-<tenant>-YYYY.MM.DD), typed
    index templates applied lazily once, idempotent bulk indexing, and archive-then-delete
    retention to object storage."""

    def __init__(self, url: str, retention_days: int, archive: ArchiveConfig | None = None) -> None:
        self._url = url
        self._retention_days = retention_days
        self._archive = archive
        self._client = None
        self._s3_archive: S3Archive | None = None
        self._template_done = False
        self._stats_cache: dict[str, tuple[dict, float]] = {}  # per-tenant

    async def _get_client(self):
        if self._client is None:
            from opensearchpy import AsyncOpenSearch

            self._client = AsyncOpenSearch(hosts=[self._url], verify_certs=False)
        if not self._template_done:
            await self._client.indices.put_index_template(name="logs", body=_TEMPLATE)
            await self._client.indices.put_index_template(name="alerts", body=_ALERTS_TEMPLATE)
            self._template_done = True
        return self._client

    def _archive_store(self) -> S3Archive:
        if self._s3_archive is None:
            self._s3_archive = S3Archive(self._archive)
        return self._s3_archive

    @staticmethod
    def _index_for(event: dict) -> str:
        tenant = event.get("tenant", DEFAULT_TENANT)
        return f"logs-{tenant}-" + parse_ts(event["timestamp"]).strftime("%Y.%m.%d")

    async def index(self, events: list[dict]) -> int:
        from opensearchpy.helpers import async_streaming_bulk

        client = await self._get_client()
        actions = [{"_index": self._index_for(e), "_id": doc_id(e), "_source": e} for e in events]
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

    async def search(self, tenant: str = DEFAULT_TENANT, q: str = "", level: str = "", service: str = "", limit: int = 100) -> list[dict]:
        client = await self._get_client()
        body = _build_search_body(q, level, service, limit)
        res = await client.search(index=f"logs-{tenant}-*", body=body, ignore_unavailable=True)
        return [h["_source"] for h in res["hits"]["hits"]]

    async def count(self, tenant: str = DEFAULT_TENANT) -> int:
        client = await self._get_client()
        res = await client.count(index=f"logs-{tenant}-*", ignore_unavailable=True)
        return res.get("count", 0)

    async def stats(self, tenant: str = DEFAULT_TENANT) -> dict:
        now = time.monotonic()
        cached = self._stats_cache.get(tenant)
        if cached is not None and now - cached[1] < _STATS_TTL_S:
            return cached[0]
        client = await self._get_client()
        body = {
            "size": 0,
            "track_total_hits": True,  # else hits.total.value caps at 10000
            "aggs": {
                "by_level": {"terms": {"field": "level"}},
                "by_service": {"terms": {"field": "service"}},
            },
        }
        res = await client.search(index=f"logs-{tenant}-*", body=body, ignore_unavailable=True)
        aggs = res.get("aggregations", {})
        result = {
            "total": res["hits"]["total"]["value"],
            "by_level": {b["key"]: b["doc_count"] for b in aggs.get("by_level", {}).get("buckets", [])},
            "by_service": {b["key"]: b["doc_count"] for b in aggs.get("by_service", {}).get("buckets", [])},
        }
        self._stats_cache[tenant] = (result, now)
        return result

    async def apply_retention(self) -> int:
        client = await self._get_client()
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
        indices = await client.indices.get(index="logs-*", ignore_unavailable=True)
        removed = 0
        for name in list(indices):
            try:  # date is the last dash-segment: logs-<tenant>-YYYY.MM.DD (and legacy logs-YYYY.MM.DD)
                day = datetime.strptime(name.rsplit("-", 1)[-1], "%Y.%m.%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if day < cutoff:
                if self._archive is not None and self._archive.enabled:
                    await self._archive_index(name)  # export to object storage first
                await client.indices.delete(index=name)
                removed += 1
        return removed

    async def record_alert(self, alert: dict) -> None:
        client = await self._get_client()
        await client.index(index="alerts", body=alert, refresh=True)

    async def recent_alerts(self, tenant: str = DEFAULT_TENANT, limit: int = 20) -> list[dict]:
        client = await self._get_client()
        body = {"query": {"term": {"tenant": tenant}}, "sort": [{"timestamp": "desc"}], "size": limit}
        res = await client.search(index="alerts", body=body, ignore_unavailable=True)
        return [h["_source"] for h in res["hits"]["hits"]]

    async def _archive_index(self, name: str) -> None:
        """Export every document of `name` as gzipped JSONL to the object store."""
        from opensearchpy.helpers import async_scan

        client = await self._get_client()
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            async for doc in async_scan(client, index=name, query={"query": {"match_all": {}}}):
                gz.write((json.dumps(doc["_source"]) + "\n").encode())
        await asyncio.to_thread(self._archive_store().put, f"{name}.jsonl.gz", buf.getvalue())

    async def restore(self, name: str) -> int:
        """Restore an archived index back into `logs-*` from object storage. Re-indexing is
        idempotent (content-derived `_id`), so restoring twice is safe."""
        data = await asyncio.to_thread(self._archive_store().fetch, f"{name}.jsonl.gz")
        events = [json.loads(line) for line in gzip.decompress(data).decode().splitlines() if line.strip()]
        return await self.index(events)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
