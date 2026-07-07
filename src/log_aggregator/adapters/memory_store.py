from __future__ import annotations

from datetime import datetime, timedelta, timezone

from log_aggregator.domain.ids import doc_id, parse_ts
from log_aggregator.ports.store import DEFAULT_TENANT


class MemoryStore:
    """In-memory store — offline tests only. Mirrors the Store surface exactly (idempotency,
    tenant filtering) so the offline pipeline test exercises the same worker code as prod."""

    def __init__(self, retention_days: int = 7) -> None:
        self._events: list[dict] = []
        self._ids: set[str] = set()
        self._alerts: list[dict] = []
        self._retention_days = retention_days

    async def index(self, events: list[dict]) -> int:
        added = 0
        for e in events:
            eid = doc_id(e)
            if eid in self._ids:  # redelivered event — already stored, mirror OpenSearch upsert
                continue
            self._ids.add(eid)
            self._events.append(e)
            added += 1
        return added

    async def search(self, tenant: str = DEFAULT_TENANT, q: str = "", level: str = "", service: str = "", limit: int = 100) -> list[dict]:
        out = []
        for e in reversed(self._events):
            if e.get("tenant", DEFAULT_TENANT) != tenant:
                continue
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

    async def count(self, tenant: str = DEFAULT_TENANT) -> int:
        return sum(1 for e in self._events if e.get("tenant", DEFAULT_TENANT) == tenant)

    async def stats(self, tenant: str = DEFAULT_TENANT) -> dict:
        by_level: dict[str, int] = {}
        by_service: dict[str, int] = {}
        total = 0
        for e in self._events:
            if e.get("tenant", DEFAULT_TENANT) != tenant:
                continue
            total += 1
            by_level[e.get("level", "INFO")] = by_level.get(e.get("level", "INFO"), 0) + 1
            by_service[e.get("service", "unknown")] = by_service.get(e.get("service", "unknown"), 0) + 1
        return {"total": total, "by_level": by_level, "by_service": by_service}

    async def record_alert(self, alert: dict) -> None:
        self._alerts.append(alert)

    async def recent_alerts(self, tenant: str = DEFAULT_TENANT, limit: int = 20) -> list[dict]:
        return [a for a in reversed(self._alerts) if a.get("tenant", DEFAULT_TENANT) == tenant][:limit]

    async def apply_retention(self) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
        before = len(self._events)
        self._events = [e for e in self._events if parse_ts(e["timestamp"]) >= cutoff]
        self._ids = {doc_id(e) for e in self._events}
        return before - len(self._events)

    async def close(self) -> None:
        return None
