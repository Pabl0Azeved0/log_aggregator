"""Composition root: the single place that wires abstractions (ports) to concrete adapters
from `Settings`. Workers and the API depend on these factories, not on concrete adapters."""

from __future__ import annotations

from log_aggregator.adapters.archive import ArchiveConfig
from log_aggregator.adapters.memory_store import MemoryStore
from log_aggregator.adapters.opensearch_store import OpenSearchStore
from log_aggregator.config import Settings
from log_aggregator.ports.store import Store


def make_store(settings: Settings) -> Store:
    if settings.store_backend == "memory":
        return MemoryStore(settings.retention_days)
    archive = ArchiveConfig(
        enabled=settings.archive_enabled,
        bucket=settings.archive_bucket,
        endpoint=settings.s3_endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    return OpenSearchStore(settings.opensearch_url, settings.retention_days, archive)
