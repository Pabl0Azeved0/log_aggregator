"""Indexer worker: drain the buffer in batches, bulk-index into the store with retries,
and dead-letter what cannot be indexed. Retention is applied on startup and then once
per hour.

Run: python -m log_aggregator.indexer
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

from log_aggregator.buffer import Buffer, make_buffer
from log_aggregator.config import Settings, get_settings
from log_aggregator.store import Store, make_store

log = logging.getLogger("indexer")

_RETRIES = 3
_RETENTION_INTERVAL_S = 3600


async def _index_with_retry(store: Store, batch: list[dict], dead_letter: Path) -> int:
    for attempt in range(1, _RETRIES + 1):
        try:
            return await store.index(batch)
        except Exception as exc:  # noqa: BLE001
            log.warning("index attempt %d/%d failed: %s", attempt, _RETRIES, exc)
            if attempt < _RETRIES:
                await asyncio.sleep(2 ** (attempt - 1))
    dead_letter.parent.mkdir(parents=True, exist_ok=True)
    with dead_letter.open("a") as fh:
        for event in batch:
            fh.write(json.dumps(event) + "\n")
    log.error("dead-lettered %d events to %s", len(batch), dead_letter)
    return 0


async def run(buffer: Buffer, store: Store, settings: Settings, once: bool = False) -> int:
    """Consume → index loop. `once=True` drains a single wait cycle (tests/smoke)."""
    dead_letter = Path(settings.dead_letter_path)
    indexed_total = 0
    last_retention = 0.0
    while True:
        now = time.monotonic()
        if now - last_retention >= _RETENTION_INTERVAL_S:
            removed = await store.apply_retention()
            if removed:
                log.info("retention removed %s old indices/events", removed)
            last_retention = now
        batch = await buffer.get_batch(settings.batch_size, settings.batch_timeout_s)
        if batch:
            indexed_total += await _index_with_retry(store, batch, dead_letter)
        elif once:
            return indexed_total


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = get_settings()
    buffer = make_buffer(settings)
    store = make_store(settings)
    log.info("indexer starting: buffer=%s store=%s", settings.buffer_backend, settings.store_backend)
    asyncio.run(run(buffer, store, settings))


if __name__ == "__main__":
    main()
