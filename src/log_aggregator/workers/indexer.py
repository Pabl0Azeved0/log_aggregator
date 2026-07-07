"""Indexer worker: drain the buffer in batches, bulk-index into the store with retries,
and dead-letter what cannot be indexed. Retention is applied on startup and then once
per hour.

Run: python -m log_aggregator.workers.indexer
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from pathlib import Path

from log_aggregator.composition import make_buffer, make_store
from log_aggregator.config import Settings, get_settings
from log_aggregator.domain.errors import PartialIndexError
from log_aggregator.ports.buffer import Buffer
from log_aggregator.ports.store import Store

log = logging.getLogger("indexer")

_RETRIES = 3
_RETENTION_INTERVAL_S = 3600


def _dead_letter(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")
    log.error("dead-lettered %d events to %s", len(events), path)


async def _index_with_retry(store: Store, batch: list[dict], dead_letter: Path) -> int:
    for attempt in range(1, _RETRIES + 1):
        try:
            return await store.index(batch)
        except PartialIndexError as exc:
            # per-document rejections won't succeed on retry — dead-letter only those,
            # keep the ones that indexed.
            _dead_letter(dead_letter, exc.failed)
            return exc.indexed
        except Exception as exc:  # noqa: BLE001
            log.warning("index attempt %d/%d failed: %s", attempt, _RETRIES, exc)
            if attempt < _RETRIES:
                await asyncio.sleep(2 ** (attempt - 1))
    _dead_letter(dead_letter, batch)
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
            # commit only after the batch is handled (indexed and/or dead-lettered); a crash
            # before this re-delivers the batch, and idempotent _id keeps re-indexing safe.
            await buffer.commit()
        elif once:
            return indexed_total


async def _serve(buffer: Buffer, store: Store, settings: Settings) -> None:
    """Run the loop until cancelled by SIGTERM/SIGINT, then close the buffer and store
    so in-flight offsets/clients shut down cleanly."""
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(run(buffer, store, settings))
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        log.info("indexer shutting down")
    finally:
        await buffer.close()
        await store.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = get_settings()
    buffer = make_buffer(settings)
    store = make_store(settings)
    log.info("indexer starting: buffer=%s store=%s", settings.buffer_backend, settings.store_backend)
    asyncio.run(_serve(buffer, store, settings))


if __name__ == "__main__":
    main()
