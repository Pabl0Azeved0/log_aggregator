from __future__ import annotations

import asyncio

from log_aggregator.domain.errors import BufferFull


class MemoryBuffer:
    """In-process asyncio.Queue buffer — offline tests and the `make smoke` path only.
    Bounded: publishing past capacity raises BufferFull (the ingest API turns that
    into a 429), mirroring how the real pipeline signals backpressure."""

    def __init__(self, maxsize: int) -> None:
        self._q: asyncio.Queue[dict] = asyncio.Queue(maxsize=maxsize)

    async def publish(self, events: list[dict]) -> None:
        for e in events:
            try:
                self._q.put_nowait(e)
            except asyncio.QueueFull as exc:
                raise BufferFull("memory buffer at capacity") from exc

    async def get_batch(self, max_items: int, timeout_s: float) -> list[dict]:
        batch: list[dict] = []
        try:
            batch.append(await asyncio.wait_for(self._q.get(), timeout=timeout_s))
        except asyncio.TimeoutError:
            return batch
        while len(batch) < max_items:
            try:
                batch.append(self._q.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def commit(self) -> None:
        return None

    async def close(self) -> None:
        return None
