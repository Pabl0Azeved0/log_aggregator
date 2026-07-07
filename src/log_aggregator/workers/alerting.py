"""Alerting worker: consume the logs stream on its own consumer group, evaluate threshold
rules (domain) over the events, and notify + persist when one fires.

Run: python -m log_aggregator.workers.alerting
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time

from log_aggregator.composition import make_buffer, make_notifier, make_store
from log_aggregator.config import get_settings
from log_aggregator.domain.rules import RuleEngine, load_rules
from log_aggregator.ports.buffer import Buffer
from log_aggregator.ports.store import Store

log = logging.getLogger("alerting")


async def run(buffer: Buffer, engine: RuleEngine, notify, store: Store, once: bool = False) -> int:
    fired = 0
    while True:
        batch = await buffer.get_batch(500, 1.0)
        if batch:
            now = time.time()
            for event in batch:
                for alert in engine.observe(event, now):
                    fired += 1
                    await notify(alert)
                    await store.record_alert(alert)
            await buffer.commit()
        elif once:
            return fired


async def _serve(buffer: Buffer, engine: RuleEngine, notify, store: Store) -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(run(buffer, engine, notify, store))
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        log.info("alerting shutting down")
    finally:
        await buffer.close()
        await store.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = get_settings()
    engine = RuleEngine(load_rules(settings.alert_rules))
    buffer = make_buffer(settings)
    store = make_store(settings)
    notify = make_notifier(settings)
    log.info("alerting starting: %d rule(s), group=%s", len(engine.rules), settings.kafka_group)
    asyncio.run(_serve(buffer, engine, notify, store))


if __name__ == "__main__":
    main()
