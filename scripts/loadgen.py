"""Load generator — the ONLY legitimate source of the README's performance numbers.

Drives the ingest API at a target rate, measures achieved throughput and request
latencies, then (optionally) polls the query API until every sent event is searchable
to measure ingest→searchable lag.

    make loadgen
    python scripts/loadgen.py --rate 5000 --duration 30 --query-url http://localhost:8080
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time

import httpx

_SERVICES = ["checkout", "auth", "payments", "search", "notifications"]
_LEVELS = ["INFO"] * 8 + ["WARNING", "ERROR"]
_WORDS = "request completed user login failed timeout retry cache miss db query slow ok".split()


def _event() -> dict:
    return {
        "service": random.choice(_SERVICES),
        "level": random.choice(_LEVELS),
        "message": " ".join(random.choices(_WORDS, k=random.randint(4, 9))),
        "attrs": {"req_id": random.randrange(10**9)},
    }


def _pctl(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    return sorted(values)[min(int(len(values) * p), len(values) - 1)]


async def run(url: str, rate: int, duration: int, batch: int, query_url: str | None) -> None:
    sent = 0
    errors = 0
    latencies: list[float] = []
    interval = batch / rate
    async with httpx.AsyncClient(timeout=10) as client:
        baseline = 0
        if query_url:
            try:
                baseline = (await client.get(f"{query_url}/stats")).json().get("total", 0)
            except Exception:
                query_url = None
        start = time.monotonic()
        next_send = start
        while time.monotonic() - start < duration:
            now = time.monotonic()
            if now < next_send:
                await asyncio.sleep(next_send - now)
            next_send += interval
            payload = [_event() for _ in range(batch)]
            t0 = time.monotonic()
            try:
                r = await client.post(f"{url}/logs", json=payload)
                latencies.append(time.monotonic() - t0)
                if r.status_code == 202:
                    sent += batch
                else:
                    errors += 1
            except Exception:
                errors += 1
        elapsed = time.monotonic() - start

        print("--- loadgen results (paste-ready for README) ---")
        print(f"sent            : {sent} events in {elapsed:.1f}s")
        print(f"achieved rate   : {sent / elapsed:.0f} events/s (target {rate}/s)")
        print(f"request latency : p50 {_pctl(latencies, 0.50) * 1000:.0f} ms · p99 {_pctl(latencies, 0.99) * 1000:.0f} ms")
        print(f"request errors  : {errors}")

        if query_url:
            lag_start = time.monotonic()
            deadline = lag_start + 120
            indexed = 0
            while time.monotonic() < deadline:
                stats = (await client.get(f"{query_url}/stats")).json()
                indexed = stats.get("total", 0) - baseline
                if indexed >= sent:
                    break
                await asyncio.sleep(0.25)
            lag = time.monotonic() - lag_start
            print(f"searchable      : {indexed}/{sent} events, drain lag after send {lag:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--rate", type=int, default=5000, help="target events/s")
    ap.add_argument("--duration", type=int, default=30, help="seconds")
    ap.add_argument("--batch", type=int, default=100, help="events per request")
    ap.add_argument("--query-url", default=None, help="query API base URL to measure searchable lag")
    args = ap.parse_args()
    asyncio.run(run(args.url, args.rate, args.duration, args.batch, args.query_url))


if __name__ == "__main__":
    main()
