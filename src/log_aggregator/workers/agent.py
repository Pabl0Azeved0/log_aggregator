"""Log shipper agent (sidecar): tail a file and ship its lines to the ingest API's
`/logs/raw` endpoint. Backpressure is honored — a 429 is retried with exponential backoff,
never dropped — and file truncation/rotation is handled by reopening.

Run: python -m log_aggregator.workers.agent   (configured via env, see main())
"""

from __future__ import annotations

import logging
import os
import time

import httpx

log = logging.getLogger("agent")


def ship(client: httpx.Client, url: str, service: str, lines: list[str], headers: dict, sleep=time.sleep) -> None:
    """POST a batch of raw lines, retrying until accepted. 429 (backpressure) and transport
    errors back off exponentially and retry the same batch — nothing is dropped."""
    body = "\n".join(lines)
    delay = 0.5
    while True:
        try:
            r = client.post(f"{url}/logs/raw", params={"service": service}, content=body, headers=headers)
            if r.status_code == 202:
                return
            log.warning("ingest returned %s%s — retrying in %.1fs", r.status_code,
                        " (backpressure)" if r.status_code == 429 else "", delay)
        except Exception as exc:  # noqa: BLE001 — a transient ingest outage must not drop logs
            log.warning("post failed (%s) — retrying in %.1fs", exc, delay)
        sleep(delay)
        delay = min(delay * 2, 10.0)


def _open_at(path: str, from_start: bool):
    while not os.path.exists(path):
        time.sleep(0.5)
    f = open(path)
    if not from_start:
        f.seek(0, os.SEEK_END)
    return f


def run(url: str, path: str, service: str, headers: dict, batch: int, flush_s: float, from_start: bool) -> None:
    f = _open_at(path, from_start)
    buf: list[str] = []
    with httpx.Client(timeout=10) as client:
        def flush():
            if buf:
                ship(client, url, service, buf, headers)
                buf.clear()

        last = time.monotonic()
        while True:
            line = f.readline()
            if line.endswith("\n"):
                buf.append(line.rstrip("\n"))
                if len(buf) >= batch:
                    flush(); last = time.monotonic()
            else:
                if line:  # partial trailing line — rewind and wait for the rest
                    f.seek(f.tell() - len(line))
                if buf and time.monotonic() - last >= flush_s:
                    flush(); last = time.monotonic()
                if os.path.getsize(path) < f.tell():  # truncated/rotated — reopen from start
                    f.close(); f = _open_at(path, from_start=True)
                time.sleep(0.3)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    url = os.getenv("INGEST_URL", "http://localhost:8000")
    path = os.getenv("AGENT_FILE", "/var/log/app.log")
    service = os.getenv("AGENT_SERVICE", "shipper")
    batch = int(os.getenv("AGENT_BATCH", "50"))
    flush_s = float(os.getenv("AGENT_FLUSH_S", "1.0"))
    from_start = os.getenv("AGENT_FROM_START", "true").lower() == "true"
    api_key = os.getenv("AGENT_API_KEY", "")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    log.info("agent shipping %s -> %s (service=%s)", path, url, service)
    run(url, path, service, headers, batch, flush_s, from_start)


if __name__ == "__main__":
    main()
