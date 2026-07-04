# log-aggregator

A high-throughput log aggregation microservice: collect logs from many sources, buffer
them through **Kafka**, index them into **OpenSearch**, and search them through a query
API + live dashboard.

```
producers ──► ingest API (FastAPI, async, backpressure)
                 │
                 ▼
               Kafka (single-node KRaft)
                 │
                 ▼
             indexer worker (batching · retries · dead-letter)
                 │
                 ▼
            OpenSearch (daily indices · retention)
                 │
                 ▼
        query API ──► dashboard (search · live tail)
```

## Quickstart

```bash
docker compose up -d --build     # kafka + opensearch + ingest + indexer + query
make install                     # local venv (loadgen + tests)
make loadgen                     # drive load through the pipeline, print measurements
```

Dashboard: http://localhost:8080 · Ingest: `POST http://localhost:8000/logs`

```bash
curl -X POST localhost:8000/logs -H 'content-type: application/json' \
  -d '{"service":"checkout","level":"ERROR","message":"payment failed","attrs":{"order":42}}'
```

## Measured performance

**No numbers are claimed until measured.** Every figure below comes from the committed
load generator (`scripts/loadgen.py`) and is reproducible with `make loadgen` on the
documented machine.

| metric | value |
|---|---|
| sustained ingest rate | _run `make loadgen`_ |
| p99 ingest request latency | _run `make loadgen`_ |
| ingest → searchable lag | _run `make loadgen`_ |

Target (decisions.md): ≥ 5k events/s single-node, p99 ingest→searchable < 2s.

## Design notes

- **Backpressure is explicit:** when the buffer can't keep up the ingest API returns
  `429` instead of buffering unboundedly.
- **At-least-once delivery:** Kafka consumer auto-commit; duplicates are possible on
  indexer restart — dedup is a documented v2 item, not silently claimed.
- **Failures are kept:** batches that exhaust indexing retries land in a JSONL
  dead-letter file for inspection, not dropped.
- **Retention:** one index per day, deleted after `RETENTION_DAYS`.
- Offline tests exercise the exact pipeline with in-memory buffer/store fakes — no
  containers needed for `make test`.

## Limitations (v1, honest)

Single-node Kafka and OpenSearch (throughput ceiling is the machine, not the design);
no auth on the APIs; no exactly-once semantics; retention is delete-only.

## Roadmap (v2+)

Alerting rules · shipper agents/sidecars · multi-tenant auth · Kubernetes manifests ·
exactly-once via manual offset commit tied to bulk-index success.
