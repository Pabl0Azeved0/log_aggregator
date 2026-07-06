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

![log-aggreagator demo](docs/dashboard-demo.gif)

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
load generator (`scripts/loadgen.py`) and is reproducible with `make loadgen`.

Measured on a laptop — Intel i7-1165G7 (4C/8T), 7.6 GiB RAM, WSL2 — with the whole
stack (Kafka, OpenSearch, ingest, indexer, query) on the one host and OpenSearch
capped at a 512 MB heap. Target was ≥ 5k events/s single-node, ingest→searchable < 2s.

| target rate | achieved | p50 | p99 | errors | ingest→searchable (drain) |
|---|---|---|---|---|---|
| 5 000/s  | 5 002/s  | 4 ms  | 7 ms  | 0 | **0.8 s** |
| 15 000/s | 15 005/s | 6 ms  | 22 ms | 0 | **0.8 s** |
| 30 000/s | 29 494/s | 8 ms  | 26 ms | 0 | 15.4 s |
| 50 000/s | 32 115/s | 13 ms | 32 ms | 0 | 18.1 s |

- **Target met with margin:** at 5k events/s, everything is searchable **0.8 s** after
  the last event is sent — well under the 2 s goal — with a **7 ms p99** ingest latency
  and zero errors.
- **Real-time end-to-end holds to ~15k events/s** (still 0.8 s drain). Beyond that the
  Kafka buffer absorbs the burst and the indexer drains it in ~15–18 s: nothing is
  dropped and no backpressure `429`s are hit at these rates — buffering working as
  designed, with the indexer as the steady-state ceiling.
- **The ingest API sustains 30k+ events/s at p99 ≤ 26 ms.** The ~32k plateau at the top
  is the load generator itself (single sequential client), **not** a proven server
  ceiling; a concurrent generator is on the roadmap to push past it.

## Design notes

- **Backpressure is explicit:** when the buffer can't keep up the ingest API returns
  `429` instead of buffering unboundedly.
- **Effectively-once delivery:** the consumer commits offsets only after a batch is
  indexed, and every document is written under a content-derived `_id`, so a redelivered
  event overwrites rather than duplicates. Verified by rewinding the consumer-group
  offset to force redelivery — the document count stays flat.
- **Failures are kept:** batches that exhaust indexing retries land in a JSONL
  dead-letter file for inspection, not dropped.
- **Retention:** one index per day, deleted after `RETENTION_DAYS`.
- Offline tests exercise the exact pipeline with in-memory buffer/store fakes — no
  containers needed for `make test`.

## Limitations (v1, honest)

Single-node Kafka and OpenSearch (throughput ceiling is the machine, not the design);
no auth on the APIs; retention is delete-only.

## Roadmap (v2+)

Alerting rules · shipper agents/sidecars · multi-tenant auth · Kubernetes manifests ·
long-term retention to object storage.
