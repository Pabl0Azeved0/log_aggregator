# log-aggregator

A high-throughput log aggregation microservice: collect logs from many sources, buffer
them through **Kafka**, index them into **OpenSearch**, and search them through a query
API + live dashboard.

```
apps / file-shipping agent ──► ingest API (FastAPI, async, backpressure)
                                   │
                                   ▼
                                 Kafka (single-node KRaft)
                                   │
                     ┌─────────────┴─────────────┐
                     ▼                           ▼
           indexer worker              alerting worker (rules · notify)
        (batch · retry · dead-letter)
                     │
                     ▼
            OpenSearch (daily indices · tiered retention)
                     │
                     ▼
        query API ──► dashboard (search · live tail · alerts)
```

![log-aggreagator demo](docs/dashboard-demo.gif)

## Quickstart

```bash
docker compose up -d --build             # kafka + opensearch + ingest + indexer + alerting + query
make install                             # local venv (loadgen + tests)
make loadgen                             # drive load through the pipeline, print measurements

docker compose --profile sidecar up -d   # optional: a demo app + the file-shipping agent
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
- **Multi-node (Kubernetes):** the `k8s/` manifests scale ingest (HPA) and indexer (one
  replica per Kafka partition) across a 3-broker Kafka + 3-node OpenSearch cluster. A
  measured throughput row for that topology is **pending a real-cluster run** — not
  claimed here, in keeping with the rule above.

## Design notes

- **Backpressure is explicit:** when the buffer can't keep up the ingest API returns
  `429` instead of buffering unboundedly.
- **Effectively-once delivery:** the consumer commits offsets only after a batch is
  indexed, and every document is written under a content-derived `_id`, so a redelivered
  event overwrites rather than duplicates. Verified by rewinding the consumer-group
  offset to force redelivery — the document count stays flat.
- **Failures are kept:** batches that exhaust indexing retries land in a JSONL
  dead-letter file for inspection, not dropped.
- **Tiered retention:** one index per day; after `RETENTION_DAYS` an expiring index is
  exported to object storage (S3/MinIO) as gzipped JSONL, then deleted — not lost.
  `OpenSearchStore.restore()` re-indexes an archived day back on demand (idempotent).
- **Multi-tenant auth:** set `AUTH_ENABLED=true` and `API_KEYS="key1:tenantA,key2:tenantB"`.
  Producers send `Authorization: Bearer <api-key>`; the dashboard exchanges a key for a
  short-lived JWT via `POST /auth/token`. Each tenant's data lives in its own indices
  (`logs-<tenant>-*`) and reads are scoped to the caller — no cross-tenant access. Off by
  default (single `default` tenant, open APIs) so local dev and `make loadgen` need no keys.
- **Alerting:** a separate `alerting` worker consumes the same stream on its **own consumer
  group** (independent of the indexer) and evaluates threshold rules over sliding windows,
  per `(tenant, rule)`, with a cooldown so one incident isn't a thousand alerts. Fired
  alerts POST to a Slack-compatible `ALERT_WEBHOOK` (console when unset) and surface on the
  dashboard via `GET /alerts`. Rules are JSON in `ALERT_RULES`, e.g.
  `[{"name":"error-burst","level":"ERROR","threshold":500,"window_s":10,"cooldown_s":30}]`.
- **Shipper agent (sidecar):** `log_aggregator.agent` tails a file and ships lines to
  `/logs/raw`, **honoring backpressure** — a `429` is retried with exponential backoff, never
  dropped — and reopening on truncation/rotation. Run the bundled demo with
  `docker compose --profile sidecar up -d` (a fake app + the agent shipping its logs).
- **Kubernetes:** `k8s/` holds Kustomize manifests for a multi-node deployment — 3-broker
  Kafka, 3-node OpenSearch, ingest behind an HPA, one indexer per partition — that closes
  the single-node ceiling below. `kubectl apply -k k8s/` (see `k8s/README.md`).
- Offline tests exercise the exact pipeline with in-memory buffer/store fakes — no
  containers needed for `make test`.

## Limitations (honest)

The **Docker Compose** demo is single-node (throughput ceiling is the machine, not the
design). The **Kubernetes** manifests (`k8s/`) run multi-node Kafka + OpenSearch and scale
ingest/indexer horizontally; they render cleanly (`kubectl kustomize`) and pass a
structural check, but a live-cluster throughput measurement is still pending (no
multi-node number is claimed until measured).

## Roadmap

v2 delivered: effectively-once delivery · tiered object-storage retention · multi-tenant
auth · alerting · shipper agent · Kubernetes manifests. Next: a concurrent load generator
and a measured multi-node run; exactly-once producer path; OpenSearch security (TLS + auth).
