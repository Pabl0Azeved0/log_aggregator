# log-aggregator — Architecture & file-by-file reference

Internal reference for a technical deep-dive. **log-aggregator** is a high-throughput log
pipeline: producers (apps, or the bundled file-shipping agent) push logs to an ingest API,
which buffers them through **Kafka**; an indexer worker batches them into **OpenSearch**;
a query API + live dashboard search them; a separate alerting worker watches the same stream
for threshold breaches. It is multi-tenant, effectively-once, archives expiring data to object
storage, and ships with Docker Compose (single-node) and Kubernetes (multi-node) deployments.

**Stack:** Python 3.12, **FastAPI** (async) + **uvicorn**, **aiokafka** (Kafka KRaft),
**opensearch-py** (async) on **OpenSearch**, **boto3** → **MinIO/S3** for archival,
**PyJWT** + API keys for auth, **prometheus-client** metrics, a vanilla-JS dashboard.
Everything runs under **Docker Compose**; `k8s/` has Kustomize manifests (3-broker Kafka +
3-node OpenSearch, HPA) verified on kind. Offline tests use in-memory fakes — no containers.

```
apps / agent ──HTTP──► ingest API (:8000)   POST /logs · /logs/raw   [Bearer key/JWT]
                          │  429 backpressure · Prometheus counters
                          ▼
                        Kafka topic "logs" (KRaft)
                          │
              ┌───────────┴───────────────┐
              ▼                            ▼
     indexer worker                 alerting worker  (own consumer group)
   consume→batch→bulk index       sliding-window rules → webhook/console + alerts index
   retry · dead-letter · commit
              │
              ▼
        OpenSearch  (logs-<tenant>-YYYY.MM.DD · tiered retention → MinIO)
              ▲
   query API (:8080) ──► dashboard (search · live tail · alerts · sign-in)
   GET /search · /stats · /alerts · POST /auth/token   [tenant-scoped]
```

Data flow of one event: `POST /logs` → validate `LogEvent`, tag tenant → `Buffer.publish`
(Kafka) → `indexer.run` `get_batch` → `store.index` (bulk, deterministic `_id`) → commit
offset → searchable via `query_api`. The same Kafka messages are independently consumed by
`alerting` under a different group id.

---

## Root

- **`README.md`** — overview, quickstart, the measured-performance table, design notes,
  limitations, roadmap.
- **`Makefile`** — `install` (venv + deps), `test` (pytest), `smoke`, `format` (black),
  `up`/`down`/`logs` (Docker Compose), `loadgen` (drive load, print measurements).
- **`docker-compose.yml`** — the single-node stack: `kafka` (KRaft, single broker),
  `opensearch` (single-node, 512 MB heap), `minio` (archival), `ingest` (uvicorn, 1 worker
  so metrics are exact), `indexer`, `alerting` (own `KAFKA_GROUP`), `query`. A **`sidecar`
  profile** adds `demo-app` + `agent` on a shared volume. Volumes: `osdata`, `miniodata`,
  `shared-logs`.
- **`docker-compose.override.yml`** — *local, gitignored.* Remaps `ingest` to host `8001`
  (another project holds `8000` on the dev box) via the `!override` tag, and bind-mounts the
  dashboard for live edits. A commented block shows how to enable auth locally.
- **`Dockerfile`** — python:3.12-slim; installs `requirements.txt`; `COPY src scripts`;
  `PYTHONPATH=/app/src`; default entry runs the ingest API. One image, run with different
  commands per service.
- **`requirements.txt`** — fastapi, uvicorn[standard], pydantic, aiokafka,
  opensearch-py[async], boto3, httpx, prometheus-client, pyjwt, python-dotenv, pytest, black.
- **`conftest.py`** — puts `src/` on `sys.path` so tests import `log_aggregator` uninstalled.
- **`.env.example`** — copy to `.env` for local config (gitignored).
- **`.gitignore`** — ignores `.venv`, `dead_letter/`, the compose override, and **all `*.md`
  except `README.md`** (planning/architecture docs stay local).
- **`docs/dashboard-demo.gif`** — the dashboard demo embedded in the README.

---

## `src/log_aggregator/` (the pipeline)

- **`config.py`** — one env-driven `Settings` dataclass (buffer/store backends, Kafka
  bootstrap/topic/**group**, OpenSearch URL, retention days, batch size/timeout, dead-letter
  path, **archive** flags + S3 creds, **auth** flags + `API_KEYS`/`JWT_SECRET`, **alerting**
  `ALERT_RULES`/`ALERT_WEBHOOK`). `get_settings()` is `@lru_cache`d (read once per process).

- **`models.py`** — **`LogEvent`** (pydantic): `timestamp` (default `utcnow`), `level`,
  `service`, `message`, `attrs`, `tenant` (set authoritatively by the ingest API). A
  `field_validator` normalizes level (aliases WARN→WARNING, FATAL→CRITICAL; unknown→INFO).
  **`parse_line(raw, service)`** — tolerant parser: JSON lines map onto the schema; plain text
  keeps the whole line as `message` with keyword-detected level; never raises.

- **`buffer.py`** — the `Buffer` `Protocol` (`publish`, `get_batch`, **`commit`**, `close`) +
  two backends. **`MemoryBuffer`** (bounded `asyncio.Queue`; `BufferFull` → 429; offline
  tests). **`KafkaBuffer`** (aiokafka; producer created under a **double-checked
  `asyncio.Lock`** so a cold-start burst never sends on an unstarted producer; consumer uses a
  configurable `group_id`, `enable_auto_commit=False`; `commit()` commits offsets). Lazy
  imports so importing the module needs no broker. `make_buffer(settings)` picks the backend.

- **`store.py`** — the `Store` `Protocol` + `MemoryStore` (fake) + `OpenSearchStore` (real),
  plus module helpers. The biggest file; owns indexing, search, stats, retention, archival.
  - **`_doc_id(event)`** — deterministic SHA-1 over `(tenant, timestamp, service, level,
    message, attrs)` → idempotent writes (a redelivered event overwrites, no dupe).
  - **`_build_search_body(q, level, service, limit)`** — pure query builder; message search is
    **`match_phrase_prefix`** (contiguous phrase, not loose OR) + `term` filters.
  - **`ArchiveConfig`** — S3/MinIO target for retention archival.
  - **`MemoryStore`** — list-backed, idempotent by `_doc_id`, tenant-filtered reads; alerts
    list; delete-only retention.
  - **`OpenSearchStore`** — index-per-tenant-per-day `logs-<tenant>-YYYY.MM.DD`; lazy client
    that installs the `logs` + `alerts` index templates once; **`index`** via
    `async_streaming_bulk` (per-doc errors → `PartialIndexError` so only rejects are
    dead-lettered, not the whole batch); **`search`/`count`/`stats`** scoped to
    `logs-<tenant>-*` (`stats` has a 1 s per-tenant TTL cache; `track_total_hits: true`);
    **`record_alert`/`recent_alerts`** on a single `alerts` index (tenant filter);
    **`apply_retention`** — for each expiring daily index, `_archive_index` (scan → gzip
    JSONL → S3 via `asyncio.to_thread`) then delete; **`restore(name)`** re-indexes an
    archived day (idempotent). `PartialIndexError` carries `indexed`/`failed`.

- **`ingest_api.py`** — FastAPI producer side. `create_app(buffer, settings)` builds a
  `require_tenant` dependency, a `lifespan` that closes the buffer on shutdown (flushes the
  producer's linger window), and a shared **`_accept(events)`** helper (publish → 202, or
  `BufferFull` → **429**, incrementing Prometheus `INGESTED`/`REJECTED`). Endpoints:
  **`POST /logs`** (`LogEvent | list`), **`POST /logs/raw`** (text via `parse_line`) — both
  tag events with the authenticated tenant; **`/healthz`**, **`/metrics`**.

- **`query_api.py`** — FastAPI read side. `create_app(store, settings)` with `require_tenant`
  + a store-closing `lifespan`. **`GET /search`** / **`/stats`** / **`/alerts`**
  (tenant-scoped), **`POST /auth/token`** (API key → short-lived JWT), **`/healthz`**, and
  **`/`** serving the dashboard.

- **`security.py`** — multi-tenant auth. `parse_api_keys("k:tenant,…")`, `mint_jwt` /
  `verify_jwt` (HS256, PyJWT), `_bearer` header parse, and **`make_require_tenant(settings)`**
  — a FastAPI dependency returning the caller's tenant from an API key **or** a JWT (401
  otherwise). When `auth_enabled` is false it returns `"default"` (open dev mode).

- **`indexer.py`** — the indexing worker. **`_dead_letter(path, events)`** writes JSONL;
  **`_index_with_retry`** — `store.index`, on `PartialIndexError` dead-letter only the rejects
  (no retry), on transient errors retry 3× with backoff then dead-letter the batch;
  **`run(buffer, store, settings, once)`** — hourly retention + `get_batch` →
  `_index_with_retry` → **`buffer.commit()`** (offset committed only after a batch is handled
  → effectively-once with the idempotent `_id`). `_serve`/`main` add SIGTERM-clean shutdown.

- **`alerting.py`** — the alerting worker (separate consumer group). **`Rule`** dataclass +
  **`load_rules(json)`**; **`RuleEngine`** — deterministic sliding-window threshold detection
  per `(rule, tenant)` with cooldown, bounded memory (`observe(event, now)` → fired alerts);
  **`make_notifier(settings)`** — POST to a Slack-compatible `ALERT_WEBHOOK` or log to console;
  **`run`** consumes → `engine.observe` → notify + `store.record_alert`; `_serve`/`main`.

- **`agent.py`** — the log shipper (sidecar). **`ship(client, url, service, lines, headers,
  sleep)`** — POST a batch to `/logs/raw`, retrying 429/transport errors with exponential
  backoff (**never drops**). **`run`** tails a file (batch by size or flush interval, rewind
  partial trailing lines, reopen on truncation/rotation). `main` reads `INGEST_URL`,
  `AGENT_FILE`, `AGENT_SERVICE`, optional `AGENT_API_KEY`, etc.

- **`static/dashboard.html`** — self-contained (no external assets) OLED-dark observability
  dashboard: live KPI strip (total, computed ingest rate, level-distribution bar, top
  services), phrase-prefix search + level/service filters, sticky-header live-tail table with
  new-row flash, a firing-**alerts** panel, and a **sign-in overlay** (appears on 401; API key
  → `/auth/token` → JWT in `localStorage`, sent as Bearer). Polls `/search`+`/stats`+`/alerts`
  every 1.5 s.

- **`__init__.py`** — empty package marker.

---

## `scripts/`

- **`loadgen.py`** — the load generator, the **only** legitimate source of the README's perf
  numbers. Async client drives `/logs` at a target rate, measures achieved rate + p50/p99
  request latency, then polls `/stats` to measure ingest→searchable drain lag. `--rate`,
  `--duration`, `--batch`, `--url`, `--query-url`.
- **`demo_app.py`** — a tiny fake service that appends random log lines to a file; the source
  for the `agent` sidecar demo (`sidecar` compose profile).

---

## `k8s/` (Kustomize; verified on kind)

- **`base/`** — the full multi-node deployment. `00-namespace`, `10-config` (ConfigMap +
  demo Secret), `20-kafka` (3-broker KRaft StatefulSet + headless/client Services, node id
  from the pod ordinal, `publishNotReadyAddresses` so the quorum can resolve peers),
  `21-opensearch` (3-node StatefulSet + `vm.max_map_count` init container), `22-minio`,
  `30-ingest` (Deployment + Service + **HPA** 2→6 @ CPU 70 %), `31-indexer` (×3, one per
  Kafka partition), `32-alerting` (×1, own group), `33-query` (Deployment + Service +
  Ingress), `kustomization.yaml`.
- **`overlays/kind/`** — single-node overlay (1 broker RF=1, 1 OpenSearch node, 1 replica each)
  so the whole stack fits a laptop. `kubectl apply -k k8s/overlays/kind`.
- **`README.md`** — deploy steps, scaling guidance, production notes (prefer operators for the
  stateful services), and the validation status (kubeconform 20/20 + server dry-run + kind
  smoke deploy all pass; multi-node throughput still unmeasured).

---

## `tests/` (offline — in-memory fakes, no containers)

- **`test_pipeline_offline.py`** — the real `indexer.run` + real FastAPI apps against
  `MemoryBuffer`/`MemoryStore`: ingest→index→search roundtrip, backpressure `BufferFull`/429,
  retention, dead-letter on persistent failure, **partial-failure isolation**, **idempotent
  redelivery**, **commit-after-batch**.
- **`test_store.py`** — `_build_search_body` (phrase-prefix + terms + match_all), stats TTL
  cache, retention **archive-then-delete** ordering vs delete-only, restore round-trip.
- **`test_auth.py`** — API-key parsing, JWT round-trip, 401 enforcement, dev-mode-open,
  end-to-end **tenant isolation**, API-key → JWT exchange.
- **`test_alerting.py`** — rule fires at threshold, cooldown blocks then re-fires, level/service
  filters, per-tenant state, worker records + notifies.
- **`test_agent.py`** — the shipper **never drops** on 429/outage/500 (retries same batch),
  immediate return on 202.
- **`test_buffer_concurrency.py`** — the Kafka producer is created/started exactly once and
  never used before start under concurrent publishes.
- **`test_models.py`** — `LogEvent` validation / level normalization / `parse_line`.

---

## Pipeline stages (quick map)

`producer/agent → ingest_api (validate, tag tenant, backpressure) → Buffer (Kafka) → indexer
(batch, idempotent bulk index, dead-letter, commit) → OpenSearch (per-tenant daily indices) →
query_api/dashboard`. Parallel: `Buffer (Kafka, alerting group) → alerting (rules) →
notify + alerts index`. Lifecycle: `apply_retention → archive to MinIO → delete`.

## HTTP surface (quick map)

- **Ingest (:8000):** `POST /logs`, `POST /logs/raw`, `GET /healthz`, `GET /metrics`.
- **Query (:8080):** `GET /search`, `GET /stats`, `GET /alerts`, `POST /auth/token`,
  `GET /healthz`, `GET /` (dashboard). All data endpoints resolve a tenant via `require_tenant`.

## Guarantees (quick map)

- **Backpressure:** buffer-full → `429`, never unbounded buffering.
- **Effectively-once:** manual offset commit after a handled batch + deterministic `_id`
  (redelivery overwrites). Verified by offset-rewind → 0 dupes.
- **Partial-failure isolation:** one poison doc is dead-lettered; the rest of the batch indexes.
- **Tiered retention:** expiring indices archived to object storage (gzip JSONL) then deleted;
  `restore()` brings a day back.
- **Multi-tenancy:** per-tenant indices `logs-<tenant>-*`; reads scoped to the caller.

---

## Notable design observations (decision-relevant)

Factual notes if you plan to build on / harden this — not judgments:

1. **One image, many roles.** ingest / indexer / alerting / query / agent are the same image
   run with different commands; behavior is entirely env-driven via `Settings`.
2. **Dev mode is open by design.** `AUTH_ENABLED=false` (default) → single `default` tenant,
   no credentials — so `make loadgen` and the offline suite need no secrets. Turn it on with
   `API_KEYS` + a 32-byte `JWT_SECRET`. The committed compose stays dev-mode.
3. **Alerting is single-replica.** Rule window state is in-process; scaling the alerting
   worker would need per-tenant sharding or externalized state (noted in `k8s/`).
4. **Ingest runs one uvicorn worker** so the in-process Prometheus counters are exact; scale
   out with replicas (or Prometheus multiprocess mode) rather than `--workers N`.
5. **Fakes mirror the real stores/buffers exactly** (same `Protocol`, same idempotency/tenant
   semantics) so the offline tests exercise the real `indexer`/`alerting`/API code paths.
6. **Two isolation models by volume:** logs use *index-name* isolation (`logs-<tenant>-*`,
   strong, per-tenant lifecycle); low-volume alerts use *field* isolation (one `alerts` index
   + tenant filter) to avoid index sprawl.
7. **Producer-side exactly-once is not claimed.** Effectively-once covers the consumer/store
   (idempotent `_id` + commit-after-index); a transient-failure batch is still dead-lettered
   then committed rather than seek-retried.
8. **The multi-node throughput number is deliberately unmeasured** — the k8s manifests deploy
   and run (kind smoke test), but no multi-node figure is claimed until run on a real cluster.

---

*Not tracked in git: `.venv/`, `dead_letter/`, `.env`, `docker-compose.override.yml`, and the
local planning docs `DEVELOPMENT.md` / `V2_PROGRESS.md` (all `*.md` are gitignored except
`README.md`, this `ARCHITECTURE.md`, and `k8s/README.md`).*
