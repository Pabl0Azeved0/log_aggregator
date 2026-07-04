# Handoff — log_aggregator (portfolio project, build FIRST)

## What this is and why it exists

A **public portfolio project**: a high-throughput **log aggregation microservice** —
collect logs from many sources, ingest them through a real pipeline, index them, and make
them searchable through a query API + a small dashboard.

**Positioning (from `reasoning.md`, the source of truth for why this project):** the
pinned portfolio already proves AI/RAG (clinical-rag) and real-time backend (pulse-platform).
The résumé's biggest UNPROVEN claim is **distributed systems / high-throughput infra**
("1M+ events/day via Kafka" at 1STi — no repo demonstrates it). This project exists to
close that gap. It must read as **infrastructure engineering, not a bootcamp exercise**.

Origin brief (transcribed in `reasoning.md`, Appendix A): a junior Node + ELK wiring
exercise (Logstash TCP input → Elasticsearch index). We keep only its concept; everything
below is the senior elevation.

## The one non-negotiable: measured numbers

The entire value of this project is a README that states **evidence, not adjectives**:
"sustained **X events/s** ingestion on a single machine, p99 indexing latency Y ms,
Z-day retention" — with the load-test script committed so anyone can reproduce it.
If the project ships without reproducible throughput numbers, it has failed its purpose.
This is the same honesty discipline as clinical-rag (measured evals) and master-profile
(measured validator gaps).

## Architecture (target, senior elevation)

```
producers (demo apps / load generator)
   │  HTTP + optional TCP/UDP syslog-style
   ▼
ingestion API (Python/FastAPI, async)  ← backpressure story lives here
   │
   ▼
buffer (decision Q2: Kafka vs Redis Streams vs in-process queue)
   │
   ▼
indexer worker(s) — batch, retry, dead-letter
   │
   ▼
search store (decision Q1: Elasticsearch vs OpenSearch)
   │
   ▼
query API (FastAPI) ──► dashboard (decision Q4)
```

- **Stack:** Python 3.12 + FastAPI (async) — matches the owner's résumé stack and
  pulse-platform; NOT the brief's Node.js.
- **Structured logs:** JSON schema (timestamp, level, service, message, attrs) with a
  tolerant parser for plain-text lines.
- **Docker Compose** for the whole stack, one command up (pulse-platform discipline).
- **Load generator** committed (`scripts/loadgen.py`): configurable rate/payload; this is
  what produces the README numbers.
- **Observability of itself:** basic Prometheus metrics (ingest rate, queue depth, index
  lag) — the owner's résumé lists Prometheus/Grafana; eating our own dog food is the story.

## Decisions to lock BEFORE scaffold (builder: ask, don't assume)

- **Q1 — Search store:** Elasticsearch vs **OpenSearch** (leaning OpenSearch: Apache-2.0,
  no license friction in a public repo, same API surface for the résumé keyword).
- **Q2 — Buffer:** **Kafka** (ties directly to the 1STi résumé claim; heavier) vs Redis
  Streams (lighter, already known from pulse) vs none-v1 (ingest → batch indexer direct,
  add Kafka in v2). Leaning Kafka — it's the résumé keyword being proven — but confirm
  the owner's machine can run the full compose stack comfortably.
- **Q3 — Throughput target:** what number makes the claim credible on a dev machine?
  Propose: sustain ≥ 5k events/s single-node with p99 ingest→searchable < 2s, then report
  whatever the real measured ceiling is (honest numbers > round numbers).
- **Q4 — Dashboard:** Kibana/OpenSearch-Dashboards out-of-the-box (zero code, less
  impressive) vs a small custom page (React or server-rendered) showing search + live
  tail + error-rate chart. Leaning custom-but-small: it's the demo GIF for the README.
- **Q5 — Retention/ILM:** simple index-per-day + delete-after-N-days policy in v1?
- **Q6 — Deploy:** local-compose-only v1 (like pulse) with a README GIF, or a small live
  demo? (clinical-rag lesson: the GIF sold it; live demo optional.)

## v1 scope

**In:** ingestion API + buffer + indexer + search store + query API + minimal dashboard +
load generator + measured numbers in README + Docker Compose + tests (unit + an offline
end-to-end smoke, master-profile style).
**Out (v2+):** multi-tenant auth, alerting rules, agents/sidecars for log shipping,
Kubernetes manifests, live deploy.

## Reuse from existing projects

- **pulse-platform:** Docker Compose discipline, async FastAPI patterns, Makefile targets
  (`make restart`, `make test`, `make populate-db` → here `make loadgen`).
- **clinical-rag:** README structure that sells (one-line hook + GIF at top, honest
  limitations section, measured tables), micro-commit style, golden-set/eval discipline.
- **master-profile:** offline smoke pipeline pattern (`make smoke`, fakes for the store),
  GOTCHAS.md habit for hard-won environment lessons.

## Repo conventions

Public repo (this IS for the portfolio, unlike master-profile). Lowercase imperative
commit subjects, no Co-Authored-By trailer (same as clinical-rag). README in English.
No secrets in git; `.env.example` only.

## Hard rules

1. **No fabricated numbers.** Every performance figure in the README must come from the
   committed load-test script, reproducible with one command.
2. The brief's scope (Logstash config + one index) is NOT the project — if time-boxed,
   cut the dashboard before cutting the pipeline/measurement.
3. English-only repo content (recruiter-facing).
