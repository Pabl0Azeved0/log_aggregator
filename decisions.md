# Locked decisions — log_aggregator v1

Resolved from `handoff.md` Q1–Q6 (owner approved the builder's leanings, 2026-07-03).

- **Q1 — Search store: OpenSearch 2.x.** Apache-2.0 (no license friction in a public
  repo), same API surface as Elasticsearch for the résumé keyword.
- **Q2 — Buffer: Kafka (single-node KRaft) in compose.** It is the résumé claim being
  proven ("1M+ events/day via Kafka"). A `memory` backend exists for offline tests only —
  never the demo path.
- **Q3 — Throughput target:** sustain ≥ 5k events/s single-node with p99
  ingest→searchable < 2s. **Published numbers must come from `make loadgen` runs** —
  honest measured values beat round targets; the README states whatever was actually
  measured.
- **Q4 — Dashboard: small custom page** (vanilla JS, served by the query API): search,
  live tail, level filter. It exists to be the README demo GIF; no React tooling.
- **Q5 — Retention:** index-per-day (`logs-YYYY.MM.DD`), delete indices older than
  `RETENTION_DAYS` (default 7). Applied by the indexer on startup and periodically.
- **Q6 — Deploy:** local Docker Compose only in v1; README sells with a GIF. No live
  demo.

Architecture: ingest API (FastAPI, async, backpressure → 429) → Kafka → indexer worker
(batching, retry, JSONL dead-letter) → OpenSearch (daily indices, template with typed
mappings) → query API + dashboard. Three processes, one image.

Known v1 simplifications (documented, not hidden): Kafka auto-commit (at-least-once with
possible re-delivery, no exactly-once), single-node everything, retention is
delete-only (no rollover/ILM), dead-letter is a local JSONL file.
