# Kubernetes deployment

Multi-node manifests (Kustomize) for the whole pipeline: 3-broker Kafka, 3-node OpenSearch,
MinIO, and the app workloads (`ingest` behind an HPA, `indexer` ×3 = one per Kafka partition,
`alerting`, `query`). This closes the v1 "single-node" limitation.

```
k8s/
├── base/                 full multi-node deployment
│   ├── 00-namespace.yaml
│   ├── 10-config.yaml        ConfigMap (non-secret) + Secret (demo values — replace!)
│   ├── 20-kafka.yaml         StatefulSet ×3 (KRaft), headless + client Services
│   ├── 21-opensearch.yaml    StatefulSet ×3, headless + client Services
│   ├── 22-minio.yaml         Deployment + PVC + Service (archival target)
│   ├── 30-ingest.yaml        Deployment ×2 + Service + HorizontalPodAutoscaler (2→6, CPU 70%)
│   ├── 31-indexer.yaml       Deployment ×3 (one consumer per partition)
│   ├── 32-alerting.yaml      Deployment ×1 (own consumer group)
│   ├── 33-query.yaml         Deployment ×2 + Service + Ingress (log-aggregator.local)
│   └── kustomization.yaml
└── overlays/
    └── kind/             single-node overlay for a laptop smoke test (1 broker, 1 OS node)
```

## Deploy

```bash
# 1. Build the app image and load it into the cluster (kind example):
docker build -t log-aggregator:latest .
kind load docker-image log-aggregator:latest        # minikube: `minikube image load ...`

# 2. Apply — full multi-node…
kubectl apply -k k8s/base
#    …or the single-node overlay for a laptop (fits in a few GiB):
kubectl apply -k k8s/overlays/kind

# 3. Watch it come up (Kafka + OpenSearch are StatefulSets, give them a minute):
kubectl -n log-aggregator get pods -w

# 4. Reach the dashboard:
kubectl -n log-aggregator port-forward svc/query 8080:8080   # http://localhost:8080
```

## Scaling

- **Ingest** scales automatically via the HPA (needs metrics-server). Raise `maxReplicas` for
  more front-end throughput.
- **Indexer** throughput scales with Kafka partitions: keep `indexer.replicas` == the topic
  partition count (`KAFKA_NUM_PARTITIONS`, default 3) so each replica owns a partition.
- **OpenSearch** scales by raising `replicas` (and shard/replica counts in the index template).

## Production notes (honest — security)

- **You must set a real `JWT_SECRET`.** With `AUTH_ENABLED=true` (the base default) the app
  **fails closed** on the placeholder secret in `10-config.yaml` — the ingest/query pods refuse
  to boot until you replace `app-secret` with a real Secret (≥32-byte random `JWT_SECRET`, real
  `API_KEYS`, non-`minioadmin` S3 creds), e.g. via `kubectl create secret` or an
  external-secrets / SealedSecrets flow. This is intentional: it prevents shipping the demo
  secret to production.
- **Terminate TLS at the Ingress.** The provided `Ingress` has no `tls:` block — add a cert +
  `tls:` (cert-manager) so credentials/tokens aren't sent in cleartext. Bearer tokens over plain
  HTTP are interceptable.
- **Lock down the data plane.** OpenSearch runs with the security plugin **disabled** and Kafka
  as **PLAINTEXT** for the demo — anyone who can reach `:9200`/`:9092` bypasses the app's authz
  entirely. Before production: enable the OpenSearch security plugin (TLS + auth), Kafka
  SASL/TLS, and a `NetworkPolicy` so only the app pods can reach the datastores. Never expose
  those ports publicly (the compose file binds them to localhost for dev only).
- **Stateful services:** the Kafka and OpenSearch StatefulSets are hand-rolled for a
  self-contained demo. In production prefer operators — **Strimzi** (Kafka) and the
  **OpenSearch Operator** — which handle rebalancing, rolling upgrades, storage, *and* the TLS/
  auth above.
- **Validation status:** kustomize renders cleanly, a structural pass and **`kubeconform
  -strict` (20/20 valid, 0 skipped)** both pass, a **server dry-run** against a live API
  server accepts all 20 resources, and the **`overlays/kind` single-node stack was deployed
  and smoke-tested** on kind: all 8 pods reach Ready and a log POSTed to `ingest` flows
  through Kafka → indexer → OpenSearch and is returned by `query` in a few seconds, with
  auth enforced (401 without a credential). Still pending: a **multi-node throughput
  measurement** on a real (multi-GiB) cluster — no number is claimed until measured.
