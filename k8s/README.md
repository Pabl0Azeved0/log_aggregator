# Kubernetes deployment

Multi-node manifests (Kustomize) for the whole pipeline: 3-broker Kafka, 3-node OpenSearch,
MinIO, and the app workloads (`ingest` behind an HPA, `indexer` ×3 = one per Kafka partition,
`alerting`, `query`). This closes the v1 "single-node" limitation.

```
k8s/
├── 00-namespace.yaml
├── 10-config.yaml        ConfigMap (non-secret) + Secret (demo values — replace!)
├── 20-kafka.yaml         StatefulSet ×3 (KRaft), headless + client Services
├── 21-opensearch.yaml    StatefulSet ×3, headless + client Services
├── 22-minio.yaml         Deployment + PVC + Service (archival target)
├── 30-ingest.yaml        Deployment ×2 + Service + HorizontalPodAutoscaler (2→6, CPU 70%)
├── 31-indexer.yaml       Deployment ×3 (one consumer per partition)
├── 32-alerting.yaml      Deployment ×1 (own consumer group)
├── 33-query.yaml         Deployment ×2 + Service + Ingress (log-aggregator.local)
└── kustomization.yaml
```

## Deploy

```bash
# 1. Build the app image and make it available to the cluster (kind example):
docker build -t log-aggregator:latest .
kind load docker-image log-aggregator:latest        # minikube: `minikube image load ...`

# 2. Apply everything:
kubectl apply -k k8s/

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

## Production notes (honest)

- **Secrets:** `10-config.yaml` ships demo credentials so `apply -k` works out of the box —
  replace `app-secret` with a real Secret (or an external-secrets/SealedSecrets flow) and set
  `AUTH_ENABLED` + real `API_KEYS`/`JWT_SECRET`.
- **Stateful services:** the Kafka and OpenSearch StatefulSets are hand-rolled for a
  self-contained demo. In production prefer operators — **Strimzi** (Kafka) and the
  **OpenSearch Operator** — which handle rebalancing, rolling upgrades, and storage.
- **Security:** OpenSearch runs with the security plugin disabled and Kafka as PLAINTEXT for
  the demo; enable TLS + auth before exposing anything.
- **Validation status:** these manifests are schema-shaped and reviewed but were authored
  without a live cluster in this environment; validate with `kubectl apply --dry-run=server -k k8s/`
  (or `kubeconform`) and a smoke deploy on kind/minikube before relying on them. The
  multi-node throughput figure for the main README is **pending a real cluster run** — no
  number is claimed until measured.
