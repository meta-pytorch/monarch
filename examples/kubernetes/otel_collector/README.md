# OTEL Collector on Kubernetes

End-to-end example of exporting Monarch metrics and logs to an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) running in a Kubernetes cluster, with [Grafana](https://grafana.com/grafana/) for visualization.

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, Monarch's telemetry layer exports metrics and logs via OTLP/HTTP to the specified collector. Metrics are exported automatically. Log export additionally requires `USE_UNIFIED_LAYER=true`, which enables the unified tracing layer that wires up the OTLP log sink. Built-in actor system metrics (mailbox posts, messages sent/received, queue sizes, etc.) and log events are exported with no code changes.

## Architecture

```
┌──────────────┐
│  Controller  │──OTLP/HTTP──┐
│  (main.py)   │             │
└──────────────┘             ▼
                        ┌────────────────┐     ┌────────────┐
                        │ OTEL Collector │────▶│  stdout    │
                        │  (port 4318)   │     │  (debug)   │
                        └────────────────┘     └────────────┘
┌──────────────┐             ▲           │     ┌────────────┐
│  Worker pods │──OTLP/HTTP──┘           ├────▶│ Prometheus │
│  (mesh)      │                         │     │ (port 8889)│
└──────────────┘                         │     └────────────┘
                                         │     ┌────────────┐
                                         └────▶│   Loki     │
                                               │ (port 3100)│
                                               └────────────┘
                                                     │
                                         ┌───────────┴───────────┐
                                         │       Grafana         │
                                         │     (port 3000)       │
                                         │  Prometheus + Loki    │
                                         │    datasources        │
                                         └───────────────────────┘
```

Both the controller and worker pods set `OTEL_EXPORTER_OTLP_ENDPOINT` pointing at the collector service. The collector receives OTLP metrics and logs and fans them out to:

- **debug exporter** — logs metrics and log records to stdout (verify with `kubectl logs`)
- **prometheus exporter** — exposes a `/metrics` endpoint on port 8889
- **loki exporter** — forwards log records to [Grafana Loki](https://grafana.com/oss/loki/) for aggregation and querying

[Grafana](https://grafana.com/grafana/) connects to both Prometheus and Loki as datasources, providing a unified UI for exploring metrics and logs.

## Prerequisites

- A Kubernetes cluster with [Monarch CRD and operator](https://github.com/meta-pytorch/monarch-kubernetes/) installed
- `kubectl` configured to access the cluster

## Deploy

```bash
# Create the namespace
kubectl create namespace monarch-tests

# Deploy Loki (log aggregation backend)
kubectl apply -f manifests/loki.yaml

# Wait for Loki to be ready
kubectl rollout status deployment/loki -n monarch-tests

# Deploy the OTEL collector
kubectl apply -f manifests/otel-collector.yaml

# Wait for the collector to be ready
kubectl rollout status deployment/otel-collector -n monarch-tests

# Deploy Grafana (visualization)
kubectl apply -f manifests/grafana.yaml

# Wait for Grafana to be ready
kubectl rollout status deployment/grafana -n monarch-tests

# Deploy the controller pod (includes RBAC)
kubectl apply -f manifests/controller.yaml

# Wait for the controller pod
kubectl wait --for=condition=Ready pod/otel-controller -n monarch-tests --timeout=120s
```

## Run the Example

```bash
# Copy the script to the controller
kubectl cp main.py monarch-tests/otel-controller:/tmp/main.py

# Run the example
kubectl exec -it otel-controller -n monarch-tests -- \
  python /tmp/main.py --num-replicas=2 --iterations=5
```

The script provisions a MonarchMesh with `OTEL_EXPORTER_OTLP_ENDPOINT` set on worker pods, spawns actors, runs several rounds of work, then cleans up.

## Visualize in Grafana

Port-forward and open the Grafana UI:

```bash
kubectl port-forward -n monarch-tests svc/grafana 3000:3000
```

Open [http://localhost:3000](http://localhost:3000) (no login required).

### Explore Metrics

1. Go to **Explore** (compass icon in the left sidebar)
2. Select **Prometheus** datasource from the dropdown
3. Try these queries:
   - `mailbox_posts_total` — total mailbox posts across all actors
   - `actor_messages_sent_total` — messages sent by the actor system
   - `actor_messages_received_total` — messages received
   - `rate(mailbox_posts_total[1m])` — mailbox post rate per second

### Explore Logs

1. Go to **Explore** (compass icon in the left sidebar)
2. Select **Loki** datasource from the dropdown
3. Try these queries:
   - `{service_name="monarch-worker"}` — all worker logs
   - `{service_name="monarch-controller"}` — all controller logs
   - `{service_name=~".+"}` — all logs from any service
   - `{service_name="monarch-worker"} |= "error"` — filter for error messages

## Verify Metrics and Logs

Check the collector's debug output to confirm metrics and logs are being received:

```bash
kubectl logs -n monarch-tests deployment/otel-collector --tail=100
```

You should see metric data points logged with names like `mailbox.posts`, `actor.messages_sent`, `actor.messages_received`, etc., as well as log records from the actor system.

To view metrics via Prometheus:

```bash
# Port-forward the Prometheus endpoint
kubectl port-forward -n monarch-tests svc/otel-collector 8889:8889

# In another terminal, scrape metrics
curl -s http://localhost:8889/metrics | head -50
```

## Query Logs in Loki

Loki receives log records from the OTel Collector. Port-forward and query via the HTTP API:

```bash
# Port-forward the Loki endpoint
kubectl port-forward -n monarch-tests svc/loki 3100:3100

# List available labels (confirm logs are ingested)
curl -s http://localhost:3100/loki/api/v1/labels

# Query recent logs (last 1 hour)
# Change service name monarch-controller or monarch-worker to filter by worker/controller logs
curl -s http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service_name=~".+"}' \
  --data-urlencode "start=$(date -d '1 hour ago' +%s)" \
  --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'limit=10' | python3 -m json.tool

# Compact view: show level, target, and log body
# Change service name monarch-controller or monarch-worker to filter by worker/controller logs
curl -s http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service_name=~".+"}' \
  --data-urlencode "start=$(date -d '1 hour ago' +%s)" \
  --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'limit=20' \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
for stream in data.get('data', {}).get('result', []):
    labels = stream.get('stream', {})
    level = labels.get('level', '?')
    target = labels.get('log_target', labels.get('target', ''))
    for ts, body in stream.get('values', []):
        print(f'[{level}] {target}: {body}')
"
```

## Expected Output

```
Controller OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector.monarch-tests.svc.cluster.local:4318
Connecting to Kubernetes...
Waiting for 2 worker pod(s)...
Spawning actors...
  Round 1: ['pong from workers-0.workers.monarch-tests.svc.cluster.local', ...]
    workers-0...: computed 1000 iterations
    workers-1...: computed 1000 iterations
  Round 2: ['pong from workers-0...', ...]
  ...
Waiting for metrics and logs to flush to collector...

Verify metrics and logs in the OTEL collector logs:
  kubectl logs -n monarch-tests deployment/otel-collector --tail=100

Scrape Prometheus endpoint:
  kubectl port-forward -n monarch-tests svc/otel-collector 8889:8889
  curl http://localhost:8889/metrics
Done.
```

## Cleanup

```bash
kubectl delete -f manifests/controller.yaml
kubectl delete -f manifests/grafana.yaml
kubectl delete -f manifests/otel-collector.yaml
kubectl delete -f manifests/loki.yaml
kubectl delete namespace monarch-tests
```

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (unset) | Collector endpoint. When set, enables OTLP metric and log export. |
| `OTEL_SERVICE_NAME` | `unknown_service` | Service name attached to all exported telemetry. Used as the `service_name` label in Loki. |
| `OTEL_METRIC_EXPORT_INTERVAL` | `1s` | How often the periodic metric reader pushes to the exporter. |
| `USE_UNIFIED_LAYER` | `false` | Must be `true` to enable the unified tracing layer, which wires up the OTLP log sink. |
| `ENABLE_OTEL_METRICS` | `true` | Set to `false` to disable OTel metrics entirely. |
| `OTEL_EXPORTER_OTLP_HEADERS` | (unset) | Additional headers for the OTLP exporter (e.g., auth tokens). |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | (unset) | Timeout for OTLP export requests. |
