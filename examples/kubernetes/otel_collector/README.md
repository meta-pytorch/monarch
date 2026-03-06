# OTEL Collector on Kubernetes

End-to-end example of exporting Monarch metrics to an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) running in a Kubernetes cluster.

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, Monarch's telemetry layer exports metrics via OTLP/HTTP to the specified collector. Built-in actor system metrics (mailbox posts, messages sent/received, queue sizes, etc.) are exported automatically with no code changes.

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
┌──────────────┐             ▲                ┌────────────┐
│  Worker pods │──OTLP/HTTP──┘           ────▶ │ Prometheus │
│  (mesh)      │                               │ (port 8889)│
└──────────────┘                               └────────────┘
```

Both the controller and worker pods set `OTEL_EXPORTER_OTLP_ENDPOINT` pointing at the collector service. The collector receives OTLP metrics and fans them out to:

- **debug exporter** — logs metrics to stdout (verify with `kubectl logs`)
- **prometheus exporter** — exposes a `/metrics` endpoint on port 8889

## Prerequisites

- A Kubernetes cluster with [Monarch CRD and operator](https://github.com/meta-pytorch/monarch-kubernetes/) installed
- `kubectl` configured to access the cluster

## Deploy

```bash
# Create the namespace
kubectl create namespace monarch-tests

# Deploy the OTEL collector
kubectl apply -f manifests/otel-collector.yaml

# Wait for the collector to be ready
kubectl rollout status deployment/otel-collector -n monarch-tests

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

## Verify Metrics

Check the collector's debug output to confirm metrics are being received:

```bash
kubectl logs -n monarch-tests deployment/otel-collector --tail=100
```

You should see metric data points logged with names like `mailbox.posts`, `actor.messages_sent`, `actor.messages_received`, etc.

To view metrics via Prometheus:

```bash
# Port-forward the Prometheus endpoint
kubectl port-forward -n monarch-tests svc/otel-collector 8889:8889

# In another terminal, scrape metrics
curl -s http://localhost:8889/metrics | head -50
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
Waiting for metrics to flush to collector...

Verify metrics in the OTEL collector logs:
  kubectl logs -n monarch-tests deployment/otel-collector --tail=100

Scrape Prometheus endpoint:
  kubectl port-forward -n monarch-tests svc/otel-collector 8889:8889
  curl http://localhost:8889/metrics
Done.
```

## Cleanup

```bash
kubectl delete -f manifests/controller.yaml
kubectl delete -f manifests/otel-collector.yaml
kubectl delete namespace monarch-tests
```

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (unset) | Collector endpoint. When set, enables OTLP metric export. |
| `OTEL_METRIC_EXPORT_INTERVAL` | `1s` | How often the periodic metric reader pushes to the exporter. |
| `ENABLE_OTEL_METRICS` | `true` | Set to `false` to disable OTel metrics entirely. |
| `OTEL_EXPORTER_OTLP_HEADERS` | (unset) | Additional headers for the OTLP exporter (e.g., auth tokens). |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | (unset) | Timeout for OTLP export requests. |
