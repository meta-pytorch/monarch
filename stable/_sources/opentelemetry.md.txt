# OpenTelemetry

Monarch can export its metrics, logs, and spans over OTLP/HTTP to an
[OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) or any
backend that accepts OTLP. Built-in telemetry is exported with no code changes.
OTLP export is independent of job-local
[distributed telemetry](distributed-telemetry), and the two can run on the same
job.

## What Monarch exports

| Signal | Contents | Notes |
|--------|----------|-------|
| Metrics | Built-in actor, mailbox, and channel metrics, plus instruments created with `monarch.actor.get_meter` | Cumulative temporality, pushed every `OTEL_METRIC_EXPORT_INTERVAL` |
| Logs | Events recorded with Rust `tracing`, with level, target, thread, source location, and event fields as attributes | A log emitted inside a span carries that span's trace and span IDs |
| Spans | Rust `tracing` spans and Python `monarch.actor.span()` blocks, timed from when each starts and ends | Python spans are named after the name passed to `span()` |

Some noisy internal targets are filtered out of logs and spans.

## Enable export

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to the collector's base URL in the
environment of the client and of every worker host:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
```

Monarch appends the standard signal paths and sends OTLP/HTTP with protobuf
payloads to `/v1/metrics`, `/v1/logs`, and `/v1/traces`. Export is off when the
variable is unset. The collector must define a pipeline for each signal it
receives; see the example collector configuration in
[OpenTelemetry and Grafana](./generated/examples/otel_collector).

## Configuration

Set these values before telemetry initializes in each client and worker
process. Later changes do not reconfigure existing exporters, metric readers,
or tracing sinks.

| Setting | Default | Effect |
|---------|---------|--------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Unset | Collector base URL; enables metric, log, and span export |
| `OTEL_EXPORTER_OTLP_HEADERS` | Unset | Extra request headers, such as authentication tokens |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | Unset | Timeout for each export request |
| `OTEL_RESOURCE_ATTRIBUTES` | Unset | Extra resource attributes, as comma-separated `key=value` pairs |
| `OTEL_METRIC_EXPORT_INTERVAL` | `1s` | How often metrics are collected and pushed |
| `ENABLE_OTEL_METRICS` | `true` | Set to `false` to disable OTel metrics entirely, including job-local metric tables |
| `ENABLE_OTEL_TRACING` | `true` | Set to `false` to disable OTLP log and span export |

## Resource attributes

Every exported metric, log, and span carries these resource attributes:

| Attribute | Value |
|-----------|-------|
| `service.name` | `monarch/monarch` |
| `execution_id` | ID shared by the processes of one Monarch run |
| `user` | User running the process |
| `hostname` | Host name |
| `environment` | Execution environment detected by Monarch, such as `local` |
| `monarch_client_trace_id` | Client trace ID, when one is set |

Attributes from `OTEL_RESOURCE_ATTRIBUTES` are added to these. They do not
replace the attributes above, and `OTEL_SERVICE_NAME` does not change
`service.name`.

## Delivery and limitations

- Export is best-effort. A slow or unreachable collector drops telemetry rather
  than failing the job.
- Logs and spans are batched and sent in the background. The tracing dispatcher
  flushes its sinks periodically and drains queued events and flushes again
  when it shuts down. Process exit does not guarantee that dispatcher shutdown
  runs; abrupt termination such as `SIGABRT` bypasses it. A process that exits
  immediately after emitting telemetry can lose its final batch.
- A span is exported when it ends. A span that never closes is not exported.
- Python log records do not carry the trace context of an enclosing Python
  `span()`.

## Related documentation

- [Metrics](metrics) explains metric collection and how OTLP export relates to
  job-local metric tables.
- [OpenTelemetry and Grafana](./generated/examples/otel_collector) deploys a
  collector, Prometheus, Loki, and Grafana on Kubernetes.
- [Observability overview](observability)
