# Metrics

Monarch records built-in counters, gauges, and histograms with OpenTelemetry
(OTel). The same instruments can feed job-local distributed telemetry, an
external OpenTelemetry Protocol (OTLP) endpoint, or both. Start with the
[observability overview](observability) if you are choosing among Monarch's
diagnostic surfaces.

## Recording model

One process-global OTel meter provider owns metric instruments and their
readers. Libraries request a meter and record values without knowing which
backends are active. Threads and libraries in the same Rust process share this
provider. A different process, binary, or language runtime has its own provider
and exports independently.

Each instrument has a name, scope, unit, and type. The instrumentation code
that records a point also selects its attribute set. OTel aggregates points by
their complete identity, including attributes, so high-cardinality attributes
create more time series and more rows at each collection interval.

Independent periodic readers collect the same instruments for each enabled
export path. `OTEL_METRIC_EXPORT_INTERVAL` controls their interval and defaults
to one second. This provides low-latency visibility for short-lived jobs but
produces more export traffic than typical monitoring intervals. Increase the
interval for long-running jobs or high-cardinality instruments to reduce
application and backend load. A slow or unavailable destination does not
require different instrumentation for the other destinations.

## Record custom metrics

Use Monarch's process-global meter to create OpenTelemetry instruments. The
same instrument feeds every enabled export path:

```python
from monarch.actor import get_meter

requests = get_meter().create_counter("example.requests")
requests.add(1, {"operation": "predict", "outcome": "success"})
```

Create instruments once and reuse them. Use attributes only for bounded
dimensions such as operation and outcome; request IDs and other unbounded
values create a new time series for each value.

## Metric identity and context

An exported metric point combines context from several levels. Distributed
telemetry preserves those levels separately:

| OTel concept | Set by | Meaning | Distributed telemetry representation |
|--------------|--------|---------|--------------------------------------|
| Resource attributes | Meter-provider initialization | Process-wide service and runtime identity shared by every metric, such as `service.name` | `resource_attributes_json` |
| Instrumentation scope | The library requesting a meter | The library or module that owns the instrument | `scope_name` |
| Instrument | The instrument builder | The metric name, unit, and aggregation type | `name`, `unit`, and the selected metric table |
| Point attributes | Each record operation | Dimensions for one measurement, such as operation or outcome | `attributes_json` |

Point attributes are part of a metric series identity. Monarch's built-in
instrumentation selects these attributes. If instrumentation code records
the same instrument with `{"outcome": "success"}` and `{"outcome": "failure"}`,
OTel creates two independently aggregated series and therefore up to two rows
per collection interval. Instrumentation authors should use attributes for
bounded dimensions, not request IDs, timestamps, or other values that grow
without limit.

Resource attributes describe the process rather than an individual
measurement. Startup configuration selects supported values such as
`service.name` before the process-global provider is built. These attributes
are then copied into every distributed metric row and cannot change per
measurement. Instrumentation scope is also separate from point attributes: it
identifies the library that created the instrument and disambiguates
same-named instruments from different libraries. When grouping or joining
metric rows, treat resource attributes, scope, instrument metadata, and point
attributes as the complete context.

Distributed telemetry currently stores only the instrumentation scope name.
OTel scope version, schema URL, and scope attributes are not included in the
metric tables.

## Choose an export path

| Path | Enablement | Destination | Primary use |
|------|------------|-------------|-------------|
| Distributed telemetry | `job.enable_telemetry(...)` | Job-local SQL tables | Debugging and analysis within the current job |
| OTLP | `OTEL_EXPORTER_OTLP_ENDPOINT` | OpenTelemetry Collector over OTLP/HTTP | External aggregation, monitoring, and visualization |

You can enable both paths. They share instruments and resource attributes but
collect and export independently.

## Distributed telemetry path

```text
OTel instruments
    └─> periodic reader
          └─> Unix Domain Socket (UDS) metric exporter
                └─> host-local telemetry sidecar
                      └─> DataFusion SQL tables
```

Calling `job.enable_telemetry(...)` activates the process-global Unix-socket
sink. On each collection, the UDS exporter encodes non-empty aggregation
buffers as Arrow batches and sends them to the host-local telemetry sidecar.
The sidecar exposes three physical tables:

| Table | Aggregation |
|-------|-------------|
| `metric_gauges` | Gauge points |
| `metric_sums` | Counter and up-down-counter points |
| `metric_histograms` | Explicit histogram points and buckets |

The UDS exporter uses delta temporality, so each row describes one collection
interval. Each unique metric name, scope, and attribute set produces its own
row. Numeric values remain in separate `f64`, `i64`, and `u64` columns rather
than being converted to one common type.

This path is job-local, in memory, and best-effort. Empty aggregation buffers
produce no frame. A full producer queue, serialization failure, oversized
frame, or socket failure drops the affected frame rather than blocking the
application. Activating the socket starts delivery for future intervals; it
does not replay metrics collected while the sink was inactive. Metric tables
follow `TelemetryConfig.retention_secs`.

See [Distributed Telemetry](distributed-telemetry) for complete table contents
and SQL examples.

## OTLP path

```text
OTel instruments
    └─> periodic reader
          └─> OTLP metric exporter
                └─> OpenTelemetry Collector
                      └─> external metrics backend
```

In a build with OTLP support, setting `OTEL_EXPORTER_OTLP_ENDPOINT` enables an
independent metric reader. It sends collections to the configured OpenTelemetry
Collector over OTLP/HTTP. The collector controls routing, processing, and
delivery to external metrics systems.

See [OpenTelemetry and Grafana](./generated/examples/otel_collector) for a
Kubernetes collector, Prometheus, and Grafana example.

## Configuration

| Setting | Default | Effect |
|---------|---------|--------|
| `ENABLE_OTEL_METRICS` | `true` | Enables Monarch OTel metric collection |
| `OTEL_METRIC_EXPORT_INTERVAL` | `1s` | Sets the periodic reader interval |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Unset | Enables OTLP metric export when configured |
| `OTEL_SERVICE_NAME` | `unknown_service` | Sets the service name resource attribute |
| `TelemetryConfig.retention_secs` | `3600` | Retains distributed metric rows for this many seconds; `0` disables retention |

## Related documentation

- [Observability overview](observability)
- [Distributed Telemetry](distributed-telemetry)
- [OpenTelemetry and Grafana](./generated/examples/otel_collector)
