# Prometheus Backend for Monarch Telemetry

This module provides **modern Prometheus support** as an OpenTelemetry backend for the Monarch telemetry system, following the [official Prometheus OpenTelemetry guide](https://prometheus.io/docs/guides/opentelemetry/).

The implementation uses OpenTelemetry's OTLP HTTP protocol to send metrics directly to Prometheus's native OTLP receiver, eliminating the need for custom HTTP servers or scraping configurations.

## OTLP Approach

Uses **OpenTelemetry APIs** with Prometheus's native OTLP receiver:
- **API**: `opentelemetry::global::meter()` + `Counter`/`Histogram` instruments
- **Transport**: OTLP HTTP directly to Prometheus
- **Ecosystem**: Full OpenTelemetry ecosystem compatibility
- **Standards**: Follows OpenTelemetry semantic conventions
- **Benefits**: UTF-8 support, resource attributes, delta temporality

## Quick Start

1. **Start Prometheus with OTLP receiver**:
   ```bash
   prometheus --web.enable-otlp-receiver
   ```

2. **Set environment variables**:
   ```bash
   export HYPERACTOR_OTEL_BACKEND=prometheus
   export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
   export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:9090/api/v1/otlp/v1/metrics
   export OTEL_SERVICE_NAME="monarch"
   ```

## Configuration

### Standard OpenTelemetry Environment Variables

- **`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`**: Prometheus OTLP endpoint (default: `http://localhost:9090/api/v1/otlp/v1/metrics`)
- **`OTEL_EXPORTER_OTLP_PROTOCOL`**: Protocol to use (default: `http/protobuf`)
- **`OTEL_METRIC_EXPORT_INTERVAL`**: Export interval in milliseconds (default: `15000`)
- **`OTEL_SERVICE_NAME`**: Service name for resource attributes (default: `monarch`)
- **`OTEL_SERVICE_INSTANCE_ID`**: Service instance ID (default: auto-generated UUID)
- **`OTEL_RESOURCE_ATTRIBUTES`**: Additional resource attributes (format: `key1=value1,key2=value2`)
- **`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`**: Set to `delta` to use delta temporality (default: `cumulative`)

### Hyperactor-Specific Variables

- **`HYPERACTOR_OTEL_BACKEND`**: Set to `prometheus` to use Prometheus backend

## Prometheus Configuration

Configure Prometheus with OTLP support:

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

# Enable out-of-order ingestion for OTLP
storage:
  tsdb:
    out_of_order_time_window: 30m

# Configure resource attribute promotion
otlp:
  # UTF-8 support - no metric name translation needed
  translation_strategy: NoTranslation

  # Promote important resource attributes to labels
  promote_resource_attributes:
    - service.instance.id
    - service.name
    - service.namespace
    - service.version
    - deployment.environment
    - k8s.cluster.name
    - k8s.namespace.name
```

Start Prometheus with:
```bash
prometheus --config.file=prometheus.yml --web.enable-otlp-receiver
```

## Usage Examples

### Metrics Usage

```rust
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

let meter = global::meter("my-service");

// Counter
let requests = meter.u64_counter("http_requests_total").init();
requests.add(1, &[
    KeyValue::new("method", "GET"),
    KeyValue::new("status", "200"),
]);

// Histogram
let duration = meter.f64_histogram("http_request_duration_seconds").init();
duration.record(0.045, &[KeyValue::new("endpoint", "/api/data")]);

// Or use the convenient hyperactor_telemetry macros
use hyperactor_telemetry::{declare_static_counter, declare_static_histogram, kv_pairs};

declare_static_counter!(REQUESTS_TOTAL, "http_requests_total");
declare_static_histogram!(REQUEST_DURATION, "http_request_duration_seconds");

REQUESTS_TOTAL.add(1, kv_pairs!("method" => "GET", "status" => "200"));
REQUEST_DURATION.record(0.045, kv_pairs!("endpoint" => "/api/data"));
```

### Querying with Resource Attributes

```promql
# Rate of HTTP requests
rate(http_requests_total[2m])

# Join with resource attributes from target_info
rate(http_requests_total[2m])
* on (job, instance) group_left (k8s_cluster_name)
target_info
```

## Architecture

```
Monarch Library
    |
    | (OTLP HTTP)
    ↓
Prometheus OTLP Receiver
    |
    ↓
Prometheus TSDB
```




## Benefits

- ✅ **No custom HTTP server** - simpler deployment
- ✅ **Standard OpenTelemetry configuration** - better compatibility
- ✅ **UTF-8 metric names** - full OpenTelemetry semantic conventions
- ✅ **Resource attribute promotion** - automatic label generation
- ✅ **Delta temporality support** - experimental feature
- ✅ **Out-of-order samples** - better reliability
- ✅ **Query-time joins** - flexible resource attribute access
- ✅ **Cloud-native ready** - follows modern observability practices
- ✅ **Vendor neutral** - uses OpenTelemetry standards
