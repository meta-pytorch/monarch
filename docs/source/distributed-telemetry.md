# Distributed Telemetry

Monarch distributed telemetry collects actor-system events and OpenTelemetry
metrics on each host and makes them queryable as one logical database. It uses
[Apache DataFusion](https://github.com/apache/datafusion), an extensible SQL
query engine built on [Apache Arrow](https://arrow.apache.org/). Distributed
telemetry is the query layer behind the [Monarch Dashboard](monarch-dashboard)
and [actor profiling](profiling), and complements the live
[Mesh Admin TUI](admin-tui).

Start with the [observability overview](observability) if you are choosing a
diagnostic surface or configuring a job for the first time.

## Quick start

Enable telemetry before obtaining job state:

```python
from monarch.job import ProcessJob, TelemetryConfig

job = ProcessJob({"workers": 2}).enable_telemetry(
    TelemetryConfig(retention_secs=3600)
)
state = job.state(cached_path=None)

client = state.query_engine_client
if client is None:
    raise RuntimeError("telemetry did not start")

result = client.query(
    """
    SELECT a.full_name, s.new_status, s.reason, s.timestamp_us
    FROM actor_status_events AS s
    JOIN actors AS a ON a.id = s.actor_id
    ORDER BY s.timestamp_us DESC
    LIMIT 50
    """
)
for row in result["rows"]:
    print(row)
```

`QueryEngineClient.query()` accepts DataFusion SQL and returns a dictionary
whose `rows` value is a list of dictionaries. Timestamps ending in `_us` are
microseconds since the Unix epoch. IDs are opaque join keys; do not infer
meaning from their numeric values.

## Core tables

| Table | Contents |
|-------|----------|
| `actor_status_events` | Actor status transitions and reasons |
| `actors` | Actor identity, rank, mesh, and creation time |
| `events` | Structured tracing events and log fields |
| `gpu_metrics` | Per-device GPU utilization, memory, temperature, and power samples |
| `host_metrics` | Per-host CPU, load, memory, swap, network, and disk samples |
| `meshes` | Mesh creation, shape, class, name, and parent view |
| `metric_gauges` | Gauge points with typed `value_f64`, `value_i64`, or `value_u64` values |
| `metric_histograms` | Explicit histogram counts, typed summaries, bounds, and bucket counts |
| `metric_sums` | Counter and up-down-counter points with typed sums and sum semantics |
| `message_status_events` | Message delivery status transitions |
| `messages` | Individual sender-to-receiver message deliveries |
| `sent_messages` | One-to-many send operations and destination views |
| `span_events` | Span enter, exit, and close events |
| `spans` | Trace span metadata and parent relationships |

Mesh-introspection snapshots add `actor_failures`, `actor_inbound_orderings`,
`actor_nodes`, `children`, `host_nodes`, `nodes`, `ordering_sessions`,
`proc_nodes`, `resolution_errors`, `root_nodes`, and `snapshots`. Persisted
py-spy captures add `pyspy_dumps`, `pyspy_frames`, `pyspy_local_variables`, and
`pyspy_stack_traces`.

Discover the current table catalog with:

```sql
SELECT table_name
FROM information_schema.tables
ORDER BY table_name
```

### Metric tables

Metrics are split by OTel aggregation rather than numeric type. Query
`metric_gauges`, `metric_sums`, or `metric_histograms` based on the instrument
aggregation. Each table preserves `f64`, `i64`, and `u64` in separate nullable
columns so large integers do not lose precision.

All metric rows include the instrument name, scope, unit, timestamps, point
attributes, and resource attributes. Sum rows also include temporality and
monotonicity. Histogram rows retain the count, typed sum, optional typed minimum
and maximum, explicit bounds, and bucket counts. Bounds and bucket counts are
JSON arrays in `bounds_json` and `bucket_counts_json`.

## Query examples

Count actor status transitions:

```sql
SELECT new_status, COUNT(*) AS count
FROM actor_status_events
GROUP BY new_status
ORDER BY count DESC
```

Find recent actor failure events:

```sql
SELECT a.full_name, s.reason, s.timestamp_us
FROM actor_status_events AS s
JOIN actors AS a ON a.id = s.actor_id
WHERE s.new_status = 'Failed'
ORDER BY s.timestamp_us DESC
LIMIT 100
```

Summarize message delivery transitions:

```sql
SELECT status, COUNT(*) AS count
FROM message_status_events
GROUP BY status
ORDER BY count DESC
```

Sum delta counter points within the retention window:

```sql
SELECT scope_name, SUM(sum_u64) AS mailbox_posts
FROM metric_sums
WHERE name = 'mailbox.posts'
  AND temporality = 'delta'
  AND sum_u64 IS NOT NULL
GROUP BY scope_name
ORDER BY mailbox_posts DESC
```

## Architecture

Each producer writes framed Arrow IPC batches to a host-local Unix socket.
Trace and entity dispatchers write their corresponding tables, while an OTel
UDS metric exporter writes one frame for each non-empty aggregation buffer. A
host-local `TelemetryActor` owns the socket, a Rust `DatabaseScanner`, and the
in-memory tables. The root query engine plans SQL with DataFusion, pushes table
scans to active collectors, and merges their Arrow results.

The telemetry job sidecar owns the root collector and query service. Its
in-memory state survives repeated `state()` calls and parent-process refreshes
for the same job allocation. Calling `job.kill()` stops the sidecar and removes
that state.

Distributed scans are best-effort. If a worker collector fails or times out,
the query can return reduced coverage while the job and healthy collectors
continue operating.

## Retention and snapshots

`TelemetryConfig.retention_secs` applies to `sent_messages`, `messages`,
`message_status_events`, `metric_gauges`, `metric_sums`,
`metric_histograms`, `spans`, `span_events`, `events`, `host_metrics`, and
`gpu_metrics`.
Trace tables are
filtered independently by row timestamp, with spans filtered before dependent
trace rows. A recent trace row is not displayed by span-joined views if its span
has already expired. The default is 3600 seconds. Set it to `0` to disable
automatic retention. Periodic maintenance runs approximately once per tenth of
the retention window, bounded between 30 seconds and 5 minutes and never longer
than the retention window. Queries do not run retention, so expired rows can
remain visible until the next periodic sweep. Other core tables have no
automatic retention window.

`TelemetryConfig.snapshot_interval_secs` controls periodic Mesh Admin topology
snapshots. The default is 30 seconds; set it to `0` to disable them. Snapshots
and event telemetry describe different views of the same job:

- Event tables record changes such as actor creation, status transitions, and
  message delivery.
- Snapshot tables capture the administrative topology and actor diagnostics at
  a point in time.

## Host metrics

Every telemetry host samples its own CPU, load average, memory, swap, network,
and disk usage into `host_metrics`, one row per host per interval.
`TelemetryConfig.host_metrics_interval_secs` sets the interval; the default is
1 second, and `0` disables sampling. Each wait varies randomly by about 10%
around the interval, so hosts do not sample in lockstep. Samplers run next to
each host's telemetry collector, including the client host. Values are
host-wide, not limited to the job's container or cgroup.

A host has one row per sample, so `hostname` alone is not unique; `hostname`
and `timestamp_us` together identify a row.

To find the metrics for a Monarch host, start from its `HostAgent` in
`actors`, find its snapshot row in `host_nodes`, whose `node_id` is `host:`
followed by the agent's `full_name`, and join `host_nodes.hostname` to
`host_metrics.hostname`. Several Monarch hosts on one machine all match that
machine's rows. Snapshots repeat every `snapshot_interval_secs`, so pick one
snapshot to avoid duplicate rows:

```sql
WITH latest AS (
  SELECT snapshot_id FROM snapshots ORDER BY snapshot_ts DESC LIMIT 1
)
SELECT a.full_name AS host_agent, a.rank, hm.timestamp_us, hm.cpu_percent
FROM actors AS a
JOIN meshes AS m ON m.id = a.mesh_id AND m.class = 'Host'
JOIN host_nodes AS hn ON hn.node_id = 'host:' || a.full_name
JOIN latest ON latest.snapshot_id = hn.snapshot_id
JOIN host_metrics AS hm ON hm.hostname = hn.hostname
ORDER BY a.rank, hm.timestamp_us
```

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_us` | int64 | Sample time, microseconds since the Unix epoch |
| `hostname` | string | Host name of the sampling host |
| `cpu_percent` | float64 | System-wide CPU utilization, 0 to 100, since the previous sample |
| `cpu_count` | uint64, nullable | Number of logical CPUs; null when it cannot be determined |
| `load_avg_1m`, `load_avg_5m`, `load_avg_15m` | float64 | System load averages over 1, 5, and 15 minutes |
| `memory_total_bytes` | uint64 | Total physical memory |
| `memory_available_bytes` | uint64 | Memory available to new processes without swapping |
| `memory_used_bytes` | uint64 | Memory in use |
| `swap_used_bytes` | uint64 | Swap in use |
| `net_rx_bytes`, `net_tx_bytes` | uint64 | Bytes received and sent on all network interfaces, cumulative since boot |
| `disk_read_bytes`, `disk_write_bytes` | uint64, nullable | Bytes read from and written to all disks, cumulative since boot; null where disk counters are unavailable, as in some containers |
| `disk_root_free_bytes`, `disk_tmp_free_bytes` | uint64, nullable | Free space available to unprivileged users on `/` and `/tmp`; null when the path cannot be read |

### GPU metrics

`gpu_metrics` holds one row per GPU per interval: utilization, memory used and
total, temperature, and power, read through NVML.
`TelemetryConfig.gpu_metrics_interval_secs` sets the interval; the default is
1 second, and `0` disables GPU sampling. Hosts without NVIDIA GPUs or NVML
leave the table empty. All rows from one sampling pass share a timestamp.

`hostname`, `gpu_uuid`, and `timestamp_us` together identify a row.
`gpu_metrics.hostname` matches `host_metrics.hostname` for the same host, but
the two samplers run on separate timers, so their timestamps do not line up.
Join the tables on `hostname` and a time window rather than on `timestamp_us`.

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_us` | int64 | Sample time, microseconds since the Unix epoch |
| `hostname` | string | Host name of the sampling host; matches `host_metrics.hostname` |
| `gpu_index` | uint32 | NVML device index as seen by the sampling process; it can differ from the CUDA device ordinal and is renumbered inside containers |
| `gpu_uuid` | string | NVML device UUID; the stable identifier for joins and de-duplication |
| `gpu_name` | string | NVML product name, such as `NVIDIA A10G` |
| `utilization_percent` | float64, nullable | Percent of NVML's recent sample window in which at least one kernel was running; null when NVML does not support it |
| `memory_used_bytes`, `memory_total_bytes` | uint64 | Device memory in use and total; a GPU whose memory NVML cannot read is skipped for that pass |
| `temperature_c` | uint32, nullable | GPU core temperature in degrees Celsius; null when unsupported |
| `power_watts` | float64, nullable | Board power draw in watts; null when unsupported |

## Troubleshooting

- If `state.query_engine_client` is `None`, telemetry bootstrap failed. Check
  the parent-process logs for `job sidecar telemetry bootstrap failed`.
- If a query is empty immediately after work runs, retry briefly; ingestion and
  collector flushes are asynchronous.
- If rows from one host are missing, inspect collector warnings and use the [Mesh Admin TUI](admin-tui) to inspect that host.
- If a SQL request fails, the raised HTTP error includes the DataFusion parse or
  planning detail returned by the query service.

## Related documentation

- [Metrics](metrics)
- [Observability overview](observability)
- [Actor profiling](profiling)
- [Monarch Dashboard](monarch-dashboard)
- [Mesh Admin TUI](admin-tui)
- [OpenTelemetry and Grafana](./generated/examples/otel_collector)
- [Jobs API](api/monarch.job)
