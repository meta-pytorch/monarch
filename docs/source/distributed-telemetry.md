# Distributed Telemetry

Monarch distributed telemetry collects actor-system events on each host and
makes them queryable as one logical database. It uses
[Apache DataFusion](https://github.com/apache/datafusion), an extensible SQL
query engine built on [Apache Arrow](https://arrow.apache.org/). Distributed
telemetry is the query layer behind the [Monarch Dashboard](monarch-dashboard)
and complements the live [Mesh Admin TUI](admin-tui).

Start with the [observability overview](observability) if you are choosing a
diagnostic surface or configuring a job for the first time.

## Quick start

Enable telemetry before obtaining job state:

```python
from monarch.job import ProcessJob, TelemetryConfig

job = ProcessJob({"workers": 2}).enable_telemetry(
    TelemetryConfig(retention_secs=600)
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
| `meshes` | Mesh creation, shape, class, name, and parent view |
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

## Architecture

Each producer writes framed Arrow IPC batches to a host-local Unix socket. A
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

`TelemetryConfig.retention_secs` applies to `sent_messages`, `messages`, and
`message_status_events`. The default is 600 seconds. Set it to `0` to disable
automatic retention. Retention runs every 30 seconds, so expired rows can
remain visible until the next sweep. Other core tables have no automatic
retention window.

`TelemetryConfig.snapshot_interval_secs` controls periodic Mesh Admin topology
snapshots. The default is 30 seconds; set it to `0` to disable them. Snapshots
and event telemetry describe different views of the same job:

- Event tables record changes such as actor creation, status transitions, and
  message delivery.
- Snapshot tables capture the administrative topology and actor diagnostics at
  a point in time.

## Troubleshooting

- If `state.query_engine_client` is `None`, telemetry bootstrap failed. Check
  the parent-process logs for `job sidecar telemetry bootstrap failed`.
- If a query is empty immediately after work runs, retry briefly; ingestion and
  collector flushes are asynchronous.
- If rows from one host are missing, inspect collector warnings and use the
  [Mesh Admin TUI diagnostics](admin-tui) to check that host.
- If a SQL request fails, the raised HTTP error includes the DataFusion parse or
  planning detail returned by the query service.

## Related documentation

- [Observability overview](observability)
- [Monarch Dashboard](monarch-dashboard)
- [Mesh Admin TUI](admin-tui)
- [OpenTelemetry and Grafana](./generated/examples/otel_collector)
- [Jobs API](api/monarch.job)
