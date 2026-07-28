# Observability

Monarch provides one observability setup with three complementary job-local
surfaces: distributed telemetry for SQL analysis, the Monarch Dashboard for
visual monitoring, and the Mesh Admin TUI for live diagnostics. Enable
telemetry on a job to start all three data paths consistently. OpenTelemetry
provides a separate export path for external backends.

## Choose a surface

| Use case | Surface | What it provides |
|----------|---------|------------------|
| Query actor, mesh, message, and trace history | [Distributed telemetry](distributed-telemetry) | DataFusion SQL across host-local collectors |
| Monitor topology, status, failures, and traffic in a browser | [Monarch Dashboard](monarch-dashboard) | Live summaries, hierarchy views, and a job DAG |
| Diagnose a running mesh from a terminal | [Mesh Admin TUI](admin-tui) | Live topology, health checks, and py-spy stack traces |
| Export metrics and logs to an external backend | [OpenTelemetry and Grafana](./generated/examples/otel_collector) | OTLP export to systems such as Prometheus and Loki |

The dashboard reads distributed telemetry. The TUI reads the Mesh Admin API.
Periodic mesh-admin snapshots also flow into telemetry so the dashboard can
show the live administrative topology.

Distributed telemetry is a job-scoped, in-memory query system. OpenTelemetry
export is the external aggregation path for metrics and logs. You can use both
on the same job.

## Enable observability

Configure observability before calling `state()`:

```python
from monarch.job import ProcessJob, TelemetryConfig

job = ProcessJob({"workers": 2}).enable_telemetry(
    TelemetryConfig(
        retention_secs=600,
        include_dashboard=True,
        dashboard_port=8265,
        snapshot_interval_secs=30,
    )
)
state = job.state(cached_path=None)
```

`enable_telemetry()` also enables mesh admin and periodic introspection
snapshots. Adapt the example to your existing `LocalJob`, `ProcessJob`,
`SlurmJob`, or `KubernetesJob` rather than creating a second job.

The returned state exposes each surface:

```python
print(state.dashboard_url)
print(state.admin_url)

client = state.query_engine_client
if client is None:
    raise RuntimeError("telemetry did not start")

result = client.query(
    "SELECT new_status, COUNT(*) AS count "
    "FROM actor_status_events GROUP BY new_status"
)
print(result["rows"])
```

Use `state.dashboard_url` in a browser. Pass `state.admin_url` to
`monarch-tui --addr`.

## Data flow

```text
actor and proc events ──> host-local collectors ──> distributed query engine
                                                        ├─> SQL query client
                                                        └─> Monarch Dashboard

live mesh ──> Mesh Admin API ──────────────────────────────> Mesh Admin TUI
     └──────> periodic snapshots ──> distributed telemetry ─> Dashboard DAG
```

Telemetry collection and distributed query fan-out are best-effort. A failed
or slow worker collector can reduce query coverage without failing the job.
Telemetry tables are in memory and belong to the current job allocation; they
are not a durable event archive.

## Configuration

| `TelemetryConfig` field | Default | Effect |
|-------------------------|---------|--------|
| `retention_secs` | `600` | Retention window for message tables; `0` disables retention |
| `include_dashboard` | `False` | Advertise the browser dashboard |
| `dashboard_port` | `8265` | Preferred dashboard port; use `0` for an ephemeral port |
| `snapshot_interval_secs` | `30` | Mesh-introspection snapshot interval; `0` disables periodic snapshots |

For an admin server without distributed telemetry, call `job.enable_admin()`.
That supports the TUI, but it does not provide the dashboard or SQL history.

## Next steps

- [Query distributed telemetry](distributed-telemetry)
- [Use the Monarch Dashboard](monarch-dashboard)
- [Diagnose a mesh with the Admin TUI](admin-tui)
- [Export metrics and logs with OpenTelemetry](./generated/examples/otel_collector)

```{toctree}
:maxdepth: 1
:hidden:
distributed-telemetry
admin-tui
monarch-dashboard
```
