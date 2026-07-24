# Telemetry Query Benchmark

This benchmark isolates telemetry SQL performance from concurrent writes to its
target tables. It synchronously preloads deterministic queried columns before
issuing the first SQL query. Query traffic does not modify the four `pyspy_*`
tables. The benchmark verifies every query result.

`benchmark.py` defines the query matrix, execution protocol, and reporting.
`pyspy_fixture.py` contains the workload-specific payload generation and
expected rows. `query_backend.py` owns the direct and sidecar execution
boundaries.

Run the representative shape with:

```bash
buck run @mode/opt fbcode//monarch/python/benches/telemetry_query_benchmark:telemetry_query_benchmark
```

The default `query-engine` backend measures the direct DataFusion-to-PyArrow
path. Run the end-to-end sidecar integration backend with:

```bash
buck run @mode/opt fbcode//monarch/python/benches/telemetry_query_benchmark:telemetry_query_benchmark -- \
  --backend sidecar-http
```

The fixed fixture contains 20 dump rows, 80 stack-trace rows, 100,000 frame
rows, and 100,000 local-variable rows. Each run preloads rows before running
each case 100 times. The ingestion timestamp in `pyspy_dumps` varies by run and
is intentionally excluded from every query.

## Backends

| Backend | Timed boundary |
|---|---|
| `query-engine` | Direct `QueryEngine.query()` through DataFusion and PyArrow table creation |
| `sidecar-http` | Loopback HTTP, `QueryEngine`, JSON, and Python-row materialization |

Both backends use the same fixture, SQL, and validators, but their actor and
process topology differs. The direct backend materializes Python rows for
validation only after the timed query call. Backend results have separate
baselines and must not be compared to each other.

## Query Matrix

| Case | Work measured |
|---|---|
| `frame_count` | Full one-table scan and scalar aggregation |
| `filtered_count` | Selective one-table filter and aggregation |
| `group_by_filename` | Grouping, aggregation, and ordering |
| `four_table_join` | Locals joined to frames, traces, and dumps |
| `ordered_projection` | Sorted, bounded multi-column result serialization |

## Scope

The direct backend exercises DataFusion, the local telemetry actor and scanner,
and PyArrow result construction. The sidecar backend additionally exercises
`ProcessJob`, HTTP, JSON, and Python-row materialization. Neither backend
measures concurrent ingestion, remote collector fan-out, peak memory, or
production query mixes.
