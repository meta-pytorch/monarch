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
path. Add `--workers N` to spread the fixture round-robin by dump across the
query root and `N` local worker collectors, each with its own store, so every
query fans out across `N + 1` collectors:

```bash
buck run @mode/opt fbcode//monarch/python/benches/telemetry_query_benchmark:telemetry_query_benchmark -- \
  --workers 3
```

Run the end-to-end sidecar integration backend with:

```bash
buck run @mode/opt fbcode//monarch/python/benches/telemetry_query_benchmark:telemetry_query_benchmark -- \
  --backend sidecar-http
```

The fixed fixture contains 20 dump rows, 80 stack-trace rows, 100,000 frame
rows, and 100,000 local-variable rows. Each run preloads rows before running
any query. The ingestion timestamp in `pyspy_dumps` varies by run and is
intentionally excluded from every query.

## Measurements

Each case reports:

| Column | Meaning |
|---|---|
| Warm | Median of 100 sequential queries |
| Concurrent | Median wall time of 20 rounds of `--concurrency` simultaneous queries |

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
| `fanout_floor` | Scan of a 20-row table; with `--workers`, the fixed cost of fanning out to every collector |
| `frame_count` | Full one-table scan and scalar aggregation |
| `filtered_count` | Selective one-table filter and aggregation |
| `group_by_filename` | Grouping, aggregation, and ordering |
| `four_table_join` | Locals joined to frames, traces, and dumps |
| `ordered_projection` | Sorted, bounded multi-column result serialization |

## Scope

The direct backend exercises DataFusion, the local telemetry actor and scanner,
and PyArrow result construction; with `--workers`, it also exercises collector
fan-out between processes. The sidecar backend additionally exercises
`ProcessJob`, HTTP, JSON, and Python-row materialization. Neither backend
measures concurrent ingestion, remote collector fan-out, peak memory, or
production query mixes.
