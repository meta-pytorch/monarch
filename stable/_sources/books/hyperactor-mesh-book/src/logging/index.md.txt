# Logging

Monarch v1's logging subsystem streams `stdout`/`stderr` from remote procs back to the client and lets Python control log delivery and levels. This section is written **top-down**: start with the big picture, then dive into each component.

## What's in this section

- **[Overview](overview.md)** — Python kickoff → Rust actors: how `ProcMesh` boots logging, what `LoggingManager` does, what `LoggingMeshClient.spawn(...)` creates, and how the Python log level is set from the Python side.
- **[Forwarder internals](forwarder.md)** — `LogForwardActor`, `BOOTSTRAP_LOG_CHANNEL`, streaming vs. silent mode, and the versioned sync-flush path.
- **[Stream forwarders](stream-forwarders.md)** — `StreamFwder`, `tee`, `FileAppender`, `RotatingLineBuffer`; how raw bytes become lines sent to forwarders/files.
- **[Client actor](client.md)** — `LogClientActor` aggregation windows, similarity bucketing, flush barriers, and teardown.
- **[Python control surface](python.md)** — `logging_option(...)`, `flush()`, IPython cell-end flushers, FD capture.
- **[Config & env](config.md)** — Tunables like `HYPERACTOR_READ_LOG_BUFFER`, `HYPERACTOR_FORCE_FILE_LOG`, `HYPERACTOR_PREFIX_WITH_RANK`, defaults.
- **[Ordering](ordering.md)** — what is guaranteed (and what isn't)
- **[Teardown](teardown.md)** — barrier-before-stop, EOF handling, drop paths
- **[File aggregation](file-aggregation.md)** — per-proc files on bootstrap hosts

## Quick mental model

- **Moving parts**: a client-side coordinator (`LogClientActor`) and one per-proc mesh (`LogForwardActor`, optional). The per-proc Python log level is set on the Python side by a `_LogLevelSetter` actor, keeping the Rust forwarding stack Python-free.
- **Two planes**: raw FD streams (stdout/stderr) → forwarders (if enabled); and Python logging (levels) → the Python-side log-level actor.
- **Barriers**: versioned sync flush guarantees all logs up to a point have been delivered.
- **Conditional forwarding**: The `LogForwardActor` mesh is only spawned if `MESH_ENABLE_LOG_FORWARDING` is true; otherwise logs stay local.

## Quickstart (Python)

```python
pm = host_mesh.spawn_procs(per_host={"gpus": 1})
await pm.logging_option(
    stream_to_client=True,
    aggregate_window_sec=3,
    level=logging.INFO,
)
# …run workload; logs stream back…

await pm.stop()  # does a blocking flush before teardown
```
