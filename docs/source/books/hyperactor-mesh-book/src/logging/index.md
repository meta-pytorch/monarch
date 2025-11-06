# Logging

Monarch v1's logging subsystem streams `stdout`/`stderr` from remote procs back to the client and lets Python control log delivery and levels. This section is written **top-down**: start with the big picture, then dive into each component.

## What's in this section

- **[Overview](overview.md)** — Python kickoff → Rust actors: how `ProcMesh` boots logging, what `LoggingManager` does, and what `LoggingMeshClient.spawn(...)` creates.
- **[Forwarder internals](forwarder.md)** — `LogForwardActor`, `BOOTSTRAP_LOG_CHANNEL`, streaming vs. silent mode, and the versioned sync-flush path.
- **[Stream forwarders](stream-forwarders.md)** — `StreamFwder`, `tee`, `FileAppender`, `RotatingLineBuffer`; how raw bytes become lines sent to forwarders/files.
- **[Client actor](client.md)** — `LogClientActor` aggregation windows, similarity bucketing, flush barriers, and teardown.
- **[Python control surface](python.md)** — `logging_option(...)`, `flush()`, IPython cell-end flushers, FD capture.
- **[Config & env](config.md)** — Tunables like `HYPERACTOR_READ_LOG_BUFFER`, `HYPERACTOR_FORCE_FILE_LOG`, `HYPERACTOR_PREFIX_WITH_RANK`, defaults.
- **[Ordering](ordering.md)** — what is guaranteed (and what isn't)
- **[Teardown](teardown.md)** — barrier-before-stop, EOF handling, drop paths
- **[File aggregation](file-aggregation.md)** — per-proc files on bootstrap hosts

## Quick mental model

- **Three moving parts**: a client-side coordinator (`LogClientActor`) and two per-proc meshes (`LogForwardActor`, `LoggerRuntimeActor`).
- **Two planes**: raw FD streams (stdout/stderr) → forwarders; and Python logging (levels/handlers) → logger runtime.
- **Barriers**: versioned sync flush guarantees all logs up to a point have been delivered.

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
