# Python Control Surface

## What It Exposes

The Python API installs logging for a `ProcMesh`, streams remote stdout and stderr back to the notebook or REPL, adjusts aggregation and Python verbosity, and performs a versioned sync flush before teardown.

There is exactly one `LogClientActor`, and it runs inside the Python driver process. Python calls fan out to per-proc forwarders and back.

## API Surface

### `LoggingMeshClient.spawn(instance, raw, *, stream_to_client, aggregate_window_sec) -> Handle[LoggingMeshClient]`

Starts immediately, waits for the raw proc-creation Handle, and then creates and configures:

- a single `LogClientActor` in the Python driver process;
- per-proc `LogForwardActor`s when forwarding is enabled.

Both are Rust actors. Python log levels are handled separately by the per-proc `_LogLevelSetter` actor that `LoggingManager` installs on an ordinary Python proc mesh.

### `LoggingMeshClient.set_mode(instance, stream_to_client: bool, aggregate_window_sec: Optional[int]) -> None`

- Turns streaming on or off.
- Sets the aggregation window only when streaming is enabled.

If `aggregate_window_sec` is set while `stream_to_client` is false, `set_mode` raises a runtime error.

### `LoggingMeshClient.flush(instance) -> Handle[None]`

Performs a versioned sync flush across all forwarders. The returned Handle completes after all logs through the chosen version have been delivered. Dropping the Handle does not cancel the flush.

See [Client actor → Barrier protocol](client.md#barrier-protocol-sync-flush) and [Forwarder internals](forwarder.md).

## Minimal Example

```python
from monarch._rust_bindings.monarch_hyperactor.logging import LoggingMeshClient

client = await LoggingMeshClient.spawn(
    instance,
    raw_proc_handle,
    stream_to_client=True,
    aggregate_window_sec=3,
)

# Run work that produces stdout or stderr in remote procs.

await client.flush(instance)
```

The Handle completes only after Rust actor creation and initial forwarding-mode submission succeed. At the `ProcMesh` layer, readiness also waits for `_LogLevelSetter` to apply the initial Python level. Later `pm.logging_option(...)` calls send stream and aggregation changes to `LoggingMeshClient` and broadcast Python level changes to `_LogLevelSetter`.

## Related

- Client actor — aggregation, similarity bucketing, barriers
- Forwarder internals — in-band `LogMessage::{Log, Flush}`
- Stream forwarders — FD capture, `tee`, local files
