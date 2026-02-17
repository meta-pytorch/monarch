# Python control surface

## What it exposes
A small, explicit API to start logging for a `ProcMesh`, stream remote `stdout/stderr` back to your notebook/REPL, tune aggregation & verbosity, and perform a **versioned sync flush** before teardown.

> **Placement:** There is exactly one `LogClientActor`, and it runs inside the Python/driver process (your notebook/REPL). Python calls fan out to per-proc forwarders and back.

## API surface

### `LoggingMeshClient.spawn(instance, proc_mesh) -> PyPythonTask`
Spawns:
- a single `LogClientActor` (in the Python/driver process),
- per-proc `LogForwardActor`s (in remote procs),
- per-proc `LoggerRuntimeActor`s (Python logging runtime).

### `LoggingMeshClient.set_mode(instance, stream_to_client: bool, aggregate_window_sec: Optional[int], level: int) -> None`
- Turns streaming on/off (`stream_to_client`).
- Sets aggregation window (seconds) **only if** streaming is enabled.
- Sets Python logging level in the per-proc `LoggerRuntimeActor`.

> **Constraint:** if `aggregate_window_sec` is `Some` while `stream_to_client` is `False`, `set_mode` raises a runtime error (see source).

### `LoggingMeshClient.flush(instance) -> PyPythonTask`
Performs a **versioned sync flush** across all forwarders so the caller deterministically waits until "all logs up to now" are delivered.
See: [Client actor → Barrier protocol](client.md#barrier-protocol-sync-flush) and [Forwarder internals](forwarder.md).

## Minimal example

```python
from monarch._rust_bindings.monarch_hyperactor.logging import LoggingMeshClient
import logging

client = await LoggingMeshClient.spawn(instance, proc_mesh)  # one client actor in driver
client.set_mode(instance, stream_to_client=True, aggregate_window_sec=3, level=logging.INFO)

# … run workload producing stdout/stderr in remote procs …

await client.flush(instance)  # barrier: all logs observed up to this point
```

## Related

  - Client actor — aggregation, similarity bucketing, barriers
  - Forwarder internals — in-band `LogMessage::{Log, Flush}`
  - Stream forwarders — FD capture, `tee`, local files
