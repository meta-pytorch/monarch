# Logging

## Python kickoff → Rust actors

### High-level flow

1. Python `ProcMesh` boots and attaches a `LoggingManager`.
2. On mesh init, Python asks Rust to spawn a logging stack via `LoggingMeshClient.spawn(...)`.
3. Rust spawns:
    * a `LogClientActor` in the *client process*
    * a `LogForwardActor` mesh across all procs in the `ProcMesh`
    * a `LoggerRuntimeActor` mesh (per-proc Python logging integration).
4. Python configures behavior with `set_mode(...)` and can sync-flush logs via `flush(...)`.

### Python entry point: `ProcMesh` wires up logging

When a `ProcMesh` is constructed from a `HostMesh`, it defers init then calls into the logging manager:
```python
# proc_mesh.py
pm = ProcMesh(hy_proc_mesh, host_mesh, region, region, None)

async def task(
    pm: "ProcMesh",
    hy_proc_mesh_task: "Shared[HyProcMesh]",
    setup_actor: Optional["SetupActor"],
    stream_log_to_client: bool,
) → HyProcMesh:
    hy_proc_mesh = await hy_proc_mesh_task

    await pm._logging_manager.init(hy_proc_mesh, stream_log_to_client)
    ...
```

`LoggingManager.init(...)` spawns the client:
```python
# actor/logging.py
self._logging_mesh_client = await LoggingMeshClient.spawn(instance, proc_mesh=proc_mesh)
self._logging_mesh_client.set_mode(
  instance,
  stream_to_client=stream_to_client,
  aggregate_window_sec=3 if stream_to_client else None,
  level=logging.INFO,
)
```
Two public levers from Python:
- `await pm.logging_option(stream_to_client=True, aggregate_window_sec=3, level=logging.INFO)`
    - forward to `LoggingMeshClient.set_mode(...)` and (in notebooks), registers a cell-end flusher and enables FD capture so OS-level stdout/stderr show up.
- `pm.stop()` (or `async with ProcMesh`) calls a blocking flush before tearing down the mesh.

---

### Crossing the FFI: what `LoggingMeshClient.spawn(...)` does

Rust sets up the three actor pieces the Python side depends on:
- `LogClientActor` (in the client proc): orchestrates flushes and aggregates client-side buffering.
- `LogForwardActor` mesh (on every proc): receives stdout/stderr, forwards to the client (optionally aggregating).
- `LoggerRuntimeActor` mesh (on every proc): controls the Python logging subsystem (log levels, handlers, etc).

#### Schematic (for orientation)
```rust
// One local client actor, plus two per-proc meshes:
let client_actor = instance.proc().spawn("log_client", ()).await;
let client_actor_ref = client_actor.bind();
let forwarder_mesh = proc_mesh.spawn(cx_instance, "log_forwarder", &client_actor_ref).await;
let logger_mesh = proc_mesh.spawn(cx_instance, "logger", &()).await;
```

#### Real code (trimmed to essentials)

```rust
// monarch_hyperactor logging.rs

#[pymethods]
impl LoggingMeshClient {
    #[staticmethod]
    fn spawn(instance: &PyInstance, proc_mesh: &PyProcMesh) -> PyResult<PyPythonTask> {
        let proc_mesh = proc_mesh.mesh_ref()?;
        let instance = instance.clone();

        PyPythonTask::new(async move {
            // 1) Client-side coordinator actor (in the client process)
            let client_actor: ActorHandle<LogClientActor> =
                instance_dispatch!(instance, async move |cx_instance| {
                    cx_instance
                        .proc()
                        .spawn(&Name::new("log_client").to_string(), LogClientActor::default())
                        .await
                })?;
            let client_actor_ref = client_actor.bind();

            // 2) Per-proc forwarders (wired back to the client actor)
            // Only spawned if MESH_ENABLE_LOG_FORWARDING is true
            let forwarder_mesh = if hyperactor_config::global::get(MESH_ENABLE_LOG_FORWARDING) {
                Some(instance_dispatch!(instance, async |cx_instance| {
                    proc_mesh
                        .spawn(cx_instance, "log_forwarder", &client_actor_ref)
                        .await
                }).map_err(anyhow::Error::from)?)
            } else {
                None
            };

            // 3) Per-proc Python logging integration (always spawned)
            let logger_mesh = instance_dispatch!(instance, async |cx_instance| {
                proc_mesh.spawn(cx_instance, "logger", &()).await
            }).map_err(anyhow::Error::from)?;

            Ok(Self { forwarder_mesh, logger_mesh, client_actor })
        })
    }
}
```
#### What to notice:
- `PyPythonTask::new(async move {...})`: bridges the Python call into Rust async, returning a task handle back to Python.
- `instance_dispatch!`: executes the spawn on the correct actor instance context.
- LogClientActor is spawned with `LogClientActor::default()`, not `()`.
- Forwarder mesh is conditionally spawned only if `MESH_ENABLE_LOG_FORWARDING` is true; it's `Option<ActorMesh<LogForwardActor>>`.
- Three things get created: one `LogClientActor` (client proc), **N** `LogForwardActor` (per remote proc, if enabled), **N** `LoggerRuntimeActor` (per remote proc).

---

### Control surface from Python → Rust

### `set_mode(stream_to_client, aggregate_window_sec_level)`
```rust
// logging.rs (inside LoggingMeshClient::set_mode)
if let Some(ref forwarder_mesh) = self.forwarder_mesh {
    forwarder_mesh
        .cast(cx, LogForwardMessage::SetMode { stream_to_client })?;
}

self.logger_mesh
    .cast(cx, LoggerRuntimeMessage::SetLogging { level })?;

self.client_actor
    .send(LogClientMessage::SetAggregate { aggregate_window_sec })?;
```
- Forwarders (if they exist) decide whether to stream back to the client
- LoggerRuntime sets Python logging level inside each remote process.
- Client actor records/updates aggregation windowing behavior.

> From Python you call await `pm.logging_option(...)`; under the hood that calls `set_mode(...)` and (if in a notebook) registers a cell-end flusher + enables FD capture so OS-level writes show up.

---

### Sync flush barrier (`flush()`)

```rust
// logging.rs
impl LoggingMeshClient {
    async fn flush_internal(
        cx: &impl context::Actor,
        client_actor: ActorHandle<LogClientActor>,
        forwarder_mesh: ActorMeshRef<LogForwardActor>,
    ) -> Result<(), anyhow::Error> {
        let (reply_tx, reply_rx) = cx.instance().open_once_port::<()>();
        let (version_tx, version_rx) = cx.instance().open_once_port::<u64>();

        // 1) Ask the client actor to initiate a versioned sync flush.
        client_actor.send(LogClientMessage::StartSyncFlush {
            expected_procs: forwarder_mesh.region().num_ranks(),
            reply: reply_tx.bind(),
            version: version_tx.bind(),
        })?;

        // 2) Wait for the chosen version.
        let version = version_rx.recv().await?;

        // 3) Tell all forwarders to flush up to that version.
        forwarder_mesh.cast(cx, LogForwardMessage::ForceSyncFlush { version })?;

        // 4) Wait for the all-clear from the client actor.
        reply_rx.recv().await?;
        Ok(())
    }
}
```
**Flow in one breath**: client allocates a version, forwarders drain/logs up to that version, client receives confirmations and signals the barrier is complete. Python's `LoggingManager.flush()` wraps this with a short timeout during teardown and cell ends.

---

### Teardown (client-side)

```rust
impl Drop for LoggingMeshClient {
    fn drop(&mut self) {
        if let Err(e) = self.client_actor.drain_and_stop() {
            // During shutdown, channels may already be closed.
            tracing::debug!("error draining logging client actor during shutdown: {}", e);
        }
    }
}
```
This ensures the coordinator actor is drained/stopped when the Python object goes away. Separately, `ProcMesh.stop()` also does a blocking flush before it tears down the mesh.
