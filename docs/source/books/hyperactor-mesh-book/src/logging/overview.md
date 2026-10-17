# Logging

## Python Kickoff → Native Actors

### High-Level Flow

1. `ProcMesh.from_host_mesh` submits logging installation against the raw proc-creation Handle.
2. Native code waits for the procs, creates the logging actors and applies the requested mode.
3. If user setup is needed, its actor spawns after logging succeeds. Ordinary user actors wait for the setup reply as well.
4. Python can change the mode afterward and flush logs before teardown.

### Proc Initialization

```python
pm = ProcMesh(raw, host_mesh, region, region, None)
log = pm._logging_manager.start(instance, raw, host_mesh.stream_logs)
logged = _after([log], raw)
pm._proc_mesh = logged
setup_actor = SetupActor.startup_actor_from_setup_function(
    pm, logged, setup, run_startup_functions=_attach_controller_controller
)
if setup_actor is not None:
    done = setup_actor.setup.call()._as_handle()
    pm._proc_mesh = _after([logged, done], logged)
```

The setup actor's init message reserves `logged`, not the completion that waits for its own reply. That avoids a dependency cycle. Waiting for `logged` before the setup reply also makes a logging failure the reported initialization error.

### What Logging Installation Creates

`LoggingMeshClient.spawn(instance, raw, stream_to_client=..., aggregate_window_sec=..., level=...)` starts native work immediately and returns a Handle. Dropping or timing out an observer does not cancel installation.

After raw proc creation succeeds, it creates:

- one `LogClientActor` in the controller process, to aggregate output and coordinate flush barriers;
- a `LogForwardActor` on each proc if `MESH_ENABLE_LOG_FORWARDING` is enabled;
- a `LoggerRuntimeActor` on each proc to configure Python logging.

It then calls the existing native `set_mode` implementation. Creation or mode errors fail the Handle. `LoggingManager._logging_mesh_client` polls that Handle: it exposes a client only after installation succeeds, and returns `None` while pending or after failure.

Python's `pm.logging_option(...)` changes the installed client's mode. In notebooks it also registers the cell-end flusher and enables FD capture. `ProcMesh.stop()` awaits the async flush before native stop.

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
This ensures the coordinator actor is drained/stopped when the Python object goes away. Separately, `ProcMesh.stop()` also awaits the async flush before it tears down the mesh.
