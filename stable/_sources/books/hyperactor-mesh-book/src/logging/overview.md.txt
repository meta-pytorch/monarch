# Logging

## Python Kickoff → Rust Actors

### High-Level Flow

1. `ProcMesh.from_host_mesh` submits the pure-Rust logging client and a Python log-level actor against the raw proc-creation Handle.
2. Native code waits for the procs, creates the forwarding actors, and applies the requested streaming and aggregation mode. The Python actor applies the initial root-logger level on each proc.
3. Proc readiness waits for both logging paths. If user setup is needed, its actor spawns afterward, and ordinary user actors wait for its reply as well.
4. Python can change the forwarding mode and Python log level afterward and flush logs before teardown.

### Proc Initialization

```python
pm = ProcMesh(raw, host_mesh, region, region, None)
log = pm._logging_manager.start(instance, raw, host_mesh.stream_logs)
level = pm._logging_manager.spawn_log_level_actor(pm, raw)
logged = _after([log, level], raw)
pm._proc_mesh = logged
setup_actor = SetupActor.startup_actor_from_setup_function(
    pm, logged, setup, run_startup_functions=_attach_controller_controller
)
if setup_actor is not None:
    done = setup_actor.setup.call()._as_handle()
    pm._proc_mesh = _after([logged, done], logged)
```

`logged` waits for both the Rust client and `_LogLevelSetter`, so the initial `INFO` level is applied before proc initialization completes.

Client bootstrap builds the root client's proc mesh with `ProcMesh._from_rust`, not `from_host_mesh`, and `_start_root_logging` gives it the same logging: a forwarding client and an initial `INFO` level. Its only proc is the driver process, so the level is set directly instead of through `_LogLevelSetter`, and `logging_option(level=...)` updates the driver's root logger.

The setup actor's init message reserves `logged`, not the completion that waits for its own reply. That avoids a dependency cycle. Waiting for `logged` before the setup reply also makes a logging failure the reported initialization error.

### What Logging Installation Creates

`LoggingMeshClient.spawn(instance, raw, stream_to_client=..., aggregate_window_sec=...)` starts native work immediately and returns a Handle. Dropping or timing out an observer does not cancel installation.

After raw proc creation succeeds, it creates the pure-Rust forwarding stack:

- one `LogClientActor` in the controller process, to aggregate output and coordinate flush barriers;
- a `LogForwardActor` on each proc if `MESH_ENABLE_LOG_FORWARDING` is enabled.

It then applies the initial streaming and aggregation mode. Creation or mode errors fail the Handle. `LoggingManager._logging_mesh_client` polls that Handle: it exposes a client only after installation succeeds, and returns `None` while pending or after failure.

Separately, Python spawns `_LogLevelSetter` on each ordinary Python proc. Its constructor applies `logging.INFO`, and `pm.logging_option(...)` broadcasts later level changes to it. Neither Rust forwarding actor touches a Python runtime.

In notebooks, `pm.logging_option(...)` also registers the cell-end flusher and enables FD capture. `ProcMesh.stop()` awaits the async flush before native stop.

---

### Control Surface from Python to Rust

### `set_mode(stream_to_client, aggregate_window_sec)`

```rust
// logging.rs (inside LoggingMeshClient::set_mode)
if let Some(ref forwarder_mesh) = self.forwarder_mesh {
    forwarder_mesh.cast(
        instance.deref(),
        LogForwardMessage::SetMode { stream_to_client },
    )?;
}

self.client_actor.post(
    instance.deref(),
    LogClientMessage::SetAggregate { aggregate_window_sec },
);
```

- Forwarders, when present, decide whether to stream back to the client.
- The client actor records the aggregation window.
- Python logging level is not set here. `LoggingManager.logging_option(...)` broadcasts it to `_LogLevelSetter` on the Python side.

---

### Sync Flush Barrier (`flush()`)

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

        client_actor.post(
            cx,
            LogClientMessage::StartSyncFlush {
                expected_procs: forwarder_mesh.region().num_ranks(),
                reply: reply_tx.bind(),
                version: version_tx.bind(),
            },
        );

        let version = version_rx.recv().await?;
        forwarder_mesh.cast(cx, LogForwardMessage::ForceSyncFlush { version })?;
        reply_rx.recv().await?;
        Ok(())
    }
}
```

The client allocates a version, the forwarders drain logs through that version, and the client replies after every forwarder has reached the barrier. Python's `LoggingManager.flush()` wraps this with a short timeout during teardown and cell ends.

---

### Teardown (Client Side)

```rust
impl Drop for LoggingMeshClient {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self
                .client_actor
                .drain_and_stop("logging client shutdown");
        }));
    }
}
```

This drains and stops the coordinator when the Python object goes away. Separately, `ProcMesh.stop()` awaits the async flush before it tears down the mesh.
