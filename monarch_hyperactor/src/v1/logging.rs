/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::ops::Deref;

use hyperactor::ActorHandle;
use hyperactor::context;
use hyperactor_mesh::logging::LogClientActor;
use hyperactor_mesh::logging::LogClientMessage;
use hyperactor_mesh::logging::LogForwardActor;
use hyperactor_mesh::logging::LogForwardMessage;
use hyperactor_mesh::v1::ActorMesh;
use hyperactor_mesh::v1::Name;
use hyperactor_mesh::v1::actor_mesh::ActorMeshRef;
use ndslice::View;
use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::context::PyInstance;
use crate::instance_dispatch;
use crate::logging::LoggerRuntimeActor;
use crate::logging::LoggerRuntimeMessage;
use crate::pytokio::PyPythonTask;
use crate::v1::proc_mesh::PyProcMesh;

#[pyclass(
    frozen,
    name = "LoggingMeshClient",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.logging"
)]
pub struct LoggingMeshClient {
    // Per-proc LogForwardActor mesh (optional). When enabled, each
    // remote proc forwards its stdout/stderr back to the client. This
    // actor does not interact with the embedded Python runtime.
    forwarder_mesh: Option<ActorMesh<LogForwardActor>>,

    // Per-proc LoggerRuntimeActor mesh. Runs on every proc in the
    // mesh and drives that proc's Python logging configuration (log
    // level, handlers, etc.). If the proc isn't running embedded
    // Python, this is effectively a no-op.
    logger_mesh: ActorMesh<LoggerRuntimeActor>,

    // Client-side LogClientActor. Lives in the client process;
    // receives forwarded output, aggregates and buffers it, and
    // coordinates sync flush barriers.
    client_actor: ActorHandle<LogClientActor>,
}

impl LoggingMeshClient {
    async fn flush_internal(
        cx: &impl context::Actor,
        client_actor: ActorHandle<LogClientActor>,
        forwarder_mesh: ActorMeshRef<LogForwardActor>,
    ) -> Result<(), anyhow::Error> {
        let (reply_tx, reply_rx) = cx.instance().open_once_port::<()>();
        let (version_tx, version_rx) = cx.instance().open_once_port::<u64>();

        // First initialize a sync flush.
        client_actor.send(LogClientMessage::StartSyncFlush {
            expected_procs: forwarder_mesh.region().num_ranks(),
            reply: reply_tx.bind(),
            version: version_tx.bind(),
        })?;

        let version = version_rx.recv().await?;

        // Then ask all the flushers to ask the log forwarders to sync flush
        forwarder_mesh.cast(cx, LogForwardMessage::ForceSyncFlush { version })?;

        // Finally the forwarder will send sync point back to the client, flush, and return.
        reply_rx.recv().await?;

        Ok(())
    }
}

#[pymethods]
impl LoggingMeshClient {
    /// Initialize logging for a `ProcMesh` and return a
    /// `LoggingMeshClient`.
    ///
    /// This wires up three pieces of logging infrastructure:
    ///
    /// 1. A single `LogClientActor` in the *client* process. This
    ///    actor receives forwarded stdout/stderr, buffers and
    ///    aggregates it, and coordinates sync flush barriers.
    ///
    /// 2. (Optional) A `LogForwardActor` on every remote proc in the
    ///    mesh. These forwarders read that proc's stdout/stderr and
    ///    stream it back to the client. We only spawn this mesh if
    ///    `MESH_ENABLE_LOG_FORWARDING` was `true` in the config. If
    ///    forwarding is disabled at startup, we do not spawn these
    ///    actors and `forwarder_mesh` will be `None`.
    ///
    /// 3. A `LoggerRuntimeActor` on every remote proc in the mesh.
    ///    This actor controls the Python logging runtime (log level,
    ///    handlers, etc.) in that process. This is always spawned,
    ///    even if log forwarding is disabled.
    ///
    /// The returned `LoggingMeshClient` holds handles to those
    /// actors. Later, `set_mode(...)` can adjust per-proc log level
    /// and (if forwarding was enabled) toggle whether remote output
    /// is actually streamed back to the client. If forwarding was
    /// disabled by config, requests to enable streaming will fail.
    #[staticmethod]
    fn spawn(instance: &PyInstance, proc_mesh: &PyProcMesh) -> PyResult<PyPythonTask> {
        let proc_mesh = proc_mesh.mesh_ref()?;
        let instance = instance.clone();

        PyPythonTask::new(async move {
            // 1. Spawn the client-side coordinator actor (lives in
            // the caller's process).
            let client_actor: ActorHandle<LogClientActor> =
                instance_dispatch!(instance, async move |cx_instance| {
                    cx_instance
                        .proc()
                        .spawn(&Name::new("log_client").to_string(), ())
                        .await
                })?;
            let client_actor_ref = client_actor.bind();

            // Read config to decide if we stand up per-proc
            // stdout/stderr forwarding.
            let forwarding_enabled = hyperactor::config::global::get(
                hyperactor_mesh::bootstrap::MESH_ENABLE_LOG_FORWARDING,
            );

            // 2. Optionally spawn per-proc `LogForwardActor` mesh
            // (stdout/stderr forwarders).
            let forwarder_mesh = if forwarding_enabled {
                // Spawn a `LogFwdActor` on every proc.
                let mesh = instance_dispatch!(instance, async |cx_instance| {
                    proc_mesh
                        .spawn(cx_instance, "log_forwarder", &client_actor_ref)
                        .await
                })
                .map_err(anyhow::Error::from)?;

                Some(mesh)
            } else {
                None
            };

            // 3. Always spawn a `LoggerRuntimeActor` on every proc.
            let logger_mesh = instance_dispatch!(instance, async |cx_instance| {
                proc_mesh.spawn(cx_instance, "logger", &()).await
            })
            .map_err(anyhow::Error::from)?;

            Ok(Self {
                forwarder_mesh,
                logger_mesh,
                client_actor,
            })
        })
    }

    /// Update logging behavior for this mesh.
    ///
    /// `stream_to_client` controls whether remote procs actively
    /// stream their stdout/stderr back to the client process.
    ///
    /// - If log forwarding was enabled at startup, `forwarder_mesh`
    ///   is `Some` and we propagate this flag to every per-proc
    ///   `LogForwardActor`.
    /// - If log forwarding was disabled at startup, `forwarder_mesh`
    ///   is `None`.
    ///   In that case:
    ///     * requesting `stream_to_client = false` is a no-op
    ///       (accepted),
    ///     * requesting `stream_to_client = true` is rejected,
    ///       because we did not spawn forwarders and we don't
    ///       dynamically create them later.
    ///
    /// `aggregate_window_sec` configures how the client-side
    /// `LogClientActor` batches forwarded output. It is only
    /// meaningful when streaming is enabled. Calling this with
    /// `Some(..)` while `stream_to_client == false` is invalid and
    /// returns an error.
    ///
    /// `level` is the desired Python logging level. We always
    /// broadcast this to the per-proc `LoggerRuntimeActor` mesh so
    /// each remote process can update its own Python logger
    /// configuration, regardless of whether stdout/stderr forwarding
    /// is active.
    fn set_mode(
        &self,
        instance: &PyInstance,
        stream_to_client: bool,
        aggregate_window_sec: Option<u64>,
        level: u8,
    ) -> PyResult<()> {
        // We can't ask for an aggregation window if we're not
        // streaming.
        if aggregate_window_sec.is_some() && !stream_to_client {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "cannot set aggregate window without streaming to client".to_string(),
            ));
        }

        // Handle the forwarder side (stdout/stderr streaming back to
        // client).
        match (&self.forwarder_mesh, stream_to_client) {
            // Forwarders exits (config enabled at startup). We can
            // toggle live.
            (Some(fwd_mesh), _) => {
                instance_dispatch!(instance, |cx_instance| {
                    fwd_mesh.cast(cx_instance, LogForwardMessage::SetMode { stream_to_client })
                })
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            }

            // Forwarders were never spawned (global forwarding
            // disabled) and the caller is asking NOT to stream.
            // That's effectively a no-op so we silently accept.
            (None, false) => {
                // Nothing to do.
            }

            // Forwarders were never spawned, but caller is asking to
            // stream. We can't satisfy this request without
            // re-spawning infra, which we deliberately don't do at
            // runtime.
            (None, true) => {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "log forwarding disabled by config at startup; cannot enable streaming_to_client",
                ));
            }
        }

        // Always update the per-proc Python logging level.
        instance_dispatch!(instance, |cx_instance| {
            self.logger_mesh
                .cast(cx_instance, LoggerRuntimeMessage::SetLogging { level })
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        // Always update the client actor's aggregation window.
        self.client_actor
            .send(LogClientMessage::SetAggregate {
                aggregate_window_sec,
            })
            .map_err(anyhow::Error::msg)?;

        Ok(())
    }

    /// Force a sync flush of remote stdout/stderr back to the client,
    /// and wait for completion.
    ///
    /// If log forwarding was disabled at startup (so we never spawned
    /// any `LogForwardActor`s), this becomes a no-op success: there's
    /// nothing to flush from remote procs in that mode, and we don't
    /// try to manufacture it dynamically.
    fn flush(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        let forwarder_mesh_opt = self
            .forwarder_mesh
            .as_ref()
            .map(|mesh| mesh.deref().clone());
        let client_actor = self.client_actor.clone();
        let instance = instance.clone();

        PyPythonTask::new(async move {
            // If there's no forwarer mesh (forwarding disabled by
            // config), we just succeed immediately.
            let Some(forwarder_mesh) = forwarder_mesh_opt else {
                return Ok(());
            };

            instance_dispatch!(instance, async move |cx_instance| {
                Self::flush_internal(cx_instance, client_actor, forwarder_mesh).await
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }
}

impl Drop for LoggingMeshClient {
    fn drop(&mut self) {
        match self.client_actor.drain_and_stop() {
            Ok(_) => {}
            Err(e) => {
                // it is ok as during shutdown, the channel might already be closed
                tracing::debug!("error draining logging client actor during shutdown: {}", e);
            }
        }
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<LoggingMeshClient>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hyperactor::Instance;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::proc::Proc;
    use hyperactor_mesh::alloc::AllocSpec;
    use hyperactor_mesh::alloc::Allocator;
    use hyperactor_mesh::alloc::ProcessAllocator;
    use hyperactor_mesh::v1::ProcMesh;
    use hyperactor_mesh::v1::host_mesh::HostMesh;
    use ndslice::Extent;
    use ndslice::View; // .region(), .num_ranks() etc.
    use ndslice::extent;
    use tokio::process::Command;

    use super::*;
    use crate::pytokio::AwaitPyExt;
    use crate::pytokio::ensure_python;

    /// Bring up a minimal "world" suitable for integration-style
    /// tests.
    ///
    /// This does all the real work:
    ///   - spawns a root `Proc` locally,
    ///   - creates a client `Instance<()>` on that proc for
    ///     sending/receiving messages,
    ///   - allocates a 1-host `HostMesh` via the bootstrap binary,
    ///   - spawns a 1-rank `ProcMesh` on that host.
    pub async fn test_world() -> anyhow::Result<(
        Proc,         // root proc
        Instance<()>, // client instance handle for messaging
        HostMesh,     // allocated host mesh
        ProcMesh,     // spawned proc mesh
    )> {
        ensure_python();

        let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string())
            .await
            .expect("failed to start root Proc");

        let (instance, _handle) = proc
            .instance("client")
            .expect("failed to create Proc instance");

        let mut allocator = ProcessAllocator::new(Command::new(
            buck_resources::get("monarch/monarch_hyperactor/bootstrap")
                .expect("missing bootstrap exe"),
        ));

        let alloc = allocator
            .allocate(AllocSpec {
                extent: extent!(replicas = 1),
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
            })
            .await
            .expect("allocator.allocate failed");

        let host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
            .await
            .expect("HostMesh::allocate failed");

        let proc_mesh = host_mesh
            .spawn(&instance, "p0", Extent::unity())
            .await
            .expect("host_mesh.spawn failed");

        Ok((proc, instance, host_mesh, proc_mesh))
    }

    #[tokio::test]
    async fn test_world_smoke() {
        let (proc, instance, host_mesh, proc_mesh) =
            test_world().await.expect("bootstrap_cannonical failed");

        assert_eq!(
            host_mesh.region().num_ranks(),
            1,
            "bootstrap_cannonical() should allocate exactly one host"
        );

        assert_eq!(
            proc_mesh.region().num_ranks(),
            1,
            "bootstrap_cannonical() should spawn exactly one proc"
        );

        assert_eq!(
            instance.self_id().proc_id(),
            proc.proc_id(),
            "returned Instance<()> should be bound to the root Proc"
        );

        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }

    #[tokio::test]
    async fn spawn_respects_forwarding_flag() {
        let (_proc, instance, host_mesh, proc_mesh) = test_world().await.expect("world failed");

        let py_instance = PyInstance::from(&instance);
        let py_proc_mesh = PyProcMesh::new_owned(proc_mesh);

        let lock = hyperactor::config::global::lock();

        // Case 1: forwarding disabled => `forwarder_mesh` should be `None`.
        {
            let _guard = lock.override_key(
                hyperactor_mesh::bootstrap::MESH_ENABLE_LOG_FORWARDING,
                false,
            );

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding disabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding disabled)");

            Python::with_gil(|py| {
                let client_ref = client_py.borrow(py);
                assert!(
                    client_ref.forwarder_mesh.is_none(),
                    "forwarder_mesh should be None when forwarding disabled"
                );
            });
        }

        // Case 2: forwarding enabled => `forwarder_mesh` should be `Some`.
        {
            let _guard =
                lock.override_key(hyperactor_mesh::bootstrap::MESH_ENABLE_LOG_FORWARDING, true);

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding enabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding enabled)");

            Python::with_gil(|py| {
                let client_ref = client_py.borrow(py);
                assert!(
                    client_ref.forwarder_mesh.is_some(),
                    "forwarder_mesh should be Some(..) when forwarding is enabled"
                );
            });
        }

        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }
}
