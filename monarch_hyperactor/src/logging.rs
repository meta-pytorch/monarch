/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::Unbind;
use hyperactor::context;
use hyperactor_config::Attrs;
use hyperactor_mesh::bootstrap::MESH_ENABLE_LOG_FORWARDING;
use hyperactor_mesh::logging::LogClientActor;
use hyperactor_mesh::logging::LogClientMessage;
use hyperactor_mesh::logging::LogForwardActor;
use hyperactor_mesh::logging::LogForwardMessage;
use hyperactor_mesh::v1::ActorMesh;
use hyperactor_mesh::v1::actor_mesh::ActorMeshRef;
use monarch_types::SerializablePyErr;
use ndslice::Point;
use ndslice::View;
use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::types::PyString;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::context::PyInstance;
use crate::proc::PyActorId;
use crate::proc_mesh::PyProcMesh;
use crate::pytokio::PyPythonTask;
use crate::runtime::monarch_with_gil;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub enum LoggerRuntimeMessage {
    SetLogging { level: u8 },
}

/// Simple Rust actor that invokes python logger APIs. It needs a python runtime.
#[derive(Debug)]
#[hyperactor::export(spawn = true, handlers = [LoggerRuntimeMessage {cast = true}])]
pub struct LoggerRuntimeActor {
    logger: Arc<Py<PyAny>>,
}

impl LoggerRuntimeActor {
    fn get_logger(py: Python) -> PyResult<Py<PyAny>> {
        // Import the Python AutoReloader class
        let logging_module = py.import("logging")?;
        let logger = logging_module.call_method0("getLogger")?;

        Ok(logger.into())
    }

    fn set_logger_level(py: Python, logger: &Py<PyAny>, level: u8) -> PyResult<()> {
        let logger = logger.bind(py);
        logger.call_method1("setLevel", (level,))?;
        Ok(())
    }
}
impl Actor for LoggerRuntimeActor {}

#[async_trait]
impl RemoteSpawn for LoggerRuntimeActor {
    type Params = ();

    async fn new(_: (), _environment: Attrs) -> Result<Self, anyhow::Error> {
        let logger =
            monarch_with_gil(|py| Self::get_logger(py).map_err(SerializablePyErr::from_fn(py)))
                .await?;
        Ok(Self {
            logger: Arc::new(logger),
        })
    }
}

#[async_trait]
#[hyperactor::forward(LoggerRuntimeMessage)]
impl LoggerRuntimeMessageHandler for LoggerRuntimeActor {
    async fn set_logging(&mut self, _cx: &Context<Self>, level: u8) -> Result<(), anyhow::Error> {
        let logger: Arc<_> = self.logger.clone();
        monarch_with_gil(|py| {
            Self::set_logger_level(py, logger.as_ref(), level)
                .map_err(SerializablePyErr::from_fn(py))
        })
        .await?;
        Ok(())
    }
}

/// `LoggingMeshClient` is the Python-facing handle for distributed
/// logging over a `ProcMesh`.
///
/// Calling `spawn(...)` builds three pieces of logging infra:
///
///   - `client_actor`: a single `LogClientActor` running in the
///     *local* process. It aggregates forwarded stdout/stderr,
///     batches it, and coordinates sync flush barriers.
///
///   - `forwarder_mesh`: (optional) an `ActorMesh<LogForwardActor>`
///     with one actor per remote proc. Each `LogForwardActor` sits in
///     that proc and forwards its stdout/stderr back to the client.
///     This mesh only exists if `MESH_ENABLE_LOG_FORWARDING` was `true`
///     at startup; otherwise it's `None` and we never spawn any
///     forwarders.
///
///   - `logger_mesh`: an `ActorMesh<LoggerRuntimeActor>` with one
///     actor per remote proc. Each `LoggerRuntimeActor` controls that
///     proc's Python logging runtime (log level, handlers, etc.).
///     This mesh is always created, even if forwarding is disabled.
///
/// The Python object you get back holds references to all of this so
/// that you can:
///   - toggle streaming vs "stay quiet" (`set_mode(...)`),
///   - adjust the per-proc Python log level (`set_mode(...)`),
///   - force a sync flush of forwarded output and wait for completion
///     (`flush(...)`).
///
/// Drop semantics:
///   Dropping the Python handle runs `Drop` on this Rust struct,
///   which drains/stops the local `LogClientActor` but does *not*
///   synchronously tear down the per-proc meshes. The remote
///   `LogForwardActor` / `LoggerRuntimeActor` instances keep running
///   until the remote procs themselves are shut down (e.g. via
///   `host_mesh.shutdown(...)` in tests).
#[pyclass(
    frozen,
    name = "LoggingMeshClient",
    module = "monarch._rust_bindings.monarch_hyperactor.logging"
)]
pub struct LoggingMeshClient {
    // Per-proc LogForwardActor mesh (optional). When enabled, each
    // remote proc forwards its stdout/stderr back to the client. This
    // actor does not interact with the embedded Python runtime.
    forwarder_mesh: Option<ActorMesh<LogForwardActor>>,

    // Per-proc LoggerRuntimeActor mesh. One LoggerRuntimeActor runs
    // on every proc in the mesh and is responsible for driving that
    // proc's Python logging configuration (log level, handlers,
    // etc.).
    //
    // `set_mode(..)` always broadcasts the requested log level to
    // this mesh, regardless of whether stdout/stderr forwarding is
    // enabled.
    //
    // Even on a proc that isn't meaningfully running Python code, we
    // still spawn LoggerRuntimeActor and it will still apply the new
    // level to that proc's Python logger. In that case, updating the
    // level may have no visible effect simply because nothing on that
    // proc ever emits logs through Python's `logging` module.
    logger_mesh: ActorMesh<LoggerRuntimeActor>,

    // Client-side LogClientActor. Lives in the client process;
    // receives forwarded output, aggregates and buffers it, and
    // coordinates sync flush barriers.
    client_actor: ActorHandle<LogClientActor>,
}

impl LoggingMeshClient {
    /// Drive a synchronous "drain all logs now" barrier across the
    /// mesh.
    ///
    /// Protocol:
    ///   1. Tell the local `LogClientActor` we're starting a sync
    ///      flush. We give it:
    ///      - how many procs we expect to hear from
    ///        (`expected_procs`),
    ///      - a `reply` port it will use to signal completion,
    ///      - a `version` port it will use to hand us a flush version
    ///        token. After this send, the client_actor is now in "sync
    ///        flush vN" mode.
    ///
    ///   2. Wait for that version token from the client. This tells
    ///      us which flush epoch we're coordinating
    ///      (`version_rx.recv()`).
    ///
    ///   3. Broadcast `ForceSyncFlush { version }` to every
    ///      `LogForwardActor` in the `forwarder_mesh`. Each forwarder
    ///      tells its proc-local logger/forwarding loop: "flush
    ///      everything you have for this version now, then report
    ///      back."
    ///
    ///   4. Wait on `reply_rx`. The `LogClientActor` only replies
    ///      once it has:
    ///      - received the per-proc sync points for this version from
    ///        all forwarders,
    ///      - emitted/forwarded their buffered output,
    ///      - and finished flushing its own buffers.
    ///
    /// When this returns `Ok(())`, all stdout/stderr that existed at
    /// the moment we kicked off the flush has been forwarded to the
    /// client and drained. This is used by
    /// `LoggingMeshClient.flush()`.
    async fn flush_internal(
        cx: &impl context::Actor,
        client_actor: ActorHandle<LogClientActor>,
        forwarder_mesh: ActorMeshRef<LogForwardActor>,
    ) -> Result<(), anyhow::Error> {
        let (reply_tx, reply_rx) = cx.instance().open_once_port::<()>();
        let (version_tx, version_rx) = cx.instance().open_once_port::<u64>();

        // First initialize a sync flush.
        client_actor.send(
            cx,
            LogClientMessage::StartSyncFlush {
                expected_procs: forwarder_mesh.region().num_ranks(),
                reply: reply_tx.bind(),
                version: version_tx.bind(),
            },
        )?;

        let version = version_rx.recv().await?;

        // Then ask all the flushers to ask the log forwarders to sync
        // flush
        forwarder_mesh.cast(cx, LogForwardMessage::ForceSyncFlush { version })?;

        // Finally the forwarder will send sync point back to the
        // client, flush, and return.
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
                instance.spawn(LogClientActor::default())?;
            let client_actor_ref = client_actor.bind();

            // Read config to decide if we stand up per-proc
            // stdout/stderr forwarding.
            let forwarding_enabled = hyperactor_config::global::get(MESH_ENABLE_LOG_FORWARDING);

            // 2. Optionally spawn per-proc `LogForwardActor` mesh
            // (stdout/stderr forwarders).
            let forwarder_mesh = if forwarding_enabled {
                // Spawn a `LogFwdActor` on every proc.
                let mesh = proc_mesh
                    .spawn(instance.deref(), "log_forwarder", &client_actor_ref)
                    .await
                    .map_err(anyhow::Error::from)?;

                Some(mesh)
            } else {
                None
            };

            // 3. Always spawn a `LoggerRuntimeActor` on every proc.
            let logger_mesh = proc_mesh
                .spawn(instance.deref(), "logger", &())
                .await
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
            // Forwarders exist (config enabled at startup). We can
            // toggle live.
            (Some(fwd_mesh), _) => {
                fwd_mesh
                    .cast(
                        instance.deref(),
                        LogForwardMessage::SetMode { stream_to_client },
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;
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
                // return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                //     "log forwarding disabled by config at startup; cannot enable streaming_to_client",
                // ));
            }
        }

        // Always update the per-proc Python logging level.
        self.logger_mesh
            .cast(instance.deref(), LoggerRuntimeMessage::SetLogging { level })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        // Always update the client actor's aggregation window.
        self.client_actor
            .send(
                instance.deref(),
                LogClientMessage::SetAggregate {
                    aggregate_window_sec,
                },
            )
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

            Self::flush_internal(instance.deref(), client_actor, forwarder_mesh)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }
}

// NOTE ON LIFECYCLE / CLEANUP
//
// `LoggingMeshClient` is a thin owner for three pieces of logging
// infra:
//
//   - `client_actor`: a single `LogClientActor` in the *local*
//     process.
//   - `forwarder_mesh`: (optional) an `ActorMesh<LogForwardActor>`
//     with one actor per remote proc in the `ProcMesh`, responsible for
//     forwarding that proc's stdout/stderr back to the client.
//   - `logger_mesh`: an `ActorMesh<LoggerRuntimeActor>` with one
//     actor per remote proc, responsible for driving that proc's Python
//     logging configuration.
//
// The Python-facing handle we hand back to callers is a
// `Py<LoggingMeshClient>`. When that handle is dropped (or goes out
// of scope in a test), PyO3 will run `Drop` for `LoggingMeshClient`.
//
// Important:
//
// - In `Drop` we *only* call `drain_and_stop()` on the local
//   `LogClientActor`. This asks the client-side aggregator to
//   flush/stop so we don't leave a local task running.
// - We do NOT synchronously tear down the per-proc meshes here.
//   Dropping `forwarder_mesh` / `logger_mesh` just releases our
//   handles; the actual `LogForwardActor` / `LoggerRuntimeActor`
//   instances keep running on the remote procs until those procs are
//   shut down.
//
// This is fine in tests because we always shut the world down
// afterward via `host_mesh.shutdown(&instance)`, which tears down the
// spawned procs and all actors running in them. In other words:
//
//   drop(Py<LoggingMeshClient>)
//     → stops the local `LogClientActor`, drops mesh handles
//   host_mesh.shutdown(...)
//     → kills the remote procs, which takes out the per-proc actors
//
// If you reuse this type outside tests, keep in mind that simply
// dropping `LoggingMeshClient` does *not* on its own tear down the
// remote logging actors; it only stops the local client actor.
impl Drop for LoggingMeshClient {
    fn drop(&mut self) {
        // Use catch_unwind to guard against panics during interpreter shutdown.
        // During Python teardown, the tokio runtime or channels may already be
        // deallocated, and attempting to drain could cause a segfault.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match self.client_actor.drain_and_stop("logging client shutdown") {
                Ok(_) => {}
                Err(e) => {
                    // it is ok as during shutdown, the channel might already be closed
                    tracing::debug!("error draining logging client actor during shutdown: {}", e);
                }
            }
        }));
    }
}

/// Turns a python exception into a string with a traceback. If the traceback doesn't
/// exist or can't be formatted, returns just the exception message.
fn format_traceback<'py>(py: Python<'py>, err: PyErr) -> String {
    let traceback = err.traceback(py);
    if traceback.is_some() {
        let inner = || -> PyResult<String> {
            let formatted = py
                .import("traceback")?
                .call_method1("format_exception", (err.clone_ref(py),))?;
            Ok(PyString::new(py, "")
                .call_method1("join", (formatted,))?
                .to_string())
        };
        match inner() {
            Ok(s) => s,
            Err(e) => format!("{}: no traceback {}", err, e),
        }
    } else {
        err.to_string()
    }
}

#[pyfunction]
fn log_endpoint_exception<'py>(
    py: Python<'py>,
    e: Py<PyAny>,
    endpoint: Py<PyAny>,
    actor_id: PyActorId,
) {
    let pyerr = PyErr::from_value(e.into_bound(py));
    let exception_str = format_traceback(py, pyerr);
    let endpoint = endpoint.into_bound(py).to_string();
    tracing::info!(
        actor_id = actor_id.inner.to_string(),
        %endpoint,
        "exception occurred in endpoint: {}",
        exception_str,
    );
}

/// Register the Python-facing types for this module.
///
/// `pyo3` calls this when building `monarch._rust_bindings...`. We
/// expose `LoggingMeshClient` so that Python can construct it and
/// call its methods (`spawn`, `set_mode`, `flush`, ...).
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<LoggingMeshClient>()?;
    let log_endpoint_exception = wrap_pyfunction!(log_endpoint_exception, module.py())?;
    log_endpoint_exception.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.logging",
    )?;
    module.add_function(log_endpoint_exception)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use hyperactor::Instance;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::proc::Proc;
    use hyperactor_mesh::v1::ProcMesh;
    use hyperactor_mesh::v1::host_mesh::HostMesh;
    use ndslice::Extent;
    use ndslice::View; // .region(), .num_ranks() etc.

    use super::*;
    use crate::actor::PythonActor;
    use crate::pytokio::AwaitPyExt;
    use crate::pytokio::ensure_python;

    /// Bring up a minimal "world" suitable for integration-style
    /// tests.
    pub async fn test_world() -> Result<(Proc, Instance<PythonActor>, HostMesh, ProcMesh)> {
        ensure_python();

        let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string())
            .expect("failed to start root Proc");

        let (instance, ..) = proc
            .actor_instance("client")
            .expect("failed to create proc Instance");

        let host_mesh = HostMesh::local_with_bootstrap(
            crate::testresource::get("monarch/monarch_hyperactor/bootstrap").into(),
        )
        .await
        .expect("failed to bootstrap HostMesh");

        let proc_mesh = host_mesh
            .spawn(&instance, "p0", Extent::unity())
            .await
            .expect("failed to spawn ProcMesh");

        Ok((proc, instance, host_mesh, proc_mesh))
    }

    #[tokio::test]
    async fn test_world_smoke() {
        let (proc, instance, mut host_mesh, proc_mesh) = test_world().await.expect("world failed");

        assert_eq!(
            host_mesh.region().num_ranks(),
            1,
            "should allocate exactly one host"
        );
        assert_eq!(
            proc_mesh.region().num_ranks(),
            1,
            "should spawn exactly one proc"
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
        let (_, instance, mut host_mesh, proc_mesh) = test_world().await.expect("world failed");

        let py_instance = PyInstance::from(&instance);
        let py_proc_mesh = PyProcMesh::new_owned(proc_mesh);
        let lock = hyperactor_config::global::lock();

        // Case 1: forwarding disabled => `forwarder_mesh` should be `None`.
        {
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, false);

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding disabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding disabled)");

            monarch_with_gil(|py| {
                let client_ref = client_py.borrow(py);
                assert!(
                    client_ref.forwarder_mesh.is_none(),
                    "forwarder_mesh should be None when forwarding disabled"
                );
            })
            .await;

            drop(client_py); // See "NOTE ON LIFECYCLE / CLEANUP"
        }

        // Case 2: forwarding enabled => `forwarder_mesh` should be `Some`.
        {
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, true);

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding enabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding enabled)");

            monarch_with_gil(|py| {
                let client_ref = client_py.borrow(py);
                assert!(
                    client_ref.forwarder_mesh.is_some(),
                    "forwarder_mesh should be Some(..) when forwarding is enabled"
                );
            })
            .await;

            drop(client_py); // See "NOTE ON LIFECYCLE / CLEANUP"
        }

        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }

    #[tokio::test]
    async fn set_mode_behaviors() {
        let (_proc, instance, mut host_mesh, proc_mesh) = test_world().await.expect("world failed");

        let py_instance = PyInstance::from(&instance);
        let py_proc_mesh = PyProcMesh::new_owned(proc_mesh);
        let lock = hyperactor_config::global::lock();

        // Case 1: forwarding disabled => `forwarder_mesh.is_none()`.
        {
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, false);

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding disabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding disabled)");

            monarch_with_gil(|py| {
                let client_ref = client_py.borrow(py);

                // (a) stream_to_client = false, no aggregate window
                // -> OK
                let res = client_ref.set_mode(&py_instance, false, None, 10);
                assert!(res.is_ok(), "expected Ok(..), got {res:?}");

                // (b) stream_to_client = false,
                // aggregate_window_sec.is_some() -> Err = Some(..) ->
                // Err
                let res = client_ref.set_mode(&py_instance, false, Some(1), 10);
                assert!(
                    res.is_err(),
                    "expected Err(..) for window without streaming"
                );
                if let Err(e) = res {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("cannot set aggregate window without streaming to client"),
                        "unexpected err for aggregate_window without streaming: {msg}"
                    );
                }

                /*
                // Update (SF: 2025, 11, 13): We now ignore stream to client requests if
                // log forwarding is enabled.
                // (c) stream_to_client = true when forwarding was
                //     never spawned -> Err
                let res = client_ref.set_mode(&py_instance, true, None, 10);
                assert!(
                    res.is_err(),
                    "expected Err(..) when enabling streaming but no forwarders"
                );
                if let Err(e) = res {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("log forwarding disabled by config at startup"),
                        "unexpected err when enabling streaming with no forwarders: {msg}"
                    );
                }
                */
            })
            .await;

            drop(client_py); // See note "NOTE ON LIFECYCLE / CLEANUP"
        }

        // Case 2: forwarding enabled => `forwarder_mesh.is_some()`.
        {
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, true);

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding enabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding enabled)");

            monarch_with_gil(|py| {
                let client_ref = client_py.borrow(py);

                // (d) stream_to_client = true, aggregate_window_sec =
                //     Some(..) -> OK now that we *do* have forwarders,
                //     enabling streaming should succeed.
                let res = client_ref.set_mode(&py_instance, true, Some(2), 20);
                assert!(
                    res.is_ok(),
                    "expected Ok(..) enabling streaming w/ window: {res:?}"
                );

                // (e) aggregate_window_sec = Some(..) but
                //     stream_to_client = false -> still Err (this
                //     rule doesn't care about forwarding being
                //     enabled or not).
                let res = client_ref.set_mode(&py_instance, false, Some(2), 20);
                assert!(
                    res.is_err(),
                    "expected Err(..) for window without streaming even w/ forwarders"
                );
                if let Err(e) = res {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("cannot set aggregate window without streaming to client"),
                        "unexpected err when setting window but disabling streaming: {msg}"
                    );
                }
            })
            .await;

            drop(client_py); // See note "NOTE ON LIFECYCLE / CLEANUP"
        }

        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }

    #[tokio::test]
    async fn flush_behaviors() {
        let (_proc, instance, mut host_mesh, proc_mesh) = test_world().await.expect("world failed");

        let py_instance = PyInstance::from(&instance);
        let py_proc_mesh = PyProcMesh::new_owned(proc_mesh);
        let lock = hyperactor_config::global::lock();

        // Case 1: forwarding disabled => `forwarder_mesh.is_none()`.
        {
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, false);

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding disabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding disabled)");

            // Call flush() and bring the PyPythonTask back out.
            let flush_task = monarch_with_gil(|py| {
                let client_ref = client_py.borrow(py);
                client_ref
                    .flush(&py_instance)
                    .expect("flush() PyPythonTask (forwarding disabled)")
            })
            .await;

            // Await the returned PyPythonTask's future outside the
            // GIL.
            let flush_result = flush_task
                .await_unit()
                .await
                .expect("flush failed (forwarding disabled)");

            let _ = flush_result;
            drop(client_py); // See "NOTE ON LIFECYCLE / CLEANUP"
        }

        // Case 2: forwarding enabled => `forwarder_mesh.is_some()`.
        {
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, true);

            let client_task = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)
                .expect("spawn PyPythonTask (forwarding enabled)");

            let client_py: Py<LoggingMeshClient> = client_task
                .await_py()
                .await
                .expect("spawn failed (forwarding enabled)");

            // Call flush() to exercise the barrier path, and pull the
            // PyPythonTask out.
            let flush_task = monarch_with_gil(|py| {
                client_py
                    .borrow(py)
                    .flush(&py_instance)
                    .expect("flush() PyPythonTask (forwarding enabled)")
            })
            .await;

            // Await the returned PyPythonTask's future outside the
            // GIL.
            let flush_result = flush_task
                .await_unit()
                .await
                .expect("flush failed (forwarding enabled)");

            let _ = flush_result;
            drop(client_py); // See note "NOTE ON LIFECYCLE / CLEANUP"
        }

        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }
}
