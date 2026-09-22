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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorEnvironment;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::context;
use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::actor_mesh::ActorMeshRef;
use hyperactor_mesh::bootstrap::MESH_ENABLE_LOG_FORWARDING;
use hyperactor_mesh::logging::LogClientActor;
use hyperactor_mesh::logging::LogClientMessage;
use hyperactor_mesh::logging::LogForwardActor;
use hyperactor_mesh::logging::LogForwardMessage;
use monarch_types::SerializablePyErr;
use ndslice::View;
use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::types::PyString;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::context::PyInstance;
use crate::proc::PyActorAddr;
use crate::proc_mesh::PyProcMesh;
use crate::pytokio::PyPythonTask;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient
)]
pub enum LoggerRuntimeMessage {
    SetLogging { level: u8 },
}

/// Simple Rust actor that invokes python logger APIs. It needs a python runtime.
#[derive(Debug)]
#[hyperactor::export(handlers = [LoggerRuntimeMessage])]
#[hyperactor::spawnable]
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
#[async_trait]
impl Actor for LoggerRuntimeActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        Ok(())
    }
}

#[async_trait]
impl RemoteSpawn for LoggerRuntimeActor {
    type Params = ();

    async fn new(_: (), _environment: &ActorEnvironment) -> Result<Self, anyhow::Error> {
        let logger = monarch_with_gil(GilSite::Logging, |py| {
            Self::get_logger(py).map_err(SerializablePyErr::from_fn(py))
        })
        .await?;
        Ok(Self {
            logger: Arc::new(logger),
        })
    }
}

#[async_trait]
#[hyperactor::handle(LoggerRuntimeMessage)]
impl LoggerRuntimeMessageHandler for LoggerRuntimeActor {
    async fn set_logging(&mut self, _cx: &Context<Self>, level: u8) -> Result<(), anyhow::Error> {
        let logger: Arc<_> = self.logger.clone();
        monarch_with_gil(GilSite::Logging, |py| {
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
        client_actor.post(
            cx,
            LogClientMessage::StartSyncFlush {
                expected_procs: forwarder_mesh.region().num_ranks(),
                reply: reply_tx.bind(),
                version: version_tx.bind(),
            },
        );

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
            static LOG_CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(0);
            let id = LOG_CLIENT_COUNTER.fetch_add(1, Ordering::Relaxed);
            let name = if id == 0 {
                "log_client".to_string()
            } else {
                format!("log_client_{}", id)
            };
            let client_actor: ActorHandle<LogClientActor> = instance
                .proc()
                .spawn_with_label(&name, LogClientActor::default());
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
            .cast_with_return_undeliverable(
                instance.deref(),
                &Default::default(),
                LoggerRuntimeMessage::SetLogging { level },
                false,
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        // Always update the client actor's aggregation window.
        self.client_actor.post(
            instance.deref(),
            LogClientMessage::SetAggregate {
                aggregate_window_sec,
            },
        );

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
pub(crate) fn format_traceback(py: Python<'_>, err: &PyErr) -> String {
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
fn log_endpoint_exception(
    py: Python<'_>,
    e: Py<PyAny>,
    endpoint: Py<PyAny>,
    actor_id: PyActorAddr,
) {
    let pyerr = PyErr::from_value(e.into_bound(py));
    let exception_str = format_traceback(py, &pyerr);
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
    use std::time::Duration;

    use anyhow::Result;
    use hyperactor::Instance;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::proc::Proc;
    use hyperactor_mesh::ProcMesh;
    use hyperactor_mesh::host_mesh::HostMesh;
    use hyperactor_mesh::host_mesh::HostMeshShutdownGuard;
    use hyperactor_mesh::logging::LogMessage;
    use ndslice::Extent;
    use ndslice::View; // .region(), .num_ranks() etc.
    use tokio::time::timeout;

    use super::*;
    use crate::actor::PythonActor;
    use crate::pytokio::AwaitPyExt;
    use crate::pytokio::ensure_python;

    const TEST_DEADLINE: Duration = Duration::from_secs(30);

    /// The guard covers setup failures and panics. This helper gives ordinary
    /// `Result` exits a bounded explicit shutdown.
    async fn shutdown_after_test(
        mut host_mesh: HostMeshShutdownGuard,
        instance: &Instance<PythonActor>,
        test_result: Result<()>,
    ) -> Result<()> {
        let shutdown_result = match timeout(TEST_DEADLINE, host_mesh.shutdown(instance)).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("host shutdown exceeded {TEST_DEADLINE:?}")),
        };

        match (test_result, shutdown_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(test_error), Ok(())) => Err(test_error),
            (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
            (Err(test_error), Err(shutdown_error)) => Err(anyhow::anyhow!(
                "test failed: {test_error:#}; host shutdown also failed: {shutdown_error:#}"
            )),
        }
    }

    /// Drive a logging-client task without the test-only conversion helper's
    /// internal `expect`s, so an error remains available to the cleanup path.
    async fn drive_logging_client(mut task: PyPythonTask) -> Result<Py<LoggingMeshClient>> {
        let future = task.take_task()?;
        let value = timeout(TEST_DEADLINE, future)
            .await
            .map_err(|_| anyhow::anyhow!("logging client spawn exceeded {TEST_DEADLINE:?}"))??;
        Ok(monarch_with_gil(GilSite::Test, |py| {
            value
                .bind(py)
                .extract::<Py<LoggingMeshClient>>()
                .map_err(|error| pyo3::exceptions::PyTypeError::new_err(error.to_string()))
        })
        .await?)
    }

    /// Drive a unit-returning Python task without a panic-bearing conversion.
    async fn drive_unit_task(mut task: PyPythonTask, operation: &str) -> Result<()> {
        let future = task.take_task()?;
        timeout(TEST_DEADLINE, future)
            .await
            .map_err(|_| anyhow::anyhow!("{operation} exceeded {TEST_DEADLINE:?}"))??;
        Ok(())
    }

    fn log_client_ordinal(label: &str) -> Result<usize> {
        if label == "log_client" {
            return Ok(0);
        }
        let suffix = label
            .strip_prefix("log_client_")
            .ok_or_else(|| anyhow::anyhow!("unexpected logging client label {label:?}"))?;
        Ok(suffix.parse()?)
    }

    /// Read the current sync-flush version without leaving a flush in progress.
    ///
    /// `StartSyncFlush` increments the version, so this probe changes the value it
    /// observes. Posting the matching acknowledgement and awaiting the reply closes
    /// that operation before another probe or production flush begins.
    async fn completed_sync_flush_probe(
        instance: &Instance<PythonActor>,
        client_actor: &ActorHandle<LogClientActor>,
    ) -> Result<u64> {
        let (reply_tx, reply_rx) = instance.open_once_port::<()>();
        let (version_tx, version_rx) = instance.open_once_port::<u64>();

        client_actor.try_post(
            instance,
            LogClientMessage::StartSyncFlush {
                expected_procs: 1,
                reply: reply_tx.bind(),
                version: version_tx.bind(),
            },
        )?;
        let version = timeout(TEST_DEADLINE, version_rx.recv())
            .await
            .map_err(|_| {
                anyhow::anyhow!("sync-flush version probe exceeded {TEST_DEADLINE:?}")
            })??;

        client_actor.try_post(
            instance,
            LogMessage::Flush {
                sync_version: Some(version),
            },
        )?;
        timeout(TEST_DEADLINE, reply_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("sync-flush reply probe exceeded {TEST_DEADLINE:?}"))??;

        Ok(version)
    }

    /// Bring up a minimal "world" suitable for integration-style
    /// tests.
    pub async fn test_world()
    -> Result<(Proc, Instance<PythonActor>, HostMeshShutdownGuard, ProcMesh)> {
        ensure_python();

        let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string())?;

        let ai = proc.actor_instance("client")?;
        let instance = ai.instance;

        let host_mesh = HostMesh::local_with_bootstrap(
            crate::testresource::get("monarch/monarch_hyperactor/bootstrap").into(),
        )
        .await?
        .shutdown_guard();

        let proc_mesh = host_mesh
            .spawn(&instance, "p0", Extent::unity(), None, None)
            .await?;

        Ok((proc, instance, host_mesh, proc_mesh))
    }

    #[cfg_attr(not(target_os = "linux"), ignore = "linux-only")]
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
            instance.self_addr().proc_addr(),
            proc.proc_addr().clone(),
            "returned Instance<()> should be bound to the root Proc"
        );

        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }

    #[cfg_attr(not(target_os = "linux"), ignore = "linux-only")]
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

            monarch_with_gil(GilSite::Test, |py| {
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

            monarch_with_gil(GilSite::Test, |py| {
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

    /// Constructing the raw spawn task validates and captures its inputs but does
    /// not spawn the local logging actor. The monotonic label counter is durable
    /// even if a wrongly created actor stops before the assertion can inspect it.
    #[cfg_attr(not(target_os = "linux"), ignore = "linux-only")]
    #[tokio::test]
    async fn discarded_spawn_task_creates_no_log_client_actor() -> Result<()> {
        let (_proc, instance, host_mesh, proc_mesh) =
            timeout(TEST_DEADLINE, test_world())
                .await
                .map_err(|_| anyhow::anyhow!("test world setup exceeded {TEST_DEADLINE:?}"))??;

        let test_result = async {
            let py_instance = PyInstance::from(&instance);
            let py_proc_mesh = PyProcMesh::new_owned(proc_mesh);
            let lock = hyperactor_config::global::lock();
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, false);

            let baseline_py = drive_logging_client(LoggingMeshClient::spawn(
                &py_instance,
                &py_proc_mesh,
            )?)
            .await?;
            let baseline_label = monarch_with_gil(GilSite::Test, |py| {
                baseline_py
                    .borrow(py)
                    .client_actor
                    .actor_addr()
                    .id()
                    .label()
                    .map(|label| label.as_str().to_owned())
            })
            .await
            .ok_or_else(|| anyhow::anyhow!("baseline logging client has no label"))?;
            let baseline_ordinal = log_client_ordinal(&baseline_label)?;
            drop(baseline_py);

            let discarded = LoggingMeshClient::spawn(&py_instance, &py_proc_mesh)?;
            drop(discarded);

            let control_py = drive_logging_client(LoggingMeshClient::spawn(
                &py_instance,
                &py_proc_mesh,
            )?)
            .await?;
            let control_label = monarch_with_gil(GilSite::Test, |py| {
                control_py
                    .borrow(py)
                    .client_actor
                    .actor_addr()
                    .id()
                    .label()
                    .map(|label| label.as_str().to_owned())
            })
            .await
            .ok_or_else(|| anyhow::anyhow!("control logging client has no label"))?;
            let control_ordinal = log_client_ordinal(&control_label)?;
            anyhow::ensure!(
                control_ordinal == baseline_ordinal + 1,
                "dropping an undriven spawn task must not consume a LogClientActor id: baseline {baseline_label}, control {control_label}"
            );

            drop(control_py);
            Ok(())
        }
        .await;

        shutdown_after_test(host_mesh, &instance, test_result).await
    }

    #[cfg_attr(not(target_os = "linux"), ignore = "linux-only")]
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

            monarch_with_gil(GilSite::Test, |py| {
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

            monarch_with_gil(GilSite::Test, |py| {
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

    #[cfg_attr(not(target_os = "linux"), ignore = "linux-only")]
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
            let flush_task = monarch_with_gil(GilSite::Test, |py| {
                let client_ref = client_py.borrow(py);
                client_ref
                    .flush(&py_instance)
                    .expect("flush() PyPythonTask (forwarding disabled)")
            })
            .await;

            // Await the returned PyPythonTask's future outside the
            // GIL.
            flush_task
                .await_unit()
                .await
                .expect("flush failed (forwarding disabled)");

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
            let flush_task = monarch_with_gil(GilSite::Test, |py| {
                client_py
                    .borrow(py)
                    .flush(&py_instance)
                    .expect("flush() PyPythonTask (forwarding enabled)")
            })
            .await;

            // Await the returned PyPythonTask's future outside the
            // GIL.
            flush_task
                .await_unit()
                .await
                .expect("flush failed (forwarding enabled)");

            drop(client_py); // See note "NOTE ON LIFECYCLE / CLEANUP"
        }

        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }

    /// A raw flush is inert until driven. Each version probe increments the value
    /// itself, so `+1` after discard means zero production flushes and `+3` after
    /// the driven control means exactly one production flush across three probes.
    #[cfg_attr(not(target_os = "linux"), ignore = "linux-only")]
    #[tokio::test]
    async fn discarded_flush_task_does_not_advance_sync_flush_version() -> Result<()> {
        let (_, instance, host_mesh, proc_mesh) = timeout(TEST_DEADLINE, test_world())
            .await
            .map_err(|_| anyhow::anyhow!("test world setup exceeded {TEST_DEADLINE:?}"))??;

        let test_result = async {
            let py_instance = PyInstance::from(&instance);
            let py_proc_mesh = PyProcMesh::new_owned(proc_mesh);
            let lock = hyperactor_config::global::lock();
            let _guard = lock.override_key(MESH_ENABLE_LOG_FORWARDING, true);

            let client_py = drive_logging_client(LoggingMeshClient::spawn(
                &py_instance,
                &py_proc_mesh,
            )?)
            .await?;
            let (forwarding_enabled, client_actor) =
                monarch_with_gil(GilSite::Test, |py| {
                    let client = client_py.borrow(py);
                    (client.forwarder_mesh.is_some(), client.client_actor.clone())
                })
                .await;
            anyhow::ensure!(
                forwarding_enabled,
                "the flush witness requires a real forwarding mesh"
            );

            let baseline = completed_sync_flush_probe(&instance, &client_actor).await?;

            let discarded = monarch_with_gil(GilSite::Test, |py| {
                client_py.borrow(py).flush(&py_instance)
            })
            .await?;
            drop(discarded);

            let after_discard = completed_sync_flush_probe(&instance, &client_actor).await?;
            anyhow::ensure!(
                after_discard == baseline + 1,
                "a discarded raw flush must add no real flush increment: baseline probe {baseline}, next probe {after_discard}"
            );

            let control = monarch_with_gil(GilSite::Test, |py| {
                client_py.borrow(py).flush(&py_instance)
            })
            .await?;
            drive_unit_task(control, "logging flush").await?;

            let after_control = completed_sync_flush_probe(&instance, &client_actor).await?;
            anyhow::ensure!(
                after_control == baseline + 3,
                "one driven flush between completed probes must add one real flush increment: baseline probe {baseline}, final probe {after_control}"
            );

            drop(client_py);
            Ok(())
        }
        .await;

        shutdown_after_test(host_mesh, &instance, test_result).await
    }
}
