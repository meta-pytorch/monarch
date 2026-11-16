/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Blocking-driver execution state for a sync [`PythonActor`].

use super::super::*;
use super::inbox::Receiver;

/// Sync actor state retained between construction, initialization, and cleanup.
#[derive(Debug)]
pub(in super::super) struct State {
    /// Where the actor's driver is in its lifecycle.
    lifecycle: Lifecycle,
    /// Shared flag that prevents the driver from claiming another callback
    /// after cleanup begins.
    stopping: Arc<AtomicBool>,
}

/// A sync actor's driver lifecycle. The inbox receiver and the driver thread
/// are never held together: `init` moves the receiver into the thread it
/// starts, and `cleanup` takes that thread to join it.
#[derive(Debug)]
enum Lifecycle {
    /// Constructed; `init` has not started the driver.
    Pending(Receiver),
    /// The driver thread is running `_sync_dispatch_loop`.
    Running(Py<PyAny>),
    /// The receiver was consumed or the driver was taken for joining.
    Stopped,
}

impl State {
    /// Create pre-initialization state: retain the inbox receiver, share its
    /// stop flag with Rust cleanup, and leave the driver thread unstarted.
    pub(in super::super) fn new(inbox_receiver: Receiver) -> Self {
        Self {
            stopping: inbox_receiver.stop_flag(),
            lifecycle: Lifecycle::Pending(inbox_receiver),
        }
    }

    /// Start `_sync_dispatch_loop` on the actor's Python driver thread.
    pub(in super::super) fn init(
        &mut self,
        py: Python<'_>,
        actor_mesh_mod: &Bound<'_, PyModule>,
        actor: &Py<PyAny>,
        self_instance: &Py<PyInstance>,
    ) -> anyhow::Result<()> {
        let inbox_receiver = match std::mem::replace(&mut self.lifecycle, Lifecycle::Stopped) {
            Lifecycle::Pending(inbox_receiver) => inbox_receiver,
            other => {
                self.lifecycle = other;
                anyhow::bail!("sync actor driver already started");
            }
        };
        self.lifecycle = Lifecycle::Running(start_driver(
            py,
            actor_mesh_mod,
            actor,
            inbox_receiver,
            self_instance,
        )?);
        Ok(())
    }

    /// Stop the driver claiming callbacks, send it a GIL-free cleanup request,
    /// await the cleanup outcome, then join the driver exactly once (SA-4,
    /// defined in `monarch._src.actor.actor_mesh`). If hyperactor's cleanup
    /// timeout drops this future while it waits, the driver still runs the
    /// queued `__cleanup__` after its active item and exits unjoined.
    pub(in super::super) async fn cleanup(
        &mut self,
        control_sender: &Sender,
        instance: Option<Arc<Py<PyInstance>>>,
        cx: &Context<'_, PythonActor>,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        // Take the thread so no later cleanup can join it again. No driver
        // means initialization never reached user actor construction, so there
        // is no user `__cleanup__` to run.
        let driver_thread = match std::mem::replace(&mut self.lifecycle, Lifecycle::Stopped) {
            Lifecycle::Running(driver_thread) => driver_thread,
            other => {
                self.lifecycle = other;
                return Ok(());
            }
        };
        // PythonActor::init caches the PyInstance before `State::init` starts
        // and stores the driver thread. A stored driver therefore implies a
        // cached instance.
        let instance =
            instance.expect("a sync driver starts only after Actor::init creates its PyInstance");
        // SI-6: publish stopping before enqueueing PendingSyncCleanup. If an
        // older QueuedSupervision or QueuedUndeliverable is ahead of cleanup
        // in the control queue, the driver drops it instead of invoking user
        // code.
        self.stopping.store(true, AtomicOrdering::Release);

        // Rust retains the Handle. The driver converts PendingSyncCleanup into
        // its Python form, runs `__cleanup__`, and settles the paired completer.
        let (handle, completer) = handle_pair();
        let cleanup = PendingSyncCleanup {
            instance,
            rank: cx.cast_point(),
            recording_span: cx.recording_span(),
            error,
            completer,
        };
        // A successful send waits for the driver to publish the hook's outcome.
        // If conversion or the driver fails first, dropping the completer fails
        // the Handle. A failed send means the driver receiver is already gone.
        // Either way, retain the result and still join below.
        let cleanup_result = match control_sender.send(cleanup) {
            Ok(()) => handle
                .wait_future()
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from),
            Err(_) => Err(anyhow::anyhow!("sync actor driver exited before cleanup")),
        };
        // SA-4: join even when cleanup delivery or execution failed, and only
        // propagate that failure after the driver has ended.
        let join_result = join_driver_thread(driver_thread).await;
        cleanup_result?;
        join_result
    }
}

/// Tells a sync actor's Python driver thread to run user `__cleanup__` and exit.
///
/// Rust cannot run the hook itself because the driver thread owns the actor's
/// Python state. Cleanup sends this through the control queue and waits on the
/// paired `Handle`. After any active item finishes, the driver runs the hook,
/// completes `completer` with its outcome, and exits. Rust then joins the driver
/// thread.
#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
struct SyncCleanup {
    /// The actor context installed while `__cleanup__` runs.
    #[pyo3(get)]
    context: Py<PyContext>,
    /// The string form of the actor failure passed to `__cleanup__`, or `None`
    /// for a normal stop.
    #[pyo3(get)]
    error: Option<String>,
    /// The sole producer for the `Handle` awaited by Rust cleanup.
    #[pyo3(get)]
    completer: Py<PyHandleCompleter>,
}

/// A [`SyncCleanup`] constructed on the Python driver thread so Rust cleanup
/// can enqueue it without taking the GIL.
struct PendingSyncCleanup {
    /// Cached Python actor identity used to construct the cleanup context.
    instance: Arc<Py<PyInstance>>,
    /// Actor rank installed in the cleanup context.
    rank: Point,
    /// Actor recording span installed in the cleanup context.
    recording_span: tracing::Span,
    /// Failure string passed to `__cleanup__`, or `None` for a normal stop.
    error: Option<String>,
    /// Sole producer for the Handle awaited by Rust cleanup.
    completer: PyHandleCompleter,
}

impl<'py> IntoPyObject<'py> for PendingSyncCleanup {
    type Target = SyncCleanup;
    type Output = Bound<'py, SyncCleanup>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let context = PyContext::from_parts(
            self.instance.clone_ref(py),
            self.rank,
            Some(self.recording_span),
            None,
        );
        Bound::new(
            py,
            SyncCleanup {
                context: Py::new(py, context)?,
                error: self.error,
                completer: Py::new(py, self.completer)?,
            },
        )
    }
}

/// Join the Python driver without blocking a Tokio async worker. Calling a
/// method on the Python `Thread` object requires the GIL; `Thread.join`
/// releases it during the actual wait.
async fn join_driver_thread(driver_thread: Py<PyAny>) -> anyhow::Result<()> {
    // Run the blocking Python call on Tokio's blocking pool. Awaiting this
    // task yields the current async worker so it can continue polling other
    // work.
    tokio::task::spawn_blocking(move || {
        // The blocking-pool thread acquires the GIL to enter `Thread.join`.
        // `join` releases the GIL while waiting, allowing the driver thread to
        // acquire the GIL, finish, and exit. The blocking-pool thread then
        // reacquires the GIL before `join` returns.
        monarch_with_gil_blocking(GilSite::Stop, |py| {
            driver_thread.call_method0(py, "join").map(|_| ())
        })
    })
    .await?
    .map_err(anyhow::Error::from)
}

/// Start `monarch-actor-driver` running
/// `_sync_dispatch_loop(actor, inbox, self_instance)`, and mark the thread for
/// the interpreter-exit reaper. Return the Python Thread retained for cleanup.
fn start_driver(
    py: Python<'_>,
    actor_mesh_mod: &Bound<'_, PyModule>,
    actor: &Py<PyAny>,
    inbox: Receiver,
    self_instance: &Py<PyInstance>,
) -> PyResult<Py<PyAny>> {
    let target = actor_mesh_mod.getattr("_sync_dispatch_loop")?;
    let args = (actor.clone_ref(py), inbox, self_instance.clone_ref(py)).into_pyobject(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("target", target)?;
    kwargs.set_item("args", args)?;
    kwargs.set_item("daemon", true)?;
    kwargs.set_item("name", "monarch-actor-driver")?;
    let thread = py
        .import("threading")?
        .call_method("Thread", (), Some(&kwargs))?;
    mark_actor_driver_thread(&thread)?;
    thread.call_method0("start")?;
    Ok(thread.unbind())
}

pub(in super::super) fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SyncCleanup>()?;
    Ok(())
}
