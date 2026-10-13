/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// `pytokio` is Monarch's Python <-> Tokio async bridge.
///
/// It provides a small, *non-asyncio* async world where Python code
/// can *compose* Rust/Tokio futures using `await`.
///
/// ## The core idea
///
/// In `pytokio`:
///
/// - `PythonTask` = a one-shot Rust/Tokio future that produces a
///   Python value.
/// - `from_coroutine` = wraps a Python coroutine as a Rust future
///   that drives it.
/// - `Shared` = an awaitable handle to a spawned background Tokio
///   task.
///
/// More concretely:
///
/// - Rust bindings return a Python-visible `PythonTask`
///   (`PyPythonTask`), which wraps a Rust `PythonTask` holding a
///   boxed Tokio future returning `PyResult<Py<PyAny>>`.
/// - `PythonTask.from_coroutine(coro)` wraps a *Python coroutine* as
///   a `PythonTask` by creating a Rust/Tokio future that drives
///   `coro.__await__()` (via `send`/`throw`) and awaits the
///   `PythonTask`s it yields.
/// - Python code may `await` a `PythonTask` / `Shared` **only** when
///   running under `PythonTask.from_coroutine(...)`. Awaiting
///   arbitrary Python awaitables (e.g. `asyncio` futures) is an
///   error.
/// - Calling `task.spawn()` returns a `Shared`
///   (`PyShared`), which yields the result of the background Tokio
///   task running the original `PythonTask`.
///
/// This is intentionally *not* a general-purpose async bridge: it’s a
/// way to use Python syntax to drive and compose Tokio futures.
///
/// ## Wrapping a Python coroutine
///
/// ```ignore
/// async def work():
///     x = await some_rust_binding()      # must yield PythonTask / Shared
///     await PythonTask.sleep(0.1)        # also a PythonTask
///     return x
///
/// task = PythonTask.from_coroutine(work())
/// result = task.block_on()              # block the calling Python thread while a
///                                       # Tokio runtime drives the task to completion
/// ```
///
/// `from_coroutine` drives the coroutine by repeatedly resuming it
/// and awaiting the `PythonTask`s it yields, using a Tokio runtime.
///
/// ## Spawning
///
/// `spawn()` runs a `PythonTask` on a background Tokio task and
/// returns a `Shared` handle.
///
/// To `await` the handle, you must still be inside a
/// `from_coroutine`-driven coroutine:
///
/// ```ignore
/// async def work():
///     task = some_rust_binding()
///     shared = task.spawn()
///     # ... do other work ...
///     result = await shared             # valid here (inside from_coroutine world)
///     return result
///
/// result = PythonTask.from_coroutine(work()).block_on()
/// ```
///
/// In synchronous contexts, you can wait for a spawned task without
/// `from_coroutine`:
///
/// ```ignore
/// shared = task.spawn()
/// result = shared.block_on()            # blocks the calling Python thread
/// ```
///
/// ## Context propagation
///
/// `from_coroutine` preserves Monarch’s `context()` across Tokio
/// thread hops, so code calling `context()` inside a `PythonTask`
/// sees the same actor context as the call site that constructed the
/// task.
use std::future::Future;
use std::pin::Pin;

use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use monarch_types::py_global;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyNone;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use tokio::sync::Mutex;
use tokio::sync::watch;

use crate::handle::HandleCore;
use crate::handle::PyHandle;
use crate::handle::send_result;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on;

// Shared class for pickling PyShared values
py_global!(
    shared_class,
    "monarch._rust_bindings.monarch_hyperactor.pytokio",
    "Shared"
);

/// Reduce a PyShared for pickling.
///
/// This function implements the pickle protocol for PyShared:
/// 1. If the shared is already finished, return (Shared.from_value, (value,))
/// 2. Otherwise, block on the shared and return (Shared.from_value, (value,))
pub fn reduce_shared<'py>(
    py: Python<'py>,
    py_shared: &Bound<'py, PyShared>,
) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyTuple>)> {
    // First, check if the shared is already finished
    if let Some(value) = py_shared.borrow().poll()? {
        let from_value = shared_class(py).getattr("from_value")?;
        let args = PyTuple::new(py, [value])?;
        return Ok((from_value, args));
    }

    // Pending: block on the shared. Meshes ride the out-of-band table via their
    // own type-specific reducers; this generic path is the fallback for any
    // other pending `PyShared`.
    let value = PyShared::block_on(py_shared.borrow(), py)?;
    let from_value = shared_class(py).getattr("from_value")?;
    let args = PyTuple::new(py, [value])?;
    Ok((from_value, args))
}

declare_attrs! {
    /// If true, capture a Python stack trace at `PythonTask` creation
    /// time and log it when a spawned task errors but is never
    /// awaited/polled.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_HYPERACTOR_ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK".to_string()),
        Some("enable_unawaited_python_task_traceback".to_string()),
    ))
    pub attr ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK: bool = false;
}

// Import Python helpers used for actor context propagation.
// `context()` returns the current Monarch actor context.
// `actor_mesh` is the module that owns the `_context` contextvar we
// must manually set/restore when driving coroutines on Tokio threads.
py_global!(context, "monarch._src.actor.actor_mesh", "context");
py_global!(actor_mesh_module, "monarch._src.actor", "actor_mesh");

/// Capture the current Python stack trace (creation call site) if
/// `ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK` is enabled.
///
/// Returns `None` when disabled to avoid the overhead of
/// `traceback.extract_stack()`.
fn current_traceback() -> PyResult<Option<Py<PyAny>>> {
    if hyperactor_config::global::get(ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK) {
        monarch_with_gil_blocking(GilSite::Traceback, |py| {
            Ok(Some(
                py.import("traceback")?
                    .call_method0("extract_stack")?
                    .unbind(),
            ))
        })
    } else {
        Ok(None)
    }
}

/// Helper struct to make a Rust/Tokio future (returning a Python
/// result) passable in an actor message.
///
/// The future resolves to `PyResult<Py<PyAny>>` so it can return a
/// Python value or raise a Python exception, and it is `Send +
/// 'static` so it can cross thread/actor boundaries.
///
/// Also so that we don't have to write this massive type signature
/// everywhere.
pub(crate) struct PythonTask {
    /// Boxed, pinned Rust/Tokio future producing a Python result,
    /// protected so it can be taken/consumed exactly once when the
    /// task is driven.
    // Type decoder ring:
    //
    // Mutex<Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send + 'static>>>
    //   │     │   │   │                                        │      │
    //   │     │   │   │                                        │      └─ owns all data, no dangling refs
    //   │     │   │   │                                        └─ can cross thread boundaries
    //   │     │   │   └─ any future type (type-erased)
    //   │     │   └─ heap-allocated (because unsized)
    //   │     └─ immovable (safe to poll self-referential futures)
    //   └─ exclusive access for consumption
    future: Mutex<Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send + 'static>>>,

    /// Optional Python stack trace captured at task construction
    /// time, used to annotate logs when a spawned task errors but
    /// nobody awaits/polls it.
    traceback: Option<Py<PyAny>>,
}

impl PythonTask {
    /// Construct a `PythonTask` from a Rust/Tokio future and an
    /// optional captured Python traceback.
    ///
    /// The future is boxed and pinned so it can be stored in the
    /// struct and later driven safely.
    fn new_with_traceback(
        fut: impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
        traceback: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            future: Mutex::new(Box::pin(fut)),
            traceback,
        }
    }

    /// Construct a `PythonTask`, capturing a creation-site traceback
    /// if enabled by `ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK`.
    pub(crate) fn new(
        fut: impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    ) -> PyResult<Self> {
        Ok(Self::new_with_traceback(fut, current_traceback()?))
    }

    /// Return the optional captured creation-site traceback (if
    /// enabled).
    fn traceback(&self) -> &Option<Py<PyAny>> {
        &self.traceback
    }

    /// Consume the task and return the boxed, pinned future.
    ///
    /// This is a one-shot operation: it moves the future out of the
    /// struct so it can be driven to completion.
    pub(crate) fn take(
        self,
    ) -> Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send + 'static>> {
        self.future.into_inner()
    }
}

impl std::fmt::Debug for PythonTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonTask")
            .field("future", &"<PythonFuture>")
            .finish()
    }
}

/// Python-visible wrapper for a one-shot `PythonTask`.
///
/// Exposed to Python as
/// `monarch._rust_bindings.monarch_hyperactor.pytokio.PythonTask`.
/// This object owns the underlying Rust task and is *consumed* when
/// it is run (e.g. via `spawn()` or `block_on()`), hence `inner: Option<_>`.
#[pyclass(
    name = "PythonTask",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
pub struct PyPythonTask {
    inner: Option<PythonTask>,
}

impl From<PythonTask> for PyPythonTask {
    fn from(task: PythonTask) -> Self {
        Self { inner: Some(task) }
    }
}

/// Minimal await-iterator used to implement Python's `__await__`
/// protocol for pytokio.
///
/// This iterator yields the task object exactly once. The Rust-side
/// coroutine driver (`from_coroutine`) resumes the Python coroutine
/// and expects it to yield a `PythonTask` (or `Shared`) object back
/// to Rust.
#[pyclass(
    name = "PythonTaskAwaitIterator",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
struct PythonTaskAwaitIterator {
    value: Option<Py<PyAny>>,
}

impl PythonTaskAwaitIterator {
    /// Create an await-iterator that will yield `task` exactly once.
    fn new(task: Py<PyAny>) -> PythonTaskAwaitIterator {
        PythonTaskAwaitIterator { value: Some(task) }
    }
}

#[pymethods]
impl PythonTaskAwaitIterator {
    /// First `send(...)` yields the stored task; subsequent sends
    /// raise `StopIteration`.
    ///
    /// Python's await machinery calls `send(None)` to advance the
    /// iterator.
    fn send(&mut self, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.value
            .take()
            .ok_or_else(|| PyStopIteration::new_err((value,)))
    }

    /// Convert the thrown Python exception value into a `PyErr` and
    /// surface it to Rust.
    fn throw(&mut self, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        Err(monarch_with_gil_blocking(GilSite::Convert, |py| {
            PyErr::from_value(value.into_bound(py))
        }))
    }

    /// Iterator protocol: `next(it)` is equivalent to
    /// `it.send(None)`.
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.send(py.None())
    }
}

impl PyPythonTask {
    /// Construct a Python-visible `PythonTask` from a Rust future,
    /// attaching an explicit creation-site traceback (if provided).
    ///
    /// The input future produces a Rust value `T`; on completion we
    /// reacquire the GIL and convert `T` into a Python object
    /// (`Py<PyAny>`).
    fn new_with_traceback<F, T>(fut: F, traceback: Option<Py<PyAny>>) -> PyResult<Self>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send,
    {
        Ok(PythonTask::new_with_traceback(
            async {
                let result = fut.await?;
                monarch_with_gil(GilSite::Convert, |py| result.into_py_any(py)).await
            },
            traceback,
        )
        .into())
    }

    /// Construct a `PythonTask`, capturing a creation-site traceback
    /// if enabled.
    ///
    /// See `new_with_traceback` for conversion semantics (`T` ->
    /// Python object under the GIL).
    pub fn new<F, T>(fut: F) -> PyResult<Self>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send,
    {
        Self::new_with_traceback(fut, current_traceback()?)
    }
}

impl PyPythonTask {
    /// Consume this `PythonTask` and return the underlying Rust
    /// future.
    ///
    /// This is a one-shot operation: after calling `take_task`, the
    /// `PyPythonTask` is considered *consumed* and cannot be
    /// spawned/awaited/blocked-on again.
    pub fn take_task(
        &mut self,
    ) -> PyResult<Pin<Box<dyn Future<Output = Result<Py<PyAny>, PyErr>> + Send + 'static>>> {
        self.inner
            .take()
            .map(|task| task.take())
            .ok_or_else(|| PyValueError::new_err("PythonTask already consumed"))
    }

    /// Return the captured creation-site traceback (if enabled),
    /// cloning it under the GIL.
    ///
    /// Fails if the task has already been consumed.
    fn traceback(&self) -> PyResult<Option<Py<PyAny>>> {
        if let Some(task) = &self.inner {
            Ok(monarch_with_gil_blocking(GilSite::Traceback, |py| {
                task.traceback().as_ref().map(|t| t.clone_ref(py))
            }))
        } else {
            Err(PyValueError::new_err("PythonTask already consumed"))
        }
    }

    /// Spawn this task onto the Tokio runtime and return the shared
    /// `HandleCore` that observes its completion via the `watch` channel.
    ///
    /// Consumes the task. Shared by `spawn`/`spawn_handle`; losing every
    /// observer does not cancel the producer.
    fn spawn_core(&mut self) -> PyResult<HandleCore> {
        let (tx, rx) = watch::channel(None);
        let traceback = self.traceback()?;
        // Clone the second owned copy under the same (single) GIL section, and
        // only when a traceback was actually captured -- avoids a second GIL
        // round-trip per spawn in the common (capture-disabled) case.
        let traceback1 = traceback
            .as_ref()
            .map(|t| monarch_with_gil_blocking(GilSite::Traceback, |py| t.clone_ref(py)));
        let task = self.take_task()?;
        get_tokio_runtime().spawn(async move {
            send_result(tx, task.await, traceback1);
        });
        Ok(HandleCore::new(rx, traceback))
    }
}

#[pymethods]
impl PyPythonTask {
    /// Run this task to completion synchronously on the embedded
    /// Tokio runtime.
    ///
    /// This blocks the calling Python thread until the underlying
    /// Rust future completes. Consumes the task (like `spawn`): the
    /// `PyPythonTask` cannot be used again.
    fn block_on(mut slf: PyRefMut<PyPythonTask>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let task = slf.take_task()?;

        // Mutable borrows of Python objects must be dropped before
        // releasing the GIL. `signal_safe_block_on` releases the GIL;
        // holding `slf` across that would make other Python access
        // throw.
        drop(slf);
        signal_safe_block_on(py, task)?
    }

    /// Spawn this task onto the Tokio runtime and return a `Shared`
    /// handle.
    ///
    /// The returned `Shared` is awaitable *inside* the
    /// `from_coroutine` world, or may be waited on synchronously via
    /// `Shared.block_on()`. Consumes the task.
    pub(crate) fn spawn(&mut self) -> PyResult<PyShared> {
        Ok(PyShared {
            core: self.spawn_core()?,
        })
    }

    /// Spawn this task onto the Tokio runtime and return an observe-only
    /// `Handle`.
    ///
    /// Like `spawn`, but hands back the clean `Handle` (`get`/`poll`/
    /// `as_asyncio`/`await`) rather than `Shared`; non-abortable on drop.
    /// Consumes the task.
    pub(crate) fn spawn_handle(&mut self) -> PyResult<PyHandle> {
        Ok(PyHandle::from_core(self.spawn_core()?))
    }

    /// Implement Python's `await` protocol for `PythonTask`.
    ///
    /// This is only supported inside the `pytokio` world driven by
    /// `PythonTask.from_coroutine`; attempting to `await` a
    /// `PythonTask` while an `asyncio` event loop is running is an
    /// error.
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<PythonTaskAwaitIterator> {
        let py = slf.py();
        let l = pyo3_async_runtimes::get_running_loop(py);
        if l.is_ok() {
            return Err(PyRuntimeError::new_err(
                "Attempting to __await__ a PythonTask when the asyncio event loop is active. PythonTask objects should only be awaited in coroutines passed to PythonTask.from_coroutine",
            ));
        }

        Ok(PythonTaskAwaitIterator::new(slf.into_py_any(py)?))
    }

    /// Wrap a Python coroutine into a `PythonTask` that is driven by
    /// Tokio.
    ///
    /// This converts `coro` into its await-iterator
    /// (`coro.__await__()`), then repeatedly resumes it via
    /// `send`/`throw`. Whenever the coroutine yields a
    /// `PythonTask`/`Shared`, we extract its underlying Rust future,
    /// `await` it on Tokio, and feed the result back into the
    /// coroutine on the next iteration.
    ///
    /// Inside this coroutine, `await` is only supported for pytokio
    /// values (`PythonTask` / `Shared`). Awaiting arbitrary Python
    /// awaitables (e.g. `asyncio` futures) is an error.
    ///
    /// The current Monarch `context()` is captured at construction
    /// time and restored while running the coroutine so `context()`
    /// inside the task reflects the call site that created it (even
    /// across Tokio thread hops).
    #[staticmethod]
    fn from_coroutine(py: Python<'_>, coro: Py<PyAny>) -> PyResult<PyPythonTask> {
        // context() used inside a PythonTask should inherit the value of
        // context() from the context in which the PythonTask was constructed.
        // We need to do this manually because the value of the contextvar isn't
        // maintained inside the tokio runtime.
        let monarch_context = context(py).call0()?.unbind();
        PyPythonTask::new(async move {
            let (coroutine_iterator, none) = monarch_with_gil(GilSite::AwaitDrive, |py| {
                coro.into_bound(py)
                    .call_method0("__await__")
                    .map(|x| (x.unbind(), py.None()))
            })
            .await?;
            let mut last: PyResult<Py<PyAny>> = Ok(none);
            enum Action {
                Return(Py<PyAny>),
                Wait(Pin<Box<dyn Future<Output = Result<Py<PyAny>, PyErr>> + Send + 'static>>),
            }
            loop {
                let action = monarch_with_gil(GilSite::AwaitDrive, |py| -> PyResult<Action> {
                    // We may be executing in a new thread at this point, so we need to set the value
                    // of context().
                    let _context = actor_mesh_module(py).getattr("_context")?;
                    let old_context = _context.call_method1("get", (PyNone::get(py),))?;
                    _context
                        .call_method1("set", (monarch_context.clone_ref(py),))
                        .expect("failed to set _context");

                    let result = match last {
                        Ok(value) => coroutine_iterator.bind(py).call_method1("send", (value,)),
                        Err(pyerr) => coroutine_iterator
                            .bind(py)
                            .call_method1("throw", (pyerr.into_value(py),)),
                    };

                    // Reset context() so that when this tokio thread yields, it has its original state.
                    _context
                        .call_method1("set", (old_context,))
                        .expect("failed to restore _context");
                    match result {
                        Ok(task) => Ok(Action::Wait(
                            task.extract::<Py<PyPythonTask>>()
                                .map_err(Into::<PyErr>::into)
                                .and_then(|t| t.borrow_mut(py).take_task())
                                .unwrap_or_else(|pyerr| Box::pin(async move { Err(pyerr) })),
                        )),
                        Err(err) => {
                            let err = err.into_pyobject(py)?.into_any();
                            if err.is_instance_of::<PyStopIteration>() {
                                Ok(Action::Return(
                                    err.into_pyobject(py)?.getattr("value")?.unbind(),
                                ))
                            } else {
                                Err(PyErr::from_value(err))
                            }
                        }
                    }
                })
                .await?;
                match action {
                    Action::Return(x) => {
                        return Ok(x);
                    }
                    Action::Wait(task) => {
                        last = task.await;
                    }
                };
            }
        })
    }

    /// Wrap this task with a timeout and return a new `PythonTask`.
    ///
    /// Consumes the original task. If it does not complete within
    /// `seconds`, the returned task fails with `TimeoutError`.
    fn with_timeout(&mut self, seconds: f64) -> PyResult<PyPythonTask> {
        // Reject a negative, NaN, or non-finite timeout with ValueError up front
        // rather than panicking in Duration::from_secs_f64 on a Tokio worker
        // thread (matching Handle.get(timeout)).
        let duration = std::time::Duration::try_from_secs_f64(seconds)
            .map_err(|e| PyValueError::new_err(format!("invalid timeout {seconds}: {e}")))?;
        let tb = self.traceback()?;
        let task = self.take_task()?;
        PyPythonTask::new_with_traceback(
            async move {
                tokio::time::timeout(duration, task)
                    .await
                    .map_err(|_| PyTimeoutError::new_err(()))?
            },
            tb,
        )
    }

    /// Run a Python callable on Tokio's blocking thread pool and
    /// return a `Shared` handle.
    ///
    /// This is for CPU-bound or otherwise blocking Python work that
    /// must not run on a Tokio async worker thread. The callable `f`
    /// is executed via `tokio::spawn_blocking`, and its result (or
    /// raised exception) is delivered through the returned `Shared`.
    ///
    /// The current Monarch `context()` is captured and restored while
    /// running `f` so calls to `context()` from inside `f` see the
    /// originating actor context.
    #[staticmethod]
    fn spawn_blocking(py: Python<'_>, f: Py<PyAny>) -> PyResult<PyShared> {
        let (tx, rx) = watch::channel(None);
        let traceback = current_traceback()?;
        let traceback1 = traceback
            .as_ref()
            .map(|t| monarch_with_gil_blocking(GilSite::Traceback, |py| t.clone_ref(py)));
        let monarch_context = context(py).call0()?.unbind();
        // The `_context` contextvar needs to be propagated through to the thread that
        // runs the blocking tokio task. Upon completion, the original value of `_context`
        // is restored.
        get_tokio_runtime().spawn_blocking(move || {
            let result = monarch_with_gil_blocking(GilSite::AwaitDrive, |py| {
                let _context = actor_mesh_module(py).getattr("_context")?;
                let old_context = _context.call_method1("get", (PyNone::get(py),))?;
                _context
                    .call_method1("set", (monarch_context.clone_ref(py),))
                    .expect("failed to set _context");
                let result = f.call0(py);
                _context
                    .call_method1("set", (old_context,))
                    .expect("failed to restore _context");
                result
            });
            send_result(tx, result, traceback1);
        });
        Ok(PyShared {
            core: HandleCore::new(rx, traceback),
        })
    }

    /// Wait for the first task to complete and return `(result,
    /// index)`.
    ///
    /// This consumes all input tasks (each is `take_task()`'d). The
    /// returned task resolves to a tuple of the winning task's result
    /// and its index in the input list.
    #[staticmethod]
    fn select_one(mut tasks: Vec<PyRefMut<'_, PyPythonTask>>) -> PyResult<PyPythonTask> {
        if tasks.is_empty() {
            return Err(PyValueError::new_err("Cannot select from empty task list"));
        }

        let mut futures = Vec::new();
        for task_ref in tasks.iter_mut() {
            futures.push(task_ref.take_task()?);
        }

        PyPythonTask::new(async move {
            let (result, index, _remaining) = futures::future::select_all(futures).await;
            result.map(|r| (r, index))
        })
    }

    /// Sleep for `seconds` on the Tokio runtime.
    #[staticmethod]
    fn sleep(seconds: f64) -> PyResult<PyPythonTask> {
        PyPythonTask::new(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(seconds)).await;
            Ok(())
        })
    }

    /// Support `PythonTask[T]` type syntax on the Python side (no
    /// runtime effect).
    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _arg: Py<PyAny>) -> Py<PyAny> {
        cls.clone().unbind().into()
    }
}

/// Awaitable handle to a spawned background Tokio task.
///
/// `Shared` is returned by `PythonTask.spawn()`. It carries a `watch` receiver that is
/// fulfilled exactly once with the task's `PyResult<Py<PyAny>>`.
///
/// Usage:
///   - `await shared` inside the `PythonTask.from_coroutine(...)`
///     world, or
///   - `shared.block_on()` to wait synchronously.
///
/// Dropping this object does not cancel the underlying Tokio task.
#[pyclass(
    name = "Shared",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
pub struct PyShared {
    /// The watch-channel core.
    core: HandleCore,
}

impl PyShared {
    /// Await this `Shared`'s result from Rust, yielding the resolved
    /// `Py<PyAny>` (or the producer's `PyErr`).
    ///
    /// Returns the same future the Python `await` path drives, so a Rust
    /// caller in another crate can resolve a `Shared[T]` inside an `async fn`
    /// without a pickle round-trip or a blocking `block_on`.
    pub fn wait_future(&self) -> impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static {
        self.core.wait_future()
    }
}

#[pymethods]
impl PyShared {
    /// Convert this `Shared` handle into a `PythonTask` that waits
    /// for its result.
    ///
    /// Internally, this clones the `watch::Receiver` and returns a
    /// new one-shot task that:
    ///   1) waits for the sender to publish `Some(result)`, and then
    ///   2) returns/clones the stored `Py<PyAny>` / `PyErr` under the
    ///      GIL.
    ///
    /// Cloning the receiver allows multiple independent awaiters to
    /// observe the same completion.
    pub(crate) fn task(&self) -> PyResult<PyPythonTask> {
        PyPythonTask::new_with_traceback(self.core.wait_future(), self.core.traceback_clone())
    }

    /// Implement Python's `await` protocol for `Shared`.
    ///
    /// This delegates to `self.task()` (which returns a `PythonTask`
    /// that waits for the background result) and then returns that
    /// task's await-iterator.
    ///
    /// Note: `await shared` is only supported inside the
    /// `PythonTask.from_coroutine(...)` world (because it ultimately
    /// awaits a `PythonTask`).
    fn __await__(&self, py: Python<'_>) -> PyResult<PythonTaskAwaitIterator> {
        let task = self.task()?;
        Ok(PythonTaskAwaitIterator::new(task.into_py_any(py)?))
    }

    /// Wait synchronously for this `Shared` to resolve.
    ///
    /// This blocks the calling Python thread until the underlying
    /// background task has published its result into the watch
    /// channel, then returns that `Py<PyAny>` (or raises the stored
    /// Python exception).
    ///
    /// If the value is already available, returns immediately without
    /// blocking. This is important for cases where `block_on` is called
    /// from within a tokio runtime (e.g., during unpickling on a worker
    /// thread) - we can't call `runtime.block_on()` from within a runtime.
    pub fn block_on(slf: PyRef<PyShared>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Check if value is already available - return immediately if so.
        // This avoids calling into the tokio runtime when unnecessary,
        // which is critical when called from within a tokio worker thread.
        if let Some(value) = slf.poll()? {
            return Ok(value);
        }

        let wait = slf.core.wait_future();
        // Explicitly drop the reference so that if another thread attempts to borrow
        // this object mutably during signal_safe_block_on, it won't throw an exception.
        drop(slf);
        signal_safe_block_on(py, wait)?
    }

    /// Support `Shared[T]` type syntax on the Python side (no runtime
    /// effect).
    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _arg: Py<PyAny>) -> Py<PyAny> {
        cls.clone().unbind().into()
    }

    /// Non-blocking check for completion.
    ///
    /// Returns:
    ///   - `Ok(None)` if the background task has not finished yet,
    ///   - `Ok(Some(obj))` if it completed successfully,
    ///   - `Err(pyerr)` if it completed with an exception.
    ///
    /// This does not wait; it only inspects the current watch value.
    pub(crate) fn poll(&self) -> PyResult<Option<Py<PyAny>>> {
        self.core.poll()
    }

    /// Construct a `Shared` that is already completed with `value`.
    ///
    /// This is a convenience for APIs that want to return a `Shared`
    /// without spawning a background task. The returned handle has no
    /// `JoinHandle` and will immediately yield `value` via `poll()`,
    /// `await` (inside `from_coroutine`), or `block_on()`.
    #[classmethod]
    fn from_value(_cls: &Bound<'_, PyType>, value: Py<PyAny>) -> PyResult<Self> {
        Ok(Self {
            core: HandleCore::from_value(value)?,
        })
    }

    /// Pickle protocol support for PyShared.
    ///
    /// Delegates to `reduce_shared`: a finished shared pickles as
    /// `(Shared.from_value, (value,))`; a pending one blocks on the shared and
    /// then pickles the resolved value. Mesh references do not take this generic
    /// path -- their own reducers record a `MeshRef` in the message's
    /// out-of-band `refs` table, a pending mesh's slot filled sender-side (by
    /// awaiting the handle) before the send.
    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyTuple>)> {
        reduce_shared(py, slf)
    }
}

/// Register the pytokio Python bindings into the given module.
///
/// This wires up the legacy `PythonTask` and `Shared` pyclasses.
pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyPythonTask>()?;
    hyperactor_mod.add_class::<PyShared>()?;

    Ok(())
}

#[cfg(test)]
pub(crate) use crate::runtime::ensure_python;

#[cfg(test)]
mod tests {
    use super::*;

    // with_timeout validates the seconds up front, raising ValueError for a
    // negative/NaN/non-finite timeout rather than panicking in
    // Duration::from_secs_f64 on a worker thread (matching Handle.get(timeout)).
    #[test]
    fn with_timeout_rejects_invalid_seconds() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            for bad in [-1.0, f64::NAN, f64::INFINITY] {
                let mut task = PyPythonTask::sleep(3600.0).unwrap();
                let err = task
                    .with_timeout(bad)
                    .err()
                    .expect("with_timeout should reject an invalid timeout");
                assert!(
                    err.is_instance_of::<PyValueError>(py),
                    "with_timeout({bad}) should raise ValueError, not panic"
                );
            }
        });
    }
}
