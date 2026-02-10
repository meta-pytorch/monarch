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
/// - Calling `task.spawn()` / `spawn_abortable()` returns a `Shared`
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
/// If `spawn_abortable()` is used, dropping the returned `Shared`
/// aborts the underlying Tokio task.
///
/// ## Context propagation
///
/// `from_coroutine` preserves Monarch’s `context()` across Tokio
/// thread hops, so code calling `context()` inside a `PythonTask`
/// sees the same actor context as the call site that constructed the
/// task.
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use monarch_types::SerializablePyErr;
use monarch_types::py_global;
use pyo3::IntoPyObjectExt;
#[cfg(test)]
use pyo3::PyClass;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyNone;
use pyo3::types::PyString;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use serde_multipart::Part;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::buffers::Buffer;
use crate::buffers::FrozenBuffer;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on;

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
        monarch_with_gil_blocking(|py| {
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

/// Format a captured traceback (from `traceback.extract_stack()`) as
/// a single string suitable for logging.
fn format_traceback(py: Python<'_>, traceback: &Py<PyAny>) -> PyResult<String> {
    let tb = py
        .import("traceback")?
        .call_method1("format_list", (traceback,))?;
    PyString::new(py, "")
        .call_method1("join", (tb,))?
        .extract::<String>()
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
/// it is run (e.g. via `spawn()`, `spawn_abortable()`, or
/// `block_on()`), hence `inner: Option<_>`.
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
        Err(monarch_with_gil_blocking(|py| {
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
                monarch_with_gil(|py| result.into_py_any(py)).await
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

// Helper: convert a Rust error into a generic Python ValueError.
fn to_py_error<T>(e: T) -> PyErr
where
    T: Error,
{
    PyErr::new::<PyValueError, _>(e.to_string())
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
            Ok(monarch_with_gil_blocking(|py| {
                task.traceback().as_ref().map(|t| t.clone_ref(py))
            }))
        } else {
            Err(PyValueError::new_err("PythonTask already consumed"))
        }
    }

    /// Spawn this task onto the Tokio runtime and return a `Shared`
    /// handle that *aborts on drop*.
    ///
    /// Use this when the underlying future is *abort-safe*
    /// (cancellation-safe): dropping the returned `Shared` will call
    /// `JoinHandle::abort()`, preventing the background task from
    /// running forever.
    ///
    /// This is especially useful for long-lived or periodic tasks
    /// (e.g. timers) where "nobody is awaiting the result anymore"
    /// should stop the work.
    ///
    /// Like `spawn()`, this consumes the `PyPythonTask` (it can only
    /// be spawned once).
    pub(crate) fn spawn_abortable(&mut self) -> PyResult<PyShared> {
        let (tx, rx) = watch::channel(None);
        let traceback = self.traceback()?;
        let traceback1 = self.traceback()?;
        let task = self.take_task()?;
        let handle = get_tokio_runtime().spawn(async move {
            send_result(tx, task.await, traceback1);
        });
        Ok(PyShared {
            rx,
            handle: Some(handle),
            abort: true,
            traceback,
        })
    }
}

/// Publish a completed task result to the `watch` channel.
///
/// If the receiver has already been dropped, `watch::Sender::send`
/// returns the unsent value as `SendError`. We treat that as "nobody
/// will ever observe this result".
///
/// In the special case where the unobserved result is an error, we
/// log it (and include the task creation traceback when available) to
/// avoid silently losing failures from background tasks.
fn send_result(
    tx: tokio::sync::watch::Sender<Option<PyResult<Py<PyAny>>>>,
    result: PyResult<Py<PyAny>>,
    traceback: Option<Py<PyAny>>,
) {
    // a SendErr just means that there are no consumers of the value left.
    match tx.send(Some(result)) {
        Err(tokio::sync::watch::error::SendError(Some(Err(pyerr)))) => {
            monarch_with_gil_blocking(|py| {
                let tb = if let Some(tb) = traceback {
                    format_traceback(py, &tb).unwrap()
                } else {
                    "None (run with `MONARCH_HYPERACTOR_ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK=1` to see a traceback here)\n".into()
                };
                tracing::error!(
                    "PythonTask errored but is not being awaited; this will not crash your program, but indicates that \
                    something went wrong.\n{}\nTraceback where the task was created (most recent call last):\n{}",
                    SerializablePyErr::from(py, &pyerr),
                    tb
                );
            });
        }
        _ => {}
    };
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
        let (tx, rx) = watch::channel(None);
        let traceback = self.traceback()?;
        let traceback1 = self.traceback()?;
        let task = self.take_task()?;
        let handle = get_tokio_runtime().spawn(async move {
            send_result(tx, task.await, traceback1);
        });
        Ok(PyShared {
            rx,
            handle: Some(handle),
            abort: false,
            traceback,
        })
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
            let (coroutine_iterator, none) = monarch_with_gil(|py| {
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
                let action = monarch_with_gil(|py| -> PyResult<Action> {
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
        let tb = self.traceback()?;
        let task = self.take_task()?;
        PyPythonTask::new_with_traceback(
            async move {
                RealClock
                    .timeout(std::time::Duration::from_secs_f64(seconds), task)
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
        let traceback1 = traceback.as_ref().map_or_else(
            || None,
            |t| monarch_with_gil_blocking(|py| Some(t.clone_ref(py))),
        );
        let monarch_context = context(py).call0()?.unbind();
        // The `_context` contextvar needs to be propagated through to the thread that
        // runs the blocking tokio task. Upon completion, the original value of `_context`
        // is restored.
        let handle = get_tokio_runtime().spawn_blocking(move || {
            let result = monarch_with_gil_blocking(|py| {
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
            rx,
            handle: Some(handle),
            abort: false,
            traceback,
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
            RealClock
                .sleep(tokio::time::Duration::from_secs_f64(seconds))
                .await;
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
/// `Shared` is returned by `PythonTask.spawn()` /
/// `spawn_abortable()`. It carries a `watch` receiver that is
/// fulfilled exactly once with the task's `PyResult<Py<PyAny>>`.
///
/// Usage:
///   - `await shared` inside the `PythonTask.from_coroutine(...)`
///     world, or
///   - `shared.block_on()` to wait synchronously.
///
/// If `abort` is true (from `spawn_abortable()`), dropping this
/// object aborts the underlying Tokio task via its `JoinHandle`.
#[pyclass(
    name = "Shared",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
pub struct PyShared {
    /// One-shot result channel. Starts as `None`; becomes
    /// `Some(Ok(obj))` or `Some(Err(pyerr))` when the background task
    /// completes.
    rx: watch::Receiver<Option<PyResult<Py<PyAny>>>>,

    /// Handle for the spawned Tokio task that is producing `rx`’s
    /// result. `None` for `Shared.from_value(...)`.
    handle: Option<JoinHandle<()>>,

    /// If true, dropping `Shared` aborts the background task via
    /// `handle.abort()`. This is set by `spawn_abortable()`.
    abort: bool,

    /// Optional creation-site traceback (captured when enabled) used
    /// when logging un-awaited errors / for derived tasks.
    traceback: Option<Py<PyAny>>,
}

/// If this `Shared` was created via `spawn_abortable()`, abort the
/// underlying Tokio task on drop.
///
/// This prevents abandoned background work from running forever when
/// no receivers remain. We guard against panics during interpreter
/// shutdown / runtime teardown.
impl Drop for PyShared {
    fn drop(&mut self) {
        if self.abort {
            // When the PyShared is dropped, we don't want the background task to go
            // forever, because nothing will wait on the rx.
            if let Some(h) = self.handle.as_ref() {
                // Guard against panics during interpreter shutdown when tokio runtime may be gone
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    h.abort();
                }));
            }
        }
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
        // watch channels start unchanged, and when a value is sent to them signal
        // the receivers `changed` future.
        // By cloning the rx before awaiting it,
        // we can have multiple awaiters get triggered by the same change.
        // self.rx will always be in the state where it hasn't see the change yet.
        let mut rx = self.rx.clone();
        PyPythonTask::new_with_traceback(
            async move {
                // Check if a value is already available (not None).
                // The channel is initialized with None, and the sender sets it to Some(result).
                // If it's still None, wait for a change. Otherwise, the value is ready.
                if rx.borrow().is_none() {
                    rx.changed().await.map_err(to_py_error)?;
                }
                // We need to hold the GIL when cloning Python objects (Py<PyAny> and PyErr).
                monarch_with_gil(|py| {
                    let borrowed = rx.borrow();
                    match borrowed.as_ref().unwrap() {
                        Ok(v) => Ok(v.bind(py).clone().unbind()),
                        Err(err) => Err(err.clone_ref(py)),
                    }
                })
                .await
            },
            self.traceback.as_ref().map_or_else(
                || None,
                |t| monarch_with_gil_blocking(|py| Some(t.clone_ref(py))),
            ),
        )
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
    fn __await__(&mut self, py: Python<'_>) -> PyResult<PythonTaskAwaitIterator> {
        let task = self.task()?;
        Ok(PythonTaskAwaitIterator::new(task.into_py_any(py)?))
    }

    /// Wait synchronously for this `Shared` to resolve.
    ///
    /// This blocks the calling Python thread until the underlying
    /// background task has published its result into the watch
    /// channel, then returns that `Py<PyAny>` (or raises the stored
    /// Python exception).
    pub fn block_on(slf: PyRef<PyShared>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let task = slf.task()?.take_task()?;
        // Explicitly drop the reference so that if another thread attempts to borrow
        // this object mutably during signal_safe_block_on, it won't throw an exception.
        drop(slf);
        signal_safe_block_on(py, task)?
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
        let b = self.rx.borrow();
        let r = b.as_ref();
        match r {
            None => Ok(None),
            Some(r) => Python::attach(|py| match r {
                Ok(v) => Ok(Some(v.clone_ref(py))),
                Err(err) => Err(err.clone_ref(py)),
            }),
        }
    }

    /// Construct a `Shared` that is already completed with `value`.
    ///
    /// This is a convenience for APIs that want to return a `Shared`
    /// without spawning a background task. The returned handle has no
    /// `JoinHandle` and will immediately yield `value` via `poll()`,
    /// `await` (inside `from_coroutine`), or `block_on()`.
    #[classmethod]
    fn from_value(_cls: &Bound<'_, PyType>, value: Py<PyAny>) -> PyResult<Self> {
        let (tx, rx) = watch::channel(None);
        tx.send(Some(Ok(value))).map_err(to_py_error)?;
        Ok(Self {
            rx,
            handle: None,
            abort: false,
            traceback: None,
        })
    }
}

/// Return true if the current thread is executing within a Tokio
/// runtime context.
///
/// This checks whether `tokio::runtime::Handle::try_current()`
/// succeeds.
#[pyfunction]
fn is_tokio_thread() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

/// Marker wrapper for a `Shared` whose value will be pickled later.
///
/// Some message payloads are pickled as part of sending/forwarding,
/// but certain values cannot be pickled yet because they depend on
/// async work (represented by a `Shared`). `PendingPickle` marks
/// those `Shared`s as *allowed* placeholders during an initial
/// `flatten(...)` pass.
///
/// Later, in an async context, we await the underlying `Shared`,
/// substitute its resolved value, and re-pickle the payload.
///
/// A plain `Shared` is *not* generally picklable; only `Shared`s
/// wrapped in `PendingPickle` participate in this deferred-pickling
/// protocol.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.pytokio")]
#[derive(Clone)]
pub(crate) struct PendingPickle(Py<PyShared>);

#[pymethods]
impl PendingPickle {
    /// Wrap an existing `Shared` as a deferred-pickling placeholder.
    #[new]
    pub(crate) fn new(py_shared: Py<PyShared>) -> PyResult<Self> {
        Ok(Self(py_shared))
    }
}

impl PendingPickle {
    /// Convenience: create a deferred-pickling placeholder from a
    /// Rust future.
    ///
    /// Spawns the future as an abortable background task and wraps
    /// the resulting `Shared` in `PendingPickle`, making it eligible
    /// for the deferred pickling flow.
    pub(crate) fn from_future<F>(f: F) -> PyResult<Self>
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        let py_shared = PyPythonTask::new(f)?.spawn_abortable()?;
        Ok(Self(Python::attach(|py| {
            Ok::<_, PyErr>(py_shared.into_pyobject(py)?.unbind())
        })?))
    }

    /// Await the underlying `Shared` and return its resolved Python
    /// value.
    ///
    /// Used by `PendingPickleState::resolve` to substitute resolved
    /// values before re-pickling.
    pub(crate) async fn result(&self) -> PyResult<Py<PyAny>> {
        let mut task = Python::attach(|py| self.0.borrow(py).task())?;
        task.take_task()?.await
    }
}

// Python helper used to reconstruct an object graph from a pickled
// buffer plus a list of “unflatten values” (including placeholders).
py_global!(unflatten, "monarch._src.actor.pickle", "unflatten");

// Python helper used to pickle an object graph, optionally using a
// filter to replace certain values with placeholders (e.g.
// `PendingPickle`).
//
// We use `flatten`/`unflatten` to support “deferred pickling”:
// initially pickle with placeholders, then later resolve futures and
// re-pickle with concrete values.
py_global!(flatten, "monarch._src.actor.pickle", "flatten");

/// State captured during “deferred pickling”.
///
/// `flatten(...)` can be called with a Python-side filter that
/// replaces certain values with placeholders (notably
/// `PendingPickle`, i.e. a `Shared` that will eventually produce a
/// Python value). When that happens, `flatten` returns:
///
/// - a pickled payload that references an *unflatten list*, and
/// - the corresponding `unflatten_values` list (some entries may be
///   placeholders), plus the `flatten_filter` used to produce it.
///
/// `PendingPickleState` stores the pieces needed to finish the job
/// later: resolve the placeholders (by awaiting them) and then re-run
/// `flatten` so the final pickled payload contains the concrete
/// values.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.pytokio")]
#[derive(Debug, Clone)]
pub struct PendingPickleState {
    /// The `unflatten` value list captured from the initial `flatten`
    /// call. Some entries may be `PendingPickle` placeholders that
    /// must be resolved.
    unflatten_values: Vec<Py<PyAny>>,

    /// The filter object originally passed to `flatten`, used when
    /// re-pickling after placeholders have been resolved.
    flatten_filter: Py<PyAny>,
}

#[pymethods]
impl PendingPickleState {
    /// Construct a deferred-pickling state object from `flatten`
    /// outputs.
    #[new]
    fn new(unflatten_values: Vec<Py<PyAny>>, flatten_filter: Py<PyAny>) -> Self {
        Self {
            unflatten_values,
            flatten_filter,
        }
    }
}

impl PendingPickleState {
    /// Finish deferred pickling.
    ///
    /// Takes the "pre-pickled" payload produced by the initial
    /// `flatten` call, awaits any `PendingPickle` placeholders stored
    /// in `unflatten_values`, then `unflatten`s the object graph and
    /// `flatten`s it again so the returned payload contains resolved
    /// values.
    pub(crate) async fn resolve(self, pickled: impl Into<Bytes>) -> PyResult<Part> {
        let (idxs, futs): (Vec<_>, Vec<_>) = Python::attach(|py| {
            self.unflatten_values
                .iter()
                .enumerate()
                .filter_map(|(i, py_obj)| {
                    py_obj.extract::<PendingPickle>(py).map_or(None, |pending| {
                        Some((i, async move { pending.result().await }))
                    })
                })
                .unzip()
        });

        // This really shouldn't happen. This `PendingPickleState` object
        // shouldn't have been created if there are no futures to resolve.
        if futs.is_empty() {
            return Ok(Part::from(pickled.into()));
        }

        let result = futures::future::join_all(futs)
            .await
            .into_iter()
            .collect::<PyResult<Vec<_>>>()?;

        let mut unflatten_values = Vec::with_capacity(self.unflatten_values.len());
        let mut fut_idx = 0;
        for i in 0..self.unflatten_values.len() {
            if idxs.get(fut_idx).is_some_and(|idx| *idx == i) {
                Python::attach(|py| {
                    unflatten_values.push(result[fut_idx].clone_ref(py));
                });
                fut_idx += 1;
            } else {
                Python::attach(|py| {
                    unflatten_values.push(self.unflatten_values[i].clone_ref(py));
                });
            }
        }

        Python::attach(|py| {
            let unpickled = unflatten(py).call1((
                FrozenBuffer {
                    inner: pickled.into(),
                },
                unflatten_values,
            ))?;
            let repickled = flatten(py)
                .call1((unpickled, self.flatten_filter))?
                .downcast_into::<PyTuple>()?;
            let buffer = repickled.get_item(1)?.downcast_into::<Buffer>()?;
            Ok(buffer.borrow_mut().take_part())
        })
    }
}

/// Register the pytokio Python bindings into the given module.
///
/// This wires up the exported pyclasses (`PythonTask`, `Shared`,
/// deferred pickling helpers) and module-level functions used by the
/// Monarch Python layer.
pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyPythonTask>()?;
    hyperactor_mod.add_class::<PyShared>()?;
    hyperactor_mod.add_class::<PendingPickle>()?;
    hyperactor_mod.add_class::<PendingPickleState>()?;
    let f = wrap_pyfunction!(is_tokio_thread, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.pytokio",
    )?;
    hyperactor_mod.add_function(f)?;

    Ok(())
}

/// Ensure the embedded Python interpreter is initialized exactly
/// once.
///
/// Safe to call from multiple threads, multiple times.
#[cfg(test)]
pub(crate) fn ensure_python() {
    static INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INIT.get_or_init(|| {
        pyo3::Python::initialize();
    });
}

#[cfg(test)]
// Helper: let us "await" a `PyPythonTask` in Rust.
//
// Semantics:
//   - consume the `PyPythonTask`,
//   - take the inner future,
//   - `.await` it on tokio to get `Py<PyAny>`,
//   - turn that into `Py<T>`.
pub(crate) trait AwaitPyExt {
    async fn await_py<T: PyClass>(self) -> Result<Py<T>, PyErr>;

    // For tasks whose future just resolves to (), i.e. no object,
    // just "did it work?"
    async fn await_unit(self) -> Result<(), PyErr>;
}

#[cfg(test)]
impl AwaitPyExt for PyPythonTask {
    async fn await_py<T: PyClass>(mut self) -> Result<Py<T>, PyErr> {
        // Take ownership of the inner future.
        let fut = self
            .take_task()
            .expect("PyPythonTask already consumed in await_py");

        // Await a Result<Py<PyAny>, PyErr>.
        let py_any: Py<PyAny> = fut.await?;

        // Convert Py<PyAny> -> Py<T>.
        monarch_with_gil(|py| {
            let bound_any = py_any.bind(py);

            // Try extract a Py<T>.
            let obj: Py<T> = bound_any
                .extract::<Py<T>>()
                .expect("spawn() did not return expected Python type");

            Ok(obj)
        })
        .await
    }

    async fn await_unit(mut self) -> Result<(), PyErr> {
        let fut = self
            .take_task()
            .expect("PyPythonTask already consumed in await_unit");

        // Await it. This still gives us a Py<PyAny> because
        // Python-side return values are always materialized as 'some
        // object'. For "no value" / None, that's just a PyAny(None).
        let py_any: Py<PyAny> = fut.await?;

        // We don't need to extract anything. Just drop it.
        drop(py_any);

        Ok(())
    }
}
