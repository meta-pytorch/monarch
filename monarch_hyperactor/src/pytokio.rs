/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// Pytokio allows Python coroutines to await Rust futures, in specific contexts.
///
/// A PythonTask is constructed in Python from `PythonTask.from_coroutine()`:
///
/// ```ignore
/// async def task():
///     # ... async work, await other python tasks
/// task = PythonTask.from_coroutine(coro=task())
/// ```
///
/// The task may only await *other* PythonTasks; it is an error to await arbitrary
/// Python awaitables. In this way, Pytokio is a way to use Python to compose Tokio futures.
///
/// A task can be spawned in order to produce an awaitable that can be awaited in
/// any async context:
///
/// ```ignore
/// shared = task.spawn()
/// result = await shared
/// ```
///
/// Spawn spawns a tokio task that drives the coroutine to completion, and, using the Python
/// awaitable protocol, allows those coroutines to await other Tokio futures in turn.
///
/// PythonTasks can also be awaited synchronously by `block_on`:
///
/// ```ignore
/// result = task.block_on()
/// ```
///
/// This allows PythonTasks to be used in either async or sync contexts -- the underlying
/// code executes in exactly the same way, driven by an underlying tokio task.
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use hyperactor::attrs::declare_attrs;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::config;
use hyperactor::config::CONFIG;
use hyperactor::config::ConfigAttr;
use monarch_types::SerializablePyErr;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyNone;
use pyo3::types::PyString;
use pyo3::types::PyType;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::runtime::get_tokio_runtime;
use crate::runtime::signal_safe_block_on;

declare_attrs! {
    /// If true, when a pytokio PythonTask fails, the traceback of the original callsite
    /// will be logged.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("MONARCH_HYPERACTOR_UNAWAITED_PYTOKIO_TRACEBACK".to_string()),
        py_name: Some("unawaited_pytokio_traceback".to_string()),
    })
    pub attr UNAWAITED_PYTOKIO_TRACEBACK: u8 = 0;
}

fn current_traceback() -> PyResult<Option<PyObject>> {
    if config::global::get(UNAWAITED_PYTOKIO_TRACEBACK) != 0 {
        Python::with_gil(|py| {
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

fn format_traceback(py: Python<'_>, traceback: &PyObject) -> PyResult<String> {
    let tb = py
        .import("traceback")?
        .call_method1("format_list", (traceback,))?;
    PyString::new(py, "")
        .call_method1("join", (tb,))?
        .extract::<String>()
}

/// Helper struct to make a Python future passable in an actor message.
///
/// Also so that we don't have to write this massive type signature everywhere
pub(crate) struct PythonTask {
    future: Mutex<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send + 'static>>>,
    traceback: Option<PyObject>,
}

impl PythonTask {
    fn new_with_traceback(
        fut: impl Future<Output = PyResult<PyObject>> + Send + 'static,
        traceback: Option<PyObject>,
    ) -> Self {
        Self {
            future: Mutex::new(Box::pin(fut)),
            traceback,
        }
    }

    pub(crate) fn new(
        fut: impl Future<Output = PyResult<PyObject>> + Send + 'static,
    ) -> PyResult<Self> {
        Ok(Self::new_with_traceback(fut, current_traceback()?))
    }

    fn traceback(&self) -> &Option<PyObject> {
        &self.traceback
    }

    pub(crate) fn take(self) -> Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send + 'static>> {
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

#[pyclass(
    name = "PythonTaskAwaitIterator",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
struct PythonTaskAwaitIterator {
    value: Option<PyObject>,
}

impl PythonTaskAwaitIterator {
    fn new(task: PyObject) -> PythonTaskAwaitIterator {
        PythonTaskAwaitIterator { value: Some(task) }
    }
}

#[pymethods]
impl PythonTaskAwaitIterator {
    fn send(&mut self, value: PyObject) -> PyResult<PyObject> {
        self.value
            .take()
            .ok_or_else(|| PyStopIteration::new_err((value,)))
    }
    fn throw(&mut self, value: PyObject) -> PyResult<PyObject> {
        Err(Python::with_gil(|py| {
            PyErr::from_value(value.into_bound(py))
        }))
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        self.send(py.None())
    }
}

impl PyPythonTask {
    fn new_with_traceback<F, T>(fut: F, traceback: Option<PyObject>) -> PyResult<Self>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py>,
    {
        Ok(PythonTask::new_with_traceback(
            async {
                fut.await
                    .and_then(|t| Python::with_gil(|py| t.into_py_any(py)))
            },
            traceback,
        )
        .into())
    }

    pub fn new<F, T>(fut: F) -> PyResult<Self>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py>,
    {
        Self::new_with_traceback(fut, current_traceback()?)
    }
}

fn to_py_error<T>(e: T) -> PyErr
where
    T: Error,
{
    PyErr::new::<PyValueError, _>(e.to_string())
}

impl PyPythonTask {
    pub(crate) fn take_task(
        &mut self,
    ) -> PyResult<Pin<Box<dyn Future<Output = Result<Py<PyAny>, PyErr>> + Send + 'static>>> {
        self.inner
            .take()
            .map(|task| task.take())
            .ok_or_else(|| PyValueError::new_err("PythonTask already consumed"))
    }

    fn traceback(&self) -> PyResult<Option<PyObject>> {
        if let Some(task) = &self.inner {
            Ok(Python::with_gil(|py| {
                task.traceback().as_ref().map(|t| t.clone_ref(py))
            }))
        } else {
            Err(PyValueError::new_err("PythonTask already consumed"))
        }
    }

    /// Prefer spawn_abortable over spawn if the future can be safely cancelled
    /// when it is dropped.
    /// This way any resources it is using will be freed up. This is especially
    /// important for potentially infinite tasks that will never complete on their
    /// own.
    /// An example of this could be a timer task that periodically wakes up.
    /// Without spawn_abortable, that task would run forever even if the returned
    /// PyShared is dropped.
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
            handle,
            abort: true,
            traceback,
        })
    }
}

fn send_result(
    tx: tokio::sync::watch::Sender<Option<PyResult<PyObject>>>,
    result: PyResult<PyObject>,
    traceback: Option<PyObject>,
) {
    // a SendErr just means that there are no consumers of the value left.
    match tx.send(Some(result)) {
        Err(tokio::sync::watch::error::SendError(Some(Err(pyerr)))) => {
            Python::with_gil(|py| {
                let tb = if let Some(tb) = traceback {
                    format_traceback(py, &tb).unwrap()
                } else {
                    "None (run with `MONARCH_HYPERACTOR_UNAWAITED_PYTOKIO_TRACEBACK=1` to see a traceback here)\n".into()
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
    fn block_on(mut slf: PyRefMut<PyPythonTask>, py: Python<'_>) -> PyResult<PyObject> {
        let task = slf.take_task()?;

        // mutable references to python objects must be dropped before calling
        // signal_safe_block_on. It will release the GIL, and any other thread
        // trying to access slf will throw.
        drop(slf);
        signal_safe_block_on(py, task)?
    }

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
            handle,
            abort: false,
            traceback,
        })
    }

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

    #[staticmethod]
    fn from_coroutine(py: Python<'_>, coro: PyObject) -> PyResult<PyPythonTask> {
        // context() used inside a PythonTask should inherit the value of
        // context() from the context in which the PythonTask was constructed.
        // We need to do this manually because the value of the contextvar isn't
        // maintained inside the tokio runtime.
        let monarch_context = py
            .import("monarch._src.actor.actor_mesh")?
            .call_method0("context")?
            .unbind();
        PyPythonTask::new(async move {
            let (coroutine_iterator, none) = Python::with_gil(|py| {
                coro.into_bound(py)
                    .call_method0("__await__")
                    .map(|x| (x.unbind(), py.None()))
            })?;
            let mut last: PyResult<PyObject> = Ok(none);
            enum Action {
                Return(PyObject),
                Wait(Pin<Box<dyn Future<Output = Result<Py<PyAny>, PyErr>> + Send + 'static>>),
            }
            loop {
                let action: PyResult<Action> = Python::with_gil(|py| {
                    // We may be executing in a new thread at this point, so we need to set the value
                    // of context().
                    let _context = py
                        .import("monarch._src.actor.actor_mesh")?
                        .getattr("_context")?;
                    let old_context = _context.call_method1("get", (PyNone::get(py),))?;
                    _context.call_method1("set", (monarch_context.clone_ref(py),))?;

                    let result = match last {
                        Ok(value) => coroutine_iterator.bind(py).call_method1("send", (value,)),
                        Err(pyerr) => coroutine_iterator
                            .bind(py)
                            .call_method1("throw", (pyerr.into_value(py),)),
                    };

                    // Reset context() so that when this tokio thread yields, it has its original state.
                    _context.call_method1("set", (old_context,))?;
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
                });
                match action? {
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

    #[staticmethod]
    fn spawn_blocking(py: Python<'_>, f: PyObject) -> PyResult<PyShared> {
        let (tx, rx) = watch::channel(None);
        let traceback = current_traceback()?;
        let traceback1 = traceback
            .as_ref()
            .map_or_else(|| None, |t| Python::with_gil(|py| Some(t.clone_ref(py))));
        let monarch_context = py
            .import("monarch._src.actor.actor_mesh")?
            .call_method0("context")?
            .unbind();
        // The `_context` contextvar needs to be propagated through to the thread that
        // runs the blocking tokio task. Upon completion, the original value of `_context`
        // is restored.
        let handle = get_tokio_runtime().spawn_blocking(move || {
            let result = Python::with_gil(|py| {
                let _context = py
                    .import("monarch._src.actor.actor_mesh")?
                    .getattr("_context")?;
                let old_context = _context.call_method1("get", (PyNone::get(py),))?;
                _context.call_method1("set", (monarch_context.clone_ref(py),))?;
                let result = f.call0(py);
                _context.call_method1("set", (old_context,))?;
                result
            });
            send_result(tx, result, traceback1);
        });
        Ok(PyShared {
            rx,
            handle,
            abort: false,
            traceback,
        })
    }

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

    #[staticmethod]
    fn sleep(seconds: f64) -> PyResult<PyPythonTask> {
        PyPythonTask::new(async move {
            RealClock
                .sleep(tokio::time::Duration::from_secs_f64(seconds))
                .await;
            Ok(())
        })
    }

    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _arg: PyObject) -> PyObject {
        cls.clone().unbind().into()
    }
}

#[pyclass(
    name = "Shared",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
pub struct PyShared {
    rx: watch::Receiver<Option<PyResult<PyObject>>>,
    handle: JoinHandle<()>,
    abort: bool,
    traceback: Option<PyObject>,
}

impl Drop for PyShared {
    fn drop(&mut self) {
        if self.abort {
            // When the PyShared is dropped, we don't want the background task to go
            // forever, because nothing will wait on the rx.
            self.handle.abort();
        }
    }
}

#[pymethods]
impl PyShared {
    pub(crate) fn task(&mut self) -> PyResult<PyPythonTask> {
        // watch channels start unchanged, and when a value is sent to them signal
        // the receivers `changed` future.
        // By cloning the rx before awaiting it,
        // we can have multiple awaiters get triggered by the same change.
        // self.rx will always be in the state where it hasn't see the change yet.
        let mut rx = self.rx.clone();
        PyPythonTask::new_with_traceback(
            async move {
                rx.changed().await.map_err(to_py_error)?;
                let b = rx.borrow();
                let r = b.as_ref().unwrap();
                Python::with_gil(|py| match r {
                    Ok(v) => Ok(v.bind(py).clone().unbind()),
                    Err(err) => Err(err.clone_ref(py)),
                })
            },
            self.traceback
                .as_ref()
                .map_or_else(|| None, |t| Python::with_gil(|py| Some(t.clone_ref(py)))),
        )
    }

    fn __await__(&mut self, py: Python<'_>) -> PyResult<PythonTaskAwaitIterator> {
        let task = self.task()?;
        Ok(PythonTaskAwaitIterator::new(task.into_py_any(py)?))
    }

    pub fn block_on(mut slf: PyRefMut<PyShared>, py: Python<'_>) -> PyResult<PyObject> {
        let task = slf.task()?.take_task()?;
        // mutable references to python objects must be dropped before calling
        // signal_safe_block_on. It will release the GIL, and any other thread
        // trying to access slf will throw.
        drop(slf);
        signal_safe_block_on(py, task)?
    }

    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _arg: PyObject) -> PyObject {
        cls.clone().unbind().into()
    }
}

#[pyfunction]
fn is_tokio_thread() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyPythonTask>()?;
    hyperactor_mod.add_class::<PyShared>()?;
    let f = wrap_pyfunction!(is_tokio_thread, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.pytokio",
    )?;
    hyperactor_mod.add_function(f)?;

    Ok(())
}
