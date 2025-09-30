/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use monarch_types::SerializablePyErr;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyNone;
use pyo3::types::PyType;
use tokio::sync::Mutex;
use tokio::sync::watch;

use crate::runtime::get_tokio_runtime;
use crate::runtime::signal_safe_block_on;

/// Helper struct to make a Python future passable in an actor message.
///
/// Also so that we don't have to write this massive type signature everywhere
pub(crate) struct PythonTask {
    future: Mutex<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send + 'static>>>,
}

impl PythonTask {
    pub(crate) fn new(fut: impl Future<Output = PyResult<PyObject>> + Send + 'static) -> Self {
        Self {
            future: Mutex::new(Box::pin(fut)),
        }
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
    pub fn new<F, T>(fut: F) -> PyResult<Self>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py>,
    {
        Ok(PythonTask::new(async {
            fut.await
                .and_then(|t| Python::with_gil(|py| t.into_py_any(py)))
        })
        .into())
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
}

fn send_result(
    tx: tokio::sync::watch::Sender<Option<PyResult<PyObject>>>,
    result: PyResult<PyObject>,
) {
    // a SendErr just means that there are no consumers of the value left.
    match tx.send(Some(result)) {
        Err(tokio::sync::watch::error::SendError(Some(Err(pyerr)))) => {
            Python::with_gil(|py| {
                panic!(
                    "PythonTask errored but is not being awaited: {}",
                    SerializablePyErr::from(py, &pyerr)
                )
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
        let task = self.take_task()?;
        get_tokio_runtime().spawn(async move {
            send_result(tx, task.await);
        });
        Ok(PyShared { rx })
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
        let task = self.take_task()?;
        PyPythonTask::new(async move {
            RealClock
                .timeout(std::time::Duration::from_secs_f64(seconds), task)
                .await
                .map_err(|_| PyTimeoutError::new_err(()))?
        })
    }

    #[staticmethod]
    fn spawn_blocking(f: PyObject) -> PyResult<PyShared> {
        let (tx, rx) = watch::channel(None);
        get_tokio_runtime().spawn_blocking(move || {
            let result = Python::with_gil(|py| f.call0(py));
            send_result(tx, result);
        });
        Ok(PyShared { rx })
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
        PyPythonTask::new(async move {
            rx.changed().await.map_err(to_py_error)?;
            let b = rx.borrow();
            let r = b.as_ref().unwrap();
            Python::with_gil(|py| match r {
                Ok(v) => Ok(v.bind(py).clone().unbind()),
                Err(err) => Err(err.clone_ref(py)),
            })
        })
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
