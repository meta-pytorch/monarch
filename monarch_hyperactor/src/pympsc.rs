/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A mpsc channel that can is used to send messages from Rust to Python without acquiring
//! the GIL on the sender side.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;

use monarch_types::MapPyErr;
use pyo3::Bound;
use pyo3::IntoPyObject;
use pyo3::IntoPyObjectExt;
use pyo3::Py;
use pyo3::PyAny;
use pyo3::PyResult;
use pyo3::Python;
use pyo3::pyclass;
use pyo3::pymethods;
use pyo3::types::PyModule;
use pyo3::types::PyModuleMethods;
use pyo3::wrap_pyfunction;

use crate::py_cell::PyCell;
use crate::pywaker::PyEvent;
use crate::pywaker::{self};

/// Create a new channel with a Rust sender and a Python receiver.
pub fn channel() -> Result<(Sender, PyReceiver), nix::Error> {
    let (tx, rx) = mpsc::channel();
    let rx = Arc::new(Mutex::new(rx));

    let (waker, event) = pywaker::event()?;

    Ok((
        Sender {
            tx,
            waker: Arc::new(waker),
        },
        PyReceiver {
            rx,
            event: PyCell::new(event),
        },
    ))
}

/// A blanket trait used to convert boxed objects into python objects.
pub trait IntoPyObjectBox: Send {
    fn into_py_object(self: Box<Self>, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl<T> IntoPyObjectBox for T
where
    T: for<'py> IntoPyObject<'py> + Send,
{
    fn into_py_object(self: Box<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        (*self).into_py_any(py)
    }
}

/// Error type for send operations
#[derive(Debug)]
pub struct SendError;

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SendError")
    }
}

impl std::error::Error for SendError {}

/// A channel that can be used to send messages from Rust to Python without acquiring
/// the GIL on the sender side.
#[derive(Clone)]
pub struct Sender {
    tx: mpsc::Sender<Box<dyn IntoPyObjectBox>>,
    waker: Arc<pywaker::Waker>,
}

impl std::fmt::Debug for Sender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl Sender {
    /// Send a message to the channel. The object must be convertible to a Python object;
    /// conversion is deferred until the message is received in a Python context.
    pub fn send<T>(&self, msg: T) -> Result<(), SendError>
    where
        T: IntoPyObjectBox + Send + 'static,
    {
        self.tx.send(Box::new(msg)).map_err(|_| SendError)?;
        let _ = self.waker.wake();
        Ok(())
    }
}

/// The receiver side of a channel. Objects are converted to Python heap objects when
/// they are received.
#[pyclass(name = "Receiver", module = "monarch._src.actor.mpsc")]
pub struct PyReceiver {
    rx: Arc<Mutex<mpsc::Receiver<Box<dyn IntoPyObjectBox>>>>,
    event: PyCell<PyEvent>,
}

impl std::fmt::Debug for PyReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyReceiver").finish_non_exhaustive()
    }
}

#[pymethods]
impl PyReceiver {
    fn try_recv(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.rx.lock().unwrap().try_recv() {
            Ok(boxed_msg) => Ok(Some(boxed_msg.into_py_object(py)?)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                Err(pyo3::exceptions::PyEOFError::new_err("Channel closed"))
            }
        }
    }

    fn _event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyEvent>> {
        let py_event = self.event.clone_ref(py)?;
        Ok(py_event.bind(py).clone())
    }
}

mod testing {
    use pyo3::pyfunction;
    use pyo3::types::PyAnyMethods;

    use super::*;

    // NOTE: We can't use a Python calss name that starts with "Test" since
    // during Python testing, Pytest will inspect anything that starts with
    // "Test" and check if its callable which in pyo3 >= 0.26 will raise
    // a TypeError.
    #[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.pympsc")]
    struct PyTestSender {
        sender: Arc<Mutex<Sender>>,
    }

    #[pymethods]
    impl PyTestSender {
        fn send(&self, _py: Python<'_>, obj: Py<PyAny>) -> PyResult<()> {
            self.sender.lock().unwrap().send(obj).map_pyerr()?;
            Ok(())
        }
    }

    #[pyfunction]
    fn channel_for_test(_py: Python<'_>) -> PyResult<(PyTestSender, PyReceiver)> {
        let (tx, rx) = channel().map_pyerr()?;
        let tx = PyTestSender {
            sender: Arc::new(Mutex::new(tx)),
        };
        Ok((tx, rx))
    }

    pub(super) fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
        hyperactor_mod.add_class::<PyTestSender>()?;
        let channel_for_test = wrap_pyfunction!(channel_for_test, hyperactor_mod)?;
        channel_for_test.setattr(
            "__module__",
            "monarch._rust_bindings.monarch_hyperactor.pympsc",
        )?;

        hyperactor_mod.add_function(channel_for_test)?;
        Ok(())
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyReceiver>()?;
    testing::register_python_bindings(hyperactor_mod)?;
    Ok(())
}
