/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Deferred pickling support for Monarch.
//!
//! This module provides utilities for deferring the pickling of objects
//! that contain async values (futures/tasks) that must be resolved before
//! the final pickle can be produced.

use std::cell::RefCell;
use std::collections::VecDeque;

use monarch_types::py_global;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::types::PyTuple;

use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::buffers::Buffer;
use crate::pytokio::PyShared;

// Python helper used to reconstruct an object graph from a pickled
// buffer plus a list of "unflatten values" (including placeholders).
py_global!(unflatten, "monarch._src.actor.pickle", "unflatten");

// Python helper used to pickle an object graph, optionally using a
// filter to replace certain values with placeholders (e.g.
// `PendingPickle`).
//
// We use `flatten`/`unflatten` to support "deferred pickling":
// initially pickle with placeholders, then later resolve futures and
// re-pickle with concrete values.
py_global!(flatten, "monarch._src.actor.pickle", "flatten");

// cloudpickle module for serialization
py_global!(cloudpickle, "cloudpickle", "cloudpickle");

// Importing monarch._src.actor.pickle applies a monkeypatch to cloudpickle
// that injects RemoteImportLoader into pickled function globals, enabling
// source loading for pickle-by-value code on remote hosts (needed for
// debugger and tracebacks). We access this before pickling to ensure
// the monkeypatch is applied.
py_global!(
    pickle_monkeypatch,
    "monarch._src.actor.pickle",
    "_function_getstate"
);

// Shared class for pickling PyShared values
py_global!(
    shared_class,
    "monarch._rust_bindings.monarch_hyperactor.pytokio",
    "Shared"
);

// pop_pending_pickle function for unpickling deferred PyShared values
py_global!(
    pop_pending_pickle_fn,
    "monarch._rust_bindings.monarch_hyperactor.pickle",
    "pop_pending_pickle"
);

// Thread-local storage for the active pickling state.
// Set by pickle/unpickle operations so free functions used in __reduce__
// implementations can access it.
thread_local! {
    static ACTIVE_PICKLING_STATE: RefCell<Option<ActivePicklingState>> = const { RefCell::new(None) };
}

/// State maintained during active pickling/unpickling operations.
///
/// This is the thread-local state used while cloudpickle is running.
/// It collects tensor engine references and pending pickles during serialization.
struct ActivePicklingState {
    /// References to tensor engine objects that need special handling.
    tensor_engine_references: VecDeque<Py<PyAny>>,
    /// Pending pickles (PyShared values) that must be resolved.
    pending_pickles: VecDeque<Py<PyShared>>,
    /// Whether pending pickles are allowed in this pickling context.
    allow_pending_pickles: bool,
    /// Whether tensor engine references are allowed in this pickling context.
    allow_tensor_engine_references: bool,
}

impl ActivePicklingState {
    /// Create a new ActivePicklingState.
    fn new(allow_pending_pickles: bool, allow_tensor_engine_references: bool) -> Self {
        Self {
            tensor_engine_references: VecDeque::new(),
            pending_pickles: VecDeque::new(),
            allow_pending_pickles,
            allow_tensor_engine_references,
        }
    }

    /// Convert this active state into a frozen PicklingState.
    fn into_pickling_state(self, buffer: crate::buffers::FrozenBuffer) -> PicklingStateInner {
        PicklingStateInner {
            buffer,
            tensor_engine_references: self.tensor_engine_references,
            pending_pickles: self.pending_pickles,
        }
    }
}

/// Inner data for a completed pickling operation.
///
/// This contains the frozen pickled bytes and any collected references.
/// Does not require GIL for access to the FrozenBuffer.
pub struct PicklingStateInner {
    /// The pickled bytes as a FrozenBuffer (zero-copy).
    buffer: crate::buffers::FrozenBuffer,
    /// References to tensor engine objects that need special handling.
    tensor_engine_references: VecDeque<Py<PyAny>>,
    /// Pending pickles (PyShared values) that must be resolved.
    pending_pickles: VecDeque<Py<PyShared>>,
}

impl PicklingStateInner {
    /// Get a reference to the pending pickles.
    pub fn pending_pickles(&self) -> &VecDeque<Py<PyShared>> {
        &self.pending_pickles
    }

    /// Take the FrozenBuffer (pickled bytes) from this inner state.
    pub fn take_buffer(self) -> crate::buffers::FrozenBuffer {
        self.buffer
    }
}

/// Python-visible wrapper for the result of a pickling operation.
///
/// Contains the pickled bytes and any tensor engine references or pending
/// pickles that were collected during serialization.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.pickle")]
pub struct PicklingState {
    inner: Option<PicklingStateInner>,
}

impl PicklingState {
    pub fn take_inner(&mut self) -> PyResult<PicklingStateInner> {
        self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("PicklingState has already been consumed")
        })
    }

    fn inner_ref(&self) -> PyResult<&PicklingStateInner> {
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("PicklingState has already been consumed")
        })
    }
}

#[pymethods]
impl PicklingState {
    /// Create a new PicklingState from a buffer and optional tensor engine references.
    ///
    /// This is used for unpickling received messages that may contain tensor engine
    /// references that need to be restored during deserialization.
    #[new]
    #[pyo3(signature = (buffer, tensor_engine_references=None))]
    fn py_new(
        buffer: PyRef<'_, crate::buffers::FrozenBuffer>,
        tensor_engine_references: Option<&Bound<'_, PyList>>,
    ) -> PyResult<Self> {
        let refs: VecDeque<Py<PyAny>> = tensor_engine_references
            .map(|list| list.iter().map(|item| item.unbind()).collect())
            .unwrap_or_default();

        Ok(Self {
            inner: Some(PicklingStateInner {
                buffer: buffer.clone(),
                tensor_engine_references: refs,
                pending_pickles: VecDeque::new(),
            }),
        })
    }

    /// Get a copy of all tensor engine references from this pickling state.
    ///
    /// Returns a Python list containing copies of the tensor engine references.
    fn tensor_engine_references(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let inner = self.inner_ref()?;
        let refs: Vec<Py<PyAny>> = inner
            .tensor_engine_references
            .iter()
            .map(|r| r.clone_ref(py))
            .collect();
        Ok(PyList::new(py, refs)?.unbind())
    }

    /// Get the buffer from this pickling state.
    ///
    /// Returns a FrozenBuffer containing the pickled bytes.
    /// This does not consume the PicklingState.
    fn buffer(&self) -> PyResult<crate::buffers::FrozenBuffer> {
        let inner = self.inner_ref()?;
        Ok(inner.buffer.clone())
    }

    /// Unpickle the buffer contents.
    ///
    /// This consumes the PicklingState. It will fail if there are any pending
    /// pickles that haven't been resolved.
    fn unpickle(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = self.take_inner()?;

        // Verify all pending pickles are resolved before unpickling
        for pending in &inner.pending_pickles {
            if pending.borrow(py).poll()?.is_none() {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot unpickle: there are unresolved pending pickles",
                ));
            }
        }

        // Set up an active state for unpickling (to handle pop calls)
        let mut active = ActivePicklingState::new(false, false);
        active.pending_pickles = inner.pending_pickles;
        active.tensor_engine_references = inner.tensor_engine_references;

        ACTIVE_PICKLING_STATE.with(|cell| {
            *cell.borrow_mut() = Some(active);
        });

        // Unpickle the object
        let result = cloudpickle(py).getattr("loads")?.call1((inner.buffer,));

        // Clear the active state
        ACTIVE_PICKLING_STATE.with(|cell| {
            *cell.borrow_mut() = None;
        });

        result.map(|obj| obj.unbind())
    }
}

impl PicklingState {
    /// Resolve all pending pickles and return a new PicklingState without pending pickles.
    ///
    /// This consumes the PicklingState. It:
    /// 1. If there are no pending pickles, returns self immediately
    /// 2. Otherwise, awaits all pending pickles until they're finished
    /// 3. Calls unpickle to reconstruct the object
    /// 4. Calls pickle again to get a new PicklingState without pending pickles
    pub async fn resolve(mut self) -> PyResult<PicklingState> {
        // Short-circuit if there are no pending pickles
        if self.inner_ref()?.pending_pickles.is_empty() {
            return Ok(self);
        }

        // Await all pending pickles to ensure they're resolved
        let pending: Vec<Py<PyShared>> = Python::attach(|py| {
            self.inner_ref().map(|inner| {
                inner
                    .pending_pickles
                    .iter()
                    .map(|p| p.clone_ref(py))
                    .collect()
            })
        })?;

        for pending_pickle in pending {
            let mut task = Python::attach(|py| pending_pickle.borrow(py).task())?;
            task.take_task()?.await?;
        }

        // Unpickle (pending pickles are now resolved) and re-pickle without allowing new ones
        Python::attach(|py| {
            let obj = self.unpickle(py)?;
            pickle(py, obj, false, true)
        })
    }
}

/// A message that is pending resolution of async values before it can be sent.
///
/// Contains a `PythonMessageKind` and a `PicklingState`. The `PicklingState` may contain
/// pending pickles (unresolved async values) that must be resolved before the message
/// can be converted into a `PythonMessage`.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.pickle")]
pub struct PendingMessage {
    pub(crate) kind: PythonMessageKind,
    state: PicklingState,
}

impl PendingMessage {
    /// Create a new PendingMessage from a kind and pickling state.
    pub fn new(kind: PythonMessageKind, state: PicklingState) -> Self {
        Self { kind, state }
    }

    /// Take ownership of the inner state from a mutable reference.
    ///
    /// This is used by pyo3 pymethods that receive `&mut PendingMessage`
    /// but need to pass ownership to the trait method.
    pub fn take(&mut self) -> PyResult<PendingMessage> {
        let inner = self.state.take_inner()?;
        Ok(PendingMessage {
            kind: std::mem::take(&mut self.kind),
            state: PicklingState { inner: Some(inner) },
        })
    }

    /// Resolve all pending pickles and convert this into a PythonMessage.
    ///
    /// This is an async method that:
    /// 1. Awaits all pending pickles in the PicklingState
    /// 2. Re-pickles the resolved object
    /// 3. Returns a PythonMessage with the resolved bytes (no GIL needed for final step)
    pub async fn resolve(self) -> PyResult<PythonMessage> {
        // Resolve the pickling state (awaits all pending pickles and re-pickles)
        let mut resolved_state = self.state.resolve().await?;

        // Take the FrozenBuffer directly - no GIL needed since FrozenBuffer doesn't contain Py<>
        let inner = resolved_state.take_inner()?;
        Ok(PythonMessage::new_from_buf(self.kind, inner.take_buffer()))
    }
}

#[pymethods]
impl PendingMessage {
    /// Create a new PendingMessage from a kind and pickling state.
    #[new]
    pub fn py_new(
        kind: PythonMessageKind,
        mut state: PyRefMut<'_, PicklingState>,
    ) -> PyResult<Self> {
        // Take the inner state from the PicklingState
        let inner = state.take_inner()?;
        Ok(Self {
            kind,
            state: PicklingState { inner: Some(inner) },
        })
    }

    /// Get the message kind.
    #[getter]
    fn kind(&self) -> PythonMessageKind {
        self.kind.clone()
    }
}

/// Push a tensor engine reference to the active pickling state if one is active.
///
/// This is called from Python during pickling when a tensor engine object
/// is encountered that needs special handling.
///
/// Returns False if there is no active pickling state.
/// Returns True if the reference was successfully pushed.
/// Raises an error if tensor engine references are not allowed in the current pickling context.
#[pyfunction]
fn push_tensor_engine_reference_if_active(obj: Py<PyAny>) -> PyResult<bool> {
    ACTIVE_PICKLING_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        match state.as_mut() {
            Some(s) => {
                if !s.allow_tensor_engine_references {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "Tensor engine references are not allowed in the current pickling context",
                    ));
                }
                s.tensor_engine_references.push_back(obj);
                Ok(true)
            }
            None => Ok(false),
        }
    })
}

/// Pop a tensor engine reference from the active pickling state.
///
/// This is called from Python during unpickling to retrieve tensor engine
/// objects in the order they were pushed.
#[pyfunction]
fn pop_tensor_engine_reference(py: Python<'_>) -> PyResult<Py<PyAny>> {
    ACTIVE_PICKLING_STATE
        .with(|cell| {
            let mut state = cell.borrow_mut();
            match state.as_mut() {
                Some(s) => s.tensor_engine_references.pop_front().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "No tensor engine references remaining",
                    )
                }),
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "No active pickling state",
                )),
            }
        })
        .map(|obj| obj.clone_ref(py))
}

/// Pop a pending pickle from the active pickling state.
///
/// This is called from Python during unpickling to retrieve the PyShared
/// object that was deferred during pickling.
#[pyfunction]
fn pop_pending_pickle(py: Python<'_>) -> PyResult<Py<PyShared>> {
    ACTIVE_PICKLING_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        match state.as_mut() {
            Some(s) => {
                let shared = s.pending_pickles.pop_front().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("No pending pickles remaining")
                })?;
                Ok(shared.clone_ref(py))
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "No active pickling state",
            )),
        }
    })
}

/// Push a pending pickle to the active pickling state (Rust-only).
///
/// This is used by __reduce__ implementations to register a PyShared
/// that must be resolved before the pickle is complete.
///
/// Returns an error if there is no active pickling state or if pending
/// pickles are not allowed in the current pickling context.
pub fn push_pending_pickle(py_shared: Py<PyShared>) -> PyResult<()> {
    ACTIVE_PICKLING_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        match state.as_mut() {
            Some(s) => {
                if !s.allow_pending_pickles {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "Pending pickles are not allowed in the current pickling context",
                    ));
                }
                s.pending_pickles.push_back(py_shared);
                Ok(())
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "No active pickling state",
            )),
        }
    })
}

/// Reduce a PyShared for pickling.
///
/// This function implements the pickle protocol for PyShared:
/// 1. If the shared is already finished, return (Shared.from_value, (value,))
/// 2. If pending pickles are allowed, push it as a pending pickle and return (pop_pending_pickle, ())
/// 3. Otherwise, block on the shared and return (Shared.from_value, (value,))
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

    // Try to push as a pending pickle (will fail if not allowed or no active state)
    let py_shared_py: Py<PyShared> = py_shared.clone().unbind();
    if push_pending_pickle(py_shared_py).is_ok() {
        let pop_fn = pop_pending_pickle_fn(py);
        let args = PyTuple::empty(py);
        return Ok((pop_fn, args));
    }

    // Fall back to blocking on the shared
    let value = PyShared::block_on(py_shared.borrow(), py)?;
    let from_value = shared_class(py).getattr("from_value")?;
    let args = PyTuple::new(py, [value])?;
    Ok((from_value, args))
}

/// Pickle an object with support for pending pickles and tensor engine references.
///
/// This function creates a PicklingState and calls cloudpickle.dumps with
/// an active thread-local PicklingState, allowing __reduce__ implementations
/// to push tensor engine references and pending pickles.
///
/// # Arguments
/// * `obj` - The Python object to pickle
/// * `allow_pending_pickles` - If true, allow PyShared values to be registered as pending
/// * `allow_tensor_engine_references` - If true, allow tensor engine references to be registered
///
/// # Returns
/// A PicklingState containing the pickled buffer and any registered references/pending pickles
#[pyfunction]
#[pyo3(signature = (obj, allow_pending_pickles=true, allow_tensor_engine_references=true))]
pub fn pickle(
    py: Python<'_>,
    obj: Py<PyAny>,
    allow_pending_pickles: bool,
    allow_tensor_engine_references: bool,
) -> PyResult<PicklingState> {
    // Ensure the cloudpickle monkeypatch for RemoteImportLoader is applied.
    pickle_monkeypatch(py);

    let active = ActivePicklingState::new(allow_pending_pickles, allow_tensor_engine_references);
    let buffer = Py::new(py, Buffer::default())?;

    // Set up this state as the active pickling state
    ACTIVE_PICKLING_STATE.with(|cell| {
        *cell.borrow_mut() = Some(active);
    });

    // Get the Pickler class and create an instance with our buffer
    let pickler = cloudpickle(py)
        .getattr("Pickler")?
        .call1((buffer.bind(py),))?;

    // Dump the object
    let result = pickler.call_method1("dump", (&obj,));

    // Retrieve the state (which may have been modified during pickling)
    let active = ACTIVE_PICKLING_STATE
        .with(|cell| cell.borrow_mut().take())
        .ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Pickling state was unexpectedly cleared")
        })?;

    result?;

    // Convert to frozen PicklingState
    let frozen_buffer = buffer.borrow_mut(py).freeze();
    let inner = active.into_pickling_state(frozen_buffer);
    Ok(PicklingState { inner: Some(inner) })
}

/// Register the pickle Python bindings into the given module.
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PicklingState>()?;
    module.add_class::<PendingMessage>()?;
    module.add_function(wrap_pyfunction!(pickle, module)?)?;
    module.add_function(wrap_pyfunction!(
        push_tensor_engine_reference_if_active,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(pop_tensor_engine_reference, module)?)?;
    module.add_function(wrap_pyfunction!(pop_pending_pickle, module)?)?;
    Ok(())
}
