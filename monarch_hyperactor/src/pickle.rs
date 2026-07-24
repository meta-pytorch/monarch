/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Pickling support for Monarch.
//!
//! This module pickles Python objects for actor messages, collecting tensor
//! engine references and out-of-band mesh references during serialization so
//! they are restored together on decode.
//!
//! ## Out-of-band mesh-reference invariants
//!
//! Mesh references (proc/host/actor meshes) are not pickled inline. They ride
//! *out of band* in a `refs` table carried next to the payload bytes, leaving a
//! `pop_mesh_reference` sentinel in the pickle stream. Two invariants keep the
//! table and its payload from ever drifting apart:
//!
//! **REFS-1 (ref-aware decode).** Any payload that can carry out-of-band refs
//! must be decoded only through a ref-aware path: `PicklingState::from_parts`
//! and `PicklingState::unpickle`, as wrapped by `PythonMessage::decode` and
//! `PythonResponseMessage::decode`. A bare `pickle.loads`/`cloudpickle.loads`
//! on the payload bytes drops the table, so a sentinel later pops with no active
//! state and raises "No active pickling state". The raw payload bytes are
//! deliberately not exposed on `PythonMessage` (there is no `.message` getter),
//! so a bare decode is not even expressible from Python.
//!
//! **REFS-2 (refs preserved through intermediates).** Any intermediate that
//! relays a payload -- `PythonResponseMessage` and the `ValueOverlay` behind a
//! `.call()` valuemesh -- must carry its `refs` beside the bytes, all the way to
//! the decode site. Dropping refs at a relay boundary reintroduces the REFS-1
//! failure downstream. Refs are therefore part of `PythonResponseMessage`
//! equality: two runs with identical bytes but different refs must not coalesce.
//!
//! These invariants have one sound exception. A table entry and a
//! `pop_mesh_reference` sentinel are emitted together during pickling (1:1),
//! so an empty `refs` table means the payload carries no sentinels: a bare
//! decode of it pops nothing and is sound. The `.call()` valuemesh collector
//! (`collect_valuemesh` in `endpoint.rs`) relies on this to preserve
//! D96180139's lazy unpickle: it decodes a ref-empty batch lazily via
//! `PyValueMesh::build_from_parts` (`value_mesh.rs`), which resolves on access
//! outside the ref-aware path, and only a ref-carrying batch eagerly via
//! `build_from_objects`. That lazy build is the sole bare decode of a payload
//! that *could* carry refs; see `collect_valuemesh` for the gate itself.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use monarch_types::py_global;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::types::PyTuple;
use serde_multipart::Part;

use crate::actor::MeshRef;
use crate::actor::PyMeshRef;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::buffers::Buffer;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil_blocking;

// cloudpickle module for serialization
py_global!(cloudpickle, "cloudpickle", "cloudpickle");

py_global!(_unpickle, "pickle", "loads");

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

// Check if torch has been loaded into the current Python process.
// Returns the torch module if loaded, otherwise None.
py_global!(maybe_torch_fn, "monarch._src.actor.pickle", "maybe_torch");

// Torch-aware dump function: uses a Pickler subclass with dispatch_table
// entries for torch storage types (UntypedStorage, TypedStorage, etc.).
py_global!(torch_dump_fn, "monarch._src.actor.pickle", "torch_dump");

// Torch-aware loads function: wraps cloudpickle.loads with
// torch.utils._python_dispatch._disable_current_modes().
py_global!(torch_loads_fn, "monarch._src.actor.pickle", "torch_loads");

// Shared class for pickling PyShared values
py_global!(
    shared_class,
    "monarch._rust_bindings.monarch_hyperactor.pytokio",
    "Shared"
);

// Thread-local storage for the active pickling state.
// Set by pickle/unpickle operations so free functions used in __reduce__
// implementations can access it.
//
// It is thread-local by design: two threads pickling at once (even the same
// mesh ref) collect into independent `mesh_references` / `pending_mesh_fills`,
// so there is nothing shared to race on. `pickle()` then moves the state out of
// the thread-local into an owned `PicklingState` before the GIL-releasing
// `PicklingState::resolve`, so the sender-side slot fill mutates a single-owner
// value across its awaits, never this thread-local. Both properties (thread-
// local, and moved out before resolve) are what make releasing the GIL safe.
thread_local! {
    static ACTIVE_PICKLING_STATE: RefCell<Option<ActivePicklingState>> = const { RefCell::new(None) };
}

/// Counters for tests. `PENDING_RESERVE_COUNT` bumps when a still-pending mesh
/// reserves an out-of-band slot on the send side; it is pending-specific (a
/// resolved mesh fills directly), so it proves the pending path ran.
/// `MESH_POP_COUNT` bumps when any out-of-band mesh reference is reunited on the
/// decode side; it is not pending-specific, since a resolved mesh also travels
/// out-of-band, so it proves receive and reconstruct, not pending-ness.
static PENDING_RESERVE_COUNT: AtomicUsize = AtomicUsize::new(0);
static MESH_POP_COUNT: AtomicUsize = AtomicUsize::new(0);

/// RAII guard that sets the thread-local `ACTIVE_PICKLING_STATE` on creation
/// and restores the previous state (if any) on drop. This supports nesting:
/// if a guard already exists, the new guard saves the old state and restores
/// it when dropped, even on panic.
struct ActivePicklingGuard {
    previous: Option<ActivePicklingState>,
}

impl ActivePicklingGuard {
    /// Set `state` as the active pickling state, saving any existing state.
    fn enter(state: ActivePicklingState) -> Self {
        let previous = ACTIVE_PICKLING_STATE.with(|cell| cell.borrow_mut().replace(state));
        Self { previous }
    }
}

impl Drop for ActivePicklingGuard {
    fn drop(&mut self) {
        ACTIVE_PICKLING_STATE.with(|cell| {
            *cell.borrow_mut() = self.previous.take();
        });
    }
}

/// State maintained during active pickling/unpickling operations.
///
/// This is the thread-local state used while cloudpickle is running.
/// It collects tensor engine references and pending pickles during serialization.
struct ActivePicklingState {
    /// References to tensor engine objects that need special handling.
    tensor_engine_references: VecDeque<Py<PyAny>>,
    /// Mesh references collected out-of-band into the message's `refs` table.
    /// A `None` is a slot reserved for a pending mesh, filled sender-side.
    mesh_references: VecDeque<Option<MeshRef>>,
    /// Pending mesh slots awaiting a sender-side fill: (index into
    /// `mesh_references`, the handle whose resolved mesh supplies the ref).
    pending_mesh_fills: Vec<(usize, Py<PyShared>)>,
    /// Whether tensor engine references are allowed in this pickling context.
    allow_tensor_engine_references: bool,
    /// Whether mesh references are collected out-of-band in this context.
    allow_mesh_references: bool,
}

impl ActivePicklingState {
    /// Create a new ActivePicklingState.
    fn new(allow_tensor_engine_references: bool, allow_mesh_references: bool) -> Self {
        Self {
            tensor_engine_references: VecDeque::new(),
            mesh_references: VecDeque::new(),
            pending_mesh_fills: Vec::new(),
            allow_tensor_engine_references,
            allow_mesh_references,
        }
    }

    /// Convert this active state into a frozen PicklingState.
    fn into_pickling_state(self, buffer: Part) -> PicklingStateInner {
        PicklingStateInner {
            buffer,
            tensor_engine_references: self.tensor_engine_references,
            mesh_references: self.mesh_references,
            pending_mesh_fills: self.pending_mesh_fills,
        }
    }
}

/// Inner data for a completed pickling operation.
///
/// This contains the pickled bytes as a fragmented [`Part`] (zero-copy)
/// and any collected references.
pub struct PicklingStateInner {
    /// The pickled bytes as a fragmented Part (zero-copy).
    buffer: Part,
    /// References to tensor engine objects that need special handling.
    tensor_engine_references: VecDeque<Py<PyAny>>,
    /// Mesh references carried out-of-band into the message's `refs` table.
    /// A `None` is a slot reserved for a pending mesh, filled sender-side.
    mesh_references: VecDeque<Option<MeshRef>>,
    /// Pending mesh slots awaiting a sender-side fill.
    pending_mesh_fills: Vec<(usize, Py<PyShared>)>,
}

impl PicklingStateInner {
    /// Take the Part (pickled bytes) from this inner state.
    pub fn take_buffer(self) -> Part {
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

    fn has_pending_mesh_fills(&self) -> PyResult<bool> {
        Ok(!self.inner_ref()?.pending_mesh_fills.is_empty())
    }

    /// Build a PicklingState directly from already-separated parts: the pickled
    /// `buffer`, the ordered local-state/tensor-engine list, and the resolved
    /// mesh-reference table. Used by `PythonMessage::decode` so a message's
    /// payload is always unpickled together with its `refs`.
    pub(crate) fn from_parts(
        buffer: Part,
        tensor_engine_references: VecDeque<Py<PyAny>>,
        mesh_references: VecDeque<Option<MeshRef>>,
    ) -> Self {
        Self {
            inner: Some(PicklingStateInner {
                buffer,
                tensor_engine_references,
                mesh_references,
                pending_mesh_fills: Vec::new(),
            }),
        }
    }
}

#[pymethods]
impl PicklingState {
    /// Create a new PicklingState from a buffer and optional tensor engine references.
    ///
    /// This is used for unpickling received messages that may contain tensor engine
    /// references that need to be restored during deserialization.
    #[new]
    #[pyo3(signature = (buffer, tensor_engine_references=None, mesh_references=None))]
    fn py_new(
        py: Python<'_>,
        buffer: PyRef<'_, crate::buffers::FrozenBuffer>,
        tensor_engine_references: Option<&Bound<'_, PyList>>,
        mesh_references: Option<Vec<Py<PyMeshRef>>>,
    ) -> PyResult<Self> {
        let refs: VecDeque<Py<PyAny>> = tensor_engine_references
            .map(|list| list.iter().map(|item| item.unbind()).collect())
            .unwrap_or_default();

        // pyo3 type-checks each element as a `PyMeshRef` during extraction.
        let mesh_refs: VecDeque<Option<MeshRef>> = mesh_references
            .map(|list| {
                list.into_iter()
                    .map(|m| Some(m.borrow(py).inner.clone()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            inner: Some(PicklingStateInner {
                buffer: Part::from(buffer.inner.clone()),
                tensor_engine_references: refs,
                mesh_references: mesh_refs,
                pending_mesh_fills: Vec::new(),
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
        Ok(crate::buffers::FrozenBuffer {
            inner: inner.buffer.clone().into_bytes(),
        })
    }

    /// Unpickle the buffer contents.
    ///
    /// This consumes the PicklingState. It will fail if there are any pending
    /// pickles that haven't been resolved.
    pub(crate) fn unpickle(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = self.take_inner()?;

        // Set up an active state for unpickling (to handle pop calls).
        // The guard restores any previous state on drop (including on panic).
        let mut active = ActivePicklingState::new(false, false);
        active.tensor_engine_references = inner.tensor_engine_references;
        active.mesh_references = inner.mesh_references;
        active.pending_mesh_fills = inner.pending_mesh_fills;

        let _guard = ActivePicklingGuard::enter(active);

        let frozen = crate::buffers::FrozenBuffer {
            inner: inner.buffer.into_bytes(),
        };

        // Unpickle the object. If torch is loaded, use torch_loads which
        // disables dispatch modes during unpickling.
        let result = if maybe_torch_fn(py).call0()?.is_truthy()? {
            torch_loads_fn(py).call1((frozen,))
        } else {
            cloudpickle(py).getattr("loads")?.call1((frozen,))
        };

        result.map(|obj| obj.unbind())
    }
}

impl PicklingState {
    /// Fill the reserved mesh slots from their pending handles, producing a
    /// PicklingState whose out-of-band `refs` table is fully populated.
    ///
    /// Awaits each pending mesh's init task, extracts its `*MeshRef`, and writes
    /// it into the reserved slot. The payload bytes are unchanged (no re-pickle).
    pub async fn resolve(mut self) -> PyResult<PicklingState> {
        // Take the pending mesh fills out: a plain move, no GIL and no
        // `clone_ref`. Each is a slot index plus the handle whose resolved mesh
        // supplies the ref.
        let fills = std::mem::take(
            &mut self
                .inner
                .as_mut()
                .ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "PicklingState has already been consumed",
                    )
                })?
                .pending_mesh_fills,
        );

        for (index, handle) in fills {
            // Await the mesh's init task (these run concurrently, so the wait
            // is the slowest init, not the sum).
            let mut task =
                monarch_with_gil_blocking(GilSite::AwaitDrive, |py| handle.borrow(py).task())?;
            task.take_task()?.await?;

            // Extract the `*MeshRef` from the now-resolved mesh and fill the slot.
            monarch_with_gil_blocking(GilSite::Convert, |py| -> PyResult<()> {
                let value = handle.borrow(py).poll()?.ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("pending mesh handle did not resolve")
                })?;
                let mesh_ref = crate::actor::mesh_ref_from_pyobject(value.bind(py))?;
                if let Some(inner) = self.inner.as_mut() {
                    inner.mesh_references[index] = Some(mesh_ref);
                }
                Ok(())
            })?;
        }

        Ok(self)
    }
}

/// A message whose reserved mesh slots must be filled before it can be sent.
///
/// Contains a `PythonMessageKind` and a `PicklingState`. The `PicklingState` may
/// hold mesh slots reserved for pending meshes, filled sender-side once their
/// handles resolve, before the message can be converted into a `PythonMessage`.
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

    fn into_python_message(mut self) -> PyResult<PythonMessage> {
        let inner = self.state.take_inner()?;
        let refs: Vec<MeshRef> = inner
            .mesh_references
            .into_iter()
            .map(|reference| {
                reference.ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "mesh reference slot was never filled",
                    )
                })
            })
            .collect::<PyResult<_>>()?;
        Ok(PythonMessage::new_from_buf_with_refs(
            self.kind,
            inner.buffer,
            refs,
        ))
    }

    /// Convert this message synchronously when no mesh fills are pending.
    ///
    /// A pending message is returned unchanged in the inner `Err` so the
    /// caller can use the existing asynchronous resolution path.
    /// Every reserved `None` mesh slot is created together with one pending
    /// fill entry, while resolved and reconstructed slots are always `Some`.
    /// Therefore a fresh zero-fill message cannot fail assembly due to an
    /// unfilled slot.
    pub fn try_resolve_now(self) -> PyResult<Result<PythonMessage, PendingMessage>> {
        if self.state.has_pending_mesh_fills()? {
            return Ok(Err(self));
        }

        Ok(Ok(self.into_python_message()?))
    }

    /// Resolve any reserved mesh slots and convert this into a PythonMessage.
    ///
    /// This is an async method that:
    /// 1. Fills the reserved mesh slots from their pending handles
    /// 2. Builds a PythonMessage with the resolved bytes and `refs` table
    pub async fn resolve(self) -> PyResult<PythonMessage> {
        let Self { kind, state } = self;
        let state = state.resolve().await?;
        Self::new(kind, state).into_python_message()
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

    /// Return a materialized message when no mesh fills are pending.
    ///
    /// A `None` result leaves this object unchanged for `resolve()`.
    #[pyo3(name = "try_resolve_now")]
    fn py_try_resolve_now(&mut self) -> PyResult<Option<PythonMessage>> {
        if self.state.has_pending_mesh_fills()? {
            return Ok(None);
        }

        self.take()?.into_python_message().map(Some)
    }

    /// Fill reserved mesh slots and return a fully materialized PythonMessage.
    #[pyo3(name = "resolve")]
    fn py_resolve(&mut self) -> PyResult<PyPythonTask> {
        let message = self.take()?;
        PyPythonTask::new(async move { message.resolve().await })
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

/// Pop a mesh reference from the active pickling state and rebuild its
/// Python mesh wrapper.
///
/// Called from the unpickle stream wherever a mesh slot was emitted in
/// place of inline bytes.
#[pyfunction]
fn pop_mesh_reference(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let mesh_ref = ACTIVE_PICKLING_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        match state.as_mut() {
            Some(s) => s.mesh_references.pop_front().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("No mesh references remaining")
            }),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "No active pickling state",
            )),
        }
    })?;
    let mesh_ref = mesh_ref.ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("mesh reference slot was never filled")
    })?;
    MESH_POP_COUNT.fetch_add(1, Ordering::Relaxed);
    mesh_ref.reconstruct(py)
}

/// Push a mesh reference to the active pickling state if mesh-reference
/// collection is active in this context.
///
/// Returns true if the reference was collected (the caller emits a slot);
/// false otherwise (the caller inlines the reference as usual).
pub fn push_mesh_reference_if_active(mesh_ref: MeshRef) -> bool {
    ACTIVE_PICKLING_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        match state.as_mut() {
            Some(s) if s.allow_mesh_references => {
                s.mesh_references.push_back(Some(mesh_ref));
                true
            }
            _ => false,
        }
    })
}

/// Reserve a slot for a *pending* mesh whose `*MeshRef` is not available yet,
/// registering `handle` for a sender-side fill once the mesh resolves.
///
/// Returns true if a slot was reserved (the caller emits a `pop_mesh_reference`
/// placeholder); false otherwise (the caller keeps its current behavior).
pub fn reserve_mesh_reference_if_active(handle: Py<PyShared>) -> bool {
    ACTIVE_PICKLING_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        match state.as_mut() {
            Some(s) if s.allow_mesh_references => {
                let index = s.mesh_references.len();
                s.mesh_references.push_back(None);
                s.pending_mesh_fills.push((index, handle));
                PENDING_RESERVE_COUNT.fetch_add(1, Ordering::Relaxed);
                true
            }
            _ => false,
        }
    })
}

/// Python-callable wrapper over [`reserve_mesh_reference_if_active`] for the
/// typed Proc/Host reduces, which reserve their pending slot from Python.
#[pyfunction]
fn reserve_mesh_reference(handle: Py<PyShared>) -> bool {
    reserve_mesh_reference_if_active(handle)
}

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

/// Pickle a Python object into a [`Buffer`].
///
/// This is the shared pickling core. The caller is responsible for setting up
/// the [`ActivePicklingGuard`] before calling this function.
fn pickle_into_buffer(py: Python<'_>, obj: &Py<PyAny>, buffer: &Py<Buffer>) -> PyResult<()> {
    // Ensure the cloudpickle monkeypatch for RemoteImportLoader is applied.
    pickle_monkeypatch(py);

    // If torch is loaded, use the torch-aware pickler that handles
    // torch storage types via dispatch_table.
    if maybe_torch_fn(py).call0()?.is_truthy()? {
        torch_dump_fn(py).call1((obj, buffer.bind(py)))?;
    } else {
        let pickler = cloudpickle(py)
            .getattr("Pickler")?
            .call1((buffer.bind(py),))?;
        pickler.call_method1("dump", (obj,))?;
    }

    Ok(())
}

/// Pickle a Python object and return the serialized data as a [`Part`].
///
/// This is a simplified variant of [`pickle`] that disallows pending pickles
/// and tensor engine references, and returns the raw serialized bytes instead
/// of a [`PicklingState`].
pub fn pickle_to_part(py: Python<'_>, obj: &Py<PyAny>) -> PyResult<Part> {
    let active = ActivePicklingState::new(false, false);
    let buffer = Py::new(py, Buffer::default())?;
    let _guard = ActivePicklingGuard::enter(active);

    pickle_into_buffer(py, obj, &buffer)?;

    Ok(buffer.borrow_mut(py).take_part())
}

/// Pickle an object with support for pending pickles and tensor engine references.
///
/// This function creates a PicklingState and calls cloudpickle.dumps with
/// an active thread-local PicklingState, allowing __reduce__ implementations
/// to push tensor engine references and pending pickles.
///
/// # Arguments
/// * `obj` - The Python object to pickle
/// * `allow_tensor_engine_references` - If true, allow tensor engine references to be registered
///
/// # Returns
/// A PicklingState containing the pickled buffer and any registered references/pending pickles
#[pyfunction]
#[pyo3(signature = (obj, allow_tensor_engine_references=true, allow_mesh_references=false))]
pub fn pickle(
    py: Python<'_>,
    obj: Py<PyAny>,
    allow_tensor_engine_references: bool,
    allow_mesh_references: bool,
) -> PyResult<PicklingState> {
    let active = ActivePicklingState::new(allow_tensor_engine_references, allow_mesh_references);
    let buffer = Py::new(py, Buffer::default())?;
    let _guard = ActivePicklingGuard::enter(active);

    pickle_into_buffer(py, &obj, &buffer)?;

    // Take the state (which may have been modified during pickling).
    // The guard will restore the previous state on drop.
    let active = ACTIVE_PICKLING_STATE
        .with(|cell| cell.borrow_mut().take())
        .expect("active pickling state should still be set");

    // Take the Part (zero-copy fragmented buffer) directly.
    let part = buffer.borrow_mut(py).take_part();
    let inner = active.into_pickling_state(part);
    Ok(PicklingState { inner: Some(inner) })
}

pub(crate) fn unpickle(
    py: Python<'_>,
    buffer: crate::buffers::FrozenBuffer,
) -> PyResult<Bound<'_, PyAny>> {
    _unpickle(py).call1((buffer.into_py_any(py)?,))
}

/// Test helper: read the pending-mesh reserve counter.
#[pyfunction]
fn _get_pending_reserve_count() -> usize {
    PENDING_RESERVE_COUNT.load(Ordering::Relaxed)
}

/// Test helper: reset the pending-mesh reserve counter to zero.
#[pyfunction]
fn _reset_pending_reserve_count() {
    PENDING_RESERVE_COUNT.store(0, Ordering::Relaxed);
}

/// Test helper: read the mesh-pop counter.
#[pyfunction]
fn _get_mesh_pop_count() -> usize {
    MESH_POP_COUNT.load(Ordering::Relaxed)
}

/// Test helper: reset the mesh-pop counter to zero.
#[pyfunction]
fn _reset_mesh_pop_count() {
    MESH_POP_COUNT.store(0, Ordering::Relaxed);
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
    module.add_function(wrap_pyfunction!(pop_mesh_reference, module)?)?;
    module.add_function(wrap_pyfunction!(reserve_mesh_reference, module)?)?;
    module.add_function(wrap_pyfunction!(_get_pending_reserve_count, module)?)?;
    module.add_function(wrap_pyfunction!(_reset_pending_reserve_count, module)?)?;
    module.add_function(wrap_pyfunction!(_get_mesh_pop_count, module)?)?;
    module.add_function(wrap_pyfunction!(_reset_mesh_pop_count, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hyperactor::ActorRef;
    use hyperactor::ProcAddr;
    use hyperactor::ProcId;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::id::Label;
    use hyperactor::id::Uid;
    use hyperactor_mesh::mesh_id::ProcMeshId;
    use hyperactor_mesh::proc_agent::PROC_AGENT_ACTOR_NAME;
    use hyperactor_mesh::proc_agent::ProcAgent;
    use hyperactor_mesh::proc_mesh::ProcMeshRef;
    use hyperactor_mesh::proc_mesh::ProcRef;

    use super::*;

    fn resolved_proc_mesh_ref() -> MeshRef {
        let proc_id = ProcId::new(
            Uid::Instance(1, None),
            Some(Label::new("local").expect("test label should be valid")),
        );
        let proc_addr = ProcAddr::new(proc_id, ChannelAddr::Local(1).into());
        let agent: ActorRef<ProcAgent> =
            ActorRef::attest(proc_addr.actor_addr(PROC_AGENT_ACTOR_NAME));
        let proc_ref = ProcRef::new(proc_addr, 0, agent);
        MeshRef::Proc(Box::new(
            ProcMeshRef::new_singleton(
                ProcMeshId::singleton(Label::new("mesh").expect("test label should be valid")),
                proc_ref,
            )
            .expect("test proc mesh should be valid"),
        ))
    }

    fn pending_message(
        kind: PythonMessageKind,
        buffer: Vec<u8>,
        refs: Vec<MeshRef>,
    ) -> PendingMessage {
        PendingMessage::new(
            kind,
            PicklingState {
                inner: Some(PicklingStateInner {
                    buffer: Part::from(buffer),
                    tensor_engine_references: VecDeque::new(),
                    mesh_references: refs.into_iter().map(Some).collect(),
                    pending_mesh_fills: Vec::new(),
                }),
            },
        )
    }

    #[tokio::test]
    async fn try_resolve_now_matches_async_resolution_with_resolved_refs() {
        let kind = PythonMessageKind::Result { rank: Some(7) };
        let buffer = vec![0, 1, 2, 3, 127, 128, 254, 255];

        for refs in [Vec::new(), vec![resolved_proc_mesh_ref()]] {
            let synchronous = match pending_message(kind.clone(), buffer.clone(), refs.clone())
                .try_resolve_now()
                .expect("zero-pending probe should succeed")
            {
                Ok(message) => message,
                Err(_) => panic!("zero-pending message should resolve synchronously"),
            };
            let asynchronous = pending_message(kind.clone(), buffer.clone(), refs)
                .resolve()
                .await
                .expect("zero-pending async resolution should succeed");

            assert_eq!(
                synchronous, asynchronous,
                "sync and async resolution should preserve kind, bytes, and refs"
            );
        }
    }
}
