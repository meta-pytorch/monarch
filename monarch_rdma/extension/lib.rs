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
use std::time::Duration;

use hyperactor_mesh::ActorMesh;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::proc_mesh::PyProcMesh;
use monarch_hyperactor::pytokio::PyPythonTask;
use monarch_hyperactor::runtime::GilSite;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_rdma::RdmaAction;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::RdmaManagerMessageClient;
use monarch_rdma::RdmaRemoteBuffer;
use monarch_rdma::ScannedSegment;
use monarch_rdma::ibverbs_supported;
use monarch_rdma::local_memory::Keepalive;
use monarch_rdma::local_memory::KeepaliveLocalMemory;
use monarch_rdma::local_memory::WeakKeepalive;
use monarch_rdma::local_memory::WeakLocalMemory;
use monarch_rdma::rdma_supported;
use monarch_rdma::register_cuda_segment_scanner;
use monarch_types::py_global;
use monarch_types::py_module_add_function;
use pyo3::IntoPyObjectExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyMemoryView;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use typeuri::Named;

/// CUDA segment scanner backed by PyTorch's memory snapshot API.
///
/// Enumerates the live CUDA caching-allocator segments via
/// `torch.cuda.memory._snapshot()`. Returns an empty list when torch is not
/// imported, CUDA is unavailable, or the snapshot fails — so the mlx5dv
/// segment binder simply falls back to per-buffer dmabuf MRs rather than
/// erroring.
fn pytorch_cuda_segments() -> Vec<ScannedSegment> {
    // Acquire the GIL to call Python code.
    let result = monarch_with_gil_blocking(GilSite::Rdma, |py| -> PyResult<Vec<ScannedSegment>> {
        // Check if torch is already imported - don't import it ourselves.
        let sys = py.import("sys")?;
        let modules = sys.getattr("modules")?;
        let torch = match modules.get_item("torch") {
            Ok(torch_module) => torch_module,
            Err(_) => return Ok(Vec::new()),
        };

        let cuda_available: bool = torch
            .getattr("cuda")?
            .getattr("is_available")?
            .call0()?
            .extract()?;
        if !cuda_available {
            return Ok(Vec::new());
        }

        let snapshot = torch
            .getattr("cuda")?
            .getattr("memory")?
            .getattr("_snapshot")?
            .call0()?;
        let segments = snapshot.get_item("segments")?;
        let segments_list: Vec<Bound<'_, PyAny>> = segments.extract()?;

        segments_list
            .iter()
            .map(|segment| {
                Ok(ScannedSegment {
                    address: segment.get_item("address")?.extract::<u64>()? as usize,
                    size: segment.get_item("total_size")?.extract()?,
                    cuda_ordinal: segment.get_item("device")?.extract()?,
                    is_expandable: segment.get_item("is_expandable")?.extract()?,
                })
            })
            .collect()
    });

    result.unwrap_or_else(|e| {
        tracing::error!("pytorch_cuda_segments failed: {}", e);
        Vec::new()
    })
}

/// Resolve a Python `weakref.ref` to a strong [`Py<PyAny>`], or
/// `None` if the referent has gone away. Caller must hold the GIL.
fn upgrade_weakref(py: Python<'_>, weak: &Py<PyAny>) -> Option<Py<PyAny>> {
    let obj = weak.call0(py).ok()?;
    if obj.bind(py).is_none() {
        return None;
    }
    Some(obj)
}

py_global!(weakref_ref, "weakref", "ref");

/// Build a `weakref.ref(obj)`. Returns `None` for objects that
/// don't carry a `__weakref__` slot (`bytes`, `bytearray`, ...).
fn make_weakref(py: Python<'_>, obj: &Py<PyAny>) -> Option<Py<PyAny>> {
    let weak_ref = weakref_ref(py).call1((obj.bind(py),)).ok()?;
    Some(weak_ref.unbind())
}

/// Read the current `(addr, size)` of a `memoryview` via the buffer
/// protocol. Returns `None` if the buffer can't be acquired.
fn memoryview_addr_size(py: Python<'_>, mv: &Py<PyAny>) -> Option<(usize, usize)> {
    let buffer = PyBuffer::<u8>::get(mv.bind(py)).ok()?;
    Some((buffer.buf_ptr() as usize, buffer.len_bytes()))
}

/// Whether `obj` is a torch tensor, without importing torch: if torch
/// has not been imported, no object can be a tensor.
fn is_torch_tensor(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let torch = py
        .import("sys")?
        .getattr("modules")?
        .call_method1("get", ("torch",))?;
    if torch.is_none() {
        return Ok(false);
    }
    obj.is_instance(&torch.getattr("Tensor")?)
}

/// `ValueError` describing the supported-input contract, mirroring the
/// message the Python wrapper historically raised.
fn unsupported_buffer_error(buf: &Bound<'_, PyAny>) -> PyErr {
    let repr = buf
        .str()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "<unrepresentable>".to_string());
    PyValueError::new_err(format!(
        "RDMABuffer only supports 1d contiguous torch.Tensor or 1d c-contiguous memoryview. Got: {}",
        repr
    ))
}

/// Validate that `buf` is a 1d contiguous torch tensor or a 1d
/// c-contiguous memoryview, raising `ValueError` otherwise. The
/// local-memory handle constructors derive `(addr, size)` assuming this
/// layout, so a non-contiguous or multi-dimensional input would yield a
/// region that misrepresents the data.
#[pyfunction]
fn _assert_1d_contiguous(py: Python<'_>, buf: &Bound<'_, PyAny>) -> PyResult<()> {
    if is_torch_tensor(py, buf)? {
        let dim: usize = buf.call_method0("dim")?.extract()?;
        let contiguous: bool = buf.call_method0("is_contiguous")?.extract()?;
        if dim != 1 || !contiguous {
            return Err(unsupported_buffer_error(buf));
        }
    } else if buf.is_instance_of::<PyMemoryView>() {
        let ndim: usize = buf.getattr("ndim")?.extract()?;
        let c_contiguous: bool = buf.getattr("c_contiguous")?.extract()?;
        if ndim != 1 || !c_contiguous {
            return Err(unsupported_buffer_error(buf));
        }
    } else {
        return Err(unsupported_buffer_error(buf));
    }
    Ok(())
}

/// Compute the `(addr, size)` of a torch tensor's data: the storage
/// pointer offset by `storage_offset`, and `element_size * numel`.
/// Assumes a 1d contiguous tensor (see [`_assert_1d_contiguous`]).
#[pyfunction]
fn _get_tensor_addr_and_size(tensor: &Bound<'_, PyAny>) -> PyResult<(usize, usize)> {
    let base_addr: usize = tensor
        .call_method0("untyped_storage")?
        .call_method0("data_ptr")?
        .extract()?;
    let storage_offset: usize = tensor.call_method0("storage_offset")?.extract()?;
    let element_size: usize = tensor.call_method0("element_size")?.extract()?;
    let numel: usize = tensor.call_method0("numel")?.extract()?;
    Ok((
        base_addr + storage_offset * element_size,
        element_size * numel,
    ))
}

/// Compute the `(addr, size)` of a `memoryview` via the buffer protocol.
#[pyfunction]
fn _get_memoryview_addr_and_size(
    py: Python<'_>,
    mv: &Bound<'_, PyAny>,
) -> PyResult<(usize, usize)> {
    memoryview_addr_size(py, &mv.clone().unbind())
        .ok_or_else(|| PyRuntimeError::new_err("failed to acquire memoryview buffer"))
}

/// [`Keepalive`] for a Python `memoryview`. Holding the memoryview
/// keeps its buffer exporter pinned for the lifetime of this value.
/// Caches the `(addr, size)` read at construction so the
/// [`Keepalive::addr`] / [`Keepalive::size`]
/// implementations don't re-enter the buffer protocol.
struct PyMemoryViewKeepalive {
    mv: Py<PyAny>,
    addr: usize,
    size: usize,
}

impl Keepalive for PyMemoryViewKeepalive {
    fn addr(&self) -> usize {
        self.addr
    }

    fn size(&self) -> usize {
        self.size
    }

    fn downgrade(&self) -> Option<Arc<dyn WeakKeepalive>> {
        monarch_with_gil_blocking(GilSite::Rdma, |py| {
            let weak = make_weakref(py, &self.mv)?;
            Some(Arc::new(PyMemoryViewWeakKeepalive { weak }) as Arc<dyn WeakKeepalive>)
        })
    }
}

/// [`WeakKeepalive`] for a `memoryview`. Upgrades by re-acquiring the
/// memoryview via the weakref and re-reading `(addr, size)` from the
/// buffer protocol; a retargeted memoryview will produce a strong
/// keepalive whose cached values no longer match the paired
/// [`WeakLocalMemory`], so [`WeakLocalMemory::upgrade`] fails.
struct PyMemoryViewWeakKeepalive {
    weak: Py<PyAny>,
}

impl WeakKeepalive for PyMemoryViewWeakKeepalive {
    fn upgrade(&self) -> Option<Arc<dyn Keepalive>> {
        monarch_with_gil_blocking(GilSite::Rdma, |py| {
            let mv = upgrade_weakref(py, &self.weak)?;
            let (addr, size) = memoryview_addr_size(py, &mv)?;
            Some(Arc::new(PyMemoryViewKeepalive { mv, addr, size }) as Arc<dyn Keepalive>)
        })
    }
}

/// [`Keepalive`] for a torch tensor's `UntypedStorage`. Pinning the
/// storage (rather than any specific tensor view onto it) keeps the
/// backing allocation alive across temporary views like
/// `tensor.view(...).flatten()` — those views can be dropped while
/// the storage outlives them. `(addr, size)` are recomputed from the
/// cached shape components so they remain correct on upgrade.
struct PyTorchUntypedStorageKeepalive {
    storage: Py<PyAny>,
    base_addr: usize,
    storage_offset: usize,
    element_size: usize,
    numel: usize,
}

impl Keepalive for PyTorchUntypedStorageKeepalive {
    fn addr(&self) -> usize {
        self.base_addr + self.storage_offset * self.element_size
    }

    fn size(&self) -> usize {
        self.element_size * self.numel
    }

    fn downgrade(&self) -> Option<Arc<dyn WeakKeepalive>> {
        monarch_with_gil_blocking(GilSite::Rdma, |py| {
            let weak = make_weakref(py, &self.storage)?;
            Some(Arc::new(PyTorchUntypedStorageWeakKeepalive {
                weak,
                storage_offset: self.storage_offset,
                element_size: self.element_size,
                numel: self.numel,
            }) as Arc<dyn WeakKeepalive>)
        })
    }
}

/// [`WeakKeepalive`] for a `UntypedStorage`. Upgrades by re-acquiring
/// the storage via the weakref and re-reading its `data_ptr()`; a
/// fresh `base_addr` combined with the cached shape components
/// reproduces the original `(addr, size)`.
struct PyTorchUntypedStorageWeakKeepalive {
    weak: Py<PyAny>,
    storage_offset: usize,
    element_size: usize,
    numel: usize,
}

impl WeakKeepalive for PyTorchUntypedStorageWeakKeepalive {
    fn upgrade(&self) -> Option<Arc<dyn Keepalive>> {
        monarch_with_gil_blocking(GilSite::Rdma, |py| {
            let storage = upgrade_weakref(py, &self.weak)?;
            let base_addr: usize = storage
                .bind(py)
                .call_method0("data_ptr")
                .ok()?
                .extract()
                .ok()?;
            Some(Arc::new(PyTorchUntypedStorageKeepalive {
                storage,
                base_addr,
                storage_offset: self.storage_offset,
                element_size: self.element_size,
                numel: self.numel,
            }) as Arc<dyn Keepalive>)
        })
    }
}

/// Local memory handle exposed to Python.
///
/// Wraps a [`KeepaliveLocalMemory`] whose keepalive guard is a Python
/// object reference, preventing the backing allocation from being
/// garbage-collected.
#[pyclass(name = "_LocalMemoryHandle", module = "monarch._rust_bindings.rdma")]
#[derive(Clone)]
pub struct PyLocalMemoryHandle {
    inner: KeepaliveLocalMemory,
}

/// Catch-all [`Keepalive`] for a Python object the caller has
/// already inspected to obtain `(addr, size)`. Holds the object
/// strongly to pin the underlying allocation while the
/// [`KeepaliveLocalMemory`] is in use.
struct PyKeepalive {
    #[expect(dead_code, reason = "held only to pin the Python object alive")]
    obj: Py<PyAny>,
    addr: usize,
    size: usize,
}

impl Keepalive for PyKeepalive {
    fn addr(&self) -> usize {
        self.addr
    }

    fn size(&self) -> usize {
        self.size
    }
}

#[pymethods]
impl PyLocalMemoryHandle {
    #[new]
    fn new(obj: Py<PyAny>, addr: usize, size: usize) -> Self {
        let keepalive: Arc<dyn Keepalive> = Arc::new(PyKeepalive { obj, addr, size });
        Self {
            inner: KeepaliveLocalMemory::new(keepalive),
        }
    }

    #[getter]
    fn addr(&self) -> usize {
        self.inner.addr()
    }

    #[getter]
    fn size(&self) -> usize {
        self.inner.size()
    }

    fn read_at(&self, offset: usize, size: usize) -> PyResult<Vec<u8>> {
        let mut buf = vec![0u8; size];
        // SAFETY: `self.inner`'s `AccessLock` is shared with every
        // clone derived from it (including any held by an
        // `RdmaManagerActor` after `create_rdma_buffer`), so intra-
        // handle races are already excluded. The Python caller is
        // responsible for ensuring no *external* view of the same
        // allocation (e.g., a torch tensor whose data pointer was
        // wrapped, or a C extension running with the GIL released)
        // mutates the byte range while this call runs.
        unsafe { self.inner.read_at(offset, &mut buf) }
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(buf)
    }

    fn write_at(&self, offset: usize, data: &[u8]) -> PyResult<()> {
        // SAFETY: see `read_at`.
        unsafe { self.inner.write_at(offset, data) }
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Pair off a [`PyWeakLocalMemoryHandle`] sharing this handle's
    /// MR slot and access lock. Returns `None` when the backing
    /// keepalive (e.g. a memoryview over a non-weak-referenceable
    /// object like `bytes` or `bytearray`) has no weak form.
    fn downgrade(&self) -> Option<PyWeakLocalMemoryHandle> {
        self.inner
            .downgrade()
            .map(|inner| PyWeakLocalMemoryHandle { inner })
    }

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> String {
        format!(
            "<LocalMemoryHandle addr={:#x} size={}>",
            self.inner.addr(),
            self.inner.size()
        )
    }
}

/// Weak counterpart of [`PyLocalMemoryHandle`]. Holds the shared
/// MR slot but only a weak reference to the backing Python object,
/// so caching this handle does not pin the allocation. `upgrade()`
/// returns a fresh strong handle if the referent is still alive.
#[pyclass(
    name = "_WeakLocalMemoryHandle",
    module = "monarch._rust_bindings.rdma"
)]
#[derive(Clone)]
pub struct PyWeakLocalMemoryHandle {
    inner: WeakLocalMemory,
}

#[pymethods]
impl PyWeakLocalMemoryHandle {
    #[getter]
    fn addr(&self) -> usize {
        self.inner.addr()
    }

    #[getter]
    fn size(&self) -> usize {
        self.inner.size()
    }

    /// Try to re-acquire a strong [`PyLocalMemoryHandle`] for the
    /// same allocation. Returns `None` if the backing object has
    /// been garbage-collected.
    fn upgrade(&self) -> Option<PyLocalMemoryHandle> {
        self.inner
            .upgrade()
            .map(|inner| PyLocalMemoryHandle { inner })
    }

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> String {
        format!(
            "<WeakLocalMemoryHandle addr={:#x} size={}>",
            self.inner.addr(),
            self.inner.size()
        )
    }
}

/// Construct a [`PyLocalMemoryHandle`] from a Python `memoryview`.
/// The strong keepalive holds the memoryview (pinning the buffer
/// export); `(addr, size)` come from the buffer protocol and are
/// cached on the keepalive.
#[pyfunction]
fn _make_local_memory_handle_from_memoryview(
    py: Python<'_>,
    mv: &Bound<'_, PyAny>,
) -> PyResult<PyLocalMemoryHandle> {
    _assert_1d_contiguous(py, mv)?;
    let mv_owned = mv.clone().unbind();
    let (addr, size) = memoryview_addr_size(py, &mv_owned)
        .ok_or_else(|| PyRuntimeError::new_err("failed to acquire memoryview buffer"))?;
    let keepalive: Arc<dyn Keepalive> = Arc::new(PyMemoryViewKeepalive {
        mv: mv_owned,
        addr,
        size,
    });
    Ok(PyLocalMemoryHandle {
        inner: KeepaliveLocalMemory::new(keepalive),
    })
}

/// Construct a [`PyLocalMemoryHandle`] from a torch tensor. Extracts
/// the tensor's underlying `UntypedStorage` and the shape components
/// needed to compute `(addr, size)`; the keepalive pins the storage
/// (not the input tensor view), so a transient view like
/// `t.view(...).flatten()` can be dropped without invalidating
/// cached weak handles.
#[pyfunction]
fn _make_local_memory_handle_from_tensor(
    py: Python<'_>,
    tensor: &Bound<'_, PyAny>,
) -> PyResult<PyLocalMemoryHandle> {
    _assert_1d_contiguous(py, tensor)?;
    let storage = tensor.call_method0("untyped_storage")?;
    let base_addr: usize = storage.call_method0("data_ptr")?.extract()?;
    let storage_offset: usize = tensor.call_method0("storage_offset")?.extract()?;
    let element_size: usize = tensor.call_method0("element_size")?.extract()?;
    let numel: usize = tensor.call_method0("numel")?.extract()?;
    let keepalive: Arc<dyn Keepalive> = Arc::new(PyTorchUntypedStorageKeepalive {
        storage: storage.unbind(),
        base_addr,
        storage_offset,
        element_size,
        numel,
    });
    Ok(PyLocalMemoryHandle {
        inner: KeepaliveLocalMemory::new(keepalive),
    })
}

#[pyclass(name = "_RdmaBuffer", module = "monarch._rust_bindings.rdma")]
#[derive(Clone, Named)]
struct PyRdmaBuffer {
    buffer: RdmaRemoteBuffer,
}

/// Batched RDMA action exposed to Python. Wraps a [`RdmaAction`] behind
/// an async mutex so concurrent `submit` calls from Python serialize
/// (preserving the local-range overlap guarantee), and mutations via
/// `add_*` while a submit is in flight are rejected.
#[pyclass(name = "_RdmaAction", module = "monarch._rust_bindings.rdma")]
pub struct PyRdmaAction {
    inner: Arc<tokio::sync::Mutex<RdmaAction>>,
}

impl PyRdmaAction {
    fn try_lock_sync(&self) -> PyResult<tokio::sync::MutexGuard<'_, RdmaAction>> {
        self.inner.try_lock().map_err(|_| {
            PyRuntimeError::new_err(
                "RdmaAction is currently being submitted; await the in-flight \
                 submit before mutating it",
            )
        })
    }
}

#[pymethods]
impl PyRdmaAction {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(RdmaAction::new())),
        }
    }

    fn add_read_into_local(
        &self,
        remote: PyRdmaBuffer,
        local: PyLocalMemoryHandle,
    ) -> PyResult<()> {
        self.try_lock_sync()?
            .add_read_into_local(remote.buffer, local.inner)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn add_write_from_local(
        &self,
        remote: PyRdmaBuffer,
        local: PyLocalMemoryHandle,
    ) -> PyResult<()> {
        self.try_lock_sync()?
            .add_write_from_local(remote.buffer, local.inner)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Submit the queued ops. Returns a [`PyPythonTask`] that resolves
    /// when every op completes (or the first error). Concurrent submits
    /// queue on the inner async mutex and run one at a time, so the
    /// local-range overlap checks performed at `add_*` time remain
    /// meaningful.
    fn submit(&self, _py: Python<'_>, client: PyInstance, timeout: u64) -> PyResult<PyPythonTask> {
        let inner = self.inner.clone();
        PyPythonTask::new(async move {
            let mut action = inner.lock().await;
            action
                .submit(client.deref(), Duration::from_secs(timeout))
                .await
                .map_err(|e| PyException::new_err(format!("RdmaAction.submit failed: {}", e)))?;
            Ok(())
        })
    }
}

async fn create_rdma_buffer(
    local: PyLocalMemoryHandle,
    client: PyInstance,
) -> PyResult<PyRdmaBuffer> {
    let owner_handle = RdmaManagerActor::local_handle(client.deref());

    let buffer = owner_handle
        .request_buffer(client.deref(), local.inner)
        .await
        .map_err(|e| PyException::new_err(format!("failed to request buffer: {}", e)))?;

    Ok(PyRdmaBuffer { buffer })
}

#[pymethods]
impl PyRdmaBuffer {
    #[classmethod]
    fn create_rdma_buffer_nonblocking(
        _cls: &Bound<'_, PyType>,
        _py: Python<'_>,
        local: PyLocalMemoryHandle,
        client: PyInstance,
    ) -> PyResult<PyPythonTask> {
        if !rdma_supported() {
            return Err(PyException::new_err("RDMA is not supported on this system"));
        }
        PyPythonTask::new(create_rdma_buffer(local, client))
    }

    #[classmethod]
    fn create_rdma_buffer_blocking(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        local: PyLocalMemoryHandle,
        client: PyInstance,
    ) -> PyResult<PyRdmaBuffer> {
        if !rdma_supported() {
            return Err(PyException::new_err("RDMA is not supported on this system"));
        }
        signal_safe_block_on(py, create_rdma_buffer(local, client))?
    }

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> String {
        format!("<RdmaBuffer'{:?}'>", self.buffer)
    }

    /// Reads from this remote RDMA buffer into a local memory region.
    ///
    /// # Arguments
    /// * `dst` - Local memory region to read into
    /// * `client` - The actor performing the read
    /// * `timeout` - Maximum time in seconds to wait for the operation
    fn read_into(
        &self,
        _py: Python<'_>,
        dst: PyLocalMemoryHandle,
        client: PyInstance,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let buffer = self.buffer.clone();

        PyPythonTask::new(async move {
            buffer
                .read_into_local(client.deref(), dst.inner, timeout)
                .await
                .map_err(|e| {
                    PyException::new_err(format!(
                        "failed to read from remote buffer into local buffer: {}",
                        e
                    ))
                })?;

            Ok(())
        })
    }

    /// Writes from a local memory region into this remote RDMA buffer.
    ///
    /// # Arguments
    /// * `src` - Local memory region to write from
    /// * `client` - The actor performing the write
    /// * `timeout` - Maximum time in seconds to wait for the operation
    fn write_from(
        &self,
        _py: Python<'_>,
        src: PyLocalMemoryHandle,
        client: PyInstance,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let buffer = self.buffer.clone();

        PyPythonTask::new(async move {
            buffer
                .write_from_local(client.deref(), src.inner, timeout)
                .await
                .map_err(|e| {
                    PyException::new_err(format!(
                        "failed to write from local buffer into remote buffer: {}",
                        e
                    ))
                })?;

            Ok(())
        })
    }

    fn size(&self) -> usize {
        self.buffer.size
    }

    fn __reduce__(&self) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        monarch_with_gil_blocking(GilSite::Rdma, |py| {
            let ctor = py.get_type::<PyRdmaBuffer>().into_py_any(py)?;
            let json = serde_json::to_string(&self.buffer).map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Serialization failed: {}", e))
            })?;

            let args = PyTuple::new(py, [json])?.into_py_any(py)?;
            Ok((ctor, args))
        })
    }

    #[new]
    fn new_from_json(json: &str) -> PyResult<Self> {
        let buffer: RdmaRemoteBuffer = serde_json::from_str(json)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("Deserialization failed: {}", e)))?;
        Ok(PyRdmaBuffer { buffer })
    }

    fn drop(&self, _py: Python<'_>, client: PyInstance) -> PyResult<PyPythonTask> {
        let buffer = self.buffer.clone();
        PyPythonTask::new(async move {
            buffer
                .drop_buffer(client.deref())
                .await
                .map_err(|e| PyException::new_err(format!("Failed to drop buffer: {}", e)))?;
            Ok(())
        })
    }

    fn owner_actor_id(&self) -> String {
        self.buffer.owner.actor_addr().to_string()
    }
}

#[pyclass(name = "_RdmaManager", module = "monarch._rust_bindings.rdma")]
pub struct PyRdmaManager {
    #[allow(dead_code)] // field never read
    inner: ActorMesh<RdmaManagerActor>,
    device: String,
}

#[pymethods]
impl PyRdmaManager {
    #[pyo3(name = "__repr__")]
    fn repr(&self) -> String {
        format!("<RdmaManager(device='{}')>", self.device)
    }

    #[getter]
    fn device(&self) -> &str {
        &self.device
    }
    /// Creates an RDMA manager actor on the given ProcMesh (async version).
    /// Returns the actor mesh if RDMA is supported, None otherwise.
    #[classmethod]
    fn create_rdma_manager_nonblocking(
        _cls: &Bound<'_, PyType>,
        proc_mesh: &Bound<'_, PyAny>,
        client: PyInstance,
    ) -> PyResult<PyPythonTask> {
        tracing::debug!("spawning RDMA manager on target proc_mesh nodes");

        let proc_mesh = proc_mesh.downcast::<PyProcMesh>()?.borrow().mesh_ref()?;
        PyPythonTask::new(async move {
            let actor_mesh: ActorMesh<RdmaManagerActor> = proc_mesh
                // Pass None to use default config - RdmaManagerActor will use default IbverbsConfig
                // TODO - make IbverbsConfig configurable
                .spawn_service(client.deref(), "rdma_manager", &None)
                .await
                .map_err(|err| PyException::new_err(err.to_string()))?;

            Ok(Some(PyRdmaManager {
                inner: actor_mesh,
                device: "remote_rdma_device".to_string(),
            }))
        })
    }
}

/// Whether ibverbs RDMA hardware is available on this system.
#[pyfunction]
#[pyo3(name = "is_ibverbs_available")]
fn is_ibverbs_available_py() -> bool {
    ibverbs_supported()
}

/// Whether any RDMA backend (ibverbs or TCP fallback) is available.
#[pyfunction]
#[pyo3(name = "rdma_supported")]
fn rdma_supported_py() -> bool {
    rdma_supported()
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Install the process-wide CUDA segment scanner, backed by
    // torch.cuda.memory._snapshot(). The mlx5dv segment binder consults it to
    // map whole allocator segments to a single indirect mkey.
    register_cuda_segment_scanner(Arc::new(pytorch_cuda_segments));

    module.add_class::<PyLocalMemoryHandle>()?;
    module.add_class::<PyWeakLocalMemoryHandle>()?;
    module.add_class::<PyRdmaBuffer>()?;
    module.add_class::<PyRdmaAction>()?;
    module.add_class::<PyRdmaManager>()?;
    py_module_add_function!(
        module,
        "monarch._rust_bindings.rdma",
        is_ibverbs_available_py
    );
    py_module_add_function!(module, "monarch._rust_bindings.rdma", rdma_supported_py);
    py_module_add_function!(
        module,
        "monarch._rust_bindings.rdma",
        _make_local_memory_handle_from_memoryview
    );
    py_module_add_function!(
        module,
        "monarch._rust_bindings.rdma",
        _make_local_memory_handle_from_tensor
    );
    py_module_add_function!(module, "monarch._rust_bindings.rdma", _assert_1d_contiguous);
    py_module_add_function!(
        module,
        "monarch._rust_bindings.rdma",
        _get_tensor_addr_and_size
    );
    py_module_add_function!(
        module,
        "monarch._rust_bindings.rdma",
        _get_memoryview_addr_and_size
    );
    Ok(())
}
