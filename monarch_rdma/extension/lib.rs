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
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_rdma::RdmaAction;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::RdmaManagerMessageClient;
use monarch_rdma::RdmaRemoteBuffer;
use monarch_rdma::ibverbs_supported;
use monarch_rdma::local_memory::Keepalive;
use monarch_rdma::local_memory::KeepaliveLocalMemory;
use monarch_rdma::rdma_supported;
use monarch_rdma::register_segment_scanner;
use monarch_types::py_module_add_function;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use typeuri::Named;

/// Segment scanner callback that uses PyTorch's memory snapshot API.
///
/// This function calls torch.cuda.memory._snapshot() to get CUDA memory segments
/// and fills the provided buffer with segment information.
///
/// # Safety
/// This function is called from C code as a callback.
unsafe extern "C" fn pytorch_segment_scanner(
    segments_out: *mut monarch_rdma::rdmaxcel_sys::rdmaxcel_scanned_segment_t,
    max_segments: usize,
) -> usize {
    // Acquire the GIL to call Python code
    // Note: We use Python::attach here instead of monarch_with_gil_blocking because
    // the raw pointer segments_out is not Sync and monarch_with_gil_blocking requires Send.
    let result = Python::attach(|py| -> PyResult<usize> {
        // Check if torch is already imported - don't import it ourselves
        let sys = py.import("sys")?;
        let modules = sys.getattr("modules")?;

        // Try to get torch from sys.modules
        let torch = match modules.get_item("torch") {
            Ok(torch_module) => torch_module,
            Err(_) => {
                // torch not imported yet, return 0 segments
                return Ok(0);
            }
        };

        // Check if CUDA is available
        let cuda_available: bool = torch
            .getattr("cuda")?
            .getattr("is_available")?
            .call0()?
            .extract()?;

        if !cuda_available {
            return Ok(0);
        }

        // Call torch.cuda.memory._snapshot()
        let snapshot = torch
            .getattr("cuda")?
            .getattr("memory")?
            .getattr("_snapshot")?
            .call0()?;

        // Get the segments list from the snapshot dict
        let segments = snapshot.get_item("segments")?;
        let segments_list: Vec<Bound<'_, PyAny>> = segments.extract()?;

        let num_segments = segments_list.len();

        // Fill the output buffer with as many segments as will fit
        let segments_to_write = num_segments.min(max_segments);

        for (i, segment) in segments_list.iter().take(segments_to_write).enumerate() {
            // Extract fields from the segment dict
            let address: u64 = segment.get_item("address")?.extract()?;
            let total_size: usize = segment.get_item("total_size")?.extract()?;
            let device: i32 = segment.get_item("device")?.extract()?;
            let is_expandable: bool = segment.get_item("is_expandable")?.extract()?;

            // Write to the output buffer - only the fields the scanner needs to provide
            let seg_info = &mut *segments_out.add(i);
            seg_info.address = address as usize;
            seg_info.size = total_size;
            seg_info.device = device;
            seg_info.is_expandable = if is_expandable { 1 } else { 0 };
        }

        // Return total number of segments found (may be > max_segments)
        Ok(num_segments)
    });

    match result {
        Ok(count) => count,
        Err(e) => {
            // Log the specific error for debugging
            eprintln!("[monarch_rdma] pytorch_segment_scanner failed: {}", e);
            0
        }
    }
}

/// Wrapper implementing [`Keepalive`] for a Python object reference.
///
/// Prevents garbage collection of the backing Python object while RDMA
/// operations are in flight.
struct PyKeepalive(#[allow(dead_code)] Py<PyAny>);

impl Keepalive for PyKeepalive {}

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

#[pymethods]
impl PyLocalMemoryHandle {
    #[new]
    fn new(obj: Py<PyAny>, addr: usize, size: usize) -> Self {
        let keepalive: Arc<dyn Keepalive> = Arc::new(PyKeepalive(obj));
        Self {
            inner: KeepaliveLocalMemory::new(addr, size, keepalive),
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

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> String {
        format!(
            "<LocalMemoryHandle addr={:#x} size={}>",
            self.inner.addr(),
            self.inner.size()
        )
    }
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
            .add_read_into_local(remote.buffer, Arc::new(local.inner))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn add_write_from_local(
        &self,
        remote: PyRdmaBuffer,
        local: PyLocalMemoryHandle,
    ) -> PyResult<()> {
        self.try_lock_sync()?
            .add_write_from_local(remote.buffer, Arc::new(local.inner))
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

    let local: Arc<KeepaliveLocalMemory> = Arc::new(local.inner);
    let buffer = owner_handle
        .request_buffer(client.deref(), local)
        .await
        .map_err(|e| PyException::new_err(format!("failed to request buffer: {}", e)))?;

    Ok(PyRdmaBuffer { buffer })
}

#[pymethods]
impl PyRdmaBuffer {
    #[classmethod]
    fn create_rdma_buffer_nonblocking<'py>(
        _cls: &Bound<'_, PyType>,
        _py: Python<'py>,
        local: PyLocalMemoryHandle,
        client: PyInstance,
    ) -> PyResult<PyPythonTask> {
        if !rdma_supported() {
            return Err(PyException::new_err("RDMA is not supported on this system"));
        }
        PyPythonTask::new(create_rdma_buffer(local, client))
    }

    #[classmethod]
    fn create_rdma_buffer_blocking<'py>(
        _cls: &Bound<'_, PyType>,
        py: Python<'py>,
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
    fn read_into<'py>(
        &self,
        _py: Python<'py>,
        dst: PyLocalMemoryHandle,
        client: PyInstance,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let buffer = self.buffer.clone();

        PyPythonTask::new(async move {
            let local_memory: Arc<KeepaliveLocalMemory> = Arc::new(dst.inner);

            buffer
                .read_into_local(client.deref(), local_memory, timeout)
                .await
                .map_err(|e| PyException::new_err(format!("failed to read into buffer: {}", e)))?;

            Ok(())
        })
    }

    /// Writes from a local memory region into this remote RDMA buffer.
    ///
    /// # Arguments
    /// * `src` - Local memory region to write from
    /// * `client` - The actor performing the write
    /// * `timeout` - Maximum time in seconds to wait for the operation
    fn write_from<'py>(
        &self,
        _py: Python<'py>,
        src: PyLocalMemoryHandle,
        client: PyInstance,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let buffer = self.buffer.clone();

        PyPythonTask::new(async move {
            let local_memory: Arc<KeepaliveLocalMemory> = Arc::new(src.inner);

            buffer
                .write_from_local(client.deref(), local_memory, timeout)
                .await
                .map_err(|e| PyException::new_err(format!("failed to write from buffer: {}", e)))?;

            Ok(())
        })
    }

    fn size(&self) -> usize {
        self.buffer.size
    }

    fn __reduce__(&self) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        monarch_with_gil_blocking(|py| {
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

    fn drop<'py>(&self, _py: Python<'py>, client: PyInstance) -> PyResult<PyPythonTask> {
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
    // Register the PyTorch segment scanner callback.
    // This calls torch.cuda.memory._snapshot() to get CUDA memory segments.
    register_segment_scanner(Some(pytorch_segment_scanner));

    module.add_class::<PyLocalMemoryHandle>()?;
    module.add_class::<PyRdmaBuffer>()?;
    module.add_class::<PyRdmaAction>()?;
    module.add_class::<PyRdmaManager>()?;
    py_module_add_function!(
        module,
        "monarch._rust_bindings.rdma",
        is_ibverbs_available_py
    );
    py_module_add_function!(module, "monarch._rust_bindings.rdma", rdma_supported_py);
    Ok(())
}
