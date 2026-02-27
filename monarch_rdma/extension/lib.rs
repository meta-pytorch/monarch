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

use hyperactor_mesh::ActorMesh;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::proc_mesh::PyProcMesh;
use monarch_hyperactor::pytokio::PyPythonTask;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_rdma::RdmaLocalMemory;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::RdmaManagerMessageClient;
use monarch_rdma::RdmaRemoteBuffer;
use monarch_rdma::ibverbs_supported;
use monarch_rdma::rdma_supported;
use monarch_rdma::register_segment_scanner;
use monarch_types::py_module_add_function;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
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

/// A handle to a region of local memory that prevents the backing Python
/// object from being garbage-collected while RDMA operations are in flight.
#[pyclass(name = "_LocalMemoryHandle", module = "monarch._rust_bindings.rdma")]
#[derive(Clone)]
pub struct PyLocalMemoryHandle {
    addr: usize,
    size: usize,
    _obj: Py<PyAny>,
}

impl std::fmt::Debug for PyLocalMemoryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyLocalMemoryHandle")
            .field("addr", &self.addr)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl RdmaLocalMemory for PyLocalMemoryHandle {
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
        Self {
            addr,
            size,
            _obj: obj,
        }
    }

    #[getter]
    fn addr(&self) -> usize {
        self.addr
    }

    #[getter]
    fn size(&self) -> usize {
        self.size
    }

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> String {
        format!(
            "<LocalMemoryHandle addr={:#x} size={}>",
            self.addr, self.size
        )
    }
}

#[pyclass(name = "_RdmaBuffer", module = "monarch._rust_bindings.rdma")]
#[derive(Clone, Named)]
struct PyRdmaBuffer {
    buffer: RdmaRemoteBuffer,
}

async fn create_rdma_buffer(
    local: PyLocalMemoryHandle,
    client: PyInstance,
) -> PyResult<PyRdmaBuffer> {
    let owner_handle = RdmaManagerActor::local_handle(client.deref());

    let local: Arc<dyn RdmaLocalMemory> = Arc::new(local);
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
            let local_memory: Arc<dyn RdmaLocalMemory> = Arc::new(dst);

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
            let local_memory: Arc<dyn RdmaLocalMemory> = Arc::new(src);

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
        self.buffer.owner.actor_id().to_string()
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
    module.add_class::<PyRdmaManager>()?;
    py_module_add_function!(
        module,
        "monarch._rust_bindings.rdma",
        is_ibverbs_available_py
    );
    py_module_add_function!(module, "monarch._rust_bindings.rdma", rdma_supported_py);
    Ok(())
}
