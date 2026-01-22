/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]
use std::ops::Deref;

use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::ProcId;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::shared_cell::SharedCell;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::proc_mesh::PyProcMesh;
use monarch_hyperactor::pytokio::PyPythonTask;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_hyperactor::v1::proc_mesh::PyProcMesh as PyProcMeshV1;
use monarch_rdma::RdmaBuffer;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::RdmaManagerMessageClient;
use monarch_rdma::rdma_supported;
use monarch_rdma::register_segment_scanner;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use serde::Deserialize;
use serde::Serialize;
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
    // Note: We use Python::with_gil here instead of monarch_with_gil_blocking because
    // the raw pointer segments_out is not Sync and monarch_with_gil_blocking requires Send.
    let result = Python::with_gil(|py| -> PyResult<usize> {
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

fn setup_rdma_context(
    rdma_buffer: &PyRdmaBuffer,
    local_proc_id: String,
) -> (ActorRef<RdmaManagerActor>, RdmaBuffer) {
    let proc_id: ProcId = local_proc_id.parse().unwrap();
    // TODO: find some better way to look this up, or else formally define "service names"
    let local_owner_id = ActorId(proc_id, "rdma_manager".to_string(), 0);
    let local_owner_ref: ActorRef<RdmaManagerActor> = ActorRef::attest(local_owner_id);
    let buffer = rdma_buffer.buffer.clone();
    (local_owner_ref, buffer)
}

#[pyclass(name = "_RdmaBuffer", module = "monarch._rust_bindings.rdma")]
#[derive(Clone, Serialize, Deserialize, Named)]
struct PyRdmaBuffer {
    buffer: RdmaBuffer,
    owner_ref: ActorRef<RdmaManagerActor>,
}

async fn create_rdma_buffer(
    addr: usize,
    size: usize,
    proc_id: ProcId,
    client: PyInstance,
) -> PyResult<PyRdmaBuffer> {
    // Get the owning RdmaManagerActor's ActorRef
    // TODO: find some better way to look this up, or else formally define "service names"
    let owner_id = ActorId(proc_id, "rdma_manager".to_string(), 0);
    let owner_ref: ActorRef<RdmaManagerActor> = ActorRef::attest(owner_id);

    // Create the RdmaBuffer
    let buffer = owner_ref
        .request_buffer_deprecated(client.deref(), addr, size)
        .await?;
    Ok(PyRdmaBuffer { buffer, owner_ref })
}

#[pymethods]
impl PyRdmaBuffer {
    #[classmethod]
    fn create_rdma_buffer_nonblocking<'py>(
        _cls: &Bound<'_, PyType>,
        _py: Python<'py>,
        addr: usize,
        size: usize,
        proc_id: String,
        client: PyInstance,
    ) -> PyResult<PyPythonTask> {
        if !rdma_supported() {
            return Err(PyException::new_err("RDMA is not supported on this system"));
        }
        PyPythonTask::new(create_rdma_buffer(
            addr,
            size,
            proc_id.parse().unwrap(),
            client,
        ))
    }

    #[classmethod]
    fn create_rdma_buffer_blocking<'py>(
        _cls: &Bound<'_, PyType>,
        py: Python<'py>,
        addr: usize,
        size: usize,
        proc_id: String,
        client: PyInstance,
    ) -> PyResult<PyRdmaBuffer> {
        if !rdma_supported() {
            return Err(PyException::new_err("RDMA is not supported on this system"));
        }
        signal_safe_block_on(
            py,
            create_rdma_buffer(addr, size, proc_id.parse().unwrap(), client),
        )?
    }

    #[classmethod]
    fn rdma_supported<'py>(_cls: &Bound<'_, PyType>, _py: Python<'py>) -> bool {
        rdma_supported()
    }

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> String {
        format!("<RdmaBuffer'{:?}'>", self.buffer)
    }

    /// Reads data from the local buffer and places it into this remote RDMA buffer.
    ///
    /// This operation appears as "read_into" from the caller's perspective (reading from local memory
    /// into the remote buffer), but internally it's implemented as a "write_from" operation on the
    /// local buffer since the data flows from the local buffer to the remote one.
    ///
    /// # Arguments
    /// * `addr` - The address of the local buffer to read from
    /// * `size` - The size of the data to transfer
    /// * `local_proc_id` - The process ID where the local buffer resides
    /// * `client` - The actor who does the reading.
    /// * `timeout` - Maximum time in milliseconds to wait for the operation
    #[pyo3(signature = (addr, size, local_proc_id, client, timeout))]
    fn read_into<'py>(
        &self,
        _py: Python<'py>,
        addr: usize,
        size: usize,
        local_proc_id: String,
        client: PyInstance,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let (local_owner_ref, buffer) = setup_rdma_context(self, local_proc_id);
        PyPythonTask::new(async move {
            let local_buffer = local_owner_ref
                .request_buffer_deprecated(client.deref(), addr, size)
                .await?;
            local_buffer
                .write_from(client.deref(), buffer, timeout)
                .await
                .map_err(|e| PyException::new_err(format!("failed to read into buffer: {}", e)))?;
            local_owner_ref
                .release_buffer_deprecated(client.deref(), local_buffer)
                .await?;
            Ok(())
        })
    }

    /// Writes data from this remote RDMA buffer into a local buffer.
    ///
    /// This operation appears as "write_from" from the caller's perspective (writing from the remote
    /// buffer into local memory), but internally it's implemented as a "read_into" operation on the
    /// local buffer since the data flows from the remote buffer to the local one.
    ///
    /// # Arguments
    /// * `addr` - The address of the local buffer to write to
    /// * `size` - The size of the data to transfer
    /// * `local_proc_id` - The process ID where the local buffer resides
    /// * `client` - The actor who does the writing
    /// * `timeout` - Maximum time in milliseconds to wait for the operation
    #[pyo3(signature = (addr, size, local_proc_id, client, timeout))]
    fn write_from<'py>(
        &self,
        _py: Python<'py>,
        addr: usize,
        size: usize,
        local_proc_id: String,
        client: PyInstance,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let (local_owner_ref, buffer) = setup_rdma_context(self, local_proc_id);
        PyPythonTask::new(async move {
            let local_buffer = local_owner_ref
                .request_buffer_deprecated(client.deref(), addr, size)
                .await?;
            local_buffer
                .read_into(client.deref(), buffer, timeout)
                .await
                .map_err(|e| PyException::new_err(format!("failed to write from buffer: {}", e)))?;
            local_owner_ref
                .release_buffer_deprecated(client.deref(), local_buffer)
                .await?;
            Ok(())
        })
    }

    fn size(&self) -> usize {
        self.buffer.size
    }

    fn __reduce__(&self) -> PyResult<(PyObject, PyObject)> {
        monarch_with_gil_blocking(|py| {
            let ctor = py.get_type::<PyRdmaBuffer>().into_py_any(py)?;
            let json = serde_json::to_string(self).map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Serialization failed: {}", e))
            })?;

            let args = PyTuple::new(py, [json])?.into_py_any(py)?;
            Ok((ctor, args))
        })
    }

    #[new]
    fn new_from_json(json: &str) -> PyResult<Self> {
        let deserialized: PyRdmaBuffer = serde_json::from_str(json)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("Deserialization failed: {}", e)))?;
        Ok(deserialized)
    }

    fn drop<'py>(
        &self,
        _py: Python<'py>,
        local_proc_id: String,
        client: PyInstance,
    ) -> PyResult<PyPythonTask> {
        let (_local_owner_ref, buffer) = setup_rdma_context(self, local_proc_id);
        PyPythonTask::new(async move {
            buffer
                .drop_buffer(client.deref())
                .await
                .map_err(|e| PyException::new_err(format!("Failed to drop buffer: {}", e)))?;
            Ok(())
        })
    }

    fn owner_actor_id(&self) -> String {
        self.owner_ref.actor_id().to_string()
    }
}

#[pyclass(name = "_RdmaManager", module = "monarch._rust_bindings.rdma")]
pub struct PyRdmaManager {
    #[allow(dead_code)] // field never read
    inner: SharedCell<RootActorMesh<'static, RdmaManagerActor>>,
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

        if let Ok(v0) = proc_mesh.downcast::<PyProcMesh>() {
            let tracked_proc_mesh = v0.borrow().try_inner()?;
            PyPythonTask::new(async move {
                // Spawns the `RdmaManagerActor` on the target proc_mesh.
                // This allows the `RdmaController` to run on any node while real RDMA operations occur on appropriate hardware.
                let actor_mesh: SharedCell<RootActorMesh<RdmaManagerActor>> = tracked_proc_mesh
                    // Pass None to use default config - RdmaManagerActor will use default IbverbsConfig
                    // TODO - make IbverbsConfig configurable
                    .spawn(client.deref(), "rdma_manager", &None)
                    .await
                    .map_err(|err| PyException::new_err(err.to_string()))?;

                // Use placeholder device name since actual device is determined on remote node
                Ok(Some(PyRdmaManager {
                    inner: actor_mesh,
                    device: "remote_rdma_device".to_string(),
                }))
            })
        } else {
            let proc_mesh = proc_mesh.downcast::<PyProcMeshV1>()?.borrow().mesh_ref()?;
            PyPythonTask::new(async move {
                let actor_mesh: hyperactor_mesh::v1::ActorMesh<RdmaManagerActor> = proc_mesh
                    // Pass None to use default config - RdmaManagerActor will use default IbverbsConfig
                    // TODO - make IbverbsConfig configurable
                    .spawn_service(client.deref(), "rdma_manager", &None)
                    .await
                    .map_err(|err| PyException::new_err(err.to_string()))?;

                let actor_mesh = RootActorMesh::from(actor_mesh);
                let actor_mesh = SharedCell::from(actor_mesh);

                Ok(Some(PyRdmaManager {
                    inner: actor_mesh,
                    device: "remote_rdma_device".to_string(),
                }))
            })
        }
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register the PyTorch segment scanner callback.
    // This calls torch.cuda.memory._snapshot() to get CUDA memory segments.
    register_segment_scanner(Some(pytorch_segment_scanner));

    module.add_class::<PyRdmaBuffer>()?;
    module.add_class::<PyRdmaManager>()?;
    Ok(())
}
