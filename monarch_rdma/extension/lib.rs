/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::shared_cell::SharedCell;
use monarch_hyperactor::mailbox::PyMailbox;
use monarch_hyperactor::proc_mesh::PyProcMesh;
use monarch_hyperactor::pytokio::PyPythonTask;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_rdma::IbverbsConfig;
use monarch_rdma::RdmaBuffer;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::RdmaManagerMessageClient;
use monarch_rdma::ibverbs_supported;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use serde::Deserialize;
use serde::Serialize;

fn setup_rdma_context(
    rdma_buffer: &PyRdmaBuffer,
    local_proc_id: String,
) -> (ActorRef<RdmaManagerActor>, RdmaBuffer) {
    let proc_id: ProcId = local_proc_id.parse().unwrap();
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
    client: PyMailbox,
) -> PyResult<PyRdmaBuffer> {
    // Get the owning RdmaManagerActor's ActorRef
    let owner_id = ActorId(proc_id, "rdma_manager".to_string(), 0);
    let owner_ref: ActorRef<RdmaManagerActor> = ActorRef::attest(owner_id);

    let caps = client.get_inner();
    // Create the RdmaBuffer
    let buffer = owner_ref
        .request_buffer_deprecated(caps, addr, size)
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
        client: PyMailbox,
    ) -> PyResult<PyPythonTask> {
        if !ibverbs_supported() {
            return Err(PyException::new_err(
                "ibverbs is not supported on this system",
            ));
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
        client: PyMailbox,
    ) -> PyResult<PyRdmaBuffer> {
        if !ibverbs_supported() {
            return Err(PyException::new_err(
                "ibverbs is not supported on this system",
            ));
        }
        signal_safe_block_on(
            py,
            create_rdma_buffer(addr, size, proc_id.parse().unwrap(), client),
        )?
    }

    #[classmethod]
    fn rdma_supported<'py>(_cls: &Bound<'_, PyType>, _py: Python<'py>) -> bool {
        ibverbs_supported()
    }

    #[classmethod]
    fn pt_cuda_allocator_compatibility<'py>(_cls: &Bound<'_, PyType>, _py: Python<'py>) -> bool {
        monarch_rdma::pt_cuda_allocator_compatibility()
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
    /// * `client` - The mailbox for communication
    /// * `timeout` - Maximum time in milliseconds to wait for the operation
    #[pyo3(signature = (addr, size, local_proc_id, client, timeout))]
    fn read_into<'py>(
        &self,
        _py: Python<'py>,
        addr: usize,
        size: usize,
        local_proc_id: String,
        client: PyMailbox,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let (local_owner_ref, buffer) = setup_rdma_context(self, local_proc_id);
        PyPythonTask::new(async move {
            let caps = client.get_inner();
            let local_buffer = local_owner_ref
                .request_buffer_deprecated(caps, addr, size)
                .await?;
            let _result_ = local_buffer
                .write_from(caps, buffer, timeout)
                .await
                .map_err(|e| PyException::new_err(format!("failed to read into buffer: {}", e)))?;
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
    /// * `client` - The mailbox for communication
    /// * `timeout` - Maximum time in milliseconds to wait for the operation
    #[pyo3(signature = (addr, size, local_proc_id, client, timeout))]
    fn write_from<'py>(
        &self,
        _py: Python<'py>,
        addr: usize,
        size: usize,
        local_proc_id: String,
        client: PyMailbox,
        timeout: u64,
    ) -> PyResult<PyPythonTask> {
        let (local_owner_ref, buffer) = setup_rdma_context(self, local_proc_id);
        PyPythonTask::new(async move {
            let caps = client.get_inner();
            let local_buffer = local_owner_ref
                .request_buffer_deprecated(caps, addr, size)
                .await?;
            let _result_ = local_buffer
                .read_into(caps, buffer, timeout)
                .await
                .map_err(|e| PyException::new_err(format!("failed to write from buffer: {}", e)))?;
            Ok(())
        })
    }

    fn size(&self) -> usize {
        self.buffer.size
    }

    fn __reduce__(&self) -> PyResult<(PyObject, PyObject)> {
        Python::with_gil(|py| {
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

    fn drop<'py>(&self) -> PyResult<PyPythonTask> {
        // no op with CPUs, currently a stub.
        // TODO - replace with correct GPU behavior.
        PyPythonTask::new(async move { Ok(()) })
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
    fn create_rdma_manager_nonblocking<'py>(
        _cls: &Bound<'_, PyType>,
        proc_mesh: &PyProcMesh,
    ) -> PyResult<PyPythonTask> {
        // TODO - make this configurable
        let config = IbverbsConfig::default();
        tracing::debug!("rdma is enabled, using device {}", config.device);

        let tracked_proc_mesh = proc_mesh.try_inner()?;
        let device = config.device.to_string();

        PyPythonTask::new(async move {
            if !ibverbs_supported() {
                tracing::info!("rdma is not enabled on this hardware");
                return Ok(None);
            }

            let actor_mesh = tracked_proc_mesh
                .spawn::<RdmaManagerActor>("rdma_manager", &config)
                .await
                .map_err(|err| PyException::new_err(err.to_string()))?;

            Ok(Some(PyRdmaManager {
                inner: actor_mesh,
                device,
            }))
        })
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRdmaBuffer>()?;
    module.add_class::<PyRdmaManager>()?;
    Ok(())
}
