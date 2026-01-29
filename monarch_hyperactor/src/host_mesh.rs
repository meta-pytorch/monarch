/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use hyperactor::ActorHandle;
use hyperactor::Instance;
use hyperactor::Proc;
use hyperactor_mesh::bootstrap::BootstrapCommand;
use hyperactor_mesh::bootstrap::host;
use hyperactor_mesh::proc_mesh::default_bind_spec;
use hyperactor_mesh::proc_mesh::mesh_agent::GetProcClient;
use hyperactor_mesh::shared_cell::SharedCell;
use hyperactor_mesh::v1::ProcMeshRef;
use hyperactor_mesh::v1::host_mesh::HostMesh;
use hyperactor_mesh::v1::host_mesh::HostMeshRef;
use hyperactor_mesh::v1::host_mesh::mesh_agent::GetLocalProcClient;
use hyperactor_mesh::v1::host_mesh::mesh_agent::HostMeshAgent;
use hyperactor_mesh::v1::host_mesh::mesh_agent::ShutdownHostClient;
use hyperactor_mesh::v1::proc_mesh::ProcRef;
use ndslice::View;
use ndslice::view::RankedSliceable;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyType;

use crate::actor::PythonActor;
use crate::actor::to_py_error;
use crate::alloc::PyAlloc;
use crate::context::PyInstance;
use crate::proc_mesh::PyProcMesh;
use crate::pytokio::PyPythonTask;
use crate::runtime::monarch_with_gil;
use crate::shape::PyExtent;
use crate::shape::PyRegion;

#[pyclass(
    name = "BootstrapCommand",
    module = "monarch._rust_bindings.monarch_hyperactor.host_mesh"
)]
#[derive(Clone)]
pub struct PyBootstrapCommand {
    #[pyo3(get, set)]
    pub program: String,
    #[pyo3(get, set)]
    pub arg0: Option<String>,
    #[pyo3(get, set)]
    pub args: Vec<String>,
    #[pyo3(get, set)]
    pub env: HashMap<String, String>,
}

#[pymethods]
impl PyBootstrapCommand {
    #[new]
    fn new(
        program: String,
        arg0: Option<String>,
        args: Vec<String>,
        env: HashMap<String, String>,
    ) -> Self {
        Self {
            program,
            arg0,
            args,
            env,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "BootstrapCommand(program='{}', args={:?}, env={:?})",
            self.program, self.args, self.env
        )
    }
}

impl PyBootstrapCommand {
    pub fn to_rust(&self) -> BootstrapCommand {
        BootstrapCommand {
            program: PathBuf::from(&self.program),
            arg0: self.arg0.clone(),
            args: self.args.clone(),
            env: self.env.clone(),
        }
    }

    pub fn default<'py>(py: Python<'py>) -> PyResult<Bound<'py, Self>> {
        py.import("monarch._src.actor.host_mesh")?
            .getattr("_bootstrap_cmd")?
            .call0()?
            .downcast::<PyBootstrapCommand>()
            .cloned()
            .map_err(to_py_error)
    }
}

#[pyclass(
    name = "HostMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.host_mesh"
)]
pub(crate) enum PyHostMesh {
    Owned(PyHostMeshImpl),
    Ref(PyHostMeshRefImpl),
}

impl PyHostMesh {
    pub(crate) fn new_owned(inner: HostMesh) -> Self {
        Self::Owned(PyHostMeshImpl(SharedCell::from(inner)))
    }

    pub(crate) fn new_ref(inner: HostMeshRef) -> Self {
        Self::Ref(PyHostMeshRefImpl(inner))
    }

    fn mesh_ref(&self) -> Result<HostMeshRef, anyhow::Error> {
        match self {
            PyHostMesh::Owned(inner) => Ok(inner.0.borrow()?.clone()),
            PyHostMesh::Ref(inner) => Ok(inner.0.clone()),
        }
    }
}

#[pymethods]
impl PyHostMesh {
    #[classmethod]
    fn allocate_nonblocking(
        _cls: &Bound<'_, PyType>,
        instance: &PyInstance,
        alloc: &mut PyAlloc,
        name: String,
        bootstrap_params: Option<PyBootstrapCommand>,
    ) -> PyResult<PyPythonTask> {
        let bootstrap_params =
            bootstrap_params.map_or_else(|| alloc.bootstrap_command.clone(), |b| Some(b.to_rust()));
        let alloc = match alloc.take() {
            Some(alloc) => alloc,
            None => {
                return Err(PyException::new_err(
                    "Alloc object already used".to_string(),
                ));
            }
        };
        let instance = instance.clone();
        PyPythonTask::new(async move {
            let mesh = HostMesh::allocate(instance.deref(), alloc, &name, bootstrap_params)
                .await
                .map_err(|err| PyException::new_err(err.to_string()))?;
            Ok(Self::new_owned(mesh))
        })
    }

    fn spawn_nonblocking(
        &self,
        instance: &PyInstance,
        name: String,
        per_host: &PyExtent,
    ) -> PyResult<PyPythonTask> {
        let host_mesh = self.mesh_ref()?.clone();
        let instance = instance.clone();
        let per_host = per_host.clone().into();
        let mesh_impl = async move {
            let proc_mesh = host_mesh
                .spawn(instance.deref(), &name, per_host)
                .await
                .map_err(to_py_error)?;
            Ok(PyProcMesh::new_owned(proc_mesh))
        };
        PyPythonTask::new(mesh_impl)
    }

    fn sliced(&self, region: &PyRegion) -> PyResult<Self> {
        Ok(Self::new_ref(
            self.mesh_ref()?.sliced(region.as_inner().clone()),
        ))
    }

    #[getter]
    fn region(&self) -> PyResult<PyRegion> {
        Ok(PyRegion::from(self.mesh_ref()?.region()))
    }

    /// Get the name of this host mesh (without UUID suffix).
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.mesh_ref()?.name().name().to_string())
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let bytes = bincode::serialize(&self.mesh_ref()?)
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let from_bytes =
            PyModule::import(py, "monarch._rust_bindings.monarch_hyperactor.host_mesh")?
                .getattr("py_host_mesh_from_bytes")?;
        Ok((from_bytes, py_bytes))
    }

    fn __eq__(&self, other: &PyHostMesh) -> PyResult<bool> {
        Ok(self.mesh_ref()? == other.mesh_ref()?)
    }

    fn shutdown(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        match self {
            PyHostMesh::Owned(inner) => {
                let instance = instance.clone();
                let mesh_borrow = inner.0.clone();
                let fut = async move {
                    match mesh_borrow.take().await {
                        Ok(mut mesh) => {
                            mesh.shutdown(instance.deref()).await?;
                            Ok(())
                        }
                        Err(_) => {
                            // Don't return an exception, silently ignore the stop request
                            // because it was already done.
                            tracing::info!("shutdown was already called on host mesh");
                            Ok(())
                        }
                    }
                };
                PyPythonTask::new(fut)
            }
            PyHostMesh::Ref(_) => Err(PyRuntimeError::new_err(
                "cannot shut down `HostMesh` that is a reference instead of owned",
            )),
        }
    }
}

#[derive(Clone)]
#[pyclass(
    name = "HostMeshImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.host_mesh"
)]
pub(crate) struct PyHostMeshImpl(SharedCell<HostMesh>);

#[derive(Debug, Clone)]
#[pyclass(
    name = "HostMeshRefImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.host_mesh"
)]
pub(crate) struct PyHostMeshRefImpl(HostMeshRef);

impl PyHostMeshRefImpl {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<HostMeshRefImpl {:?}>", self.0))
    }
}

/// Static storage for the root client instance when using host-based bootstrap.
static ROOT_CLIENT_INSTANCE_FOR_HOST: OnceLock<Instance<PythonActor>> = OnceLock::new();

/// Static storage for the host mesh agent created by bootstrap_host().
static HOST_MESH_AGENT_FOR_HOST: OnceLock<ActorHandle<HostMeshAgent>> = OnceLock::new();

/// Bootstrap the client host and root client actor.
///
/// This creates a proper Host with BootstrapProcManager, spawns the root client
/// actor on the Host's local_proc.
///
/// Returns a tuple of (HostMesh, ProcMesh, PyInstance) where:
/// - PyHostMesh: the bootstrapped (local) host mesh; and
/// - PyProcMesh: the local ProcMesh on this HostMesh; and
/// - PyInstance: the root client actor instance, on the ProcMesh.
///
/// The HostMesh is served on the default transport.
///
/// This should be called only once, at process initialization
#[pyfunction]
fn bootstrap_host(bootstrap_cmd: Option<PyBootstrapCommand>) -> PyResult<PyPythonTask> {
    let bootstrap_cmd = match bootstrap_cmd {
        Some(cmd) => cmd.to_rust(),
        None => BootstrapCommand::current().map_err(|e| PyException::new_err(e.to_string()))?,
    };

    PyPythonTask::new(async move {
        let host_mesh_agent = host(
            default_bind_spec().binding_addr(),
            Some(bootstrap_cmd),
            None,
        )
        .await
        .map_err(|e| PyException::new_err(e.to_string()))?;

        // Store the agent for later shutdown
        HOST_MESH_AGENT_FOR_HOST.set(host_mesh_agent.clone()).ok(); // Ignore error if already set

        let host_mesh_name = hyperactor_mesh::v1::Name::new_reserved("local").unwrap();
        let host_mesh = HostMeshRef::from_host_agent(host_mesh_name, host_mesh_agent.bind())
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // We require a temporary instance to make a call to the host/proc agent.
        let temp_proc = Proc::local();
        let (temp_instance, _) = temp_proc
            .instance("temp")
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let local_proc_agent: hyperactor::ActorHandle<
            hyperactor_mesh::proc_mesh::mesh_agent::ProcMeshAgent,
        > = host_mesh_agent
            .get_local_proc(&temp_instance)
            .await
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let proc_mesh_name = hyperactor_mesh::v1::Name::new_reserved("local").unwrap();
        let proc_mesh = ProcMeshRef::new_singleton(
            proc_mesh_name,
            ProcRef::new(
                local_proc_agent.actor_id().proc_id().clone(),
                0,
                local_proc_agent.bind(),
            ),
        );

        let local_proc = local_proc_agent
            .get_proc(&temp_instance)
            .await
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let (instance, _handle) = monarch_with_gil(|py| {
            PythonActor::bootstrap_client_inner(py, local_proc, &ROOT_CLIENT_INSTANCE_FOR_HOST)
        })
        .await;

        Ok((
            PyHostMesh::new_ref(host_mesh),
            PyProcMesh::new_ref(proc_mesh),
            PyInstance::from(instance),
        ))
    })
}

#[pyfunction]
fn py_host_mesh_from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<PyHostMesh> {
    let r: PyResult<HostMeshRef> = bincode::deserialize(bytes.as_bytes())
        .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()));
    r.map(PyHostMesh::new_ref)
}

#[pyfunction]
fn shutdown_local_host_mesh() -> PyResult<PyPythonTask> {
    let agent = HOST_MESH_AGENT_FOR_HOST
        .get()
        .ok_or_else(|| PyException::new_err("No local host mesh to shutdown"))?
        .clone();

    PyPythonTask::new(async move {
        // Create a temporary instance to send the shutdown message
        let temp_proc = hyperactor::Proc::local();
        let (instance, _) = temp_proc
            .instance("shutdown_requester")
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // Use same defaults as HostMesh::shutdown():
        // - MESH_TERMINATE_TIMEOUT = 10 seconds
        // - MESH_TERMINATE_CONCURRENCY = 16
        agent
            .shutdown_host(&instance, Duration::from_secs(10), 16)
            .await
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(())
    })
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(py_host_mesh_from_bytes, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.host_mesh",
    )?;
    hyperactor_mod.add_function(f)?;

    let f2 = wrap_pyfunction!(bootstrap_host, hyperactor_mod)?;
    f2.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.host_mesh",
    )?;
    hyperactor_mod.add_function(f2)?;

    let f3 = wrap_pyfunction!(shutdown_local_host_mesh, hyperactor_mod)?;
    f3.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.host_mesh",
    )?;
    hyperactor_mod.add_function(f3)?;

    hyperactor_mod.add_class::<PyHostMesh>()?;
    hyperactor_mod.add_class::<PyBootstrapCommand>()?;
    Ok(())
}
