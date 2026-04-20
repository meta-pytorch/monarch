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
use hyperactor_mesh::ProcMeshRef;
use hyperactor_mesh::bootstrap::BootstrapCommand;
use hyperactor_mesh::bootstrap::ProcBind;
use hyperactor_mesh::bootstrap::host;
use hyperactor_mesh::host_mesh;
use hyperactor_mesh::host_mesh::HostMesh;
use hyperactor_mesh::host_mesh::HostMeshRef;
use hyperactor_mesh::host_mesh::host_agent::GetLocalProcClient;
use hyperactor_mesh::host_mesh::host_agent::HostAgent;
use hyperactor_mesh::host_mesh::host_agent::ShutdownHost;
use hyperactor_mesh::mesh_admin::MeshAdminMessageClient;
use hyperactor_mesh::proc_agent::GetProcClient;
use hyperactor_mesh::proc_mesh::ProcRef;
use hyperactor_mesh::shared_cell::SharedCell;
use hyperactor_mesh::transport::default_bind_spec;
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

    #[pyo3(signature = (instance, name, per_host, proc_bind = None))]
    fn spawn_nonblocking(
        &self,
        instance: &PyInstance,
        name: String,
        per_host: &PyExtent,
        proc_bind: Option<Vec<HashMap<String, String>>>,
    ) -> PyResult<PyPythonTask> {
        let host_mesh = self.mesh_ref()?.clone();
        let instance = instance.clone();
        let per_host = per_host.clone().into();
        let proc_bind = proc_bind.map(|v| v.into_iter().map(ProcBind::from).collect());
        let mesh_impl = async move {
            let proc_mesh = host_mesh
                .spawn(instance.deref(), &name, per_host, proc_bind)
                .await
                .map_err(to_py_error)?;
            Ok(PyProcMesh::new_owned(proc_mesh))
        };
        PyPythonTask::new(mesh_impl)
    }

    fn with_bootstrap(&self, bootstrap_command: &PyBootstrapCommand) -> PyResult<Self> {
        match self {
            PyHostMesh::Owned(inner) => {
                let cmd = bootstrap_command.to_rust();
                inner
                    .0
                    .try_with_mut(|mesh| mesh.set_bootstrap(cmd))
                    .map_err(|e| PyException::new_err(e.to_string()))?;
                Ok(Self::Owned(inner.clone()))
            }
            PyHostMesh::Ref(_) => Ok(Self::new_ref(
                self.mesh_ref()?.with_bootstrap(bootstrap_command.to_rust()),
            )),
        }
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

    fn stop(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        match self {
            PyHostMesh::Owned(inner) => {
                let instance = instance.clone();
                let mesh_borrow = inner.0.clone();
                let fut = async move {
                    match mesh_borrow.take().await {
                        Ok(mut mesh) => {
                            mesh.stop(instance.deref()).await?;
                            Ok(())
                        }
                        Err(_) => {
                            tracing::info!("stop was already called on host mesh");
                            Ok(())
                        }
                    }
                };
                PyPythonTask::new(fut)
            }
            PyHostMesh::Ref(_) => Err(PyRuntimeError::new_err(
                "cannot stop `HostMesh` that is a reference instead of owned",
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
static HOST_MESH_AGENT_FOR_HOST: OnceLock<ActorHandle<HostAgent>> = OnceLock::new();

/// Static storage for the host shutdown handle created by bootstrap_host().
/// Used during shutdown_context to join the mailbox server and flush
/// receive-side acks.
static HOST_SHUTDOWN_HANDLE: OnceLock<
    tokio::sync::Mutex<Option<hyperactor_mesh::bootstrap::HostShutdownHandle>>,
> = OnceLock::new();

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
        let (host_mesh_agent, shutdown_handle) = host(
            default_bind_spec().binding_addr(),
            Some(bootstrap_cmd),
            None,
            false,
        )
        .await
        .map_err(|e| PyException::new_err(e.to_string()))?;

        // Store the agent and shutdown handle for later shutdown
        HOST_MESH_AGENT_FOR_HOST.set(host_mesh_agent.clone()).ok();
        HOST_SHUTDOWN_HANDLE.get_or_init(|| tokio::sync::Mutex::new(Some(shutdown_handle)));

        let host_mesh_id = hyperactor_mesh::mesh_id::HostMeshId::singleton(
            hyperactor::id::Label::new("local").unwrap(),
        );
        let host_mesh = HostMeshRef::from_host_agent(host_mesh_id, host_mesh_agent.bind())
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // Register C so MeshAdminAgent can discover it ("A/C
        // invariant" - hyperactor_mesh/src/mesh_admin.rs).
        hyperactor_mesh::global_context::register_client_host(host_mesh.clone());

        // We require a temporary instance to make a call to the host/proc agent.
        let temp_proc = Proc::local();
        let (temp_instance, _) = temp_proc
            .instance("temp")
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let local_proc_agent: hyperactor::ActorHandle<hyperactor_mesh::proc_agent::ProcAgent> =
            host_mesh_agent
                .get_local_proc(&temp_instance)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

        let proc_mesh = ProcMeshRef::new_singleton(
            hyperactor_mesh::mesh_id::ProcMeshId::singleton(
                hyperactor::id::Label::new("local").unwrap(),
            ),
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

        // Notify telemetry of the bootstrap host mesh, proc mesh, and client actor.
        {
            let now = std::time::SystemTime::now();

            let host_name_str = host_mesh.id().to_string();
            let host_mesh_id = hyperactor_telemetry::hash_to_u64(&host_name_str);
            hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
                id: host_mesh_id,
                timestamp: now,
                class: "Host".to_string(),
                given_name: host_mesh
                    .id()
                    .label()
                    .map(|l| l.as_str())
                    .unwrap_or("unnamed")
                    .to_string(),
                full_name: host_name_str,
                shape_json: serde_json::to_string(&host_mesh.region().extent()).unwrap_or_default(),
                parent_mesh_id: None,
                parent_view_json: None,
            });

            let host_agent_id = host_mesh_agent.actor_id();
            hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                id: hyperactor_telemetry::hash_to_u64(host_agent_id),
                timestamp: now,
                mesh_id: host_mesh_id,
                rank: 0,
                full_name: host_agent_id.to_string(),
                display_name: None,
            });

            let proc_id_str = proc_mesh.id().to_string();
            let proc_mesh_id = hyperactor_telemetry::hash_to_u64(&proc_id_str);
            hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
                id: proc_mesh_id,
                timestamp: now,
                class: "Proc".to_string(),
                given_name: proc_mesh
                    .id()
                    .label()
                    .map(|l| l.as_str())
                    .unwrap_or("unnamed")
                    .to_string(),
                full_name: proc_id_str,
                shape_json: serde_json::to_string(&proc_mesh.region().extent()).unwrap_or_default(),
                parent_mesh_id: Some(host_mesh_id),
                parent_view_json: None,
            });

            let proc_agent_id = local_proc_agent.actor_id();
            hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                id: hyperactor_telemetry::hash_to_u64(proc_agent_id),
                timestamp: now,
                mesh_id: proc_mesh_id,
                rank: 0,
                full_name: proc_agent_id.to_string(),
                display_name: None,
            });

            let client_mesh_name = format!("{}/client", proc_mesh.id());
            let client_mesh_id = hyperactor_telemetry::hash_to_u64(&client_mesh_name);
            hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
                id: client_mesh_id,
                timestamp: now,
                class: <PythonActor as typeuri::Named>::typename().to_string(),
                given_name: "client".to_string(),
                full_name: client_mesh_name,
                shape_json: serde_json::to_string(&proc_mesh.region().extent()).unwrap_or_default(),
                parent_mesh_id: Some(proc_mesh_id),
                parent_view_json: None,
            });

            hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                id: hyperactor_telemetry::hash_to_u64(instance.self_id()),
                timestamp: now,
                mesh_id: client_mesh_id,
                rank: 0,
                full_name: instance.self_id().to_string(),
                display_name: Some("<root>".to_string()),
            });
        }

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

        tracing::info!(
            "sending shutdown_host request to agent {}",
            agent.actor_id()
        );
        // Use same defaults as HostMesh::shutdown():
        // - MESH_TERMINATE_TIMEOUT = 10 seconds
        // - MESH_TERMINATE_CONCURRENCY = 16

        let (port, _) = instance.open_port();
        let mut port = port.bind();
        // We don't need the ack, and this temporary proc doesn't have a mailbox
        // receiver set up anyways. Just ignore the message.
        port.return_undeliverable(false);
        agent
            .send(
                &instance,
                ShutdownHost {
                    timeout: Duration::from_secs(10),
                    max_in_flight: 16,
                    ack: port,
                },
            )
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // Join the host's mailbox server to flush receive-side acks
        // before the process exits.
        if let Some(lock) = HOST_SHUTDOWN_HANDLE.get() {
            if let Some(handle) = lock.lock().await.take() {
                handle.join().await;
            }
        }

        Ok(())
    })
}

/// Opaque capability token for `ActorRef<MeshAdminAgent>` across the
/// Python boundary. No methods, no getters — Python never inspects
/// this. It exists solely to transport the typed ref from
/// `_spawn_admin` to `_start_periodic_snapshots`.
#[pyclass(
    name = "PyMeshAdminRef",
    module = "monarch._rust_bindings.monarch_hyperactor.host_mesh"
)]
#[derive(Clone)]
pub struct PyMeshAdminRef(
    hyperactor::reference::ActorRef<hyperactor_mesh::mesh_admin::MeshAdminAgent>,
);

impl PyMeshAdminRef {
    pub fn actor_ref(
        &self,
    ) -> hyperactor::reference::ActorRef<hyperactor_mesh::mesh_admin::MeshAdminAgent> {
        self.0.clone()
    }
}

/// `MESH_ADMIN_ADDR` config.
///
/// Python-facing wrapper around
/// [`hyperactor_mesh::host_mesh::spawn_admin`].
#[pyfunction]
fn _spawn_admin(
    host_meshes: Vec<PyRef<'_, PyHostMesh>>,
    instance: &PyInstance,
    admin_addr: Option<String>,
    telemetry_url: Option<String>,
) -> PyResult<PyPythonTask> {
    if host_meshes.is_empty() {
        return Err(PyException::new_err("at least one mesh is required"));
    }

    let admin_addr = admin_addr
        .map(|s| {
            s.parse::<std::net::SocketAddr>()
                .map_err(|e| PyException::new_err(format!("invalid admin_addr '{}': {}", s, e)))
        })
        .transpose()?;

    let mesh_refs = host_meshes
        .iter()
        .map(|m| -> PyResult<HostMeshRef> { Ok(m.mesh_ref()?.clone()) })
        .collect::<PyResult<Vec<HostMeshRef>>>()?;

    let instance = instance.clone();
    PyPythonTask::new(async move {
        let admin_ref =
            host_mesh::spawn_admin(&mesh_refs, instance.deref(), admin_addr, telemetry_url)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
        let admin_url = admin_ref
            .get_admin_addr(instance.deref())
            .await
            .map_err(|e| PyException::new_err(e.to_string()))?
            .addr
            .ok_or_else(|| PyException::new_err("mesh admin agent did not report an address"))?;
        Ok((admin_url, PyMeshAdminRef(admin_ref)))
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

    let f4 = wrap_pyfunction!(_spawn_admin, hyperactor_mod)?;
    f4.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.host_mesh",
    )?;
    hyperactor_mod.add_function(f4)?;

    hyperactor_mod.add_class::<PyHostMesh>()?;
    hyperactor_mod.add_class::<PyBootstrapCommand>()?;
    hyperactor_mod.add_class::<PyMeshAdminRef>()?;
    Ok(())
}
