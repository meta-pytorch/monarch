/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use hyperactor::Actor;
use hyperactor::Mailbox;
use hyperactor::RemoteMessage;
use hyperactor::WorldId;
use hyperactor::actor::RemoteActor;
use hyperactor::proc::Proc;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::alloc::Alloc;
use hyperactor_mesh::alloc::ProcStopReason;
use hyperactor_mesh::proc_mesh::ProcEvent;
use hyperactor_mesh::proc_mesh::ProcEvents;
use hyperactor_mesh::proc_mesh::ProcMesh;
use hyperactor_mesh::proc_mesh::SharedSpawnable;
use hyperactor_mesh::shared_cell::SharedCell;
use hyperactor_mesh::shared_cell::SharedCellPool;
use hyperactor_mesh::shared_cell::SharedCellRef;
use monarch_types::PickledPyObject;
use ndslice::Shape;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pycell::PyRef;
use pyo3::types::PyType;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::actor_mesh::PythonActorMesh;
use crate::alloc::PyAlloc;
use crate::mailbox::PyMailbox;
use crate::runtime::signal_safe_block_on;
use crate::shape::PyShape;
use crate::supervision::SupervisionError;

// A wrapper around `ProcMesh` which keeps track of all `RootActorMesh`s that it spawns.
pub struct TrackedProcMesh {
    inner: SharedCellRef<ProcMesh>,
    cell: SharedCell<ProcMesh>,
    children: SharedCellPool,
}

impl Debug for TrackedProcMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&*self.inner, f)
    }
}

impl Display for TrackedProcMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&*self.inner, f)
    }
}

impl From<ProcMesh> for TrackedProcMesh {
    fn from(mesh: ProcMesh) -> Self {
        let cell = SharedCell::from(mesh);
        let inner = cell.borrow().unwrap();
        Self {
            inner,
            cell,
            children: SharedCellPool::new(),
        }
    }
}

impl TrackedProcMesh {
    pub async fn spawn<A: Actor + RemoteActor>(
        &self,
        actor_name: &str,
        params: &A::Params,
    ) -> Result<SharedCell<RootActorMesh<'static, A>>, anyhow::Error>
    where
        A::Params: RemoteMessage,
    {
        let mesh = self.cell.borrow()?;
        let actor = mesh.spawn(actor_name, params).await?;
        Ok(self.children.insert(actor))
    }

    pub fn client(&self) -> &Mailbox {
        self.inner.client()
    }

    pub fn shape(&self) -> &Shape {
        self.inner.shape()
    }

    pub fn client_proc(&self) -> &Proc {
        self.inner.client_proc()
    }

    pub fn into_inner(self) -> (SharedCell<ProcMesh>, SharedCellPool) {
        (self.cell, self.children)
    }
}

#[pyclass(
    name = "ProcMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.proc_mesh"
)]
pub struct PyProcMesh {
    pub inner: SharedCell<TrackedProcMesh>,
    keepalive: Keepalive,
    proc_events: SharedCell<Mutex<ProcEvents>>,
    user_monitor_receiver: SharedCell<Mutex<mpsc::UnboundedReceiver<ProcEvent>>>,
    user_monitor_registered: Arc<AtomicBool>,
    unhealthy_event: Arc<Mutex<Option<Option<ProcEvent>>>>,
}

fn allocate_proc_mesh<'py>(py: Python<'py>, alloc: &PyAlloc) -> PyResult<Bound<'py, PyAny>> {
    let alloc = match alloc.take() {
        Some(alloc) => alloc,
        None => {
            return Err(PyException::new_err(
                "Alloc object already been used".to_string(),
            ));
        }
    };
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let world_id = alloc.world_id().clone();
        let mesh = ProcMesh::allocate(alloc)
            .await
            .map_err(|err| PyException::new_err(err.to_string()))?;
        Ok(PyProcMesh::monitored(mesh, world_id))
    })
}

fn allocate_proc_mesh_blocking<'py>(py: Python<'py>, alloc: &PyAlloc) -> PyResult<PyProcMesh> {
    let alloc = match alloc.take() {
        Some(alloc) => alloc,
        None => {
            return Err(PyException::new_err(
                "Alloc object already been used".to_string(),
            ));
        }
    };
    signal_safe_block_on(py, async move {
        let world_id = alloc.world_id().clone();
        let mesh = ProcMesh::allocate(alloc)
            .await
            .map_err(|err| PyException::new_err(err.to_string()))?;
        Ok(PyProcMesh::monitored(mesh, world_id))
    })?
}

impl PyProcMesh {
    /// Create a new [`PyProcMesh`] with self health status monitoring.
    fn monitored(mut proc_mesh: ProcMesh, world_id: WorldId) -> Self {
        let proc_events = SharedCell::from(Mutex::new(proc_mesh.events().unwrap()));
        let (user_sender, user_receiver) = mpsc::unbounded_channel::<ProcEvent>();
        let user_monitor_registered = Arc::new(AtomicBool::new(false));
        let unhealthy_event = Arc::new(Mutex::new(None));
        let monitor = tokio::spawn(Self::default_proc_mesh_monitor(
            proc_events
                .borrow()
                .expect("borrowing immediately after creation"),
            world_id,
            user_sender,
            user_monitor_registered.clone(),
            unhealthy_event.clone(),
        ));
        Self {
            inner: SharedCell::from(TrackedProcMesh::from(proc_mesh)),
            keepalive: Keepalive::new(monitor),
            proc_events,
            user_monitor_receiver: SharedCell::from(Mutex::new(user_receiver)),
            user_monitor_registered: user_monitor_registered.clone(),
            unhealthy_event,
        }
    }

    /// The default monitor of the proc mesh for crashes.
    async fn default_proc_mesh_monitor(
        events: SharedCellRef<Mutex<ProcEvents>>,
        world_id: WorldId,
        user_sender: mpsc::UnboundedSender<ProcEvent>,
        user_monitor_registered: Arc<AtomicBool>,
        unhealthy_event: Arc<Mutex<Option<Option<ProcEvent>>>>,
    ) {
        loop {
            let mut proc_events = events.lock().await;
            tokio::select! {
                event = proc_events.next() => {
                    let mut inner_unhealthy_event = unhealthy_event.lock().await;
                    match event {
                        None => {
                            *inner_unhealthy_event = Some(None);
                            tracing::info!("ProcMesh {}: alloc has stopped", world_id);
                            break;
                        }
                        Some(event) => match event {
                            // Graceful stops can be ignored.
                            ProcEvent::Stopped(_, ProcStopReason::Stopped) => continue,
                            event => {
                                *inner_unhealthy_event = Some(Some(event.clone()));
                                tracing::info!("ProcMesh {}: {}", world_id, event);
                                if user_monitor_registered.load(std::sync::atomic::Ordering::SeqCst) {
                                    if user_sender.send(event).is_err() {
                                        tracing::error!("failed to deliver the supervision event to user");
                                    }
                                }
                            }
                        }
                    }
                }
                _ = events.preempted() => {
                    let mut inner_unhealthy_event = unhealthy_event.lock().await;
                    *inner_unhealthy_event = Some(None);
                    tracing::info!("ProcMesh {}: is stopped", world_id);
                    break;
                }
            }
        }
    }

    pub fn try_inner(&self) -> PyResult<SharedCellRef<TrackedProcMesh>> {
        self.inner
            .borrow()
            .map_err(|_| PyRuntimeError::new_err("`ProcMesh` has already been stopped"))
    }

    async fn stop_mesh(
        inner: SharedCell<TrackedProcMesh>,
        proc_events: SharedCell<Mutex<ProcEvents>>,
    ) -> Result<(), anyhow::Error> {
        // "Take" the proc mesh wrapper.  Once we do, it should be impossible for new
        // actor meshes to be spawned.
        let tracked_proc_mesh = inner.take().await.map_err(|e| {
            PyRuntimeError::new_err(format!("`ProcMesh` has already been stopped: {}", e))
        })?;
        let (proc_mesh, children) = tracked_proc_mesh.into_inner();

        // Now we discard all in-flight actor meshes.  After this, the `ProcMesh` should be "unused".
        children.discard_all().await?;

        // Finally, take ownership of the inner proc mesh, which will allowing dropping it.
        let _proc_mesh = proc_mesh.take().await?;

        // Grab the alloc back from `ProcEvents` and use that to stop the mesh.
        let proc_events_taken = proc_events.take().await?;
        let mut alloc = proc_events_taken.into_inner().into_alloc();

        alloc.stop_and_wait().await?;

        anyhow::Ok(())
    }
}

// Return with error if the mesh is unhealthy.
async fn ensure_mesh_healthy(
    unhealthy_event: &Mutex<Option<Option<ProcEvent>>>,
) -> Result<(), PyErr> {
    let locked = unhealthy_event.lock().await;
    if let Some(event) = &*locked {
        let msg = match event {
            Some(e) => format!("proc mesh is stopped with reason: {:?}", e),
            None => "proc mesh is stopped with reason: alloc is stopped".to_string(),
        };
        return Err(SupervisionError::new_err(msg));
    }
    Ok(())
}

#[pymethods]
impl PyProcMesh {
    #[classmethod]
    fn allocate_nonblocking<'py>(
        _cls: &Bound<'_, PyType>,
        py: Python<'py>,
        alloc: &PyAlloc,
    ) -> PyResult<Bound<'py, PyAny>> {
        allocate_proc_mesh(py, alloc)
    }

    #[classmethod]
    fn allocate_blocking<'py>(
        _cls: &Bound<'_, PyType>,
        py: Python<'py>,
        alloc: &PyAlloc,
    ) -> PyResult<PyProcMesh> {
        allocate_proc_mesh_blocking(py, alloc)
    }

    fn spawn_nonblocking<'py>(
        &self,
        py: Python<'py>,
        name: String,
        actor: &Bound<'py, PyType>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let unhealthy_event = Arc::clone(&self.unhealthy_event);
        let pickled_type = PickledPyObject::pickle(actor.as_any())?;
        let proc_mesh = self.try_inner()?;
        let keepalive = self.keepalive.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            ensure_mesh_healthy(&unhealthy_event).await?;

            let mailbox = proc_mesh.client().clone();
            let actor_mesh = proc_mesh.spawn(&name, &pickled_type).await?;
            let actor_events = actor_mesh.with_mut(|a| a.events()).await.unwrap().unwrap();
            let python_actor_mesh = PythonActorMesh::monitored(
                actor_mesh,
                PyMailbox { inner: mailbox },
                keepalive,
                actor_events,
            );
            Python::with_gil(|py| python_actor_mesh.into_py_any(py))
        })
    }

    fn spawn_blocking<'py>(
        &self,
        py: Python<'py>,
        name: String,
        actor: &Bound<'py, PyType>,
    ) -> PyResult<PyObject> {
        let unhealthy_event = Arc::clone(&self.unhealthy_event);
        let pickled_type = PickledPyObject::pickle(actor.as_any())?;
        let proc_mesh = self.try_inner()?;
        let keepalive = self.keepalive.clone();
        signal_safe_block_on(py, async move {
            ensure_mesh_healthy(&unhealthy_event).await?;

            let mailbox = proc_mesh.client().clone();
            let actor_mesh = proc_mesh.spawn(&name, &pickled_type).await?;
            let actor_events = actor_mesh.with_mut(|a| a.events()).await.unwrap().unwrap();
            let python_actor_mesh = PythonActorMesh::monitored(
                actor_mesh,
                PyMailbox { inner: mailbox },
                keepalive,
                actor_events,
            );
            Python::with_gil(|py| python_actor_mesh.into_py_any(py))
        })?
    }

    // User can call this to monitor the proc mesh events. This will override
    // the default monitor that exits the client on process crash, so user can
    // handle the process crash in their own way.
    fn monitor<'py>(&mut self, py: Python<'py>) -> PyResult<PyObject> {
        // TODO(alberlti): remove user_monitor_registered, use take() on `user_monitor_receiver`
        if self
            .user_monitor_registered
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(PyException::new_err(
                "user already registered a monitor for this proc mesh".to_string(),
            ));
        }
        let receiver = self.user_monitor_receiver.clone();
        Ok(pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Create a new user monitor
            Ok(PyProcMeshMonitor { receiver })
        })?
        .into())
    }

    #[getter]
    fn client(&self) -> PyResult<PyMailbox> {
        Ok(PyMailbox {
            inner: self.try_inner()?.client().clone(),
        })
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<ProcMesh {}>", *self.try_inner()?))
    }

    #[getter]
    fn shape(&self) -> PyResult<PyShape> {
        Ok(self.try_inner()?.shape().clone().into())
    }

    fn stop_nonblocking<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Clone the necessary fields from self to avoid capturing self in the async block
        let inner = self.inner.clone();
        let proc_events = self.proc_events.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::stop_mesh(inner, proc_events).await?;
            PyResult::Ok(())
        })
    }

    fn stop_blocking<'py>(&self, py: Python<'py>) -> PyResult<()> {
        // Clone the necessary fields from self to avoid capturing self in the async block
        let inner = self.inner.clone();
        let proc_events = self.proc_events.clone();

        signal_safe_block_on(py, async move {
            Self::stop_mesh(inner, proc_events)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
        })?
    }
}

/// A keepalive token that aborts a task only after the last clone
/// of the token is dropped.
#[derive(Clone, Debug)]
pub(crate) struct Keepalive {
    /// The function of this field is to maintain a reference to the
    /// state.
    _state: Arc<KeepaliveState>,
}

impl Keepalive {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            _state: Arc::new(KeepaliveState(handle)),
        }
    }
}

#[derive(Debug)]
struct KeepaliveState(tokio::task::JoinHandle<()>);

impl Drop for KeepaliveState {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[pyclass(
    name = "ProcMeshMonitor",
    module = "monarch._rust_bindings.monarch_hyperactor.proc_mesh"
)]
pub struct PyProcMeshMonitor {
    receiver: SharedCell<Mutex<mpsc::UnboundedReceiver<ProcEvent>>>,
}

#[pymethods]
impl PyProcMeshMonitor {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__(&self, py: Python<'_>) -> PyResult<PyObject> {
        let receiver = self.receiver.clone();
        Ok(pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let receiver = receiver
                .borrow()
                .map_err(|_| PyRuntimeError::new_err("`ProcEvent receiver` is shutdown"))?;
            let mut proc_event_receiver = receiver.lock().await;
            tokio::select! {
                () = receiver.preempted() => {
                    Err(PyRuntimeError::new_err("shutting down `ProcEvents` receiver"))
                },
                event = proc_event_receiver.recv() => {
                    match event {
                        Some(event) => Ok(PyProcEvent::from(event)),
                        None => Err(::pyo3::exceptions::PyStopAsyncIteration::new_err(
                            "stop iteration",
                        )),
                    }
                }
            }
        })?
        .into())
    }
}

#[pyclass(
    name = "ProcEvent",
    module = "monarch._rust_bindings.monarch_hyperactor.proc_mesh"
)]
pub enum PyProcEvent {
    /// The proc of the given rank was stopped with the provided reason.
    /// The arguments represent the rank id and stop reason.
    #[pyo3(name = "Stopped")]
    Stopped(usize, String),
    /// The proc crashed, with the provided "reason". This is reserved for
    /// unhandled supervision events.
    /// The arguments represent the rank id and crash reason.
    #[pyo3(name = "Crashed")]
    Crashed(usize, String),
}

impl From<ProcEvent> for PyProcEvent {
    fn from(event: ProcEvent) -> Self {
        match event {
            ProcEvent::Stopped(pid, reason) => PyProcEvent::Stopped(pid, reason.to_string()),
            ProcEvent::Crashed(pid, reason) => PyProcEvent::Crashed(pid, reason),
        }
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyProcMesh>()?;
    hyperactor_mod.add_class::<PyProcMeshMonitor>()?;
    hyperactor_mod.add_class::<PyProcEvent>()?;
    Ok(())
}
