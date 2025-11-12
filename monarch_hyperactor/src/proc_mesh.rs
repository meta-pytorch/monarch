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
use hyperactor::RemoteMessage;
use hyperactor::WorldId;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::context::Mailbox as _;
use hyperactor::proc::Instance;
use hyperactor::proc::Proc;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::alloc::ProcStopReason;
use hyperactor_mesh::proc_mesh::ProcEvent;
use hyperactor_mesh::proc_mesh::ProcEvents;
use hyperactor_mesh::proc_mesh::ProcMesh;
use hyperactor_mesh::proc_mesh::SharedSpawnable;
use hyperactor_mesh::proc_mesh::global_root_client;
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

type OnStopCallback = Box<dyn FnOnce() -> Box<dyn std::future::Future<Output = ()> + Send> + Send>;

use crate::actor_mesh::ActorMeshProtocol;
use crate::actor_mesh::PythonActorMesh;
use crate::actor_mesh::PythonActorMeshImpl;
use crate::alloc::PyAlloc;
use crate::context::PyInstance;
use crate::mailbox::PyMailbox;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;
use crate::pytokio::PythonTask;
use crate::shape::PyRegion;
use crate::supervision::SupervisionError;
use crate::supervision::Unhealthy;

// A wrapper around `ProcMesh` which keeps track of all `RootActorMesh`s that it spawns.
pub struct TrackedProcMesh {
    inner: SharedCellRef<ProcMesh>,
    cell: SharedCell<ProcMesh>,
    children: SharedCellPool,
    onstop_callbacks: Arc<Mutex<Vec<OnStopCallback>>>,
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
            onstop_callbacks: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl TrackedProcMesh {
    pub async fn spawn<A: Actor + Referable>(
        &self,
        cx: &impl context::Actor,
        actor_name: &str,
        params: &A::Params,
    ) -> Result<SharedCell<RootActorMesh<'static, A>>, anyhow::Error>
    where
        A::Params: RemoteMessage,
    {
        let mesh = self.cell.borrow()?;
        let actor = mesh.spawn(cx, actor_name, params).await?;
        Ok(self.children.insert(actor))
    }

    pub fn client(&self) -> &Instance<()> {
        self.inner.client()
    }

    pub fn shape(&self) -> &Shape {
        self.inner.shape()
    }

    pub fn client_proc(&self) -> &Proc {
        self.inner.client_proc()
    }

    pub fn into_inner(
        self,
    ) -> (
        SharedCell<ProcMesh>,
        SharedCellPool,
        Arc<Mutex<Vec<OnStopCallback>>>,
    ) {
        (self.cell, self.children, self.onstop_callbacks)
    }

    /// Register a callback to be called when this TrackedProcMesh is stopped
    pub async fn register_onstop_callback<F, Fut>(&self, callback: F) -> Result<(), anyhow::Error>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut callbacks = self.onstop_callbacks.lock().await;
        callbacks.push(Box::new(|| Box::new(callback())));
        Ok(())
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
    unhealthy_event: Arc<Mutex<Unhealthy<ProcEvent>>>,
}

fn allocate_proc_mesh(alloc: &mut PyAlloc) -> PyResult<PyPythonTask> {
    let alloc = match alloc.take() {
        Some(alloc) => alloc,
        None => {
            return Err(PyException::new_err(
                "Alloc object already been used".to_string(),
            ));
        }
    };
    PyPythonTask::new(async move {
        let world_id = alloc.world_id().clone();
        let mesh = ProcMesh::allocate_boxed(alloc)
            .await
            .map_err(|err| PyException::new_err(err.to_string()))?;
        Ok(PyProcMesh::monitored(mesh, world_id))
    })
}

impl PyProcMesh {
    /// Create a new [`PyProcMesh`] with self health status monitoring.
    fn monitored(mut proc_mesh: ProcMesh, world_id: WorldId) -> Self {
        let proc_events = SharedCell::from(Mutex::new(proc_mesh.events().unwrap()));
        let (user_sender, user_receiver) = mpsc::unbounded_channel::<ProcEvent>();
        let user_monitor_registered = Arc::new(AtomicBool::new(false));
        let unhealthy_event = Arc::new(Mutex::new(Unhealthy::SoFarSoGood));
        let monitor = tokio::spawn(Self::default_proc_mesh_monitor(
            proc_events
                .borrow()
                .expect("borrowing immediately after creation"),
            world_id,
            user_sender,
            Arc::clone(&user_monitor_registered),
            Arc::clone(&unhealthy_event),
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
        unhealthy_event: Arc<Mutex<Unhealthy<ProcEvent>>>,
    ) {
        loop {
            let mut proc_events = events.lock().await;
            tokio::select! {
                event = proc_events.next() => {
                    tracing::debug!("monitor on ProcMesh {} received event: {:?}", world_id, event);
                    let mut inner_unhealthy_event = unhealthy_event.lock().await;
                    match event {
                        None => {
                            *inner_unhealthy_event = Unhealthy::StreamClosed;
                            tracing::info!("ProcMesh {}: alloc has stopped", world_id);
                            break;
                        }
                        Some(event) => match event {
                            // Graceful stops can be ignored.
                            ProcEvent::Stopped(_, ProcStopReason::Stopped) => continue,
                            event => {
                                *inner_unhealthy_event = Unhealthy::Crashed(event.clone());
                                tracing::info!("ProcMesh {}: {}", world_id, event);
                                if user_monitor_registered.load(std::sync::atomic::Ordering::SeqCst) {
                                    tracing::debug!("sending event to user monitor");
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
                    *inner_unhealthy_event = Unhealthy::StreamClosed;
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
        let (proc_mesh, children, drop_callbacks) = tracked_proc_mesh.into_inner();

        // Call all registered drop callbacks before stopping
        let mut callbacks = drop_callbacks.lock().await;
        let callbacks_to_call = callbacks.drain(..).collect::<Vec<_>>();
        drop(callbacks); // Release the lock

        for callback in callbacks_to_call {
            let future = callback();
            std::pin::Pin::from(future).await;
        }

        // Now we discard all in-flight actor meshes.  After this, the `ProcMesh` should be "unused".
        // Discarding actor meshes that have been individually stopped will result in an expected error
        // which we can safely ignore
        children.discard_or_error_all().await;

        // Finally, take ownership of the inner proc mesh, which will allowing dropping it.
        let _proc_mesh = proc_mesh.take().await?;

        // Grab the alloc back from `ProcEvents` and use that to stop the mesh.
        let proc_events_taken = proc_events.take().await?;
        let mut alloc = proc_events_taken.into_inner().into_alloc();

        alloc.stop_and_wait().await?;

        anyhow::Ok(())
    }
}

async fn ensure_mesh_healthy(unhealthy_event: &Mutex<Unhealthy<ProcEvent>>) -> Result<(), PyErr> {
    let locked = unhealthy_event.lock().await;
    match &*locked {
        Unhealthy::SoFarSoGood => Ok(()),
        Unhealthy::StreamClosed => Err(SupervisionError::new_err(
            "proc mesh is stopped with reason: alloc is stopped".to_string(),
        )),
        Unhealthy::Crashed(event) => Err(SupervisionError::new_err(format!(
            "proc mesh is stopped with reason: {}",
            event
        ))),
    }
}

#[pymethods]
impl PyProcMesh {
    #[classmethod]
    fn allocate_nonblocking<'py>(
        _cls: &Bound<'_, PyType>,
        _py: Python<'py>,
        alloc: &mut PyAlloc,
    ) -> PyResult<PyPythonTask> {
        allocate_proc_mesh(alloc)
    }

    fn spawn_nonblocking<'py>(
        &self,
        name: String,
        actor: &Bound<'py, PyType>,
    ) -> PyResult<PyPythonTask> {
        let unhealthy_event = Arc::clone(&self.unhealthy_event);
        let pickled_type: PickledPyObject = PickledPyObject::pickle(actor.as_any())?;
        let proc_mesh = self.try_inner()?;
        let keepalive = self.keepalive.clone();
        let meshimpl = async move {
            ensure_mesh_healthy(&unhealthy_event).await?;
            // TODO: thread through context, or access the actual python context;
            // for now this is basically equivalent (arguably better) to using the proc mesh client.
            let instance = global_root_client();
            let actor_mesh = proc_mesh.spawn(&instance, &name, &pickled_type).await?;
            let actor_events = actor_mesh.with_mut(|a| a.events()).await.unwrap().unwrap();
            let im = PythonActorMeshImpl::new(
                actor_mesh,
                PyMailbox {
                    inner: instance.mailbox().clone(),
                },
                keepalive,
                actor_events,
            );
            Ok(PythonActorMesh::from_impl(Box::new(im)))
        };
        PyPythonTask::new(meshimpl)
    }

    #[staticmethod]
    fn spawn_async(
        proc_mesh: &mut PyShared,
        name: String,
        actor: Py<PyType>,
    ) -> PyResult<PyObject> {
        let task = proc_mesh.task()?.take_task()?;
        let meshimpl = async move {
            let proc_mesh = task.await?;
            let (proc_mesh, pickled_type, unhealthy_event, keepalive) =
                Python::with_gil(|py| -> PyResult<_> {
                    let slf: Bound<PyProcMesh> = proc_mesh.extract(py)?;
                    let slf = slf.borrow();
                    let unhealthy_event = Arc::clone(&slf.unhealthy_event);
                    let pickled_type = PickledPyObject::pickle(actor.bind(py).as_any())?;
                    let proc_mesh = slf.try_inner()?;
                    let keepalive = slf.keepalive.clone();
                    Ok((proc_mesh, pickled_type, unhealthy_event, keepalive))
                })?;
            ensure_mesh_healthy(&unhealthy_event).await?;
            // TODO: thread through context, or access the actual python context;
            // for now this is basically equivalent (arguably better) to using the proc mesh client.
            let instance = global_root_client();

            let actor_mesh = proc_mesh.spawn(&instance, &name, &pickled_type).await?;
            let actor_events = actor_mesh.with_mut(|a| a.events()).await.unwrap().unwrap();
            Ok::<_, PyErr>(Box::new(PythonActorMeshImpl::new(
                actor_mesh,
                PyMailbox {
                    inner: instance.mailbox().clone(),
                },
                keepalive,
                actor_events,
            )))
        };
        let r = PythonActorMesh::new(
            async move {
                let meshimpl: Box<dyn ActorMeshProtocol> = meshimpl.await?;
                Ok(meshimpl)
            },
            true,
        );
        Python::with_gil(|py| r.into_py_any(py))
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
        Ok(crate::runtime::future_into_py(py, async move {
            // Create a new user monitor
            Ok(PyProcMeshMonitor { receiver })
        })?
        .into())
    }

    #[getter]
    fn client(&self) -> PyResult<PyInstance> {
        Ok(self.try_inner()?.client().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<ProcMesh {}>", *self.try_inner()?))
    }

    #[getter]
    fn region(&self) -> PyResult<PyRegion> {
        Ok(PyRegion {
            inner: self.try_inner()?.shape().into(),
        })
    }

    fn stop_nonblocking(&self) -> PyResult<PyPythonTask> {
        // Clone the necessary fields from self to avoid capturing self in the async block
        let inner = self.inner.clone();
        let proc_events = self.proc_events.clone();

        Ok(PythonTask::new(async move {
            Self::stop_mesh(inner, proc_events).await?;
            Python::with_gil(|py| Ok(py.None()))
        })?
        .into())
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
        Ok(crate::runtime::future_into_py(py, async move {
            let receiver = receiver
                .borrow()
                .map_err(|_| PyRuntimeError::new_err("`ProcEvent receiver` is shutdown"))?;
            let mut proc_event_receiver = receiver.lock().await;
            tokio::select! {
                () = receiver.preempted() => {
                    Err(PyRuntimeError::new_err("shutting down `ProcEvents` receiver"))
                },
                event = proc_event_receiver.recv() => {
                    tracing::debug!("user monitor on ProcMesh received event: {:?}", event);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;

    use anyhow::Result;
    use hyperactor::channel::ChannelTransport;
    use hyperactor_mesh::alloc::AllocSpec;
    use hyperactor_mesh::alloc::Allocator;
    use hyperactor_mesh::alloc::local::LocalAllocator;
    use hyperactor_mesh::proc_mesh::ProcMesh;
    use ndslice::extent;
    use tokio::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn test_register_onstop_callback_single() -> Result<()> {
        // Create a TrackedProcMesh
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent! { replica = 1 },
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Local,
                proc_allocation_mode: Default::default(),
            })
            .await?;

        let mut proc_mesh = ProcMesh::allocate(alloc).await?;

        // Extract events before wrapping in TrackedProcMesh
        let events = proc_mesh.events().unwrap();
        let proc_events_cell = SharedCell::from(tokio::sync::Mutex::new(events));

        let tracked_proc_mesh = TrackedProcMesh::from(proc_mesh);

        // Create a flag to track if callback was executed
        let callback_executed = Arc::new(AtomicBool::new(false));
        let callback_executed_clone = callback_executed.clone();

        // Register a callback
        tracked_proc_mesh
            .register_onstop_callback(move || {
                let flag = callback_executed_clone.clone();
                async move {
                    flag.store(true, Ordering::SeqCst);
                }
            })
            .await?;

        // Create a SharedCell<TrackedProcMesh> for stop_mesh
        let tracked_proc_mesh_cell = SharedCell::from(tracked_proc_mesh);

        // Call stop_mesh (this should trigger the callback)
        PyProcMesh::stop_mesh(tracked_proc_mesh_cell, proc_events_cell).await?;

        // Verify the callback was executed
        assert!(
            callback_executed.load(Ordering::SeqCst),
            "Callback should have been executed"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_register_onstop_callback_multiple() -> Result<()> {
        // Create a TrackedProcMesh
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent! { replica = 1 },
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Local,
                proc_allocation_mode: Default::default(),
            })
            .await?;

        let mut proc_mesh = ProcMesh::allocate(alloc).await?;

        // Extract events before wrapping in TrackedProcMesh
        let events = proc_mesh.events().unwrap();
        let proc_events_cell = SharedCell::from(tokio::sync::Mutex::new(events));

        let tracked_proc_mesh = TrackedProcMesh::from(proc_mesh);

        // Create counters to track callback executions
        let callback_count = Arc::new(AtomicU32::new(0));
        let execution_order = Arc::new(Mutex::new(Vec::<u32>::new()));

        // Register multiple callbacks
        for i in 1..=3 {
            let count = callback_count.clone();
            let order = execution_order.clone();
            tracked_proc_mesh
                .register_onstop_callback(move || {
                    let count_clone = count.clone();
                    let order_clone = order.clone();
                    async move {
                        count_clone.fetch_add(1, Ordering::SeqCst);
                        let mut order_vec = order_clone.lock().await;
                        order_vec.push(i);
                    }
                })
                .await?;
        }

        // Create a SharedCell<TrackedProcMesh> for stop_mesh
        let tracked_proc_mesh_cell = SharedCell::from(tracked_proc_mesh);

        // Call stop_mesh (this should trigger all callbacks)
        PyProcMesh::stop_mesh(tracked_proc_mesh_cell, proc_events_cell).await?;

        // Verify all callbacks were executed
        assert_eq!(
            callback_count.load(Ordering::SeqCst),
            3,
            "All 3 callbacks should have been executed"
        );

        // Verify execution order (callbacks should be executed in registration order)
        let order_vec = execution_order.lock().await;
        assert_eq!(
            *order_vec,
            vec![1, 2, 3],
            "Callbacks should be executed in registration order"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_register_onstop_callback_error_handling() -> Result<()> {
        // Create a TrackedProcMesh
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent! { replica = 1 },
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Local,
                proc_allocation_mode: Default::default(),
            })
            .await?;

        let mut proc_mesh = ProcMesh::allocate(alloc).await?;

        // Extract events before wrapping in TrackedProcMesh
        let events = proc_mesh.events().unwrap();
        let proc_events_cell = SharedCell::from(tokio::sync::Mutex::new(events));

        let tracked_proc_mesh = TrackedProcMesh::from(proc_mesh);

        // Create flags to track callback executions
        let callback1_executed = Arc::new(AtomicBool::new(false));
        let callback2_executed = Arc::new(AtomicBool::new(false));

        let callback1_executed_clone = callback1_executed.clone();
        let callback2_executed_clone = callback2_executed.clone();

        // Register a callback that panics
        tracked_proc_mesh
            .register_onstop_callback(move || {
                let flag = callback1_executed_clone.clone();
                async move {
                    flag.store(true, Ordering::SeqCst);
                    // This callback completes successfully
                }
            })
            .await?;

        // Register another callback that should still execute even if the first one had issues
        tracked_proc_mesh
            .register_onstop_callback(move || {
                let flag = callback2_executed_clone.clone();
                async move {
                    flag.store(true, Ordering::SeqCst);
                }
            })
            .await?;

        // Create a SharedCell<TrackedProcMesh> for stop_mesh
        let tracked_proc_mesh_cell = SharedCell::from(tracked_proc_mesh);

        // Call stop_mesh (this should trigger both callbacks)
        PyProcMesh::stop_mesh(tracked_proc_mesh_cell, proc_events_cell).await?;

        // Verify both callbacks were executed
        assert!(
            callback1_executed.load(Ordering::SeqCst),
            "First callback should have been executed"
        );
        assert!(
            callback2_executed.load(Ordering::SeqCst),
            "Second callback should have been executed"
        );

        Ok(())
    }
}
