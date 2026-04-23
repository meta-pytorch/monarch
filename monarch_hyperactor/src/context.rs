/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::Instance;
use hyperactor::context;
use hyperactor_mesh::comm::multicast::CastInfo;
use ndslice::Extent;
use ndslice::Point;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::actor::PythonActor;
use crate::actor::root_client_actor;
use crate::mailbox::PyMailbox;
use crate::proc::PyActorId;
use crate::runtime;
use crate::shape::PyPoint;

#[pyclass(name = "Instance", module = "monarch._src.actor.actor_mesh")]
pub struct PyInstance {
    inner: Instance<PythonActor>,
    #[pyo3(get, set)]
    proc_mesh: Option<Py<PyAny>>,
    #[pyo3(get, set, name = "_controller_controller")]
    controller_controller: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    pub(crate) rank: PyPoint,
    #[pyo3(get, set, name = "_children")]
    children: Option<Py<PyAny>>,

    #[pyo3(get, set, name = "name")]
    name: String,
    #[pyo3(get, set, name = "class_name")]
    class_name: Option<String>,
    #[pyo3(get, set, name = "creator")]
    creator: Option<Py<PyAny>>,

    #[pyo3(get, set, name = "_mock_tensor_engine_factory")]
    mock_tensor_engine_factory: Option<Py<PyAny>>,
}

impl Clone for PyInstance {
    fn clone(&self) -> Self {
        PyInstance {
            inner: self.inner.clone_for_py(),
            proc_mesh: self.proc_mesh.clone(),
            controller_controller: self.controller_controller.clone(),
            rank: self.rank.clone(),
            children: self.children.clone(),
            name: self.name.clone(),
            class_name: self.class_name.clone(),
            creator: self.creator.clone(),
            mock_tensor_engine_factory: self.mock_tensor_engine_factory.clone(),
        }
    }
}

impl std::ops::Deref for PyInstance {
    type Target = Instance<PythonActor>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[pymethods]
impl PyInstance {
    #[getter]
    pub(crate) fn _mailbox(&self) -> PyMailbox {
        PyMailbox {
            inner: self.inner.mailbox_for_py().clone(),
        }
    }

    #[getter]
    pub fn actor_id(&self) -> PyActorId {
        self.inner.self_id().clone().into()
    }

    #[pyo3(signature = (reason = None))]
    fn abort(&self, reason: Option<&str>) -> PyResult<()> {
        let reason = reason.unwrap_or("(no reason provided)");
        Ok(self.inner.abort(reason).map_err(anyhow::Error::from)?)
    }

    #[pyo3(signature = (reason = None))]
    fn stop(&self, reason: Option<&str>) -> PyResult<()> {
        tracing::info!(actor_id = %self.inner.self_id(), "stopping PyInstance");
        let reason = reason.unwrap_or("(no reason provided)");
        self.inner
            .stop(reason)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Stop the actor and return a future that resolves when it reaches
    /// a terminal status (stopped or failed). This ensures all pending
    /// messages are drained and connections are flushed before returning.
    #[pyo3(signature = (reason = None))]
    fn stop_and_wait(&self, reason: Option<&str>) -> PyResult<crate::pytokio::PyPythonTask> {
        let reason = reason.unwrap_or("shutdown").to_string();
        let actor_id = self.inner.self_id().clone();
        let proc = self.inner.proc().clone();
        crate::pytokio::PyPythonTask::new(async move {
            let status_rx = proc.stop_actor(&actor_id, reason);
            if let Some(mut rx) = status_rx {
                let _ = rx.wait_for(|s| s.is_terminal()).await;
            }
            if let Err(e) = proc.flush().await {
                tracing::warn!(%actor_id, "stop_and_wait: flush failed: {}", e);
            }
            Ok(())
        })
    }

    /// Mark this actor as system/infrastructure.
    ///
    /// **PY-SYS-2:** Python actors use the `_is_system_actor = True`
    /// class attribute so that this is called during actor init,
    /// before ProcAgent publishes its first introspection snapshot.
    fn set_system(&self) {
        self.inner.set_system();
    }
}

impl PyInstance {
    pub fn into_instance(self) -> Instance<PythonActor> {
        self.inner
    }
}

impl<I: context::Actor<A = PythonActor>> From<I> for PyInstance {
    fn from(ins: I) -> Self {
        PyInstance {
            inner: ins.instance().clone_for_py(),
            proc_mesh: None,
            controller_controller: None,
            rank: PyPoint::new(0, Extent::unity().into()),
            children: None,
            name: "root".to_string(),
            class_name: None,
            creator: None,
            mock_tensor_engine_factory: None,
        }
    }
}

#[pyclass(name = "Context", module = "monarch._src.actor.actor_mesh")]
pub struct PyContext {
    instance: Py<PyInstance>,
    rank: Point,
    /// Cloneable handle to a span carrying the actor's recording key.
    /// When entered, events emitted under this span are captured by
    /// the per-actor flight recorder. `None` for bootstrap/client
    /// contexts that are not actor handler execution paths.
    recording_span: Option<tracing::Span>,
}

#[pymethods]
impl PyContext {
    #[getter]
    fn actor_instance(&self) -> &Py<PyInstance> {
        &self.instance
    }

    #[getter]
    fn message_rank(&self) -> PyPoint {
        self.rank.clone().into()
    }

    #[staticmethod]
    fn _root_client_context(py: Python<'_>) -> PyResult<PyContext> {
        let _guard = runtime::get_tokio_runtime().enter();
        let instance: PyInstance = root_client_actor(py).into();
        Ok(PyContext {
            instance: instance.into_pyobject(py)?.into(),
            rank: Extent::unity().point_of_rank(0).unwrap(),
            recording_span: None,
        })
    }

    /// Create a context from an existing instance.
    /// This is used when the root client was bootstrapped via bootstrap_host()
    /// instead of the default bootstrap_client().
    #[staticmethod]
    fn _from_instance(py: Python<'_>, instance: PyInstance) -> PyResult<PyContext> {
        Ok(PyContext {
            instance: instance.into_pyobject(py)?.into(),
            rank: Extent::unity().point_of_rank(0).unwrap(),
            recording_span: None,
        })
    }
}

impl PyContext {
    pub(crate) fn new<T: hyperactor::actor::Actor>(
        cx: &hyperactor::Context<T>,
        instance: Py<PyInstance>,
    ) -> PyContext {
        PyContext {
            instance,
            rank: cx.cast_point(),
            recording_span: Some(cx.recording_span()),
        }
    }

    /// The actor's recording span, if this context is an actor handler
    /// execution path. Used by `forward_to_tracing` to enter the
    /// recording scope on the asyncio thread so that log events are
    /// captured in the flight recorder.
    pub(crate) fn recording_span(&self) -> Option<&tracing::Span> {
        self.recording_span.as_ref()
    }

    /// Test-only: build a PyContext with a chosen recording span.
    /// Uses the root client actor for the instance field (the test
    /// only exercises the recording_span extraction path).
    #[doc(hidden)]
    pub fn for_test(py: Python<'_>, recording_span: Option<tracing::Span>) -> PyResult<PyContext> {
        let mut ctx = Self::_root_client_context(py)?;
        ctx.recording_span = recording_span;
        Ok(ctx)
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyInstance>()?;
    hyperactor_mod.add_class::<PyContext>()?;
    Ok(())
}
