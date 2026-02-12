/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use async_trait::async_trait;
use hyperactor::Instance;
use hyperactor_mesh::supervision::MeshFailure;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::actor::PythonActor;
use crate::context::PyInstance;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;

/// Trait for types that can provide supervision events.
///
/// This trait abstracts the supervision functionality, allowing endpoint
/// operations to work with any type that can monitor actor health without
/// depending on the full ActorMesh interface.
#[async_trait]
pub trait Supervisable: Send + Sync {
    /// Wait for the next supervision event indicating an actor failure.
    ///
    /// Returns `Some(PyErr)` if a supervision failure is detected,
    /// or `None` if supervision is not available or the mesh is healthy.
    async fn supervision_event(&self, instance: &Instance<PythonActor>) -> Option<PyErr>;
}

/// Python-exposed wrapper for supervision functionality.
///
/// This provides a concrete pyclass that can be passed to endpoint collectors
/// while abstracting over different supervision implementations.
#[pyclass(
    name = "SupervisionMonitor",
    module = "monarch._rust_bindings.monarch_hyperactor.supervision"
)]
#[derive(Clone)]
pub struct PySupervisionMonitor {
    inner: Arc<dyn Supervisable>,
}

impl PySupervisionMonitor {
    pub fn new<S: Supervisable + 'static>(supervisable: S) -> Self {
        Self {
            inner: Arc::new(supervisable),
        }
    }

    pub fn from_arc(inner: Arc<dyn Supervisable>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PySupervisionMonitor {
    fn __repr__(&self) -> String {
        "PySupervisionMonitor(...)".to_string()
    }

    /// Returns a PythonTask that resolves when a supervision event occurs.
    ///
    /// This is used by Python code that needs to store and await the supervision
    /// event later, rather than directly awaiting it in Rust.
    fn supervision_event_task(&self, instance: &PyInstance) -> PyResult<Option<PyShared>> {
        let inner = self.inner.clone();
        let instance = instance.clone().into_instance();
        PyPythonTask::new(async move {
            match inner.supervision_event(&instance).await {
                Some(err) => Err(err),
                None => Ok(()),
            }
        })
        .map(|mut x| x.spawn_abortable().map(Some))?
    }
}

#[pyclass(
    name = "SupervisionError",
    module = "monarch._rust_bindings.monarch_hyperactor.supervision",
    extends = PyRuntimeError
)]
#[derive(Clone, Debug)]
pub struct SupervisionError {
    #[pyo3(set)]
    pub endpoint: Option<String>,
    pub message: String,
}

#[pymethods]
impl SupervisionError {
    #[new]
    #[pyo3(signature = (message, endpoint=None))]
    fn new(message: String, endpoint: Option<String>) -> Self {
        SupervisionError { endpoint, message }
    }

    #[staticmethod]
    pub fn new_err(message: String) -> PyErr {
        PyErr::new::<Self, _>(message)
    }

    #[staticmethod]
    pub fn new_err_from_endpoint(message: String, endpoint: String) -> PyErr {
        PyErr::new::<Self, _>((message, Some(endpoint)))
    }

    fn __str__(&self) -> String {
        if let Some(ep) = &self.endpoint {
            format!("Endpoint call {} failed, {}", ep, self.message)
        } else {
            self.message.clone()
        }
    }

    fn __repr__(&self) -> String {
        if let Some(ep) = &self.endpoint {
            format!("SupervisionError(endpoint='{}', '{}')", ep, self.message)
        } else {
            format!("SupervisionError('{}')", self.message)
        }
    }
}

impl SupervisionError {
    // Not From<MeshFailure> because the return type needs to be PyErr.
    #[allow(dead_code)]
    pub(crate) fn new_err_from(failure: MeshFailure) -> PyErr {
        let event = failure.event;
        Self::new_err(format!(
            "Actor {} exited because of the following reason: {}",
            event.actor_id, event,
        ))
    }
    /// Set the endpoint on a PyErr containing a SupervisionError.
    ///
    /// If the error is a SupervisionError, sets its endpoint field and returns a new
    /// error with the endpoint prefix. If not a SupervisionError, returns the original error.
    pub fn set_endpoint_on_err(py: Python<'_>, err: PyErr, endpoint: String) -> PyErr {
        if let Ok(supervision_err) = err.value(py).extract::<SupervisionError>() {
            Self::new_err_from_endpoint(supervision_err.message, endpoint)
        } else {
            err
        }
    }
}

// TODO: find out how to extend a Python exception and have internal data.
#[derive(Clone, Debug)]
#[pyclass(
    name = "MeshFailure",
    module = "monarch._rust_bindings.monarch_hyperactor.supervision"
)]
pub struct PyMeshFailure {
    pub inner: MeshFailure,
}

impl PyMeshFailure {
    pub fn new(failure: MeshFailure) -> Self {
        Self { inner: failure }
    }
}

impl From<MeshFailure> for PyMeshFailure {
    fn from(failure: MeshFailure) -> Self {
        Self { inner: failure }
    }
}

impl std::fmt::Display for PyMeshFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MeshFailure(mesh_name={}, rank={}, event={})",
            self.inner
                .actor_mesh_name
                .clone()
                .unwrap_or("<none>".into()),
            self.inner.rank.map_or("<none>".into(), |r| r.to_string()),
            self.inner.event
        )
    }
}

#[pymethods]
impl PyMeshFailure {
    // TODO: store and return the mesh object.
    #[getter]
    fn mesh(&self) {}

    fn __repr__(&self) -> String {
        format!("{}", self)
    }

    fn report(&self) -> String {
        format!("{}", self.inner.event)
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Get the Python interpreter instance from the module
    let py = module.py();
    // Add the exception to the module using its type object
    module.add("SupervisionError", py.get_type::<SupervisionError>())?;
    module.add("MeshFailure", py.get_type::<PyMeshFailure>())?;
    module.add_class::<PySupervisionMonitor>()?;
    Ok(())
}
