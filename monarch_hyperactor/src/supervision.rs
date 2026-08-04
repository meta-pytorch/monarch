/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Python-facing supervision boundary. `MeshFailure.mesh_name`
//! exposes optional display metadata; callers that need identity
//! should use `MeshFailure.mesh_id` and compare it with the mesh's
//! public `id`.

use async_trait::async_trait;
use hyperactor::Instance;
use hyperactor_mesh::supervision::MeshFailure;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::actor::PythonActor;
use crate::actor_mesh::PyActorSupervisionEvent;
use crate::shape::PyPoint;

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
        let message = event
            .failure_report()
            .unwrap_or_else(|| format!("{}", event));
        Self::new_err(message)
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
            "MeshFailure(mesh_name={}, crashed_ranks={:?}, event={})",
            self.inner
                .actor_mesh_name
                .clone()
                .unwrap_or("<none>".into()),
            self.inner.crashed_ranks,
            self.inner.event
        )
    }
}

#[pymethods]
impl PyMeshFailure {
    // TODO: store and return the mesh object.
    #[getter]
    fn mesh(&self) {}

    #[getter]
    fn mesh_name(&self) -> Option<String> {
        self.inner.actor_mesh_name.clone()
    }

    #[getter]
    fn mesh_id(&self) -> Option<String> {
        self.inner.mesh_id.clone()
    }

    #[getter]
    fn coordinate(&self) -> Option<PyPoint> {
        self.inner.coordinate.clone().map(PyPoint::from)
    }

    #[getter]
    fn event(&self) -> PyActorSupervisionEvent {
        PyActorSupervisionEvent::from(self.inner.event.clone())
    }

    fn __repr__(&self) -> String {
        format!("{}", self)
    }

    fn report(&self) -> String {
        self.inner
            .event
            .failure_report()
            .unwrap_or_else(|| format!("{}", self.inner.event))
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Get the Python interpreter instance from the module
    let py = module.py();
    // Add the exception to the module using its type object
    module.add("SupervisionError", py.get_type::<SupervisionError>())?;
    module.add("MeshFailure", py.get_type::<PyMeshFailure>())?;
    Ok(())
}
