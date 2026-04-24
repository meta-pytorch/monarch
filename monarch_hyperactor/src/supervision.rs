/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Python-facing supervision boundary. `SupervisionError` is the
//! Python-visible exception raised when supervision propagates a
//! failure across an actor mesh. It extends `RuntimeError` and
//! exposes the structured `Attribution` carrier from the source
//! `ActorSupervisionEvent` as read-only properties (`mesh_name`,
//! `actor_class`, `actor_display_name`, `rank`) for callers that
//! want to inspect failure context without parsing rendered text.

use async_trait::async_trait;
use hyperactor::Instance;
use hyperactor_mesh::supervision::MeshFailure;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::actor::PythonActor;

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
    /// Structured attribution carried on the source
    /// `ActorSupervisionEvent` (see
    /// `hyperactor::supervision::Attribution`). Populated when the
    /// error was constructed from a `MeshFailure`; `None` on
    /// constructors that do not carry event context (raw
    /// `new_err`, direct Python `SupervisionError("msg")`).
    #[pyo3(get)]
    pub mesh_name: Option<String>,
    #[pyo3(get)]
    pub actor_class: Option<String>,
    #[pyo3(get)]
    pub actor_display_name: Option<String>,
    #[pyo3(get)]
    pub rank: Option<usize>,
}

#[pymethods]
impl SupervisionError {
    #[new]
    #[pyo3(signature = (
        message,
        endpoint=None,
        mesh_name=None,
        actor_class=None,
        actor_display_name=None,
        rank=None,
    ))]
    fn new(
        message: String,
        endpoint: Option<String>,
        mesh_name: Option<String>,
        actor_class: Option<String>,
        actor_display_name: Option<String>,
        rank: Option<usize>,
    ) -> Self {
        SupervisionError {
            endpoint,
            message,
            mesh_name,
            actor_class,
            actor_display_name,
            rank,
        }
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
        let attribution = event.attribution.unwrap_or_default();
        PyErr::new::<Self, _>((
            message,
            None::<String>,
            attribution.mesh_name,
            attribution.actor_class,
            attribution.actor_display_name,
            attribution.rank,
        ))
    }
    /// Set the endpoint on a PyErr containing a SupervisionError.
    ///
    /// If the error is a SupervisionError, returns a new error with
    /// the endpoint attached while preserving the structured
    /// attribution fields (`mesh_name`, `actor_class`,
    /// `actor_display_name`, `rank`) of the original. If not a
    /// SupervisionError, returns the original error.
    pub fn set_endpoint_on_err(py: Python<'_>, err: PyErr, endpoint: String) -> PyErr {
        if let Ok(supervision_err) = err.value(py).extract::<SupervisionError>() {
            PyErr::new::<Self, _>((
                supervision_err.message,
                Some(endpoint),
                supervision_err.mesh_name,
                supervision_err.actor_class,
                supervision_err.actor_display_name,
                supervision_err.rank,
            ))
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
    fn mesh_name(&self) -> String {
        self.inner
            .actor_mesh_name
            .clone()
            .unwrap_or("<none>".into())
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
