/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor_mesh::supervision::MeshFailure;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

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
    Ok(())
}

// Shared between mesh types.
#[derive(Debug, Clone)]
pub(crate) enum Unhealthy<Event> {
    SoFarSoGood,    // Still healthy
    StreamClosed,   // Event stream closed
    Crashed(Event), // Bad health event received
}

impl<Event> Unhealthy<Event> {
    #[allow(dead_code)] // No uses yet.
    pub(crate) fn is_healthy(&self) -> bool {
        matches!(self, Unhealthy::SoFarSoGood)
    }

    #[allow(dead_code)] // No uses yet.
    pub(crate) fn is_crashed(&self) -> bool {
        matches!(self, Unhealthy::Crashed(_))
    }
}
