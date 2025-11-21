/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::Bind;
use hyperactor::Named;
use hyperactor::Unbind;
use hyperactor::supervision::ActorSupervisionEvent;
use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::Deserialize;
use serde::Serialize;

create_exception!(
    monarch._rust_bindings.monarch_hyperactor.supervision,
    SupervisionError,
    PyRuntimeError
);

#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Bind, Unbind)]
pub struct SupervisionFailureMessage {
    pub actor_mesh_name: String,
    pub rank: usize,
    pub event: ActorSupervisionEvent,
}

// TODO: find out how to extend a Python exception and have internal data.
#[derive(Clone, Debug)]
#[pyclass(
    name = "MeshFailure",
    module = "monarch._rust_bindings.monarch_hyperactor.supervision"
)]
pub struct MeshFailure {
    pub mesh_name: String,
    pub rank: usize,
    pub event: ActorSupervisionEvent,
}

impl MeshFailure {
    pub fn new(mesh_name: &impl ToString, rank: usize, event: ActorSupervisionEvent) -> Self {
        Self {
            mesh_name: mesh_name.to_string(),
            rank,
            event,
        }
    }
}

impl From<SupervisionFailureMessage> for MeshFailure {
    fn from(message: SupervisionFailureMessage) -> Self {
        Self {
            mesh_name: message.actor_mesh_name,
            rank: message.rank,
            event: message.event,
        }
    }
}

impl std::fmt::Display for MeshFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MeshFailure(mesh_name={}, rank={}, event={})",
            self.mesh_name, self.rank, self.event
        )
    }
}

#[pymethods]
impl MeshFailure {
    // TODO: store and return the mesh object.
    #[getter]
    fn mesh(&self) {}

    fn __repr__(&self) -> String {
        format!("{}", self)
    }

    fn report(&self) -> String {
        format!("{}", self.event)
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Get the Python interpreter instance from the module
    let py = module.py();
    // Add the exception to the module using its type object
    module.add("SupervisionError", py.get_type::<SupervisionError>())?;
    module.add("MeshFailure", py.get_type::<MeshFailure>())?;
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
