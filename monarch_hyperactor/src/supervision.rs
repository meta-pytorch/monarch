/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Python-facing supervision boundary.
//!
//! See FA-* (failure-attribution invariants) in
//! `hyperactor/src/supervision.rs` for the substrate contract on
//! supervision-path rendering. This module adds additive structured
//! attribution fields to `SupervisionError` (`mesh_name`,
//! `actor_class`, `actor_display_name`, `rank`) that project
//! directly from `failure.event.attribution`. Consumers read those
//! fields programmatically instead of parsing `message` or the
//! rendered prose.
//!
//! Invariant: `SupervisionError::set_endpoint_on_err(...)` must
//! preserve the structured attribution fields when grafting an
//! endpoint onto an existing supervision error. Endpoint grafting
//! is a presentation-layer concern; it must not drop the
//! structured carriers.

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
    /// Mesh name of the destination actor, when known at send or
    /// synthesis time. Additive structured field populated from
    /// `failure.event.attribution.mesh_name`; callers that want a
    /// programmatic handle read this instead of parsing `message`.
    #[pyo3(get)]
    pub mesh_name: Option<String>,
    /// Python actor class token, when known. Additive structured field
    /// populated from `failure.event.attribution.actor_class`.
    #[pyo3(get)]
    pub actor_class: Option<String>,
    /// Free-form rendered display name for the destination actor,
    /// when known. Additive structured field populated from
    /// `failure.event.attribution.actor_display_name`.
    #[pyo3(get)]
    pub actor_display_name: Option<String>,
    /// Per-rank rank when the failure is per-rank. Additive structured
    /// field populated from `failure.event.attribution.rank`.
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
    /// Build a `PyErr` wrapping a `SupervisionError` populated from
    /// a `MeshFailure`. Structured attribution fields are copied
    /// from the root-cause event's `attribution` (via
    /// `ActorSupervisionEvent::caused_by`, matching SV-1) so
    /// consumers can read `e.mesh_name` / `e.actor_class` / etc.
    /// without parsing `message` or the rendered prose.
    ///
    /// The chain walk is necessary because intermediate supervisor
    /// synthesis sites wrap the root-cause event in a new
    /// `ActorSupervisionEvent` whose own `attribution` is `None`;
    /// reading `attribution` only on the top-level event would
    /// miss the data the producer attached at the originating
    /// site. Whether the structured fields are populated after the
    /// walk depends on whether the originating site had the
    /// attribution in scope when it constructed the event; callers
    /// should treat these fields as best-effort and tolerate
    /// `None`.
    pub(crate) fn new_err_from(failure: MeshFailure) -> PyErr {
        let event = failure.event;
        let message = event
            .failure_report()
            .unwrap_or_else(|| format!("{}", event));
        // Walk to the root-cause event (SV-1) before reading
        // attribution. Intermediate wrappers typically carry
        // `attribution: None`; the data the producer attached
        // lives on the leaf.
        let source = event.caused_by();
        let (mesh_name, actor_class, actor_display_name, rank) = match &source.attribution {
            Some(a) => (
                a.mesh_name.clone(),
                a.actor_class.clone(),
                a.actor_display_name.clone(),
                a.rank,
            ),
            None => (None, None, None, None),
        };
        PyErr::new::<Self, _>((
            message,
            None::<String>,
            mesh_name,
            actor_class,
            actor_display_name,
            rank,
        ))
    }

    /// Set the endpoint on a PyErr containing a SupervisionError.
    ///
    /// If the error is a SupervisionError, sets its endpoint field
    /// while preserving the structured attribution fields, and
    /// returns a new error with the endpoint prefix. If not a
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

#[cfg(test)]
mod tests {
    use pyo3::Python;

    use super::*;

    /// Endpoint grafting must not drop structured attribution fields.
    /// `SupervisionError::set_endpoint_on_err` builds a new error
    /// with an endpoint annotation; this test locks in that
    /// `mesh_name`, `actor_class`, `actor_display_name`, and `rank`
    /// survive the graft.
    ///
    /// This is a preservation test: it validates that input values
    /// round-trip through `set_endpoint_on_err` unchanged. It does
    /// not validate any string format, so the test uses neutral
    /// tokens rather than production-shape strings.
    #[test]
    fn test_set_endpoint_on_err_preserves_structured_fields() {
        Python::initialize();
        Python::attach(|py| {
            // Construct a SupervisionError with all four structured
            // fields populated by invoking the pyclass constructor.
            // Tokens are neutral placeholders; this test is about
            // preservation, not format.
            let err_py = PyErr::new::<SupervisionError, _>((
                "MESSAGE".to_string(),
                None::<String>,
                Some("MESH_NAME".to_string()),
                Some("ACTOR_CLASS".to_string()),
                Some("DISPLAY_NAME".to_string()),
                Some(7usize),
            ));

            let grafted = SupervisionError::set_endpoint_on_err(py, err_py, "ENDPOINT".to_string());

            let extracted: SupervisionError = grafted
                .value(py)
                .extract()
                .expect("grafted PyErr is a SupervisionError");

            // Endpoint grafted as expected.
            assert_eq!(extracted.endpoint.as_deref(), Some("ENDPOINT"));
            // Structured fields preserved verbatim.
            assert_eq!(extracted.mesh_name.as_deref(), Some("MESH_NAME"));
            assert_eq!(extracted.actor_class.as_deref(), Some("ACTOR_CLASS"));
            assert_eq!(
                extracted.actor_display_name.as_deref(),
                Some("DISPLAY_NAME"),
            );
            assert_eq!(extracted.rank, Some(7usize));
            // Message passes through unchanged.
            assert_eq!(extracted.message, "MESSAGE");
        });
    }
}
