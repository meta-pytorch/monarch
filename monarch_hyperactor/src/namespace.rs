/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Python bindings for read-only namespace operations.
//!
//! This module exposes only the read portion of the Namespace API to Python,
//! allowing lookups but not registration or unregistration.

use std::sync::Arc;

use hyperactor_mesh::v1::ActorMeshRef;
use hyperactor_mesh::v1::HostMeshRef;
use hyperactor_mesh::v1::InMemoryNamespace;
use hyperactor_mesh::v1::MeshKind;
use hyperactor_mesh::v1::Namespace;
use hyperactor_mesh::v1::NamespaceError;
use hyperactor_mesh::v1::ProcMeshRef;
use hyperactor_mesh::v1::Registrable;
use pyo3::exceptions::PyKeyError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::actor::PythonActor;
use crate::actor_mesh::PythonActorMeshImpl;
use crate::host_mesh::PyHostMesh;
use crate::proc_mesh::PyProcMesh;
use crate::pytokio::PyPythonTask;

/// Convert NamespaceError to PyErr.
fn namespace_error_to_pyerr(e: NamespaceError) -> PyErr {
    match e {
        NamespaceError::NotFound(key) => PyKeyError::new_err(key),
        NamespaceError::DeserializationError(msg) => PyValueError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    }
}

/// The kind of mesh (host, proc, or actor).
#[pyclass(
    name = "MeshKind",
    module = "monarch._rust_bindings.monarch_hyperactor.namespace",
    eq
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PyMeshKind {
    Host,
    Proc,
    Actor,
}

#[pymethods]
impl PyMeshKind {
    /// MeshKind.Host
    #[classattr]
    const HOST: Self = PyMeshKind::Host;

    /// MeshKind.Proc
    #[classattr]
    const PROC: Self = PyMeshKind::Proc;

    /// MeshKind.Actor
    #[classattr]
    const ACTOR: Self = PyMeshKind::Actor;

    fn __repr__(&self) -> &'static str {
        match self {
            PyMeshKind::Host => "MeshKind.Host",
            PyMeshKind::Proc => "MeshKind.Proc",
            PyMeshKind::Actor => "MeshKind.Actor",
        }
    }

    fn __str__(&self) -> &'static str {
        match self {
            PyMeshKind::Host => "host",
            PyMeshKind::Proc => "proc",
            PyMeshKind::Actor => "actor",
        }
    }
}

impl From<PyMeshKind> for MeshKind {
    fn from(kind: PyMeshKind) -> Self {
        match kind {
            PyMeshKind::Host => MeshKind::Host,
            PyMeshKind::Proc => MeshKind::Proc,
            PyMeshKind::Actor => MeshKind::Actor,
        }
    }
}

impl From<MeshKind> for PyMeshKind {
    fn from(kind: MeshKind) -> Self {
        match kind {
            MeshKind::Host => PyMeshKind::Host,
            MeshKind::Proc => PyMeshKind::Proc,
            MeshKind::Actor => PyMeshKind::Actor,
        }
    }
}

/// A read-only namespace for looking up meshes.
///
/// This class only exposes read operations (get, contains) and does not
/// allow registration or unregistration of meshes.
#[pyclass(
    name = "Namespace",
    module = "monarch._rust_bindings.monarch_hyperactor.namespace"
)]
pub struct PyNamespace {
    inner: Arc<InMemoryNamespace>,
}

impl PyNamespace {
    /// Create a new PyNamespace from an InMemoryNamespace.
    pub fn new(namespace: Arc<InMemoryNamespace>) -> Self {
        Self { inner: namespace }
    }
}

#[pymethods]
impl PyNamespace {
    /// Get the namespace name.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Check if a mesh exists in the namespace.
    ///
    /// Args:
    ///     kind: The mesh kind (MeshKind.Host, MeshKind.Proc, or MeshKind.Actor)
    ///     name: The mesh name
    ///
    /// Returns:
    ///     True if the mesh exists, False otherwise
    fn contains(&self, kind: PyMeshKind, name: String) -> PyResult<PyPythonTask> {
        let ns = self.inner.clone();
        match kind {
            PyMeshKind::Host => PyPythonTask::new(async move {
                ns.contains::<HostMeshRef>(&name)
                    .await
                    .map_err(namespace_error_to_pyerr)
            }),
            PyMeshKind::Proc => PyPythonTask::new(async move {
                ns.contains::<ProcMeshRef>(&name)
                    .await
                    .map_err(namespace_error_to_pyerr)
            }),
            PyMeshKind::Actor => PyPythonTask::new(async move {
                ns.contains::<ActorMeshRef<PythonActor>>(&name)
                    .await
                    .map_err(namespace_error_to_pyerr)
            }),
        }
    }

    /// Get a mesh from the namespace.
    ///
    /// Args:
    ///     kind: The mesh kind (MeshKind.Host, MeshKind.Proc, or MeshKind.Actor)
    ///     name: The mesh name
    ///
    /// Returns:
    ///     HostMesh, ProcMesh, or ActorMesh depending on kind
    ///
    /// Raises:
    ///     KeyError: If the mesh is not found
    fn get(&self, kind: PyMeshKind, name: String) -> PyResult<PyPythonTask> {
        let ns = self.inner.clone();

        match kind {
            PyMeshKind::Host => PyPythonTask::new(async move {
                let mesh: HostMeshRef = ns.get(&name).await.map_err(namespace_error_to_pyerr)?;
                Ok(PyHostMesh::new_ref(mesh))
            }),
            PyMeshKind::Proc => PyPythonTask::new(async move {
                let mesh: ProcMeshRef = ns.get(&name).await.map_err(namespace_error_to_pyerr)?;
                Ok(PyProcMesh::new_ref(mesh))
            }),
            PyMeshKind::Actor => PyPythonTask::new(async move {
                let mesh: ActorMeshRef<PythonActor> =
                    ns.get(&name).await.map_err(namespace_error_to_pyerr)?;
                Ok(PythonActorMeshImpl::new_ref(mesh))
            }),
        }
    }

    fn __repr__(&self) -> String {
        format!("Namespace(name='{}')", self.inner.name())
    }
}

/// Create an in-memory namespace for testing.
///
/// Args:
///     name: The namespace name (e.g., "my.namespace")
///
/// Returns:
///     A Namespace instance backed by in-memory storage
#[pyfunction]
fn create_in_memory_namespace(name: String) -> PyNamespace {
    PyNamespace::new(Arc::new(InMemoryNamespace::new(name)))
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMeshKind>()?;
    module.add_class::<PyNamespace>()?;
    module.add_function(wrap_pyfunction!(create_in_memory_namespace, module)?)?;
    Ok(())
}
