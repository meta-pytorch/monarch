/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use futures::TryFutureExt;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::shared_cell::SharedCell;
use monarch_hyperactor::code_sync::WorkspaceLocation;
use monarch_hyperactor::code_sync::manager::CodeSyncManager;
use monarch_hyperactor::code_sync::manager::CodeSyncManagerParams;
use monarch_hyperactor::code_sync::manager::CodeSyncMethod;
use monarch_hyperactor::code_sync::manager::WorkspaceConfig;
use monarch_hyperactor::code_sync::manager::WorkspaceShape;
use monarch_hyperactor::code_sync::manager::code_sync_mesh;
use monarch_hyperactor::proc_mesh::PyProcMesh;
use monarch_hyperactor::runtime::signal_safe_block_on;
use pyo3::Bound;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyModule;
use serde::Deserialize;
use serde::Serialize;

#[pyclass(
    name = "WorkspaceLocation",
    module = "monarch._rust_bindings.monarch_extension.code_sync",
    eq,
    frozen
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum PyWorkspaceLocation {
    Constant(PathBuf),
    FromEnvVar { env: String, relpath: PathBuf },
}

impl From<PyWorkspaceLocation> for WorkspaceLocation {
    fn from(workspace: PyWorkspaceLocation) -> WorkspaceLocation {
        match workspace {
            PyWorkspaceLocation::Constant(v) => WorkspaceLocation::Constant(v),
            PyWorkspaceLocation::FromEnvVar { env, relpath } => {
                WorkspaceLocation::FromEnvVar { env, relpath }
            }
        }
    }
}

#[pymethods]
impl PyWorkspaceLocation {
    #[staticmethod]
    fn from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<Self> {
        bincode::deserialize(bytes.as_bytes())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
    }

    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyBytes>,))> {
        let bytes = bincode::serialize(&*slf.borrow())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = PyBytes::new(slf.py(), &bytes);
        Ok((slf.as_any().getattr("from_bytes")?, (py_bytes,)))
    }

    fn resolve(&self) -> PyResult<PathBuf> {
        let loc: WorkspaceLocation = self.clone().into();
        loc.resolve()
            .map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyclass(
    name = "WorkspaceShape",
    module = "monarch._rust_bindings.monarch_extension.code_sync",
    eq,
    frozen,
    get_all
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PyWorkspaceShape {
    dimension: Option<String>,
}

#[pymethods]
impl PyWorkspaceShape {
    #[staticmethod]
    fn shared(dimension: String) -> Self {
        Self {
            dimension: Some(dimension),
        }
    }

    #[staticmethod]
    fn exclusive() -> Self {
        Self { dimension: None }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyclass(
    module = "monarch._rust_bindings.monarch_extension.code_sync",
    eq,
    frozen,
    get_all
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct RemoteWorkspace {
    location: PyWorkspaceLocation,
    shape: PyWorkspaceShape,
}

#[pymethods]
impl RemoteWorkspace {
    #[new]
    #[pyo3(signature = (*, location, shape = PyWorkspaceShape::exclusive()))]
    fn new(location: PyWorkspaceLocation, shape: PyWorkspaceShape) -> Self {
        Self { location, shape }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyclass(
    name = "CodeSyncMethod",
    module = "monarch._rust_bindings.monarch_extension.code_sync",
    eq,
    frozen
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum PyCodeSyncMethod {
    Rsync {},
    CondaSync {
        path_prefix_replacements: HashMap<PathBuf, PyWorkspaceLocation>,
    },
}

impl From<PyCodeSyncMethod> for CodeSyncMethod {
    fn from(method: PyCodeSyncMethod) -> CodeSyncMethod {
        match method {
            PyCodeSyncMethod::Rsync {} => CodeSyncMethod::Rsync,
            PyCodeSyncMethod::CondaSync {
                path_prefix_replacements,
            } => CodeSyncMethod::CondaSync {
                path_prefix_replacements: path_prefix_replacements
                    .into_iter()
                    .map(|(l, r)| (l, r.into()))
                    .collect(),
            },
        }
    }
}

#[pymethods]
impl PyCodeSyncMethod {
    #[staticmethod]
    fn from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<Self> {
        bincode::deserialize(bytes.as_bytes())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
    }

    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyBytes>,))> {
        let bytes = bincode::serialize(&*slf.borrow())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = PyBytes::new(slf.py(), &bytes);
        Ok((slf.as_any().getattr("from_bytes")?, (py_bytes,)))
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyclass(
    name = "WorkspaceConfig",
    module = "monarch._rust_bindings.monarch_extension.code_sync",
    eq,
    frozen,
    get_all
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PyWorkspaceConfig {
    local: PathBuf,
    remote: RemoteWorkspace,
    method: PyCodeSyncMethod,
}

#[pymethods]
impl PyWorkspaceConfig {
    #[new]
    #[pyo3(signature = (*, local, remote, method = PyCodeSyncMethod::Rsync {}))]
    fn new(local: PathBuf, remote: RemoteWorkspace, method: PyCodeSyncMethod) -> Self {
        Self {
            local,
            remote,
            method,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyclass(
    frozen,
    name = "CodeSyncMeshClient",
    module = "monarch._rust_bindings.monarch_extension.code_sync"
)]
pub struct CodeSyncMeshClient {
    actor_mesh: SharedCell<RootActorMesh<'static, CodeSyncManager>>,
}

impl CodeSyncMeshClient {
    async fn sync_workspace_(
        actor_mesh: SharedCell<RootActorMesh<'static, CodeSyncManager>>,
        local: PathBuf,
        remote: RemoteWorkspace,
        method: CodeSyncMethod,
        auto_reload: bool,
    ) -> Result<()> {
        let actor_mesh = actor_mesh.borrow()?;
        let shape = WorkspaceShape {
            shape: actor_mesh.shape().clone(),
            dimension: remote.shape.dimension.clone(),
        };
        eprintln!("Syncing workspace: {:?}", shape.owners()?);
        let remote = WorkspaceConfig {
            location: remote.location.into(),
            shape,
        };
        code_sync_mesh(&actor_mesh, local, remote, method, auto_reload)
            .await
            .map_err(|err| PyRuntimeError::new_err(format!("{:#?}", err)))?;
        Ok(())
    }
}

#[pymethods]
impl CodeSyncMeshClient {
    #[staticmethod]
    #[pyo3(signature = (*, proc_mesh))]
    fn spawn_blocking(py: Python, proc_mesh: &PyProcMesh) -> PyResult<Self> {
        let proc_mesh = proc_mesh.try_inner()?;
        signal_safe_block_on(py, async move {
            let actor_mesh = proc_mesh
                .spawn("code_sync_manager", &CodeSyncManagerParams {})
                .await?;
            Ok(Self { actor_mesh })
        })?
    }

    #[pyo3(signature = (*, local, remote, method = PyCodeSyncMethod::Rsync {}, auto_reload = false))]
    fn sync_workspace<'py>(
        &self,
        py: Python<'py>,
        local: PathBuf,
        remote: RemoteWorkspace,
        method: PyCodeSyncMethod,
        auto_reload: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        monarch_hyperactor::runtime::future_into_py(
            py,
            CodeSyncMeshClient::sync_workspace_(
                self.actor_mesh.clone(),
                local,
                remote,
                method.into(),
                auto_reload,
            )
            .err_into(),
        )
    }

    #[pyo3(signature = (*, workspaces, auto_reload = false))]
    fn sync_workspaces<'py>(
        &self,
        py: Python<'py>,
        workspaces: Vec<PyWorkspaceConfig>,
        auto_reload: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let actor_mesh = self.actor_mesh.clone();
        monarch_hyperactor::runtime::future_into_py(
            py,
            async move {
                for workspace in workspaces.into_iter() {
                    CodeSyncMeshClient::sync_workspace_(
                        actor_mesh.clone(),
                        workspace.local,
                        workspace.remote,
                        workspace.method.into(),
                        auto_reload,
                    )
                    .await?
                }
                anyhow::Ok(())
            }
            .err_into(),
        )
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CodeSyncMeshClient>()?;
    module.add_class::<PyCodeSyncMethod>()?;
    module.add_class::<PyWorkspaceConfig>()?;
    module.add_class::<PyWorkspaceLocation>()?;
    module.add_class::<PyWorkspaceShape>()?;
    module.add_class::<RemoteWorkspace>()?;
    Ok(())
}
