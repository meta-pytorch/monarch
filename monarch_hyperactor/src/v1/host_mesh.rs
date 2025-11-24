/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;

use hyperactor_mesh::bootstrap::BootstrapCommand;
use hyperactor_mesh::shared_cell::SharedCell;
use hyperactor_mesh::v1::host_mesh::HostMesh;
use hyperactor_mesh::v1::host_mesh::HostMeshRef;
use ndslice::View;
use ndslice::view::RankedSliceable;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyType;

use crate::actor::to_py_error;
use crate::alloc::PyAlloc;
use crate::context::PyInstance;
use crate::pytokio::PyPythonTask;
use crate::shape::PyExtent;
use crate::shape::PyRegion;
use crate::v1::proc_mesh::PyProcMesh;

#[pyclass(
    name = "BootstrapCommand",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.host_mesh"
)]
#[derive(Clone)]
pub struct PyBootstrapCommand {
    #[pyo3(get, set)]
    pub program: String,
    #[pyo3(get, set)]
    pub arg0: Option<String>,
    #[pyo3(get, set)]
    pub args: Vec<String>,
    #[pyo3(get, set)]
    pub env: HashMap<String, String>,
}

#[pymethods]
impl PyBootstrapCommand {
    #[new]
    fn new(
        program: String,
        arg0: Option<String>,
        args: Vec<String>,
        env: HashMap<String, String>,
    ) -> Self {
        Self {
            program,
            arg0,
            args,
            env,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "BootstrapCommand(program='{}', args={:?}, env={:?})",
            self.program, self.args, self.env
        )
    }
}

impl PyBootstrapCommand {
    pub fn to_rust(&self) -> BootstrapCommand {
        BootstrapCommand {
            program: PathBuf::from(&self.program),
            arg0: self.arg0.clone(),
            args: self.args.clone(),
            env: self.env.clone(),
        }
    }

    pub fn default<'py>(py: Python<'py>) -> PyResult<Bound<'py, Self>> {
        py.import("monarch._src.actor.v1.host_mesh")?
            .getattr("_bootstrap_cmd")?
            .call0()?
            .downcast::<PyBootstrapCommand>()
            .cloned()
            .map_err(to_py_error)
    }
}

#[pyclass(
    name = "HostMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.host_mesh"
)]
pub(crate) enum PyHostMesh {
    Owned(PyHostMeshImpl),
    Ref(PyHostMeshRefImpl),
}

impl PyHostMesh {
    pub(crate) fn new_owned(inner: HostMesh) -> Self {
        Self::Owned(PyHostMeshImpl(SharedCell::from(inner)))
    }

    fn new_ref(inner: HostMeshRef) -> Self {
        Self::Ref(PyHostMeshRefImpl(inner))
    }

    fn mesh_ref(&self) -> Result<HostMeshRef, anyhow::Error> {
        match self {
            PyHostMesh::Owned(inner) => Ok(inner.0.borrow()?.clone()),
            PyHostMesh::Ref(inner) => Ok(inner.0.clone()),
        }
    }
}

#[pymethods]
impl PyHostMesh {
    #[classmethod]
    fn allocate_nonblocking(
        _cls: &Bound<'_, PyType>,
        instance: &PyInstance,
        alloc: &mut PyAlloc,
        name: String,
        bootstrap_params: Option<PyBootstrapCommand>,
    ) -> PyResult<PyPythonTask> {
        let bootstrap_params =
            bootstrap_params.map_or_else(|| alloc.bootstrap_command.clone(), |b| Some(b.to_rust()));
        let alloc = match alloc.take() {
            Some(alloc) => alloc,
            None => {
                return Err(PyException::new_err(
                    "Alloc object already used".to_string(),
                ));
            }
        };
        let instance = instance.clone();
        PyPythonTask::new(async move {
            let mesh = HostMesh::allocate(instance.deref(), alloc, &name, bootstrap_params)
                .await
                .map_err(|err| PyException::new_err(err.to_string()))?;
            Ok(Self::new_owned(mesh))
        })
    }

    fn spawn_nonblocking(
        &self,
        instance: &PyInstance,
        name: String,
        per_host: &PyExtent,
    ) -> PyResult<PyPythonTask> {
        let host_mesh = self.mesh_ref()?.clone();
        let instance = instance.clone();
        let per_host = per_host.clone().into();
        let mesh_impl = async move {
            let proc_mesh = host_mesh
                .spawn(instance.deref(), &name, per_host)
                .await
                .map_err(to_py_error)?;
            Ok(PyProcMesh::new_owned(proc_mesh))
        };
        PyPythonTask::new(mesh_impl)
    }

    fn sliced(&self, region: &PyRegion) -> PyResult<Self> {
        Ok(Self::new_ref(
            self.mesh_ref()?.sliced(region.as_inner().clone()),
        ))
    }

    #[getter]
    fn region(&self) -> PyResult<PyRegion> {
        Ok(PyRegion::from(self.mesh_ref()?.region()))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let bytes = bincode::serialize(&self.mesh_ref()?)
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let from_bytes =
            PyModule::import(py, "monarch._rust_bindings.monarch_hyperactor.v1.host_mesh")?
                .getattr("py_host_mesh_from_bytes")?;
        Ok((from_bytes, py_bytes))
    }

    fn __eq__(&self, other: &PyHostMesh) -> PyResult<bool> {
        Ok(self.mesh_ref()? == other.mesh_ref()?)
    }

    fn shutdown(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        match self {
            PyHostMesh::Owned(inner) => {
                let instance = instance.clone();
                let mesh_borrow = inner.0.borrow().map_err(anyhow::Error::from)?;
                let fut = async move { Ok(mesh_borrow.shutdown(instance.deref()).await?) };
                PyPythonTask::new(fut)
            }
            PyHostMesh::Ref(_) => Err(PyRuntimeError::new_err(
                "cannot shut down `HostMesh` that is a reference instead of owned",
            )),
        }
    }
}

#[derive(Clone)]
#[pyclass(
    name = "HostMeshImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.host_mesh"
)]
pub(crate) struct PyHostMeshImpl(SharedCell<HostMesh>);

#[derive(Debug, Clone)]
#[pyclass(
    name = "HostMeshRefImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.host_mesh"
)]
pub(crate) struct PyHostMeshRefImpl(HostMeshRef);

impl PyHostMeshRefImpl {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<HostMeshRefImpl {:?}>", self.0))
    }
}

#[pyfunction]
fn py_host_mesh_from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<PyHostMesh> {
    let r: PyResult<HostMeshRef> = bincode::deserialize(bytes.as_bytes())
        .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()));
    r.map(PyHostMesh::new_ref)
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(py_host_mesh_from_bytes, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.v1.host_mesh",
    )?;
    hyperactor_mod.add_function(f)?;
    hyperactor_mod.add_class::<PyHostMesh>()?;
    hyperactor_mod.add_class::<PyBootstrapCommand>()?;
    Ok(())
}
