/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::ProcMeshRef;
use hyperactor_mesh::shared_cell::SharedCell;
use monarch_types::PickledPyObject;
use monarch_types::py_module_add_function;
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
use crate::actor_mesh::ActorMeshProtocol;
use crate::actor_mesh::PythonActorMesh;
use crate::actor_mesh::PythonActorMeshImpl;
use crate::alloc::PyAlloc;
use crate::context::PyInstance;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::shape::PyRegion;

#[pyclass(
    name = "ProcMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.proc_mesh"
)]
pub enum PyProcMesh {
    Owned(PyProcMeshImpl),
    Ref(PyProcMeshRefImpl),
}

impl PyProcMesh {
    pub fn new_owned(inner: ProcMesh) -> Self {
        Self::Owned(PyProcMeshImpl(inner.into()))
    }

    pub(crate) fn new_ref(inner: ProcMeshRef) -> Self {
        Self::Ref(PyProcMeshRefImpl(inner))
    }

    pub fn mesh_ref(&self) -> PyResult<ProcMeshRef> {
        match self {
            PyProcMesh::Owned(inner) => Ok(inner
                .0
                .borrow()
                .map_err(|_| PyRuntimeError::new_err("`ProcMesh` has already been stopped"))?
                .clone()),
            PyProcMesh::Ref(inner) => Ok(inner.0.clone()),
        }
    }
}

#[pymethods]
impl PyProcMesh {
    #[classmethod]
    fn allocate_nonblocking<'py>(
        _cls: &Bound<'_, PyType>,
        _py: Python<'py>,
        instance: &PyInstance,
        alloc: &mut PyAlloc,
        name: String,
    ) -> PyResult<PyPythonTask> {
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
            let mesh = ProcMesh::allocate(instance.deref(), alloc, &name)
                .await
                .map_err(|err| PyException::new_err(err.to_string()))?;
            Ok(Self::new_owned(mesh))
        })
    }

    #[pyo3(signature = (instance, name, actor, supervision_display_name = None))]
    fn spawn_nonblocking<'py>(
        &self,
        instance: &PyInstance,
        name: String,
        actor: &Bound<'py, PyType>,
        supervision_display_name: Option<String>,
    ) -> PyResult<PyPythonTask> {
        let pickled_type = PickledPyObject::pickle(actor.as_any())?;
        let proc_mesh = self.mesh_ref()?.clone();
        let instance = instance.clone();
        let mesh_impl = async move {
            let full_name = hyperactor_mesh::Name::new(name).unwrap();
            let actor_mesh = proc_mesh
                .spawn_with_name(
                    instance.deref(),
                    full_name,
                    &pickled_type,
                    supervision_display_name,
                    false,
                )
                .await
                .map_err(to_py_error)?;
            Ok(PythonActorMesh::from_impl(Arc::new(
                PythonActorMeshImpl::new_owned(actor_mesh),
            )))
        };
        PyPythonTask::new(mesh_impl)
    }

    #[staticmethod]
    #[pyo3(signature = (proc_mesh, instance, name, actor, emulated, supervision_display_name = None))]
    fn spawn_async(
        proc_mesh: &mut PyShared,
        instance: &PyInstance,
        name: String,
        actor: Py<PyType>,
        emulated: bool,
        supervision_display_name: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let task = proc_mesh.task()?.take_task()?;
        let instance = instance.clone();
        let mesh_impl = async move {
            let proc_mesh = task.await?;
            let (proc_mesh, pickled_type) = monarch_with_gil(|py| -> PyResult<_> {
                let slf: Bound<PyProcMesh> = proc_mesh.extract(py)?;
                let slf = slf.borrow();
                let pickled_type = PickledPyObject::pickle(actor.bind(py).as_any())?;
                Ok((slf.mesh_ref()?.clone(), pickled_type))
            })
            .await?;

            let full_name = hyperactor_mesh::Name::new(name).unwrap();
            let actor_mesh = proc_mesh
                .spawn_with_name(
                    instance.deref(),
                    full_name,
                    &pickled_type,
                    supervision_display_name,
                    false,
                )
                .await
                .map_err(anyhow::Error::from)?;
            Ok::<_, PyErr>(Box::new(PythonActorMeshImpl::new_owned(actor_mesh)))
        };
        if emulated {
            // we give up on doing mesh spawn async for the emulated old version
            // it is too complicated to make both work.
            let r = get_tokio_runtime().block_on(mesh_impl)?;
            monarch_with_gil_blocking(|py| r.into_py_any(py))
        } else {
            let r = PythonActorMesh::new(
                async move {
                    let mesh_impl: Box<dyn ActorMeshProtocol> = mesh_impl.await?;
                    Ok(mesh_impl)
                },
                true,
            );
            monarch_with_gil_blocking(|py| r.into_py_any(py))
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        match self {
            PyProcMesh::Owned(inner) => Ok(format!("<ProcMesh: {:?}>", inner.__repr__()?)),
            PyProcMesh::Ref(inner) => Ok(format!("<ProcMesh: {:?}>", inner.__repr__()?)),
        }
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let bytes = bincode::serialize(&self.mesh_ref()?)
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let from_bytes =
            PyModule::import(py, "monarch._rust_bindings.monarch_hyperactor.proc_mesh")?
                .getattr("py_proc_mesh_from_bytes")?;
        Ok((from_bytes, py_bytes))
    }

    #[getter]
    fn region(&self) -> PyResult<PyRegion> {
        Ok(self.mesh_ref()?.region().into())
    }

    fn stop_nonblocking(&self, instance: &PyInstance, reason: String) -> PyResult<PyPythonTask> {
        // Clone the necessary fields from self to avoid capturing self in the async block
        let (owned_inner, instance) = monarch_with_gil_blocking(|_py| {
            let owned_inner = match self {
                PyProcMesh::Owned(inner) => inner.clone(),
                PyProcMesh::Ref(_) => {
                    return Err(PyValueError::new_err(
                        "ProcMesh is not owned; must be stopped by an owner",
                    ));
                }
            };

            let instance = instance.clone();
            Ok((owned_inner, instance))
        })?;
        PyPythonTask::new(async move {
            let mesh = owned_inner.0.take().await;
            match mesh {
                Ok(mut mesh) => mesh
                    .stop(instance.deref(), reason)
                    .await
                    .map_err(|e| PyValueError::new_err(format!("error stopping mesh: {}", e))),
                Err(e) => {
                    // Don't return an exception, silently ignore the stop request
                    // because it was already done.
                    tracing::info!("proc mesh already stopped: {}", e);
                    Ok(())
                }
            }
        })
    }

    fn sliced(&self, region: &PyRegion) -> PyResult<Self> {
        Ok(Self::new_ref(
            self.mesh_ref()?.sliced(region.as_inner().clone()),
        ))
    }
}

#[derive(Clone)]
#[pyclass(
    name = "ProcMeshImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.proc_mesh"
)]
pub struct PyProcMeshImpl(SharedCell<ProcMesh>);

impl PyProcMeshImpl {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "<ProcMeshImpl {:?}>",
            *self.0.borrow().map_err(anyhow::Error::from)?
        ))
    }
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "ProcMeshRefImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.proc_mesh"
)]
pub struct PyProcMeshRefImpl(ProcMeshRef);

impl PyProcMeshRefImpl {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<ProcMeshRefImpl {:?}>", self.0))
    }
}

#[pyfunction]
fn py_proc_mesh_from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<PyProcMesh> {
    let r: PyResult<ProcMeshRef> = bincode::deserialize(bytes.as_bytes())
        .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()));
    r.map(PyProcMesh::new_ref)
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyProcMesh>()?;
    py_module_add_function!(
        hyperactor_mod,
        "monarch._rust_bindings.monarch_hyperactor.proc_mesh",
        py_proc_mesh_from_bytes
    );
    Ok(())
}
