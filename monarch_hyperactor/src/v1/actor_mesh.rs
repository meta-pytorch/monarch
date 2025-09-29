/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::ActorRef;
use hyperactor_mesh::v1::actor_mesh::ActorMesh;
use hyperactor_mesh::v1::actor_mesh::ActorMeshRef;
use ndslice::Selection;
use ndslice::view::Ranked;
use ndslice::view::RankedSliceable;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor_mesh::ActorMeshProtocol;
use crate::actor_mesh::PythonActorMesh;
use crate::context::PyInstance;
use crate::instance_dispatch;
use crate::proc::PyActorId;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;
use crate::shape::PyRegion;

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) struct PyActorMesh(ActorMesh<PythonActor>);

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMeshRef",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) struct PyActorMeshRef(ActorMeshRef<PythonActor>);

#[derive(Debug, Clone)]
#[pyclass(
    name = "PythonActorMeshImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) enum PythonActorMeshImpl {
    Owned(PyActorMesh),
    Ref(PyActorMeshRef),
}

impl PythonActorMeshImpl {
    /// Get a new owned [`PythonActorMeshImpl`].
    pub(crate) fn new_owned(inner: ActorMesh<PythonActor>) -> Self {
        PythonActorMeshImpl::Owned(PyActorMesh(inner))
    }

    /// Get a new ref-based [`PythonActorMeshImpl`].
    pub(crate) fn new_ref(inner: ActorMeshRef<PythonActor>) -> Self {
        PythonActorMeshImpl::Ref(PyActorMeshRef(inner))
    }

    fn mesh_ref(&self) -> ActorMeshRef<PythonActor> {
        match self {
            PythonActorMeshImpl::Owned(inner) => (*inner.0).clone(),
            PythonActorMeshImpl::Ref(inner) => inner.0.clone(),
        }
    }
}

impl ActorMeshProtocol for PythonActorMeshImpl {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        <ActorMeshRef<PythonActor> as ActorMeshProtocol>::cast(
            &self.mesh_ref(),
            message,
            selection,
            instance,
        )
    }

    fn supervision_event(&self) -> PyResult<Option<PyShared>> {
        Err(PyErr::new::<PyNotImplementedError, _>(
            "supervision_event is not implemented yet for v1::PythonActorMeshImpl",
        ))
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        self.mesh_ref().new_with_region(region)
    }

    fn stop<'py>(&self) -> PyResult<PyPythonTask> {
        Err(PyErr::new::<PyNotImplementedError, _>(
            "stop is not implemented yet for v1::PythonActorMeshImpl",
        ))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.mesh_ref().__reduce__(py)
    }
}

impl ActorMeshProtocol for ActorMeshRef<PythonActor> {
    fn cast(
        &self,
        message: PythonMessage,
        _selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        instance_dispatch!(instance, |cx_instance| {
            self.cast(cx_instance, message.clone())
                .map_err(|err| PyException::new_err(err.to_string()))?;
        });
        Ok(())
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        let sliced = self.sliced(region.as_inner().clone());
        Ok(Box::new(sliced))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let bytes =
            bincode::serialize(self).map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let module = py
            .import("monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh")
            .unwrap();
        let from_bytes = module.getattr("py_actor_mesh_from_bytes").unwrap();
        Ok((from_bytes, py_bytes))
    }
}

#[pymethods]
impl PythonActorMeshImpl {
    fn get(&self, rank: usize) -> PyResult<Option<PyActorId>> {
        Ok(self
            .mesh_ref()
            .get(rank)
            .map(|r| ActorRef::into_actor_id(r.clone()))
            .map(PyActorId::from))
    }
}

#[pyfunction]
fn py_actor_mesh_from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<PythonActorMesh> {
    let r: PyResult<ActorMeshRef<PythonActor>> = bincode::deserialize(bytes.as_bytes())
        .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()));
    r.map(|r| PythonActorMesh::from_impl(Box::new(PythonActorMeshImpl::new_ref(r))))
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PythonActorMeshImpl>()?;
    let f = wrap_pyfunction!(py_actor_mesh_from_bytes, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh",
    )?;
    hyperactor_mod.add_function(f)?;
    Ok(())
}
