/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt::Debug;
use std::ops::Deref;

use hyperactor::id::Label;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::ProcMeshRef;
use hyperactor_mesh::mesh_id::ActorMeshId;
use hyperactor_mesh::shared_cell::SharedCell;
use monarch_types::PickledPyObject;
use monarch_types::py_module_add_function;
use ndslice::View;
use ndslice::view::RankedSliceable;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyType;

use crate::actor::ActorKind;
use crate::actor::PythonActorParams;
use crate::actor_mesh::PythonActorMesh;
use crate::actor_mesh::PythonActorMeshImpl;
use crate::actor_mesh::SupervisableActorMesh;
use crate::context::PyInstance;
use crate::handle::PyHandle;
use crate::pickle::PendingMessage;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::shape::PyRegion;

#[pyclass(
    name = "ProcMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.proc_mesh"
)]
#[expect(
    clippy::large_enum_variant,
    reason = "PyO3 #[pyclass] enum; Box wrapping interacts with PyO3 codegen and Python interop — separate diff"
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
    /// Return a Handle that resolves to the requested slice after proc mesh creation
    /// succeeds. If creation or slicing returns an error, `.get()`, `poll()` and
    /// `await` raise it.
    #[staticmethod]
    fn sliced_handle(proc_mesh: &PyHandle, region: &PyRegion) -> PyHandle {
        let mesh = proc_mesh.wait_future();
        let region = region.clone();
        PyHandle::spawn(async move {
            let mesh = mesh.await?;
            monarch_with_gil(GilSite::Convert, |py| {
                mesh.bind(py)
                    .extract::<PyRef<'_, PyProcMesh>>()?
                    .sliced(&region)
            })
            .await
        })
    }

    #[staticmethod]
    #[pyo3(signature = (proc_mesh, instance, mesh_base_name, actor, init_message, kind, emulated, supervision_display_name = None))]
    fn spawn_async(
        py: Python<'_>,
        proc_mesh: &PyHandle,
        instance: &PyInstance,
        mesh_base_name: String,
        actor: Py<PyType>,
        init_message: &mut PendingMessage,
        kind: ActorKind,
        emulated: bool,
        supervision_display_name: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        // Pickling may run Python code; do it on the calling Python thread.
        let pickled_type = PickledPyObject::pickle(actor.bind(py).as_any())?;
        let init_message = init_message.take()?;
        let task = proc_mesh.wait_future();
        let instance = instance.clone();
        let mesh_impl = async move {
            let proc_mesh = task.await?;

            let init_message = init_message.resolve().await?;

            let proc_mesh = monarch_with_gil(GilSite::ActorConstruct, |py| -> PyResult<_> {
                let slf: Bound<PyProcMesh> = proc_mesh.extract(py)?;
                let slf = slf.borrow();
                Ok(slf.mesh_ref()?.clone())
            })
            .await?;
            let params = PythonActorParams::new(pickled_type, Some(init_message), kind);

            let mesh_name = ActorMeshId::instance(Label::strip(&mesh_base_name));
            let actor_mesh = proc_mesh
                .spawn_with_name(
                    instance.deref(),
                    mesh_name,
                    &params,
                    supervision_display_name,
                    false,
                )
                .await
                .map_err(anyhow::Error::from)?;
            Ok::<_, PyErr>(Box::new(PythonActorMeshImpl::new_owned(
                actor_mesh,
                instance.deref().clone_for_py(),
            )))
        };
        if emulated {
            // we give up on doing mesh spawn async for the emulated old version
            // it is too complicated to make both work.
            let r = get_tokio_runtime().block_on(mesh_impl)?;
            monarch_with_gil_blocking(GilSite::Convert, |py| r.into_py_any(py))
        } else {
            let r = PythonActorMesh::new(
                async move {
                    let mesh_impl: Box<dyn SupervisableActorMesh> = mesh_impl.await?;
                    Ok(mesh_impl)
                },
                true,
            );
            monarch_with_gil_blocking(GilSite::Convert, |py| r.into_py_any(py))
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        match self {
            PyProcMesh::Owned(inner) => Ok(format!("<ProcMesh: {:?}>", inner.__repr__()?)),
            PyProcMesh::Ref(inner) => Ok(format!("<ProcMesh: {:?}>", inner.__repr__()?)),
        }
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let mesh_ref = self.mesh_ref()?;
        if crate::pickle::push_mesh_reference_if_active(crate::actor::MeshRef::Proc(Box::new(
            mesh_ref.clone(),
        ))) {
            let pop_fn = PyModule::import(py, "monarch._rust_bindings.monarch_hyperactor.pickle")?
                .getattr("pop_mesh_reference")?;
            return Ok((pop_fn, pyo3::types::PyTuple::empty(py).into_any()));
        }
        let bytes = bincode::serde::encode_to_vec(&mesh_ref, bincode::config::legacy())
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

    fn stop_nonblocking(&self, instance: &PyInstance, reason: String) -> PyResult<PyHandle> {
        // Clone the necessary fields from self to avoid capturing self in the async block
        let (owned_inner, instance) = monarch_with_gil_blocking(GilSite::Stop, |_py| {
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
        Ok(PyHandle::spawn(async move {
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
        }))
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
    let r: PyResult<ProcMeshRef> =
        bincode::serde::decode_from_slice(bytes.as_bytes(), bincode::config::legacy())
            .map(|(v, _)| v)
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
