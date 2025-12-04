/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::Instance;
use hyperactor::context;
use hyperactor_mesh::comm::multicast::CastInfo;
use ndslice::Extent;
use ndslice::Point;
use pyo3::prelude::*;

use crate::actor::PythonActor;
use crate::actor::root_client_actor;
use crate::mailbox::PyMailbox;
use crate::proc::PyActorId;
use crate::runtime;
use crate::shape::PyPoint;

#[pyclass(name = "Instance", module = "monarch._src.actor.actor_mesh")]
pub struct PyInstance {
    inner: Instance<PythonActor>,
    #[pyo3(get, set)]
    proc_mesh: Option<PyObject>,
    #[pyo3(get, set, name = "_controller_controller")]
    controller_controller: Option<PyObject>,
    #[pyo3(get, set)]
    pub(crate) rank: PyPoint,
    #[pyo3(get, set, name = "_children")]
    children: Option<PyObject>,

    #[pyo3(get, set, name = "name")]
    name: String,
    #[pyo3(get, set, name = "class_name")]
    class_name: Option<String>,
    #[pyo3(get, set, name = "creator")]
    creator: Option<PyObject>,
}

impl Clone for PyInstance {
    fn clone(&self) -> Self {
        PyInstance {
            inner: self.inner.clone_for_py(),
            proc_mesh: self.proc_mesh.clone(),
            controller_controller: self.controller_controller.clone(),
            rank: self.rank.clone(),
            children: self.children.clone(),
            name: self.name.clone(),
            class_name: self.class_name.clone(),
            creator: self.creator.clone(),
        }
    }
}

impl std::ops::Deref for PyInstance {
    type Target = Instance<PythonActor>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[pymethods]
impl PyInstance {
    #[getter]
    pub(crate) fn _mailbox(&self) -> PyMailbox {
        PyMailbox {
            inner: self.inner.mailbox_for_py().clone(),
        }
    }

    #[getter]
    pub fn actor_id(&self) -> PyActorId {
        self.inner.self_id().clone().into()
    }
}

impl PyInstance {
    pub fn into_instance(self) -> Instance<PythonActor> {
        self.inner
    }
}

impl<I: context::Actor<A = PythonActor>> From<I> for PyInstance {
    fn from(ins: I) -> Self {
        PyInstance {
            inner: ins.instance().clone_for_py(),
            proc_mesh: None,
            controller_controller: None,
            rank: PyPoint::new(0, Extent::unity().into()),
            children: None,
            name: "root".to_string(),
            class_name: None,
            creator: None,
        }
    }
}

#[pyclass(name = "Context", module = "monarch._src.actor.actor_mesh")]
pub(crate) struct PyContext {
    instance: Py<PyInstance>,
    rank: Point,
}

#[pymethods]
impl PyContext {
    #[getter]
    fn actor_instance(&self) -> &Py<PyInstance> {
        &self.instance
    }

    #[getter]
    fn message_rank(&self) -> PyPoint {
        self.rank.clone().into()
    }

    #[staticmethod]
    fn _root_client_context(py: Python<'_>) -> PyResult<PyContext> {
        let _guard = runtime::get_tokio_runtime().enter();
        let instance: PyInstance = root_client_actor().into();
        Ok(PyContext {
            instance: instance.into_pyobject(py)?.into(),
            rank: Extent::unity().point_of_rank(0).unwrap(),
        })
    }
}

impl PyContext {
    pub(crate) fn new<T: hyperactor::actor::Actor>(
        cx: &hyperactor::Context<T>,
        instance: Py<PyInstance>,
    ) -> PyContext {
        PyContext {
            instance,
            rank: cx.cast_point(),
        }
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyInstance>()?;
    hyperactor_mod.add_class::<PyContext>()?;
    Ok(())
}
