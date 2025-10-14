/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor_mesh::comm::multicast::CastInfo;
use hyperactor_mesh::proc_mesh::global_root_client;
use ndslice::Extent;
use ndslice::Point;
use pyo3::prelude::*;

use crate::actor::PythonActor;
use crate::mailbox::PyMailbox;
use crate::proc::PyActorId;
use crate::runtime;
use crate::shape::PyPoint;

pub enum ContextInstance {
    Client(hyperactor::Instance<()>),
    PythonActor(hyperactor::Instance<PythonActor>),
}

impl ContextInstance {
    fn mailbox_for_py(&self) -> &hyperactor::Mailbox {
        match self {
            ContextInstance::Client(ins) => ins.mailbox_for_py(),
            ContextInstance::PythonActor(ins) => ins.mailbox_for_py(),
        }
    }

    fn self_id(&self) -> &hyperactor::ActorId {
        match self {
            ContextInstance::Client(ins) => ins.self_id(),
            ContextInstance::PythonActor(ins) => ins.self_id(),
        }
    }
}

impl Clone for ContextInstance {
    fn clone(&self) -> Self {
        match self {
            ContextInstance::Client(ins) => ContextInstance::Client(ins.clone_for_py()),
            ContextInstance::PythonActor(ins) => ContextInstance::PythonActor(ins.clone_for_py()),
        }
    }
}

#[macro_export]
macro_rules! instance_dispatch {
    ($ins:expr, |$cx:ident| $code:block) => {
        match $ins.context_instance() {
            $crate::context::ContextInstance::Client($cx) => $code,
            $crate::context::ContextInstance::PythonActor($cx) => $code,
        }
    };
    ($ins:expr, |$cx:ident| $code:block) => {
        match $ins.into_context_instance() {
            $crate::context::ContextInstance::Client($cx) => $code,
            $crate::context::ContextInstance::PythonActor($cx) => $code,
        }
    };
    ($ins:expr, async |$cx:ident| $code:block) => {
        match $ins.context_instance() {
            $crate::context::ContextInstance::Client($cx) => async $code.await,
            $crate::context::ContextInstance::PythonActor($cx) => async $code.await,
        }
    };
    ($ins:expr, async move |$cx:ident| $code:block) => {
        match $ins.context_instance() {
            $crate::context::ContextInstance::Client($cx) => async move $code.await,
            $crate::context::ContextInstance::PythonActor($cx) => async move $code.await,
        }
    };
}

/// Similar to `instance_dispatch!`, but moves the PyInstance into an Instance<T>
/// instead of a borrow.
#[macro_export]
macro_rules! instance_into_dispatch {
    ($ins:expr, |$cx:ident| $code:block) => {
        match $ins.into_context_instance() {
            $crate::context::ContextInstance::Client($cx) => $code,
            $crate::context::ContextInstance::PythonActor($cx) => $code,
        }
    };
    ($ins:expr, async |$cx:ident| $code:block) => {
        match $ins.into_context_instance() {
            $crate::context::ContextInstance::Client($cx) => async $code.await,
            $crate::context::ContextInstance::PythonActor($cx) => async $code.await,
        }
    };
    ($ins:expr, async move |$cx:ident| $code:block) => {
        match $ins.into_context_instance() {
            $crate::context::ContextInstance::Client($cx) => async move $code.await,
            $crate::context::ContextInstance::PythonActor($cx) => async move $code.await,
        }
    };
}

#[derive(Clone)]
#[pyclass(name = "Instance", module = "monarch._src.actor.actor_mesh")]
pub struct PyInstance {
    inner: ContextInstance,
    #[pyo3(get, set)]
    proc_mesh: Option<PyObject>,
    #[pyo3(get, set, name = "_controller_controller")]
    controller_controller: Option<PyObject>,
    #[pyo3(get, set)]
    pub(crate) rank: PyPoint,
    #[pyo3(get, set, name = "_children")]
    children: Option<PyObject>,
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
    fn actor_id(&self) -> PyActorId {
        self.inner.self_id().clone().into()
    }
}

impl PyInstance {
    pub fn context_instance(&self) -> &ContextInstance {
        &self.inner
    }

    pub fn into_context_instance(self) -> ContextInstance {
        self.inner
    }
}

impl From<&hyperactor::Instance<PythonActor>> for ContextInstance {
    fn from(ins: &hyperactor::Instance<PythonActor>) -> Self {
        ContextInstance::PythonActor(ins.clone_for_py())
    }
}

impl From<&hyperactor::Instance<()>> for ContextInstance {
    fn from(ins: &hyperactor::Instance<()>) -> Self {
        ContextInstance::Client(ins.clone_for_py())
    }
}

impl From<&hyperactor::Context<'_, PythonActor>> for ContextInstance {
    fn from(cx: &hyperactor::Context<'_, PythonActor>) -> Self {
        ContextInstance::PythonActor(cx.clone_for_py())
    }
}

impl From<&hyperactor::Context<'_, ()>> for ContextInstance {
    fn from(cx: &hyperactor::Context<'_, ()>) -> Self {
        ContextInstance::Client(cx.clone_for_py())
    }
}

impl<I: Into<ContextInstance>> From<I> for PyInstance {
    fn from(ins: I) -> Self {
        PyInstance {
            inner: ins.into(),
            proc_mesh: None,
            controller_controller: None,
            rank: PyPoint::new(0, Extent::unity().into()),
            children: None,
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
        let instance: PyInstance = global_root_client().into();
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
