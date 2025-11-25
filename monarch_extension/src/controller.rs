/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// These the controller messages that are exposed to python to allow the client to construct and
/// send messages to the controller. For more details of the definitions take a look at
/// [`monarch_messages::controller::ControllerMessage`].
use hyperactor::data::Serialized;
use monarch_hyperactor::ndslice::PySlice;
use monarch_hyperactor::proc::PySerialized;
use monarch_messages::controller::Seq;
use monarch_messages::controller::*;
use monarch_messages::worker::Ref;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::tensor_worker::PyWorkerMessage;

#[pyclass(
    frozen,
    get_all,
    module = "monarch._rust_bindings.monarch_extension.controller"
)]
struct Node {
    seq: Seq,
    defs: Vec<Ref>,
    uses: Vec<Ref>,
}

#[pymethods]
impl Node {
    #[new]
    #[pyo3(signature = (*, seq, defs, uses))]
    fn new(seq: Seq, defs: Vec<Ref>, uses: Vec<Ref>) -> Self {
        Node { seq, defs, uses }
    }

    fn serialize(&self) -> PyResult<PySerialized> {
        PySerialized::new(&ControllerMessage::Node {
            seq: self.seq,
            defs: self.defs.clone(),
            uses: self.uses.clone(),
        })
    }

    #[staticmethod]
    fn from_serialized(serialized: &PySerialized) -> PyResult<Self> {
        let message = serialized.deserialized()?;
        match message {
            ControllerMessage::Node { seq, defs, uses } => Ok(Node { seq, defs, uses }),
            _ => Err(PyValueError::new_err(format!(
                "Expected Node message, got {:?}",
                message
            ))),
        }
    }
}

#[derive(Clone, FromPyObject)]
pub enum PyRanks {
    Slice(PySlice),
    SliceList(Vec<PySlice>),
}

#[pyclass(frozen, module = "monarch._rust_bindings.monarch_extension.controller")]
struct Send {
    ranks: Ranks,
    message: Serialized,
}

#[pymethods]
impl Send {
    #[new]
    #[pyo3(signature = (*, ranks, message))]
    fn new(ranks: PyRanks, message: PyRef<PyWorkerMessage>) -> PyResult<Self> {
        let ranks = match ranks {
            PyRanks::Slice(r) => Ranks::Slice(r.into()),
            PyRanks::SliceList(r) => {
                if r.is_empty() {
                    return Err(PyValueError::new_err("Send requires at least one rank"));
                }
                Ranks::SliceList(r.into_iter().map(|r| r.into()).collect())
            }
        };
        // println!("send: {:?}", &message.message);
        Ok(Self {
            ranks,
            message: message.to_serialized()?,
        })
    }

    #[getter]
    fn ranks(&self) -> Vec<PySlice> {
        match &self.ranks {
            Ranks::Slice(r) => vec![r.clone().into()],
            Ranks::SliceList(r) => r.iter().map(|r| r.clone().into()).collect(),
        }
    }

    #[getter]
    fn message(&self, py: Python<'_>) -> PyResult<PyObject> {
        let worker_message = self.message.deserialized().map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to deserialize worker message: {}", err))
        })?;
        crate::tensor_worker::worker_message_to_py(py, &worker_message)
    }

    fn serialize(&self) -> PyResult<PySerialized> {
        let msg = ControllerMessage::Send {
            ranks: self.ranks.clone(),
            message: self.message.clone(),
        };
        PySerialized::new(&msg)
    }

    #[staticmethod]
    fn from_serialized(serialized: &PySerialized) -> PyResult<Self> {
        let message = serialized.deserialized()?;
        match message {
            ControllerMessage::Send { ranks, message } => Ok(Send { ranks, message }),
            _ => Err(PyValueError::new_err(format!(
                "Expected Send message, got {:?}",
                message
            ))),
        }
    }
}

pub(crate) fn register_python_bindings(controller_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    controller_mod.add_class::<Node>()?;
    controller_mod.add_class::<Send>()?;
    Ok(())
}
