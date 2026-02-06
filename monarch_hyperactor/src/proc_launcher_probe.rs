/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Probe module for validating the explicit response port contract.
//!
//! This module exposes a Rust function callable from Python that
//! inspects what Rust receives when a Python actor sends on a port
//! created via `explicit_response_port=True`.
//!
//! In particular, it answers the question: when Python calls
//! `Port.send(value)` or `Port.exception(error)`, does Rust receive a
//! `PythonMessage` envelope with `kind = Result` or `kind =
//! Exception`?

use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::types::PyModuleMethods;

use crate::actor::MethodSpecifier;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::actor_mesh::PythonActorMesh;
use crate::context::PyInstance;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PyMailbox;
use crate::mailbox::PythonOncePortRef;
use crate::pickle::PendingMessage;
use crate::pickle::PicklingState;
use crate::pytokio::PyPythonTask;

/// Report describing what Rust received on the port.
///
/// This is returned to Python so tests can assert on the wire-level
/// message shape, without decoding or interpreting the payload.
#[pyclass(
    frozen,
    module = "monarch._rust_bindings.monarch_hyperactor.proc_launcher_probe"
)]
#[derive(Debug, Clone)]
pub struct ProbeReport {
    /// High-level classification of what was received: e.g.
    /// "PythonMessage" or "Error".
    #[pyo3(get)]
    pub received_type: String,

    /// If a PythonMessage was received, the message kind ("Result",
    /// "Exception", etc).
    #[pyo3(get)]
    pub kind: Option<String>,

    /// If a PythonMessage was received, the `rank` field carried by
    /// the message kind (if any).
    #[pyo3(get)]
    pub rank: Option<usize>,

    /// Length in bytes of the raw message payload.
    #[pyo3(get)]
    pub payload_len: usize,

    /// Raw payload bytes as received by Rust.
    ///
    /// Exposed so Python can decode the payload (e.g. via
    /// cloudpickle) and verify its contents.
    #[pyo3(get)]
    pub payload_bytes: Vec<u8>,

    /// Error message if the probe failed before receiving a message.
    #[pyo3(get)]
    pub error: Option<String>,
}

/// Register the probe bindings in the Python extension module.
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ProbeReport>()?;
    module.add_function(wrap_pyfunction!(probe_exit_port_via_mesh, module)?)?;
    Ok(())
}

/// Probe the explicit response port via the actor mesh.
///
/// This function:
/// 1. Opens a `OncePort<PythonMessage>` from the given mailbox.
/// 2. Sends a `CallMethod(ExplicitPort)` message to `method_name` via
///    `actor_mesh_inner.cast_unresolved(...)`.
/// 3. Awaits the first message received on the port.
/// 4. Returns a `ProbeReport` describing what Rust observed.
///
/// The purpose is not to test endpoint semantics, but to validate the
/// *wire envelope* delivered to Rust for explicit response ports.
///
/// Arguments:
/// - `actor_mesh_inner`: The internal actor mesh used to dispatch the
///   call.
/// - `instance`: The calling context's Rust instance handle.
/// - `mailbox`: The mailbox used to allocate the response port.
/// - `method_name`: Name of the Python endpoint to invoke.
/// - `pickling_state`: The pickled arguments wrapped in a PicklingState.
///
/// Returns:
/// An awaitable task yielding a `ProbeReport`.
#[pyfunction]
#[pyo3(signature = (actor_mesh_inner, instance, mailbox, method_name, pickling_state))]
pub(crate) fn probe_exit_port_via_mesh(
    actor_mesh_inner: &PythonActorMesh,
    instance: &PyInstance,
    mailbox: &PyMailbox,
    method_name: String,
    pickling_state: PyRefMut<'_, PicklingState>,
) -> PyResult<PyPythonTask> {
    // Open a OncePort<PythonMessage> - this is what ActorProcLauncher
    // does
    let (exit_port, exit_port_rx) = mailbox.get_inner().open_once_port::<PythonMessage>();

    // Build the PythonMessageKind with ExplicitPort
    let bound_port = exit_port.bind();
    let kind = PythonMessageKind::CallMethod {
        name: MethodSpecifier::ExplicitPort {
            name: method_name.clone(),
        },
        response_port: Some(EitherPortRef::Once(PythonOncePortRef::from(bound_port))),
    };

    // Create a PendingMessage using py_new which takes PyRefMut
    let mut pending_message = PendingMessage::py_new(kind, pickling_state)?;

    // Cast to all actors in the mesh using cast_unresolved
    actor_mesh_inner.cast_unresolved(&mut pending_message, "all", instance)?;

    // Return an awaitable task that receives the result
    PyPythonTask::new(async move {
        let msg = exit_port_rx.recv().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("recv failed: {}", e))
        })?;

        let (kind, rank) = match &msg.kind {
            PythonMessageKind::Result { rank } => ("Result".to_string(), *rank),
            PythonMessageKind::Exception { rank } => ("Exception".to_string(), *rank),
            PythonMessageKind::CallMethod { .. } => ("CallMethod".to_string(), None),
            PythonMessageKind::CallMethodIndirect { .. } => {
                ("CallMethodIndirect".to_string(), None)
            }
            PythonMessageKind::Uninit {} => ("Uninit".to_string(), None),
        };

        let payload = msg.message.to_bytes().to_vec();
        Ok(ProbeReport {
            received_type: "PythonMessage".to_string(),
            kind: Some(kind),
            rank,
            payload_len: payload.len(),
            payload_bytes: payload,
            error: None,
        })
    })
}
