/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::ops::Deref;

use hyperactor::ActorHandle;
use hyperactor::context;
use hyperactor_mesh::logging::LogClientActor;
use hyperactor_mesh::logging::LogClientMessage;
use hyperactor_mesh::logging::LogForwardActor;
use hyperactor_mesh::logging::LogForwardMessage;
use hyperactor_mesh::v1::ActorMesh;
use hyperactor_mesh::v1::Name;
use hyperactor_mesh::v1::actor_mesh::ActorMeshRef;
use ndslice::View;
use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::context::PyInstance;
use crate::instance_dispatch;
use crate::logging::LoggerRuntimeActor;
use crate::logging::LoggerRuntimeMessage;
use crate::pytokio::PyPythonTask;
use crate::v1::proc_mesh::PyProcMesh;

#[pyclass(
    frozen,
    name = "LoggingMeshClient",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.logging"
)]
pub struct LoggingMeshClient {
    // handles remote process log forwarding; no python runtime
    forwarder_mesh: ActorMesh<LogForwardActor>,
    // handles python logger; has python runtime
    logger_mesh: ActorMesh<LoggerRuntimeActor>,
    client_actor: ActorHandle<LogClientActor>,
}

impl LoggingMeshClient {
    async fn flush_internal(
        cx: &impl context::Actor,
        client_actor: ActorHandle<LogClientActor>,
        forwarder_mesh: ActorMeshRef<LogForwardActor>,
    ) -> Result<(), anyhow::Error> {
        let (reply_tx, reply_rx) = cx.instance().open_once_port::<()>();
        let (version_tx, version_rx) = cx.instance().open_once_port::<u64>();

        // First initialize a sync flush.
        client_actor.send(
            cx,
            LogClientMessage::StartSyncFlush {
                expected_procs: forwarder_mesh.region().num_ranks(),
                reply: reply_tx.bind(),
                version: version_tx.bind(),
            },
        )?;

        let version = version_rx.recv().await?;

        // Then ask all the flushers to ask the log forwarders to sync flush
        forwarder_mesh.cast(cx, LogForwardMessage::ForceSyncFlush { version })?;

        // Finally the forwarder will send sync point back to the client, flush, and return.
        reply_rx.recv().await?;

        Ok(())
    }
}

#[pymethods]
impl LoggingMeshClient {
    #[staticmethod]
    fn spawn(instance: &PyInstance, proc_mesh: &PyProcMesh) -> PyResult<PyPythonTask> {
        let proc_mesh = proc_mesh.mesh_ref()?;
        let instance = instance.clone();
        PyPythonTask::new(async move {
            let client_actor: ActorHandle<LogClientActor> =
                instance_dispatch!(instance, async move |cx_instance| {
                    cx_instance
                        .proc()
                        .spawn(&Name::new("log_client").to_string(), ())
                        .await
                })?;
            let client_actor_ref = client_actor.bind();
            let forwarder_mesh = instance_dispatch!(instance, async |cx_instance| {
                proc_mesh
                    .spawn(cx_instance, "log_forwarder", &client_actor_ref)
                    .await
            })
            .map_err(anyhow::Error::from)?;
            let logger_mesh = instance_dispatch!(instance, async |cx_instance| {
                proc_mesh.spawn(cx_instance, "logger", &()).await
            })
            .map_err(anyhow::Error::from)?;

            // FIXME: Flush on proc mesh stop.

            Ok(Self {
                forwarder_mesh,
                logger_mesh,
                client_actor,
            })
        })
    }

    fn set_mode(
        &self,
        instance: &PyInstance,
        stream_to_client: bool,
        aggregate_window_sec: Option<u64>,
        level: u8,
    ) -> PyResult<()> {
        if aggregate_window_sec.is_some() && !stream_to_client {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "cannot set aggregate window without streaming to client".to_string(),
            ));
        }

        instance_dispatch!(instance, |cx_instance| {
            self.forwarder_mesh
                .cast(cx_instance, LogForwardMessage::SetMode { stream_to_client })
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        instance_dispatch!(instance, |cx_instance| {
            self.logger_mesh
                .cast(cx_instance, LoggerRuntimeMessage::SetLogging { level })
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        instance_dispatch!(instance, |cx_instance| {
            self.client_actor.send(
                cx_instance,
                LogClientMessage::SetAggregate {
                    aggregate_window_sec,
                },
            )
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    // A sync flush mechanism for the client make sure all the stdout/stderr are streamed back and flushed.
    fn flush(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        let forwarder_mesh = self.forwarder_mesh.deref().clone();
        let client_actor = self.client_actor.clone();
        let instance = instance.clone();

        PyPythonTask::new(async move {
            instance_dispatch!(instance, async move |cx_instance| {
                Self::flush_internal(cx_instance, client_actor, forwarder_mesh).await
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
        })
    }
}

impl Drop for LoggingMeshClient {
    fn drop(&mut self) {
        match self.client_actor.drain_and_stop() {
            Ok(_) => {}
            Err(e) => {
                // it is ok as during shutdown, the channel might already be closed
                tracing::debug!("error draining logging client actor during shutdown: {}", e);
            }
        }
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<LoggingMeshClient>()?;
    Ok(())
}
