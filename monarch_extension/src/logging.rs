/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::ops::Deref;
use std::time::Duration;

use hyperactor::ActorHandle;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::logging::LogClientActor;
use hyperactor_mesh::logging::LogClientMessage;
use hyperactor_mesh::logging::LogForwardActor;
use hyperactor_mesh::logging::LogForwardMessage;
use hyperactor_mesh::selection::Selection;
use hyperactor_mesh::shared_cell::SharedCell;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::logging::LoggerRuntimeActor;
use monarch_hyperactor::logging::LoggerRuntimeMessage;
use monarch_hyperactor::proc_mesh::PyProcMesh;
use monarch_hyperactor::pytokio::PyPythonTask;
use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::PyModule;

static FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

#[pyclass(
    frozen,
    name = "LoggingMeshClient",
    module = "monarch._rust_bindings.monarch_extension.logging"
)]
pub struct LoggingMeshClient {
    // handles remote process log forwarding; no python runtime
    forwarder_mesh: SharedCell<RootActorMesh<'static, LogForwardActor>>,
    // handles python logger; has python runtime
    logger_mesh: SharedCell<RootActorMesh<'static, LoggerRuntimeActor>>,
    client_actor: ActorHandle<LogClientActor>,
}

impl LoggingMeshClient {
    async fn flush_internal(
        cx: &impl context::Actor,
        client_actor: ActorHandle<LogClientActor>,
        forwarder_mesh: SharedCell<RootActorMesh<'static, LogForwardActor>>,
    ) -> Result<(), anyhow::Error> {
        let forwarder_inner_mesh = forwarder_mesh.borrow().map_err(anyhow::Error::msg)?;
        let (reply_tx, reply_rx) = forwarder_inner_mesh
            .proc_mesh()
            .client()
            .open_once_port::<()>();
        let (version_tx, version_rx) = forwarder_inner_mesh
            .proc_mesh()
            .client()
            .open_once_port::<u64>();

        // First initialize a sync flush.
        client_actor.send(
            cx,
            LogClientMessage::StartSyncFlush {
                expected_procs: forwarder_inner_mesh.proc_mesh().shape().slice().len(),
                reply: reply_tx.bind(),
                version: version_tx.bind(),
            },
        )?;

        let version = version_rx.recv().await?;

        // Then ask all the flushers to ask the log forwarders to sync flush
        forwarder_inner_mesh.cast(
            forwarder_inner_mesh.proc_mesh().client(),
            Selection::True,
            LogForwardMessage::ForceSyncFlush { version },
        )?;

        // Finally the forwarder will send sync point back to the client, flush, and return.
        reply_rx.recv().await?;

        Ok(())
    }
}

#[pymethods]
impl LoggingMeshClient {
    #[staticmethod]
    fn spawn(instance: PyInstance, proc_mesh: &PyProcMesh) -> PyResult<PyPythonTask> {
        let proc_mesh = proc_mesh.try_inner()?;
        PyPythonTask::new(async move {
            let client_actor = proc_mesh
                .client_proc()
                .spawn("log_client", LogClientActor::default())?;
            let client_actor_ref = client_actor.bind();
            let forwarder_mesh = proc_mesh
                .spawn(instance.deref(), "log_forwarder", &client_actor_ref)
                .await?;
            let logger_mesh = proc_mesh.spawn(instance.deref(), "logger", &()).await?;

            // Register flush_internal as a on-stop callback
            let client_actor_for_callback = client_actor.clone();
            let forwarder_mesh_for_callback = forwarder_mesh.clone();
            proc_mesh
                .register_onstop_callback(|| async move {
                    match RealClock
                        .timeout(
                            FLUSH_TIMEOUT,
                            Self::flush_internal(
                                instance.deref(),
                                client_actor_for_callback,
                                forwarder_mesh_for_callback,
                            ),
                        )
                        .await
                    {
                        Ok(Ok(())) => {
                            tracing::debug!("flush completed successfully during shutdown");
                        }
                        Ok(Err(e)) => {
                            tracing::error!("error during flush: {}", e);
                        }
                        Err(_) => {
                            tracing::error!(
                                "flush timed out after {} seconds during shutdown",
                                FLUSH_TIMEOUT.as_secs()
                            );
                        }
                    }
                })
                .await?;

            Ok(Self {
                forwarder_mesh,
                logger_mesh,
                client_actor,
            })
        })
    }

    fn set_mode<'py>(
        &self,
        _py: Python<'py>,
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

        let forwarder_inner_mesh = self.forwarder_mesh.borrow().map_err(anyhow::Error::msg)?;

        let mailbox = forwarder_inner_mesh.proc_mesh().client();
        forwarder_inner_mesh
            .cast(
                mailbox,
                Selection::True,
                LogForwardMessage::SetMode { stream_to_client },
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let logger_inner_mesh = self.logger_mesh.borrow().map_err(anyhow::Error::msg)?;
        logger_inner_mesh
            .cast(
                mailbox,
                Selection::True,
                LoggerRuntimeMessage::SetLogging { level },
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        self.client_actor
            .send(
                instance.deref(),
                LogClientMessage::SetAggregate {
                    aggregate_window_sec,
                },
            )
            .map_err(anyhow::Error::msg)?;

        Ok(())
    }

    // A sync flush mechanism for the client make sure all the stdout/stderr are streamed back and flushed.
    fn flush(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        let forwarder_mesh = self.forwarder_mesh.clone();
        let client_actor = self.client_actor.clone();
        let instance_for_task = instance.clone();

        PyPythonTask::new(async move {
            Self::flush_internal(instance_for_task.deref(), client_actor, forwarder_mesh)
                .await
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
