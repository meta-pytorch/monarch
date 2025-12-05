/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Proc-related functionality that requires hyperactor_multiprocess.
//!
//! This module provides the Python-facing `init_proc` function and `world_status`
//! which need hyperactor_multiprocess for system actor integration. This is separated
//! from monarch_hyperactor to avoid that dependency in the core library.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use hyperactor::ActorRef;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::ClockKind;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxClient;
use hyperactor::proc::Proc;
use hyperactor::reference::ProcId;
use hyperactor::reference::WorldId;
use hyperactor_multiprocess::proc_actor::ProcActor;
use hyperactor_multiprocess::supervision::ProcSupervisor;
use hyperactor_multiprocess::system_actor::ProcLifecycleMode;
use hyperactor_multiprocess::system_actor::SYSTEM_ACTOR_REF;
use hyperactor_multiprocess::system_actor::SystemMessageClient;
use hyperactor_multiprocess::system_actor::SystemSnapshotFilter;
use hyperactor_multiprocess::system_actor::WorldStatus;
use monarch_hyperactor::actor::PickledMessageClientActor;
use monarch_hyperactor::proc::PyProc;
use monarch_hyperactor::runtime::signal_safe_block_on;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyModule;

/// Bootstrap a proc into the system at the provided bootstrap address.
/// The proc will report to the system actor every
/// [`supervision_update_interval_in_sec`] seconds.
async fn bootstrap_proc(
    proc_id: &str,
    bootstrap_addr: &str,
    supervision_update_interval_in_sec: u64,
    listen_addr: Option<String>,
) -> Result<PyProc> {
    let proc_id: ProcId = proc_id.parse()?;
    let bootstrap_addr: ChannelAddr = bootstrap_addr.parse()?;
    let listen_addr = if let Some(listen_addr) = listen_addr {
        listen_addr.parse()?
    } else {
        ChannelAddr::any(bootstrap_addr.transport())
    };
    let chan = channel::dial(bootstrap_addr.clone())?;
    let system_sender = BoxedMailboxSender::new(MailboxClient::new(chan));
    let proc_forwarder =
        BoxedMailboxSender::new(DialMailboxRouter::new_with_default(system_sender));
    let proc = Proc::new_with_clock(
        proc_id.clone(),
        proc_forwarder,
        ClockKind::for_channel_addr(&bootstrap_addr),
    );

    let system_supervision_ref: ActorRef<ProcSupervisor> =
        ActorRef::attest(SYSTEM_ACTOR_REF.actor_id().clone());

    let bootstrap = ProcActor::bootstrap_for_proc(
        proc.clone().clone(),
        proc.clone()
            .proc_id()
            .world_id()
            .expect("proc must be ranked for world id")
            .clone(), // REFACTOR(marius): factor out world id
        listen_addr,
        bootstrap_addr.clone(),
        system_supervision_ref,
        Duration::from_secs(supervision_update_interval_in_sec),
        HashMap::new(),
        ProcLifecycleMode::Detached,
    )
    .await
    .inspect_err(|err| {
        tracing::error!("could not spawn proc actor for {}: {}", proc.proc_id(), err,);
    })?;

    tokio::spawn(async move {
        tracing::info!(
            "proc actor for {} exited with status {}",
            proc_id,
            bootstrap.proc_actor.await
        );
    });

    Ok(PyProc::new_from_proc(proc))
}

#[pyfunction]
#[pyo3(signature = (*, proc_id, bootstrap_addr, timeout = 5, supervision_update_interval = 0, listen_addr = None))]
pub fn init_proc(
    py: Python<'_>,
    proc_id: &str,
    bootstrap_addr: &str,
    #[allow(unused_variables)] // pyo3 will complain if we name this _timeout
    timeout: u64,
    supervision_update_interval: u64,
    listen_addr: Option<String>,
) -> PyResult<PyProc> {
    // TODO: support configuring supervision_update_interval in Python binding.
    let proc_id = proc_id.to_owned();
    let bootstrap_addr = bootstrap_addr.to_owned();
    signal_safe_block_on(py, async move {
        bootstrap_proc(
            &proc_id,
            &bootstrap_addr,
            supervision_update_interval,
            listen_addr,
        )
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    })?
}

#[pyfunction]
pub fn world_status<'py>(
    py: Python<'py>,
    actor: &PickledMessageClientActor,
) -> PyResult<Bound<'py, PyDict>> {
    let instance = actor.instance_arc().clone();

    let worlds = signal_safe_block_on(py, async move {
        let instance = instance.lock().await;
        let filter: SystemSnapshotFilter = Default::default();
        let snapshot = SYSTEM_ACTOR_REF
            .snapshot(instance.instance(), filter)
            .await?;

        // TODO: pulling snapshot is expensive as it contains all proc details
        // We do not need those extra information.
        let result: HashMap<WorldId, WorldStatus> = snapshot
            .worlds
            .into_iter()
            .map(|(k, v)| (k, v.status))
            .collect();
        Ok::<_, anyhow::Error>(result)
    })
    .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))??;

    let py_dict = PyDict::new(py);
    for (world, status) in worlds {
        py_dict.set_item(world.to_string(), status.to_string())?;
    }
    Ok(py_dict)
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(init_proc, module)?;
    f.setattr("__module__", "monarch._rust_bindings.proc")?;
    module.add_function(f)?;

    let f = wrap_pyfunction!(world_status, module)?;
    f.setattr("__module__", "monarch._rust_bindings.proc")?;
    module.add_function(f)?;

    Ok(())
}
