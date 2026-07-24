/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! End-to-end proof that Python bootstrap gives the application actor hierarchy
//! access to services owned by the program root.
//!
//! Python's `RootClientActor` and the root ProcAgent are different actors on the
//! same client proc. `RootClientActor` is the program's singleton root
//! orchestration actor; the root ProcAgent owns program-level services.
//! `bootstrap_host` binds the restricted client-root API on that ProcAgent and
//! stores the resulting `ClientRootRef` in the root actor's environment, where
//! descendants can inherit it.
//!
//! This test drives that production bootstrap, reads the capability from the
//! root actor's environment, and uses it from an unrelated `Proc::client` to
//! ensure a service. The `Proc::client` is only a temporary request/reply
//! context; the service must be created on the root proc, not the caller's proc.
//!
//! Rust's `GlobalClientActor` provides the same root role. Its bootstrap path is
//! covered by `hyperactor_mesh::global_context::tests::test_bootstrap_seeds_client_root`;
//! capability inheritance and the missing-capability case are covered by
//! `hyperactor_mesh::proc_agent::tests::client_root_service_observes_inherited_capability`
//! and `hyperactor_mesh::proc_agent::tests::client_root_from_env_absent_fails_closed`.
//!
//! `bootstrap_host` initializes process-global state that shutdown does not
//! reset. This must remain the only test in this binary that calls it.
//!
//! Requires the real `monarch` Python package via `py_deps` on
//! `test_monarch_hyperactor`.

use anyhow::Result;
use hyperactor::Proc;
use hyperactor::channel::ChannelTransport;
use hyperactor::id::ProcId;
use hyperactor::proc::Instance;
use hyperactor_mesh::client_root::ClientRootRef;
use hyperactor_mesh::client_root::ClientRootService;
use hyperactor_mesh::testactor::TestActor;
use monarch_hyperactor::actor::PythonActor;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::runtime::GilSite;
use monarch_hyperactor::runtime::get_tokio_runtime;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

#[test]
#[cfg_attr(not(fbcode_build), ignore)]
fn python_bootstrap_seeds_client_root_on_local_proc_agent() -> Result<()> {
    pyo3::Python::initialize();

    let test_result = exercise_python_bootstrap_contract();
    let shutdown_result = shutdown_python_host();

    match (test_result, shutdown_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(test_error), Ok(())) => Err(test_error),
        (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
        (Err(test_error), Err(shutdown_error)) => anyhow::bail!(
            "client-root contract failed: {test_error:#}; host shutdown also failed: {shutdown_error:#}"
        ),
    }
}

fn exercise_python_bootstrap_contract() -> Result<()> {
    // Run the production Python host bootstrap and recover the native instance
    // of Python's RootClientActor (the program's singleton root orchestration actor).
    // Read the restricted root ProcAgent reference from that actor's environment
    // and record which proc hosts the actor.
    let (root_proc_agent_capability, root_actor_proc_id) =
        monarch_with_gil_blocking(GilSite::Test, |py| -> PyResult<(ClientRootRef, ProcId)> {
            py.run(c_str!("import monarch._rust_bindings"), None, None)?;

            let host_mesh_mod = py.import("monarch._rust_bindings.monarch_hyperactor.host_mesh")?;
            let host_mesh_src = py.import("monarch._src.actor.host_mesh")?;
            let cmd = host_mesh_src.getattr("default_bootstrap_cmd")?.call0()?;

            // The native binding returns a PyPythonTask holding the Rust
            // bootstrap future. block_on consumes it and drives it on Monarch's
            // embedded Tokio runtime, producing
            // (host_mesh, proc_mesh, root_actor_instance).
            let bootstrap_result = host_mesh_mod
                .getattr("bootstrap_host")?
                .call1((cmd,))?
                .call_method0("block_on")?;
            let root_actor_obj = bootstrap_result.downcast::<PyTuple>()?.get_item(2)?;
            let root_actor = root_actor_obj.downcast::<PyInstance>()?.borrow();
            let root_actor_instance: &Instance<PythonActor> = &root_actor;

            // This is the contract under test: the root actor is not the root
            // ProcAgent. Its persistent environment must contain the restricted
            // capability that bootstrap_host bound to that separate ProcAgent.
            let root_actor_environment = root_actor_instance.actor_environment();
            let root_proc_agent_capability = ClientRootRef::from_env(root_actor_environment)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let root_actor_proc_id = root_actor_instance.self_addr().id().proc_id().clone();
            Ok((root_proc_agent_capability, root_actor_proc_id))
        })?;

    // Exercise the capability from an unrelated caller context. This
    // Proc::client is just a mailbox and caller identity; it is not the Python
    // RootClientActor. Proc::direct registers network I/O, so construct it
    // inside the runtime that drives the request.
    let (service, caller_proc_id) = get_tokio_runtime().block_on(async {
        let test_proc = Proc::direct(ChannelTransport::Unix.any(), "client_root_test".to_string())?;
        let caller = test_proc.client("caller");
        let caller_proc_id = caller.self_addr().id().proc_id().clone();

        // `caller` supplies only the mailbox context for the request and reply;
        // it neither owns nor places the service. The inherited capability
        // routes the request to the root ProcAgent.
        //
        // This is only a local descriptor: a static service name paired with
        // the TestActor type. Constructing it does not spawn or contact an
        // actor. `ensure` performs the request, creating or reusing that named
        // root-owned service and returning its typed actor reference.
        let service_descriptor =
            ClientRootService::<TestActor>::declare("client-root-integration-test");
        let service_ref = service_descriptor
            .ensure(&caller, &root_proc_agent_capability, ())
            .await?;
        Ok::<_, anyhow::Error>((service_ref, caller_proc_id))
    })?;

    // The root ProcAgent and RootClientActor live on the same canonical local
    // proc. A service created through this capability must therefore land on
    // that proc, rather than on the unrelated caller's proc.
    let service_proc_id = service.actor_addr().id().proc_id();
    anyhow::ensure!(
        service_proc_id == &root_actor_proc_id,
        "root-owned service landed on {service_proc_id}, not root proc {root_actor_proc_id}",
    );
    anyhow::ensure!(
        service_proc_id != &caller_proc_id,
        "root-owned service was incorrectly created on requester proc {caller_proc_id}",
    );

    Ok(())
}

fn shutdown_python_host() -> Result<()> {
    // Always exercise the public shutdown path after the contract body returns,
    // including when a lookup, ensure, or ownership check fails.
    monarch_with_gil_blocking(GilSite::Test, |py| -> PyResult<()> {
        let host_mesh_mod = py.import("monarch._rust_bindings.monarch_hyperactor.host_mesh")?;
        host_mesh_mod
            .getattr("shutdown_local_host_mesh")?
            .call0()?
            .call_method0("block_on")?;
        Ok(())
    })?;

    Ok(())
}
