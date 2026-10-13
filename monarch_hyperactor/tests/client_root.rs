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

use std::time::Duration;

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
use monarch_hyperactor::handle::PyHandle;
use monarch_hyperactor::host_mesh::python_client_root;
use monarch_hyperactor::runtime::GilSite;
use monarch_hyperactor::runtime::get_tokio_runtime;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use pyo3::PyTypeInfo;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

const WAIT: Duration = Duration::from_secs(30);

#[test]
#[cfg_attr(not(fbcode_build), ignore)]
fn python_bootstrap_seeds_client_root_on_local_proc_agent() -> Result<()> {
    pyo3::Python::initialize();

    let test_result =
        assert_bootstrap_rejects_invalid_via().and_then(|()| exercise_python_bootstrap_contract());
    let shutdown_result = shutdown_python_host_if_present();

    match (test_result, shutdown_result) {
        (Ok(()), Ok(true)) => Ok(()),
        (Ok(()), Ok(false)) => anyhow::bail!("client-root contract completed without a host"),
        (Err(test_error), Ok(_)) => Err(test_error),
        (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
        (Err(test_error), Err(shutdown_error)) => anyhow::bail!(
            "client-root contract failed: {test_error:#}; host shutdown also failed: {shutdown_error:#}"
        ),
    }
}

fn assert_bootstrap_rejects_invalid_via() -> Result<()> {
    anyhow::ensure!(
        python_client_root()?.is_none(),
        "the Python client root must be absent before bootstrap",
    );
    assert_shutdown_without_host_is_runtime_error()?;

    monarch_with_gil_blocking(GilSite::Test, |py| -> Result<()> {
        py.run(c_str!("import monarch._rust_bindings"), None, None)?;
        let host_mesh_mod = py.import("monarch._rust_bindings.monarch_hyperactor.host_mesh")?;
        let host_mesh_src = py.import("monarch._src.actor.host_mesh")?;
        let cmd = host_mesh_src.getattr("default_bootstrap_cmd")?.call0()?;

        let error = match host_mesh_mod
            .getattr("bootstrap_host")?
            .call1((&cmd, "invalid://scheme"))
        {
            Ok(_) => anyhow::bail!("an invalid via address bootstrapped a host"),
            Err(error) => error,
        };
        anyhow::ensure!(
            error.get_type(py).is(PyValueError::type_object(py)),
            "invalid via must raise exactly ValueError, got {error}",
        );
        let message = error.value(py).to_string();
        anyhow::ensure!(
            message.contains("via address")
                && message.contains("unsupported ZMQ scheme")
                && message.contains("invalid"),
            "invalid via must identify the address, unsupported scheme, and supplied scheme: {message}",
        );
        Ok(())
    })?;

    anyhow::ensure!(
        python_client_root()?.is_none(),
        "a rejected bootstrap call must not register a client root",
    );
    assert_shutdown_without_host_is_runtime_error()
}

fn assert_shutdown_without_host_is_runtime_error() -> Result<()> {
    monarch_with_gil_blocking(GilSite::Test, |py| -> Result<()> {
        py.run(c_str!("import monarch._rust_bindings"), None, None)?;
        let host_mesh_mod = py.import("monarch._rust_bindings.monarch_hyperactor.host_mesh")?;
        let error = match host_mesh_mod.getattr("shutdown_local_host_mesh")?.call0() {
            Ok(_) => anyhow::bail!("shutdown without a host returned a Handle"),
            Err(error) => error,
        };
        anyhow::ensure!(
            is_no_local_host_error(py, &error),
            "shutdown without a host must raise exactly RuntimeError and identify the missing local host mesh: {error}",
        );
        Ok(())
    })
}

fn is_no_local_host_error(py: Python<'_>, error: &PyErr) -> bool {
    if !error.get_type(py).is(PyRuntimeError::type_object(py)) {
        return false;
    }
    let normalized = error.value(py).to_string().to_ascii_lowercase();
    normalized.contains("no local host mesh") && normalized.contains("shutdown")
}

fn exercise_python_bootstrap_contract() -> Result<()> {
    // Run the production Python host bootstrap and recover the native instance
    // of Python's RootClientActor (the program's singleton root orchestration actor).
    // Read the restricted root ProcAgent reference from that actor's environment
    // and record which proc hosts the actor.
    let (root_proc_agent_capability, root_actor_proc_id) =
        monarch_with_gil_blocking(GilSite::Test, |py| {
            py.run(c_str!("import monarch._rust_bindings"), None, None)?;

            let host_mesh_mod = py.import("monarch._rust_bindings.monarch_hyperactor.host_mesh")?;
            let host_mesh_src = py.import("monarch._src.actor.host_mesh")?;
            let cmd = host_mesh_src.getattr("default_bootstrap_cmd")?.call0()?;

            // The native binding waits for bootstrap and returns
            // (host_mesh, proc_mesh, root_actor_instance) directly.
            let bootstrap_result = host_mesh_mod.getattr("bootstrap_host")?.call1((cmd,))?;
            let bootstrap_result = bootstrap_result.cast::<PyTuple>()?;
            let root_actor_obj = bootstrap_result.get_item(2)?;
            let root_actor = root_actor_obj.cast::<PyInstance>()?.borrow();
            let root_actor_instance: &Instance<PythonActor> = &root_actor;

            // This is the contract under test: the root actor is not the root
            // ProcAgent. Its persistent environment must contain the restricted
            // capability that bootstrap_host bound to that separate ProcAgent.
            let root_actor_environment = root_actor_instance.actor_environment();
            let root_proc_agent_capability = ClientRootRef::from_env(root_actor_environment)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let root_actor_proc_id = root_actor_instance.self_addr().id().proc_id().clone();
            Ok::<_, PyErr>((root_proc_agent_capability, root_actor_proc_id))
        })?;

    // Exercise the capability from an unrelated caller context. This caller is
    // only a mailbox and identity; it neither owns nor places the service.
    let (service, caller_proc_id) =
        ensure_root_service_with_timeout(&root_proc_agent_capability, "client_root_test")?;

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

async fn ensure_root_service(
    root_proc_agent_capability: &ClientRootRef,
    caller_name: &str,
) -> Result<(hyperactor::ActorRef<TestActor>, ProcId)> {
    // Proc::direct registers network I/O, so construct it inside the runtime
    // that drives the request.
    let test_proc = Proc::direct(ChannelTransport::Unix.any(), format!("{caller_name}_proc"))?;
    let caller = test_proc.client(caller_name);
    let caller_proc_id = caller.self_addr().id().proc_id().clone();

    // This is only a local descriptor. `ensure` performs the request, creating
    // or reusing the named root-owned service.
    let service_descriptor =
        ClientRootService::<TestActor>::declare("client-root-integration-test");
    let service_ref = service_descriptor
        .ensure(&caller, root_proc_agent_capability, ())
        .await?;
    Ok((service_ref, caller_proc_id))
}

fn ensure_root_service_with_timeout(
    root_proc_agent_capability: &ClientRootRef,
    caller_name: &str,
) -> Result<(hyperactor::ActorRef<TestActor>, ProcId)> {
    get_tokio_runtime().block_on(async {
        tokio::time::timeout(
            WAIT,
            ensure_root_service(root_proc_agent_capability, caller_name),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out ensuring root service for {caller_name}"))?
    })
}

fn shutdown_python_host_if_present() -> Result<bool> {
    // Always exercise the public shutdown path after the contract body returns,
    // including when a lookup, ensure, or ownership check fails.
    let completion = monarch_with_gil_blocking(GilSite::Test, |py| -> Result<Option<_>> {
        let host_mesh_mod = py.import("monarch._rust_bindings.monarch_hyperactor.host_mesh")?;
        let handle = match host_mesh_mod.getattr("shutdown_local_host_mesh")?.call0() {
            Ok(handle) => handle,
            Err(error) if is_no_local_host_error(py, &error) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let handle = handle.cast::<PyHandle>().map_err(PyErr::from)?;
        Ok(Some(handle.borrow().wait_future()))
    })?;
    let Some(completion) = completion else {
        return Ok(false);
    };
    let result = get_tokio_runtime()
        .block_on(async { tokio::time::timeout(WAIT, completion).await })
        .map_err(|_| anyhow::anyhow!("timed out shutting down the Python host"))??;
    monarch_with_gil_blocking(GilSite::Test, |py| -> Result<()> {
        anyhow::ensure!(result.is_none(py), "host shutdown must resolve to None");
        Ok(())
    })?;

    Ok(true)
}
