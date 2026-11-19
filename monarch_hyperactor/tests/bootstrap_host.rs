/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Native `bootstrap_host` and `shutdown_local_host_mesh` failure and shutdown
//! behavior, driven through the real Python bindings.
//!
//! Shutdown without a local host raises `RuntimeError`. An invalid `via`
//! address raises `ValueError` and leaves no local host behind. A valid
//! bootstrap succeeds, and the public shutdown path always runs afterwards and
//! resolves to `None`.
//!
//! `bootstrap_host` initializes process-global state that shutdown does not
//! reset. This must remain the only test in this binary that calls it.
//!
//! Requires the real `monarch` Python package via `py_deps` on
//! `test_monarch_hyperactor`.

use std::time::Duration;

use anyhow::Result;
use monarch_hyperactor::handle::PyHandle;
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
#[ignore = "in opt builds the test binary and the Python extension each link hyperactor_mesh, so actor types register twice and the host agent panics"]
fn python_bootstrap_host_rejects_invalid_via_and_shuts_down() -> Result<()> {
    pyo3::Python::initialize();

    let test_result = assert_bootstrap_rejects_invalid_via().and_then(|()| bootstrap_python_host());
    let shutdown_result = shutdown_python_host_if_present();

    match (test_result, shutdown_result) {
        (Ok(()), Ok(true)) => Ok(()),
        (Ok(()), Ok(false)) => anyhow::bail!("bootstrap completed without a host"),
        (Err(test_error), Ok(_)) => Err(test_error),
        (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
        (Err(test_error), Err(shutdown_error)) => anyhow::bail!(
            "bootstrap test failed: {test_error:#}; host shutdown also failed: {shutdown_error:#}"
        ),
    }
}

fn assert_bootstrap_rejects_invalid_via() -> Result<()> {
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

    // A rejected bootstrap must leave no local host behind.
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

fn bootstrap_python_host() -> Result<()> {
    monarch_with_gil_blocking(GilSite::Test, |py| -> Result<()> {
        py.run(c_str!("import monarch._rust_bindings"), None, None)?;
        let host_mesh_mod = py.import("monarch._rust_bindings.monarch_hyperactor.host_mesh")?;
        let host_mesh_src = py.import("monarch._src.actor.host_mesh")?;
        let cmd = host_mesh_src.getattr("default_bootstrap_cmd")?.call0()?;

        // The native binding waits for bootstrap and returns
        // (host_mesh, proc_mesh, root_actor_instance) directly.
        let bootstrap_result = host_mesh_mod.getattr("bootstrap_host")?.call1((cmd,))?;
        let bootstrap_result = bootstrap_result.cast::<PyTuple>().map_err(PyErr::from)?;
        anyhow::ensure!(
            bootstrap_result.len() == 3,
            "bootstrap_host must return (host_mesh, proc_mesh, root_actor_instance), got {} items",
            bootstrap_result.len(),
        );
        Ok(())
    })
}

fn shutdown_python_host_if_present() -> Result<bool> {
    // Always exercise the public shutdown path after the test body returns,
    // including when bootstrap fails.
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
