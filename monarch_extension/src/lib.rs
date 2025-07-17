/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

#[cfg(feature = "tensor_engine")]
mod client;
pub mod code_sync;
#[cfg(feature = "tensor_engine")]
mod controller;
#[cfg(feature = "tensor_engine")]
pub mod convert;
#[cfg(feature = "tensor_engine")]
mod debugger;
mod logging;
#[cfg(feature = "tensor_engine")]
mod mesh_controller;
mod simulation_tools;
mod simulator_client;
#[cfg(feature = "tensor_engine")]
mod tensor_worker;

mod blocking;
mod panic;
use pyo3::prelude::*;

#[pyfunction]
fn has_tensor_engine() -> bool {
    cfg!(feature = "tensor_engine")
}

fn get_or_add_new_module<'py>(
    module: &Bound<'py, PyModule>,
    module_name: &str,
) -> PyResult<Bound<'py, pyo3::types::PyModule>> {
    let mut current_module = module.clone();
    let mut parts = Vec::new();
    for part in module_name.split(".") {
        parts.push(part);
        let submodule = current_module.getattr(part).ok();
        if let Some(submodule) = submodule {
            current_module = submodule.extract()?;
        } else {
            let new_module = PyModule::new(current_module.py(), part)?;
            current_module.add_submodule(&new_module)?;
            current_module
                .py()
                .import("sys")?
                .getattr("modules")?
                .set_item(
                    format!("monarch._rust_bindings.{}", parts.join(".")),
                    new_module.clone(),
                )?;
            current_module = new_module;
        }
    }
    Ok(current_module)
}

#[pymodule]
#[pyo3(name = "_rust_bindings")]
pub fn mod_init(module: &Bound<'_, PyModule>) -> PyResult<()> {
    monarch_hyperactor::runtime::initialize(module.py())?;
    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();
    ::hyperactor::initialize_with_log_prefix(
        runtime.handle().clone(),
        Some(::hyperactor_mesh::bootstrap::BOOTSTRAP_INDEX_ENV.to_string()),
    );

    monarch_hyperactor::shape::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.shape",
    )?)?;

    monarch_hyperactor::selection::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.selection",
    )?)?;

    monarch_hyperactor::supervision::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.supervision",
    )?)?;

    #[cfg(feature = "tensor_engine")]
    {
        client::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.client",
        )?)?;
        tensor_worker::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.tensor_worker",
        )?)?;
        controller::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.controller",
        )?)?;
        debugger::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.debugger",
        )?)?;
        monarch_messages::debugger::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_messages.debugger",
        )?)?;
        simulator_client::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.simulator_client",
        )?)?;
        ::controller::bootstrap::register_python_bindings(&get_or_add_new_module(
            module,
            "controller.bootstrap",
        )?)?;
        ::monarch_tensor_worker::bootstrap::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_tensor_worker.bootstrap",
        )?)?;
        crate::convert::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.convert",
        )?)?;
        crate::mesh_controller::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.mesh_controller",
        )?)?;
        monarch_rdma_extension::register_python_bindings(&get_or_add_new_module(module, "rdma")?)?;
    }
    simulation_tools::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_extension.simulation_tools",
    )?)?;
    monarch_hyperactor::bootstrap::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.bootstrap",
    )?)?;

    monarch_hyperactor::proc::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.proc",
    )?)?;

    monarch_hyperactor::actor::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.actor",
    )?)?;

    monarch_hyperactor::mailbox::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.mailbox",
    )?)?;

    monarch_hyperactor::alloc::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.alloc",
    )?)?;
    monarch_hyperactor::channel::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.channel",
    )?)?;
    monarch_hyperactor::actor_mesh::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.actor_mesh",
    )?)?;
    monarch_hyperactor::proc_mesh::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.proc_mesh",
    )?)?;

    monarch_hyperactor::runtime::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.runtime",
    )?)?;
    monarch_hyperactor::telemetry::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.telemetry",
    )?)?;
    code_sync::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_extension.code_sync",
    )?)?;

    crate::panic::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_extension.panic",
    )?)?;

    crate::blocking::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_extension.blocking",
    )?)?;

    crate::logging::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_extension.logging",
    )?)?;

    #[cfg(fbcode_build)]
    {
        monarch_hyperactor::meta::alloc::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_hyperactor.meta.alloc",
        )?)?;
        monarch_hyperactor::meta::alloc_mock::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_hyperactor.meta.alloc_mock",
        )?)?;
    }
    // Add feature detection function
    module.add_function(wrap_pyfunction!(has_tensor_engine, module)?)?;

    Ok(())
}
