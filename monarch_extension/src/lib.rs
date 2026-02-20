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
pub mod convert;
#[cfg(feature = "tensor_engine")]
mod debugger;
#[cfg(feature = "tensor_engine")]
mod mesh_controller;
mod simulation_tools;
#[cfg(feature = "tensor_engine")]
mod tensor_worker;

mod blocking;
mod panic;
mod trace;

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
            let full_name = format!("monarch._rust_bindings.{}", parts.join("."));
            let new_module = PyModule::new(current_module.py(), &full_name)?;
            // Use setattr with short name instead of add_submodule which uses full name
            current_module.setattr(part, &new_module)?;
            current_module
                .py()
                .import("sys")?
                .getattr("modules")?
                .set_item(&full_name, new_module.clone())?;
            current_module = new_module;
        }
    }
    Ok(current_module)
}

#[pymodule]
#[pyo3(name = "_rust_bindings")]
pub fn mod_init(module: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_telemetry::trace::get_or_create_trace_id();
    monarch_hyperactor::runtime::initialize(module.py())?;
    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();
    ::hyperactor::initialize_with_log_prefix(
        runtime.handle().clone(),
        Some(::hyperactor_mesh::bootstrap::BOOTSTRAP_INDEX_ENV.to_string()),
    );
    monarch_hyperactor::buffers::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.buffers",
    )?)?;

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

    monarch_hyperactor::value_mesh::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.value_mesh",
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
        debugger::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_extension.debugger",
        )?)?;
        monarch_messages::debugger::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_messages.debugger",
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

    monarch_hyperactor::pytokio::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.pytokio",
    )?)?;

    monarch_hyperactor::pywaker::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.pywaker",
    )?)?;

    monarch_hyperactor::pympsc::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.pympsc",
    )?)?;

    monarch_hyperactor::mailbox::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.mailbox",
    )?)?;

    monarch_hyperactor::endpoint::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.endpoint",
    )?)?;

    monarch_hyperactor::context::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.context",
    )?)?;

    monarch_hyperactor::config::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.config",
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

    monarch_hyperactor::host_mesh::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.host_mesh",
    )?)?;

    monarch_hyperactor::runtime::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.runtime",
    )?)?;
    monarch_hyperactor::telemetry::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.telemetry",
    )?)?;
    monarch_hyperactor::testing::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.testing",
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

    monarch_hyperactor::logging::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.logging",
    )?)?;

    monarch_hyperactor::namespace::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.namespace",
    )?)?;

    monarch_hyperactor::proc_launcher_probe::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_hyperactor.proc_launcher_probe",
    )?)?;

    crate::trace::register_python_bindings(&get_or_add_new_module(
        module,
        "monarch_extension.trace",
    )?)?;

    #[cfg(feature = "distributed_sql_telemetry")]
    {
        monarch_distributed_telemetry::register_python_bindings(&get_or_add_new_module(
            module,
            "monarch_distributed_telemetry",
        )?)?;
        monarch_distributed_telemetry::database_scanner::register_python_bindings(
            &get_or_add_new_module(module, "monarch_distributed_telemetry.database_scanner")?,
        )?;
        monarch_distributed_telemetry::query_engine::register_python_bindings(
            &get_or_add_new_module(module, "monarch_distributed_telemetry.query_engine")?,
        )?;
    }

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
