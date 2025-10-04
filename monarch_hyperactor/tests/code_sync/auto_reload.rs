/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use anyhow::Result;
use anyhow::anyhow;
use hyperactor::channel::ChannelTransport;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::local::LocalAllocator;
use hyperactor_mesh::mesh::Mesh;
use hyperactor_mesh::proc_mesh::ProcMesh;
use monarch_hyperactor::code_sync::auto_reload::AutoReloadActor;
use monarch_hyperactor::code_sync::auto_reload::AutoReloadMessage;
use monarch_hyperactor::code_sync::auto_reload::AutoReloadParams;
use ndslice::extent;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use tempfile::TempDir;
use tokio::fs;

#[tokio::test]
async fn test_auto_reload_actor() -> Result<()> {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| py.run(c_str!("import monarch._rust_bindings"), None, None))?;

    // Create a temporary directory for Python files
    let temp_dir = TempDir::new()?;
    let py_file_path = temp_dir.path().join("test_module.py");

    // Create initial Python file content
    let initial_content = r#"
# Test module for auto-reload
def get_value():
    return "initial_value"

CONSTANT = "initial_constant"
"#;
    fs::write(&py_file_path, initial_content).await?;

    // Set up a single AutoReloadActor
    let alloc = LocalAllocator
        .allocate(AllocSpec {
            extent: extent! { replica = 1 },
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Local,
        })
        .await?;

    let proc_mesh = ProcMesh::allocate(alloc).await?;
    let params = AutoReloadParams {};
    let actor_mesh = proc_mesh
        .spawn::<AutoReloadActor>("auto_reload_test", &params)
        .await?;

    // Get a reference to the single actor
    let actor_ref = actor_mesh
        .get(0)
        .ok_or_else(|| anyhow!("No actor at index 0"))?;
    let mailbox = actor_mesh.proc_mesh().client();

    // First, we need to import the module to get it tracked by the AutoReloader
    // We'll do this by running Python code that imports our test module
    let temp_path = temp_dir.path().to_path_buf();
    let import_result = tokio::task::spawn_blocking({
        move || {
            Python::with_gil(|py| -> PyResult<String> {
                // Add the temp directory to Python path
                let sys = py.import("sys")?;
                let path = sys.getattr("path")?;
                let path_list = path.downcast::<pyo3::types::PyList>()?;
                path_list.insert(0, temp_path.to_string_lossy())?;

                // Import the test module
                let test_module = py.import("test_module")?;
                let get_value_func = test_module.getattr("get_value")?;
                let initial_value: String = get_value_func.call0()?.extract()?;

                Ok(initial_value)
            })
        }
    })
    .await??;

    // Verify we got the initial value
    assert_eq!(import_result, "initial_value");
    println!("Initial import successful, got: {}", import_result);

    // Now modify the Python file
    let modified_content = r#"
# Test module for auto-reload (MODIFIED)
def get_value():
    return "modified_value"

CONSTANT = "modified_constant"
"#;
    fs::write(&py_file_path, modified_content).await?;
    println!("Modified Python file");

    // Send AutoReloadMessage to trigger reload
    let (result_tx, mut result_rx) = mailbox.open_port::<Result<(), String>>();
    actor_ref.send(
        &mailbox,
        AutoReloadMessage {
            result: result_tx.bind(),
        },
    )?;

    // Wait for reload to complete
    let reload_result = result_rx.recv().await?;
    reload_result.map_err(|e| anyhow!("Reload failed: {}", e))?;
    println!("Auto-reload completed successfully");

    // Now import the module again and verify the changes were propagated
    let final_result = tokio::task::spawn_blocking({
        move || {
            Python::with_gil(|py| -> PyResult<String> {
                // Re-import the test module (it should be reloaded now)
                let test_module = py.import("test_module")?;
                let get_value_func = test_module.getattr("get_value")?;
                let final_value: String = get_value_func.call0()?.extract()?;

                Ok(final_value)
            })
        }
    })
    .await??;

    // Verify that the changes were propagated
    assert_eq!(final_result, "modified_value");
    println!("Final import successful, got: {}", final_result);

    // Verify that the module was actually reloaded by checking if we get the new value
    assert_ne!(import_result, final_result);
    println!("Auto-reload test completed successfully - module was reloaded!");

    Ok(())
}
