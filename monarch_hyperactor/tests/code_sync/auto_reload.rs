/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use anyhow::Result;
use anyhow::anyhow;
use hyperactor::context::Mailbox;
use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::global_root_client;
use hyperactor_mesh::test_utils;
use monarch_hyperactor::code_sync::auto_reload::AutoReloadActor;
use monarch_hyperactor::code_sync::auto_reload::AutoReloadMessage;
use monarch_hyperactor::code_sync::auto_reload::AutoReloadParams;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use ndslice::View;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use tempfile::TempDir;
use tokio::fs;

#[tokio::test]
// TODO: OSS: ModuleNotFoundError: No module named 'monarch'
#[cfg_attr(not(fbcode_build), ignore)]
async fn test_auto_reload_actor() -> Result<()> {
    pyo3::Python::initialize();
    monarch_with_gil_blocking(|py| py.run(c_str!("import monarch._rust_bindings"), None, None))?;

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

    let instance = global_root_client();
    let mut host_mesh = test_utils::local_host_mesh(1).await;
    let proc_mesh = host_mesh
        .spawn(instance, "auto_reload_test", ndslice::Extent::unity())
        .await
        .unwrap();
    let params = AutoReloadParams {};
    let actor_mesh: ActorMesh<AutoReloadActor> = proc_mesh
        .spawn(instance, "auto_reload_test", &params)
        .await?;

    // Get a reference to the single actor
    let actor_ref = actor_mesh
        .get(0)
        .ok_or_else(|| anyhow!("No actor at index 0"))?;

    // First, we need to import the module to get it tracked by the AutoReloader
    // We'll do this by running Python code that imports our test module
    let temp_path = temp_dir.path().to_path_buf();
    let import_result = tokio::task::spawn_blocking({
        move || {
            monarch_with_gil_blocking(|py| -> PyResult<String> {
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
    let (result_tx, mut result_rx) = instance.mailbox().open_port::<Result<(), String>>();
    actor_ref.send(
        instance,
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
            monarch_with_gil_blocking(|py| -> PyResult<String> {
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

    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}
