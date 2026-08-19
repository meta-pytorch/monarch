/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use futures::future::try_join_all;
use hyperactor::Gateway;
use hyperactor::ProcAddr;
use hyperactor::channel::ChannelAddr;
use hyperactor::id::Label;
use hyperactor::id::Uid;
use hyperactor_mesh::bootstrap::BootstrapCommand;
use hyperactor_mesh::bootstrap::bootstrap;
use hyperactor_mesh::bootstrap::halt;
use hyperactor_mesh::bootstrap::host;
use hyperactor_mesh::host_mesh::HostMesh;
use hyperactor_mesh::mesh_id::HostMeshId;
use monarch_types::MapPyErr;
use pyo3::Bound;
use pyo3::PyAny;
use pyo3::PyRef;
use pyo3::PyResult;
use pyo3::Python;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::pyfunction;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::types::PyModuleMethods;
use pyo3::wrap_pyfunction;

use crate::host_mesh::PyHostMesh;
use crate::proc::PyProcId;
use crate::pytokio::PyPythonTask;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil;

#[pyfunction]
#[pyo3(signature = ())]
pub fn bootstrap_main(py: Python) -> PyResult<Bound<PyAny>> {
    #[cfg(fbcode_build)]
    // SAFETY: this is a correct use of this function.
    unsafe {
        fbinit::perform_init();
    };

    hyperactor::internal_macro_support::tracing::debug!("entering async bootstrap");
    crate::runtime::future_into_py::<_, i32>(py, async move {
        // SAFETY:
        // - Only one of these is ever created.
        // - This is the entry point of this program, so this will be dropped when
        // no more FB C++ code is running.
        #[cfg(fbcode_build)]
        let _destroy_guard = unsafe { fbinit::DestroyGuard::new() };
        bootstrap()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))
    })
}

#[pyfunction]
#[pyo3(signature = (address, service_proc_id=None))]
pub fn run_worker_loop_forever(
    _py: Python<'_>,
    address: &str,
    service_proc_id: Option<&PyProcId>,
) -> PyResult<PyPythonTask> {
    let (addr, listener) = ChannelAddr::from_zmq_url_with_listener(address)?;
    let service_proc_id = service_proc_id.map(|proc_id| proc_id.inner.clone());
    if service_proc_id
        .as_ref()
        .is_some_and(|proc_id| !matches!(proc_id.uid(), Uid::Instance(..)))
    {
        return Err(PyValueError::new_err(
            "service_proc_id must have an instance UID",
        ));
    }
    // Check if we're running in a PAR/XAR build by looking for FB_XAR_INVOKED_NAME environment variable
    let invoked_name = std::env::var("FB_XAR_INVOKED_NAME");

    let mut env: std::collections::HashMap<String, String> = std::env::vars().collect();

    let command = Some(if let Ok(invoked_name) = invoked_name {
        // For PAR/XAR builds: use argv[0] from Python's sys.argv as the current executable
        let current_exe = std::path::PathBuf::from(&invoked_name);

        // For PAR/XAR builds: set PAR_MAIN_OVERRIDE and no additional args
        env.insert(
            "PAR_MAIN_OVERRIDE".to_string(),
            "monarch._src.actor.bootstrap_main".to_string(),
        );
        BootstrapCommand {
            program: current_exe,
            arg0: Some(invoked_name),
            args: vec![],
            env,
        }
    } else {
        // For regular Python builds: use argv[0] to preserve the original
        // invocation path.  current_exe() resolves symlinks, which breaks
        // virtual environments — the resolved path doesn't find pyvenv.cfg
        // so site-packages aren't activated in subprocesses.
        let current_exe = std::env::args()
            .next()
            .map(std::path::PathBuf::from)
            .or_else(|| std::env::current_exe().ok())
            .ok_or_else(|| {
                pyo3::PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Failed to determine current executable",
                )
            })?;
        let current_exe_str = current_exe.to_string_lossy().to_string();
        BootstrapCommand {
            program: current_exe,
            arg0: Some(current_exe_str),
            args: vec![
                "-m".to_string(),
                "monarch._src.actor.bootstrap_main".to_string(),
            ],
            env,
        }
    });

    PyPythonTask::new(async move {
        let (_agent_handle, shutdown) = host(
            addr,
            command,
            None,
            true,
            listener,
            Gateway::new(),
            None,
            service_proc_id,
        )
        .await
        .map_pyerr()?;
        shutdown.stop_and_join().await;
        halt::<()>().await;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (instance, workers, name=None, service_proc_ids=None))]
pub fn attach_to_workers(
    instance: &crate::context::PyInstance,
    workers: Vec<Bound<'_, PyPythonTask>>,
    name: Option<&str>,
    service_proc_ids: Option<Vec<PyRef<'_, PyProcId>>>,
) -> PyResult<PyPythonTask> {
    let service_proc_ids = service_proc_ids.map(|proc_ids| {
        proc_ids
            .into_iter()
            .map(|proc_id| proc_id.inner.clone())
            .collect::<Vec<_>>()
    });
    let worker_count = workers.len();
    if let Some(service_proc_ids) = &service_proc_ids
        && service_proc_ids.len() != worker_count
    {
        return Err(PyValueError::new_err(format!(
            "worker/service proc id count mismatch: {worker_count} workers, {} ids",
            service_proc_ids.len()
        )));
    }
    if service_proc_ids.as_ref().is_some_and(|proc_ids| {
        proc_ids
            .iter()
            .any(|proc_id| !matches!(proc_id.uid(), Uid::Instance(..)))
    }) {
        return Err(PyValueError::new_err(
            "service_proc_ids must all have instance UIDs",
        ));
    }
    let tasks = workers
        .into_iter()
        .map(|x| x.borrow_mut().take_task())
        .collect::<PyResult<Vec<_>>>()?;

    // `Label::strip` (vs. `Label::new`) sanitizes user-supplied names — lowercases,
    // drops illegal characters, falls back to "nil" if empty. Callers pass names
    // derived from experiment / job names that may contain uppercase or punctuation;
    // rejecting them surfaces as an opaque PyException far from the input site.
    let name = HostMeshId::instance(Label::strip(name.unwrap_or("hosts")));
    let instance = instance.clone();
    PyPythonTask::new(async move {
        let results = try_join_all(tasks).await?;

        let addresses: Result<Vec<ChannelAddr>, anyhow::Error> =
            monarch_with_gil(GilSite::Bootstrap, |py| {
                results
                    .into_iter()
                    .map(|result| {
                        let url_str: String = result.bind(py).extract()?;
                        Ok(ChannelAddr::from_zmq_url(&url_str)?.into_dial_addr())
                    })
                    .collect()
            })
            .await;
        let addresses = addresses?;

        let host_mesh = match service_proc_ids {
            Some(service_proc_ids) => {
                let service_procs = addresses
                    .into_iter()
                    .zip(service_proc_ids)
                    .map(|(address, proc_id)| ProcAddr::new(proc_id, address.into()))
                    .collect();
                HostMesh::attach_with_service_procs(&*instance, name, service_procs).await
            }
            None => HostMesh::attach(&*instance, name, addresses).await,
        }
        .map_err(|e| anyhow::anyhow!("attach failed: {}", e))?;
        Ok(PyHostMesh::new_owned(host_mesh))
    })
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(bootstrap_main, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.bootstrap",
    )?;
    hyperactor_mod.add_function(f)?;

    let f = wrap_pyfunction!(run_worker_loop_forever, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.bootstrap",
    )?;
    hyperactor_mod.add_function(f)?;

    let f = wrap_pyfunction!(attach_to_workers, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.bootstrap",
    )?;
    hyperactor_mod.add_function(f)?;

    Ok(())
}
