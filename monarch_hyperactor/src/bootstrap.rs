/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use futures::future::try_join_all;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use hyperactor_mesh::Bootstrap;
use hyperactor_mesh::bootstrap::BootstrapCommand;
use hyperactor_mesh::bootstrap_or_die;
use hyperactor_mesh::v1::HostMeshRef;
use hyperactor_mesh::v1::Name;
use hyperactor_mesh::v1::host_mesh::HostMesh;
use monarch_types::MapPyErr;
use pyo3::Bound;
use pyo3::PyAny;
use pyo3::PyResult;
use pyo3::Python;
use pyo3::pyclass;
use pyo3::pyfunction;
use pyo3::pymethods;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::types::PyModuleMethods;
use pyo3::wrap_pyfunction;

use crate::pytokio::PyPythonTask;
use crate::runtime::get_tokio_runtime;
use crate::runtime::signal_safe_block_on;
use crate::v1::host_mesh::PyHostMesh;

#[pyfunction]
#[pyo3(signature = ())]
pub fn bootstrap_main(py: Python) -> PyResult<Bound<PyAny>> {
    // SAFETY: this is a correct use of this function.
    let _ = unsafe {
        fbinit::perform_init();
    };

    hyperactor::tracing::debug!("entering async bootstrap");
    crate::runtime::future_into_py::<_, ()>(py, async move {
        // SAFETY:
        // - Only one of these is ever created.
        // - This is the entry point of this program, so this will be dropped when
        // no more FB C++ code is running.
        let _destroy_guard = unsafe { fbinit::DestroyGuard::new() };
        bootstrap_or_die().await;
    })
}

#[pyfunction]
pub fn run_worker_loop_forever(_py: Python<'_>, address: &str) -> PyResult<PyPythonTask> {
    let addr = ChannelAddr::from_zmq_url(address)?;

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
        // For regular Python builds: use current_exe() and -m arguments
        let current_exe = std::env::current_exe().map_err(|e| {
            pyo3::PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to get current executable: {}",
                e
            ))
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

    let boot = Bootstrap::Host {
        addr,
        config: None,
        command,
    };

    PyPythonTask::new(async {
        let err = boot.bootstrap().await;
        Err(err).map_pyerr()?;
        Ok(())
    })
}

#[pyfunction]
pub fn attach_to_workers<'py>(
    workers: Vec<Bound<'py, PyPythonTask>>,
    name: Option<&str>,
) -> PyResult<PyPythonTask> {
    let tasks = workers
        .into_iter()
        .map(|x| x.borrow_mut().take_task())
        .collect::<PyResult<Vec<_>>>()?;

    let name = Name::new(name.unwrap_or("hosts"));
    PyPythonTask::new(async move {
        let results = try_join_all(tasks).await?;

        let addresses: Result<Vec<ChannelAddr>, anyhow::Error> = Python::with_gil(|py| {
            results
                .into_iter()
                .map(|result| {
                    let url_str: String = result.bind(py).extract()?;
                    ChannelAddr::from_zmq_url(&url_str)
                })
                .collect()
        });
        let addresses = addresses?;

        let host_mesh = HostMesh::take(HostMeshRef::from_hosts(name, addresses));
        Ok(PyHostMesh::new_owned(host_mesh))
    })
}

/// Python wrapper for ChannelTx that sends bytes messages.
#[pyclass(
    name = "ChannelTx",
    module = "monarch._rust_bindings.monarch_hyperactor.bootstrap"
)]
pub struct PyChannelTx {
    inner: std::sync::Arc<channel::ChannelTx<Vec<u8>>>,
}

#[pymethods]
impl PyChannelTx {
    /// Send a message (bytes) on the channel. Returns a PyPythonTask that completes when the message has been delivered.
    fn send(&self, message: &[u8]) -> PyResult<PyPythonTask> {
        let inner = self.inner.clone();
        let message = message.to_vec();

        PyPythonTask::new(async move {
            inner
                .send(message)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e.0))?;
            Ok(())
        })
    }
}

/// Python wrapper for ChannelRx that receives bytes messages.
#[pyclass(
    name = "ChannelRx",
    module = "monarch._rust_bindings.monarch_hyperactor.bootstrap"
)]
pub struct PyChannelRx {
    inner: std::sync::Arc<tokio::sync::Mutex<channel::ChannelRx<Vec<u8>>>>,
}

#[pymethods]
impl PyChannelRx {
    /// Receive the next message (bytes) from the channel. Returns a PyPythonTask that completes with the message bytes.
    fn recv(&self) -> PyResult<PyPythonTask> {
        let inner = self.inner.clone();

        PyPythonTask::new(async move {
            let mut rx = inner.lock().await;
            let message = rx
                .recv()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to receive message: {}", e))?;
            Ok(message)
        })
    }
}

/// Dial a channel address and return a transmitter for sending bytes.
#[pyfunction]
pub fn dial(address: &str) -> PyResult<PyChannelTx> {
    let addr = ChannelAddr::from_zmq_url(address)?;
    let tx = channel::dial::<Vec<u8>>(addr).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to dial channel: {}", e))
    })?;
    Ok(PyChannelTx {
        inner: std::sync::Arc::new(tx),
    })
}

/// Serve on a channel address and return a tuple of (address, receiver) for receiving bytes.
#[pyfunction]
pub fn serve(py: Python<'_>, address: &str) -> PyResult<PyChannelRx> {
    let addr = ChannelAddr::from_zmq_url(address)?;

    let rx = signal_safe_block_on(py, async move {
        let (_, rx) = channel::serve::<Vec<u8>>(addr).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to serve channel: {}", e))
        })?;
        Ok::<channel::ChannelRx<Vec<u8>>, pyo3::PyErr>(rx)
    })??;
    Ok(PyChannelRx {
        inner: std::sync::Arc::new(tokio::sync::Mutex::new(rx)),
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

    let f = wrap_pyfunction!(dial, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.bootstrap",
    )?;
    hyperactor_mod.add_function(f)?;

    let f = wrap_pyfunction!(serve, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.bootstrap",
    )?;
    hyperactor_mod.add_function(f)?;

    hyperactor_mod.add_class::<PyChannelTx>()?;
    hyperactor_mod.add_class::<PyChannelRx>()?;

    Ok(())
}
