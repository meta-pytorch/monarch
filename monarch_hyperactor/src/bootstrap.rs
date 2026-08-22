/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use futures::future::try_join_all;
use hyperactor::Gateway;
use hyperactor::Location;
use hyperactor::ProcAddr;
use hyperactor::ProcId;
use hyperactor::channel::ChannelAddr;
use hyperactor::id::Label;
use hyperactor_mesh::bootstrap::BootstrapCommand;
use hyperactor_mesh::bootstrap::bootstrap;
use hyperactor_mesh::bootstrap::halt;
use hyperactor_mesh::bootstrap::host;
use hyperactor_mesh::host_mesh::HostMesh;
use hyperactor_mesh::mesh_id::HostMeshId;
use monarch_types::MapPyErr;
use pyo3::Bound;
use pyo3::PyAny;
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
use crate::pytokio::PyPythonTask;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil;

const SERVICE_ADDRESS_FORMAT: &str = "expected a channel URL such as 'tcp://host:4444' or a ProcAddr such as 'service<2MuAHeDjLCEd>@tcp://host:4444'";

fn legacy_service_proc_id() -> ProcId {
    ProcId::singleton(Label::strip(hyperactor::proc::LEGACY_SERVICE_PROC_NAME))
}

fn service_address_parse_error(address: &str, error: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("invalid worker address {address:?}: {error}; {SERVICE_ADDRESS_FORMAT}")
}

fn service_proc_id_and_location(address: &str) -> (ProcId, &str) {
    // TODO: Remove this manual parse by either avoiding reserved-port addresses
    // here or making listener-preserving ProcAddr parsing a first-class API.
    if let Some((proc_id, location)) = address.split_once('@')
        && let Ok(proc_id) = proc_id.parse::<ProcId>()
    {
        return (proc_id, location);
    }

    (legacy_service_proc_id(), address)
}

fn into_dial_location(location: Location) -> Location {
    match location {
        Location::Addr(addr) => addr.into_dial_addr().into(),
        Location::Via(uid, inner) => into_dial_location(*inner).with_via(uid),
    }
}

fn parse_service_proc_addr(address: &str) -> anyhow::Result<ProcAddr> {
    let proc_addr = match address.parse::<ProcAddr>() {
        Ok(proc_addr) => proc_addr,
        Err(_) => address
            .parse::<Location>()
            .map(|location| ProcAddr::new(legacy_service_proc_id(), location))
            .map_err(|error| service_address_parse_error(address, error))?,
    };

    Ok(ProcAddr::new(
        proc_addr.id().clone(),
        into_dial_location(proc_addr.location().clone()),
    ))
}

/// Parses a service address without discarding serve-only resources.
///
/// [`ProcAddr`] contains the parsed [`ChannelAddr`], but it cannot retain the
/// [`std::net::TcpListener`] adopted from an `fdNNN` address. Parsing the
/// channel URL here preserves that listener for the host. We also cannot use
/// [`ProcAddr::addr`] because it peels [`Location::Via`], which is not a valid
/// frontend bind specification.
fn parse_service_proc_addr_for_serve(
    address: &str,
) -> anyhow::Result<(ProcAddr, Option<std::net::TcpListener>)> {
    let (proc_id, location) = service_proc_id_and_location(address);
    let (addr, listener) = ChannelAddr::from_zmq_url_with_listener(location)
        .map_err(|error| service_address_parse_error(address, error))?;
    Ok((ProcAddr::new(proc_id, addr.into()), listener))
}

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
pub fn run_worker_loop_forever(_py: Python<'_>, address: &str) -> PyResult<PyPythonTask> {
    let (service_proc_addr, listener) = parse_service_proc_addr_for_serve(address)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
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
            service_proc_addr,
            command,
            None,
            true,
            listener,
            Gateway::new(),
            None,
        )
        .await
        .map_pyerr()?;
        shutdown.stop_and_join().await;
        halt::<()>().await;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (instance, workers, name=None))]
pub fn attach_to_workers(
    instance: &crate::context::PyInstance,
    workers: Vec<Bound<'_, PyPythonTask>>,
    name: Option<&str>,
) -> PyResult<PyPythonTask> {
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

        let service_procs: Result<Vec<ProcAddr>, anyhow::Error> =
            monarch_with_gil(GilSite::Bootstrap, |py| {
                results
                    .into_iter()
                    .map(|result| {
                        let address: String = result.bind(py).extract()?;
                        parse_service_proc_addr(&address)
                    })
                    .collect()
            })
            .await;
        let service_procs = service_procs?;

        let host_mesh = HostMesh::attach_with_service_procs(&*instance, name, service_procs)
            .await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bare_worker_address_uses_legacy_service_proc() {
        let service_proc = parse_service_proc_addr("tcp://127.0.0.1:1234").unwrap();

        assert_eq!(service_proc.id(), &legacy_service_proc_id());
        assert_eq!(service_proc.location().to_string(), "tcp://127.0.0.1:1234");
    }

    #[test]
    fn test_proc_worker_address_preserves_service_identity() {
        let service_proc_id = ProcId::instance(Label::strip("service"));
        let service_proc = ProcAddr::new(
            service_proc_id.clone(),
            ChannelAddr::from_zmq_url("tcp://127.0.0.1:1234")
                .unwrap()
                .into(),
        );

        let parsed = parse_service_proc_addr(&service_proc.to_string()).unwrap();

        assert_eq!(parsed, service_proc);
    }

    #[test]
    fn test_worker_address_disambiguates_proc_addr_from_channel_alias() {
        let alias = "tcp://127.0.0.1:1234@tcp://0.0.0.0:1234";
        let dial_addr = ChannelAddr::from_zmq_url("tcp://127.0.0.1:1234").unwrap();
        let legacy = parse_service_proc_addr(alias).unwrap();
        assert_eq!(legacy.id(), &legacy_service_proc_id());
        assert_eq!(legacy.addr(), &dial_addr);

        let service_proc_id = ProcId::instance(Label::strip("service"));
        let proc_address = format!("{service_proc_id}@{alias}");
        let parsed = parse_service_proc_addr(&proc_address).unwrap();
        assert_eq!(parsed.id(), &service_proc_id);
        assert_eq!(parsed.addr(), &dial_addr);
    }

    #[test]
    fn test_proc_worker_address_preserves_via_and_canonicalizes_alias() {
        let service_proc_id = ProcId::instance(Label::strip("service"));
        let via_uid = hyperactor::Uid::Instance(0x71a, Some(Label::strip("via")));
        let alias = ChannelAddr::from_zmq_url("tcp://127.0.0.1:1234@tcp://0.0.0.0:1234").unwrap();
        let service_proc = ProcAddr::new(
            service_proc_id.clone(),
            Location::from(alias).with_via(via_uid.clone()),
        );

        let parsed = parse_service_proc_addr(&service_proc.to_string()).unwrap();
        let (parsed_via, inner) = parsed.location().as_via().unwrap();

        assert_eq!(parsed.id(), &service_proc_id);
        assert_eq!(parsed_via, &via_uid);
        assert_eq!(inner.addr().to_zmq_url(), "tcp://127.0.0.1:1234");
    }

    #[test]
    fn test_explicit_singleton_service_proc_is_accepted() {
        let service_proc = parse_service_proc_addr("service@tcp://127.0.0.1:1234").unwrap();

        assert_eq!(service_proc.id(), &legacy_service_proc_id());
        assert_eq!(service_proc.location().to_string(), "tcp://127.0.0.1:1234");
    }

    #[test]
    fn test_serve_worker_address_accepts_explicit_singleton() {
        let (service_proc, listener) =
            parse_service_proc_addr_for_serve("service@tcp://127.0.0.1:1234").unwrap();

        assert_eq!(service_proc.id(), &legacy_service_proc_id());
        assert_eq!(service_proc.location().to_string(), "tcp://127.0.0.1:1234");
        assert!(listener.is_none());
    }

    #[test]
    fn test_invalid_worker_address_error_includes_valid_formats() {
        let error = parse_service_proc_addr("not an address").unwrap_err();
        let message = error.to_string();
        assert!(message.contains("invalid worker address \"not an address\""));
        assert!(message.contains("tcp://host:4444"));
        assert!(message.contains("service<2MuAHeDjLCEd>@tcp://host:4444"));
    }
}
