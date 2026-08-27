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
    use std::os::fd::AsFd;
    use std::os::fd::AsRawFd;
    use std::os::fd::IntoRawFd;
    use std::os::fd::OwnedFd;
    use std::os::fd::RawFd;

    use nix::errno::Errno;
    use nix::libc;
    use nix::sys::socket::AddressFamily;
    use nix::sys::socket::SockFlag;
    use nix::sys::socket::SockType;
    use nix::sys::socket::SockaddrIn;
    use nix::sys::socket::bind;
    use nix::sys::socket::getsockname;
    use nix::sys::socket::getsockopt;
    use nix::sys::socket::socket;
    use nix::sys::socket::sockopt;
    use nix::unistd::dup;
    use pyo3::PyErr;
    use pyo3::PyTypeInfo;

    use super::*;
    use crate::runtime::monarch_with_gil_blocking;

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

    /// A bound but deliberately not-yet-listening IPv4 socket, plus the address
    /// it holds. Starting from a listening socket would make the
    /// `SO_ACCEPTCONN` transition below vacuous.
    fn bound_not_listening() -> (OwnedFd, SockaddrIn) {
        let sock = socket(
            AddressFamily::Inet,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )
        .expect("test socket should be creatable");
        let wildcard = SockaddrIn::new(127, 0, 0, 1, 0);
        bind(sock.as_raw_fd(), &wildcard).expect("binding an ephemeral port should succeed");
        let bound = getsockname::<SockaddrIn>(sock.as_raw_fd()).expect("bound socket has a name");
        (sock, bound)
    }

    /// What the kernel says when a fresh socket tries to take `addr`. A live
    /// listener holds it; a released one does not. The error is preserved so a
    /// caller can require the specific refusal rather than any failure.
    fn rebind_result(addr: &SockaddrIn) -> nix::Result<()> {
        let probe = socket(
            AddressFamily::Inet,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )
        .expect("probe socket should be creatable");
        bind(probe.as_raw_fd(), addr)
    }

    /// A non-alias TCP `host:fdN` location transfers descriptor ownership while
    /// the binding is being called, and the returned task holds that listener.
    ///
    /// The test exercises two spellings of this same path, each with its own
    /// freshly bound socket: a bare channel URL using the legacy service proc id
    /// and an explicit `ProcId@location` using a generated instance id. The tls,
    /// metatls, quic, and metaquic schemes use the same `host:fdN` parser but are
    /// not exercised here. A TCP alias is different: its nested listener is
    /// discarded and closed during parsing rather than retained by the task.
    ///
    /// The address is the durable ownership witness. Rebinding is checked on
    /// both sides of the drop, because a rebind that succeeds afterwards proves
    /// nothing unless it fails while the task still owns the listener.
    #[test]
    fn run_worker_loop_forever_retains_non_alias_tcp_fd_until_drop() {
        pyo3::Python::initialize();

        for build_address in [
            (&|fd| format!("tcp://127.0.0.1:fd{fd}")) as &dyn Fn(RawFd) -> String,
            &|fd| {
                format!(
                    "{}@tcp://127.0.0.1:fd{fd}",
                    ProcId::instance(Label::strip("service"))
                )
            },
        ] {
            let (sock, bound) = bound_not_listening();
            let observer = dup(sock.as_fd()).expect("duplicating for observation should succeed");
            assert!(
                !getsockopt(&observer, sockopt::AcceptConn).expect("SO_ACCEPTCONN is readable"),
                "the fixture must hand over a bound socket that is not yet listening"
            );

            // Ownership leaves Rust here: the number is all that is passed, and
            // the binding adopts it.
            //
            // If construction were to fail instead, this fixture cannot reclaim
            // the descriptor. Whether it is still open depends on where the
            // failure happened -- a parse failure leaves it untouched, while a
            // failure after the listener is built has already closed it -- and
            // the caller cannot tell those apart. Closing it here would risk a
            // double close, which in a shared test process could take out an
            // unrelated descriptor opened concurrently. Leaking one descriptor
            // in a test that is failing anyway is the safer of the two.
            let transferred = sock.into_raw_fd();
            let address = build_address(transferred);

            let task = monarch_with_gil_blocking(GilSite::Test, |py| {
                run_worker_loop_forever(py, &address)
            })
            .expect("a valid descriptor address should construct a task");

            // SAFETY: `F_GETFD` only reads the flags of a descriptor number and
            // never closes it. A number that is not open is defined input here:
            // the call returns -1 and sets EBADF, which is exactly the outcome
            // this assertion rules out. Taking a `BorrowedFd` first would
            // instead assume the validity being tested.
            let flags = unsafe { libc::fcntl(transferred, libc::F_GETFD) };
            assert_ne!(
                flags, -1,
                "construction must leave the transferred descriptor number open"
            );
            // Still open is not enough: the number could have been closed and
            // handed back out by another thread. Requiring it to name the same
            // socket rules that out.
            assert_eq!(
                getsockname::<SockaddrIn>(transferred)
                    .expect("a retained descriptor names its socket"),
                bound,
                "the retained descriptor must still be the socket that was handed over"
            );
            assert!(
                getsockopt(&observer, sockopt::AcceptConn).expect("SO_ACCEPTCONN is readable"),
                "construction must call listen before it returns"
            );

            // The task must be the only owner left before the address can say
            // anything about what the task holds.
            drop(observer);
            assert_eq!(
                rebind_result(&bound),
                Err(Errno::EADDRINUSE),
                "the undriven task must still hold the listener"
            );

            monarch_with_gil_blocking(GilSite::Test, |_py| {
                drop(task);
                Ok::<_, PyErr>(())
            })
            .expect("dropping the task should not fail");

            // Deliberately not probing `transferred` here: once released, that
            // number may already belong to another thread's descriptor.
            assert_eq!(
                rebind_result(&bound),
                Ok(()),
                "dropping the undriven task must release the adopted listener"
            );
        }
    }

    /// An `fdN` on the bind side of a TCP alias is listened on during parsing,
    /// but the alias parser discards that listener before the binding returns.
    /// The returned task therefore retains no ownership of the bind socket.
    #[test]
    fn run_worker_loop_forever_alias_fd_listens_and_closes_during_construction() {
        pyo3::Python::initialize();

        let (sock, bound) = bound_not_listening();
        let observer = dup(sock.as_fd()).expect("duplicating for observation should succeed");
        assert!(
            !getsockopt(&observer, sockopt::AcceptConn).expect("SO_ACCEPTCONN is readable"),
            "the fixture must hand over a bound socket that is not yet listening"
        );

        let transferred = sock.into_raw_fd();
        let address = format!("tcp://127.0.0.1:4444@tcp://127.0.0.1:fd{transferred}");
        let task =
            monarch_with_gil_blocking(GilSite::Test, |py| run_worker_loop_forever(py, &address))
                .expect("a valid alias descriptor address should construct a task");

        assert!(
            getsockopt(&observer, sockopt::AcceptConn).expect("SO_ACCEPTCONN is readable"),
            "alias parsing must call listen before discarding the adopted listener"
        );
        drop(observer);
        assert_eq!(
            rebind_result(&bound),
            Ok(()),
            "the returned task must not retain the alias bind listener"
        );

        monarch_with_gil_blocking(GilSite::Test, |_py| {
            drop(task);
            Ok::<_, PyErr>(())
        })
        .expect("dropping the task should not fail");
    }

    /// A textual address failure and a descriptor-syntax failure are both
    /// raised by the call itself, so no task is created.
    ///
    /// Both are `ValueError`, as is the later bind failure when a task is
    /// driven, so the type alone does not say which phase failed. What
    /// distinguishes these is that there is no task to drive. This says nothing
    /// about a numeric descriptor that is syntactically valid; that precondition
    /// belongs to the caller and is not exercised here.
    #[test]
    fn run_worker_loop_forever_rejects_invalid_addresses_before_returning_task() {
        pyo3::Python::initialize();

        for (address, cause) in [
            (
                "tcp://127.0.0.1:fdnot-a-number",
                "invalid file descriptor number: fdnot-a-number",
            ),
            ("zzz://127.0.0.1:1234", "unsupported ZMQ scheme: zzz"),
        ] {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                let error = match run_worker_loop_forever(py, address) {
                    Ok(_) => panic!("{address} must not produce a task"),
                    Err(error) => error,
                };

                assert!(
                    error.get_type(py).is(PyValueError::type_object(py)),
                    "an address failure must be exactly ValueError"
                );
                let message = error.value(py).to_string();
                assert!(
                    message.contains(&format!("invalid worker address {address:?}")),
                    "the message must name the rejected address, got {message}"
                );
                assert!(
                    message.contains(cause),
                    "the message must retain the underlying cause, got {message}"
                );
                Ok::<_, PyErr>(())
            })
            .expect("assertions should not fail");
        }
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
