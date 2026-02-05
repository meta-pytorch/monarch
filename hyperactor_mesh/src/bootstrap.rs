/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::VecDeque;
use std::env::VarError;
use std::fmt;
use std::fs::OpenOptions;
use std::future;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use base64::prelude::*;
use futures::StreamExt;
use futures::stream;
use humantime::format_duration;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::ProcId;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelError;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::ProcHandle;
use hyperactor::host::ProcManager;
use hyperactor::host::TerminateSummary;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::IntoBoxedMailboxSender;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxServer;
use hyperactor::proc::Proc;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::Attrs;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::global::override_or_global;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::Level;
use typeuri::Named;

use crate::config::MESH_PROC_LAUNCHER_KIND;
use crate::host_mesh::mesh_agent::HostAgentMode;
use crate::host_mesh::mesh_agent::HostMeshAgent;
use crate::logging::OutputTarget;
use crate::logging::StreamFwder;
use crate::mesh_agent::ProcMeshAgent;
use crate::proc_launcher::LaunchOptions;
use crate::proc_launcher::NativeProcLauncher;
use crate::proc_launcher::ProcExitKind;
use crate::proc_launcher::ProcExitResult;
use crate::proc_launcher::ProcLauncher;
use crate::proc_launcher::ProcLauncherError;
use crate::proc_launcher::StdioHandling;
use crate::proc_launcher::SystemdProcLauncher;
use crate::proc_launcher::format_process_name;
use crate::resource;

mod mailbox;

declare_attrs! {
    /// Enable forwarding child stdout/stderr over the mesh log
    /// channel.
    ///
    /// When `true` (default): child stdio is piped; [`StreamFwder`]
    /// mirrors output to the parent console and forwards bytes to the
    /// log channel so a `LogForwardActor` can receive them.
    ///
    /// When `false`: no channel forwarding occurs. Child stdio may
    /// still be piped if [`MESH_ENABLE_FILE_CAPTURE`] is `true` or
    /// [`MESH_TAIL_LOG_LINES`] > 0; otherwise the child inherits the
    /// parent stdio (no interception).
    ///
    /// This flag does not affect console mirroring: child output
    /// always reaches the parent console—either via inheritance (no
    /// piping) or via [`StreamFwder`] when piping is active.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ENABLE_LOG_FORWARDING".to_string()),
        Some("enable_log_forwarding".to_string()),
    ))
    pub attr MESH_ENABLE_LOG_FORWARDING: bool = false;

    /// When `true`: if stdio is piped, each child's `StreamFwder`
    /// also forwards lines to a host-scoped `FileAppender` managed by
    /// the `BootstrapProcManager`. That appender creates exactly two
    /// files per manager instance—one for stdout and one for
    /// stderr—and **all** child processes' lines are multiplexed into
    /// those two files. This can be combined with
    /// [`MESH_ENABLE_LOG_FORWARDING`] ("stream+local").
    ///
    /// Notes:
    /// - The on-disk files are *aggregate*, not per-process.
    ///   Disambiguation is via the optional rank prefix (see
    ///   `PREFIX_WITH_RANK`), which `StreamFwder` prepends to lines
    ///   before writing.
    /// - On local runs, file capture is suppressed unless
    ///   `FORCE_FILE_LOG=true`. In that case `StreamFwder` still
    ///   runs, but the `FileAppender` may be `None` and no files are
    ///   written.
    /// - `MESH_TAIL_LOG_LINES` only controls the in-memory rotating
    ///   buffer used for peeking—independent of file capture.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ENABLE_FILE_CAPTURE".to_string()),
        Some("enable_file_capture".to_string()),
    ))
    pub attr MESH_ENABLE_FILE_CAPTURE: bool = false;

    /// Maximum number of log lines retained in a proc's stderr/stdout
    /// tail buffer. Used by [`StreamFwder`] when wiring child
    /// pipes. Default: 100
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_TAIL_LOG_LINES".to_string()),
        Some("tail_log_lines".to_string()),
    ))
    pub attr MESH_TAIL_LOG_LINES: usize = 0;

    /// If enabled (default), bootstrap child processes install
    /// `PR_SET_PDEATHSIG(SIGKILL)` so the kernel reaps them if the
    /// parent dies unexpectedly. This is a **production safety net**
    /// against leaked children; tests usually disable it via
    /// `std::env::set_var("HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG",
    /// "false")`.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG".to_string()),
        Some("mesh_bootstrap_enable_pdeathsig".to_string()),
    ))
    pub attr MESH_BOOTSTRAP_ENABLE_PDEATHSIG: bool = true;

    /// Maximum number of child terminations to run concurrently
    /// during bulk shutdown. Prevents unbounded spawning of
    /// termination tasks (which could otherwise spike CPU, I/O, or
    /// file descriptor load).
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_TERMINATE_CONCURRENCY".to_string()),
        Some("mesh_terminate_concurrency".to_string()),
    ))
    pub attr MESH_TERMINATE_CONCURRENCY: usize = 16;

    /// Per-child grace window for termination. When a shutdown is
    /// requested, the manager sends SIGTERM and waits this long for
    /// the child to exit before escalating to SIGKILL.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_TERMINATE_TIMEOUT".to_string()),
        Some("mesh_terminate_timeout".to_string()),
    ))
    pub attr MESH_TERMINATE_TIMEOUT: Duration = Duration::from_secs(10);
}

pub const BOOTSTRAP_ADDR_ENV: &str = "HYPERACTOR_MESH_BOOTSTRAP_ADDR";
pub const BOOTSTRAP_INDEX_ENV: &str = "HYPERACTOR_MESH_INDEX";
pub const CLIENT_TRACE_ID_ENV: &str = "MONARCH_CLIENT_TRACE_ID";

/// A channel used by each process to receive its own stdout and stderr
/// Because stdout and stderr can only be obtained by the parent process,
/// they need to be streamed back to the process.
pub(crate) const BOOTSTRAP_LOG_CHANNEL: &str = "BOOTSTRAP_LOG_CHANNEL";

pub(crate) const BOOTSTRAP_MODE_ENV: &str = "HYPERACTOR_MESH_BOOTSTRAP_MODE";
pub(crate) const PROCESS_NAME_ENV: &str = "HYPERACTOR_PROCESS_NAME";

/// Messages sent from the process to the allocator. This is an envelope
/// containing the index of the process (i.e., its "address" assigned by
/// the allocator), along with the control message in question.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub(crate) struct Process2Allocator(pub usize, pub Process2AllocatorMessage);
wirevalue::register_type!(Process2Allocator);

/// Control messages sent from processes to the allocator.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub(crate) enum Process2AllocatorMessage {
    /// Initialize a process2allocator session. The process is
    /// listening on the provided channel address, to which
    /// [`Allocator2Process`] messages are sent.
    Hello(ChannelAddr),

    /// A proc with the provided ID was started. Its mailbox is
    /// served at the provided channel address. Procs are started
    /// after instruction by the allocator through the corresponding
    /// [`Allocator2Process`] message.
    StartedProc(ProcId, ActorRef<ProcMeshAgent>, ChannelAddr),

    Heartbeat,
}
wirevalue::register_type!(Process2AllocatorMessage);

/// Messages sent from the allocator to a process.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub(crate) enum Allocator2Process {
    /// Request to start a new proc with the provided ID, listening
    /// to an address on the indicated channel transport.
    StartProc(ProcId, ChannelTransport),

    /// A request for the process to shut down its procs and exit the
    /// process with the provided code.
    StopAndExit(i32),

    /// A request for the process to immediately exit with the provided
    /// exit code
    Exit(i32),
}
wirevalue::register_type!(Allocator2Process);

async fn exit_if_missed_heartbeat(bootstrap_index: usize, bootstrap_addr: ChannelAddr) {
    let tx = match channel::dial(bootstrap_addr.clone()) {
        Ok(tx) => tx,

        Err(err) => {
            tracing::error!(
                "Failed to establish heartbeat connection to allocator, exiting! (addr: {:?}): {}",
                bootstrap_addr,
                err
            );
            std::process::exit(1);
        }
    };
    tracing::info!(
        "Heartbeat connection established to allocator (idx: {bootstrap_index}, addr: {bootstrap_addr:?})",
    );
    loop {
        RealClock.sleep(Duration::from_secs(5)).await;

        let result = tx
            .send(Process2Allocator(
                bootstrap_index,
                Process2AllocatorMessage::Heartbeat,
            ))
            .await;

        if let Err(err) = result {
            tracing::error!(
                "Heartbeat failed to allocator, exiting! (addr: {:?}): {}",
                bootstrap_addr,
                err
            );
            std::process::exit(1);
        }
    }
}

#[macro_export]
macro_rules! ok {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(value) => value,
            Err(e) => return ::anyhow::Error::from(e),
        }
    };
}

async fn halt<R>() -> R {
    future::pending::<()>().await;
    unreachable!()
}

/// Bootstrap a host in this process, returning a handle to the mesh agent.
///
/// To obtain the local proc, use `GetLocalProc` on the returned host mesh agent,
/// then use `GetProc` on the returned proc mesh agent.
///
/// - `addr`: the listening address of the host; this is used to bind the frontend address;
/// - `command`: optional bootstrap command to spawn procs, otherwise [`BootstrapProcManager::current`];
/// - `config`: optional runtime config overlay.
/// - `exit_on_shutdown`: if true, exit the process after handling a shutdown request.
pub async fn host(
    addr: ChannelAddr,
    command: Option<BootstrapCommand>,
    config: Option<Attrs>,
    exit_on_shutdown: bool,
) -> anyhow::Result<ActorHandle<HostMeshAgent>> {
    if let Some(attrs) = config {
        hyperactor_config::global::set(hyperactor_config::global::Source::Runtime, attrs);
        tracing::debug!("bootstrap: installed Runtime config snapshot (Host)");
    } else {
        tracing::debug!("bootstrap: no config snapshot provided (Host)");
    }

    let command = match command {
        Some(command) => command,
        None => BootstrapCommand::current()?,
    };
    let manager = BootstrapProcManager::new(command)?;

    // REMOVE(V0): forward unknown destinations to the default sender.
    let host = Host::new_with_default(manager, addr, Some(crate::router::global().clone().boxed()))
        .await?;
    let addr = host.addr().clone();
    let system_proc = host.system_proc().clone();
    let host_mesh_agent = system_proc.spawn::<HostMeshAgent>(
        "agent",
        HostMeshAgent::new(HostAgentMode::Process {
            host,
            exit_on_shutdown,
        }),
    )?;

    tracing::info!(
        "serving host at {}, agent: {}",
        addr,
        host_mesh_agent.bind::<HostMeshAgent>()
    );

    Ok(host_mesh_agent)
}

/// Bootstrap configures how a mesh process starts up.
///
/// Both `Proc` and `Host` variants may include an optional
/// configuration snapshot (`hyperactor_config::Attrs`). This
/// snapshot is serialized into the bootstrap payload and made
/// available to the child. Interpretation and application of that
/// snapshot is up to the child process; if omitted, the child falls
/// back to environment/default values.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Bootstrap {
    /// Bootstrap as a "v1" proc
    Proc {
        /// The ProcId of the proc to be bootstrapped.
        proc_id: ProcId,
        /// The backend address to which messages are forwarded.
        /// See [`hyperactor::host`] for channel topology details.
        backend_addr: ChannelAddr,
        /// The callback address used to indicate successful spawning.
        callback_addr: ChannelAddr,
        /// Directory for storing proc socket files. Procs place their sockets
        /// in this directory, so that they can be looked up by other procs
        /// for direct transfer.
        socket_dir_path: PathBuf,
        /// Optional config snapshot (`hyperactor_config::Attrs`)
        /// captured by the parent. If present, the child installs it
        /// as the `ClientOverride` layer so the parent's effective config
        /// takes precedence over Defaults.
        config: Option<Attrs>,
    },

    /// Bootstrap as a "v1" host bootstrap. This sets up a new `Host`,
    /// managed by a [`crate::host_mesh::mesh_agent::HostMeshAgent`].
    Host {
        /// The address on which to serve the host.
        addr: ChannelAddr,
        /// If specified, use the provided command instead of
        /// [`BootstrapCommand::current`].
        command: Option<BootstrapCommand>,
        /// Optional config snapshot (`hyperactor_config::Attrs`)
        /// captured by the parent. If present, the child installs it
        /// as the `ClientOverride` layer so the parent's effective config
        /// takes precedence over Defaults.
        config: Option<Attrs>,
        /// If true, exit the process after handling a shutdown request.
        exit_on_shutdown: bool,
    },

    /// Bootstrap as a legacy "v0" proc.
    V0ProcMesh {
        /// Optional config snapshot (`hyperactor_config::Attrs`)
        /// captured by the parent. If present, the child installs it
        /// as the `ClientOverride` layer so the parent's effective config
        /// takes precedence over Env/Defaults.
        config: Option<Attrs>,
    },
}

impl Default for Bootstrap {
    fn default() -> Self {
        Bootstrap::V0ProcMesh { config: None }
    }
}

impl Bootstrap {
    /// Serialize the mode into a environment-variable-safe string by
    /// base64-encoding its JSON representation.
    #[allow(clippy::result_large_err)]
    pub(crate) fn to_env_safe_string(&self) -> crate::Result<String> {
        Ok(BASE64_STANDARD.encode(serde_json::to_string(&self)?))
    }

    /// Deserialize the mode from the representation returned by [`to_env_safe_string`].
    #[allow(clippy::result_large_err)]
    pub(crate) fn from_env_safe_string(str: &str) -> crate::Result<Self> {
        let data = BASE64_STANDARD.decode(str)?;
        let data = std::str::from_utf8(&data)?;
        Ok(serde_json::from_str(data)?)
    }

    /// Get a bootstrap configuration from the environment; returns `None`
    /// if the environment does not specify a boostrap config.
    pub fn get_from_env() -> anyhow::Result<Option<Self>> {
        match std::env::var("HYPERACTOR_MESH_BOOTSTRAP_MODE") {
            Ok(mode) => match Bootstrap::from_env_safe_string(&mode) {
                Ok(mode) => Ok(Some(mode)),
                Err(e) => {
                    Err(anyhow::Error::from(e).context("parsing HYPERACTOR_MESH_BOOTSTRAP_MODE"))
                }
            },
            Err(VarError::NotPresent) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context("reading HYPERACTOR_MESH_BOOTSTRAP_MODE")),
        }
    }

    /// Inject this bootstrap configuration into the environment of the provided command.
    pub fn to_env(&self, cmd: &mut Command) {
        cmd.env(
            "HYPERACTOR_MESH_BOOTSTRAP_MODE",
            self.to_env_safe_string().unwrap(),
        );
    }

    /// Bootstrap this binary according to this configuration.
    /// This either runs forever, or returns an error.
    pub async fn bootstrap(self) -> anyhow::Error {
        tracing::info!(
            "bootstrapping mesh process: {}",
            serde_json::to_string(&self).unwrap()
        );

        if Debug::is_active() {
            let mut buf = Vec::new();
            writeln!(&mut buf, "bootstrapping {}:", std::process::id()).unwrap();
            #[cfg(unix)]
            writeln!(
                &mut buf,
                "\tparent pid: {}",
                std::os::unix::process::parent_id()
            )
            .unwrap();
            writeln!(
                &mut buf,
                "\tconfig: {}",
                serde_json::to_string(&self).unwrap()
            )
            .unwrap();
            match std::env::current_exe() {
                Ok(path) => writeln!(&mut buf, "\tcurrent_exe: {}", path.display()).unwrap(),
                Err(e) => writeln!(&mut buf, "\tcurrent_exe: error<{}>", e).unwrap(),
            }
            writeln!(&mut buf, "\targs:").unwrap();
            for arg in std::env::args() {
                writeln!(&mut buf, "\t\t{}", arg).unwrap();
            }
            writeln!(&mut buf, "\tenv:").unwrap();
            for (key, val) in std::env::vars() {
                writeln!(&mut buf, "\t\t{}={}", key, val).unwrap();
            }
            let _ = Debug.write(&buf);
            if let Ok(s) = std::str::from_utf8(&buf) {
                tracing::info!("{}", s);
            } else {
                tracing::info!("{:?}", buf);
            }
        }

        match self {
            Bootstrap::Proc {
                proc_id,
                backend_addr,
                callback_addr,
                socket_dir_path,
                config,
            } => {
                let entered = tracing::span!(
                    Level::INFO,
                    "proc_bootstrap",
                    %proc_id,
                    %backend_addr,
                    %callback_addr,
                    socket_dir_path = %socket_dir_path.display(),
                )
                .entered();
                if let Some(attrs) = config {
                    hyperactor_config::global::set(
                        hyperactor_config::global::Source::ClientOverride,
                        attrs,
                    );
                    tracing::debug!("bootstrap: installed ClientOverride config snapshot (Proc)");
                } else {
                    tracing::debug!("bootstrap: no config snapshot provided (Proc)");
                }

                if hyperactor_config::global::get(MESH_BOOTSTRAP_ENABLE_PDEATHSIG) {
                    // Safety net: normal shutdown is via
                    // `host_mesh.shutdown(&instance)`; PR_SET_PDEATHSIG
                    // is a last-resort guard against leaks if that
                    // protocol is bypassed.
                    let _ = install_pdeathsig_kill();
                } else {
                    eprintln!("(bootstrap) PDEATHSIG disabled via config");
                }

                let (local_addr, name) = ok!(proc_id
                    .as_direct()
                    .ok_or_else(|| anyhow::anyhow!("invalid proc id type: {}", proc_id)));
                // TODO provide a direct way to construct these
                let serve_addr = format!("unix:{}", socket_dir_path.join(name).display());
                let serve_addr = serve_addr.parse().unwrap();

                // The following is a modified host::spawn_proc to support direct
                // dialing between local procs: 1) we bind each proc to a deterministic
                // address in socket_dir_path; 2) we use LocalProcDialer to dial these
                // addresses for local procs.
                let proc_sender = mailbox::LocalProcDialer::new(
                    local_addr.clone(),
                    socket_dir_path,
                    ok!(MailboxClient::dial(backend_addr)),
                );

                let proc = Proc::new(proc_id.clone(), proc_sender.into_boxed());

                let agent_handle = ok!(ProcMeshAgent::boot_v1(proc.clone())
                    .map_err(|e| HostError::AgentSpawnFailure(proc_id, e)));

                let span = entered.exit();

                // Finally serve the proc on the same transport as the backend address,
                // and call back.
                let (proc_addr, proc_rx) = ok!(channel::serve(serve_addr));
                proc.clone().serve(proc_rx);
                ok!(ok!(channel::dial(callback_addr))
                    .send((proc_addr, agent_handle.bind::<ProcMeshAgent>()))
                    .instrument(span)
                    .await
                    .map_err(ChannelError::from));

                halt().await
            }
            Bootstrap::Host {
                addr,
                command,
                config,
                exit_on_shutdown,
            } => {
                ok!(host(addr, command, config, exit_on_shutdown).await);
                halt().await
            }
            Bootstrap::V0ProcMesh { config } => bootstrap_v0_proc_mesh(config).await,
        }
    }

    /// A variant of [`bootstrap`] that logs the error and exits the process
    /// if bootstrapping fails.
    pub async fn bootstrap_or_die(self) -> ! {
        let err = self.bootstrap().await;
        tracing::error!("failed to bootstrap mesh process: {}", err);
        std::process::exit(1)
    }
}

/// Install "kill me if parent dies" and close the race window.
pub fn install_pdeathsig_kill() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `getppid()` is a simple libc syscall returning the
        // parent PID; it has no side effects and does not touch memory.
        let ppid_before = unsafe { libc::getppid() };

        // SAFETY: Calling into libc; does not dereference memory, just
        // asks the kernel to deliver SIGKILL on parent death.
        let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_int) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        // Race-close: if the parent died between our exec and prctl(),
        // we won't get a signal, so detect that and exit now.
        //
        // If the parent PID changed, the parent has died and we've been
        // reparented. Note: We cannot assume ppid == 1 means the parent
        // died, as in container environments (e.g., Kubernetes) the parent
        // may legitimately run as PID 1.
        // SAFETY: `getppid()` is a simple libc syscall returning the
        // parent PID; it has no side effects and does not touch memory.
        let ppid_after = unsafe { libc::getppid() };
        if ppid_before != ppid_after {
            std::process::exit(0);
        }
    }
    Ok(())
}

/// Represents the lifecycle state of a **proc as hosted in an OS
/// process** managed by `BootstrapProcManager`.
///
/// Note: This type is deliberately distinct from [`ProcState`] and
/// [`ProcStopReason`] (see `alloc.rs`). Those types model allocator
/// *events* - e.g. "a proc was Created/Running/Stopped" - and are
/// consumed from an event stream during allocation. By contrast,
/// [`ProcStatus`] is a **live, queryable view**: it reflects the
/// current observed status of a running proc, as seen through the
/// [`BootstrapProcHandle`] API (stop, kill, status).
///
/// In short:
/// - `ProcState`/`ProcStopReason`: historical / event-driven model
/// - `ProcStatus`: immediate status surface for lifecycle control
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProcStatus {
    /// The OS process has been spawned but is not yet fully running.
    /// (Process-level: child handle exists, no confirmation yet.)
    Starting,
    /// The OS process is alive and considered running.
    /// (Proc-level: bootstrap may still be running.)
    Running { started_at: SystemTime },
    /// Ready means bootstrap has completed and the proc is serving.
    /// (Proc-level: bootstrap completed.)
    Ready {
        started_at: SystemTime,
        addr: ChannelAddr,
        agent: ActorRef<ProcMeshAgent>,
    },
    /// A stop has been requested (SIGTERM, graceful shutdown, etc.),
    /// but the OS process has not yet fully exited. (Proc-level:
    /// shutdown in progress; Process-level: still running.)
    Stopping { started_at: SystemTime },
    /// The process exited with a normal exit code. (Process-level:
    /// exit observed.)
    Stopped {
        exit_code: i32,
        stderr_tail: Vec<String>,
    },
    /// The process was killed by a signal (e.g. SIGKILL).
    /// (Process-level: abnormal termination.)
    Killed { signal: i32, core_dumped: bool },
    /// The proc or its process failed for some other reason
    /// (bootstrap error, unexpected condition, etc.). (Both levels:
    /// catch-all failure.)
    Failed { reason: String },
}

impl ProcStatus {
    /// Returns `true` if the proc is in a terminal (exited) state:
    /// [`ProcStatus::Stopped`], [`ProcStatus::Killed`], or
    /// [`ProcStatus::Failed`].
    #[inline]
    pub fn is_exit(&self) -> bool {
        matches!(
            self,
            ProcStatus::Stopped { .. } | ProcStatus::Killed { .. } | ProcStatus::Failed { .. }
        )
    }
}

impl std::fmt::Display for ProcStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcStatus::Starting => write!(f, "Starting"),
            ProcStatus::Running { started_at } => {
                let uptime = started_at
                    .elapsed()
                    .map(|d| format!(" up {}", format_duration(d)))
                    .unwrap_or_default();
                write!(f, "Running{uptime}")
            }
            ProcStatus::Ready {
                started_at, addr, ..
            } => {
                let uptime = started_at
                    .elapsed()
                    .map(|d| format!(" up {}", format_duration(d)))
                    .unwrap_or_default();
                write!(f, "Ready at {addr}{uptime}")
            }
            ProcStatus::Stopping { started_at } => {
                let uptime = started_at
                    .elapsed()
                    .map(|d| format!(" up {}", format_duration(d)))
                    .unwrap_or_default();
                write!(f, "Stopping{uptime}")
            }
            ProcStatus::Stopped { exit_code, .. } => write!(f, "Stopped(exit={exit_code})"),
            ProcStatus::Killed {
                signal,
                core_dumped,
            } => {
                if *core_dumped {
                    write!(f, "Killed(sig={signal}, core)")
                } else {
                    write!(f, "Killed(sig={signal})")
                }
            }
            ProcStatus::Failed { reason } => write!(f, "Failed({reason})"),
        }
    }
}

/// Error returned by [`BootstrapProcHandle::ready`].
#[derive(Debug, Clone)]
pub enum ReadyError {
    /// The proc reached a terminal state before `Ready`.
    Terminal(ProcStatus),
    /// The internal watch channel closed unexpectedly.
    ChannelClosed,
}

impl std::fmt::Display for ReadyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadyError::Terminal(st) => write!(f, "proc terminated before running: {st:?}"),
            ReadyError::ChannelClosed => write!(f, "status channel closed"),
        }
    }
}
impl std::error::Error for ReadyError {}

/// A handle to a proc launched by [`BootstrapProcManager`].
///
/// `BootstrapProcHandle` is a lightweight supervisor for an external
/// process: it tracks and broadcasts lifecycle state, and exposes a
/// small control/observation surface. While it may temporarily hold a
/// `tokio::process::Child` (shared behind a mutex) so the exit
/// monitor can `wait()` it, it is **not** the unique owner of the OS
/// process, and dropping a `BootstrapProcHandle` does not by itself
/// terminate the process.
///
/// What it pairs together:
/// - the **logical proc identity** (`ProcId`)
/// - the **live status surface** ([`ProcStatus`]), available both as
///   a synchronous snapshot (`status()`) and as an async stream via a
///   `tokio::sync::watch` channel (`watch()` / `changed()`)
///
/// Responsibilities:
/// - Retain the child handle only until the exit monitor claims it,
///   so the OS process can be awaited and its terminal status
///   recorded.
/// - Hold stdout/stderr tailers until the exit monitor takes them,
///   then join to recover buffered output for diagnostics.
/// - Update status via the `mark_*` transitions and broadcast changes
///   over the watch channel so tasks can `await` lifecycle
///   transitions without polling.
/// - Provide the foundation for higher-level APIs like `wait()`
///   (await terminal) and, later, `terminate()` / `kill()`.
///
/// Notes:
/// - Manager-level cleanup happens in [`BootstrapProcManager::drop`]:
///   it SIGKILLs any still-recorded PIDs; we do not rely on
///   `Child::kill_on_drop`.
///
/// Relationship to types:
/// - [`ProcStatus`]: live status surface, updated by this handle.
/// - [`ProcState`]/[`ProcStopReason`] (in `alloc.rs`):
///   allocator-facing, historical event log; not directly updated by
///   this type.
#[derive(Clone)]
pub struct BootstrapProcHandle {
    /// Logical identity of the proc in the mesh.
    proc_id: ProcId,

    /// Live lifecycle snapshot (see [`ProcStatus`]). Kept in a mutex
    /// so [`BootstrapProcHandle::status`] can return a synchronous
    /// copy. All mutations now flow through
    /// [`BootstrapProcHandle::transition`], which updates this field
    /// under the lock and then broadcasts on the watch channel.
    status: Arc<std::sync::Mutex<ProcStatus>>,

    /// Launcher used to terminate/kill the proc. The launcher owns
    /// the actual OS child handle and PID tracking.
    ///
    /// We hold a `Weak` reference so that when `BootstrapProcManager`
    /// drops, the launcher's `Arc` refcount reaches zero and its `Drop`
    /// runs, cleaning up any remaining child processes. If the manager
    /// is gone when we try to terminate/kill, we treat it as a no-op
    /// (the proc is being killed by the launcher's Drop anyway).
    launcher: Weak<dyn ProcLauncher>,

    /// Stdout monitor for this proc. Created with `StreamFwder::start`, it
    /// forwards output to a log channel and keeps a bounded ring buffer.
    /// Transferred to the exit monitor, which joins it after `wait()`
    /// to recover buffered lines.
    stdout_fwder: Arc<std::sync::Mutex<Option<StreamFwder>>>,

    /// Stderr monitor for this proc. Same behavior as `stdout_fwder`
    /// but for stderr (used for exit-reason enrichment).
    stderr_fwder: Arc<std::sync::Mutex<Option<StreamFwder>>>,

    /// Watch sender for status transitions. Every `mark_*` goes
    /// through [`BootstrapProcHandle::transition`], which updates the
    /// snapshot under the lock and then `send`s the new
    /// [`ProcStatus`].
    tx: tokio::sync::watch::Sender<ProcStatus>,

    /// Watch receiver seed. `watch()` clones this so callers can
    /// `borrow()` the current status and `changed().await` future
    /// transitions independently.
    rx: tokio::sync::watch::Receiver<ProcStatus>,
}

impl fmt::Debug for BootstrapProcHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = self.status.lock().expect("status mutex poisoned").clone();
        f.debug_struct("BootstrapProcHandle")
            .field("proc_id", &self.proc_id)
            .field("status", &status)
            .field("launcher", &"<dyn ProcLauncher>")
            .field("tx", &"<watch::Sender>")
            .field("rx", &"<watch::Receiver>")
            // Intentionally skip stdout_tailer / stderr_tailer (not
            // Debug).
            .finish()
    }
}

// Locking invariant:
// - Do not acquire other locks from inside `transition(...)` (it
//   holds the status mutex).
impl BootstrapProcHandle {
    /// Construct a new [`BootstrapProcHandle`] for a freshly spawned
    /// OS process hosting a proc.
    ///
    /// - Initializes the status to [`ProcStatus::Starting`] since the
    ///   child process has been created but not yet confirmed running.
    /// - Stores the launcher reference for terminate/kill delegation.
    ///
    /// This is the canonical entry point used by
    /// `BootstrapProcManager` when it launches a proc into a new
    /// process.
    pub(crate) fn new(proc_id: ProcId, launcher: Weak<dyn ProcLauncher>) -> Self {
        let (tx, rx) = watch::channel(ProcStatus::Starting);
        Self {
            proc_id,
            status: Arc::new(std::sync::Mutex::new(ProcStatus::Starting)),
            launcher,
            stdout_fwder: Arc::new(std::sync::Mutex::new(None)),
            stderr_fwder: Arc::new(std::sync::Mutex::new(None)),
            tx,
            rx,
        }
    }

    /// Return the logical proc identity in the mesh.
    #[inline]
    pub fn proc_id(&self) -> &ProcId {
        &self.proc_id
    }

    /// Create a new subscription to this proc's status stream.
    ///
    /// Each call returns a fresh [`watch::Receiver`] tied to this
    /// handle's internal [`ProcStatus`] channel. The receiver can be
    /// awaited on (`rx.changed().await`) to observe lifecycle
    /// transitions as they occur.
    ///
    /// Notes:
    /// - Multiple subscribers can exist simultaneously; each sees
    ///   every status update in order.
    /// - Use [`BootstrapProcHandle::status`] for a one-off snapshot;
    ///   use `watch()` when you need to await changes over time.
    #[inline]
    pub fn watch(&self) -> tokio::sync::watch::Receiver<ProcStatus> {
        self.rx.clone()
    }

    /// Wait until this proc's status changes.
    ///
    /// This is a convenience wrapper around
    /// [`watch::Receiver::changed`]: it subscribes internally via
    /// [`BootstrapProcHandle::watch`] and awaits the next transition.
    /// If no subscribers exist or the channel is closed, this returns
    /// without error.
    ///
    /// Typical usage:
    /// ```ignore
    /// handle.changed().await;
    /// match handle.status() {
    ///     ProcStatus::Running { .. } => { /* now running */ }
    ///     ProcStatus::Stopped { .. } => { /* exited */ }
    ///     _ => {}
    /// }
    /// ```
    #[inline]
    pub async fn changed(&self) {
        let _ = self.watch().changed().await;
    }

    /// Return a snapshot of the current [`ProcStatus`] for this proc.
    ///
    /// This is a *live view* of the lifecycle state as tracked by
    /// [`BootstrapProcManager`]. It reflects what is currently known
    /// about the underlying OS process (e.g., `Starting`, `Running`,
    /// `Stopping`, etc.).
    ///
    /// Internally this reads the mutex-guarded status. Use this when
    /// you just need a synchronous snapshot; use
    /// [`BootstrapProcHandle::watch`] or
    /// [`BootstrapProcHandle::changed`] if you want to await
    /// transitions asynchronously.
    #[must_use]
    pub fn status(&self) -> ProcStatus {
        // Source of truth for now is the mutex. We broadcast via
        // `watch` in `transition`, but callers that want a
        // synchronous snapshot should read the guarded value.
        self.status.lock().expect("status mutex poisoned").clone()
    }

    /// Atomically apply a state transition while holding the status
    /// lock, and send the updated value on the watch channel **while
    /// still holding the lock**. This guarantees the mutex state and
    /// the broadcast value stay in sync and avoids reordering between
    /// concurrent transitions.
    fn transition<F>(&self, f: F) -> bool
    where
        F: FnOnce(&mut ProcStatus) -> bool,
    {
        let mut guard = self.status.lock().expect("status mutex poisoned");
        let _before = guard.clone();
        let changed = f(&mut guard);
        if changed {
            // Publish while still holding the lock to preserve order.
            let _ = self.tx.send(guard.clone());
        }
        changed
    }

    /// Transition this proc into the [`ProcStatus::Running`] state.
    ///
    /// Called internally once the child OS process has been spawned.
    /// Records the `started_at` timestamp so that callers can query it
    /// later via [`BootstrapProcHandle::status`].
    ///
    /// This is a best-effort marker: it reflects that the process
    /// exists at the OS level, but does not guarantee that the proc
    /// has completed bootstrap or is fully ready.
    pub(crate) fn mark_running(&self, started_at: SystemTime) -> bool {
        self.transition(|st| match *st {
            ProcStatus::Starting => {
                *st = ProcStatus::Running { started_at };
                true
            }
            _ => {
                tracing::warn!(
                    "illegal transition: {:?} -> Running; leaving status unchanged",
                    *st
                );
                false
            }
        })
    }

    /// Attempt to transition this proc into the [`ProcStatus::Ready`]
    /// state.
    ///
    /// This records the listening address and agent once the proc has
    /// successfully started and is ready to serve. The `started_at`
    /// timestamp is derived from the current `Running` state.
    ///
    /// Returns `true` if the transition succeeded (from `Starting` or
    /// `Running`), or `false` if the current state did not allow
    /// moving to `Ready`. In the latter case the state is left
    /// unchanged and a warning is logged.
    pub(crate) fn mark_ready(&self, addr: ChannelAddr, agent: ActorRef<ProcMeshAgent>) -> bool {
        tracing::info!(proc_id = %self.proc_id, %addr, "{} ready at {}", self.proc_id, addr);
        self.transition(|st| match st {
            ProcStatus::Starting => {
                // Unexpected: we should be Running before Ready, but
                // handle gracefully with current time.
                *st = ProcStatus::Ready {
                    started_at: RealClock.system_time_now(),
                    addr,
                    agent,
                };
                true
            }
            ProcStatus::Running { started_at } => {
                let started_at = *started_at;
                *st = ProcStatus::Ready {
                    started_at,
                    addr,
                    agent,
                };
                true
            }
            _ => {
                tracing::warn!(
                    "illegal transition: {:?} -> Ready; leaving status unchanged",
                    st
                );
                false
            }
        })
    }

    /// Record that a stop has been requested for the proc (e.g. a
    /// graceful shutdown via SIGTERM), but the underlying process has
    /// not yet fully exited.
    pub(crate) fn mark_stopping(&self) -> bool {
        let now = hyperactor::clock::RealClock.system_time_now();

        self.transition(|st| match *st {
            ProcStatus::Running { started_at } => {
                *st = ProcStatus::Stopping { started_at };
                true
            }
            ProcStatus::Ready { started_at, .. } => {
                *st = ProcStatus::Stopping { started_at };
                true
            }
            ProcStatus::Starting => {
                *st = ProcStatus::Stopping { started_at: now };
                true
            }
            _ => false,
        })
    }

    /// Record that the process has exited normally with the given
    /// exit code.
    pub(crate) fn mark_stopped(&self, exit_code: i32, stderr_tail: Vec<String>) -> bool {
        self.transition(|st| match *st {
            ProcStatus::Starting
            | ProcStatus::Running { .. }
            | ProcStatus::Ready { .. }
            | ProcStatus::Stopping { .. } => {
                *st = ProcStatus::Stopped {
                    exit_code,
                    stderr_tail,
                };
                true
            }
            _ => {
                tracing::warn!(
                    "illegal transition: {:?} -> Stopped; leaving status unchanged",
                    *st
                );
                false
            }
        })
    }

    /// Record that the process was killed by the given signal (e.g.
    /// SIGKILL, SIGTERM).
    pub(crate) fn mark_killed(&self, signal: i32, core_dumped: bool) -> bool {
        self.transition(|st| match *st {
            ProcStatus::Starting
            | ProcStatus::Running { .. }
            | ProcStatus::Ready { .. }
            | ProcStatus::Stopping { .. } => {
                *st = ProcStatus::Killed {
                    signal,
                    core_dumped,
                };
                true
            }
            _ => {
                tracing::warn!(
                    "illegal transition: {:?} -> Killed; leaving status unchanged",
                    *st
                );
                false
            }
        })
    }

    /// Record that the proc or its process failed for an unexpected
    /// reason (bootstrap error, spawn failure, etc.).
    pub(crate) fn mark_failed<S: Into<String>>(&self, reason: S) -> bool {
        self.transition(|st| match *st {
            ProcStatus::Starting
            | ProcStatus::Running { .. }
            | ProcStatus::Ready { .. }
            | ProcStatus::Stopping { .. } => {
                *st = ProcStatus::Failed {
                    reason: reason.into(),
                };
                true
            }
            _ => {
                tracing::warn!(
                    "illegal transition: {:?} -> Failed; leaving status unchanged",
                    *st
                );
                false
            }
        })
    }

    /// Wait until the proc has reached a terminal state and return
    /// it.
    ///
    /// Terminal means [`ProcStatus::Stopped`],
    /// [`ProcStatus::Killed`], or [`ProcStatus::Failed`]. If the
    /// current status is already terminal, returns immediately.
    ///
    /// Non-consuming: `BootstrapProcHandle` is a supervisor, not the
    /// owner of the OS process, so you can call `wait()` from
    /// multiple tasks concurrently.
    ///
    /// Implementation detail: listens on this handle's `watch`
    /// channel. It snapshots the current status, and if not terminal
    /// awaits the next change. If the channel closes unexpectedly,
    /// returns the last observed status.
    ///
    /// Mirrors `tokio::process::Child::wait()`, but yields the
    /// higher-level [`ProcStatus`] instead of an `ExitStatus`.
    #[must_use]
    pub async fn wait_inner(&self) -> ProcStatus {
        let mut rx = self.watch();
        loop {
            let st = rx.borrow().clone();
            if st.is_exit() {
                return st;
            }
            // If the channel closes, return the last observed value.
            if rx.changed().await.is_err() {
                return st;
            }
        }
    }

    /// Wait until the proc reaches the [`ProcStatus::Ready`] state.
    ///
    /// If the proc hits a terminal state ([`ProcStatus::Stopped`],
    /// [`ProcStatus::Killed`], or [`ProcStatus::Failed`]) before ever
    /// becoming `Ready`, this returns
    /// `Err(ReadyError::Terminal(status))`. If the internal watch
    /// channel closes unexpectedly, this returns
    /// `Err(ReadyError::ChannelClosed)`. Otherwise it returns
    /// `Ok(())` when `Ready` is first observed.
    ///
    /// Non-consuming: `BootstrapProcHandle` is a supervisor, not the
    /// owner; multiple tasks may await `ready()` concurrently.
    /// `Stopping` is not treated as terminal here; we continue
    /// waiting until `Ready` or a terminal state is seen.
    ///
    /// Companion to [`BootstrapProcHandle::wait_inner`]:
    /// `wait_inner()` resolves on exit; `ready_inner()` resolves on
    /// startup.
    pub async fn ready_inner(&self) -> Result<(), ReadyError> {
        let mut rx = self.watch();
        loop {
            let st = rx.borrow().clone();
            match &st {
                ProcStatus::Ready { .. } => return Ok(()),
                s if s.is_exit() => return Err(ReadyError::Terminal(st)),
                _non_terminal => {
                    if rx.changed().await.is_err() {
                        return Err(ReadyError::ChannelClosed);
                    }
                }
            }
        }
    }

    pub fn set_stream_monitors(&self, out: Option<StreamFwder>, err: Option<StreamFwder>) {
        *self
            .stdout_fwder
            .lock()
            .expect("stdout_tailer mutex poisoned") = out;
        *self
            .stderr_fwder
            .lock()
            .expect("stderr_tailer mutex poisoned") = err;
    }

    fn take_stream_monitors(&self) -> (Option<StreamFwder>, Option<StreamFwder>) {
        let out = self
            .stdout_fwder
            .lock()
            .expect("stdout_tailer mutex poisoned")
            .take();
        let err = self
            .stderr_fwder
            .lock()
            .expect("stderr_tailer mutex poisoned")
            .take();
        (out, err)
    }

    /// Sends a StopAll message to the ProcMeshAgent, which should exit the process.
    /// Waits for the successful state change of the process. If the process
    /// doesn't reach a terminal state, returns Err.
    async fn send_stop_all(
        &self,
        cx: &impl context::Actor,
        agent: ActorRef<ProcMeshAgent>,
        timeout: Duration,
        reason: &str,
    ) -> anyhow::Result<ProcStatus> {
        // For all of the messages and replies in this function:
        // if the proc is already dead, then the message will be undeliverable,
        // which should be ignored.
        // If this message isn't deliverable to the agent, the process may have
        // stopped already. No need to produce any errors, just continue with
        // killing the process.
        let mut agent_port = agent.port();
        agent_port.return_undeliverable(false);
        agent_port.send(
            cx,
            resource::StopAll {
                reason: reason.to_string(),
            },
        )?;
        // The agent handling Stop should exit the process, if it doesn't within
        // the time window, we escalate to SIGTERM.
        match RealClock.timeout(timeout, self.wait()).await {
            Ok(Ok(st)) => Ok(st),
            Ok(Err(e)) => Err(anyhow::anyhow!("agent did not exit the process: {:?}", e)),
            Err(_) => Err(anyhow::anyhow!("agent did not exit the process in time")),
        }
    }
}

#[async_trait]
impl hyperactor::host::ProcHandle for BootstrapProcHandle {
    type Agent = ProcMeshAgent;
    type TerminalStatus = ProcStatus;

    #[inline]
    fn proc_id(&self) -> &ProcId {
        &self.proc_id
    }

    #[inline]
    fn addr(&self) -> Option<ChannelAddr> {
        match &*self.status.lock().expect("status mutex poisoned") {
            ProcStatus::Ready { addr, .. } => Some(addr.clone()),
            _ => None,
        }
    }

    #[inline]
    fn agent_ref(&self) -> Option<ActorRef<Self::Agent>> {
        match &*self.status.lock().expect("status mutex poisoned") {
            ProcStatus::Ready { agent, .. } => Some(agent.clone()),
            _ => None,
        }
    }

    /// Wait until this proc first reaches the [`ProcStatus::Ready`]
    /// state.
    ///
    /// Returns `Ok(())` once `Ready` is observed.
    ///
    /// If the proc transitions directly to a terminal state before
    /// becoming `Ready`, returns `Err(ReadyError::Terminal(status))`.
    ///
    /// If the internal status watch closes unexpectedly before
    /// `Ready` is observed, returns `Err(ReadyError::ChannelClosed)`.
    async fn ready(&self) -> Result<(), hyperactor::host::ReadyError<Self::TerminalStatus>> {
        match self.ready_inner().await {
            Ok(()) => Ok(()),
            Err(ReadyError::Terminal(status)) => {
                Err(hyperactor::host::ReadyError::Terminal(status))
            }
            Err(ReadyError::ChannelClosed) => Err(hyperactor::host::ReadyError::ChannelClosed),
        }
    }

    /// Wait until this proc reaches a terminal [`ProcStatus`].
    ///
    /// Returns `Ok(status)` when a terminal state is observed
    /// (`Stopped`, `Killed`, or `Failed`).
    ///
    /// If the internal status watch closes before any terminal state
    /// is seen, returns `Err(WaitError::ChannelClosed)`.
    async fn wait(&self) -> Result<Self::TerminalStatus, hyperactor::host::WaitError> {
        let status = self.wait_inner().await;
        if status.is_exit() {
            Ok(status)
        } else {
            Err(hyperactor::host::WaitError::ChannelClosed)
        }
    }

    /// Attempt to terminate the underlying OS process.
    ///
    /// This drives **process-level** teardown only:
    /// - First attempts graceful shutdown via `ProcMeshAgent` if available.
    /// - If that fails or times out, delegates to the launcher's
    ///   `terminate()` method, which handles SIGTERM/SIGKILL escalation.
    ///
    /// If the process was already in a terminal state when called,
    /// returns [`TerminateError::AlreadyTerminated`].
    ///
    /// # Parameters
    /// - `timeout`: Grace period to wait after graceful shutdown before
    ///   escalating.
    /// - `reason`: Human-readable reason for termination.
    ///
    /// # Returns
    /// - `Ok(ProcStatus)` if the process exited during the
    ///   termination sequence.
    /// - `Err(TerminateError)` if already exited, signaling failed,
    ///   or the channel was lost.
    async fn terminate(
        &self,
        cx: &impl context::Actor,
        timeout: Duration,
        reason: &str,
    ) -> Result<ProcStatus, hyperactor::host::TerminateError<Self::TerminalStatus>> {
        // If already terminal, return that.
        let st0 = self.status();
        if st0.is_exit() {
            tracing::debug!(?st0, "terminate(): already terminal");
            return Err(hyperactor::host::TerminateError::AlreadyTerminated(st0));
        }

        // Before signaling, try to close actors normally. Only works if
        // they are in the Ready state and have an Agent we can message.
        let agent = self.agent_ref();
        if let Some(agent) = agent {
            match self.send_stop_all(cx, agent.clone(), timeout, reason).await {
                Ok(st) => return Ok(st),
                Err(e) => {
                    // Variety of possible errors, proceed with launcher termination.
                    tracing::warn!(
                        "ProcMeshAgent {} could not successfully stop all actors: {}",
                        agent.actor_id(),
                        e,
                    );
                }
            }
        }

        // Mark "Stopping" (ok if state races).
        let _ = self.mark_stopping();

        // Delegate to launcher for SIGTERM/SIGKILL escalation.
        tracing::info!(proc_id = %self.proc_id, ?timeout, "terminate(): delegating to launcher");
        if let Some(launcher) = self.launcher.upgrade() {
            if let Err(e) = launcher.terminate(&self.proc_id, timeout).await {
                tracing::warn!(proc_id = %self.proc_id, error=%e, "terminate(): launcher termination failed");
                return Err(hyperactor::host::TerminateError::Io(anyhow::anyhow!(
                    "launcher termination failed: {}",
                    e
                )));
            }
        } else {
            // Launcher dropped - its Drop is killing all procs anyway.
            tracing::debug!(proc_id = %self.proc_id, "terminate(): launcher gone, proc cleanup in progress");
        }

        // Wait for the exit monitor to observe terminal state.
        let st = self.wait_inner().await;
        if st.is_exit() {
            tracing::info!(proc_id = %self.proc_id, ?st, "terminate(): exited");
            Ok(st)
        } else {
            Err(hyperactor::host::TerminateError::ChannelClosed)
        }
    }

    /// Forcibly kill the underlying OS process.
    ///
    /// This bypasses any graceful shutdown semantics and immediately
    /// delegates to the launcher's `kill()` method. It is intended as
    /// a last-resort termination mechanism when `terminate()` fails or
    /// when no grace period is desired.
    ///
    /// # Behavior
    /// - If the process was already in a terminal state, returns
    ///   [`TerminateError::AlreadyTerminated`].
    /// - Otherwise delegates to the launcher's `kill()` method.
    /// - Then waits for the exit monitor to observe a terminal state.
    ///
    /// # Returns
    /// - `Ok(ProcStatus)` if the process exited after kill.
    /// - `Err(TerminateError)` if already exited, signaling failed,
    ///   or the channel was lost.
    async fn kill(
        &self,
    ) -> Result<ProcStatus, hyperactor::host::TerminateError<Self::TerminalStatus>> {
        // If already terminal, return that.
        let st0 = self.status();
        if st0.is_exit() {
            return Err(hyperactor::host::TerminateError::AlreadyTerminated(st0));
        }

        // Delegate to launcher for kill.
        tracing::info!(proc_id = %self.proc_id, "kill(): delegating to launcher");
        if let Some(launcher) = self.launcher.upgrade() {
            if let Err(e) = launcher.kill(&self.proc_id).await {
                tracing::warn!(proc_id = %self.proc_id, error=%e, "kill(): launcher kill failed");
                return Err(hyperactor::host::TerminateError::Io(anyhow::anyhow!(
                    "launcher kill failed: {}",
                    e
                )));
            }
        } else {
            // Launcher dropped - its Drop is killing all procs anyway.
            tracing::debug!(proc_id = %self.proc_id, "kill(): launcher gone, proc cleanup in progress");
        }

        // Wait for exit monitor to record terminal status.
        let st = self.wait_inner().await;
        if st.is_exit() {
            Ok(st)
        } else {
            Err(hyperactor::host::TerminateError::ChannelClosed)
        }
    }
}

/// A specification of the command used to bootstrap procs.
#[derive(Debug, Named, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct BootstrapCommand {
    pub program: PathBuf,
    pub arg0: Option<String>,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}
wirevalue::register_type!(BootstrapCommand);

impl BootstrapCommand {
    /// Creates a bootstrap command specification to replicate the
    /// invocation of the currently running process.
    pub fn current() -> io::Result<Self> {
        let mut args: VecDeque<String> = std::env::args().collect();
        let arg0 = args.pop_front();

        Ok(Self {
            program: std::env::current_exe()?,
            arg0,
            args: args.into(),
            env: std::env::vars().collect(),
        })
    }

    /// Create a new `Command` reflecting this bootstrap command
    /// configuration.
    pub fn new(&self) -> Command {
        let mut cmd = Command::new(&self.program);
        if let Some(arg0) = &self.arg0 {
            cmd.arg0(arg0);
        }
        for arg in &self.args {
            cmd.arg(arg);
        }
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        cmd
    }

    /// Bootstrap command used for testing, invoking the Buck-built
    /// `monarch/hyperactor_mesh/bootstrap` binary.
    ///
    /// Intended for integration tests where we need to spawn real
    /// bootstrap processes under proc manager control. Not available
    /// outside of test builds.
    #[cfg(test)]
    #[cfg(fbcode_build)]
    pub(crate) fn test() -> Self {
        Self {
            program: crate::testresource::get("monarch/hyperactor_mesh/bootstrap"),
            arg0: None,
            args: vec![],
            env: HashMap::new(),
        }
    }
}

impl<T: Into<PathBuf>> From<T> for BootstrapCommand {
    /// Creates a bootstrap command from the provided path.
    fn from(s: T) -> Self {
        Self {
            program: s.into(),
            arg0: None,
            args: vec![],
            env: HashMap::new(),
        }
    }
}

/// Selects which built-in process launcher backend to use for
/// spawning procs.
///
/// This is an internal "implementation choice" control for the
/// `ProcLauncher` abstraction: both variants are expected to satisfy
/// the same lifecycle contract (launch, observe exit,
/// terminate/kill), but they differ in *how* the OS process is
/// supervised.
///
/// Variants:
/// - [`LauncherKind::Native`]: spawns and supervises child processes
///   directly using `tokio::process` (traditional parent/child
///   model).
/// - [`LauncherKind::Systemd`]: delegates supervision to `systemd
///   --user` by creating transient `.service` units and observing
///   lifecycle via D-Bus.
///
/// Configuration/parsing:
/// - The empty string and `"native"` map to [`LauncherKind::Native`]
///   (default).
/// - `"systemd"` maps to [`LauncherKind::Systemd`].
/// - Any other value is rejected as [`io::ErrorKind::InvalidInput`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LauncherKind {
    /// Spawn and supervise OS children directly (tokio-based
    /// launcher).
    Native,
    /// Spawn via transient `systemd --user` units and observe via
    /// D-Bus.
    Systemd,
}

impl FromStr for LauncherKind {
    type Err = io::Error;

    /// Parse a launcher kind from configuration text.
    ///
    /// Accepted values (case-insensitive, surrounding whitespace
    /// ignored):
    /// - `""` or `"native"` → [`LauncherKind::Native`]
    /// - `"systemd"` → [`LauncherKind::Systemd`]
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for any other string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "native" => Ok(Self::Native),
            "systemd" => Ok(Self::Systemd),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown proc launcher kind {other:?}; expected 'native' or 'systemd'"),
            )),
        }
    }
}

/// Host-side manager for launching and supervising **bootstrap
/// processes** (via the `bootstrap` entry point).
///
/// `BootstrapProcManager` is responsible for:
/// - choosing and constructing the configured [`ProcLauncher`]
///   backend,
/// - preparing the bootstrap command/environment for each proc,
/// - tracking proc lifecycle state via [`BootstrapProcHandle`] /
///   [`ProcStatus`],
/// - providing status/query APIs over the set of active procs.
///
/// It maintains an async registry mapping [`ProcId`] →
/// [`BootstrapProcHandle`] for lifecycle queries and exit
/// observation.
///
/// ## Stdio and cleanup
///
/// Stdio handling and shutdown/cleanup behavior are
/// **launcher-dependent**:
/// - The native launcher may capture/tail stdout/stderr and manages
///   OS child processes directly.
/// - The systemd launcher delegates supervision to systemd transient
///   units on the user manager and does not expose a PID; stdio is
///   managed by systemd.
///
/// On drop/shutdown, process cleanup is *best-effort* and performed
/// via the selected launcher (e.g. direct child termination for
/// native, `StopUnit` for systemd).
pub struct BootstrapProcManager {
    /// The process launcher backend.
    launcher: Arc<dyn ProcLauncher>,

    /// The command specification used to bootstrap new processes.
    command: BootstrapCommand,

    /// Async registry of running children, keyed by [`ProcId`]. Holds
    /// [`BootstrapProcHandle`]s so callers can query or monitor
    /// status.
    children: Arc<tokio::sync::Mutex<HashMap<ProcId, BootstrapProcHandle>>>,

    /// FileMonitor that aggregates logs from all children. None if
    /// file monitor creation failed.
    file_appender: Option<Arc<crate::logging::FileAppender>>,

    /// Directory for storing proc socket files. Procs place their
    /// sockets in this directory, so that they can be looked up by
    /// other procs for direct transfer.
    socket_dir: TempDir,
}

impl BootstrapProcManager {
    /// Construct a new [`BootstrapProcManager`] that will launch
    /// procs using the given bootstrap command specification.
    ///
    /// This is the general entry point when you want to manage procs
    /// backed by a specific binary path (e.g. a bootstrap
    /// trampoline).
    pub(crate) fn new(command: BootstrapCommand) -> Result<Self, io::Error> {
        let kind_str = hyperactor_config::global::get_cloned(MESH_PROC_LAUNCHER_KIND);
        let kind: LauncherKind = kind_str.parse()?;

        let launcher: Arc<dyn ProcLauncher> = match kind {
            LauncherKind::Native => Arc::new(NativeProcLauncher::new()),
            LauncherKind::Systemd => Arc::new(SystemdProcLauncher::new()),
        };

        tracing::info!(kind = ?kind, config_value = %kind_str, "proc launcher selected");

        let file_appender = if hyperactor_config::global::get(MESH_ENABLE_FILE_CAPTURE) {
            match crate::logging::FileAppender::new() {
                Some(fm) => {
                    tracing::info!("file appender created successfully");
                    Some(Arc::new(fm))
                }
                None => {
                    tracing::warn!("failed to create file appender");
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            launcher,
            command,
            children: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            file_appender,
            socket_dir: runtime_dir()?,
        })
    }

    /// The bootstrap command used to launch processes.
    pub fn command(&self) -> &BootstrapCommand {
        &self.command
    }

    /// The socket directory, where per-proc Unix sockets are placed.
    pub fn socket_dir(&self) -> &Path {
        self.socket_dir.path()
    }

    /// Return the current [`ProcStatus`] for the given [`ProcId`], if
    /// the proc is known to this manager.
    ///
    /// This queries the live [`BootstrapProcHandle`] stored in the
    /// manager's internal map. It provides an immediate snapshot of
    /// lifecycle state (`Starting`, `Running`, `Stopping`, `Stopped`,
    /// etc.).
    ///
    /// Returns `None` if the manager has no record of the proc (e.g.
    /// never spawned here, or entry already removed).
    pub async fn status(&self, proc_id: &ProcId) -> Option<ProcStatus> {
        self.children.lock().await.get(proc_id).map(|h| h.status())
    }

    fn spawn_exit_monitor(
        &self,
        proc_id: ProcId,
        handle: BootstrapProcHandle,
        exit_rx: tokio::sync::oneshot::Receiver<ProcExitResult>,
    ) {
        tokio::spawn(async move {
            // Wait for the launcher to report terminal status.
            let exit_result = match exit_rx.await {
                Ok(res) => res,
                Err(_) => {
                    // exit_rx sender was dropped without sending - launcher error.
                    let _ = handle.mark_failed("exit_rx sender dropped unexpectedly");
                    tracing::error!(
                        name = "ProcStatus",
                        status = "Exited::ChannelDropped",
                        %proc_id,
                        "exit channel closed without result"
                    );
                    return;
                }
            };

            // Collect stderr tail from StreamFwder if we captured stdio.
            // The launcher may also provide stderr_tail in exit_result;
            // prefer StreamFwder's tail if available (more complete).
            let mut stderr_tail: Vec<String> = Vec::new();
            let (stdout_mon, stderr_mon) = handle.take_stream_monitors();

            if let Some(t) = stderr_mon {
                let (lines, _bytes) = t.abort().await;
                stderr_tail = lines;
            }
            if let Some(t) = stdout_mon {
                let (_lines, _bytes) = t.abort().await;
            }

            // Fall back to launcher-provided tail if we didn't capture.
            if stderr_tail.is_empty() {
                if let Some(tail) = exit_result.stderr_tail {
                    stderr_tail = tail;
                }
            }

            let tail_str = if stderr_tail.is_empty() {
                None
            } else {
                Some(stderr_tail.join("\n"))
            };

            match exit_result.kind {
                ProcExitKind::Exited { code } => {
                    let _ = handle.mark_stopped(code, stderr_tail);
                    tracing::info!(
                        name = "ProcStatus",
                        status = "Exited::ExitWithCode",
                        %proc_id,
                        exit_code = code,
                        tail = tail_str,
                        "proc exited with code {code}"
                    );
                }
                ProcExitKind::Signaled {
                    signal,
                    core_dumped,
                } => {
                    let _ = handle.mark_killed(signal, core_dumped);
                    tracing::info!(
                        name = "ProcStatus",
                        status = "Exited::KilledBySignal",
                        %proc_id,
                        tail = tail_str,
                        "killed by signal {signal}"
                    );
                }
                ProcExitKind::Failed { reason } => {
                    let _ = handle.mark_failed(&reason);
                    tracing::info!(
                        name = "ProcStatus",
                        status = "Exited::Failed",
                        %proc_id,
                        tail = tail_str,
                        "proc failed: {reason}"
                    );
                }
            }
        });
    }
}

/// The configuration used for bootstrapped procs.
pub struct BootstrapProcConfig {
    /// The proc's create rank.
    pub create_rank: usize,

    /// Config values to set on the spawned proc's global config,
    /// at the `ClientOverride` layer.
    pub client_config_override: Attrs,
}

#[async_trait]
impl ProcManager for BootstrapProcManager {
    type Handle = BootstrapProcHandle;

    type Config = BootstrapProcConfig;

    /// Return the [`ChannelTransport`] used by this proc manager.
    ///
    /// For `BootstrapProcManager` this is always
    /// [`ChannelTransport::Unix`], since all procs are spawned
    /// locally on the same host and communicate over Unix domain
    /// sockets.
    fn transport(&self) -> ChannelTransport {
        ChannelTransport::Unix
    }

    /// Launch a new proc under this [`BootstrapProcManager`].
    ///
    /// Spawns the configured bootstrap binary (`self.program`) in a
    /// fresh child process. The environment is populated with
    /// variables that describe the bootstrap context — most
    /// importantly `HYPERACTOR_MESH_BOOTSTRAP_MODE`, which carries a
    /// base64-encoded JSON [`Bootstrap::Proc`] payload (proc id,
    /// backend addr, callback addr, optional config snapshot).
    /// Additional variables like `BOOTSTRAP_LOG_CHANNEL` are also set
    /// up for logging and control.
    ///
    /// Responsibilities performed here:
    /// - Create a one-shot callback channel so the child can confirm
    ///   successful bootstrap and return its mailbox address plus agent
    ///   reference.
    /// - Spawn the OS process with stdout/stderr piped.
    /// - Stamp the new [`BootstrapProcHandle`] as
    ///   [`ProcStatus::Running`] once a PID is observed.
    /// - Wire stdout/stderr pipes into local writers and forward them
    ///   over the logging channel (`BOOTSTRAP_LOG_CHANNEL`).
    /// - Insert the handle into the manager's children map and start
    ///   an exit monitor to track process termination.
    ///
    /// Returns a [`BootstrapProcHandle`] that exposes the child
    /// process's lifecycle (status, wait/ready, termination). Errors
    /// are surfaced as [`HostError`].
    #[hyperactor::instrument(fields(proc_id=proc_id.to_string(), addr=backend_addr.to_string()))]
    async fn spawn(
        &self,
        proc_id: ProcId,
        backend_addr: ChannelAddr,
        config: BootstrapProcConfig,
    ) -> Result<Self::Handle, HostError> {
        let (callback_addr, mut callback_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix))?;

        // Decide whether we need to capture stdio.
        let overrides = &config.client_config_override;
        let enable_forwarding = override_or_global(overrides, MESH_ENABLE_LOG_FORWARDING);
        let enable_file_capture = override_or_global(overrides, MESH_ENABLE_FILE_CAPTURE);
        let tail_size = override_or_global(overrides, MESH_TAIL_LOG_LINES);
        let need_stdio = enable_forwarding || enable_file_capture || tail_size > 0;

        let mode = Bootstrap::Proc {
            proc_id: proc_id.clone(),
            backend_addr,
            callback_addr,
            socket_dir_path: self.socket_dir.path().to_owned(),
            config: Some(config.client_config_override.clone()),
        };

        // Build LaunchOptions for the launcher.
        let bootstrap_payload = mode
            .to_env_safe_string()
            .map_err(|e| HostError::ProcessConfigurationFailure(proc_id.clone(), e.into()))?;

        let opts = LaunchOptions {
            bootstrap_payload,
            process_name: format_process_name(&proc_id),
            command: self.command.clone(),
            want_stdio: need_stdio,
            tail_lines: tail_size,
            log_channel: if enable_forwarding {
                Some(ChannelAddr::any(ChannelTransport::Unix))
            } else {
                None
            },
        };

        // Launch via the configured launcher backend.
        let launch_result = self
            .launcher
            .launch(&proc_id, opts.clone())
            .await
            .map_err(|e| {
                let io_err = match e {
                    ProcLauncherError::Launch(io_err) => io_err,
                    other => std::io::Error::other(other.to_string()),
                };
                HostError::ProcessSpawnFailure(proc_id.clone(), io_err)
            })?;

        // Wire up StreamFwders if stdio was captured.
        let (out_fwder, err_fwder) = match launch_result.stdio {
            StdioHandling::Captured { stdout, stderr } => {
                let (file_stdout, file_stderr) = if enable_file_capture {
                    match self.file_appender.as_deref() {
                        Some(fm) => (
                            Some(fm.addr_for(OutputTarget::Stdout)),
                            Some(fm.addr_for(OutputTarget::Stderr)),
                        ),
                        None => {
                            tracing::warn!("enable_file_capture=true but no FileAppender");
                            (None, None)
                        }
                    }
                } else {
                    (None, None)
                };

                let out = StreamFwder::start(
                    stdout,
                    file_stdout,
                    OutputTarget::Stdout,
                    tail_size,
                    opts.log_channel.clone(),
                    &proc_id,
                    config.create_rank,
                );
                let err = StreamFwder::start(
                    stderr,
                    file_stderr,
                    OutputTarget::Stderr,
                    tail_size,
                    opts.log_channel.clone(),
                    &proc_id,
                    config.create_rank,
                );
                (Some(out), Some(err))
            }
            StdioHandling::Inherited | StdioHandling::ManagedByLauncher => {
                if !need_stdio {
                    tracing::info!(
                        %proc_id, enable_forwarding, enable_file_capture, tail_size,
                        "child stdio NOT captured (forwarding/file_capture/tail all disabled)"
                    );
                }
                (None, None)
            }
        };

        // Create handle with launcher reference for terminate/kill delegation.
        let handle = BootstrapProcHandle::new(proc_id.clone(), Arc::downgrade(&self.launcher));
        handle.mark_running(launch_result.started_at);
        handle.set_stream_monitors(out_fwder, err_fwder);

        // Retain handle for lifecycle management.
        {
            let mut children = self.children.lock().await;
            children.insert(proc_id.clone(), handle.clone());
        }

        // Kick off an exit monitor that updates ProcStatus when the
        // launcher reports terminal status.
        self.spawn_exit_monitor(proc_id.clone(), handle.clone(), launch_result.exit_rx);

        // Handle callback from child proc when it confirms bootstrap.
        let h = handle.clone();
        tokio::spawn(async move {
            match callback_rx.recv().await {
                Ok((addr, agent)) => {
                    let _ = h.mark_ready(addr, agent);
                }
                Err(e) => {
                    // Child never called back; record failure.
                    let _ = h.mark_failed(format!("bootstrap callback failed: {e}"));
                }
            }
        });

        // Callers do `handle.read().await` for mesh readiness.
        Ok(handle)
    }
}

#[async_trait]
impl hyperactor::host::SingleTerminate for BootstrapProcManager {
    /// Attempt to gracefully terminate one child procs managed by
    /// this `BootstrapProcManager`.
    ///
    /// Each child handle is asked to `terminate(timeout)`, which
    /// sends SIGTERM, waits up to the deadline, and escalates to
    /// SIGKILL if necessary. Termination is attempted concurrently,
    /// with at most `max_in_flight` tasks running at once.
    ///
    /// Logs a warning for each failure.
    async fn terminate_proc(
        &self,
        cx: &impl context::Actor,
        proc: &ProcId,
        timeout: Duration,
        reason: &str,
    ) -> Result<(Vec<ActorId>, Vec<ActorId>), anyhow::Error> {
        // Snapshot to avoid holding the lock across awaits.
        let proc_handle: Option<BootstrapProcHandle> = {
            let mut guard = self.children.lock().await;
            guard.remove(proc)
        };

        if let Some(h) = proc_handle {
            h.terminate(cx, timeout, reason)
                .await
                .map(|_| (Vec::new(), Vec::new()))
                .map_err(|e| e.into())
        } else {
            Err(anyhow::anyhow!("proc doesn't exist: {}", proc))
        }
    }
}

#[async_trait]
impl hyperactor::host::BulkTerminate for BootstrapProcManager {
    /// Attempt to gracefully terminate all child procs managed by
    /// this `BootstrapProcManager`.
    ///
    /// Each child handle is asked to `terminate(timeout)`, which
    /// sends SIGTERM, waits up to the deadline, and escalates to
    /// SIGKILL if necessary. Termination is attempted concurrently,
    /// with at most `max_in_flight` tasks running at once.
    ///
    /// Returns a [`TerminateSummary`] with counts of how many procs
    /// were attempted, how many successfully terminated (including
    /// those that were already terminal), and how many failed.
    ///
    /// Logs a warning for each failure.
    async fn terminate_all(
        &self,
        cx: &impl context::Actor,
        timeout: Duration,
        max_in_flight: usize,
        reason: &str,
    ) -> TerminateSummary {
        // Snapshot to avoid holding the lock across awaits.
        let handles: Vec<BootstrapProcHandle> = {
            let guard = self.children.lock().await;
            guard.values().cloned().collect()
        };

        let attempted = handles.len();
        let mut ok = 0usize;

        let results = stream::iter(handles.into_iter().map(|h| async move {
            match h.terminate(cx, timeout, reason).await {
                Ok(_) | Err(hyperactor::host::TerminateError::AlreadyTerminated(_)) => {
                    // Treat "already terminal" as success.
                    true
                }
                Err(e) => {
                    tracing::warn!(error=%e, "terminate_all: failed to terminate child");
                    false
                }
            }
        }))
        .buffer_unordered(max_in_flight.max(1))
        .collect::<Vec<bool>>()
        .await;

        for r in results {
            if r {
                ok += 1;
            }
        }

        TerminateSummary {
            attempted,
            ok,
            failed: attempted.saturating_sub(ok),
        }
    }
}

/// Entry point to processes managed by hyperactor_mesh. Any process that is part
/// of a hyperactor_mesh program should call [`bootstrap`], which then configures
/// the process according to how it is invoked.
///
/// If bootstrap returns any error, it is defunct from the point of view of hyperactor_mesh,
/// and the process should likely exit:
///
/// ```ignore
/// let err = hyperactor_mesh::bootstrap().await;
/// tracing::error("could not bootstrap mesh process: {}", err);
/// std::process::exit(1);
/// ```
///
/// Use [`bootstrap_or_die`] to implement this behavior directly.
pub async fn bootstrap() -> anyhow::Error {
    let boot = ok!(Bootstrap::get_from_env()).unwrap_or_else(Bootstrap::default);
    boot.bootstrap().await
}

/// Bootstrap a v0 proc mesh. This launches a control process that responds to
/// Allocator2Process messages, conveying its own state in Process2Allocator messages.
///
/// The bootstrapping process is controlled by the
/// following environment variables:
///
/// - `HYPERACTOR_MESH_BOOTSTRAP_ADDR`: the channel address to which Process2Allocator messages
///   should be sent.
/// - `HYPERACTOR_MESH_INDEX`: an index used to identify this process to the allocator.
async fn bootstrap_v0_proc_mesh(config: Option<Attrs>) -> anyhow::Error {
    // Apply config before entering the nested scope
    if let Some(attrs) = config {
        hyperactor_config::global::set(hyperactor_config::global::Source::ClientOverride, attrs);
        tracing::debug!("bootstrap: installed ClientOverride config snapshot (V0ProcMesh)");
    } else {
        tracing::debug!("bootstrap: no config snapshot provided (V0ProcMesh)");
    }
    tracing::info!(
        "bootstrap_v0_proc_mesh config:\n{}",
        hyperactor_config::global::attrs()
    );

    pub async fn go() -> Result<(), anyhow::Error> {
        let procs = Arc::new(tokio::sync::Mutex::new(Vec::<Proc>::new()));
        let procs_for_cleanup = procs.clone();
        let _cleanup_guard = hyperactor::register_signal_cleanup_scoped(Box::pin(async move {
            for proc_to_stop in procs_for_cleanup.lock().await.iter_mut() {
                if let Err(err) = proc_to_stop
                    .destroy_and_wait::<()>(
                        Duration::from_millis(10),
                        None,
                        "execute cleanup callback",
                    )
                    .await
                {
                    tracing::error!(
                        "error while stopping proc {}: {}",
                        proc_to_stop.proc_id(),
                        err
                    );
                }
            }
        }));

        let bootstrap_addr: ChannelAddr = std::env::var(BOOTSTRAP_ADDR_ENV)
            .map_err(|err| anyhow::anyhow!("read `{}`: {}", BOOTSTRAP_ADDR_ENV, err))?
            .parse()?;
        let bootstrap_index: usize = std::env::var(BOOTSTRAP_INDEX_ENV)
            .map_err(|err| anyhow::anyhow!("read `{}`: {}", BOOTSTRAP_INDEX_ENV, err))?
            .parse()?;
        let listen_addr = ChannelAddr::any(bootstrap_addr.transport());

        let entered = tracing::span!(
            Level::INFO,
            "bootstrap_v0_proc_mesh",
            %bootstrap_addr,
            %bootstrap_index,
            %listen_addr,
        )
        .entered();

        let (serve_addr, mut rx) = channel::serve(listen_addr)?;
        let tx = channel::dial(bootstrap_addr.clone())?;

        let (rtx, mut return_channel) = oneshot::channel();
        tx.try_post(
            Process2Allocator(bootstrap_index, Process2AllocatorMessage::Hello(serve_addr)),
            rtx,
        );
        tokio::spawn(exit_if_missed_heartbeat(bootstrap_index, bootstrap_addr));

        let _ = entered.exit();

        let mut the_msg;

        tokio::select! {
            msg = rx.recv() => {
                the_msg = msg;
            }
            returned_msg = &mut return_channel => {
                match returned_msg {
                    Ok(msg) => {
                        return Err(anyhow::anyhow!("Hello message was not delivered:{:?}", msg));
                    }
                    Err(_) => {
                        the_msg = rx.recv().await;
                    }
                }
            }
        }
        loop {
            match the_msg? {
                Allocator2Process::StartProc(proc_id, listen_transport) => {
                    let span = tracing::span!(Level::INFO, "Allocator2Process::StartProc", %proc_id, %listen_transport);
                    let (proc, mesh_agent) = ProcMeshAgent::bootstrap(proc_id.clone())
                        .instrument(span.clone())
                        .await?;
                    let entered = span.entered();
                    let (proc_addr, proc_rx) = channel::serve(ChannelAddr::any(listen_transport))?;
                    let handle = proc.clone().serve(proc_rx);
                    drop(handle); // linter appeasement; it is safe to drop this future
                    let span = entered.exit();
                    tx.send(Process2Allocator(
                        bootstrap_index,
                        Process2AllocatorMessage::StartedProc(
                            proc_id.clone(),
                            mesh_agent.bind(),
                            proc_addr,
                        ),
                    ))
                    .instrument(span)
                    .await?;
                    procs.lock().await.push(proc);
                }
                Allocator2Process::StopAndExit(code) => {
                    tracing::info!("stopping procs with code {code}");
                    {
                        for proc_to_stop in procs.lock().await.iter_mut() {
                            if let Err(err) = proc_to_stop
                                .destroy_and_wait::<()>(
                                    Duration::from_millis(10),
                                    None,
                                    "stop and exit",
                                )
                                .await
                            {
                                tracing::error!(
                                    "error while stopping proc {}: {}",
                                    proc_to_stop.proc_id(),
                                    err
                                );
                            }
                        }
                    }
                    tracing::info!("exiting with {code}");
                    std::process::exit(code);
                }
                Allocator2Process::Exit(code) => {
                    tracing::info!("exiting with {code}");
                    std::process::exit(code);
                }
            }
            the_msg = rx.recv().await;
        }
    }

    go().await.unwrap_err()
}

/// A variant of [`bootstrap`] that logs the error and exits the process
/// if bootstrapping fails.
pub async fn bootstrap_or_die() -> ! {
    let err = bootstrap().await;
    let _ = writeln!(Debug, "failed to bootstrap mesh process: {}", err);
    tracing::error!("failed to bootstrap mesh process: {}", err);
    std::process::exit(1)
}

#[derive(enum_as_inner::EnumAsInner)]
enum DebugSink {
    File(std::fs::File),
    Sink,
}

impl DebugSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            DebugSink::File(f) => f.write(buf),
            DebugSink::Sink => Ok(buf.len()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            DebugSink::File(f) => f.flush(),
            DebugSink::Sink => Ok(()),
        }
    }
}

fn debug_sink() -> &'static Mutex<DebugSink> {
    static DEBUG_SINK: OnceLock<Mutex<DebugSink>> = OnceLock::new();
    DEBUG_SINK.get_or_init(|| {
        let debug_path = {
            let mut p = std::env::temp_dir();
            if let Ok(user) = std::env::var("USER") {
                p.push(user);
            }
            std::fs::create_dir_all(&p).ok();
            p.push("monarch-bootstrap-debug.log");
            p
        };
        let sink = if debug_path.exists() {
            match OpenOptions::new()
                .append(true)
                .create(true)
                .open(debug_path.clone())
            {
                Ok(f) => DebugSink::File(f),
                Err(_e) => {
                    eprintln!(
                        "failed to open {} for bootstrap debug logging",
                        debug_path.display()
                    );
                    DebugSink::Sink
                }
            }
        } else {
            DebugSink::Sink
        };
        Mutex::new(sink)
    })
}

/// If true, send `Debug` messages to stderr.
const DEBUG_TO_STDERR: bool = false;

/// A bootstrap specific debug writer. If the file /tmp/monarch-bootstrap-debug.log
/// exists, then the writer's destination is that file; otherwise it discards all writes.
struct Debug;

impl Debug {
    fn is_active() -> bool {
        DEBUG_TO_STDERR || debug_sink().lock().unwrap().is_file()
    }
}

impl Write for Debug {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = debug_sink().lock().unwrap().write(buf);
        if DEBUG_TO_STDERR {
            let n = match res {
                Ok(n) => n,
                Err(_) => buf.len(),
            };
            let _ = io::stderr().write_all(&buf[..n]);
        }

        res
    }
    fn flush(&mut self) -> io::Result<()> {
        let res = debug_sink().lock().unwrap().flush();
        if DEBUG_TO_STDERR {
            let _ = io::stderr().flush();
        }
        res
    }
}

/// Create a new runtime [`TempDir`]. The directory is created in
/// `$XDG_RUNTIME_DIR`, otherwise falling back to the system tempdir.
fn runtime_dir() -> io::Result<TempDir> {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(runtime_dir) => {
            let path = PathBuf::from(runtime_dir);
            tempfile::tempdir_in(path)
        }
        None => tempfile::tempdir(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use hyperactor::ActorId;
    use hyperactor::ActorRef;
    use hyperactor::ProcId;
    use hyperactor::RemoteSpawn;
    use hyperactor::WorldId;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::TcpMode;
    use hyperactor::clock::RealClock;
    use hyperactor::context::Mailbox as _;
    use hyperactor::host::ProcHandle;
    use hyperactor::id;
    use ndslice::Extent;
    use ndslice::ViewExt;
    use ndslice::extent;
    use tokio::process::Command;

    use super::*;
    use crate::ActorMesh;
    use crate::alloc::AllocSpec;
    use crate::alloc::Allocator;
    use crate::alloc::ProcessAllocator;
    use crate::host_mesh::HostMesh;
    use crate::testactor;
    use crate::testing;

    // Helper: Avoid repeating
    // `ChannelAddr::any(ChannelTransport::Unix)`.
    fn any_addr_for_test() -> ChannelAddr {
        ChannelAddr::any(ChannelTransport::Unix)
    }

    #[test]
    fn test_bootstrap_mode_env_string_none_config_proc() {
        let values = [
            Bootstrap::default(),
            Bootstrap::Proc {
                proc_id: id!(foo[0]),
                backend_addr: ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
                callback_addr: ChannelAddr::any(ChannelTransport::Unix),
                socket_dir_path: PathBuf::from("notexist"),
                config: None,
            },
        ];

        for value in values {
            let safe = value.to_env_safe_string().unwrap();
            let round = Bootstrap::from_env_safe_string(&safe).unwrap();

            // Re-encode and compare: deterministic round-trip of the
            // wire format.
            let safe2 = round.to_env_safe_string().unwrap();
            assert_eq!(safe, safe2, "env-safe round-trip should be stable");

            // Sanity: the decoded variant is what we expect.
            match (&value, &round) {
                (Bootstrap::Proc { config: None, .. }, Bootstrap::Proc { config: None, .. }) => {}
                (
                    Bootstrap::V0ProcMesh { config: None },
                    Bootstrap::V0ProcMesh { config: None },
                ) => {}
                _ => panic!("decoded variant mismatch: got {:?}", round),
            }
        }
    }

    #[test]
    fn test_bootstrap_mode_env_string_none_config_host() {
        let value = Bootstrap::Host {
            addr: ChannelAddr::any(ChannelTransport::Unix),
            command: None,
            config: None,
            exit_on_shutdown: false,
        };

        let safe = value.to_env_safe_string().unwrap();
        let round = Bootstrap::from_env_safe_string(&safe).unwrap();

        // Wire-format round-trip should be identical.
        let safe2 = round.to_env_safe_string().unwrap();
        assert_eq!(safe, safe2);

        // Sanity: decoded variant is Host with None config.
        match round {
            Bootstrap::Host { config: None, .. } => {}
            other => panic!("expected Host with None config, got {:?}", other),
        }
    }

    #[test]
    fn test_bootstrap_mode_env_string_invalid() {
        // Not valid base64
        assert!(Bootstrap::from_env_safe_string("!!!").is_err());
    }

    #[test]
    fn test_bootstrap_config_snapshot_roundtrip() {
        // Build a small, distinctive Attrs snapshot.
        let mut attrs = Attrs::new();
        attrs[MESH_TAIL_LOG_LINES] = 123;
        attrs[MESH_BOOTSTRAP_ENABLE_PDEATHSIG] = false;

        let socket_dir = runtime_dir().unwrap();

        // Proc case
        {
            let original = Bootstrap::Proc {
                proc_id: id!(foo[42]),
                backend_addr: ChannelAddr::any(ChannelTransport::Unix),
                callback_addr: ChannelAddr::any(ChannelTransport::Unix),
                config: Some(attrs.clone()),
                socket_dir_path: socket_dir.path().to_owned(),
            };
            let env_str = original.to_env_safe_string().expect("encode bootstrap");
            let decoded = Bootstrap::from_env_safe_string(&env_str).expect("decode bootstrap");
            match &decoded {
                Bootstrap::Proc { config, .. } => {
                    let cfg = config.as_ref().expect("expected Some(attrs)");
                    assert_eq!(cfg[MESH_TAIL_LOG_LINES], 123);
                    assert!(!cfg[MESH_BOOTSTRAP_ENABLE_PDEATHSIG]);
                }
                other => panic!("unexpected variant after roundtrip: {:?}", other),
            }
        }

        // Host case
        {
            let original = Bootstrap::Host {
                addr: ChannelAddr::any(ChannelTransport::Unix),
                command: None,
                config: Some(attrs.clone()),
                exit_on_shutdown: false,
            };
            let env_str = original.to_env_safe_string().expect("encode bootstrap");
            let decoded = Bootstrap::from_env_safe_string(&env_str).expect("decode bootstrap");
            match &decoded {
                Bootstrap::Host { config, .. } => {
                    let cfg = config.as_ref().expect("expected Some(attrs)");
                    assert_eq!(cfg[MESH_TAIL_LOG_LINES], 123);
                    assert!(!cfg[MESH_BOOTSTRAP_ENABLE_PDEATHSIG]);
                }
                other => panic!("unexpected variant after roundtrip: {:?}", other),
            }
        }

        // V0ProcMesh case
        {
            let original = Bootstrap::V0ProcMesh {
                config: Some(attrs.clone()),
            };
            let env_str = original.to_env_safe_string().expect("encode bootstrap");
            let decoded = Bootstrap::from_env_safe_string(&env_str).expect("decode bootstrap");
            match &decoded {
                Bootstrap::V0ProcMesh { config } => {
                    let cfg = config.as_ref().expect("expected Some(attrs)");
                    assert_eq!(cfg[MESH_TAIL_LOG_LINES], 123);
                    assert!(!cfg[MESH_BOOTSTRAP_ENABLE_PDEATHSIG]);
                }
                other => panic!("unexpected variant after roundtrip: {:?}", other),
            }
        }
    }

    #[tokio::test]
    async fn test_v1_child_logging() {
        use hyperactor::channel;
        use hyperactor::mailbox::BoxedMailboxSender;
        use hyperactor::mailbox::DialMailboxRouter;
        use hyperactor::mailbox::MailboxServer;
        use hyperactor::proc::Proc;

        use crate::bootstrap::BOOTSTRAP_LOG_CHANNEL;
        use crate::logging::LogClientActor;
        use crate::logging::LogClientMessageClient;
        use crate::logging::LogForwardActor;
        use crate::logging::LogMessage;
        use crate::logging::OutputTarget;
        use crate::logging::test_tap;

        let router = DialMailboxRouter::new();
        let (proc_addr, proc_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix)).unwrap();
        let proc = Proc::new(id!(client[0]), BoxedMailboxSender::new(router.clone()));
        proc.clone().serve(proc_rx);
        router.bind(id!(client[0]).into(), proc_addr.clone());
        let (client, _handle) = proc.instance("client").unwrap();

        let (tap_tx, mut tap_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        test_tap::install(tap_tx);

        let log_channel = ChannelAddr::any(ChannelTransport::Unix);
        // SAFETY: unit-test scoped env var
        unsafe {
            std::env::set_var(BOOTSTRAP_LOG_CHANNEL, log_channel.to_string());
        }

        // Spawn the log client and disable aggregation (immediate
        // print + tap push).
        let log_client_actor = LogClientActor::new(()).await.unwrap();
        let log_client: ActorRef<LogClientActor> =
            proc.spawn("log_client", log_client_actor).unwrap().bind();
        log_client.set_aggregate(&client, None).await.unwrap();

        // Spawn the forwarder in this proc (it will serve
        // BOOTSTRAP_LOG_CHANNEL).
        let log_forwarder_actor = LogForwardActor::new(log_client.clone()).await.unwrap();
        let _log_forwarder: ActorRef<LogForwardActor> = proc
            .spawn("log_forwarder", log_forwarder_actor)
            .unwrap()
            .bind();

        // Dial the channel but don't post until we know the forwarder
        // is receiving.
        let tx = channel::dial::<LogMessage>(log_channel.clone()).unwrap();

        // Send a fake log message as if it came from the proc
        // manager's writer.
        tx.post(LogMessage::Log {
            hostname: "testhost".into(),
            proc_id: "testproc[0]".into(),
            output_target: OutputTarget::Stdout,
            payload: wirevalue::Any::serialize(&"hello from child".to_string()).unwrap(),
        });

        // Assert we see it via the tap.
        let line = RealClock
            .timeout(Duration::from_secs(2), tap_rx.recv())
            .await
            .expect("timed out waiting for log line")
            .expect("tap channel closed unexpectedly");
        assert!(
            line.contains("hello from child"),
            "log line did not appear via LogClientActor; got: {line}"
        );
    }

    mod proc_handle {

        use std::sync::Arc;
        use std::time::Duration;

        use async_trait::async_trait;
        use hyperactor::ActorId;
        use hyperactor::ActorRef;
        use hyperactor::ProcId;
        use hyperactor::WorldId;
        use hyperactor::host::ProcHandle;

        use super::super::*;
        use super::any_addr_for_test;
        use crate::proc_launcher::LaunchOptions;
        use crate::proc_launcher::LaunchResult;
        use crate::proc_launcher::ProcLauncher;
        use crate::proc_launcher::ProcLauncherError;

        /// A test launcher that panics on any method call.
        ///
        /// This is used by unit tests that only exercise status
        /// transitions on `BootstrapProcHandle` without actually
        /// launching or terminating processes. If any launcher method
        /// is called, the test will panic—indicating an unexpected
        /// code path.
        struct TestProcLauncher;

        #[async_trait]
        impl ProcLauncher for TestProcLauncher {
            async fn launch(
                &self,
                _proc_id: &ProcId,
                _opts: LaunchOptions,
            ) -> Result<LaunchResult, ProcLauncherError> {
                panic!("TestProcLauncher::launch should not be called in unit tests");
            }

            async fn terminate(
                &self,
                _proc_id: &ProcId,
                _timeout: Duration,
            ) -> Result<(), ProcLauncherError> {
                panic!("TestProcLauncher::terminate should not be called in unit tests");
            }

            async fn kill(&self, _proc_id: &ProcId) -> Result<(), ProcLauncherError> {
                panic!("TestProcLauncher::kill should not be called in unit tests");
            }
        }

        // Helper: build a ProcHandle for state-transition unit tests.
        //
        // This creates a handle with a no-op test launcher. The
        // launcher will panic if any of its methods are called,
        // ensuring tests only exercise status transitions and not
        // actual process lifecycle.
        fn handle_for_test() -> BootstrapProcHandle {
            let proc_id = ProcId::Ranked(WorldId("test".into()), 0);
            let launcher: Arc<dyn ProcLauncher> = Arc::new(TestProcLauncher);
            BootstrapProcHandle::new(proc_id, Arc::downgrade(&launcher))
        }

        #[tokio::test]
        async fn starting_to_running_ok() {
            let h = handle_for_test();
            assert!(matches!(h.status(), ProcStatus::Starting));
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_started_at));
            match h.status() {
                ProcStatus::Running { started_at } => {
                    assert_eq!(started_at, child_started_at);
                }
                other => panic!("expected Running, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn running_to_stopping_to_stopped_ok() {
            let h = handle_for_test();
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_started_at));
            assert!(h.mark_stopping());
            assert!(matches!(h.status(), ProcStatus::Stopping { .. }));
            assert!(h.mark_stopped(0, Vec::new()));
            assert!(matches!(
                h.status(),
                ProcStatus::Stopped { exit_code: 0, .. }
            ));
        }

        #[tokio::test]
        async fn running_to_killed_ok() {
            let h = handle_for_test();
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_started_at));
            assert!(h.mark_killed(9, true));
            assert!(matches!(
                h.status(),
                ProcStatus::Killed {
                    signal: 9,
                    core_dumped: true
                }
            ));
        }

        #[tokio::test]
        async fn running_to_failed_ok() {
            let h = handle_for_test();
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_started_at));
            assert!(h.mark_failed("bootstrap error"));
            match h.status() {
                ProcStatus::Failed { reason } => {
                    assert_eq!(reason, "bootstrap error");
                }
                other => panic!("expected Failed(\"bootstrap error\"), got {other:?}"),
            }
        }

        #[tokio::test]
        async fn illegal_transitions_are_rejected() {
            let h = handle_for_test();
            let child_started_at = RealClock.system_time_now();
            // Starting -> Running is fine; second Running should be rejected.
            assert!(h.mark_running(child_started_at));
            assert!(!h.mark_running(RealClock.system_time_now()));
            assert!(matches!(h.status(), ProcStatus::Running { .. }));
            // Once Stopped, we can't go to Running/Killed/Failed/etc.
            assert!(h.mark_stopping());
            assert!(h.mark_stopped(0, Vec::new()));
            assert!(!h.mark_running(child_started_at));
            assert!(!h.mark_killed(9, false));
            assert!(!h.mark_failed("nope"));

            assert!(matches!(
                h.status(),
                ProcStatus::Stopped { exit_code: 0, .. }
            ));
        }

        #[tokio::test]
        async fn transitions_from_ready_are_legal() {
            let h = handle_for_test();
            let addr = any_addr_for_test();
            // Mark Running.
            let t0 = RealClock.system_time_now();
            assert!(h.mark_running(t0));
            // Build a consistent AgentRef for Ready using the
            // handle's ProcId.
            let proc_id = <BootstrapProcHandle as ProcHandle>::proc_id(&h);
            let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
            let agent_ref: ActorRef<ProcMeshAgent> = ActorRef::attest(actor_id);
            // Ready -> Stopping -> Stopped should be legal.
            assert!(h.mark_ready(addr, agent_ref));
            assert!(h.mark_stopping());
            assert!(h.mark_stopped(0, Vec::new()));
        }

        #[tokio::test]
        async fn ready_to_killed_is_legal() {
            let h = handle_for_test();
            let addr = any_addr_for_test();
            // Starting -> Running
            let t0 = RealClock.system_time_now();
            assert!(h.mark_running(t0));
            // Build a consistent AgentRef for Ready using the
            // handle's ProcId.
            let proc_id = <BootstrapProcHandle as ProcHandle>::proc_id(&h);
            let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
            let agent: ActorRef<ProcMeshAgent> = ActorRef::attest(actor_id);
            // Running -> Ready
            assert!(h.mark_ready(addr, agent));
            // Ready -> Killed
            assert!(h.mark_killed(9, false));
        }

        #[tokio::test]
        async fn mark_failed_from_stopping_is_allowed() {
            let h = handle_for_test();

            // Drive Starting -> Stopping.
            assert!(h.mark_stopping(), "precondition: to Stopping");

            // Now allow Stopping -> Failed.
            assert!(
                h.mark_failed("boom"),
                "mark_failed() should succeed from Stopping"
            );
            match h.status() {
                ProcStatus::Failed { reason } => assert_eq!(reason, "boom"),
                other => panic!("expected Failed(\"boom\"), got {other:?}"),
            }
        }
    }

    /// A test launcher that panics on any method call.
    ///
    /// This is used by unit tests that only exercise status
    /// transitions on `BootstrapProcHandle` without actually
    /// launching or terminating processes.
    struct TestLauncher;

    #[async_trait::async_trait]
    impl crate::proc_launcher::ProcLauncher for TestLauncher {
        async fn launch(
            &self,
            _proc_id: &ProcId,
            _opts: crate::proc_launcher::LaunchOptions,
        ) -> Result<crate::proc_launcher::LaunchResult, crate::proc_launcher::ProcLauncherError>
        {
            panic!("TestLauncher::launch should not be called in unit tests");
        }

        async fn terminate(
            &self,
            _proc_id: &ProcId,
            _timeout: std::time::Duration,
        ) -> Result<(), crate::proc_launcher::ProcLauncherError> {
            panic!("TestLauncher::terminate should not be called in unit tests");
        }

        async fn kill(
            &self,
            _proc_id: &ProcId,
        ) -> Result<(), crate::proc_launcher::ProcLauncherError> {
            panic!("TestLauncher::kill should not be called in unit tests");
        }
    }

    fn test_handle(proc_id: ProcId) -> BootstrapProcHandle {
        let launcher: std::sync::Arc<dyn crate::proc_launcher::ProcLauncher> =
            std::sync::Arc::new(TestLauncher);
        BootstrapProcHandle::new(proc_id, std::sync::Arc::downgrade(&launcher))
    }

    #[tokio::test]
    async fn watch_notifies_on_status_changes() {
        let proc_id = ProcId::Ranked(WorldId("test".into()), 1);
        let handle = test_handle(proc_id);
        let mut rx = handle.watch();

        // Starting -> Running
        let now = RealClock.system_time_now();
        assert!(handle.mark_running(now));
        rx.changed().await.ok(); // Observe the transition.
        match &*rx.borrow() {
            ProcStatus::Running { started_at } => {
                assert_eq!(*started_at, now);
            }
            s => panic!("expected Running, got {s:?}"),
        }

        // Running -> Stopped
        assert!(handle.mark_stopped(0, Vec::new()));
        rx.changed().await.ok(); // Observe the transition.
        assert!(matches!(
            &*rx.borrow(),
            ProcStatus::Stopped { exit_code: 0, .. }
        ));
    }

    #[tokio::test]
    async fn ready_errs_if_process_exits_before_running() {
        let proc_id = ProcId::Direct(
            ChannelAddr::any(ChannelTransport::Unix),
            "early-exit".into(),
        );
        let handle = test_handle(proc_id);

        // Simulate the exit monitor doing its job directly here.
        // (Equivalent outcome: terminal state before Running.)
        assert!(handle.mark_stopped(7, Vec::new()));

        // `ready()` should return Err with the terminal status.
        match handle.ready_inner().await {
            Ok(()) => panic!("ready() unexpectedly succeeded"),
            Err(ReadyError::Terminal(ProcStatus::Stopped { exit_code, .. })) => {
                assert_eq!(exit_code, 7)
            }
            Err(other) => panic!("expected Stopped(7), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_unknown_proc_is_none() {
        let manager = BootstrapProcManager::new(BootstrapCommand {
            program: PathBuf::from("/bin/true"),
            ..Default::default()
        })
        .unwrap();
        let unknown = ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "nope".into());
        assert!(manager.status(&unknown).await.is_none());
    }

    #[tokio::test]
    async fn handle_ready_allows_waiters() {
        let proc_id = ProcId::Ranked(WorldId("test".into()), 42);
        let handle = test_handle(proc_id.clone());

        let started_at = RealClock.system_time_now();
        assert!(handle.mark_running(started_at));

        let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
        let agent_ref: ActorRef<ProcMeshAgent> = ActorRef::attest(actor_id);

        // Pick any addr to carry in Ready (what the child would have
        // called back with).
        let ready_addr = any_addr_for_test();

        // Stamp Ready and assert ready().await unblocks.
        assert!(handle.mark_ready(ready_addr.clone(), agent_ref));
        handle
            .ready_inner()
            .await
            .expect("ready_inner() should complete after Ready");

        // Sanity-check the Ready fields we control
        // (started_at/addr).
        match handle.status() {
            ProcStatus::Ready {
                started_at: t,
                addr: a,
                ..
            } => {
                assert_eq!(t, started_at);
                assert_eq!(a, ready_addr);
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[test]
    fn display_running_includes_uptime() {
        let started_at = RealClock.system_time_now() - Duration::from_secs(42);
        let st = ProcStatus::Running { started_at };

        let s = format!("{}", st);
        assert!(s.contains("Running"));
        assert!(s.contains("42s"));
    }

    #[test]
    fn display_ready_includes_addr() {
        let started_at = RealClock.system_time_now() - Duration::from_secs(5);
        let addr = ChannelAddr::any(ChannelTransport::Unix);
        let agent =
            ActorRef::attest(ProcId::Direct(addr.clone(), "proc".into()).actor_id("agent", 0));

        let st = ProcStatus::Ready {
            started_at,
            addr: addr.clone(),
            agent,
        };

        let s = format!("{}", st);
        assert!(s.contains(&addr.to_string())); // addr
        assert!(s.contains("Ready"));
    }

    #[test]
    fn display_stopped_includes_exit_code() {
        let st = ProcStatus::Stopped {
            exit_code: 7,
            stderr_tail: Vec::new(),
        };
        let s = format!("{}", st);
        assert!(s.contains("Stopped"));
        assert!(s.contains("7"));
    }

    #[test]
    fn display_other_variants_does_not_panic() {
        let samples = vec![
            ProcStatus::Starting,
            ProcStatus::Stopping {
                started_at: RealClock.system_time_now(),
            },
            ProcStatus::Ready {
                started_at: RealClock.system_time_now(),
                addr: ChannelAddr::any(ChannelTransport::Unix),
                agent: ActorRef::attest(
                    ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "x".into())
                        .actor_id("agent", 0),
                ),
            },
            ProcStatus::Killed {
                signal: 9,
                core_dumped: false,
            },
            ProcStatus::Failed {
                reason: "boom".into(),
            },
        ];

        for st in samples {
            let _ = format!("{}", st); // Just make sure it doesn't panic.
        }
    }

    #[tokio::test]
    async fn proc_handle_ready_ok_through_trait() {
        let proc_id = ProcId::Direct(any_addr_for_test(), "ph-ready-ok".into());
        let handle = test_handle(proc_id.clone());

        // Starting -> Running
        let t0 = RealClock.system_time_now();
        assert!(handle.mark_running(t0));

        // Synthesize Ready data
        let addr = any_addr_for_test();
        let agent: ActorRef<ProcMeshAgent> =
            ActorRef::attest(ActorId(proc_id.clone(), "agent".into(), 0));
        assert!(handle.mark_ready(addr, agent));

        // Call the trait method (not ready_inner).
        let r = <BootstrapProcHandle as hyperactor::host::ProcHandle>::ready(&handle).await;
        assert!(r.is_ok(), "expected Ok(()), got {r:?}");
    }

    #[tokio::test]
    async fn proc_handle_wait_returns_terminal_status() {
        let proc_id = ProcId::Direct(any_addr_for_test(), "ph-wait".into());
        let handle = test_handle(proc_id);

        // Drive directly to a terminal state before calling wait()
        assert!(handle.mark_stopped(0, Vec::new()));

        // Call the trait method (not wait_inner)
        let st = <BootstrapProcHandle as hyperactor::host::ProcHandle>::wait(&handle)
            .await
            .expect("wait should return Ok(terminal)");

        match st {
            ProcStatus::Stopped { exit_code, .. } => assert_eq!(exit_code, 0),
            other => panic!("expected Stopped(0), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ready_wrapper_maps_terminal_to_trait_error() {
        let proc_id = ProcId::Direct(any_addr_for_test(), "wrap".into());
        let handle = test_handle(proc_id);

        assert!(handle.mark_stopped(7, Vec::new()));

        match <BootstrapProcHandle as hyperactor::host::ProcHandle>::ready(&handle).await {
            Ok(()) => panic!("expected Err"),
            Err(hyperactor::host::ReadyError::Terminal(ProcStatus::Stopped {
                exit_code, ..
            })) => {
                assert_eq!(exit_code, 7);
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    /// Create a ProcId and a host **backend_addr** channel that the
    /// bootstrap child proc will dial to attach its mailbox to the
    /// host.
    ///
    /// - `proc_id`: logical identity for the child proc (pure name;
    ///   not an OS pid).
    /// - `backend_addr`: a mailbox address served by the **parent
    ///   (host) proc** here; the spawned bootstrap process dials this
    ///   so its messages route via the host.
    async fn make_proc_id_and_backend_addr(
        instance: &hyperactor::Instance<()>,
        _tag: &str,
    ) -> (ProcId, ChannelAddr) {
        // Serve a Unix channel as the "backend_addr" and hook it into
        // this test proc.
        let (backend_addr, rx) = channel::serve(ChannelAddr::any(ChannelTransport::Unix)).unwrap();

        // Route messages arriving on backend_addr into this test
        // proc's mailbox so the bootstrap child can reach the host
        // router.
        instance.proc().clone().serve(rx);

        // We return an arbitrary (but unbound!) unix direct proc id here;
        // it is okay, as we're not testing connectivity.
        let proc_id = ProcId::Direct(ChannelTransport::Unix.any(), "test".to_string());
        (proc_id, backend_addr)
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn bootstrap_handle_terminate_graceful() {
        // Create a root direct-addressed proc + client instance.
        let root =
            hyperactor::Proc::direct(ChannelTransport::Unix.any(), "root".to_string()).unwrap();
        let (instance, _handle) = root.instance("client").unwrap();

        let mgr = BootstrapProcManager::new(BootstrapCommand::test()).unwrap();
        let (proc_id, backend_addr) = make_proc_id_and_backend_addr(&instance, "t_term").await;
        let handle = mgr
            .spawn(
                proc_id.clone(),
                backend_addr.clone(),
                BootstrapProcConfig {
                    create_rank: 0,
                    client_config_override: Attrs::new(),
                },
            )
            .await
            .expect("spawn bootstrap child");

        handle.ready().await.expect("ready");

        let deadline = Duration::from_secs(2);
        match RealClock
            .timeout(
                deadline * 2,
                handle.terminate(&instance, deadline, "test terminate"),
            )
            .await
        {
            Err(_) => panic!("terminate() future hung"),
            Ok(Ok(st)) => {
                match st {
                    ProcStatus::Stopped { exit_code, .. } => {
                        // child called exit(0) on SIGTERM
                        assert_eq!(exit_code, 0, "expected clean exit; got {exit_code}");
                    }
                    ProcStatus::Killed { signal, .. } => {
                        // If the child didn't trap SIGTERM, we'd see
                        // SIGTERM (15) here and indeed, this is what
                        // we see. Since we call
                        // `hyperactor::initialize_with_current_runtime();`
                        // we seem unable to trap `SIGTERM` and
                        // instead folly intercepts:
                        // [0] *** Aborted at 1758850539 (Unix time, try 'date -d @1758850539') ***
                        // [0] *** Signal 15 (SIGTERM) (0x3951c00173692) received by PID 1527420 (pthread TID 0x7f803de66cc0) (linux TID 1527420) (maybe from PID 1521298, UID 234780) (code: 0), stack trace: ***
                        // [0]     @ 000000000000e713 folly::symbolizer::(anonymous namespace)::innerSignalHandler(int, siginfo_t*, void*)
                        // [0]                        ./fbcode/folly/debugging/symbolizer/SignalHandler.cpp:485
                        // It gets worse. When run with
                        // '@fbcode//mode/dev-nosan' it terminates
                        // with a SEGFAULT (metamate says this is a
                        // well known issue at Meta). So, TL;DR I
                        // restore default `SIGTERM` handling after
                        // the test exe has called
                        // `initialize_with_runtime`.
                        assert_eq!(signal, libc::SIGTERM, "expected SIGTERM; got {signal}");
                    }
                    other => panic!("expected Stopped or Killed(SIGTERM); got {other:?}"),
                }
            }
            Ok(Err(e)) => panic!("terminate() failed: {e:?}"),
        }
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn bootstrap_handle_kill_forced() {
        // Root proc + client instance (so the child can dial back).
        let root =
            hyperactor::Proc::direct(ChannelTransport::Unix.any(), "root".to_string()).unwrap();
        let (instance, _handle) = root.instance("client").unwrap();

        let mgr = BootstrapProcManager::new(BootstrapCommand::test()).unwrap();

        // Proc identity + host backend channel the child will dial.
        let (proc_id, backend_addr) = make_proc_id_and_backend_addr(&instance, "t_kill").await;

        // Launch the child bootstrap process.
        let handle = mgr
            .spawn(
                proc_id.clone(),
                backend_addr.clone(),
                BootstrapProcConfig {
                    create_rank: 0,
                    client_config_override: Attrs::new(),
                },
            )
            .await
            .expect("spawn bootstrap child");

        // Wait until the child reports Ready (addr+agent returned via
        // callback).
        handle.ready().await.expect("ready");

        // Force-kill the child and assert we observe a Killed
        // terminal status.
        let deadline = Duration::from_secs(5);
        match RealClock.timeout(deadline, handle.kill()).await {
            Err(_) => panic!("kill() future hung"),
            Ok(Ok(st)) => {
                // We expect a KILLED terminal state.
                match st {
                    ProcStatus::Killed { signal, .. } => {
                        // On Linux this should be SIGKILL (9).
                        assert_eq!(signal, libc::SIGKILL, "expected SIGKILL; got {}", signal);
                    }
                    other => panic!("expected Killed status after kill(); got: {other:?}"),
                }
            }
            Ok(Err(e)) => panic!("kill() failed: {e:?}"),
        }
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn bootstrap_canonical_simple() {
        // SAFETY: unit-test scoped
        unsafe {
            std::env::set_var("HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG", "false");
        }
        // Create an actor instance we'll use to send and receive
        // messages.
        let instance = testing::instance();

        // Configure a ProcessAllocator with the bootstrap binary.
        let mut allocator = ProcessAllocator::new(Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        )));
        // Request a new allocation of procs from the ProcessAllocator.
        let alloc = allocator
            .allocate(AllocSpec {
                extent: extent!(replicas = 1),
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            })
            .await
            .unwrap();

        // Build a HostMesh with explicit OS-process boundaries (per
        // rank):
        //
        // (1) Allocator → bootstrap proc [OS process #1]
        //     `ProcMesh::allocate(..)` starts one OS process per
        //     rank; each runs our runtime and the trampoline actor.
        //
        // (2) Host::serve(..) sets up a Host in the same OS process
        //     (no new process). It binds front/back channels, creates
        //     an in-process service proc (`Proc::new(..)`), and
        //     stores the `BootstrapProcManager` for later spawns.
        //
        // (3) Install HostMeshAgent (still no new OS process).
        //     `host.system_proc().spawn::<HostMeshAgent>("agent",
        //     host).await?` creates the HostMeshAgent actor in that
        //     service proc.
        //
        // (4) Collect & assemble. The trampoline returns a
        //     direct-addressed `ActorRef<HostMeshAgent>`; we collect
        //     one per rank and assemble a `HostMesh`.
        //
        // Note: When the Host is later asked to start a proc
        // (`host.spawn(name)`), it calls `ProcManager::spawn` on the
        // stored `BootstrapProcManager`, which does a
        // `Command::spawn()` to launch a new OS child process for
        // that proc.
        let mut host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
            .await
            .unwrap();

        // Spawn a ProcMesh named "p0" on the host mesh:
        //
        // (1) Each HostMeshAgent (running inside its host's service
        // proc) receives the request.
        //
        // (2) The Host calls into its `BootstrapProcManager::spawn`,
        // which does `Command::spawn()` to launch a brand-new OS
        // process for the proc.
        //
        // (3) Inside that new process, bootstrap runs and a
        // `ProcMeshAgent` is started to manage it.
        //
        // (4) We collect the per-host procs into a `ProcMesh` and
        // return it.
        let proc_mesh = host_mesh
            .spawn(&instance, "p0", Extent::unity())
            .await
            .unwrap();

        // Note: There is no support for status() in v1.
        // assert!(proc_mesh.status(&instance).await.is_err());
        // let proc_ref = proc_mesh.values().next().expect("one proc");
        // assert!(proc_ref.status(&instance).await.unwrap(), "proc should be alive");

        // Spawn an `ActorMesh<TestActor>` named "a0" on the proc mesh:
        //
        // (1) For each proc (already running in its own OS process),
        // the `ProcMeshAgent` receives the request.
        //
        // (2) It spawns a `TestActor` inside that existing proc (no
        // new OS process).
        //
        // (3) The per-proc actors are collected into an
        // `ActorMesh<TestActor>` and returned.
        let actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(&instance, "a0", &()).await.unwrap();

        // Open a fresh port on the client instance and send a
        // GetActorId message to the actor mesh. Each TestActor will
        // reply with its actor ID to the bound port. Receive one
        // reply and assert it matches the ID of the (single) actor in
        // the mesh.
        let (port, mut rx) = instance.mailbox().open_port();
        actor_mesh
            .cast(&instance, testactor::GetActorId(port.bind()))
            .unwrap();
        let (got_id, _seq) = rx.recv().await.unwrap();
        assert_eq!(
            got_id,
            actor_mesh.values().next().unwrap().actor_id().clone()
        );

        // **Important**: If we don't shutdown the hosts, the
        // BootstrapProcManager's won't send SIGKILLs to their spawned
        // children and there will be orphans left behind.
        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }

    /// Same as `bootstrap_canonical_simple` but using the systemd
    /// launcher backend.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn bootstrap_canonical_simple_systemd_launcher() {
        // Acquire exclusive config lock and select systemd launcher.
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(MESH_PROC_LAUNCHER_KIND, "systemd".to_string());

        // Create an actor instance we'll use to send and receive
        // messages.
        let instance = testing::instance();

        // Configure a ProcessAllocator with the bootstrap binary.
        let mut allocator = ProcessAllocator::new(Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        )));
        // Request a new allocation of procs from the
        // ProcessAllocator.
        let alloc = allocator
            .allocate(AllocSpec {
                extent: extent!(replicas = 1),
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            })
            .await
            .unwrap();

        // Build a HostMesh (see bootstrap_canonical_simple for
        // detailed comments).
        let mut host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
            .await
            .unwrap();

        // Spawn a ProcMesh named "p0" on the host mesh.
        let proc_mesh = host_mesh
            .spawn(&instance, "p0", Extent::unity())
            .await
            .unwrap();

        // Spawn an `ActorMesh<TestActor>` named "a0" on the proc
        // mesh.
        let actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(&instance, "a0", &()).await.unwrap();

        // Send a GetActorId message and verify we get a response.
        let (port, mut rx) = instance.mailbox().open_port();
        actor_mesh
            .cast(&instance, testactor::GetActorId(port.bind()))
            .unwrap();
        let (got_id, _) = rx.recv().await.unwrap();
        assert_eq!(
            got_id,
            actor_mesh.values().next().unwrap().actor_id().clone()
        );

        // Capture the proc_id and expected unit name before shutdown.
        use crate::proc_launcher::SystemdProcLauncher;
        use crate::systemd::SystemdManagerProxy;
        use crate::systemd::SystemdUnitProxy;

        let proc_id = proc_mesh.proc_ids().next().expect("one proc");
        let expected_unit = SystemdProcLauncher::unit_name(&proc_id);

        // Shutdown cleanly.
        //
        // Contract: after shutdown, procs are no longer running and
        // the mesh is quiescent. We intentionally do NOT assert that
        // the systemd transient unit disappears. With
        // RemainAfterExit=true, systemd may keep the unit around for
        // observation and GC it later according to its own policy;
        // that persistence is not considered a leak.
        host_mesh.shutdown(&instance).await.expect("host shutdown");

        // Liveness check (best-effort): unit may persist, but should
        // not remain running. With RemainAfterExit=true, systemd may
        // keep the unit around in active/exited after the process has
        // terminated. That is not a leak. We only require that the
        // unit is not running after shutdown.
        let conn = zbus::Connection::session().await.expect("D-Bus session");
        let manager = SystemdManagerProxy::new(&conn)
            .await
            .expect("manager proxy");

        let mut ok = false;
        for _ in 0..100 {
            match manager.get_unit(&expected_unit).await {
                Err(_) => {
                    // Unit already gone: fine.
                    ok = true;
                    break;
                }
                Ok(path) => {
                    if let Ok(unit) = SystemdUnitProxy::builder(&conn)
                        .path(path)
                        .unwrap()
                        .build()
                        .await
                    {
                        let active = unit.active_state().await.unwrap_or_default();
                        let sub = unit.sub_state().await.unwrap_or_default();
                        // Fail only if still running; any other state
                        // is acceptable.
                        if !(active == "active" && sub == "running") && active != "activating" {
                            ok = true;
                            break;
                        }
                    }
                }
            }
            RealClock.sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            ok,
            "unit should be gone or quiescent (not running) after shutdown"
        );
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_host_bootstrap() {
        use crate::host_mesh::mesh_agent::GetLocalProcClient;
        use crate::mesh_agent::NewClientInstanceClient;

        // Create a local instance just to call the local bootstrap actor.
        // We should find a way to avoid this for local handles.
        let temp_proc = Proc::local();
        let (temp_instance, _) = temp_proc.instance("temp").unwrap();

        let handle = host(
            any_addr_for_test(),
            Some(BootstrapCommand::test()),
            None,
            false,
        )
        .await
        .unwrap();

        let local_proc = handle.get_local_proc(&temp_instance).await.unwrap();
        let _local_instance = local_proc
            .new_client_instance(&temp_instance)
            .await
            .unwrap();
    }
}
