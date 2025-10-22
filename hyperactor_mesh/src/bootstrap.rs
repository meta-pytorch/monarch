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
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use base64::prelude::*;
use futures::StreamExt;
use futures::stream;
use humantime::format_duration;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::attrs::Attrs;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::config::CONFIG;
use hyperactor::config::ConfigAttr;
use hyperactor::config::global as config;
use hyperactor::declare_attrs;
use hyperactor::host;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::ProcHandle;
use hyperactor::host::ProcManager;
use hyperactor::host::TerminateSummary;
use hyperactor::mailbox::MailboxServer;
use hyperactor::proc::Proc;
use serde::Deserialize;
use serde::Serialize;
use tokio::process::Child;
use tokio::process::ChildStderr;
use tokio::process::ChildStdout;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::logging::OutputTarget;
use crate::logging::StreamFwder;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::v1;
use crate::v1::host_mesh::mesh_agent::HostAgentMode;
use crate::v1::host_mesh::mesh_agent::HostMeshAgent;

declare_attrs! {
    /// If enabled (default), bootstrap child processes install
    /// `PR_SET_PDEATHSIG(SIGKILL)` so the kernel reaps them if the
    /// parent dies unexpectedly. This is a **production safety net**
    /// against leaked children; tests usually disable it via
    /// `std::env::set_var("HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG",
    /// "false")`.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG".to_string()),
        py_name: None,
    })
    pub attr MESH_BOOTSTRAP_ENABLE_PDEATHSIG: bool = true;

    /// Maximum number of log lines retained in a proc's stderr/stdout
    /// tail buffer. Used by [`StreamFwder`] when wiring child
    /// pipes. Default: 100
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_TAIL_LOG_LINES".to_string()),
        py_name: None,
    })
    pub attr MESH_TAIL_LOG_LINES: usize = 100;

    /// Maximum number of child terminations to run concurrently
    /// during bulk shutdown. Prevents unbounded spawning of
    /// termination tasks (which could otherwise spike CPU, I/O, or
    /// file descriptor load).
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_TERMINATE_CONCURRENCY".to_string()),
        py_name: None,
    })
    pub attr MESH_TERMINATE_CONCURRENCY: usize = 16;

    /// Per-child grace window for termination. When a shutdown is
    /// requested, the manager sends SIGTERM and waits this long for
    /// the child to exit before escalating to SIGKILL.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_TERMINATE_TIMEOUT".to_string()),
        py_name: None,
    })
    pub attr MESH_TERMINATE_TIMEOUT: Duration = Duration::from_secs(10);
}

pub const BOOTSTRAP_ADDR_ENV: &str = "HYPERACTOR_MESH_BOOTSTRAP_ADDR";
pub const BOOTSTRAP_INDEX_ENV: &str = "HYPERACTOR_MESH_INDEX";
pub const CLIENT_TRACE_ID_ENV: &str = "MONARCH_CLIENT_TRACE_ID";
/// A channel used by each process to receive its own stdout and stderr
/// Because stdout and stderr can only be obtained by the parent process,
/// they need to be streamed back to the process.
pub(crate) const BOOTSTRAP_LOG_CHANNEL: &str = "BOOTSTRAP_LOG_CHANNEL";

/// Messages sent from the process to the allocator. This is an envelope
/// containing the index of the process (i.e., its "address" assigned by
/// the allocator), along with the control message in question.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub(crate) struct Process2Allocator(pub usize, pub Process2AllocatorMessage);

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

async fn exit_if_missed_heartbeat(bootstrap_index: usize, bootstrap_addr: ChannelAddr) {
    let tx = match channel::dial(bootstrap_addr.clone(), "bootstrap-heartbeat".to_string()) {
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

/// Bootstrap configures how a mesh process starts up.
///
/// Both `Proc` and `Host` variants may include an optional
/// configuration snapshot (`hyperactor::config::Attrs`). This
/// snapshot is serialized into the bootstrap payload and made
/// available to the child. Interpretation and application of that
/// snapshot is up to the child process; if omitted, the child falls
/// back to environment/default values.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub enum Bootstrap {
    /// "v1" proc bootstrap
    Proc {
        /// The ProcId of the proc to be bootstrapped.
        proc_id: ProcId,
        /// The backend address to which messages are forwarded.
        /// See [`hyperactor::host`] for channel topology details.
        backend_addr: ChannelAddr,
        /// The callback address used to indicate successful spawning.
        callback_addr: ChannelAddr,
        /// Optional config snapshot (`hyperactor::config::Attrs`)
        /// captured by the parent. If present, the child installs it
        /// as the `Runtime` layer so the parent's effective config
        /// takes precedence over Env/File/Defaults.
        config: Option<Attrs>,
    },

    /// Host bootstrap. This sets up a new `Host`, managed by a
    /// [`crate::v1::host_mesh::mesh_agent::HostMeshAgent`].
    Host {
        /// The address on which to serve the host.
        addr: ChannelAddr,
        /// If specified, use the provided command instead of
        /// [`BootstrapCommand::current`].
        command: Option<BootstrapCommand>,
        /// Optional config snapshot (`hyperactor::config::Attrs`)
        /// captured by the parent. If present, the child installs it
        /// as the `Runtime` layer so the parent’s effective config
        /// takes precedence over Env/File/Defaults.
        config: Option<Attrs>,
    },

    #[default]
    V0ProcMesh, // pass through to the v0 allocator
}

impl Bootstrap {
    /// Serialize the mode into a environment-variable-safe string by
    /// base64-encoding its JSON representation.
    fn to_env_safe_string(&self) -> v1::Result<String> {
        Ok(BASE64_STANDARD.encode(serde_json::to_string(&self)?))
    }

    /// Deserialize the mode from the representation returned by [`to_env_safe_string`].
    fn from_env_safe_string(str: &str) -> v1::Result<Self> {
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
                config,
            } => {
                if let Some(attrs) = config {
                    config::set(config::Source::ClientOverride, attrs);
                    tracing::debug!("bootstrap: installed ClientOverride config snapshot (Proc)");
                } else {
                    tracing::debug!("bootstrap: no config snapshot provided (Proc)");
                }

                if hyperactor::config::global::get(MESH_BOOTSTRAP_ENABLE_PDEATHSIG) {
                    // Safety net: normal shutdown is via
                    // `host_mesh.shutdown(&instance)`; PR_SET_PDEATHSIG
                    // is a last-resort guard against leaks if that
                    // protocol is bypassed.
                    let _ = install_pdeathsig_kill();
                } else {
                    eprintln!("(bootstrap) PDEATHSIG disabled via config");
                }

                let result =
                    host::spawn_proc(proc_id, backend_addr, callback_addr, |proc| async move {
                        ProcMeshAgent::boot_v1(proc).await
                    })
                    .await;
                match result {
                    Ok(_proc) => halt().await,
                    Err(e) => e.into(),
                }
            }
            Bootstrap::Host {
                addr,
                command,
                config,
            } => {
                if let Some(attrs) = config {
                    config::set(config::Source::Runtime, attrs);
                    tracing::debug!("bootstrap: installed Runtime config snapshot (Host)");
                } else {
                    tracing::debug!("bootstrap: no config snapshot provided (Host)");
                }

                let command = match command {
                    Some(command) => command,
                    None => ok!(BootstrapCommand::current()),
                };
                let manager = BootstrapProcManager::new(command);
                let (host, _handle) = ok!(Host::serve(manager, addr).await);
                let addr = host.addr().clone();
                let host_mesh_agent = ok!(host
                    .system_proc()
                    .clone()
                    .spawn::<HostMeshAgent>("agent", HostAgentMode::Process(host))
                    .await);

                tracing::info!(
                    "serving host at {}, agent: {}",
                    addr,
                    host_mesh_agent.bind::<HostMeshAgent>()
                );
                halt().await
            }
            Bootstrap::V0ProcMesh => bootstrap_v0_proc_mesh().await,
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
        // SAFETY: Calling into libc; does not dereference memory, just
        // asks the kernel to deliver SIGKILL on parent death.
        let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_int) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // Race-close: if the parent died between our exec and prctl(),
    // we won't get a signal, so detect that and exit now.
    //
    // If getppid() == 1, we've already been reparented (parent gone).
    // SAFETY: `getppid()` is a simple libc syscall returning the
    // parent PID; it has no side effects and does not touch memory.
    if unsafe { libc::getppid() } == 1 {
        std::process::exit(0);
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
    /// (Process-level: `pid` is known; Proc-level: bootstrap
    /// may still be running.)
    Running { pid: u32, started_at: SystemTime },
    /// Ready means bootstrap has completed and the proc is serving.
    /// (Process-level: `pid` is known; Proc-level: bootstrap
    /// completed.)
    Ready {
        pid: u32,
        started_at: SystemTime,
        addr: ChannelAddr,
        agent: ActorRef<ProcMeshAgent>,
    },
    /// A stop has been requested (SIGTERM, graceful shutdown, etc.),
    /// but the OS process has not yet fully exited. (Proc-level:
    /// shutdown in progress; Process-level: still running.)
    Stopping { pid: u32, started_at: SystemTime },
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
            ProcStatus::Running { pid, started_at } => {
                let uptime = started_at
                    .elapsed()
                    .map(|d| format!(" up {}", format_duration(d)))
                    .unwrap_or_default();
                write!(f, "Running[{pid}]{uptime}")
            }
            ProcStatus::Ready {
                pid,
                started_at,
                addr,
                ..
            } => {
                let uptime = started_at
                    .elapsed()
                    .map(|d| format!(" up {}", format_duration(d)))
                    .unwrap_or_default();
                write!(f, "Ready[{pid}] at {addr}{uptime}")
            }
            ProcStatus::Stopping { pid, started_at } => {
                let uptime = started_at
                    .elapsed()
                    .map(|d| format!(" up {}", format_duration(d)))
                    .unwrap_or_default();
                write!(f, "Stopping[{pid}]{uptime}")
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
    /// Underlying OS child handle. Held only until the exit monitor
    /// claims it (consumed by `wait()` there). Not relied on for
    /// teardown; manager `Drop` handles best-effort SIGKILL.
    child: Arc<std::sync::Mutex<Option<Child>>>,
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
            .field("child", &"<child>")
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
// - If you need the child PID during a transition, snapshot it
//   *before* calling `transition`.
impl BootstrapProcHandle {
    /// Construct a new [`BootstrapProcHandle`] for a freshly spawned
    /// OS process hosting a proc.
    ///
    /// - Initializes the status to [`ProcStatus::Starting`] since the
    ///   child process has been created but not yet confirmed running.
    /// - Wraps the provided [`Child`] handle in an
    ///   `Arc<Mutex<Option<Child>>>` so lifecycle methods (`wait`,
    ///   `terminate`, etc.) can consume it later.
    ///
    /// This is the canonical entry point used by
    /// `BootstrapProcManager` when it launches a proc into a new
    /// process.
    pub fn new(proc_id: ProcId, child: Child) -> Self {
        let (tx, rx) = watch::channel(ProcStatus::Starting);
        Self {
            proc_id,
            status: Arc::new(std::sync::Mutex::new(ProcStatus::Starting)),
            child: Arc::new(std::sync::Mutex::new(Some(child))),
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

    /// Return the OS process ID (`pid`) for this proc.
    ///
    /// If the proc is currently `Running`, this returns the cached
    /// pid even if the `Child` has already been taken by the exit
    /// monitor. Otherwise, it falls back to `Child::id()` if the
    /// handle is still present. Returns `None` once the proc has
    /// exited or if the handle has been consumed.
    #[inline]
    pub fn pid(&self) -> Option<u32> {
        match *self.status.lock().expect("status mutex poisoned") {
            ProcStatus::Running { pid, .. } | ProcStatus::Ready { pid, .. } => Some(pid),
            _ => self
                .child
                .lock()
                .expect("child mutex poisoned")
                .as_ref()
                .and_then(|c| c.id()),
        }
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
    /// Called internally once the child OS process has been spawned
    /// and we can observe a valid `pid`. Records the `pid` and the
    /// `started_at` timestamp so that callers can query them later
    /// via [`BootstrapProcHandle::status`] or
    /// [`BootstrapProcHandle::pid`].
    ///
    /// This is a best-effort marker: it reflects that the process
    /// exists at the OS level, but does not guarantee that the proc
    /// has completed bootstrap or is fully ready.
    pub(crate) fn mark_running(&self, pid: u32, started_at: SystemTime) -> bool {
        self.transition(|st| match *st {
            ProcStatus::Starting => {
                *st = ProcStatus::Running { pid, started_at };
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
    /// This records the process ID, start time, listening address,
    /// and agent once the proc has successfully started and is ready
    /// to serve.
    ///
    /// Returns `true` if the transition succeeded (from `Starting` or
    /// `Running`), or `false` if the current state did not allow
    /// moving to `Ready`. In the latter case the state is left
    /// unchanged and a warning is logged.
    pub(crate) fn mark_ready(
        &self,
        pid: u32,
        started_at: SystemTime,
        addr: ChannelAddr,
        agent: ActorRef<ProcMeshAgent>,
    ) -> bool {
        self.transition(|st| match st {
            ProcStatus::Starting | ProcStatus::Running { .. } => {
                *st = ProcStatus::Ready {
                    pid,
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
        // Avoid taking out additional locks in .transition().
        let child_pid = self.child_pid_snapshot();
        let now = hyperactor::clock::RealClock.system_time_now();

        self.transition(|st| match *st {
            ProcStatus::Running { pid, started_at } => {
                *st = ProcStatus::Stopping { pid, started_at };
                true
            }
            ProcStatus::Ready {
                pid, started_at, ..
            } => {
                *st = ProcStatus::Stopping { pid, started_at };
                true
            }
            ProcStatus::Starting => {
                if let Some(pid) = child_pid {
                    *st = ProcStatus::Stopping {
                        pid,
                        started_at: now,
                    };
                    true
                } else {
                    false
                }
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

    /// Best-effort, non-blocking snapshot of the child's PID. Locks
    /// only the `child` mutex and never touches `status`.
    fn child_pid_snapshot(&self) -> Option<u32> {
        self.child
            .lock()
            .expect("child mutex poisoned")
            .as_ref()
            .and_then(|c| c.id())
    }

    /// Try to fetch a PID we can signal. Prefer cached PID from
    /// status; fall back to Child::id() if still present. None once
    /// terminal or if monitor already consumed the Child and we never
    /// cached a pid.
    fn signalable_pid(&self) -> Option<i32> {
        match &*self.status.lock().expect("status mutex poisoned") {
            ProcStatus::Running { pid, .. }
            | ProcStatus::Ready { pid, .. }
            | ProcStatus::Stopping { pid, .. } => Some(*pid as i32),
            _ => self
                .child
                .lock()
                .expect("child mutex poisoned")
                .as_ref()
                .and_then(|c| c.id())
                .map(|p| p as i32),
        }
    }

    /// Send a signal to the pid. Returns Ok(()) if delivered. If
    /// ESRCH (no such process), we treat that as "already gone" and
    /// return Ok(()) so callers can proceed to observe the terminal
    /// state through the monitor.
    fn send_signal(pid: i32, sig: i32) -> Result<(), anyhow::Error> {
        // SAFETY: Sending a POSIX signal; does not dereference
        // memory.
        let rc = unsafe { libc::kill(pid, sig) };
        if rc == 0 {
            Ok(())
        } else {
            let e = std::io::Error::last_os_error();
            if let Some(libc::ESRCH) = e.raw_os_error() {
                // Process already gone; proceed to wait/observe
                // terminal state.
                Ok(())
            } else {
                Err(anyhow::anyhow!("kill({pid}, {sig}) failed: {e}"))
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
    /// - Sends `SIGTERM` to the process.
    /// - Waits up to `timeout` for it to exit cleanly.
    /// - Escalates to `SIGKILL` if still alive, then waits a short
    ///   hard-coded grace period (`HARD_WAIT_AFTER_KILL`) to ensure
    ///   the process is reaped.
    ///
    /// If the process was already in a terminal state when called,
    /// returns [`TerminateError::AlreadyTerminated`].
    ///
    /// # Notes
    /// - This does *not* attempt a graceful proc-level stop via
    ///   `ProcMeshAgent` or other actor messaging. That integration
    ///   will come later once proc-level control is wired up.
    /// - Errors may also be returned if the process PID cannot be
    ///   determined, if signal delivery fails, or if the status
    ///   channel is closed unexpectedly.
    ///
    /// # Parameters
    /// - `timeout`: Grace period to wait after `SIGTERM` before
    ///   escalating.
    ///
    /// # Returns
    /// - `Ok(ProcStatus)` if the process exited during the
    ///   termination sequence.
    /// - `Err(TerminateError)` if already exited, signaling failed,
    ///   or the channel was lost.
    async fn terminate(
        &self,
        timeout: Duration,
    ) -> Result<ProcStatus, hyperactor::host::TerminateError<Self::TerminalStatus>> {
        const HARD_WAIT_AFTER_KILL: Duration = Duration::from_secs(5);

        // If already terminal, return that.
        let st0 = self.status();
        if st0.is_exit() {
            tracing::debug!(?st0, "terminate(): already terminal");
            return Err(hyperactor::host::TerminateError::AlreadyTerminated(st0));
        }

        // Find a PID to signal.
        let pid = self.signalable_pid().ok_or_else(|| {
            let st = self.status();
            tracing::warn!(?st, "terminate(): no signalable pid");
            hyperactor::host::TerminateError::Io(anyhow::anyhow!(
                "no signalable pid (state: {:?})",
                st
            ))
        })?;

        // Best-effort mark "Stopping" (ok if state races).
        let _ = self.mark_stopping();

        // Send SIGTERM (ESRCH is treated as "already gone").
        tracing::info!(pid, ?timeout, "terminate(): sending SIGTERM");
        if let Err(e) = Self::send_signal(pid, libc::SIGTERM) {
            tracing::warn!(pid, error=%e, "terminate(): SIGTERM delivery failed");
            return Err(hyperactor::host::TerminateError::Io(e));
        }
        tracing::debug!(pid, "terminate(): SIGTERM sent");

        // Wait up to the timeout for a terminal state.
        match RealClock.timeout(timeout, self.wait_inner()).await {
            Ok(st) if st.is_exit() => {
                tracing::info!(pid, ?st, "terminate(): exited after SIGTERM");
                Ok(st)
            }
            Ok(non_exit) => {
                // wait_inner() should only resolve terminal; treat as
                // channel issue.
                tracing::warn!(pid, ?non_exit, "terminate(): wait returned non-terminal");
                Err(hyperactor::host::TerminateError::ChannelClosed)
            }
            Err(_elapsed) => {
                // Escalate to SIGKILL
                tracing::warn!(pid, "terminate(): timeout; escalating to SIGKILL");
                if let Some(pid2) = self.signalable_pid() {
                    if let Err(e) = Self::send_signal(pid2, libc::SIGKILL) {
                        tracing::warn!(pid=pid2, error=%e, "terminate(): SIGKILL delivery failed");
                        return Err(hyperactor::host::TerminateError::Io(e));
                    }
                    tracing::info!(pid = pid2, "terminate(): SIGKILL sent");
                } else {
                    tracing::warn!("terminate(): lost pid before SIGKILL escalation");
                }
                // Hard bound after KILL so we can't hang forever.
                match RealClock
                    .timeout(HARD_WAIT_AFTER_KILL, self.wait_inner())
                    .await
                {
                    Ok(st) if st.is_exit() => {
                        tracing::info!(?st, "terminate(): exited after SIGKILL");
                        Ok(st)
                    }
                    other => {
                        tracing::warn!(
                            ?other,
                            "terminate(): post-KILL wait did not yield terminal"
                        );
                        Err(hyperactor::host::TerminateError::ChannelClosed)
                    }
                }
            }
        }
    }

    /// Forcibly kill the underlying OS process with `SIGKILL`.
    ///
    /// This bypasses any graceful shutdown semantics and immediately
    /// delivers a non-catchable `SIGKILL` to the child. It is
    /// intended as a last-resort termination mechanism when
    /// `terminate()` fails or when no grace period is desired.
    ///
    /// # Behavior
    /// - If the process was already in a terminal state, returns
    ///   [`TerminateError::AlreadyTerminated`].
    /// - Otherwise attempts to send `SIGKILL` to the current PID.
    /// - Then waits for the exit monitor to observe a terminal state.
    ///
    /// # Notes
    /// - This is strictly an **OS-level kill**. It does *not* attempt
    ///   proc-level shutdown via `ProcMeshAgent` or actor messages.
    ///   That integration will be layered in later.
    /// - Errors may be returned if the PID cannot be determined, if
    ///   signal delivery fails, or if the status channel closes
    ///   unexpectedly.
    ///
    /// # Returns
    /// - `Ok(ProcStatus)` if the process exited after `SIGKILL`.
    /// - `Err(TerminateError)` if already exited, signaling failed,
    ///   or the channel was lost.
    async fn kill(
        &self,
    ) -> Result<ProcStatus, hyperactor::host::TerminateError<Self::TerminalStatus>> {
        // NOTE: This is a pure OS-level kill (SIGKILL) of the child
        // process. It bypasses any proc-level shutdown semantics.
        //
        // Future work: layer in proc-level stop semantics through
        // actor messages once those are implemented.

        // If already terminal, return that.
        let st0 = self.status();
        if st0.is_exit() {
            return Err(hyperactor::host::TerminateError::AlreadyTerminated(st0));
        }

        // Try to get a PID and send SIGKILL.
        let pid = self.signalable_pid().ok_or_else(|| {
            hyperactor::host::TerminateError::Io(anyhow::anyhow!(
                "no signalable pid (state: {:?})",
                self.status()
            ))
        })?;

        if let Err(e) = Self::send_signal(pid, libc::SIGKILL) {
            return Err(hyperactor::host::TerminateError::Io(e));
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

    /// Bootstrap command used for testing, invoking the Buck-built
    /// `monarch/hyperactor_mesh/bootstrap` binary.
    ///
    /// Intended for integration tests where we need to spawn real
    /// bootstrap processes under proc manager control. Not available
    /// outside of test builds.
    #[cfg(test)]
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

/// A process manager for launching and supervising **bootstrap
/// processes** (via the [`bootstrap`] entry point).
///
/// `BootstrapProcManager` is the host-side runtime for external procs
/// in a hyperactor mesh. It spawns the configured bootstrap binary,
/// forwards each child's stdout/stderr into the logging channel,
/// tracks lifecycle state through [`BootstrapProcHandle`] /
/// [`ProcStatus`], and ensures best-effort teardown on drop.
///
/// Internally it maintains two maps:
/// - [`children`]: async map from [`ProcId`] to
///   [`BootstrapProcHandle`], used for lifecycle queries (`status`) and
///   exit monitoring.
/// - [`pid_table`]: synchronous map from [`ProcId`] to raw PIDs, used
///   in [`Drop`] to synchronously send `SIGKILL` to any still-running
///   children.
///
/// Together these provide both a queryable, real-time status surface
/// and a deterministic cleanup path, so no child processes are left
/// orphaned if the manager itself is dropped.
#[derive(Debug)]
pub struct BootstrapProcManager {
    /// The command specification used to bootstrap new processes.
    command: BootstrapCommand,
    /// Async registry of running children, keyed by [`ProcId`]. Holds
    /// [`BootstrapProcHandle`]s so callers can query or monitor
    /// status.
    children: Arc<tokio::sync::Mutex<HashMap<ProcId, BootstrapProcHandle>>>,
    /// Synchronous table of raw PIDs, keyed by [`ProcId`]. Used
    /// exclusively in the [`Drop`] impl to send `SIGKILL` without
    /// needing async context.
    pid_table: Arc<std::sync::Mutex<HashMap<ProcId, u32>>>,
    /// FileMonitor that aggregates logs from all children.
    /// None if file monitor creation failed.
    file_appender: Option<Arc<crate::logging::FileAppender>>,
}

impl Drop for BootstrapProcManager {
    /// Drop implementation for [`BootstrapProcManager`].
    ///
    /// Ensures that when the manager itself is dropped, any child
    /// processes it spawned are also terminated. This is a
    /// best-effort cleanup mechanism: the manager iterates over its
    /// recorded PID table and sends `SIGKILL` to each one.
    ///
    /// Notes:
    /// - Uses a synchronous `Mutex` so it can be locked safely in
    ///   `Drop` without async context.
    /// - Safety: sending `SIGKILL` is low-level but safe here because
    ///   it only instructs the OS to terminate the process. The only
    ///   risk is PID reuse, in which case an unrelated process might
    ///   be signaled. This is accepted for shutdown semantics.
    /// - This makes `BootstrapProcManager` robust against leaks:
    ///   dropping it should not leave stray child processes running.
    fn drop(&mut self) {
        if let Ok(table) = self.pid_table.lock() {
            for (proc_id, pid) in table.iter() {
                // SAFETY: We own these PIDs because they were spawned and recorded
                // by this manager. Sending SIGKILL is safe here since it does not
                // dereference any memory; it only instructs the OS to terminate the
                // process. The worst-case outcome is that the PID has already exited
                // and been reused, in which case the signal may affect an unrelated
                // process. We accept that risk in Drop semantics because this path
                // is only used for cleanup at shutdown.
                unsafe {
                    libc::kill(*pid as i32, libc::SIGKILL);
                }
                tracing::info!(
                    "BootstrapProcManager::drop: sent SIGKILL to pid {} for {:?}",
                    pid,
                    proc_id
                );
            }
        }
    }
}

impl BootstrapProcManager {
    /// Construct a new [`BootstrapProcManager`] that will launch
    /// procs using the given bootstrap command specification.
    ///
    /// This is the general entry point when you want to manage procs
    /// backed by a specific binary path (e.g. a bootstrap
    /// trampoline).
    pub(crate) fn new(command: BootstrapCommand) -> Self {
        let log_monitor = match crate::logging::FileAppender::new() {
            Some(fm) => {
                tracing::info!("log monitor created successfully");
                Some(Arc::new(fm))
            }
            None => {
                tracing::warn!("failed to create log monitor");
                None
            }
        };

        Self {
            command,
            children: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            pid_table: Arc::new(std::sync::Mutex::new(HashMap::new())),
            file_appender: log_monitor,
        }
    }

    /// The bootstrap command used to launch processes.
    pub fn command(&self) -> &BootstrapCommand {
        &self.command
    }

    /// Return the current [`ProcStatus`] for the given [`ProcId`], if
    /// the proc is known to this manager.
    ///
    /// This querprocies the live [`BootstrapProcHandle`] stored in the
    /// manager's internal map. It provides an immediate snapshot of
    /// lifecycle state (`Starting`, `Running`, `Stopping`, `Stopped`,
    /// etc.).
    ///
    /// Returns `None` if the manager has no record of the proc (e.g.
    /// never spawned here, or entry already removed).
    pub async fn status(&self, proc_id: &ProcId) -> Option<ProcStatus> {
        self.children.lock().await.get(proc_id).map(|h| h.status())
    }

    fn spawn_exit_monitor(&self, proc_id: ProcId, handle: BootstrapProcHandle) {
        let pid_table = Arc::clone(&self.pid_table);

        let maybe_child = {
            let mut guard = handle.child.lock().expect("child mutex");
            let taken = guard.take();
            debug_assert!(guard.is_none(), "no Child should remain in handles");
            taken
        };

        let Some(mut child) = maybe_child else {
            tracing::debug!("proc {proc_id}: child was already taken; skipping wait");
            return;
        };

        tokio::spawn(async move {
            let wait_res = child.wait().await;

            let mut stderr_tail: Vec<String> = Vec::new();
            let (stdout_mon, stderr_mon) = handle.take_stream_monitors();

            if let Some(t) = stderr_mon {
                let (lines, _bytes) = t.abort().await;
                stderr_tail = lines;
            }
            if let Some(t) = stdout_mon {
                let (_lines, _bytes) = t.abort().await;
            }

            match wait_res {
                Ok(status) => {
                    if let Some(sig) = status.signal() {
                        let _ = handle.mark_killed(sig, status.core_dumped());
                        if let Ok(mut table) = pid_table.lock() {
                            table.remove(&proc_id);
                        }
                        if stderr_tail.is_empty() {
                            tracing::debug!("proc {proc_id} killed by signal {sig}");
                        } else {
                            let tail = stderr_tail.join("\n");
                            tracing::debug!(
                                "proc {proc_id} killed by signal {sig}; stderr tail:\n{tail}"
                            );
                        }
                    } else if let Some(code) = status.code() {
                        let _ = handle.mark_stopped(code, stderr_tail.clone());
                        if let Ok(mut table) = pid_table.lock() {
                            table.remove(&proc_id);
                        }
                        let tail_str = if stderr_tail.is_empty() {
                            None
                        } else {
                            Some(stderr_tail.join("\n"))
                        };
                        if code == 0 {
                            tracing::debug!(%proc_id, exit_code = code, tail = tail_str.as_deref(), "proc exited");
                        } else {
                            tracing::info!(%proc_id, exit_code = code, tail = tail_str.as_deref(), "proc exited");
                        }
                    } else {
                        debug_assert!(
                            false,
                            "unreachable: process terminated with neither signal nor exit code"
                        );
                        tracing::error!(
                            "proc {proc_id}: unreachable exit status (no code, no signal)"
                        );
                        let _ = handle.mark_failed("process exited with unknown status");
                        if let Ok(mut table) = pid_table.lock() {
                            table.remove(&proc_id);
                        }
                        if stderr_tail.is_empty() {
                            tracing::warn!("proc {proc_id} unknown exit");
                        } else {
                            let tail = stderr_tail.join("\n");
                            tracing::warn!("proc {proc_id} unknown exit; stderr tail:\n{tail}");
                        }
                    }
                }
                Err(e) => {
                    let _ = handle.mark_failed(format!("wait_inner() failed: {e}"));
                    if let Ok(mut table) = pid_table.lock() {
                        table.remove(&proc_id);
                    }
                    if stderr_tail.is_empty() {
                        tracing::info!("proc {proc_id} wait failed");
                    } else {
                        let tail = stderr_tail.join("\n");
                        tracing::info!("proc {proc_id} wait failed; stderr tail:\n{tail}");
                    }
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
    async fn spawn(
        &self,
        proc_id: ProcId,
        backend_addr: ChannelAddr,
        config: BootstrapProcConfig,
    ) -> Result<Self::Handle, HostError> {
        let (callback_addr, mut callback_rx) = channel::serve(
            ChannelAddr::any(ChannelTransport::Unix),
            "bootstrap-spawn".to_string(),
        )?;

        let mode = Bootstrap::Proc {
            proc_id: proc_id.clone(),
            backend_addr,
            callback_addr,
            config: Some(config.client_config_override),
        };
        let mut cmd = Command::new(&self.command.program);
        if let Some(arg0) = &self.command.arg0 {
            cmd.arg0(arg0);
        }
        for arg in &self.command.args {
            cmd.arg(arg);
        }
        for (k, v) in &self.command.env {
            cmd.env(k, v);
        }
        cmd.env(
            "HYPERACTOR_MESH_BOOTSTRAP_MODE",
            mode.to_env_safe_string()
                .map_err(|e| HostError::ProcessConfigurationFailure(proc_id.clone(), e.into()))?,
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

        let log_channel = ChannelAddr::any(ChannelTransport::Unix);
        cmd.env(BOOTSTRAP_LOG_CHANNEL, log_channel.to_string());
        let mut child = cmd
            .spawn()
            .map_err(|e| HostError::ProcessSpawnFailure(proc_id.clone(), e))?;
        let pid = child.id().unwrap_or_default();
        let stdout: ChildStdout = child.stdout.take().expect("stdout piped but missing");
        let stderr: ChildStderr = child.stderr.take().expect("stderr piped but missing");

        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        if let Some(pid) = handle.pid() {
            handle.mark_running(pid, hyperactor::clock::RealClock.system_time_now());
            if let Ok(mut table) = self.pid_table.lock() {
                table.insert(proc_id.clone(), pid);
            }
        }

        let tail_size = hyperactor::config::global::get(MESH_TAIL_LOG_LINES);

        // Get FileMonitor addresses if available
        let file_monitor_stdout_addr = self
            .file_appender
            .as_ref()
            .map(|fm| fm.addr_for(OutputTarget::Stdout));
        let file_monitor_stderr_addr = self
            .file_appender
            .as_ref()
            .map(|fm| fm.addr_for(OutputTarget::Stderr));

        // Create StreamFwders with FileMonitor addresses
        let stdout_monitor = StreamFwder::start(
            stdout,
            file_monitor_stdout_addr,
            OutputTarget::Stdout,
            tail_size,
            log_channel.clone(),
            pid,
            config.create_rank,
        );

        let stderr_monitor = StreamFwder::start(
            stderr,
            file_monitor_stderr_addr,
            OutputTarget::Stderr,
            tail_size,
            log_channel.clone(),
            pid,
            config.create_rank,
        );
        handle.set_stream_monitors(Some(stdout_monitor), Some(stderr_monitor));

        // Retain handle for lifecycle mgt.
        {
            let mut children = self.children.lock().await;
            children.insert(proc_id.clone(), handle.clone());
        }

        // Kick off an exit monitor that updates ProcStatus when the
        // OS process ends
        self.spawn_exit_monitor(proc_id.clone(), handle.clone());

        let h = handle.clone();
        let pid_table = Arc::clone(&self.pid_table);
        tokio::spawn(async move {
            match callback_rx.recv().await {
                Ok((addr, agent)) => {
                    let pid = match h.pid() {
                        Some(p) => p,
                        None => {
                            tracing::warn!("mark_ready called with missing pid; using 0");
                            0
                        }
                    };
                    let started_at = RealClock.system_time_now();
                    let _ = h.mark_ready(pid, started_at, addr, agent);
                }
                Err(e) => {
                    // Child never called back; record failure.
                    let _ = h.mark_failed(format!("bootstrap callback failed: {e}"));
                    // Cleanup pid table entry if it was set.
                    if let Ok(mut table) = pid_table.lock() {
                        table.remove(&proc_id);
                    }
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
        proc: &ProcId,
        timeout: Duration,
    ) -> Result<(Vec<ActorId>, Vec<ActorId>), anyhow::Error> {
        // Snapshot to avoid holding the lock across awaits.
        let proc_handle: Option<BootstrapProcHandle> = {
            let mut guard = self.children.lock().await;
            guard.remove(proc)
        };

        if let Some(h) = proc_handle {
            h.terminate(timeout)
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
    async fn terminate_all(&self, timeout: Duration, max_in_flight: usize) -> TerminateSummary {
        // Snapshot to avoid holding the lock across awaits.
        let handles: Vec<BootstrapProcHandle> = {
            let guard = self.children.lock().await;
            guard.values().cloned().collect()
        };

        let attempted = handles.len();
        let mut ok = 0usize;

        let results = stream::iter(handles.into_iter().map(|h| async move {
            match h.terminate(timeout).await {
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
async fn bootstrap_v0_proc_mesh() -> anyhow::Error {
    pub async fn go() -> Result<(), anyhow::Error> {
        let procs = Arc::new(tokio::sync::Mutex::new(Vec::<Proc>::new()));
        let procs_for_cleanup = procs.clone();
        let _cleanup_guard = hyperactor::register_signal_cleanup_scoped(Box::pin(async move {
            for proc_to_stop in procs_for_cleanup.lock().await.iter_mut() {
                if let Err(err) = proc_to_stop
                    .destroy_and_wait::<()>(Duration::from_millis(10), None)
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
        let (serve_addr, mut rx) = channel::serve(listen_addr, "v0-serve".to_string())?;
        let tx = channel::dial(bootstrap_addr.clone(), "v0-bootstrap".to_string())?;

        let (rtx, mut return_channel) = oneshot::channel();
        tx.try_post(
            Process2Allocator(bootstrap_index, Process2AllocatorMessage::Hello(serve_addr)),
            rtx,
        )?;
        tokio::spawn(exit_if_missed_heartbeat(bootstrap_index, bootstrap_addr));

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
            let _ = hyperactor::tracing::info_span!("wait_for_next_message_from_mesh_agent");
            match the_msg? {
                Allocator2Process::StartProc(proc_id, listen_transport) => {
                    let (proc, mesh_agent) = ProcMeshAgent::bootstrap(proc_id.clone()).await?;
                    let (proc_addr, proc_rx) = channel::serve(
                        ChannelAddr::any(listen_transport),
                        "bootstrap-proc".to_string(),
                    )?;
                    let handle = proc.clone().serve(proc_rx);
                    drop(handle); // linter appeasement; it is safe to drop this future
                    tx.send(Process2Allocator(
                        bootstrap_index,
                        Process2AllocatorMessage::StartedProc(
                            proc_id.clone(),
                            mesh_agent.bind(),
                            proc_addr,
                        ),
                    ))
                    .await?;
                    procs.lock().await.push(proc);
                }
                Allocator2Process::StopAndExit(code) => {
                    tracing::info!("stopping procs with code {code}");
                    {
                        for proc_to_stop in procs.lock().await.iter_mut() {
                            if let Err(err) = proc_to_stop
                                .destroy_and_wait::<()>(Duration::from_millis(10), None)
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Stdio;

    use hyperactor::ActorId;
    use hyperactor::ActorRef;
    use hyperactor::ProcId;
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
    use tokio::io::AsyncReadExt;
    use tokio::process::Command;

    use super::*;
    use crate::alloc::AllocSpec;
    use crate::alloc::Allocator;
    use crate::alloc::ProcessAllocator;
    use crate::v1::ActorMesh;
    use crate::v1::host_mesh::HostMesh;
    use crate::v1::testactor;

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
                (Bootstrap::V0ProcMesh, Bootstrap::V0ProcMesh) => {}
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
    fn test_bootstrap_env_roundtrip_with_config_proc_and_host() {
        // Build a small, distinctive Attrs snapshot.
        let mut attrs = Attrs::new();
        attrs[MESH_TAIL_LOG_LINES] = 123;
        attrs[MESH_BOOTSTRAP_ENABLE_PDEATHSIG] = false;

        // Proc case
        {
            let original = Bootstrap::Proc {
                proc_id: id!(foo[42]),
                backend_addr: ChannelAddr::any(ChannelTransport::Unix),
                callback_addr: ChannelAddr::any(ChannelTransport::Unix),
                config: Some(attrs.clone()),
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
    }

    #[tokio::test]
    async fn test_child_terminated_on_manager_drop() {
        use std::path::PathBuf;
        use std::process::Stdio;

        use tokio::process::Command;
        use tokio::time::Duration;

        // Manager; program path is irrelevant for this test.
        let command = BootstrapCommand {
            program: PathBuf::from("/bin/true"),
            ..Default::default()
        };
        let manager = BootstrapProcManager::new(command);

        // Spawn a long-running child process (sleep 30) with
        // kill_on_drop(true).
        let mut cmd = Command::new("/bin/sleep");
        cmd.arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);

        let child = cmd.spawn().expect("spawn sleep");
        let pid = child.id().expect("pid");

        // Insert into the manager's children map (simulates a spawned
        // proc).
        let proc_id = ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "test".to_string());
        {
            let mut children = manager.children.lock().await;
            children.insert(
                proc_id.clone(),
                BootstrapProcHandle::new(proc_id.clone(), child),
            );
        }

        // (Linux-only) Verify the process exists before drop.
        #[cfg(target_os = "linux")]
        {
            let path = format!("/proc/{}", pid);
            assert!(
                std::fs::metadata(&path).is_ok(),
                "expected /proc/{pid} to exist before drop"
            );
        }

        // Drop the manager. We don't rely on `Child::kill_on_drop`.
        // The exit monitor owns the Child; the manager's Drop uses
        // `pid_table` to best-effort SIGKILL any recorded PIDs. This
        // should terminate the child.
        drop(manager);

        // Poll until the /proc entry disappears OR the state is
        // Zombie.
        #[cfg(target_os = "linux")]
        {
            let deadline = std::time::Instant::now() + Duration::from_millis(1500);
            let proc_dir = format!("/proc/{}", pid);
            let status_file = format!("{}/status", proc_dir);

            let mut ok = false;
            while std::time::Instant::now() < deadline {
                match std::fs::read_to_string(&status_file) {
                    Ok(s) => {
                        if let Some(state_line) = s.lines().find(|l| l.starts_with("State:")) {
                            if state_line.contains('Z') {
                                // Only 'Z' (Zombie) is acceptable.
                                ok = true;
                                break;
                            } else {
                                // Still alive (e.g. 'R', 'S', etc.). Give it more time.
                            }
                        }
                    }
                    Err(_) => {
                        // /proc/<pid>/status no longer exists -> fully gone
                        ok = true;
                        break;
                    }
                }
                RealClock.sleep(Duration::from_millis(100)).await;
            }

            assert!(ok, "expected /proc/{pid} to be gone or zombie after drop");
        }

        // On non-Linux, absence of panics/hangs is the signal; PID
        // probing is platform-specific.
    }

    #[tokio::test]
    async fn test_v1_child_logging() {
        use hyperactor::channel;
        use hyperactor::data::Serialized;
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
            channel::serve(ChannelAddr::any(ChannelTransport::Unix), "test".to_string()).unwrap();
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
        let log_client: ActorRef<LogClientActor> =
            proc.spawn("log_client", ()).await.unwrap().bind();
        log_client.set_aggregate(&client, None).await.unwrap();

        // Spawn the forwarder in this proc (it will serve
        // BOOTSTRAP_LOG_CHANNEL).
        let _log_forwarder: ActorRef<LogForwardActor> = proc
            .spawn("log_forwarder", log_client.clone())
            .await
            .unwrap()
            .bind();

        // Dial the channel but don't post until we know the forwarder
        // Dial the file monitor channel if provided
        let tx = channel::dial::<LogMessage>(log_channel.clone(), "test".to_string()).unwrap();

        // Send a fake log message as if it came from the proc
        // manager's writer.
        tx.post(LogMessage::Log {
            hostname: "testhost".into(),
            pid: 12345,
            output_target: OutputTarget::Stdout,
            payload: Serialized::serialize(&"hello from child".to_string()).unwrap(),
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

        use hyperactor::ActorId;
        use hyperactor::ActorRef;
        use hyperactor::ProcId;
        use hyperactor::WorldId;
        use hyperactor::host::ProcHandle;
        use tokio::process::Command;

        use super::super::*;
        use super::any_addr_for_test;

        // Helper: build a ProcHandle with a short-lived child
        // process. We don't rely on the actual process; we only
        // exercise the status transitions.
        fn handle_for_test() -> BootstrapProcHandle {
            // Spawn a trivial child that exits immediately.
            let child = Command::new("sh")
                .arg("-c")
                .arg("exit 0")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("failed to spawn test child process");

            let proc_id = ProcId::Ranked(WorldId("test".into()), 0);
            BootstrapProcHandle::new(proc_id, child)
        }

        #[tokio::test]
        async fn starting_to_running_ok() {
            let h = handle_for_test();
            assert!(matches!(h.status(), ProcStatus::Starting));
            let child_pid = h.pid().expect("child should have a pid");
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_pid, child_started_at));
            match h.status() {
                ProcStatus::Running { pid, started_at } => {
                    assert_eq!(pid, child_pid);
                    assert_eq!(started_at, child_started_at);
                }
                other => panic!("expected Running, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn running_to_stopping_to_stopped_ok() {
            let h = handle_for_test();
            let child_pid = h.pid().expect("child should have a pid");
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_pid, child_started_at));
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
            let child_pid = h.pid().expect("child should have a pid");
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_pid, child_started_at));
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
            let child_pid = h.pid().expect("child should have a pid");
            let child_started_at = RealClock.system_time_now();
            assert!(h.mark_running(child_pid, child_started_at));
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
            let child_pid = h.pid().expect("child should have a pid");
            let child_started_at = RealClock.system_time_now();
            // Starting -> Running is fine; second Running should be rejected.
            assert!(h.mark_running(child_pid, child_started_at));
            assert!(!h.mark_running(child_pid, RealClock.system_time_now()));
            match h.status() {
                ProcStatus::Running { pid, .. } => assert_eq!(pid, child_pid),
                other => panic!("expected Running, got {other:?}"),
            }
            // Once Stopped, we can't go to Running/Killed/Failed/etc.
            assert!(h.mark_stopping());
            assert!(h.mark_stopped(0, Vec::new()));
            assert!(!h.mark_running(child_pid, child_started_at));
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
            let pid = h.pid().expect("child should have a pid");
            let t0 = RealClock.system_time_now();
            assert!(h.mark_running(pid, t0));
            // Build a consistent AgentRef for Ready using the
            // handle's ProcId.
            let proc_id = <BootstrapProcHandle as ProcHandle>::proc_id(&h);
            let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
            let agent_ref: ActorRef<ProcMeshAgent> = ActorRef::attest(actor_id);
            // Ready -> Stopping -> Stopped should be legal.
            assert!(h.mark_ready(pid, t0, addr, agent_ref));
            assert!(h.mark_stopping());
            assert!(h.mark_stopped(0, Vec::new()));
        }

        #[tokio::test]
        async fn ready_to_killed_is_legal() {
            let h = handle_for_test();
            let addr = any_addr_for_test();
            // Starting -> Running
            let pid = h.pid().expect("child should have a pid");
            let t0 = RealClock.system_time_now();
            assert!(h.mark_running(pid, t0));
            // Build a consistent AgentRef for Ready using the
            // handle's ProcId.
            let proc_id = <BootstrapProcHandle as ProcHandle>::proc_id(&h);
            let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
            let agent: ActorRef<ProcMeshAgent> = ActorRef::attest(actor_id);
            // Running -> Ready
            assert!(h.mark_ready(pid, t0, addr, agent));
            // Ready -> Killed
            assert!(h.mark_killed(9, false));
        }

        #[tokio::test]
        async fn mark_stopping_from_starting_uses_child_pid_when_available() {
            let h = handle_for_test();

            // In `Starting`, we should still be able to see the PID via
            // Child::id().
            let child_pid = h
                .pid()
                .expect("Child::id() should be available in Starting");

            // mark_stopping() should transition to Stopping{pid,
            // started_at} and stash the same pid we observed.
            assert!(
                h.mark_stopping(),
                "mark_stopping() should succeed from Starting"
            );
            match h.status() {
                ProcStatus::Stopping { pid, started_at } => {
                    assert_eq!(pid, child_pid, "Stopping pid should come from Child::id()");
                    assert!(
                        started_at <= RealClock.system_time_now(),
                        "started_at should be sane"
                    );
                }
                other => panic!("expected Stopping{{..}}; got {other:?}"),
            }
        }

        #[tokio::test]
        async fn mark_stopping_noop_when_no_child_pid_available() {
            let h = handle_for_test();

            // Simulate the exit-monitor having already taken the
            // Child so there is no Child::id() and we haven't cached
            // a pid yet.
            {
                let _ = h.child.lock().expect("child mutex").take();
            }

            // With no observable pid, mark_stopping() must no-op and
            // leave us in Starting.
            assert!(
                !h.mark_stopping(),
                "mark_stopping() should no-op from Starting when no pid is observable"
            );
            assert!(matches!(h.status(), ProcStatus::Starting));
        }

        #[tokio::test]
        async fn mark_failed_from_stopping_is_allowed() {
            let h = handle_for_test();

            // Drive Starting -> Stopping (pid available via
            // Child::id()).
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

    #[tokio::test]
    async fn test_exit_monitor_updates_status_on_clean_exit() {
        let command = BootstrapCommand {
            program: PathBuf::from("/bin/true"),
            ..Default::default()
        };
        let manager = BootstrapProcManager::new(command);

        // Spawn a fast-exiting child.
        let mut cmd = Command::new("true");
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn().expect("spawn true");

        let proc_id = ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "clean".into());
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        // Put into manager & start monitor.
        {
            let mut children = manager.children.lock().await;
            children.insert(proc_id.clone(), handle.clone());
        }
        manager.spawn_exit_monitor(proc_id.clone(), handle.clone());
        {
            let guard = handle.child.lock().expect("child mutex");
            assert!(
                guard.is_none(),
                "expected Child to be taken by exit monitor"
            );
        }

        let st = handle.wait_inner().await;
        assert!(matches!(st, ProcStatus::Stopped { .. }), "status={st:?}");
    }

    #[tokio::test]
    async fn test_exit_monitor_updates_status_on_kill() {
        let command = BootstrapCommand {
            program: PathBuf::from("/bin/sleep"),
            ..Default::default()
        };
        let manager = BootstrapProcManager::new(command);

        // Spawn a process that will live long enough to kill.
        let mut cmd = Command::new("/bin/sleep");
        cmd.arg("5").stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn().expect("spawn sleep");
        let pid = child.id().expect("pid") as i32;

        // Register with the manager and start the exit monitor.
        let proc_id = ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "killed".into());
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);
        {
            let mut children = manager.children.lock().await;
            children.insert(proc_id.clone(), handle.clone());
        }
        manager.spawn_exit_monitor(proc_id.clone(), handle.clone());
        {
            let guard = handle.child.lock().expect("child mutex");
            assert!(
                guard.is_none(),
                "expected Child to be taken by exit monitor"
            );
        }
        // Send SIGKILL to the process.
        #[cfg(unix)]
        // SAFETY: We own the child process we just spawned and have
        // its PID.
        // Sending SIGKILL is safe here because:
        //  - the PID comes directly from `child.id()`
        //  - the process is guaranteed to be in our test scope
        //  - we don't touch any memory via this call, only signal the
        //    OS
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }

        let st = handle.wait_inner().await;
        match st {
            ProcStatus::Killed { signal, .. } => assert_eq!(signal, libc::SIGKILL),
            other => panic!("expected Killed(SIGKILL), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn watch_notifies_on_status_changes() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn");

        let proc_id = ProcId::Ranked(WorldId("test".into()), 1);
        let handle = BootstrapProcHandle::new(proc_id, child);
        let mut rx = handle.watch();

        // Starting -> Running
        let pid = handle.pid().unwrap_or(0);
        let now = RealClock.system_time_now();
        assert!(handle.mark_running(pid, now));
        rx.changed().await.ok(); // Observe the transition.
        match &*rx.borrow() {
            ProcStatus::Running { pid: p, started_at } => {
                assert_eq!(*p, pid);
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
        // Child exits immediately.
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 7")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn");

        let proc_id = ProcId::Direct(
            ChannelAddr::any(ChannelTransport::Unix),
            "early-exit".into(),
        );
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

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
        });
        let unknown = ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "nope".into());
        assert!(manager.status(&unknown).await.is_none());
    }

    #[tokio::test]
    async fn exit_monitor_child_already_taken_leaves_status_unchanged() {
        let manager = BootstrapProcManager::new(BootstrapCommand {
            program: PathBuf::from("/bin/sleep"),
            ..Default::default()
        });

        // Long-ish child so it's alive while we "steal" it.
        let mut cmd = Command::new("/bin/sleep");
        cmd.arg("5").stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn().expect("spawn sleep");

        let proc_id = ProcId::Direct(
            ChannelAddr::any(ChannelTransport::Unix),
            "already-taken".into(),
        );
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        // Insert, then deliberately consume the Child before starting
        // the monitor.
        {
            let mut children = manager.children.lock().await;
            children.insert(proc_id.clone(), handle.clone());
        }
        {
            let _ = handle.child.lock().expect("child mutex").take();
        }

        // This should not panic; monitor will see None and bail.
        manager.spawn_exit_monitor(proc_id.clone(), handle.clone());

        // Status should remain whatever it was (Starting in this
        // setup).
        assert!(matches!(
            manager.status(&proc_id).await,
            Some(ProcStatus::Starting)
        ));
    }

    #[tokio::test]
    async fn pid_none_after_exit_monitor_takes_child() {
        let manager = BootstrapProcManager::new(BootstrapCommand {
            program: PathBuf::from("/bin/sleep"),
            ..Default::default()
        });

        let mut cmd = Command::new("/bin/sleep");
        cmd.arg("5").stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn().expect("spawn sleep");

        let proc_id = ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "pid-none".into());
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        // Before monitor: we should still be able to read a pid.
        assert!(handle.pid().is_some());
        {
            let mut children = manager.children.lock().await;
            children.insert(proc_id.clone(), handle.clone());
        }
        // `spawn_exit_monitor` takes the Child synchronously before
        // spawning the task.
        manager.spawn_exit_monitor(proc_id.clone(), handle.clone());

        // Immediately after, the handle should no longer expose a pid
        // (Child is gone).
        assert!(handle.pid().is_none());
    }

    #[tokio::test]
    async fn starting_may_directly_be_marked_stopped() {
        // Unit-level transition check for the "process exited very
        // fast" case.
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn true");

        let proc_id = ProcId::Direct(ChannelAddr::any(ChannelTransport::Unix), "fast-exit".into());
        let handle = BootstrapProcHandle::new(proc_id, child);

        // Take the child (don't hold the mutex across an await).
        let mut c = {
            let mut guard = handle.child.lock().expect("child mutex");
            guard.take()
        }
        .expect("child already taken");

        let status = c.wait().await.expect("wait");
        let code = status.code().unwrap_or(0);
        assert!(handle.mark_stopped(code, Vec::new()));

        assert!(matches!(
            handle.status(),
            ProcStatus::Stopped { exit_code: 0, .. }
        ));
    }

    #[tokio::test]
    async fn handle_ready_allows_waiters() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let proc_id = ProcId::Ranked(WorldId("test".into()), 42);
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        let pid = handle.pid().expect("child should have a pid");
        let started_at = RealClock.system_time_now();
        assert!(handle.mark_running(pid, started_at));

        let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
        let agent_ref: ActorRef<ProcMeshAgent> = ActorRef::attest(actor_id);

        // Pick any addr to carry in Ready (what the child would have
        // called back with).
        let ready_addr = any_addr_for_test();

        // Stamp Ready and assert ready().await unblocks.
        assert!(handle.mark_ready(pid, started_at, ready_addr.clone(), agent_ref));
        handle
            .ready_inner()
            .await
            .expect("ready_inner() should complete after Ready");

        // Sanity-check the Ready fields we control
        // (pid/started_at/addr).
        match handle.status() {
            ProcStatus::Ready {
                pid: p,
                started_at: t,
                addr: a,
                ..
            } => {
                assert_eq!(p, pid);
                assert_eq!(t, started_at);
                assert_eq!(a, ready_addr);
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pid_behavior_across_states_running_ready_then_stopped() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn");

        let proc_id = ProcId::Ranked(WorldId("test".into()), 0);
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        // Running → pid() Some
        let pid = handle.pid().expect("initial Child::id");
        let t0 = RealClock.system_time_now();
        assert!(handle.mark_running(pid, t0));
        assert_eq!(handle.pid(), Some(pid));

        // Ready → pid() still Some even if Child taken
        let addr = any_addr_for_test();
        let agent = {
            let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
            ActorRef::<ProcMeshAgent>::attest(actor_id)
        };
        assert!(handle.mark_ready(pid, t0, addr, agent));
        {
            let _ = handle.child.lock().expect("child mutex").take();
        }
        assert_eq!(handle.pid(), Some(pid));

        // Terminal (Stopped) → pid() None
        assert!(handle.mark_stopped(0, Vec::new()));
        assert_eq!(handle.pid(), None, "pid() should be None once terminal");
    }

    #[tokio::test]
    async fn pid_is_available_in_ready_even_after_child_taken() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn");

        let proc_id = ProcId::Ranked(WorldId("test".into()), 99);
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        // Mark Running.
        let pid = handle.pid().expect("child should have pid (via Child::id)");
        let started_at = RealClock.system_time_now();
        assert!(handle.mark_running(pid, started_at));

        // Stamp Ready with synthetic addr/agent (attested).
        let addr = any_addr_for_test();
        let agent = {
            let actor_id = ActorId(proc_id.clone(), "agent".into(), 0);
            ActorRef::<ProcMeshAgent>::attest(actor_id)
        };
        assert!(handle.mark_ready(pid, started_at, addr, agent));

        // Simulate exit monitor taking the Child (so Child::id() is
        // no longer available).
        {
            let _ = handle.child.lock().expect("child mutex").take();
        }

        // **Regression check**: pid() must still return the cached
        // pid in Ready.
        assert_eq!(handle.pid(), Some(pid), "pid() should be cached in Ready");
    }

    #[test]
    fn display_running_includes_pid_and_uptime() {
        let started_at = RealClock.system_time_now() - Duration::from_secs(42);
        let st = ProcStatus::Running {
            pid: 1234,
            started_at,
        };

        let s = format!("{}", st);
        assert!(s.contains("1234"));
        assert!(s.contains("Running"));
        assert!(s.contains("42s"));
    }

    #[test]
    fn display_ready_includes_pid_and_addr() {
        let started_at = RealClock.system_time_now() - Duration::from_secs(5);
        let addr = ChannelAddr::any(ChannelTransport::Unix);
        let agent =
            ActorRef::attest(ProcId::Direct(addr.clone(), "proc".into()).actor_id("agent", 0));

        let st = ProcStatus::Ready {
            pid: 4321,
            started_at,
            addr: addr.clone(),
            agent,
        };

        let s = format!("{}", st);
        assert!(s.contains("4321")); // pid
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
                pid: 42,
                started_at: RealClock.system_time_now(),
            },
            ProcStatus::Ready {
                pid: 42,
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
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn");

        let proc_id = ProcId::Direct(any_addr_for_test(), "ph-ready-ok".into());
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        // Starting -> Running
        let pid = handle.pid().expect("pid");
        let t0 = RealClock.system_time_now();
        assert!(handle.mark_running(pid, t0));

        // Synthesize Ready data
        let addr = any_addr_for_test();
        let agent: ActorRef<ProcMeshAgent> =
            ActorRef::attest(ActorId(proc_id.clone(), "agent".into(), 0));
        assert!(handle.mark_ready(pid, t0, addr, agent));

        // Call the trait method (not ready_inner).
        let r = <BootstrapProcHandle as hyperactor::host::ProcHandle>::ready(&handle).await;
        assert!(r.is_ok(), "expected Ok(()), got {r:?}");
    }

    #[tokio::test]
    async fn proc_handle_wait_returns_terminal_status() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn");

        let proc_id = ProcId::Direct(any_addr_for_test(), "ph-wait".into());
        let handle = BootstrapProcHandle::new(proc_id, child);

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
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 7")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn");
        let proc_id = ProcId::Direct(any_addr_for_test(), "wrap".into());
        let handle = BootstrapProcHandle::new(proc_id, child);

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
        let proc_id = id!(bootstrap_child[0]);

        // Serve a Unix channel as the "backend_addr" and hook it into
        // this test proc.
        let (backend_addr, rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix), "test".to_string()).unwrap();

        // Route messages arriving on backend_addr into this test
        // proc's mailbox so the bootstrap child can reach the host
        // router.
        instance.proc().clone().serve(rx);

        (proc_id, backend_addr)
    }

    #[tokio::test]
    async fn bootstrap_handle_terminate_graceful() {
        // Create a root direct-addressed proc + client instance.
        let root = hyperactor::Proc::direct(ChannelTransport::Unix.any(), "root".to_string())
            .await
            .unwrap();
        let (instance, _handle) = root.instance("client").unwrap();

        let mgr = BootstrapProcManager::new(BootstrapCommand::test());
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
            .timeout(deadline * 2, handle.terminate(deadline))
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
    async fn bootstrap_handle_kill_forced() {
        // Root proc + client instance (so the child can dial back).
        let root = hyperactor::Proc::direct(ChannelTransport::Unix.any(), "root".to_string())
            .await
            .unwrap();
        let (instance, _handle) = root.instance("client").unwrap();

        let mgr = BootstrapProcManager::new(BootstrapCommand::test());

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
    async fn bootstrap_cannonical_simple() {
        // SAFETY: unit-test scoped
        unsafe {
            std::env::set_var("HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG", "false");
        }
        // Create a "root" direct addressed proc.
        let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string())
            .await
            .unwrap();
        // Create an actor instance we'll use to send and receive
        // messages.
        let (instance, _handle) = proc.instance("client").unwrap();

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
        let host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
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
        let got_id = rx.recv().await.unwrap();
        assert_eq!(
            got_id,
            actor_mesh.values().next().unwrap().actor_id().clone()
        );

        // **Important**: If we don't shutdown the hosts, the
        // BootstrapProcManager's won't send SIGKILLs to their spawned
        // children and there will be orphans left behind.
        host_mesh.shutdown(&instance).await.expect("host shutdown");
    }

    #[tokio::test]
    async fn exit_tail_is_attached_and_logged() {
        hyperactor_telemetry::initialize_logging_for_test();
        // Spawn a child that writes to stderr then exits 7.
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("printf 'boom-1\\nboom-2\\n' 1>&2; exit 7")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd.spawn().expect("spawn");
        let pid = child.id().unwrap_or_default();

        // Build a BootstrapProcHandle around this child (like
        // manager.spawn() does).
        let proc_id = hyperactor::id!(testproc[0]);
        let handle = BootstrapProcHandle::new(proc_id.clone(), child);

        // Wire tailers + dummy writers (stdout/stderr -> sinks), then
        // stash them on the handle.
        {
            // Lock the child to get its pipes.
            let mut guard = handle.child.lock().expect("child mutex poisoned");
            if let Some(child) = guard.as_mut() {
                let out = child.stdout.take().expect("child stdout must be piped");
                let err = child.stderr.take().expect("child stderr must be piped");

                // Set up logging like ProcManager does
                let log_channel = ChannelAddr::any(ChannelTransport::Unix);
                let tail_size = hyperactor::config::global::get(MESH_TAIL_LOG_LINES);

                // Set up StreamFwders if pipes are available
                let stdout_monitor = StreamFwder::start(
                    out,
                    None,
                    OutputTarget::Stdout,
                    tail_size,
                    log_channel.clone(),
                    pid,
                    0,
                );

                let stderr_monitor = StreamFwder::start(
                    err,
                    None,
                    OutputTarget::Stderr,
                    tail_size,
                    log_channel.clone(),
                    pid,
                    0,
                );

                // Set the stream monitors on the handle
                handle.set_stream_monitors(Some(stdout_monitor), Some(stderr_monitor));
                tracing::info!("set stream monitors");
            } else {
                panic!("child already taken before wiring tailers");
            }
        }

        // Start an exit monitor (consumes the Child and tailers from
        // the handle).
        let manager = BootstrapProcManager::new(BootstrapCommand {
            program: std::path::PathBuf::from("/bin/true"), // unused in this test
            ..Default::default()
        });

        // Give the monitors time to start
        RealClock.sleep(Duration::from_millis(1000)).await;

        // Now start the exit monitor which will take the child and wait for it to exit
        manager.spawn_exit_monitor(proc_id.clone(), handle.clone());

        // Await terminal status and assert on exit code and stderr
        // tail.
        let st = RealClock
            .timeout(Duration::from_secs(2), handle.wait_inner())
            .await
            .expect("wait_inner() timed out (exit monitor stuck?)");

        match st {
            ProcStatus::Stopped {
                exit_code,
                stderr_tail,
            } => {
                assert_eq!(
                    exit_code, 7,
                    "unexpected exit code; stderr_tail={:?}",
                    stderr_tail
                );
                let tail = stderr_tail.join("\n");
                assert!(tail.contains("boom-1"), "missing boom-1; tail:\n{tail}");
                assert!(tail.contains("boom-2"), "missing boom-2; tail:\n{tail}");
            }
            other => panic!("expected Stopped(7), got {other:?}"),
        }
    }
}
