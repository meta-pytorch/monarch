/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines [`Host`], which represents all the procs running on a host.
//! The procs themselves are managed by an implementation of [`ProcManager`], which may,
//! for example, fork new processes for each proc, or spawn them in the same process
//! for testing purposes.
//!
//! The primary purpose of a host is to manage the lifecycle of these procs, and to
//! serve as a single front-end for all the procs on a host, multiplexing network
//! channels.
//!
//! ## Channel muxing
//!
//! A [`Host`] maintains a single frontend address, through which all procs are accessible
//! through direct addressing: the id of each proc is the `ProcId::Direct(frontend_addr, proc_name)`.
//! In the following, the frontend address is denoted by `*`. The host listens on `*` and
//! multiplexes messages based on the proc name. When spawning procs, the host maintains
//! backend channels with separate addresses. In the diagram `#` is the backend address of
//! the host, while `#n` is the backend address for proc *n*. The host forwards messages
//! to the appropriate backend channel, while procs forward messages to the host backend
//! channel at `#`.
//!
//! ```text
//!                      ┌────────────┐
//!                  ┌───▶  proc *,1  │
//!                  │ #1└────────────┘
//!                  │
//!  ┌──────────┐    │   ┌────────────┐
//!  │   Host   │◀───┼───▶  proc *,2  │
//! *└──────────┘#   │ #2└────────────┘
//!                  │
//!                  │   ┌────────────┐
//!                  └───▶  proc *,3  │
//!                    #3└────────────┘
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::Future;
use futures::StreamExt;
use futures::stream;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::Mutex;

use crate as hyperactor;
use crate::Actor;
use crate::ActorHandle;
use crate::ActorId;
use crate::ActorRef;
use crate::PortHandle;
use crate::Proc;
use crate::ProcId;
use crate::actor::Binds;
use crate::actor::Referable;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelRx;
use crate::channel::ChannelTransport;
use crate::channel::Rx;
use crate::channel::Tx;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::context;
use crate::mailbox::BoxableMailboxSender;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::MailboxClient;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxServer;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::Undeliverable;

/// The type of error produced by host operations.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// A channel error occurred during a host operation.
    #[error(transparent)]
    ChannelError(#[from] ChannelError),

    /// The named proc already exists and cannot be spawned.
    #[error("proc '{0}' already exists")]
    ProcExists(String),

    /// Failures occuring while spawning a subprocess.
    #[error("proc '{0}' failed to spawn process: {1}")]
    ProcessSpawnFailure(ProcId, #[source] std::io::Error),

    /// Failures occuring while configuring a subprocess.
    #[error("proc '{0}' failed to configure process: {1}")]
    ProcessConfigurationFailure(ProcId, #[source] anyhow::Error),

    /// Failures occuring while spawning a management actor in a proc.
    #[error("failed to spawn agent on proc '{0}': {1}")]
    AgentSpawnFailure(ProcId, #[source] anyhow::Error),

    /// An input parameter was missing.
    #[error("parameter '{0}' missing: {1}")]
    MissingParameter(String, std::env::VarError),

    /// An input parameter was invalid.
    #[error("parameter '{0}' invalid: {1}")]
    InvalidParameter(String, anyhow::Error),
}

/// A host, managing the lifecycle of several procs, and their backend
/// routing, as described in this module's documentation.
pub struct Host<M> {
    procs: HashSet<String>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    router: DialMailboxRouter,
    manager: M,
    service_proc: Proc,
    local_proc: Proc,
    frontend_rx: Option<ChannelRx<MessageEnvelope>>,
}

impl<M: ProcManager> Host<M> {
    /// Serve a host using the provided ProcManager, on the provided `addr`.
    /// On success, the host will multiplex messages for procs on the host
    /// on the address of the host.
    pub async fn new(manager: M, addr: ChannelAddr) -> Result<Self, HostError> {
        Self::new_with_default(manager, addr, None).await
    }

    /// Like [`new`], serves a host using the provided ProcManager, on the provided `addr`.
    /// Unknown destinations are forwarded to the default sender.
    #[crate::instrument(fields(addr=addr.to_string()))]
    pub async fn new_with_default(
        manager: M,
        addr: ChannelAddr,
        default_sender: Option<BoxedMailboxSender>,
    ) -> Result<Self, HostError> {
        let (frontend_addr, frontend_rx) = channel::serve(addr)?;

        // We set up a cascade of routers: first, the outer router supports
        // sending to the the system proc, while the dial router manages dialed
        // connections.
        let router = match default_sender {
            Some(d) => DialMailboxRouter::new_with_default(d),
            None => DialMailboxRouter::new(),
        };

        // Establish a backend channel on the preferred transport. We currently simply
        // serve the same router on both.
        let (backend_addr, backend_rx) = channel::serve(ChannelAddr::any(manager.transport()))?;

        // Set up a system proc. This is often used to manage the host itself.
        let service_proc_id = ProcId::Direct(frontend_addr.clone(), "service".to_string());
        let service_proc = Proc::new(service_proc_id.clone(), router.boxed());

        let local_proc_id = ProcId::Direct(frontend_addr.clone(), "local".to_string());
        let local_proc = Proc::new(local_proc_id.clone(), router.boxed());

        tracing::info!(
            frontend_addr = frontend_addr.to_string(),
            backend_addr = backend_addr.to_string(),
            service_proc_id = service_proc_id.to_string(),
            local_proc_id = local_proc_id.to_string(),
            "serving host"
        );

        let host = Host {
            procs: HashSet::new(),
            frontend_addr,
            backend_addr,
            router,
            manager,
            service_proc,
            local_proc,
            frontend_rx: Some(frontend_rx),
        };

        // We the same router on both frontend and backend addresses.
        let _backend_handle = host.forwarder().serve(backend_rx);

        Ok(host)
    }

    /// Start serving this host's mailbox on its frontend address.
    /// Returns the server handle on first invocation; afterwards None.
    pub fn serve(&mut self) -> Option<MailboxServerHandle> {
        Some(self.forwarder().serve(self.frontend_rx.take()?))
    }

    /// The underlying proc manager.
    pub fn manager(&self) -> &M {
        &self.manager
    }

    /// The address which accepts messages destined for this host.
    pub fn addr(&self) -> &ChannelAddr {
        &self.frontend_addr
    }

    /// The system proc associated with this host.
    /// This is used to run host-level system services like host managers.
    pub fn system_proc(&self) -> &Proc {
        &self.service_proc
    }

    /// The local proc associated with this host.
    /// This is the local proc used in processes that are also hosts.
    pub fn local_proc(&self) -> &Proc {
        &self.local_proc
    }

    /// Spawn a new process with the given `name`. On success, the
    /// proc has been spawned, and is reachable through the returned,
    /// direct-addressed ProcId, which will be
    /// `ProcId::Direct(self.addr(), name)`.
    pub async fn spawn(
        &mut self,
        name: String,
        config: M::Config,
    ) -> Result<(ProcId, ActorRef<ManagerAgent<M>>), HostError> {
        if self.procs.contains(&name) {
            return Err(HostError::ProcExists(name));
        }

        let proc_id = ProcId::Direct(self.frontend_addr.clone(), name.clone());
        let handle = self
            .manager
            .spawn(proc_id.clone(), self.backend_addr.clone(), config)
            .await?;

        // Await readiness (config-driven; 0s disables timeout).
        let to: Duration = hyperactor_config::global::get(crate::config::HOST_SPAWN_READY_TIMEOUT);
        let ready = if to == Duration::from_secs(0) {
            ReadyProc::ensure(&handle).await
        } else {
            match RealClock.timeout(to, ReadyProc::ensure(&handle)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(ReadyProcError::Timeout),
            }
        }
        .map_err(|e| {
            HostError::ProcessConfigurationFailure(proc_id.clone(), anyhow::anyhow!("{e:?}"))
        })?;

        self.router
            .bind(proc_id.clone().into(), ready.addr().clone());
        self.procs.insert(name);

        Ok((proc_id, ready.agent_ref().clone()))
    }

    fn forwarder(&self) -> ProcOrDial {
        ProcOrDial {
            service_proc: self.service_proc.clone(),
            local_proc: self.local_proc.clone(),
            dialer: self.router.clone(),
        }
    }
}

/// A router used to route to the system proc, or else fall back to
/// the dial mailbox router.
#[derive(Clone)]
struct ProcOrDial {
    service_proc: Proc,
    local_proc: Proc,
    dialer: DialMailboxRouter,
}

impl MailboxSender for ProcOrDial {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if envelope.dest().actor_id().proc_id() == self.service_proc.proc_id() {
            self.service_proc.post_unchecked(envelope, return_handle);
        } else if envelope.dest().actor_id().proc_id() == self.local_proc.proc_id() {
            self.local_proc.post_unchecked(envelope, return_handle);
        } else {
            self.dialer.post_unchecked(envelope, return_handle)
        }
    }
}

/// Error returned by [`ProcHandle::ready`].
#[derive(Debug, Clone)]
pub enum ReadyError<TerminalStatus> {
    /// The proc reached a terminal state before becoming Ready.
    Terminal(TerminalStatus),
    /// Implementation lost its status channel / cannot observe state.
    ChannelClosed,
}

/// Error returned by [`ready_proc`].
#[derive(Debug, Clone)]
pub enum ReadyProcError<TerminalStatus> {
    /// Timed out waiting for ready.
    Timeout,
    /// The underlying `ready()` call failed.
    Ready(ReadyError<TerminalStatus>),
    /// The handle's `addr()` returned `None` after `ready()` succeeded.
    MissingAddr,
    /// The handle's `agent_ref()` returned `None` after `ready()`
    /// succeeded.
    MissingAgentRef,
}

impl<T> From<ReadyError<T>> for ReadyProcError<T> {
    fn from(e: ReadyError<T>) -> Self {
        ReadyProcError::Ready(e)
    }
}

/// Error returned by [`ProcHandle::wait`].
#[derive(Debug, Clone)]
pub enum WaitError {
    /// Implementation lost its status channel / cannot observe state.
    ChannelClosed,
}

/// Error returned by [`ProcHandle::terminate`] and
/// [`ProcHandle::kill`].
///
/// - `Unsupported`: the manager cannot perform the requested proc
///   signaling (e.g., local/in-process manager that doesn't emulate
///   kill).
/// - `AlreadyTerminated(term)`: the proc was already terminal; `term`
///   is the same value `wait()` would return.
/// - `ChannelClosed`: the manager lost its lifecycle channel and
///   cannot reliably observe state transitions.
/// - `Io(err)`: manager-specific failure delivering the signal or
///   performing shutdown (e.g., OS error on kill).
#[derive(Debug)]
pub enum TerminateError<TerminalStatus> {
    /// Manager doesn't support signaling (e.g., Local manager).
    Unsupported,
    /// A terminal state was already reached while attempting
    /// terminate/kill.
    AlreadyTerminated(TerminalStatus),
    /// Implementation lost its status channel / cannot observe state.
    ChannelClosed,
    /// Manager-specific failure to deliver signal or perform
    /// shutdown.
    Io(anyhow::Error),
}

impl<T: fmt::Debug> fmt::Display for TerminateError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TerminateError::Unsupported => write!(f, "terminate/kill unsupported by manager"),
            TerminateError::AlreadyTerminated(st) => {
                write!(f, "proc already terminated (status: {st:?})")
            }
            TerminateError::ChannelClosed => {
                write!(f, "lifecycle channel closed; cannot observe state")
            }
            TerminateError::Io(err) => write!(f, "I/O error during terminate/kill: {err}"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for TerminateError<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TerminateError::Io(err) => Some(err.root_cause()),
            _ => None,
        }
    }
}

/// Summary of results from a bulk termination attempt.
///
/// - `attempted`: total number of child procs for which termination
///   was attempted.
/// - `ok`: number of procs successfully terminated (includes those
///   that were already in a terminal state).
/// - `failed`: number of procs that could not be terminated (e.g.
///   signaling errors or lost lifecycle channel).
#[derive(Debug)]
pub struct TerminateSummary {
    /// Total number of child procs for which termination was
    /// attempted.
    pub attempted: usize,
    /// Number of procs that successfully reached a terminal state.
    ///
    /// This count includes both procs that exited cleanly after
    /// `terminate(timeout)` and those that were already in a terminal
    /// state before termination was attempted.
    pub ok: usize,
    /// Number of procs that failed to terminate.
    ///
    /// Failures typically arise from signaling errors (e.g., OS
    /// failure to deliver SIGTERM/SIGKILL) or a lost lifecycle
    /// channel, meaning the manager could no longer observe state
    /// transitions.
    pub failed: usize,
}

impl fmt::Display for TerminateSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "attempted={} ok={} failed={}",
            self.attempted, self.ok, self.failed
        )
    }
}

#[async_trait::async_trait]
/// Trait for terminating a single proc.
pub trait SingleTerminate: Send + Sync {
    /// Gracefully terminate the given proc.
    ///
    /// Initiates a polite shutdown for each child, waits up to
    /// `timeout` for completion, then escalates to a forceful stop
    /// The returned [`TerminateSummary`] reports how
    /// many children were attempted, succeeded, and failed.
    ///
    /// Implementation notes:
    /// - "Polite shutdown" and "forceful stop" are intentionally
    ///   abstract. Implementors should map these to whatever
    ///   semantics they control (e.g., proc-level drain/abort, RPCs,
    ///   OS signals).
    /// - The operation must be idempotent and tolerate races with
    ///   concurrent termination or external exits.
    ///
    /// # Parameters
    /// - `timeout`: Per-child grace period before escalation to a
    ///   forceful stop.
    /// Returns a tuple of (polite shutdown actors vec, forceful stop actors vec)
    async fn terminate_proc(
        &self,
        cx: &impl context::Actor,
        proc: &ProcId,
        timeout: std::time::Duration,
    ) -> Result<(Vec<ActorId>, Vec<ActorId>), anyhow::Error>;
}

/// Trait for managers that can terminate many child **units** in
/// bulk.
///
/// Implementors provide a concurrency-bounded, graceful shutdown over
/// all currently tracked children (polite stop → wait → forceful
/// stop), returning a summary of outcomes. The exact stop/kill
/// semantics are manager-specific: for example, an OS-process manager
/// might send signals, while an in-process manager might drain/abort
/// tasks.
#[async_trait::async_trait]
pub trait BulkTerminate: Send + Sync {
    /// Gracefully terminate all known children.
    ///
    /// Initiates a polite shutdown for each child, waits up to
    /// `timeout` for completion, then escalates to a forceful stop
    /// for any that remain. Work may be done in parallel, capped by
    /// `max_in_flight`. The returned [`TerminateSummary`] reports how
    /// many children were attempted, succeeded, and failed.
    ///
    /// Implementation notes:
    /// - "Polite shutdown" and "forceful stop" are intentionally
    ///   abstract. Implementors should map these to whatever
    ///   semantics they control (e.g., proc-level drain/abort, RPCs,
    ///   OS signals).
    /// - The operation must be idempotent and tolerate races with
    ///   concurrent termination or external exits.
    ///
    /// # Parameters
    /// - `timeout`: Per-child grace period before escalation to a
    ///   forceful stop.
    /// - `max_in_flight`: Upper bound on concurrent terminations (≥
    ///   1) to prevent resource spikes (I/O, CPU, file descriptors,
    ///   etc.).
    async fn terminate_all(
        &self,
        cx: &impl context::Actor,
        timeout: std::time::Duration,
        max_in_flight: usize,
    ) -> TerminateSummary;
}

// Host convenience that's available only when its manager supports
// bulk termination.
impl<M: ProcManager + BulkTerminate> Host<M> {
    /// Gracefully terminate all procs spawned by this host.
    ///
    /// Delegates to the underlying manager’s
    /// [`BulkTerminate::terminate_all`] implementation. Use this to
    /// perform orderly teardown during scale-down or shutdown.
    ///
    /// # Parameters
    /// - `timeout`: Per-child grace period before escalation.
    /// - `max_in_flight`: Upper bound on concurrent terminations.
    ///
    /// # Returns
    /// A [`TerminateSummary`] with counts of attempted/ok/failed
    /// terminations.
    pub async fn terminate_children(
        &self,
        cx: &impl context::Actor,
        timeout: Duration,
        max_in_flight: usize,
    ) -> TerminateSummary {
        self.manager.terminate_all(cx, timeout, max_in_flight).await
    }
}

#[async_trait::async_trait]
impl<M: ProcManager + SingleTerminate> SingleTerminate for Host<M> {
    async fn terminate_proc(
        &self,
        cx: &impl context::Actor,
        proc: &ProcId,
        timeout: Duration,
    ) -> Result<(Vec<ActorId>, Vec<ActorId>), anyhow::Error> {
        self.manager.terminate_proc(cx, proc, timeout).await
    }
}

/// Capability proving a proc is ready.
///
/// [`ReadyProc::ensure`] validates that `addr()` and `agent_ref()`
/// are available; this type carries that proof, providing infallible
/// accessors.
///
/// Obtain a `ReadyProc` by calling `ready_proc(&handle).await`.
pub struct ReadyProc<'a, H: ProcHandle> {
    handle: &'a H,
    addr: ChannelAddr,
    agent_ref: ActorRef<H::Agent>,
}

impl<'a, H: ProcHandle> ReadyProc<'a, H> {
    /// Wait for a proc to become ready, then return a capability that
    /// provides infallible access to `addr()` and `agent_ref()`.
    ///
    /// This is the type-safe way to obtain the proc's address and
    /// agent reference. After this function returns `Ok(ready)`, both
    /// `ready.addr()` and `ready.agent_ref()` are guaranteed to
    /// succeed.
    pub async fn ensure(
        handle: &'a H,
    ) -> Result<ReadyProc<'a, H>, ReadyProcError<H::TerminalStatus>> {
        handle.ready().await?;
        let addr = handle.addr().ok_or(ReadyProcError::MissingAddr)?;
        let agent_ref = handle.agent_ref().ok_or(ReadyProcError::MissingAgentRef)?;
        Ok(ReadyProc {
            handle,
            addr,
            agent_ref,
        })
    }

    /// The proc's logical identity.
    pub fn proc_id(&self) -> &ProcId {
        self.handle.proc_id()
    }

    /// The proc's address (guaranteed available after ready).
    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }

    /// The agent actor reference (guaranteed available after ready).
    pub fn agent_ref(&self) -> &ActorRef<H::Agent> {
        &self.agent_ref
    }
}

/// Minimal uniform surface for a spawned-**proc** handle returned by
/// a `ProcManager`. Each manager can return its own concrete handle,
/// as long as it exposes these. A **proc** is the Hyperactor runtime
/// + its actors (lifecycle controlled via `Proc` APIs such as
/// `destroy_and_wait`). A proc **may** be hosted *inside* an OS
/// **process**, but it is conceptually distinct:
///
/// - `LocalProcManager`: runs the proc **in this OS process**; there
///   is no child process to signal. Lifecycle is entirely proc-level.
/// - `ProcessProcManager` (test-only here): launches an **external OS
///   process** which hosts the proc, but this toy manager does
///   **not** wire a control plane for shutdown, nor an exit monitor.
///
/// This trait is therefore written in terms of the **proc**
/// lifecycle:
///
/// - `ready()` resolves when the proc is Ready (mailbox bound; agent
///   available).
/// - `wait()` resolves with the proc's terminal status
///   (Stopped/Killed/Failed).
/// - `terminate()` requests a graceful shutdown of the *proc* and
///   waits up to the deadline; managers that also own a child OS
///   process may escalate to `SIGKILL` if the proc does not exit in
///   time.
/// - `kill()` requests an immediate, forced termination. For
///    in-process procs, this may be implemented as an immediate
///    drain/abort of actor tasks. For external procs, this is
///    typically a `SIGKILL`.
///
/// The shape of the terminal value is `Self::TerminalStatus`.
/// Managers that track rich info (exit code, signal, address, agent)
/// can expose it; trivial managers may use `()`.
///
/// Managers that do not support signaling must return `Unsupported`.
#[async_trait]
pub trait ProcHandle: Clone + Send + Sync + 'static {
    /// The agent actor type installed in the proc by the manager.
    /// Must implement both:
    /// - [`Actor`], because the agent actually runs inside the proc,
    ///   and
    /// - [`Referable`], so callers can hold `ActorRef<Self::Agent>`.
    type Agent: Actor + Referable;

    /// The type of terminal status produced when the proc exits.
    ///
    /// For example, an external proc manager may use a rich status
    /// enum (e.g. `ProcStatus`), while an in-process manager may use
    /// a trivial unit type. This is the value returned by
    /// [`ProcHandle::wait`] and carried by [`ReadyError::Terminal`].
    type TerminalStatus: std::fmt::Debug + Clone + Send + Sync + 'static;

    /// The proc's logical identity on this host.
    fn proc_id(&self) -> &ProcId;

    /// The proc's address (the one callers bind into the host
    /// router). May return `None` before `ready()` completes.
    /// Guaranteed to return `Some` after `ready()` succeeds.
    ///
    /// **Prefer [`ready_proc()`]** for type-safe access that
    /// guarantees availability at compile time.
    fn addr(&self) -> Option<ChannelAddr>;

    /// The agent actor reference hosted in the proc. May return
    /// `None` before `ready()` completes. Guaranteed to return `Some`
    /// after `ready()` succeeds.
    ///
    /// **Prefer [`ready_proc()`]** for type-safe access that
    /// guarantees availability at compile time.
    fn agent_ref(&self) -> Option<ActorRef<Self::Agent>>;

    /// Resolves when the proc becomes Ready. Multi-waiter,
    /// non-consuming.
    async fn ready(&self) -> Result<(), ReadyError<Self::TerminalStatus>>;

    /// Resolves with the terminal status (Stopped/Killed/Failed/etc).
    /// Multi-waiter, non-consuming.
    async fn wait(&self) -> Result<Self::TerminalStatus, WaitError>;

    /// Politely stop the proc before the deadline; managers that own
    /// a child OS process may escalate to a forced kill at the
    /// deadline. Idempotent and race-safe: concurrent callers
    /// coalesce; the first terminal outcome wins and all callers
    /// observe it via `wait()`.
    ///
    /// Returns the single terminal status the proc reached (the same
    /// value `wait()` will return). Never fabricates terminal states:
    /// this is only returned after the exit monitor observes
    /// termination.
    async fn terminate(
        &self,
        cx: &impl context::Actor,
        timeout: Duration,
    ) -> Result<Self::TerminalStatus, TerminateError<Self::TerminalStatus>>;

    /// Force the proc down immediately. For in-process managers this
    /// may abort actor tasks; for external managers this typically
    /// sends `SIGKILL`. Also idempotent/race-safe; the terminal
    /// outcome is the one observed by `wait()`.
    async fn kill(&self) -> Result<Self::TerminalStatus, TerminateError<Self::TerminalStatus>>;
}

/// A trait describing a manager of procs, responsible for bootstrapping
/// procs on a host, and managing their lifetimes. The manager spawns an
/// `Agent`-typed actor on each proc, responsible for managing the proc.
#[async_trait]
pub trait ProcManager {
    /// Concrete handle type this manager returns.
    type Handle: ProcHandle;

    /// Additional configuration for the proc, supported by this manager.
    type Config = ();

    /// The preferred transport for this ProcManager.
    /// In practice this will be [`ChannelTransport::Local`]
    /// for testing, and [`ChannelTransport::Unix`] for external
    /// processes.
    fn transport(&self) -> ChannelTransport;

    /// Spawn a new proc with the provided proc id. The proc
    /// should use the provided forwarder address for messages
    /// destined outside of the proc. The returned address accepts
    /// messages destined for the proc.
    ///
    /// An agent actor is also spawned, and the corresponding actor
    /// ref is returned.
    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
        config: Self::Config,
    ) -> Result<Self::Handle, HostError>;
}

/// Type alias for the agent actor managed by a given [`ProcManager`].
///
/// This resolves to the `Agent` type exposed by the manager's
/// associated `Handle` (via [`ProcHandle::Agent`]). It provides a
/// convenient shorthand so call sites can refer to
/// `ActorRef<ManagerAgent<M>>` instead of the more verbose
/// `<M::Handle as ProcHandle>::Agent`.
///
/// # Example
/// ```ignore
/// fn takes_agent_ref<M: ProcManager>(r: ActorRef<ManagerAgent<M>>) { … }
/// ```
pub type ManagerAgent<M> = <<M as ProcManager>::Handle as ProcHandle>::Agent; // rust issue #112792

/// A ProcManager that spawns **in-process** procs (test-only).
///
/// The proc runs inside this same OS process; there is **no** child
/// process to signal. Lifecycle is purely proc-level:
/// - `terminate(timeout)`: delegates to
///   `Proc::destroy_and_wait(timeout, None)`, which drains and, at the
///   deadline, aborts remaining actors.
/// - `kill()`: uses a zero deadline to emulate a forced stop via
///   `destroy_and_wait(Duration::ZERO, None)`.
/// - `wait()`: trivial (no external lifecycle to observe).
///
///   No OS signals are sent or required.
pub struct LocalProcManager<S> {
    procs: Arc<Mutex<HashMap<ProcId, Proc>>>,
    spawn: S,
}

impl<S> LocalProcManager<S> {
    /// Create a new in-process proc manager with the given agent
    /// params.
    pub fn new(spawn: S) -> Self {
        Self {
            procs: Arc::new(Mutex::new(HashMap::new())),
            spawn,
        }
    }
}

#[async_trait]
impl<S> BulkTerminate for LocalProcManager<S>
where
    S: Send + Sync,
{
    async fn terminate_all(
        &self,
        _cx: &impl context::Actor,
        timeout: std::time::Duration,
        max_in_flight: usize,
    ) -> TerminateSummary {
        // Snapshot procs so we don't hold the lock across awaits.
        let procs: Vec<Proc> = {
            let guard = self.procs.lock().await;
            guard.values().cloned().collect()
        };

        let attempted = procs.len();

        let results = stream::iter(procs.into_iter().map(|mut p| async move {
            // For local manager, graceful proc-level stop.
            match p.destroy_and_wait::<()>(timeout, None).await {
                Ok(_) => true,
                Err(e) => {
                    tracing::warn!(error=%e, "terminate_all(local): destroy_and_wait failed");
                    false
                }
            }
        }))
        .buffer_unordered(max_in_flight.max(1))
        .collect::<Vec<bool>>()
        .await;

        let ok = results.into_iter().filter(|b| *b).count();

        TerminateSummary {
            attempted,
            ok,
            failed: attempted.saturating_sub(ok),
        }
    }
}

#[async_trait::async_trait]
impl<S> SingleTerminate for LocalProcManager<S>
where
    S: Send + Sync,
{
    async fn terminate_proc(
        &self,
        _cx: &impl context::Actor,
        proc: &ProcId,
        timeout: std::time::Duration,
    ) -> Result<(Vec<ActorId>, Vec<ActorId>), anyhow::Error> {
        // Snapshot procs so we don't hold the lock across awaits.
        let procs: Option<Proc> = {
            let mut guard = self.procs.lock().await;
            guard.remove(proc)
        };
        if let Some(mut p) = procs {
            p.destroy_and_wait::<()>(timeout, None).await
        } else {
            Err(anyhow::anyhow!("proc {} doesn't exist", proc))
        }
    }
}

/// A lightweight [`ProcHandle`] for procs managed **in-process** via
/// [`LocalProcManager`].
///
/// This handle wraps the minimal identifying state of a spawned proc:
/// - its [`ProcId`] (logical identity on the host),
/// - the proc's [`ChannelAddr`] (the address callers bind into the
///   host router), and
/// - the [`ActorRef`] to the agent actor hosted in the proc.
///
/// Unlike external handles, `LocalHandle` does **not** manage an OS
/// child process. It provides a uniform surface (`proc_id()`,
/// `addr()`, `agent_ref()`) and implements `terminate()`/`kill()` by
/// calling into the underlying `Proc::destroy_and_wait`, i.e.,
/// **proc-level** shutdown.
///
/// **Type parameter:** `A` is constrained by the `ProcHandle::Agent`
/// bound (`Actor + Referable`).
pub struct LocalHandle<A: Actor + Referable> {
    proc_id: ProcId,
    addr: ChannelAddr,
    agent_ref: ActorRef<A>,
    procs: Arc<Mutex<HashMap<ProcId, Proc>>>,
}

// Manual `Clone` to avoid requiring `A: Clone`.
impl<A: Actor + Referable> Clone for LocalHandle<A> {
    fn clone(&self) -> Self {
        Self {
            proc_id: self.proc_id.clone(),
            addr: self.addr.clone(),
            agent_ref: self.agent_ref.clone(),
            procs: Arc::clone(&self.procs),
        }
    }
}

#[async_trait]
impl<A: Actor + Referable> ProcHandle for LocalHandle<A> {
    /// `Agent = A` (inherits `Actor + Referable` from the trait
    /// bound).
    type Agent = A;
    type TerminalStatus = ();

    fn proc_id(&self) -> &ProcId {
        &self.proc_id
    }

    fn addr(&self) -> Option<ChannelAddr> {
        Some(self.addr.clone())
    }

    fn agent_ref(&self) -> Option<ActorRef<Self::Agent>> {
        Some(self.agent_ref.clone())
    }

    /// Always resolves immediately: a local proc is created
    /// in-process and is usable as soon as the handle exists.
    async fn ready(&self) -> Result<(), ReadyError<Self::TerminalStatus>> {
        Ok(())
    }
    /// Always resolves immediately with `()`: a local proc has no
    /// external lifecycle to await. There is no OS child process
    /// behind this handle.
    async fn wait(&self) -> Result<Self::TerminalStatus, WaitError> {
        Ok(())
    }

    async fn terminate(
        &self,
        _cx: &impl context::Actor,
        timeout: Duration,
    ) -> Result<(), TerminateError<Self::TerminalStatus>> {
        let mut proc = {
            let guard = self.procs.lock().await;
            match guard.get(self.proc_id()) {
                Some(p) => p.clone(),
                None => {
                    // The proc was already removed; treat as already
                    // terminal.
                    return Err(TerminateError::AlreadyTerminated(()));
                }
            }
        };

        // Graceful stop of the *proc* (actors) with a deadline. This
        // will drain and then abort remaining actors at expiry.
        let _ = proc
            .destroy_and_wait::<()>(timeout, None)
            .await
            .map_err(TerminateError::Io)?;

        Ok(())
    }

    async fn kill(&self) -> Result<(), TerminateError<Self::TerminalStatus>> {
        // Forced stop == zero deadline; `destroy_and_wait` will
        // immediately abort remaining actors and return.
        let mut proc = {
            let guard = self.procs.lock().await;
            match guard.get(self.proc_id()) {
                Some(p) => p.clone(),
                None => return Err(TerminateError::AlreadyTerminated(())),
            }
        };

        let _ = proc
            .destroy_and_wait::<()>(Duration::from_millis(0), None)
            .await
            .map_err(TerminateError::Io)?;

        Ok(())
    }
}

/// Local, in-process ProcManager.
///
/// **Type bounds:**
/// - `A: Actor + Referable + Binds<A>`
///   - `Actor`: the agent actually runs inside the proc.
///   - `Referable`: callers hold `ActorRef<A>` to the agent; this
///     bound is required for typed remote refs.
///   - `Binds<A>`: lets the runtime wire the agent's message ports.
/// - `F: Future<Output = anyhow::Result<ActorHandle<A>>> + Send`:
///   the spawn closure returns a Send future (we `tokio::spawn` it).
/// - `S: Fn(Proc) -> F + Sync`: the factory can be called from
///   concurrent contexts.
///
/// Result handle is `LocalHandle<A>` (whose `Agent = A` via `ProcHandle`).
#[async_trait]
impl<A, S, F> ProcManager for LocalProcManager<S>
where
    A: Actor + Referable + Binds<A>,
    F: Future<Output = anyhow::Result<ActorHandle<A>>> + Send,
    S: Fn(Proc) -> F + Sync,
{
    type Handle = LocalHandle<A>;

    fn transport(&self) -> ChannelTransport {
        ChannelTransport::Local
    }

    #[crate::instrument(fields(proc_id=proc_id.to_string(), addr=forwarder_addr.to_string()))]
    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
        _config: (),
    ) -> Result<Self::Handle, HostError> {
        let transport = forwarder_addr.transport();
        let proc = Proc::new(
            proc_id.clone(),
            MailboxClient::dial(forwarder_addr)?.into_boxed(),
        );
        let (proc_addr, rx) = channel::serve(ChannelAddr::any(transport))?;
        self.procs
            .lock()
            .await
            .insert(proc_id.clone(), proc.clone());
        let _handle = proc.clone().serve(rx);
        let agent_handle = (self.spawn)(proc)
            .await
            .map_err(|e| HostError::AgentSpawnFailure(proc_id.clone(), e))?;

        Ok(LocalHandle {
            proc_id,
            addr: proc_addr,
            agent_ref: agent_handle.bind(),
            procs: Arc::clone(&self.procs),
        })
    }
}

/// A ProcManager that manages each proc as a **separate OS process**
/// (test-only toy).
///
/// This implementation launches a child via `Command` and relies on
/// `kill_on_drop(true)` so that children are SIGKILLed if the manager
/// (or host) drops. There is **no** proc control plane (no RPC to a
/// proc agent for shutdown) and **no** exit monitor wired here.
/// Consequently:
/// - `terminate()` and `kill()` return `Unsupported`.
/// - `wait()` is trivial (no lifecycle observation).
///
/// It follows a simple protocol:
///
/// Each process is launched with the following environment variables:
/// - `HYPERACTOR_HOST_BACKEND_ADDR`: the backend address to which all messages are forwarded,
/// - `HYPERACTOR_HOST_PROC_ID`: the proc id to assign the launched proc, and
/// - `HYPERACTOR_HOST_CALLBACK_ADDR`: the channel address with which to return the proc's address
///
/// The launched proc should also spawn an actor to manage it - the details of this are
/// implementation dependent, and outside the scope of the process manager.
///
/// The function [`boot_proc`] provides a convenient implementation of the
/// protocol.
pub struct ProcessProcManager<A> {
    program: std::path::PathBuf,
    children: Arc<Mutex<HashMap<ProcId, Child>>>,
    _phantom: PhantomData<A>,
}

impl<A> ProcessProcManager<A> {
    /// Create a new ProcessProcManager that runs the provided
    /// command.
    pub fn new(program: std::path::PathBuf) -> Self {
        Self {
            program,
            children: Arc::new(Mutex::new(HashMap::new())),
            _phantom: PhantomData,
        }
    }
}

impl<A> Drop for ProcessProcManager<A> {
    fn drop(&mut self) {
        // When the manager is dropped, `children` is dropped, which
        // drops each `Child` handle. With `kill_on_drop(true)`, the OS
        // will SIGKILL the processes. Nothing else to do here.
    }
}

/// A [`ProcHandle`] implementation for procs managed as separate
/// OS processes via [`ProcessProcManager`].
///
/// This handle records the logical identity and connectivity of an
/// external child process:
/// - its [`ProcId`] (unique identity on the host),
/// - the proc's [`ChannelAddr`] (address registered in the host
///   router),
/// - and the [`ActorRef`] of the agent actor spawned inside the proc.
///
/// Unlike [`LocalHandle`], this corresponds to a real OS process
/// launched by the manager. In this **toy** implementation the handle
/// does not own/monitor the `Child` and there is no shutdown control
/// plane. It is a stable, clonable surface exposing the proc's
/// identity, address, and agent reference so host code can interact
/// uniformly with local/external procs. `terminate()`/`kill()` are
/// intentionally `Unsupported` here; process cleanup relies on
/// `cmd.kill_on_drop(true)` when launching the child (the OS will
/// SIGKILL it if the handle is dropped).
///
/// The type bound `A: Actor + Referable` comes from the
/// [`ProcHandle::Agent`] requirement: `Actor` because the agent
/// actually runs inside the proc, and `Referable` because it must
/// be referenceable via [`ActorRef<A>`] (i.e., safe to carry as a
/// typed remote reference).
#[derive(Debug)]
pub struct ProcessHandle<A: Actor + Referable> {
    proc_id: ProcId,
    addr: ChannelAddr,
    agent_ref: ActorRef<A>,
}

// Manual `Clone` to avoid requiring `A: Clone`.
impl<A: Actor + Referable> Clone for ProcessHandle<A> {
    fn clone(&self) -> Self {
        Self {
            proc_id: self.proc_id.clone(),
            addr: self.addr.clone(),
            agent_ref: self.agent_ref.clone(),
        }
    }
}

#[async_trait]
impl<A: Actor + Referable> ProcHandle for ProcessHandle<A> {
    /// Agent must be both an `Actor` (runs in the proc) and a
    /// `Referable` (so it can be referenced via `ActorRef<A>`).
    type Agent = A;
    type TerminalStatus = ();

    fn proc_id(&self) -> &ProcId {
        &self.proc_id
    }

    fn addr(&self) -> Option<ChannelAddr> {
        Some(self.addr.clone())
    }

    fn agent_ref(&self) -> Option<ActorRef<Self::Agent>> {
        Some(self.agent_ref.clone())
    }

    /// Resolves immediately. `ProcessProcManager::spawn` returns this
    /// handle only after the child has called back with (addr,
    /// agent), i.e. after readiness.
    async fn ready(&self) -> Result<(), ReadyError<Self::TerminalStatus>> {
        Ok(())
    }
    /// Resolves immediately with `()`. This handle does not track
    /// child lifecycle; there is no watcher in this implementation.
    async fn wait(&self) -> Result<Self::TerminalStatus, WaitError> {
        Ok(())
    }

    async fn terminate(
        &self,
        _cx: &impl context::Actor,
        _deadline: Duration,
    ) -> Result<(), TerminateError<Self::TerminalStatus>> {
        Err(TerminateError::Unsupported)
    }

    async fn kill(&self) -> Result<(), TerminateError<Self::TerminalStatus>> {
        Err(TerminateError::Unsupported)
    }
}

#[async_trait]
impl<A> ProcManager for ProcessProcManager<A>
where
    // Agent actor runs in the proc (`Actor`) and must be
    // referenceable (`Referable`).
    A: Actor + Referable + Sync,
{
    type Handle = ProcessHandle<A>;

    fn transport(&self) -> ChannelTransport {
        ChannelTransport::Unix
    }

    #[crate::instrument(fields(proc_id=proc_id.to_string(), addr=forwarder_addr.to_string()))]
    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
        _config: (),
    ) -> Result<Self::Handle, HostError> {
        let (callback_addr, mut callback_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix))?;

        let mut cmd = Command::new(&self.program);
        cmd.env("HYPERACTOR_HOST_PROC_ID", proc_id.to_string());
        cmd.env("HYPERACTOR_HOST_BACKEND_ADDR", forwarder_addr.to_string());
        cmd.env("HYPERACTOR_HOST_CALLBACK_ADDR", callback_addr.to_string());

        // Lifetime strategy: mark the child with
        // `kill_on_drop(true)` so the OS will send SIGKILL if the
        // handle is dropped and retain the `Child` in
        // `self.children`, tying its lifetime to the manager/host.
        //
        // This is the simplest viable policy to avoid orphaned
        // subprocesses in CI; more sophisticated lifecycle control
        // (graceful shutdown, restart) will be layered on later.

        // Kill the child when its handle is dropped.
        cmd.kill_on_drop(true);

        let child = cmd
            .spawn()
            .map_err(|e| HostError::ProcessSpawnFailure(proc_id.clone(), e))?;

        // Retain the handle so it lives for the life of the
        // manager/host.
        {
            let mut children = self.children.lock().await;
            children.insert(proc_id.clone(), child);
        }

        // Wait for the child's callback with (addr, agent_ref)
        let (proc_addr, agent_ref) = callback_rx.recv().await?;

        // TODO(production): For a non-test implementation, plumb a
        // shutdown path:
        // - expose a proc-level graceful stop RPC on the agent and
        //   implement `terminate(timeout)` by invoking it and, on
        //   deadline, call `Child::kill()`; implement `kill()` as
        //   immediate `Child::kill()`.
        // - wire an exit monitor so `wait()` resolves with a real
        //   terminal status.
        Ok(ProcessHandle {
            proc_id,
            addr: proc_addr,
            agent_ref,
        })
    }
}

impl<A> ProcessProcManager<A>
where
    // `Actor`: runs in the proc; `Referable`: referenceable via
    // ActorRef; `Binds<A>`: wires ports.
    A: Actor + Referable + Binds<A>,
{
    /// Boot a process in a ProcessProcManager<A>. Should be called from processes spawned
    /// by the process manager. `boot_proc` will spawn the provided actor type (with parameters)
    /// onto the newly created Proc, and bind its handler. This allows the user to install an agent to
    /// manage the proc itself.
    pub async fn boot_proc<S, F>(spawn: S) -> Result<Proc, HostError>
    where
        S: FnOnce(Proc) -> F,
        F: Future<Output = Result<ActorHandle<A>, anyhow::Error>>,
    {
        let proc_id: ProcId = Self::parse_env("HYPERACTOR_HOST_PROC_ID")?;
        let backend_addr: ChannelAddr = Self::parse_env("HYPERACTOR_HOST_BACKEND_ADDR")?;
        let callback_addr: ChannelAddr = Self::parse_env("HYPERACTOR_HOST_CALLBACK_ADDR")?;
        spawn_proc(proc_id, backend_addr, callback_addr, spawn).await
    }

    fn parse_env<T, E>(key: &str) -> Result<T, HostError>
    where
        T: FromStr<Err = E>,
        E: Into<anyhow::Error>,
    {
        std::env::var(key)
            .map_err(|e| HostError::MissingParameter(key.to_string(), e))?
            .parse()
            .map_err(|e: E| HostError::InvalidParameter(key.to_string(), e.into()))
    }
}

/// Spawn a proc at `proc_id` with an `A`-typed agent actor,
/// forwarding messages to the provided `backend_addr`,
/// and returning the proc's address and agent actor on
/// the provided `callback_addr`.
#[crate::instrument(fields(proc_id=proc_id.to_string(), addr=backend_addr.to_string(), callback_addr=callback_addr.to_string()))]
pub async fn spawn_proc<A, S, F>(
    proc_id: ProcId,
    backend_addr: ChannelAddr,
    callback_addr: ChannelAddr,
    spawn: S,
) -> Result<Proc, HostError>
where
    // `Actor`: runs in the proc; `Referable`: allows ActorRef<A>;
    // `Binds<A>`: wires ports
    A: Actor + Referable + Binds<A>,
    S: FnOnce(Proc) -> F,
    F: Future<Output = Result<ActorHandle<A>, anyhow::Error>>,
{
    let backend_transport = backend_addr.transport();
    let proc = Proc::new(
        proc_id.clone(),
        MailboxClient::dial(backend_addr)?.into_boxed(),
    );

    let agent_handle = spawn(proc.clone())
        .await
        .map_err(|e| HostError::AgentSpawnFailure(proc_id.clone(), e))?;

    // Finally serve the proc on the same transport as the backend address,
    // and call back.
    let (proc_addr, proc_rx) = channel::serve(ChannelAddr::any(backend_transport))?;
    proc.clone().serve(proc_rx);
    channel::dial(callback_addr)?
        .send((proc_addr, agent_handle.bind::<A>()))
        .await
        .map_err(ChannelError::from)?;

    Ok(proc)
}

/// Testing support for hosts. This is linked outside of cfg(test)
/// as it is needed by an external binary.
pub mod testing {
    use async_trait::async_trait;

    use crate as hyperactor;
    use crate::Actor;
    use crate::ActorId;
    use crate::Context;
    use crate::Handler;
    use crate::OncePortRef;

    /// Just a simple actor, available in both the bootstrap binary as well as
    /// hyperactor tests.
    #[derive(Debug, Default)]
    #[hyperactor::export(handlers = [OncePortRef<ActorId>])]
    pub struct EchoActor;

    impl Actor for EchoActor {}

    #[async_trait]
    impl Handler<OncePortRef<ActorId>> for EchoActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            reply: OncePortRef<ActorId>,
        ) -> Result<(), anyhow::Error> {
            reply.send(cx, cx.self_id().clone())?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::testing::EchoActor;
    use super::*;
    use crate::channel::ChannelTransport;
    use crate::clock::Clock;
    use crate::clock::RealClock;
    use crate::context::Mailbox;

    #[tokio::test]
    async fn test_basic() {
        let proc_manager =
            LocalProcManager::new(|proc: Proc| async move { proc.spawn::<()>("agent", ()) });
        let procs = Arc::clone(&proc_manager.procs);
        let mut host = Host::new(proc_manager, ChannelAddr::any(ChannelTransport::Local))
            .await
            .unwrap();

        let (proc_id1, _ref) = host.spawn("proc1".to_string(), ()).await.unwrap();
        assert_eq!(
            proc_id1,
            ProcId::Direct(host.addr().clone(), "proc1".to_string())
        );
        assert!(procs.lock().await.contains_key(&proc_id1));

        let (proc_id2, _ref) = host.spawn("proc2".to_string(), ()).await.unwrap();
        assert!(procs.lock().await.contains_key(&proc_id2));

        let proc1 = procs.lock().await.get(&proc_id1).unwrap().clone();
        let proc2 = procs.lock().await.get(&proc_id2).unwrap().clone();

        // Make sure they can talk to each other:
        let (instance1, _handle) = proc1.instance("client").unwrap();
        let (instance2, _handle) = proc2.instance("client").unwrap();

        let (port, mut rx) = instance1.mailbox().open_port();

        port.bind().send(&instance2, "hello".to_string()).unwrap();
        assert_eq!(rx.recv().await.unwrap(), "hello".to_string());

        // Make sure that the system proc is also wired in correctly.
        let (system_actor, _handle) = host.system_proc().instance("test").unwrap();

        // system->proc
        port.bind()
            .send(&system_actor, "hello from the system proc".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the system proc".to_string()
        );

        // system->system
        let (port, mut rx) = system_actor.mailbox().open_port();
        port.bind()
            .send(&system_actor, "hello from the system".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the system".to_string()
        );

        // proc->system
        port.bind()
            .send(&instance1, "hello from the instance1".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the instance1".to_string()
        );
    }

    #[tokio::test]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: ReadFailed { manifest_path: "/meta-pytorch/monarch/target/debug/deps/hyperactor-0e1fe83af739d976.resources.json", source: Os { code: 2, kind: NotFound, message: "No such file or directory" } }
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_process_proc_manager() {
        hyperactor_telemetry::initialize_logging(crate::clock::ClockKind::default());

        // EchoActor is "agent" used to test connectivity.
        let process_manager = ProcessProcManager::<EchoActor>::new(
            buck_resources::get("monarch/hyperactor/bootstrap").unwrap(),
        );
        let mut host = Host::new(process_manager, ChannelAddr::any(ChannelTransport::Unix))
            .await
            .unwrap();

        // Manually serve this: the agent isn't actually doing anything in this case,
        // but we are testing connectivity.
        host.serve();

        // (1) Spawn and check invariants.
        assert!(matches!(host.addr().transport(), ChannelTransport::Unix));
        let (proc1, echo1) = host.spawn("proc1".to_string(), ()).await.unwrap();
        let (proc2, echo2) = host.spawn("proc2".to_string(), ()).await.unwrap();
        assert_eq!(echo1.actor_id().proc_id(), &proc1);
        assert_eq!(echo2.actor_id().proc_id(), &proc2);

        // (2) Duplicate name rejection.
        let dup = host.spawn("proc1".to_string(), ()).await;
        assert!(matches!(dup, Err(HostError::ProcExists(_))));

        // (3) Create a standalone client proc and verify echo1 agent responds.
        // Request: client proc -> host frontend/router -> echo1 (proc1).
        // Reply:   echo1 (proc1) -> host backend -> host router -> client port.
        // This confirms that an external proc (created via
        // `Proc::direct`) can address a child proc through the host,
        // and receive a correct reply.
        let client = Proc::direct(
            ChannelAddr::any(host.addr().transport()),
            "test".to_string(),
        )
        .unwrap();
        let (client_inst, _h) = client.instance("test").unwrap();
        let (port, rx) = client_inst.mailbox().open_once_port();
        echo1.send(&client_inst, port.bind()).unwrap();
        let id = RealClock
            .timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id, *echo1.actor_id());

        // (4) Child <-> external client request -> reply:
        // Request: client proc (standalone via `Proc::direct`) ->
        //          host frontend/router -> echo2 (proc2).
        // Reply:   echo2 (proc2) -> host backend -> host router ->
        //          client port (standalone proc).
        // This exercises cross-proc routing between a child and an
        // external client under the same host.
        let (port2, rx2) = client_inst.mailbox().open_once_port();
        echo2.send(&client_inst, port2.bind()).unwrap();
        let id2 = RealClock
            .timeout(Duration::from_secs(5), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id2, *echo2.actor_id());

        // (5) System -> child request -> cross-proc reply:
        // Request: system proc -> host router (frontend) -> echo1
        //          (proc1, child).
        // Reply: echo1 (proc1) -> proc1 forwarder -> host backend ->
        //        host router -> client proc direct addr (Proc::direct) ->
        //        client port.
        // Because `client_inst` runs in its own proc, the reply
        // traverses the host (not local delivery within proc1).
        let (sys_inst, _h) = host.system_proc().instance("sys-client").unwrap();
        let (port3, rx3) = client_inst.mailbox().open_once_port();
        // Send from system -> child via a message that ultimately
        // replies to client's port
        echo1.send(&sys_inst, port3.bind()).unwrap();
        let id3 = RealClock
            .timeout(Duration::from_secs(5), rx3.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id3, *echo1.actor_id());
    }

    #[tokio::test]
    async fn local_ready_and_wait_are_immediate() {
        // Build a LocalHandle directly.
        let addr = ChannelAddr::any(ChannelTransport::Local);
        let proc_id = ProcId::Direct(addr.clone(), "p".into());
        let agent_ref = ActorRef::<()>::attest(proc_id.actor_id("agent", 0));
        let h = LocalHandle::<()> {
            proc_id,
            addr,
            agent_ref,
            procs: Arc::new(Mutex::new(HashMap::new())),
        };

        // ready() resolves immediately
        assert!(h.ready().await.is_ok());

        // wait() resolves immediately with unit TerminalStatus
        assert!(h.wait().await.is_ok());

        // Multiple concurrent waiters both succeed
        let (r1, r2) = tokio::join!(h.ready(), h.ready());
        assert!(r1.is_ok() && r2.is_ok());
    }

    // --
    // Fixtures for `host::spawn` tests.

    #[derive(Debug, Clone, Copy)]
    enum ReadyMode {
        OkAfter(Duration),
        ErrTerminal,
        ErrChannelClosed,
    }

    #[derive(Debug, Clone)]
    struct TestHandle {
        id: ProcId,
        addr: ChannelAddr,
        agent: ActorRef<()>,
        mode: ReadyMode,
        omit_addr: bool,
        omit_agent: bool,
    }

    #[async_trait::async_trait]
    impl ProcHandle for TestHandle {
        type Agent = ();
        type TerminalStatus = ();

        fn proc_id(&self) -> &ProcId {
            &self.id
        }

        fn addr(&self) -> Option<ChannelAddr> {
            if self.omit_addr {
                None
            } else {
                Some(self.addr.clone())
            }
        }

        fn agent_ref(&self) -> Option<ActorRef<Self::Agent>> {
            if self.omit_agent {
                None
            } else {
                Some(self.agent.clone())
            }
        }

        async fn ready(&self) -> Result<(), ReadyError<Self::TerminalStatus>> {
            match self.mode {
                ReadyMode::OkAfter(d) => {
                    if !d.is_zero() {
                        RealClock.sleep(d).await;
                    }
                    Ok(())
                }
                ReadyMode::ErrTerminal => Err(ReadyError::Terminal(())),
                ReadyMode::ErrChannelClosed => Err(ReadyError::ChannelClosed),
            }
        }
        async fn wait(&self) -> Result<Self::TerminalStatus, WaitError> {
            Ok(())
        }
        async fn terminate(
            &self,
            _cx: &impl context::Actor,
            _timeout: Duration,
        ) -> Result<Self::TerminalStatus, TerminateError<Self::TerminalStatus>> {
            Err(TerminateError::Unsupported)
        }
        async fn kill(&self) -> Result<Self::TerminalStatus, TerminateError<Self::TerminalStatus>> {
            Err(TerminateError::Unsupported)
        }
    }

    #[derive(Debug, Clone)]
    struct TestManager {
        mode: ReadyMode,
        omit_addr: bool,
        omit_agent: bool,
        transport: ChannelTransport,
    }

    impl TestManager {
        fn local(mode: ReadyMode) -> Self {
            Self {
                mode,
                omit_addr: false,
                omit_agent: false,
                transport: ChannelTransport::Local,
            }
        }
        fn with_omissions(mut self, addr: bool, agent: bool) -> Self {
            self.omit_addr = addr;
            self.omit_agent = agent;
            self
        }
    }

    #[async_trait::async_trait]
    impl ProcManager for TestManager {
        type Handle = TestHandle;

        fn transport(&self) -> ChannelTransport {
            self.transport.clone()
        }

        async fn spawn(
            &self,
            proc_id: ProcId,
            forwarder_addr: ChannelAddr,
            _config: (),
        ) -> Result<Self::Handle, HostError> {
            let agent = ActorRef::<()>::attest(proc_id.actor_id("agent", 0));
            Ok(TestHandle {
                id: proc_id,
                addr: forwarder_addr,
                agent,
                mode: self.mode,
                omit_addr: self.omit_addr,
                omit_agent: self.omit_agent,
            })
        }
    }

    #[tokio::test]
    async fn host_spawn_times_out_when_configured() {
        let cfg = hyperactor_config::global::lock();
        let _g = cfg.override_key(
            crate::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_millis(10),
        );

        let mut host = Host::new(
            TestManager::local(ReadyMode::OkAfter(Duration::from_millis(50))),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let err = host.spawn("t".into(), ()).await.expect_err("must time out");
        assert!(matches!(err, HostError::ProcessConfigurationFailure(_, _)));
    }

    #[tokio::test]
    async fn host_spawn_timeout_zero_disables_and_succeeds() {
        let cfg = hyperactor_config::global::lock();
        let _g = cfg.override_key(
            crate::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(0),
        );

        let mut host = Host::new(
            TestManager::local(ReadyMode::OkAfter(Duration::from_millis(20))),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let (pid, agent) = host.spawn("ok".into(), ()).await.expect("must succeed");
        assert_eq!(agent.actor_id().proc_id(), &pid);
        assert!(host.procs.contains("ok"));
    }

    #[tokio::test]
    async fn host_spawn_maps_channel_closed_ready_error_to_config_failure() {
        let mut host = Host::new(
            TestManager::local(ReadyMode::ErrChannelClosed),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let err = host.spawn("p".into(), ()).await.expect_err("must fail");
        assert!(matches!(err, HostError::ProcessConfigurationFailure(_, _)));
    }

    #[tokio::test]
    async fn host_spawn_maps_terminal_ready_error_to_config_failure() {
        let mut host = Host::new(
            TestManager::local(ReadyMode::ErrTerminal),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let err = host.spawn("p".into(), ()).await.expect_err("must fail");
        assert!(matches!(err, HostError::ProcessConfigurationFailure(_, _)));
    }

    #[tokio::test]
    async fn host_spawn_fails_if_ready_but_missing_addr() {
        let mut host = Host::new(
            TestManager::local(ReadyMode::OkAfter(Duration::ZERO)).with_omissions(true, false),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let err = host
            .spawn("no-addr".into(), ())
            .await
            .expect_err("must fail");
        assert!(matches!(err, HostError::ProcessConfigurationFailure(_, _)));
    }

    #[tokio::test]
    async fn host_spawn_fails_if_ready_but_missing_agent() {
        let mut host = Host::new(
            TestManager::local(ReadyMode::OkAfter(Duration::ZERO)).with_omissions(false, true),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let err = host
            .spawn("no-agent".into(), ())
            .await
            .expect_err("must fail");
        assert!(matches!(err, HostError::ProcessConfigurationFailure(_, _)));
    }
}
