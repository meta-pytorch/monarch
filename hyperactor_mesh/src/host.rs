/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Host lifecycle management for procs running on one machine.
//!
//! A [`Host`] owns one [`Gateway`] and one [`ProcManager`]. The manager makes
//! procs real, either by spawning them in-process for tests or by launching
//! separate OS processes. The gateway owns connectivity: it serves the backend
//! and frontend endpoints, multiplexes inbound traffic to in-process procs,
//! and routes spawned child proc gateways through peer routes.
//!
//! Use [`Host::new`] or [`Host::new_with_gateway`] to construct a host, start its
//! backend and frontend accept loops, then [`Host::spawn`] to create child procs.
//! Spawned children are returned as [`ProcAddr`] values whose location is
//! advertised through this host's gateway.
//!
//! ## Gateway topology
//!
//! A host gateway exposes a frontend endpoint `*` and serves a backend
//! endpoint `#` for child proc gateways. Each spawned child has its own
//! gateway endpoint (`#1`, `#2`, ...), uses `#` as its forwarder, and is
//! advertised as `Via(child_uid, host_location)`. The `host_location` is the
//! host gateway's advertised location: the newest active frontend serve or
//! `serve_via` session.
//!
//! The host gateway keeps one peer route per child uid. When it receives a
//! message for `Via(child_uid, host_location)`, it peels the child hop and
//! forwards the envelope to the child's gateway. In-process service and local
//! procs are delivered through the gateway's local proc table.
//!
//! ```text
//! inbound to host_location
//! (*, or Via(...) when attached)
//!          |
//!          v
//!   +---------------+     peer child_uid_1 -> #1     +----------------------+
//!   | Host Gateway  |------------------------------->| child gateway/proc 1 |
//!   | local procs:  |                                | addr #1              |
//!   | service/local |<-------------------------------| forwarder -> #       |
//!   +---------------+          host backend #         +----------------------+
//!          |
//!          | peer child_uid_2 -> #2
//!          v
//!   +----------------------+
//!   | child gateway/proc 2 |
//!   | addr #2              |
//!   | forwarder -> #       |
//!   +----------------------+
//! ```
//!
//! The built-in service and local procs are created during construction and
//! run in-process on the host gateway. The local proc starts with no actors; a
//! `ProcAgent` and root client actor are added lazily when
//! `HostMeshAgent::handle(GetLocalProc)` first asks for it.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use async_trait::async_trait;
use futures::Future;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Gateway;
use hyperactor::Location;
use hyperactor::Proc;
use hyperactor::ProcAddr;
use hyperactor::ProcId;
use hyperactor::actor::Binds;
use hyperactor::actor::Referable;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelError;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::Rx;
use hyperactor::channel::ServerError;
use hyperactor::channel::Tx;
use hyperactor::context;
use hyperactor::gateway::GatewayServeHandle;
use hyperactor::gateway::PeerAttachError;
use hyperactor::gateway::PeerAttachGuard;
use hyperactor::id::Label;
use hyperactor::mailbox::IntoBoxedMailboxSender as _;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxServer;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::mesh_id::ResourceId;

/// Name of the system service proc on a host.
///
/// Hosts the admin actor layer: HostMeshAgent, MeshAdminAgent, and bridge.
pub const SERVICE_PROC_NAME: &str = hyperactor::proc::LEGACY_SERVICE_PROC_NAME;

pub(crate) fn legacy_service_proc_id() -> ProcId {
    ProcId::singleton(Label::strip(SERVICE_PROC_NAME))
}

/// Name of the local client proc on a host.
///
/// See LP-1 (lazy activation) in module doc.
///
/// In pure-Rust programs (e.g. sieve, dining_philosophers)
/// `GetLocalProc` is never sent, so the local proc remains empty
/// throughout the program's lifetime. Code that inspects the local
/// proc's actors must not assume they exist.
pub const LOCAL_PROC_NAME: &str = "local";

/// The type of error produced by host operations.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// A channel error occurred during a host operation.
    #[error(transparent)]
    ChannelError(#[from] ChannelError),

    /// A duplex server error occurred during a host operation.
    #[error(transparent)]
    ServerError(#[from] ServerError),

    /// The named proc already exists and cannot be spawned.
    #[error("proc '{0}' already exists")]
    ProcExists(String),

    /// The host's proc-name registry cannot be used safely.
    #[error("host proc-name registry is poisoned")]
    ProcRegistryPoisoned,

    /// Attaching a spawned proc to the host gateway failed.
    #[error("failed to attach proc '{0}' to the host gateway: {1}")]
    PeerAttachFailure(ProcAddr, #[source] PeerAttachError),

    /// The detached task spawning a proc panicked.
    #[error("proc '{0}' spawn panicked: {1}")]
    SpawnPanicked(String, String),

    /// Failures occuring while spawning a subprocess.
    #[error("proc '{0}' (command: {1}) failed to spawn process: {2}")]
    ProcessSpawnFailure(ProcAddr, String, #[source] std::io::Error),

    /// Failures occuring while configuring a subprocess.
    #[error("proc '{0}' failed to configure process: {1}")]
    ProcessConfigurationFailure(ProcAddr, #[source] anyhow::Error),

    /// Failures occuring while spawning a management actor in a proc.
    #[error("failed to spawn agent on proc '{0}': {1}")]
    AgentSpawnFailure(ProcAddr, #[source] anyhow::Error),

    /// An input parameter was missing.
    #[error("parameter '{0}' missing: {1}")]
    MissingParameter(String, std::env::VarError),

    /// An input parameter was invalid.
    #[error("parameter '{0}' invalid: {1}")]
    InvalidParameter(String, anyhow::Error),

    /// Attaching the gateway to a remote `serve_via` session failed.
    #[error("failed to attach gateway via session: {0}")]
    ViaAttachFailure(#[source] anyhow::Error),

    /// Constructing a built-in proc failed.
    #[error("failed to construct built-in proc: {0}")]
    ProcConstructionFailure(#[source] anyhow::Error),

    /// The service proc startup address contained a source route.
    #[error("host service proc address must have a direct frontend location, got {0}")]
    InvalidFrontendLocation(Location),
}

/// Client transport handles transferred to the bootstrap shutdown task.
///
/// Dropping this value stops any handles that have not already been joined,
/// so cancellation cannot detach a live frontend or via transport.
#[derive(Debug)]
pub(crate) struct HostShutdownHandles {
    frontend: Option<GatewayServeHandle>,
    via: Option<GatewayServeHandle>,
}

impl HostShutdownHandles {
    pub(crate) fn new(frontend: GatewayServeHandle, via: Option<GatewayServeHandle>) -> Self {
        Self {
            frontend: Some(frontend),
            via,
        }
    }

    pub(crate) async fn stop_and_join(mut self, reason: &str) {
        if let Some(mut frontend) = self.frontend.take() {
            frontend.stop(reason);
            if let Err(error) = frontend.join().await {
                tracing::warn!(%error, "failed to join host shutdown frontend");
            }
        }
        if let Some(mut via) = self.via.take() {
            via.stop(reason);
            if let Err(error) = via.join().await {
                tracing::warn!(%error, "failed to join host shutdown via transport");
            }
        }
    }
}

impl Drop for HostShutdownHandles {
    fn drop(&mut self) {
        if let Some(frontend) = &mut self.frontend {
            frontend.stop("host shutdown handles dropped");
        }
        if let Some(via) = &mut self.via {
            via.stop("host shutdown handles dropped");
        }
    }
}

struct ProcHandleKillGuard<H: ProcHandle> {
    handle: Option<H>,
}

impl<H: ProcHandle> ProcHandleKillGuard<H> {
    fn handle(&self) -> &H {
        self.handle
            .as_ref()
            .expect("active spawn cleanup owns the proc handle")
    }

    fn disarm(mut self) {
        self.handle = None;
    }

    fn maybe_kill(&mut self) -> impl Future<Output = ()> + Send + 'static {
        let handle = self.handle.take();

        async move {
            let Some(handle) = handle else {
                return;
            };
            match handle.kill().await {
                Ok(_) | Err(TerminateError::AlreadyTerminated(_)) => {}
                Err(error) => {
                    tracing::warn!(proc = %handle.proc_addr(), %error, "failed to clean up proc spawn");
                }
            }
        }
    }
}

impl<H: ProcHandle> Drop for ProcHandleKillGuard<H> {
    fn drop(&mut self) {
        if self.handle.is_some() {
            tokio::spawn(self.maybe_kill());
        }
    }
}

/// Lifecycle manager for the procs on one machine.
///
/// The host delegates all connectivity to its [`Gateway`]. It creates
/// built-in service/local procs, asks its [`ProcManager`] to spawn children,
/// and keeps the gateway peer registrations for those children alive.
pub struct Host<M> {
    /// Peer guards for spawned child procs, keyed by name. The stored
    /// [`PeerAttachGuard`] keeps the gateway peer route for the child
    /// alive; dropping it removes the entry (used by
    /// [`Host::terminate_children`] to free slots).
    procs: Arc<StdMutex<HashMap<String, PeerAttachGuard>>>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    /// Connectivity for every proc owned by this host.
    ///
    /// The built-in procs share the gateway in-process. Spawned children have
    /// their own gateways and are registered here with
    /// [`Gateway::attach_peer`].
    gateway: Gateway,
    frontend_handle: Option<GatewayServeHandle>,
    backend_handle: Option<GatewayServeHandle>,
    /// Duplex `serve_via` session to a remote gateway, present when this
    /// host was bootstrapped out-of-cluster. Kept alive for the host's
    /// lifetime so the cluster route and outbound forwarder persist.
    via_handle: Option<GatewayServeHandle>,
    manager: Arc<M>,
    service_proc: Proc,
    local_proc: Proc,
}

impl<M: ProcManager> Host<M> {
    /// Construct a host and start its gateway frontend server on `addr`.
    pub async fn new(manager: M, addr: ChannelAddr) -> Result<Self, HostError> {
        Self::new_with_default(manager, addr, None).await
    }

    /// Like [`new`], but optionally uses an already-bound listener.
    ///
    /// When `listener` is `Some`, it is used as the frontend listening socket
    /// instead of binding a new one.
    pub async fn new_with_default(
        manager: M,
        addr: ChannelAddr,
        listener: Option<std::net::TcpListener>,
    ) -> Result<Self, HostError> {
        // Default to the process-wide global gateway so procs on this
        // host share one routing table with the rest of the process.
        // Callers that need a different gateway (e.g. via attach) build
        // it externally and pass it to [`new_with_gateway`].
        Self::new_with_gateway(
            manager,
            ProcAddr::new(legacy_service_proc_id(), addr.into()),
            listener,
            Gateway::global().clone(),
            None,
        )
        .await
    }

    /// Like [`new_with_default`], but uses a caller-provided [`Gateway`] and
    /// service [`ProcAddr`].
    ///
    /// `addr` is a startup specification: its proc id becomes the service proc
    /// identity, and its direct location is used to serve the frontend. The
    /// final service proc location comes from the gateway after binding, so it
    /// may differ because of an alias, an ephemeral address, or `via`. A
    /// source-routed [`Location::Via`] is therefore rejected here.
    ///
    /// When `via` is `Some`, the gateway attaches to that remote duplex
    /// address with [`Gateway::serve_via`] *after* the local
    /// frontend/backend serves but *before* minting the built-in procs,
    /// so the via session is the newest active serve. That ordering
    /// makes every ref minted on this host advertise the routable
    /// `Via` location rather than the bare local frontend, which is
    /// what lets an out-of-cluster client receive return traffic over
    /// the duplex.
    #[hyperactor::instrument(fields(addr=addr.to_string()))]
    pub async fn new_with_gateway(
        manager: M,
        addr: ProcAddr,
        listener: Option<std::net::TcpListener>,
        gateway: Gateway,
        via: Option<ChannelAddr>,
    ) -> Result<Self, HostError> {
        let service_proc_id = addr.id().clone();
        let addr = match addr.location() {
            Location::Addr(addr) => addr.clone(),
            Location::Via(..) => {
                return Err(HostError::InvalidFrontendLocation(addr.location().clone()));
            }
        };
        let mut backend_handle = Gateway::serve(&gateway, ChannelAddr::any(manager.transport()))?;
        let backend_addr = gateway.default_location().addr().clone();

        // Serve the frontend. Kernel-socket (net) transports use a muxed
        // listener so simplex clients and duplex attach clients share one
        // address; the gateway owns both the simplex and (`AttachWire`)
        // duplex accept paths, so there is a single attach protocol whether
        // a frontend is muxed or a plain duplex endpoint. `serve_mux`
        // requires a net transport, so we branch on `is_net()` rather than
        // `supports_duplex()` (the in-process `Local` transport supports
        // duplex but is not a kernel socket). Both paths register the bound
        // address as the gateway's active serve location.
        let frontend_result: Result<GatewayServeHandle, ChannelError> = if addr.transport().is_net()
        {
            gateway.serve_mux_with_listener(addr, listener)
        } else {
            gateway.serve_with_listener(addr, listener)
        };
        let mut frontend_handle = match frontend_result {
            Ok(handle) => handle,
            Err(error) => {
                backend_handle.stop("host setup failed");
                if let Err(join_error) = backend_handle.join().await {
                    tracing::warn!(
                        error = %join_error,
                        "failed to join backend server after host setup error"
                    );
                }
                return Err(error.into());
            }
        };
        let frontend_addr = gateway.default_location().addr().clone();
        assert!(
            !matches!(frontend_addr, ChannelAddr::Alias { .. }),
            "gateway frontend serve must canonicalize aliases"
        );

        // Attach to the remote gateway after the local serves are live
        // but before any proc or actor ref is minted below. `serve_via`
        // must be the newest active serve so it supplies the gateway's
        // `default_location`; otherwise the local frontend serve above
        // would win, and refs would advertise a bare, cluster-
        // unreachable address — the out-of-cluster return-path bug.
        let via_handle = match via {
            Some(via_addr) => match gateway.serve_via(via_addr).await {
                Ok(handle) => Some(handle),
                Err(error) => {
                    frontend_handle.stop("host setup failed");
                    if let Err(join_error) = frontend_handle.join().await {
                        tracing::warn!(
                            error = %join_error,
                            "failed to join frontend server after via attach error"
                        );
                    }
                    backend_handle.stop("host setup failed");
                    if let Err(join_error) = backend_handle.join().await {
                        tracing::warn!(
                            error = %join_error,
                            "failed to join backend server after via attach error"
                        );
                    }
                    return Err(HostError::ViaAttachFailure(error));
                }
            },
            None => None,
        };

        // Set up the system proc and the local client proc after the
        // gateway servers are live. The HostAgent is published only
        // after it binds its handler, so the brief unroutable window is
        // before normal clients can discover this host.
        let expected_service_proc_addr =
            ProcAddr::new(service_proc_id.clone(), gateway.default_location());
        let service_proc = Proc::builder()
            .proc_id(service_proc_id)
            .shared_gateway(gateway.clone())
            .build()
            .map_err(HostError::ProcConstructionFailure)?;
        assert_eq!(
            service_proc.proc_addr(),
            expected_service_proc_addr,
            "service proc must use the gateway's active advertised location"
        );
        let local_proc = Proc::builder()
            .proc_id(ProcId::instance(Label::strip(LOCAL_PROC_NAME)))
            .shared_gateway(gateway.clone())
            .build()
            .map_err(HostError::ProcConstructionFailure)?;
        let service_proc_id = service_proc.proc_addr().clone();
        let local_proc_id = local_proc.proc_addr().clone();

        tracing::info!(
            frontend_addr = frontend_addr.to_string(),
            backend_addr = backend_addr.to_string(),
            service_proc_id = service_proc_id.to_string(),
            local_proc_id = local_proc_id.to_string(),
            "serving host"
        );

        Ok(Host {
            procs: Arc::new(StdMutex::new(HashMap::new())),
            frontend_addr,
            backend_addr,
            gateway,
            frontend_handle: Some(frontend_handle),
            backend_handle: Some(backend_handle),
            via_handle,
            manager: Arc::new(manager),
            service_proc,
            local_proc,
        })
    }

    /// The underlying proc manager.
    pub fn manager(&self) -> &M {
        self.manager.as_ref()
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

    /// The local proc associated with this host (`LOCAL_PROC_NAME`).
    ///
    /// Starts with zero actors; see invariant LP-1 on
    /// [`LOCAL_PROC_NAME`] for activation semantics.
    pub fn local_proc(&self) -> &Proc {
        &self.local_proc
    }

    /// Spawn a child proc with the given `name`.
    ///
    /// On success, the proc is ready and reachable through the returned
    /// [`ProcAddr`]. The proc id is derived from `name`; its location is
    /// advertised through this host's frontend gateway using a `Via(child_uid,
    /// host_location)` source route.
    /// The caller must ensure that no other spawn with the same `name` is in
    /// flight. This method only rejects names that are already registered with the host.
    pub(crate) fn spawn(
        &self,
        name: String,
        config: M::Config,
    ) -> impl Future<Output = Result<(ProcAddr, ActorRef<ManagerAgent<M>>), HostError>> + Send + 'static
    where
        M: Send + Sync + 'static,
        M::Config: Send + 'static,
    {
        let procs = Arc::clone(&self.procs);
        let backend_addr = self.backend_addr.clone();
        let gateway = self.gateway.clone();
        let manager = Arc::clone(&self.manager);

        async move {
            if procs
                .lock()
                .map_err(|_| HostError::ProcRegistryPoisoned)?
                .contains_key(&name)
            {
                return Err(HostError::ProcExists(name));
            }

            // Advertise the child with a `Via(child_uid, host_location)`
            // location so peers source-route through this host: the outer
            // hop matches the peer entry installed below, and gets peeled
            // to deliver to the child's gateway. The host location comes
            // from the gateway's active routing state, so a later
            // `serve`/`serve_via` controls newly spawned child refs.
            let resource_id = ResourceId::from_name(&name);
            let proc_uid = resource_id.uid().clone();
            let proc_id =
                resource_id.proc_addr(gateway.default_location().with_via(proc_uid.clone()));
            let handle = manager.spawn(proc_id.clone(), backend_addr, config).await?;
            let mut proc_handle_guard = ProcHandleKillGuard {
                handle: Some(handle),
            };

            let created = async {
                let timeout: Duration =
                    hyperactor_config::global::get(hyperactor::config::HOST_SPAWN_READY_TIMEOUT);

                let ready = if timeout == Duration::from_secs(0) {
                    ReadyProc::ensure(proc_handle_guard.handle()).await
                } else {
                    match tokio::time::timeout(
                        timeout,
                        ReadyProc::ensure(proc_handle_guard.handle()),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_elapsed) => Err(ReadyProcError::Timeout),
                    }
                }
                .map_err(|e| {
                    HostError::ProcessConfigurationFailure(
                        proc_id.clone(),
                        anyhow::anyhow!("{e:?}"),
                    )
                })?;

                let child_sender = MailboxClient::dial(ready.addr().clone()).map_err(|e| {
                    HostError::ProcessConfigurationFailure(
                        proc_id.clone(),
                        anyhow::anyhow!("failed to dial spawned proc at {}: {}", ready.addr(), e),
                    )
                })?;
                let agent_ref = ready.agent_ref().clone();
                drop(ready);

                let guard = gateway
                    .attach_peer(proc_uid.clone(), child_sender.into_boxed())
                    .map_err(|error| HostError::PeerAttachFailure(proc_id.clone(), error))?;
                match procs
                    .lock()
                    .map_err(|_| HostError::ProcRegistryPoisoned)?
                    .entry(name.clone())
                {
                    Entry::Vacant(entry) => {
                        entry.insert(guard);
                    }
                    Entry::Occupied(_) => return Err(HostError::ProcExists(name.clone())),
                }

                Ok((proc_id.clone(), agent_ref))
            }
            .await;

            match created {
                Ok(created) => {
                    proc_handle_guard.disarm();
                    Ok(created)
                }
                Err(error) => {
                    proc_handle_guard.maybe_kill().await;
                    Err(error)
                }
            }
        }
    }

    /// The host's [`Gateway`]. All incoming traffic addressed to this
    /// host's procs is routed through the gateway: in-process procs
    /// via the gateway's local delivery path, and spawned child
    /// proc gateways through peer routes registered with
    /// [`Gateway::attach_peer`].
    pub fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    /// Remove a child proc's gateway registration and release its name.
    pub(crate) fn remove_proc(&self, name: &str) {
        let guard = self
            .procs
            .lock()
            .expect("procs mutex poisoned")
            .remove(name);
        drop(guard);
    }

    /// Take ownership of the frontend server handle.
    ///
    /// This is only used by bootstrap shutdown: the host is dropped after
    /// taking the handle, and the bootstrap join path stops and drains the
    /// frontend server explicitly.
    pub(crate) fn take_frontend_handle(&mut self) -> Option<GatewayServeHandle> {
        self.frontend_handle.take()
    }

    /// Take the transport handles that must remain live through final client flushes.
    pub(crate) fn take_shutdown_handles(&mut self) -> HostShutdownHandles {
        let frontend = self
            .take_frontend_handle()
            .expect("host shutdown must own its frontend handle");
        HostShutdownHandles::new(frontend, self.via_handle.take())
    }

    /// Stop and join every gateway server owned by this host.
    pub(crate) async fn shutdown_servers(&mut self) {
        if let Some(mut handle) = self.frontend_handle.take() {
            handle.stop("host shutdown");
            if let Err(error) = handle.join().await {
                tracing::warn!(%error, "failed to join host frontend server");
            }
        }
        if let Some(mut handle) = self.backend_handle.take() {
            handle.stop("host shutdown");
            if let Err(error) = handle.join().await {
                tracing::warn!(%error, "failed to join host backend server");
            }
        }
        if let Some(mut handle) = self.via_handle.take() {
            handle.stop("host shutdown");
            if let Err(error) = handle.join().await {
                tracing::warn!(%error, "failed to join host via server");
            }
        }
    }
}

impl<M> Drop for Host<M> {
    fn drop(&mut self) {
        if let Some(mut handle) = self.frontend_handle.take() {
            handle.stop("host dropped");
        }
        if let Some(mut handle) = self.backend_handle.take() {
            handle.stop("host dropped");
        }
        if let Some(mut handle) = self.via_handle.take() {
            handle.stop("host dropped");
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
    /// Initiates a polite shutdown for the child, waits up to `timeout` for
    /// completion, then escalates to a forceful stop.
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
    /// - `reason`: Human-readable reason for termination.
    /// Returns a tuple of (polite shutdown actors vec, forceful stop actors vec)
    async fn terminate_proc(
        &self,
        cx: &impl context::Actor,
        proc: &ProcAddr,
        timeout: std::time::Duration,
        reason: &str,
    ) -> Result<(Vec<ActorAddr>, Vec<ActorAddr>), anyhow::Error>;
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
        reason: &str,
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
        &mut self,
        cx: &impl context::Actor,
        timeout: Duration,
        max_in_flight: usize,
        reason: &str,
    ) -> TerminateSummary {
        let summary = self
            .manager
            .terminate_all(cx, timeout, max_in_flight, reason)
            .await;
        {
            let _guards = {
                let mut entries = self.procs.lock().expect("procs mutex poisoned");
                std::mem::take(&mut *entries)
            };
        }
        summary
    }
}

#[async_trait::async_trait]
impl<M: ProcManager + SingleTerminate> SingleTerminate for Host<M> {
    async fn terminate_proc(
        &self,
        cx: &impl context::Actor,
        proc: &ProcAddr,
        timeout: Duration,
        reason: &str,
    ) -> Result<(Vec<ActorAddr>, Vec<ActorAddr>), anyhow::Error> {
        self.manager.terminate_proc(cx, proc, timeout, reason).await
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

    /// The proc's logical address.
    pub fn proc_addr(&self) -> &ProcAddr {
        self.handle.proc_addr()
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

    /// The proc's logical address on this host.
    fn proc_addr(&self) -> &ProcAddr;

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
    ///
    /// # Parameters
    /// - `cx`: The actor context for sending messages.
    /// - `timeout`: Grace period before escalation.
    /// - `reason`: Human-readable reason for termination.
    async fn terminate(
        &self,
        cx: &impl context::Actor,
        timeout: Duration,
        reason: &str,
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

    /// Spawn a new proc with the provided proc address. The proc should use
    /// `forwarder_addr` for messages destined outside of itself. The returned
    /// handle exposes the address that accepts messages for the proc.
    ///
    /// An agent actor is also spawned, and the corresponding actor
    /// ref is returned.
    async fn spawn(
        &self,
        proc_id: ProcAddr,
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

/// Lifecycle status for procs managed by [`LocalProcManager`].
///
/// Used by [`LocalProcManager::request_stop`] to track background
/// teardown progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalProcStatus {
    /// A stop has been requested but teardown is still in progress.
    Stopping,
    /// Teardown completed.
    Stopped,
}

/// A ProcManager that spawns **in-process** procs (test-only).
///
/// The proc runs inside this same OS process; there is **no** child
/// process to signal. Lifecycle is purely proc-level:
/// - `terminate(timeout)`: delegates to
///   `Proc::destroy_and_wait(timeout)`, which drains and, at the
///   deadline, aborts remaining actors.
/// - `kill()`: uses a zero deadline to emulate a forced stop via
///   `destroy_and_wait(Duration::ZERO)`.
/// - `wait()`: trivial (no external lifecycle to observe).
///
///   No OS signals are sent or required.
pub struct LocalProcManager<S> {
    procs: Arc<Mutex<HashMap<ProcAddr, Proc>>>,
    stopping: Arc<Mutex<HashMap<ProcAddr, tokio::sync::watch::Sender<LocalProcStatus>>>>,
    spawn: S,
}

impl<S> LocalProcManager<S> {
    /// Create a new in-process proc manager with the given agent
    /// params.
    pub fn new(spawn: S) -> Self {
        Self {
            procs: Arc::new(Mutex::new(HashMap::new())),
            stopping: Arc::new(Mutex::new(HashMap::new())),
            spawn,
        }
    }

    async fn discard_proc(&self, proc_id: &ProcAddr, reason: &str) {
        let proc = self.procs.lock().await.remove(proc_id);
        if let Some(mut proc) = proc
            && let Err(error) = proc.destroy_and_wait(Duration::ZERO, reason).await
        {
            tracing::warn!(%proc_id, %error, "failed to discard local proc");
        }
    }

    /// Non-blocking stop: remove the proc and spawn a background task
    /// that tears it down.
    ///
    /// Status transitions through `Stopping` -> `Stopped` and is
    /// observable via [`local_proc_status`] and [`watch`]. Idempotent:
    /// no-ops if the proc is already stopping or stopped.
    pub async fn request_stop(&self, proc: &ProcAddr, timeout: Duration, reason: &str) {
        {
            let guard = self.stopping.lock().await;
            if guard.contains_key(proc) {
                return;
            }
        }

        let mut proc_handle = {
            let mut guard = self.procs.lock().await;
            match guard.remove(proc) {
                Some(p) => p,
                None => return,
            }
        };

        let proc_ref: ProcAddr = proc_handle.proc_addr().clone();
        let (tx, _) = tokio::sync::watch::channel(LocalProcStatus::Stopping);
        self.stopping.lock().await.insert(proc_ref.clone(), tx);

        let stopping = Arc::clone(&self.stopping);
        let reason = reason.to_string();
        tokio::spawn(async move {
            if let Err(e) = proc_handle.destroy_and_wait(timeout, &reason).await {
                tracing::warn!(error = %e, "request_stop(local): destroy_and_wait failed");
            }
            if let Some(tx) = stopping.lock().await.get(&proc_ref) {
                let _ = tx.send(LocalProcStatus::Stopped);
            }
        });
    }

    /// Query the lifecycle status of a proc that was stopped via
    /// [`request_stop`].
    ///
    /// Returns `None` if the proc was never stopped through this path.
    pub async fn local_proc_status(&self, proc: &ProcAddr) -> Option<LocalProcStatus> {
        self.stopping.lock().await.get(proc).map(|tx| *tx.borrow())
    }

    /// Subscribe to lifecycle status changes for a proc that was
    /// stopped via [`request_stop`].
    ///
    /// Returns `None` if the proc was never stopped through this path.
    pub async fn watch(
        &self,
        proc: &ProcAddr,
    ) -> Option<tokio::sync::watch::Receiver<LocalProcStatus>> {
        self.stopping
            .lock()
            .await
            .get(proc)
            .map(|tx| tx.subscribe())
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
        reason: &str,
    ) -> TerminateSummary {
        // Drain procs so we don't hold the lock across awaits and subsequent
        // calls to terminate_all don't try to re-terminate.
        let procs: Vec<Proc> = {
            let mut guard = self.procs.lock().await;
            guard.drain().map(|(_, v)| v).collect()
        };

        let attempted = procs.len();

        let results = stream::iter(procs.into_iter().map(|mut p| async move {
            // For local manager, graceful proc-level stop.
            match p.destroy_and_wait(timeout, reason).await {
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
        proc: &ProcAddr,
        timeout: std::time::Duration,
        reason: &str,
    ) -> Result<(Vec<ActorAddr>, Vec<ActorAddr>), anyhow::Error> {
        // Snapshot procs so we don't hold the lock across awaits.
        let procs: Option<Proc> = {
            let mut guard = self.procs.lock().await;
            guard.remove(proc)
        };
        if let Some(mut p) = procs {
            p.destroy_and_wait(timeout, reason).await
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
/// - the [`ActorAddr`] to the agent actor hosted in the proc.
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
    proc_id: ProcAddr,
    addr: ChannelAddr,
    agent_ref: ActorRef<A>,
    procs: Arc<Mutex<HashMap<ProcAddr, Proc>>>,
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

    fn proc_addr(&self) -> &ProcAddr {
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
        reason: &str,
    ) -> Result<(), TerminateError<Self::TerminalStatus>> {
        let mut proc = {
            let guard = self.procs.lock().await;
            match guard.get(self.proc_addr()) {
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
            .destroy_and_wait(timeout, reason)
            .await
            .map_err(TerminateError::Io)?;

        Ok(())
    }

    async fn kill(&self) -> Result<(), TerminateError<Self::TerminalStatus>> {
        // Forced stop == zero deadline; `destroy_and_wait` will
        // immediately abort remaining actors and return.
        let mut proc = {
            let guard = self.procs.lock().await;
            match guard.get(self.proc_addr()) {
                Some(p) => p.clone(),
                None => return Err(TerminateError::AlreadyTerminated(())),
            }
        };

        let _ = proc
            .destroy_and_wait(Duration::from_millis(0), "kill")
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
///   - `Binds<A>`: lets the runtime wire the agent's handler ports.
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

    #[hyperactor::instrument(fields(proc_id=proc_id.to_string(), addr=forwarder_addr.to_string()))]
    async fn spawn(
        &self,
        proc_id: ProcAddr,
        forwarder_addr: ChannelAddr,
        _config: (),
    ) -> Result<Self::Handle, HostError> {
        let transport = forwarder_addr.transport();
        let proc = Proc::configured(
            proc_id.clone(),
            MailboxClient::dial(forwarder_addr)?.into_boxed(),
        );
        let (proc_addr, rx) = channel::serve(ChannelAddr::any(transport))?;
        self.procs
            .lock()
            .await
            .insert(proc_id.clone(), proc.clone());
        let _handle = proc.clone().serve(rx);
        let agent_handle = match AssertUnwindSafe((self.spawn)(proc)).catch_unwind().await {
            Ok(Ok(agent_handle)) => agent_handle,
            Ok(Err(error)) => {
                self.discard_proc(&proc_id, "proc agent failed to spawn")
                    .await;
                return Err(HostError::AgentSpawnFailure(proc_id, error));
            }
            Err(panic) => {
                self.discard_proc(&proc_id, "proc agent spawn panicked")
                    .await;
                std::panic::resume_unwind(panic);
            }
        };

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
/// (or host) drops. There is **no** graceful proc control plane or
/// exit monitor wired here. `terminate()` and `kill()` therefore
/// remove and force-kill the child directly, while `wait()` remains
/// trivial and does not report real lifecycle state.
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
    children: Arc<Mutex<HashMap<ProcAddr, Child>>>,
    _phantom: PhantomData<A>,
}

struct ProcessChildCleanup {
    children: Arc<Mutex<HashMap<ProcAddr, Child>>>,
    proc_id: Option<ProcAddr>,
}

impl ProcessChildCleanup {
    fn disarm(mut self) {
        self.proc_id = None;
    }
}

impl Drop for ProcessChildCleanup {
    fn drop(&mut self) {
        let Some(proc_id) = self.proc_id.take() else {
            return;
        };
        let children = Arc::clone(&self.children);

        tokio::spawn(async move {
            if let Some(mut child) = children.lock().await.remove(&proc_id) {
                if let Err(error) = child.kill().await {
                    tracing::warn!(%proc_id, %error, "failed to kill interrupted process spawn");
                }

                if let Err(error) = child.wait().await {
                    tracing::warn!(%proc_id, %error, "failed to reap interrupted process spawn");
                }
            }
        });
    }
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
/// has no graceful shutdown control plane or exit monitor. It retains
/// access to the manager's child registry so `terminate()` and `kill()`
/// can remove, kill, and reap the process when a host spawn fails.
///
/// The type bound `A: Actor + Referable` comes from the
/// [`ProcHandle::Agent`] requirement: `Actor` because the agent
/// actually runs inside the proc, and `Referable` because it must
/// be referenceable via [`ActorRef<A>`] (i.e., safe to carry as a
/// typed remote reference).
#[derive(Debug)]
pub struct ProcessHandle<A: Actor + Referable> {
    proc_id: ProcAddr,
    addr: ChannelAddr,
    agent_ref: ActorRef<A>,
    children: Arc<Mutex<HashMap<ProcAddr, Child>>>,
}

// Manual `Clone` to avoid requiring `A: Clone`.
impl<A: Actor + Referable> Clone for ProcessHandle<A> {
    fn clone(&self) -> Self {
        Self {
            proc_id: self.proc_id.clone(),
            addr: self.addr.clone(),
            agent_ref: self.agent_ref.clone(),
            children: Arc::clone(&self.children),
        }
    }
}

#[async_trait]
impl<A: Actor + Referable> ProcHandle for ProcessHandle<A> {
    /// Agent must be both an `Actor` (runs in the proc) and a
    /// `Referable` (so it can be referenced via `ActorRef<A>`).
    type Agent = A;
    type TerminalStatus = ();

    fn proc_addr(&self) -> &ProcAddr {
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
        _reason: &str,
    ) -> Result<(), TerminateError<Self::TerminalStatus>> {
        self.kill().await
    }

    async fn kill(&self) -> Result<(), TerminateError<Self::TerminalStatus>> {
        let Some(mut child) = self.children.lock().await.remove(&self.proc_id) else {
            return Err(TerminateError::AlreadyTerminated(()));
        };
        child
            .kill()
            .await
            .map_err(|error| TerminateError::Io(error.into()))?;
        child
            .wait()
            .await
            .map_err(|error| TerminateError::Io(error.into()))?;
        Ok(())
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

    #[hyperactor::instrument(fields(proc_id=proc_id.to_string(), addr=forwarder_addr.to_string()))]
    async fn spawn(
        &self,
        proc_id: ProcAddr,
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

        let child = cmd.spawn().map_err(|e| {
            HostError::ProcessSpawnFailure(proc_id.clone(), self.program.display().to_string(), e)
        })?;

        // Retain the handle so it lives for the life of the
        // manager/host.
        {
            let mut children = self.children.lock().await;
            children.insert(proc_id.clone(), child);
        }
        let cleanup = ProcessChildCleanup {
            children: Arc::clone(&self.children),
            proc_id: Some(proc_id.clone()),
        };

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
        let handle = ProcessHandle {
            proc_id,
            addr: proc_addr,
            agent_ref,
            children: Arc::clone(&self.children),
        };
        cleanup.disarm();
        Ok(handle)
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
        let proc_id: ProcAddr = Self::parse_env("HYPERACTOR_HOST_PROC_ID")?;
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
#[hyperactor::instrument(fields(proc_id=proc_id.to_string(), addr=backend_addr.to_string(), callback_addr=callback_addr.to_string()))]
pub async fn spawn_proc<A, S, F>(
    proc_id: ProcAddr,
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
    let proc = Proc::configured(
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
    let agent_ref: ActorRef<A> = agent_handle.bind::<A>();
    channel::dial::<(ChannelAddr, ActorRef<A>)>(callback_addr)?
        .send((proc_addr, agent_ref))
        .await
        .map_err(ChannelError::from)?;

    Ok(proc)
}

/// Testing support for hosts. This is linked outside of cfg(test)
/// as it is needed by an external binary.
pub mod testing {
    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::ActorAddr;
    use hyperactor::Context;
    use hyperactor::Endpoint as _;
    use hyperactor::Handler;
    use hyperactor::OncePortRef;
    /// Just a simple actor, available in both the bootstrap binary as well as
    /// hyperactor tests.
    #[derive(Debug, Default)]
    #[hyperactor::export(handlers = [OncePortRef<ActorAddr>])]
    pub struct EchoActor;

    impl Actor for EchoActor {}

    #[async_trait]
    impl Handler<OncePortRef<ActorAddr>> for EchoActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            reply: OncePortRef<ActorAddr>,
        ) -> Result<(), anyhow::Error> {
            reply.post(cx, cx.self_addr().clone());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::Duration;

    use hyperactor::Addr;
    use hyperactor::Endpoint as _;
    use hyperactor::Label;
    use hyperactor::Location;
    use hyperactor::OncePortRef;
    use hyperactor::Uid;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::Tx;
    use hyperactor::channel::TxStatus;
    use hyperactor::context::Mailbox;
    use hyperactor::mailbox::DialMailboxRouter;
    use hyperactor::mailbox::MessageEnvelope;
    use hyperactor::port::Port;
    #[cfg(fbcode_build)]
    use hyperactor_config::attrs::Attrs;
    use timed_test::async_timed_test;

    use super::testing::EchoActor;
    use super::*;
    #[cfg(fbcode_build)]
    use crate::bootstrap::BootstrapCommand;
    #[cfg(fbcode_build)]
    use crate::bootstrap::BootstrapProcConfig;
    #[cfg(fbcode_build)]
    use crate::bootstrap::BootstrapProcManager;
    #[cfg(fbcode_build)]
    use crate::config_dump::ConfigDump;
    #[cfg(fbcode_build)]
    use crate::config_dump::ConfigDumpResult;

    #[tokio::test]
    async fn test_basic() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<()>("host_agent", ()))
        });
        let procs = Arc::clone(&proc_manager.procs);
        let host = Host::new(proc_manager, ChannelAddr::any(ChannelTransport::Unix))
            .await
            .unwrap();

        let (proc_id1, _ref) = host.spawn("proc1".to_string(), ()).await.unwrap();
        // The spawned proc's identity matches the requested name, and
        // its location resolves to the host's frontend address with
        // a `Via(proc_uid, ...)` source-routing prefix.
        assert_eq!(proc_id1.id(), &ResourceId::from_name("proc1").proc_id());
        assert_eq!(proc_id1.location().addr(), host.addr());
        let (via_uid, _) = proc_id1
            .location()
            .as_via()
            .expect("spawned proc_addr must carry a via prefix");
        assert_eq!(via_uid, proc_id1.id().uid());
        assert!(procs.lock().await.contains_key(&proc_id1));

        let (proc_id2, _ref) = host.spawn("proc2".to_string(), ()).await.unwrap();
        assert!(procs.lock().await.contains_key(&proc_id2));

        let proc1 = procs.lock().await.get(&proc_id1).unwrap().clone();
        let proc2 = procs.lock().await.get(&proc_id2).unwrap().clone();

        // Make sure they can talk to each other:
        let instance1 = proc1.client("client");
        let instance2 = proc2.client("client");

        let (port, mut rx) = instance1.mailbox().open_port();

        port.bind().post(&instance2, "hello".to_string());
        assert_eq!(rx.recv().await.unwrap(), "hello".to_string());

        // Make sure that the system proc is also wired in correctly.
        let system_actor = host.system_proc().client("test");

        // system->proc
        port.bind()
            .post(&system_actor, "hello from the system proc".to_string());
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the system proc".to_string()
        );

        // system->system
        let (port, mut rx) = system_actor.mailbox().open_port();
        port.bind()
            .post(&system_actor, "hello from the system".to_string());
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the system".to_string()
        );

        // proc->system
        port.bind()
            .post(&instance1, "hello from the instance1".to_string());
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the instance1".to_string()
        );
    }

    #[cfg(fbcode_build)]
    async fn assert_bootstrap_child_round_trip(service_proc_id: ProcId) {
        let manager = BootstrapProcManager::new(BootstrapCommand::test()).unwrap();
        let mut host = Host::new_with_gateway(
            manager,
            ProcAddr::new(
                service_proc_id,
                ChannelAddr::any(ChannelTransport::Unix).into(),
            ),
            None,
            Gateway::new(),
            None,
        )
        .await
        .unwrap();

        let child_name = ResourceId::instance(Label::strip("route-child")).to_string();
        let (_child_proc, child_agent) = host
            .spawn(
                child_name,
                BootstrapProcConfig {
                    create_rank: 0,
                    client_config_override: Attrs::new(),
                    proc_bind: None,
                    bootstrap_command: None,
                },
            )
            .await
            .unwrap();

        let service_client = host.system_proc().client("route-test");
        let (reply_handle, reply_rx) = service_client
            .mailbox()
            .open_once_port::<ConfigDumpResult>();
        child_agent.post(
            &service_client,
            ConfigDump {
                result: reply_handle.bind(),
            },
        );
        tokio::time::timeout(Duration::from_secs(5), reply_rx.recv())
            .await
            .expect("service-to-child request and child-to-service reply timed out")
            .expect("config dump reply channel closed");

        let summary = host
            .terminate_children(
                &service_client,
                Duration::from_secs(2),
                1,
                "routing test complete",
            )
            .await;
        assert_eq!(summary.failed, 0);
        host.shutdown_servers().await;
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_bootstrap_child_round_trip_with_legacy_service_proc() {
        assert_bootstrap_child_round_trip(legacy_service_proc_id()).await;
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_bootstrap_child_round_trip_with_instance_service_proc() {
        let service_proc_id = ProcId::new(
            Uid::Instance(0x5e12_71ce, Some(Label::strip(SERVICE_PROC_NAME))),
            None,
        );
        assert_bootstrap_child_round_trip(service_proc_id).await;
    }

    #[tokio::test]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: ReadFailed { manifest_path: "/meta-pytorch/monarch/target/debug/deps/hyperactor-0e1fe83af739d976.resources.json", source: Os { code: 2, kind: NotFound, message: "No such file or directory" } }
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_process_proc_manager() {
        hyperactor_telemetry::initialize_logging(hyperactor_telemetry::DefaultTelemetryClock {});

        // EchoActor is "host_agent" used to test connectivity.
        let process_manager = ProcessProcManager::<EchoActor>::new(
            buck_resources::get("monarch/hyperactor_mesh/host_bootstrap").unwrap(),
        );
        let host = Host::new(process_manager, ChannelAddr::any(ChannelTransport::Unix))
            .await
            .unwrap();

        // (1) Spawn and check invariants.
        assert!(matches!(host.addr().transport(), ChannelTransport::Unix));
        let (proc1, echo1) = host.spawn("proc1".to_string(), ()).await.unwrap();
        let (proc2, echo2) = host.spawn("proc2".to_string(), ()).await.unwrap();
        assert_eq!(echo1.actor_addr().proc_addr(), proc1);
        assert_eq!(echo2.actor_addr().proc_addr(), proc2);

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
        let client_inst = client.client("test");
        let (port, rx) = client_inst.mailbox().open_once_port();
        echo1.post(&client_inst, port.bind());
        let id = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id, *echo1.actor_addr());

        // (4) Child <-> external client request -> reply:
        // Request: client proc (standalone via `Proc::direct`) ->
        //          host frontend/router -> echo2 (proc2).
        // Reply:   echo2 (proc2) -> host backend -> host router ->
        //          client port (standalone proc).
        // This exercises cross-proc routing between a child and an
        // external client under the same host.
        let (port2, rx2) = client_inst.mailbox().open_once_port();
        echo2.post(&client_inst, port2.bind());
        let id2 = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id2, *echo2.actor_addr());

        // (5) System -> child request -> cross-proc reply:
        // Request: system proc -> host router (frontend) -> echo1
        //          (proc1, child).
        // Reply: echo1 (proc1) -> proc1 forwarder -> host backend ->
        //        host router -> client proc direct addr (Proc::direct) ->
        //        client port.
        // Because `client_inst` runs in its own proc, the reply
        // traverses the host (not local delivery within proc1).
        let sys_inst = host.system_proc().client("sys-client");
        let (port3, rx3) = client_inst.mailbox().open_once_port();
        // Send from system -> child via a message that ultimately
        // replies to client's port
        echo1.post(&sys_inst, port3.bind());
        let id3 = tokio::time::timeout(Duration::from_secs(5), rx3.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id3, *echo1.actor_addr());
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn cancelled_process_startup_kills_reaps_and_unregisters_child() {
        fn process_exists(pid: u32) -> bool {
            (
                // SAFETY: signal 0 only checks the PID
                unsafe { libc::kill(pid as libc::pid_t, 0) }
            ) == 0
        }

        let program_dir = tempfile::tempdir().expect("test directory should be created");
        let program = {
            let program = program_dir.path().join("pending_process.sh");

            tokio::fs::write(&program, "#!/bin/sh\nexec /bin/sleep 600\n")
                .await
                .expect("test program should be written");

            let mut permissions = tokio::fs::metadata(&program)
                .await
                .expect("test program metadata should be available")
                .permissions();

            permissions.set_mode(0o700);

            tokio::fs::set_permissions(&program, permissions)
                .await
                .expect("test program should be executable");

            program
        };

        let manager = Arc::new(ProcessProcManager::<EchoActor>::new(program));
        let proc_id = ResourceId::proc_addr_from_name(
            ChannelAddr::any(ChannelTransport::Unix),
            "cancelled-process",
        );
        let startup = tokio::spawn({
            let manager = Arc::clone(&manager);
            let proc_id = proc_id.clone();

            async move {
                manager
                    .spawn(proc_id, ChannelAddr::any(ChannelTransport::Unix), ())
                    .await
            }
        });

        let pid = loop {
            {
                let children = manager.children.lock().await;

                if let Some(child) = children.get(&proc_id) {
                    break child.id().expect("test child should have a process ID");
                }
            }
            tokio::task::yield_now().await;
        };
        assert!(process_exists(pid));

        startup.abort();

        assert!(
            startup
                .await
                .expect_err("startup task should be cancelled")
                .is_cancelled(),
        );

        loop {
            if !process_exists(pid) {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::ESRCH),
                    "cancelled child should be killed and reaped",
                );
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !manager.children.lock().await.contains_key(&proc_id),
            "cancelled child should be removed from the child registry",
        );
    }

    #[tokio::test]
    async fn local_ready_and_wait_are_immediate() {
        // Build a LocalHandle directly.
        let addr = ChannelAddr::any(ChannelTransport::Local);
        let proc_ref = ResourceId::proc_addr_from_name(addr.clone(), "p");
        let actor_ref = proc_ref.actor_addr("host_agent");
        let agent_ref = ActorRef::<()>::attest(actor_ref);
        let h = LocalHandle::<()> {
            proc_id: proc_ref,
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
        Pending,
        ErrTerminal,
        ErrChannelClosed,
    }

    #[derive(Debug, Clone)]
    struct TestHandle {
        id: ProcAddr,
        addr: ChannelAddr,
        agent: ActorRef<()>,
        mode: ReadyMode,
        omit_addr: bool,
        omit_agent: bool,
        kill_notify: Option<Arc<tokio::sync::Notify>>,
        kill_release: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait::async_trait]
    impl ProcHandle for TestHandle {
        type Agent = ();
        type TerminalStatus = ();

        fn proc_addr(&self) -> &ProcAddr {
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
                        tokio::time::sleep(d).await;
                    }
                    Ok(())
                }
                ReadyMode::Pending => std::future::pending().await,
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
            _reason: &str,
        ) -> Result<Self::TerminalStatus, TerminateError<Self::TerminalStatus>> {
            Err(TerminateError::Unsupported)
        }
        async fn kill(&self) -> Result<Self::TerminalStatus, TerminateError<Self::TerminalStatus>> {
            if let Some(notify) = &self.kill_notify {
                notify.notify_one();
            }
            if let Some(release) = &self.kill_release {
                release.notified().await;
            }
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct TestManager {
        mode: ReadyMode,
        omit_addr: bool,
        omit_agent: bool,
        transport: ChannelTransport,
        kill_notify: Option<Arc<tokio::sync::Notify>>,
        kill_release: Option<Arc<tokio::sync::Notify>>,
    }

    impl TestManager {
        fn local(mode: ReadyMode) -> Self {
            Self {
                mode,
                omit_addr: false,
                omit_agent: false,
                transport: ChannelTransport::Local,
                kill_notify: None,
                kill_release: None,
            }
        }
        fn with_omissions(mut self, addr: bool, agent: bool) -> Self {
            self.omit_addr = addr;
            self.omit_agent = agent;
            self
        }
        fn with_kill_notify(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
            self.kill_notify = Some(notify);
            self
        }
        fn with_kill_release(mut self, release: Arc<tokio::sync::Notify>) -> Self {
            self.kill_release = Some(release);
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
            proc_id: ProcAddr,
            forwarder_addr: ChannelAddr,
            _config: (),
        ) -> Result<Self::Handle, HostError> {
            let agent = ActorRef::<()>::attest(proc_id.actor_addr("host_agent"));
            Ok(TestHandle {
                id: proc_id,
                addr: forwarder_addr,
                agent,
                mode: self.mode,
                omit_addr: self.omit_addr,
                omit_agent: self.omit_agent,
                kill_notify: self.kill_notify.clone(),
                kill_release: self.kill_release.clone(),
            })
        }
    }

    #[tokio::test]
    async fn host_spawn_cleans_up_handle_after_readiness_failure() {
        let cleanup_started = Arc::new(tokio::sync::Notify::new());
        let cleanup_release = Arc::new(tokio::sync::Notify::new());
        let host = Host::new(
            TestManager::local(ReadyMode::ErrTerminal)
                .with_kill_notify(Arc::clone(&cleanup_started))
                .with_kill_release(Arc::clone(&cleanup_release)),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .expect("host should start");

        let spawn = tokio::spawn(host.spawn("failed-proc".to_string(), ()));
        cleanup_started.notified().await;
        assert!(
            !spawn.is_finished(),
            "spawn returned before failed-proc cleanup completed",
        );
        cleanup_release.notify_one();

        let error = spawn
            .await
            .expect("spawn task should complete")
            .expect_err("readiness failure should fail the spawn");
        assert!(matches!(
            error,
            HostError::ProcessConfigurationFailure(_, _)
        ));
        assert!(
            !host
                .procs
                .lock()
                .expect("procs mutex poisoned")
                .contains_key("failed-proc"),
            "failed spawn should not leave a peer registration",
        );
    }

    #[tokio::test]
    async fn local_manager_cleans_up_proc_when_agent_spawn_panics() {
        async fn panicking_spawn(_proc: Proc) -> anyhow::Result<ActorHandle<()>> {
            panic!("test agent spawn panic");
        }

        let manager = LocalProcManager::new(panicking_spawn);
        let managed_procs = Arc::clone(&manager.procs);
        let host = Host::new(manager, ChannelAddr::any(ChannelTransport::Local))
            .await
            .expect("host should start");

        let result = AssertUnwindSafe(host.spawn("panicking-proc".to_string(), ()))
            .catch_unwind()
            .await;
        assert!(result.is_err(), "agent spawn panic should be preserved");
        assert!(
            managed_procs.lock().await.is_empty(),
            "panicking agent spawn should not leave a managed proc",
        );
        assert!(
            !host
                .procs
                .lock()
                .expect("procs mutex poisoned")
                .contains_key("panicking-proc"),
            "panicking agent spawn should release its name reservation",
        );
    }

    #[tokio::test]
    async fn host_spawn_times_out_when_configured() {
        let cfg = hyperactor_config::global::lock();
        let _g = cfg.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_millis(10),
        );

        let host = Host::new(
            TestManager::local(ReadyMode::Pending),
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
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(0),
        );

        let host = Host::new(
            TestManager::local(ReadyMode::OkAfter(Duration::from_millis(20))),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let (pid, agent) = host.spawn("ok".into(), ()).await.expect("must succeed");
        assert_eq!(agent.actor_addr().proc_addr(), pid);
        assert!(
            host.procs
                .lock()
                .expect("procs mutex poisoned")
                .contains_key("ok")
        );
    }

    #[tokio::test]
    async fn host_spawn_maps_channel_closed_ready_error_to_config_failure() {
        let host = Host::new(
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
        let host = Host::new(
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
        let host = Host::new(
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
        let host = Host::new(
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

    // test_duplex_remote_proc, test_duplex_undeliverable_from_client,
    // test_duplex_undeliverable_from_host, and test_duplex_teardown
    // were removed: proc-level attach is gone. Gateway-attach is
    // exercised by the via tests in `hyperactor::gateway::tests`;
    // undeliverable bouncing is unchanged at the host level and is
    // exercised by the gateway-attach tests.

    /// Repro for the OSS broken-link issue: when the host's duplex
    /// frontend shuts down with messages still on the wire, the
    /// simplex peer must see a clean close (and pending acks must
    /// flush) rather than retry-looping for `MESSAGE_DELIVERY_TIMEOUT`.
    ///
    /// Before the fix: the peer's `NetTx` got no acks for in-flight
    /// messages and no `Closed` response, so it spent the full 30 s
    /// `MESSAGE_DELIVERY_TIMEOUT` reconnecting against a dead host.
    ///
    /// This test posts a message, then stops the serve handle and
    /// asserts the simplex `NetTx` transitions to `Closed` quickly —
    /// well under `MESSAGE_DELIVERY_TIMEOUT`.
    #[tokio::test]
    async fn test_simplex_peer_sees_clean_close_on_host_shutdown() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<EchoActor>("host_agent", EchoActor))
        });
        let mut host =
            Host::new_with_default(proc_manager, ChannelAddr::any(ChannelTransport::Unix), None)
                .await
                .unwrap();
        let mut serve_handle = host.take_frontend_handle().unwrap();

        // Spawn an EchoActor and send a request from a simplex client.
        let echo_handle = host.system_proc().spawn(EchoActor);
        let echo_ref = echo_handle.bind::<EchoActor>();

        let dial_router = DialMailboxRouter::new();
        dial_router.bind(
            Addr::from(host.system_proc().proc_addr().clone()),
            host.addr().clone(),
        );
        let client_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (client_listen_addr, client_rx) = channel::serve(client_addr).unwrap();
        let client_proc_id = ResourceId::proc_addr_from_name(client_listen_addr, "client");
        let client_proc = Proc::configured(client_proc_id, dial_router.into_boxed());
        let _client_handle = client_proc.clone().serve(client_rx);

        let client_inst = client_proc.client("requester");
        let (reply_port, reply_handle) = client_inst.mailbox().open_once_port::<ActorAddr>();
        let reply_port = reply_port.bind();
        echo_ref
            .port::<OncePortRef<ActorAddr>>()
            .post(&client_inst, reply_port);
        let _ = tokio::time::timeout(Duration::from_secs(5), reply_handle.recv())
            .await
            .expect("baseline round-trip timed out")
            .expect("baseline recv failed");

        // Snapshot the client's outbound NetTx status before shutdown.
        let host_tx = channel::dial::<MessageEnvelope>(host.addr().clone()).unwrap();
        // Push one message so the lazy-connect kicks in.
        let dummy_dest = host
            .system_proc()
            .proc_addr()
            .actor_addr("noop")
            .port_addr(Port::from(0u64));
        let envelope = MessageEnvelope::serialize(
            client_inst.self_addr().clone(),
            dummy_dest,
            &"warmup".to_string(),
            Default::default(),
        )
        .unwrap();
        host_tx.post(envelope);
        // Wait briefly for connection to establish.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(matches!(*host_tx.status().borrow(), TxStatus::Active));

        // Shut down the host's frontend. The fix ensures pending
        // recv-side acks are flushed AND a `Closed` response is sent,
        // so the simplex peer transitions to `Closed` promptly.
        serve_handle.stop("test shutdown");
        let _ = tokio::time::timeout(Duration::from_secs(5), serve_handle.join())
            .await
            .expect("serve handle did not resolve");

        // The simplex peer should see Closed within a few seconds —
        // not the full MESSAGE_DELIVERY_TIMEOUT (30 s). Wait for the
        // status watch to flip.
        let mut status = host_tx.status().clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let TxStatus::Closed(_) = *status.borrow() {
                    return;
                }
                if status.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .expect("simplex peer did not see Closed within 10s of host shutdown");

        match &*host_tx.status().borrow() {
            TxStatus::Closed(_) => {}
            other => panic!("expected TxStatus::Closed, got {:?}", other),
        }
    }

    /// Stress repro: many simplex clients send rapid request+reply
    /// traffic to the host's duplex frontend and the host shuts down
    /// while traffic is in flight. This mirrors the OSS test pattern
    /// where `HostMeshShutdownGuard::drop` sends `ShutdownHost`.
    #[tokio::test]
    async fn test_simplex_clients_during_host_shutdown() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<EchoActor>("host_agent", EchoActor))
        });
        let mut host =
            Host::new_with_default(proc_manager, ChannelAddr::any(ChannelTransport::Unix), None)
                .await
                .unwrap();
        let mut serve_handle = host.take_frontend_handle().unwrap();

        let echo_handle = host.system_proc().spawn(EchoActor);
        let echo_ref = echo_handle.bind::<EchoActor>();
        let host_addr = host.addr().clone();
        let echo_actor_id = echo_ref.actor_addr().clone();
        let system_proc_id = host.system_proc().proc_addr().clone();

        // Spawn N clients, each sending M requests.
        const N_CLIENTS: usize = 4;
        const M_REQUESTS: usize = 5;

        let mut client_tasks = Vec::new();
        for ci in 0..N_CLIENTS {
            let host_addr = host_addr.clone();
            let echo_actor_id = echo_actor_id.clone();
            let system_proc_id = system_proc_id.clone();
            client_tasks.push(tokio::spawn(async move {
                let dial_router = DialMailboxRouter::new();
                dial_router.bind(Addr::from(system_proc_id.clone()), host_addr);
                let client_addr = ChannelAddr::any(ChannelTransport::Unix);
                let (client_listen_addr, client_rx) = channel::serve(client_addr).unwrap();
                let client_proc_id =
                    ResourceId::proc_addr_from_name(client_listen_addr, format!("client-{}", ci));
                let client_proc = Proc::configured(client_proc_id, dial_router.into_boxed());
                let _client_handle = client_proc.clone().serve(client_rx);

                let echo_ref = ActorRef::<EchoActor>::attest(echo_actor_id);

                for ri in 0..M_REQUESTS {
                    let client_inst = client_proc.client(&format!("req-{}", ri));
                    let (reply_port, reply_handle) =
                        client_inst.mailbox().open_once_port::<ActorAddr>();
                    let reply_port = reply_port.bind();
                    echo_ref
                        .port::<OncePortRef<ActorAddr>>()
                        .post(&client_inst, reply_port);
                    let received =
                        tokio::time::timeout(Duration::from_secs(10), reply_handle.recv())
                            .await
                            .expect("timeout waiting for reply")
                            .expect("recv failed");
                    assert_eq!(received, *echo_ref.actor_addr());
                }
            }));
        }

        for task in client_tasks {
            task.await.unwrap();
        }

        // Shut down. The handle must resolve cleanly.
        serve_handle.stop("test cleanup");
        tokio::time::timeout(Duration::from_secs(10), serve_handle.join())
            .await
            .expect("serve handle did not resolve")
            .expect("serve task error");
    }

    /// Repro for the broken-link errors seen in OSS Python tests:
    /// an external simplex `Proc::direct` dialing the host's duplex
    /// frontend should be able to round-trip a request + reply.
    #[tokio::test]
    async fn test_simplex_client_to_duplex_host() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<EchoActor>("host_agent", EchoActor))
        });
        let host =
            Host::new_with_default(proc_manager, ChannelAddr::any(ChannelTransport::Unix), None)
                .await
                .unwrap();

        // Spawn an EchoActor on the host's system_proc.
        let echo_handle = host.system_proc().spawn(EchoActor);
        let echo_ref = echo_handle.bind::<EchoActor>();

        // Create an external simplex client proc with a dial router
        // bound to the host's frontend address. This mirrors what the
        // Python "root client" does: a `Proc::direct` whose forwarder
        // is a `DialMailboxRouter` with the host's frontend address as
        // a route to the host's procs.
        let client_addr = ChannelAddr::any(ChannelTransport::Unix);
        let dial_router = DialMailboxRouter::new();
        dial_router.bind(
            Addr::from(host.system_proc().proc_addr().clone()),
            host.addr().clone(),
        );
        let (client_listen_addr, client_rx) = channel::serve(client_addr).unwrap();
        let client_proc_id = ResourceId::proc_addr_from_name(client_listen_addr, "external-client");
        let client_proc = Proc::configured(client_proc_id, dial_router.into_boxed());
        let _client_handle = client_proc.clone().serve(client_rx);

        let client_inst = client_proc.client("requester");

        // Send a request to the echo actor on the host. The reply
        // travels back through the host's dial router → simplex dial
        // → client's frontend.
        let (reply_port, reply_handle) = client_inst.mailbox().open_once_port::<ActorAddr>();
        let reply_port = reply_port.bind();
        echo_ref
            .port::<OncePortRef<ActorAddr>>()
            .post(&client_inst, reply_port);

        let received = tokio::time::timeout(Duration::from_secs(10), reply_handle.recv())
            .await
            .expect("timed out waiting for reply")
            .expect("recv failed");
        assert_eq!(received, *echo_ref.actor_addr());
    }

    #[tokio::test]
    async fn test_explicit_service_and_local_proc_ids_are_instances() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<()>("host_agent", ()))
        });
        let service_proc_id = ProcId::instance(hyperactor::id::Label::strip("service"));

        let host = Host::new_with_gateway(
            proc_manager,
            ProcAddr::new(
                service_proc_id.clone(),
                ChannelAddr::any(ChannelTransport::Unix).into(),
            ),
            None,
            Gateway::new(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(host.system_proc().proc_id(), &service_proc_id);
        assert!(matches!(
            host.local_proc().proc_id().uid(),
            hyperactor::id::Uid::Instance(..)
        ));
        assert_eq!(
            host.local_proc()
                .proc_id()
                .label()
                .map(|label| label.as_str()),
            Some(LOCAL_PROC_NAME)
        );
    }

    #[tokio::test]
    async fn test_new_with_gateway_rejects_via_frontend_location() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<()>("host_agent", ()))
        });
        let location = Location::from(ChannelAddr::any(ChannelTransport::Unix))
            .with_via(Uid::instance(Label::strip("remote")));
        let service_proc_addr = ProcAddr::new(legacy_service_proc_id(), location.clone());

        let result =
            Host::new_with_gateway(proc_manager, service_proc_addr, None, Gateway::new(), None)
                .await;

        assert!(matches!(
            result,
            Err(HostError::InvalidFrontendLocation(error_location))
                if error_location == location
        ));
    }

    #[tokio::test]
    async fn test_new_with_gateway_remints_service_proc_at_bound_address() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<()>("host_agent", ()))
        });
        let service_proc_id = ProcId::instance(Label::strip("service"));
        let startup_addr = ChannelAddr::from_zmq_url("tcp://127.0.0.1:0").unwrap();
        let mut host = Host::new_with_gateway(
            proc_manager,
            ProcAddr::new(service_proc_id.clone(), startup_addr.into()),
            None,
            Gateway::new(),
            None,
        )
        .await
        .unwrap();

        let ChannelAddr::Tcp(bound_addr) = host.addr() else {
            panic!("expected TCP frontend address")
        };
        assert_ne!(bound_addr.port(), 0);
        assert_eq!(
            host.system_proc().proc_addr(),
            ProcAddr::new(service_proc_id, Location::from(host.addr().clone()))
        );

        host.shutdown_servers().await;
    }

    #[tokio::test]
    async fn test_spawn_uses_latest_serve_location_after_prior_default_override() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<()>("host_agent", ()))
        });
        let gateway = Gateway::new();
        let attached_host_addr: ChannelAddr = "tcp:10.0.159.108:26600".parse().unwrap();
        let attached_uid = Uid::instance(Label::strip("attached"));
        let attached_location = Location::from(attached_host_addr).with_via(attached_uid);
        gateway.set_default_location(attached_location.clone());

        let host = Host::new_with_gateway(
            proc_manager,
            ProcAddr::new(
                legacy_service_proc_id(),
                ChannelAddr::any(ChannelTransport::Unix).into(),
            ),
            None,
            gateway,
            None,
        )
        .await
        .unwrap();

        let (proc_id, _agent) = host.spawn("proc1".to_string(), ()).await.unwrap();
        let (proc_uid, host_location) = proc_id
            .location()
            .as_via()
            .expect("spawned proc must carry child via");

        assert_eq!(proc_uid, proc_id.id().uid());
        assert_eq!(host_location.as_ref(), &Location::from(host.addr().clone()));
        assert_ne!(host_location.as_ref(), &attached_location);
    }

    #[tokio::test]
    async fn test_spawn_uses_latest_direct_gateway_serve_location() {
        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<()>("host_agent", ()))
        });

        let host = Host::new_with_gateway(
            proc_manager,
            ProcAddr::new(
                legacy_service_proc_id(),
                ChannelAddr::any(ChannelTransport::Unix).into(),
            ),
            None,
            Gateway::new(),
            None,
        )
        .await
        .unwrap();
        let mut later_frontend = host
            .gateway()
            .serve_with_listener(ChannelAddr::any(ChannelTransport::Unix), None)
            .unwrap();
        let later_location = host.gateway().default_location();
        assert_ne!(later_location.addr(), host.addr());

        let (proc_id, _agent) = host.spawn("proc1".to_string(), ()).await.unwrap();
        let (proc_uid, host_location) = proc_id
            .location()
            .as_via()
            .expect("spawned proc must carry child via");

        assert_eq!(proc_uid, proc_id.id().uid());
        assert_eq!(host_location.as_ref(), &later_location);

        later_frontend.stop("test cleanup");
        later_frontend.join().await.unwrap();
    }

    // Regression: an out-of-cluster client (`via` set) must advertise
    // its refs at the `Via` location so in-cluster hosts route return
    // traffic back over the duplex. Before the fix, the host's own
    // frontend serve clobbered the via as `default_location`, so refs
    // carried a bare, cluster-unreachable address and the attach-time
    // config-push acks timed out (`MESH_ATTACH_CONFIG_TIMEOUT`).
    #[tokio::test]
    async fn test_new_with_gateway_via_advertises_via_location() {
        // A remote gateway accepting duplex attaches stands in for the
        // in-cluster host the client attaches to.
        let remote_gw = Gateway::new();
        let mut remote_accept = remote_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let remote_addr = remote_gw.default_location().addr().clone();

        let proc_manager = LocalProcManager::new(|proc: Proc| async move {
            Ok(proc.spawn_with_label::<()>("host_agent", ()))
        });

        let gateway = Gateway::new();
        let fallback_location = gateway.default_location();
        let mut host = Host::new_with_gateway(
            proc_manager,
            ProcAddr::new(
                legacy_service_proc_id(),
                ChannelAddr::any(ChannelTransport::Unix).into(),
            ),
            None,
            gateway.clone(),
            Some(remote_addr.clone()),
        )
        .await
        .unwrap();

        // The gateway must advertise the Via (not the bare local
        // frontend) as its default location for newly bound refs.
        let default_location = host.gateway().default_location();
        let (via_uid, inner) = default_location
            .as_via()
            .expect("default location must carry the via prefix");
        assert_eq!(via_uid, host.gateway().uid());
        assert_eq!(inner.addr(), &remote_addr);

        // The built-in service proc, minted after `serve_via`, must
        // also carry the Via — it advertised a bare address before the
        // fix.
        let svc_proc_addr = host.system_proc().proc_addr();
        let (_, svc_inner) = svc_proc_addr
            .location()
            .as_via()
            .expect("service proc must carry the via prefix");
        assert_eq!(svc_inner.addr(), &remote_addr);

        let shutdown_handles = host.take_shutdown_handles();
        drop(host);
        assert_eq!(
            gateway.default_location(),
            default_location,
            "shutdown handles must retain the via location after the host drops"
        );
        shutdown_handles.stop_and_join("test cleanup").await;
        assert_eq!(gateway.default_location(), fallback_location);

        remote_accept.stop("test cleanup");
        remote_accept.join().await.unwrap();
    }
}
