/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Host-mesh attach and lifecycle.
//!
//! ## Host-mesh invariants (HM-*)
//!
//! These are the load-bearing semantic contracts of `HostMesh::attach()`
//! and `HostMeshRef::push_config()`. They describe what callers may
//! rely on; they do not pin specific mechanisms (the current
//! implementation chooses a particular send path, error taxonomy, and
//! timeout shape — those are not invariants and may evolve).
//!
//! - **HM-1 (attach-config-complete).** If `HostMesh::attach()` returns
//!   `Ok`, every attached host has installed the client's propagatable
//!   config snapshot.
//!
//! - **HM-2 (attach-config-fails-closed).** If config push fails on
//!   any attached host, `HostMesh::attach()` returns `Err`. It must
//!   not return a partially-configured mesh as success.
//!
//! - **HM-3 (in-band-request-failure-surface).** Attach-time
//!   config-push *request* failure must surface through `attach()` /
//!   `push_config()` as a structured error. It must not bypass that
//!   result path by returning the outbound request on the caller's
//!   `Undeliverable<MessageEnvelope>` channel. The invariant names a
//!   specific prohibited bypass; what a caller chooses to do with the
//!   returned `Err` (escalate, retry, abort) is outside scope.
//!   Cross-cutting: depends on hyperactor undeliverable semantics in
//!   `hyperactor::reference` and `hyperactor::actor`; the invariant
//!   still belongs here because the attach contract is owned here.
//!
//! - **HM-4 (host-scoped-error-reporting).** Config-push failure
//!   reported from `push_config()` identifies the failing host(s)
//!   individually, so callers can act per-host. The contract commits
//!   to per-host *identity*; the failure-mode taxonomy carried
//!   alongside it (the `ConfigPushFailure` variant set) is
//!   implementation detail and may evolve without changing the
//!   contract.

#![allow(clippy::result_large_err)]

use hyperactor::ActorRef;
use hyperactor::Endpoint as _;
use hyperactor::Gateway;
use hyperactor::Handler;
use hyperactor::accum::StreamingReducerOpts;
use hyperactor::channel::ChannelTransport;
use hyperactor::id::Label;
use hyperactor::id::Uid;
use hyperactor_cast::cast_actor::CastActor;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use ndslice::view::CollectMeshExt;

use crate::mesh_admin::MeshAdminAgent;
use crate::supervision::MeshFailure;

pub mod host_agent;

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Deref;
use std::ops::DerefMut;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use hyperactor::ActorAddr;
use hyperactor::ProcAddr;
use hyperactor::channel::ChannelAddr;
use hyperactor::context;
use hyperactor_cast::cast_actor::CAST_ACTOR_NAME;
use ndslice::Extent;
use ndslice::Region;
use ndslice::ViewExt;
use ndslice::extent;
use ndslice::view;
use ndslice::view::Ranked;
use ndslice::view::RegionParseError;
use serde::Deserialize;
use serde::Serialize;
use tracing::Instrument;
use typeuri::Named;

use crate::ActorMeshRef;
use crate::Bootstrap;
use crate::ProcMesh;
use crate::ValueMesh;
use crate::bootstrap::BootstrapCommand;
use crate::bootstrap::BootstrapProcManager;
use crate::bootstrap::ProcBind;
use crate::host::Host;
use crate::host::LocalProcManager;
use crate::host::SERVICE_PROC_NAME;
pub use crate::host_mesh::host_agent::HostAgent;
use crate::host_mesh::host_agent::ProcManagerSpawnFn;
use crate::host_mesh::host_agent::ProcState;
use crate::host_mesh::host_agent::ShutdownHostClient;
use crate::mesh_controller::ProcMeshController;
use crate::mesh_id::ActorMeshId;
use crate::mesh_id::HostMeshId;
use crate::mesh_id::ProcMeshId;
use crate::mesh_id::ResourceId;
use crate::proc_agent::ProcAgent;
use crate::proc_mesh::ProcMeshRef;
use crate::resource;
use crate::resource::CreateOrUpdateClient;
use crate::resource::GetRankStatus;
use crate::resource::GetRankStatusClient;
use crate::resource::RankedValues;
use crate::resource::Status;
use crate::resource::WaitRankStatusClient;
use crate::transport::DEFAULT_TRANSPORT;

/// Actor name for `ProcMeshController` when spawned as a named child.
pub const PROC_MESH_CONTROLLER_NAME: &str = "proc_mesh_controller";

declare_attrs! {
    /// The maximum idle time between updates while spawning proc
    /// meshes.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE".to_string()),
        Some("mesh_proc_spawn_max_idle".to_string()),
    ))
    pub attr PROC_SPAWN_MAX_IDLE: Duration = Duration::from_secs(30);

    /// The maximum idle time between updates while stopping proc
    /// meshes.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_PROC_STOP_MAX_IDLE".to_string()),
        Some("proc_stop_max_idle".to_string()),
    ))
    pub attr PROC_STOP_MAX_IDLE: Duration = Duration::from_secs(30);

    /// The maximum idle time between updates while querying host meshes
    /// for their proc states.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_GET_PROC_STATE_MAX_IDLE".to_string()),
        Some("get_proc_state_max_idle".to_string()),
    ))
    pub attr GET_PROC_STATE_MAX_IDLE: Duration = Duration::from_mins(1);
}

/// A reference to a single host.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize)]
pub struct HostRef(ChannelAddr);
wirevalue::register_type!(HostRef);

impl HostRef {
    /// Create a host reference from a channel address.
    ///
    /// `ChannelAddr::Alias` is a socket setup convenience, not a host
    /// identity. The host consumes the alias when serving and advertises the
    /// `dial_to` address, so remote references must use that same address in
    /// proc and actor identities.
    pub(crate) fn new(addr: ChannelAddr) -> Self {
        Self(addr.into_dial_addr())
    }

    /// The host mesh agent associated with this host.
    pub(crate) fn mesh_agent(&self) -> ActorRef<HostAgent> {
        ActorRef::attest(
            self.service_proc()
                .actor_addr(host_agent::HOST_MESH_AGENT_ACTOR_NAME),
        )
    }

    /// The ProcAddr for the proc with name `name` on this host.
    ///
    /// Mirrors the convention of `Host::spawn`: a spawned child proc is
    /// advertised at `Via(proc_uid, host_addr)` so the host's gateway
    /// peels the outer hop via its peer table and forwards to the
    /// child's gateway. Without the via prefix the host would bounce
    /// the envelope as a self-loop.
    fn named_proc(&self, id: &ResourceId) -> ProcAddr {
        let location = hyperactor::Location::from(self.0.clone()).with_via(id.uid().clone());
        ProcAddr::new(id.proc_id(), location)
    }

    /// The service proc on this host.
    fn service_proc(&self) -> ProcAddr {
        ResourceId::proc_addr_from_name(self.0.clone(), SERVICE_PROC_NAME)
    }
}

impl<'de> Deserialize<'de> for HostRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::new(ChannelAddr::deserialize(deserializer)?))
    }
}

impl TryFrom<ActorRef<HostAgent>> for HostRef {
    type Error = crate::Error;

    fn try_from(value: ActorRef<HostAgent>) -> Result<Self, crate::Error> {
        let proc_id = value.actor_addr().proc_addr();
        Ok(Self::new(proc_id.addr().clone()))
    }
}

impl std::fmt::Display for HostRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for HostRef {
    type Err = <ChannelAddr as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(ChannelAddr::from_str(s)?))
    }
}

/// Per-host failure modes for the attach-time config push.
///
/// **Implementation detail of [`ConfigPushError`].** The variant
/// taxonomy is *not* part of HM-4 or any other invariant — HM-4
/// commits to per-host *identity* in the error, not to a specific
/// menu of failure subtypes. The variant set may grow, shrink, or be
/// reshaped without changing the contract; tests should not pin to a
/// specific variant unless the fixture deterministically guarantees
/// it.
#[derive(Debug, thiserror::Error)]
pub enum ConfigPushFailure {
    /// Synchronous send failure (e.g. the request port refused the
    /// post). The error is preserved for triage.
    #[error("send failed: {0}")]
    SendFailed(#[source] Box<hyperactor::mailbox::MailboxSenderError>),

    /// The collective cast failed before the caller observed the
    /// acknowledgement barrier.
    #[error("cast failed: {0}")]
    CastFailed(String),

    /// The awaited reply did not arrive within
    /// `MESH_ATTACH_CONFIG_TIMEOUT`. With request-bounce suppression
    /// (HM-3 mechanism), this is the dominant failure mode for an
    /// unreachable host: the channel-side `BrokenLink` is logged at
    /// debug from `MailboxClient`'s buffer task; the contract surface
    /// is just "did not reply in time".
    #[error("reply timed out after MESH_ATTACH_CONFIG_TIMEOUT")]
    ReplyTimedOut,

    /// The reply receiver closed before any value arrived (sender
    /// side dropped, or the local mailbox tore down).
    #[error("reply channel closed before reply")]
    ReplyChannelClosed,
}

/// Aggregated `attach()` config-push failure surface — one entry per
/// host that didn't acknowledge installation.
///
/// HM-4: per-host identity is preserved so callers can act per-host.
/// The host-identity key is `HostRef` (the iteration unit in
/// `push_config()`); the per-host cause is a `ConfigPushFailure`
/// (taxonomy is implementation-detail; see the type's doc).
#[derive(Debug)]
pub struct ConfigPushError {
    /// One entry per host whose config push didn't succeed.
    pub failures: Vec<(HostRef, ConfigPushFailure)>,
}

impl std::fmt::Display for ConfigPushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "config push failed during attach on {} host(s):",
            self.failures.len()
        )?;
        for (host, failure) in &self.failures {
            write!(f, "\n  - {}: {}", host, failure)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigPushError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Per-host causes are surfaced through `Display`. A single
        // top-level `source()` would arbitrarily pick one host's
        // cause and obscure the others.
        None
    }
}

/// An owned mesh of hosts.
///
/// # Lifecycle
/// `HostMesh` owns host lifecycles. Callers **must** invoke
/// [`HostMesh::shutdown`] for deterministic teardown.
///
/// In tests and production, prefer explicit shutdown to guarantee
/// that host agents drop their `BootstrapProcManager`s and that all
/// child procs are reaped. You can use `shutdown_guard` to get a wrapper
/// which will try to do a best-effort shutdown on Drop.
pub struct HostMesh {
    id: HostMeshId,
    extent: Extent,
    /// Whether this `HostMesh` should best-effort shut down hosts on Drop.
    cleanup_on_drop: bool,
    current_ref: HostMeshRef,
}

impl HostMesh {
    /// Emit a telemetry event for this host mesh creation.
    fn notify_created(&self) {
        let name_str = self.id.to_string();
        let mesh_id_hash = hyperactor_telemetry::hash_to_u64(&self.id);

        hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
            id: mesh_id_hash,
            timestamp: std::time::SystemTime::now(),
            class: "Host".to_string(),
            given_name: self
                .id
                .display_label()
                .map(|l| l.as_str())
                .unwrap_or("unnamed")
                .to_string(),
            full_name: name_str,
            shape_json: serde_json::to_string(&self.extent).unwrap_or_default(),
            parent_mesh_id: None,
            parent_view_json: None,
        });

        // Notify telemetry of each HostAgent actor in this mesh.
        // These are skipped in Proc::spawn_inner. mesh_id directly points to host mesh.
        let now = std::time::SystemTime::now();
        for (rank, host) in self.current_ref.hosts().iter().enumerate() {
            let actor = host.mesh_agent();
            hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                id: hyperactor_telemetry::hash_to_u64(actor.actor_addr().id()),
                timestamp: now,
                mesh_id: mesh_id_hash,
                rank: rank as u64,
                full_name: actor.actor_addr().to_string(),
                display_name: None,
            });
        }
    }

    /// Bring up a local single-host mesh and, in the launcher
    /// process, return a `HostMesh` handle for it.
    ///
    /// There are two execution modes:
    ///
    /// - bootstrap-child mode: if `Bootstrap::get_from_env()` says
    ///   this process was launched as a bootstrap child, we call
    ///   `boot.bootstrap().await`, which hands control to the
    ///   bootstrap logic for this process (as defined by the
    ///   `BootstrapCommand` the parent used to spawn it). if that
    ///   call returns, we log the error and terminate. this branch
    ///   does not produce a `HostMesh`.
    ///
    /// - launcher mode: otherwise, we are the process that is setting
    ///   up the mesh. we create a `Host`, spawn a `HostAgent` in
    ///   it, and build a single-host `HostMesh` around that. that
    ///   `HostMesh` is returned to the caller.
    ///
    /// This API is intended for tests, examples, and local bring-up,
    /// not production.
    ///
    /// TODO: fix up ownership
    pub async fn local() -> crate::Result<HostMesh> {
        Self::local_with_bootstrap(BootstrapCommand::current()?).await
    }

    /// Same as [`local`], but the caller supplies the
    /// `BootstrapCommand` instead of deriving it from the current
    /// process.
    ///
    /// The provided `bootstrap_cmd` is used when spawning bootstrap
    /// children and determines the behavior of
    /// `boot.bootstrap().await` in those children.
    pub async fn local_with_bootstrap(bootstrap_cmd: BootstrapCommand) -> crate::Result<HostMesh> {
        if let Ok(Some(boot)) = Bootstrap::get_from_env() {
            let result = boot.bootstrap().await;
            if let Err(err) = result {
                tracing::error!("failed to bootstrap local host mesh process: {}", err);
            }
            std::process::exit(1);
        }

        let addr = hyperactor_config::global::get_cloned(DEFAULT_TRANSPORT).binding_addr();

        let manager = BootstrapProcManager::new(bootstrap_cmd)?;
        // Use a dedicated gateway, not the process-wide global one. This
        // host coexists with the global-context singleton host (see
        // `global_context`), which owns the global gateway; sharing it
        // would collide on the legacy `service`/`local` pseudo-singleton
        // proc ids.
        let host = Host::new_with_gateway(manager, addr, None, Gateway::new()).await?;
        let addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_mesh_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(host_agent::HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .map_err(crate::Error::SingletonActorSpawnError)?;
        HostAgent::wait_initialized(&host_mesh_agent).await?;
        host_mesh_agent.bind::<HostAgent>();
        let cast_handle = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::strip(CAST_ACTOR_NAME)),
                CastActor::default(),
            )
            .map_err(crate::Error::SingletonActorSpawnError)?;
        cast_handle.bind::<CastActor>();

        let host = HostRef::new(addr);
        let host_mesh_ref = HostMeshRef::new(
            HostMeshId::instance(Label::new("local").unwrap()),
            extent!(hosts = 1).into(),
            vec![host],
        )?;
        Ok(HostMesh::take(host_mesh_ref))
    }

    /// Create a local in-process host mesh where all procs run in the
    /// current OS process.
    ///
    /// Unlike [`local`] which spawns child processes for each proc,
    /// this method uses [`LocalProcManager`] to run everything
    /// in-process. This makes all actors visible in the admin tree
    /// (useful for debugging with the TUI).
    ///
    /// This API is intended for tests, examples, and debugging.
    pub async fn local_in_process() -> crate::Result<HostMesh> {
        let addr = hyperactor_config::global::get_cloned(DEFAULT_TRANSPORT).binding_addr();
        Ok(HostMesh::take(Self::local_n_in_process(vec![addr]).await?))
    }

    /// Create a local in-process host mesh with multiple hosts, where
    /// all procs run in the current OS process using [`LocalProcManager`].
    ///
    /// Each address in `addrs` becomes a separate host. The resulting
    /// mesh has `extent!(hosts = addrs.len())`.
    ///
    /// This API is intended for unit tests that need a multi-host mesh
    /// within a single process.
    pub(crate) async fn local_n_in_process(addrs: Vec<ChannelAddr>) -> crate::Result<HostMeshRef> {
        let n = addrs.len();
        let mut host_refs = Vec::with_capacity(n);
        for addr in addrs {
            host_refs.push(Self::create_in_process_host(addr).await?);
        }
        HostMeshRef::new(
            HostMeshId::instance(Label::new("local").unwrap()),
            extent!(hosts = n).into(),
            host_refs,
        )
    }

    /// Create a single in-process host at the given address, returning
    /// a [`HostRef`] for it.
    async fn create_in_process_host(addr: ChannelAddr) -> crate::Result<HostRef> {
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let manager = LocalProcManager::new(spawn);
        // Each in-process host gets its own gateway, not the
        // process-wide global one. Several hosts coexist in one process
        // here, and the legacy `service`/`local` pseudo-singleton proc
        // ids would collide if they all attached to the same gateway.
        let host = Host::new_with_gateway(manager, addr, None, Gateway::new()).await?;
        let addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_mesh_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(host_agent::HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_local(host),
            )
            .map_err(crate::Error::SingletonActorSpawnError)?;
        HostAgent::wait_initialized(&host_mesh_agent).await?;
        host_mesh_agent.bind::<HostAgent>();

        let cast_handle = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::strip(CAST_ACTOR_NAME)),
                CastActor::default(),
            )
            .map_err(crate::Error::SingletonActorSpawnError)?;

        cast_handle.bind::<CastActor>();

        Ok(HostRef::new(addr))
    }

    /// Create a new process-based host mesh. Each host is represented by a local process,
    /// which manages its set of procs. This is not a true host mesh the sense that each host
    /// is not independent. The intent of `process` is for testing, examples, and experimentation.
    ///
    /// The bootstrap command is used to bootstrap both hosts and processes, thus it should be
    /// a command that reaches [`crate::bootstrap_or_die`]. `process` is itself a valid bootstrap
    /// entry point; thus using `BootstrapCommand::current` works correctly as long as `process`
    /// is called early in the lifecycle of the process and reached unconditionally.
    ///
    /// TODO: thread through ownership
    pub async fn process(extent: Extent, command: BootstrapCommand) -> crate::Result<HostMesh> {
        if let Ok(Some(boot)) = Bootstrap::get_from_env() {
            let result = boot.bootstrap().await;
            if let Err(err) = result {
                tracing::error!("failed to bootstrap process host mesh process: {}", err);
            }
            std::process::exit(1);
        }

        let bind_spec = hyperactor_config::global::get_cloned(DEFAULT_TRANSPORT);
        let mut hosts = Vec::with_capacity(extent.num_ranks());
        for _ in 0..extent.num_ranks() {
            // Note: this can be racy. Possibly we should have a callback channel.
            let addr = bind_spec.binding_addr();
            let bootstrap = Bootstrap::Host {
                addr: addr.clone(),
                command: Some(command.clone()),
                config: Some(hyperactor_config::global::attrs()),
                exit_on_shutdown: false,
            };

            let mut cmd = command.new();
            bootstrap.to_env(&mut cmd);
            cmd.spawn()?;
            hosts.push(HostRef::new(addr));
        }

        let host_mesh_ref = HostMeshRef::new(
            HostMeshId::instance(Label::new("process").unwrap()),
            extent.into(),
            hosts,
        )?;
        Ok(HostMesh::take(host_mesh_ref))
    }
    /// Take ownership of an existing host mesh reference.
    ///
    /// Consumes the `HostMeshRef`, captures its region/hosts, and
    /// returns an owned `HostMesh` that assumes lifecycle
    /// responsibility for those hosts (i.e., will shut them down on
    /// Drop).
    pub fn take(mesh: HostMeshRef) -> Self {
        let id = mesh.id.clone();
        let extent = mesh.region.extent().clone();
        let result = Self {
            id,
            extent,
            cleanup_on_drop: true,
            current_ref: mesh,
        };
        result.notify_created();
        result
    }

    /// Attach to pre-existing workers and push client config.
    ///
    /// This is the "simple bootstrap" attach protocol:
    /// 1. Wraps the provided addresses into a `HostMeshRef`.
    /// 2. Snapshots `propagatable_attrs()` from the client's global config.
    /// 3. Pushes the config to each host agent as `Source::ClientOverride`,
    ///    awaiting per-host installation acknowledgement.
    /// 4. Returns the owned `HostMesh`.
    ///
    /// HM-1 / HM-2 / HM-3 / HM-4 (see module docs): if config push
    /// fails on any host, this returns `Err`. A successful return
    /// means every attached host installed the propagatable config
    /// snapshot.
    pub async fn attach(
        cx: &impl context::Actor,
        id: HostMeshId,
        addresses: Vec<ChannelAddr>,
    ) -> crate::Result<Self> {
        let mesh_ref = HostMeshRef::from_hosts(id, addresses);
        let config = hyperactor_config::global::propagatable_attrs();
        mesh_ref.push_config(cx, config).await?;
        Ok(Self::take(mesh_ref))
    }

    /// Request a clean shutdown of all hosts owned by this
    /// `HostMesh`.
    ///
    /// Uses a two-phase approach:
    /// 1. Cast `DrainHost` and wait for every host to finish draining
    ///    its user procs while networking stays alive.
    /// 2. Cast `ShutdownHost` and wait for every host to acknowledge
    ///    that its local shutdown handler has completed.
    #[hyperactor::instrument(fields(host_mesh=self.id.to_string()))]
    pub async fn shutdown(&mut self, cx: &impl hyperactor::context::Actor) -> anyhow::Result<()> {
        let t0 = std::time::Instant::now();
        tracing::info!(name = "HostMeshStatus", status = "Shutdown::Attempt");

        // Phase 1: terminate all user procs while service infrastructure stays
        // alive so forwarder flushes can complete across hosts.
        if let Err(e) = self.current_ref.cast_drain(cx, None).await {
            tracing::warn!(
                name = "HostMeshStatus",
                status = "Shutdown::Drain::Failed",
                drain_ms = t0.elapsed().as_millis(),
                error = %e,
                "failed to cast DrainHost barrier"
            );
        }
        let drain_ms = t0.elapsed().as_millis();

        // Phase 2: request host shutdown once the drain barrier has cleared.
        let t1 = std::time::Instant::now();
        let shutdown_result = self.current_ref.cast_shutdown(cx).await;
        let shutdown_ack_ms = t1.elapsed().as_millis();
        let total_ms = t0.elapsed().as_millis();
        if let Err(e) = shutdown_result {
            tracing::warn!(
                name = "HostMeshStatus",
                status = "Shutdown::Ack::Failed",
                drain_ms,
                shutdown_ack_ms,
                total_ms,
                error = %e,
                "failed waiting for ShutdownHost acknowledgment barrier"
            );
        } else {
            tracing::info!(
                name = "HostMeshStatus",
                status = "Shutdown::Success",
                drain_ms,
                shutdown_ack_ms,
                total_ms
            );
        }

        Ok(())
    }

    /// Consumes and wraps this HostMesh with a HostMeshShutdownGuard, which will
    /// ensure shutdown is run on Drop.
    pub fn shutdown_guard(self) -> HostMeshShutdownGuard {
        HostMeshShutdownGuard(self)
    }

    /// Stop all hosts owned by this `HostMesh`, draining user procs
    /// but keeping worker processes and their sockets alive for
    /// reconnection.
    ///
    /// After `stop`, the same worker addresses can be passed to
    /// [`HostMesh::attach`] to create a new mesh.
    #[hyperactor::instrument(fields(host_mesh=self.id.to_string()))]
    pub async fn stop(&mut self, cx: &impl hyperactor::context::Actor) -> anyhow::Result<()> {
        let t0 = std::time::Instant::now();
        tracing::info!(name = "HostMeshStatus", status = "Stop::Attempt");

        let result = self.current_ref.cast_drain(cx, Some(self.id.clone())).await;
        let total_ms = t0.elapsed().as_millis();
        match result {
            Ok(()) => {
                tracing::info!(name = "HostMeshStatus", status = "Stop::Success", total_ms,);
            }
            Err(e) => tracing::warn!(
                name = "HostMeshStatus",
                status = "Stop::Drain::Failed",
                total_ms,
                error = %e,
                "failed waiting for DrainHost acknowledgment barrier"
            ),
        }

        // Defuse the Drop impl so it doesn't send ShutdownHost to hosts
        // we intentionally kept alive.
        self.cleanup_on_drop = false;

        Ok(())
    }
}

impl HostMesh {
    /// Set the bootstrap command on the underlying `HostMeshRef`,
    /// so that future `spawn` calls use it. Unlike
    /// `HostMeshRef::with_bootstrap` this mutates in place,
    /// preserving ownership.
    pub fn set_bootstrap(&mut self, cmd: BootstrapCommand) {
        self.current_ref = self.current_ref.clone().with_bootstrap(cmd);
    }
}

impl Deref for HostMesh {
    type Target = HostMeshRef;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
    }
}

impl AsRef<HostMeshRef> for HostMesh {
    fn as_ref(&self) -> &HostMeshRef {
        self
    }
}

impl AsRef<HostMeshRef> for HostMeshRef {
    fn as_ref(&self) -> &HostMeshRef {
        self
    }
}

/// Wrapper around HostMesh that runs shutdown on Drop.
pub struct HostMeshShutdownGuard(pub HostMesh);

impl Deref for HostMeshShutdownGuard {
    type Target = HostMesh;

    fn deref(&self) -> &HostMesh {
        &self.0
    }
}

impl DerefMut for HostMeshShutdownGuard {
    fn deref_mut(&mut self) -> &mut HostMesh {
        &mut self.0
    }
}

impl Drop for HostMeshShutdownGuard {
    /// Best-effort cleanup for owned host meshes on drop.
    ///
    /// When a `HostMesh` is dropped, it attempts to shut down all
    /// hosts it owns:
    /// - If a Tokio runtime is available, we spawn an ephemeral
    ///   `Proc` + `Instance` and best-effort cast `ShutdownHost`
    ///   through the owned hosts. This ensures that the embedded
    ///   `BootstrapProcManager`s are dropped, and all child procs they
    ///   spawned are killed when the cast succeeds.
    /// - If no runtime is available, we cannot perform async cleanup
    ///   here; in that case we log a warning and rely on kernel-level
    ///   PDEATHSIG or the individual `BootstrapProcManager`'s `Drop`
    ///   as the final safeguard.
    ///
    /// This path is **last resort**: callers should prefer explicit
    /// [`HostMesh::shutdown`] to guarantee orderly teardown. Drop
    /// only provides opportunistic cleanup to prevent process leaks
    /// if shutdown is skipped.
    fn drop(&mut self) {
        tracing::info!(
            name = "HostMeshStatus",
            host_mesh = %self.0.id,
            status = "Dropping",
        );
        let cleanup_on_drop = self.0.cleanup_on_drop;
        let host_count = self.0.current_ref.hosts().len();

        // Best-effort only when a Tokio runtime is available.
        if !cleanup_on_drop {
            tracing::debug!(
                host_mesh = %self.0.id,
                "HostMesh drop cleanup skipped because host cleanup ownership was released"
            );
        } else if host_count == 0 {
            tracing::debug!(
                host_mesh = %self.0.id,
                "HostMesh drop cleanup skipped because no owned hosts remain"
            );
        } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mesh_id = self.0.id.clone();
            let current_ref = self.0.current_ref.clone();
            let span = tracing::info_span!(
                "hostmesh_drop_cleanup",
                host_mesh = %mesh_id,
                hosts = host_count,
            );

            handle.spawn(
                async move {
                    // Spin up a tiny ephemeral proc+instance to get an
                    // Actor context.
                    match hyperactor::Proc::direct(
                        ChannelTransport::Unix.any(),
                        "hostmesh-drop".to_string(),
                    ) {
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "failed to construct ephemeral Proc for drop-cleanup; \
                                 relying on PDEATHSIG/manager Drop"
                            );
                        }
                        Ok(proc) => {
                            let client = proc.client("drop");
                            if let Err(e) = current_ref.cast_shutdown(&client).await {
                                tracing::warn!(
                                    error = %e,
                                    "drop-cleanup: failed to cast ShutdownHost"
                                );
                            } else {
                                tracing::info!(
                                    hosts = host_count,
                                    "hostmesh drop-cleanup shutdown barrier complete"
                                );
                            }
                        }
                    }
                }
                .instrument(span),
            );
        } else {
            // No runtime here; PDEATHSIG and manager Drop remain the
            // last-resort safety net.
            tracing::warn!(
                host_mesh = %self.0.id,
                hosts = host_count,
                "HostMesh dropped without a Tokio runtime; skipping \
                 best-effort shutdown. This indicates that .shutdown() \
                 on this mesh has not been called before program exit \
                 (perhaps due to a missing call to \
                 'monarch.actor.shutdown_context()'?) This in turn can \
                 lead to backtrace output due to folly SIGTERM \
                 handlers."
            );
        }

        tracing::info!(
            name = "HostMeshStatus",
            host_mesh = %self.0.id,
            status = "Dropped",
        );
    }
}

/// Helper: legacy shim for error types that still require
/// RankedValues<Status>. TODO(shayne-fletcher): Delete this
/// shim once Error::ActorSpawnError carries a StatusMesh
/// (ValueMesh<Status>) directly. At that point, use the mesh
/// as-is and remove `mesh_to_rankedvalues_*` calls below.
/// is_sentinel should return true if the value matches a previous filled in
/// value. If the input value matches the sentinel, it gets replaced with the
/// default.
pub(crate) fn mesh_to_rankedvalues_with_default<T, F>(
    mesh: &ValueMesh<T>,
    default: T,
    is_sentinel: F,
    len: usize,
) -> RankedValues<T>
where
    T: Eq + Clone + 'static,
    F: Fn(&T) -> bool,
{
    let mut out = RankedValues::from((0..len, default));
    for (i, s) in mesh.values().enumerate() {
        if !is_sentinel(&s) {
            out.merge_from(RankedValues::from((i..i + 1, s)));
        }
    }
    out
}

/// A non-owning reference to a mesh of hosts.
///
/// Logically, this is a data structure that contains a set of ranked
/// hosts organized into a [`Region`]. `HostMeshRef`s can be sliced to
/// produce new references that contain a subset of the hosts in the
/// original mesh.
///
/// `HostMeshRef`s have a concrete syntax, implemented by its
/// `Display` and `FromStr` implementations.
///
/// This type does **not** control lifecycle. It only describes the
/// topology of hosts. To take ownership and perform deterministic
/// teardown, use [`HostMesh::take`], which returns an owned
/// [`HostMesh`] that guarantees cleanup on `shutdown()` or `Drop`.
///
/// Cloning this type does not confer ownership. If a corresponding
/// owned [`HostMesh`] shuts down the hosts, operations via a cloned
/// `HostMeshRef` may fail because the hosts are no longer running.
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct HostMeshRef {
    id: HostMeshId,
    region: Region,
    ranks: Arc<Vec<HostRef>>,
    host_agent_mesh: ActorMeshRef<HostAgent>,
    /// Uniform bootstrap command to use when spawning procs on this
    /// mesh. When `None`, each host agent uses its own default
    /// command. Per-proc overrides are supplied at spawn time via the
    /// `per_rank_bootstrap` parameter on [`HostMeshRef::spawn`].
    #[serde(default)]
    pub bootstrap_command: Option<BootstrapCommand>,
}

/// A function that produces a per-rank [`BootstrapCommand`], called
/// once per proc during spawn with that proc's [`view::Point`] over
/// the combined `host_extent ⊕ per_host` extent. Returning an error
/// aborts the spawn with that error surfaced as a configuration
/// failure.
pub type PerRankBootstrapFn = dyn Fn(view::Point) -> anyhow::Result<BootstrapCommand> + Send + Sync;
// Cast-domain materialization state is derived from the host ids and is not
// part of host mesh identity.
impl PartialEq for HostMeshRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.region == other.region
            && self.ranks == other.ranks
            && self.bootstrap_command == other.bootstrap_command
    }
}

impl Eq for HostMeshRef {}

impl std::hash::Hash for HostMeshRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.region.hash(state);
        self.ranks.hash(state);
        self.bootstrap_command.hash(state);
    }
}

wirevalue::register_type!(HostMeshRef);

impl HostMeshRef {
    /// Create a new (raw) HostMeshRef from the provided region and associated
    /// ranks, which must match in cardinality.
    #[allow(clippy::result_large_err)]
    fn new(id: HostMeshId, region: Region, ranks: Vec<HostRef>) -> crate::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(crate::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        let host_agent_mesh = Self::host_agent_mesh_ref(&region, &ranks)?;
        Ok(Self {
            id,
            region,
            ranks: Arc::new(ranks),
            host_agent_mesh,
            bootstrap_command: None,
        })
    }

    /// Create a new HostMeshRef from an arbitrary set of hosts. This is meant to
    /// enable extrinsic bootstrapping.
    pub fn from_hosts(id: HostMeshId, hosts: Vec<ChannelAddr>) -> Self {
        let region = extent!(hosts = hosts.len()).into();
        let ranks: Vec<HostRef> = hosts.into_iter().map(HostRef::new).collect();
        let host_agent_mesh = Self::host_agent_mesh_ref(&region, &ranks)
            .expect("host rank cardinality must match generated region");
        Self {
            id,
            region,
            ranks: Arc::new(ranks),
            host_agent_mesh,
            bootstrap_command: None,
        }
    }

    /// Create a new HostMeshRef from an arbitrary set of host mesh agents.
    pub fn from_host_agents(
        id: HostMeshId,
        agents: Vec<ActorRef<HostAgent>>,
    ) -> crate::Result<Self> {
        let region = extent!(hosts = agents.len()).into();
        let ranks: Vec<HostRef> = agents
            .into_iter()
            .map(HostRef::try_from)
            .collect::<crate::Result<_>>()?;
        let host_agent_mesh = Self::host_agent_mesh_ref(&region, &ranks)?;
        Ok(Self {
            id,
            region,
            ranks: Arc::new(ranks),
            host_agent_mesh,
            bootstrap_command: None,
        })
    }

    /// Create a unit HostMeshRef from a host mesh agent.
    pub fn from_host_agent(id: HostMeshId, agent: ActorRef<HostAgent>) -> crate::Result<Self> {
        let region = Extent::unity().into();
        let ranks = vec![HostRef::try_from(agent)?];
        let host_agent_mesh = Self::host_agent_mesh_ref(&region, &ranks)?;
        Ok(Self {
            id,
            region,
            ranks: Arc::new(ranks),
            host_agent_mesh,
            bootstrap_command: None,
        })
    }

    /// Return a new `HostMeshRef` that will use `cmd` when spawning procs,
    /// overriding the host agent's default bootstrap command.
    pub fn with_bootstrap(self, cmd: BootstrapCommand) -> Self {
        Self {
            bootstrap_command: Some(cmd),
            ..self
        }
    }

    fn host_agent_mesh_ref(
        region: &Region,
        ranks: &[HostRef],
    ) -> crate::Result<ActorMeshRef<HostAgent>> {
        let members = Arc::new(
            ranks
                .iter()
                .map(|host| host.mesh_agent().actor_addr().clone())
                .collect_mesh::<ValueMesh<_>>(region.clone())
                .map_err(|error| crate::Error::ConfigurationError(error.into()))?,
        );

        Ok(ActorMeshRef::new(
            ActorMeshId::singleton(Label::strip(host_agent::HOST_MESH_AGENT_ACTOR_NAME)),
            // The host-agent mesh is not backed by a user proc mesh.
            None,
            region.clone(),
            None,
            members,
        ))
    }

    async fn cast_drain(
        &self,
        cx: &impl context::Actor,
        host_mesh_id: Option<HostMeshId>,
    ) -> anyhow::Result<()> {
        let region = self.region.clone();
        let num_hosts = region.num_ranks();
        if num_hosts == 0 {
            return Ok(());
        }

        // Each host reports a single-rank `Stopped` overlay once it has
        // drained; reduce them into a full StatusMesh so we can tell which
        // hosts (if any) never acknowledged.
        let (reply, rx) = cx.mailbox().open_accum_port_opts(
            crate::StatusMesh::from_single(region.clone(), Status::NotExist),
            StreamingReducerOpts {
                max_update_interval: Some(std::time::Duration::from_millis(50)),
                initial_update_interval: None,
            },
        );
        let mut reply = reply.bind();
        reply.return_undeliverable(false);

        let terminate_timeout =
            hyperactor_config::global::get(crate::bootstrap::MESH_TERMINATE_TIMEOUT);

        self.host_agent_mesh.cast(
            cx,
            host_agent::DrainHost {
                timeout: terminate_timeout,
                max_in_flight: hyperactor_config::global::get(
                    crate::bootstrap::MESH_TERMINATE_CONCURRENCY,
                )
                .clamp(1, 256),
                host_mesh_id,
                rank: Default::default(),
                reply,
            },
        )?;

        // Hosts only report after a (timeout-bounded) `terminate_children`, so
        // the barrier's max-idle must exceed the per-host drain timeout.
        let barrier_timeout = terminate_timeout.saturating_add(std::time::Duration::from_secs(30));

        match GetRankStatus::wait(rx, num_hosts, barrier_timeout, region).await {
            Ok(_) => Ok(()),
            Err(partial) => {
                let missing: Vec<usize> = partial
                    .values()
                    .enumerate()
                    .filter(|(_, status)| status.is_not_exist())
                    .map(|(rank, _)| rank)
                    .collect();
                anyhow::bail!(
                    "DrainHost barrier timed out after {:?}; {} of {} hosts did not acknowledge (host ranks {:?})",
                    barrier_timeout,
                    missing.len(),
                    num_hosts,
                    missing,
                )
            }
        }
    }

    async fn cast_shutdown(&self, cx: &impl context::Actor) -> anyhow::Result<()> {
        let num_hosts = self.ranks.len();
        if num_hosts == 0 {
            return Ok(());
        }

        // Each host replies its own rank directly once shutdown work is done.
        // `ShutdownHost` acks cannot be tree-reduced (hosts exit right after
        // acking), so we collect one direct reply per host on a plain
        // multi-receive port rather than a reduced barrier, and track which
        // ranks acknowledged so we can report the hosts that didn't.
        let (ack, mut rx) = cx.mailbox().open_port::<usize>();
        // Bind `.unsplit()`: every `PortRef` becomes a multipart part the cast
        // split loop would otherwise tree-reduce. `ShutdownHost` acks must reach
        // the caller directly (hosts exit right after acking), so mark this port
        // unsplit to keep it out of the reduction tree.
        let mut ack = ack.bind().unsplit();
        ack.return_undeliverable(false);

        let terminate_timeout =
            hyperactor_config::global::get(crate::bootstrap::MESH_TERMINATE_TIMEOUT);

        self.host_agent_mesh.cast(
            cx,
            host_agent::ShutdownHost {
                timeout: terminate_timeout,
                max_in_flight: hyperactor_config::global::get(
                    crate::bootstrap::MESH_TERMINATE_CONCURRENCY,
                )
                .clamp(1, 256),
                rank: Default::default(),
                ack,
            },
        )?;

        // Hosts only reply after a (timeout-bounded) termination pass, so the
        // per-reply wait must exceed the per-host terminate timeout.
        let barrier_timeout = terminate_timeout.saturating_add(std::time::Duration::from_secs(30));

        let mut acked = std::collections::HashSet::new();

        while acked.len() < num_hosts {
            match tokio::time::timeout(barrier_timeout, rx.recv()).await {
                Ok(Ok(rank)) => {
                    acked.insert(rank);
                }
                Ok(Err(err)) => return Err(anyhow::Error::from(err)),
                Err(_) => {
                    let missing: Vec<usize> =
                        (0..num_hosts).filter(|r| !acked.contains(r)).collect();

                    anyhow::bail!(
                        "ShutdownHost barrier timed out after {:?}; {} of {} hosts did not acknowledge shutdown (host ranks {:?})",
                        barrier_timeout,
                        missing.len(),
                        num_hosts,
                        missing,
                    );
                }
            }
        }
        Ok(())
    }

    /// Returns the host entries as `(addr_string, ActorRef<HostAgent>)` pairs.
    /// Used by `MeshAdminAgent::effective_hosts()` to merge C into the
    /// admin's host list (see CH-1 in mesh_admin module doc).
    pub(crate) fn host_entries(&self) -> Vec<(String, ActorRef<HostAgent>)> {
        self.ranks
            .iter()
            .map(|h| (h.0.to_string(), h.mesh_agent()))
            .collect()
    }

    /// Push client config to all host agents in this mesh via the HostAgent
    /// actor mesh.
    ///
    /// Each host installs the attrs as `Source::ClientOverride`.
    /// Idempotent: sending the same attrs twice replaces the layer.
    ///
    /// Implements HM-1, HM-2, HM-3, and HM-4 (see module docs): returns
    /// `Err(ConfigPushError)` if the acknowledgement barrier does not complete.
    /// Each host acks its own ordinal via a reduced status barrier, so a
    /// timeout names exactly the hosts that did not install. Only a synchronous
    /// failure to initiate the cast at all is reported mesh-wide.
    pub(crate) async fn push_config(
        &self,
        cx: &impl context::Actor,
        attrs: hyperactor_config::attrs::Attrs,
    ) -> Result<(), ConfigPushError> {
        let timeout = hyperactor_config::global::get(crate::config::MESH_ATTACH_CONFIG_TIMEOUT);
        let hosts: Vec<_> = self.values().collect();
        let num_hosts = hosts.len();

        if num_hosts == 0 {
            tracing::info!(success = 0, "push_config complete");
            return Ok(());
        }

        fn failures_for_hosts(
            hosts: &[HostRef],
            mut make_failure: impl FnMut() -> ConfigPushFailure,
        ) -> ConfigPushError {
            ConfigPushError {
                failures: hosts
                    .iter()
                    .cloned()
                    .map(|host| (host, make_failure()))
                    .collect(),
            }
        }

        let region = self.region.clone();

        // Each host posts a single-rank `Running` overlay at its ordinal once
        // it has installed the config; reduce them into a StatusMesh barrier so
        // a timeout names exactly which hosts (if any) never acknowledged.
        let (reply, rx) = cx.mailbox().open_accum_port_opts(
            crate::StatusMesh::from_single(region.clone(), Status::NotExist),
            StreamingReducerOpts {
                max_update_interval: Some(std::time::Duration::from_millis(50)),
                initial_update_interval: None,
            },
        );
        let mut reply = reply.bind();
        reply.return_undeliverable(false);

        // HM-3: an unreachable host does not bounce the outbound request into
        // the caller's `Undeliverable<MessageEnvelope>` handler — it surfaces as
        // a channel-level `BrokenLink` (logged at debug), and the missing ack is
        // detected by the barrier timeout below. `return_undeliverable(false)`
        // above covers the ack direction. Covered by
        // `test_attach_fails_closed_on_unreachable_host`.
        if let Err(err) = self.host_agent_mesh.cast(
            cx,
            host_agent::SetClientConfig {
                attrs,
                rank: Default::default(),
                reply,
            },
        ) {
            let error = err.to_string();

            tracing::warn!(error = %error, "config push cast failed");

            // The collective cast could not be initiated at all (a synchronous
            // send failure, before any host was contacted) — genuinely
            // mesh-wide, so report every host.
            return Err(failures_for_hosts(&hosts, || {
                ConfigPushFailure::CastFailed(error.clone())
            }));
        }

        match GetRankStatus::wait(rx, num_hosts, timeout, region).await {
            Ok(_) => {
                tracing::info!(success = num_hosts, "push_config complete");
                Ok(())
            }
            Err(partial) => {
                // Ranks still at `NotExist` never acknowledged within the
                // timeout (or the reply channel closed). Report exactly those
                // hosts, preserving per-host identity (HM-4).
                let failures: Vec<(HostRef, ConfigPushFailure)> = partial
                    .values()
                    .enumerate()
                    .filter(|(_, status)| status.is_not_exist())
                    .map(|(rank, _)| (hosts[rank].clone(), ConfigPushFailure::ReplyTimedOut))
                    .collect();

                tracing::info!(
                    success = num_hosts - failures.len(),
                    failed = failures.len(),
                    "push_config complete with failures"
                );

                Err(ConfigPushError { failures })
            }
        }
    }

    /// Spawn a ProcMesh onto this host mesh. The per_host extent specifies the shape
    /// of the procs to spawn on each host.
    ///
    /// `proc_bind`, when provided, is a per-process CPU/NUMA binding
    /// configuration. Its length must equal the number of ranks in
    /// `per_host`. Each entry maps binding keys (`cpunodebind`,
    /// `membind`, `physcpubind`, `cpus`) to their values.
    /// Only takes effect when running on Linux.
    ///
    /// `per_rank_bootstrap`, when provided, is a function called once
    /// per proc to produce that proc's [`BootstrapCommand`]. The
    /// function receives a [`view::Point`] over the combined
    /// `host_extent ⊕ per_host` extent. Its return value takes
    /// precedence over `self.bootstrap_command` for that proc only.
    ///
    /// Currently, spawn issues direct calls to each host agent. This will be fixed by
    /// maintaining a comm actor on the host service procs themselves.
    #[allow(clippy::result_large_err)]
    pub async fn spawn<C: context::Actor>(
        &self,
        cx: &C,
        name: &str,
        per_host: Extent,
        proc_bind: Option<Vec<ProcBind>>,
        per_rank_bootstrap: Option<Box<PerRankBootstrapFn>>,
    ) -> crate::Result<ProcMesh>
    where
        C::A: Handler<MeshFailure>,
    {
        self.spawn_inner(
            cx,
            ProcMeshId::instance(Label::strip(name)),
            per_host,
            proc_bind,
            per_rank_bootstrap,
        )
        .await
    }

    #[hyperactor::instrument(fields(host_mesh=self.id.to_string(), proc_mesh=proc_mesh_id.to_string()))]
    async fn spawn_inner<C: context::Actor>(
        &self,
        cx: &C,
        proc_mesh_id: ProcMeshId,
        per_host: Extent,
        proc_bind: Option<Vec<ProcBind>>,
        per_rank_bootstrap: Option<Box<PerRankBootstrapFn>>,
    ) -> crate::Result<ProcMesh>
    where
        C::A: Handler<MeshFailure>,
    {
        tracing::info!(name = "HostMeshStatus", status = "ProcMesh::Spawn::Attempt");
        tracing::info!(name = "ProcMeshStatus", status = "Spawn::Attempt",);
        let result = self
            .spawn_inner_inner(cx, proc_mesh_id, per_host, proc_bind, per_rank_bootstrap)
            .await;
        match &result {
            Ok(_) => {
                tracing::info!(name = "HostMeshStatus", status = "ProcMesh::Spawn::Success");
                tracing::info!(name = "ProcMeshStatus", status = "Spawn::Success");
            }
            Err(error) => {
                tracing::error!(name = "HostMeshStatus", status = "ProcMesh::Spawn::Failed", %error);
                tracing::error!(name = "ProcMeshStatus", status = "Spawn::Failed", %error);
            }
        }
        result
    }

    async fn spawn_inner_inner<C: context::Actor>(
        &self,
        cx: &C,
        proc_mesh_id: ProcMeshId,
        per_host: Extent,
        proc_bind: Option<Vec<ProcBind>>,
        per_rank_bootstrap: Option<Box<PerRankBootstrapFn>>,
    ) -> crate::Result<ProcMesh>
    where
        C::A: Handler<MeshFailure>,
    {
        let per_host_labels = per_host.labels().iter().collect::<HashSet<_>>();
        let host_labels = self.region.labels().iter().collect::<HashSet<_>>();
        if !per_host_labels
            .intersection(&host_labels)
            .collect::<Vec<_>>()
            .is_empty()
        {
            return Err(crate::Error::ConfigurationError(anyhow::anyhow!(
                "per_host dims overlap with existing dims when spawning proc mesh"
            )));
        }
        if let Some(proc_bind) = proc_bind.as_ref()
            && proc_bind.len() != per_host.num_ranks()
        {
            return Err(crate::Error::ConfigurationError(anyhow::anyhow!(
                "proc_bind length does not match per_host extent"
            )));
        }

        let extent = self
            .region
            .extent()
            .concat(&per_host)
            .map_err(|err| crate::Error::ConfigurationError(err.into()))?;

        let region: Region = extent.clone().into();

        tracing::info!(
            name = "ProcMeshStatus",
            status = "Spawn::Attempt",
            %region,
            "spawning proc mesh"
        );

        let mut procs = Vec::new();
        let num_ranks = region.num_ranks();
        // Accumulator outputs full StatusMesh snapshots; seed with
        // NotExist.
        let (port, rx) = cx.mailbox().open_accum_port_opts(
            crate::StatusMesh::from_single(region.clone(), Status::NotExist),
            StreamingReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
                initial_update_interval: None,
            },
        );

        // Create or update each proc, then fence on receiving status
        // overlays. This prevents a race where procs become
        // addressable before their local muxers are ready, which
        // could make early messages unroutable. A future improvement
        // would allow buffering in the host-level muxer to eliminate
        // the need for this synchronization step.
        let mut proc_names = Vec::new();
        let client_config_override = hyperactor_config::global::propagatable_attrs();
        for (host_rank, host) in self.ranks.iter().enumerate() {
            for per_host_rank in 0..per_host.num_ranks() {
                let create_rank = per_host.num_ranks() * host_rank + per_host_rank;
                let proc_name = ResourceId::instance(Label::strip(&format!(
                    "{}-{}",
                    proc_mesh_id
                        .display_label()
                        .map(|l| l.as_str())
                        .unwrap_or("unnamed"),
                    per_host_rank
                )));
                proc_names.push(proc_name.clone());
                let bind = proc_bind.as_ref().map(|v| v[per_host_rank].clone());
                let bootstrap_command = match per_rank_bootstrap.as_ref() {
                    Some(f) => Some(
                        f(extent
                            .point_of_rank(create_rank)
                            .expect("rank in combined extent"))
                        .map_err(crate::Error::ConfigurationError)?,
                    ),
                    None => self.bootstrap_command.clone(),
                };
                let proc_spec = resource::ProcSpec {
                    client_config_override: client_config_override.clone(),
                    bootstrap_command,
                    proc_bind: bind,
                    host_mesh_id: Some(self.id.clone()),
                };
                host.mesh_agent()
                    .create_or_update(
                        cx,
                        proc_name.clone(),
                        resource::Rank::new(create_rank),
                        proc_spec,
                    )
                    .await
                    .map_err(|e| {
                        crate::Error::HostMeshAgentConfigurationError(
                            host.mesh_agent().actor_addr().clone(),
                            format!("failed while creating proc: {}", e),
                        )
                    })?;
                let mut reply_port = port.bind();
                // If this proc dies or some other issue renders the reply undeliverable,
                // the reply does not need to be returned to the sender.
                reply_port.return_undeliverable(false);
                host.mesh_agent()
                    .get_rank_status(cx, proc_name.clone(), reply_port)
                    .await
                    .map_err(|e| {
                        crate::Error::HostMeshAgentConfigurationError(
                            host.mesh_agent().actor_addr().clone(),
                            format!("failed while querying proc status: {}", e),
                        )
                    })?;
                let proc_id = host.named_proc(&proc_name);
                tracing::info!(
                    name = "ProcMeshStatus",
                    status = "Spawn::CreatingProc",
                    %proc_id,
                    rank = create_rank,
                );
                procs.push(crate::proc_mesh::ProcRef::new(
                    proc_id,
                    create_rank,
                    // TODO: specify or retrieve from state instead, to avoid attestation.
                    ActorRef::attest(
                        host.named_proc(&proc_name)
                            .actor_addr(crate::proc_agent::PROC_AGENT_ACTOR_NAME),
                    ),
                ));
            }
        }

        let start_time = tokio::time::Instant::now();

        // Wait on accumulated StatusMesh snapshots until complete or
        // timeout.
        match GetRankStatus::wait(
            rx,
            num_ranks,
            hyperactor_config::global::get(PROC_SPAWN_MAX_IDLE),
            region.clone(), // fallback mesh if nothing arrives
        )
        .await
        {
            Ok(statuses) => {
                // If any rank is terminating, surface a
                // ProcCreationError pointing at that rank.
                if let Some((rank, status)) = statuses
                    .values()
                    .enumerate()
                    .find(|(_, s)| s.is_terminating())
                {
                    let proc_name = &proc_names[rank];
                    let host_rank = rank / per_host.num_ranks();
                    let mesh_agent = self.ranks[host_rank].mesh_agent();
                    let (reply_tx, mut reply_rx) = cx.mailbox().open_port();
                    let mut reply_tx = reply_tx.bind();
                    // If this proc dies or some other issue renders the reply undeliverable,
                    // the reply does not need to be returned to the sender.
                    reply_tx.return_undeliverable(false);
                    mesh_agent.post(
                        cx,
                        resource::GetState {
                            id: proc_name.clone(),
                            reply: reply_tx,
                        },
                    );
                    let state = match tokio::time::timeout(
                        hyperactor_config::global::get(PROC_SPAWN_MAX_IDLE),
                        reply_rx.recv(),
                    )
                    .await
                    {
                        Ok(Ok(state)) => state,
                        _ => resource::State {
                            id: proc_name.clone(),
                            status,
                            state: None,
                            generation: 0,
                            timestamp: std::time::SystemTime::now(),
                        },
                    };

                    tracing::error!(
                        name = "ProcMeshStatus",
                        status = "Spawn::GetRankStatus",
                        rank = host_rank,
                        "rank {} is terminating with state: {}",
                        host_rank,
                        state
                    );

                    return Err(crate::Error::ProcCreationError {
                        state: Box::new(state),
                        host_rank,
                        mesh_agent,
                    });
                }
            }
            Err(complete) => {
                tracing::error!(
                    name = "ProcMeshStatus",
                    status = "Spawn::GetRankStatus",
                    "timeout after {:?} when waiting for procs being created",
                    hyperactor_config::global::get(PROC_SPAWN_MAX_IDLE),
                );
                // Fill remaining ranks with a timeout status via the
                // legacy shim.
                let legacy = mesh_to_rankedvalues_with_default(
                    &complete,
                    Status::Timeout(start_time.elapsed()),
                    Status::is_not_exist,
                    num_ranks,
                );
                return Err(crate::Error::ProcSpawnError { statuses: legacy });
            }
        }

        let mut mesh = ProcMesh::create(proc_mesh_id, extent, self.clone(), procs);
        if let Ok(ref mut mesh) = mesh {
            // Spawn a unique mesh controller for each proc mesh, so the type of the
            // mesh can be preserved. Procs reached a non-terminating state above,
            // so seed the controller's per-rank statuses as Running.
            let mesh_ref: ProcMeshRef = (**mesh).clone();
            let region = ndslice::view::Ranked::region(&mesh_ref).clone();
            let initial_statuses: crate::ValueMesh<resource::Status> =
                std::iter::repeat_n(resource::Status::Running, region.num_ranks())
                    .collect_mesh::<crate::ValueMesh<_>>(region)?;
            let controller = ProcMeshController::new(mesh_ref, None, None, initial_statuses);
            // hyperactor::proc AI-3: controller name must include mesh
            // identity for proc-wide ActorAddr uniqueness.
            let controller_name = format!("{}_{}", PROC_MESH_CONTROLLER_NAME, mesh.id());
            let controller_handle = cx.spawn_with_label(&controller_name, controller);
            // Bind the actor's well-known ports (Signal, IntrospectMessage,
            // Undeliverable). Without this, the controller's mailbox has no
            // port entries and messages (including introspection queries)
            // are returned as undeliverable.
            let controller_ref: ActorRef<ProcMeshController> = controller_handle.bind();
            mesh.set_controller(Some(controller_ref));
        }
        mesh
    }

    /// The identity of the referenced host mesh.
    pub fn id(&self) -> &HostMeshId {
        &self.id
    }

    /// The host references (channel addresses) in rank order.
    pub fn hosts(&self) -> &[HostRef] {
        &self.ranks
    }

    /// Stop every proc in this proc mesh.
    ///
    /// On success returns the final per-rank `StatusMesh`, in which every
    /// rank is guaranteed to be `is_terminated()` (`Stopped`, `Failed`, or
    /// `Timeout`). Callers can apply these statuses to controller health
    /// state so that subsequent `GetState` queries reflect reality.
    ///
    /// Returns `crate::Error::ProcMeshStopError` if any rank did not reach
    /// a terminal state within `PROC_STOP_MAX_IDLE`; the error carries the
    /// best-known per-rank statuses for the same purpose.
    #[hyperactor::instrument(fields(host_mesh=self.id.to_string(), proc_mesh=proc_mesh_id.to_string()))]
    pub(crate) async fn stop_proc_mesh(
        &self,
        cx: &impl hyperactor::context::Actor,
        proc_mesh_id: &ProcMeshId,
        procs: impl IntoIterator<Item = ProcAddr>,
        region: Region,
        reason: String,
    ) -> crate::Result<crate::StatusMesh> {
        // Accumulator outputs full StatusMesh snapshots; seed with
        // NotExist.
        let mut proc_names = Vec::new();
        let num_ranks = region.num_ranks();
        // Accumulator outputs full StatusMesh snapshots; seed with
        // NotExist.
        let (port, rx) = cx.mailbox().open_accum_port_opts(
            crate::StatusMesh::from_single(region.clone(), Status::NotExist),
            StreamingReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
                initial_update_interval: None,
            },
        );
        for proc_id in procs.into_iter() {
            let addr = proc_id.addr().clone();
            // The name stored in HostAgent is not the same as the
            // one stored in the ProcMesh. We instead take each proc id
            // and map it to that particular agent.
            let proc_resource_id = ResourceId::new(proc_id.uid().clone(), proc_id.label().cloned());
            proc_names.push(proc_resource_id.clone());

            // Note that we don't send 1 message per host agent, we send 1 message
            // per proc.
            let host = HostRef::new(addr);
            host.mesh_agent().post(
                cx,
                resource::Stop {
                    id: proc_resource_id.clone(),
                    reason: reason.clone(),
                },
            );
            host.mesh_agent()
                .wait_rank_status(cx, proc_resource_id, Status::Stopped, port.bind())
                .await
                .map_err(|e| crate::Error::CallError(host.mesh_agent().actor_addr().clone(), e))?;

            tracing::info!(
                name = "ProcMeshStatus",
                %proc_id,
                status = "Stop::Sent",
            );
        }
        tracing::info!(
            name = "HostMeshStatus",
            status = "ProcMesh::Stop::Sent",
            "sending Stop to proc mesh for {} procs: {}",
            proc_names.len(),
            proc_names
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let start_time = tokio::time::Instant::now();

        match GetRankStatus::wait(
            rx,
            num_ranks,
            hyperactor_config::global::get(PROC_STOP_MAX_IDLE),
            region.clone(), // fallback mesh if nothing arrives
        )
        .await
        {
            Ok(statuses) => {
                let all_stopped = statuses.values().all(|s| s.is_terminated());
                if !all_stopped {
                    let legacy = mesh_to_rankedvalues_with_default(
                        &statuses,
                        Status::NotExist,
                        Status::is_not_exist,
                        num_ranks,
                    );
                    tracing::error!(
                        name = "ProcMeshStatus",
                        status = "FailedToStop",
                        "failed to terminate proc mesh: {:?}",
                        statuses,
                    );
                    return Err(crate::Error::ProcMeshStopError { statuses: legacy });
                }
                tracing::info!(name = "ProcMeshStatus", status = "Stopped");
                Ok(statuses)
            }
            Err(complete) => {
                // Fill remaining ranks with a timeout status via the
                // legacy shim.
                let legacy = mesh_to_rankedvalues_with_default(
                    &complete,
                    Status::Timeout(start_time.elapsed()),
                    Status::is_not_exist,
                    num_ranks,
                );
                tracing::error!(
                    name = "ProcMeshStatus",
                    status = "StoppingTimeout",
                    "failed to terminate proc mesh {} before timeout: {:?}",
                    proc_mesh_id,
                    legacy,
                );
                Err(crate::Error::ProcMeshStopError { statuses: legacy })
            }
        }
    }

    /// Get the state of all procs with Name in this host mesh.
    /// The procs iterator must be in rank order.
    /// The returned ValueMesh will have a non-empty inner state unless there
    /// was a timeout reaching the host mesh agent.
    ///
    /// If `keepalive` is `Some`, send `KeepaliveGetState` so the host agent
    /// extends each proc's expiry time; otherwise send a plain `GetState`.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn proc_states(
        &self,
        cx: &impl context::Actor,
        procs: impl IntoIterator<Item = ProcAddr>,
        region: Region,
        keepalive: Option<std::time::SystemTime>,
    ) -> crate::Result<ValueMesh<resource::State<ProcState>>> {
        let (tx, mut rx) = cx.mailbox().open_port();

        let mut num_ranks = 0;
        let procs: Vec<ProcAddr> = procs.into_iter().collect();
        let mut proc_names = Vec::new();
        for proc_id in procs.iter() {
            num_ranks += 1;
            let addr = proc_id.addr().clone();

            // Note that we don't send 1 message per host agent, we send 1 message
            // per proc.
            let host = HostRef::new(addr);
            let proc_resource_id = ResourceId::new(proc_id.uid().clone(), proc_id.label().cloned());
            proc_names.push(proc_resource_id.clone());
            let mut reply = tx.bind();
            // If this proc dies or some other issue renders the reply undeliverable,
            // the reply does not need to be returned to the sender.
            reply.return_undeliverable(false);
            let mut send_port = host.mesh_agent().port();
            // If the message is undeliverable, the timeout below will catch the issue, and the caller
            // can handle the error as it pleases. Set this so an undeliverable message doesn't cause
            // a supervision crash.
            send_port.return_undeliverable(false);
            let get_state = resource::GetState {
                id: proc_resource_id,
                reply,
            };
            if let Some(expires_after) = keepalive {
                let mut keepalive_port = host.mesh_agent().port();
                keepalive_port.return_undeliverable(false);
                keepalive_port.post(
                    cx,
                    resource::KeepaliveGetState {
                        expires_after,
                        get_state,
                    },
                );
            } else {
                send_port.post(cx, get_state);
            }
        }

        let mut states = Vec::with_capacity(num_ranks);
        let timeout = hyperactor_config::global::get(GET_PROC_STATE_MAX_IDLE);
        for _ in 0..num_ranks {
            // The agent runs on the same process as the running actor, so if some
            // fatal event caused the process to crash (e.g. OOM, signal, process exit),
            // the agent will be unresponsive.
            // We handle this by setting a timeout on the recv, and if we don't get a
            // message we assume the agent is dead and return a failed state.
            let state = tokio::time::timeout(timeout, rx.recv()).await;
            if let Ok(state) = state {
                // Handle non-timeout receiver error.
                let state = state?;
                match state.state {
                    Some(ref inner) => {
                        states.push((inner.create_rank, state));
                    }
                    None => {
                        return Err(crate::Error::NotExist(state.id));
                    }
                }
            } else {
                // Timeout error, stop reading from the receiver and send back what we have so far,
                // padding with failed states.
                tracing::warn!(
                    "Timeout waiting for response from host mesh agent for proc_states after {:?}",
                    timeout
                );
                let all_ranks = (0..num_ranks).collect::<HashSet<_>>();
                let completed_ranks = states.iter().map(|(rank, _)| *rank).collect::<HashSet<_>>();
                let mut leftover_ranks = all_ranks.difference(&completed_ranks).collect::<Vec<_>>();
                assert_eq!(leftover_ranks.len(), num_ranks - states.len());
                while states.len() < num_ranks {
                    let rank = *leftover_ranks
                        .pop()
                        .expect("leftover ranks should not be empty");
                    states.push((
                        // We populate with any ranks leftover at the time of the timeout.
                        rank,
                        resource::State {
                            id: proc_names[rank].clone(),
                            status: resource::Status::Timeout(timeout),
                            state: None,
                            generation: 0,
                            timestamp: std::time::SystemTime::now(),
                        },
                    ));
                }
                break;
            }
        }
        // Ensure that all ranks have replied. Note that if the mesh is sliced,
        // not all create_ranks may be in the mesh.
        // Sort by rank, so that the resulting mesh is ordered.
        states.sort_by_key(|(rank, _)| *rank);
        let vm = states
            .into_iter()
            .map(|(_, state)| state)
            .collect_mesh::<ValueMesh<_>>(region)?;
        Ok(vm)
    }
}

/// An ordered set of host entries, deduplicated by `HostAgent` `ActorAddr`
/// in first-seen order.
///
/// Insertion is idempotent by construction — SA-3 (dedup by ActorAddr)
/// is a property of this type, not a comment on careful control flow.
/// First-seen order is preserved: the first occurrence of a given
/// ActorAddr wins; subsequent duplicates are silently dropped.
struct HostSet {
    seen: HashSet<ActorAddr>,
    entries: Vec<(String, ActorRef<HostAgent>)>,
}

impl HostSet {
    fn new() -> Self {
        Self {
            seen: HashSet::new(),
            entries: Vec::new(),
        }
    }

    /// Insert a host entry. No-op if `ActorAddr` already present (SA-3).
    /// First-seen order is preserved.
    fn insert(&mut self, addr: String, agent_ref: ActorRef<HostAgent>) {
        if self.seen.insert(agent_ref.actor_addr().clone()) {
            self.entries.push((addr, agent_ref));
        }
    }

    /// Extend from a `HostMeshRef`. SA-3 applies per entry.
    fn extend_from_mesh(&mut self, mesh: &HostMeshRef) {
        for h in mesh.hosts() {
            self.insert(h.0.to_string(), h.mesh_agent());
        }
    }

    fn into_vec(self) -> Vec<(String, ActorRef<HostAgent>)> {
        self.entries
    }
}

/// Ordered union of hosts from meshes and optional client host
/// entries, deduplicated by `HostAgent` `ActorAddr` in first-seen
/// order.
///
/// SA-3 dedup and SA-6 client-host merge are structural properties
/// of [`HostSet`], not invariants on this function's control flow.
fn aggregate_hosts(
    meshes: &[impl AsRef<HostMeshRef>],
    client_host_entries: Option<Vec<(String, ActorRef<HostAgent>)>>,
) -> Vec<(String, ActorRef<HostAgent>)> {
    let mut set = HostSet::new();

    // SA-3: dedup across all mesh hosts in first-seen order.
    for mesh in meshes {
        set.extend_from_mesh(mesh.as_ref());
    }

    // CH-1 / SA-6: client host entries merged after mesh aggregation.
    if let Some(entries) = client_host_entries {
        for (addr, agent_ref) in entries {
            set.insert(addr, agent_ref);
        }
    }

    set.into_vec()
}

/// Spawn a [`MeshAdminAgent`] that aggregates hosts from multiple
/// meshes.
///
/// The admin agent runs on the caller's local proc — the `Proc` of
/// the actor context `cx`. Hosts are deduplicated by actor ID across
/// all meshes.
///
/// Spawn a `MeshAdminAgent` aggregating topology across one or more
/// meshes. Returns a typed `ActorRef<MeshAdminAgent>`. Callers that
/// need the admin URL query it via `get_admin_addr`.
///
/// See the `mesh_admin` module doc for the SA-* (spawn/aggregation),
/// CH-* (client host), and AI-* (admin identity) invariants.
pub async fn spawn_admin(
    meshes: impl IntoIterator<Item = impl AsRef<HostMeshRef>>,
    cx: &impl hyperactor::context::Actor,
    admin_addr: Option<std::net::SocketAddr>,
    telemetry_url: Option<String>,
) -> anyhow::Result<ActorRef<MeshAdminAgent>> {
    let meshes: Vec<_> = meshes.into_iter().collect();
    anyhow::ensure!(!meshes.is_empty(), "at least one mesh is required (SA-1)");
    for (i, mesh) in meshes.iter().enumerate() {
        anyhow::ensure!(
            !mesh.as_ref().hosts().is_empty(),
            "mesh at index {} has no hosts (SA-2)",
            i,
        );
    }

    let client_entries =
        crate::global_context::try_this_host().map(|client_host| client_host.host_entries());
    let hosts = aggregate_hosts(&meshes, client_entries);

    let root_client_id = cx.mailbox().actor_addr().clone();

    // Spawn the admin on the caller's local proc. Placement now
    // follows the caller context rather than mesh topology.
    let local_proc = cx.instance().proc();
    let agent_handle = local_proc.spawn_with_uid(
        Uid::singleton(Label::new(crate::mesh_admin::MESH_ADMIN_ACTOR_NAME).unwrap()),
        crate::mesh_admin::MeshAdminAgent::new(
            hosts,
            Some(root_client_id),
            admin_addr,
            telemetry_url,
        ),
    )?;
    let admin_ref = agent_handle.bind();
    Ok(admin_ref)
}

impl view::Ranked for HostMeshRef {
    type Item = HostRef;

    fn region(&self) -> &Region {
        &self.region
    }

    fn get(&self, rank: usize) -> Option<&Self::Item> {
        self.ranks.get(rank)
    }
}

impl view::RankedSliceable for HostMeshRef {
    fn sliced(&self, region: Region) -> Self {
        let ranks = self
            .region()
            .remap(&region)
            .unwrap()
            .map(|index| self.get(index).unwrap().clone());
        Self {
            bootstrap_command: self.bootstrap_command.clone(),
            ..Self::new(self.id.clone(), region, ranks.collect()).unwrap()
        }
    }
}

impl std::fmt::Display for HostMeshRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:", self.id)?;
        for (rank, host) in self.ranks.iter().enumerate() {
            if rank > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", host)?;
        }
        write!(f, "@{}", self.region)
    }
}

/// The type of error occuring during `HostMeshRef` parsing.
#[derive(thiserror::Error, Debug)]
pub enum HostMeshRefParseError {
    #[error(transparent)]
    RegionParseError(#[from] RegionParseError),

    #[error("invalid host mesh ref: missing region")]
    MissingRegion,

    #[error("invalid host mesh ref: missing id")]
    MissingId,

    #[error(transparent)]
    InvalidId(#[from] crate::mesh_id::ResourceIdParseError),

    #[error(transparent)]
    InvalidHostMeshRef(#[from] Box<crate::Error>),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<crate::Error> for HostMeshRefParseError {
    fn from(err: crate::Error) -> Self {
        Self::InvalidHostMeshRef(Box::new(err))
    }
}

impl FromStr for HostMeshRef {
    type Err = HostMeshRefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id_str, rest) = s.split_once(':').ok_or(HostMeshRefParseError::MissingId)?;

        let id = HostMeshId::from_str(id_str)?;

        let (hosts, region) = rest
            .split_once('@')
            .ok_or(HostMeshRefParseError::MissingRegion)?;
        let hosts = hosts
            .split(',')
            .map(|host| host.trim())
            .map(|host| host.parse::<HostRef>())
            .collect::<Result<Vec<_>, _>>()?;
        let region = region.parse()?;
        Ok(HostMeshRef::new(id, region, hosts)?)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(fbcode_build)]
    use std::assert_matches;

    #[cfg(fbcode_build)]
    use hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER;
    #[cfg(fbcode_build)]
    use hyperactor_config::attrs::Attrs;
    use ndslice::ViewExt;
    use ndslice::extent;
    #[cfg(fbcode_build)]
    use timed_test::assert_no_process_leak;
    #[cfg(fbcode_build)]
    use tokio::process::Command;
    #[cfg(fbcode_build)]
    use tracing_test::traced_test;

    use super::*;
    #[cfg(fbcode_build)]
    use crate::ActorMesh;
    #[cfg(fbcode_build)]
    use crate::Bootstrap;
    #[cfg(fbcode_build)]
    use crate::bootstrap::MESH_TAIL_LOG_LINES;
    #[cfg(fbcode_build)]
    use crate::comm::ENABLE_NATIVE_V1_CASTING;
    #[cfg(fbcode_build)]
    use crate::resource::Status;
    #[cfg(fbcode_build)]
    use crate::testactor;
    #[cfg(fbcode_build)]
    use crate::testactor::GetConfigAttrs;
    #[cfg(fbcode_build)]
    use crate::testactor::SetConfigAttrs;
    use crate::testing;

    #[test]
    fn test_host_mesh_subset() {
        let hosts: HostMeshRef = "test:local:1,local:2,local:3,local:4@replica=2/2,host=2/1"
            .parse()
            .unwrap();
        assert_eq!(
            hosts.range("replica", 1).unwrap().to_string(),
            "test:local:3,local:4@2+replica=1/2,host=2/1"
        );
    }

    #[test]
    fn test_host_mesh_ref_parse_roundtrip() {
        let host_mesh_ref = HostMeshRef::new(
            HostMeshId::singleton(Label::new("test").unwrap()),
            extent!(replica = 2, host = 2).into(),
            vec![
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
            ],
        )
        .unwrap();

        let parsed: HostMeshRef = host_mesh_ref.to_string().parse().unwrap();
        assert_eq!(parsed.id().to_string(), host_mesh_ref.id().to_string());
        assert_eq!(parsed.region(), host_mesh_ref.region());
        assert_eq!(parsed.hosts(), host_mesh_ref.hosts());
        assert_eq!(parsed.bootstrap_command, host_mesh_ref.bootstrap_command);
    }

    /// Allocate a new port on localhost. This drops the listener, releasing the socket,
    /// before returning. Hyperactor's channel::net applies SO_REUSEADDR, so we do not hav
    /// to wait out the socket's TIMED_WAIT state.
    ///
    /// Even so, this is racy.
    fn free_localhost_addr() -> ChannelAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        ChannelAddr::Tcp(listener.local_addr().unwrap())
    }

    #[cfg(fbcode_build)]
    async fn execute_extrinsic_allocation(config: &hyperactor_config::global::ConfigLock) {
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);

        let program = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");

        let hosts = vec![free_localhost_addr(), free_localhost_addr()];

        let mut children = Vec::new();
        for host in hosts.iter() {
            let mut cmd = Command::new(program.clone());
            let boot = Bootstrap::Host {
                addr: host.clone(),
                command: None, // use current binary
                config: None,
                exit_on_shutdown: false,
            };
            boot.to_env(&mut cmd);
            cmd.kill_on_drop(true);
            children.push(cmd.spawn().unwrap());
        }

        let instance = testing::instance();
        let host_mesh =
            HostMeshRef::from_hosts(HostMeshId::singleton(Label::new("test").unwrap()), hosts);

        let proc_mesh = host_mesh
            .spawn(&testing::instance(), "test", Extent::unity(), None, None)
            .await
            .unwrap();

        let actor_mesh: ActorMesh<testactor::TestActor> = proc_mesh
            .spawn(&testing::instance(), "test", &())
            .await
            .unwrap();

        testactor::assert_mesh_shape(actor_mesh).await;

        HostMesh::take(host_mesh)
            .shutdown(&instance)
            .await
            .expect("hosts shutdown");
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_extrinsic_allocation_v0() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, false);
        execute_extrinsic_allocation(&config).await;
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_extrinsic_allocation_v1() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard1 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        execute_extrinsic_allocation(&config).await;
    }

    /// `HostMesh::shutdown` emits a `Shutdown::Success` status log once every
    /// host tears down cleanly with no failed hosts (see the `tracing::info!`
    /// in `HostMesh::shutdown`). Drive the full allocate-then-shutdown path and
    /// assert that the success log fired.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "tracing-test's #[traced_test] enters a span whose Entered guard is held across awaits; this is inherent to the macro and harmless in a test"
    )]
    #[traced_test]
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_shutdown_succeeds() {
        let config = hyperactor_config::global::lock();
        execute_extrinsic_allocation(&config).await;

        assert!(
            logs_contain("Shutdown::Success"),
            "Shutdown::Success status log not found after shutting down host mesh"
        );
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_failing_proc_allocation() {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(MESH_TAIL_LOG_LINES, 100);

        let program = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");

        let hosts = vec![free_localhost_addr(), free_localhost_addr()];

        let mut children = Vec::new();
        for host in hosts.iter() {
            let mut cmd = Command::new(program.clone());
            let boot = Bootstrap::Host {
                addr: host.clone(),
                config: None,
                // The entire purpose of this is to fail:
                command: Some(BootstrapCommand::from("false")),
                exit_on_shutdown: false,
            };
            boot.to_env(&mut cmd);
            cmd.kill_on_drop(true);
            children.push(cmd.spawn().unwrap());
        }
        let host_mesh =
            HostMeshRef::from_hosts(HostMeshId::singleton(Label::new("test").unwrap()), hosts);

        let instance = testing::instance();

        let err = host_mesh
            .spawn(&instance, "test", Extent::unity(), None, None)
            .await
            .unwrap_err();
        assert_matches!(
            err,
            crate::Error::ProcCreationError { state, .. }
            if matches!(state.status, resource::Status::Failed(ref msg) if msg.contains("failed to configure process: Ready(Terminal(Stopped { exit_code: 1"))
        );
    }

    #[cfg(fbcode_build)]
    #[assert_no_process_leak]
    #[tokio::test]
    async fn test_halting_proc_allocation() {
        let config = hyperactor_config::global::lock();
        let _guard1 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(20));

        let program = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");

        let hosts = vec![free_localhost_addr(), free_localhost_addr()];

        let mut children = Vec::new();

        for (index, host) in hosts.iter().enumerate() {
            let mut cmd = Command::new(program.clone());
            let command = if index == 0 {
                let mut command = BootstrapCommand::from("sleep");
                command.args.push("60".to_string());
                Some(command)
            } else {
                None
            };
            let boot = Bootstrap::Host {
                addr: host.clone(),
                config: None,
                command,
                exit_on_shutdown: false,
            };
            boot.to_env(&mut cmd);
            cmd.kill_on_drop(true);
            children.push(cmd.spawn().unwrap());
        }
        let host_mesh =
            HostMeshRef::from_hosts(HostMeshId::singleton(Label::new("test").unwrap()), hosts);

        let instance = testing::instance();

        let err = host_mesh
            .spawn(&instance, "test", Extent::unity(), None, None)
            .await
            .unwrap_err();
        let statuses = err.into_proc_spawn_error().unwrap();
        assert_matches!(
            &statuses.materialized_iter(2).cloned().collect::<Vec<_>>()[..],
            &[Status::Timeout(_), Status::Running]
        );
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_client_config_override() {
        let config = hyperactor_config::global::lock();
        let _guard1 = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);
        let _guard2 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_mins(2),
        );
        let _guard3 = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_mins(1),
        );
        let _guard4 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_mins(2));

        // Unset env vars that were mirrored by TestOverride, so child
        // processes don't inherit them. This allows Runtime layer to
        // override ClientOverride. SAFETY: Single-threaded test under
        // global config lock.
        unsafe {
            std::env::remove_var("HYPERACTOR_HOST_SPAWN_READY_TIMEOUT");
            std::env::remove_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT");
        }

        let instance = testing::instance();

        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();
        let proc_ids = proc_mesh
            .proc_ids()
            .map(|proc_addr| proc_addr.id().clone())
            .collect::<Vec<_>>();
        let unique_proc_ids = proc_ids.iter().collect::<std::collections::HashSet<_>>();

        assert_eq!(proc_ids.len(), 2);
        assert_eq!(unique_proc_ids.len(), proc_ids.len());

        let actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(instance, "test", &()).await.unwrap();

        let mut attrs_override = Attrs::new();
        attrs_override.set(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_mins(3),
        );
        actor_mesh
            .cast(
                instance,
                SetConfigAttrs(
                    bincode::serde::encode_to_vec(&attrs_override, bincode::config::legacy())
                        .unwrap(),
                ),
            )
            .unwrap();

        let (tx, mut rx) = instance.open_port();
        actor_mesh
            .cast(instance, GetConfigAttrs(tx.bind()))
            .unwrap();
        let actual_attrs = rx.recv().await.unwrap();
        let actual_attrs =
            bincode::serde::decode_from_slice::<Attrs, _>(&actual_attrs, bincode::config::legacy())
                .map(|(v, _)| v)
                .unwrap();

        assert_eq!(
            *actual_attrs
                .get(hyperactor::config::HOST_SPAWN_READY_TIMEOUT)
                .unwrap(),
            Duration::from_mins(3)
        );
        assert_eq!(
            *actual_attrs
                .get(hyperactor::config::MESSAGE_DELIVERY_TIMEOUT)
                .unwrap(),
            Duration::from_mins(1)
        );

        let _ = hm.shutdown(instance).await;
    }

    // ---- HM-* invariant tests ----
    //
    // HM-1 (attach-config-complete) is covered by
    // `test_client_config_override` above: a successful end-to-end
    // attach + per-host config-override observation.
    //
    // The tests below cover HM-2, HM-3, and HM-4 in a single fixture
    // — `attach()` against a host address with no listener. Because
    // `testing::TestRootClient::handle::<MeshFailure>` panics on any
    // supervision event, a passing test is itself the HM-3
    // observation: no `Undeliverable<MessageEnvelope>` reached the
    // root client (had the bounce escaped, the test would panic).

    /// HM-2 / HM-3 / HM-4: `attach()` against an unreachable host
    /// returns a structured `Err` that names the failing host, and
    /// the calling actor stays alive (no supervision crash from a
    /// bounce on the request path).
    #[tokio::test]
    async fn test_attach_fails_closed_on_unreachable_host() {
        let config = hyperactor_config::global::lock();
        // Tighten the per-host timeout so the test doesn't sit on
        // the 10 s default.
        let _guard = config.override_key(
            crate::config::MESH_ATTACH_CONFIG_TIMEOUT,
            Duration::from_millis(500),
        );

        let instance = testing::instance();

        // `free_localhost_addr` binds a TCP port and immediately
        // drops the listener. SO_REUSEADDR + no further bind means
        // sends to this address never connect — exactly the
        // production-shape failure mode.
        let unreachable = free_localhost_addr();

        let id = HostMeshId::instance(Label::new("hm_test").unwrap());
        let result = HostMesh::attach(instance, id, vec![unreachable.clone()]).await;

        // HM-2: attach returns Err on any failed config push.
        let err = match result {
            Ok(_) => panic!("HM-2: attach must fail when a host is unreachable"),
            Err(e) => e,
        };

        // HM-4: the structured error names the failing host.
        let push_err = match err {
            crate::Error::ConfigPushFailed(e) => e,
            other => panic!("expected ConfigPushFailed, got: {other:?}"),
        };
        assert_eq!(push_err.failures.len(), 1);
        let (failed_host, _failure) = &push_err.failures[0];
        assert_eq!(
            failed_host,
            &HostRef::new(unreachable),
            "HM-4: failure entry must identify the unreachable host"
        );
        // Intentionally do NOT pin the `_failure` variant — the
        // contract commits to per-host identity, not to a specific
        // failure-mode subtype (see ConfigPushFailure's doc).

        // HM-3: the test process getting here without panicking is
        // itself the assertion. `TestRootClient::handle::<MeshFailure>`
        // panics on supervision events; if the request bounce had
        // escaped through `Undeliverable<MessageEnvelope>`, the
        // root-client's default delivery-failure handling would
        // surface an `UndeliverableMessageError::DeliveryFailure`,
        // supervision would fire, and we wouldn't be here.
    }

    #[test]
    fn test_host_refs_canonicalize_alias_to_dial_addr() {
        let dial_to = ChannelAddr::from_zmq_url("tcp://127.0.0.1:26600").unwrap();
        let alias = ChannelAddr::from_zmq_url("tcp://127.0.0.1:26600@tcp://0.0.0.0:26600").unwrap();

        let mesh = HostMeshRef::from_hosts(
            HostMeshId::singleton(Label::new("alias").unwrap()),
            vec![alias],
        );

        assert_eq!(mesh.hosts(), &[HostRef::new(dial_to.clone())]);
        assert_eq!(
            mesh.hosts()[0].mesh_agent().actor_addr().proc_addr().addr(),
            &dial_to
        );
    }

    #[tokio::test]
    async fn test_sa1_empty_mesh_set_rejected() {
        let instance = testing::instance();
        let result = spawn_admin(std::iter::empty::<&HostMeshRef>(), instance, None, None).await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("SA-1"), "expected SA-1 error, got: {err}");
    }

    #[tokio::test]
    async fn test_sa2_empty_hosts_rejected() {
        let instance = testing::instance();
        let mesh =
            HostMeshRef::from_hosts(HostMeshId::singleton(Label::new("empty").unwrap()), vec![]);
        let result = spawn_admin([&mesh], instance, None, None).await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("SA-2"), "expected SA-2 error, got: {err}");
    }

    /// SA-3: `HostSet::insert` is idempotent — inserting the same
    /// `ActorAddr` twice does not add a duplicate entry, and first-seen
    /// order is preserved. This is a structural property of `HostSet`,
    /// not an invariant on `aggregate_hosts` control flow.
    #[test]
    fn test_sa3_host_set_insert_idempotent() {
        let addr_a: ChannelAddr = "tcp:127.0.0.1:2001".parse().unwrap();
        let addr_b: ChannelAddr = "tcp:127.0.0.1:2002".parse().unwrap();

        let ref_a = HostRef::new(addr_a.clone()).mesh_agent();
        let ref_b = HostRef::new(addr_b.clone()).mesh_agent();

        let mut set = HostSet::new();
        set.insert(addr_a.to_string(), ref_a.clone());
        set.insert(addr_b.to_string(), ref_b.clone());
        // Insert ref_a again — should be a no-op (SA-3).
        set.insert("duplicate_addr".to_string(), ref_a.clone());

        let result = set.into_vec();
        assert_eq!(
            result.len(),
            2,
            "SA-3: duplicate ActorAddr must not add entry"
        );
        assert_eq!(
            result[0].0,
            addr_a.to_string(),
            "SA-3: first-seen order preserved"
        );
        assert_eq!(
            result[1].0,
            addr_b.to_string(),
            "SA-3: first-seen order preserved"
        );
    }

    #[test]
    fn test_sa3_aggregate_hosts_dedup() {
        let addr_a: ChannelAddr = "tcp:127.0.0.1:1001".parse().unwrap();
        let addr_b: ChannelAddr = "tcp:127.0.0.1:1002".parse().unwrap();
        let addr_c: ChannelAddr = "tcp:127.0.0.1:1003".parse().unwrap();

        // mesh_a: hosts a, b
        let mesh_a = HostMeshRef::from_hosts(
            HostMeshId::singleton(Label::new("mesh-a").unwrap()),
            vec![addr_a.clone(), addr_b.clone()],
        );
        // mesh_b: hosts b, c  (b overlaps with mesh_a)
        let mesh_b = HostMeshRef::from_hosts(
            HostMeshId::singleton(Label::new("mesh-b").unwrap()),
            vec![addr_b.clone(), addr_c.clone()],
        );

        let result = aggregate_hosts(&[&mesh_a, &mesh_b], None);

        // 3 unique hosts: a, b, c — b is deduplicated.
        assert_eq!(result.len(), 3, "expected 3 hosts, got {:?}", result);

        // First-seen order: a (mesh_a[0]), b (mesh_a[1]), c (mesh_b[1]).
        let addrs: Vec<String> = result.iter().map(|(a, _)| a.clone()).collect();
        assert_eq!(addrs[0], addr_a.to_string());
        assert_eq!(addrs[1], addr_b.to_string());
        assert_eq!(addrs[2], addr_c.to_string());
    }

    /// SA-6 / CH-1: client host entries are deduplicated against the
    /// already-aggregated mesh host set.
    #[test]
    fn test_sa6_ch1_client_host_dedup() {
        let addr_a: ChannelAddr = "tcp:127.0.0.1:1001".parse().unwrap();
        let addr_b: ChannelAddr = "tcp:127.0.0.1:1002".parse().unwrap();

        let mesh = HostMeshRef::from_hosts(
            HostMeshId::singleton(Label::new("mesh").unwrap()),
            vec![addr_a.clone(), addr_b.clone()],
        );

        // Client host entry overlaps with addr_a.
        let client_ref = HostRef::new(addr_a.clone()).mesh_agent();
        let client_entries = vec![("client_addr".to_string(), client_ref)];

        let result = aggregate_hosts(&[&mesh], Some(client_entries));

        // addr_a already in mesh — client entry is deduplicated.
        assert_eq!(result.len(), 2, "expected 2 hosts, got {:?}", result);
        let addrs: Vec<String> = result.iter().map(|(a, _)| a.clone()).collect();
        assert_eq!(addrs[0], addr_a.to_string());
        assert_eq!(addrs[1], addr_b.to_string());
    }
}
