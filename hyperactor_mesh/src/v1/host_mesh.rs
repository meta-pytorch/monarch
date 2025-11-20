/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::accum::ReducerOpts;
use hyperactor::channel::ChannelTransport;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::config;
use hyperactor::config::CONFIG;
use hyperactor::config::ConfigAttr;
use hyperactor::declare_attrs;
use hyperactor::host::Host;
use ndslice::view::CollectMeshExt;

pub mod mesh_agent;

use std::collections::HashSet;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::channel::ChannelAddr;
use hyperactor::context;
use ndslice::Extent;
use ndslice::Region;
use ndslice::ViewExt;
use ndslice::extent;
use ndslice::view;
use ndslice::view::Ranked;
use ndslice::view::RegionParseError;
use serde::Deserialize;
use serde::Serialize;

use crate::Bootstrap;
use crate::alloc::Alloc;
use crate::bootstrap::BootstrapCommand;
use crate::bootstrap::BootstrapProcManager;
use crate::proc_mesh::DEFAULT_TRANSPORT;
use crate::resource;
use crate::resource::CreateOrUpdateClient;
use crate::resource::GetRankStatus;
use crate::resource::GetRankStatusClient;
use crate::resource::ProcSpec;
use crate::resource::RankedValues;
use crate::resource::Status;
use crate::v1;
use crate::v1::Name;
use crate::v1::ProcMesh;
use crate::v1::ProcMeshRef;
use crate::v1::ValueMesh;
use crate::v1::host_mesh::mesh_agent::HostAgentMode;
pub use crate::v1::host_mesh::mesh_agent::HostMeshAgent;
use crate::v1::host_mesh::mesh_agent::HostMeshAgentProcMeshTrampoline;
use crate::v1::host_mesh::mesh_agent::ProcState;
use crate::v1::host_mesh::mesh_agent::ShutdownHostClient;
use crate::v1::mesh_controller::HostMeshController;
use crate::v1::mesh_controller::ProcMeshController;
use crate::v1::proc_mesh::ProcRef;

declare_attrs! {
    /// The maximum idle time between updates while spawning proc
    /// meshes.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE".to_string()),
        py_name: None,
    })
    pub attr PROC_SPAWN_MAX_IDLE: Duration = Duration::from_secs(30);

    /// The maximum idle time between updates while stopping proc
    /// meshes.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_PROC_STOP_MAX_IDLE".to_string()),
        py_name: None,
    })
    pub attr PROC_STOP_MAX_IDLE: Duration = Duration::from_secs(30);

    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_GET_PROC_STATE_MAX_IDLE".to_string()),
        py_name: None,
    })
    pub attr GET_PROC_STATE_MAX_IDLE: Duration = Duration::from_secs(60);
}

/// A reference to a single host.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct HostRef(ChannelAddr);

impl HostRef {
    /// The host mesh agent associated with this host.
    fn mesh_agent(&self) -> ActorRef<HostMeshAgent> {
        ActorRef::attest(self.service_proc().actor_id("agent", 0))
    }

    /// The ProcId for the proc with name `name` on this host.
    fn named_proc(&self, name: &Name) -> ProcId {
        ProcId::Direct(self.0.clone(), name.to_string())
    }

    /// The service proc on this host.
    fn service_proc(&self) -> ProcId {
        ProcId::Direct(self.0.clone(), "service".to_string())
    }

    /// Request an orderly teardown of this host and all procs it
    /// spawned.
    ///
    /// This resolves the per-child grace **timeout** and the maximum
    /// termination **concurrency** from config and sends a
    /// [`ShutdownHost`] message to the host's agent. The agent then:
    ///
    /// 1) Performs a graceful termination pass over all tracked
    ///    children (TERM → wait(`timeout`) → KILL), with at most
    ///    `max_in_flight` running concurrently.
    /// 2) After the pass completes, **drops the Host**, which also
    ///    drops the embedded `BootstrapProcManager`. The manager's
    ///    `Drop` serves as a last-resort safety net (it SIGKILLs
    ///    anything that somehow remains).
    ///
    /// This call returns `Ok(()))` only after the agent has finished
    /// the termination pass and released the host, so the host is no
    /// longer reachable when this returns.
    pub(crate) async fn shutdown(
        &self,
        cx: &impl hyperactor::context::Actor,
    ) -> anyhow::Result<()> {
        let agent = self.mesh_agent();
        let terminate_timeout =
            hyperactor::config::global::get(crate::bootstrap::MESH_TERMINATE_TIMEOUT);
        let max_in_flight =
            hyperactor::config::global::get(crate::bootstrap::MESH_TERMINATE_CONCURRENCY);
        agent
            .shutdown_host(cx, terminate_timeout, max_in_flight.clamp(1, 256))
            .await?;
        Ok(())
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
        Ok(HostRef(ChannelAddr::from_str(s)?))
    }
}

/// An owned mesh of hosts.
///
/// # Lifecycle
/// `HostMesh` owns host lifecycles. Callers **must** invoke
/// [`HostMesh::shutdown`] for deterministic teardown. The `Drop` impl
/// performs **best-effort** cleanup only (spawned via Tokio if
/// available); it is a safety net, not a substitute for orderly
/// shutdown.
///
/// In tests and production, prefer explicit shutdown to guarantee
/// that host agents drop their `BootstrapProcManager`s and that all
/// child procs are reaped.
#[allow(dead_code)]
pub struct HostMesh {
    name: Name,
    extent: Extent,
    allocation: HostMeshAllocation,
    current_ref: HostMeshRef,
}

/// Allocation backing for an owned [`HostMesh`].
///
/// This enum records how the underlying hosts were provisioned, which
/// in turn determines how their lifecycle is managed:
///
/// - `ProcMesh`: Hosts were allocated intrinsically via a
///   [`ProcMesh`]. The `HostMesh` owns the proc mesh and its service
///   procs, and dropping the mesh ensures that all spawned child procs
///   are terminated.
/// - `Owned`: Hosts were constructed externally and "taken" under
///   ownership. The `HostMesh` assumes responsibility for their
///   lifecycle from this point forward, ensuring consistent cleanup on
///   drop.
///
/// Additional variants may be added for other provisioning sources,
/// but in all cases `HostMesh` is an owned resource that guarantees
/// no leaked child processes.
#[allow(dead_code)]
enum HostMeshAllocation {
    /// Hosts were allocated intrinsically via a [`ProcMesh`].
    ///
    /// In this mode, the `HostMesh` owns both the `ProcMesh` itself
    /// and the service procs that implement each host. Dropping the
    /// `HostMesh` also drops the embedded `ProcMesh`, ensuring that
    /// all spawned child procs are terminated cleanly.
    ProcMesh {
        proc_mesh: ProcMesh,
        proc_mesh_ref: ProcMeshRef,
        hosts: Vec<HostRef>,
    },
    /// Hosts were constructed externally and explicitly transferred
    /// under ownership by this `HostMesh`.
    ///
    /// In this mode, the `HostMesh` assumes responsibility for the
    /// provided hosts going forward. Dropping the mesh guarantees
    /// teardown of all associated state and signals to prevent any
    /// leaked processes.
    Owned { hosts: Vec<HostRef> },
}

impl HostMesh {
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
    ///   up the mesh. we create a `Host`, spawn a `HostMeshAgent` in
    ///   it, and build a single-host `HostMesh` around that. that
    ///   `HostMesh` is returned to the caller.
    ///
    /// This API is intended for tests, examples, and local bring-up,
    /// not production.
    ///
    /// TODO: fix up ownership
    pub async fn local() -> v1::Result<HostMesh> {
        Self::local_with_bootstrap(BootstrapCommand::current()?).await
    }

    /// Same as [`local`], but the caller supplies the
    /// `BootstrapCommand` instead of deriving it from the current
    /// process.
    ///
    /// The provided `bootstrap_cmd` is used when spawning bootstrap
    /// children and determines the behavior of
    /// `boot.bootstrap().await` in those children.
    pub async fn local_with_bootstrap(bootstrap_cmd: BootstrapCommand) -> v1::Result<HostMesh> {
        if let Ok(Some(boot)) = Bootstrap::get_from_env() {
            let err = boot.bootstrap().await;
            tracing::error!("failed to bootstrap local host mesh process: {}", err);
            std::process::exit(1);
        }

        let addr = config::global::get_cloned(DEFAULT_TRANSPORT).any();

        let manager = BootstrapProcManager::new(bootstrap_cmd)?;
        let (host, _handle) = Host::serve(manager, addr).await?;
        let addr = host.addr().clone();
        let host_mesh_agent = host
            .system_proc()
            .clone()
            .spawn::<HostMeshAgent>("agent", HostAgentMode::Process(host))
            .await
            .map_err(v1::Error::SingletonActorSpawnError)?;
        host_mesh_agent.bind::<HostMeshAgent>();

        let host = HostRef(addr);
        let host_mesh_ref =
            HostMeshRef::new(Name::new("local"), extent!(hosts = 1).into(), vec![host])?;
        Ok(HostMesh::take(host_mesh_ref))
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
    pub async fn process(extent: Extent, command: BootstrapCommand) -> v1::Result<HostMesh> {
        if let Ok(Some(boot)) = Bootstrap::get_from_env() {
            let err = boot.bootstrap().await;
            tracing::error!("failed to bootstrap process host mesh process: {}", err);
            std::process::exit(1);
        }

        let transport = config::global::get_cloned(DEFAULT_TRANSPORT);
        let mut hosts = Vec::with_capacity(extent.num_ranks());
        for _ in 0..extent.num_ranks() {
            // Note: this can be racy. Possibly we should have a callback channel.
            let addr = transport.any();
            let bootstrap = Bootstrap::Host {
                addr: addr.clone(),
                command: Some(command.clone()),
                config: Some(config::global::attrs()),
            };

            let mut cmd = command.new();
            bootstrap.to_env(&mut cmd);
            cmd.spawn()?;
            hosts.push(HostRef(addr));
        }

        let host_mesh_ref = HostMeshRef::new(Name::new("process"), extent.into(), hosts)?;
        Ok(HostMesh::take(host_mesh_ref))
    }

    /// Allocate a host mesh from an [`Alloc`]. This creates a HostMesh with the same extent
    /// as the provided alloc. Allocs generate procs, and thus we define and run a Host for each
    /// proc allocated by it.
    ///
    /// ## Allocation strategy
    ///
    /// Because HostMeshes use direct-addressed procs, and must fully control the procs they are
    /// managing, `HostMesh::allocate` uses a trampoline actor to launch the host, which in turn
    /// runs a [`crate::v1::host_mesh::mesh_agent::HostMeshAgent`] actor to manage the host itself.
    /// The host (and thus all of its procs) are exposed directly through a separate listening
    /// channel, established by the host.
    ///
    /// ```text
    ///                        ┌ ─ ─┌────────────────────┐
    ///                             │allocated Proc:     │
    ///                        │    │ ┌─────────────────┐│
    ///                             │ │TrampolineActor  ││
    ///                        │    │ │ ┌──────────────┐││
    ///                             │ │ │Host          │││
    ///               ┌────┬ ─ ┘    │ │ │ ┌──────────┐ │││
    ///            ┌─▶│Proc│        │ │ │ │HostAgent │ │││
    ///            │  └────┴ ─ ┐    │ │ │ └──────────┘ │││
    ///            │  ┌────┐        │ │ │             ██████
    /// ┌────────┐ ├─▶│Proc│   │    │ │ └──────────────┘││ ▲
    /// │ Client │─┤  └────┘        │ └─────────────────┘│ listening channel
    /// └────────┘ │  ┌────┐   └ ─ ─└────────────────────┘
    ///            ├─▶│Proc│
    ///            │  └────┘
    ///            │  ┌────┐
    ///            └─▶│Proc│
    ///               └────┘
    ///                 ▲
    ///
    ///          `Alloc`-provided
    ///                procs
    /// ```
    ///
    /// ## Lifecycle
    ///
    /// The returned `HostMesh` **owns** the underlying hosts. Call
    /// [`shutdown`](Self::shutdown) to deterministically tear them
    /// down. If you skip shutdown, `Drop` will attempt best-effort
    /// cleanup only. Do not rely on `Drop` for correctness.
    pub async fn allocate(
        cx: &impl context::Actor,
        alloc: Box<dyn Alloc + Send + Sync>,
        name: &str,
        bootstrap_params: Option<BootstrapCommand>,
    ) -> v1::Result<Self> {
        Self::allocate_inner(cx, alloc, Name::new(name), bootstrap_params).await
    }

    // Use allocate_inner to set field mesh_name in span
    #[hyperactor::instrument(fields(mesh_name=name.to_string()))]
    async fn allocate_inner(
        cx: &impl context::Actor,
        alloc: Box<dyn Alloc + Send + Sync>,
        name: Name,
        bootstrap_params: Option<BootstrapCommand>,
    ) -> v1::Result<Self> {
        tracing::info!(name = "HostMeshStatus", status = "Allocate::Attempt");
        let transport = alloc.transport();
        let extent = alloc.extent().clone();
        let is_local = alloc.is_local();
        let proc_mesh = ProcMesh::allocate(cx, alloc, name.name()).await?;

        // TODO: figure out how to deal with MAST allocs. It requires an extra dimension,
        // into which it launches multiple procs, so we need to always specify an additional
        // sub-host dimension of size 1.

        let (mesh_agents, mut mesh_agents_rx) = cx.mailbox().open_port();
        let _trampoline_actor_mesh = proc_mesh
            .spawn::<HostMeshAgentProcMeshTrampoline>(
                cx,
                "host_mesh_trampoline",
                &(transport, mesh_agents.bind(), bootstrap_params, is_local),
            )
            .await?;

        // TODO: don't re-rank the hosts
        let mut hosts = Vec::new();
        for _rank in 0..extent.num_ranks() {
            let mesh_agent = mesh_agents_rx.recv().await?;

            let Some((addr, _)) = mesh_agent.actor_id().proc_id().as_direct() else {
                return Err(v1::Error::HostMeshAgentConfigurationError(
                    mesh_agent.actor_id().clone(),
                    "host mesh agent must be a direct actor".to_string(),
                ));
            };

            let host_ref = HostRef(addr.clone());
            if host_ref.mesh_agent() != mesh_agent {
                return Err(v1::Error::HostMeshAgentConfigurationError(
                    mesh_agent.actor_id().clone(),
                    format!(
                        "expected mesh agent actor id to be {}",
                        host_ref.mesh_agent().actor_id()
                    ),
                ));
            }
            hosts.push(host_ref);
        }

        let proc_mesh_ref = proc_mesh.clone();
        let mesh = Self {
            name: name.clone(),
            extent: extent.clone(),
            allocation: HostMeshAllocation::ProcMesh {
                proc_mesh,
                proc_mesh_ref,
                hosts: hosts.clone(),
            },
            current_ref: HostMeshRef::new(name, extent.into(), hosts).unwrap(),
        };

        // Spawn a unique mesh controller for each proc mesh, so the type of the
        // mesh can be preserved.
        let _controller: ActorHandle<HostMeshController> =
            HostMeshController::spawn(cx, mesh.deref().clone())
                .await
                .map_err(|e| v1::Error::ControllerActorSpawnError(mesh.name().clone(), e))?;

        tracing::info!(name = "HostMeshStatus", status = "Allocate::Created");
        Ok(mesh)
    }

    /// Take ownership of an existing host mesh reference.
    ///
    /// Consumes the `HostMeshRef`, captures its region/hosts, and
    /// returns an owned `HostMesh` that assumes lifecycle
    /// responsibility for those hosts (i.e., will shut them down on
    /// Drop).
    pub fn take(mesh: HostMeshRef) -> Self {
        let region = mesh.region().clone();
        let hosts: Vec<HostRef> = mesh.values().collect();

        let current_ref = HostMeshRef::new(mesh.name.clone(), region.clone(), hosts.clone())
            .expect("region/hosts cardinality must match");

        Self {
            name: mesh.name,
            extent: region.extent().clone(),
            allocation: HostMeshAllocation::Owned { hosts },
            current_ref,
        }
    }

    /// Request a clean shutdown of all hosts owned by this
    /// `HostMesh`.
    ///
    /// For each host, this sends `ShutdownHost` to its
    /// `HostMeshAgent`. The agent takes and drops its `Host` (via
    /// `Option::take()`), which in turn drops the embedded
    /// `BootstrapProcManager`. On drop, the manager walks its PID
    /// table and sends SIGKILL to any procs it spawned—tying proc
    /// lifetimes to their hosts and preventing leaks.
    pub async fn shutdown(&self, cx: &impl hyperactor::context::Actor) -> anyhow::Result<()> {
        let mut attempted = 0;
        let mut ok = 0;
        for host in self.current_ref.values() {
            attempted += 1;
            if let Err(e) = host.shutdown(cx).await {
                tracing::warn!(host = %host, error = %e, "host shutdown failed");
            } else {
                ok += 1;
            }
        }
        tracing::info!(attempted, ok, "hostmesh shutdown summary");
        Ok(())
    }
}

impl Deref for HostMesh {
    type Target = HostMeshRef;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
    }
}

impl Drop for HostMesh {
    /// Best-effort cleanup for owned host meshes on drop.
    ///
    /// When a `HostMesh` is dropped, it attempts to shut down all
    /// hosts it owns:
    /// - If a Tokio runtime is available, we spawn an ephemeral
    ///   `Proc` + `Instance` and send `ShutdownHost` messages to each
    ///   host. This ensures that the embedded `BootstrapProcManager`s
    ///   are dropped, and all child procs they spawned are killed.
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
            mesh_name = %self.name,
            status = "Dropping",
        );
        // Snapshot the owned hosts we're responsible for.
        let hosts: Vec<HostRef> = match &self.allocation {
            HostMeshAllocation::ProcMesh { hosts, .. } | HostMeshAllocation::Owned { hosts } => {
                hosts.clone()
            }
        };

        // Best-effort only when a Tokio runtime is available.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mesh_name = self.name.clone();
            let allocation_label = match &self.allocation {
                HostMeshAllocation::ProcMesh { .. } => "proc_mesh",
                HostMeshAllocation::Owned { .. } => "owned",
            }
            .to_string();

            handle.spawn(async move {
                let span = tracing::info_span!(
                    "hostmesh_drop_cleanup",
                    %mesh_name,
                    allocation = %allocation_label,
                    hosts = hosts.len(),
                );
                let _g = span.enter();

                // Spin up a tiny ephemeral proc+instance to get an
                // Actor context.
                match hyperactor::Proc::direct(
                    ChannelTransport::Unix.any(),
                    "hostmesh-drop".to_string(),
                )
                    .await
                {
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "failed to construct ephemeral Proc for drop-cleanup; \
                             relying on PDEATHSIG/manager Drop"
                        );
                    }
                    Ok(proc) => {
                        match proc.instance("drop") {
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "failed to create ephemeral instance for drop-cleanup; \
                                     relying on PDEATHSIG/manager Drop"
                                );
                            }
                            Ok((instance, _guard)) => {
                                let mut attempted = 0usize;
                                let mut ok = 0usize;
                                let mut err = 0usize;

                                for host in hosts {
                                    attempted += 1;
                                    tracing::debug!(host = %host, "drop-cleanup: shutdown start");
                                    match host.shutdown(&instance).await {
                                        Ok(()) => {
                                            ok += 1;
                                            tracing::debug!(host = %host, "drop-cleanup: shutdown ok");
                                        }
                                        Err(e) => {
                                            err += 1;
                                            tracing::warn!(host = %host, error = %e, "drop-cleanup: shutdown failed");
                                        }
                                    }
                                }

                                tracing::info!(
                                    attempted, ok, err,
                                    "hostmesh drop-cleanup summary"
                                );
                            }
                        }
                    }
                }
            });
        } else {
            // No runtime here; PDEATHSIG and manager Drop remain the
            // last-resort safety net.
            tracing::warn!(
                mesh_name = %self.name,
                hosts = hosts.len(),
                "HostMesh dropped without a tokio runtime; skipping best-effort shutdown"
            );
        }

        tracing::info!(
            name = "HostMeshStatus",
            mesh_name = %self.name,
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct HostMeshRef {
    name: Name,
    region: Region,
    ranks: Arc<Vec<HostRef>>,
}

impl HostMeshRef {
    /// Create a new (raw) HostMeshRef from the provided region and associated
    /// ranks, which must match in cardinality.
    #[allow(clippy::result_large_err)]
    fn new(name: Name, region: Region, ranks: Vec<HostRef>) -> v1::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(v1::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        Ok(Self {
            name,
            region,
            ranks: Arc::new(ranks),
        })
    }

    /// Create a new HostMeshRef from an arbitrary set of hosts. This is meant to
    /// enable extrinsic bootstrapping.
    pub fn from_hosts(name: Name, hosts: Vec<ChannelAddr>) -> Self {
        Self {
            name,
            region: extent!(hosts = hosts.len()).into(),
            ranks: Arc::new(hosts.into_iter().map(HostRef).collect()),
        }
    }

    /// Spawn a ProcMesh onto this host mesh. The per_host extent specifies the shape
    /// of the procs to spawn on each host.
    ///
    /// Currently, spawn issues direct calls to each host agent. This will be fixed by
    /// maintaining a comm actor on the host service procs themselves.
    #[allow(clippy::result_large_err)]
    pub async fn spawn(
        &self,
        cx: &impl context::Actor,
        name: &str,
        per_host: Extent,
    ) -> v1::Result<ProcMesh> {
        self.spawn_inner(cx, Name::new(name), per_host).await
    }

    #[hyperactor::instrument(fields(mesh_name=mesh_name.to_string()))]
    async fn spawn_inner(
        &self,
        cx: &impl context::Actor,
        mesh_name: Name,
        per_host: Extent,
    ) -> v1::Result<ProcMesh> {
        let per_host_labels = per_host.labels().iter().collect::<HashSet<_>>();
        let host_labels = self.region.labels().iter().collect::<HashSet<_>>();
        if !per_host_labels
            .intersection(&host_labels)
            .collect::<Vec<_>>()
            .is_empty()
        {
            return Err(v1::Error::ConfigurationError(anyhow::anyhow!(
                "per_host dims overlap with existing dims when spawning proc mesh"
            )));
        }

        let extent = self
            .region
            .extent()
            .concat(&per_host)
            .map_err(|err| v1::Error::ConfigurationError(err.into()))?;

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
            crate::v1::StatusMesh::from_single(region.clone(), Status::NotExist),
            Some(ReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
            }),
        );

        // Create or update each proc, then fence on receiving status
        // overlays. This prevents a race where procs become
        // addressable before their local muxers are ready, which
        // could make early messages unroutable. A future improvement
        // would allow buffering in the host-level muxer to eliminate
        // the need for this synchronization step.
        let mut proc_names = Vec::new();
        let client_config_override = config::global::attrs();
        for (host_rank, host) in self.ranks.iter().enumerate() {
            for per_host_rank in 0..per_host.num_ranks() {
                let create_rank = per_host.num_ranks() * host_rank + per_host_rank;
                let proc_name = Name::new(format!("{}_{}", mesh_name.name(), per_host_rank));
                proc_names.push(proc_name.clone());
                host.mesh_agent()
                    .create_or_update(
                        cx,
                        proc_name.clone(),
                        resource::Rank::new(create_rank),
                        ProcSpec::new(client_config_override.clone()),
                    )
                    .await
                    .map_err(|e| {
                        v1::Error::HostMeshAgentConfigurationError(
                            host.mesh_agent().actor_id().clone(),
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
                        v1::Error::HostMeshAgentConfigurationError(
                            host.mesh_agent().actor_id().clone(),
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
                procs.push(ProcRef::new(
                    proc_id,
                    create_rank,
                    // TODO: specify or retrieve from state instead, to avoid attestation.
                    ActorRef::attest(host.named_proc(&proc_name).actor_id("agent", 0)),
                ));
            }
        }

        let start_time = RealClock.now();

        // Wait on accumulated StatusMesh snapshots until complete or
        // timeout.
        match GetRankStatus::wait(
            rx,
            num_ranks,
            config::global::get(PROC_SPAWN_MAX_IDLE),
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
                    mesh_agent
                        .send(
                            cx,
                            resource::GetState {
                                name: proc_name.clone(),
                                reply: reply_tx,
                            },
                        )
                        .map_err(|e| {
                            v1::Error::SendingError(mesh_agent.actor_id().clone(), e.into())
                        })?;
                    let state = match RealClock
                        .timeout(config::global::get(PROC_SPAWN_MAX_IDLE), reply_rx.recv())
                        .await
                    {
                        Ok(Ok(state)) => state,
                        _ => resource::State {
                            name: proc_name.clone(),
                            status,
                            state: None,
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

                    return Err(v1::Error::ProcCreationError {
                        state,
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
                    config::global::get(PROC_SPAWN_MAX_IDLE),
                );
                // Fill remaining ranks with a timeout status via the
                // legacy shim.
                let legacy = mesh_to_rankedvalues_with_default(
                    &complete,
                    Status::Timeout(start_time.elapsed()),
                    Status::is_not_exist,
                    num_ranks,
                );
                return Err(v1::Error::ProcSpawnError { statuses: legacy });
            }
        }

        let mesh =
            ProcMesh::create_owned_unchecked(cx, mesh_name, extent, self.clone(), procs).await;
        tracing::info!(name = "ProcMeshStatus", status = "Spawn::Created",);
        if let Ok(ref mesh) = mesh {
            // Spawn a unique mesh controller for each proc mesh, so the type of the
            // mesh can be preserved.
            let _controller: ActorHandle<ProcMeshController> =
                ProcMeshController::spawn(cx, mesh.deref().clone())
                    .await
                    .map_err(|e| v1::Error::ControllerActorSpawnError(mesh.name().clone(), e))?;
        }
        mesh
    }

    /// The name of the referenced host mesh.
    pub fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) async fn stop_proc_mesh(
        &self,
        cx: &impl hyperactor::context::Actor,
        proc_mesh_name: &Name,
        procs: impl IntoIterator<Item = ProcId>,
        region: Region,
    ) -> anyhow::Result<()> {
        // Accumulator outputs full StatusMesh snapshots; seed with
        // NotExist.
        let mut proc_names = Vec::new();
        let num_ranks = region.num_ranks();
        // Accumulator outputs full StatusMesh snapshots; seed with
        // NotExist.
        let (port, rx) = cx.mailbox().open_accum_port_opts(
            crate::v1::StatusMesh::from_single(region.clone(), Status::NotExist),
            Some(ReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
            }),
        );
        for proc_id in procs.into_iter() {
            let Some((addr, proc_name)) = proc_id.as_direct() else {
                return Err(anyhow::anyhow!(
                    "host mesh proc {} must be direct addressed",
                    proc_id,
                ));
            };
            // The name stored in HostMeshAgent is not the same as the
            // one stored in the ProcMesh. We instead take each proc id
            // and map it to that particular agent.
            let proc_name = proc_name.parse::<Name>()?;
            proc_names.push(proc_name.clone());

            // Note that we don't send 1 message per host agent, we send 1 message
            // per proc.
            let host = HostRef(addr.clone());
            host.mesh_agent().send(
                cx,
                resource::Stop {
                    name: proc_name.clone(),
                },
            )?;
            host.mesh_agent()
                .get_rank_status(cx, proc_name, port.bind())
                .await?;

            tracing::info!(
                name = "ProcMeshStatus",
                mesh_name = %proc_mesh_name,
                %proc_id,
                status = "Stop::Sent",
            );
        }
        tracing::info!(
            mesh_name = %self.name,
            name = "HostMeshStatus",
            status = "ProcMesh::Stop::Sent",
            "Sending Stop to host mesh {} for {:?} procs",
            self.name,
            proc_names
        );

        let start_time = RealClock.now();

        match GetRankStatus::wait(
            rx,
            num_ranks,
            config::global::get(PROC_STOP_MAX_IDLE),
            region.clone(), // fallback mesh if nothing arrives
        )
        .await
        {
            Ok(statuses) => {
                let all_stopped = statuses.values().all(|s| s.is_terminating());
                if !all_stopped {
                    tracing::error!(
                        name = "ProcMeshStatus",
                        mesh_name = %proc_mesh_name,
                        status = "FailedToStop",
                        "failed to terminate proc mesh: {:?}",
                        statuses,
                    );
                    return Err(anyhow::anyhow!(
                        "failed to terminate proc mesh: {:?}",
                        statuses,
                    ));
                }
                tracing::info!(
                    name = "ProcMeshStatus",
                    mesh_name = %proc_mesh_name,
                    status = "Stopped",
                );
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
                    mesh_name = %proc_mesh_name,
                    status = "StoppingTimeout",
                    "failed to terminate proc mesh before timeout: {:?}",
                    legacy,
                );
                return Err(anyhow::anyhow!(
                    "failed to terminate proc mesh {} before timeout: {:?}",
                    proc_mesh_name,
                    legacy
                ));
            }
        }
        Ok(())
    }

    /// Get the state of all procs with Name in this host mesh.
    /// The procs iterator must be in rank order.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn proc_states(
        &self,
        cx: &impl context::Actor,
        procs: impl IntoIterator<Item = ProcId>,
        region: Region,
    ) -> v1::Result<ValueMesh<resource::State<ProcState>>> {
        let (tx, mut rx) = cx.mailbox().open_port();

        let mut num_ranks = 0;
        let procs: Vec<ProcId> = procs.into_iter().collect();
        let mut proc_names = Vec::new();
        for proc_id in procs.iter() {
            num_ranks += 1;
            let Some((addr, proc_name)) = proc_id.as_direct() else {
                return Err(v1::Error::ConfigurationError(anyhow::anyhow!(
                    "host mesh proc {} must be direct addressed",
                    proc_id,
                )));
            };

            // Note that we don't send 1 message per host agent, we send 1 message
            // per proc.
            let host = HostRef(addr.clone());
            let proc_name = proc_name.parse::<Name>()?;
            proc_names.push(proc_name.clone());
            let mut reply = tx.bind();
            // If this proc dies or some other issue renders the reply undeliverable,
            // the reply does not need to be returned to the sender.
            reply.return_undeliverable(false);
            host.mesh_agent()
                .send(
                    cx,
                    resource::GetState {
                        name: proc_name,
                        reply,
                    },
                )
                .map_err(|e| {
                    v1::Error::CallError(host.mesh_agent().actor_id().clone(), e.into())
                })?;
        }

        let mut states = Vec::with_capacity(num_ranks);
        let timeout = config::global::get(GET_PROC_STATE_MAX_IDLE);
        for _ in 0..num_ranks {
            // The agent runs on the same process as the running actor, so if some
            // fatal event caused the process to crash (e.g. OOM, signal, process exit),
            // the agent will be unresponsive.
            // We handle this by setting a timeout on the recv, and if we don't get a
            // message we assume the agent is dead and return a failed state.
            let state = RealClock.timeout(timeout, rx.recv()).await;
            if let Ok(state) = state {
                // Handle non-timeout receiver error.
                let state = state?;
                match state.state {
                    Some(ref inner) => {
                        states.push((inner.create_rank, state));
                    }
                    None => {
                        return Err(v1::Error::NotExist(state.name));
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
                            name: proc_names[rank].clone(),
                            status: resource::Status::Timeout(timeout),
                            state: None,
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
        Self::new(self.name.clone(), region, ranks.collect()).unwrap()
    }
}

impl std::fmt::Display for HostMeshRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:", self.name)?;
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

    #[error("invalid host mesh ref: missing name")]
    MissingName,

    #[error(transparent)]
    InvalidName(#[from] v1::NameParseError),

    #[error(transparent)]
    InvalidHostMeshRef(#[from] Box<v1::Error>),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<v1::Error> for HostMeshRefParseError {
    fn from(err: v1::Error) -> Self {
        Self::InvalidHostMeshRef(Box::new(err))
    }
}

impl FromStr for HostMeshRef {
    type Err = HostMeshRefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, rest) = s
            .split_once(':')
            .ok_or(HostMeshRefParseError::MissingName)?;

        let name = Name::from_str(name)?;

        let (hosts, region) = rest
            .split_once('@')
            .ok_or(HostMeshRefParseError::MissingRegion)?;
        let hosts = hosts
            .split(',')
            .map(|host| host.trim())
            .map(|host| host.parse::<HostRef>())
            .collect::<Result<Vec<_>, _>>()?;
        let region = region.parse()?;
        Ok(HostMeshRef::new(name, region, hosts)?)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashSet;
    use std::collections::VecDeque;

    use hyperactor::attrs::Attrs;
    use hyperactor::context::Mailbox as _;
    use itertools::Itertools;
    use ndslice::ViewExt;
    use ndslice::extent;
    use tokio::process::Command;

    use super::*;
    use crate::Bootstrap;
    use crate::bootstrap::MESH_TAIL_LOG_LINES;
    use crate::resource::Status;
    use crate::v1::ActorMesh;
    use crate::v1::testactor;
    use crate::v1::testactor::GetConfigAttrs;
    use crate::v1::testactor::SetConfigAttrs;
    use crate::v1::testing;

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
            Name::new("test"),
            extent!(replica = 2, host = 2).into(),
            vec![
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
            ],
        )
        .unwrap();

        assert_eq!(
            host_mesh_ref.to_string().parse::<HostMeshRef>().unwrap(),
            host_mesh_ref
        );
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_allocate() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);

        let instance = testing::instance().await;

        for alloc in testing::allocs(extent!(replicas = 4)).await {
            let host_mesh = HostMesh::allocate(instance, alloc, "test", None)
                .await
                .unwrap();

            let proc_mesh1 = host_mesh
                .spawn(instance, "test_1", Extent::unity())
                .await
                .unwrap();

            let actor_mesh1: ActorMesh<testactor::TestActor> =
                proc_mesh1.spawn(instance, "test", &()).await.unwrap();

            let proc_mesh2 = host_mesh
                .spawn(instance, "test_2", extent!(gpus = 3, extra = 2))
                .await
                .unwrap();
            assert_eq!(
                proc_mesh2.extent(),
                extent!(replicas = 4, gpus = 3, extra = 2)
            );
            assert_eq!(proc_mesh2.values().count(), 24);

            let actor_mesh2: ActorMesh<testactor::TestActor> =
                proc_mesh2.spawn(instance, "test", &()).await.unwrap();
            assert_eq!(
                actor_mesh2.extent(),
                extent!(replicas = 4, gpus = 3, extra = 2)
            );
            assert_eq!(actor_mesh2.values().count(), 24);

            // Host meshes can be dereferenced to produce a concrete ref.
            let host_mesh_ref: HostMeshRef = host_mesh.clone();
            // Here, the underlying host mesh does not change:
            assert_eq!(
                host_mesh_ref.iter().collect::<Vec<_>>(),
                host_mesh.iter().collect::<Vec<_>>(),
            );

            // Validate we can cast:
            for actor_mesh in [&actor_mesh1, &actor_mesh2] {
                let (port, mut rx) = instance.mailbox().open_port();
                actor_mesh
                    .cast(instance, testactor::GetActorId(port.bind()))
                    .unwrap();

                let mut expected_actor_ids: HashSet<_> = actor_mesh
                    .values()
                    .map(|actor_ref| actor_ref.actor_id().clone())
                    .collect();

                while !expected_actor_ids.is_empty() {
                    let actor_id = rx.recv().await.unwrap();
                    assert!(
                        expected_actor_ids.remove(&actor_id),
                        "got {actor_id}, expect {expected_actor_ids:?}"
                    );
                }
            }

            // Now forward a message through all directed edges across the two meshes.
            // This tests the full connectivity of all the hosts, procs, and actors
            // involved in these two meshes.
            let mut to_visit: VecDeque<_> = actor_mesh1
                .values()
                .chain(actor_mesh2.values())
                .map(|actor_ref| actor_ref.port())
                // Each ordered pair of ports
                .permutations(2)
                // Flatten them to create a path:
                .flatten()
                .collect();

            let expect_visited: Vec<_> = to_visit.clone().into();

            // We are going to send to the first, and then set up a port to receive the last.
            let (last, mut last_rx) = instance.mailbox().open_port();
            to_visit.push_back(last.bind());

            let forward = testactor::Forward {
                to_visit,
                visited: Vec::new(),
            };
            let first = forward.to_visit.front().unwrap().clone();
            first.send(instance, forward).unwrap();

            let forward = last_rx.recv().await.unwrap();
            assert_eq!(forward.visited, expect_visited);

            let _ = host_mesh.shutdown(&instance).await;
        }
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

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_extrinsic_allocation() {
        let config = hyperactor::config::global::lock();
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
            };
            boot.to_env(&mut cmd);
            cmd.kill_on_drop(true);
            children.push(cmd.spawn().unwrap());
        }

        let instance = testing::instance().await;
        let host_mesh = HostMeshRef::from_hosts(Name::new("test"), hosts);

        let proc_mesh = host_mesh
            .spawn(&testing::instance().await, "test", Extent::unity())
            .await
            .unwrap();

        let actor_mesh: ActorMesh<testactor::TestActor> = proc_mesh
            .spawn(&testing::instance().await, "test", &())
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
    async fn test_failing_proc_allocation() {
        let lock = hyperactor::config::global::lock();
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
            };
            boot.to_env(&mut cmd);
            cmd.kill_on_drop(true);
            children.push(cmd.spawn().unwrap());
        }
        let host_mesh = HostMeshRef::from_hosts(Name::new("test"), hosts);

        let instance = testing::instance().await;

        let err = host_mesh
            .spawn(&instance, "test", Extent::unity())
            .await
            .unwrap_err();
        assert_matches!(
            err, v1::Error::ProcCreationError { state: resource::State { status: resource::Status::Failed(msg), ..}, .. }
            if msg.contains("failed to configure process: Terminal(Stopped { exit_code: 1")
        );
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_halting_proc_allocation() {
        let config = config::global::lock();
        let _guard1 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(5));

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
            };
            boot.to_env(&mut cmd);
            cmd.kill_on_drop(true);
            children.push(cmd.spawn().unwrap());
        }
        let host_mesh = HostMeshRef::from_hosts(Name::new("test"), hosts);

        let instance = testing::instance().await;

        let err = host_mesh
            .spawn(&instance, "test", Extent::unity())
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
        let config = hyperactor::config::global::lock();
        let _guard1 = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);
        let _guard2 =
            config.override_key(config::HOST_SPAWN_READY_TIMEOUT, Duration::from_secs(120));
        let _guard3 =
            config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(60));

        let instance = testing::instance().await;

        let proc_meshes = testing::proc_meshes(instance, extent!(replicas = 2)).await;
        let proc_mesh = proc_meshes.get(1).unwrap();

        let actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(instance, "test", &()).await.unwrap();

        let mut attrs_override = Attrs::new();
        attrs_override.set(config::HOST_SPAWN_READY_TIMEOUT, Duration::from_secs(180));
        actor_mesh
            .cast(
                instance,
                SetConfigAttrs(bincode::serialize(&attrs_override).unwrap()),
            )
            .unwrap();

        let (tx, mut rx) = instance.open_port();
        actor_mesh
            .cast(instance, GetConfigAttrs(tx.bind()))
            .unwrap();
        let actual_attrs = rx.recv().await.unwrap();
        let actual_attrs = bincode::deserialize::<Attrs>(&actual_attrs).unwrap();

        assert_eq!(
            *actual_attrs.get(config::HOST_SPAWN_READY_TIMEOUT).unwrap(),
            Duration::from_secs(180)
        );
        assert_eq!(
            *actual_attrs.get(config::MESSAGE_DELIVERY_TIMEOUT).unwrap(),
            Duration::from_secs(60)
        );
    }
}
