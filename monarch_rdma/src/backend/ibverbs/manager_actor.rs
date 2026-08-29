/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # Ibverbs Manager
//!
//! Contains ibverbs-specific RDMA logic.
//!
//! Manages ibverbs resources including:
//! - Memory registration (CPU and CUDA via dmabuf or segment scanning)
//! - Queue pair creation and connection establishment
//! - RDMA domain and protection domain management
//! - Device selection and PCI-to-RDMA device mapping

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::OncePortRef;
use hyperactor::PortHandle;
use hyperactor::actor::Referable;
use rand::seq::IteratorRandom;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use super::IbvOp;
use super::cq_actor::CompletionQueueActor;
use super::device::IbvDevice;
use super::device::IbvDeviceImpl;
use super::device_selection::PeerDeviceAffinityPolicy;
use super::device_selection::configured_peer_device_affinity;
use super::device_selection::resolve_target;
use super::device_selection::select_optimal_ibv_devices;
use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::efa_device::EfaDevice;
use super::memory_region::IbvMemoryRegionView;
use super::memory_region::IbvRemoteMemoryRegionView;
use super::mlx_device::MlxDevice;
use super::primitives::IbvConfig;
use super::primitives::IbvCq;
use super::primitives::IbvQpInfo;
use super::primitives::ibverbs_supported;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::OpResult;
use super::queue_pair::ProcessOps;
use super::queue_pair::QpKey;
use super::queue_pair::QueuePairActor;
use super::queue_pair::QueuePairOp;
use super::queue_pair::legacy;
use crate::RdmaOp;
use crate::RdmaTransportLevel;
use crate::backend::RdmaBackend;
use crate::backend::RdmaConfig;
use crate::backend::ResolveRemoteBackendContext;
use crate::local_memory::KeepaliveLocalMemory;
use crate::rdma_components::RdmaRemoteBuffer;
use crate::rdma_manager_actor::RdmaManagerActor;
use crate::validate_execution_context;

/// Cross-proc message: the active side asks the peer's manager to
/// create and connect a mirror QP for an in-flight [`QueuePairActor`].
/// Generic over the manager actor type so test code can swap in a
/// mock.
#[derive(Debug, Serialize, Deserialize, Named)]
#[serde(bound(serialize = "", deserialize = ""))]
pub(super) struct CreatePeerQueuePair<M: Referable> {
    /// The active side's manager.
    pub(super) sender: ActorRef<M>,
    /// Device the active side picked for its QP.
    pub(super) sender_device: String,
    /// Device the peer should create its mirror QP on.
    pub(super) receiver_device: String,
    /// Active side's endpoint, captured right after QP creation.
    pub(super) sender_info: IbvQpInfo,
    /// One-shot reply carrying the peer's endpoint, or an error.
    pub(super) reply: OncePortRef<Result<IbvQpInfo, String>>,
}
wirevalue::register_type!(CreatePeerQueuePair<IbvManagerActor<MlxDevice>>);
wirevalue::register_type!(CreatePeerQueuePair<IbvManagerActor<EfaDevice>>);

/// Local-only message: submit a batch of RDMA ops for end-to-end
/// execution. The manager iterates the batch, resolves each op's
/// local MRs via [`IbvManagerActor::resolve_local_mrs`], settles on the
/// NIC pair to run it over with
/// [`IbvManagerActor::pick_peer_pair`], looks up (or spawns) the
/// active-side [`QueuePairActor`] for the op's [`QpKey`], and
/// immediately dispatches a one-item [`ProcessOps`] to that QP — so
/// the QP can start posting op `i` while the manager resolves the MRs
/// for op `i+1`.
///
/// Per-op completion notifications stream back on `reply` as
/// [`OpResult`] values.
pub(super) struct SubmitOps<I: IbvDeviceImpl> {
    pub(super) ops: Vec<IbvOp<IbvManagerActor<I>>>,
    pub(super) reply: PortHandle<OpResult>,
}

/// Local-only message: create a fresh, unconnected legacy
/// [`legacy::IbvQueuePair`] on `self_device` and return it. The caller drives
/// the connection itself — exchange [`IbvQpInfo`] with the other endpoint's QP
/// and call `connect` on each side. Lets doorbell tests and the
/// `cuda_ping_pong` example poke a real QP without going through
/// [`QueuePairActor`]; both want the legacy queue pair for its direct
/// device-doorbell data path, independent of the backend's production
/// [`IbvDomainImpl::QueuePair`].
pub struct RawQueuePair {
    pub self_device: String,
    pub reply: OncePortHandle<Result<legacy::IbvQueuePair, String>>,
}

/// Local-only messages for [`IbvManagerActor`].
#[derive(Handler, HandleClient, Debug)]
pub enum IbvManagerLocalMessage {
    /// Register `local`'s MRs and reply with one
    /// [`IbvRemoteMemoryRegionView`] per registration. Called by
    /// [`crate::rdma_manager_actor::RdmaManagerActor::request_buffer`]
    /// at buffer-creation time.
    ///
    /// The registrations live on `local` itself, so they stay in force for as
    /// long as any holder of that handle does.
    RegisterRemoteBuffer {
        local: KeepaliveLocalMemory,
        #[reply]
        reply: OncePortHandle<Result<Vec<IbvRemoteMemoryRegionView>, String>>,
    },
}

/// Default key used for the per-device protection domain inside
/// each [`IbvDevice<I>`] entry of [`IbvManagerActor::devices`].
const DEFAULT_DOMAIN: &str = "default";

/// Manages all ibverbs-specific RDMA resources and operations.
///
/// This struct handles memory registration, queue pair management,
/// and connection establishment using the ibverbs API.
///
/// Generic over `I: IbvDeviceImpl` so the same actor implementation
/// drives every concrete backend (`IbvManagerActor<MlxDevice>`,
/// `IbvManagerActor<EfaDevice>`, ...).
#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        CreatePeerQueuePair<IbvManagerActor<I>>,
    ],
)]
pub struct IbvManagerActor<I: IbvDeviceImpl> {
    owner: OnceLock<ActorHandle<RdmaManagerActor>>,

    /// Active-side [`QueuePairActor`] children, keyed from this
    /// manager's perspective. Lazily populated on the first
    /// [`SubmitOps`] that targets a new `(self_device, peer,
    /// other_device)` triple.
    qp_handles: HashMap<
        QpKey,
        ActorHandle<QueuePairActor<IbvManagerActor<I>, <I::Domain as IbvDomainImpl>::QueuePair>>,
    >,

    /// Passive-side mirror QPs, created in response to a peer's
    /// [`CreatePeerQueuePair`]. The peer's [`QueuePairActor`] owns
    /// the active side; we hold the connected mirror here so the
    /// peer can read/write our memory. The map's `Drop` destroys
    /// each QP via its own `Drop`.
    peer_created_qps: HashMap<QpKey, <I::Domain as IbvDomainImpl>::QueuePair>,

    /// Map of RDMA device names to their opened [`IbvDevice<I>`], each of
    /// which owns the per-device `Arc<IbvContext>` and the `DEFAULT_DOMAIN`
    /// `Arc<IbvDomain>`.
    devices: HashMap<String, IbvDevice<I>>,

    /// Which of a peer's NICs each of this manager's NICs may pair with, from
    /// [`RDMA_PEER_DEVICE_AFFINITY`](crate::config::RDMA_PEER_DEVICE_AFFINITY).
    /// Read once, when the manager starts.
    peer_device_affinity: PeerDeviceAffinityPolicy,

    /// The actors responsible for polling completion queues. When
    /// [`crate::config::RDMA_CQ_POLLER_PER_DEVICE`] is true (default), there
    /// is a one-to-one mapping from CQ actors to NICs: one CQ actor polls
    /// all of the CQs for a specific NIC and no others. When the config value
    /// is false, there is one CQ actor responsible for polling all CQs for
    /// all NICs, and this map is keyed by the empty string only.
    cq_pollers: HashMap<String, ActorHandle<CompletionQueueActor<IbvCq>>>,

    config: IbvConfig,
}

#[async_trait]
impl<I: IbvDeviceImpl> Actor for IbvManagerActor<I> {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        let owner = if let Some(owner) = this.parent_handle() {
            owner
        } else {
            anyhow::bail!("RdmaManagerActor not found as parent of IbvManagerActor");
        };
        self.owner
            .set(owner)
            .expect("owner should only be set once during init");
        Ok(())
    }

    // This actor is implemented in Rust, but the RDMA registration path may enter
    // Python and take the GIL. Run its loop on the dedicated rdma runtime rather
    // than the shared control-plane runtime; see `crate::rdma_runtime`.
    fn spawn_server_task<F>(future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        crate::rdma_runtime::spawn_on_rdma_runtime(future)
    }
}

impl<I: IbvDeviceImpl> Drop for IbvManagerActor<I> {
    fn drop(&mut self) {
        // Drain active-side QP actors. Each child owns its
        // `IbvQueuePair`; `drain_and_stop` schedules the actor to
        // finish in-flight ops and exit, dropping the QP via its
        // own `Drop`.
        for (_key, handle) in self.qp_handles.drain() {
            let _ = handle.drain_and_stop("IbvManagerActor dropped");
        }

        for (_key, poller) in self.cq_pollers.drain() {
            let _ = poller.drain_and_stop("IbvManagerActor dropped");
        }

        // The remaining fields (`peer_created_qps`, `devices`) free their FFI
        // resources through their elements' `Drop`s when this struct is dropped.
    }
}

impl<I: IbvDeviceImpl> IbvManagerActor<I> {
    /// Create a new IbvManagerActor with the given configuration.
    pub async fn new(params: Option<IbvConfig>) -> Result<Self, anyhow::Error> {
        if !ibverbs_supported() {
            return Err(anyhow::anyhow!(
                "Cannot create IbvManagerActor because RDMA is not supported on this machine"
            ));
        }

        // Use the caller's config; when none is given, start from the
        // defaults and let the backend seed its own.
        let mut config = match params {
            Some(config) => config,
            None => {
                let mut config = IbvConfig::default();
                I::apply_config_defaults(&mut config);
                config
            }
        };

        // Preserve an explicit per-manager target. Otherwise parse the process
        // default from `RDMA_IBVERBS_TARGET`; empty preserves automatic selection.
        if config.target.is_none() {
            config.target = super::device_selection::configured_ibverbs_target()?;
        }
        tracing::debug!("rdma is enabled, config target: {:?}", config.target);

        // check config and hardware support align
        if config.use_gpu_direct {
            match validate_execution_context().await {
                Ok(_) => {
                    tracing::info!("GPU Direct RDMA execution context validated successfully");
                }
                Err(e) => {
                    tracing::warn!(
                        "GPU Direct RDMA execution context validation failed: {}. Downgrading to standard ibverbs mode.",
                        e
                    );
                    config.use_gpu_direct = false;
                }
            }
        }

        let actor = Self {
            owner: OnceLock::new(),
            qp_handles: HashMap::new(),
            peer_created_qps: HashMap::new(),
            devices: HashMap::new(),
            peer_device_affinity: configured_peer_device_affinity()?,
            cq_pollers: HashMap::new(),
            config,
        };

        Ok(actor)
    }

    /// Get or create the `DEFAULT_DOMAIN` for the named RDMA device, opening
    /// the device on first use.
    fn get_or_create_device_domain(
        &mut self,
        device_name: &str,
    ) -> Result<&IbvDomain<I::Domain>, anyhow::Error> {
        self.get_or_create_device(device_name)?
            .get_or_create_domain(DEFAULT_DOMAIN)
    }

    /// Get the named RDMA device, opening it on first use.
    fn get_or_create_device(
        &mut self,
        device_name: &str,
    ) -> Result<&mut IbvDevice<I>, anyhow::Error> {
        if !self.devices.contains_key(device_name) {
            let device = IbvDevice::<I>::try_open(device_name, self.config.clone())?;
            // Print device info if MONARCH_DEBUG_RDMA=1 is set.
            crate::print_device_info_if_debug_enabled(device.context().as_ptr());
            self.devices.insert(device_name.to_string(), device);
        }
        Ok(self
            .devices
            .get_mut(device_name)
            .expect("device just inserted or already present"))
    }

    /// Chooses a set of NICs on which to register `mem`, then registers
    /// `mem` on those NICs (caching the registrations inside the handle)
    /// and returns the relevant [`IbvMemoryRegionView`]s. The call fails
    /// only when there are no successful registrations.
    ///
    /// If an explicit `config.target` is set, then that is the NIC that is
    /// chosen. Otherwise, NICs are chosen based on `pick_optimal_devices`.
    ///
    /// A region already registered on this backend keeps the NICs it has: the
    /// set is chosen once, on the first call.
    fn resolve_local_mrs(
        &mut self,
        mem: &KeepaliveLocalMemory,
    ) -> Result<Vec<IbvMemoryRegionView>, anyhow::Error> {
        let already_serving = mem.registered_mrs::<I>();
        if !already_serving.is_empty() {
            return Ok(already_serving);
        }

        let device_names = match &self.config.target {
            Some(target) => vec![
                resolve_target::<I>(target)?
                    .ok_or_else(|| {
                        anyhow::anyhow!("configured device target {:?} not found", target)
                    })?
                    .name()
                    .clone(),
            ],
            None => self.pick_optimal_devices(mem)?,
        };

        let mut views = Vec::with_capacity(device_names.len());
        let mut failure: Option<anyhow::Error> = None;
        for device_name in &device_names {
            match self.resolve_local_mr_on(mem, device_name) {
                Ok(view) => views.push(view),
                Err(error) => {
                    tracing::warn!(
                        "not serving [{:#x}, {:#x}) from {device_name}: {error:#}",
                        mem.addr(),
                        mem.addr() + mem.size(),
                    );
                    failure.get_or_insert(error);
                }
            }
        }
        if views.is_empty() {
            return Err(failure.expect("`device_names` is never empty, so one of them failed"));
        }
        Ok(views)
    }

    /// Resolve `mem` to its [`IbvMemoryRegionView`] on `device_name`,
    /// registering the region there on first use.
    ///
    /// The registration lives on `mem` itself, shared by every clone of that
    /// handle, so one handle registers a given device at most once. Nothing
    /// deduplicates separate handles over the same region, though: each
    /// carries its own registrations, so a region covered by two handles is
    /// registered once per handle. A failure is recorded on `mem` too and
    /// returned to later callers rather than retried.
    fn resolve_local_mr_on(
        &mut self,
        mem: &KeepaliveLocalMemory,
        device_name: &str,
    ) -> Result<IbvMemoryRegionView, anyhow::Error> {
        if let Some(mrv) = mem.registered_mr::<I>(device_name)? {
            return Ok(mrv);
        }
        tracing::debug!(
            "Using RDMA device: {} for memory at 0x{:x}",
            device_name,
            mem.addr()
        );

        // The backend strategy handles host vs. device memory (standard MR,
        // dmabuf MR, or a device-specific segment binding).
        let registered = self
            .get_or_create_device_domain(device_name)
            .and_then(|domain| domain.register_mr(mem));
        match registered {
            Ok(mrv) => mem.install_mr::<I>(mrv),
            Err(error) => {
                mem.record_mr_failure::<I>(device_name, &error);
                Err(error)
            }
        }
    }

    /// The NICs of backend `I` to serve `mem` from: up to
    /// [`RDMA_MAX_NICS_PER_BUFFER`](crate::config::RDMA_MAX_NICS_PER_BUFFER)
    /// of the optimal NICs returned by [`select_optimal_ibv_devices`], chosen
    /// using [`PeerDeviceAffinityPolicy::choose`].
    fn pick_optimal_devices(
        &self,
        mem: &KeepaliveLocalMemory,
    ) -> Result<Vec<String>, anyhow::Error> {
        let location = mem.location();
        let devices = select_optimal_ibv_devices::<I>(location)?;
        anyhow::ensure!(
            !devices.is_empty(),
            "no {} RDMA device has a path to {location:?}",
            I::backend_name(),
        );
        let names: Vec<String> = devices.iter().map(|device| device.name().clone()).collect();
        let max = hyperactor_config::global::get(crate::config::RDMA_MAX_NICS_PER_BUFFER);
        // `names` is passed in the order returned by `select_optimal_ibv_devices`, which is
        // lexicographic. `PeerDeviceAffinityPolicy::choose` is sensitive to input order,
        // so ensuring `names` has the same order across procs is important for consistency.
        Ok(self
            .peer_device_affinity
            .choose(&names, max.map(hyperactor_config::NonZeroUsize::into_std)))
    }

    /// Given a list of local registrations and remote registrations, uses
    /// [`Self::peer_device_affinity`] to decide on one local/remote pair
    /// to use. Errors when the policy gives no valid pair.
    fn pick_peer_pair<'a>(
        &self,
        local: &'a [IbvMemoryRegionView],
        remote: &'a [IbvRemoteMemoryRegionView],
    ) -> Result<(&'a IbvMemoryRegionView, &'a IbvRemoteMemoryRegionView), anyhow::Error> {
        // Sort both sides by device name because `PeerDeviceAffinityPolicy::pairs` is
        // sensitive to input order. This ensures that `pick_peer_pair`'s behavior is
        // dependent only on the unordered set of local and remote devices, and it is
        // therefore consistent across processes.
        let mut local: Vec<&IbvMemoryRegionView> = local.iter().collect();
        local.sort_by(|a, b| a.device_name.cmp(&b.device_name));
        let mut remote: Vec<&IbvRemoteMemoryRegionView> = remote.iter().collect();
        remote.sort_by(|a, b| a.device_name.cmp(&b.device_name));

        let local_names: Vec<String> = local.iter().map(|mr| mr.device_name.clone()).collect();
        let remote_names: Vec<String> = remote.iter().map(|mr| mr.device_name.clone()).collect();
        self.peer_device_affinity
            .pairs(&local_names, &remote_names)
            .into_iter()
            .enumerate()
            .filter_map(|(i, peer)| peer.map(|j| (local[i], remote[j])))
            .choose(&mut rand::rng())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no NIC of {local_names:?} pairs with a peer NIC of {remote_names:?} under \
                     {:?}",
                    self.peer_device_affinity,
                )
            })
    }

    /// Build a passive-side mirror QP for `qp_key`, connect it to
    /// `sender_info`, and store it in [`Self::peer_created_qps`].
    /// Returns the local endpoint the active side needs to finish
    /// its own `connect`. Called from
    /// [`Handler<CreatePeerQueuePair>`].
    fn create_peer_qp(
        &mut self,
        qp_key: &QpKey,
        sender_info: &IbvQpInfo,
    ) -> Result<IbvQpInfo, anyhow::Error> {
        if self.peer_created_qps.contains_key(qp_key) {
            anyhow::bail!("peer queue pair already exists for {qp_key:?}");
        }
        let self_device = qp_key.self_device.clone();
        let config = self.config.clone();
        let mut qp = self
            .get_or_create_device(&self_device)?
            .create_non_posting_queue_pair(DEFAULT_DOMAIN, &config)
            .map_err(|e| anyhow::anyhow!("could not create peer IbvQueuePair: {}", e))?;
        let local_info = qp
            .get_qp_info()
            .map_err(|e| anyhow::anyhow!("could not extract peer QP info: {}", e))?;
        qp.connect(sender_info)
            .map_err(|e| anyhow::anyhow!("could not connect peer QP: {}", e))?;
        self.peer_created_qps.insert(qp_key.clone(), qp);
        Ok(local_info)
    }

    /// The poller that serves `device`, spawned on first request.
    /// One per device, or one for all of them; see
    /// [`crate::config::RDMA_CQ_POLLER_PER_DEVICE`].
    fn ensure_cq_poller(
        &mut self,
        cx: &Context<'_, Self>,
        device: &str,
    ) -> ActorHandle<CompletionQueueActor<IbvCq>> {
        let key = if hyperactor_config::global::get(crate::config::RDMA_CQ_POLLER_PER_DEVICE) {
            device.to_string()
        } else {
            String::new()
        };
        self.cq_pollers
            .entry(key)
            .or_insert_with(|| cx.spawn(CompletionQueueActor::new()))
            .clone()
    }

    /// Lazy active-side QP actor: if `qp_key` is absent from
    /// [`Self::qp_handles`], create an [`IbvQueuePair`] on the
    /// requested device and spawn a [`QueuePairActor`] to drive its
    /// handshake + data path. Returns a clone of the actor handle.
    fn ensure_qp_actor(
        &mut self,
        cx: &Context<'_, Self>,
        qp_key: &QpKey,
        peer_manager: ActorRef<Self>,
    ) -> Result<
        ActorHandle<QueuePairActor<Self, <I::Domain as IbvDomainImpl>::QueuePair>>,
        anyhow::Error,
    > {
        if let Some(h) = self.qp_handles.get(qp_key) {
            return Ok(h.clone());
        }
        let self_device = qp_key.self_device.clone();
        let config = self.config.clone();
        let cq_poller = self.ensure_cq_poller(cx, &self_device);
        let (qp, cq_lease) = self
            .get_or_create_device(&self_device)?
            .create_queue_pair(DEFAULT_DOMAIN, &config)
            .map_err(|e| anyhow::anyhow!("could not create IbvQueuePair for {qp_key:?}: {}", e))?;
        let local_manager: ActorRef<Self> = cx.bind();
        let is_loopback = local_manager.actor_addr() == peer_manager.actor_addr()
            && qp_key.self_device == qp_key.other_device;
        let actor = cx.spawn(QueuePairActor::new(
            qp_key.clone(),
            local_manager,
            peer_manager,
            qp,
            cq_poller,
            cq_lease,
            is_loopback,
            config.max_send_wr,
        ));
        self.qp_handles.insert(qp_key.clone(), actor.clone());
        Ok(actor)
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> Handler<SubmitOps<I>> for IbvManagerActor<I> {
    async fn handle(&mut self, cx: &Context<Self>, msg: SubmitOps<I>) -> Result<(), anyhow::Error> {
        let SubmitOps { ops, reply } = msg;

        // Interleave MR resolution with QP dispatch: as soon as op `i`'s
        // local MRs are resolved and its QP actor is in place, ship a
        // one-item `ProcessOps` to that QP. The QP can then post and
        // poll op `i` while we run `resolve_local_mrs` for op `i+1`.
        for (i, op) in ops.into_iter().enumerate() {
            let local_mrs = match self.resolve_local_mrs(&op.local_memory) {
                Ok(mrs) => mrs,
                Err(e) => {
                    reply.try_post(
                        cx,
                        OpResult {
                            op_idx: i,
                            result: Err(e.to_string()),
                        },
                    )?;
                    continue;
                }
            };
            let (local, remote) = match self.pick_peer_pair(&local_mrs, &op.remote_buffers) {
                Ok((local, remote)) => (local.clone(), remote.clone()),
                Err(e) => {
                    reply.try_post(
                        cx,
                        OpResult {
                            op_idx: i,
                            result: Err(e.to_string()),
                        },
                    )?;
                    continue;
                }
            };
            let qp_key = QpKey {
                self_device: local.device_name.clone(),
                other_id: op.remote_manager.actor_addr().id().clone(),
                other_device: remote.device_name.clone(),
            };
            let handle = match self.ensure_qp_actor(cx, &qp_key, op.remote_manager) {
                Ok(h) => h,
                Err(e) => {
                    reply.try_post(
                        cx,
                        OpResult {
                            op_idx: i,
                            result: Err(e.to_string()),
                        },
                    )?;
                    continue;
                }
            };
            handle.try_post(
                cx,
                ProcessOps {
                    items: vec![QueuePairOp {
                        op_idx: i,
                        op_type: op.op_type,
                        local_memory: op.local_memory,
                        local,
                        remote,
                    }],
                    reply: reply.clone(),
                },
            )?;
        }
        Ok(())
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> Handler<RawQueuePair> for IbvManagerActor<I> {
    async fn handle(&mut self, cx: &Context<Self>, msg: RawQueuePair) -> Result<(), anyhow::Error> {
        let RawQueuePair { self_device, reply } = msg;
        // Build a fresh, unconnected legacy QP on `self_device` and hand it
        // back; the caller exchanges endpoint info and connects it.
        let config = self.config.clone();
        let result = self
            .get_or_create_device_domain(&self_device)
            .and_then(|domain| legacy::IbvQueuePair::new(domain, config))
            .map_err(|e| e.to_string());
        let _ = reply.try_post(cx, result);
        Ok(())
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> Handler<CreatePeerQueuePair<IbvManagerActor<I>>> for IbvManagerActor<I> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: CreatePeerQueuePair<IbvManagerActor<I>>,
    ) -> Result<(), anyhow::Error> {
        let CreatePeerQueuePair {
            sender,
            sender_device,
            receiver_device,
            sender_info,
            reply,
        } = msg;
        let qp_key = QpKey {
            self_device: receiver_device,
            other_id: sender.actor_addr().id().clone(),
            other_device: sender_device,
        };
        match self.create_peer_qp(&qp_key, &sender_info) {
            Ok(local_info) => reply.post(cx, Ok(local_info)),
            Err(e) => reply.post(cx, Err(e.to_string())),
        }
        Ok(())
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> IbvManagerLocalMessageHandler for IbvManagerActor<I> {
    async fn register_remote_buffer(
        &mut self,
        _cx: &Context<Self>,
        local: KeepaliveLocalMemory,
    ) -> Result<Result<Vec<IbvRemoteMemoryRegionView>, String>, anyhow::Error> {
        // The registration is installed on `local`'s shared map, so every clone
        // of this handle — including the one the caller holds — reuses it rather
        // than registering the same region on that device again.
        Ok(self
            .resolve_local_mrs(&local)
            .map(|mrs| mrs.iter().map(IbvRemoteMemoryRegionView::from).collect())
            .map_err(|error| format!("{error:#}")))
    }
}

// `#[hyperactor::handle(IbvManagerLocalMessage)]` would generate a non-generic
// `impl Handler<...> for IbvManagerActor<I>` that can't see `I`; we write the
// generic delegation by hand.
#[async_trait]
impl<I: IbvDeviceImpl> Handler<IbvManagerLocalMessage> for IbvManagerActor<I> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: IbvManagerLocalMessage,
    ) -> Result<(), anyhow::Error> {
        <Self as IbvManagerLocalMessageHandler>::handle(self, cx, message).await
    }
}

/// Wrapper around [`ActorHandle<IbvManagerActor<I>>`] that moves the RDMA
/// data-plane (post send/recv, poll CQ) off the actor loop while keeping
/// state-mutating operations (MR registration/deregistration, QP management)
/// serialized through actor messages.
#[derive(Debug)]
pub struct IbvBackend<I: IbvDeviceImpl>(pub ActorHandle<IbvManagerActor<I>>);

impl<I: IbvDeviceImpl> Clone for IbvBackend<I> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<I: IbvDeviceImpl> std::ops::Deref for IbvBackend<I> {
    type Target = ActorHandle<IbvManagerActor<I>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Serializable per-buffer context for an ibverbs backend: the manager to route
/// ops through and the wire description of every MR the buffer is registered
/// under, one per NIC serving it.
#[derive(Serialize, Deserialize, Named)]
#[serde(bound = "")]
pub struct IbvRemoteBackendContext<I: IbvDeviceImpl> {
    pub manager: ActorRef<IbvManagerActor<I>>,
    pub buffers: Vec<IbvRemoteMemoryRegionView>,
}

// `Clone` and `Debug` are hand-rolled to avoid the spurious `I: Clone`
// and `I: Debug` bounds the derives would impose; neither field depends
// on `I` implementing them.
impl<I: IbvDeviceImpl> Clone for IbvRemoteBackendContext<I> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
            buffers: self.buffers.clone(),
        }
    }
}

impl<I: IbvDeviceImpl> std::fmt::Debug for IbvRemoteBackendContext<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbvRemoteBackendContext")
            .field("manager", &self.manager)
            .field("buffers", &self.buffers)
            .finish()
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> RdmaBackend for IbvBackend<I>
where
    RdmaRemoteBuffer: ResolveRemoteBackendContext<IbvBackend<I>>,
{
    type RemoteBackendContext = IbvRemoteBackendContext<I>;
    type TransportInfo = ();

    fn available() -> bool {
        if IbvDevice::<I>::available() {
            if hyperactor_config::global::get(crate::config::RDMA_DISABLE_IBVERBS) {
                tracing::warn!(
                    "ibverbs ({}) is available, but it was disabled by configuration (RDMA_DISABLE_IBVERBS=true)",
                    I::backend_name()
                );
                return false;
            }
            return true;
        }
        false
    }

    fn transport_level(&self) -> RdmaTransportLevel {
        RdmaTransportLevel::Nic
    }

    fn transport_info(&self) -> Option<Self::TransportInfo> {
        None
    }

    async fn spawn(
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        config: &RdmaConfig,
    ) -> Result<Self> {
        let actor = IbvManagerActor::<I>::new(config.ibv.clone()).await?;
        Ok(IbvBackend(cx.spawn(actor)))
    }

    async fn register_remote_buffer(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        _remote_buf_id: usize,
        local: KeepaliveLocalMemory,
    ) -> Result<IbvRemoteBackendContext<I>> {
        let buffers = self
            .0
            .register_remote_buffer(cx, local)
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(IbvRemoteBackendContext {
            manager: self.0.bind(),
            buffers,
        })
    }

    /// No-op: this backend holds no per-buffer state. A region's registrations
    /// live on the [`KeepaliveLocalMemory`] they were made for, so a buffer is
    /// released by its owner dropping that handle.
    async fn release_buffer(
        &self,
        _cx: &(impl hyperactor::context::Actor + Send + Sync),
        _remote_buf_id: usize,
    ) -> Result<()> {
        Ok(())
    }

    /// Submit a batch of RDMA operations.
    ///
    /// Translates each op to an `IbvOp`, then ships the whole batch to
    /// [`IbvManagerActor`] via [`SubmitOps`]. The manager interleaves
    /// local-MR resolution with per-op dispatch: each op is sent to its
    /// [`QueuePairActor`] as a one-item [`ProcessOps`] the moment its MRs
    /// are ready, so QP work on op `i` overlaps MR registration for op
    /// `i+1`.
    ///
    /// Always waits for exactly `ops.len()` per-op replies before
    /// returning. Per-op failures are collected and formatted into a single
    /// multi-line `Err` listing each `op_idx` and its error message.
    async fn submit(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let mut ibv_ops = Vec::with_capacity(ops.len());
        for op in ops {
            let ctx = <RdmaRemoteBuffer as ResolveRemoteBackendContext<IbvBackend<I>>>::resolve(
                &op.remote,
            )
            .expect("op routed to incompatible backend");
            ibv_ops.push(IbvOp {
                op_type: op.op_type,
                local_memory: op.local.clone(),
                remote_buffers: ctx.buffers,
                remote_manager: ctx.manager,
            });
        }
        let n = ibv_ops.len();

        let (reply, mut reply_rx) = cx.mailbox().open_port::<OpResult>();

        self.0.try_post(
            cx,
            SubmitOps {
                ops: ibv_ops,
                reply,
            },
        )?;

        let mut failures: Vec<(usize, String)> = Vec::with_capacity(n);
        let mut received = 0usize;
        let mut terminal: Option<String> = None;
        let deadline = tokio::time::Instant::now() + timeout;
        while received < n {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => {
                    terminal = Some(format!(
                        "submit timed out after {received}/{n} replies with {} failures",
                        failures.len()
                    ));
                    break;
                }
                recv = reply_rx.recv() => {
                    match recv {
                        Ok(OpResult { result: Ok(()), .. }) => received += 1,
                        Ok(OpResult { op_idx, result: Err(e) }) => {
                            received += 1;
                            failures.push((op_idx, e));
                        }
                        Err(e) => {
                            terminal = Some(format!(
                                "SubmitOps reply port closed after {received}/{n} replies with {} failures: {e}",
                                failures.len()
                            ));
                            break;
                        }
                    }
                }
            }
        }

        if terminal.is_none() && failures.is_empty() {
            return Ok(());
        }

        failures.sort_by_key(|(idx, _)| *idx);
        let mut msg = terminal.unwrap_or_else(|| format!("{}/{n} ops failed", failures.len()));
        if !failures.is_empty() {
            msg.push(':');
            for (idx, err) in &failures {
                write!(msg, "\n  op {idx}: {err}").expect("infallible String write");
            }
        }
        Err(anyhow::anyhow!(msg))
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end coverage of the [`SubmitOps`] → [`ProcessOps`] →
    //! [`QueuePairActor`] data path.
    //!
    //! Each test stands up two RDMA participants in two
    //! [`Proc::direct`] procs in the test process. Each proc hosts an
    //! [`RdmaManagerActor`] and a [`BufferHelperActor`]. Tests
    //! allocate buffers on either side via the helpers, drive RDMA
    //! through [`IbvBackend::submit`] (called inside the helper
    //! actor), and verify by reading back local contents through
    //! [`BufferHelperMessage::ReadContents`]. The
    //! [`BufferHelperActor::cleanup`] impl releases any CUDA
    //! allocations when the actor stops; [`TestEnv::shutdown`]
    //! explicitly drains both procs.

    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::ActorEnvironment;
    use hyperactor::ActorRef;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::Instance;
    use hyperactor::Label;
    use hyperactor::OncePortRef;
    use hyperactor::Proc;
    use hyperactor::RefClient;
    use hyperactor::RemoteSpawn;
    use hyperactor::Uid;
    use hyperactor::actor::ActorError;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use serde::Deserialize;
    use serde::Serialize;
    use typeuri::Named;

    use crate::IbvConfig;
    use crate::RdmaManagerActor;
    use crate::RdmaManagerMessageClient;
    use crate::RdmaOp;
    use crate::RdmaOpType;
    use crate::RdmaRemoteBuffer;
    use crate::backend::RdmaBackendHandle;
    use crate::backend::cuda_test_utils::CudaAllocation;
    use crate::backend::cuda_test_utils::CudaAllocator;
    use crate::backend::ibverbs::device::list_all_devices;
    use crate::backend::ibverbs::device_selection::IbvDeviceTarget;
    use crate::backend::ibverbs::device_selection::resolve_target;
    use crate::backend::ibverbs::device_selection::select_optimal_ibv_devices;
    use crate::backend::ibverbs::mlx_device::MlxDevice;
    use crate::backend::ibverbs::primitives::IbvQpType;
    use crate::device_selection::MemoryLocation;
    use crate::local_memory::KeepaliveLocalMemory;

    // ====================================================================
    // BufferHelperActor
    // ====================================================================

    /// Device a test buffer is allocated on.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, Named)]
    pub enum BufferDevice {
        Cpu,
        Cuda(i32),
    }

    /// One op for [`BufferHelperMessage::Submit`]. The helper looks up
    /// the local memory behind `local_buf` (registered earlier via
    /// `Allocate`) and pairs it with `remote_buf` to form an
    /// [`RdmaOp`].
    #[derive(Debug, Clone, Serialize, Deserialize, Named)]
    pub struct BufferHelperOp {
        op_type: RdmaOpType,
        local_buf: RdmaRemoteBuffer,
        remote_buf: RdmaRemoteBuffer,
    }

    /// Test helper that owns local buffers (CPU or CUDA) and drives
    /// [`IbvBackend::submit`] against its own [`RdmaManagerActor`].
    #[hyperactor::export(handlers = [BufferHelperMessage])]
    #[hyperactor::spawnable]
    #[derive(Debug)]
    pub struct BufferHelperActor {
        rdma_manager: ActorRef<RdmaManagerActor>,
        /// CUDA allocations tracked for cleanup. Each is also held as
        /// `Keepalive` inside the registered `KeepaliveLocalMemory`;
        /// both clones must drop before the FFI memory is released.
        cuda_allocs: Vec<CudaAllocation>,
    }

    #[async_trait]
    impl Actor for BufferHelperActor {
        async fn cleanup(
            &mut self,
            _this: &Instance<Self>,
            _err: Option<&ActorError>,
        ) -> Result<(), anyhow::Error> {
            for alloc in self.cuda_allocs.drain(..) {
                alloc.try_free();
            }
            Ok(())
        }
    }

    #[async_trait]
    impl RemoteSpawn for BufferHelperActor {
        type Params = ActorRef<RdmaManagerActor>;

        async fn new(
            rdma_manager: ActorRef<RdmaManagerActor>,
            _env: &ActorEnvironment,
        ) -> Result<Self, anyhow::Error> {
            Ok(Self {
                rdma_manager,
                cuda_allocs: Vec::new(),
            })
        }
    }

    #[derive(Handler, RefClient, Named, Serialize, Deserialize, Debug)]
    pub enum BufferHelperMessage {
        /// Allocate `size` bytes on `device`, pre-fill with `pattern`,
        /// register with the local `RdmaManagerActor`, and reply with
        /// the resulting `RdmaRemoteBuffer`.
        Allocate {
            size: usize,
            device: BufferDevice,
            pattern: u8,
            #[reply]
            reply: OncePortRef<RdmaRemoteBuffer>,
        },
        /// Look up the local memory behind `remote.id` and reply with
        /// the byte range `[offset, offset + len)`. Tests use this to
        /// sample buffers too large to ship over a single actor
        /// message in one piece.
        ReadContents {
            remote: Box<RdmaRemoteBuffer>,
            offset: usize,
            len: usize,
            #[reply]
            reply: OncePortRef<Vec<u8>>,
        },
        /// Drive a batch of RDMA ops through `IbvBackend::submit`.
        /// Each op's `local_buf` is resolved against this helper's
        /// `RdmaManagerActor`; `remote_buf` is shipped as-is to the
        /// peer.
        Submit {
            ops: Vec<BufferHelperOp>,
            timeout_secs: u64,
            #[reply]
            reply: OncePortRef<Result<(), String>>,
        },
    }

    impl BufferHelperActor {
        async fn allocate_impl(
            &mut self,
            cx: &Context<'_, Self>,
            size: usize,
            device: BufferDevice,
            pattern: u8,
        ) -> Result<RdmaRemoteBuffer, anyhow::Error> {
            let local = match device {
                BufferDevice::Cpu => {
                    let buf: Box<[u8]> = vec![pattern; size].into_boxed_slice();
                    KeepaliveLocalMemory::try_new(Arc::new(buf))?
                }
                BufferDevice::Cuda(device_id) => {
                    let alloc = CudaAllocator::get().allocate(device_id, size, size);
                    let local = KeepaliveLocalMemory::try_new(Arc::new(alloc.clone()))?;
                    self.cuda_allocs.push(alloc);
                    let fill = vec![pattern; size];
                    // SAFETY: `local` is freshly constructed; no other
                    // holder touches this CUDA range yet.
                    unsafe { local.write_at(0, &fill) }?;
                    local
                }
            };
            let handle = self
                .rdma_manager
                .downcast_handle(cx)
                .ok_or_else(|| anyhow::anyhow!("rdma_manager not local to BufferHelperActor"))?;
            handle.request_buffer(cx, local).await
        }

        async fn read_contents_impl(
            &mut self,
            cx: &Context<'_, Self>,
            remote: RdmaRemoteBuffer,
            offset: usize,
            len: usize,
        ) -> Result<Vec<u8>, anyhow::Error> {
            let handle = self
                .rdma_manager
                .downcast_handle(cx)
                .ok_or_else(|| anyhow::anyhow!("rdma_manager not local"))?;
            let local = handle
                .request_local_memory(cx, remote.id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no local memory registered on this side for remote_buf_id={}",
                        remote.id,
                    )
                })?;
            let mut out = vec![0u8; len];
            // SAFETY: by convention the caller has ensured all RDMA
            // ops against this buffer have completed before invoking
            // ReadContents.
            unsafe { local.read_at(offset, &mut out)? };
            Ok(out)
        }

        async fn submit_impl(
            &mut self,
            cx: &Context<'_, Self>,
            ops: Vec<BufferHelperOp>,
            timeout_secs: u64,
        ) -> Result<Result<(), String>, anyhow::Error> {
            let handle = self
                .rdma_manager
                .downcast_handle(cx)
                .ok_or_else(|| anyhow::anyhow!("rdma_manager not local"))?;
            let mut rdma_ops = Vec::with_capacity(ops.len());
            for (i, op) in ops.into_iter().enumerate() {
                let local = handle
                    .request_local_memory(cx, op.local_buf.id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "op {i}: no local memory registered for remote_buf_id={}",
                            op.local_buf.id,
                        )
                    })?;
                rdma_ops.push(RdmaOp {
                    op_type: op.op_type,
                    local,
                    remote: op.remote_buf,
                });
            }
            let nic = RdmaManagerActor::local_handle(cx)
                .get_backend_handles(cx)
                .await?
                .into_iter()
                .find(|h| !matches!(h, RdmaBackendHandle::Tcp(_)))
                .ok_or_else(|| anyhow::anyhow!("no NIC backend on this proc"))?;
            let result = nic
                .submit(cx, rdma_ops, Duration::from_secs(timeout_secs))
                .await;
            Ok(result.map_err(|e| format!("{e}")))
        }
    }

    #[async_trait]
    #[hyperactor::handle(BufferHelperMessage)]
    impl BufferHelperMessageHandler for BufferHelperActor {
        async fn allocate(
            &mut self,
            cx: &Context<Self>,
            size: usize,
            device: BufferDevice,
            pattern: u8,
        ) -> Result<RdmaRemoteBuffer, anyhow::Error> {
            self.allocate_impl(cx, size, device, pattern).await
        }

        async fn read_contents(
            &mut self,
            cx: &Context<Self>,
            remote: Box<RdmaRemoteBuffer>,
            offset: usize,
            len: usize,
        ) -> Result<Vec<u8>, anyhow::Error> {
            self.read_contents_impl(cx, *remote, offset, len).await
        }

        async fn submit(
            &mut self,
            cx: &Context<Self>,
            ops: Vec<BufferHelperOp>,
            timeout_secs: u64,
        ) -> Result<Result<(), String>, anyhow::Error> {
            self.submit_impl(cx, ops, timeout_secs).await
        }
    }

    // ====================================================================
    // TestEnv
    // ====================================================================

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Two-sided test environment.
    ///
    /// Each side is a `Proc::direct` in the test process hosting its
    /// own `RdmaManagerActor` and `BufferHelperActor`. A client minted
    /// from `proc_b` drives both helpers through their `ActorRef`s.
    struct TestEnv {
        client: hyperactor::Client,
        proc_a: Proc,
        helper_a: ActorRef<BufferHelperActor>,
        proc_b: Proc,
        helper_b: ActorRef<BufferHelperActor>,
    }

    impl TestEnv {
        /// Asymmetric setup: side A uses `config_a`, side B uses `config_b`.
        async fn new(config_a: IbvConfig, config_b: IbvConfig) -> Result<Self, anyhow::Error> {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let proc_a = Proc::direct(
                ChannelAddr::any(ChannelTransport::Unix),
                format!("rdma_side_a_{id}"),
            )?;
            let helper_a = Self::spawn_side(&proc_a, config_a).await?;
            let proc_b = Proc::direct(
                ChannelAddr::any(ChannelTransport::Unix),
                format!("rdma_side_b_{id}"),
            )?;
            let helper_b = Self::spawn_side(&proc_b, config_b).await?;
            let client = proc_b.client("test_client");
            Ok(Self {
                client,
                proc_a,
                helper_a,
                proc_b,
                helper_b,
            })
        }

        /// Symmetric setup: both sides use `config`.
        async fn same_config(config: IbvConfig) -> Result<Self, anyhow::Error> {
            Self::new(config.clone(), config).await
        }

        /// Spawn an `RdmaManagerActor` + `BufferHelperActor` on `proc`
        /// and return the helper's `ActorRef`.
        async fn spawn_side(
            proc: &Proc,
            config: IbvConfig,
        ) -> Result<ActorRef<BufferHelperActor>, anyhow::Error> {
            let rdma_actor =
                RdmaManagerActor::new(Some(config), &ActorEnvironment::default()).await?;
            // Must match `RdmaManagerActor::local_handle`'s singleton lookup of "rdma_manager".
            let rdma_handle =
                proc.spawn_with_uid(Uid::singleton(Label::strip("rdma_manager")), rdma_actor)?;
            let rdma: ActorRef<RdmaManagerActor> = rdma_handle.bind();
            let helper_actor = BufferHelperActor::new(rdma, &ActorEnvironment::default()).await?;
            let helper_handle = proc.spawn_with_label("helper", helper_actor);
            Ok(helper_handle.bind())
        }

        async fn shutdown(mut self) -> Result<(), anyhow::Error> {
            let _ = self
                .proc_a
                .destroy_and_wait(Duration::from_secs(10), "TestEnv shutdown proc_a")
                .await?;
            let _ = self
                .proc_b
                .destroy_and_wait(Duration::from_secs(10), "TestEnv shutdown proc_b")
                .await?;
            Ok(())
        }
    }

    // ====================================================================
    // Shared test bodies
    // ====================================================================

    async fn assert_remote_pattern(
        helper: &ActorRef<BufferHelperActor>,
        cx: &hyperactor::Client,
        remote: RdmaRemoteBuffer,
        size: usize,
        pattern: u8,
    ) -> Result<(), anyhow::Error> {
        let got = helper.read_contents(cx, Box::new(remote), 0, size).await?;
        assert_eq!(got, vec![pattern; size]);
        Ok(())
    }

    /// Drive a single write from side A's buffer into side B's buffer
    /// and assert the destination now matches the pattern.
    async fn run_cross_actor_write(
        env: &TestEnv,
        src_dev: BufferDevice,
        dst_dev: BufferDevice,
        size: usize,
        pattern: u8,
        timeout_secs: u64,
    ) -> Result<(), anyhow::Error> {
        let src = env
            .helper_a
            .allocate(&env.client, size, src_dev, pattern)
            .await?;
        let dst = env.helper_b.allocate(&env.client, size, dst_dev, 0).await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src,
                    remote_buf: dst.clone(),
                }],
                timeout_secs,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, dst, size, pattern).await
    }

    /// Drive a single read from side B's buffer into side A's buffer
    /// and assert the destination now matches the pattern.
    async fn run_cross_actor_read(
        env: &TestEnv,
        dst_dev: BufferDevice,
        src_dev: BufferDevice,
        size: usize,
        pattern: u8,
        timeout_secs: u64,
    ) -> Result<(), anyhow::Error> {
        let dst = env.helper_a.allocate(&env.client, size, dst_dev, 0).await?;
        let src = env
            .helper_b
            .allocate(&env.client, size, src_dev, pattern)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local_buf: dst.clone(),
                    remote_buf: src,
                }],
                timeout_secs,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_a, &env.client, dst, size, pattern).await
    }

    /// Drive both a write and a read in a single
    /// `IbvBackend::submit` batch — both ops target the same peer
    /// QP and so resolve to a single `ProcessOps` group. After the
    /// batch completes, side B's `write_dst` and side A's `read_dst`
    /// both contain their respective patterns.
    async fn run_multi_op_same_qp(
        env: &TestEnv,
        dev_a: BufferDevice,
        dev_b: BufferDevice,
        size: usize,
        timeout_secs: u64,
    ) -> Result<(), anyhow::Error> {
        const WRITE_PATTERN: u8 = 0xa1;
        const READ_PATTERN: u8 = 0xb2;
        let write_src = env
            .helper_a
            .allocate(&env.client, size, dev_a, WRITE_PATTERN)
            .await?;
        let write_dst = env.helper_b.allocate(&env.client, size, dev_b, 0).await?;
        let read_dst = env.helper_a.allocate(&env.client, size, dev_a, 0).await?;
        let read_src = env
            .helper_b
            .allocate(&env.client, size, dev_b, READ_PATTERN)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: write_src,
                        remote_buf: write_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::ReadIntoLocal,
                        local_buf: read_dst.clone(),
                        remote_buf: read_src,
                    },
                ],
                timeout_secs,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, write_dst, size, WRITE_PATTERN).await?;
        assert_remote_pattern(&env.helper_a, &env.client, read_dst, size, READ_PATTERN).await
    }

    /// Drive a write + read between two buffers registered with the
    /// *same* `RdmaManagerActor` on the *same* device. Exercises the
    /// loopback path (`is_loopback = true`) where the active actor
    /// connects its QP to its own endpoint and skips the
    /// `CreatePeerQueuePair` round trip.
    async fn run_true_loopback(
        env: &TestEnv,
        dev: BufferDevice,
        size: usize,
    ) -> Result<(), anyhow::Error> {
        const PATTERN: u8 = 0x5d;
        let src = env
            .helper_a
            .allocate(&env.client, size, dev, PATTERN)
            .await?;
        let dst = env.helper_a.allocate(&env.client, size, dev, 0).await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src,
                    remote_buf: dst.clone(),
                }],
                5,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_a, &env.client, dst, size, PATTERN).await
    }

    // ====================================================================
    // Helpers
    // ====================================================================

    fn require_rdma() {
        if list_all_devices().is_empty() {
            panic!("SKIPPED: no RDMA devices available");
        }
    }

    fn require_cuda() {
        if !crate::is_cuda_available() {
            panic!("SKIPPED: CUDA not available");
        }
    }

    // ====================================================================
    // Tests
    // ====================================================================

    /// `register_remote_buffer` must record its registration on the
    /// `KeepaliveLocalMemory` it is handed — under the name of the device it
    /// registered on — so that later `resolve_local_mrs` calls for that device
    /// reuse it instead of registering the same region again.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_register_remote_buffer_records_its_registration() -> Result<(), anyhow::Error> {
        require_rdma();
        let target = IbvDeviceTarget::cpu(0);
        let device = resolve_target::<MlxDevice>(&target)?
            .expect("cpu:0 should resolve to a NIC")
            .name()
            .clone();
        let env = TestEnv::same_config(IbvConfig::targeting(target)).await?;
        let buf: Box<[u8]> = vec![0u8; 1024].into_boxed_slice();
        let local = KeepaliveLocalMemory::try_new(Arc::new(buf))?;
        assert!(
            local.registered_mr::<MlxDevice>(&device)?.is_none(),
            "the region should have no registration before it is registered",
        );
        RdmaManagerActor::local_handle(&env.client)
            .request_buffer(&env.client, local.clone())
            .await?;
        assert!(
            local.registered_mr::<MlxDevice>(&device)?.is_some(),
            "registration should be recorded under the pinned device {device}",
        );
        env.shutdown().await
    }

    /// With `rdma_max_nics_per_buffer` above 1, a buffer is registered on that
    /// many of its tied-for-best NICs. Under the `match_name` policy, a remote
    /// buffer must always be registered on the first N devices returned by
    /// `select_optimal_ibv_devices` -- otherwise, a mismatch could make a transfer
    /// impossible.
    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_match_name_nic_selection() -> Result<(), anyhow::Error> {
        require_rdma();
        const MAX_NICS: usize = 4;
        let lock = hyperactor_config::global::lock();
        let _max_guard = lock.override_key(
            crate::config::RDMA_MAX_NICS_PER_BUFFER,
            Some(hyperactor_config::NonZeroUsize::new(MAX_NICS).expect("MAX_NICS is 4")),
        );
        let _policy_guard = lock.override_key(
            crate::config::RDMA_PEER_DEVICE_AFFINITY,
            "match_name".to_string(),
        );
        let expected: BTreeSet<String> =
            select_optimal_ibv_devices::<MlxDevice>(MemoryLocation::Cpu(None))?
                .iter()
                .take(MAX_NICS)
                .map(|nic| nic.name().clone())
                .collect();

        let env = TestEnv::same_config(IbvConfig::default()).await?;
        let buffer = env
            .helper_a
            .allocate(&env.client, 32, BufferDevice::Cpu, 0)
            .await?;
        let served_by: BTreeSet<String> = buffer
            .resolve_mlx()
            .expect("the buffer is registered on a Mellanox NIC")
            .buffers
            .iter()
            .map(|mr| mr.device_name.clone())
            .collect();
        assert_eq!(served_by, expected);

        for pattern in 0..2 * MAX_NICS as u8 {
            run_cross_actor_write(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, pattern, 5)
                .await?;
        }
        env.shutdown().await
    }

    /// Under `any` a buffer starts at a NIC drawn at random, so at one NIC per
    /// buffer the buffers spread over the tied NICs instead of all landing on
    /// the first.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_any_nic_selection() -> Result<(), anyhow::Error> {
        require_rdma();
        let lock = hyperactor_config::global::lock();
        let _max_guard = lock.override_key(
            crate::config::RDMA_MAX_NICS_PER_BUFFER,
            Some(hyperactor_config::NonZeroUsize::MIN),
        );
        let _policy_guard =
            lock.override_key(crate::config::RDMA_PEER_DEVICE_AFFINITY, "any".to_string());
        let tied = select_optimal_ibv_devices::<MlxDevice>(MemoryLocation::Cpu(None))?.len();

        let env = TestEnv::same_config(IbvConfig::default()).await?;
        let mut served_by: BTreeSet<String> = BTreeSet::new();
        for _ in 0..16 {
            let buffer = env
                .helper_a
                .allocate(&env.client, 32, BufferDevice::Cpu, 0)
                .await?;
            let registrations = buffer
                .resolve_mlx()
                .expect("the buffer is registered on a Mellanox NIC")
                .buffers;
            let [mr] = registrations.as_slice() else {
                panic!("one NIC serves a buffer here, got {registrations:?}");
            };
            served_by.insert(mr.device_name.clone());
        }
        // Sixteen buffers all drawing the same one of several NICs would be a
        // one-in-billions coincidence.
        assert_eq!(
            served_by.len() > 1,
            tied > 1,
            "{tied} NICs tie for host memory, but the buffers landed on {served_by:?}",
        );
        env.shutdown().await
    }

    /// Cross-actor RDMA write over a single device (both sides target
    /// `cpu:0`). The two `RdmaManagerActor`s differ — this exercises
    /// the asymmetric `CreatePeerQueuePair` handshake even though the
    /// underlying device is shared.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_actor_same_device_write() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_cross_actor_write(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0xa5, 5).await?;
        env.shutdown().await
    }

    /// Cross-actor RDMA read over a single device.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_actor_same_device_read() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_cross_actor_read(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0x3c, 5).await?;
        env.shutdown().await
    }

    /// True loopback write: both buffers registered with the same
    /// `RdmaManagerActor` on the same device. The `QueuePairActor`
    /// sees `is_loopback = true` and connects its QP to its own
    /// endpoint without going through `CreatePeerQueuePair`.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_loopback_write() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_true_loopback(&env, BufferDevice::Cpu, 32).await?;
        env.shutdown().await
    }

    /// Cross-device write (cpu:0 → cpu:1).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_device_write() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_write(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0x77, 5).await?;
        env.shutdown().await
    }

    /// Cross-device read (cpu:0 ← cpu:1).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_device_read() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_read(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0x88, 5).await?;
        env.shutdown().await
    }

    /// One write + one read in a single `IbvBackend::submit` batch.
    /// Both ops share the same `QpKey` so the manager groups them
    /// into a single `ProcessOps` dispatched to one `QueuePairActor`.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_multi_op_same_qp_cpu() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_multi_op_same_qp(&env, BufferDevice::Cpu, BufferDevice::Cpu, 64, 5).await?;
        env.shutdown().await
    }

    /// Same as `test_multi_op_same_qp_cpu` but with 2 MiB CUDA buffers,
    /// pulled apart into a separate test because the buffer-size +
    /// device split is the only thing that differs.
    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_multi_op_same_qp_cuda() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_multi_op_same_qp(&env, BufferDevice::Cuda(0), BufferDevice::Cuda(1), SIZE, 10).await?;
        env.shutdown().await
    }

    /// CUDA → CPU write.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cpu_write() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cpu,
            SIZE,
            0x9b,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA → CPU read (source is the CUDA side).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cpu_read() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_read(
            &env,
            BufferDevice::Cpu,
            BufferDevice::Cuda(1),
            SIZE,
            0x37,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CPU → CUDA write.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cpu_to_cuda_write() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cpu,
            BufferDevice::Cuda(1),
            SIZE,
            0x5a,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CPU → CUDA read (source is the CPU side).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cpu_to_cuda_read() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_read(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cpu,
            SIZE,
            0x4e,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA → CUDA write.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cuda_write() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cuda(1),
            SIZE,
            0xee,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA → CUDA read.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cuda_read() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_read(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cuda(1),
            SIZE,
            0x42,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA buffers with `IbvQpType::Standard` (no mlx5dv).
    /// Exercises the per-buffer dmabuf MR-registration path: without
    /// mlx5dv the manager cannot use indirect mkeys via segment
    /// scanning and instead registers each buffer as a standalone
    /// dmabuf MR (`ibv_reg_dmabuf_mr`).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_standard_qp_cuda_dmabuf_fallback() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 16 * 1024 * 1024;
        let mut config_a = IbvConfig::targeting(IbvDeviceTarget::gpu(0));
        config_a.qp_type = IbvQpType::Standard;
        let mut config_b = IbvConfig::targeting(IbvDeviceTarget::gpu(1));
        config_b.qp_type = IbvQpType::Standard;
        let env = TestEnv::new(config_a, config_b).await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cuda(1),
            SIZE,
            0x33,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// Two `IbvBackend::submit` calls back-to-back through the same
    /// helper. The second batch reuses the cached `QueuePairActor`
    /// from the first (the manager's `qp_handles` entry persists
    /// across submits).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_multi_batch_same_qp() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        let src1 = env
            .helper_a
            .allocate(&env.client, 32, BufferDevice::Cpu, 0xa1)
            .await?;
        let dst1 = env
            .helper_b
            .allocate(&env.client, 32, BufferDevice::Cpu, 0)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src1,
                    remote_buf: dst1.clone(),
                }],
                5,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, dst1, 32, 0xa1).await?;

        let src2 = env
            .helper_a
            .allocate(&env.client, 32, BufferDevice::Cpu, 0xb2)
            .await?;
        let dst2 = env
            .helper_b
            .allocate(&env.client, 32, BufferDevice::Cpu, 0)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src2,
                    remote_buf: dst2.clone(),
                }],
                5,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, dst2, 32, 0xb2).await?;
        env.shutdown().await
    }

    /// Single submit batch with ops landing on multiple `QpKey`
    /// groups: loopback (helper_a → helper_a), cross-actor cpu↔cpu,
    /// cpu↔cuda, cuda↔cpu, cuda↔cuda. Exercises the manager's
    /// per-QP slicing and concurrent multi-QP dispatch.
    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_multi_op_multi_qp() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;

        let env = TestEnv::same_config(IbvConfig::default()).await?;

        const LOOPBACK_PAT: u8 = 0x11;
        let lb_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, LOOPBACK_PAT)
            .await?;
        let lb_dst = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        const CC_PAT: u8 = 0x22;
        let cc_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, CC_PAT)
            .await?;
        let cc_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        const CG_PAT: u8 = 0x33;
        let cg_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, CG_PAT)
            .await?;
        let cg_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cuda(1), 0)
            .await?;

        const GC_PAT: u8 = 0x44;
        let gc_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cuda(0), GC_PAT)
            .await?;
        let gc_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        const GG_PAT: u8 = 0x55;
        let gg_src = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cuda(1), GG_PAT)
            .await?;
        let gg_dst = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cuda(0), 0)
            .await?;

        env.helper_a
            .submit(
                &env.client,
                vec![
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: lb_src,
                        remote_buf: lb_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: cc_src,
                        remote_buf: cc_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: cg_src,
                        remote_buf: cg_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: gc_src,
                        remote_buf: gc_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::ReadIntoLocal,
                        local_buf: gg_dst.clone(),
                        remote_buf: gg_src,
                    },
                ],
                30,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;

        assert_remote_pattern(&env.helper_a, &env.client, lb_dst, SIZE, LOOPBACK_PAT).await?;
        assert_remote_pattern(&env.helper_b, &env.client, cc_dst, SIZE, CC_PAT).await?;
        assert_remote_pattern(&env.helper_b, &env.client, cg_dst, SIZE, CG_PAT).await?;
        assert_remote_pattern(&env.helper_b, &env.client, gc_dst, SIZE, GC_PAT).await?;
        assert_remote_pattern(&env.helper_a, &env.client, gg_dst, SIZE, GG_PAT).await?;

        env.shutdown().await
    }

    /// Force the timeout branch in `IbvBackend::submit`. A near-zero
    /// timeout fires before any per-op replies arrive, so the
    /// aggregated error reports the timeout terminal cause.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_submit_timeout() -> Result<(), anyhow::Error> {
        require_rdma();
        const SIZE: usize = 1024 * 1024;
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        let src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0x77)
            .await?;
        let dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;
        let result = env
            .helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src,
                    remote_buf: dst,
                }],
                0,
            )
            .await?;
        let err = result.expect_err("expected submit to time out");
        assert!(
            err.contains("submit timed out"),
            "unexpected error message: {err}",
        );
        env.shutdown().await
    }

    /// Submit a batch with a bogus op in the middle. RC
    /// completions fire in posting order, so the good op before it
    /// completes normally, the bogus op fails with `REM_ACCESS_ERR`
    /// (it puts the QP into error state), and the good op after it
    /// gets flushed with `WC_WR_FLUSH_ERR`. Verifies (a) op 0 is
    /// absent from the aggregated error and its bytes transferred,
    /// (b) ops 1 and 2 both appear in the error, and (c) op 2's
    /// destination was *not* written (the flush meant nothing was
    /// transferred).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_partial_failure_batch() -> Result<(), anyhow::Error> {
        require_rdma();
        const SIZE: usize = 32;
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;

        const GOOD_PAT: u8 = 0xc3;
        const POST_FLUSH_PAT: u8 = 0xde;

        let good_src_0 = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, GOOD_PAT)
            .await?;
        let good_dst_0 = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        let bogus_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0xee)
            .await?;
        let real_remote = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;
        let mut bogus_remote = real_remote.clone();
        let bufs = [
            bogus_remote
                .backends
                .mlx
                .as_mut()
                .map(|ctx| &mut ctx.buffers),
            bogus_remote
                .backends
                .efa
                .as_mut()
                .map(|ctx| &mut ctx.buffers),
        ];
        for buf in bufs.into_iter().flatten().flatten() {
            buf.rkey = 0xdead_beef;
            buf.addr = 0xdead_0000;
        }

        let post_flush_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, POST_FLUSH_PAT)
            .await?;
        let post_flush_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        let result = env
            .helper_a
            .submit(
                &env.client,
                vec![
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: good_src_0,
                        remote_buf: good_dst_0.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: bogus_src,
                        remote_buf: bogus_remote,
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: post_flush_src,
                        remote_buf: post_flush_dst.clone(),
                    },
                ],
                10,
            )
            .await?;
        let err = result.expect_err("expected at least one op to fail");
        let rem_access = format!(
            "status={:?}",
            rdmaxcel_sys::ibv_wc_status::IBV_WC_REM_ACCESS_ERR,
        );
        let wr_flush = format!(
            "status={:?}",
            rdmaxcel_sys::ibv_wc_status::IBV_WC_WR_FLUSH_ERR,
        );
        assert!(
            !err.contains("op 0:"),
            "op 0 should not appear in error: {err}",
        );
        let op1 = err
            .split("\n  ")
            .find(|line| line.starts_with("op 1:"))
            .unwrap_or_else(|| panic!("expected op 1 line in error: {err}"));
        assert!(
            op1.contains("completion failed") && op1.contains(&rem_access),
            "expected op 1 to fail with REM_ACCESS_ERR: {op1}",
        );
        let op2 = err
            .split("\n  ")
            .find(|line| line.starts_with("op 2:"))
            .unwrap_or_else(|| panic!("expected op 2 line in error: {err}"));
        assert!(
            op2.contains("completion failed") && op2.contains(&wr_flush),
            "expected op 2 to be flushed with WR_FLUSH_ERR: {op2}",
        );

        assert_remote_pattern(&env.helper_b, &env.client, good_dst_0, SIZE, GOOD_PAT).await?;
        // The flushed op was never transferred; destination stays zero.
        assert_remote_pattern(&env.helper_b, &env.client, post_flush_dst, SIZE, 0).await?;

        env.shutdown().await
    }
}
