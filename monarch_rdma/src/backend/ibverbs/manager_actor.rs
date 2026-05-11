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
//!
//! ## Queue-pair lifecycle
//!
//! Bringing up a queue pair to a peer is a two-sided handshake (each
//! side has its own QP and must learn the other side's endpoint
//! before transitioning `INIT → RTR → RTS`). Doing all of that in
//! response to a single message would block our actor loop while
//! awaiting peer RPCs, and the peer's symmetric request would block
//! waiting for us — a deadlock.
//!
//! Instead, [`IbvManagerActor`] does only sync bookkeeping in the
//! handler and offloads the handshake to a per-QP child actor,
//! [`QueuePairInitializer`]. The store of QPs ([`Self::qps`]) is
//! keyed by [`QpKey`] and holds a [`QpState`]: `Pending { info,
//! initializer, waiters }` while the handshake runs, `Ready(qp)`
//! once this side is RTS and has observed the peer's RTS, or
//! `Failed(error)` as a tombstone after a fatal error.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use backoff::ExponentialBackoff;
use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::actor::Referable;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use self::shared_state::SharedStateMut;
use super::IbvBuffer;
use super::IbvOp;
use super::domain::IbvDomain;
use super::primitives::IbvConfig;
use super::primitives::IbvDevice;
use super::primitives::IbvMemoryRegion;
use super::primitives::IbvMemoryRegionView;
use super::primitives::IbvQpInfo;
use super::primitives::ibverbs_supported;
use super::primitives::mlx5dv_supported;
use super::primitives::resolve_qp_type;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::PeerInfo;
use super::queue_pair::PollCompletionError;
use super::queue_pair::PollTarget;
use super::queue_pair::QpGuard;
use super::queue_pair::QpKey;
use super::queue_pair::QueuePairInitializer;
use crate::RdmaOp;
use crate::RdmaOpType;
use crate::RdmaTransportLevel;
use crate::backend::RdmaBackend;
use crate::local_memory::RdmaLocalMemory;
use crate::rdma_components::get_registered_cuda_segments;
use crate::rdma_manager_actor::GetIbvActorRefClient;
use crate::rdma_manager_actor::RdmaManagerActor;
use crate::validate_execution_context;

/// Cross-proc message: peer asks for our endpoint, lazily creating
/// the entry on our side if absent. Generic over the manager actor
/// type so tests can swap in a mock.
#[derive(Debug, Serialize, Deserialize, Named)]
#[serde(bound(serialize = "", deserialize = ""))]
pub(super) struct EnsureQueuePair<A: Referable> {
    pub(super) sender: ActorRef<A>,
    pub(super) sender_device: String,
    pub(super) receiver_device: String,
    pub(super) reply: PortRef<PeerInfo>,
}
wirevalue::register_type!(EnsureQueuePair<IbvManagerActor>);

/// Per-QpKey state in [`IbvManagerActor::qps`].
///
/// `Pending` covers the entire handshake (an initializer is running);
/// `Ready` is the terminal usable state; `Failed` is a tombstone that
/// records the error so subsequent `RequestQueuePair` / `EnsureQueuePair`
/// calls for the same key surface the same error rather than retrying
/// or hanging.
///
/// TODO: add recovery — allow retries via an explicit message or after
/// a backoff. For now the entry stays `Failed` for the life of the
/// manager.
#[derive(Debug)]
enum QpState {
    Pending {
        /// Local endpoint, captured when the QP was first created so
        /// repeated `EnsureQueuePair` calls don't have to re-extract it.
        info: IbvQpInfo,
        /// Child actor driving the handshake. Stopped on
        /// `QpInitializerDone`/`QpInitializerFailed`.
        initializer: ActorHandle<QueuePairInitializer<IbvManagerActor>>,
        /// Local `RequestQueuePair` callers waiting for the QP. Drained
        /// to `Ok(qp.clone())` on `Ready`, or `Err(_)` on failure.
        waiters: Vec<OncePortHandle<Result<IbvQueuePair, String>>>,
    },
    Ready(IbvQueuePair),
    Failed(String),
}

// ---------------------------------------------------------------------
// SharedState — sub-module so its fields stay private.
// ---------------------------------------------------------------------

mod shared_state {
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::sync::RwLockReadGuard;
    use std::sync::RwLockWriteGuard;

    use dashmap::DashMap;

    use super::IbvConfig;
    use super::IbvDomain;
    use super::IbvQueuePair;
    use super::QpKey;
    use super::QpState;

    /// Data-path state intended to be shared between
    /// [`super::IbvManagerActor`] and a future processor child via
    /// [`Arc<SharedState>`]. Fields are private to this module: outside
    /// callers see the read-only API; mutable views live on [`SharedStateMut`].
    ///
    /// Note that [`SharedState`] does NOT call `rdmaxcel_qp_destroy` on
    /// [`Drop`]: [`IbvQueuePair`] derives `Clone`, so we can't prove the
    /// last reference is gone, and destroying the FFI handle here would
    /// risk use-after-free in any holder of a clone (e.g. an in-flight
    /// op). We accept the kernel-side leak for now; revisit when QP
    /// ownership stops being clonable.
    #[derive(Debug)]
    pub(super) struct SharedState {
        config: IbvConfig,
        mlx5dv_enabled: bool,

        /// Per-QP state keyed by [`QpKey`]. Holds both peer QPs (driven
        /// by `QueuePairInitializer`) and per-device loopback QPs (inserted
        /// directly as `Ready`). `DashMap` is per-shard concurrent.
        qps: DashMap<QpKey, QpState>,

        /// Protection domain per RDMA device; filled lazily during MR
        /// registration / QP creation.
        device_domains: RwLock<HashMap<String, IbvDomain>>,
    }

    impl SharedState {
        /// The manager's [`IbvConfig`].
        #[allow(dead_code)]
        pub(super) fn config(&self) -> &IbvConfig {
            &self.config
        }

        /// Whether mlx5dv-specific code paths are enabled.
        #[allow(dead_code)]
        pub(super) fn mlx5dv_enabled(&self) -> bool {
            self.mlx5dv_enabled
        }

        /// Read-only view of `device_domains`. Returned guard releases
        /// the `RwLock` on drop.
        #[allow(dead_code)]
        pub(super) fn device_domains(&self) -> RwLockReadGuard<'_, HashMap<String, IbvDomain>> {
            self.device_domains
                .read()
                .expect("device_domains lock poisoned")
        }

        /// Fast-path lookup of a QP by [`QpKey`]. Returns
        /// `Ok(Some(qp))` if the entry is `Ready`, `Ok(None)` if the
        /// entry is absent or still `Pending` (callers should fall
        /// back to the manager's slow path
        /// [`IbvManagerLocalMessage::RequestQueuePair`]), and
        /// `Err(error)` if the entry is the `Failed` tombstone — the
        /// error message is the one captured at handshake time.
        #[allow(dead_code)]
        pub(super) fn lookup_qp(&self, key: &QpKey) -> Result<Option<IbvQueuePair>, String> {
            let Some(entry) = self.qps.get(key) else {
                return Ok(None);
            };
            match entry.value() {
                QpState::Ready(qp) => Ok(Some(qp.clone())),
                QpState::Failed(error) => Err(error.clone()),
                QpState::Pending { .. } => Ok(None),
            }
        }
    }

    /// Mutating handle on the [`SharedState`]. Held by the
    /// [`super::IbvManagerActor`]; exposes write views over the
    /// mutable fields so the business logic can live on the actor
    /// itself. `Deref<Target = SharedState>` lets callers reach the
    /// read-only API as well.
    #[derive(Debug)]
    pub(super) struct SharedStateMut(Arc<SharedState>);

    impl SharedStateMut {
        /// Create a fresh, empty shared state.
        pub(super) fn new(config: IbvConfig, mlx5dv_enabled: bool) -> Self {
            Self(Arc::new(SharedState {
                config,
                mlx5dv_enabled,
                qps: DashMap::new(),
                device_domains: RwLock::new(HashMap::new()),
            }))
        }

        /// Hand out an [`Arc<SharedState>`] for read-only sharing.
        #[allow(dead_code)]
        pub(super) fn read(&self) -> Arc<SharedState> {
            Arc::clone(&self.0)
        }

        /// Direct mutable access to the per-QP state map. Used by the
        /// [`super::IbvManagerActor`] handlers that drive the QP
        /// lifecycle.
        pub(super) fn qps(&self) -> &DashMap<QpKey, QpState> {
            &self.0.qps
        }

        /// Write view of `device_domains` for the
        /// [`super::IbvManagerActor`] business logic to insert / read
        /// per-device PDs. Returned guard releases the `RwLock` on drop.
        pub(super) fn device_domains_mut(
            &self,
        ) -> RwLockWriteGuard<'_, HashMap<String, IbvDomain>> {
            self.0
                .device_domains
                .write()
                .expect("device_domains lock poisoned")
        }
    }

    impl Deref for SharedStateMut {
        type Target = SharedState;
        fn deref(&self) -> &SharedState {
            &self.0
        }
    }
}

/// Cross-proc messages handled by [`IbvManagerActor`].
///
/// `EnsureQueuePair` is defined as a separate top-level message
/// because it's generic over the manager actor type to allow
/// mocking in tests.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum IbvManagerMessage {
    /// Release a buffer registration by `remote_buf_id`. Fire-and-forget
    /// (no reply port) to avoid blocking the caller during teardown.
    ReleaseBuffer { remote_buf_id: usize },
}
wirevalue::register_type!(IbvManagerMessage);

/// Local-only messages for [`IbvManagerActor`].
#[derive(Handler, HandleClient, Debug)]
pub enum IbvManagerLocalMessage {
    /// Register a memory region, returning the
    /// [`IbvMemoryRegionView`]. The view's `Arc<IbvMemoryRegion>`
    /// keeps the underlying MR alive for as long as the caller (and
    /// anyone they clone the view to) holds it; deregistration
    /// happens automatically when the last clone drops.
    RegisterMr {
        addr: usize,
        size: usize,
        #[reply]
        reply: OncePortHandle<Result<IbvMemoryRegionView, String>>,
    },
    /// Register a remote-facing buffer's MR and return its
    /// [`IbvBuffer`]. Called by
    /// [`crate::rdma_manager_actor::RdmaManagerActor::request_buffer`]
    /// at buffer-creation time.
    ///
    /// The MR lives in [`IbvManagerActor::buffer_registrations`] and
    /// is deregistered on [`IbvManagerMessage::ReleaseBuffer`].
    RegisterRemoteBuffer {
        remote_buf_id: usize,
        local: Arc<dyn RdmaLocalMemory>,
        #[reply]
        reply: OncePortHandle<Result<IbvBuffer, String>>,
    },
    /// User-facing entry point: get a connected `IbvQueuePair` for
    /// `(self_device, other actor's id, other_device)`. Lazily creates
    /// the QP + initializer if absent; if a handshake is in flight,
    /// the reply port is queued and answered when the QP becomes
    /// `Ready` (or fails).
    ///
    /// No `#[reply]` because the handler may park `reply` on the
    /// `Pending` entry and answer it later from `QpInitializerDone`/
    /// `QpInitializerFailed`.
    RequestQueuePair {
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        reply: OncePortHandle<Result<IbvQueuePair, String>>,
    },
    /// Initializer reports that the handshake succeeded. Manager
    /// moves the entry to `Ready(qp)`, drains waiters, then stops
    /// the initializer.
    QpInitializerDone { qp_key: QpKey, qp: QpGuard },
    /// Initializer reports that the handshake failed. Manager
    /// errors out waiters, tombstones the entry as
    /// `QpState::Failed(error)`, then stops the initializer.
    QpInitializerFailed { qp_key: QpKey, error: String },
}

/// Adaptive wait between completion polls.
///
/// While the elapsed time since [`Self::yield_now`] was first called
/// is below `yield_window`, the policy yields cooperatively
/// (`tokio::task::yield_now`) — keeping latency tight when the WR
/// completes shortly after being posted. `tokio::time::sleep` has a
/// minimum resolution of ~1ms (the timer wheel tick), so even a
/// `sleep(Duration::from_micros(100))` would block that long; `yield_now` is
/// sub-millisecond and lets the next poll fire as soon as the runtime
/// schedules us. Past `yield_window` the policy switches to an
/// exponential backoff (1ms initial, doubling, capped at 10ms) so
/// long-running operations don't keep the runtime spinning.
///
/// `yield_window` is read from
/// [`crate::config::RDMA_CQ_BUSY_POLL_WINDOW`]. When it's `None`
/// (the default) the policy disables the cutoff and only ever
/// yields, never sleeps.
struct PollSleepPolicy {
    yield_window: Option<Duration>,
    started_at: Option<Instant>,
    backoff: Option<ExponentialBackoff>,
}

impl PollSleepPolicy {
    fn new() -> Self {
        let yield_window = hyperactor_config::global::get(crate::config::RDMA_CQ_BUSY_POLL_WINDOW);
        Self {
            yield_window,
            started_at: None,
            backoff: None,
        }
    }

    /// Suspend the current task before the next poll. If no yield
    /// window is configured (the default), always yields. Otherwise,
    /// yields while within the window and then walks an exponential
    /// backoff up to 10ms past it.
    async fn yield_now(&mut self) {
        let Some(window) = self.yield_window else {
            tokio::task::yield_now().await;
            return;
        };
        let started = *self.started_at.get_or_insert_with(Instant::now);
        if started.elapsed() < window {
            tokio::task::yield_now().await;
            return;
        }
        let backoff = self.backoff.get_or_insert_with(|| {
            ExponentialBackoffBuilder::new()
                .with_initial_interval(Duration::from_millis(1))
                .with_max_interval(Duration::from_millis(10))
                .with_multiplier(2.0)
                .with_randomization_factor(0.0)
                .with_max_elapsed_time(None)
                .build()
        });
        match backoff.next_backoff() {
            Some(delay) => tokio::time::sleep(delay).await,
            None => tokio::task::yield_now().await,
        }
    }
}

/// Computed segment-scanner result for an address: just the fields
/// that depend on the matched segment, with no MR ownership or
/// caller-supplied identity. The caller assembles the full
/// [`IbvMemoryRegionView`] (allocating an `id`, attaching the
/// segment-singleton [`Arc<IbvMemoryRegion>`], and tagging the device name).
#[derive(Debug, PartialEq, Eq)]
pub(super) struct SegmentInfo {
    pub virtual_addr: usize,
    pub rdma_addr: usize,
    pub size: usize,
    pub lkey: u32,
    pub rkey: u32,
}

/// Look up `(addr, size)` in a slice of registered CUDA segments
/// and return a view into the matching mkey.
///
/// Bounded by `mr_size` (what the mkey actually covers), NOT by
/// `phys_size` (the scanner-reported extent). They diverge when
/// `register_segments` hits `max_sge` and stops growing the binding.
/// Returning a view based on `phys_size` would hand out an
/// `(lkey, offset)` past the bound and the WR would fail with
/// `IBV_WC_LOC_PROT_ERR`; bounding by `mr_size` makes the gap a
/// miss so the caller falls back to per-buffer dmabuf.
///
/// Free function so the boundary can be unit-tested without an actor.
pub(super) fn lookup_segment_for_address(
    segments: &[rdmaxcel_sys::rdma_segment_info_t],
    addr: usize,
    size: usize,
) -> Option<SegmentInfo> {
    for segment in segments {
        let start_addr = segment.phys_address;
        let end_addr = start_addr + segment.mr_size;
        if start_addr <= addr && addr + size <= end_addr {
            let offset = addr - start_addr;
            let rdma_addr = segment.mr_addr + offset;
            return Some(SegmentInfo {
                virtual_addr: addr,
                rdma_addr,
                size,
                lkey: segment.lkey,
                rkey: segment.rkey,
            });
        }
    }
    None
}

/// Manages all ibverbs-specific RDMA resources and operations.
///
/// This struct handles memory registration, queue pair management,
/// and connection establishment using the ibverbs API.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        IbvManagerMessage,
        EnsureQueuePair<IbvManagerActor>,
    ],
)]
pub struct IbvManagerActor {
    owner: OnceLock<ActorHandle<RdmaManagerActor>>,

    /// Mutating handle on the data-path state. Holds `qps`,
    /// `device_domains`, `config`, and `mlx5dv_enabled`.
    state: SharedStateMut,

    // Map from buffer_id to registration details. Storing the
    // [`IbvMemoryRegionView`] keeps the MR alive until the entry is
    // removed by `release_buffer`; no manual FFI cleanup is needed.
    buffer_registrations: HashMap<usize, (IbvBuffer, IbvMemoryRegionView)>,

    // Counter for the next `IbvMemoryRegionView::id`. Mutated only
    // from this actor's handlers, so a plain `usize` suffices.
    mrv_id: usize,

    // Singleton owner of the mlx5dv segment scanner registrations.
    // Lazily set on the first segment-backed view; cloned into every
    // such view so segments are deregistered once the last view drops.
    segment_owner: Option<Arc<IbvMemoryRegion>>,
}

#[async_trait]
impl Actor for IbvManagerActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
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
}

impl Drop for IbvManagerActor {
    fn drop(&mut self) {
        // Drop the buffer registrations and segment owner explicitly so the
        // [`Arc<IbvMemoryRegion>`] decrements (and the ibv_dereg_mr calls
        // they trigger) happen before the [`SharedStateMut`] field,
        // which would otherwise tear down the PDs the MRs reference.
        self.buffer_registrations.clear();
        self.segment_owner.take();
    }
}

impl IbvManagerActor {
    /// Construct an [`ActorHandle`] for the [`IbvManagerActor`] co-located
    /// with the caller by querying the local [`RdmaManagerActor`].
    pub async fn local_handle(
        client: &(impl hyperactor::context::Actor + Send + Sync),
    ) -> Result<ActorHandle<Self>, anyhow::Error> {
        let rdma_handle = RdmaManagerActor::local_handle(client);
        let ibv_ref: ActorRef<IbvManagerActor> = rdma_handle
            .get_ibv_actor_ref(client)
            .await?
            .ok_or_else(|| anyhow::anyhow!("local RdmaManagerActor has no ibverbs backend"))?;
        ibv_ref
            .downcast_handle(client)
            .ok_or_else(|| anyhow::anyhow!("IbvManagerActor is not in the local process"))
    }

    /// Create a new IbvManagerActor with the given configuration.
    pub async fn new(params: Option<IbvConfig>) -> Result<Self, anyhow::Error> {
        if !ibverbs_supported() {
            return Err(anyhow::anyhow!(
                "Cannot create IbvManagerActor because RDMA is not supported on this machine"
            ));
        }

        // Use provided config or default if none provided
        let mut config = params.unwrap_or_default();
        tracing::debug!("rdma is enabled, config device hint: {}", config.device);

        let mlx5dv_enabled = mlx5dv_supported()
            && resolve_qp_type(config.qp_type) == rdmaxcel_sys::RDMA_QP_TYPE_MLX5DV
            && !crate::efa::is_efa_device();

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

        Ok(Self {
            owner: OnceLock::new(),
            state: SharedStateMut::new(config, mlx5dv_enabled),
            buffer_registrations: HashMap::new(),
            mrv_id: 0,
            segment_owner: None,
        })
    }

    /// Look up or lazily create the [`IbvDomain`] for `device_name`.
    fn get_or_create_device_domain(
        &mut self,
        device_name: &str,
        rdma_device: &IbvDevice,
    ) -> Result<IbvDomain, anyhow::Error> {
        let mut domains = self.state.device_domains_mut();
        if let Some(domain) = domains.get(device_name) {
            return Ok(domain.clone());
        }
        let domain = IbvDomain::new(rdma_device.clone()).map_err(|e| {
            anyhow::anyhow!("could not create domain for device {}: {}", device_name, e)
        })?;
        // Print device info if MONARCH_DEBUG_RDMA=1 is set (before
        // initial QP creation).
        crate::print_device_info_if_debug_enabled(domain.context);
        domains.insert(device_name.to_string(), domain.clone());
        Ok(domain)
    }

    /// Look up or lazily create the per-device loopback QP, used by
    /// the C++ segment scanner. The QP is stored in
    /// [`SharedStateMut::qps`] as `Ready` under a self-keyed
    /// [`QpKey`] so it sits alongside peer QPs in the same map.
    ///
    /// Initialization is synchronous (no [`QueuePairInitializer`]):
    /// there is no peer to rendezvous with — the endpoint connects to
    /// itself.
    fn get_or_create_loopback_qp(
        &mut self,
        cx: &Context<Self>,
        device_name: &str,
        domain: &IbvDomain,
    ) -> Result<IbvQueuePair, anyhow::Error> {
        let qp_key = self.loopback_qp_key(cx, device_name);
        if let Some(qp) = self
            .state
            .lookup_qp(&qp_key)
            .map_err(|e| anyhow::anyhow!("loopback QP for device {} failed: {}", device_name, e))?
        {
            return Ok(qp);
        }
        // Wrap in `QpGuard` so an early return from `get_qp_info` /
        // `connect` destroys the underlying `rdmaxcel_qp_t` instead
        // of leaking it.
        let mut qp = QpGuard::new(
            IbvQueuePair::new(domain.context, domain.pd, self.state.config().clone()).map_err(
                |e| {
                    anyhow::anyhow!(
                        "could not create loopback QP for device {}: {}",
                        device_name,
                        e
                    )
                },
            )?,
        );
        let endpoint = qp.get_qp_info().map_err(|e| {
            anyhow::anyhow!("could not get QP info for device {}: {}", device_name, e)
        })?;
        qp.connect(&endpoint).map_err(|e| {
            anyhow::anyhow!(
                "could not connect loopback QP for device {}: {}",
                device_name,
                e
            )
        })?;
        let qp = qp.into_inner();
        self.state.qps().insert(qp_key, QpState::Ready(qp.clone()));
        Ok(qp)
    }

    /// Build the [`QpKey`] that identifies this actor's loopback QP
    /// for `device_name`: `self_device == other_device == device_name`,
    /// `other_id == self's actor id`.
    fn loopback_qp_key(&self, cx: &Context<Self>, device_name: &str) -> QpKey {
        QpKey {
            self_device: device_name.to_string(),
            other_id: cx.self_addr().id().clone(),
            other_device: device_name.to_string(),
        }
    }

    /// Build parallel PD/QP arrays indexed by CUDA device ordinal
    /// for the C++ `register_segments` call. A device with no domain
    /// (or no loopback QP) gets `null_mut()` in both slots — the
    /// scanner skips those entries.
    fn build_per_device_pd_qp_arrays(
        &self,
        cx: &Context<Self>,
    ) -> (
        Vec<*mut rdmaxcel_sys::ibv_pd>,
        Vec<*mut rdmaxcel_sys::rdmaxcel_qp_t>,
    ) {
        let domains = self.state.device_domains();
        let cuda_map = super::device_selection::get_cuda_device_to_ibv_device();
        let mut pds = Vec::with_capacity(cuda_map.len());
        let mut qps = Vec::with_capacity(cuda_map.len());
        for maybe_device in cuda_map {
            let (pd, qp_ptr) = match maybe_device
                .as_ref()
                .and_then(|device| domains.get(device.name()).map(|domain| (device, domain)))
            {
                Some((device, domain)) => {
                    let qp_key = self.loopback_qp_key(cx, device.name());
                    // We never store loopback entries as `Failed`, so
                    // an error here would mean a peer entry collided
                    // on the same key — surface it as a null QP and
                    // let the C++ scanner skip the device.
                    let qp_ptr = match self.state.lookup_qp(&qp_key) {
                        Ok(Some(qp)) => qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp_t,
                        Ok(None) => std::ptr::null_mut(),
                        Err(error) => {
                            tracing::error!(
                                "unexpected Failed loopback QP for device {}: {}",
                                device.name(),
                                error
                            );
                            std::ptr::null_mut()
                        }
                    };
                    (domain.pd, qp_ptr)
                }
                None => (std::ptr::null_mut(), std::ptr::null_mut()),
            };
            pds.push(pd);
            qps.push(qp_ptr);
        }
        (pds, qps)
    }

    fn find_cuda_segment_for_address(
        &mut self,
        addr: usize,
        size: usize,
        pd: *mut rdmaxcel_sys::ibv_pd,
        device_name: &str,
    ) -> Option<IbvMemoryRegionView> {
        let registered_segments = get_registered_cuda_segments(pd);
        // Allocate the id only after a hit so misses don't burn ids.
        let info = lookup_segment_for_address(&registered_segments, addr, size)?;
        let id = self.mrv_id;
        self.mrv_id += 1;
        // Lazily create the segment-scanner singleton on first hit;
        // every subsequent segment-backed view shares this `Arc` so
        // `deregister_segments` runs when the last view drops.
        let mr = self
            .segment_owner
            .get_or_insert_with(|| Arc::new(IbvMemoryRegion::Segments))
            .clone();
        Some(IbvMemoryRegionView {
            id,
            virtual_addr: info.virtual_addr,
            rdma_addr: info.rdma_addr,
            size: info.size,
            lkey: info.lkey,
            rkey: info.rkey,
            device_name: device_name.to_string(),
            mr,
        })
    }

    /// Register `[addr, addr + size)` as an ibverbs MR and return a
    /// [`IbvMemoryRegionView`] whose `mr: Arc<IbvMemoryRegion>` keeps
    /// the underlying FFI registration alive. Deregistration happens
    /// automatically when the last clone of the view (held by the
    /// caller or `buffer_registrations`) drops.
    fn register_mr_impl(
        &mut self,
        cx: &Context<Self>,
        addr: usize,
        size: usize,
    ) -> Result<IbvMemoryRegionView, anyhow::Error> {
        let mut mem_type: i32 = 0;
        let ptr = addr as rdmaxcel_sys::CUdeviceptr;
        // SAFETY: if `ptr` isn't a valid CUDA pointer, the function
        // will return an error. `mem_type` is guaranteed to be non-null.
        let err = unsafe {
            rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
                &mut mem_type as *mut _ as *mut std::ffi::c_void,
                rdmaxcel_sys::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                ptr,
            )
        };
        let is_cuda = err == rdmaxcel_sys::CUDA_SUCCESS;

        let mut selected_rdma_device = None;

        if is_cuda {
            // Get device ordinal from the CUDA pointer
            let mut device_ordinal: i32 = -1;
            // SAFETY: `device_ordinal` is guaranteed non-null. If `ptr` isn't valid,
            // the function will return an error.
            let err = unsafe {
                rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
                    &mut device_ordinal as *mut _ as *mut std::ffi::c_void,
                    rdmaxcel_sys::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
                    ptr,
                )
            };
            if err == rdmaxcel_sys::CUDA_SUCCESS && device_ordinal >= 0 {
                selected_rdma_device = super::device_selection::get_cuda_device_to_ibv_device()
                    .get(device_ordinal as usize)
                    .and_then(|d| d.clone());
            }
        }

        // Determine the RDMA device to use
        let rdma_device = if let Some(device) = selected_rdma_device {
            device
        } else {
            // Fallback to default device from config
            self.state.config().device.clone()
        };

        let device_name = rdma_device.name().clone();
        tracing::debug!(
            "using RDMA device: {} for memory at 0x{:x}",
            device_name,
            addr
        );

        let domain = self.get_or_create_device_domain(&device_name, &rdma_device)?;
        // The segment scanner needs a loopback QP per device to
        // execute its mkey binding.
        if self.state.mlx5dv_enabled() {
            self.get_or_create_loopback_qp(cx, &device_name, &domain)?;
        }

        let access = if crate::efa::is_efa_device() {
            crate::efa::mr_access_flags()
        } else {
            rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC
        };

        if is_cuda {
            // First, try to use segment scanning if mlx5dv is enabled
            let mut segment_mrv = None;
            if self.state.mlx5dv_enabled() {
                // Try to find in already registered segments
                segment_mrv =
                    self.find_cuda_segment_for_address(addr, size, domain.pd, &device_name);

                // If not found, trigger a re-sync with the allocator and retry
                if segment_mrv.is_none() {
                    let (mut pds, mut qps) = self.build_per_device_pd_qp_arrays(cx);
                    let max_sge_override = self.state.config().max_sge_override;
                    // SAFETY: `pds` and `qps` are guaranteed to be non-null, and `register_segments`
                    // properly handles null array entries and array length.
                    let err = unsafe {
                        rdmaxcel_sys::register_segments(
                            pds.as_mut_ptr(),
                            qps.as_mut_ptr(),
                            pds.len() as i32,
                            max_sge_override,
                        )
                    };
                    // Only retry on success; on failure (e.g.
                    // scanner returned 0 segments) we fall back
                    // to dmabuf.
                    if err == 0i32 {
                        segment_mrv =
                            self.find_cuda_segment_for_address(addr, size, domain.pd, &device_name);
                    }
                }
            }

            if let Some(mrv_from_segment) = segment_mrv {
                Ok(mrv_from_segment)
            } else {
                // Dmabuf path: used when mlx5dv is disabled OR the
                // scanner returned no segments.
                let mut fd: i32 = -1;
                // SAFETY: `fd` is guaranteed to be non-null. If `addr` isn't valid,
                // the function will return an error.
                let cu_err = unsafe {
                    rdmaxcel_sys::rdmaxcel_cuMemGetHandleForAddressRange(
                        &mut fd,
                        addr as rdmaxcel_sys::CUdeviceptr,
                        size,
                        rdmaxcel_sys::CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                        0,
                    )
                };
                if cu_err != rdmaxcel_sys::CUDA_SUCCESS || fd < 0 {
                    return Err(anyhow::anyhow!(
                        "failed to get dmabuf handle for CUDA memory (addr: 0x{:x}, size: {}, cu_err: {}, fd: {})",
                        addr,
                        size,
                        cu_err,
                        fd
                    ));
                }
                // SAFETY: `domain.pd` is guaranteed to be non-null, and `fd` is guaranteed to be a valid file
                // descriptor.
                let mr = unsafe {
                    rdmaxcel_sys::ibv_reg_dmabuf_mr(domain.pd, 0, size, 0, fd, access.0 as i32)
                };
                if mr.is_null() {
                    return Err(anyhow::anyhow!("failed to register dmabuf MR"));
                }
                // SAFETY: `mr` is guaranteed to be non-null.
                unsafe { Ok(self.build_direct_view(addr, size, mr, device_name)) }
            }
        } else {
            // CPU memory path
            // SAFETY: `domain.pd` and `addr` are guaranteed to be non-null and point at valid memory.
            let mr = unsafe {
                rdmaxcel_sys::ibv_reg_mr(
                    domain.pd,
                    addr as *mut std::ffi::c_void,
                    size,
                    access.0 as i32,
                )
            };

            if mr.is_null() {
                return Err(anyhow::anyhow!("failed to register standard MR"));
            }
            // SAFETY: `mr` is guaranteed to be non-null.
            unsafe { Ok(self.build_direct_view(addr, size, mr, device_name)) }
        }
    }

    /// Wrap a freshly-registered `*mut ibv_mr` in an
    /// [`IbvMemoryRegionView`]. The view owns the `Arc<IbvMemoryRegion>` that
    /// deregisters the MR on last drop.
    ///
    /// SAFETY: caller must pass a non-null `mr` that has not yet been
    /// wrapped in an `IbvMemoryRegion`.
    unsafe fn build_direct_view(
        &mut self,
        virtual_addr: usize,
        size: usize,
        mr: *mut rdmaxcel_sys::ibv_mr,
        device_name: String,
    ) -> IbvMemoryRegionView {
        let id = self.mrv_id;
        self.mrv_id += 1;
        unsafe {
            IbvMemoryRegionView {
                id,
                virtual_addr,
                rdma_addr: (*mr).addr as usize,
                size,
                lkey: (*mr).lkey,
                rkey: (*mr).rkey,
                device_name,
                mr: Arc::new(IbvMemoryRegion::Direct(mr)),
            }
        }
    }

    /// Lazy QP creation: if `qp_key` is absent, create the local
    /// `IbvQueuePair`, capture its `IbvQpInfo`, and spawn a
    /// `QueuePairInitializer` to drive the handshake. Returns the
    /// `QpState` entry — either the freshly-inserted `Pending` one,
    /// or the existing `Pending`/`Ready`/`Failed`.
    fn ensure_queue_pair_impl<'a>(
        &'a mut self,
        cx: &Context<'_, Self>,
        other: ActorRef<IbvManagerActor>,
        qp_key: &QpKey,
    ) -> Result<dashmap::mapref::one::RefMut<'a, QpKey, QpState>, anyhow::Error> {
        if !self.state.qps().contains_key(qp_key) {
            let self_device = &qp_key.self_device;
            let rdma_device = super::primitives::get_all_devices()
                .into_iter()
                .find(|d| d.name() == self_device)
                .ok_or_else(|| anyhow::anyhow!("RDMA device '{}' not found", self_device))?;
            let domain = self.get_or_create_device_domain(self_device, &rdma_device)?;
            // Wrap the freshly-created QP in a `QpGuard` immediately
            // so that any early-return path below (e.g. `get_qp_info`
            // failing) destroys the underlying `rdmaxcel_qp_t` via
            // the guard's `Drop` rather than leaking it.
            let mut qp = QpGuard::new(
                IbvQueuePair::new(domain.context, domain.pd, self.state.config().clone())
                    .map_err(|e| anyhow::anyhow!("could not create IbvQueuePair: {}", e))?,
            );
            let info = qp
                .get_qp_info()
                .map_err(|e| anyhow::anyhow!("could not extract QP info: {}", e))?;
            let initializer =
                QueuePairInitializer::new(Instance::handle(cx), other, qp_key.clone(), qp)
                    .spawn(cx)?;
            self.state.qps().insert(
                qp_key.clone(),
                QpState::Pending {
                    info,
                    initializer,
                    waiters: Vec::new(),
                },
            );
        }
        Ok(self
            .state
            .qps()
            .get_mut(qp_key)
            .expect("entry just inserted or pre-existing"))
    }
}

#[async_trait]
#[hyperactor::handle(IbvManagerMessage)]
impl IbvManagerMessageHandler for IbvManagerActor {
    async fn release_buffer(
        &mut self,
        _cx: &Context<Self>,
        remote_buf_id: usize,
    ) -> Result<(), anyhow::Error> {
        // Drop the entry; the [`Arc<IbvMemoryRegion>`] decrement deregisters
        // the MR via [`IbvMemoryRegion::drop`] when no other holder remains.
        self.buffer_registrations.remove(&remote_buf_id);
        Ok(())
    }
}

#[async_trait]
impl Handler<EnsureQueuePair<IbvManagerActor>> for IbvManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: EnsureQueuePair<IbvManagerActor>,
    ) -> Result<(), anyhow::Error> {
        let EnsureQueuePair {
            sender,
            sender_device,
            receiver_device,
            reply,
        } = msg;
        let qp_key = QpKey {
            self_device: receiver_device,
            other_id: sender.actor_addr().id().clone(),
            other_device: sender_device,
        };
        let state = match self.ensure_queue_pair_impl(cx, sender, &qp_key) {
            Ok(state) => state,
            Err(e) => {
                reply.send(cx, PeerInfo(Err(e.to_string())))?;
                return Ok(());
            }
        };
        match state.value() {
            QpState::Pending {
                info, initializer, ..
            } => {
                let notify_rts = initializer.bind::<QueuePairInitializer<Self>>().port();
                reply.send(cx, PeerInfo(Ok((info.clone(), notify_rts))))?;
            }
            QpState::Ready(_) => {
                // `Ready` means a prior handshake completed and the
                // initializer was stopped — we can't hand back an
                // initializer ref. Reaching here represents a logic
                // error (peer is asking us to redo a handshake we've
                // already finished); surface it as `Err`.
                reply.send(
                    cx,
                    PeerInfo(Err(format!(
                        "EnsureQueuePair on already-Ready entry {qp_key:?}"
                    ))),
                )?;
            }
            QpState::Failed(error) => {
                reply.send(cx, PeerInfo(Err(error.clone())))?;
            }
        }
        Ok(())
    }
}

#[async_trait]
#[hyperactor::handle(IbvManagerLocalMessage)]
impl IbvManagerLocalMessageHandler for IbvManagerActor {
    async fn register_mr(
        &mut self,
        cx: &Context<Self>,
        addr: usize,
        size: usize,
    ) -> Result<Result<IbvMemoryRegionView, String>, anyhow::Error> {
        Ok(self
            .register_mr_impl(cx, addr, size)
            .map_err(|e| e.to_string()))
    }

    async fn register_remote_buffer(
        &mut self,
        cx: &Context<Self>,
        remote_buf_id: usize,
        local: Arc<dyn RdmaLocalMemory>,
    ) -> Result<Result<IbvBuffer, String>, anyhow::Error> {
        if let Some((buf, _)) = self.buffer_registrations.get(&remote_buf_id) {
            return Ok(Ok(buf.clone()));
        }
        // Remote holders expect the buffer to survive until
        // `release_buffer`, so park the view in `buffer_registrations`
        // for the duration.
        let mrv = match self.register_mr_impl(cx, local.addr(), local.size()) {
            Ok(mrv) => mrv,
            Err(e) => return Ok(Err(e.to_string())),
        };
        let buf = IbvBuffer {
            mr_id: mrv.id,
            lkey: mrv.lkey,
            rkey: mrv.rkey,
            addr: mrv.rdma_addr,
            size: mrv.size,
            device_name: mrv.device_name.clone(),
        };
        self.buffer_registrations
            .insert(remote_buf_id, (buf.clone(), mrv));
        Ok(Ok(buf))
    }

    async fn request_queue_pair(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        reply: OncePortHandle<Result<IbvQueuePair, String>>,
    ) -> Result<(), anyhow::Error> {
        let qp_key = QpKey {
            self_device,
            other_id: other.actor_addr().id().clone(),
            other_device,
        };
        let mut state = match self.ensure_queue_pair_impl(cx, other, &qp_key) {
            Ok(state) => state,
            Err(e) => {
                reply.send(cx, Err(e.to_string()))?;
                return Ok(());
            }
        };
        match state.value_mut() {
            QpState::Pending { waiters, .. } => waiters.push(reply),
            QpState::Ready(qp) => reply.send(cx, Ok(qp.clone()))?,
            QpState::Failed(error) => reply.send(cx, Err(error.clone()))?,
        }
        Ok(())
    }

    async fn qp_initializer_done(
        &mut self,
        cx: &Context<Self>,
        qp_key: QpKey,
        qp: QpGuard,
    ) -> Result<(), anyhow::Error> {
        let qp = qp.into_inner();
        // Take the entry out, transition to Ready, drain waiters,
        // then stop the initializer.
        let initializer = match self.state.qps().remove(&qp_key).map(|(_, v)| v) {
            Some(QpState::Pending {
                waiters,
                initializer,
                ..
            }) => {
                for w in waiters {
                    let waiter_dbg = format!("{w:?}");
                    if let Err(e) = w.send(cx, Ok(qp.clone())) {
                        tracing::error!(
                            "qp_initializer_done: failed to deliver to waiter {waiter_dbg} for {qp_key:?}: {e}"
                        );
                    }
                }
                initializer
            }
            other => {
                unreachable!("qp_initializer_done received but state is {other:?}: {qp_key:?}")
            }
        };
        self.state.qps().insert(qp_key.clone(), QpState::Ready(qp));
        initializer.drain_and_stop("QpInitializerDone")?;
        let status = initializer.await;
        if status.is_failed() {
            // The QP itself is already `Ready` and waiters have been
            // drained, so a non-clean initializer shutdown is not
            // user-visible — log and move on rather than crashing
            // the manager.
            tracing::error!(
                "QueuePairInitializer for {qp_key:?} terminated with failure after Done: {status:?}"
            );
        }
        Ok(())
    }

    async fn qp_initializer_failed(
        &mut self,
        cx: &Context<Self>,
        qp_key: QpKey,
        error: String,
    ) -> Result<(), anyhow::Error> {
        let initializer = match self.state.qps().remove(&qp_key).map(|(_, v)| v) {
            Some(QpState::Pending {
                waiters,
                initializer,
                ..
            }) => {
                for w in waiters {
                    let waiter_dbg = format!("{w:?}");
                    if let Err(e) = w.send(cx, Err(error.clone())) {
                        tracing::error!(
                            "qp_initializer_failed: failed to deliver to waiter {waiter_dbg} for {qp_key:?}: {e}"
                        );
                    }
                }
                initializer
            }
            other => {
                unreachable!("qp_initializer_failed received but state is {other:?}: {qp_key:?}")
            }
        };
        // Tombstone the entry: subsequent `RequestQueuePair` calls
        // for the same key surface the same error rather than
        // retrying or hanging. TODO: add recovery.
        self.state
            .qps()
            .insert(qp_key.clone(), QpState::Failed(error));
        initializer.drain_and_stop("QpInitializerFailed")?;
        let status = initializer.await;
        if status.is_failed() {
            tracing::error!(
                "QueuePairInitializer for {qp_key:?} terminated with failure after Failed: {status:?}"
            );
        }
        Ok(())
    }
}

/// Free helper around [`IbvManagerLocalMessage::RequestQueuePair`] — opens
/// a `OncePortHandle` for the reply, sends the message, and awaits the
/// answer. Exists because `RequestQueuePair` doesn't use `#[reply]`
/// (the handler may park the port until the QP becomes `Ready`), so
/// the auto-derived client method only does fire-and-forget.
pub(super) async fn request_queue_pair(
    actor: &ActorHandle<IbvManagerActor>,
    cx: &(impl hyperactor::context::Actor + Send + Sync),
    other: ActorRef<IbvManagerActor>,
    self_device: String,
    other_device: String,
) -> Result<Result<IbvQueuePair, String>, anyhow::Error> {
    let (reply, rx) = cx
        .mailbox()
        .open_once_port::<Result<IbvQueuePair, String>>();
    actor
        .request_queue_pair(cx, other, self_device, other_device, reply)
        .await?;
    rx.recv()
        .await
        .map_err(|e| anyhow::anyhow!("request_queue_pair port closed: {e}"))
}

/// Wrapper around [`ActorHandle<IbvManagerActor>`] that moves the RDMA
/// data-plane (post send/recv, poll CQ) off the actor loop while keeping
/// state-mutating operations (MR registration/deregistration, QP management)
/// serialized through actor messages.
#[derive(Debug, Clone)]
pub struct IbvBackend(pub ActorHandle<IbvManagerActor>);

impl std::ops::Deref for IbvBackend {
    type Target = ActorHandle<IbvManagerActor>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IbvBackend {
    /// Waits for the completion of RDMA operations.
    ///
    /// Polls the completion queue until all specified work requests complete
    /// or until the timeout is reached. Pure CQ polling — no actor state needed.
    async fn wait_for_completion(
        local_buf: &IbvBuffer,
        qp: &mut IbvQueuePair,
        poll_target: PollTarget,
        expected_wr_ids: &[u64],
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let start_time = std::time::Instant::now();

        let mut remaining: std::collections::HashSet<u64> =
            expected_wr_ids.iter().copied().collect();
        let mut poll_policy = PollSleepPolicy::new();

        while start_time.elapsed() < timeout {
            if remaining.is_empty() {
                return Ok(());
            }

            let wr_ids_to_poll: Vec<u64> = remaining.iter().copied().collect();
            match qp.poll_completion(poll_target, &wr_ids_to_poll) {
                Ok(completions) => {
                    for (wr_id, _wc) in completions {
                        remaining.remove(&wr_id);
                    }
                    if remaining.is_empty() {
                        return Ok(());
                    }
                    poll_policy.yield_now().await;
                }
                Err(e) => {
                    // When the returned error is WR_FLUSH_ERR, which is generally a
                    // secondary error, drain the remaining completions to find the
                    // original root cause error. WR_FLUSH_ERR means the QP entered
                    // error state due to a DIFFERENT WR's failure, so the actual root
                    // cause may be cached or still in the CQ.
                    let mut root_cause: Option<PollCompletionError> = None;
                    if e.is_wr_flush_err() {
                        for &wr_id in &wr_ids_to_poll {
                            if let Err(inner_err) = qp.poll_completion(poll_target, &[wr_id]) {
                                if !inner_err.is_wr_flush_err() {
                                    root_cause = Some(inner_err);
                                    break;
                                }
                            }
                        }
                    }
                    let error_detail = if let Some(cause) = root_cause {
                        format!(
                            "RDMA polling completion failed: {} (root cause: {})",
                            e, cause
                        )
                    } else {
                        format!("RDMA polling completion failed: {}", e)
                    };
                    return Err(anyhow::anyhow!(
                        "{} [lkey={}, rkey={}, addr=0x{:x}, size={}]",
                        error_detail,
                        local_buf.lkey,
                        local_buf.rkey,
                        local_buf.addr,
                        local_buf.size
                    ));
                }
            }
        }
        tracing::error!(
            "timed out while waiting on request completion for wr_ids={:?}",
            remaining
        );
        Err(anyhow::anyhow!(
            "[ibv_buffer({:?})] rdma operation did not complete in time (expected wr_ids={:?})",
            local_buf,
            expected_wr_ids
        ))
    }

    /// Core submit logic: registers local MR via actor message, resolves remote
    /// IbvBuffer lazily, executes the op locally, and deregisters local MR.
    async fn execute_op(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        op: IbvOp,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        // Register the local memory via actor message. The returned
        // view's `Arc<IbvMemoryRegion>` keeps the MR alive for the
        // duration of this scope; dropping it deregisters
        // automatically — no follow-up message required.
        let local_mrv = self
            .register_mr(cx, op.local_memory.addr(), op.local_memory.size())
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;

        let local_buffer = IbvBuffer {
            mr_id: local_mrv.id,
            lkey: local_mrv.lkey,
            rkey: local_mrv.rkey,
            addr: local_mrv.rdma_addr,
            size: local_mrv.size,
            device_name: local_mrv.device_name.clone(),
        };

        let mut qp = request_queue_pair(
            &self.0,
            cx,
            op.remote_manager.clone(),
            local_buffer.device_name.clone(),
            op.remote_buffer.device_name.clone(),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;

        let wr_id = match op.op_type {
            RdmaOpType::WriteFromLocal => qp.put(local_buffer.clone(), op.remote_buffer)?,
            RdmaOpType::ReadIntoLocal => qp.get(local_buffer.clone(), op.remote_buffer)?,
        };

        Self::wait_for_completion(&local_buffer, &mut qp, PollTarget::Send, &wr_id, timeout).await
    }
}

#[async_trait]
impl RdmaBackend for IbvBackend {
    type TransportInfo = ();

    /// Submit a batch of RDMA operations.
    ///
    /// Resolves ibv ops, then executes each directly — registering MRs
    /// via actor messages while performing QP put/get and CQ
    /// polling locally.
    async fn submit(
        &mut self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let mut ibv_ops = Vec::with_capacity(ops.len());
        for op in ops {
            let (remote_manager, remote_buffer) = op.remote.resolve_ibv().ok_or_else(|| {
                anyhow::anyhow!("ibverbs backend not found for buffer: {:?}", op.remote)
            })?;
            ibv_ops.push(IbvOp {
                op_type: op.op_type,
                local_memory: op.local.clone(),
                remote_buffer,
                remote_manager,
            });
        }

        let deadline = Instant::now() + timeout;
        for op in ibv_ops {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(anyhow::anyhow!("submit timed out"));
            }
            self.execute_op(cx, op, remaining).await?;
        }
        Ok(())
    }

    fn transport_level(&self) -> RdmaTransportLevel {
        RdmaTransportLevel::Nic
    }

    fn transport_info(&self) -> Option<Self::TransportInfo> {
        None
    }
}
