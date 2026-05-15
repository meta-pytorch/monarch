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
//! initializer, waiter }` while the handshake runs, `Ready(qp)`
//! once this side is RTS and has observed the peer's RTS,
//! `Consumed` after the processor (or a test) has claimed the QP
//! (`IbvQueuePair` is single-owner, so it can only be handed out
//! once), or `Failed(error)` as a tombstone after a fatal error.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
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

use super::IbvBuffer;
use super::domain::IbvDomain;
use super::primitives::IbvConfig;
use super::primitives::IbvDevice;
use super::primitives::IbvMemoryRegion;
use super::primitives::IbvMemoryRegionView;
use super::primitives::IbvQpInfo;
use super::primitives::ibverbs_supported;
use super::primitives::mlx5dv_supported;
use super::primitives::resolve_qp_type;
use super::processor_actor::IbvProcessorActor;
use super::processor_actor::RegisterMr;
use super::processor_actor::RequestQueuePair;
use super::processor_actor::SubmitOps;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::PeerInfo;
use super::queue_pair::QpKey;
use super::queue_pair::QueuePairInitializer;
use crate::RdmaOp;
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
/// `Pending` covers the entire handshake (an initializer is
/// running); `Ready` holds the freshly-handshaken QP until the
/// processor claims it; `Consumed` is the post-claim tombstone;
/// `Failed` is a fatal-error tombstone that surfaces the original
/// error to later `RequestQueuePair` / `EnsureQueuePair` callers.
///
/// TODO: add recovery — allow retries via an explicit message or
/// after a backoff. For now `Failed` (and `Consumed`) entries stay
/// for the life of the manager.
#[derive(Debug)]
enum QpState {
    Pending {
        /// Local endpoint, captured when the QP was first created so
        /// repeated `EnsureQueuePair` calls don't have to re-extract it.
        info: IbvQpInfo,
        /// Child actor driving the handshake. Stopped on
        /// [`QpInitializerDone`]/[`QpInitializerFailed`].
        initializer: ActorHandle<QueuePairInitializer<IbvManagerActor>>,
        /// Single parked [`RequestQueuePair`] caller. The QP is
        /// single-owner so only one request can ever be satisfied;
        /// a second concurrent request arriving while this slot is
        /// occupied is rejected immediately with a clear error.
        waiter: Option<OncePortHandle<Result<IbvQueuePair, String>>>,
    },
    Ready(IbvQueuePair),
    /// The processor has taken ownership of this QP. The manager
    /// keeps the tombstone so a duplicate `RequestQueuePair` or a
    /// late `EnsureQueuePair` for the same key surfaces a clear
    /// error rather than silently recreating the QP.
    Consumed,
    Failed(String),
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
}

/// Local-only handshake-success report. The initializer sends this
/// to its owning manager once both sides have reached RTS, handing
/// over the freshly-connected [`IbvQueuePair`].
#[derive(Debug)]
pub(super) struct QpInitializerDone {
    pub(super) qp_key: QpKey,
    pub(super) qp: IbvQueuePair,
}

/// Local-only handshake-failure report. The initializer sends this
/// to its owning manager when the handshake aborted; the underlying
/// QP has already been dropped on the initializer side.
#[derive(Debug)]
pub(super) struct QpInitializerFailed {
    pub(super) qp_key: QpKey,
    pub(super) error: String,
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
                rdma_addr,
                size,
                lkey: segment.lkey,
                rkey: segment.rkey,
            });
        }
    }
    None
}

/// Result of a successful [`lookup_segment_for_address`] hit. Just
/// the device-derived facts about the matched mkey; the caller
/// composes these with whatever provenance it needs (mrv id,
/// device name, refcounted owners) when materializing an
/// [`IbvMemoryRegionView`].
#[derive(Debug)]
pub(super) struct SegmentInfo {
    pub(super) rdma_addr: usize,
    pub(super) size: usize,
    pub(super) lkey: u32,
    pub(super) rkey: u32,
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

    /// Child data-path actor. Spawned in [`Actor::init`]. The
    /// manager forwards [`SubmitOps`] from [`IbvBackend`] to it,
    /// and the processor sends `RegisterMr` / `RequestQueuePair`
    /// back to the manager as it resolves MRs and peer QPs on the
    /// data path.
    processor: OnceLock<ActorHandle<IbvProcessorActor<IbvManagerActor, IbvQueuePair>>>,

    /// Per-QP state, keyed from this manager's perspective. See [`QpKey`].
    qps: HashMap<QpKey, QpState>,

    /// Per-device protection domains. Created lazily when memory is
    /// registered or a peer QP is requested for the device. The
    /// `Arc<IbvDomain>` is cloned into every `IbvMemoryRegionView`
    /// and `IbvQueuePair` the manager hands out, so the PD outlives
    /// the resources that reference it.
    device_domains: HashMap<String, Arc<IbvDomain>>,

    /// Per-device loopback QPs used by the mlx5dv segment scanner.
    /// EFA / non-mlx5dv devices have no entry. Owned by the manager
    /// for the lifetime of the device.
    loopback_qps: HashMap<String, IbvQueuePair>,

    config: IbvConfig,

    mlx5dv_enabled: bool,

    /// Singleton Arc owning the CUDA segment scanner state. `Some`
    /// once `register_segments` succeeds; cloned into every
    /// segment-backed view. `deregister_segments` runs from the
    /// `IbvMemoryRegion::Segments` Drop when the last reference goes
    /// away.
    segments_mr: Option<Arc<IbvMemoryRegion>>,

    /// Id for next mrv created.
    mrv_id: usize,

    /// Map from buffer_id to the buffer's `(IbvBuffer, view)`. The
    /// view keeps the MR (and its PD) alive for the lifetime of the
    /// registration; `ReleaseBuffer` drops the entry, and the FFI
    /// resources are released by the `Arc`s' `Drop`s once no other
    /// holder of those views remains.
    buffer_registrations: HashMap<usize, (IbvBuffer, IbvMemoryRegionView)>,
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
        let mr_lru_capacity = hyperactor_config::global::get(crate::config::RDMA_MR_LRU_CACHE_SIZE);
        let processor = IbvProcessorActor::<IbvManagerActor, IbvQueuePair>::new(
            Instance::handle(this),
            self.config.clone(),
            mr_lru_capacity,
        );
        let processor_handle = this.spawn(processor)?;
        self.processor
            .set(processor_handle)
            .expect("processor should only be set once during init");
        Ok(())
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

        let mlx5dv_enabled = resolve_qp_type(config.qp_type) == rdmaxcel_sys::RDMA_QP_TYPE_MLX5DV;

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
            processor: OnceLock::new(),
            qps: HashMap::new(),
            device_domains: HashMap::new(),
            loopback_qps: HashMap::new(),
            config,
            mlx5dv_enabled,
            segments_mr: None,
            mrv_id: 0,
            buffer_registrations: HashMap::new(),
        };

        Ok(actor)
    }

    /// Get or create a domain and loopback QP for the specified RDMA
    /// device. Returns the shared `Arc<IbvDomain>`; the loopback QP
    /// (when applicable) lives in [`Self::loopback_qps`] and is only
    /// inspected by `build_per_device_pd_qp_arrays`.
    fn get_or_create_device_domain(
        &mut self,
        device_name: &str,
        rdma_device: &IbvDevice,
    ) -> Result<Arc<IbvDomain>, anyhow::Error> {
        if let Some(domain) = self.device_domains.get(device_name) {
            return Ok(Arc::clone(domain));
        }

        // Create new domain for this device
        let domain = Arc::new(IbvDomain::new(rdma_device.clone()).map_err(|e| {
            anyhow::anyhow!("could not create domain for device {}: {}", device_name, e)
        })?);

        // Print device info if MONARCH_DEBUG_RDMA=1 is set (before initial QP creation)
        crate::print_device_info_if_debug_enabled(domain.context);

        // Create loopback QP for this domain if mlx5dv is supported (needed for segment registration)
        // For EFA, we don't need a loopback QP for segment scanning
        if mlx5dv_supported() && !crate::efa::is_efa_device() {
            let mut qp =
                IbvQueuePair::new(Arc::clone(&domain), self.config.clone()).map_err(|e| {
                    anyhow::anyhow!(
                        "could not create loopback QP for device {}: {}",
                        device_name,
                        e
                    )
                })?;

            // Get connection info and connect to itself
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

            self.loopback_qps.insert(device_name.to_string(), qp);
        }

        self.device_domains
            .insert(device_name.to_string(), Arc::clone(&domain));
        Ok(domain)
    }

    /// Build parallel PD/QP arrays indexed by CUDA device ordinal
    /// for the C++ register_segments call.
    fn build_per_device_pd_qp_arrays(
        &self,
    ) -> (
        Vec<*mut rdmaxcel_sys::ibv_pd>,
        Vec<*mut rdmaxcel_sys::rdmaxcel_qp_t>,
    ) {
        let cuda_map = super::device_selection::get_cuda_device_to_ibv_device();
        let mut pds = Vec::with_capacity(cuda_map.len());
        let mut qps = Vec::with_capacity(cuda_map.len());
        for maybe_device in cuda_map {
            if let Some(device) = maybe_device {
                if let Some(domain) = self.device_domains.get(device.name()) {
                    pds.push(domain.pd);
                    qps.push(
                        self.loopback_qps
                            .get(device.name())
                            .map(|q| q.qp as *mut rdmaxcel_sys::rdmaxcel_qp_t)
                            .unwrap_or(std::ptr::null_mut()),
                    );
                } else {
                    pds.push(std::ptr::null_mut());
                    qps.push(std::ptr::null_mut());
                }
            } else {
                pds.push(std::ptr::null_mut());
                qps.push(std::ptr::null_mut());
            }
        }
        (pds, qps)
    }

    fn find_cuda_segment_for_address(
        &self,
        addr: usize,
        size: usize,
        pd: *mut rdmaxcel_sys::ibv_pd,
    ) -> Option<SegmentInfo> {
        lookup_segment_for_address(&get_registered_cuda_segments(pd), addr, size)
    }

    fn register_mr_impl(
        &mut self,
        addr: usize,
        size: usize,
    ) -> Result<(IbvMemoryRegionView, String), anyhow::Error> {
        unsafe {
            let mut mem_type: i32 = 0;
            let ptr = addr as rdmaxcel_sys::CUdeviceptr;
            let err = rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
                &mut mem_type as *mut _ as *mut std::ffi::c_void,
                rdmaxcel_sys::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                ptr,
            );
            let is_cuda = err == rdmaxcel_sys::CUDA_SUCCESS;

            let mut selected_rdma_device = None;

            if is_cuda {
                // Get device ordinal from the CUDA pointer
                let mut device_ordinal: i32 = -1;
                let err = rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
                    &mut device_ordinal as *mut _ as *mut std::ffi::c_void,
                    rdmaxcel_sys::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
                    ptr,
                );
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
                self.config.device.clone()
            };

            let device_name = rdma_device.name().clone();
            tracing::debug!(
                "Using RDMA device: {} for memory at 0x{:x}",
                device_name,
                addr
            );

            // Get or create the protection domain for this device.
            // The loopback QP (when applicable) lives in
            // `self.loopback_qps` and is consulted lazily by
            // `build_per_device_pd_qp_arrays` below.
            let domain = self.get_or_create_device_domain(&device_name, &rdma_device)?;

            let access = if crate::efa::is_efa_device() {
                crate::efa::mr_access_flags()
            } else {
                rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                    | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                    | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                    | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC
            };

            let mrv;

            if is_cuda {
                // First, try to use segment scanning if mlx5dv is enabled
                let mut segment_info = None;
                if self.mlx5dv_enabled {
                    // Try to find in already registered segments
                    segment_info = self.find_cuda_segment_for_address(addr, size, domain.pd);

                    // If not found, trigger a re-sync with the allocator and retry
                    if segment_info.is_none() {
                        let (mut pds, mut qps) = self.build_per_device_pd_qp_arrays();
                        let err = rdmaxcel_sys::register_segments(
                            pds.as_mut_ptr(),
                            qps.as_mut_ptr(),
                            pds.len() as i32,
                            self.config.max_sge_override,
                        );
                        // Only retry if register_segments succeeded
                        // If it fails (e.g., scanner returns 0 segments), we'll fall back to dmabuf
                        if err == 0 {
                            // The scanner just registered (or
                            // re-synced) global segment state. Lazily
                            // install the singleton `segments_mr` now,
                            // independent of whether *this* address
                            // matches a segment. Without this, a
                            // subsequent retry that doesn't find a
                            // segment would leak the newly-registered
                            // global state on manager teardown.
                            self.segments_mr
                                .get_or_insert_with(|| Arc::new(IbvMemoryRegion::Segments));
                            segment_info =
                                self.find_cuda_segment_for_address(addr, size, domain.pd);
                        }
                    }
                }

                // Use segment if found, otherwise fall back to direct dmabuf registration
                if let Some(info) = segment_info {
                    let segments_mr = Arc::clone(
                        self.segments_mr
                            .get_or_insert_with(|| Arc::new(IbvMemoryRegion::Segments)),
                    );
                    let id = self.mrv_id;
                    self.mrv_id += 1;
                    mrv = IbvMemoryRegionView::new(
                        id,
                        addr,
                        info.rdma_addr,
                        info.size,
                        info.lkey,
                        info.rkey,
                        device_name.clone(),
                        segments_mr,
                    );
                } else {
                    // Dmabuf path: used when mlx5dv is disabled OR scanner returns no segments
                    let mut fd: i32 = -1;
                    let cu_err = rdmaxcel_sys::rdmaxcel_cuMemGetHandleForAddressRange(
                        &mut fd,
                        addr as rdmaxcel_sys::CUdeviceptr,
                        size,
                        rdmaxcel_sys::CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                        0,
                    );
                    if cu_err != rdmaxcel_sys::CUDA_SUCCESS || fd < 0 {
                        return Err(anyhow::anyhow!(
                            "failed to get dmabuf handle for CUDA memory (addr: 0x{:x}, size: {}, cu_err: {}, fd: {})",
                            addr,
                            size,
                            cu_err,
                            fd
                        ));
                    }
                    let mr =
                        rdmaxcel_sys::ibv_reg_dmabuf_mr(domain.pd, 0, size, 0, fd, access.0 as i32);
                    if mr.is_null() {
                        return Err(anyhow::anyhow!("Failed to register dmabuf MR"));
                    }
                    let id = self.mrv_id;
                    self.mrv_id += 1;
                    mrv = IbvMemoryRegionView::new(
                        id,
                        addr,
                        (*mr).addr as usize,
                        size,
                        (*mr).lkey,
                        (*mr).rkey,
                        device_name.clone(),
                        Arc::new(IbvMemoryRegion::Direct {
                            mr,
                            _domain: Arc::clone(&domain),
                        }),
                    );
                }
            } else {
                // CPU memory path
                let mr = rdmaxcel_sys::ibv_reg_mr(
                    domain.pd,
                    addr as *mut std::ffi::c_void,
                    size,
                    access.0 as i32,
                );

                if mr.is_null() {
                    return Err(anyhow::anyhow!("failed to register standard MR"));
                }

                let id = self.mrv_id;
                self.mrv_id += 1;
                mrv = IbvMemoryRegionView::new(
                    id,
                    addr,
                    (*mr).addr as usize,
                    size,
                    (*mr).lkey,
                    (*mr).rkey,
                    device_name.clone(),
                    Arc::new(IbvMemoryRegion::Direct {
                        mr,
                        _domain: Arc::clone(&domain),
                    }),
                );
            }
            Ok((mrv, device_name))
        }
    }

    /// Lazy QP creation: if `qp_key` is absent, create the local
    /// `IbvQueuePair`, capture its `IbvQpInfo`, and spawn a
    /// `QueuePairInitializer` to drive the handshake. Returns the
    /// `QpState` entry — either the freshly-inserted `Pending` one,
    /// or the existing `Pending`/`Ready`/`Consumed`/`Failed`.
    fn ensure_queue_pair_impl(
        &mut self,
        cx: &Context<'_, Self>,
        other: ActorRef<IbvManagerActor>,
        qp_key: &QpKey,
    ) -> Result<&mut QpState, anyhow::Error> {
        if !self.qps.contains_key(qp_key) {
            let self_device = &qp_key.self_device;
            let rdma_device = super::primitives::get_all_devices()
                .into_iter()
                .find(|d| d.name() == self_device)
                .ok_or_else(|| anyhow::anyhow!("RDMA device '{}' not found", self_device))?;
            let domain = self.get_or_create_device_domain(self_device, &rdma_device)?;
            // The freshly-created QP is moved straight into the
            // initializer; any early-return path below (e.g.
            // `get_qp_info` failing) drops it, and
            // `IbvQueuePair::Drop` destroys the underlying
            // `rdmaxcel_qp_t`.
            let mut qp = IbvQueuePair::new(domain, self.config.clone())
                .map_err(|e| anyhow::anyhow!("could not create IbvQueuePair: {}", e))?;
            let info = qp
                .get_qp_info()
                .map_err(|e| anyhow::anyhow!("could not extract QP info: {}", e))?;
            let initializer =
                QueuePairInitializer::new(Instance::handle(cx), other, qp_key.clone(), qp)
                    .spawn(cx)?;
            self.qps.insert(
                qp_key.clone(),
                QpState::Pending {
                    info,
                    initializer,
                    waiter: None,
                },
            );
        }
        Ok(self
            .qps
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
        // Dropping the entry releases the manager's `Arc` clones on
        // the view's MR and PD; FFI cleanup happens via their `Drop`s
        // once the last referencing view is gone.
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
        match state {
            QpState::Pending {
                info, initializer, ..
            } => {
                let notify_rts = initializer.bind::<QueuePairInitializer<Self>>().port();
                reply.send(cx, PeerInfo(Ok((info.clone(), notify_rts))))?;
            }
            QpState::Ready(_) | QpState::Consumed => {
                // The handshake for this key already completed (and
                // the initializer was stopped, so we can't hand back
                // a fresh `NotifyRts` port). Reaching here represents
                // a peer asking us to redo work that's already done;
                // surface it as `Err` rather than silently
                // recreating the QP.
                reply.send(
                    cx,
                    PeerInfo(Err(format!(
                        "EnsureQueuePair on already-completed entry {qp_key:?}"
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
    async fn register_remote_buffer(
        &mut self,
        _cx: &Context<Self>,
        remote_buf_id: usize,
        local: Arc<dyn RdmaLocalMemory>,
    ) -> Result<Result<IbvBuffer, String>, anyhow::Error> {
        if let Some((buf, _)) = self.buffer_registrations.get(&remote_buf_id) {
            return Ok(Ok(buf.clone()));
        }
        let (mrv, device_name) = match self.register_mr_impl(local.addr(), local.size()) {
            Ok(v) => v,
            Err(e) => return Ok(Err(e.to_string())),
        };
        let buf = IbvBuffer {
            mr_id: mrv.id,
            lkey: mrv.lkey,
            rkey: mrv.rkey,
            addr: mrv.rdma_addr,
            size: mrv.size,
            device_name,
        };
        self.buffer_registrations
            .insert(remote_buf_id, (buf.clone(), mrv));
        Ok(Ok(buf))
    }
}

#[async_trait]
impl Handler<QpInitializerDone> for IbvManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: QpInitializerDone,
    ) -> Result<(), anyhow::Error> {
        let QpInitializerDone { qp_key, qp } = msg;
        // Transition `Pending → Ready`, handing the QP to the parked
        // waiter (if any) and tombstoning as `Consumed`. Otherwise
        // leave the QP in `Ready` so the next `RequestQueuePair`
        // claims it.
        let (initializer, waiter) = match self.qps.remove(&qp_key) {
            Some(QpState::Pending {
                waiter,
                initializer,
                ..
            }) => (initializer, waiter),
            other => {
                unreachable!("QpInitializerDone received but state is {other:?}: {qp_key:?}")
            }
        };
        if let Some(w) = waiter {
            let waiter_dbg = format!("{w:?}");
            if let Err(e) = w.send(cx, Ok(qp)) {
                tracing::error!(
                    "QpInitializerDone: failed to deliver to waiter {waiter_dbg} for {qp_key:?}: {e}"
                );
            }
            self.qps.insert(qp_key.clone(), QpState::Consumed);
        } else {
            self.qps.insert(qp_key.clone(), QpState::Ready(qp));
        }
        initializer.drain_and_stop("QpInitializerDone")?;
        let status = initializer.await;
        if status.is_failed() {
            // The QP has been handed off (or is parked in `Ready`)
            // and the primary waiter has been answered, so a
            // non-clean initializer shutdown is not user-visible —
            // log and move on rather than crashing the manager.
            tracing::error!(
                "QueuePairInitializer for {qp_key:?} terminated with failure after Done: {status:?}"
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<QpInitializerFailed> for IbvManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: QpInitializerFailed,
    ) -> Result<(), anyhow::Error> {
        let QpInitializerFailed { qp_key, error } = msg;
        let initializer = match self.qps.remove(&qp_key) {
            Some(QpState::Pending {
                waiter,
                initializer,
                ..
            }) => {
                if let Some(w) = waiter {
                    let waiter_dbg = format!("{w:?}");
                    if let Err(e) = w.send(cx, Err(error.clone())) {
                        tracing::error!(
                            "QpInitializerFailed: failed to deliver to waiter {waiter_dbg} for {qp_key:?}: {e}"
                        );
                    }
                }
                initializer
            }
            other => {
                unreachable!("QpInitializerFailed received but state is {other:?}: {qp_key:?}")
            }
        };
        // Tombstone the entry: subsequent `RequestQueuePair` calls
        // for the same key surface the same error rather than
        // retrying or hanging. TODO: add recovery.
        self.qps.insert(qp_key.clone(), QpState::Failed(error));
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

#[async_trait]
impl Handler<SubmitOps<IbvManagerActor>> for IbvManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: SubmitOps<IbvManagerActor>,
    ) -> Result<(), anyhow::Error> {
        // Forward straight to the processor. `SubmitOps` carries the
        // caller's reply port, so the processor's response goes
        // back to the original caller without bouncing through the
        // manager again.
        self.processor
            .get()
            .expect("processor spawned during Actor::init")
            .send(cx, msg)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<RegisterMr> for IbvManagerActor {
    async fn handle(&mut self, cx: &Context<Self>, msg: RegisterMr) -> Result<(), anyhow::Error> {
        let RegisterMr { addr, size, reply } = msg;
        let result = self
            .register_mr_impl(addr, size)
            .map(|(mrv, _device_name)| mrv)
            .map_err(|e| e.to_string());
        reply.send(cx, result)?;
        Ok(())
    }
}

/// Send a [`RequestQueuePair`] to the manager and await the QP it
/// hands back.
///
/// Note: a successful return *transfers ownership* of the
/// underlying QP to the caller — the manager tombstones the entry
/// as [`QpState::Consumed`] and any later request for the same key
/// fails.
pub async fn request_queue_pair(
    actor: &ActorHandle<IbvManagerActor>,
    cx: &(impl hyperactor::context::Actor + Send + Sync),
    other: ActorRef<IbvManagerActor>,
    self_device: String,
    other_device: String,
) -> Result<Result<IbvQueuePair, String>, anyhow::Error> {
    let qp_key = QpKey {
        self_device,
        other_id: other.actor_addr().id().clone(),
        other_device,
    };
    let (reply, rx) = cx
        .mailbox()
        .open_once_port::<Result<IbvQueuePair, String>>();
    actor.send(
        cx,
        RequestQueuePair {
            qp_key,
            remote_manager: other,
            reply,
        },
    )?;
    rx.recv()
        .await
        .map_err(|e| anyhow::anyhow!("request_queue_pair port closed: {e}"))
}

#[async_trait]
impl Handler<RequestQueuePair<IbvManagerActor, IbvQueuePair>> for IbvManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: RequestQueuePair<IbvManagerActor, IbvQueuePair>,
    ) -> Result<(), anyhow::Error> {
        let RequestQueuePair {
            qp_key,
            remote_manager,
            reply,
        } = msg;
        let state = match self.ensure_queue_pair_impl(cx, remote_manager, &qp_key) {
            Ok(state) => state,
            Err(e) => {
                reply.send(cx, Err(e.to_string()))?;
                return Ok(());
            }
        };
        match state {
            QpState::Pending { waiter, .. } => {
                if waiter.is_some() {
                    // The (single-owner) QP can satisfy exactly one
                    // request. Reject any concurrent caller right
                    // away rather than letting them queue behind a
                    // request whose handle they will never receive.
                    reply.send(
                        cx,
                        Err(format!(
                            "RequestQueuePair for {qp_key:?}: another request is already pending"
                        )),
                    )?;
                } else {
                    *waiter = Some(reply);
                }
            }
            QpState::Ready(_) => {
                // Take the QP out, hand it to the caller, and
                // tombstone the entry as `Consumed`.
                let qp = match self.qps.insert(qp_key.clone(), QpState::Consumed) {
                    Some(QpState::Ready(qp)) => qp,
                    _ => unreachable!("just matched Ready"),
                };
                reply.send(cx, Ok(qp))?;
            }
            QpState::Consumed => {
                reply.send(
                    cx,
                    Err(format!(
                        "RequestQueuePair for {qp_key:?} but QP has already been claimed"
                    )),
                )?;
            }
            QpState::Failed(error) => reply.send(cx, Err(error.clone()))?,
        }
        Ok(())
    }
}

/// Wrapper around [`ActorHandle<IbvManagerActor>`] for the ibverbs
/// data plane. `submit` sends a [`SubmitOps`] to the manager, which
/// forwards it to its private [`IbvProcessorActor`] child.
#[derive(Debug, Clone)]
pub struct IbvBackend(pub ActorHandle<IbvManagerActor>);

impl std::ops::Deref for IbvBackend {
    type Target = ActorHandle<IbvManagerActor>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl RdmaBackend for IbvBackend {
    type TransportInfo = ();

    /// Submit a batch of RDMA operations via the manager's data-path
    /// child actor. On any per-op failure the returned `Err`
    /// enumerates every failed op (with its original batch index)
    /// and its underlying error message.
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
            ibv_ops.push(super::IbvOp {
                op_type: op.op_type,
                local_memory: op.local.clone(),
                remote_buffer,
                remote_manager,
            });
        }

        let (reply, rx) = cx.mailbox().open_once_port::<Vec<Result<(), String>>>();
        self.0.send(
            cx,
            SubmitOps {
                ops: ibv_ops,
                timeout,
                reply,
            },
        )?;
        let results = rx
            .recv()
            .await
            .map_err(|e| anyhow::anyhow!("SubmitOps reply port closed: {e}"))?;
        let total = results.len();
        let failures: Vec<String> = results
            .into_iter()
            .enumerate()
            .filter_map(|(i, res)| res.err().map(|e| format!("op {i}: {e}")))
            .collect();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "{} of {} submitted ibverbs ops failed:\n  {}",
                failures.len(),
                total,
                failures.join("\n  ")
            ))
        }
    }

    fn transport_level(&self) -> RdmaTransportLevel {
        RdmaTransportLevel::Nic
    }

    fn transport_info(&self) -> Option<Self::TransportInfo> {
        None
    }
}
