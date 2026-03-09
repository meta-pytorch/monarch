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
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use futures::lock::Mutex;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use super::IbvBuffer;
use super::IbvOp;
use super::domain::IbvDomain;
use super::primitives::IbvConfig;
use super::primitives::IbvDevice;
use super::primitives::IbvMemoryRegionView;
use super::primitives::IbvQpInfo;
use super::primitives::ibverbs_supported;
use super::primitives::mlx5dv_supported;
use super::primitives::resolve_qp_type;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::PollTarget;
use crate::RdmaOp;
use crate::RdmaOpType;
use crate::RdmaTransportLevel;
use crate::backend::RdmaBackend;
use crate::rdma_components::get_registered_cuda_segments;
use crate::rdma_manager_actor::GetIbvActorRefClient;
use crate::rdma_manager_actor::RdmaManagerActor;
use crate::rdma_manager_actor::RdmaManagerMessageClient;
use crate::rdma_manager_actor::get_rdmaxcel_error_message;
use crate::validate_execution_context;

/// Messages handled by [`IbvManagerActor`].
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum IbvManagerMessage {
    /// Register the MR for a buffer identified by `remote_buf_id`. Resolves
    /// the local memory via the parent [`RdmaManagerActor`]'s
    /// `RequestLocalMemory`, registers it as an ibverbs MR, and returns
    /// the resulting [`IbvBuffer`].
    ///
    /// Returns `None` if the buffer has already been released or does not
    /// exist.
    RequestBuffer {
        remote_buf_id: usize,
        #[reply]
        reply: OncePortRef<Option<IbvBuffer>>,
    },
    /// Release a buffer registration by `remote_buf_id`.
    ReleaseBuffer {
        remote_buf_id: usize,
        #[reply]
        reply: OncePortRef<()>,
    },
    RequestQueuePair {
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        reply: OncePortRef<IbvQueuePair>,
    },
    Connect {
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        endpoint: IbvQpInfo,
    },
    InitializeQP {
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        reply: OncePortRef<bool>,
    },
    ConnectionInfo {
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        reply: OncePortRef<IbvQpInfo>,
    },
    ReleaseQueuePair {
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        qp: IbvQueuePair,
    },
    GetQpState {
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        reply: OncePortRef<u32>,
    },
}
wirevalue::register_type!(IbvManagerMessage);

/// Local-only submit message for the [`IbvManagerActor`].
///
/// Not serializable because [`IbvOp`] contains `Arc<dyn RdmaLocalMemory>`.
/// Can only be sent within the same process.
#[derive(Handler, HandleClient, Debug)]
pub struct IbvSubmit {
    pub ops: Vec<IbvOp>,
    pub timeout: Duration,
    #[reply]
    pub reply: OncePortHandle<Result<(), String>>,
}

/// Manages all ibverbs-specific RDMA resources and operations.
///
/// This struct handles memory registration, queue pair management,
/// and connection establishment using the ibverbs API.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        IbvManagerMessage,
    ],
)]
pub struct IbvManagerActor {
    owner: OnceLock<ActorHandle<RdmaManagerActor>>,

    // Nested map: local_device -> (ActorId, remote_device) -> IbvQueuePair
    device_qps: HashMap<String, HashMap<(ActorId, String), IbvQueuePair>>,

    // Track QPs currently being created to prevent duplicate creation
    // Wrapped in Arc<Mutex> to allow safe concurrent access
    pending_qp_creation: Arc<Mutex<HashSet<(String, ActorId, String)>>>,

    // Map of RDMA device names to their domains and loopback QPs
    // Created lazily when memory is registered for a specific device
    device_domains: HashMap<String, (IbvDomain, Option<IbvQueuePair>)>,

    config: IbvConfig,

    mlx5dv_enabled: bool,

    // Map of unique IbvMemoryRegionView to ibv_mr*.  In case of cuda w/ pytorch its -1
    // since its managed independently.  Only used for registration/deregistration purposes
    mr_map: HashMap<usize, usize>,

    // Id for next mrv created
    mrv_id: usize,

    // Map of PCI addresses to their optimal RDMA devices
    // This is populated during actor initialization using the device selection algorithm
    pci_to_device: HashMap<String, IbvDevice>,

    // Map from buffer_id to registration details.
    buffer_registrations: HashMap<usize, IbvBuffer>,
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
        // Helper function to destroy QP resources
        // We can't use Drop on IbvQueuePair because it derives Clone
        // Note: rdmaxcel_qp_destroy handles destroying both the QP and its CQs internally,
        // so we must NOT call ibv_destroy_cq separately (would cause double-free/SIGSEGV)
        fn destroy_queue_pair(qp: &IbvQueuePair, _context: &str) {
            unsafe {
                if qp.qp != 0 {
                    let rdmaxcel_qp = qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
                    rdmaxcel_sys::rdmaxcel_qp_destroy(rdmaxcel_qp);
                }
            }
        }

        // 1. Clean up all queue pairs (both regular and loopback)
        for (_device_name, device_map) in self.device_qps.drain() {
            for ((actor_id, _remote_device), qp) in device_map {
                destroy_queue_pair(&qp, &format!("actor {:?}", actor_id));
            }
        }

        // 2. Clean up device domains (which contain PDs and loopback QPs)
        for (device_name, (domain, qp)) in self.device_domains.drain() {
            if let Some(qp) = qp {
                destroy_queue_pair(&qp, &format!("loopback QP on device {}", device_name));
            }
            drop(domain);
        }

        // 3. Clean up memory regions
        let _mr_count = self.mr_map.len();
        for (id, mr_ptr) in self.mr_map.drain() {
            if mr_ptr != 0 {
                unsafe {
                    let result = rdmaxcel_sys::ibv_dereg_mr(mr_ptr as *mut rdmaxcel_sys::ibv_mr);
                    if result != 0 {
                        tracing::error!(
                            "Failed to deregister MR with id {}: error code {}",
                            id,
                            result
                        );
                    }
                }
            }
        }

        // 4. Deregister all CUDA segments (if using mlx5dv)
        // The segment scanner in Python handles compatibility checks
        if self.mlx5dv_enabled {
            unsafe {
                let result = rdmaxcel_sys::deregister_segments();
                if result != 0 {
                    let error_msg = get_rdmaxcel_error_message(result);
                    tracing::error!(
                        "Failed to deregister CUDA segments: {} (error code: {})",
                        error_msg,
                        result
                    );
                }
            }
        }
    }
}

impl IbvManagerActor {
    /// Construct an [`ActorHandle`] for the [`IbvManagerActor`] co-located
    /// with the caller by querying the local [`RdmaManagerActor`].
    pub async fn local_handle(
        client: &(impl hyperactor::context::Actor + Send + Sync),
    ) -> Result<ActorHandle<Self>, anyhow::Error> {
        let rdma_handle = RdmaManagerActor::local_handle(client);
        let ibv_ref = rdma_handle
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

        // Build the CUDA to RDMA device mapping using device selection algorithm
        let pci_to_device = super::device_selection::create_cuda_to_ibv_mapping();
        tracing::debug!(
            "Built CUDA to RDMA device mapping with {} entries",
            pci_to_device.len()
        );

        Ok(Self {
            owner: OnceLock::new(),
            device_qps: HashMap::new(),
            pending_qp_creation: Arc::new(Mutex::new(HashSet::new())),
            device_domains: HashMap::new(),
            config,
            mlx5dv_enabled,
            mr_map: HashMap::new(),
            mrv_id: 0,
            pci_to_device,
            buffer_registrations: HashMap::new(),
        })
    }

    /// Get or create a domain and loopback QP for the specified RDMA device
    fn get_or_create_device_domain(
        &mut self,
        device_name: &str,
        rdma_device: &IbvDevice,
    ) -> Result<(IbvDomain, Option<IbvQueuePair>), anyhow::Error> {
        if let Some((domain, qp)) = self.device_domains.get(device_name) {
            return Ok((domain.clone(), qp.clone()));
        }

        // Create new domain for this device
        let domain = IbvDomain::new(rdma_device.clone()).map_err(|e| {
            anyhow::anyhow!("could not create domain for device {}: {}", device_name, e)
        })?;

        // Print device info if MONARCH_DEBUG_RDMA=1 is set (before initial QP creation)
        crate::print_device_info_if_debug_enabled(domain.context);

        // Create loopback QP for this domain if mlx5dv is supported (needed for segment registration)
        // For EFA, we don't need a loopback QP for segment scanning
        let qp = if mlx5dv_supported() && !crate::efa::is_efa_device() {
            let mut qp = IbvQueuePair::new(domain.context, domain.pd, self.config.clone())
                .map_err(|e| {
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

            Some(qp)
        } else {
            None
        };

        self.device_domains
            .insert(device_name.to_string(), (domain.clone(), qp.clone()));
        Ok((domain, qp))
    }

    fn find_cuda_segment_for_address(
        &mut self,
        addr: usize,
        size: usize,
    ) -> Option<IbvMemoryRegionView> {
        let registered_segments = get_registered_cuda_segments();
        for segment in registered_segments {
            let start_addr = segment.phys_address;
            let end_addr = start_addr + segment.phys_size;
            if start_addr <= addr && addr + size <= end_addr {
                let offset = addr - start_addr;
                let rdma_addr = segment.mr_addr + offset;

                let mrv = IbvMemoryRegionView {
                    id: self.mrv_id,
                    virtual_addr: addr,
                    rdma_addr,
                    size,
                    lkey: segment.lkey,
                    rkey: segment.rkey,
                };
                self.mrv_id += 1;
                return Some(mrv);
            }
        }
        None
    }

    fn register_mr(
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
                // Use rdmaxcel utility to get PCI address from CUDA pointer
                let mut pci_addr_buf: [std::os::raw::c_char; 16] = [0; 16]; // Enough space for "ffff:ff:ff.0\0"
                let err = rdmaxcel_sys::get_cuda_pci_address_from_ptr(
                    addr as u64,
                    pci_addr_buf.as_mut_ptr(),
                    pci_addr_buf.len(),
                );
                if err != 0 {
                    let error_msg = get_rdmaxcel_error_message(err);
                    return Err(anyhow::anyhow!(
                        "RdmaXcel get_cuda_pci_address_from_ptr failed (addr: 0x{:x}, size: {}): {}",
                        addr,
                        size,
                        error_msg
                    ));
                }

                // Convert C string to Rust string
                let pci_addr = std::ffi::CStr::from_ptr(pci_addr_buf.as_ptr())
                    .to_str()
                    .unwrap();
                selected_rdma_device = self.pci_to_device.get(pci_addr).cloned();
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

            // Get or create domain and loopback QP for this device
            let (domain, qp) = self.get_or_create_device_domain(&device_name, &rdma_device)?;

            let access = if crate::efa::is_efa_device() {
                crate::efa::mr_access_flags()
            } else {
                rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                    | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                    | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                    | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC
            };

            let mut mr: *mut rdmaxcel_sys::ibv_mr = std::ptr::null_mut();
            let mrv;

            if is_cuda {
                // First, try to use segment scanning if mlx5dv is enabled
                let mut segment_mrv = None;
                if self.mlx5dv_enabled {
                    // Try to find in already registered segments
                    segment_mrv = self.find_cuda_segment_for_address(addr, size);

                    // If not found, trigger a re-sync with the allocator and retry
                    if segment_mrv.is_none() {
                        let err = rdmaxcel_sys::register_segments(
                            domain.pd,
                            qp.unwrap().qp as *mut rdmaxcel_sys::rdmaxcel_qp_t,
                        );
                        // Only retry if register_segments succeeded
                        // If it fails (e.g., scanner returns 0 segments), we'll fall back to dmabuf
                        if err == 0 {
                            segment_mrv = self.find_cuda_segment_for_address(addr, size);
                        }
                    }
                }

                // Use segment if found, otherwise fall back to direct dmabuf registration
                if let Some(mrv_from_segment) = segment_mrv {
                    mrv = mrv_from_segment;
                } else {
                    // Dmabuf path: used when mlx5dv is disabled OR scanner returns no segments
                    let mut fd: i32 = -1;
                    rdmaxcel_sys::rdmaxcel_cuMemGetHandleForAddressRange(
                        &mut fd,
                        addr as rdmaxcel_sys::CUdeviceptr,
                        size,
                        rdmaxcel_sys::CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                        0,
                    );
                    mr =
                        rdmaxcel_sys::ibv_reg_dmabuf_mr(domain.pd, 0, size, 0, fd, access.0 as i32);
                    if mr.is_null() {
                        return Err(anyhow::anyhow!("Failed to register dmabuf MR"));
                    }
                    mrv = IbvMemoryRegionView {
                        id: self.mrv_id,
                        virtual_addr: addr,
                        rdma_addr: (*mr).addr as usize,
                        size,
                        lkey: (*mr).lkey,
                        rkey: (*mr).rkey,
                    };
                    self.mrv_id += 1;
                }
            } else {
                // CPU memory path
                mr = rdmaxcel_sys::ibv_reg_mr(
                    domain.pd,
                    addr as *mut std::ffi::c_void,
                    size,
                    access.0 as i32,
                );

                if mr.is_null() {
                    return Err(anyhow::anyhow!("failed to register standard MR"));
                }

                mrv = IbvMemoryRegionView {
                    id: self.mrv_id,
                    virtual_addr: addr,
                    rdma_addr: (*mr).addr as usize,
                    size,
                    lkey: (*mr).lkey,
                    rkey: (*mr).rkey,
                };
                self.mrv_id += 1;
            }
            self.mr_map.insert(mrv.id, mr as usize);
            Ok((mrv, device_name))
        }
    }

    fn deregister_mr(&mut self, id: usize) -> Result<(), anyhow::Error> {
        if let Some(mr_ptr) = self.mr_map.remove(&id) {
            if mr_ptr != 0 {
                unsafe {
                    rdmaxcel_sys::ibv_dereg_mr(mr_ptr as *mut rdmaxcel_sys::ibv_mr);
                }
            }
        }
        Ok(())
    }

    /// Waits for the completion of RDMA operations.
    ///
    /// Polls the completion queue until all specified work requests complete
    /// or until the timeout is reached.
    ///
    /// # Arguments
    /// * `local_buf` - The ibv buffer describing the local side of the RDMA ops
    /// * `qp` - The RDMA Queue Pair to poll for completion
    /// * `poll_target` - Which CQ to poll (Send or Recv)
    /// * `expected_wr_ids` - The work request IDs to wait for
    /// * `timeout` - Timeout for the RDMA operation to complete.
    async fn wait_for_completion(
        &self,
        local_buf: &IbvBuffer,
        qp: &mut IbvQueuePair,
        poll_target: PollTarget,
        expected_wr_ids: &[u64],
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let start_time = std::time::Instant::now();

        let mut remaining: std::collections::HashSet<u64> =
            expected_wr_ids.iter().copied().collect();

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
                    RealClock.sleep(Duration::from_millis(1)).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "RDMA polling completion failed: {} [lkey={}, rkey={}, addr=0x{:x}, size={}]",
                        e,
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

    /// Core submit logic: registers local MR, resolves remote
    /// IbvBuffer lazily, executes the op, and deregisters local MR.
    async fn execute_op(
        &mut self,
        cx: &Context<'_, Self>,
        op: IbvOp,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        // Register the local memory to get an IbvBuffer
        let (local_mrv, local_device_name) =
            self.register_mr(op.local_memory.addr(), op.local_memory.size())?;
        let local_buffer = IbvBuffer {
            mr_id: local_mrv.id,
            lkey: local_mrv.lkey,
            rkey: local_mrv.rkey,
            addr: local_mrv.rdma_addr,
            size: local_mrv.size,
            device_name: local_device_name,
        };

        let op_result = async {
            let mut qp = self
                .request_queue_pair(
                    cx,
                    op.remote_manager.clone(),
                    local_buffer.device_name.clone(),
                    op.remote_buffer.device_name.clone(),
                )
                .await?;

            let wr_id = match op.op_type {
                RdmaOpType::WriteFromLocal => qp.put(local_buffer.clone(), op.remote_buffer)?,
                RdmaOpType::ReadIntoLocal => qp.get(local_buffer.clone(), op.remote_buffer)?,
            };

            self.wait_for_completion(&local_buffer, &mut qp, PollTarget::Send, &wr_id, timeout)
                .await
        }
        .await;

        // Always deregister the locally registered MR
        let dereg_result = self.deregister_mr(local_buffer.mr_id);

        match (op_result, dereg_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(e), Ok(())) => Err(e),
            (Ok(()), Err(e)) => Err(e),
            (Err(op_err), Err(dereg_err)) => Err(anyhow::anyhow!(
                "deregister MR error: {}; op error: {}",
                dereg_err,
                op_err
            )),
        }
    }
}

#[async_trait]
#[hyperactor::handle(IbvManagerMessage)]
impl IbvManagerMessageHandler for IbvManagerActor {
    async fn request_buffer(
        &mut self,
        cx: &Context<Self>,
        remote_buf_id: usize,
    ) -> Result<Option<IbvBuffer>, anyhow::Error> {
        // If already registered, return it
        if let Some(buf) = self.buffer_registrations.get(&remote_buf_id) {
            return Ok(Some(buf.clone()));
        }

        // Resolve local memory from the parent RdmaManagerActor.
        // Returns None if the buffer has already been released or does
        // not exist.
        let owner = self.owner.get().unwrap();
        let mem = match owner.request_local_memory(cx, remote_buf_id).await? {
            Some(mem) => mem,
            None => return Ok(None),
        };

        let (mrv, device_name) = self.register_mr(mem.addr(), mem.size())?;

        let buf = IbvBuffer {
            mr_id: mrv.id,
            lkey: mrv.lkey,
            rkey: mrv.rkey,
            addr: mrv.rdma_addr,
            size: mrv.size,
            device_name,
        };

        self.buffer_registrations.insert(remote_buf_id, buf.clone());

        Ok(Some(buf))
    }

    async fn release_buffer(
        &mut self,
        _cx: &Context<Self>,
        remote_buf_id: usize,
    ) -> Result<(), anyhow::Error> {
        if let Some(buf) = self.buffer_registrations.remove(&remote_buf_id) {
            self.deregister_mr(buf.mr_id)
                .map_err(|e| anyhow::anyhow!("could not deregister buffer: {}", e))?;
        }
        Ok(())
    }

    async fn request_queue_pair(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<IbvQueuePair, anyhow::Error> {
        let self_ref: ActorRef<IbvManagerActor> = cx.bind();
        let other_id = other.actor_id().clone();

        // Use the nested map structure: local_device -> (actor_id, remote_device) -> IbvQueuePair
        let inner_key = (other_id.clone(), other_device.clone());

        // Check if queue pair exists in map
        if let Some(device_map) = self.device_qps.get(&self_device) {
            if let Some(qp) = device_map.get(&inner_key) {
                return Ok(qp.clone());
            }
        }

        // Try to acquire lock and mark as pending (hold lock only once!)
        let pending_key = (self_device.clone(), other_id.clone(), other_device.clone());
        let mut pending = self.pending_qp_creation.lock().await;

        if pending.contains(&pending_key) {
            // Another task is creating this QP, release lock and wait
            drop(pending);

            // Loop checking device_qps until QP is created (no more locks needed)
            // Timeout after 1 second
            let start = Instant::now();
            let timeout = Duration::from_secs(1);

            loop {
                RealClock.sleep(Duration::from_micros(200)).await;

                // Check if QP was created while we waited
                if let Some(device_map) = self.device_qps.get(&self_device) {
                    if let Some(qp) = device_map.get(&inner_key) {
                        return Ok(qp.clone());
                    }
                }

                // Check for timeout
                if start.elapsed() >= timeout {
                    return Err(anyhow::anyhow!(
                        "Timeout waiting for QP creation (device {} -> actor {} device {}). \
                         Another task is creating it but hasn't completed in 1 second",
                        self_device,
                        other_id,
                        other_device
                    ));
                }
            }
        } else {
            // Not pending, add to set and proceed with creation
            pending.insert(pending_key.clone());
            drop(pending);
            // Fall through to create QP
        }

        // Queue pair doesn't exist - need to create connection
        let result = async {
            let is_loopback = other_id == *self_ref.actor_id() && self_device == other_device;

            if is_loopback {
                // Loopback connection setup
                self.initialize_qp(cx, other.clone(), self_device.clone(), other_device.clone())
                    .await?;
                let endpoint = self
                    .connection_info(cx, other.clone(), other_device.clone(), self_device.clone())
                    .await?;
                self.connect(
                    cx,
                    other.clone(),
                    self_device.clone(),
                    other_device.clone(),
                    endpoint,
                )
                .await?;
            } else {
                // Remote connection setup
                self.initialize_qp(cx, other.clone(), self_device.clone(), other_device.clone())
                    .await?;
                other
                    .initialize_qp(
                        cx,
                        self_ref.clone(),
                        other_device.clone(),
                        self_device.clone(),
                    )
                    .await?;
                let other_endpoint: IbvQpInfo = other
                    .connection_info(
                        cx,
                        self_ref.clone(),
                        other_device.clone(),
                        self_device.clone(),
                    )
                    .await?;
                self.connect(
                    cx,
                    other.clone(),
                    self_device.clone(),
                    other_device.clone(),
                    other_endpoint,
                )
                .await?;
                let local_endpoint = self
                    .connection_info(cx, other.clone(), self_device.clone(), other_device.clone())
                    .await?;
                other
                    .connect(
                        cx,
                        self_ref.clone(),
                        other_device.clone(),
                        self_device.clone(),
                        local_endpoint,
                    )
                    .await?;

                // BARRIER: Ensure remote side has completed its connection and is ready
                let remote_state = other
                    .get_qp_state(
                        cx,
                        self_ref.clone(),
                        other_device.clone(),
                        self_device.clone(),
                    )
                    .await?;

                if remote_state != rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTS {
                    return Err(anyhow::anyhow!(
                        "Remote QP not in RTS state after connection setup. \
                         Local is ready but remote is in state {}. \
                         This indicates a synchronization issue in connection setup.",
                        remote_state
                    ));
                }
            }

            // Now that connection is established, get and clone the queue pair
            if let Some(device_map) = self.device_qps.get(&self_device) {
                if let Some(qp) = device_map.get(&inner_key) {
                    Ok(qp.clone())
                } else {
                    Err(anyhow::anyhow!(
                        "Failed to create connection for actor {} on device {}",
                        other_id,
                        other_device
                    ))
                }
            } else {
                Err(anyhow::anyhow!(
                    "Failed to create connection for actor {} on device {} - no device map",
                    other_id,
                    other_device
                ))
            }
        }
        .await;

        // Always remove from pending set when done (success or failure)
        let mut pending = self.pending_qp_creation.lock().await;
        pending.remove(&pending_key);
        drop(pending);

        result
    }

    async fn connect(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
        endpoint: IbvQpInfo,
    ) -> Result<(), anyhow::Error> {
        tracing::debug!("connecting with {:?}", other);
        let other_id = other.actor_id().clone();

        let inner_key = (other_id.clone(), other_device.clone());

        if let Some(device_map) = self.device_qps.get_mut(&self_device) {
            match device_map.get_mut(&inner_key) {
                Some(qp) => {
                    qp.connect(&endpoint).map_err(|e| {
                        anyhow::anyhow!("could not connect to RDMA endpoint: {}", e)
                    })?;
                    Ok(())
                }
                None => Err(anyhow::anyhow!(
                    "No connection found for actor {}",
                    other_id
                )),
            }
        } else {
            Err(anyhow::anyhow!(
                "No device map found for device {}",
                self_device
            ))
        }
    }

    async fn initialize_qp(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<bool, anyhow::Error> {
        let other_id = other.actor_id().clone();
        let inner_key = (other_id.clone(), other_device.clone());

        // Check if QP already exists in nested structure
        if let Some(device_map) = self.device_qps.get(&self_device) {
            if device_map.contains_key(&inner_key) {
                return Ok(true);
            }
        }

        // Resolve the RDMA device for the local device
        let rdma_device = self
            .pci_to_device
            .iter()
            .find(|(_, device)| device.name() == &self_device)
            .map(|(_, device)| device.clone())
            .unwrap_or_else(|| {
                // Fallback to default device from config
                super::device_selection::resolve_ibv_device(&self.config.device)
                    .unwrap_or_else(|| self.config.device.clone())
            });

        // Get or create domain and extract pointers to avoid borrowing issues
        let (domain_context, domain_pd) = {
            // Check if we already have a domain for the device
            let (domain, _) = self.get_or_create_device_domain(&self_device, &rdma_device)?;
            (domain.context, domain.pd)
        };

        let qp = IbvQueuePair::new(domain_context, domain_pd, self.config.clone())
            .map_err(|e| anyhow::anyhow!("could not create IbvQueuePair: {}", e))?;

        // Insert the QP into the nested map structure
        self.device_qps
            .entry(self_device.clone())
            .or_insert_with(HashMap::new)
            .insert(inner_key, qp);

        tracing::debug!(
            "successfully created a connection with {:?} for local device {} -> remote device {}",
            other,
            self_device,
            other_device
        );

        Ok(true)
    }

    async fn connection_info(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<IbvQpInfo, anyhow::Error> {
        tracing::debug!("getting connection info with {:?}", other);
        let other_id = other.actor_id().clone();

        let inner_key = (other_id.clone(), other_device.clone());

        if let Some(device_map) = self.device_qps.get_mut(&self_device) {
            match device_map.get_mut(&inner_key) {
                Some(qp) => {
                    let connection_info = qp.get_qp_info()?;
                    Ok(connection_info)
                }
                None => Err(anyhow::anyhow!(
                    "No connection found for actor {}",
                    other_id
                )),
            }
        } else {
            Err(anyhow::anyhow!(
                "No device map found for self device {}",
                self_device
            ))
        }
    }

    async fn release_queue_pair(
        &mut self,
        _cx: &Context<Self>,
        _other: ActorRef<IbvManagerActor>,
        _self_device: String,
        _other_device: String,
        _qp: IbvQueuePair,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn get_qp_state(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<IbvManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<u32, anyhow::Error> {
        let other_id = other.actor_id().clone();
        let inner_key = (other_id.clone(), other_device.clone());

        if let Some(device_map) = self.device_qps.get_mut(&self_device) {
            match device_map.get_mut(&inner_key) {
                Some(qp) => qp.state(),
                None => Err(anyhow::anyhow!(
                    "No connection found for actor {} on device {}",
                    other_id,
                    other_device
                )),
            }
        } else {
            Err(anyhow::anyhow!(
                "No device map found for self device {}",
                self_device
            ))
        }
    }
}

#[async_trait]
#[hyperactor::handle(IbvSubmit)]
impl IbvSubmitHandler for IbvManagerActor {
    async fn ibv_submit(
        &mut self,
        cx: &Context<Self>,
        ops: Vec<IbvOp>,
        timeout: Duration,
    ) -> Result<Result<(), String>, anyhow::Error> {
        let deadline = Instant::now() + timeout;
        let mut result = Ok(());
        for op in ops {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                result = Err("submit timed out".to_string());
                break;
            }
            if let Err(e) = self.execute_op(cx, op, remaining).await {
                result = Err(e.to_string());
                break;
            }
        }
        Ok(result)
    }
}

#[async_trait]
impl RdmaBackend for ActorHandle<IbvManagerActor> {
    type TransportInfo = ();

    /// Submit a batch of RDMA operations.
    ///
    /// Delegates to the [`IbvManagerActor`], which manages QPs and connections
    /// internally. Each op is routed based on its type (read/write).
    /// Registers local memory on the fly; remote buffers are lazily
    /// resolved via [`IbvManagerMessageClient::request_buffer`].
    async fn submit(
        &mut self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let mut ibv_ops = Vec::with_capacity(ops.len());
        for op in ops {
            let (remote_ibv_mgr, remote_ibv_buffer) = op.remote.resolve_ibv(cx).await?;

            ibv_ops.push(IbvOp {
                op_type: op.op_type,
                local_memory: op.local.clone(),
                remote_buffer: remote_ibv_buffer,
                remote_manager: remote_ibv_mgr,
            });
        }

        <Self as IbvSubmitClient>::ibv_submit(self, cx, ibv_ops, timeout)
            .await?
            .map_err(|e: String| anyhow::anyhow!(e))
    }

    fn transport_level(&self) -> RdmaTransportLevel {
        RdmaTransportLevel::Nic
    }

    fn transport_info(&self) -> Option<Self::TransportInfo> {
        None
    }
}
