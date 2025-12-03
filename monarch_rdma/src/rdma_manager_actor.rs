/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # RDMA Manager Actor
//!
//! Manages RDMA connections and operations using `hyperactor` for asynchronous messaging.
//!
//! ## Architecture
//!
//! `RdmaManagerActor` is a per-host entity that:
//! - Manages connections to multiple remote RdmaManagerActors (i.e. across the hosts in a Monarch cluster)
//! - Handles memory registration, connection setup, and data transfer
//! - Manages all RdmaBuffers in its associated host
//!
//! ## Core Operations
//!
//! - Connection establishment with partner actors
//! - RDMA operations (put/write, get/read)
//! - Completion polling
//! - Memory region management
//!
//! ## Usage
//!
//! See test examples: `test_rdma_write_loopback` and `test_rdma_read_loopback`.
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use futures::lock::Mutex;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::clock::Clock;
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;

use crate::ibverbs_primitives::IbverbsConfig;
use crate::ibverbs_primitives::RdmaMemoryRegionView;
use crate::ibverbs_primitives::RdmaQpInfo;
use crate::ibverbs_primitives::ibverbs_supported;
use crate::ibverbs_primitives::mlx5dv_supported;
use crate::ibverbs_primitives::resolve_qp_type;
use crate::rdma_components::RdmaBuffer;
use crate::rdma_components::RdmaDomain;
use crate::rdma_components::RdmaQueuePair;
use crate::rdma_components::get_registered_cuda_segments;
use crate::validate_execution_context;

/// Helper function to get detailed error messages from RDMAXCEL error codes
pub fn get_rdmaxcel_error_message(error_code: i32) -> String {
    unsafe {
        let c_str = rdmaxcel_sys::rdmaxcel_error_string(error_code);
        std::ffi::CStr::from_ptr(c_str)
            .to_string_lossy()
            .into_owned()
    }
}

/// Represents a reference to a remote RDMA buffer that can be accessed via RDMA operations.
/// This struct encapsulates all the information needed to identify and access a memory region
/// on a remote host using RDMA.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum RdmaManagerMessage {
    RequestBuffer {
        addr: usize,
        size: usize,
        #[reply]
        /// `reply` - Reply channel to return the RDMA buffer handle
        reply: OncePortRef<RdmaBuffer>,
    },
    ReleaseBuffer {
        buffer: RdmaBuffer,
    },
    RequestQueuePair {
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<RdmaQueuePair>,
    },
    Connect {
        /// `other` - The ActorId of the actor to connect to
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        /// `endpoint` - Connection information needed to establish the RDMA connection
        endpoint: RdmaQpInfo,
    },
    InitializeQP {
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<bool>,
    },
    ConnectionInfo {
        /// `other` - The ActorId to get connection info for
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the connection info
        reply: OncePortRef<RdmaQpInfo>,
    },
    ReleaseQueuePair {
        /// `other` - The ActorId to release queue pair for
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        /// `qp` - The queue pair to return (ownership transferred back)
        qp: RdmaQueuePair,
    },
    GetQpState {
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the QP state
        reply: OncePortRef<u32>,
    },
}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        RdmaManagerMessage,
    ],
)]
pub struct RdmaManagerActor {
    // Nested map: local_device -> (ActorId, remote_device) -> RdmaQueuePair
    device_qps: HashMap<String, HashMap<(ActorId, String), RdmaQueuePair>>,

    // Track QPs currently being created to prevent duplicate creation
    // Wrapped in Arc<Mutex> to allow safe concurrent access
    pending_qp_creation: Arc<Mutex<HashSet<(String, ActorId, String)>>>,

    // Map of RDMA device names to their domains and loopback QPs
    // Created lazily when memory is registered for a specific device
    device_domains: HashMap<String, (RdmaDomain, Option<RdmaQueuePair>)>,

    config: IbverbsConfig,

    // Flag indicating PyTorch CUDA allocator compatibility
    // True if both C10 CUDA allocator is enabled AND expandable segments are enabled
    pt_cuda_alloc: bool,

    mlx5dv_enabled: bool,

    // Map of unique RdmaMemoryRegionView to ibv_mr*.  In case of cuda w/ pytorch its -1
    // since its managed independently.  Only used for registration/deregistration purposes
    mr_map: HashMap<usize, usize>,
    // Id for next mrv created
    mrv_id: usize,

    // Map of PCI addresses to their optimal RDMA devices
    // This is populated during actor initialization using the device selection algorithm
    pci_to_device: HashMap<String, crate::ibverbs_primitives::RdmaDevice>,
}

impl Drop for RdmaManagerActor {
    fn drop(&mut self) {
        // Helper function to manually destroy QP and CQs
        // We can't use Drop on RdmaQueuePair because it derives Clone
        fn destroy_queue_pair(qp: &RdmaQueuePair, context: &str) {
            unsafe {
                if qp.qp != 0 {
                    let rdmaxcel_qp = qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
                    rdmaxcel_sys::rdmaxcel_qp_destroy(rdmaxcel_qp);
                }
                if qp.send_cq != 0 {
                    let result =
                        rdmaxcel_sys::ibv_destroy_cq(qp.send_cq as *mut rdmaxcel_sys::ibv_cq);
                    if result != 0 {
                        tracing::debug!(
                            "ibv_destroy_cq (send) returned {} for {} (may be busy during shutdown)",
                            result,
                            context
                        );
                    }
                }
                if qp.recv_cq != 0 {
                    let result =
                        rdmaxcel_sys::ibv_destroy_cq(qp.recv_cq as *mut rdmaxcel_sys::ibv_cq);
                    if result != 0 {
                        tracing::debug!(
                            "ibv_destroy_cq (recv) returned {} for {} (may be busy during shutdown)",
                            result,
                            context
                        );
                    }
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

        // 4. Deregister all CUDA segments (if using PyTorch CUDA allocator)
        if self.cuda_pt_alloc_enabled() {
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

impl RdmaManagerActor {
    /// Whether to register all memory regions allocated by the PyTorch CUDA allocator
    /// True if both `pt_cuda_alloc` and `mlx5dv_enabled` are true
    fn cuda_pt_alloc_enabled(&self) -> bool {
        self.pt_cuda_alloc && self.mlx5dv_enabled
    }
    /// Get or create a domain and loopback QP for the specified RDMA device
    fn get_or_create_device_domain(
        &mut self,
        device_name: &str,
        rdma_device: &crate::ibverbs_primitives::RdmaDevice,
    ) -> Result<(RdmaDomain, Option<RdmaQueuePair>), anyhow::Error> {
        // Check if we already have a domain for this device
        if let Some((domain, qp)) = self.device_domains.get(device_name) {
            return Ok((domain.clone(), qp.clone()));
        }

        // Create new domain for this device
        let domain = RdmaDomain::new(rdma_device.clone()).map_err(|e| {
            anyhow::anyhow!("could not create domain for device {}: {}", device_name, e)
        })?;

        // Print device info if MONARCH_DEBUG_RDMA=1 is set (before initial QP creation)
        crate::print_device_info_if_debug_enabled(domain.context);

        // Create loopback QP for this domain if mlx5dv is supported
        let qp = if mlx5dv_supported() {
            let mut qp = RdmaQueuePair::new(domain.context, domain.pd, self.config.clone())
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
    ) -> Option<RdmaMemoryRegionView> {
        let registered_segments = get_registered_cuda_segments();
        for segment in registered_segments {
            let start_addr = segment.phys_address;
            let end_addr = start_addr + segment.phys_size;
            if start_addr <= addr && addr + size <= end_addr {
                let offset = addr - start_addr;
                let rdma_addr = segment.mr_addr + offset;

                let mrv = RdmaMemoryRegionView {
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
    ) -> Result<(RdmaMemoryRegionView, String), anyhow::Error> {
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

            let access = rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;

            let mut mr: *mut rdmaxcel_sys::ibv_mr = std::ptr::null_mut();
            let mrv;

            if is_cuda && self.cuda_pt_alloc_enabled() {
                // Get registered segments and check if our memory range is covered
                let mut maybe_mrv = self.find_cuda_segment_for_address(addr, size);
                // not found, lets re-sync with caching allocator  and retry
                if maybe_mrv.is_none() {
                    let err = rdmaxcel_sys::register_segments(
                        domain.pd,
                        qp.unwrap().qp as *mut rdmaxcel_sys::rdmaxcel_qp_t,
                    );
                    if err != 0 {
                        let error_msg = get_rdmaxcel_error_message(err);
                        return Err(anyhow::anyhow!(
                            "RdmaXcel register_segments failed (addr: 0x{:x}, size: {}): {}",
                            addr,
                            size,
                            error_msg
                        ));
                    }

                    maybe_mrv = self.find_cuda_segment_for_address(addr, size);
                }
                // if still not found, throw exception
                if maybe_mrv.is_none() {
                    return Err(anyhow::anyhow!(
                        "MR registration failed for cuda (addr: 0x{:x}, size: {}), unable to find segment in CudaCachingAllocator",
                        addr,
                        size
                    ));
                }
                mrv = maybe_mrv.unwrap();
            } else if is_cuda {
                let mut fd: i32 = -1;
                rdmaxcel_sys::rdmaxcel_cuMemGetHandleForAddressRange(
                    &mut fd,
                    addr as rdmaxcel_sys::CUdeviceptr,
                    size,
                    rdmaxcel_sys::CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                    0,
                );
                mr = rdmaxcel_sys::ibv_reg_dmabuf_mr(domain.pd, 0, size, 0, fd, access.0 as i32);
                if mr.is_null() {
                    return Err(anyhow::anyhow!("Failed to register dmabuf MR"));
                }
                mrv = RdmaMemoryRegionView {
                    id: self.mrv_id,
                    virtual_addr: addr,
                    rdma_addr: (*mr).addr as usize,
                    size,
                    lkey: (*mr).lkey,
                    rkey: (*mr).rkey,
                };
                self.mrv_id += 1;
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

                mrv = RdmaMemoryRegionView {
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
}

#[async_trait]
impl RemoteSpawn for RdmaManagerActor {
    type Params = Option<IbverbsConfig>;

    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        if !ibverbs_supported() {
            return Err(anyhow::anyhow!(
                "Cannot create RdmaManagerActor because RDMA is not supported on this machine"
            ));
        }

        // Use provided config or default if none provided
        let mut config = params.unwrap_or_default();
        tracing::debug!("rdma is enabled, config device hint: {}", config.device);

        let pt_cuda_alloc = crate::rdma_components::pt_cuda_allocator_compatibility();

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
        let pci_to_device = crate::device_selection::create_cuda_to_rdma_mapping();
        tracing::debug!(
            "Built CUDA to RDMA device mapping with {} entries",
            pci_to_device.len()
        );

        Ok(Self {
            device_qps: HashMap::new(),
            pending_qp_creation: Arc::new(Mutex::new(HashSet::new())),
            device_domains: HashMap::new(),
            config,
            pt_cuda_alloc,
            mlx5dv_enabled,
            mr_map: HashMap::new(),
            mrv_id: 0,
            pci_to_device,
        })
    }
}

#[async_trait]
impl Actor for RdmaManagerActor {
    async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
        tracing::debug!("RdmaManagerActor initialized with lazy domain/QP creation");
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        _cx: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        tracing::error!("rdmaManagerActor supervision event: {:?}", _event);
        tracing::error!("rdmaManagerActor error occurred, stop the worker process, exit code: 1");
        std::process::exit(1);
    }
}

#[async_trait]
#[hyperactor::forward(RdmaManagerMessage)]
impl RdmaManagerMessageHandler for RdmaManagerActor {
    /// Requests a buffer to be registered with the RDMA domain.
    ///
    /// This function registers a memory region with the RDMA domain and returns an `RdmaBuffer`
    /// that encapsulates the necessary information for RDMA operations.
    ///
    /// # Arguments
    ///
    /// * `this` - The context of the actor requesting the buffer.
    /// * `addr` - The starting address of the memory region to be registered.
    /// * `size` - The size of the memory region to be registered.
    ///
    /// # Returns
    ///
    /// * `Result<RdmaBuffer, anyhow::Error>` - On success, returns an `RdmaBuffer` containing
    ///   the registered memory region's details. On failure, returns an error.
    async fn request_buffer(
        &mut self,
        cx: &Context<Self>,
        addr: usize,
        size: usize,
    ) -> Result<RdmaBuffer, anyhow::Error> {
        let (mrv, device_name) = self.register_mr(addr, size)?;

        Ok(RdmaBuffer {
            owner: cx.bind().clone(),
            mr_id: mrv.id,
            addr: mrv.rdma_addr,
            size: mrv.size,
            rkey: mrv.rkey,
            lkey: mrv.lkey,
            device_name,
        })
    }

    /// Deregisters a buffer from the RDMA domain.
    ///
    /// This function removes the specified `RdmaBuffer` from the RDMA domain,
    /// effectively releasing the resources associated with it.
    ///
    /// # Arguments
    ///
    /// * `_this` - The context of the actor releasing the buffer.
    /// * `buffer` - The `RdmaBuffer` to be deregistered.
    ///
    /// # Returns
    ///
    /// * `Result<(), anyhow::Error>` - On success, returns `Ok(())`. On failure, returns an error.
    async fn release_buffer(
        &mut self,
        _cx: &Context<Self>,
        buffer: RdmaBuffer,
    ) -> Result<(), anyhow::Error> {
        self.deregister_mr(buffer.mr_id)
            .map_err(|e| anyhow::anyhow!("could not deregister buffer: {}", e))?;
        Ok(())
    }

    /// Requests a queue pair for communication with a remote RDMA manager actor.
    ///
    /// Basic logic: if queue pair exists in map, return it; if None, create connection first.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the actor requesting the queue pair.
    /// * `remote` - The ActorRef of the remote RDMA manager actor to communicate with.
    ///
    /// # Returns
    ///
    /// * `Result<RdmaQueuePair, anyhow::Error>` - On success, returns the queue pair for communication.
    ///   On failure, returns an error.
    async fn request_queue_pair(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<RdmaQueuePair, anyhow::Error> {
        let other_id = other.actor_id().clone();

        // Use the nested map structure: local_device -> (actor_id, remote_device) -> RdmaQueuePair
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
                cx.clock().sleep(Duration::from_micros(200)).await;

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
            let is_loopback = other_id == cx.bind::<RdmaManagerActor>().actor_id().clone()
                && self_device == other_device;

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
                        cx.bind().clone(),
                        other_device.clone(),
                        self_device.clone(),
                    )
                    .await?;
                let other_endpoint: RdmaQpInfo = other
                    .connection_info(
                        cx,
                        cx.bind().clone(),
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
                        cx.bind().clone(),
                        other_device.clone(),
                        self_device.clone(),
                        local_endpoint,
                    )
                    .await?;

                // BARRIER: Ensure remote side has completed its connection and is ready
                let remote_state = other
                    .get_qp_state(
                        cx,
                        cx.bind().clone(),
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

    async fn initialize_qp(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
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
                crate::device_selection::resolve_rdma_device(&self.config.device)
                    .unwrap_or_else(|| self.config.device.clone())
            });

        // Get or create domain and extract pointers to avoid borrowing issues
        let (domain_context, domain_pd) = {
            // Check if we already have a domain for the device
            let (domain, _) = self.get_or_create_device_domain(&self_device, &rdma_device)?;
            (domain.context, domain.pd)
        };

        let qp = RdmaQueuePair::new(domain_context, domain_pd, self.config.clone())
            .map_err(|e| anyhow::anyhow!("could not create RdmaQueuePair: {}", e))?;

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

    /// Establishes a connection with another actor
    ///
    /// # Arguments
    /// * `other` - The ActorRef of the actor to connect to
    /// * `endpoint` - Connection information needed to establish the RDMA connection
    async fn connect(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        endpoint: RdmaQpInfo,
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

    /// Gets connection information for establishing an RDMA connection
    ///
    /// # Arguments
    /// * `other` - The ActorRef to get connection info for
    ///
    /// # Returns
    /// * `RdmaQpInfo` - Connection information needed for the RDMA connection
    async fn connection_info(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<RdmaQpInfo, anyhow::Error> {
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

    /// Releases a queue pair back to the HashMap
    ///
    /// This method is now a no-op since RdmaQueuePair is Clone and can be safely shared.
    /// The queue pair is not actually checked out, so there's nothing to release.
    /// This method is kept for API compatibility.
    ///
    /// # Arguments
    /// * `remote` - The ActorRef of the remote actor to return the queue pair for
    /// * `qp` - The queue pair to release (ignored)
    async fn release_queue_pair(
        &mut self,
        _cx: &Context<Self>,
        _other: ActorRef<RdmaManagerActor>,
        _self_device: String,
        _other_device: String,
        _qp: RdmaQueuePair,
    ) -> Result<(), anyhow::Error> {
        // No-op: Queue pairs are now cloned and shared via atomic counters
        // Nothing needs to be released
        Ok(())
    }

    /// Gets the state of a queue pair
    ///
    /// # Arguments
    /// * `other` - The ActorRef to get the QP state for
    /// * `self_device` - Local device name
    /// * `other_device` - Remote device name
    ///
    /// # Returns
    /// * `u32` - The QP state (e.g., IBV_QPS_RTS = Ready To Send)
    async fn get_qp_state(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
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
