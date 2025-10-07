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

use async_trait::async_trait;
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
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;

use crate::ibverbs_primitives::IbverbsConfig;
use crate::ibverbs_primitives::RdmaMemoryRegionView;
use crate::ibverbs_primitives::RdmaQpInfo;
use crate::ibverbs_primitives::ibverbs_supported;
use crate::rdma_components::RdmaBuffer;
use crate::rdma_components::RdmaDomain;
use crate::rdma_components::RdmaQueuePair;
use crate::rdma_components::get_registered_cuda_segments;
use crate::validate_execution_context;

/// Represents the state of a queue pair in the manager, either available or checked out.
#[derive(Debug, Clone)]
pub enum QueuePairState {
    Available(RdmaQueuePair),
    CheckedOut,
}

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
        remote: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<RdmaQueuePair>,
    },
    Connect {
        /// `other` - The ActorId of the actor to connect to
        other: ActorRef<RdmaManagerActor>,
        /// `endpoint` - Connection information needed to establish the RDMA connection
        endpoint: RdmaQpInfo,
    },
    InitializeQP {
        remote: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<bool>,
    },
    ConnectionInfo {
        /// `other` - The ActorId to get connection info for
        other: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return the connection info
        reply: OncePortRef<RdmaQpInfo>,
    },
    ReleaseQueuePair {
        /// `other` - The ActorId to release queue pair for  
        other: ActorRef<RdmaManagerActor>,
        /// `qp` - The queue pair to return (ownership transferred back)
        qp: RdmaQueuePair,
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
    // Map between ActorIds and their corresponding RdmaQueuePair
    qp_map: HashMap<ActorId, QueuePairState>,

    // MR configuration QP for self that cannot be loaned out
    loopback_qp: Option<RdmaQueuePair>,

    // The RDMA domain associated with this actor.
    //
    // The domain is responsible for managing the RDMA resources and configurations
    // specific to this actor. It encapsulates the context and protection domain
    // necessary for RDMA operations, ensuring that all RDMA activities are
    // performed within a consistent and isolated environment.
    //
    // This domain is initialized during the creation of the `RdmaManagerActor`
    // and is used throughout the actor's lifecycle to manage RDMA connections
    // and operations.
    domain: RdmaDomain,
    config: IbverbsConfig,

    // Flag indicating PyTorch CUDA allocator compatibility
    // True if both C10 CUDA allocator is enabled AND expandable segments are enabled
    pt_cuda_alloc: bool,

    // Map of unique RdmaMemoryRegionView to ibv_mr*.  In case of cuda w/ pytorch its -1
    // since its managed independently.  Only used for registration/deregistration purposes
    mr_map: HashMap<usize, usize>,
    // Id for next mrv created
    mrv_id: usize,
}

impl RdmaManagerActor {
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
    ) -> Result<RdmaMemoryRegionView, anyhow::Error> {
        unsafe {
            let mut mem_type: i32 = 0;
            let ptr = addr as cuda_sys::CUdeviceptr;
            let err = cuda_sys::cuPointerGetAttribute(
                &mut mem_type as *mut _ as *mut std::ffi::c_void,
                cuda_sys::CUpointer_attribute_enum::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                ptr,
            );
            let is_cuda = err == cuda_sys::CUresult::CUDA_SUCCESS;

            let access = rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;

            let mut mr: *mut rdmaxcel_sys::ibv_mr = std::ptr::null_mut();
            let mrv;

            if is_cuda && self.pt_cuda_alloc {
                // Get registered segments and check if our memory range is covered
                let mut maybe_mrv = self.find_cuda_segment_for_address(addr, size);
                // not found, lets re-sync with caching allocator  and retry
                if maybe_mrv.is_none() {
                    let qp = self.loopback_qp.as_mut().unwrap();
                    let err = rdmaxcel_sys::register_segments(
                        self.domain.pd,
                        qp.qp as *mut rdmaxcel_sys::ibv_qp,
                    );
                    if err != 0 {
                        let error_msg = get_rdmaxcel_error_message(err);
                        return Err(anyhow::anyhow!(
                            "RdmaXcel register_sements failed (addr: 0x{:x}, size: {}): {}",
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
                cuda_sys::cuMemGetHandleForAddressRange(
                    &mut fd as *mut i32 as *mut std::ffi::c_void,
                    addr as cuda_sys::CUdeviceptr,
                    size,
                    cuda_sys::CUmemRangeHandleType::CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                    0,
                );
                mr = rdmaxcel_sys::ibv_reg_dmabuf_mr(
                    self.domain.pd,
                    0,
                    size,
                    0,
                    fd,
                    access.0 as i32,
                );
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
                    self.domain.pd,
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
            Ok(mrv)
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
impl Actor for RdmaManagerActor {
    type Params = Option<IbverbsConfig>;

    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        if !ibverbs_supported() {
            return Err(anyhow::anyhow!(
                "Cannot create RdmaManagerActor because RDMA is not supported on this machine"
            ));
        }

        // Use provided config or default if none provided
        let mut config = params.unwrap_or_default();
        tracing::debug!("rdma is enabled, using device {}", config.device);

        let pt_cuda_alloc = crate::rdma_components::pt_cuda_allocator_compatibility();

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

        // Auto-detect device if needed
        let device = crate::device_selection::resolve_rdma_device(&config.device)
            .unwrap_or_else(|| config.device.clone());

        let domain = RdmaDomain::new(device)
            .map_err(|e| anyhow::anyhow!("rdmaManagerActor could not create domain: {}", e))?;

        Ok(Self {
            qp_map: HashMap::new(),
            loopback_qp: None,
            domain,
            config,
            pt_cuda_alloc,
            mr_map: HashMap::new(),
            mrv_id: 0,
        })
    }

    async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Create a loopback queue pair for self-communication
        let mut qp = RdmaQueuePair::new(self.domain.context, self.domain.pd, self.config.clone())
            .map_err(|e| anyhow::anyhow!("could not create RdmaQueuePair: {}", e))?;

        // Get connection info for loopback
        let endpoint = qp
            .get_qp_info()
            .map_err(|e| anyhow::anyhow!("could not get QP info: {}", e))?;

        // Connect to itself
        qp.connect(&endpoint)
            .map_err(|e| anyhow::anyhow!("could not connect to RDMA endpoint: {}", e))?;

        self.loopback_qp = Some(qp);
        tracing::debug!("successfully created special loopback connection");

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
        let mrv = self.register_mr(addr, size)?;

        Ok(RdmaBuffer {
            owner: cx.bind().clone(),
            mr_id: mrv.id,
            addr: mrv.rdma_addr,
            size: mrv.size,
            rkey: mrv.rkey,
            lkey: mrv.lkey,
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
        remote: ActorRef<RdmaManagerActor>,
    ) -> Result<RdmaQueuePair, anyhow::Error> {
        let remote_id = remote.actor_id().clone();

        // Check if queue pair exists in map.
        // IMPOTRANT we clone QP here, but its all simple metadata
        // and subsequent owner will update it and return it.
        match self.qp_map.get(&remote_id).cloned() {
            Some(QueuePairState::Available(qp)) => {
                // Queue pair exists and is available - return it
                self.qp_map.insert(remote_id, QueuePairState::CheckedOut);
                Ok(qp)
            }
            Some(QueuePairState::CheckedOut) => {
                // Queue pair exists but is already checked out
                Err(anyhow::anyhow!(
                    "Queue pair for actor {} is already checked out",
                    remote_id
                ))
            }
            None => {
                // Queue pair doesn't exist - need to create connection
                let is_loopback = remote_id == cx.bind::<RdmaManagerActor>().actor_id().clone();

                if is_loopback {
                    // Loopback connection setup
                    self.initialize_qp(cx, remote.clone()).await?;
                    let endpoint = self.connection_info(cx, remote.clone()).await?;
                    self.connect(cx, remote.clone(), endpoint).await?;
                } else {
                    // Remote connection setup
                    self.initialize_qp(cx, remote.clone()).await?;
                    remote.initialize_qp(cx, cx.bind().clone()).await?;
                    let remote_endpoint = remote.connection_info(cx, cx.bind().clone()).await?;
                    self.connect(cx, remote.clone(), remote_endpoint).await?;
                    let local_endpoint = self.connection_info(cx, remote.clone()).await?;
                    remote
                        .connect(cx, cx.bind().clone(), local_endpoint)
                        .await?;
                }

                // Now that connection is established, get the queue pair
                match self.qp_map.get(&remote_id).cloned() {
                    Some(QueuePairState::Available(qp)) => {
                        self.qp_map.insert(remote_id, QueuePairState::CheckedOut);
                        Ok(qp)
                    }
                    _ => Err(anyhow::anyhow!(
                        "Failed to create connection for actor {}",
                        remote_id
                    )),
                }
            }
        }
    }

    /// Convenience utility to create a new RdmaQueuePair.
    ///
    /// This function initializes a new RDMA connection with another actor if one doesn't already exist.
    /// It creates a new RdmaQueuePair associated with the specified actor ID and adds it to the
    /// connection map.
    ///
    /// # Arguments
    ///
    /// * `other` - The ActorRef of the remote actor to connect with
    async fn initialize_qp(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
    ) -> Result<bool, anyhow::Error> {
        let key = other.actor_id().clone();

        if let std::collections::hash_map::Entry::Vacant(e) = self.qp_map.entry(key) {
            let qp = RdmaQueuePair::new(self.domain.context, self.domain.pd, self.config.clone())
                .map_err(|e| anyhow::anyhow!("could not create RdmaQueuePair: {}", e))?;
            e.insert(QueuePairState::Available(qp));
            tracing::debug!("successfully created a connection with {:?}", other);
        }
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
        endpoint: RdmaQpInfo,
    ) -> Result<(), anyhow::Error> {
        tracing::debug!("connecting with {:?}", other);
        let other_id = other.actor_id().clone();

        match self.qp_map.get_mut(&other_id) {
            Some(QueuePairState::Available(qp)) => {
                qp.connect(&endpoint)
                    .map_err(|e| anyhow::anyhow!("could not connect to RDMA endpoint: {}", e))?;
                Ok(())
            }
            Some(QueuePairState::CheckedOut) => Err(anyhow::anyhow!(
                "Cannot connect: queue pair for actor {} is checked out",
                other_id
            )),
            None => Err(anyhow::anyhow!(
                "On connect, no connection found for actor {}",
                other_id
            )),
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
    ) -> Result<RdmaQpInfo, anyhow::Error> {
        tracing::debug!("getting connection info with {:?}", other);
        let other_id = other.actor_id().clone();

        match self.qp_map.get_mut(&other_id) {
            Some(QueuePairState::Available(qp)) => {
                let connection_info = qp.get_qp_info()?;
                Ok(connection_info)
            }
            Some(QueuePairState::CheckedOut) => Err(anyhow::anyhow!(
                "Cannot get connection info: queue pair for actor {} is checked out",
                other_id
            )),
            None => Err(anyhow::anyhow!(
                "No connection found for actor {}",
                other_id
            )),
        }
    }

    /// Releases a queue pair back to the HashMap
    ///
    /// This method returns a queue pair to the HashMap after the caller has finished
    /// using it. This completes the request/release cycle, similar to RdmaBuffer.
    ///
    /// # Arguments
    /// * `other` - The ActorRef to release queue pair for
    /// * `qp` - The queue pair to return (ownership transferred back)
    async fn release_queue_pair(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        qp: RdmaQueuePair,
    ) -> Result<(), anyhow::Error> {
        let remote_id = other.actor_id().clone();

        // Check if the queue pair is in the expected CheckedOut state
        match self.qp_map.get(&remote_id) {
            Some(QueuePairState::CheckedOut) => {
                // Restore the queue pair to Available state
                self.qp_map
                    .insert(remote_id.clone(), QueuePairState::Available(qp));
                tracing::debug!("Released queue pair for actor {:?}", remote_id);
                Ok(())
            }
            Some(QueuePairState::Available(_)) => Err(anyhow::anyhow!(
                "Cannot release queue pair for actor {}: queue pair is not checked out",
                remote_id
            )),
            None => Err(anyhow::anyhow!(
                "Cannot release queue pair for actor {}: no queue pair found",
                remote_id
            )),
        }
    }
}
