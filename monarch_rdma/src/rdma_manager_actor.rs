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
    IsConnected {
        /// `other` - The ActorId of the actor to check connection with
        other: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return whether the actors have connected
        reply: OncePortRef<bool>,
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
    qp_map: HashMap<ActorId, RdmaQueuePair>,

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

    // Map of memory region IDs to (RdmaMemoryRegionView, ibv_mr* as usize (optional)), representing registered memory regions
    mr_map: HashMap<usize, (RdmaMemoryRegionView, usize)>,
}

impl RdmaManagerActor {
    fn find_cuda_segment_for_address(
        &self,
        addr: usize,
        size: usize,
    ) -> Option<RdmaMemoryRegionView> {
        let registered_segments = get_registered_cuda_segments();
        for segment in registered_segments {
            let start_addr = segment.phys_address;
            let end_addr = start_addr + segment.phys_size;

            if start_addr <= addr && addr + size <= end_addr {
                let offset = addr - start_addr;
                return Some(RdmaMemoryRegionView {
                    virtual_addr: addr,
                    rdma_addr: segment.mr_addr + offset,
                    size,
                    lkey: segment.lkey,
                    rkey: segment.rkey,
                });
            }
        }
        None
    }

    fn register_mr(
        &mut self,
        actor_id: ActorId,
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
            let mut mrv: Option<RdmaMemoryRegionView> = None;

            if is_cuda && self.pt_cuda_alloc {
                // Get registered segments and check if our memory range is covered
                mrv = self.find_cuda_segment_for_address(addr, size);
                // not found, lets re-sync with caching allocator  and retry
                if mrv.is_none() {
                    let qp = self.qp_map.get(&actor_id).unwrap();
                    rdmaxcel_sys::register_segments(
                        self.domain.pd,
                        qp.qp as *mut rdmaxcel_sys::ibv_qp,
                    );
                    mrv = self.find_cuda_segment_for_address(addr, size);
                }
                // if still not found, throw exception
                if mrv.is_none() {
                    return Err(anyhow::anyhow!(
                        "MR registration failed for cuda (addr: 0x{:x}, size: {}), unable to find segment in CudaCachingAllocator",
                        addr,
                        size
                    ));
                }
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
                mrv = Some(RdmaMemoryRegionView {
                    virtual_addr: addr,
                    rdma_addr: (*mr).addr as usize,
                    size,
                    lkey: (*mr).lkey,
                    rkey: (*mr).rkey,
                });
            } else {
                // CPU memory path
                mr = rdmaxcel_sys::ibv_reg_mr(
                    self.domain.pd,
                    addr as *mut std::ffi::c_void,
                    size,
                    access.0 as i32,
                );

                if mr.is_null() {
                    return Err(anyhow::anyhow!("failed to register memory region (MR)"));
                }

                mrv = Some(RdmaMemoryRegionView {
                    virtual_addr: addr,
                    rdma_addr: (*mr).addr as usize,
                    size,
                    lkey: (*mr).lkey,
                    rkey: (*mr).rkey,
                });
            }

            let result_mrv = mrv.unwrap();
            self.mr_map.insert(addr, (result_mrv.clone(), mr as usize));
            Ok(result_mrv)
        }
    }

    fn deregister_mr(&mut self, id: usize) -> Result<(), anyhow::Error> {
        let mr_tuple = self.mr_map.remove(&id);
        if let Some((_, mr_ptr)) = mr_tuple {
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

        let domain = RdmaDomain::new(config.device.clone())
            .map_err(|e| anyhow::anyhow!("rdmaManagerActor could not create domain: {}", e))?;

        Ok(Self {
            qp_map: HashMap::new(),
            domain,
            config,
            pt_cuda_alloc,
            mr_map: HashMap::new(),
        })
    }

    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Create a loopback queue pair for self-communication
        let self_ref: ActorRef<RdmaManagerActor> = this.bind().clone();
        let self_id = self_ref.actor_id().clone();

        // Initialize the queue pair directly (without going through message handler)
        if !self.qp_map.contains_key(&self_id) {
            let mut qp =
                RdmaQueuePair::new(self.domain.context, self.domain.pd, self.config.clone())
                    .map_err(|e| anyhow::anyhow!("could not create RdmaQueuePair: {}", e))?;

            // Get connection info for loopback
            let endpoint = qp
                .get_qp_info()
                .map_err(|e| anyhow::anyhow!("could not get QP info: {}", e))?;

            // Connect to itself
            qp.connect(&endpoint)
                .map_err(|e| anyhow::anyhow!("could not connect to RDMA endpoint: {}", e))?;

            self.qp_map.insert(self_id, qp);
            tracing::debug!("successfully created loopback connection");
        }

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
        let mrv = if let Some((mrv_, _)) = self.mr_map.get(&addr) {
            mrv_
        } else {
            let actor_id = cx.bind::<RdmaManagerActor>().actor_id().clone();
            &self.register_mr(actor_id, addr, size)?
        };

        Ok(RdmaBuffer {
            owner: cx.bind().clone(),
            mr_id: mrv.virtual_addr,
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
    /// This function checks if a connection already exists with the specified remote actor.
    /// If not, it initializes a new queue pair and establishes a connection with the remote actor.
    /// It then retrieves the queue pair associated with the remote actor for communication.
    ///
    /// # Arguments
    ///
    /// * `this` - The context of the actor requesting the queue pair.
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
        if !self.is_connected(cx, remote.clone()).await? {
            let is_loopback =
                remote.actor_id().clone() == cx.bind::<RdmaManagerActor>().actor_id().clone();

            if is_loopback {
                self.initialize_qp(cx, remote.clone()).await?;
                let endpoint = self.connection_info(cx, remote.clone()).await?;
                self.connect(cx, remote.clone(), endpoint).await?;
            } else {
                self.initialize_qp(cx, remote.clone()).await?;
                remote.initialize_qp(cx, cx.bind().clone()).await?;
                let remote_endpoint = remote.connection_info(cx, cx.bind().clone()).await?;
                self.connect(cx, remote.clone(), remote_endpoint).await?;
                let local_endpoint = self.connection_info(cx, remote.clone()).await?;
                remote
                    .connect(cx, cx.bind().clone(), local_endpoint)
                    .await?;
            }
        }

        let qp = self
            .qp_map
            .get_mut(&remote.actor_id().clone())
            .ok_or_else(|| anyhow::anyhow!("on get, no connection found for actor {}", remote))?;
        Ok(qp.clone())
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
            e.insert(qp);
            tracing::debug!("successfully created a connection with {:?}", other);
        }
        Ok(true)
    }

    /// Checks if a connection exists with another actor.
    ///
    /// # Arguments
    /// * `other` - The ActorRef of the actor to check the connection with.
    ///
    /// # Returns
    /// * `bool` - Returns true if connected, false otherwise.
    async fn is_connected(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
    ) -> Result<bool, anyhow::Error> {
        tracing::debug!("checking if connected with {:?}", other);
        if !self.qp_map.contains_key(&other.actor_id().clone()) {
            return Ok(false);
        }
        let qp_state = self
            .qp_map
            .get_mut(&other.actor_id().clone())
            .unwrap()
            .state()?;
        Ok(qp_state == rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTS)
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
        let qp = self
            .qp_map
            .get_mut(&other.actor_id().clone())
            .ok_or_else(|| {
                anyhow::anyhow!("on connect, no connection found for actor {}", other)
            })?;
        qp.connect(&endpoint)
            .map_err(|e| anyhow::anyhow!("could not connect to RDMA endpoint: {}", e))?;
        Ok(())
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

        let connection_info = self
            .qp_map
            .get_mut(&other.actor_id().clone())
            .ok_or_else(|| anyhow::anyhow!("no connection found for actor {}", other))?
            .get_qp_info()?;
        Ok(connection_info)
    }
}
