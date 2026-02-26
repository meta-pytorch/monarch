/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # RDMA Components
//!
//! This module provides the core RDMA building blocks for establishing and managing RDMA connections.
//!
//! ## Core Components
//!
//! * `IbvDomain` - Manages RDMA resources including context, protection domain, and memory region
//! * `IbvQueuePair` - Handles communication between endpoints via queue pairs and completion queues
//!
//! ## RDMA Overview
//!
//! Remote Direct Memory Access (RDMA) allows direct memory access from the memory of one computer
//! into the memory of another without involving either computer's operating system. This permits
//! high-throughput, low-latency networking with minimal CPU overhead.
//!
//! ## Connection Architecture
//!
//! The module manages the following ibverbs primitives:
//!
//! 1. **Queue Pairs (QP)**: Each connection has a send queue and a receive queue
//! 2. **Completion Queues (CQ)**: Events are reported when operations complete
//! 3. **Memory Regions (MR)**: Memory must be registered with the RDMA device before use
//! 4. **Protection Domains (PD)**: Provide isolation between different connections
//!
//! ## Connection Lifecycle
//!
//! 1. Create an `IbvDomain` with `new()`
//! 2. Create an `IbvQueuePair` from the domain
//! 3. Exchange connection info with remote peer (application must handle this)
//! 4. Connect to remote endpoint with `connect()`
//! 5. Perform RDMA operations (read/write)
//! 6. Poll for completions
//! 7. Resources are cleaned up when dropped

/// Maximum size for a single RDMA operation in bytes (1 GiB)
use std::fs;
use std::result::Result;
use std::time::Duration;

use hyperactor::ActorRef;
use hyperactor::context;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::GetIbvActorRefClient;
use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::RdmaOpType;
use crate::backend::ibverbs::IbvBuffer;
use crate::backend::ibverbs::IbvOp;
use crate::backend::ibverbs::manager_actor::IbvManagerActor;
use crate::backend::ibverbs::manager_actor::IbvSubmitClient;

#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct RdmaRemoteBuffer {
    pub owner: ActorRef<RdmaManagerActor>,
    pub mr_id: usize,
    pub lkey: u32,
    pub rkey: u32,
    pub addr: usize,
    pub size: usize,
    pub device_name: String,
}
wirevalue::register_type!(RdmaRemoteBuffer);

impl From<&RdmaRemoteBuffer> for IbvBuffer {
    fn from(buf: &RdmaRemoteBuffer) -> Self {
        IbvBuffer {
            mr_id: buf.mr_id,
            lkey: buf.lkey,
            rkey: buf.rkey,
            addr: buf.addr,
            size: buf.size,
            device_name: buf.device_name.clone(),
        }
    }
}

impl RdmaRemoteBuffer {
    /// Push data from this local buffer into the remote buffer (local→remote).
    ///
    /// Both buffers must already be registered. Resolves the local
    /// [`IbvManagerActor`] via [`IbvManagerActor::local_handle`] and sends
    /// an [`IbvSubmit`] with pre-registered [`IbvBuffer`]s.
    pub async fn read_into(
        &self,
        client: &(impl context::Actor + Send + Sync),
        remote: RdmaRemoteBuffer,
        timeout: u64,
    ) -> Result<bool, anyhow::Error> {
        let local_ibv = IbvManagerActor::local_handle(client).await?;
        let remote_ibv_ref = remote
            .owner
            .get_ibv_actor_ref(client)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote owner has no ibverbs backend"))?;
        IbvSubmitClient::ibv_submit(
            &local_ibv,
            client,
            IbvOp {
                op_type: RdmaOpType::WriteFromLocal,
                local_buffer: IbvBuffer::from(self),
                remote_buffer: IbvBuffer::from(&remote),
                remote_manager: remote_ibv_ref,
            },
            Duration::from_secs(timeout),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
        Ok(true)
    }

    /// Pull data from the remote buffer into this local buffer (remote→local).
    ///
    /// Both buffers must already be registered. Resolves the local
    /// [`IbvManagerActor`] via [`IbvManagerActor::local_handle`] and sends
    /// an [`IbvSubmit`] with pre-registered [`IbvBuffer`]s.
    pub async fn write_from(
        &self,
        client: &(impl context::Actor + Send + Sync),
        remote: RdmaRemoteBuffer,
        timeout: u64,
    ) -> Result<bool, anyhow::Error> {
        let local_ibv = IbvManagerActor::local_handle(client).await?;
        let remote_ibv_ref = remote
            .owner
            .get_ibv_actor_ref(client)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote owner has no ibverbs backend"))?;
        IbvSubmitClient::ibv_submit(
            &local_ibv,
            client,
            IbvOp {
                op_type: RdmaOpType::ReadIntoLocal,
                local_buffer: IbvBuffer::from(self),
                remote_buffer: IbvBuffer::from(&remote),
                remote_manager: remote_ibv_ref,
            },
            Duration::from_secs(timeout),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
        Ok(true)
    }

    /// Drop the buffer and release remote handles.
    pub async fn drop_buffer(&self, client: &impl context::Actor) -> Result<(), anyhow::Error> {
        tracing::debug!("[buffer] dropping buffer {:?}", self);
        self.owner.release_buffer(client, self.clone()).await?;
        Ok(())
    }
}

/// Utility to validate execution context.
///
/// Remote Execution environments do not always have access to the nvidia_peermem module
/// and/or set the PeerMappingOverride parameter due to security. This function can be
/// used to validate that the execution context when running operations that need this
/// functionality (ie. cudaHostRegisterIoMemory).
///
/// # Returns
///
/// * `Ok(())` if the execution context is valid
/// * `Err(anyhow::Error)` if the execution context is invalid
pub async fn validate_execution_context() -> Result<(), anyhow::Error> {
    // Check for nvidia peermem
    match fs::read_to_string("/proc/modules") {
        Ok(contents) => {
            if !contents.contains("nvidia_peermem") {
                return Err(anyhow::anyhow!(
                    "nvidia_peermem module not found in /proc/modules"
                ));
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e));
        }
    }

    // Test file access to nvidia params
    match fs::read_to_string("/proc/driver/nvidia/params") {
        Ok(contents) => {
            if !contents.contains("PeerMappingOverride=1") {
                return Err(anyhow::anyhow!(
                    "PeerMappingOverride=1 not found in /proc/driver/nvidia/params"
                ));
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e));
        }
    }
    Ok(())
}

/// Get all segments that have been registered with MRs
///
/// # Returns
/// * `Vec<SegmentInfo>` - Vector containing all registered segment information
pub fn get_registered_cuda_segments() -> Vec<rdmaxcel_sys::rdma_segment_info_t> {
    unsafe {
        let segment_count = rdmaxcel_sys::rdma_get_active_segment_count();
        if segment_count <= 0 {
            return Vec::new();
        }

        let mut segments = vec![
            std::mem::MaybeUninit::<rdmaxcel_sys::rdma_segment_info_t>::zeroed()
                .assume_init();
            segment_count as usize
        ];
        let actual_count =
            rdmaxcel_sys::rdma_get_all_segment_info(segments.as_mut_ptr(), segment_count);

        if actual_count > 0 {
            segments.truncate(actual_count as usize);
            segments
        } else {
            Vec::new()
        }
    }
}

/// Segment scanner callback type alias for convenience.
pub type SegmentScannerFn = rdmaxcel_sys::RdmaxcelSegmentScannerFn;

/// Register a segment scanner callback.
///
/// The scanner callback is called during RDMA segment registration to discover
/// CUDA memory segments. The callback should fill the provided buffer with
/// segment information and return the total count of segments found.
///
/// If the returned count exceeds the buffer size, the caller will allocate
/// a larger buffer and retry.
///
/// Pass `None` to unregister the scanner.
///
/// # Safety
///
/// The provided callback function must be safe to call from C code and must
/// properly handle the segment buffer.
pub fn register_segment_scanner(scanner: SegmentScannerFn) {
    // SAFETY: We are registering a callback function pointer with rdmaxcel.
    unsafe { rdmaxcel_sys::rdmaxcel_register_segment_scanner(scanner) }
}
