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
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::backend::ibverbs::IbvQueuePair;
use crate::backend::ibverbs::PollTarget;

#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct RdmaBuffer {
    pub owner: ActorRef<RdmaManagerActor>,
    pub mr_id: usize,
    pub lkey: u32,
    pub rkey: u32,
    pub addr: usize,
    pub size: usize,
    pub device_name: String,
}
wirevalue::register_type!(RdmaBuffer);

impl RdmaBuffer {
    /// Read from the RdmaBuffer into the provided memory.
    ///
    /// This method transfers data from the buffer into the local memory region provided over RDMA.
    /// This involves calling a `Put` operation on the RdmaBuffer's actor side.
    ///
    /// # Arguments
    /// * `client` - The actor who is reading.
    /// * `remote` - RdmaBuffer representing the remote memory region
    /// * `timeout` - Timeout in seconds for the RDMA operation to complete.
    ///
    /// # Returns
    /// `Ok(bool)` indicating if the operation completed successfully.
    pub async fn read_into(
        &self,
        client: &impl context::Actor,
        remote: RdmaBuffer,
        timeout: u64,
    ) -> Result<bool, anyhow::Error> {
        tracing::debug!(
            "[buffer] reading from {:?} into remote ({:?}) at {:?}",
            self,
            remote.owner.actor_id(),
            remote,
        );
        let remote_owner = remote.owner.clone();

        let local_device = self.device_name.clone();
        let remote_device = remote.device_name.clone();
        let mut qp = self
            .owner
            .request_queue_pair(
                client,
                remote_owner.clone(),
                local_device.clone(),
                remote_device.clone(),
            )
            .await?;

        let wr_id = qp.put(self.clone(), remote)?;
        let result = self
            .wait_for_completion(&mut qp, PollTarget::Send, &wr_id, timeout)
            .await;

        // Release the queue pair back to the actor
        self.owner
            .release_queue_pair(client, remote_owner, local_device, remote_device, qp)
            .await?;

        result
    }

    /// Write from the provided memory into the RdmaBuffer.
    ///
    /// This method performs an RDMA write operation, transferring data from the caller's
    /// memory region to this buffer.
    /// This involves calling a `Fetch` operation on the RdmaBuffer's actor side.
    ///
    /// # Arguments
    /// * `client` - The actor who is writing.
    /// * `remote` - RdmaBuffer representing the remote memory region
    /// * `timeout` - Timeout in seconds for the RDMA operation to complete.
    ///
    /// # Returns
    /// `Ok(bool)` indicating if the operation completed successfully.
    pub async fn write_from(
        &self,
        client: &impl context::Actor,
        remote: RdmaBuffer,
        timeout: u64,
    ) -> Result<bool, anyhow::Error> {
        tracing::debug!(
            "[buffer] writing into {:?} from remote ({:?}) at {:?}",
            self,
            remote.owner.actor_id(),
            remote,
        );
        let remote_owner = remote.owner.clone(); // Clone before the move!

        // Extract device name from buffer, fallback to a default if not present
        let local_device = self.device_name.clone();
        let remote_device = remote.device_name.clone();

        let mut qp = self
            .owner
            .request_queue_pair(
                client,
                remote_owner.clone(),
                local_device.clone(),
                remote_device.clone(),
            )
            .await?;
        let wr_id = qp.get(self.clone(), remote)?;
        let result = self
            .wait_for_completion(&mut qp, PollTarget::Send, &wr_id, timeout)
            .await;

        // Release the queue pair back to the actor
        self.owner
            .release_queue_pair(client, remote_owner, local_device, remote_device, qp)
            .await?;

        result
    }
    /// Waits for the completion of RDMA operations.
    ///
    /// This method polls the completion queue until all specified work requests complete
    /// or until the timeout is reached.
    ///
    /// # Arguments
    /// * `qp` - The RDMA Queue Pair to poll for completion
    /// * `poll_target` - Which CQ to poll (Send or Recv)
    /// * `expected_wr_ids` - The work request IDs to wait for
    /// * `timeout` - Timeout in seconds for the RDMA operation to complete.
    ///
    /// # Returns
    /// `Ok(true)` if all operations complete successfully within the timeout,
    /// or an error if the timeout is reached
    async fn wait_for_completion(
        &self,
        qp: &mut IbvQueuePair,
        poll_target: PollTarget,
        expected_wr_ids: &[u64],
        timeout: u64,
    ) -> Result<bool, anyhow::Error> {
        let timeout = Duration::from_secs(timeout);
        let start_time = std::time::Instant::now();

        let mut remaining: std::collections::HashSet<u64> =
            expected_wr_ids.iter().copied().collect();

        while start_time.elapsed() < timeout {
            if remaining.is_empty() {
                return Ok(true);
            }

            let wr_ids_to_poll: Vec<u64> = remaining.iter().copied().collect();
            match qp.poll_completion(poll_target, &wr_ids_to_poll) {
                Ok(completions) => {
                    for (wr_id, _wc) in completions {
                        remaining.remove(&wr_id);
                    }
                    if remaining.is_empty() {
                        return Ok(true);
                    }
                    RealClock.sleep(Duration::from_millis(1)).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "RDMA polling completion failed: {} [lkey={}, rkey={}, addr=0x{:x}, size={}]",
                        e,
                        self.lkey,
                        self.rkey,
                        self.addr,
                        self.size
                    ));
                }
            }
        }
        tracing::error!(
            "timed out while waiting on request completion for wr_ids={:?}",
            remaining
        );
        Err(anyhow::anyhow!(
            "[buffer({:?})] rdma operation did not complete in time (expected wr_ids={:?})",
            self,
            expected_wr_ids
        ))
    }

    /// Drop the buffer and release remote handles.
    ///
    /// This method calls the owning RdmaManagerActor to release the buffer and clean up
    /// associated memory regions. This is typically called when the buffer is no longer
    /// needed and resources should be freed.
    ///
    /// # Arguments
    /// * `client` - Mailbox used for communication
    ///
    /// # Returns
    /// `Ok(())` if the operation completed successfully.
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
