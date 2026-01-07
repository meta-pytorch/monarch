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
//! * `RdmaDomain` - Manages RDMA resources including context, protection domain, and memory region
//! * `RdmaQueuePair` - Handles communication between endpoints via queue pairs and completion queues
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
//! 1. Create an `RdmaDomain` with `new()`
//! 2. Create an `RdmaQueuePair` from the domain
//! 3. Exchange connection info with remote peer (application must handle this)
//! 4. Connect to remote endpoint with `connect()`
//! 5. Perform RDMA operations (read/write)
//! 6. Poll for completions
//! 7. Resources are cleaned up when dropped

/// Maximum size for a single RDMA operation in bytes (1 GiB)
const MAX_RDMA_MSG_SIZE: usize = 1024 * 1024 * 1024;

use std::ffi::CStr;
use std::fs;
use std::io::Error;
use std::result::Result;
use std::thread::sleep;
use std::time::Duration;

use hyperactor::ActorRef;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::RdmaDevice;
use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::ibverbs_primitives::Gid;
use crate::ibverbs_primitives::IbvWc;
use crate::ibverbs_primitives::IbverbsConfig;
use crate::ibverbs_primitives::RdmaOperation;
use crate::ibverbs_primitives::RdmaQpInfo;
use crate::ibverbs_primitives::resolve_qp_type;

#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct DoorBell {
    pub src_ptr: usize,
    pub dst_ptr: usize,
    pub size: usize,
}
wirevalue::register_type!(DoorBell);

impl DoorBell {
    /// Rings the doorbell to trigger the execution of previously enqueued operations.
    ///
    /// This method uses unsafe code to directly interact with the RDMA device,
    /// sending a signal from the source pointer to the destination pointer.
    ///
    /// # Returns
    /// * `Ok(())` if the operation is successful.
    /// * `Err(anyhow::Error)` if an error occurs during the operation.
    pub fn ring(&self) -> Result<(), anyhow::Error> {
        unsafe {
            let src_ptr = self.src_ptr as *mut std::ffi::c_void;
            let dst_ptr = self.dst_ptr as *mut std::ffi::c_void;
            rdmaxcel_sys::db_ring(dst_ptr, src_ptr);
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Named, Clone)]
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
        qp: &mut RdmaQueuePair,
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

/// Represents a domain for RDMA operations, encapsulating the necessary resources
/// for establishing and managing RDMA connections.
///
/// An `RdmaDomain` manages the context, protection domain (PD), and memory region (MR)
/// required for RDMA operations. It provides the foundation for creating queue pairs
/// and establishing connections between RDMA devices.
///
/// # Fields
///
/// * `context`: A pointer to the RDMA device context, representing the connection to the RDMA device.
/// * `pd`: A pointer to the protection domain, which provides isolation between different connections.
#[derive(Clone)]
pub struct RdmaDomain {
    pub context: *mut rdmaxcel_sys::ibv_context,
    pub pd: *mut rdmaxcel_sys::ibv_pd,
}

impl std::fmt::Debug for RdmaDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaDomain")
            .field("context", &format!("{:p}", self.context))
            .field("pd", &format!("{:p}", self.pd))
            .finish()
    }
}

// SAFETY:
// This function contains code marked unsafe as it interacts with the Rdma device through rdmaxcel_sys calls.
// RdmaDomain is `Send` because the raw pointers to ibverbs structs can be
// accessed from any thread, and it is safe to drop `RdmaDomain` (and run the
// ibverbs destructors) from any thread.
unsafe impl Send for RdmaDomain {}

// SAFETY:
// This function contains code marked unsafe as it interacts with the Rdma device through rdmaxcel_sys calls.
// RdmaDomain is `Sync` because the underlying ibverbs APIs are thread-safe.
unsafe impl Sync for RdmaDomain {}

impl Drop for RdmaDomain {
    fn drop(&mut self) {
        unsafe {
            rdmaxcel_sys::ibv_dealloc_pd(self.pd);
        }
    }
}

impl RdmaDomain {
    /// Creates a new RdmaDomain.
    ///
    /// This function initializes the RDMA device context, creates a protection domain,
    /// and registers a memory region with appropriate access permissions.
    ///
    /// SAFETY:
    /// Our memory region (MR) registration uses implicit ODP for RDMA access, which maps large virtual
    /// address ranges without explicit pinning. This is convenient, but it broadens the memory footprint
    /// exposed to the NIC and introduces a security liability.
    ///
    /// We currently assume a trusted, single-environment and are not enforcing finer-grained memory isolation
    /// at this layer. We plan to investigate mitigations - such as memory windows or tighter registration
    /// boundaries in future follow-ups.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration settings for the RDMA operations
    ///
    /// # Errors
    ///
    /// This function may return errors if:
    /// * No RDMA devices are found
    /// * The specified device cannot be found
    /// * Device context creation fails
    /// * Protection domain allocation fails
    /// * Memory region registration fails
    pub fn new(device: RdmaDevice) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating RdmaDomain for device {}", device.name());
        // SAFETY:
        // This code uses unsafe rdmaxcel_sys calls to interact with the RDMA device, but is safe because:
        // - All pointers are properly initialized and checked for null before use
        // - Memory registration follows the ibverbs API contract with proper access flags
        // - Resources are properly cleaned up in error cases to prevent leaks
        // - The operations follow the documented RDMA protocol for device initialization
        unsafe {
            // Get the device based on the provided RdmaDevice
            let device_name = device.name();
            let mut num_devices = 0i32;
            let devices = rdmaxcel_sys::ibv_get_device_list(&mut num_devices as *mut _);

            if devices.is_null() || num_devices == 0 {
                return Err(anyhow::anyhow!("no RDMA devices found"));
            }

            // Find the device with the matching name
            let mut device_ptr = std::ptr::null_mut();
            for i in 0..num_devices {
                let dev = *devices.offset(i as isize);
                let dev_name =
                    CStr::from_ptr(rdmaxcel_sys::ibv_get_device_name(dev)).to_string_lossy();

                if dev_name == *device_name {
                    device_ptr = dev;
                    break;
                }
            }

            // If we didn't find the device, return an error
            if device_ptr.is_null() {
                rdmaxcel_sys::ibv_free_device_list(devices);
                return Err(anyhow::anyhow!("device '{}' not found", device_name));
            }
            tracing::info!("using RDMA device: {}", device_name);

            // Open device
            let context = rdmaxcel_sys::ibv_open_device(device_ptr);
            if context.is_null() {
                rdmaxcel_sys::ibv_free_device_list(devices);
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!("failed to create context: {}", os_error));
            }

            // Create protection domain
            let pd = rdmaxcel_sys::ibv_alloc_pd(context);
            if pd.is_null() {
                rdmaxcel_sys::ibv_close_device(context);
                rdmaxcel_sys::ibv_free_device_list(devices);
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!(
                    "failed to create protection domain (PD): {}",
                    os_error
                ));
            }

            // Avoids memory leaks
            rdmaxcel_sys::ibv_free_device_list(devices);

            let domain = RdmaDomain { context, pd };

            Ok(domain)
        }
    }
}
/// Enum to specify which completion queue to poll
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PollTarget {
    Send,
    Recv,
}

/// Represents an RDMA Queue Pair (QP) that enables communication between two endpoints.
///
/// An `RdmaQueuePair` encapsulates the send and receive queues, completion queue,
/// and other resources needed for RDMA communication. It provides methods for
/// establishing connections and performing RDMA operations like read and write.
///
/// # Fields
///
/// * `send_cq` - Send Completion Queue pointer for tracking send operation completions
/// * `recv_cq` - Receive Completion Queue pointer for tracking receive operation completions
/// * `qp` - Queue Pair pointer that manages send and receive operations
/// * `dv_qp` - Pointer to the mlx5 device-specific queue pair structure
/// * `dv_send_cq` - Pointer to the mlx5 device-specific send completion queue structure
/// * `dv_recv_cq` - Pointer to the mlx5 device-specific receive completion queue structure
/// * `context` - RDMA device context pointer
/// * `config` - Configuration settings for the queue pair
///
/// # Connection Lifecycle
///
/// 1. Create with `new()` from an `RdmaDomain`
/// 2. Get connection info with `get_qp_info()`
/// 3. Exchange connection info with remote peer (application must handle this)
/// 4. Connect to remote endpoint with `connect()`
/// 5. Perform RDMA operations with `put()` or `get()`
/// 6. Poll for completions with `poll_send_completion(wr_id)` or `poll_recv_completion(wr_id)`
///
/// # Notes
/// - The `qp` field stores a pointer to `rdmaxcel_qp_t` (not `ibv_qp`)
/// - `rdmaxcel_qp_t` contains atomic counters and completion caches internally
/// - This makes RdmaQueuePair trivially Clone and Serialize
/// - Multiple clones share the same underlying rdmaxcel_qp_t via the pointer
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
pub struct RdmaQueuePair {
    pub send_cq: usize,    // *mut rdmaxcel_sys::ibv_cq,
    pub recv_cq: usize,    // *mut rdmaxcel_sys::ibv_cq,
    pub qp: usize,         // *mut rdmaxcel_sys::rdmaxcel_qp_t
    pub dv_qp: usize,      // *mut rdmaxcel_sys::mlx5dv_qp,
    pub dv_send_cq: usize, // *mut rdmaxcel_sys::mlx5dv_cq,
    pub dv_recv_cq: usize, // *mut rdmaxcel_sys::mlx5dv_cq,
    context: usize,        // *mut rdmaxcel_sys::ibv_context,
    config: IbverbsConfig,
}
wirevalue::register_type!(RdmaQueuePair);

impl RdmaQueuePair {
    /// Applies hardware initialization delay if this is the first operation since RTS.
    ///
    /// This ensures the hardware has sufficient time to settle after reaching
    /// Ready-to-Send state before the first actual operation.
    fn apply_first_op_delay(&self, wr_id: u64) {
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            if wr_id == 0 {
                let rts_timestamp = rdmaxcel_sys::rdmaxcel_qp_load_rts_timestamp(qp);
                assert!(
                    rts_timestamp != u64::MAX,
                    "First operation attempted before queue pair reached RTS state! Call connect() first."
                );
                let current_nanos = RealClock
                    .system_time_now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let elapsed_nanos = current_nanos - rts_timestamp;
                let elapsed = Duration::from_nanos(elapsed_nanos);
                let init_delay = Duration::from_millis(self.config.hw_init_delay_ms);
                if elapsed < init_delay {
                    let remaining_delay = init_delay - elapsed;
                    sleep(remaining_delay);
                }
            }
        }
    }

    /// Creates a new RdmaQueuePair from a given RdmaDomain.
    ///
    /// This function initializes a new Queue Pair (QP) and associated Completion Queue (CQ)
    /// using the resources from the provided RdmaDomain. The QP is created in the RESET state
    /// and must be transitioned to other states via the `connect()` method before use.
    ///
    /// # Arguments
    ///
    /// * `domain` - Reference to an RdmaDomain that provides the context, protection domain,
    ///   and memory region for this queue pair
    ///
    /// # Returns
    ///
    /// * `Result<Self>` - A new RdmaQueuePair instance or an error if creation fails
    ///
    /// # Errors
    ///
    /// This function may return errors if:
    /// * Completion queue (CQ) creation fails
    /// * Queue pair (QP) creation fails
    pub fn new(
        context: *mut rdmaxcel_sys::ibv_context,
        pd: *mut rdmaxcel_sys::ibv_pd,
        config: IbverbsConfig,
    ) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating an RdmaQueuePair from config {}", config);
        unsafe {
            // Resolve Auto to a concrete QP type based on device capabilities
            let resolved_qp_type = resolve_qp_type(config.qp_type);
            let qp = rdmaxcel_sys::rdmaxcel_qp_create(
                context,
                pd,
                config.cq_entries,
                config.max_send_wr.try_into().unwrap(),
                config.max_recv_wr.try_into().unwrap(),
                config.max_send_sge.try_into().unwrap(),
                config.max_recv_sge.try_into().unwrap(),
                resolved_qp_type,
            );

            if qp.is_null() {
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!(
                    "failed to create queue pair (QP): {}",
                    os_error
                ));
            }

            let send_cq = (*(*qp).ibv_qp).send_cq;
            let recv_cq = (*(*qp).ibv_qp).recv_cq;

            // mlx5dv provider APIs
            let dv_qp = rdmaxcel_sys::create_mlx5dv_qp((*qp).ibv_qp);
            let dv_send_cq = rdmaxcel_sys::create_mlx5dv_send_cq((*qp).ibv_qp);
            let dv_recv_cq = rdmaxcel_sys::create_mlx5dv_recv_cq((*qp).ibv_qp);

            if dv_qp.is_null() || dv_send_cq.is_null() || dv_recv_cq.is_null() {
                rdmaxcel_sys::ibv_destroy_cq((*(*qp).ibv_qp).recv_cq);
                rdmaxcel_sys::ibv_destroy_cq((*(*qp).ibv_qp).send_cq);
                rdmaxcel_sys::ibv_destroy_qp((*qp).ibv_qp);
                return Err(anyhow::anyhow!(
                    "failed to init mlx5dv_qp or completion queues"
                ));
            }

            // GPU Direct RDMA specific registrations
            if config.use_gpu_direct {
                let ret = rdmaxcel_sys::register_cuda_memory(dv_qp, dv_recv_cq, dv_send_cq);
                if ret != 0 {
                    rdmaxcel_sys::ibv_destroy_cq((*(*qp).ibv_qp).recv_cq);
                    rdmaxcel_sys::ibv_destroy_cq((*(*qp).ibv_qp).send_cq);
                    rdmaxcel_sys::ibv_destroy_qp((*qp).ibv_qp);
                    return Err(anyhow::anyhow!(
                        "failed to register GPU Direct RDMA memory: {:?}",
                        ret
                    ));
                }
            }
            Ok(RdmaQueuePair {
                send_cq: send_cq as usize,
                recv_cq: recv_cq as usize,
                qp: qp as usize,
                dv_qp: qp as usize,
                dv_send_cq: dv_send_cq as usize,
                dv_recv_cq: dv_recv_cq as usize,
                context: context as usize,
                config,
            })
        }
    }

    /// Returns the information required for a remote peer to connect to this queue pair.
    ///
    /// This method retrieves the local queue pair attributes and port information needed by
    /// a remote peer to establish an RDMA connection. The returned `RdmaQpInfo` contains
    /// the queue pair number, LID, GID, and other necessary connection parameters.
    ///
    /// # Returns
    ///
    /// * `Result<RdmaQpInfo>` - Connection information for the remote peer or an error
    ///
    /// # Errors
    ///
    /// This function may return errors if:
    /// * Port attribute query fails
    /// * GID query fails
    pub fn get_qp_info(&mut self) -> Result<RdmaQpInfo, anyhow::Error> {
        // SAFETY:
        // This code uses unsafe rdmaxcel_sys calls to query RDMA device information, but is safe because:
        // - All pointers are properly initialized before use
        // - Port and GID queries follow the documented ibverbs API contract
        // - Error handling properly checks return codes from ibverbs functions
        // - The memory address provided is only stored, not dereferenced in this function
        unsafe {
            let context = self.context as *mut rdmaxcel_sys::ibv_context;
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let mut port_attr = rdmaxcel_sys::ibv_port_attr::default();
            let errno = rdmaxcel_sys::ibv_query_port(
                context,
                self.config.port_num,
                &mut port_attr as *mut rdmaxcel_sys::ibv_port_attr as *mut _,
            );
            if errno != 0 {
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!(
                    "Failed to query port attributes: {}",
                    os_error
                ));
            }

            let mut gid = Gid::default();
            let ret = rdmaxcel_sys::ibv_query_gid(
                context,
                self.config.port_num,
                i32::from(self.config.gid_index),
                gid.as_mut(),
            );
            if ret != 0 {
                return Err(anyhow::anyhow!("Failed to query GID"));
            }

            Ok(RdmaQpInfo {
                qp_num: (*(*qp).ibv_qp).qp_num,
                lid: port_attr.lid,
                gid: Some(gid),
                psn: self.config.psn,
            })
        }
    }

    pub fn state(&mut self) -> Result<u32, anyhow::Error> {
        // SAFETY: This block interacts with the RDMA device through rdmaxcel_sys calls.
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let mut qp_attr = rdmaxcel_sys::ibv_qp_attr {
                ..Default::default()
            };
            let mut qp_init_attr = rdmaxcel_sys::ibv_qp_init_attr {
                ..Default::default()
            };
            let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE;
            let errno = rdmaxcel_sys::ibv_query_qp(
                (*qp).ibv_qp,
                &mut qp_attr,
                mask.0 as i32,
                &mut qp_init_attr,
            );
            if errno != 0 {
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!("failed to query QP state: {}", os_error));
            }
            Ok(qp_attr.qp_state)
        }
    }
    /// Connect to a remote Rdma connection point.
    ///
    /// This performs the necessary QP state transitions (INIT->RTR->RTS) to establish a connection.
    ///
    /// # Arguments
    ///
    /// * `connection_info` - The remote connection info to connect to
    pub fn connect(&mut self, connection_info: &RdmaQpInfo) -> Result<(), anyhow::Error> {
        // SAFETY:
        // This unsafe block is necessary because we're interacting with the RDMA device through rdmaxcel_sys calls.
        // The operations are safe because:
        // 1. We're following the documented ibverbs API contract
        // 2. All pointers used are properly initialized and owned by this struct
        // 3. The QP state transitions (INIT->RTR->RTS) follow the required RDMA connection protocol
        // 4. Memory access is properly bounded by the registered memory regions
        unsafe {
            // Transition to INIT
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;

            let qp_access_flags = rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;

            let mut qp_attr = rdmaxcel_sys::ibv_qp_attr {
                qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_INIT,
                qp_access_flags: qp_access_flags.0,
                pkey_index: self.config.pkey_index,
                port_num: self.config.port_num,
                ..Default::default()
            };

            let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_PORT
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;

            let errno = rdmaxcel_sys::ibv_modify_qp((*qp).ibv_qp, &mut qp_attr, mask.0 as i32);
            if errno != 0 {
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!(
                    "failed to transition QP to INIT: {}",
                    os_error
                ));
            }

            // Transition to RTR (Ready to Receive)
            let mut qp_attr = rdmaxcel_sys::ibv_qp_attr {
                qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTR,
                path_mtu: self.config.path_mtu,
                dest_qp_num: connection_info.qp_num,
                rq_psn: connection_info.psn,
                max_dest_rd_atomic: self.config.max_dest_rd_atomic,
                min_rnr_timer: self.config.min_rnr_timer,
                ah_attr: rdmaxcel_sys::ibv_ah_attr {
                    dlid: connection_info.lid,
                    sl: 0,
                    src_path_bits: 0,
                    port_num: self.config.port_num,
                    grh: Default::default(),
                    ..Default::default()
                },
                ..Default::default()
            };

            // If the remote connection info contains a Gid, the routing will be global.
            // Otherwise, it will be local, i.e. using LID.
            if let Some(gid) = connection_info.gid {
                qp_attr.ah_attr.is_global = 1;
                qp_attr.ah_attr.grh.dgid = gid.into();
                qp_attr.ah_attr.grh.hop_limit = 0xff;
                qp_attr.ah_attr.grh.sgid_index = self.config.gid_index;
            } else {
                // Use LID-based routing, e.g. for Infiniband/RoCEv1
                qp_attr.ah_attr.is_global = 0;
            }

            let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_AV
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_PATH_MTU
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_DEST_QPN
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_RQ_PSN
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;

            let errno = rdmaxcel_sys::ibv_modify_qp((*qp).ibv_qp, &mut qp_attr, mask.0 as i32);
            if errno != 0 {
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!(
                    "failed to transition QP to RTR: {}",
                    os_error
                ));
            }

            // Transition to RTS (Ready to Send)
            let mut qp_attr = rdmaxcel_sys::ibv_qp_attr {
                qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTS,
                sq_psn: self.config.psn,
                max_rd_atomic: self.config.max_rd_atomic,
                retry_cnt: self.config.retry_cnt,
                rnr_retry: self.config.rnr_retry,
                timeout: self.config.qp_timeout,
                ..Default::default()
            };

            let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_TIMEOUT
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_RETRY_CNT
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_SQ_PSN
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_RNR_RETRY
                | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

            let errno = rdmaxcel_sys::ibv_modify_qp((*qp).ibv_qp, &mut qp_attr, mask.0 as i32);
            if errno != 0 {
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!(
                    "failed to transition QP to RTS: {}",
                    os_error
                ));
            }
            tracing::debug!(
                "connection sequence has successfully completed (qp: {:?})",
                qp
            );

            // Record RTS time now that the queue pair is ready to send
            let rts_timestamp_nanos = RealClock
                .system_time_now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            rdmaxcel_sys::rdmaxcel_qp_store_rts_timestamp(qp, rts_timestamp_nanos);

            Ok(())
        }
    }

    pub fn recv(&mut self, lhandle: RdmaBuffer, rhandle: RdmaBuffer) -> Result<u64, anyhow::Error> {
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let idx = rdmaxcel_sys::rdmaxcel_qp_fetch_add_recv_wqe_idx(qp);
            self.post_op(
                0,
                lhandle.lkey,
                0,
                idx,
                true,
                RdmaOperation::Recv,
                0,
                rhandle.rkey,
            )
            .unwrap();
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_recv_db_idx(qp);
            Ok(idx)
        }
    }

    pub fn put_with_recv(
        &mut self,
        lhandle: RdmaBuffer,
        rhandle: RdmaBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let idx = rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_wqe_idx(qp);
            self.post_op(
                lhandle.addr,
                lhandle.lkey,
                lhandle.size,
                idx,
                true,
                RdmaOperation::WriteWithImm,
                rhandle.addr,
                rhandle.rkey,
            )
            .unwrap();
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_db_idx(qp);
            Ok(vec![idx])
        }
    }

    pub fn put(
        &mut self,
        lhandle: RdmaBuffer,
        rhandle: RdmaBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        let total_size = lhandle.size;
        if rhandle.size < total_size {
            return Err(anyhow::anyhow!(
                "Remote buffer size ({}) is smaller than local buffer size ({})",
                rhandle.size,
                total_size
            ));
        }

        let mut remaining = total_size;
        let mut offset = 0;
        let mut wr_ids = Vec::new();
        while remaining > 0 {
            let chunk_size = std::cmp::min(remaining, MAX_RDMA_MSG_SIZE);
            let idx = unsafe {
                rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_wqe_idx(
                    self.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
                )
            };
            wr_ids.push(idx);
            self.post_op(
                lhandle.addr + offset,
                lhandle.lkey,
                chunk_size,
                idx,
                true,
                RdmaOperation::Write,
                rhandle.addr + offset,
                rhandle.rkey,
            )?;
            unsafe {
                rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_db_idx(
                    self.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
                );
            }

            remaining -= chunk_size;
            offset += chunk_size;
        }

        Ok(wr_ids)
    }

    /// Get a doorbell for the queue pair.
    ///
    /// This method returns a doorbell that can be used to trigger the execution of
    /// previously enqueued operations.
    ///
    /// # Returns
    ///
    /// * `Result<DoorBell, anyhow::Error>` - A doorbell for the queue pair
    pub fn ring_doorbell(&mut self) -> Result<(), anyhow::Error> {
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let dv_qp = self.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
            let base_ptr = (*dv_qp).sq.buf as *mut u8;
            let wqe_cnt = (*dv_qp).sq.wqe_cnt;
            let stride = (*dv_qp).sq.stride;
            let send_wqe_idx = rdmaxcel_sys::rdmaxcel_qp_load_send_wqe_idx(qp);
            let mut send_db_idx = rdmaxcel_sys::rdmaxcel_qp_load_send_db_idx(qp);
            if (wqe_cnt as u64) < (send_wqe_idx - send_db_idx) {
                return Err(anyhow::anyhow!("Overflow of WQE, possible data loss"));
            }
            self.apply_first_op_delay(send_db_idx);
            while send_db_idx < send_wqe_idx {
                let offset = (send_db_idx % wqe_cnt as u64) * stride as u64;
                let src_ptr = (base_ptr as *mut u8).wrapping_add(offset as usize);
                rdmaxcel_sys::db_ring((*dv_qp).bf.reg, src_ptr as *mut std::ffi::c_void);
                send_db_idx += 1;
                rdmaxcel_sys::rdmaxcel_qp_store_send_db_idx(qp, send_db_idx);
            }
            Ok(())
        }
    }

    /// Enqueues a put operation without ringing the doorbell.
    ///
    /// This method prepares a put operation but does not execute it.
    /// Use `get_doorbell().ring()` to execute the operation.
    ///
    /// # Arguments
    ///
    /// * `lhandle` - Local buffer handle
    /// * `rhandle` - Remote buffer handle
    ///
    /// # Returns
    ///
    /// * `Result<Vec<u64>, anyhow::Error>` - The work request IDs or error
    pub fn enqueue_put(
        &mut self,
        lhandle: RdmaBuffer,
        rhandle: RdmaBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        let idx = unsafe {
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_wqe_idx(
                self.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
            )
        };

        self.send_wqe(
            lhandle.addr,
            lhandle.lkey,
            lhandle.size,
            idx,
            true,
            RdmaOperation::Write,
            rhandle.addr,
            rhandle.rkey,
        )?;
        Ok(vec![idx])
    }

    /// Enqueues a put with receive operation without ringing the doorbell.
    ///
    /// This method prepares a put with receive operation but does not execute it.
    /// Use `get_doorbell().ring()` to execute the operation.
    ///
    /// # Arguments
    ///
    /// * `lhandle` - Local buffer handle
    /// * `rhandle` - Remote buffer handle
    ///
    /// # Returns
    ///
    /// * `Result<Vec<u64>, anyhow::Error>` - The work request IDs or error
    pub fn enqueue_put_with_recv(
        &mut self,
        lhandle: RdmaBuffer,
        rhandle: RdmaBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        let idx = unsafe {
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_wqe_idx(
                self.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
            )
        };

        self.send_wqe(
            lhandle.addr,
            lhandle.lkey,
            lhandle.size,
            idx,
            true,
            RdmaOperation::WriteWithImm,
            rhandle.addr,
            rhandle.rkey,
        )?;
        Ok(vec![idx])
    }

    /// Enqueues a get operation without ringing the doorbell.
    ///
    /// This method prepares a get operation but does not execute it.
    /// Use `get_doorbell().ring()` to execute the operation.
    ///
    /// # Arguments
    ///
    /// * `lhandle` - Local buffer handle
    /// * `rhandle` - Remote buffer handle
    ///
    /// # Returns
    ///
    /// * `Result<Vec<u64>, anyhow::Error>` - The work request IDs or error
    pub fn enqueue_get(
        &mut self,
        lhandle: RdmaBuffer,
        rhandle: RdmaBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        let idx = unsafe {
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_wqe_idx(
                self.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
            )
        };

        self.send_wqe(
            lhandle.addr,
            lhandle.lkey,
            lhandle.size,
            idx,
            true,
            RdmaOperation::Read,
            rhandle.addr,
            rhandle.rkey,
        )?;
        Ok(vec![idx])
    }

    pub fn get(
        &mut self,
        lhandle: RdmaBuffer,
        rhandle: RdmaBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        let total_size = lhandle.size;
        if rhandle.size < total_size {
            return Err(anyhow::anyhow!(
                "Remote buffer size ({}) is smaller than local buffer size ({})",
                rhandle.size,
                total_size
            ));
        }

        let mut remaining = total_size;
        let mut offset = 0;
        let mut wr_ids = Vec::new();

        while remaining > 0 {
            let chunk_size = std::cmp::min(remaining, MAX_RDMA_MSG_SIZE);
            let idx = unsafe {
                rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_wqe_idx(
                    self.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
                )
            };
            wr_ids.push(idx);

            self.post_op(
                lhandle.addr + offset,
                lhandle.lkey,
                chunk_size,
                idx,
                true,
                RdmaOperation::Read,
                rhandle.addr + offset,
                rhandle.rkey,
            )?;
            unsafe {
                rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_db_idx(
                    self.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
                );
            }

            remaining -= chunk_size;
            offset += chunk_size;
        }

        Ok(wr_ids)
    }

    /// Posts a request to the queue pair.
    ///
    /// # Arguments
    ///
    /// * `local_addr` - The local address containing data to send
    /// * `length` - Length of the data to send
    /// * `wr_id` - Work request ID for completion identification
    /// * `signaled` - Whether to generate a completion event
    /// * `op_type` - Optional operation type
    /// * `raddr` - the remote address, representing the memory location on the remote peer
    /// * `rkey` - the remote key, representing the key required to access the remote memory region
    fn post_op(
        &mut self,
        laddr: usize,
        lkey: u32,
        length: usize,
        wr_id: u64,
        signaled: bool,
        op_type: RdmaOperation,
        raddr: usize,
        rkey: u32,
    ) -> Result<(), anyhow::Error> {
        // SAFETY:
        // This code uses unsafe rdmaxcel_sys calls to post work requests to the RDMA device, but is safe because:
        // - All pointers (send_sge, send_wr) are properly initialized on the stack before use
        // - The memory address in `local_addr` is not dereferenced, only passed to the device
        // - The remote connection info is verified to exist before accessing
        // - The ibverbs post_send operation follows the documented API contract
        // - Error codes from the device are properly checked and propagated
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let context = self.context as *mut rdmaxcel_sys::ibv_context;
            let ops = &mut (*context).ops;
            let errno;
            if op_type == RdmaOperation::Recv {
                let mut sge = rdmaxcel_sys::ibv_sge {
                    addr: laddr as u64,
                    length: length as u32,
                    lkey,
                };
                let mut wr = rdmaxcel_sys::ibv_recv_wr {
                    wr_id,
                    sg_list: &mut sge as *mut _,
                    num_sge: 1,
                    ..Default::default()
                };
                let mut bad_wr: *mut rdmaxcel_sys::ibv_recv_wr = std::ptr::null_mut();
                errno =
                    ops.post_recv.as_mut().unwrap()((*qp).ibv_qp, &mut wr as *mut _, &mut bad_wr);
            } else if op_type == RdmaOperation::Write
                || op_type == RdmaOperation::Read
                || op_type == RdmaOperation::WriteWithImm
            {
                // Apply hardware initialization delay if this is the first operation
                self.apply_first_op_delay(wr_id);
                let send_flags = if signaled {
                    rdmaxcel_sys::ibv_send_flags::IBV_SEND_SIGNALED.0
                } else {
                    0
                };
                let mut sge = rdmaxcel_sys::ibv_sge {
                    addr: laddr as u64,
                    length: length as u32,
                    lkey,
                };
                let mut wr = rdmaxcel_sys::ibv_send_wr {
                    wr_id,
                    next: std::ptr::null_mut(),
                    sg_list: &mut sge as *mut _,
                    num_sge: 1,
                    opcode: op_type.into(),
                    send_flags,
                    wr: Default::default(),
                    qp_type: Default::default(),
                    __bindgen_anon_1: Default::default(),
                    __bindgen_anon_2: Default::default(),
                };

                wr.wr.rdma.remote_addr = raddr as u64;
                wr.wr.rdma.rkey = rkey;
                let mut bad_wr: *mut rdmaxcel_sys::ibv_send_wr = std::ptr::null_mut();

                errno =
                    ops.post_send.as_mut().unwrap()((*qp).ibv_qp, &mut wr as *mut _, &mut bad_wr);
            } else {
                panic!("Not Implemented");
            }

            if errno != 0 {
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!("Failed to post send request: {}", os_error));
            }
            tracing::debug!(
                "completed sending {:?} request (lkey: {}, addr: 0x{:x}, length {}) to (raddr 0x{:x}, rkey {})",
                op_type,
                lkey,
                laddr,
                length,
                raddr,
                rkey,
            );

            Ok(())
        }
    }

    fn send_wqe(
        &mut self,
        laddr: usize,
        lkey: u32,
        length: usize,
        wr_id: u64,
        signaled: bool,
        op_type: RdmaOperation,
        raddr: usize,
        rkey: u32,
    ) -> Result<DoorBell, anyhow::Error> {
        unsafe {
            let op_type_val = match op_type {
                RdmaOperation::Write => rdmaxcel_sys::MLX5_OPCODE_RDMA_WRITE,
                RdmaOperation::WriteWithImm => rdmaxcel_sys::MLX5_OPCODE_RDMA_WRITE_IMM,
                RdmaOperation::Read => rdmaxcel_sys::MLX5_OPCODE_RDMA_READ,
                RdmaOperation::Recv => 0,
            };

            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let dv_qp = self.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
            let _dv_cq = if op_type == RdmaOperation::Recv {
                self.dv_recv_cq as *mut rdmaxcel_sys::mlx5dv_cq
            } else {
                self.dv_send_cq as *mut rdmaxcel_sys::mlx5dv_cq
            };

            // Create the WQE parameters struct

            let buf = if op_type == RdmaOperation::Recv {
                (*dv_qp).rq.buf as *mut u8
            } else {
                (*dv_qp).sq.buf as *mut u8
            };

            let params = rdmaxcel_sys::wqe_params_t {
                laddr,
                lkey,
                length,
                wr_id,
                signaled,
                op_type: op_type_val,
                raddr,
                rkey,
                qp_num: (*(*qp).ibv_qp).qp_num,
                buf,
                dbrec: (*dv_qp).dbrec,
                wqe_cnt: (*dv_qp).sq.wqe_cnt,
            };

            // Call the C function to post the WQE
            if op_type == RdmaOperation::Recv {
                rdmaxcel_sys::recv_wqe(params);
                std::ptr::write_volatile((*dv_qp).dbrec, 1_u32.to_be());
            } else {
                rdmaxcel_sys::send_wqe(params);
            };

            // Create and return a DoorBell struct
            Ok(DoorBell {
                dst_ptr: (*dv_qp).bf.reg as usize,
                src_ptr: (*dv_qp).sq.buf as usize,
                size: 8,
            })
        }
    }

    /// Poll for work completions by wr_ids.
    ///
    /// # Arguments
    ///
    /// * `target` - Which completion queue to poll (Send, Receive)
    /// * `expected_wr_ids` - Slice of work request IDs to wait for
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<(u64, IbvWc)>)` - Vector of (wr_id, completion) pairs that were found
    /// * `Err(e)` - An error occurred
    pub fn poll_completion(
        &mut self,
        target: PollTarget,
        expected_wr_ids: &[u64],
    ) -> Result<Vec<(u64, IbvWc)>, anyhow::Error> {
        if expected_wr_ids.is_empty() {
            return Ok(Vec::new());
        }

        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let qp_num = (*(*qp).ibv_qp).qp_num;

            let (cq, cache, cq_type) = match target {
                PollTarget::Send => (
                    self.send_cq as *mut rdmaxcel_sys::ibv_cq,
                    rdmaxcel_sys::rdmaxcel_qp_get_send_cache(qp),
                    "send",
                ),
                PollTarget::Recv => (
                    self.recv_cq as *mut rdmaxcel_sys::ibv_cq,
                    rdmaxcel_sys::rdmaxcel_qp_get_recv_cache(qp),
                    "recv",
                ),
            };

            let mut results = Vec::new();

            // Single-shot poll: check each wr_id once and return what we find
            for &expected_wr_id in expected_wr_ids {
                let mut poll_ctx = rdmaxcel_sys::poll_context_t {
                    expected_wr_id,
                    expected_qp_num: qp_num,
                    cache,
                    cq,
                };

                let mut wc = std::mem::MaybeUninit::<rdmaxcel_sys::ibv_wc>::zeroed().assume_init();
                let ret = rdmaxcel_sys::poll_cq_with_cache(&mut poll_ctx, &mut wc);

                match ret {
                    1 => {
                        // Found completion
                        if !wc.is_valid() {
                            if let Some((status, vendor_err)) = wc.error() {
                                return Err(anyhow::anyhow!(
                                    "{} completion failed for wr_id={}: status={:?}, vendor_err={}",
                                    cq_type,
                                    expected_wr_id,
                                    status,
                                    vendor_err,
                                ));
                            }
                        }
                        results.push((expected_wr_id, IbvWc::from(wc)));
                    }
                    0 => {
                        // Not found yet - this is fine for single-shot poll
                    }
                    -17 => {
                        // RDMAXCEL_COMPLETION_FAILED: Completion found but failed - wc contains the error details
                        let error_msg =
                            std::ffi::CStr::from_ptr(rdmaxcel_sys::rdmaxcel_error_string(ret))
                                .to_str()
                                .unwrap_or("Unknown error");
                        if let Some((status, vendor_err)) = wc.error() {
                            return Err(anyhow::anyhow!(
                                "Failed to poll {} CQ for wr_id={}: {} [status={:?}, vendor_err={}, qp_num={}, byte_len={}]",
                                cq_type,
                                expected_wr_id,
                                error_msg,
                                status,
                                vendor_err,
                                wc.qp_num,
                                wc.len(),
                            ));
                        } else {
                            return Err(anyhow::anyhow!(
                                "Failed to poll {} CQ for wr_id={}: {} [qp_num={}, byte_len={}]",
                                cq_type,
                                expected_wr_id,
                                error_msg,
                                wc.qp_num,
                                wc.len(),
                            ));
                        }
                    }
                    _ => {
                        // Other errors
                        let error_msg =
                            std::ffi::CStr::from_ptr(rdmaxcel_sys::rdmaxcel_error_string(ret))
                                .to_str()
                                .unwrap_or("Unknown error");
                        return Err(anyhow::anyhow!(
                            "Failed to poll {} CQ for wr_id={}: {}",
                            cq_type,
                            expected_wr_id,
                            error_msg
                        ));
                    }
                }
            }

            Ok(results)
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_connection() {
        // Skip test if RDMA devices are not available
        if crate::ibverbs_primitives::get_all_devices().is_empty() {
            println!("Skipping test: RDMA devices not available");
            return;
        }

        let config = IbverbsConfig {
            use_gpu_direct: false,
            ..Default::default()
        };
        let domain = RdmaDomain::new(config.device.clone());
        assert!(domain.is_ok());

        let domain = domain.unwrap();
        let queue_pair = RdmaQueuePair::new(domain.context, domain.pd, config.clone());
        assert!(queue_pair.is_ok());
    }

    #[test]
    fn test_loopback_connection() {
        // Skip test if RDMA devices are not available
        if crate::ibverbs_primitives::get_all_devices().is_empty() {
            println!("Skipping test: RDMA devices not available");
            return;
        }

        let server_config = IbverbsConfig {
            use_gpu_direct: false,
            ..Default::default()
        };
        let client_config = IbverbsConfig {
            use_gpu_direct: false,
            ..Default::default()
        };

        let server_domain = RdmaDomain::new(server_config.device.clone()).unwrap();
        let client_domain = RdmaDomain::new(client_config.device.clone()).unwrap();

        let mut server_qp = RdmaQueuePair::new(
            server_domain.context,
            server_domain.pd,
            server_config.clone(),
        )
        .unwrap();
        let mut client_qp = RdmaQueuePair::new(
            client_domain.context,
            client_domain.pd,
            client_config.clone(),
        )
        .unwrap();

        let server_connection_info = server_qp.get_qp_info().unwrap();
        let client_connection_info = client_qp.get_qp_info().unwrap();

        assert!(server_qp.connect(&client_connection_info).is_ok());
        assert!(client_qp.connect(&server_connection_info).is_ok());
    }
}
