/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs queue pair, doorbell, and completion polling.
//!
//! An [`IbvQueuePair`] encapsulates the send and receive queues, completion
//! queues, and other resources needed for RDMA communication. It provides
//! methods for establishing connections and performing RDMA operations.

/// Maximum size for a single RDMA operation in bytes (1 GiB).
const MAX_RDMA_MSG_SIZE: usize = 1024 * 1024 * 1024;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::Error;
use std::result::Result;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use backoff::ExponentialBackoff;
use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortHandle;
use hyperactor::actor::Referable;
use hyperactor::actor::RemoteHandles;
use hyperactor::context::Mailbox;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use super::IbvBuffer;
use super::IbvOp;
use super::domain::IbvDomain;
use super::manager_actor::CreatePeerQueuePair;
use super::primitives::Gid;
use super::primitives::IbvConfig;
use super::primitives::IbvMemoryRegionView;
use super::primitives::IbvOperation;
use super::primitives::IbvQpInfo;
use super::primitives::IbvWc;
use super::primitives::resolve_qp_type;
use crate::RdmaOpType;

/// A structured error from [`IbvQueuePair::poll_completion`].
///
/// Carries the `ibv_wc_status` and vendor error code (when available) so
/// callers can match on specific completion statuses without string parsing.
#[derive(Debug)]
pub struct PollCompletionError {
    pub status: Option<rdmaxcel_sys::ibv_wc_status::Type>,
    pub vendor_err: Option<u32>,
    message: String,
}

impl std::fmt::Display for PollCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PollCompletionError {}

impl PollCompletionError {
    /// Returns `true` when the completion status is `IBV_WC_WR_FLUSH_ERR`,
    /// which typically indicates a secondary failure after the QP entered
    /// error state due to a different work request's failure.
    pub fn is_wr_flush_err(&self) -> bool {
        self.status == Some(rdmaxcel_sys::ibv_wc_status::IBV_WC_WR_FLUSH_ERR)
    }

    #[cfg(test)]
    pub(super) fn for_test(message: &str) -> Self {
        Self {
            message: message.to_string(),
            status: None,
            vendor_err: None,
        }
    }
}

/// A doorbell trigger for batched RDMA operations.
///
/// Rings the hardware doorbell to execute previously enqueued work requests.
#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct DoorBell {
    pub src_ptr: usize,
    pub dst_ptr: usize,
    pub size: usize,
}
wirevalue::register_type!(DoorBell);

/// Specifies which completion queue to poll.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PollTarget {
    Send,
    Recv,
}

/// An RDMA Queue Pair (QP) for communication between two endpoints.
///
/// Encapsulates the send/receive queues, completion queues, and mlx5dv
/// device-specific structures needed for RDMA communication.
///
/// # Connection Lifecycle
///
/// 1. Create with `new()` from context and protection domain pointers
/// 2. Get connection info with `get_qp_info()`
/// 3. Exchange connection info with remote peer
/// 4. Connect to remote endpoint with `connect()`
/// 5. Perform RDMA operations with `put()` or `get()`
/// 6. Poll for completions with `poll_completion()`
///
/// # Notes
/// - The `qp` field stores a pointer to `rdmaxcel_qp_t` (not `ibv_qp`).
///   It is private; external callers reach it via [`Self::as_ptr`].
/// - `rdmaxcel_qp_t` contains atomic counters and completion caches internally
/// - `IbvQueuePair` is single-owner: its `Drop` destroys the FFI QP, so
///   the type is intentionally `!Clone` (and not sent across the wire).
#[derive(Debug)]
pub struct IbvQueuePair {
    pub send_cq: usize,    // *mut rdmaxcel_sys::ibv_cq,
    pub recv_cq: usize,    // *mut rdmaxcel_sys::ibv_cq,
    qp: usize,             // *mut rdmaxcel_sys::rdmaxcel_qp_t
    pub dv_qp: usize,      // *mut rdmaxcel_sys::mlx5dv_qp,
    pub dv_send_cq: usize, // *mut rdmaxcel_sys::mlx5dv_cq,
    pub dv_recv_cq: usize, // *mut rdmaxcel_sys::mlx5dv_cq,
    context: usize,        // *mut rdmaxcel_sys::ibv_context,
    config: IbvConfig,
    is_efa: bool,
    // The QP's `context` and `pd` pointers are owned by this domain;
    // hold an `Arc` so the domain outlives the QP regardless of what
    // happens to other owners (the manager, registered MRs, etc.).
    _domain: Arc<IbvDomain>,
}

impl Drop for IbvQueuePair {
    fn drop(&mut self) {
        if self.qp == 0 {
            return;
        }
        // SAFETY: `IbvQueuePair` is `!Clone`, so `Drop` runs at most
        // once. Any external use of the `qp` pointer goes through an
        // `unsafe { qp.as_ptr() }` call on a `&IbvQueuePair` borrow,
        // which statically prevents `Drop` from running concurrently.
        // Any caller that uses `as_ptr` promises they will not use the
        // returned pointer after the borrow goes out of scope.
        unsafe {
            rdmaxcel_sys::rdmaxcel_qp_destroy(self.qp as *mut rdmaxcel_sys::rdmaxcel_qp);
        }
    }
}

impl IbvQueuePair {
    /// Returns the underlying `rdmaxcel_qp_t` pointer for callers
    /// driving the device doorbell or building FFI parameter blocks
    /// directly.
    ///
    /// # Safety
    ///
    /// The returned pointer is only valid for as long as the borrow
    /// of `self` lives. The caller must ensure all reads/writes
    /// through the pointer complete before this `IbvQueuePair` is
    /// dropped, since `Drop` calls `rdmaxcel_qp_destroy` on it.
    pub unsafe fn as_ptr(&self) -> *mut rdmaxcel_sys::rdmaxcel_qp {
        self.qp as *mut rdmaxcel_sys::rdmaxcel_qp
    }

    fn is_efa(&self) -> bool {
        self.is_efa
    }

    /// Applies hardware initialization delay if this is the first operation since RTS.
    fn apply_first_op_delay(&self, wr_id: u64) {
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            if wr_id == 0 {
                let rts_timestamp = rdmaxcel_sys::rdmaxcel_qp_load_rts_timestamp(qp);
                assert!(
                    rts_timestamp != u64::MAX,
                    "First operation attempted before queue pair reached RTS state! Call connect() first."
                );
                let current_nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let elapsed_nanos = current_nanos - rts_timestamp;
                let elapsed = Duration::from_nanos(elapsed_nanos);
                let init_delay = Duration::from_millis(self.config.hw_init_delay_ms);
                if elapsed < init_delay {
                    let remaining_delay = init_delay - elapsed;
                    // Sync context within unsafe block; tokio::time::sleep is async
                    // and converting would require propagating async through the
                    // entire post_op / ring_doorbell call chain.
                    std::thread::sleep(remaining_delay);
                }
            }
        }
    }

    /// Creates a new IbvQueuePair.
    ///
    /// Initializes a new Queue Pair (QP) and associated Completion Queues (CQ)
    /// using the provided context and protection domain. The QP is created in
    /// the RESET state and must be transitioned via `connect()` before use.
    ///
    /// # Errors
    ///
    /// Returns errors if CQ or QP creation fails.
    pub fn new(domain: Arc<IbvDomain>, config: IbvConfig) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating an IbvQueuePair from config {}", config);
        let context = domain.context;
        let pd = domain.pd;
        unsafe {
            // Resolve Auto to a concrete QP type based on device capabilities
            let resolved_qp_type = resolve_qp_type(config.qp_type);
            let is_efa = resolved_qp_type == rdmaxcel_sys::RDMA_QP_TYPE_EFA;
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

            // EFA uses standard ibverbs (not mlx5dv), so skip dv setup
            if is_efa {
                return Ok(IbvQueuePair {
                    send_cq: send_cq as usize,
                    recv_cq: recv_cq as usize,
                    qp: qp as usize,
                    dv_qp: 0,
                    dv_send_cq: 0,
                    dv_recv_cq: 0,
                    context: context as usize,
                    config,
                    is_efa: true,
                    _domain: domain,
                });
            }

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
            Ok(IbvQueuePair {
                send_cq: send_cq as usize,
                recv_cq: recv_cq as usize,
                qp: qp as usize,
                dv_qp: dv_qp as usize,
                dv_send_cq: dv_send_cq as usize,
                dv_recv_cq: dv_recv_cq as usize,
                context: context as usize,
                config,
                is_efa: false,
                _domain: domain,
            })
        }
    }

    /// Returns the connection info needed by a remote peer to connect to this QP.
    pub fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error> {
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

            Ok(IbvQpInfo {
                qp_num: (*(*qp).ibv_qp).qp_num,
                lid: port_attr.lid,
                gid: Some(gid),
                psn: self.config.psn,
            })
        }
    }

    /// Returns the current state of the QP.
    pub fn state(&mut self) -> Result<u32, anyhow::Error> {
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

    /// Transitions the QP through INIT -> RTR -> RTS to establish a connection.
    ///
    /// # Arguments
    ///
    /// * `connection_info` - The remote connection info to connect to
    pub fn connect(&mut self, connection_info: &IbvQpInfo) -> Result<(), anyhow::Error> {
        // EFA: use unified C function for QP state transitions
        if self.is_efa() {
            return self.efa_connect(connection_info);
        }

        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;

            let qp_access_flags = rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;

            // Transition to INIT
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

            if let Some(gid) = connection_info.gid {
                qp_attr.ah_attr.is_global = 1;
                qp_attr.ah_attr.grh.dgid = rdmaxcel_sys::ibv_gid::from(gid);
                qp_attr.ah_attr.grh.hop_limit = 0xff;
                qp_attr.ah_attr.grh.sgid_index = self.config.gid_index;
            } else {
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

            let rts_timestamp_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            rdmaxcel_sys::rdmaxcel_qp_store_rts_timestamp(qp, rts_timestamp_nanos);

            Ok(())
        }
    }

    /// Connects via the EFA-specific C function for QP state transitions.
    fn efa_connect(&mut self, connection_info: &IbvQpInfo) -> Result<(), anyhow::Error> {
        let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;

        let gid_ptr = connection_info.gid.as_ref().map_or(std::ptr::null(), |g| {
            let ibv_gid: &rdmaxcel_sys::ibv_gid = g.as_ref();
            unsafe { ibv_gid.raw.as_ptr() }
        });

        unsafe {
            let ret = rdmaxcel_sys::rdmaxcel_efa_connect(
                qp,
                self.config.port_num,
                self.config.pkey_index,
                0x4242, // qkey
                self.config.psn,
                self.config.gid_index,
                gid_ptr,
                connection_info.qp_num,
            );
            if ret != 0 {
                let msg = std::ffi::CStr::from_ptr(rdmaxcel_sys::rdmaxcel_error_string(ret))
                    .to_str()
                    .unwrap_or("unknown");
                return Err(anyhow::anyhow!("EFA connect failed: {}", msg));
            }
        }

        // Store RTS timestamp for first-op delay
        let rts_timestamp_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        unsafe {
            rdmaxcel_sys::rdmaxcel_qp_store_rts_timestamp(qp, rts_timestamp_nanos);
        }

        Ok(())
    }

    pub fn recv(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> Result<u64, anyhow::Error> {
        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let idx = rdmaxcel_sys::rdmaxcel_qp_fetch_add_recv_wqe_idx(qp);
            self.post_op(
                0,
                lhandle.lkey,
                0,
                idx,
                true,
                IbvOperation::Recv,
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
        lhandle: IbvBuffer,
        rhandle: IbvBuffer,
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
                IbvOperation::WriteWithImm,
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
        lhandle: IbvBuffer,
        rhandle: IbvBuffer,
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
                IbvOperation::Write,
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

    /// Rings the doorbell to execute all enqueued operations.
    pub fn ring_doorbell(&mut self) -> Result<(), anyhow::Error> {
        // EFA uses standard ibverbs (not mlx5dv), so skip doorbell ringing
        if self.is_efa() {
            return Ok(());
        }

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
                let src_ptr = base_ptr.wrapping_add(offset as usize);
                rdmaxcel_sys::db_ring((*dv_qp).bf.reg, src_ptr as *mut std::ffi::c_void);
                send_db_idx += 1;
                rdmaxcel_sys::rdmaxcel_qp_store_send_db_idx(qp, send_db_idx);
            }
            Ok(())
        }
    }

    /// Enqueues a put operation without ringing the doorbell.
    pub fn enqueue_put(
        &mut self,
        lhandle: IbvBuffer,
        rhandle: IbvBuffer,
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
            IbvOperation::Write,
            rhandle.addr,
            rhandle.rkey,
        )?;
        Ok(vec![idx])
    }

    /// Enqueues a put-with-receive operation without ringing the doorbell.
    pub fn enqueue_put_with_recv(
        &mut self,
        lhandle: IbvBuffer,
        rhandle: IbvBuffer,
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
            IbvOperation::WriteWithImm,
            rhandle.addr,
            rhandle.rkey,
        )?;
        Ok(vec![idx])
    }

    /// Enqueues a get operation without ringing the doorbell.
    pub fn enqueue_get(
        &mut self,
        lhandle: IbvBuffer,
        rhandle: IbvBuffer,
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
            IbvOperation::Read,
            rhandle.addr,
            rhandle.rkey,
        )?;
        Ok(vec![idx])
    }

    pub fn get(
        &mut self,
        lhandle: IbvBuffer,
        rhandle: IbvBuffer,
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
                IbvOperation::Read,
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
    fn post_op(
        &mut self,
        laddr: usize,
        lkey: u32,
        length: usize,
        wr_id: u64,
        signaled: bool,
        op_type: IbvOperation,
        raddr: usize,
        rkey: u32,
    ) -> Result<(), anyhow::Error> {
        // EFA: use unified C function
        if self.is_efa() {
            return self.post_op_efa(laddr, lkey, length, wr_id, signaled, op_type, raddr, rkey);
        }

        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let context = self.context as *mut rdmaxcel_sys::ibv_context;
            let ops = &mut (*context).ops;
            let errno;
            if op_type == IbvOperation::Recv {
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
            } else if op_type == IbvOperation::Write
                || op_type == IbvOperation::Read
                || op_type == IbvOperation::WriteWithImm
            {
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

    /// Posts an RDMA operation via the EFA-specific C function.
    fn post_op_efa(
        &mut self,
        laddr: usize,
        lkey: u32,
        length: usize,
        wr_id: u64,
        signaled: bool,
        op_type: IbvOperation,
        raddr: usize,
        rkey: u32,
    ) -> Result<(), anyhow::Error> {
        let c_op = match op_type {
            IbvOperation::Write => 0,
            IbvOperation::Read => 1,
            IbvOperation::Recv => 2,
            IbvOperation::WriteWithImm => 3,
        };

        unsafe {
            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let ret = rdmaxcel_sys::rdmaxcel_qp_post_op(
                qp,
                laddr as *mut std::ffi::c_void,
                lkey,
                length,
                raddr as *mut std::ffi::c_void,
                rkey,
                wr_id,
                signaled as i32,
                c_op,
            );
            if ret != 0 {
                let msg = std::ffi::CStr::from_ptr(rdmaxcel_sys::rdmaxcel_error_string(ret))
                    .to_str()
                    .unwrap_or("unknown");
                return Err(anyhow::anyhow!("EFA post_op failed: {}", msg));
            }
        }
        Ok(())
    }

    fn send_wqe(
        &mut self,
        laddr: usize,
        lkey: u32,
        length: usize,
        wr_id: u64,
        signaled: bool,
        op_type: IbvOperation,
        raddr: usize,
        rkey: u32,
    ) -> Result<DoorBell, anyhow::Error> {
        // Non-mlx5 devices use the unified C post_op path
        if self.is_efa() {
            self.post_op(laddr, lkey, length, wr_id, signaled, op_type, raddr, rkey)?;
            return Ok(DoorBell {
                dst_ptr: 0,
                src_ptr: 0,
                size: 0,
            });
        }

        unsafe {
            let op_type_val = match op_type {
                IbvOperation::Write => rdmaxcel_sys::MLX5_OPCODE_RDMA_WRITE,
                IbvOperation::WriteWithImm => rdmaxcel_sys::MLX5_OPCODE_RDMA_WRITE_IMM,
                IbvOperation::Read => rdmaxcel_sys::MLX5_OPCODE_RDMA_READ,
                IbvOperation::Recv => 0,
            };

            let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let dv_qp = self.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
            let _dv_cq = if op_type == IbvOperation::Recv {
                self.dv_recv_cq as *mut rdmaxcel_sys::mlx5dv_cq
            } else {
                self.dv_send_cq as *mut rdmaxcel_sys::mlx5dv_cq
            };

            let buf = if op_type == IbvOperation::Recv {
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

            if op_type == IbvOperation::Recv {
                rdmaxcel_sys::recv_wqe(params);
                std::ptr::write_volatile((*dv_qp).dbrec, 1_u32.to_be());
            } else {
                rdmaxcel_sys::send_wqe(params);
            };

            Ok(DoorBell {
                dst_ptr: (*dv_qp).bf.reg as usize,
                src_ptr: (*dv_qp).sq.buf as usize,
                size: 8,
            })
        }
    }

    /// Polls for work completions for each of `expected_wr_ids`.
    ///
    /// # Returns
    ///
    /// * Outer `Ok` — the CQ poll itself succeeded. Each element is
    ///   `(wr_id, Ok(IbvWc))` for a completed WR or `(wr_id,
    ///   Err(PollCompletionError))` for a WR whose completion landed
    ///   with a non-success status (per-WR failure; the QP may or may
    ///   not be in error state). wr_ids that have no completion yet
    ///   are omitted.
    /// * Outer `Err` — the CQ itself is unusable (`ibv_poll_cq`
    ///   returned a negative value, i.e. `RDMAXCEL_CQ_POLL_FAILED`,
    ///   or some other non-classifiable rdmaxcel return code). The
    ///   QP should be treated as poisoned.
    pub fn poll_completion(
        &mut self,
        target: PollTarget,
        expected_wr_ids: &HashSet<u64>,
    ) -> Result<Vec<(u64, Result<IbvWc, PollCompletionError>)>, PollCompletionError> {
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
                        if !wc.is_valid()
                            && let Some((status, vendor_err)) = wc.error()
                        {
                            results.push((
                                expected_wr_id,
                                Err(PollCompletionError {
                                    status: Some(status),
                                    vendor_err: Some(vendor_err),
                                    message: format!(
                                        "{} completion failed for wr_id={}: status={:?}, vendor_err={}",
                                        cq_type, expected_wr_id, status, vendor_err,
                                    ),
                                }),
                            ));
                        } else {
                            results.push((expected_wr_id, Ok(IbvWc::from(wc))));
                        }
                    }
                    0 => {
                        // Not found yet
                    }
                    // RDMAXCEL_COMPLETION_FAILED: per-WR completion landed
                    // with a non-success status. Surface as an inner Err
                    // so the caller can report the failure for this op
                    // without poisoning the QP.
                    -17 => {
                        let error_msg =
                            std::ffi::CStr::from_ptr(rdmaxcel_sys::rdmaxcel_error_string(ret))
                                .to_str()
                                .unwrap_or("Unknown error");
                        let (status, vendor_err) =
                            wc.error().map_or((None, None), |(s, v)| (Some(s), Some(v)));
                        let message = match (status, vendor_err) {
                            (Some(s), Some(v)) => format!(
                                "{} wr_id={} completed with non-success status: {} [status={:?}, vendor_err={}, qp_num={}, byte_len={}]",
                                cq_type,
                                expected_wr_id,
                                error_msg,
                                s,
                                v,
                                wc.qp_num,
                                wc.len(),
                            ),
                            _ => format!(
                                "{} wr_id={} completed with non-success status: {} [qp_num={}, byte_len={}]",
                                cq_type,
                                expected_wr_id,
                                error_msg,
                                wc.qp_num,
                                wc.len(),
                            ),
                        };
                        results.push((
                            expected_wr_id,
                            Err(PollCompletionError {
                                status,
                                vendor_err,
                                message,
                            }),
                        ));
                    }
                    // RDMAXCEL_CQ_POLL_FAILED (-16) and any other
                    // non-classifiable rdmaxcel return code: fatal at
                    // the CQ level; the QP is no longer pollable.
                    _ => {
                        let error_msg =
                            std::ffi::CStr::from_ptr(rdmaxcel_sys::rdmaxcel_error_string(ret))
                                .to_str()
                                .unwrap_or("Unknown error");
                        return Err(PollCompletionError {
                            status: None,
                            vendor_err: None,
                            message: format!(
                                "{} CQ poll failed (ret={}) while looking up wr_id={}: {}",
                                cq_type, ret, expected_wr_id, error_msg,
                            ),
                        });
                    }
                }
            }

            Ok(results)
        }
    }
}

/// Identifies a per-peer queue pair held by one
/// [`super::manager_actor::IbvManagerActor`]. The same conceptual
/// QP is referenced by two distinct keys, one from each side: each
/// manager stores the local view (its own device, the peer's actor
/// id, the peer's device).
#[derive(Clone, Hash, Eq, PartialEq, Debug, Serialize, Deserialize, Named)]
pub(super) struct QpKey {
    pub(super) self_device: String,
    pub(super) other_id: ActorId,
    pub(super) other_device: String,
}

// =====================================================================
// QueuePairActor
// =====================================================================
//
// Owns one simplex queue pair to a peer. The active side spawns a
// `QueuePairActor` that owns and operates the local QP; the peer's
// manager eagerly creates its mirror QP during the handshake, stores
// it as a passive endpoint, and never wraps it in an actor. If the
// peer later wants to RDMA us, it spawns its own `QueuePairActor`
// locally, which creates a fresh pair the same way.

/// Operations a [`QueuePairActor`] performs on its QP. Implemented
/// on the real [`IbvQueuePair`] and on mocks in `#[cfg(test)]` code.
pub(super) trait QueuePair: std::fmt::Debug + Send + Sync + 'static {
    /// Returns the local endpoint other peers need in order to
    /// connect to this QP.
    fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error>;

    /// Transitions the QP through `INIT -> RTR -> RTS`, connected to
    /// `info`.
    fn connect(&mut self, info: &IbvQpInfo) -> Result<(), anyhow::Error>;

    /// Post an RDMA WRITE from `lhandle` to `rhandle`. Returns one
    /// wr_id per `MAX_RDMA_MSG_SIZE` chunk the QP issues.
    fn put(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> Result<Vec<u64>, anyhow::Error>;

    /// Post an RDMA READ from `rhandle` into `lhandle`. Returns one
    /// wr_id per chunk.
    fn get(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> Result<Vec<u64>, anyhow::Error>;

    /// Poll for completions of every wr_id in `expected_wr_ids`. See
    /// [`IbvQueuePair::poll_completion`] for the outer/inner `Result`
    /// semantics.
    fn poll_completion(
        &mut self,
        target: PollTarget,
        expected_wr_ids: &HashSet<u64>,
    ) -> Result<Vec<(u64, Result<IbvWc, PollCompletionError>)>, PollCompletionError>;
}

impl QueuePair for IbvQueuePair {
    fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error> {
        IbvQueuePair::get_qp_info(self)
    }
    fn connect(&mut self, info: &IbvQpInfo) -> Result<(), anyhow::Error> {
        IbvQueuePair::connect(self, info)
    }
    fn put(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> Result<Vec<u64>, anyhow::Error> {
        IbvQueuePair::put(self, lhandle, rhandle)
    }
    fn get(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> Result<Vec<u64>, anyhow::Error> {
        IbvQueuePair::get(self, lhandle, rhandle)
    }
    fn poll_completion(
        &mut self,
        target: PollTarget,
        expected_wr_ids: &HashSet<u64>,
    ) -> Result<Vec<(u64, Result<IbvWc, PollCompletionError>)>, PollCompletionError> {
        IbvQueuePair::poll_completion(self, target, expected_wr_ids)
    }
}

/// Adaptive backoff for the scheduler's `Tick` self-message. Use
/// [`Self::next_interval`] to ask "how long should I wait before the
/// next poll attempt?"; call [`Self::reset`] whenever the previous
/// poll observed completions (so the actor stays tight while work
/// is making progress).
///
/// While the elapsed time since the first non-zero interval is below
/// `yield_window`, the policy returns `Duration::ZERO` so the actor
/// just re-sends `Tick` to itself with no delay — keeping latency
/// tight when WRs are about to complete. Past the window, it walks
/// an exponential backoff (1ms initial, doubling, capped at 10ms)
/// so a long-running op doesn't keep the runtime spinning. When
/// `yield_window` is `None` (the default for
/// `RDMA_CQ_BUSY_POLL_WINDOW`) the policy always returns
/// `Duration::ZERO`.
#[derive(Debug)]
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

    /// Forget all accumulated backoff state. Called after a poll
    /// returns completions, so the next idle stretch starts fresh.
    fn reset(&mut self) {
        self.started_at = None;
        self.backoff = None;
    }

    /// Suggested delay before the next `Tick`. `Duration::ZERO`
    /// means "send `Tick` immediately".
    fn next_interval(&mut self) -> Duration {
        let Some(window) = self.yield_window else {
            return Duration::ZERO;
        };
        let started = *self.started_at.get_or_insert_with(Instant::now);
        if started.elapsed() < window {
            return Duration::ZERO;
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
        backoff.next_backoff().unwrap_or(Duration::ZERO)
    }
}

/// Bundle of trait bounds for an actor type that can serve as the
/// peer manager — i.e. the recipient of [`CreatePeerQueuePair`].
pub(super) trait Manager:
    Actor + Referable + RemoteHandles<CreatePeerQueuePair<Self>>
{
}

impl<T> Manager for T where T: Actor + Referable + RemoteHandles<CreatePeerQueuePair<T>> {}

/// Per-op completion reply emitted by [`QueuePairActor`] back to the
/// manager via [`ProcessOps::reply`]. A named newtype (rather than a
/// raw `(usize, Result<…>)` tuple) so the manager can identify
/// undeliverable `OpResult`s by type name in
/// `handle_undeliverable_message` and absorb them when the original
/// caller has gone away (typical at test teardown).
#[allow(dead_code)] // not yet referenced by IbvManagerActor
#[derive(Debug)]
pub(super) struct OpResult {
    pub(super) op_idx: usize,
    pub(super) result: Result<(), String>,
}

/// Local-only message: enqueue a batch of ops on this QP. As each
/// op resolves the actor sends one `(op_idx, result)` tuple on
/// `reply` — `op_idx` is the original index of the op in the
/// user-facing `IbvManagerActor::submit_ops` request, so the receiver
/// can correlate replies across batches that were sliced per-QP. The
/// inner `Result` is `Ok(())` if every WR for the op completed
/// successfully, otherwise `Err` carrying the first per-WR error
/// observed (held back until the op's other WRs also report, so the
/// MR registration outlives the data path).
#[derive(Debug)]
pub(super) struct ProcessOps<M: Referable> {
    pub(super) items: Vec<(usize, IbvOp<M>, IbvMemoryRegionView)>,
    pub(super) reply: PortHandle<OpResult>,
}

/// Local-only self-message that drives one round of the scheduler.
#[derive(Debug)]
struct Tick;

/// An op accepted by the actor but not yet posted to the QP.
#[derive(Debug)]
struct PendingOp<M: Referable> {
    op_idx: usize,
    op: IbvOp<M>,
    mrv: IbvMemoryRegionView,
    reply: PortHandle<OpResult>,
    /// WR count this op will issue when posted, computed once at
    /// construction so retries from a credit head-block don't redo
    /// the work.
    wrs: u32,
    is_read: bool,
}

/// State of an op whose WRs are in flight on the QP.
#[derive(Debug)]
struct PostedOpEntry {
    op_idx: usize,
    pending_wrs: HashSet<u64>,
    is_read: bool,
    /// Kept alive so the MR registration outlives every in-flight
    /// WR touching it. Field is intentionally unread.
    _mrv: IbvMemoryRegionView,
    reply: PortHandle<OpResult>,
    /// First per-WR error observed for this op. The op's final
    /// reply is held back until `pending_wrs.is_empty()` so we don't
    /// release the MR while remaining WRs are still in flight.
    first_error: Option<String>,
}

/// Per-peer queue-pair actor.
///
/// Generic over the manager actor type `M` (so tests can swap in a
/// mock) and the queue-pair type `Qp` (so unit tests run without
/// RDMA hardware). The QP is constructed by the spawning manager
/// and handed in as a spawn param; the actor owns it for life and
/// drops it when the actor stops.
#[derive(Debug)]
pub(super) struct QueuePairActor<M: Manager, Qp: QueuePair> {
    qp_key: QpKey,
    /// Filled into [`CreatePeerQueuePair::sender`] so the peer can
    /// build its own [`QpKey`] from our identity.
    local_manager: ActorRef<M>,
    /// Recipient of [`CreatePeerQueuePair`].
    peer_manager: ActorRef<M>,
    qp: Qp,
    /// `true` when the peer QP is colocated with this actor's QP —
    /// i.e. both endpoints live in the same `IbvManagerActor` *and*
    /// target the same RDMA device. In that case `init` connects
    /// the QP to its own endpoint and skips the cross-actor
    /// handshake.
    is_loopback: bool,
    init_timeout: Duration,
    /// QP-wide cap on outstanding send-queue WRs (reads + writes).
    max_send_wr: u32,
    /// QP-wide cap on outstanding RDMA-READ WRs at the initiator.
    max_rd_atomic: u32,
    /// Single FIFO of ops awaiting their first post attempt. If the
    /// head op bumps against either credit cap, ops queued behind
    /// it stall — a write stuck behind a credit-blocked read is the
    /// known consequence; smarter interleaving is future work.
    queue: VecDeque<PendingOp<M>>,
    /// op-id → entry, for tracking WR completion. op-ids are local
    /// to this actor (monotonic counter); they exist so we can
    /// route per-WR completions to the right `PostedOpEntry`
    /// without making any assumptions about uniqueness of `op_idx`
    /// across batches.
    posted: HashMap<u64, PostedOpEntry>,
    /// wr_id → local op-id.
    wr_to_op: HashMap<u64, u64>,
    /// Mirrors `wr_to_op.keys()` exactly, maintained incrementally
    /// so each tick can hand it straight to `poll_completion`
    /// without rebuilding.
    to_poll: HashSet<u64>,
    next_op_id: u64,
    in_flight_reads: u32,
    in_flight_writes: u32,
    /// `true` while a `Tick` self-message is already in flight; the
    /// flag prevents stacking redundant ticks.
    tick_armed: bool,
    poll_policy: PollSleepPolicy,
}

impl<M: Manager, Qp: QueuePair> QueuePairActor<M, Qp> {
    pub(super) fn new(
        qp_key: QpKey,
        local_manager: ActorRef<M>,
        peer_manager: ActorRef<M>,
        qp: Qp,
        is_loopback: bool,
        max_send_wr: u32,
        max_rd_atomic: u32,
    ) -> Self {
        let init_timeout = hyperactor_config::global::get(crate::config::RDMA_QP_INIT_TIMEOUT);
        Self {
            qp_key,
            local_manager,
            peer_manager,
            qp,
            is_loopback,
            init_timeout,
            max_send_wr,
            max_rd_atomic,
            queue: VecDeque::new(),
            posted: HashMap::new(),
            wr_to_op: HashMap::new(),
            to_poll: HashSet::new(),
            next_op_id: 0,
            in_flight_reads: 0,
            in_flight_writes: 0,
            tick_armed: false,
            poll_policy: PollSleepPolicy::new(),
        }
    }

    /// Number of WRs the QP will issue for an op that targets
    /// `local_size` bytes. The QP splits large transfers into
    /// `MAX_RDMA_MSG_SIZE`-bound chunks; a zero-byte op still
    /// consumes one WR.
    fn wr_count(local_size: usize) -> u32 {
        local_size.div_ceil(MAX_RDMA_MSG_SIZE).max(1) as u32
    }

    /// Try to post the head of `queue`.
    ///
    /// * `Ok(true)` — head was either posted or rejected with a
    ///   per-op error (e.g. op too large for this QP); caller
    ///   should attempt the next head.
    /// * `Ok(false)` — head can't be posted because the QP is at
    ///   either `max_send_wr` or, for a head read, `max_rd_atomic`.
    ///   The op stays at the head; caller should stop walking.
    /// * `Err(_)` — `qp.put`/`qp.get` failed (e.g. the QP is in
    ///   error state). Fatal: the actor's handler returns this,
    ///   which raises a supervision event.
    fn try_post_head(&mut self, cx: &Instance<Self>) -> Result<bool, anyhow::Error> {
        let pending = self.queue.pop_front().expect("non-empty queue");
        let PendingOp {
            op_idx,
            op,
            mrv,
            reply,
            wrs,
            is_read,
        } = pending;

        let local_buf = IbvBuffer {
            mr_id: mrv.id,
            lkey: mrv.lkey,
            rkey: mrv.rkey,
            addr: mrv.rdma_addr,
            size: mrv.size,
            device_name: mrv.device_name.clone(),
        };

        // 1. Per-op fatal: op alone exceeds the QP's capacity.
        if wrs > self.max_send_wr {
            let err = format!(
                "op too large for this QP [op_idx={}, qp_key={:?}, op_type={:?}, wrs={}, max_send_wr={}, local: {:?}, remote: {:?}]",
                op_idx, self.qp_key, op.op_type, wrs, self.max_send_wr, local_buf, op.remote_buffer,
            );
            reply.try_post(
                cx,
                OpResult {
                    op_idx,
                    result: Err(err),
                },
            )?;
            return Ok(true);
        }
        if is_read && wrs > self.max_rd_atomic {
            let err = format!(
                "read op too large for this QP [op_idx={}, qp_key={:?}, wrs={}, max_rd_atomic={}, local: {:?}, remote: {:?}]",
                op_idx, self.qp_key, wrs, self.max_rd_atomic, local_buf, op.remote_buffer,
            );
            reply.try_post(
                cx,
                OpResult {
                    op_idx,
                    result: Err(err),
                },
            )?;
            return Ok(true);
        }

        // 2. Credit gating. Every op consumes a send-queue slot;
        //    reads additionally consume read credits. A read at the
        //    head that hits either cap stalls the whole queue.
        let projected_total = self.in_flight_reads + self.in_flight_writes + wrs;
        if projected_total > self.max_send_wr
            || (is_read && self.in_flight_reads + wrs > self.max_rd_atomic)
        {
            self.queue.push_front(PendingOp {
                op_idx,
                op,
                mrv,
                reply,
                wrs,
                is_read,
            });
            return Ok(false);
        }

        // 3. Post.
        let post_result = match op.op_type {
            RdmaOpType::WriteFromLocal => self.qp.put(local_buf.clone(), op.remote_buffer.clone()),
            RdmaOpType::ReadIntoLocal => self.qp.get(local_buf.clone(), op.remote_buffer.clone()),
        };
        let wr_ids = post_result.map_err(|e| {
            anyhow::anyhow!(
                "qp.{} failed [op_idx={}, qp_key={:?}, local: {:?}, remote: {:?}]: {e}",
                if is_read { "get" } else { "put" },
                op_idx,
                self.qp_key,
                local_buf,
                op.remote_buffer,
            )
        })?;

        // 4. Track in-flight state.
        let op_id = self.next_op_id;
        self.next_op_id += 1;
        let mut pending_wrs = HashSet::with_capacity(wr_ids.len());
        for id in &wr_ids {
            self.wr_to_op.insert(*id, op_id);
            self.to_poll.insert(*id);
            pending_wrs.insert(*id);
        }
        if is_read {
            self.in_flight_reads += wr_ids.len() as u32;
        } else {
            self.in_flight_writes += wr_ids.len() as u32;
        }
        self.posted.insert(
            op_id,
            PostedOpEntry {
                op_idx,
                pending_wrs,
                is_read,
                _mrv: mrv,
                reply,
                first_error: None,
            },
        );
        Ok(true)
    }

    /// One scheduler round: post everything that fits, poll for
    /// completions, emit replies for finished ops, and re-arm
    /// `Tick` if work remains. Returns `Err` for fatal QP-level
    /// failures; the surrounding handler propagates the error so
    /// supervision tears the actor down.
    fn advance(&mut self, cx: &Instance<Self>) -> Result<(), anyhow::Error> {
        // 1. Post from the queue head until either it's empty or
        //    the head is credit-blocked.
        while !self.queue.is_empty() {
            if !self.try_post_head(cx)? {
                break;
            }
        }

        // 2. Poll for completions.
        let mut progressed = false;
        if !self.to_poll.is_empty() {
            let completions = self
                .qp
                .poll_completion(PollTarget::Send, &self.to_poll)
                .map_err(|e| anyhow::anyhow!("CQ poll failed for qp_key={:?}: {e}", self.qp_key))?;
            progressed = !completions.is_empty();
            for (wr_id, wc_result) in completions {
                self.to_poll.remove(&wr_id);
                let op_id = self
                    .wr_to_op
                    .remove(&wr_id)
                    .expect("completed wr_id missing from wr_to_op");
                let entry = self
                    .posted
                    .get_mut(&op_id)
                    .expect("op_id missing from posted");
                entry.pending_wrs.remove(&wr_id);
                if entry.is_read {
                    self.in_flight_reads -= 1;
                } else {
                    self.in_flight_writes -= 1;
                }
                if let Err(per_wr) = wc_result
                    && entry.first_error.is_none()
                {
                    entry.first_error = Some(per_wr.to_string());
                }
                if entry.pending_wrs.is_empty() {
                    let entry = self.posted.remove(&op_id).expect("just verified");
                    let result = match entry.first_error {
                        Some(err) => Err(err),
                        None => Ok(()),
                    };
                    entry.reply.try_post(
                        cx,
                        OpResult {
                            op_idx: entry.op_idx,
                            result,
                        },
                    )?;
                }
            }
        }
        if progressed {
            self.poll_policy.reset();
        }

        // 3. Re-arm Tick if any work remains.
        let pending_work = !self.queue.is_empty() || !self.posted.is_empty();
        if pending_work && !self.tick_armed {
            self.tick_armed = true;
            let interval = self.poll_policy.next_interval();
            if interval.is_zero() {
                cx.handle().try_post(cx, Tick)?;
            } else {
                cx.post_after(cx, Tick, interval);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<M: Manager, Qp: QueuePair> Actor for QueuePairActor<M, Qp> {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        let local_info = self.qp.get_qp_info().map_err(|e| {
            tracing::error!(qp_key = ?self.qp_key, error = %e, "QueuePairActor init: get_qp_info failed");
            anyhow::anyhow!("could not extract local QP info: {e}")
        })?;

        let peer_info = if self.is_loopback {
            // The "peer" is ourselves; skip the round-trip and
            // connect to our own endpoint.
            local_info.clone()
        } else {
            let (reply, rx) = this.mailbox().open_once_port::<Result<IbvQpInfo, String>>();
            self.peer_manager.post(
                this,
                CreatePeerQueuePair {
                    sender: self.local_manager.clone(),
                    sender_device: self.qp_key.self_device.clone(),
                    receiver_device: self.qp_key.other_device.clone(),
                    sender_info: local_info,
                    reply: reply.bind(),
                },
            );
            match tokio::time::timeout(self.init_timeout, rx.recv()).await {
                Ok(Ok(Ok(info))) => info,
                Ok(Ok(Err(e))) => {
                    tracing::error!(
                        qp_key = ?self.qp_key,
                        peer_manager = ?self.peer_manager,
                        error = %e,
                        "QueuePairActor init: peer manager rejected CreatePeerQueuePair",
                    );
                    return Err(anyhow::anyhow!("peer manager rejected QP request: {e}"));
                }
                Ok(Err(e)) => {
                    tracing::error!(
                        qp_key = ?self.qp_key,
                        peer_manager = ?self.peer_manager,
                        error = %e,
                        "QueuePairActor init: peer reply port closed",
                    );
                    return Err(anyhow::anyhow!("peer reply port closed: {e}"));
                }
                Err(_) => {
                    tracing::error!(
                        qp_key = ?self.qp_key,
                        peer_manager = ?self.peer_manager,
                        timeout = ?self.init_timeout,
                        "QueuePairActor init: timed out waiting for peer reply",
                    );
                    return Err(anyhow::anyhow!(
                        "QP initialization timed out after {:?}",
                        self.init_timeout
                    ));
                }
            }
        };

        self.qp.connect(&peer_info).map_err(|e| {
            tracing::error!(
                qp_key = ?self.qp_key,
                peer_info = ?peer_info,
                error = %e,
                "QueuePairActor init: connect failed",
            );
            anyhow::anyhow!("could not connect QP to peer: {e}")
        })?;
        Ok(())
    }
}

#[async_trait]
impl<M: Manager, Qp: QueuePair> Handler<ProcessOps<M>> for QueuePairActor<M, Qp> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: ProcessOps<M>,
    ) -> Result<(), anyhow::Error> {
        for (op_idx, op, mrv) in msg.items.into_iter() {
            let wrs = Self::wr_count(op.local_memory.size());
            let is_read = matches!(op.op_type, RdmaOpType::ReadIntoLocal);
            self.queue.push_back(PendingOp {
                op_idx,
                op,
                mrv,
                reply: msg.reply.clone(),
                wrs,
                is_read,
            });
        }
        // If a tick is already armed it will pick up the new ops on
        // its next round; advancing here would just duplicate work.
        if !self.tick_armed {
            self.advance(cx)?;
        }
        Ok(())
    }
}

#[async_trait]
impl<M: Manager, Qp: QueuePair> Handler<Tick> for QueuePairActor<M, Qp> {
    async fn handle(&mut self, cx: &Context<Self>, _msg: Tick) -> Result<(), anyhow::Error> {
        self.tick_armed = false;
        self.advance(cx)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use anyhow::Result;
    use async_trait::async_trait;
    use hyperactor::ActorHandle;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::proc::Proc;

    use super::*;
    use crate::backend::ibverbs::domain::IbvDomain;
    use crate::backend::ibverbs::primitives::IbvConfig;
    use crate::backend::ibverbs::primitives::get_all_devices;

    #[test]
    fn test_create_connection() {
        if get_all_devices().is_empty() {
            println!("Skipping test: RDMA devices not available");
            return;
        }

        let config = IbvConfig {
            use_gpu_direct: false,
            ..Default::default()
        };
        let domain = IbvDomain::new(config.device.clone());
        assert!(domain.is_ok());

        let domain = Arc::new(domain.unwrap());
        let queue_pair = IbvQueuePair::new(domain, config.clone());
        assert!(queue_pair.is_ok());
    }

    #[test]
    fn test_loopback_connection() {
        if get_all_devices().is_empty() {
            println!("Skipping test: RDMA devices not available");
            return;
        }

        let server_config = IbvConfig {
            use_gpu_direct: false,
            ..Default::default()
        };
        let client_config = IbvConfig {
            use_gpu_direct: false,
            ..Default::default()
        };

        let server_domain = Arc::new(IbvDomain::new(server_config.device.clone()).unwrap());
        let client_domain = Arc::new(IbvDomain::new(client_config.device.clone()).unwrap());

        let mut server_qp = IbvQueuePair::new(server_domain, server_config.clone()).unwrap();
        let mut client_qp = IbvQueuePair::new(client_domain, client_config.clone()).unwrap();

        let server_connection_info = server_qp.get_qp_info().unwrap();
        let client_connection_info = client_qp.get_qp_info().unwrap();

        assert!(server_qp.connect(&client_connection_info).is_ok());
        assert!(client_qp.connect(&server_connection_info).is_ok());
    }

    // =================================================================
    // QueuePairActor init handshake
    // =================================================================

    /// Captured fields from a `CreatePeerQueuePair` message; we
    /// can't keep the original because the `reply` port is consumed
    /// to send the response.
    #[derive(Debug, Clone)]
    struct CreateCapture {
        sender_id: hyperactor::ActorId,
        sender_device: String,
        receiver_device: String,
        sender_qp_num: u32,
    }

    #[derive(Debug)]
    struct QpaMockState {
        creates: Vec<CreateCapture>,
        response: Option<Result<IbvQpInfo, String>>,
        /// Forwards every supervision event the parent receives to
        /// the test.
        supervision_tx: Option<
            tokio::sync::mpsc::UnboundedSender<hyperactor::supervision::ActorSupervisionEvent>,
        >,
    }

    /// Mock manager used by `QueuePairActor` tests.
    ///
    /// Plays two roles:
    /// 1. As the *parent*, it spawns `QueuePairActor` children via
    ///    [`SpawnQpaChild`].
    /// 2. As the *peer*, it handles `CreatePeerQueuePair`.
    #[derive(Debug)]
    #[hyperactor::export(handlers = [CreatePeerQueuePair<QpaMockManager>])]
    struct QpaMockManager {
        state: Arc<Mutex<QpaMockState>>,
    }

    #[async_trait]
    impl Actor for QpaMockManager {
        async fn handle_supervision_event(
            &mut self,
            _this: &Instance<Self>,
            event: &hyperactor::supervision::ActorSupervisionEvent,
        ) -> Result<bool> {
            let tx = self.state.lock().unwrap().supervision_tx.clone();
            let Some(tx) = tx else {
                return Ok(!event.is_error());
            };
            tx.send(event.clone())
                .map_err(|e| anyhow::anyhow!("supervision_tx send failed: {e}"))?;
            Ok(true)
        }
    }

    #[async_trait]
    impl Handler<CreatePeerQueuePair<QpaMockManager>> for QpaMockManager {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: CreatePeerQueuePair<QpaMockManager>,
        ) -> Result<()> {
            let response = {
                let mut state = self.state.lock().unwrap();
                state.creates.push(CreateCapture {
                    sender_id: msg.sender.actor_addr().id().clone(),
                    sender_device: msg.sender_device.clone(),
                    receiver_device: msg.receiver_device.clone(),
                    sender_qp_num: msg.sender_info.qp_num,
                });
                state.response.take()
            };
            if let Some(response) = response {
                msg.reply.post(cx, response);
            }
            // None → drop reply intentionally to test the timeout path.
            Ok(())
        }
    }

    /// Local message that spawns a `QueuePairActor` as a child of
    /// this manager so supervision events route here. The reply
    /// carries the resulting `ActorHandle` so the test can observe
    /// lifecycle transitions.
    #[derive(Debug)]
    struct SpawnQpaChild {
        qp_key: QpKey,
        peer_manager: ActorRef<QpaMockManager>,
        qp: MockQp,
        is_loopback: bool,
        max_send_wr: u32,
        max_rd_atomic: u32,
        reply: hyperactor::OncePortHandle<ActorHandle<QueuePairActor<QpaMockManager, MockQp>>>,
    }

    #[async_trait]
    impl Handler<SpawnQpaChild> for QpaMockManager {
        async fn handle(&mut self, cx: &Context<Self>, msg: SpawnQpaChild) -> Result<()> {
            let local_manager = cx.bind::<QpaMockManager>();
            let actor = QueuePairActor::new(
                msg.qp_key,
                local_manager,
                msg.peer_manager,
                msg.qp,
                msg.is_loopback,
                msg.max_send_wr,
                msg.max_rd_atomic,
            );
            let handle = cx.spawn(actor);
            msg.reply.try_post(cx, handle)?;
            Ok(())
        }
    }

    /// One `put` or `get` call recorded by the mock, surfaced to the
    /// test via the `posted_rx` channel returned alongside the QP.
    #[derive(Debug)]
    enum PostedOp {
        Put {
            lhandle: IbvBuffer,
            rhandle: IbvBuffer,
            wr_ids: Vec<u64>,
        },
        Get {
            lhandle: IbvBuffer,
            rhandle: IbvBuffer,
            wr_ids: Vec<u64>,
        },
    }

    #[derive(Debug)]
    struct MockQpInner {
        connect_calls: Vec<IbvQpInfo>,
        next_wr_id: u64,
        /// Every `put`/`get` call is forwarded here in order, so the
        /// test body can `await` rather than poll.
        posted_tx: tokio::sync::mpsc::UnboundedSender<PostedOp>,
        /// FIFO of completions the next `poll_completion` will hand
        /// back (after filtering by `expected_wr_ids`). Each entry
        /// is either `Ok(IbvWc)` (success) or `Err(...)` (per-WR
        /// completion failure).
        pending_completions: VecDeque<(u64, std::result::Result<IbvWc, PollCompletionError>)>,
        /// One-shot CQ-level error; cleared after the next poll consumes it.
        poll_error: Option<PollCompletionError>,
        /// One-shot error for the next `put` or `get` call.
        post_error: Option<String>,
    }

    /// `QueuePair` mock used by `QueuePairActor` tests. Cloning is
    /// cheap (shared `Arc<Mutex<...>>`); the test typically holds
    /// one clone while handing another to the actor.
    #[derive(Debug, Clone)]
    struct MockQp {
        info: IbvQpInfo,
        inner: Arc<Mutex<MockQpInner>>,
    }

    impl MockQp {
        /// Returns the mock QP and the receiver that observes its
        /// posts. Tests that don't care about post events can drop
        /// the receiver.
        fn new(qp_num: u32, psn: u32) -> (Self, tokio::sync::mpsc::UnboundedReceiver<PostedOp>) {
            let (posted_tx, posted_rx) = tokio::sync::mpsc::unbounded_channel();
            let qp = Self {
                info: IbvQpInfo {
                    qp_num,
                    lid: 0,
                    gid: None,
                    psn,
                },
                inner: Arc::new(Mutex::new(MockQpInner {
                    connect_calls: Vec::new(),
                    next_wr_id: 0,
                    posted_tx,
                    pending_completions: VecDeque::new(),
                    poll_error: None,
                    post_error: None,
                })),
            };
            (qp, posted_rx)
        }

        fn connect_calls(&self) -> Vec<IbvQpInfo> {
            self.inner.lock().unwrap().connect_calls.clone()
        }

        /// Queue a successful WC for `wr_id`. The next poll whose
        /// `expected_wr_ids` contains `wr_id` returns it.
        fn queue_completion(&self, wr_id: u64) {
            self.inner
                .lock()
                .unwrap()
                .pending_completions
                .push_back((wr_id, Ok(IbvWc::for_test(wr_id, true))));
        }

        /// Queue a per-WR completion failure for `wr_id` — the actor
        /// receives it as the inner `Err` from `poll_completion` and
        /// should fail just that op (not poison the QP).
        fn queue_per_wr_error(&self, wr_id: u64, message: &str) {
            self.inner
                .lock()
                .unwrap()
                .pending_completions
                .push_back((wr_id, Err(PollCompletionError::for_test(message))));
        }

        /// Queue a CQ-level poll error (one-shot). The next poll
        /// returns this as the outer `Err`, simulating a poisoned QP.
        fn queue_poll_error(&self, err: PollCompletionError) {
            self.inner.lock().unwrap().poll_error = Some(err);
        }

        /// Make the next `put`/`get` return this error (one-shot).
        fn queue_post_error(&self, message: &str) {
            self.inner.lock().unwrap().post_error = Some(message.to_string());
        }
    }

    impl QueuePair for MockQp {
        fn get_qp_info(&mut self) -> Result<IbvQpInfo> {
            Ok(self.info.clone())
        }

        fn connect(&mut self, info: &IbvQpInfo) -> Result<()> {
            self.inner.lock().unwrap().connect_calls.push(info.clone());
            Ok(())
        }

        fn put(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> Result<Vec<u64>> {
            let mut inner = self.inner.lock().unwrap();
            if let Some(msg) = inner.post_error.take() {
                return Err(anyhow::anyhow!(msg));
            }
            let wrs = lhandle.size.div_ceil(MAX_RDMA_MSG_SIZE).max(1);
            let mut wr_ids = Vec::with_capacity(wrs);
            for _ in 0..wrs {
                wr_ids.push(inner.next_wr_id);
                inner.next_wr_id += 1;
            }
            let _ = inner.posted_tx.send(PostedOp::Put {
                lhandle,
                rhandle,
                wr_ids: wr_ids.clone(),
            });
            Ok(wr_ids)
        }

        fn get(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> Result<Vec<u64>> {
            let mut inner = self.inner.lock().unwrap();
            if let Some(msg) = inner.post_error.take() {
                return Err(anyhow::anyhow!(msg));
            }
            let wrs = lhandle.size.div_ceil(MAX_RDMA_MSG_SIZE).max(1);
            let mut wr_ids = Vec::with_capacity(wrs);
            for _ in 0..wrs {
                wr_ids.push(inner.next_wr_id);
                inner.next_wr_id += 1;
            }
            let _ = inner.posted_tx.send(PostedOp::Get {
                lhandle,
                rhandle,
                wr_ids: wr_ids.clone(),
            });
            Ok(wr_ids)
        }

        fn poll_completion(
            &mut self,
            _target: PollTarget,
            expected_wr_ids: &HashSet<u64>,
        ) -> std::result::Result<
            Vec<(u64, std::result::Result<IbvWc, PollCompletionError>)>,
            PollCompletionError,
        > {
            let mut inner = self.inner.lock().unwrap();
            if let Some(err) = inner.poll_error.take() {
                return Err(err);
            }
            let mut matched = Vec::new();
            let mut remaining = VecDeque::new();
            while let Some((wr_id, wc)) = inner.pending_completions.pop_front() {
                if expected_wr_ids.contains(&wr_id) {
                    matched.push((wr_id, wc));
                } else {
                    remaining.push_back((wr_id, wc));
                }
            }
            inner.pending_completions = remaining;
            Ok(matched)
        }
    }

    struct QpaHarness {
        proc: Proc,
        parent: ActorHandle<QpaMockManager>,
        peer: ActorHandle<QpaMockManager>,
        peer_state: Arc<Mutex<QpaMockState>>,
        client: hyperactor::Client,
        supervision_rx:
            tokio::sync::mpsc::UnboundedReceiver<hyperactor::supervision::ActorSupervisionEvent>,
    }

    impl QpaHarness {
        fn build() -> Result<Self> {
            let proc = Proc::anonymous();
            let (supervision_tx, supervision_rx) = tokio::sync::mpsc::unbounded_channel();
            let parent = proc.spawn_with_label(
                "parent",
                QpaMockManager {
                    state: Arc::new(Mutex::new(QpaMockState {
                        creates: Vec::new(),
                        response: None,
                        supervision_tx: Some(supervision_tx),
                    })),
                },
            );
            let peer_state = Arc::new(Mutex::new(QpaMockState {
                creates: Vec::new(),
                response: None,
                supervision_tx: None,
            }));
            let peer = proc.spawn_with_label(
                "peer",
                QpaMockManager {
                    state: Arc::clone(&peer_state),
                },
            );
            let client = proc.client("client");
            Ok(Self {
                proc,
                parent,
                peer,
                peer_state,
                client,
                supervision_rx,
            })
        }

        fn peer_id(&self) -> hyperactor::ActorId {
            self.peer.actor_addr().id().clone()
        }

        fn parent_id(&self) -> hyperactor::ActorId {
            self.parent.actor_addr().id().clone()
        }

        /// Await the next forwarded child-error supervision event.
        async fn next_supervision_failure(
            &mut self,
        ) -> hyperactor::supervision::ActorSupervisionEvent {
            tokio::time::timeout(Duration::from_secs(5), self.supervision_rx.recv())
                .await
                .expect("timed out waiting for child failure event")
                .expect("supervision channel closed")
        }

        /// Destroys the proc, closes the supervision channel, drains
        /// every remaining event, and asserts none are unexpected.
        async fn teardown(mut self) {
            self.supervision_rx.close();
            self.proc
                .destroy_and_wait(Duration::from_secs(30), "test teardown")
                .await
                .expect("destroy_and_wait failed");
            let mut leftover = Vec::new();
            while let Some(event) = self.supervision_rx.recv().await {
                leftover.push(event);
            }
            assert!(
                leftover.is_empty(),
                "unexpected supervision events at teardown: {leftover:?}",
            );
        }

        async fn spawn_actor(
            &self,
            qp_key: QpKey,
            peer_manager: ActorRef<QpaMockManager>,
            qp: MockQp,
            is_loopback: bool,
        ) -> Result<ActorHandle<QueuePairActor<QpaMockManager, MockQp>>> {
            self.spawn_actor_with_caps(qp_key, peer_manager, qp, is_loopback, 4, 2)
                .await
        }

        async fn spawn_actor_with_caps(
            &self,
            qp_key: QpKey,
            peer_manager: ActorRef<QpaMockManager>,
            qp: MockQp,
            is_loopback: bool,
            max_send_wr: u32,
            max_rd_atomic: u32,
        ) -> Result<ActorHandle<QueuePairActor<QpaMockManager, MockQp>>> {
            let (reply, rx) = self.client.mailbox().open_once_port();
            self.parent.try_post(
                &self.client,
                SpawnQpaChild {
                    qp_key,
                    peer_manager,
                    qp,
                    is_loopback,
                    max_send_wr,
                    max_rd_atomic,
                    reply,
                },
            )?;
            Ok(rx.recv().await?)
        }
    }

    async fn await_status(
        handle: &ActorHandle<QueuePairActor<QpaMockManager, MockQp>>,
        expected: impl Fn(&hyperactor::actor::ActorStatus) -> bool,
    ) -> hyperactor::actor::ActorStatus {
        let mut status = handle.status();
        status.wait_for(|s| expected(s)).await.unwrap();
        status.borrow().clone()
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_init_succeeds_via_peer() -> Result<()> {
        let harness = QpaHarness::build()?;
        let peer_info = IbvQpInfo {
            qp_num: 0xbeef,
            lid: 0,
            gid: None,
            psn: 0xc0ffee,
        };
        harness.peer_state.lock().unwrap().response = Some(Ok(peer_info.clone()));

        let (qp, _posted_rx) = MockQp::new(0x1234, 0xdead);
        let qp_key = QpKey {
            self_device: "mlx5_0".into(),
            other_id: harness.peer_id(),
            other_device: "mlx5_1".into(),
        };
        let handle = harness
            .spawn_actor(
                qp_key.clone(),
                harness.peer.bind::<QpaMockManager>(),
                qp.clone(),
                false,
            )
            .await?;

        await_status(&handle, |s| {
            matches!(s, hyperactor::actor::ActorStatus::Idle)
        })
        .await;

        let creates = harness.peer_state.lock().unwrap().creates.clone();
        assert_eq!(creates.len(), 1);
        assert_eq!(creates[0].sender_device, "mlx5_0");
        assert_eq!(creates[0].receiver_device, "mlx5_1");
        assert_eq!(creates[0].sender_qp_num, 0x1234);
        // The sender ref carries the local manager's identity so the
        // receiver can build its own `QpKey`.
        assert_eq!(creates[0].sender_id, harness.parent_id());

        let connects = qp.connect_calls();
        assert_eq!(connects.len(), 1);
        assert_eq!(connects[0].qp_num, peer_info.qp_num);
        assert_eq!(connects[0].psn, peer_info.psn);
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_init_loopback_skips_peer_round_trip() -> Result<()> {
        let harness = QpaHarness::build()?;

        let (qp, _posted_rx) = MockQp::new(0xaaa, 0xbbb);
        let qp_key = QpKey {
            self_device: "mlx5_0".into(),
            other_id: harness.parent_id(),
            other_device: "mlx5_0".into(),
        };
        let handle = harness
            .spawn_actor(
                qp_key,
                harness.peer.bind::<QpaMockManager>(),
                qp.clone(),
                true,
            )
            .await?;

        await_status(&handle, |s| {
            matches!(s, hyperactor::actor::ActorStatus::Idle)
        })
        .await;
        assert!(harness.peer_state.lock().unwrap().creates.is_empty());

        let connects = qp.connect_calls();
        assert_eq!(connects.len(), 1);
        assert_eq!(connects[0].qp_num, 0xaaa);
        assert_eq!(connects[0].psn, 0xbbb);
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_init_peer_error_fails_actor() -> Result<()> {
        let mut harness = QpaHarness::build()?;
        harness.peer_state.lock().unwrap().response =
            Some(Err("peer rejected, no domain on receiver_device".into()));

        let qp_key = QpKey {
            self_device: "mlx5_0".into(),
            other_id: harness.peer_id(),
            other_device: "mlx5_99".into(),
        };
        let (qp, _posted_rx) = MockQp::new(1, 2);
        let handle = harness
            .spawn_actor(qp_key, harness.peer.bind::<QpaMockManager>(), qp, false)
            .await?;

        let event = harness.next_supervision_failure().await;
        assert_eq!(&event.actor_id, handle.actor_addr());
        let report = event.failure_report().expect("event should be a failure");
        assert!(
            report.contains("peer rejected"),
            "failure report should surface the peer's error string; got: {report}"
        );
        await_status(&handle, |s| {
            matches!(s, hyperactor::actor::ActorStatus::Failed(_))
        })
        .await;
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_init_timeout_fails_actor() -> Result<()> {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(
            crate::config::RDMA_QP_INIT_TIMEOUT,
            Duration::from_millis(100),
        );

        let mut harness = QpaHarness::build()?;

        let qp_key = QpKey {
            self_device: "mlx5_0".into(),
            other_id: harness.peer_id(),
            other_device: "mlx5_1".into(),
        };
        let (qp, _posted_rx) = MockQp::new(1, 2);
        let handle = harness
            .spawn_actor(qp_key, harness.peer.bind::<QpaMockManager>(), qp, false)
            .await?;

        let event = harness.next_supervision_failure().await;
        assert_eq!(&event.actor_id, handle.actor_addr());
        let report = event.failure_report().expect("event should be a failure");
        assert!(
            report.contains("timed out"),
            "failure report should mention timeout; got: {report}"
        );
        await_status(&handle, |s| {
            matches!(s, hyperactor::actor::ActorStatus::Failed(_))
        })
        .await;
        harness.teardown().await;
        Ok(())
    }

    // =================================================================
    // QueuePairActor op processing
    // =================================================================

    use crate::backend::ibverbs::primitives::IbvMemoryRegion;
    use crate::local_memory::Keepalive;
    use crate::local_memory::KeepaliveLocalMemory;

    fn null_domain() -> Arc<IbvDomain> {
        Arc::new(IbvDomain {
            context: std::ptr::null_mut(),
            pd: std::ptr::null_mut(),
        })
    }

    /// No-op [`Keepalive`] for tests that mint a [`KeepaliveLocalMemory`]
    /// from a fake address never actually read or written through.
    struct FakeKeepalive;
    impl Keepalive for FakeKeepalive {}

    fn fake_local_memory(addr: usize, size: usize) -> Arc<KeepaliveLocalMemory> {
        Arc::new(KeepaliveLocalMemory::new(
            addr,
            size,
            Arc::new(FakeKeepalive),
        ))
    }

    fn fake_mrv(id: usize, addr: usize, size: usize) -> IbvMemoryRegionView {
        IbvMemoryRegionView::new(
            id,
            addr,
            addr,
            size,
            0x1234,
            0x5678,
            "dev0".to_string(),
            Arc::new(IbvMemoryRegion::Direct {
                mr: std::ptr::null_mut(),
                _domain: null_domain(),
            }),
        )
    }

    /// Build a `QpaMockManager` ref attested to an unrelated proc;
    /// only used to populate `IbvOp::remote_manager` (the actor
    /// never sends to it during op processing — it just reads
    /// `op.remote_buffer`).
    fn fake_remote_ref() -> ActorRef<QpaMockManager> {
        let proc_id = hyperactor::id::ProcId::new(
            hyperactor::id::Uid::Instance(0xc0ffee, None),
            Some(hyperactor::id::Label::new("remote").unwrap()),
        );
        let proc_addr =
            hyperactor::ProcAddr::new(proc_id, hyperactor::channel::ChannelAddr::Local(1).into());
        ActorRef::attest(proc_addr.actor_addr("remote-mgr"))
    }

    fn make_op(op_type: RdmaOpType, addr: usize, size: usize) -> IbvOp<QpaMockManager> {
        IbvOp {
            op_type,
            local_memory: fake_local_memory(addr, size),
            remote_buffer: IbvBuffer {
                mr_id: 1,
                lkey: 0,
                rkey: 0,
                addr: 0x4000_0000,
                size,
                device_name: "remote_dev".to_string(),
            },
            remote_manager: fake_remote_ref(),
        }
    }

    impl QpaHarness {
        /// Spawn a `QueuePairActor` in loopback mode (no peer round
        /// trip) so init completes immediately; await it reaching
        /// `Idle`. Returns the actor handle along with the mock QP
        /// the test can drive.
        async fn spawn_ready_actor(
            &self,
            max_send_wr: u32,
            max_rd_atomic: u32,
        ) -> Result<(
            ActorHandle<QueuePairActor<QpaMockManager, MockQp>>,
            MockQp,
            tokio::sync::mpsc::UnboundedReceiver<PostedOp>,
        )> {
            let (qp, posted_rx) = MockQp::new(0x1, 0x2);
            let qp_key = QpKey {
                self_device: "mlx5_0".into(),
                other_id: self.parent_id(),
                other_device: "mlx5_0".into(),
            };
            let handle = self
                .spawn_actor_with_caps(
                    qp_key,
                    self.peer.bind::<QpaMockManager>(),
                    qp.clone(),
                    true,
                    max_send_wr,
                    max_rd_atomic,
                )
                .await?;
            await_status(&handle, |s| {
                matches!(s, hyperactor::actor::ActorStatus::Idle)
            })
            .await;
            Ok((handle, qp, posted_rx))
        }
    }

    /// Await the next `PostedOp` from the mock; panics on timeout.
    async fn recv_posted(rx: &mut tokio::sync::mpsc::UnboundedReceiver<PostedOp>) -> PostedOp {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for post on MockQp")
            .expect("MockQp post channel closed")
    }

    /// Assert that no further `PostedOp` arrives in `wait`. Used to
    /// pin down a credit-blocked state.
    async fn assert_no_post(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<PostedOp>,
        wait: Duration,
    ) {
        if let Ok(Some(p)) = tokio::time::timeout(wait, rx.recv()).await {
            panic!("unexpected post on MockQp: {p:?}");
        }
    }

    fn expect_put(p: PostedOp) -> (IbvBuffer, IbvBuffer, Vec<u64>) {
        match p {
            PostedOp::Put {
                lhandle,
                rhandle,
                wr_ids,
            } => (lhandle, rhandle, wr_ids),
            other => panic!("expected Put, got {other:?}"),
        }
    }

    fn expect_get(p: PostedOp) -> (IbvBuffer, IbvBuffer, Vec<u64>) {
        match p {
            PostedOp::Get {
                lhandle,
                rhandle,
                wr_ids,
            } => (lhandle, rhandle, wr_ids),
            other => panic!("expected Get, got {other:?}"),
        }
    }

    /// Open a multi-shot port and send a `ProcessOps` batch. Returns
    /// the receiver so the caller can await per-op results.
    fn submit_ops(
        harness: &QpaHarness,
        actor: &ActorHandle<QueuePairActor<QpaMockManager, MockQp>>,
        items: Vec<(usize, IbvOp<QpaMockManager>, IbvMemoryRegionView)>,
    ) -> Result<hyperactor::mailbox::PortReceiver<OpResult>> {
        let (reply, rx) = harness.client.mailbox().open_port::<OpResult>();
        actor.try_post(&harness.client, ProcessOps { items, reply })?;
        Ok(rx)
    }

    /// Collect exactly `n` replies with a per-recv timeout, sorted
    /// by op_idx for deterministic comparison.
    async fn collect_replies(
        rx: &mut hyperactor::mailbox::PortReceiver<OpResult>,
        n: usize,
    ) -> Vec<(usize, Result<(), String>)> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let m = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out waiting for ProcessOps reply")
                .expect("ProcessOps reply port closed");
            out.push((m.op_idx, m.result));
        }
        out.sort_by_key(|(i, _)| *i);
        out
    }

    /// Try to recv with a short timeout, returning `None` on timeout
    /// so callers can assert "no reply yet".
    async fn try_recv(
        rx: &mut hyperactor::mailbox::PortReceiver<OpResult>,
        wait: Duration,
    ) -> Option<(usize, Result<(), String>)> {
        match tokio::time::timeout(wait, rx.recv()).await {
            Ok(Ok(m)) => Some((m.op_idx, m.result)),
            Ok(Err(e)) => panic!("ProcessOps reply port closed: {e}"),
            Err(_) => None,
        }
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_processes_single_write() -> Result<()> {
        let harness = QpaHarness::build()?;
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(4, 2).await?;

        let items = vec![(
            7usize,
            make_op(RdmaOpType::WriteFromLocal, 0x1000, 4096),
            fake_mrv(1, 0x1000, 4096),
        )];
        let mut rx = submit_ops(&harness, &actor, items)?;

        // wr_ids start at 0 (fresh MockQp), so the single WR is wr 0.
        let (lhandle, rhandle, wr_ids) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(wr_ids, vec![0]);
        assert_eq!(lhandle.addr, 0x1000);
        assert_eq!(lhandle.size, 4096);
        assert_eq!(lhandle.lkey, 0x1234);
        assert_eq!(lhandle.rkey, 0x5678);
        assert_eq!(lhandle.device_name, "dev0");
        assert_eq!(rhandle.addr, 0x4000_0000);
        assert_eq!(rhandle.size, 4096);
        assert_eq!(rhandle.device_name, "remote_dev");

        qp.queue_completion(0);
        let replies = collect_replies(&mut rx, 1).await;
        assert_eq!(replies, vec![(7, Ok(()))]);
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_chunked_op_waits_for_all_wrs() -> Result<()> {
        let harness = QpaHarness::build()?;
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(8, 4).await?;

        // A 3-chunk write (3 * MAX_RDMA_MSG_SIZE) splits into 3 WRs;
        // the op's reply must be held back until all 3 complete.
        let items = vec![(
            11usize,
            make_op(RdmaOpType::WriteFromLocal, 0x1000, 3 * MAX_RDMA_MSG_SIZE),
            fake_mrv(1, 0x1000, 3 * MAX_RDMA_MSG_SIZE),
        )];
        let mut rx = submit_ops(&harness, &actor, items)?;

        let (_, _, wr_ids) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(wr_ids, vec![0, 1, 2]);

        // Complete the first two — no reply yet.
        qp.queue_completion(wr_ids[0]);
        qp.queue_completion(wr_ids[1]);
        assert!(
            try_recv(&mut rx, Duration::from_millis(100))
                .await
                .is_none(),
            "op should not be reported until every WR has completed",
        );

        // Complete the third — now the op resolves.
        qp.queue_completion(wr_ids[2]);
        let replies = collect_replies(&mut rx, 1).await;
        assert_eq!(replies, vec![(11, Ok(()))]);
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_per_wr_error_held_until_all_wrs_complete() -> Result<()> {
        let harness = QpaHarness::build()?;
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(8, 4).await?;

        // 3-WR write: simulate the second WR failing first; the op's
        // Err must not fire until the other 2 WRs have also reported
        // so the MR registration outlives every in-flight WR.
        let items = vec![(
            42usize,
            make_op(RdmaOpType::WriteFromLocal, 0x1000, 3 * MAX_RDMA_MSG_SIZE),
            fake_mrv(1, 0x1000, 3 * MAX_RDMA_MSG_SIZE),
        )];
        let mut rx = submit_ops(&harness, &actor, items)?;

        let (_, _, wr_ids) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(wr_ids.len(), 3);

        // Deliver the per-WR error first.
        qp.queue_per_wr_error(wr_ids[1], "simulated WR fail");
        assert!(
            try_recv(&mut rx, Duration::from_millis(100))
                .await
                .is_none(),
            "op error must wait for the other WRs to drain",
        );

        // The remaining WRs complete (flush-style or otherwise) and
        // finally the op's Err is reported.
        qp.queue_completion(wr_ids[0]);
        qp.queue_completion(wr_ids[2]);

        let replies = collect_replies(&mut rx, 1).await;
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].0, 42);
        let err = replies[0]
            .1
            .as_ref()
            .expect_err("op should fail because one WR errored");
        assert!(
            err.contains("simulated WR fail"),
            "op error should surface the per-WR error: {err}",
        );
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_per_wr_error_isolates_to_failing_op() -> Result<()> {
        let harness = QpaHarness::build()?;
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(8, 4).await?;

        // Two writes share a batch: op_idx 0 is multi-WR (3 chunks),
        // op_idx 1 is single-WR. One of op_idx 0's WRs fails;
        // op_idx 1's WR succeeds independently.
        let items = vec![
            (
                0usize,
                make_op(RdmaOpType::WriteFromLocal, 0x1000, 3 * MAX_RDMA_MSG_SIZE),
                fake_mrv(1, 0x1000, 3 * MAX_RDMA_MSG_SIZE),
            ),
            (
                1usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(2, 0x2000, 4096),
            ),
        ];
        let mut rx = submit_ops(&harness, &actor, items)?;

        let (_, _, big_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        let (_, _, small_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(big_wrs, vec![0, 1, 2]);
        assert_eq!(small_wrs, vec![3]);

        // op_idx 1 completes successfully on its own.
        qp.queue_completion(small_wrs[0]);
        // op_idx 0: middle WR fails, others succeed.
        qp.queue_per_wr_error(big_wrs[1], "isolated per-WR fail");
        qp.queue_completion(big_wrs[0]);
        qp.queue_completion(big_wrs[2]);

        let replies = collect_replies(&mut rx, 2).await;
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].0, 0);
        let err = replies[0]
            .1
            .as_ref()
            .expect_err("op_idx 0 should fail because one WR errored");

        assert!(
            err.contains("isolated per-WR fail"),
            "op_idx 0 error should name the per-WR error: {err}",
        );
        assert_eq!(replies[1], (1usize, Ok(())));
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_read_credit_gating() -> Result<()> {
        let harness = QpaHarness::build()?;
        // max_rd_atomic=2 lets at most 2 RDMA_READs sit on the QP.
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(8, 2).await?;

        let items = (0..3usize)
            .map(|i| {
                (
                    i,
                    make_op(RdmaOpType::ReadIntoLocal, 0x1000 + i * 0x1000, 4096),
                    fake_mrv(i + 1, 0x1000 + i * 0x1000, 4096),
                )
            })
            .collect();
        let mut rx = submit_ops(&harness, &actor, items)?;

        // Only the first two reads make it onto the wire; the third
        // stays parked at the queue head.
        let (_, _, r0_wrs) = expect_get(recv_posted(&mut posted_rx).await);
        let (_, _, r1_wrs) = expect_get(recv_posted(&mut posted_rx).await);
        assert_eq!(r0_wrs, vec![0]);
        assert_eq!(r1_wrs, vec![1]);
        assert_no_post(&mut posted_rx, Duration::from_millis(50)).await;

        // Complete the first read; the third should post.
        qp.queue_completion(r0_wrs[0]);
        let (_, _, r2_wrs) = expect_get(recv_posted(&mut posted_rx).await);
        assert_eq!(r2_wrs, vec![2]);

        qp.queue_completion(r1_wrs[0]);
        qp.queue_completion(r2_wrs[0]);
        let replies = collect_replies(&mut rx, 3).await;
        assert_eq!(replies, vec![(0, Ok(())), (1, Ok(())), (2, Ok(()))]);
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_write_slot_gating() -> Result<()> {
        let harness = QpaHarness::build()?;
        // max_send_wr=2 caps total in-flight WRs; submit 4 writes.
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(2, 8).await?;

        let items = (0..4usize)
            .map(|i| {
                (
                    i,
                    make_op(RdmaOpType::WriteFromLocal, 0x1000 + i * 0x1000, 4096),
                    fake_mrv(i + 1, 0x1000 + i * 0x1000, 4096),
                )
            })
            .collect();
        let mut rx = submit_ops(&harness, &actor, items)?;

        let (_, _, w0_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        let (_, _, w1_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(w0_wrs, vec![0]);
        assert_eq!(w1_wrs, vec![1]);
        assert_no_post(&mut posted_rx, Duration::from_millis(50)).await;

        // Complete one — the third write should post; one slot still busy.
        qp.queue_completion(w0_wrs[0]);
        let (_, _, w2_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(w2_wrs, vec![2]);
        assert_no_post(&mut posted_rx, Duration::from_millis(50)).await;

        // Complete another — the fourth write posts.
        qp.queue_completion(w1_wrs[0]);
        let (_, _, w3_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(w3_wrs, vec![3]);

        qp.queue_completion(w2_wrs[0]);
        qp.queue_completion(w3_wrs[0]);
        let replies = collect_replies(&mut rx, 4).await;
        assert_eq!(
            replies,
            vec![(0, Ok(())), (1, Ok(())), (2, Ok(())), (3, Ok(()))],
        );
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_blocked_until_one_read_and_one_write_complete() -> Result<()> {
        let harness = QpaHarness::build()?;
        // max_send_wr=4, max_rd_atomic=2. The trailing 2-WR read
        // needs *both* a free read credit (only 1 in use) and a free
        // slot (currently full at 4) — so it sits parked until one
        // read AND one write complete.
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(4, 2).await?;

        let items = vec![
            (
                0usize,
                make_op(RdmaOpType::ReadIntoLocal, 0x1000, 4096),
                fake_mrv(1, 0x1000, 4096),
            ),
            (
                1usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(2, 0x2000, 4096),
            ),
            (
                2usize,
                make_op(RdmaOpType::WriteFromLocal, 0x3000, 4096),
                fake_mrv(3, 0x3000, 4096),
            ),
            (
                3usize,
                make_op(RdmaOpType::WriteFromLocal, 0x4000, 4096),
                fake_mrv(4, 0x4000, 4096),
            ),
            (
                4usize,
                make_op(RdmaOpType::ReadIntoLocal, 0x5000, 2 * MAX_RDMA_MSG_SIZE),
                fake_mrv(5, 0x5000, 2 * MAX_RDMA_MSG_SIZE),
            ),
        ];
        let mut rx = submit_ops(&harness, &actor, items)?;

        // First four post: 1 read WR (op 0) + 3 write WRs (ops 1-3).
        let (_, _, r0_wrs) = expect_get(recv_posted(&mut posted_rx).await);
        let (_, _, w1_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        let (_, _, w2_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        let (_, _, w3_wrs) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(r0_wrs, vec![0]);
        assert_eq!(w1_wrs, vec![1]);
        assert_eq!(w2_wrs, vec![2]);
        assert_eq!(w3_wrs, vec![3]);
        // The 2-WR read at op_idx 4 stays parked.
        assert_no_post(&mut posted_rx, Duration::from_millis(50)).await;

        // Completing just the in-flight read frees a read credit but
        // doesn't free a slot — op 4 still blocks.
        qp.queue_completion(r0_wrs[0]);
        let first_reply = collect_replies(&mut rx, 1).await;
        assert_eq!(first_reply, vec![(0, Ok(()))]);
        assert_no_post(&mut posted_rx, Duration::from_millis(50)).await;

        // Completing one write frees the last slot needed; op 4 posts.
        qp.queue_completion(w1_wrs[0]);
        let second_reply = collect_replies(&mut rx, 1).await;
        assert_eq!(second_reply, vec![(1, Ok(()))]);
        let (_, _, r4_wrs) = expect_get(recv_posted(&mut posted_rx).await);
        assert_eq!(r4_wrs, vec![4, 5]);

        // Drain everything else.
        qp.queue_completion(w2_wrs[0]);
        qp.queue_completion(w3_wrs[0]);
        for &id in &r4_wrs {
            qp.queue_completion(id);
        }
        let rest = collect_replies(&mut rx, 3).await;
        assert_eq!(rest, vec![(2, Ok(())), (3, Ok(())), (4, Ok(()))]);
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_multiple_batches_share_credit() -> Result<()> {
        let harness = QpaHarness::build()?;
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(4, 2).await?;

        // Batch A: 2 writes with op_idx 10, 11.
        let batch_a = vec![
            (
                10usize,
                make_op(RdmaOpType::WriteFromLocal, 0x1000, 4096),
                fake_mrv(1, 0x1000, 4096),
            ),
            (
                11usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(2, 0x2000, 4096),
            ),
        ];
        let mut rx_a = submit_ops(&harness, &actor, batch_a)?;

        // Batch B: 2 writes with op_idx 20, 21. Shares the QP with
        // Batch A — together they sit at 4/4 max_send_wr.
        let batch_b = vec![
            (
                20usize,
                make_op(RdmaOpType::WriteFromLocal, 0x3000, 4096),
                fake_mrv(3, 0x3000, 4096),
            ),
            (
                21usize,
                make_op(RdmaOpType::WriteFromLocal, 0x4000, 4096),
                fake_mrv(4, 0x4000, 4096),
            ),
        ];
        let mut rx_b = submit_ops(&harness, &actor, batch_b)?;

        // Collect 4 post events (one per write).
        let mut all_wr_ids = Vec::new();
        for _ in 0..4 {
            let (_, _, wrs) = expect_put(recv_posted(&mut posted_rx).await);
            all_wr_ids.extend(wrs);
        }
        for &id in &all_wr_ids {
            qp.queue_completion(id);
        }

        let replies_a = collect_replies(&mut rx_a, 2).await;
        assert_eq!(replies_a, vec![(10, Ok(())), (11, Ok(()))]);
        let replies_b = collect_replies(&mut rx_b, 2).await;
        assert_eq!(replies_b, vec![(20, Ok(())), (21, Ok(()))]);
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_op_too_large_for_qp() -> Result<()> {
        let harness = QpaHarness::build()?;
        // max_send_wr=1 with a 2-chunk write → can never fit.
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(1, 1).await?;

        let items = vec![
            (
                0usize,
                make_op(RdmaOpType::WriteFromLocal, 0x1000, 2 * MAX_RDMA_MSG_SIZE),
                fake_mrv(1, 0x1000, 2 * MAX_RDMA_MSG_SIZE),
            ),
            (
                1usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(2, 0x2000, 4096),
            ),
        ];
        let mut rx = submit_ops(&harness, &actor, items)?;

        // Only op_idx 1 reaches the wire (op_idx 0 was rejected).
        let (_, _, wrs) = expect_put(recv_posted(&mut posted_rx).await);
        assert_eq!(wrs, vec![0]);
        qp.queue_completion(0);

        let replies = collect_replies(&mut rx, 2).await;
        // op_idx 0 must report a "too large" error; op_idx 1 succeeds.
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].0, 0);
        let err = replies[0]
            .1
            .as_ref()
            .expect_err("op_idx 0 should fail as too large");
        assert!(err.contains("too large"), "expected too-large error: {err}");
        assert_eq!(replies[1], (1usize, Ok(())));
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_poll_error_kills_actor_via_supervision() -> Result<()> {
        let mut harness = QpaHarness::build()?;
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(4, 2).await?;

        // Post one op so the next poll has something to look at.
        let items = vec![(
            0usize,
            make_op(RdmaOpType::WriteFromLocal, 0x1000, 4096),
            fake_mrv(1, 0x1000, 4096),
        )];
        let _rx = submit_ops(&harness, &actor, items)?;
        let _ = recv_posted(&mut posted_rx).await;
        qp.queue_poll_error(PollCompletionError::for_test("simulated CQ poison"));

        let event = harness.next_supervision_failure().await;
        assert_eq!(&event.actor_id, actor.actor_addr());
        let report = event.failure_report().expect("event should be a failure");
        assert!(
            report.contains("CQ poll failed") && report.contains("simulated CQ poison"),
            "supervision report should name the poll failure: {report}",
        );
        await_status(&actor, |s| {
            matches!(s, hyperactor::actor::ActorStatus::Failed(_))
        })
        .await;
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn qpa_post_error_kills_actor_via_supervision() -> Result<()> {
        let mut harness = QpaHarness::build()?;
        let (actor, qp, _posted_rx) = harness.spawn_ready_actor(4, 2).await?;

        qp.queue_post_error("simulated post failure");
        let items = vec![(
            0usize,
            make_op(RdmaOpType::WriteFromLocal, 0x1000, 4096),
            fake_mrv(1, 0x1000, 4096),
        )];
        let _rx = submit_ops(&harness, &actor, items)?;

        let event = harness.next_supervision_failure().await;
        assert_eq!(&event.actor_id, actor.actor_addr());
        let report = event.failure_report().expect("event should be a failure");
        assert!(
            report.contains("qp.put failed") && report.contains("simulated post failure"),
            "supervision report should name the post failure: {report}",
        );
        await_status(&actor, |s| {
            matches!(s, hyperactor::actor::ActorStatus::Failed(_))
        })
        .await;
        harness.teardown().await;
        Ok(())
    }
}
