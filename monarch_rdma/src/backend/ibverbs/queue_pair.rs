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

use std::io::Error;
use std::result::Result;
use std::time::Duration;

use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use super::primitives::Gid;
use super::primitives::IbvConfig;
use super::primitives::IbvOperation;
use super::primitives::IbvQpInfo;
use super::primitives::IbvWc;
use super::primitives::resolve_qp_type;
use crate::rdma_components::RdmaRemoteBuffer;

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
/// - The `qp` field stores a pointer to `rdmaxcel_qp_t` (not `ibv_qp`)
/// - `rdmaxcel_qp_t` contains atomic counters and completion caches internally
/// - This makes IbvQueuePair trivially Clone and Serialize
/// - Multiple clones share the same underlying rdmaxcel_qp_t via the pointer
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
pub struct IbvQueuePair {
    pub send_cq: usize,    // *mut rdmaxcel_sys::ibv_cq,
    pub recv_cq: usize,    // *mut rdmaxcel_sys::ibv_cq,
    pub qp: usize,         // *mut rdmaxcel_sys::rdmaxcel_qp_t
    pub dv_qp: usize,      // *mut rdmaxcel_sys::mlx5dv_qp,
    pub dv_send_cq: usize, // *mut rdmaxcel_sys::mlx5dv_cq,
    pub dv_recv_cq: usize, // *mut rdmaxcel_sys::mlx5dv_cq,
    context: usize,        // *mut rdmaxcel_sys::ibv_context,
    config: IbvConfig,
    is_efa: bool,
}
wirevalue::register_type!(IbvQueuePair);

impl IbvQueuePair {
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
                    // Sync context within unsafe block; Clock::sleep is async
                    // and converting would require propagating async through the
                    // entire post_op / ring_doorbell call chain.
                    #[allow(clippy::disallowed_methods)]
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
    pub fn new(
        context: *mut rdmaxcel_sys::ibv_context,
        pd: *mut rdmaxcel_sys::ibv_pd,
        config: IbvConfig,
    ) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating an IbvQueuePair from config {}", config);
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

            let rts_timestamp_nanos = RealClock
                .system_time_now()
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
        let rts_timestamp_nanos = RealClock
            .system_time_now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        unsafe {
            rdmaxcel_sys::rdmaxcel_qp_store_rts_timestamp(qp, rts_timestamp_nanos);
        }

        Ok(())
    }

    pub fn recv(
        &mut self,
        lhandle: RdmaRemoteBuffer,
        rhandle: RdmaRemoteBuffer,
    ) -> Result<u64, anyhow::Error> {
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
        lhandle: RdmaRemoteBuffer,
        rhandle: RdmaRemoteBuffer,
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
        lhandle: RdmaRemoteBuffer,
        rhandle: RdmaRemoteBuffer,
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
        lhandle: RdmaRemoteBuffer,
        rhandle: RdmaRemoteBuffer,
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
        lhandle: RdmaRemoteBuffer,
        rhandle: RdmaRemoteBuffer,
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
        lhandle: RdmaRemoteBuffer,
        rhandle: RdmaRemoteBuffer,
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
        lhandle: RdmaRemoteBuffer,
        rhandle: RdmaRemoteBuffer,
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

    /// Polls for work completions by wr_ids.
    ///
    /// # Arguments
    ///
    /// * `target` - Which completion queue to poll (Send, Receive)
    /// * `expected_wr_ids` - Slice of work request IDs to wait for
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<(u64, IbvWc)>)` - Vector of (wr_id, completion) pairs found
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
                        // Not found yet
                    }
                    -17 => {
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

#[cfg(test)]
mod tests {
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

        let domain = domain.unwrap();
        let queue_pair = IbvQueuePair::new(domain.context, domain.pd, config.clone());
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

        let server_domain = IbvDomain::new(server_config.device.clone()).unwrap();
        let client_domain = IbvDomain::new(client_config.device.clone()).unwrap();

        let mut server_qp = IbvQueuePair::new(
            server_domain.context,
            server_domain.pd,
            server_config.clone(),
        )
        .unwrap();
        let mut client_qp = IbvQueuePair::new(
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
