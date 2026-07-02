/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use super::*;
use crate::backend::ibverbs::primitives::IbvPd;

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
    // The source GID (carrying its table index), resolved from the owning
    // device's `IbvDeviceInfo` at construction: index 0 on the EFA path,
    // otherwise the first global RoCE v2 GID on `config.port_num`.
    gid: Gid,
    is_efa: bool,
    // Keepalive for the protection domain this QP was built against, so the PD
    // (and, through it, the context) outlives the QP regardless of other owners
    // (the manager, registered MRs, etc.). Never read directly.
    _pd: Arc<IbvPd>,
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
    pub fn new<I: IbvDomainImpl>(
        domain: &IbvDomain<I>,
        config: IbvConfig,
    ) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating an IbvQueuePair from config {}", config);
        let context = domain.context().as_ptr();
        let pd = domain.as_ptr();
        // Resolve Auto to a concrete QP type based on device capabilities.
        let resolved_qp_type = resolve_qp_type(config.qp_type);
        let is_efa = resolved_qp_type == rdmaxcel_sys::RDMA_QP_TYPE_EFA;
        // EFA is not RoCE, so its `gid_attrs/types` never reports "RoCE v2"; it
        // always uses GID index 0. RoCE selects the first global RoCE v2 GID.
        let gid = if is_efa {
            domain.device_info().gid_at(config.port_num, 0)?
        } else {
            domain.device_info().select_gid(
                config.port_num,
                Some(GidScope::Global),
                Some(GidType::RoCEv2),
            )?
        };
        unsafe {
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
                    gid,
                    is_efa: true,
                    _pd: domain.pd().clone(),
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
                gid,
                is_efa: false,
                _pd: domain.pd().clone(),
            })
        }
    }

    /// Returns the connection info needed by a remote peer to connect to this QP.
    pub fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error> {
        let context = self.context as *mut rdmaxcel_sys::ibv_context;
        let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
        // SAFETY: `(*qp).ibv_qp` is the live `ibv_qp` owned by this `rdmaxcel_qp`,
        // and `context` is its live device context.
        unsafe { super::get_qp_info((*qp).ibv_qp, context, &self.config, self.gid) }
    }

    /// Returns the current state of the QP.
    pub fn state(&mut self) -> Result<u32, anyhow::Error> {
        let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
        // SAFETY: `(*qp).ibv_qp` is the live `ibv_qp` owned by this `rdmaxcel_qp`.
        unsafe { super::state((*qp).ibv_qp) }
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

        let access_flags = (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC)
            .0 as i32;
        let qp = self.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
        // SAFETY: `(*qp).ibv_qp` is the live `ibv_qp` owned by this `rdmaxcel_qp`.
        unsafe {
            super::connect(
                (*qp).ibv_qp,
                &self.config,
                access_flags,
                connection_info,
                self.gid.index(),
            )
        }?;

        tracing::debug!(
            "connection sequence has successfully completed (qp: {:?})",
            qp
        );

        // Record the RTS timestamp so the first posted op can apply the hardware
        // init delay; specific to the legacy `rdmaxcel_qp` doorbell path.
        let rts_timestamp_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        // SAFETY: `qp` is the live `rdmaxcel_qp`.
        unsafe { rdmaxcel_sys::rdmaxcel_qp_store_rts_timestamp(qp, rts_timestamp_nanos) };
        Ok(())
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
                self.gid.index(),
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
                "remote buffer size ({}) is smaller than local buffer size ({})",
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
        let total_size = rhandle.size;
        if rhandle.size > lhandle.size {
            return Err(anyhow::anyhow!(
                "remote buffer size ({}) is larger than local buffer size ({})",
                rhandle.size,
                lhandle.size
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

    /// Polls `target`'s completion queue for a single work completion.
    ///
    /// This is a thin wrapper over one `ibv_poll_cq` call. It does no
    /// per-WR bookkeeping: the caller owns matching each completion back
    /// to the work request it posted (by [`IbvWc::wr_id`]) and must keep
    /// polling until the queue drains so that no completion is lost.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` — the CQ is currently empty.
    /// * `Ok(Some(Ok(wc)))` — a completion landed with success status.
    /// * `Ok(Some(Err(_)))` — a completion landed with a non-success
    ///   status; the [`WorkRequestError`] names the failed request.
    /// * `Err(_)` — `ibv_poll_cq` itself failed; the QP should be treated
    ///   as poisoned.
    pub fn poll_completion(
        &mut self,
        target: PollTarget,
    ) -> Result<Option<Result<IbvWc, WorkRequestError>>, PollCompletionError> {
        unsafe {
            let (cq, cq_type) = match target {
                PollTarget::Send => (self.send_cq as *mut rdmaxcel_sys::ibv_cq, "send"),
                PollTarget::Recv => (self.recv_cq as *mut rdmaxcel_sys::ibv_cq, "recv"),
            };
            let context = self.context as *mut rdmaxcel_sys::ibv_context;
            let poll_cq = (*context)
                .ops
                .poll_cq
                .expect("poll_cq verb missing from ibv_context ops");

            let mut wc = std::mem::MaybeUninit::<rdmaxcel_sys::ibv_wc>::zeroed().assume_init();
            let ret = poll_cq(cq, 1, &mut wc);

            if ret < 0 {
                return Err(PollCompletionError {
                    message: format!("{} CQ poll failed (ibv_poll_cq returned {})", cq_type, ret),
                });
            }
            if ret == 0 {
                return Ok(None);
            }

            // ret >= 1: a single entry was requested, so `wc` holds one
            // completion. `error()` is `Some` exactly when the status is
            // not `IBV_WC_SUCCESS`.
            if let Some((status, vendor_err)) = wc.error() {
                return Ok(Some(Err(WorkRequestError {
                    wr_id: wc.wr_id(),
                    status,
                    vendor_err,
                    message: format!(
                        "{} completion failed for wr_id={}: status={:?}, vendor_err={}",
                        cq_type,
                        wc.wr_id(),
                        status,
                        vendor_err,
                    ),
                })));
            }
            Ok(Some(Ok(IbvWc::from(wc))))
        }
    }
}
