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
use super::domain::IbvDomainImpl;
use super::manager_actor::CreatePeerQueuePair;
use super::memory_region::IbvMemoryRegionView;
use super::primitives::Gid;
use super::primitives::GidScope;
use super::primitives::GidType;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::primitives::IbvCq;
use super::primitives::IbvOperation;
use super::primitives::IbvPd;
use super::primitives::IbvQp;
use super::primitives::IbvQpInfo;
use super::primitives::IbvWc;
use super::primitives::resolve_qp_type;
use crate::RdmaOpType;

/// A per-work-request completion failure: a work request completed with
/// a non-success `ibv_wc_status`. Carries the `wr_id`, status, and vendor
/// error code so callers can correlate the failure to the request and
/// match on the status without string parsing. The QP may or may not be
/// in error state.
#[derive(Debug)]
pub struct WorkRequestError {
    pub wr_id: u64,
    pub status: rdmaxcel_sys::ibv_wc_status::Type,
    pub vendor_err: u32,
    message: String,
}

impl std::fmt::Display for WorkRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WorkRequestError {}

impl WorkRequestError {
    /// Returns `true` when the completion status is `IBV_WC_WR_FLUSH_ERR`,
    /// which typically indicates a secondary failure after the QP entered
    /// error state due to a different work request's failure.
    pub fn is_wr_flush_err(&self) -> bool {
        self.status == rdmaxcel_sys::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
    }

    #[cfg(test)]
    pub(super) fn for_test(wr_id: u64, message: &str) -> Self {
        Self {
            wr_id,
            status: rdmaxcel_sys::ibv_wc_status::IBV_WC_GENERAL_ERR,
            vendor_err: 0,
            message: message.to_string(),
        }
    }
}

/// A CQ-level poll failure from [`IbvQueuePair::poll_completion`]:
/// `ibv_poll_cq` itself failed and the completion queue is no longer
/// usable. The owning QP should be treated as poisoned.
#[derive(Debug)]
pub struct PollCompletionError {
    message: String,
}

impl std::fmt::Display for PollCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PollCompletionError {}

impl PollCompletionError {
    #[cfg(test)]
    pub(super) fn for_test(message: &str) -> Self {
        Self {
            message: message.to_string(),
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

/// The legacy single-type queue pair, retained while the backends migrate onto
/// the [`IbvQueuePair`] trait.
pub mod legacy;

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

/// A NIC-backend queue pair: the unit of RDMA communication between two
/// endpoints, and the operations a [`QueuePairActor`] performs on it. Each
/// [`IbvDomainImpl`] names its concrete queue-pair type via
/// [`IbvDomainImpl::QueuePair`](super::domain::IbvDomainImpl::QueuePair) and builds
/// it through [`Self::new`].
pub trait IbvQueuePair: std::fmt::Debug + Send + Sync + 'static + Sized {
    /// Creates a queue pair against `domain` in the RESET state;
    /// [`Self::connect`] transitions it to RTS before use.
    ///
    /// # Safety
    ///
    /// `domain`'s PD (`domain.as_ptr()`) must be null or a valid protection
    /// domain. Callers must ensure the PD outlives the QP; the easiest way to
    /// do this is for implementers to store the value of `domain` inside the
    /// QP.
    unsafe fn new<I: IbvDomainImpl<QueuePair = Self>>(
        domain: Arc<IbvDomain<I>>,
        config: IbvConfig,
    ) -> Result<Self, anyhow::Error>;

    /// Transitions the QP through `INIT -> RTR -> RTS`, connected to `info`.
    fn connect(&mut self, info: &IbvQpInfo) -> Result<(), anyhow::Error>;

    /// Returns the local endpoint other peers need in order to connect to this
    /// QP.
    fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error>;

    /// Returns the current `ibv_qp_state` of the QP.
    fn state(&mut self) -> Result<u32, anyhow::Error>;

    /// Post an RDMA WRITE of `local_src` into `remote_dst`. The request may
    /// be chunked into multiple WRs. This method returns the list of WR ids
    /// that were posted.
    fn put(
        &mut self,
        remote_dst: IbvBuffer,
        local_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error>;

    /// Post an RDMA READ of `remote_src` into `local_dst`. The request may
    /// be chunked into multiple WRs. This method returns the list of WR ids
    /// that were posted.
    fn get(
        &mut self,
        local_dst: IbvBuffer,
        remote_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error>;

    /// Poll `target`'s completion queue for a single work completion.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` — the CQ is currently empty.
    /// * `Ok(Some(Ok(wc)))` — a completion landed with success status.
    /// * `Ok(Some(Err(_)))` — a completion landed with a non-success
    ///   status; the [`WorkRequestError`] names the failed request.
    /// * `Err(_)` — `ibv_poll_cq` itself failed; the QP should be treated
    ///   as poisoned.
    fn poll_completion(
        &mut self,
        target: PollTarget,
    ) -> Result<Option<Result<IbvWc, WorkRequestError>>, PollCompletionError>;
}

impl IbvQueuePair for legacy::IbvQueuePair {
    unsafe fn new<I: IbvDomainImpl<QueuePair = Self>>(
        domain: Arc<IbvDomain<I>>,
        config: IbvConfig,
    ) -> Result<Self, anyhow::Error> {
        legacy::IbvQueuePair::new(domain, config)
    }

    fn connect(&mut self, info: &IbvQpInfo) -> Result<(), anyhow::Error> {
        legacy::IbvQueuePair::connect(self, info)
    }

    fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error> {
        legacy::IbvQueuePair::get_qp_info(self)
    }

    fn state(&mut self) -> Result<u32, anyhow::Error> {
        legacy::IbvQueuePair::state(self)
    }

    fn put(
        &mut self,
        remote_dst: IbvBuffer,
        local_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        legacy::IbvQueuePair::put(self, local_src, remote_dst)
    }

    fn get(
        &mut self,
        local_dst: IbvBuffer,
        remote_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        legacy::IbvQueuePair::get(self, local_dst, remote_src)
    }

    fn poll_completion(
        &mut self,
        target: PollTarget,
    ) -> Result<Option<Result<IbvWc, WorkRequestError>>, PollCompletionError> {
        legacy::IbvQueuePair::poll_completion(self, target)
    }
}

/// Queries the local endpoint info for `qp`, whose device `context` and the QP
/// `config` (port, PSN) describe the connection. `gid` is the port's source GID.
///
/// # Safety
///
/// `qp` must be a live `ibv_qp` (non-null) and `context` must be its live device
/// context.
pub(super) unsafe fn get_qp_info(
    qp: *mut rdmaxcel_sys::ibv_qp,
    context: *mut rdmaxcel_sys::ibv_context,
    config: &IbvConfig,
    gid: Gid,
) -> Result<IbvQpInfo, anyhow::Error> {
    let mut port_attr = rdmaxcel_sys::ibv_port_attr::default();
    // SAFETY: `context` is a live device context (caller contract); the
    // out-param is a writable, properly aligned `ibv_port_attr`. `ibv_query_port`
    // returns the errno on failure.
    let errno = unsafe {
        rdmaxcel_sys::ibv_query_port(
            context,
            config.port_num,
            &mut port_attr as *mut rdmaxcel_sys::ibv_port_attr as *mut _,
        )
    };
    if errno != 0 {
        return Err(anyhow::anyhow!(
            "failed to query port attributes: {}",
            Error::from_raw_os_error(errno)
        ));
    }

    // SAFETY: `qp` is a live `ibv_qp` (caller contract).
    let qp_num = unsafe { (*qp).qp_num };
    Ok(IbvQpInfo {
        qp_num,
        lid: port_attr.lid,
        gid: Some(gid),
        psn: config.psn,
    })
}

/// Returns the current `ibv_qp_state` of `qp`.
///
/// # Safety
///
/// `qp` must be a live `ibv_qp` (non-null).
unsafe fn state(qp: *mut rdmaxcel_sys::ibv_qp) -> Result<u32, anyhow::Error> {
    let mut qp_attr = rdmaxcel_sys::ibv_qp_attr::default();
    let mut qp_init_attr = rdmaxcel_sys::ibv_qp_init_attr::default();
    let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE;
    // SAFETY: `qp` wraps a live `ibv_qp` (caller contract); the out-params are
    // writable, properly aligned attr structs. `ibv_query_qp` returns the errno.
    let errno =
        unsafe { rdmaxcel_sys::ibv_query_qp(qp, &mut qp_attr, mask.0 as i32, &mut qp_init_attr) };
    if errno != 0 {
        return Err(anyhow::anyhow!(
            "failed to query QP state: {}",
            Error::from_raw_os_error(errno)
        ));
    }
    Ok(qp_attr.qp_state)
}

/// Transitions `qp` through `INIT -> RTR -> RTS`, connected to `info`, granting
/// remote peers `access_flags` and using the connection parameters in `config`.
/// `sgid_index` is the local source-GID table index, used as the address
/// handle's `sgid_index` when `info` carries a (remote) GID.
///
/// # Safety
///
/// `qp` must be a live `ibv_qp` (non-null).
pub(super) unsafe fn connect(
    qp: *mut rdmaxcel_sys::ibv_qp,
    config: &IbvConfig,
    access_flags: i32,
    info: &IbvQpInfo,
    sgid_index: u8,
) -> Result<(), anyhow::Error> {
    // Transition to INIT.
    let mut qp_attr = rdmaxcel_sys::ibv_qp_attr {
        qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_INIT,
        qp_access_flags: access_flags as u32,
        pkey_index: config.pkey_index,
        port_num: config.port_num,
        ..Default::default()
    };
    let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_PORT
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
    // SAFETY: `qp` is a live `ibv_qp` (caller contract); `qp_attr` is a valid
    // `ibv_qp_attr` whose populated fields match `mask`. `ibv_modify_qp` returns
    // the errno.
    let errno = unsafe { rdmaxcel_sys::ibv_modify_qp(qp, &mut qp_attr, mask.0 as i32) };
    if errno != 0 {
        return Err(anyhow::anyhow!(
            "failed to transition QP to INIT: {}",
            Error::from_raw_os_error(errno)
        ));
    }

    // Transition to RTR (Ready to Receive).
    let mut qp_attr = rdmaxcel_sys::ibv_qp_attr {
        qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTR,
        path_mtu: config.path_mtu,
        dest_qp_num: info.qp_num,
        rq_psn: info.psn,
        max_dest_rd_atomic: config.max_dest_rd_atomic,
        min_rnr_timer: config.min_rnr_timer,
        ah_attr: rdmaxcel_sys::ibv_ah_attr {
            dlid: info.lid,
            sl: 0,
            src_path_bits: 0,
            port_num: config.port_num,
            grh: Default::default(),
            ..Default::default()
        },
        ..Default::default()
    };
    if let Some(gid) = info.gid {
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.dgid = rdmaxcel_sys::ibv_gid::from(gid);
        qp_attr.ah_attr.grh.hop_limit = 0xff;
        qp_attr.ah_attr.grh.sgid_index = sgid_index;
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
    // SAFETY: as for the INIT transition above.
    let errno = unsafe { rdmaxcel_sys::ibv_modify_qp(qp, &mut qp_attr, mask.0 as i32) };
    if errno != 0 {
        return Err(anyhow::anyhow!(
            "failed to transition QP to RTR: {}",
            Error::from_raw_os_error(errno)
        ));
    }

    // Transition to RTS (Ready to Send).
    let mut qp_attr = rdmaxcel_sys::ibv_qp_attr {
        qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTS,
        sq_psn: config.psn,
        max_rd_atomic: config.max_rd_atomic,
        retry_cnt: config.retry_cnt,
        rnr_retry: config.rnr_retry,
        timeout: config.qp_timeout,
        ..Default::default()
    };
    let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_TIMEOUT
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_RETRY_CNT
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_SQ_PSN
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_RNR_RETRY
        | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
    // SAFETY: as for the INIT transition above.
    let errno = unsafe { rdmaxcel_sys::ibv_modify_qp(qp, &mut qp_attr, mask.0 as i32) };
    if errno != 0 {
        return Err(anyhow::anyhow!(
            "failed to transition QP to RTS: {}",
            Error::from_raw_os_error(errno)
        ));
    }
    Ok(())
}

/// The owned ibverbs handles backing a queue pair: the queue pair and its two
/// completion queues.
///
/// `qp` is declared before the CQs so it is destroyed first whenever a
/// `QpParts` is dropped — a CQ cannot be destroyed while a queue pair still
/// references it. Because struct fields drop in declaration order, this holds
/// however the value is moved, stored, or unwound; keep the parts bundled in
/// this struct across any fallible step rather than splitting them into
/// separate locals (whose drop order would not be guaranteed).
pub(super) struct QpParts {
    pub(super) qp: IbvQp,
    pub(super) send_cq: IbvCq,
    pub(super) recv_cq: IbvCq,
}

/// An RDMA reliable-connected (RC) queue pair built on plain ibverbs
/// (`ibv_post_send`), independent of any device-specific verbs.
///
/// Single-owner: it owns the QP and its two completion queues and destroys them
/// on drop, so the type is intentionally `!Clone`. Its fields are declared
/// QP-before-CQs (see [`QpParts`]). The device context comes from the held
/// `Arc<IbvContext>`.
#[derive(Debug)]
pub struct RCQueuePair {
    qp: IbvQp,
    send_cq: IbvCq,
    recv_cq: IbvCq,
    /// The device context, used for the data-path verbs.
    context: Arc<IbvContext>,
    config: IbvConfig,
    /// The source GID (carrying its table index), resolved from the owning
    /// device's `IbvDeviceInfo` at construction: the first global RoCE v2 GID on
    /// `config.port_num`.
    gid: Gid,
    /// Remote-access flags granted to peers at connect time, taken from the
    /// owning domain at construction.
    access_flags: i32,
    /// Monotonic work-request id, handed out one per posted WR. Standard
    /// ibverbs carries no internal counter, so the QP tracks its own.
    next_wr_id: u64,
    /// The protection domain this QP was built against, kept alive so the PD
    /// outlives the QP. Never read directly.
    _pd: Arc<IbvPd>,
}

impl RCQueuePair {
    /// Assembles an `RCQueuePair` that owns the already-created queue pair and
    /// completion queues in `parts`. `access_flags` is granted to peers at
    /// [`Self::connect`]; it is taken from the owning domain by the caller.
    ///
    /// # Safety
    ///
    /// `context` must wrap a non-null, live `ibv_context` (the data-path verbs
    /// invoke it without re-checking). `parts.qp` must wrap a live RC `ibv_qp`
    /// created against that context's device and `pd`, with
    /// `parts.send_cq`/`parts.recv_cq` as its completion queues.
    pub(super) unsafe fn from_parts(
        parts: QpParts,
        context: Arc<IbvContext>,
        config: IbvConfig,
        gid: Gid,
        access_flags: i32,
        pd: Arc<IbvPd>,
    ) -> Self {
        let QpParts {
            qp,
            send_cq,
            recv_cq,
        } = parts;
        RCQueuePair {
            qp,
            send_cq,
            recv_cq,
            context,
            config,
            gid,
            access_flags,
            next_wr_id: 0,
            _pd: pd,
        }
    }

    /// Posts `op` over `[laddr, laddr + total_size)` to `[raddr, ...)`,
    /// splitting into `MAX_RDMA_MSG_SIZE`-bound chunks and returning one wr_id
    /// per chunk.
    fn post_chunked(
        &mut self,
        op: IbvOperation,
        laddr: usize,
        lkey: u32,
        raddr: usize,
        rkey: u32,
        total_size: usize,
    ) -> Result<Vec<u64>, anyhow::Error> {
        let mut remaining = total_size;
        let mut offset = 0;
        let mut wr_ids = Vec::new();
        while remaining > 0 {
            let chunk = std::cmp::min(remaining, MAX_RDMA_MSG_SIZE);
            let wr_id = self.next_wr_id;
            self.next_wr_id += 1;
            self.post_one(op, laddr + offset, lkey, chunk, raddr + offset, rkey, wr_id)?;
            wr_ids.push(wr_id);
            remaining -= chunk;
            offset += chunk;
        }
        Ok(wr_ids)
    }

    /// Posts a single signaled RDMA `op` work request via `ibv_post_send`.
    fn post_one(
        &self,
        op: IbvOperation,
        laddr: usize,
        lkey: u32,
        length: usize,
        raddr: usize,
        rkey: u32,
        wr_id: u64,
    ) -> Result<(), anyhow::Error> {
        let qp = self.qp.as_ptr();
        let context = self.context.as_ptr();
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
            opcode: op.into(),
            send_flags: rdmaxcel_sys::ibv_send_flags::IBV_SEND_SIGNALED.0,
            wr: Default::default(),
            qp_type: Default::default(),
            __bindgen_anon_1: Default::default(),
            __bindgen_anon_2: Default::default(),
        };
        // Set the RDMA target. Writing through a union field is safe; the device
        // reads the `rdma` member for the RDMA_WRITE/RDMA_READ opcodes set above.
        wr.wr.rdma.remote_addr = raddr as u64;
        wr.wr.rdma.rkey = rkey;
        let mut bad_wr: *mut rdmaxcel_sys::ibv_send_wr = std::ptr::null_mut();
        // SAFETY: `context` is the QP's live device context, non-null per
        // `from_parts`'s contract; we invoke its `post_send` verb through the ops
        // table. `qp` is live and `wr`/`sge`/`bad_wr` are valid for the duration
        // of the call.
        let errno = unsafe {
            let post_send = (*context)
                .ops
                .post_send
                .expect("post_send verb missing from ibv_context ops");
            post_send(qp, &mut wr as *mut _, &mut bad_wr)
        };
        if errno != 0 {
            return Err(anyhow::anyhow!(
                "failed to post {:?} request: {}",
                op,
                Error::from_raw_os_error(errno)
            ));
        }
        Ok(())
    }
}

impl IbvQueuePair for RCQueuePair {
    unsafe fn new<I: IbvDomainImpl<QueuePair = Self>>(
        domain: Arc<IbvDomain<I>>,
        config: IbvConfig,
    ) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating an RCQueuePair from config {}", config);
        let context = domain.context().clone();
        let context_ptr = context.as_ptr();
        // `IbvDomain`'s `pd` accessor permits null (e.g. a test domain); a real
        // QP needs one, so reject null up front (`IbvCq::create` likewise rejects
        // a null context).
        let pd = domain.as_ptr();
        if pd.is_null() {
            anyhow::bail!("cannot create an RCQueuePair on a null protection domain");
        }

        // Resolve the source GID up front (before allocating any FFI resources),
        // so a port without a global RoCE v2 GID fails cleanly here.
        let gid = domain.device_info().select_gid(
            config.port_num,
            Some(GidScope::Global),
            Some(GidType::RoCEv2),
        )?;

        // Separate send/recv completion queues. Each `IbvCq` destroys its queue
        // on drop, so an early return below (or a panic) cleans them up.
        // SAFETY: `context_ptr`, if non-null, is live (kept alive by `context`);
        // `IbvCq::create` rejects a null context.
        let send_cq = unsafe { IbvCq::create(context_ptr, config.cq_entries) }?;
        // SAFETY: as for `send_cq` above.
        let recv_cq = unsafe { IbvCq::create(context_ptr, config.cq_entries) }?;

        // A standard RC QP with the caps from `config`.
        let mut init_attr = rdmaxcel_sys::ibv_qp_init_attr {
            send_cq: send_cq.as_ptr(),
            recv_cq: recv_cq.as_ptr(),
            cap: rdmaxcel_sys::ibv_qp_cap {
                max_send_wr: config.max_send_wr,
                max_recv_wr: config.max_recv_wr,
                max_send_sge: config.max_send_sge,
                max_recv_sge: config.max_recv_sge,
                max_inline_data: 0,
            },
            qp_type: rdmaxcel_sys::ibv_qp_type::IBV_QPT_RC,
            sq_sig_all: 0,
            ..Default::default()
        };
        // SAFETY: `pd` is non-null (checked above) and live per `IbvDomain`'s
        // construction contract; `init_attr` is a fully initialized
        // `ibv_qp_init_attr`. `ibv_create_qp` returns null on failure.
        let qp = unsafe { rdmaxcel_sys::ibv_create_qp(pd, &mut init_attr) };
        if qp.is_null() {
            // `send_cq`/`recv_cq` drop here, destroying the CQs.
            anyhow::bail!(
                "failed to create queue pair (QP): {}",
                Error::last_os_error()
            );
        }
        // SAFETY: `qp` is a live RC QP just created above; `IbvQp` takes
        // ownership and destroys it on drop.
        let qp = unsafe { IbvQp::from_raw(qp) };
        let parts = QpParts {
            qp,
            send_cq,
            recv_cq,
        };
        let access_flags = domain.access_flags();

        // SAFETY: `parts` wraps a live RC QP just created against `pd`/`context`
        // with its completion queues; ownership transfers to the returned value.
        Ok(unsafe {
            Self::from_parts(
                parts,
                context,
                config,
                gid,
                access_flags,
                domain.pd().clone(),
            )
        })
    }

    fn connect(&mut self, info: &IbvQpInfo) -> Result<(), anyhow::Error> {
        // SAFETY: `self.qp` is the live QP, kept alive for `self`'s lifetime.
        unsafe {
            connect(
                self.qp.as_ptr(),
                &self.config,
                self.access_flags,
                info,
                self.gid.index(),
            )
        }
    }

    fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error> {
        let context = self.context.as_ptr();
        // SAFETY: `self.qp` is the live QP and `context` its non-null device
        // context (validated inside `new`), both valid for `self`'s lifetime.
        unsafe { get_qp_info(self.qp.as_ptr(), context, &self.config, self.gid) }
    }

    fn state(&mut self) -> Result<u32, anyhow::Error> {
        // SAFETY: `self.qp` is the live QP, kept alive for `self`'s lifetime.
        unsafe { state(self.qp.as_ptr()) }
    }

    fn put(
        &mut self,
        remote_dst: IbvBuffer,
        local_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        if remote_dst.size < local_src.size {
            return Err(anyhow::anyhow!(
                "remote buffer size ({}) is smaller than local buffer size ({})",
                remote_dst.size,
                local_src.size
            ));
        }
        self.post_chunked(
            IbvOperation::Write,
            local_src.addr,
            local_src.lkey,
            remote_dst.addr,
            remote_dst.rkey,
            local_src.size,
        )
    }

    fn get(
        &mut self,
        local_dst: IbvBuffer,
        remote_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        if local_dst.size < remote_src.size {
            return Err(anyhow::anyhow!(
                "local buffer size ({}) is smaller than remote buffer size ({})",
                local_dst.size,
                remote_src.size
            ));
        }
        self.post_chunked(
            IbvOperation::Read,
            local_dst.addr,
            local_dst.lkey,
            remote_src.addr,
            remote_src.rkey,
            remote_src.size,
        )
    }

    fn poll_completion(
        &mut self,
        target: PollTarget,
    ) -> Result<Option<Result<IbvWc, WorkRequestError>>, PollCompletionError> {
        let (cq, cq_type) = match target {
            PollTarget::Send => (self.send_cq.as_ptr(), "send"),
            PollTarget::Recv => (self.recv_cq.as_ptr(), "recv"),
        };
        let context = self.context.as_ptr();
        // SAFETY: `context` is non-null (per `from_parts`'s contract) and the
        // QP's live device context; we invoke its `poll_cq` verb through the ops
        // table.
        let poll_cq = unsafe {
            (*context)
                .ops
                .poll_cq
                .expect("poll_cq verb missing from ibv_context ops")
        };
        let mut wc = rdmaxcel_sys::ibv_wc::default();
        // SAFETY: `cq` is a live `ibv_cq` belonging to this QP; `&mut wc` has
        // room for the single entry requested, and `poll_cq` overwrites it
        // whenever it returns a completion (`ret >= 1`).
        let ret = unsafe { poll_cq(cq, 1, &mut wc) };

        if ret < 0 {
            return Err(PollCompletionError {
                message: format!("{} CQ poll failed (ibv_poll_cq returned {})", cq_type, ret),
            });
        }
        if ret == 0 {
            return Ok(None);
        }

        // `ret >= 1`: a single entry was requested, so `wc` holds one completion.
        // `error()` is `Some` exactly when the status is not `IBV_WC_SUCCESS`.
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
pub(super) struct QueuePairActor<M: Manager, Qp: IbvQueuePair> {
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
    /// QP-wide cap on outstanding RDMA-READ WRs at the initiator. A
    /// configured value of 0 means the device imposes no separate read
    /// limit; the constructor normalizes it to `max_send_wr`, so reads
    /// gate only against the send-queue slot cap.
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
    next_op_id: u64,
    in_flight_reads: u32,
    in_flight_writes: u32,
    /// `true` while a `Tick` self-message is already in flight; the
    /// flag prevents stacking redundant ticks.
    tick_armed: bool,
    poll_policy: PollSleepPolicy,
}

impl<M: Manager, Qp: IbvQueuePair> QueuePairActor<M, Qp> {
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
        // A configured max_rd_atomic of 0 means "no separate read
        // limit"; fall back to the send-queue slot cap so reads gate
        // only against max_send_wr.
        let max_rd_atomic = if max_rd_atomic == 0 {
            max_send_wr
        } else {
            max_rd_atomic
        };
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
            RdmaOpType::WriteFromLocal => self.qp.put(op.remote_buffer.clone(), local_buf.clone()),
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

        // 2. Drain the send CQ. Each completion identifies its WR by
        //    `wr_id`; we correlate it back to its op and, once every WR
        //    for an op has reported, emit the op's reply. Polling only
        //    while WRs are outstanding keeps the loop from spinning on an
        //    empty queue, and draining to `None` ensures no completion is
        //    left behind.
        let mut progressed = false;
        while !self.wr_to_op.is_empty() {
            let completion = self
                .qp
                .poll_completion(PollTarget::Send)
                .map_err(|e| anyhow::anyhow!("CQ poll failed for qp_key={:?}: {e}", self.qp_key))?;
            let Some(wc_result) = completion else {
                break;
            };
            progressed = true;

            let (wr_id, wr_error) = match wc_result {
                Ok(wc) => (wc.wr_id(), None),
                Err(per_wr) => (per_wr.wr_id, Some(per_wr.to_string())),
            };

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
            if let Some(err) = wr_error
                && entry.first_error.is_none()
            {
                entry.first_error = Some(err);
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
impl<M: Manager, Qp: IbvQueuePair> Actor for QueuePairActor<M, Qp> {
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

    // This actor is implemented in Rust, but the RDMA registration path may enter
    // Python and take the GIL. Run its loop on the dedicated rdma runtime rather
    // than the shared control-plane runtime; see `crate::rdma_runtime`.
    fn spawn_server_task<F>(future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        crate::rdma_runtime::spawn_on_rdma_runtime(future)
    }
}

#[async_trait]
impl<M: Manager, Qp: IbvQueuePair> Handler<ProcessOps<M>> for QueuePairActor<M, Qp> {
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
impl<M: Manager, Qp: IbvQueuePair> Handler<Tick> for QueuePairActor<M, Qp> {
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
    use crate::backend::ibverbs::device::IbvDevice;
    use crate::backend::ibverbs::device::list_all_devices;
    use crate::backend::ibverbs::device_selection::resolve_target;
    use crate::backend::ibverbs::mlx_device::MlxDevice;
    use crate::backend::ibverbs::primitives::IbvConfig;

    #[test]
    fn test_create_connection() {
        if list_all_devices().is_empty() {
            println!("Skipping test: RDMA devices not available");
            return;
        }

        let config = IbvConfig {
            use_gpu_direct: false,
            ..Default::default()
        };
        let device_info = resolve_target::<MlxDevice>(&config.target).unwrap();
        let mut device = IbvDevice::<MlxDevice>::open(device_info.name(), config.clone())
            .expect("resolved device should open");
        let domain = device
            .get_or_create_domain("test")
            .expect("domain creation should succeed");
        let queue_pair = legacy::IbvQueuePair::new(domain, config.clone());
        assert!(queue_pair.is_ok());
    }

    #[test]
    fn test_loopback_connection() {
        if list_all_devices().is_empty() {
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

        let server_info = resolve_target::<MlxDevice>(&server_config.target).unwrap();
        let mut server_device =
            IbvDevice::<MlxDevice>::open(server_info.name(), server_config.clone())
                .expect("server device should open");
        let server_domain = server_device
            .get_or_create_domain("test")
            .expect("server domain creation should succeed");
        let client_info = resolve_target::<MlxDevice>(&client_config.target).unwrap();
        let mut client_device =
            IbvDevice::<MlxDevice>::open(client_info.name(), client_config.clone())
                .expect("client device should open");
        let client_domain = client_device
            .get_or_create_domain("test")
            .expect("client domain creation should succeed");

        let mut server_qp =
            legacy::IbvQueuePair::new(server_domain, server_config.clone()).unwrap();
        let mut client_qp =
            legacy::IbvQueuePair::new(client_domain, client_config.clone()).unwrap();

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
            remote_dst: IbvBuffer,
            local_src: IbvBuffer,
            wr_ids: Vec<u64>,
        },
        Get {
            local_dst: IbvBuffer,
            remote_src: IbvBuffer,
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
        /// FIFO of completions the next `poll_completion` calls hand
        /// back, one per call. Each entry is either `Ok(IbvWc)`
        /// (success) or `Err(...)` (per-WR completion failure).
        pending_completions: VecDeque<std::result::Result<IbvWc, WorkRequestError>>,
        /// One-shot CQ-level error; cleared after the next poll consumes it.
        poll_error: Option<PollCompletionError>,
        /// One-shot error for the next `put` or `get` call.
        post_error: Option<String>,
    }

    /// `IbvQueuePair` mock used by `QueuePairActor` tests. Cloning is
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

        /// Queue a successful WC for `wr_id`. A subsequent
        /// `poll_completion` call returns it in FIFO order.
        fn queue_completion(&self, wr_id: u64) {
            self.inner
                .lock()
                .unwrap()
                .pending_completions
                .push_back(Ok(IbvWc::for_test(wr_id, true)));
        }

        /// Queue a per-WR completion failure for `wr_id` — the actor
        /// receives it as the inner `Err` from `poll_completion` and
        /// should fail just that op (not poison the QP).
        fn queue_per_wr_error(&self, wr_id: u64, message: &str) {
            self.inner
                .lock()
                .unwrap()
                .pending_completions
                .push_back(Err(WorkRequestError::for_test(wr_id, message)));
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

    impl IbvQueuePair for MockQp {
        unsafe fn new<I: IbvDomainImpl<QueuePair = Self>>(
            _domain: Arc<IbvDomain<I>>,
            _config: IbvConfig,
        ) -> Result<Self> {
            // No `IbvDomainImpl` sets `Q = MockQp`, so this is never reached;
            // the mock is built directly via `MockQp::new`.
            unreachable!("MockQp is constructed directly, not from a domain")
        }

        fn connect(&mut self, info: &IbvQpInfo) -> Result<()> {
            self.inner.lock().unwrap().connect_calls.push(info.clone());
            Ok(())
        }

        fn get_qp_info(&mut self) -> Result<IbvQpInfo> {
            Ok(self.info.clone())
        }

        fn state(&mut self) -> Result<u32> {
            Ok(rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTS)
        }

        fn put(&mut self, remote_dst: IbvBuffer, local_src: IbvBuffer) -> Result<Vec<u64>> {
            let mut inner = self.inner.lock().unwrap();
            if let Some(msg) = inner.post_error.take() {
                return Err(anyhow::anyhow!(msg));
            }
            let wrs = local_src.size.div_ceil(MAX_RDMA_MSG_SIZE).max(1);
            let mut wr_ids = Vec::with_capacity(wrs);
            for _ in 0..wrs {
                wr_ids.push(inner.next_wr_id);
                inner.next_wr_id += 1;
            }
            let _ = inner.posted_tx.send(PostedOp::Put {
                remote_dst,
                local_src,
                wr_ids: wr_ids.clone(),
            });
            Ok(wr_ids)
        }

        fn get(&mut self, local_dst: IbvBuffer, remote_src: IbvBuffer) -> Result<Vec<u64>> {
            let mut inner = self.inner.lock().unwrap();
            if let Some(msg) = inner.post_error.take() {
                return Err(anyhow::anyhow!(msg));
            }
            let wrs = local_dst.size.div_ceil(MAX_RDMA_MSG_SIZE).max(1);
            let mut wr_ids = Vec::with_capacity(wrs);
            for _ in 0..wrs {
                wr_ids.push(inner.next_wr_id);
                inner.next_wr_id += 1;
            }
            let _ = inner.posted_tx.send(PostedOp::Get {
                local_dst,
                remote_src,
                wr_ids: wr_ids.clone(),
            });
            Ok(wr_ids)
        }

        fn poll_completion(
            &mut self,
            _target: PollTarget,
        ) -> std::result::Result<
            Option<std::result::Result<IbvWc, WorkRequestError>>,
            PollCompletionError,
        > {
            let mut inner = self.inner.lock().unwrap();
            if let Some(err) = inner.poll_error.take() {
                return Err(err);
            }
            Ok(inner.pending_completions.pop_front())
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

    use crate::backend::ibverbs::memory_region::IbvMemoryRegion;
    use crate::backend::ibverbs::primitives::IbvPd;
    use crate::local_memory::Keepalive;
    use crate::local_memory::KeepaliveLocalMemory;

    /// A null [`Arc<IbvPd>`] keepalive for [`fake_mrv`]'s region; its `Drop`
    /// deallocates nothing (null `pd`/`context`).
    fn null_pd() -> Arc<IbvPd> {
        Arc::new(IbvPd::null())
    }

    /// No-op [`Keepalive`] for tests that mint a [`KeepaliveLocalMemory`]
    /// from a fake address never actually read or written through.
    struct FakeKeepalive {
        addr: usize,
        size: usize,
    }
    impl Keepalive for FakeKeepalive {
        fn addr(&self) -> usize {
            self.addr
        }
        fn size(&self) -> usize {
            self.size
        }
    }

    fn fake_local_memory(addr: usize, size: usize) -> KeepaliveLocalMemory {
        KeepaliveLocalMemory::new(Arc::new(FakeKeepalive { addr, size }))
    }

    fn fake_mrv(addr: usize, size: usize) -> IbvMemoryRegionView {
        IbvMemoryRegionView::new(
            addr,
            addr,
            size,
            0x1234,
            0x5678,
            "dev0".to_string(),
            Arc::new(IbvMemoryRegion {
                mr: std::ptr::null_mut(),
                _pd: null_pd(),
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
                remote_dst,
                local_src,
                wr_ids,
            } => (local_src, remote_dst, wr_ids),
            other => panic!("expected Put, got {other:?}"),
        }
    }

    fn expect_get(p: PostedOp) -> (IbvBuffer, IbvBuffer, Vec<u64>) {
        match p {
            PostedOp::Get {
                local_dst,
                remote_src,
                wr_ids,
            } => (local_dst, remote_src, wr_ids),
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
            fake_mrv(0x1000, 4096),
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
            fake_mrv(0x1000, 3 * MAX_RDMA_MSG_SIZE),
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
            fake_mrv(0x1000, 3 * MAX_RDMA_MSG_SIZE),
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
                fake_mrv(0x1000, 3 * MAX_RDMA_MSG_SIZE),
            ),
            (
                1usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(0x2000, 4096),
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
                    fake_mrv(0x1000 + i * 0x1000, 4096),
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
    async fn qpa_zero_max_rd_atomic_uses_send_wr() -> Result<()> {
        let harness = QpaHarness::build()?;
        // max_rd_atomic=0 means "no separate read limit"; reads gate
        // only against max_send_wr=2.
        let (actor, qp, mut posted_rx) = harness.spawn_ready_actor(2, 0).await?;

        let items = (0..3usize)
            .map(|i| {
                (
                    i,
                    make_op(RdmaOpType::ReadIntoLocal, 0x1000 + i * 0x1000, 4096),
                    fake_mrv(0x1000 + i * 0x1000, 4096),
                )
            })
            .collect();
        let mut rx = submit_ops(&harness, &actor, items)?;

        // Two reads fit the send-queue cap; the third parks. Were 0
        // taken literally, every read would be rejected as too large.
        let (_, _, r0_wrs) = expect_get(recv_posted(&mut posted_rx).await);
        let (_, _, r1_wrs) = expect_get(recv_posted(&mut posted_rx).await);
        assert_eq!(r0_wrs, vec![0]);
        assert_eq!(r1_wrs, vec![1]);
        assert_no_post(&mut posted_rx, Duration::from_millis(50)).await;

        // Freeing one slot lets the third read post.
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
                    fake_mrv(0x1000 + i * 0x1000, 4096),
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
                fake_mrv(0x1000, 4096),
            ),
            (
                1usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(0x2000, 4096),
            ),
            (
                2usize,
                make_op(RdmaOpType::WriteFromLocal, 0x3000, 4096),
                fake_mrv(0x3000, 4096),
            ),
            (
                3usize,
                make_op(RdmaOpType::WriteFromLocal, 0x4000, 4096),
                fake_mrv(0x4000, 4096),
            ),
            (
                4usize,
                make_op(RdmaOpType::ReadIntoLocal, 0x5000, 2 * MAX_RDMA_MSG_SIZE),
                fake_mrv(0x5000, 2 * MAX_RDMA_MSG_SIZE),
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
                fake_mrv(0x1000, 4096),
            ),
            (
                11usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(0x2000, 4096),
            ),
        ];
        let mut rx_a = submit_ops(&harness, &actor, batch_a)?;

        // Batch B: 2 writes with op_idx 20, 21. Shares the QP with
        // Batch A — together they sit at 4/4 max_send_wr.
        let batch_b = vec![
            (
                20usize,
                make_op(RdmaOpType::WriteFromLocal, 0x3000, 4096),
                fake_mrv(0x3000, 4096),
            ),
            (
                21usize,
                make_op(RdmaOpType::WriteFromLocal, 0x4000, 4096),
                fake_mrv(0x4000, 4096),
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
                fake_mrv(0x1000, 2 * MAX_RDMA_MSG_SIZE),
            ),
            (
                1usize,
                make_op(RdmaOpType::WriteFromLocal, 0x2000, 4096),
                fake_mrv(0x2000, 4096),
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
            fake_mrv(0x1000, 4096),
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
            fake_mrv(0x1000, 4096),
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
