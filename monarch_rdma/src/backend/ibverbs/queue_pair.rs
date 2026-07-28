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

use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::PortHandle;
use hyperactor::actor::Referable;

use super::IbvBuffer;
use super::IbvOp;
use super::device::IbvDeviceImpl;
use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::manager_actor::IbvManagerActor;
use super::memory_region::IbvMemoryRegionView;
use super::primitives::Gid;
use super::primitives::IbvConfig;
use super::primitives::IbvQpInfo;
use super::primitives::IbvWc;

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

/// Specifies which completion queue to poll.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PollTarget {
    Send,
    Recv,
}

/// The legacy single-type queue pair, retained while the backends migrate onto
/// the [`IbvQueuePair`] trait.
pub mod legacy;
/// mlx5 queue pair built on the mlx5dv extended verbs.
pub mod mlx_queue_pair;
/// Reliable-connection queue pair, its `connect` helper, the actor that drives
/// it, and the RC connection manager.
pub mod rc_queue_pair;

/// A NIC-backend queue pair: the unit of RDMA communication between two
/// endpoints, and the low-level operations performed on it. Each backend builds
/// its concrete implementation through [`Self::new`].
pub trait IbvQueuePair: std::fmt::Debug + Send + Sync + 'static + Sized {
    /// Creates a queue pair against `domain` in the RESET state;
    /// [`Self::connect`] transitions it to RTS before use.
    ///
    /// # Safety
    ///
    /// `domain`'s PD (`domain.as_ptr()`) must be null or a valid protection
    /// domain. Callers must ensure the PD outlives the QP; the easiest way to
    /// do this is for implementers to store a clone of `domain.pd()` (an
    /// `Arc<IbvPd>`) inside the QP.
    unsafe fn new<I: IbvDomainImpl>(
        domain: &IbvDomain<I>,
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
/// Per-op completion reply emitted by [`RCQueuePairActor`](rc_queue_pair::RCQueuePairActor)
/// back to the manager via [`ProcessOps::reply`]. A named newtype (rather than a
/// raw `(usize, Result<…>)` tuple) so the manager can identify
/// undeliverable `OpResult`s by type name in
/// `handle_undeliverable_message` and absorb them when the original
/// caller has gone away (typical at test teardown).
#[derive(Debug)]
pub struct OpResult {
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
pub struct ProcessOps<M: Referable> {
    pub(super) items: Vec<(usize, IbvOp<M>, IbvMemoryRegionView)>,
    pub(super) reply: PortHandle<OpResult>,
}

/// Per-backend strategy for establishing and managing queue pair connections to
/// peers. It vends the actor that carries RDMA ops to a peer, and services
/// connection requests initiated by peers. Built against a device's resources
/// (its [`IbvDomain`]), which it borrows to create queue pairs but does not own.
pub trait QueuePairManager: std::fmt::Debug + Send + Sync + 'static {
    /// The device backend this strategy serves.
    type Device: IbvDeviceImpl;

    /// The actor that carries RDMA ops to a peer, accepting a batch as
    /// [`ProcessOps`] and reporting per-op results.
    type QueuePair: Actor + Handler<ProcessOps<IbvManagerActor<Self::Device>>>;

    /// Builds a strategy for a device opened with `config`.
    fn new(config: IbvConfig) -> Self;

    /// Returns the actor that DMAs the peer `peer_ref`/`peer_device` from the
    /// device `domain` lives on, establishing the connection if it does not yet
    /// exist. A peer may be reachable through more than one local device, so the
    /// actor is identified by both `domain`'s device and the peer.
    fn get_or_create_queue_pair(
        &mut self,
        cx: &Context<'_, IbvManagerActor<Self::Device>>,
        domain: &IbvDomain<<Self::Device as IbvDeviceImpl>::Domain>,
        peer_ref: ActorRef<IbvManagerActor<Self::Device>>,
        peer_device: String,
    ) -> anyhow::Result<ActorHandle<Self::QueuePair>>;

    /// Handles a connection request from a peer, returning the local endpoint
    /// the peer needs to complete it. `domain` is the local device's domain.
    fn process_conn_request(
        &mut self,
        domain: &IbvDomain<<Self::Device as IbvDeviceImpl>::Domain>,
        peer_id: ActorId,
        peer_device: String,
        peer_qp_info: &IbvQpInfo,
    ) -> anyhow::Result<IbvQpInfo>;
}
