/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EFA SRD queue pair built on the efadv and extended-verbs work-request
//! builder.

use std::io::Error;
use std::result::Result;
use std::sync::Arc;

use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::memory_region::IbvMemoryRegionView;
use super::memory_region::IbvRemoteMemoryRegionView;
use super::primitives::Gid;
use super::primitives::IbvAh;
use super::primitives::IbvConfig;
use super::primitives::IbvCq;
use super::primitives::IbvQp;
use super::primitives::IbvQpInfo;
use super::primitives::IbvWc;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::PollCompletionError;
use super::queue_pair::PollTarget;
use super::queue_pair::WorkRequestError;

/// Queue key for EFA SRD traffic. Both peers must present the same value or the
/// responder drops the traffic silently; [`IbvQpInfo`] carries no queue key, so
/// this is a shared constant rather than something negotiated at connect time.
const EFA_QKEY: u32 = 0x4242;

/// The RDMA operations an EFA SRD queue pair posts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EfaOp {
    Write,
    Read,
}

/// One work request within a posting session: the byte offset into both the
/// local and the remote buffer, the length, and the id its completion carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Chunk {
    offset: usize,
    len: usize,
    wr_id: u64,
}

/// Splits a `total_size`-byte transfer into `chunk_max`-bound work requests,
/// numbering them from `first_wr_id`. A zero-byte transfer still yields one
/// work request, so every posted operation produces a completion.
fn chunks(total_size: usize, chunk_max: usize, first_wr_id: u64) -> Vec<Chunk> {
    assert!(chunk_max > 0, "chunk size must be positive");
    let mut chunks = Vec::with_capacity(total_size.div_ceil(chunk_max).max(1));
    let mut offset = 0;
    let mut remaining = total_size;
    loop {
        let len = std::cmp::min(remaining, chunk_max);
        let wr_id = first_wr_id + chunks.len() as u64;
        chunks.push(Chunk { offset, len, wr_id });
        offset += len;
        remaining -= len;
        if remaining == 0 {
            break;
        }
    }
    chunks
}

/// The address-handle attributes describing a peer reachable at `dgid`, sent
/// from `port_num` using local GID-table entry `sgid_index`.
///
/// Only the destination GID reaches the EFA device: it resolves the handle from
/// `grh.dgid` alone and returns an opaque routing token. The remaining GRH
/// fields — `hop_limit`, `traffic_class`, `sl` — carry IP header values that
/// matter on RoCE v2 and never reach the wire here, so they stay zero.
/// `sgid_index` must still name a populated GID-table entry, because the kernel
/// validates it whenever the handle is global.
fn ah_attr(port_num: u8, dgid: Gid, sgid_index: u8) -> rdmaxcel_sys::ibv_ah_attr {
    rdmaxcel_sys::ibv_ah_attr {
        port_num,
        is_global: 1,
        grh: rdmaxcel_sys::ibv_global_route {
            dgid: rdmaxcel_sys::ibv_gid::from(dgid),
            sgid_index,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// One RDMA work request to add to a [`WrSession`]. Addresses and lengths are
/// resolved by the caller, so building it cannot fail.
#[derive(Debug, Clone, Copy)]
struct Wr {
    op: EfaOp,
    wr_id: u64,
    laddr: u64,
    lkey: u32,
    raddr: u64,
    rkey: u32,
    len: u32,
}

/// An open work-request session on one queue pair's extended builder, borrowing
/// both that queue pair and the peer it is routed to for as long as the session
/// lives.
///
/// `ibv_wr_start` takes the send queue's lock, which only `ibv_wr_complete` or
/// `ibv_wr_abort` releases. This guard makes that pairing unconditional: the
/// session opens on construction and, if the guard is dropped without
/// [`Self::post`] — an unwind out of the caller, say — `Drop` aborts it. Leaving
/// it open would strand the lock and deadlock every later post on the queue
/// pair, and `ibv_destroy_qp` would then destroy a held lock.
struct WrSession<'a> {
    qpex: *mut rdmaxcel_sys::ibv_qp_ex,
    rdma_write: unsafe extern "C" fn(*mut rdmaxcel_sys::ibv_qp_ex, u32, u64),
    rdma_read: unsafe extern "C" fn(*mut rdmaxcel_sys::ibv_qp_ex, u32, u64),
    set_sge: unsafe extern "C" fn(*mut rdmaxcel_sys::ibv_qp_ex, u32, u64, u32),
    set_ud_addr:
        unsafe extern "C" fn(*mut rdmaxcel_sys::ibv_qp_ex, *mut rdmaxcel_sys::ibv_ah, u32, u32),
    complete: unsafe extern "C" fn(*mut rdmaxcel_sys::ibv_qp_ex) -> std::os::raw::c_int,
    abort: unsafe extern "C" fn(*mut rdmaxcel_sys::ibv_qp_ex),
    /// The queue pair `qpex` points into. Never read: it is held so the borrow
    /// checker keeps the queue pair alive for as long as the builder pointer
    /// derived from it.
    _qp: &'a IbvQp,
    /// The destination every request in this session is routed to. Borrowed, so
    /// the address handle cannot be destroyed while the session is open.
    peer: &'a EfaPeer,
    /// Set once `wr_complete` has run. It releases the lock even when it reports
    /// failure, so past that point [`Drop`] must not abort.
    completed: bool,
}

impl<'a> WrSession<'a> {
    /// Resolves `qp`'s extended work-request builder and opens a session on it,
    /// routed to `peer`.
    ///
    /// Every entry point is resolved before the session opens, so a queue pair
    /// missing one fails here rather than partway through building requests.
    ///
    /// # Safety
    ///
    /// `qp` must hold a non-null, live `ibv_qp`, and `peer`'s address handle must
    /// be a live handle created on the same protection domain as `qp`. Borrowing
    /// `peer` keeps that handle from being destroyed for the life of the session,
    /// but says nothing about which device or domain it addresses.
    unsafe fn start(qp: &'a IbvQp, peer: &'a EfaPeer) -> Result<Self, anyhow::Error> {
        // SAFETY: `qp` holds a live `ibv_qp` (caller contract).
        // `ibv_qp_to_qp_ex` returns null unless the QP was created with
        // `IBV_QP_INIT_ATTR_SEND_OPS_FLAGS`.
        let qpex = unsafe { rdmaxcel_sys::ibv_qp_to_qp_ex(qp.as_ptr()) };
        if qpex.is_null() {
            anyhow::bail!(
                "queue pair has no extended work-request builder; it was not created with IBV_QP_INIT_ATTR_SEND_OPS_FLAGS"
            );
        }

        // SAFETY: `qpex` is non-null (checked above) and points into the live
        // `qp`, so reading its function-pointer fields is sound.
        let ops = unsafe { &*qpex };
        let session = Self {
            qpex,
            rdma_write: ops.wr_rdma_write.ok_or_else(|| missing("wr_rdma_write"))?,
            rdma_read: ops.wr_rdma_read.ok_or_else(|| missing("wr_rdma_read"))?,
            set_sge: ops.wr_set_sge.ok_or_else(|| missing("wr_set_sge"))?,
            set_ud_addr: ops
                .wr_set_ud_addr
                .ok_or_else(|| missing("wr_set_ud_addr"))?,
            complete: ops.wr_complete.ok_or_else(|| missing("wr_complete"))?,
            abort: ops.wr_abort.ok_or_else(|| missing("wr_abort"))?,
            _qp: qp,
            peer,
            completed: false,
        };
        let start = ops.wr_start.ok_or_else(|| missing("wr_start"))?;
        // SAFETY: `qpex` is the live builder resolved above. From here the
        // send-queue lock is held, and `session`'s `Drop` releases it on any
        // path that does not reach `post`.
        unsafe { start(qpex) };
        Ok(session)
    }

    /// Adds one signaled work request to the session.
    fn add(&mut self, wr: Wr) {
        // SAFETY: `self.qpex` is the live builder this open session was started
        // on, and `self.peer`'s address handle is borrowed for the session, so it
        // is still live. Each request is fully specified — `wr_id`/`wr_flags`,
        // then the opcode builder, then the scatter/gather entry, then the
        // destination — before the next one begins.
        unsafe {
            // The builder call below reads both of these, so they are set per
            // request rather than once per session.
            (*self.qpex).wr_id = wr.wr_id;
            (*self.qpex).wr_flags = rdmaxcel_sys::ibv_send_flags::IBV_SEND_SIGNALED.0;
            match wr.op {
                EfaOp::Write => (self.rdma_write)(self.qpex, wr.rkey, wr.raddr),
                EfaOp::Read => (self.rdma_read)(self.qpex, wr.rkey, wr.raddr),
            }
            // Order matters: `wr_set_sge` dispatches on the opcode the builder
            // just wrote.
            (self.set_sge)(self.qpex, wr.lkey, wr.laddr, wr.len);
            (self.set_ud_addr)(
                self.qpex,
                self.peer.ah.as_ptr(),
                self.peer.remote_qpn,
                self.peer.qkey,
            );
        }
    }

    /// Closes the session, posting every request added to it.
    ///
    /// All-or-nothing: on failure the provider rolls the whole session back, so
    /// no completion arrives for any of its requests.
    fn post(mut self) -> Result<(), anyhow::Error> {
        // SAFETY: `self.qpex` is the live builder this open session was started
        // on.
        let errno = unsafe { (self.complete)(self.qpex) };
        // `wr_complete` releases the send-queue lock whether or not it
        // succeeded, so the session is closed either way and `Drop` must not
        // abort it. Nothing may be inserted above this line: `Drop` would then
        // be reachable for a session that is already unlocked.
        self.completed = true;
        if errno != 0 {
            return Err(anyhow::anyhow!(
                "failed to post work-request session: {}",
                Error::from_raw_os_error(errno)
            ));
        }
        Ok(())
    }
}

impl Drop for WrSession<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        // Abandoned without posting — an unwind out of the caller, say. Roll the
        // session back so the send-queue lock is released; leaving it held would
        // deadlock every later post on this queue pair.
        // SAFETY: the session is still open (`completed` is false) and
        // `self.qpex` is the live builder it was started on.
        unsafe { (self.abort)(self.qpex) };
    }
}

fn missing(verb: &str) -> anyhow::Error {
    anyhow::anyhow!("EFA queue pair is missing the {} extended verb", verb)
}

/// Transitions `qp` to the state in `attr`, reporting a failure using
/// the string in `target`.
///
/// # Safety
///
/// `qp` must be a live `ibv_qp` (non-null).
unsafe fn modify_qp(
    qp: *mut rdmaxcel_sys::ibv_qp,
    attr: &mut rdmaxcel_sys::ibv_qp_attr,
    mask: rdmaxcel_sys::ibv_qp_attr_mask,
    target: &str,
) -> Result<(), anyhow::Error> {
    // SAFETY: `qp` is a live `ibv_qp` (caller contract); `attr` is a valid
    // `ibv_qp_attr` whose populated fields match `mask`. `ibv_modify_qp` returns
    // the errno.
    let errno = unsafe { rdmaxcel_sys::ibv_modify_qp(qp, attr, mask.0 as i32) };
    if errno != 0 {
        return Err(anyhow::anyhow!(
            "failed to transition EFA queue pair to {}: {}",
            target,
            Error::from_raw_os_error(errno)
        ));
    }
    Ok(())
}

/// Queries the EFA-specific attributes of the device behind `context`.
///
/// # Safety
///
/// `context` must be a live `ibv_context` belonging to an EFA device.
unsafe fn query_device(
    context: *mut rdmaxcel_sys::ibv_context,
) -> Result<rdmaxcel_sys::efadv_device_attr, anyhow::Error> {
    let mut attr = rdmaxcel_sys::efadv_device_attr::default();
    // SAFETY: `context` is a live EFA device context (caller contract); the
    // out-param is a writable, properly aligned `efadv_device_attr` whose size
    // we pass as `inlen`. `efadv_query_device` returns the errno.
    let errno = unsafe {
        rdmaxcel_sys::efadv_query_device(
            context,
            &mut attr,
            std::mem::size_of::<rdmaxcel_sys::efadv_device_attr>() as u32,
        )
    };
    if errno != 0 {
        return Err(anyhow::anyhow!(
            "failed to query EFA device attributes: {}",
            Error::from_raw_os_error(errno)
        ));
    }
    Ok(attr)
}

/// The remote endpoint of a connected [`EfaQueuePair`].
///
/// EFA SRD is UD-addressed: the destination is not a queue-pair attribute, so
/// every work request names it explicitly through an address handle, the peer's
/// queue-pair number, and the queue key.
#[derive(Debug)]
struct EfaPeer {
    ah: IbvAh,
    remote_qpn: u32,
    qkey: u32,
}

/// An EFA SRD queue pair, created through `efadv_create_qp_ex` and driven by the
/// extended work-request builder reached via `ibv_qp_to_qp_ex`.
///
/// SRD is a driver queue-pair type that the device runs through the unreliable
/// datagram state machine, so this shares almost nothing with the
/// reliable-connected path: the connection handshake carries a queue key rather
/// than access flags, and each work request supplies its own destination. Only
/// endpoint discovery, state queries, and completion polling are common, and
/// those delegate to the shared helpers in [`super::queue_pair`].
///
/// Single-owner: it owns the [`IbvQp`] — which in turn owns its two completion
/// queues and the protection domain — and destroys them on drop, so the type is
/// intentionally `!Clone`.
#[derive(Debug)]
pub struct EfaQueuePair {
    qp: IbvQp,
    /// Declared after `qp` so the queue pair is destroyed first. Every work
    /// request embeds the address handle's device token, so the handle has to
    /// outlive any request still referencing it.
    peer: Option<EfaPeer>,
    config: IbvConfig,
    /// The source GID, always table entry 0: EFA is not RoCE, so its
    /// `gid_attrs/types` never reports "RoCE v2" and its table holds one entry.
    gid: Gid,
    /// Largest transfer issued as a single work request, from the device's
    /// reported `max_rdma_size`.
    max_msg_size: usize,
    /// Monotonic work-request id, handed out one per posted WR. The extended
    /// verbs carry no internal counter, so the queue pair tracks its own.
    next_wr_id: u64,
}

impl EfaQueuePair {
    /// Posts `op` over `total_size` bytes from `laddr` to `raddr` as a single
    /// work-request session, split into [`Self::max_msg_size`]-bound chunks, and
    /// returns one work-request id per chunk.
    ///
    /// The session is all-or-nothing: on failure `ibv_wr_complete` rolls every
    /// request in it back, so an `Err` means nothing was posted and no
    /// completion will arrive for any of the ids.
    fn post_chunked(
        &mut self,
        op: EfaOp,
        laddr: usize,
        lkey: u32,
        raddr: usize,
        rkey: u32,
        total_size: usize,
    ) -> Result<Vec<u64>, anyhow::Error> {
        if self.peer.is_none() {
            anyhow::bail!("cannot post on an EfaQueuePair that has not been connected");
        }

        // Resolve every request before opening the session, so the window where
        // the send-queue lock is held does no arithmetic and no allocation.
        // `WrSession` makes an unwind there recoverable.
        let plan = chunks(total_size, self.max_msg_size, self.next_wr_id);
        self.next_wr_id += plan.len() as u64;
        let wrs: Vec<Wr> = plan
            .iter()
            .map(|chunk| Wr {
                op,
                wr_id: chunk.wr_id,
                laddr: (laddr + chunk.offset) as u64,
                lkey,
                raddr: (raddr + chunk.offset) as u64,
                rkey,
                len: chunk.len as u32,
            })
            .collect();

        let peer = self.peer.as_ref().expect("checked above");
        // SAFETY: `self.qp` holds the live queue pair created in `new`, and
        // `peer`'s address handle is owned by `self` and outlives the session.
        let mut session = unsafe { WrSession::start(&self.qp, peer) }?;
        for wr in wrs {
            session.add(wr);
        }
        session.post().map_err(|e| {
            anyhow::anyhow!("{:?} session of {} work request(s): {e}", op, plan.len())
        })?;
        Ok(plan.into_iter().map(|chunk| chunk.wr_id).collect())
    }
}

impl IbvQueuePair for EfaQueuePair {
    unsafe fn new<I: IbvDomainImpl<QueuePair = Self>>(
        domain: &IbvDomain<I>,
        config: IbvConfig,
        send_cq: Arc<IbvCq>,
        recv_cq: Arc<IbvCq>,
    ) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating an EfaQueuePair from config {}", config);
        // `IbvDomain`'s `pd` accessor permits null (e.g. a test domain); a real
        // queue pair needs one, so reject null up front. Everything below then
        // has a live context too, since a PD is only ever allocated against one.
        let pd = domain.as_ptr();
        if pd.is_null() {
            anyhow::bail!("cannot create an EfaQueuePair on a null protection domain");
        }
        let context = domain.context().as_ptr();

        // Resolve the source GID up front (before allocating any FFI resources),
        // so a port without a usable GID fails cleanly here.
        let gid = domain.device_info().gid_at(config.port_num, 0)?;

        // SAFETY: `context` is the live context the PD above was allocated
        // against.
        let device_attr = unsafe { query_device(context) }?;
        let required = rdmaxcel_sys::EFADV_DEVICE_ATTR_CAPS_RDMA_READ
            | rdmaxcel_sys::EFADV_DEVICE_ATTR_CAPS_RDMA_WRITE;
        anyhow::ensure!(
            device_attr.device_caps & required == required,
            "EFA device does not support both RDMA read and RDMA write (device_caps: {:#x})",
            device_attr.device_caps
        );
        let max_msg_size = device_attr.max_rdma_size as usize;
        anyhow::ensure!(
            max_msg_size > 0,
            "EFA device reports a maximum RDMA transfer size of 0"
        );

        // EFA accepts exactly one scatter/gather entry per RDMA work request.
        // `EfaDevice::apply_config_defaults` already caps these, but the manager
        // seeds those defaults only when it spawns without an explicit config,
        // so enforce it here rather than trust the caller.
        let mut config = config;
        config.max_send_sge = 1;
        config.max_recv_sge = 1;
        // The queue depths must fit the device. Reject rather than clamp: the
        // owning `QueuePairActor` budgets send-queue credits against the depth in
        // its own config, so quietly granting fewer would let it over-commit.
        anyhow::ensure!(
            config.max_send_wr <= device_attr.max_sq_wr,
            "configured max_send_wr ({}) exceeds the EFA device's limit ({})",
            config.max_send_wr,
            device_attr.max_sq_wr
        );
        anyhow::ensure!(
            config.max_recv_wr <= device_attr.max_rq_wr,
            "configured max_recv_wr ({}) exceeds the EFA device's limit ({})",
            config.max_recv_wr,
            device_attr.max_rq_wr
        );

        // An SRD queue pair: a driver queue-pair type selected through
        // `efadv_qp_init_attr`. The send-ops flags both request the RDMA
        // builders and are what makes `ibv_qp_to_qp_ex` yield a builder at all.
        let mut init_attr = rdmaxcel_sys::ibv_qp_init_attr_ex {
            send_cq: send_cq.as_ptr(),
            recv_cq: recv_cq.as_ptr(),
            cap: rdmaxcel_sys::ibv_qp_cap {
                max_send_wr: config.max_send_wr,
                max_recv_wr: config.max_recv_wr,
                max_send_sge: config.max_send_sge,
                max_recv_sge: config.max_recv_sge,
                max_inline_data: 0,
            },
            qp_type: rdmaxcel_sys::ibv_qp_type::IBV_QPT_DRIVER,
            sq_sig_all: 0,
            pd,
            comp_mask: rdmaxcel_sys::IBV_QP_INIT_ATTR_PD
                | rdmaxcel_sys::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS,
            send_ops_flags: (rdmaxcel_sys::IBV_QP_EX_WITH_RDMA_WRITE
                | rdmaxcel_sys::IBV_QP_EX_WITH_RDMA_READ) as u64,
            ..Default::default()
        };
        let mut efa_attr = rdmaxcel_sys::efadv_qp_init_attr {
            driver_qp_type: rdmaxcel_sys::EFADV_QP_DRIVER_TYPE_SRD,
            ..Default::default()
        };
        // `efadv_create_qp_ex` writes the granted queue depths back into
        // `init_attr.cap`, hence the mutable borrow.
        // SAFETY: `context` and `pd` are non-null (checked above) and live; both
        // attr structs are fully initialized and outlive the call, and
        // `init_attr`'s CQ pointers came from the freshly created
        // `send_cq`/`recv_cq`. `efadv_create_qp_ex` returns null on failure.
        let qp = unsafe {
            rdmaxcel_sys::efadv_create_qp_ex(
                context,
                &mut init_attr,
                &mut efa_attr,
                std::mem::size_of::<rdmaxcel_sys::efadv_qp_init_attr>() as u32,
            )
        };
        if qp.is_null() {
            anyhow::bail!(
                "failed to create EFA SRD queue pair (QP): {}",
                Error::last_os_error()
            );
        }

        // SAFETY: `qp` is a live SRD QP just created against `pd` with
        // `send_cq`/`recv_cq`; `IbvQp` holds a clone of each, keeping them alive
        // for at least as long as the QP it destroys on drop.
        let qp = unsafe { IbvQp::from_raw(qp, send_cq, recv_cq, domain.pd().clone()) };

        Ok(Self {
            qp,
            peer: None,
            config,
            gid,
            max_msg_size,
            next_wr_id: 0,
        })
    }

    fn connect(&mut self, info: &IbvQpInfo) -> Result<(), anyhow::Error> {
        let Some(dgid) = info.gid else {
            anyhow::bail!(
                "EFA addresses peers by GID, but the peer endpoint {:?} carries none",
                info
            );
        };

        // Build the address handle before the transitions. It is a
        // protection-domain operation that does not depend on queue-pair state,
        // so failing here leaves the queue pair in RESET rather than in RTS with
        // no route — a state in which every post would fail.
        let mut attr = ah_attr(self.config.port_num, dgid, self.gid.index());
        // SAFETY: the queue pair was created against this PD, which it keeps
        // alive; `attr` is fully initialized and outlives the call.
        let ah = unsafe { IbvAh::create(self.qp.pd().clone(), &mut attr) }?;

        // The device runs an SRD queue pair through the unreliable datagram
        // state machine: INIT carries the queue key in place of access flags,
        // RTR carries nothing but the state, and RTS only the send-queue packet
        // sequence number. None of the reliable-connected path attributes apply,
        // because the destination travels with each work request instead.
        let qp = self.qp.as_ptr();
        let mut attr = rdmaxcel_sys::ibv_qp_attr {
            qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_INIT,
            qkey: EFA_QKEY,
            pkey_index: self.config.pkey_index,
            port_num: self.config.port_num,
            ..Default::default()
        };
        let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE
            | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_PORT
            | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_QKEY;
        // SAFETY: `qp` is the live queue pair, kept alive for `self`'s lifetime.
        unsafe { modify_qp(qp, &mut attr, mask, "INIT") }?;

        let mut attr = rdmaxcel_sys::ibv_qp_attr {
            qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTR,
            ..Default::default()
        };
        let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE;
        // SAFETY: as for the INIT transition above.
        unsafe { modify_qp(qp, &mut attr, mask, "RTR") }?;

        let mut attr = rdmaxcel_sys::ibv_qp_attr {
            qp_state: rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTS,
            sq_psn: self.config.psn,
            ..Default::default()
        };
        let mask = rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_STATE
            | rdmaxcel_sys::ibv_qp_attr_mask::IBV_QP_SQ_PSN;
        // SAFETY: as for the INIT transition above.
        unsafe { modify_qp(qp, &mut attr, mask, "RTS") }?;

        self.peer = Some(EfaPeer {
            ah,
            remote_qpn: info.qp_num,
            qkey: EFA_QKEY,
        });
        tracing::debug!(
            "EfaQueuePair reached RTS and is routed to {:?} (qp: {:?})",
            info,
            qp
        );
        Ok(())
    }

    fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error> {
        let context = self.qp.context().as_ptr();
        // SAFETY: `self.qp` is the live queue pair and `context` its non-null
        // device context (both validated in `new`), valid for `self`'s lifetime.
        unsafe { super::queue_pair::get_qp_info(self.qp.as_ptr(), context, &self.config, self.gid) }
    }

    fn state(&mut self) -> Result<u32, anyhow::Error> {
        // SAFETY: `self.qp` is the live queue pair, kept alive for `self`'s
        // lifetime.
        unsafe { super::queue_pair::state(self.qp.as_ptr()) }
    }

    fn max_msg_size(&self) -> usize {
        self.max_msg_size
    }

    fn put(
        &mut self,
        remote_dst: IbvRemoteMemoryRegionView,
        local_src: IbvMemoryRegionView,
    ) -> Result<Vec<u64>, anyhow::Error> {
        if remote_dst.size < local_src.size {
            return Err(anyhow::anyhow!(
                "remote buffer size ({}) is smaller than local buffer size ({})",
                remote_dst.size,
                local_src.size
            ));
        }
        self.post_chunked(
            EfaOp::Write,
            local_src.rdma_addr,
            local_src.lkey,
            remote_dst.addr,
            remote_dst.rkey,
            local_src.size,
        )
    }

    fn get(
        &mut self,
        local_dst: IbvMemoryRegionView,
        remote_src: IbvRemoteMemoryRegionView,
    ) -> Result<Vec<u64>, anyhow::Error> {
        if local_dst.size < remote_src.size {
            return Err(anyhow::anyhow!(
                "local buffer size ({}) is smaller than remote buffer size ({})",
                local_dst.size,
                remote_src.size
            ));
        }
        self.post_chunked(
            EfaOp::Read,
            local_dst.rdma_addr,
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
        // SAFETY: `self.qp` owns the live queue pair built in `new`, along with
        // its completion queues and device context, all non-null and alive for
        // `self`'s lifetime. `&mut self` excludes another poll through this queue
        // pair, and its lease leaves it the only queue pair polling that
        // completion queue, so no other thread is polling it.
        unsafe { super::queue_pair::poll_one(&self.qp, target) }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;

    // A transfer that fits in one work request still gets exactly one, and it
    // is numbered from the id handed in.
    #[test]
    fn chunks_fitting_transfer_yields_one_wr() {
        assert_eq!(
            chunks(1024, 4096, 7),
            vec![Chunk {
                offset: 0,
                len: 1024,
                wr_id: 7
            }]
        );
    }

    // A zero-byte transfer still yields one work request, so the caller always
    // receives a completion to match against.
    #[test]
    fn chunks_zero_byte_transfer_yields_one_wr() {
        assert_eq!(
            chunks(0, 4096, 0),
            vec![Chunk {
                offset: 0,
                len: 0,
                wr_id: 0
            }]
        );
    }

    // An oversized transfer splits into consecutive, contiguous chunks with a
    // short final one, each carrying the next id.
    #[test]
    fn chunks_oversized_transfer_splits_with_short_tail() {
        let plan = chunks(2500, 1000, 100);
        assert_eq!(
            plan,
            vec![
                Chunk {
                    offset: 0,
                    len: 1000,
                    wr_id: 100
                },
                Chunk {
                    offset: 1000,
                    len: 1000,
                    wr_id: 101
                },
                Chunk {
                    offset: 2000,
                    len: 500,
                    wr_id: 102
                },
            ]
        );
        let total: usize = plan.iter().map(|chunk| chunk.len).sum();
        assert_eq!(total, 2500, "chunks must cover the transfer exactly");
    }

    // An exact multiple of the chunk size produces no trailing empty request.
    #[test]
    fn chunks_exact_multiple_has_no_empty_tail() {
        let plan = chunks(2000, 1000, 0);
        assert_eq!(plan.len(), 2, "2000 bytes at 1000 per WR is two requests");
        assert!(plan.iter().all(|chunk| chunk.len == 1000));
    }

    // The address handle carries the peer's GID and the *local* source-GID
    // index, and leaves the RoCE-only GRH fields zero.
    #[test]
    fn ah_attr_carries_peer_gid_and_local_sgid_index() {
        let peer = Gid::for_test(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 2), 3);
        let attr = ah_attr(1, peer, 0);

        assert_eq!(attr.port_num, 1);
        assert_eq!(attr.is_global, 1, "EFA requires a global address handle");
        // SAFETY: `raw` is one arm of the `ibv_gid` union and is always
        // initialized; reading the 16 address bytes back is sound.
        let dgid = unsafe { attr.grh.dgid.raw };
        assert_eq!(
            dgid,
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 2).octets(),
            "the handle must address the peer's GID"
        );
        assert_eq!(
            attr.grh.sgid_index, 0,
            "sgid_index names the local GID table entry, not the peer's index 3"
        );
        assert_eq!(attr.grh.hop_limit, 0, "hop_limit is RoCE-only");
        assert_eq!(attr.grh.traffic_class, 0, "traffic_class is RoCE-only");
        assert_eq!(attr.sl, 0);
        assert_eq!(attr.dlid, 0, "EFA has no LID");
    }
}
