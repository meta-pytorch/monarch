/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `IbvProcessorActor` — data-path child of `IbvManagerActor`.
//!
//! Owns the peer queue pairs handed off from the manager via
//! [`RequestQueuePair`], caches local MR registrations via
//! [`RegisterMr`], and runs batches of [`IbvOp`]s submitted through
//! [`SubmitOps`]. Generic over a [`Manager`] actor type and a
//! [`QueuePair`] type so unit tests can swap in mocks.

use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use backoff::ExponentialBackoff;
use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::actor::Referable;
use hyperactor::context::Mailbox;
use lru::LruCache;
use tokio::sync::Mutex;
use tokio::sync::OwnedMutexGuard;

use super::IbvBuffer;
use super::IbvOp;
use super::primitives::IbvConfig;
use super::primitives::IbvMemoryRegionView;
use super::primitives::IbvWc;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::MAX_RDMA_MSG_SIZE;
use super::queue_pair::PollCompletionError;
use super::queue_pair::PollTarget;
use super::queue_pair::QpKey;
use crate::RdmaOpType;

/// Data-path operations the processor performs against a queue pair.
/// Implemented on the real `IbvQueuePair` and on `MockQueuePair` in
/// tests.
pub(super) trait QueuePair: std::fmt::Debug + Send + Sync + 'static {
    fn put(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> anyhow::Result<Vec<u64>>;
    fn get(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> anyhow::Result<Vec<u64>>;
    fn poll_completion(
        &mut self,
        target: PollTarget,
        expected_wr_ids: &HashSet<u64>,
    ) -> Result<Vec<(u64, IbvWc)>, PollCompletionError>;
}

impl QueuePair for IbvQueuePair {
    fn put(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> anyhow::Result<Vec<u64>> {
        IbvQueuePair::put(self, lhandle, rhandle)
    }

    fn get(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> anyhow::Result<Vec<u64>> {
        IbvQueuePair::get(self, lhandle, rhandle)
    }

    fn poll_completion(
        &mut self,
        target: PollTarget,
        expected_wr_ids: &HashSet<u64>,
    ) -> Result<Vec<(u64, IbvWc)>, PollCompletionError> {
        IbvQueuePair::poll_completion(self, target, expected_wr_ids)
    }
}

/// Adaptive wait between completion polls.
///
/// While the elapsed time since [`Self::yield_now`] was first called
/// is below `yield_window`, the policy yields cooperatively
/// (`tokio::task::yield_now`) — keeping latency tight when the WR
/// completes shortly after being posted. `tokio::time::sleep` has a
/// minimum resolution of ~1ms (the timer wheel tick), so even a
/// `sleep(Duration::from_micros(100))` would block that long;
/// `yield_now` is sub-millisecond and lets the next poll fire as
/// soon as the runtime schedules us. Past `yield_window` the policy
/// switches to an exponential backoff (1ms initial, doubling, capped
/// at 10ms) so long-running operations don't keep the runtime
/// spinning.
///
/// `yield_window` is read from
/// [`crate::config::RDMA_CQ_BUSY_POLL_WINDOW`]. When it's `None`
/// (the default) the policy disables the cutoff and only ever
/// yields, never sleeps.
pub(super) struct PollSleepPolicy {
    yield_window: Option<Duration>,
    started_at: Option<Instant>,
    backoff: Option<ExponentialBackoff>,
}

impl PollSleepPolicy {
    pub(super) fn new() -> Self {
        let yield_window = hyperactor_config::global::get(crate::config::RDMA_CQ_BUSY_POLL_WINDOW);
        Self {
            yield_window,
            started_at: None,
            backoff: None,
        }
    }

    /// Suspend the current task before the next poll. If no yield
    /// window is configured (the default), always yields. Otherwise,
    /// yields while within the window and then walks an exponential
    /// backoff up to 10ms past it.
    pub(super) async fn yield_now(&mut self) {
        let Some(window) = self.yield_window else {
            tokio::task::yield_now().await;
            return;
        };
        let started = *self.started_at.get_or_insert_with(Instant::now);
        if started.elapsed() < window {
            tokio::task::yield_now().await;
            return;
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
        match backoff.next_backoff() {
            Some(delay) => tokio::time::sleep(delay).await,
            None => tokio::task::yield_now().await,
        }
    }
}

/// Per-op state tracked while WRs are in flight on a queue pair.
/// Holds the [`IbvMemoryRegionView`] so the FFI MR survives until
/// the last WR completes and so error messages can name the MR.
struct PostedOpEntry {
    pending: HashSet<u64>,
    is_read: bool,
    mrv: IbvMemoryRegionView,
}

/// Tracks ops posted to a queue pair and their pending WR ids,
/// honoring `max_send_wr` and `max_rd_atomic`. Generic over
/// [`QueuePair`] so it can be unit-tested with a mock QP.
struct PostedOps<'a, Qp: QueuePair> {
    qp: &'a mut Qp,
    max_send_wr: u32,
    max_rd_atomic: u32,
    /// Running count of in-flight read WRs.
    in_flight_reads: u32,
    /// Running count of in-flight write WRs.
    in_flight_writes: u32,
    /// WR id → op index.
    wr_to_op: HashMap<u64, usize>,
    /// Op index → per-op state.
    op_state: HashMap<usize, PostedOpEntry>,
}

impl<'a, Qp: QueuePair> PostedOps<'a, Qp> {
    fn new(qp: &'a mut Qp, max_send_wr: u32, max_rd_atomic: u32) -> Self {
        Self {
            qp,
            max_send_wr,
            max_rd_atomic,
            in_flight_reads: 0,
            in_flight_writes: 0,
            wr_to_op: HashMap::new(),
            op_state: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.op_state.is_empty()
    }

    /// Try to post `op` to the QP, using `mrv` as the local MR.
    /// Computes the WR count (one per `MAX_RDMA_MSG_SIZE` chunk of
    /// the local buffer — the QP will issue exactly this many WRs).
    /// Returns:
    /// - `Ok(Ok(()))` and posts the op when it fits.
    /// - `Ok(Err((read_excess, slot_excess)))` *without posting*
    ///   when the op would push past `max_rd_atomic` (`read_excess`,
    ///   read completions only) or `max_send_wr` (`slot_excess`,
    ///   completions of any type). The caller should
    ///   [`drain`](Self::drain) at least that much headroom and retry.
    ///   A read completion satisfies both constraints, so the two
    ///   excesses overlap rather than add.
    /// - `Err(_)` if the op alone exceeds what the QP can ever
    ///   handle, or some other failure occurs.
    fn post<M: Referable>(
        &mut self,
        orig_idx: usize,
        op: &IbvOp<M>,
        mrv: &IbvMemoryRegionView,
    ) -> anyhow::Result<Result<(), (u32, u32)>> {
        let local_size = op.local_memory.size();
        let wrs = (local_size.div_ceil(MAX_RDMA_MSG_SIZE).max(1)) as u32;
        let is_read = matches!(op.op_type, RdmaOpType::ReadIntoLocal);
        if wrs > self.max_send_wr {
            return Err(anyhow::anyhow!(
                "op is too large for this QP to handle (would split into {} WRs) [{}]",
                wrs,
                mrv,
            ));
        } else if is_read && wrs > self.max_rd_atomic {
            return Err(anyhow::anyhow!(
                "op is too large for this QP to handle (would split into {} RDMA_READs) [{}]",
                wrs,
                mrv,
            ));
        }
        let projected_total = self.wr_to_op.len() as u32 + wrs;
        let projected_reads = self.in_flight_reads + if is_read { wrs } else { 0 };
        let read_excess = projected_reads.saturating_sub(self.max_rd_atomic);
        let slot_excess = projected_total.saturating_sub(self.max_send_wr);
        if read_excess > 0 || slot_excess > 0 {
            return Ok(Err((read_excess, slot_excess)));
        }
        let local_buf = IbvBuffer {
            mr_id: mrv.id,
            lkey: mrv.lkey,
            rkey: mrv.rkey,
            addr: mrv.rdma_addr,
            size: mrv.size,
            device_name: mrv.device_name.clone(),
        };
        let wr_ids = match op.op_type {
            RdmaOpType::WriteFromLocal => self.qp.put(local_buf, op.remote_buffer.clone())?,
            RdmaOpType::ReadIntoLocal => self.qp.get(local_buf, op.remote_buffer.clone())?,
        };
        let mut pending = HashSet::with_capacity(wr_ids.len());
        for id in &wr_ids {
            self.wr_to_op.insert(*id, orig_idx);
            pending.insert(*id);
        }
        if is_read {
            self.in_flight_reads += wr_ids.len() as u32;
        } else {
            self.in_flight_writes += wr_ids.len() as u32;
        }
        self.op_state.insert(
            orig_idx,
            PostedOpEntry {
                pending,
                is_read,
                mrv: mrv.clone(),
            },
        );
        Ok(Ok(()))
    }

    /// Poll the QP until at least `reads_needed` read completions
    /// have arrived AND the in-flight WR count has dropped by at
    /// least `slots_needed` (counting completions of either type),
    /// or until `deadline` is reached. A read completion counts
    /// toward both targets simultaneously, so the caller can pass
    /// the two excesses returned by [`Self::post`] without
    /// double-counting. Successful ops yield `(orig_idx, Ok(()))`;
    /// on `Err` from `poll_completion` or on timeout the
    /// still-outstanding ops yield `(orig_idx, Err(...))` and `self`
    /// is left empty so callers can post a fresh batch.
    async fn drain(
        &mut self,
        deadline: Instant,
        reads_needed: u32,
        slots_needed: u32,
    ) -> Vec<(usize, Result<(), String>)> {
        let mut results: Vec<(usize, Result<(), String>)> = Vec::new();
        let read_target = self.in_flight_reads.saturating_sub(reads_needed);
        let slot_target =
            (self.in_flight_reads + self.in_flight_writes).saturating_sub(slots_needed);
        let mut poll_policy = PollSleepPolicy::new();
        // Maintained incrementally as completions are processed so
        // we don't reallocate a fresh set on every poll cycle.
        let mut to_poll: HashSet<u64> = self.wr_to_op.keys().copied().collect();
        while self.in_flight_reads > read_target
            || self.in_flight_reads + self.in_flight_writes > slot_target
        {
            if Instant::now() >= deadline {
                tracing::error!(
                    "timed out while waiting on request completion for wr_ids={:?}",
                    self.wr_to_op.keys().copied().collect::<Vec<_>>()
                );
                for (orig_idx, entry) in self.op_state.drain() {
                    results.push((
                        orig_idx,
                        Err(format!(
                            "rdma operation did not complete in time [{}, expected wr_ids={:?}]",
                            entry.mrv,
                            entry.pending.into_iter().collect::<Vec<_>>(),
                        )),
                    ));
                }
                self.wr_to_op.clear();
                self.in_flight_reads = 0;
                self.in_flight_writes = 0;
                return results;
            }
            match self.qp.poll_completion(PollTarget::Send, &to_poll) {
                Ok(completions) => {
                    if completions.is_empty() {
                        poll_policy.yield_now().await;
                        continue;
                    }
                    for (wr_id, _wc) in completions {
                        // TODO: validate `_wc.status == IBV_WC_SUCCESS`
                        // and surface non-success completions as
                        // per-op errors instead of treating every
                        // returned completion as success.
                        to_poll.remove(&wr_id);
                        let orig_idx = self
                            .wr_to_op
                            .remove(&wr_id)
                            .expect("completed wr_id missing from wr_to_op");
                        let entry = self
                            .op_state
                            .get_mut(&orig_idx)
                            .expect("orig_idx missing from op_state");
                        entry.pending.remove(&wr_id);
                        if entry.is_read {
                            self.in_flight_reads -= 1;
                        } else {
                            self.in_flight_writes -= 1;
                        }
                        if entry.pending.is_empty() {
                            self.op_state.remove(&orig_idx);
                            results.push((orig_idx, Ok(())));
                        }
                    }
                }
                Err(e) => {
                    let error_detail = format!("RDMA polling completion failed: {}", e);
                    for (orig_idx, entry) in self.op_state.drain() {
                        results.push((
                            orig_idx,
                            Err(format!(
                                "{} [{}, expected wr_ids={:?}]",
                                error_detail,
                                entry.mrv,
                                entry.pending.into_iter().collect::<Vec<_>>(),
                            )),
                        ));
                    }
                    self.wr_to_op.clear();
                    self.in_flight_reads = 0;
                    self.in_flight_writes = 0;
                    return results;
                }
            }
        }
        results
    }

    /// Drain every in-flight WR by `deadline`.
    async fn drain_all(&mut self, deadline: Instant) -> Vec<(usize, Result<(), String>)> {
        self.drain(
            deadline,
            self.in_flight_reads,
            self.in_flight_reads + self.in_flight_writes,
        )
        .await
    }
}

/// Local-only message: ask the manager to register an MR for
/// `[addr, addr + size)` and return the resulting view.
#[derive(Debug)]
pub(super) struct RegisterMr {
    pub addr: usize,
    pub size: usize,
    pub reply: OncePortHandle<Result<IbvMemoryRegionView, String>>,
}

/// Local-only message: ask the manager to set up (and transfer
/// ownership of) a peer queue pair. The manager hands the QP value
/// across the reply port and forgets it on its own side — the
/// processor becomes the sole owner.
#[derive(Debug)]
pub(super) struct RequestQueuePair<M: Referable, Qp: QueuePair> {
    pub qp_key: QpKey,
    pub remote_manager: ActorRef<M>,
    pub reply: OncePortHandle<Result<Qp, String>>,
}

/// Bundle of trait bounds for an actor type that can serve the
/// processor's slow-path requests. `Referable` is required so the
/// processor can carry `ActorRef<Self>` inside [`RequestQueuePair`].
pub(super) trait Manager<Qp: QueuePair>:
    Actor + Referable + Handler<RegisterMr> + Handler<RequestQueuePair<Self, Qp>>
{
}

impl<T, Qp: QueuePair> Manager<Qp> for T where
    T: Actor + Referable + Handler<RegisterMr> + Handler<RequestQueuePair<Self, Qp>>
{
}

/// Local-only message handled by [`IbvProcessorActor`]: run a batch
/// of ops to completion and reply with per-op results.
#[derive(Debug)]
pub(super) struct SubmitOps<M: Referable> {
    pub ops: Vec<IbvOp<M>>,
    pub timeout: Duration,
    pub reply: OncePortHandle<Vec<Result<(), String>>>,
}

/// Single-purpose child actor that runs the ibverbs data path.
/// Generic over a [`Manager`] (slow path) and [`QueuePair`] (data
/// path) so it can be unit-tested with mocks.
#[derive(Debug)]
pub(super) struct IbvProcessorActor<M: Actor, Qp> {
    manager: ActorHandle<M>,
    config: IbvConfig,
    mr_lru: LruCache<(usize, usize), IbvMemoryRegionView>,
    /// Peer QPs the processor took ownership of via
    /// [`RequestQueuePair`]. Each entry is its own `Arc<Mutex<Qp>>`
    /// so the data path can hand out an
    /// [`OwnedMutexGuard`](tokio::sync::OwnedMutexGuard) per QP and
    /// lock distinct QPs concurrently from the parallel per-QP
    /// futures in `process_batch`.
    peer_qps: HashMap<QpKey, Arc<Mutex<Qp>>>,
}

impl<M, Qp> IbvProcessorActor<M, Qp>
where
    M: Manager<Qp>,
    Qp: QueuePair,
{
    /// `mr_lru_capacity` should be non-zero. A zero value is
    /// clamped to one with a `tracing::warn`; we treat the
    /// degenerate single-entry LRU as the floor rather than
    /// crashing the manager.
    pub(super) fn new(manager: ActorHandle<M>, config: IbvConfig, mr_lru_capacity: usize) -> Self {
        let mr_lru_capacity = NonZeroUsize::new(mr_lru_capacity).unwrap_or_else(|| {
            tracing::warn!(
                "RDMA_MR_LRU_CACHE_SIZE was 0; clamping to 1 (LRU disabled in practice)"
            );
            NonZeroUsize::MIN
        });
        Self {
            manager,
            config,
            mr_lru: LruCache::new(mr_lru_capacity),
            peer_qps: HashMap::new(),
        }
    }

    /// Resolve `(addr, size)` via the LRU cache, asking the manager
    /// on miss.
    async fn resolve_mr(
        &mut self,
        cx: &Context<'_, Self>,
        addr: usize,
        size: usize,
    ) -> Result<IbvMemoryRegionView, String> {
        if let Some(mrv) = self.mr_lru.get(&(addr, size)).cloned() {
            return Ok(mrv);
        }
        let (reply, rx) = cx
            .mailbox()
            .open_once_port::<Result<IbvMemoryRegionView, String>>();
        self.manager
            .send(cx, RegisterMr { addr, size, reply })
            .map_err(|e| {
                format!(
                    "Requesting MR registration from {:?} failed [virtual_addr=0x{:x}, size={}]: {}",
                    self.manager, addr, size, e
                )
            })?;
        let mrv = rx
            .recv()
            .await
            .map_err(|e| {
                format!(
                    "MR registration port closed [virtual_addr=0x{:x}, size={}]: {}",
                    addr, size, e
                )
            })?
            .map_err(|e| {
                format!(
                    "MR registration failed [virtual_addr=0x{:x}, size={}]: {}",
                    addr, size, e
                )
            })?;
        self.mr_lru.put((addr, size), mrv.clone());
        Ok(mrv)
    }

    /// Return an [`OwnedMutexGuard`] over the QP for `qp_key`,
    /// requesting a fresh QP from the manager and inserting it into
    /// `peer_qps` on miss. The QP stays in the map for the lifetime
    /// of the actor; the caller drives the QP through the guard and
    /// the lock releases when the guard drops. Locking is via
    /// `try_lock_owned` because the actor serializes its handlers
    /// and only one `submit_ops` is in flight at a time, so the
    /// guard is always uncontended at acquisition.
    async fn get_or_request_qp(
        &mut self,
        cx: &Context<'_, Self>,
        qp_key: &QpKey,
        remote_manager: ActorRef<M>,
    ) -> Result<OwnedMutexGuard<Qp>, String> {
        if !self.peer_qps.contains_key(qp_key) {
            let (reply, rx) = cx.mailbox().open_once_port::<Result<Qp, String>>();
            self.manager
                .send(
                    cx,
                    RequestQueuePair {
                        qp_key: qp_key.clone(),
                        remote_manager,
                        reply,
                    },
                )
                .map_err(|e| {
                    format!(
                        "Requesting QP from {:?} failed for {} -> {} on {}: {}",
                        self.manager, qp_key.self_device, qp_key.other_id, qp_key.other_device, e
                    )
                })?;
            let qp = rx
                .recv()
                .await
                .map_err(|e| {
                    format!(
                        "QP setup port closed for {} -> {} on {}: {}",
                        qp_key.self_device, qp_key.other_id, qp_key.other_device, e
                    )
                })?
                .map_err(|e| {
                    format!(
                        "QP setup failed for {} -> {} on {}: {}",
                        qp_key.self_device, qp_key.other_id, qp_key.other_device, e
                    )
                })?;
            self.peer_qps
                .insert(qp_key.clone(), Arc::new(Mutex::new(qp)));
        }
        Ok(Arc::clone(self.peer_qps.get(qp_key).expect("just ensured"))
            .try_lock_owned()
            .expect("no other holders"))
    }

    async fn process_batch(
        &mut self,
        cx: &Context<'_, Self>,
        ops: Vec<IbvOp<M>>,
        timeout: Duration,
    ) -> Vec<Result<(), String>> {
        let n = ops.len();
        let mut results: Vec<Result<(), String>> = (0..n)
            .map(|i| Err(format!("op {} not processed (internal bug)", i)))
            .collect();

        // Resolve MRs and group ops by QP key.
        let mut by_qp: HashMap<QpKey, Vec<(usize, IbvOp<M>, IbvMemoryRegionView)>> = HashMap::new();
        for (i, op) in ops.into_iter().enumerate() {
            match self
                .resolve_mr(cx, op.local_memory.addr(), op.local_memory.size())
                .await
            {
                Ok(mrv) => {
                    let qp_key = QpKey {
                        self_device: mrv.device_name.clone(),
                        other_id: op.remote_manager.actor_addr().id().clone(),
                        other_device: op.remote_buffer.device_name.clone(),
                    };
                    by_qp.entry(qp_key).or_default().push((i, op, mrv));
                }
                Err(e) => {
                    results[i] = Err(e);
                }
            }
        }

        // Acquire an `OwnedMutexGuard` per QP and build a per-QP
        // future. The guards don't borrow `self`, so the resulting
        // futures can run concurrently in `join_all`.
        let max_send_wr = self.config.max_send_wr;
        let max_rd_atomic = self.config.max_rd_atomic as u32;
        let mut group_futures = Vec::with_capacity(by_qp.len());
        for (qp_key, group) in by_qp {
            let remote_manager = group[0].1.remote_manager.clone();
            let guard = match self.get_or_request_qp(cx, &qp_key, remote_manager).await {
                Ok(g) => g,
                Err(msg) => {
                    for (orig_idx, _, _) in group {
                        results[orig_idx] = Err(msg.clone());
                    }
                    continue;
                }
            };
            group_futures.push(async move {
                let mut qp = guard;
                process_qp_group(&mut *qp, group, timeout, max_send_wr, max_rd_atomic).await
            });
        }

        for group_results in futures::future::join_all(group_futures).await {
            for (orig_idx, res) in group_results {
                results[orig_idx] = res;
            }
        }

        results
    }
}

/// Run all of `group`'s ops through [`PostedOps`] on `qp`. Pulled
/// out of the actor body so it can be unit-tested directly against
/// a mock QP without spawning the processor.
async fn process_qp_group<M, Qp>(
    qp: &mut Qp,
    group: Vec<(usize, IbvOp<M>, IbvMemoryRegionView)>,
    timeout: Duration,
    max_send_wr: u32,
    max_rd_atomic: u32,
) -> Vec<(usize, Result<(), String>)>
where
    M: Referable,
    Qp: QueuePair,
{
    let mut posted = PostedOps::new(qp, max_send_wr, max_rd_atomic);

    let mut results = Vec::with_capacity(group.len());
    let deadline = Instant::now() + timeout;

    for (orig_idx, op, mrv) in group {
        loop {
            match posted.post(orig_idx, &op, &mrv) {
                Ok(Ok(())) => break,
                Ok(Err((reads_to_drain, slots_to_drain))) => {
                    results.extend(posted.drain(deadline, reads_to_drain, slots_to_drain).await);
                }
                Err(e) => {
                    let verb = match op.op_type {
                        RdmaOpType::WriteFromLocal => "WriteFromLocal",
                        RdmaOpType::ReadIntoLocal => "ReadIntoLocal",
                    };
                    results.push((
                        orig_idx,
                        Err(format!(
                            "post {} failed [local: {}, remote: {:?}]: {}",
                            verb, mrv, op.remote_buffer, e,
                        )),
                    ));
                    break;
                }
            }
        }
    }

    if !posted.is_empty() {
        results.extend(posted.drain_all(deadline).await);
    }

    results
}

#[async_trait]
impl<M, Qp> Actor for IbvProcessorActor<M, Qp>
where
    M: Manager<Qp>,
    Qp: QueuePair,
{
    async fn init(&mut self, _this: &Instance<Self>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<M, Qp> Handler<SubmitOps<M>> for IbvProcessorActor<M, Qp>
where
    M: Manager<Qp>,
    Qp: QueuePair,
{
    async fn handle(&mut self, cx: &Context<Self>, msg: SubmitOps<M>) -> anyhow::Result<()> {
        let SubmitOps {
            ops,
            timeout,
            reply,
        } = msg;
        let results = self.process_batch(cx, ops, timeout).await;
        reply
            .send(cx, results)
            .map_err(|e| anyhow::anyhow!("SubmitOps reply send failed: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use hyperactor::ActorRef;
    use hyperactor::Proc;
    use hyperactor::ProcAddr;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::id::Label;
    use hyperactor::id::ProcId;
    use hyperactor::id::Uid;
    use typeuri::Named;

    use super::super::IbvBuffer;
    use super::super::domain::IbvDomain;
    use super::super::primitives::IbvMemoryRegion;
    use super::*;
    use crate::RdmaOpType;
    use crate::local_memory::RdmaLocalMemory;

    fn null_domain() -> Arc<IbvDomain> {
        Arc::new(IbvDomain {
            context: std::ptr::null_mut(),
            pd: std::ptr::null_mut(),
        })
    }

    // ----------------------------- MockQp -----------------------------

    #[derive(Debug)]
    struct MockQpInner {
        next_wr_id: u64,
        posted_puts: Vec<(IbvBuffer, IbvBuffer, Vec<u64>)>,
        posted_gets: Vec<(IbvBuffer, IbvBuffer, Vec<u64>)>,
        /// Completions waiting to be returned from `poll_completion`,
        /// in order. Each call to `poll_completion` returns all
        /// pending completions whose wr_id is in `expected_wr_ids`.
        pending_completions: VecDeque<(u64, IbvWc)>,
        /// If `Some`, the next `poll_completion` call returns this
        /// error and clears it.
        poll_error: Option<PollCompletionError>,
        /// When `true`, `put`/`get` push completions for every
        /// generated `wr_id` so the QP self-completes.
        auto_complete: bool,
    }

    impl MockQpInner {
        fn new(auto_complete: bool) -> Self {
            Self {
                next_wr_id: 0,
                posted_puts: Vec::new(),
                posted_gets: Vec::new(),
                pending_completions: VecDeque::new(),
                poll_error: None,
                auto_complete,
            }
        }
    }

    #[derive(Debug, Clone)]
    struct MockQp {
        inner: Arc<StdMutex<MockQpInner>>,
    }

    impl MockQp {
        fn new() -> Self {
            Self {
                inner: Arc::new(StdMutex::new(MockQpInner::new(false))),
            }
        }

        fn new_auto_complete() -> Self {
            Self {
                inner: Arc::new(StdMutex::new(MockQpInner::new(true))),
            }
        }

        fn queue_completion(&self, wr_id: u64) {
            self.inner
                .lock()
                .unwrap()
                .pending_completions
                .push_back((wr_id, IbvWc::for_test(wr_id, true)));
        }

        fn queue_poll_error(&self, err: PollCompletionError) {
            self.inner.lock().unwrap().poll_error = Some(err);
        }

        fn posted_put_count(&self) -> usize {
            self.inner.lock().unwrap().posted_puts.len()
        }

        fn posted_get_count(&self) -> usize {
            self.inner.lock().unwrap().posted_gets.len()
        }
    }

    impl QueuePair for MockQp {
        fn put(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> anyhow::Result<Vec<u64>> {
            let mut inner = self.inner.lock().unwrap();
            let wrs = lhandle.size.div_ceil(MAX_RDMA_MSG_SIZE).max(1);
            let mut wr_ids = Vec::with_capacity(wrs);
            for _ in 0..wrs {
                wr_ids.push(inner.next_wr_id);
                inner.next_wr_id += 1;
            }
            if inner.auto_complete {
                for id in &wr_ids {
                    inner
                        .pending_completions
                        .push_back((*id, IbvWc::for_test(*id, true)));
                }
            }
            inner.posted_puts.push((lhandle, rhandle, wr_ids.clone()));
            Ok(wr_ids)
        }

        fn get(&mut self, lhandle: IbvBuffer, rhandle: IbvBuffer) -> anyhow::Result<Vec<u64>> {
            let mut inner = self.inner.lock().unwrap();
            let wrs = lhandle.size.div_ceil(MAX_RDMA_MSG_SIZE).max(1);
            let mut wr_ids = Vec::with_capacity(wrs);
            for _ in 0..wrs {
                wr_ids.push(inner.next_wr_id);
                inner.next_wr_id += 1;
            }
            if inner.auto_complete {
                for id in &wr_ids {
                    inner
                        .pending_completions
                        .push_back((*id, IbvWc::for_test(*id, true)));
                }
            }
            inner.posted_gets.push((lhandle, rhandle, wr_ids.clone()));
            Ok(wr_ids)
        }

        fn poll_completion(
            &mut self,
            _target: PollTarget,
            expected_wr_ids: &HashSet<u64>,
        ) -> Result<Vec<(u64, IbvWc)>, PollCompletionError> {
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

    // -------------------------- MockManagerActor -----------------------

    #[derive(Debug, Default)]
    struct MockManagerInner {
        register_mr_calls: Vec<(usize, usize)>,
        request_qp_calls: Vec<QpKey>,
        next_mr_id: usize,
        /// QPs returned from `RequestQueuePair`. Tests may
        /// pre-populate this map to install a pre-configured
        /// `MockQp` for a given `QpKey` (e.g. one with a queued
        /// `poll_error`); otherwise the handler lazily inserts a
        /// fresh auto-completing `MockQp`.
        qps: HashMap<QpKey, MockQp>,
    }

    #[derive(Debug, Named)]
    struct MockManagerActor {
        inner: Arc<StdMutex<MockManagerInner>>,
    }

    impl Referable for MockManagerActor {}

    #[async_trait]
    impl Actor for MockManagerActor {}

    #[async_trait]
    impl Handler<RegisterMr> for MockManagerActor {
        async fn handle(&mut self, cx: &Context<Self>, msg: RegisterMr) -> anyhow::Result<()> {
            let id = {
                let mut inner = self.inner.lock().unwrap();
                inner.register_mr_calls.push((msg.addr, msg.size));
                let id = inner.next_mr_id;
                inner.next_mr_id += 1;
                id
            };
            let mrv = IbvMemoryRegionView::new(
                id,
                msg.addr,
                msg.addr,
                msg.size,
                0x1234,
                0x5678,
                "dev0".to_string(),
                Arc::new(IbvMemoryRegion::Direct {
                    mr: std::ptr::null_mut(),
                    _domain: null_domain(),
                }),
            );
            msg.reply.send(cx, Ok(mrv))?;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<RequestQueuePair<MockManagerActor, MockQp>> for MockManagerActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: RequestQueuePair<MockManagerActor, MockQp>,
        ) -> anyhow::Result<()> {
            let qp = {
                let mut inner = self.inner.lock().unwrap();
                inner.request_qp_calls.push(msg.qp_key.clone());
                inner
                    .qps
                    .entry(msg.qp_key.clone())
                    .or_insert_with(MockQp::new_auto_complete)
                    .clone()
            };
            msg.reply.send(cx, Ok(qp))?;
            Ok(())
        }
    }

    // ------------------------ IbvOp / mrv helpers ----------------------

    #[derive(Debug)]
    struct FakeLocalMemory {
        addr: usize,
        size: usize,
    }

    impl RdmaLocalMemory for FakeLocalMemory {
        fn addr(&self) -> usize {
            self.addr
        }
        fn size(&self) -> usize {
            self.size
        }
        fn read_at(&self, _offset: usize, _dst: &mut [u8]) -> anyhow::Result<()> {
            unimplemented!("test stub")
        }
        fn write_at(&self, _offset: usize, _src: &[u8]) -> anyhow::Result<()> {
            unimplemented!("test stub")
        }
    }

    fn fake_remote_ref(label: &str, uid: u64) -> ActorRef<MockManagerActor> {
        let proc_id = ProcId::new(Uid::Instance(uid, None), Some(Label::new(label).unwrap()));
        let proc_addr = ProcAddr::new(proc_id, ChannelAddr::Local(1).into());
        ActorRef::attest(proc_addr.actor_addr("remote-mgr"))
    }

    fn fake_buffer(device: &str, addr: usize, size: usize) -> IbvBuffer {
        IbvBuffer {
            mr_id: 1,
            lkey: 0x1234,
            rkey: 0x5678,
            addr,
            size,
            device_name: device.to_string(),
        }
    }

    fn mrv_at(addr: usize, size: usize) -> IbvMemoryRegionView {
        IbvMemoryRegionView::new(
            1,
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

    fn make_op(
        op_type: RdmaOpType,
        addr: usize,
        size: usize,
        remote_device: &str,
        remote_manager: ActorRef<MockManagerActor>,
    ) -> IbvOp<MockManagerActor> {
        IbvOp {
            op_type,
            local_memory: Arc::new(FakeLocalMemory { addr, size }),
            remote_buffer: fake_buffer(remote_device, 0x4000_0000, size),
            remote_manager,
        }
    }

    fn fake_op(addr: usize, size: usize, remote_device: &str) -> IbvOp<MockManagerActor> {
        make_op(
            RdmaOpType::WriteFromLocal,
            addr,
            size,
            remote_device,
            fake_remote_ref("remote-a", 0xabc123),
        )
    }

    fn fake_op_with_remote(
        addr: usize,
        size: usize,
        remote_device: &str,
        remote_manager: ActorRef<MockManagerActor>,
    ) -> IbvOp<MockManagerActor> {
        make_op(
            RdmaOpType::WriteFromLocal,
            addr,
            size,
            remote_device,
            remote_manager,
        )
    }

    // --------------------------- PostedOps tests -----------------------

    fn post_write(
        posted: &mut PostedOps<'_, MockQp>,
        idx: usize,
        size: usize,
    ) -> anyhow::Result<Result<(), (u32, u32)>> {
        let op = make_op(
            RdmaOpType::WriteFromLocal,
            0x1000_0000,
            size,
            "dev0",
            fake_remote_ref("remote-a", 0xabc123),
        );
        let mrv = mrv_at(0x1000_0000, size);
        posted.post(idx, &op, &mrv)
    }

    fn post_read(
        posted: &mut PostedOps<'_, MockQp>,
        idx: usize,
        size: usize,
    ) -> anyhow::Result<Result<(), (u32, u32)>> {
        let op = make_op(
            RdmaOpType::ReadIntoLocal,
            0x1000_0000,
            size,
            "dev0",
            fake_remote_ref("remote-a", 0xabc123),
        );
        let mrv = mrv_at(0x1000_0000, size);
        posted.post(idx, &op, &mrv)
    }

    #[tokio::test]
    async fn posted_ops_empty_drain_is_noop() {
        let mut qp = MockQp::new();
        let mut posted = PostedOps::new(&mut qp, 4, 2);
        assert!(posted.is_empty());
        let res = posted
            .drain(Instant::now() + Duration::from_secs(1), 0, 0)
            .await;
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_post_within_limits_succeeds() {
        let mut qp = MockQp::new();
        let mut posted = PostedOps::new(&mut qp, 4, 2);
        let r = post_write(&mut posted, 0, 4096).expect("post should succeed");
        assert_eq!(r, Ok(()), "op within limits should report no excess");
        assert!(!posted.is_empty());
        assert_eq!(qp.posted_put_count(), 1);
    }

    #[tokio::test]
    async fn posted_ops_post_signals_read_excess() {
        let mut qp = MockQp::new();
        let mut posted = PostedOps::new(&mut qp, 4, 2);
        // Two reads fill `max_rd_atomic`.
        assert_eq!(post_read(&mut posted, 0, 4096).unwrap(), Ok(()));
        assert_eq!(post_read(&mut posted, 1, 4096).unwrap(), Ok(()));
        // Third read should report 1 read in excess and not post.
        assert_eq!(post_read(&mut posted, 2, 4096).unwrap(), Err((1, 0)));
        assert_eq!(qp.posted_get_count(), 2);
    }

    #[tokio::test]
    async fn posted_ops_post_signals_write_excess() {
        let mut qp = MockQp::new();
        let mut posted = PostedOps::new(&mut qp, 4, 2);
        for i in 0..4usize {
            assert_eq!(post_write(&mut posted, i, 4096).unwrap(), Ok(()));
        }
        // Fifth write should report 1 write in excess.
        assert_eq!(post_write(&mut posted, 4, 4096).unwrap(), Err((0, 1)));
        assert_eq!(qp.posted_put_count(), 4);
    }

    #[tokio::test]
    async fn posted_ops_post_signals_both_excesses() {
        let mut qp = MockQp::new();
        let mut posted = PostedOps::new(&mut qp, 8, 2);
        // 7 writes + 1 single-WR read fill the QP at 8/8 send_wr and
        // 1/2 max_rd_atomic.
        for i in 0..7usize {
            assert_eq!(post_write(&mut posted, i, 4096).unwrap(), Ok(()));
        }
        assert_eq!(post_read(&mut posted, 7, 4096).unwrap(), Ok(()));
        // A 2-WR read on top would push reads to 3/2 (1 over
        // max_rd_atomic) and total to 10/8 (2 over max_send_wr).
        // The drain must free 1 read slot and 2 total slots;
        // draining the 1 read counts toward both targets.
        assert_eq!(
            post_read(&mut posted, 8, 2 * MAX_RDMA_MSG_SIZE).unwrap(),
            Err((1, 2)),
        );
        assert_eq!(qp.posted_put_count(), 7);
        assert_eq!(qp.posted_get_count(), 1);
    }

    /// A write that needs only a `max_send_wr` slot when the QP is
    /// full of reads must drain a read to make progress: `drain`
    /// with `slots_needed=1` has to count a read completion against
    /// the total-slots target, not look for a write-specific
    /// completion.
    #[tokio::test]
    async fn posted_ops_drain_frees_slot_for_write_when_reads_fill_qp() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 4, 4);
        for i in 0..4usize {
            assert_eq!(post_read(&mut posted, i, 4096).unwrap(), Ok(()));
        }
        assert_eq!(post_write(&mut posted, 4, 4096).unwrap(), Err((0, 1)));
        qp_ctl.queue_completion(0);
        let drained = posted
            .drain(Instant::now() + Duration::from_secs(5), 0, 1)
            .await;
        assert_eq!(drained, vec![(0, Ok(()))]);
        assert_eq!(post_write(&mut posted, 4, 4096).unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn posted_ops_empty_op_uses_one_wr() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 4, 2);
        // A 0-byte op should still produce exactly one WR.
        assert_eq!(post_write(&mut posted, 0, 0).unwrap(), Ok(()));
        assert_eq!(
            qp_ctl.inner.lock().unwrap().posted_puts[0].2,
            vec![0u64],
            "0-byte op should produce exactly wr_id 0",
        );
        qp_ctl.queue_completion(0);
        let results = posted
            .drain_all(Instant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(results, vec![(0, Ok(()))]);
        assert!(posted.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_chunked_op_succeeds_when_all_wrs_complete() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 4, 4);
        // Single op chunked into 3 WRs (wr_ids 0, 1, 2).
        assert_eq!(
            post_write(&mut posted, 0, 3 * MAX_RDMA_MSG_SIZE).unwrap(),
            Ok(()),
        );
        for id in [0u64, 1, 2] {
            qp_ctl.queue_completion(id);
        }
        let results = posted
            .drain_all(Instant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(results, vec![(0, Ok(()))]);
        assert!(posted.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_chunked_op_blocks_until_last_wr() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 4, 4);
        // 3-WR op (wr_ids 0, 1, 2); only 2 of 3 complete.
        assert_eq!(
            post_write(&mut posted, 0, 3 * MAX_RDMA_MSG_SIZE).unwrap(),
            Ok(()),
        );
        qp_ctl.queue_completion(0);
        qp_ctl.queue_completion(1);
        let results = posted
            .drain_all(Instant::now() + Duration::from_millis(50))
            .await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
        let err = results[0].1.as_ref().unwrap_err();
        assert!(
            err.contains("did not complete in time"),
            "chunked op should time out when 1 WR is still outstanding: {}",
            err
        );
        assert!(posted.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_drain_completes_in_flight_ops() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 8, 4);
        // 4 ops: 2 reads (wr_ids 0, 1) then 2 writes (wr_ids 2, 3).
        assert_eq!(post_read(&mut posted, 0, 4096).unwrap(), Ok(()));
        assert_eq!(post_read(&mut posted, 1, 4096).unwrap(), Ok(()));
        assert_eq!(post_write(&mut posted, 2, 4096).unwrap(), Ok(()));
        assert_eq!(post_write(&mut posted, 3, 4096).unwrap(), Ok(()));
        for id in [0u64, 1, 2, 3] {
            qp_ctl.queue_completion(id);
        }
        let results = posted
            .drain(Instant::now() + Duration::from_secs(5), 2, 2)
            .await;
        let mut by_idx: Vec<(usize, Result<(), String>)> = results;
        by_idx.sort_by_key(|(i, _)| *i);
        assert_eq!(
            by_idx,
            vec![(0, Ok(())), (1, Ok(())), (2, Ok(())), (3, Ok(()))],
        );
        assert!(posted.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_drain_partial_leaves_remaining() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 8, 4);
        // 4 reads (wr_ids 0, 1, 2, 3).
        for i in 0..4usize {
            assert_eq!(post_read(&mut posted, i, 4096).unwrap(), Ok(()));
        }
        // Queue completions for wr_ids 0 and 1 only; drain just 2.
        qp_ctl.queue_completion(0);
        qp_ctl.queue_completion(1);
        let first = posted
            .drain(Instant::now() + Duration::from_secs(5), 2, 0)
            .await;
        let mut by_idx: Vec<(usize, Result<(), String>)> = first;
        by_idx.sort_by_key(|(i, _)| *i);
        assert_eq!(by_idx, vec![(0, Ok(())), (1, Ok(()))]);
        assert!(!posted.is_empty(), "ops 2 and 3 are still in flight");
        // Queue the remaining completions; a follow-up drain picks
        // them up and clears `posted`.
        qp_ctl.queue_completion(2);
        qp_ctl.queue_completion(3);
        let mut second = posted
            .drain_all(Instant::now() + Duration::from_secs(5))
            .await;
        second.sort_by_key(|(i, _)| *i);
        assert_eq!(second, vec![(2, Ok(())), (3, Ok(()))]);
        assert!(posted.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_drain_all_finishes_in_flight() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 8, 4);
        // 3 ops: 2 writes (wr_ids 0, 1) and 1 read (wr_id 2).
        assert_eq!(post_write(&mut posted, 0, 4096).unwrap(), Ok(()));
        assert_eq!(post_write(&mut posted, 1, 4096).unwrap(), Ok(()));
        assert_eq!(post_read(&mut posted, 2, 4096).unwrap(), Ok(()));
        for id in [0u64, 1, 2] {
            qp_ctl.queue_completion(id);
        }
        let results = posted
            .drain_all(Instant::now() + Duration::from_secs(5))
            .await;
        let mut by_idx: Vec<(usize, Result<(), String>)> = results;
        by_idx.sort_by_key(|(i, _)| *i);
        assert_eq!(by_idx, vec![(0, Ok(())), (1, Ok(())), (2, Ok(()))]);
        assert!(posted.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_drain_surfaces_poll_error() {
        let mut qp = MockQp::new();
        let qp_ctl = qp.clone();
        let mut posted = PostedOps::new(&mut qp, 8, 4);
        // 3 ops: every one should surface the error.
        assert_eq!(post_write(&mut posted, 0, 4096).unwrap(), Ok(()));
        assert_eq!(post_write(&mut posted, 1, 4096).unwrap(), Ok(()));
        assert_eq!(post_read(&mut posted, 2, 4096).unwrap(), Ok(()));
        qp_ctl.queue_poll_error(PollCompletionError::for_test("boom"));
        let mut results = posted
            .drain_all(Instant::now() + Duration::from_secs(5))
            .await;
        results.sort_by_key(|(i, _)| *i);
        assert_eq!(results.len(), 3);
        for (idx, (orig_idx, res)) in results.iter().enumerate() {
            assert_eq!(*orig_idx, idx);
            let err = res.as_ref().expect_err(&format!("op {} expected Err", idx));
            assert!(
                err.contains("boom"),
                "op {} missing poll error: {}",
                idx,
                err
            );
        }
        assert!(posted.is_empty());
    }

    #[tokio::test]
    async fn posted_ops_drain_times_out() {
        let mut qp = MockQp::new();
        let mut posted = PostedOps::new(&mut qp, 8, 4);
        // 3 ops; no completions queued.
        assert_eq!(post_write(&mut posted, 0, 4096).unwrap(), Ok(()));
        assert_eq!(post_read(&mut posted, 1, 4096).unwrap(), Ok(()));
        assert_eq!(post_write(&mut posted, 2, 4096).unwrap(), Ok(()));
        let mut results = posted
            .drain_all(Instant::now() + Duration::from_millis(50))
            .await;
        results.sort_by_key(|(i, _)| *i);
        assert_eq!(results.len(), 3);
        for (idx, (orig_idx, res)) in results.iter().enumerate() {
            assert_eq!(*orig_idx, idx);
            let err = res.as_ref().expect_err(&format!("op {} expected Err", idx));
            assert!(
                err.contains("did not complete in time"),
                "op {} missing timeout message: {}",
                idx,
                err
            );
        }
        assert!(posted.is_empty());
    }

    // ---------------------- IbvProcessorActor tests --------------------

    struct Harness {
        client: hyperactor::Instance<()>,
        processor: ActorHandle<IbvProcessorActor<MockManagerActor, MockQp>>,
        mgr_inner: Arc<StdMutex<MockManagerInner>>,
    }

    async fn setup_with_lru(lru_capacity: usize) -> Harness {
        let proc = Proc::new();
        let (client, _) = proc.instance("client").unwrap();
        let mgr_inner = Arc::new(StdMutex::new(MockManagerInner::default()));
        let mgr = MockManagerActor {
            inner: Arc::clone(&mgr_inner),
        };
        let mgr_handle = proc.spawn::<MockManagerActor>("mgr", mgr).unwrap();
        let processor = IbvProcessorActor::<MockManagerActor, MockQp>::new(
            mgr_handle,
            super::super::primitives::IbvConfig::default(),
            lru_capacity,
        );
        let processor_handle = proc
            .spawn::<IbvProcessorActor<MockManagerActor, MockQp>>("processor", processor)
            .unwrap();
        Harness {
            client,
            processor: processor_handle,
            mgr_inner,
        }
    }

    async fn setup() -> Harness {
        setup_with_lru(1024).await
    }

    async fn submit(
        harness: &Harness,
        ops: Vec<IbvOp<MockManagerActor>>,
    ) -> Vec<Result<(), String>> {
        let (reply, rx) = harness.client.mailbox().open_once_port();
        harness
            .processor
            .send(
                &harness.client,
                SubmitOps {
                    ops,
                    timeout: Duration::from_secs(5),
                    reply,
                },
            )
            .unwrap();
        rx.recv().await.unwrap()
    }

    #[tokio::test]
    async fn submit_ops_returns_ok_for_each_op() {
        let h = setup().await;
        let results = submit(&h, vec![fake_op(0x1000, 4096, "dev0")]).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok(), "op should complete: {:?}", results[0]);
    }

    #[tokio::test]
    async fn submit_ops_caches_mr_lru_across_submits() {
        let h = setup().await;
        // Submit the same op twice. The second submit hits the LRU
        // and skips the manager round-trip.
        let _ = submit(&h, vec![fake_op(0x1000, 4096, "dev_remote_a")]).await;
        let _ = submit(&h, vec![fake_op(0x1000, 4096, "dev_remote_a")]).await;
        assert_eq!(
            h.mgr_inner.lock().unwrap().register_mr_calls,
            vec![(0x1000, 4096)],
            "second submit should hit the LRU cache",
        );
    }

    #[tokio::test]
    async fn submit_ops_reuses_qp_across_calls() {
        let h = setup().await;
        let _ = submit(&h, vec![fake_op(0x1000, 4096, "dev_remote_a")]).await;
        let _ = submit(&h, vec![fake_op(0x1000, 4096, "dev_remote_a")]).await;
        assert_eq!(
            h.mgr_inner.lock().unwrap().request_qp_calls.len(),
            1,
            "second submit should reuse the cached peer QP",
        );
    }

    #[tokio::test]
    async fn submit_ops_dispatches_one_request_per_distinct_qp_key() {
        let h = setup().await;
        // Cover all three `QpKey` axes in a single batch:
        // - two ops to the same key (dedup);
        // - one op with a distinct `other_device`;
        // - one op with a distinct `other_id` (remote manager).
        // The mock manager always returns "dev0" as the local
        // device, so `self_device` is constant.
        let remote_a = fake_remote_ref("remote-a", 0xa1);
        let remote_b = fake_remote_ref("remote-b", 0xb2);
        let other_a = remote_a.actor_addr().id().clone();
        let other_b = remote_b.actor_addr().id().clone();
        let ops = vec![
            fake_op_with_remote(0x1000, 4096, "dev_x", remote_a.clone()),
            fake_op_with_remote(0x2000, 4096, "dev_x", remote_a.clone()),
            fake_op_with_remote(0x3000, 4096, "dev_y", remote_a.clone()),
            fake_op_with_remote(0x4000, 4096, "dev_x", remote_b.clone()),
        ];
        let results = submit(&h, ops).await;
        assert!(results.iter().all(|r| r.is_ok()));
        let inner = h.mgr_inner.lock().unwrap();
        let got: HashSet<QpKey> = inner.request_qp_calls.iter().cloned().collect();
        let expected: HashSet<QpKey> = [
            QpKey {
                self_device: "dev0".into(),
                other_id: other_a.clone(),
                other_device: "dev_x".into(),
            },
            QpKey {
                self_device: "dev0".into(),
                other_id: other_a,
                other_device: "dev_y".into(),
            },
            QpKey {
                self_device: "dev0".into(),
                other_id: other_b,
                other_device: "dev_x".into(),
            },
        ]
        .into_iter()
        .collect();
        assert_eq!(inner.request_qp_calls.len(), 3, "no duplicate requests");
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn submit_ops_mr_lru_eviction_re_requests() {
        // LRU of size 2: registering a third distinct `(addr, size)`
        // evicts the least-recently-used entry. A subsequent op for
        // the evicted MR has to round-trip the manager again.
        let h = setup_with_lru(2).await;
        let _ = submit(
            &h,
            vec![
                fake_op(0x1000, 4096, "dev0"),
                fake_op(0x2000, 4096, "dev0"),
                // Third entry evicts (0x1000, 4096) since the
                // earlier ops are touched in order.
                fake_op(0x3000, 4096, "dev0"),
            ],
        )
        .await;
        // (0x1000, 4096) is no longer in the cache. Requesting it
        // again triggers a fresh register_mr call.
        let _ = submit(&h, vec![fake_op(0x1000, 4096, "dev0")]).await;
        assert_eq!(
            h.mgr_inner.lock().unwrap().register_mr_calls,
            vec![
                (0x1000, 4096),
                (0x2000, 4096),
                (0x3000, 4096),
                (0x1000, 4096),
            ],
        );
    }

    // ------------------- IbvProcessorActor + PostedOps -----------------

    /// End-to-end: the processor groups ops by QP key, locks each
    /// peer QP, and runs the group through `PostedOps`. Verify that
    /// each peer QP sees exactly the local/remote buffers from its
    /// own group (and not any other group's).
    #[tokio::test]
    async fn submit_ops_funnels_buffers_to_correct_qps() {
        let h = setup().await;
        let remote_a = fake_remote_ref("remote-a", 0xa1);
        let remote_b = fake_remote_ref("remote-b", 0xb2);
        let ops = vec![
            // QP_A_x: two ops.
            fake_op_with_remote(0x1000, 4096, "dev_x", remote_a.clone()),
            fake_op_with_remote(0x2000, 4096, "dev_x", remote_a.clone()),
            // QP_A_y: one op.
            fake_op_with_remote(0x3000, 4096, "dev_y", remote_a.clone()),
            // QP_B_x: one op.
            fake_op_with_remote(0x4000, 4096, "dev_x", remote_b.clone()),
        ];
        let results = submit(&h, ops).await;
        assert!(results.iter().all(|r| r.is_ok()));

        let inner = h.mgr_inner.lock().unwrap();
        let qp_key = |other_id, other_device: &str| QpKey {
            self_device: "dev0".into(),
            other_id,
            other_device: other_device.into(),
        };
        let id_a = remote_a.actor_addr().id().clone();
        let id_b = remote_b.actor_addr().id().clone();

        // Within a single QP, ops are processed serially in the
        // order they appear in the batch — `posted_puts` is a Vec
        // recording that order.
        let posted_addrs = |key: &QpKey| -> Vec<(usize, usize)> {
            inner.qps[key]
                .inner
                .lock()
                .unwrap()
                .posted_puts
                .iter()
                .map(|(local, remote, _)| (local.addr, remote.addr))
                .collect()
        };

        assert_eq!(
            posted_addrs(&qp_key(id_a.clone(), "dev_x")),
            vec![(0x1000, 0x4000_0000), (0x2000, 0x4000_0000)],
        );
        assert_eq!(
            posted_addrs(&qp_key(id_a, "dev_y")),
            vec![(0x3000, 0x4000_0000)],
        );
        assert_eq!(
            posted_addrs(&qp_key(id_b, "dev_x")),
            vec![(0x4000, 0x4000_0000)],
        );
    }

    /// End-to-end: a pre-seeded `MockQp` with a queued poll error
    /// fails its group's op, while a sibling group on a healthy QP
    /// still succeeds.
    #[tokio::test]
    async fn submit_ops_propagates_per_qp_poll_errors() {
        let h = setup().await;
        let remote = fake_remote_ref("remote-a", 0xa1);
        let other_id = remote.actor_addr().id().clone();
        let bad_key = QpKey {
            self_device: "dev0".into(),
            other_id,
            other_device: "dev_bad".into(),
        };
        // Seed a non-auto-completing QP that errors on the first
        // poll; the processor will receive it from
        // `RequestQueuePair` instead of creating a fresh one.
        let bad_qp = MockQp::new();
        bad_qp.queue_poll_error(PollCompletionError::for_test("simulated-poll-error"));
        h.mgr_inner.lock().unwrap().qps.insert(bad_key, bad_qp);

        let results = submit(
            &h,
            vec![
                fake_op_with_remote(0x1000, 4096, "dev_good", remote.clone()),
                fake_op_with_remote(0x2000, 4096, "dev_bad", remote.clone()),
            ],
        )
        .await;
        assert_eq!(results.len(), 2);
        assert!(
            results[0].is_ok(),
            "op 0 (healthy QP) should succeed: {:?}",
            results[0]
        );
        let err = results[1].as_ref().expect_err("op 1 (bad QP) should fail");
        assert!(
            err.contains("simulated-poll-error"),
            "op 1 should surface the seeded poll error: {}",
            err,
        );
    }
}
