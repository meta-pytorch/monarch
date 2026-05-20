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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::actor::Referable;
use hyperactor::context::Mailbox;
use lru::LruCache;
use tokio::sync::Mutex;
use tokio::sync::OwnedMutexGuard;

use super::IbvOp;
use super::primitives::IbvConfig;
use super::primitives::IbvMemoryRegionView;
use super::queue_pair::QpKey;

/// A queue-pair value the processor owns end-to-end. The trait is a
/// marker today; data-path methods (post send / poll completion)
/// land alongside the real per-batch WR accounting in a follow-up.
pub(super) trait QueuePair: std::fmt::Debug + Send + Sync + 'static {}

/// Local-only message: ask the manager to register an MR for
/// `[addr, addr + size)` and return the resulting view.
#[cfg_attr(not(test), expect(dead_code, reason = "wired up in D105213930"))]
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
#[cfg_attr(not(test), expect(dead_code, reason = "wired up in D105213930"))]
#[derive(Debug)]
pub(super) struct RequestQueuePair<M: Referable, Qp: QueuePair> {
    pub qp_key: QpKey,
    #[expect(dead_code, reason = "read by IbvManagerActor handler in D105213930")]
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

#[cfg_attr(not(test), expect(dead_code, reason = "wired up in D105213930"))]
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
        self.manager.post(cx, RegisterMr { addr, size, reply });
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
            self.manager.post(
                cx,
                RequestQueuePair {
                    qp_key: qp_key.clone(),
                    remote_manager,
                    reply,
                },
            );
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

/// Stub. The real per-QP processing — `PostedOps`-driven posting,
/// CQ polling, and WR accounting — lands in a follow-up. Marks
/// every op in `group` as successful so the actor can be exercised
/// end-to-end without a working data path.
async fn process_qp_group<M, Qp>(
    _qp: &mut Qp,
    group: Vec<(usize, IbvOp<M>, IbvMemoryRegionView)>,
    _timeout: Duration,
    _max_send_wr: u32,
    _max_rd_atomic: u32,
) -> Vec<(usize, Result<(), String>)>
where
    M: Referable,
    Qp: QueuePair,
{
    group
        .into_iter()
        .map(|(orig_idx, _, _)| (orig_idx, Ok(())))
        .collect()
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
        reply.post(cx, results);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
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
    use crate::local_memory::Keepalive;
    use crate::local_memory::KeepaliveLocalMemory;

    fn null_domain() -> Arc<IbvDomain> {
        Arc::new(IbvDomain {
            context: std::ptr::null_mut(),
            pd: std::ptr::null_mut(),
        })
    }

    #[derive(Debug, Default)]
    struct MockManagerInner {
        register_mr_calls: Vec<(usize, usize)>,
        request_qp_calls: Vec<QpKey>,
        next_mr_id: usize,
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
            msg.reply.post(cx, Ok(mrv));
            Ok(())
        }
    }

    #[derive(Debug)]
    struct MockQp;
    impl QueuePair for MockQp {}

    #[async_trait]
    impl Handler<RequestQueuePair<MockManagerActor, MockQp>> for MockManagerActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: RequestQueuePair<MockManagerActor, MockQp>,
        ) -> anyhow::Result<()> {
            self.inner
                .lock()
                .unwrap()
                .request_qp_calls
                .push(msg.qp_key.clone());
            msg.reply.post(cx, Ok(MockQp));
            Ok(())
        }
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

    fn fake_remote_ref(label: &str, uid: u64) -> ActorRef<MockManagerActor> {
        let proc_id = ProcId::new(Uid::Instance(uid, None), Some(Label::new(label).unwrap()));
        let proc_addr = ProcAddr::new(proc_id, ChannelAddr::Local(1).into());
        ActorRef::attest(proc_addr.actor_addr("remote-mgr"))
    }

    fn fake_op_with_remote(
        addr: usize,
        size: usize,
        remote_device: &str,
        remote_manager: ActorRef<MockManagerActor>,
    ) -> IbvOp<MockManagerActor> {
        IbvOp {
            op_type: RdmaOpType::WriteFromLocal,
            local_memory: fake_local_memory(addr, size),
            remote_buffer: IbvBuffer {
                mr_id: 1,
                lkey: 0,
                rkey: 0,
                addr: 0x4000_0000,
                size,
                device_name: remote_device.to_string(),
            },
            remote_manager,
        }
    }

    fn fake_op(addr: usize, size: usize, remote_device: &str) -> IbvOp<MockManagerActor> {
        fake_op_with_remote(
            addr,
            size,
            remote_device,
            fake_remote_ref("remote-a", 0xabc123),
        )
    }

    struct Harness {
        client: hyperactor::Client,
        processor: ActorHandle<IbvProcessorActor<MockManagerActor, MockQp>>,
        mgr_inner: Arc<StdMutex<MockManagerInner>>,
    }

    async fn setup_with_lru(lru_capacity: usize) -> Harness {
        let proc = Proc::anonymous();
        let client = proc.client("client");
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
        harness.processor.post(
            &harness.client,
            SubmitOps {
                ops,
                timeout: Duration::from_secs(5),
                reply,
            },
        );
        rx.recv().await.unwrap()
    }

    #[tokio::test]
    async fn submit_ops_returns_ok_for_each_op_via_stub() {
        let h = setup().await;
        let results = submit(&h, vec![fake_op(0x1000, 4096, "dev0")]).await;
        assert_eq!(results.len(), 1);
        assert!(
            results[0].is_ok(),
            "stub should return Ok: {:?}",
            results[0]
        );
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
        // First submit: request a QP, store it in `peer_qps`.
        let _ = submit(&h, vec![fake_op(0x1000, 4096, "dev_remote_a")]).await;
        // Second submit with the same QP key: should reuse the
        // stored QP, not call the manager again.
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
}
