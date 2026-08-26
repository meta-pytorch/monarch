/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! One poller for many CQs: the actor that polls them, and the routes it holds
//! from each queue pair's completions back to that queue pair.
//!
//! A queue pair attaches to the CQ it reports on ([`Attach`]) and receives its
//! completions as [`CompletionBatch`] messages. Many queue pairs share one
//! [`CompletionQueueActor`] and one actor polls many CQs.
//!
//! A CQ is polled only while a queue pair on it has CQEs outstanding. Nothing
//! here decides that a CQ has stopped working: noticing that completions have
//! stopped arriving is the queue pair's responsibility.

// Nothing outside the tests below uses this module.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::PortHandle;

use super::primitives::IbvCq;
use super::primitives::IbvWc;
use super::queue_pair::PollCompletionError;
use super::queue_pair::WorkRequestError;

/// The number of CQEs requested in each `ibv_poll_cq` call.
const CQES_PER_POLL: usize = 64;

/// Identifies one CQ, distinct from every other live one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct CqId(usize);

/// One completion a poll consumed: the queue pair it completed on, and either the
/// work completion or the failure its work request reported.
#[derive(Debug)]
pub(super) struct Completion {
    qp_num: u32,
    result: Result<IbvWc, WorkRequestError>,
}

/// A CQ that hands back completions in bounded batches.
pub(super) trait IbvCompletionQueue: std::fmt::Debug + Send + Sync + 'static {
    /// This CQ's [`CqId`].
    fn cq_id(&self) -> CqId;

    /// Consumes a batch of completions, appending each to `out`. A batch is
    /// bounded, so appending nothing means the CQ was empty, while a full batch
    /// says nothing about what is left in it. A failure appends nothing.
    ///
    /// # Safety
    ///
    /// The CQ must be live or null, never a dangling `ibv_cq`, and no other
    /// thread may poll it for the duration. Polling a null CQ consumes nothing.
    unsafe fn poll(&self, out: &mut Vec<Completion>) -> Result<(), PollCompletionError>;
}

impl IbvCompletionQueue for IbvCq {
    fn cq_id(&self) -> CqId {
        CqId(self.as_ptr().addr())
    }

    unsafe fn poll(&self, out: &mut Vec<Completion>) -> Result<(), PollCompletionError> {
        let cq = self.as_ptr();
        // A placeholder holds no CQ, so nothing can complete on one.
        if cq.is_null() {
            return Ok(());
        }
        // SAFETY: `cq` is non-null and so a live `ibv_cq` (caller contract), which
        // holds its device context alive; we invoke that context's `poll_cq` verb
        // through the ops table.
        let poll_cq = unsafe {
            (*(*cq).context)
                .ops
                .poll_cq
                .expect("poll_cq verb missing from ibv_context ops")
        };
        let mut wcs = [rdmaxcel_sys::ibv_wc::default(); CQES_PER_POLL];
        // SAFETY: `cq` is live and no other thread is polling it (caller
        // contract); `wcs` holds the `CQES_PER_POLL` entries requested, of which
        // `poll_cq` overwrites the first `consumed` entries whenever it returns a
        // positive count.
        let consumed = unsafe { poll_cq(cq, CQES_PER_POLL as i32, wcs.as_mut_ptr()) };
        if consumed < 0 {
            return Err(PollCompletionError::new(format!(
                "CQ poll failed (ibv_poll_cq returned {consumed})"
            )));
        }
        for wc in &wcs[..consumed as usize] {
            // `error()` is `Some` exactly when the status is not `IBV_WC_SUCCESS`.
            out.push(Completion {
                qp_num: wc.qp_num,
                result: match wc.error() {
                    Some((status, vendor_err)) => Err(WorkRequestError::from_status(
                        wc.wr_id(),
                        status,
                        vendor_err,
                    )),
                    None => Ok(IbvWc::from(*wc)),
                },
            });
        }
        Ok(())
    }
}

/// The completions consumed for one queue pair, in the order its CQ produced
/// them. Each is either a work completion or the failure its work
/// request reported.
#[derive(Debug)]
pub(super) struct CompletionBatch {
    pub(super) completions: Vec<Result<IbvWc, WorkRequestError>>,
}

/// Route the completions `qp_num` produces on `cq` to `port`.
#[derive(Debug)]
pub(super) struct Attach<Cq: IbvCompletionQueue> {
    pub(super) cq: Arc<Cq>,
    /// Local number of the attaching queue pair, carried by every completion it
    /// produces.
    pub(super) qp_num: u32,
    pub(super) port: PortHandle<CompletionBatch>,
    /// Number of pending, posted WRs associated with this queue pair. Incremented
    /// inside `QueuePairActor` whenever it posts new WRs. The `CompletionQueueActor`
    /// stores this and uses it to determine if it is done processing work for this
    /// QP.
    pub(super) posted: Arc<AtomicU64>,
    pub(super) reply: OncePortHandle<()>,
}

/// Wakes the poller: some queue pair has posted.
#[derive(Debug)]
pub(super) struct Posted;

/// Self-message that drives one polling round.
#[derive(Debug)]
struct Poll;

/// One queue pair reporting on a CQ.
#[derive(Debug)]
struct QpSlot {
    /// Where the QP's completions go.
    port: PortHandle<CompletionBatch>,
    /// The QP's [`Attach::posted`] counter, which stops moving when it stops posting.
    posted: Arc<AtomicU64>,
    /// CQEs consumed for this queue pair.
    consumed: u64,
}

impl QpSlot {
    /// WRs the queue pair has posted for and the poller has yet to consume.
    /// Negative between consuming a completion and observing the post that
    /// produced it.
    fn outstanding(&self) -> i64 {
        self.posted.load(Ordering::Relaxed) as i64 - self.consumed as i64
    }
}

/// One CQ, with the queue pairs reporting on it.
#[derive(Debug)]
struct CqSlot<Cq> {
    cq: Arc<Cq>,
    queue_pairs: HashMap<u32, QpSlot>,
}

impl<Cq: IbvCompletionQueue> CqSlot<Cq> {
    /// Whether any queue pair here awaits a completion, and so whether the CQ is
    /// worth polling.
    fn expects_completions(&self) -> bool {
        self.queue_pairs.values().any(|qp| qp.outstanding() > 0)
    }

    /// Hands each of `consumed` to the queue pair that produced it.
    fn route(
        &mut self,
        cq_id: CqId,
        cx: &Instance<CompletionQueueActor<Cq>>,
        consumed: &mut Vec<Completion>,
    ) {
        for (qp_num, completions) in group_by_queue_pair(consumed) {
            let qp = self.queue_pairs.get_mut(&qp_num).unwrap_or_else(|| {
                // When a `QueuePairActor` terminates, we hold its `QpSlot` alive
                // until all its pending CQEs have drained, so it should be impossible
                // to observe a `qp_num` without a corresponding `QpSlot`.
                panic!("CQ {cq_id:?} completed for unattached queue pair {qp_num}")
            });
            qp.consumed += completions.len() as u64;

            if let Err(error) = qp.port.try_post(cx, CompletionBatch { completions }) {
                // It's okay to just log and do nothing here. It won't mess up the QP's internal credit
                // accounting, because the delivery should only be able to fail once the QP is
                // already dead
                tracing::error!(
                    qp_num,
                    ?cq_id,
                    %error,
                    "delivering completions to a queue pair failed",
                );
            }
        }
    }
}

/// Groups `consumed` by the queue pair each completion came from, keeping the
/// order the CQ produced them in.
fn group_by_queue_pair(
    consumed: &mut Vec<Completion>,
) -> HashMap<u32, Vec<Result<IbvWc, WorkRequestError>>> {
    let mut batches: HashMap<u32, Vec<Result<IbvWc, WorkRequestError>>> = HashMap::new();
    for Completion { qp_num, result } in consumed.drain(..) {
        batches.entry(qp_num).or_default().push(result);
    }
    batches
}

/// Polls a set of CQs and delivers every completion to the queue pair that
/// produced it.
///
/// Generic over the CQ type `Cq` so unit tests run without RDMA hardware. A
/// poller starts empty: CQs arrive with the queue pairs that attach to them.
#[derive(Debug)]
pub(super) struct CompletionQueueActor<Cq: IbvCompletionQueue> {
    completion_queues: HashMap<CqId, CqSlot<Cq>>,
    /// The completions one poll consumes, reused by every round.
    consumed: Vec<Completion>,
    /// `true` while a `Poll` self-message is already in flight; the flag
    /// prevents stacking redundant rounds.
    poll_armed: bool,
}

impl<Cq: IbvCompletionQueue> CompletionQueueActor<Cq> {
    pub(super) fn new() -> Self {
        Self {
            completion_queues: HashMap::new(),
            consumed: Vec::with_capacity(CQES_PER_POLL),
            poll_armed: false,
        }
    }

    /// Arms a polling round, unless one is already armed or no queue pair awaits
    /// a completion.
    fn arm(&mut self, cx: &Instance<Self>) -> Result<(), anyhow::Error> {
        if self.poll_armed
            || !self
                .completion_queues
                .values()
                .any(|slot| slot.expects_completions())
        {
            return Ok(());
        }
        self.poll_armed = true;
        if let Err(error) = cx.handle().try_post(cx, Poll) {
            self.poll_armed = false;
            return Err(error.into());
        }
        Ok(())
    }

    /// One polling round: consumes a batch from every CQ with completions expected,
    /// routes what comes back, and arms the next round if anything is still
    /// expected.
    ///
    /// Returns `Err` if arming the next round fails, which the surrounding
    /// handler propagates so supervision tears the actor down. A poll that fails
    /// does not bring down the actor, since it does not mean that the CQ is
    /// poisoned.
    fn poll_round(&mut self, cx: &Instance<Self>) -> Result<(), anyhow::Error> {
        let Self {
            completion_queues,
            consumed,
            ..
        } = self;
        for (cq_id, slot) in completion_queues {
            if !slot.expects_completions() {
                continue;
            }
            consumed.clear();
            // SAFETY: a CQ stays live or null while a queue pair is attached to
            // it, and every `Arc` of it reaches this one poller, whose loop is
            // single-threaded -- so nothing else polls it here. Both are
            // obligations on the wiring that sends `Attach`, which does not exist
            // yet.
            if let Err(error) = unsafe { slot.cq.poll(consumed) } {
                // The failing entry is consumed either way, so the CQ can still
                // make progress next round.
                tracing::warn!(?cq_id, %error, "consuming from a CQ failed");
                continue;
            }
            slot.route(*cq_id, cx, consumed);
        }
        self.arm(cx)
    }
}

#[async_trait]
impl<Cq: IbvCompletionQueue> Actor for CompletionQueueActor<Cq> {
    // Polling is data-plane work that runs for as long as transfers do. Run its
    // loop on the dedicated rdma runtime rather than the shared control-plane
    // runtime; see `crate::rdma_runtime`.
    fn spawn_server_task<F>(future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        crate::rdma_runtime::spawn_on_rdma_runtime(future)
    }
}

#[async_trait]
impl<Cq: IbvCompletionQueue> Handler<Attach<Cq>> for CompletionQueueActor<Cq> {
    async fn handle(&mut self, cx: &Context<Self>, msg: Attach<Cq>) -> Result<(), anyhow::Error> {
        let Attach {
            cq,
            qp_num,
            port,
            posted,
            reply,
        } = msg;
        let cq_id = cq.cq_id();
        let slot = self
            .completion_queues
            .entry(cq_id)
            .or_insert_with(|| CqSlot {
                cq,
                queue_pairs: HashMap::new(),
            });
        assert!(
            !slot.queue_pairs.contains_key(&qp_num),
            "queue pair {qp_num} is already attached to CQ {cq_id:?}",
        );
        slot.queue_pairs.insert(
            qp_num,
            QpSlot {
                port,
                posted,
                consumed: 0,
            },
        );
        // The route is in place whether or not anyone is left to hear that, and
        // failing here would take every other queue pair on this poller with it.
        if let Err(error) = reply.try_post(cx, ()) {
            tracing::warn!(qp_num, ?cq_id, %error, "failed to deliver Attach reply");
        }
        self.arm(cx)
    }
}

#[async_trait]
impl<Cq: IbvCompletionQueue> Handler<Posted> for CompletionQueueActor<Cq> {
    async fn handle(&mut self, cx: &Context<Self>, _msg: Posted) -> Result<(), anyhow::Error> {
        self.arm(cx)
    }
}

#[async_trait]
impl<Cq: IbvCompletionQueue> Handler<Poll> for CompletionQueueActor<Cq> {
    async fn handle(&mut self, cx: &Context<Self>, _msg: Poll) -> Result<(), anyhow::Error> {
        self.poll_armed = false;
        self.poll_round(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use anyhow::Result;
    use hyperactor::ActorHandle;
    use hyperactor::context::Mailbox;
    use hyperactor::mailbox::PortReceiver;
    use hyperactor::proc::Proc;

    use super::*;
    use crate::backend::ibverbs::device::IbvDevice;
    use crate::backend::ibverbs::mlx_device::MlxDevice;
    use crate::backend::ibverbs::primitives::IbvConfig;
    use crate::backend::ibverbs::primitives::IbvDeviceInfo;

    /// [`IbvCompletionQueue`] mock: hands back scripted completions in FIFO
    /// order, at most `batch` of them per poll, and counts the polls it serves.
    /// The test and the actor share one through an `Arc`.
    #[derive(Debug)]
    struct MockCq {
        cq_id: CqId,
        batch: usize,
        inner: Mutex<MockCqInner>,
    }

    #[derive(Debug, Default)]
    struct MockCqInner {
        queued: VecDeque<Completion>,
        /// Failures the next polls return, one each, before any completion.
        poll_errors: VecDeque<PollCompletionError>,
        polls: usize,
    }

    impl MockCq {
        fn new(cq_id: usize, batch: usize) -> Arc<Self> {
            Arc::new(Self {
                cq_id: CqId(cq_id),
                batch,
                inner: Mutex::new(MockCqInner::default()),
            })
        }

        /// Queue a successful completion for `wr_id` on `qp_num`.
        fn queue_completion(&self, qp_num: u32, wr_id: u64) {
            self.queue(Completion {
                qp_num,
                result: Ok(IbvWc::for_test(wr_id, true)),
            });
        }

        /// Queue a completion reporting that `wr_id` failed.
        fn queue_wr_error(&self, qp_num: u32, wr_id: u64, message: &str) {
            self.queue(Completion {
                qp_num,
                result: Err(WorkRequestError::for_test(wr_id, message)),
            });
        }

        fn queue(&self, completion: Completion) {
            self.inner.lock().unwrap().queued.push_back(completion);
        }

        /// Make each of the next `count` polls fail.
        fn queue_poll_errors(&self, count: usize, message: &str) {
            let mut inner = self.inner.lock().unwrap();
            for _ in 0..count {
                inner
                    .poll_errors
                    .push_back(PollCompletionError::new(message.to_string()));
            }
        }

        fn polls(&self) -> usize {
            self.inner.lock().unwrap().polls
        }
    }

    impl IbvCompletionQueue for MockCq {
        fn cq_id(&self) -> CqId {
            self.cq_id
        }

        unsafe fn poll(&self, out: &mut Vec<Completion>) -> Result<(), PollCompletionError> {
            let mut inner = self.inner.lock().unwrap();
            inner.polls += 1;
            if let Some(error) = inner.poll_errors.pop_front() {
                return Err(error);
            }
            let consumed = inner.queued.len().min(self.batch);
            out.extend(inner.queued.drain(..consumed));
            Ok(())
        }
    }

    /// One attached queue pair, from the test's side: where its completions
    /// arrive, and the counter it posts against.
    struct TestRoute {
        completions: PortReceiver<CompletionBatch>,
        posted: Arc<AtomicU64>,
    }

    /// Parent of the actor under test, so its supervision events land here and
    /// the test can await them.
    #[derive(Debug)]
    struct MockParent {
        supervision_tx:
            tokio::sync::mpsc::UnboundedSender<hyperactor::supervision::ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for MockParent {
        async fn handle_supervision_event(
            &mut self,
            _this: &Instance<Self>,
            event: &hyperactor::supervision::ActorSupervisionEvent,
        ) -> Result<bool> {
            self.supervision_tx
                .send(event.clone())
                .map_err(|e| anyhow::anyhow!("supervision_tx send failed: {e}"))?;
            Ok(true)
        }
    }

    /// Local message that spawns the poller as a child of this parent. The reply
    /// carries its handle so the test can drive it and watch its status.
    #[derive(Debug)]
    struct SpawnPoller {
        reply: OncePortHandle<ActorHandle<CompletionQueueActor<MockCq>>>,
    }

    #[async_trait]
    impl Handler<SpawnPoller> for MockParent {
        async fn handle(&mut self, cx: &Context<Self>, msg: SpawnPoller) -> Result<()> {
            let actor = CompletionQueueActor::<MockCq>::new();
            let handle = cx.spawn(actor);
            msg.reply.try_post(cx, handle)?;
            Ok(())
        }
    }

    /// The poller under test.
    struct Poller {
        handle: ActorHandle<CompletionQueueActor<MockCq>>,
    }

    struct CqaHarness {
        proc: Proc,
        parent: ActorHandle<MockParent>,
        client: hyperactor::Client,
        supervision_rx:
            tokio::sync::mpsc::UnboundedReceiver<hyperactor::supervision::ActorSupervisionEvent>,
    }

    impl CqaHarness {
        fn build() -> Self {
            let proc = Proc::anonymous();
            let (supervision_tx, supervision_rx) = tokio::sync::mpsc::unbounded_channel();
            let parent = proc.spawn_with_label("parent", MockParent { supervision_tx });
            let client = proc.client("client");
            Self {
                proc,
                parent,
                client,
                supervision_rx,
            }
        }

        async fn spawn_poller(&self) -> Result<Poller> {
            let (reply, handle_rx) = self.client.mailbox().open_once_port();
            self.parent.try_post(&self.client, SpawnPoller { reply })?;
            Ok(Poller {
                handle: handle_rx.recv().await?,
            })
        }

        /// Attach `qp_num` on `cq`.
        async fn attach(
            &self,
            poller: &Poller,
            cq: &Arc<MockCq>,
            qp_num: u32,
        ) -> Result<TestRoute> {
            let (port, completions) = self.client.mailbox().open_port::<CompletionBatch>();
            let (reply, rx) = self.client.mailbox().open_once_port();
            let posted = Arc::new(AtomicU64::new(0));
            poller.handle.try_post(
                &self.client,
                Attach {
                    cq: Arc::clone(cq),
                    qp_num,
                    port,
                    posted: Arc::clone(&posted),
                    reply,
                },
            )?;
            rx.recv().await?;
            Ok(TestRoute {
                completions,
                posted,
            })
        }

        /// Post for `n` further completions, as a queue pair does: count them,
        /// then wake the poller.
        fn post(&self, poller: &Poller, route: &TestRoute, n: u64) -> Result<()> {
            self.post_without_wake(route, n);
            poller.handle.try_post(&self.client, Posted)?;
            Ok(())
        }

        /// Count `n` completions without waking the poller, as a queue pair whose
        /// wake-up was lost does.
        fn post_without_wake(&self, route: &TestRoute, n: u64) {
            route.posted.fetch_add(n, Ordering::Relaxed);
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

        /// Destroys the proc, closes the supervision channel, drains every
        /// remaining event, and asserts none are unexpected.
        async fn teardown(mut self) {
            self.proc
                .destroy_and_wait(Duration::from_secs(30), "test teardown")
                .await
                .expect("destroy_and_wait failed");
            self.supervision_rx.close();
            let mut leftover = Vec::new();
            while let Some(event) = self.supervision_rx.recv().await {
                leftover.push(event);
            }
            assert!(
                leftover.is_empty(),
                "unexpected supervision events at teardown: {leftover:?}",
            );
        }
    }

    /// Await the next batch of completions, each rendered as its `wr_id` or the
    /// message of the failure it reported; panics on timeout.
    async fn recv_batch(rx: &mut PortReceiver<CompletionBatch>) -> Vec<Result<u64, String>> {
        let batch = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for a completion batch")
            .expect("completion port closed");
        batch
            .completions
            .into_iter()
            .map(|completion| {
                completion
                    .map(|wc| wc.wr_id())
                    .map_err(|error| error.to_string())
            })
            .collect()
    }

    /// The `wr_id`s of `n` successful completions, however many batches they
    /// arrive in.
    async fn recv_wr_ids(rx: &mut PortReceiver<CompletionBatch>, n: usize) -> Vec<u64> {
        let mut wr_ids = Vec::with_capacity(n);
        while wr_ids.len() < n {
            for completion in recv_batch(rx).await {
                wr_ids.push(completion.expect("completion should be successful"));
            }
        }
        wr_ids
    }

    /// Assert no batch arrives within `wait`.
    async fn assert_no_batch(rx: &mut PortReceiver<CompletionBatch>, wait: Duration) {
        if let Ok(Ok(batch)) = tokio::time::timeout(wait, rx.recv()).await {
            panic!("unexpected completions: {batch:?}");
        }
    }

    #[test]
    fn grouping_keeps_each_queue_pairs_completions_in_order() {
        let mut consumed = vec![
            Completion {
                qp_num: 7,
                result: Ok(IbvWc::for_test(100, true)),
            },
            Completion {
                qp_num: 9,
                result: Ok(IbvWc::for_test(200, true)),
            },
            Completion {
                qp_num: 7,
                result: Err(WorkRequestError::for_test(101, "failed")),
            },
            Completion {
                qp_num: 7,
                result: Ok(IbvWc::for_test(102, true)),
            },
        ];
        let batches = group_by_queue_pair(&mut consumed);
        assert!(consumed.is_empty(), "grouping consumes what it groups");
        assert_eq!(batches.len(), 2, "two queue pairs completed: {batches:?}");

        let wr_ids = |qp_num: u32| -> Vec<u64> {
            batches[&qp_num]
                .iter()
                .map(|completion| match completion {
                    Ok(wc) => wc.wr_id(),
                    Err(error) => error.wr_id,
                })
                .collect()
        };
        assert_eq!(wr_ids(7), vec![100, 101, 102]);
        assert_eq!(wr_ids(9), vec![200]);
    }

    /// A CQ nobody is waiting on is left alone, however much it holds: what
    /// drives a poll is a queue pair expecting completions.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_leaves_an_idle_cq_alone() -> Result<()> {
        let harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, CQES_PER_POLL);
        let mut qp = harness.attach(&poller, &cq, 7).await?;
        cq.queue_completion(7, 100);

        assert_no_batch(&mut qp.completions, Duration::from_millis(200)).await;
        assert_eq!(cq.polls(), 0, "an attach alone must not start polling");
        harness.teardown().await;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_routes_completions_to_the_queue_pair_that_posted_them() -> Result<()> {
        let harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, CQES_PER_POLL);
        let mut qp = harness.attach(&poller, &cq, 7).await?;

        cq.queue_completion(7, 100);
        cq.queue_wr_error(7, 101, "simulated WR fail");
        harness.post(&poller, &qp, 2)?;

        let batch = recv_batch(&mut qp.completions).await;
        let [first, second] = batch.as_slice() else {
            panic!("expected two completions, got: {batch:?}");
        };
        assert_eq!(
            *first.as_ref().expect("wr 100 succeeded"),
            100,
            "a successful completion arrives as `Ok`",
        );
        let error = second.as_ref().expect_err("wr 101 failed");
        assert!(
            error.contains("simulated WR fail"),
            "a failed work request arrives as its own error: {error}",
        );
        harness.teardown().await;
        Ok(())
    }

    /// Once everything posted for has been consumed, polling stops rather than
    /// spinning on an idle CQ.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_stops_polling_once_everything_posted_for_is_consumed() -> Result<()> {
        let harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, CQES_PER_POLL);
        let mut qp = harness.attach(&poller, &cq, 7).await?;

        cq.queue_completion(7, 100);
        harness.post(&poller, &qp, 1)?;
        assert_eq!(recv_wr_ids(&mut qp.completions, 1).await, vec![100]);

        tokio::time::sleep(Duration::from_millis(100)).await;
        let settled = cq.polls();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            cq.polls(),
            settled,
            "nothing is outstanding, so the CQ is no longer polled",
        );
        harness.teardown().await;
        Ok(())
    }

    /// Queue pairs sharing a CQ each see only their own completions, which is
    /// what lets one poller serve several of them.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_splits_a_shared_cq_between_its_queue_pairs() -> Result<()> {
        let harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, CQES_PER_POLL);
        let mut first = harness.attach(&poller, &cq, 7).await?;
        let mut second = harness.attach(&poller, &cq, 9).await?;

        cq.queue_completion(7, 100);
        cq.queue_completion(9, 200);
        cq.queue_completion(7, 101);
        harness.post(&poller, &first, 2)?;
        harness.post(&poller, &second, 1)?;

        assert_eq!(recv_wr_ids(&mut first.completions, 2).await, vec![100, 101]);
        assert_eq!(recv_wr_ids(&mut second.completions, 1).await, vec![200]);
        harness.teardown().await;
        Ok(())
    }

    /// A queue pair with more outstanding than one poll consumes gets the rest in
    /// later batches.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_delivers_more_completions_than_a_batch_holds() -> Result<()> {
        let harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, 2);
        let mut qp = harness.attach(&poller, &cq, 7).await?;

        for wr_id in 0..5 {
            cq.queue_completion(7, wr_id);
        }
        harness.post(&poller, &qp, 5)?;

        let mut wr_ids = Vec::new();
        while wr_ids.len() < 5 {
            let batch = recv_batch(&mut qp.completions).await;
            assert!(
                batch.len() <= 2,
                "a batch is bounded: {} consumed",
                batch.len(),
            );
            wr_ids.extend(
                batch
                    .into_iter()
                    .map(|completion| completion.expect("completion should be successful")),
            );
        }
        assert_eq!(wr_ids, vec![0, 1, 2, 3, 4]);
        harness.teardown().await;
        Ok(())
    }

    /// A shared CQ is polled on any queue pair's behalf, so a completion can be
    /// consumed before the post that produced it has been counted.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_tolerates_a_completion_consumed_before_its_post_is_counted() -> Result<()> {
        let harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, CQES_PER_POLL);
        let mut first = harness.attach(&poller, &cq, 7).await?;
        let mut second = harness.attach(&poller, &cq, 9).await?;

        // Only queue pair 9 has counted anything, and the round it drives consumes
        // queue pair 7's completion too.
        cq.queue_completion(7, 100);
        cq.queue_completion(9, 200);
        harness.post(&poller, &second, 1)?;

        assert_eq!(recv_wr_ids(&mut first.completions, 1).await, vec![100]);
        assert_eq!(recv_wr_ids(&mut second.completions, 1).await, vec![200]);

        // Queue pair 7 is owed nothing now, so counting its post afterwards
        // leaves it expecting nothing rather than a second entry.
        harness.post(&poller, &first, 1)?;
        assert_no_batch(&mut first.completions, Duration::from_millis(100)).await;
        harness.teardown().await;
        Ok(())
    }

    /// Every queue pair on a CQ has a route, so a completion for one that does
    /// not is the device attributing an entry to a queue pair that never existed
    /// here -- unroutable, and not something to carry on through.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_fails_on_a_completion_for_an_unattached_queue_pair() -> Result<()> {
        let mut harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, CQES_PER_POLL);
        let qp7 = harness.attach(&poller, &cq, 7).await?;

        cq.queue_completion(404, 900);
        harness.post(&poller, &qp7, 1)?;

        let event = harness.next_supervision_failure().await;
        assert_eq!(&event.actor_id, poller.handle.actor_addr());

        let report = event.failure_report().expect("event should be a failure");
        assert!(
            report.contains("panic"),
            "supervision report should say the poller panicked: {report}",
        );
        let mut status = poller.handle.status();
        status
            .wait_for(|s| matches!(s, hyperactor::actor::ActorStatus::Failed(_)))
            .await?;
        harness.teardown().await;
        Ok(())
    }

    /// A failed poll is retried, not fatal.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn cqa_survives_a_failed_poll() -> Result<()> {
        let harness = CqaHarness::build();
        let poller = harness.spawn_poller().await?;
        let cq = MockCq::new(1, CQES_PER_POLL);
        let mut qp7 = harness.attach(&poller, &cq, 7).await?;

        cq.queue_poll_errors(3, "simulated stale entry");
        cq.queue_completion(7, 100);
        harness.post(&poller, &qp7, 1)?;

        assert_eq!(recv_wr_ids(&mut qp7.completions, 1).await, vec![100]);
        assert!(
            cq.polls() > 3,
            "the failures were retried, not fatal: {} polls",
            cq.polls(),
        );
        harness.teardown().await;
        Ok(())
    }

    /// Polling a real, empty CQ consumes nothing, and distinct CQs have distinct ids.
    #[test]
    fn polling_an_empty_cq_consumes_nothing() {
        let info = IbvDeviceInfo::first_available().expect("a device is available");
        let device = IbvDevice::<MlxDevice>::try_open(info.name(), IbvConfig::default())
            .expect("the first available device should open");
        // SAFETY: the device holds its `ibv_context` open for its own lifetime.
        let cq = unsafe { IbvCq::create(device.context(), 16) }.expect("creating a CQ should work");
        // SAFETY: as above.
        let other =
            unsafe { IbvCq::create(device.context(), 16) }.expect("creating a CQ should work");
        assert_ne!(cq.cq_id(), other.cq_id());

        let mut consumed = Vec::new();
        // SAFETY: `cq` is the live CQ created above, which nothing else has been
        // handed and so nothing else polls.
        unsafe { cq.poll(&mut consumed) }.expect("polling an empty CQ should succeed");
        assert!(consumed.is_empty());
    }
}
