/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Consuming completions from a completion queue (CQ).
//!
//! [`IbvCompletionQueue`] is what a CQ offers a consumer: one call hands back a
//! bounded batch, each completion naming the queue pair it completed on.

// Nothing outside the tests below uses this module.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

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
    /// The CQ must be live -- a null `ibv_cq` does not qualify -- and no other
    /// thread may poll it for the duration.
    unsafe fn poll(&self, out: &mut Vec<Completion>) -> Result<(), PollCompletionError>;
}

impl IbvCompletionQueue for IbvCq {
    fn cq_id(&self) -> CqId {
        CqId(self.as_ptr().addr())
    }

    unsafe fn poll(&self, out: &mut Vec<Completion>) -> Result<(), PollCompletionError> {
        let cq = self.as_ptr();
        // SAFETY: `cq` is a live `ibv_cq` (caller contract), which holds its device
        // context alive; we invoke that context's `poll_cq` verb through the ops
        // table.
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

pub(super) type CompletionResult = Result<IbvWc, WorkRequestError>;

/// Producer-side route used by a CQ poller to deliver one queue pair's
/// completions directly to its data-plane task.
#[derive(Clone, Debug)]
pub(super) struct CompletionRoute {
    sender: mpsc::UnboundedSender<CompletionResult>,
}

/// Consumer-side queue owned by a queue pair's data-plane task.
#[derive(Debug)]
pub(super) struct CompletionInbox {
    receiver: mpsc::UnboundedReceiver<CompletionResult>,
}

impl CompletionInbox {
    pub(super) fn new() -> (Self, CompletionRoute) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { receiver }, CompletionRoute { sender })
    }

    pub(super) async fn recv(&mut self) -> Option<CompletionResult> {
        self.receiver.recv().await
    }

    pub(super) fn try_recv(&mut self) -> Result<CompletionResult, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

impl CompletionRoute {
    #[cfg(test)]
    pub(super) fn sender_for_test(&self) -> mpsc::UnboundedSender<CompletionResult> {
        self.sender.clone()
    }

    fn deliver(&self, qp_num: u32, cq_id: CqId, result: CompletionResult) {
        if let Err(error) = self.sender.send(result) {
            tracing::error!(qp_num, ?cq_id, %error, "queueing a completion for a queue pair failed");
        }
    }
}

/// One queue pair reporting on a CQ.
#[derive(Debug)]
struct QpSlot {
    route: CompletionRoute,
    /// Number of WRs the queue pair has posted. The queue pair increments this
    /// before waking the poller.
    posted: Arc<AtomicU64>,
    /// Number of CQEs the poller has consumed for this queue pair.
    consumed: u64,
}

impl QpSlot {
    fn expects_completions(&self) -> bool {
        // This count only decides whether polling is useful. A relaxed load is
        // sufficient because the producer calls `unpark` after incrementing its
        // posted count. A subsequent wake from `park` on the consumer is then
        // guaranteed to see the most recent value.
        self.posted.load(Ordering::Relaxed) > self.consumed
    }
}

/// One CQ and the routes for queue pairs that report completions on it.
#[derive(Debug)]
struct CqSlot<Cq> {
    cq: Arc<Cq>,
    queue_pairs: HashMap<u32, QpSlot>,
}

impl<Cq: IbvCompletionQueue> CqSlot<Cq> {
    fn expects_completions(&self) -> bool {
        self.queue_pairs.values().any(QpSlot::expects_completions)
    }

    fn route(&mut self, cq_id: CqId, consumed: &mut Vec<Completion>) -> anyhow::Result<()> {
        for Completion { qp_num, result } in consumed.drain(..) {
            let qp = self.queue_pairs.get_mut(&qp_num).ok_or_else(|| {
                anyhow::anyhow!("CQ {cq_id:?} completed for unattached queue pair {qp_num}")
            })?;
            // Increment unconditionally, even if the delivery ultimately fails.
            // Delivery can fail only if the QP actor is stopped or dead, so it
            // shouldn't be possible to hang the QP actor.
            qp.consumed += 1;
            qp.route.deliver(qp_num, cq_id, result);
        }
        Ok(())
    }
}

/// Completion queues and per-QP routes owned by one poller.
#[derive(Debug)]
struct PollerState<Cq> {
    completion_queues: HashMap<CqId, CqSlot<Cq>>,
    consumed: Vec<Completion>,
}

impl<Cq: IbvCompletionQueue> PollerState<Cq> {
    fn new() -> Self {
        Self {
            completion_queues: HashMap::new(),
            consumed: Vec::with_capacity(CQES_PER_POLL),
        }
    }

    fn attach(
        &mut self,
        cq: Arc<Cq>,
        qp_num: u32,
        route: CompletionRoute,
        posted: Arc<AtomicU64>,
    ) -> anyhow::Result<()> {
        let cq_id = cq.cq_id();
        let slot = self
            .completion_queues
            .entry(cq_id)
            .or_insert_with(|| CqSlot {
                cq,
                queue_pairs: HashMap::new(),
            });
        if slot.queue_pairs.contains_key(&qp_num) {
            return Err(anyhow::anyhow!(
                "queue pair {qp_num} is already attached to CQ {cq_id:?}"
            ));
        }
        slot.queue_pairs.insert(
            qp_num,
            QpSlot {
                route,
                posted,
                consumed: 0,
            },
        );
        Ok(())
    }

    fn expects_completions(&self) -> bool {
        self.completion_queues
            .values()
            .any(CqSlot::expects_completions)
    }

    fn poll_round(&mut self) -> anyhow::Result<()> {
        for (cq_id, slot) in &mut self.completion_queues {
            if !slot.expects_completions() {
                continue;
            }
            self.consumed.clear();
            // SAFETY: the poller exclusively owns every CQ in
            // `completion_queues`; each `Arc<Cq>` keeps its CQ live.
            if let Err(error) = unsafe { slot.cq.poll(&mut self.consumed) } {
                tracing::warn!(?cq_id, %error, "consuming from a CQ failed");
                continue;
            }
            slot.route(*cq_id, &mut self.consumed)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::backend::ibverbs::device::IbvDevice;
    use crate::backend::ibverbs::mlx_device::MlxDevice;
    use crate::backend::ibverbs::primitives::IbvConfig;
    use crate::backend::ibverbs::primitives::IbvDeviceInfo;

    #[derive(Debug)]
    struct MockCq {
        cq_id: CqId,
        batch: usize,
        inner: Mutex<MockCqInner>,
    }

    #[derive(Debug, Default)]
    struct MockCqInner {
        queued: VecDeque<Completion>,
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

        fn queue_completion(&self, qp_num: u32, wr_id: u64) {
            self.inner
                .lock()
                .expect("mock CQ lock poisoned")
                .queued
                .push_back(Completion {
                    qp_num,
                    result: Ok(IbvWc::for_test(wr_id, true)),
                });
        }

        fn queue_poll_error(&self, message: &str) {
            self.inner
                .lock()
                .expect("mock CQ lock poisoned")
                .poll_errors
                .push_back(PollCompletionError::new(message.to_owned()));
        }

        fn polls(&self) -> usize {
            self.inner.lock().expect("mock CQ lock poisoned").polls
        }
    }

    impl IbvCompletionQueue for MockCq {
        fn cq_id(&self) -> CqId {
            self.cq_id
        }

        unsafe fn poll(&self, out: &mut Vec<Completion>) -> Result<(), PollCompletionError> {
            let mut inner = self.inner.lock().expect("mock CQ lock poisoned");
            inner.polls += 1;
            if let Some(error) = inner.poll_errors.pop_front() {
                return Err(error);
            }
            let consumed = inner.queued.len().min(self.batch);
            out.extend(inner.queued.drain(..consumed));
            Ok(())
        }
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

    #[tokio::test]
    async fn completion_inbox_preserves_order() {
        let (mut inbox, route) = CompletionInbox::new();
        route.deliver(7, CqId(1), Ok(IbvWc::for_test(100, true)));
        route.deliver(7, CqId(1), Ok(IbvWc::for_test(200, true)));
        assert_eq!(
            inbox
                .recv()
                .await
                .expect("first completion")
                .expect("successful completion")
                .wr_id(),
            100,
        );
        assert_eq!(
            inbox
                .recv()
                .await
                .expect("second completion")
                .expect("successful completion")
                .wr_id(),
            200,
        );
    }

    #[tokio::test]
    async fn completion_inbox_closes_when_its_route_is_dropped() {
        let (mut inbox, route) = CompletionInbox::new();
        drop(route);
        assert!(
            inbox.recv().await.is_none(),
            "dropping the producer must close the completion channel",
        );
    }

    #[tokio::test]
    async fn poller_routes_a_shared_cq_by_queue_pair() {
        let cq = MockCq::new(1, CQES_PER_POLL);
        let (mut first_inbox, first_route) = CompletionInbox::new();
        let (mut second_inbox, second_route) = CompletionInbox::new();
        let first_posted = Arc::new(AtomicU64::new(1));
        let second_posted = Arc::new(AtomicU64::new(1));
        let mut poller = PollerState::new();
        poller
            .attach(Arc::clone(&cq), 7, first_route, first_posted)
            .expect("first queue pair should attach");
        poller
            .attach(Arc::clone(&cq), 8, second_route, second_posted)
            .expect("second queue pair should attach");
        cq.queue_completion(8, 200);
        cq.queue_completion(7, 100);

        poller.poll_round().expect("polling should succeed");

        assert_eq!(
            first_inbox
                .try_recv()
                .expect("first completion")
                .expect("successful completion")
                .wr_id(),
            100,
        );
        assert_eq!(
            second_inbox
                .try_recv()
                .expect("second completion")
                .expect("successful completion")
                .wr_id(),
            200,
        );
    }

    #[test]
    fn poller_leaves_idle_completion_queues_alone() {
        let cq = MockCq::new(1, CQES_PER_POLL);
        let (_inbox, route) = CompletionInbox::new();
        let mut poller = PollerState::new();
        poller
            .attach(Arc::clone(&cq), 7, route, Arc::new(AtomicU64::new(0)))
            .expect("queue pair should attach");
        cq.queue_completion(7, 100);

        poller.poll_round().expect("polling should succeed");

        assert_eq!(cq.polls(), 0, "an idle queue pair must not trigger a poll");
    }

    #[test]
    fn poller_retries_after_a_poll_error() {
        let cq = MockCq::new(1, CQES_PER_POLL);
        let (mut inbox, route) = CompletionInbox::new();
        let posted = Arc::new(AtomicU64::new(1));
        let mut poller = PollerState::new();
        poller
            .attach(Arc::clone(&cq), 7, route, posted)
            .expect("queue pair should attach");
        cq.queue_poll_error("temporary failure");
        cq.queue_completion(7, 100);

        poller.poll_round().expect("a CQ poll error is recoverable");
        assert!(
            matches!(inbox.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "a failed poll must not fabricate a completion",
        );
        poller.poll_round().expect("the next poll should succeed");
        assert_eq!(
            inbox
                .try_recv()
                .expect("completion after retry")
                .expect("successful completion")
                .wr_id(),
            100,
        );
    }
}
