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

use tokio::sync::mpsc;

use super::primitives::IbvCq;
use super::primitives::IbvWc;
use super::queue_pair::PollCompletionError;
use super::queue_pair::WorkRequestError;

/// The number of CQEs requested in each `ibv_poll_cq` call.
const CQES_PER_POLL: usize = 64;

/// Identifies one CQ, distinct from every other live one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ibverbs::device::IbvDevice;
    use crate::backend::ibverbs::mlx_device::MlxDevice;
    use crate::backend::ibverbs::primitives::IbvConfig;
    use crate::backend::ibverbs::primitives::IbvDeviceInfo;

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
}
