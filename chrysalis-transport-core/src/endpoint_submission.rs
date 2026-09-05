/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use crate::AdmittedSubmission;
use crate::CommandSender;
use crate::CommandSubmission;
use crate::CompletionCredits;
use crate::DriverId;
use crate::Notifier;
use crate::SubmissionLimits;
use crate::SubmissionSender;
use crate::queue;
use crate::submission::ByteBudget;
use crate::submission::CancellationQueue;
use crate::submission::SharedSubmissionState;

/// One application submission in the endpoint-wide ordering domain.
#[derive(Debug)]
pub enum EndpointSubmission<T> {
    /// An endpoint control command.
    Command(CommandSubmission<T>),
    /// A stream operation.
    Stream(AdmittedSubmission),
}

pub(crate) struct SharedSender<T> {
    queue: queue::Sender<EndpointSubmission<T>>,
}

impl<T> Clone for SharedSender<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

impl<T> SharedSender<T> {
    pub(crate) fn try_push(
        &self,
        submission: EndpointSubmission<T>,
    ) -> Result<(), queue::TryPushError<EndpointSubmission<T>>> {
        self.queue.try_push(submission)
    }

    pub(crate) fn try_push_and_close(
        &self,
        submission: EndpointSubmission<T>,
    ) -> Result<(), queue::TryPushError<EndpointSubmission<T>>> {
        self.queue.try_push_and_close(submission)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.queue.is_closed()
    }
}

/// Single-consumer endpoint submission queue owned by one driver.
pub struct EndpointSubmissionReceiver<T> {
    queue: queue::Receiver<EndpointSubmission<T>>,
    cancellations: CancellationQueue,
    send_budget: Arc<ByteBudget>,
    receive_budget: Arc<ByteBudget>,
}

impl<T> EndpointSubmissionReceiver<T> {
    /// Stops admission while preserving accepted entries for ordered draining.
    pub fn close(&self) {
        self.queue.close();
    }

    /// Removes the oldest accepted command or stream operation.
    pub fn try_pop(&self) -> Option<EndpointSubmission<T>> {
        self.queue.try_pop()
    }

    /// Removes one newly cancelled operation, if available.
    pub fn try_pop_cancellation(&self) -> Option<(crate::OperationId, crate::StreamId)> {
        self.cancellations.try_pop()
    }

    /// Returns whether no accepted submission remains.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of submissions waiting for the driver.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns whether every sender is gone and every accepted submission was consumed.
    pub fn is_closed(&self) -> bool {
        self.queue.is_closed()
    }

    /// Returns a sequence that changes whenever work arrives or all senders close.
    pub fn sequence(&self) -> u64 {
        self.queue.sequence()
    }

    /// Returns the visible payload bytes currently retained for accepted sends.
    pub fn retained_send_bytes(&self) -> usize {
        self.send_budget.used()
    }

    /// Returns the capacity of receive buffers currently held by the transport.
    pub fn posted_receive_bytes(&self) -> usize {
        self.receive_budget.used()
    }
}

/// Constructs command and stream handles sharing one bounded FIFO and completion budget.
pub fn endpoint_submission_queue<T>(
    driver: DriverId,
    limits: SubmissionLimits,
    notifier: Arc<dyn Notifier>,
) -> (
    CommandSender<T>,
    SubmissionSender<T>,
    EndpointSubmissionReceiver<T>,
) {
    let credits = CompletionCredits::new(limits.queue_capacity(), notifier.clone());
    endpoint_submission_queue_with_credits(driver, limits, notifier, credits)
}

/// Constructs a unified endpoint submission queue with an external completion budget.
pub fn endpoint_submission_queue_with_credits<T>(
    driver: DriverId,
    limits: SubmissionLimits,
    notifier: Arc<dyn Notifier>,
    credits: CompletionCredits,
) -> (
    CommandSender<T>,
    SubmissionSender<T>,
    EndpointSubmissionReceiver<T>,
) {
    let cancellations = CancellationQueue::new(credits.capacity(), notifier.clone());
    let (sender, receiver) =
        queue::bounded::<EndpointSubmission<T>>(limits.queue_capacity(), notifier.clone());
    let send_budget = Arc::new(ByteBudget::new(
        limits.retained_send_bytes(),
        notifier.clone(),
    ));
    let receive_budget = Arc::new(ByteBudget::new(
        limits.posted_receive_bytes(),
        notifier.clone(),
    ));
    let sender = SharedSender { queue: sender };
    (
        CommandSender::from_shared(driver, sender.clone(), credits.clone()),
        SubmissionSender::from_shared(SharedSubmissionState {
            driver,
            queue: sender,
            credits,
            cancellations: cancellations.clone(),
            send_budget: send_budget.clone(),
            receive_budget: receive_budget.clone(),
        }),
        EndpointSubmissionReceiver {
            queue: receiver,
            cancellations,
            send_budget,
            receive_budget,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytes::Bytes;

    use super::*;
    use crate::ConnectionId;
    use crate::EndpointSubmission;
    use crate::NoopNotifier;
    use crate::StreamId;

    fn limits() -> SubmissionLimits {
        SubmissionLimits::new(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(16).unwrap(),
        )
    }

    #[test]
    fn commands_and_stream_work_share_fifo_and_terminal_close() {
        let driver = DriverId::from_u16(1);
        let stream = StreamId::new(ConnectionId::new(driver, 1), 0);
        let (commands, streams, queue) =
            endpoint_submission_queue(driver, limits(), Arc::new(NoopNotifier));

        let first = streams
            .try_send(stream, Bytes::from_static(b"first"))
            .unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.retained_send_bytes(), 5);
        assert_eq!(queue.posted_receive_bytes(), 0);
        assert_ne!(queue.sequence(), 0);
        assert!(!queue.is_closed());
        let shutdown = commands.try_submit_and_close("shutdown").unwrap();
        assert!(commands.try_submit("late").is_err());
        assert!(streams.try_finish(stream).is_err());

        let EndpointSubmission::Stream(admitted) = queue.try_pop().unwrap() else {
            panic!("stream operation should remain first");
        };
        assert_eq!(admitted.operation(), first.operation());
        let EndpointSubmission::Command(command) = queue.try_pop().unwrap() else {
            panic!("shutdown should remain second");
        };
        assert_eq!(command.request(), shutdown.request());
        assert!(queue.is_empty());
        drop(commands);
        drop(streams);
        assert!(queue.is_closed());
    }
}
