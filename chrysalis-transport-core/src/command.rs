/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::CompletionCredits;
use crate::CompletionPermit;
use crate::DriverId;
use crate::Notifier;
use crate::RequestId;
use crate::endpoint_submission::EndpointSubmission;
use crate::endpoint_submission::SharedSender;
use crate::queue;

/// One driver command paired with its completion correlation ID.
#[derive(Debug)]
pub struct CommandSubmission<T> {
    request: RequestId,
    command: T,
    permit: CompletionPermit,
}

impl<T> CommandSubmission<T> {
    /// Returns the request ID allocated when this command was accepted.
    pub const fn request(&self) -> RequestId {
        self.request
    }

    /// Separates the request ID and command.
    pub fn into_parts(self) -> (RequestId, T, CompletionPermit) {
        (self.request, self.command, self.permit)
    }
}

/// A command rejected by a full or closed driver queue.
#[derive(Debug)]
pub enum TryCommandError<T> {
    /// The bounded command queue has no available slot.
    Full(T),
    /// No terminal-completion slot is available.
    CompletionFull(T),
    /// The driver stopped accepting commands.
    Closed(T),
}

impl<T> TryCommandError<T> {
    /// Returns ownership of the rejected command.
    pub fn into_command(self) -> T {
        match self {
            Self::Full(command) | Self::CompletionFull(command) | Self::Closed(command) => command,
        }
    }
}

/// Correlates one accepted command with its eventual completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandReceipt {
    request: RequestId,
}

impl CommandReceipt {
    /// Returns the accepted request ID.
    pub const fn request(self) -> RequestId {
        self.request
    }
}

/// Thread-safe, nonblocking producer for one driver's control commands.
pub struct CommandSender<T> {
    driver: DriverId,
    queue: CommandQueue<T>,
    next_request: Arc<AtomicU64>,
    credits: CompletionCredits,
}

enum CommandQueue<T> {
    Dedicated(queue::Sender<CommandSubmission<T>>),
    Shared(SharedSender<T>),
}

impl<T> Clone for CommandQueue<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Dedicated(sender) => Self::Dedicated(sender.clone()),
            Self::Shared(sender) => Self::Shared(sender.clone()),
        }
    }
}

impl<T> Clone for CommandSender<T> {
    fn clone(&self) -> Self {
        Self {
            driver: self.driver,
            queue: self.queue.clone(),
            next_request: self.next_request.clone(),
            credits: self.credits.clone(),
        }
    }
}

/// Single-consumer control queue owned by one driver.
pub struct CommandReceiver<T> {
    queue: queue::Receiver<CommandSubmission<T>>,
}

/// Constructs bounded application and driver halves for typed control commands.
pub fn command_queue<T>(
    driver: DriverId,
    capacity: NonZeroUsize,
    notifier: Arc<dyn Notifier>,
) -> (CommandSender<T>, CommandReceiver<T>) {
    let credits = CompletionCredits::new(capacity, notifier.clone());
    command_queue_with_credits(driver, capacity, notifier, credits)
}

/// Constructs a command queue that shares terminal-completion admission credits.
///
/// `capacity` bounds commands waiting for this queue's driver. Dequeueing a
/// command immediately releases that queue slot. `credits` instead bound
/// accepted commands across their full lifetime: a credit remains occupied
/// while the driver executes the command and until the application consumes or
/// drops its terminal completion. The limits are distinct because the driver
/// can drain the command queue while earlier commands remain in flight or their
/// completions remain unread. Sharing `credits` lets multiple queues enforce
/// one aggregate terminal-completion limit.
pub fn command_queue_with_credits<T>(
    driver: DriverId,
    capacity: NonZeroUsize,
    notifier: Arc<dyn Notifier>,
    credits: CompletionCredits,
) -> (CommandSender<T>, CommandReceiver<T>) {
    let (sender, receiver) = queue::bounded(capacity, notifier);
    (
        CommandSender {
            driver,
            queue: CommandQueue::Dedicated(sender),
            next_request: Arc::new(AtomicU64::new(1)),
            credits,
        },
        CommandReceiver { queue: receiver },
    )
}

impl<T> CommandSender<T> {
    pub(crate) fn from_shared(
        driver: DriverId,
        queue: SharedSender<T>,
        credits: CompletionCredits,
    ) -> Self {
        Self {
            driver,
            queue: CommandQueue::Shared(queue),
            next_request: Arc::new(AtomicU64::new(1)),
            credits,
        }
    }

    /// Submits one command without waiting for driver progress.
    pub fn try_submit(&self, command: T) -> Result<CommandReceipt, TryCommandError<T>> {
        self.try_submit_inner(command, false)
    }

    /// Submits one final command and atomically stops further admission.
    pub fn try_submit_and_close(&self, command: T) -> Result<CommandReceipt, TryCommandError<T>> {
        self.try_submit_inner(command, true)
    }

    fn try_submit_inner(
        &self,
        command: T,
        close: bool,
    ) -> Result<CommandReceipt, TryCommandError<T>> {
        let Some(permit) = self.credits.try_acquire() else {
            return Err(TryCommandError::CompletionFull(command));
        };
        let sequence = self.next_request.fetch_add(1, Ordering::Relaxed);
        assert_ne!(sequence, 0, "request ID space exhausted");
        let request = RequestId::new(self.driver, sequence);
        let submission = CommandSubmission {
            request,
            command,
            permit,
        };
        let result = match &self.queue {
            CommandQueue::Dedicated(sender) if close => sender.try_push_and_close(submission),
            CommandQueue::Dedicated(sender) => sender.try_push(submission),
            CommandQueue::Shared(sender) if close => sender
                .try_push_and_close(EndpointSubmission::Command(submission))
                .map_err(|error| match error {
                    queue::TryPushError::Full(EndpointSubmission::Command(submission)) => {
                        queue::TryPushError::Full(submission)
                    }
                    queue::TryPushError::Closed(EndpointSubmission::Command(submission)) => {
                        queue::TryPushError::Closed(submission)
                    }
                    _ => unreachable!("shared queue should return a command"),
                }),
            CommandQueue::Shared(sender) => sender
                .try_push(EndpointSubmission::Command(submission))
                .map_err(|error| match error {
                    queue::TryPushError::Full(EndpointSubmission::Command(submission)) => {
                        queue::TryPushError::Full(submission)
                    }
                    queue::TryPushError::Closed(EndpointSubmission::Command(submission)) => {
                        queue::TryPushError::Closed(submission)
                    }
                    _ => unreachable!("shared queue should return a command"),
                }),
        };
        match result {
            Ok(()) => Ok(CommandReceipt { request }),
            Err(queue::TryPushError::Full(submission)) => {
                let (_, command, _) = submission.into_parts();
                Err(TryCommandError::Full(command))
            }
            Err(queue::TryPushError::Closed(submission)) => {
                let (_, command, _) = submission.into_parts();
                Err(TryCommandError::Closed(command))
            }
        }
    }

    /// Returns whether the driver stopped accepting commands.
    pub fn is_closed(&self) -> bool {
        match &self.queue {
            CommandQueue::Dedicated(sender) => sender.is_closed(),
            CommandQueue::Shared(sender) => sender.is_closed(),
        }
    }
}

impl<T> CommandReceiver<T> {
    /// Stops admission while preserving already accepted commands for draining.
    pub fn close(&self) {
        self.queue.close();
    }

    /// Removes the next accepted command, if one is available.
    pub fn try_pop(&self) -> Option<CommandSubmission<T>> {
        self.queue.try_pop()
    }

    /// Returns whether no accepted commands are waiting.
    pub fn is_empty(&self) -> bool {
        self.queue.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopNotifier;

    #[test]
    fn typed_commands_are_correlated_and_drained_after_close() {
        let driver = DriverId::from_u16(7);
        let (sender, receiver) = command_queue(
            driver,
            NonZeroUsize::new(2).unwrap(),
            Arc::new(NoopNotifier),
        );
        let receipt = sender.try_submit(String::from("connect")).unwrap();

        receiver.close();

        assert!(matches!(
            sender.try_submit(String::from("late")),
            Err(TryCommandError::Closed(command)) if command == "late"
        ));
        let (request, command, _permit) = receiver.try_pop().unwrap().into_parts();
        assert_eq!(request, receipt.request());
        assert_eq!(request.driver(), driver);
        assert_eq!(command, "connect");
    }
}
