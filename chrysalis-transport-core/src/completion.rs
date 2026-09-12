/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use bytes::BytesMut;
use chrysalis_core::Pid;

use crate::CompletionPermit;
use crate::ConnectionId;
use crate::DriverId;
use crate::Notifier;
use crate::OperationId;
use crate::PostedBuffer;
use crate::RequestId;
use crate::StreamId;
use crate::queue;

/// Describes why a posted receive returned to its caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiveStatus {
    /// The buffer reached its completion threshold and the stream remains open.
    Data,
    /// The peer finished its send half after these bytes.
    Fin,
    /// The peer reset the stream with an application error code.
    Reset(u64),
    /// The connection or driver closed before the receive completed.
    Closed,
    /// The caller cancelled the posted receive before another outcome won.
    Cancelled,
    /// The application stopped its local receive half with this error code.
    Stopped(u64),
}

/// Describes how one accepted send released its retained bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendOutcome {
    /// Every byte was acknowledged by the peer.
    Acknowledged {
        /// The stream's cumulative acknowledged offset.
        acknowledged_through: u64,
    },
    /// The stream send half was already finishing or terminal.
    Rejected,
    /// The stream, connection, or driver closed before acknowledgement.
    Abandoned,
}

/// Describes how one accepted control operation ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlOutcome {
    /// The requested operation completed.
    Complete,
    /// The operation was invalid for the stream's current state.
    Rejected,
    /// The stream, connection, or driver closed before completion.
    Abandoned,
}

/// Stable failure classes for runtime-neutral driver commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandError {
    /// The command is invalid for the endpoint's client or server role.
    WrongRole,
    /// The referenced connection is not owned by this driver.
    UnknownConnection,
    /// A command argument violates the transport contract.
    InvalidArgument,
    /// The endpoint stopped accepting work.
    DriverStopped,
    /// The transport implementation rejected the command.
    Transport,
}

/// Successful result or stable failure of one driver command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandResult {
    /// A connection state machine was created; handshake completion is a separate event.
    ConnectionCreated(ConnectionId),
    /// A locally initiated bidirectional stream ID was allocated.
    StreamOpened(StreamId),
    /// A connection close was accepted by the QUIC state machine.
    CloseQueued(ConnectionId),
    /// The endpoint entered its draining state.
    ShutdownStarted,
    /// The endpoint began immediate ordered abort.
    AbortStarted,
    /// The command did not take effect.
    Failed(CommandError),
}

/// Correlates a driver command result with its accepted request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandCompletion {
    request: RequestId,
    result: CommandResult,
}

/// An authenticated QUIC connection and its certificate-derived peer PID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionEstablished {
    connection: ConnectionId,
    local: Pid,
    peer: Pid,
}

impl ConnectionEstablished {
    /// Constructs an authenticated connection event.
    pub const fn new(connection: ConnectionId, local: Pid, peer: Pid) -> Self {
        Self {
            connection,
            local,
            peer,
        }
    }

    /// Returns the local connection handle.
    pub const fn connection(self) -> ConnectionId {
        self.connection
    }

    /// Returns the local PID encoded in this connection's source CIDs.
    pub const fn local(self) -> Pid {
        self.local
    }

    /// Returns the certificate-derived peer PID.
    pub const fn peer(self) -> Pid {
        self.peer
    }
}

/// A completed TLS handshake that did not authenticate the requested PID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticationFailed {
    connection: ConnectionId,
    expected: Option<Pid>,
    actual: Option<Pid>,
}

impl AuthenticationFailed {
    /// Constructs an authentication failure event.
    pub const fn new(connection: ConnectionId, expected: Option<Pid>, actual: Option<Pid>) -> Self {
        Self {
            connection,
            expected,
            actual,
        }
    }

    /// Returns the rejected local connection handle.
    pub const fn connection(self) -> ConnectionId {
        self.connection
    }

    /// Returns the requested peer PID, for outbound connections.
    pub const fn expected(self) -> Option<Pid> {
        self.expected
    }

    /// Returns the certificate-derived PID, or `None` when no certificate was presented.
    pub const fn actual(self) -> Option<Pid> {
        self.actual
    }
}

impl CommandCompletion {
    /// Constructs a command completion.
    pub const fn new(request: RequestId, result: CommandResult) -> Self {
        Self { request, result }
    }

    /// Returns the accepted request ID.
    pub const fn request(self) -> RequestId {
        self.request
    }

    /// Returns the command result.
    pub const fn result(self) -> CommandResult {
        self.result
    }
}

/// Returns one posted caller buffer and identifies the bytes appended by the driver.
#[derive(Debug)]
pub struct ReceiveCompletion {
    operation: OperationId,
    stream: StreamId,
    buffer: PostedBuffer,
    filled: Range<usize>,
    status: ReceiveStatus,
}

impl ReceiveCompletion {
    /// Constructs a completion after a driver extends `buffer` from `initial_len`.
    ///
    /// # Panics
    ///
    /// Panics if `initial_len` exceeds the returned buffer length.
    pub fn new(
        operation: OperationId,
        stream: StreamId,
        buffer: PostedBuffer,
        initial_len: usize,
        status: ReceiveStatus,
    ) -> Self {
        assert!(
            initial_len <= buffer.buffer().len(),
            "initial receive length should not exceed returned buffer length"
        );
        let filled = initial_len..buffer.buffer().len();
        Self {
            operation,
            stream,
            buffer,
            filled,
            status,
        }
    }

    /// Returns the operation ID.
    pub const fn operation(&self) -> OperationId {
        self.operation
    }

    /// Returns the completed stream.
    pub const fn stream(&self) -> StreamId {
        self.stream
    }

    /// Returns the bytes appended by the driver.
    pub fn data(&self) -> &[u8] {
        &self.buffer.buffer()[self.filled.clone()]
    }

    /// Returns why the driver completed this receive.
    pub const fn status(&self) -> ReceiveStatus {
        self.status
    }

    /// Returns ownership of the caller buffer.
    pub fn into_buffer(self) -> BytesMut {
        self.buffer.into_buffer()
    }
}

/// One transport operation or lifecycle event delivered to the application.
#[derive(Debug)]
pub enum Completion {
    /// One thread-safe driver command completed.
    Command(CommandCompletion),
    /// A client or accepted connection completed its authenticated QUIC handshake.
    ConnectionEstablished(ConnectionEstablished),
    /// A completed handshake did not present or match a required certificate identity.
    AuthenticationFailed(AuthenticationFailed),
    /// The peer opened a bidirectional stream.
    IncomingStream(StreamId),
    /// Releases one accepted send operation.
    Send {
        /// The completed operation.
        operation: OperationId,
        /// The completed stream.
        stream: StreamId,
        /// The submitted payload size.
        bytes: usize,
        /// How the send ended.
        outcome: SendOutcome,
    },
    /// Returns one caller-owned receive buffer.
    Receive(ReceiveCompletion),
    /// Releases one accepted local-FIN operation.
    Finish {
        /// The completed operation.
        operation: OperationId,
        /// The completed stream.
        stream: StreamId,
        /// How the FIN operation ended.
        outcome: ControlOutcome,
    },
    /// Releases one accepted receive-discard operation.
    Discard {
        /// The completed operation.
        operation: OperationId,
        /// The completed stream.
        stream: StreamId,
        /// The bytes discarded by this operation.
        bytes: usize,
        /// Why the discard operation completed.
        status: ReceiveStatus,
    },
    /// Releases one accepted reset-send operation.
    Reset {
        /// The completed operation.
        operation: OperationId,
        /// The reset stream.
        stream: StreamId,
        /// How reset ended.
        outcome: ControlOutcome,
    },
    /// Releases one accepted stop-receiving operation.
    Stop {
        /// The completed operation.
        operation: OperationId,
        /// The stopped stream.
        stream: StreamId,
        /// How stop ended.
        outcome: ControlOutcome,
    },
    /// A stream can no longer make progress.
    Closed {
        /// The closed stream.
        stream: StreamId,
        /// The peer's application error code, when it reset the stream.
        error_code: Option<u64>,
    },
    /// A connection can no longer make progress.
    ConnectionClosed {
        /// The closed connection.
        connection: ConnectionId,
        /// The peer's application error code, when available.
        error_code: Option<u64>,
    },
    /// One driver stopped after returning its outstanding operation ownership.
    DriverStopped(DriverId),
}

/// A completion rejected by a full or closed application queue.
#[derive(Debug)]
pub enum TryCompleteError {
    /// The application must drain completions before the driver retries.
    Full {
        /// Rejected completion.
        completion: Completion,
        /// Reserved output slot retained for retry.
        permit: CompletionPermit,
    },
    /// The application dropped its completion queue.
    Closed {
        /// Rejected completion.
        completion: Completion,
        /// Reserved output slot released when this error is dropped.
        permit: CompletionPermit,
    },
}

/// Driver-side producer for one completion queue.
#[derive(Clone)]
pub struct CompletionSender {
    queue: queue::Sender<LeasedCompletion>,
}

/// Application-side consumer for one completion queue.
pub struct CompletionQueue {
    queue: queue::Receiver<LeasedCompletion>,
}

/// One completion that retains its driver admission credit until consumed or dropped.
pub struct LeasedCompletion {
    completion: Completion,
    _permit: CompletionPermit,
}

impl LeasedCompletion {
    /// Borrows the retained completion.
    pub const fn completion(&self) -> &Completion {
        &self.completion
    }

    /// Consumes the lease and releases its admission credit.
    pub fn into_completion(self) -> Completion {
        self.completion
    }
}

/// Constructs the driver and application halves of one bounded completion queue.
pub fn completion_queue(
    capacity: NonZeroUsize,
    notifier: Arc<dyn Notifier>,
) -> (CompletionSender, CompletionQueue) {
    let (sender, receiver) = queue::bounded(capacity, notifier);
    (
        CompletionSender { queue: sender },
        CompletionQueue { queue: receiver },
    )
}

impl CompletionSender {
    /// Delivers a completion without waiting for application progress.
    #[allow(clippy::result_large_err)] // Backpressure must return ownership without allocating.
    pub fn try_push(
        &self,
        completion: Completion,
        permit: CompletionPermit,
    ) -> Result<(), TryCompleteError> {
        let admitted = LeasedCompletion {
            completion,
            _permit: permit,
        };
        match self.queue.try_push(admitted) {
            Ok(()) => Ok(()),
            Err(queue::TryPushError::Full(admitted)) => Err(TryCompleteError::Full {
                completion: admitted.completion,
                permit: admitted._permit,
            }),
            Err(queue::TryPushError::Closed(admitted)) => Err(TryCompleteError::Closed {
                completion: admitted.completion,
                permit: admitted._permit,
            }),
        }
    }

    /// Returns whether the application dropped its queue.
    pub fn is_closed(&self) -> bool {
        self.queue.is_closed()
    }
}

impl CompletionQueue {
    /// Removes the next completion while retaining its driver admission credit.
    pub fn try_pop_leased(&self) -> Option<LeasedCompletion> {
        self.queue.try_pop()
    }

    /// Removes the next completion, if one is available.
    pub fn try_pop(&self) -> Option<Completion> {
        self.try_pop_leased().map(LeasedCompletion::into_completion)
    }

    /// Returns the number of queued completions.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns whether no completions are queued.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether every producer is gone and every completion was consumed.
    pub fn is_closed(&self) -> bool {
        self.queue.is_closed()
    }

    /// Returns a sequence that changes whenever a completion arrives or all producers close.
    pub fn sequence(&self) -> u64 {
        self.queue.sequence()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bytes::BytesMut;

    use super::*;
    use crate::CompletionCredits;
    use crate::ConnectionId;
    use crate::DriverId;
    use crate::NoopNotifier;
    use crate::Submission;
    use crate::SubmissionLimits;
    use crate::submission_queue;

    const DRIVER: DriverId = DriverId::from_u16(1);
    const STREAM: StreamId = StreamId::new(ConnectionId::new(DRIVER, 2), 4);

    #[derive(Default)]
    struct CountingNotifier(AtomicUsize);

    impl Notifier for CountingNotifier {
        fn notify(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl CountingNotifier {
        fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn finished(operation: u64) -> Completion {
        Completion::Finish {
            operation: OperationId::new(DRIVER, operation),
            stream: STREAM,
            outcome: ControlOutcome::Complete,
        }
    }

    fn credits(capacity: usize) -> CompletionCredits {
        CompletionCredits::new(
            NonZeroUsize::new(capacity).expect("test capacity should be nonzero"),
            Arc::new(NoopNotifier),
        )
    }

    #[test]
    fn notifier_coalesces_until_the_queue_becomes_empty() {
        let notifier = Arc::new(CountingNotifier::default());
        let (sender, queue) = completion_queue(
            NonZeroUsize::new(4).expect("test capacity should be nonzero"),
            notifier.clone(),
        );
        let credits = credits(4);

        sender
            .try_push(finished(1), credits.try_acquire().unwrap())
            .expect("push first completion");
        sender
            .try_push(finished(2), credits.try_acquire().unwrap())
            .expect("push second completion");
        assert_eq!(notifier.count(), 1);
        queue.try_pop().expect("pop first completion");
        sender
            .try_push(finished(3), credits.try_acquire().unwrap())
            .expect("push while queue remains nonempty");
        assert_eq!(notifier.count(), 1);
        queue.try_pop().expect("pop second completion");
        queue.try_pop().expect("pop third completion");

        sender
            .try_push(finished(4), credits.try_acquire().unwrap())
            .expect("push after draining queue");
        assert_eq!(notifier.count(), 2);
    }

    #[test]
    fn full_completion_queue_returns_event_ownership() {
        let (sender, _queue) = completion_queue(NonZeroUsize::MIN, Arc::new(crate::NoopNotifier));
        let credits = credits(2);
        sender
            .try_push(finished(1), credits.try_acquire().unwrap())
            .expect("fill completion queue");

        let error = sender
            .try_push(finished(2), credits.try_acquire().unwrap())
            .expect_err("full completion queue should reject event");
        assert!(matches!(
            error,
            TryCompleteError::Full {
                completion: Completion::Finish { .. },
                ..
            }
        ));
    }

    #[test]
    fn leased_completion_retains_credit_until_consumed() {
        let (sender, queue) = completion_queue(NonZeroUsize::MIN, Arc::new(crate::NoopNotifier));
        let credits = credits(1);
        sender
            .try_push(finished(1), credits.try_acquire().unwrap())
            .expect("queue completion");

        let completion = queue.try_pop_leased().expect("pop leased completion");
        assert_eq!(credits.used(), 1);
        assert!(matches!(completion.completion(), Completion::Finish { .. }));

        drop(completion);
        assert_eq!(credits.used(), 0);
    }

    #[test]
    fn receive_completion_returns_appended_bytes_and_original_allocation() {
        let mut buffer = BytesMut::with_capacity(16);
        buffer.extend_from_slice(b"prefix");
        let pointer = buffer.as_ptr();
        let initial_len = buffer.len();
        let limits = SubmissionLimits::new(
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            NonZeroUsize::new(16).expect("test receive budget should be nonzero"),
        );
        let (submission_sender, submission_receiver) =
            submission_queue(DRIVER, limits, Arc::new(NoopNotifier));
        let operation = submission_sender
            .try_receive(STREAM, buffer, crate::ReceiveOptions::default())
            .expect("post receive buffer")
            .operation();
        let (submission, _permit) = submission_receiver
            .try_pop()
            .expect("pop receive submission")
            .into_parts();
        let Submission::Receive(submission) = submission else {
            panic!("expected receive submission")
        };
        let (_, _, mut buffer, _) = submission.into_parts();
        buffer.buffer_mut().extend_from_slice(b"data");
        let completion =
            ReceiveCompletion::new(operation, STREAM, buffer, initial_len, ReceiveStatus::Data);

        assert_eq!(completion.data(), b"data");
        assert_eq!(completion.status(), ReceiveStatus::Data);
        assert_eq!(submission_sender.posted_receive_bytes(), 16);
        assert_eq!(completion.into_buffer().as_ptr(), pointer);
        assert_eq!(submission_sender.posted_receive_bytes(), 0);
    }

    #[test]
    fn last_sender_close_changes_sequence_and_wakes_consumer() {
        let notifier = Arc::new(CountingNotifier::default());
        let (sender, queue) = completion_queue(NonZeroUsize::MIN, notifier.clone());
        let sequence = queue.sequence();

        drop(sender);

        assert!(queue.is_closed());
        assert!(queue.sequence() > sequence);
        assert_eq!(notifier.count(), 1);
    }

    #[test]
    fn dropping_completion_queue_releases_posted_receive_budget() {
        let limits = SubmissionLimits::new(
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            NonZeroUsize::new(16).expect("test receive budget should be nonzero"),
        );
        let (submission_sender, submission_receiver) =
            submission_queue(DRIVER, limits, Arc::new(NoopNotifier));
        let operation = submission_sender
            .try_receive(
                STREAM,
                BytesMut::with_capacity(16),
                crate::ReceiveOptions::default(),
            )
            .expect("post receive buffer")
            .operation();
        let (submission, permit) = submission_receiver
            .try_pop()
            .expect("pop receive submission")
            .into_parts();
        let Submission::Receive(submission) = submission else {
            panic!("expected receive submission")
        };
        let (_, _, buffer, _) = submission.into_parts();
        let completion = Completion::Receive(ReceiveCompletion::new(
            operation,
            STREAM,
            buffer,
            0,
            ReceiveStatus::Closed,
        ));
        let (completion_sender, completion_queue) =
            completion_queue(NonZeroUsize::MIN, Arc::new(NoopNotifier));
        completion_sender
            .try_push(completion, permit)
            .expect("queue receive completion");
        assert_eq!(submission_sender.posted_receive_bytes(), 16);

        drop(completion_queue);

        assert_eq!(submission_sender.posted_receive_bytes(), 0);
    }
}
