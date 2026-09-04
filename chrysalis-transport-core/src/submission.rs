/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use bytes::BytesMut;

use crate::CompletionCredits;
use crate::CompletionPermit;
use crate::DriverId;
use crate::Notifier;
use crate::OperationId;
use crate::StreamId;
use crate::endpoint_submission::EndpointSubmission;
use crate::endpoint_submission::SharedSender;
use crate::queue;

/// Bounded memory and command limits for one transport driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmissionLimits {
    queue_capacity: NonZeroUsize,
    retained_send_bytes: NonZeroUsize,
    posted_receive_bytes: NonZeroUsize,
}

impl SubmissionLimits {
    /// Constructs submission limits for one driver.
    pub const fn new(
        queue_capacity: NonZeroUsize,
        retained_send_bytes: NonZeroUsize,
        posted_receive_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            queue_capacity,
            retained_send_bytes,
            posted_receive_bytes,
        }
    }

    /// Returns the maximum number of submissions waiting for the driver.
    pub const fn queue_capacity(self) -> NonZeroUsize {
        self.queue_capacity
    }

    /// Returns the maximum visible payload bytes retained for unacknowledged sends.
    pub const fn retained_send_bytes(self) -> NonZeroUsize {
        self.retained_send_bytes
    }

    /// Returns the maximum capacity of receive buffers held by the transport.
    pub const fn posted_receive_bytes(self) -> NonZeroUsize {
        self.posted_receive_bytes
    }
}

/// Options for one posted receive buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveOptions {
    min_bytes: NonZeroUsize,
}

impl ReceiveOptions {
    /// Constructs receive options with a minimum completion size.
    pub const fn new(min_bytes: NonZeroUsize) -> Self {
        Self { min_bytes }
    }

    /// Returns the preferred minimum bytes for a data completion.
    pub const fn min_bytes(self) -> NonZeroUsize {
        self.min_bytes
    }
}

impl Default for ReceiveOptions {
    fn default() -> Self {
        Self {
            min_bytes: NonZeroUsize::MIN,
        }
    }
}

/// A reference-counted payload view whose visible bytes count against a driver's send budget.
#[derive(Clone, Debug)]
pub struct RetainedBytes {
    bytes: Bytes,
    reservation: Arc<Reservation>,
}

impl RetainedBytes {
    /// Returns the visible byte slice.
    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the visible byte count.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the visible view is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Splits off the first `at` bytes while retaining one shared budget reservation.
    pub fn split_to(&mut self, at: usize) -> Self {
        Self {
            bytes: self.bytes.split_to(at),
            reservation: self.reservation.clone(),
        }
    }

    /// Truncates the visible view without changing the retained-byte reservation.
    pub fn truncate(&mut self, len: usize) {
        self.bytes.truncate(len);
    }

    /// Returns the visible bytes and releases this view's budget ownership.
    pub fn into_bytes(self) -> Bytes {
        let Self { bytes, reservation } = self;
        drop(reservation);
        bytes
    }
}

/// One accepted stream-send operation.
#[derive(Debug)]
pub struct SendSubmission {
    operation: OperationId,
    stream: StreamId,
    payload: RetainedBytes,
}

impl SendSubmission {
    /// Separates the operation ID, stream ID, and retained payload.
    pub fn into_parts(self) -> (OperationId, StreamId, RetainedBytes) {
        (self.operation, self.stream, self.payload)
    }
}

/// One accepted posted-receive operation.
#[derive(Debug)]
pub struct ReceiveSubmission {
    operation: OperationId,
    stream: StreamId,
    buffer: PostedBuffer,
    options: ReceiveOptions,
    cancellation: OperationCancellation,
}

impl ReceiveSubmission {
    /// Separates the operation ID, stream ID, caller buffer, and receive options.
    pub fn into_parts(
        self,
    ) -> (
        OperationId,
        StreamId,
        PostedBuffer,
        ReceiveOptions,
        OperationCancellation,
    ) {
        let Self {
            operation,
            stream,
            buffer,
            options,
            cancellation,
        } = self;
        (operation, stream, buffer, options, cancellation)
    }

    /// Returns whether the caller cancelled this posted receive.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns a cancellation token shared with the caller.
    pub fn cancellation(&self) -> OperationCancellation {
        self.cancellation.clone()
    }
}

/// A caller-owned receive allocation held against one driver's posted-byte budget.
#[derive(Debug)]
pub struct PostedBuffer {
    buffer: BytesMut,
    reservation: Reservation,
}

impl PostedBuffer {
    /// Returns the caller buffer.
    pub fn buffer(&self) -> &BytesMut {
        &self.buffer
    }

    /// Extends the initialized region to its reserved capacity and returns the writable suffix.
    pub fn writable(&mut self) -> &mut [u8] {
        let length = self.buffer.len();
        self.buffer.resize(self.reservation.bytes, 0);
        &mut self.buffer[length..]
    }

    /// Truncates the initialized region after a receive attempt.
    pub fn truncate(&mut self, length: usize) {
        self.buffer.truncate(length);
    }

    /// Returns ownership of the caller buffer and releases its posted-byte reservation.
    pub fn into_buffer(self) -> BytesMut {
        let Self {
            buffer,
            reservation,
        } = self;
        drop(reservation);
        buffer
    }
}

/// One accepted local-FIN operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinishSubmission {
    operation: OperationId,
    stream: StreamId,
}

impl FinishSubmission {
    /// Returns the operation ID.
    pub const fn operation(self) -> OperationId {
        self.operation
    }

    /// Returns the target stream.
    pub const fn stream(self) -> StreamId {
        self.stream
    }
}

/// One accepted receive-discard operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscardSubmission {
    operation: OperationId,
    stream: StreamId,
    max_bytes: NonZeroUsize,
}

/// One accepted reset-send operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResetSubmission {
    operation: OperationId,
    stream: StreamId,
    error_code: u64,
}

impl ResetSubmission {
    /// Returns the operation ID.
    pub const fn operation(self) -> OperationId {
        self.operation
    }

    /// Returns the target stream.
    pub const fn stream(self) -> StreamId {
        self.stream
    }

    /// Returns the application error code.
    pub const fn error_code(self) -> u64 {
        self.error_code
    }
}

/// One accepted stop-receiving operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StopSubmission {
    operation: OperationId,
    stream: StreamId,
    error_code: u64,
}

impl StopSubmission {
    /// Returns the operation ID.
    pub const fn operation(self) -> OperationId {
        self.operation
    }

    /// Returns the target stream.
    pub const fn stream(self) -> StreamId {
        self.stream
    }

    /// Returns the application error code.
    pub const fn error_code(self) -> u64 {
        self.error_code
    }
}

impl DiscardSubmission {
    /// Returns the operation ID.
    pub const fn operation(self) -> OperationId {
        self.operation
    }

    /// Returns the target stream.
    pub const fn stream(self) -> StreamId {
        self.stream
    }

    /// Returns the maximum bytes discarded by this operation.
    pub const fn max_bytes(self) -> NonZeroUsize {
        self.max_bytes
    }
}

/// One command accepted by a transport driver.
#[derive(Debug)]
pub enum Submission {
    /// Retains and sends immutable stream bytes.
    Send(SendSubmission),
    /// Fills and returns one caller-owned receive buffer.
    Receive(ReceiveSubmission),
    /// Queues a local FIN after preceding sends.
    Finish(FinishSubmission),
    /// Discards received stream bytes without materializing them.
    Discard(DiscardSubmission),
    /// Resets the local send half after preceding sends.
    Reset(ResetSubmission),
    /// Stops the local receive half after preceding receives and discards.
    Stop(StopSubmission),
}

impl Submission {
    /// Returns the operation ID.
    pub const fn operation(&self) -> OperationId {
        match self {
            Self::Send(submission) => submission.operation,
            Self::Receive(submission) => submission.operation,
            Self::Finish(submission) => submission.operation,
            Self::Discard(submission) => submission.operation,
            Self::Reset(submission) => submission.operation,
            Self::Stop(submission) => submission.operation,
        }
    }

    /// Returns the target stream.
    pub const fn stream(&self) -> StreamId {
        match self {
            Self::Send(submission) => submission.stream,
            Self::Receive(submission) => submission.stream,
            Self::Finish(submission) => submission.stream,
            Self::Discard(submission) => submission.stream,
            Self::Reset(submission) => submission.stream,
            Self::Stop(submission) => submission.stream,
        }
    }
}

/// Identifies why a send could not be accepted without blocking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendBlockReason {
    /// The driver's command queue is full.
    QueueFull,
    /// The driver's terminal-completion budget is exhausted.
    CompletionFull,
    /// The driver's unacknowledged send-byte budget is exhausted.
    RetainedBytes,
}

/// A rejected send that returns ownership of its immutable bytes.
#[derive(Debug)]
pub enum TrySendError {
    /// The payload is empty and cannot make stream progress.
    Empty(Bytes),
    /// The operation can be retried after transport progress.
    WouldBlock {
        /// The resource that prevented submission.
        reason: SendBlockReason,
        /// The rejected payload.
        bytes: Bytes,
    },
    /// The driver submission queue is closed.
    Closed(Bytes),
}

impl TrySendError {
    /// Returns ownership of the rejected bytes.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Empty(bytes) | Self::WouldBlock { bytes, .. } | Self::Closed(bytes) => bytes,
        }
    }
}

/// Identifies why a receive could not be accepted without blocking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiveBlockReason {
    /// The driver's command queue is full.
    QueueFull,
    /// The driver's terminal-completion budget is exhausted.
    CompletionFull,
    /// The driver's posted receive-byte budget is exhausted.
    PostedBytes,
}

/// A rejected receive that returns ownership of its caller buffer.
#[derive(Debug)]
pub enum TryReceiveError {
    /// The buffer has less spare capacity than `options.min_bytes()`.
    InvalidBuffer {
        /// The rejected caller buffer.
        buffer: BytesMut,
        /// The requested receive behavior.
        options: ReceiveOptions,
    },
    /// The operation can be retried after transport progress.
    WouldBlock {
        /// The resource that prevented submission.
        reason: ReceiveBlockReason,
        /// The rejected caller buffer.
        buffer: BytesMut,
        /// The requested receive behavior.
        options: ReceiveOptions,
    },
    /// The driver submission queue is closed.
    Closed {
        /// The rejected caller buffer.
        buffer: BytesMut,
        /// The requested receive behavior.
        options: ReceiveOptions,
    },
}

impl TryReceiveError {
    /// Returns ownership of the rejected buffer and its receive options.
    pub fn into_parts(self) -> (BytesMut, ReceiveOptions) {
        match self {
            Self::InvalidBuffer { buffer, options }
            | Self::WouldBlock {
                buffer, options, ..
            }
            | Self::Closed { buffer, options } => (buffer, options),
        }
    }
}

/// Identifies why a control operation could not be accepted without blocking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlBlockReason {
    /// The driver's command queue is full.
    QueueFull,
    /// The driver's terminal-completion budget is exhausted.
    CompletionFull,
}

/// A rejected control operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TryControlError {
    /// The operation can be retried after transport progress.
    WouldBlock(ControlBlockReason),
    /// The driver submission queue is closed.
    Closed,
}

/// Correlates an accepted submission with its eventual completion.
#[derive(Clone, Debug)]
pub struct SubmissionReceipt {
    operation: OperationId,
    cancellation: Option<OperationCancellation>,
}

impl SubmissionReceipt {
    /// Returns the accepted operation ID.
    pub const fn operation(&self) -> OperationId {
        self.operation
    }

    /// Returns a cancellation token for cancellable operations.
    pub fn cancellation(&self) -> Option<OperationCancellation> {
        self.cancellation.clone()
    }
}

/// Runtime-neutral cancellation for an accepted operation.
#[derive(Clone)]
pub struct OperationCancellation {
    inner: Arc<CancellationInner>,
}

struct CancellationInner {
    state: AtomicU8,
    operation: OperationId,
    stream: StreamId,
    queue: CancellationQueue,
}

const CANCELLATION_ACTIVE: u8 = 0;
const CANCELLATION_REQUESTED: u8 = 1;
const CANCELLATION_COMPLETED: u8 = 2;

impl fmt::Debug for OperationCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperationCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl OperationCancellation {
    fn new(operation: OperationId, stream: StreamId, queue: CancellationQueue) -> Self {
        Self {
            inner: Arc::new(CancellationInner {
                state: AtomicU8::new(CANCELLATION_ACTIVE),
                operation,
                stream,
                queue,
            }),
        }
    }

    /// Requests cancellation and wakes the owning driver.
    ///
    /// Returns `true` only for the first cancellation request.
    /// The driver linearizes this request against receive completion: the caller observes either
    /// `ReceiveStatus::Cancelled` with its buffer or the data, FIN, reset, or close that won first.
    pub fn cancel(&self) -> bool {
        let requested = self
            .inner
            .state
            .compare_exchange(
                CANCELLATION_ACTIVE,
                CANCELLATION_REQUESTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if requested {
            self.inner
                .queue
                .push(self.inner.operation, self.inner.stream);
        }
        requested
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == CANCELLATION_REQUESTED
    }

    /// Marks the operation as terminal.
    ///
    /// Returns `false` when cancellation was requested first. A receive driver
    /// may still report data that it had already consumed before observing the
    /// request; cancellation must not discard bytes from an ordered stream.
    pub fn complete(&self) -> bool {
        self.inner
            .state
            .swap(CANCELLATION_COMPLETED, Ordering::AcqRel)
            == CANCELLATION_ACTIVE
    }
}

#[derive(Clone)]
pub(crate) struct CancellationQueue {
    inner: Arc<CancellationQueueInner>,
}

struct CancellationQueueInner {
    values: Mutex<VecDeque<(OperationId, StreamId)>>,
    notifier: Arc<dyn Notifier>,
}

impl CancellationQueue {
    pub(crate) fn new(capacity: usize, notifier: Arc<dyn Notifier>) -> Self {
        Self {
            inner: Arc::new(CancellationQueueInner {
                values: Mutex::new(VecDeque::with_capacity(capacity)),
                notifier,
            }),
        }
    }

    fn push(&self, operation: OperationId, stream: StreamId) {
        let mut values = self
            .inner
            .values
            .lock()
            .expect("cancellation queue mutex should not be poisoned");
        values.push_back((operation, stream));
        drop(values);
        self.inner.notifier.notify();
    }

    pub(crate) fn try_pop(&self) -> Option<(OperationId, StreamId)> {
        self.inner
            .values
            .lock()
            .expect("cancellation queue mutex should not be poisoned")
            .pop_front()
    }
}

/// Thread-safe nonblocking submission handle for one driver.
pub struct SubmissionSender<T = ()> {
    driver: DriverId,
    queue: SubmissionQueue<T>,
    send_budget: Arc<ByteBudget>,
    receive_budget: Arc<ByteBudget>,
    next_operation: Arc<AtomicU64>,
    credits: CompletionCredits,
    cancellations: CancellationQueue,
}

enum SubmissionQueue<T> {
    Dedicated(queue::Sender<AdmittedSubmission>),
    Shared(SharedSender<T>),
}

impl<T> Clone for SubmissionSender<T> {
    fn clone(&self) -> Self {
        Self {
            driver: self.driver,
            queue: match &self.queue {
                SubmissionQueue::Dedicated(sender) => SubmissionQueue::Dedicated(sender.clone()),
                SubmissionQueue::Shared(sender) => SubmissionQueue::Shared(sender.clone()),
            },
            send_budget: self.send_budget.clone(),
            receive_budget: self.receive_budget.clone(),
            next_operation: self.next_operation.clone(),
            credits: self.credits.clone(),
            cancellations: self.cancellations.clone(),
        }
    }
}

/// Single-consumer command queue owned by one transport driver.
pub struct SubmissionReceiver {
    queue: queue::Receiver<AdmittedSubmission>,
    send_budget: Arc<ByteBudget>,
    receive_budget: Arc<ByteBudget>,
    cancellations: CancellationQueue,
}

/// One accepted stream operation and its reserved terminal-completion slot.
#[derive(Debug)]
pub struct AdmittedSubmission {
    submission: Submission,
    permit: CompletionPermit,
}

impl AdmittedSubmission {
    /// Returns the operation ID.
    pub const fn operation(&self) -> OperationId {
        self.submission.operation()
    }

    /// Returns the target stream.
    pub const fn stream(&self) -> StreamId {
        self.submission.stream()
    }

    /// Separates the stream operation and its completion permit.
    pub fn into_parts(self) -> (Submission, CompletionPermit) {
        (self.submission, self.permit)
    }
}

/// Constructs the application and driver halves of one bounded submission queue.
pub fn submission_queue(
    driver: DriverId,
    limits: SubmissionLimits,
    notifier: Arc<dyn Notifier>,
) -> (SubmissionSender, SubmissionReceiver) {
    let credits = CompletionCredits::new(limits.queue_capacity, notifier.clone());
    submission_queue_with_credits(driver, limits, notifier, credits)
}

/// Constructs a submission queue that shares terminal-completion admission credit.
pub fn submission_queue_with_credits(
    driver: DriverId,
    limits: SubmissionLimits,
    notifier: Arc<dyn Notifier>,
    credits: CompletionCredits,
) -> (SubmissionSender, SubmissionReceiver) {
    let cancellations = CancellationQueue::new(credits.capacity(), notifier.clone());
    let (sender, receiver) = queue::bounded(limits.queue_capacity, notifier.clone());
    let send_budget = Arc::new(ByteBudget::new(
        limits.retained_send_bytes,
        notifier.clone(),
    ));
    let receive_budget = Arc::new(ByteBudget::new(limits.posted_receive_bytes, notifier));
    (
        SubmissionSender {
            driver,
            queue: SubmissionQueue::Dedicated(sender),
            send_budget: send_budget.clone(),
            receive_budget: receive_budget.clone(),
            next_operation: Arc::new(AtomicU64::new(1)),
            credits,
            cancellations: cancellations.clone(),
        },
        SubmissionReceiver {
            queue: receiver,
            send_budget,
            receive_budget,
            cancellations,
        },
    )
}

impl<T> SubmissionSender<T> {
    pub(crate) fn from_shared(shared: SharedSubmissionState<T>) -> Self {
        let SharedSubmissionState {
            driver,
            queue,
            credits,
            cancellations,
            send_budget,
            receive_budget,
        } = shared;
        Self {
            driver,
            queue: SubmissionQueue::Shared(queue),
            send_budget,
            receive_budget,
            next_operation: Arc::new(AtomicU64::new(1)),
            credits,
            cancellations,
        }
    }

    /// Submits nonempty immutable bytes without waiting for queue, memory, or network progress.
    pub fn try_send(
        &self,
        stream: StreamId,
        bytes: Bytes,
    ) -> Result<SubmissionReceipt, TrySendError> {
        if bytes.is_empty() {
            return Err(TrySendError::Empty(bytes));
        }
        let Some(permit) = self.credits.try_acquire() else {
            return Err(TrySendError::WouldBlock {
                reason: SendBlockReason::CompletionFull,
                bytes,
            });
        };
        let Some(reservation) = self.send_budget.reserve(bytes.len()) else {
            return Err(TrySendError::WouldBlock {
                reason: SendBlockReason::RetainedBytes,
                bytes,
            });
        };
        let operation = self.next_operation();
        let submission = Submission::Send(SendSubmission {
            operation,
            stream,
            payload: RetainedBytes {
                bytes,
                reservation: Arc::new(reservation),
            },
        });
        match self.try_push(AdmittedSubmission { submission, permit }) {
            Ok(()) => Ok(SubmissionReceipt {
                operation,
                cancellation: None,
            }),
            Err(queue::TryPushError::Full(admitted)) => {
                let (submission, _) = admitted.into_parts();
                let Submission::Send(submission) = submission else {
                    unreachable!("send queue should return the submitted send")
                };
                let (_, _, payload) = submission.into_parts();
                Err(TrySendError::WouldBlock {
                    reason: SendBlockReason::QueueFull,
                    bytes: payload.into_bytes(),
                })
            }
            Err(queue::TryPushError::Closed(admitted)) => {
                let (submission, _) = admitted.into_parts();
                let Submission::Send(submission) = submission else {
                    unreachable!("send queue should return the submitted send")
                };
                let (_, _, payload) = submission.into_parts();
                Err(TrySendError::Closed(payload.into_bytes()))
            }
        }
    }

    /// Posts a caller-owned buffer without waiting for queue or network progress.
    pub fn try_receive(
        &self,
        stream: StreamId,
        buffer: BytesMut,
        options: ReceiveOptions,
    ) -> Result<SubmissionReceipt, TryReceiveError> {
        if buffer.capacity() - buffer.len() < options.min_bytes.get() {
            return Err(TryReceiveError::InvalidBuffer { buffer, options });
        }
        let Some(permit) = self.credits.try_acquire() else {
            return Err(TryReceiveError::WouldBlock {
                reason: ReceiveBlockReason::CompletionFull,
                buffer,
                options,
            });
        };
        let Some(reservation) = self.receive_budget.reserve(buffer.capacity()) else {
            return Err(TryReceiveError::WouldBlock {
                reason: ReceiveBlockReason::PostedBytes,
                buffer,
                options,
            });
        };
        let operation = self.next_operation();
        let cancellation =
            OperationCancellation::new(operation, stream, self.cancellations.clone());
        let submission = Submission::Receive(ReceiveSubmission {
            operation,
            stream,
            buffer: PostedBuffer {
                buffer,
                reservation,
            },
            options,
            cancellation: cancellation.clone(),
        });
        match self.try_push(AdmittedSubmission { submission, permit }) {
            Ok(()) => Ok(SubmissionReceipt {
                operation,
                cancellation: Some(cancellation),
            }),
            Err(queue::TryPushError::Full(admitted)) => {
                let (submission, _) = admitted.into_parts();
                let Submission::Receive(submission) = submission else {
                    unreachable!("receive queue should return the submitted receive")
                };
                let (_, _, buffer, options, _) = submission.into_parts();
                Err(TryReceiveError::WouldBlock {
                    reason: ReceiveBlockReason::QueueFull,
                    buffer: buffer.into_buffer(),
                    options,
                })
            }
            Err(queue::TryPushError::Closed(admitted)) => {
                let (submission, _) = admitted.into_parts();
                let Submission::Receive(submission) = submission else {
                    unreachable!("receive queue should return the submitted receive")
                };
                let (_, _, buffer, options, _) = submission.into_parts();
                Err(TryReceiveError::Closed {
                    buffer: buffer.into_buffer(),
                    options,
                })
            }
        }
    }

    /// Queues a local FIN after preceding stream sends.
    pub fn try_finish(&self, stream: StreamId) -> Result<SubmissionReceipt, TryControlError> {
        let operation = self.next_operation();
        self.try_control(
            Submission::Finish(FinishSubmission { operation, stream }),
            operation,
        )
    }

    /// Requests that received bytes be consumed without being copied to an application buffer.
    pub fn try_discard(
        &self,
        stream: StreamId,
        max_bytes: NonZeroUsize,
    ) -> Result<SubmissionReceipt, TryControlError> {
        let operation = self.next_operation();
        self.try_control(
            Submission::Discard(DiscardSubmission {
                operation,
                stream,
                max_bytes,
            }),
            operation,
        )
    }

    /// Resets the local send half after preceding sends.
    ///
    /// Bytes already accepted by QUIC are not retracted and may have reached the peer.
    pub fn try_reset(
        &self,
        stream: StreamId,
        error_code: u64,
    ) -> Result<SubmissionReceipt, TryControlError> {
        let operation = self.next_operation();
        self.try_control(
            Submission::Reset(ResetSubmission {
                operation,
                stream,
                error_code,
            }),
            operation,
        )
    }

    /// Stops receiving after preceding receive operations.
    pub fn try_stop(
        &self,
        stream: StreamId,
        error_code: u64,
    ) -> Result<SubmissionReceipt, TryControlError> {
        let operation = self.next_operation();
        self.try_control(
            Submission::Stop(StopSubmission {
                operation,
                stream,
                error_code,
            }),
            operation,
        )
    }

    /// Returns the visible payload bytes currently retained for accepted sends.
    pub fn retained_send_bytes(&self) -> usize {
        self.send_budget.used()
    }

    /// Returns the capacity of receive buffers currently held by the transport.
    pub fn posted_receive_bytes(&self) -> usize {
        self.receive_budget.used()
    }

    /// Returns whether the driver side of this queue is closed.
    pub fn is_closed(&self) -> bool {
        match &self.queue {
            SubmissionQueue::Dedicated(sender) => sender.is_closed(),
            SubmissionQueue::Shared(sender) => sender.is_closed(),
        }
    }

    fn try_control(
        &self,
        submission: Submission,
        operation: OperationId,
    ) -> Result<SubmissionReceipt, TryControlError> {
        let Some(permit) = self.credits.try_acquire() else {
            return Err(TryControlError::WouldBlock(
                ControlBlockReason::CompletionFull,
            ));
        };
        match self.try_push(AdmittedSubmission { submission, permit }) {
            Ok(()) => Ok(SubmissionReceipt {
                operation,
                cancellation: None,
            }),
            Err(queue::TryPushError::Full(_)) => {
                Err(TryControlError::WouldBlock(ControlBlockReason::QueueFull))
            }
            Err(queue::TryPushError::Closed(_)) => Err(TryControlError::Closed),
        }
    }

    fn next_operation(&self) -> OperationId {
        let operation = self.next_operation.fetch_add(1, Ordering::Relaxed);
        assert_ne!(operation, 0, "operation ID space exhausted");
        OperationId::new(self.driver, operation)
    }

    fn try_push(
        &self,
        admitted: AdmittedSubmission,
    ) -> Result<(), queue::TryPushError<AdmittedSubmission>> {
        match &self.queue {
            SubmissionQueue::Dedicated(sender) => sender.try_push(admitted),
            SubmissionQueue::Shared(sender) => sender
                .try_push(EndpointSubmission::Stream(admitted))
                .map_err(|error| match error {
                    queue::TryPushError::Full(EndpointSubmission::Stream(admitted)) => {
                        queue::TryPushError::Full(admitted)
                    }
                    queue::TryPushError::Closed(EndpointSubmission::Stream(admitted)) => {
                        queue::TryPushError::Closed(admitted)
                    }
                    _ => unreachable!("shared queue should return stream work"),
                }),
        }
    }
}

pub(crate) struct SharedSubmissionState<T> {
    pub(crate) driver: DriverId,
    pub(crate) queue: SharedSender<T>,
    pub(crate) credits: CompletionCredits,
    pub(crate) cancellations: CancellationQueue,
    pub(crate) send_budget: Arc<ByteBudget>,
    pub(crate) receive_budget: Arc<ByteBudget>,
}

impl SubmissionReceiver {
    /// Stops admission of new submissions while preserving accepted commands for draining.
    pub fn close(&self) {
        self.queue.close();
    }

    /// Removes the next driver submission, if one is available.
    pub fn try_pop(&self) -> Option<AdmittedSubmission> {
        self.queue.try_pop()
    }

    /// Removes one newly cancelled operation, if available.
    pub fn try_pop_cancellation(&self) -> Option<(OperationId, StreamId)> {
        self.cancellations.try_pop()
    }

    /// Returns the number of commands waiting for the driver.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether every sender is gone and every accepted command was consumed.
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

pub(crate) struct ByteBudget {
    capacity: usize,
    used: AtomicUsize,
    notifier: Arc<dyn Notifier>,
}

impl ByteBudget {
    pub(crate) fn new(capacity: NonZeroUsize, notifier: Arc<dyn Notifier>) -> Self {
        Self {
            capacity: capacity.get(),
            used: AtomicUsize::new(0),
            notifier,
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|next| *next <= self.capacity)?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => used = actual,
            }
        }
        Some(Reservation {
            budget: self.clone(),
            bytes,
        })
    }

    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

impl fmt::Debug for ByteBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByteBudget")
            .field("capacity", &self.capacity)
            .field("used", &self.used())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        assert!(
            previous >= self.bytes,
            "byte reservation release should not underflow"
        );
        self.budget.notifier.notify();
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use bytes::BytesMut;

    use super::*;
    use crate::ConnectionId;
    use crate::DriverId;
    use crate::NoopNotifier;

    const DRIVER: DriverId = DriverId::from_u16(1);
    const STREAM: StreamId = StreamId::new(ConnectionId::new(DRIVER, 2), 4);

    #[derive(Default)]
    struct CountingNotifier(AtomicUsize);

    impl Notifier for CountingNotifier {
        fn notify(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn limits(queue: usize, send: usize, receive: usize) -> SubmissionLimits {
        SubmissionLimits::new(
            NonZeroUsize::new(queue).expect("test queue limit should be nonzero"),
            NonZeroUsize::new(send).expect("test send limit should be nonzero"),
            NonZeroUsize::new(receive).expect("test receive limit should be nonzero"),
        )
    }

    #[test]
    fn retained_send_budget_lasts_until_every_view_is_dropped() {
        let (sender, receiver) = submission_queue(DRIVER, limits(4, 4, 16), Arc::new(NoopNotifier));
        sender
            .try_send(STREAM, Bytes::from_static(b"data"))
            .expect("submit send within budget");
        assert_eq!(sender.retained_send_bytes(), 4);

        let blocked = sender
            .try_send(STREAM, Bytes::from_static(b"x"))
            .expect_err("reject send beyond budget");
        assert!(matches!(
            blocked,
            TrySendError::WouldBlock {
                reason: SendBlockReason::RetainedBytes,
                ..
            }
        ));

        let (submission, _permit) = receiver.try_pop().expect("pop accepted send").into_parts();
        let Submission::Send(submission) = submission else {
            panic!("expected send submission")
        };
        let (_, _, mut payload) = submission.into_parts();
        let prefix = payload.split_to(2);
        drop(payload);
        assert_eq!(sender.retained_send_bytes(), 4);
        drop(prefix);
        assert_eq!(sender.retained_send_bytes(), 0);
    }

    #[test]
    fn releasing_a_byte_reservation_notifies_waiters() {
        let notifier = Arc::new(CountingNotifier::default());
        let budget = Arc::new(ByteBudget::new(NonZeroUsize::MIN, notifier.clone()));
        let reservation = budget.reserve(1).expect("reserve byte budget");

        drop(reservation);

        assert_eq!(notifier.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn retained_send_budget_counts_visible_payload_bytes() {
        let (sender, receiver) = submission_queue(DRIVER, limits(1, 4, 16), Arc::new(NoopNotifier));
        let allocation = Bytes::from(vec![0; 1024]);
        sender
            .try_send(STREAM, allocation.slice(..4))
            .expect("submit visible payload within budget");

        assert_eq!(sender.retained_send_bytes(), 4);
        drop(receiver);
        assert_eq!(sender.retained_send_bytes(), 0);
    }

    #[test]
    fn full_queue_returns_the_original_send_allocation() {
        let notifier = Arc::new(NoopNotifier);
        let credits = CompletionCredits::new(NonZeroUsize::new(2).unwrap(), notifier.clone());
        let (sender, _receiver) =
            submission_queue_with_credits(DRIVER, limits(1, 16, 16), notifier, credits);
        sender
            .try_finish(STREAM)
            .expect("fill command queue with finish");
        let bytes = Bytes::from_static(b"retry");
        let pointer = bytes.as_ptr();

        let error = sender
            .try_send(STREAM, bytes)
            .expect_err("full queue should reject send");
        assert!(matches!(
            error,
            TrySendError::WouldBlock {
                reason: SendBlockReason::QueueFull,
                ..
            }
        ));
        assert_eq!(error.into_bytes().as_ptr(), pointer);
        assert_eq!(sender.retained_send_bytes(), 0);
    }

    #[test]
    fn posted_receive_returns_the_same_buffer_and_releases_budget() {
        let (sender, receiver) = submission_queue(DRIVER, limits(2, 16, 8), Arc::new(NoopNotifier));
        let buffer = BytesMut::with_capacity(8);
        let pointer = buffer.as_ptr();
        sender
            .try_receive(STREAM, buffer, ReceiveOptions::default())
            .expect("post receive buffer");
        assert_eq!(sender.posted_receive_bytes(), 8);

        let (submission, _permit) = receiver.try_pop().expect("pop posted receive").into_parts();
        let Submission::Receive(submission) = submission else {
            panic!("expected receive submission")
        };
        let (_, _, buffer, _, _) = submission.into_parts();
        assert_eq!(buffer.buffer().as_ptr(), pointer);
        assert_eq!(sender.posted_receive_bytes(), 8);
        let buffer = buffer.into_buffer();
        assert_eq!(buffer.as_ptr(), pointer);
        assert_eq!(sender.posted_receive_bytes(), 0);
    }

    #[test]
    fn receive_rejects_insufficient_spare_capacity() {
        let (sender, _receiver) =
            submission_queue(DRIVER, limits(2, 16, 16), Arc::new(NoopNotifier));
        let mut buffer = BytesMut::with_capacity(4);
        buffer.extend_from_slice(b"abc");
        let options = ReceiveOptions::new(NonZeroUsize::new(2).expect("two is nonzero"));

        let error = sender
            .try_receive(STREAM, buffer, options)
            .expect_err("buffer has only one spare byte");
        assert!(matches!(error, TryReceiveError::InvalidBuffer { .. }));
        let (buffer, returned_options) = error.into_parts();
        assert_eq!(&buffer[..], b"abc");
        assert_eq!(returned_options, options);
    }

    #[test]
    fn receive_budget_rejection_returns_the_original_allocation() {
        let (sender, _receiver) =
            submission_queue(DRIVER, limits(2, 16, 4), Arc::new(NoopNotifier));
        let buffer = BytesMut::with_capacity(8);
        let pointer = buffer.as_ptr();

        let error = sender
            .try_receive(STREAM, buffer, ReceiveOptions::default())
            .expect_err("receive allocation exceeds posted-byte budget");
        assert!(matches!(
            error,
            TryReceiveError::WouldBlock {
                reason: ReceiveBlockReason::PostedBytes,
                ..
            }
        ));
        let (buffer, _) = error.into_parts();
        assert_eq!(buffer.as_ptr(), pointer);
        assert_eq!(sender.posted_receive_bytes(), 0);
    }

    #[test]
    fn dropping_driver_queue_closes_senders_and_releases_commands() {
        let (sender, receiver) =
            submission_queue(DRIVER, limits(2, 16, 16), Arc::new(NoopNotifier));
        sender
            .try_send(STREAM, Bytes::from_static(b"held"))
            .expect("queue retained send");
        drop(receiver);

        assert!(sender.is_closed());
        assert_eq!(sender.retained_send_bytes(), 0);
        let error = sender
            .try_send(STREAM, Bytes::from_static(b"closed"))
            .expect_err("closed driver should reject send");
        assert!(matches!(error, TrySendError::Closed(_)));
    }

    #[test]
    fn closing_driver_queue_rejects_new_work_but_preserves_accepted_commands() {
        let (sender, receiver) =
            submission_queue(DRIVER, limits(2, 16, 16), Arc::new(NoopNotifier));
        let accepted = sender
            .try_finish(STREAM)
            .expect("accept command before close");

        receiver.close();

        assert!(matches!(
            sender.try_finish(STREAM),
            Err(TryControlError::Closed)
        ));
        assert_eq!(
            receiver
                .try_pop()
                .expect("accepted command remains drainable")
                .operation(),
            accepted.operation()
        );
    }

    #[test]
    fn receive_cancellation_is_idempotent_and_wakes_the_driver_queue() {
        let (sender, receiver) =
            submission_queue(DRIVER, limits(2, 16, 16), Arc::new(NoopNotifier));
        let receipt = sender
            .try_receive(
                STREAM,
                BytesMut::with_capacity(8),
                ReceiveOptions::default(),
            )
            .unwrap();
        let cancellation = receipt.cancellation().unwrap();
        assert!(cancellation.cancel());
        assert!(!cancellation.cancel());
        assert_eq!(
            receiver.try_pop_cancellation(),
            Some((receipt.operation(), STREAM))
        );
        assert!(receiver.try_pop_cancellation().is_none());

        let (submission, _permit) = receiver.try_pop().unwrap().into_parts();
        let Submission::Receive(receive) = submission else {
            panic!("expected receive submission");
        };
        assert!(receive.is_cancelled());
    }

    #[test]
    fn completed_receive_cannot_enqueue_stale_cancellation() {
        let (sender, receiver) =
            submission_queue(DRIVER, limits(1, 16, 16), Arc::new(NoopNotifier));
        let receipt = sender
            .try_receive(
                STREAM,
                BytesMut::with_capacity(8),
                ReceiveOptions::default(),
            )
            .expect("post receive");
        let caller_cancellation = receipt.cancellation().expect("receive is cancellable");
        let (submission, _permit) = receiver.try_pop().expect("pop receive").into_parts();
        let Submission::Receive(receive) = submission else {
            panic!("expected receive submission");
        };
        let (_, _, _, _, driver_cancellation) = receive.into_parts();

        assert!(driver_cancellation.complete());
        assert!(!caller_cancellation.cancel());
        assert!(receiver.try_pop_cancellation().is_none());
    }

    #[test]
    fn completing_a_cancelled_receive_makes_the_token_terminal() {
        let (sender, receiver) =
            submission_queue(DRIVER, limits(1, 16, 16), Arc::new(NoopNotifier));
        let receipt = sender
            .try_receive(
                STREAM,
                BytesMut::with_capacity(8),
                ReceiveOptions::default(),
            )
            .expect("post receive");
        let caller_cancellation = receipt.cancellation().expect("receive is cancellable");
        let (submission, _permit) = receiver.try_pop().expect("pop receive").into_parts();
        let Submission::Receive(receive) = submission else {
            panic!("expected receive submission");
        };
        let (_, _, _, _, driver_cancellation) = receive.into_parts();

        assert!(caller_cancellation.cancel());
        assert!(!driver_cancellation.complete());
        assert!(!caller_cancellation.is_cancelled());
        assert!(!caller_cancellation.cancel());
    }

    #[test]
    fn empty_send_is_rejected_without_consuming_capacity() {
        let (sender, receiver) =
            submission_queue(DRIVER, limits(1, 16, 16), Arc::new(NoopNotifier));

        assert!(matches!(
            sender.try_send(STREAM, Bytes::new()),
            Err(TrySendError::Empty(bytes)) if bytes.is_empty()
        ));
        assert!(receiver.is_empty());
        sender
            .try_finish(STREAM)
            .expect("empty send should not consume queue or completion capacity");
    }
}
