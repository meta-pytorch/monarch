/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use chrysalis_transport_core::OperationId;
use chrysalis_transport_core::RetainedBytes;
use chrysalis_transport_core::StreamId;

#[derive(Debug)]
pub(crate) struct SendState {
    pub(crate) operation: OperationId,
    pub(crate) stream: StreamId,
    pub(crate) bytes: usize,
    pub(crate) acknowledged_through: u64,
    completion_ready: AtomicBool,
    abandoned: AtomicBool,
}

impl SendState {
    pub(crate) fn new(
        operation: OperationId,
        stream: StreamId,
        bytes: usize,
        acknowledged_through: u64,
    ) -> Self {
        Self {
            operation,
            stream,
            bytes,
            acknowledged_through,
            completion_ready: AtomicBool::new(bytes == 0),
            abandoned: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_completion_ready(&self) -> bool {
        self.completion_ready.load(Ordering::Acquire)
    }

    pub(crate) fn is_abandoned(&self) -> bool {
        self.abandoned.load(Ordering::Acquire)
    }

    pub(crate) fn abandon(&self) {
        self.abandoned.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
pub(crate) struct BufferLease {
    state: Arc<SendState>,
    completions: Arc<SendCompletionSink>,
}

impl BufferLease {
    pub(crate) fn new(state: Arc<SendState>, completions: Arc<SendCompletionSink>) -> Self {
        Self { state, completions }
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        self.state.completion_ready.store(true, Ordering::Release);
        self.completions.push(self.state.stream.stream());
    }
}

#[derive(Debug, Default)]
pub(crate) struct SendCompletionSink {
    streams: Mutex<VecDeque<u64>>,
}

impl SendCompletionSink {
    pub(crate) fn push(&self, stream: u64) {
        self.streams
            .lock()
            .expect("send completion sink poisoned")
            .push_back(stream);
    }

    pub(crate) fn pop(&self) -> Option<u64> {
        self.streams
            .lock()
            .expect("send completion sink poisoned")
            .pop_front()
    }
}

#[derive(Clone)]
enum Storage {
    Internal(Bytes),
    Submission(RetainedBytes),
}

impl Storage {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Internal(bytes) => bytes,
            Self::Submission(bytes) => bytes.as_bytes(),
        }
    }

    fn truncate(&mut self, length: usize) {
        match self {
            Self::Internal(bytes) => bytes.truncate(length),
            Self::Submission(bytes) => bytes.truncate(length),
        }
    }

    fn discard_prefix(&mut self, length: usize) {
        match self {
            Self::Internal(bytes) => {
                let _ = bytes.split_to(length);
            }
            Self::Submission(bytes) => {
                bytes.split_to(length);
            }
        }
    }
}

/// A cheap, splittable quiche send buffer.
#[derive(Clone)]
pub(crate) struct QuicheBuffer {
    storage: Storage,
    _lease: Option<Arc<BufferLease>>,
}

impl QuicheBuffer {
    pub(crate) fn submission(bytes: RetainedBytes, lease: Arc<BufferLease>) -> Self {
        Self {
            storage: Storage::Submission(bytes),
            _lease: Some(lease),
        }
    }
}

impl fmt::Debug for QuicheBuffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuicheBuffer")
            .field("length", &self.as_ref().len())
            .field("submission", &self._lease.is_some())
            .finish()
    }
}

impl AsRef<[u8]> for QuicheBuffer {
    fn as_ref(&self) -> &[u8] {
        self.storage.as_ref()
    }
}

impl quiche::BufSplit for QuicheBuffer {
    fn split_at(&mut self, at: usize) -> Self {
        assert!(at <= self.as_ref().len(), "split exceeds QUIC buffer");
        let mut remainder = self.clone();
        remainder.storage.discard_prefix(at);
        self.storage.truncate(at);
        remainder
    }
}

/// Buffer factory used by every connection on one endpoint.
#[derive(Clone, Debug, Default)]
pub(crate) struct BufferFactory;

impl quiche::BufFactory for BufferFactory {
    type Buf = QuicheBuffer;

    fn buf_from_slice(buffer: &[u8]) -> Self::Buf {
        QuicheBuffer {
            storage: Storage::Internal(Bytes::copy_from_slice(buffer)),
            _lease: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use bytes::Bytes;
    use chrysalis_transport_core::ConnectionId;
    use chrysalis_transport_core::DriverId;
    use chrysalis_transport_core::NoopNotifier;
    use chrysalis_transport_core::StreamId;
    use chrysalis_transport_core::Submission;
    use chrysalis_transport_core::SubmissionLimits;
    use chrysalis_transport_core::submission_queue;
    use quiche::BufSplit;

    use super::*;

    #[test]
    fn split_submission_retains_one_budget_until_every_view_drops() {
        let driver = DriverId::from_u16(1);
        let stream = StreamId::new(ConnectionId::new(driver, 1), 0);
        let limits = SubmissionLimits::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(16).unwrap(),
        );
        let (sender, receiver) = submission_queue(driver, limits, Arc::new(NoopNotifier));
        sender
            .try_send(stream, Bytes::from_static(b"abcdef"))
            .unwrap();
        let (submission, _permit) = receiver.try_pop().unwrap().into_parts();
        let Submission::Send(submission) = submission else {
            panic!("expected send submission");
        };
        let (operation, _, payload) = submission.into_parts();
        let state = Arc::new(SendState::new(operation, stream, 6, 6));
        let lease = Arc::new(BufferLease::new(
            state.clone(),
            Arc::new(SendCompletionSink::default()),
        ));
        let mut prefix = QuicheBuffer::submission(payload, lease.clone());
        let remainder = prefix.split_at(2);
        drop(lease);

        assert_eq!(prefix.as_ref(), b"ab");
        assert_eq!(remainder.as_ref(), b"cdef");
        assert_eq!(sender.retained_send_bytes(), 6);
        assert!(!state.is_completion_ready());
        drop(prefix);
        assert!(!state.is_completion_ready());
        drop(remainder);
        assert!(state.is_completion_ready());
        assert_eq!(sender.retained_send_bytes(), 0);
    }
}
