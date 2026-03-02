/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared protocol loops for simplex and duplex channels.
//!
//! Both simplex and duplex connections use the same recv/send protocol
//! logic, parameterized on a [`FrameStream`] (read side) and a
//! [`MuxWriter`] (write side). Duplex runs two instances concurrently
//! with a demuxing reader and a shared, mutex-protected writer.

use std::any::type_name;
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::io::IoSlice;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use async_trait::async_trait;
use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use bytes::Buf;
use bytes::Bytes;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::Span;

use super::ClientError;
use super::Frame;
use super::NetRxResponse;
use super::deserialize_response;
use super::framed::FrameReader;
use super::framed::FrameWrite;
use super::serialize_response;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::SendError;
use crate::channel::TxStatus;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::config;
use crate::metrics;

/// Per-tag buffer array. Each tag value (0..255) gets its own slot.
const NUM_TAGS: usize = 256;

struct DemuxState<R> {
    reader: FrameReader<R>,
    /// One buffer slot per tag value. A reader for tag `t` checks
    /// `buffered[t]`; if populated, it takes the frame without
    /// touching the underlying reader.
    buffered: Box<[Option<Bytes>; NUM_TAGS]>,
    eof: bool,
}

/// Demultiplexes a single `FrameReader` into per-tag views.
/// Buffers at most one frame per tag value.
pub(super) struct DemuxFrameReader<R> {
    inner: tokio::sync::Mutex<DemuxState<R>>,
    notify: tokio::sync::Notify,
}

impl<R: AsyncRead + Unpin + Send> DemuxFrameReader<R> {
    pub fn new(reader: FrameReader<R>) -> Self {
        // Use Box to keep the large array off the stack.
        const NONE: Option<Bytes> = None;
        Self {
            inner: tokio::sync::Mutex::new(DemuxState {
                reader,
                buffered: Box::new([NONE; NUM_TAGS]),
                eof: false,
            }),
            notify: tokio::sync::Notify::new(),
        }
    }

    async fn next_tagged(&self, tag: u8) -> io::Result<Option<Bytes>> {
        loop {
            {
                let mut state = self.inner.lock().await;
                if state.eof {
                    return Ok(None);
                }
                // Check if our tag already has a buffered frame.
                if let Some(bytes) = state.buffered[tag as usize].take() {
                    drop(state);
                    self.notify.notify_waiters();
                    return Ok(Some(bytes));
                }
                // No buffered frame for our tag — read from the underlying reader.
                match state.reader.next().await? {
                    Some((t, bytes)) if t == tag => return Ok(Some(bytes)),
                    Some((t, bytes)) => {
                        state.buffered[t as usize] = Some(bytes);
                        drop(state);
                        self.notify.notify_waiters();
                    }
                    None => {
                        state.eof = true;
                        drop(state);
                        self.notify.notify_waiters();
                        return Ok(None);
                    }
                }
            }
            self.notify.notified().await;
        }
    }
}

/// Newtype that implements `AsyncWrite` by forwarding to the guarded writer.
/// Holds an `OwnedMutexGuard` so the lock persists across await points.
pub(super) struct OwnedWriter<W>(OwnedMutexGuard<W>);

impl<W: AsyncWrite + Unpin> AsyncWrite for OwnedWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
}

/// Handle for a single in-flight frame write.
///
/// Created by [`TaggedStream::write`] (sync, no I/O). Must be driven
/// via [`drive`](Completion::drive) until it returns `Ok(())`.
///
/// Cancel safety: `drive()` is cancel-safe at every await point.
/// Dropping a `Completion` before it completes releases the lock but
/// corrupts the stream (the connection should be torn down).
pub(super) enum Completion<W: AsyncWrite + Unpin + Send, B: Buf> {
    Pending {
        writer: Arc<tokio::sync::Mutex<W>>,
        body: B,
        tag: u8,
        max_len: usize,
    },
    Acquiring {
        writer: Arc<tokio::sync::Mutex<W>>,
        body: B,
        tag: u8,
        max_len: usize,
    },
    Writing(FrameWrite<OwnedWriter<W>, B>),
    Broken,
}

impl<W: AsyncWrite + Unpin + Send, B: Buf> Completion<W, B> {
    /// Drive the write to completion. Cancel-safe at every await point.
    pub async fn drive(&mut self) -> io::Result<()> {
        loop {
            match self {
                Self::Pending { writer: _, .. } => {
                    // Transition to Acquiring (same state, just marks intent).
                    let Self::Pending {
                        writer,
                        body,
                        tag,
                        max_len,
                    } = std::mem::replace(self, Self::Broken)
                    else {
                        unreachable!()
                    };
                    *self = Self::Acquiring {
                        writer,
                        body,
                        tag,
                        max_len,
                    };
                }
                Self::Acquiring { writer, .. } => {
                    let writer_clone = Arc::clone(writer);
                    let guard = writer_clone.lock_owned().await;
                    let Self::Acquiring {
                        body, tag, max_len, ..
                    } = std::mem::replace(self, Self::Broken)
                    else {
                        unreachable!()
                    };
                    match FrameWrite::new(OwnedWriter(guard), body, max_len, tag) {
                        Ok(fw) => *self = Self::Writing(fw),
                        Err((_owned, e)) => {
                            *self = Self::Broken;
                            return Err(e);
                        }
                    }
                }
                Self::Writing(fw) => {
                    fw.send().await?;
                    let Self::Writing(fw) = std::mem::replace(self, Self::Broken) else {
                        unreachable!()
                    };
                    let _ = fw.complete(); // drops OwnedWriter → releases lock
                    return Ok(());
                }
                Self::Broken => panic!("Completion: illegal state"),
            }
        }
    }
}

/// Bidirectional frame channel for a single mux tag.
///
/// Reads are demuxed: only frames whose tag matches are returned.
/// Writes are muxed: the tag byte is written into the frame header.
///
/// Multiple `TaggedStream`s can coexist (sharing the underlying
/// reader and writer) as long as they use different tags.
pub(super) struct TaggedStream<R, W> {
    demux: Arc<DemuxFrameReader<R>>,
    writer: Arc<tokio::sync::Mutex<W>>,
    tag: u8,
    max_frame_len: usize,
}

impl<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send> TaggedStream<R, W> {
    /// Read the next frame body for this tag. Cancel-safe.
    pub async fn next(&self) -> io::Result<Option<Bytes>> {
        self.demux.next_tagged(self.tag).await
    }

    /// Begin a frame write. Sync (no I/O). Returns a [`Completion`]
    /// that must be driven via [`Completion::drive`].
    pub fn write<B: Buf>(&self, body: B) -> Completion<W, B> {
        Completion::Pending {
            writer: Arc::clone(&self.writer),
            body,
            tag: self.tag,
            max_len: self.max_frame_len,
        }
    }

    /// Maximum frame body length accepted by this stream's writer.
    pub fn max_frame_len(&self) -> usize {
        self.max_frame_len
    }
}

/// Tagged frame multiplexer over a split connection.
///
/// A `Mux` wraps a `DemuxFrameReader` (read side) and a shared
/// writer (write side). Call [`stream`](Mux::stream) to get a
/// [`TaggedStream`] for a specific tag byte.
///
/// For simplex, use `mux.stream(0)`.
/// For duplex, use `mux.stream(Side::A)` and `mux.stream(Side::B)`.
pub(super) struct Mux<R, W> {
    demux: Arc<DemuxFrameReader<R>>,
    writer: Arc<tokio::sync::Mutex<W>>,
    max_frame_len: usize,
}

impl<R: AsyncRead + Unpin + Send, W> Mux<R, W> {
    /// Create a new Mux from a reader and writer half.
    pub fn new(reader: R, writer: W, max_frame_len: usize) -> Self {
        Self {
            demux: Arc::new(DemuxFrameReader::new(FrameReader::new(
                reader,
                max_frame_len,
            ))),
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            max_frame_len,
        }
    }

    /// Create a [`TaggedStream`] for the given tag byte.
    pub fn stream(&self, tag: u8) -> TaggedStream<R, W> {
        TaggedStream {
            demux: Arc::clone(&self.demux),
            writer: Arc::clone(&self.writer),
            tag,
            max_frame_len: self.max_frame_len,
        }
    }
}

impl<R, W: AsyncWrite + Unpin> Mux<R, W> {
    /// Shutdown the underlying writer. Best-effort.
    pub async fn shutdown(self) {
        let mut w = self.writer.lock().await;
        let _ = w.shutdown().await;
    }
}

pub(super) struct QueuedMessage<M: RemoteMessage> {
    seq: u64,
    message: serde_multipart::Message,
    received_at: Instant,
    sent_at: Option<Instant>,
    return_channel: oneshot::Sender<SendError<M>>,
}

impl<M: RemoteMessage> fmt::Display for QueuedMessage<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rcv_secs = self.received_at.elapsed().as_secs();
        match self.sent_at {
            Some(s) => {
                write!(
                    f,
                    "(seq={}, since_rcv={}sec, since_sent={}sec)",
                    self.seq,
                    rcv_secs,
                    s.elapsed().as_secs()
                )
            }
            None => {
                write!(f, "(seq={}, since_rcv={}sec)", self.seq, rcv_secs)
            }
        }
    }
}

impl<M: RemoteMessage> QueuedMessage<M> {
    /// Attempt to deserialize this queued frame as a
    /// `Frame::Message<M>` and return it to the original
    /// sender. Falls back to logging if the frame is not a
    /// message or deserialization fails.
    pub(super) fn try_return(self, reason: Option<String>) {
        match serde_multipart::deserialize_bincode::<Frame<M>>(self.message) {
            Ok(Frame::Message(_, msg)) => {
                let _ = self.return_channel.send(SendError {
                    error: ChannelError::Closed,
                    message: msg,
                    reason,
                });
            }
            Err(_e) => {
                tracing::warn!(
                    seq = self.seq,
                    "failed to deserialize queued frame for return"
                );
            }
        }
    }
}

pub(super) struct MessageDeque<M: RemoteMessage>(VecDeque<QueuedMessage<M>>);

impl<M: RemoteMessage> MessageDeque<M> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn front(&self) -> Option<&QueuedMessage<M>> {
        self.0.front()
    }

    fn back(&self) -> Option<&QueuedMessage<M>> {
        self.0.back()
    }

    fn num_bytes_queued(&self) -> usize {
        self.0.iter().map(|m| m.message.len()).sum()
    }
}

impl<M: RemoteMessage> Deref for MessageDeque<M> {
    type Target = VecDeque<QueuedMessage<M>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<M: RemoteMessage> DerefMut for MessageDeque<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Only show the first and last N messages, and display how many are
// omitted in the middle.
impl<M: RemoteMessage> fmt::Display for MessageDeque<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_msg<M: RemoteMessage>(
            f: &mut fmt::Formatter<'_>,
            i: usize,
            msg: &QueuedMessage<M>,
        ) -> fmt::Result {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", msg)
        }

        let len = self.0.len();
        let n: usize = 3;

        write!(f, "[")?;

        if len <= n * 2 {
            for (i, msg) in self.0.iter().enumerate() {
                write_msg(f, i, msg)?;
            }
        } else {
            // first N
            for (i, msg) in self.0.iter().take(n).enumerate() {
                write_msg(f, i, msg)?;
            }
            // middle
            write!(f, ", ... omit {} messages ..., ", len - 2 * n)?;
            // last N
            for (i, msg) in self.0.iter().skip(len - n).enumerate() {
                write_msg(f, i, msg)?;
            }
        }

        write!(f, "]")
    }
}

pub(super) struct Outbox<M: RemoteMessage> {
    /// Seq number of the next new message. Requeued unacked messages
    /// keep their already assigned seq numbers.
    pub(super) next_seq: u64,
    pub(super) deque: MessageDeque<M>,
    pub(super) log_id: String,
    pub(super) dest_addr: ChannelAddr,
    pub(super) session_id: u64,
}

impl<M: RemoteMessage> Outbox<M> {
    pub(super) fn new(log_id: String, dest_addr: ChannelAddr, session_id: u64) -> Self {
        Self {
            next_seq: 0,
            deque: MessageDeque(VecDeque::new()),
            log_id,
            dest_addr,
            session_id,
        }
    }

    pub(super) fn is_expired(&self) -> bool {
        match self.deque.front() {
            None => false,
            Some(msg) => {
                msg.received_at.elapsed()
                    > hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }

    pub(super) fn front_message(&self) -> Option<serde_multipart::Message> {
        self.deque.front().map(|msg| msg.message.clone())
    }

    pub(super) fn front_size(&self) -> Option<usize> {
        self.deque.front().map(|msg| msg.message.frame_len())
    }

    pub(super) fn pop_front(&mut self) -> Option<QueuedMessage<M>> {
        self.deque.pop_front()
    }

    pub(super) fn push_back(
        &mut self,
        (message, return_channel, received_at): (M, oneshot::Sender<SendError<M>>, Instant),
    ) -> Result<(), String> {
        assert!(
            self.deque.back().is_none_or(|msg| msg.seq < self.next_seq),
            "{}: unexpected: seq should be in ascending order, but got {} vs {}",
            self.log_id,
            self.deque
                .back()
                .map_or("None".to_string(), |m| m.to_string()),
            self.next_seq
        );

        let frame = Frame::Message(self.next_seq, message);
        let message = serde_multipart::serialize_bincode(&frame)
            .map_err(|e| format!("serialization error: {e}"))?;
        let message_size = message.frame_len();
        metrics::REMOTE_MESSAGE_SEND_SIZE.record(message_size as f64, &[]);

        // Track throughput for this channel pair
        metrics::CHANNEL_THROUGHPUT_BYTES.add(
            message_size as u64,
            hyperactor_telemetry::kv_pairs!(
                "dest" => self.dest_addr.to_string(),
                "session_id" => self.session_id.to_string(),
            ),
        );
        metrics::CHANNEL_THROUGHPUT_MESSAGES.add(
            1,
            hyperactor_telemetry::kv_pairs!(
                "dest" => self.dest_addr.to_string(),
                "session_id" => self.session_id.to_string(),
            ),
        );

        self.deque.push_back(QueuedMessage {
            seq: self.next_seq,
            message,
            received_at,
            sent_at: None,
            return_channel,
        });
        self.next_seq += 1;
        Ok(())
    }

    pub(super) fn requeue_unacked(&mut self, unacked: MessageDeque<M>) {
        if let (Some(last), Some(first)) = (unacked.back(), self.deque.front()) {
            assert!(
                last.seq < first.seq,
                "{}: seq should be in ascending order, but got {} vs {}",
                self.log_id,
                last.seq,
                first.seq,
            );
        }

        let mut outbox = unacked;
        outbox.append(&mut self.deque);
        self.deque = outbox;
    }
}

impl<M: RemoteMessage> fmt::Display for Outbox<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(next_seq: {}, deque: {})", self.next_seq, self.deque)
    }
}

/// Acked sequence number with timestamp.
#[derive(Debug, Clone)]
pub(super) struct AckedSeq(pub(super) u64, pub(super) Instant);

impl fmt::Display for AckedSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let acked_secs = self.1.elapsed().as_secs();
        write!(f, "(seq={}, since_acked={}sec)", self.0, acked_secs)
    }
}

pub(super) struct Unacked<M: RemoteMessage> {
    pub(super) deque: MessageDeque<M>,
    pub(super) largest_acked: Option<AckedSeq>,
    pub(super) log_id: String,
}

impl<M: RemoteMessage> Unacked<M> {
    pub(super) fn new(largest_acked: Option<AckedSeq>, log_id: String) -> Self {
        Self {
            deque: MessageDeque(VecDeque::new()),
            largest_acked,
            log_id,
        }
    }

    pub(super) fn push_back(&mut self, message: QueuedMessage<M>) {
        assert!(
            self.deque.back().is_none_or(|msg| msg.seq < message.seq),
            "{}: seq should be in ascending order, but got {} vs {}",
            self.log_id,
            self.deque
                .back()
                .map_or("None".to_string(), |m| m.to_string()),
            message.seq
        );

        if let Some(AckedSeq(largest, _)) = self.largest_acked {
            if message.seq <= largest {
                return;
            }
        }

        self.deque.push_back(message);
    }

    /// Remove acked messages from the deque.
    pub(super) fn prune(
        &mut self,
        acked: u64,
        acked_at: Instant,
        dest_addr: &ChannelAddr,
        session_id: u64,
    ) {
        assert!(
            self.largest_acked.as_ref().map_or(0, |i| i.0) <= acked,
            "{}: received out-of-order ack; received: {}; stored largest: {}",
            self.log_id,
            acked,
            self.largest_acked
                .as_ref()
                .map_or("None".to_string(), |l| l.to_string()),
        );

        self.largest_acked = Some(AckedSeq(acked, acked_at));
        let deque = &mut self.deque;
        while let Some(msg) = deque.front() {
            if msg.seq <= acked {
                let msg: QueuedMessage<M> = deque.pop_front().unwrap();
                let latency_micros = msg.received_at.elapsed().as_micros() as i64;
                metrics::CHANNEL_LATENCY_MICROS.record(
                    latency_micros as f64,
                    hyperactor_telemetry::kv_pairs!(
                        "dest" => dest_addr.to_string(),
                        "session_id" => session_id.to_string(),
                    ),
                );
            } else {
                break;
            }
        }
    }

    pub(super) fn is_expired(&self) -> bool {
        matches!(
            self.deque.front(),
            Some(msg) if msg.received_at.elapsed() > hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
        )
    }

    /// Return when the oldest message has not been acked within the
    /// timeout limit.
    pub(super) async fn wait_for_timeout(&self) {
        match self.deque.front() {
            Some(msg) => {
                RealClock
                    .sleep_until(
                        msg.received_at
                            + hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT),
                    )
                    .await
            }
            None => std::future::pending::<()>().await,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }
}

impl<M: RemoteMessage> fmt::Display for Unacked<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(deque: {}, largest_acked: {})",
            self.deque,
            self.largest_acked
                .as_ref()
                .map_or("None".to_string(), |l| l.to_string())
        )
    }
}

pub(super) struct Deliveries<M: RemoteMessage> {
    pub(super) outbox: Outbox<M>,
    pub(super) unacked: Unacked<M>,
}

impl<M: RemoteMessage> Deliveries<M> {
    /// Move all unacked messages back to the outbox for retransmission.
    /// Preserves `largest_acked`.
    pub(super) fn requeue_unacked(&mut self) {
        let old_deque = std::mem::replace(&mut self.unacked.deque, MessageDeque(VecDeque::new()));
        self.outbox.requeue_unacked(old_deque);
    }
}

impl<M: RemoteMessage> fmt::Display for Deliveries<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(outbox: {}, unacked: {})", self.outbox, self.unacked)
    }
}

/// Outcome of the receive protocol loop.
pub(super) enum RecvResult {
    Eof,
    Cancelled,
    SequenceError(String),
}

/// Receive protocol loop: read message frames, validate sequence
/// numbers, deliver to the application, and periodically send acks.
///
/// This is the shared implementation used by both simplex (`server.rs`)
/// and duplex (`duplex.rs`).
pub(super) async fn recv_loop<
    M: RemoteMessage,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
>(
    stream: &TaggedStream<R, W>,
    deliver_tx: &mpsc::Sender<M>,
    next: &mut Next,
    cancel: CancellationToken,
) -> Result<RecvResult, anyhow::Error> {
    let ack_time_interval = hyperactor_config::global::get(config::MESSAGE_ACK_TIME_INTERVAL);
    let ack_msg_interval: u64 =
        hyperactor_config::global::get(config::MESSAGE_ACK_EVERY_N_MESSAGES);

    let mut last_ack_time = RealClock.now();
    let mut pending_ack: Option<(Completion<W, Bytes>, u64)> = None;

    loop {
        let ack_behind = next.ack + ack_msg_interval <= next.seq
            || (next.ack < next.seq && last_ack_time.elapsed() > ack_time_interval);

        // Begin ack write if idle and behind.
        if pending_ack.is_none() && ack_behind {
            let ack = serialize_response(NetRxResponse::Ack(next.seq - 1))?;
            pending_ack = Some((stream.write(ack), next.seq));
        }

        tokio::select! {
            biased;

            // Drive ack write to completion.
            result = async { pending_ack.as_mut().unwrap().0.drive().await },
                if pending_ack.is_some() => {
                match result {
                    Ok(()) => {
                        let acked_seq = pending_ack.take().unwrap().1;
                        last_ack_time = RealClock.now();
                        next.ack = acked_seq;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            // Ack timer tick: loop back so the ack_behind check fires.
            _ = RealClock.sleep_until(last_ack_time + ack_time_interval),
                if next.ack < next.seq => {}

            _ = cancel.cancelled() => {
                return Ok(RecvResult::Cancelled);
            }

            bytes_result = stream.next() => {
                let bytes = match bytes_result {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => return Ok(RecvResult::Eof),
                    Err(e) => return Err(e.into()),
                };

                let message = serde_multipart::Message::from_framed(bytes)?;
                match serde_multipart::deserialize_bincode::<Frame<M>>(message) {
                    Ok(Frame::Message(seq, _)) if seq < next.seq => {
                        // Retransmit — ignore.
                        tracing::debug!(seq, next_seq = next.seq, "ignoring retransmit");
                    }
                    Ok(Frame::Message(seq, msg)) => {
                        if seq > next.seq {
                            let error_msg = format!(
                                "out-of-sequence message, expected seq {}, got {}",
                                next.seq, seq
                            );
                            tracing::error!(seq, next_seq = next.seq, "{}", error_msg);
                            return Ok(RecvResult::SequenceError(error_msg));
                        }
                        deliver_tx
                            .send(msg)
                            .await
                            .map_err(|_| anyhow::anyhow!("deliver channel closed"))?;
                        next.seq = seq + 1;
                    }
                    Err(e) => {
                        return Err(anyhow::Error::from(e).context(format!(
                            "deserialize message with M = {}",
                            type_name::<M>(),
                        )));
                    }
                }
            }
        }
    }
}

/// Outcome of the send protocol loop.
pub(super) enum SendLoopResult {
    Eof,
    Cancelled,
    AppClosed,
    Rejected(String),
    ServerClosed,
    DeliveryTimeout,
    OversizedFrame(String),
    Error(anyhow::Error),
}

/// Used to bookkeep message processing states.
#[derive(Clone)]
pub(super) struct Next {
    /// The next expected inbound sequence number.
    pub(super) seq: u64,
    /// The sequence number up to which we have acked.
    pub(super) ack: u64,
}

impl fmt::Display for Next {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(seq: {}, ack: {})", self.seq, self.ack)
    }
}

/// Send protocol loop: accept messages from the application,
/// serialize, write via [`Completion`], read ack responses, and
/// manage the outbox/unacked buffers.
///
/// This is the shared implementation used by both simplex (`client.rs`)
/// and duplex (`duplex.rs`).
pub(super) async fn send_loop<M, R, W>(
    stream: &TaggedStream<R, W>,
    deliveries: &mut Deliveries<M>,
    receiver: &mut mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
    cancel: CancellationToken,
) -> SendLoopResult
where
    M: RemoteMessage,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let mut pending: Option<Completion<W, serde_multipart::Frame>> = None;

    loop {
        // Begin write if idle and outbox has messages.
        if pending.is_none() && !deliveries.outbox.is_empty() {
            let len = deliveries.outbox.front_size().expect("not empty");
            let max = stream.max_frame_len();
            if len > max {
                let reason = format!(
                    "rejecting oversize frame: len={} > max={}. \
                    ack will not arrive before timeout; increase CODEC_MAX_FRAME_LENGTH to allow.",
                    len, max
                );
                deliveries
                    .outbox
                    .pop_front()
                    .expect("not empty")
                    .try_return(Some(reason.clone()));
                return SendLoopResult::OversizedFrame(reason);
            }
            let message = deliveries.outbox.front_message().expect("not empty");
            pending = Some(stream.write(message.framed()));
        }

        tokio::select! {
            biased;

            // Read acks/responses from the peer.
            ack_result = stream.next() => {
                match ack_result {
                    Ok(Some(buffer)) => {
                        let response = match deserialize_response(buffer) {
                            Ok(r) => r,
                            Err(e) => return SendLoopResult::Error(e.into()),
                        };
                        match response {
                            NetRxResponse::Ack(ack) => {
                                deliveries.unacked.prune(
                                    ack,
                                    RealClock.now(),
                                    &deliveries.outbox.dest_addr,
                                    deliveries.outbox.session_id,
                                );
                            }
                            NetRxResponse::Reject(reason) => {
                                return SendLoopResult::Rejected(reason);
                            }
                            NetRxResponse::Closed => {
                                return SendLoopResult::ServerClosed;
                            }
                        }
                    }
                    Ok(None) => return SendLoopResult::Eof,
                    Err(e) => return SendLoopResult::Error(e.into()),
                }
            }

            // Delivery timeout on oldest unacked message.
            _ = deliveries.unacked.wait_for_timeout(), if !deliveries.unacked.is_empty() => {
                return SendLoopResult::DeliveryTimeout;
            }

            // Drive frame write to completion.
            send_result = async { pending.as_mut().unwrap().drive().await },
                if pending.is_some() => {
                match send_result {
                    Ok(()) => {
                        pending = None;
                        let mut message = deliveries.outbox.pop_front()
                            .expect("outbox should not be empty");
                        message.sent_at = Some(RealClock.now());
                        deliveries.unacked.push_back(message);
                    }
                    Err(e) => return SendLoopResult::Error(e.into()),
                }
            }

            // Accept new messages from the application (only when
            // outbox is empty so queued messages are sent first).
            msg = receiver.recv(), if deliveries.outbox.is_empty() => {
                match msg {
                    Some(item) => {
                        if let Err(e) = deliveries.outbox.push_back(item) {
                            return SendLoopResult::Error(anyhow::anyhow!(e));
                        }
                    }
                    None => return SendLoopResult::AppClosed,
                }
            }

            _ = cancel.cancelled() => {
                return SendLoopResult::Cancelled;
            }
        }
    }
}

/// Captures the two things that differ between simplex and duplex
/// client sessions: how to connect and what to run when connected.
#[async_trait]
pub(super) trait SessionConnector<M: RemoteMessage>: Send {
    /// Opaque connected state (holds framing).
    type Connected: Send;

    fn dest(&self) -> ChannelAddr;
    fn session_id(&self) -> u64;

    /// Whether to connect on demand (after the first outbound message
    /// arrives). Simplex returns true; duplex returns false so it can
    /// start receiving immediately.
    fn on_demand(&self) -> bool {
        true
    }

    /// Establish connection + set up framing.
    async fn connect(&mut self) -> Result<Self::Connected, ClientError>;

    /// Run the protocol. Simplex: send_loop. Duplex: select!{send_loop, recv_loop}.
    async fn run_connected(
        &mut self,
        connected: &Self::Connected,
        deliveries: &mut Deliveries<M>,
        receiver: &mut mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
        cancel: CancellationToken,
    ) -> SendLoopResult;

    /// Shut down the connection (best-effort writer flush + close).
    async fn shutdown(connected: Self::Connected);
}

enum State<M: RemoteMessage> {
    /// Channel is running.
    Running(Deliveries<M>),
    /// Message delivery not possible.
    Closing {
        deliveries: Deliveries<M>,
        reason: String,
    },
}

impl<M: RemoteMessage> State<M> {
    fn is_closing(&self) -> bool {
        matches!(self, Self::Closing { .. })
    }

    fn init(log_id: String, dest_addr: ChannelAddr, session_id: u64) -> Self {
        Self::Running(Deliveries {
            outbox: Outbox::new(log_id.clone(), dest_addr, session_id),
            unacked: Unacked::new(None, log_id),
        })
    }

    fn deliveries(&self) -> &Deliveries<M> {
        match self {
            Self::Running(deliveries) => deliveries,
            Self::Closing { deliveries, .. } => deliveries,
        }
    }
}

impl<M: RemoteMessage> fmt::Display for State<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Running(deliveries) => {
                write!(f, "Running(deliveries: {})", deliveries)
            }
            State::Closing { deliveries, reason } => {
                write!(f, "Closing(deliveries: {}, reason: {})", deliveries, reason)
            }
        }
    }
}

enum Conn<C: Send> {
    /// Disconnected.
    Disconnected {
        backoff: Box<dyn Backoff + Send>,
        first_failure_at: Instant,
    },
    /// Connected and ready to go.
    Connected(C),
}

impl<C: Send> Conn<C> {
    fn is_connected(&self) -> bool {
        matches!(self, Self::Connected(_))
    }

    fn reconnect_with_default() -> Self {
        Self::Disconnected {
            backoff: Box::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_interval(Duration::from_millis(1))
                    .with_multiplier(2.0)
                    .with_randomization_factor(0.1)
                    .with_max_interval(Duration::from_millis(1000))
                    .with_max_elapsed_time(None)
                    .build(),
            ),
            first_failure_at: RealClock.now(),
        }
    }

    fn reconnect(backoff: impl Backoff + Send + 'static, first_failure_at: Instant) -> Self {
        Self::Disconnected {
            backoff: Box::new(backoff),
            first_failure_at,
        }
    }
}

/// Main client loop. Drives the state machine through connect/send/reconnect
/// cycles until the channel closes.
pub(super) async fn client_run<M: RemoteMessage, D: SessionConnector<M>>(
    mut connector: D,
    mut receiver: mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
    notify: Option<watch::Sender<TxStatus>>,
) {
    let session_id = connector.session_id();
    let log_id = format!("session {}.{:016x}", connector.dest(), session_id);
    let dest = connector.dest();
    let mut state: State<M> = State::init(log_id.clone(), dest.clone(), session_id);
    let mut conn: Conn<D::Connected> = Conn::reconnect_with_default();

    let (state, conn) = loop {
        let span = state_span(&state, &conn, session_id, &connector);

        (state, conn) = step(state, conn, &log_id, &mut connector, &mut receiver)
            .instrument(span)
            .await;

        if state.is_closing() {
            break (state, conn);
        }

        if let Conn::Disconnected {
            ref mut backoff, ..
        } = conn
        {
            RealClock.sleep(backoff.next_backoff().unwrap()).await;
        }
    };

    let span = state_span(&state, &conn, session_id, &connector);

    tracing::info!(
        parent: &span,
        dest = %dest,
        session_id = session_id,
        "NetTx exited its loop with state: {}", state
    );

    match state {
        State::Closing {
            deliveries:
                Deliveries {
                    mut outbox,
                    mut unacked,
                },
            reason,
        } => {
            receiver.close();
            unacked
                .deque
                .drain(..)
                .chain(outbox.deque.drain(..))
                .for_each(|queued| queued.try_return(Some(reason.clone())));
            while let Ok((msg, return_channel, _)) = receiver.try_recv() {
                let _ = return_channel.send(SendError {
                    error: ChannelError::Closed,
                    message: msg,
                    reason: Some(reason.clone()),
                });
            }
        }
        _ => (),
    }

    if let Some(notify) = notify {
        let _ = notify.send(TxStatus::Closed);
    }

    match conn {
        Conn::Connected(c) => D::shutdown(c).await,
        Conn::Disconnected { .. } => (),
    };

    tracing::info!(
        parent: &span,
        dest = %dest,
        session_id = session_id,
        "client_run exits"
    );
}

fn state_span<M, C, D>(state: &State<M>, conn: &Conn<C>, session_id: u64, connector: &D) -> Span
where
    M: RemoteMessage,
    C: Send,
    D: SessionConnector<M>,
{
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return Span::none();
    }

    let deliveries = state.deliveries();
    if !tracing::enabled!(tracing::Level::TRACE) {
        return hyperactor_telemetry::context_span!(
            "net i/o loop",
            dest = %connector.dest(),
            session_id = session_id,
            next_seq = deliveries.outbox.next_seq,
        );
    }

    use valuable::NamedField;
    use valuable::Structable;
    use valuable::Tuplable;
    use valuable::Valuable;
    use valuable::Value;

    struct TimestampValue(Instant);

    impl From<Instant> for TimestampValue {
        fn from(timestamp: Instant) -> TimestampValue {
            TimestampValue(timestamp)
        }
    }

    impl Valuable for TimestampValue {
        fn as_value(&self) -> Value<'_> {
            Value::Tuplable(self)
        }

        fn visit(&self, visit: &mut dyn valuable::Visit) {
            let elapsed = humantime::format_duration(self.0.elapsed()).to_string();
            visit.visit_unnamed_fields(&[Value::String(&elapsed)]);
        }
    }

    impl Tuplable for TimestampValue {
        fn definition(&self) -> valuable::TupleDef {
            valuable::TupleDef::new_static(1)
        }
    }

    #[derive(Valuable)]
    struct QueueEntryValue {
        seq: u64,
        since_received: TimestampValue,
        since_sent: Option<TimestampValue>,
    }

    impl<M: RemoteMessage> From<&QueuedMessage<M>> for QueueEntryValue {
        fn from(m: &QueuedMessage<M>) -> QueueEntryValue {
            QueueEntryValue {
                seq: m.seq,
                since_received: m.received_at.into(),
                since_sent: m.sent_at.map(TimestampValue::from),
            }
        }
    }

    #[derive(Valuable)]
    enum QueueValue {
        Empty,
        NonEmpty {
            len: usize,
            num_bytes_queued: usize,
            front: QueueEntryValue,
            back: QueueEntryValue,
        },
    }

    impl<M: RemoteMessage> From<&MessageDeque<M>> for QueueValue {
        fn from(q: &MessageDeque<M>) -> QueueValue {
            if q.is_empty() {
                return QueueValue::Empty;
            }

            QueueValue::NonEmpty {
                len: q.len(),
                num_bytes_queued: q.num_bytes_queued(),
                front: q.front().unwrap().into(),
                back: q.back().unwrap().into(),
            }
        }
    }

    struct AckedSeqValue(AckedSeq);

    static ACKED_SEQ_FIELDS: &[NamedField<'static>] =
        &[NamedField::new("seq"), NamedField::new("timestamp")];

    impl Valuable for AckedSeqValue {
        fn as_value(&self) -> Value<'_> {
            Value::Structable(self)
        }

        fn visit(&self, visit: &mut dyn valuable::Visit) {
            let AckedSeq(seq, timestamp) = &self.0;
            visit.visit_named_fields(&valuable::NamedValues::new(
                ACKED_SEQ_FIELDS,
                &[
                    seq.as_value(),
                    Value::String(&humantime::format_duration(timestamp.elapsed()).to_string()),
                ],
            ));
        }
    }

    impl Structable for AckedSeqValue {
        fn definition(&self) -> valuable::StructDef<'_> {
            valuable::StructDef::new_static("AckedSeq", valuable::Fields::Named(ACKED_SEQ_FIELDS))
        }
    }

    let largest_acked = deliveries
        .unacked
        .largest_acked
        .as_ref()
        .map(|acked_seq| AckedSeqValue(acked_seq.clone()));

    hyperactor_telemetry::context_span!(
        "net i/o loop",
        dest = %connector.dest(),
        session_id = session_id,
        connected = conn.is_connected(),
        next_seq = deliveries.outbox.next_seq,
        largest_acked = largest_acked.as_value(),
        outbox = QueueValue::from(&deliveries.outbox.deque).as_value(),
        unacked = QueueValue::from(&deliveries.unacked.deque).as_value(),
    )
}

async fn step<M, D>(
    state: State<M>,
    conn: Conn<D::Connected>,
    log_id: &str,
    connector: &mut D,
    receiver: &mut mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
) -> (State<M>, Conn<D::Connected>)
where
    M: RemoteMessage,
    D: SessionConnector<M>,
{
    let session_id = connector.session_id();

    match (state, conn) {
        // Lazy connection: wait for the first message before connecting.
        // Only applies when the connector opts in (simplex). Duplex skips
        // this to start receiving immediately.
        (
            State::Running(Deliveries {
                mut outbox,
                unacked,
            }),
            conn,
        ) if connector.on_demand() && outbox.is_empty() && unacked.is_empty() => {
            match receiver.recv().await {
                Some(msg) => match outbox.push_back(msg) {
                    Ok(()) => {
                        let running = State::Running(Deliveries { outbox, unacked });
                        (running, conn)
                    }
                    Err(err) => {
                        let error_msg = "failed to push message to outbox";
                        tracing::error!(
                            dest = %connector.dest(),
                            session_id = session_id,
                            error = %err,
                            "{}", error_msg
                        );
                        (
                            State::Closing {
                                deliveries: Deliveries { outbox, unacked },
                                reason: format!("{log_id}: {error_msg}: {err}"),
                            },
                            conn,
                        )
                    }
                },
                None => (
                    State::Closing {
                        deliveries: Deliveries { outbox, unacked },
                        reason: "NetTx is dropped".to_string(),
                    },
                    conn,
                ),
            }
        }

        // Connected: delegate to connector.run_connected().
        (State::Running(mut deliveries), Conn::Connected(connected)) => {
            let ct = CancellationToken::new();
            let result = connector
                .run_connected(&connected, &mut deliveries, receiver, ct)
                .await;

            match result {
                SendLoopResult::Eof => (State::Running(deliveries), Conn::reconnect_with_default()),
                SendLoopResult::AppClosed => (
                    State::Closing {
                        deliveries,
                        reason: "NetTx is dropped".to_string(),
                    },
                    Conn::Connected(connected),
                ),
                SendLoopResult::Rejected(reason) => {
                    let error_msg = format!("server rejected connection due to: {reason}");
                    tracing::error!(
                        dest = %connector.dest(),
                        session_id = session_id,
                        "{}", error_msg
                    );
                    (
                        State::Closing {
                            deliveries,
                            reason: error_msg,
                        },
                        Conn::reconnect_with_default(),
                    )
                }
                SendLoopResult::ServerClosed => {
                    let msg = "server closed the channel".to_string();
                    tracing::info!(
                        dest = %connector.dest(),
                        session_id = session_id,
                        "{}", msg
                    );
                    (
                        State::Closing {
                            deliveries,
                            reason: msg,
                        },
                        Conn::reconnect_with_default(),
                    )
                }
                SendLoopResult::DeliveryTimeout => {
                    let error_msg = format!(
                        "failed to receive ack within timeout {:?}; link is currently connected",
                        hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT),
                    );
                    tracing::error!(
                        dest = %connector.dest(),
                        session_id = session_id,
                        "{}", error_msg,
                    );
                    (
                        State::Closing {
                            deliveries,
                            reason: format!("{log_id}: {error_msg}"),
                        },
                        Conn::Connected(connected),
                    )
                }
                SendLoopResult::OversizedFrame(reason) => {
                    tracing::error!(
                        dest = %connector.dest(),
                        session_id = session_id,
                        "oversized frame was rejected. closing channel",
                    );
                    (
                        State::Closing { deliveries, reason },
                        Conn::Connected(connected),
                    )
                }
                SendLoopResult::Cancelled => (
                    State::Closing {
                        deliveries,
                        reason: "cancelled".to_string(),
                    },
                    Conn::Connected(connected),
                ),
                SendLoopResult::Error(err) => {
                    tracing::info!(
                        dest = %connector.dest(),
                        session_id,
                        error = %err,
                        "send loop error"
                    );
                    metrics::CHANNEL_ERRORS.add(
                        1,
                        hyperactor_telemetry::kv_pairs!(
                            "dest" => connector.dest().to_string(),
                            "session_id" => session_id.to_string(),
                            "error_type" => metrics::ChannelErrorType::SendError.as_str(),
                        ),
                    );
                    (State::Running(deliveries), Conn::reconnect_with_default())
                }
            }
        }

        // Disconnected with messages to send.
        (
            State::Running(mut deliveries),
            Conn::Disconnected {
                mut backoff,
                first_failure_at,
            },
        ) => {
            if deliveries.outbox.is_expired() {
                let error_msg = format!(
                    "failed to deliver message within timeout {:?}",
                    hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
                );
                tracing::error!(
                    dest = %connector.dest(),
                    session_id,
                    "{}", error_msg
                );
                (
                    State::Closing {
                        deliveries,
                        reason: format!("{log_id}: {error_msg}"),
                    },
                    Conn::reconnect_with_default(),
                )
            } else if deliveries.unacked.is_expired() {
                let error_msg = format!(
                    "failed to receive ack within timeout {:?}; link is currently broken",
                    hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT),
                );
                tracing::error!(
                    dest = %connector.dest(),
                    session_id = session_id,
                    "{}", error_msg
                );
                (
                    State::Closing {
                        deliveries,
                        reason: format!("{log_id}: {error_msg}"),
                    },
                    Conn::reconnect_with_default(),
                )
            } else {
                match connector.connect().await {
                    Ok(connected) => {
                        metrics::CHANNEL_CONNECTIONS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => connector.dest().transport().to_string(),
                                "reason" => "link connected",
                            ),
                        );

                        let num_retries = deliveries.unacked.deque.len();
                        if num_retries > 0 {
                            metrics::CHANNEL_RECONNECTIONS.add(
                                1,
                                hyperactor_telemetry::kv_pairs!(
                                    "dest" => connector.dest().to_string(),
                                    "transport" => connector.dest().transport().to_string(),
                                    "reason" => "reconnect_with_unacked",
                                ),
                            );
                        }
                        deliveries.requeue_unacked();
                        backoff.reset();
                        (State::Running(deliveries), Conn::Connected(connected))
                    }
                    Err(err) => {
                        let elapsed = first_failure_at.elapsed();
                        let timeout =
                            hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT);

                        if elapsed > Duration::from_secs(20) || elapsed > timeout * 2 / 3 {
                            tracing::error!(
                                dest = %connector.dest(),
                                error = %err,
                                session_id = session_id,
                                elapsed_secs = elapsed.as_secs_f64(),
                                "failed to reconnect after {:.1}s; check {} metric to verify server is alive",
                                elapsed.as_secs_f64(),
                                metrics::SERVER_HEARTBEAT_METRIC_NAME
                            );
                        } else {
                            tracing::debug!(
                                dest = %connector.dest(),
                                error = %err,
                                session_id = session_id,
                                elapsed_secs = elapsed.as_secs_f64(),
                                "failed to reconnect after {:.1}s", elapsed.as_secs_f64()
                            );
                        }

                        metrics::CHANNEL_ERRORS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "dest" => connector.dest().to_string(),
                                "session_id" => session_id.to_string(),
                                "error_type" => metrics::ChannelErrorType::ConnectionError.as_str(),
                            ),
                        );
                        (
                            State::Running(deliveries),
                            Conn::reconnect(backoff, first_failure_at),
                        )
                    }
                }
            }
        }

        // The link is no longer viable.
        (State::Closing { deliveries, reason }, stream) => {
            (State::Closing { deliveries, reason }, stream)
        }
    }
}
