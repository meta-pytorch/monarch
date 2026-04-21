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
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Buf;
use bytes::Bytes;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::Frame;
use super::Link;
use super::NetRxResponse;
use super::Stream;
use super::deserialize_response;
use super::framed::FrameReader;
use super::framed::FrameWrite;
use super::serialize_response;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::SendError;
use crate::config;
use crate::metrics;

struct DemuxState<R> {
    reader: FrameReader<R>,
    /// Spaced to store one buffered frame. A reader for tag `t` checks if
    /// if the buffered message matches that tag. If not, it waits for some other
    /// reader to clear the slot. If the tag matches, takes the frame and clears
    /// the buffer.
    buffered: Option<(u8, Bytes)>,
    eof: bool,
}

/// Demultiplexes a single `FrameReader` into per-tag views.
/// Buffers at most one frame.
pub(super) struct DemuxFrameReader<R> {
    inner: tokio::sync::Mutex<DemuxState<R>>,
    notify: tokio::sync::Notify,
}

impl<R: AsyncRead + Unpin + Send> DemuxFrameReader<R> {
    pub fn new(reader: FrameReader<R>) -> Self {
        // Use Box to keep the large array off the stack.
        Self {
            inner: tokio::sync::Mutex::new(DemuxState {
                reader,
                buffered: None,
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
                if let Some((t, _)) = &state.buffered {
                    if *t == tag {
                        let (_, bytes) = state.buffered.take().unwrap();
                        drop(state);
                        self.notify.notify_waiters();
                        return Ok(Some(bytes));
                    }
                    // Else the buffered tag doesn't match, wait until the right
                    // reader consumes the buffered message.
                    // We don't buffer more than one message because it is
                    // the expectation that all available tags are dequeued
                    // eagerly, and we don't want buffer bloat. There are
                    // other backpressure mechanisms from higher up.
                    // wait is handled in notified() below.
                } else {
                    match state.reader.next().await? {
                        Some((t, bytes)) if t == tag => return Ok(Some(bytes)),
                        Some((t, bytes)) => {
                            state.buffered = Some((t, bytes));
                            drop(state);
                            // notify waiters that there's a new buffered frame to
                            // be read.
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
/// For duplex, use `mux.stream(INITIATOR_TO_ACCEPTOR)` and
/// `mux.stream(ACCEPTOR_TO_INITIATOR)`.
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
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn front(&self) -> Option<&QueuedMessage<M>> {
        self.0.front()
    }

    fn back(&self) -> Option<&QueuedMessage<M>> {
        self.0.back()
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

    pub(super) fn is_expired(&self, timeout: tokio::time::Duration) -> bool {
        self.deque
            .front()
            .is_some_and(|msg| msg.received_at.elapsed() > timeout)
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

    /// Return when the oldest message has not been acked within the
    /// timeout limit.
    pub(super) async fn wait_for_timeout(&self) {
        match self.deque.front() {
            Some(msg) => {
                tokio::time::sleep_until(
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

    /// Return the instant at which the earliest queued message (outbox or
    /// unacked) would expire, or `None` if no messages are queued.
    pub(super) fn expiry_time(&self) -> Option<Instant> {
        let timeout = hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT);
        self.outbox
            .deque
            .front()
            .map(|m| m.received_at)
            .into_iter()
            .chain(self.unacked.deque.front().map(|m| m.received_at))
            .min()
            .map(|t| t + timeout)
    }

    /// Resolves when the oldest queued message (outbox or unacked)
    /// exceeds the delivery timeout. Pends forever if no messages.
    #[allow(dead_code)] // used in later commit
    pub(super) async fn expired(&self) {
        match self.expiry_time() {
            Some(t) => tokio::time::sleep_until(t).await,
            None => std::future::pending::<()>().await,
        }
    }
}

impl<M: RemoteMessage> fmt::Display for Deliveries<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(outbox: {}, unacked: {})", self.outbox, self.unacked)
    }
}

/// Error from the receive protocol loop. An `Ok(())` from
/// [`recv_connected`] indicates normal connection close (EOF).
pub(super) enum RecvLoopError {
    /// I/O error on the underlying connection.
    Io(anyhow::Error),
    /// Cancellation was requested.
    Cancelled,
    /// Out-of-sequence message received.
    SequenceError(String),
}

impl fmt::Display for RecvLoopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::SequenceError(e) => write!(f, "sequence error: {e}"),
        }
    }
}

/// Inner receive protocol loop: read message frames, validate
/// sequence numbers, deliver to the application, and periodically
/// send acks. Runs on a single physical connection.
pub(super) async fn recv_connected<
    M: RemoteMessage,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
>(
    stream: &TaggedStream<R, W>,
    deliver_tx: &mpsc::Sender<M>,
    next: &mut Next,
) -> Result<(), RecvLoopError> {
    let ack_time_interval = hyperactor_config::global::get(config::MESSAGE_ACK_TIME_INTERVAL);
    let ack_msg_interval: u64 =
        hyperactor_config::global::get(config::MESSAGE_ACK_EVERY_N_MESSAGES);

    let mut last_ack_time = tokio::time::Instant::now();
    let mut pending_ack: Option<(Completion<W, Bytes>, u64)> = None;

    loop {
        let ack_behind = next.ack + ack_msg_interval <= next.seq
            || (next.ack < next.seq && last_ack_time.elapsed() > ack_time_interval);

        // Begin ack write if idle and behind.
        if pending_ack.is_none() && ack_behind {
            let ack = serialize_response(NetRxResponse::Ack(next.seq - 1))
                .map_err(|e| RecvLoopError::Io(e.into()))?;
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
                        last_ack_time = tokio::time::Instant::now();
                        next.ack = acked_seq;
                    }
                    Err(e) => return Err(RecvLoopError::Io(e.into())),
                }
            }

            // Ack timer tick: loop back so the ack_behind check fires.
            _ = tokio::time::sleep_until(last_ack_time + ack_time_interval),
                if next.ack < next.seq => {}

            bytes_result = stream.next() => {
                let bytes = match bytes_result {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(RecvLoopError::Io(e.into())),
                };

                let message = serde_multipart::Message::from_framed(bytes)
                    .map_err(|e| RecvLoopError::Io(e.into()))?;
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
                            return Err(RecvLoopError::SequenceError(error_msg));
                        }
                        deliver_tx
                            .send(msg)
                            .await
                            .map_err(|_| RecvLoopError::Io(anyhow::anyhow!("deliver channel closed")))?;
                        next.seq = seq + 1;
                    }
                    Err(e) => {
                        return Err(RecvLoopError::Io(anyhow::Error::from(e).context(format!(
                            "deserialize message with M = {}",
                            type_name::<M>(),
                        ))));
                    }
                }
            }
        }
    }
}

/// Error from the send protocol loop. An `Ok(())` from
/// [`send_connected`] indicates normal connection close (EOF).
pub(super) enum SendLoopError {
    /// I/O error on the underlying connection.
    Io(anyhow::Error),
    /// Application closed the send channel.
    AppClosed,
    /// Server rejected the connection.
    Rejected(String),
    /// Server closed the channel.
    ServerClosed,
    /// Delivery timeout on oldest unacked message.
    DeliveryTimeout,
    /// Frame exceeds maximum allowed size.
    OversizedFrame(String),
}

impl fmt::Display for SendLoopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::AppClosed => write!(f, "application closed"),
            Self::Rejected(r) => write!(f, "rejected: {r}"),
            Self::ServerClosed => write!(f, "server closed"),
            Self::DeliveryTimeout => write!(f, "delivery timeout"),
            Self::OversizedFrame(r) => write!(f, "oversized frame: {r}"),
        }
    }
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

/// Inner send protocol loop: accept messages from the application,
/// serialize, write via [`Completion`], read ack responses, and
/// manage the outbox/unacked buffers. Runs on a single physical
/// connection. Cancel-safe: the caller may wrap the returned future
/// in a `select!` branch.
pub(super) async fn send_connected<M, R, W>(
    stream: &TaggedStream<R, W>,
    deliveries: &mut Deliveries<M>,
    receiver: &mut mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
) -> Result<(), SendLoopError>
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
                return Err(SendLoopError::OversizedFrame(reason));
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
                            Err(e) => return Err(SendLoopError::Io(e.into())),
                        };
                        match response {
                            NetRxResponse::Ack(ack) => {
                                deliveries.unacked.prune(
                                    ack,
                                    tokio::time::Instant::now(),
                                    &deliveries.outbox.dest_addr,
                                    deliveries.outbox.session_id,
                                );
                            }
                            NetRxResponse::Reject(reason) => {
                                return Err(SendLoopError::Rejected(reason));
                            }
                            NetRxResponse::Closed => {
                                return Err(SendLoopError::ServerClosed);
                            }
                        }
                    }
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(SendLoopError::Io(e.into())),
                }
            }

            // Delivery timeout on oldest unacked message.
            _ = deliveries.unacked.wait_for_timeout(), if !deliveries.unacked.is_empty() => {
                return Err(SendLoopError::DeliveryTimeout);
            }

            // Drive frame write to completion.
            send_result = async { pending.as_mut().unwrap().drive().await },
                if pending.is_some() => {
                match send_result {
                    Ok(()) => {
                        pending = None;
                        let mut message = deliveries.outbox.pop_front()
                            .expect("outbox should not be empty");
                        message.sent_at = Some(tokio::time::Instant::now());
                        deliveries.unacked.push_back(message);
                    }
                    Err(e) => return Err(SendLoopError::Io(e.into())),
                }
            }

            // Accept new messages from the application (only when
            // outbox is empty so queued messages are sent first).
            msg = receiver.recv(), if deliveries.outbox.is_empty() => {
                match msg {
                    Some(item) => {
                        if let Err(e) = deliveries.outbox.push_back(item) {
                            return Err(SendLoopError::Io(anyhow::anyhow!(e)));
                        }
                    }
                    None => return Err(SendLoopError::AppClosed),
                }
            }
        }
    }
}

// ── Session typestates ──────────────────────────────────────────

/// Disconnected state: the session has no active connection.
pub(super) struct Disconnected;

/// Connected state: the session holds an active mux.
pub(super) struct Connected<S: Stream> {
    mux: Mux<ReadHalf<S>, WriteHalf<S>>,
}

/// Connection-managed session with typestate-enforced lifecycle.
///
/// A session starts [`Disconnected`]. Call [`connect`](Session::connect)
/// to transition to [`Connected`]. Call [`release`](Session::release)
/// to transition back. Streams borrow the connected session,
/// preventing release while they exist.
///
/// The session owns the [`Link`] and calls `link.next()` inline
/// in `connect()` — no background driver task.
pub(super) struct Session<L: Link, State = Disconnected> {
    link: L,
    state: State,
}

impl<L: Link> Session<L, Disconnected> {
    /// Create a new disconnected session that will acquire connections
    /// from the given link.
    pub(super) fn new(link: L) -> Self {
        Self {
            link,
            state: Disconnected,
        }
    }

    /// Acquire a connection from the link. Consumes `self` and
    /// returns `Ok(Session<Connected>)` on success, or `Err(self)`
    /// if the link fails.
    ///
    /// Returns a boxed future so the self-referential borrow from
    /// `link.next()` is hidden behind the box, keeping the outer
    /// generator's Send analysis clean.
    pub(super) fn connect(
        self,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Session<L, Connected<L::Stream>>, Self>> + Send,
        >,
    > {
        Box::pin(async move {
            let Session { link, state: _ } = self;
            match link.next().await {
                Ok(stream) => {
                    let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
                    let (reader, writer) = tokio::io::split(stream);
                    let mux = Mux::new(reader, writer, max);
                    Ok(Session {
                        link,
                        state: Connected { mux },
                    })
                }
                Err(_) => Err(Session {
                    link,
                    state: Disconnected,
                }),
            }
        })
    }

    /// Like [`connect`](Self::connect), but returns `Err(self)` if
    /// `deadline` is reached before a connection is available. Retries
    /// on transient `link.next()` failures until the deadline fires.
    pub(super) fn connect_by(
        self,
        deadline: Instant,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Session<L, Connected<L::Stream>>, Self>> + Send,
        >,
    > {
        Box::pin(async move {
            let Session { link, state: _ } = self;
            let sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(sleep);
            let mut consecutive_failures: u32 = 0;
            loop {
                let result = tokio::select! {
                    biased;
                    _ = &mut sleep => None,
                    r = link.next() => Some(r),
                };
                match result {
                    Some(Ok(stream)) => {
                        let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
                        let (reader, writer) = tokio::io::split(stream);
                        let mux = Mux::new(reader, writer, max);
                        return Ok(Session {
                            link,
                            state: Connected { mux },
                        });
                    }
                    Some(Err(err)) => {
                        consecutive_failures += 1;
                        let delay = Duration::from_millis(
                            10u64.saturating_mul(1u64 << consecutive_failures.min(9)),
                        )
                        .min(Duration::from_secs(5));
                        tracing::info!(
                            dest = %link.dest(),
                            error = %err,
                            consecutive_failures,
                            delay_ms = delay.as_millis() as u64,
                            "connect_by: link.next() failed, retrying after backoff"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    None => {
                        return Err(Session {
                            link,
                            state: Disconnected,
                        });
                    }
                }
            }
        })
    }
}

impl<L: Link> Session<L, Connected<L::Stream>> {
    /// Obtain a [`ConnectionStream`] for the given tag, borrowing this
    /// session so it cannot be released while the stream exists.
    pub(super) fn stream(&self, tag: u8) -> ConnectionStream<'_, L::Stream> {
        ConnectionStream {
            inner: self.state.mux.stream(tag),
            _session: PhantomData,
        }
    }

    /// Release the connection and return to the disconnected state.
    pub(super) fn release(self) -> Session<L, Disconnected> {
        Session {
            link: self.link,
            state: Disconnected,
        }
    }
}

/// A tagged stream bound to a connected [`Session`]'s lifetime.
/// Prevents release while the stream exists. Derefs to
/// [`TaggedStream`] so protocol functions work unchanged.
pub(super) struct ConnectionStream<'a, S: Stream> {
    inner: TaggedStream<ReadHalf<S>, WriteHalf<S>>,
    _session: PhantomData<&'a ()>,
}

impl<S: Stream> Deref for ConnectionStream<'_, S> {
    type Target = TaggedStream<ReadHalf<S>, WriteHalf<S>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Wait for the next task in a `JoinSet` to complete. If the set is
/// empty, pend forever (so it can be used in a `select!` branch
/// without busy-spinning).
#[allow(dead_code)] // used in later commit
pub(super) async fn join_nonempty<T: 'static>(
    set: &mut tokio::task::JoinSet<T>,
) -> Result<T, tokio::task::JoinError> {
    match set.join_next().await {
        None => std::future::pending().await,
        Some(result) => result,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::io::AsyncWriteExt;

    use super::super::framed::FrameReader;
    use super::super::framed::FrameWrite;
    use super::DemuxFrameReader;

    async fn write_frame(
        writer: tokio::io::DuplexStream,
        payload: &[u8],
        tag: u8,
    ) -> tokio::io::DuplexStream {
        let mut fw = FrameWrite::new(writer, Bytes::from(payload.to_vec()), 4096, tag).unwrap();
        fw.send().await.unwrap();
        fw.complete()
    }

    /// Regression test for a data-loss bug in DemuxFrameReader.
    ///
    /// The bug: when `next_tagged(A)` reads a frame for tag B, it buffers
    /// it and then blocks on `self.notify.notified().await` waiting for
    /// another consumer to make progress. In production, this read is
    /// used inside `select!`, so the future may be **cancelled** while
    /// parked at `notified()`. On the next call, `next_tagged(A)` finds
    /// `buffered[B]` already occupied and must wait for B to read it.
    ///
    /// This test exercises the pushback path:
    ///
    ///   Wire:  B0, A0
    ///
    ///   1. `next_tagged(A)` reads B0 → stored in `buffered`, parks → cancel
    ///   2. `next_tagged(A)` re-enters: `buffered` occupied, parks → cancel
    ///   3. Repeated cancellation: `buffered` occupied → blocks immediately
    ///   4. `next_tagged(B)` takes B0 from `buffered`
    ///   5. `next_tagged(A)` reads A0 from wire
    #[tokio::test]
    async fn test_demux_does_not_drop_buffered_frames() {
        const MAX_LEN: usize = 4096;
        let (reader, writer) = tokio::io::duplex(MAX_LEN * 16);
        let demux = Arc::new(DemuxFrameReader::new(FrameReader::new(reader, MAX_LEN)));

        let tag_a: u8 = 0;
        let tag_b: u8 = 1;

        // Wire: B0, A0. Tag-A will read B0 (stored in buffered),
        // then block because buffered is occupied.
        let w = write_frame(writer, b"B0", tag_b).await;
        let w = write_frame(w, b"A0", tag_a).await;
        let mut w = w;
        w.shutdown().await.unwrap();

        // First call: tag-A reads B0 then blocks.
        let result =
            tokio::time::timeout(Duration::from_millis(100), demux.next_tagged(tag_a)).await;
        assert!(result.is_err(), "expected timeout, got {:?}", result);

        // Repeated cancellation: tag-A re-enters, finds pushback occupied,
        // immediately blocks. No additional frames are read.
        for _ in 0..3 {
            let result =
                tokio::time::timeout(Duration::from_millis(50), demux.next_tagged(tag_a)).await;
            assert!(result.is_err(), "expected timeout, got {:?}", result);
        }

        // Tag-B takes B0 from buffered[B].
        let b0 = demux.next_tagged(tag_b).await.unwrap().unwrap();
        assert_eq!(b0, Bytes::from_static(b"B0"), "first B frame was dropped");

        // Tag-A: pushback B1 drains to buffered[B], then reads A0 from wire.
        let a0 = demux.next_tagged(tag_a).await.unwrap().unwrap();
        assert_eq!(a0, Bytes::from_static(b"A0"));

        // Both at EOF.
        assert!(demux.next_tagged(tag_a).await.unwrap().is_none());
        assert!(demux.next_tagged(tag_b).await.unwrap().is_none());
    }
}
