/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TODO

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::ops::DerefMut;

use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use enum_as_inner::EnumAsInner;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::Instrument;
use tracing::Span;

use super::framed::FrameReader;
use super::framed::FrameWrite;
use super::framed::WriteState;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::SendError;
use crate::channel::TxStatus;
use crate::channel::net::Frame;
use crate::channel::net::Link;
use crate::channel::net::NetRxResponse;
use crate::channel::net::NetTx;
use crate::channel::net::Stream;
use crate::channel::net::deserialize_response;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::config;
use crate::metrics;

/// Use to prevent [futures::Stream] objects using the wrong next() method by
/// accident. Bascially, we want to use [tokio_stream::StreamExt::next] since it
/// is cancel safe. However, there is another trait, [futures::StreamExt::next],
/// which has the same method name and similar functionality, except it is not
/// cancel safe. It is quite easy to import the wrong trait and use the wrong
/// method by doing `stream.next()`. Adding this trait would prevent this
/// from happening in this file. The callsite would have to `StreamExt::next()`
/// to disambiguate.
trait UnimplementedStreamExt {
    fn next(&mut self) {
        unimplemented!()
    }
}

impl<T: futures::Stream> UnimplementedStreamExt for T {}

struct QueuedMessage<M: RemoteMessage> {
    seq: u64,
    message: serde_multipart::Message,
    received_at: Instant,
    // When this message was written to the stream. None means it is not
    // written yet.
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

// A new type to provide custom Debug impl.
struct MessageDeque<M: RemoteMessage>(VecDeque<QueuedMessage<M>>);

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

// only show the first and last N messages, and display how many are
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

impl<M: RemoteMessage> QueuedMessage<M> {
    /// Attempt to deserialize this queued frame as a
    /// `Frame::Message<M>` and return it to the original
    /// sender. Falls back to logging if the frame is not a
    /// message or deserialization fails.
    pub(crate) fn try_return(self) {
        match serde_multipart::deserialize_bincode::<Frame<M>>(self.message) {
            Ok(Frame::Message(_, msg)) => {
                let _ = self
                    .return_channel
                    .send(SendError(ChannelError::Closed, msg));
            }
            Ok(_) => {
                tracing::debug!(
                    seq = self.seq,
                    "queued frame was not a Frame::Message; dropping without return"
                );
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

struct Outbox<'a, M: RemoteMessage> {
    // The seq number of the next new message put into outbox. Requeued
    // unacked messages should still use their already assigned seq
    // numbers.
    next_seq: u64,
    deque: MessageDeque<M>,
    log_id: &'a str,
    dest_addr: &'a ChannelAddr,
    session_id: u64,
}

impl<'a, M: RemoteMessage> Outbox<'a, M> {
    fn new(log_id: &'a str, dest_addr: &'a ChannelAddr, session_id: u64) -> Self {
        Self {
            next_seq: 0,
            deque: MessageDeque(VecDeque::new()),
            log_id,
            dest_addr,
            session_id,
        }
    }

    fn is_expired(&self) -> bool {
        match self.deque.front() {
            None => false,
            Some(msg) => {
                msg.received_at.elapsed()
                    > hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }

    fn front_message(&self) -> Option<serde_multipart::Message> {
        self.deque.front().map(|msg| msg.message.clone())
    }

    fn front_size(&self) -> Option<usize> {
        self.deque.front().map(|msg| msg.message.frame_len())
    }

    fn pop_front(&mut self) -> Option<QueuedMessage<M>> {
        self.deque.pop_front()
    }

    fn push_back(
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

    fn requeue_unacked(&mut self, unacked: MessageDeque<M>) {
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

impl<'a, M: RemoteMessage> fmt::Display for Outbox<'a, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(next_seq: {}, deque: {})", self.next_seq, self.deque)
    }
}

// A tuple of acked seq and when it was acked.
#[derive(Debug, Clone)]
struct AckedSeq(u64, Instant);

impl fmt::Display for AckedSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let acked_secs = self.1.elapsed().as_secs();
        write!(f, "(seq={}, since_acked={}sec)", self.0, acked_secs)
    }
}

struct Unacked<'a, M: RemoteMessage> {
    deque: MessageDeque<M>,
    largest_acked: Option<AckedSeq>,
    log_id: &'a str,
}

impl<'a, M: RemoteMessage> Unacked<'a, M> {
    fn new(largest_acked: Option<AckedSeq>, log_id: &'a str) -> Self {
        Self {
            deque: MessageDeque(VecDeque::new()),
            largest_acked,
            log_id,
        }
    }

    fn push_back(&mut self, message: QueuedMessage<M>) {
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
            // Note: some scenarios of why this if branch could happen:
            //
            // message.0 <= largest could happen in the following scenario:
            //
            // 1. NetTx sent seq=2 and seq=3.
            // 2. NetRx received messages and put them on its mpsc channel.
            //    But before NetRx acked, the connection was broken.
            // 3. NetTx reconnected. In this case, NetTx will put unacked
            //    messages, i.e. 2 and 3, back to outbox.
            // 2. At the beginning of the new connection. NetRx acked 2
            //    and 3 immediately.
            // 3. Before sending messages, NetTx received the acks first
            //    with the new connection. NetTx Stored 3 as largest_acked.
            // 4. Now NetRx finally got the chance to resend 2 and 3.
            //    When it resent 2, 2 < largest_acked, which is 3.
            //    * similarly, if there was only one message, seq=3
            //      involved, we would have 3 == largest_acked.
            //
            // message.0 == largest could also happen in the following
            // scenario:
            //
            // The message was delivered, but the send branch did not push
            // it into unacked queue. This chould happen when:
            //   1. `outbox.send_message` future was canceled by tokio::select.
            //   2. `outbox.send_message` returns an error, which makes
            //      the deliver result unknown to Tx.
            // When this happens, Tx will resend the same message. However,
            // since Rx already received the message, it might ack before
            // Tx resends. As a result, this message's ack would be
            // recorded already by `largest_acked` before it is put into
            // unacked queue.
            if message.seq <= largest {
                // since the message is already delivered and acked, it
                // does need to be put in the queue again.
                return;
            }
        }

        self.deque.push_back(message);
    }

    /// Remove acked messages from the deque.
    fn prune(&mut self, acked: u64, acked_at: Instant, dest_addr: &ChannelAddr, session_id: u64) {
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
                // Track latency: time from when message was first received to when it was acked
                let latency_micros = msg.received_at.elapsed().as_micros() as i64;
                metrics::CHANNEL_LATENCY_MICROS.record(
                    latency_micros as f64,
                    hyperactor_telemetry::kv_pairs!(
                        "dest" => dest_addr.to_string(),
                        "session_id" => session_id.to_string(),
                    ),
                );
            } else {
                // Messages in the deque are orderd by seq in ascending
                // order. So we could return early once we encounter
                // a message that is not acked.
                break;
            }
        }
    }

    fn is_expired(&self) -> bool {
        matches!(
            self.deque.front(),
            Some(msg) if msg.received_at.elapsed() > hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
        )
    }

    /// Return when the oldest message has not been acked within the
    /// timeout limit. This method is used in tokio::select with other
    /// branches.
    async fn wait_for_timeout(&self) {
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

    fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }
}

impl<'a, M: RemoteMessage> fmt::Display for Unacked<'a, M> {
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

struct Deliveries<'a, M: RemoteMessage> {
    outbox: Outbox<'a, M>,
    unacked: Unacked<'a, M>,
}

impl<'a, M: RemoteMessage> fmt::Display for Deliveries<'a, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(outbox: {}, unacked: {})", self.outbox, self.unacked)
    }
}

#[derive(EnumAsInner)]
enum State<'a, M: RemoteMessage> {
    /// Channel is running.
    Running(Deliveries<'a, M>),
    /// Message delivery not possible.
    Closing {
        deliveries: Deliveries<'a, M>,
        /// why closing
        reason: String,
    },
}

impl<'a, M: RemoteMessage> State<'a, M> {
    fn init(log_id: &'a str, dest_addr: &'a ChannelAddr, session_id: u64) -> Self {
        Self::Running(Deliveries {
            outbox: Outbox::new(log_id, dest_addr, session_id),
            unacked: Unacked::new(None, log_id),
        })
    }

    fn deliveries(&self) -> &Deliveries<'a, M> {
        match self {
            Self::Running(deliveries) => deliveries,
            Self::Closing { deliveries, .. } => deliveries,
        }
    }
}

impl<'a, M: RemoteMessage> fmt::Display for State<'a, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Running(deliveries) => {
                write!(f, "Running(deliveries: {})", deliveries)
            }
            State::Closing { deliveries, reason } => {
                write!(f, "Running(deliveries: {}, reason: {})", deliveries, reason)
            }
        }
    }
}

#[derive(EnumAsInner)]
enum Conn<S: Stream> {
    /// Disconnected.
    Disconnected(Box<dyn Backoff + Send>),
    /// Connected and ready to go.
    Connected {
        reader: FrameReader<ReadHalf<S>>,
        write_state: WriteState<WriteHalf<S>, serde_multipart::Frame, ()>,
    },
}

impl<S: Stream> Conn<S> {
    fn reconnect_with_default() -> Self {
        Self::Disconnected(Box::new(
            ExponentialBackoffBuilder::new()
                .with_initial_interval(Duration::from_millis(1))
                .with_multiplier(2.0)
                .with_randomization_factor(0.1)
                .with_max_interval(Duration::from_millis(1000))
                .with_max_elapsed_time(None) // Allow infinite retries
                .build(),
        ))
    }

    fn reconnect(backoff: impl Backoff + Send + 'static) -> Self {
        Self::Disconnected(Box::new(backoff))
    }
}

/// Creates a new session, and assigns it a guid.
pub(super) fn dial<M: RemoteMessage>(link: impl Link + 'static) -> NetTx<M> {
    let (sender, receiver) = mpsc::unbounded_channel();
    let dest = link.dest();
    let (notify, status) = watch::channel(TxStatus::Active);

    let tx = NetTx {
        sender,
        dest,
        status,
    };
    crate::init::get_runtime().spawn(run(link, receiver, notify));
    tx
}

async fn run<M: RemoteMessage>(
    link: impl Link,
    mut receiver: mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
    notify: watch::Sender<TxStatus>,
) {
    // If we can't deliver a message within this limit consider
    // `link` broken and return.

    let session_id = rand::random();
    let log_id = format!("session {}.{}", link.dest(), session_id);
    let dest = link.dest();
    let mut state = State::init(&log_id, &dest, session_id);
    let mut conn = Conn::reconnect_with_default();

    let (state, conn) = loop {
        let span = state_span(&state, &conn, session_id, &link);

        (state, conn) = step(state, conn, session_id, &log_id, &link, &mut receiver)
            .instrument(span)
            .await;

        if state.is_closing() {
            break (state, conn);
        }

        if let Conn::Disconnected(ref mut backoff) = conn {
            RealClock.sleep(backoff.next_backoff().unwrap()).await;
        }
    }; // loop

    let span = state_span(&state, &conn, session_id, &link);

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
            // TODO(T233029051): Return reason through return_channel too.
            reason: _,
        } => {
            // Close the channel to prevent any further messages from being sent.
            receiver.close();
            // Return in order from oldest to newest, messages
            // either not acknowledged or not sent.
            unacked
                .deque
                .drain(..)
                .chain(outbox.deque.drain(..))
                .for_each(|queued| queued.try_return());
            while let Ok((msg, return_channel, _)) = receiver.try_recv() {
                let _ = return_channel.send(SendError(ChannelError::Closed, msg));
            }
        }
        _ => (),
    }

    // Notify senders that this link is no longer usable. It is okay if the notify
    // channel is closed because that means no one is listening for the notification.
    let _ = notify.send(TxStatus::Closed);

    match conn {
        Conn::Connected {
            mut write_state, ..
        } => {
            if let WriteState::Writing(frame_writer, ()) = &mut write_state {
                if let Err(err) = frame_writer.send().await {
                    tracing::info!(
                        parent: &span,
                        dest = %dest,
                        error = %err,
                        session_id = session_id,
                        "write error during cleanup"
                    );
                }
            };
            if let Some(mut w) = write_state.into_writer() {
                // Try to shutdown the connection gracefully. This is a best effort
                // operation, and we don't care if it fails.
                let _ = w.shutdown().await;
            }
        }
        Conn::Disconnected(_) => (),
    };

    tracing::info!(
        parent: &span,
        dest = %dest,
        session_id = session_id,
        "NetTx::run exits"
    );
}

fn state_span<'a, L, S, M>(state: &State<'a, M>, conn: &Conn<S>, session_id: u64, link: &L) -> Span
where
    S: Stream,
    L: Link<Stream = S>,
    M: RemoteMessage,
{
    let deliveries = state.deliveries();

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
        session = format!("{}.{}", link.dest(), session_id),
        connected = conn.is_connected(),
        next_seq = deliveries.outbox.next_seq,
        largest_acked = largest_acked.as_value(),
        outbox = QueueValue::from(&deliveries.outbox.deque).as_value(),
        unacked = QueueValue::from(&deliveries.unacked.deque).as_value(),
    )
}

async fn step<'a, L, S, M>(
    state: State<'a, M>,
    conn: Conn<S>,
    session_id: u64,
    log_id: &'a str,
    link: &L,
    receiver: &mut mpsc::UnboundedReceiver<(M, oneshot::Sender<SendError<M>>, Instant)>,
) -> (State<'a, M>, Conn<S>)
where
    S: Stream,
    L: Link<Stream = S>,
    M: RemoteMessage,
{
    match (state, conn) {
        // This branch is to provide lazy connection creation. It can be removed after
        // we move to eager creation.
        (
            State::Running(Deliveries {
                mut outbox,
                unacked,
            }),
            conn,
        ) if outbox.is_empty() && unacked.is_empty() => match receiver.recv().await {
            Some(msg) => match outbox.push_back(msg) {
                Ok(()) => {
                    let running = State::Running(Deliveries { outbox, unacked });
                    (running, conn)
                }
                Err(err) => {
                    let error_msg = "failed to push message to outbox";
                    tracing::error!(
                        dest = %link.dest(),
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
        },
        (
            State::Running(Deliveries {
                mut outbox,
                unacked,
            }),
            Conn::Connected {
                reader,
                write_state: WriteState::Idle(writer),
                ..
            },
        ) if !outbox.is_empty() => {
            let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
            let len = outbox.front_size().expect("not empty");
            let message = outbox.front_message().expect("not empty");

            match FrameWrite::new(writer, message.framed(), max) {
                Ok(fw) => (
                    State::Running(Deliveries { outbox, unacked }),
                    Conn::Connected {
                        reader,
                        write_state: WriteState::Writing(fw, ()),
                    },
                ),
                Err((writer, e)) => {
                    debug_assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                    tracing::error!(
                        dest = %link.dest(),
                        session_id = session_id,
                        "rejecting oversize frame: len={} > max={}. \
                                ack will not arrive before timeout; increase CODEC_MAX_FRAME_LENGTH to allow.",
                        len,
                        max
                    );
                    // Reject and return.
                    outbox.pop_front().expect("not empty").try_return();
                    let error_msg = "oversized frame was rejected. closing channel";
                    tracing::error!(
                        dest = %link.dest(),
                        session_id = session_id,
                        "{}", error_msg,
                    );
                    // Close the channel (avoid sequence
                    // violations).
                    (
                        State::Closing {
                            deliveries: Deliveries { outbox, unacked },
                            reason: format!("{log_id}: {error_msg}"),
                        },
                        Conn::Connected {
                            reader,
                            write_state: WriteState::Idle(writer),
                        },
                    )
                }
            }
        }
        (
            State::Running(Deliveries {
                mut outbox,
                mut unacked,
            }),
            Conn::Connected {
                mut reader,
                mut write_state,
            },
        ) => {
            tokio::select! {
                biased;

                ack_result = reader.next().instrument(hyperactor_telemetry::context_span!("read ack")) => {
                    match ack_result {
                        Ok(Some(buffer)) => {
                            match deserialize_response(buffer) {
                                Ok(response) => {
                                    match response {
                                        NetRxResponse::Ack(ack) => {
                                            unacked.prune(ack, RealClock.now(), &link.dest(), session_id);
                                            (State::Running(Deliveries { outbox, unacked }), Conn::Connected { reader, write_state })
                                        }
                                        NetRxResponse::Reject(reason) => {
                                            let error_msg = format!("server rejected connection due to: {reason}");
                                            tracing::error!(
                                                        dest = %link.dest(),
                                                        session_id = session_id,
                                                        "{}", error_msg
                                                    );
                                            (State::Closing {
                                                deliveries: Deliveries{outbox, unacked},
                                                reason: error_msg,
                                            }, Conn::reconnect_with_default())
                                        }
                                        NetRxResponse::Closed => {
                                            let msg = "server closed the channel".to_string();
                                            tracing::info!(
                                                        dest = %link.dest(),
                                                        session_id = session_id,
                                                        "{}", msg
                                                    );
                                            (State::Closing {
                                                deliveries: Deliveries{outbox, unacked},
                                                reason: msg,
                                            }, Conn::reconnect_with_default())
                                        }
                                    }
                                }
                                Err(err) => {
                                    let error_msg = "failed deserializing response";
                                    tracing::error!(
                                                dest = %link.dest(),
                                                session_id = session_id,
                                                error = %err,
                                                "{}", error_msg
                                            );
                                    // Similar to the message flow, we always close the
                                    // channel when encountering ser/deser errors.
                                    (State::Closing {
                                        deliveries: Deliveries{outbox, unacked},
                                        reason: format!(
                                            "{log_id}: {error_msg}: {err}",
                                        ),
                                    }, Conn::Connected { reader, write_state })
                                }
                            }
                        }
                        Ok(None) => {
                            // Graceful of stream: reconnect
                            (State::Running(Deliveries { outbox, unacked }), Conn::reconnect_with_default())
                        }
                        Err(err) => {
                                tracing::error!(
                                            dest = %link.dest(),
                                            session_id = session_id,
                                            error = %err,
                                            "failed while receiving ack"
                                        );
                                // Reconnect and wish the error will go away.
                                (State::Running(Deliveries { outbox, unacked }), Conn::reconnect_with_default())
                        }
                    }
                },

                // If acking message takes too long, consider the link broken.
                _ = unacked.wait_for_timeout(), if !unacked.is_empty() => {
                    let error_msg = format!(
                        "failed to receive ack within timeout {:?}; link is currently connected",
                        hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT),
                    );
                    tracing::error!(
                                dest = %link.dest(),
                                session_id = session_id,
                                "{}", error_msg,
                            );
                    (State::Closing {
                        deliveries: Deliveries{outbox, unacked},
                        reason: format!("{log_id}: {error_msg}"),
                    }, Conn::Connected { reader, write_state })
                }


                // We have to be careful to manage outgoing write states, so that we never write
                // partial frames in the presence cancellation.
                send_result = write_state.send().instrument(hyperactor_telemetry::context_span!("write bytes")) => {
                    match send_result {
                        Ok(()) => {
                            let mut message = outbox.pop_front().expect("outbox should not be empty");
                            // If this message was re-put into `outbox` from `unacked` due to reconnection,
                            // its `sent_at` field would be set in the last attempt. In that case, we simply
                            // overwrite the old one here.
                            message.sent_at = Some(RealClock.now());
                            unacked.push_back(message);
                            (State::Running(Deliveries { outbox, unacked }), Conn::Connected { reader, write_state })
                        }
                        Err(err) => {
                            tracing::info!(
                                dest = %link.dest(),
                                session_id,
                                error = %err,
                                "outbox send error; message size: {}",
                                outbox.front_size().expect("outbox should not be empty"),
                            );
                            // Track error for this channel pair
                            metrics::CHANNEL_ERRORS.add(
                                1,
                                hyperactor_telemetry::kv_pairs!(
                                    "dest" => link.dest().to_string(),
                                    "session_id" => session_id.to_string(),
                                    "error_type" => metrics::ChannelErrorType::SendError.as_str(),
                                ),
                            );
                            (State::Running(Deliveries { outbox, unacked }), Conn::reconnect_with_default())
                        }
                    }
                }
                // UnboundedReceiver::recv() is cancel safe.
                // Only checking mpsc channel when outbox is empty. In this way, we prioritize
                // sending messages already in outbox.
                work_result = receiver.recv(), if outbox.is_empty() => {
                    match work_result {
                        Some(msg) => {
                            match outbox.push_back(msg) {
                                Ok(()) => {
                                    let running = State::Running (Deliveries{
                                        outbox,
                                        unacked,
                                    });
                                    (running, Conn::Connected { reader, write_state })
                                }
                                Err(err) => {
                                    let error_msg = "failed to push message to outbox";
                                    tracing::error!(
                                        dest = %link.dest(),
                                        session_id,
                                        error = %err,
                                        "{}", error_msg,
                                    );
                                    (State::Closing {
                                        deliveries: Deliveries {outbox, unacked},
                                        reason: format!("{log_id}: {error_msg}: {err}"),
                                    }, Conn::Connected { reader, write_state })
                                }
                            }
                        }
                        None => (State::Closing {
                            deliveries: Deliveries{outbox, unacked},
                            reason: "NetTx is dropped".to_string(),
                        }, Conn::Connected { reader, write_state }),
                    }
                },
            }
        }

        // We have a message to send, but not an active link.
        (
            State::Running(Deliveries {
                mut outbox,
                unacked,
            }),
            Conn::Disconnected(mut backoff),
        ) => {
            // If delivering this message is taking too long,
            // consider the link broken.
            if outbox.is_expired() {
                let error_msg = format!(
                    "failed to deliver message within timeout {:?}",
                    hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
                );
                tracing::error!(
                    dest = %link.dest(),
                    session_id,
                    "{}", error_msg
                );
                (
                    State::Closing {
                        deliveries: Deliveries { outbox, unacked },
                        reason: format!("{log_id}: {error_msg}"),
                    },
                    Conn::reconnect_with_default(),
                )
            } else if unacked.is_expired() {
                let error_msg = format!(
                    "failed to receive ack within timeout {:?}; link is currently broken",
                    hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT),
                );
                tracing::error!(
                    dest = %link.dest(),
                    session_id = session_id,
                    "{}", error_msg
                );
                (
                    State::Closing {
                        deliveries: Deliveries { outbox, unacked },
                        reason: format!("{log_id}: {error_msg}"),
                    },
                    Conn::reconnect_with_default(),
                )
            } else {
                match link.connect().await {
                    Ok(stream) => {
                        let message =
                            serde_multipart::serialize_bincode(&Frame::<M>::Init(session_id))
                                .unwrap();

                        let mut write = FrameWrite::new(
                            stream,
                            message.framed(),
                            hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
                        )
                        .expect("enough length");
                        let initialized = write.send().await.is_ok();
                        let stream = write.complete();

                        metrics::CHANNEL_CONNECTIONS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => link.dest().transport().to_string(),
                                "reason" => "link connected",
                            ),
                        );

                        // Need to resend unacked after reconnecting.
                        let largest_acked = unacked.largest_acked;
                        let num_retries = unacked.deque.len();
                        if num_retries > 0 {
                            // Track reconnection for this channel pair
                            metrics::CHANNEL_RECONNECTIONS.add(
                                1,
                                hyperactor_telemetry::kv_pairs!(
                                    "dest" => link.dest().to_string(),
                                    "transport" => link.dest().transport().to_string(),
                                    "reason" => "reconnect_with_unacked",
                                ),
                            );
                        }
                        outbox.requeue_unacked(unacked.deque);
                        (
                            State::Running(Deliveries {
                                outbox,
                                // unacked messages are put back to outbox. So they are not
                                // considered as "sent yet unacked" message anymore. But
                                // we still want to keep `largest_acked` to known Rx's watermark.
                                unacked: Unacked::new(largest_acked, log_id),
                            }),
                            if initialized {
                                backoff.reset();
                                let (reader, writer) = tokio::io::split(stream);
                                Conn::Connected {
                                    reader: FrameReader::new(
                                        reader,
                                        hyperactor_config::global::get(
                                            config::CODEC_MAX_FRAME_LENGTH,
                                        ),
                                    ),
                                    write_state: WriteState::Idle(writer),
                                }
                            } else {
                                Conn::reconnect(backoff)
                            },
                        )
                    }
                    Err(err) => {
                        tracing::debug!(
                            dest = %link.dest(),
                            error = %err,
                            session_id = session_id,
                            "failed to connect"
                        );
                        // Track connection error for this channel pair
                        metrics::CHANNEL_ERRORS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "dest" => link.dest().to_string(),
                                "session_id" => session_id.to_string(),
                                "error_type" => metrics::ChannelErrorType::ConnectionError.as_str(),
                            ),
                        );
                        (
                            State::Running(Deliveries { outbox, unacked }),
                            Conn::reconnect(backoff),
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
