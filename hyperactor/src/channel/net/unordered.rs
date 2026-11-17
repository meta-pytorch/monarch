/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Unordered multi-stream simplex sessions.
//!
//! The contract and the protocol are specified in the "Unordered
//! Channels" chapter of the hyperactor book
//! (`docs/source/books/hyperactor-book/src/channels/unordered.md`).
//!
//! Posting appends a message to a queue shared by the session's writers,
//! one per stream. A writer takes the oldest queued message, numbers it
//! in its own stream's sequence, and holds it in its [`Outbox`] until the
//! server acknowledges it or it expires. After a reconnect, a writer
//! rewrites the messages it holds, with the same numbers, before it takes
//! new ones. The server keeps one watermark per stream and acks
//! cumulatively (see
//! [`Sequencing::Unordered`](super::session::Sequencing::Unordered)).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::Frame;
use super::INITIATOR_TO_ACCEPTOR;
use super::Link;
use super::NetRxResponse;
use super::deserialize_response;
use super::session::Completion;
use super::session::SendLoopError;
use super::session::Session;
use super::session::TaggedStream;
use crate::RemoteMessage;
use crate::channel::ChannelError;
use crate::channel::ChannelTx;
use crate::channel::CloseReason;
use crate::channel::CompletionSink;
use crate::channel::SendError;
use crate::channel::SendErrorReason;
use crate::channel::TxSink;
use crate::channel::TxStatus;
use crate::config;
use crate::metrics;

/// Establish an unordered session over `links`, which share one
/// `SessionId` and carry distinct nonzero stream ids. Returns a single
/// send handle whose messages are spread across the links.
pub(crate) fn spawn<M: RemoteMessage>(links: Vec<impl Link + 'static>) -> ChannelTx<M> {
    assert!(!links.is_empty());
    let dest = links[0].dest();
    let (notify, status) = watch::channel(TxStatus::Active);
    let shared = Arc::new(Shared::new());
    let tx = ChannelTx {
        sink: TxSink::Unordered(Sink(Arc::clone(&shared))),
        dest: dest.clone(),
        status,
    };

    let shutdown = CancellationToken::new();
    let writers: Vec<_> = links
        .into_iter()
        .map(|link| {
            crate::init::get_runtime().spawn(write_loop(
                Arc::clone(&shared),
                link,
                shutdown.clone(),
            ))
        })
        .collect();

    crate::init::get_runtime().spawn(async move {
        expire_until_closed(&shared).await;
        shutdown.cancel();
        for writer in writers {
            let _ = writer.await;
        }
        let _ = notify.send(TxStatus::Closed(CloseReason::Other(format!(
            "unordered session to {dest}: sender dropped"
        ))));
    });

    tx
}

/// The handle through which a [`ChannelTx`] posts to an unordered session.
/// Posting hands the message to the writers directly. Dropping the sink
/// closes the session once its messages have settled.
pub(crate) struct Sink<M: RemoteMessage>(Arc<Shared<M>>);

impl<M: RemoteMessage> Sink<M> {
    pub(crate) fn post(&self, message: M, completion: CompletionSink<M>) {
        self.0.push(message, completion, Instant::now());
    }
}

impl<M: RemoteMessage> Drop for Sink<M> {
    fn drop(&mut self) {
        self.0.queue.lock().unwrap().open = false;
        self.0.wake.notify_one();
    }
}

/// Expire queued messages older than `MESSAGE_DELIVERY_TIMEOUT`; writers
/// expire the messages they hold. Returns once the sink is dropped and
/// every message has settled.
async fn expire_until_closed<M: RemoteMessage>(shared: &Shared<M>) {
    loop {
        let timeout = hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT);
        let deadline = shared.expire_queued(timeout);
        if shared.closed() {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)),
                if deadline.is_some() => {}
            _ = shared.wake.notified() => {}
        }
    }
}

/// Drive one stream of the session: connect while the stream holds
/// messages or messages are queued, write them, and settle the messages
/// the stream holds, in every state, by their deadlines.
async fn write_loop<M: RemoteMessage>(
    shared: Arc<Shared<M>>,
    link: impl Link,
    shutdown: CancellationToken,
) {
    let dest = link.dest();
    let session_id = link.link_id();
    let mut session = Session::new(link);
    let mut outbox = Outbox::new();
    let mut backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(10))
        .with_multiplier(2.0)
        .with_randomization_factor(0.1)
        .with_max_interval(Duration::from_secs(5))
        .with_max_elapsed_time(None)
        .build();

    loop {
        if outbox.is_empty() {
            tokio::select! {
                _ = shared.ready_for_write() => {}
                _ = shutdown.cancelled() => return,
            }
        }
        let timeout = hyperactor_config::global::get(config::MESSAGE_DELIVERY_TIMEOUT);
        // Stop trying to connect when the oldest message this writer could
        // write expires, so that a writer whose messages have all expired
        // goes idle. Held messages were posted before any queued one.
        let soon = Instant::now() + Duration::from_millis(10);
        let deadline = outbox
            .next_deadline(timeout)
            .or_else(|| shared.queue_deadline(timeout))
            .map_or(soon, |deadline| deadline.max(soon));
        let connected = tokio::select! {
            result = session.connect_by(deadline) => result,
            _ = shutdown.cancelled() => return,
        };
        let connected = match connected {
            Ok(connected) => connected,
            Err(disconnected) => {
                session = disconnected;
                shared.settle(outbox.expire(Instant::now(), timeout));
                tracing::info!(%dest, %session_id, "unordered writer: no connection before the oldest message expired");
                continue;
            }
        };
        metrics::CHANNEL_CONNECTIONS.add(
            1,
            hyperactor_telemetry::kv_pairs!(
                "transport" => dest.transport().to_string(),
                "mode" => "unordered",
                "reason" => "link connected",
            ),
        );

        let connected_at = Instant::now();
        let result = {
            let stream = connected.stream(INITIATOR_TO_ACCEPTOR);
            tokio::select! {
                result = send_connected(&shared, &stream, &mut outbox, timeout) => result,
                _ = shutdown.cancelled() => Err(SendLoopError::AppClosed),
            }
        };
        session = connected.release();
        let requeued = outbox.requeue();

        match &result {
            Ok(()) => {
                tracing::info!(%dest, %session_id, requeued, "unordered writer: connection closed")
            }
            Err(SendLoopError::AppClosed) => return,
            Err(err) => {
                tracing::info!(%dest, %session_id, requeued, error = %err, "unordered writer: connection lost")
            }
        }
        if connected_at.elapsed() > Duration::from_secs(1) {
            backoff.reset();
        }
        if let Some(delay) = backoff.next_backoff()
            && !back_off(
                &shared,
                &mut outbox,
                Instant::now() + delay,
                timeout,
                &shutdown,
            )
            .await
        {
            return;
        }
    }
}

/// Wait until `until`, expiring the messages in `outbox` as they come
/// due. Returns false if `shutdown` fires first.
async fn back_off<M: RemoteMessage>(
    shared: &Shared<M>,
    outbox: &mut Outbox<M>,
    until: Instant,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> bool {
    loop {
        let now = Instant::now();
        shared.settle(outbox.expire(now, timeout));
        if now >= until {
            return true;
        }
        let wake = outbox
            .next_deadline(timeout)
            .map_or(until, |deadline| deadline.min(until));
        tokio::select! {
            _ = tokio::time::sleep_until(wake) => {}
            _ = shutdown.cancelled() => return false,
        }
    }
}

/// Run the send protocol on one connection until it ends: rewrite the
/// messages `outbox` holds, then take new ones. Every response other than
/// an ack ends the connection; the caller reconnects. Cancel-safe.
async fn send_connected<M, R, W>(
    shared: &Shared<M>,
    stream: &TaggedStream<R, W>,
    outbox: &mut Outbox<M>,
    timeout: Duration,
) -> Result<(), SendLoopError>
where
    M: RemoteMessage,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let mut pending: Option<Completion<W, serde_multipart::Frame>> = None;

    loop {
        if pending.is_none()
            && let Some(frame) = outbox.next_unwritten()
        {
            pending = Some(stream.write(frame.framed()));
        }
        let deadline = outbox.next_deadline(timeout);

        tokio::select! {
            biased;

            response = stream.next() => {
                let buffer = match response {
                    Ok(Some(buffer)) => buffer,
                    Ok(None) => return Ok(()),
                    Err(err) => return Err(SendLoopError::Io(err.into())),
                };
                match deserialize_response(buffer).map_err(|err| SendLoopError::Io(err.into()))? {
                    NetRxResponse::Ack(seq) => shared.settle(outbox.accept(seq)),
                    NetRxResponse::Closed => return Err(SendLoopError::ServerClosed),
                    other => {
                        return Err(SendLoopError::Io(anyhow::anyhow!(
                            "unexpected response on unordered session: {other:?}"
                        )));
                    }
                }
            }

            // Ahead of the write, so that continuous writes cannot starve it.
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)),
                if deadline.is_some() => {
                shared.settle(outbox.expire(Instant::now(), timeout));
            }

            result = async { pending.as_mut().unwrap().drive().await }, if pending.is_some() => {
                result.map_err(|err| SendLoopError::Io(err.into()))?;
                pending = None;
            }

            queued = shared.take(), if pending.is_none() && !outbox.has_unwritten() => {
                match outbox.number(queued, stream.max_frame_len()) {
                    Ok(frame) => pending = Some(stream.write(frame.framed())),
                    Err(rejection) => shared.settle(vec![rejection]),
                }
            }
        }
    }
}

/// Client state shared by the sink, the writers, and the task that
/// expires queued messages.
struct Shared<M: RemoteMessage> {
    queue: Mutex<Queue<M>>,
    /// Signaled when a message is queued.
    ready: Notify,
    /// Wakes the close task: when the queue becomes nonempty, when the
    /// sink is dropped, and, after that, when messages settle.
    wake: Notify,
}

struct Queue<M: RemoteMessage> {
    /// Posted messages that no stream has taken yet, oldest first.
    messages: VecDeque<Queued<M>>,
    /// Messages posted and not yet settled, wherever they are held.
    unsettled: usize,
    /// Whether the sink still exists.
    open: bool,
}

struct Queued<M: RemoteMessage> {
    message: M,
    posted_at: Instant,
    completion: CompletionSink<M>,
}

/// A message to settle after any lock is released, so that completion
/// callbacks never run under it.
enum Settlement<M: RemoteMessage> {
    Accept(CompletionSink<M>),
    Reject(CompletionSink<M>, SendError<M>),
}

impl<M: RemoteMessage> Settlement<M> {
    fn expired(completion: CompletionSink<M>, message: M, timeout: Duration) -> Self {
        Self::Reject(
            completion,
            SendError {
                error: ChannelError::Timeout(timeout),
                message,
                reason: Some(SendErrorReason::DeliveryTimeout(timeout)),
            },
        )
    }

    fn run(self) {
        match self {
            Self::Accept(completion) => completion.accept(),
            Self::Reject(completion, error) => completion.reject(error),
        }
    }
}

impl<M: RemoteMessage> Shared<M> {
    fn new() -> Self {
        Self {
            queue: Mutex::new(Queue {
                messages: VecDeque::new(),
                unsettled: 0,
                open: true,
            }),
            ready: Notify::new(),
            wake: Notify::new(),
        }
    }

    // HOT PATH: Be mindful of performance when making changes here.
    fn push(&self, message: M, completion: CompletionSink<M>, posted_at: Instant) {
        let was_empty = {
            let mut queue = self.queue.lock().unwrap();
            let was_empty = queue.messages.is_empty();
            queue.messages.push_back(Queued {
                message,
                posted_at,
                completion,
            });
            queue.unsettled += 1;
            was_empty
        };
        self.ready.notify_waiters();
        if was_empty {
            self.wake.notify_one();
        }
    }

    /// Wait until a message is queued.
    async fn ready_for_write(&self) {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.queue.lock().unwrap().messages.is_empty() {
                return;
            }
            notified.await;
        }
    }

    /// Take the oldest queued message, waiting until there is one.
    /// Cancel-safe: nothing is taken until the last await.
    async fn take(&self) -> Queued<M> {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(queued) = self.queue.lock().unwrap().messages.pop_front() {
                return queued;
            }
            notified.await;
        }
    }

    /// When the oldest queued message expires, if any message is queued.
    fn queue_deadline(&self, timeout: Duration) -> Option<Instant> {
        self.queue
            .lock()
            .unwrap()
            .messages
            .front()
            .map(|queued| queued.posted_at + timeout)
    }

    /// Reject the queued messages posted `timeout` or more ago. Returns
    /// when the next queued message expires, if any message is queued.
    fn expire_queued(&self, timeout: Duration) -> Option<Instant> {
        let now = Instant::now();
        let mut settlements = Vec::new();
        let deadline = {
            let mut queue = self.queue.lock().unwrap();
            while let Some(queued) = queue
                .messages
                .pop_front_if(|queued| queued.posted_at + timeout <= now)
            {
                settlements.push(Settlement::expired(
                    queued.completion,
                    queued.message,
                    timeout,
                ));
            }
            queue
                .messages
                .front()
                .map(|queued| queued.posted_at + timeout)
        };
        warn_expired(settlements.len(), timeout);
        self.settle(settlements);
        deadline
    }

    /// Whether the sink is dropped and every message has settled.
    fn closed(&self) -> bool {
        let queue = self.queue.lock().unwrap();
        !queue.open && queue.unsettled == 0
    }

    /// Run `settlements`. Every accepted and rejected message goes through
    /// here.
    fn settle(&self, settlements: Vec<Settlement<M>>) {
        if settlements.is_empty() {
            return;
        }
        let count = settlements.len();
        for settlement in settlements {
            settlement.run();
        }
        let closed = {
            let mut queue = self.queue.lock().unwrap();
            queue.unsettled -= count;
            !queue.open && queue.unsettled == 0
        };
        if closed {
            self.wake.notify_one();
        }
    }
}

/// The messages that one stream has numbered and not yet settled, owned by
/// the stream's writer. `written` followed by `unwritten` is in number
/// order, which is also posting order.
struct Outbox<M: RemoteMessage> {
    /// Written on the current connection, possibly in part.
    written: VecDeque<Numbered<M>>,
    /// Numbered, but not yet written on the current connection:
    /// retransmissions.
    unwritten: VecDeque<Numbered<M>>,
    next_seq: u64,
}

struct Numbered<M: RemoteMessage> {
    seq: u64,
    /// Only the frame is kept; a rejected message is recovered from it.
    frame: serde_multipart::Message,
    posted_at: Instant,
    completion: CompletionSink<M>,
}

impl<M: RemoteMessage> Outbox<M> {
    fn new() -> Self {
        Self {
            written: VecDeque::new(),
            unwritten: VecDeque::new(),
            next_seq: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.written.is_empty() && self.unwritten.is_empty()
    }

    fn has_unwritten(&self) -> bool {
        !self.unwritten.is_empty()
    }

    /// Mark the oldest unwritten message as written and return its frame.
    /// An ack that arrives while the frame is being written accepts it.
    fn next_unwritten(&mut self) -> Option<serde_multipart::Message> {
        let numbered = self.unwritten.pop_front()?;
        let frame = numbered.frame.clone();
        self.written.push_back(numbered);
        Some(frame)
    }

    /// Serialize `queued` with the next sequence number and hold it as
    /// written. Returns the frame to write, or the rejection of a message
    /// that cannot be serialized within `max_frame_len`, which consumes no
    /// sequence number.
    #[expect(
        clippy::result_large_err,
        reason = "the rejection carries the message back to its sender"
    )]
    fn number(
        &mut self,
        queued: Queued<M>,
        max_frame_len: usize,
    ) -> Result<serde_multipart::Message, Settlement<M>> {
        assert!(
            self.unwritten.is_empty(),
            "a stream numbers new messages only after rewriting the ones it holds"
        );
        let Queued {
            message,
            posted_at,
            completion,
        } = queued;
        let frame = Frame::Message(self.next_seq, message);
        let reason = match serde_multipart::serialize_bincode(&frame) {
            Ok(serialized) if serialized.frame_len() <= max_frame_len => {
                record_send_metrics(&serialized);
                self.written.push_back(Numbered {
                    seq: self.next_seq,
                    frame: serialized.clone(),
                    posted_at,
                    completion,
                });
                self.next_seq += 1;
                return Ok(serialized);
            }
            Ok(serialized) => SendErrorReason::OversizedFrame {
                len: serialized.frame_len(),
                max: max_frame_len,
            },
            Err(err) => SendErrorReason::Other(format!("serialization error: {err}")),
        };
        tracing::error!(reason = %reason, "unordered session: rejecting message");
        let Frame::Message(_, message) = frame;
        Err(Settlement::Reject(
            completion,
            SendError {
                error: ChannelError::Closed,
                message,
                reason: Some(reason),
            },
        ))
    }

    /// Accept every held message numbered `seq` or below.
    fn accept(&mut self, seq: u64) -> Vec<Settlement<M>> {
        let mut settlements = Vec::new();
        for held in [&mut self.written, &mut self.unwritten] {
            while let Some(numbered) = held.pop_front_if(|numbered| numbered.seq <= seq) {
                settlements.push(Settlement::Accept(numbered.completion));
            }
        }
        settlements
    }

    /// Reject the held messages posted `timeout` or more before `now`.
    fn expire(&mut self, now: Instant, timeout: Duration) -> Vec<Settlement<M>> {
        let mut settlements = Vec::new();
        for held in [&mut self.written, &mut self.unwritten] {
            while let Some(numbered) =
                held.pop_front_if(|numbered| numbered.posted_at + timeout <= now)
            {
                let Frame::Message(_, message) =
                    serde_multipart::deserialize_bincode::<Frame<M>>(numbered.frame)
                        .expect("a frame serialized by this session deserializes");
                settlements.push(Settlement::expired(numbered.completion, message, timeout));
            }
        }
        warn_expired(settlements.len(), timeout);
        settlements
    }

    /// Make every held message unwritten, for the next connection. Returns
    /// how many were written on the connection that ended.
    fn requeue(&mut self) -> usize {
        let requeued = self.written.len();
        self.written.append(&mut self.unwritten);
        std::mem::swap(&mut self.written, &mut self.unwritten);
        requeued
    }

    /// When the oldest held message expires, if the stream holds any.
    fn next_deadline(&self, timeout: Duration) -> Option<Instant> {
        self.written
            .front()
            .or_else(|| self.unwritten.front())
            .map(|numbered| numbered.posted_at + timeout)
    }
}

fn warn_expired(count: usize, timeout: Duration) {
    if count > 0 {
        tracing::warn!(
            count,
            ?timeout,
            "unordered session: messages not acknowledged within delivery timeout"
        );
    }
}

fn record_send_metrics(frame: &serde_multipart::Message) {
    let size = frame.frame_len();
    metrics::REMOTE_MESSAGE_SEND_SIZE.record(size as f64, &[]);
}

#[cfg(test)]
mod tests {
    use timed_test::async_timed_test;

    use super::*;
    use crate::channel::ChannelAddr;
    use crate::channel::ChannelRx;
    use crate::channel::Rx;
    use crate::channel::Tx;
    use crate::channel::unordered;
    use crate::testing::tcp_relay::TcpRelay;
    use crate::testing::tcp_relay::tcp_loopback;
    use crate::testing::tcp_relay::unreachable_tcp_addr;

    /// Serve at `addr`, retrying while a server that was just stopped
    /// still holds the address.
    async fn serve_at<M: RemoteMessage>(addr: &ChannelAddr) -> ChannelRx<M> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match unordered::serve::<M>(addr.clone()) {
                Ok((_, rx)) => return rx,
                Err(err) if Instant::now() < deadline => {
                    tracing::info!(%addr, %err, "retrying serve");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => panic!("serve {addr}: {err}"),
            }
        }
    }

    async fn assert_nothing_more<M: RemoteMessage + std::fmt::Debug>(rx: &mut ChannelRx<M>) {
        if let Ok(message) = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
            panic!("unexpected delivery: {message:?}");
        }
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn delivers_every_message_once_across_streams() {
        let (addr, mut rx) = unordered::serve::<u64>(tcp_loopback()).unwrap();
        let tx = unordered::dial::<u64>(addr, 4).unwrap();

        for i in 0..100 {
            tx.post(i);
        }
        let last = tx.try_post(100);
        let mut received = Vec::new();
        for _ in 0..=100 {
            received.push(rx.recv().await.unwrap());
        }
        received.sort();
        assert_eq!(received, (0..=100).collect::<Vec<_>>());
        last.await.expect("the last message should be acknowledged");
        assert_nothing_more(&mut rx).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn reconnects_to_server_restarted_at_same_address() {
        let (addr, mut rx) = unordered::serve::<u64>(tcp_loopback()).unwrap();
        let tx = unordered::dial::<u64>(addr.clone(), 1).unwrap();
        tx.send(1).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), 1);

        // The new server has no record of the stream, whose next sequence
        // number is past 0. It must accept the stream anyway.
        rx.join().await;
        let mut rx = serve_at::<u64>(&addr).await;
        tx.send(2).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), 2);
        assert!(!tx.status().borrow().is_closed());
    }

    /// Post `count` messages through a relay with acks out of reach,
    /// receive them all, and drop the connections. The server acks what
    /// each stream delivered when the stream reconnects, so every message
    /// is accepted without posting anything else, and none is delivered
    /// twice.
    async fn accepts_delivered_messages_after_connection_loss(num_streams: usize, count: u64) {
        let config = hyperactor_config::global::lock();
        let _every = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1_000_000);
        let _interval =
            config.override_key(config::MESSAGE_ACK_TIME_INTERVAL, Duration::from_secs(600));

        let (addr, mut rx) = unordered::serve::<u64>(tcp_loopback()).unwrap();
        let mut relay = TcpRelay::start(&addr).await;
        let tx = unordered::dial::<u64>(relay.addr.clone(), num_streams).unwrap();
        let completions: Vec<_> = (0..count).map(|i| tx.try_post(i)).collect();
        let mut received = Vec::new();
        for _ in 0..count {
            received.push(rx.recv().await.unwrap());
        }
        received.sort();
        assert_eq!(received, (0..count).collect::<Vec<_>>());

        relay.drop_connections();
        for (i, completion) in completions.into_iter().enumerate() {
            completion
                .await
                .unwrap_or_else(|err| panic!("message {i} should be accepted: {err:?}"));
        }
        assert_nothing_more(&mut rx).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn retransmits_after_connection_loss_without_duplicates() {
        accepts_delivered_messages_after_connection_loss(1, 3).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn retransmits_across_streams_after_connection_loss_without_duplicates() {
        accepts_delivered_messages_after_connection_loss(4, 100).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn rejects_expired_message_and_stays_active() {
        let config = hyperactor_config::global::lock();
        let _timeout =
            config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(1));

        let addr = unreachable_tcp_addr();
        let tx = unordered::dial::<u64>(addr.clone(), 1).unwrap();
        let err = tx.send(0).await.unwrap_err();
        assert_eq!(err.message, 0);
        assert_eq!(
            err.reason,
            Some(SendErrorReason::DeliveryTimeout(Duration::from_secs(1)))
        );
        assert!(!tx.status().borrow().is_closed());

        let mut rx = serve_at::<u64>(&addr).await;
        tx.send(1).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), 1);
        assert_nothing_more(&mut rx).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn rejects_held_message_on_time_while_peer_unreachable() {
        let timeout = Duration::from_secs(3);
        let config = hyperactor_config::global::lock();
        let _timeout = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, timeout);
        let _every = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1_000_000);
        let _interval =
            config.override_key(config::MESSAGE_ACK_TIME_INTERVAL, Duration::from_secs(600));

        let (addr, mut rx) = unordered::serve::<u64>(tcp_loopback()).unwrap();
        let mut relay = TcpRelay::start(&addr).await;
        let tx = unordered::dial::<u64>(relay.addr.clone(), 1).unwrap();
        let posted_at = Instant::now();
        let completion = tx.try_post(0);
        assert_eq!(rx.recv().await.unwrap(), 0);

        // Delivered, but not acknowledged. The relay now accepts and
        // closes every connection, so the writer holds the message while
        // it connects, loses the connection, and backs off.
        relay.restart_peer_at(&unreachable_tcp_addr());
        let err = completion.await.unwrap_err();
        let elapsed = posted_at.elapsed();
        assert_eq!(err.message, 0);
        assert_eq!(err.reason, Some(SendErrorReason::DeliveryTimeout(timeout)));
        assert!(
            elapsed >= timeout && elapsed < timeout + Duration::from_millis(1500),
            "rejected after {elapsed:?}, expected about {timeout:?}"
        );
        assert!(!tx.status().borrow().is_closed());
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn rejects_oversized_message_and_delivers_the_next() {
        let config = hyperactor_config::global::lock();
        let _max = config.override_key(config::CODEC_MAX_FRAME_LENGTH, 1024);

        let (addr, mut rx) = unordered::serve::<Vec<u8>>(tcp_loopback()).unwrap();
        let tx = unordered::dial::<Vec<u8>>(addr, 1).unwrap();
        let err = tx.send(vec![0; 4096]).await.unwrap_err();
        assert!(
            matches!(err.reason, Some(SendErrorReason::OversizedFrame { .. })),
            "expected an oversized frame, got {:?}",
            err.reason
        );

        tx.send(vec![1; 8]).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), vec![1; 8]);
        assert!(!tx.status().borrow().is_closed());
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn delivers_to_local_duplex_endpoint() {
        // Gateways dial in-process hosts' duplex frontends with simplex
        // channels; the Local duplex server must accept unordered sessions.
        let mut server = crate::channel::duplex::serve::<u64, u64>(
            ChannelAddr::any(crate::channel::ChannelTransport::Local),
            None,
        )
        .unwrap();
        let tx = unordered::dial::<u64>(server.addr().clone(), 2).unwrap();
        for i in 0..10 {
            tx.post(i);
        }
        let last = tx.try_post(10);

        let (mut rx, _tx) = server.accept().await.unwrap();
        let mut received = Vec::new();
        for _ in 0..=10 {
            received.push(rx.recv().await.unwrap());
        }
        received.sort();
        assert_eq!(received, (0..=10).collect::<Vec<_>>());
        last.await.expect("the last message should be acknowledged");
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn closes_after_drop_once_messages_settle() {
        let (addr, mut rx) = unordered::serve::<u64>(tcp_loopback()).unwrap();
        let tx = unordered::dial::<u64>(addr, 1).unwrap();
        let mut status = tx.status().clone();
        tx.post(7);
        drop(tx);

        assert_eq!(rx.recv().await.unwrap(), 7);
        status.wait_for(TxStatus::is_closed).await.unwrap();
    }

    fn queued<M: RemoteMessage>(message: M, posted_at: Instant) -> Queued<M> {
        Queued {
            message,
            posted_at,
            completion: CompletionSink::ignore(),
        }
    }

    /// An outbox holding `values`, numbered from 0 and all written.
    fn outbox_of(values: &[u64], posted_at: Instant) -> Outbox<u64> {
        let mut outbox = Outbox::new();
        for value in values {
            outbox
                .number(queued(*value, posted_at), usize::MAX)
                .unwrap_or_else(|_| panic!("{value} should be numbered"));
        }
        outbox
    }

    fn seqs<M: RemoteMessage>(held: &VecDeque<Numbered<M>>) -> Vec<u64> {
        held.iter().map(|numbered| numbered.seq).collect()
    }

    fn frame_seq<M: RemoteMessage>(frame: serde_multipart::Message) -> u64 {
        let Frame::Message(seq, _) =
            serde_multipart::deserialize_bincode::<Frame<M>>(frame).expect("frames deserialize");
        seq
    }

    #[test]
    fn outbox_numbers_consecutively_and_rejections_consume_no_number() {
        let now = Instant::now();
        let mut outbox = Outbox::<Vec<u8>>::new();
        let first = outbox.number(queued(vec![1; 8], now), 1024);
        let rejected = outbox.number(queued(vec![2; 4096], now), 1024);
        let second = outbox.number(queued(vec![3; 8], now), 1024);

        assert_eq!(frame_seq::<Vec<u8>>(first.ok().unwrap()), 0);
        assert_eq!(frame_seq::<Vec<u8>>(second.ok().unwrap()), 1);
        match rejected {
            Err(Settlement::Reject(_, err)) => {
                assert_eq!(err.message, vec![2; 4096]);
                assert!(
                    matches!(err.reason, Some(SendErrorReason::OversizedFrame { .. })),
                    "expected an oversized frame, got {:?}",
                    err.reason
                );
            }
            _ => panic!("the oversized message should be rejected"),
        }
        assert_eq!(seqs(&outbox.written), vec![0, 1]);
        assert_eq!(outbox.next_seq, 2);
    }

    #[test]
    fn outbox_accepts_across_written_and_unwritten() {
        let mut outbox = outbox_of(&[10, 11, 12, 13, 14], Instant::now());
        assert_eq!(outbox.requeue(), 5);
        outbox.next_unwritten().unwrap();
        outbox.next_unwritten().unwrap();
        assert_eq!(seqs(&outbox.written), vec![0, 1]);
        assert_eq!(seqs(&outbox.unwritten), vec![2, 3, 4]);

        let accepted = outbox.accept(3);
        assert_eq!(accepted.len(), 4);
        assert!(
            accepted
                .iter()
                .all(|settlement| matches!(settlement, Settlement::Accept(_)))
        );
        assert!(outbox.written.is_empty());
        assert_eq!(seqs(&outbox.unwritten), vec![4]);

        assert!(outbox.accept(3).is_empty());
    }

    #[test]
    fn outbox_requeue_preserves_order() {
        let mut outbox = outbox_of(&[10, 11, 12], Instant::now());
        assert_eq!(outbox.requeue(), 3);
        assert_eq!(frame_seq::<u64>(outbox.next_unwritten().unwrap()), 0);
        assert_eq!(outbox.requeue(), 1);
        let rewritten: Vec<u64> = std::iter::from_fn(|| outbox.next_unwritten())
            .map(frame_seq::<u64>)
            .collect();
        assert_eq!(rewritten, vec![0, 1, 2]);
        assert!(!outbox.has_unwritten());
    }

    #[test]
    fn outbox_expires_only_expired_messages_from_the_front() {
        let timeout = Duration::from_secs(10);
        let start = Instant::now();
        let mut outbox = Outbox::new();
        for (i, value) in [10u64, 11, 12].into_iter().enumerate() {
            outbox
                .number(
                    queued(value, start + Duration::from_secs(i as u64)),
                    usize::MAX,
                )
                .unwrap_or_else(|_| panic!("{value} should be numbered"));
        }
        outbox.requeue();
        outbox.next_unwritten().unwrap();
        assert_eq!(outbox.next_deadline(timeout), Some(start + timeout));

        let expired = outbox.expire(start + timeout + Duration::from_millis(1500), timeout);
        let messages: Vec<u64> = expired
            .into_iter()
            .map(|settlement| match settlement {
                Settlement::Reject(_, err) => {
                    assert_eq!(err.reason, Some(SendErrorReason::DeliveryTimeout(timeout)));
                    err.message
                }
                Settlement::Accept(_) => panic!("expired messages are rejected"),
            })
            .collect();
        assert_eq!(messages, vec![10, 11]);
        assert!(outbox.written.is_empty());
        assert_eq!(seqs(&outbox.unwritten), vec![2]);
        assert_eq!(
            outbox.next_deadline(timeout),
            Some(start + Duration::from_secs(2) + timeout)
        );
    }
}
