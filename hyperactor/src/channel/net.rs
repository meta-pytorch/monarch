/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A simple socket channel implementation, using a simple framing
//! protocol with no multiplexing. TCP channels serialize messages
//! using bincode, and frames each message with a 32-bit length,
//! followed by that many bytes of serialized data, e.g.:
//!
//! ```text
//! +---- len: u32 ----+---- data --------------------+
//! | \x00\x00\x00\x0b |  11-bytes of serialized data |
//! +------------------+------------------------------+
//! ```
//!
//! Thus, each socket connection is a sequence of such framed messages.
use std::any::type_name;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::mem::take;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::task::Poll;

use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use enum_as_inner::EnumAsInner;
use futures::Sink;
use futures::SinkExt;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use serde::de::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tokio::sync::watch;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;
use tokio_util::codec::Framed;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::net::Listener;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::Message;
use crate::RemoteMessage;
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

pub(crate) trait Stream:
    AsyncRead + AsyncWrite + Unpin + Send + Sync + Debug + 'static
{
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + Debug + 'static> Stream for S {}

/// Link represents a network link through which a stream may be established or accepted.
// TODO: unify this with server connections
#[async_trait]
pub(crate) trait Link: Send + Sync + Debug {
    /// The underlying stream type.
    type Stream: Stream;

    /// The address of the link's destination.
    // Consider embedding the session ID in this address, making it truly persistent.
    fn dest(&self) -> ChannelAddr;

    /// Connect to the destination, returning a connected stream.
    async fn connect(&self) -> Result<Self::Stream, ClientError>;
}

/// Frames are the messages sent between clients and servers over sessions.
#[derive(Debug, Serialize, Deserialize, EnumAsInner, PartialEq)]
enum Frame<M> {
    /// Initialize a session with the given id.
    Init(u64),

    /// Send a message with the provided sequence number.
    Message(u64, M),
}

#[derive(thiserror::Error, Debug)]
enum FrameError<E> {
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error("framer: {0}")]
    Framer(E),
    #[error("eof")]
    Eof,
}

impl<M: RemoteMessage> Frame<M> {
    async fn send<T, I, U>(&self, framer: &mut Framed<T, U>) -> Result<(), FrameError<U::Error>>
    where
        T: AsyncWrite + std::marker::Unpin,
        U: Encoder<I>,
        I: From<Vec<u8>>,
        U::Error: From<io::Error>,
    {
        let data = bincode::serialize(self).expect("unexpected serialization error");
        framer.send(data.into()).await.map_err(FrameError::Framer)
    }

    async fn next<T, U>(
        stream: &mut SplitStream<Framed<T, U>>,
    ) -> Result<Self, FrameError<U::Error>>
    where
        T: AsyncRead + std::marker::Unpin,
        U: Decoder,
        U::Item: Into<Vec<u8>>,
    {
        match tokio_stream::StreamExt::next(stream).await {
            Some(Ok(data)) => Ok(bincode::deserialize(&data.into())?),
            Some(Err(error)) => Err(FrameError::Framer(error)),
            None => Err(FrameError::Eof),
        }
    }
}

fn serialize_ack(seq: u64) -> Bytes {
    let mut data = BytesMut::with_capacity(8);
    data.put_slice(&seq.to_be_bytes());
    data.freeze()
}

fn deserialize_ack(data: BytesMut) -> Result<u64, usize> {
    let slice = data.as_ref();
    let array: [u8; 8] = slice.try_into().map_err(|_| slice.len())?;
    Ok(u64::from_be_bytes(array))
}

fn build_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(config::global::get(config::CODEC_MAX_FRAME_LENGTH))
        .new_codec()
}

#[derive(thiserror::Error, Debug)]
enum HandshakeError {
    #[error(transparent)]
    DeserializationFailed(bincode::Error),
    #[error("sending init frame failed: {0}")]
    SendFailed(String),
    #[error("timed out waiting for handshake response")]
    Timeout,
    #[error("channel closed waiting for handshake response")]
    ChannelClosed,
    #[error(transparent)]
    IoError(io::Error),
    #[error("mismatched rx session ids: {0} (stored) vs. {1} (received)")]
    MismatchedRxSessionIds(u64, u64),
}

async fn do_handshake<S: Stream, M: Serialize>(
    tx_session_id: u64,
    rx_session_id: &mut Option<u64>,
    sink: &mut SplitSink<Framed<S, LengthDelimitedCodec>, Bytes>,
    stream: &mut SplitStream<Framed<S, LengthDelimitedCodec>>,
) -> Result<(), HandshakeError> {
    let data = bincode::serialize(&Frame::<M>::Init(tx_session_id))
        .expect("unexpected serialization error");

    if let Err(err) = sink.send(data.into()).await {
        return Err(HandshakeError::SendFailed(format!("{}", err)));
    }

    let handshake_response = tokio::select! {
        _ = RealClock.sleep(config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)) => None,
        response = tokio_stream::StreamExt::next(stream) => Some(response)
    };

    match handshake_response {
        None => Err(HandshakeError::Timeout),
        Some(None) => Err(HandshakeError::ChannelClosed),
        Some(Some(handshake_response)) => match handshake_response {
            Err(err) => Err(HandshakeError::IoError(err)),
            Ok(handshake_response) => match bincode::deserialize::<u64>(&handshake_response) {
                Err(err) => Err(HandshakeError::DeserializationFailed(err)),
                Ok(received_rx_session_id) => {
                    if rx_session_id
                        .is_some_and(|rx_session_id| rx_session_id != received_rx_session_id)
                    {
                        Err(HandshakeError::MismatchedRxSessionIds(
                            rx_session_id.unwrap(),
                            received_rx_session_id,
                        ))
                    } else {
                        rx_session_id.replace(received_rx_session_id);
                        Ok(())
                    }
                }
            },
        },
    }
}

/// A Tx implemented on top of a Link. The Tx manages the link state,
/// reconnections, etc.
#[derive(Debug)]
pub(crate) struct NetTx<M: Message> {
    sender: mpsc::UnboundedSender<(M, oneshot::Sender<M>, Instant)>,
    dest: ChannelAddr,
    status: watch::Receiver<TxStatus>,
}

impl<M: RemoteMessage> NetTx<M> {
    /// Creates a new session, and assigns it a guid.
    fn new(link: impl Link + 'static) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let dest = link.dest();
        let (notify, status) = watch::channel(TxStatus::Active);

        let tx = Self {
            sender,
            dest,
            status,
        };
        crate::init::get_runtime().spawn(Self::run(link, receiver, notify));
        tx
    }

    // TODO(T216506659) Simplify this function as it is getting too long and
    // hard to maintain.
    async fn run(
        link: impl Link,
        mut receiver: mpsc::UnboundedReceiver<(M, oneshot::Sender<M>, Instant)>,
        notify: watch::Sender<TxStatus>,
    ) {
        // If we can't deliver a message within this limit consider
        // `link` broken and return.

        #[derive(Debug)]
        struct QueuedMessage<M: RemoteMessage> {
            seq: u64,
            data: Bytes,
            received_at: Instant,
            return_channel: oneshot::Sender<M>,
        }

        #[derive(Debug)]
        struct Outbox<'a, M: RemoteMessage> {
            // The seq number of the next new message put into outbox. Requeued
            // unacked messages should still use their already assigned seq
            // numbers.
            next_seq: u64,
            deque: VecDeque<QueuedMessage<M>>,
            log_id: &'a str,
        }

        impl<'a, M: RemoteMessage> Outbox<'a, M> {
            fn new(log_id: &'a str) -> Self {
                Self {
                    next_seq: 0,
                    deque: VecDeque::new(),
                    log_id,
                }
            }

            fn is_expired(&self) -> bool {
                match self.deque.front() {
                    None => false,
                    Some(msg) => {
                        msg.received_at.elapsed()
                            > config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
                    }
                }
            }

            fn is_empty(&self) -> bool {
                self.deque.is_empty()
            }

            // Send the oldest message in the outbox, but do not remove it from
            // the outbox. Return error if the outbox is empty.
            async fn send_message<T: Sink<Bytes> + Unpin>(&self, sink: &mut T) -> Result<(), String>
            where
                T::Error: fmt::Display,
            {
                let data = self
                    .deque
                    .front()
                    .ok_or_else(|| {
                        format!(
                            "{}: unexpected: send_message cannot be used when outbox is empty",
                            self.log_id,
                        )
                    })?
                    .data
                    .clone();
                sink.send(data).await.map_err(|e| e.to_string())?;
                Ok(())
            }

            fn front_size(&self) -> Option<usize> {
                self.deque.front().map(|msg| msg.data.len())
            }

            fn pop_front(&mut self) -> Option<QueuedMessage<M>> {
                self.deque.pop_front()
            }

            fn push_back(
                &mut self,
                (message, return_channel, received_at): (M, oneshot::Sender<M>, Instant),
            ) -> Result<(), String> {
                assert!(
                    self.deque.back().is_none_or(|msg| msg.seq < self.next_seq),
                    "{}: unexpected: seq should be in ascending order, but got {:?} vs {}",
                    self.log_id,
                    self.deque.back(),
                    self.next_seq
                );

                let frame = Frame::Message(self.next_seq, message);
                let data: Bytes = bincode::serialize(&frame)
                    .map_err(|e| format!("serialization error: {e}"))?
                    .into();
                metrics::REMOTE_MESSAGE_SEND_SIZE.record(data.len() as f64, &[]);

                self.deque.push_back(QueuedMessage {
                    seq: self.next_seq,
                    data,
                    received_at,
                    return_channel,
                });
                self.next_seq += 1;
                Ok(())
            }

            fn requeue_unacked(&mut self, unacked: Unacked<M>) {
                match (unacked.deque.back(), self.deque.front()) {
                    (Some(last), Some(first)) => {
                        assert!(
                            last.seq < first.seq,
                            "{}: seq should be in ascending order, but got {} vs {:?}",
                            self.log_id,
                            last.seq,
                            first.seq,
                        );
                    }
                    _ => (),
                }

                let mut outbox = unacked.deque;
                outbox.append(&mut self.deque);
                self.deque = outbox;
            }
        }

        #[derive(Debug)]
        struct Unacked<'a, M: RemoteMessage> {
            deque: VecDeque<QueuedMessage<M>>,
            largest_acked: Option<u64>,
            log_id: &'a str,
        }

        impl<'a, M: RemoteMessage> Unacked<'a, M> {
            fn new(largest_acked: Option<u64>, log_id: &'a str) -> Self {
                Self {
                    deque: VecDeque::new(),
                    largest_acked,
                    log_id,
                }
            }

            fn push_back(&mut self, message: QueuedMessage<M>) {
                assert!(
                    self.deque.back().is_none_or(|msg| msg.seq < message.seq),
                    "{}: seq should be in ascending order, but got {:?} vs {}",
                    self.log_id,
                    self.deque.back(),
                    message.seq
                );

                if let Some(largest) = self.largest_acked {
                    // Note: some scenarios of why this if branch could happen:
                    //
                    // message.0 <= largest could happen in the following scenario:
                    //
                    // 1. NetTx sent seq=2 and seq=3.
                    // 2. NetRx received messages and put them on its mspc channel.
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
            fn prune(&mut self, acked: u64) {
                assert!(
                    self.largest_acked.unwrap_or(0) <= acked,
                    "{}: received out-of-order ack; received: {}; stored largest: {:?}",
                    self.log_id,
                    acked,
                    self.largest_acked,
                );

                self.largest_acked = Some(acked);
                let deque = &mut self.deque;
                while let Some(msg) = deque.front() {
                    if msg.seq <= acked {
                        deque.pop_front();
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
                    Some(msg) if msg.received_at.elapsed() > config::global::get(config::MESSAGE_DELIVERY_TIMEOUT)
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
                                msg.received_at.clone()
                                    + config::global::get(config::MESSAGE_DELIVERY_TIMEOUT),
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

        #[derive(Debug)]
        struct Deliveries<'a, M: RemoteMessage> {
            outbox: Outbox<'a, M>,
            unacked: Unacked<'a, M>,
        }

        #[derive(Debug)]
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
            fn init(log_id: &'a str) -> Self {
                Self::Running(Deliveries {
                    outbox: Outbox::new(log_id),
                    unacked: Unacked::new(None, log_id),
                })
            }
        }

        enum Conn<S: Stream> {
            /// Disconnected.
            Disconnected(Box<dyn Backoff + Send>),
            /// Connected and ready to go.
            Connected {
                sink: SplitSink<Framed<S, LengthDelimitedCodec>, Bytes>,
                stream: SplitStream<Framed<S, LengthDelimitedCodec>>,
            },
        }

        impl<S: Stream> Conn<S> {
            fn reconnect_with_default() -> Self {
                Self::Disconnected(Box::new(
                    ExponentialBackoffBuilder::new()
                        .with_initial_interval(Duration::from_millis(50))
                        .with_multiplier(2.0)
                        .with_randomization_factor(0.1)
                        .with_max_interval(Duration::from_millis(1000))
                        .build(),
                ))
            }

            fn reconnect(backoff: impl Backoff + Send + 'static) -> Self {
                Self::Disconnected(Box::new(backoff))
            }
        }

        let session_id = rand::random();
        let mut rx_session_id = None;
        let log_id = format!("session {}.{}", link.dest(), session_id);
        let mut state = State::init(&log_id);
        let mut conn = Conn::reconnect_with_default();

        let (state, conn) = loop {
            (state, conn) = match (state, conn) {
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
                            let error_msg = format!(
                                "session {}.{}: failed to push message to outbox: {}",
                                link.dest(),
                                session_id,
                                err
                            );
                            tracing::error!(error_msg);
                            (
                                State::Closing {
                                    deliveries: Deliveries { outbox, unacked },
                                    reason: error_msg,
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
                        mut unacked,
                    }),
                    Conn::Connected {
                        mut sink,
                        mut stream,
                    },
                ) => {
                    tokio::select! {
                        // If acking message takes too long, consider the link broken.
                        _ = unacked.wait_for_timeout(), if !unacked.is_empty() => {
                            let error_msg = format!(
                                "session {}.{}: failed to receive ack within timeout {} secs; link is currently connected",
                                link.dest(),
                                session_id,
                                config::global::get(config::MESSAGE_DELIVERY_TIMEOUT).as_secs(),
                            );
                            tracing::error!(error_msg);
                            (State::Closing {
                                deliveries: Deliveries{outbox, unacked},
                                reason: error_msg,
                            }, Conn::Connected { sink, stream })
                        }
                        // tokio_stream::StreamExt::next is cancel safe.
                        ack_result = tokio_stream::StreamExt::next(&mut stream) => {
                            match ack_result {
                                Some(Ok(data)) => {
                                    match deserialize_ack(data) {
                                        Ok(ack) => {
                                            unacked.prune(ack);
                                            (State::Running(Deliveries { outbox, unacked }), Conn::Connected { sink, stream })
                                        }
                                        Err(len) => {
                                            let error_msg = format!(
                                                "session {}.{}: ack message size is not 8 bytes. It is {} bytes",
                                                link.dest(),
                                                session_id,
                                                len,
                                            );
                                            tracing::error!(error_msg);
                                            // Similar to the message flow, we always close the
                                            // channel when encountering ser/deser errors.
                                            (State::Closing {
                                                deliveries: Deliveries{outbox, unacked},
                                                reason: error_msg,
                                            }, Conn::Connected { sink, stream })
                                        }
                                    }
                                },
                                Some(Err(err)) => {
                                        tracing::error!(
                                            "session {}.{}: failed to receiving ack: {}",
                                            link.dest(),
                                            session_id,
                                            err
                                        );
                                        // Reconnect and wish the error will go away.
                                        (State::Running(Deliveries { outbox, unacked }), Conn::reconnect_with_default())
                                }
                                None => {
                                    // None means connection is closed. Reconnect.
                                    (State::Running(Deliveries { outbox, unacked }), Conn::reconnect_with_default())
                                }
                            }
                        },
                        // It does matter whether `fn send_message` is cancel safe or not. Since
                        // `fn send_message` does not remove the message from outbox, when it is
                        // canceled, the message will not be dropped. In the worst case, the same
                        // message would get sent multiple times. But that is okay. The seq order is
                        // still preserved.
                        send_result = outbox.send_message(&mut sink), if !outbox.is_empty() => {
                            match send_result {
                                Ok(()) => {
                                    let message = outbox.pop_front().expect("outbox should not be empty");
                                    unacked.push_back(message);
                                    (State::Running(Deliveries { outbox, unacked }), Conn::Connected { sink, stream })
                                }
                                Err(err) => {
                                    tracing::info!(
                                        "session {}.{}: outbox send error: {}; message size: {}",
                                        link.dest(),
                                        session_id,
                                        err,
                                        outbox.front_size().expect("outbox should not be empty"),
                                    );
                                    (State::Running(Deliveries { outbox, unacked }), Conn::reconnect_with_default())
                                }
                            }
                        }
                        // UnboundedReceiver::recv() is cancel safe.
                        // Only checking mspc channel when outbox is empty. In this way, we prioritize
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
                                            (running, Conn::Connected { sink, stream })
                                        }
                                        Err(err) => {
                                            let error_msg = format!(
                                                "session {}.{}: failed to push message to outbox: {}",
                                                link.dest(),
                                                session_id,
                                                err
                                            );
                                            tracing::error!(error_msg);
                                            (State::Closing {
                                                deliveries: Deliveries {outbox, unacked},
                                                reason: error_msg,
                                            }, Conn::Connected { sink, stream })
                                        }
                                    }
                                }
                                None => (State::Closing {
                                    deliveries: Deliveries{outbox, unacked},
                                    reason: "NetTx is dropped".to_string(),
                                }, Conn::Connected { sink, stream }),
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
                            "session {}.{}: failed to deliver message within timeout",
                            link.dest(),
                            session_id,
                        );
                        tracing::error!(error_msg);
                        (
                            State::Closing {
                                deliveries: Deliveries { outbox, unacked },
                                reason: error_msg,
                            },
                            Conn::reconnect_with_default(),
                        )
                    } else if unacked.is_expired() {
                        let error_msg = format!(
                            "session {}.{}: failed to receive ack within timeout {} secs; link is currently broken",
                            link.dest(),
                            session_id,
                            config::global::get(config::MESSAGE_DELIVERY_TIMEOUT).as_secs(),
                        );
                        tracing::error!(error_msg);
                        (
                            State::Closing {
                                deliveries: Deliveries { outbox, unacked },
                                reason: error_msg,
                            },
                            Conn::reconnect_with_default(),
                        )
                    } else {
                        match link.connect().await {
                            Ok(stream) => {
                                let framed = Framed::new(stream, build_codec());
                                let (mut sink, mut stream) = futures::StreamExt::split(framed);

                                metrics::CHANNEL_CONNECTIONS.add(
                                    1,
                                    hyperactor_telemetry::kv_pairs!(
                                        "transport" => link.dest().transport().to_string(),
                                        "reason" => "link connected",
                                    ),
                                );

                                // Need to resend unacked after reconnecting.
                                let largest_acked = unacked.largest_acked;
                                outbox.requeue_unacked(unacked);

                                let handshake_result = do_handshake::<_, M>(
                                    session_id,
                                    &mut rx_session_id,
                                    &mut sink,
                                    &mut stream,
                                )
                                .await;

                                match handshake_result {
                                    Ok(_) => {
                                        (
                                            State::Running(Deliveries {
                                                outbox,
                                                // unacked messages are put back to outbox. So they are not
                                                // considered as "sent yet unacked" message anymore. But
                                                // we still want to keep `largest_acked` to known Rx's watermark.
                                                unacked: Unacked::new(largest_acked, &log_id),
                                            }),
                                            {
                                                backoff.reset();
                                                Conn::Connected { sink, stream }
                                            },
                                        )
                                    }
                                    Err(err) => match err {
                                        HandshakeError::SendFailed(msg) => {
                                            tracing::debug!(
                                                "session {}.{}: handshake send failed: {}",
                                                link.dest(),
                                                session_id,
                                                msg
                                            );
                                            (
                                                State::Running(Deliveries {
                                                    outbox,
                                                    unacked: Unacked::new(largest_acked, &log_id),
                                                }),
                                                Conn::reconnect(backoff),
                                            )
                                        }
                                        err => {
                                            let error_msg = format!(
                                                "session {}.{}: handshake failed: {}",
                                                link.dest(),
                                                session_id,
                                                err
                                            );
                                            tracing::error!(error_msg);
                                            (
                                                State::Closing {
                                                    deliveries: Deliveries {
                                                        outbox,
                                                        unacked: Unacked::new(
                                                            largest_acked,
                                                            &log_id,
                                                        ),
                                                    },
                                                    reason: error_msg,
                                                },
                                                Conn::reconnect_with_default(),
                                            )
                                        }
                                    },
                                }
                            }
                            Err(err) => {
                                tracing::debug!(
                                    "session {}.{}: failed to connect: {}",
                                    link.dest(),
                                    session_id,
                                    err
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
                    break (State::Closing { deliveries, reason }, stream);
                }
            };

            if !matches!(state, State::Closing { .. }) {
                if let Conn::Disconnected(ref mut backoff) = conn {
                    RealClock.sleep(backoff.next_backoff().unwrap()).await;
                }
            }
        }; // loop

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
                // Return in order from oldest to newest, messages
                // either not acknowledged or not sent.
                unacked
                    .deque
                    .drain(..)
                    .chain(outbox.deque.drain(..))
                    .filter_map(|queued_msg| {
                        bincode::deserialize(&queued_msg.data)
                            .ok()
                            .and_then(|frame| match frame {
                                Frame::Message(_, msg) => Some((queued_msg.return_channel, msg)),
                                _ => None,
                            })
                    })
                    .for_each(|(return_channel, msg)| {
                        let _ = return_channel.send(msg);
                    });
                while let Ok((msg, return_channel, _)) = receiver.try_recv() {
                    let _ = return_channel.send(msg);
                }
            }
            _ => (),
        }

        // Notify senders that this link is no longer usable
        if let Err(err) = notify.send(TxStatus::Closed) {
            tracing::debug!(
                "session {}.{}: tx status update error: {}",
                link.dest(),
                session_id,
                err
            );
        }

        if let Conn::Connected {
            mut sink,
            stream: _,
        } = conn
        {
            if let Err(err) = sink.flush().await {
                tracing::info!(
                    "session {}.{}: flush error: {}",
                    link.dest(),
                    session_id,
                    err
                );
            }
        }

        tracing::debug!("session {}.{}: NetTx::run exits", link.dest(), session_id);
    }
}

#[async_trait]
impl<M: RemoteMessage> Tx<M> for NetTx<M> {
    fn addr(&self) -> ChannelAddr {
        self.dest.clone()
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        &self.status
    }

    fn try_post(&self, message: M, return_channel: oneshot::Sender<M>) -> Result<(), SendError<M>> {
        tracing::trace!(name = "post", "sending message to {}", self.dest);
        self.sender
            .send((message, return_channel, RealClock.now()))
            .map_err(|err| SendError(ChannelError::Closed, err.0.0))
    }
}

fn is_closed_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
    )
}

#[derive(Debug)]
pub struct NetRx<M: RemoteMessage>(mpsc::Receiver<M>, ChannelAddr, ServerHandle);

#[async_trait]
impl<M: RemoteMessage> Rx<M> for NetRx<M> {
    async fn recv(&mut self) -> Result<M, ChannelError> {
        tracing::trace!(name = "recv", "receiving message from {}", self.1);
        self.0.recv().await.ok_or(ChannelError::Closed)
    }

    fn addr(&self) -> ChannelAddr {
        self.1.clone()
    }
}

impl<M: RemoteMessage> Drop for NetRx<M> {
    fn drop(&mut self) {
        self.2
            .stop(&format!("NetRx dropped; channel address: {}", self.1));
    }
}

/// Handle to an underlying server that is actively listening.
#[derive(Debug)]
pub struct ServerHandle {
    /// Join handle of the listening task.
    join_handle: JoinHandle<Result<(), ServerError>>,

    /// A cancellation token used to indicate that a stop has been
    /// initiated.
    cancel_token: CancellationToken,

    /// Address of the channel that was used to create the listener. This can be used to dial the that listener.
    channel_addr: ChannelAddr,
}

impl fmt::Display for ServerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.channel_addr)
    }
}

impl ServerHandle {
    /// Signal the server to stop. This will stop accepting new
    /// incoming connection requests, and drain pending operations
    /// on active connections. After draining is completed, the
    /// connections are closed.
    fn stop(&self, reason: &str) {
        tracing::info!("stopping server: {}; reason: {}", self, reason);
        self.cancel_token.cancel();
    }

    /// Reference to the channel that was used to start the server.
    fn local_channel_addr(&self) -> &ChannelAddr {
        &self.channel_addr
    }
}

impl Future for ServerHandle {
    type Output = <JoinHandle<Result<(), ServerError>> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY: This is safe to do because self is pinned.
        let join_handle_pinned =
            unsafe { self.map_unchecked_mut(|container| &mut container.join_handle) };
        join_handle_pinned.poll(cx)
    }
}

/// Error returned during server operations.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("io: {1}")]
    Io(ChannelAddr, #[source] std::io::Error),
    #[error("listen: {0} {1}")]
    Listen(ChannelAddr, #[source] std::io::Error),
    #[error("resolve: {0} {1}")]
    Resolve(ChannelAddr, #[source] std::io::Error),
    #[error("internal: {0} {1}")]
    Internal(ChannelAddr, #[source] anyhow::Error),
}

/// serve new connections that are accepted from the given listener.
pub async fn serve<M: RemoteMessage, L: Listener + Send + Unpin + 'static>(
    listener: L,
    channel_addr: ChannelAddr,
    is_tls: bool,
) -> Result<(ChannelAddr, NetRx<M>), ServerError>
where
    L::Addr: Sync + Send + fmt::Debug + Into<ChannelAddr>,
    L::Io: Sync + Send + Unpin + fmt::Debug,
{
    metrics::CHANNEL_CONNECTIONS.add(
        1,
        hyperactor_telemetry::kv_pairs!(
            "transport" => channel_addr.transport().to_string(),
            "operation" => "serve"
        ),
    );

    let (tx, rx) = mpsc::channel::<M>(1024);
    let cancel_token = CancellationToken::new();
    let join_handle = tokio::spawn(listen(
        listener,
        channel_addr.clone(),
        tx,
        cancel_token.child_token(),
        is_tls,
    ));
    let server_handle = ServerHandle {
        join_handle,
        cancel_token,
        channel_addr: channel_addr.clone(),
    };

    Ok((
        server_handle.local_channel_addr().clone(),
        NetRx(rx, channel_addr, server_handle),
    ))
}

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("connection to {0} failed: {1}: {2}")]
    Connect(ChannelAddr, std::io::Error, String),
    #[error("unable to resolve address: {0}")]
    Resolve(ChannelAddr),
    #[error("io: {0} {1}")]
    Io(ChannelAddr, std::io::Error),
    #[error("send {0}: serialize: {1}")]
    Serialize(ChannelAddr, bincode::ErrorKind),
}

struct ServerConn<S> {
    sink: SplitSink<Framed<S, LengthDelimitedCodec>, Bytes>,
    stream: SplitStream<Framed<S, LengthDelimitedCodec>>,
    source: ChannelAddr,
    dest: ChannelAddr,
}

impl<S: AsyncRead + AsyncWrite> ServerConn<S> {
    fn new(stream: S, source: ChannelAddr, dest: ChannelAddr) -> Self {
        let framed = Framed::new(stream, build_codec());
        let (sink, stream) = futures::StreamExt::split(framed);
        Self {
            sink,
            stream,
            source,
            dest,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Send + 'static + Unpin> ServerConn<S> {
    async fn handshake<M: RemoteMessage>(&mut self) -> Result<u64, anyhow::Error> {
        let Frame::Init(session_id) = Frame::<M>::next(&mut self.stream).await? else {
            anyhow::bail!("unexpected initial frame from {}", self.source);
        };
        Ok(session_id)
    }

    /// Handles a server side stream created during the `listen` loop.
    async fn process<M: RemoteMessage>(
        &mut self,
        session_id: u64,
        tx: mpsc::Sender<M>,
        cancel_token: CancellationToken,
        mut next: Next,
    ) -> (Next, Result<(), anyhow::Error>) {
        use anyhow::Context;
        let mut last_ack_time = RealClock.now();

        let ack_time_interval = config::global::get(config::MESSAGE_ACK_TIME_INTERVAL);
        let ack_msg_interval = config::global::get(config::MESSAGE_ACK_EVERY_N_MESSAGES);

        let (mut final_next, final_result) = loop {
            tokio::select! {
                // tokio_stream::StreamExt::next is cancel safe.
                rcv_result = tokio_stream::StreamExt::next(&mut self.stream) => {
                    match rcv_result {
                        Some(Ok(data)) => {
                            match bincode::deserialize(&data) {
                                Ok(Frame::Init(_)) => {
                                    break (next, Err(anyhow::anyhow!("unexpected init frame from {}", self.source)))
                                },
                                // Ignore retransmits.
                                Ok(Frame::Message(seq, _)) if seq < next.seq => (),
                                // The following segment ensures exactly-once semantics.
                                // That means No out-of-order delivery and no duplicate delivery.
                                Ok(Frame::Message(seq, message)) => {
                                    // received seq should be equal to next seq. Else error out!
                                    if seq > next.seq {
                                        tracing::error!("out-of-sequence message from {}", self.source);
                                        let next_seq = next.seq;
                                        break (next, Err(anyhow::anyhow!("out-of-sequence message from {}, expected seq {}, got {}", self.source, next_seq, seq)))
                                    }
                                    match tx.send(message).await {
                                        Ok(()) => {
                                            // In channel's contract, "delivered" means the message
                                            // is sent to the NetRx object. Therefore, we could bump
                                            // `next_seq` as far as the message is put on the mspc
                                            // channel.
                                            //
                                            // Note that when/how the messages in NetRx are processed
                                            // is not covered by channel's contract. For example,
                                            // the message might never be taken out of netRx, but
                                            // channel still considers those messages delivered.
                                            next.seq = seq+1;
                                        }
                                        Err(err) => {
                                            break (next, Err::<(), anyhow::Error>(err.into()).context(format!("error relaying message to mspc channel for {:?}", self.source)))
                                        }
                                    }
                                },
                                Err(err) => break (
                                    next,
                                    Err::<(), anyhow::Error>(err.into()).context(
                                        format!(
                                            "error deserializing into Frame with M = {} for data from {:?}",
                                            type_name::<M>(),
                                            self.source,
                                        )
                                    )
                                ),
                            }
                        }
                        Some(Err(err)) => {
                            break (next, Err::<(), anyhow::Error>(err.into()).context(format!("error receiving peer message from {:?}", self.source)))
                        }

                        None => break (next, Ok(()))
                    }
                }
                // It does matter whether send_ack is cancel safe. If it is not,
                // the same seq might get acked multiple times. But that is okay.
                ack_result = Self::send_ack(&mut self.sink, next.seq), if next.ack + ack_msg_interval <= next.seq ||
                    (next.ack < next.seq && last_ack_time.elapsed() > ack_time_interval) => {
                    match ack_result {
                        Ok(()) => {
                            last_ack_time = RealClock.now();
                            next.ack = next.seq;
                        }
                        Err(err) => {
                            break (next, Err::<(), anyhow::Error>(err.into()).context(format!("error acking peer message from {:?}", self.source)))
                        }
                    }
                }
                // Have a tick to abort select! call to make sure the ack for the last message can get the chance
                // to be sent as a result of time interval being reached.
                _ = RealClock.sleep_until(last_ack_time + ack_time_interval), if next.ack < next.seq => {},
                _ = cancel_token.cancelled() => break (next, Ok(()))
            }
        };
        // best effort: "flush" any remaining ack before closing this session
        if final_next.ack < final_next.seq {
            match Self::send_ack(&mut self.sink, final_next.seq).await {
                Ok(()) => {
                    final_next.ack = final_next.seq;
                }
                Err(e) => {
                    tracing::warn!(
                        "session {}:{} failed to flush acks for connection from \
                        {} due to error : {:?}. Normally, this is okay because \
                        Tx will reconnect, and acks will be resent in the next \
                        connection. However, if either Tx or Rx is dropped, the \
                        reconnection will not happen, and subsequently the \
                        pending ack will never be sent out.",
                        self.dest,
                        session_id,
                        self.source,
                        e
                    );
                }
            }
        }
        (final_next, final_result)
    }

    async fn send_ack(
        sink: &mut SplitSink<Framed<S, LengthDelimitedCodec>, bytes::Bytes>,
        next_seq: u64,
    ) -> Result<(), std::io::Error> {
        let serialized = serialize_ack(next_seq - 1);
        futures::SinkExt::send(sink, serialized).await
    }

    pub async fn send_handshake_response(&mut self, rx_session_id: u64) -> anyhow::Result<()> {
        let serialized = bincode::serialize(&rx_session_id)?;
        futures::SinkExt::send(&mut self.sink, serialized.into())
            .await
            .map_err(anyhow::Error::from)
    }
}

/// An MVar is a primitive that combines synchronization and the exchange
/// of a value. Its semantics are analogous to a synchronous channel of
/// size 1: if the MVar is full, then `put` blocks until it is emptied;
/// if the MVar is empty, then `take` blocks until it is filled.
///
/// MVars, first introduced in "[Concurrent Haskell](https://www.microsoft.com/en-us/research/wp-content/uploads/1996/01/concurrent-haskell.pdf)"
/// are surprisingly versatile in use. They can be used as:
/// - a communication channel (with `put` and `take` corresponding to `send` and `recv`);
/// - a semaphore (with `put` and `take` corresponding to `signal` and `wait`);
/// - a mutex (with `put` and `take` corresponding to `lock` and `unlock`);
#[derive(Clone, Debug)]
struct MVar<T> {
    seq: watch::Sender<usize>,
    value: Arc<Mutex<Option<T>>>,
}

impl<T> MVar<T> {
    fn new(init: Option<T>) -> Self {
        let (seq, _) = watch::channel(0);
        Self {
            seq,
            value: Arc::new(Mutex::new(init)),
        }
    }

    fn full(value: T) -> Self {
        Self::new(Some(value))
    }

    fn empty() -> Self {
        Self::new(None)
    }

    async fn waitseq(&self, seq: usize) -> (MutexGuard<'_, Option<T>>, usize) {
        let mut sub = self.seq.subscribe();
        while *sub.borrow_and_update() < seq {
            sub.changed().await.unwrap();
        }
        let locked = self.value.lock().await;
        let seq = *sub.borrow_and_update();
        (locked, seq + 1)
    }

    fn notify(&self, seq: usize) {
        self.seq.send_replace(seq);
    }

    async fn take(&self) -> T {
        let mut seq = 0;
        loop {
            let mut value;
            (value, seq) = self.waitseq(seq).await;

            if let Some(current_value) = take(&mut *value) {
                self.notify(seq);
                break current_value;
            }
            drop(value);
        }
    }

    async fn put(&self, new_value: T) {
        let mut seq = 0;
        loop {
            let mut value;
            (value, seq) = self.waitseq(seq).await;

            if value.is_none() {
                *value = Some(new_value);
                self.notify(seq);
                break;
            }
            drop(value);
        }
    }
}

/// Used to bookkeep message processing states.
#[derive(Clone)]
struct Next {
    // The last received message's seq number + 1.
    seq: u64,
    // The last acked seq number + 1.
    ack: u64,
}

/// Unique NetRx session identifier for the process. Used during handshake
/// to determine whether or not to allow a reconnection from a NetTx.
static GLOBAL_RX_SESSION_ID: LazyLock<u64> = LazyLock::new(rand::random);

/// Manages persistent sessions, ensuring that only one connection can own
/// a session at a time, and arranging for session handover.
#[derive(Clone)]
struct SessionManager {
    sessions: Arc<DashMap<u64, MVar<Next>>>,
}

impl SessionManager {
    fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }

    async fn serve<S, M>(
        &self,
        mut conn: ServerConn<S>,
        tx: mpsc::Sender<M>,
        cancel_token: CancellationToken,
    ) -> Result<(), anyhow::Error>
    where
        S: AsyncRead + AsyncWrite + Send + 'static + Unpin,
        M: RemoteMessage,
    {
        let tx_session_id = conn.handshake::<M>().await?;
        conn.send_handshake_response(*GLOBAL_RX_SESSION_ID).await?;

        let session_var = match self.sessions.entry(tx_session_id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                // We haven't seen this session before. We begin with seq=0 and ack=0.
                let var = MVar::full(Next { seq: 0, ack: 0 });
                entry.insert(var.clone());
                var
            }
        };

        let next = session_var.take().await;
        let (next, res) = conn.process(tx_session_id, tx, cancel_token, next).await;
        session_var.put(next).await;

        if let Err(ref err) = res {
            tracing::info!("process encountered an error: {:#}", err);
        }

        res
    }
}

/// Main listen loop that actually runs the server. The loop will exit when `parent_cancel_token` is
/// canceled.
async fn listen<M: RemoteMessage, L: Listener>(
    mut listener: L,
    listener_channel_addr: ChannelAddr,
    tx: mpsc::Sender<M>,
    parent_cancel_token: CancellationToken,
    is_tls: bool,
) -> Result<(), ServerError>
where
    L::Addr: Send + Sync + fmt::Debug + 'static + Into<ChannelAddr>,
    L::Io: Send + Unpin + fmt::Debug + 'static,
{
    let mut connections: JoinSet<Result<(), anyhow::Error>> = JoinSet::new();

    // Cancellation token used to cancel our children only.
    let child_cancel_token = CancellationToken::new();

    let manager = SessionManager::new();
    let result: Result<(), ServerError> = loop {
        let _ = tracing::info_span!("channel_listen_accept_loop");
        tokio::select! {
            result = listener.accept() => {
                tracing::debug!("listener accepted a new connection to {}", listener_channel_addr);
                match result {
                    Ok((stream, addr)) => {
                        metrics::CHANNEL_CONNECTIONS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => listener_channel_addr.transport().to_string(),
                                "operation" => "accept"
                            ),
                        );

                        let tx = tx.clone();
                        let child_cancel_token = child_cancel_token.child_token();
                        let source : ChannelAddr = addr.into();
                        let dest  = listener_channel_addr.clone();
                        let manager = manager.clone();
                        connections.spawn(async move {
                            let res = if is_tls {
                                let conn = ServerConn::new(meta::tls_acceptor(true)?
                                    .accept(stream)
                                    .await?, source.clone(), dest.clone());
                                manager.serve(conn, tx, child_cancel_token).await
                            } else {
                                let conn = ServerConn::new(stream, source.clone(), dest.clone());
                                manager.serve(conn, tx, child_cancel_token).await
                            };

                            if let Err(ref err) = res {
                                metrics::CHANNEL_CONNECTION_ERRORS.add(
                                    1,
                                    hyperactor_telemetry::kv_pairs!(
                                        "transport" => dest.transport().to_string(),
                                        "error" => err.to_string(),
                                    ),
                                );

                                // we don't want the health probe TCP connections to be counted as an error.
                                match source {
                                    ChannelAddr::Tcp(source_addr) if source_addr.ip().is_loopback() => {},
                                    _ => {
                                        tracing::info!(
                                            "serve: error processing peer connection {} <- {}: {:?}",
                                            dest, source, err
                                            );
                                    }
                                }
                            }
                            res
                    });
                    }
                    Err(err) => {
                        metrics::CHANNEL_CONNECTION_ERRORS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => listener_channel_addr.transport().to_string(),
                                "operation" => "accept",
                                "error" => err.to_string(),
                            ),
                        );

                        tracing::info!("serve {}: accept error: {}", listener_channel_addr, err)
                    }
                }
            }

            _ = parent_cancel_token.cancelled() => {
                tracing::info!("serve {}: received parent token cancellation", listener_channel_addr);
                break Ok(());
            }

            result = join_nonempty(&mut connections) => {
                if let Err(err) = result {
                    tracing::info!("connection error: {}: {}", listener_channel_addr, err);
                }
            }
        }
    };

    child_cancel_token.cancel();
    while connections.join_next().await.is_some() {}

    result
}

async fn join_nonempty<T: 'static>(set: &mut JoinSet<T>) -> Result<T, JoinError> {
    match set.join_next().await {
        None => std::future::pending::<Result<T, JoinError>>().await,
        Some(result) => result,
    }
}

/// Tells whether the address is a 'net' address. These currently have different semantics
/// from local transports.
pub fn is_net_addr(addr: &ChannelAddr) -> bool {
    [ChannelTransport::Tcp, ChannelTransport::Unix].contains(&addr.transport())
}

pub(crate) mod unix {

    use core::str;
    use std::os::unix::net::SocketAddr as StdSocketAddr;
    use std::os::unix::net::UnixDatagram as StdUnixDatagram;
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::os::unix::net::UnixStream as StdUnixStream;

    use rand::Rng;
    use rand::distributions::Alphanumeric;
    use tokio::net::UnixListener;
    use tokio::net::UnixStream;

    use super::*;
    use crate::RemoteMessage;

    #[derive(Debug)]
    pub(crate) struct UnixLink(SocketAddr);

    #[async_trait]
    impl Link for UnixLink {
        type Stream = UnixStream;

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::Unix(self.0.clone())
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            match &self.0 {
                SocketAddr::Bound(sock_addr) => {
                    let std_stream: StdUnixStream = StdUnixStream::connect_addr(sock_addr)
                        .map_err(|err| {
                            ClientError::Connect(
                                self.dest(),
                                err,
                                "cannot connect unix socket".to_string(),
                            )
                        })?;
                    std_stream
                        .set_nonblocking(true)
                        .map_err(|err| ClientError::Io(self.dest(), err))?;
                    UnixStream::from_std(std_stream)
                        .map_err(|err| ClientError::Io(self.dest(), err))
                }
                SocketAddr::Unbound => Err(ClientError::Resolve(self.dest())),
            }
        }
    }

    /// Dial the given unix socket.
    pub fn dial<M: RemoteMessage>(addr: SocketAddr) -> NetTx<M> {
        NetTx::new(UnixLink(addr))
    }

    /// Listen and serve connections on this socket address.
    pub async fn serve<M: RemoteMessage>(
        addr: SocketAddr,
    ) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        let caddr = ChannelAddr::Unix(addr.clone());
        let maybe_listener = match &addr {
            SocketAddr::Bound(sock_addr) => StdUnixListener::bind_addr(sock_addr),
            SocketAddr::Unbound => StdUnixDatagram::unbound()
                .and_then(|u| u.local_addr())
                .and_then(|uaddr| StdUnixListener::bind_addr(&uaddr)),
        };
        let std_listener =
            maybe_listener.map_err(|err| ServerError::Listen(ChannelAddr::Unix(addr), err))?;

        std_listener
            .set_nonblocking(true)
            .map_err(|err| ServerError::Listen(caddr.clone(), err))?;
        let local_addr = std_listener
            .local_addr()
            .map_err(|err| ServerError::Resolve(caddr.clone(), err))?;
        let listener: UnixListener = UnixListener::from_std(std_listener)
            .map_err(|err| ServerError::Io(caddr.clone(), err))?;
        super::serve(listener, local_addr.into(), false).await
    }

    /// Wrapper around std-lib's unix::SocketAddr that lets us implement equality functions
    #[derive(Clone, Debug)]
    pub enum SocketAddr {
        Bound(Box<StdSocketAddr>),
        Unbound,
    }

    impl PartialOrd for SocketAddr {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for SocketAddr {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.to_string().cmp(&other.to_string())
        }
    }

    impl<'de> Deserialize<'de> for SocketAddr {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            Self::from_str(&s).map_err(D::Error::custom)
        }
    }

    impl Serialize for SocketAddr {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str(String::from(self).as_str())
        }
    }

    impl From<&SocketAddr> for String {
        fn from(value: &SocketAddr) -> Self {
            match value {
                SocketAddr::Bound(addr) => match addr.as_pathname() {
                    Some(path) => path
                        .to_str()
                        .expect("unable to get str for path")
                        .to_string(),
                    #[cfg(target_os = "linux")]
                    _ => match addr.as_abstract_name() {
                        Some(name) => format!("@{}", String::from_utf8_lossy(name)),
                        _ => String::from("(unnamed)"),
                    },
                    #[cfg(not(target_os = "linux"))]
                    _ => String::from("(unnamed)"),
                },
                SocketAddr::Unbound => String::from("(unbound)"),
            }
        }
    }

    impl FromStr for SocketAddr {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
            match s {
                "" => {
                    // TODO: ensure this socket doesn't already exist. 24 bytes of randomness should be good for now but is not perfect.
                    // We can't use annon sockets because those are not valid across processes that aren't in the same process hierarchy aka forked.
                    let random_string = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(24)
                        .map(char::from)
                        .collect::<String>();
                    SocketAddr::from_abstract_name(&random_string)
                }
                // by convention, named sockets are displayed with an '@' prefix
                name if name.starts_with("@") => {
                    SocketAddr::from_abstract_name(name.strip_prefix("@").unwrap())
                }
                path => SocketAddr::from_pathname(path),
            }
        }
    }

    impl Eq for SocketAddr {}
    impl std::hash::Hash for SocketAddr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            String::from(self).hash(state);
        }
    }
    impl PartialEq for SocketAddr {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Self::Bound(saddr), Self::Bound(oaddr)) => {
                    if saddr.is_unnamed() || oaddr.is_unnamed() {
                        return false;
                    }

                    #[cfg(target_os = "linux")]
                    {
                        saddr.as_pathname() == oaddr.as_pathname()
                            && saddr.as_abstract_name() == oaddr.as_abstract_name()
                    }
                    #[cfg(not(target_os = "linux"))]
                    {
                        // On non-Linux platforms, only compare pathname since no abstract names
                        saddr.as_pathname() == oaddr.as_pathname()
                    }
                }
                (Self::Unbound, _) | (_, Self::Unbound) => false,
            }
        }
    }

    impl fmt::Display for SocketAddr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Bound(addr) => match addr.as_pathname() {
                    Some(path) => {
                        write!(f, "{}", path.to_string_lossy())
                    }
                    #[cfg(target_os = "linux")]
                    _ => match addr.as_abstract_name() {
                        Some(name) => {
                            if name.starts_with(b"@") {
                                return write!(f, "{}", String::from_utf8_lossy(name));
                            }
                            write!(f, "@{}", String::from_utf8_lossy(name))
                        }
                        _ => write!(f, "(unnamed)"),
                    },
                    #[cfg(not(target_os = "linux"))]
                    _ => write!(f, "(unnamed)"),
                },
                Self::Unbound => write!(f, "(unbound)"),
            }
        }
    }

    impl SocketAddr {
        /// Wraps the stdlib socket address for use with this module
        pub fn new(addr: StdSocketAddr) -> Self {
            Self::Bound(Box::new(addr))
        }

        /// Abstract socket names start with a "@" by convention when displayed. If there is an
        /// "@" prefix, it will be stripped from the name before used.
        #[cfg(target_os = "linux")]
        pub fn from_abstract_name(name: &str) -> anyhow::Result<Self> {
            Ok(Self::new(StdSocketAddr::from_abstract_name(
                name.strip_prefix("@").unwrap_or(name),
            )?))
        }

        #[cfg(not(target_os = "linux"))]
        pub fn from_abstract_name(name: &str) -> anyhow::Result<Self> {
            // On non-Linux platforms, convert abstract names to filesystem paths
            let name = name.strip_prefix("@").unwrap_or(name);
            let path = Self::abstract_to_filesystem_path(name);
            Self::from_pathname(&path.to_string_lossy())
        }

        #[cfg(not(target_os = "linux"))]
        fn abstract_to_filesystem_path(abstract_name: &str) -> std::path::PathBuf {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hash;
            use std::hash::Hasher;

            // Generate a stable hash of the abstract name for deterministic paths
            let mut hasher = DefaultHasher::new();
            abstract_name.hash(&mut hasher);
            let hash = hasher.finish();

            // Include process ID to prevent inter-process conflicts
            let process_id = std::process::id();

            // TODO: we just leak these. Should we do something smarter?
            std::path::PathBuf::from(format!("/tmp/hyperactor_{}_{:x}", process_id, hash))
        }

        /// Pathnames may be absolute or relative.
        pub fn from_pathname(name: &str) -> anyhow::Result<Self> {
            Ok(Self::new(StdSocketAddr::from_pathname(name)?))
        }
    }

    impl TryFrom<SocketAddr> for StdSocketAddr {
        type Error = anyhow::Error;

        fn try_from(value: SocketAddr) -> Result<Self, Self::Error> {
            match value {
                SocketAddr::Bound(addr) => Ok(*addr),
                SocketAddr::Unbound => Err(anyhow::anyhow!(
                    "std::os::unix::SocketAddr must be a bound address"
                )),
            }
        }
    }
}

pub(crate) mod tcp {
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    use super::*;
    use crate::RemoteMessage;

    #[derive(Debug)]
    pub(crate) struct TcpLink(SocketAddr);

    #[async_trait]
    impl Link for TcpLink {
        type Stream = TcpStream;

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::Tcp(self.0.clone())
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            let stream = TcpStream::connect(&self.0).await.map_err(|err| {
                ClientError::Connect(self.dest(), err, "cannot connect TCP socket".to_string())
            })?;
            // Always disable Nagle algorithm, so it doesn't hurt the latency of small messages.
            stream.set_nodelay(true).map_err(|err| {
                ClientError::Connect(
                    self.dest(),
                    err,
                    "cannot disables Nagle algorithm".to_string(),
                )
            })?;
            Ok(stream)
        }
    }

    pub fn dial<M: RemoteMessage>(addr: SocketAddr) -> NetTx<M> {
        NetTx::new(TcpLink(addr))
    }

    /// Serve the given address. Supports both v4 and v6 address. If port 0 is provided as
    /// dynamic port will be resolved and is available on the returned ServerHandle.
    pub async fn serve<M: RemoteMessage>(
        addr: SocketAddr,
    ) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|err| ServerError::Listen(ChannelAddr::Tcp(addr), err))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| ServerError::Resolve(ChannelAddr::Tcp(addr), err))?;
        super::serve(listener, ChannelAddr::Tcp(local_addr), false).await
    }
}

// TODO: Try to simplify the TLS creation T208304433
pub(crate) mod meta {
    use std::fs::File;
    use std::io;
    use std::io::BufReader;
    use std::sync::Arc;

    use anyhow::Context;
    use anyhow::Result;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::TlsConnector;
    use tokio_rustls::client::TlsStream;
    use tokio_rustls::rustls::RootCertStore;
    use tokio_rustls::rustls::pki_types::CertificateDer;
    use tokio_rustls::rustls::pki_types::PrivateKeyDer;
    use tokio_rustls::rustls::pki_types::ServerName;

    use super::*;
    use crate::RemoteMessage;

    const THRIFT_TLS_SRV_CA_PATH_ENV: &str = "THRIFT_TLS_SRV_CA_PATH";
    const DEFAULT_SRV_CA_PATH: &str = "/var/facebook/rootcanal/ca.pem";
    const THRIFT_TLS_CL_CERT_PATH_ENV: &str = "THRIFT_TLS_CL_CERT_PATH";
    const THRIFT_TLS_CL_KEY_PATH_ENV: &str = "THRIFT_TLS_CL_KEY_PATH";
    const DEFAULT_SERVER_PEM_PATH: &str = "/var/facebook/x509_identities/server.pem";

    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ChannelError`.
    pub(crate) fn parse(addr_string: &str) -> Result<ChannelAddr, ChannelError> {
        // use right split to allow for ipv6 addresses where ":" is expected.
        let parts = addr_string.rsplit_once(":");
        match parts {
            Some((hostname, port_str)) => {
                let Ok(port) = port_str.parse() else {
                    return Err(ChannelError::InvalidAddress(addr_string.to_string()));
                };
                Ok(ChannelAddr::MetaTls(hostname.to_string(), port))
            }
            _ => Err(ChannelError::InvalidAddress(addr_string.to_string())),
        }
    }

    /// Returns the root cert store
    fn root_cert_store() -> Result<RootCertStore> {
        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
        let ca_cert_path =
            std::env::var_os(THRIFT_TLS_SRV_CA_PATH_ENV).unwrap_or(DEFAULT_SRV_CA_PATH.into());
        let ca_certs = rustls_pemfile::certs(&mut BufReader::new(
            File::open(ca_cert_path).context("Failed to open {ca_cert_path:?}")?,
        ))?;
        for cert in ca_certs {
            root_cert_store
                .add(cert.into())
                .context("Failed to add certificate to root store")?;
        }
        Ok(root_cert_store)
    }

    /// Creates a TLS acceptor by looking for necessary certs and keys in a Meta server environment.
    pub(crate) fn tls_acceptor(enforce_client_tls: bool) -> Result<TlsAcceptor> {
        let server_cert_path = DEFAULT_SERVER_PEM_PATH;
        let certs = rustls_pemfile::certs(&mut BufReader::new(
            File::open(server_cert_path).context("failed to open {server_cert_path}")?,
        ))?
        .into_iter()
        .map(CertificateDer::from)
        .collect();
        // certs are good here
        let server_key_path = DEFAULT_SERVER_PEM_PATH;
        let mut key_reader = BufReader::new(
            File::open(server_key_path).context("failed to open {server_key_path}")?,
        );
        let key = loop {
            break match rustls_pemfile::read_one(&mut key_reader)? {
                Some(rustls_pemfile::Item::RSAKey(key)) => key,
                Some(rustls_pemfile::Item::PKCS8Key(key)) => key,
                Some(rustls_pemfile::Item::ECKey(key)) => key,
                Some(_) => continue,
                None => {
                    anyhow::bail!("no key found in {server_key_path}");
                }
            };
        };

        let config = tokio_rustls::rustls::ServerConfig::builder();

        let config = if enforce_client_tls {
            let client_cert_verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder(
                Arc::new(root_cert_store()?),
            )
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build client verifier: {}", e))?;
            config.with_client_cert_verifier(client_cert_verifier)
        } else {
            config.with_no_client_auth()
        }
        .with_single_cert(
            certs,
            PrivateKeyDer::try_from(key)
                .map_err(|_| anyhow::anyhow!("Invalid private key format"))?,
        )?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    fn load_client_pem() -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
        let Some(cert_path) = std::env::var_os(THRIFT_TLS_CL_CERT_PATH_ENV) else {
            return Ok(None);
        };
        let Some(key_path) = std::env::var_os(THRIFT_TLS_CL_KEY_PATH_ENV) else {
            return Ok(None);
        };
        let certs = rustls_pemfile::certs(&mut BufReader::new(
            File::open(cert_path).context("failed to open {cert_path}")?,
        ))?
        .into_iter()
        .map(CertificateDer::from)
        .collect();
        let mut key_reader =
            BufReader::new(File::open(key_path).context("failed to open {key_path}")?);
        let key = loop {
            break match rustls_pemfile::read_one(&mut key_reader)? {
                Some(rustls_pemfile::Item::RSAKey(key)) => key,
                Some(rustls_pemfile::Item::PKCS8Key(key)) => key,
                Some(rustls_pemfile::Item::ECKey(key)) => key,
                Some(_) => continue,
                None => return Ok(None),
            };
        };
        // Certs are verified to be good here.
        Ok(Some((
            certs,
            PrivateKeyDer::try_from(key)
                .map_err(|_| anyhow::anyhow!("Invalid private key format"))?,
        )))
    }

    /// Creates a TLS connector by looking for necessary certs and keys in a Meta server environment.
    fn tls_connector() -> Result<TlsConnector> {
        // TODO (T208180540): try to simplify the logic here.
        let config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(root_cert_store()?));
        let result = load_client_pem()?;
        let config = if let Some((certs, key)) = result {
            config
                .with_client_auth_cert(certs, key)
                .context("Failed to load client certs")?
        } else {
            config.with_no_client_auth()
        };
        Ok(TlsConnector::from(Arc::new(config)))
    }

    fn tls_connector_config(peer_host_name: &str) -> Result<(TlsConnector, ServerName<'static>)> {
        let connector = tls_connector()?;
        let server_name = ServerName::try_from(peer_host_name.to_string())?;
        Ok((connector, server_name))
    }

    #[derive(Debug)]
    pub(crate) struct MetaLink {
        hostname: Hostname,
        port: Port,
    }

    #[async_trait]
    impl Link for MetaLink {
        type Stream = TlsStream<TcpStream>;

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::MetaTls(self.hostname.clone(), self.port)
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            let mut addrs = (self.hostname.as_ref(), self.port)
                .to_socket_addrs()
                .map_err(|_| ClientError::Resolve(self.dest()))?;
            let addr = addrs.next().ok_or(ClientError::Resolve(self.dest()))?;
            let stream = TcpStream::connect(&addr).await.map_err(|err| {
                ClientError::Connect(self.dest(), err, format!("cannot connect to {}", addr))
            })?;
            // Always disable Nagle algorithm, so it doesn't hurt the latency of small messages.
            stream.set_nodelay(true).map_err(|err| {
                ClientError::Connect(
                    self.dest(),
                    err,
                    "cannot disables Nagle algorithm".to_string(),
                )
            })?;
            let (connector, domain_name) = tls_connector_config(&self.hostname).map_err(|err| {
                ClientError::Connect(
                    self.dest(),
                    io::Error::other(err.to_string()),
                    format!("cannot config tls connector for addr {}", addr),
                )
            })?;
            connector
                .connect(domain_name.clone(), stream)
                .await
                .map_err(|err| {
                    ClientError::Connect(
                        self.dest(),
                        err,
                        format!("cannot establish TLS connection to {:?}", domain_name),
                    )
                })
        }
    }

    pub fn dial<M: RemoteMessage>(hostname: Hostname, port: Port) -> NetTx<M> {
        NetTx::new(MetaLink { hostname, port })
    }

    /// Serve the given address with hostname and port. If port 0 is provided,
    /// dynamic port will be resolved and is available on the returned ServerHandle.
    pub async fn serve<M: RemoteMessage>(
        hostname: Hostname,
        port: Port,
    ) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        let mut addrs = (hostname.as_ref(), port).to_socket_addrs().map_err(|err| {
            ServerError::Resolve(ChannelAddr::MetaTls(hostname.clone(), port), err)
        })?;
        let addr = addrs.next().ok_or(ServerError::Resolve(
            ChannelAddr::MetaTls(hostname.clone(), port),
            io::Error::other("no available socket addr"),
        ))?;
        let channel_addr = ChannelAddr::MetaTls(hostname.clone(), port);
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| ServerError::Listen(channel_addr.clone(), err))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| ServerError::Resolve(channel_addr, err))?;
        super::serve(
            listener,
            ChannelAddr::MetaTls(hostname, local_addr.port()),
            true,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;
    use std::sync::RwLock;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    #[cfg(target_os = "linux")] // uses abstract names
    use std::time::UNIX_EPOCH;

    #[cfg(target_os = "linux")] // uses abstract names
    use anyhow::Result;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::distributions::Alphanumeric;
    use timed_test::async_timed_test;
    use tokio::io::DuplexStream;

    use super::*;

    fn unused_return_channel<M>() -> oneshot::Sender<M> {
        oneshot::channel().0
    }

    #[cfg(target_os = "linux")] // uses abstract names
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_unix_basic() -> Result<()> {
        let timestamp = RealClock
            .system_time_now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_address = format!("test_unix_basic_{}", timestamp);

        let (addr, mut rx) =
            net::unix::serve::<u64>(unix::SocketAddr::from_abstract_name(&unique_address)?)
                .await
                .unwrap();

        // It is important to keep Tx alive until all expected messages are
        // received. Otherwise, the channel would be closed when Tx is dropped.
        // Although the messages are sent to the server's buffer before the
        // channel was closed, NetRx could still error out before taking them
        // out of the buffer because NetRx could not ack through the closed
        // channel.
        {
            let tx = crate::channel::dial::<u64>(addr.clone()).unwrap();
            tx.try_post(123, unused_return_channel()).unwrap();
            assert_eq!(rx.recv().await.unwrap(), 123);
        }

        {
            let tx = dial::<u64>(addr).unwrap();
            tx.try_post(321, unused_return_channel()).unwrap();
            tx.try_post(111, unused_return_channel()).unwrap();
            tx.try_post(444, unused_return_channel()).unwrap();

            assert_eq!(rx.recv().await.unwrap(), 321);
            assert_eq!(rx.recv().await.unwrap(), 111);
            assert_eq!(rx.recv().await.unwrap(), 444);
        }

        Ok(())
    }

    #[cfg(target_os = "linux")] // uses abstract names
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_unix_basic_client_before_server() -> Result<()> {
        // We run this test on Unix because we can pick our own port names more easily.
        let timestamp = RealClock
            .system_time_now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let socket_addr =
            unix::SocketAddr::from_abstract_name(&format!("test_unix_basic_{}", timestamp))
                .unwrap();

        // Dial the channel before we actually serve it.
        let addr = ChannelAddr::Unix(socket_addr.clone());
        let tx = crate::channel::dial::<u64>(addr.clone()).unwrap();
        tx.try_post(123, unused_return_channel()).unwrap();

        let (_, mut rx) = net::unix::serve::<u64>(socket_addr).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), 123);

        tx.try_post(321, unused_return_channel()).unwrap();
        tx.try_post(111, unused_return_channel()).unwrap();
        tx.try_post(444, unused_return_channel()).unwrap();

        assert_eq!(rx.recv().await.unwrap(), 321);
        assert_eq!(rx.recv().await.unwrap(), 111);
        assert_eq!(rx.recv().await.unwrap(), 444);

        Ok(())
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_tcp_basic() {
        let (addr, mut rx) = tcp::serve::<u64>("[::1]:0".parse().unwrap()).await.unwrap();
        {
            let tx = dial::<u64>(addr.clone()).unwrap();
            tx.try_post(123, unused_return_channel()).unwrap();
            assert_eq!(rx.recv().await.unwrap(), 123);
        }

        {
            let tx = dial::<u64>(addr).unwrap();
            tx.try_post(321, unused_return_channel()).unwrap();
            tx.try_post(111, unused_return_channel()).unwrap();
            tx.try_post(444, unused_return_channel()).unwrap();

            assert_eq!(rx.recv().await.unwrap(), 321);
            assert_eq!(rx.recv().await.unwrap(), 111);
            assert_eq!(rx.recv().await.unwrap(), 444);
        }
    }

    // The message size is limited by CODEC_MAX_FRAME_LENGTH due to LengthDelimitedCodec
    // is used.
    #[async_timed_test(timeout_secs = 5)]
    async fn test_tcp_message_size() {
        let default_size_in_bytes = 100 * 1024 * 1024;
        // Use temporary config for this test
        let config = config::global::lock();
        let _guard1 = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(1));
        let _guard2 = config.override_key(config::CODEC_MAX_FRAME_LENGTH, default_size_in_bytes);

        let (addr, mut rx) = tcp::serve::<String>("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let tx = dial::<String>(addr.clone()).unwrap();
        // Default size is okay
        {
            // Leave some headroom because Tx will wrap the payload in Frame::Message.
            let message = "a".repeat(default_size_in_bytes - 1024);
            tx.try_post(message.clone(), unused_return_channel())
                .unwrap();
            assert_eq!(rx.recv().await.unwrap(), message);
        }
        // Bigger than the default size will fail.
        {
            let (return_channel, return_receiver) = oneshot::channel();
            let message = "a".repeat(default_size_in_bytes + 1024);
            tx.try_post(message.clone(), return_channel).unwrap();
            let returned = return_receiver.await.unwrap();
            assert_eq!(message, returned);
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_ack_flush() {
        let config = config::global::lock();
        // Set a large value to effectively prevent acks from being sent except
        // during shutdown flush.
        let _guard_message_ack =
            config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 100000000);
        let _guard_delivery_timeout =
            config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(5));

        let (addr, mut net_rx) = tcp::serve::<u64>("[::1]:0".parse().unwrap()).await.unwrap();
        let net_tx = dial::<u64>(addr.clone()).unwrap();
        let (tx, rx) = oneshot::channel();
        net_tx.try_post(1, tx).unwrap();
        assert_eq!(net_rx.recv().await.unwrap(), 1);
        drop(net_rx);
        // Using `is_err` to confirm the message is delivered/acked is confusing,
        // but is correct. See how send is implemented: https://fburl.com/code/ywt8lip2
        assert!(rx.await.is_err());
    }

    #[ignore = "fix problem in Sandcastle T208303369"]
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_meta_tls_basic() {
        let addr = ChannelAddr::any(ChannelTransport::MetaTls);
        let (hostname, port) = match addr {
            ChannelAddr::MetaTls(hostname, port) => (hostname, port),
            _ => ("".to_string(), 0),
        };
        let (local_addr, mut rx) = net::meta::serve::<u64>(hostname, port).await.unwrap();
        {
            let tx = dial::<u64>(local_addr.clone()).unwrap();
            tx.try_post(123, unused_return_channel()).unwrap();
        }
        assert_eq!(rx.recv().await.unwrap(), 123);

        {
            let tx = dial::<u64>(local_addr).unwrap();
            tx.try_post(321, unused_return_channel()).unwrap();
            tx.try_post(111, unused_return_channel()).unwrap();
            tx.try_post(444, unused_return_channel()).unwrap();
            assert_eq!(rx.recv().await.unwrap(), 321);
            assert_eq!(rx.recv().await.unwrap(), 111);
            assert_eq!(rx.recv().await.unwrap(), 444);
        }
    }

    #[tokio::test]
    async fn test_mvar() {
        let mv0 = MVar::full(0);
        let mv1 = MVar::empty();

        assert_eq!(mv0.take().await, 0);

        tokio::spawn({
            let mv0 = mv0.clone();
            let mv1 = mv1.clone();
            async move { mv1.put(mv0.take().await).await }
        });

        mv0.put(1).await;
        assert_eq!(mv1.take().await, 1);
    }

    #[derive(Clone, Debug, Default)]
    struct NetworkFlakiness {
        // A tuple of:
        //   1. the probability of a network failure when sending a message.
        //   2. the max number of disconnections allowed.
        //   3. the minimum duration between disconnections.
        //
        //   2 and 3 are useful to prevent frequent disconnections leading to
        //   unacked messages being sent repeatedly.
        disconnect_params: Option<(f64, u64, Duration)>,
        // The max possible latency when sending a message. The actual latency
        // is randomly generated between 0 and max_latency.
        latency_range: Option<(Duration, Duration)>,
    }

    impl NetworkFlakiness {
        // Calculate whether to disconnect
        async fn should_disconnect(
            &self,
            rng: &mut impl rand::Rng,
            disconnected_count: u64,
            prev_diconnected_at: &RwLock<Instant>,
        ) -> bool {
            let Some((prob, max_disconnects, duration)) = &self.disconnect_params else {
                return false;
            };

            let disconnected_at = prev_diconnected_at.read().unwrap();
            if disconnected_at.elapsed() > *duration && disconnected_count < *max_disconnects {
                rng.gen_bool(*prob)
            } else {
                false
            }
        }
    }

    #[derive(Debug)]
    struct MockLink<M> {
        buffer_size: usize,
        receiver_storage: Arc<MVar<DuplexStream>>,
        // If true, `connect()` on this link will always return an error.
        fail_connects: Arc<AtomicBool>,
        // Used to break the existing connection, if there is one. It still
        // allows reconnect.
        disconnect_signal: watch::Sender<()>,
        network_flakiness: NetworkFlakiness,
        disconnected_count: Arc<AtomicU64>,
        prev_diconnected_at: Arc<RwLock<Instant>>,
        // If set, print logs every `debug_log_sampling_rate` messages. This
        // is normally set only when debugging a test failure.
        debug_log_sampling_rate: Option<u64>,
        _message_type: PhantomData<M>,
    }

    impl<M: RemoteMessage> MockLink<M> {
        fn new() -> Self {
            let (sender, _) = watch::channel(());
            Self {
                buffer_size: 64,
                receiver_storage: Arc::new(MVar::empty()),
                fail_connects: Arc::new(AtomicBool::new(false)),
                disconnect_signal: sender,
                network_flakiness: NetworkFlakiness::default(),
                disconnected_count: Arc::new(AtomicU64::new(0)),
                prev_diconnected_at: Arc::new(RwLock::new(RealClock.now())),
                debug_log_sampling_rate: None,
                _message_type: PhantomData,
            }
        }

        // If `fail_connects` is true, `connect()` on this link will
        // always return an error.
        fn fail_connects() -> Self {
            Self {
                fail_connects: Arc::new(AtomicBool::new(true)),
                ..Self::new()
            }
        }

        fn with_network_flakiness(network_flakiness: NetworkFlakiness) -> Self {
            if let Some((min, max)) = network_flakiness.latency_range {
                assert!(min < max);
            }

            Self {
                network_flakiness,
                ..Self::new()
            }
        }

        fn receiver_storage(&self) -> Arc<MVar<DuplexStream>> {
            self.receiver_storage.clone()
        }

        fn source(&self) -> ChannelAddr {
            // Use a dummy address as a placeholder.
            ChannelAddr::Local(u64::MAX)
        }

        fn disconnected_count(&self) -> Arc<AtomicU64> {
            self.disconnected_count.clone()
        }

        fn disconnect_signal(&self) -> &watch::Sender<()> {
            &self.disconnect_signal
        }

        fn fail_connects_switch(&self) -> Arc<AtomicBool> {
            self.fail_connects.clone()
        }

        fn set_buffer_size(&mut self, size: usize) {
            self.buffer_size = size;
        }

        fn set_sampling_rate(&mut self, sampling_rate: u64) {
            self.debug_log_sampling_rate = Some(sampling_rate);
        }
    }

    #[async_trait]
    impl<M: RemoteMessage> Link for MockLink<M> {
        type Stream = DuplexStream;

        fn dest(&self) -> ChannelAddr {
            // Use a dummy address as a placeholder.
            ChannelAddr::Local(u64::MAX)
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            tracing::debug!("MockLink starts to connect.");
            if self.fail_connects.load(Ordering::Acquire) {
                return Err(ClientError::Connect(
                    self.dest(),
                    std::io::Error::other("intentional error"),
                    "expected failure injected by the mock".to_string(),
                ));
            }

            // Add relays between server and client streams. The relays provides
            // the place to inject network flakiness. The message flow looks like:
            //
            // server <-> server relay <-> injection logic <-> client relay <-> client
            async fn relay_message<M: RemoteMessage>(
                mut disconnect_signal: watch::Receiver<()>,
                network_flakiness: NetworkFlakiness,
                disconnected_count: Arc<AtomicU64>,
                prev_diconnected_at: Arc<RwLock<Instant>>,
                mut this_half_stream: SplitStream<Framed<DuplexStream, LengthDelimitedCodec>>,
                mut other_half_sink: SplitSink<Framed<DuplexStream, LengthDelimitedCodec>, Bytes>,
                // Used by client and server tokio tasks to coordinate stopping together.
                task_coordination_token: CancellationToken,
                debug_log_sampling_rate: Option<u64>,
                // Whether the relayed message is from client to server.
                is_from_client: bool,
            ) {
                // Used to simulate latency. Breifly, messages are buffered in
                // the queue and wait for the expected latency elapse.
                async fn wait_for_latency_elapse(
                    queue: &VecDeque<(BytesMut, Instant)>,
                    network_flakiness: &NetworkFlakiness,
                    rng: &mut impl rand::Rng,
                ) {
                    if let Some((min, max)) = network_flakiness.latency_range {
                        let diff = max.abs_diff(min);
                        let factor = rng.gen_range(0.0..=1.0);
                        let latency = min + diff.mul_f64(factor);
                        RealClock
                            .sleep_until(queue.front().unwrap().1 + latency)
                            .await;
                    }
                }

                let mut rng = rand::rngs::SmallRng::from_entropy();
                let mut queue: VecDeque<(BytesMut, Instant)> = VecDeque::new();
                let mut send_count = 0u64;
                loop {
                    tokio::select! {
                        server_result = tokio_stream::StreamExt::next(&mut this_half_stream) => {
                            match server_result {
                                    Some(Ok(data)) => {
                                        queue.push_back((data, RealClock.now()));
                                    },
                                    Some(Err(_)) | None => {
                                        tracing::debug!("The upstream is closed or dropped. MockLink disconnects");
                                        break;
                                    }
                            };
                        }
                        _ = wait_for_latency_elapse(&queue, &network_flakiness, &mut rng), if !queue.is_empty() => {
                            let count = disconnected_count.load(Ordering::Relaxed);
                            if network_flakiness.should_disconnect(&mut rng, count, &prev_diconnected_at).await {
                                tracing::debug!("MockLink disconnects");
                                disconnected_count.fetch_add(1, Ordering::Relaxed);

                                metrics::CHANNEL_RECONNECTIONS.add(
                                    1,
                                    hyperactor_telemetry::kv_pairs!(
                                        "transport" => "mock",
                                        "reason" => "network_flakiness",
                                    ),
                                );

                                let mut w = prev_diconnected_at.write().unwrap();
                                *w = RealClock.now();
                                break;
                            }
                            let data = queue.pop_front().unwrap().0;
                            let is_sampled = debug_log_sampling_rate.is_some_and(|sample_rate| send_count % sample_rate == 1);
                            if is_sampled {
                                if is_from_client {
                                    if let Ok(Frame::Message(_seq, msg)) = bincode::deserialize::<Frame<M>>(&data) {
                                        tracing::debug!("MockLink relays a msg from client. msg: {:?}", msg);
                                    }
                                } else {
                                    let result = deserialize_ack(data.clone());
                                    if let Ok(seq) = result {
                                        tracing::debug!("MockLink relays an ack from server. seq: {:?}", seq);
                                    }
                                }
                            }
                            if other_half_sink.send(data.into()).await.is_err() {
                                break;
                            }
                            send_count += 1;
                        }
                        _ = task_coordination_token.cancelled() => break,
                        disconnect_result = disconnect_signal.changed() => {
                            tracing::debug!("MockLink disconnects per disconnect_signal {:?}", disconnect_result);
                            break;
                        }
                    }
                }
                task_coordination_token.cancel();
            }

            let (server, server_relay) = tokio::io::duplex(self.buffer_size);
            let (client, client_relay) = tokio::io::duplex(self.buffer_size);
            let (server_relay_sink, server_relay_stream) =
                futures::StreamExt::split(Framed::new(server_relay, build_codec()));
            let (client_relay_sink, client_relay_stream) =
                futures::StreamExt::split(Framed::new(client_relay, build_codec()));

            let task_coordination_token = CancellationToken::new();
            let _server_relay_task_handle = tokio::spawn(relay_message::<M>(
                self.disconnect_signal.subscribe(),
                self.network_flakiness.clone(),
                self.disconnected_count.clone(),
                self.prev_diconnected_at.clone(),
                server_relay_stream,
                client_relay_sink,
                task_coordination_token.clone(),
                self.debug_log_sampling_rate.clone(),
                /*is_from_client*/ false,
            ));
            let _client_relay_task_handle = tokio::spawn(relay_message::<M>(
                self.disconnect_signal.subscribe(),
                self.network_flakiness.clone(),
                self.disconnected_count.clone(),
                self.prev_diconnected_at.clone(),
                client_relay_stream,
                server_relay_sink,
                task_coordination_token,
                self.debug_log_sampling_rate.clone(),
                /*is_from_client*/ true,
            ));

            self.receiver_storage.put(server).await;
            Ok(client)
        }
    }

    struct MockLinkListener {
        receiver_storage: Arc<MVar<DuplexStream>>,
        channel_addr: ChannelAddr,
        cached_future: Option<Pin<Box<dyn Future<Output = DuplexStream> + Send>>>,
    }

    impl MockLinkListener {
        fn new(receiver_storage: Arc<MVar<DuplexStream>>, channel_addr: ChannelAddr) -> Self {
            Self {
                receiver_storage,
                channel_addr,
                cached_future: None,
            }
        }
    }

    impl Listener for MockLinkListener {
        type Io = DuplexStream;
        type Addr = ChannelAddr;

        fn poll_accept(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<(Self::Io, Self::Addr)>> {
            if self.cached_future.is_none() {
                let storage = self.receiver_storage.clone();
                let fut = async move { storage.take().await };
                self.cached_future = Some(Box::pin(fut));
            }
            self.cached_future
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(cx)
                .map(|io| {
                    self.cached_future = None;
                    Ok((io, self.channel_addr.clone()))
                })
        }

        fn local_addr(&self) -> std::io::Result<Self::Addr> {
            Ok(self.channel_addr.clone())
        }
    }

    async fn serve<M>(
        manager: &SessionManager,
    ) -> (
        JoinHandle<std::result::Result<(), anyhow::Error>>,
        Framed<DuplexStream, LengthDelimitedCodec>,
        mpsc::Receiver<M>,
        CancellationToken,
    )
    where
        M: RemoteMessage,
    {
        let cancel_token = CancellationToken::new();
        // When testing ServerConn, we do not need a Link object, but only a
        // duplex stream. Therefore, we create them directly so the test will
        // not have dependence on Link.
        let (sender, receiver) = tokio::io::duplex(5000);
        let source = ChannelAddr::Local(u64::MAX);
        let dest = ChannelAddr::Local(u64::MAX);
        let conn = ServerConn::new(receiver, source, dest);
        let manager1 = manager.clone();
        let cancel_token_1 = cancel_token.child_token();
        let (tx, rx) = mpsc::channel(1);
        let join_handle =
            tokio::spawn(async move { manager1.serve(conn, tx, cancel_token_1).await });
        let framed = Framed::new(sender, build_codec());
        (join_handle, framed, rx, cancel_token)
    }

    async fn write_stream<M: RemoteMessage + std::cmp::PartialEq + Clone>(
        framed: &mut Framed<DuplexStream, LengthDelimitedCodec>,
        session_id: u64,
        messages: &[(u64, M)],
        init: bool,
    ) {
        if init {
            framed
                .send(
                    bincode::serialize(&Frame::<u64>::Init(session_id))
                        .unwrap()
                        .into(),
                )
                .await
                .unwrap();

            // Process handshake
            bincode::deserialize::<u64>(
                tokio_stream::StreamExt::next(framed)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
            )
            .unwrap();
        }

        for (seq, message) in messages {
            framed
                .send(
                    bincode::serialize(&Frame::Message(*seq, message.clone()))
                        .unwrap()
                        .into(),
                )
                .await
                .unwrap();
        }
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_persistent_server_session() {
        // Use temporary config for this test
        let config = config::global::lock();
        let _guard = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);
        async fn verify_ack(
            framed: &mut Framed<DuplexStream, LengthDelimitedCodec>,
            expected_last: u64,
        ) {
            let mut last_acked: i128 = -1;
            loop {
                let acked = deserialize_ack(
                    tokio_stream::StreamExt::next(framed)
                        .await
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();

                assert!(
                    acked as i128 > last_acked,
                    "acks should be delivered in ascending order"
                );
                last_acked = acked as i128;
                assert!(acked <= expected_last);
                if acked == expected_last {
                    break;
                }
            }
        }

        let manager = SessionManager::new();
        let session_id = 123;

        {
            let (handle, mut framed, mut rx, _cancel_token) = serve(&manager).await;
            write_stream(
                &mut framed,
                session_id,
                &[(0, 100), (1, 101), (2, 102), (3, 103)],
                /*init*/ true,
            )
            .await;

            assert_eq!(rx.recv().await, Some(100));
            assert_eq!(rx.recv().await, Some(101));
            assert_eq!(rx.recv().await, Some(102));
            // Intentionally skip 103, so we can verify it still can be received
            // after the connection is closed.
            // assert_eq!(rx.recv().await, Some(103));

            // server side might or might not ack seq<3 depending on the order
            // of execution introduced by tokio::select. But it definitely would
            // ack 3.
            verify_ack(&mut framed, 3).await;

            // Drop the sender side and cause the connection to close.
            drop(framed);
            handle.await.unwrap().unwrap();
            // mspc is closed too and there should be no unread message left.
            assert_eq!(rx.recv().await, Some(103));
            assert_eq!(rx.recv().await, None);
        };

        // Now, create a new connection with the same session.
        {
            let (handle, mut framed, mut rx, cancel_token) = serve(&manager).await;
            write_stream(
                &mut framed,
                session_id,
                &[(2, 102), (3, 103), (4, 104), (5, 105)],
                /*init*/ true,
            )
            .await;

            // We don't get another '102' and '103' because they were already
            // delivered in the previous connection.
            assert_eq!(rx.recv().await, Some(104));
            assert_eq!(rx.recv().await, Some(105));

            verify_ack(&mut framed, 5).await;

            // Wait long enough to ensure server processed everything.
            RealClock.sleep(Duration::from_secs(5)).await;

            cancel_token.cancel();
            handle.await.unwrap().unwrap();
            // mspc is closed too and there should be no unread message left.
            assert!(rx.recv().await.is_none());
            // No more acks from server.
            assert!(tokio_stream::StreamExt::next(&mut framed).await.is_none());
        };
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_server_session_handshake_response() {
        let config = config::global::lock();
        let _guard = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);

        let manager = SessionManager::new();
        let (_handle, mut framed, _rx, _cancel_token) = serve::<i32>(&manager).await;

        framed
            .send(bincode::serialize(&Frame::<u64>::Init(123)).unwrap().into())
            .await
            .unwrap();

        let rx_session_id = bincode::deserialize::<u64>(
            tokio_stream::StreamExt::next(&mut framed)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
        )
        .unwrap();

        assert_eq!(rx_session_id, *GLOBAL_RX_SESSION_ID);
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_ack_from_server_session() {
        let config = config::global::lock();
        let _guard = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);
        let manager = SessionManager::new();
        let session_id = 123;

        let (handle, mut framed, mut rx, cancel_token) = serve(&manager).await;
        for i in 0..100 {
            write_stream(
                &mut framed,
                session_id,
                &[(i, 100 + i)],
                /*init*/ i == 0,
            )
            .await;
            assert_eq!(rx.recv().await, Some(100 + i));
            let acked = deserialize_ack(
                tokio_stream::StreamExt::next(&mut framed)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(acked, i);
        }

        // Wait long enough to ensure server processed everything.
        RealClock.sleep(Duration::from_secs(5)).await;

        cancel_token.cancel();
        handle.await.unwrap().unwrap();
        // mspc is closed too and there should be no unread message left.
        assert!(rx.recv().await.is_none());
        // No more acks from server.
        assert!(tokio_stream::StreamExt::next(&mut framed).await.is_none());
    }

    #[tracing_test::traced_test]
    async fn verify_tx_closed(tx_status: &mut watch::Receiver<TxStatus>, expected_log: &str) {
        match RealClock
            .timeout(Duration::from_secs(5), tx_status.changed())
            .await
        {
            Ok(Ok(())) => {
                let current_status = *tx_status.borrow();
                assert_eq!(current_status, TxStatus::Closed);
                logs_assert(|logs| {
                    if logs.iter().any(|log| log.contains(expected_log)) {
                        Ok(())
                    } else {
                        Err("expected log not found".to_string())
                    }
                });
            }
            Ok(Err(_)) => panic!("watch::Receiver::changed() failed because sender is dropped."),
            Err(_) => panic!("timeout before tx_status changed"),
        }
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_tcp_tx_delivery_timeout() {
        // This link always fails to connect.
        let link = MockLink::<u64>::fail_connects();
        let tx = NetTx::<u64>::new(link);
        // Override the default (1m) for the purposes of this test.
        let config = config::global::lock();
        let _guard = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(1));
        let mut tx_receiver = tx.status().clone();
        let (return_channel, _return_receiver) = oneshot::channel();
        tx.try_post(123, return_channel).unwrap();
        verify_tx_closed(&mut tx_receiver, "failed to deliver message within timeout").await;
    }

    async fn take_receiver(
        receiver_storage: &MVar<DuplexStream>,
    ) -> (
        SplitSink<Framed<DuplexStream, LengthDelimitedCodec>, Bytes>,
        SplitStream<Framed<DuplexStream, LengthDelimitedCodec>>,
    ) {
        let client = receiver_storage.take().await;
        let framed = Framed::new(client, build_codec());
        futures::StreamExt::split(framed)
    }

    async fn verify_message<M: RemoteMessage + std::cmp::PartialEq>(
        stream: &mut SplitStream<Framed<DuplexStream, LengthDelimitedCodec>>,
        expect: (u64, M),
        loc: u32,
    ) {
        let expected = Frame::Message(expect.0, expect.1);
        let frame = Frame::<M>::next(stream).await.unwrap();
        assert_eq!(frame, expected, "from ln={loc}");
    }

    static RX_SESSION_ID: u64 = 456;

    async fn verify_stream<M: RemoteMessage + std::cmp::PartialEq + Clone>(
        stream: &mut SplitStream<Framed<DuplexStream, LengthDelimitedCodec>>,
        sink: &mut SplitSink<Framed<DuplexStream, LengthDelimitedCodec>, Bytes>,
        expects: &[(u64, M)],
        expect_session_id: Option<u64>,
        loc: u32,
    ) -> u64 {
        let session_id = {
            let frame = Frame::<M>::next(stream).await.unwrap();
            match frame {
                Frame::Init(session_id) => session_id,
                _ => panic!("the 1st frame is not Init: {:?}. from ln={loc}", frame),
            }
        };

        if let Some(expected_id) = expect_session_id {
            assert_eq!(session_id, expected_id, "from ln={loc}");
        }

        send_handshake_response(sink, RX_SESSION_ID).await;

        for expect in expects {
            verify_message(stream, expect.clone(), loc).await;
        }

        session_id
    }

    async fn send_handshake_response(
        sink: &mut SplitSink<Framed<DuplexStream, LengthDelimitedCodec>, Bytes>,
        handshake_response: u64,
    ) {
        sink.send(bincode::serialize(&handshake_response).unwrap().into())
            .await
            .unwrap();
    }

    async fn net_tx_send(tx: &NetTx<u64>, msgs: &[u64]) {
        for msg in msgs {
            tx.try_post(*msg, unused_return_channel()).unwrap();
        }
    }

    // Happy path: all messages are acked.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ack_in_net_tx_basic() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let tx = NetTx::<u64>::new(link);

        // Send some messages, but not acking any of them.
        net_tx_send(&tx, &[100, 101, 102, 103, 104]).await;
        let session_id = {
            let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
            let id = verify_stream(
                &mut stream,
                &mut sink,
                &[(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)],
                None,
                line!(),
            )
            .await;

            for i in 0..5 {
                sink.send(serialize_ack(i)).await.unwrap();
            }
            // Wait for the acks to be processed by NetTx.
            RealClock.sleep(Duration::from_secs(3)).await;
            // client DuplexStream is dropped here. This breaks the connection.
            id
        };

        // Sent a new message to verify all sent messages will not be resent.
        net_tx_send(&tx, &[105]).await;
        {
            let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
            verify_stream(
                &mut stream,
                &mut sink,
                &[(5, 105)],
                Some(session_id),
                line!(),
            )
            .await;
            // client DuplexStream is dropped here. This breaks the connection.
        };
    }

    // Verify unacked message will be resent after reconnection.
    #[async_timed_test(timeout_secs = 60)]
    async fn test_persistent_net_tx() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();

        let tx = NetTx::<u64>::new(link);
        let mut session_id = None;

        // Send some messages, but not acking any of them.
        net_tx_send(&tx, &[100, 101, 102, 103, 104]).await;

        // How many times to reconnect.
        let n = 10;

        // Reconnect multiple times. The messages should be resent every time
        // because none of them is acked.
        for i in 0..n {
            {
                let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
                let id = verify_stream(
                    &mut stream,
                    &mut sink,
                    &[(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)],
                    session_id,
                    line!(),
                )
                .await;
                if i == 0 {
                    assert!(session_id.is_none());
                    session_id = Some(id);
                }

                // In the last iteration, ack part of the messages. This should
                // prune them from future resent.
                if i == n - 1 {
                    sink.send(serialize_ack(1)).await.unwrap();
                    // Wait for the acks to be processed by NetTx.
                    RealClock.sleep(Duration::from_secs(3)).await;
                }
                // client DuplexStream is dropped here. This breaks the connection.
            };
        }

        // Verify only unacked are resent.
        for _ in 0..n {
            {
                let client = receiver_storage.take().await;
                let framed = Framed::new(client, build_codec());
                let (mut sink, mut stream) = futures::StreamExt::split(framed);
                verify_stream(
                    &mut stream,
                    &mut sink,
                    &[(2, 102), (3, 103), (4, 104)],
                    session_id,
                    line!(),
                )
                .await;
                // client DuplexStream is dropped here. This breaks the connection.
            };
        }

        // Now send more messages.
        net_tx_send(&tx, &[105, 106, 107, 108, 109]).await;
        // Verify the unacked messages from the 1st send will be grouped with
        // the 2nd send.
        for i in 0..n {
            {
                let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
                verify_stream(
                    &mut stream,
                    &mut sink,
                    &[
                        // From the 1st send.
                        (2, 102),
                        (3, 103),
                        (4, 104),
                        // From the 2nd send.
                        (5, 105),
                        (6, 106),
                        (7, 107),
                        (8, 108),
                        (9, 109),
                    ],
                    session_id,
                    line!(),
                )
                .await;

                // In the last iteration, ack part of the messages from the 1st
                // sent.
                if i == n - 1 {
                    // Intentionally ack 1 again to verify it is okay to ack
                    // messages that was already acked.
                    sink.send(serialize_ack(1)).await.unwrap();
                    sink.send(serialize_ack(2)).await.unwrap();
                    sink.send(serialize_ack(3)).await.unwrap();
                    // Wait for the acks to be processed by NetTx.
                    RealClock.sleep(Duration::from_secs(3)).await;
                }
                // client DuplexStream is dropped here. This breaks the connection.
            };
        }

        for i in 0..n {
            {
                let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
                verify_stream(
                    &mut stream,
                    &mut sink,
                    &[
                        // From the 1st send.
                        (4, 104),
                        // From the 2nd send.
                        (5, 105),
                        (6, 106),
                        (7, 107),
                        (8, 108),
                        (9, 109),
                    ],
                    session_id,
                    line!(),
                )
                .await;

                // In the last iteration, ack part of the messages from the 2nd send.
                if i == n - 1 {
                    sink.send(serialize_ack(7)).await.unwrap();
                    // Wait for the acks to be processed by NetTx.
                    RealClock.sleep(Duration::from_secs(3)).await;
                }
                // client DuplexStream is dropped here. This breaks the connection.
            };
        }

        for _ in 0..n {
            {
                let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
                verify_stream(
                    &mut stream,
                    &mut sink,
                    &[
                        // From the 2nd send.
                        (8, 108),
                        (9, 109),
                    ],
                    session_id,
                    line!(),
                )
                .await;
                // client DuplexStream is dropped here. This breaks the connection.
            };
        }
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_handshake_timeout() {
        let link = MockLink::<u64>::new();
        // Override the default (1m) for the purposes of this test.
        let config = config::global::lock();
        let _guard = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(1));
        let mut tx = NetTx::<u64>::new(link);
        net_tx_send(&tx, &[100]).await;
        // Never send the handshake response
        verify_tx_closed(&mut tx.status, "timed out waiting for handshake response").await;
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 60)]
    async fn test_handshake_incorrect_rx_session_id_failure() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let mut tx = NetTx::<u64>::new(link);
        net_tx_send(&tx, &[100]).await;

        {
            let (mut sink, _stream) = take_receiver(&receiver_storage).await;
            send_handshake_response(&mut sink, 1234321).await;
            // Wait for handshake response to be processed by NetTx before dropping sink/stream. Otherwise
            // the channel will be closed and we will get the wrong error.
            RealClock.sleep(tokio::time::Duration::from_secs(3)).await;
        }

        {
            let (mut sink, _stream) = take_receiver(&receiver_storage).await;
            send_handshake_response(&mut sink, 123).await;
            // Ditto
            RealClock.sleep(tokio::time::Duration::from_secs(3)).await;
        }

        verify_tx_closed(
            &mut tx.status,
            format!(
                "mismatched rx session ids: {} (stored) vs. {} (received)",
                1234321, 123
            )
            .as_str(),
        )
        .await;
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 60)]
    async fn test_handshake_deserialization_failure() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let mut tx = NetTx::<u64>::new(link);
        net_tx_send(&tx, &[100]).await;

        let (mut sink, _stream) = take_receiver(&receiver_storage).await;
        sink.send(Bytes::from_static(b"bad")).await.unwrap();

        verify_tx_closed(
            &mut tx.status,
            "handshake failed: io error: unexpected end of file",
        )
        .await;
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 60)]
    async fn test_handshake_channel_closed_failure() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let mut tx = NetTx::<u64>::new(link);
        net_tx_send(&tx, &[100]).await;

        {
            let _ = take_receiver(&receiver_storage).await;
        }

        verify_tx_closed(
            &mut tx.status,
            "handshake failed: channel closed waiting for handshake response",
        )
        .await;
    }

    #[async_timed_test(timeout_secs = 15)]
    async fn test_ack_before_redelivery_in_net_tx() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let net_tx = NetTx::<u64>::new(link);

        // Verify sent-and-ack a message. This is necessary for the test to
        // trigger a connection.
        let (return_channel_tx, return_channel_rx) = oneshot::channel();
        net_tx.try_post(100, return_channel_tx).unwrap();
        let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
        verify_stream(&mut stream, &mut sink, &[(0, 100)], None, line!()).await;
        // ack it
        sink.send(serialize_ack(0)).await.unwrap();
        // confirm Tx received ack
        //
        // Using `is_err` to confirm the message is delivered/acked is confusing,
        // but is correct. See how send is implemented: https://fburl.com/code/ywt8lip2
        assert!(return_channel_rx.await.is_err());

        // Now fake an unknown delivery for Tx:
        // Although Tx did not actually send seq=1, we still ack it from Rx to
        // pretend Tx already sent it, just it did not know it was sent
        // successfully.
        sink.send(serialize_ack(1)).await.unwrap();

        let (return_channel_tx, return_channel_rx) = oneshot::channel();
        net_tx.try_post(101, return_channel_tx).unwrap();
        // Verify the message is sent to Rx.
        verify_message(&mut stream, (1, 101), line!()).await;
        // although we did not ack the message after it is sent, since we already
        // acked it previously, Tx will treat it as acked, and considered the
        // message delivered successfully.
        //
        // Using `is_err` to confirm the message is delivered/acked is confusing,
        // but is correct. See how send is implemented: https://fburl.com/code/ywt8lip2
        assert!(return_channel_rx.await.is_err());
    }

    async fn verify_ack_exceeded_limit(disconnect_before_ack: bool) {
        // Use temporary config for this test
        let config = config::global::lock();
        let _guard = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(2));

        let link: MockLink<u64> = MockLink::<u64>::new();
        let disconnect_signal = link.disconnect_signal().clone();
        let fail_connect_switch = link.fail_connects_switch();
        let receiver_storage = link.receiver_storage();
        let tx = NetTx::<u64>::new(link);
        let mut tx_status = tx.status().clone();
        // send a message
        tx.try_post(100, unused_return_channel()).unwrap();
        let (mut sink, mut stream) = take_receiver(&receiver_storage).await;
        // Confirm message is sent to rx.
        verify_stream(&mut stream, &mut sink, &[(0, 100)], None, line!()).await;
        // ack it
        sink.send(serialize_ack(0)).await.unwrap();
        RealClock.sleep(Duration::from_secs(3)).await;
        // Channel should be still alive because ack was sent.
        assert!(!tx_status.has_changed().unwrap());
        assert_eq!(*tx_status.borrow(), TxStatus::Active);

        tx.try_post(101, unused_return_channel()).unwrap();
        // Confirm message is sent to rx.
        verify_message(&mut stream, (1, 101), line!()).await;

        if disconnect_before_ack {
            // Prevent link from reconnect
            fail_connect_switch.store(true, Ordering::Release);
            // Break the existing connection
            disconnect_signal.send(()).unwrap();
        }

        // Verify the channel is closed due to ack timeout based on the log.
        let expected_log: &str = if disconnect_before_ack {
            "failed to receive ack within timeout 2 secs; link is currently broken"
        } else {
            "failed to receive ack within timeout 2 secs; link is currently connected"
        };

        verify_tx_closed(&mut tx_status, expected_log).await;
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ack_exceeded_limit_with_connected_link() {
        verify_ack_exceeded_limit(false).await;
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ack_exceeded_limit_with_broken_link() {
        verify_ack_exceeded_limit(true).await;
    }

    // Verify a large number of messages can be delivered and acked with the
    // presence of flakiness in the network, i.e. random delay and disconnection.
    #[async_timed_test(timeout_secs = 60)]
    async fn test_network_flakiness_in_channel() {
        let sampling_rate = 100;
        let mut link = MockLink::<u64>::with_network_flakiness(NetworkFlakiness {
            disconnect_params: Some((0.001, 15, Duration::from_millis(400))),
            latency_range: Some((Duration::from_millis(100), Duration::from_millis(200))),
        });
        link.set_sampling_rate(sampling_rate);
        // Set a large buffer size to improve throughput.
        link.set_buffer_size(1024000);
        let disconnected_count = link.disconnected_count();
        let receiver_storage = link.receiver_storage();
        let listener = MockLinkListener::new(receiver_storage.clone(), link.dest());
        let local_addr = listener.local_addr().unwrap();
        let (_, mut nx): (ChannelAddr, NetRx<u64>) =
            super::serve(listener, local_addr, false).await.unwrap();
        let tx = NetTx::<u64>::new(link);
        let messages: Vec<_> = (0..10001).collect();
        let messages_clone = messages.clone();
        // Put the sender side in a separate task so we can start the receiver
        // side concurrently.
        let send_task_handle = tokio::spawn(async move {
            for message in messages_clone {
                // Add a small delay between messages to give NetRx time to ack.
                // Technically, this test still can pass without this delay. But
                // the test will need a might larger timeout. The reason is
                // fairly convoluted:
                //
                // MockLink uses the number of delivery to calculate the disconnection
                // probability. If NetRx sends messages much faster than NetTx
                // can ack them, there is a higher chance that the messages are
                // not acked before reconnect. Then those message would be redelivered.
                // The repeated redelivery increases the total time of sending
                // these messages.
                RealClock
                    .sleep(Duration::from_micros(rand::random::<u64>() % 100))
                    .await;
                tx.try_post(message, unused_return_channel()).unwrap();
            }
            tracing::debug!("NetTx sent all messages");
            // It is important to return tx instead of dropping it here, because
            // Rx might not receive all messages yet.
            tx
        });

        for message in &messages {
            if message % sampling_rate == 0 {
                tracing::debug!("NetRx received a message: {message}");
            }
            assert_eq!(nx.recv().await.unwrap(), *message);
        }
        tracing::debug!("NetRx received all messages");

        let send_result = send_task_handle.await;
        assert!(send_result.is_ok());

        tracing::debug!(
            "MockLink disconnected {} times.",
            disconnected_count.load(Ordering::SeqCst)
        );
        // TODO(pzhang) after the return_handle work in NetTx is done, add a
        // check here to verify the messages are acked correctly.
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_ack_every_n_messages() {
        let config = config::global::lock();
        let _guard_message_ack = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 600);
        let _guard_time_interval =
            config.override_key(config::MESSAGE_ACK_TIME_INTERVAL, Duration::from_secs(1000));
        sparse_ack().await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_ack_every_time_interval() {
        let config = config::global::lock();
        let _guard_message_ack =
            config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 100000000);
        let _guard_time_interval = config.override_key(
            config::MESSAGE_ACK_TIME_INTERVAL,
            Duration::from_millis(500),
        );
        sparse_ack().await;
    }

    async fn sparse_ack() {
        let mut link = MockLink::<u64>::new();
        // Set a large buffer size to improve throughput.
        link.set_buffer_size(1024000);
        let disconnected_count = link.disconnected_count();
        let receiver_storage = link.receiver_storage();
        let listener = MockLinkListener::new(receiver_storage.clone(), link.dest());
        let local_addr = listener.local_addr().unwrap();
        let (_, mut nx): (ChannelAddr, NetRx<u64>) =
            super::serve(listener, local_addr, false).await.unwrap();
        let tx = NetTx::<u64>::new(link);
        let messages: Vec<_> = (0..20001).collect();
        let messages_clone = messages.clone();
        // Put the sender side in a separate task so we can start the receiver
        // side concurrently.
        let send_task_handle = tokio::spawn(async move {
            for message in messages_clone {
                RealClock
                    .sleep(Duration::from_micros(rand::random::<u64>() % 100))
                    .await;
                tx.try_post(message, unused_return_channel()).unwrap();
            }
            RealClock.sleep(Duration::from_secs(5)).await;
            tracing::debug!("NetTx sent all messages");
            tx
        });

        for message in &messages {
            assert_eq!(nx.recv().await.unwrap(), *message);
        }
        tracing::debug!("NetRx received all messages");

        let send_result = send_task_handle.await;
        assert!(send_result.is_ok());

        tracing::debug!(
            "MockLink disconnected {} times.",
            disconnected_count.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn test_metatls_parsing() {
        // host:port
        let channel: ChannelAddr = "metatls!localhost:1234".parse().unwrap();
        assert_eq!(channel, ChannelAddr::MetaTls("localhost".to_string(), 1234));
        // ipv4:port
        let channel: ChannelAddr = "metatls!1.2.3.4:1234".parse().unwrap();
        assert_eq!(channel, ChannelAddr::MetaTls("1.2.3.4".to_string(), 1234));
        // ipv6:port
        let channel: ChannelAddr = "metatls!2401:db00:33c:6902:face:0:2a2:0:1234"
            .parse()
            .unwrap();
        assert_eq!(
            channel,
            ChannelAddr::MetaTls("2401:db00:33c:6902:face:0:2a2:0".to_string(), 1234)
        );
    }

    #[async_timed_test(timeout_secs = 300)]
    async fn test_tcp_throughput() {
        let config = config::global::lock();
        let _guard =
            config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(300));

        let socket_addr: SocketAddr = "[::1]:0".parse().unwrap();
        let (local_addr, mut rx) = tcp::serve::<String>(socket_addr).await.unwrap();

        // Test with 10 connections (senders), each sends 500K messages, 5M messages in total.
        let total_num_msgs = 500000;

        let receive_handle = tokio::spawn(async move {
            let mut num = 0;
            for _ in 0..10 * total_num_msgs {
                rx.recv().await.unwrap();
                num += 1;

                if num % 100000 == 0 {
                    tracing::info!("total number of received messages: {}", num);
                }
            }
        });

        let mut tx_handles = vec![];
        let mut txs = vec![];
        for _ in 0..10 {
            let server_addr = local_addr.clone();
            let tx = Arc::new(dial::<String>(server_addr).unwrap());
            let tx2 = Arc::clone(&tx);
            txs.push(tx);
            tx_handles.push(tokio::spawn(async move {
                let random_string = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(2048)
                    .map(char::from)
                    .collect::<String>();
                for _ in 0..total_num_msgs {
                    let _ = tx2.try_post(random_string.clone(), unused_return_channel());
                }
            }));
        }

        receive_handle.await.unwrap();
        for handle in tx_handles {
            handle.await.unwrap();
        }
    }
}
