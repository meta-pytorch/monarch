/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TODO

use std::any::type_name;
use std::fmt;
use std::mem::replace;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use anyhow::Context;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use hyperactor_telemetry::skip_record;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt as _;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio_util::net::Listener;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;

use super::serialize_response;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::net::Frame;
use crate::channel::net::NetRx;
use crate::channel::net::NetRxResponse;
use crate::channel::net::ServerError;
use crate::channel::net::framed::FrameReader;
use crate::channel::net::framed::FrameWrite;
use crate::channel::net::framed::WriteState;
use crate::channel::net::meta;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::config;
use crate::metrics;
use crate::sync::mvar::MVar;

fn process_state_span(
    source: &ChannelAddr,
    dest: &ChannelAddr,
    session_id: u64,
    next: &Next,
    rcv_raw_frame_count: u64,
    last_ack_time: tokio::time::Instant,
) -> Span {
    let since_last_ack_str = humantime::format_duration(last_ack_time.elapsed()).to_string();

    let pending_ack_count = if next.seq > next.ack {
        next.seq - next.ack - 1
    } else {
        0
    };

    tracing::span!(
        Level::ERROR,
        "net i/o loop",
        session_id = format!("{}.{}", dest, session_id),
        source = source.to_string(),
        next_seq = next.seq,
        last_ack = next.ack,
        pending_ack_count = pending_ack_count,
        rcv_raw_frame_count = rcv_raw_frame_count,
        since_last_ack = since_last_ack_str.as_str(),
        skip_record,
    )
}

pub(super) struct ServerConn<S> {
    reader: FrameReader<ReadHalf<S>>,
    write_state: WriteState<WriteHalf<S>, Bytes, u64>,
    source: ChannelAddr,
    dest: ChannelAddr,
}

impl<S: AsyncRead + AsyncWrite> ServerConn<S> {
    pub(super) fn new(stream: S, source: ChannelAddr, dest: ChannelAddr) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reader: FrameReader::new(
                reader,
                hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
            ),
            write_state: WriteState::Idle(writer),
            source,
            dest,
        }
    }
}

#[derive(Debug)]
enum RejectConn {
    /// Reject the connection due to the given error.
    EncounterError(String),
    /// The server is being closed.
    ServerClosing,
    /// Do not reject the connection.
    No,
}

impl<S: AsyncRead + AsyncWrite + Send + 'static + Unpin> ServerConn<S> {
    async fn handshake<M: RemoteMessage>(&mut self) -> Result<u64, anyhow::Error> {
        let Some(frame) = self
            .reader
            .next()
            .instrument(tracing::span!(Level::ERROR, "read handshake", skip_record))
            .await?
        else {
            anyhow::bail!("end of stream before first frame from {}", self.source);
        };
        let message = serde_multipart::Message::from_framed(frame)?;
        let Frame::Init(session_id) = serde_multipart::deserialize_bincode::<Frame<M>>(message)?
        else {
            anyhow::bail!("unexpected initial frame from {}", self.source);
        };
        Ok(session_id)
    }

    async fn process_step<M: RemoteMessage>(
        &mut self,
        session_id: u64,
        tx: &mpsc::Sender<M>,
        cancel_token: &CancellationToken,
        next: &Next,
        last_ack_time: &mut tokio::time::Instant,
        rcv_raw_frame_count: &mut u64,
        ack_time_interval: Duration,
        ack_msg_interval: u64,
        log_id: &str,
    ) -> (Next, Option<(Result<(), anyhow::Error>, RejectConn)>) {
        let mut next = next.clone();
        if self.write_state.is_idle()
            && (next.ack + ack_msg_interval <= next.seq
                || (next.ack < next.seq && last_ack_time.elapsed() > ack_time_interval))
        {
            let Ok(writer) = replace(&mut self.write_state, WriteState::Broken).into_idle() else {
                panic!("illegal state");
            };
            let ack = match serialize_response(NetRxResponse::Ack(next.seq - 1)) {
                Ok(ack) => ack,
                Err(err) => {
                    return (
                        next,
                        Some((
                            Err::<(), anyhow::Error>(err.into())
                                .context(format!("{log_id}: serializing ack")),
                            RejectConn::No,
                        )),
                    );
                }
            };
            match FrameWrite::new(
                writer,
                ack,
                hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
            ) {
                Ok(fw) => {
                    self.write_state = WriteState::Writing(fw, next.seq);
                }
                Err((writer, e)) => {
                    debug_assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
                    tracing::error!(
                        source = %self.source,
                        dest = %self.dest,
                        error = %e,
                        "failed to create ack frame"
                    );
                    self.write_state = WriteState::Idle(writer);
                }
            }
        }

        tokio::select! {
            // Prioritize ack, and then shutdown. Leave read last because
            // there could be a large volume of messages to read, which
            // subsequently starves the other select! branches.
            biased;

            // We have to be careful to manage the ack write state here, so that we do not
            // write partial acks in the presence of cancellation.
            ack_result = self.write_state.send().instrument(tracing::span!(Level::ERROR, "write ack", skip_record)) => {
                match ack_result {
                    Ok(acked_seq) => {
                        *last_ack_time = RealClock.now();
                        next.ack = acked_seq;
                    }
                    Err(err) => {
                        let v = self.write_state.value();
                        return (
                            next,
                            Some((
                                Err::<(), anyhow::Error>(err.into())
                                    .context(format!("{log_id}: acking peer message: {v:?}")),
                                RejectConn::No,
                            )),
                        );
                    }
                }
            },
            // Have a tick to abort select! call to make sure the ack for the last message can get the chance
            // to be sent as a result of time interval being reached.
            _ = RealClock.sleep_until(*last_ack_time + ack_time_interval), if next.ack < next.seq => {},

            _ = cancel_token.cancelled() => return (next, Some((Ok(()), RejectConn::ServerClosing))),

            bytes_result = self.reader.next().instrument(tracing::span!(Level::ERROR, "read bytes", skip_record)) => {
                *rcv_raw_frame_count += 1;
                // First handle transport-level I/O errors, and EOFs.
                let bytes = match bytes_result {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => {
                        tracing::debug!(
                                source = %self.source,
                                dest = %self.dest,
                                "received EOF from client"
                            );
                        return (next, Some((Ok(()), RejectConn::No)));
                    }
                    Err(err) => {
                        return (
                            next,
                            Some((
                                Err::<(), anyhow::Error>(err.into()).context(format!(
                                    "{log_id}: reading into Frame with M = {}",
                                    type_name::<M>(),
                                )),
                                RejectConn::No,
                            )),
                        )
                    }
                };

                // De-frame the multi-part message.
                let bytes_len = bytes.len();
                let message = match serde_multipart::Message::from_framed(bytes) {
                    Ok(message) => message,
                    Err(err) => {
                        // Track deframing error for this channel pair
                        metrics::CHANNEL_ERRORS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "source" => self.source.to_string(),
                                "dest" => self.dest.to_string(),
                                "session_id" => session_id.to_string(),
                                "error_type" => metrics::ChannelErrorType::DeframeError.as_str(),
                            ),
                        );
                        return (
                            next,
                            Some((
                                Err::<(), anyhow::Error>(err.into()).context(format!(
                                    "{log_id}: de-frame message with M = {}",
                                    type_name::<M>(),
                                )),
                                RejectConn::No,
                            )),
                        )
                    }
                };

                // Finally decode the message. This assembles the M-typed message
                // from its constituent parts.
                match serde_multipart::deserialize_bincode(message) {
                    Ok(Frame::Init(_)) => {
                        return (
                            next,
                            Some((
                                Err(anyhow::anyhow!("{log_id}: unexpected init frame")),
                                RejectConn::EncounterError("expect Frame::Message; got Frame::Int".to_string())
                            )),
                        )
                    },
                    // Ignore retransmits.
                    Ok(Frame::Message(seq, _)) if seq < next.seq => {
                        tracing::debug!(
                            source = %self.source,
                            dest = %self.dest,
                            seq = seq,
                            "ignoring retransmit; retransmit seq: {}; expected next seq: {}",
                            seq,
                            next.seq
                        );
                    },
                    // The following segment ensures exactly-once semantics.
                    // That means No out-of-order delivery and no duplicate delivery.
                    Ok(Frame::Message(seq, message)) => {
                        // received seq should be equal to next seq. Else error out!
                        if seq > next.seq {
                            let error_msg = format!("out-of-sequence message, expected seq {}, got {}", next.seq, seq);
                            tracing::error!(
                                source = %self.source,
                                dest = %self.dest,
                                seq = seq,
                                "{}", error_msg
                            );
                            return (
                                next,
                                Some((
                                    Err(anyhow::anyhow!(format!("{log_id}: {error_msg}"))),
                                    RejectConn::EncounterError(error_msg)
                                ))
                            )
                        }
                        match self.send_with_buffer_metric(session_id, tx, message)
                            .instrument(tracing::info_span!(
                                "send_with_buffer_metric",
                                seq = seq,
                                skip_record,
                            ))
                            .await
                        {
                            Ok(()) => {
                                // Track throughput for this channel pair
                                metrics::CHANNEL_THROUGHPUT_BYTES.add(
                                    bytes_len as u64,
                                    hyperactor_telemetry::kv_pairs!(
                                        "source" => self.source.to_string(),
                                        "dest" => self.dest.to_string(),
                                        "session_id" => session_id.to_string(),
                                    ),
                                );
                                metrics::CHANNEL_THROUGHPUT_MESSAGES.add(
                                    1,
                                    hyperactor_telemetry::kv_pairs!(
                                        "source" => self.source.to_string(),
                                        "dest" => self.dest.to_string(),
                                        "session_id" => session_id.to_string(),
                                    ),
                                );
                                // In channel's contract, "delivered" means the message
                                // is sent to the NetRx object. Therefore, we could bump
                                // `next_seq` as far as the message is put on the mpsc
                                // channel.
                                //
                                // Note that when/how the messages in NetRx are processed
                                // is not covered by channel's contract. For example,
                                // the message might never be taken out of netRx, but
                                // channel still considers those messages delivered.
                                next.seq = seq+1;
                            }
                            Err(err) => {
                                return (
                                    next,
                                    Some((
                                        Err::<(), anyhow::Error>(err)
                                            .context(format!("{log_id}: relaying message to mpsc channel")),
                                        RejectConn::No,
                                    )),
                                )
                            }
                        }
                    },
                    Err(err) => {
                        // Track deserialization error for this channel pair
                        metrics::CHANNEL_ERRORS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "source" => self.source.to_string(),
                                "dest" => self.dest.to_string(),
                                "session_id" => session_id.to_string(),
                                "error_type" => metrics::ChannelErrorType::DeserializeError.as_str(),
                            ),
                        );
                        return (
                           next,
                            Some((
                                Err::<(), anyhow::Error>(err.into()).context(format!(
                                    "{log_id}: deserialize message with M = {}",
                                    type_name::<M>(),
                                )),
                                RejectConn::No,
                            )),
                        )
                    }
                }
            },
        }

        (next, None)
    }

    /// Handles a server side stream created during the `listen` loop.
    async fn process<M: RemoteMessage>(
        &mut self,
        session_id: u64,
        tx: mpsc::Sender<M>,
        cancel_token: CancellationToken,
        mut next: Next,
    ) -> (Next, Result<(), anyhow::Error>) {
        let log_id = format!("session {}.{}<-{}", self.dest, session_id, self.source);
        let initial_next: Next = next.clone();
        let mut rcv_raw_frame_count = 0u64;
        let mut last_ack_time = RealClock.now();

        let ack_time_interval = hyperactor_config::global::get(config::MESSAGE_ACK_TIME_INTERVAL);
        let ack_msg_interval = hyperactor_config::global::get(config::MESSAGE_ACK_EVERY_N_MESSAGES);

        let (mut final_next, final_result, reject_conn) = loop {
            let span = process_state_span(
                &self.source,
                &self.dest,
                session_id,
                &next,
                rcv_raw_frame_count,
                last_ack_time,
            );

            let (new_next, break_info) = self
                .process_step(
                    session_id,
                    &tx,
                    &cancel_token,
                    &next,
                    &mut last_ack_time,
                    &mut rcv_raw_frame_count,
                    ack_time_interval,
                    ack_msg_interval,
                    &log_id,
                )
                .instrument(span)
                .await;

            next = new_next;

            if let Some((result, reject_conn)) = break_info {
                break (next, result, reject_conn);
            }
        };

        // Note:
        //   1. processed seq/ack is Next-1;
        //   2. rcv_raw_frame_count contains the last frame which might not be
        //      desrializable, e.g. EOF, error, etc.
        let debug_msg = format!(
            "NetRx::process exited its loop with states: initial Next \
            was {initial_next}; final Next is {final_next}; since acked: {}sec; \
            rcv raw frame count is {rcv_raw_frame_count}; final result: {:?}; \
            reject_conn is {:?}",
            last_ack_time.elapsed().as_secs(),
            final_result,
            reject_conn,
        );
        tracing::info!(
            source = %self.source,
            dest = %self.dest,
            session_id,
            "{}", debug_msg
        );

        let mut final_ack = final_next.ack;
        // Flush any ongoing write.
        if self.write_state.is_writing() {
            if let Ok(acked_seq) = self.write_state.send().await {
                if acked_seq > final_ack {
                    final_ack = acked_seq;
                }
            };
        }
        // best effort: "flush" any remaining ack before closing this session
        if self.write_state.is_idle() && final_ack < final_next.seq {
            let Ok(writer) = replace(&mut self.write_state, WriteState::Broken).into_idle() else {
                panic!("illegal state");
            };
            let result = async {
                let ack = serialize_response(NetRxResponse::Ack(final_next.seq - 1))
                    .map_err(anyhow::Error::from)?;

                let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
                let fw =
                    FrameWrite::new(writer, ack, max).map_err(|(_, e)| anyhow::Error::from(e))?;
                self.write_state = WriteState::Writing(fw, final_next.seq);
                self.write_state.send().await.map_err(anyhow::Error::from)
            };

            match result.await {
                Ok(acked_seq) => {
                    final_next.ack = acked_seq;
                }
                Err(e) => {
                    tracing::debug!(
                        session_id,
                        source = %self.source,
                        dest = %self.dest,
                        error = %e,
                        "failed to flush acks during cleanup"
                    );
                }
            }
        }

        if self.write_state.is_idle()
            && matches!(
                reject_conn,
                RejectConn::EncounterError(_) | RejectConn::ServerClosing
            )
        {
            let Ok(writer) = replace(&mut self.write_state, WriteState::Broken).into_idle() else {
                panic!("illegal state");
            };
            let rsp = match reject_conn {
                RejectConn::EncounterError(reason) => NetRxResponse::Reject(reason),
                RejectConn::ServerClosing => NetRxResponse::Closed,
                RejectConn::No => panic!("illegal state"),
            };
            if let Ok(data) = serialize_response(rsp) {
                match FrameWrite::new(
                    writer,
                    data,
                    hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
                ) {
                    Ok(fw) => {
                        self.write_state = WriteState::Writing(fw, 0);
                        let _ = self.write_state.send().await;
                    }
                    Err((w, e)) => {
                        debug_assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
                        tracing::debug!(
                            source = %self.source,
                            dest = %self.dest,
                            session_id = session_id,
                            error = %e,
                            "failed to create reject frame"
                        );
                        self.write_state = WriteState::Idle(w);
                        // drop the reject; we're closing anyway
                    }
                }
            };
        }

        if let Some(mut w) = replace(&mut self.write_state, WriteState::Broken).into_writer() {
            // Try to shutdown the connection gracefully. This is a best effort
            // operation, and we don't care if it fails.
            let _ = w.shutdown().await;
        }

        (final_next, final_result)
    }

    // NetRx's buffer, i.e. the mpsc channel between NetRx and its
    // client, should rarely be full for long. But when it is full, it
    // will block NetRx from taking more messages, sending back ack,
    // and subsequently lead to uncommon behaviors such as ack
    // timeout, backpressure on NetTx, etc. In order to aid debugging,
    // it is important to add a metric measuring full buffer
    // occurences.
    async fn send_with_buffer_metric<M: RemoteMessage>(
        &mut self,
        session_id: u64,
        tx: &mpsc::Sender<M>,
        message: M,
    ) -> anyhow::Result<()> {
        let start = RealClock.now();
        loop {
            tokio::select! {
                biased;
                permit_result = tx.reserve() => {
                    permit_result?.send(message);
                    return Ok(())
                }
                _ = RealClock.sleep(hyperactor_config::global::get(config::CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL)) => {
                    // When buffer is full too long, we log it.
                    metrics::CHANNEL_NET_RX_BUFFER_FULL.add(
                        1,
                        hyperactor_telemetry::kv_pairs!(
                            "dest" => self.dest.to_string(),
                            "source" => self.source.to_string(),
                            "session_id" => session_id.to_string(),
                        ),
                    );
                    // Full buffer should happen rarely. So we also add a log
                    // here to make debugging easy.
                     tracing::debug!(
                        source = %self.source,
                        dest = %self.dest,
                        session_id = session_id,
                        "encountered full mpsc channel for {} secs",
                        start.elapsed().as_secs(),
                    );
                }
            }
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

impl fmt::Display for Next {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(seq: {}, ack: {})", self.seq, self.ack)
    }
}

/// Manages persistent sessions, ensuring that only one connection can own
/// a session at a time, and arranging for session handover.
#[derive(Clone)]
pub(super) struct SessionManager {
    sessions: Arc<DashMap<u64, MVar<Next>>>,
}

impl SessionManager {
    pub(super) fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }

    pub(super) async fn serve<S, M>(
        &self,
        mut conn: ServerConn<S>,
        tx: mpsc::Sender<M>,
        cancel_token: CancellationToken,
    ) -> Result<(), anyhow::Error>
    where
        S: AsyncRead + AsyncWrite + Send + 'static + Unpin,
        M: RemoteMessage,
    {
        let session_id = conn
            .handshake::<M>()
            .await
            .context("while serving handshake")?;

        let session_var = match self.sessions.entry(session_id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                // We haven't seen this session before. We begin with seq=0 and ack=0.
                let var = MVar::full(Next { seq: 0, ack: 0 });
                entry.insert(var.clone());
                var
            }
        };

        let source = conn.source.clone();
        let dest = conn.dest.clone();

        let next = session_var.take().await;
        let (next, res) = conn.process(session_id, tx, cancel_token, next).await;
        session_var.put(next).await;

        if let Err(ref err) = res {
            tracing::info!(
                source = %source,
                dest = %dest,
                error = ?err,
                session_id = session_id,
                "process encountered an error"
            );
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
                match result {
                    Ok((stream, addr)) => {
                        let source : ChannelAddr = addr.into();
                        tracing::debug!(
                            source = %source,
                            addr = %listener_channel_addr,
                            "new connection accepted"
                        );
                        metrics::CHANNEL_CONNECTIONS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => listener_channel_addr.transport().to_string(),
                                "operation" => "accept"
                            ),
                        );

                        let tx = tx.clone();
                        let child_cancel_token = child_cancel_token.child_token();
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
                                        metrics::CHANNEL_ERRORS.add(
                                    1,
                                    hyperactor_telemetry::kv_pairs!(
                                        "transport" => dest.transport().to_string(),
                                        "error" => err.to_string(),
                                        "error_type" => metrics::ChannelErrorType::ConnectionError.as_str(),
                                        "dest" => dest.to_string(),
                                    ),
                                );

                                // we don't want the health probe TCP connections to be counted as an error.
                                match source {
                                    ChannelAddr::Tcp(source_addr) if source_addr.ip().is_loopback() => {},
                                    _ => {
                                        tracing::info!(
                                            source = %source,
                                            dest = %dest,
                                            error = ?err,
                                            "error processing peer connection"
                                        );

                                    }
                                }
                            }
                            res
                    });
                    }
                    Err(err) => {
                        metrics::CHANNEL_ERRORS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => listener_channel_addr.transport().to_string(),
                                "operation" => "accept",
                                "error" => err.to_string(),
                                "error_type" => metrics::ChannelErrorType::ConnectionError.as_str(),
                            ),
                        );

                        tracing::info!(
                            addr = %listener_channel_addr,
                            error = %err,
                            "accept error"
                        );
                    }
                }
            }

            _ = parent_cancel_token.cancelled() => {
                tracing::info!(
                    addr = %listener_channel_addr,
                    "received parent token cancellation"
                );
                break Ok(());
            }

            result = join_nonempty(&mut connections) => {
                if let Err(err) = result {
                    tracing::info!(
                        addr = %listener_channel_addr,
                        error = %err,
                        "connection task join error"
                    );
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
    pub(crate) fn stop(&self, reason: &str) {
        tracing::info!(
            name = "ChannelServerStatus",
            addr = %self.channel_addr,
            status = "Stop::Sent",
            reason,
            "sent Stop signal; check server logs for the stop progress"
        );
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

/// serve new connections that are accepted from the given listener.
pub(super) fn serve<M: RemoteMessage, L: Listener + Send + Unpin + 'static>(
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
