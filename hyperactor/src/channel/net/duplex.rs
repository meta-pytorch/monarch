/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Duplex-mode channels over the net link layer.
//!
//! A single physical connection carries messages in both directions,
//! each with independent sequence/ack state.
//!
//! ## Wire protocol
//!
//! Each connection starts with a `DuplexLinkInit` header (12 bytes,
//! unframed):
//!
//! ```text
//! [magic: 4B "DPX\0"] [link_id: 8B u64 BE]
//! ```
//!
//! After the init, the standard tagged frame format is used. The tag
//! byte in the 8-byte header distinguishes logical channels:
//!
//! - `Side::A = 0x00` — initiator→acceptor channel
//! - `Side::B = 0x01` — acceptor→initiator channel

#![allow(dead_code)] // until used

use std::io;
use std::mem::replace;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::WriteHalf;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use super::Frame;
use super::LinkId;
use super::NetRxResponse;
use super::ServerError;
use super::deserialize_response;
use super::framed::FrameReader;
use super::framed::FrameWrite;
use super::framed::WriteState;
use super::serialize_response;
use super::server::Next;
use super::server::ServerHandle;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::config;
use crate::sync::mvar::MVar;

/// Logical channel tag packed into the frame header's first byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Side {
    /// Initiator→acceptor logical channel.
    A = 0x00,
    /// Acceptor→initiator logical channel.
    B = 0x01,
}

const DUPLEX_LINK_INIT_MAGIC: [u8; 4] = *b"DPX\0";
const DUPLEX_LINK_INIT_SIZE: usize = 4 + 8;

/// Write a DuplexLinkInit header to the stream.
async fn write_duplex_link_init<S: AsyncWrite + Unpin>(
    stream: &mut S,
    link_id: LinkId,
) -> Result<(), io::Error> {
    let mut buf = [0u8; DUPLEX_LINK_INIT_SIZE];
    buf[0..4].copy_from_slice(&DUPLEX_LINK_INIT_MAGIC);
    buf[4..12].copy_from_slice(&link_id.0.to_be_bytes());
    stream.write_all(&buf).await
}

/// Read a DuplexLinkInit header from the stream.
async fn read_duplex_link_init<S: AsyncRead + Unpin>(stream: &mut S) -> Result<LinkId, io::Error> {
    let mut buf = [0u8; DUPLEX_LINK_INIT_SIZE];
    stream.read_exact(&mut buf).await?;
    if buf[0..4] != DUPLEX_LINK_INIT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid DuplexLinkInit magic: expected {:?}, got {:?}",
                DUPLEX_LINK_INIT_MAGIC,
                &buf[0..4]
            ),
        ));
    }
    let link_id = LinkId(u64::from_be_bytes(buf[4..12].try_into().unwrap()));
    Ok(link_id)
}

/// Both channels share one `WriteState`. Acks are `Bytes` and messages
/// are `serde_multipart::Frame`. This enum delegates `Buf` to the
/// active variant without allocation.
pub(crate) enum DuplexBuf {
    Ack(Bytes),
    Msg(serde_multipart::Frame),
}

impl Buf for DuplexBuf {
    fn remaining(&self) -> usize {
        match self {
            DuplexBuf::Ack(b) => b.remaining(),
            DuplexBuf::Msg(f) => f.remaining(),
        }
    }

    fn chunk(&self) -> &[u8] {
        match self {
            DuplexBuf::Ack(b) => b.chunk(),
            DuplexBuf::Msg(f) => f.chunk(),
        }
    }

    fn advance(&mut self, cnt: usize) {
        match self {
            DuplexBuf::Ack(b) => b.advance(cnt),
            DuplexBuf::Msg(f) => f.advance(cnt),
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [io::IoSlice<'a>]) -> usize {
        match self {
            DuplexBuf::Ack(b) => b.chunks_vectored(dst),
            DuplexBuf::Msg(f) => f.chunks_vectored(dst),
        }
    }
}

/// Per-link server state persisting across reconnections.
struct DuplexServerLink<M1: RemoteMessage, M2: RemoteMessage> {
    #[allow(dead_code)]
    id: LinkId,
    /// (inbound_next, outbound_next) — taken/put atomically with MVar.
    next: MVar<(Next, Next)>,
    /// Delivers inbound M1 messages to the link's Rx.
    inbound_tx: mpsc::Sender<M1>,
    /// Taken by the connection handler, put back on disconnect.
    outbound_rx: MVar<mpsc::UnboundedReceiver<M2>>,
}

/// Public duplex server that yields `(NetRx<M1>, DuplexNetTx<M2>)` pairs.
pub(crate) struct DuplexServer<M1: RemoteMessage, M2: RemoteMessage> {
    accept_rx: mpsc::Receiver<(DuplexNetRx<M1>, DuplexNetTx<M2>)>,
    _handle: ServerHandle,
    addr: ChannelAddr,
}

impl<M1: RemoteMessage, M2: RemoteMessage> DuplexServer<M1, M2> {
    pub async fn accept(&mut self) -> Result<(DuplexNetRx<M1>, DuplexNetTx<M2>), ChannelError> {
        self.accept_rx.recv().await.ok_or(ChannelError::Closed)
    }

    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }
}

/// Receiver half of a duplex channel.
pub(crate) struct DuplexNetRx<M: RemoteMessage> {
    rx: mpsc::Receiver<M>,
    addr: ChannelAddr,
}

impl<M: RemoteMessage> DuplexNetRx<M> {
    pub async fn recv(&mut self) -> Result<M, ChannelError> {
        self.rx.recv().await.ok_or(ChannelError::Closed)
    }

    #[allow(dead_code)]
    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }
}

/// Sender half of a duplex channel.
pub(crate) struct DuplexNetTx<M: RemoteMessage> {
    tx: mpsc::UnboundedSender<M>,
    addr: ChannelAddr,
}

impl<M: RemoteMessage> DuplexNetTx<M> {
    pub fn send(&self, message: M) -> Result<(), ChannelError> {
        self.tx.send(message).map_err(|_| ChannelError::Closed)
    }

    #[allow(dead_code)]
    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }
}

impl<M: RemoteMessage> Clone for DuplexNetTx<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            addr: self.addr.clone(),
        }
    }
}

/// Start a duplex server on the given address.
pub(crate) fn serve<M1: RemoteMessage, M2: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<DuplexServer<M1, M2>, ServerError> {
    match addr {
        ChannelAddr::Tcp(socket_addr) => {
            let std_listener = std::net::TcpListener::bind(socket_addr)
                .map_err(|err| ServerError::Listen(ChannelAddr::Tcp(socket_addr), err))?;
            std_listener
                .set_nonblocking(true)
                .map_err(|e| ServerError::Listen(ChannelAddr::Tcp(socket_addr), e))?;
            let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
                .map_err(|e| ServerError::Listen(ChannelAddr::Tcp(socket_addr), e))?;
            let local_addr = tokio_listener
                .local_addr()
                .map_err(|err| ServerError::Resolve(ChannelAddr::Tcp(socket_addr), err))?;

            let listener = super::tcp::TcpSocketListener {
                inner: tokio_listener,
                addr: local_addr,
            };
            let channel_addr = ChannelAddr::Tcp(local_addr);
            serve_with_listener(listener, channel_addr)
        }
        ChannelAddr::Unix(ref unix_addr) => {
            use std::os::unix::net::UnixDatagram as StdUnixDatagram;
            use std::os::unix::net::UnixListener as StdUnixListener;

            let caddr = addr.clone();
            let maybe_listener = match unix_addr {
                super::unix::SocketAddr::Bound(sock_addr) => StdUnixListener::bind_addr(sock_addr),
                super::unix::SocketAddr::Unbound => StdUnixDatagram::unbound()
                    .and_then(|u| u.local_addr())
                    .and_then(|uaddr| StdUnixListener::bind_addr(&uaddr)),
            };
            let std_listener =
                maybe_listener.map_err(|err| ServerError::Listen(caddr.clone(), err))?;
            std_listener
                .set_nonblocking(true)
                .map_err(|err| ServerError::Listen(caddr.clone(), err))?;
            let local_addr = std_listener
                .local_addr()
                .map_err(|err| ServerError::Resolve(caddr.clone(), err))?;
            let tokio_listener = tokio::net::UnixListener::from_std(std_listener)
                .map_err(|err| ServerError::Io(caddr, err))?;
            let bound_addr = super::unix::SocketAddr::new(local_addr);
            let listener = super::unix::UnixSocketListener {
                inner: tokio_listener,
                addr: bound_addr.clone(),
            };
            let channel_addr = ChannelAddr::Unix(bound_addr);
            serve_with_listener(listener, channel_addr)
        }
        _ => Err(ServerError::Listen(
            addr.clone(),
            io::Error::other(format!("duplex not supported for transport: {}", addr)),
        )),
    }
}

/// Generic helper that wires a listener to the duplex listen loop.
fn serve_with_listener<M1: RemoteMessage, M2: RemoteMessage, L: super::Listener>(
    listener: L,
    channel_addr: ChannelAddr,
) -> Result<DuplexServer<M1, M2>, ServerError> {
    let (accept_tx, accept_rx) = mpsc::channel(16);

    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();
    let ca = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        duplex_listen::<M1, M2, L>(listener, ca, accept_tx, child_token).await
    });

    let server_handle = ServerHandle::new(join_handle, cancel_token, channel_addr.clone());

    Ok(DuplexServer {
        accept_rx,
        _handle: server_handle,
        addr: channel_addr,
    })
}

/// Main listen loop for duplex connections.
async fn duplex_listen<M1: RemoteMessage, M2: RemoteMessage, L: super::Listener>(
    mut listener: L,
    listener_addr: ChannelAddr,
    accept_tx: mpsc::Sender<(DuplexNetRx<M1>, DuplexNetTx<M2>)>,
    cancel_token: CancellationToken,
) -> Result<(), ServerError> {
    let child_cancel_token = CancellationToken::new();
    let links: Arc<DashMap<LinkId, Arc<DuplexServerLink<M1, M2>>>> = Arc::new(DashMap::new());
    let mut connections: JoinSet<Result<(), anyhow::Error>> = JoinSet::new();

    let result: Result<(), ServerError> = loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((mut stream, _peer_addr)) => {
                        // Read DuplexLinkInit from the connection.
                        let link_id = match read_duplex_link_init(&mut stream).await {
                            Ok(id) => id,
                            Err(e) => {
                                tracing::info!(error = %e, "failed to read DuplexLinkInit");
                                continue;
                            }
                        };

                        let links = Arc::clone(&links);
                        let accept_tx = accept_tx.clone();
                        let ct = child_cancel_token.child_token();
                        let addr = listener_addr.clone();

                        connections.spawn(async move {
                            // Look up or create the link.
                            let is_new;
                            let link = {
                                let entry = links.entry(link_id);
                                match entry {
                                    dashmap::mapref::entry::Entry::Occupied(e) => {
                                        is_new = false;
                                        e.get().clone()
                                    }
                                    dashmap::mapref::entry::Entry::Vacant(e) => {
                                        is_new = true;
                                        let (inbound_tx, inbound_rx) = mpsc::channel::<M1>(1024);
                                        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<M2>();
                                        let link = Arc::new(DuplexServerLink {
                                            id: link_id,
                                            next: MVar::full((
                                                Next { seq: 0, ack: 0 },
                                                Next { seq: 0, ack: 0 },
                                            )),
                                            inbound_tx,
                                            outbound_rx: MVar::full(outbound_rx),
                                        });
                                        e.insert(link.clone());

                                        // Send the new channel pair to accept().
                                        let net_rx = DuplexNetRx {
                                            rx: inbound_rx,
                                            addr: addr.clone(),
                                        };
                                        let net_tx = DuplexNetTx {
                                            tx: outbound_tx,
                                            addr: addr.clone(),
                                        };
                                        let _ = accept_tx.send((net_rx, net_tx)).await;

                                        link
                                    }
                                }
                            };

                            tracing::debug!(
                                link_id = %link_id,
                                is_new = is_new,
                                "duplex connection accepted"
                            );

                            handle_duplex_connection(stream, link, ct).await
                        });
                    }
                    Err(err) => {
                        tracing::info!(error = %err, "duplex accept error");
                    }
                }
            }

            _ = cancel_token.cancelled() => {
                break Ok(());
            }

            result = join_nonempty(&mut connections) => {
                if let Err(err) = result {
                    tracing::info!(error = %err, "duplex connection task join error");
                }
            }
        }
    };

    child_cancel_token.cancel();
    while connections.join_next().await.is_some() {}
    result
}

async fn join_nonempty<T: 'static>(set: &mut JoinSet<T>) -> Result<T, tokio::task::JoinError> {
    match set.join_next().await {
        None => std::future::pending().await,
        Some(result) => result,
    }
}

/// Handle a single duplex connection. Takes `(inbound_next, outbound_next)`
/// from the link's MVar, runs the select! loop, and puts them back on close.
async fn handle_duplex_connection<M1: RemoteMessage, M2: RemoteMessage, S>(
    stream: S,
    link: Arc<DuplexServerLink<M1, M2>>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let mut frame_reader = FrameReader::new(
        reader,
        hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
    );

    // Take session state.
    let (mut inbound_next, mut outbound_next) = link.next.take().await;
    let mut outbound_rx = link.outbound_rx.take().await;

    let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
    let ack_msg_interval: u64 =
        hyperactor_config::global::get(config::MESSAGE_ACK_EVERY_N_MESSAGES);
    let ack_time_interval: Duration =
        hyperactor_config::global::get(config::MESSAGE_ACK_TIME_INTERVAL);

    let mut write_state: WriteState<WriteHalf<S>, DuplexBuf, Side> = WriteState::Idle(writer);
    let mut last_inbound_ack_time = RealClock.now();

    // Retransmit unacked outbound messages.
    // We don't have a replay buffer on the server side for outbound yet,
    // so we just track seq/ack for ordering. In a full implementation
    // we'd replay from a buffer. For now, outbound starts fresh each
    // connection.

    let result: Result<(), anyhow::Error> = loop {
        let inbound_ack_behind = inbound_next.ack + ack_msg_interval <= inbound_next.seq
            || (inbound_next.ack < inbound_next.seq
                && last_inbound_ack_time.elapsed() > ack_time_interval);

        tokio::select! {
            biased;

            // Drive in-progress write to completion.
            result = write_state.send() => {
                match result {
                    Ok(side) => {
                        if side == Side::A {
                            last_inbound_ack_time = RealClock.now();
                            inbound_next.ack = inbound_next.seq;
                        }
                    }
                    Err(e) => {
                        break Err(e.into());
                    }
                }
            }

            // Inbound ack write (Side::A — we ack the client's messages).
            _ = std::future::ready(()), if write_state.is_idle() && inbound_ack_behind => {
                let Ok(writer) = replace(&mut write_state, WriteState::Broken).into_idle() else {
                    panic!("illegal state");
                };
                let ack = serialize_response(NetRxResponse::Ack(inbound_next.seq - 1))?;
                match FrameWrite::new(writer, DuplexBuf::Ack(ack), max, Side::A as u8) {
                    Ok(fw) => {
                        write_state = WriteState::Writing(fw, Side::A);
                    }
                    Err((w, _e)) => {
                        write_state = WriteState::Idle(w);
                    }
                }
            }

            // Outbound message write (Side::B — server sends M2 to client).
            msg = outbound_rx.recv(), if write_state.is_idle() => {
                match msg {
                    Some(message) => {
                        let Ok(writer) = replace(&mut write_state, WriteState::Broken).into_idle() else {
                            panic!("illegal state");
                        };
                        let msg = serde_multipart::serialize_bincode(
                            &Frame::Message(outbound_next.seq, message),
                        )?;
                        match FrameWrite::new(writer, DuplexBuf::Msg(msg.framed()), max, Side::B as u8) {
                            Ok(fw) => {
                                outbound_next.seq += 1;
                                write_state = WriteState::Writing(fw, Side::B);
                            }
                            Err((w, e)) => {
                                write_state = WriteState::Idle(w);
                                tracing::error!(error = %e, "failed to create outbound frame");
                            }
                        }
                    }
                    None => {
                        // Outbound channel closed; send Closed response and exit.
                        break Ok(());
                    }
                }
            }

            // Read from the wire.
            frame_result = frame_reader.next() => {
                match frame_result {
                    Ok(Some((tag, bytes))) => {
                        if tag == Side::A as u8 {
                            // Side::A frame from client = inbound message (M1).
                            let message = serde_multipart::Message::from_framed(bytes)?;
                            let frame: Frame<M1> = serde_multipart::deserialize_bincode(message)?;
                            match frame {
                                Frame::Message(seq, msg) => {
                                    if seq < inbound_next.seq {
                                        // Retransmit — ignore.
                                        tracing::debug!(seq, "duplex: ignoring inbound retransmit");
                                    } else if seq == inbound_next.seq {
                                        link.inbound_tx.send(msg).await
                                            .map_err(|_| anyhow::anyhow!("inbound channel closed"))?;
                                        inbound_next.seq += 1;
                                    } else {
                                        break Err(anyhow::anyhow!(
                                            "out-of-sequence inbound message: expected {}, got {}",
                                            inbound_next.seq, seq
                                        ));
                                    }
                                }
                            }
                        } else if tag == Side::B as u8 {
                            // Side::B frame from client = ack for our outbound M2.
                            let response = deserialize_response(bytes)?;
                            match response {
                                NetRxResponse::Ack(acked_seq) => {
                                    outbound_next.ack = acked_seq + 1;
                                }
                                NetRxResponse::Reject(reason) => {
                                    break Err(anyhow::anyhow!("peer rejected: {}", reason));
                                }
                                NetRxResponse::Closed => {
                                    break Ok(());
                                }
                            }
                        } else {
                            tracing::warn!(tag, "duplex: unknown tag, ignoring frame");
                        }
                    }
                    Ok(None) => {
                        // EOF — peer disconnected.
                        break Ok(());
                    }
                    Err(e) => {
                        break Err(e.into());
                    }
                }
            }

            _ = cancel_token.cancelled() => {
                break Ok(());
            }
        }
    };

    // Put state back for reconnection.
    link.next.put((inbound_next, outbound_next)).await;
    link.outbound_rx.put(outbound_rx).await;

    result
}

/// Connect to a duplex server, returning tx and rx handles.
pub(crate) async fn dial<M1: RemoteMessage, M2: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(DuplexNetTx<M1>, DuplexNetRx<M2>), super::ClientError> {
    let link_id = LinkId::random();

    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<M1>();
    let (inbound_tx, inbound_rx) = mpsc::channel::<M2>(1024);

    let ca = addr.clone();
    tokio::spawn(async move {
        duplex_client_run::<M1, M2>(ca, link_id, outbound_rx, inbound_tx).await;
    });

    Ok((
        DuplexNetTx {
            tx: outbound_tx,
            addr: addr.clone(),
        },
        DuplexNetRx {
            rx: inbound_rx,
            addr,
        },
    ))
}

/// Background task that manages the client side of a duplex connection,
/// including reconnection.
async fn duplex_client_run<M1: RemoteMessage, M2: RemoteMessage>(
    addr: ChannelAddr,
    link_id: LinkId,
    mut outbound_rx: mpsc::UnboundedReceiver<M1>,
    inbound_tx: mpsc::Sender<M2>,
) {
    use backoff::ExponentialBackoffBuilder;
    use backoff::backoff::Backoff;

    let mut outbound_next = Next { seq: 0, ack: 0 };
    let mut inbound_next = Next { seq: 0, ack: 0 };

    // Replay buffer for unacked outbound messages.
    let mut unacked: std::collections::VecDeque<(u64, Bytes)> = std::collections::VecDeque::new();

    let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
    let ack_msg_interval: u64 =
        hyperactor_config::global::get(config::MESSAGE_ACK_EVERY_N_MESSAGES);
    let ack_time_interval: Duration =
        hyperactor_config::global::get(config::MESSAGE_ACK_TIME_INTERVAL);

    let mut backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(100))
        .with_max_interval(Duration::from_secs(5))
        .with_max_elapsed_time(None)
        .build();

    loop {
        // Connect.
        let stream = match super::connect_raw(&addr).await {
            Ok(mut s) => {
                if let Err(e) = write_duplex_link_init(&mut s, link_id).await {
                    tracing::info!(error = %e, "failed to write DuplexLinkInit");
                    if let Some(d) = backoff.next_backoff() {
                        RealClock.sleep(d).await;
                    }
                    continue;
                }
                backoff.reset();
                s
            }
            Err(e) => {
                tracing::debug!(error = %e, "duplex client connect failed");
                if let Some(d) = backoff.next_backoff() {
                    RealClock.sleep(d).await;
                }
                continue;
            }
        };

        let (reader, writer) = tokio::io::split(stream);
        let mut frame_reader = FrameReader::new(reader, max);
        let mut write_state: WriteState<WriteHalf<Box<dyn super::Stream>>, DuplexBuf, Side> =
            WriteState::Idle(writer);

        // Retransmit unacked outbound messages.
        let mut retransmit_queue: std::collections::VecDeque<(u64, Bytes)> = unacked.clone();

        let mut last_inbound_ack_time = RealClock.now();
        #[allow(unused_assignments)] // initial false IS read on clean-break paths
        let mut conn_broken = false;

        loop {
            let inbound_ack_behind = inbound_next.ack + ack_msg_interval <= inbound_next.seq
                || (inbound_next.ack < inbound_next.seq
                    && last_inbound_ack_time.elapsed() > ack_time_interval);
            let has_retransmit = !retransmit_queue.is_empty();

            tokio::select! {
                biased;

                // Drive in-progress write.
                result = write_state.send() => {
                    match result {
                        Ok(side) => {
                            if side == Side::B {
                                last_inbound_ack_time = RealClock.now();
                                inbound_next.ack = inbound_next.seq;
                            }
                        }
                        Err(_e) => {
                            break;
                        }
                    }
                }

                // Retransmit unacked outbound (Side::A).
                _ = std::future::ready(()), if write_state.is_idle() && has_retransmit => {
                    let Ok(writer) = replace(&mut write_state, WriteState::Broken).into_idle() else {
                        panic!("illegal state");
                    };
                    let (_, data) = retransmit_queue.pop_front().unwrap();
                    match FrameWrite::new(writer, DuplexBuf::Ack(data), max, Side::A as u8) {
                        Ok(fw) => {
                            write_state = WriteState::Writing(fw, Side::A);
                        }
                        Err((_w, _)) => {
                            break;
                        }
                    }
                }

                // Send outbound M1 (Side::A).
                msg = outbound_rx.recv(), if write_state.is_idle() && !has_retransmit => {
                    match msg {
                        Some(message) => {
                            let Ok(writer) = replace(&mut write_state, WriteState::Broken).into_idle() else {
                                panic!("illegal state");
                            };
                            let seq = outbound_next.seq;
                            let msg = match serde_multipart::serialize_bincode(
                                &Frame::Message(seq, message),
                            ) {
                                Ok(f) => f,
                                Err(e) => {
                                    tracing::error!(error = %e, "failed to serialize outbound message");
                                    conn_broken = true;
                                    break;
                                }
                            };
                            // Convert to framed bytes for both sending and replay.
                            let mut framed = msg.framed();
                            let payload = framed.copy_to_bytes(framed.remaining());
                            match FrameWrite::new(writer, DuplexBuf::Ack(payload.clone()), max, Side::A as u8) {
                                Ok(fw) => {
                                    unacked.push_back((seq, payload));
                                    outbound_next.seq += 1;
                                    write_state = WriteState::Writing(fw, Side::A);
                                }
                                Err((w, e)) => {
                                    write_state = WriteState::Idle(w);
                                    tracing::error!(error = %e, "failed to create outbound frame");
                                }
                            }
                        }
                        None => {
                            // Outbound channel dropped — we're done.
                            return;
                        }
                    }
                }

                // Ack inbound (Side::B).
                _ = std::future::ready(()), if write_state.is_idle() && inbound_ack_behind && !has_retransmit => {
                    let Ok(writer) = replace(&mut write_state, WriteState::Broken).into_idle() else {
                        panic!("illegal state");
                    };
                    let ack = match serialize_response(NetRxResponse::Ack(inbound_next.seq - 1)) {
                        Ok(a) => a,
                        Err(_) => {
                            break;
                        }
                    };
                    match FrameWrite::new(writer, DuplexBuf::Ack(ack), max, Side::B as u8) {
                        Ok(fw) => {
                            write_state = WriteState::Writing(fw, Side::B);
                        }
                        Err((w, _)) => {
                            write_state = WriteState::Idle(w);
                        }
                    }
                }

                // Read from the wire.
                frame_result = frame_reader.next() => {
                    match frame_result {
                        Ok(Some((tag, bytes))) => {
                            if tag == Side::A as u8 {
                                // Side::A from server = ack for our outbound M1.
                                match deserialize_response(bytes) {
                                    Ok(NetRxResponse::Ack(acked_seq)) => {
                                        // Prune unacked up to and including acked_seq.
                                        while let Some((seq, _)) = unacked.front() {
                                            if *seq <= acked_seq {
                                                unacked.pop_front();
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                    Ok(NetRxResponse::Reject(reason)) => {
                                        tracing::error!(reason = %reason, "duplex server rejected");
                                        return;
                                    }
                                    Ok(NetRxResponse::Closed) => {
                                        return;
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "failed to deserialize ack");
                                        conn_broken = true;
                                        break;
                                    }
                                }
                            } else if tag == Side::B as u8 {
                                // Side::B from server = inbound M2 message.
                                match serde_multipart::Message::from_framed(bytes) {
                                    Ok(message) => {
                                        match serde_multipart::deserialize_bincode::<Frame<M2>>(message) {
                                            Ok(Frame::Message(seq, msg)) => {
                                                if seq < inbound_next.seq {
                                                    // Retransmit — ignore.
                                                    tracing::debug!(seq, "duplex client: ignoring inbound retransmit");
                                                } else if seq == inbound_next.seq {
                                                    if inbound_tx.send(msg).await.is_err() {
                                                        // Receiver dropped.
                                                        return;
                                                    }
                                                    inbound_next.seq += 1;
                                                } else {
                                                    tracing::error!(
                                                        expected = inbound_next.seq,
                                                        got = seq,
                                                        "out-of-sequence inbound message"
                                                    );
                                                    conn_broken = true;
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!(error = %e, "failed to deserialize inbound message");
                                                conn_broken = true;
                                                break;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "failed to deframe inbound message");
                                        conn_broken = true;
                                        break;
                                    }
                                }
                            } else {
                                tracing::warn!(tag, "duplex client: unknown tag");
                            }
                        }
                        Ok(None) => {
                            // EOF — reconnect.
                            break;
                        }
                        Err(_e) => {
                            break;
                        }
                    }
                }
            }
        }

        if !conn_broken {
            return;
        }

        // Reconnect with backoff.
        if let Some(d) = backoff.next_backoff() {
            RealClock.sleep(d).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use timed_test::async_timed_test;

    use super::*;

    #[async_timed_test(timeout_secs = 30)]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" })
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_duplex_basic() {
        let mut server =
            serve::<u64, String>(ChannelAddr::Tcp("[::1]:0".parse().unwrap())).unwrap();
        let server_addr = server.addr().clone();

        // Client: sends u64, receives String.
        let (client_tx, mut client_rx) = dial::<u64, String>(server_addr).await.unwrap();

        // Server: receives u64, sends String.
        let (mut server_rx, server_tx) = server.accept().await.unwrap();

        // Client sends to server.
        client_tx.send(42).unwrap();
        let received = server_rx.recv().await.unwrap();
        assert_eq!(received, 42);

        // Server sends to client.
        server_tx.send("hello".to_string()).unwrap();
        let received = client_rx.recv().await.unwrap();
        assert_eq!(received, "hello");

        // Multiple messages both ways.
        for i in 0..10u64 {
            client_tx.send(i).unwrap();
            assert_eq!(server_rx.recv().await.unwrap(), i);

            server_tx.send(format!("msg-{}", i)).unwrap();
            assert_eq!(client_rx.recv().await.unwrap(), format!("msg-{}", i));
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_duplex_multiple_links() {
        let mut server = serve::<u64, u64>(ChannelAddr::Tcp("[::1]:0".parse().unwrap())).unwrap();
        let server_addr = server.addr().clone();

        // Two independent clients.
        let (tx1, mut rx1) = dial::<u64, u64>(server_addr.clone()).await.unwrap();
        let (mut srx1, stx1) = server.accept().await.unwrap();

        let (tx2, mut rx2) = dial::<u64, u64>(server_addr).await.unwrap();
        let (mut srx2, stx2) = server.accept().await.unwrap();

        // Send on link 1.
        tx1.send(100).unwrap();
        assert_eq!(srx1.recv().await.unwrap(), 100);
        stx1.send(200).unwrap();
        assert_eq!(rx1.recv().await.unwrap(), 200);

        // Send on link 2.
        tx2.send(300).unwrap();
        assert_eq!(srx2.recv().await.unwrap(), 300);
        stx2.send(400).unwrap();
        assert_eq!(rx2.recv().await.unwrap(), 400);
    }
}
