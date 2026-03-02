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
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::LinkId;
use super::ServerError;
use super::framed::FrameReader;
use super::session;
use super::session::Next;
use super::session::SessionConnector;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::SendError;
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

use super::server::ServerHandle;

/// Per-link server state persisting across reconnections.
struct DuplexServerLink<M1: RemoteMessage, M2: RemoteMessage> {
    id: LinkId,
    /// (inbound_next, outbound_next) — taken/put atomically with MVar.
    next: MVar<(Next, Next)>,
    /// Delivers inbound M1 messages to the link's Rx.
    inbound_tx: mpsc::Sender<M1>,
    /// Taken by the connection handler, put back on disconnect.
    outbound_rx: MVar<mpsc::UnboundedReceiver<(M2, oneshot::Sender<SendError<M2>>, Instant)>>,
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
    tx: mpsc::UnboundedSender<(M, oneshot::Sender<SendError<M>>, Instant)>,
    addr: ChannelAddr,
}

impl<M: RemoteMessage> DuplexNetTx<M> {
    pub fn send(&self, message: M) -> Result<(), ChannelError> {
        let (return_tx, _) = oneshot::channel();
        self.tx
            .send((message, return_tx, RealClock.now()))
            .map_err(|_| ChannelError::Closed)
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
                                        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<(M2, oneshot::Sender<SendError<M2>>, Instant)>();
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

                            handle_duplex_connection(stream, link, ct, addr).await
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
/// from the link's MVar, runs recv_loop and send_loop concurrently, and
/// puts state back on close.
async fn handle_duplex_connection<M1: RemoteMessage, M2: RemoteMessage, S>(
    stream: S,
    link: Arc<DuplexServerLink<M1, M2>>,
    cancel_token: CancellationToken,
    addr: ChannelAddr,
) -> Result<(), anyhow::Error>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
    let demux = session::DemuxFrameReader::new(FrameReader::new(reader, max));
    let mux = session::MuxWriter::new(writer, max);

    let (mut inbound_next, outbound_next) = link.next.take().await;
    let mut outbound_rx = link.outbound_rx.take().await;

    let ct = cancel_token.child_token();
    let side_a = demux.side(Side::A as u8);
    let side_b = demux.side(Side::B as u8);

    let session_id = link.id.0;
    let log_id = format!("duplex server {:016x}", session_id);
    let mut deliveries = session::Deliveries {
        outbox: session::Outbox::new(log_id.clone(), addr.clone(), session_id),
        unacked: session::Unacked::new(None, log_id),
    };
    deliveries.outbox.next_seq = outbound_next.seq;

    // Run both directions concurrently. When either finishes, the other
    // is dropped (the physical stream is broken once one direction fails).
    let result: Result<(), anyhow::Error> = tokio::select! {
        r = session::recv_loop::<M1, _>(
            &side_a, &mux, Side::A as u8,
            &link.inbound_tx, &mut inbound_next, ct.clone(),
        ) => r.map(|_| ()),
        r = session::send_loop(
            &side_b, &mux, Side::B as u8,
            &mut deliveries, &mut outbound_rx, ct.clone(),
        ) => match r {
            session::SendLoopResult::Error(e) => Err(e),
            _ => Ok(()),
        },
    };

    let new_outbound_next = Next {
        seq: deliveries.outbox.next_seq,
        ack: deliveries
            .unacked
            .largest_acked
            .as_ref()
            .map_or(outbound_next.ack, |a| a.0 + 1),
    };
    link.next.put((inbound_next, new_outbound_next)).await;
    link.outbound_rx.put(outbound_rx).await;
    result
}

struct DuplexConnection {
    reader: session::DemuxFrameReader<tokio::io::ReadHalf<Box<dyn super::Stream>>>,
    writer: session::MuxWriter<tokio::io::WriteHalf<Box<dyn super::Stream>>>,
}

struct DuplexConnector<M2: RemoteMessage> {
    addr: ChannelAddr,
    link_id: LinkId,
    inbound_tx: mpsc::Sender<M2>,
    inbound_next: Next,
}

#[async_trait]
impl<M1: RemoteMessage, M2: RemoteMessage> SessionConnector<M1> for DuplexConnector<M2> {
    type Connected = DuplexConnection;

    fn dest(&self) -> ChannelAddr {
        self.addr.clone()
    }

    fn session_id(&self) -> u64 {
        self.link_id.0
    }

    fn on_demand(&self) -> bool {
        false
    }

    async fn connect(&mut self) -> Result<DuplexConnection, super::ClientError> {
        let mut s = super::connect_raw(&self.addr).await?;
        write_duplex_link_init(&mut s, self.link_id)
            .await
            .map_err(|e| {
                super::ClientError::Connect(self.addr.clone(), e, "DuplexLinkInit".into())
            })?;
        let (r, w) = tokio::io::split(s);
        let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
        Ok(DuplexConnection {
            reader: session::DemuxFrameReader::new(FrameReader::new(r, max)),
            writer: session::MuxWriter::new(w, max),
        })
    }

    async fn run_connected(
        &mut self,
        connected: &DuplexConnection,
        deliveries: &mut session::Deliveries<M1>,
        receiver: &mut mpsc::UnboundedReceiver<(M1, oneshot::Sender<SendError<M1>>, Instant)>,
        cancel: CancellationToken,
    ) -> session::SendLoopResult {
        let side_a = connected.reader.side(Side::A as u8);
        let side_b = connected.reader.side(Side::B as u8);
        tokio::select! {
            r = session::send_loop(
                &side_a, &connected.writer, Side::A as u8,
                deliveries, receiver, cancel.clone(),
            ) => r,
            r = session::recv_loop::<M2, _>(
                &side_b, &connected.writer, Side::B as u8,
                &self.inbound_tx, &mut self.inbound_next, cancel,
            ) => match r {
                Ok(outcome) => match outcome {
                    session::RecvResult::Eof => session::SendLoopResult::Eof,
                    session::RecvResult::Cancelled => session::SendLoopResult::Cancelled,
                    session::RecvResult::SequenceError(e) => session::SendLoopResult::Rejected(e),
                },
                Err(e) => session::SendLoopResult::Error(e),
            },
        }
    }

    async fn shutdown(connected: DuplexConnection) {
        let inner = connected.writer.into_inner();
        let mut w = inner.lock().await;
        let _ = w.shutdown().await;
    }
}

/// Connect to a duplex server, returning tx and rx handles.
pub(crate) async fn dial<M1: RemoteMessage, M2: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(DuplexNetTx<M1>, DuplexNetRx<M2>), super::ClientError> {
    let link_id = LinkId::random();

    let (outbound_tx, outbound_rx) =
        mpsc::unbounded_channel::<(M1, oneshot::Sender<SendError<M1>>, Instant)>();
    let (inbound_tx, inbound_rx) = mpsc::channel::<M2>(1024);

    let connector = DuplexConnector::<M2> {
        addr: addr.clone(),
        link_id,
        inbound_tx,
        inbound_next: Next { seq: 0, ack: 0 },
    };

    tokio::spawn(async move {
        session::client_run(connector, outbound_rx, None).await;
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
