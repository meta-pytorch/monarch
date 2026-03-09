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
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::LinkId;
use super::ServerError;
use super::session;
use super::session::Next;
use super::session::SessionConnector;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::Rx;
use crate::channel::SendError;
use crate::channel::Tx;
use crate::channel::TxStatus;
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

/// Public duplex server that yields `(DuplexRx<M1>, DuplexTx<M2>)` pairs.
pub struct DuplexServer<M1: RemoteMessage, M2: RemoteMessage> {
    accept_rx: mpsc::Receiver<(DuplexRx<M1>, DuplexTx<M2>)>,
    _handle: ServerHandle,
    addr: ChannelAddr,
}

impl<M1: RemoteMessage, M2: RemoteMessage> DuplexServer<M1, M2> {
    /// Accept a new duplex link, returning `(rx, tx)` handles.
    pub async fn accept(&mut self) -> Result<(DuplexRx<M1>, DuplexTx<M2>), ChannelError> {
        self.accept_rx.recv().await.ok_or(ChannelError::Closed)
    }

    /// The address this server is listening on.
    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }
}

/// Receiver half of a duplex channel.
pub struct DuplexRx<M: RemoteMessage>(mpsc::Receiver<M>, ChannelAddr);

#[async_trait]
impl<M: RemoteMessage> Rx<M> for DuplexRx<M> {
    async fn recv(&mut self) -> Result<M, ChannelError> {
        self.0.recv().await.ok_or(ChannelError::Closed)
    }

    fn addr(&self) -> ChannelAddr {
        self.1.clone()
    }

    async fn join(self) {}
}

/// Sender half of a duplex channel.
pub struct DuplexTx<M: RemoteMessage> {
    tx: mpsc::UnboundedSender<(M, oneshot::Sender<SendError<M>>, Instant)>,
    addr: ChannelAddr,
    status: watch::Receiver<TxStatus>,
}

#[async_trait]
impl<M: RemoteMessage> Tx<M> for DuplexTx<M> {
    fn do_post(&self, message: M, return_channel: Option<oneshot::Sender<SendError<M>>>) {
        let return_channel = return_channel.unwrap_or_else(|| oneshot::channel().0);
        if let Err(mpsc::error::SendError((message, return_channel, _))) =
            self.tx.send((message, return_channel, RealClock.now()))
        {
            let _ = return_channel.send(SendError {
                error: ChannelError::Closed,
                message,
                reason: None,
            });
        }
    }

    fn addr(&self) -> ChannelAddr {
        self.addr.clone()
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        &self.status
    }
}

impl<M: RemoteMessage> Clone for DuplexTx<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            addr: self.addr.clone(),
            status: self.status.clone(),
        }
    }
}

/// Start a duplex server on the given address.
pub fn serve<M1: RemoteMessage, M2: RemoteMessage>(
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
    accept_tx: mpsc::Sender<(DuplexRx<M1>, DuplexTx<M2>)>,
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
                                        let (_, status) = watch::channel(TxStatus::Active);
                                        let net_rx = DuplexRx(inbound_rx, addr.clone());
                                        let net_tx = DuplexTx {
                                            tx: outbound_tx,
                                            addr: addr.clone(),
                                            status,
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
/// from the link's MVar, runs recv_connected and send_connected concurrently,
/// and puts state back on close.
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
    let mux = session::Mux::new(reader, writer, max);
    let stream_a = mux.stream(Side::A as u8);
    let stream_b = mux.stream(Side::B as u8);

    let (mut inbound_next, outbound_next) = link.next.take().await;
    let mut outbound_rx = link.outbound_rx.take().await;

    let ct = cancel_token.child_token();

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
        r = session::recv_connected::<M1, _, _>(
            &stream_a, &link.inbound_tx, &mut inbound_next, ct.clone(),
        ) => r.map(|_| ()),
        r = session::send_connected(
            &stream_b, &mut deliveries, &mut outbound_rx, ct.clone(),
        ) => r.map(|_| ()),
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
    mux: session::Mux<
        tokio::io::ReadHalf<Box<dyn super::Stream>>,
        tokio::io::WriteHalf<Box<dyn super::Stream>>,
    >,
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
            mux: session::Mux::new(r, w, max),
        })
    }

    async fn run_connected(
        &mut self,
        connected: &DuplexConnection,
        deliveries: &mut session::Deliveries<M1>,
        receiver: &mut mpsc::UnboundedReceiver<(M1, oneshot::Sender<SendError<M1>>, Instant)>,
        cancel: CancellationToken,
    ) -> Result<session::SendLoopStatus, anyhow::Error> {
        let stream_a = connected.mux.stream(Side::A as u8);
        let stream_b = connected.mux.stream(Side::B as u8);
        tokio::select! {
            r = session::send_connected(
                &stream_a, deliveries, receiver, cancel.clone(),
            ) => r,
            r = session::recv_connected::<M2, _, _>(
                &stream_b, &self.inbound_tx, &mut self.inbound_next, cancel,
            ) => match r {
                Ok(status) => Ok(match status {
                    session::RecvResult::Eof => session::SendLoopStatus::Eof,
                    session::RecvResult::Cancelled => session::SendLoopStatus::Cancelled,
                    session::RecvResult::SequenceError(e) => session::SendLoopStatus::Rejected(e),
                }),
                Err(e) => Err(e),
            },
        }
    }

    async fn shutdown(connected: DuplexConnection) {
        connected.mux.shutdown().await;
    }
}

/// Connect to a duplex server, returning tx and rx handles.
pub async fn dial<M1: RemoteMessage, M2: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(DuplexTx<M1>, DuplexRx<M2>), super::ClientError> {
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

    let (_, status) = watch::channel(TxStatus::Active);
    Ok((
        DuplexTx {
            tx: outbound_tx,
            addr: addr.clone(),
            status,
        },
        DuplexRx(inbound_rx, addr),
    ))
}

#[cfg(test)]
mod tests {
    use timed_test::async_timed_test;

    use super::*;
    use crate::channel::ChannelTransport;

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
        client_tx.post(42);
        let received = server_rx.recv().await.unwrap();
        assert_eq!(received, 42);

        // Server sends to client.
        server_tx.post("hello".to_string());
        let received = client_rx.recv().await.unwrap();
        assert_eq!(received, "hello");

        // Multiple messages both ways.
        for i in 0..10u64 {
            client_tx.post(i);
            assert_eq!(server_rx.recv().await.unwrap(), i);

            server_tx.post(format!("msg-{}", i));
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
        tx1.post(100);
        assert_eq!(srx1.recv().await.unwrap(), 100);
        stx1.post(200);
        assert_eq!(rx1.recv().await.unwrap(), 200);

        // Send on link 2.
        tx2.post(300);
        assert_eq!(srx2.recv().await.unwrap(), 300);
        stx2.post(400);
        assert_eq!(rx2.recv().await.unwrap(), 400);
    }

    /// Ping-pong helper: server echoes back each message it receives.
    /// Returns elapsed time for `iterations` round-trips.
    async fn duplex_ping_pong(
        addr: ChannelAddr,
        iterations: usize,
    ) -> anyhow::Result<std::time::Duration> {
        let mut server = serve::<u64, u64>(addr)?;
        let server_addr = server.addr().clone();

        let server_handle = tokio::spawn(async move {
            let (mut rx, tx) = server.accept().await.unwrap();
            while let Ok(msg) = rx.recv().await {
                tx.post(msg);
            }
        });

        let (client_tx, mut client_rx) = dial::<u64, u64>(server_addr).await?;

        // Warmup.
        for i in 0..10u64 {
            client_tx.post(i);
            assert_eq!(client_rx.recv().await?, i);
        }

        let start = std::time::Instant::now();
        for i in 0..iterations as u64 {
            client_tx.post(i);
            assert_eq!(client_rx.recv().await?, i);
        }
        let elapsed = start.elapsed();

        server_handle.abort();
        Ok(elapsed)
    }

    #[async_timed_test(timeout_secs = 30)]
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_duplex_ping_pong_tcp() {
        let elapsed = duplex_ping_pong(ChannelAddr::Tcp("[::1]:0".parse().unwrap()), 100)
            .await
            .unwrap();
        println!("TCP duplex: 100 round-trips in {elapsed:?}");
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_duplex_ping_pong_unix() {
        let elapsed = duplex_ping_pong(ChannelAddr::any(ChannelTransport::Unix), 100)
            .await
            .unwrap();
        println!("Unix duplex: 100 round-trips in {elapsed:?}");
    }
}
