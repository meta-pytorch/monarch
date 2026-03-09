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
//! Each connection starts with a unified `LinkInit` header (13 bytes,
//! unframed) containing only the `session_id`:
//!
//! ```text
//! [magic: 4B "LNK\0"] [session_id: 8B u64 BE]
//! ```
//!
//! After the init, the standard tagged frame format is used. The tag
//! byte in the 8-byte header distinguishes logical channels:
//!
//! - `INITIATOR_TO_ACCEPTOR = 0x00`
//! - `ACCEPTOR_TO_INITIATOR = 0x01`

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::ClientError;
use super::Link;
use super::ServerError;
use super::SessionId;
use super::log_send_error;
use super::read_link_init;
use super::server::AcceptorLink;
use super::server::ServerHandle;
use super::session;
use super::session::Next;
use super::session::Session;
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
use crate::metrics;
use crate::sync::mvar::MVar;

/// Public duplex server that yields `(DuplexRx<In>, DuplexTx<Out>)` pairs.
pub struct DuplexServer<In: RemoteMessage, Out: RemoteMessage> {
    accept_rx: mpsc::Receiver<(DuplexRx<In>, DuplexTx<Out>)>,
    _handle: ServerHandle,
    addr: ChannelAddr,
}

impl<In: RemoteMessage, Out: RemoteMessage> DuplexServer<In, Out> {
    /// Accept a new duplex link, returning `(rx, tx)` handles.
    pub async fn accept(&mut self) -> Result<(DuplexRx<In>, DuplexTx<Out>), ChannelError> {
        self.accept_rx.recv().await.ok_or(ChannelError::Closed)
    }

    /// The address this server is listening on.
    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }
}

/// Receiver half of a duplex channel.
pub struct DuplexRx<M: RemoteMessage>(mpsc::Receiver<M>, ChannelAddr);

impl<M: RemoteMessage> DuplexRx<M> {
    pub(super) fn new(rx: mpsc::Receiver<M>, addr: ChannelAddr) -> Self {
        Self(rx, addr)
    }
}

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

impl<M: RemoteMessage> DuplexTx<M> {
    pub(super) fn new(
        tx: mpsc::UnboundedSender<(M, oneshot::Sender<SendError<M>>, Instant)>,
        addr: ChannelAddr,
        status: watch::Receiver<TxStatus>,
    ) -> Self {
        Self { tx, addr, status }
    }
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
pub fn serve<In: RemoteMessage, Out: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<DuplexServer<In, Out>, ServerError> {
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
fn serve_with_listener<In: RemoteMessage, Out: RemoteMessage, L: super::Listener>(
    listener: L,
    channel_addr: ChannelAddr,
) -> Result<DuplexServer<In, Out>, ServerError> {
    let (accept_tx, accept_rx) = mpsc::channel(16);

    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();
    let ca = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        duplex_listen::<In, Out, L>(listener, ca, accept_tx, child_token).await
    });

    let server_handle = ServerHandle::new(join_handle, cancel_token, channel_addr.clone());

    Ok(DuplexServer {
        accept_rx,
        _handle: server_handle,
        addr: channel_addr,
    })
}

/// Helper to distinguish send errors from recv errors in duplex select.
enum Either {
    Send(session::SendLoopError),
    Recv(session::RecvLoopError),
}

/// Dispatch a stream to the appropriate duplex session, creating one
/// if this is the first connection for the given session ID.
async fn dispatch_duplex_stream<In: RemoteMessage, Out: RemoteMessage, S: super::Stream>(
    session_id: SessionId,
    stream: S,
    sessions: &DashMap<SessionId, MVar<S>>,
    addr: ChannelAddr,
    accept_tx: &mpsc::Sender<(DuplexRx<In>, DuplexTx<Out>)>,
    cancel: CancellationToken,
) {
    let mvar = {
        let entry = sessions.entry(session_id);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => e.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let mvar: MVar<S> = MVar::empty();
                let link = AcceptorLink {
                    dest: addr.clone(),
                    session_id,
                    stream: mvar.clone(),
                    cancel: cancel.clone(),
                };

                let (inbound_tx, inbound_rx) = mpsc::channel::<In>(1024);
                let (outbound_tx, outbound_rx) =
                    mpsc::unbounded_channel::<(Out, oneshot::Sender<SendError<Out>>, Instant)>();
                let (notify, status) = watch::channel(TxStatus::Active);
                let net_rx = DuplexRx(inbound_rx, addr.clone());
                let net_tx = DuplexTx {
                    tx: outbound_tx,
                    addr: addr.clone(),
                    status,
                };
                let _ = accept_tx.send((net_rx, net_tx)).await;

                let session_ct = cancel.clone();
                let dest = addr.clone();
                tokio::spawn(async move {
                    let mut session = Session::new(link);
                    let mut recv_next = Next { seq: 0, ack: 0 };
                    let log_id = format!("duplex server {:016x}", session_id.0);
                    let mut deliveries = session::Deliveries {
                        outbox: session::Outbox::new(log_id.clone(), dest, session_id.0),
                        unacked: session::Unacked::new(None, log_id),
                    };
                    let mut outbound_rx = outbound_rx;

                    loop {
                        let connected = match session.connect().await {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        deliveries.requeue_unacked();
                        let result = {
                            let recv_stream = connected.stream(super::INITIATOR_TO_ACCEPTOR);
                            let send_stream = connected.stream(super::ACCEPTOR_TO_INITIATOR);
                            tokio::select! {
                                r = session::recv_connected::<In, _, _>(
                                    &recv_stream,
                                    &inbound_tx,
                                    &mut recv_next,
                                ) => r.map_err(Either::Recv),
                                r = session::send_connected(
                                    &send_stream,
                                    &mut deliveries,
                                    &mut outbound_rx,
                                ) => r.map_err(Either::Send),
                                _ = session_ct.cancelled() => Err(Either::Recv(session::RecvLoopError::Cancelled)),
                            }
                        };

                        let terminal = match &result {
                            Ok(()) => false,
                            Err(Either::Send(session::SendLoopError::Io(err))) => {
                                tracing::info!(
                                    session_id = session_id.0,
                                    error = %err,
                                    "duplex send/recv error",
                                );
                                false
                            }
                            Err(Either::Recv(session::RecvLoopError::Io(_))) => false,
                            _ => true,
                        };
                        session = connected.release();
                        if terminal {
                            break;
                        }
                    }

                    let _ = notify.send(TxStatus::Closed);
                });

                e.insert(mvar.clone());
                mvar
            }
        }
    };

    mvar.put(stream).await;
}

/// Main listen loop for duplex connections. Uses AcceptorLink +
/// Session with acquire/release per session.
async fn duplex_listen<In: RemoteMessage, Out: RemoteMessage, L: super::Listener>(
    mut listener: L,
    listener_addr: ChannelAddr,
    accept_tx: mpsc::Sender<(DuplexRx<In>, DuplexTx<Out>)>,
    cancel_token: CancellationToken,
) -> Result<(), ServerError> {
    let child_cancel_token = CancellationToken::new();
    let sessions: Arc<DashMap<SessionId, MVar<L::Stream>>> = Arc::new(DashMap::new());
    let mut connections: JoinSet<Result<(), anyhow::Error>> = JoinSet::new();

    let result: Result<(), ServerError> = loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((mut stream, _peer_addr)) => {
                        let sessions = Arc::clone(&sessions);
                        let accept_tx = accept_tx.clone();
                        let ct = child_cancel_token.child_token();
                        let addr = listener_addr.clone();

                        connections.spawn(async move {
                            let session_id = read_link_init(&mut stream).await
                                .map_err(|e| anyhow::anyhow!("LinkInit read failed: {e}"))?;

                            dispatch_duplex_stream::<In, Out, _>(
                                session_id, stream, &sessions, addr, &accept_tx, ct,
                            )
                            .await;
                            Ok(())
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

            result = session::join_nonempty(&mut connections) => {
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

/// Establish a duplex (bidirectional) session over the given link.
/// Returns send and receive handles.
pub(crate) fn spawn<Out: RemoteMessage, In: RemoteMessage>(
    link: impl Link,
) -> (DuplexTx<Out>, DuplexRx<In>) {
    let addr = link.dest();
    let session_id = link.link_id();
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel::<In>(1024);
    let (notify, status) = watch::channel(TxStatus::Active);
    let dest = addr.clone();
    crate::init::get_runtime().spawn(async move {
        let mut session = Session::new(link);
        let log_id = format!("session {}.{:016x}", dest, session_id.0);
        let mut deliveries = session::Deliveries {
            outbox: session::Outbox::new(log_id.clone(), dest.clone(), session_id.0),
            unacked: session::Unacked::new(None, log_id),
        };
        let mut outbound_rx = outbound_rx;
        let mut recv_next = Next { seq: 0, ack: 0 };

        loop {
            let connected = match session.connect().await {
                Ok(s) => s,
                Err(_) => break,
            };

            metrics::CHANNEL_CONNECTIONS.add(
                1,
                hyperactor_telemetry::kv_pairs!(
                    "transport" => dest.transport().to_string(),
                    "mode" => "duplex",
                    "reason" => "link connected",
                ),
            );

            if !deliveries.unacked.is_empty() {
                metrics::CHANNEL_RECONNECTIONS.add(
                    1,
                    hyperactor_telemetry::kv_pairs!(
                        "dest" => dest.to_string(),
                        "transport" => dest.transport().to_string(),
                        "mode" => "duplex",
                        "reason" => "reconnect_with_unacked",
                    ),
                );
            }
            deliveries.requeue_unacked();
            let result = {
                let send_stream = connected.stream(super::INITIATOR_TO_ACCEPTOR);
                let recv_stream = connected.stream(super::ACCEPTOR_TO_INITIATOR);
                tokio::select! {
                    r = session::send_connected(
                        &send_stream, &mut deliveries, &mut outbound_rx,
                    ) => r.map_err(Either::Send),
                    r = session::recv_connected::<In, _, _>(
                        &recv_stream, &inbound_tx, &mut recv_next,
                    ) => r.map_err(Either::Recv),
                }
            };

            let terminal = match &result {
                Ok(()) => false, // EOF — reconnect
                Err(Either::Send(e)) => log_send_error(e, &dest, session_id.0, "duplex"),
                Err(Either::Recv(session::RecvLoopError::Io(err))) => {
                    tracing::info!(
                        dest = %dest,
                        session_id = session_id.0,
                        error = %err,
                        mode = "duplex",
                        "recv error",
                    );
                    metrics::CHANNEL_ERRORS.add(
                        1,
                        hyperactor_telemetry::kv_pairs!(
                            "dest" => dest.to_string(),
                            "session_id" => session_id.0.to_string(),
                            "error_type" => metrics::ChannelErrorType::SendError.as_str(),
                            "mode" => "duplex",
                        ),
                    );
                    false
                }
                _ => true, // terminal
            };
            session = connected.release();
            if terminal {
                break;
            }
        }

        let _ = notify.send(TxStatus::Closed);
    });
    (
        DuplexTx::new(outbound_tx, addr.clone(), status),
        DuplexRx::new(inbound_rx, addr),
    )
}

/// Connect to a duplex server, returning tx and rx handles.
pub fn dial<Out: RemoteMessage, In: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(DuplexTx<Out>, DuplexRx<In>), ClientError> {
    Ok(spawn(super::link(addr)?))
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
        let (client_tx, mut client_rx) = dial::<u64, String>(server_addr).unwrap();

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
        let (tx1, mut rx1) = dial::<u64, u64>(server_addr.clone()).unwrap();
        let (mut srx1, stx1) = server.accept().await.unwrap();

        let (tx2, mut rx2) = dial::<u64, u64>(server_addr).unwrap();
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

        let (client_tx, mut client_rx) = dial::<u64, u64>(server_addr).unwrap();

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
