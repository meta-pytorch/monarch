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

use std::sync::Arc;

use async_trait::async_trait;
use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::ClientError;
use super::Link;
use super::LinkStatus;
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
use crate::channel::ChannelTransport;
use crate::channel::CloseReason;
use crate::channel::Rx;
use crate::channel::SendError;
use crate::channel::SendErrorReason;
use crate::channel::Tx;
use crate::channel::TxStatus;
use crate::channel::net::Stream;
use crate::channel::net::meta;
use crate::channel::net::tls;
use crate::metrics;

/// Public duplex server that yields `(DuplexRx<In>, DuplexTx<Out>)` pairs.
pub struct DuplexServer<In: RemoteMessage, Out: RemoteMessage> {
    accept_rx: mpsc::Receiver<(DuplexRx<In>, DuplexTx<Out>)>,
    handle: ServerHandle,
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

    /// Gracefully shut down the duplex server. Cancels the listener
    /// and awaits its task; structured concurrency in
    /// [`dispatch_duplex_stream`] guarantees every in-flight session
    /// has finished its terminal cleanup (final ack flush + `Closed`
    /// emit) before this returns.
    pub async fn join(mut self) {
        self.handle.stop(&format!(
            "DuplexServer joined; channel address: {}",
            self.addr
        ));
        let _ = (&mut self.handle).await;
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

/// A handle to a duplex client session: wraps the send/recv halves
/// and the spawned task driving the connection. Owns a cancellation
/// token so callers can deterministically stop the recv/send loop
/// via [`DuplexClient::join`].
///
/// Dropping a `DuplexClient` does *not* cancel — that would tear
/// down sessions whose tx/rx halves the application has handed off
/// elsewhere (e.g., into a mailbox). Call [`join`](Self::join) for
/// orderly shutdown.
pub struct DuplexClient<Out: RemoteMessage, In: RemoteMessage> {
    tx: DuplexTx<Out>,
    rx: Option<DuplexRx<In>>,
    join_handle: tokio::task::JoinHandle<()>,
    cancel_token: CancellationToken,
    addr: ChannelAddr,
}

impl<Out: RemoteMessage, In: RemoteMessage> DuplexClient<Out, In> {
    /// Get a new clone of the [`DuplexTx`] for sending messages to
    /// the peer.
    pub fn tx(&self) -> DuplexTx<Out> {
        self.tx.clone()
    }

    /// Take the [`DuplexRx`] out of the client. Returns `None` on
    /// subsequent calls — the receiver is single-consumer.
    pub fn take_rx(&mut self) -> Option<DuplexRx<In>> {
        self.rx.take()
    }

    /// The peer address this client dialed.
    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }

    /// Gracefully shut down the duplex client session. Cancels the
    /// recv/send loop's cancellation token (which the spawned task
    /// observes in its `select!`s) and awaits the spawned task. On
    /// return, the task has finished its terminal cleanup (final
    /// ack flush on `ACCEPTOR_TO_INITIATOR`) and dropped its
    /// [`inbound_tx`](super::session) / outbound receiver halves —
    /// so any in-progress [`DuplexRx::recv`](super::Rx::recv) on
    /// the receiver half resolves with [`ChannelError::Closed`].
    pub async fn join(self) {
        self.cancel_token.cancel();
        let _ = self.join_handle.await;
    }
}

impl<Out: RemoteMessage, In: RemoteMessage> std::fmt::Debug for DuplexClient<Out, In> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuplexClient")
            .field("addr", &self.addr)
            .field("rx_taken", &self.rx.is_none())
            .finish()
    }
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
            self.tx
                .send((message, return_channel, tokio::time::Instant::now()))
        {
            let reason = self
                .status
                .borrow()
                .as_closed()
                .map(|r| SendErrorReason::Other(r.to_string()));
            let _ = return_channel.send(SendError {
                error: ChannelError::Closed,
                message,
                reason,
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

impl<M: RemoteMessage> std::fmt::Debug for DuplexTx<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuplexTx")
            .field("addr", &self.addr)
            .finish()
    }
}

/// Start a duplex server on the given address.
pub fn serve<In: RemoteMessage, Out: RemoteMessage>(
    addr: ChannelAddr,
    listener: Option<std::net::TcpListener>,
) -> Result<DuplexServer<In, Out>, ServerError> {
    let (mut listener, channel_addr) = super::listen_with_prebound(addr, listener)?;

    let (accept_tx, accept_rx) = mpsc::channel(16);
    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    let is_tls = matches!(
        channel_addr.transport(),
        ChannelTransport::Tls | ChannelTransport::MetaTls(_)
    );
    let dest = channel_addr.clone();
    let prepare = move |stream: Box<dyn Stream>, source: ChannelAddr| {
        let dest = dest.clone();
        async move {
            if is_tls {
                let tls_acceptor = match dest.transport() {
                    ChannelTransport::Tls => tls::tls_acceptor()?,
                    _ => meta::tls_acceptor(true)?,
                };
                let mut tls_stream = tls_acceptor.accept(stream).await?;
                let link_init = read_link_init(&mut tls_stream)
                    .await
                    .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
                Ok((link_init, Box::new(tls_stream) as Box<dyn Stream>))
            } else {
                let mut stream = stream;
                let link_init = read_link_init(&mut stream)
                    .await
                    .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
                Ok((link_init, stream))
            }
        }
    };

    let sessions: Arc<DashMap<SessionId, mpsc::UnboundedSender<Box<dyn Stream>>>> =
        Arc::new(DashMap::new());
    let dispatch_dest = channel_addr.clone();
    let dispatch_cancel = child_token.clone();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let accept_tx = accept_tx.clone();
        let dest = dispatch_dest;
        move |link_init: super::LinkInit, stream: Box<dyn Stream>| {
            let sessions = Arc::clone(&sessions);
            let accept_tx = accept_tx.clone();
            let cancel = dispatch_cancel.clone();
            let dest = dest.clone();
            async move {
                dispatch_duplex_stream::<In, Out>(
                    link_init.session_id,
                    stream,
                    sessions,
                    dest,
                    &accept_tx,
                    cancel,
                )
                .await;
            }
        }
    };

    let ca = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        super::server::accept_loop(&mut listener, &ca, &child_token, prepare, dispatch).await
    });

    let server_handle = ServerHandle::new(join_handle, cancel_token, channel_addr.clone());

    Ok(DuplexServer {
        accept_rx,
        handle: server_handle,
        addr: channel_addr,
    })
}

/// Test-only variant that accepts an arbitrary [`super::Listener`].
/// Mirrors [`super::server::serve_with_listener`] but for duplex
/// servers; lets wire-level tests stage `DuplexStream`s via a
/// custom listener so they can inspect terminal-flush behavior on
/// the read side.
#[cfg(test)]
pub(super) fn serve_with_listener<In, Out, L>(
    mut listener: L,
    channel_addr: ChannelAddr,
) -> Result<DuplexServer<In, Out>, ServerError>
where
    In: RemoteMessage,
    Out: RemoteMessage,
    L: super::Listener + 'static,
    L::Stream: Unpin + std::fmt::Debug + 'static,
{
    let (accept_tx, accept_rx) = mpsc::channel(16);
    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    let prepare = |stream: L::Stream, source: ChannelAddr| async move {
        let mut boxed: Box<dyn Stream> = Box::new(stream);
        let link_init = read_link_init(&mut boxed)
            .await
            .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
        Ok((link_init, boxed))
    };

    let sessions: Arc<DashMap<SessionId, mpsc::UnboundedSender<Box<dyn Stream>>>> =
        Arc::new(DashMap::new());
    let dispatch_cancel = child_token.clone();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let accept_tx = accept_tx.clone();
        let dest = channel_addr.clone();
        move |link_init: super::LinkInit, stream: Box<dyn Stream>| {
            let sessions = Arc::clone(&sessions);
            let accept_tx = accept_tx.clone();
            let cancel = dispatch_cancel.clone();
            let dest = dest.clone();
            async move {
                dispatch_duplex_stream::<In, Out>(
                    link_init.session_id,
                    stream,
                    sessions,
                    dest,
                    &accept_tx,
                    cancel,
                )
                .await;
            }
        }
    };

    let ca = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        super::server::accept_loop(&mut listener, &ca, &child_token, prepare, dispatch).await
    });

    let server_handle = ServerHandle::new(join_handle, cancel_token, channel_addr.clone());

    Ok(DuplexServer {
        accept_rx,
        handle: server_handle,
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
///
/// Structured concurrency: the first dispatch for a session runs the
/// recv/send loop inline and only returns after its terminal cleanup
/// (flush any pending recv ack, emit `Closed` on cancellation).
/// Reconnects hand the connection off via the per-session channel
/// and return immediately. [`accept_loop`](super::server::accept_loop)
/// joins every dispatch in its `connections` `JoinSet`, so it
/// finishes only after every recv/send loop has finished — same
/// contract as the simplex [`dispatch_stream`](super::server::dispatch_stream).
async fn dispatch_duplex_stream<In: RemoteMessage, Out: RemoteMessage>(
    session_id: SessionId,
    stream: Box<dyn Stream>,
    sessions: Arc<DashMap<SessionId, mpsc::UnboundedSender<Box<dyn Stream>>>>,
    addr: ChannelAddr,
    accept_tx: &mpsc::Sender<(DuplexRx<In>, DuplexTx<Out>)>,
    cancel: CancellationToken,
) {
    // Insert into the session map and drop the DashMap shard guard
    // before any await. Vacant inserts the sender side; occupied
    // returns the existing sender so this dispatch acts as a feeder.
    // Unbounded so this task can publish the sender before draining
    // the first conn without deadlocking a concurrent feeder for
    // the same session_id.
    let entry_result = {
        let entry = sessions.entry(session_id);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => Err(e.get().clone()),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let (sender, receiver) = mpsc::unbounded_channel::<Box<dyn Stream>>();
                e.insert(sender.clone());
                Ok((sender, receiver))
            }
        }
    };

    let (sender, receiver) = match entry_result {
        Err(sender) => {
            // Feeder: forward the conn through the existing channel.
            // Send returns Err if the processor task exited and
            // dropped the receiver — drop the conn in that case.
            let _ = sender.send(stream);
            return;
        }
        Ok(pair) => pair,
    };

    // First dispatch for this session_id: set up the duplex
    // application-facing handles and yield them to the server.
    let (inbound_tx, inbound_rx) = mpsc::channel::<In>(1024);
    let (outbound_tx, mut outbound_rx) =
        mpsc::unbounded_channel::<(Out, oneshot::Sender<SendError<Out>>, Instant)>();
    let (notify, status) = watch::channel(TxStatus::Active);
    let net_rx = DuplexRx(inbound_rx, addr.clone());
    let net_tx = DuplexTx {
        tx: outbound_tx,
        addr: addr.clone(),
        status,
    };
    let _ = accept_tx.send((net_rx, net_tx)).await;

    // Hand the first connection off through the channel; the loop
    // below picks it up via `session.connect().await`.
    let _ = sender.send(stream);
    drop(sender);

    let link = AcceptorLink {
        dest: addr.clone(),
        session_id,
        stream: receiver,
        cancel: cancel.clone(),
    };
    let session_ct = cancel;
    let dest = addr;
    let log_id = format!("duplex server {:016x}", session_id.0);
    let mut deliveries = session::Deliveries {
        outbox: session::Outbox::new(log_id.clone(), dest.clone(), session_id.0),
        unacked: session::Unacked::new(None, log_id),
    };
    let mut session = Session::new(link);
    let mut recv_next = Next { seq: 0, ack: 0 };

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
            Ok(()) => {
                tracing::info!(
                    session_id = session_id.0,
                    "duplex recv_connected returned EOF, awaiting reconnect"
                );
                false
            }
            Err(Either::Send(session::SendLoopError::Io(err))) => {
                tracing::info!(
                    session_id = session_id.0,
                    error = %err,
                    "duplex send error (recoverable)",
                );
                false
            }
            Err(Either::Recv(session::RecvLoopError::Io(err))) => {
                tracing::info!(
                    session_id = session_id.0,
                    error = %err,
                    "duplex recv error (recoverable)",
                );
                false
            }
            Err(Either::Send(e)) => {
                tracing::info!(
                    session_id = session_id.0,
                    error = %e,
                    "duplex send terminal error"
                );
                true
            }
            Err(Either::Recv(e)) => {
                tracing::info!(
                    session_id = session_id.0,
                    error = %e,
                    "duplex recv terminal error"
                );
                true
            }
        };

        // Flush any pending recv ack so the peer's
        // unacked queue clears cleanly before this
        // connection goes away. Mirrors the simplex
        // server's drain logic (see
        // `dispatch_stream`); without it, peers
        // retry-loop until `MESSAGE_DELIVERY_TIMEOUT`.
        if recv_next.ack < recv_next.seq {
            let recv_stream = connected.stream(super::INITIATOR_TO_ACCEPTOR);
            let ack = super::serialize_response(super::NetRxResponse::Ack(recv_next.seq - 1))
                .expect("serialize ack");
            let mut completion = recv_stream.write(ack);
            match completion.drive().await {
                Ok(()) => {
                    recv_next.ack = recv_next.seq;
                }
                Err(e) => {
                    tracing::debug!(
                        session_id = session_id.0,
                        error = %e,
                        "duplex: failed to flush acks during cleanup"
                    );
                }
            }
        }

        // On terminal exit, tell the peer we're
        // closing so it stops trying to reconnect.
        let terminal_response = match &result {
            Err(Either::Recv(session::RecvLoopError::SequenceError(reason))) => {
                Some(super::NetRxResponse::Reject(reason.clone()))
            }
            Err(Either::Recv(session::RecvLoopError::Cancelled))
            | Err(Either::Send(session::SendLoopError::AppClosed)) => {
                Some(super::NetRxResponse::Closed)
            }
            _ => None,
        };
        if let Some(rsp) = terminal_response {
            let recv_stream = connected.stream(super::INITIATOR_TO_ACCEPTOR);
            let data = super::serialize_response(rsp).expect("serialize terminal response");
            let mut completion = recv_stream.write(data);
            let _ = completion.drive().await;
        }

        session = connected.release();
        if terminal {
            break;
        }
    }

    // Recv/send loop is finished — drop the session entry so a later
    // reconnect for the same session_id starts a fresh dispatch task
    // instead of feeding a dead channel; any in-flight feeder's send
    // fails after the link's receiver above is dropped.
    sessions.remove(&session_id);

    let _ = notify.send(TxStatus::Closed(CloseReason::Other(
        "duplex session ended".into(),
    )));
}

/// Establish a duplex (bidirectional) session over the given link.
/// Returns a [`DuplexClient`] wrapping the send/recv halves and the
/// spawned recv/send task; the client owns a cancellation token so
/// callers can deterministically tear the session down via
/// [`DuplexClient::join`].
pub(crate) fn spawn<Out: RemoteMessage, In: RemoteMessage>(
    link: impl Link,
) -> DuplexClient<Out, In> {
    let addr = link.dest();
    let session_id = link.link_id();
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel::<In>(1024);
    let (notify, status) = watch::channel(TxStatus::Active);
    let cancel_token = CancellationToken::new();
    let task_cancel = cancel_token.clone();
    let dest = addr.clone();
    let join_handle = crate::init::get_runtime().spawn(async move {
        let mut session = Session::new(link);
        let log_id = format!("session {}.{:016x}", dest, session_id.0);
        let mut deliveries = session::Deliveries {
            outbox: session::Outbox::new(log_id.clone(), dest.clone(), session_id.0),
            unacked: session::Unacked::new(None, log_id),
        };
        let mut outbound_rx = outbound_rx;
        let mut recv_next = Next { seq: 0, ack: 0 };
        let mut reconnect_backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(std::time::Duration::from_millis(10))
            .with_multiplier(2.0)
            .with_randomization_factor(0.1)
            .with_max_interval(std::time::Duration::from_secs(5))
            .with_max_elapsed_time(None)
            .build();

        let mut link_status = LinkStatus::NeverConnected;

        loop {
            // Race connect against cancel so a `DuplexClient::join`
            // call mid-dial doesn't have to wait for the dial
            // backoff to elapse.
            let connected = tokio::select! {
                result = session.connect() => match result {
                    Ok(s) => s,
                    Err(_) => break,
                },
                _ = task_cancel.cancelled() => break,
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

            link_status.connected();
            let connected_at = tokio::time::Instant::now();

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
                    _ = task_cancel.cancelled() => Err(Either::Recv(session::RecvLoopError::Cancelled)),
                }
            };

            link_status.disconnected();

            if connected_at.elapsed() > tokio::time::Duration::from_secs(1) {
                reconnect_backoff.reset();
            }

            let terminal = match &result {
                Ok(()) => {
                    if let Some(delay) = reconnect_backoff.next_backoff() {
                        tracing::info!(
                            dest = %dest,
                            session_id = session_id.0,
                            delay_ms = delay.as_millis() as u64,
                            "duplex send_connected returned EOF, reconnecting after backoff; {link_status}"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    false
                }
                Err(Either::Send(e)) => {
                    let terminal = log_send_error(e, &dest, session_id.0, "duplex", &link_status);
                    if !terminal {
                        // Recoverable send error — reconnect after backoff.
                        if let Some(delay) = reconnect_backoff.next_backoff() {
                            tracing::info!(
                                dest = %dest,
                                session_id = session_id.0,
                                error = %e,
                                delay_ms = delay.as_millis() as u64,
                                mode = "duplex",
                                "send error (recoverable), reconnecting after backoff; {link_status}",
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                    terminal
                }
                Err(Either::Recv(session::RecvLoopError::Io(err))) => {
                    if let Some(delay) = reconnect_backoff.next_backoff() {
                        tracing::info!(
                            dest = %dest,
                            session_id = session_id.0,
                            error = %err,
                            delay_ms = delay.as_millis() as u64,
                            mode = "duplex",
                            "recv error (recoverable), reconnecting after backoff; {link_status}",
                        );
                        tokio::time::sleep(delay).await;
                    }
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
                Err(Either::Recv(e)) => {
                    tracing::info!(
                        dest = %dest,
                        session_id = session_id.0,
                        error = %e,
                        "duplex recv terminal error; {link_status}"
                    );
                    true
                }
            };

            // Flush any pending recv ack so the peer's send-side
            // unacked queue clears cleanly before this connection
            // goes away. Mirrors the cleanup in
            // `dispatch_duplex_stream` but on the other tag — the
            // initiator reads data on `ACCEPTOR_TO_INITIATOR`, so
            // its acks travel back on that same tag.
            if recv_next.ack < recv_next.seq {
                let recv_stream = connected.stream(super::ACCEPTOR_TO_INITIATOR);
                let ack = super::serialize_response(super::NetRxResponse::Ack(recv_next.seq - 1))
                    .expect("serialize ack");
                let mut completion = recv_stream.write(ack);
                match completion.drive().await {
                    Ok(()) => {
                        recv_next.ack = recv_next.seq;
                    }
                    Err(e) => {
                        tracing::debug!(
                            dest = %dest,
                            session_id = session_id.0,
                            error = %e,
                            "duplex client: failed to flush acks during cleanup"
                        );
                    }
                }
            }

            // On terminal exit, tell the peer we're closing (or
            // rejecting) on the same tag we read data on. Mirrors
            // `dispatch_duplex_stream`; without it, the peer's
            // server-side dispatch keeps awaiting a reconnect that
            // will never come.
            let terminal_response = match &result {
                Err(Either::Recv(session::RecvLoopError::SequenceError(reason))) => {
                    Some(super::NetRxResponse::Reject(reason.clone()))
                }
                Err(Either::Recv(session::RecvLoopError::Cancelled))
                | Err(Either::Send(session::SendLoopError::AppClosed)) => {
                    Some(super::NetRxResponse::Closed)
                }
                _ => None,
            };
            if let Some(rsp) = terminal_response {
                let recv_stream = connected.stream(super::ACCEPTOR_TO_INITIATOR);
                let data =
                    super::serialize_response(rsp).expect("serialize terminal response");
                let mut completion = recv_stream.write(data);
                let _ = completion.drive().await;
            }

            session = connected.release();
            if terminal {
                break;
            }
        }

        let _ = notify.send(TxStatus::Closed(CloseReason::Other(
            "duplex session ended".into(),
        )));
    });
    let tx = DuplexTx::new(outbound_tx, addr.clone(), status);
    let rx = DuplexRx::new(inbound_rx, addr.clone());
    DuplexClient {
        tx,
        rx: Some(rx),
        join_handle,
        cancel_token,
        addr,
    }
}

/// Connect to a duplex server. Returns a [`DuplexClient`] wrapping
/// the send/recv halves and the spawned recv/send task; callers use
/// [`DuplexClient::tx`] / [`DuplexClient::take_rx`] to extract the
/// halves and [`DuplexClient::join`] to deterministically shut the
/// session down.
pub fn dial<Out: RemoteMessage, In: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<DuplexClient<Out, In>, ClientError> {
    Ok(spawn(super::link(addr, super::SessionId::random(), 0)?))
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
            serve::<u64, String>(ChannelAddr::Tcp("[::1]:0".parse().unwrap()), None).unwrap();
        let server_addr = server.addr().clone();

        // Client: sends u64, receives String.
        let mut client = dial::<u64, String>(server_addr).unwrap();
        let client_tx = client.tx();
        let mut client_rx = client.take_rx().unwrap();

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
        let mut server =
            serve::<u64, u64>(ChannelAddr::Tcp("[::1]:0".parse().unwrap()), None).unwrap();
        let server_addr = server.addr().clone();

        // Two independent clients.
        let mut client1 = dial::<u64, u64>(server_addr.clone()).unwrap();
        let tx1 = client1.tx();
        let mut rx1 = client1.take_rx().unwrap();
        let (mut srx1, stx1) = server.accept().await.unwrap();

        let mut client2 = dial::<u64, u64>(server_addr).unwrap();
        let tx2 = client2.tx();
        let mut rx2 = client2.take_rx().unwrap();
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
        let mut server = serve::<u64, u64>(addr, None)?;
        let server_addr = server.addr().clone();

        let server_handle = tokio::spawn(async move {
            let (mut rx, tx) = server.accept().await.unwrap();
            while let Ok(msg) = rx.recv().await {
                tx.post(msg);
            }
        });

        let mut client = dial::<u64, u64>(server_addr).unwrap();
        let client_tx = client.tx();
        let mut client_rx = client.take_rx().unwrap();

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
