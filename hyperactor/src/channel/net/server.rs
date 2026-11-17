/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Server (receive) side of simplex channels.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;

use super::ClientError;
use super::Link;
use super::SessionId;
#[cfg(test)]
use super::read_link_init;
use super::session;
use super::session::Next;
use super::session::Sequencing;
use super::session::Session;
use super::session::TaggedStream;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelRx;
use crate::channel::net::ServerError;
use crate::channel::net::Stream;
use crate::config;
use crate::metrics;

/// Server-side link that receives pre-established streams from the
/// dispatcher via an unbounded mpsc channel. Implements [`Link`] so
/// it can be used with [`Session::new`]. Dropping the link drops
/// the receiver, which causes any feeder's later `send` to fail
/// with `SendError` (the conn is dropped). The channel is unbounded
/// so the dispatch task can publish its sender to the session map
/// before draining the first connection without risking a deadlock
/// against a concurrent feeder for the same session_id.
pub(super) struct AcceptorLink<S: Stream> {
    pub(super) dest: ChannelAddr,
    pub(super) session_id: SessionId,
    pub(super) stream: mpsc::UnboundedReceiver<S>,
    pub(super) cancel: CancellationToken,
}

impl<S: Stream> fmt::Debug for AcceptorLink<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcceptorLink")
            .field("dest", &self.dest)
            .field("session_id", &self.session_id)
            .finish()
    }
}

#[async_trait]
impl<S: Stream> Link for AcceptorLink<S> {
    type Stream = S;

    fn dest(&self) -> ChannelAddr {
        self.dest.clone()
    }

    fn link_id(&self) -> SessionId {
        self.session_id
    }

    async fn next(&mut self) -> Result<S, ClientError> {
        let dest = self.dest.clone();
        let make_err = move || {
            ClientError::Connect(
                dest.clone(),
                std::io::Error::other("acceptor closed"),
                "acceptor channel closed".into(),
            )
        };
        tokio::select! {
            result = self.stream.recv() => result.ok_or_else(&make_err),
            _ = self.cancel.cancelled() => Err(make_err()),
        }
    }
}

/// Dispatch a newly accepted connection to the right session.
///
/// Stream 0 is an ordered session. A stream id above 0 is one stream of
/// an unordered session: streams share nothing on the server, and each
/// keeps its own sequence numbers and watermark (see
/// [`Sequencing::Unordered`]).
///
/// Structured concurrency: the first dispatch for a (`session_id`,
/// `stream_id`) runs the recv-loop inline and only returns after its
/// terminal cleanup (flush any pending ack, emit `Closed` on
/// cancellation). Reconnects hand the connection off via the
/// per-stream channel and return immediately. [`accept_loop`] joins
/// every dispatch in its `connections` `JoinSet`, so it finishes only
/// after every recv-loop has finished.
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn dispatch_stream<M: RemoteMessage, S: Stream>(
    session_id: SessionId,
    stream_id: u8,
    conn: S,
    sessions: &DashMap<(SessionId, u8), mpsc::UnboundedSender<S>>,
    dest: ChannelAddr,
    tx: mpsc::Sender<M>,
    cancel: CancellationToken,
) {
    let sequencing = if stream_id == 0 {
        Sequencing::Ordered
    } else {
        Sequencing::Unordered
    };

    // Insert into the session map and drop the DashMap shard guard
    // before any await. Vacant inserts the sender side; occupied
    // returns the existing sender so this dispatch acts as a feeder.
    // Unbounded so this task can publish the sender before draining
    // the first conn without deadlocking a concurrent feeder for the
    // same stream.
    let entry_result = {
        let entry = sessions.entry((session_id, stream_id));
        match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => Err(e.get().clone()),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let (sender, receiver) = mpsc::unbounded_channel::<S>();
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
            let _ = sender.send(conn);
            return;
        }
        Ok(pair) => pair,
    };

    let link = AcceptorLink {
        dest: dest.clone(),
        session_id,
        stream: receiver,
        cancel: cancel.clone(),
    };
    let deliver_tx = tx;
    let ct = cancel;

    // Hand the first connection off through the channel; the
    // recv-loop below picks it up via `session.connect().await`.
    let _ = sender.send(conn);
    drop(sender);

    let mut session = Session::new(link);
    let mut next = Next { seq: 0, ack: 0 };

    loop {
        let connected = match session.connect().await {
            Ok(s) => s,
            Err(_) => break,
        };

        let result = {
            let conn = connected.stream(super::INITIATOR_TO_ACCEPTOR);
            tokio::select! {
                r = session::recv_sequenced::<M, _, _>(
                    &conn,
                    &deliver_tx,
                    &mut next,
                    sequencing,
                ) => r,
                _ = ct.cancelled() => Err(session::RecvLoopError::Cancelled),
            }
        };

        // Flush a final ack if behind.
        if next.ack < next.seq {
            let ack = super::serialize_response(super::NetRxResponse::Ack(next.seq - 1)).unwrap();
            let conn = connected.stream(super::INITIATOR_TO_ACCEPTOR);
            let mut completion = conn.write(ack);
            match completion.drive().await {
                Ok(()) => {
                    next.ack = next.seq;
                }
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        "failed to flush acks during cleanup"
                    );
                }
            }
        }

        // Send reject or closed response if appropriate.
        let terminal_response = match &result {
            Err(session::RecvLoopError::SequenceError(reason)) => {
                Some(super::NetRxResponse::Reject(reason.clone()))
            }
            Err(session::RecvLoopError::Cancelled) => Some(super::NetRxResponse::Closed),
            _ => None,
        };
        if let Some(rsp) = terminal_response {
            let rejected = !matches!(rsp, super::NetRxResponse::Closed);
            let data = super::serialize_response(rsp).unwrap();
            let conn = connected.stream(super::INITIATOR_TO_ACCEPTOR);
            let mut completion = conn.write(data);
            let _ = completion.drive().await;
            if rejected {
                linger_until_closed(&conn, &ct).await;
            }
        }

        let recoverable = matches!(&result, Ok(()) | Err(session::RecvLoopError::Io(_)));

        match &result {
            Ok(()) => {
                tracing::info!(
                    %dest,
                    %session_id,
                    stream_id,
                    "recv_connected returned EOF, awaiting reconnect"
                );
            }
            Err(e) => {
                tracing::info!(
                    %dest,
                    %session_id,
                    stream_id,
                    error = %e,
                    recoverable,
                    "recv_connected returned error"
                );
            }
        }

        session = connected.release();

        if recoverable {
            continue;
        }
        break;
    }

    // Recv-loop is finished — no further conns will be drained.
    // Drop the session entry so a later reconnect for the same
    // stream starts a fresh dispatch task instead of feeding a dead
    // channel; any in-flight feeder's send fails after the receiver
    // above is dropped.
    sessions.remove(&(session_id, stream_id));
}

/// How long a dispatcher that rejected a session waits for the client to
/// close the connection.
const REJECT_LINGER: Duration = Duration::from_secs(5);

/// Read and discard frames until the client closes the connection, for at
/// most [`REJECT_LINGER`] or until `cancel` fires. Closing the connection
/// with frames still unread would reset it, and the client could lose the
/// rejection; it would then reconnect and count the frames it wrote here as
/// possibly delivered.
async fn linger_until_closed<R, W>(conn: &TaggedStream<R, W>, cancel: &CancellationToken)
where
    R: tokio::io::AsyncRead + Unpin + Send,
    W: tokio::io::AsyncWrite + Unpin + Send,
{
    tokio::select! {
        _ = tokio::time::sleep(REJECT_LINGER) => {}
        _ = cancel.cancelled() => {}
        _ = async { while let Ok(Some(_)) = conn.next().await {} } => {}
    }
}

/// Generic accept loop. Accepts connections from `listener`, transforms
/// each via `prepare` (which may do TLS negotiation), then hands them
/// to `dispatch`.
pub(super) async fn accept_loop<S, L, F, Fut, D, DFut>(
    listener: &mut L,
    listener_addr: &ChannelAddr,
    parent_cancel: &CancellationToken,
    prepare: F,
    dispatch: D,
) -> Result<(), ServerError>
where
    S: Send + 'static,
    L: super::Listener,
    F: Fn(L::Stream, ChannelAddr) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(super::LinkInit, S), anyhow::Error>> + Send + 'static,
    D: Fn(super::LinkInit, S) -> DFut + Clone + Send + 'static,
    DFut: Future<Output = ()> + Send + 'static,
{
    let mut connections: JoinSet<Result<(), anyhow::Error>> = JoinSet::new();

    let heartbeat_interval = hyperactor_config::global::get(config::SERVER_HEARTBEAT_INTERVAL);
    let mut heartbeat_timer: Interval = tokio::time::interval(heartbeat_interval);

    let result: Result<(), ServerError> = loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, source)) => {
                        tracing::debug!(
                            source = %source,
                            dest = %listener_addr,
                            "new connection accepted"
                        );
                        metrics::CHANNEL_CONNECTIONS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => listener_addr.transport().to_string(),
                                "operation" => "accept"
                            ),
                        );

                        let prepare = prepare.clone();
                        let dispatch = dispatch.clone();
                        connections.spawn(async move {
                            let (link_init, stream) = prepare(stream, source).await?;
                            dispatch(link_init, stream).await;
                            Ok(())
                        });
                    }
                    Err(err) => {
                        metrics::CHANNEL_ERRORS.add(
                            1,
                            hyperactor_telemetry::kv_pairs!(
                                "transport" => listener_addr.transport().to_string(),
                                "operation" => "accept",
                                "error" => err.to_string(),
                                "error_type" => metrics::ChannelErrorType::ConnectionError.as_str(),
                            ),
                        );
                        tracing::info!(
                            dest = %listener_addr,
                            error = %err,
                            "accept error"
                        );
                    }
                }
            }

            _ = heartbeat_timer.tick() => {
                metrics::SERVER_HEARTBEAT.add(
                    1,
                    hyperactor_telemetry::kv_pairs!(
                        "dest" => listener_addr.to_string()
                    ),
                );
            }

            _ = parent_cancel.cancelled() => {
                tracing::info!(
                    dest = %listener_addr,
                    "received parent token cancellation"
                );
                break Ok(());
            }

            result = session::join_nonempty(&mut connections) => {
                if let Err(err) = result {
                    tracing::info!(
                        dest = %listener_addr,
                        error = %err,
                        "connection task join error"
                    );
                }
            }
        }
    };

    while connections.join_next().await.is_some() {}
    result
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
    /// Create a new server handle.
    pub(super) fn new(
        join_handle: JoinHandle<Result<(), ServerError>>,
        cancel_token: CancellationToken,
        channel_addr: ChannelAddr,
    ) -> Self {
        Self {
            join_handle,
            cancel_token,
            channel_addr,
        }
    }

    /// Signal the server to stop. This will stop accepting new
    /// incoming connection requests, and drain pending operations
    /// on active connections. After draining is completed, the
    /// connections are closed.
    pub(crate) fn stop(&self, reason: &str) {
        tracing::info!(
            name = "ChannelServerStatus",
            dest = %self.channel_addr,
            status = "Stop::Sent",
            reason,
            "sent Stop signal; check server logs for the stop progress"
        );
        self.cancel_token.cancel();
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

/// Serve new connections on the given address, optionally using a pre-opened TCP listener.
/// When `prebound_listener` is `Some`, it is used instead of binding a new socket.
/// This is only supported for TCP-based transports (Tcp, Tls, MetaTls).
#[tracing::instrument(level = "debug", skip_all)]
pub(in crate::channel) fn serve<M: RemoteMessage>(
    addr: ChannelAddr,
    prebound_listener: Option<std::net::TcpListener>,
) -> Result<(ChannelAddr, ChannelRx<M>), ServerError> {
    let (mut listener, channel_addr) = super::listen_with_prebound(addr, prebound_listener)?;

    metrics::CHANNEL_CONNECTIONS.add(
        1,
        hyperactor_telemetry::kv_pairs!(
            "transport" => channel_addr.transport().to_string(),
            "operation" => "serve"
        ),
    );

    let (tx, rx) = mpsc::channel::<M>(1024);
    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    let prepare = super::preparer_for(channel_addr.clone(), Some(super::ProtocolKind::Simplex));

    let sessions: Arc<DashMap<(SessionId, u8), mpsc::UnboundedSender<Box<dyn Stream>>>> =
        Arc::new(DashMap::new());
    let dispatch_dest = channel_addr.clone();
    let dispatch_cancel = child_token.clone();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let tx = tx.clone();
        let dest = dispatch_dest;
        move |link_init: super::LinkInit, stream: Box<dyn Stream>| {
            let sessions = Arc::clone(&sessions);
            let tx = tx.clone();
            let cancel = dispatch_cancel.clone();
            let dest = dest.clone();
            async move {
                dispatch_stream(
                    link_init.session_id,
                    link_init.stream_id,
                    stream,
                    &sessions,
                    dest,
                    tx,
                    cancel,
                )
                .await;
            }
        }
    };

    let ca: ChannelAddr = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        accept_loop(&mut listener, &ca, &child_token, prepare, dispatch).await
    });

    let server_handle = ServerHandle {
        join_handle,
        cancel_token,
        channel_addr: channel_addr.clone(),
    };

    Ok((
        server_handle.channel_addr.clone(),
        ChannelRx {
            receiver: rx,
            dest: channel_addr,
            server: server_handle,
        },
    ))
}

/// Test-only variant that accepts an arbitrary `Listener`. Used by
/// mock-link tests that cannot go through `net::listen_with_prebound`.
#[cfg(test)]
#[tracing::instrument(level = "debug", skip_all)]
pub(super) fn serve_with_listener<M, L>(
    mut listener: L,
    channel_addr: ChannelAddr,
) -> Result<(ChannelAddr, ChannelRx<M>), ServerError>
where
    M: RemoteMessage,
    L: super::Listener + 'static,
    L::Stream: Unpin + std::fmt::Debug + 'static,
{
    let (tx, rx) = mpsc::channel::<M>(1024);
    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    let prepare = |mut stream: L::Stream, source: ChannelAddr| async move {
        let link_init = read_link_init(&mut stream)
            .await
            .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
        if link_init.kind != super::ProtocolKind::Simplex {
            return Err(anyhow::anyhow!(
                "simplex server received {:?} client from {}",
                link_init.kind,
                source
            ));
        }
        Ok((link_init, stream))
    };

    let sessions: Arc<DashMap<(SessionId, u8), mpsc::UnboundedSender<L::Stream>>> =
        Arc::new(DashMap::new());
    let dispatch_cancel = child_token.clone();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let tx = tx.clone();
        let dest = channel_addr.clone();
        move |link_init: super::LinkInit, stream: L::Stream| {
            let sessions = Arc::clone(&sessions);
            let tx = tx.clone();
            let cancel = dispatch_cancel.clone();
            let dest = dest.clone();
            async move {
                dispatch_stream(
                    link_init.session_id,
                    link_init.stream_id,
                    stream,
                    &sessions,
                    dest,
                    tx,
                    cancel,
                )
                .await;
            }
        }
    };

    let ca = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        accept_loop(&mut listener, &ca, &child_token, prepare, dispatch).await
    });

    let server_handle = ServerHandle {
        join_handle,
        cancel_token,
        channel_addr: channel_addr.clone(),
    };

    Ok((
        server_handle.channel_addr.clone(),
        ChannelRx {
            receiver: rx,
            dest: channel_addr,
            server: server_handle,
        },
    ))
}
