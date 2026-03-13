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
use super::read_link_init;
use super::session;
use super::session::Next;
use super::session::Session;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;
use crate::channel::net::NetRx;
use crate::channel::net::ServerError;
use crate::channel::net::Stream;
use crate::channel::net::meta;
use crate::channel::net::tls;
use crate::config;
use crate::metrics;
use crate::sync::mvar::MVar;

/// Server-side link that receives pre-established streams from the
/// dispatcher via an MVar. Implements [`Link`] so it can be used
/// with [`Session::new`].
pub(super) struct AcceptorLink<S: Stream> {
    pub(super) dest: ChannelAddr,
    pub(super) session_id: SessionId,
    pub(super) stream: MVar<S>,
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

    async fn next(&self) -> Result<S, ClientError> {
        tokio::select! {
            stream = self.stream.take() => Ok(stream),
            _ = self.cancel.cancelled() => Err(ClientError::Connect(
                self.dest.clone(),
                std::io::Error::other("acceptor closed"),
                "acceptor channel closed".into(),
            )),
        }
    }
}

/// Dispatch a stream to the appropriate session, creating one if this
/// is the first connection for the given session ID.
pub(super) async fn dispatch_stream<M: RemoteMessage, S: Stream>(
    session_id: SessionId,
    conn: S,
    sessions: &DashMap<SessionId, MVar<S>>,
    dest: ChannelAddr,
    tx: mpsc::Sender<M>,
    cancel: CancellationToken,
) {
    let stream = {
        let entry = sessions.entry(session_id);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => e.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let stream: MVar<S> = MVar::empty();
                let link = AcceptorLink {
                    dest: dest.clone(),
                    session_id,
                    stream: stream.clone(),
                    cancel: cancel.clone(),
                };
                let deliver_tx = tx;
                let ct = cancel.clone();
                tokio::spawn(async move {
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
                                r = session::recv_connected::<M, _, _>(
                                    &conn,
                                    &deliver_tx,
                                    &mut next,
                                ) => r,
                                _ = ct.cancelled() => Err(session::RecvLoopError::Cancelled),
                            }
                        };

                        // Flush remaining ack if behind.
                        if next.ack < next.seq {
                            let ack =
                                super::serialize_response(super::NetRxResponse::Ack(next.seq - 1))
                                    .unwrap();
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
                            Err(session::RecvLoopError::Cancelled) => {
                                Some(super::NetRxResponse::Closed)
                            }
                            _ => None,
                        };
                        if let Some(rsp) = terminal_response {
                            let data = super::serialize_response(rsp).unwrap();
                            let conn = connected.stream(super::INITIATOR_TO_ACCEPTOR);
                            let mut completion = conn.write(data);
                            let _ = completion.drive().await;
                        }

                        let recoverable =
                            matches!(&result, Ok(()) | Err(session::RecvLoopError::Io(_)));
                        session = connected.release();

                        if recoverable {
                            continue;
                        }
                        break; // SequenceError or Cancelled
                    }
                });
                e.insert(stream.clone());
                stream
            }
        }
    };

    stream.put(conn).await;
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
    S: Stream,
    L: super::Listener,
    F: Fn(L::Stream, ChannelAddr) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(SessionId, S), anyhow::Error>> + Send + 'static,
    D: Fn(SessionId, S) -> DFut + Clone + Send + 'static,
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
                            let (session_id, stream) = prepare(stream, source).await?;
                            dispatch(session_id, stream).await;
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

/// Serve new connections on the given address.
pub(in crate::channel) fn serve<M: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
    let (mut listener, channel_addr) = super::listen(addr)?;

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
                let session_id = read_link_init(&mut tls_stream)
                    .await
                    .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
                Ok((session_id, Box::new(tls_stream) as Box<dyn Stream>))
            } else {
                let mut stream = stream;
                let session_id = read_link_init(&mut stream)
                    .await
                    .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
                Ok((session_id, stream))
            }
        }
    };

    let sessions: Arc<DashMap<SessionId, MVar<Box<dyn Stream>>>> = Arc::new(DashMap::new());
    let child_cancel = CancellationToken::new();
    let dispatch_dest = channel_addr.clone();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let tx = tx.clone();
        let child_cancel = child_cancel.clone();
        let dest = dispatch_dest;
        move |session_id: SessionId, stream: Box<dyn Stream>| {
            let sessions = Arc::clone(&sessions);
            let tx = tx.clone();
            let cancel = child_cancel.child_token();
            let dest = dest.clone();
            async move {
                dispatch_stream(session_id, stream, &sessions, dest, tx, cancel).await;
            }
        }
    };

    let ca: ChannelAddr = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        let result = accept_loop(&mut listener, &ca, &child_token, prepare, dispatch).await;
        child_cancel.cancel();
        result
    });

    let server_handle = ServerHandle {
        join_handle,
        cancel_token,
        channel_addr: channel_addr.clone(),
    };

    Ok((
        server_handle.channel_addr.clone(),
        NetRx(rx, channel_addr, server_handle),
    ))
}

/// Test-only variant that accepts an arbitrary `Listener`. Used by
/// mock-link tests that cannot go through `net::listen()`.
#[cfg(test)]
pub(super) fn serve_with_listener<M, L>(
    mut listener: L,
    channel_addr: ChannelAddr,
) -> Result<(ChannelAddr, NetRx<M>), ServerError>
where
    M: RemoteMessage,
    L: super::Listener + 'static,
    L::Stream: Unpin + std::fmt::Debug + 'static,
{
    let (tx, rx) = mpsc::channel::<M>(1024);
    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    let prepare = |mut stream: L::Stream, source: ChannelAddr| async move {
        let session_id = read_link_init(&mut stream)
            .await
            .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
        Ok((session_id, stream))
    };

    let sessions: Arc<DashMap<SessionId, MVar<L::Stream>>> = Arc::new(DashMap::new());
    let child_cancel = CancellationToken::new();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let tx = tx.clone();
        let child_cancel = child_cancel.clone();
        let dest = channel_addr.clone();
        move |session_id: SessionId, stream: L::Stream| {
            let sessions = Arc::clone(&sessions);
            let tx = tx.clone();
            let cancel = child_cancel.child_token();
            let dest = dest.clone();
            async move {
                dispatch_stream(session_id, stream, &sessions, dest, tx, cancel).await;
            }
        }
    };

    let ca = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        let result = accept_loop(&mut listener, &ca, &child_token, prepare, dispatch).await;
        child_cancel.cancel();
        result
    });

    let server_handle = ServerHandle {
        join_handle,
        cancel_token,
        channel_addr: channel_addr.clone(),
    };

    Ok((
        server_handle.channel_addr.clone(),
        NetRx(rx, channel_addr, server_handle),
    ))
}
