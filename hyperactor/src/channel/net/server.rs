/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Server (receive) side of simplex channels.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;

use super::read_link_init;
use super::serialize_response;
use super::session;
use super::session::Mux;
use super::session::Next;
use crate::RemoteMessage;
use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;
use crate::channel::net::NetRx;
use crate::channel::net::NetRxResponse;
use crate::channel::net::ServerError;
use crate::channel::net::meta;
use crate::channel::net::tls;
use crate::config;
use crate::metrics;
use crate::sync::mvar::MVar;

/// Server-side representation of a logical link. Created when a new `SessionId` is
/// first seen. Wraps `MVar<Next>` for session state; the MVar acts as both
/// storage and a serializer — only one connection at a time can hold the
/// `Next` state for a given link.
pub(super) struct ServerLink {
    id: super::SessionId,
    next: MVar<Next>,
}

impl Clone for ServerLink {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            next: self.next.clone(),
        }
    }
}

impl ServerLink {
    pub(super) fn new(id: super::SessionId) -> Self {
        Self {
            id,
            next: MVar::full(Next { seq: 0, ack: 0 }),
        }
    }
}

/// Handles a single connection for a logical link. Takes `Next` state from
/// the link's MVar, processes the connection, then puts `Next` back.
/// The MVar take/put provides natural serialization across reconnections.
pub(super) async fn handle_connection<S, M>(
    stream: S,
    source: ChannelAddr,
    dest: ChannelAddr,
    link: ServerLink,
    tx: mpsc::Sender<M>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error>
where
    S: super::Stream,
    M: RemoteMessage,
{
    let session_id = link.id;
    let mut next = link.next.take().await;
    let initial_next = next.clone();

    let max = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
    let (reader, writer) = tokio::io::split(stream);
    let mux = Mux::new(reader, writer, max);
    let stream = mux.stream(0);

    let result = tokio::select! {
        r = session::recv_connected::<M, _, _>(&stream, &tx, &mut next) => r,
        _ = cancel_token.cancelled() => Err(session::RecvLoopError::Cancelled),
    };

    tracing::info!(
        source = %source,
        dest = %dest,
        session_id = %session_id,
        "recv_connected exited: initial {initial_next}, final {next}, outcome: {}",
        match &result {
            Ok(()) => "eof".to_string(),
            Err(e) => format!("{e}"),
        }
    );

    // Post-loop cleanup: flush final ack, send reject/closed, shutdown.

    // Flush remaining ack if behind.
    if next.ack < next.seq {
        if let Ok(ack) = serialize_response(NetRxResponse::Ack(next.seq - 1)) {
            let mut completion = stream.write(ack);
            match completion.drive().await {
                Ok(()) => {
                    next.ack = next.seq;
                }
                Err(e) => {
                    tracing::debug!(
                        session_id = %session_id,
                        source = %source,
                        dest = %dest,
                        error = %e,
                        "failed to flush acks during cleanup"
                    );
                }
            }
        }
    }

    // Send reject or closed response if appropriate.
    let terminal_response = match &result {
        Err(session::RecvLoopError::SequenceError(reason)) => {
            Some(NetRxResponse::Reject(reason.clone()))
        }
        Err(session::RecvLoopError::Cancelled) => Some(NetRxResponse::Closed),
        _ => None,
    };

    if let Some(rsp) = terminal_response {
        if let Ok(data) = serialize_response(rsp) {
            let mut completion = stream.write(data);
            let _ = completion.drive().await;
        }
    }

    link.next.put(next).await;

    result.map_err(|e| anyhow::anyhow!("{e}"))
}

/// Main listen loop. Each accepted connection spawns a task that looks up (or
/// creates) the `ServerLink` for its `SessionId`, then calls `handle_connection`.
/// The MVar inside `ServerLink` serializes overlapping connections for the
/// same link.
async fn listen<M: RemoteMessage, L: super::Listener>(
    mut listener: L,
    listener_channel_addr: ChannelAddr,
    tx: mpsc::Sender<M>,
    parent_cancel_token: CancellationToken,
    is_tls: bool,
) -> Result<(), ServerError>
where
    L::Stream: Unpin + fmt::Debug + 'static,
{
    let mut connections: JoinSet<Result<(), anyhow::Error>> = JoinSet::new();

    // Cancellation token used to cancel our children only.
    let child_cancel_token = CancellationToken::new();

    let links: Arc<DashMap<super::SessionId, ServerLink>> = Arc::new(DashMap::new());

    // Heartbeat timer for server health metrics
    let heartbeat_interval = hyperactor_config::global::get(config::SERVER_HEARTBEAT_INTERVAL);
    let mut heartbeat_timer: Interval = tokio::time::interval(heartbeat_interval);

    let result: Result<(), ServerError> = loop {
        let _ = tracing::info_span!("channel_listen_accept_loop");
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, source)) => {
                        tracing::debug!(
                            source = %source,
                            dest = %listener_channel_addr,
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
                        let dest = listener_channel_addr.clone();
                        let links = Arc::clone(&links);
                        connections.spawn(async move {
                            let res = if is_tls {
                                let tls_acceptor = match dest.transport() {
                                    ChannelTransport::Tls => tls::tls_acceptor()?,
                                    _ => meta::tls_acceptor(true)?,
                                };
                                let mut stream = tls_acceptor.accept(stream).await?;
                                let session_id = read_link_init(&mut stream).await
                                    .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
                                let link = links.entry(session_id).or_insert_with(|| ServerLink::new(session_id)).value().clone();
                                handle_connection(stream, source.clone(), dest.clone(), link, tx, child_cancel_token).await
                            } else {
                                let mut stream = stream;
                                let session_id = read_link_init(&mut stream).await
                                    .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
                                let link = links.entry(session_id).or_insert_with(|| ServerLink::new(session_id)).value().clone();
                                handle_connection(stream, source.clone(), dest.clone(), link, tx, child_cancel_token).await
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
                            dest = %listener_channel_addr,
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
                        "dest" => listener_channel_addr.to_string()
                    ),
                );
            }

            _ = parent_cancel_token.cancelled() => {
                tracing::info!(
                    dest = %listener_channel_addr,
                    "received parent token cancellation"
                );
                break Ok(());
            }

            result = join_nonempty(&mut connections) => {
                if let Err(err) = result {
                    tracing::info!(
                        dest = %listener_channel_addr,
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
pub(super) fn serve<M: RemoteMessage, L: super::Listener>(
    listener: L,
    channel_addr: ChannelAddr,
    is_tls: bool,
) -> Result<(ChannelAddr, NetRx<M>), ServerError>
where
    L::Stream: Unpin + fmt::Debug + 'static,
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
