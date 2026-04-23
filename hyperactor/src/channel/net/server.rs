/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Server (receive) side of simplex channels.

use std::collections::HashMap;
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

/// Shared state for multi-stream sessions. Each additional stream
/// (stream_id > 0) gets its own MVar for connection handoff. The
/// [`AckWatermark`] is shared across all streams so cumulative acks
/// reflect the global sequence space.
pub(super) struct StreamState<S: Stream> {
    /// Per-stream MVars for connection handoff. Keyed by stream_id.
    /// Accessed concurrently from multiple accept-loop dispatch tasks
    /// (one per incoming connection for the same session), but only
    /// for a brief `entry().or_insert_with()` — a plain std Mutex is
    /// cheaper than DashMap here.
    streams: std::sync::Mutex<HashMap<u8, MVar<S>>>,
    /// Shared ack tracker: each stream records its received seqs here.
    ack_watermark: tokio::sync::Mutex<session::AckWatermark>,
}

impl<S: Stream> StreamState<S> {
    pub(super) fn new() -> Self {
        let ack_msg_interval: u64 =
            hyperactor_config::global::get(config::MESSAGE_ACK_EVERY_N_MESSAGES);
        let ack_time_interval = hyperactor_config::global::get(config::MESSAGE_ACK_TIME_INTERVAL);
        Self {
            streams: std::sync::Mutex::new(HashMap::new()),
            ack_watermark: tokio::sync::Mutex::new(session::AckWatermark::new(
                ack_msg_interval,
                ack_time_interval,
            )),
        }
    }
}

/// If `link_init` is for a multi-stream connection (`stream_id > 0`),
/// resolve (or create) the shared per-session state and return
/// `Some((stream_id, state))`. Otherwise return `None`.
fn resolve_stream<S: Stream>(
    stream_state: &std::sync::Mutex<HashMap<SessionId, Arc<StreamState<S>>>>,
    link_init: &super::LinkInit,
) -> Option<(u8, Arc<StreamState<S>>)> {
    if link_init.stream_id == 0 {
        return None;
    }
    let state = stream_state
        .lock()
        .unwrap()
        .entry(link_init.session_id)
        .or_insert_with(|| Arc::new(StreamState::new()))
        .clone();
    Some((link_init.stream_id, state))
}

/// Dispatch a newly accepted connection to the right session.
///
/// When `streams` is `None`, this is a single-stream (strictly
/// in-order) session, and the MVar to hand the connection to is
/// looked up in `sessions` (creating a fresh session reader on first
/// use).
///
/// When `streams` is `Some((stream_id, state))`, this is the
/// `stream_id`-th stream of a multi-stream session sharing `state`
/// (the caller resolves the per-session state from its own map). The
/// connection is handed to a per-`stream_id` MVar inside `state`.
pub(super) async fn dispatch_stream<M: RemoteMessage, S: Stream>(
    session_id: SessionId,
    streams: Option<(u8, Arc<StreamState<S>>)>,
    conn: S,
    sessions: &DashMap<SessionId, MVar<S>>,
    dest: ChannelAddr,
    tx: mpsc::Sender<M>,
    cancel: CancellationToken,
) {
    if let Some((stream_id, state)) = streams {
        // Multi-stream: route to a per-stream reader task.
        dispatch_multi_stream::<M, S>(session_id, stream_id, conn, state, dest, tx, cancel).await;
        return;
    }

    // Single-stream: existing logic.
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

                        match &result {
                            Ok(()) => {
                                tracing::info!(
                                    %dest,
                                    %session_id,
                                    "recv_connected returned EOF, awaiting reconnect"
                                );
                            }
                            Err(e) => {
                                tracing::info!(
                                    %dest,
                                    %session_id,
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

/// Handle a multi-stream connection (stream_id > 0). Spawns a per-stream
/// reader task that reads frames, deserializes, delivers to the shared
/// mpsc, and sends acks. Uses an AckWatermark shared across all streams
/// of the same session for cumulative out-of-order acking.
async fn dispatch_multi_stream<M: RemoteMessage, S: Stream>(
    session_id: SessionId,
    stream_id: u8,
    conn: S,
    state: Arc<StreamState<S>>,
    dest: ChannelAddr,
    tx: mpsc::Sender<M>,
    cancel: CancellationToken,
) {
    tracing::info!(
        %dest, %session_id, stream_id,
        "dispatch_multi_stream: new connection"
    );

    // Get or create an MVar for this stream_id.
    let stream_mvar = {
        use std::collections::hash_map::Entry;
        let mut streams = state.streams.lock().unwrap();
        match streams.entry(stream_id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let mvar: MVar<S> = MVar::empty();
                let link = AcceptorLink {
                    dest: dest.clone(),
                    session_id,
                    stream: mvar.clone(),
                    cancel: cancel.clone(),
                };
                let deliver_tx = tx;
                let ct = cancel.clone();
                let shared_state = state.clone();

                // Spawn a reader task for this stream.
                tokio::spawn(async move {
                    let mut session = Session::new(link);

                    loop {
                        let connected = match session.connect().await {
                            Ok(s) => s,
                            Err(_) => break,
                        };

                        let result = {
                            let conn = connected.stream(super::INITIATOR_TO_ACCEPTOR);
                            tokio::select! {
                                r = session::multi_stream_recv_connected::<M, _, _>(
                                    &conn,
                                    &shared_state.ack_watermark,
                                    &deliver_tx,
                                ) => r,
                                _ = ct.cancelled() => Err(session::RecvLoopError::Cancelled),
                            }
                        };

                        let recoverable =
                            matches!(&result, Ok(()) | Err(session::RecvLoopError::Io(_)));

                        match &result {
                            Ok(()) => {
                                tracing::info!(
                                    %dest,
                                    %session_id,
                                    stream_id,
                                    "multi-stream recv EOF, awaiting reconnect"
                                );
                            }
                            Err(e) => {
                                tracing::info!(
                                    %dest,
                                    %session_id,
                                    stream_id,
                                    error = %e,
                                    recoverable,
                                    "multi-stream recv error"
                                );
                            }
                        }

                        session = connected.release();

                        if recoverable {
                            continue;
                        }
                        break;
                    }
                });

                e.insert(mvar.clone());
                mvar
            }
        }
    };

    stream_mvar.put(conn).await;
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
pub(in crate::channel) fn serve<M: RemoteMessage>(
    addr: ChannelAddr,
    prebound_listener: Option<std::net::TcpListener>,
) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
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

    let sessions: Arc<DashMap<SessionId, MVar<Box<dyn Stream>>>> = Arc::new(DashMap::new());
    let stream_state: Arc<std::sync::Mutex<HashMap<SessionId, Arc<StreamState<Box<dyn Stream>>>>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let child_cancel = CancellationToken::new();
    let dispatch_dest = channel_addr.clone();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let stream_state = Arc::clone(&stream_state);
        let tx = tx.clone();
        let child_cancel = child_cancel.clone();
        let dest = dispatch_dest;
        move |link_init: super::LinkInit, stream: Box<dyn Stream>| {
            let sessions = Arc::clone(&sessions);
            let stream_state = Arc::clone(&stream_state);
            let tx = tx.clone();
            let cancel = child_cancel.child_token();
            let dest = dest.clone();
            async move {
                let streams = resolve_stream(&stream_state, &link_init);
                dispatch_stream(
                    link_init.session_id,
                    streams,
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
        let link_init = read_link_init(&mut stream)
            .await
            .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
        Ok((link_init, stream))
    };

    let sessions: Arc<DashMap<SessionId, MVar<L::Stream>>> = Arc::new(DashMap::new());
    let stream_state: Arc<std::sync::Mutex<HashMap<SessionId, Arc<StreamState<L::Stream>>>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let child_cancel = CancellationToken::new();
    let dispatch = {
        let sessions = Arc::clone(&sessions);
        let stream_state = Arc::clone(&stream_state);
        let tx = tx.clone();
        let child_cancel = child_cancel.clone();
        let dest = channel_addr.clone();
        move |link_init: super::LinkInit, stream: L::Stream| {
            let sessions = Arc::clone(&sessions);
            let stream_state = Arc::clone(&stream_state);
            let tx = tx.clone();
            let cancel = child_cancel.child_token();
            let dest = dest.clone();
            async move {
                let streams = resolve_stream(&stream_state, &link_init);
                dispatch_stream(
                    link_init.session_id,
                    streams,
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
