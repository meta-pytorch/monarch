/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A single listener that accepts both simplex and duplex clients on
//! the same address. Incoming connections are demultiplexed by the
//! [`ProtocolKind`] byte in the `LinkInit` header; simplex sessions
//! feed a [`ChannelRx<M>`], duplex sessions feed a [`DuplexServer<In, Out>`].

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::ChannelAddr;
use super::ProtocolKind;
use super::ServerError;
use super::ServerHandle;
use super::SessionId;
use super::Stream;
use super::duplex::DuplexServer;
use super::duplex::dispatch_duplex_stream;
use super::server::StreamState;
use super::server::accept_loop;
use super::server::dispatch_stream;
use super::server::resolve_stream;
use crate::RemoteMessage;
use crate::channel::ChannelRx;

/// Parts produced by [`serve`]: the bound address, the per-kind halves,
/// the accept-loop join handle, and the shared cancellation token.
pub(crate) struct MuxServeParts<M, In, Out>
where
    M: RemoteMessage,
    In: RemoteMessage,
    Out: RemoteMessage,
{
    pub addr: ChannelAddr,
    pub simplex: ChannelRx<M>,
    pub duplex: DuplexServer<In, Out>,
    pub join_handle: JoinHandle<Result<(), ServerError>>,
    pub cancel: CancellationToken,
}

/// Start a muxed server on `addr`. The accept-loop's [`JoinHandle`] is
/// returned alongside the per-kind halves and the shared cancellation
/// token so callers can build a true awaitable shutdown handle on top
/// — see [`MuxServer`](super::super::MuxServer) for the public
/// wrapper.
pub(crate) fn serve<M, In, Out>(
    addr: ChannelAddr,
    prebound_listener: Option<std::net::TcpListener>,
) -> Result<MuxServeParts<M, In, Out>, ServerError>
where
    M: RemoteMessage,
    In: RemoteMessage,
    Out: RemoteMessage,
{
    let (listener, channel_addr) = super::listen_with_prebound(addr, prebound_listener)?;
    // None: the muxed listener accepts both kinds and dispatches by
    // `link_init.kind` after the header is read.
    let prepare = super::preparer_for(channel_addr.clone(), None);
    serve_with_prepare(listener, channel_addr, prepare)
}

/// Shared implementation for the muxed listener. Wires up the
/// per-kind state, the dispatch closure, the accept loop, and the
/// drain-aware coordinator that exposes `drained` to the per-half
/// handles. Production [`serve`] passes a [`super::preparer_for`]
/// closure; tests pass an inline prepare closure that boxes
/// in-memory streams (see [`serve_with_listener`]).
fn serve_with_prepare<M, In, Out, L, F, Fut>(
    mut listener: L,
    channel_addr: ChannelAddr,
    prepare: F,
) -> Result<MuxServeParts<M, In, Out>, ServerError>
where
    M: RemoteMessage,
    In: RemoteMessage,
    Out: RemoteMessage,
    L: super::Listener + 'static,
    F: Fn(L::Stream, ChannelAddr) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<(super::LinkInit, Box<dyn Stream>), anyhow::Error>>
        + Send
        + 'static,
{
    let (simplex_tx, simplex_rx) = mpsc::channel::<M>(1024);
    let (duplex_accept_tx, duplex_accept_rx) = mpsc::channel(16);

    let simplex_sessions: Arc<DashMap<SessionId, mpsc::UnboundedSender<Box<dyn Stream>>>> =
        Arc::new(DashMap::new());
    let simplex_stream_state: Arc<
        std::sync::Mutex<HashMap<SessionId, Arc<StreamState<Box<dyn Stream>>>>>,
    > = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let duplex_sessions: Arc<DashMap<SessionId, mpsc::UnboundedSender<Box<dyn Stream>>>> =
        Arc::new(DashMap::new());

    // Accept-loop cancel. Cloned into both halves' `ServerHandle`s
    // and returned to the caller so dropping any of the parts (or the
    // returned token) tears down the shared listener.
    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    // Per-session cancellation propagated into dispatched sessions.
    // These are children of `cancel_token` so cancelling the parent
    // immediately propagates to in-flight sessions; without that, the
    // accept loop's `connections.join_next()` drain would block on
    // session tasks whose cancel tokens never fire, and shutdown would
    // hang. Mirrors the simplex (`server::serve`) and duplex
    // (`duplex::serve`) shapes, which also derive the dispatch cancel
    // from the listener's parent token.
    let simplex_child_cancel = cancel_token.child_token();
    let duplex_child_cancel = cancel_token.child_token();

    let dispatch = {
        let simplex_tx = simplex_tx.clone();
        let simplex_sessions = Arc::clone(&simplex_sessions);
        let simplex_stream_state = Arc::clone(&simplex_stream_state);
        let duplex_sessions = Arc::clone(&duplex_sessions);
        let duplex_accept_tx = duplex_accept_tx.clone();
        let simplex_cancel = simplex_child_cancel.clone();
        let duplex_cancel = duplex_child_cancel.clone();
        let dest = channel_addr.clone();
        move |link_init: super::LinkInit, stream: Box<dyn Stream>| {
            let simplex_tx = simplex_tx.clone();
            let simplex_sessions = Arc::clone(&simplex_sessions);
            let simplex_stream_state = Arc::clone(&simplex_stream_state);
            let duplex_sessions = Arc::clone(&duplex_sessions);
            let duplex_accept_tx = duplex_accept_tx.clone();
            let s_cancel = simplex_cancel.child_token();
            let d_cancel = duplex_cancel.child_token();
            let dest = dest.clone();
            async move {
                match link_init.kind {
                    ProtocolKind::Simplex => {
                        let streams = resolve_stream(&simplex_stream_state, &link_init);
                        dispatch_stream(
                            link_init.session_id,
                            streams,
                            stream,
                            &simplex_sessions,
                            dest,
                            simplex_tx,
                            s_cancel,
                        )
                        .await;
                    }
                    ProtocolKind::Duplex => {
                        dispatch_duplex_stream::<In, Out>(
                            link_init.session_id,
                            stream,
                            duplex_sessions,
                            dest,
                            &duplex_accept_tx,
                            d_cancel,
                        )
                        .await;
                    }
                }
            }
        }
    };

    let accept_ca = channel_addr.clone();
    let cancel_after = cancel_token.clone();
    // Real accept-loop task. Wrapped in a coordinator below that
    // surfaces the listener's drain as a watch signal so the per-half
    // `ServerHandle`s can honor `Rx::join`'s "all pending acks
    // flushed" contract.
    let inner_accept = tokio::spawn(async move {
        let result = accept_loop(&mut listener, &accept_ca, &child_token, prepare, dispatch).await;
        // If `accept_loop` returned for a reason other than parent
        // cancellation (e.g., listener error), cancelling here cascades
        // to the per-session children and tears any survivors down.
        cancel_after.cancel();
        result
    });

    // `drained_tx` flips to `true` once the accept loop has joined
    // every per-session task — i.e., every dispatch's terminal cleanup
    // (final ack flush + `Closed` emit) has finished. Per-half
    // `ServerHandle::join` waits on this signal so `ChannelRx::join` and
    // `DuplexServer::join` actually observe the drain rather than
    // returning on bare cancellation.
    let (drained_tx, drained_rx) = watch::channel(false);
    let coord_addr = channel_addr.clone();
    let join_handle = tokio::spawn(async move {
        let result = match inner_accept.await {
            Ok(r) => r,
            Err(e) => Err(ServerError::Internal(coord_addr, anyhow::anyhow!(e))),
        };
        let _ = drained_tx.send(true);
        result
    });

    // Each half gets a drain-aware `ServerHandle` whose `stop` cancels
    // the shared listener and whose `await` resolves only after the
    // accept loop has fully drained. Drop on either half also cancels
    // (via [`ChannelRx::Drop`] / [`DuplexServer::Drop`]).
    let simplex_handle = drain_aware_handle(
        cancel_token.clone(),
        drained_rx.clone(),
        channel_addr.clone(),
    );
    let duplex_handle = drain_aware_handle(cancel_token.clone(), drained_rx, channel_addr.clone());

    let net_rx = ChannelRx {
        receiver: simplex_rx,
        dest: channel_addr.clone(),
        server: simplex_handle,
    };
    let duplex_server =
        DuplexServer::from_parts(duplex_accept_rx, duplex_handle, channel_addr.clone());

    Ok(MuxServeParts {
        addr: channel_addr,
        simplex: net_rx,
        duplex: duplex_server,
        join_handle,
        cancel: cancel_token,
    })
}

/// Build a [`ServerHandle`] for a muxed half. `stop` cancels the
/// shared listener; `await` resolves only after both the cancel fires
/// and the accept-loop coordinator has signaled `drained` — i.e.,
/// every dispatch's terminal cleanup (final ack flush + `Closed` emit)
/// has finished. This honors the [`Rx::join`](super::Rx::join)
/// contract for [`ChannelRx`] and the equivalent contract for
/// [`DuplexServer::join`](DuplexServer::join), neither of which would
/// be satisfied by a bare cancel-only handle.
fn drain_aware_handle(
    cancel: CancellationToken,
    drained: watch::Receiver<bool>,
    addr: ChannelAddr,
) -> ServerHandle {
    let cancel_wait = cancel.clone();
    let mut drained = drained;
    let join = tokio::spawn(async move {
        cancel_wait.cancelled().await;
        let _ = drained.wait_for(|d| *d).await;
        Ok::<(), ServerError>(())
    });
    ServerHandle::new(join, cancel, addr)
}

/// Test-only variant of [`serve`] that accepts an arbitrary `Listener`.
/// Mirrors [`server::serve_with_listener`](super::server::serve_with_listener)
/// and [`duplex::serve_with_listener`](super::duplex::serve_with_listener):
/// used by mock-link tests that drive the mux through in-memory
/// `DuplexStream`s instead of going through `net::listen_with_prebound`.
///
/// The returned [`MuxServeParts`] expose the same shape as production
/// `serve`, including drain-aware per-half handles and the coordinator's
/// drained-watch signal — so wire-level tests can exercise the
/// `Rx::join` ack-flush contract and the shutdown ordering.
#[cfg(test)]
pub(super) fn serve_with_listener<M, In, Out, L>(
    listener: L,
    channel_addr: ChannelAddr,
) -> Result<MuxServeParts<M, In, Out>, ServerError>
where
    M: RemoteMessage,
    In: RemoteMessage,
    Out: RemoteMessage,
    L: super::Listener + 'static,
    L::Stream: Unpin + std::fmt::Debug + 'static,
{
    // Box the test stream and read the LinkInit; mirrors production
    // `preparer_for(_, None)` but without the TLS branches that
    // `preparer_for` carries for real transports.
    let prepare = |stream: L::Stream, source: ChannelAddr| async move {
        let mut boxed: Box<dyn Stream> = Box::new(stream);
        let link_init = super::read_link_init(&mut boxed)
            .await
            .map_err(|e| anyhow::anyhow!("LinkInit read failed from {}: {}", source, e))?;
        Ok((link_init, boxed))
    };
    serve_with_prepare(listener, channel_addr, prepare)
}

#[cfg(test)]
mod tests {
    use timed_test::async_timed_test;

    use crate::channel;
    use crate::channel::ChannelAddr;
    use crate::channel::ChannelTransport;
    use crate::channel::Rx;
    use crate::channel::Tx;

    /// A simplex client dialing a muxed listener delivers into the
    /// frontend's simplex half.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_routes_simplex_to_net_rx() {
        let mut frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();

        let tx = channel::dial::<u64>(frontend.addr().clone()).unwrap();
        tx.post(42);

        let received = tokio::time::timeout(Duration::from_secs(5), frontend.simplex_mut().recv())
            .await
            .expect("simplex recv timed out")
            .expect("simplex recv failed");
        assert_eq!(received, 42);
    }

    /// A duplex client dialing a muxed listener pops out of the
    /// frontend's duplex half and round-trips messages on both halves
    /// of the link.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_routes_duplex_to_duplex_server() {
        let mut frontend =
            channel::serve_mux::<u64, u64, String>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();

        // Client side: dials as duplex.
        let mut client = channel::duplex::dial::<u64, String>(frontend.addr().clone()).unwrap();
        let client_tx = client.tx();
        let mut client_rx = client.take_rx().unwrap();

        // Server side: accept the new session.
        let (mut server_rx, server_tx) =
            tokio::time::timeout(Duration::from_secs(5), frontend.duplex_mut().accept())
                .await
                .expect("accept timed out")
                .expect("accept failed");

        // Client → server.
        client_tx.post(7);
        let got = tokio::time::timeout(Duration::from_secs(5), server_rx.recv())
            .await
            .expect("server recv timed out")
            .expect("server recv failed");
        assert_eq!(got, 7);

        // Server → client.
        server_tx.post("hello".to_string());
        let got = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
            .await
            .expect("client recv timed out")
            .expect("client recv failed");
        assert_eq!(got, "hello");
    }

    /// A muxed listener handles concurrent simplex and duplex traffic
    /// on the same address, with each kind dispatched to the correct
    /// half by [`ProtocolKind`].
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_simplex_and_duplex_share_address() {
        let mut frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();

        let simplex_tx = channel::dial::<u64>(frontend.addr().clone()).unwrap();
        simplex_tx.post(1);

        let mut duplex_client = channel::duplex::dial::<u64, u64>(frontend.addr().clone()).unwrap();
        let duplex_tx = duplex_client.tx();
        let _duplex_rx = duplex_client.take_rx().unwrap();
        duplex_tx.post(2);

        let from_simplex =
            tokio::time::timeout(Duration::from_secs(5), frontend.simplex_mut().recv())
                .await
                .expect("simplex recv timed out")
                .expect("simplex recv failed");
        assert_eq!(from_simplex, 1);

        let (mut server_rx, _server_tx) =
            tokio::time::timeout(Duration::from_secs(5), frontend.duplex_mut().accept())
                .await
                .expect("duplex accept timed out")
                .expect("duplex accept failed");
        let from_duplex = tokio::time::timeout(Duration::from_secs(5), server_rx.recv())
            .await
            .expect("duplex recv timed out")
            .expect("duplex recv failed");
        assert_eq!(from_duplex, 2);
    }

    /// Splitting a [`MuxServer`] hands out the parts; dropping the
    /// returned [`MuxShutdown`] cancels the shared listener and
    /// terminates the simplex half's `recv`.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_split_shutdown_drops_listener() {
        let frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();
        let (_addr, mut simplex, _duplex, shutdown) = frontend.split();

        // Drop the shutdown guard; the simplex receiver should observe
        // a closed channel rather than hang forever.
        drop(shutdown);

        let result = tokio::time::timeout(Duration::from_secs(5), simplex.recv()).await;
        assert!(
            matches!(result, Ok(Err(_))),
            "simplex recv should fail with channel closed after shutdown",
        );
    }

    /// Mismatched protocols are rejected at the (non-muxed) sibling
    /// servers: a duplex client dialing a simplex-only [`channel::serve`]
    /// fails (and vice versa). The muxed listener accepts both; this
    /// guards the per-kind enforcement that the mux design relies on.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mismatched_protocol_kinds_are_rejected() {
        // Simplex server, duplex client — should not produce a usable session.
        let (simplex_addr, _simplex_rx) =
            channel::serve::<u64>(ChannelAddr::any(ChannelTransport::Unix)).unwrap();
        let mut duplex_client = channel::duplex::dial::<u64, u64>(simplex_addr).unwrap();
        let duplex_tx = duplex_client.tx();
        let mut duplex_rx = duplex_client.take_rx().unwrap();
        duplex_tx.post(99);
        let result = tokio::time::timeout(Duration::from_secs(2), duplex_rx.recv()).await;
        assert!(
            !matches!(result, Ok(Ok(_))),
            "duplex dial against simplex server should not deliver",
        );

        // Duplex server, simplex client — same shape.
        let mut duplex_server =
            channel::net::duplex::serve::<u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();
        let duplex_addr = duplex_server.addr().clone();
        let simplex_tx = channel::dial::<u64>(duplex_addr).unwrap();
        simplex_tx.post(99);
        let result = tokio::time::timeout(Duration::from_secs(2), duplex_server.accept()).await;
        assert!(
            !matches!(result, Ok(Ok(_))),
            "simplex dial against duplex server should not produce an accepted session",
        );
    }

    /// Regression test for the shutdown deadlock: with simplex and
    /// duplex sessions actively dispatched (their per-connection tasks
    /// blocked in `tokio::select!` on the session cancel), calling
    /// [`MuxShutdown::stop`] must propagate cancellation through the
    /// per-session children so the accept loop's `connections`
    /// drain completes and the listener task exits promptly.
    ///
    /// If the per-session cancel tokens were independent of
    /// `cancel_token`, the accept loop would wait on connections that
    /// are themselves waiting on tokens that never fire — an unbounded
    /// hang.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_shutdown_completes_with_active_sessions() {
        let mut frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();
        let addr = frontend.addr().clone();

        // Open both kinds of sessions and route at least one message
        // through each so the dispatch tasks are spawned and parked on
        // their session cancels.
        let simplex_tx = channel::dial::<u64>(addr.clone()).unwrap();
        simplex_tx.post(1);
        let mut duplex_client = channel::duplex::dial::<u64, u64>(addr).unwrap();
        let duplex_tx = duplex_client.tx();
        let _duplex_rx = duplex_client.take_rx().unwrap();
        duplex_tx.post(2);

        tokio::time::timeout(Duration::from_secs(5), frontend.simplex_mut().recv())
            .await
            .expect("simplex recv timed out")
            .expect("simplex recv failed");
        let (_server_rx, _server_tx) =
            tokio::time::timeout(Duration::from_secs(5), frontend.duplex_mut().accept())
                .await
                .expect("duplex accept timed out")
                .expect("duplex accept failed");

        // Hold the client tx ends across shutdown so the server-side
        // dispatch tasks remain blocked on their cancel branches —
        // without the deadlock fix, the listener cannot drain.
        let (_addr, _simplex, _duplex, shutdown) = frontend.split();
        shutdown.stop("test shutdown");
        let result = tokio::time::timeout(Duration::from_secs(5), shutdown).await;
        assert!(
            result.is_ok(),
            "muxed listener shutdown hung with active sessions",
        );

        drop(simplex_tx);
        drop(duplex_tx);
    }

    /// After [`MuxServer::split`], dropping the simplex half cancels
    /// the shared shutdown via `ChannelRx`'s `Drop` impl, which calls
    /// `ServerHandle::stop` on the cloned cancel token. The shutdown
    /// handle then completes promptly.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_drop_simplex_cancels_shutdown() {
        let frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();
        let (_addr, simplex, _duplex, shutdown) = frontend.split();

        drop(simplex);

        let result = tokio::time::timeout(Duration::from_secs(5), shutdown).await;
        assert!(
            result.is_ok(),
            "shutdown should complete after dropping simplex half",
        );
    }

    /// After [`MuxServer::split`], dropping the duplex half cancels
    /// the shared shutdown via `DuplexServer`'s `Drop` impl, which
    /// calls `handle.stop()` on the cloned cancel token. The shutdown
    /// handle then completes promptly.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_drop_duplex_cancels_shutdown() {
        let frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();
        let (_addr, _simplex, duplex, shutdown) = frontend.split();

        drop(duplex);

        let result = tokio::time::timeout(Duration::from_secs(5), shutdown).await;
        assert!(
            result.is_ok(),
            "shutdown should complete after dropping duplex half",
        );
    }

    /// After [`MuxServer::split`], dropping the address alone has no
    /// effect on the listener — `ChannelAddr` is a plain value type
    /// with no resource ownership. Both halves remain usable.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_drop_addr_alone_does_not_shut_down() {
        let frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();
        let (addr, mut simplex, _duplex, shutdown) = frontend.split();

        let dial_addr = addr.clone();
        drop(addr);

        // The listener should still accept simplex traffic.
        let tx = channel::dial::<u64>(dial_addr).unwrap();
        tx.post(123);
        let received = tokio::time::timeout(Duration::from_secs(5), simplex.recv())
            .await
            .expect("simplex recv timed out")
            .expect("simplex recv failed");
        assert_eq!(received, 123);

        // Tear down explicitly to confirm the shutdown still resolves.
        shutdown.stop("test shutdown");
        let result = tokio::time::timeout(Duration::from_secs(5), shutdown).await;
        assert!(result.is_ok(), "shutdown should complete after stop");
    }

    /// Coverage gap from the AI review: the existing mux tests check
    /// that shutdown completes, but did not assert that the duplex
    /// peer actually observes a clean session close (driven by the
    /// dispatch's terminal `Closed` frame) after the mux is stopped.
    /// This complements the deeper wire-level ack/Closed test in
    /// `channel/net.rs`'s frame-flush suite, which would need a
    /// `mux::serve_with_listener` test helper to exercise; here we
    /// pin the public-API contract that survives the shutdown
    /// reorder.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_mux_shutdown_signals_clean_close_to_duplex_peer() {
        use crate::mailbox::MailboxServerError;
        use crate::mailbox::MailboxServerHandle;

        async fn wait_for_stop(mut stopped_rx: tokio::sync::watch::Receiver<bool>) {
            let ok = stopped_rx.wait_for(|stopped| *stopped).await.is_ok();
            if !ok {
                std::future::pending::<()>().await;
            }
        }

        fn passthrough_mailbox_handle(mut rx: channel::ChannelRx<u64>) -> MailboxServerHandle {
            let (stopped_tx, mut stopped_rx) = tokio::sync::watch::channel(false);
            let join_handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = rx.recv() => {
                            if result.is_err() { break; }
                        }
                        _ = stopped_rx.changed() => {
                            if *stopped_rx.borrow() { break; }
                        }
                    }
                }
                Ok::<(), MailboxServerError>(())
            });
            MailboxServerHandle::from_parts(join_handle, stopped_tx)
        }

        let frontend =
            channel::serve_mux::<u64, u64, u64>(ChannelAddr::any(ChannelTransport::Unix), None)
                .unwrap();
        let addr = frontend.addr().clone();

        let mut client = channel::duplex::dial::<u64, u64>(addr).unwrap();
        let client_tx = client.tx();
        let mut client_rx = client.take_rx().unwrap();

        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let (delivered_tx, delivered_rx) = tokio::sync::oneshot::channel();

        let mut accepted_tx = Some(accepted_tx);
        let mut delivered_tx = Some(delivered_tx);
        let handle = frontend.serve(
            passthrough_mailbox_handle,
            move |mut duplex_server, stop_rx| async move {
                let (mut server_rx, _server_tx) = duplex_server.accept().await.unwrap();
                accepted_tx.take().unwrap().send(()).unwrap();

                // Drain inbound messages until stop fires; this proves
                // the recv-side path was active up to the shutdown
                // point so any Closed frame is the result of the
                // shutdown drain, not a starved session.
                tokio::select! {
                    _ = wait_for_stop(stop_rx) => {}
                    msg = server_rx.recv() => {
                        let _ = msg;
                        delivered_tx.take().unwrap().send(()).unwrap();
                    }
                }

                duplex_server.stop("test mux clean close");
                duplex_server.join().await;
            },
        );

        accepted_rx.await.unwrap();

        // Drive at least one message through so the session is live.
        client_tx.post(7);
        tokio::time::timeout(Duration::from_secs(5), delivered_rx)
            .await
            .expect("server did not see inbound message before stop")
            .unwrap();

        handle.stop("test mux clean close");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("mux handle did not stop")
            .expect("mux task panicked")
            .expect("mux task failed");

        // After the mux fully stops, the duplex peer's recv side must
        // observe a terminal close (`ChannelError::Closed`) rather
        // than hang. This is the application-visible consequence of
        // the dispatch's terminal `Closed` frame being delivered
        // during shutdown drain.
        let result = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;
        assert!(
            matches!(result, Ok(Err(_))),
            "client_rx should observe a terminal close after mux shutdown, got {:?}",
            result,
        );
    }
}
