/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! QUIC transport for connecting actors across machines (or local processes).
//!
//! Structurally a sibling of the UNIX transport: a connection's coroutines bring
//! up one bidirectional QUIC stream, produce a [`ConnectionTransport`] for it, and
//! hand it to the command loop via `Command::TransportConnected`; the reader
//! forwards every decoded frame back as a `ConnectionAction`. Establishment policy
//! (identity exchange, hello, liveness reporting) lives in the command loop and is
//! identical across transports. Wire framing is shared (see [`crate::framing`]).
//!
//! ## Why QUIC differs from UNIX
//!
//! QUIC runs over UDP in userspace, so there is no file-descriptor close to signal
//! a lost peer: a crashed, frozen, or partitioned peer simply stops sending. So
//! instead of relying on EOF we run an application-level **bidirectional
//! heartbeat**: each side's writer emits a [`framing::write_heartbeat`] every
//! [`HEARTBEAT_INTERVAL`], and each side's reader wraps every frame read in a
//! [`HEARTBEAT_TIMEOUT`]; any frame (data or heartbeat) refreshes the deadline, and
//! a timeout emits `Severed`. A clean drop still finishes the stream, so the peer
//! also observes immediate EOF — the heartbeat is the backstop for the unclean
//! cases.
//!
//! ## Security
//!
//! TLS material is taken from the environment (the "we will provide it" hook):
//! `MM_QUIC_CERT` / `MM_QUIC_KEY` (the cert chain + key this endpoint serves) and
//! `MM_QUIC_CA` (the authority a joiner trusts). The server presents its cert; the
//! client verifies it against the CA for the fixed server name [`SERVER_NAME`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;

use quinn::ClientConfig;
use quinn::Connection;
use quinn::Endpoint;
use quinn::RecvStream;
use quinn::SendStream;
use quinn::ServerConfig;
use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::MissedTickBehavior;

use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::connection::SideChannelMessage;
use crate::ctx::Command;
use crate::framing;
use crate::framing::Incoming;
use crate::framing::Preamble;
use crate::matcher::Matcher;
use crate::shm::MapperHandle;
use crate::shm::ShmClient;
use crate::shm::ShmClientSlot;
use crate::transport::Transport;

/// Server name the client uses to verify the server's certificate (the cert's SAN
/// must cover it). Fixed: routing/identity is handled above this layer.
const SERVER_NAME: &str = "monarch-mini";

/// Connect-retry backoff bounds. A join may precede its serve, and the QUIC
/// handshake fails until the server is bound, so the connector polls — fast at
/// first, backing off to a steady poll.
const CONNECT_RETRY_MIN: Duration = Duration::from_millis(5);
const CONNECT_RETRY_MAX: Duration = Duration::from_millis(1000);

/// How often each side emits a heartbeat, and how long a side waits for any frame
/// before declaring the connection broken. The timeout is several intervals so a
/// stray scheduling delay doesn't trip it.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(250);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);

/// What a connection's reader needs for shared memory: the context-global mapper
/// and the owning actor's gateway-client slot. Mirrors the unix transport's
/// `ShmCtx`. Only the quic *reader* uses it — to read a large incoming part
/// straight into a slab block (every actor on a quic link is a gateway, so it has
/// a client). The writer needs nothing: a `Shm` part carries its own mapper.
#[derive(Clone)]
struct ShmCtx {
    mapper: MapperHandle,
    client: ShmClientSlot,
}

impl ShmCtx {
    /// Snapshot the owning actor's gateway client (`None` until it is learned; a
    /// gateway seeds its own at creation, before any quic frame can arrive).
    fn client(&self) -> Option<ShmClient> {
        *self.client.lock().expect("shm client slot mutex poisoned")
    }
}

/// Process-wide rustls crypto provider (ring), installed once before any config is
/// built. Ignoring the result is intentional: a competing install is fine.
fn ensure_crypto_provider() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Server + client TLS configs, built once from the environment and shared by all
/// quic serves/joins in this context.
struct TlsConfig {
    server: ServerConfig,
    client: ClientConfig,
}

fn load_certs(path: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let data = std::fs::read(path).map_err(|err| anyhow::anyhow!("reading {path}: {err}"))?;
    let certs = rustls_pemfile::certs(&mut &data[..]).collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(!certs.is_empty(), "no certificates in {path}");
    Ok(certs)
}

fn load_key(path: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    let data = std::fs::read(path).map_err(|err| anyhow::anyhow!("reading {path}: {err}"))?;
    rustls_pemfile::private_key(&mut &data[..])?
        .ok_or_else(|| anyhow::anyhow!("no private key in {path}"))
}

fn load_tls() -> anyhow::Result<TlsConfig> {
    ensure_crypto_provider();
    let cert_path =
        std::env::var("MM_QUIC_CERT").map_err(|_| anyhow::anyhow!("MM_QUIC_CERT not set"))?;
    let key_path =
        std::env::var("MM_QUIC_KEY").map_err(|_| anyhow::anyhow!("MM_QUIC_KEY not set"))?;
    let ca_path = std::env::var("MM_QUIC_CA").map_err(|_| anyhow::anyhow!("MM_QUIC_CA not set"))?;

    let server = ServerConfig::with_single_cert(load_certs(&cert_path)?, load_key(&key_path)?)?;

    let mut roots = RootCertStore::empty();
    for ca in load_certs(&ca_path)? {
        roots.add(ca)?;
    }
    let client = ClientConfig::with_root_certificates(Arc::new(roots))?;

    Ok(TlsConfig { server, client })
}

fn parse_addr(url: &str) -> anyhow::Result<SocketAddr> {
    let authority = url.strip_prefix("quic://").unwrap_or(url);
    authority
        .parse::<SocketAddr>()
        .map_err(|err| anyhow::anyhow!("invalid quic address {authority:?}: {err}"))
}

/// Owns all QUIC transport state and coroutines. Mirrors `UnixTransport`: the
/// command loop holds one and forwards serves/joins to it; it never sees streams
/// or pairing state. TLS configs are built lazily on first use and cached.
pub(crate) struct QuicTransport {
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown_tx: watch::Sender<bool>,
    // The context-global address-space mapper, captured once at construction and
    // handed to every connection's reader so a large incoming part can be read
    // straight into a slab block.
    mapper: MapperHandle,
    // The context's single shm client slot, used *only* by side-channel readers,
    // which are not tied to any actor. A join/serve connection reads into its
    // owning actor's own slot instead (passed through serve/join).
    context_shm: ShmClientSlot,
    // One listener coroutine per url; serve connections are forwarded to it and it
    // owns the serve/accept pairing.
    listeners: HashMap<String, mpsc::UnboundedSender<(ConnectionRef, ShmCtx)>>,
    // One side-channel writer per remote gateway, keyed by its dial address (the
    // `@specifier` tag). Each owns a task that lazily connects (retrying until the
    // gateway is live), streams message frames, and reconnects if the connection
    // drops. Never heartbeated; cached across messages.
    side_channels: HashMap<String, mpsc::UnboundedSender<SideChannelMessage>>,
    tls: Option<Arc<TlsConfig>>,
    // Liveness-token issuer: each connection's writer holds a clone for its
    // lifetime. Teardown drops this issuing copy and waits for `alive_rx` to close
    // — i.e. for every writer to have flushed and exited.
    alive_tx: Option<mpsc::UnboundedSender<()>>,
    alive_rx: mpsc::UnboundedReceiver<()>,
}

impl QuicTransport {
    pub(crate) fn new(
        loop_tx: mpsc::UnboundedSender<Command>,
        mapper: MapperHandle,
        context_shm: ShmClientSlot,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let (alive_tx, alive_rx) = mpsc::unbounded_channel();
        Self {
            loop_tx,
            shutdown_tx,
            mapper,
            context_shm,
            listeners: HashMap::new(),
            side_channels: HashMap::new(),
            tls: None,
            alive_tx: Some(alive_tx),
            alive_rx,
        }
    }

    /// A connection's shared-memory context: the context mapper paired with the
    /// given client slot. A join/serve connection passes its owning actor's slot; a
    /// side-channel passes [`Self::context_shm`].
    fn shm_ctx(&self, client: ShmClientSlot) -> ShmCtx {
        ShmCtx {
            mapper: self.mapper.clone(),
            client,
        }
    }

    fn alive_token(&self) -> mpsc::UnboundedSender<()> {
        self.alive_tx
            .as_ref()
            .expect("alive-token issuer present before shutdown")
            .clone()
    }

    /// Build (or return the cached) TLS configs from the environment.
    fn tls(&mut self) -> anyhow::Result<Arc<TlsConfig>> {
        if let Some(tls) = &self.tls {
            return Ok(tls.clone());
        }
        let tls = Arc::new(load_tls()?);
        self.tls = Some(tls.clone());
        Ok(tls)
    }

    /// Send a self-addressing [`SideChannelMessage`] to a remote gateway over a
    /// direct side-channel, opening (and caching) the connection on first use. The
    /// caller (the command loop) has already derived `tag` from the message's
    /// `gateway_for_actor`. The side-channel is best-effort and not heartbeated; a
    /// message enqueued while the remote gateway is not yet live waits for the
    /// connection to come up.
    pub(crate) fn send_to_gateway(&mut self, tag: String, message: SideChannelMessage) {
        if !self.side_channels.contains_key(&tag) {
            let client = match self.tls() {
                Ok(tls) => tls.client.clone(),
                Err(err) => {
                    tracing::warn!("side channel tls: {err:#}");
                    return;
                }
            };
            let addr = match parse_addr(&tag) {
                Ok(addr) => addr,
                Err(err) => {
                    tracing::warn!("side channel address: {err:#}");
                    return;
                }
            };
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::task::spawn_local(side_channel_writer_task(
                addr,
                client,
                rx,
                self.shutdown_tx.subscribe(),
                self.alive_token(),
            ));
            self.side_channels.insert(tag.clone(), tx);
        }
        let _ = self
            .side_channels
            .get(&tag)
            .expect("side channel just inserted")
            .send(message);
    }

    /// Signal every writer to flush and exit, then wait until they all have — the
    /// same teardown contract as the UNIX transport.
    pub(crate) async fn shutdown(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.alive_tx = None;
        let _ = self.alive_rx.recv().await;
    }
}

impl Transport for QuicTransport {
    fn serve(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot) {
        let tls = match self.tls() {
            Ok(tls) => tls,
            Err(err) => {
                sever(
                    &self.loop_tx,
                    connection,
                    format!("quic tls: {err:#}").into_bytes(),
                );
                return;
            }
        };
        // The first serve on a url spawns its listener coroutine; later serves just
        // forward another connection to it. The listener carries the *context* shm
        // context for side-channel readers (which have no owning actor); each Join
        // connection instead uses the owning actor's slot, forwarded alongside it.
        if !self.listeners.contains_key(&url) {
            let addr = match parse_addr(&url) {
                Ok(addr) => addr,
                Err(err) => {
                    sever(&self.loop_tx, connection, format!("{err:#}").into_bytes());
                    return;
                }
            };
            let (tx, rx) = mpsc::unbounded_channel();
            let context_shm = self.shm_ctx(self.context_shm.clone());
            tokio::task::spawn_local(listener_task(
                addr,
                tls.server.clone(),
                rx,
                self.loop_tx.clone(),
                self.shutdown_tx.subscribe(),
                self.alive_token(),
                context_shm,
            ));
            self.listeners.insert(url.clone(), tx);
        }
        let shm = self.shm_ctx(shm_client);
        let _ = self
            .listeners
            .get(&url)
            .expect("listener just inserted")
            .send((connection, shm));
    }

    fn join(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot) {
        let tls = match self.tls() {
            Ok(tls) => tls,
            Err(err) => {
                sever(
                    &self.loop_tx,
                    connection,
                    format!("quic tls: {err:#}").into_bytes(),
                );
                return;
            }
        };
        let addr = match parse_addr(&url) {
            Ok(addr) => addr,
            Err(err) => {
                sever(&self.loop_tx, connection, format!("{err:#}").into_bytes());
                return;
            }
        };
        tokio::task::spawn_local(connector_task(
            addr,
            tls.client.clone(),
            connection,
            self.loop_tx.clone(),
            self.shutdown_tx.subscribe(),
            self.alive_token(),
            self.shm_ctx(shm_client),
        ));
    }
}

/// A connection accepted by the listener, classified by the joiner's preamble.
/// A `Join` is paired with a serve and driven by the command loop as before; a
/// `SideChannel` is a gateway-to-gateway message stream read directly into the
/// home gateway's routing.
enum Accepted {
    Join(Connection, SendStream, RecvStream),
    SideChannel(Connection, RecvStream),
}

/// Bind a QUIC server endpoint on `addr` and dispatch each accepted connection by
/// its preamble: a `Join` is paired with the next queued serve (either may arrive
/// first); a `SideChannel` is read straight into the context's gateway routing. On
/// a bind failure the serves are severed instead. Stops on teardown or when the
/// command loop drops the serve sender. `side_channel_shm` is the *context* shm
/// context, used only for side-channel readers (which have no owning actor); each
/// Join connection instead uses the per-serve shm forwarded with it.
async fn listener_task(
    addr: SocketAddr,
    server_config: ServerConfig,
    mut serves: mpsc::UnboundedReceiver<(ConnectionRef, ShmCtx)>,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    side_channel_shm: ShmCtx,
) {
    let endpoint = match Endpoint::server(server_config, addr) {
        Ok(endpoint) => endpoint,
        Err(err) => {
            // Bind failed (e.g. port in use). The url is dead for serving; stay alive
            // and fail every serve on it until teardown rather than respawn-retrying.
            let reason = format!("quic bind failed: {err}").into_bytes();
            loop {
                tokio::select! {
                    serve = serves.recv() => match serve {
                        Some((connection, _shm)) => sever(&loop_tx, connection, reason.clone()),
                        None => return,
                    },
                    _ = shutdown.changed() => return,
                }
            }
        }
    };

    // Accepting a connection, its stream, and its preamble is a multi-step
    // handshake; run it off the pairing loop so a slow handshake doesn't stall
    // matching. Each classified connection lands on `accepted_rx`.
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel();
    tokio::task::spawn_local(acceptor_task(
        endpoint.clone(),
        accepted_tx,
        shutdown.clone(),
    ));

    let mut matcher: Matcher<(ConnectionRef, ShmCtx), (Connection, SendStream, RecvStream)> =
        Matcher::new();
    loop {
        tokio::select! {
            serve = serves.recv() => {
                let Some((connection, shm)) = serve else { return; };
                let _ = matcher.push_left((connection, shm), |(connection, shm), (conn, send, recv)| {
                    spawn_connection(
                        send, recv, connection,
                        loop_tx.clone(), shutdown.clone(), alive.clone(),
                        KeepAlive { _endpoint: None, _connection: conn },
                        shm,
                    )
                });
            }
            accepted = accepted_rx.recv() => {
                match accepted {
                    None => return,
                    Some(Accepted::Join(conn, send, recv)) => {
                        let _ = matcher.push_right((conn, send, recv), |(connection, shm), (conn, send, recv)| {
                            spawn_connection(
                                send, recv, connection,
                                loop_tx.clone(), shutdown.clone(), alive.clone(),
                                KeepAlive { _endpoint: None, _connection: conn },
                                shm,
                            )
                        });
                    }
                    Some(Accepted::SideChannel(conn, recv)) => {
                        tokio::task::spawn_local(side_channel_reader_task(
                            conn, recv, loop_tx.clone(), side_channel_shm.clone(),
                        ));
                    }
                }
            }
            _ = shutdown.changed() => return,
        }
    }
    // `endpoint` is held for the whole task, keeping the server socket (and every
    // accepted connection's driver) alive until teardown.
}

/// Accept connections on `endpoint`, accept the one bi-stream each joiner opens,
/// read its preamble, and forward the classified result. Each handshake runs in
/// its own task so one slow peer doesn't block others.
async fn acceptor_task(
    endpoint: Endpoint,
    accepted_tx: mpsc::UnboundedSender<Accepted>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { return; }; // endpoint closed
                let accepted_tx = accepted_tx.clone();
                tokio::task::spawn_local(async move {
                    let Ok(connecting) = incoming.accept() else { return; };
                    let Ok(connection) = connecting.await else { return; };
                    let Ok((send, mut recv)) = connection.accept_bi().await else { return; };
                    // The joiner's first frame says what this stream is for.
                    let accepted = match framing::read_preamble(&mut recv).await {
                        Ok(Preamble::Join) => Accepted::Join(connection, send, recv),
                        Ok(Preamble::SideChannel) => Accepted::SideChannel(connection, recv),
                        Err(_) => return,
                    };
                    let _ = accepted_tx.send(accepted);
                });
            }
            _ = shutdown.changed() => return,
        }
    }
}

/// Read message frames off a gateway side-channel and forward each to the command
/// loop, which resolves the owning gateway from the destination. There is no
/// heartbeat and no establishment: an EOF or error just ends the reader (the
/// sending gateway reconnects when it next has something to send). `_conn` is held
/// only to keep the QUIC connection (and thus `recv`) alive.
async fn side_channel_reader_task(
    _conn: Connection,
    mut recv: RecvStream,
    loop_tx: mpsc::UnboundedSender<Command>,
    shm: ShmCtx,
) {
    loop {
        match framing::read_side_channel(&mut recv, &shm.mapper, shm.client()).await {
            Ok(message) => {
                if loop_tx.send(Command::SideChannelDeliver(message)).is_err() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

/// Connect to `addr`, retrying until the server binds and the handshake succeeds
/// (so a join may precede its serve), then open one bi-stream and wire it up.
async fn connector_task(
    addr: SocketAddr,
    client_config: ClientConfig,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
) {
    let endpoint = match Endpoint::client("0.0.0.0:0".parse().expect("valid bind addr")) {
        Ok(mut endpoint) => {
            endpoint.set_default_client_config(client_config);
            endpoint
        }
        Err(err) => {
            sever(
                &loop_tx,
                connection,
                format!("quic client bind failed: {err}").into_bytes(),
            );
            return;
        }
    };

    let mut retry = CONNECT_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return;
        }
        // connect() fails synchronously on a bad config; the handshake (.await)
        // fails until the server is up. Both just back off and retry.
        let connected = match endpoint.connect(addr, SERVER_NAME) {
            Ok(connecting) => connecting.await.ok(),
            Err(_) => None,
        };
        if let Some(conn) = connected {
            match conn.open_bi().await {
                Ok((mut send, recv)) => {
                    // Tell the acceptor this is an ordinary join (not a side-channel)
                    // before the command loop drives establishment over the stream.
                    if framing::write_preamble(&mut send, Preamble::Join)
                        .await
                        .is_err()
                    {
                        sever(&loop_tx, connection, b"quic open stream failed".to_vec());
                        return;
                    }
                    spawn_connection(
                        send,
                        recv,
                        connection,
                        loop_tx,
                        shutdown,
                        alive,
                        KeepAlive {
                            _endpoint: Some(endpoint),
                            _connection: conn,
                        },
                        shm,
                    );
                    return;
                }
                Err(_) => {
                    sever(&loop_tx, connection, b"quic open stream failed".to_vec());
                    return;
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(retry) => {}
            _ = shutdown.changed() => return,
        }
        retry = (retry * 2).min(CONNECT_RETRY_MAX);
    }
}

/// Drive a gateway side-channel writer: drain queued message commands, writing
/// each as a frame to the remote gateway. The connection is established lazily on
/// the first message (retrying until the gateway is live) and reconnected if it
/// drops — there is no heartbeat, so a dropped connection is noticed on the next
/// write. A message in flight when the connection drops may be lost; the channel
/// is best-effort by design (any gateway may drop a side-channel to reclaim state).
async fn side_channel_writer_task(
    addr: SocketAddr,
    client_config: ClientConfig,
    mut rx: mpsc::UnboundedReceiver<SideChannelMessage>,
    mut shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
) {
    let endpoint = match Endpoint::client("0.0.0.0:0".parse().expect("valid bind addr")) {
        Ok(mut endpoint) => {
            endpoint.set_default_client_config(client_config);
            endpoint
        }
        Err(err) => {
            tracing::warn!("side channel client bind failed: {err}");
            return;
        }
    };

    // The current connection, if up: its send stream plus a keep-alive holding the
    // QUIC connection open. `None` means we must (re)connect before the next write.
    let mut stream: Option<(SendStream, KeepAlive)> = None;
    loop {
        let message = tokio::select! {
            message = rx.recv() => match message {
                Some(message) => message,
                None => break, // sender dropped (gateway gone)
            },
            _ = shutdown.changed() => break,
        };
        if stream.is_none() {
            let Some((mut send, keep)) = connect_side_channel(&endpoint, addr, &mut shutdown).await
            else {
                break; // shutting down before the gateway came up
            };
            // Announce the stream as a side-channel so the peer reads messages
            // rather than driving a join handshake.
            if framing::write_preamble(&mut send, Preamble::SideChannel)
                .await
                .is_err()
            {
                continue; // reconnect on the next message (this one is dropped)
            }
            stream = Some((send, keep));
        }
        let (send, _keep) = stream.as_mut().expect("stream is connected");
        if framing::write_side_channel(send, message).await.is_err() {
            stream = None; // dropped; reconnect on the next message
        }
    }
    if let Some((mut send, _keep)) = stream {
        let _ = send.finish();
    }
}

/// Connect a side-channel to `addr`, retrying with backoff until the gateway binds
/// and a bi-stream opens, or until teardown (then `None`). Mirrors the join
/// connector's retry so a side-channel may be opened before its target gateway is
/// live.
async fn connect_side_channel(
    endpoint: &Endpoint,
    addr: SocketAddr,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<(SendStream, KeepAlive)> {
    let mut retry = CONNECT_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return None;
        }
        let connected = match endpoint.connect(addr, SERVER_NAME) {
            Ok(connecting) => connecting.await.ok(),
            Err(_) => None,
        };
        if let Some(conn) = connected {
            if let Ok((send, _recv)) = conn.open_bi().await {
                return Some((
                    send,
                    KeepAlive {
                        _endpoint: None,
                        _connection: conn,
                    },
                ));
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(retry) => {}
            _ = shutdown.changed() => return None,
        }
        retry = (retry * 2).min(CONNECT_RETRY_MAX);
    }
}

/// Wire up an established bi-stream: build its [`QuicConnectionTransport`], announce
/// it to the command loop, and spawn the writer (drains commands → frames, plus a
/// heartbeat tick) and reader (frames → `ConnectionAction`, with a heartbeat
/// timeout). `keep` holds the QUIC connection (and, for a joiner, the client
/// endpoint) alive for the duration of the writer.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of per-connection state handed off to the writer or reader; bundling them adds indirection without clarifying anything"
)]
fn spawn_connection(
    send: SendStream,
    recv: RecvStream,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    keep: KeepAlive,
    shm: ShmCtx,
) {
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    let transport = Box::new(QuicConnectionTransport { tx: writer_tx });
    let _ = loop_tx.send(Command::TransportConnected {
        connection,
        transport,
    });
    tokio::task::spawn_local(writer_task(send, writer_rx, shutdown, alive, keep));
    tokio::task::spawn_local(reader_task(recv, connection, loop_tx, shm));
}

/// Keeps the QUIC connection (and a joiner's client endpoint) alive for as long as
/// the writer runs; dropping it closes them, which the peer observes.
struct KeepAlive {
    _endpoint: Option<Endpoint>,
    _connection: Connection,
}

/// Transport for one end of a QUIC stream: `send` hands a command to the writer
/// task. Dropping it ends the writer, which finishes the stream — the peer's reader
/// then sees EOF.
struct QuicConnectionTransport {
    tx: mpsc::UnboundedSender<ConnectionCommand>,
}

impl ConnectionTransport for QuicConnectionTransport {
    fn send(&self, action: ConnectionCommand) -> bool {
        self.tx.send(action).is_ok()
    }
}

/// Write each queued command as a frame, and a heartbeat every
/// [`HEARTBEAT_INTERVAL`] so the peer's reader keeps seeing traffic. On teardown,
/// drains queued frames first, then finishes the stream.
async fn writer_task(
    mut send: SendStream,
    mut rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    mut shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
    _keep: KeepAlive,
) {
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break; // transport dropped
                };
                if framing::write_command(&mut send, command).await.is_err() {
                    return;
                }
            }
            _ = heartbeat.tick() => {
                if framing::write_heartbeat(&mut send).await.is_err() {
                    return;
                }
            }
            _ = shutdown.changed() => {
                // Teardown: no further commands will be enqueued. Flush whatever is
                // already queued, then stop.
                while let Ok(command) = rx.try_recv() {
                    if framing::write_command(&mut send, command).await.is_err() {
                        return;
                    }
                }
                break;
            }
        }
    }
    // Finish the stream so the peer's reader sees EOF promptly (the fast path); the
    // heartbeat timeout is the backstop when a peer dies without finishing.
    let _ = send.finish();
}

/// Decode each frame off the stream and forward it to the command loop. A read
/// that does not complete within [`HEARTBEAT_TIMEOUT`] — or an error/EOF — emits
/// `Severed`. Heartbeat frames refresh the deadline and are not forwarded.
async fn reader_task(
    mut recv: RecvStream,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    shm: ShmCtx,
) {
    loop {
        // The owning actor's gateway client is snapshot per frame so a large part
        // is read straight into the slab once the client is known (a gateway seeds
        // its own at creation, before any frame arrives).
        let read = framing::read_frame(&mut recv, &shm.mapper, shm.client());
        match tokio::time::timeout(HEARTBEAT_TIMEOUT, read).await {
            Err(_elapsed) => {
                sever(&loop_tx, connection, b"quic heartbeat timeout".to_vec());
                return;
            }
            Ok(Ok(Incoming::Command(action))) => {
                if loop_tx
                    .send(Command::ConnectionAction { connection, action })
                    .is_err()
                {
                    return;
                }
            }
            Ok(Ok(Incoming::Heartbeat)) => continue,
            Ok(Err(_)) => {
                sever(&loop_tx, connection, b"quic connection closed".to_vec());
                return;
            }
        }
    }
}

fn sever(loop_tx: &mpsc::UnboundedSender<Command>, connection: ConnectionRef, reason: Vec<u8>) {
    let _ = loop_tx.send(Command::ConnectionAction {
        connection,
        action: ConnectionCommand::Severed { reason },
    });
}
