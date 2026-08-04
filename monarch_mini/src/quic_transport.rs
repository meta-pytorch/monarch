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
use crate::ctx::Command;
use crate::framing;
use crate::framing::Incoming;
use crate::matcher::Matcher;
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
    // One listener coroutine per url; serve connections are forwarded to it and it
    // owns the serve/accept pairing.
    listeners: HashMap<String, mpsc::UnboundedSender<ConnectionRef>>,
    tls: Option<Arc<TlsConfig>>,
    // Liveness-token issuer: each connection's writer holds a clone for its
    // lifetime. Teardown drops this issuing copy and waits for `alive_rx` to close
    // — i.e. for every writer to have flushed and exited.
    alive_tx: Option<mpsc::UnboundedSender<()>>,
    alive_rx: mpsc::UnboundedReceiver<()>,
}

impl QuicTransport {
    pub(crate) fn new(loop_tx: mpsc::UnboundedSender<Command>) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let (alive_tx, alive_rx) = mpsc::unbounded_channel();
        Self {
            loop_tx,
            shutdown_tx,
            listeners: HashMap::new(),
            tls: None,
            alive_tx: Some(alive_tx),
            alive_rx,
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

    /// Signal every writer to flush and exit, then wait until they all have — the
    /// same teardown contract as the UNIX transport.
    pub(crate) async fn shutdown(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.alive_tx = None;
        let _ = self.alive_rx.recv().await;
    }
}

impl Transport for QuicTransport {
    fn serve(&mut self, url: String, connection: ConnectionRef, _shm_client: ShmClientSlot) {
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
        // forward another connection to it.
        if !self.listeners.contains_key(&url) {
            let addr = match parse_addr(&url) {
                Ok(addr) => addr,
                Err(err) => {
                    sever(&self.loop_tx, connection, format!("{err:#}").into_bytes());
                    return;
                }
            };
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::task::spawn_local(listener_task(
                addr,
                tls.server.clone(),
                rx,
                self.loop_tx.clone(),
                self.shutdown_tx.subscribe(),
                self.alive_token(),
            ));
            self.listeners.insert(url.clone(), tx);
        }
        let _ = self
            .listeners
            .get(&url)
            .expect("listener just inserted")
            .send(connection);
    }

    fn join(&mut self, url: String, connection: ConnectionRef, _shm_client: ShmClientSlot) {
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
        ));
    }
}

/// Bind a QUIC server endpoint on `addr` and pair each accepted connection's
/// stream with the next queued serve (either may arrive first). On a bind failure
/// the serves are severed instead. Stops on teardown or when the command loop
/// drops the serve sender.
async fn listener_task(
    addr: SocketAddr,
    server_config: ServerConfig,
    mut serves: mpsc::UnboundedReceiver<ConnectionRef>,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
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
                        Some(connection) => sever(&loop_tx, connection, reason.clone()),
                        None => return,
                    },
                    _ = shutdown.changed() => return,
                }
            }
        }
    };

    // Accepting a connection and its stream is a multi-step handshake; run it off
    // the pairing loop so a slow handshake doesn't stall matching. Each accepted
    // (connection, stream) lands on `accepted_rx`.
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel();
    tokio::task::spawn_local(acceptor_task(
        endpoint.clone(),
        accepted_tx,
        shutdown.clone(),
    ));

    let mut matcher: Matcher<ConnectionRef, (Connection, SendStream, RecvStream)> = Matcher::new();
    loop {
        tokio::select! {
            serve = serves.recv() => {
                let Some(connection) = serve else { return; };
                let _ = matcher.push_left(connection, |connection, (conn, send, recv)| {
                    spawn_connection(
                        send, recv, connection,
                        loop_tx.clone(), shutdown.clone(), alive.clone(),
                        KeepAlive { _endpoint: None, _connection: conn },
                    )
                });
            }
            accepted = accepted_rx.recv() => {
                let Some(triple) = accepted else { return; };
                let _ = matcher.push_right(triple, |connection, (conn, send, recv)| {
                    spawn_connection(
                        send, recv, connection,
                        loop_tx.clone(), shutdown.clone(), alive.clone(),
                        KeepAlive { _endpoint: None, _connection: conn },
                    )
                });
            }
            _ = shutdown.changed() => return,
        }
    }
    // `endpoint` is held for the whole task, keeping the server socket (and every
    // accepted connection's driver) alive until teardown.
}

/// Accept connections on `endpoint`, accept the one bi-stream each joiner opens,
/// and forward the result. Each handshake runs in its own task so one slow peer
/// doesn't block others.
async fn acceptor_task(
    endpoint: Endpoint,
    accepted_tx: mpsc::UnboundedSender<(Connection, SendStream, RecvStream)>,
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
                    if let Ok((send, recv)) = connection.accept_bi().await {
                        let _ = accepted_tx.send((connection, send, recv));
                    }
                });
            }
            _ = shutdown.changed() => return,
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
                Ok((send, recv)) => {
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

/// Wire up an established bi-stream: build its [`QuicConnectionTransport`], announce
/// it to the command loop, and spawn the writer (drains commands → frames, plus a
/// heartbeat tick) and reader (frames → `ConnectionAction`, with a heartbeat
/// timeout). `keep` holds the QUIC connection (and, for a joiner, the client
/// endpoint) alive for the duration of the writer.
fn spawn_connection(
    send: SendStream,
    recv: RecvStream,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    keep: KeepAlive,
) {
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    let transport = Box::new(QuicConnectionTransport { tx: writer_tx });
    let _ = loop_tx.send(Command::TransportConnected {
        connection,
        transport,
    });
    tokio::task::spawn_local(writer_task(send, writer_rx, shutdown, alive, keep));
    tokio::task::spawn_local(reader_task(recv, connection, loop_tx));
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
) {
    loop {
        match tokio::time::timeout(HEARTBEAT_TIMEOUT, framing::read_frame(&mut recv)).await {
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
