/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TCP implementation of the [`Net`] transport seam — the cross-region fallback.
//!
//! QUIC runs over UDP, which is silently packet-filter-dropped across regions, so a
//! devserver can only reach same-region QUIC workers. A TLS-over-TCP flow is what the
//! cross-region enforcer clears, so `tcp://` reaches workers in any region. It reuses
//! the same TLS material as quic (`MM_QUIC_CERT`/`MM_QUIC_KEY`/`MM_QUIC_CA`), verified
//! against the CA for the fixed server name [`SERVER_NAME`].
//!
//! Everything protocol-independent — establishment, heartbeats (on their own stream),
//! matching serves to joins, side channels, retry/backoff, shutdown — lives in
//! [`crate::net_transport`], generic over [`Net`]. This module supplies only the raw
//! networking. The command loop drives tcp as `NetTransport<Tcp>` (aliased
//! `TcpTransport` in [`crate::ctx`]).
//!
//! ## Streams: one socket each, paired by a prefix
//!
//! A QUIC connection multiplexes its two streams (data + heartbeat) on one transport
//! connection; TCP has no multiplexing, so each stream is its own TLS-over-TCP socket.
//! Unlike quic — where a stream is materialized lazily and the "first messenger" side
//! opens it — TCP must open **both** sockets up front on connect: the generic layer
//! may make either side the first messenger of a given stream, and a plain socket has
//! no way to be opened "from the accepting side". So the dialer opens all `streams`
//! sockets eagerly, each prefixed with `(connection_id, stream_index)`; the listener
//! reads that prefix and groups sockets with the same id into one logical connection,
//! ordered by index. [`NetConn::stream`] then just hands back the pre-opened sockets
//! in order (`first_messenger` is irrelevant — a socket is full-duplex). This is
//! cruder than the quic path (two sockets even for a message-only side channel, and a
//! slow TLS handshake head-of-line-blocks accept), but tcp is the fallback, so simple
//! and correct beats optimal.
//!
//! ## No transport keepalive
//!
//! A root opens a huge number of connections, so — exactly like the quic side, which
//! disables quinn's PING keep-alive and idle timeout — these sockets must carry **zero**
//! periodic per-connection traffic of their own. We never enable TCP keepalive
//! (`SO_KEEPALIVE`, off by default and left off), and TCP has no idle timeout, so an
//! idle connection stays open sending nothing. Liveness is entirely the heartbeat
//! subsystem's job (its own stream, and delegated so the root's steady-state cost is
//! bounded); a link with no heartbeat obligation — e.g. a delegated one — is genuinely
//! silent, which is what lets the connection count scale.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::future::Ready;
use std::hash::BuildHasher;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::ServerName;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;

use crate::net::Net;
use crate::net::NetConn;

/// Server name the client uses to verify the server's certificate (the cert's SAN
/// must cover it). Shared with the quic transport, which uses the same cert set.
const SERVER_NAME: &str = "monarch-mini";

/// How long a connection may stay half-paired — some of its sockets arrived, but not
/// all — before the listener reaps it, dropping the sockets that did arrive and freeing
/// their fds.
///
/// Deliberately **longer than the heartbeat timeout**: a connection whose sockets pair
/// (even slowly) finishes setup and starts heartbeating well within this window, so we
/// never reap a valid pairing; and once set up, an inactive link is severed by the
/// heartbeat subsystem, not by us. That makes a reap indistinguishable from "the
/// connection set up, then was severed for inactivity" — a path the peer already
/// handles — rather than a special failure. A truly abandoned pairing (a client that
/// could not open every socket, so its `connect` failed and the generic connector is
/// already retrying with a fresh id) is all that is left for us to clean up.
fn pending_reap_timeout() -> Duration {
    crate::heartbeat::heartbeat_timeout() * 2
}

/// The ring crypto provider, passed explicitly to every rustls config builder so this
/// transport does not depend on a process-global default being installed.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Server + client TLS configs, built once from the environment and shared by all tcp
/// serves/joins in this context.
struct TlsConfig {
    server: Arc<rustls::ServerConfig>,
    client: Arc<rustls::ClientConfig>,
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
    let cert_path =
        std::env::var("MM_QUIC_CERT").map_err(|_| anyhow::anyhow!("MM_QUIC_CERT not set"))?;
    let key_path =
        std::env::var("MM_QUIC_KEY").map_err(|_| anyhow::anyhow!("MM_QUIC_KEY not set"))?;
    let ca_path = std::env::var("MM_QUIC_CA").map_err(|_| anyhow::anyhow!("MM_QUIC_CA not set"))?;

    let server = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|err| anyhow::anyhow!("tls server versions: {err}"))?
        .with_no_client_auth()
        .with_single_cert(load_certs(&cert_path)?, load_key(&key_path)?)?;

    let mut roots = RootCertStore::empty();
    for ca in load_certs(&ca_path)? {
        roots.add(ca)?;
    }
    let client = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|err| anyhow::anyhow!("tls client versions: {err}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConfig {
        server: Arc::new(server),
        client: Arc::new(client),
    })
}

/// Parse a `tcp://host:port` (or bare `host:port`) url into a socket address.
fn parse_addr(url: &str) -> anyhow::Result<SocketAddr> {
    let authority = url.strip_prefix("tcp://").unwrap_or(url);
    authority
        .parse::<SocketAddr>()
        .map_err(|err| anyhow::anyhow!("invalid tcp address {authority:?}: {err}"))
}

/// A fresh, effectively-unique connection id used to pair the sockets of one logical
/// connection at the listener. 128 random bits (two independently-seeded `RandomState`
/// hashes) — a collision would require two live connections to a single server to draw
/// the same 128-bit id, which is negligible.
fn new_connection_id() -> u128 {
    let hi = std::collections::hash_map::RandomState::new().hash_one(0xA5u8) as u128;
    let lo = std::collections::hash_map::RandomState::new().hash_one(0x5Au8) as u128;
    (hi << 64) | lo
}

/// Write the socket-pairing prefix (`connection_id` then `stream_index`) that lets the
/// listener group the sockets of one logical connection. Precedes every generic frame.
async fn write_pairing_prefix<W: AsyncWrite + Unpin>(
    writer: &mut W,
    connection_id: u128,
    stream_index: usize,
) -> io::Result<()> {
    writer.write_all(&connection_id.to_le_bytes()).await?;
    writer.write_all(&[stream_index as u8]).await?;
    writer.flush().await
}

/// Read the socket-pairing prefix written by [`write_pairing_prefix`]. `None` on EOF
/// or a short read (a peer that spoke a bad dialect — the socket is dropped).
async fn read_pairing_prefix<R: AsyncRead + Unpin>(reader: &mut R) -> Option<(u128, usize)> {
    let mut id = [0u8; 16];
    reader.read_exact(&mut id).await.ok()?;
    let mut index = [0u8; 1];
    reader.read_exact(&mut index).await.ok()?;
    Some((u128::from_le_bytes(id), index[0] as usize))
}

/// A TLS-over-TCP stream, server- or client-side. Both appear as one `Net::Conn`
/// stream type, so they are unified in an enum that delegates the async IO traits.
pub(crate) enum TlsStream {
    Server(ServerTlsStream<TcpStream>),
    Client(ClientTlsStream<TcpStream>),
}

impl AsyncRead for TlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsStream::Server(s) => Pin::new(s).poll_read(cx, buf),
            TlsStream::Client(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TlsStream::Server(s) => Pin::new(s).poll_write(cx, buf),
            TlsStream::Client(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsStream::Server(s) => Pin::new(s).poll_flush(cx),
            TlsStream::Client(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsStream::Server(s) => Pin::new(s).poll_shutdown(cx),
            TlsStream::Client(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// The shared per-context TCP state: TLS configs, loaded lazily on first use.
pub(crate) struct Tcp {
    tls: Option<Arc<TlsConfig>>,
}

impl Tcp {
    fn tls(&mut self) -> anyhow::Result<Arc<TlsConfig>> {
        if let Some(tls) = &self.tls {
            return Ok(tls.clone());
        }
        let tls = Arc::new(load_tls()?);
        self.tls = Some(tls.clone());
        Ok(tls)
    }
}

/// A cheap, cloneable dialing handle: the TLS connector plus the fixed server name.
#[derive(Clone)]
pub(crate) struct TcpDialer {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

/// Connection ids mid-pairing, each mapped to its per-index sockets so far. A
/// completed connection is removed from the map (and handed up); an abandoned one is
/// removed by its [`reap_unpaired`] timeout coroutine.
type PendingMap = HashMap<u128, Vec<Option<TlsStream>>>;

/// A bound TCP server plus the pairing state accept needs. `accept` runs the TLS
/// handshake, reads each socket's pairing prefix, and groups sockets by connection id
/// until a logical connection has all its streams. `pending` is interior-mutable
/// because [`Net::accept`] takes `&self` (single-threaded, so the `RefCell` is never
/// contended and no borrow is held across an await); it is shared with the per-
/// connection [`reap_unpaired`] timeout coroutines via a `Weak`, so it drops (with any
/// half-paired sockets) as soon as the listener does.
pub(crate) struct TcpListenerHandle {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    streams: usize,
    pending: Rc<RefCell<PendingMap>>,
}

/// Reap `connection_id` if it never finishes pairing. Spawned when its first socket
/// arrives: after [`pending_reap_timeout`], if the id is still in the map it never
/// completed (its client abandoned the connect after opening only some sockets), so
/// drop it, freeing the orphaned sockets' fds. If the id is already gone, it completed
/// and there is nothing to do.
async fn reap_unpaired(pending: Weak<RefCell<PendingMap>>, connection_id: u128) {
    tokio::time::sleep(pending_reap_timeout()).await;
    if let Some(pending) = pending.upgrade() {
        pending.borrow_mut().remove(&connection_id);
    }
}

impl Net for Tcp {
    type Dialer = TcpDialer;
    type Listener = TcpListenerHandle;
    type Conn = TcpConn;

    fn create() -> anyhow::Result<Self> {
        Ok(Self { tls: None })
    }

    fn parse_addr(url: &str) -> anyhow::Result<SocketAddr> {
        parse_addr(url)
    }

    fn dialer(&mut self, _addr: SocketAddr) -> anyhow::Result<TcpDialer> {
        let connector = TlsConnector::from(self.tls()?.client.clone());
        let server_name = ServerName::try_from(SERVER_NAME)
            .map_err(|err| anyhow::anyhow!("tcp server name: {err}"))?
            .to_owned();
        Ok(TcpDialer {
            connector,
            server_name,
        })
    }

    fn bind(&mut self, addr: SocketAddr, streams: usize) -> anyhow::Result<TcpListenerHandle> {
        let acceptor = TlsAcceptor::from(self.tls()?.server.clone());
        // `bind` is synchronous (the command loop can't await); build the listener via
        // std and adopt it into tokio without an await.
        let std_listener = std::net::TcpListener::bind(addr)?;
        std_listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(std_listener)?;
        let pending: Rc<RefCell<PendingMap>> = Rc::new(RefCell::new(HashMap::new()));
        Ok(TcpListenerHandle {
            listener,
            acceptor,
            streams,
            pending,
        })
    }

    async fn accept(listener: &TcpListenerHandle) -> Option<TcpConn> {
        loop {
            let Ok((tcp, peer)) = listener.listener.accept().await else {
                continue; // transient accept error; the listener stays open
            };
            let Ok(mut stream) = listener.acceptor.accept(tcp).await.map(TlsStream::Server) else {
                continue; // TLS handshake failed
            };
            let Some((connection_id, index)) = read_pairing_prefix(&mut stream).await else {
                continue; // bad/short prefix
            };
            if index >= listener.streams {
                continue; // out-of-range index; drop this socket
            }
            // Group by connection id. No await is held across this borrow.
            let mut pending = listener.pending.borrow_mut();
            let is_new = !pending.contains_key(&connection_id);
            let slots = pending
                .entry(connection_id)
                .or_insert_with(|| (0..listener.streams).map(|_| None).collect());
            slots[index] = Some(stream);
            if slots.iter().all(Option::is_some) {
                // Complete: remove it (so its reaper sees it gone and knows it
                // completed) and hand it up.
                let slots = pending.remove(&connection_id).expect("just inserted");
                let streams = slots
                    .into_iter()
                    .map(|s| s.expect("all slots filled"))
                    .collect();
                return Some(TcpConn::new(peer, streams));
            }
            if is_new {
                // First socket for this id: bound how long it may stay half-paired.
                tokio::task::spawn_local(reap_unpaired(
                    Rc::downgrade(&listener.pending),
                    connection_id,
                ));
            }
        }
    }

    async fn connect(dialer: &TcpDialer, addr: SocketAddr, streams: usize) -> Option<TcpConn> {
        // Open every stream up front (see the module docs): the accepting side may be
        // the first messenger of a stream, and a plain socket can't be opened from the
        // accepting side. Each socket is prefixed so the listener can pair them.
        let connection_id = new_connection_id();
        let mut opened = Vec::with_capacity(streams);
        for index in 0..streams {
            let tcp = TcpStream::connect(addr).await.ok()?;
            let tls = dialer
                .connector
                .connect(dialer.server_name.clone(), tcp)
                .await
                .ok()?;
            let mut stream = TlsStream::Client(tls);
            write_pairing_prefix(&mut stream, connection_id, index)
                .await
                .ok()?;
            opened.push(stream);
        }
        Some(TcpConn::new(addr, opened))
    }
}

/// One logical TCP connection: the `streams` pre-opened, pairing-ordered sockets.
/// [`NetConn::stream`] hands them out in index order (`first_messenger` is irrelevant
/// — each socket is full-duplex, and both ends already hold their matching socket).
pub(crate) struct TcpConn {
    remote: SocketAddr,
    streams: RefCell<Vec<Option<TlsStream>>>,
    cursor: Cell<usize>,
}

impl TcpConn {
    fn new(remote: SocketAddr, streams: Vec<TlsStream>) -> Self {
        Self {
            remote,
            streams: RefCell::new(streams.into_iter().map(Some).collect()),
            cursor: Cell::new(0),
        }
    }
}

impl fmt::Debug for TcpConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TcpConn").field(&self.remote).finish()
    }
}

impl NetConn for TcpConn {
    type Send = WriteHalf<TlsStream>;
    type Recv = ReadHalf<TlsStream>;
    // The sockets are already open, so producing a stream is synchronous — a ready
    // future, never touching the wire (the generic layer just awaits it like quic's).
    type Stream = Ready<io::Result<(Self::Send, Self::Recv)>>;

    fn stream(&self, _first_messenger: bool) -> Self::Stream {
        let index = self.cursor.get();
        self.cursor.set(index + 1);
        let taken = self
            .streams
            .borrow_mut()
            .get_mut(index)
            .and_then(Option::take);
        match taken {
            Some(stream) => {
                let (recv, send) = tokio::io::split(stream);
                std::future::ready(Ok((send, recv)))
            }
            None => std::future::ready(Err(io::Error::other("tcp stream index exhausted"))),
        }
    }
}
