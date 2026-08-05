/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! QUIC implementation of the [`Net`] transport seam.
//!
//! This module supplies only the quic-specific networking: TLS material, the client
//! endpoint pool, binding a server endpoint, dialing, and a [`QuicConn`] that opens
//! and accepts bidirectional streams. Everything protocol-independent — establishment,
//! heartbeats, matching serves to joins, side channels, retry/backoff, shutdown —
//! lives in [`crate::net_transport`], generic over [`Net`]. The command loop drives
//! quic as `NetTransport<Quic>` (aliased `QuicTransport` in [`crate::ctx`]).
//!
//! ## Two streams per connection
//!
//! Each connection carries two bidirectional streams (see `net_transport`): a
//! data/control stream and a companion heartbeat stream. QUIC's streams are
//! independently ordered and flow-controlled and interleave at the packet level, so a
//! large message on the data stream cannot head-of-line-block a beat. The heartbeat
//! stream is given a higher send priority (its stream index) so beats are packed into
//! packets ahead of data even under a full congestion window. (Flow-control *windows*
//! are left at their defaults: they are credit ceilings, not pre-allocated buffers,
//! and beats are tiny.)
//!
//! ## Why QUIC needs heartbeats
//!
//! QUIC runs over UDP in userspace, so there is no file-descriptor close to signal a
//! lost peer: a crashed, frozen, or partitioned peer simply stops sending. So instead
//! of relying on EOF the transport runs an application-level bidirectional heartbeat
//! on the heartbeat stream (see [`crate::net_transport`] and
//! [`crate::heartbeat`]).
//!
//! ## Security
//!
//! TLS material is taken from the environment (the "we will provide it" hook):
//! `MM_QUIC_CERT` / `MM_QUIC_KEY` (the cert chain + key this endpoint serves) and
//! `MM_QUIC_CA` (the authority a joiner trusts). The server presents its cert; the
//! client verifies it against the CA for the fixed server name [`SERVER_NAME`].

use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::Context;
use std::task::Poll;

use quinn::ClientConfig;
use quinn::Connection;
use quinn::Endpoint;
use quinn::RecvStream;
use quinn::SendStream;
use quinn::ServerConfig;
use quinn::VarInt;
use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use crate::net::Net;
use crate::net::NetConn;

/// Server name the client uses to verify the server's certificate (the cert's SAN
/// must cover it). Fixed: routing/identity is handled above this layer.
const SERVER_NAME: &str = "monarch-mini";

/// Per-stream flow-control receive window (`MAX_STREAM_DATA`): how many bytes a peer
/// may have in flight on one stream before it must wait for the receiver to extend
/// the window. quinn's default is ~1.25 MB (sized for a 100 ms RTT), which caps a
/// large message's throughput at window/RTT — a severe throttle on a fast, low-RTT
/// link. We raise it so a big message can keep the pipe full.
///
/// This is a *ceiling* the receiver may buffer up to on a stream actively receiving
/// unread data, not a preallocation: idle connections cost nothing. It does raise the
/// worst-case memory a busy connection can hold, which matters for a root with very
/// many connections all mid-large-transfer — so it is env-tunable
/// (`MM_QUIC_STREAM_RECV_WINDOW_BYTES`). Only the data stream ever fills it; the
/// companion heartbeat stream carries tiny frames. The connection-level
/// `receive_window` is raised in lockstep (to `window * 8`, matching the send window)
/// in [`load_tls`] so the connection does not re-throttle its streams below this
/// per-stream window.
const DEFAULT_STREAM_RECV_WINDOW_BYTES: u64 = 16 * 1024 * 1024;

fn stream_recv_window_bytes() -> u64 {
    std::env::var("MM_QUIC_STREAM_RECV_WINDOW_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_STREAM_RECV_WINDOW_BYTES)
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

    // Disable all periodic QUIC traffic so a delegated link is truly silent:
    // `keep_alive_interval = None` (no PING keep-alives) and `max_idle_timeout = None`
    // (QUIC must not reap an idle delegated connection — liveness is now the heartbeat
    // subsystem's responsibility, not the transport's). Applied to both roles.
    // Message-carrying links are unaffected; delegated links rely on the sibling
    // fabric. See `HEARTBEAT_DELEGATION_DESIGN.md` §9.
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(None);
    transport.max_idle_timeout(None);
    // Raise the per-stream flow-control window (and the send window with it, keeping
    // quinn's 8x send:stream ratio) so a large message is not throttled to window/RTT.
    // See `stream_recv_window_bytes`.
    let window = stream_recv_window_bytes();
    transport.stream_receive_window(VarInt::from_u64(window).unwrap_or(VarInt::MAX));
    transport.send_window(window.saturating_mul(8));
    // Raise the connection-level receive window in lockstep with the send window so
    // the aggregate of a connection's streams can fill the bandwidth-delay product on
    // a high-RTT cross-region path, rather than being re-throttled below the
    // per-stream window. Sized to match `send_window` (window * 8).
    let conn_window = window.saturating_mul(8);
    transport.receive_window(VarInt::from_u64(conn_window).unwrap_or(VarInt::MAX));
    // Congestion control: quinn defaults to CUBIC, which on a high-RTT cross-region
    // path with sporadic loss ramps slowly and backs off hard (quinn#2262: CUBIC
    // stalls at ~1 MiB cwnd where iperf/BBR reach the BDP). Pin BBR unconditionally —
    // it is delay-model-based and holds a higher steady rate across light loss, and on
    // loopback / the lossless intra-cluster fabric it is ~neutral. quinn re-exports
    // BbrConfig at quinn::congestion.
    transport.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
    let transport = Arc::new(transport);

    let mut server = ServerConfig::with_single_cert(load_certs(&cert_path)?, load_key(&key_path)?)?;
    server.transport_config(transport.clone());

    let mut roots = RootCertStore::empty();
    for ca in load_certs(&ca_path)? {
        roots.add(ca)?;
    }
    let mut client = ClientConfig::with_root_certificates(Arc::new(roots))?;
    client.transport_config(transport);

    Ok(TlsConfig { server, client })
}

/// Parse a `quic://host:port` (or bare `host:port`) url into a socket address.
fn parse_addr(url: &str) -> anyhow::Result<SocketAddr> {
    let authority = url.strip_prefix("quic://").unwrap_or(url);
    authority
        .parse::<SocketAddr>()
        .map_err(|err| anyhow::anyhow!("invalid quic address {authority:?}: {err}"))
}

/// The wildcard client bind address matching `target`'s address family. A QUIC client
/// endpoint binds a local UDP socket before dialing, and that socket's family must
/// match the destination: an IPv4-bound socket cannot reach an IPv6 peer (and vice
/// versa). Dialing across machines is normally over IPv6, so this must pick `[::]:0`
/// for an IPv6 target rather than always binding `0.0.0.0:0`.
fn client_bind_addr(target: &SocketAddr) -> SocketAddr {
    match target {
        SocketAddr::V6(_) => {
            SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0)
        }
        SocketAddr::V4(_) => {
            SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
        }
    }
}

/// Default *total* kernel UDP buffer budget across the whole client endpoint pool
/// (64 MiB). It is divided evenly among the pool's sockets (see
/// [`client_udp_buf_per_socket`](client_udp_buf_total_bytes)), so adding endpoints
/// shrinks each socket's share while keeping the aggregate fixed. This matters for
/// non-root deployments: a per-socket request is clamped to `net.core.rmem_max`, so to
/// reach a large total without `CAP_NET_ADMIN` you spread it over more sockets.
/// Override the total with `MM_QUIC_UDP_BUF_BYTES`.
const DEFAULT_UDP_BUF_TOTAL_BYTES: usize = 64 * 1024 * 1024;

fn client_udp_buf_total_bytes() -> usize {
    std::env::var("MM_QUIC_UDP_BUF_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_UDP_BUF_TOTAL_BYTES)
}

/// Optional explicit override for the number of client endpoints. When unset, the pool
/// size is chosen adaptively (see [`Quic::build_client_pool`]): one socket if it can
/// hold the whole buffer budget, otherwise enough sockets to reach it. When set,
/// exactly that many sockets are used, each requesting an even split of the budget.
fn explicit_client_endpoints() -> Option<usize> {
    std::env::var("MM_QUIC_CLIENT_ENDPOINTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
}

/// When `MM_QUIC_UDP_BUF_NO_FORCE` is set, skip the privileged `SO_*BUFFORCE` options
/// so the buffer is exactly what an unprivileged process would get (clamped to
/// `net.core.rmem_max`). Used to validate a non-root-deployable config even while the
/// job happens to run as root.
fn udp_buf_force_disabled() -> bool {
    std::env::var("MM_QUIC_UDP_BUF_NO_FORCE").is_ok_and(|v| v != "0" && !v.is_empty())
}

/// Enlarge a UDP socket's kernel send/recv buffers, best-effort. Tries the privileged
/// `SO_*BUFFORCE` options first — these bypass the `net.core.{r,w}mem_max` ceiling and
/// work when the process has `CAP_NET_ADMIN` (e.g. running as root on MAST) — and
/// falls back to the ordinary setters (which the kernel clamps to that ceiling) when
/// not permitted.
fn set_udp_buffers(socket: &socket2::Socket, bytes: usize) {
    use std::os::fd::AsRawFd;

    if udp_buf_force_disabled() {
        // Non-root path only: the kernel clamps these to net.core.{r,w}mem_max.
        let _ = socket.set_recv_buffer_size(bytes);
        let _ = socket.set_send_buffer_size(bytes);
        return;
    }

    let fd = socket.as_raw_fd();
    let size = bytes.min(i32::MAX as usize) as libc::c_int;
    let len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `fd` is a valid UDP socket for the call's duration; the option value is
    // an `int` of length `len`, as required by SO_RCVBUFFORCE/SO_SNDBUFFORCE.
    let (forced_recv, forced_send) = unsafe {
        let value = std::ptr::from_ref(&size).cast::<libc::c_void>();
        (
            libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUFFORCE, value, len),
            libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUFFORCE, value, len),
        )
    };
    if forced_recv != 0 {
        let _ = socket.set_recv_buffer_size(bytes);
    }
    if forced_send != 0 {
        let _ = socket.set_send_buffer_size(bytes);
    }
}

/// Build a client [`Endpoint`] on a UDP socket whose kernel buffers we request at
/// `requested` bytes (see [`set_udp_buffers`]), bound to `bind`, with `client_config`
/// as its default. Returns the endpoint and the *usable* recv buffer the kernel
/// actually granted (it reports ~2× the usable size for bookkeeping, so we halve it),
/// which the caller uses to decide whether one socket reached the target or a pool is
/// needed.
fn make_client_endpoint(
    bind: SocketAddr,
    client_config: ClientConfig,
    requested: usize,
) -> anyhow::Result<(Endpoint, usize)> {
    let domain = if bind.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    set_udp_buffers(&socket, requested);
    let granted = socket.recv_buffer_size().unwrap_or(0);
    let usable = granted / 2;
    eprintln!(
        "MM_QUIC client endpoint: recv buf requested {} B, granted {} B (~{} B usable, force={})",
        requested,
        granted,
        usable,
        !udp_buf_force_disabled()
    );
    socket.bind(&bind.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    let runtime =
        quinn::default_runtime().ok_or_else(|| anyhow::anyhow!("no quic async runtime"))?;
    let mut endpoint = Endpoint::new(quinn::EndpointConfig::default(), None, std_socket, runtime)?;
    endpoint.set_default_client_config(client_config);
    Ok((endpoint, usable))
}

/// The shared per-context QUIC state: TLS configs (loaded lazily on first use) and a
/// pool of client endpoints per address family, created lazily and assigned
/// round-robin to joins/side-channels.
///
/// One endpoint per connection (the original behaviour) exhausted sockets and the
/// event loop at high fan-out. Collapsing to a single shared endpoint fixed that but
/// made the one UDP socket a throughput bottleneck: a burst of tens of thousands of
/// sends/receives overflows its buffers, dropping packets and forcing multi-second
/// QUIC retransmit timeouts. A small pool is the middle ground — connections (and
/// their burst load) are spread across `K` sockets. `K` is `MM_QUIC_CLIENT_ENDPOINTS`
/// (default adaptive).
pub(crate) struct Quic {
    tls: Option<Arc<TlsConfig>>,
    client_endpoints_v4: Vec<Endpoint>,
    client_endpoints_v6: Vec<Endpoint>,
    // Round-robin cursor for assigning connections to pool endpoints.
    client_rr: usize,
}

impl Quic {
    /// Build (or return the cached) TLS configs from the environment.
    fn tls(&mut self) -> anyhow::Result<Arc<TlsConfig>> {
        if let Some(tls) = &self.tls {
            return Ok(tls.clone());
        }
        let tls = Arc::new(load_tls()?);
        self.tls = Some(tls.clone());
        Ok(tls)
    }

    /// A client [`Endpoint`] for `target`'s address family, assigned round-robin from
    /// a lazily-created pool. Returns a cheap clone (the endpoint is internally
    /// reference-counted). Spreading connections across several UDP sockets keeps any
    /// one socket's buffers from overflowing under a fan-out burst, which otherwise
    /// causes packet loss and multi-second QUIC retransmit stalls.
    fn client_endpoint(&mut self, target: &SocketAddr) -> anyhow::Result<Endpoint> {
        let is_v6 = target.is_ipv6();
        let empty = if is_v6 {
            self.client_endpoints_v6.is_empty()
        } else {
            self.client_endpoints_v4.is_empty()
        };
        if empty {
            let pool = self.build_client_pool(client_bind_addr(target))?;
            if is_v6 {
                self.client_endpoints_v6 = pool;
            } else {
                self.client_endpoints_v4 = pool;
            }
        }
        let pool = if is_v6 {
            &self.client_endpoints_v6
        } else {
            &self.client_endpoints_v4
        };
        let endpoint = pool[self.client_rr % pool.len()].clone();
        self.client_rr = self.client_rr.wrapping_add(1);
        Ok(endpoint)
    }

    /// Build the client endpoint pool for one address family. Aim for
    /// [`client_udp_buf_total_bytes`] of total UDP buffer. First try to get it all on a
    /// single socket; if the kernel caps a single socket below the target (e.g.
    /// unprivileged, clamped to `net.core.rmem_max`), fall back to enough sockets —
    /// each requesting the measured per-socket max — to reach the total.
    /// `MM_QUIC_CLIENT_ENDPOINTS` overrides this with a fixed pool size.
    fn build_client_pool(&mut self, bind: SocketAddr) -> anyhow::Result<Vec<Endpoint>> {
        let total = client_udp_buf_total_bytes();
        let client_config = self.tls()?.client.clone();

        if let Some(n) = explicit_client_endpoints() {
            let per = (total / n).max(1);
            let mut pool = Vec::with_capacity(n);
            for _ in 0..n {
                pool.push(make_client_endpoint(bind, client_config.clone(), per)?.0);
            }
            eprintln!("MM_QUIC client pool: {n} endpoints (explicit), {per} B requested each");
            return Ok(pool);
        }

        // Adaptive: try to put the whole budget on one socket.
        let (first, usable) = make_client_endpoint(bind, client_config.clone(), total)?;
        if usable >= total {
            eprintln!("MM_QUIC client pool: 1 endpoint holds the full {total} B budget");
            return Ok(vec![first]);
        }

        // Capped below target — spread across enough sockets of `usable` each.
        let n = total.div_ceil(usable.max(1));
        eprintln!(
            "MM_QUIC client pool: single socket capped at {usable} B usable; using {n} endpoints to reach {total} B total"
        );
        let mut pool = Vec::with_capacity(n);
        pool.push(first);
        for _ in 1..n {
            pool.push(make_client_endpoint(bind, client_config.clone(), usable)?.0);
        }
        Ok(pool)
    }
}

impl Net for Quic {
    // A client endpoint clone (cheap; internally reference-counted). Dialing through it
    // repeatedly is what a connector/side-channel task does.
    type Dialer = Endpoint;
    // A server endpoint. quic multiplexes all streams on one connection, so it ignores
    // the stream count passed to `bind`.
    type Listener = Endpoint;
    type Conn = QuicConn;

    fn create() -> anyhow::Result<Self> {
        ensure_crypto_provider();
        Ok(Self {
            tls: None,
            client_endpoints_v4: Vec::new(),
            client_endpoints_v6: Vec::new(),
            client_rr: 0,
        })
    }

    fn parse_addr(url: &str) -> anyhow::Result<SocketAddr> {
        parse_addr(url)
    }

    fn dialer(&mut self, addr: SocketAddr) -> anyhow::Result<Endpoint> {
        self.client_endpoint(&addr)
    }

    fn bind(&mut self, addr: SocketAddr, _streams: usize) -> anyhow::Result<Endpoint> {
        let server = self.tls()?.server.clone();
        Ok(Endpoint::server(server, addr)?)
    }

    async fn accept(listener: &Endpoint) -> Option<QuicConn> {
        let incoming = listener.accept().await?;
        let connecting = incoming.accept().ok()?;
        let conn = connecting.await.ok()?;
        Some(QuicConn::new(conn))
    }

    async fn connect(dialer: &Endpoint, addr: SocketAddr, _streams: usize) -> Option<QuicConn> {
        // connect() fails synchronously on a bad config; the handshake (.await) fails
        // until the server is up. Both surface as `None` and the caller retries.
        let connecting = dialer.connect(addr, SERVER_NAME).ok()?;
        let conn = connecting.await.ok()?;
        Some(QuicConn::new(conn))
    }
}

/// One QUIC connection. Each [`NetConn::stream`] call hands out the *next*
/// bidirectional stream — `open_bi` when this side is the first messenger, `accept_bi`
/// otherwise. quinn pairs streams in open/accept order, and the two peers call
/// `stream` in the same order, so the pairs line up without any explicit index.
///
/// `next_priority` is the send priority handed to the next stream, bumped each call,
/// so a later stream (the heartbeat stream) outranks an earlier one (the data
/// stream). Not `Clone`: the cursor is per-connection state, and each connection is
/// created once and moved between its tasks (single-threaded runtime, so the `Cell`
/// is never contended).
pub(crate) struct QuicConn {
    conn: Connection,
    next_priority: Cell<i32>,
}

impl QuicConn {
    fn new(conn: Connection) -> Self {
        Self {
            conn,
            next_priority: Cell::new(0),
        }
    }
}

impl fmt::Debug for QuicConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("QuicConn")
            .field(&self.conn.remote_address())
            .finish()
    }
}

impl NetConn for QuicConn {
    type Send = QuicSend;
    type Recv = QuicRecv;
    type Stream = Pin<Box<dyn Future<Output = io::Result<(QuicSend, QuicRecv)>>>>;

    fn stream(&self, first_messenger: bool) -> Self::Stream {
        // Claim this stream's send priority now (at call order), not at poll time.
        let priority = self.next_priority.get();
        self.next_priority.set(priority + 1);
        let conn = self.conn.clone();
        Box::pin(async move {
            let (send, recv) = if first_messenger {
                conn.open_bi().await
            } else {
                conn.accept_bi().await
            }
            .map_err(io::Error::other)?;
            // A later stream ⇒ higher send priority, so a beat (the heartbeat stream,
            // opened after the data stream) is packed ahead of queued data under a full
            // congestion window.
            let _ = send.set_priority(priority);
            Ok((
                QuicSend {
                    send,
                    _conn: conn.clone(),
                },
                QuicRecv { recv, _conn: conn },
            ))
        })
    }
}

/// The send half of a QUIC stream. Holds a clone of the connection so it stays alive
/// as long as the half is in use — dropping every half of a connection closes it,
/// which the peer observes.
pub(crate) struct QuicSend {
    send: SendStream,
    _conn: Connection,
}

impl AsyncWrite for QuicSend {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.get_mut().send), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.get_mut().send), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.get_mut().send), cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write_vectored(Pin::new(&mut self.get_mut().send), cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        AsyncWrite::is_write_vectored(&self.send)
    }
}

/// The receive half of a QUIC stream (see [`QuicSend`] for the connection keep-alive).
pub(crate) struct QuicRecv {
    recv: RecvStream,
    _conn: Connection,
}

impl AsyncRead for QuicRecv {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        AsyncRead::poll_read(Pin::new(&mut self.get_mut().recv), cx, buf)
    }
}
