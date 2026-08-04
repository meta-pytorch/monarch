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
//! ## kTLS, with a userspace fallback
//!
//! When the kernel supports it, every socket is upgraded to **kTLS** immediately after
//! its rustls handshake: the negotiated secrets are handed to the kernel (via the
//! `ktls` crate), so the AES-GCM record crypto runs in-kernel and the data path keeps
//! TSO — one sender core can then drive tens of Gbps, unlike userspace TLS which is
//! per-connection-core-bound. Both configs enable secret extraction so the keys can be
//! exported, and the `CorkStream` wrapper lets `ktls` drain rustls' buffered records
//! cleanly at the handoff.
//!
//! kTLS is a purely *local* optimization — the wire bytes are identical TLS either way,
//! and the two ends decide independently — so a host whose kernel lacks the TLS ULP
//! (the `tls` module) simply keeps userspace `tokio-rustls`. We [`probe_tls_ulp`] once
//! per process and route every handshake to the kTLS or the userspace path accordingly,
//! so a kTLS-less host never wastes a connection discovering it the hard way (this
//! transport is already sensitive to connect-time waste at high fan-out). A kTLS setup
//! that fails at runtime latches the whole process to userspace ([`disable_ktls`]) so
//! we don't retry a path the kernel has already refused. `MM_TCP_KTLS=0` forces the
//! userspace path outright (for debugging or to sidestep a flaky kernel). The three
//! stream flavors — in-kernel and the two userspace half-types — are unified behind
//! [`TlsStream`], over which the pairing prefix and every generic frame flow
//! identically.
//!
//! Everything protocol-independent — establishment, heartbeats (on their own stream),
//! matching serves to joins, side channels, retry/backoff, shutdown — lives in
//! [`crate::net_transport`], generic over [`Net`]. This module supplies only the raw
//! networking. The command loop drives tcp as `NetTransport<Tcp>` (aliased
//! `TcpTransport` in [`crate::ctx`]).
//!
//! ## Streams: one socket each, opened on demand and paired by index
//!
//! A QUIC connection multiplexes its streams (data + heartbeat) on one transport
//! connection; TCP has no multiplexing, so each stream is its own TLS-over-TCP socket.
//! A plain socket can only be opened from the dialing side, so a tcp connection is
//! *directional*: the dialer's [`NetConn::stream`] dials a fresh socket, the acceptor's
//! awaits one. Each socket the dialer opens carries a `(connection_id, stream_index)`
//! prefix. Many logical connections share one listening socket, so a single listener
//! demux pairs each accepted socket with the acceptor-side `stream` request naming the
//! same `(connection_id, index)` — a matcher per pair, dropped as soon as the two halves
//! meet. A socket for stream index 0 (the connection's first stream) surfaces the
//! connection out of [`Net::accept`]; later sockets (the heartbeat stream, or data
//! streams for striping) just pair against their requests.
//!
//! Streams are opened lazily and per index — nothing is pre-opened at connect time — so
//! a message-only side channel opens exactly one socket, and the transport can request a
//! heartbeat or additional data stream whenever it needs one. `priority` is ignored:
//! tcp has no per-stream send priority.
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
//!
//! ## Throughput tuning
//!
//! The fast settings that are *safe* to enable everywhere are on by default; the ones
//! whose cost or policy impact scales with the connection count are left opt-in.
//!
//! On by default (safe regardless of scale):
//!
//! - `TCP_NODELAY` — always on: framing flushes per frame, so Nagle would only add
//!   latency.
//! - Congestion control defaults to **BBR**, pinned with `TCP_CONGESTION` *after* the
//!   connection is up — matching the quic transport, which pins BBR unconditionally.
//!   The post-connect timing is deliberate: cross-region flows at Meta have a NetEdit
//!   cgroup eBPF `sockops` force their own CUBIC variant at connect time, overriding
//!   any pre-connect `setsockopt`; a post-connect set sticks. The set is best-effort,
//!   so a host without the `tcp_bbr` module simply keeps the kernel default. Override
//!   with `MM_TCP_CONGESTION` (e.g. `cubic`, or empty to pin nothing).
//! - Listen backlog and `SO_REUSEADDR` on the listener — robustness with negligible
//!   cost.
//!
//! Opt-in (unset ⇒ OS default), because a fixed kernel socket-buffer size both
//! *disables* Linux's per-socket buffer autotuning and — since tcp uses one socket per
//! stream per connection, and a root holds a huge number of connections — multiplies
//! that fixed cost across every socket. Autotuning is the better default at scale;
//! these are for reproducing the cross-region bandwidth ceilings in a bench:
//!
//! - `MM_TCP_SNDBUF_BYTES` / `MM_TCP_RCVBUF_BYTES` — kernel socket-buffer sizes,
//!   set *before* connect/bind so TCP window scaling is negotiated for the enlarged
//!   window (accepted sockets inherit the listener's scale). Requested with the
//!   privileged `SO_*BUFFORCE` options (which bypass `net.core.{r,w}mem_max` under
//!   `CAP_NET_ADMIN`, e.g. as root on MAST) with a fall back to the ordinary setters
//!   otherwise.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::BuildHasher;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use ktls::CorkStream;
use ktls::KtlsStream;
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
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;

use crate::matcher::Matcher;
use crate::net::Net;
use crate::net::NetConn;

/// Server name the client uses to verify the server's certificate (the cert's SAN
/// must cover it). Shared with the quic transport, which uses the same cert set.
const SERVER_NAME: &str = "monarch-mini";

/// Listen backlog. Generous so a burst of joiners (e.g. a many-connection bench
/// against one server) is not refused before the accept loop drains them.
const LISTEN_BACKLOG: u32 = 1024;

/// Optional kernel send/recv buffer request, in bytes. Unset ⇒ OS default.
fn sndbuf_bytes() -> Option<usize> {
    env_usize("MM_TCP_SNDBUF_BYTES")
}

fn rcvbuf_bytes() -> Option<usize> {
    env_usize("MM_TCP_RCVBUF_BYTES")
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
}

/// The congestion-control algorithm to pin after connect. Defaults to BBR — matching
/// the quic transport, which pins BBR unconditionally: it holds a higher steady rate
/// than CUBIC across the light loss of a high-RTT cross-region path (quinn#2262: CUBIC
/// stalls at ~1 MiB cwnd where BBR reaches the BDP) and is ~neutral on the lossless
/// intra-cluster fabric. Best-effort — if the `tcp_bbr` module is not loaded the set
/// fails and the kernel default stays (see [`set_congestion`]). Override the algorithm
/// with `MM_TCP_CONGESTION` (e.g. `cubic` to pin the classic default); an empty
/// `MM_TCP_CONGESTION=` pins nothing at all, leaving whatever the kernel/NetEdit picks.
fn congestion() -> Option<String> {
    match std::env::var("MM_TCP_CONGESTION") {
        Ok(name) if name.is_empty() => None, // explicit opt-out: pin nothing
        Ok(name) => Some(name),
        Err(_) => Some("bbr".to_owned()), // default
    }
}

/// Log the active tuning knobs once per process, so a bench run records which
/// levers were set without spamming a line per connection.
fn log_tuning_once() {
    static LOGGED: OnceLock<()> = OnceLock::new();
    LOGGED.get_or_init(|| {
        if sndbuf_bytes().is_some() || rcvbuf_bytes().is_some() || congestion().is_some() {
            eprintln!(
                "MM_TCP tuning: sndbuf={:?} rcvbuf={:?} congestion={:?}",
                sndbuf_bytes(),
                rcvbuf_bytes(),
                congestion(),
            );
        }
    });
}

/// Enlarge a socket's kernel send/recv buffers, best-effort, *before* connect/bind
/// so TCP window scaling is negotiated for the enlarged window. Tries the privileged
/// `SO_*BUFFORCE` options first — these bypass the `net.core.{r,w}mem_max` ceiling
/// under `CAP_NET_ADMIN` (e.g. running as root on MAST) — and falls back to the
/// ordinary setters (which the kernel clamps to that ceiling) when not permitted. A
/// no-op for a buffer whose env knob is unset.
fn set_buffers(fd: RawFd) {
    if let Some(bytes) = sndbuf_bytes() {
        set_buffer(fd, libc::SO_SNDBUFFORCE, libc::SO_SNDBUF, bytes);
    }
    if let Some(bytes) = rcvbuf_bytes() {
        set_buffer(fd, libc::SO_RCVBUFFORCE, libc::SO_RCVBUF, bytes);
    }
}

fn set_buffer(fd: RawFd, force_opt: libc::c_int, opt: libc::c_int, bytes: usize) {
    let size = bytes.min(i32::MAX as usize) as libc::c_int;
    let len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `fd` is a valid socket for the call's duration; the option value is an
    // `int` of length `len`, as required by SO_*BUF{,FORCE}.
    let forced = unsafe {
        let value = std::ptr::from_ref(&size).cast::<libc::c_void>();
        libc::setsockopt(fd, libc::SOL_SOCKET, force_opt, value, len)
    };
    if forced != 0 {
        // SAFETY: as above; the ordinary setter is clamped by the kernel.
        unsafe {
            let value = std::ptr::from_ref(&size).cast::<libc::c_void>();
            libc::setsockopt(fd, libc::SOL_SOCKET, opt, value, len);
        }
    }
}

/// Apply the post-connect socket tuning to an established socket: `TCP_NODELAY` (always
/// on — framing flushes per frame, so Nagle would only add latency) and the pinned
/// congestion-control algorithm (BBR by default; see [`congestion`]). Both are
/// best-effort. The congestion set must run *after* connect — see the module docs on
/// the NetEdit connect-time override.
fn tune_established(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    set_congestion(stream.as_raw_fd());
}

/// Pin the congestion-control algorithm on an established connection, if
/// `MM_TCP_CONGESTION` / `MM_TCP_BBR` requested one. Best-effort: a failure (e.g. the
/// `tcp_bbr` module is not loaded) leaves the kernel default in place.
fn set_congestion(fd: RawFd) {
    let Some(name) = congestion() else {
        return;
    };
    // SAFETY: `fd` is a valid connected TCP socket; `name` bytes are a valid buffer
    // of `name.len()`, the form TCP_CONGESTION expects (a non-terminated string).
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_CONGESTION,
            name.as_ptr().cast::<libc::c_void>(),
            name.len() as libc::socklen_t,
        );
    }
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

    // Both configs enable secret extraction so the negotiated keys can be handed to
    // the kernel for kTLS (see the module docs and [`client_ktls`]/[`server_ktls`]).
    let mut server = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|err| anyhow::anyhow!("tls server versions: {err}"))?
        .with_no_client_auth()
        .with_single_cert(load_certs(&cert_path)?, load_key(&key_path)?)?;
    server.enable_secret_extraction = true;

    let mut roots = RootCertStore::empty();
    for ca in load_certs(&ca_path)? {
        roots.add(ca)?;
    }
    let mut client = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|err| anyhow::anyhow!("tls client versions: {err}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.enable_secret_extraction = true;

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
/// listener demux group the sockets of one logical connection and route each to its
/// stream-index request. Precedes every generic frame on a dialed socket.
async fn write_pairing_prefix<W: AsyncWrite + Unpin>(
    writer: &mut W,
    connection_id: u128,
    stream_index: usize,
) -> io::Result<()> {
    writer.write_all(&connection_id.to_le_bytes()).await?;
    writer
        .write_all(&(stream_index as u16).to_le_bytes())
        .await?;
    writer.flush().await
}

/// Read the socket-pairing prefix written by [`write_pairing_prefix`], as
/// `(connection_id, stream_index)`. `None` on EOF or a short read (a peer that spoke a
/// bad dialect — the socket is dropped).
async fn read_pairing_prefix<R: AsyncRead + Unpin>(reader: &mut R) -> Option<(u128, usize)> {
    let mut id = [0u8; 16];
    reader.read_exact(&mut id).await.ok()?;
    let mut index = [0u8; 2];
    reader.read_exact(&mut index).await.ok()?;
    Some((u128::from_le_bytes(id), u16::from_le_bytes(index) as usize))
}

/// A TLS-over-TCP stream in one of three flavors, unified so framing (and the pairing
/// prefix) drives them identically: an in-kernel [`KtlsStream`] on a host with the TLS
/// ULP, or a userspace `tokio-rustls` stream (server- or client-side) otherwise. See
/// the module docs on the kTLS fallback.
pub(crate) enum TlsStream {
    Ktls(KtlsStream<TcpStream>),
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
            TlsStream::Ktls(s) => Pin::new(s).poll_read(cx, buf),
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
            TlsStream::Ktls(s) => Pin::new(s).poll_write(cx, buf),
            TlsStream::Server(s) => Pin::new(s).poll_write(cx, buf),
            TlsStream::Client(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsStream::Ktls(s) => Pin::new(s).poll_flush(cx),
            TlsStream::Server(s) => Pin::new(s).poll_flush(cx),
            TlsStream::Client(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsStream::Ktls(s) => Pin::new(s).poll_shutdown(cx),
            TlsStream::Server(s) => Pin::new(s).poll_shutdown(cx),
            TlsStream::Client(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Set once a kTLS setup has failed at runtime (or the up-front probe found the TLS ULP
/// missing): from then on every handshake takes the userspace path. Starts `false`; the
/// first [`use_ktls`] call resolves the probe into it.
static KTLS_DISABLED: AtomicBool = AtomicBool::new(false);

/// Whether to attempt kTLS for the next handshake. `true` until we learn the kernel
/// can't do it — either because `MM_TCP_KTLS=0` opted out, from the one-time
/// [`probe_tls_ulp`] (resolved on first call), or from a runtime failure that called
/// [`disable_ktls`].
fn use_ktls() -> bool {
    static PROBED: OnceLock<()> = OnceLock::new();
    PROBED.get_or_init(|| {
        if matches!(
            std::env::var("MM_TCP_KTLS").as_deref(),
            Ok("0") | Ok("false")
        ) {
            disable_ktls("opted out via MM_TCP_KTLS");
        } else if !probe_tls_ulp() {
            disable_ktls("kernel TLS ULP (tls module) not available");
        }
    });
    !KTLS_DISABLED.load(Ordering::Relaxed)
}

/// Latch kTLS off for the whole process, logging the reason once. Called on a runtime
/// kTLS setup failure so the connection retry — and every later handshake — uses
/// userspace TLS instead of re-attempting a path the kernel refused.
fn disable_ktls(reason: &str) {
    if !KTLS_DISABLED.swap(true, Ordering::Relaxed) {
        eprintln!("MM_TCP kTLS disabled, falling back to userspace TLS: {reason}");
    }
}

/// Probe whether the kernel exposes the TLS upper-layer protocol (kTLS), by trying to
/// set the `tls` ULP on a throwaway TCP socket. A missing kernel module fails with
/// `ENOENT`; any other outcome — including `ENOTCONN`, since we deliberately don't
/// connect the socket — means the module is present. Cheap and connection-free, so a
/// kTLS-less host learns its fate without sacrificing a real connection.
fn probe_tls_ulp() -> bool {
    // SAFETY: a plain `socket()`/`setsockopt()`/`close()` sequence. `fd` is valid
    // between its creation and close; the option value is the 3-byte string `tls` of
    // the matching length, as `TCP_ULP` expects (a non-terminated name).
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return false;
        }
        let name = b"tls";
        let ret = libc::setsockopt(
            fd,
            libc::SOL_TCP,
            libc::TCP_ULP,
            name.as_ptr().cast::<libc::c_void>(),
            name.len() as libc::socklen_t,
        );
        let errno = (ret != 0).then(|| std::io::Error::last_os_error().raw_os_error());
        libc::close(fd);
        !matches!(errno, Some(Some(libc::ENOENT)))
    }
}

/// Run the client rustls handshake over `tcp`, then hand back a [`TlsStream`]. When
/// [`use_ktls`] is set the socket is wrapped in a `CorkStream` and upgraded to kTLS
/// (the cork lets `ktls` drain rustls' buffered records cleanly at the handoff, and any
/// decrypted-but-unconsumed app data is re-injected for the caller); a kTLS setup that
/// fails latches the process to userspace so the connection's retry succeeds there.
/// Otherwise the plain userspace `tokio-rustls` stream is returned directly.
async fn client_tls(
    connector: &TlsConnector,
    server_name: ServerName<'static>,
    tcp: TcpStream,
) -> anyhow::Result<TlsStream> {
    if use_ktls() {
        let stream = connector.connect(server_name, CorkStream::new(tcp)).await?;
        return match ktls::config_ktls_client(stream).await {
            Ok(ktls) => Ok(TlsStream::Ktls(ktls)),
            Err(err) => {
                disable_ktls(&format!("client kTLS setup failed: {err}"));
                Err(anyhow::anyhow!("client kTLS setup failed: {err}"))
            }
        };
    }
    let stream = connector.connect(server_name, tcp).await?;
    Ok(TlsStream::Client(stream))
}

/// Run the server rustls handshake over `tcp`, then hand back a [`TlsStream`] (see
/// [`client_tls`] for the kTLS/userspace split).
async fn server_tls(acceptor: &TlsAcceptor, tcp: TcpStream) -> anyhow::Result<TlsStream> {
    if use_ktls() {
        let stream = acceptor.accept(CorkStream::new(tcp)).await?;
        return match ktls::config_ktls_server(stream).await {
            Ok(ktls) => Ok(TlsStream::Ktls(ktls)),
            Err(err) => {
                disable_ktls(&format!("server kTLS setup failed: {err}"));
                Err(anyhow::anyhow!("server kTLS setup failed: {err}"))
            }
        };
    }
    let stream = acceptor.accept(tcp).await?;
    Ok(TlsStream::Server(stream))
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

/// A bound TCP server. All accept/pairing work runs in the [`listener_demux`] coroutine
/// spawned by [`Net::bind`]; the handle keeps only the receiving end of the
/// newly-surfaced-connection channel. [`Net::accept`] pulls the next new connection from
/// it. `Mutex` (tokio) because `accept` takes `&self` and holds the receiver across the
/// await — uncontended, since a single task accepts.
pub(crate) struct TcpListenerHandle {
    new_conns: Mutex<mpsc::UnboundedReceiver<TcpConn>>,
}

/// The single demux for a bound tcp server. Many logical connections share one listening
/// socket, so it pairs each accepted socket with the [`TcpStreamRequest`] naming the same
/// `(connection_id, index)` — one [`Matcher`] per pair, removed the moment its two halves
/// meet, so a healthy connection leaves nothing behind (no per-connection coroutine, no
/// live map). A socket for stream index 0 is a connection's first stream (it carries the
/// preamble and arrives once), so it surfaces a fresh [`TcpConn`] out of [`Net::accept`].
///
/// Each socket's TLS handshake + prefix read runs in its own task — so a slow handshake
/// doesn't head-of-line-block accept — then lands on `sockets`. Requests from every
/// acceptor connection's `stream` calls share `requests`. The loop holds no lock and
/// never awaits while touching the matchers, so sockets and requests never block each
/// other.
async fn listener_demux(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    new_conns: mpsc::UnboundedSender<TcpConn>,
) {
    // Requests from every acceptor connection's `stream` calls. The sender is cloned into
    // each surfaced connection; the loop keeps `requests_tx` too, so `requests.recv()`
    // never closes on its own.
    let (requests_tx, mut requests) = mpsc::unbounded_channel::<TcpStreamRequest>();
    // Handshaked sockets from the per-socket handshake tasks.
    let (sock_tx, mut sockets) = mpsc::unbounded_channel::<(SocketAddr, u128, usize, TlsStream)>();
    // For each (connection_id, index): the socket or the request that arrived first,
    // parked until its partner shows up. An entry lives only while a half is waiting.
    let mut matchers: HashMap<(u128, usize), Matcher<TcpStreamRequest, TlsStream>> = HashMap::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((tcp, peer)) = accepted else {
                    continue; // transient accept error; the listener stays open
                };
                // The connection is up (accept returned), so pin nodelay/congestion now;
                // the buffer window scale was inherited from the listening socket.
                tune_established(&tcp);
                let acceptor = acceptor.clone();
                let sock_tx = sock_tx.clone();
                tokio::task::spawn_local(async move {
                    let Ok(mut stream) = server_tls(&acceptor, tcp).await else {
                        return; // TLS/kTLS handshake failed
                    };
                    let Some((cid, index)) = read_pairing_prefix(&mut stream).await else {
                        return; // bad/short prefix
                    };
                    let _ = sock_tx.send((peer, cid, index, stream));
                });
            }
            sock = sockets.recv() => {
                let Some((peer, cid, index, stream)) = sock else { continue; };
                let key = (cid, index);
                if matchers
                    .entry(key)
                    .or_insert_with(Matcher::new)
                    .push_right(stream, fulfill_tcp)
                    .is_some()
                {
                    matchers.remove(&key);
                }
                // Index 0 is the connection's first stream; surface it once, here.
                if index == 0 {
                    let conn = TcpConn::Acceptor {
                        remote: peer,
                        connection_id: cid,
                        requests: requests_tx.clone(),
                    };
                    if new_conns.send(conn).is_err() {
                        return; // transport gone
                    }
                }
            }
            req = requests.recv() => {
                // We hold `requests_tx`, so recv never returns None; guard anyway.
                let Some(req) = req else { return; };
                let key = (req.connection_id, req.index);
                if matchers
                    .entry(key)
                    .or_insert_with(Matcher::new)
                    .push_left(req, fulfill_tcp)
                    .is_some()
                {
                    matchers.remove(&key);
                }
            }
        }
    }
}

impl Net for Tcp {
    type Dialer = TcpDialer;
    type Listener = TcpListenerHandle;
    type Conn = TcpConn;

    fn create(_runtime: Option<Handle>) -> anyhow::Result<Self> {
        // Ignored: kTLS/rustls crypto runs in the stream poll (the read/write syscall),
        // not a background driver, so it follows whichever task polls the stream. The
        // generic transport places those data coroutines on the runtime instead.
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

    fn bind(&mut self, addr: SocketAddr) -> anyhow::Result<TcpListenerHandle> {
        log_tuning_once();
        let acceptor = TlsAcceptor::from(self.tls()?.server.clone());
        // Build the listener socket directly so the kernel buffers can be enlarged
        // *before* bind (so the window scale accepted sockets inherit reflects the
        // enlarged window), then bind and listen — none of which awaits.
        let socket = if addr.is_ipv6() {
            TcpSocket::new_v6()?
        } else {
            TcpSocket::new_v4()?
        };
        set_buffers(socket.as_raw_fd());
        socket.set_reuseaddr(true)?;
        socket.bind(addr)?;
        let listener = socket.listen(LISTEN_BACKLOG)?;
        // Spawn the demux on the command-loop LocalSet (bind is called from it). It owns
        // the listener and pairs accepted sockets to stream requests, surfacing new
        // connections on `new_tx`.
        let (new_tx, new_rx) = mpsc::unbounded_channel();
        tokio::task::spawn_local(listener_demux(listener, acceptor, new_tx));
        Ok(TcpListenerHandle {
            new_conns: Mutex::new(new_rx),
        })
    }

    async fn accept(listener: &TcpListenerHandle) -> Option<TcpConn> {
        listener.new_conns.lock().await.recv().await
    }

    async fn connect(dialer: &TcpDialer, addr: SocketAddr) -> Option<TcpConn> {
        // Lazy: no socket is opened here. Each stream dials its own socket on demand (see
        // `TcpConn::stream`), so a tcp join's reachability is proven by the first stream
        // call, not here. Just mint the id that groups this connection's sockets.
        Some(TcpConn::Dialer {
            dialer: dialer.clone(),
            addr,
            connection_id: new_connection_id(),
        })
    }

    /// tcp opens an OS socket plus a TLS handshake per stream, so it needs a much smaller
    /// default than quic: an unthrottled connect storm at high fan-out leaves stragglers
    /// whose first socket never finishes handshaking (seen as the root's `only N-1/N
    /// workers connected` aborts). 128 has proven safe through a full 65k-worker sweep;
    /// raise with `MM_QUIC_MAX_CONCURRENT_CONNECTS` if a run needs faster connect ramp.
    /// (The throttle covers the first stream's dial; the heartbeat and any data streams
    /// open later, naturally staggered.)
    fn default_connect_concurrency() -> Option<usize> {
        Some(128)
    }
}

/// The send/recv halves of one paired tcp stream.
type TcpHalves = (WriteHalf<TlsStream>, ReadHalf<TlsStream>);

/// A [`NetConn::stream`] request from an acceptor connection to the listener demux: which
/// `(connection_id, index)` socket it wants, and the one-shot the demux fulfils with the
/// paired socket's halves. (tcp has no per-stream priority, so none is carried.)
pub(crate) struct TcpStreamRequest {
    connection_id: u128,
    index: usize,
    reply: oneshot::Sender<io::Result<TcpHalves>>,
}

/// Fulfil a request with the socket the demux paired to its `(connection_id, index)`:
/// split the socket and send the halves back through the one-shot.
fn fulfill_tcp(req: TcpStreamRequest, stream: TlsStream) {
    let (recv, send) = tokio::io::split(stream);
    let _ = req.reply.send(Ok((send, recv)));
}

/// Dial one TLS-over-TCP socket to `addr` (see the module tuning docs for the socket
/// options). The caller ([`NetConn::stream`]) writes the pairing prefix afterwards.
async fn dial_one(dialer: &TcpDialer, addr: SocketAddr) -> io::Result<TlsStream> {
    log_tuning_once();
    // Build the socket directly so the kernel buffers can be enlarged *before* connect
    // (so window scaling is negotiated for the enlarged window), then pin
    // nodelay/congestion once the connection is up.
    let socket = if addr.is_ipv6() {
        TcpSocket::new_v6()?
    } else {
        TcpSocket::new_v4()?
    };
    set_buffers(socket.as_raw_fd());
    let tcp = socket.connect(addr).await?;
    tune_established(&tcp);
    client_tls(&dialer.connector, dialer.server_name.clone(), tcp)
        .await
        .map_err(|e| io::Error::other(format!("tcp/tls dial {addr}: {e:#}")))
}

/// One logical TCP connection, in one of two roles set at construction, and cloneable so
/// several tasks (data reader/writer, heartbeat) can each open the streams they own. A
/// **dialer** connection (from [`Net::connect`]) opens each requested stream by dialing a
/// fresh socket and writing its `(connection_id, index)` prefix. An **acceptor**
/// connection (from [`Net::accept`]) sends each request — tagged with its `connection_id`
/// and index — to the shared [`listener_demux`], which pairs it with the matching socket.
/// The demux keeps no per-connection state past a pending pair, so cloning the handle is
/// free of lifecycle concerns.
#[derive(Clone)]
pub(crate) enum TcpConn {
    Dialer {
        dialer: TcpDialer,
        addr: SocketAddr,
        connection_id: u128,
    },
    Acceptor {
        remote: SocketAddr,
        connection_id: u128,
        requests: mpsc::UnboundedSender<TcpStreamRequest>,
    },
}

impl fmt::Debug for TcpConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let remote = match self {
            TcpConn::Dialer { addr, .. } => addr,
            TcpConn::Acceptor { remote, .. } => remote,
        };
        f.debug_tuple("TcpConn").field(remote).finish()
    }
}

impl NetConn for TcpConn {
    type Send = WriteHalf<TlsStream>;
    type Recv = ReadHalf<TlsStream>;
    type Stream = Pin<Box<dyn Future<Output = io::Result<(Self::Send, Self::Recv)>>>>;

    fn stream(&self, index: usize, _priority: i32) -> Self::Stream {
        match self {
            // Dialer: open a fresh socket for this index and tag it with the prefix.
            TcpConn::Dialer {
                dialer,
                addr,
                connection_id,
            } => {
                let dialer = dialer.clone();
                let addr = *addr;
                let connection_id = *connection_id;
                Box::pin(async move {
                    let mut stream = dial_one(&dialer, addr).await?;
                    write_pairing_prefix(&mut stream, connection_id, index).await?;
                    let (recv, send) = tokio::io::split(stream);
                    Ok((send, recv))
                })
            }
            // Acceptor: ask the demux for this (connection, index) socket and await the
            // paired halves.
            TcpConn::Acceptor {
                connection_id,
                requests,
                ..
            } => {
                let connection_id = *connection_id;
                let requests = requests.clone();
                Box::pin(async move {
                    let (reply, rx) = oneshot::channel();
                    requests
                        .send(TcpStreamRequest {
                            connection_id,
                            index,
                            reply,
                        })
                        .map_err(|_| io::Error::other("tcp connection closed"))?;
                    rx.await
                        .map_err(|_| io::Error::other("tcp connection closed"))?
                })
            }
        }
    }
}
