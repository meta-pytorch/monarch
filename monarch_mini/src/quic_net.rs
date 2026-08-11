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
//! `MM_QUIC_CERT` / `MM_QUIC_KEY` (the cert chain + key this endpoint presents) and
//! `MM_QUIC_CA` (the authority it trusts). The client verifies the server certificate
//! against the CA for the fixed name [`SERVER_NAME`], while the server verifies the
//! client's `clientAuth` certificate against the CA.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use quinn::ClientConfig;
use quinn::Connection;
use quinn::Endpoint;
use quinn::RecvStream;
use quinn::SendStream;
use quinn::ServerConfig;
use quinn::VarInt;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::crypto::rustls::QuicServerConfig;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::matcher::Matcher;
use crate::net::Net;
use crate::net::NetConn;
use crate::tls;
use crate::tls::SERVER_NAME;

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

/// Server + client TLS configs, built once from the environment and shared by all
/// quic serves/joins in this context.
struct TlsConfig {
    server: ServerConfig,
    client: ClientConfig,
}

fn load_tls() -> anyhow::Result<TlsConfig> {
    let tls::Config {
        mut server,
        mut client,
    } = tls::Config::load()?;

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
    // Raise the MTU-discovery ceiling so QUIC can use the jumbo headroom of the
    // intra-cluster fabric. On GPU hosts the frontend `eth0` carries a deliberate
    // jumbo MTU of 5000 (chef sets `JUMBO_MTU=5000` for GPU_TC hosts), but quinn's
    // default MTU discovery caps the UDP payload at 1452, so it never uses that
    // headroom — leaving ~3x more datagrams and their per-packet kernel cost
    // (`sendmsg` on TX, skb alloc/free on RX) on the table at the single-host receive
    // ceiling. We only raise the *ceiling* (from `mtu_search_upper_bound`) and let
    // quinn's DPLPMTUD *discover* the real path MTU — we never pin `initial_mtu` or
    // disable discovery — so an over-large ceiling on a smaller path simply settles
    // lower (a lost probe leaves the MTU where it was, it never stalls data or
    // black-holes the handshake). The companion `endpoint_config()` advertises the
    // same `max_udp_payload_size` on both roles; without that the peer would clamp our
    // probes back to its 1472 default (quinn-proto `mtud`: `current_mtu.min(peer_max)`).
    let mut mtud = quinn::MtuDiscoveryConfig::default();
    mtud.upper_bound(mtu_search_upper_bound());
    transport.mtu_discovery_config(Some(mtud));
    let transport = Arc::new(transport);

    server.max_early_data_size = u32::MAX;
    let mut server = ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server)?));
    server.transport_config(transport.clone());

    client.enable_early_data = true;
    let mut client = ClientConfig::new(Arc::new(QuicClientConfig::try_from(client)?));
    client.transport_config(transport);

    Ok(TlsConfig { server, client })
}

/// IP + UDP header overhead subtracted from a link MTU to get the largest QUIC UDP
/// payload that fits. Cross-machine QUIC is IPv6 (40 B header) + UDP (8 B) = 48 B;
/// using the IPv6 figure is the conservative choice (IPv4's 28 B is smaller, so this
/// never over-estimates the payload).
const IP_UDP_OVERHEAD: u16 = 48;

/// Fallback UDP-payload discovery ceiling when the local link MTU can't be read (see
/// [`eth0_udp_payload`]). Larger than quinn's 1452 default so discovery can still
/// climb on a jumbo fabric; because it is only a *ceiling* for DPLPMTUD probing (never
/// pinned), an over-large value on a smaller path just settles lower.
const DEFAULT_MTU_SEARCH_UPPER_BOUND: u16 = 4800;

/// The largest QUIC UDP payload that fits `eth0`'s link MTU (`mtu − IP_UDP_OVERHEAD`),
/// or `None` if the sysfs file is absent/unparseable. `eth0` is the frontend fabric
/// that on GPU hosts carries a jumbo MTU (chef sets `JUMBO_MTU=5000` for GPU_TC
/// hosts). Deliberately reads only this one file — no interface scanning and no active
/// probing.
fn eth0_udp_payload() -> Option<u16> {
    let mtu: u16 = std::fs::read_to_string("/sys/class/net/eth0/mtu")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    mtu.checked_sub(IP_UDP_OVERHEAD).filter(|&p| p >= 1200)
}

/// The UDP-payload ceiling for QUIC MTU discovery, read in one place so the
/// [`quinn::TransportConfig`] (the DPLPMTUD `upper_bound`) and the
/// [`quinn::EndpointConfig`] (the advertised `max_udp_payload_size`, which caps what
/// the peer probes toward us) stay consistent. It is `MM_QUIC_MTU` if set (≥1200; an
/// operator override, e.g. to reduce the ceiling), else `eth0`'s link MTU (see
/// [`eth0_udp_payload`]), else [`DEFAULT_MTU_SEARCH_UPPER_BOUND`]. This is only a
/// ceiling — quinn always *discovers* the real path MTU up to it (see [`load_tls`]),
/// so a too-large value never breaks a smaller path.
fn mtu_search_upper_bound() -> u16 {
    std::env::var("MM_QUIC_MTU")
        .ok()
        .and_then(|m| m.parse::<u16>().ok())
        .filter(|&m| m >= 1200)
        .or_else(eth0_udp_payload)
        .unwrap_or(DEFAULT_MTU_SEARCH_UPPER_BOUND)
}

/// The [`quinn::EndpointConfig`] for every client/server socket in this context.
/// quinn's default advertises `max_udp_payload_size = 1472` as this endpoint's receive
/// limit, and QUIC clamps the *peer's* probed MTU down to it (quinn-proto
/// `mtud::on_peer_max_udp_payload_size_received`: `current_mtu.min(peer_max)`), so
/// raising the discovery ceiling on the sender alone is a no-op — the receiver must
/// advertise the larger size too. We set it to [`mtu_search_upper_bound`] on both
/// roles; this value also sizes quinn's GRO recv buffer (`max_udp_payload_size ·
/// gro_segments · BATCH_SIZE`).
fn endpoint_config() -> quinn::EndpointConfig {
    let mut cfg = quinn::EndpointConfig::default();
    // Only errors if the value is < 1200, already excluded by `mtu_search_upper_bound`.
    let _ = cfg.max_udp_payload_size(mtu_search_upper_bound());
    cfg
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
    if crate::ctx::connection_debug() {
        eprintln!(
            "MM_QUIC client endpoint: recv buf requested {} B, granted {} B (~{} B usable, force={})",
            requested,
            granted,
            usable,
            !udp_buf_force_disabled()
        );
    }
    socket.bind(&bind.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    let runtime =
        quinn::default_runtime().ok_or_else(|| anyhow::anyhow!("no quic async runtime"))?;
    let mut endpoint = Endpoint::new(endpoint_config(), None, std_socket, runtime)?;
    endpoint.set_default_client_config(client_config);
    Ok((endpoint, usable))
}

/// Default kernel recv-buffer size for the *server* UDP socket (64 MiB). quinn never
/// raises `SO_RCVBUF` itself, so a high-rate receiver drops datagrams whenever the
/// socket backlog spikes (→ QUIC RTO stalls, seen as throughput variance at the
/// single-host ceiling). We force it up by default — matching the total budget the
/// client pool targets ([`DEFAULT_UDP_BUF_TOTAL_BYTES`]) and the value validated in
/// the throughput sweep. Override (typically to *reduce* it on a small host) with
/// `MM_QUIC_SERVER_UDP_BUF_BYTES`.
const DEFAULT_SERVER_UDP_BUF_BYTES: usize = 64 * 1024 * 1024;

fn server_udp_buf_bytes() -> usize {
    std::env::var("MM_QUIC_SERVER_UDP_BUF_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_SERVER_UDP_BUF_BYTES)
}

/// Build a server [`Endpoint`] bound to `addr`. Like [`Endpoint::server`] but (a) it
/// uses [`endpoint_config`] so the advertised `max_udp_payload_size` matches our MTU
/// discovery ceiling (a plain `Endpoint::server` hardcodes quinn's 1472 default, which
/// would clamp the peer's probed MTU back down), and (b) it enlarges the receive
/// buffer to [`server_udp_buf_bytes`] (best-effort; see [`set_udp_buffers`]).
fn make_server_endpoint(addr: SocketAddr, server_config: ServerConfig) -> anyhow::Result<Endpoint> {
    let domain = if addr.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    set_udp_buffers(&socket, server_udp_buf_bytes());
    socket.bind(&addr.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    let runtime =
        quinn::default_runtime().ok_or_else(|| anyhow::anyhow!("no quic async runtime"))?;
    Ok(Endpoint::new(
        endpoint_config(),
        Some(server_config),
        std_socket,
        runtime,
    )?)
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
    // When set, the multi-threaded runtime to build endpoints under, so quinn spawns
    // each endpoint's driver (which runs the packet crypto) on the pool rather than on
    // the command-loop thread. Entered around `Endpoint::new` in `bind`/`dialer`.
    rt: Option<Handle>,
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
            if crate::ctx::connection_debug() {
                eprintln!("MM_QUIC client pool: {n} endpoints (explicit), {per} B requested each");
            }
            return Ok(pool);
        }

        // Adaptive: try to put the whole budget on one socket.
        let (first, usable) = make_client_endpoint(bind, client_config.clone(), total)?;
        if usable >= total {
            if crate::ctx::connection_debug() {
                eprintln!("MM_QUIC client pool: 1 endpoint holds the full {total} B budget");
            }
            return Ok(vec![first]);
        }

        // Capped below target — spread across enough sockets of `usable` each.
        let n = total.div_ceil(usable.max(1));
        if crate::ctx::connection_debug() {
            eprintln!(
                "MM_QUIC client pool: single socket capped at {usable} B usable; using {n} endpoints to reach {total} B total"
            );
        }
        let mut pool = Vec::with_capacity(n);
        pool.push(first);
        for _ in 1..n {
            pool.push(make_client_endpoint(bind, client_config.clone(), usable)?.0);
        }
        Ok(pool)
    }
}

/// A dialing handle: a pooled client endpoint plus the data runtime to enter when
/// dialing, so quinn spawns the resulting connection's driver (which runs its packet
/// crypto) on that runtime rather than on whatever thread calls [`Net::connect`] (the
/// command loop). `None` ⇒ the connection driver follows the caller (single core).
#[derive(Clone)]
pub(crate) struct QuicDialer {
    endpoint: Endpoint,
    rt: Option<Handle>,
}

/// A bound server endpoint plus the data runtime to enter when accepting, for the same
/// reason as [`QuicDialer`]: the accepted connection's driver spawns on that runtime.
pub(crate) struct QuicListener {
    endpoint: Endpoint,
    rt: Option<Handle>,
}

impl Net for Quic {
    // A pooled client endpoint (cheap clone; internally reference-counted) plus the
    // runtime to place dialed connections' drivers on.
    type Dialer = QuicDialer;
    // A server endpoint plus that runtime. quic multiplexes all streams on one
    // connection; each stream is opened/accepted on demand, addressed by index.
    type Listener = QuicListener;
    type Conn = QuicConn;

    fn create(runtime: Option<Handle>) -> anyhow::Result<Self> {
        Ok(Self {
            tls: None,
            client_endpoints_v4: Vec::new(),
            client_endpoints_v6: Vec::new(),
            client_rr: 0,
            rt: runtime,
        })
    }

    fn parse_addr(url: &str) -> anyhow::Result<SocketAddr> {
        parse_addr(url)
    }

    fn dialer(&mut self, addr: SocketAddr) -> anyhow::Result<QuicDialer> {
        // Enter the data runtime (if any) so quinn spawns the pool endpoints' *endpoint*
        // drivers there. Clone the handle first: the guard borrows the clone, leaving
        // `self` free for the `&mut self` pool build below. The handle also rides along
        // on the QuicDialer so `connect` can place each connection's driver there too.
        let rt = self.rt.clone();
        let endpoint = {
            let _guard = rt.as_ref().map(Handle::enter);
            self.client_endpoint(&addr)?
        };
        Ok(QuicDialer { endpoint, rt })
    }

    fn bind(&mut self, addr: SocketAddr) -> anyhow::Result<QuicListener> {
        let server = self.tls()?.server.clone();
        // Enter the data runtime (if any) so quinn spawns this server endpoint's driver
        // there; the handle also rides on the QuicListener for `accept` to use.
        let rt = self.rt.clone();
        let endpoint = {
            let _guard = rt.as_ref().map(Handle::enter);
            make_server_endpoint(addr, server)?
        };
        Ok(QuicListener { endpoint, rt })
    }

    async fn accept(listener: &QuicListener) -> Option<QuicConn> {
        let incoming = listener.endpoint.accept().await?;
        // `incoming.accept()` synchronously creates the Connecting and spawns its
        // connection driver (via ambient `tokio::spawn`), so enter the data runtime
        // around exactly that call to place the driver — and its crypto — on the pool.
        let connecting = {
            let _guard = listener.rt.as_ref().map(Handle::enter);
            incoming.accept().ok()?
        };
        let conn = connecting.await.ok()?;
        Some(QuicConn::new_acceptor(conn))
    }

    async fn connect(dialer: &QuicDialer, addr: SocketAddr) -> Option<QuicConn> {
        // connect() fails synchronously on a bad config; the handshake (.await) fails
        // until the server is up. Both surface as `None` and the caller retries. Enter
        // the data runtime around the synchronous connect() — which spawns the
        // connection driver — so that driver's crypto runs on the pool, not the caller.
        let connecting = {
            let _guard = dialer.rt.as_ref().map(Handle::enter);
            dialer.endpoint.connect(addr, SERVER_NAME).ok()?
        };
        let conn = connecting.await.ok()?;
        Some(QuicConn::new_dialer(conn))
    }

    /// quic multiplexes all streams on one endpoint-paced connection, so a large number
    /// of simultaneous handshakes is cheap; 1024 has proven safe at 64k+ fan-out.
    /// Overridable via `MM_QUIC_MAX_CONCURRENT_CONNECTS`.
    fn default_connect_concurrency() -> Option<usize> {
        Some(1024)
    }
}

/// The two-byte little-endian index prefix the dialer writes at the head of every
/// bidirectional stream it opens, so the acceptor can pair it with the peer's stream of
/// the same index (a bare quic connection is already private to the two peers, so unlike
/// tcp no connection id is needed — only the stream index). `open_bi`/`accept_bi` pair
/// in open order per initiator, but only the dialer opens, and it opens streams in
/// whatever order the transport requests them; the explicit index removes any dependence
/// on that order and lets the acceptor demultiplex.
async fn write_index_prefix(send: &mut SendStream, index: usize) -> io::Result<()> {
    // quinn's inherent `write_all` returns its own `WriteError`; map it to io::Error.
    send.write_all(&(index as u16).to_le_bytes())
        .await
        .map_err(io::Error::other)
}

async fn read_index_prefix(recv: &mut RecvStream) -> io::Result<usize> {
    let mut buf = [0u8; 2];
    // quinn's inherent `read_exact` returns its own `ReadExactError`; map it to io::Error.
    recv.read_exact(&mut buf).await.map_err(io::Error::other)?;
    Ok(u16::from_le_bytes(buf) as usize)
}

/// A [`NetConn::stream`] request handed to an acceptor connection's demux: the wanted
/// index, its send priority, and the one-shot the demux fulfils with the paired stream.
pub(crate) struct QuicStreamRequest {
    index: usize,
    priority: i32,
    reply: oneshot::Sender<io::Result<(QuicSend, QuicRecv)>>,
}

/// Fulfil a request with its paired stream: set the send priority and send the halves
/// back through the one-shot.
fn fulfill_quic(conn: &Connection, req: QuicStreamRequest, stream: (SendStream, RecvStream)) {
    let (send, recv) = stream;
    let _ = send.set_priority(req.priority);
    let _ = req.reply.send(Ok(quic_halves(conn.clone(), send, recv)));
}

/// The acceptor-side demux for one connection: the single owner of `accept_bi`. It reads
/// each accepted stream's index prefix and pairs it with the matching [`QuicStreamRequest`]
/// using a per-index [`Matcher`] (each index sees at most one stream and one request,
/// whichever arrives first parks for the other). Being the only accept-driver means
/// `stream` never touches `accept_bi` directly — it just sends a request and awaits the
/// one-shot — so there is no mutex held across an await. Ends when the connection drops
/// (its senders close) or `accept_bi` errors; dropping the parked requests then fails
/// their awaiting `stream` calls.
async fn quic_acceptor_demux(
    conn: Connection,
    mut requests: mpsc::UnboundedReceiver<QuicStreamRequest>,
) {
    let mut matchers: HashMap<usize, Matcher<QuicStreamRequest, (SendStream, RecvStream)>> =
        HashMap::new();
    loop {
        tokio::select! {
            req = requests.recv() => {
                let Some(req) = req else {
                    return; // every `stream` handle dropped: the connection is going away
                };
                matchers
                    .entry(req.index)
                    .or_insert_with(Matcher::new)
                    .push_left(req, |req, stream| fulfill_quic(&conn, req, stream));
            }
            // `accept_bi` is cancel-safe, so racing it in `select!` never drops a stream.
            accepted = conn.accept_bi() => {
                let Ok((send, mut recv)) = accepted else {
                    return; // connection lost: dropping the matchers fails parked requests
                };
                let Ok(index) = read_index_prefix(&mut recv).await else {
                    continue; // peer opened a stream with a bad prefix — drop it
                };
                matchers
                    .entry(index)
                    .or_insert_with(Matcher::new)
                    .push_right((send, recv), |req, stream| fulfill_quic(&conn, req, stream));
            }
        }
    }
}

/// One QUIC connection, in one of two roles set at construction. A **dialer** connection
/// (from [`Net::connect`]) opens each requested stream with `open_bi` and writes the
/// index prefix. An **acceptor** connection (from [`Net::accept`]) delegates all
/// `accept_bi` work to a per-connection [`quic_acceptor_demux`] coroutine; its `stream`
/// just sends a request down `requests` and awaits the paired stream. The demux holds the
/// `Connection` alive; the acceptor keeps `remote` only for [`fmt::Debug`].
///
/// Cloneable so the reader, writer, heartbeat task, and striping coordinator can
/// each hold a handle: a dialer clone shares the `Connection` (internally
/// reference-counted); an acceptor clone shares the request-sender to the one demux.
#[derive(Clone)]
pub(crate) enum QuicConn {
    Dialer {
        conn: Connection,
    },
    Acceptor {
        remote: SocketAddr,
        requests: mpsc::UnboundedSender<QuicStreamRequest>,
    },
}

impl QuicConn {
    fn new_dialer(conn: Connection) -> Self {
        Self::Dialer { conn }
    }

    fn new_acceptor(conn: Connection) -> Self {
        let remote = conn.remote_address();
        let (requests, rx) = mpsc::unbounded_channel();
        // Spawn on the command-loop LocalSet (`accept` runs there); the demux owns the
        // connection and lives until every `stream` handle is dropped or it errors.
        tokio::task::spawn_local(quic_acceptor_demux(conn, rx));
        Self::Acceptor { remote, requests }
    }
}

impl fmt::Debug for QuicConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let remote = match self {
            QuicConn::Dialer { conn } => conn.remote_address(),
            QuicConn::Acceptor { remote, .. } => *remote,
        };
        f.debug_tuple("QuicConn").field(&remote).finish()
    }
}

fn quic_halves(conn: Connection, send: SendStream, recv: RecvStream) -> (QuicSend, QuicRecv) {
    (
        QuicSend {
            send,
            _conn: conn.clone(),
        },
        QuicRecv { recv, _conn: conn },
    )
}

impl NetConn for QuicConn {
    type Send = QuicSend;
    type Recv = QuicRecv;
    type Stream = Pin<Box<dyn Future<Output = io::Result<(QuicSend, QuicRecv)>> + Send>>;

    fn stream(&self, index: usize, priority: i32) -> Self::Stream {
        match self {
            QuicConn::Dialer { conn } => {
                let conn = conn.clone();
                Box::pin(async move {
                    let (mut send, recv) = conn.open_bi().await.map_err(io::Error::other)?;
                    write_index_prefix(&mut send, index).await?;
                    let _ = send.set_priority(priority);
                    Ok(quic_halves(conn, send, recv))
                })
            }
            QuicConn::Acceptor { requests, .. } => {
                let requests = requests.clone();
                Box::pin(async move {
                    let (reply, rx) = oneshot::channel();
                    requests
                        .send(QuicStreamRequest {
                            index,
                            priority,
                            reply,
                        })
                        .map_err(|_| io::Error::other("quic connection closed"))?;
                    rx.await
                        .map_err(|_| io::Error::other("quic connection closed"))?
                })
            }
        }
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
