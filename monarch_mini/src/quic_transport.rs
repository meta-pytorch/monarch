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

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
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
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;

use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::connection::SideChannelMessage;
use crate::connection::sever;
use crate::ctx::Command;
use crate::framing;
use crate::framing::Incoming;
use crate::framing::Preamble;
use crate::framing::SideChannelIncoming;
use crate::matcher::Matcher;
use crate::quic_heartbeat::BeatKind;
use crate::quic_heartbeat::ConnectionId;
use crate::quic_heartbeat::Heartbeat;
use crate::quic_heartbeat::HeartbeatEvent;
use crate::quic_heartbeat::Heartbeats;
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

/// On graceful shutdown each writer sends an explicit `Severed{"context shutdown"}`
/// frame (reliable stream data — retransmitted by QUIC, unlike `CONNECTION_CLOSE`),
/// then keeps the connection open until the peer *responds* (its own `Severed`/EOF,
/// observed by our reader) before closing — so we close as soon as we know the peer
/// got it, rather than after a fixed delay. This is the upper bound on that wait,
/// for a peer that never responds (e.g. already dead). Tunable via
/// `MM_QUIC_SHUTDOWN_ACK_TIMEOUT_MS`.
const DEFAULT_SHUTDOWN_ACK_TIMEOUT: Duration = Duration::from_secs(10);

fn shutdown_ack_timeout() -> Duration {
    std::env::var("MM_QUIC_SHUTDOWN_ACK_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SHUTDOWN_ACK_TIMEOUT)
}

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

    // Disable all periodic QUIC traffic so a delegated link is truly silent:
    // `keep_alive_interval = None` (no PING keep-alives) and `max_idle_timeout =
    // None` (QUIC must not reap an idle delegated connection — liveness is now the
    // heartbeat subsystem's responsibility, not the transport's). Applied to both
    // roles. Message-carrying links are unaffected; delegated links rely on the
    // sibling fabric. See `HEARTBEAT_DELEGATION_DESIGN.md` §9.
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(None);
    transport.max_idle_timeout(None);
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

/// The gateway dial tag of `ident` (the substring after the last `@`), as a
/// `String`, or `None` if it has none or is non-utf8. This is the address a beat is
/// dialed at — the recipient's own gateway.
fn gateway_tag_str(ident: &[u8]) -> Option<String> {
    let pos = ident.iter().rposition(|&b| b == b'@')?;
    let tag = &ident[pos + 1..];
    if tag.is_empty() {
        return None;
    }
    std::str::from_utf8(tag).ok().map(str::to_owned)
}

fn parse_addr(url: &str) -> anyhow::Result<SocketAddr> {
    let authority = url.strip_prefix("quic://").unwrap_or(url);
    authority
        .parse::<SocketAddr>()
        .map_err(|err| anyhow::anyhow!("invalid quic address {authority:?}: {err}"))
}

/// The wildcard client bind address matching `target`'s address family. A QUIC
/// client endpoint binds a local UDP socket before dialing, and that socket's
/// family must match the destination: an IPv4-bound socket cannot reach an IPv6
/// peer (and vice versa). Dialing across machines is normally over IPv6, so this
/// must pick `[::]:0` for an IPv6 target rather than always binding `0.0.0.0:0`.
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
/// [`client_udp_buf_per_socket`]), so adding endpoints shrinks each socket's share
/// while keeping the aggregate fixed. This matters for non-root deployments: a
/// per-socket request is clamped to `net.core.rmem_max`, so to reach a large total
/// without `CAP_NET_ADMIN` you spread it over more sockets. Override the total with
/// `MM_QUIC_UDP_BUF_BYTES`.
const DEFAULT_UDP_BUF_TOTAL_BYTES: usize = 64 * 1024 * 1024;

fn client_udp_buf_total_bytes() -> usize {
    std::env::var("MM_QUIC_UDP_BUF_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_UDP_BUF_TOTAL_BYTES)
}

/// Optional explicit override for the number of client endpoints. When unset, the
/// pool size is chosen adaptively (see [`QuicTransport::build_client_pool`]): one
/// socket if it can hold the whole buffer budget, otherwise enough sockets to reach
/// it. When set, exactly that many sockets are used, each requesting an even split
/// of the budget.
fn explicit_client_endpoints() -> Option<usize> {
    std::env::var("MM_QUIC_CLIENT_ENDPOINTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
}

/// Optional cap on how many client connect *attempts* may be in flight at once
/// across the whole context. A root that joins tens of thousands of peers spawns
/// that many connector tasks, each driving a QUIC handshake; run all at once on the
/// single-threaded runtime they compete for CPU and reorder connection setup
/// badly. `MM_QUIC_MAX_CONCURRENT_CONNECTS` bounds the simultaneous attempts (each
/// connector still retries independently, and the permit is released while it backs
/// off). Unset or `0` ⇒ unlimited (the original behaviour).
fn max_concurrent_connects() -> Option<usize> {
    std::env::var("MM_QUIC_MAX_CONCURRENT_CONNECTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
}

/// When `MM_QUIC_UDP_BUF_NO_FORCE` is set, skip the privileged `SO_*BUFFORCE`
/// options so the buffer is exactly what an unprivileged process would get
/// (clamped to `net.core.rmem_max`). Used to validate a non-root-deployable
/// config even while the job happens to run as root.
fn udp_buf_force_disabled() -> bool {
    std::env::var("MM_QUIC_UDP_BUF_NO_FORCE").is_ok_and(|v| v != "0" && !v.is_empty())
}

/// Enlarge a UDP socket's kernel send/recv buffers, best-effort. Tries the
/// privileged `SO_*BUFFORCE` options first — these bypass the
/// `net.core.{r,w}mem_max` ceiling and work when the process has `CAP_NET_ADMIN`
/// (e.g. running as root on MAST) — and falls back to the ordinary setters (which
/// the kernel clamps to that ceiling) when not permitted.
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
    // SAFETY: `fd` is a valid UDP socket for the call's duration; the option value
    // is an `int` of length `len`, as required by SO_RCVBUFFORCE/SO_SNDBUFFORCE.
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
/// `requested` bytes (see [`set_udp_buffers`]), bound to `bind`, with
/// `client_config` as its default. Returns the endpoint and the *usable* recv
/// buffer the kernel actually granted (it reports ~2× the usable size for
/// bookkeeping, so we halve it), which the caller uses to decide whether one
/// socket reached the target or a pool is needed.
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

/// What a gateway side-channel writer carries: either a routable
/// [`SideChannelMessage`] (from ctx) or a transport-internal delegated-heartbeat
/// beat/ack (from the heartbeat coroutines). Both ride the same per-gateway writer,
/// so a beat reuses the exact connection an ordinary message would.
pub(crate) enum SideChannelOut {
    Message(SideChannelMessage),
    Heartbeat {
        recipient: Vec<u8>,
        from: Vec<u8>,
        conn_id: ConnectionId,
        kind: BeatKind,
    },
}

/// The client-side dialing + gateway side-channel state, shared (`Rc<RefCell>`)
/// between the command-loop-facing [`QuicTransport`] and the heartbeat coroutines.
/// QUIC owns every side-channel path end to end — opening, reusing, and writing it
/// — so a delegated-heartbeat beat is sent by the heartbeat coroutine directly
/// through here, never routed through ctx. Single-threaded (LocalSet), hence
/// `Rc<RefCell>`. TLS is built lazily on first use and cached.
pub(crate) struct SideChannels {
    shutdown_tx: watch::Sender<bool>,
    tls: Option<Arc<TlsConfig>>,
    // A *pool* of client endpoints (one UDP socket + driver each) per address
    // family, created lazily and assigned round-robin to joins/side-channels.
    //
    // One endpoint per connection (the original behaviour) exhausted sockets and
    // the event loop at high fan-out. Collapsing to a single shared endpoint fixed
    // that but made the one UDP socket a throughput bottleneck: a burst of tens of
    // thousands of sends/receives overflows its buffers, dropping packets and
    // forcing multi-second QUIC retransmit timeouts. A small pool is the middle
    // ground — connections (and their burst load) are spread across `K` sockets.
    // `K` is `MM_QUIC_CLIENT_ENDPOINTS` (default below).
    client_endpoints_v4: Vec<Endpoint>,
    client_endpoints_v6: Vec<Endpoint>,
    // Round-robin cursor for assigning connections to pool endpoints.
    client_rr: usize,
    // One side-channel writer per remote gateway, keyed by its dial address (the
    // `@specifier` tag). Each owns a task that lazily connects (retrying until the
    // gateway is live), streams frames, and reconnects if the connection drops.
    // Cached across messages/beats and shared by ordinary messages and heartbeats.
    channels: HashMap<String, mpsc::UnboundedSender<SideChannelOut>>,
    // Liveness-token issuer: each connection's/side-channel's writer holds a clone
    // for its lifetime. Teardown drops this issuing copy so `alive_rx` closes once
    // every writer has flushed and exited.
    alive_tx: Option<mpsc::UnboundedSender<()>>,
}

/// Owns all QUIC transport state and coroutines. Mirrors `UnixTransport`: the
/// command loop holds one and forwards serves/joins to it; it never sees streams
/// or pairing state.
pub(crate) struct QuicTransport {
    loop_tx: mpsc::UnboundedSender<Command>,
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
    // Bounds simultaneous client connect attempts (see `max_concurrent_connects`).
    // Shared by every connector task; `None` ⇒ unlimited. Created once so all joins
    // in this context contend for the same pool of attempt slots.
    connect_sem: Option<Arc<Semaphore>>,
    // Closed once every writer has exited (see `SideChannels::alive_tx`).
    alive_rx: mpsc::UnboundedReceiver<()>,
    // Delegated-heartbeat state, shared by every connection's heartbeat coroutine.
    // Spawns the per-connection coroutines and routes inbound side-channel beats.
    // See [`crate::quic_heartbeat`].
    heartbeat: Heartbeats,
    // Client dialing + side-channel writers, shared with the heartbeat coroutines
    // so QUIC drives every side-channel path without ctx involvement.
    side_channels: Rc<RefCell<SideChannels>>,
}

impl QuicTransport {
    pub(crate) fn new(
        loop_tx: mpsc::UnboundedSender<Command>,
        mapper: MapperHandle,
        context_shm: ShmClientSlot,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let (alive_tx, alive_rx) = mpsc::unbounded_channel();
        if let Some(n) = max_concurrent_connects() {
            eprintln!("MM_QUIC connect concurrency capped at {n} simultaneous attempts");
        }
        Self {
            loop_tx,
            mapper,
            context_shm,
            listeners: HashMap::new(),
            connect_sem: max_concurrent_connects().map(|n| Arc::new(Semaphore::new(n))),
            alive_rx,
            heartbeat: Heartbeats::new(),
            side_channels: Rc::new(RefCell::new(SideChannels {
                shutdown_tx,
                tls: None,
                client_endpoints_v4: Vec::new(),
                client_endpoints_v6: Vec::new(),
                client_rr: 0,
                channels: HashMap::new(),
                alive_tx: Some(alive_tx),
            })),
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
}

impl SideChannels {
    fn alive_token(&self) -> mpsc::UnboundedSender<()> {
        self.alive_tx
            .as_ref()
            .expect("alive-token issuer present before shutdown")
            .clone()
    }

    fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
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

    /// A client [`Endpoint`] for `target`'s address family, assigned round-robin
    /// from a lazily-created pool of endpoints. Returns a cheap clone (the endpoint
    /// is internally reference-counted). Spreading connections across several UDP
    /// sockets keeps any one socket's send/receive buffers from overflowing under a
    /// fan-out burst, which otherwise causes packet loss and multi-second QUIC
    /// retransmit stalls.
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
    /// [`client_udp_buf_total_bytes`] of total UDP buffer. First try to get it all
    /// on a single socket; if the kernel caps a single socket below the target
    /// (e.g. unprivileged, clamped to `net.core.rmem_max`), fall back to enough
    /// sockets — each requesting the measured per-socket max — to reach the total.
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

    /// The writer for the gateway at `tag`, opening (and caching) it on first use.
    /// `None` if the tag is unparseable or an endpoint can't be built.
    fn writer_for(&mut self, tag: String) -> Option<&mpsc::UnboundedSender<SideChannelOut>> {
        if !self.channels.contains_key(&tag) {
            let addr = match parse_addr(&tag) {
                Ok(addr) => addr,
                Err(err) => {
                    tracing::warn!("side channel address: {err:#}");
                    return None;
                }
            };
            let endpoint = match self.client_endpoint(&addr) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    tracing::warn!("side channel endpoint: {err:#}");
                    return None;
                }
            };
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::task::spawn_local(side_channel_writer_task(
                addr,
                endpoint,
                rx,
                self.shutdown_tx.subscribe(),
                self.alive_token(),
            ));
            self.channels.insert(tag.clone(), tx);
        }
        self.channels.get(&tag)
    }

    /// Send a self-addressing [`SideChannelMessage`] to the gateway at `tag`,
    /// opening (and caching) the connection on first use. Best-effort and not
    /// heartbeated; a message enqueued while the remote gateway is not yet live
    /// waits for the connection to come up.
    fn send_message(&mut self, tag: String, message: SideChannelMessage) {
        if let Some(tx) = self.writer_for(tag) {
            let _ = tx.send(SideChannelOut::Message(message));
        }
    }

    /// Send a delegated-heartbeat beat/ack to `recipient`, dialing the side channel
    /// at `recipient`'s own gateway `@tag` and reusing the same per-gateway side
    /// channel an ordinary message would (design §6.2). Dropped if `recipient` has
    /// no gateway tag to dial. Called by the heartbeat coroutines directly — never
    /// via ctx.
    pub(crate) fn send_heartbeat(
        &mut self,
        recipient: Vec<u8>,
        from: Vec<u8>,
        conn_id: ConnectionId,
        kind: BeatKind,
    ) {
        let Some(tag) = gateway_tag_str(&recipient) else {
            return;
        };
        if let Some(tx) = self.writer_for(tag) {
            let _ = tx.send(SideChannelOut::Heartbeat {
                recipient,
                from,
                conn_id,
                kind,
            });
        }
    }

    /// Signal every writer to flush and exit, and drop the issuing alive token.
    fn begin_shutdown(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.alive_tx = None;
    }
}

impl QuicTransport {
    /// Send a self-addressing [`SideChannelMessage`] to a remote gateway over a
    /// direct side-channel. The caller (the command loop) has already derived `tag`
    /// from the message's `gateway_for_actor`.
    pub(crate) fn send_to_gateway(&mut self, tag: String, message: SideChannelMessage) {
        self.side_channels.borrow_mut().send_message(tag, message);
    }

    /// Signal every writer to flush and exit, then wait until they all have.
    ///
    /// Each writer, on this signal, sends an explicit `Severed{"context shutdown"}`
    /// frame and then holds its connection open for [`shutdown_grace`] so QUIC can
    /// reliably deliver that frame (retransmitting if the first packet is lost)
    /// before the connection is dropped. So by the time every writer has exited
    /// (`alive_rx` closed) the peers have been told — we do not rely on the lossy
    /// `CONNECTION_CLOSE`/socket-drop being noticed.
    pub(crate) async fn shutdown(&mut self) {
        self.side_channels.borrow_mut().begin_shutdown();
        let _ = self.alive_rx.recv().await;
    }
}

impl Transport for QuicTransport {
    fn serve(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot) {
        let tls = match self.side_channels.borrow_mut().tls() {
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
            let shutdown = self.side_channels.borrow().subscribe_shutdown();
            let alive = self.side_channels.borrow().alive_token();
            tokio::task::spawn_local(listener_task(
                addr,
                tls.server.clone(),
                rx,
                self.loop_tx.clone(),
                shutdown,
                alive,
                context_shm,
                self.heartbeat.clone(),
                self.side_channels.clone(),
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
        let addr = match parse_addr(&url) {
            Ok(addr) => addr,
            Err(err) => {
                sever(&self.loop_tx, connection, format!("{err:#}").into_bytes());
                return;
            }
        };
        let endpoint = match self.side_channels.borrow_mut().client_endpoint(&addr) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                sever(
                    &self.loop_tx,
                    connection,
                    format!("quic client endpoint: {err:#}").into_bytes(),
                );
                return;
            }
        };
        let shutdown = self.side_channels.borrow().subscribe_shutdown();
        let alive = self.side_channels.borrow().alive_token();
        tokio::task::spawn_local(connector_task(
            addr,
            endpoint,
            connection,
            self.loop_tx.clone(),
            shutdown,
            alive,
            self.shm_ctx(shm_client),
            self.connect_sem.clone(),
            self.heartbeat.clone(),
            self.side_channels.clone(),
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
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of listener state; bundling adds indirection without clarifying anything"
)]
async fn listener_task(
    addr: SocketAddr,
    server_config: ServerConfig,
    mut serves: mpsc::UnboundedReceiver<(ConnectionRef, ShmCtx)>,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    side_channel_shm: ShmCtx,
    heartbeat: Heartbeats,
    side_channels: Rc<RefCell<SideChannels>>,
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
    // One spawn callback for both Matcher arms: the Matcher hands back
    // `(serve_side, accepted_side)` either way, so a serve-first (`push_left`) and
    // an accept-first (`push_right`) pairing spawn identically — serve/acceptor
    // side, so `log_heartbeats = true` and `dialed = false` (the peer dialed us).
    // Owns a `shutdown` clone (the select below needs `&mut shutdown` for
    // `changed()`); the rest are cloned per spawn.
    let spawn_shutdown = shutdown.clone();
    let spawn = |(connection, shm): (ConnectionRef, ShmCtx),
                 (conn, send, recv): (Connection, SendStream, RecvStream)| {
        spawn_connection(
            send,
            recv,
            connection,
            loop_tx.clone(),
            spawn_shutdown.clone(),
            alive.clone(),
            conn,
            shm,
            true,
            false,
            heartbeat.clone(),
            side_channels.clone(),
        );
    };
    // The Matcher just pairs the two sides; `spawn` (reused across iterations) does
    // the work, so the pairing callback is a trivial identity.
    loop {
        tokio::select! {
            serve = serves.recv() => {
                let Some((connection, shm)) = serve else { return; };
                if let Some((left, right)) = matcher.push_left((connection, shm), |l, r| (l, r)) {
                    spawn(left, right);
                }
            }
            accepted = accepted_rx.recv() => {
                match accepted {
                    None => return,
                    Some(Accepted::Join(conn, send, recv)) => {
                        if let Some((left, right)) =
                            matcher.push_right((conn, send, recv), |l, r| (l, r))
                        {
                            spawn(left, right);
                        }
                    }
                    Some(Accepted::SideChannel(conn, recv)) => {
                        tokio::task::spawn_local(side_channel_reader_task(
                            conn, recv, loop_tx.clone(), side_channel_shm.clone(),
                            heartbeat.clone(),
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

/// Read frames off a gateway side-channel. A routable [`SideChannelMessage`] is
/// forwarded to the command loop (which resolves the owning gateway); a
/// delegated-heartbeat beat/ack is routed *directly* into the heartbeat subsystem
/// here — it never reaches ctx. There is no establishment: an EOF or error just
/// ends the reader (the sending gateway reconnects when it next has something to
/// send). `_conn` is held only to keep the QUIC connection (and thus `recv`) alive.
async fn side_channel_reader_task(
    _conn: Connection,
    mut recv: RecvStream,
    loop_tx: mpsc::UnboundedSender<Command>,
    shm: ShmCtx,
    heartbeat: Heartbeats,
) {
    loop {
        match framing::read_side_channel(&mut recv, &shm.mapper, shm.client()).await {
            Ok(SideChannelIncoming::Message(message)) => {
                if loop_tx.send(Command::SideChannelDeliver(message)).is_err() {
                    return;
                }
            }
            Ok(SideChannelIncoming::Heartbeat {
                recipient,
                from,
                conn_id,
                kind,
            }) => {
                heartbeat.deliver(&recipient, from, conn_id, kind);
            }
            Err(_) => return,
        }
    }
}

/// Connect to `addr`, retrying until the server binds and the handshake succeeds
/// (so a join may precede its serve), then open one bi-stream and wire it up.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of connector state; bundling adds indirection without clarifying anything"
)]
async fn connector_task(
    addr: SocketAddr,
    endpoint: Endpoint,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
    // Bounds how many connect attempts run at once across the context (see
    // `max_concurrent_connects`). `None` ⇒ unlimited. A permit is held only for the
    // duration of one attempt (connect + handshake + open_bi) and released before
    // backing off, so a waiting task can attempt while this one sleeps.
    connect_sem: Option<Arc<Semaphore>>,
    heartbeat: Heartbeats,
    side_channels: Rc<RefCell<SideChannels>>,
) {
    let mut retry = CONNECT_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return;
        }
        // Take a connect-attempt permit before doing any handshake work, so a root
        // joining tens of thousands of peers drives at most N handshakes at once
        // instead of spawning them all to fight over the single-threaded runtime.
        let permit = match &connect_sem {
            Some(sem) => Some(
                Arc::clone(sem)
                    .acquire_owned()
                    .await
                    .expect("connect semaphore never closed"),
            ),
            None => None,
        };
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
                    // Connection is up; free the attempt slot before handing off.
                    drop(permit);
                    // Joiner side (the root has tens of thousands of writers): do not
                    // log heartbeat sends here even under debug — it would flood.
                    spawn_connection(
                        send,
                        recv,
                        connection,
                        loop_tx,
                        shutdown,
                        alive,
                        conn,
                        shm,
                        false,
                        // We dialed this connection (join): the delegation guard's
                        // "parent is the joiner" precondition holds on this side.
                        true,
                        heartbeat,
                        side_channels,
                    );
                    return;
                }
                Err(_) => {
                    sever(&loop_tx, connection, b"quic open stream failed".to_vec());
                    return;
                }
            }
        }
        // Attempt failed; release the slot so another task can try while we back off.
        drop(permit);
        tokio::select! {
            _ = tokio::time::sleep(retry) => {}
            _ = shutdown.changed() => return,
        }
        retry = (retry * 2).min(CONNECT_RETRY_MAX);
    }
}

/// Drive a gateway side-channel writer: drain queued outbound items — routable
/// [`SideChannelMessage`]s and transport-internal heartbeat beats/acks alike —
/// writing each as a frame to the remote gateway. The connection is established
/// lazily on the first item (retrying until the gateway is live) and reconnected if
/// it drops. An item in flight when the connection drops may be lost; the channel
/// is best-effort by design (any gateway may drop a side-channel to reclaim state).
async fn side_channel_writer_task(
    addr: SocketAddr,
    endpoint: Endpoint,
    mut rx: mpsc::UnboundedReceiver<SideChannelOut>,
    mut shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
) {
    // The current connection, if up: its send stream plus the QUIC connection,
    // held to keep it open. `None` means we must (re)connect before the next write.
    let mut stream: Option<(SendStream, Connection)> = None;
    loop {
        let item = tokio::select! {
            item = rx.recv() => match item {
                Some(item) => item,
                None => break, // sender dropped (gateway gone)
            },
            _ = shutdown.changed() => break,
        };
        if stream.is_none() {
            let Some((mut send, conn)) = connect_side_channel(&endpoint, addr, &mut shutdown).await
            else {
                break; // shutting down before the gateway came up
            };
            // Announce the stream as a side-channel so the peer reads messages
            // rather than driving a join handshake.
            if framing::write_preamble(&mut send, Preamble::SideChannel)
                .await
                .is_err()
            {
                continue; // reconnect on the next item (this one is dropped)
            }
            stream = Some((send, conn));
        }
        let (send, _conn) = stream.as_mut().expect("stream is connected");
        let wrote = match item {
            SideChannelOut::Message(message) => framing::write_side_channel(send, message).await,
            SideChannelOut::Heartbeat {
                recipient,
                from,
                conn_id,
                kind,
            } => framing::write_side_channel_heartbeat(send, recipient, from, conn_id, kind).await,
        };
        if wrote.is_err() {
            stream = None; // dropped; reconnect on the next item
        }
    }
    if let Some((mut send, _conn)) = stream {
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
) -> Option<(SendStream, Connection)> {
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
                return Some((send, conn));
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
/// timeout). `conn` (the QUIC connection) is moved into the writer to keep it
/// alive for the writer's duration; dropping it closes the connection, which the
/// peer observes. The client endpoint is not held per-connection — it is owned
/// (shared) by the [`QuicTransport`] for the whole context lifetime.
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
    conn: Connection,
    shm: ShmCtx,
    // Forwarded to the writer: log heartbeat sends (gated to the serve/acceptor side
    // by the call sites, and further gated by MM_QUIC_DEBUG in the writer).
    log_heartbeats: bool,
    // Whether *we* dialed this connection (join / "quic connect"). One half of the
    // delegation guard (§8): only a link the parent dialed may be delegated.
    dialed: bool,
    heartbeat: Heartbeats,
    side_channels: Rc<RefCell<SideChannels>>,
) {
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    // Reader → writer signal: the peer responded to our shutdown, so the writer
    // may close. Unbounded but only ever carries a single `()`.
    let (peer_responded_tx, peer_responded_rx) = mpsc::unbounded_channel();
    // heartbeat coroutine → writer: beats to write out.
    let (beats_tx, beats_rx) = mpsc::unbounded_channel();
    let transport = Box::new(QuicConnectionTransport { tx: writer_tx });
    let _ = loop_tx.send(Command::TransportConnected {
        connection,
        transport,
    });
    // The heartbeat coroutine sends its side-channel beats through this closure — it
    // never sees the transport's side-channel types, keeping `quic_heartbeat`
    // decoupled from the transport. The closure reuses the existing gateway side
    // channels.
    let send_beat =
        move |recipient: Vec<u8>, from: Vec<u8>, conn_id: ConnectionId, kind: BeatKind| {
            side_channels
                .borrow_mut()
                .send_heartbeat(recipient, from, conn_id, kind);
        };
    // Spawn the coroutine; the returned sender is how the reader/writer feed it
    // inbound beats, Establish snoops, and reader-closed.
    let hb_event_tx = heartbeat.spawn(connection, dialed, beats_tx, send_beat, loop_tx.clone());
    tokio::task::spawn_local(writer_task(
        send,
        writer_rx,
        shutdown,
        alive,
        peer_responded_rx,
        conn,
        log_heartbeats,
        beats_rx,
        hb_event_tx.clone(),
    ));
    tokio::task::spawn_local(reader_task(
        recv,
        connection,
        loop_tx,
        peer_responded_tx,
        shm,
        hb_event_tx,
    ));
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

/// Write each queued command as a frame, plus the heartbeat beats the connection's
/// `heartbeat_task` sends over `beats` (the writer no longer drives its own tick —
/// liveness policy lives in the heartbeat_task). Also snoops its own outgoing
/// `Establish` and forwards the local ident to the heartbeat_task. On teardown,
/// drains queued frames first, then finishes the stream.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct per-connection channel/handle; bundling adds indirection without clarifying anything"
)]
async fn writer_task(
    mut send: SendStream,
    mut rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    mut shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
    // Signalled by this connection's reader once the peer responds to our shutdown
    // (its own Severed frame, or the connection ending) — our cue that the peer
    // received the shutdown notice and we can close.
    mut peer_responded: mpsc::UnboundedReceiver<()>,
    // Held only to keep the QUIC connection alive for the writer's lifetime;
    // dropping it closes the connection, which the peer's reader observes.
    _conn: Connection,
    // When set (acceptor/serve side only, so the joiner root's tens of thousands of
    // writers don't flood), and MM_QUIC_DEBUG is on, log each heartbeat sent — so a
    // connection the peer later reaps can be proven to have kept sending.
    log_heartbeats: bool,
    // Beats this connection's heartbeat_task wants sent.
    mut beats: mpsc::UnboundedReceiver<Heartbeat>,
    // To forward our own outgoing Establish's ident to the heartbeat_task.
    hb_events: mpsc::UnboundedSender<HeartbeatEvent>,
) {
    let log_heartbeats = log_heartbeats && crate::ctx::connection_debug();
    let mut heartbeats_sent: u64 = 0;
    let mut graceful = false;
    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break; // transport dropped
                };
                // Snoop our own Establish so the heartbeat_task learns our ident (its
                // side-channel address / delegate-eligibility key) without any new
                // threading through ctx.
                if let ConnectionCommand::Establish { ident: Some(ident), .. } = &command {
                    let _ = hb_events.send(HeartbeatEvent::EstablishLocal {
                        local_ident: ident.clone(),
                    });
                }
                if framing::write_command(&mut send, command).await.is_err() {
                    return;
                }
            }
            beat = beats.recv() => {
                let Some(heartbeat) = beat else {
                    break; // heartbeat_task gone
                };
                if framing::write_heartbeat(&mut send, heartbeat).await.is_err() {
                    return;
                }
                // Flushed to the QUIC stream; log it from the *sender* so loss vs. a
                // stalled sender can be told apart at the far (receiving) end.
                if log_heartbeats {
                    heartbeats_sent += 1;
                    eprintln!(
                        "{} MM_HB pid={} sent={} on {:?}",
                        crate::ctx::wall_clock_hms(),
                        std::process::id(),
                        heartbeats_sent,
                        _conn.remote_address(),
                    );
                }
            }
            _ = shutdown.changed() => {
                // Graceful teardown. Flush whatever is already queued, then send an
                // explicit shutdown notice. A Severed frame is stream data, which
                // QUIC retransmits until delivered (unlike CONNECTION_CLOSE), so the
                // peer learns of teardown directly rather than by inferring a dropped
                // socket.
                while let Ok(command) = rx.try_recv() {
                    if framing::write_command(&mut send, command).await.is_err() {
                        return;
                    }
                }
                let _ = framing::write_command(
                    &mut send,
                    ConnectionCommand::Severed {
                        reason: b"context shutdown".to_vec(),
                    },
                )
                .await;
                graceful = true;
                break;
            }
        }
    }
    // Finish the stream so the peer's reader sees EOF promptly (the fast path).
    let _ = send.finish();
    // On graceful shutdown, hold the connection (`_conn`) open until the peer
    // responds to our shutdown notice — keeping it open lets QUIC retransmit the
    // notice if the first packet was lost, and we close as soon as the peer's reply
    // arrives rather than after a fixed delay. Bounded so a peer that never replies
    // (e.g. already dead) can't hang teardown.
    if graceful {
        let _ = tokio::time::timeout(shutdown_ack_timeout(), peer_responded.recv()).await;
    }
}

/// Decode each frame off the stream and forward it to the command loop. The
/// heartbeat *timeout* has moved to this connection's `heartbeat_task`; the reader
/// is dumb plumbing now. Heartbeat frames are forwarded to the heartbeat_task as
/// [`HeartbeatEvent`]s (never to ctx). An error/EOF is a hard transport close: it
/// emits `Severed` to the loop and `ReaderClosed` to the heartbeat_task.
///
/// A message flood must not flood the heartbeat_task, so ordinary data frames
/// notify it only as a *throttled backstop*: the reader tracks `last_notify` and
/// sends an empty beat (matching the peer's direction) only when a full
/// `2 × HEARTBEAT_INTERVAL` has passed with no real beat — so real beats cost one
/// event each while data frames cost nothing, yet a peer that stops beating but
/// keeps sending data still proves the pipe live at ≤ one event per two intervals.
async fn reader_task(
    mut recv: RecvStream,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    // Signals this connection's writer that the peer has responded to our shutdown
    // (it sent its own Severed, or the connection ended) so the writer can close.
    peer_responded: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
    // Forwards inbound heartbeats and liveness/close signals to the heartbeat_task.
    hb_events: mpsc::UnboundedSender<HeartbeatEvent>,
) {
    // The peer's heartbeat direction: the peer of a Parent-side connection is a
    // Child (sends FromChild) and vice versa. Used for the empty backstop beat.
    let peer_role = connection.role();
    let backstop_gap = crate::quic_heartbeat::heartbeat_interval() * 2;
    // Per-connection diagnostics, folded into the failure reason under MM_QUIC_DEBUG:
    // did we ever read the peer's Establish (identity exchange)? how many heartbeats
    // and other frames arrived, and how long since the last read? This pinpoints
    // exactly how far a reaped connection got. Tracked cheaply either way; only
    // formatted into the reason when debug is on.
    let debug = crate::ctx::connection_debug();
    let start = std::time::Instant::now();
    let mut established = false;
    let mut heartbeats: u64 = 0;
    let mut commands: u64 = 0;
    let mut last_read: Option<std::time::Duration> = None;
    // When the heartbeat_task was last notified (by a real beat or a backstop),
    // throttling the data-frame backstop. Seeded to now so a fresh pipe waits a
    // full gap before its first backstop.
    let mut last_notify = std::time::Instant::now();
    let stats = |established: bool, heartbeats, commands, last_read: Option<_>| {
        let last = match last_read {
            Some(d) => format!(
                "{:.1}s ago",
                start.elapsed().saturating_sub(d).as_secs_f64()
            ),
            None => "never".to_owned(),
        };
        format!(
            "established={established}, heartbeats={heartbeats}, commands={commands}, \
             age={:.1}s, last_read={last}",
            start.elapsed().as_secs_f64(),
        )
    };
    loop {
        // The owning actor's gateway client is snapshot per frame so a large part
        // is read straight into the slab once the client is known (a gateway seeds
        // its own at creation, before any frame arrives).
        let read = framing::read_frame(&mut recv, &shm.mapper, shm.client());
        match read.await {
            Ok(Incoming::Command(action)) => {
                last_read = Some(start.elapsed());
                commands += 1;
                // The peer's Establish is how we learn its identity: reading it is
                // the moment this side considers the connection established. Forward
                // the peer ident to the heartbeat_task (one-shot, at setup).
                if let ConnectionCommand::Establish { ident, .. } = &action {
                    established = true;
                    if let Some(ident) = ident {
                        let _ = hb_events.send(HeartbeatEvent::EstablishPeer {
                            peer_ident: ident.clone(),
                        });
                    }
                }
                // A Severed from the peer is its response to our shutdown (or the
                // peer initiating its own) — wake any writer waiting to close.
                if matches!(action, ConnectionCommand::Severed { .. }) {
                    let _ = peer_responded.send(());
                }
                // Throttled liveness backstop: an ordinary frame proves the pipe is
                // live even if the peer's beats stopped. An empty beat refreshes the
                // heartbeat_task's Direct deadline and is a no-op for delegation.
                if last_notify.elapsed() >= backstop_gap {
                    last_notify = std::time::Instant::now();
                    let _ = hb_events.send(HeartbeatEvent::ReceivedHeartbeat(
                        Heartbeat::empty_for_peer(peer_role),
                    ));
                }
                if loop_tx
                    .send(Command::ConnectionAction { connection, action })
                    .is_err()
                {
                    return;
                }
            }
            Ok(Incoming::Heartbeat(heartbeat)) => {
                last_read = Some(start.elapsed());
                heartbeats += 1;
                // A real beat: forward it (with its coverage diff / delegate
                // instruction) and count it as a notification for the backstop.
                last_notify = std::time::Instant::now();
                let _ = hb_events.send(HeartbeatEvent::ReceivedHeartbeat(heartbeat));
            }
            Err(err) => {
                let _ = peer_responded.send(());
                let _ = hb_events.send(HeartbeatEvent::ReaderClosed);
                let reason = if debug {
                    // Walk the io::Error source chain to recover the concrete quinn
                    // ConnectionError — ReadError::ConnectionLost's own Display is just
                    // the bare "connection lost", dropping the real cause ("timed out"
                    // vs "reset by peer" vs "closed by peer").
                    let mut detail = err.to_string();
                    let mut src = std::error::Error::source(&err);
                    while let Some(e) = src {
                        detail.push_str(": ");
                        detail.push_str(&e.to_string());
                        src = e.source();
                    }
                    format!(
                        "quic connection closed: {detail} ({})",
                        stats(established, heartbeats, commands, last_read)
                    )
                    .into_bytes()
                } else {
                    b"quic connection closed".to_vec()
                };
                sever(&loop_tx, connection, reason);
                return;
            }
        }
    }
}
