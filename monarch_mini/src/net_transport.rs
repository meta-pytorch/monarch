/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Protocol-independent connection transport, generic over a [`Net`].
//!
//! A connection's coroutines bring up the connection's streams, produce a
//! [`ConnectionTransport`] for the data stream, and hand it to the command loop via
//! `Command::TransportConnected`; the data reader forwards every decoded frame back
//! as a `ConnectionAction`. Establishment policy (identity exchange, hello, liveness
//! reporting) lives in the command loop and is identical across transports. Wire
//! framing is shared (see [`crate::framing`]).
//!
//! ## Two streams per connection
//!
//! Every connection carries [`STREAMS`] ordered stream pairs. A single reliable,
//! in-order stream head-of-line-blocks everything queued behind a large message —
//! including heartbeats, whose whole job is to keep flowing while data does. So we
//! put heartbeats on their **own** stream, separate from the data/control stream;
//! the heartbeat stream is requested second so it carries a higher send priority, and
//! a beat is packed ahead of queued data even under a full congestion window.
//!
//! The heartbeat stream is materialized lazily (see [`crate::net`]): a message-only
//! gateway side channel never opens it. A parent/child link always heartbeats, so it
//! opens the stream off the establishment path — the first beat never delays either
//! establishment or serve-pairing.
//!
//! ## Liveness
//!
//! A protocol like quic runs in userspace over an unreliable datagram, so there is
//! no file-descriptor close to signal a lost peer. Instead an application-level
//! **bidirectional heartbeat** runs on the heartbeat stream (see
//! [`crate::heartbeat`]); a clean drop still finishes both streams so the peer
//! also observes EOF — the heartbeat is the backstop for the unclean cases.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;

use crate::Role;
use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::connection::SideChannelMessage;
use crate::connection::sever;
use crate::ctx::Command;
use crate::framing;
use crate::framing::Preamble;
use crate::framing::SideChannelHeartbeat;
use crate::heartbeat::BeatKind;
use crate::heartbeat::ConnectionId;
use crate::heartbeat::Heartbeat;
use crate::heartbeat::HeartbeatEvent;
use crate::heartbeat::Heartbeats;
use crate::matcher::Matcher;
use crate::net::Net;
use crate::net::NetConn;
use crate::shm::MapperHandle;
use crate::shm::ShmClient;
use crate::shm::ShmClientSlot;
use crate::transport::Transport;

/// The send half of a connection's stream, for the protocol `N`.
type ConnSend<N> = <<N as Net>::Conn as NetConn>::Send;
/// The receive half of a connection's stream, for the protocol `N`.
type ConnRecv<N> = <<N as Net>::Conn as NetConn>::Recv;

/// A side-channel writer's live connection: the connection handle (held to keep it
/// alive and to open the heartbeat stream lazily), the message send stream, and the
/// heartbeat send stream once a beat has been sent (`None` until then).
type LiveSideChannel<N> = (<N as Net>::Conn, ConnSend<N>, Option<ConnSend<N>>);

/// Streams every connection carries, passed to [`Net::bind`]/[`Net::connect`]. Each
/// connection requests them in order via [`NetConn::stream`]: first the data/control
/// stream (the preamble and every [`ConnectionCommand`] frame), then the heartbeat
/// stream (bare heartbeat probes, materialized lazily and — being requested second —
/// given a higher send priority than the data stream).
const STREAMS: usize = 2;

/// Connect-retry backoff bounds. A join may precede its serve, and the handshake
/// fails until the server is bound, so the connector polls — fast at first, backing
/// off to a steady poll.
const CONNECT_RETRY_MIN: Duration = Duration::from_millis(5);
const CONNECT_RETRY_MAX: Duration = Duration::from_millis(1000);

/// On graceful shutdown each writer sends an explicit `Severed{"context shutdown"}`
/// frame (reliable stream data — retransmitted by the transport, unlike an abrupt
/// connection close), then keeps the connection open until the peer *responds* (its
/// own `Severed`/EOF, observed by our reader) before closing — so we close as soon
/// as we know the peer got it, rather than after a fixed delay. This is the upper
/// bound on that wait, for a peer that never responds (e.g. already dead). Tunable
/// via `MM_QUIC_SHUTDOWN_ACK_TIMEOUT_MS`.
const DEFAULT_SHUTDOWN_ACK_TIMEOUT: Duration = Duration::from_secs(10);

fn shutdown_ack_timeout() -> Duration {
    std::env::var("MM_QUIC_SHUTDOWN_ACK_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SHUTDOWN_ACK_TIMEOUT)
}

/// Optional cap on how many client connect *attempts* may be in flight at once
/// across the whole context. A root that joins tens of thousands of peers spawns
/// that many connector tasks, each driving a handshake; run all at once on the
/// single-threaded runtime they compete for CPU and reorder connection setup badly.
/// Bounds the simultaneous attempts (each connector still retries independently, and
/// the permit is released while it backs off).
///
/// `MM_QUIC_MAX_CONCURRENT_CONNECTS` overrides it: a positive value caps to that;
/// `0` means unlimited. When the env var is unset (or unparseable) the protocol's
/// [`Net::default_connect_concurrency`] applies — 1024 for quic, a much smaller cap for
/// tcp, which otherwise starves stragglers under a large connect storm.
fn max_concurrent_connects<N: Net>() -> Option<usize> {
    match std::env::var("MM_QUIC_MAX_CONCURRENT_CONNECTS") {
        Ok(v) => match v.parse::<usize>() {
            Ok(0) => None,                              // explicit opt-out ⇒ unlimited
            Ok(n) => Some(n),                           // explicit cap
            Err(_) => N::default_connect_concurrency(), // unparseable ⇒ protocol default
        },
        Err(_) => N::default_connect_concurrency(), // unset ⇒ protocol default
    }
}

/// Build the optional multi-threaded runtime that the per-connection data
/// coroutines (the message writer/reader) run on, off the single-threaded command
/// loop. `MM_NET_DATA_THREADS` selects it: unset or `0` keeps every coroutine on the
/// command-loop thread (today's behaviour, no data parallelism); a positive `N`
/// stands up an `N`-worker runtime so the data-stream framing/crypto/copy for many
/// connections runs across cores while accounting stays serialized on one core.
///
/// Only the two *data* coroutines move; the connection handle, heartbeat stream,
/// side channels, and pairing all stay on the command-loop thread. The data stream
/// halves were materialized on the command-loop runtime's I/O driver, so readiness is
/// still driven there — this offloads the per-frame CPU work (notably kTLS-over-TCP's
/// in-task syscalls/copy), not the epoll wakeups. Fully decoupling the reactor would
/// require materializing the streams on this runtime, a later step.
fn data_runtime() -> Option<tokio::runtime::Runtime> {
    let n = std::env::var("MM_NET_DATA_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if n == 0 {
        return None;
    }
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(n)
        .thread_name("mm-net-data")
        .enable_all()
        .build()
    {
        Ok(rt) => {
            eprintln!("MM_NET_DATA_THREADS: net data coroutines on a {n}-worker runtime");
            Some(rt)
        }
        Err(err) => {
            tracing::warn!("MM_NET_DATA_THREADS set but runtime build failed: {err:#}");
            None
        }
    }
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

/// What a connection's reader needs for shared memory: the context-global mapper and
/// the owning actor's gateway-client slot. Only the *reader* uses it — to read a
/// large incoming part straight into a slab block (every actor on a cross-machine
/// link is a gateway, so it has a client). The writer needs nothing: a `Shm` part
/// carries its own mapper.
#[derive(Clone)]
struct ShmCtx {
    mapper: MapperHandle,
    client: ShmClientSlot,
}

impl ShmCtx {
    /// Snapshot the owning actor's gateway client (`None` until it is learned; a
    /// gateway seeds its own at creation, before any frame can arrive).
    fn client(&self) -> Option<ShmClient> {
        *self.client.lock().expect("shm client slot mutex poisoned")
    }
}

/// What a gateway side-channel writer carries: either a routable
/// [`SideChannelMessage`] (from ctx) or a transport-internal delegated-heartbeat
/// beat/ack (from the heartbeat coroutines). Both ride the same per-gateway writer,
/// so a beat reuses the exact connection an ordinary message would.
enum SideChannelOut {
    Message(SideChannelMessage),
    Heartbeat {
        recipient: Vec<u8>,
        from: Vec<u8>,
        conn_id: ConnectionId,
        kind: BeatKind,
    },
}

/// The client-side dialing + gateway side-channel state, shared (`Rc<RefCell>`)
/// between the command-loop-facing [`NetTransport`] and the heartbeat coroutines.
/// The transport owns every side-channel path end to end — opening, reusing, and
/// writing it — so a delegated-heartbeat beat is sent by the heartbeat coroutine
/// directly through here, never routed through ctx. Single-threaded (LocalSet),
/// hence `Rc<RefCell>`. The protocol state ([`Net`]) is built lazily on first use.
struct SideChannels<N: Net> {
    shutdown_tx: watch::Sender<bool>,
    /// The shared per-context networking state, built lazily on the first serve/join.
    net: Option<N>,
    /// One side-channel writer per remote gateway, keyed by its dial address (the
    /// `@specifier` tag). Each owns a task that lazily connects (retrying until the
    /// gateway is live), streams frames, and reconnects if the connection drops.
    /// Cached across messages/beats and shared by ordinary messages and heartbeats.
    channels: HashMap<String, mpsc::UnboundedSender<SideChannelOut>>,
    /// Liveness-token issuer: each connection's/side-channel's writer holds a clone
    /// for its lifetime. Teardown drops this issuing copy so `alive_rx` closes once
    /// every writer has flushed and exited.
    alive_tx: Option<mpsc::UnboundedSender<()>>,
    /// The data runtime handle, handed to [`Net::create`] so a protocol whose work is
    /// in a background driver (quic) builds its endpoints there. `None` ⇒ single core.
    data_rt: Option<Handle>,
}

/// Owns all transport state and coroutines. The command loop holds one and forwards
/// serves/joins to it; it never sees streams or pairing state.
pub(crate) struct NetTransport<N: Net> {
    loop_tx: mpsc::UnboundedSender<Command>,
    // The context-global address-space mapper, captured once at construction and
    // handed to every connection's reader so a large incoming part can be read
    // straight into a slab block.
    mapper: MapperHandle,
    // The context's single shm client slot, used *only* by side-channel readers,
    // which are not tied to any actor. A join/serve connection reads into its owning
    // actor's own slot instead (passed through serve/join).
    context_shm: ShmClientSlot,
    // One listener coroutine per url; serve connections are forwarded to it and it
    // owns the serve/accept pairing.
    listeners: HashMap<String, mpsc::UnboundedSender<(ConnectionRef, ShmCtx)>>,
    // Bounds simultaneous client connect attempts (see `max_concurrent_connects`).
    // Shared by every connector task; `None` ⇒ unlimited.
    connect_sem: Option<Arc<Semaphore>>,
    // Closed once every writer has exited (see `SideChannels::alive_tx`).
    alive_rx: mpsc::UnboundedReceiver<()>,
    // Delegated-heartbeat state, shared by every connection's heartbeat coroutine.
    heartbeat: Heartbeats,
    // Client dialing + side-channel writers, shared with the heartbeat coroutines so
    // the transport drives every side-channel path without ctx involvement.
    side_channels: Rc<RefCell<SideChannels<N>>>,
    // Optional multi-threaded runtime the per-connection data coroutines run on (see
    // `data_runtime`). `None` ⇒ they run on the command-loop thread like everything
    // else. Owned here for its lifetime; torn down (non-blocking) in `shutdown`.
    data_rt: Option<tokio::runtime::Runtime>,
}

impl<N: Net> NetTransport<N> {
    pub(crate) fn new(
        loop_tx: mpsc::UnboundedSender<Command>,
        mapper: MapperHandle,
        context_shm: ShmClientSlot,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let (alive_tx, alive_rx) = mpsc::unbounded_channel();
        if let Some(n) = max_concurrent_connects::<N>() {
            eprintln!("MM_QUIC connect concurrency capped at {n} simultaneous attempts");
        }
        // The data runtime is owned here for its lifetime; its handle is shared with
        // the side channels (→ `Net::create`, for quic's endpoint drivers) and with
        // `spawn_connection` via `data_handle` (for the tcp data coroutines).
        let data_rt = data_runtime();
        let data_rt_handle = data_rt.as_ref().map(|rt| rt.handle().clone());
        Self {
            loop_tx,
            mapper,
            context_shm,
            listeners: HashMap::new(),
            connect_sem: max_concurrent_connects::<N>().map(|n| Arc::new(Semaphore::new(n))),
            alive_rx,
            heartbeat: Heartbeats::new(),
            side_channels: Rc::new(RefCell::new(SideChannels {
                shutdown_tx,
                net: None,
                channels: HashMap::new(),
                alive_tx: Some(alive_tx),
                data_rt: data_rt_handle,
            })),
            data_rt,
        }
    }

    /// A handle to the data-coroutine runtime, or `None` to run them on the
    /// command-loop thread. Cloned per connection and passed to [`spawn_connection`].
    fn data_handle(&self) -> Option<Handle> {
        self.data_rt.as_ref().map(|rt| rt.handle().clone())
    }

    /// A connection's shared-memory context: the context mapper paired with the given
    /// client slot. A join/serve connection passes its owning actor's slot; a
    /// side-channel passes [`Self::context_shm`].
    fn shm_ctx(&self, client: ShmClientSlot) -> ShmCtx {
        ShmCtx {
            mapper: self.mapper.clone(),
            client,
        }
    }

    /// Send a self-addressing [`SideChannelMessage`] to a remote gateway over a
    /// direct side-channel. The caller (the command loop) has already derived `tag`
    /// from the message's `gateway_for_actor`.
    pub(crate) fn send_to_gateway(&mut self, tag: String, message: SideChannelMessage) {
        self.side_channels.borrow_mut().send_message(tag, message);
    }

    /// Signal every writer to flush and exit, then wait until they all have.
    ///
    /// Each writer, on this signal, sends an explicit `Severed{"context shutdown"}`
    /// frame and holds its connection open until the peer *responds* so the frame is
    /// reliably delivered before the connection is dropped. So by the time every
    /// writer has exited (`alive_rx` closed) the peers have been told — we do not
    /// rely on a lossy connection-close being noticed.
    pub(crate) async fn shutdown(&mut self) {
        self.side_channels.borrow_mut().begin_shutdown();
        let _ = self.alive_rx.recv().await;
        // Tear the data runtime down without blocking: we are inside the command
        // loop's async context, where dropping a `Runtime` (which blocks on its worker
        // threads) would panic. The writers above have already flushed and the peers
        // acknowledged, so its tasks have nothing left to do.
        if let Some(rt) = self.data_rt.take() {
            rt.shutdown_background();
        }
    }
}

impl<N: Net> SideChannels<N> {
    /// The shared networking state, built on first use.
    fn net(&mut self) -> anyhow::Result<&mut N> {
        if self.net.is_none() {
            self.net = Some(N::create(self.data_rt.clone())?);
        }
        Ok(self.net.as_mut().expect("net just created"))
    }

    /// A dialing handle for `addr`, from the (lazily built) client pool.
    fn dialer(&mut self, addr: SocketAddr) -> anyhow::Result<N::Dialer> {
        self.net()?.dialer(addr)
    }

    /// Bind a server listener on `addr` carrying `STREAMS` streams.
    fn bind(&mut self, addr: SocketAddr) -> anyhow::Result<N::Listener> {
        self.net()?.bind(addr, STREAMS)
    }

    fn alive_token(&self) -> mpsc::UnboundedSender<()> {
        self.alive_tx
            .as_ref()
            .expect("alive-token issuer present before shutdown")
            .clone()
    }

    fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// The writer for the gateway at `tag`, opening (and caching) it on first use.
    /// `None` if the tag is unparseable or the dialer can't be built.
    fn writer_for(&mut self, tag: String) -> Option<&mpsc::UnboundedSender<SideChannelOut>> {
        if !self.channels.contains_key(&tag) {
            let addr = match N::parse_addr(&tag) {
                Ok(addr) => addr,
                Err(err) => {
                    tracing::warn!("side channel address: {err:#}");
                    return None;
                }
            };
            let dialer = match self.dialer(addr) {
                Ok(dialer) => dialer,
                Err(err) => {
                    tracing::warn!("side channel dialer: {err:#}");
                    return None;
                }
            };
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::task::spawn_local(side_channel_writer_task::<N>(
                dialer,
                addr,
                rx,
                self.shutdown_tx.subscribe(),
                self.alive_token(),
            ));
            self.channels.insert(tag.clone(), tx);
        }
        self.channels.get(&tag)
    }

    /// Send a self-addressing [`SideChannelMessage`] to the gateway at `tag`, opening
    /// (and caching) the connection on first use. Best-effort and not heartbeated; a
    /// message enqueued while the remote gateway is not yet live waits for the
    /// connection to come up.
    fn send_message(&mut self, tag: String, message: SideChannelMessage) {
        if let Some(tx) = self.writer_for(tag) {
            let _ = tx.send(SideChannelOut::Message(message));
        }
    }

    /// Send a delegated-heartbeat beat/ack to `recipient`, dialing the side channel
    /// at `recipient`'s own gateway `@tag` and reusing the same per-gateway side
    /// channel an ordinary message would. Dropped if `recipient` has no gateway tag
    /// to dial. Called by the heartbeat coroutines directly — never via ctx.
    fn send_heartbeat(
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

impl<N: Net> Transport for NetTransport<N> {
    fn serve(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot) {
        // The first serve on a url spawns its listener coroutine; later serves just
        // forward another connection to it. The listener carries the *context* shm
        // context for side-channel readers (which have no owning actor); each Join
        // connection instead uses the owning actor's slot, forwarded alongside it.
        if !self.listeners.contains_key(&url) {
            let addr = match N::parse_addr(&url) {
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
            let data_rt = self.data_handle();
            // Bind synchronously so a port-in-use failure is known now; a failed bind
            // spawns a task that severs every serve on this url until teardown rather
            // than respawn-retrying.
            match self.side_channels.borrow_mut().bind(addr) {
                Ok(listener) => {
                    tokio::task::spawn_local(listener_task::<N>(
                        listener,
                        rx,
                        self.loop_tx.clone(),
                        shutdown,
                        alive,
                        context_shm,
                        self.heartbeat.clone(),
                        self.side_channels.clone(),
                        data_rt,
                    ));
                }
                Err(err) => {
                    let reason = format!("quic bind failed: {err:#}").into_bytes();
                    tokio::task::spawn_local(dead_listener_task(
                        rx,
                        self.loop_tx.clone(),
                        shutdown,
                        reason,
                    ));
                }
            }
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
        let addr = match N::parse_addr(&url) {
            Ok(addr) => addr,
            Err(err) => {
                sever(&self.loop_tx, connection, format!("{err:#}").into_bytes());
                return;
            }
        };
        let dialer = match self.side_channels.borrow_mut().dialer(addr) {
            Ok(dialer) => dialer,
            Err(err) => {
                sever(
                    &self.loop_tx,
                    connection,
                    format!("quic client dialer: {err:#}").into_bytes(),
                );
                return;
            }
        };
        let shutdown = self.side_channels.borrow().subscribe_shutdown();
        let alive = self.side_channels.borrow().alive_token();
        let data_rt = self.data_handle();
        tokio::task::spawn_local(connector_task::<N>(
            dialer,
            addr,
            connection,
            self.loop_tx.clone(),
            shutdown,
            alive,
            self.shm_ctx(shm_client),
            self.connect_sem.clone(),
            self.heartbeat.clone(),
            self.side_channels.clone(),
            data_rt,
        ));
    }
}

/// A connection accepted by the listener, classified by the joiner's data-stream
/// preamble. A `Join` is paired with a serve and driven by the command loop; a
/// `SideChannel` is a gateway-to-gateway link read directly into the home gateway's
/// routing (the joiner only sends, so we keep just the message recv).
enum Accepted<N: Net> {
    Join(JoinStreams<N>),
    SideChannel {
        conn: N::Conn,
        msg_recv: ConnRecv<N>,
    },
}

/// The connection plus the data-stream halves of a join link, as paired by the
/// listener's [`Matcher`] and handed to [`spawn_connection`]. The heartbeat stream is
/// not here — it is acquired later off the establishment path.
struct JoinStreams<N: Net> {
    conn: N::Conn,
    data_send: ConnSend<N>,
    data_recv: ConnRecv<N>,
}

/// Fail every serve forwarded to a url whose bind failed, until teardown. The url is
/// dead for serving; we stay alive and sever each serve rather than respawn-retrying.
async fn dead_listener_task(
    mut serves: mpsc::UnboundedReceiver<(ConnectionRef, ShmCtx)>,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    reason: Vec<u8>,
) {
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

/// Dispatch each accepted connection by its preamble: a `Join` is paired with the
/// next queued serve (either may arrive first); a `SideChannel` is read straight into
/// the context's gateway routing. Stops on teardown or when the command loop drops
/// the serve sender. `side_channel_shm` is the *context* shm context, used only for
/// side-channel readers (which have no owning actor); each Join connection instead
/// uses the per-serve shm forwarded with it.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of listener state; bundling adds indirection without clarifying anything"
)]
async fn listener_task<N: Net>(
    listener: N::Listener,
    mut serves: mpsc::UnboundedReceiver<(ConnectionRef, ShmCtx)>,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    side_channel_shm: ShmCtx,
    heartbeat: Heartbeats,
    side_channels: Rc<RefCell<SideChannels<N>>>,
    data_rt: Option<Handle>,
) {
    // Accepting a connection, its data stream, and its preamble is a multi-step
    // handshake; run it off the pairing loop so a slow handshake doesn't stall
    // matching. Each classified connection lands on `accepted_rx`.
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel();
    tokio::task::spawn_local(acceptor_task::<N>(listener, accepted_tx, shutdown.clone()));

    let mut matcher: Matcher<(ConnectionRef, ShmCtx), JoinStreams<N>> = Matcher::new();
    loop {
        tokio::select! {
            serve = serves.recv() => {
                let Some((connection, shm)) = serve else { return; };
                if let Some((left, right)) = matcher.push_left((connection, shm), |l, r| (l, r)) {
                    spawn_join(left, right, &loop_tx, &shutdown, &alive, &heartbeat, &side_channels, &data_rt);
                }
            }
            accepted = accepted_rx.recv() => {
                match accepted {
                    None => return,
                    Some(Accepted::Join(streams)) => {
                        if let Some((left, right)) = matcher.push_right(streams, |l, r| (l, r)) {
                            spawn_join(left, right, &loop_tx, &shutdown, &alive, &heartbeat, &side_channels, &data_rt);
                        }
                    }
                    Some(Accepted::SideChannel { conn, msg_recv }) => {
                        // Messages and delegated beats arrive on separate streams, so
                        // read each in its own task. The message recv keeps the
                        // connection alive; the heartbeat reader materializes its own
                        // stream, which may open late (on the first beat) or never.
                        tokio::task::spawn_local(side_channel_reader_task::<N>(
                            msg_recv, loop_tx.clone(), side_channel_shm.clone(),
                        ));
                        tokio::task::spawn_local(side_channel_heartbeat_reader_task::<N>(
                            conn, heartbeat.clone(),
                        ));
                    }
                }
            }
            _ = shutdown.changed() => return,
        }
    }
}

/// Pair a queued serve with an accepted join and wire up the connection. Both the
/// serve-first (`push_left`) and accept-first (`push_right`) paths land here, so a
/// pairing spawns identically: this is the serve/acceptor side, so `log_heartbeats =
/// true` and `dialed = false` (the peer dialed us). The data-stream halves were
/// already split (with `first_messenger = false`) and the preamble read by the
/// acceptor.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of per-connection state forwarded to spawn_connection; bundling adds indirection without clarifying anything"
)]
fn spawn_join<N: Net>(
    serve: (ConnectionRef, ShmCtx),
    streams: JoinStreams<N>,
    loop_tx: &mpsc::UnboundedSender<Command>,
    shutdown: &watch::Receiver<bool>,
    alive: &mpsc::UnboundedSender<()>,
    heartbeat: &Heartbeats,
    side_channels: &Rc<RefCell<SideChannels<N>>>,
    data_rt: &Option<Handle>,
) {
    let (connection, shm) = serve;
    let JoinStreams {
        conn,
        data_send,
        data_recv,
    } = streams;
    spawn_connection::<N>(
        data_send,
        data_recv,
        connection,
        loop_tx.clone(),
        shutdown.clone(),
        alive.clone(),
        conn,
        shm,
        true,
        false,
        heartbeat.clone(),
        side_channels.clone(),
        data_rt.clone(),
    );
}

/// Accept connections on `listener`, split each one's data stream (this side is the
/// responder, so `first_messenger = false`), read its preamble, and forward the
/// classified result. Each handshake runs in its own task so one slow peer doesn't
/// block others.
async fn acceptor_task<N: Net>(
    listener: N::Listener,
    accepted_tx: mpsc::UnboundedSender<Accepted<N>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            conn = N::accept(&listener) => {
                let Some(conn) = conn else { return; }; // listener closed
                let accepted_tx = accepted_tx.clone();
                tokio::task::spawn_local(async move {
                    // The data stream (index 0) carries the preamble; accept it and
                    // classify. The heartbeat stream (index 1) is materialized later
                    // (by the heartbeat task, or the side-channel heartbeat reader),
                    // so establishment/pairing never waits on the first beat.
                    let Ok((data_send, mut data_recv)) =
                        conn.stream(false).await else { return; };
                    let accepted = match framing::read_preamble(&mut data_recv).await {
                        Ok(Preamble::Join) => Accepted::Join(JoinStreams {
                            conn,
                            data_send,
                            data_recv,
                        }),
                        // A side-channel is unidirectional (the joiner only sends), so
                        // keep just the message recv and drop the send half.
                        Ok(Preamble::SideChannel) => Accepted::SideChannel {
                            conn,
                            msg_recv: data_recv,
                        },
                        // A decode error: the peer spoke a bad dialect — drop it.
                        Err(_) => return,
                    };
                    let _ = accepted_tx.send(accepted);
                });
            }
            _ = shutdown.changed() => return,
        }
    }
}

/// Read routable messages off a gateway side-channel's message stream and forward
/// each to the command loop (which resolves the owning gateway). There is no
/// establishment: an EOF or error just ends the reader (the sending gateway
/// reconnects when it next has something to send). `recv` keeps the connection alive.
/// Delegated beats travel on the companion heartbeat stream (see
/// [`side_channel_heartbeat_reader_task`]).
async fn side_channel_reader_task<N: Net>(
    mut recv: ConnRecv<N>,
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

/// Read delegated-heartbeat beats/acks/releases off a gateway side-channel's
/// heartbeat stream and route them *directly* into the heartbeat subsystem — they
/// never reach ctx. Kept on its own stream so a large routed message can never delay
/// a beat. The heartbeat stream is materialized here (this side is the responder,
/// `first_messenger = false`): it may open late (on the peer's first beat) or never
/// (a channel that only carries messages), and awaiting it here never blocks the
/// message reader.
async fn side_channel_heartbeat_reader_task<N: Net>(conn: N::Conn, heartbeat: Heartbeats) {
    let Ok((_send, mut recv)) = conn.stream(false).await else {
        return; // connection closed before any heartbeat stream opened
    };
    loop {
        match framing::read_side_channel_heartbeat(&mut recv).await {
            Ok(SideChannelHeartbeat {
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

/// Connect to `addr`, retrying until the server binds and the handshake succeeds (so
/// a join may precede its serve), then split the data stream, announce ourselves as
/// the joiner via the preamble, and wire it up. This side is the *first messenger* on
/// the data stream (`first_messenger = true`).
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of connector state; bundling adds indirection without clarifying anything"
)]
async fn connector_task<N: Net>(
    dialer: N::Dialer,
    addr: SocketAddr,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
    // Bounds how many connect attempts run at once across the context (see
    // `max_concurrent_connects`). `None` ⇒ unlimited. A permit is held only for the
    // duration of one attempt and released before backing off, so a waiting task can
    // attempt while this one sleeps.
    connect_sem: Option<Arc<Semaphore>>,
    heartbeat: Heartbeats,
    side_channels: Rc<RefCell<SideChannels<N>>>,
    data_rt: Option<Handle>,
) {
    let mut retry = CONNECT_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return;
        }
        // Take a connect-attempt permit before doing any handshake work, so a root
        // joining tens of thousands of peers drives at most N handshakes at once.
        let permit = match &connect_sem {
            Some(sem) => Some(
                Arc::clone(sem)
                    .acquire_owned()
                    .await
                    .expect("connect semaphore never closed"),
            ),
            None => None,
        };
        if let Some(conn) = N::connect(&dialer, addr, STREAMS).await {
            match conn.stream(true).await {
                Ok((mut data_send, data_recv)) => {
                    // Tell the acceptor this is an ordinary join (not a side-channel)
                    // before the command loop drives establishment over the stream.
                    if framing::write_preamble(&mut data_send, Preamble::Join)
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
                    // `dialed = true`: this is one half of the delegation guard (only
                    // a link the parent dialed may be delegated).
                    spawn_connection::<N>(
                        data_send,
                        data_recv,
                        connection,
                        loop_tx,
                        shutdown,
                        alive,
                        conn,
                        shm,
                        false,
                        true,
                        heartbeat,
                        side_channels,
                        data_rt,
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
/// it drops. The **heartbeat stream is opened only when the first beat is dequeued**,
/// so a message-only channel never establishes it. An item in flight when the
/// connection drops may be lost; the channel is best-effort by design.
async fn side_channel_writer_task<N: Net>(
    dialer: N::Dialer,
    addr: SocketAddr,
    mut rx: mpsc::UnboundedReceiver<SideChannelOut>,
    mut shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
) {
    // The current connection, if up: the connection handle (held to keep it alive and
    // to open the heartbeat stream lazily), the message send stream, and the
    // heartbeat send stream once a beat has been sent. `None` means we must
    // (re)connect before the next write.
    let mut live: Option<LiveSideChannel<N>> = None;
    loop {
        let item = tokio::select! {
            item = rx.recv() => match item {
                Some(item) => item,
                None => break, // sender dropped (gateway gone)
            },
            _ = shutdown.changed() => break,
        };
        if live.is_none() {
            let Some(conn) = connect_side_channel::<N>(&dialer, addr, &mut shutdown).await else {
                break; // shutting down before the gateway came up
            };
            // The message stream (index 0) carries the preamble (which also opens it
            // on the wire). The heartbeat stream (index 1) carries no preamble and is
            // opened lazily below, so a message-only channel never opens it.
            let Ok((mut msg_send, _msg_recv)) = conn.stream(true).await else {
                continue; // reconnect on the next item
            };
            if framing::write_preamble(&mut msg_send, Preamble::SideChannel)
                .await
                .is_err()
            {
                continue; // reconnect on the next item (this one is dropped)
            }
            live = Some((conn, msg_send, None));
        }
        let (conn, msg_send, hb_send) = live.as_mut().expect("connected");
        let wrote = match item {
            SideChannelOut::Message(message) => {
                framing::write_side_channel(msg_send, message).await
            }
            SideChannelOut::Heartbeat {
                recipient,
                from,
                conn_id,
                kind,
            } => {
                // Open the heartbeat stream on the first beat only. It rides a
                // higher-priority stream (index > the message stream), so a beat jumps
                // ahead of queued message bytes under a full congestion window.
                if hb_send.is_none() {
                    match conn.stream(true).await {
                        Ok((send, _recv)) => *hb_send = Some(send),
                        Err(_) => {
                            live = None; // reconnect on the next item
                            continue;
                        }
                    }
                }
                let hb_send = hb_send.as_mut().expect("heartbeat stream opened");
                framing::write_side_channel_heartbeat(hb_send, recipient, from, conn_id, kind).await
            }
        };
        if wrote.is_err() {
            live = None; // dropped; reconnect on the next item
        }
    }
    if let Some((_conn, mut msg_send, hb_send)) = live {
        let _ = msg_send.shutdown().await;
        if let Some(mut hb_send) = hb_send {
            let _ = hb_send.shutdown().await;
        }
    }
}

/// Connect a side-channel to `addr`, retrying with backoff until the gateway binds
/// and the handshake succeeds, or until teardown (then `None`). Streams are opened
/// lazily by the caller. Mirrors the join connector's retry so a side-channel may be
/// opened before its target gateway is live.
async fn connect_side_channel<N: Net>(
    dialer: &N::Dialer,
    addr: SocketAddr,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<N::Conn> {
    let mut retry = CONNECT_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return None;
        }
        if let Some(conn) = N::connect(dialer, addr, STREAMS).await {
            return Some(conn);
        }
        tokio::select! {
            _ = tokio::time::sleep(retry) => {}
            _ = shutdown.changed() => return None,
        }
        retry = (retry * 2).min(CONNECT_RETRY_MAX);
    }
}

/// Wire up an established connection: build its [`StreamConnectionTransport`] over the
/// data stream, announce it to the command loop, and spawn three tasks — a data
/// writer (commands → frames) and data reader (frames → `ConnectionAction`) on the
/// data stream, plus a heartbeat management task that materializes the heartbeat
/// stream and runs a beat writer + reader on it. Splitting the streams keeps a large
/// message from delaying a beat. Crucially, only the two data-stream halves are
/// passed in: the heartbeat stream is materialized *inside* the management task, so
/// neither establishment nor serve-pairing ever waits on the first heartbeat.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of per-connection state handed off to the writer or reader; bundling them adds indirection without clarifying anything"
)]
fn spawn_connection<N: Net>(
    data_send: ConnSend<N>,
    data_recv: ConnRecv<N>,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    conn: N::Conn,
    shm: ShmCtx,
    // Forwarded to the heartbeat writer: log heartbeat sends (gated to the
    // serve/acceptor side by the call sites, and further gated by MM_QUIC_DEBUG).
    log_heartbeats: bool,
    // Whether *we* dialed this connection (join). One half of the delegation guard
    // (only a link the parent dialed may be delegated).
    dialed: bool,
    heartbeat: Heartbeats,
    side_channels: Rc<RefCell<SideChannels<N>>>,
    // When `Some`, the data writer/reader run on this multi-threaded runtime instead
    // of the command-loop thread (see `data_runtime`). The heartbeat stream, side
    // channels, and pairing always stay on the command-loop thread.
    data_rt: Option<Handle>,
) {
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    // Reader → writer signal: the peer responded to our shutdown, so the writer may
    // close. Unbounded but only ever carries a single `()`.
    let (peer_responded_tx, peer_responded_rx) = mpsc::unbounded_channel();
    // heartbeat coroutine → heartbeat writer: beats to write out.
    let (beats_tx, beats_rx) = mpsc::unbounded_channel();
    let transport = Box::new(StreamConnectionTransport { tx: writer_tx });
    let _ = loop_tx.send(Command::TransportConnected {
        connection,
        transport,
    });
    // The heartbeat coroutine sends its side-channel beats through this closure — it
    // never sees the transport's side-channel types, keeping `heartbeat`
    // decoupled from the transport. The closure reuses the existing gateway side
    // channels.
    let send_beat =
        move |recipient: Vec<u8>, from: Vec<u8>, conn_id: ConnectionId, kind: BeatKind| {
            side_channels
                .borrow_mut()
                .send_heartbeat(recipient, from, conn_id, kind);
        };
    // Spawn the coroutine; the returned sender is how the readers/writer feed it
    // inbound beats, Establish snoops, and reader-closed.
    let hb_event_tx = heartbeat.spawn(connection, dialed, beats_tx, send_beat, loop_tx.clone());
    // Data stream: command writer and frame reader. Built here (moving in their state)
    // then spawned either on the data runtime (parallel across connections) or, by
    // default, on the command-loop thread. Only these two coroutines may move off the
    // command-loop thread — their captured state is `Send` (the `Send`-bounded stream
    // halves, channel handles, `Copy` connection ref, and `Arc`-backed shm); the
    // heartbeat/side-channel machinery below stays local.
    let writer = writer_task::<N>(
        data_send,
        writer_rx,
        shutdown.clone(),
        alive,
        peer_responded_rx,
        hb_event_tx.clone(),
    );
    let reader = reader_task::<N>(
        data_recv,
        connection,
        loop_tx,
        peer_responded_tx,
        shm,
        hb_event_tx.clone(),
    );
    match &data_rt {
        Some(handle) => {
            handle.spawn(writer);
            handle.spawn(reader);
        }
        None => {
            tokio::task::spawn_local(writer);
            tokio::task::spawn_local(reader);
        }
    }
    // Heartbeat stream: materialized off the establishment path, then a beat writer
    // and reader run on it. `first_messenger` is whether this side is the heartbeat
    // **Child** — which always beats first — so the Child opens the stream and the
    // Parent accepts it. This (not `dialed`) is what avoids a deadlock: the Child
    // writes its first beat as soon as it opens the stream, so the Parent's accept
    // resolves.
    let hb_first = connection.role() == Role::Child;
    tokio::task::spawn_local(heartbeat_stream_task::<N>(
        conn,
        hb_first,
        beats_rx,
        hb_event_tx,
        shutdown,
        log_heartbeats,
    ));
}

/// Materialize the connection's heartbeat stream and run the beat writer + reader on
/// it. `first_messenger` follows the heartbeat *role*, not who dialed the connection:
/// the **Child** (which always beats first) opens the stream, and the **Parent**
/// (which only ever answers) accepts it. Materialization happens here, in a task
/// spawned *after* the connection is established and paired, so the first beat never
/// delays either. If the connection dies before the stream comes up, this just ends.
async fn heartbeat_stream_task<N: Net>(
    conn: N::Conn,
    first_messenger: bool,
    beats_rx: mpsc::UnboundedReceiver<Heartbeat>,
    hb_events: mpsc::UnboundedSender<HeartbeatEvent>,
    shutdown: watch::Receiver<bool>,
    log_heartbeats: bool,
) {
    let Ok((hb_send, hb_recv)) = conn.stream(first_messenger).await else {
        return; // connection died before the heartbeat stream came up
    };
    // Reader in its own task; writer runs inline (it owns `conn` to keep it alive and
    // to name it in the send log).
    tokio::task::spawn_local(heartbeat_reader_task::<N>(hb_recv, hb_events));
    heartbeat_writer_task::<N>(hb_send, beats_rx, shutdown, conn, log_heartbeats).await;
}

/// Transport for one end of a stream: `send` hands a command to the writer task.
/// Dropping it ends the writer, which finishes the stream — the peer's reader then
/// sees EOF.
struct StreamConnectionTransport {
    tx: mpsc::UnboundedSender<ConnectionCommand>,
}

impl ConnectionTransport for StreamConnectionTransport {
    fn send(&self, action: ConnectionCommand) -> bool {
        self.tx.send(action).is_ok()
    }
}

/// Write each queued command as a frame onto the data stream. Beats travel on the
/// separate heartbeat stream (see [`heartbeat_writer_task`]), so this task only
/// handles data/control frames. Snoops its own outgoing `Establish` and forwards the
/// local ident to the heartbeat task. On teardown, drains queued frames first, then
/// finishes the stream. The send half keeps the connection alive for the writer's
/// lifetime — including the graceful-shutdown wait below — so dropping it (together
/// with the reader's and heartbeat's halves) closes the connection.
async fn writer_task<N: Net>(
    mut send: ConnSend<N>,
    mut rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    mut shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
    // Signalled by this connection's reader once the peer responds to our shutdown
    // (its own Severed frame, or the connection ending) — our cue that the peer
    // received the shutdown notice and we can close.
    mut peer_responded: mpsc::UnboundedReceiver<()>,
    // To forward our own outgoing Establish's ident to the heartbeat task.
    hb_events: mpsc::UnboundedSender<HeartbeatEvent>,
) {
    let mut graceful = false;
    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break; // transport dropped
                };
                // Snoop our own Establish so the heartbeat task learns our ident (its
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
            _ = shutdown.changed() => {
                // Graceful teardown. Flush whatever is already queued, then send an
                // explicit shutdown notice. A Severed frame is stream data, which the
                // transport retransmits until delivered, so the peer learns of
                // teardown directly rather than by inferring a dropped socket.
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
    let _ = send.shutdown().await;
    // On graceful shutdown, hold the connection (via `send`) open until the peer
    // responds to our shutdown notice — keeping it open lets the transport retransmit
    // the notice if the first packet was lost, and we close as soon as the peer's
    // reply arrives rather than after a fixed delay. Bounded so a peer that never
    // replies (e.g. already dead) can't hang teardown.
    if graceful {
        let _ = tokio::time::timeout(shutdown_ack_timeout(), peer_responded.recv()).await;
    }
    // `send` is dropped here, releasing this task's hold on the connection.
    drop(send);
}

/// Write each beat this connection's heartbeat task produces onto the dedicated
/// heartbeat stream. The stream's send priority (set when it was materialized) packs
/// beats ahead of queued data-stream bytes under a full congestion window. Holds the
/// connection so it stays alive while beats flow, and finishes the stream on
/// shutdown. A write error just ends the task — the data reader is the one that
/// severs on connection loss.
async fn heartbeat_writer_task<N: Net>(
    mut send: ConnSend<N>,
    mut beats: mpsc::UnboundedReceiver<Heartbeat>,
    mut shutdown: watch::Receiver<bool>,
    // Held to keep the connection alive while beats flow and to name it in the log.
    conn: N::Conn,
    // When set (acceptor/serve side only, so the joiner root's tens of thousands of
    // connections don't flood), and MM_QUIC_DEBUG is on, log each heartbeat sent — so
    // a connection the peer later reaps can be proven to have kept sending.
    log_heartbeats: bool,
) {
    let log_heartbeats = log_heartbeats && crate::ctx::connection_debug();
    let mut heartbeats_sent: u64 = 0;
    loop {
        tokio::select! {
            beat = beats.recv() => {
                let Some(heartbeat) = beat else {
                    break; // heartbeat task gone
                };
                if framing::write_heartbeat(&mut send, heartbeat).await.is_err() {
                    return;
                }
                // Flushed to the stream; log it from the *sender* so loss vs. a
                // stalled sender can be told apart at the far (receiving) end.
                if log_heartbeats {
                    heartbeats_sent += 1;
                    eprintln!(
                        "{} MM_HB pid={} sent={} on {:?}",
                        crate::ctx::wall_clock_hms(),
                        std::process::id(),
                        heartbeats_sent,
                        conn,
                    );
                }
            }
            _ = shutdown.changed() => break,
        }
    }
    let _ = send.shutdown().await;
    // `conn` is dropped here, releasing this task's hold on the connection.
    drop(conn);
}

/// Read the peer's beats off the dedicated heartbeat stream and forward each to this
/// connection's heartbeat task. On EOF/error it ends quietly: liveness loss is caught
/// either by the heartbeat task's own beat timeout or by the data reader's `Severed`
/// on connection close.
async fn heartbeat_reader_task<N: Net>(
    mut recv: ConnRecv<N>,
    hb_events: mpsc::UnboundedSender<HeartbeatEvent>,
) {
    loop {
        match framing::read_heartbeat(&mut recv).await {
            Ok(heartbeat) => {
                if hb_events
                    .send(HeartbeatEvent::ReceivedHeartbeat(heartbeat))
                    .is_err()
                {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

/// Decode each frame off the data stream and forward it to the command loop. Beats
/// arrive on the separate heartbeat stream (see [`heartbeat_reader_task`]), so this
/// reader is pure data plumbing. An error/EOF is a hard transport close: it emits
/// `Severed` to the loop and `ReaderClosed` to the heartbeat task — this is the path
/// that detects an unclean peer loss even for a link whose beats are silent (a
/// delegated child, or a live-but-idle connection).
async fn reader_task<N: Net>(
    mut recv: ConnRecv<N>,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    // Signals this connection's writer that the peer has responded to our shutdown
    // (it sent its own Severed, or the connection ended) so the writer can close.
    peer_responded: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
    // Forwards inbound heartbeats and liveness/close signals to the heartbeat task.
    hb_events: mpsc::UnboundedSender<HeartbeatEvent>,
) {
    // Per-connection diagnostics, folded into the failure reason under MM_QUIC_DEBUG:
    // did we ever read the peer's Establish? how many commands arrived, and how long
    // since the last read? Tracked cheaply either way; only formatted when debug is on.
    let debug = crate::ctx::connection_debug();
    let start = std::time::Instant::now();
    let mut established = false;
    let mut commands: u64 = 0;
    let mut last_read: Option<std::time::Duration> = None;
    let stats = |established: bool, commands, last_read: Option<_>| {
        let last = match last_read {
            Some(d) => format!(
                "{:.1}s ago",
                start.elapsed().saturating_sub(d).as_secs_f64()
            ),
            None => "never".to_owned(),
        };
        format!(
            "established={established}, commands={commands}, age={:.1}s, last_read={last}",
            start.elapsed().as_secs_f64(),
        )
    };
    loop {
        // The owning actor's gateway client is snapshot per frame so a large part is
        // read straight into the slab once the client is known (a gateway seeds its
        // own at creation, before any frame arrives).
        let read = framing::read_frame(&mut recv, &shm.mapper, shm.client());
        match read.await {
            Ok(action) => {
                last_read = Some(start.elapsed());
                commands += 1;
                // The peer's Establish is how we learn its identity: reading it is the
                // moment this side considers the connection established. Forward the
                // peer ident to the heartbeat task (one-shot, at setup).
                if let ConnectionCommand::Establish { ident, .. } = &action {
                    established = true;
                    if let Some(ident) = ident {
                        let _ = hb_events.send(HeartbeatEvent::EstablishPeer {
                            peer_ident: ident.clone(),
                        });
                    }
                }
                // A Severed from the peer is its response to our shutdown (or the peer
                // initiating its own) — wake any writer waiting to close.
                if matches!(action, ConnectionCommand::Severed { .. }) {
                    let _ = peer_responded.send(());
                }
                if loop_tx
                    .send(Command::ConnectionAction { connection, action })
                    .is_err()
                {
                    return;
                }
            }
            Err(err) => {
                let _ = peer_responded.send(());
                let _ = hb_events.send(HeartbeatEvent::ReaderClosed);
                let reason = if debug {
                    // Walk the io::Error source chain to recover the concrete
                    // underlying error — the top-level Display is often just a bare
                    // "connection lost", dropping the real cause ("timed out" vs
                    // "reset by peer" vs "closed by peer").
                    let mut detail = err.to_string();
                    let mut src = std::error::Error::source(&err);
                    while let Some(e) = src {
                        detail.push_str(": ");
                        detail.push_str(&e.to_string());
                        src = e.source();
                    }
                    format!(
                        "quic connection closed: {detail} ({})",
                        stats(established, commands, last_read)
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
