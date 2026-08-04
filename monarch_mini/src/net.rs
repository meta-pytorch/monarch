/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The seam between the generic connection transport and a concrete network
//! protocol (quic today; tcp later).
//!
//! Everything about *establishment* — announcing identity, the hello, liveness
//! reporting, gateway side channels, matching serves to joins, retry/backoff,
//! graceful shutdown — lives in [`crate::net_transport`] and is identical across
//! protocols. A [`Net`] implementation supplies only the raw networking: parsing an
//! address, building the shared dialing state, binding a listener, dialing, and
//! producing a [`NetConn`] that hands out ordered byte-stream pairs.
//!
//! ## Streams, addressed by index
//!
//! A connection hands out independent, ordered stream pairs on demand via
//! [`NetConn::stream`], each addressed by an explicit `index`. The two peers name the
//! same index for the two halves of one logical stream, so stream *k* on one side pairs
//! with stream *k* on the other regardless of the order either side requests them — the
//! index is carried on the wire (a small prefix), so the pairing can never drift.
//! `priority` is the send priority (quic uses it to pack a higher-priority stream — the
//! heartbeat stream — ahead of a backlogged data stream; tcp has no per-stream priority
//! and ignores it).
//!
//! **Direction is a property of the connection, not the call.** A [`Net::connect`]
//! yields a *dialer* connection whose [`NetConn::stream`] **opens** each stream (quic:
//! `open_bi`; tcp: dials a fresh socket); a [`Net::accept`] yields an *acceptor*
//! connection whose [`NetConn::stream`] **awaits** the peer's matching stream (quic:
//! `accept_bi`, demultiplexed by the index prefix; tcp: the socket the listener routed
//! to this connection for that index). Both peers just call `stream(k, prio)` — the
//! generic transport never has to reason about who speaks first. This is what lets a
//! plain tcp socket work: only the dialer can open one, so only the dialer's `stream`
//! opens; the acceptor's waits.
//!
//! The connection knows nothing about heartbeats or parent/child roles. The generic
//! transport picks the indices: index 0 is the data/control stream, index 1 is the
//! heartbeat stream (a higher `priority`), and it may later request further data-stream
//! indices for striping large messages.
//!
//! ## Laziness
//!
//! [`NetConn::stream`] returns an awaitable that touches the wire only on its first poll
//! (quic dialer: `open_bi`; quic acceptor: an `accept_bi` demux; tcp dialer: a socket
//! dial). The generic transport awaits the data stream immediately but can hold a
//! heartbeat or data stream un-awaited until it is actually needed, so a message-only
//! side channel (whose dialer never opens index 1) never establishes a heartbeat
//! stream.

use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::runtime::Handle;

/// The network-protocol-specific operations the generic transport is built on.
pub(crate) trait Net: Sized + 'static {
    /// A cheap, cloneable dialing handle a connector or side-channel task owns and
    /// dials through repeatedly (quic: a client `Endpoint` from the pool).
    type Dialer: Clone + 'static;
    /// A bound server an acceptor task owns and accepts connections on (quic: a
    /// server `Endpoint`).
    type Listener: 'static;
    /// One established connection; hands out ordered stream pairs.
    type Conn: NetConn + 'static;

    /// Build the shared per-context networking state, lazily, on the first serve or
    /// join (quic: install the crypto provider; TLS is loaded on first bind/dial).
    ///
    /// `runtime`, when `Some`, is the multi-threaded runtime the caller wants this
    /// protocol's network *work* to run on (so it spreads across cores rather than
    /// serializing on the command-loop thread). A protocol whose work lives in a
    /// background driver (quic: the per-endpoint driver quinn spawns at endpoint
    /// construction) builds its endpoints/sockets with this runtime entered, placing
    /// that driver — and thus its crypto — on the pool. A protocol whose work lives in
    /// the foreground stream poll (tcp/kTLS: the read/write syscalls happen when the
    /// stream is polled) cannot relocate its work from here — the generic transport
    /// spawns its data coroutines on the same runtime instead — so it ignores this.
    /// `None` ⇒ everything stays on the command-loop thread (single-core default).
    fn create(runtime: Option<Handle>) -> anyhow::Result<Self>;

    /// Parse a transport url (e.g. `quic://[::1]:7001`, or a bare `@endpoint` tag)
    /// into a socket address.
    fn parse_addr(url: &str) -> anyhow::Result<SocketAddr>;

    /// A dialing handle for `addr`'s address family, from the (lazily built) client
    /// pool.
    fn dialer(&mut self, addr: SocketAddr) -> anyhow::Result<Self::Dialer>;

    /// Bind a server listener on `addr`. Streams are opened lazily and addressed by
    /// index (see [`NetConn::stream`]), so the listener needs no stream count up front.
    fn bind(&mut self, addr: SocketAddr) -> anyhow::Result<Self::Listener>;

    /// The next inbound connection, or `None` once `listener` is closed. The returned
    /// connection is an *acceptor* connection: its [`NetConn::stream`] awaits the peer's
    /// streams rather than opening them.
    async fn accept(listener: &Self::Listener) -> Option<Self::Conn>;

    /// Dial `addr` once, or `None` on failure — the caller retries with backoff (a join
    /// may precede its serve). The returned connection is a *dialer* connection: its
    /// [`NetConn::stream`] opens streams. No stream is opened here (quic does complete
    /// its handshake; tcp opens nothing until the first [`NetConn::stream`]), so a tcp
    /// join's reachability is proven by the first `stream` call, not by `connect`.
    async fn connect(dialer: &Self::Dialer, addr: SocketAddr) -> Option<Self::Conn>;

    /// The default cap on simultaneous client connect *attempts* when
    /// `MM_QUIC_MAX_CONCURRENT_CONNECTS` is unset (see [`crate::net_transport`]'s
    /// connect throttle). `None` ⇒ unlimited; each protocol overrides with a concrete
    /// bound. tcp needs a much smaller cap than quic: it opens `streams` OS sockets
    /// *and* a TLS handshake per connection and pairs them on the acceptor, so a large
    /// concurrent connect storm on the single-threaded runtime starves stragglers
    /// (connections that never finish pairing), whereas quic multiplexes its streams on
    /// one endpoint-paced connection.
    fn default_connect_concurrency() -> Option<usize> {
        None
    }
}

/// An established connection. It knows nothing about heartbeats, roles, or
/// preambles — it just hands out ordered streams, told per stream which side speaks
/// first. `Debug` supplies the peer identity for diagnostics (quic: the remote
/// address), used by the heartbeat send log.
pub(crate) trait NetConn: Debug + 'static {
    /// The send half of a stream; framing drives it via [`AsyncWrite`]. It keeps the
    /// underlying connection alive for its lifetime. `Send` so the data writer
    /// coroutine can run on a multi-threaded runtime (the connection handle and the
    /// heartbeat stream stay single-threaded; only the two data halves cross threads).
    type Send: AsyncWrite + Unpin + Send + 'static;
    /// The receive half of a stream; framing drives it via [`AsyncRead`]. It keeps
    /// the underlying connection alive for its lifetime. `Send` for the same reason as
    /// [`Self::Send`] — the data reader coroutine may run off the command-loop thread.
    type Recv: AsyncRead + Unpin + Send + 'static;
    /// The awaitable that materializes one stream's halves on first poll.
    type Stream: Future<Output = std::io::Result<(Self::Send, Self::Recv)>> + 'static;

    /// An awaitable for the stream addressed by `index`, paired with the peer's stream
    /// of the same index. On a *dialer* connection it opens the stream; on an *acceptor*
    /// connection it awaits the peer's matching one (see the module docs). The index is
    /// carried on the wire so the two peers' *k*-th streams pair up regardless of request
    /// order. `priority` is the send priority (quic; tcp ignores it) — the transport
    /// gives the heartbeat stream a higher priority than the data stream so a beat is
    /// packed ahead of backlogged data. Nothing touches the wire until the returned
    /// future is polled.
    fn stream(&self, index: usize, priority: i32) -> Self::Stream;
}
