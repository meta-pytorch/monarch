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
//! ## Streams and "who speaks first"
//!
//! Every connection carries a fixed number of independent, ordered stream pairs (the
//! `STREAMS` count in [`crate::net_transport`], passed to [`Net::bind`]/
//! [`Net::connect`] so a protocol that maps each stream onto its own OS connection
//! (tcp) knows how many to pair; quic multiplexes them on one connection). Each call
//! to [`NetConn::stream`] hands back the *next* stream in sequence — there is no
//! index to get wrong, so the pairs cannot be requested out of order. The two peers
//! request their streams in the same order, so stream *k* on one side pairs with
//! stream *k* on the other; each successive stream carries a higher send priority, so
//! the heartbeat stream (requested after the data stream) is packed ahead of a
//! backlogged data stream.
//!
//! The connection abstraction knows nothing about heartbeats or parent/child roles.
//! For each stream it is simply told, via [`NetConn::stream`]'s `first_messenger`
//! flag, whether **this** side opens it and writes first or waits for the peer to
//! open it — the two peers pass complementary values. The generic transport (which
//! *does* know about heartbeats) computes those flags.
//!
//! ## Laziness
//!
//! [`NetConn::stream`] returns an awaitable that touches the wire only on its first
//! poll (quic: `open_bi`/`accept_bi`). The generic transport awaits the data stream
//! immediately but holds the heartbeat stream un-awaited until a beat actually needs
//! to flow, so a message-only side channel never establishes a heartbeat stream. The
//! data stream is always requested before the heartbeat stream, which is how the two
//! peers keep their pairs aligned.

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

    /// Bind a server listener on `addr`. `streams` is how many parallel streams
    /// every accepted connection will carry (a per-stream-per-socket protocol pairs
    /// that many; quic ignores it).
    fn bind(&mut self, addr: SocketAddr, streams: usize) -> anyhow::Result<Self::Listener>;

    /// The next inbound connection, or `None` once `listener` is closed.
    async fn accept(listener: &Self::Listener) -> Option<Self::Conn>;

    /// Dial `addr` once, or `None` on failure — the caller retries with backoff (a
    /// join may precede its serve). `streams` is as in [`Net::bind`].
    async fn connect(dialer: &Self::Dialer, addr: SocketAddr, streams: usize)
    -> Option<Self::Conn>;

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

    /// An awaitable for the **next** stream on this connection. Each call advances a
    /// per-connection cursor, so streams cannot be requested out of order and the two
    /// peers' *k*-th streams pair up. `first_messenger` selects whether **this** side
    /// opens the stream and writes first, or waits for the peer to open it; the two
    /// peers pass complementary values. Each successive stream is given a higher send
    /// priority, so a later stream (the heartbeat stream) outranks an earlier one (the
    /// data stream). Nothing touches the wire until the returned future is polled.
    fn stream(&self, first_messenger: bool) -> Self::Stream;
}
