/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A Chrysalis carrier that carries datagrams over byte streams.
//!
//! Chrysalis sends its traffic as datagrams: each QUIC packet is a separate,
//! self-contained message, and its other carriers (in-process, UDP and Unix
//! datagram sockets) all deliver datagrams. In this configuration, a
//! Firecracker microVM has no IP network, so its only link to its host is vsock,
//! which Firecracker supports only as byte streams. [`VsockDatagramSocket`]
//! adapts one to the other. It frames each datagram onto a stream and recovers
//! it at the other end. QUIC still provides reliability, ordering and
//! encryption; the stream underneath only carries its packets.
//!
//! ## Terminology
//!
//! - **Logical datagram socket:** the public [`VsockDatagramSocket`]. It sends
//!   and receives addressed datagrams and owns the engine.
//! - **Listener:** a local OS object that accepts new stream connections. It
//!   does not carry datagrams itself. The implementations are [`UnixListener`]
//!   and the `AsyncFd<OwnedFd>` returned by `native_listen`.
//! - **Stream connection:** one bidirectional byte path between two processes.
//!   Each process owns one local stream endpoint; kernel and VMM state connect
//!   those endpoints. No single Rust value owns the whole connection.
//! - **Stream endpoint:** one process's local OS handle for a stream
//!   connection. A connection has one endpoint in each process. This module
//!   represents a local endpoint as [`UnixStream`] or `NativeStreamEndpoint`.
//! - **Engine:** `VsockDatagramEngine`, the listeners, local stream endpoints,
//!   queues, tasks, and shutdown machinery behind one logical datagram socket.
//!
//! A message from a microVM on one host to a microVM on another crosses three
//! links. Only the two vsock links, between each microVM and its host's relay,
//! are byte streams and need this adapter. The link between the hosts carries
//! datagrams over an ordinary carrier such as `UdpSocket`, and each relay
//! combines the two in a `DatagramSocketSet`.
//!
//! ```text
//!     host 1                              host 2
//!
//!     relay ◄────── UDP datagrams ──────► relay
//!       ║              (no adapter)         ║
//!       ║  vsock: byte stream               ║  vsock: byte stream
//!       ║  (this adapter)                   ║  (this adapter)
//!       ║                                   ║
//!     VM 1                                VM 2
//! ```
//!
//! The four Chrysalis nodes expose these node-facing logical datagram sockets:
//!
//! ```text
//! VM 1:
//!   VsockDatagramSocket
//!
//! relay 1:
//!   DatagramSocketSet
//!   ├── VsockDatagramSocket   local VM link: byte stream, this adapter
//!   └── UdpSocket             host-to-host link: datagrams, no adapter
//!
//! relay 2:
//!   DatagramSocketSet
//!   ├── UdpSocket             host-to-host link: datagrams, no adapter
//!   └── VsockDatagramSocket   local VM link: byte stream, this adapter
//!
//! VM 2:
//!   VsockDatagramSocket
//! ```
//!
//! The namespace tree is:
//!
//! ```text
//! Alice (VM A, host 1)           namespace root
//! └── Relay 1 (host 1)           child of Alice
//!     └── Relay 2 (host 2)       child of Relay 1
//!         └── Bob (VM B, host 2) child of Relay 2
//! ```
//!
//! Children establish the parent links:
//!
//! ```text
//! Relay 1 ── Firecracker hybrid vsock ──► Alice
//! Relay 2 ─────────── UDP ──────────────► Relay 1
//! Bob     ── native vsock to host 2 ────► Relay 2
//! ```
//!
//! Alice's end-to-end QUIC traffic to Bob follows the namespace downward:
//!
//! ```text
//! Alice ──vsock──► Relay 1 ──UDP──► Relay 2 ──vsock──► Bob
//! ```
//!
//! `VsockDatagramSocket` obtains its underlying byte stream in three ways:
//!
//! - `unix:` connects to or listens on a Unix stream socket;
//! - `native:` connects to or listens on an `AF_VSOCK` CID and port;
//! - `hybrid:` reaches a guest from the host by connecting to Firecracker's
//!   Unix socket and requesting a guest port with `CONNECT <port>\n`.
//!
//! Each stream connection is bidirectional. Both local stream endpoints can
//! send framed datagrams over that same connection.
//!
//! A byte stream does not preserve message boundaries, so the carrier writes a
//! sequence of frames. Each frame has a five-byte header followed by `length`
//! payload bytes. The length does not include the header.
//!
//! ```text
//! offset  0               1                  5
//!         ┌───────────────┬──────────────────┬──────────────────────┐
//!         │ kind: 1 byte  │ length: 4 bytes  │ payload: length bytes│
//!         │               │ big-endian u32   │                      │
//!         └───────────────┴──────────────────┴──────────────────────┘
//! ```
//!
//! Kind 0 is the connection's initial HELLO frame. Kind 1 carries one complete
//! datagram. The length tells the receiver exactly where the payload ends and
//! the next frame begins.
//!
//! A dialer starts the connection with one HELLO frame containing its
//! `node:<name>` address. `<name>` is the caller-supplied socket name; it is a
//! carrier routing label, not an authenticated identity. The acceptor stores
//! the connection under that label and reports its datagrams as coming from
//! that `node:` address.
//!
//! On the side that opened the connection, datagrams read from the stream are
//! reported as coming from the endpoint it originally dialed. On either side,
//! the address returned by `recv_from` can therefore be passed to `try_send_to`
//! to reply on the same connection. QUIC/TLS above this carrier authenticates
//! the peer.
//!
//! [`UnixStream`]: tokio::net::UnixStream

use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;

use tokio::io::ReadBuf;
use tokio::net::UnixListener;

use crate::DatagramAddr;
use crate::DatagramSocket;
use crate::DatagramTransmit;

mod address;
mod engine;
mod frame;
mod hybrid;
mod native;
#[cfg(test)]
mod tests;

use address::ParsedDestination;
pub use address::VSOCK_SCHEME;
pub use address::VsockDatagramAddr;
use engine::VsockDatagramEngine;
use frame::FrameKind;
use frame::MAX_PAYLOAD;
use frame::encode_frame;
pub use native::VMADDR_CID_ANY;
pub use native::VMADDR_CID_HOST;
use native::native_listen;

/// Creates an [`io::ErrorKind::InvalidInput`] error with `message`.
fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Creates the [`io::ErrorKind::BrokenPipe`] error returned after shutdown.
fn shutdown_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "vsock transport is shut down")
}

/// A [`DatagramSocket`] that transports complete datagrams over bidirectional
/// Unix or AF_VSOCK byte streams.
///
/// Here, "socket" means one logical datagram transport endpoint, not one kernel
/// file descriptor. This implementation combines listeners, local stream
/// endpoints, per-connection send queues, and one shared receive queue behind the
/// [`DatagramSocket`] interface.
///
/// One logical datagram socket owns one engine, which may manage several
/// listeners and local stream endpoints:
///
/// ```text
/// VsockDatagramSocket
/// └── VsockDatagramEngine
///     ├── listeners
///     │   ├── Unix listener
///     │   └── native AF_VSOCK listener
///     ├── local stream endpoints
///     │   ├── accepted endpoint A ── its send queue
///     │   ├── accepted endpoint B ── its send queue
///     │   └── dialed endpoint C   ── its send queue
///     └── one receive queue shared by every endpoint
/// ```
///
/// One Alice–Relay stream connection has two endpoints, one in each engine.
/// Each direction starts in one engine's send queue and ends in the other
/// engine's receive queue:
///
/// ```text
/// Alice → Relay:
///   try_send_to → send queue → writer → Alice endpoint
///   Alice endpoint → framed datagrams over stream → Relay endpoint
///   Relay endpoint → reader → receive queue → recv_from
///
/// Relay → Alice:
///   try_send_to → send queue → writer → Relay endpoint
///   Relay endpoint → framed datagrams over stream → Alice endpoint
///   Alice endpoint → reader → receive queue → recv_from
/// ```
///
/// The Alice and Relay endpoints are the two ends of the same bidirectional
/// Unix or vsock stream connection. The send and receive queues are local to
/// their engines; they are not part of the stream connection.
///
/// [`VsockDatagramSocket::bind`] gives the socket its `node:<name>` address and
/// optionally opens `native:` or `unix:` listeners. The first send to a
/// `native:`, `hybrid:`, or `unix:` address opens a connection; later sends
/// reuse it. Sending to a `node:` address reuses the live connection that
/// announced that name and fails with [`io::ErrorKind::HostUnreachable`] when
/// no such connection exists.
///
/// Datagrams received on a connection opened by this socket are reported as
/// coming from the address it dialed. Datagrams received on an accepted
/// connection are reported as coming from the peer's announced `node:` address.
/// In either case, callers can reply to the address returned by `recv_from`.
///
/// `try_send_to` never waits. It queues the datagram for the connection, or
/// drops it when that connection's send queue is full. Calling `shutdown`, or
/// dropping the socket, stops its tasks and closes its connections.
#[derive(Debug)]
pub struct VsockDatagramSocket {
    /// Internal engine shared with every task this socket starts.
    inner: Arc<VsockDatagramEngine>,
}

impl VsockDatagramSocket {
    /// Creates a socket named `node:<name>` and binds its listeners.
    ///
    /// `name` is required because this socket's local address is
    /// `node:<name>`. If the socket later opens a connection, its HELLO frame
    /// tells the peer to use that address for replies over the same connection.
    ///
    /// Each `Native` or `Unix` address in `listen` is bound immediately so
    /// peers can connect to it. An empty slice creates a socket with no
    /// listeners. `Node` and `Hybrid` are not valid listening addresses.
    ///
    /// This function does not open any peer connections. `try_send_to` opens
    /// one later when needed.
    ///
    /// This function starts listener tasks and must be called inside a Tokio
    /// runtime. The socket keeps that runtime for work started by later sends,
    /// which may come from threads without a runtime of their own.
    pub fn bind(name: &str, listen: &[VsockDatagramAddr]) -> io::Result<Self> {
        // Validate the logical address and runtime before acquiring listeners.
        let local_addr = DatagramAddr::try_from(&VsockDatagramAddr::Node(name.to_owned()))?;
        let runtime = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;

        // Bind every listener before starting tasks. If any bind fails, release
        // every Unix path created earlier in this call.
        let mut unix_listeners = Vec::new();
        let mut native_listeners = Vec::new();
        let mut listener_paths = Vec::new();
        let bound = listen.iter().try_for_each(|address| {
            match address {
                VsockDatagramAddr::Unix(path) => {
                    unix_listeners.push(UnixListener::bind(path)?);
                    listener_paths.push(path.clone());
                }
                VsockDatagramAddr::Native { cid, port } => {
                    native_listeners.push(native_listen(*cid, *port)?);
                }
                VsockDatagramAddr::Node(_) | VsockDatagramAddr::Hybrid { .. } => {
                    return Err(invalid_input(
                        "a vsock socket listens only on native and unix addresses",
                    ));
                }
            }
            Ok(())
        });
        if let Err(error) = bound {
            remove_paths(&listener_paths);
            return Err(error);
        }

        // Create the shared queues and shutdown tracking before tasks start.
        let engine = Arc::new(VsockDatagramEngine::new(
            runtime,
            local_addr,
            listener_paths,
        ));
        // Move each bound listener into a task that accepts peer connections.
        engine.start_tasks(unix_listeners, native_listeners);
        // The socket owns the original Arc; its tasks hold only engine clones.
        Ok(Self { inner: engine })
    }

    /// Requests shutdown and removes this socket's Unix listener paths.
    ///
    /// The first call signals every engine-owned task to stop and prevents new
    /// datagrams from entering the receive queue. Datagrams already queued
    /// remain readable. Later calls do nothing. Use [`Self::join`] to wait for
    /// every task to finish.
    pub fn shutdown(&self) {
        // Atomically change RUNNING to SHUTTING_DOWN. Only the caller that
        // performs that transition runs the one-time cleanup below.
        if !self.inner.shutdown.shutdown() {
            return;
        }
        // Reject and wake future sends, but keep buffered datagrams available
        // for `recv_from` to drain before the queue reports end-of-stream.
        lock_or_recover(&self.inner.receive_rx).close();
        // Use synchronous path removal because `Drop` also calls this method.
        remove_paths(&self.inner.listener_paths);
    }

    /// Waits until every task this socket started has ended.
    pub async fn join(&self) {
        self.inner.shutdown.join().await;
    }
}

impl DatagramSocket for VsockDatagramSocket {
    /// Signals this socket's tasks and closes its receive queue.
    fn shutdown(&self) {
        VsockDatagramSocket::shutdown(self);
    }

    /// Waits for the shutdown supervisor to observe that every engine-owned
    /// task has ended.
    fn join(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(VsockDatagramSocket::join(self))
    }

    /// Returns the logical `node:<name>` address created by [`Self::bind`].
    fn local_addr(&self) -> &DatagramAddr {
        &self.inner.local_addr
    }

    /// Queues one datagram for `destination` without waiting.
    ///
    /// A live connection is reused. Otherwise, a dialable address starts one
    /// connection attempt and the datagram waits behind it. A full connection
    /// queue drops the datagram and still returns success.
    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        if !self.inner.shutdown.is_running() {
            return Err(shutdown_error());
        }
        if datagram.len() > MAX_PAYLOAD {
            return Err(invalid_input("a vsock datagram is at most 65,535 bytes"));
        }
        let destination = ParsedDestination::try_from(destination)
            .map_err(|error| io::Error::new(io::ErrorKind::AddrNotAvailable, error))?;
        let frame = encode_frame(FrameKind::Datagram, datagram);

        self.inner.queue_or_dial(destination, frame)
    }

    /// Reports immediate send readiness while the socket is running.
    ///
    /// Sends never wait for queue capacity: `try_send_to` drops a datagram when
    /// its connection queue is full. After shutdown, this returns `BrokenPipe`.
    fn poll_send_ready(
        &self,
        _cx: &mut Context<'_>,
        _transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.inner.shutdown.is_running() {
            return Poll::Ready(Err(shutdown_error()));
        }
        Poll::Ready(Ok(()))
    }

    /// Attempts to receive the first queued datagram without blocking the
    /// thread.
    ///
    /// If no datagram is available, this registers the current task to be
    /// woken when one arrives and returns `Poll::Pending`.
    ///
    /// If the datagram is larger than the buffer's remaining space, only the
    /// fitting prefix is copied and the rest is discarded. During shutdown,
    /// datagrams already queued are returned first; once the queue is empty,
    /// this returns `BrokenPipe`.
    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>> {
        // A pending receive must wake for either data or shutdown. `poll_recv`
        // below registers this task with the receive queue for data; this
        // registers it with `ShutdownState` for shutdown. Repeated polls
        // deduplicate the same waker.
        self.inner.shutdown.register_waker(cx.waker());
        // `poll_recv` needs mutable access to the one receiver, while
        // `DatagramSocket` exposes this method through `&self`. Lock only the
        // receiver for this poll; connection tasks can still send without this
        // lock.
        let mut receive_rx = lock_or_recover(&self.inner.receive_rx);
        if !self.inner.shutdown.is_running() {
            // Stop new deliveries while preserving datagrams already queued.
            receive_rx.close();
        }
        match receive_rx.poll_recv(cx) {
            Poll::Ready(Some((source, payload))) => {
                // Follow DatagramSocket's UDP-style truncation: copy the prefix
                // that fits and discard the rest of this datagram.
                let len = payload.len().min(buffer.remaining());
                buffer.put_slice(&payload[..len]);
                Poll::Ready(Ok(source))
            }
            // This receiver is closed only during shutdown, and returns None
            // only after every buffered datagram has been drained.
            Poll::Ready(None) => Poll::Ready(Err(shutdown_error())),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Reports that this carrier preserves each datagram as one complete
    /// message.
    ///
    /// The underlying byte stream may split the frame's bytes across reads,
    /// but the receiver reconstructs the complete frame before delivering its
    /// payload. QUIC therefore never receives carrier-level datagram fragments.
    fn may_fragment(&self) -> bool {
        false
    }
}

impl Drop for VsockDatagramSocket {
    /// Requests shutdown when the socket is dropped.
    ///
    /// Drop cannot wait for task completion; callers that need that guarantee
    /// must call `shutdown` and then `join`.
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Best-effort removes Unix listener paths during bind rollback or shutdown.
///
/// Removal errors are ignored so cleanup does not replace the original bind
/// error or make shutdown and `Drop` fail.
fn remove_paths(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
    }
}

/// Locks `mutex`, recovering the data if a holder panicked: every critical
/// section here leaves its data consistent.
fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
