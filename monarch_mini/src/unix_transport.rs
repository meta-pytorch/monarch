/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! UNIX-socket transport for connecting actors in two different local
//! processes.
//!
//! This is pure plumbing. A connection's coroutines bring up the socket, produce
//! a [`ConnectionTransport`] for it, and hand it to the command loop via
//! `Command::TransportConnected`; the reader forwards every frame it decodes back
//! as a `ConnectionAction`. All establishment policy (announcing our identity,
//! learning the peer's, hello, liveness) lives in the command loop and is
//! identical to inproc — this file never reads actor state or builds an
//! `Establish`. An `Establish` is just another frame on the wire.
//!
//! ## Framing and shared memory
//!
//! Framing lives in [`crate::unix_framing`]. Because the UNIX transport carries
//! shared-memory part descriptors and passes file descriptors (the slab grant
//! tokens) over the socket via `SCM_RIGHTS`, the reader and writer share one
//! [`UnixStream`] (via `Arc`) and drive it through the readiness API
//! (`readable`/`writable` + `try_read`/`try_write`/`try_io`) rather than the
//! split read/write halves — both directions and the fd-exchange step then use a
//! single reactor registration. Each connection is handed the context's
//! [`MapperHandle`] and its owning actor's [`ShmClientSlot`]; large parts move
//! through the slab once that slot holds a gateway client, and stay inline
//! otherwise.
//!
//! ## Liveness
//!
//! Dropping a connection's [`UnixConnectionTransport`] drops its writer channel,
//! which ends the writer task; the writer then shuts the socket's write half down,
//! and the peer's reader sees EOF and emits `Severed`. That is the same "drop the
//! transport ⇒ peer severed" contract the inproc transport implements explicitly.

use std::collections::HashMap;
use std::future::Future;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;

use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::ctx::Command;
use crate::matcher::Matcher;
use crate::shm::MapperHandle;
use crate::shm::ShmClient;
use crate::shm::ShmClientSlot;
use crate::transport::Transport;
use crate::unix_framing;

/// Connect-retry backoff bounds. A join may be posted before its serve, so the
/// connector polls; it starts fast and backs off (doubling) to a steady poll so a
/// serve that is slow — or never comes — doesn't spam the OS with connect()s.
const CONNECT_RETRY_MIN: Duration = Duration::from_millis(5);
const CONNECT_RETRY_MAX: Duration = Duration::from_millis(1000);

/// A reader waiting for a frame spins this long peeking the socket before
/// parking on `readable().await`, trading CPU for the elimination of the kernel
/// read-wakeup latency. Sized to comfortably exceed a small-message round trip
/// so steady ping-pong traffic never parks; an idle reader spins this long once,
/// then parks to zero CPU.
const READ_SPIN: Duration = Duration::from_micros(200);

/// What a connection's coroutines need for shared memory: the context-global
/// mapper and the owning actor's client slot.
#[derive(Clone)]
struct ShmCtx {
    mapper: MapperHandle,
    client: ShmClientSlot,
}

impl ShmCtx {
    /// Snapshot the owning actor's gateway client (`None` until it arrives).
    fn client(&self) -> Option<ShmClient> {
        *self.client.lock().expect("shm client slot mutex poisoned")
    }

    /// Record the owning actor's gateway client (called by the reader the instant
    /// it decodes a gateway-state handoff, before the command loop re-propagates).
    fn set_client(&self, client: ShmClient) {
        *self.client.lock().expect("shm client slot mutex poisoned") = Some(client);
    }
}

/// Owns all UNIX transport state and coroutines. The command loop holds one of
/// these and forwards serves/joins to it; it never sees sockets, urls, or pairing
/// state. The loop sender, teardown signal, and context mapper are captured here.
pub(crate) struct UnixTransport {
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown_tx: watch::Sender<bool>,
    // The context-global address-space mapper, captured once at construction and
    // handed to every connection's coroutines (the per-actor client slot is passed
    // in per serve/join).
    mapper: MapperHandle,
    // One listener coroutine per url; serve connections are forwarded to it and
    // it owns the serve/accept pairing.
    listeners: HashMap<String, mpsc::UnboundedSender<(ConnectionRef, ShmClientSlot)>>,
    // Liveness-token issuer: each connection's writer holds a clone for its
    // lifetime, as do the listener/connector coroutines. Teardown drops this
    // issuing copy and waits for `alive_rx` to close — i.e. for every writer to
    // have flushed and exited.
    alive_tx: Option<mpsc::UnboundedSender<()>>,
    alive_rx: mpsc::UnboundedReceiver<()>,
}

impl UnixTransport {
    pub(crate) fn new(loop_tx: mpsc::UnboundedSender<Command>, mapper: MapperHandle) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let (alive_tx, alive_rx) = mpsc::unbounded_channel();
        Self {
            loop_tx,
            shutdown_tx,
            mapper,
            listeners: HashMap::new(),
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

    /// Pair the context mapper with an actor's client slot into a connection's
    /// shared-memory context.
    fn shm_ctx(&self, client: ShmClientSlot) -> ShmCtx {
        ShmCtx {
            mapper: self.mapper.clone(),
            client,
        }
    }

    /// Signal every writer to flush and exit, then wait until they all have.
    /// Reader/listener/connector coroutines observe the same signal and stop,
    /// dropping their liveness tokens; once all tokens are gone `alive_rx`
    /// closes. Called on context shutdown before the runtime is torn down, so no
    /// queued frame is lost.
    pub(crate) async fn shutdown(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.alive_tx = None; // drop the issuing token; only coroutine clones remain
        // We never send on this channel; recv resolves to None exactly when the
        // last clone is dropped (every writer flushed and exited).
        let _ = self.alive_rx.recv().await;
    }
}

impl Transport for UnixTransport {
    fn serve(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot) {
        // The first serve on a url spawns its listener coroutine; later serves
        // just forward another connection to it.
        if !self.listeners.contains_key(&url) {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::task::spawn_local(listener_task(
                socket_path(&url),
                rx,
                self.loop_tx.clone(),
                self.shutdown_tx.subscribe(),
                self.alive_token(),
                self.mapper.clone(),
            ));
            self.listeners.insert(url.clone(), tx);
        }
        let _ = self
            .listeners
            .get(&url)
            .expect("listener just inserted")
            .send((connection, shm_client));
    }

    fn join(&mut self, url: String, connection: ConnectionRef, shm_client: ShmClientSlot) {
        tokio::task::spawn_local(connector_task(
            socket_path(&url),
            connection,
            self.loop_tx.clone(),
            self.shutdown_tx.subscribe(),
            self.alive_token(),
            self.shm_ctx(shm_client),
        ));
    }
}

/// Strip the `unix://` prefix from a url, yielding the filesystem path.
fn socket_path(url: &str) -> PathBuf {
    PathBuf::from(url.strip_prefix("unix://").unwrap_or(url))
}

/// Bind `path` and pair each accepted socket with the next queued serve (either
/// may arrive first, so both sides are buffered). On a bind failure the serves
/// are severed instead. Stops on the teardown signal or when the command loop
/// drops the serve sender.
async fn listener_task(
    path: PathBuf,
    mut serves: mpsc::UnboundedReceiver<(ConnectionRef, ShmClientSlot)>,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    mapper: MapperHandle,
) {
    // A stale socket file from a previous run would make bind fail with
    // EADDRINUSE; the server owns the path, so removing it is safe.
    let _ = tokio::fs::remove_file(&path).await;
    let listener = match UnixListener::bind(&path) {
        Ok(listener) => listener,
        Err(err) => {
            // Bind failed. Do NOT retry — a later bind could race a real server on
            // this path. The url is dead for serving, so stay alive and fail every
            // serve on it (those already queued and any that arrive later) until
            // teardown, rather than letting future serves respawn and retry.
            let reason = format!("unix bind failed: {err}").into_bytes();
            loop {
                tokio::select! {
                    serve = serves.recv() => match serve {
                        Some((connection, _)) => sever(&loop_tx, connection, reason.clone()),
                        None => return, // command loop gone
                    },
                    _ = shutdown.changed() => return,
                }
            }
        }
    };

    // Serves and accepted sockets arrive independently; the matcher pairs them up
    // regardless of order and wires up each pair as it completes. Each serve
    // carries the owning actor's client slot, paired into the connection's shm
    // context.
    let mut matcher: Matcher<(ConnectionRef, ShmCtx), UnixStream> = Matcher::new();
    loop {
        tokio::select! {
            serve = serves.recv() => {
                let Some((connection, shm_client)) = serve else {
                    return; // command loop gone
                };
                let shm = ShmCtx { mapper: mapper.clone(), client: shm_client };
                let _ = matcher.push_left((connection, shm), |(connection, shm), stream| {
                    spawn_connection(stream, connection, loop_tx.clone(), shutdown.clone(), alive.clone(), shm)
                });
            }
            accepted = listener.accept() => {
                let (stream, _addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        // Usually transient (e.g. EMFILE); keep accepting, but don't
                        // swallow it silently.
                        tracing::warn!("unix accept on {} failed: {err}", path.display());
                        continue;
                    }
                };
                let _ = matcher.push_right(stream, |(connection, shm), stream| {
                    spawn_connection(stream, connection, loop_tx.clone(), shutdown.clone(), alive.clone(), shm)
                });
            }
            _ = shutdown.changed() => return,
        }
    }
}

/// Connect to `path`, retrying until the server binds (so a join may precede its
/// serve), then wire up the connection. Stops on the teardown signal.
async fn connector_task(
    path: PathBuf,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
) {
    let mut retry = CONNECT_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match UnixStream::connect(&path).await {
            Ok(stream) => {
                spawn_connection(stream, connection, loop_tx, shutdown, alive, shm);
                return;
            }
            Err(_) => {
                tokio::select! {
                    _ = tokio::time::sleep(retry) => {}
                    _ = shutdown.changed() => return,
                }
                // Back off toward the cap so a slow or absent serve is polled at
                // most once per CONNECT_RETRY_MAX.
                retry = (retry * 2).min(CONNECT_RETRY_MAX);
            }
        }
    }
}

/// Wire up a connected socket: build its [`UnixConnectionTransport`], announce it
/// to the command loop, and spawn the writer (drains commands → frames) and
/// reader (frames → `ConnectionAction`). Both share the `UnixStream` (via `Arc`)
/// so the fd-exchange step and both byte directions use one registration.
/// `TransportConnected` is enqueued before the reader can forward anything, so the
/// command loop installs the transport before any frame (including the peer's
/// `Establish`) arrives.
fn spawn_connection(
    stream: UnixStream,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
) {
    let stream = Arc::new(stream);
    let fd = stream.as_raw_fd();
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();

    let transport = Box::new(UnixConnectionTransport { tx: writer_tx });
    let _ = loop_tx.send(Command::TransportConnected {
        connection,
        transport,
    });
    tokio::task::spawn_local(writer_task(
        Arc::clone(&stream),
        fd,
        writer_rx,
        shutdown,
        alive,
        shm.clone(),
    ));
    tokio::task::spawn_local(reader_task(stream, fd, READ_SPIN, connection, loop_tx, shm));
}

/// Transport for one end of a UNIX connection: `send` hands a command to the
/// writer task, which serializes it to the socket. Dropping it ends the writer
/// (closing the write half), which the peer observes as EOF.
struct UnixConnectionTransport {
    tx: mpsc::UnboundedSender<ConnectionCommand>,
}

impl ConnectionTransport for UnixConnectionTransport {
    fn send(&self, action: ConnectionCommand) -> bool {
        // The unix wire carries every command, including gateway state (the slab +
        // dgram fds go via SCM_RIGHTS — see unix_framing).
        self.tx.send(action).is_ok()
    }
}

/// Write each queued command as a frame. Exits on a write error, when the
/// channel closes (transport dropped), or on the teardown signal — and in that
/// last case it first drains every command already queued so those writes reach
/// the OS before teardown completes. Holds `_alive` for its whole lifetime:
/// dropping it on exit is how teardown learns this writer has finished flushing.
async fn writer_task(
    stream: Arc<UnixStream>,
    fd: RawFd,
    rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
    shm: ShmCtx,
) {
    writer_task_with(
        stream,
        fd,
        rx,
        shutdown,
        shm,
        |stream, command, shm| async move {
            unix_framing::write_command(&stream, command, &shm.mapper, shm.client()).await
        },
    )
    .await;
}

async fn writer_task_with<Write, WriteFuture>(
    stream: Arc<UnixStream>,
    fd: RawFd,
    mut rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    mut shutdown: watch::Receiver<bool>,
    shm: ShmCtx,
    mut write: Write,
) where
    Write: FnMut(Arc<UnixStream>, ConnectionCommand, ShmCtx) -> WriteFuture,
    WriteFuture: Future<Output = std::io::Result<()>>,
{
    'writer: loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break; // transport dropped
                };
                if write(Arc::clone(&stream), command, shm.clone()).await.is_err() {
                    break 'writer;
                }
            }
            _ = shutdown.changed() => {
                // Teardown: the command loop has stopped, so no further commands
                // will be enqueued. Flush whatever is already queued, then stop.
                while let Ok(command) = rx.try_recv() {
                    if write(Arc::clone(&stream), command, shm.clone()).await.is_err() {
                        break 'writer;
                    }
                }
                break;
            }
        }
    }
    // Every queued frame has been handed to the OS; shut the write side down to
    // flush and signal EOF to the peer. (The reader keeps its half open.)
    shutdown_write(fd);
}

/// Half-close the write direction of the socket so the peer's reader sees EOF.
fn shutdown_write(fd: RawFd) {
    // SAFETY: `fd` is the live connection socket owned by this connection's tasks;
    // shutting down only its write direction has no effect on the read half the
    // reader task still uses.
    unsafe {
        libc::shutdown(fd, libc::SHUT_WR);
    }
}

/// Decode each frame off the socket and forward it to the command loop. Any read
/// error or EOF turns into a `Severed` so the command loop tears the connection
/// down. The UNIX wire has no heartbeat frame — liveness is socket EOF.
async fn reader_task(
    stream: Arc<UnixStream>,
    fd: RawFd,
    spin: Duration,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    shm: ShmCtx,
) {
    loop {
        // Before parking in read_command's await, cooperatively spin peeking the
        // socket so an imminent frame is picked up without a kernel read-wakeup.
        // yield_now (not a tight spin) keeps the loop processing other commands
        // — e.g. the reply this peer is about to send.
        spin_for_readable(fd, spin).await;
        match unix_framing::read_command(&stream, &shm.mapper, shm.client()).await {
            Ok(action) => {
                // Record the gateway client immediately, before the command loop
                // processes it: a large message can follow on this same connection
                // and the reader needs the slab fd to reconstruct it. The forwarded
                // action still drives the loop to re-propagate.
                if let ConnectionCommand::GatewayState { client } = &action {
                    shm.set_client(*client);
                }
                if loop_tx
                    .send(Command::ConnectionAction { connection, action })
                    .is_err()
                {
                    return;
                }
            }
            Err(_) => {
                sever(&loop_tx, connection, b"unix connection closed".to_vec());
                return;
            }
        }
    }
}

/// Spin up to `spin` waiting for the socket to become readable, yielding between
/// probes so the event loop keeps running. Returns as soon as data is available
/// (or EOF / the window elapses); the following `read_command` then proceeds
/// without parking when a frame is already waiting. A no-op when `spin` is zero.
async fn spin_for_readable(fd: RawFd, spin: Duration) {
    if spin.is_zero() {
        return;
    }
    let start = Instant::now();
    let mut probe = 0u8;
    loop {
        // MSG_PEEK so the byte is not consumed (read_command still reads it);
        // MSG_DONTWAIT so the probe never blocks.
        // SAFETY: `fd` is the live socket fd shared by this connection's tasks;
        // `probe` is a valid 1-byte buffer. recv only reads into it.
        let r = unsafe {
            libc::recv(
                fd,
                (&mut probe as *mut u8).cast(),
                1,
                libc::MSG_PEEK | libc::MSG_DONTWAIT,
            )
        };
        // r >= 0 means data is ready (>0) or the peer closed (0) — either way let
        // read_command run now. r < 0 is almost always EAGAIN (nothing yet).
        if r >= 0 || start.elapsed() >= spin {
            return;
        }
        tokio::task::yield_now().await;
    }
}

fn sever(loop_tx: &mpsc::UnboundedSender<Command>, connection: ConnectionRef, reason: Vec<u8>) {
    let _ = loop_tx.send(Command::ConnectionAction {
        connection,
        action: ConnectionCommand::Severed { reason },
    });
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Mutex;

    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::shm::ShmMapper;

    #[tokio::test]
    async fn writer_error_shuts_down_write_half() {
        let (writer, mut peer) = UnixStream::pair().expect("socket pair should open");
        let writer = Arc::new(writer);
        let _reader_half = Arc::clone(&writer);
        let fd = writer.as_raw_fd();
        let (tx, rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let shm = ShmCtx {
            mapper: Arc::new(Mutex::new(ShmMapper::new())),
            client: Arc::new(Mutex::new(None)),
        };
        tx.send(ConnectionCommand::Severed {
            reason: b"test".to_vec(),
        })
        .expect("command should enqueue");

        writer_task_with(writer, fd, rx, shutdown_rx, shm, |_, _, _| {
            std::future::ready(Err(io::Error::other("injected write failure")))
        })
        .await;

        let mut byte = [0u8; 1];
        assert_eq!(
            peer.read(&mut byte).await.expect("peer read should finish"),
            0,
            "peer should observe EOF after the writer fails"
        );
    }
}
