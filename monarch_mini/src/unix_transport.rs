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
//! as a `ConnectionAction`. All establishment policy (announcing our
//! identity, learning the peer's, hello, liveness) lives in the command loop and
//! is identical to inproc — this file never reads actor state or builds an
//! `Establish`. An `Establish` is just another frame on the wire.
//!
//! ## Framing
//!
//! Each frame is `[u64 header_len][header][raw part bytes...]`. The header is a
//! bincode-encoded [`WireFrame`] holding only small metadata; for a message it
//! carries the *lengths* of the parts, never their bytes. Message-part bytes are
//! written straight from the owning `MsgPart` and read straight into a freshly
//! allocated per-part buffer — no copies of payload beyond the socket read/write.
//!
//! ## Liveness
//!
//! Dropping a connection's [`UnixConnectionTransport`] drops its writer channel,
//! which ends the writer task and closes the socket's write half; the peer's
//! reader then sees EOF and emits `Severed`. That is the same "drop the transport
//! ⇒ peer severed" contract the inproc transport implements explicitly.

use std::collections::HashMap;
use std::path::PathBuf;

use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::net::unix::OwnedReadHalf;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;

use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::ctx::Command;
use crate::framing;
use crate::framing::Incoming;
use crate::matcher::Matcher;
use crate::transport::Transport;

/// Connect-retry backoff bounds. A join may be posted before its serve, so the
/// connector polls; it starts fast and backs off (doubling) to a steady poll so a
/// serve that is slow — or never comes — doesn't spam the OS with connect()s.
const CONNECT_RETRY_MIN: Duration = Duration::from_millis(5);
const CONNECT_RETRY_MAX: Duration = Duration::from_millis(1000);

/// Owns all UNIX transport state and coroutines. The command loop holds one of
/// these and forwards serves/joins to it; it never sees sockets, urls, or pairing
/// state. The loop sender and the teardown signal are captured here once.
pub(crate) struct UnixTransport {
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown_tx: watch::Sender<bool>,
    // One listener coroutine per url; serve connections are forwarded to it and
    // it owns the serve/accept pairing.
    listeners: HashMap<String, mpsc::UnboundedSender<ConnectionRef>>,
    // Liveness-token issuer: each connection's writer holds a clone for its
    // lifetime, as do the listener/connector coroutines. Teardown drops this
    // issuing copy and waits for `alive_rx` to close — i.e. for every writer to
    // have flushed and exited.
    alive_tx: Option<mpsc::UnboundedSender<()>>,
    alive_rx: mpsc::UnboundedReceiver<()>,
}

impl UnixTransport {
    pub(crate) fn new(loop_tx: mpsc::UnboundedSender<Command>) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let (alive_tx, alive_rx) = mpsc::unbounded_channel();
        Self {
            loop_tx,
            shutdown_tx,
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
    fn serve(&mut self, url: String, connection: ConnectionRef) {
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
            ));
            self.listeners.insert(url.clone(), tx);
        }
        let _ = self
            .listeners
            .get(&url)
            .expect("listener just inserted")
            .send(connection);
    }

    fn join(&mut self, url: String, connection: ConnectionRef) {
        tokio::task::spawn_local(connector_task(
            socket_path(&url),
            connection,
            self.loop_tx.clone(),
            self.shutdown_tx.subscribe(),
            self.alive_token(),
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
    mut serves: mpsc::UnboundedReceiver<ConnectionRef>,
    loop_tx: mpsc::UnboundedSender<Command>,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
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
                        Some(connection) => sever(&loop_tx, connection, reason.clone()),
                        None => return, // command loop gone
                    },
                    _ = shutdown.changed() => return,
                }
            }
        }
    };

    // Serves and accepted sockets arrive independently; the matcher pairs them up
    // regardless of order and wires up each pair as it completes.
    let mut matcher: Matcher<ConnectionRef, UnixStream> = Matcher::new();
    loop {
        tokio::select! {
            serve = serves.recv() => {
                let Some(connection) = serve else {
                    return; // command loop gone
                };
                let _ = matcher.push_left(connection, |connection, stream| {
                    spawn_connection(stream, connection, loop_tx.clone(), shutdown.clone(), alive.clone())
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
                let _ = matcher.push_right(stream, |connection, stream| {
                    spawn_connection(stream, connection, loop_tx.clone(), shutdown.clone(), alive.clone())
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
) {
    let mut retry = CONNECT_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match UnixStream::connect(&path).await {
            Ok(stream) => {
                spawn_connection(stream, connection, loop_tx, shutdown, alive);
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
/// reader (frames → `ConnectionAction`). `TransportConnected` is enqueued
/// before the reader can forward anything, so the command loop installs the
/// transport before any frame (including the peer's `Establish`) arrives.
fn spawn_connection(
    stream: UnixStream,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
    shutdown: watch::Receiver<bool>,
    alive: mpsc::UnboundedSender<()>,
) {
    let (read_half, write_half) = stream.into_split();
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();

    let transport = Box::new(UnixConnectionTransport { tx: writer_tx });
    let _ = loop_tx.send(Command::TransportConnected {
        connection,
        transport,
    });
    tokio::task::spawn_local(writer_task(write_half, writer_rx, shutdown, alive));
    tokio::task::spawn_local(reader_task(read_half, connection, loop_tx));
}

/// Transport for one end of a UNIX connection: `send` hands a command to the
/// writer task, which serializes it to the socket. Dropping it ends the writer
/// (closing the socket), which the peer observes as EOF.
struct UnixConnectionTransport {
    tx: mpsc::UnboundedSender<ConnectionCommand>,
}

impl ConnectionTransport for UnixConnectionTransport {
    fn send(&self, action: ConnectionCommand) -> bool {
        self.tx.send(action).is_ok()
    }
}

/// Write each queued command as a frame. Exits on a write error, when the
/// channel closes (transport dropped), or on the teardown signal — and in that
/// last case it first drains every command already queued so those writes reach
/// the OS before teardown completes. Holds `_alive` for its whole lifetime:
/// dropping it on exit is how teardown learns this writer has finished flushing.
async fn writer_task(
    mut write_half: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    mut shutdown: watch::Receiver<bool>,
    _alive: mpsc::UnboundedSender<()>,
) {
    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break; // transport dropped
                };
                if framing::write_command(&mut write_half, command).await.is_err() {
                    return;
                }
            }
            _ = shutdown.changed() => {
                // Teardown: the command loop has stopped, so no further commands
                // will be enqueued. Flush whatever is already queued, then stop.
                while let Ok(command) = rx.try_recv() {
                    if framing::write_command(&mut write_half, command).await.is_err() {
                        return;
                    }
                }
                break;
            }
        }
    }
    // Every queued frame has been handed to the OS; shut the write side down to
    // flush and signal EOF to the peer.
    let _ = write_half.shutdown().await;
}

/// Decode each frame off the socket and forward it to the command loop. Any read
/// error or EOF turns into a `Severed` so the command loop tears the connection
/// down. UNIX never sends heartbeats, so a `Heartbeat` frame is not expected here;
/// it is harmlessly ignored if one ever arrives.
async fn reader_task(
    mut read_half: OwnedReadHalf,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
) {
    loop {
        match framing::read_frame(&mut read_half).await {
            Ok(Incoming::Command(action)) => {
                if loop_tx
                    .send(Command::ConnectionAction { connection, action })
                    .is_err()
                {
                    return;
                }
            }
            Ok(Incoming::Heartbeat) => continue,
            Err(_) => {
                sever(&loop_tx, connection, b"unix connection closed".to_vec());
                return;
            }
        }
    }
}

fn sever(loop_tx: &mpsc::UnboundedSender<Command>, connection: ConnectionRef, reason: Vec<u8>) {
    let _ = loop_tx.send(Command::ConnectionAction {
        connection,
        action: ConnectionCommand::Severed { reason },
    });
}
