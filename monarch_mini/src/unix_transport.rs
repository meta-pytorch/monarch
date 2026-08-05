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
//! as a `ConnectionSentCommand`. All establishment policy (announcing our
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

use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::net::unix::OwnedReadHalf;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;

use crate::Role;
use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::ctx::Command;
use crate::matcher::Matcher;
use crate::msg::MsgPart;
use crate::transport::Transport;

/// Connect-retry backoff bounds. A join may be posted before its serve, so the
/// connector polls; it starts fast and backs off (doubling) to a steady poll so a
/// serve that is slow — or never comes — doesn't spam the OS with connect()s.
const CONNECT_RETRY_MIN: Duration = Duration::from_millis(5);
const CONNECT_RETRY_MAX: Duration = Duration::from_millis(1000);

/// One frame on the wire. There is a variant per [`ConnectionCommand`] that
/// crosses the pipe (including `Establish`, which is how the two ends exchange
/// identity). Message-part *bytes* are never carried here — only their lengths.
#[derive(Serialize, Deserialize)]
enum WireFrame {
    Establish {
        role: Role,
        ident: Option<Vec<u8>>,
        name_for_other: Option<Vec<u8>>,
        alive: bool,
    },
    Message {
        destination_ident: Vec<u8>,
        part_lens: Vec<u64>,
    },
    PublishRoutes {
        actor_idents: Vec<Vec<u8>>,
    },
    Severed {
        reason: Vec<u8>,
    },
}

fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

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
/// reader (frames → `ConnectionSentCommand`). `TransportConnected` is enqueued
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
                if write_command(&mut write_half, command).await.is_err() {
                    return;
                }
            }
            _ = shutdown.changed() => {
                // Teardown: the command loop has stopped, so no further commands
                // will be enqueued. Flush whatever is already queued, then stop.
                while let Ok(command) = rx.try_recv() {
                    if write_command(&mut write_half, command).await.is_err() {
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
/// down. The command loop's command handling is shared with inproc and unchanged.
async fn reader_task(
    mut read_half: OwnedReadHalf,
    connection: ConnectionRef,
    loop_tx: mpsc::UnboundedSender<Command>,
) {
    loop {
        match read_command(&mut read_half).await {
            Ok(action) => {
                if loop_tx
                    .send(Command::ConnectionSentCommand { connection, action })
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

fn sever(loop_tx: &mpsc::UnboundedSender<Command>, connection: ConnectionRef, reason: Vec<u8>) {
    let _ = loop_tx.send(Command::ConnectionSentCommand {
        connection,
        action: ConnectionCommand::Severed { reason },
    });
}

/// Serialize one command and write it: a length-prefixed bincode header, then —
/// for a message — each part's bytes streamed straight from its buffer. Every
/// command crosses the wire, `Establish` included.
async fn write_command(
    write_half: &mut OwnedWriteHalf,
    command: ConnectionCommand,
) -> std::io::Result<()> {
    // Figure out the header frame to write; a message additionally keeps a list of
    // part bytes to stream raw after the header.
    let mut parts: Vec<MsgPart> = Vec::new();
    let frame = match command {
        ConnectionCommand::SendMessage {
            destination_ident,
            parts: message_parts,
        } => {
            let part_lens = message_parts
                .iter()
                .map(|part| part.as_bytes().len() as u64)
                .collect();
            parts = message_parts;
            WireFrame::Message {
                destination_ident,
                part_lens,
            }
        }
        ConnectionCommand::Establish {
            role,
            ident,
            name_for_other,
            alive,
        } => WireFrame::Establish {
            role,
            ident,
            name_for_other,
            alive,
        },
        ConnectionCommand::PublishRoutes { actor_idents } => {
            WireFrame::PublishRoutes { actor_idents }
        }
        ConnectionCommand::Severed { reason } => WireFrame::Severed { reason },
    };

    let header = bincode::serde::encode_to_vec(&frame, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    write_half
        .write_all(&(header.len() as u64).to_le_bytes())
        .await?;
    write_half.write_all(&header).await?;
    // Write each part's bytes straight from its owning buffer — no copy.
    for part in parts {
        write_half.write_all(part.as_bytes()).await?;
    }
    Ok(())
}

/// Read one frame and decode it straight into a [`ConnectionCommand`]. For a
/// message the part bytes are read directly into the command's own buffers — the
/// only copy is the unavoidable kernel-to-userspace one, and there is no
/// intermediate `WireFrame`-plus-parts to re-match.
async fn read_command(read_half: &mut OwnedReadHalf) -> std::io::Result<ConnectionCommand> {
    let mut len_buf = [0u8; 8];
    read_half.read_exact(&mut len_buf).await?;
    let header_len = u64::from_le_bytes(len_buf) as usize;

    let mut header = vec![0u8; header_len];
    read_half.read_exact(&mut header).await?;
    let (frame, _) = bincode::serde::decode_from_slice::<WireFrame, _>(&header, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;

    Ok(match frame {
        WireFrame::Message {
            destination_ident,
            part_lens,
        } => {
            let mut parts = Vec::with_capacity(part_lens.len());
            for len in part_lens {
                let mut buf = vec![0u8; len as usize];
                read_half.read_exact(&mut buf).await?;
                parts.push(MsgPart::from_bytes(buf));
            }
            ConnectionCommand::SendMessage {
                destination_ident,
                parts,
            }
        }
        WireFrame::Establish {
            role,
            ident,
            name_for_other,
            alive,
        } => ConnectionCommand::Establish {
            role,
            ident,
            name_for_other,
            alive,
        },
        WireFrame::PublishRoutes { actor_idents } => {
            ConnectionCommand::PublishRoutes { actor_idents }
        }
        WireFrame::Severed { reason } => ConnectionCommand::Severed { reason },
    })
}
