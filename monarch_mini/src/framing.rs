/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Wire framing for the QUIC transport (and, later, tcp). The UNIX transport has
//! its own framing (see [`crate::unix_framing`]) so machine-local shared-memory
//! and fd-passing concerns never leak into the cross-machine wire format.
//!
//! Each frame is `[u64 header_len][header][raw part bytes...]`. The header is a
//! bincode-encoded [`WireFrame`] holding only small metadata; for a message it
//! carries the *lengths* of the parts, never their bytes. Message-part bytes are
//! written straight from the owning `MsgPart`.
//!
//! On the read side a small part is read into a freshly allocated owned buffer.
//! A *large* part, when the receiving actor has learned its gateway's
//! [`ShmClient`] (every actor on a quic link is a gateway, so it has one), is read
//! **straight into a freshly allocated shared-memory slab block** instead: the
//! reader allocates the block, maps it, and reads the stream bytes directly into
//! the mapping, yielding a [`MsgPart::Shm`]. The payload then never touches an
//! owned heap buffer — and if the message is later forwarded across a machine-local
//! unix hop, that hop relays the slab descriptor by reference rather than copying
//! the bytes into shared memory a second time. Either way the only copy is the
//! unavoidable kernel-to-userspace one.
//!
//! The reader/writer are generic over tokio's [`AsyncRead`]/[`AsyncWrite`], so the
//! same code drives a QUIC stream's `RecvStream`/`SendStream`. Liveness policy
//! (heartbeats, timeouts, EOF) lives in each
//! transport; this module only serializes frames — with the one exception of the
//! [`WireFrame::Heartbeat`] probe, which a transport may emit/consume but which is
//! never a [`ConnectionCommand`].

use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::Role;
use crate::connection::AncestorPayload;
use crate::connection::ConnectionCommand;
use crate::connection::SendPayload;
use crate::msg::MsgPart;
use crate::shm::MapperHandle;
use crate::shm::SHM_THRESHOLD;
use crate::shm::ShmClient;

/// One frame on the wire. There is a variant per [`ConnectionCommand`] that
/// crosses the pipe (including `Establish`, which is how the two ends exchange
/// identity), plus a transport-internal `Heartbeat`. Message-part *bytes* are
/// never carried here — only their lengths.
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
        payload: WirePayload,
    },
    PublishRoutes {
        live: Vec<Vec<u8>>,
        dead: Vec<Vec<u8>>,
    },
    ToAncestor {
        to_monitor: Vec<u8>,
        payload: AncestorPayload,
    },
    Severed {
        reason: Vec<u8>,
    },
    /// Transport-internal liveness probe; consumed by the reader to refresh its
    /// deadline and never surfaced as a [`ConnectionCommand`].
    Heartbeat,
}

/// Wire form of a [`SendMessage`](ConnectionCommand::SendMessage)'s payload. An
/// actor message carries only its parts' *lengths* in the header (the bytes are
/// streamed raw afterwards, zero-copy); a monitor fire carries the small dead
/// target ident inline.
#[derive(Serialize, Deserialize)]
enum WirePayload {
    ActorMessage { part_lens: Vec<u64> },
    FireMonitor { to_monitor: Vec<u8> },
}

fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// A frame decoded off the wire: either a command for the loop, or a transport
/// heartbeat (consumed internally to refresh the liveness deadline).
pub(crate) enum Incoming {
    Command(ConnectionCommand),
    Heartbeat,
}

/// Serialize and write one command: a length-prefixed bincode header, then — for
/// a message — each part's bytes streamed straight from its buffer. Flushes so the
/// peer (and, for quic, its heartbeat deadline) sees it promptly.
pub(crate) async fn write_command<W: AsyncWrite + Unpin>(
    writer: &mut W,
    command: ConnectionCommand,
) -> std::io::Result<()> {
    // A message additionally keeps a list of part bytes to stream raw after the
    // header; every other command is header-only.
    let mut parts: Vec<MsgPart> = Vec::new();
    let frame = match command {
        ConnectionCommand::SendMessage {
            destination_ident,
            payload,
        } => {
            let payload = match payload {
                SendPayload::ActorMessage(message_parts) => {
                    let part_lens = message_parts
                        .iter()
                        .map(|part| part.as_bytes().len() as u64)
                        .collect();
                    parts = message_parts;
                    WirePayload::ActorMessage { part_lens }
                }
                SendPayload::FireMonitor(to_monitor) => WirePayload::FireMonitor { to_monitor },
            };
            WireFrame::Message {
                destination_ident,
                payload,
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
        ConnectionCommand::PublishRoutes { live, dead } => WireFrame::PublishRoutes { live, dead },
        ConnectionCommand::ToAncestor {
            to_monitor,
            payload,
        } => WireFrame::ToAncestor {
            to_monitor,
            payload,
        },
        ConnectionCommand::Severed { reason } => WireFrame::Severed { reason },
        // Shared memory is machine-local, so quic drops gateway state rather than
        // forwarding it: skip without writing anything to the wire.
        ConnectionCommand::GatewayState { .. } => return Ok(()),
    };
    write_frame(writer, &frame, &parts).await
}

/// Write a transport heartbeat probe.
pub(crate) async fn write_heartbeat<W: AsyncWrite + Unpin>(writer: &mut W) -> std::io::Result<()> {
    write_frame(writer, &WireFrame::Heartbeat, &[]).await
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &WireFrame,
    parts: &[MsgPart],
) -> std::io::Result<()> {
    let header = bincode::serde::encode_to_vec(frame, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    writer
        .write_all(&(header.len() as u64).to_le_bytes())
        .await?;
    writer.write_all(&header).await?;
    // Write each part's bytes straight from its owning buffer — no copy.
    for part in parts {
        writer.write_all(part.as_bytes()).await?;
    }
    writer.flush().await
}

/// Read one frame. For a message each part is read directly into its destination
/// buffer — an owned heap buffer for a small part, or (when `client` is present
/// and the part is large) a freshly allocated shared-memory slab block, so the
/// only copy is the unavoidable kernel-to-userspace one. `mapper`/`client` are the
/// receiving actor's shared-memory context (see [`read_part`]). A `Heartbeat`
/// frame returns [`Incoming::Heartbeat`]; everything else maps straight back to a
/// [`ConnectionCommand`].
pub(crate) async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    mapper: &MapperHandle,
    client: Option<ShmClient>,
) -> std::io::Result<Incoming> {
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf).await?;
    let header_len = u64::from_le_bytes(len_buf) as usize;

    let mut header = vec![0u8; header_len];
    reader.read_exact(&mut header).await?;
    let (frame, _) = bincode::serde::decode_from_slice::<WireFrame, _>(&header, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;

    Ok(match frame {
        WireFrame::Heartbeat => Incoming::Heartbeat,
        WireFrame::Message {
            destination_ident,
            payload,
        } => {
            let payload = match payload {
                WirePayload::ActorMessage { part_lens } => {
                    let mut parts = Vec::with_capacity(part_lens.len());
                    for len in part_lens {
                        parts.push(read_part(reader, len, mapper, client).await?);
                    }
                    SendPayload::ActorMessage(parts)
                }
                WirePayload::FireMonitor { to_monitor } => SendPayload::FireMonitor(to_monitor),
            };
            Incoming::Command(ConnectionCommand::SendMessage {
                destination_ident,
                payload,
            })
        }
        WireFrame::Establish {
            role,
            ident,
            name_for_other,
            alive,
        } => Incoming::Command(ConnectionCommand::Establish {
            role,
            ident,
            name_for_other,
            alive,
        }),
        WireFrame::PublishRoutes { live, dead } => {
            Incoming::Command(ConnectionCommand::PublishRoutes { live, dead })
        }
        WireFrame::ToAncestor {
            to_monitor,
            payload,
        } => Incoming::Command(ConnectionCommand::ToAncestor {
            to_monitor,
            payload,
        }),
        WireFrame::Severed { reason } => Incoming::Command(ConnectionCommand::Severed { reason }),
    })
}

/// Read one message part of `len` bytes off the stream.
///
/// A part `>= SHM_THRESHOLD` on a connection whose actor has learned its gateway
/// [`ShmClient`] is read straight into a freshly allocated slab block: allocate the
/// block, map it through the context `mapper`, and read the stream bytes directly
/// into the mapping — yielding a [`MsgPart::Shm`] that a later unix hop forwards by
/// descriptor without copying into shared memory again. Everything else (no client,
/// or a small part) is read into an owned heap buffer as before.
async fn read_part<R: AsyncRead + Unpin>(
    reader: &mut R,
    len: u64,
    mapper: &MapperHandle,
    client: Option<ShmClient>,
) -> std::io::Result<MsgPart> {
    let Some(client) = client.filter(|_| len >= SHM_THRESHOLD) else {
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).await?;
        return Ok(MsgPart::from_bytes(buf));
    };

    let (offset, token) = client.allocate(len).await?;
    let dst = {
        let mut mapper = mapper.lock().expect("shm mapper mutex poisoned");
        // SAFETY: `offset` was just granted for `len` bytes against `client`'s slab,
        // so the file is grown to cover it and the mapper can map the range.
        unsafe { mapper.map(client.slab_fd(), offset, len as usize)? }
    };
    // SAFETY: `dst` points at a writable mapping of exactly `len` bytes that stays
    // valid for the mapper's (context's) lifetime — well past this read — so the
    // slice is sound to fill across the await. The mapper lock was released above,
    // so it is not held across IO. `len >= SHM_THRESHOLD > 0`, so this is non-empty.
    let buf = unsafe { std::slice::from_raw_parts_mut(dst, len as usize) };
    reader.read_exact(buf).await?;

    Ok(MsgPart::new_shm(
        mapper.clone(),
        client.slab_fd(),
        token,
        offset,
        len,
    ))
}
