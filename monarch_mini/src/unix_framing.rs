/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Wire framing for the UNIX-socket transport.
//!
//! Deliberately **separate** from the generic [`crate::framing`] used by quic
//! (and, later, tcp): the UNIX transport is where machine-local shared-memory
//! handling lives, so its wire format grows fd-passing and shared-memory part
//! descriptors that have no business leaking into the cross-machine framing.
//! Keeping a private copy of the small set of frame types is cheaper than a
//! shared abstraction that both sides have to bend around.
//!
//! Each frame is `[u64 header_len][header][raw part bytes...]`. The header is a
//! bincode-encoded [`UnixFrame`] holding only small metadata; for a message it
//! carries the *lengths* of the parts, never their bytes. Message-part bytes are
//! written straight from the owning `MsgPart` and read straight into a freshly
//! allocated per-part buffer — no copies of payload beyond the socket read/write.
//!
//! Unlike quic, the UNIX transport relies on socket EOF for liveness and never
//! emits heartbeats, so there is no heartbeat frame and a decoded frame maps
//! straight to a [`ConnectionCommand`].

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

/// One frame on the UNIX wire. There is a variant per [`ConnectionCommand`] that
/// crosses the pipe (including `Establish`, which is how the two ends exchange
/// identity). Message-part *bytes* are never carried here — only their lengths.
#[derive(Serialize, Deserialize)]
enum UnixFrame {
    Establish {
        role: Role,
        ident: Option<Vec<u8>>,
        name_for_other: Option<Vec<u8>>,
        alive: bool,
    },
    Message {
        destination_ident: Vec<u8>,
        payload: UnixPayload,
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
}

/// Wire form of a [`SendMessage`](ConnectionCommand::SendMessage)'s payload. An
/// actor message carries only its parts' *lengths* in the header (the bytes are
/// streamed raw afterwards, zero-copy); a monitor fire carries the small dead
/// target ident inline.
#[derive(Serialize, Deserialize)]
enum UnixPayload {
    ActorMessage { part_lens: Vec<u64> },
    FireMonitor { to_monitor: Vec<u8> },
}

fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// Serialize and write one command: a length-prefixed bincode header, then — for
/// a message — each part's bytes streamed straight from its buffer. Flushes so
/// the peer sees it promptly.
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
                    UnixPayload::ActorMessage { part_lens }
                }
                SendPayload::FireMonitor(to_monitor) => UnixPayload::FireMonitor { to_monitor },
            };
            UnixFrame::Message {
                destination_ident,
                payload,
            }
        }
        ConnectionCommand::Establish {
            role,
            ident,
            name_for_other,
            alive,
        } => UnixFrame::Establish {
            role,
            ident,
            name_for_other,
            alive,
        },
        ConnectionCommand::PublishRoutes { live, dead } => UnixFrame::PublishRoutes { live, dead },
        ConnectionCommand::ToAncestor {
            to_monitor,
            payload,
        } => UnixFrame::ToAncestor {
            to_monitor,
            payload,
        },
        ConnectionCommand::Severed { reason } => UnixFrame::Severed { reason },
    };
    write_frame(writer, &frame, &parts).await
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &UnixFrame,
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

/// Read one frame and map it straight back to a [`ConnectionCommand`]. For a
/// message the part bytes are read directly into the command's own buffers — the
/// only copy is the unavoidable kernel-to-userspace one.
pub(crate) async fn read_command<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<ConnectionCommand> {
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf).await?;
    let header_len = u64::from_le_bytes(len_buf) as usize;

    let mut header = vec![0u8; header_len];
    reader.read_exact(&mut header).await?;
    let (frame, _) = bincode::serde::decode_from_slice::<UnixFrame, _>(&header, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;

    Ok(match frame {
        UnixFrame::Message {
            destination_ident,
            payload,
        } => {
            let payload = match payload {
                UnixPayload::ActorMessage { part_lens } => {
                    let mut parts = Vec::with_capacity(part_lens.len());
                    for len in part_lens {
                        let mut buf = vec![0u8; len as usize];
                        reader.read_exact(&mut buf).await?;
                        parts.push(MsgPart::from_bytes(buf));
                    }
                    SendPayload::ActorMessage(parts)
                }
                UnixPayload::FireMonitor { to_monitor } => SendPayload::FireMonitor(to_monitor),
            };
            ConnectionCommand::SendMessage {
                destination_ident,
                payload,
            }
        }
        UnixFrame::Establish {
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
        UnixFrame::PublishRoutes { live, dead } => ConnectionCommand::PublishRoutes { live, dead },
        UnixFrame::ToAncestor {
            to_monitor,
            payload,
        } => ConnectionCommand::ToAncestor {
            to_monitor,
            payload,
        },
        UnixFrame::Severed { reason } => ConnectionCommand::Severed { reason },
    })
}
