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
use crate::connection::ConnectionCommand;
use crate::connection::MonitorOp;
use crate::connection::SendPayload;
use crate::connection::SideChannelAction;
use crate::connection::SideChannelMessage;
use crate::msg::MsgPart;
use crate::quic_heartbeat::BeatKind;
use crate::quic_heartbeat::ConnectionId;
use crate::quic_heartbeat::Heartbeat;
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
    UpdateMonitorSubscription {
        listener: Vec<u8>,
        target: Vec<u8>,
        op: MonitorOp,
    },
    Severed {
        reason: Vec<u8>,
    },
    PublishGatewayRoutes {
        live: Vec<Vec<u8>>,
    },
    GatewayDied {
        dead: Vec<Vec<u8>>,
    },
    /// Transport-internal liveness probe carrying a directional [`Heartbeat`] body
    /// (a Child sends `FromChild`, a Parent sends `FromParent`). Consumed by the
    /// reader/heartbeat coroutine and never surfaced as a [`ConnectionCommand`].
    Heartbeat(Heartbeat),
}

/// Wire form of a [`SendMessage`](ConnectionCommand::SendMessage)'s payload. An
/// actor message carries only its parts' *lengths* in the header (the bytes are
/// streamed raw afterwards, zero-copy); a monitor fire carries the small dead
/// target ident inline.
#[derive(Serialize, Deserialize)]
enum WirePayload {
    ActorMessage {
        part_lens: Vec<u64>,
    },
    FireMonitor {
        to_monitor: Vec<u8>,
        is_timeout: bool,
    },
}

/// One frame on a gateway side-channel — the wire form of a [`SideChannelMessage`].
/// `Send` of an actor message reuses [`WirePayload`]'s zero-copy part-length header
/// + streamed bytes (+ shm); every other case is header-only.
#[derive(Serialize, Deserialize)]
enum SideChannelFrame {
    Send {
        gateway_for_actor: Vec<u8>,
        payload: WirePayload,
    },
    UpdateRemoteMonitorState {
        gateway_for_actor: Vec<u8>,
        listener: Vec<u8>,
        op: MonitorOp,
    },
    AckRemoteMonitor {
        gateway_for_actor: Vec<u8>,
        monitoring: Vec<u8>,
    },
    /// A sibling side-channel heartbeat message (see [`crate::quic_heartbeat`]).
    /// `gateway_for_actor` is the recipient (whose gateway tag the message is dialed
    /// at); `from` is the sender's ident; `conn_id` is the delegated connection's id;
    /// `kind` is the [`BeatKind`] (beat / ack / release).
    Heartbeat {
        gateway_for_actor: Vec<u8>,
        from: Vec<u8>,
        conn_id: ConnectionId,
        kind: BeatKind,
    },
}

fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// The first frame a QUIC *joiner* (the connecting side) writes, declaring what
/// the freshly opened bi-stream is for. The accepting side reads exactly one of
/// these before deciding how to drive the stream:
///
/// - `Join` — an ordinary parent/child connection; the command loop takes over
///   (identity exchange, hello, heartbeated liveness) exactly as before.
/// - `SideChannel` — a gateway-to-gateway message channel: the accepting gateway
///   just reads [`WireFrame::Message`] frames and routes them locally. It is
///   never paired with a serve, never establishes a parent/child link, and is
///   never heartbeated.
///
/// Only the joiner writes a preamble (one direction); the server never does, so
/// the joiner's own reader keeps reading [`WireFrame`]s as before.
#[derive(Serialize, Deserialize)]
pub(crate) enum Preamble {
    Join,
    SideChannel,
}

/// Write the connection preamble (see [`Preamble`]). Length-prefixed bincode,
/// the same scheme as [`write_frame`], so a reader can decode it with
/// [`read_preamble`] before switching to the per-frame loop.
pub(crate) async fn write_preamble<W: AsyncWrite + Unpin>(
    writer: &mut W,
    preamble: Preamble,
) -> std::io::Result<()> {
    let header = bincode::serde::encode_to_vec(&preamble, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    writer
        .write_all(&(header.len() as u64).to_le_bytes())
        .await?;
    writer.write_all(&header).await?;
    writer.flush().await
}

/// Read the connection preamble written by [`write_preamble`].
pub(crate) async fn read_preamble<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Preamble> {
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf).await?;
    let header_len = u64::from_le_bytes(len_buf) as usize;
    let mut header = vec![0u8; header_len];
    reader.read_exact(&mut header).await?;
    let (preamble, _) = bincode::serde::decode_from_slice::<Preamble, _>(&header, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    Ok(preamble)
}

/// A frame decoded off the wire: either a command for the loop, or a transport
/// heartbeat (consumed internally to refresh the liveness deadline).
pub(crate) enum Incoming {
    Command(ConnectionCommand),
    Heartbeat(Heartbeat),
}

/// A frame decoded off a gateway side-channel: either a routable
/// [`SideChannelMessage`] for the command loop, or a transport-internal
/// delegated-heartbeat beat/ack. The heartbeat variant is *never* surfaced to ctx
/// — the receiving quic reader hands it straight to the heartbeat subsystem — so it
/// stays out of ctx's [`SideChannelMessage`]/`SideChannelAction` routing types.
pub(crate) enum SideChannelIncoming {
    Message(SideChannelMessage),
    Heartbeat {
        /// The actor whose gateway received the beat — i.e. the one the addressed
        /// heartbeat coroutine belongs to as a child.
        recipient: Vec<u8>,
        from: Vec<u8>,
        conn_id: ConnectionId,
        kind: BeatKind,
    },
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
                SendPayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                } => WirePayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                },
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
        ConnectionCommand::UpdateMonitorSubscription {
            listener,
            target,
            op,
        } => WireFrame::UpdateMonitorSubscription {
            listener,
            target,
            op,
        },
        ConnectionCommand::Severed { reason } => WireFrame::Severed { reason },
        ConnectionCommand::PublishGatewayRoutes { live } => {
            WireFrame::PublishGatewayRoutes { live }
        }
        ConnectionCommand::GatewayDied { dead } => WireFrame::GatewayDied { dead },
        // Shared memory is machine-local, so quic drops gateway state rather than
        // forwarding it: skip without writing anything to the wire.
        ConnectionCommand::GatewayState { .. } => return Ok(()),
    };
    write_frame(writer, &frame, &parts).await
}

/// Write a transport heartbeat probe carrying `heartbeat`.
pub(crate) async fn write_heartbeat<W: AsyncWrite + Unpin>(
    writer: &mut W,
    heartbeat: Heartbeat,
) -> std::io::Result<()> {
    write_frame(writer, &WireFrame::Heartbeat(heartbeat), &[]).await
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &impl Serialize,
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
        WireFrame::Heartbeat(heartbeat) => Incoming::Heartbeat(heartbeat),
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
                WirePayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                } => SendPayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                },
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
        WireFrame::UpdateMonitorSubscription {
            listener,
            target,
            op,
        } => Incoming::Command(ConnectionCommand::UpdateMonitorSubscription {
            listener,
            target,
            op,
        }),
        WireFrame::Severed { reason } => Incoming::Command(ConnectionCommand::Severed { reason }),
        WireFrame::PublishGatewayRoutes { live } => {
            Incoming::Command(ConnectionCommand::PublishGatewayRoutes { live })
        }
        WireFrame::GatewayDied { dead } => {
            Incoming::Command(ConnectionCommand::GatewayDied { dead })
        }
    })
}

/// Serialize and write one [`SideChannelMessage`]: a length-prefixed bincode
/// header, then — for a `Send` of an actor message — each part's bytes streamed
/// raw after it (zero-copy, same scheme as [`write_command`]). Every other action
/// is header-only.
pub(crate) async fn write_side_channel<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: SideChannelMessage,
) -> std::io::Result<()> {
    let SideChannelMessage {
        gateway_for_actor,
        action,
    } = message;
    let mut parts: Vec<MsgPart> = Vec::new();
    let frame = match action {
        SideChannelAction::Send(payload) => {
            let payload = match payload {
                SendPayload::ActorMessage(message_parts) => {
                    let part_lens = message_parts
                        .iter()
                        .map(|part| part.as_bytes().len() as u64)
                        .collect();
                    parts = message_parts;
                    WirePayload::ActorMessage { part_lens }
                }
                SendPayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                } => WirePayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                },
            };
            SideChannelFrame::Send {
                gateway_for_actor,
                payload,
            }
        }
        SideChannelAction::UpdateRemoteMonitorState { listener, op } => {
            SideChannelFrame::UpdateRemoteMonitorState {
                gateway_for_actor,
                listener,
                op,
            }
        }
        SideChannelAction::AckRemoteMonitor { monitoring } => SideChannelFrame::AckRemoteMonitor {
            gateway_for_actor,
            monitoring,
        },
    };
    write_frame(writer, &frame, &parts).await
}

/// Write a sibling side-channel heartbeat message as a frame. Header-only; the
/// transport builds the wire frame directly, so heartbeats never pass through
/// ctx's [`SideChannelMessage`] routing types.
pub(crate) async fn write_side_channel_heartbeat<W: AsyncWrite + Unpin>(
    writer: &mut W,
    recipient: Vec<u8>,
    from: Vec<u8>,
    conn_id: ConnectionId,
    kind: BeatKind,
) -> std::io::Result<()> {
    let frame = SideChannelFrame::Heartbeat {
        gateway_for_actor: recipient,
        from,
        conn_id,
        kind,
    };
    write_frame(writer, &frame, &[]).await
}

/// Read one [`SideChannelMessage`] off a gateway side-channel. A `Send` of an
/// actor message reads each part exactly like [`read_frame`] (small parts into
/// owned buffers, large ones into the slab via `mapper`/`client`); other actions
/// are header-only.
pub(crate) async fn read_side_channel<R: AsyncRead + Unpin>(
    reader: &mut R,
    mapper: &MapperHandle,
    client: Option<ShmClient>,
) -> std::io::Result<SideChannelIncoming> {
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf).await?;
    let header_len = u64::from_le_bytes(len_buf) as usize;

    let mut header = vec![0u8; header_len];
    reader.read_exact(&mut header).await?;
    let (frame, _) =
        bincode::serde::decode_from_slice::<SideChannelFrame, _>(&header, bincode_config())
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;

    // A heartbeat is transport-internal: return it as its own variant so the reader
    // routes it into the heartbeat subsystem without ever building a ctx-level
    // SideChannelMessage.
    if let SideChannelFrame::Heartbeat {
        gateway_for_actor,
        from,
        conn_id,
        kind,
    } = frame
    {
        return Ok(SideChannelIncoming::Heartbeat {
            recipient: gateway_for_actor,
            from,
            conn_id,
            kind,
        });
    }

    let message = match frame {
        SideChannelFrame::Send {
            gateway_for_actor,
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
                WirePayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                } => SendPayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                },
            };
            SideChannelMessage {
                gateway_for_actor,
                action: SideChannelAction::Send(payload),
            }
        }
        SideChannelFrame::UpdateRemoteMonitorState {
            gateway_for_actor,
            listener,
            op,
        } => SideChannelMessage {
            gateway_for_actor,
            action: SideChannelAction::UpdateRemoteMonitorState { listener, op },
        },
        SideChannelFrame::AckRemoteMonitor {
            gateway_for_actor,
            monitoring,
        } => SideChannelMessage {
            gateway_for_actor,
            action: SideChannelAction::AckRemoteMonitor { monitoring },
        },
        SideChannelFrame::Heartbeat { .. } => unreachable!("heartbeat handled above"),
    };
    Ok(SideChannelIncoming::Message(message))
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
