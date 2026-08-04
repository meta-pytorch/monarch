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
//! A frame is a length-prefixed bincode header ([`WireFrame`]) holding only small
//! routing/control metadata — never part bytes. An actor message's routing header is
//! followed by a per-part plan ([`WirePart`], each part inline or striped) and then the
//! inline parts' bytes: a small part streams inline right after the plan (from the owning
//! `MsgPart`), a large part is **striped** across the connection's data streams and named
//! only by its per-shard lengths (see [`crate::dataio`]).
//!
//! This module owns the header/plan wire format and streams *inline* part bytes; the
//! parts themselves go through the [`PartReader`]/[`PartWriter`] the read/write functions
//! are handed (per part), so the shared-memory and striping concerns — a large part read
//! straight into a slab block, or striped across data streams — never involve framing.
//!
//! The reader/writer are generic over tokio's [`AsyncRead`]/[`AsyncWrite`], so the
//! same code drives a QUIC stream's `RecvStream`/`SendStream`. Liveness policy
//! (heartbeats, timeouts, EOF) lives in each transport; this module only serializes
//! frames — data-stream [`WireFrame`]s, and the bare [`Heartbeat`] probe that rides
//! its own stream (see [`write_heartbeat`]), which is never a [`ConnectionCommand`].

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
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
use crate::dataio::PartReader;
use crate::dataio::PartWriter;
use crate::heartbeat::BeatKind;
use crate::heartbeat::ConnectionId;
use crate::heartbeat::Heartbeat;
use crate::net::Net;

/// One frame header on the data stream. There is a variant per [`ConnectionCommand`]
/// that crosses the pipe (including `Establish`, which is how the two ends exchange
/// identity). A header is routing/control only — it carries no message-part bytes; an
/// [`ActorMessage`](WireFrame::ActorMessage)'s parts follow it via
/// [`PartWriter::write_message_parts`]/[`PartReader::read_message_parts`]. Heartbeats are *not* here: they ride
/// a dedicated stream, serialized as a bare [`Heartbeat`] (see
/// [`write_heartbeat`]/[`read_heartbeat`]).
#[derive(Serialize, Deserialize)]
enum WireFrame {
    Establish {
        role: Role,
        ident: Option<Vec<u8>>,
        name_for_other: Option<Vec<u8>>,
        alive: bool,
    },
    /// An actor message: this header carries only the destination; the parts follow.
    ActorMessage {
        destination_ident: Vec<u8>,
    },
    /// A monitor fire — the only other [`SendMessage`](ConnectionCommand::SendMessage);
    /// it has no parts, so it is a plain header (kept separate from `ActorMessage` so
    /// only the message path deals with parts).
    FireMonitorMessage {
        destination_ident: Vec<u8>,
        to_monitor: Vec<u8>,
        is_timeout: bool,
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
}

/// How one message part is carried on the wire. A small part is streamed **inline**
/// on the control stream right after the header (zero-copy, as always). A large part
/// is **striped**: its bytes travel out-of-band on the connection's data streams (see
/// [`crate::dataio`]), and the header carries only the per-shard byte lengths — one
/// per data stream — which fully describe the split so the data streams themselves
/// stay unframed. The sender alone decides inline-vs-striped (from its size heuristic
/// and stream count), so the receiver never has to agree on a threshold: it just
/// reads whichever form the descriptor names.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum WirePart {
    Inline { len: u64 },
    Striped { shard_lens: Vec<u64> },
}

/// One frame header on a gateway side-channel's *message* stream — the wire form of a
/// [`SideChannelMessage`], mirroring [`WireFrame`]. A `Send` of an actor message
/// (`SendActorMessage`) is a routing-only header whose parts follow via
/// [`PartWriter::write_message_parts`]/[`PartReader::read_message_parts`], exactly like a connection message;
/// every other case is header-only. Delegated heartbeats are not here — they ride the
/// companion heartbeat stream as a bare [`SideChannelHeartbeat`].
#[derive(Serialize, Deserialize)]
enum SideChannelFrame {
    /// A `Send` of an actor message: this header carries only the gateway; parts follow.
    SendActorMessage { gateway_for_actor: Vec<u8> },
    /// A `Send` of a monitor fire — no parts, so a plain header.
    SendFireMonitor {
        gateway_for_actor: Vec<u8>,
        to_monitor: Vec<u8>,
        is_timeout: bool,
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
}

fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// Encode `value` as bincode and write it length-prefixed as `[u64 header_len]
/// [header]`. Every framed write goes through this for its header — the frame header,
/// and (for a message) the per-part plan written by [`PartWriter::write_message_parts`]. Callers
/// layer their own concerns on top: trailing inline part bytes and the flush (every
/// caller decides when to flush; this helper never does). The length prefix is
/// little-endian, matching [`read_header`].
pub(crate) async fn write_header<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> std::io::Result<()> {
    let header = bincode::serde::encode_to_vec(value, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    writer
        .write_all(&(header.len() as u64).to_le_bytes())
        .await?;
    writer.write_all(&header).await
}

/// Read a `[u64 header_len][header]`-framed bincode value written by
/// [`write_header`] and decode it as `T`. The single place every framed read goes
/// through for its header; callers that carry trailing message-part bytes read them
/// after this (see [`read_frame`]).
pub(crate) async fn read_header<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
) -> std::io::Result<T> {
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf).await?;
    let header_len = u64::from_le_bytes(len_buf) as usize;
    let mut header = vec![0u8; header_len];
    reader.read_exact(&mut header).await?;
    let (value, _) = bincode::serde::decode_from_slice::<T, _>(&header, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    Ok(value)
}

/// The first frame a QUIC *joiner* (the connecting side) writes on the **first**
/// bi-stream it opens, declaring what kind of connection this is. Every connection
/// carries two bi-streams — a data/message stream and a companion heartbeat stream
/// — so heartbeats never queue behind a large data transfer on a single ordered
/// stream. Only the first stream carries a preamble: the second is *always* the
/// heartbeat stream (opened right after the first), so the acceptor classifies it
/// by order and reads no preamble on it.
///
/// - `Join` — an ordinary parent/child connection; the command loop takes over
///   (identity exchange, hello, routing) over the data stream.
/// - `SideChannel` — a gateway-to-gateway channel: the accepting gateway reads
///   [`SideChannelFrame`] frames and routes them locally. Never paired with a
///   serve, never establishes a parent/child link.
///
/// Only the joiner writes the preamble (one direction); the server never does, so
/// the joiner's own readers keep reading frames as before.
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
    write_header(writer, &preamble).await?;
    writer.flush().await
}

/// Read the connection preamble written by [`write_preamble`].
pub(crate) async fn read_preamble<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Preamble> {
    read_header(reader).await
}

/// A delegated-heartbeat beat/ack/release carried on a gateway side-channel's
/// dedicated heartbeat stream. It is the bare wire form on that stream (which
/// carries only these), and is *never* surfaced to ctx — the receiving quic reader
/// hands it straight to the heartbeat subsystem — so it stays out of ctx's
/// [`SideChannelMessage`]/`SideChannelAction` routing types.
#[derive(Serialize, Deserialize)]
pub(crate) struct SideChannelHeartbeat {
    /// The actor whose gateway received the beat — i.e. the one the addressed
    /// heartbeat coroutine belongs to as a child.
    pub(crate) recipient: Vec<u8>,
    pub(crate) from: Vec<u8>,
    pub(crate) conn_id: ConnectionId,
    pub(crate) kind: BeatKind,
}

/// Serialize and write one command. An actor message writes a routing-only header then
/// its parts via [`PartWriter::write_message_parts`] (planned and, if large, striped by
/// `part_writer`); every other command is a header-only frame. Flushes so the peer (and,
/// for quic, its heartbeat deadline) sees it promptly.
pub(crate) async fn write_command<W, N>(
    writer: &mut W,
    command: ConnectionCommand,
    part_writer: &mut PartWriter<N>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    N: Net,
{
    let frame = match command {
        ConnectionCommand::SendMessage {
            destination_ident,
            payload: SendPayload::ActorMessage(parts),
        } => {
            // Routing-only header, then the parts (plan + bytes) via the part writer.
            write_header(writer, &WireFrame::ActorMessage { destination_ident }).await?;
            return part_writer.write_message_parts(writer, parts).await;
        }
        ConnectionCommand::SendMessage {
            destination_ident,
            payload:
                SendPayload::FireMonitor {
                    to_monitor,
                    is_timeout,
                },
        } => WireFrame::FireMonitorMessage {
            destination_ident,
            to_monitor,
            is_timeout,
        },
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
    write_frame(writer, &frame).await
}

/// Write a transport heartbeat probe carrying `heartbeat`. The dedicated heartbeat
/// stream carries only these, so it is serialized as a bare [`Heartbeat`] (no
/// enclosing frame variant) and has no message parts.
pub(crate) async fn write_heartbeat<W: AsyncWrite + Unpin>(
    writer: &mut W,
    heartbeat: Heartbeat,
) -> std::io::Result<()> {
    write_frame(writer, &heartbeat).await
}

/// Read one heartbeat probe off a connection's dedicated heartbeat stream, written
/// by [`write_heartbeat`] as a bare [`Heartbeat`] (no message parts, so no
/// shared-memory context is needed).
pub(crate) async fn read_heartbeat<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Heartbeat> {
    read_header(reader).await
}

/// Write a header-only frame: the length-prefixed bincode header, then flush. An actor
/// message instead writes its routing header and then its parts via
/// [`PartWriter::write_message_parts`]; every command/action that reaches here is header-only.
async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &impl Serialize,
) -> std::io::Result<()> {
    write_header(writer, frame).await?;
    writer.flush().await
}

/// Read and decode one control-stream frame into its [`ConnectionCommand`]. An actor
/// message's routing header comes back here; its parts are read through `part_reader`
/// (which owns the shared memory and striping), so those concerns stay out of framing. A
/// striped part's [`MsgPart`] is returned before its bytes have all landed; the caller
/// gates delivery on the part reader's batch (see [`crate::dataio`]). Heartbeats never
/// arrive here — they ride their own stream (see [`read_heartbeat`]).
pub(crate) async fn read_frame<R, N>(
    reader: &mut R,
    part_reader: &mut PartReader<N>,
) -> std::io::Result<ConnectionCommand>
where
    R: AsyncRead + Unpin,
    N: Net,
{
    Ok(match read_header::<_, WireFrame>(reader).await? {
        WireFrame::ActorMessage { destination_ident } => ConnectionCommand::SendMessage {
            destination_ident,
            payload: SendPayload::ActorMessage(part_reader.read_message_parts(reader).await?),
        },
        WireFrame::FireMonitorMessage {
            destination_ident,
            to_monitor,
            is_timeout,
        } => ConnectionCommand::SendMessage {
            destination_ident,
            payload: SendPayload::FireMonitor {
                to_monitor,
                is_timeout,
            },
        },
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
        WireFrame::PublishRoutes { live, dead } => ConnectionCommand::PublishRoutes { live, dead },
        WireFrame::UpdateMonitorSubscription {
            listener,
            target,
            op,
        } => ConnectionCommand::UpdateMonitorSubscription {
            listener,
            target,
            op,
        },
        WireFrame::Severed { reason } => ConnectionCommand::Severed { reason },
        WireFrame::PublishGatewayRoutes { live } => {
            ConnectionCommand::PublishGatewayRoutes { live }
        }
        WireFrame::GatewayDied { dead } => ConnectionCommand::GatewayDied { dead },
    })
}

/// Serialize and write one [`SideChannelMessage`]. A `Send` of an actor message writes a
/// routing-only header then its parts via [`PartWriter::write_message_parts`] (planned and, if
/// large, striped by `part_writer`); every other action is a header-only frame.
pub(crate) async fn write_side_channel<W, N>(
    writer: &mut W,
    message: SideChannelMessage,
    part_writer: &mut PartWriter<N>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    N: Net,
{
    let SideChannelMessage {
        gateway_for_actor,
        action,
    } = message;
    let frame = match action {
        SideChannelAction::Send(SendPayload::ActorMessage(parts)) => {
            write_header(
                writer,
                &SideChannelFrame::SendActorMessage { gateway_for_actor },
            )
            .await?;
            return part_writer.write_message_parts(writer, parts).await;
        }
        SideChannelAction::Send(SendPayload::FireMonitor {
            to_monitor,
            is_timeout,
        }) => SideChannelFrame::SendFireMonitor {
            gateway_for_actor,
            to_monitor,
            is_timeout,
        },
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
    write_frame(writer, &frame).await
}

/// Write a sibling side-channel heartbeat onto the heartbeat stream as a bare
/// [`SideChannelHeartbeat`] (no message parts). The transport builds it directly, so
/// heartbeats never pass through ctx's [`SideChannelMessage`] routing types.
pub(crate) async fn write_side_channel_heartbeat<W: AsyncWrite + Unpin>(
    writer: &mut W,
    recipient: Vec<u8>,
    from: Vec<u8>,
    conn_id: ConnectionId,
    kind: BeatKind,
) -> std::io::Result<()> {
    let heartbeat = SideChannelHeartbeat {
        recipient,
        from,
        conn_id,
        kind,
    };
    write_frame(writer, &heartbeat).await
}

/// Read and decode one side-channel frame into its [`SideChannelMessage`]. A `Send` of an
/// actor message reads its parts through `parts` (exactly like [`read_frame`]); every
/// other action is header-only. Heartbeats travel on the companion heartbeat stream (see
/// [`read_side_channel_heartbeat`]) and never appear here.
pub(crate) async fn read_side_channel<R, N>(
    reader: &mut R,
    part_reader: &mut PartReader<N>,
) -> std::io::Result<SideChannelMessage>
where
    R: AsyncRead + Unpin,
    N: Net,
{
    Ok(match read_header::<_, SideChannelFrame>(reader).await? {
        SideChannelFrame::SendActorMessage { gateway_for_actor } => SideChannelMessage {
            gateway_for_actor,
            action: SideChannelAction::Send(SendPayload::ActorMessage(
                part_reader.read_message_parts(reader).await?,
            )),
        },
        SideChannelFrame::SendFireMonitor {
            gateway_for_actor,
            to_monitor,
            is_timeout,
        } => SideChannelMessage {
            gateway_for_actor,
            action: SideChannelAction::Send(SendPayload::FireMonitor {
                to_monitor,
                is_timeout,
            }),
        },
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
    })
}

/// Read one delegated-heartbeat beat/ack/release off a gateway side-channel's
/// dedicated heartbeat stream, written by [`write_side_channel_heartbeat`] as a bare
/// [`SideChannelHeartbeat`] (no message parts).
pub(crate) async fn read_side_channel_heartbeat<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<SideChannelHeartbeat> {
    read_header(reader).await
}
