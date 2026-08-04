/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Wire framing for the UNIX-socket transport, including shared-memory parts.
//!
//! Deliberately **separate** from the generic [`crate::framing`] used by quic:
//! the UNIX transport is where machine-local shared-memory handling lives, so its
//! wire format carries fd-passing and slab part descriptors that have no business
//! in the cross-machine framing.
//!
//! A frame is `[u64 header_len][header][maybe fd-exchange][inline part bytes]`.
//! The header is a bincode [`UnixFrame`]; the header and inline bytes are
//! read/written with plain readiness-driven `try_read`/`try_write`. Only if the
//! header says fds are present (one per [`PartDesc::Shm`] part) do we do one extra
//! `sendmsg`/`recvmsg` step — a 1-byte payload carrying all the fds via
//! `SCM_RIGHTS`. No-fd frames never touch the fd-passing syscalls.
//!
//! Per part on send: a part already in the slab is relayed by descriptor (no map,
//! no copy); a large owned part on a connection that has a [`ShmClient`] is
//! allocated, `memcpy`d into the slab, and sent as a descriptor + liveness token;
//! everything else streams inline. On receive, an inline part is read into owned
//! bytes and a slab part is reconstructed as *unmapped* metadata (mapped only when
//! the user takes the bytes).

use std::os::fd::AsRawFd;
use std::os::fd::IntoRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;

use serde::Deserialize;
use serde::Serialize;
use tokio::io::Interest;
use tokio::net::UnixStream;

use crate::Role;
use crate::connection::AncestorPayload;
use crate::connection::ConnectionCommand;
use crate::connection::SendPayload;
use crate::msg::MsgPart;
use crate::shm::MapperHandle;
use crate::shm::SHM_THRESHOLD;
use crate::shm::ShmClient;
use crate::shm::recv_with_fds;
use crate::shm::send_with_fds;

/// One frame on the UNIX wire. A variant per [`ConnectionCommand`] that crosses
/// the pipe (including `Establish`). Message-part *bytes* are never carried here —
/// only inline-part lengths and slab-part descriptors.
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
    /// Gateway-state handoff: the header carries nothing; two fds (the slab object
    /// then the dgram request socket) ride the fd-exchange step.
    GatewayState,
}

/// Wire form of a message payload. An actor message carries a descriptor per part
/// (inline length or slab offset/length); a monitor fire carries the small dead
/// target ident inline.
#[derive(Serialize, Deserialize)]
enum UnixPayload {
    ActorMessage { parts: Vec<PartDesc> },
    FireMonitor { to_monitor: Vec<u8> },
}

/// How one message part is represented on the wire. `Inline` parts have their
/// bytes streamed raw after the header; `Shm` parts carry only an offset+length
/// into the slab and are accompanied (in part order) by one liveness-token fd in
/// the fd-exchange step.
#[derive(Serialize, Deserialize)]
enum PartDesc {
    Inline { len: u64 },
    Shm { offset: u64, len: u64 },
}

fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// Serialize and write one command over `stream`. For a message, large owned
/// parts are moved into the slab (allocate via `client`, `memcpy` via `mapper`)
/// and sent as descriptors + token fds; slab parts are relayed by descriptor;
/// small/owned parts stream inline. `client`/`mapper` are unused by non-message
/// commands and by messages with no large parts.
pub(crate) async fn write_command(
    stream: &UnixStream,
    command: ConnectionCommand,
    mapper: &MapperHandle,
    client: Option<ShmClient>,
) -> std::io::Result<()> {
    match command {
        ConnectionCommand::SendMessage {
            destination_ident,
            payload: SendPayload::ActorMessage(parts),
        } => write_actor_message(stream, destination_ident, parts, mapper, client).await,
        ConnectionCommand::SendMessage {
            destination_ident,
            payload: SendPayload::FireMonitor(to_monitor),
        } => {
            let frame = UnixFrame::Message {
                destination_ident,
                payload: UnixPayload::FireMonitor { to_monitor },
            };
            write_header(stream, &frame).await
        }
        ConnectionCommand::Establish {
            role,
            ident,
            name_for_other,
            alive,
        } => {
            write_header(
                stream,
                &UnixFrame::Establish {
                    role,
                    ident,
                    name_for_other,
                    alive,
                },
            )
            .await
        }
        ConnectionCommand::PublishRoutes { live, dead } => {
            write_header(stream, &UnixFrame::PublishRoutes { live, dead }).await
        }
        ConnectionCommand::ToAncestor {
            to_monitor,
            payload,
        } => {
            write_header(
                stream,
                &UnixFrame::ToAncestor {
                    to_monitor,
                    payload,
                },
            )
            .await
        }
        ConnectionCommand::Severed { reason } => {
            write_header(stream, &UnixFrame::Severed { reason }).await
        }
        ConnectionCommand::GatewayState { client } => {
            write_header(stream, &UnixFrame::GatewayState).await?;
            // Order matters: slab fd first, then the dgram request socket — the
            // reader reconstructs in the same order.
            send_fd_exchange(stream, &[client.slab_fd(), client.dgram_fd()]).await
        }
    }
}

/// Build and write a `Message` frame: classify each part (relay slab / shm-ify
/// large owned / inline), write the header, hand over the token fds in one
/// fd-exchange step, then stream the inline bytes.
async fn write_actor_message(
    stream: &UnixStream,
    destination_ident: Vec<u8>,
    parts: Vec<MsgPart>,
    mapper: &MapperHandle,
    client: Option<ShmClient>,
) -> std::io::Result<()> {
    let mut descs = Vec::with_capacity(parts.len());
    let mut tokens: Vec<OwnedFd> = Vec::new();
    let mut inline: Vec<MsgPart> = Vec::new();

    for part in parts {
        if part.is_shm() {
            // Already in the slab: relay the descriptor and its token, no copy.
            let (offset, len, token) = part.into_shm();
            descs.push(PartDesc::Shm { offset, len });
            tokens.push(token);
            continue;
        }
        let len = part.len() as u64;
        match client {
            Some(client) if len >= SHM_THRESHOLD => {
                let (offset, token) = client.allocate(len).await?;
                let dst = {
                    let mut mapper = mapper.lock().expect("shm mapper mutex poisoned");
                    // SAFETY: `offset` was just granted for `len` bytes against
                    // `client`'s slab, so the file covers it and the mapper can map it.
                    unsafe { mapper.map(client.slab_fd(), offset, len as usize)? }
                };
                let src = part.as_bytes();
                // SAFETY: `dst` is a writable mapping of `len` bytes (just mapped)
                // and `src` is exactly `len` bytes; the regions do not overlap (one
                // is the user buffer, the other the slab).
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, len as usize) };
                descs.push(PartDesc::Shm { offset, len });
                tokens.push(token);
                // `part` (the owned bytes) drops here, freeing the user buffer.
            }
            _ => {
                descs.push(PartDesc::Inline { len });
                inline.push(part);
            }
        }
    }

    let frame = UnixFrame::Message {
        destination_ident,
        payload: UnixPayload::ActorMessage { parts: descs },
    };
    write_header(stream, &frame).await?;

    if !tokens.is_empty() {
        let raw: Vec<RawFd> = tokens.iter().map(AsRawFd::as_raw_fd).collect();
        send_fd_exchange(stream, &raw).await?;
        // The kernel has duplicated the tokens to the peer; drop our copies so the
        // grant's liveness now rides with the peer (and any relay hop).
        drop(tokens);
    }

    for part in &inline {
        write_all(stream, part.as_bytes()).await?;
    }
    Ok(())
}

/// Serialize `frame` and write `[u64 len][header]`.
async fn write_header(stream: &UnixStream, frame: &UnixFrame) -> std::io::Result<()> {
    let header = bincode::serde::encode_to_vec(frame, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    write_all(stream, &(header.len() as u64).to_le_bytes()).await?;
    write_all(stream, &header).await
}

/// Read one frame and map it back to a [`ConnectionCommand`]. Inline parts are
/// read into owned buffers; slab parts are reconstructed as unmapped metadata
/// using this connection's `mapper` and `client` (for the slab fd) plus the token
/// fds received in the fd-exchange step.
pub(crate) async fn read_command(
    stream: &UnixStream,
    mapper: &MapperHandle,
    client: Option<ShmClient>,
) -> std::io::Result<ConnectionCommand> {
    let mut len_buf = [0u8; 8];
    read_exact(stream, &mut len_buf).await?;
    let header_len = u64::from_le_bytes(len_buf) as usize;

    let mut header = vec![0u8; header_len];
    read_exact(stream, &mut header).await?;
    let (frame, _) = bincode::serde::decode_from_slice::<UnixFrame, _>(&header, bincode_config())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;

    Ok(match frame {
        UnixFrame::Message {
            destination_ident,
            payload,
        } => {
            let payload = read_message_payload(stream, payload, mapper, client).await?;
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
        UnixFrame::GatewayState => {
            // Two fds in the order they were sent: slab object, then dgram socket.
            // They must stay open for the process lifetime, so leak the received
            // owned fds into raw fds the (non-owning, `Copy`) ShmClient holds.
            let mut fds = recv_fd_exchange(stream, 2).await?.into_iter();
            let slab = fds.next().expect("two fds checked by recv_fd_exchange");
            let dgram = fds.next().expect("two fds checked by recv_fd_exchange");
            ConnectionCommand::GatewayState {
                client: ShmClient::from_raw(dgram.into_raw_fd(), slab.into_raw_fd()),
            }
        }
    })
}

/// Reconstruct a message payload's parts: collect token fds (one per slab part)
/// in the single fd-exchange step, then walk the descriptors in order, reading
/// inline bytes and pairing each slab descriptor with the next token fd.
async fn read_message_payload(
    stream: &UnixStream,
    payload: UnixPayload,
    mapper: &MapperHandle,
    client: Option<ShmClient>,
) -> std::io::Result<SendPayload> {
    let descs = match payload {
        UnixPayload::ActorMessage { parts } => parts,
        UnixPayload::FireMonitor { to_monitor } => return Ok(SendPayload::FireMonitor(to_monitor)),
    };

    let n_fds = descs
        .iter()
        .filter(|d| matches!(d, PartDesc::Shm { .. }))
        .count();
    let mut tokens = if n_fds > 0 {
        recv_fd_exchange(stream, n_fds).await?.into_iter()
    } else {
        Vec::new().into_iter()
    };

    let mut parts = Vec::with_capacity(descs.len());
    for desc in descs {
        match desc {
            PartDesc::Inline { len } => {
                let mut buf = vec![0u8; len as usize];
                read_exact(stream, &mut buf).await?;
                parts.push(MsgPart::from_bytes(buf));
            }
            PartDesc::Shm { offset, len } => {
                let token = tokens.next().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "missing shm token fd")
                })?;
                let client = client.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "received a slab part but have no shm client",
                    )
                })?;
                parts.push(MsgPart::new_shm(
                    mapper.clone(),
                    client.slab_fd(),
                    token,
                    offset,
                    len,
                ));
            }
        }
    }
    Ok(SendPayload::ActorMessage(parts))
}

// ---------------------------------------------------------------------------
// Readiness-driven byte and fd IO over the shared UnixStream
// ---------------------------------------------------------------------------

/// Fill `buf` exactly, awaiting readability between partial reads. EOF before the
/// buffer is full is an error (a truncated frame).
async fn read_exact(stream: &UnixStream, buf: &mut [u8]) -> std::io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        stream.readable().await?;
        match stream.try_read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unix frame truncated",
                ));
            }
            Ok(n) => filled += n,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Write all of `buf`, awaiting writability between partial writes.
async fn write_all(stream: &UnixStream, buf: &[u8]) -> std::io::Result<()> {
    let mut sent = 0;
    while sent < buf.len() {
        stream.writable().await?;
        match stream.try_write(&buf[sent..]) {
            Ok(n) => sent += n,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// The fd-exchange step on send: one `sendmsg` of a single marker byte carrying
/// all `fds` via `SCM_RIGHTS`, driven under write readiness.
async fn send_fd_exchange(stream: &UnixStream, fds: &[RawFd]) -> std::io::Result<()> {
    loop {
        stream.writable().await?;
        match stream.try_io(Interest::WRITABLE, || {
            send_with_fds(stream.as_raw_fd(), &[0u8], fds)
        }) {
            Ok(_) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        }
    }
}

/// The fd-exchange step on receive: one `recvmsg` of the single marker byte,
/// collecting the `expected` token fds. The 1-byte iovec bounds the read to the
/// marker so following inline bytes stay in the stream.
async fn recv_fd_exchange(stream: &UnixStream, expected: usize) -> std::io::Result<Vec<OwnedFd>> {
    loop {
        stream.readable().await?;
        let mut marker = [0u8; 1];
        match stream.try_io(Interest::READABLE, || {
            recv_with_fds(stream.as_raw_fd(), &mut marker, expected)
        }) {
            Ok((0, _)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unix frame truncated before fd exchange",
                ));
            }
            Ok((_, fds)) => {
                if fds.len() != expected {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected fd count in fd exchange",
                    ));
                }
                return Ok(fds);
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;
    use crate::connection::ConnectionCommand;
    use crate::connection::SendPayload;
    use crate::msg::MsgPart;
    use crate::shm::MapperHandle;
    use crate::shm::SHM_THRESHOLD;
    use crate::shm::ShmMapper;
    use crate::shm::ShmServer;

    fn run_local<F: std::future::Future>(future: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        tokio::task::LocalSet::new().block_on(&rt, future)
    }

    fn mapper() -> MapperHandle {
        Arc::new(Mutex::new(ShmMapper::new()))
    }

    /// A `[small header, big payload]` message: the header stays inline and the
    /// payload travels through the slab (a descriptor + token on the socket, not
    /// the bytes), and both arrive intact and in order. The two mappers stand in
    /// for the sender's and receiver's separate contexts mapping the one slab.
    #[test]
    fn large_part_goes_through_slab_small_part_inline() {
        run_local(async {
            let server = ShmServer::new().expect("shm server");
            let client = server.client();
            let send_mapper = mapper();
            let recv_mapper = mapper();
            let (a, b) = UnixStream::pair().expect("socketpair");

            let header = b"header".to_vec();
            let payload = vec![0x42u8; SHM_THRESHOLD as usize + 100];
            let command = ConnectionCommand::SendMessage {
                destination_ident: b"dest".to_vec(),
                payload: SendPayload::ActorMessage(vec![
                    MsgPart::from_bytes(header.clone()),
                    MsgPart::from_bytes(payload.clone()),
                ]),
            };

            let (write_res, read_res) = tokio::join!(
                write_command(&a, command, &send_mapper, Some(client)),
                read_command(&b, &recv_mapper, Some(client)),
            );
            write_res.expect("write");
            let received = read_res.expect("read");

            let ConnectionCommand::SendMessage {
                destination_ident,
                payload: SendPayload::ActorMessage(parts),
            } = received
            else {
                panic!("expected an actor message");
            };
            assert_eq!(destination_ident, b"dest");
            assert_eq!(parts.len(), 2, "both parts reconstructed");
            assert_eq!(
                parts[0].as_bytes(),
                header.as_slice(),
                "inline header intact"
            );
            assert_eq!(
                parts[1].as_bytes(),
                payload.as_slice(),
                "slab payload intact and in order"
            );
        });
    }

    /// With no shm client, even a large part falls back to streaming inline.
    #[test]
    fn large_part_without_client_streams_inline() {
        run_local(async {
            let map = mapper();
            let (a, b) = UnixStream::pair().expect("socketpair");
            let payload = vec![0x7u8; SHM_THRESHOLD as usize + 100];
            let command = ConnectionCommand::SendMessage {
                destination_ident: b"d".to_vec(),
                payload: SendPayload::ActorMessage(vec![MsgPart::from_bytes(payload.clone())]),
            };

            let (write_res, read_res) = tokio::join!(
                write_command(&a, command, &map, None),
                read_command(&b, &map, None),
            );
            write_res.expect("write");
            let received = read_res.expect("read");

            let ConnectionCommand::SendMessage {
                payload: SendPayload::ActorMessage(parts),
                ..
            } = received
            else {
                panic!("expected an actor message");
            };
            assert_eq!(parts.len(), 1);
            assert_eq!(
                parts[0].as_bytes(),
                payload.as_slice(),
                "inline fallback intact"
            );
        });
    }
}
