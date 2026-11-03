/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;

use bytes::BufMut as _;
use bytes::Bytes;
use bytes::BytesMut;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt as _;

/// Maximum payload length of a HELLO or DATAGRAM frame.
pub(super) const MAX_PAYLOAD: usize = 65_535;

/// Identifies what a frame's payload contains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum FrameKind {
    /// Carries the dialer's initial `node:<name>` routing label.
    Hello = 0,
    /// Carries one complete Chrysalis datagram.
    Datagram = 1,
}

impl TryFrom<u8> for FrameKind {
    type Error = io::Error;

    /// Decodes the frame-kind byte, rejecting unrecognized values.
    fn try_from(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(Self::Hello),
            1 => Ok(Self::Datagram),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown vsock frame kind",
            )),
        }
    }
}

/// Encodes one complete HELLO or DATAGRAM frame.
///
/// The returned bytes contain the one-byte frame kind, the payload length as a
/// big-endian `u32`, and the payload itself. `BytesMut::freeze` returns an
/// immutable `Bytes` value that can be queued and cheaply cloned.
///
/// # Panics
///
/// Panics if `payload` exceeds [`MAX_PAYLOAD`]. Public send paths reject that
/// size before calling this function; the prebuilt HELLO is bounded by
/// [`MAX_NAME`].
///
/// [`MAX_NAME`]: super::address::MAX_NAME
pub(super) fn encode_frame(kind: FrameKind, payload: &[u8]) -> Bytes {
    assert!(
        payload.len() <= MAX_PAYLOAD,
        "a frame payload is at most {MAX_PAYLOAD} bytes"
    );
    let len = payload.len() as u32;
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.put_u8(kind as u8);
    frame.put_u32(len);
    frame.put_slice(payload);
    frame.freeze()
}

/// Reads one frame, rejecting a payload over 65,535 bytes.
pub(super) async fn read_frame<R>(read: &mut R) -> io::Result<(FrameKind, Bytes)>
where
    R: AsyncRead + Unpin,
{
    let kind = read.read_u8().await?;
    let len = read.read_u32().await? as usize;
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "vsock frame payload exceeds 65,535 bytes",
        ));
    }
    let mut payload = vec![0; len];
    read.read_exact(&mut payload).await?;
    Ok((FrameKind::try_from(kind)?, Bytes::from(payload)))
}
