/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::path::Path;

use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::net::UnixStream;

/// Maximum length of Firecracker's one-line reply to `CONNECT <port>\n`,
/// including the newline. A successful reply is `OK <digits>\n`; the digits
/// are validated but otherwise ignored. This limit does not apply to datagrams.
const MAX_REPLY_LEN: usize = 64;

/// Opens the host-side Unix-stream endpoint for a connection to a guest port.
///
/// On the Relay 1 → Alice link, Relay 1 calls this with VM A's Firecracker
/// socket path and Alice's guest port 5000:
///
/// 1. connect to Firecracker's host Unix socket;
/// 2. send `CONNECT 5000\n`;
/// 3. read one reply line, bounded by [`MAX_REPLY_LEN`];
/// 4. require `OK <digits>\n`;
/// 5. return the same `UnixStream`, now bridged to Alice's accepted native-vsock
///    endpoint.
///
/// The digits identify Firecracker's assigned host-side port and are validated
/// but otherwise ignored. Bytes after the reply newline remain unread for the
/// carrier protocol.
pub(super) async fn hybrid_connect(uds: &Path, port: u32) -> io::Result<UnixStream> {
    let mut stream = UnixStream::connect(uds).await?;
    // Ask Firecracker to bridge this Unix stream to the guest port.
    stream
        .write_all(format!("CONNECT {port}\n").as_bytes())
        .await?;

    // Read exactly one bounded line without consuming carrier bytes that
    // follow it.
    let mut line = Vec::with_capacity(MAX_REPLY_LEN);
    while line.last() != Some(&b'\n') {
        if line.len() == MAX_REPLY_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "hybrid vsock reply exceeds 64 bytes",
            ));
        }
        line.push(stream.read_u8().await?);
    }
    // Firecracker accepts with `OK <assigned_hostside_port>\n`.
    let accepted = line
        .strip_prefix(b"OK ")
        .and_then(|rest| rest.strip_suffix(b"\n"))
        .is_some_and(|digits| !digits.is_empty() && digits.iter().all(u8::is_ascii_digit));
    if !accepted {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!(
                "hybrid vsock connect to port {port} refused: {:?}",
                String::from_utf8_lossy(&line)
            ),
        ));
    }
    Ok(stream)
}
