/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use super::frame::MAX_PAYLOAD;
use super::invalid_input;
use crate::DatagramAddr;

/// The [`DatagramAddr`] scheme routed through [`VsockDatagramSocket`].
///
/// [`VsockDatagramSocket`]: super::VsockDatagramSocket
pub const VSOCK_SCHEME: &str = "vsock";

/// Prefix for peer addresses announced over an already established connection.
const NODE_PREFIX: &str = "node:";

/// Prefix for direct kernel AF_VSOCK addresses.
const NATIVE_PREFIX: &str = "native:";

/// Prefix for guest ports reached through a VMM's host-side Unix socket.
const HYBRID_PREFIX: &str = "hybrid:";

/// Prefix for direct Unix stream-socket addresses.
const UNIX_PREFIX: &str = "unix:";

/// The longest name whose `node:` address fits in one HELLO payload.
pub(super) const MAX_NAME: usize = MAX_PAYLOAD - NODE_PREFIX.len();

/// The structured form of a [`DatagramAddr`] used by [`VsockDatagramSocket`].
///
/// Chrysalis passes addresses between carrier-independent components as
/// `DatagramAddr`. When its scheme is `vsock`, `VsockDatagramSocket` parses the
/// carrier-specific bytes into this enum:
///
/// ```text
/// VsockDatagramAddr::Native { cid: 2, port: 5001 }
///                         ⇅
///                  fallible TryFrom
///                         ⇅
/// DatagramAddr {
///     scheme: "vsock",
///     address bytes: "native:2:5001",
/// }
/// ```
///
/// `Node` names a peer on an existing connection. `Native`, `Hybrid`, and
/// `Unix` describe how to open or listen for a byte-stream connection.
///
/// [`VsockDatagramSocket`]: super::VsockDatagramSocket
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum VsockDatagramAddr {
    /// A peer name learned from the HELLO frame on an accepted connection.
    ///
    /// Sending to this address reuses that live connection; the name alone
    /// cannot open a new connection.
    Node(String),
    /// A direct kernel `AF_VSOCK` CID and port.
    Native {
        /// The context identifier.
        cid: u32,
        /// The port.
        port: u32,
    },
    /// A guest port reached through a VMM's host-side Unix socket.
    Hybrid {
        /// The guest port.
        port: u32,
        /// The VMM's host-side socket.
        uds: PathBuf,
    },
    /// A direct Unix stream-socket path.
    Unix(PathBuf),
}

/// Encodes a carrier-specific address as a generic [`DatagramAddr`].
impl TryFrom<&VsockDatagramAddr> for DatagramAddr {
    type Error = io::Error;

    /// Fails with [`io::ErrorKind::InvalidInput`] for an empty or overlong
    /// name, or an empty or non-UTF-8 path.
    fn try_from(addr: &VsockDatagramAddr) -> io::Result<Self> {
        let text = match addr {
            VsockDatagramAddr::Node(name) => format!("{NODE_PREFIX}{}", checked_name(name)?),
            VsockDatagramAddr::Native { cid, port } => format!("{NATIVE_PREFIX}{cid}:{port}"),
            VsockDatagramAddr::Hybrid { port, uds } => {
                format!("{HYBRID_PREFIX}{port}:{}", path_text(uds)?)
            }
            VsockDatagramAddr::Unix(path) => format!("{UNIX_PREFIX}{}", path_text(path)?),
        };
        Ok(Self::new(VSOCK_SCHEME, text.into_bytes()))
    }
}

/// Decodes a carrier-specific address from a generic [`DatagramAddr`].
impl TryFrom<&DatagramAddr> for VsockDatagramAddr {
    type Error = io::Error;

    /// Fails with [`io::ErrorKind::InvalidInput`] for anything outside this
    /// address space.
    fn try_from(addr: &DatagramAddr) -> io::Result<Self> {
        if addr.scheme() != VSOCK_SCHEME {
            return Err(invalid_input("not a vsock address"));
        }
        let text = std::str::from_utf8(addr.opaque())
            .map_err(|_| invalid_input("vsock address is not UTF-8"))?;
        if let Some(name) = text.strip_prefix(NODE_PREFIX) {
            return Ok(Self::Node(checked_name(name)?.to_owned()));
        }
        if let Some(rest) = text.strip_prefix(NATIVE_PREFIX) {
            let (cid, port) = rest
                .split_once(':')
                .ok_or_else(|| invalid_input("native vsock address needs a CID and a port"))?;
            return Ok(Self::Native {
                cid: parse_u32(cid)?,
                port: parse_u32(port)?,
            });
        }
        if let Some(rest) = text.strip_prefix(HYBRID_PREFIX) {
            let (port, uds) = rest
                .split_once(':')
                .ok_or_else(|| invalid_input("hybrid vsock address needs a port and a path"))?;
            return Ok(Self::Hybrid {
                port: parse_u32(port)?,
                uds: checked_path(uds)?,
            });
        }
        if let Some(path) = text.strip_prefix(UNIX_PREFIX) {
            return Ok(Self::Unix(checked_path(path)?));
        }
        Err(invalid_input("unknown vsock address kind"))
    }
}

impl FromStr for VsockDatagramAddr {
    type Err = io::Error;

    /// Parses the address text used inside a `vsock` [`DatagramAddr`].
    fn from_str(text: &str) -> io::Result<Self> {
        Self::try_from(&DatagramAddr::new(VSOCK_SCHEME, text.as_bytes()))
    }
}

/// A caller-supplied [`DatagramAddr`] and its parsed [`VsockDatagramAddr`] form.
///
/// For example, these two fields represent the same destination:
///
/// ```text
/// datagram_addr = DatagramAddr {
///     scheme: "vsock",
///     address bytes: "native:2:5001",
/// }
///
/// stream_addr = VsockDatagramAddr::Native {
///     cid: 2,
///     port: 5001,
/// }
/// ```
pub(super) struct ParsedDestination<'a> {
    /// The original address supplied by the caller and used as a connection
    /// map key.
    pub(super) datagram_addr: &'a DatagramAddr,

    /// The validated carrier address used to decide how to connect.
    pub(super) stream_addr: VsockDatagramAddr,
}

impl<'a> TryFrom<&'a DatagramAddr> for ParsedDestination<'a> {
    type Error = io::Error;

    /// Parses `datagram_addr` once and retains both representations together.
    fn try_from(datagram_addr: &'a DatagramAddr) -> io::Result<Self> {
        Ok(Self {
            datagram_addr,
            stream_addr: VsockDatagramAddr::try_from(datagram_addr)?,
        })
    }
}

/// Returns `name` unchanged when it is nonempty and fits in a HELLO frame.
fn checked_name(name: &str) -> io::Result<&str> {
    if name.is_empty() || name.len() > MAX_NAME {
        return Err(invalid_input("a vsock name must be 1 to 65,530 bytes"));
    }
    Ok(name)
}

/// Returns the nonempty UTF-8 text used to encode `path` in a [`DatagramAddr`].
fn path_text(path: &Path) -> io::Result<&str> {
    let text = path
        .to_str()
        .ok_or_else(|| invalid_input("a vsock address path must be UTF-8"))?;
    if text.is_empty() {
        return Err(invalid_input("a vsock address path must be non-empty"));
    }
    Ok(text)
}

/// Converts nonempty address text into a filesystem path.
fn checked_path(text: &str) -> io::Result<PathBuf> {
    if text.is_empty() {
        return Err(invalid_input("a vsock address path must be non-empty"));
    }
    Ok(PathBuf::from(text))
}

/// Parses a decimal AF_VSOCK context ID or port.
fn parse_u32(text: &str) -> io::Result<u32> {
    text.parse()
        .map_err(|_| invalid_input("a vsock CID or port must be a decimal u32"))
}
