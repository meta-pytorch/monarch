/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Dependency-free identity and QUIC CID wire types for Chrysalis.

use std::fmt;
use std::str::FromStr;

/// The number of bytes in a process ID.
pub const PID_LEN: usize = 16;

/// The number of bytes in a nameserver link ID.
pub const LINK_ID_LEN: usize = 16;

/// The fixed Chrysalis QUIC connection ID length.
pub const CID_LEN: usize = 20;

/// A globally unique process identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Pid([u8; PID_LEN]);

impl Pid {
    /// The reserved link-local protocol PID.
    pub const LINK_LOCAL: Self = Self([0; PID_LEN]);

    /// Constructs a PID from its bytes.
    pub const fn from_bytes(bytes: [u8; PID_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the PID bytes.
    pub const fn as_bytes(&self) -> &[u8; PID_LEN] {
        &self.0
    }

    /// Returns whether this is the reserved link-local protocol PID.
    pub fn is_link_local(self) -> bool {
        self.0 == Self::LINK_LOCAL.0
    }
}

/// An abbreviated process identifier used to match PIDs by leading hexadecimal digits.
///
/// Prefixes contain between 1 and 32 digits and support human-facing lookup when a
/// complete PID is unnecessary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PidPrefix {
    bytes: [u8; PID_LEN],
    digits: u8,
}

impl PidPrefix {
    /// Constructs the complete prefix for `pid`.
    pub const fn from_pid(pid: Pid) -> Self {
        Self {
            bytes: *pid.as_bytes(),
            digits: (PID_LEN * 2) as u8,
        }
    }

    /// Returns whether `pid` begins with this prefix.
    pub fn matches(self, pid: Pid) -> bool {
        let digits = usize::from(self.digits);
        let complete = digits / 2;
        if self.bytes[..complete] != pid.as_bytes()[..complete] {
            return false;
        }
        digits % 2 == 0 || self.bytes[complete] >> 4 == pid.as_bytes()[complete] >> 4
    }

    /// Returns the complete PID when this prefix contains all 32 digits.
    pub const fn as_pid(self) -> Option<Pid> {
        if self.digits as usize == PID_LEN * 2 {
            Some(Pid::from_bytes(self.bytes))
        } else {
            None
        }
    }
}

impl From<Pid> for PidPrefix {
    fn from(pid: Pid) -> Self {
        Self::from_pid(pid)
    }
}

impl FromStr for PidPrefix {
    type Err = ParsePidPrefixError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(ParsePidPrefixError::Empty);
        }
        if value.len() > PID_LEN * 2 {
            return Err(ParsePidPrefixError::TooLong);
        }
        let mut bytes = [0; PID_LEN];
        for (index, digit) in value.bytes().enumerate() {
            let nibble = match digit {
                b'0'..=b'9' => digit - b'0',
                b'a'..=b'f' => digit - b'a' + 10,
                b'A'..=b'F' => digit - b'A' + 10,
                _ => return Err(ParsePidPrefixError::InvalidHex),
            };
            if index % 2 == 0 {
                bytes[index / 2] = nibble << 4;
            } else {
                bytes[index / 2] |= nibble;
            }
        }
        Ok(Self {
            bytes,
            digits: value.len() as u8,
        })
    }
}

impl fmt::Display for PidPrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for index in 0..usize::from(self.digits) {
            let byte = self.bytes[index / 2];
            let nibble = if index % 2 == 0 {
                byte >> 4
            } else {
                byte & 0xf
            };
            write!(formatter, "{nibble:x}")?;
        }
        Ok(())
    }
}

/// A malformed hexadecimal PID prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParsePidPrefixError {
    /// The prefix has no digits.
    Empty,
    /// The prefix exceeds a complete 128-bit PID.
    TooLong,
    /// The prefix contains a non-hexadecimal character.
    InvalidHex,
}

impl fmt::Display for ParsePidPrefixError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "PID prefix must not be empty",
            Self::TooLong => "PID prefix must contain at most 32 hexadecimal digits",
            Self::InvalidHex => "PID prefix must contain only hexadecimal digits",
        })
    }
}

impl std::error::Error for ParsePidPrefixError {}

/// A one-shot identifier for one admitted parent-child link.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkId([u8; LINK_ID_LEN]);

impl LinkId {
    /// Constructs a link ID from its wire bytes.
    pub const fn from_bytes(bytes: [u8; LINK_ID_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the wire bytes.
    pub const fn as_bytes(&self) -> &[u8; LINK_ID_LEN] {
        &self.0
    }
}

/// The side occupied by an adjacent peer on one link.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LinkSide {
    /// The peer is this process's parent.
    Parent,
    /// The peer is one of this process's children.
    Child,
}

/// The authenticated topology context for one link-local protocol session.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LinkContext {
    link: LinkId,
    peer: Pid,
    side: LinkSide,
}

impl LinkContext {
    /// Constructs a context for one admitted link.
    pub const fn new(link: LinkId, peer: Pid, side: LinkSide) -> Self {
        Self { link, peer, side }
    }

    /// Returns the parent-allocated link incarnation.
    pub const fn link(self) -> LinkId {
        self.link
    }

    /// Returns the transport-authenticated adjacent process.
    pub const fn peer(self) -> Pid {
        self.peer
    }

    /// Returns where the adjacent process lies relative to this process.
    pub const fn side(self) -> LinkSide {
        self.side
    }
}

/// The endpoint-local suffix of an endpoint-issued CID.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectionKey(u32);

impl ConnectionKey {
    /// Constructs a connection key from its wire value.
    pub const fn from_u32(value: u32) -> Self {
        Self(value)
    }

    /// Returns the wire value.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// The client-chosen suffix of an Initial DCID.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InitialNonce(u32);

impl InitialNonce {
    /// Constructs an Initial nonce from its wire value.
    pub const fn from_u32(value: u32) -> Self {
        Self(value)
    }

    /// Returns the wire value.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// A fixed-width QUIC CID that routes to a PID.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RoutedCid([u8; CID_LEN]);

impl RoutedCid {
    /// Constructs an endpoint-issued CID.
    pub fn issued(pid: Pid, connection_key: ConnectionKey) -> Self {
        Self::new(pid, connection_key.as_u32())
    }

    /// Constructs a client-chosen Initial DCID.
    pub fn initial(pid: Pid, nonce: InitialNonce) -> Self {
        Self::new(pid, nonce.as_u32())
    }

    /// Returns the CID bytes.
    pub const fn as_bytes(&self) -> &[u8; CID_LEN] {
        &self.0
    }

    /// Returns the destination PID encoded in the CID.
    pub fn target(self) -> Pid {
        let mut bytes = [0; PID_LEN];
        bytes.copy_from_slice(&self.0[..PID_LEN]);
        Pid(bytes)
    }

    fn new(pid: Pid, suffix: u32) -> Self {
        let mut bytes = [0; CID_LEN];
        bytes[..PID_LEN].copy_from_slice(pid.as_bytes());
        bytes[PID_LEN..].copy_from_slice(&suffix.to_be_bytes());
        Self(bytes)
    }
}

/// Extracts the destination PID from the first QUIC packet in `datagram`.
pub fn target_pid(datagram: &[u8]) -> Option<Pid> {
    let first = *datagram.first()?;
    let start = if first & 0x80 == 0 {
        // A short header has a fixed-width DCID immediately after its flags byte.
        1
    } else {
        // A long header places its DCID after the flags byte, four-byte version,
        // and one-byte DCID length. Chrysalis routes only its fixed-width CIDs.
        let dcid_len = usize::from(*datagram.get(5)?);
        if dcid_len != CID_LEN {
            return None;
        }
        6
    };
    let bytes: [u8; PID_LEN] = datagram.get(start..start + PID_LEN)?.try_into().ok()?;
    Some(Pid(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PID: Pid = Pid::from_bytes([0x42; PID_LEN]);

    #[test]
    fn pid_prefixes_parse_display_and_match_at_nibble_boundaries() {
        let even: PidPrefix = "42".parse().expect("parse even PID prefix");
        let odd: PidPrefix = "424".parse().expect("parse odd PID prefix");
        let upper: PidPrefix = "42A".parse().expect("parse uppercase PID prefix");

        assert_eq!(even.to_string(), "42");
        assert_eq!(odd.to_string(), "424");
        assert!(even.matches(PID));
        assert!(odd.matches(PID));
        assert!(!upper.matches(PID));
        assert_eq!(PidPrefix::from(PID).as_pid(), Some(PID));
    }

    #[test]
    fn pid_prefixes_reject_empty_nonhex_and_oversized_text() {
        assert_eq!("".parse::<PidPrefix>(), Err(ParsePidPrefixError::Empty));
        assert_eq!(
            "x".parse::<PidPrefix>(),
            Err(ParsePidPrefixError::InvalidHex)
        );
        assert_eq!(
            "0".repeat(PID_LEN * 2 + 1).parse::<PidPrefix>(),
            Err(ParsePidPrefixError::TooLong)
        );
    }

    #[test]
    fn issued_cid_contains_pid_and_key() {
        let cid = RoutedCid::issued(PID, ConnectionKey::from_u32(0x1234_5678));

        assert_eq!(&cid.as_bytes()[..PID_LEN], PID.as_bytes());
        assert_eq!(&cid.as_bytes()[PID_LEN..], &[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(cid.target(), PID);
    }

    #[test]
    fn reserves_zero_for_link_local_protocols() {
        assert!(Pid::LINK_LOCAL.is_link_local());
        assert_eq!(Pid::LINK_LOCAL.as_bytes(), &[0; PID_LEN]);
        assert!(!PID.is_link_local());
    }

    #[test]
    fn link_context_identifies_incarnation_peer_and_orientation() {
        let link = LinkId::from_bytes([0x24; LINK_ID_LEN]);
        let context = LinkContext::new(link, PID, LinkSide::Parent);

        assert_eq!(context.link(), link);
        assert_eq!(context.peer(), PID);
        assert_eq!(context.side(), LinkSide::Parent);
    }

    #[test]
    fn extracts_pid_from_long_header() {
        let cid = RoutedCid::initial(PID, InitialNonce::from_u32(1));
        let mut datagram = vec![0xc0, 0, 0, 0, 1, CID_LEN as u8];
        datagram.extend_from_slice(cid.as_bytes());

        assert_eq!(target_pid(&datagram), Some(PID));
    }

    #[test]
    fn extracts_pid_from_short_header() {
        let cid = RoutedCid::issued(PID, ConnectionKey::from_u32(1));
        let mut datagram = vec![0x40];
        datagram.extend_from_slice(cid.as_bytes());

        assert_eq!(target_pid(&datagram), Some(PID));
    }

    #[test]
    fn rejects_wrong_long_header_cid_length() {
        let datagram = [0xc0, 0, 0, 0, 1, 8];

        assert_eq!(target_pid(&datagram), None);
    }
}
