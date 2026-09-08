/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::error;
use std::fmt;
use std::io;

/// An endpoint construction or driver error.
#[derive(Debug)]
pub enum Error {
    /// Packet I/O failed.
    Io(io::Error),
    /// The QUIC state machine rejected an operation or packet.
    Quiche(quiche::Error),
    /// An operation is invalid for the endpoint role.
    WrongRole,
    /// A connection ID is unknown to this endpoint.
    UnknownConnection,
    /// A received packet cannot be routed to one connection.
    UnroutablePacket,
    /// Two connections advertised the same source CID.
    CidCollision,
    /// The endpoint no longer accepts new work.
    DriverStopped,
    /// The endpoint exhausted the QUIC stream ID space for one connection.
    StreamIdExhausted,
    /// One connection reached its configured active-stream limit.
    StreamLimit,
    /// The endpoint exhausted its one-shot connection key space.
    ConnectionKeyExhausted,
    /// Server connection or handshake admission is at its configured limit.
    AdmissionLimited,
    /// A QUIC Retry token was invalid, expired, or bound to another source.
    InvalidRetryToken,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "packet i/o: {error}"),
            Self::Quiche(error) => write!(formatter, "quic: {error}"),
            Self::WrongRole => formatter.write_str("operation is invalid for endpoint role"),
            Self::UnknownConnection => formatter.write_str("unknown connection"),
            Self::UnroutablePacket => formatter.write_str("unroutable QUIC packet"),
            Self::CidCollision => formatter.write_str("QUIC source CID collision"),
            Self::DriverStopped => formatter.write_str("endpoint driver is shut down"),
            Self::StreamIdExhausted => formatter.write_str("QUIC stream ID space exhausted"),
            Self::StreamLimit => formatter.write_str("QUIC active stream limit reached"),
            Self::ConnectionKeyExhausted => {
                formatter.write_str("QUIC connection key space exhausted")
            }
            Self::AdmissionLimited => formatter.write_str("QUIC connection admission limited"),
            Self::InvalidRetryToken => formatter.write_str("invalid QUIC Retry token"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Quiche(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<quiche::Error> for Error {
    fn from(error: quiche::Error) -> Self {
        Self::Quiche(error)
    }
}
