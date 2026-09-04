/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Stateless QUIC address validation for server-side connection admission.
//!
//! A server cannot authenticate the peer's certificate-derived PID until the QUIC TLS handshake
//! completes, but accepting that handshake already allocates connection state. For raw UDP, an
//! attacker can therefore spoof source addresses and consume pending-handshake capacity without
//! ever reaching PID authentication. QUIC Retry validates the packet's return address first:
//!
//! 1. An unvalidated Initial packet carries no token, so the server returns a Retry packet whose
//!    token is bound to the observed source IP and port.
//! 2. A client that receives the Retry echoes the token in a new Initial packet.
//! 3. The server verifies the token's HMAC, age, and source address before allocating connection
//!    state. The TLS handshake then authenticates the certificate and derives the peer PID.
//!
//! This proves recent reachability at an IP and port, not peer identity. It limits state-exhaustion
//! and reflection attacks before identity authentication is possible. The token carries the state
//! needed for verification, so the server retains only rotating secrets rather than a challenge
//! record per client. An authenticated upstream boundary may provide equivalent address validation
//! and allow the endpoint to skip Retry.
//!
//! See [QUIC address validation], [address validation using Retry packets], and the [Retry packet]
//! format in RFC 9000.
//!
//! [QUIC address validation]: https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1
//! [address validation using Retry packets]: https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.2
//! [Retry packet]: https://www.rfc-editor.org/rfc/rfc9000.html#section-17.2.5

use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use ring::hmac;
use ring::rand::SecureRandom;
use ring::rand::SystemRandom;

/// Format version stored in the first byte of every Retry token body.
const TOKEN_VERSION: u8 = 1;
/// Bytes of operating-system randomness used as the HMAC-SHA256 key.
const SECRET_BYTES: usize = 32;
/// Bytes in the HMAC-SHA256 tag appended to each serialized token body.
const TAG_BYTES: usize = 32;
/// Time for which a secret mints tokens before it becomes the previous verification secret.
const ROTATION_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Maximum accepted age of a signed token, limiting replay independently of key rotation.
const TOKEN_LIFETIME: Duration = Duration::from_secs(2 * 60);

struct Secret {
    bytes: [u8; SECRET_BYTES],
    born: Instant,
}

impl Secret {
    fn random() -> Self {
        let mut bytes = [0; SECRET_BYTES];
        SystemRandom::new()
            .fill(&mut bytes)
            .expect("operating-system randomness should be available for QUIC Retry");
        Self {
            bytes,
            born: Instant::now(),
        }
    }
}

pub(crate) struct RetryTokens {
    current: Secret,
    previous: Option<Secret>,
}

impl RetryTokens {
    pub(crate) fn new() -> Self {
        Self {
            current: Secret::random(),
            previous: None,
        }
    }

    pub(crate) fn mint(&mut self, peer: SocketAddr, original_dcid: &[u8]) -> Vec<u8> {
        self.rotate();
        let mut token = token_body(peer, original_dcid);
        let key = hmac::Key::new(hmac::HMAC_SHA256, &self.current.bytes);
        token.extend_from_slice(hmac::sign(&key, &token).as_ref());
        token
    }

    pub(crate) fn validate(&mut self, peer: SocketAddr, token: &[u8]) -> Option<Vec<u8>> {
        self.rotate();
        let (body, tag) = token.split_at_checked(token.len().checked_sub(TAG_BYTES)?)?;
        let valid = verify(&self.current, body, tag)
            || self
                .previous
                .as_ref()
                .is_some_and(|secret| verify(secret, body, tag));
        if !valid {
            return None;
        }
        parse_body(peer, body)
    }

    fn rotate(&mut self) {
        if self.current.born.elapsed() < ROTATION_INTERVAL {
            return;
        }
        self.previous = Some(std::mem::replace(&mut self.current, Secret::random()));
    }
}

fn verify(secret: &Secret, body: &[u8], tag: &[u8]) -> bool {
    let key = hmac::Key::new(hmac::HMAC_SHA256, &secret.bytes);
    hmac::verify(&key, body, tag).is_ok()
}

fn token_body(peer: SocketAddr, original_dcid: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(1 + 8 + 1 + 16 + 2 + 1 + original_dcid.len());
    body.push(TOKEN_VERSION);
    body.extend_from_slice(&unix_seconds().to_be_bytes());
    match peer.ip() {
        IpAddr::V4(address) => {
            body.push(4);
            body.extend_from_slice(&address.octets());
        }
        IpAddr::V6(address) => {
            body.push(6);
            body.extend_from_slice(&address.octets());
        }
    }
    body.extend_from_slice(&peer.port().to_be_bytes());
    body.push(u8::try_from(original_dcid.len()).expect("QUIC CID length fits in one byte"));
    body.extend_from_slice(original_dcid);
    body
}

fn parse_body(peer: SocketAddr, body: &[u8]) -> Option<Vec<u8>> {
    let (&version, rest) = body.split_first()?;
    if version != TOKEN_VERSION {
        return None;
    }
    let (timestamp, rest) = rest.split_at_checked(8)?;
    let issued = u64::from_be_bytes(timestamp.try_into().ok()?);
    let age = unix_seconds().checked_sub(issued)?;
    if age > TOKEN_LIFETIME.as_secs() {
        return None;
    }
    let (&family, rest) = rest.split_first()?;
    let address_length = match family {
        4 => 4,
        6 => 16,
        _ => return None,
    };
    let (address, rest) = rest.split_at_checked(address_length)?;
    if address != address_bytes(peer.ip()) {
        return None;
    }
    let (port, rest) = rest.split_at_checked(2)?;
    if u16::from_be_bytes(port.try_into().ok()?) != peer.port() {
        return None;
    }
    let (&cid_length, cid) = rest.split_first()?;
    (cid.len() == usize::from(cid_length)).then(|| cid.to_vec())
}

fn address_bytes(address: IpAddr) -> Vec<u8> {
    match address {
        IpAddr::V4(address) => address.octets().to_vec(),
        IpAddr::V6(address) => address.octets().to_vec(),
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_token_authenticates_source_and_original_dcid() {
        let peer: SocketAddr = "[::1]:1234".parse().unwrap();
        let mut tokens = RetryTokens::new();
        let token = tokens.mint(peer, b"original");

        assert_eq!(tokens.validate(peer, &token).unwrap(), b"original");
        assert!(
            tokens
                .validate("[::1]:1235".parse().unwrap(), &token)
                .is_none()
        );
        let mut forged = token;
        forged[10] ^= 1;
        assert!(tokens.validate(peer, &forged).is_none());
    }
}
