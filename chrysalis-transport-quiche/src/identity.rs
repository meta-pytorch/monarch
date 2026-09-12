/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use chrysalis_core::PID_LEN;
use chrysalis_core::Pid;
use sha2::Digest as _;
use sha2::Sha256;

/// The local process identity bound to a QUIC leaf certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointIdentity {
    pid: Pid,
}

impl EndpointIdentity {
    /// Derives a self-certifying process identity from a DER leaf certificate.
    pub fn from_leaf_certificate(certificate: &[u8]) -> Self {
        Self {
            pid: certificate_pid(certificate),
        }
    }

    /// Returns the local self-certifying process ID.
    pub const fn pid(self) -> Pid {
        self.pid
    }
}

/// Derives a 128-bit process ID from an authenticated DER leaf certificate.
pub fn certificate_pid(certificate: &[u8]) -> Pid {
    let digest = Sha256::digest(certificate);
    let mut pid = [0; PID_LEN];
    pid.copy_from_slice(&digest[..PID_LEN]);
    Pid::from_bytes(pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_identity_is_stable_and_certificate_specific() {
        let first = EndpointIdentity::from_leaf_certificate(b"first certificate");
        let same = EndpointIdentity::from_leaf_certificate(b"first certificate");
        let second = EndpointIdentity::from_leaf_certificate(b"second certificate");

        assert_eq!(first, same);
        assert_ne!(first, second);
        assert!(!first.pid().is_link_local());
    }
}
