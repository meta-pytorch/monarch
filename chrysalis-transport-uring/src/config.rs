/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;

/// Fixed capacities and socket options for one UDP driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverConfig {
    ring_depth: NonZeroU32,
    receive_depth: NonZeroUsize,
    segment_size: NonZeroUsize,
    max_gso_segments: NonZeroUsize,
    socket_buffer_bytes: NonZeroUsize,
    gro: bool,
}

impl DriverConfig {
    /// Constructs a driver configuration.
    pub const fn new(
        ring_depth: NonZeroU32,
        receive_depth: NonZeroUsize,
        segment_size: NonZeroUsize,
        max_gso_segments: NonZeroUsize,
        socket_buffer_bytes: NonZeroUsize,
        gro: bool,
    ) -> Self {
        Self {
            ring_depth,
            receive_depth,
            segment_size,
            max_gso_segments,
            socket_buffer_bytes,
            gro,
        }
    }

    /// Returns the io_uring submission and completion queue depth.
    pub const fn ring_depth(self) -> NonZeroU32 {
        self.ring_depth
    }

    /// Returns the number of UDP receives kept in flight.
    pub const fn receive_depth(self) -> NonZeroUsize {
        self.receive_depth
    }

    /// Returns the maximum QUIC datagram size and UDP GSO segment size.
    pub const fn segment_size(self) -> NonZeroUsize {
        self.segment_size
    }

    /// Returns the maximum datagrams assembled into one UDP GSO send.
    pub const fn max_gso_segments(self) -> NonZeroUsize {
        self.max_gso_segments
    }

    /// Returns the requested kernel socket send and receive buffer size.
    pub const fn socket_buffer_bytes(self) -> NonZeroUsize {
        self.socket_buffer_bytes
    }

    /// Returns whether UDP GRO is enabled.
    pub const fn gro(self) -> bool {
        self.gro
    }

    pub(crate) fn validate(self) -> io::Result<()> {
        if self.ring_depth.get() < 8 {
            return Err(invalid_input("ring depth must be at least 8"));
        }
        if self.receive_depth.get() + 2 >= self.ring_depth.get() as usize {
            return Err(invalid_input(
                "receive depth must leave at least two ring entries for sends and waits",
            ));
        }
        if self.segment_size.get() > u16::MAX as usize {
            return Err(invalid_input("UDP GSO segment size exceeds u16"));
        }
        let aggregate_size = self
            .segment_size
            .get()
            .checked_mul(self.max_gso_segments.get())
            .ok_or_else(|| invalid_input("transmit slot size overflows usize"))?;
        if aggregate_size > u16::MAX as usize {
            return Err(invalid_input("UDP GSO aggregate exceeds 65535 bytes"));
        }
        Ok(())
    }
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self::new(
            NonZeroU32::new(256).expect("default ring depth is nonzero"),
            NonZeroUsize::new(64).expect("default receive depth is nonzero"),
            NonZeroUsize::new(1450).expect("default segment size is nonzero"),
            NonZeroUsize::new(12).expect("default GSO count is nonzero"),
            NonZeroUsize::new(64 * 1024 * 1024).expect("default socket buffer is nonzero"),
            true,
        )
    }
}

pub(crate) fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_preserves_ring_space_for_progress() {
        let config = DriverConfig::new(
            NonZeroU32::new(8).unwrap(),
            NonZeroUsize::new(6).unwrap(),
            NonZeroUsize::new(1200).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1024).unwrap(),
            false,
        );

        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn validation_rejects_an_oversized_gso_aggregate() {
        let config = DriverConfig::new(
            NonZeroU32::new(16).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(2000).unwrap(),
            NonZeroUsize::new(33).unwrap(),
            NonZeroUsize::new(1024).unwrap(),
            false,
        );

        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
