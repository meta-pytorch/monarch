/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Packet I/O boundary between the QUIC state machine and transport drivers.
//!
//! UDP completion ownership is asymmetric. A received event holds its stable
//! receive slot until this adapter releases it after delivery, including when
//! delivery returns an error. A transmitted event is only a notification:
//! [`UdpDriver`] recycles its stable transmit slot before enqueuing the event,
//! so this adapter can discard it without further cleanup. Kernel transmit
//! completion does not acknowledge QUIC stream data; quiche tracks that
//! lifecycle separately.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use chrysalis_transport_core::Notifier;
use chrysalis_transport_uring::IoEvent;
use chrysalis_transport_uring::SendSlot;
use chrysalis_transport_uring::UdpDriver;

use crate::Error;

/// One driver-owned transmit allocation filled directly by quiche.
pub trait PacketSendSlot {
    /// Returns the allocation available for packet or GSO assembly.
    fn buffer_mut(&mut self) -> &mut [u8];

    /// Queues the initialized prefix for transmission.
    fn submit(
        self: Box<Self>,
        length: usize,
        destination: SocketAddr,
        send_at: Instant,
    ) -> io::Result<()>;
}

/// Runtime-neutral packet I/O consumed by one quiche endpoint thread.
pub trait PacketIo: Send {
    /// Returns whether an authenticated upstream boundary validated source addresses.
    fn peer_addresses_validated(&self) -> bool {
        false
    }

    /// Returns the local address represented in quiche path metadata.
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Returns the maximum QUIC datagram size.
    fn segment_size(&self) -> usize;

    /// Returns the maximum datagrams assembled into one transmit allocation.
    fn max_gso_segments(&self) -> usize;

    /// Returns a notifier that interrupts a blocked [`Self::poll`].
    fn notifier(&self) -> Arc<dyn Notifier>;

    /// Borrows a free transmit allocation.
    fn try_send_slot(&mut self) -> Option<Box<dyn PacketSendSlot + '_>>;

    /// Waits for packet I/O, pacing, or an application notification.
    fn poll(&mut self, timeout: Duration) -> io::Result<()>;

    /// Delivers every completed receive datagram to `receive`.
    ///
    /// Packet transmit completions are not surfaced. Their packet slots have
    /// already been recycled, and QUIC acknowledgements are tracked separately.
    fn drain_received(
        &mut self,
        receive: &mut dyn FnMut(&mut [u8], SocketAddr, SocketAddr) -> Result<(), Error>,
    ) -> Result<(), Error>;
}

struct UringSendSlot<'a>(SendSlot<'a>);

impl PacketSendSlot for UringSendSlot<'_> {
    fn buffer_mut(&mut self) -> &mut [u8] {
        self.0.buffer_mut()
    }

    fn submit(
        self: Box<Self>,
        length: usize,
        destination: SocketAddr,
        send_at: Instant,
    ) -> io::Result<()> {
        let Self(slot) = *self;
        slot.submit(length, destination, send_at)
    }
}

impl PacketIo for UdpDriver {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        UdpDriver::local_addr(self)
    }

    fn segment_size(&self) -> usize {
        UdpDriver::segment_size(self)
    }

    fn max_gso_segments(&self) -> usize {
        UdpDriver::max_gso_segments(self)
    }

    fn notifier(&self) -> Arc<dyn Notifier> {
        Arc::new(UdpDriver::notifier(self))
    }

    fn try_send_slot(&mut self) -> Option<Box<dyn PacketSendSlot + '_>> {
        UdpDriver::try_send_slot(self)
            .map(|slot| Box::new(UringSendSlot(slot)) as Box<dyn PacketSendSlot>)
    }

    fn poll(&mut self, timeout: Duration) -> io::Result<()> {
        UdpDriver::poll(self, timeout).map(|_| ())
    }

    fn drain_received(
        &mut self,
        receive: &mut dyn FnMut(&mut [u8], SocketAddr, SocketAddr) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let local = self.local_addr()?;
        let mut first_error = None;
        while let Some(event) = self.try_next_event() {
            let datagram = match event {
                IoEvent::Received(datagram) => datagram,
                IoEvent::Transmitted(_) => {
                    // UdpDriver recycled the slot before enqueuing this notification.
                    continue;
                }
            };
            let source = datagram.source();
            let segment_size = datagram.segment_size();
            let length = datagram.len();
            let result = if length == 0 {
                Ok(())
            } else if segment_size == 0 {
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "nonempty UDP aggregate has zero segment size",
                )))
            } else {
                let aggregate = self.received(datagram);
                let mut result = Ok(());
                for packet in aggregate[..length].chunks_mut(segment_size) {
                    if let Err(error) = receive(packet, source, local)
                        && result.is_ok()
                    {
                        result = Err(error);
                    }
                }
                result
            };
            self.release_receive(datagram.slot());
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
