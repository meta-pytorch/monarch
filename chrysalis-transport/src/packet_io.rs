/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Adapts carrier-neutral Chrysalis datagrams to quiche's IP-shaped packet interface.
//!
//! Quiche identifies a network path by a pair of `SocketAddr` values. Its connection constructors,
//! receive metadata, and send metadata all require concrete IP socket addresses. Chrysalis carriers
//! instead use `DatagramAddr`, whose scheme and opaque bytes can name Unix sockets, in-process
//! endpoints, switched routes, or UDP addresses. Those addresses cannot be represented faithfully as
//! `SocketAddr`, even though quiche still needs stable values for path identity, validation, and
//! packet generation. Removing this translation would require changing quiche to accept an abstract
//! address type and carrying that change as an upstream patch or fork.
//!
//! `CarrierAddressBook` therefore gives each carrier address an endpoint-local synthetic IPv6 socket
//! address. ID 1 represents the local endpoint, peer IDs begin at 2, and every address uses the fixed
//! synthetic port. The two maps in `AddressTable` preserve a stable bijection for the lifetime of the
//! packet driver. `QuicTransport` shares the address book with `CarrierPacketIo`, so an outbound dial
//! registers its `DatagramAddr` before giving the resulting synthetic peer to quiche.
//!
//! On receive, `CarrierPacketIo` registers the carrier-reported source and passes its synthetic source
//! plus the synthetic local address to quiche. On send, quiche returns that synthetic destination in
//! its send metadata; the adapter resolves it back to the original `DatagramAddr` immediately before
//! calling the carrier. The synthetic addresses never appear in QUIC packets or reach an operating
//! system socket. `DatagramAddr` and QUIC connection IDs remain responsible for actual carrier and
//! connection routing.
//!
//! The mapping must remain stable, one-to-one, and scoped to one endpoint. Reassigning a synthetic
//! address while quiche retains path state would merge distinct peers or split one path. Accepting an
//! unknown synthetic destination would bypass the reverse mapping, so transmission fails instead.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::io::IoSliceMut;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use chrysalis_transport_core::Notifier;
use chrysalis_transport_quiche::Error;
use chrysalis_transport_quiche::PacketIo;
use chrysalis_transport_quiche::PacketSendSlot;

use crate::DatagramAddr;
use crate::DatagramRecvMeta;
use crate::DatagramSocket;
use crate::DatagramTransmit;

const SYNTHETIC_PORT: u16 = 1;
const LOCAL_ADDRESS_ID: u128 = 1;
const FIRST_PEER_ADDRESS_ID: u128 = 2;
const TRANSMIT_DEPTH: usize = 8;

#[derive(Debug, Default)]
pub(crate) struct CarrierIoStats {
    transmit_calls: AtomicU64,
    transmit_datagrams: AtomicU64,
    transmit_bytes: AtomicU64,
    transmit_blocked: AtomicU64,
    receive_calls: AtomicU64,
    receive_datagrams: AtomicU64,
    receive_bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CarrierIoStatsSnapshot {
    pub(crate) transmit_calls: u64,
    pub(crate) transmit_datagrams: u64,
    pub(crate) transmit_bytes: u64,
    pub(crate) transmit_blocked: u64,
    pub(crate) receive_calls: u64,
    pub(crate) receive_datagrams: u64,
    pub(crate) receive_bytes: u64,
}

impl CarrierIoStats {
    pub(crate) fn snapshot(&self) -> CarrierIoStatsSnapshot {
        CarrierIoStatsSnapshot {
            transmit_calls: self.transmit_calls.load(Ordering::Relaxed),
            transmit_datagrams: self.transmit_datagrams.load(Ordering::Relaxed),
            transmit_bytes: self.transmit_bytes.load(Ordering::Relaxed),
            transmit_blocked: self.transmit_blocked.load(Ordering::Relaxed),
            receive_calls: self.receive_calls.load(Ordering::Relaxed),
            receive_datagrams: self.receive_datagrams.load(Ordering::Relaxed),
            receive_bytes: self.receive_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CarrierAddressBook {
    inner: Arc<Mutex<AddressTable>>,
}

impl CarrierAddressBook {
    pub(crate) fn register(&self, address: DatagramAddr) -> SocketAddr {
        self.inner
            .lock()
            .expect("carrier address table lock poisoned")
            .register(address)
    }
}

#[derive(Debug)]
struct AddressTable {
    next_id: u128,
    by_address: HashMap<DatagramAddr, SocketAddr>,
    by_synthetic: HashMap<SocketAddr, DatagramAddr>,
}

impl AddressTable {
    fn register(&mut self, address: DatagramAddr) -> SocketAddr {
        if let Some(synthetic) = self.by_address.get(&address) {
            return *synthetic;
        }
        let synthetic = synthetic_addr(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("carrier address ID exhausted");
        self.by_address.insert(address.clone(), synthetic);
        self.by_synthetic.insert(synthetic, address);
        synthetic
    }

    fn resolve(&self, address: SocketAddr) -> io::Result<DatagramAddr> {
        self.by_synthetic.get(&address).cloned().ok_or_else(|| {
            io::Error::new(io::ErrorKind::AddrNotAvailable, "unknown carrier address")
        })
    }
}

impl Default for AddressTable {
    fn default() -> Self {
        Self {
            next_id: FIRST_PEER_ADDRESS_ID,
            by_address: HashMap::new(),
            by_synthetic: HashMap::new(),
        }
    }
}

#[derive(Debug, Default)]
struct WakeState {
    notified: Mutex<bool>,
    ready: Condvar,
}

impl WakeState {
    fn wait(&self, timeout: Duration) {
        let mut notified = self.notified.lock().expect("packet wake lock poisoned");
        if !*notified {
            let (guard, _) = self
                .ready
                .wait_timeout(notified, timeout)
                .expect("packet wake lock poisoned");
            notified = guard;
        }
        *notified = false;
    }

    fn signal(&self) {
        let mut notified = self.notified.lock().expect("packet wake lock poisoned");
        *notified = true;
        self.ready.notify_one();
    }
}

impl Notifier for WakeState {
    fn notify(&self) {
        self.signal();
    }
}

impl Wake for WakeState {
    fn wake(self: Arc<Self>) {
        self.signal();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}

#[derive(Debug)]
struct PendingTransmit {
    buffer: Vec<u8>,
    offset: usize,
    length: usize,
    destination: SocketAddr,
    send_at: Instant,
}

#[derive(Debug)]
struct ReceivedAggregate {
    buffer: Vec<u8>,
    length: usize,
    stride: usize,
    source: SocketAddr,
}

/// Packet driver over one carrier-neutral datagram socket.
#[derive(Debug)]
pub(crate) struct CarrierPacketIo<T> {
    socket: Arc<T>,
    addresses: CarrierAddressBook,
    wake: Arc<WakeState>,
    local: SocketAddr,
    segment_size: usize,
    max_segments: usize,
    free_transmit: Vec<Vec<u8>>,
    pending_transmit: VecDeque<PendingTransmit>,
    receive_buffer: Vec<u8>,
    receive_meta: DatagramRecvMeta,
    free_receive: Vec<Vec<u8>>,
    received: VecDeque<ReceivedAggregate>,
    stats: Arc<CarrierIoStats>,
}

impl<T: DatagramSocket> CarrierPacketIo<T> {
    pub(crate) fn new(
        socket: Arc<T>,
        segment_size: usize,
        max_segments: usize,
    ) -> (Self, CarrierAddressBook, Arc<CarrierIoStats>) {
        assert!(segment_size > 0, "packet segment size must be nonzero");
        assert!(max_segments > 0, "packet segment count must be nonzero");
        let max_segments = max_segments.min(socket.max_transmit_segments().max(1));
        let addresses = CarrierAddressBook {
            inner: Arc::new(Mutex::new(AddressTable::default())),
        };
        let capacity = segment_size
            .checked_mul(max_segments)
            .expect("carrier packet allocation exceeds usize");
        let free_transmit = (0..TRANSMIT_DEPTH).map(|_| vec![0; capacity]).collect();
        let stats = Arc::new(CarrierIoStats::default());
        (
            Self {
                socket,
                addresses: addresses.clone(),
                wake: Arc::new(WakeState::default()),
                local: synthetic_addr(LOCAL_ADDRESS_ID),
                segment_size,
                max_segments,
                free_transmit,
                pending_transmit: VecDeque::new(),
                receive_buffer: vec![0; capacity],
                receive_meta: DatagramRecvMeta::default(),
                free_receive: Vec::new(),
                received: VecDeque::new(),
                stats: stats.clone(),
            },
            addresses,
            stats,
        )
    }

    fn poll_once(&mut self) -> io::Result<()> {
        let waker = Waker::from(self.wake.clone());
        let mut context = Context::from_waker(&waker);
        self.flush_transmits(&mut context)?;
        let mut buffers = [IoSliceMut::new(&mut self.receive_buffer)];
        let meta = std::slice::from_mut(&mut self.receive_meta);
        match self.socket.poll_recv(&mut context, &mut buffers, meta) {
            Poll::Ready(Ok(0)) | Poll::Pending => {}
            Poll::Ready(Ok(1)) => {
                self.stats.receive_calls.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .receive_bytes
                    .fetch_add(self.receive_meta.len as u64, Ordering::Relaxed);
                let stride = self.receive_meta.stride.max(1);
                self.stats.receive_datagrams.fetch_add(
                    self.receive_meta.len.div_ceil(stride) as u64,
                    Ordering::Relaxed,
                );
                let source = self.addresses.register(self.receive_meta.source.clone());
                let mut buffer = self.free_receive.pop().unwrap_or_default();
                buffer.clear();
                buffer.extend_from_slice(&self.receive_buffer[..self.receive_meta.len]);
                self.received.push_back(ReceivedAggregate {
                    buffer,
                    length: self.receive_meta.len,
                    stride: self.receive_meta.stride.max(1),
                    source,
                });
            }
            Poll::Ready(Ok(count)) => panic!("single-buffer receive returned {count} datagrams"),
            Poll::Ready(Err(error)) => return Err(error),
        }
        Ok(())
    }

    fn flush_transmits(&mut self, context: &mut Context<'_>) -> io::Result<()> {
        loop {
            let Some(pending) = self.pending_transmit.front() else {
                return Ok(());
            };
            if pending.send_at > Instant::now() {
                return Ok(());
            }
            let destination = self
                .addresses
                .inner
                .lock()
                .expect("carrier address table lock poisoned")
                .resolve(pending.destination)?;
            let segment_size = (pending.length > self.segment_size).then_some(self.segment_size);
            let transmit = DatagramTransmit {
                destination: &destination,
                contents: &pending.buffer[pending.offset..pending.length],
                segment_size,
                ecn: None,
                source_ip: None,
            };
            match self.socket.try_send_transmit(&transmit) {
                Ok(0) => {
                    self.stats.transmit_calls.fetch_add(1, Ordering::Relaxed);
                    self.stats.transmit_blocked.fetch_add(1, Ordering::Relaxed);
                    let _ = self.socket.poll_send_ready(context, &transmit);
                    return Ok(());
                }
                Ok(accepted) => {
                    let segment_count = transmit.segment_count();
                    assert!(
                        accepted <= segment_count,
                        "datagram socket accepted more segments than were offered"
                    );
                    let remaining = pending.length - pending.offset;
                    let accepted_bytes = remaining.min(accepted * self.segment_size);
                    self.stats.transmit_calls.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .transmit_bytes
                        .fetch_add(accepted_bytes as u64, Ordering::Relaxed);
                    self.stats
                        .transmit_datagrams
                        .fetch_add(accepted as u64, Ordering::Relaxed);
                    if accepted == segment_count {
                        let pending = self.pending_transmit.pop_front().unwrap();
                        self.free_transmit.push(pending.buffer);
                    } else {
                        self.pending_transmit
                            .front_mut()
                            .expect("pending transmit remains queued")
                            .offset += accepted_bytes;
                    }
                }
                Err(error) if is_peer_delivery_error(&error) => {
                    self.stats.transmit_calls.fetch_add(1, Ordering::Relaxed);
                    let pending = self.pending_transmit.pop_front().unwrap();
                    self.free_transmit.push(pending.buffer);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn next_pacing_wait(&self, timeout: Duration) -> Duration {
        self.pending_transmit.front().map_or(timeout, |pending| {
            timeout.min(pending.send_at.saturating_duration_since(Instant::now()))
        })
    }
}

struct CarrierSendSlot<'a, T> {
    io: &'a mut CarrierPacketIo<T>,
    buffer: Option<Vec<u8>>,
}

impl<T> PacketSendSlot for CarrierSendSlot<'_, T> {
    fn buffer_mut(&mut self) -> &mut [u8] {
        self.buffer
            .as_mut()
            .expect("carrier send slot was submitted")
    }

    fn submit(
        mut self: Box<Self>,
        length: usize,
        destination: SocketAddr,
        send_at: Instant,
    ) -> io::Result<()> {
        let buffer = self.buffer.take().expect("carrier send slot was submitted");
        if length > buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet exceeds carrier send slot",
            ));
        }
        let position = self
            .io
            .pending_transmit
            .iter()
            .position(|pending| pending.send_at > send_at)
            .unwrap_or(self.io.pending_transmit.len());
        self.io.pending_transmit.insert(
            position,
            PendingTransmit {
                buffer,
                offset: 0,
                length,
                destination,
                send_at,
            },
        );
        self.io.wake.signal();
        Ok(())
    }
}

impl<T> Drop for CarrierSendSlot<'_, T> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.io.free_transmit.push(buffer);
        }
    }
}

impl<T: DatagramSocket> PacketIo for CarrierPacketIo<T> {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }

    fn segment_size(&self) -> usize {
        self.segment_size
    }

    fn max_gso_segments(&self) -> usize {
        self.max_segments
    }

    fn notifier(&self) -> Arc<dyn Notifier> {
        self.wake.clone()
    }

    fn try_send_slot(&mut self) -> Option<Box<dyn PacketSendSlot + '_>> {
        let buffer = self.free_transmit.pop()?;
        Some(Box::new(CarrierSendSlot {
            io: self,
            buffer: Some(buffer),
        }))
    }

    fn poll(&mut self, timeout: Duration) -> io::Result<()> {
        self.poll_once()?;
        if self.received.is_empty() {
            self.wake.wait(self.next_pacing_wait(timeout));
            self.poll_once()?;
        }
        Ok(())
    }

    fn drain_received(
        &mut self,
        receive: &mut dyn FnMut(&mut [u8], SocketAddr, SocketAddr) -> Result<(), Error>,
    ) -> Result<(), Error> {
        while let Some(mut aggregate) = self.received.pop_front() {
            let mut result = Ok(());
            for packet in aggregate.buffer[..aggregate.length].chunks_mut(aggregate.stride) {
                if let Err(error) = receive(packet, aggregate.source, self.local) {
                    result = Err(error);
                    break;
                }
            }
            aggregate.buffer.clear();
            self.free_receive.push(aggregate.buffer);
            result?;
        }
        Ok(())
    }
}

fn is_peer_delivery_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
    )
}

fn synthetic_addr(id: u128) -> SocketAddr {
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(id), SYNTHETIC_PORT, 0, 0))
}
