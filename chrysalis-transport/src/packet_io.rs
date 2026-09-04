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

use chrysalis_core::Pid;
use chrysalis_core::target_pid;
use chrysalis_transport_core::Notifier;
use chrysalis_transport_quiche::Error;
use chrysalis_transport_quiche::PacketIo;
use chrysalis_transport_quiche::PacketSendSlot;
use chrysalis_transport_uring::DriverStatsHandle;
use chrysalis_transport_uring::IoEvent;
use chrysalis_transport_uring::SendSlot;
use chrysalis_transport_uring::UdpDriver;

use crate::DatagramAddr;
use crate::DatagramRecvMeta;
use crate::DatagramSocket;
use crate::DatagramTransmit;
use crate::Route;
use crate::Router;
use crate::udp::decode_udp_addr;

const SYNTHETIC_PORT: u16 = 1;
const LOCAL_ADDRESS_ID: u128 = 1;
const FIRST_PEER_ADDRESS_ID: u128 = 2;
const TRANSMIT_DEPTH: usize = 8;
const FALLBACK_RECEIVE_CAPACITY: usize = u16::MAX as usize;
const FALLBACK_RECEIVE_BATCH_SIZE: usize = 32;
const TRANSIT_QUEUE_CAPACITY: usize = 256;
const MAX_ROUTED_GSO_SEGMENTS: usize = 8;

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

#[derive(Clone, Debug)]
pub(crate) struct RoutedUdpIoStats {
    udp: DriverStatsHandle,
    fallback: Arc<CarrierIoStats>,
}

impl RoutedUdpIoStats {
    pub(crate) fn snapshot(&self) -> CarrierIoStatsSnapshot {
        let udp = self.udp.snapshot();
        let fallback = self.fallback.snapshot();
        CarrierIoStatsSnapshot {
            transmit_calls: udp.sends_completed + fallback.transmit_calls,
            transmit_datagrams: udp.datagrams_sent + fallback.transmit_datagrams,
            transmit_bytes: udp.bytes_sent + fallback.transmit_bytes,
            transmit_blocked: fallback.transmit_blocked,
            receive_calls: udp.receives_completed + fallback.receive_calls,
            receive_datagrams: udp.datagrams_received + fallback.receive_datagrams,
            receive_bytes: udp.bytes_received + fallback.receive_bytes,
        }
    }
}

#[derive(Debug)]
enum RoutedDestination {
    LinkLocal(DatagramAddr),
    Route(Route),
}

impl RoutedDestination {
    fn address(&self) -> &DatagramAddr {
        match self {
            Self::LinkLocal(address) => address,
            Self::Route(route) => route.destination(),
        }
    }

    fn try_with_address<T>(
        &self,
        use_address: impl FnOnce(&DatagramAddr) -> io::Result<T>,
    ) -> io::Result<Option<T>> {
        match self {
            Self::LinkLocal(address) => use_address(address).map(Some),
            Self::Route(route) => route.try_with_destination(use_address),
        }
    }
}

#[derive(Debug)]
struct FallbackTransmit {
    bytes: Arc<Vec<u8>>,
    offset: usize,
    end: usize,
    segment_size: usize,
    destination: RoutedDestination,
    send_at: Instant,
}

#[derive(Debug)]
struct TransitTransmit {
    bytes: Arc<Vec<u8>>,
    offset: usize,
    length: usize,
    destination: Route,
}

#[derive(Clone, Copy, Debug)]
struct TransitSlice {
    offset: usize,
    length: usize,
    segment_size: usize,
    target: Pid,
}

#[derive(Debug)]
struct FallbackAggregate {
    bytes: Vec<u8>,
    source: SocketAddr,
    stride: usize,
}

pub(crate) struct RoutedUdpPacketIo {
    udp: UdpDriver,
    local_pid: Pid,
    router: Arc<Router>,
    fallback: Option<Arc<dyn DatagramSocket>>,
    addresses: CarrierAddressBook,
    local: SocketAddr,
    segment_size: usize,
    max_segments: usize,
    transit_transmits: VecDeque<TransitTransmit>,
    fallback_transmits: VecDeque<FallbackTransmit>,
    fallback_send_blocked: bool,
    fallback_receive_buffer: Vec<u8>,
    fallback_receive_meta: DatagramRecvMeta,
    fallback_received: VecDeque<FallbackAggregate>,
    free_transit_buffers: Vec<Vec<u8>>,
    fallback_stats: Arc<CarrierIoStats>,
    wake: Arc<NotifierWake>,
}

impl RoutedUdpPacketIo {
    pub(crate) fn new(
        udp: UdpDriver,
        local_pid: Pid,
        router: Arc<Router>,
        fallback: Option<Arc<dyn DatagramSocket>>,
    ) -> (Self, CarrierAddressBook, RoutedUdpIoStats) {
        let segment_size = udp.segment_size();
        let max_segments = udp.max_gso_segments().min(MAX_ROUTED_GSO_SEGMENTS);
        let udp_stats = udp.stats_handle();
        let addresses = CarrierAddressBook {
            inner: Arc::new(Mutex::new(AddressTable::default())),
        };
        let fallback_stats = Arc::new(CarrierIoStats::default());
        let wake = Arc::new(NotifierWake(udp.notifier()));
        let stats = RoutedUdpIoStats {
            udp: udp_stats,
            fallback: fallback_stats.clone(),
        };
        (
            Self {
                udp,
                local_pid,
                router,
                fallback,
                addresses: addresses.clone(),
                local: synthetic_addr(LOCAL_ADDRESS_ID),
                segment_size,
                max_segments,
                transit_transmits: VecDeque::new(),
                fallback_transmits: VecDeque::new(),
                fallback_send_blocked: false,
                fallback_receive_buffer: vec![0; FALLBACK_RECEIVE_CAPACITY],
                fallback_receive_meta: DatagramRecvMeta::default(),
                fallback_received: VecDeque::new(),
                free_transit_buffers: Vec::new(),
                fallback_stats,
                wake,
            },
            addresses,
            stats,
        )
    }

    fn enqueue_transit(
        &mut self,
        bytes: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
        segment_size: usize,
        target: Pid,
    ) -> bool {
        let Some(end) = offset.checked_add(length).filter(|end| *end <= bytes.len()) else {
            return false;
        };
        if segment_size == 0 {
            return false;
        }
        let Some(route) = self
            .router
            .get(target)
            .or_else(|| self.router.default_route())
        else {
            return false;
        };
        if route.destination().scheme() == "udp" {
            if segment_size > self.segment_size {
                return false;
            }
            let batch_size = if segment_size == self.segment_size {
                self.segment_size * self.max_segments
            } else {
                segment_size
            };
            let batch_count = length.div_ceil(batch_size);
            if self
                .transit_transmits
                .len()
                .checked_add(batch_count)
                .is_none_or(|count| count > TRANSIT_QUEUE_CAPACITY)
            {
                return false;
            }
            for batch_offset in (offset..end).step_by(batch_size) {
                self.transit_transmits.push_back(TransitTransmit {
                    bytes: bytes.clone(),
                    offset: batch_offset,
                    length: (end - batch_offset).min(batch_size),
                    destination: route.clone(),
                });
            }
        } else {
            if self.fallback.is_none() {
                return false;
            }
            if self.fallback_transmits.len() >= TRANSIT_QUEUE_CAPACITY {
                return false;
            }
            let pending = FallbackTransmit {
                bytes,
                offset,
                end,
                segment_size,
                destination: RoutedDestination::Route(route),
                send_at: Instant::now(),
            };
            let position = self
                .fallback_transmits
                .iter()
                .position(|queued| queued.send_at > pending.send_at)
                .unwrap_or(self.fallback_transmits.len());
            self.fallback_transmits.insert(position, pending);
        }
        true
    }

    fn recycle_transit_buffer(&mut self, bytes: Arc<Vec<u8>>) {
        if self.free_transit_buffers.len() < TRANSIT_QUEUE_CAPACITY
            && let Ok(mut bytes) = Arc::try_unwrap(bytes)
        {
            bytes.clear();
            self.free_transit_buffers.push(bytes);
        }
    }

    fn flush_transit_transmits(&mut self) -> io::Result<()> {
        loop {
            let Some(pending) = self.transit_transmits.front() else {
                return Ok(());
            };
            let Some(mut slot) = self.udp.try_send_slot() else {
                return Ok(());
            };
            if pending.length > slot.buffer_mut().len() {
                drop(slot);
                let pending = self
                    .transit_transmits
                    .pop_front()
                    .expect("oversized transit remains queued");
                self.recycle_transit_buffer(pending.bytes);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "transit batch exceeds the routed UDP send slot",
                ));
            }
            let end = pending.offset + pending.length;
            slot.buffer_mut()[..pending.length]
                .copy_from_slice(&pending.bytes[pending.offset..end]);
            let length = pending.length;
            let result = pending.destination.try_with_destination(|destination| {
                slot.submit(length, decode_udp_addr(destination)?, Instant::now())
            });
            match result {
                Ok(Some(())) | Ok(None) => {
                    let pending = self
                        .transit_transmits
                        .pop_front()
                        .expect("completed transit remains queued");
                    self.recycle_transit_buffer(pending.bytes);
                }
                Err(error) if is_peer_delivery_error(&error) => {
                    let pending = self
                        .transit_transmits
                        .pop_front()
                        .expect("failed transit remains queued");
                    self.recycle_transit_buffer(pending.bytes);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn flush_fallback_transmits(&mut self, context: &mut Context<'_>) -> io::Result<()> {
        let Some(socket) = self.fallback.clone() else {
            self.fallback_transmits.clear();
            self.fallback_send_blocked = false;
            return Ok(());
        };
        loop {
            let Some(pending) = self.fallback_transmits.front() else {
                self.fallback_send_blocked = false;
                return Ok(());
            };
            if pending.send_at > Instant::now() {
                self.fallback_send_blocked = false;
                return Ok(());
            }
            let remaining = pending.end - pending.offset;
            let segment_size = (remaining > pending.segment_size).then_some(pending.segment_size);
            let result = pending.destination.try_with_address(|destination| {
                let transmit = DatagramTransmit {
                    destination,
                    contents: &pending.bytes[pending.offset..pending.end],
                    segment_size,
                    ecn: None,
                    source_ip: None,
                };
                let accepted = socket.try_send_transmit(&transmit)?;
                if accepted == 0 {
                    let _ = socket.poll_send_ready(context, &transmit);
                }
                Ok(accepted)
            });
            match result {
                Ok(Some(0)) => {
                    self.fallback_stats
                        .transmit_calls
                        .fetch_add(1, Ordering::Relaxed);
                    self.fallback_stats
                        .transmit_blocked
                        .fetch_add(1, Ordering::Relaxed);
                    self.fallback_send_blocked = true;
                    return Ok(());
                }
                Ok(Some(accepted)) => {
                    self.fallback_send_blocked = false;
                    let segment_count = remaining.div_ceil(pending.segment_size);
                    assert!(
                        accepted <= segment_count,
                        "datagram socket accepted more segments than were offered"
                    );
                    let accepted_bytes = remaining.min(accepted * pending.segment_size);
                    self.fallback_stats
                        .transmit_calls
                        .fetch_add(1, Ordering::Relaxed);
                    self.fallback_stats
                        .transmit_datagrams
                        .fetch_add(accepted as u64, Ordering::Relaxed);
                    self.fallback_stats
                        .transmit_bytes
                        .fetch_add(accepted_bytes as u64, Ordering::Relaxed);
                    if accepted == segment_count {
                        let pending = self
                            .fallback_transmits
                            .pop_front()
                            .expect("completed fallback transmit remains queued");
                        self.recycle_transit_buffer(pending.bytes);
                    } else {
                        self.fallback_transmits
                            .front_mut()
                            .expect("fallback transmit remains queued")
                            .offset += accepted_bytes;
                    }
                }
                Ok(None) => {
                    self.fallback_send_blocked = false;
                    let pending = self
                        .fallback_transmits
                        .pop_front()
                        .expect("withdrawn fallback transmit remains queued");
                    self.recycle_transit_buffer(pending.bytes);
                }
                Err(error) if is_peer_delivery_error(&error) => {
                    self.fallback_send_blocked = false;
                    let pending = self
                        .fallback_transmits
                        .pop_front()
                        .expect("failed fallback transmit remains queued");
                    self.recycle_transit_buffer(pending.bytes);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn poll_fallback(&mut self, context: &mut Context<'_>) -> io::Result<bool> {
        let Some(socket) = self.fallback.as_deref() else {
            return Ok(false);
        };
        let mut received = 0;
        while self.fallback_received.len() < TRANSIT_QUEUE_CAPACITY
            && received < FALLBACK_RECEIVE_BATCH_SIZE
        {
            let mut buffers = [IoSliceMut::new(&mut self.fallback_receive_buffer)];
            let meta = std::slice::from_mut(&mut self.fallback_receive_meta);
            match socket.poll_recv(context, &mut buffers, meta) {
                Poll::Ready(Ok(0)) | Poll::Pending => return Ok(received > 0),
                Poll::Ready(Ok(1)) => {
                    let length = self.fallback_receive_meta.len;
                    let stride = self.fallback_receive_meta.stride.max(1);
                    self.fallback_stats
                        .receive_calls
                        .fetch_add(1, Ordering::Relaxed);
                    self.fallback_stats
                        .receive_datagrams
                        .fetch_add(length.div_ceil(stride) as u64, Ordering::Relaxed);
                    self.fallback_stats
                        .receive_bytes
                        .fetch_add(length as u64, Ordering::Relaxed);
                    let source = self
                        .addresses
                        .register(self.fallback_receive_meta.source.clone());
                    let mut bytes = self.free_transit_buffers.pop().unwrap_or_default();
                    bytes.clear();
                    bytes.extend_from_slice(&self.fallback_receive_buffer[..length]);
                    self.fallback_received.push_back(FallbackAggregate {
                        bytes,
                        source,
                        stride,
                    });
                    received += 1;
                }
                Poll::Ready(Ok(count)) => {
                    panic!("single-buffer fallback returned {count} datagrams")
                }
                Poll::Ready(Err(error)) => return Err(error),
            }
        }
        Ok(received > 0)
    }

    fn next_fallback_wait(&self, timeout: Duration) -> Duration {
        if self.fallback_send_blocked {
            return timeout;
        }
        self.fallback_transmits.front().map_or(timeout, |pending| {
            timeout.min(pending.send_at.saturating_duration_since(Instant::now()))
        })
    }
}

struct RoutedUdpSendSlot<'a> {
    slot: Option<SendSlot<'a>>,
    router: &'a Router,
    addresses: &'a CarrierAddressBook,
    fallback: Option<&'a dyn DatagramSocket>,
    fallback_transmits: &'a mut VecDeque<FallbackTransmit>,
    segment_size: usize,
}

impl PacketSendSlot for RoutedUdpSendSlot<'_> {
    fn buffer_mut(&mut self) -> &mut [u8] {
        self.slot
            .as_mut()
            .expect("routed UDP send slot was submitted")
            .buffer_mut()
    }

    fn submit(
        mut self: Box<Self>,
        length: usize,
        peer: SocketAddr,
        send_at: Instant,
    ) -> io::Result<()> {
        let mut slot = self
            .slot
            .take()
            .expect("routed UDP send slot was submitted");
        let segment_size = self.segment_size.min(length);
        let Some(target) = target_pid(&slot.buffer_mut()[..segment_size]) else {
            return Ok(());
        };
        let destination = if target.is_link_local() {
            self.addresses
                .inner
                .lock()
                .expect("carrier address table lock poisoned")
                .resolve(peer)
                .map(RoutedDestination::LinkLocal)?
        } else {
            let Some(route) = self
                .router
                .get(target)
                .or_else(|| self.router.default_route())
            else {
                return Ok(());
            };
            RoutedDestination::Route(route)
        };
        if destination.address().scheme() == "udp" {
            let _ = destination.try_with_address(|address| {
                let address = decode_udp_addr(address)?;
                slot.submit(length, address, send_at)
            })?;
            return Ok(());
        }
        if self.fallback.is_none() {
            return Ok(());
        }
        if self.fallback_transmits.len() >= TRANSIT_QUEUE_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "fallback transmit queue is full",
            ));
        }
        let bytes = Arc::new(slot.buffer_mut()[..length].to_vec());
        drop(slot);
        let position = self
            .fallback_transmits
            .iter()
            .position(|pending| pending.send_at > send_at)
            .unwrap_or(self.fallback_transmits.len());
        self.fallback_transmits.insert(
            position,
            FallbackTransmit {
                bytes,
                offset: 0,
                end: length,
                segment_size: self.segment_size,
                destination,
                send_at,
            },
        );
        Ok(())
    }
}

impl PacketIo for RoutedUdpPacketIo {
    fn peer_addresses_validated(&self) -> bool {
        false
    }

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
        let router = self.router.as_ref();
        let addresses = &self.addresses;
        let fallback = self.fallback.as_deref();
        let fallback_transmits = &mut self.fallback_transmits;
        let segment_size = self.segment_size;
        let slot = self.udp.try_send_slot()?;
        Some(Box::new(RoutedUdpSendSlot {
            slot: Some(slot),
            router,
            addresses,
            fallback,
            fallback_transmits,
            segment_size,
        }))
    }

    fn poll(&mut self, timeout: Duration) -> io::Result<()> {
        let waker = Waker::from(self.wake.clone());
        let mut context = Context::from_waker(&waker);
        self.flush_transit_transmits()?;
        self.flush_fallback_transmits(&mut context)?;
        let fallback_ready = self.poll_fallback(&mut context)?;
        let wait = if fallback_ready {
            Duration::ZERO
        } else {
            self.next_fallback_wait(timeout)
        };
        self.udp.poll(wait)?;
        self.flush_transit_transmits()?;
        self.flush_fallback_transmits(&mut context)?;
        self.poll_fallback(&mut context)?;
        Ok(())
    }

    fn drain_received(
        &mut self,
        receive: &mut dyn FnMut(&mut [u8], SocketAddr, SocketAddr) -> Result<(), Error>,
    ) -> Result<(), Error> {
        while let Some(event) = self.udp.try_next_event() {
            let IoEvent::Received(datagram) = event else {
                continue;
            };
            let source = self
                .addresses
                .register(crate::UdpSocket::datagram_addr(datagram.source()));
            let segment_size = datagram.segment_size().max(1);
            let length = datagram.len();
            let mut transit_buffer = self.free_transit_buffers.pop().unwrap_or_default();
            let (result, transit) = if length == 0 {
                (Ok(()), Vec::new())
            } else {
                let aggregate = self.udp.received(datagram);
                let (result, transit) = classify_received_aggregate(
                    self.local_pid,
                    &mut aggregate[..length],
                    segment_size,
                    source,
                    self.local,
                    receive,
                );
                if !transit.is_empty() {
                    transit_buffer.clear();
                    transit_buffer.extend_from_slice(&aggregate[..length]);
                }
                (result, transit)
            };
            self.udp.release_receive(datagram.slot());
            if transit.is_empty() {
                self.free_transit_buffers.push(transit_buffer);
            } else {
                let bytes = Arc::new(transit_buffer);
                for transit in transit {
                    self.enqueue_transit(
                        bytes.clone(),
                        transit.offset,
                        transit.length,
                        transit.segment_size,
                        transit.target,
                    );
                }
                self.recycle_transit_buffer(bytes);
            }
            result?;
        }
        while let Some(mut aggregate) = self.fallback_received.pop_front() {
            let (result, transit) = classify_received_aggregate(
                self.local_pid,
                &mut aggregate.bytes,
                aggregate.stride,
                aggregate.source,
                self.local,
                receive,
            );
            if transit.is_empty() {
                aggregate.bytes.clear();
                self.free_transit_buffers.push(aggregate.bytes);
            } else {
                let bytes = Arc::new(aggregate.bytes);
                for transit in transit {
                    self.enqueue_transit(
                        bytes.clone(),
                        transit.offset,
                        transit.length,
                        transit.segment_size,
                        transit.target,
                    );
                }
                self.recycle_transit_buffer(bytes);
            }
            result?;
        }
        Ok(())
    }
}

fn classify_received_aggregate(
    local_pid: Pid,
    bytes: &mut [u8],
    segment_size: usize,
    source: SocketAddr,
    local: SocketAddr,
    receive: &mut dyn FnMut(&mut [u8], SocketAddr, SocketAddr) -> Result<(), Error>,
) -> (Result<(), Error>, Vec<TransitSlice>) {
    let first_target = target_pid(&bytes[..bytes.len().min(segment_size)]);
    let uniform_target = first_target.filter(|target| {
        bytes
            .chunks(segment_size)
            .all(|packet| target_pid(packet) == Some(*target))
    });
    if let Some(target) = uniform_target
        && target != local_pid
        && !target.is_link_local()
    {
        return (
            Ok(()),
            vec![TransitSlice {
                offset: 0,
                length: bytes.len(),
                segment_size,
                target,
            }],
        );
    }

    let mut transit = Vec::new();
    let mut result = Ok(());
    for offset in (0..bytes.len()).step_by(segment_size) {
        let end = (offset + segment_size).min(bytes.len());
        let packet = &mut bytes[offset..end];
        let Some(target) = target_pid(packet) else {
            continue;
        };
        if target == local_pid || target.is_link_local() {
            if let Err(error) = receive(packet, source, local) {
                result = Err(error);
                break;
            }
        } else {
            transit.push(TransitSlice {
                offset,
                length: packet.len(),
                segment_size: packet.len(),
                target,
            });
        }
    }
    (result, transit)
}

struct NotifierWake(chrysalis_transport_uring::WakeHandle);

impl Notifier for NotifierWake {
    fn notify(&self) {
        self.0.notify();
    }
}

impl Wake for NotifierWake {
    fn wake(self: Arc<Self>) {
        self.0.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify();
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

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;
    use std::num::NonZeroUsize;

    use chrysalis_core::ConnectionKey;
    use chrysalis_core::Pid;
    use chrysalis_core::RoutedCid;

    use super::*;
    use crate::QuicConfig;
    use crate::UdpSocket;

    const TARGET: Pid = Pid::from_bytes([0x42; chrysalis_core::PID_LEN]);
    const SOURCE: Pid = Pid::from_bytes([0x24; chrysalis_core::PID_LEN]);
    const SEGMENT_SIZE: usize = 1_200;

    fn config() -> QuicConfig {
        QuicConfig::default()
            .try_with_max_udp_payload_size(SEGMENT_SIZE as u16)
            .expect("configure test UDP payload")
            .with_max_transmit_batch_segments(NonZeroUsize::new(4).expect("nonzero GSO count"))
    }

    fn socket() -> std::net::UdpSocket {
        std::net::UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)).expect("bind loopback UDP socket")
    }

    fn payload(target: Pid) -> Vec<u8> {
        payload_segments(target, 3)
    }

    fn payload_segments(target: Pid, segments: usize) -> Vec<u8> {
        let cid = RoutedCid::issued(target, ConnectionKey::from_u32(7));
        let mut payload = vec![0; SEGMENT_SIZE * segments];
        for (index, segment) in payload.chunks_mut(SEGMENT_SIZE).enumerate() {
            segment[0] = 0x40;
            segment[1..1 + chrysalis_core::CID_LEN].copy_from_slice(cid.as_bytes());
            segment[1 + chrysalis_core::CID_LEN..].fill(index as u8 + 1);
        }
        payload
    }

    #[test]
    fn routed_udp_splits_transit_gro_at_send_slot_capacity() {
        let config = config();
        let router = Arc::new(Router::new());
        router.insert(
            TARGET,
            Route::permanent(UdpSocket::datagram_addr(
                socket().local_addr().expect("read destination address"),
            )),
        );
        let driver =
            UdpDriver::new(socket(), config.udp_driver_config()).expect("create forwarder driver");
        let (mut forwarder, _, _) = RoutedUdpPacketIo::new(driver, SOURCE, router, None);
        let expected = payload_segments(TARGET, 10);

        assert!(
            forwarder.enqueue_transit(
                Arc::new(expected.clone()),
                0,
                expected.len(),
                SEGMENT_SIZE,
                TARGET,
            ),
            "queue transit aggregate"
        );

        assert_eq!(forwarder.transit_transmits.len(), 3);
        let shared = &forwarder
            .transit_transmits
            .front()
            .expect("first transit batch")
            .bytes;
        assert!(
            forwarder
                .transit_transmits
                .iter()
                .all(|pending| pending.length <= SEGMENT_SIZE * 4),
            "every queued batch should fit one UDP transmit slot"
        );
        assert!(
            forwarder
                .transit_transmits
                .iter()
                .all(|pending| Arc::ptr_eq(shared, &pending.bytes)),
            "split batches should share one aggregate allocation"
        );
        assert_eq!(
            forwarder
                .transit_transmits
                .iter()
                .flat_map(|pending| {
                    pending.bytes[pending.offset..pending.offset + pending.length]
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn routed_udp_classifies_mixed_aggregate_without_copying_packets() {
        let mut aggregate = payload_segments(SOURCE, 1);
        aggregate.extend_from_slice(&payload_segments(TARGET, 1));
        let mut local_packets = 0;

        let (result, transit) = classify_received_aggregate(
            SOURCE,
            &mut aggregate,
            SEGMENT_SIZE,
            synthetic_addr(2),
            synthetic_addr(1),
            &mut |_, _, _| {
                local_packets += 1;
                Ok(())
            },
        );

        result.expect("deliver local packet");
        assert_eq!(local_packets, 1);
        assert_eq!(transit.len(), 1);
        assert_eq!(transit[0].offset, SEGMENT_SIZE);
        assert_eq!(transit[0].length, SEGMENT_SIZE);
        assert_eq!(transit[0].target, TARGET);
    }

    #[test]
    fn routed_udp_preserves_completion_owned_gso_and_gro() {
        let config = config();
        let receiver_socket = socket();
        let receiver_address = receiver_socket.local_addr().expect("read receiver address");
        let receiver_driver =
            UdpDriver::new(receiver_socket, config.udp_driver_config()).expect("create receiver");
        let (mut receiver, _, receiver_stats) =
            RoutedUdpPacketIo::new(receiver_driver, TARGET, Arc::new(Router::new()), None);

        let sender_router = Arc::new(Router::new());
        sender_router.insert(
            TARGET,
            Route::permanent(UdpSocket::datagram_addr(receiver_address)),
        );
        let sender_driver =
            UdpDriver::new(socket(), config.udp_driver_config()).expect("create sender");
        let (mut sender, _, sender_stats) =
            RoutedUdpPacketIo::new(sender_driver, SOURCE, sender_router, None);

        let expected = payload(TARGET);
        let mut slot = sender.try_send_slot().expect("borrow routed UDP slot");
        slot.buffer_mut()[..expected.len()].copy_from_slice(&expected);
        slot.submit(expected.len(), synthetic_addr(99), Instant::now())
            .expect("submit routed GSO batch");

        let mut received = Vec::new();
        for _ in 0..20 {
            sender.poll(Duration::from_millis(10)).expect("poll sender");
            receiver
                .poll(Duration::from_millis(10))
                .expect("poll receiver");
            receiver
                .drain_received(&mut |packet, _, _| {
                    received.extend_from_slice(packet);
                    Ok(())
                })
                .expect("drain routed receive completions");
            if received.len() == expected.len() {
                break;
            }
        }

        assert_eq!(
            received, expected,
            "routed completion path changed packet bytes"
        );
        let sent = sender_stats.snapshot();
        assert_eq!(
            sent.transmit_calls, 1,
            "GSO batch should use one send completion"
        );
        assert_eq!(
            sent.transmit_datagrams, 3,
            "GSO batch should contain three datagrams"
        );
        let received = receiver_stats.snapshot();
        assert_eq!(
            received.receive_datagrams, 3,
            "GRO accounting should expand all received datagrams"
        );
    }

    #[test]
    fn routed_udp_forwards_nonlocal_cids_inside_the_completion_owner() {
        let config = config();
        let destination_socket = socket();
        let destination_address = destination_socket
            .local_addr()
            .expect("read destination address");
        let destination_driver = UdpDriver::new(destination_socket, config.udp_driver_config())
            .expect("create destination");
        let (mut destination, _, _) =
            RoutedUdpPacketIo::new(destination_driver, TARGET, Arc::new(Router::new()), None);

        let forwarder_socket = socket();
        let forwarder_address = forwarder_socket
            .local_addr()
            .expect("read forwarder address");
        let forwarder_router = Arc::new(Router::new());
        forwarder_router.insert(
            TARGET,
            Route::permanent(UdpSocket::datagram_addr(destination_address)),
        );
        let forwarder_driver =
            UdpDriver::new(forwarder_socket, config.udp_driver_config()).expect("create forwarder");
        let (mut forwarder, _, forwarder_stats) =
            RoutedUdpPacketIo::new(forwarder_driver, SOURCE, forwarder_router, None);

        let source_router = Arc::new(Router::new());
        source_router.insert(
            TARGET,
            Route::permanent(UdpSocket::datagram_addr(forwarder_address)),
        );
        let source_driver =
            UdpDriver::new(socket(), config.udp_driver_config()).expect("create source");
        let (mut source, _, _) = RoutedUdpPacketIo::new(
            source_driver,
            Pid::from_bytes([0x18; chrysalis_core::PID_LEN]),
            source_router,
            None,
        );

        let expected = payload(TARGET);
        let mut slot = source.try_send_slot().expect("borrow source slot");
        slot.buffer_mut()[..expected.len()].copy_from_slice(&expected);
        slot.submit(expected.len(), synthetic_addr(99), Instant::now())
            .expect("submit source batch");

        let mut received = Vec::new();
        for _ in 0..30 {
            source.poll(Duration::from_millis(10)).expect("poll source");
            forwarder
                .poll(Duration::from_millis(10))
                .expect("poll forwarder");
            forwarder
                .drain_received(&mut |_, _, _| panic!("transit packet terminated locally"))
                .expect("route forwarder completions");
            destination
                .poll(Duration::from_millis(10))
                .expect("poll destination");
            destination
                .drain_received(&mut |packet, _, _| {
                    received.extend_from_slice(packet);
                    Ok(())
                })
                .expect("drain destination completions");
            if received.len() == expected.len() {
                break;
            }
        }

        assert_eq!(received, expected, "forwarding changed packet bytes");
        let forwarded = forwarder_stats.snapshot();
        assert_eq!(
            forwarded.transmit_datagrams, 3,
            "forwarder should retain the segmented batch"
        );
    }
}
