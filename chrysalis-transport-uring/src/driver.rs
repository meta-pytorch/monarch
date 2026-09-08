/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Single-owner io_uring driver for paced UDP packet I/O.
//!
//! `UdpDriver` owns an unconnected UDP socket, its io_uring, and fixed pools of
//! receive and transmit buffers. The socket is registered in fixed-file slot
//! zero, and every submitted `recvmsg` or `sendmsg` refers to stable boxed
//! storage owned by the driver. This lets the kernel access packet buffers
//! directly while an operation is in flight.
//!
//! Each call to `UdpDriver::poll` performs one driver cycle:
//!
//! 1. It fills available submission entries with receives, an eventfd read, and
//!    transmits whose pacing deadlines have elapsed.
//! 2. It submits them and waits until an I/O completion, the caller's timeout,
//!    or the next pacing deadline.
//! 3. It translates io_uring completion entries into `IoEvent`s. Interrupted or
//!    temporarily unavailable operations return to their pending queues.
//! 4. It submits newly due transmits and publishes a statistics snapshot.
//!
//! `PollOutcome::events` counts events added during that call. Consumers remove
//! queued events with `UdpDriver::try_next_event`; they should drain the queue
//! after polling. A `Received` event transfers logical ownership of its receive
//! slot to the consumer. `UdpDriver::received` borrows the kernel-written bytes,
//! and `UdpDriver::release_receive` must be called after processing so that the
//! slot can be submitted again. Holding receive events without releasing their
//! slots therefore reduces the number of receives that the driver can keep in
//! flight. A `Transmitted` event is informational: its transmit slot has already
//! returned to the free pool. Dropping an unsubmitted `SendSlot` also returns
//! that slot to the pool.
//!
//! `UdpDriver::notifier` returns an eventfd-backed handle that another thread
//! can use to interrupt a blocked poll after placing application work in a
//! queue. Such a wakeup is reported by `PollOutcome::woken`; it is not an
//! `IoEvent`.
//!
//! When GSO is configured, one transmit slot can contain several consecutive
//! QUIC datagrams. Every segment except the final one has
//! `DriverConfig::segment_size` bytes, and the final segment may be shorter.
//! The driver submits the aggregate with one `sendmsg`; the kernel emits the
//! individual UDP datagrams. The corresponding `Transmitted` event describes
//! the whole aggregate, while `TransmitCompletion::segments` reports how many
//! datagrams it contained.
//!
//! With GRO enabled, the kernel may place several received UDP datagrams in one
//! receive slot. One `Received` event describes that aggregate. Its
//! `ReceivedDatagram::segment_size` comes from the UDP GRO control message, so
//! the consumer can split the slice returned by `UdpDriver::received` with
//! `slice::chunks`. An ordinary receive reports its full payload length as the
//! segment size and therefore produces one chunk. Empty datagrams must be
//! handled separately because their segment size is zero.
//!
//! ```no_run
//! use std::io;
//! use std::net::SocketAddr;
//! use std::net::UdpSocket;
//! use std::time::Duration;
//! use std::time::Instant;
//!
//! use chrysalis_transport_uring::DriverConfig;
//! use chrysalis_transport_uring::IoEvent;
//! use chrysalis_transport_uring::UdpDriver;
//!
//! fn drive(socket: UdpSocket, peer: SocketAddr) -> io::Result<()> {
//!     let mut driver = UdpDriver::new(socket, DriverConfig::default())?;
//!
//!     let payload = b"one QUIC datagram";
//!     let mut slot = driver.try_send_slot().ok_or_else(|| {
//!         io::Error::new(io::ErrorKind::WouldBlock, "no transmit slot available")
//!     })?;
//!     slot.buffer_mut()[..payload.len()].copy_from_slice(payload);
//!     slot.submit(payload.len(), peer, Instant::now())?;
//!
//!     loop {
//!         let outcome = driver.poll(Duration::from_millis(10))?;
//!         while let Some(event) = driver.try_next_event() {
//!             match event {
//!                 IoEvent::Received(datagram) => {
//!                     let receive_slot = datagram.slot();
//!                     {
//!                         let aggregate = driver.received(datagram);
//!                         if aggregate.is_empty() {
//!                             // Process one empty UDP datagram.
//!                         } else {
//!                             for packet in aggregate.chunks(datagram.segment_size()) {
//!                                 // Pass each UDP datagram to QUIC.
//!                                 let _ = packet;
//!                             }
//!                         }
//!                     }
//!                     driver.release_receive(receive_slot);
//!                 }
//!                 IoEvent::Transmitted(completion) => {
//!                     // The aggregate is complete and its send slot is reusable.
//!                     let _ = completion;
//!                 }
//!             }
//!         }
//!         if outcome.woken() {
//!             // Drain application work that prompted the notifier.
//!         }
//!     }
//! }
//! ```

use std::collections::VecDeque;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use chrysalis_transport_core::Notifier;
use io_uring::IoUring;
use io_uring::opcode;
use io_uring::types;

use crate::DriverConfig;
use crate::config::invalid_input;
use crate::socket::CONTROL_BUFFER_SIZE;
use crate::socket::ControlBuffer;
use crate::socket::SocketAddress;
use crate::socket::UDP_GRO;
use crate::socket::cmsg_align;
use crate::socket::configure_socket;

const RX_TAG: u64 = 1 << 63;
const TX_TAG: u64 = 1 << 62;
const WAKE_TAG: u64 = 3 << 62;
const INDEX_MASK: u64 = TX_TAG - 1;
const RECEIVE_CAPACITY: usize = u16::MAX as usize;

/// Identifies a receive slot held by the caller.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReceiveSlotId(usize);

/// Describes one kernel-written UDP payload retained in a receive slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceivedDatagram {
    slot: ReceiveSlotId,
    source: SocketAddr,
    length: usize,
    segment_size: usize,
}

impl ReceivedDatagram {
    /// Returns the slot containing this payload.
    pub const fn slot(self) -> ReceiveSlotId {
        self.slot
    }

    /// Returns the source address written by the kernel.
    pub const fn source(self) -> SocketAddr {
        self.source
    }

    /// Returns the complete payload length, possibly containing several GRO segments.
    pub const fn len(self) -> usize {
        self.length
    }

    /// Returns whether this payload is empty.
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    /// Returns the UDP GRO segment size, or the payload length for an ordinary datagram.
    pub const fn segment_size(self) -> usize {
        self.segment_size
    }
}

/// Describes one completed UDP send.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransmitCompletion {
    destination: SocketAddr,
    length: usize,
    segments: usize,
}

impl TransmitCompletion {
    /// Returns the destination of this send.
    pub const fn destination(self) -> SocketAddr {
        self.destination
    }

    /// Returns the GSO aggregate byte count.
    pub const fn len(self) -> usize {
        self.length
    }

    /// Returns whether the completed payload was empty.
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    /// Returns the number of QUIC datagrams in the aggregate.
    pub const fn segments(self) -> usize {
        self.segments
    }
}

/// One packet-I/O completion consumed by the QUIC driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoEvent {
    /// A kernel-written payload is held until [`UdpDriver::release_receive`] is called.
    Received(ReceivedDatagram),
    /// A send completed and its stable transmit slot was recycled.
    Transmitted(TransmitCompletion),
}

/// Result of one driver wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PollOutcome {
    events: usize,
    timed_out: bool,
    woken: bool,
}

impl PollOutcome {
    /// Returns the events added to the driver's completion queue.
    pub const fn events(self) -> usize {
        self.events
    }

    /// Returns whether the caller deadline elapsed before an I/O completion.
    pub const fn timed_out(self) -> bool {
        self.timed_out
    }

    /// Returns whether an application notification woke the driver.
    pub const fn woken(self) -> bool {
        self.woken
    }
}

/// Thread-safe eventfd notifier for a blocked driver.
#[derive(Clone, Debug)]
pub struct WakeHandle {
    fd: Arc<OwnedFd>,
}

impl Notifier for WakeHandle {
    fn notify(&self) {
        let value = 1u64.to_ne_bytes();
        loop {
            // SAFETY: fd is a live eventfd and value is readable for exactly eight bytes.
            let result =
                unsafe { libc::write(self.fd.as_raw_fd(), value.as_ptr().cast(), value.len()) };
            if result == value.len() as isize {
                return;
            }
            if result < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            // Notifier cannot report errors. EFD_NONBLOCK only fails when the u64 counter is
            // saturated, which already means a wakeup is pending.
            return;
        }
    }
}

/// Packet counters owned by one driver thread.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DriverStats {
    /// QUIC datagrams submitted, after expanding GSO aggregates.
    pub datagrams_sent: u64,
    /// UDP send operations completed.
    pub sends_completed: u64,
    /// UDP payload bytes completed by the kernel send path.
    pub bytes_sent: u64,
    /// QUIC datagrams received, after splitting GRO aggregates.
    pub datagrams_received: u64,
    /// UDP receive operations completed.
    pub receives_completed: u64,
    /// UDP payload bytes completed by the kernel receive path.
    pub bytes_received: u64,
    /// Latest cumulative kernel receive-queue overflow count.
    pub socket_overflow_drops: u64,
}

#[derive(Debug, Default)]
struct SharedDriverStats {
    datagrams_sent: AtomicU64,
    sends_completed: AtomicU64,
    bytes_sent: AtomicU64,
    datagrams_received: AtomicU64,
    receives_completed: AtomicU64,
    bytes_received: AtomicU64,
    socket_overflow_drops: AtomicU64,
}

/// Thread-safe snapshots of one UDP driver's packet counters.
#[derive(Clone, Debug, Default)]
pub struct DriverStatsHandle {
    inner: Arc<SharedDriverStats>,
}

impl DriverStatsHandle {
    /// Returns the most recently published driver counters.
    pub fn snapshot(&self) -> DriverStats {
        DriverStats {
            datagrams_sent: self.inner.datagrams_sent.load(Ordering::Relaxed),
            sends_completed: self.inner.sends_completed.load(Ordering::Relaxed),
            bytes_sent: self.inner.bytes_sent.load(Ordering::Relaxed),
            datagrams_received: self.inner.datagrams_received.load(Ordering::Relaxed),
            receives_completed: self.inner.receives_completed.load(Ordering::Relaxed),
            bytes_received: self.inner.bytes_received.load(Ordering::Relaxed),
            socket_overflow_drops: self.inner.socket_overflow_drops.load(Ordering::Relaxed),
        }
    }

    fn publish(&self, stats: DriverStats) {
        self.inner
            .datagrams_sent
            .store(stats.datagrams_sent, Ordering::Relaxed);
        self.inner
            .sends_completed
            .store(stats.sends_completed, Ordering::Relaxed);
        self.inner
            .bytes_sent
            .store(stats.bytes_sent, Ordering::Relaxed);
        self.inner
            .datagrams_received
            .store(stats.datagrams_received, Ordering::Relaxed);
        self.inner
            .receives_completed
            .store(stats.receives_completed, Ordering::Relaxed);
        self.inner
            .bytes_received
            .store(stats.bytes_received, Ordering::Relaxed);
        self.inner
            .socket_overflow_drops
            .store(stats.socket_overflow_drops, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiveState {
    Pending,
    Submitted,
    Held,
}

struct ReceiveSlot {
    data: Box<[u8]>,
    source: SocketAddress,
    control: ControlBuffer,
    iovec: libc::iovec,
    message: libc::msghdr,
    state: ReceiveState,
}

impl ReceiveSlot {
    fn new(capacity: usize) -> Self {
        // SAFETY: prepare initializes both structures before submission.
        let iovec = unsafe { mem::zeroed() };
        // SAFETY: prepare initializes every msghdr field consumed by recvmsg.
        let message = unsafe { mem::zeroed() };
        Self {
            data: vec![0; capacity].into_boxed_slice(),
            source: SocketAddress::default(),
            control: ControlBuffer([0; CONTROL_BUFFER_SIZE]),
            iovec,
            message,
            state: ReceiveState::Pending,
        }
    }

    fn prepare(&mut self) {
        self.source = SocketAddress::default();
        self.source.length = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        self.iovec.iov_base = self.data.as_mut_ptr().cast();
        self.iovec.iov_len = self.data.len();
        self.message.msg_name = self.source.as_mut_ptr();
        self.message.msg_namelen = self.source.length;
        self.message.msg_iov = &mut self.iovec;
        self.message.msg_iovlen = 1;
        self.message.msg_control = self.control.0.as_mut_ptr().cast();
        self.message.msg_controllen = self.control.0.len();
        self.message.msg_flags = 0;
    }

    fn metadata(&self, payload_length: usize) -> io::Result<(usize, Option<u32>)> {
        if self.message.msg_flags & libc::MSG_CTRUNC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UDP control message was truncated",
            ));
        }
        if self.message.msg_flags & libc::MSG_TRUNC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UDP payload was truncated",
            ));
        }

        let control_length = self.message.msg_controllen;
        if control_length > self.control.0.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UDP control length exceeds its buffer",
            ));
        }

        let header_size = cmsg_align(mem::size_of::<libc::cmsghdr>());
        let mut offset = 0;
        let mut segment_size = payload_length;
        let mut overflow = None;
        while offset + mem::size_of::<libc::cmsghdr>() <= control_length {
            // SAFETY: The bounds above cover a complete cmsghdr; unaligned access is allowed.
            let header = unsafe {
                self.control
                    .0
                    .as_ptr()
                    .add(offset)
                    .cast::<libc::cmsghdr>()
                    .read_unaligned()
            };
            if header.cmsg_len < header_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "UDP control message has an invalid length",
                ));
            }
            let end = offset.checked_add(header.cmsg_len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "UDP control length overflow")
            })?;
            if end > control_length {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "UDP control message exceeds its buffer",
                ));
            }

            if header.cmsg_level == libc::SOL_UDP && header.cmsg_type == UDP_GRO {
                if header.cmsg_len < header_size + mem::size_of::<u16>() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "UDP GRO control message is too short",
                    ));
                }
                // SAFETY: cmsg_len proves that a u16 follows the aligned header.
                segment_size = unsafe {
                    self.control
                        .0
                        .as_ptr()
                        .add(offset + header_size)
                        .cast::<u16>()
                        .read_unaligned()
                } as usize;
                if segment_size == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "UDP GRO segment size is zero",
                    ));
                }
            } else if header.cmsg_level == libc::SOL_SOCKET && header.cmsg_type == libc::SO_RXQ_OVFL
            {
                if header.cmsg_len < header_size + mem::size_of::<u32>() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "SO_RXQ_OVFL control message is too short",
                    ));
                }
                // SAFETY: cmsg_len proves that a u32 follows the aligned header.
                overflow = Some(unsafe {
                    self.control
                        .0
                        .as_ptr()
                        .add(offset + header_size)
                        .cast::<u32>()
                        .read_unaligned()
                });
            }
            offset = cmsg_align(end);
        }
        Ok((segment_size, overflow))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransmitState {
    Free,
    Pending,
    Submitted,
}

struct TransmitMessage {
    destination: SocketAddress,
    iovec: libc::iovec,
    message: libc::msghdr,
}

impl TransmitMessage {
    fn new() -> Self {
        // SAFETY: prepare initializes both structures before submission.
        let iovec = unsafe { mem::zeroed() };
        // SAFETY: prepare initializes every msghdr field consumed by sendmsg.
        let message = unsafe { mem::zeroed() };
        Self {
            destination: SocketAddress::default(),
            iovec,
            message,
        }
    }

    fn prepare(&mut self, data: *const u8, length: usize, destination: SocketAddr) {
        self.destination = destination.into();
        self.iovec.iov_base = data.cast_mut().cast();
        self.iovec.iov_len = length;
        self.message.msg_name = self.destination.as_mut_ptr();
        self.message.msg_namelen = self.destination.length;
        self.message.msg_iov = &mut self.iovec;
        self.message.msg_iovlen = 1;
        self.message.msg_control = ptr::null_mut();
        self.message.msg_controllen = 0;
        self.message.msg_flags = 0;
    }
}

struct TransmitSlot {
    data: Box<[u8]>,
    message: Box<TransmitMessage>,
    state: TransmitState,
    length: usize,
    segments: usize,
    destination: Option<SocketAddr>,
    send_at: Instant,
}

/// Single-owner io_uring UDP engine.
pub struct UdpDriver {
    // The ring must be dropped before stable memory referenced by in-flight operations.
    ring: IoUring,
    socket: UdpSocket,
    config: DriverConfig,
    receive_slots: Vec<ReceiveSlot>,
    transmit_slots: Vec<TransmitSlot>,
    pending_receive: VecDeque<usize>,
    free_transmit: VecDeque<usize>,
    pending_transmit: VecDeque<usize>,
    events: VecDeque<IoEvent>,
    raw_completions: Vec<(u64, i32)>,
    wake: WakeHandle,
    wake_value: Box<u64>,
    wake_pending: bool,
    stats: DriverStats,
    stats_handle: DriverStatsHandle,
}

// SAFETY: UdpDriver has exclusive ownership of its ring, descriptors, and every packet slot.
// Raw pointers in msghdr/iovec values refer only to boxed storage owned by the same driver and
// remain stable when the driver moves. No API exposes concurrent access after ownership transfer.
unsafe impl Send for UdpDriver {}

impl UdpDriver {
    /// Creates a driver around an unconnected UDP socket.
    pub fn new(socket: UdpSocket, config: DriverConfig) -> io::Result<Self> {
        config.validate()?;
        configure_socket(&socket, config)?;
        let ring = IoUring::new(config.ring_depth().get())?;
        if !ring.params().is_feature_ext_arg() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kernel io_uring does not support deadline waits",
            ));
        }
        ring.submitter().register_files(&[socket.as_raw_fd()])?;
        // SAFETY: eventfd has no pointer arguments and returns a new owned descriptor.
        let wake_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if wake_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: eventfd returned a fresh descriptor whose ownership transfers here.
        let wake_fd = unsafe { OwnedFd::from_raw_fd(wake_fd) };
        let wake = WakeHandle {
            fd: Arc::new(wake_fd),
        };

        let transmit_capacity = config
            .segment_size()
            .get()
            .checked_mul(config.max_gso_segments().get())
            .ok_or_else(|| invalid_input("transmit slot size overflows usize"))?;
        let transmit_depth = config.ring_depth().get() as usize - config.receive_depth().get() - 1;
        let receive_slots = (0..config.receive_depth().get())
            .map(|_| ReceiveSlot::new(RECEIVE_CAPACITY))
            .collect();
        let transmit_slots = (0..transmit_depth)
            .map(|_| TransmitSlot {
                data: vec![0; transmit_capacity].into_boxed_slice(),
                message: Box::new(TransmitMessage::new()),
                state: TransmitState::Free,
                length: 0,
                segments: 0,
                destination: None,
                send_at: Instant::now(),
            })
            .collect();

        Ok(Self {
            ring,
            socket,
            config,
            receive_slots,
            transmit_slots,
            pending_receive: (0..config.receive_depth().get()).collect(),
            free_transmit: (0..transmit_depth).collect(),
            pending_transmit: VecDeque::new(),
            events: VecDeque::with_capacity(config.ring_depth().get() as usize),
            raw_completions: Vec::with_capacity(config.ring_depth().get() as usize),
            wake,
            wake_value: Box::new(0),
            wake_pending: false,
            stats: DriverStats::default(),
            stats_handle: DriverStatsHandle::default(),
        })
    }

    /// Returns the bound UDP address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Returns the maximum QUIC datagram and UDP GSO segment size.
    pub fn segment_size(&self) -> usize {
        self.config.segment_size().get()
    }

    /// Returns the maximum datagrams that fit in one transmit slot.
    pub fn max_gso_segments(&self) -> usize {
        self.config.max_gso_segments().get()
    }

    /// Returns a notifier suitable for the driver's submission queue.
    pub fn notifier(&self) -> WakeHandle {
        self.wake.clone()
    }

    /// Borrows a free stable transmit slot for direct packet or GSO assembly.
    pub fn try_send_slot(&mut self) -> Option<SendSlot<'_>> {
        let slot = self.free_transmit.pop_front()?;
        debug_assert_eq!(self.transmit_slots[slot].state, TransmitState::Free);
        Some(SendSlot {
            driver: self,
            slot,
            submitted: false,
        })
    }

    /// Waits for I/O or a pacing deadline and queues resulting events.
    pub fn poll(&mut self, timeout: Duration) -> io::Result<PollOutcome> {
        self.queue_receives();
        self.queue_wakeup();
        self.queue_due_transmits(Instant::now())?;

        let pacing_wait = self.pending_transmit.front().map(|slot| {
            self.transmit_slots[*slot]
                .send_at
                .saturating_duration_since(Instant::now())
        });
        let (wait, caller_timeout) = poll_wait(timeout, pacing_wait);
        let timed_out = if wait.is_zero() {
            self.ring.submit()?;
            caller_timeout
        } else {
            let timespec = types::Timespec::from(wait);
            let arguments = types::SubmitArgs::new().timespec(&timespec);
            match self.ring.submitter().submit_with_args(1, &arguments) {
                Ok(_) => false,
                Err(error) if error.raw_os_error() == Some(libc::ETIME) => caller_timeout,
                Err(error) => return Err(error),
            }
        };

        let before = self.events.len();
        let woken = self.process_completions()?;
        self.queue_due_transmits(Instant::now())?;
        self.ring.submit()?;
        self.stats_handle.publish(self.stats);
        Ok(PollOutcome {
            events: self.events.len() - before,
            timed_out,
            woken,
        })
    }

    /// Removes the next packet-I/O event.
    pub fn try_next_event(&mut self) -> Option<IoEvent> {
        self.events.pop_front()
    }

    /// Returns the kernel-written bytes for a held receive slot.
    pub fn received(&mut self, datagram: ReceivedDatagram) -> &mut [u8] {
        let slot = &mut self.receive_slots[datagram.slot.0];
        assert_eq!(slot.state, ReceiveState::Held, "receive slot is not held");
        &mut slot.data[..datagram.length]
    }

    /// Releases a receive slot so the kernel can write another datagram into it.
    pub fn release_receive(&mut self, slot: ReceiveSlotId) {
        let receive = &mut self.receive_slots[slot.0];
        assert_eq!(
            receive.state,
            ReceiveState::Held,
            "receive slot is not held"
        );
        receive.state = ReceiveState::Pending;
        self.pending_receive.push_back(slot.0);
    }

    /// Returns a snapshot of packet counters.
    pub const fn stats(&self) -> DriverStats {
        self.stats
    }

    /// Returns a thread-safe handle updated after every driver poll.
    pub fn stats_handle(&self) -> DriverStatsHandle {
        self.stats_handle.clone()
    }

    fn queue_receives(&mut self) {
        while let Some(slot) = self.pending_receive.front().copied() {
            let receive = &mut self.receive_slots[slot];
            debug_assert_eq!(receive.state, ReceiveState::Pending);
            receive.prepare();
            let entry = opcode::RecvMsg::new(types::Fixed(0), &mut receive.message)
                .build()
                .user_data(RX_TAG | slot as u64);
            let mut submissions = self.ring.submission();
            // SAFETY: receive slot storage is stable and not reused until this operation completes.
            if unsafe { submissions.push(&entry) }.is_err() {
                break;
            }
            receive.state = ReceiveState::Submitted;
            self.pending_receive.pop_front();
        }
    }

    fn queue_wakeup(&mut self) {
        if self.wake_pending {
            return;
        }
        let entry = opcode::Read::new(
            types::Fd(self.wake.fd.as_raw_fd()),
            ptr::from_mut(self.wake_value.as_mut()).cast(),
            mem::size_of::<u64>() as u32,
        )
        .build()
        .user_data(WAKE_TAG);
        let mut submissions = self.ring.submission();
        // SAFETY: wake_value is boxed and remains stable until the read completes. There is at
        // most one outstanding eventfd read.
        if unsafe { submissions.push(&entry) }.is_ok() {
            self.wake_pending = true;
        }
    }

    fn queue_due_transmits(&mut self, now: Instant) -> io::Result<()> {
        while let Some(slot) = self.pending_transmit.front().copied() {
            if self.transmit_slots[slot].send_at > now || self.ring.submission().is_full() {
                break;
            }
            self.pending_transmit.pop_front();
            let transmit = &mut self.transmit_slots[slot];
            debug_assert_eq!(transmit.state, TransmitState::Pending);
            let destination = transmit
                .destination
                .expect("pending transmit should have a destination");
            transmit
                .message
                .prepare(transmit.data.as_ptr(), transmit.length, destination);
            let entry = opcode::SendMsg::new(types::Fixed(0), &transmit.message.message)
                .flags(libc::MSG_NOSIGNAL as u32)
                .build()
                .user_data(TX_TAG | slot as u64);
            let mut submissions = self.ring.submission();
            // SAFETY: transmit slot storage is stable and retained until completion.
            unsafe { submissions.push(&entry) }
                .map_err(|_| io::Error::other("io_uring submission queue unexpectedly full"))?;
            transmit.state = TransmitState::Submitted;
        }
        Ok(())
    }

    fn process_completions(&mut self) -> io::Result<bool> {
        // CompletionQueue exclusively borrows the ring and publishes its consumed head when
        // dropped. Copy the small completion records into reusable storage so that completion
        // handlers can borrow the whole driver mutably. Indexing the records below likewise
        // avoids holding a drain borrow across those calls.
        self.raw_completions.clear();
        {
            let mut queue = self.ring.completion();
            for completion in &mut queue {
                self.raw_completions
                    .push((completion.user_data(), completion.result()));
            }
        }

        let mut woken = false;
        for index in 0..self.raw_completions.len() {
            let (user_data, result) = self.raw_completions[index];
            let tag = user_data & !INDEX_MASK;
            let slot = (user_data & INDEX_MASK) as usize;
            if result < 0 {
                let error = -result;
                if matches!(error, libc::EAGAIN | libc::EINTR) {
                    match tag {
                        RX_TAG => {
                            let receive = &mut self.receive_slots[slot];
                            debug_assert_eq!(receive.state, ReceiveState::Submitted);
                            receive.state = ReceiveState::Pending;
                            self.pending_receive.push_back(slot);
                        }
                        TX_TAG => {
                            let send_at = {
                                let transmit = &mut self.transmit_slots[slot];
                                debug_assert_eq!(transmit.state, TransmitState::Submitted);
                                transmit.state = TransmitState::Pending;
                                transmit.send_at
                            };
                            let position = self
                                .pending_transmit
                                .iter()
                                .position(|pending| self.transmit_slots[*pending].send_at > send_at)
                                .unwrap_or(self.pending_transmit.len());
                            self.pending_transmit.insert(position, slot);
                        }
                        WAKE_TAG => self.wake_pending = false,
                        _ => return Err(io::Error::from_raw_os_error(error)),
                    }
                    continue;
                }
                match tag {
                    RX_TAG => self.recycle_receive(slot),
                    TX_TAG => self.recycle_transmit(slot),
                    WAKE_TAG => self.wake_pending = false,
                    _ => {}
                }
                return Err(io::Error::from_raw_os_error(error));
            }
            match tag {
                RX_TAG => self.complete_receive(slot, result as usize)?,
                TX_TAG => self.complete_transmit(slot, result as usize)?,
                WAKE_TAG => {
                    self.wake_pending = false;
                    if result as usize != mem::size_of::<u64>() {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("eventfd read returned {result} bytes"),
                        ));
                    }
                    woken = true;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown io_uring completion tag {tag:#x}"),
                    ));
                }
            }
        }
        Ok(woken)
    }

    fn complete_receive(&mut self, slot: usize, length: usize) -> io::Result<()> {
        let metadata = {
            let receive = &self.receive_slots[slot];
            debug_assert_eq!(receive.state, ReceiveState::Submitted);
            receive
                .metadata(length)
                .and_then(|(segment_size, overflow)| {
                    receive
                        .source
                        .to_std()
                        .map(|source| (segment_size, overflow, source))
                })
        };
        let (segment_size, overflow, source) = match metadata {
            Ok(metadata) => metadata,
            Err(error) => {
                self.recycle_receive(slot);
                return Err(error);
            }
        };
        let receive = &mut self.receive_slots[slot];
        receive.state = ReceiveState::Held;
        self.stats.receives_completed += 1;
        self.stats.bytes_received += length as u64;
        self.stats.datagrams_received += if length == 0 {
            1
        } else {
            length.div_ceil(segment_size) as u64
        };
        self.stats.socket_overflow_drops = self
            .stats
            .socket_overflow_drops
            .max(overflow.map(u64::from).unwrap_or_default());
        self.events.push_back(IoEvent::Received(ReceivedDatagram {
            slot: ReceiveSlotId(slot),
            source,
            length,
            segment_size,
        }));
        Ok(())
    }

    fn complete_transmit(&mut self, slot: usize, length: usize) -> io::Result<()> {
        let expected = self.transmit_slots[slot].length;
        if length != expected {
            self.recycle_transmit(slot);
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("short UDP send: sent {length} of {expected} bytes"),
            ));
        }
        let transmit = &mut self.transmit_slots[slot];
        debug_assert_eq!(transmit.state, TransmitState::Submitted);
        let completion = TransmitCompletion {
            destination: transmit
                .destination
                .take()
                .expect("submitted transmit should have a destination"),
            length,
            segments: transmit.segments,
        };
        transmit.state = TransmitState::Free;
        transmit.length = 0;
        transmit.segments = 0;
        self.free_transmit.push_back(slot);
        self.stats.datagrams_sent += completion.segments as u64;
        self.stats.sends_completed += 1;
        self.stats.bytes_sent += length as u64;
        self.events.push_back(IoEvent::Transmitted(completion));
        Ok(())
    }

    fn recycle_receive(&mut self, slot: usize) {
        let receive = &mut self.receive_slots[slot];
        debug_assert_eq!(receive.state, ReceiveState::Submitted);
        receive.state = ReceiveState::Pending;
        self.pending_receive.push_back(slot);
    }

    fn recycle_transmit(&mut self, slot: usize) {
        let transmit = &mut self.transmit_slots[slot];
        debug_assert_eq!(transmit.state, TransmitState::Submitted);
        transmit.state = TransmitState::Free;
        transmit.length = 0;
        transmit.segments = 0;
        transmit.destination = None;
        self.free_transmit.push_back(slot);
    }

    fn submit_slot(
        &mut self,
        slot: usize,
        length: usize,
        destination: SocketAddr,
        send_at: Instant,
    ) -> io::Result<()> {
        if length == 0 || length > self.transmit_slots[slot].data.len() {
            return Err(invalid_input("send length is outside the transmit slot"));
        }
        let transmit = &mut self.transmit_slots[slot];
        debug_assert_eq!(transmit.state, TransmitState::Free);
        transmit.length = length;
        transmit.segments = length.div_ceil(self.config.segment_size().get());
        transmit.destination = Some(destination);
        transmit.send_at = send_at;
        transmit.state = TransmitState::Pending;
        let position = self
            .pending_transmit
            .iter()
            .position(|pending| self.transmit_slots[*pending].send_at > send_at)
            .unwrap_or(self.pending_transmit.len());
        self.pending_transmit.insert(position, slot);
        Ok(())
    }

    fn release_unsent_slot(&mut self, slot: usize) {
        debug_assert_eq!(self.transmit_slots[slot].state, TransmitState::Free);
        self.free_transmit.push_front(slot);
    }
}

fn poll_wait(timeout: Duration, pacing_wait: Option<Duration>) -> (Duration, bool) {
    match pacing_wait {
        Some(pacing) if pacing.is_zero() && !timeout.is_zero() => (timeout, true),
        Some(pacing) if pacing < timeout => (pacing, false),
        _ => (timeout, true),
    }
}

/// Exclusive access to a stable transmit buffer.
pub struct SendSlot<'a> {
    driver: &'a mut UdpDriver,
    slot: usize,
    submitted: bool,
}

impl SendSlot<'_> {
    /// Returns the entire fixed-capacity buffer for direct GSO assembly.
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.driver.transmit_slots[self.slot].data
    }

    /// Queues the initialized prefix for userspace-paced transmission.
    pub fn submit(
        mut self,
        length: usize,
        destination: SocketAddr,
        send_at: Instant,
    ) -> io::Result<()> {
        self.driver
            .submit_slot(self.slot, length, destination, send_at)?;
        self.submitted = true;
        Ok(())
    }
}

impl Drop for SendSlot<'_> {
    fn drop(&mut self) {
        if !self.submitted {
            self.driver.release_unsent_slot(self.slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::num::NonZeroUsize;

    use super::*;

    fn test_config(gso_segments: usize, gro: bool) -> DriverConfig {
        DriverConfig::default()
            .with_ring_depth(NonZeroU32::new(16).unwrap())
            .with_receive_depth(NonZeroUsize::new(4).unwrap())
            .with_segment_size(NonZeroUsize::new(1200).unwrap())
            .with_max_gso_segments(NonZeroUsize::new(gso_segments).unwrap())
            .with_socket_buffer_bytes(NonZeroUsize::new(1024 * 1024).unwrap())
            .with_gro(gro)
    }

    fn loopback_socket() -> UdpSocket {
        UdpSocket::bind("[::1]:0").expect("bind IPv6 loopback UDP socket")
    }

    fn poll_until_event(driver: &mut UdpDriver) -> IoEvent {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            driver
                .poll(deadline.saturating_duration_since(Instant::now()))
                .expect("poll UDP driver");
            if let Some(event) = driver.try_next_event() {
                return event;
            }
            assert!(Instant::now() < deadline, "timed out waiting for UDP event");
        }
    }

    #[test]
    fn receives_into_a_stable_slot_until_released() {
        let socket = loopback_socket();
        let address = socket.local_addr().unwrap();
        let mut driver = UdpDriver::new(socket, test_config(1, false)).unwrap();
        let sender = loopback_socket();
        sender.send_to(b"first", address).unwrap();

        let IoEvent::Received(first) = poll_until_event(&mut driver) else {
            panic!("expected receive completion");
        };
        assert_eq!(driver.received(first), b"first");
        let first_slot = first.slot();
        driver.release_receive(first_slot);

        sender.send_to(b"second", address).unwrap();
        let IoEvent::Received(second) = poll_until_event(&mut driver) else {
            panic!("expected second receive completion");
        };
        assert_eq!(driver.received(second), b"second");
        driver.release_receive(second.slot());
        assert_eq!(driver.stats().receives_completed, 2);
    }

    #[test]
    fn receive_slots_are_starved_until_one_is_released() {
        let socket = loopback_socket();
        let address = socket.local_addr().unwrap();
        let mut driver = UdpDriver::new(socket, test_config(1, false)).unwrap();
        let sender = loopback_socket();
        let capacity = driver.receive_slots.len();
        for value in 0..=capacity {
            sender.send_to(&[value as u8], address).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut held = Vec::with_capacity(capacity);
        while held.len() < capacity {
            if let Some(event) = driver.try_next_event() {
                let IoEvent::Received(datagram) = event else {
                    panic!("expected receive completion");
                };
                held.push(datagram);
                continue;
            }
            driver
                .poll(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            assert!(Instant::now() < deadline, "timed out filling receive slots");
        }

        let outcome = driver.poll(Duration::from_millis(10)).unwrap();
        assert_eq!(outcome.events(), 0);
        assert!(outcome.timed_out());
        assert!(driver.try_next_event().is_none());

        let released = held.pop().unwrap();
        driver.release_receive(released.slot());
        let IoEvent::Received(next) = poll_until_event(&mut driver) else {
            panic!("expected receive after releasing a slot");
        };
        assert_eq!(driver.received(next), &[capacity as u8]);
        driver.release_receive(next.slot());
        for datagram in held {
            driver.release_receive(datagram.slot());
        }
    }

    #[test]
    fn send_slot_transmits_without_an_intermediate_copy() {
        let receiver = loopback_socket();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let destination = receiver.local_addr().unwrap();
        let mut driver = UdpDriver::new(loopback_socket(), test_config(1, false)).unwrap();

        let mut slot = driver.try_send_slot().expect("free transmit slot");
        slot.buffer_mut()[..5].copy_from_slice(b"hello");
        slot.submit(5, destination, Instant::now()).unwrap();
        let IoEvent::Transmitted(completion) = poll_until_event(&mut driver) else {
            panic!("expected transmit completion");
        };
        assert_eq!(completion.len(), 5);
        assert_eq!(completion.segments(), 1);

        let mut received = [0; 16];
        let (length, _) = receiver.recv_from(&mut received).unwrap();
        assert_eq!(&received[..length], b"hello");
    }

    #[test]
    fn abandoned_send_slot_returns_to_the_driver() {
        let mut driver = UdpDriver::new(loopback_socket(), test_config(1, false)).unwrap();
        let capacity = driver.free_transmit.len();
        drop(driver.try_send_slot().expect("free transmit slot"));
        assert_eq!(driver.free_transmit.len(), capacity);
    }

    #[test]
    fn transmit_slots_are_starved_until_a_send_completes() {
        let receiver = loopback_socket();
        let destination = receiver.local_addr().unwrap();
        let mut driver = UdpDriver::new(loopback_socket(), test_config(1, false)).unwrap();
        let capacity = driver.free_transmit.len();
        for value in 0..capacity {
            let mut slot = driver.try_send_slot().expect("free transmit slot");
            slot.buffer_mut()[0] = value as u8;
            slot.submit(1, destination, Instant::now()).unwrap();
        }

        assert!(driver.try_send_slot().is_none());
        let IoEvent::Transmitted(_) = poll_until_event(&mut driver) else {
            panic!("expected transmit completion");
        };
        assert!(driver.try_send_slot().is_some());
    }

    #[test]
    fn one_gso_send_delivers_each_quic_datagram() {
        const SEGMENTS: usize = 4;
        const SEGMENT_SIZE: usize = 1200;

        let receiver = loopback_socket();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let destination = receiver.local_addr().unwrap();
        let mut driver = UdpDriver::new(loopback_socket(), test_config(SEGMENTS, false)).unwrap();

        let mut slot = driver.try_send_slot().expect("free GSO slot");
        for segment in 0..SEGMENTS {
            slot.buffer_mut()[segment * SEGMENT_SIZE..(segment + 1) * SEGMENT_SIZE]
                .fill(segment as u8);
        }
        slot.submit(SEGMENTS * SEGMENT_SIZE, destination, Instant::now())
            .unwrap();

        let IoEvent::Transmitted(completion) = poll_until_event(&mut driver) else {
            panic!("expected GSO transmit completion");
        };
        assert_eq!(completion.len(), SEGMENTS * SEGMENT_SIZE);
        assert_eq!(completion.segments(), SEGMENTS);
        assert_eq!(driver.stats().datagrams_sent, SEGMENTS as u64);
        assert_eq!(driver.stats().sends_completed, 1);

        let mut datagram = [0; SEGMENT_SIZE + 1];
        for segment in 0..SEGMENTS {
            let (length, _) = receiver.recv_from(&mut datagram).unwrap();
            assert_eq!(length, SEGMENT_SIZE);
            assert!(datagram[..length].iter().all(|byte| *byte == segment as u8));
        }
    }

    #[test]
    fn gro_reports_segments_without_copying_the_aggregate() {
        const SEGMENTS: usize = 4;
        const SEGMENT_SIZE: usize = 1200;

        let mut receiver = UdpDriver::new(loopback_socket(), test_config(SEGMENTS, true)).unwrap();
        let destination = receiver.local_addr().unwrap();
        let mut sender = UdpDriver::new(loopback_socket(), test_config(SEGMENTS, false)).unwrap();
        let mut slot = sender.try_send_slot().unwrap();
        for segment in 0..SEGMENTS {
            slot.buffer_mut()[segment * SEGMENT_SIZE..(segment + 1) * SEGMENT_SIZE]
                .fill(segment as u8);
        }
        slot.submit(SEGMENTS * SEGMENT_SIZE, destination, Instant::now())
            .unwrap();
        let IoEvent::Transmitted(_) = poll_until_event(&mut sender) else {
            panic!("expected GSO transmit completion");
        };

        let IoEvent::Received(datagram) = poll_until_event(&mut receiver) else {
            panic!("expected GRO receive completion");
        };
        assert_eq!(datagram.len(), SEGMENTS * SEGMENT_SIZE);
        assert_eq!(datagram.segment_size(), SEGMENT_SIZE);
        let aggregate = receiver.received(datagram);
        for segment in 0..SEGMENTS {
            assert!(
                aggregate[segment * SEGMENT_SIZE..(segment + 1) * SEGMENT_SIZE]
                    .iter()
                    .all(|byte| *byte == segment as u8)
            );
        }
        receiver.release_receive(datagram.slot());
        assert_eq!(receiver.stats().datagrams_received, SEGMENTS as u64);
        assert_eq!(receiver.stats().receives_completed, 1);
    }

    #[test]
    fn pacing_deadline_delays_submission() {
        let receiver = loopback_socket();
        receiver
            .set_nonblocking(true)
            .expect("make receiver nonblocking");
        let destination = receiver.local_addr().unwrap();
        let mut driver = UdpDriver::new(loopback_socket(), test_config(1, false)).unwrap();
        let send_at = Instant::now() + Duration::from_millis(40);

        let mut slot = driver.try_send_slot().unwrap();
        slot.buffer_mut()[0] = 7;
        slot.submit(1, destination, send_at).unwrap();
        driver.poll(Duration::from_millis(5)).unwrap();
        assert!(driver.try_next_event().is_none());
        let mut byte = [0; 1];
        assert_eq!(
            receiver.recv_from(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        let IoEvent::Transmitted(_) = poll_until_event(&mut driver) else {
            panic!("expected paced transmit completion");
        };
        let (length, _) = receiver.recv_from(&mut byte).unwrap();
        assert_eq!(length, 1);
        assert_eq!(byte[0], 7);
    }

    #[test]
    fn notifier_wakes_a_blocked_driver() {
        let mut driver = UdpDriver::new(loopback_socket(), test_config(1, false)).unwrap();
        let notifier = driver.notifier();
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            notifier.notify();
        });

        let started = Instant::now();
        let outcome = driver.poll(Duration::from_secs(2)).unwrap();
        assert!(outcome.woken());
        assert!(!outcome.timed_out());
        assert!(started.elapsed() < Duration::from_secs(1));
        thread.join().unwrap();
    }

    #[test]
    fn pacing_wait_is_not_reported_as_caller_timeout() {
        let caller_timeout = Duration::from_millis(50);

        assert_eq!(
            poll_wait(caller_timeout, Some(Duration::from_millis(10))),
            (Duration::from_millis(10), false)
        );
        assert_eq!(
            poll_wait(caller_timeout, Some(Duration::ZERO)),
            (caller_timeout, true)
        );
        assert_eq!(poll_wait(caller_timeout, None), (caller_timeout, true));
    }

    #[test]
    fn malformed_receive_metadata_recycles_slot() {
        let mut driver = UdpDriver::new(loopback_socket(), test_config(1, false)).unwrap();
        let slot = driver
            .pending_receive
            .pop_front()
            .expect("receive slot should be pending");
        let receive = &mut driver.receive_slots[slot];
        receive.prepare();
        receive.state = ReceiveState::Submitted;
        receive.message.msg_flags = libc::MSG_TRUNC;

        assert_eq!(
            driver
                .complete_receive(slot, 1)
                .expect_err("truncated datagram should be rejected")
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(driver.receive_slots[slot].state, ReceiveState::Pending);
        assert_eq!(driver.pending_receive.back(), Some(&slot));
    }
}
