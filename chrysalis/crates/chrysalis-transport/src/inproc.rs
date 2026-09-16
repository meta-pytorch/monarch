/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use tokio::io::ReadBuf;

use crate::DatagramAddr;
use crate::DatagramSocket;
use crate::DatagramTransmit;
use crate::shutdown::ShutdownState;

/// Default maximum number of datagrams queued for each in-process endpoint.
const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Process-wide source of IDs that distinguish independent in-process networks.
static NEXT_NETWORK_ID: AtomicU64 = AtomicU64::new(1);

/// An address within one in-process datagram network.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InprocAddr {
    network: u64,
    endpoint: u64,
}

impl InprocAddr {
    fn datagram_addr(self) -> DatagramAddr {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.network.to_be_bytes());
        bytes[8..].copy_from_slice(&self.endpoint.to_be_bytes());
        DatagramAddr::new("inproc", bytes)
    }
}

/// A bounded in-process datagram network.
#[derive(Debug)]
pub struct InprocNetwork {
    id: u64,
    queue_capacity: NonZeroUsize,
    endpoints: Mutex<HashMap<u64, Weak<InprocEndpoint>>>,
}

impl InprocNetwork {
    /// Constructs a network with a bounded queue for each binding.
    pub fn new(queue_capacity: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_NETWORK_ID.fetch_add(1, Ordering::Relaxed),
            queue_capacity,
            endpoints: Mutex::new(HashMap::new()),
        })
    }

    /// Binds `endpoint` within this network.
    pub fn bind(self: &Arc<Self>, endpoint: u64) -> io::Result<InprocSocket> {
        let state = Arc::new(InprocEndpoint {
            queue: Mutex::new(InprocQueue::default()),
            shutdown_state: ShutdownState::default(),
        });
        let mut endpoints = self
            .endpoints
            .lock()
            .expect("in-process network lock poisoned");
        match endpoints.entry(endpoint) {
            Entry::Occupied(mut entry) if entry.get().upgrade().is_none() => {
                entry.insert(Arc::downgrade(&state));
            }
            Entry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "in-process address already bound",
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::downgrade(&state));
            }
        }
        drop(endpoints);
        let address = InprocAddr {
            network: self.id,
            endpoint,
        };
        Ok(InprocSocket {
            network: self.clone(),
            address,
            datagram_addr: address.datagram_addr(),
            state,
        })
    }

    fn endpoint(&self, destination: InprocAddr) -> io::Result<Arc<InprocEndpoint>> {
        if destination.network != self.id {
            return Err(io::Error::new(
                io::ErrorKind::HostUnreachable,
                "in-process destination belongs to another network",
            ));
        }
        self.endpoints
            .lock()
            .expect("in-process network lock poisoned")
            .get(&destination.endpoint)
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::HostUnreachable, "unknown in-process address")
            })
    }

    fn send(&self, source: InprocAddr, destination: InprocAddr, bytes: &[u8]) -> io::Result<()> {
        let state = self.endpoint(destination)?;
        if !state.shutdown_state.is_running() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "in-process destination is shut down",
            ));
        }
        let mut queue = state.queue.lock().expect("in-process queue lock poisoned");
        if queue.datagrams.len() >= self.queue_capacity.get() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "in-process datagram queue is full",
            ));
        }
        queue.datagrams.push_back(InprocDatagram {
            source,
            bytes: bytes.to_vec(),
        });
        let waker = queue.waker.take();
        drop(queue);
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    fn unbind(&self, endpoint: u64, state: &Arc<InprocEndpoint>) {
        let mut endpoints = self
            .endpoints
            .lock()
            .expect("in-process network lock poisoned");
        let removed = if endpoints
            .get(&endpoint)
            .and_then(Weak::upgrade)
            .is_some_and(|bound| Arc::ptr_eq(&bound, state))
        {
            endpoints.remove(&endpoint);
            true
        } else {
            false
        };
        drop(endpoints);
        if removed {
            state.wake_senders();
        }
    }
}

impl Default for InprocNetwork {
    fn default() -> Self {
        Self {
            id: NEXT_NETWORK_ID.fetch_add(1, Ordering::Relaxed),
            queue_capacity: NonZeroUsize::new(DEFAULT_QUEUE_CAPACITY)
                .expect("default in-process queue capacity is nonzero"),
            endpoints: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Debug)]
struct InprocEndpoint {
    queue: Mutex<InprocQueue>,
    shutdown_state: ShutdownState,
}

impl InprocEndpoint {
    fn wake_senders(&self) {
        let wakers = std::mem::take(
            &mut self
                .queue
                .lock()
                .expect("in-process queue lock poisoned")
                .send_wakers,
        );
        for waker in wakers {
            waker.wake();
        }
    }
}

#[derive(Debug, Default)]
struct InprocQueue {
    datagrams: VecDeque<InprocDatagram>,
    waker: Option<Waker>,
    send_wakers: Vec<Waker>,
}

#[derive(Debug)]
struct InprocDatagram {
    source: InprocAddr,
    bytes: Vec<u8>,
}

/// One binding on an in-process datagram network.
#[derive(Debug)]
pub struct InprocSocket {
    network: Arc<InprocNetwork>,
    address: InprocAddr,
    datagram_addr: DatagramAddr,
    state: Arc<InprocEndpoint>,
}

impl InprocSocket {
    /// Idempotently requests transport shutdown.
    pub fn shutdown(&self) {
        if self.state.shutdown_state.shutdown() {
            self.network.unbind(self.address.endpoint, &self.state);
            self.state.shutdown_state.terminate();
        }
    }

    /// Waits until transport shutdown has completed.
    pub async fn join(&self) {
        self.state.shutdown_state.join().await;
    }

    /// Returns the typed in-process address.
    pub const fn address(&self) -> InprocAddr {
        self.address
    }

    /// Encodes an in-process address for use with [`DatagramSocket::try_send_to`].
    pub fn datagram_addr(address: InprocAddr) -> DatagramAddr {
        address.datagram_addr()
    }
}

impl DatagramSocket for InprocSocket {
    fn shutdown(&self) {
        InprocSocket::shutdown(self);
    }

    fn join(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(InprocSocket::join(self))
    }

    fn local_addr(&self) -> &DatagramAddr {
        &self.datagram_addr
    }

    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        if !self.state.shutdown_state.is_running() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "in-process transport is shut down",
            ));
        }
        let destination = decode_inproc_addr(destination)?;
        self.network.send(self.address, destination, datagram)
    }

    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.state.shutdown_state.is_running() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "in-process transport is shut down",
            )));
        }
        self.state.shutdown_state.register_waker(cx.waker());
        if !self.state.shutdown_state.is_running() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "in-process transport is shut down",
            )));
        }
        let destination = match decode_inproc_addr(transmit.destination)
            .and_then(|destination| self.network.endpoint(destination))
        {
            Ok(destination) => destination,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let mut queue = destination
            .queue
            .lock()
            .expect("in-process queue lock poisoned");
        if !destination.shutdown_state.is_running() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "in-process destination is shut down",
            )));
        }
        if queue.datagrams.len() < self.network.queue_capacity.get() {
            return Poll::Ready(Ok(()));
        }
        if !queue
            .send_wakers
            .iter()
            .any(|registered| registered.will_wake(cx.waker()))
        {
            queue.send_wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>> {
        if !self.state.shutdown_state.is_running() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "in-process transport is shut down",
            )));
        }
        self.state.shutdown_state.register_waker(cx.waker());
        if !self.state.shutdown_state.is_running() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "in-process transport is shut down",
            )));
        }
        let mut queue = self
            .state
            .queue
            .lock()
            .expect("in-process queue lock poisoned");
        let Some(datagram) = queue.datagrams.pop_front() else {
            if queue
                .waker
                .as_ref()
                .is_none_or(|waker| !waker.will_wake(cx.waker()))
            {
                queue.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        };
        drop(queue);
        self.state.wake_senders();
        if datagram.bytes.len() > buffer.remaining() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "datagram exceeds receive buffer",
            )));
        }
        buffer.put_slice(&datagram.bytes);
        Poll::Ready(Ok(datagram.source.datagram_addr()))
    }
}

fn decode_inproc_addr(address: &DatagramAddr) -> io::Result<InprocAddr> {
    let bytes = address.opaque();
    if address.scheme() != "inproc" || bytes.len() != 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid in-process datagram address",
        ));
    }
    Ok(InprocAddr {
        network: u64::from_be_bytes(bytes[..8].try_into().expect("checked length")),
        endpoint: u64::from_be_bytes(bytes[8..].try_into().expect("checked length")),
    })
}

impl Drop for InprocSocket {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[test]
    fn duplicate_bind_is_rejected() {
        let network = Arc::new(InprocNetwork::default());
        let _first = network.bind(1).expect("bind first endpoint");

        let error = network.bind(1).expect_err("reject duplicate endpoint");

        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[tokio::test]
    async fn shutdown_wakes_receive_rejects_sends_and_joins() {
        let network = Arc::new(InprocNetwork::default());
        let transport = Arc::new(network.bind(1).expect("bind endpoint"));
        let destination = transport.local_addr().clone();
        let receiver_transport = transport.clone();
        let receiver = tokio::spawn(async move {
            let mut buffer = [0; 64];
            receiver_transport.recv_from(&mut buffer).await
        });
        tokio::task::yield_now().await;

        transport.shutdown();
        transport.shutdown();
        timeout(Duration::from_secs(1), transport.join())
            .await
            .expect("join timed out");
        let error = timeout(Duration::from_secs(1), receiver)
            .await
            .expect("receive task timed out")
            .expect("receive task failed")
            .expect_err("receive must fail after shutdown");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(
            transport
                .try_send_to(b"after shutdown", &destination)
                .expect_err("send after shutdown must fail")
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        let _replacement = network.bind(1).expect("rebind shut down endpoint");
    }

    #[tokio::test]
    async fn round_trip_preserves_boundaries_and_source_addresses() {
        let network = Arc::new(InprocNetwork::default());
        let first = network.bind(1).expect("bind first endpoint");
        let second = network.bind(2).expect("bind second endpoint");
        let mut buffer = [0; 64];

        first
            .try_send_to(b"request", second.local_addr())
            .expect("send request");
        let (request_len, request_source) = second
            .recv_from(&mut buffer)
            .await
            .expect("receive request");
        assert_eq!(&buffer[..request_len], b"request");
        assert_eq!(&request_source, first.local_addr());

        second
            .try_send_to(b"response", &request_source)
            .expect("send response");
        let (response_len, response_source) = first
            .recv_from(&mut buffer)
            .await
            .expect("receive response");
        assert_eq!(&buffer[..response_len], b"response");
        assert_eq!(&response_source, second.local_addr());
    }

    #[tokio::test]
    async fn poll_recv_from_appends_to_read_buf() {
        let network = Arc::new(InprocNetwork::default());
        let first = network.bind(1).expect("bind first endpoint");
        let second = network.bind(2).expect("bind second endpoint");
        first
            .try_send_to(b"payload", second.local_addr())
            .expect("send datagram");
        let mut bytes = [0; 64];
        let mut buffer = ReadBuf::new(&mut bytes);
        buffer.put_slice(b"prefix:");

        let source = poll_fn(|cx| second.poll_recv_from(cx, &mut buffer))
            .await
            .expect("receive datagram");

        assert_eq!(buffer.filled(), b"prefix:payload");
        assert_eq!(&source, first.local_addr());
    }

    #[test]
    fn full_queue_applies_backpressure() {
        let network = InprocNetwork::new(NonZeroUsize::new(1).expect("nonzero capacity"));
        let first = network.bind(1).expect("bind first endpoint");
        let second = network.bind(2).expect("bind second endpoint");
        first
            .try_send_to(b"first", second.local_addr())
            .expect("fill queue");
        let error = first
            .try_send_to(b"second", second.local_addr())
            .expect_err("queue is full");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn consuming_a_datagram_wakes_send_readiness() {
        let network = InprocNetwork::new(NonZeroUsize::new(1).expect("nonzero capacity"));
        let first = network.bind(1).expect("bind first endpoint");
        let second = network.bind(2).expect("bind second endpoint");
        first
            .try_send_to(b"first", second.local_addr())
            .expect("fill queue");
        assert_eq!(
            first
                .try_send_to(b"second", second.local_addr())
                .expect_err("queue is full")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let transmit = DatagramTransmit {
            destination: second.local_addr(),
            contents: b"second",
            segment_size: None,
            ecn: None,
            source_ip: None,
        };

        let mut readiness = Box::pin(poll_fn(|cx| first.poll_send_ready(cx, &transmit)));
        assert!(
            timeout(Duration::from_millis(10), &mut readiness)
                .await
                .is_err(),
            "send readiness must remain pending while the queue is full"
        );
        let mut buffer = [0; 64];
        second
            .recv_from(&mut buffer)
            .await
            .expect("consume datagram");
        timeout(Duration::from_secs(1), readiness)
            .await
            .expect("send readiness was not woken")
            .expect("send readiness failed");
        first
            .try_send_to(b"second", second.local_addr())
            .expect("retry send after readiness");
    }

    #[tokio::test]
    async fn send_readiness_is_scoped_to_the_destination() {
        let network = InprocNetwork::new(NonZeroUsize::new(1).expect("nonzero capacity"));
        let sender = network.bind(1).expect("bind sender");
        let first = network.bind(2).expect("bind first destination");
        let second = network.bind(3).expect("bind second destination");
        sender
            .try_send_to(b"first queued", first.local_addr())
            .expect("fill first queue");
        sender
            .try_send_to(b"second queued", second.local_addr())
            .expect("fill second queue");
        let first_transmit = DatagramTransmit {
            destination: first.local_addr(),
            contents: b"first blocked",
            segment_size: None,
            ecn: None,
            source_ip: None,
        };
        let second_transmit = DatagramTransmit {
            destination: second.local_addr(),
            contents: b"second blocked",
            segment_size: None,
            ecn: None,
            source_ip: None,
        };
        assert_eq!(
            sender
                .try_send_transmit(&first_transmit)
                .expect("report blocked first queue"),
            0
        );
        assert_eq!(
            sender
                .try_send_transmit(&second_transmit)
                .expect("report blocked second queue"),
            0
        );

        let mut first_readiness =
            Box::pin(poll_fn(|cx| sender.poll_send_ready(cx, &first_transmit)));
        let mut second_readiness =
            Box::pin(poll_fn(|cx| sender.poll_send_ready(cx, &second_transmit)));
        assert!(
            timeout(Duration::from_millis(10), &mut first_readiness)
                .await
                .is_err(),
            "first destination must remain blocked while its queue is full"
        );
        assert!(
            timeout(Duration::from_millis(10), &mut second_readiness)
                .await
                .is_err(),
            "second destination must remain blocked while its queue is full"
        );

        let mut buffer = [0; 64];
        first
            .recv_from(&mut buffer)
            .await
            .expect("drain first destination");
        timeout(Duration::from_secs(1), first_readiness)
            .await
            .expect("first destination readiness was not woken")
            .expect("first destination readiness failed");
        assert!(
            timeout(Duration::from_millis(10), &mut second_readiness)
                .await
                .is_err(),
            "draining the first destination must not wake the second"
        );

        second
            .recv_from(&mut buffer)
            .await
            .expect("drain second destination");
        timeout(Duration::from_secs(1), second_readiness)
            .await
            .expect("second destination readiness was not woken")
            .expect("second destination readiness failed");
    }
}
