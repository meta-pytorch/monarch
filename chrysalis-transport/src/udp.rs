/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::io::IoSliceMut;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::task::Context;
use std::task::Poll;

use tokio::io::ReadBuf;

use crate::DatagramAddr;
use crate::DatagramRecvMeta;
use crate::DatagramSocket;
use crate::DatagramTransmit;
use crate::datagram::DATAGRAM_BATCH_SIZE;
use crate::shutdown::ShutdownState;

/// A UDP datagram binding.
#[derive(Debug)]
pub struct UdpSocket {
    socket: Option<tokio::net::UdpSocket>,
    address: SocketAddr,
    datagram_addr: DatagramAddr,
    shutdown_state: ShutdownState,
}

impl UdpSocket {
    /// Binds a UDP socket.
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        let socket = tokio::net::UdpSocket::bind(address).await?;
        let address = socket.local_addr()?;
        Ok(Self {
            socket: Some(socket),
            address,
            datagram_addr: udp_addr(address),
            shutdown_state: ShutdownState::default(),
        })
    }

    /// Idempotently requests transport shutdown.
    pub fn shutdown(&self) {
        if self.shutdown_state.shutdown() {
            self.shutdown_state.terminate();
        }
    }

    /// Waits until transport shutdown has completed.
    pub async fn join(&self) {
        self.shutdown_state.join().await;
    }

    /// Returns the bound UDP address.
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Transfers the bound socket to a runtime-neutral UDP driver.
    pub fn into_std(mut self) -> io::Result<std::net::UdpSocket> {
        self.shutdown_state.shutdown();
        self.shutdown_state.terminate();
        self.socket
            .take()
            .expect("live UDP socket should own its Tokio binding")
            .into_std()
    }

    /// Encodes a UDP socket address for use with [`DatagramSocket::try_send_to`].
    pub fn datagram_addr(address: SocketAddr) -> DatagramAddr {
        udp_addr(address)
    }
}

impl DatagramSocket for UdpSocket {
    fn shutdown(&self) {
        UdpSocket::shutdown(self);
    }

    fn join(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(UdpSocket::join(self))
    }

    fn local_addr(&self) -> &DatagramAddr {
        &self.datagram_addr
    }

    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        match self.try_send_transmit(&DatagramTransmit {
            destination,
            contents: datagram,
            segment_size: None,
            ecn: None,
            source_ip: None,
        })? {
            0 => Err(io::ErrorKind::WouldBlock.into()),
            1 => Ok(()),
            _ => unreachable!("one datagram transmit accepted multiple datagrams"),
        }
    }

    fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
        if !self.shutdown_state.is_running() {
            return Err(shutdown_error("UDP transport is shut down"));
        }
        let destination = decode_udp_addr(transmit.destination)?;
        let segment_size = transmit.segment_size.unwrap_or(transmit.contents.len());
        if segment_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP segment size must be nonzero",
            ));
        }
        let mut sent = 0;
        for datagram in transmit.contents.chunks(segment_size) {
            match self.socket().try_send_to(datagram, destination) {
                Ok(written) if written == datagram.len() => sent += 1,
                Ok(_) => {
                    if sent > 0 {
                        return Ok(sent);
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "UDP send accepted a partial datagram",
                    ));
                }
                Err(error) if sent > 0 || error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(sent);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(sent)
    }

    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        _transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("UDP transport is shut down")));
        }
        self.shutdown_state.register_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("UDP transport is shut down")));
        }
        self.socket().poll_send_ready(cx)
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("UDP transport is shut down")));
        }
        self.shutdown_state.register_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("UDP transport is shut down")));
        }
        match self.socket().poll_recv_from(cx, buffer) {
            Poll::Ready(Ok(source)) => Poll::Ready(Ok(udp_addr(source))),
            Poll::Ready(Err(error)) => {
                self.shutdown();
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        meta: &mut [DatagramRecvMeta],
    ) -> Poll<io::Result<usize>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("UDP transport is shut down")));
        }
        self.shutdown_state.register_waker(cx.waker());
        let capacity = buffers.len().min(meta.len()).min(DATAGRAM_BATCH_SIZE);
        if capacity == 0 {
            return Poll::Ready(Ok(0));
        }
        let mut count = 0;
        while count < capacity {
            let mut buffer = ReadBuf::new(&mut buffers[count]);
            match self.socket().poll_recv_from(cx, &mut buffer) {
                Poll::Ready(Ok(source)) => {
                    let len = buffer.filled().len();
                    meta[count] = DatagramRecvMeta {
                        source: udp_addr(source),
                        len,
                        stride: len,
                        ecn: None,
                        destination_ip: None,
                    };
                    count += 1;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending if count == 0 => return Poll::Pending,
                Poll::Pending => return Poll::Ready(Ok(count)),
            }
        }
        Poll::Ready(Ok(count))
    }

    fn max_transmit_segments(&self) -> usize {
        DATAGRAM_BATCH_SIZE
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        true
    }
}

impl UdpSocket {
    fn socket(&self) -> &tokio::net::UdpSocket {
        self.socket
            .as_ref()
            .expect("UDP socket was transferred to another transport")
    }
}

fn udp_addr(address: SocketAddr) -> DatagramAddr {
    let mut bytes = Vec::new();
    match address {
        SocketAddr::V4(address) => {
            bytes.push(4);
            bytes.extend_from_slice(&address.ip().octets());
            bytes.extend_from_slice(&address.port().to_be_bytes());
        }
        SocketAddr::V6(address) => {
            bytes.push(6);
            bytes.extend_from_slice(&address.ip().octets());
            bytes.extend_from_slice(&address.port().to_be_bytes());
            bytes.extend_from_slice(&address.flowinfo().to_be_bytes());
            bytes.extend_from_slice(&address.scope_id().to_be_bytes());
        }
    }
    DatagramAddr::new("udp", bytes)
}

pub(crate) fn decode_udp_addr(address: &DatagramAddr) -> io::Result<SocketAddr> {
    let bytes = address.opaque();
    match (address.scheme(), bytes.first().copied(), bytes.len()) {
        ("udp", Some(4), 7) => {
            let ip = Ipv4Addr::from(<[u8; 4]>::try_from(&bytes[1..5]).expect("checked length"));
            let port =
                u16::from_be_bytes(<[u8; 2]>::try_from(&bytes[5..7]).expect("checked length"));
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        ("udp", Some(6), 27) => {
            let ip = Ipv6Addr::from(<[u8; 16]>::try_from(&bytes[1..17]).expect("checked length"));
            let port =
                u16::from_be_bytes(<[u8; 2]>::try_from(&bytes[17..19]).expect("checked length"));
            let flowinfo =
                u32::from_be_bytes(<[u8; 4]>::try_from(&bytes[19..23]).expect("checked length"));
            let scope_id =
                u32::from_be_bytes(<[u8; 4]>::try_from(&bytes[23..27]).expect("checked length"));
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid UDP datagram address",
        )),
    }
}

fn shutdown_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, message)
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn round_trip_preserves_boundaries_and_source_addresses() {
        let first = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind first UDP socket");
        let second = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind second UDP socket");
        let mut buffer = [0; 64];
        let request = DatagramTransmit {
            destination: second.local_addr(),
            contents: b"request",
            segment_size: None,
            ecn: None,
            source_ip: None,
        };

        poll_fn(|cx| first.poll_send_ready(cx, &request))
            .await
            .expect("wait for UDP request send readiness");
        first.try_send_transmit(&request).expect("send UDP request");
        let (request_len, request_source) = second
            .recv_from(&mut buffer)
            .await
            .expect("receive UDP request");
        assert_eq!(&buffer[..request_len], b"request");
        assert_eq!(&request_source, first.local_addr());
        let response = DatagramTransmit {
            destination: &request_source,
            contents: b"response",
            segment_size: None,
            ecn: None,
            source_ip: None,
        };

        poll_fn(|cx| second.poll_send_ready(cx, &response))
            .await
            .expect("wait for UDP response send readiness");
        second
            .try_send_transmit(&response)
            .expect("send UDP response");
        let (response_len, response_source) = first
            .recv_from(&mut buffer)
            .await
            .expect("receive UDP response");
        assert_eq!(&buffer[..response_len], b"response");
        assert_eq!(&response_source, second.local_addr());
    }

    #[tokio::test]
    async fn segmented_send_preserves_udp_datagram_boundaries() {
        let first = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind first UDP socket");
        let second = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind second UDP socket");
        if first.max_transmit_segments() == 1 {
            return;
        }
        let segment_size = 1_200;
        let payload = (0..4)
            .flat_map(|index| std::iter::repeat_n(index, segment_size))
            .collect::<Vec<_>>();
        let transmit = DatagramTransmit {
            destination: second.local_addr(),
            contents: &payload,
            segment_size: Some(segment_size),
            ecn: None,
            source_ip: None,
        };

        poll_fn(|cx| first.poll_send_ready(cx, &transmit))
            .await
            .expect("wait for segmented UDP send readiness");
        let sent = first
            .try_send_transmit(&transmit)
            .expect("send segmented UDP payload");
        assert_eq!(sent, transmit.segment_count());

        let mut storage = vec![0; 65_535 * DATAGRAM_BATCH_SIZE];
        let mut buffers = storage
            .chunks_mut(65_535)
            .map(IoSliceMut::new)
            .collect::<Vec<_>>();
        let mut meta = vec![DatagramRecvMeta::default(); DATAGRAM_BATCH_SIZE];
        let mut segment = 0;
        while segment < 4 {
            let count = poll_fn(|cx| second.poll_recv(cx, &mut buffers, &mut meta))
                .await
                .expect("receive segmented UDP payload");
            for (buffer, received) in buffers[..count].iter().zip(&meta[..count]) {
                for bytes in buffer[..received.len].chunks(received.stride) {
                    assert_eq!(bytes, vec![segment as u8; segment_size]);
                    segment += 1;
                }
            }
        }
        assert_eq!(segment, 4);
    }

    #[tokio::test]
    async fn single_receive_splits_coalesced_udp_datagrams() {
        let first = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind first UDP socket");
        let second = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind second UDP socket");
        if first.max_transmit_segments() == 1 {
            return;
        }
        let segment_size = 1_200;
        let payload = (0..4)
            .flat_map(|index| std::iter::repeat_n(index, segment_size))
            .collect::<Vec<_>>();
        let transmit = DatagramTransmit {
            destination: second.local_addr(),
            contents: &payload,
            segment_size: Some(segment_size),
            ecn: None,
            source_ip: None,
        };

        poll_fn(|cx| first.poll_send_ready(cx, &transmit))
            .await
            .expect("wait for segmented UDP send readiness");
        let sent = first
            .try_send_transmit(&transmit)
            .expect("send segmented UDP payload");
        assert_eq!(sent, transmit.segment_count());

        let mut buffer = vec![0; segment_size];
        for expected in 0..4 {
            let (len, source) = second
                .recv_from(&mut buffer)
                .await
                .expect("receive one UDP segment");
            assert_eq!(source, *first.local_addr());
            assert_eq!(len, segment_size);
            assert_eq!(&buffer[..len], vec![expected; segment_size]);
        }
    }

    #[tokio::test]
    async fn shutdown_wakes_receive_rejects_sends_and_joins() {
        let transport = Arc::new(
            UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .expect("bind UDP socket"),
        );
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
    }
}
