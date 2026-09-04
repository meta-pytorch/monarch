/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::future::Future;
use std::io;
use std::io::IoSliceMut;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use tokio::io::ReadBuf;

/// Maximum number of datagrams processed in one carrier I/O batch.
///
/// This bounds segmented sends, receive polling work, and the fixed arrays used
/// to prepare Unix `recvmmsg` calls.
pub(crate) const DATAGRAM_BATCH_SIZE: usize = 32;

/// A carrier-neutral transmission containing one or more equal-sized datagrams.
#[derive(Clone, Copy, Debug)]
pub struct DatagramTransmit<'a> {
    /// Address selected for this transmission.
    pub destination: &'a DatagramAddr,
    /// Concatenated datagram bytes.
    pub contents: &'a [u8],
    /// Segment size for concatenated datagrams, or `None` when `contents` is one
    /// datagram. A final segment may be shorter.
    pub segment_size: Option<usize>,
    /// Explicit congestion notification bits.
    pub ecn: Option<u8>,
    /// Optional transport source IP.
    pub source_ip: Option<IpAddr>,
}

impl DatagramTransmit<'_> {
    /// Returns the number of datagrams encoded by this transmission.
    pub fn segment_count(&self) -> usize {
        self.segment_size
            .filter(|size| *size < self.contents.len())
            .map(|size| self.contents.len().div_ceil(size))
            .unwrap_or(1)
    }
}

/// Metadata for one receive buffer containing one or more datagrams.
#[derive(Clone, Debug)]
pub struct DatagramRecvMeta {
    /// Address that sent the datagram or coalesced datagrams.
    pub source: DatagramAddr,
    /// Number of initialized bytes in the receive buffer.
    pub len: usize,
    /// Size of each datagram except a possibly shorter final segment.
    pub stride: usize,
    /// Explicit congestion notification bits.
    pub ecn: Option<u8>,
    /// Destination IP observed by the carrier.
    pub destination_ip: Option<IpAddr>,
}

impl Default for DatagramRecvMeta {
    fn default() -> Self {
        Self {
            source: DatagramAddr::new("", []),
            len: 0,
            stride: 0,
            ecn: None,
            destination_ip: None,
        }
    }
}

/// A stable, transport-qualified datagram address.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct DatagramAddr {
    scheme: Arc<str>,
    opaque: Arc<[u8]>,
}

impl DatagramAddr {
    /// Constructs an opaque address for a transport scheme.
    pub fn new(scheme: impl Into<Arc<str>>, opaque: impl Into<Arc<[u8]>>) -> Self {
        Self {
            scheme: scheme.into(),
            opaque: opaque.into(),
        }
    }

    /// Returns the transport scheme.
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Returns the transport-specific address bytes.
    pub fn opaque(&self) -> &[u8] {
        &self.opaque
    }
}

impl fmt::Debug for DatagramAddr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatagramAddr")
            .field("scheme", &self.scheme)
            .field("opaque", &self.opaque)
            .finish()
    }
}

/// A bound, nonblocking datagram socket.
///
/// At most one receive operation may be pending on a socket across
/// [`DatagramSocket::poll_recv_from`] and [`DatagramSocket::poll_recv`].
/// Implementations may retain only the most recently supplied receive waker,
/// so callers must serialize receive operations.
pub trait DatagramSocket: fmt::Debug + Send + Sync + 'static {
    /// Idempotently requests transport shutdown without waiting for completion.
    fn shutdown(&self);

    /// Waits until all tasks owned by this transport have terminated.
    fn join(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Returns this binding's stable address.
    fn local_addr(&self) -> &DatagramAddr;

    /// Attempts to send one complete datagram to `destination`.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when the socket is not currently writable.
    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()>;

    /// Attempts to send one or more segmented datagrams.
    ///
    /// Returns a number in `0..=transmit.segment_count()` identifying the
    /// accepted prefix. Zero means that the socket would block. An error means
    /// that no datagram was accepted because of another failure.
    fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
        let segment_size = transmit.segment_size.unwrap_or(transmit.contents.len());
        if segment_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "datagram segment size must be nonzero",
            ));
        }
        let mut sent = 0;
        for datagram in transmit.contents.chunks(segment_size) {
            match self.try_send_to(datagram, transmit.destination) {
                Ok(()) => sent += 1,
                Err(error) if sent > 0 || error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(sent);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(sent)
    }

    /// Polls until `transmit` should be retried after a send accepts no datagrams.
    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>>;

    /// Polls for one complete datagram.
    ///
    /// The caller must not leave another receive operation pending on this
    /// socket.
    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>>;

    /// Polls for datagrams into caller-owned buffers and metadata slots.
    ///
    /// Each call borrows both slices only until it returns. On [`Poll::Pending`],
    /// the implementation retains neither slice, and no entries are valid; the
    /// caller may reuse or replace the storage before polling again.
    ///
    /// The returned count identifies the valid prefix of both slices. For each
    /// index below that count, the buffer contains the payload described by the
    /// metadata at the same index. Caller-owned storage permits reuse without a
    /// per-poll allocation.
    ///
    /// The caller must not leave another receive operation pending on this
    /// socket.
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        meta: &mut [DatagramRecvMeta],
    ) -> Poll<io::Result<usize>> {
        let Some(buffer) = buffers.first_mut() else {
            return Poll::Ready(Ok(0));
        };
        let Some(meta) = meta.first_mut() else {
            return Poll::Ready(Ok(0));
        };
        let mut buffer = ReadBuf::new(buffer);
        match self.poll_recv_from(cx, &mut buffer) {
            Poll::Ready(Ok(source)) => {
                let len = buffer.filled().len();
                *meta = DatagramRecvMeta {
                    source,
                    len,
                    stride: len,
                    ecn: None,
                    destination_ip: None,
                };
                Poll::Ready(Ok(1))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Preferred maximum number of datagrams in one transmission batch.
    fn max_transmit_segments(&self) -> usize {
        1
    }

    /// Maximum datagrams that one receive buffer may contain.
    fn max_receive_segments(&self) -> usize {
        1
    }

    /// Whether the carrier may fragment datagrams below QUIC.
    fn may_fragment(&self) -> bool {
        true
    }

    /// Receives one complete datagram and its source address.
    fn recv_from<'a>(&'a self, buffer: &'a mut [u8]) -> RecvFrom<'a, Self>
    where
        Self: Sized,
    {
        RecvFrom {
            socket: self,
            buffer,
        }
    }
}

/// A future that receives one complete datagram and its source address.
pub struct RecvFrom<'a, T: DatagramSocket + ?Sized> {
    socket: &'a T,
    buffer: &'a mut [u8],
}

impl<T: DatagramSocket + ?Sized> Future for RecvFrom<'_, T> {
    type Output = io::Result<(usize, DatagramAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut buffer = ReadBuf::new(this.buffer);
        match this.socket.poll_recv_from(cx, &mut buffer) {
            Poll::Ready(Ok(source)) => Poll::Ready(Ok((buffer.filled().len(), source))),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}
