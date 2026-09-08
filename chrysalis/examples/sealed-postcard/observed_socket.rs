// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

use std::future::Future;
use std::io;
use std::io::IoSliceMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use chrysalis::DatagramAddr;
use chrysalis::DatagramSocket;
use chrysalis::Pid;
use chrysalis::target_pid;
use chrysalis::transport::DatagramRecvMeta;
use chrysalis::transport::DatagramTransmit;
use tokio::io::ReadBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatagramDirection {
    Ingress,
    Egress,
}

#[derive(Clone, Debug)]
pub(crate) struct ObservedDatagram {
    pub(crate) direction: DatagramDirection,
    pub(crate) target: Option<Pid>,
    pub(crate) peer: DatagramAddr,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct ObservedSocket<T> {
    inner: Arc<T>,
    observations: Mutex<Vec<ObservedDatagram>>,
}

impl<T> ObservedSocket<T> {
    pub(crate) fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            observations: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn observations(&self) -> Vec<ObservedDatagram> {
        self.observations
            .lock()
            .expect("observation lock should not be poisoned")
            .clone()
    }

    fn record(&self, direction: DatagramDirection, peer: &DatagramAddr, bytes: &[u8]) {
        self.observations
            .lock()
            .expect("observation lock should not be poisoned")
            .push(ObservedDatagram {
                direction,
                target: target_pid(bytes),
                peer: peer.clone(),
                bytes: bytes.to_vec(),
            });
    }
}

impl<T: DatagramSocket> DatagramSocket for ObservedSocket<T> {
    fn shutdown(&self) {
        self.inner.shutdown();
    }

    fn join(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.join()
    }

    fn local_addr(&self) -> &DatagramAddr {
        self.inner.local_addr()
    }

    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        self.inner.try_send_to(datagram, destination)?;
        self.record(DatagramDirection::Egress, destination, datagram);
        Ok(())
    }

    fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
        let accepted = self.inner.try_send_transmit(transmit)?;
        if accepted > 0 {
            let segment_size = transmit.segment_size.unwrap_or(transmit.contents.len());
            for datagram in transmit.contents.chunks(segment_size).take(accepted) {
                self.record(DatagramDirection::Egress, transmit.destination, datagram);
            }
        }
        Ok(accepted)
    }

    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        self.inner.poll_send_ready(cx, transmit)
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>> {
        let filled_before = buffer.filled().len();
        match self.inner.poll_recv_from(cx, buffer) {
            Poll::Ready(Ok(source)) => {
                self.record(
                    DatagramDirection::Ingress,
                    &source,
                    &buffer.filled()[filled_before..],
                );
                Poll::Ready(Ok(source))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        meta: &mut [DatagramRecvMeta],
    ) -> Poll<io::Result<usize>> {
        match self.inner.poll_recv(cx, buffers, meta) {
            Poll::Ready(Ok(count)) => {
                for (buffer, received) in buffers.iter().zip(meta.iter()).take(count) {
                    self.record(
                        DatagramDirection::Ingress,
                        &received.source,
                        &buffer[..received.len],
                    );
                }
                Poll::Ready(Ok(count))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}
