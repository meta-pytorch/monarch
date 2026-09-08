/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::io;
use std::io::IoSliceMut;
use std::mem;
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

const MAX_OBSERVATIONS: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatagramDirection {
    Ingress,
    Egress,
}

#[derive(Debug)]
pub(crate) struct ObservedDatagram {
    pub(crate) direction: DatagramDirection,
    pub(crate) target: Option<Pid>,
    pub(crate) peer: DatagramAddr,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Default)]
struct ObservationLog {
    datagrams: Vec<ObservedDatagram>,
    truncated: bool,
}

#[derive(Debug)]
pub(crate) struct ObservationSnapshot {
    pub(crate) datagrams: Vec<ObservedDatagram>,
    pub(crate) truncated: bool,
}

#[derive(Debug)]
pub(crate) struct ObservedSocket<T> {
    inner: Arc<T>,
    observations: Mutex<ObservationLog>,
}

impl<T> ObservedSocket<T> {
    pub(crate) fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            observations: Mutex::new(ObservationLog::default()),
        }
    }

    pub(crate) fn take_observations(&self) -> ObservationSnapshot {
        let mut observations = self
            .observations
            .lock()
            .expect("observation lock should not be poisoned");
        ObservationSnapshot {
            datagrams: mem::take(&mut observations.datagrams),
            truncated: mem::take(&mut observations.truncated),
        }
    }

    fn record(&self, direction: DatagramDirection, peer: &DatagramAddr, bytes: &[u8]) {
        let observation = ObservedDatagram {
            direction,
            target: target_pid(bytes),
            peer: peer.clone(),
            bytes: bytes.to_vec(),
        };
        let mut observations = self
            .observations
            .lock()
            .expect("observation lock should not be poisoned");
        if observations.datagrams.len() >= MAX_OBSERVATIONS {
            observations.truncated = true;
            return;
        }
        observations.datagrams.push(observation);
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
            if segment_size == 0 {
                self.record(
                    DatagramDirection::Egress,
                    transmit.destination,
                    transmit.contents,
                );
            } else {
                for datagram in transmit.contents.chunks(segment_size).take(accepted) {
                    self.record(DatagramDirection::Egress, transmit.destination, datagram);
                }
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
                    let received_bytes = &buffer[..received.len];
                    if received.len == 0 || received.stride == 0 {
                        self.record(DatagramDirection::Ingress, &received.source, received_bytes);
                    } else {
                        for datagram in received_bytes.chunks(received.stride) {
                            self.record(DatagramDirection::Ingress, &received.source, datagram);
                        }
                    }
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
