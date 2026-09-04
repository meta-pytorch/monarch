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
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use chrysalis_transport::DatagramAddr;
use chrysalis_transport::DatagramRecvMeta;
use chrysalis_transport::DatagramSocket;
use chrysalis_transport::DatagramTransmit;
use tokio::io::ReadBuf;

#[derive(Clone)]
pub(crate) struct DynSocket {
    inner: Arc<dyn DatagramSocket>,
}

impl DynSocket {
    pub(crate) fn new(inner: Arc<dyn DatagramSocket>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for DynSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynSocket")
            .field("inner", &self.inner)
            .finish()
    }
}

impl DatagramSocket for DynSocket {
    fn shutdown(&self) {
        self.inner.shutdown();
    }

    fn join(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.join()
    }

    fn local_addr(&self) -> &DatagramAddr {
        self.inner.local_addr()
    }

    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        self.inner.try_send_to(datagram, destination)
    }

    fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
        self.inner.try_send_transmit(transmit)
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
        self.inner.poll_recv_from(cx, buffer)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        meta: &mut [DatagramRecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, buffers, meta)
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
