/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::io::IoSliceMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use tokio::io::ReadBuf;

use crate::DatagramAddr;
use crate::DatagramRecvMeta;
use crate::DatagramSocket;
use crate::DatagramTransmit;
use crate::shutdown::ShutdownState;

/// A datagram socket that selects one of several carrier sockets by address scheme.
///
/// Each scheme may appear once. Receives are fanned in from every socket, while sends are
/// delegated to the socket whose local address has the destination's scheme. The primary
/// socket supplies [`DatagramSocket::local_addr`].
#[derive(Debug)]
pub struct DatagramSocketSet {
    sockets: Vec<Arc<dyn DatagramSocket>>,
    by_scheme: HashMap<Arc<str>, usize>,
    primary_addr: DatagramAddr,
    next_recv: AtomicUsize,
    shutdown_state: ShutdownState,
}

impl DatagramSocketSet {
    /// Constructs a set from one primary socket and zero or more alternative carriers.
    pub fn new(
        primary: Arc<dyn DatagramSocket>,
        alternatives: Vec<Arc<dyn DatagramSocket>>,
    ) -> io::Result<Self> {
        let primary_addr = primary.local_addr().clone();
        let mut sockets = Vec::with_capacity(alternatives.len() + 1);
        sockets.push(primary);
        sockets.extend(alternatives);
        let mut by_scheme = HashMap::with_capacity(sockets.len());
        for (index, socket) in sockets.iter().enumerate() {
            let scheme: Arc<str> = socket.local_addr().scheme().into();
            if by_scheme.insert(scheme.clone(), index).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate datagram carrier scheme: {scheme}"),
                ));
            }
        }
        Ok(Self {
            sockets,
            by_scheme,
            primary_addr,
            next_recv: AtomicUsize::new(0),
            shutdown_state: ShutdownState::default(),
        })
    }

    /// Returns every underlying carrier socket in selection order.
    pub fn sockets(&self) -> &[Arc<dyn DatagramSocket>] {
        &self.sockets
    }

    /// Idempotently requests shutdown of every carrier socket.
    pub fn shutdown(&self) {
        if self.shutdown_state.shutdown() {
            for socket in &self.sockets {
                socket.shutdown();
            }
            self.shutdown_state.terminate();
        }
    }

    /// Waits for every carrier socket to terminate.
    pub async fn join(&self) {
        for socket in &self.sockets {
            socket.join().await;
        }
        self.shutdown_state.join().await;
    }

    fn socket_for(&self, destination: &DatagramAddr) -> io::Result<&dyn DatagramSocket> {
        self.by_scheme
            .get(destination.scheme())
            .map(|index| self.sockets[*index].as_ref())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "no datagram carrier for destination scheme: {}",
                        destination.scheme()
                    ),
                )
            })
    }
}

impl DatagramSocket for DatagramSocketSet {
    fn shutdown(&self) {
        DatagramSocketSet::shutdown(self);
    }

    fn join(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(DatagramSocketSet::join(self))
    }

    fn local_addr(&self) -> &DatagramAddr {
        &self.primary_addr
    }

    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        if !self.shutdown_state.is_running() {
            return Err(shutdown_error());
        }
        self.socket_for(destination)?
            .try_send_to(datagram, destination)
    }

    fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
        if !self.shutdown_state.is_running() {
            return Err(shutdown_error());
        }
        let socket = self.socket_for(transmit.destination)?;
        socket.try_send_transmit(transmit)
    }

    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error()));
        }
        self.shutdown_state.register_send_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error()));
        }
        match self.socket_for(transmit.destination) {
            Ok(socket) => socket.poll_send_ready(cx, transmit),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error()));
        }
        self.shutdown_state.register_receive_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error()));
        }
        let start = self.next_recv.load(Ordering::Relaxed) % self.sockets.len();
        for offset in 0..self.sockets.len() {
            let index = (start + offset) % self.sockets.len();
            match self.sockets[index].poll_recv_from(cx, buffer) {
                Poll::Ready(result) => {
                    self.next_recv
                        .store((index + 1) % self.sockets.len(), Ordering::Relaxed);
                    return Poll::Ready(result);
                }
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        meta: &mut [DatagramRecvMeta],
    ) -> Poll<io::Result<usize>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error()));
        }
        self.shutdown_state.register_receive_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error()));
        }
        let start = self.next_recv.load(Ordering::Relaxed) % self.sockets.len();
        for offset in 0..self.sockets.len() {
            let index = (start + offset) % self.sockets.len();
            match self.sockets[index].poll_recv(cx, buffers, meta) {
                Poll::Ready(result) => {
                    self.next_recv
                        .store((index + 1) % self.sockets.len(), Ordering::Relaxed);
                    return Poll::Ready(result);
                }
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }

    fn max_transmit_segments(&self) -> usize {
        self.sockets
            .iter()
            .map(|socket| socket.max_transmit_segments())
            .min()
            .unwrap_or(1)
    }

    fn max_receive_segments(&self) -> usize {
        self.sockets
            .iter()
            .map(|socket| socket.max_receive_segments())
            .max()
            .unwrap_or(1)
    }

    fn may_fragment(&self) -> bool {
        self.sockets.iter().any(|socket| socket.may_fragment())
    }
}

impl Drop for DatagramSocketSet {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn shutdown_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "datagram socket set is shut down",
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct TestSocket {
        local_addr: DatagramAddr,
        send_limit: Option<usize>,
        max_segments: usize,
        sent: Mutex<Vec<(Vec<u8>, DatagramAddr)>>,
        received: Mutex<VecDeque<(Vec<u8>, DatagramAddr)>>,
    }

    impl TestSocket {
        fn new(scheme: &str) -> Self {
            Self {
                local_addr: DatagramAddr::new(scheme, []),
                send_limit: None,
                max_segments: 1,
                sent: Mutex::new(Vec::new()),
                received: Mutex::new(VecDeque::new()),
            }
        }

        fn with_send_limit(scheme: &str, send_limit: usize) -> Self {
            Self {
                send_limit: Some(send_limit),
                ..Self::new(scheme)
            }
        }

        fn with_max_segments(scheme: &str, max_segments: usize) -> Self {
            Self {
                max_segments,
                ..Self::new(scheme)
            }
        }
    }

    impl DatagramSocket for TestSocket {
        fn shutdown(&self) {}

        fn join(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn local_addr(&self) -> &DatagramAddr {
            &self.local_addr
        }

        fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
            let mut sent = self.sent.lock().expect("sent lock poisoned");
            if self
                .send_limit
                .is_some_and(|send_limit| sent.len() >= send_limit)
            {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            sent.push((datagram.to_vec(), destination.clone()));
            Ok(())
        }

        fn poll_send_ready(
            &self,
            _cx: &mut Context<'_>,
            _transmit: &DatagramTransmit<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_recv_from(
            &self,
            _cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<DatagramAddr>> {
            let Some((datagram, source)) = self
                .received
                .lock()
                .expect("received lock poisoned")
                .pop_front()
            else {
                return Poll::Pending;
            };
            buffer.put_slice(&datagram);
            Poll::Ready(Ok(source))
        }

        fn max_transmit_segments(&self) -> usize {
            self.max_segments
        }
    }

    #[test]
    fn selects_send_socket_by_destination_scheme() {
        let udp = Arc::new(TestSocket::new("udp"));
        let unix = Arc::new(TestSocket::new("unixgram"));
        let set = DatagramSocketSet::new(udp.clone(), vec![unix.clone()]).unwrap();
        let destination = DatagramAddr::new("unixgram", b"path".as_slice());

        set.try_send_to(b"hello", &destination).unwrap();

        assert!(udp.sent.lock().unwrap().is_empty());
        assert_eq!(
            *unix.sent.lock().unwrap(),
            vec![(b"hello".to_vec(), destination)]
        );
    }

    #[test]
    fn segmented_send_reports_accepted_prefix() {
        let udp = Arc::new(TestSocket::new("udp"));
        let unix = Arc::new(TestSocket::with_send_limit("unixgram", 2));
        let set =
            DatagramSocketSet::new(udp, vec![unix.clone()]).expect("construct datagram socket set");
        let destination = DatagramAddr::new("unixgram", b"path".as_slice());
        let transmit = DatagramTransmit {
            destination: &destination,
            contents: b"aaaabbbbcccc",
            segment_size: Some(4),
            ecn: None,
            source_ip: None,
        };

        let sent = set
            .try_send_transmit(&transmit)
            .expect("send available datagram prefix");

        assert_eq!(sent, 2);
        assert_eq!(
            *unix.sent.lock().expect("sent lock poisoned"),
            vec![
                (b"aaaa".to_vec(), destination.clone()),
                (b"bbbb".to_vec(), destination),
            ]
        );
    }

    #[test]
    fn transmit_batch_limit_is_safe_for_every_socket() {
        let primary = Arc::new(TestSocket::with_max_segments("udp", 32));
        let fallback = Arc::new(TestSocket::with_max_segments("unixgram", 4));
        let set =
            DatagramSocketSet::new(primary, vec![fallback]).expect("construct datagram socket set");

        assert_eq!(set.max_transmit_segments(), 4);
    }

    #[tokio::test]
    async fn receives_from_every_carrier() {
        let udp = Arc::new(TestSocket::new("udp"));
        let unix = Arc::new(TestSocket::new("unixgram"));
        let source = DatagramAddr::new("unixgram", b"peer".as_slice());
        unix.received
            .lock()
            .unwrap()
            .push_back((b"hello".to_vec(), source.clone()));
        let set = DatagramSocketSet::new(udp, vec![unix]).unwrap();
        let mut buffer = [0; 16];

        let (length, actual_source) = set.recv_from(&mut buffer).await.unwrap();

        assert_eq!(&buffer[..length], b"hello");
        assert_eq!(actual_source, source);
    }

    #[test]
    fn rejects_duplicate_schemes() {
        let first = Arc::new(TestSocket::new("udp"));
        let second = Arc::new(TestSocket::new("udp"));

        assert!(DatagramSocketSet::new(first, vec![second]).is_err());
    }
}
