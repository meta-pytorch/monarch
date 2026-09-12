/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::OsStr;
use std::io;
use std::io::IoSliceMut;
use std::mem;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::io::AsRawFd as _;
use std::path::Path;
use std::path::PathBuf;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use tokio::io::Interest;
use tokio::io::ReadBuf;
use tokio::net::UnixDatagram;

use crate::DatagramAddr;
use crate::DatagramRecvMeta;
use crate::DatagramSocket;
use crate::DatagramTransmit;
use crate::datagram::DATAGRAM_BATCH_SIZE;
use crate::shutdown::ShutdownState;

/// A filesystem-named Unix datagram binding.
#[derive(Debug)]
pub struct UnixDatagramSocket {
    socket: UnixDatagram,
    path: PathBuf,
    datagram_addr: DatagramAddr,
    shutdown_state: ShutdownState,
}

impl UnixDatagramSocket {
    /// Binds a Unix datagram socket at `path`.
    ///
    /// The caller owns removal of the socket path after this binding is dropped.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_owned();
        let socket = UnixDatagram::bind(&path)?;
        Ok(Self {
            socket,
            datagram_addr: unix_addr(&path),
            path,
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

    /// Returns the bound filesystem path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Encodes a filesystem path for use with [`DatagramSocket::try_send_to`].
    pub fn datagram_addr(path: impl AsRef<Path>) -> DatagramAddr {
        unix_addr(path.as_ref())
    }
}

impl DatagramSocket for UnixDatagramSocket {
    fn shutdown(&self) {
        UnixDatagramSocket::shutdown(self);
    }

    fn join(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(UnixDatagramSocket::join(self))
    }

    fn local_addr(&self) -> &DatagramAddr {
        &self.datagram_addr
    }

    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        if !self.shutdown_state.is_running() {
            return Err(shutdown_error("Unix datagram transport is shut down"));
        }
        let destination = decode_unix_addr(destination)?;
        match self.socket.try_send_to(datagram, destination) {
            Ok(sent) if sent == datagram.len() => Ok(()),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "partial Unix datagram send",
            )),
            Err(error) => Err(error),
        }
    }

    fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
        if !self.shutdown_state.is_running() {
            return Err(shutdown_error("Unix datagram transport is shut down"));
        }
        let destination = decode_unix_addr(transmit.destination)?;
        let segment_size = transmit.segment_size.unwrap_or(transmit.contents.len());
        if segment_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Unix datagram segment size must be nonzero",
            ));
        }
        match self.socket.try_io(Interest::WRITABLE, || {
            send_mmsg(&self.socket, transmit.contents, segment_size, destination)
        }) {
            Ok(sent) => Ok(sent),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(error) => Err(error),
        }
    }

    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        _transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("Unix datagram transport is shut down")));
        }
        self.shutdown_state.register_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("Unix datagram transport is shut down")));
        }
        self.socket.poll_send_ready(cx)
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("Unix datagram transport is shut down")));
        }
        self.shutdown_state.register_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("Unix datagram transport is shut down")));
        }
        match self.socket.poll_recv_from(cx, buffer) {
            Poll::Ready(Ok(source)) => {
                let Some(path) = source.as_pathname() else {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::AddrNotAvailable,
                        "Unix datagram source has no filesystem path",
                    )));
                };
                Poll::Ready(Ok(unix_addr(path)))
            }
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
            return Poll::Ready(Err(shutdown_error("Unix datagram transport is shut down")));
        }
        self.shutdown_state.register_waker(cx.waker());
        let capacity = buffers.len().min(meta.len()).min(DATAGRAM_BATCH_SIZE);
        if capacity == 0 {
            return Poll::Ready(Ok(0));
        }
        loop {
            ready!(self.socket.poll_recv_ready(cx))?;
            match self.socket.try_io(Interest::READABLE, || {
                recv_mmsg(&self.socket, &mut buffers[..capacity])
            }) {
                Ok(received) => {
                    let count = received.len();
                    for (output, (len, source)) in meta[..count].iter_mut().zip(received) {
                        *output = DatagramRecvMeta {
                            source: unix_addr(&source),
                            len,
                            stride: len,
                            ecn: None,
                            destination_ip: None,
                        };
                    }
                    return Poll::Ready(Ok(count));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    fn max_transmit_segments(&self) -> usize {
        DATAGRAM_BATCH_SIZE
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

fn unix_addr(path: &Path) -> DatagramAddr {
    DatagramAddr::new("unixgram", path.as_os_str().as_bytes().to_vec())
}

fn decode_unix_addr(address: &DatagramAddr) -> io::Result<&Path> {
    if address.scheme() != "unixgram" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Unix datagram address",
        ));
    }
    Ok(Path::new(OsStr::from_bytes(address.opaque())))
}

fn send_mmsg(
    socket: &UnixDatagram,
    contents: &[u8],
    segment_size: usize,
    destination: &Path,
) -> io::Result<usize> {
    let (address, address_len) = encode_sockaddr(destination)?;
    // SAFETY: Zero is a valid empty iovec; every active entry is initialized below.
    let mut iovecs: [libc::iovec; DATAGRAM_BATCH_SIZE] = unsafe { mem::zeroed() };
    // SAFETY: Zero is a valid empty mmsghdr; every active entry is initialized below.
    let mut messages: [libc::mmsghdr; DATAGRAM_BATCH_SIZE] = unsafe { mem::zeroed() };
    let mut count = 0;
    for (index, datagram) in contents
        .chunks(segment_size)
        .take(DATAGRAM_BATCH_SIZE)
        .enumerate()
    {
        iovecs[index].iov_base = datagram.as_ptr().cast_mut().cast();
        iovecs[index].iov_len = datagram.len();
        messages[index].msg_hdr.msg_name = (&address as *const libc::sockaddr_un).cast_mut().cast();
        messages[index].msg_hdr.msg_namelen = address_len;
        messages[index].msg_hdr.msg_iov = &mut iovecs[index];
        messages[index].msg_hdr.msg_iovlen = 1;
        count += 1;
    }
    loop {
        // SAFETY: Every active message references live address, iovec, and payload storage.
        let sent = unsafe {
            libc::sendmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                count as libc::c_uint,
                libc::MSG_DONTWAIT,
            )
        };
        if sent >= 0 {
            return Ok(sent as usize);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn encode_sockaddr(path: &Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    let bytes = path.as_os_str().as_bytes();
    // SAFETY: Zero initializes the unused path suffix and trailing terminator.
    let mut address: libc::sockaddr_un = unsafe { mem::zeroed() };
    if bytes.contains(&0) || bytes.len() >= address.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Unix datagram path",
        ));
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    // SAFETY: The length check bounds the copy within sun_path, and both buffers are live.
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr(),
            address.sun_path.as_mut_ptr().cast(),
            bytes.len(),
        );
    }
    let len = mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
    Ok((address, len as libc::socklen_t))
}

fn recv_mmsg(
    socket: &UnixDatagram,
    buffers: &mut [IoSliceMut<'_>],
) -> io::Result<Vec<(usize, PathBuf)>> {
    let count = buffers.len().min(DATAGRAM_BATCH_SIZE);
    // SAFETY: Zero is a valid inert representation for sockaddr_un before recvmmsg fills it.
    let mut addresses: [libc::sockaddr_un; DATAGRAM_BATCH_SIZE] = unsafe { mem::zeroed() };
    // SAFETY: Zero is a valid empty mmsghdr; every field consumed below is initialized before use.
    let mut messages: [libc::mmsghdr; DATAGRAM_BATCH_SIZE] = unsafe { mem::zeroed() };
    for index in 0..count {
        messages[index].msg_hdr.msg_name = (&mut addresses[index] as *mut libc::sockaddr_un).cast();
        messages[index].msg_hdr.msg_namelen = mem::size_of::<libc::sockaddr_un>() as _;
        messages[index].msg_hdr.msg_iov = (&mut buffers[index] as *mut IoSliceMut<'_>).cast();
        messages[index].msg_hdr.msg_iovlen = 1;
    }
    let received = loop {
        // SAFETY: Every active message references live address and buffer storage for this call.
        let received = unsafe {
            libc::recvmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                count as libc::c_uint,
                libc::MSG_DONTWAIT,
                std::ptr::null_mut(),
            )
        };
        if received >= 0 {
            break received as usize;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    (0..received)
        .map(|index| {
            Ok((
                messages[index].msg_len as usize,
                decode_sockaddr(&addresses[index], messages[index].msg_hdr.msg_namelen)?,
            ))
        })
        .collect()
}

fn decode_sockaddr(address: &libc::sockaddr_un, len: libc::socklen_t) -> io::Result<PathBuf> {
    if address.sun_family != libc::AF_UNIX as libc::sa_family_t {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received non-Unix datagram address",
        ));
    }
    let offset = mem::offset_of!(libc::sockaddr_un, sun_path);
    let len = usize::try_from(len)
        .unwrap_or_default()
        .saturating_sub(offset)
        .min(address.sun_path.len());
    // SAFETY: `len` is bounded by sun_path, whose bytes remain live for the returned slice use.
    let bytes = unsafe { std::slice::from_raw_parts(address.sun_path.as_ptr().cast(), len) };
    let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    Ok(Path::new(OsStr::from_bytes(bytes)).to_owned())
}

fn shutdown_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, message)
}

impl Drop for UnixDatagramSocket {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    fn socket_path(label: &str) -> PathBuf {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "chrysalis-{label}-{}-{id}.sock",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn round_trip_preserves_boundaries_and_source_addresses() {
        let first_path = socket_path("first");
        let second_path = socket_path("second");
        let first = UnixDatagramSocket::bind(&first_path).expect("bind first Unix socket");
        let second = UnixDatagramSocket::bind(&second_path).expect("bind second Unix socket");
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
            .expect("wait for Unix request send readiness");
        first
            .try_send_transmit(&request)
            .expect("send Unix request");
        let (request_len, request_source) = second
            .recv_from(&mut buffer)
            .await
            .expect("receive Unix request");
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
            .expect("wait for Unix response send readiness");
        second
            .try_send_transmit(&response)
            .expect("send Unix response");
        let (response_len, response_source) = first
            .recv_from(&mut buffer)
            .await
            .expect("receive Unix response");
        assert_eq!(&buffer[..response_len], b"response");
        assert_eq!(&response_source, second.local_addr());

        drop(first);
        drop(second);
        tokio::fs::remove_file(first_path)
            .await
            .expect("remove first Unix socket");
        tokio::fs::remove_file(second_path)
            .await
            .expect("remove second Unix socket");
    }

    #[tokio::test]
    async fn segmented_send_preserves_datagram_boundaries() {
        let first_path = socket_path("segmented-first");
        let second_path = socket_path("segmented-second");
        let first = UnixDatagramSocket::bind(&first_path).expect("bind first Unix socket");
        let second = UnixDatagramSocket::bind(&second_path).expect("bind second Unix socket");
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
        assert_eq!(first.max_transmit_segments(), DATAGRAM_BATCH_SIZE);
        let mut buffer = vec![0; segment_size];
        let mut sent = 0;
        while sent < transmit.segment_count() {
            let remaining = DatagramTransmit {
                contents: &payload[sent * segment_size..],
                ..transmit
            };
            poll_fn(|cx| first.poll_send_ready(cx, &remaining))
                .await
                .expect("wait for segmented Unix send readiness");
            let accepted = first
                .try_send_transmit(&remaining)
                .expect("send segmented Unix payload prefix");
            for expected in sent..sent + accepted {
                let (len, source) = second
                    .recv_from(&mut buffer)
                    .await
                    .expect("receive Unix datagram segment");
                assert_eq!(source, *first.local_addr());
                assert_eq!(len, segment_size);
                assert_eq!(&buffer[..len], vec![expected as u8; segment_size]);
            }
            sent += accepted;
        }

        drop(first);
        drop(second);
        tokio::fs::remove_file(first_path)
            .await
            .expect("remove first Unix socket");
        tokio::fs::remove_file(second_path)
            .await
            .expect("remove second Unix socket");
    }

    #[tokio::test]
    async fn shutdown_wakes_receive_rejects_sends_and_joins() {
        let path = socket_path("shutdown");
        let transport =
            Arc::new(UnixDatagramSocket::bind(&path).expect("bind Unix datagram socket"));
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

        tokio::fs::remove_file(path)
            .await
            .expect("remove Unix socket");
    }
}
