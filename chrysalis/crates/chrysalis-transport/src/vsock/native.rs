/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::mem;
use std::os::fd::AsRawFd as _;
use std::os::fd::FromRawFd as _;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::io::unix::AsyncFd;

/// The reserved AF_VSOCK context ID for a guest's host.
///
/// A guest connects to `native:2:<port>` to reach a service on its host.
pub const VMADDR_CID_HOST: u32 = 2;

/// The AF_VSOCK wildcard context ID used when binding a listener.
///
/// It lets a guest listen without knowing the CID assigned to it by the VMM.
/// It is a bind address, not a peer to dial.
pub const VMADDR_CID_ANY: u32 = u32::MAX;

/// Allocates the native AF_VSOCK descriptor used by [`native_listen`] and
/// [`native_connect`].
///
/// The descriptor starts unbound and unconnected. `native_listen` binds it and
/// turns it into a listener; `native_connect` connects it to a peer. It is made
/// nonblocking so Tokio can drive it through `AsyncFd`, and close-on-exec so a
/// child process cannot inherit it. `OwnedFd` closes it on every return path.
fn vsock_socket() -> io::Result<OwnedFd> {
    // SAFETY: `socket` reads no user-provided pointers. The arguments are
    // integer constants, and `check` converts any OS rejection into an error.
    let raw = check(unsafe {
        libc::socket(
            libc::AF_VSOCK,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        )
    })?;
    // SAFETY: `check` proved that `raw` is nonnegative. A successful `socket`
    // call returns a new descriptor owned by the caller, and no other
    // `OwnedFd` has been constructed from it.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Builds the `sockaddr_vm` argument shared by [`native_listen`] and
/// [`native_connect`].
///
/// The libc `bind` and `connect` calls both require a pointer to a
/// `sockaddr_vm` plus its byte length. This helper keeps that representation in
/// one place: it zeroes the reserved fields and padding, sets the AF_VSOCK
/// family, CID, and port, and returns the matching structure size.
fn vsock_sockaddr(cid: u32, port: u32) -> (libc::sockaddr_vm, libc::socklen_t) {
    // SAFETY: `sockaddr_vm` contains only integer fields and byte padding, for
    // which all-zero is valid. The address fields are populated below before
    // the value is passed to libc.
    let mut address: libc::sockaddr_vm = unsafe { mem::zeroed() };
    address.svm_family = libc::AF_VSOCK as libc::sa_family_t;
    address.svm_cid = cid;
    address.svm_port = port;
    (
        address,
        mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
    )
}

/// Creates the native AF_VSOCK listener requested by
/// [`VsockDatagramSocket::bind`].
///
/// On the Relay → Alice link, this runs inside Alice's guest. Alice binds
/// `VMADDR_CID_ANY:5000`, starts listening, and gives the returned `AsyncFd` to
/// `accept_native`, which accepts Relay's Firecracker-bridged connection.
///
/// This turns the unconnected descriptor from [`vsock_socket`] into a listener:
/// `bind` assigns its local CID and port, `listen` enables connection acceptance,
/// and `AsyncFd` registers the listener with Tokio.
///
/// [`VsockDatagramSocket::bind`]: super::VsockDatagramSocket::bind
pub(super) fn native_listen(cid: u32, port: u32) -> io::Result<AsyncFd<OwnedFd>> {
    let socket = vsock_socket()?;
    let (address, address_len) = vsock_sockaddr(cid, port);
    // SAFETY: `socket` is a live owned descriptor. `address` is aligned, remains
    // live for the call, and `address_len` is its exact byte size.
    check(unsafe {
        libc::bind(
            socket.as_raw_fd(),
            (&address as *const libc::sockaddr_vm).cast(),
            address_len,
        )
    })?;
    // SAFETY: `socket` remains a live descriptor; `listen` reads no pointers.
    check(unsafe { libc::listen(socket.as_raw_fd(), libc::SOMAXCONN) })?;
    // SAFETY: `register` requires a descriptor that stays open and unchanged
    // for the `AsyncFd`'s lifetime. The `AsyncFd` takes ownership of `socket`,
    // an `OwnedFd` that closes its descriptor only when dropped and whose
    // `as_raw_fd` always returns that descriptor.
    Ok(unsafe { AsyncFd::register(socket) }?)
}

/// Accepts one pending native-vsock connection and returns this process's local
/// stream endpoint.
///
/// On the Relay → Alice link, `accept_native` calls this after Alice's listener
/// becomes readable. `accept4` creates Alice's connected endpoint; the listener
/// remains open for later connections. The kernel peer address is intentionally
/// ignored because the carrier learns the peer's `node:<name>` label from the
/// connection's HELLO frame.
///
/// The accepted descriptor is nonblocking and close-on-exec from creation, then
/// wrapped in `AsyncFd` so Tokio can drive its reads and writes.
pub(super) fn native_accept(listener: &OwnedFd) -> io::Result<NativeStreamEndpoint> {
    // SAFETY: `listener` is a live listening descriptor. Passing null address
    // pointers explicitly asks `accept4` not to return the kernel peer address;
    // no memory is read through them.
    let raw = check(unsafe {
        libc::accept4(
            listener.as_raw_fd(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
        )
    })?;
    // SAFETY: `check` proved that `raw` is nonnegative. A successful `accept4`
    // call returns a new descriptor owned by the caller, and no other
    // `OwnedFd` has been constructed from it.
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    // SAFETY: `register` requires a descriptor that stays open and unchanged
    // for the `AsyncFd`'s lifetime. The `AsyncFd` takes ownership of `socket`,
    // an `OwnedFd` that closes its descriptor only when dropped and whose
    // `as_raw_fd` always returns that descriptor.
    Ok(NativeStreamEndpoint(unsafe { AsyncFd::register(socket) }?))
}

/// Opens a native-vsock connection and returns this process's local stream
/// endpoint.
///
/// On the Bob → Relay link, this runs inside Bob's guest. Bob connects to host
/// CID 2, port 5001; Firecracker bridges that request to Relay's Unix listener
/// at `<uds_B>_5001`. The returned `NativeStreamEndpoint` is Bob's local end of
/// that connection.
///
/// The socket is nonblocking, so `connect` either succeeds immediately or
/// reports `EINPROGRESS`. In the latter case Tokio waits for writability, then
/// `SO_ERROR` reports whether connection setup succeeded.
pub(super) async fn native_connect(cid: u32, port: u32) -> io::Result<NativeStreamEndpoint> {
    let socket = vsock_socket()?;
    let (address, address_len) = vsock_sockaddr(cid, port);
    // SAFETY: `socket` is a live owned descriptor. `address` is aligned, remains
    // live for the call, and `address_len` is its exact byte size.
    let connected = check(unsafe {
        libc::connect(
            socket.as_raw_fd(),
            (&address as *const libc::sockaddr_vm).cast(),
            address_len,
        )
    });
    match connected {
        Ok(_) => {}
        Err(error) if error.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(error) => return Err(error),
    }

    // SAFETY: `register` requires a descriptor that stays open and unchanged
    // for the `AsyncFd`'s lifetime. The `AsyncFd` takes ownership of `socket`,
    // an `OwnedFd` that closes its descriptor only when dropped and whose
    // `as_raw_fd` always returns that descriptor.
    let socket = unsafe { AsyncFd::register(socket) }?;
    let _writable = socket.writable().await?;
    let mut error: libc::c_int = 0;
    let mut error_len = mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `socket` is live. `error` is aligned and writable as a `c_int`;
    // `error_len` starts with its exact size, and both remain live for the call.
    check(unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            (&mut error as *mut libc::c_int).cast(),
            &mut error_len,
        )
    })?;
    if error != 0 {
        return Err(io::Error::from_raw_os_error(error));
    }
    Ok(NativeStreamEndpoint(socket))
}

/// Returns the last OS error when a libc call reports failure.
fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(result)
}

/// Converts a libc `read` or `write` result into a Rust byte count.
///
/// These calls return a nonnegative byte count on success and `-1` on failure,
/// with details in `errno`. After rejecting negative values, converting
/// `ssize_t` to `usize` is lossless.
fn check_len(result: libc::ssize_t) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(result as usize)
}

/// This process's local endpoint of one native AF_VSOCK stream connection.
///
/// In the Alice–Relay 1–Relay 2–Bob example in the module documentation, Alice
/// receives a `NativeStreamEndpoint` when she accepts Relay 1's host-initiated
/// connection, and Bob receives one when he connects to Relay 2 on host CID 2.
///
/// The peer process owns the connection's other endpoint. This wrapper owns the
/// local nonblocking descriptor, registers it with Tokio through `AsyncFd`, and
/// implements `AsyncRead` and `AsyncWrite` for the connection tasks. Dropping it
/// closes this process's endpoint.
#[derive(Debug)]
pub(super) struct NativeStreamEndpoint(
    /// Owned native-vsock descriptor registered with Tokio's reactor.
    AsyncFd<OwnedFd>,
);

impl AsyncRead for NativeStreamEndpoint {
    /// Reads bytes from this native-vsock endpoint without blocking the
    /// runtime.
    ///
    /// Tokio first waits until the descriptor may be readable. `try_io`
    /// performs one libc read. A successful read advances `buf`; an OS error is
    /// returned; and a stale readiness notification (`WouldBlock`) is cleared
    /// before retrying.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            // Returns Pending and registers this task's waker until Tokio
            // observes that the descriptor may be readable.
            let mut ready = ready!(self.0.poll_read_ready(cx))?;

            // libc needs initialized writable memory; ReadBuf tracks how much
            // of it becomes filled.
            let unfilled = buf.initialize_unfilled();
            let read = |fd: &AsyncFd<OwnedFd>| {
                // SAFETY: `fd` is live. `unfilled` is writable, byte-aligned,
                // and remains live for the call; its pointer is valid for
                // exactly `unfilled.len()` bytes.
                let len = unsafe {
                    libc::read(fd.as_raw_fd(), unfilled.as_mut_ptr().cast(), unfilled.len())
                };
                check_len(len)
            };
            match ready.try_io(read) {
                Ok(Ok(len)) => {
                    // libc initialized exactly `len` bytes.
                    buf.advance(len);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(error)) => return Poll::Ready(Err(error)),
                // The descriptor was not actually readable. `try_io` clears
                // the stale readiness state, so loop back and wait again.
                Err(_would_block) => {}
            }
        }
    }
}

impl AsyncWrite for NativeStreamEndpoint {
    /// Writes bytes to this native-vsock endpoint without blocking the runtime.
    ///
    /// Tokio first waits until the descriptor may be writable. A successful
    /// libc write may consume any prefix of `buf` and returns that byte count.
    /// An OS error is returned; stale readiness (`WouldBlock`) is cleared before
    /// retrying.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            // Returns Pending and registers this task's waker until Tokio
            // observes that the descriptor may be writable.
            let mut ready = ready!(self.0.poll_write_ready(cx))?;
            let write = |fd: &AsyncFd<OwnedFd>| {
                // SAFETY: `fd` is live. `buf` is readable and remains live for
                // the call; its pointer is valid for exactly `buf.len()` bytes.
                let len = unsafe { libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };
                check_len(len)
            };
            match ready.try_io(write) {
                Ok(result) => return Poll::Ready(result),
                // The descriptor was not actually writable. `try_io` clears
                // the stale readiness state, so loop back and wait again.
                Err(_would_block) => {}
            }
        }
    }

    /// Completes immediately because this adapter has no user-space write
    /// buffer.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Closes this endpoint's writing direction while leaving reads open.
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // SAFETY: the descriptor is live, `SHUT_WR` is a valid shutdown mode,
        // and `shutdown` reads no pointers.
        Poll::Ready(check(unsafe { libc::shutdown(self.0.as_raw_fd(), libc::SHUT_WR) }).map(drop))
    }
}
