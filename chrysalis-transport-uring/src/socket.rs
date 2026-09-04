/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::c_void;
use std::io;
use std::mem;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::net::UdpSocket;
use std::os::fd::AsRawFd;
use std::ptr;

use crate::DriverConfig;

pub(crate) const UDP_SEGMENT: libc::c_int = 103;
pub(crate) const UDP_GRO: libc::c_int = 104;
pub(crate) const CONTROL_BUFFER_SIZE: usize = 64;

#[repr(C, align(16))]
pub(crate) struct ControlBuffer(pub(crate) [u8; CONTROL_BUFFER_SIZE]);

#[derive(Clone, Copy)]
pub(crate) struct SocketAddress {
    storage: libc::sockaddr_storage,
    pub(crate) length: libc::socklen_t,
}

impl Default for SocketAddress {
    fn default() -> Self {
        // SAFETY: An all-zero sockaddr_storage is valid inert storage.
        let storage = unsafe { mem::zeroed() };
        Self { storage, length: 0 }
    }
}

impl SocketAddress {
    pub(crate) fn as_mut_ptr(&mut self) -> *mut c_void {
        ptr::from_mut(&mut self.storage).cast()
    }

    pub(crate) fn to_std(self) -> io::Result<SocketAddr> {
        match self.storage.ss_family as libc::c_int {
            libc::AF_INET => {
                // SAFETY: ss_family identifies a sockaddr_in stored at this address.
                let address = unsafe {
                    ptr::from_ref(&self.storage)
                        .cast::<libc::sockaddr_in>()
                        .read()
                };
                Ok(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()),
                    u16::from_be(address.sin_port),
                )))
            }
            libc::AF_INET6 => {
                // SAFETY: ss_family identifies a sockaddr_in6 stored at this address.
                let address = unsafe {
                    ptr::from_ref(&self.storage)
                        .cast::<libc::sockaddr_in6>()
                        .read()
                };
                Ok(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(address.sin6_addr.s6_addr),
                    u16::from_be(address.sin6_port),
                    address.sin6_flowinfo,
                    address.sin6_scope_id,
                )))
            }
            family => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported UDP address family {family}"),
            )),
        }
    }
}

impl From<SocketAddr> for SocketAddress {
    fn from(address: SocketAddr) -> Self {
        let mut value = Self::default();
        match address {
            SocketAddr::V4(address) => {
                let raw = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: address.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(address.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                // SAFETY: sockaddr_storage is large and aligned enough for sockaddr_in.
                unsafe {
                    value.as_mut_ptr().cast::<libc::sockaddr_in>().write(raw);
                }
                value.length = mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
            }
            SocketAddr::V6(address) => {
                let raw = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: address.port().to_be(),
                    sin6_flowinfo: address.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: address.ip().octets(),
                    },
                    sin6_scope_id: address.scope_id(),
                };
                // SAFETY: sockaddr_storage is large and aligned enough for sockaddr_in6.
                unsafe {
                    value.as_mut_ptr().cast::<libc::sockaddr_in6>().write(raw);
                }
                value.length = mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
            }
        }
        value
    }
}

pub(crate) fn configure_socket(socket: &UdpSocket, config: DriverConfig) -> io::Result<()> {
    let requested: libc::c_int =
        config.socket_buffer_bytes().get().try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "socket buffer exceeds c_int")
        })?;
    set_socket_option(socket, libc::SOL_SOCKET, libc::SO_SNDBUF, requested)?;
    set_socket_option(socket, libc::SOL_SOCKET, libc::SO_RCVBUF, requested)?;
    set_socket_option(socket, libc::SOL_SOCKET, libc::SO_RXQ_OVFL, 1)?;
    if config.max_gso_segments().get() > 1 {
        let segment: libc::c_int = config
            .segment_size()
            .get()
            .try_into()
            .expect("validated segment size should fit c_int");
        set_socket_option(socket, libc::SOL_UDP, UDP_SEGMENT, segment)?;
    }
    if config.gro() {
        set_socket_option(socket, libc::SOL_UDP, UDP_GRO, 1)?;
    }
    Ok(())
}

fn set_socket_option<T: Copy>(
    socket: &UdpSocket,
    level: libc::c_int,
    name: libc::c_int,
    value: T,
) -> io::Result<()> {
    // SAFETY: value points to an initialized T for the exact duration of setsockopt.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            name,
            ptr::from_ref(&value).cast(),
            mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub(crate) const fn cmsg_align(length: usize) -> usize {
    let alignment = mem::size_of::<usize>();
    (length + alignment - 1) & !(alignment - 1)
}
