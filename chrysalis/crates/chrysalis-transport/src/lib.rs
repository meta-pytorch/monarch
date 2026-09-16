/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Carrier-neutral datagram and QUIC transport for Chrysalis.

mod datagram;
mod inproc;
mod shutdown;
mod socket_set;
mod udp;
#[cfg(unix)]
mod unix;

pub use datagram::DatagramAddr;
pub use datagram::DatagramRecvMeta;
pub use datagram::DatagramSocket;
pub use datagram::DatagramTransmit;
pub use datagram::RecvFrom;
pub use inproc::InprocAddr;
pub use inproc::InprocNetwork;
pub use inproc::InprocSocket;
pub use socket_set::DatagramSocketSet;
pub use udp::UdpSocket;
#[cfg(unix)]
pub use unix::UnixDatagramSocket;
