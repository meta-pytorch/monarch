/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Carrier-neutral QUIC transport for Chrysalis.

mod datagram;
mod inproc;
mod link_local;
mod packet_io;
mod quic_transport;
mod route;
mod shutdown;
mod socket_set;
mod switch;
mod udp;
#[cfg(unix)]
mod unix;

pub use chrysalis_transport_core::ReceiveOptions;
pub use chrysalis_transport_core::ReceiveStatus;
pub use datagram::DatagramAddr;
pub use datagram::DatagramRecvMeta;
pub use datagram::DatagramSocket;
pub use datagram::DatagramTransmit;
pub use datagram::RecvFrom;
pub use inproc::InprocAddr;
pub use inproc::InprocNetwork;
pub use inproc::InprocSocket;
pub use link_local::LINK_LOCAL_PROTOCOL_ID_LEN;
pub use link_local::LinkLocalError;
pub use link_local::LinkLocalMux;
pub use link_local::LinkLocalProtocol;
pub use link_local::LinkLocalProtocolId;
pub use quic_transport::IncomingStream;
pub use quic_transport::QuicConfig;
pub use quic_transport::QuicConfigError;
pub use quic_transport::QuicConnectionStats;
pub use quic_transport::QuicIdentity;
pub use quic_transport::QuicIoStats;
pub use quic_transport::QuicTransport;
pub use quic_transport::QuicTransportError;
pub use quic_transport::RecvStream;
pub use quic_transport::SendStream;
pub use quic_transport::Stream;
pub use quic_transport::certificate_pid;
pub use route::DropReason;
pub use route::ForwardDisposition;
pub use route::Route;
pub use route::RouteGate;
pub use route::Router;
pub use socket_set::DatagramSocketSet;
pub use switch::DatagramSwitch;
pub use switch::SwitchSocket;
pub use udp::UdpSocket;
#[cfg(unix)]
pub use unix::UnixDatagramSocket;
