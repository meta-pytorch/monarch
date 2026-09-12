/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Embeddable distributed process namespace and transport.

mod link_protocol;
mod node;
mod socket;

pub use chrysalis_core as core;
pub use chrysalis_core::CID_LEN;
pub use chrysalis_core::ConnectionKey;
pub use chrysalis_core::InitialNonce;
pub use chrysalis_core::LinkContext;
pub use chrysalis_core::LinkId;
pub use chrysalis_core::LinkSide;
pub use chrysalis_core::ParsePidPrefixError;
pub use chrysalis_core::Pid;
pub use chrysalis_core::PidPrefix;
pub use chrysalis_core::RoutedCid;
pub use chrysalis_core::target_pid;
pub use chrysalis_nameserver as nameserver;
pub use chrysalis_nameserver::ChildLinkServer;
pub use chrysalis_nameserver::DEFAULT_ENUMERATION_PAGE_SIZE;
pub use chrysalis_nameserver::EnumerationCursor;
pub use chrysalis_nameserver::EnumerationPage;
pub use chrysalis_nameserver::EnumerationResult;
pub use chrysalis_nameserver::KeyError;
pub use chrysalis_nameserver::LabelError;
pub use chrysalis_nameserver::LabelKey;
pub use chrysalis_nameserver::LabelValue;
pub use chrysalis_nameserver::Labels;
pub use chrysalis_nameserver::Locator;
pub use chrysalis_nameserver::MAX_ENUMERATION_PAGE_SIZE;
pub use chrysalis_nameserver::NAMESERVER_LINK_PROTOCOL;
pub use chrysalis_nameserver::NameserverService;
pub use chrysalis_nameserver::NamespaceConfig;
pub use chrysalis_nameserver::ParentEndpoint;
pub use chrysalis_nameserver::ParentIdentity;
pub use chrysalis_nameserver::ParentLinkManager;
pub use chrysalis_nameserver::ParentManagerError;
pub use chrysalis_nameserver::ParentManagerStatus;
pub use chrysalis_nameserver::ProcEntry;
pub use chrysalis_nameserver::Resolution;
pub use chrysalis_nameserver::ResolveConsistency;
pub use chrysalis_nameserver::Revision;
pub use chrysalis_nameserver::UpstreamNameserver;
pub use chrysalis_nameserver::VERSION_1;
pub use chrysalis_nameserver::VERSION_2;
pub use chrysalis_nameserver::VERSION_3;
pub use chrysalis_nameserver::VersionRange;
pub use chrysalis_transport as transport;
pub use chrysalis_transport::DatagramAddr;
pub use chrysalis_transport::DatagramSocket;
pub use chrysalis_transport::DatagramSocketSet;
pub use chrysalis_transport::DatagramSwitch;
pub use chrysalis_transport::DropReason;
pub use chrysalis_transport::ForwardDisposition;
pub use chrysalis_transport::IncomingStream;
pub use chrysalis_transport::InprocAddr;
pub use chrysalis_transport::InprocNetwork;
pub use chrysalis_transport::InprocSocket;
pub use chrysalis_transport::LINK_LOCAL_PROTOCOL_ID_LEN;
pub use chrysalis_transport::LinkLocalError;
pub use chrysalis_transport::LinkLocalMux;
pub use chrysalis_transport::LinkLocalProtocol;
pub use chrysalis_transport::LinkLocalProtocolId;
pub use chrysalis_transport::QuicConfig;
pub use chrysalis_transport::QuicConnectionStats;
pub use chrysalis_transport::QuicIdentity;
pub use chrysalis_transport::QuicIoStats;
pub use chrysalis_transport::QuicTransport;
pub use chrysalis_transport::QuicTransportError;
pub use chrysalis_transport::ReceiveOptions;
pub use chrysalis_transport::ReceiveStatus;
pub use chrysalis_transport::RecvFrom;
pub use chrysalis_transport::RecvStream;
pub use chrysalis_transport::Route;
pub use chrysalis_transport::RouteGate;
pub use chrysalis_transport::Router;
pub use chrysalis_transport::SendStream;
pub use chrysalis_transport::Stream;
pub use chrysalis_transport::SwitchSocket;
pub use chrysalis_transport::UdpSocket;
#[cfg(unix)]
pub use chrysalis_transport::UnixDatagramSocket;
pub use chrysalis_transport::certificate_pid;
pub use node::Node;
pub use node::NodeConfig;
pub use node::NodeError;
pub use node::TransportConfig;
