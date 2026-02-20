/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! One-way, multi-process, typed communication channels. These are used
//! to send messages between mailboxes residing in different processes.

// EnumAsInner generates code that triggers a false positive
// unused_assignments lint on struct variant fields. #[allow] on the
// enum itself doesn't propagate into derive-macro-generated code, so
// the suppression must be at module scope.
#![allow(unused_assignments)]

use core::net::SocketAddr;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv6Addr;
#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
use std::panic::Location;
use std::str::FromStr;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor_config::attrs::AttrValue;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate as hyperactor;
use crate::RemoteMessage;
use crate::channel::sim::SimAddr;
use crate::simnet::SimNetError;

pub(crate) mod local;
pub(crate) mod net;
pub mod sim;

/// The type of error that can occur on channel operations.
#[derive(thiserror::Error, Debug)]
pub enum ChannelError {
    /// An operation was attempted on a closed channel.
    #[error("channel closed")]
    Closed,

    /// An error occurred during send.
    #[error("send: {0}")]
    Send(#[source] anyhow::Error),

    /// A network client error.
    #[error(transparent)]
    Client(#[from] net::ClientError),

    /// The address was not valid.
    #[error("invalid address {0:?}")]
    InvalidAddress(String),

    /// A serving error was encountered.
    #[error(transparent)]
    Server(#[from] net::ServerError),

    /// A bincode serialization or deserialization error occurred.
    #[error(transparent)]
    Bincode(#[from] Box<bincode::ErrorKind>),

    /// Data encoding errors.
    #[error(transparent)]
    Data(#[from] wirevalue::Error),

    /// Some other error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),

    /// An operation timeout occurred.
    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// A simulator error occurred.
    #[error(transparent)]
    SimNetError(#[from] SimNetError),
}

/// An error that occurred during send. Returns the message that failed to send.
#[derive(thiserror::Error, Debug)]
#[error("{error} for reason {reason:?}")]
pub struct SendError<M: RemoteMessage> {
    /// Inner channel error
    #[source]
    pub error: ChannelError,
    /// Message that couldn't be sent
    pub message: M,
    /// Reason that message couldn't be sent, if any.
    pub reason: Option<String>,
}

impl<M: RemoteMessage> From<SendError<M>> for ChannelError {
    fn from(error: SendError<M>) -> Self {
        error.error
    }
}

/// The possible states of a `Tx`.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TxStatus {
    /// The tx is good.
    Active,
    /// The tx cannot be used for message delivery.
    Closed,
}

/// The transmit end of an M-typed channel.
#[async_trait]
pub trait Tx<M: RemoteMessage> {
    /// Post a message; returning failed deliveries on the return channel, if provided.
    /// If provided, the sender is dropped when the message has been
    /// enqueued at the channel endpoint.
    ///
    /// Users should use the `try_post`, and `post` variants directly.
    fn do_post(&self, message: M, return_channel: Option<oneshot::Sender<SendError<M>>>);

    /// Enqueue a `message` on the local end of the channel. The
    /// message is either delivered, or we eventually discover that
    /// the channel has failed and it will be sent back on `return_channel`.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SendError`.
    #[tracing::instrument(level = "debug", skip_all)]
    fn try_post(&self, message: M, return_channel: oneshot::Sender<SendError<M>>) {
        self.do_post(message, Some(return_channel));
    }

    /// Enqueue a message to be sent on the channel.
    #[hyperactor::instrument_infallible]
    fn post(&self, message: M) {
        self.do_post(message, None);
    }

    /// Send a message synchronously, returning when the message has
    /// been delivered to the remote end of the channel.
    async fn send(&self, message: M) -> Result<(), SendError<M>> {
        let (tx, rx) = oneshot::channel();
        self.try_post(message, tx);
        match rx.await {
            // Channel was closed; the message was not delivered.
            Ok(err) => Err(err),

            // Channel was dropped; the message was successfully enqueued
            // on the remote end of the channel.
            Err(_) => Ok(()),
        }
    }

    /// The channel address to which this Tx is sending.
    fn addr(&self) -> ChannelAddr;

    /// A means to monitor the health of a `Tx`.
    fn status(&self) -> &watch::Receiver<TxStatus>;
}

/// The receive end of an M-typed channel.
#[async_trait]
pub trait Rx<M: RemoteMessage> {
    /// Receive the next message from the channel. If the channel returns
    /// an error it is considered broken and should be discarded.
    async fn recv(&mut self) -> Result<M, ChannelError>;

    /// The channel address from which this Rx is receiving.
    fn addr(&self) -> ChannelAddr;
}

#[allow(dead_code)] // Not used outside tests.
struct MpscTx<M: RemoteMessage> {
    tx: mpsc::UnboundedSender<M>,
    addr: ChannelAddr,
    status: watch::Receiver<TxStatus>,
}

impl<M: RemoteMessage> MpscTx<M> {
    #[allow(dead_code)] // Not used outside tests.
    pub fn new(tx: mpsc::UnboundedSender<M>, addr: ChannelAddr) -> (Self, watch::Sender<TxStatus>) {
        let (sender, receiver) = watch::channel(TxStatus::Active);
        (
            Self {
                tx,
                addr,
                status: receiver,
            },
            sender,
        )
    }
}

#[async_trait]
impl<M: RemoteMessage> Tx<M> for MpscTx<M> {
    fn do_post(&self, message: M, return_channel: Option<oneshot::Sender<SendError<M>>>) {
        if let Err(mpsc::error::SendError(message)) = self.tx.send(message) {
            if let Some(return_channel) = return_channel {
                return_channel
                    .send(SendError {
                        error: ChannelError::Closed,
                        message,
                        reason: None,
                    })
                    .unwrap_or_else(|m| tracing::warn!("failed to deliver SendError: {}", m));
            }
        }
    }

    fn addr(&self) -> ChannelAddr {
        self.addr.clone()
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        &self.status
    }
}

#[allow(dead_code)] // Not used outside tests.
struct MpscRx<M: RemoteMessage> {
    rx: mpsc::UnboundedReceiver<M>,
    addr: ChannelAddr,
    // Used to report the status to the Tx side.
    status_sender: watch::Sender<TxStatus>,
}

impl<M: RemoteMessage> MpscRx<M> {
    #[allow(dead_code)] // Not used outside tests.
    pub fn new(
        rx: mpsc::UnboundedReceiver<M>,
        addr: ChannelAddr,
        status_sender: watch::Sender<TxStatus>,
    ) -> Self {
        Self {
            rx,
            addr,
            status_sender,
        }
    }
}

impl<M: RemoteMessage> Drop for MpscRx<M> {
    fn drop(&mut self) {
        let _ = self.status_sender.send(TxStatus::Closed);
    }
}

#[async_trait]
impl<M: RemoteMessage> Rx<M> for MpscRx<M> {
    async fn recv(&mut self) -> Result<M, ChannelError> {
        self.rx.recv().await.ok_or(ChannelError::Closed)
    }

    fn addr(&self) -> ChannelAddr {
        self.addr.clone()
    }
}

/// The hostname to use for TLS connections.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::EnumIter,
    strum::Display,
    strum::EnumString
)]
pub enum TcpMode {
    /// Use localhost/loopback for the connection.
    Localhost,
    /// Use host domain name for the connection.
    Hostname,
}

/// The hostname to use for TLS connections.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::EnumIter,
    strum::Display,
    strum::EnumString
)]
pub enum TlsMode {
    /// Use IpV6 address for TLS connections.
    IpV6,
    /// Use host domain name for TLS connections.
    Hostname,
    // TODO: consider adding IpV4 support.
}

/// Address format for Tls channels. Supports both hostname/port pairs
/// (required for clients for host identity) and direct socket addresses
/// (allowed for servers).
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    EnumAsInner
)]
pub enum TlsAddr {
    /// Hostname and port pair. Required for clients to establish host identity.
    Host {
        /// The hostname to connect to.
        hostname: Hostname,
        /// The port to connect to.
        port: Port,
    },
    /// Direct socket address. Allowed for servers.
    Socket(SocketAddr),
}

impl TlsAddr {
    /// Returns the port number for this address.
    pub fn port(&self) -> Port {
        match self {
            Self::Host { port, .. } => *port,
            Self::Socket(addr) => addr.port(),
        }
    }

    /// Returns the hostname if this is a Host variant, None otherwise.
    pub fn hostname(&self) -> Option<&str> {
        match self {
            Self::Host { hostname, .. } => Some(hostname),
            Self::Socket(_) => None,
        }
    }
}

impl fmt::Display for TlsAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Host { hostname, port } => write!(f, "{}:{}", hostname, port),
            Self::Socket(addr) => write!(f, "{}", addr),
        }
    }
}

/// Types of channel transports.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    typeuri::Named
)]
pub enum ChannelTransport {
    /// Transport over a TCP connection.
    Tcp(TcpMode),

    /// Transport over a TCP connection with TLS support within Meta
    MetaTls(TlsMode),

    /// Transport over a TCP connection with configurable TLS support
    Tls,

    /// Local transports uses an in-process registry and mpsc channels.
    Local,

    /// Sim is a simulated channel for testing.
    Sim(/*simulated transport:*/ Box<ChannelTransport>),

    /// Transport over unix domain socket.
    Unix,
}

impl fmt::Display for ChannelTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(mode) => write!(f, "tcp({:?})", mode),
            Self::MetaTls(mode) => write!(f, "metatls({:?})", mode),
            Self::Tls => write!(f, "tls"),
            Self::Local => write!(f, "local"),
            Self::Sim(transport) => write!(f, "sim({})", transport),
            Self::Unix => write!(f, "unix"),
        }
    }
}

impl FromStr for ChannelTransport {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Hacky parsing; can't recurse (e.g., sim(sim(..)))
        if let Some(rest) = s.strip_prefix("sim(") {
            if let Some(end) = rest.rfind(')') {
                let inner = &rest[..end];
                let inner_transport = ChannelTransport::from_str(inner)?;
                return Ok(ChannelTransport::Sim(Box::new(inner_transport)));
            } else {
                return Err(anyhow::anyhow!("invalid sim transport"));
            }
        }

        match s {
            // Default to TcpMode::Hostname, if the mode isn't set
            "tcp" => Ok(ChannelTransport::Tcp(TcpMode::Hostname)),
            s if s.starts_with("tcp(") => {
                let inner = &s["tcp(".len()..s.len() - 1];
                let mode = inner.parse()?;
                Ok(ChannelTransport::Tcp(mode))
            }
            "local" => Ok(ChannelTransport::Local),
            "unix" => Ok(ChannelTransport::Unix),
            "tls" => Ok(ChannelTransport::Tls),
            s if s.starts_with("metatls(") && s.ends_with(")") => {
                let inner = &s["metatls(".len()..s.len() - 1];
                let mode = inner.parse()?;
                Ok(ChannelTransport::MetaTls(mode))
            }
            unknown => Err(anyhow::anyhow!("unknown channel transport: {}", unknown)),
        }
    }
}

impl ChannelTransport {
    /// All known channel transports.
    pub fn all() -> [ChannelTransport; 3] {
        [
            // TODO: @rusch add back once figuring out unspecified override for OSS CI
            // ChannelTransport::Tcp(TcpMode::Localhost),
            ChannelTransport::Tcp(TcpMode::Hostname),
            ChannelTransport::Local,
            ChannelTransport::Unix,
            // Tls requires certificate configuration, tested separately in tls::tests
            // TODO add MetaTls (T208303369)
            // TODO ChannelTransport::Sim(Box::new(ChannelTransport::Tcp)),
            // TODO ChannelTransport::Sim(Box::new(ChannelTransport::Local)),
        ]
    }

    /// Return an "any" address for this transport.
    pub fn any(&self) -> ChannelAddr {
        ChannelAddr::any(self.clone())
    }

    /// Returns true if this transport type represents a remote channel.
    pub fn is_remote(&self) -> bool {
        match self {
            ChannelTransport::Tcp(_) => true,
            ChannelTransport::MetaTls(_) => true,
            ChannelTransport::Tls => true,
            ChannelTransport::Local => false,
            ChannelTransport::Sim(_) => false,
            ChannelTransport::Unix => false,
        }
    }
}

impl AttrValue for ChannelTransport {
    fn display(&self) -> String {
        self.to_string()
    }

    fn parse(s: &str) -> Result<Self, anyhow::Error> {
        s.parse()
    }
}

/// Specifies how to bind a channel server.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    typeuri::Named
)]
pub enum BindSpec {
    /// Bind to any available address for the given transport.
    Any(ChannelTransport),

    /// Bind to a specific channel address.
    Addr(ChannelAddr),
}

impl BindSpec {
    /// Return an "any" address for this bind spec.
    pub fn binding_addr(&self) -> ChannelAddr {
        match self {
            BindSpec::Any(transport) => ChannelAddr::any(transport.clone()),
            BindSpec::Addr(addr) => addr.clone(),
        }
    }
}

impl From<ChannelTransport> for BindSpec {
    fn from(transport: ChannelTransport) -> Self {
        BindSpec::Any(transport)
    }
}

impl From<ChannelAddr> for BindSpec {
    fn from(addr: ChannelAddr) -> Self {
        BindSpec::Addr(addr)
    }
}

impl fmt::Display for BindSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any(transport) => write!(f, "{}", transport),
            Self::Addr(addr) => write!(f, "{}", addr),
        }
    }
}

impl FromStr for BindSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(transport) = ChannelTransport::from_str(s) {
            Ok(BindSpec::Any(transport))
        } else if let Ok(addr) = ChannelAddr::from_zmq_url(s) {
            Ok(BindSpec::Addr(addr))
        } else if let Ok(addr) = ChannelAddr::from_str(s) {
            Ok(BindSpec::Addr(addr))
        } else {
            Err(anyhow::anyhow!("invalid bind spec: {}", s))
        }
    }
}

impl AttrValue for BindSpec {
    fn display(&self) -> String {
        self.to_string()
    }

    fn parse(s: &str) -> Result<Self, anyhow::Error> {
        Self::from_str(s)
    }
}

/// The type of (TCP) hostnames.
pub type Hostname = String;

/// The type of (TCP) ports.
pub type Port = u16;

/// The type of a channel address, used to multiplex different underlying
/// channel implementations. ChannelAddrs also have a concrete syntax:
/// the address type (e.g., "tcp" or "local"), followed by ":", and an address
/// parseable to that type. For example:
///
/// - `tcp:127.0.0.1:1234` - localhost port 1234 over TCP
/// - `tcp:192.168.0.1:1111` - 192.168.0.1 port 1111 over TCP
/// - `local:123` - the (in-process) local port 123
/// - `unix:/some/path` - the Unix socket at `/some/path`
///
/// Both local and TCP ports 0 are reserved to indicate "any available
/// port" when serving.
///
/// ```
/// # use hyperactor::channel::ChannelAddr;
/// let addr: ChannelAddr = "tcp:127.0.0.1:1234".parse().unwrap();
/// let ChannelAddr::Tcp(socket_addr) = addr else {
///     panic!()
/// };
/// assert_eq!(socket_addr.port(), 1234);
/// assert_eq!(socket_addr.is_ipv4(), true);
/// ```
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Hash,
    typeuri::Named
)]
pub enum ChannelAddr {
    /// A socket address used to establish TCP channels. Supports
    /// both  IPv4 and IPv6 address / port pairs.
    Tcp(SocketAddr),

    /// An address to establish TCP channels with TLS support within Meta.
    /// Uses TlsAddr which supports both hostname/port pairs (required for clients)
    /// and socket addresses (allowed for servers).
    MetaTls(TlsAddr),

    /// An address to establish TCP channels with configurable TLS support.
    /// Supports both hostname/port pairs (required for clients) and
    /// socket addresses (allowed for servers).
    Tls(TlsAddr),

    /// Local addresses are registered in-process and given an integral
    /// index.
    Local(u64),

    /// Sim is a simulated channel for testing.
    Sim(SimAddr),

    /// A unix domain socket address. Supports both absolute path names as
    ///  well as "abstract" names per https://manpages.debian.org/unstable/manpages/unix.7.en.html#Abstract_sockets
    Unix(net::unix::SocketAddr),

    /// A pair of addresses, one for the client and one for the server:
    ///   - The client should dial to the `dial_to` address.
    ///   - The server should bind to the `bind_to` address.
    ///
    /// The user is responsible for ensuring the traffic to the `dial_to` address
    /// is routed to the `bind_to` address.
    ///
    /// This is useful for scenarios where the network is configured in a way,
    /// that the bound address is not directly accessible from the client.
    ///
    /// For example, in AWS, the client could be provided with the public IP
    /// address, yet the server is bound to a private IP address or simply
    /// INADDR_ANY. Traffic to the public IP address is mapped to the private
    /// IP address through network address translation (NAT).
    Alias {
        /// The address to which the client should dial to.
        dial_to: Box<ChannelAddr>,
        /// The address to which the server should bind to.
        bind_to: Box<ChannelAddr>,
    },
}

impl From<SocketAddr> for ChannelAddr {
    fn from(value: SocketAddr) -> Self {
        Self::Tcp(value)
    }
}

impl From<net::unix::SocketAddr> for ChannelAddr {
    fn from(value: net::unix::SocketAddr) -> Self {
        Self::Unix(value)
    }
}

impl From<std::os::unix::net::SocketAddr> for ChannelAddr {
    fn from(value: std::os::unix::net::SocketAddr) -> Self {
        Self::Unix(net::unix::SocketAddr::new(value))
    }
}

impl From<tokio::net::unix::SocketAddr> for ChannelAddr {
    fn from(value: tokio::net::unix::SocketAddr) -> Self {
        std::os::unix::net::SocketAddr::from(value).into()
    }
}

impl ChannelAddr {
    /// The "any" address for the given transport type. This is used to
    /// servers to "any" address.
    pub fn any(transport: ChannelTransport) -> Self {
        match transport {
            ChannelTransport::Tcp(mode) => {
                let ip = match mode {
                    TcpMode::Localhost => IpAddr::V6(Ipv6Addr::LOCALHOST),
                    TcpMode::Hostname => {
                        hostname::get()
                            .ok()
                            .and_then(|hostname| {
                                // TODO: Avoid using DNS directly once we figure out a good extensibility story here
                                hostname.to_str().and_then(|hostname_str| {
                                    dns_lookup::lookup_host(hostname_str)
                                        .ok()
                                        .and_then(|addresses| addresses.first().cloned())
                                })
                            })
                            .expect("failed to resolve hostname to ip address")
                    }
                };
                Self::Tcp(SocketAddr::new(ip, 0))
            }
            ChannelTransport::MetaTls(mode) => {
                let host_address = match mode {
                    TlsMode::Hostname => hostname::get()
                        .ok()
                        .and_then(|hostname| hostname.to_str().map(|s| s.to_string()))
                        .unwrap_or("unknown_host".to_string()),
                    TlsMode::IpV6 => {
                        get_host_ipv6_address().expect("failed to retrieve ipv6 address")
                    }
                };
                Self::MetaTls(TlsAddr::Host {
                    hostname: host_address,
                    port: 0,
                })
            }
            ChannelTransport::Local => Self::Local(0),
            ChannelTransport::Tls => {
                let host_address = hostname::get()
                    .ok()
                    .and_then(|hostname| hostname.to_str().map(|s| s.to_string()))
                    .unwrap_or("localhost".to_string());
                Self::Tls(TlsAddr::Host {
                    hostname: host_address,
                    port: 0,
                })
            }
            ChannelTransport::Sim(transport) => sim::any(*transport),
            // This works because the file will be deleted but we know we have a unique file by this point.
            ChannelTransport::Unix => Self::Unix(net::unix::SocketAddr::from_str("").unwrap()),
        }
    }

    /// The transport used by this address.
    pub fn transport(&self) -> ChannelTransport {
        match self {
            Self::Tcp(addr) => {
                if addr.ip().is_loopback() {
                    ChannelTransport::Tcp(TcpMode::Localhost)
                } else {
                    ChannelTransport::Tcp(TcpMode::Hostname)
                }
            }
            Self::MetaTls(addr) => match addr {
                TlsAddr::Host { hostname, .. } => match hostname.parse::<IpAddr>() {
                    Ok(IpAddr::V6(_)) => ChannelTransport::MetaTls(TlsMode::IpV6),
                    Ok(IpAddr::V4(_)) => ChannelTransport::MetaTls(TlsMode::Hostname),
                    Err(_) => ChannelTransport::MetaTls(TlsMode::Hostname),
                },
                TlsAddr::Socket(socket_addr) => match socket_addr.ip() {
                    IpAddr::V6(_) => ChannelTransport::MetaTls(TlsMode::IpV6),
                    IpAddr::V4(_) => ChannelTransport::MetaTls(TlsMode::Hostname),
                },
            },
            Self::Tls(_) => ChannelTransport::Tls,
            Self::Local(_) => ChannelTransport::Local,
            Self::Sim(addr) => ChannelTransport::Sim(Box::new(addr.transport())),
            Self::Unix(_) => ChannelTransport::Unix,
            // bind_to's transport is what is actually used in communication.
            // Therefore we use its transport to represent the Alias.
            Self::Alias { bind_to, .. } => bind_to.transport(),
        }
    }
}

#[cfg(fbcode_build)]
fn get_host_ipv6_address() -> anyhow::Result<String> {
    crate::meta::host_ip::host_ipv6_address()
}

#[cfg(not(fbcode_build))]
fn get_host_ipv6_address() -> anyhow::Result<String> {
    Ok(local_ip_address::local_ipv6()?.to_string())
}

impl fmt::Display for ChannelAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp:{}", addr),
            Self::MetaTls(addr) => write!(f, "metatls:{}", addr),
            Self::Tls(addr) => write!(f, "tls:{}", addr),
            Self::Local(index) => write!(f, "local:{}", index),
            Self::Sim(sim_addr) => write!(f, "sim:{}", sim_addr),
            Self::Unix(addr) => write!(f, "unix:{}", addr),
            Self::Alias { dial_to, bind_to } => {
                write!(f, "alias:dial_to={};bind_to={}", dial_to, bind_to)
            }
        }
    }
}

impl FromStr for ChannelAddr {
    type Err = anyhow::Error;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.split_once('!').or_else(|| addr.split_once(':')) {
            Some(("local", rest)) => rest
                .parse::<u64>()
                .map(Self::Local)
                .map_err(anyhow::Error::from),
            Some(("tcp", rest)) => rest
                .parse::<SocketAddr>()
                .map(Self::Tcp)
                .map_err(anyhow::Error::from),
            Some(("metatls", rest)) => net::meta::parse(rest).map_err(|e| e.into()),
            Some(("tls", rest)) => net::tls::parse(rest).map_err(|e| e.into()),
            Some(("sim", rest)) => sim::parse(rest).map_err(|e| e.into()),
            Some(("unix", rest)) => Ok(Self::Unix(net::unix::SocketAddr::from_str(rest)?)),
            Some(("alias", _)) => Err(anyhow::anyhow!(
                "detect possible alias address, but we currently do not support \
                parsing alias' string representation since we only want to \
                support parsing its zmq url format."
            )),
            Some((r#type, _)) => Err(anyhow::anyhow!("no such channel type: {type}")),
            None => Err(anyhow::anyhow!("no channel type specified")),
        }
    }
}

impl ChannelAddr {
    /// Parse ZMQ-style URL format: scheme://address
    /// Supports:
    /// - tcp://hostname:port or tcp://*:port (wildcard binding)
    /// - inproc://endpoint-name (equivalent to local)
    /// - ipc://path (equivalent to unix)
    /// - metatls://hostname:port or metatls://*:port
    /// - Alias format: dial_to_url@bind_to_url (e.g., tcp://host:port@tcp://host:port)
    ///   Note: Alias format is currently only supported for TCP addresses
    pub fn from_zmq_url(address: &str) -> Result<Self, anyhow::Error> {
        // Check for Alias format: dial_to_url@bind_to_url
        // The @ character separates two valid ZMQ URLs
        if let Some(at_pos) = address.find('@') {
            let dial_to_str = &address[..at_pos];
            let bind_to_str = &address[at_pos + 1..];

            // Validate that both addresses use TCP scheme
            if !dial_to_str.starts_with("tcp://") {
                return Err(anyhow::anyhow!(
                    "alias format is only supported for TCP addresses, got dial_to: {}",
                    dial_to_str
                ));
            }
            if !bind_to_str.starts_with("tcp://") {
                return Err(anyhow::anyhow!(
                    "alias format is only supported for TCP addresses, got bind_to: {}",
                    bind_to_str
                ));
            }

            let dial_to = Self::from_zmq_url(dial_to_str)?;
            let bind_to = Self::from_zmq_url(bind_to_str)?;

            return Ok(Self::Alias {
                dial_to: Box::new(dial_to),
                bind_to: Box::new(bind_to),
            });
        }

        // Try ZMQ-style URL format first (scheme://...)
        let (scheme, address) = address.split_once("://").ok_or_else(|| {
            anyhow::anyhow!("address must be in url form scheme://endppoint {}", address)
        })?;

        match scheme {
            "tcp" => {
                let (host, port) = Self::split_host_port(address)?;

                if host == "*" {
                    // Wildcard binding - use IPv6 unspecified address
                    Ok(Self::Tcp(SocketAddr::new("::".parse().unwrap(), port)))
                } else {
                    // Resolve hostname to IP address for proper SocketAddr creation
                    let socket_addr = Self::resolve_hostname_to_socket_addr(host, port)?;
                    Ok(Self::Tcp(socket_addr))
                }
            }
            "inproc" => {
                // inproc://port -> local:port
                // Port must be a valid u64 number
                let port = address.parse::<u64>().map_err(|_| {
                    anyhow::anyhow!("inproc endpoint must be a valid port number: {}", address)
                })?;
                Ok(Self::Local(port))
            }
            "ipc" => {
                // ipc://path -> unix:path
                Ok(Self::Unix(net::unix::SocketAddr::from_str(address)?))
            }
            "metatls" => {
                let (host, port) = Self::split_host_port(address)?;

                if host == "*" {
                    // Wildcard binding - use IPv6 unspecified address directly without hostname resolution
                    Ok(Self::MetaTls(TlsAddr::Host {
                        hostname: std::net::Ipv6Addr::UNSPECIFIED.to_string(),
                        port,
                    }))
                } else {
                    Ok(Self::MetaTls(TlsAddr::Host {
                        hostname: host.to_string(),
                        port,
                    }))
                }
            }
            "tls" => {
                let (host, port) = Self::split_host_port(address)?;

                if host == "*" {
                    // Wildcard binding - use IPv6 unspecified address directly without hostname resolution
                    Ok(Self::Tls(TlsAddr::Host {
                        hostname: std::net::Ipv6Addr::UNSPECIFIED.to_string(),
                        port,
                    }))
                } else {
                    Ok(Self::Tls(TlsAddr::Host {
                        hostname: host.to_string(),
                        port,
                    }))
                }
            }
            scheme => Err(anyhow::anyhow!("unsupported ZMQ scheme: {}", scheme)),
        }
    }

    /// Split host:port string, supporting IPv6 addresses
    fn split_host_port(address: &str) -> Result<(&str, u16), anyhow::Error> {
        if let Some((host, port_str)) = address.rsplit_once(':') {
            let port: u16 = port_str
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid port: {}", port_str))?;
            Ok((host, port))
        } else {
            Err(anyhow::anyhow!("invalid address format: {}", address))
        }
    }

    /// Resolve hostname to SocketAddr, handling both IP addresses and hostnames
    fn resolve_hostname_to_socket_addr(host: &str, port: u16) -> Result<SocketAddr, anyhow::Error> {
        // Handle IPv6 addresses in brackets by stripping the brackets
        let host_clean = if host.starts_with('[') && host.ends_with(']') {
            &host[1..host.len() - 1]
        } else {
            host
        };

        // First try to parse as an IP address directly
        if let Ok(ip_addr) = host_clean.parse::<IpAddr>() {
            return Ok(SocketAddr::new(ip_addr, port));
        }

        // If not an IP, try hostname resolution
        use std::net::ToSocketAddrs;
        let mut addrs = (host_clean, port)
            .to_socket_addrs()
            .map_err(|e| anyhow::anyhow!("failed to resolve hostname '{}': {}", host_clean, e))?;

        addrs
            .next()
            .ok_or_else(|| anyhow::anyhow!("no addresses found for hostname '{}'", host_clean))
    }
}

/// Universal channel transmitter.
pub struct ChannelTx<M: RemoteMessage> {
    inner: ChannelTxKind<M>,
}

impl<M: RemoteMessage> fmt::Debug for ChannelTx<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelTx")
            .field("addr", &self.addr())
            .finish()
    }
}

/// Universal channel transmitter.
enum ChannelTxKind<M: RemoteMessage> {
    Local(local::LocalTx<M>),
    Tcp(net::NetTx<M>),
    MetaTls(net::NetTx<M>),
    Tls(net::NetTx<M>),
    Unix(net::NetTx<M>),
    Sim(sim::SimTx<M>),
}

#[async_trait]
impl<M: RemoteMessage> Tx<M> for ChannelTx<M> {
    fn do_post(&self, message: M, return_channel: Option<oneshot::Sender<SendError<M>>>) {
        match &self.inner {
            ChannelTxKind::Local(tx) => tx.do_post(message, return_channel),
            ChannelTxKind::Tcp(tx) => tx.do_post(message, return_channel),
            ChannelTxKind::MetaTls(tx) => tx.do_post(message, return_channel),
            ChannelTxKind::Tls(tx) => tx.do_post(message, return_channel),
            ChannelTxKind::Sim(tx) => tx.do_post(message, return_channel),
            ChannelTxKind::Unix(tx) => tx.do_post(message, return_channel),
        }
    }

    fn addr(&self) -> ChannelAddr {
        match &self.inner {
            ChannelTxKind::Local(tx) => tx.addr(),
            ChannelTxKind::Tcp(tx) => Tx::<M>::addr(tx),
            ChannelTxKind::MetaTls(tx) => Tx::<M>::addr(tx),
            ChannelTxKind::Tls(tx) => Tx::<M>::addr(tx),
            ChannelTxKind::Sim(tx) => tx.addr(),
            ChannelTxKind::Unix(tx) => Tx::<M>::addr(tx),
        }
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        match &self.inner {
            ChannelTxKind::Local(tx) => tx.status(),
            ChannelTxKind::Tcp(tx) => tx.status(),
            ChannelTxKind::MetaTls(tx) => tx.status(),
            ChannelTxKind::Tls(tx) => tx.status(),
            ChannelTxKind::Sim(tx) => tx.status(),
            ChannelTxKind::Unix(tx) => tx.status(),
        }
    }
}

/// Universal channel receiver.
pub struct ChannelRx<M: RemoteMessage> {
    inner: ChannelRxKind<M>,
}

impl<M: RemoteMessage> fmt::Debug for ChannelRx<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelRx")
            .field("addr", &self.addr())
            .finish()
    }
}

/// Universal channel receiver.
enum ChannelRxKind<M: RemoteMessage> {
    Local(local::LocalRx<M>),
    Tcp(net::NetRx<M>),
    MetaTls(net::NetRx<M>),
    Tls(net::NetRx<M>),
    Unix(net::NetRx<M>),
    Sim(sim::SimRx<M>),
}

#[async_trait]
impl<M: RemoteMessage> Rx<M> for ChannelRx<M> {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn recv(&mut self) -> Result<M, ChannelError> {
        match &mut self.inner {
            ChannelRxKind::Local(rx) => rx.recv().await,
            ChannelRxKind::Tcp(rx) => rx.recv().await,
            ChannelRxKind::MetaTls(rx) => rx.recv().await,
            ChannelRxKind::Tls(rx) => rx.recv().await,
            ChannelRxKind::Sim(rx) => rx.recv().await,
            ChannelRxKind::Unix(rx) => rx.recv().await,
        }
    }

    fn addr(&self) -> ChannelAddr {
        match &self.inner {
            ChannelRxKind::Local(rx) => rx.addr(),
            ChannelRxKind::Tcp(rx) => rx.addr(),
            ChannelRxKind::MetaTls(rx) => rx.addr(),
            ChannelRxKind::Tls(rx) => rx.addr(),
            ChannelRxKind::Sim(rx) => rx.addr(),
            ChannelRxKind::Unix(rx) => rx.addr(),
        }
    }
}

/// Dial the provided address, returning the corresponding Tx, or error
/// if the channel cannot be established. The underlying connection is
/// dropped whenever the returned Tx is dropped.
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ChannelError`.
#[track_caller]
pub fn dial<M: RemoteMessage>(addr: ChannelAddr) -> Result<ChannelTx<M>, ChannelError> {
    tracing::debug!(name = "dial", caller = %Location::caller(), %addr, "dialing channel {}", addr);
    let inner = match addr {
        ChannelAddr::Local(port) => ChannelTxKind::Local(local::dial(port)?),
        ChannelAddr::Tcp(addr) => ChannelTxKind::Tcp(net::tcp::dial(addr)),
        ChannelAddr::MetaTls(meta_addr) => ChannelTxKind::MetaTls(net::meta::dial(meta_addr)?),
        ChannelAddr::Tls(tls_addr) => ChannelTxKind::Tls(net::tls::dial(tls_addr)?),
        ChannelAddr::Sim(sim_addr) => ChannelTxKind::Sim(sim::dial::<M>(sim_addr)?),
        ChannelAddr::Unix(path) => ChannelTxKind::Unix(net::unix::dial(path)),
        ChannelAddr::Alias { dial_to, .. } => dial(*dial_to)?.inner,
    };
    Ok(ChannelTx { inner })
}

/// Serve on the provided channel address. The server is turned down
/// when the returned Rx is dropped.
#[track_caller]
pub fn serve<M: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(ChannelAddr, ChannelRx<M>), ChannelError> {
    let caller = Location::caller();
    serve_inner(addr).map(|(addr, inner)| {
        tracing::debug!(
            name = "serve",
            %addr,
            %caller,
        );
        (addr, ChannelRx { inner })
    })
}

fn serve_inner<M: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(ChannelAddr, ChannelRxKind<M>), ChannelError> {
    match addr {
        ChannelAddr::Tcp(addr) => {
            let (addr, rx) = net::tcp::serve::<M>(addr)?;
            Ok((addr, ChannelRxKind::Tcp(rx)))
        }
        ChannelAddr::MetaTls(meta_addr) => {
            let (addr, rx) = net::meta::serve::<M>(meta_addr)?;
            Ok((addr, ChannelRxKind::MetaTls(rx)))
        }
        ChannelAddr::Tls(tls_addr) => {
            let (addr, rx) = net::tls::serve::<M>(tls_addr)?;
            Ok((addr, ChannelRxKind::Tls(rx)))
        }
        ChannelAddr::Unix(path) => {
            let (addr, rx) = net::unix::serve::<M>(path)?;
            Ok((addr, ChannelRxKind::Unix(rx)))
        }
        ChannelAddr::Local(0) => {
            let (port, rx) = local::serve::<M>();
            Ok((ChannelAddr::Local(port), ChannelRxKind::Local(rx)))
        }
        ChannelAddr::Sim(sim_addr) => {
            let (addr, rx) = sim::serve::<M>(sim_addr)?;
            Ok((addr, ChannelRxKind::Sim(rx)))
        }
        ChannelAddr::Local(a) => Err(ChannelError::InvalidAddress(format!(
            "invalid local addr: {}",
            a
        ))),
        ChannelAddr::Alias { dial_to, bind_to } => {
            let (bound_addr, rx) = serve_inner::<M>(*bind_to)?;
            let alias_addr = ChannelAddr::Alias {
                dial_to,
                bind_to: Box::new(bound_addr),
            };
            Ok((alias_addr, rx))
        }
    }
}

/// Serve on the local address. The server is turned down
/// when the returned Rx is dropped.
pub fn serve_local<M: RemoteMessage>() -> (ChannelAddr, ChannelRx<M>) {
    let (port, rx) = local::serve::<M>();
    (
        ChannelAddr::Local(port),
        ChannelRx {
            inner: ChannelRxKind::Local(rx),
        },
    )
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashSet;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::time::Duration;

    use tokio::task::JoinSet;

    use super::net::*;
    use super::*;
    use crate::clock::Clock;
    use crate::clock::RealClock;

    #[test]
    fn test_channel_addr() {
        let cases_ok = vec![
            (
                "tcp<DELIM>[::1]:1234",
                ChannelAddr::Tcp(SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
                    1234,
                )),
            ),
            (
                "tcp<DELIM>127.0.0.1:8080",
                ChannelAddr::Tcp(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    8080,
                )),
            ),
            #[cfg(target_os = "linux")]
            ("local<DELIM>123", ChannelAddr::Local(123)),
            (
                "unix<DELIM>@yolo",
                ChannelAddr::Unix(
                    unix::SocketAddr::from_abstract_name("yolo")
                        .expect("can't make socket from abstract name"),
                ),
            ),
            (
                "unix<DELIM>/cool/socket-path",
                ChannelAddr::Unix(
                    unix::SocketAddr::from_pathname("/cool/socket-path")
                        .expect("can't make socket from path"),
                ),
            ),
        ];

        for (raw, parsed) in cases_ok.clone() {
            for delim in ["!", ":"] {
                let raw = raw.replace("<DELIM>", delim);
                assert_eq!(raw.parse::<ChannelAddr>().unwrap(), parsed);
            }
        }

        for (raw, parsed) in cases_ok {
            for delim in ["!", ":"] {
                // We don't allow mixing and matching delims
                let raw = format!("sim{}{}", delim, raw.replace("<DELIM>", delim));
                assert_eq!(
                    raw.parse::<ChannelAddr>().unwrap(),
                    ChannelAddr::Sim(SimAddr::new(parsed.clone()).unwrap())
                );
            }
        }

        let cases_err = vec![
            ("tcp:abcdef..123124", "invalid socket address syntax"),
            ("xxx:foo", "no such channel type: xxx"),
            ("127.0.0.1", "no channel type specified"),
            ("local:abc", "invalid digit found in string"),
        ];

        for (raw, error) in cases_err {
            let Err(err) = raw.parse::<ChannelAddr>() else {
                panic!("expected error parsing: {}", &raw)
            };
            assert_eq!(format!("{}", err), error);
        }
    }

    #[test]
    fn test_zmq_style_channel_addr() {
        // Test TCP addresses
        assert_eq!(
            ChannelAddr::from_zmq_url("tcp://127.0.0.1:8080").unwrap(),
            ChannelAddr::Tcp("127.0.0.1:8080".parse().unwrap())
        );

        // Test TCP wildcard binding
        assert_eq!(
            ChannelAddr::from_zmq_url("tcp://*:5555").unwrap(),
            ChannelAddr::Tcp("[::]:5555".parse().unwrap())
        );

        // Test inproc (maps to local with numeric endpoint)
        assert_eq!(
            ChannelAddr::from_zmq_url("inproc://12345").unwrap(),
            ChannelAddr::Local(12345)
        );

        // Test ipc (maps to unix)
        assert_eq!(
            ChannelAddr::from_zmq_url("ipc:///tmp/my-socket").unwrap(),
            ChannelAddr::Unix(unix::SocketAddr::from_pathname("/tmp/my-socket").unwrap())
        );

        // Test metatls with hostname
        assert_eq!(
            ChannelAddr::from_zmq_url("metatls://example.com:443").unwrap(),
            ChannelAddr::MetaTls(TlsAddr::Host {
                hostname: "example.com".to_string(),
                port: 443
            })
        );

        // Test metatls with IP address (should be normalized)
        assert_eq!(
            ChannelAddr::from_zmq_url("metatls://192.168.1.1:443").unwrap(),
            ChannelAddr::MetaTls(TlsAddr::Host {
                hostname: "192.168.1.1".to_string(),
                port: 443
            })
        );

        // Test metatls with wildcard (should use IPv6 unspecified address)
        assert_eq!(
            ChannelAddr::from_zmq_url("metatls://*:8443").unwrap(),
            ChannelAddr::MetaTls(TlsAddr::Host {
                hostname: "::".to_string(),
                port: 8443
            })
        );

        // Test TCP hostname resolution (should resolve hostname to IP)
        // Note: This test may fail in environments without proper DNS resolution
        // We test that it at least doesn't fail to parse
        let tcp_hostname_result = ChannelAddr::from_zmq_url("tcp://localhost:8080");
        assert!(tcp_hostname_result.is_ok());

        // Test IPv6 address
        assert_eq!(
            ChannelAddr::from_zmq_url("tcp://[::1]:1234").unwrap(),
            ChannelAddr::Tcp("[::1]:1234".parse().unwrap())
        );

        // Test error cases
        assert!(ChannelAddr::from_zmq_url("invalid://scheme").is_err());
        assert!(ChannelAddr::from_zmq_url("tcp://invalid-port").is_err());
        assert!(ChannelAddr::from_zmq_url("metatls://no-port").is_err());
        assert!(ChannelAddr::from_zmq_url("inproc://not-a-number").is_err());
    }

    #[test]
    fn test_zmq_style_alias_channel_addr() {
        // Test Alias format: dial_to_url@bind_to_url
        // The format is: dial_to_url@bind_to_url where both are valid ZMQ URLs
        // Note: Alias format is only supported for TCP addresses

        // Test Alias with tcp on both sides
        let alias_addr = ChannelAddr::from_zmq_url("tcp://127.0.0.1:9000@tcp://[::]:8800").unwrap();
        match alias_addr {
            ChannelAddr::Alias { dial_to, bind_to } => {
                assert_eq!(
                    *dial_to,
                    ChannelAddr::Tcp("127.0.0.1:9000".parse().unwrap())
                );
                assert_eq!(*bind_to, ChannelAddr::Tcp("[::]:8800".parse().unwrap()));
            }
            _ => panic!("Expected Alias"),
        }

        // Test error: alias with non-tcp dial_to (not supported)
        assert!(
            ChannelAddr::from_zmq_url("metatls://example.com:443@tcp://127.0.0.1:8080").is_err()
        );

        // Test error: alias with non-tcp bind_to (not supported)
        assert!(
            ChannelAddr::from_zmq_url("tcp://127.0.0.1:8080@metatls://example.com:443").is_err()
        );

        // Test error: invalid dial_to URL in Alias
        assert!(ChannelAddr::from_zmq_url("invalid://scheme@tcp://127.0.0.1:8080").is_err());

        // Test error: invalid bind_to URL in Alias
        assert!(ChannelAddr::from_zmq_url("tcp://127.0.0.1:8080@invalid://scheme").is_err());

        // Test error: missing port in dial_to
        assert!(ChannelAddr::from_zmq_url("tcp://host@tcp://127.0.0.1:8080").is_err());

        // Test error: missing port in bind_to
        assert!(ChannelAddr::from_zmq_url("tcp://127.0.0.1:8080@tcp://example.com").is_err());
    }

    #[tokio::test]
    async fn test_multiple_connections() {
        for addr in ChannelTransport::all().map(ChannelAddr::any) {
            let (listen_addr, mut rx) = crate::channel::serve::<u64>(addr).unwrap();

            let mut sends: JoinSet<()> = JoinSet::new();
            for message in 0u64..100u64 {
                let addr = listen_addr.clone();
                sends.spawn(async move {
                    let tx = dial::<u64>(addr).unwrap();
                    tx.post(message);
                });
            }

            let mut received: HashSet<u64> = HashSet::new();
            while received.len() < 100 {
                received.insert(rx.recv().await.unwrap());
            }

            for message in 0u64..100u64 {
                assert!(received.contains(&message));
            }

            loop {
                match sends.join_next().await {
                    Some(Ok(())) => (),
                    Some(Err(err)) => panic!("{}", err),
                    None => break,
                }
            }
        }
    }

    #[tokio::test]
    async fn test_server_close() {
        for addr in ChannelTransport::all().map(ChannelAddr::any) {
            if net::is_net_addr(&addr) {
                // Net has store-and-forward semantics. We don't expect failures
                // on closure.
                continue;
            }

            let (listen_addr, rx) = crate::channel::serve::<u64>(addr).unwrap();

            let tx = dial::<u64>(listen_addr).unwrap();
            tx.post(123);
            drop(rx);

            // New transmits should fail... but there is buffering, etc.,
            // which can cause the failure to be delayed. We give it
            // a deadline, but it can still technically fail -- the test
            // should be considered a kind of integration test.
            let start = RealClock.now();

            let result = loop {
                let (return_tx, return_rx) = oneshot::channel();
                tx.try_post(123, return_tx);
                let result = return_rx.await;

                if result.is_ok() || start.elapsed() > Duration::from_secs(10) {
                    break result;
                }
            };
            assert_matches!(
                result,
                Ok(SendError {
                    error: ChannelError::Closed,
                    message: 123,
                    reason: None
                })
            );
        }
    }

    fn addrs() -> Vec<ChannelAddr> {
        use rand::Rng;
        use rand::distributions::Uniform;

        let rng = rand::thread_rng();
        vec![
            "tcp:[::1]:0".parse().unwrap(),
            "local:0".parse().unwrap(),
            #[cfg(target_os = "linux")]
            "unix:".parse().unwrap(),
            #[cfg(target_os = "linux")]
            format!(
                "unix:@{}",
                rng.sample_iter(Uniform::new_inclusive('a', 'z'))
                    .take(10)
                    .collect::<String>()
            )
            .parse()
            .unwrap(),
        ]
    }

    #[test]
    fn test_bind_spec_from_str() {
        // Test parsing ChannelTransport strings -> BindSpec::Any
        assert_eq!(
            BindSpec::from_str("tcp").unwrap(),
            BindSpec::Any(ChannelTransport::Tcp(TcpMode::Hostname))
        );
        assert_eq!(
            BindSpec::from_str("metatls(Hostname)").unwrap(),
            BindSpec::Any(ChannelTransport::MetaTls(TlsMode::Hostname))
        );

        // Test parsing ChannelAddr strings -> BindSpec::Addr
        assert_eq!(
            BindSpec::from_str("tcp:127.0.0.1:8080").unwrap(),
            BindSpec::Addr(ChannelAddr::Tcp("127.0.0.1:8080".parse().unwrap()))
        );

        // Test parsing ZMQ URL format -> BindSpec::Addr
        assert_eq!(
            BindSpec::from_str("tcp://127.0.0.1:9000").unwrap(),
            BindSpec::Addr(ChannelAddr::Tcp("127.0.0.1:9000".parse().unwrap()))
        );
        assert_eq!(
            BindSpec::from_str("tcp://127.0.0.1:9000@tcp://[::1]:7200").unwrap(),
            BindSpec::Addr(
                ChannelAddr::from_zmq_url("tcp://127.0.0.1:9000@tcp://[::1]:7200").unwrap()
            )
        );

        // Test error cases
        assert!(BindSpec::from_str("invalid_spec").is_err());
        assert!(BindSpec::from_str("unknown://scheme").is_err());
        assert!(BindSpec::from_str("").is_err());
    }

    #[tokio::test]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Server(Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" }))
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_dial_serve() {
        for addr in addrs() {
            let (listen_addr, mut rx) = crate::channel::serve::<i32>(addr).unwrap();
            let tx = crate::channel::dial(listen_addr).unwrap();
            tx.post(123);
            assert_eq!(rx.recv().await.unwrap(), 123);
        }
    }

    #[tokio::test]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Server(Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" }))
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_send() {
        let config = hyperactor_config::global::lock();

        // Use temporary config for this test
        let _guard1 = config.override_key(
            crate::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_secs(1),
        );
        let _guard2 = config.override_key(crate::config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);
        for addr in addrs() {
            let (listen_addr, mut rx) = crate::channel::serve::<i32>(addr).unwrap();
            let tx = crate::channel::dial(listen_addr).unwrap();
            tx.send(123).await.unwrap();
            assert_eq!(rx.recv().await.unwrap(), 123);

            drop(rx);
            assert_matches!(
                tx.send(123).await.unwrap_err(),
                SendError {
                    error: ChannelError::Closed,
                    message: 123,
                    ..
                }
            );
        }
    }
}
