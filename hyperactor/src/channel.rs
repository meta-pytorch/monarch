/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! One-way, multi-process, typed communication channels. These are used
//! to send messages between mailboxes residing in different processes.

#![allow(dead_code)] // Allow until this is used outside of tests.

use core::net::SocketAddr;
use std::fmt;
use std::net::IpAddr;
#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
use std::str::FromStr;

use async_trait::async_trait;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate as hyperactor;
use crate::Named;
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
#[error("{0}")]
pub struct SendError<M: RemoteMessage>(#[source] pub ChannelError, pub M);

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
pub trait Tx<M: RemoteMessage>: std::fmt::Debug {
    /// Enqueue a `message` on the local end of the channel. The
    /// message is either delivered, or we eventually discover that
    /// the channel has failed and it will be sent back on `return_handle`.
    // TODO: the return channel should be SendError<M> directly, and we should drop
    // the returned result.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SendError`.
    fn try_post(&self, message: M, return_channel: oneshot::Sender<M>) -> Result<(), SendError<M>>;

    /// Enqueue a message to be sent on the channel. The caller is expected to monitor
    /// the channel status for failures.
    fn post(&self, message: M) {
        // We ignore errors here because the caller is meant to monitor the channel's
        // status, rather than rely on this function to report errors.
        let _ignore = self.try_post(message, oneshot::channel().0);
    }

    /// Send a message synchronously, returning when the messsage has
    /// been delivered to the remote end of the channel.
    async fn send(&self, message: M) -> Result<(), SendError<M>> {
        let (tx, rx) = oneshot::channel();
        self.try_post(message, tx)?;
        match rx.await {
            // Channel was closed; the message was not delivered.
            Ok(m) => Err(SendError(ChannelError::Closed, m)),

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
pub trait Rx<M: RemoteMessage>: std::fmt::Debug {
    /// Receive the next message from the channel. If the channel returns
    /// an error it is considered broken and should be discarded.
    async fn recv(&mut self) -> Result<M, ChannelError>;

    /// The channel address from which this Rx is receiving.
    fn addr(&self) -> ChannelAddr;
}

#[derive(Debug)]
struct MpscTx<M: RemoteMessage> {
    tx: mpsc::UnboundedSender<M>,
    addr: ChannelAddr,
    status: watch::Receiver<TxStatus>,
}

impl<M: RemoteMessage> MpscTx<M> {
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
    fn try_post(
        &self,
        message: M,
        _return_channel: oneshot::Sender<M>,
    ) -> Result<(), SendError<M>> {
        self.tx
            .send(message)
            .map_err(|mpsc::error::SendError(message)| SendError(ChannelError::Closed, message))
    }

    fn addr(&self) -> ChannelAddr {
        self.addr.clone()
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        &self.status
    }
}

#[derive(Debug)]
struct MpscRx<M: RemoteMessage> {
    rx: mpsc::UnboundedReceiver<M>,
    addr: ChannelAddr,
    // Used to report the status to the Tx side.
    status_sender: watch::Sender<TxStatus>,
}

impl<M: RemoteMessage> MpscRx<M> {
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

/// Types of channel transports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelTransport {
    /// Transport over a TCP connection.
    Tcp,

    /// Transport over a TCP connection with TLS support within Meta
    MetaTls,

    /// Local transports uses an in-process registry and mpsc channels.
    Local,

    /// Sim is a simulated channel for testing.
    Sim(/*simulated transport:*/ Box<ChannelTransport>),

    /// Transport over unix domain socket.
    Unix,
}

impl ChannelTransport {
    /// All known channel transports.
    pub fn all() -> [ChannelTransport; 3] {
        [
            ChannelTransport::Tcp,
            ChannelTransport::Local,
            ChannelTransport::Unix,
            // TODO add MetaTls (T208303369)
            // TODO ChannelTransport::Sim(Box::new(ChannelTransport::Tcp)),
            // TODO ChannelTransport::Sim(Box::new(ChannelTransport::Local)),
        ]
    }
}

/// The type of (TCP) hostnames.
pub type Hostname = String;

/// The type of (TCP) ports.
pub type Port = u16;

/// The type of a channel address, used to multiplex different underlying
/// channel implementations. ChannelAddrs also have a concrete syntax:
/// the address type ("tcp" or "local"), followed by "!", and an address
/// parseable to that type. Addresses without a specified type default to
/// "tcp". For example:
///
/// - `tcp!127.0.0.1:1234` - localhost port 1234 over TCP
/// - `192.168.0.1:1111` - 192.168.0.1 port 1111 over TCP
/// - `local!123` - the (in-process) local port 123
///
/// Both local and TCP ports 0 are reserved to indicate "any available
/// port" when serving.
///
/// ```
/// # use hyperactor::channel::ChannelAddr;
/// let addr: ChannelAddr = "tcp!127.0.0.1:1234".parse().unwrap();
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
    Named
)]
pub enum ChannelAddr {
    /// A socket address used to establish TCP channels. Supports
    /// both  IPv4 and IPv6 address / port pairs.
    Tcp(SocketAddr),

    /// An address to establish TCP channels with TLS support within Meta.
    /// Composed of hostname and port.
    MetaTls(Hostname, Port),

    /// Local addresses are registered in-process and given an integral
    /// index.
    Local(u64),

    /// Sim is a simulated channel for testing.
    Sim(SimAddr),

    /// A unix domain socket address. Supports both absolute path names as
    ///  well as "abstract" names per https://manpages.debian.org/unstable/manpages/unix.7.en.html#Abstract_sockets
    Unix(net::unix::SocketAddr),
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
            ChannelTransport::Tcp => {
                let ip = hostname::get()
                    .ok()
                    .and_then(|hostname| {
                        // TODO: Avoid using DNS directly once we figure out a good extensibility story here
                        hostname.to_str().and_then(|hostname_str| {
                            dns_lookup::lookup_host(hostname_str)
                                .ok()
                                .and_then(|addresses| addresses.first().cloned())
                        })
                    })
                    .unwrap_or_else(|| IpAddr::from_str("::1").unwrap());
                Self::Tcp(SocketAddr::new(ip, 0))
            }
            ChannelTransport::MetaTls => {
                let hostname = hostname::get()
                    .ok()
                    .and_then(|hostname| hostname.to_str().map(|s| s.to_string()))
                    .unwrap_or("unknown_host".to_string());
                Self::MetaTls(hostname, 0)
            }
            ChannelTransport::Local => Self::Local(0),
            ChannelTransport::Sim(transport) => sim::any(*transport),
            // This works because the file will be deleted but we know we have a unique file by this point.
            ChannelTransport::Unix => Self::Unix(net::unix::SocketAddr::from_str("").unwrap()),
        }
    }

    /// The transport used by this address.
    pub fn transport(&self) -> ChannelTransport {
        match self {
            Self::Tcp(_) => ChannelTransport::Tcp,
            Self::MetaTls(_, _) => ChannelTransport::MetaTls,
            Self::Local(_) => ChannelTransport::Local,
            Self::Sim(addr) => ChannelTransport::Sim(Box::new(addr.transport())),
            Self::Unix(_) => ChannelTransport::Unix,
        }
    }
}

impl fmt::Display for ChannelAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp!{}", addr),
            Self::MetaTls(hostname, port) => write!(f, "metatls!{}:{}", hostname, port),
            Self::Local(index) => write!(f, "local!{}", index),
            Self::Sim(sim_addr) => write!(f, "sim!{}", sim_addr),
            Self::Unix(addr) => write!(f, "unix!{}", addr),
        }
    }
}

impl FromStr for ChannelAddr {
    type Err = anyhow::Error;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.split_once('!') {
            Some(("local", rest)) => rest
                .parse::<u64>()
                .map(Self::Local)
                .map_err(anyhow::Error::from),
            Some(("tcp", rest)) => rest
                .parse::<SocketAddr>()
                .map(Self::Tcp)
                .map_err(anyhow::Error::from),
            Some(("metatls", rest)) => net::meta::parse(rest).map_err(|e| e.into()),
            Some(("sim", rest)) => sim::parse(rest).map_err(|e| e.into()),
            Some(("unix", rest)) => Ok(Self::Unix(net::unix::SocketAddr::from_str(rest)?)),
            Some((r#type, _)) => Err(anyhow::anyhow!("no such channel type: {type}")),
            None => addr
                .parse::<SocketAddr>()
                .map(Self::Tcp)
                .map_err(anyhow::Error::from),
        }
    }
}

/// Universal channel transmitter.
#[derive(Debug)]
pub struct ChannelTx<M: RemoteMessage> {
    inner: ChannelTxKind<M>,
}

/// Universal channel transmitter.
#[derive(Debug)]
enum ChannelTxKind<M: RemoteMessage> {
    Local(local::LocalTx<M>),
    Tcp(net::NetTx<M>),
    MetaTls(net::NetTx<M>),
    Unix(net::NetTx<M>),
    Sim(sim::SimTx<M>),
}

#[async_trait]
impl<M: RemoteMessage> Tx<M> for ChannelTx<M> {
    fn try_post(&self, message: M, return_channel: oneshot::Sender<M>) -> Result<(), SendError<M>> {
        match &self.inner {
            ChannelTxKind::Local(tx) => tx.try_post(message, return_channel),
            ChannelTxKind::Tcp(tx) => tx.try_post(message, return_channel),
            ChannelTxKind::MetaTls(tx) => tx.try_post(message, return_channel),
            ChannelTxKind::Sim(tx) => tx.try_post(message, return_channel),
            ChannelTxKind::Unix(tx) => tx.try_post(message, return_channel),
        }
    }

    fn addr(&self) -> ChannelAddr {
        match &self.inner {
            ChannelTxKind::Local(tx) => tx.addr(),
            ChannelTxKind::Tcp(tx) => Tx::<M>::addr(tx),
            ChannelTxKind::MetaTls(tx) => Tx::<M>::addr(tx),
            ChannelTxKind::Sim(tx) => tx.addr(),
            ChannelTxKind::Unix(tx) => Tx::<M>::addr(tx),
        }
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        match &self.inner {
            ChannelTxKind::Local(tx) => tx.status(),
            ChannelTxKind::Tcp(tx) => tx.status(),
            ChannelTxKind::MetaTls(tx) => tx.status(),
            ChannelTxKind::Sim(tx) => tx.status(),
            ChannelTxKind::Unix(tx) => tx.status(),
        }
    }
}

/// Universal channel receiver.
#[derive(Debug)]
pub struct ChannelRx<M: RemoteMessage> {
    inner: ChannelRxKind<M>,
}

/// Universal channel receiver.
#[derive(Debug)]
enum ChannelRxKind<M: RemoteMessage> {
    Local(local::LocalRx<M>),
    Tcp(net::NetRx<M>),
    MetaTls(net::NetRx<M>),
    Unix(net::NetRx<M>),
    Sim(sim::SimRx<M>),
}

#[async_trait]
impl<M: RemoteMessage> Rx<M> for ChannelRx<M> {
    async fn recv(&mut self) -> Result<M, ChannelError> {
        match &mut self.inner {
            ChannelRxKind::Local(rx) => rx.recv().await,
            ChannelRxKind::Tcp(rx) => rx.recv().await,
            ChannelRxKind::MetaTls(rx) => rx.recv().await,
            ChannelRxKind::Sim(rx) => rx.recv().await,
            ChannelRxKind::Unix(rx) => rx.recv().await,
        }
    }

    fn addr(&self) -> ChannelAddr {
        match &self.inner {
            ChannelRxKind::Local(rx) => rx.addr(),
            ChannelRxKind::Tcp(rx) => rx.addr(),
            ChannelRxKind::MetaTls(rx) => rx.addr(),
            ChannelRxKind::Sim(rx) => rx.addr(),
            ChannelRxKind::Unix(rx) => rx.addr(),
        }
    }
}

/// Dial the provided address, returning the corresponding Tx, or error
/// if the channel cannot be established. The underlying connection is
/// dropped whenever the returned Tx is dropped.
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ChannelError`.
pub fn dial<M: RemoteMessage>(addr: ChannelAddr) -> Result<ChannelTx<M>, ChannelError> {
    tracing::debug!(name = "dial", "dialing channel {}", addr);
    let inner = match addr {
        ChannelAddr::Local(port) => ChannelTxKind::Local(local::dial(port)?),
        ChannelAddr::Tcp(addr) => ChannelTxKind::Tcp(net::tcp::dial(addr)),
        ChannelAddr::MetaTls(host, port) => ChannelTxKind::MetaTls(net::meta::dial(host, port)),
        ChannelAddr::Sim(sim_addr) => ChannelTxKind::Sim(sim::dial::<M>(sim_addr)?),
        ChannelAddr::Unix(path) => ChannelTxKind::Unix(net::unix::dial(path)),
    };
    Ok(ChannelTx { inner })
}

/// Serve on the provided channel address. The server is turned down
/// when the returned Rx is dropped.
#[crate::instrument]
pub async fn serve<M: RemoteMessage>(
    addr: ChannelAddr,
) -> Result<(ChannelAddr, ChannelRx<M>), ChannelError> {
    tracing::debug!(name = "serve", "serving channel address {}", addr);
    match addr {
        ChannelAddr::Tcp(addr) => {
            let (addr, rx) = net::tcp::serve::<M>(addr).await?;
            Ok((addr, ChannelRxKind::Tcp(rx)))
        }
        ChannelAddr::MetaTls(hostname, port) => {
            let (addr, rx) = net::meta::serve::<M>(hostname, port).await?;
            Ok((addr, ChannelRxKind::MetaTls(rx)))
        }
        ChannelAddr::Unix(path) => {
            let (addr, rx) = net::unix::serve::<M>(path).await?;
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
    }
    .map(|(addr, inner)| (addr, ChannelRx { inner }))
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
                "[::1]:1234",
                ChannelAddr::Tcp(SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
                    1234,
                )),
            ),
            (
                "tcp!127.0.0.1:8080",
                ChannelAddr::Tcp(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    8080,
                )),
            ),
            ("local!123", ChannelAddr::Local(123)),
            #[cfg(target_os = "linux")]
            (
                "unix!@yolo",
                ChannelAddr::Unix(
                    unix::SocketAddr::from_abstract_name("yolo")
                        .expect("can't make socket from abstract name"),
                ),
            ),
            (
                "unix!/cool/socket-path",
                ChannelAddr::Unix(
                    unix::SocketAddr::from_pathname("/cool/socket-path")
                        .expect("can't make socket from path"),
                ),
            ),
        ];

        let src_ok = vec![
            (
                "[::1]:1235",
                ChannelAddr::Tcp(SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
                    1235,
                )),
            ),
            (
                "tcp!127.0.0.1:8081",
                ChannelAddr::Tcp(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    8081,
                )),
            ),
            ("local!124", ChannelAddr::Local(124)),
        ];

        for (raw, parsed) in cases_ok.clone() {
            assert_eq!(raw.parse::<ChannelAddr>().unwrap(), parsed);
        }

        for (raw, parsed) in cases_ok.iter().zip(src_ok.clone()).map(|(a, _)| {
            (
                format!("sim!{}", a.0),
                ChannelAddr::Sim(SimAddr::new(a.1.clone()).unwrap()),
            )
        }) {
            assert_eq!(raw.parse::<ChannelAddr>().unwrap(), parsed);
        }

        let cases_err = vec![
            ("abcdef..123124", "invalid socket address syntax"),
            ("xxx!foo", "no such channel type: xxx"),
            ("127.0.0.1", "invalid socket address syntax"),
            ("local!abc", "invalid digit found in string"),
        ];

        for (raw, error) in cases_err {
            let Err(err) = raw.parse::<ChannelAddr>() else {
                panic!("expected error parsing: {}", &raw)
            };
            assert_eq!(format!("{}", err), error);
        }
    }

    #[tokio::test]
    async fn test_multiple_connections() {
        for addr in ChannelTransport::all().map(ChannelAddr::any) {
            let (listen_addr, mut rx) = crate::channel::serve::<u64>(addr).await.unwrap();

            let mut sends: JoinSet<()> = JoinSet::new();
            for message in 0u64..100u64 {
                let addr = listen_addr.clone();
                sends.spawn(async move {
                    let tx = dial::<u64>(addr).unwrap();
                    tx.try_post(message, oneshot::channel().0).unwrap();
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

            let (listen_addr, rx) = crate::channel::serve::<u64>(addr).await.unwrap();

            let tx = dial::<u64>(listen_addr).unwrap();
            tx.try_post(123, oneshot::channel().0).unwrap();
            drop(rx);

            // New transmits should fail... but there is buffering, etc.,
            // which can cause the failure to be delayed. We give it
            // a deadline, but it can still technically fail -- the test
            // should be considered a kind of integration test.
            let start = RealClock.now();

            let result = loop {
                let result = tx.try_post(123, oneshot::channel().0);
                if result.is_err() || start.elapsed() > Duration::from_secs(10) {
                    break result;
                }
            };
            assert_matches!(result, Err(SendError(ChannelError::Closed, 123)));
        }
    }

    fn addrs() -> Vec<ChannelAddr> {
        use rand::Rng;
        use rand::distributions::Uniform;

        let rng = rand::thread_rng();
        vec![
            "[::1]:0".parse().unwrap(),
            "local!0".parse().unwrap(),
            #[cfg(target_os = "linux")]
            "unix!".parse().unwrap(),
            #[cfg(target_os = "linux")]
            format!(
                "unix!@{}",
                rng.sample_iter(Uniform::new_inclusive('a', 'z'))
                    .take(10)
                    .collect::<String>()
            )
            .parse()
            .unwrap(),
        ]
    }

    #[tokio::test]
    async fn test_dial_serve() {
        for addr in addrs() {
            let (listen_addr, mut rx) = crate::channel::serve::<i32>(addr).await.unwrap();
            let tx = crate::channel::dial(listen_addr).unwrap();
            tx.try_post(123, oneshot::channel().0).unwrap();
            assert_eq!(rx.recv().await.unwrap(), 123);
        }
    }

    #[tokio::test]
    async fn test_send() {
        let config = crate::config::global::lock();

        // Use temporary config for this test
        let _guard1 = config.override_key(
            crate::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_secs(1),
        );
        let _guard2 = config.override_key(crate::config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);
        for addr in addrs() {
            let (listen_addr, mut rx) = crate::channel::serve::<i32>(addr).await.unwrap();
            let tx = crate::channel::dial(listen_addr).unwrap();
            tx.send(123).await.unwrap();
            assert_eq!(rx.recv().await.unwrap(), 123);

            drop(rx);
            assert_matches!(
                tx.send(123).await.unwrap_err(),
                SendError(ChannelError::Closed, 123)
            );
        }
    }
}
