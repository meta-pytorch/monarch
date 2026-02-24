/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A simple socket channel implementation using a single-stream
//! framing protocol. Each frame is encoded as an 8-byte
//! **big-endian** length prefix (u64), followed by exactly that many
//! bytes of payload.
//!
//! Message frames carry a `serde_multipart::Message` (not raw
//! bincode). In compat mode (current default), this is encoded as a
//! sentinel `u64::MAX` followed by a single bincode payload. Response frames
//! are a bincode-serialized NetRxResponse enum, containing either the acked
//! sequence number, or the Reject value indicating that the server rejected
//! the connection.
//!
//! Message frame (compat/unipart) example:
//! ```text
//! +------------------ len: u64 (BE) ------------------+----------------------- data -----------------------+
//! | \x00\x00\x00\x00\x00\x00\x00\x10                  | \xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF | <bincode bytes> |
//! |                       16                          |           u64::MAX             |                   |
//! +---------------------------------------------------+-----------------------------------------------------+
//! ```
//!
//! Response frame (wire format):
//! ```text
//! +------------------ len: u64 (BE) ------------------+---------------- data ------------------+
//! | \x00\x00\x00\x00\x00\x00\x00\x??                  | <bincode acked sequence num or reject> |
//! +---------------------------------------------------+----------------------------------------+
//! ```
//!
//! I/O is handled by `FrameReader`/`FrameWrite`, which are
//! cancellation-safe and avoid extra copies. Helper fns
//! `serialize_response(NetRxResponse) -> Result<Bytes, bincode::Error>`
//! and `deserialize_response(Bytes) -> Result<NetRxResponse, bincode::Error>`
//! convert to/from the response payload.
//!
//! ### Limits & EOF semantics
//! * **Max frame size:** frames larger than
//!   `config::CODEC_MAX_FRAME_LENGTH` are rejected with
//!   `io::ErrorKind::InvalidData`.
//! * **EOF handling:** `FrameReader::next()` returns `Ok(None)` only
//!   when EOF occurs exactly on a frame boundary. If EOF happens
//!   mid-frame, it returns `Err(io::ErrorKind::UnexpectedEof)`.

use std::fmt;
use std::fmt::Debug;
use std::net::ToSocketAddrs;

use bytes::Bytes;
use enum_as_inner::EnumAsInner;
use serde::de::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::watch;
use tokio::time::Instant;

use super::*;
use crate::RemoteMessage;
use crate::clock::Clock;
use crate::clock::RealClock;

// EnumAsInner generates code that triggers a false positive
// unused_assignments lint on struct variant fields. #[allow] on the
// enum itself doesn't propagate into derive-macro-generated code, so
// the suppression must be at module scope.
#[allow(unused_assignments)]
mod client;
mod framed;
use client::dial;
mod server;
pub use server::ServerHandle;
use server::serve;

pub(crate) trait Stream:
    AsyncRead + AsyncWrite + Unpin + Send + Sync + Debug + 'static
{
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + Debug + 'static> Stream for S {}

/// Link represents a network link through which a stream may be established or accepted.
// TODO: unify this with server connections
#[async_trait]
pub(crate) trait Link: Send + Sync + Debug {
    /// The underlying stream type.
    type Stream: Stream;

    /// The address of the link's destination.
    // Consider embedding the session ID in this address, making it truly persistent.
    fn dest(&self) -> ChannelAddr;

    /// Connect to the destination, returning a connected stream.
    async fn connect(&self) -> Result<Self::Stream, ClientError>;
}

/// Frames are the messages sent between clients and servers over sessions.
#[derive(Debug, Serialize, Deserialize, EnumAsInner, PartialEq)]
enum Frame<M> {
    /// Initialize a session with the given id.
    Init(u64),

    /// Send a message with the provided sequence number.
    Message(u64, M),
}

#[derive(Debug, Serialize, Deserialize, EnumAsInner)]
enum NetRxResponse {
    Ack(u64),
    /// This session is rejected with the given reason. NetTx should stop reconnecting.
    Reject(String),
    /// This channel is closed.
    Closed,
}

fn serialize_response(response: NetRxResponse) -> Result<Bytes, bincode::Error> {
    bincode::serialize(&response).map(|bytes| bytes.into())
}

fn deserialize_response(data: Bytes) -> Result<NetRxResponse, bincode::Error> {
    bincode::deserialize(&data)
}

/// A Tx implemented on top of a Link. The Tx manages the link state,
/// reconnections, etc.
pub(crate) struct NetTx<M: RemoteMessage> {
    sender: mpsc::UnboundedSender<(M, oneshot::Sender<SendError<M>>, Instant)>,
    dest: ChannelAddr,
    status: watch::Receiver<TxStatus>,
}

#[async_trait]
impl<M: RemoteMessage> Tx<M> for NetTx<M> {
    fn addr(&self) -> ChannelAddr {
        self.dest.clone()
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        &self.status
    }

    fn do_post(&self, message: M, return_channel: Option<oneshot::Sender<SendError<M>>>) {
        tracing::trace!(
            name = "post",
            dest = %self.dest,
            "sending message"
        );

        let return_channel = return_channel.unwrap_or_else(|| oneshot::channel().0);
        if let Err(mpsc::error::SendError((message, return_channel, _))) =
            self.sender.send((message, return_channel, RealClock.now()))
        {
            let _ = return_channel.send(SendError {
                error: ChannelError::Closed,
                message,
                reason: None,
            });
        }
    }
}

pub struct NetRx<M: RemoteMessage>(mpsc::Receiver<M>, ChannelAddr, ServerHandle);

#[async_trait]
impl<M: RemoteMessage> Rx<M> for NetRx<M> {
    async fn recv(&mut self) -> Result<M, ChannelError> {
        tracing::trace!(
            name = "recv",
            dest = %self.1,
            "receiving message"
        );
        self.0.recv().await.ok_or(ChannelError::Closed)
    }

    fn addr(&self) -> ChannelAddr {
        self.1.clone()
    }
}

impl<M: RemoteMessage> Drop for NetRx<M> {
    fn drop(&mut self) {
        self.2
            .stop(&format!("NetRx dropped; channel address: {}", self.1));
    }
}

/// Error returned during server operations.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("io: {1}")]
    Io(ChannelAddr, #[source] std::io::Error),
    #[error("listen: {0} {1}")]
    Listen(ChannelAddr, #[source] std::io::Error),
    #[error("resolve: {0} {1}")]
    Resolve(ChannelAddr, #[source] std::io::Error),
    #[error("internal: {0} {1}")]
    Internal(ChannelAddr, #[source] anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("connection to {0} failed: {1}: {2}")]
    Connect(ChannelAddr, std::io::Error, String),
    #[error("unable to resolve address: {0}")]
    Resolve(ChannelAddr),
    #[error("io: {0} {1}")]
    Io(ChannelAddr, std::io::Error),
    #[error("send {0}: serialize: {1}")]
    Serialize(ChannelAddr, bincode::ErrorKind),
    #[error("invalid address: {0}")]
    InvalidAddress(String),
}

/// Tells whether the address is a 'net' address. These currently have different semantics
/// from local transports.
#[allow(dead_code)] // Not used outside tests.
pub(super) fn is_net_addr(addr: &ChannelAddr) -> bool {
    match addr.transport() {
        ChannelTransport::Tcp(_) => true,
        ChannelTransport::MetaTls(_) => true,
        ChannelTransport::Tls => true,
        ChannelTransport::Unix => true,
        _ => false,
    }
}

pub(crate) mod unix {

    use core::str;
    use std::os::unix::net::SocketAddr as StdSocketAddr;
    use std::os::unix::net::UnixDatagram as StdUnixDatagram;
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::os::unix::net::UnixStream as StdUnixStream;

    use rand::Rng;
    use rand::distributions::Alphanumeric;
    use tokio::net::UnixListener;
    use tokio::net::UnixStream;

    use super::*;
    use crate::RemoteMessage;

    #[derive(Debug)]
    pub(crate) struct UnixLink(SocketAddr);

    #[async_trait]
    impl Link for UnixLink {
        type Stream = UnixStream;

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::Unix(self.0.clone())
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            match &self.0 {
                SocketAddr::Bound(sock_addr) => {
                    let std_stream: StdUnixStream = StdUnixStream::connect_addr(sock_addr)
                        .map_err(|err| {
                            ClientError::Connect(
                                self.dest(),
                                err,
                                "cannot connect unix socket".to_string(),
                            )
                        })?;
                    std_stream
                        .set_nonblocking(true)
                        .map_err(|err| ClientError::Io(self.dest(), err))?;
                    UnixStream::from_std(std_stream)
                        .map_err(|err| ClientError::Io(self.dest(), err))
                }
                SocketAddr::Unbound => Err(ClientError::Resolve(self.dest())),
            }
        }
    }

    /// Dial the given unix socket.
    pub fn dial<M: RemoteMessage>(addr: SocketAddr) -> NetTx<M> {
        super::dial(UnixLink(addr))
    }

    /// Listen and serve connections on this socket address.
    /// This function panics if thread-local tokio runtime is not set.
    pub fn serve<M: RemoteMessage>(
        addr: SocketAddr,
    ) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        let caddr = ChannelAddr::Unix(addr.clone());
        let maybe_listener = match &addr {
            SocketAddr::Bound(sock_addr) => StdUnixListener::bind_addr(sock_addr),
            SocketAddr::Unbound => StdUnixDatagram::unbound()
                .and_then(|u| u.local_addr())
                .and_then(|uaddr| StdUnixListener::bind_addr(&uaddr)),
        };
        let std_listener =
            maybe_listener.map_err(|err| ServerError::Listen(ChannelAddr::Unix(addr), err))?;

        std_listener
            .set_nonblocking(true)
            .map_err(|err| ServerError::Listen(caddr.clone(), err))?;
        let local_addr = std_listener
            .local_addr()
            .map_err(|err| ServerError::Resolve(caddr.clone(), err))?;
        let listener: UnixListener = UnixListener::from_std(std_listener)
            .map_err(|err| ServerError::Io(caddr.clone(), err))?;
        super::serve(listener, local_addr.into(), false)
    }

    /// Wrapper around std-lib's unix::SocketAddr that lets us implement equality functions
    #[derive(Clone, Debug)]
    pub enum SocketAddr {
        Bound(Box<StdSocketAddr>),
        Unbound,
    }

    impl PartialOrd for SocketAddr {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for SocketAddr {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.to_string().cmp(&other.to_string())
        }
    }

    impl<'de> Deserialize<'de> for SocketAddr {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            Self::from_str(&s).map_err(D::Error::custom)
        }
    }

    impl Serialize for SocketAddr {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str(String::from(self).as_str())
        }
    }

    impl From<&SocketAddr> for String {
        fn from(value: &SocketAddr) -> Self {
            match value {
                SocketAddr::Bound(addr) => match addr.as_pathname() {
                    Some(path) => path
                        .to_str()
                        .expect("unable to get str for path")
                        .to_string(),
                    #[cfg(target_os = "linux")]
                    _ => match addr.as_abstract_name() {
                        Some(name) => format!("@{}", String::from_utf8_lossy(name)),
                        _ => String::from("(unnamed)"),
                    },
                    #[cfg(not(target_os = "linux"))]
                    _ => String::from("(unnamed)"),
                },
                SocketAddr::Unbound => String::from("(unbound)"),
            }
        }
    }

    impl FromStr for SocketAddr {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
            match s {
                "" => {
                    // TODO: ensure this socket doesn't already exist. 24 bytes of randomness should be good for now but is not perfect.
                    // We can't use annon sockets because those are not valid across processes that aren't in the same process hierarchy aka forked.
                    let random_string = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(24)
                        .map(char::from)
                        .collect::<String>();
                    SocketAddr::from_abstract_name(&random_string)
                }
                // by convention, named sockets are displayed with an '@' prefix
                name if name.starts_with("@") => {
                    SocketAddr::from_abstract_name(name.strip_prefix("@").unwrap())
                }
                path => SocketAddr::from_pathname(path),
            }
        }
    }

    impl Eq for SocketAddr {}
    impl std::hash::Hash for SocketAddr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            String::from(self).hash(state);
        }
    }
    impl PartialEq for SocketAddr {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Self::Bound(saddr), Self::Bound(oaddr)) => {
                    if saddr.is_unnamed() || oaddr.is_unnamed() {
                        return false;
                    }

                    #[cfg(target_os = "linux")]
                    {
                        saddr.as_pathname() == oaddr.as_pathname()
                            && saddr.as_abstract_name() == oaddr.as_abstract_name()
                    }
                    #[cfg(not(target_os = "linux"))]
                    {
                        // On non-Linux platforms, only compare pathname since no abstract names
                        saddr.as_pathname() == oaddr.as_pathname()
                    }
                }
                (Self::Unbound, _) | (_, Self::Unbound) => false,
            }
        }
    }

    impl fmt::Display for SocketAddr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Bound(addr) => match addr.as_pathname() {
                    Some(path) => {
                        write!(f, "{}", path.to_string_lossy())
                    }
                    #[cfg(target_os = "linux")]
                    _ => match addr.as_abstract_name() {
                        Some(name) => {
                            if name.starts_with(b"@") {
                                return write!(f, "{}", String::from_utf8_lossy(name));
                            }
                            write!(f, "@{}", String::from_utf8_lossy(name))
                        }
                        _ => write!(f, "(unnamed)"),
                    },
                    #[cfg(not(target_os = "linux"))]
                    _ => write!(f, "(unnamed)"),
                },
                Self::Unbound => write!(f, "(unbound)"),
            }
        }
    }

    impl SocketAddr {
        /// Wraps the stdlib socket address for use with this module
        pub fn new(addr: StdSocketAddr) -> Self {
            Self::Bound(Box::new(addr))
        }

        /// Abstract socket names start with a "@" by convention when displayed. If there is an
        /// "@" prefix, it will be stripped from the name before used.
        #[cfg(target_os = "linux")]
        pub fn from_abstract_name(name: &str) -> anyhow::Result<Self> {
            Ok(Self::new(StdSocketAddr::from_abstract_name(
                name.strip_prefix("@").unwrap_or(name),
            )?))
        }

        #[cfg(not(target_os = "linux"))]
        pub fn from_abstract_name(name: &str) -> anyhow::Result<Self> {
            // On non-Linux platforms, convert abstract names to filesystem paths
            let name = name.strip_prefix("@").unwrap_or(name);
            let path = Self::abstract_to_filesystem_path(name);
            Self::from_pathname(&path.to_string_lossy())
        }

        #[cfg(not(target_os = "linux"))]
        fn abstract_to_filesystem_path(abstract_name: &str) -> std::path::PathBuf {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hash;
            use std::hash::Hasher;

            // Generate a stable hash of the abstract name for deterministic paths
            let mut hasher = DefaultHasher::new();
            abstract_name.hash(&mut hasher);
            let hash = hasher.finish();

            // Include process ID to prevent inter-process conflicts
            let process_id = std::process::id();

            // TODO: we just leak these. Should we do something smarter?
            std::path::PathBuf::from(format!("/tmp/hyperactor_{}_{:x}", process_id, hash))
        }

        /// Pathnames may be absolute or relative.
        pub fn from_pathname(name: &str) -> anyhow::Result<Self> {
            Ok(Self::new(StdSocketAddr::from_pathname(name)?))
        }
    }

    impl TryFrom<SocketAddr> for StdSocketAddr {
        type Error = anyhow::Error;

        fn try_from(value: SocketAddr) -> Result<Self, Self::Error> {
            match value {
                SocketAddr::Bound(addr) => Ok(*addr),
                SocketAddr::Unbound => Err(anyhow::anyhow!(
                    "std::os::unix::SocketAddr must be a bound address"
                )),
            }
        }
    }
}

pub(crate) mod tcp {
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    use super::*;
    use crate::RemoteMessage;

    #[derive(Debug)]
    pub(crate) struct TcpLink(SocketAddr);

    #[async_trait]
    impl Link for TcpLink {
        type Stream = TcpStream;

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::Tcp(self.0)
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            let stream = TcpStream::connect(&self.0).await.map_err(|err| {
                ClientError::Connect(self.dest(), err, "cannot connect TCP socket".to_string())
            })?;
            // Always disable Nagle algorithm, so it doesn't hurt the latency of small messages.
            stream.set_nodelay(true).map_err(|err| {
                ClientError::Connect(
                    self.dest(),
                    err,
                    "cannot disables Nagle algorithm".to_string(),
                )
            })?;
            Ok(stream)
        }
    }

    pub fn dial<M: RemoteMessage>(addr: SocketAddr) -> NetTx<M> {
        super::dial(TcpLink(addr))
    }

    /// Serve the given address. Supports both v4 and v6 address. If port 0 is provided as
    /// dynamic port will be resolved and is available on the returned ServerHandle.
    pub fn serve<M: RemoteMessage>(
        addr: SocketAddr,
    ) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        // Construct our own std TcpListener to avoid having to await, making this function
        // non-async.
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|err| ServerError::Listen(ChannelAddr::Tcp(addr), err))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| ServerError::Listen(ChannelAddr::Tcp(addr), e))?;
        let listener = TcpListener::from_std(std_listener)
            .map_err(|e| ServerError::Listen(ChannelAddr::Tcp(addr), e))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| ServerError::Resolve(ChannelAddr::Tcp(addr), err))?;
        super::serve(listener, ChannelAddr::Tcp(local_addr), false)
    }
}

// TODO: Try to simplify the TLS creation T208304433
pub(crate) mod meta {
    use std::io;
    use std::path::PathBuf;
    use std::sync::Arc;

    use anyhow::Result;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::TlsConnector;

    use super::*;
    use crate::RemoteMessage;
    use crate::config::Pem;
    use crate::config::PemBundle;

    const THRIFT_TLS_SRV_CA_PATH_ENV: &str = "THRIFT_TLS_SRV_CA_PATH";
    const DEFAULT_SRV_CA_PATH: &str = "/var/facebook/rootcanal/ca.pem";
    const THRIFT_TLS_CL_CERT_PATH_ENV: &str = "THRIFT_TLS_CL_CERT_PATH";
    const THRIFT_TLS_CL_KEY_PATH_ENV: &str = "THRIFT_TLS_CL_KEY_PATH";
    const DEFAULT_SERVER_PEM_PATH: &str = "/var/facebook/x509_identities/server.pem";

    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ChannelError`.
    pub(crate) fn parse(addr_string: &str) -> Result<ChannelAddr, ChannelError> {
        // Try to parse as a socket address first
        if let Ok(socket_addr) = addr_string.parse::<SocketAddr>() {
            return Ok(ChannelAddr::MetaTls(TlsAddr::Socket(socket_addr)));
        }

        // Otherwise, parse as hostname:port
        // use right split to allow for ipv6 addresses where ":" is expected.
        let parts = addr_string.rsplit_once(":");
        match parts {
            Some((hostname, port_str)) => {
                let Ok(port) = port_str.parse() else {
                    return Err(ChannelError::InvalidAddress(addr_string.to_string()));
                };
                Ok(ChannelAddr::MetaTls(TlsAddr::Host {
                    hostname: hostname.to_string(),
                    port,
                }))
            }
            _ => Err(ChannelError::InvalidAddress(addr_string.to_string())),
        }
    }

    /// Construct a PemBundle for server operations from Meta-specific paths.
    /// Server cert and key come from the same file (server.pem).
    fn get_server_pem_bundle() -> PemBundle {
        let ca_path = std::env::var_os(THRIFT_TLS_SRV_CA_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SRV_CA_PATH));
        let server_pem_path = PathBuf::from(DEFAULT_SERVER_PEM_PATH);
        PemBundle {
            ca: Pem::File(ca_path),
            cert: Pem::File(server_pem_path.clone()),
            key: Pem::File(server_pem_path),
        }
    }

    /// Construct a PemBundle for client operations from Meta-specific env vars.
    /// Returns None if client cert/key env vars are not set.
    fn get_client_pem_bundle() -> Option<PemBundle> {
        let cert_path = std::env::var_os(THRIFT_TLS_CL_CERT_PATH_ENV)?;
        let key_path = std::env::var_os(THRIFT_TLS_CL_KEY_PATH_ENV)?;
        let ca_path = std::env::var_os(THRIFT_TLS_SRV_CA_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SRV_CA_PATH));
        Some(PemBundle {
            ca: Pem::File(ca_path),
            cert: Pem::File(PathBuf::from(cert_path)),
            key: Pem::File(PathBuf::from(key_path)),
        })
    }

    /// Creates a TLS acceptor by looking for necessary certs and keys in a Meta server environment.
    pub(crate) fn tls_acceptor(enforce_client_tls: bool) -> Result<TlsAcceptor> {
        let bundle = get_server_pem_bundle();
        tls::tls_acceptor_from_bundle(&bundle, enforce_client_tls)
    }

    /// Creates a TLS connector by looking for necessary certs and keys in a Meta server environment.
    /// Supports optional client authentication (unlike the tls module which always requires it).
    fn tls_connector() -> Result<TlsConnector> {
        let ca_path = std::env::var_os(THRIFT_TLS_SRV_CA_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SRV_CA_PATH));
        let ca_pem = Pem::File(ca_path);
        let root_store = tls::build_root_store(&ca_pem)?;

        // If client certs are available, use mutual TLS; otherwise, no client auth
        let config = rustls::ClientConfig::builder().with_root_certificates(Arc::new(root_store));

        let config = if let Some(bundle) = get_client_pem_bundle() {
            let certs = tls::load_certs(&bundle.cert)?;
            let key = tls::load_key(&bundle.key)?;
            config
                .with_client_auth_cert(certs, key)
                .map_err(|e| anyhow::anyhow!("load client certs: {}", e))?
        } else {
            config.with_no_client_auth()
        };

        Ok(TlsConnector::from(Arc::new(config)))
    }

    pub fn dial<M: RemoteMessage>(addr: TlsAddr) -> Result<NetTx<M>, ClientError> {
        let connector = tls_connector().map_err(|e| {
            ClientError::Connect(
                ChannelAddr::MetaTls(addr.clone()),
                io::Error::other(e.to_string()),
                "failed to create TLS connector".to_string(),
            )
        })?;
        tls::dial_with_connector(addr, connector, tls::TlsAddrType::MetaTls)
    }

    /// Serve the given address. If port 0 is provided in a Host address,
    /// a dynamic port will be resolved and is available in the returned ChannelAddr.
    /// For Host addresses, binds to all resolved socket addresses.
    pub fn serve<M: RemoteMessage>(addr: TlsAddr) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        tls::serve_with_acceptor(addr, tls::TlsAddrType::MetaTls)
    }
}

/// TLS transport module using configurable certificates via hyperactor config attributes.
pub(crate) mod tls {
    use std::io;
    use std::io::BufReader;
    use std::sync::Arc;

    use anyhow::Context;
    use anyhow::Result;
    use rustls::RootCertStore;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::ServerName;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::TlsConnector;
    use tokio_rustls::client::TlsStream;

    use super::*;
    use crate::RemoteMessage;
    use crate::channel::TlsAddr;
    use crate::config::Pem;
    use crate::config::PemBundle;
    use crate::config::TLS_CA;
    use crate::config::TLS_CERT;
    use crate::config::TLS_KEY;

    /// Distinguishes between Tls and MetaTls for address construction.
    #[derive(Debug, Clone, Copy)]
    pub(super) enum TlsAddrType {
        Tls,
        MetaTls,
    }

    /// Parse an address string into a TlsAddr.
    #[allow(clippy::result_large_err)]
    pub(crate) fn parse(addr_string: &str) -> Result<ChannelAddr, ChannelError> {
        // Try to parse as a socket address first
        if let Ok(socket_addr) = addr_string.parse::<SocketAddr>() {
            return Ok(ChannelAddr::Tls(TlsAddr::Socket(socket_addr)));
        }

        // Otherwise, parse as hostname:port
        // use right split to allow for ipv6 addresses where ":" is expected.
        let parts = addr_string.rsplit_once(":");
        match parts {
            Some((hostname, port_str)) => {
                let Ok(port) = port_str.parse() else {
                    return Err(ChannelError::InvalidAddress(addr_string.to_string()));
                };
                Ok(ChannelAddr::Tls(TlsAddr::Host {
                    hostname: hostname.to_string(),
                    port,
                }))
            }
            _ => Err(ChannelError::InvalidAddress(addr_string.to_string())),
        }
    }

    /// Load certificates from a Pem value.
    pub(super) fn load_certs(pem: &Pem) -> Result<Vec<CertificateDer<'static>>> {
        let mut reader = BufReader::new(pem.reader()?);
        let certs = rustls_pemfile::certs(&mut reader)?
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        Ok(certs)
    }

    /// Load a private key from a Pem value.
    pub(super) fn load_key(pem: &Pem) -> Result<PrivateKeyDer<'static>> {
        let mut reader = BufReader::new(pem.reader()?);
        loop {
            break match rustls_pemfile::read_one(&mut reader)? {
                Some(rustls_pemfile::Item::RSAKey(key)) => Ok(PrivateKeyDer::try_from(key)
                    .map_err(|_| anyhow::anyhow!("invalid RSA private key format"))?),
                Some(rustls_pemfile::Item::PKCS8Key(key)) => Ok(PrivateKeyDer::try_from(key)
                    .map_err(|_| anyhow::anyhow!("invalid PKCS8 private key format"))?),
                Some(rustls_pemfile::Item::ECKey(key)) => Ok(PrivateKeyDer::try_from(key)
                    .map_err(|_| anyhow::anyhow!("invalid EC private key format"))?),
                Some(_) => continue,
                None => anyhow::bail!("no private key found in TLS key file"),
            };
        }
    }

    /// Build root certificate store from the CA pem.
    pub(super) fn build_root_store(ca_pem: &Pem) -> Result<RootCertStore> {
        let mut root_store = RootCertStore::empty();
        let certs = load_certs(ca_pem)?;
        for cert in certs {
            root_store.add(cert).context("add CA cert")?;
        }
        Ok(root_store)
    }

    /// Get the PEM bundle from configuration.
    fn get_pem_bundle() -> PemBundle {
        PemBundle {
            ca: hyperactor_config::global::get_cloned(TLS_CA),
            cert: hyperactor_config::global::get_cloned(TLS_CERT),
            key: hyperactor_config::global::get_cloned(TLS_KEY),
        }
    }

    /// Creates a TLS acceptor using certificates from the provided PEM bundle.
    /// If `enforce_client_tls` is true, requires client certificates for mutual TLS.
    pub(super) fn tls_acceptor_from_bundle(
        bundle: &PemBundle,
        enforce_client_tls: bool,
    ) -> Result<TlsAcceptor> {
        let certs = load_certs(&bundle.cert).context("load TLS certificate")?;
        let key = load_key(&bundle.key).context("load TLS key")?;
        let root_store = build_root_store(&bundle.ca).context("build root cert store")?;

        let config = rustls::ServerConfig::builder();
        let config = if enforce_client_tls {
            // Build server config with mutual TLS (require client certs)
            let client_verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                    .build()
                    .map_err(|e| anyhow::anyhow!("failed to build client verifier: {}", e))?;
            config.with_client_cert_verifier(client_verifier)
        } else {
            config.with_no_client_auth()
        }
        .with_single_cert(certs, key)?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    /// Creates a TLS acceptor using certificates from config (always enforces mutual TLS).
    pub(crate) fn tls_acceptor() -> Result<TlsAcceptor> {
        tls_acceptor_from_bundle(&get_pem_bundle(), true)
    }

    /// Creates a TLS connector using certificates from the provided PEM bundle.
    pub(super) fn tls_connector_from_bundle(bundle: &PemBundle) -> Result<TlsConnector> {
        let certs = load_certs(&bundle.cert).context("load TLS certificate")?;
        let key = load_key(&bundle.key).context("load TLS key")?;
        let root_store = build_root_store(&bundle.ca).context("build root cert store")?;

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(root_store))
            .with_client_auth_cert(certs, key)
            .context("configure client auth")?;

        Ok(TlsConnector::from(Arc::new(config)))
    }

    /// Creates a TLS connector using certificates from config.
    fn tls_connector() -> Result<TlsConnector> {
        tls_connector_from_bundle(&get_pem_bundle())
    }

    /// Shared TLS link implementation used by both tls and metatls transports.
    pub(super) struct TlsLink {
        hostname: Hostname,
        port: Port,
        connector: TlsConnector,
        addr_type: TlsAddrType,
    }

    impl std::fmt::Debug for TlsLink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TlsLink")
                .field("hostname", &self.hostname)
                .field("port", &self.port)
                .field("addr_type", &self.addr_type)
                .finish()
        }
    }

    impl TlsLink {
        pub fn new(
            hostname: Hostname,
            port: Port,
            connector: TlsConnector,
            addr_type: TlsAddrType,
        ) -> Self {
            Self {
                hostname,
                port,
                connector,
                addr_type,
            }
        }
    }

    #[async_trait]
    impl Link for TlsLink {
        type Stream = TlsStream<TcpStream>;

        fn dest(&self) -> ChannelAddr {
            let addr = TlsAddr::Host {
                hostname: self.hostname.clone(),
                port: self.port,
            };
            match self.addr_type {
                TlsAddrType::Tls => ChannelAddr::Tls(addr),
                TlsAddrType::MetaTls => ChannelAddr::MetaTls(addr),
            }
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            let mut addrs = (self.hostname.as_ref(), self.port)
                .to_socket_addrs()
                .map_err(|_| ClientError::Resolve(self.dest()))?;
            let addr = addrs.next().ok_or(ClientError::Resolve(self.dest()))?;
            let stream = TcpStream::connect(&addr).await.map_err(|err| {
                ClientError::Connect(self.dest(), err, format!("cannot connect to {}", addr))
            })?;
            // Always disable Nagle algorithm, so it doesn't hurt the latency of small messages.
            stream.set_nodelay(true).map_err(|err| {
                ClientError::Connect(
                    self.dest(),
                    err,
                    "cannot disable Nagle algorithm".to_string(),
                )
            })?;
            let server_name = ServerName::try_from(self.hostname.clone()).map_err(|e| {
                ClientError::Connect(
                    self.dest(),
                    io::Error::other(e.to_string()),
                    "invalid server name".to_string(),
                )
            })?;
            self.connector
                .connect(server_name.clone(), stream)
                .await
                .map_err(|err| {
                    ClientError::Connect(
                        self.dest(),
                        err,
                        format!("cannot establish TLS connection to {:?}", server_name),
                    )
                })
        }
    }

    /// Shared dial helper that takes a pre-built connector.
    pub(super) fn dial_with_connector<M: RemoteMessage>(
        addr: TlsAddr,
        connector: TlsConnector,
        addr_type: TlsAddrType,
    ) -> Result<NetTx<M>, ClientError> {
        match addr {
            TlsAddr::Host { hostname, port } => Ok(super::dial(TlsLink::new(
                hostname, port, connector, addr_type,
            ))),
            TlsAddr::Socket(_) => Err(ClientError::InvalidAddress(
                "TLS clients require hostname/port for host identity, not socket addresses"
                    .to_string(),
            )),
        }
    }

    /// Shared serve helper for TLS transports.
    pub(super) fn serve_with_acceptor<M: RemoteMessage>(
        addr: TlsAddr,
        addr_type: TlsAddrType,
    ) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        match addr {
            TlsAddr::Host { hostname, port } => {
                // Resolve all addresses for the hostname
                let make_channel_addr = |h: &str, p: Port| match addr_type {
                    TlsAddrType::Tls => ChannelAddr::Tls(TlsAddr::Host {
                        hostname: h.to_string(),
                        port: p,
                    }),
                    TlsAddrType::MetaTls => ChannelAddr::MetaTls(TlsAddr::Host {
                        hostname: h.to_string(),
                        port: p,
                    }),
                };

                let addrs: Vec<SocketAddr> = (hostname.as_ref(), port)
                    .to_socket_addrs()
                    .map_err(|err| ServerError::Resolve(make_channel_addr(&hostname, port), err))?
                    .collect();

                if addrs.is_empty() {
                    return Err(ServerError::Resolve(
                        make_channel_addr(&hostname, port),
                        io::Error::other("no available socket addr"),
                    ));
                }

                let channel_addr = make_channel_addr(&hostname, port);

                // Bind to all resolved addresses
                let std_listener = std::net::TcpListener::bind(&addrs[..])
                    .map_err(|err| ServerError::Listen(channel_addr.clone(), err))?;
                std_listener
                    .set_nonblocking(true)
                    .map_err(|e| ServerError::Listen(channel_addr.clone(), e))?;
                let listener = TcpListener::from_std(std_listener)
                    .map_err(|e| ServerError::Listen(channel_addr.clone(), e))?;

                let local_addr = listener
                    .local_addr()
                    .map_err(|err| ServerError::Resolve(channel_addr, err))?;
                super::serve(
                    listener,
                    make_channel_addr(&hostname, local_addr.port()),
                    true,
                )
            }
            TlsAddr::Socket(socket_addr) => {
                let make_socket_addr = |sa: SocketAddr| match addr_type {
                    TlsAddrType::Tls => ChannelAddr::Tls(TlsAddr::Socket(sa)),
                    TlsAddrType::MetaTls => ChannelAddr::MetaTls(TlsAddr::Socket(sa)),
                };

                let channel_addr = make_socket_addr(socket_addr);

                // Bind directly to the socket address
                let std_listener = std::net::TcpListener::bind(socket_addr)
                    .map_err(|err| ServerError::Listen(channel_addr.clone(), err))?;
                std_listener
                    .set_nonblocking(true)
                    .map_err(|e| ServerError::Listen(channel_addr.clone(), e))?;
                let listener = TcpListener::from_std(std_listener)
                    .map_err(|e| ServerError::Listen(channel_addr.clone(), e))?;

                let local_addr = listener
                    .local_addr()
                    .map_err(|err| ServerError::Resolve(channel_addr, err))?;
                super::serve(listener, make_socket_addr(local_addr), true)
            }
        }
    }

    pub fn dial<M: RemoteMessage>(addr: TlsAddr) -> Result<NetTx<M>, ClientError> {
        let connector = tls_connector().map_err(|e| {
            ClientError::Connect(
                ChannelAddr::Tls(addr.clone()),
                io::Error::other(e.to_string()),
                "failed to create TLS connector".to_string(),
            )
        })?;
        dial_with_connector(addr, connector, TlsAddrType::Tls)
    }

    /// Serve the given address. If port 0 is provided in a Host address,
    /// a dynamic port will be resolved and is available in the returned ChannelAddr.
    /// For Host addresses, binds to all resolved socket addresses.
    pub fn serve<M: RemoteMessage>(addr: TlsAddr) -> Result<(ChannelAddr, NetRx<M>), ServerError> {
        serve_with_acceptor(addr, TlsAddrType::Tls)
    }

    #[cfg(test)]
    mod tests {
        use timed_test::async_timed_test;

        use super::*;
        use crate::channel::Rx;
        use crate::config::Pem;
        use crate::config::TLS_CA;
        use crate::config::TLS_CERT;
        use crate::config::TLS_KEY;

        // Dummy test certificates generated with openssl for testing only.
        // These certificates include Subject Alternative Names (SAN) for localhost, 127.0.0.1, and ::1
        // CA certificate
        const TEST_CA_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUaGNmboiIosG+8Up0vgDr/+cg+2IwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHVGVzdCBDQTAeFw0yNjAxMjgxNzA4MzlaFw0yNzAxMjgx
NzA4MzlaMBIxEDAOBgNVBAMMB1Rlc3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQC9RBoMYXCajklswt8Vi1JI1lEYzic0WNOmz45vG/7H6jTWkgL3
K5Ri+Seg3MobDNc48YHWXYm4hP9wCzkx8ih3ntT5XiY1My/G3jLUuoIEE9pF/BoJ
YQwZVoPNFhA9WhXNRsINf1cXFf8NzRfXpxBfKWtQJxYXU4JiDBQ6rLnQQABo8JmQ
vYFhJbBaYip5jTSiVNn7mB1zNr5jsVxuoSF53Pb7xQ76bwBdOq4zd6PSxL5/lr4G
cHSoxwZQdZMG7PL6hbxDQ2S2YI2lYVET1zwc2WPKCfjbEXBC/jzx828CInQtuksk
18gJt6xHkTFEA8CSA29GM3lejnwYWf51xyyBAgMBAAGjUzBRMB0GA1UdDgQWBBRX
cbxSZ70NsUkAS3Hhy6irugywJDAfBgNVHSMEGDAWgBRXcbxSZ70NsUkAS3Hhy6ir
ugywJDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQA7aAFfyW67
Z+uGSVYhpsT/uH/3Z3nr7X1smTz5CGEfq2czEcTC7gbYI2l8GZ47GPfnAvHTBZVm
V/XncBCsj7/thOh2jYEHFyCbPckoaSCRyCOnK7LPUlr4HN5uP9EFe45qBLCJDEoY
GTTw7MtzwdovfjchNfKQCTtkBJCXQ95WLCf6UOh02Sn28UTlgfXzF0X0FrcWqWa3
uJZd4XOo4O6hKKlHaBaQPiEr++1xc3SWPV7jZHbckI/vKBnDdEZ9JQX5fFZuypUI
sgomYHxvxrU2hWx+7k53CRdjfaIvT9Ie44z9sSdsU/+blw2S8f/ZTmuECoIAAXYO
0qpzlxZMdr7T
-----END CERTIFICATE-----"#;

        // Server certificate (signed by CA) with SAN for localhost, 127.0.0.1, ::1
        const TEST_SERVER_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDJDCCAgygAwIBAgIUaz66DsWaH5ZXM4hCFnbVbMsyN1cwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHVGVzdCBDQTAeFw0yNjAxMjgxNzA4MzlaFw0yNzAxMjgx
NzA4MzlaMBQxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAKCbp++qNyTn5LOsV0h9gLKJALBcjg2A14I3804N9UyDhPW2
QKQ2W424u2P1MfKrw/2C+CErGlrADlnco2RQVDAarAIuGdFvBOt5UezqOS7Mk4OS
9MlS7NZnMbc37KuM9UIG5ScJjXR/Z5z9dxeR0I9y3n0Ix6khbV7tOSHobiweI0FI
8LftBS+CQnXr6vbWPcHcW6Z0FHUv7IWhqMWmv9PlZRGe9Y6VzXrRp0PBnZMOnAYf
aMQUwYRswWdm9j9Z1sMdTJ14G+KVmO3Vj6XI6Sm9uIcYhlwG/kORwogJFWlVuP9o
rloFRCjyHJ1d7GZqqnRyHHDDCBms8ed+3YfEYQECAwEAAaNwMG4wLAYDVR0RBCUw
I4IJbG9jYWxob3N0hwR/AAABhxAAAAAAAAAAAAAAAAAAAAABMB0GA1UdDgQWBBQl
J4vxUoCzqqeTwQAiLqE8wYezKzAfBgNVHSMEGDAWgBRXcbxSZ70NsUkAS3Hhy6ir
ugywJDANBgkqhkiG9w0BAQsFAAOCAQEAnXHIBDQ4AHAMV71piTOuI41ShASQed6L
bi7XUMZgZDslLkfU1vnP3BlwpliraBsAytSYQC6kbytOuz1uQ4K7yLb2tAAmUgEO
EdIVt9SXr5tCcIPeLmInF0pysPqjZO8n7vtJyd9gryKqdhm1uzA7WQWq/Az8a9Sk
uW2J6Oc5p6P7Mf3/ixqXzvGRo8rzu0CUJOJ67UTE/HhbJuplQ5dep5CEEOAIsAtH
zn9O4rW92ueBkoBJM++YILS1vQ7jKc2N3RNrnHm7FeootBrtR9mBi0TH97K73ZPZ
2Cdhnym0CsCJggrllFGH32cYo7+K2PO7/4oj5XbBCSWcssicvd8ovg==
-----END CERTIFICATE-----"#;

        // Server private key
        // test-only embedded key -- though it might expire at some point:
        // @lint-ignore PRIVATEKEY insecure-private-key-storage
        const TEST_SERVER_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQCgm6fvqjck5+Sz
rFdIfYCyiQCwXI4NgNeCN/NODfVMg4T1tkCkNluNuLtj9THyq8P9gvghKxpawA5Z
3KNkUFQwGqwCLhnRbwTreVHs6jkuzJODkvTJUuzWZzG3N+yrjPVCBuUnCY10f2ec
/XcXkdCPct59CMepIW1e7Tkh6G4sHiNBSPC37QUvgkJ16+r21j3B3FumdBR1L+yF
oajFpr/T5WURnvWOlc160adDwZ2TDpwGH2jEFMGEbMFnZvY/WdbDHUydeBvilZjt
1Y+lyOkpvbiHGIZcBv5DkcKICRVpVbj/aK5aBUQo8hydXexmaqp0chxwwwgZrPHn
ft2HxGEBAgMBAAECgf8G5qlQov+7ljs9fSpC8yGUik59RXzVF7Qq5DyQHglsQDp2
VF5yr+M/M7DZmq+KvdauDfKbej6np5j2Q4TByrHTX1IExfZWCW8srwnWJDpQyHmO
LcJW5DlI/SYluUFyHZxsOd+ezcpGNzM8i6eSW7GaeFUXCkmJ+uW4LnlF+7bALnnd
D6sak/58EsII+IJyd4lFn+voszlPn3CZGR0jkp21rvpaKgrMIsKVWWQO/sLDU5pr
VbpBThcLU5gRcnQouQX12e2VTCIlFu75WTsJ8V/KnEaOZUVlU/B/Bs+WQF3U+/Jo
eX4N+D6OsEcNQjERAFyWujxsl1WpD4uSsbFMN0ECgYEA2b7AdL+oKPQHku2KcBhr
Zw8K4tMDlr2VPPNwZcBTLo+O71vv/xXjMcXrXmowzkgEQckUmt1VB46riyydhwdP
/n9ciWcz0Va/nwHR6Y9F9unBiyUBP7PRhRyjQyRZZRGDSJvP+Xmc5UJFpRr07VLU
nfgMXDj37vXzKDpfhdEB2nkCgYEAvNMfA8P8w3+6246x5YHflvTkPdw+2oyge+LD
mphB/w7SF8mlyNGloj3+KBZmd9SkvT57wCvO96Y9/n+mBAVisRggc0hK4ymOVYhb
+im/JvqGQMbVeg6iCOHnWdaZf9tL8uVsegQy3kVTN7vAa+CMFgX1dt65cGBX6XkB
44pYmMkCgYALhbiRdQLlB+TOtZs5y1EDpxwgXKI3+9hF3Wv5NnAwapBZwje0++eF
3r9Rw7TJda4j/QwGFehF+hrBxp6fYpetE/hFnRx0225Qb7w368j8A+ql/lNOl6li
rd1F1EqWupKD6RrcTL8sspEU55RGaretlE6zIqCcGI/BdTVQ03qRoQKBgHDC3zWf
d7XD9HGjQGdfbIe4jQjIGxzmd/wjik4q+NZ5IkukVwWa9P/zZ3DHF8Ad05dT1hEH
2FwaAdGWpyyljq9VSiOuG1KXAXHgsZSuE4ISf9P1KYzvaiJFzaPfvOEWs79E9MfU
9A+6dJzG2X1SpjWMr26iSTlrv3QkmFUqzAfJAoGASBkn4wls+oC5rv/Mch43pBv5
UmKru4ltnEHJZdbSi2DJ+AnDLD222JCasb1VT1tm2XgW6DBqrdVRPPP6GOlB0MHU
+3ULtZxAczt7I+ST2bo0/DV2Hse89Cm63w4wLOiVZs7+1wrAzJZLokWF7Q5gesra
u19txmtkiMEH+aNmekk=
-----END PRIVATE KEY-----"#;

        #[async_timed_test(timeout_secs = 30)]
        async fn test_tls_basic() {
            // Ensure ring is installed as the default crypto provider
            // (no-op if already installed, e.g. under Buck with native-tls).
            let _ = rustls::crypto::ring::default_provider().install_default();

            // Set up TLS config using the standard override pattern
            let config = hyperactor_config::global::lock();
            let _guard_cert =
                config.override_key(TLS_CERT, Pem::Value(TEST_SERVER_CERT.as_bytes().to_vec()));
            let _guard_key =
                config.override_key(TLS_KEY, Pem::Value(TEST_SERVER_KEY.as_bytes().to_vec()));
            let _guard_ca =
                config.override_key(TLS_CA, Pem::Value(TEST_CA_CERT.as_bytes().to_vec()));

            // Create a TLS server bound to localhost with dynamic port
            let addr = TlsAddr::Host {
                hostname: "localhost".to_string(),
                port: 0,
            };

            let (local_addr, mut rx) = serve::<u64>(addr).expect("failed to serve");

            // Dial the server
            let tx = dial::<u64>(match &local_addr {
                ChannelAddr::Tls(addr) => addr.clone(),
                _ => panic!("unexpected address type"),
            })
            .expect("failed to dial");

            // Send a message
            tx.post(42u64);

            // Receive the message
            let received = rx.recv().await.expect("failed to receive");
            assert_eq!(received, 42u64);
        }

        #[async_timed_test(timeout_secs = 30)]
        async fn test_tls_multiple_messages() {
            let _ = rustls::crypto::ring::default_provider().install_default();

            // Set up TLS config using the standard override pattern
            let config = hyperactor_config::global::lock();
            let _guard_cert =
                config.override_key(TLS_CERT, Pem::Value(TEST_SERVER_CERT.as_bytes().to_vec()));
            let _guard_key =
                config.override_key(TLS_KEY, Pem::Value(TEST_SERVER_KEY.as_bytes().to_vec()));
            let _guard_ca =
                config.override_key(TLS_CA, Pem::Value(TEST_CA_CERT.as_bytes().to_vec()));

            let addr = TlsAddr::Host {
                hostname: "localhost".to_string(),
                port: 0,
            };

            let (local_addr, mut rx) = serve::<String>(addr).expect("failed to serve");
            let tx = dial::<String>(match &local_addr {
                ChannelAddr::Tls(addr) => addr.clone(),
                _ => panic!("unexpected address type"),
            })
            .expect("failed to dial");

            // Send multiple messages
            for i in 0..10 {
                tx.post(format!("message {}", i));
            }

            // Receive all messages
            for i in 0..10 {
                let received = rx.recv().await.expect("failed to receive");
                assert_eq!(received, format!("message {}", i));
            }
        }

        #[test]
        fn test_tls_parse_hostname_port() {
            let addr = parse("localhost:8080").expect("failed to parse");
            assert!(matches!(
                addr,
                ChannelAddr::Tls(TlsAddr::Host { hostname, port })
                    if hostname == "localhost" && port == 8080
            ));
        }

        #[test]
        fn test_tls_parse_socket_addr() {
            let addr = parse("127.0.0.1:8080").expect("failed to parse");
            assert!(matches!(addr, ChannelAddr::Tls(TlsAddr::Socket(_))));
        }

        #[test]
        fn test_tls_certs_parsing() {
            // Verify that the test certificates can be parsed correctly
            let cert_pem = Pem::Value(TEST_SERVER_CERT.as_bytes().to_vec());
            let key_pem = Pem::Value(TEST_SERVER_KEY.as_bytes().to_vec());
            let ca_pem = Pem::Value(TEST_CA_CERT.as_bytes().to_vec());

            let certs = super::load_certs(&cert_pem).expect("failed to load certs");
            assert!(!certs.is_empty(), "expected at least one certificate");

            let _key = super::load_key(&key_pem).expect("failed to load key");

            let root_store = super::build_root_store(&ca_pem).expect("failed to build root store");
            assert!(!root_store.is_empty(), "expected at least one CA cert");
        }

        #[test]
        fn test_tls_acceptor_creation() {
            // Ensure ring is installed as the default crypto provider
            // (no-op if already installed, e.g. under Buck with native-tls).
            let _ = rustls::crypto::ring::default_provider().install_default();

            // Set up TLS config using the standard override pattern
            let config = hyperactor_config::global::lock();
            let _guard_cert =
                config.override_key(TLS_CERT, Pem::Value(TEST_SERVER_CERT.as_bytes().to_vec()));
            let _guard_key =
                config.override_key(TLS_KEY, Pem::Value(TEST_SERVER_KEY.as_bytes().to_vec()));
            let _guard_ca =
                config.override_key(TLS_CA, Pem::Value(TEST_CA_CERT.as_bytes().to_vec()));

            // Verify that we can create a TLS acceptor
            let _acceptor = super::tls_acceptor().expect("failed to create TLS acceptor");
        }

        #[test]
        fn test_tls_connector_creation() {
            // Ensure ring is installed as the default crypto provider
            // (no-op if already installed, e.g. under Buck with native-tls).
            let _ = rustls::crypto::ring::default_provider().install_default();

            // Set up TLS config using the standard override pattern
            let config = hyperactor_config::global::lock();
            let _guard_cert =
                config.override_key(TLS_CERT, Pem::Value(TEST_SERVER_CERT.as_bytes().to_vec()));
            let _guard_key =
                config.override_key(TLS_KEY, Pem::Value(TEST_SERVER_KEY.as_bytes().to_vec()));
            let _guard_ca =
                config.override_key(TLS_CA, Pem::Value(TEST_CA_CERT.as_bytes().to_vec()));

            // Verify that we can create a TLS connector
            let _connector = super::tls_connector().expect("failed to create TLS connector");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::VecDeque;
    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::task::Poll;
    use std::time::Duration;
    #[cfg(target_os = "linux")] // uses abstract names
    use std::time::UNIX_EPOCH;

    #[cfg(target_os = "linux")] // uses abstract names
    use anyhow::Result;
    use bytes::Bytes;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::distributions::Alphanumeric;
    use timed_test::async_timed_test;
    use tokio::io::AsyncWrite;
    use tokio::io::DuplexStream;
    use tokio::io::ReadHalf;
    use tokio::io::WriteHalf;
    use tokio::task::JoinHandle;
    use tokio_util::net::Listener;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::channel;
    use crate::channel::net::framed::FrameReader;
    use crate::channel::net::framed::FrameWrite;
    use crate::channel::net::server::ServerConn;
    use crate::channel::net::server::SessionManager;
    use crate::config;
    use crate::metrics;
    use crate::sync::mvar::MVar;

    #[cfg(target_os = "linux")] // uses abstract names
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_unix_basic() -> Result<()> {
        let timestamp = RealClock
            .system_time_now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_address = format!("test_unix_basic_{}", timestamp);

        let (addr, mut rx) =
            net::unix::serve::<u64>(unix::SocketAddr::from_abstract_name(&unique_address)?)
                .unwrap();

        // It is important to keep Tx alive until all expected messages are
        // received. Otherwise, the channel would be closed when Tx is dropped.
        // Although the messages are sent to the server's buffer before the
        // channel was closed, NetRx could still error out before taking them
        // out of the buffer because NetRx could not ack through the closed
        // channel.
        {
            let tx: ChannelTx<u64> = channel::dial::<u64>(addr.clone()).unwrap();
            tx.post(123);
            assert_eq!(rx.recv().await.unwrap(), 123);
        }

        {
            let tx = channel::dial::<u64>(addr.clone()).unwrap();
            tx.post(321);
            tx.post(111);
            tx.post(444);

            assert_eq!(rx.recv().await.unwrap(), 321);
            assert_eq!(rx.recv().await.unwrap(), 111);
            assert_eq!(rx.recv().await.unwrap(), 444);
        }

        {
            let tx = channel::dial::<u64>(addr).unwrap();
            drop(rx);

            let (return_tx, return_rx) = oneshot::channel();
            tx.try_post(123, return_tx);
            assert_matches!(
                return_rx.await,
                Ok(SendError {
                    error: ChannelError::Closed,
                    message: 123,
                    ..
                })
            );
        }

        Ok(())
    }

    #[cfg(target_os = "linux")] // uses abstract names
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_unix_basic_client_before_server() -> Result<()> {
        // We run this test on Unix because we can pick our own port names more easily.
        let timestamp = RealClock
            .system_time_now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let socket_addr =
            unix::SocketAddr::from_abstract_name(&format!("test_unix_basic_{}", timestamp))
                .unwrap();

        // Dial the channel before we actually serve it.
        let addr = ChannelAddr::Unix(socket_addr.clone());
        let tx = crate::channel::dial::<u64>(addr.clone()).unwrap();
        tx.post(123);

        let (_, mut rx) = net::unix::serve::<u64>(socket_addr).unwrap();
        assert_eq!(rx.recv().await.unwrap(), 123);

        tx.post(321);
        tx.post(111);
        tx.post(444);

        assert_eq!(rx.recv().await.unwrap(), 321);
        assert_eq!(rx.recv().await.unwrap(), 111);
        assert_eq!(rx.recv().await.unwrap(), 444);

        Ok(())
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 60)]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" })
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_tcp_basic() {
        let (addr, mut rx) = tcp::serve::<u64>("[::1]:0".parse().unwrap()).unwrap();
        {
            let tx = channel::dial::<u64>(addr.clone()).unwrap();
            tx.post(123);
            assert_eq!(rx.recv().await.unwrap(), 123);
        }

        {
            let tx = channel::dial::<u64>(addr.clone()).unwrap();
            tx.post(321);
            tx.post(111);
            tx.post(444);

            assert_eq!(rx.recv().await.unwrap(), 321);
            assert_eq!(rx.recv().await.unwrap(), 111);
            assert_eq!(rx.recv().await.unwrap(), 444);
        }

        {
            let tx = channel::dial::<u64>(addr).unwrap();
            drop(rx);

            let (return_tx, return_rx) = oneshot::channel();
            tx.try_post(123, return_tx);
            assert_matches!(
                return_rx.await,
                Ok(SendError {
                    error: ChannelError::Closed,
                    message: 123,
                    ..
                })
            );
        }
    }

    // The message size is limited by CODEC_MAX_FRAME_LENGTH.
    #[async_timed_test(timeout_secs = 5)]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" })
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_tcp_message_size() {
        let default_size_in_bytes = 100 * 1024 * 1024;
        // Use temporary config for this test
        let config = hyperactor_config::global::lock();
        let _guard1 = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(1));
        let _guard2 = config.override_key(config::CODEC_MAX_FRAME_LENGTH, default_size_in_bytes);

        let (addr, mut rx) = tcp::serve::<String>("[::1]:0".parse().unwrap()).unwrap();

        let tx = channel::dial::<String>(addr.clone()).unwrap();
        // Default size is okay
        {
            // Leave some headroom because Tx will wrap the payload in Frame::Message.
            let message = "a".repeat(default_size_in_bytes - 1024);
            tx.post(message.clone());
            assert_eq!(rx.recv().await.unwrap(), message);
        }
        // Bigger than the default size will fail.
        {
            let (return_channel, return_receiver) = oneshot::channel();
            let message = "a".repeat(default_size_in_bytes + 1024);
            tx.try_post(message.clone(), return_channel);
            let returned = return_receiver.await.unwrap();
            assert_eq!(message, returned.message);
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" })
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_ack_flush() {
        let config = hyperactor_config::global::lock();
        // Set a large value to effectively prevent acks from being sent except
        // during shutdown flush.
        let _guard_message_ack =
            config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 100000000);
        let _guard_delivery_timeout =
            config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(5));

        let (addr, mut net_rx) = tcp::serve::<u64>("[::1]:0".parse().unwrap()).unwrap();
        let net_tx = channel::dial::<u64>(addr.clone()).unwrap();
        let (tx, rx) = oneshot::channel();
        net_tx.try_post(1, tx);
        assert_eq!(net_rx.recv().await.unwrap(), 1);
        drop(net_rx);
        // Using `is_err` to confirm the message is delivered/acked is confusing,
        // but is correct. See how send is implemented: https://fburl.com/code/ywt8lip2
        assert!(rx.await.is_err());
    }

    #[async_timed_test(timeout_secs = 60)]
    // TODO: OSS: failed to retrieve ipv6 address
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_meta_tls_basic() {
        hyperactor_telemetry::initialize_logging_for_test();

        let addr = ChannelAddr::any(ChannelTransport::MetaTls(TlsMode::IpV6));
        let meta_addr = match addr {
            ChannelAddr::MetaTls(meta_addr) => meta_addr,
            _ => panic!("expected MetaTls address"),
        };
        let (local_addr, mut rx) = net::meta::serve::<u64>(meta_addr).unwrap();
        {
            let tx = channel::dial::<u64>(local_addr.clone()).unwrap();
            tx.post(123);
        }
        assert_eq!(rx.recv().await.unwrap(), 123);

        {
            let tx = channel::dial::<u64>(local_addr.clone()).unwrap();
            tx.post(321);
            tx.post(111);
            tx.post(444);
            assert_eq!(rx.recv().await.unwrap(), 321);
            assert_eq!(rx.recv().await.unwrap(), 111);
            assert_eq!(rx.recv().await.unwrap(), 444);
        }

        {
            let tx = channel::dial::<u64>(local_addr).unwrap();
            drop(rx);

            let (return_tx, return_rx) = oneshot::channel();
            tx.try_post(123, return_tx);
            assert_matches!(
                return_rx.await,
                Ok(SendError {
                    error: ChannelError::Closed,
                    message: 123,
                    ..
                })
            );
        }
    }

    #[derive(Clone, Debug, Default)]
    struct NetworkFlakiness {
        // A tuple of:
        //   1. the probability of a network failure when sending a message.
        //   2. the max number of disconnections allowed.
        //   3. the minimum duration between disconnections.
        //
        //   2 and 3 are useful to prevent frequent disconnections leading to
        //   unacked messages being sent repeatedly.
        disconnect_params: Option<(f64, u64, Duration)>,
        // The max possible latency when sending a message. The actual latency
        // is randomly generated between 0 and max_latency.
        latency_range: Option<(Duration, Duration)>,
    }

    impl NetworkFlakiness {
        // Calculate whether to disconnect
        async fn should_disconnect(
            &self,
            rng: &mut impl rand::Rng,
            disconnected_count: u64,
            prev_diconnected_at: &RwLock<Instant>,
        ) -> bool {
            let Some((prob, max_disconnects, duration)) = &self.disconnect_params else {
                return false;
            };

            let disconnected_at = prev_diconnected_at.read().unwrap();
            if disconnected_at.elapsed() > *duration && disconnected_count < *max_disconnects {
                rng.gen_bool(*prob)
            } else {
                false
            }
        }
    }

    struct MockLink<M> {
        buffer_size: usize,
        receiver_storage: Arc<MVar<DuplexStream>>,
        // If true, `connect()` on this link will always return an error.
        fail_connects: Arc<AtomicBool>,
        // Used to break the existing connection, if there is one. It still
        // allows reconnect.
        disconnect_signal: watch::Sender<()>,
        network_flakiness: NetworkFlakiness,
        disconnected_count: Arc<AtomicU64>,
        prev_diconnected_at: Arc<RwLock<Instant>>,
        // If set, print logs every `debug_log_sampling_rate` messages. This
        // is normally set only when debugging a test failure.
        debug_log_sampling_rate: Option<u64>,
        _message_type: PhantomData<M>,
    }

    impl<M> fmt::Debug for MockLink<M> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MockLink")
                .field("buffer_size", &self.buffer_size)
                .field("receiver_storage", &"<MVar<DuplexStream>>")
                .field("fail_connects", &self.fail_connects)
                .field("disconnect_signal", &"<watch::Sender>")
                .field("network_flakiness", &self.network_flakiness)
                .field("disconnected_count", &self.disconnected_count)
                .field("prev_diconnected_at", &"<RwLock<Instant>>")
                .field("debug_log_sampling_rate", &self.debug_log_sampling_rate)
                .finish()
        }
    }

    impl<M: RemoteMessage> MockLink<M> {
        fn new() -> Self {
            let (sender, _) = watch::channel(());
            Self {
                buffer_size: 64,
                receiver_storage: Arc::new(MVar::empty()),
                fail_connects: Arc::new(AtomicBool::new(false)),
                disconnect_signal: sender,
                network_flakiness: NetworkFlakiness::default(),
                disconnected_count: Arc::new(AtomicU64::new(0)),
                prev_diconnected_at: Arc::new(RwLock::new(RealClock.now())),
                debug_log_sampling_rate: None,
                _message_type: PhantomData,
            }
        }

        // If `fail_connects` is true, `connect()` on this link will
        // always return an error.
        fn fail_connects() -> Self {
            Self {
                fail_connects: Arc::new(AtomicBool::new(true)),
                ..Self::new()
            }
        }

        fn with_network_flakiness(network_flakiness: NetworkFlakiness) -> Self {
            if let Some((min, max)) = network_flakiness.latency_range {
                assert!(min < max);
            }

            Self {
                network_flakiness,
                ..Self::new()
            }
        }

        fn receiver_storage(&self) -> Arc<MVar<DuplexStream>> {
            self.receiver_storage.clone()
        }

        #[allow(dead_code)] // Not used outside tests.
        fn source(&self) -> ChannelAddr {
            // Use a dummy address as a placeholder.
            ChannelAddr::Local(u64::MAX)
        }

        fn disconnected_count(&self) -> Arc<AtomicU64> {
            self.disconnected_count.clone()
        }

        fn disconnect_signal(&self) -> &watch::Sender<()> {
            &self.disconnect_signal
        }

        fn fail_connects_switch(&self) -> Arc<AtomicBool> {
            self.fail_connects.clone()
        }

        fn set_buffer_size(&mut self, size: usize) {
            self.buffer_size = size;
        }

        fn set_sampling_rate(&mut self, sampling_rate: u64) {
            self.debug_log_sampling_rate = Some(sampling_rate);
        }
    }

    #[async_trait]
    impl<M: RemoteMessage> Link for MockLink<M> {
        type Stream = DuplexStream;

        fn dest(&self) -> ChannelAddr {
            // Use a dummy address as a placeholder.
            ChannelAddr::Local(u64::MAX)
        }

        async fn connect(&self) -> Result<Self::Stream, ClientError> {
            tracing::debug!("MockLink starts to connect.");
            if self.fail_connects.load(Ordering::Acquire) {
                return Err(ClientError::Connect(
                    self.dest(),
                    std::io::Error::other("intentional error"),
                    "expected failure injected by the mock".to_string(),
                ));
            }

            // Add relays between server and client streams. The
            // relays provides the place to inject network flakiness.
            // The message flow looks like:
            //
            // server <-> server relay <-> injection logic <-> client relay <-> client
            async fn relay_message<M: RemoteMessage>(
                mut disconnect_signal: watch::Receiver<()>,
                network_flakiness: NetworkFlakiness,
                disconnected_count: Arc<AtomicU64>,
                prev_diconnected_at: Arc<RwLock<Instant>>,
                mut reader: FrameReader<ReadHalf<DuplexStream>>,
                mut writer: WriteHalf<DuplexStream>,
                // Used by client and server tokio tasks to coordinate
                // stopping together.
                task_coordination_token: CancellationToken,
                debug_log_sampling_rate: Option<u64>,
                // Whether the relayed message is from client to
                // server.
                is_from_client: bool,
            ) {
                // Used to simulate latency. Briefly, messages are
                // buffered in the queue and wait for the expected
                // latency elapse.
                async fn wait_for_latency_elapse(
                    queue: &VecDeque<(Bytes, Instant)>,
                    network_flakiness: &NetworkFlakiness,
                    rng: &mut impl rand::Rng,
                ) {
                    if let Some((min, max)) = network_flakiness.latency_range {
                        let diff = max.abs_diff(min);
                        let factor = rng.gen_range(0.0..=1.0);
                        let latency = min + diff.mul_f64(factor);
                        RealClock
                            .sleep_until(queue.front().unwrap().1 + latency)
                            .await;
                    }
                }

                let mut rng = rand::rngs::SmallRng::from_entropy();
                let mut queue: VecDeque<(Bytes, Instant)> = VecDeque::new();
                let mut send_count = 0u64;

                loop {
                    tokio::select! {
                        read_res = reader.next() => {
                            match read_res {
                                Ok(Some(data)) => {
                                    queue.push_back((data, RealClock.now()));
                                }
                                Ok(None) | Err(_) => {
                                        tracing::debug!("The upstream is closed or dropped. MockLink disconnects");
                                        break;
                                }
                            }
                        }
                        _ = wait_for_latency_elapse(&queue, &network_flakiness, &mut rng), if !queue.is_empty() => {
                            let count = disconnected_count.load(Ordering::Relaxed);
                            if network_flakiness.should_disconnect(&mut rng, count, &prev_diconnected_at).await {
                                tracing::debug!("MockLink disconnects");
                                disconnected_count.fetch_add(1, Ordering::Relaxed);

                                metrics::CHANNEL_RECONNECTIONS.add(
                                    1,
                                    hyperactor_telemetry::kv_pairs!(
                                        "transport" => "mock",
                                        "reason" => "network_flakiness",
                                    ),
                                );

                                let mut w = prev_diconnected_at.write().unwrap();
                                *w = RealClock.now();
                                break;
                            }
                            let data = queue.pop_front().unwrap().0;
                            let is_sampled = debug_log_sampling_rate.is_some_and(|sample_rate| send_count % sample_rate == 1);
                            if is_sampled {
                                if is_from_client {
                                    if let Ok(Frame::Message(_seq, _msg)) = bincode::deserialize::<Frame<M>>(&data) {
                                        tracing::debug!("MockLink relays a msg from client. msg type: {}", std::any::type_name::<M>());
                                    }
                                } else {
                                    let result = deserialize_response(data.clone());
                                    if let Ok(NetRxResponse::Ack(seq)) = result {
                                        tracing::debug!("MockLink relays an ack from server. seq: {}", seq);
                                    }
                                }
                            }
                            let mut fw  = FrameWrite::new(writer, data, hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH)).unwrap();
                            if fw.send().await.is_err() {
                                break;
                            }
                            writer = fw.complete();
                            send_count += 1;
                        }
                        _ = task_coordination_token.cancelled() => break,

                        changed = disconnect_signal.changed() => {
                            tracing::debug!("MockLink disconnects per disconnect_signal {:?}", changed);
                            break;
                        }
                    }
                }

                task_coordination_token.cancel();
            }

            let (server, server_relay) = tokio::io::duplex(self.buffer_size);
            let (client, client_relay) = tokio::io::duplex(self.buffer_size);

            let (server_r, server_writer) = tokio::io::split(server_relay);
            let (client_r, client_writer) = tokio::io::split(client_relay);

            let max_len = hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH);
            let server_reader = FrameReader::new(server_r, max_len);
            let client_reader = FrameReader::new(client_r, max_len);

            let task_coordination_token = CancellationToken::new();
            let _server_relay_task_handle = tokio::spawn(relay_message::<M>(
                self.disconnect_signal.subscribe(),
                self.network_flakiness.clone(),
                self.disconnected_count.clone(),
                self.prev_diconnected_at.clone(),
                server_reader,
                client_writer,
                task_coordination_token.clone(),
                self.debug_log_sampling_rate.clone(),
                /*is_from_client*/ false,
            ));
            let _client_relay_task_handle = tokio::spawn(relay_message::<M>(
                self.disconnect_signal.subscribe(),
                self.network_flakiness.clone(),
                self.disconnected_count.clone(),
                self.prev_diconnected_at.clone(),
                client_reader,
                server_writer,
                task_coordination_token,
                self.debug_log_sampling_rate.clone(),
                /*is_from_client*/ true,
            ));

            self.receiver_storage.put(server).await;
            Ok(client)
        }
    }

    struct MockLinkListener {
        receiver_storage: Arc<MVar<DuplexStream>>,
        channel_addr: ChannelAddr,
        cached_future: Option<Pin<Box<dyn Future<Output = DuplexStream> + Send>>>,
    }

    impl MockLinkListener {
        fn new(receiver_storage: Arc<MVar<DuplexStream>>, channel_addr: ChannelAddr) -> Self {
            Self {
                receiver_storage,
                channel_addr,
                cached_future: None,
            }
        }
    }

    impl Listener for MockLinkListener {
        type Io = DuplexStream;
        type Addr = ChannelAddr;

        fn poll_accept(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<(Self::Io, Self::Addr)>> {
            if self.cached_future.is_none() {
                let storage = self.receiver_storage.clone();
                let fut = async move { storage.take().await };
                self.cached_future = Some(Box::pin(fut));
            }
            self.cached_future
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(cx)
                .map(|io| {
                    self.cached_future = None;
                    Ok((io, self.channel_addr.clone()))
                })
        }

        fn local_addr(&self) -> std::io::Result<Self::Addr> {
            Ok(self.channel_addr.clone())
        }
    }

    async fn serve<M>(
        manager: &SessionManager,
    ) -> (
        JoinHandle<std::result::Result<(), anyhow::Error>>,
        FrameReader<ReadHalf<DuplexStream>>,
        WriteHalf<DuplexStream>,
        mpsc::Receiver<M>,
        CancellationToken,
    )
    where
        M: RemoteMessage,
    {
        let cancel_token = CancellationToken::new();
        // When testing ServerConn, we do not need a Link object, but
        // only a duplex stream. Therefore, we create them directly so
        // the test will not have dependence on Link.
        let (sender, receiver) = tokio::io::duplex(5000);
        let source = ChannelAddr::Local(u64::MAX);
        let dest = ChannelAddr::Local(u64::MAX);
        let conn = ServerConn::new(receiver, source, dest);
        let manager1 = manager.clone();
        let cancel_token_1 = cancel_token.child_token();
        let (tx, rx) = mpsc::channel(1);
        let join_handle =
            tokio::spawn(async move { manager1.serve(conn, tx, cancel_token_1).await });
        let (r, writer) = tokio::io::split(sender);
        let reader = FrameReader::new(
            r,
            hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
        );
        (join_handle, reader, writer, rx, cancel_token)
    }

    async fn write_stream<M, W>(
        mut writer: W,
        session_id: u64,
        messages: &[(u64, M)],
        init: bool,
    ) -> W
    where
        M: RemoteMessage + PartialEq + Clone,
        W: AsyncWrite + Unpin,
    {
        if init {
            let message =
                serde_multipart::serialize_bincode(&Frame::<u64>::Init(session_id)).unwrap();
            let mut fw = FrameWrite::new(
                writer,
                message.framed(),
                hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
            )
            .map_err(|(_w, e)| e)
            .unwrap();
            fw.send().await.unwrap();
            writer = fw.complete();
        }

        for (seq, message) in messages {
            let message =
                serde_multipart::serialize_bincode(&Frame::<M>::Message(*seq, message.clone()))
                    .unwrap();
            let mut fw = FrameWrite::new(
                writer,
                message.framed(),
                hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
            )
            .map_err(|(_w, e)| e)
            .unwrap();
            fw.send().await.unwrap();
            writer = fw.complete();
        }

        writer
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_persistent_server_session() {
        // Use temporary config for this test
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);

        async fn verify_ack(reader: &mut FrameReader<ReadHalf<DuplexStream>>, expected_last: u64) {
            let mut last_acked: i128 = -1;
            loop {
                let bytes = reader.next().await.unwrap().unwrap();
                let acked = deserialize_response(bytes).unwrap().into_ack().unwrap();
                assert!(
                    acked as i128 > last_acked,
                    "acks should be delivered in ascending order"
                );
                last_acked = acked as i128;
                assert!(acked <= expected_last);
                if acked == expected_last {
                    break;
                }
            }
        }

        let manager = SessionManager::new();
        let session_id = 123;

        {
            let (handle, mut reader, mut writer, mut rx, _cancel_token) =
                serve::<u64>(&manager).await;
            writer = write_stream(
                writer,
                session_id,
                &[
                    (0u64, 100u64),
                    (1u64, 101u64),
                    (2u64, 102u64),
                    (3u64, 103u64),
                ],
                /*init*/ true,
            )
            .await;

            assert_eq!(rx.recv().await, Some(100));
            assert_eq!(rx.recv().await, Some(101));
            assert_eq!(rx.recv().await, Some(102));
            // Intentionally skip 103, so we can verify it still can be received
            // after the connection is closed.
            // assert_eq!(rx.recv().await, Some(103));

            // server side might or might not ack seq<3 depending on the order
            // of execution introduced by tokio::select. But it definitely would
            // ack 3.
            verify_ack(&mut reader, 3).await;

            // Drop the reader and writer to cause the connection to close.
            drop(reader);
            drop(writer);
            handle.await.unwrap().unwrap();
            // mpsc is closed too and there should be no unread message left.
            assert_eq!(rx.recv().await, Some(103));
            assert_eq!(rx.recv().await, None);
        };

        // Now, create a new connection with the same session.
        {
            let (handle, mut reader, writer, mut rx, cancel_token) = serve::<u64>(&manager).await;
            let handle = tokio::spawn(async move {
                let result = handle.await.unwrap();
                eprintln!("handle joined with: {:?}", result);
                result
            });

            let _ = write_stream(
                writer,
                session_id,
                &[
                    (2u64, 102u64),
                    (3u64, 103u64),
                    (4u64, 104u64),
                    (5u64, 105u64),
                ],
                /*init*/ true,
            )
            .await;

            // We don't get another '102' and '103' because they were already
            // delivered in the previous connection.
            assert_eq!(rx.recv().await, Some(104));
            assert_eq!(rx.recv().await, Some(105));

            verify_ack(&mut reader, 5).await;

            // Wait long enough to ensure server processed everything.
            RealClock.sleep(Duration::from_secs(5)).await;

            cancel_token.cancel();
            handle.await.unwrap().unwrap();
            // mpsc is closed too and there should be no unread message left.
            assert!(rx.recv().await.is_none());
            // should send NetRxResponse::Closed before stopping server.
            let bytes = reader.next().await.unwrap().unwrap();
            assert!(deserialize_response(bytes).unwrap().is_closed());
            // No more acks from server.
            assert!(reader.next().await.unwrap().is_none());
        };
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_ack_from_server_session() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);
        let manager = SessionManager::new();
        let session_id = 123u64;

        let (handle, mut reader, mut writer, mut rx, cancel_token) = serve::<u64>(&manager).await;
        for i in 0u64..100u64 {
            writer = write_stream(
                writer,
                session_id,
                &[(i, 100u64 + i)],
                /*init*/ i == 0u64,
            )
            .await;
            assert_eq!(rx.recv().await, Some(100u64 + i));
            let bytes = reader.next().await.unwrap().unwrap();
            let acked = deserialize_response(bytes).unwrap().into_ack().unwrap();
            assert_eq!(acked, i);
        }

        // Wait long enough to ensure server processed everything.
        RealClock.sleep(Duration::from_secs(5)).await;

        cancel_token.cancel();
        handle.await.unwrap().unwrap();
        // mpsc is closed too and there should be no unread message left.
        assert!(rx.recv().await.is_none());
        // should send NetRxResponse::Closed before stopping server.
        let bytes = reader.next().await.unwrap().unwrap();
        assert!(deserialize_response(bytes).unwrap().is_closed());
        // No more acks from server.
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tracing_test::traced_test]
    async fn verify_tx_closed(tx_status: &mut watch::Receiver<TxStatus>, expected_log: &str) {
        match RealClock
            .timeout(Duration::from_secs(5), tx_status.changed())
            .await
        {
            Ok(Ok(())) => {
                let current_status = *tx_status.borrow();
                assert_eq!(current_status, TxStatus::Closed);
                logs_assert(|logs| {
                    if logs.iter().any(|log| log.contains(expected_log)) {
                        Ok(())
                    } else {
                        Err("expected log not found".to_string())
                    }
                });
            }
            Ok(Err(_)) => panic!("watch::Receiver::changed() failed because sender is dropped."),
            Err(_) => panic!("timeout before tx_status changed"),
        }
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    // TODO: OSS: The logs_assert function returned an error: expected log not found
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_tcp_tx_delivery_timeout() {
        // This link always fails to connect.
        let link = MockLink::<u64>::fail_connects();
        let tx = super::dial::<u64>(link);
        // Override the default (1m) for the purposes of this test.
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(1));
        let mut tx_receiver = tx.status().clone();
        let (return_channel, _return_receiver) = oneshot::channel();
        tx.try_post(123, return_channel);
        verify_tx_closed(&mut tx_receiver, "failed to deliver message within timeout").await;
    }

    async fn take_receiver(
        receiver_storage: &MVar<DuplexStream>,
    ) -> (FrameReader<ReadHalf<DuplexStream>>, WriteHalf<DuplexStream>) {
        let receiver = receiver_storage.take().await;
        let (r, writer) = tokio::io::split(receiver);
        let reader = FrameReader::new(
            r,
            hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
        );
        (reader, writer)
    }

    async fn verify_message<M: RemoteMessage + PartialEq + std::fmt::Debug>(
        reader: &mut FrameReader<ReadHalf<DuplexStream>>,
        expect: (u64, M),
        loc: u32,
    ) {
        let expected = Frame::Message(expect.0, expect.1);
        let bytes = reader.next().await.unwrap().expect("unexpected EOF");
        let message = serde_multipart::Message::from_framed(bytes).unwrap();
        let frame: Frame<M> = serde_multipart::deserialize_bincode(message).unwrap();

        assert_eq!(frame, expected, "from ln={loc}");
    }

    async fn verify_stream<M: RemoteMessage + PartialEq + std::fmt::Debug + Clone>(
        reader: &mut FrameReader<ReadHalf<DuplexStream>>,
        expects: &[(u64, M)],
        expect_session_id: Option<u64>,
        loc: u32,
    ) -> u64 {
        let session_id = {
            let bytes = reader.next().await.unwrap().expect("unexpected EOF");
            let message = serde_multipart::Message::from_framed(bytes).unwrap();
            let frame: Frame<M> = serde_multipart::deserialize_bincode(message).unwrap();
            match frame {
                Frame::Init(session_id) => session_id,
                _ => panic!("the 1st frame is not Init: {:?}. from ln={loc}", frame),
            }
        };

        if let Some(expected_id) = expect_session_id {
            assert_eq!(session_id, expected_id, "from ln={loc}");
        }

        for expect in expects {
            verify_message(reader, expect.clone(), loc).await;
        }

        session_id
    }

    async fn net_tx_send(tx: &NetTx<u64>, msgs: &[u64]) {
        for msg in msgs {
            tx.post(*msg);
        }
    }

    // Happy path: all messages are acked.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ack_in_net_tx_basic() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let tx = super::dial::<u64>(link);

        // Send some messages, but not acking any of them.
        net_tx_send(&tx, &[100, 101, 102, 103, 104]).await;
        let session_id = {
            let (mut reader, mut writer) = take_receiver(&receiver_storage).await;
            let id = verify_stream(
                &mut reader,
                &[
                    (0u64, 100u64),
                    (1u64, 101u64),
                    (2u64, 102u64),
                    (3u64, 103u64),
                    (4u64, 104u64),
                ],
                None,
                line!(),
            )
            .await;

            for i in 0u64..5u64 {
                writer = FrameWrite::write_frame(
                    writer,
                    serialize_response(NetRxResponse::Ack(i)).unwrap(),
                    1024,
                )
                .await
                .map_err(|(_, e)| e)
                .unwrap();
            }
            // Wait for the acks to be processed by NetTx.
            RealClock.sleep(Duration::from_secs(3)).await;
            // Drop both halves to break the in-memory connection (parity with old drop of DuplexStream).
            drop(reader);
            drop(writer);

            id
        };

        // Sent a new message to verify all sent messages will not be resent.
        net_tx_send(&tx, &[105u64]).await;
        {
            let (mut reader, _writer) = take_receiver(&receiver_storage).await;
            verify_stream(&mut reader, &[(5u64, 105u64)], Some(session_id), line!()).await;
            // Reader/writer dropped here. This breaks the connection.
        };
    }

    // Verify unacked message will be resent after reconnection.
    #[async_timed_test(timeout_secs = 60)]
    async fn test_persistent_net_tx() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();

        let tx = super::dial::<u64>(link);
        let mut session_id = None;

        // Send some messages, but not acking any of them.
        net_tx_send(&tx, &[100, 101, 102, 103, 104]).await;

        // How many times to reconnect.
        let n = 10;

        // Reconnect multiple times. The messages should be resent every time
        // because none of them is acked.
        for i in 0..n {
            {
                let (mut reader, mut writer) = take_receiver(&receiver_storage).await;
                let id = verify_stream(
                    &mut reader,
                    &[
                        (0u64, 100u64),
                        (1u64, 101u64),
                        (2u64, 102u64),
                        (3u64, 103u64),
                        (4u64, 104u64),
                    ],
                    session_id,
                    line!(),
                )
                .await;
                if i == 0 {
                    assert!(session_id.is_none());
                    session_id = Some(id);
                }

                // In the last iteration, ack part of the messages. This should
                // prune them from future resent.
                if i == n - 1 {
                    writer = FrameWrite::write_frame(
                        writer,
                        serialize_response(NetRxResponse::Ack(1)).unwrap(),
                        1024,
                    )
                    .await
                    .map_err(|(_, e)| e)
                    .unwrap();
                    // Wait for the acks to be processed by NetTx.
                    RealClock.sleep(Duration::from_secs(3)).await;
                }
                // client DuplexStream is dropped here. This breaks the connection.
                drop(reader);
                drop(writer);
            };
        }

        // Verify only unacked are resent.
        for _ in 0..n {
            {
                let (mut reader, mut _writer) = take_receiver(&receiver_storage).await;
                verify_stream(
                    &mut reader,
                    &[(2u64, 102u64), (3u64, 103u64), (4u64, 104u64)],
                    session_id,
                    line!(),
                )
                .await;
                // drop(reader/_writer) at scope end
            };
        }

        // Now send more messages.
        net_tx_send(&tx, &[105u64, 106u64, 107u64, 108u64, 109u64]).await;
        // Verify the unacked messages from the 1st send will be grouped with
        // the 2nd send.
        for i in 0..n {
            {
                let (mut reader, mut writer) = take_receiver(&receiver_storage).await;
                verify_stream(
                    &mut reader,
                    &[
                        // From the 1st send.
                        (2u64, 102u64),
                        (3u64, 103u64),
                        (4u64, 104u64),
                        // From the 2nd send.
                        (5u64, 105u64),
                        (6u64, 106u64),
                        (7u64, 107u64),
                        (8u64, 108u64),
                        (9u64, 109u64),
                    ],
                    session_id,
                    line!(),
                )
                .await;

                // In the last iteration, ack part of the messages from the 1st
                // sent.
                if i == n - 1 {
                    // Intentionally ack 1 again to verify it is okay to ack
                    // messages that was already acked.
                    writer = FrameWrite::write_frame(
                        writer,
                        serialize_response(NetRxResponse::Ack(1)).unwrap(),
                        1024,
                    )
                    .await
                    .map_err(|(_, e)| e)
                    .unwrap();
                    writer = FrameWrite::write_frame(
                        writer,
                        serialize_response(NetRxResponse::Ack(2)).unwrap(),
                        1024,
                    )
                    .await
                    .map_err(|(_, e)| e)
                    .unwrap();
                    writer = FrameWrite::write_frame(
                        writer,
                        serialize_response(NetRxResponse::Ack(3)).unwrap(),
                        1024,
                    )
                    .await
                    .map_err(|(_, e)| e)
                    .unwrap();
                    // Wait for the acks to be processed by NetTx.
                    RealClock.sleep(Duration::from_secs(3)).await;
                }
                // client DuplexStream is dropped here. This breaks the connection.
                drop(reader);
                drop(writer);
            };
        }

        for i in 0..n {
            {
                let (mut reader, mut writer) = take_receiver(&receiver_storage).await;
                verify_stream(
                    &mut reader,
                    &[
                        // From the 1st send.
                        (4u64, 104),
                        // From the 2nd send.
                        (5u64, 105u64),
                        (6u64, 106u64),
                        (7u64, 107u64),
                        (8u64, 108u64),
                        (9u64, 109u64),
                    ],
                    session_id,
                    line!(),
                )
                .await;

                // In the last iteration, ack part of the messages from the 2nd send.
                if i == n - 1 {
                    writer = FrameWrite::write_frame(
                        writer,
                        serialize_response(NetRxResponse::Ack(7)).unwrap(),
                        1024,
                    )
                    .await
                    .map_err(|(_, e)| e)
                    .unwrap();
                    // Wait for the acks to be processed by NetTx.
                    RealClock.sleep(Duration::from_secs(3)).await;
                }
                // client DuplexStream is dropped here. This breaks the connection.
                drop(reader);
                drop(writer);
            };
        }

        for _ in 0..n {
            {
                let (mut reader, writer) = take_receiver(&receiver_storage).await;
                verify_stream(
                    &mut reader,
                    &[
                        // From the 2nd send.
                        (8u64, 108u64),
                        (9u64, 109u64),
                    ],
                    session_id,
                    line!(),
                )
                .await;
                // client DuplexStream is dropped here. This breaks the connection.
                drop(reader);
                drop(writer);
            };
        }
    }

    #[async_timed_test(timeout_secs = 15)]
    async fn test_ack_before_redelivery_in_net_tx() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let net_tx = super::dial::<u64>(link);

        // Verify sent-and-ack a message. This is necessary for the test to
        // trigger a connection.
        let (return_channel_tx, return_channel_rx) = oneshot::channel();
        net_tx.try_post(100, return_channel_tx);
        let (mut reader, mut writer) = take_receiver(&receiver_storage).await;
        verify_stream(&mut reader, &[(0u64, 100u64)], None, line!()).await;
        // ack it
        writer = FrameWrite::write_frame(
            writer,
            serialize_response(NetRxResponse::Ack(0)).unwrap(),
            1024,
        )
        .await
        .map_err(|(_, e)| e)
        .unwrap();
        // confirm Tx received ack
        //
        // Using `is_err` to confirm the message is delivered/acked is confusing,
        // but is correct. See how send is implemented: https://fburl.com/code/ywt8lip2
        assert!(return_channel_rx.await.is_err());

        // Now fake an unknown delivery for Tx:
        // Although Tx did not actually send seq=1, we still ack it from Rx to
        // pretend Tx already sent it, just it did not know it was sent
        // successfully.
        let _ = FrameWrite::write_frame(
            writer,
            serialize_response(NetRxResponse::Ack(1)).unwrap(),
            1024,
        )
        .await
        .map_err(|(_, e)| e)
        .unwrap();

        let (return_channel_tx, return_channel_rx) = oneshot::channel();
        net_tx.try_post(101, return_channel_tx);
        // Verify the message is sent to Rx.
        verify_message(&mut reader, (1u64, 101u64), line!()).await;
        // although we did not ack the message after it is sent, since we already
        // acked it previously, Tx will treat it as acked, and considered the
        // message delivered successfully.
        //
        // Using `is_err` to confirm the message is delivered/acked is confusing,
        // but is correct. See how send is implemented: https://fburl.com/code/ywt8lip2
        assert!(return_channel_rx.await.is_err());
    }

    async fn verify_ack_exceeded_limit(disconnect_before_ack: bool) {
        // Use temporary config for this test
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(2));

        let link: MockLink<u64> = MockLink::<u64>::new();
        let disconnect_signal = link.disconnect_signal().clone();
        let fail_connect_switch = link.fail_connects_switch();
        let receiver_storage = link.receiver_storage();
        let tx = super::dial::<u64>(link);
        let mut tx_status = tx.status().clone();
        // send a message
        tx.post(100);
        let (mut reader, writer) = take_receiver(&receiver_storage).await;
        // Confirm message is sent to rx.
        verify_stream(&mut reader, &[(0u64, 100u64)], None, line!()).await;
        // ack it
        let _ = FrameWrite::write_frame(
            writer,
            serialize_response(NetRxResponse::Ack(0)).unwrap(),
            hyperactor_config::global::get(config::CODEC_MAX_FRAME_LENGTH),
        )
        .await
        .map_err(|(_, e)| e)
        .unwrap();
        RealClock.sleep(Duration::from_secs(3)).await;
        // Channel should be still alive because ack was sent.
        assert!(!tx_status.has_changed().unwrap());
        assert_eq!(*tx_status.borrow(), TxStatus::Active);

        tx.post(101);
        // Confirm message is sent to rx.
        verify_message(&mut reader, (1u64, 101u64), line!()).await;

        if disconnect_before_ack {
            // Prevent link from reconnect
            fail_connect_switch.store(true, Ordering::Release);
            // Break the existing connection
            disconnect_signal.send(()).unwrap();
        }

        // Verify the channel is closed due to ack timeout based on the log.
        let expected_log: &str = if disconnect_before_ack {
            "failed to receive ack within timeout 2s; link is currently broken"
        } else {
            "failed to receive ack within timeout 2s; link is currently connected"
        };

        verify_tx_closed(&mut tx_status, expected_log).await;
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    // TODO: OSS: The logs_assert function returned an error: expected log not found
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_ack_exceeded_limit_with_connected_link() {
        verify_ack_exceeded_limit(false).await;
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    // TODO: OSS: The logs_assert function returned an error: expected log not found
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_ack_exceeded_limit_with_broken_link() {
        verify_ack_exceeded_limit(true).await;
    }

    // Verify a large number of messages can be delivered and acked with the
    // presence of flakiness in the network, i.e. random delay and disconnection.
    #[async_timed_test(timeout_secs = 60)]
    async fn test_network_flakiness_in_channel() {
        hyperactor_telemetry::initialize_logging_for_test();

        let sampling_rate = 100;
        let mut link = MockLink::<u64>::with_network_flakiness(NetworkFlakiness {
            disconnect_params: Some((0.001, 15, Duration::from_millis(400))),
            latency_range: Some((Duration::from_millis(100), Duration::from_millis(200))),
        });
        link.set_sampling_rate(sampling_rate);
        // Set a large buffer size to improve throughput.
        link.set_buffer_size(1024000);
        let disconnected_count = link.disconnected_count();
        let receiver_storage = link.receiver_storage();
        let listener = MockLinkListener::new(receiver_storage.clone(), link.dest());
        let local_addr = listener.local_addr().unwrap();
        let (_, mut nx): (ChannelAddr, NetRx<u64>) =
            super::serve(listener, local_addr, false).unwrap();
        let tx = super::dial::<u64>(link);
        let messages: Vec<_> = (0..10001).collect();
        let messages_clone = messages.clone();
        // Put the sender side in a separate task so we can start the receiver
        // side concurrently.
        let send_task_handle = tokio::spawn(async move {
            for message in messages_clone {
                // Add a small delay between messages to give NetRx time to ack.
                // Technically, this test still can pass without this delay. But
                // the test will need a might larger timeout. The reason is
                // fairly convoluted:
                //
                // MockLink uses the number of delivery to calculate the disconnection
                // probability. If NetRx sends messages much faster than NetTx
                // can ack them, there is a higher chance that the messages are
                // not acked before reconnect. Then those message would be redelivered.
                // The repeated redelivery increases the total time of sending
                // these messages.
                RealClock
                    .sleep(Duration::from_micros(rand::random::<u64>() % 100))
                    .await;
                tx.post(message);
            }
            tracing::debug!("NetTx sent all messages");
            // It is important to return tx instead of dropping it here, because
            // Rx might not receive all messages yet.
            tx
        });

        for message in &messages {
            if message % sampling_rate == 0 {
                tracing::debug!("NetRx received a message: {message}");
            }
            assert_eq!(nx.recv().await.unwrap(), *message);
        }
        tracing::debug!("NetRx received all messages");

        let send_result = send_task_handle.await;
        assert!(send_result.is_ok());

        tracing::debug!(
            "MockLink disconnected {} times.",
            disconnected_count.load(Ordering::SeqCst)
        );
        // TODO(pzhang) after the return_handle work in NetTx is done, add a
        // check here to verify the messages are acked correctly.
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_ack_every_n_messages() {
        let config = hyperactor_config::global::lock();
        let _guard_message_ack = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 600);
        let _guard_time_interval =
            config.override_key(config::MESSAGE_ACK_TIME_INTERVAL, Duration::from_secs(1000));
        sparse_ack().await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_ack_every_time_interval() {
        let config = hyperactor_config::global::lock();
        let _guard_message_ack =
            config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 100000000);
        let _guard_time_interval = config.override_key(
            config::MESSAGE_ACK_TIME_INTERVAL,
            Duration::from_millis(500),
        );
        sparse_ack().await;
    }

    async fn sparse_ack() {
        let mut link = MockLink::<u64>::new();
        // Set a large buffer size to improve throughput.
        link.set_buffer_size(1024000);
        let disconnected_count = link.disconnected_count();
        let receiver_storage = link.receiver_storage();
        let listener = MockLinkListener::new(receiver_storage.clone(), link.dest());
        let local_addr = listener.local_addr().unwrap();
        let (_, mut nx): (ChannelAddr, NetRx<u64>) =
            super::serve(listener, local_addr, false).unwrap();
        let tx = super::dial::<u64>(link);
        let messages: Vec<_> = (0..20001).collect();
        let messages_clone = messages.clone();
        // Put the sender side in a separate task so we can start the receiver
        // side concurrently.
        let send_task_handle = tokio::spawn(async move {
            for message in messages_clone {
                RealClock
                    .sleep(Duration::from_micros(rand::random::<u64>() % 100))
                    .await;
                tx.post(message);
            }
            RealClock.sleep(Duration::from_secs(5)).await;
            tracing::debug!("NetTx sent all messages");
            tx
        });

        for message in &messages {
            assert_eq!(nx.recv().await.unwrap(), *message);
        }
        tracing::debug!("NetRx received all messages");

        let send_result = send_task_handle.await;
        assert!(send_result.is_ok());

        tracing::debug!(
            "MockLink disconnected {} times.",
            disconnected_count.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn test_metatls_parsing() {
        // host:port
        let channel: ChannelAddr = "metatls!localhost:1234".parse().unwrap();
        assert_eq!(
            channel,
            ChannelAddr::MetaTls(TlsAddr::Host {
                hostname: "localhost".to_string(),
                port: 1234
            })
        );
        // ipv4:port - can be parsed as hostname or socket address
        let channel: ChannelAddr = "metatls!1.2.3.4:1234".parse().unwrap();
        assert_eq!(
            channel,
            ChannelAddr::MetaTls(TlsAddr::Socket("1.2.3.4:1234".parse().unwrap()))
        );
        // ipv6:port
        let channel: ChannelAddr = "metatls!2401:db00:33c:6902:face:0:2a2:0:1234"
            .parse()
            .unwrap();
        assert_eq!(
            channel,
            ChannelAddr::MetaTls(TlsAddr::Host {
                hostname: "2401:db00:33c:6902:face:0:2a2:0".to_string(),
                port: 1234
            })
        );

        let channel: ChannelAddr = "metatls![::]:1234".parse().unwrap();
        assert_eq!(
            channel,
            ChannelAddr::MetaTls(TlsAddr::Socket("[::]:1234".parse().unwrap()))
        );
    }

    #[async_timed_test(timeout_secs = 300)]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" })
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_tcp_throughput() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_mins(5));

        let socket_addr: SocketAddr = "[::1]:0".parse().unwrap();
        let (local_addr, mut rx) = tcp::serve::<String>(socket_addr).unwrap();

        // Test with 10 connections (senders), each sends 500K messages, 5M messages in total.
        let total_num_msgs = 500000;

        let receive_handle = tokio::spawn(async move {
            let mut num = 0;
            for _ in 0..10 * total_num_msgs {
                rx.recv().await.unwrap();
                num += 1;

                if num % 100000 == 0 {
                    tracing::info!("total number of received messages: {}", num);
                }
            }
        });

        let mut tx_handles = vec![];
        let mut txs = vec![];
        for _ in 0..10 {
            let server_addr = local_addr.clone();
            let tx = Arc::new(channel::dial::<String>(server_addr).unwrap());
            let tx2 = Arc::clone(&tx);
            txs.push(tx);
            tx_handles.push(tokio::spawn(async move {
                let random_string = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(2048)
                    .map(char::from)
                    .collect::<String>();
                for _ in 0..total_num_msgs {
                    tx2.post(random_string.clone());
                }
            }));
        }

        receive_handle.await.unwrap();
        for handle in tx_handles {
            handle.await.unwrap();
        }
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 60)]
    // TODO: OSS: The logs_assert function returned an error: expected log not found
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_net_tx_closed_on_server_reject() {
        let link = MockLink::<u64>::new();
        let receiver_storage = link.receiver_storage();
        let mut tx = super::dial::<u64>(link);
        net_tx_send(&tx, &[100]).await;

        {
            let (_reader, writer) = take_receiver(&receiver_storage).await;
            let _ = FrameWrite::write_frame(
                writer,
                serialize_response(NetRxResponse::Reject("testing".to_string())).unwrap(),
                1024,
            )
            .await
            .map_err(|(_, e)| e);

            // Wait for response to be processed by NetTx before dropping reader/writer. Otherwise
            // the channel will be closed and we will get the wrong error.
            RealClock.sleep(tokio::time::Duration::from_secs(3)).await;
        }

        verify_tx_closed(&mut tx.status, "server rejected connection").await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_server_rejects_conn_on_out_of_sequence_message() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::MESSAGE_ACK_EVERY_N_MESSAGES, 1);
        let manager = SessionManager::new();
        let session_id = 123u64;

        let (_handle, mut reader, writer, mut rx, _cancel_token) = serve::<u64>(&manager).await;
        let _ = write_stream(
            writer,
            session_id,
            &[(0, 100u64), (1, 101u64), (3, 103u64)],
            true,
        )
        .await;
        assert_eq!(rx.recv().await, Some(100u64));
        assert_eq!(rx.recv().await, Some(101u64));
        let bytes = reader.next().await.unwrap().unwrap();
        let acked = deserialize_response(bytes).unwrap().into_ack().unwrap();
        assert_eq!(acked, 0);
        let bytes = reader.next().await.unwrap().unwrap();
        let acked = deserialize_response(bytes).unwrap().into_ack().unwrap();
        assert_eq!(acked, 1);
        let bytes = reader.next().await.unwrap().unwrap();
        assert!(deserialize_response(bytes).unwrap().is_reject());
    }

    #[async_timed_test(timeout_secs = 60)]
    // TODO: OSS: called `Result::unwrap()` on an `Err` value: Listen(Tcp([::1]:0), Os { code: 99, kind: AddrNotAvailable, message: "Cannot assign requested address" })
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_stop_net_tx_after_stopping_net_rx() {
        hyperactor_telemetry::initialize_logging_for_test();

        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::MESSAGE_DELIVERY_TIMEOUT, Duration::from_mins(5));
        let (addr, mut rx) = tcp::serve::<u64>("[::1]:0".parse().unwrap()).unwrap();
        let socket_addr = match addr {
            ChannelAddr::Tcp(a) => a,
            _ => panic!("unexpected channel type"),
        };
        let tx = tcp::dial::<u64>(socket_addr);
        // NetTx will not establish a connection until it sends the 1st message.
        // Without a live connection, NetTx cannot received the Closed message
        // from NetRx. Therefore, we need to send a message to establish the
        //connection.
        tx.send(100).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), 100);
        // Drop rx will close the NetRx server.
        rx.2.stop("testing");
        assert!(rx.recv().await.is_err());

        // NetTx will only read from the stream when it needs to send a message
        // or wait for an ack. Therefore we need to send a message to trigger that.
        tx.post(101);
        let mut watcher = tx.status().clone();
        // When NetRx exits, it should notify NetTx to exit as well.
        let _ = watcher.wait_for(|val| *val == TxStatus::Closed).await;
        // wait_for could return Err due to race between when watch's sender was
        // dropped and when wait_for was called. So we still need to do an
        // equality check.
        assert_eq!(*watcher.borrow(), TxStatus::Closed);
    }
}
