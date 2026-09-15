/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Authenticated, PID-addressed quiche streams over a [`DatagramSocket`].

use std::collections::HashMap;
use std::ffi::CString;
use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use chrysalis_core::Pid;
use chrysalis_transport_core::DriverId;
use chrysalis_transport_core::ReceiveCompletion;
use chrysalis_transport_core::ReceiveOptions;
use chrysalis_transport_core::ReceiveStatus;
use chrysalis_transport_core::SubmissionLimits;
use chrysalis_transport_quiche::EndpointIdentity;
pub use chrysalis_transport_quiche::certificate_pid;
use chrysalis_transport_tokio::Connection;
use chrysalis_transport_tokio::Transport;
use chrysalis_transport_uring::DriverConfig;
use chrysalis_transport_uring::DriverStatsHandle;
use chrysalis_transport_uring::UdpDriver;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::Mutex as AsyncMutex;

use crate::DatagramAddr;
use crate::DatagramSocket;
use crate::packet_io::CarrierAddressBook;
use crate::packet_io::CarrierIoStats;
use crate::packet_io::CarrierPacketIo;
use crate::udp::decode_udp_addr;

const APPLICATION_PROTOCOL: &[u8] = b"chrysalis/1";
const DEFAULT_MAX_UDP_PAYLOAD_SIZE: u16 = 1_472;
const DEFAULT_MAX_TRANSMIT_BATCH_SEGMENTS: usize = 20;
const DEFAULT_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_FLOW_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_STREAM_LIMIT: u64 = 16_384;
const DEFAULT_QUEUE_CAPACITY: usize = 4_096;
const DEFAULT_RETAINED_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_COMPLETION_CAPACITY: usize = 8_192;
const STREAM_RECEIVE_CAPACITY: usize = 64 * 1024;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

static NEXT_DRIVER: AtomicU16 = AtomicU16::new(1);

/// Endpoint, flow-control, and packet-batching policy.
#[derive(Clone, Debug)]
pub struct QuicConfig {
    max_udp_payload_size: u16,
    max_transmit_batch_segments: NonZeroUsize,
    flow_window: u64,
    max_idle_timeout: Duration,
    pacing: bool,
    initial_congestion_window_packets: usize,
}

impl QuicConfig {
    /// Raises or lowers the maximum UDP payload accepted and advertised by the endpoint.
    pub fn try_with_max_udp_payload_size(mut self, value: u16) -> Result<Self, QuicConfigError> {
        if value < 1_200 {
            return Err(QuicConfigError::UdpPayloadTooSmall(value));
        }
        self.max_udp_payload_size = value;
        Ok(self)
    }

    /// Limits the datagrams assembled into one carrier transmission.
    pub fn with_max_transmit_batch_segments(mut self, value: NonZeroUsize) -> Self {
        self.max_transmit_batch_segments = value;
        self
    }

    /// Sets the connection and per-stream flow-control windows.
    pub fn with_flow_window(mut self, value: u64) -> Self {
        self.flow_window = value;
        self
    }

    /// Enables or disables quiche packet pacing.
    pub fn with_pacing(mut self, enabled: bool) -> Self {
        self.pacing = enabled;
        self
    }

    /// Sets the initial congestion window in packets.
    pub fn with_initial_congestion_window_packets(mut self, packets: usize) -> Self {
        self.initial_congestion_window_packets = packets;
        self
    }

    /// Sets the idle timeout negotiated by new connections.
    pub fn with_max_idle_timeout(mut self, timeout: Duration) -> Self {
        self.max_idle_timeout = timeout;
        self
    }

    fn build(&self, identity: &QuicIdentity) -> Result<quiche::Config, QuicTransportError> {
        let certificate = PemFile::new("chrysalis-certificate", &identity.certificate_chain)?;
        let private_key = PemFile::new("chrysalis-private-key", &identity.private_key)?;
        let trust_roots = PemFile::new("chrysalis-trust-roots", &identity.trust_roots)?;
        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
        config.set_application_protos(&[APPLICATION_PROTOCOL])?;
        config.load_cert_chain_from_pem_file(certificate.path())?;
        config.load_priv_key_from_pem_file(private_key.path())?;
        config.load_verify_locations_from_file(trust_roots.path())?;
        config.verify_peer(true);
        config.set_max_idle_timeout(
            self.max_idle_timeout
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        config.set_max_recv_udp_payload_size(usize::from(self.max_udp_payload_size));
        config.set_max_send_udp_payload_size(usize::from(self.max_udp_payload_size));
        config.set_initial_max_data(self.flow_window);
        config.set_initial_max_stream_data_bidi_local(self.flow_window);
        config.set_initial_max_stream_data_bidi_remote(self.flow_window);
        config.set_initial_max_streams_bidi(DEFAULT_STREAM_LIMIT);
        config.set_initial_max_streams_uni(0);
        config.set_max_connection_window(self.flow_window);
        config.set_max_stream_window(self.flow_window);
        config.set_disable_active_migration(true);
        config.enable_pacing(self.pacing);
        config.set_initial_congestion_window_packets(self.initial_congestion_window_packets);
        config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
        Ok(config)
    }
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            max_udp_payload_size: DEFAULT_MAX_UDP_PAYLOAD_SIZE,
            max_transmit_batch_segments: NonZeroUsize::new(DEFAULT_MAX_TRANSMIT_BATCH_SEGMENTS)
                .expect("default transmit segment limit is nonzero"),
            flow_window: DEFAULT_FLOW_WINDOW,
            max_idle_timeout: DEFAULT_MAX_IDLE_TIMEOUT,
            pacing: true,
            initial_congestion_window_packets: 10,
        }
    }
}

/// Invalid endpoint configuration.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum QuicConfigError {
    /// QUIC requires UDP payload support of at least 1,200 bytes.
    #[error("maximum UDP payload is below 1200 bytes: {0}")]
    UdpPayloadTooSmall(u16),
}

/// PEM-backed mutual TLS identity and trust policy.
#[derive(Clone)]
pub struct QuicIdentity {
    pid: Pid,
    leaf_certificate: Arc<[u8]>,
    certificate_chain: Arc<[u8]>,
    private_key: Arc<[u8]>,
    trust_roots: Arc<[u8]>,
    server_name: ServerName,
}

#[derive(Clone, Debug)]
enum ServerName {
    Fixed(Arc<str>),
    UdpDestinationIp { fallback: Arc<str> },
}

impl QuicIdentity {
    /// Constructs an identity from PEM credentials and its DER leaf certificate.
    pub fn new(
        leaf_certificate: &[u8],
        certificate_chain: impl Into<Arc<[u8]>>,
        private_key: impl Into<Arc<[u8]>>,
        trust_roots: impl Into<Arc<[u8]>>,
        server_name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            pid: certificate_pid(leaf_certificate),
            leaf_certificate: Arc::from(leaf_certificate),
            certificate_chain: certificate_chain.into(),
            private_key: private_key.into(),
            trust_roots: trust_roots.into(),
            server_name: ServerName::Fixed(server_name.into()),
        }
    }

    /// Verifies UDP peers against their destination IP, retaining the configured name elsewhere.
    pub fn with_udp_destination_server_name(mut self) -> Self {
        let fallback = match self.server_name {
            ServerName::Fixed(name) | ServerName::UdpDestinationIp { fallback: name } => name,
        };
        self.server_name = ServerName::UdpDestinationIp { fallback };
        self
    }

    /// Returns the self-certifying process ID.
    pub const fn pid(&self) -> Pid {
        self.pid
    }

    /// Returns the default TLS server name.
    pub fn certificate_server_name(&self) -> &str {
        match &self.server_name {
            ServerName::Fixed(name) | ServerName::UdpDestinationIp { fallback: name } => name,
        }
    }

    fn server_name_for(&self, address: &DatagramAddr) -> Result<Arc<str>, QuicTransportError> {
        match &self.server_name {
            ServerName::Fixed(name) => Ok(name.clone()),
            ServerName::UdpDestinationIp { fallback } if address.scheme() != "udp" => {
                Ok(fallback.clone())
            }
            ServerName::UdpDestinationIp { .. } => {
                let address =
                    decode_udp_addr(address).map_err(QuicTransportError::InvalidUdpDestination)?;
                Ok(address.ip().to_string().into())
            }
        }
    }
}

impl fmt::Debug for QuicIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuicIdentity")
            .field("pid", &self.pid)
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// Errors produced by the authenticated QUIC stream layer.
#[derive(Debug, Error)]
pub enum QuicTransportError {
    /// Packet I/O or in-memory credential loading failed.
    #[error("QUIC I/O failed: {0}")]
    Io(#[from] io::Error),
    /// quiche rejected configuration or protocol progress.
    #[error("QUIC protocol failed: {0}")]
    Quiche(#[from] quiche::Error),
    /// The completion driver rejected an operation.
    #[error(transparent)]
    Driver(#[from] chrysalis_transport_tokio::Error),
    /// Identity discovery was requested on a globally routed transport.
    #[error("unpinned dialing requires link-local CID routing")]
    UnpinnedGlobalDial,
    /// The transport no longer accepts streams.
    #[error("QUIC transport is closed")]
    Closed,
    /// A UDP destination could not supply its TLS server name.
    #[error("invalid UDP destination for TLS verification: {0}")]
    InvalidUdpDestination(#[source] io::Error),
}

/// Cumulative packet-I/O counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QuicIoStats {
    pub transmit_calls: u64,
    pub transmit_datagrams: u64,
    pub transmit_bytes: u64,
    pub transmit_blocked: u64,
    pub receive_calls: u64,
    pub receive_datagrams: u64,
    pub receive_bytes: u64,
}

impl QuicIoStats {
    /// Computes a saturating counter delta from an earlier snapshot.
    pub fn since(self, earlier: Self) -> Self {
        Self {
            transmit_calls: self.transmit_calls.saturating_sub(earlier.transmit_calls),
            transmit_datagrams: self
                .transmit_datagrams
                .saturating_sub(earlier.transmit_datagrams),
            transmit_bytes: self.transmit_bytes.saturating_sub(earlier.transmit_bytes),
            transmit_blocked: self
                .transmit_blocked
                .saturating_sub(earlier.transmit_blocked),
            receive_calls: self.receive_calls.saturating_sub(earlier.receive_calls),
            receive_datagrams: self
                .receive_datagrams
                .saturating_sub(earlier.receive_datagrams),
            receive_bytes: self.receive_bytes.saturating_sub(earlier.receive_bytes),
        }
    }
}

/// Snapshot of one pooled connection's QUIC state.
#[derive(Clone, Copy, Debug, Default)]
pub struct QuicConnectionStats {
    pub transmit_datagrams: u64,
    pub transmit_bytes: u64,
    pub transmit_ios: u64,
    pub receive_datagrams: u64,
    pub receive_bytes: u64,
    pub receive_ios: u64,
    pub rtt: Duration,
    pub congestion_window: u64,
    pub congestion_events: u64,
    pub lost_packets: u64,
    pub lost_bytes: u64,
    pub sent_packets: u64,
    pub current_mtu: u16,
}

impl QuicConnectionStats {
    /// Computes counter deltas while retaining the latest gauge values.
    pub fn since(self, earlier: Self) -> Self {
        Self {
            transmit_datagrams: self
                .transmit_datagrams
                .saturating_sub(earlier.transmit_datagrams),
            transmit_bytes: self.transmit_bytes.saturating_sub(earlier.transmit_bytes),
            transmit_ios: self.transmit_ios.saturating_sub(earlier.transmit_ios),
            receive_datagrams: self
                .receive_datagrams
                .saturating_sub(earlier.receive_datagrams),
            receive_bytes: self.receive_bytes.saturating_sub(earlier.receive_bytes),
            receive_ios: self.receive_ios.saturating_sub(earlier.receive_ios),
            rtt: self.rtt,
            congestion_window: self.congestion_window,
            congestion_events: self
                .congestion_events
                .saturating_sub(earlier.congestion_events),
            lost_packets: self.lost_packets.saturating_sub(earlier.lost_packets),
            lost_bytes: self.lost_bytes.saturating_sub(earlier.lost_bytes),
            sent_packets: self.sent_packets.saturating_sub(earlier.sent_packets),
            current_mtu: self.current_mtu,
        }
    }
}

/// One completion-driven bidirectional stream.
pub struct Stream {
    send: SendStream,
    recv: RecvStream,
}

impl Stream {
    fn new(stream: chrysalis_transport_tokio::Stream) -> Self {
        Self {
            send: SendStream::new(stream.clone()),
            recv: RecvStream::new(stream),
        }
    }

    /// Returns the send half.
    pub const fn send(&self) -> &SendStream {
        &self.send
    }

    /// Returns the mutable send half.
    pub fn send_mut(&mut self) -> &mut SendStream {
        &mut self.send
    }

    /// Returns the receive half.
    pub const fn recv(&self) -> &RecvStream {
        &self.recv
    }

    /// Returns the mutable receive half.
    pub fn recv_mut(&mut self) -> &mut RecvStream {
        &mut self.recv
    }

    /// Separates independently owned send and receive halves.
    pub fn into_parts(self) -> (SendStream, RecvStream) {
        (self.send, self.recv)
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Stream").finish_non_exhaustive()
    }
}

type WriteFuture = Pin<Box<dyn FutureResult + Send>>;
trait FutureResult: std::future::Future<Output = Result<(), chrysalis_transport_tokio::Error>> {}
impl<T> FutureResult for T where
    T: std::future::Future<Output = Result<(), chrysalis_transport_tokio::Error>>
{
}

/// Owned send half with native `Bytes` and Tokio compatibility APIs.
pub struct SendStream {
    inner: chrysalis_transport_tokio::Stream,
    pending_write: Option<(usize, WriteFuture)>,
    pending_finish: Option<WriteFuture>,
}

impl SendStream {
    fn new(inner: chrysalis_transport_tokio::Stream) -> Self {
        Self {
            inner,
            pending_write: None,
            pending_finish: None,
        }
    }

    /// Sends owned immutable bytes and waits for acknowledgement.
    pub fn send(
        &self,
        bytes: Bytes,
    ) -> impl Future<Output = Result<(), QuicTransportError>> + Send + 'static {
        let stream = self.inner.clone();
        async move { Ok(stream.send(bytes).await?) }
    }

    /// Finishes this stream's send half.
    pub fn finish(&self) -> impl Future<Output = Result<(), QuicTransportError>> + Send + 'static {
        let stream = self.inner.clone();
        async move { Ok(stream.finish().await?) }
    }

    fn poll_pending_write(&mut self, context: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let Some((length, future)) = &mut self.pending_write else {
            return Poll::Ready(Ok(0));
        };
        match future.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let length = *length;
                self.pending_write = None;
                Poll::Ready(Ok(length))
            }
            Poll::Ready(Err(error)) => {
                self.pending_write = None;
                Poll::Ready(Err(driver_io_error(error)))
            }
        }
    }
}

impl AsyncWrite for SendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.pending_write.is_none() {
            if buffer.is_empty() {
                return Poll::Ready(Ok(0));
            }
            let length = buffer.len();
            let stream = self.inner.clone();
            let bytes = Bytes::copy_from_slice(buffer);
            self.pending_write = Some((length, Box::pin(async move { stream.send(bytes).await })));
        }
        self.poll_pending_write(context)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_pending_write(context) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_pending_write(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(_)) => {}
        }
        if self.pending_finish.is_none() {
            let stream = self.inner.clone();
            self.pending_finish = Some(Box::pin(async move { stream.finish().await }));
        }
        let future = self.pending_finish.as_mut().unwrap();
        match future.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.pending_finish = None;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.pending_finish = None;
                Poll::Ready(Err(driver_io_error(error)))
            }
        }
    }
}

type ReceiveFuture = Pin<
    Box<
        dyn std::future::Future<
                Output = Result<ReceiveCompletion, chrysalis_transport_tokio::Error>,
            > + Send,
    >,
>;

/// Owned receive half with caller-allocated and Tokio compatibility APIs.
pub struct RecvStream {
    inner: chrysalis_transport_tokio::Stream,
    pending: Option<ReceiveFuture>,
    buffered: BytesMut,
    offset: usize,
    eof: bool,
}

impl RecvStream {
    fn new(inner: chrysalis_transport_tokio::Stream) -> Self {
        Self {
            inner,
            pending: None,
            buffered: BytesMut::new(),
            offset: 0,
            eof: false,
        }
    }

    /// Receives into a caller-owned allocation.
    pub fn receive(
        &self,
        buffer: BytesMut,
        options: ReceiveOptions,
    ) -> impl Future<Output = Result<ReceiveCompletion, QuicTransportError>> + Send + 'static {
        let stream = self.inner.clone();
        async move { Ok(stream.receive(buffer, options).await?) }
    }

    /// Discards received bytes without copying them into an application buffer.
    pub fn discard(
        &self,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = Result<(usize, ReceiveStatus), QuicTransportError>> + Send + 'static
    {
        let stream = self.inner.clone();
        async move { Ok(stream.discard(max_bytes).await?) }
    }
}

impl AsyncRead for RecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.offset < self.buffered.len() {
                let count = output
                    .remaining()
                    .min(self.buffered.len().saturating_sub(self.offset));
                output.put_slice(&self.buffered[self.offset..self.offset + count]);
                self.offset += count;
                return Poll::Ready(Ok(()));
            }
            if self.eof {
                return Poll::Ready(Ok(()));
            }
            if self.pending.is_none() {
                let stream = self.inner.clone();
                self.pending = Some(Box::pin(async move {
                    stream
                        .receive(
                            BytesMut::with_capacity(STREAM_RECEIVE_CAPACITY),
                            ReceiveOptions::default(),
                        )
                        .await
                }));
            }
            let future = self.pending.as_mut().unwrap();
            let completion = match future.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(completion)) => completion,
                Poll::Ready(Err(error)) => {
                    self.pending = None;
                    return Poll::Ready(Err(driver_io_error(error)));
                }
            };
            self.pending = None;
            self.eof = match completion.status() {
                ReceiveStatus::Data | ReceiveStatus::Cancelled => false,
                ReceiveStatus::Fin | ReceiveStatus::Closed | ReceiveStatus::Stopped(_) => true,
                ReceiveStatus::Reset(code) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        format!("QUIC stream reset: {code}"),
                    )));
                }
            };
            self.buffered = completion.into_buffer();
            self.offset = 0;
        }
    }
}

/// An authenticated incoming stream.
pub struct IncomingStream {
    source: Pid,
    stream: Stream,
}

impl IncomingStream {
    /// Returns the certificate-derived source PID.
    pub const fn source(&self) -> Pid {
        self.source
    }

    /// Returns the stream.
    pub const fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Returns the mutable stream.
    pub fn stream_mut(&mut self) -> &mut Stream {
        &mut self.stream
    }

    /// Separates the source PID and stream.
    pub fn into_parts(self) -> (Pid, Stream) {
        (self.source, self.stream)
    }
}

enum PeerAddresses {
    Carrier(CarrierAddressBook),
    DirectUdp,
}

enum PacketIoStats {
    Carrier(Arc<CarrierIoStats>),
    DirectUdp(DriverStatsHandle),
}

impl PeerAddresses {
    fn register(&self, address: DatagramAddr) -> Result<std::net::SocketAddr, QuicTransportError> {
        match self {
            Self::Carrier(addresses) => Ok(addresses.register(address)),
            Self::DirectUdp => {
                decode_udp_addr(&address).map_err(QuicTransportError::InvalidUdpDestination)
            }
        }
    }
}

/// Connection-pooled QUIC transport over one carrier binding.
pub struct QuicTransport<T: DatagramSocket> {
    pid: Pid,
    link_local: bool,
    identity: QuicIdentity,
    addresses: PeerAddresses,
    endpoint: Arc<Transport>,
    connections: AsyncMutex<HashMap<Pid, Connection>>,
    link_local_connections: AsyncMutex<HashMap<Pid, Connection>>,
    shutdown: AtomicBool,
    io_stats: PacketIoStats,
    _socket: PhantomData<fn() -> T>,
}

impl<T: DatagramSocket> QuicTransport<T> {
    /// Starts a globally routed transport with default policy.
    pub fn spawn(socket: Arc<T>, identity: QuicIdentity) -> Result<Self, QuicTransportError> {
        Self::spawn_with_config(socket, identity, QuicConfig::default())
    }

    /// Starts a globally routed transport.
    pub fn spawn_with_config(
        socket: Arc<T>,
        identity: QuicIdentity,
        config: QuicConfig,
    ) -> Result<Self, QuicTransportError> {
        Self::spawn_inner(socket, identity, config, false)
    }

    /// Starts an adjacent-link transport whose CIDs terminate at PID 0.
    pub fn spawn_link_local(
        socket: Arc<T>,
        identity: QuicIdentity,
    ) -> Result<Self, QuicTransportError> {
        Self::spawn_inner(socket, identity, QuicConfig::default(), true)
    }

    fn spawn_inner(
        socket: Arc<T>,
        identity: QuicIdentity,
        config: QuicConfig,
        link_local: bool,
    ) -> Result<Self, QuicTransportError> {
        let (io, addresses, io_stats) = CarrierPacketIo::new(
            socket,
            usize::from(config.max_udp_payload_size),
            config.max_transmit_batch_segments.get(),
        );
        let endpoint_identity = EndpointIdentity::from_leaf_certificate(&identity.leaf_certificate);
        let driver = DriverId::from_u16(NEXT_DRIVER.fetch_add(1, Ordering::Relaxed));
        let limits = SubmissionLimits::new(
            NonZeroUsize::new(DEFAULT_QUEUE_CAPACITY).unwrap(),
            NonZeroUsize::new(DEFAULT_RETAINED_BYTES).unwrap(),
            NonZeroUsize::new(DEFAULT_RETAINED_BYTES).unwrap(),
        );
        let completion_capacity = NonZeroUsize::new(DEFAULT_COMPLETION_CAPACITY).unwrap();
        let client = config.build(&identity)?;
        let server = config.build(&identity)?;
        let routing_pid = if link_local {
            Pid::LINK_LOCAL
        } else {
            identity.pid()
        };
        let endpoint = Transport::spawn_duplex_routed(
            driver,
            io,
            endpoint_identity,
            routing_pid,
            client,
            server,
            limits,
            completion_capacity,
        )?;
        Ok(Self {
            pid: identity.pid(),
            link_local,
            identity,
            addresses: PeerAddresses::Carrier(addresses),
            endpoint: Arc::new(endpoint),
            connections: AsyncMutex::new(HashMap::new()),
            link_local_connections: AsyncMutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            io_stats: PacketIoStats::Carrier(io_stats),
            _socket: PhantomData,
        })
    }

    /// Returns the authenticated local PID.
    pub const fn pid(&self) -> Pid {
        self.pid
    }

    /// Returns cumulative packet-I/O counters.
    pub fn io_stats(&self) -> QuicIoStats {
        match &self.io_stats {
            PacketIoStats::Carrier(io_stats) => {
                let stats = io_stats.snapshot();
                QuicIoStats {
                    transmit_calls: stats.transmit_calls,
                    transmit_datagrams: stats.transmit_datagrams,
                    transmit_bytes: stats.transmit_bytes,
                    transmit_blocked: stats.transmit_blocked,
                    receive_calls: stats.receive_calls,
                    receive_datagrams: stats.receive_datagrams,
                    receive_bytes: stats.receive_bytes,
                }
            }
            PacketIoStats::DirectUdp(io_stats) => {
                let stats = io_stats.snapshot();
                QuicIoStats {
                    transmit_calls: stats.sends_completed,
                    transmit_datagrams: stats.datagrams_sent,
                    transmit_bytes: stats.bytes_sent,
                    transmit_blocked: 0,
                    receive_calls: stats.receives_completed,
                    receive_datagrams: stats.datagrams_received,
                    receive_bytes: stats.bytes_received,
                }
            }
        }
    }

    /// Returns a pooled connection snapshot when available.
    pub fn connection_stats(&self, target: Pid) -> Option<QuicConnectionStats> {
        self.endpoint
            .connection_stats(target)
            .map(|stats| QuicConnectionStats {
                transmit_datagrams: stats.transmit_datagrams,
                transmit_bytes: stats.transmit_bytes,
                transmit_ios: 0,
                receive_datagrams: stats.receive_datagrams,
                receive_bytes: stats.receive_bytes,
                receive_ios: 0,
                rtt: stats.rtt,
                congestion_window: stats.congestion_window,
                congestion_events: stats.congestion_events,
                lost_packets: stats.lost_packets,
                lost_bytes: stats.lost_bytes,
                sent_packets: stats.sent_packets,
                current_mtu: stats.current_mtu,
            })
    }

    /// Opens a stream using the identity's server-name policy.
    pub async fn dial(
        &self,
        target: Pid,
        address: DatagramAddr,
    ) -> Result<Stream, QuicTransportError> {
        let server_name = self.identity.server_name_for(&address)?;
        self.dial_with_server_name(target, address, server_name)
            .await
    }

    /// Opens a stream with an explicit TLS server name.
    pub async fn dial_with_server_name(
        &self,
        target: Pid,
        address: DatagramAddr,
        server_name: impl Into<Arc<str>>,
    ) -> Result<Stream, QuicTransportError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(QuicTransportError::Closed);
        }
        if let Some(connection) = self.connections.lock().await.get(&target).cloned()
            && let Ok(stream) = connection.open_stream().await
        {
            return Ok(Stream::new(stream));
        }
        let peer = self.addresses.register(address)?;
        let server_name = server_name.into();
        let connection = if self.link_local {
            self.endpoint
                .connect_routed(Pid::LINK_LOCAL, target, peer, server_name.as_ref())
                .await?
        } else {
            self.endpoint
                .connect(target, peer, server_name.as_ref())
                .await?
        };
        let stream = connection.open_stream().await?;
        self.connections.lock().await.insert(target, connection);
        Ok(Stream::new(stream))
    }

    /// Opens a PID 0 stream to an authenticated adjacent process.
    pub(crate) async fn dial_link_local(
        &self,
        target: Pid,
        address: DatagramAddr,
    ) -> Result<Stream, QuicTransportError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(QuicTransportError::Closed);
        }
        if let Some(connection) = self
            .link_local_connections
            .lock()
            .await
            .get(&target)
            .cloned()
            && let Ok(stream) = connection.open_stream().await
        {
            return Ok(Stream::new(stream));
        }
        let server_name = self.identity.server_name_for(&address)?;
        let peer = self.addresses.register(address)?;
        let connection = self
            .endpoint
            .connect_from(
                Pid::LINK_LOCAL,
                Pid::LINK_LOCAL,
                Some(target),
                peer,
                server_name.as_ref(),
            )
            .await?;
        let stream = connection.open_stream().await?;
        self.link_local_connections
            .lock()
            .await
            .insert(target, connection);
        Ok(Stream::new(stream))
    }

    pub(crate) async fn dial_link_local_unpinned(
        &self,
        address: DatagramAddr,
    ) -> Result<(Pid, Stream), QuicTransportError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(QuicTransportError::Closed);
        }
        let server_name = self.identity.server_name_for(&address)?;
        let peer = self.addresses.register(address)?;
        let connection = self
            .endpoint
            .connect_from(
                Pid::LINK_LOCAL,
                Pid::LINK_LOCAL,
                None,
                peer,
                server_name.as_ref(),
            )
            .await?;
        let source = connection.peer();
        let stream = connection.open_stream().await?;
        self.link_local_connections
            .lock()
            .await
            .insert(source, connection);
        Ok((source, Stream::new(stream)))
    }

    /// Accepts the next authenticated incoming stream.
    pub async fn accept(&self) -> Result<IncomingStream, QuicTransportError> {
        let incoming = self.endpoint.accept().await?;
        let (source, stream) = incoming.into_parts();
        Ok(IncomingStream {
            source,
            stream: Stream::new(stream),
        })
    }

    /// Accepts the next authenticated stream terminating at link-local PID 0.
    pub(crate) async fn accept_link_local(&self) -> Result<IncomingStream, QuicTransportError> {
        let incoming = self.endpoint.accept_link_local().await?;
        let (source, stream) = incoming.into_parts();
        Ok(IncomingStream {
            source,
            stream: Stream::new(stream),
        })
    }

    /// Idempotently requests transport shutdown.
    pub fn shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            let endpoint = self.endpoint.clone();
            tokio::spawn(async move {
                let _ = endpoint.shutdown(SHUTDOWN_GRACE).await;
            });
        }
    }

    /// Waits for the endpoint thread and completion pump.
    pub async fn join(&self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = self.endpoint.shutdown(SHUTDOWN_GRACE).await;
        let _ = self.endpoint.join().await;
    }
}

impl QuicTransport<crate::SwitchSocket> {
    /// Starts a globally routed transport that owns a direct io_uring UDP driver.
    pub fn spawn_direct_udp_with_config(
        socket: std::net::UdpSocket,
        identity: QuicIdentity,
        config: QuicConfig,
    ) -> Result<Self, QuicTransportError> {
        let segment_size = NonZeroUsize::new(usize::from(config.max_udp_payload_size))
            .expect("QUIC UDP payload size is nonzero");
        let maximum_segments = (u16::MAX as usize / segment_size.get()).max(1);
        let max_segments = NonZeroUsize::new(
            config
                .max_transmit_batch_segments
                .get()
                .min(maximum_segments),
        )
        .expect("direct UDP transmit segment count is nonzero");
        let io = UdpDriver::new(
            socket,
            DriverConfig::new(
                NonZeroU32::new(256).expect("direct UDP ring depth is nonzero"),
                NonZeroUsize::new(64).expect("direct UDP receive depth is nonzero"),
                segment_size,
                max_segments,
                NonZeroUsize::new(64 * 1024 * 1024)
                    .expect("direct UDP socket buffer size is nonzero"),
                true,
            ),
        )?;
        let io_stats = io.stats_handle();
        let endpoint_identity = EndpointIdentity::from_leaf_certificate(&identity.leaf_certificate);
        let driver = DriverId::from_u16(NEXT_DRIVER.fetch_add(1, Ordering::Relaxed));
        let limits = SubmissionLimits::new(
            NonZeroUsize::new(DEFAULT_QUEUE_CAPACITY).unwrap(),
            NonZeroUsize::new(DEFAULT_RETAINED_BYTES).unwrap(),
            NonZeroUsize::new(DEFAULT_RETAINED_BYTES).unwrap(),
        );
        let completion_capacity = NonZeroUsize::new(DEFAULT_COMPLETION_CAPACITY).unwrap();
        let client = config.build(&identity)?;
        let server = config.build(&identity)?;
        let endpoint = Transport::spawn_duplex(
            driver,
            io,
            endpoint_identity,
            client,
            server,
            limits,
            completion_capacity,
        )?;
        Ok(Self {
            pid: identity.pid(),
            link_local: false,
            identity,
            addresses: PeerAddresses::DirectUdp,
            endpoint: Arc::new(endpoint),
            connections: AsyncMutex::new(HashMap::new()),
            link_local_connections: AsyncMutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            io_stats: PacketIoStats::DirectUdp(io_stats),
            _socket: PhantomData,
        })
    }
}

impl<T: DatagramSocket> fmt::Debug for QuicTransport<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuicTransport")
            .field("pid", &self.pid)
            .field("link_local", &self.link_local)
            .finish_non_exhaustive()
    }
}

impl<T: DatagramSocket> Drop for QuicTransport<T> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct PemFile {
    file: File,
    path: String,
}

impl PemFile {
    fn new(name: &str, contents: &[u8]) -> io::Result<Self> {
        let name = CString::new(name).expect("PEM memfd name contains no NUL");
        // SAFETY: name is NUL-terminated and flags contain no unsupported bits.
        let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: memfd_create returned a fresh descriptor whose ownership transfers here.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let mut file = File::from(fd);
        file.write_all(contents)?;
        let path = format!("/proc/self/fd/{}", file.as_raw_fd());
        Ok(Self { file, path })
    }

    fn path(&self) -> &str {
        let _keep_alive = &self.file;
        &self.path
    }
}

fn driver_io_error(error: chrysalis_transport_tokio::Error) -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, error)
}
