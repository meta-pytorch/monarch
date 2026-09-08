/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Drives one runtime-neutral QUIC endpoint and translates between application operations and
//! packet I/O.
//!
//! `EndpointHandle` is the application boundary. Its command and stream-submission handles feed
//! one bounded FIFO, and its completion queue carries command results, operation results, and
//! unsolicited lifecycle events back to the application. The queue types in
//! `chrysalis_transport_core` provide admission, correlation IDs, retained-byte accounting, and a
//! reserved terminal-completion slot for every accepted operation. This module preserves that
//! ownership while `Endpoint::poll` serializes all mutable protocol work on one driver thread.
//!
//! The layers below the endpoint have narrower responsibilities:
//!
//! - `Network` owns the connection table, connection-ID routes, QUIC timers, and conversion between
//!   quiche packets and `PacketIo` datagrams.
//! - `ConnectionState` owns per-stream ordering and the independent send and receive halves. It
//!   advances only streams that application submissions, quiche readiness, or released send
//!   buffers mark runnable.
//! - `QuicheBuffer` and `BufferLease` carry application send ownership into quiche. Quiche may split
//!   and retain those views for packetization and retransmission without copying the payload.
//! - `PacketIo` owns datagram slots and readiness. It does not interpret connections, streams, or
//!   application operations.
//!
//! A connection, one-byte send, and orderly teardown move through those layers as follows:
//!
//! 1. The application calls `EndpointCommands::try_connect`. Admission returns a `CommandReceipt`;
//!    it does not create or authenticate the connection. `Endpoint::drain_submissions` later sends
//!    the command to `Network::connect`, which allocates the local `ConnectionId`, creates the
//!    quiche state machine, and installs its CID route. A correlated
//!    `CommandResult::ConnectionCreated` reports that local allocation. `Network::queue_packets`,
//!    `PacketIo`, `Endpoint::process_io_events`, and `Network::receive` then drive the handshake.
//!    Only after quiche establishes TLS and `ConnectionState::progress` verifies the
//!    certificate-derived PID does the application receive `Completion::ConnectionEstablished`. A
//!    server follows the same packet and authentication path, but starts from an admitted Initial
//!    packet instead of a connect command.
//!
//! 2. The application calls `EndpointCommands::try_open_bidi` with the established connection.
//!    `Network::allocate_bidi` delegates to `ConnectionState`, which allocates a QUIC stream number
//!    and an empty `StreamState`. `CommandResult::StreamOpened` reports the resulting `StreamId`.
//!    Allocation is local: QUIC puts no stream on the wire until the first data or FIN reaches
//!    quiche. A peer-created bidirectional stream instead appears as `Completion::IncomingStream`
//!    when quiche reports it readable.
//!
//! 3. The application calls `SubmissionSender::try_send(stream, bytes)` with the one byte. Admission
//!    reserves queue space, one terminal completion, and the byte from the retained-send budget,
//!    then returns a `SubmissionReceipt`. The driver moves the accepted `SendSubmission` through
//!    `Network::enqueue` into the stream's ordered send queue. `ConnectionState::progress` passes a
//!    `QuicheBuffer` to `quiche::Connection::stream_send_zc`; `Network::queue_packets` later asks
//!    quiche for encrypted packets and submits them through `PacketIo`. Incoming acknowledgement
//!    packets travel back through `PacketIo` and `Network::receive` into quiche.
//!
//!    Quiche owns every split payload view needed for transmission or retransmission. When it no
//!    longer needs any view, dropping the final `QuicheBuffer` releases the retained-byte
//!    reservation; its final `BufferLease` drop also wakes the stream. The connection then emits
//!    `Completion::Send { outcome: SendOutcome::Acknowledged, .. }`. If the stream or connection
//!    fails first, the same ownership is released and the outcome is `Abandoned`. Send completions
//!    remain ordered even if quiche releases later buffers first.
//!
//! 4. To close the local send half normally, the application calls `SubmissionSender::try_finish`.
//!    The FIN is queued behind preceding sends. `Completion::Finish` reports when quiche accepts the
//!    FIN, which can occur before the preceding send's acknowledgement completion. It does not mean
//!    that the peer acknowledged the FIN or that the bidirectional stream is closed. The receive
//!    half becomes terminal when a posted receive observes peer FIN or reset, or when the application
//!    stops it. Once both halves are terminal and all queued operations and retained send views have
//!    returned, `ConnectionState` removes the stream and emits `Completion::Closed`. Reset and stop
//!    provide the corresponding abortive close for the send and receive halves.
//!
//! 5. The application calls `EndpointCommands::try_close` to close the connection.
//!    `Network::close` asks quiche to start an application close, and the correlated
//!    `CommandResult::CloseQueued` only confirms that local acceptance. Polling continues to emit and
//!    receive close packets until quiche reaches its terminal state or the driver abandons it.
//!    `Network::reap_closed` first returns every outstanding stream operation and buffer, then removes
//!    the connection and its routes, and finally emits `Completion::ConnectionClosed`. Thus the
//!    terminal connection event follows all operation completions produced while reclaiming that
//!    connection.
//!
//! `Endpoint::stage_completions` reconnects command and stream results with the completion permits
//! reserved at admission, and reserves separate event credit for unsolicited lifecycle events.
//! `Endpoint::flush_completions` preserves result order when the public completion queue is full, so
//! backpressure delays further protocol progress rather than dropping ownership or observable
//! lifecycle transitions.

use std::cmp;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use chrysalis_core::CID_LEN;
use chrysalis_core::ConnectionKey;
use chrysalis_core::Pid;
use chrysalis_core::RoutedCid;
use chrysalis_transport_core::AuthenticationFailed;
use chrysalis_transport_core::CommandCompletion;
use chrysalis_transport_core::CommandError;
use chrysalis_transport_core::CommandResult;
use chrysalis_transport_core::Completion;
use chrysalis_transport_core::CompletionCredits;
use chrysalis_transport_core::CompletionPermit;
use chrysalis_transport_core::CompletionQueue;
use chrysalis_transport_core::CompletionSender;
use chrysalis_transport_core::ConnectionEstablished as ConnectionEstablishedCompletion;
use chrysalis_transport_core::ConnectionId;
use chrysalis_transport_core::ControlOutcome;
use chrysalis_transport_core::DiscardSubmission;
use chrysalis_transport_core::DriverId;
use chrysalis_transport_core::EndpointSubmission;
use chrysalis_transport_core::EndpointSubmissionReceiver;
use chrysalis_transport_core::FinishSubmission;
use chrysalis_transport_core::Notifier;
use chrysalis_transport_core::OperationCancellation;
use chrysalis_transport_core::OperationId;
use chrysalis_transport_core::PostedBuffer;
use chrysalis_transport_core::ReceiveCompletion;
use chrysalis_transport_core::ReceiveOptions;
use chrysalis_transport_core::ReceiveStatus;
use chrysalis_transport_core::ReceiveSubmission;
use chrysalis_transport_core::ResetSubmission;
use chrysalis_transport_core::SendOutcome;
use chrysalis_transport_core::SendSubmission;
use chrysalis_transport_core::StopSubmission;
use chrysalis_transport_core::StreamId;
use chrysalis_transport_core::Submission;
use chrysalis_transport_core::SubmissionLimits;
use chrysalis_transport_core::SubmissionSender;
use chrysalis_transport_core::TryCompleteError;
use chrysalis_transport_core::completion_queue;
use chrysalis_transport_core::endpoint_submission_queue_with_credits;

use crate::Error;
use crate::admission::RetryTokens;
use crate::buffer::BufferFactory;
use crate::buffer::BufferLease;
use crate::buffer::QuicheBuffer;
use crate::buffer::SendCompletionSink;
use crate::buffer::SendState;
use crate::command::EndpointCommand;
use crate::command::EndpointCommands;
use crate::identity::EndpointIdentity;
use crate::identity::certificate_pid;
use crate::io::PacketIo;

mod connection;
mod network;

use self::network::Network;

const AUTHENTICATION_ERROR_CODE: u64 = 1;
const EVENT_COMPLETION_CAPACITY: usize = 4_096;
const STATISTICS_INTERVAL: Duration = Duration::from_millis(100);

struct PendingCompletion {
    completion: Completion,
    permit: CompletionPermit,
}

/// Application-side handles for one endpoint driver.
pub struct EndpointHandle {
    commands: EndpointCommands,
    submissions: SubmissionSender<EndpointCommand>,
    completions: CompletionQueue,
    connection_stats: ConnectionStatsHandle,
    endpoint_stats: EndpointStatsHandle,
}

impl EndpointHandle {
    /// Returns the nonblocking endpoint-control handle.
    pub fn commands(&self) -> &EndpointCommands {
        &self.commands
    }

    /// Returns the nonblocking stream-submission handle.
    pub fn submissions(&self) -> &SubmissionSender<EndpointCommand> {
        &self.submissions
    }

    /// Returns the application completion queue.
    pub fn completions(&self) -> &CompletionQueue {
        &self.completions
    }

    /// Returns a synchronous snapshot handle updated by the endpoint driver.
    pub fn connection_stats(&self) -> ConnectionStatsHandle {
        self.connection_stats.clone()
    }

    /// Returns endpoint-level admission, lifecycle, and error diagnostics.
    pub fn endpoint_stats(&self) -> EndpointStatsHandle {
        self.endpoint_stats.clone()
    }

    /// Separates the control, stream-submission, and completion handles for an adapter.
    pub fn into_parts(
        self,
    ) -> (
        EndpointCommands,
        SubmissionSender<EndpointCommand>,
        CompletionQueue,
    ) {
        (self.commands, self.submissions, self.completions)
    }
}

/// Cumulative quiche connection and active-path statistics.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectionStats {
    pub peer: Option<Pid>,
    pub transmit_datagrams: u64,
    pub transmit_bytes: u64,
    pub receive_datagrams: u64,
    pub receive_bytes: u64,
    pub rtt: Duration,
    pub congestion_window: u64,
    pub congestion_events: u64,
    pub lost_packets: u64,
    pub lost_bytes: u64,
    pub sent_packets: u64,
    pub current_mtu: u16,
    pub active_streams: u64,
    pub runnable_streams: u64,
    pub reclaimed_streams: u64,
}

/// Read-only connection statistics shared with runtime adapters.
#[derive(Clone, Debug, Default)]
pub struct ConnectionStatsHandle {
    inner: Arc<RwLock<HashMap<ConnectionId, ConnectionStats>>>,
}

impl ConnectionStatsHandle {
    /// Returns the latest snapshot for an authenticated peer.
    pub fn get_connection(&self, connection: ConnectionId) -> Option<ConnectionStats> {
        self.inner
            .read()
            .expect("connection statistics lock poisoned")
            .get(&connection)
            .copied()
    }

    /// Deliberately aggregates every current connection authenticated as `peer`.
    pub fn aggregate_peer(&self, peer: Pid) -> Option<ConnectionStats> {
        self.inner
            .read()
            .expect("connection statistics lock poisoned")
            .values()
            .filter(|stats| stats.peer == Some(peer))
            .copied()
            .reduce(aggregate_connection_stats)
    }

    /// Returns aggregate statistics for compatibility with PID-oriented callers.
    pub fn get(&self, peer: Pid) -> Option<ConnectionStats> {
        self.aggregate_peer(peer)
    }

    fn replace(&self, snapshots: HashMap<ConnectionId, ConnectionStats>) {
        *self
            .inner
            .write()
            .expect("connection statistics lock poisoned") = snapshots;
    }
}

fn aggregate_connection_stats(
    mut left: ConnectionStats,
    right: ConnectionStats,
) -> ConnectionStats {
    left.transmit_datagrams += right.transmit_datagrams;
    left.transmit_bytes += right.transmit_bytes;
    left.receive_datagrams += right.receive_datagrams;
    left.receive_bytes += right.receive_bytes;
    left.rtt = left.rtt.max(right.rtt);
    left.congestion_window += right.congestion_window;
    left.congestion_events += right.congestion_events;
    left.lost_packets += right.lost_packets;
    left.lost_bytes += right.lost_bytes;
    left.sent_packets += right.sent_packets;
    left.current_mtu = left.current_mtu.max(right.current_mtu);
    left.active_streams += right.active_streams;
    left.runnable_streams += right.runnable_streams;
    left.reclaimed_streams += right.reclaimed_streams;
    left
}

/// Endpoint-level admission and error diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EndpointStats {
    pub active_connections: u64,
    pub pending_handshakes: u64,
    pub active_streams: u64,
    pub runnable_streams: u64,
    pub reclaimed_streams: u64,
    pub completion_backlog: u64,
    pub parse_errors: u64,
    pub routing_errors: u64,
    pub cid_collisions: u64,
    pub authentication_failures: u64,
    pub quiche_receive_errors: u64,
    pub quiche_send_errors: u64,
    pub packet_io_errors: u64,
    pub admission_rejections: u64,
    pub invalid_retry_tokens: u64,
}

/// Read-only endpoint diagnostics sampled by the driver.
#[derive(Clone, Debug, Default)]
pub struct EndpointStatsHandle {
    inner: Arc<RwLock<EndpointStats>>,
}

impl EndpointStatsHandle {
    /// Returns the latest bounded-interval snapshot.
    pub fn snapshot(&self) -> EndpointStats {
        *self
            .inner
            .read()
            .expect("endpoint statistics lock poisoned")
    }

    fn replace(&self, stats: EndpointStats) {
        *self
            .inner
            .write()
            .expect("endpoint statistics lock poisoned") = stats;
    }
}

/// Observable lifecycle of one endpoint driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownState {
    /// The driver accepts submissions and advances connections.
    Running,
    /// Submission admission is closed while accepted ownership is returned.
    Draining,
    /// All accepted ownership and the terminal completion left the driver.
    Stopped,
}

/// Bounded connection and stream admission for one endpoint driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointLimits {
    max_connections: NonZeroUsize,
    max_pending_handshakes: NonZeroUsize,
    max_connections_per_source: NonZeroUsize,
    max_streams_per_connection: NonZeroUsize,
}

impl EndpointLimits {
    /// Constructs endpoint admission limits.
    pub const fn new(
        max_connections: NonZeroUsize,
        max_pending_handshakes: NonZeroUsize,
        max_connections_per_source: NonZeroUsize,
        max_streams_per_connection: NonZeroUsize,
    ) -> Self {
        Self {
            max_connections,
            max_pending_handshakes,
            max_connections_per_source,
            max_streams_per_connection,
        }
    }
}

impl Default for EndpointLimits {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(65_536).unwrap(),
            NonZeroUsize::new(4_096).unwrap(),
            NonZeroUsize::new(1_024).unwrap(),
            NonZeroUsize::new(65_536).unwrap(),
        )
    }
}

struct EndpointConfigs {
    client: Option<quiche::Config>,
    server: Option<quiche::Config>,
}

impl EndpointConfigs {
    fn client(config: quiche::Config) -> Self {
        Self {
            client: Some(config),
            server: None,
        }
    }

    fn server(config: quiche::Config) -> Self {
        Self {
            client: None,
            server: Some(config),
        }
    }

    fn duplex(client: quiche::Config, server: quiche::Config) -> Self {
        Self {
            client: Some(client),
            server: Some(server),
        }
    }
}

fn complete_unknown(submission: Submission, completions: &mut Vec<Completion>) {
    match submission {
        Submission::Send(submission) => {
            let (operation, stream, payload) = submission.into_parts();
            let bytes = payload.len();
            drop(payload);
            completions.push(Completion::Send {
                operation,
                stream,
                bytes,
                outcome: SendOutcome::Abandoned,
            });
        }
        Submission::Receive(submission) => {
            let (operation, stream, buffer, _, cancellation) = submission.into_parts();
            let initial_length = buffer.buffer().len();
            let status = if cancellation.complete() {
                ReceiveStatus::Closed
            } else {
                ReceiveStatus::Cancelled
            };
            completions.push(Completion::Receive(ReceiveCompletion::new(
                operation,
                stream,
                buffer,
                initial_length,
                status,
            )));
        }
        Submission::Finish(submission) => completions.push(Completion::Finish {
            operation: submission.operation(),
            stream: submission.stream(),
            outcome: ControlOutcome::Abandoned,
        }),
        Submission::Discard(submission) => completions.push(Completion::Discard {
            operation: submission.operation(),
            stream: submission.stream(),
            bytes: 0,
            status: ReceiveStatus::Closed,
        }),
        Submission::Reset(submission) => completions.push(Completion::Reset {
            operation: submission.operation(),
            stream: submission.stream(),
            outcome: ControlOutcome::Abandoned,
        }),
        Submission::Stop(submission) => completions.push(Completion::Stop {
            operation: submission.operation(),
            stream: submission.stream(),
            outcome: ControlOutcome::Abandoned,
        }),
    }
}

/// Single-owner quiche endpoint state machine.
pub struct Endpoint {
    driver: DriverId,
    io: Box<dyn PacketIo>,
    network: Network,
    submission_queue: EndpointSubmissionReceiver<EndpointCommand>,
    completion_sender: CompletionSender,
    pending_completions: VecDeque<PendingCompletion>,
    new_completions: Vec<Completion>,
    request_permits: HashMap<chrysalis_transport_core::RequestId, CompletionPermit>,
    operation_permits: HashMap<OperationId, CompletionPermit>,
    event_credits: CompletionCredits,
    shutdown_state: ShutdownState,
    shutdown_deadline: Option<Instant>,
    shutdown_close_started: bool,
    stopped_completion_staged: bool,
    connection_stats: ConnectionStatsHandle,
    endpoint_stats: EndpointStatsHandle,
    next_statistics_update: Instant,
}

impl Endpoint {
    /// Constructs a client endpoint and its thread-safe application handles.
    pub fn client<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        let routing_pid = identity.pid();
        Self::new(
            driver,
            io,
            identity,
            routing_pid,
            EndpointConfigs::client(config),
            EndpointLimits::default(),
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    /// Constructs a client endpoint with explicit admission limits.
    pub fn client_with_limits<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        config: quiche::Config,
        endpoint_limits: EndpointLimits,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        let routing_pid = identity.pid();
        Self::new(
            driver,
            io,
            identity,
            routing_pid,
            EndpointConfigs::client(config),
            endpoint_limits,
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    /// Constructs a server endpoint and its thread-safe application handles.
    pub fn server<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        let routing_pid = identity.pid();
        Self::new(
            driver,
            io,
            identity,
            routing_pid,
            EndpointConfigs::server(config),
            EndpointLimits::default(),
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    /// Constructs a server endpoint with explicit admission limits.
    pub fn server_with_limits<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        config: quiche::Config,
        endpoint_limits: EndpointLimits,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        let routing_pid = identity.pid();
        Self::new(
            driver,
            io,
            identity,
            routing_pid,
            EndpointConfigs::server(config),
            endpoint_limits,
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    /// Constructs a duplex endpoint that can initiate and accept connections.
    pub fn duplex<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        client_config: quiche::Config,
        server_config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        let routing_pid = identity.pid();
        Self::duplex_routed(
            driver,
            io,
            identity,
            routing_pid,
            client_config,
            server_config,
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    /// Constructs a duplex endpoint with explicit admission limits.
    pub fn duplex_with_limits<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        client_config: quiche::Config,
        server_config: quiche::Config,
        endpoint_limits: EndpointLimits,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        let routing_pid = identity.pid();
        Self::duplex_routed_with_limits(
            driver,
            io,
            identity,
            routing_pid,
            client_config,
            server_config,
            endpoint_limits,
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    /// Constructs a duplex endpoint whose CIDs use `routing_pid` independently of its identity.
    pub fn duplex_routed<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        routing_pid: Pid,
        client_config: quiche::Config,
        server_config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        Self::duplex_routed_with_limits(
            driver,
            io,
            identity,
            routing_pid,
            client_config,
            server_config,
            EndpointLimits::default(),
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    /// Constructs a routed duplex endpoint with explicit admission limits.
    pub fn duplex_routed_with_limits<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        routing_pid: Pid,
        client_config: quiche::Config,
        server_config: quiche::Config,
        endpoint_limits: EndpointLimits,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        Self::new(
            driver,
            io,
            identity,
            routing_pid,
            EndpointConfigs::duplex(client_config, server_config),
            endpoint_limits,
            submission_limits,
            completion_capacity,
            completion_notifier,
        )
    }

    fn new<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        _identity: EndpointIdentity,
        routing_pid: Pid,
        configs: EndpointConfigs,
        endpoint_limits: EndpointLimits,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
        completion_notifier: Arc<dyn Notifier>,
    ) -> (Self, EndpointHandle) {
        let notifier = io.notifier();
        let peer_addresses_validated = io.peer_addresses_validated();
        let operation_credits =
            CompletionCredits::new(submission_limits.queue_capacity(), notifier.clone());
        let event_credits = CompletionCredits::new(
            NonZeroUsize::new(EVENT_COMPLETION_CAPACITY).unwrap(),
            notifier.clone(),
        );
        let (command_sender, submission_sender, submission_queue) =
            endpoint_submission_queue_with_credits(
                driver,
                submission_limits,
                notifier.clone(),
                operation_credits.clone(),
            );
        let (completion_sender, completions) =
            completion_queue(completion_capacity, completion_notifier);
        let connection_stats = ConnectionStatsHandle::default();
        let endpoint_stats = EndpointStatsHandle::default();
        (
            Self {
                driver,
                io: Box::new(io),
                network: Network::new(
                    driver,
                    routing_pid,
                    configs,
                    endpoint_limits,
                    peer_addresses_validated,
                ),
                submission_queue,
                completion_sender,
                pending_completions: VecDeque::new(),
                new_completions: Vec::new(),
                request_permits: HashMap::new(),
                operation_permits: HashMap::new(),
                event_credits,
                shutdown_state: ShutdownState::Running,
                shutdown_deadline: None,
                shutdown_close_started: false,
                stopped_completion_staged: false,
                connection_stats: connection_stats.clone(),
                endpoint_stats: endpoint_stats.clone(),
                next_statistics_update: Instant::now(),
            },
            EndpointHandle {
                commands: EndpointCommands::new(command_sender),
                submissions: submission_sender,
                completions,
                connection_stats,
                endpoint_stats,
            },
        )
    }

    fn connect(
        &mut self,
        peer: std::net::SocketAddr,
        route: Pid,
        expected: Option<Pid>,
        server_name: &str,
    ) -> Result<ConnectionId, Error> {
        self.connect_from(peer, None, route, expected, server_name)
    }

    fn connect_from(
        &mut self,
        peer: std::net::SocketAddr,
        source_route: Option<Pid>,
        route: Pid,
        expected: Option<Pid>,
        server_name: &str,
    ) -> Result<ConnectionId, Error> {
        let local = self.io.local_addr()?;
        self.network
            .connect(local, peer, source_route, route, expected, server_name)
    }

    /// Stops accepting submissions and begins a bounded graceful QUIC shutdown.
    pub fn shutdown(&mut self, grace_period: Duration) {
        if self.shutdown_state != ShutdownState::Running {
            return;
        }
        self.submission_queue.close();
        self.drain_submissions();
        if self.shutdown_state == ShutdownState::Running {
            self.start_shutdown(grace_period);
        }
        self.stage_completions();
        self.flush_completions();
        self.finish_shutdown_if_ready();
    }

    /// Immediately abandons connections while still returning all accepted ownership.
    pub fn abort(&mut self) {
        if self.shutdown_state == ShutdownState::Stopped {
            return;
        }
        if self.shutdown_state == ShutdownState::Running {
            self.submission_queue.close();
            self.shutdown_state = ShutdownState::Draining;
        }
        self.shutdown_deadline = Some(Instant::now());
        self.drain_submissions();
        self.network.abandon_all(&mut self.new_completions);
        self.stage_completions();
        self.flush_completions();
        self.finish_shutdown_if_ready();
    }

    /// Returns the driver's current shutdown state.
    pub const fn shutdown_state(&self) -> ShutdownState {
        self.shutdown_state
    }

    /// Returns completions retained between the state machine and public queue.
    pub fn completion_backlog(&self) -> usize {
        self.pending_completions.len() + self.new_completions.len()
    }

    /// Advances submissions, QUIC state, packet I/O, timers, and completions, waiting for I/O for
    /// at most `maximum_wait`.
    pub fn poll(&mut self, maximum_wait: Duration) -> Result<(), Error> {
        self.flush_completions();
        if !self.stage_completions() {
            self.wait_for_io(maximum_wait)?;
            return Ok(());
        }
        self.finish_shutdown_if_ready();
        if self.shutdown_state == ShutdownState::Stopped {
            return Ok(());
        }
        self.drain_submissions();
        self.network.progress(&mut self.new_completions);
        if !self.stage_completions() {
            self.flush_completions();
            self.wait_for_io(maximum_wait)?;
            return Ok(());
        }
        self.flush_completions();
        self.network
            .queue_packets(self.io.as_mut(), &mut self.new_completions)?;

        let mut wait = self.network.next_timeout(maximum_wait);
        if let Some(deadline) = self.shutdown_deadline {
            wait = wait.min(deadline.saturating_duration_since(Instant::now()));
        }
        self.wait_for_io(wait)?;
        self.process_io_events()?;
        self.network.process_timeouts();
        self.network.progress(&mut self.new_completions);
        self.network.reap_closed(&mut self.new_completions);
        self.update_statistics();
        self.expire_shutdown();
        self.network
            .queue_packets(self.io.as_mut(), &mut self.new_completions)?;
        self.stage_completions();
        self.flush_completions();
        self.finish_shutdown_if_ready();
        Ok(())
    }

    fn drain_submissions(&mut self) {
        while let Some((_operation, stream)) = self.submission_queue.try_pop_cancellation() {
            self.network.wake_cancelled_receive(stream);
        }
        while let Some(submission) = self.submission_queue.try_pop() {
            match submission {
                EndpointSubmission::Command(submission) => self.process_command(submission),
                EndpointSubmission::Stream(admitted) => self.process_submission(admitted),
            }
        }
    }

    fn process_command(
        &mut self,
        submission: chrysalis_transport_core::CommandSubmission<EndpointCommand>,
    ) {
        let (request, command, permit) = submission.into_parts();
        assert!(self.request_permits.insert(request, permit).is_none());
        if self.shutdown_state != ShutdownState::Running {
            self.new_completions
                .push(Completion::Command(CommandCompletion::new(
                    request,
                    CommandResult::Failed(CommandError::DriverStopped),
                )));
            return;
        }

        let result = match command {
            EndpointCommand::Connect {
                source,
                route,
                expected,
                peer,
                server_name,
            } => match source {
                Some(source) => {
                    self.connect_from(peer, Some(source), route, expected, &server_name)
                }
                None => self.connect(peer, route, expected, &server_name),
            }
            .map(CommandResult::ConnectionCreated),
            EndpointCommand::OpenBidi { connection } => self
                .network
                .allocate_bidi(connection)
                .map(CommandResult::StreamOpened),
            EndpointCommand::Close {
                connection,
                error_code,
                reason,
            } => self
                .network
                .close(connection, error_code, &reason)
                .map(|()| CommandResult::CloseQueued(connection)),
            EndpointCommand::Shutdown { grace_period } => {
                self.start_shutdown(grace_period);
                Ok(CommandResult::ShutdownStarted)
            }
            EndpointCommand::Abort => {
                self.start_abort();
                Ok(CommandResult::AbortStarted)
            }
        }
        .unwrap_or_else(|error| CommandResult::Failed(command_error(&error)));
        self.new_completions
            .push(Completion::Command(CommandCompletion::new(request, result)));
    }

    fn start_shutdown(&mut self, grace_period: Duration) {
        self.submission_queue.close();
        self.shutdown_state = ShutdownState::Draining;
        self.shutdown_deadline = Instant::now().checked_add(grace_period);
        self.shutdown_close_started = false;
    }

    fn start_abort(&mut self) {
        self.submission_queue.close();
        self.shutdown_state = ShutdownState::Draining;
        self.shutdown_deadline = Some(Instant::now());
        self.shutdown_close_started = true;
        self.network.abandon_all(&mut self.new_completions);
    }

    fn process_submission(&mut self, admitted: chrysalis_transport_core::AdmittedSubmission) {
        let (submission, permit) = admitted.into_parts();
        assert!(
            self.operation_permits
                .insert(submission.operation(), permit)
                .is_none()
        );
        if self.shutdown_state == ShutdownState::Running {
            self.network.enqueue(submission, &mut self.new_completions);
        } else {
            complete_unknown(submission, &mut self.new_completions);
        }
    }

    fn expire_shutdown(&mut self) {
        if self.shutdown_state != ShutdownState::Draining {
            return;
        }
        if !self.shutdown_close_started
            && self.submission_queue.is_empty()
            && !self.network.has_pending_operations()
        {
            self.network.close_all(0, b"endpoint shutdown");
            self.shutdown_close_started = true;
        }
        if self
            .shutdown_deadline
            .is_none_or(|deadline| Instant::now() >= deadline)
        {
            self.network.abandon_all(&mut self.new_completions);
        }
    }

    fn finish_shutdown_if_ready(&mut self) {
        if self.shutdown_state != ShutdownState::Draining {
            return;
        }
        if !self.stopped_completion_staged
            && self.network.is_empty()
            && self.submission_queue.is_empty()
        {
            self.new_completions
                .push(Completion::DriverStopped(self.driver));
            self.stopped_completion_staged = true;
            self.stage_completions();
            self.flush_completions();
        }
        if self.stopped_completion_staged && self.completion_backlog() == 0 {
            self.shutdown_state = ShutdownState::Stopped;
        }
    }

    fn process_io_events(&mut self) -> Result<(), Error> {
        let network = &mut self.network;
        let mut retries = Vec::new();
        let receive_result = self.io.drain_received(&mut |packet, source, local| {
            // Parse, authentication, and connection failures are scoped to one datagram or
            // connection. The connection records any required close; unrelated connections and
            // later reconnect attempts must continue to make progress.
            if let Ok(Some(retry)) = network.receive(packet, source, local) {
                retries.push(retry);
            }
            Ok(())
        });
        if let Err(error) = receive_result {
            network.counters.packet_io_errors += 1;
            return Err(error);
        }
        for retry in retries {
            let Some(mut slot) = self.io.try_send_slot() else {
                break;
            };
            let buffer = slot.buffer_mut();
            if retry.bytes.len() > buffer.len() {
                continue;
            }
            buffer[..retry.bytes.len()].copy_from_slice(&retry.bytes);
            if let Err(error) = slot.submit(retry.bytes.len(), retry.destination, Instant::now()) {
                self.network.counters.packet_io_errors += 1;
                return Err(error.into());
            }
        }
        Ok(())
    }

    fn wait_for_io(&mut self, timeout: Duration) -> Result<(), Error> {
        if let Err(error) = self.io.poll(timeout) {
            self.network.counters.packet_io_errors += 1;
            return Err(error.into());
        }
        Ok(())
    }

    fn update_statistics(&mut self) {
        let now = Instant::now();
        if now < self.next_statistics_update {
            return;
        }
        self.connection_stats
            .replace(self.network.connection_stats());
        self.endpoint_stats
            .replace(self.network.endpoint_stats(self.completion_backlog()));
        self.next_statistics_update = now + STATISTICS_INTERVAL;
    }

    fn flush_completions(&mut self) {
        while let Some(pending) = self.pending_completions.pop_front() {
            match self
                .completion_sender
                .try_push(pending.completion, pending.permit)
            {
                Ok(()) => {}
                Err(TryCompleteError::Full { completion, permit }) => {
                    self.pending_completions
                        .push_front(PendingCompletion { completion, permit });
                    break;
                }
                Err(TryCompleteError::Closed { .. }) => {
                    self.pending_completions.clear();
                    break;
                }
            }
        }
    }

    fn stage_completions(&mut self) -> bool {
        let completions = std::mem::take(&mut self.new_completions);
        let mut completions = completions.into_iter();
        while let Some(completion) = completions.next() {
            let permit = match &completion {
                Completion::Command(command) => self
                    .request_permits
                    .remove(&command.request())
                    .expect("command completion should retain admission credit"),
                Completion::Send { operation, .. }
                | Completion::Finish { operation, .. }
                | Completion::Discard { operation, .. }
                | Completion::Reset { operation, .. }
                | Completion::Stop { operation, .. } => self
                    .operation_permits
                    .remove(operation)
                    .expect("operation completion should retain admission credit"),
                Completion::Receive(receive) => self
                    .operation_permits
                    .remove(&receive.operation())
                    .expect("receive completion should retain admission credit"),
                Completion::ConnectionEstablished(_)
                | Completion::AuthenticationFailed(_)
                | Completion::IncomingStream(_)
                | Completion::Closed { .. }
                | Completion::ConnectionClosed { .. }
                | Completion::DriverStopped(_) => {
                    let Some(permit) = self.event_credits.try_acquire() else {
                        self.new_completions.push(completion);
                        self.new_completions.extend(completions);
                        return false;
                    };
                    permit
                }
            };
            self.pending_completions
                .push_back(PendingCompletion { completion, permit });
        }
        true
    }
}

fn command_error(error: &Error) -> CommandError {
    match error {
        Error::WrongRole => CommandError::WrongRole,
        Error::UnknownConnection => CommandError::UnknownConnection,
        Error::StreamIdExhausted | Error::ConnectionKeyExhausted | Error::StreamLimit => {
            CommandError::InvalidArgument
        }
        Error::DriverStopped => CommandError::DriverStopped,
        Error::Io(_)
        | Error::Quiche(_)
        | Error::UnroutablePacket
        | Error::CidCollision
        | Error::AdmissionLimited
        | Error::InvalidRetryToken => CommandError::Transport,
    }
}

#[cfg(test)]
mod tests;
