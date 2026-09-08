/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tokio futures over the runtime-neutral Chrysalis completion driver.
//!
//! The network driver remains a synchronous, single-owner state machine on a dedicated OS
//! thread. This crate only maps bounded commands and completions to Tokio futures. Sending keeps
//! `Bytes` ownership in the transport until acknowledgement; receiving posts caller-owned
//! `BytesMut` allocations and returns the same allocation in a completion.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use chrysalis_core::Pid;
use chrysalis_transport_core::AuthenticationFailed;
use chrysalis_transport_core::CommandError;
use chrysalis_transport_core::CommandResult;
use chrysalis_transport_core::Completion;
use chrysalis_transport_core::CompletionQueue;
use chrysalis_transport_core::ConnectionId;
use chrysalis_transport_core::ControlOutcome;
use chrysalis_transport_core::DriverId;
use chrysalis_transport_core::LeasedCompletion;
use chrysalis_transport_core::Notifier;
use chrysalis_transport_core::OperationCancellation;
use chrysalis_transport_core::OperationId;
use chrysalis_transport_core::ReceiveCompletion;
use chrysalis_transport_core::ReceiveOptions;
use chrysalis_transport_core::ReceiveStatus;
use chrysalis_transport_core::RequestId;
use chrysalis_transport_core::SendOutcome;
use chrysalis_transport_core::StreamId;
use chrysalis_transport_core::SubmissionLimits;
use chrysalis_transport_core::SubmissionSender;
use chrysalis_transport_core::TryCommandError;
use chrysalis_transport_core::TryControlError;
use chrysalis_transport_core::TryReceiveError;
use chrysalis_transport_core::TrySendError;
use chrysalis_transport_quiche::ConnectionStats;
use chrysalis_transport_quiche::ConnectionStatsHandle;
use chrysalis_transport_quiche::Endpoint;
use chrysalis_transport_quiche::EndpointCommand;
use chrysalis_transport_quiche::EndpointCommands;
use chrysalis_transport_quiche::EndpointHandle;
use chrysalis_transport_quiche::EndpointIdentity;
use chrysalis_transport_quiche::EndpointStats;
use chrysalis_transport_quiche::EndpointStatsHandle;
use chrysalis_transport_quiche::PacketIo;
use chrysalis_transport_quiche::ShutdownState;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle as TokioJoinHandle;

/// Failure from a Tokio transport command, operation, or driver lifecycle.
#[derive(Debug)]
pub enum Error {
    /// The driver thread could not be created.
    Spawn(io::Error),
    /// A command was rejected by the driver.
    Command(CommandError),
    /// The requested process did not authenticate as the expected PID.
    Authentication {
        /// PID requested by the caller.
        expected: Option<Pid>,
        /// PID derived from the peer certificate, if one was presented.
        actual: Option<Pid>,
    },
    /// The connection closed before it became usable.
    ConnectionClosed(ConnectionId),
    /// A send was abandoned before acknowledgement.
    SendAbandoned(StreamId),
    /// A send was submitted after the stream send half began finishing.
    SendRejected(StreamId),
    /// A local FIN was abandoned.
    FinishAbandoned(StreamId),
    /// A local FIN was submitted after the stream send half became terminal.
    FinishRejected(StreamId),
    /// A reset-send operation was abandoned.
    ResetAbandoned(StreamId),
    /// A reset-send operation was rejected.
    ResetRejected(StreamId),
    /// A stop-receiving operation was abandoned.
    StopAbandoned(StreamId),
    /// A stop-receiving operation was rejected.
    StopRejected(StreamId),
    /// The completion pump stopped before delivering an accepted operation.
    CompletionStopped,
    /// The dedicated driver returned an error.
    Driver(String),
    /// The dedicated driver thread panicked.
    DriverPanicked,
    /// The completion pump task failed.
    CompletionTask(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn(error) => write!(formatter, "spawn transport driver: {error}"),
            Self::Command(error) => write!(formatter, "transport command failed: {error:?}"),
            Self::Authentication { expected, actual } => {
                write!(
                    formatter,
                    "peer authentication failed: expected {expected:?}, got {actual:?}"
                )
            }
            Self::ConnectionClosed(connection) => {
                write!(
                    formatter,
                    "connection closed before establishment: {connection:?}"
                )
            }
            Self::SendAbandoned(stream) => {
                write!(formatter, "send was abandoned on {stream:?}")
            }
            Self::SendRejected(stream) => {
                write!(formatter, "send was rejected on {stream:?}")
            }
            Self::FinishAbandoned(stream) => {
                write!(formatter, "finish was abandoned on {stream:?}")
            }
            Self::FinishRejected(stream) => {
                write!(formatter, "finish was rejected on {stream:?}")
            }
            Self::ResetAbandoned(stream) => {
                write!(formatter, "reset was abandoned on {stream:?}")
            }
            Self::ResetRejected(stream) => write!(formatter, "reset was rejected on {stream:?}"),
            Self::StopAbandoned(stream) => {
                write!(formatter, "stop was abandoned on {stream:?}")
            }
            Self::StopRejected(stream) => write!(formatter, "stop was rejected on {stream:?}"),
            Self::CompletionStopped => formatter.write_str("transport completion pump stopped"),
            Self::Driver(error) => write!(formatter, "transport driver failed: {error}"),
            Self::DriverPanicked => formatter.write_str("transport driver thread panicked"),
            Self::CompletionTask(error) => write!(formatter, "completion task failed: {error}"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Default)]
struct TokioNotifier {
    notify: Notify,
}

impl Notifier for TokioNotifier {
    fn notify(&self) {
        self.notify.notify_one();
    }
}

struct Slots<K, V> {
    ready: HashMap<K, V>,
    waiters: HashMap<K, oneshot::Sender<V>>,
}

enum SlotRegistration<V> {
    Ready(V),
    Pending(oneshot::Receiver<V>),
    Stopped,
}

impl<K, V> Slots<K, V>
where
    K: Copy + Eq + Hash,
{
    fn new() -> Self {
        Self {
            ready: HashMap::new(),
            waiters: HashMap::new(),
        }
    }

    fn deliver(&mut self, key: K, value: V) {
        if let Some(waiter) = self.waiters.remove(&key) {
            let _ = waiter.send(value);
        } else {
            self.ready.insert(key, value);
        }
    }

    fn register(&mut self, key: K, stopped: bool) -> SlotRegistration<V> {
        if let Some(value) = self.ready.remove(&key) {
            return SlotRegistration::Ready(value);
        }
        if stopped {
            return SlotRegistration::Stopped;
        }
        let (sender, receiver) = oneshot::channel();
        assert!(
            self.waiters.insert(key, sender).is_none(),
            "one future may wait for a completion ID"
        );
        SlotRegistration::Pending(receiver)
    }
}

#[derive(Clone, Copy, Debug)]
enum EstablishmentFailure {
    Authentication {
        expected: Option<Pid>,
        actual: Option<Pid>,
    },
    Closed(ConnectionId),
    CompletionStopped,
}

struct CompletionState {
    requests: Mutex<Slots<RequestId, Result<CommandResult, ()>>>,
    operations: Mutex<Slots<OperationId, Result<Completion, ()>>>,
    establishments: Mutex<Slots<ConnectionId, Result<Pid, EstablishmentFailure>>>,
    outbound: Mutex<HashSet<ConnectionId>>,
    peers: Mutex<HashMap<ConnectionId, (Pid, Pid)>>,
    incoming: Mutex<Option<mpsc::Sender<AcceptedStream>>>,
    link_local_incoming: Mutex<Option<mpsc::Sender<AcceptedStream>>>,
    driver_error: Mutex<Option<String>>,
    acceptance_stopped: AtomicBool,
    acceptance_notify: Notify,
    stopped: AtomicBool,
}

struct AcceptedStream {
    source: Pid,
    stream: StreamId,
    _completion: LeasedCompletion,
}

impl CompletionState {
    fn new(
        incoming: mpsc::Sender<AcceptedStream>,
        link_local_incoming: mpsc::Sender<AcceptedStream>,
    ) -> Self {
        Self {
            requests: Mutex::new(Slots::new()),
            operations: Mutex::new(Slots::new()),
            establishments: Mutex::new(Slots::new()),
            outbound: Mutex::new(HashSet::new()),
            peers: Mutex::new(HashMap::new()),
            incoming: Mutex::new(Some(incoming)),
            link_local_incoming: Mutex::new(Some(link_local_incoming)),
            driver_error: Mutex::new(None),
            acceptance_stopped: AtomicBool::new(false),
            acceptance_notify: Notify::new(),
            stopped: AtomicBool::new(false),
        }
    }

    async fn process(&self, completion: LeasedCompletion) {
        if let Completion::IncomingStream(stream) = completion.completion() {
            let stream = *stream;
            let route = self
                .peers
                .lock()
                .expect("peer mutex should not be poisoned")
                .get(&stream.connection())
                .copied();
            if let Some((local, peer)) = route {
                let incoming = if local.is_link_local() {
                    &self.link_local_incoming
                } else {
                    &self.incoming
                };
                let sender = incoming
                    .lock()
                    .expect("incoming mutex should not be poisoned")
                    .clone();
                if let Some(sender) = sender {
                    let accepted = AcceptedStream {
                        source: peer,
                        stream,
                        _completion: completion,
                    };
                    tokio::select! {
                        _ = sender.send(accepted) => {}
                        () = self.acceptance_stopped() => {}
                    }
                }
            }
            return;
        }
        self.process_completion(completion.into_completion());
    }

    fn process_completion(&self, completion: Completion) {
        match completion {
            Completion::Command(completion) => {
                if let CommandResult::ConnectionCreated(connection) = completion.result() {
                    self.outbound
                        .lock()
                        .expect("outbound mutex should not be poisoned")
                        .insert(connection);
                }
                self.requests
                    .lock()
                    .expect("request mutex should not be poisoned")
                    .deliver(completion.request(), Ok(completion.result()));
            }
            Completion::ConnectionEstablished(established) => {
                let connection = established.connection();
                let local = established.local();
                let peer = established.peer();
                self.peers
                    .lock()
                    .expect("peer mutex should not be poisoned")
                    .insert(connection, (local, peer));
                if self
                    .outbound
                    .lock()
                    .expect("outbound mutex should not be poisoned")
                    .contains(&connection)
                {
                    self.establishments
                        .lock()
                        .expect("establishment mutex should not be poisoned")
                        .deliver(connection, Ok(peer));
                }
            }
            Completion::AuthenticationFailed(failure) => {
                self.complete_authentication_failure(failure);
            }
            Completion::IncomingStream(_) => unreachable!("incoming completion retains its lease"),
            Completion::ConnectionClosed { connection, .. } => {
                self.peers
                    .lock()
                    .expect("peer mutex should not be poisoned")
                    .remove(&connection);
                if self
                    .outbound
                    .lock()
                    .expect("outbound mutex should not be poisoned")
                    .remove(&connection)
                {
                    self.establishments
                        .lock()
                        .expect("establishment mutex should not be poisoned")
                        .deliver(connection, Err(EstablishmentFailure::Closed(connection)));
                }
            }
            Completion::Send { operation, .. }
            | Completion::Finish { operation, .. }
            | Completion::Discard { operation, .. }
            | Completion::Reset { operation, .. }
            | Completion::Stop { operation, .. } => {
                self.operations
                    .lock()
                    .expect("operation mutex should not be poisoned")
                    .deliver(operation, Ok(completion));
            }
            Completion::Receive(receive) => {
                let operation = receive.operation();
                self.operations
                    .lock()
                    .expect("operation mutex should not be poisoned")
                    .deliver(operation, Ok(Completion::Receive(receive)));
            }
            Completion::DriverStopped(_) | Completion::Closed { .. } => {}
        }
    }

    fn complete_authentication_failure(&self, failure: AuthenticationFailed) {
        let connection = failure.connection();
        if self
            .outbound
            .lock()
            .expect("outbound mutex should not be poisoned")
            .remove(&connection)
        {
            self.establishments
                .lock()
                .expect("establishment mutex should not be poisoned")
                .deliver(
                    connection,
                    Err(EstablishmentFailure::Authentication {
                        expected: failure.expected(),
                        actual: failure.actual(),
                    }),
                );
        }
    }

    async fn wait_request(&self, request: RequestId) -> Result<CommandResult, Error> {
        let receiver = {
            let mut slots = self
                .requests
                .lock()
                .expect("request mutex should not be poisoned");
            match slots.register(request, self.stopped.load(Ordering::Acquire)) {
                SlotRegistration::Ready(result) => {
                    return result.map_err(|()| self.completion_error());
                }
                SlotRegistration::Pending(receiver) => receiver,
                SlotRegistration::Stopped => return Err(self.completion_error()),
            }
        };
        receiver
            .await
            .map_err(|_| self.completion_error())?
            .map_err(|()| self.completion_error())
    }

    async fn wait_established(&self, connection: ConnectionId) -> Result<Pid, Error> {
        let registration = {
            let mut slots = self
                .establishments
                .lock()
                .expect("establishment mutex should not be poisoned");
            slots.register(connection, self.stopped.load(Ordering::Acquire))
        };
        let result = match registration {
            SlotRegistration::Ready(result) => result,
            SlotRegistration::Pending(receiver) => {
                receiver.await.map_err(|_| self.completion_error())?
            }
            SlotRegistration::Stopped => return Err(self.completion_error()),
        };
        self.outbound
            .lock()
            .expect("outbound mutex should not be poisoned")
            .remove(&connection);
        self.map_establishment(result)
    }

    fn stop_accepting(&self) {
        if !self.acceptance_stopped.swap(true, Ordering::AcqRel) {
            self.acceptance_notify.notify_waiters();
        }
    }

    async fn acceptance_stopped(&self) {
        if self.acceptance_stopped.load(Ordering::Acquire) {
            return;
        }
        let notified = self.acceptance_notify.notified();
        tokio::pin!(notified);
        if self.acceptance_stopped.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    async fn wait_operation(&self, operation: OperationId) -> Result<Completion, Error> {
        let receiver = {
            let mut slots = self
                .operations
                .lock()
                .expect("operation mutex should not be poisoned");
            match slots.register(operation, self.stopped.load(Ordering::Acquire)) {
                SlotRegistration::Ready(result) => {
                    return result.map_err(|()| self.completion_error());
                }
                SlotRegistration::Pending(receiver) => receiver,
                SlotRegistration::Stopped => return Err(self.completion_error()),
            }
        };
        receiver
            .await
            .map_err(|_| self.completion_error())?
            .map_err(|()| self.completion_error())
    }

    fn map_establishment(&self, result: Result<Pid, EstablishmentFailure>) -> Result<Pid, Error> {
        match result {
            Ok(peer) => Ok(peer),
            Err(EstablishmentFailure::Authentication { expected, actual }) => {
                Err(Error::Authentication { expected, actual })
            }
            Err(EstablishmentFailure::Closed(connection)) => {
                Err(Error::ConnectionClosed(connection))
            }
            Err(EstablishmentFailure::CompletionStopped) => Err(self.completion_error()),
        }
    }

    fn fail_driver(&self, error: String) {
        *self
            .driver_error
            .lock()
            .expect("driver error mutex should not be poisoned") = Some(error);
    }

    fn completion_error(&self) -> Error {
        self.driver_error
            .lock()
            .expect("driver error mutex should not be poisoned")
            .clone()
            .map_or(Error::CompletionStopped, Error::Driver)
    }

    fn stop(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        self.stop_accepting();
        let mut requests = self
            .requests
            .lock()
            .expect("request mutex should not be poisoned");
        for (_, waiter) in requests.waiters.drain() {
            let _ = waiter.send(Err(()));
        }
        drop(requests);
        let mut operations = self
            .operations
            .lock()
            .expect("operation mutex should not be poisoned");
        for (_, waiter) in operations.waiters.drain() {
            let _ = waiter.send(Err(()));
        }
        drop(operations);
        let mut establishments = self
            .establishments
            .lock()
            .expect("establishment mutex should not be poisoned");
        for (_, waiter) in establishments.waiters.drain() {
            let _ = waiter.send(Err(EstablishmentFailure::CompletionStopped));
        }
        self.incoming
            .lock()
            .expect("incoming mutex should not be poisoned")
            .take();
        self.link_local_incoming
            .lock()
            .expect("link-local incoming mutex should not be poisoned")
            .take();
    }
}

struct Shared {
    commands: EndpointCommands,
    submissions: SubmissionSender<EndpointCommand>,
    completions: Arc<CompletionState>,
}

/// Tokio facade over one completion-driven QUIC endpoint.
pub struct Transport {
    pid: Pid,
    shared: Arc<Shared>,
    incoming: AsyncMutex<mpsc::Receiver<AcceptedStream>>,
    link_local_incoming: AsyncMutex<mpsc::Receiver<AcceptedStream>>,
    abort_requested: Arc<AtomicBool>,
    driver: AsyncMutex<Option<thread::JoinHandle<Result<(), String>>>>,
    completion_task: AsyncMutex<Option<TokioJoinHandle<()>>>,
    connection_stats: ConnectionStatsHandle,
    endpoint_stats: EndpointStatsHandle,
}

impl Transport {
    /// Spawns a client-only driver thread and completion pump.
    pub fn spawn_client<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
    ) -> Result<Self, Error> {
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::client(
            driver,
            io,
            identity,
            config,
            submission_limits,
            completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            completion_capacity,
        )
    }

    /// Spawns a server-only driver thread and completion pump.
    pub fn spawn_server<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
    ) -> Result<Self, Error> {
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::server(
            driver,
            io,
            identity,
            config,
            submission_limits,
            completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            completion_capacity,
        )
    }

    /// Spawns a duplex driver that can both initiate and accept connections.
    pub fn spawn_duplex<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        client_config: quiche::Config,
        server_config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
    ) -> Result<Self, Error> {
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::duplex(
            driver,
            io,
            identity,
            client_config,
            server_config,
            submission_limits,
            completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            completion_capacity,
        )
    }

    /// Spawns a duplex driver whose routable CID prefix differs from its authenticated PID.
    pub fn spawn_duplex_routed<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        identity: EndpointIdentity,
        routing_pid: Pid,
        client_config: quiche::Config,
        server_config: quiche::Config,
        submission_limits: SubmissionLimits,
        completion_capacity: NonZeroUsize,
    ) -> Result<Self, Error> {
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::duplex_routed(
            driver,
            io,
            identity,
            routing_pid,
            client_config,
            server_config,
            submission_limits,
            completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            completion_capacity,
        )
    }

    fn spawn(
        pid: Pid,
        mut endpoint: Endpoint,
        handle: EndpointHandle,
        notifier: Arc<TokioNotifier>,
        completion_capacity: NonZeroUsize,
    ) -> Result<Self, Error> {
        let connection_stats = handle.connection_stats();
        let endpoint_stats = handle.endpoint_stats();
        let (commands, submissions, completions) = handle.into_parts();
        let (incoming_sender, incoming) = mpsc::channel(completion_capacity.get());
        let (link_local_sender, link_local_incoming) = mpsc::channel(completion_capacity.get());
        let state = Arc::new(CompletionState::new(incoming_sender, link_local_sender));
        let completion_state = state.clone();
        let completion_task = tokio::spawn(async move {
            pump_completions(completions, notifier, completion_state).await;
        });
        let driver_state = state.clone();
        let abort_requested = Arc::new(AtomicBool::new(false));
        let driver_abort_requested = abort_requested.clone();
        let driver = thread::Builder::new()
            .name(format!("chrysalis-quic-{}", driver_name(pid)))
            .spawn(move || {
                while endpoint.shutdown_state() != ShutdownState::Stopped {
                    if driver_abort_requested.load(Ordering::Acquire) {
                        endpoint.abort();
                    }
                    if let Err(error) = endpoint.poll(Duration::from_secs(1)) {
                        let error = error.to_string();
                        eprintln!("chrysalis QUIC driver failed: {error}");
                        driver_state.fail_driver(error.clone());
                        endpoint.abort();
                        return Err(error);
                    }
                }
                Ok(())
            })
            .map_err(Error::Spawn)?;
        Ok(Self {
            pid,
            shared: Arc::new(Shared {
                commands,
                submissions,
                completions: state,
            }),
            incoming: AsyncMutex::new(incoming),
            link_local_incoming: AsyncMutex::new(link_local_incoming),
            abort_requested,
            driver: AsyncMutex::new(Some(driver)),
            completion_task: AsyncMutex::new(Some(completion_task)),
            connection_stats,
            endpoint_stats,
        })
    }

    /// Returns the certificate-derived local PID.
    pub const fn pid(&self) -> Pid {
        self.pid
    }

    /// Returns the most recent driver snapshot for `peer`.
    pub fn connection_stats(&self, peer: Pid) -> Option<ConnectionStats> {
        self.connection_stats.get(peer)
    }

    /// Returns the latest bounded-interval endpoint diagnostics.
    pub fn endpoint_stats(&self) -> EndpointStats {
        self.endpoint_stats.snapshot()
    }

    /// Establishes an authenticated connection to `target`.
    pub async fn connect(
        &self,
        target: Pid,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<Connection, Error> {
        self.connect_inner(target, Some(target), peer, server_name.into())
            .await
    }

    /// Establishes a connection whose Initial CID route differs from its authenticated peer.
    pub async fn connect_routed(
        &self,
        route: Pid,
        expected: Pid,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<Connection, Error> {
        self.connect_inner(route, Some(expected), peer, server_name.into())
            .await
    }

    /// Establishes a routed connection and returns the certificate-derived peer PID.
    pub async fn connect_unpinned(
        &self,
        route: Pid,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<Connection, Error> {
        self.connect_inner(route, None, peer, server_name.into())
            .await
    }

    /// Establishes a connection with explicit local and remote CID routing identities.
    pub async fn connect_from(
        &self,
        source: Pid,
        route: Pid,
        expected: Option<Pid>,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<Connection, Error> {
        self.connect_inner_from(Some(source), route, expected, peer, server_name.into())
            .await
    }

    async fn connect_inner(
        &self,
        route: Pid,
        expected: Option<Pid>,
        peer: SocketAddr,
        server_name: Box<str>,
    ) -> Result<Connection, Error> {
        self.connect_inner_from(None, route, expected, peer, server_name)
            .await
    }

    async fn connect_inner_from(
        &self,
        source: Option<Pid>,
        route: Pid,
        expected: Option<Pid>,
        peer: SocketAddr,
        server_name: Box<str>,
    ) -> Result<Connection, Error> {
        let receipt = loop {
            let result = self.shared.commands.try_connect_from(
                source,
                route,
                expected,
                peer,
                server_name.clone(),
            );
            match result {
                Ok(receipt) => break receipt,
                Err(TryCommandError::Full(_) | TryCommandError::CompletionFull(_)) => {
                    tokio::task::yield_now().await
                }
                Err(TryCommandError::Closed(_)) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        let connection = match self
            .shared
            .completions
            .wait_request(receipt.request())
            .await?
        {
            CommandResult::ConnectionCreated(connection) => connection,
            CommandResult::Failed(error) => return Err(Error::Command(error)),
            _ => return Err(Error::Command(CommandError::Transport)),
        };
        let authenticated = self.shared.completions.wait_established(connection).await?;
        Ok(Connection {
            id: connection,
            peer: authenticated,
            shared: self.shared.clone(),
        })
    }

    /// Accepts the next authenticated peer-initiated bidirectional stream.
    pub async fn accept(&self) -> Result<IncomingStream, Error> {
        let mut incoming = self.incoming.lock().await;
        let accepted = tokio::select! {
            accepted = incoming.recv() => accepted,
            () = self.shared.completions.acceptance_stopped() => None,
        }
        .ok_or_else(|| self.shared.completions.completion_error())?;
        Ok(IncomingStream {
            source: accepted.source,
            stream: Stream {
                id: accepted.stream,
                shared: self.shared.clone(),
            },
        })
    }

    /// Accepts the next authenticated stream whose connection terminates at PID 0.
    pub async fn accept_link_local(&self) -> Result<IncomingStream, Error> {
        let mut incoming = self.link_local_incoming.lock().await;
        let accepted = tokio::select! {
            accepted = incoming.recv() => accepted,
            () = self.shared.completions.acceptance_stopped() => None,
        }
        .ok_or_else(|| self.shared.completions.completion_error())?;
        Ok(IncomingStream {
            source: accepted.source,
            stream: Stream {
                id: accepted.stream,
                shared: self.shared.clone(),
            },
        })
    }

    /// Requests bounded graceful shutdown and waits until admission is closed.
    pub async fn shutdown(&self, grace_period: Duration) -> Result<(), Error> {
        self.shared.completions.stop_accepting();
        self.drain_incoming().await;
        let receipt = loop {
            match self.shared.commands.try_shutdown(grace_period) {
                Ok(receipt) => break receipt,
                Err(TryCommandError::Full(_) | TryCommandError::CompletionFull(_)) => {
                    tokio::task::yield_now().await
                }
                Err(TryCommandError::Closed(_)) => return Ok(()),
            }
        };
        match self
            .shared
            .completions
            .wait_request(receipt.request())
            .await?
        {
            CommandResult::ShutdownStarted => Ok(()),
            CommandResult::Failed(CommandError::DriverStopped) => Ok(()),
            CommandResult::Failed(error) => Err(Error::Command(error)),
            _ => Err(Error::Command(CommandError::Transport)),
        }
    }

    async fn drain_incoming(&self) {
        let mut incoming = self.incoming.lock().await;
        incoming.close();
        while incoming.try_recv().is_ok() {}
        drop(incoming);
        let mut incoming = self.link_local_incoming.lock().await;
        incoming.close();
        while incoming.try_recv().is_ok() {}
    }

    /// Waits for the driver thread and completion pump to terminate.
    pub async fn join(&self) -> Result<(), Error> {
        let driver = self.driver.lock().await.take();
        let driver_result = if let Some(driver) = driver {
            tokio::task::spawn_blocking(move || driver.join())
                .await
                .map_err(|error| Error::CompletionTask(error.to_string()))?
                .map_err(|_| Error::DriverPanicked)?
                .map_err(Error::Driver)
        } else {
            Ok(())
        };
        let completion_task = self.completion_task.lock().await.take();
        if let Some(completion_task) = completion_task {
            completion_task
                .await
                .map_err(|error| Error::CompletionTask(error.to_string()))?;
        }
        driver_result
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.shared.completions.stop_accepting();
        self.abort_requested.store(true, Ordering::Release);
        let _ = self.shared.commands.try_abort();
    }
}

/// One authenticated physical QUIC connection.
#[derive(Clone)]
pub struct Connection {
    id: ConnectionId,
    peer: Pid,
    shared: Arc<Shared>,
}

impl Connection {
    /// Returns the authenticated peer PID.
    pub const fn peer(&self) -> Pid {
        self.peer
    }

    /// Allocates a cheap locally initiated bidirectional stream.
    pub async fn open_stream(&self) -> Result<Stream, Error> {
        let receipt = loop {
            match self.shared.commands.try_open_bidi(self.id) {
                Ok(receipt) => break receipt,
                Err(TryCommandError::Full(_) | TryCommandError::CompletionFull(_)) => {
                    tokio::task::yield_now().await
                }
                Err(TryCommandError::Closed(_)) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        match self
            .shared
            .completions
            .wait_request(receipt.request())
            .await?
        {
            CommandResult::StreamOpened(stream) => Ok(Stream {
                id: stream,
                shared: self.shared.clone(),
            }),
            CommandResult::Failed(error) => Err(Error::Command(error)),
            _ => Err(Error::Command(CommandError::Transport)),
        }
    }

    /// Requests an application-level connection close.
    pub async fn close(&self, error_code: u64, reason: Bytes) -> Result<(), Error> {
        let receipt = loop {
            match self
                .shared
                .commands
                .try_close(self.id, error_code, reason.clone())
            {
                Ok(receipt) => break receipt,
                Err(TryCommandError::Full(_) | TryCommandError::CompletionFull(_)) => {
                    tokio::task::yield_now().await
                }
                Err(TryCommandError::Closed(_)) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        match self
            .shared
            .completions
            .wait_request(receipt.request())
            .await?
        {
            CommandResult::CloseQueued(connection) if connection == self.id => Ok(()),
            CommandResult::Failed(error) => Err(Error::Command(error)),
            _ => Err(Error::Command(CommandError::Transport)),
        }
    }
}

/// One completion-driven bidirectional QUIC stream.
#[derive(Clone)]
pub struct Stream {
    id: StreamId,
    shared: Arc<Shared>,
}

struct CancelOnDrop(Option<OperationCancellation>);

impl CancelOnDrop {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancellation) = self.0.take() {
            cancellation.cancel();
        }
    }
}

impl Stream {
    /// Returns the driver-scoped stream ID.
    pub const fn id(&self) -> StreamId {
        self.id
    }

    /// Sends immutable bytes and waits until the peer acknowledges them.
    pub async fn send(&self, mut bytes: Bytes) -> Result<(), Error> {
        let receipt = loop {
            match self.shared.submissions.try_send(self.id, bytes) {
                Ok(receipt) => break receipt,
                Err(TrySendError::WouldBlock {
                    bytes: returned, ..
                }) => {
                    bytes = returned;
                    tokio::task::yield_now().await;
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        match self
            .shared
            .completions
            .wait_operation(receipt.operation())
            .await?
        {
            Completion::Send {
                outcome: SendOutcome::Acknowledged { .. },
                ..
            } => Ok(()),
            Completion::Send {
                outcome: SendOutcome::Abandoned,
                ..
            } => Err(Error::SendAbandoned(self.id)),
            Completion::Send {
                outcome: SendOutcome::Rejected,
                ..
            } => Err(Error::SendRejected(self.id)),
            _ => Err(Error::CompletionStopped),
        }
    }

    /// Posts one caller-owned receive buffer and returns it in the completion.
    pub async fn receive(
        &self,
        mut buffer: BytesMut,
        options: ReceiveOptions,
    ) -> Result<ReceiveCompletion, Error> {
        let receipt = loop {
            match self
                .shared
                .submissions
                .try_receive(self.id, buffer, options)
            {
                Ok(receipt) => break receipt,
                Err(TryReceiveError::WouldBlock {
                    buffer: returned, ..
                }) => {
                    buffer = returned;
                    tokio::task::yield_now().await;
                }
                Err(TryReceiveError::InvalidBuffer { .. }) => {
                    return Err(Error::Command(CommandError::InvalidArgument));
                }
                Err(TryReceiveError::Closed { .. }) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        let mut cancellation = CancelOnDrop(receipt.cancellation());
        let completion = self
            .shared
            .completions
            .wait_operation(receipt.operation())
            .await?;
        cancellation.disarm();
        match completion {
            Completion::Receive(receive) => Ok(receive),
            _ => Err(Error::CompletionStopped),
        }
    }

    /// Queues FIN after preceding sends.
    pub async fn finish(&self) -> Result<(), Error> {
        let receipt = loop {
            match self.shared.submissions.try_finish(self.id) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => tokio::task::yield_now().await,
                Err(TryControlError::Closed) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        match self
            .shared
            .completions
            .wait_operation(receipt.operation())
            .await?
        {
            Completion::Finish {
                outcome: ControlOutcome::Complete,
                ..
            } => Ok(()),
            Completion::Finish {
                outcome: ControlOutcome::Abandoned,
                ..
            } => Err(Error::FinishAbandoned(self.id)),
            Completion::Finish {
                outcome: ControlOutcome::Rejected,
                ..
            } => Err(Error::FinishRejected(self.id)),
            _ => Err(Error::CompletionStopped),
        }
    }

    /// Discards received bytes and reports how the operation completed.
    pub async fn discard(&self, max_bytes: NonZeroUsize) -> Result<(usize, ReceiveStatus), Error> {
        let receipt = loop {
            match self.shared.submissions.try_discard(self.id, max_bytes) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => tokio::task::yield_now().await,
                Err(TryControlError::Closed) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        match self
            .shared
            .completions
            .wait_operation(receipt.operation())
            .await?
        {
            Completion::Discard { bytes, status, .. } => Ok((bytes, status)),
            _ => Err(Error::CompletionStopped),
        }
    }

    /// Resets the local send half after preceding sends.
    pub async fn reset(&self, error_code: u64) -> Result<(), Error> {
        let receipt = loop {
            match self.shared.submissions.try_reset(self.id, error_code) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => tokio::task::yield_now().await,
                Err(TryControlError::Closed) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        match self
            .shared
            .completions
            .wait_operation(receipt.operation())
            .await?
        {
            Completion::Reset {
                outcome: ControlOutcome::Complete,
                ..
            } => Ok(()),
            Completion::Reset {
                outcome: ControlOutcome::Abandoned,
                ..
            } => Err(Error::ResetAbandoned(self.id)),
            Completion::Reset {
                outcome: ControlOutcome::Rejected,
                ..
            } => Err(Error::ResetRejected(self.id)),
            _ => Err(Error::CompletionStopped),
        }
    }

    /// Stops the local receive half after preceding receive operations.
    pub async fn stop(&self, error_code: u64) -> Result<(), Error> {
        let receipt = loop {
            match self.shared.submissions.try_stop(self.id, error_code) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => tokio::task::yield_now().await,
                Err(TryControlError::Closed) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
            }
        };
        match self
            .shared
            .completions
            .wait_operation(receipt.operation())
            .await?
        {
            Completion::Stop {
                outcome: ControlOutcome::Complete,
                ..
            } => Ok(()),
            Completion::Stop {
                outcome: ControlOutcome::Abandoned,
                ..
            } => Err(Error::StopAbandoned(self.id)),
            Completion::Stop {
                outcome: ControlOutcome::Rejected,
                ..
            } => Err(Error::StopRejected(self.id)),
            _ => Err(Error::CompletionStopped),
        }
    }
}

/// An authenticated incoming bidirectional stream.
pub struct IncomingStream {
    source: Pid,
    stream: Stream,
}

impl IncomingStream {
    /// Returns the authenticated source PID.
    pub const fn source(&self) -> Pid {
        self.source
    }

    /// Returns the bidirectional stream.
    pub const fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Separates the source PID and stream.
    pub fn into_parts(self) -> (Pid, Stream) {
        (self.source, self.stream)
    }
}

async fn pump_completions(
    queue: CompletionQueue,
    notifier: Arc<TokioNotifier>,
    state: Arc<CompletionState>,
) {
    loop {
        while let Some(completion) = queue.try_pop_leased() {
            state.process(completion).await;
        }
        if queue.is_closed() {
            state.stop();
            return;
        }

        let sequence = queue.sequence();
        let notified = notifier.notify.notified();
        tokio::pin!(notified);
        if queue.sequence() != sequence || !queue.is_empty() {
            continue;
        }
        notified.await;
    }
}

fn driver_name(pid: Pid) -> String {
    pid.as_bytes()[..4]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::UdpSocket;
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::time::Instant;

    use chrysalis_transport_core::CompletionCredits;
    use chrysalis_transport_core::ConnectionEstablished;
    use chrysalis_transport_core::NoopNotifier;
    use chrysalis_transport_core::completion_queue;
    use chrysalis_transport_uring::DriverConfig;
    use chrysalis_transport_uring::UdpDriver;
    use rcgen::BasicConstraints;
    use rcgen::CertificateParams;
    use rcgen::CertifiedIssuer;
    use rcgen::ExtendedKeyUsagePurpose;
    use rcgen::IsCa;
    use rcgen::KeyPair;
    use rcgen::KeyUsagePurpose;
    use tempfile::TempDir;
    use tokio::time::timeout;

    use super::*;

    const APPLICATION_PROTOCOL: &[u8] = b"chrysalis-transport-tokio-test/1";
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    struct Credential {
        certificate_path: String,
        key_path: String,
        identity: EndpointIdentity,
    }

    fn certificate_authority() -> CertifiedIssuer<'static, KeyPair> {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap()
    }

    fn credential(
        directory: &Path,
        name: &str,
        issuer: &CertifiedIssuer<'_, KeyPair>,
    ) -> Credential {
        let signing_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&signing_key, issuer).unwrap();
        let certificate = directory.join(format!("{name}.crt"));
        let key = directory.join(format!("{name}.key"));
        fs::write(&certificate, format!("{}{}", cert.pem(), issuer.pem())).unwrap();
        fs::write(&key, signing_key.serialize_pem()).unwrap();
        Credential {
            certificate_path: certificate.to_str().unwrap().to_owned(),
            key_path: key.to_str().unwrap().to_owned(),
            identity: EndpointIdentity::from_leaf_certificate(cert.der().as_ref()),
        }
    }

    fn config(credential: &Credential, roots: &str, verify_peer: bool) -> quiche::Config {
        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
        config
            .set_application_protos(&[APPLICATION_PROTOCOL])
            .unwrap();
        config
            .load_cert_chain_from_pem_file(&credential.certificate_path)
            .unwrap();
        config
            .load_priv_key_from_pem_file(&credential.key_path)
            .unwrap();
        config.load_verify_locations_from_file(roots).unwrap();
        config.verify_peer(verify_peer);
        config.set_max_idle_timeout(5_000);
        config.set_max_recv_udp_payload_size(1200);
        config.set_max_send_udp_payload_size(1200);
        config.set_initial_max_data(1024 * 1024);
        config.set_initial_max_stream_data_bidi_local(1024 * 1024);
        config.set_initial_max_stream_data_bidi_remote(1024 * 1024);
        config.set_initial_max_streams_bidi(16);
        config.set_initial_max_streams_uni(0);
        config.set_disable_active_migration(true);
        config.enable_pacing(true);
        config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
        config
    }

    fn io() -> UdpDriver {
        let config = DriverConfig::new(
            NonZeroU32::new(32).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(1200).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(1024 * 1024).unwrap(),
            true,
        );
        UdpDriver::new(UdpSocket::bind("[::1]:0").unwrap(), config).unwrap()
    }

    fn limits() -> SubmissionLimits {
        SubmissionLimits::new(
            NonZeroUsize::new(64).unwrap(),
            NonZeroUsize::new(1024 * 1024).unwrap(),
            NonZeroUsize::new(1024 * 1024).unwrap(),
        )
    }

    async fn receive_to_fin(stream: &Stream) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let completion = stream
                .receive(BytesMut::with_capacity(64), ReceiveOptions::default())
                .await
                .unwrap();
            result.extend_from_slice(completion.data());
            if completion.status() == ReceiveStatus::Fin {
                return result;
            }
        }
    }

    #[tokio::test]
    async fn incoming_stream_retains_completion_credit_until_acceptance() {
        let (incoming_sender, mut incoming) = mpsc::channel(1);
        let (link_local_sender, _link_local) = mpsc::channel(1);
        let state = CompletionState::new(incoming_sender, link_local_sender);
        let credits = CompletionCredits::new(NonZeroUsize::MIN, Arc::new(NoopNotifier));
        let (sender, queue) = completion_queue(NonZeroUsize::MIN, Arc::new(NoopNotifier));
        let connection = ConnectionId::new(DriverId::from_u16(1), 1);
        let local = Pid::from_bytes([1; chrysalis_core::PID_LEN]);
        let peer = Pid::from_bytes([2; chrysalis_core::PID_LEN]);

        sender
            .try_push(
                Completion::ConnectionEstablished(ConnectionEstablished::new(
                    connection, local, peer,
                )),
                credits.try_acquire().unwrap(),
            )
            .unwrap();
        state.process(queue.try_pop_leased().unwrap()).await;
        sender
            .try_push(
                Completion::IncomingStream(StreamId::new(connection, 0)),
                credits.try_acquire().unwrap(),
            )
            .unwrap();
        state.process(queue.try_pop_leased().unwrap()).await;

        assert_eq!(credits.used(), 1);
        assert!(credits.try_acquire().is_none());
        let accepted = incoming.recv().await.unwrap();
        assert_eq!(accepted.source, peer);
        drop(accepted);
        assert_eq!(credits.used(), 0);
    }

    #[tokio::test]
    async fn establishes_streams_and_preserves_separate_shutdown_and_join() {
        let directory = TempDir::new().unwrap();
        let issuer = certificate_authority();
        let client_credential = credential(directory.path(), "client", &issuer);
        let server_credential = credential(directory.path(), "server", &issuer);
        assert_ne!(
            client_credential.identity.pid(),
            server_credential.identity.pid()
        );
        let roots = directory.path().join("roots.pem");
        fs::write(&roots, issuer.pem()).unwrap();
        let roots = roots.to_str().unwrap();

        let server_io = io();
        let server_address = server_io.local_addr().unwrap();
        let server = Arc::new(
            Transport::spawn_server(
                DriverId::from_u16(1),
                server_io,
                server_credential.identity,
                config(&server_credential, roots, true),
                limits(),
                NonZeroUsize::new(128).unwrap(),
            )
            .unwrap(),
        );
        let client = Arc::new(
            Transport::spawn_client(
                DriverId::from_u16(2),
                io(),
                client_credential.identity,
                config(&client_credential, roots, true),
                limits(),
                NonZeroUsize::new(128).unwrap(),
            )
            .unwrap(),
        );

        let accepting = tokio::spawn({
            let server = server.clone();
            let expected_source = client.pid();
            async move {
                let incoming = server.accept().await.unwrap();
                assert_eq!(incoming.source(), expected_source);
                let (_, stream) = incoming.into_parts();
                assert_eq!(receive_to_fin(&stream).await, b"ping");
                tokio::try_join!(stream.send(Bytes::from_static(b"pong")), stream.finish())
                    .unwrap();
            }
        });

        let connection = timeout(
            TEST_TIMEOUT,
            client.connect(server.pid(), server_address, "localhost"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(connection.peer(), server.pid());
        let stream = connection.open_stream().await.unwrap();
        tokio::try_join!(stream.send(Bytes::from_static(b"ping")), stream.finish()).unwrap();
        assert_eq!(receive_to_fin(&stream).await, b"pong");
        timeout(TEST_TIMEOUT, accepting).await.unwrap().unwrap();

        let shutdown_started = Instant::now();
        client.shutdown(Duration::from_millis(100)).await.unwrap();
        server.shutdown(Duration::from_millis(100)).await.unwrap();
        timeout(TEST_TIMEOUT, client.join()).await.unwrap().unwrap();
        timeout(TEST_TIMEOUT, server.join()).await.unwrap().unwrap();
        assert!(shutdown_started.elapsed() < TEST_TIMEOUT);
    }
}
