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
use std::hash::Hash;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
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
use chrysalis_transport_quiche::Error as DriverError;
use chrysalis_transport_quiche::PacketIo;
use chrysalis_transport_quiche::ShutdownState;
use thiserror::Error as ThisError;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle as TokioJoinHandle;

/// Failure from a Tokio transport command, operation, or driver lifecycle.
#[derive(Debug, ThisError)]
pub enum Error {
    /// The driver thread could not be created.
    #[error("spawn transport driver: {0}")]
    Spawn(#[source] io::Error),
    /// A command was rejected by the driver.
    #[error("transport command failed: {0:?}")]
    Command(CommandError),
    /// The requested process did not authenticate as the expected PID.
    #[error("peer authentication failed: expected {expected:?}, got {actual:?}")]
    Authentication {
        /// PID requested by the caller.
        expected: Option<Pid>,
        /// PID derived from the peer certificate, if one was presented.
        actual: Option<Pid>,
    },
    /// The connection closed before it became usable.
    #[error("connection closed before establishment: {0:?}")]
    ConnectionClosed(ConnectionId),
    /// A send was abandoned before acknowledgement.
    #[error("send was abandoned on {0:?}")]
    SendAbandoned(StreamId),
    /// A send payload was empty.
    #[error("cannot send an empty payload on {0:?}")]
    EmptySend(StreamId),
    /// A send was submitted after the stream send half began finishing.
    #[error("send was rejected on {0:?}")]
    SendRejected(StreamId),
    /// A local FIN was abandoned.
    #[error("finish was abandoned on {0:?}")]
    FinishAbandoned(StreamId),
    /// A local FIN was submitted after the stream send half became terminal.
    #[error("finish was rejected on {0:?}")]
    FinishRejected(StreamId),
    /// A reset-send operation was abandoned.
    #[error("reset was abandoned on {0:?}")]
    ResetAbandoned(StreamId),
    /// A reset-send operation was rejected.
    #[error("reset was rejected on {0:?}")]
    ResetRejected(StreamId),
    /// A stop-receiving operation was abandoned.
    #[error("stop was abandoned on {0:?}")]
    StopAbandoned(StreamId),
    /// A stop-receiving operation was rejected.
    #[error("stop was rejected on {0:?}")]
    StopRejected(StreamId),
    /// The completion pump stopped before delivering an accepted operation.
    #[error("transport completion pump stopped")]
    CompletionStopped,
    /// The adapter received a completion for an operation of another kind.
    #[error("unexpected completion while waiting for {0}")]
    UnexpectedCompletion(&'static str),
    /// Two futures attempted to wait on the same completion identifier.
    #[error("completion already has a waiter")]
    DuplicateWaiter,
    /// An incoming stream arrived before its connection identity was known.
    #[error("incoming stream for unknown connection: {0:?}")]
    UnknownIncomingConnection(ConnectionId),
    /// A constructor was called outside a Tokio runtime.
    #[error("transport requires an active Tokio runtime")]
    NoRuntime,
    /// The dedicated driver returned an error.
    #[error("transport driver failed: {0}")]
    Driver(#[source] Arc<DriverError>),
    /// The dedicated driver thread panicked.
    #[error("transport driver thread panicked")]
    DriverPanicked,
    /// The completion pump task failed.
    #[error("completion task failed: {0}")]
    CompletionTask(#[source] tokio::task::JoinError),
}

#[derive(Default)]
struct TokioNotifier {
    notify: Notify,
    sequence: AtomicU64,
}

impl Notifier for TokioNotifier {
    fn notify(&self) {
        self.sequence.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }
}

impl TokioNotifier {
    fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }

    async fn wait_for_change(&self, sequence: u64) {
        if self.sequence() != sequence {
            return;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.sequence() != sequence {
            return;
        }
        notified.await;
    }
}

struct Slots<K, V> {
    ready: HashMap<K, V>,
    waiters: HashMap<K, oneshot::Sender<V>>,
}

enum SlotRegistration<V> {
    Ready(V),
    Pending(oneshot::Receiver<V>),
    Duplicate,
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
        if self.waiters.contains_key(&key) {
            return SlotRegistration::Duplicate;
        }
        let (sender, receiver) = oneshot::channel();
        self.waiters.insert(key, sender);
        SlotRegistration::Pending(receiver)
    }

    fn clear(&mut self) {
        self.ready.clear();
        self.waiters.clear();
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

#[derive(Clone, Debug)]
enum CompletionFailure {
    Driver(Arc<DriverError>),
    UnknownIncomingConnection(ConnectionId),
}

struct CompletionState {
    requests: Mutex<Slots<RequestId, Result<CommandResult, ()>>>,
    operations: Mutex<Slots<OperationId, Result<Completion, ()>>>,
    establishments: Mutex<Slots<ConnectionId, Result<Pid, EstablishmentFailure>>>,
    outbound: Mutex<HashSet<ConnectionId>>,
    peers: Mutex<HashMap<ConnectionId, (Pid, Pid)>>,
    incoming: Mutex<Option<mpsc::UnboundedSender<AcceptedStream>>>,
    link_local_incoming: Mutex<Option<mpsc::UnboundedSender<AcceptedStream>>>,
    failure: Mutex<Option<CompletionFailure>>,
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
        incoming: mpsc::UnboundedSender<AcceptedStream>,
        link_local_incoming: mpsc::UnboundedSender<AcceptedStream>,
    ) -> Self {
        Self {
            requests: Mutex::new(Slots::new()),
            operations: Mutex::new(Slots::new()),
            establishments: Mutex::new(Slots::new()),
            outbound: Mutex::new(HashSet::new()),
            peers: Mutex::new(HashMap::new()),
            incoming: Mutex::new(Some(incoming)),
            link_local_incoming: Mutex::new(Some(link_local_incoming)),
            failure: Mutex::new(None),
            acceptance_stopped: AtomicBool::new(false),
            acceptance_notify: Notify::new(),
            stopped: AtomicBool::new(false),
        }
    }

    fn process(&self, completion: LeasedCompletion) {
        if let Completion::IncomingStream(stream) = completion.completion() {
            let stream = *stream;
            let route = self
                .peers
                .lock()
                .expect("peer mutex should not be poisoned")
                .get(&stream.connection())
                .copied();
            let Some((local, peer)) = route else {
                self.fail(CompletionFailure::UnknownIncomingConnection(
                    stream.connection(),
                ));
                return;
            };
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
                let _ = sender.send(AcceptedStream {
                    source: peer,
                    stream,
                    _completion: completion,
                });
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
                SlotRegistration::Duplicate => return Err(Error::DuplicateWaiter),
                SlotRegistration::Stopped => return Err(self.completion_error()),
            }
        };
        let result = receiver.await;
        result
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
            SlotRegistration::Duplicate => return Err(Error::DuplicateWaiter),
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
        notified.as_mut().enable();
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
                SlotRegistration::Duplicate => return Err(Error::DuplicateWaiter),
                SlotRegistration::Stopped => return Err(self.completion_error()),
            }
        };
        let result = receiver.await;
        result
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

    fn fail(&self, failure: CompletionFailure) {
        *self
            .failure
            .lock()
            .expect("completion failure mutex should not be poisoned") = Some(failure);
        self.stop();
    }

    fn completion_error(&self) -> Error {
        match self
            .failure
            .lock()
            .expect("completion failure mutex should not be poisoned")
            .clone()
        {
            Some(CompletionFailure::Driver(error)) => Error::Driver(error),
            Some(CompletionFailure::UnknownIncomingConnection(connection)) => {
                Error::UnknownIncomingConnection(connection)
            }
            None => Error::CompletionStopped,
        }
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
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
        requests.clear();
        drop(requests);
        let mut operations = self
            .operations
            .lock()
            .expect("operation mutex should not be poisoned");
        for (_, waiter) in operations.waiters.drain() {
            let _ = waiter.send(Err(()));
        }
        operations.clear();
        drop(operations);
        let mut establishments = self
            .establishments
            .lock()
            .expect("establishment mutex should not be poisoned");
        for (_, waiter) in establishments.waiters.drain() {
            let _ = waiter.send(Err(EstablishmentFailure::CompletionStopped));
        }
        establishments.clear();
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
    readiness: Arc<TokioNotifier>,
    closed: AtomicBool,
}

impl Shared {
    fn ensure_open(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::Command(CommandError::DriverStopped))
        } else {
            Ok(())
        }
    }

    async fn wait_for_progress(&self, sequence: u64) -> Result<(), Error> {
        self.readiness.wait_for_change(sequence).await;
        self.ensure_open()?;
        if self.completions.is_stopped() {
            Err(self.completions.completion_error())
        } else {
            Ok(())
        }
    }
}

/// Queue and retained-memory limits for one Tokio transport.
pub struct TransportLimits {
    submission: SubmissionLimits,
    completion_capacity: NonZeroUsize,
}

impl TransportLimits {
    /// Constructs transport resource limits.
    pub const fn new(submission: SubmissionLimits, completion_capacity: NonZeroUsize) -> Self {
        Self {
            submission,
            completion_capacity,
        }
    }
}

/// Identity and QUIC policy for a client-only or server-only transport.
pub struct SingleTransportConfig {
    identity: EndpointIdentity,
    quic: quiche::Config,
    limits: TransportLimits,
}

impl SingleTransportConfig {
    /// Constructs a single-role transport configuration.
    pub fn new(identity: EndpointIdentity, quic: quiche::Config, limits: TransportLimits) -> Self {
        Self {
            identity,
            quic,
            limits,
        }
    }
}

/// Identity and distinct client/server QUIC policies for a duplex transport.
pub struct DuplexTransportConfig {
    identity: EndpointIdentity,
    client: quiche::Config,
    server: quiche::Config,
    limits: TransportLimits,
}

impl DuplexTransportConfig {
    /// Constructs a duplex transport configuration.
    pub fn new(
        identity: EndpointIdentity,
        client: quiche::Config,
        server: quiche::Config,
        limits: TransportLimits,
    ) -> Self {
        Self {
            identity,
            client,
            server,
            limits,
        }
    }
}

/// Tokio facade over one completion-driven QUIC endpoint.
///
/// Constructors must run inside an active Tokio runtime. Dropping the owner closes admission,
/// requests an abort, and leaves a lifecycle task to join the driver thread and completion tasks.
pub struct Transport {
    pid: Pid,
    shared: Arc<Shared>,
    incoming: AsyncMutex<mpsc::Receiver<AcceptedStream>>,
    link_local_incoming: AsyncMutex<mpsc::Receiver<AcceptedStream>>,
    abort_requested: Arc<AtomicBool>,
    lifecycle: AsyncMutex<Option<TokioJoinHandle<Result<(), Error>>>>,
    connection_stats: ConnectionStatsHandle,
    endpoint_stats: EndpointStatsHandle,
}

impl Transport {
    /// Spawns a client-only driver thread and completion pump.
    pub fn spawn_client<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        config: SingleTransportConfig,
    ) -> Result<Self, Error> {
        let SingleTransportConfig {
            identity,
            quic,
            limits,
        } = config;
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::client(
            driver,
            io,
            identity,
            quic,
            limits.submission,
            limits.completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            limits.completion_capacity,
        )
    }

    /// Spawns a server-only driver thread and completion pump.
    pub fn spawn_server<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        config: SingleTransportConfig,
    ) -> Result<Self, Error> {
        let SingleTransportConfig {
            identity,
            quic,
            limits,
        } = config;
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::server(
            driver,
            io,
            identity,
            quic,
            limits.submission,
            limits.completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            limits.completion_capacity,
        )
    }

    /// Spawns a duplex driver that can both initiate and accept connections.
    pub fn spawn_duplex<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        config: DuplexTransportConfig,
    ) -> Result<Self, Error> {
        let DuplexTransportConfig {
            identity,
            client,
            server,
            limits,
        } = config;
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::duplex(
            driver,
            io,
            identity,
            client,
            server,
            limits.submission,
            limits.completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            limits.completion_capacity,
        )
    }

    /// Spawns a duplex driver whose routable CID prefix differs from its authenticated PID.
    pub fn spawn_duplex_routed<I: PacketIo + 'static>(
        driver: DriverId,
        io: I,
        routing_pid: Pid,
        config: DuplexTransportConfig,
    ) -> Result<Self, Error> {
        let DuplexTransportConfig {
            identity,
            client,
            server,
            limits,
        } = config;
        let notifier = Arc::new(TokioNotifier::default());
        let (endpoint, handle) = Endpoint::duplex_routed(
            driver,
            io,
            identity,
            routing_pid,
            client,
            server,
            limits.submission,
            limits.completion_capacity,
            notifier.clone(),
        );
        Self::spawn(
            identity.pid(),
            endpoint,
            handle,
            notifier,
            limits.completion_capacity,
        )
    }

    fn spawn<I: PacketIo + 'static>(
        pid: Pid,
        mut endpoint: Endpoint<I>,
        handle: EndpointHandle,
        notifier: Arc<TokioNotifier>,
        completion_capacity: NonZeroUsize,
    ) -> Result<Self, Error> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        let connection_stats = handle.connection_stats();
        let endpoint_stats = handle.endpoint_stats();
        let (commands, submissions, completions) = handle.into_parts();
        let (incoming_sender, incoming) = mpsc::channel(completion_capacity.get());
        let (incoming_dispatch, incoming_pending) = mpsc::unbounded_channel();
        let (link_local_sender, link_local_incoming) = mpsc::channel(completion_capacity.get());
        let (link_local_dispatch, link_local_pending) = mpsc::unbounded_channel();
        let state = Arc::new(CompletionState::new(incoming_dispatch, link_local_dispatch));
        let driver_state = state.clone();
        let abort_requested = Arc::new(AtomicBool::new(false));
        let driver_abort_requested = abort_requested.clone();
        let driver_notifier = notifier.clone();
        let driver = thread::Builder::new()
            .name(format!("chrysalis-quic-{}", driver_name(pid)))
            .spawn(move || {
                while endpoint.shutdown_state() != ShutdownState::Stopped {
                    if driver_abort_requested.load(Ordering::Acquire) {
                        endpoint.abort();
                    }
                    let result = endpoint.poll(Duration::from_secs(1));
                    driver_notifier.notify();
                    if let Err(error) = result {
                        let error = Arc::new(error);
                        driver_state.fail(CompletionFailure::Driver(error.clone()));
                        endpoint.abort();
                        return Err(error);
                    }
                }
                Ok(())
            })
            .map_err(Error::Spawn)?;
        let completion_state = state.clone();
        let completion_notifier = notifier.clone();
        let incoming_state = state.clone();
        let link_local_state = state.clone();
        let lifecycle = runtime.spawn(async move {
            let driver_task = tokio::task::spawn_blocking(move || driver.join());
            let (_, _, _, driver_result) = tokio::join!(
                pump_completions(completions, completion_notifier, completion_state),
                dispatch_incoming(incoming_pending, incoming_sender, incoming_state),
                dispatch_incoming(link_local_pending, link_local_sender, link_local_state,),
                driver_task,
            );
            driver_result
                .map_err(Error::CompletionTask)?
                .map_err(|_| Error::DriverPanicked)?
                .map_err(Error::Driver)
        });
        Ok(Self {
            pid,
            shared: Arc::new(Shared {
                commands,
                submissions,
                completions: state,
                readiness: notifier,
                closed: AtomicBool::new(false),
            }),
            incoming: AsyncMutex::new(incoming),
            link_local_incoming: AsyncMutex::new(link_local_incoming),
            abort_requested,
            lifecycle: AsyncMutex::new(Some(lifecycle)),
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
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
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
                    self.shared.wait_for_progress(sequence).await?;
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
            let sequence = self.shared.readiness.sequence();
            match self.shared.commands.try_shutdown(grace_period) {
                Ok(receipt) => break receipt,
                Err(TryCommandError::Full(_) | TryCommandError::CompletionFull(_)) => {
                    self.shared.wait_for_progress(sequence).await?;
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
        if let Some(lifecycle) = self.lifecycle.lock().await.take() {
            lifecycle.await.map_err(Error::CompletionTask)??;
        }
        Ok(())
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.shared.completions.stop_accepting();
        self.shared.closed.store(true, Ordering::Release);
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
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
            match self.shared.commands.try_open_bidi(self.id) {
                Ok(receipt) => break receipt,
                Err(TryCommandError::Full(_) | TryCommandError::CompletionFull(_)) => {
                    self.shared.wait_for_progress(sequence).await?;
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
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
            match self
                .shared
                .commands
                .try_close(self.id, error_code, reason.clone())
            {
                Ok(receipt) => break receipt,
                Err(TryCommandError::Full(_) | TryCommandError::CompletionFull(_)) => {
                    self.shared.wait_for_progress(sequence).await?;
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
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
            match self.shared.submissions.try_send(self.id, bytes) {
                Ok(receipt) => break receipt,
                Err(TrySendError::WouldBlock {
                    bytes: returned, ..
                }) => {
                    bytes = returned;
                    self.shared.wait_for_progress(sequence).await?;
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(Error::Command(CommandError::DriverStopped));
                }
                Err(TrySendError::Empty(_)) => return Err(Error::EmptySend(self.id)),
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
            _ => Err(Error::UnexpectedCompletion("send")),
        }
    }

    /// Posts one caller-owned receive buffer and returns it in the completion.
    pub async fn receive(
        &self,
        mut buffer: BytesMut,
        options: ReceiveOptions,
    ) -> Result<ReceiveCompletion, Error> {
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
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
                    self.shared.wait_for_progress(sequence).await?;
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
            _ => Err(Error::UnexpectedCompletion("receive")),
        }
    }

    /// Queues FIN after preceding sends.
    pub async fn finish(&self) -> Result<(), Error> {
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
            match self.shared.submissions.try_finish(self.id) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => {
                    self.shared.wait_for_progress(sequence).await?;
                }
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
            _ => Err(Error::UnexpectedCompletion("finish")),
        }
    }

    /// Discards received bytes and reports how the operation completed.
    pub async fn discard(&self, max_bytes: NonZeroUsize) -> Result<(usize, ReceiveStatus), Error> {
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
            match self.shared.submissions.try_discard(self.id, max_bytes) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => {
                    self.shared.wait_for_progress(sequence).await?;
                }
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
            _ => Err(Error::UnexpectedCompletion("discard")),
        }
    }

    /// Resets the local send half after preceding sends.
    pub async fn reset(&self, error_code: u64) -> Result<(), Error> {
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
            match self.shared.submissions.try_reset(self.id, error_code) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => {
                    self.shared.wait_for_progress(sequence).await?;
                }
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
            _ => Err(Error::UnexpectedCompletion("reset")),
        }
    }

    /// Stops the local receive half after preceding receive operations.
    pub async fn stop(&self, error_code: u64) -> Result<(), Error> {
        self.shared.ensure_open()?;
        let receipt = loop {
            let sequence = self.shared.readiness.sequence();
            match self.shared.submissions.try_stop(self.id, error_code) {
                Ok(receipt) => break receipt,
                Err(TryControlError::WouldBlock(_)) => {
                    self.shared.wait_for_progress(sequence).await?;
                }
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
            _ => Err(Error::UnexpectedCompletion("stop")),
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
        let notify_sequence = notifier.sequence();
        while let Some(completion) = queue.try_pop_leased() {
            state.process(completion);
            notifier.notify();
        }
        if queue.is_closed() {
            state.stop();
            notifier.notify();
            return;
        }

        if !queue.is_empty() {
            continue;
        }
        notifier.wait_for_change(notify_sequence).await;
    }
}

async fn dispatch_incoming(
    mut pending: mpsc::UnboundedReceiver<AcceptedStream>,
    ready: mpsc::Sender<AcceptedStream>,
    state: Arc<CompletionState>,
) {
    while let Some(stream) = pending.recv().await {
        tokio::select! {
            result = ready.send(stream) => {
                if result.is_err() {
                    return;
                }
            }
            () = state.acceptance_stopped() => return,
        }
    }
}

fn driver_name(pid: Pid) -> String {
    pid.as_bytes()
        .iter()
        .take(4)
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
        let config = DriverConfig::default()
            .with_ring_depth(NonZeroU32::new(32).unwrap())
            .with_receive_depth(NonZeroUsize::new(8).unwrap())
            .with_segment_size(NonZeroUsize::new(1200).unwrap())
            .with_max_gso_segments(NonZeroUsize::new(4).unwrap())
            .with_socket_buffer_bytes(NonZeroUsize::new(1024 * 1024).unwrap())
            .with_gro(true);
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

    #[test]
    fn abandoned_completion_waiter_discards_late_result() {
        let slots = Mutex::new(Slots::<u64, u64>::new());
        let receiver = match slots.lock().unwrap().register(7, false) {
            SlotRegistration::Pending(receiver) => receiver,
            _ => panic!("new completion slot should be pending"),
        };

        drop(receiver);
        slots.lock().unwrap().deliver(7, 11);

        let slots = slots.lock().unwrap();
        assert!(slots.ready.is_empty());
        assert!(slots.waiters.is_empty());
    }

    #[test]
    fn duplicate_completion_waiter_returns_an_error_state() {
        let mut slots = Slots::<u64, u64>::new();
        assert!(matches!(
            slots.register(7, false),
            SlotRegistration::Pending(_)
        ));
        assert!(matches!(
            slots.register(7, false),
            SlotRegistration::Duplicate
        ));
    }

    #[tokio::test]
    async fn incoming_stream_retains_completion_credit_until_acceptance() {
        let (incoming_sender, mut incoming) = mpsc::channel(1);
        let (incoming_dispatch, incoming_pending) = mpsc::unbounded_channel();
        let (link_local_dispatch, _link_local_pending) = mpsc::unbounded_channel();
        let state = Arc::new(CompletionState::new(incoming_dispatch, link_local_dispatch));
        let dispatch = tokio::spawn(dispatch_incoming(
            incoming_pending,
            incoming_sender,
            state.clone(),
        ));
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
        state.process(queue.try_pop_leased().unwrap());
        sender
            .try_push(
                Completion::IncomingStream(StreamId::new(connection, 0)),
                credits.try_acquire().unwrap(),
            )
            .unwrap();
        state.process(queue.try_pop_leased().unwrap());

        assert_eq!(credits.used(), 1);
        assert!(credits.try_acquire().is_none());
        let accepted = incoming.recv().await.unwrap();
        assert_eq!(accepted.source, peer);
        drop(accepted);
        assert_eq!(credits.used(), 0);
        state.stop();
        dispatch.await.unwrap();
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
                SingleTransportConfig::new(
                    server_credential.identity,
                    config(&server_credential, roots, true),
                    TransportLimits::new(limits(), NonZeroUsize::new(128).unwrap()),
                ),
            )
            .unwrap(),
        );
        let client = Arc::new(
            Transport::spawn_client(
                DriverId::from_u16(2),
                io(),
                SingleTransportConfig::new(
                    client_credential.identity,
                    config(&client_credential, roots, true),
                    TransportLimits::new(limits(), NonZeroUsize::new(128).unwrap()),
                ),
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
