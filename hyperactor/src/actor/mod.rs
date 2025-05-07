#![allow(dead_code)] // Allow until this is used outside of tests.

//! This module contains all the core traits required to define and manage actors.

use std::any::TypeId;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::future::IntoFuture;
use std::hash::Hash;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate as hyperactor; // for macros
use crate::ActorRef;
use crate::Data;
use crate::Message;
use crate::Named;
use crate::RemoteMessage;
use crate::cap;
use crate::checkpoint::CheckpointError;
use crate::checkpoint::Checkpointable;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::mailbox::MailboxError;
use crate::mailbox::MailboxSenderError;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::Undeliverable;
use crate::mailbox::log::MessageLogError;
use crate::proc::Instance;
use crate::proc::InstanceCell;
use crate::proc::Ports;
use crate::proc::Proc;
use crate::reference::ActorId;
use crate::reference::GangId;
use crate::reference::Index;
use crate::reference::ProcId;
use crate::supervision::ActorSupervisionEvent;

pub mod remote;

/// An Actor is an independent, asynchronous thread of execution. Each
/// actor instance has a mailbox, whose messages are delivered through
/// the method [`Actor::handle`].
///
/// Actors communicate with each other by way of message passing.
/// Actors are assumed to be _deterministic_: that is, the state of an
/// actor is determined by the set (and order) of messages it receives.
#[async_trait]
pub trait Actor: Sized + Send + Sync + Debug + 'static {
    /// The type of initialization parameters accepted by this actor.
    type Params: Send + Sync + 'static;

    /// Creates a new actor instance given its instantiation parameters.
    async fn new(params: Self::Params) -> Result<Self, anyhow::Error>;

    /// Initialize the actor, after the runtime has been fully initialized.
    /// Init thus provides a mechanism by which an actor can reliably and always
    /// receive some initial event that can be used to kick off further
    /// (potentially delayed) processing.
    async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Default implementation: no init.
        Ok(())
    }

    /// Spawn a child actor, given a spawning capability (usually given by [`Instance`]).
    /// The spawned actor will be supervised by the parent (spawning) actor.
    async fn spawn(
        cap: &impl cap::CanSpawn,
        params: Self::Params,
    ) -> anyhow::Result<ActorHandle<Self>> {
        cap.spawn(params).await
    }

    /// Spawns this actor in a detached state, handling its messages
    /// in a background task. The returned handle is used to control
    /// the actor's lifecycle and to interact with it.
    ///
    /// Actors spawned through `spawn_detached` are not attached to a supervision
    /// hierarchy, and not managed by a [`Proc`].
    async fn spawn_detached(params: Self::Params) -> Result<ActorHandle<Self>, anyhow::Error> {
        Proc::local().spawn("anon", params).await
    }

    /// This method is used by the runtime to spawn the actor server. It can be
    /// used by actors that require customized runtime setups
    /// (e.g., dedicated actor threads), or want to use a custom tokio runtime.
    fn spawn_server_task<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future.instrument(tracing::debug_span!("server_task").or_current()))
    }

    /// Handle actor supervision event. Return `Ok(true)`` if the event is handled here.
    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        // By default, the supervision event is not handled, caller is expected to bubble it up.
        Ok(false)
    }

    /// Default undeliverable message handling behavior.
    async fn handle_undeliverable_message(
        &mut self,
        this: &Instance<Self>,
        Undeliverable(envelope): Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        let to = envelope.dest().clone();
        let from = envelope.sender().clone();

        // We don't expect returned messages we didn't send.
        assert_eq!(from, *this.self_id());

        // By this return, the default behavior is to exit this
        // actor's message loop and transition the actor's state to
        // `ActorStatus::Failed` with this error message.
        Err(anyhow::anyhow!(format!(
            "a message from {} to {} was undeliverable and returned: {:?}",
            from,
            to,
            envelope
                .error()
                .map_or("None".to_string(), |e| format!("{:?}", e))
        )))
    }
}

/// A Handler allows an actor to handle a specific message type.
#[async_trait]
pub trait Handler<M>: Actor {
    /// Handle the next M-typed message.
    async fn handle(&mut self, this: &Instance<Self>, message: M) -> Result<(), anyhow::Error>;
}

/// We provide this handler to indicate that actors can handle the [`Signal`] message.
/// Its actual handler is implemented by the runtime.
#[async_trait]
impl<A: Actor> Handler<Signal> for A {
    async fn handle(
        &mut self,
        _this: &Instance<Self>,
        _message: Signal,
    ) -> Result<(), anyhow::Error> {
        unimplemented!("signal handler should not be called directly")
    }
}

/// This handler provides a default behavior when a message sent by
/// the actor to another is returned due to delivery failure.
#[async_trait]
impl<A: Actor> Handler<Undeliverable<MessageEnvelope>> for A {
    async fn handle(
        &mut self,
        this: &Instance<Self>,
        message: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        self.handle_undeliverable_message(this, message).await
    }
}

/// A RemotableActor may be spawned remotely, and receive messages across
/// process boundaries.
pub trait RemotableActor: Actor
where
    Self::Params: RemoteMessage,
{
    /// A type-erased entry point to spawn this actor. This is primarily used by Hyperactor's
    /// remote actor registration mechanism.
    // TODO: consider making this 'private' -- by moving it into a non-public trait as in [`cap`].
    fn gspawn(
        proc: &Proc,
        name: &str,
        serialized_params: Data,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>>;

    /// The type ID of this actor.
    fn get_type_id() -> TypeId {
        TypeId::of::<Self>()
    }
}

impl<A> RemotableActor for A
where
    A: Actor + RemoteActor,
    A: Binds<A>,
    A::Params: RemoteMessage,
{
    fn gspawn(
        proc: &Proc,
        name: &str,
        serialized_params: Data,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>> {
        // TODO: fix
        let proc = proc.clone();
        let name = name.to_string();
        Box::pin(async move {
            let handle = proc
                .spawn::<A>(&name, bincode::deserialize(&serialized_params)?)
                .await?;
            // Gspawned actors are always type erased, and thus are only able to receive serialized
            // messages; they can be safely exported.
            //
            // This will soon be replaced by an export mechanism.
            Ok(handle.bind::<A>().actor_id)
        })
    }
}

#[async_trait]
impl<T> Checkpointable for T
where
    T: RemoteMessage + Clone,
{
    type State = T;
    async fn save(&self) -> Result<Self::State, CheckpointError> {
        Ok(self.clone())
    }

    async fn load(state: Self::State) -> Result<Self, CheckpointError> {
        Ok(state)
    }
}

/// Errors that occur while serving actors. Each error is associated
/// with the ID of the actor being served.
#[derive(Debug)]
pub struct ActorError {
    actor_id: ActorId,
    kind: ActorErrorKind,
}

/// The kinds of actor serving errors.
#[derive(thiserror::Error, Debug)]
pub enum ActorErrorKind {
    /// Error while processing actor, i.e., returned by the actor's
    /// processing method.
    #[error("processing error: {0}")]
    Processing(#[source] anyhow::Error),

    /// Unwound stracktrace of a panic.
    #[error("panic: {0}")]
    Panic(#[source] anyhow::Error),

    /// Error during actor initialization.
    #[error("initialization error: {0}")]
    Init(#[source] anyhow::Error),

    /// An underlying mailbox error.
    #[error(transparent)]
    Mailbox(#[from] MailboxError),

    /// An underlying mailbox sender error.
    #[error(transparent)]
    MailboxSender(#[from] MailboxSenderError),

    /// An underlying checkpoint error.
    #[error("checkpoint error: {0}")]
    Checkpoint(#[source] CheckpointError),

    /// An underlying message log error.
    #[error("message log error: {0}")]
    MessageLog(#[source] MessageLogError),

    /// The actor's state could not be determined.
    #[error("actor is in an indeterminate state")]
    IndeterminateState,

    /// A special kind of error that allows us to clone errors: we can keep the
    /// error string, but we lose the error structure.
    #[error("{0}")]
    Passthrough(#[from] anyhow::Error),
}

impl ActorError {
    /// Create a new actor server error with the provided id and kind.
    pub(crate) fn new(actor_id: ActorId, kind: ActorErrorKind) -> Self {
        Self { actor_id, kind }
    }

    /// Passthrough this error.
    fn passthrough(&self) -> Self {
        ActorError::new(
            self.actor_id.clone(),
            ActorErrorKind::Passthrough(anyhow::anyhow!("{}", self.kind)),
        )
    }
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "serving {}: ", self.actor_id)?;
        fmt::Display::fmt(&self.kind, f)
    }
}

impl std::error::Error for ActorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

impl From<MailboxError> for ActorError {
    fn from(inner: MailboxError) -> Self {
        Self::new(inner.actor_id().clone(), ActorErrorKind::from(inner))
    }
}

impl From<MailboxSenderError> for ActorError {
    fn from(inner: MailboxSenderError) -> Self {
        Self::new(
            inner.location().actor_id().clone(),
            ActorErrorKind::from(inner),
        )
    }
}

/// A collection of signals to control the behavior of the actor.
/// Signals are internal runtime control plane messages and should not be
/// sent outside of the runtime.
///
/// These messages are not handled directly by actors; instead, the runtime
/// handles the various signals.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub enum Signal {
    /// Stop the actor, after draining messages.
    DrainAndStop,

    /// Stop the actor immediately.
    Stop,

    /// The direct child with the given PID was stopped.
    ChildStopped(Index),
}

/// The runtime status of an actor.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ActorStatus {
    /// The actor status is unknown.
    Unknown,
    /// The actor was created, but not yet started.
    Created,
    /// The actor is initializing. It is not yet ready to receive messages.
    Initializing,
    /// The actor is in "client" state: the user is managing the actor's
    /// mailboxes manually.
    Client,
    /// The actor is ready to receive messages, but is currently idle.
    Idle,
    /// The actor has been processing a message, beginning at the specified
    /// instant. The message handler and arm is included.
    /// TODO: we shoudl use interned representations here, so we don't copy
    /// strings willy-nilly.
    Processing(SystemTime, Option<(String, Option<String>)>),
    /// The actor has been saving its state.
    Saving(SystemTime),
    /// The actor has been loading its state.
    Loading(SystemTime),
    /// The actor is stopping. It is draining messages.
    Stopping,
    /// The actor is stopped. It is no longer processing messages.
    Stopped,
    /// The actor failed with the provided actor error formatted in string
    /// representation.
    Failed(String),
}

impl ActorStatus {
    /// Tells whether the status is a terminal state.
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed(_))
    }

    /// Tells whether the status represents a failure.
    pub(crate) fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Create a passthrough of this status. The returned status is a clone,
    /// except that [`ActorStatus::Failed`] is replaced with its passthrough.
    fn passthrough(&self) -> Self {
        match self {
            Self::Unknown => Self::Unknown,
            Self::Created => Self::Created,
            Self::Initializing => Self::Initializing,
            Self::Client => Self::Client,
            Self::Idle => Self::Idle,
            Self::Processing(instant, handler) => {
                Self::Processing(instant.clone(), handler.clone())
            }
            Self::Saving(instant) => Self::Saving(instant.clone()),
            Self::Loading(instant) => Self::Loading(instant.clone()),
            Self::Stopping => Self::Stopping,
            Self::Stopped => Self::Stopped,
            Self::Failed(err) => Self::Failed(err.clone()),
        }
    }
}

impl fmt::Display for ActorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::Created => write!(f, "created"),
            Self::Initializing => write!(f, "initializing"),
            Self::Client => write!(f, "client"),
            Self::Idle => write!(f, "idle"),
            Self::Processing(instant, None) => {
                write!(
                    f,
                    "processing for {}ms",
                    RealClock
                        .system_time_now()
                        .duration_since(instant.clone())
                        .unwrap_or_default()
                        .as_millis()
                )
            }
            Self::Processing(instant, Some((handler, None))) => {
                write!(
                    f,
                    "{}: processing for {}ms",
                    handler,
                    RealClock
                        .system_time_now()
                        .duration_since(instant.clone())
                        .unwrap_or_default()
                        .as_millis()
                )
            }
            Self::Processing(instant, Some((handler, Some(arm)))) => {
                write!(
                    f,
                    "{},{}: processing for {}ms",
                    handler,
                    arm,
                    RealClock
                        .system_time_now()
                        .duration_since(instant.clone())
                        .unwrap_or_default()
                        .as_millis()
                )
            }
            Self::Saving(instant) => {
                write!(
                    f,
                    "saving for {}ms",
                    RealClock
                        .system_time_now()
                        .duration_since(instant.clone())
                        .unwrap_or_default()
                        .as_millis()
                )
            }
            Self::Loading(instant) => {
                write!(
                    f,
                    "loading for {}ms",
                    RealClock
                        .system_time_now()
                        .duration_since(instant.clone())
                        .unwrap_or_default()
                        .as_millis()
                )
            }
            Self::Stopping => write!(f, "stopping"),
            Self::Stopped => write!(f, "stopped"),
            Self::Failed(err) => write!(f, "failed: {}", err),
        }
    }
}

/// ActorHandles represent a (local) serving actor. It is used to access
/// its messaging and signal ports, as well as to synchronize with its
/// lifecycle (e.g., providing joins).  Once dropped, the handle is
/// detached from the underlying actor instance, and there is no longer
/// any way to join it.
///
/// Correspondingly, [`crate::ActorRef`]s refer to (possibly) remote
/// actors.
pub struct ActorHandle<A: Actor> {
    cell: InstanceCell,
    ports: Arc<Ports<A>>,
}

/// A handle to a running (local) actor.
impl<A: Actor> ActorHandle<A> {
    pub(crate) fn new(cell: InstanceCell, ports: Arc<Ports<A>>) -> Self {
        Self { cell, ports }
    }

    /// The actor's cell. Used primarily for testing.
    /// TODO: this should not be a public API.
    pub(crate) fn cell(&self) -> &InstanceCell {
        &self.cell
    }

    /// The [`ActorId`] of the actor represented by this handle.
    pub fn actor_id(&self) -> &ActorId {
        self.cell.actor_id()
    }

    /// Signal the actor to drain its current messages and then stop.
    pub fn drain_and_stop(&self) -> Result<(), ActorError> {
        self.cell.signal(Signal::DrainAndStop)
    }

    /// A watch that observes the lifecycle state of the actor.
    pub fn status(&self) -> watch::Receiver<ActorStatus> {
        self.cell.status().clone()
    }

    /// Send a message to the actor. Messages sent through the handle
    /// are always queued in process, and do not require serialization.
    pub fn send<M: Message>(&self, message: M) -> Result<(), MailboxSenderError>
    where
        A: Handler<M>,
    {
        self.ports.get().send(message)
    }

    /// Return a port for the provided message type handled by the actor.
    pub fn port<M: Message>(&self) -> PortHandle<M>
    where
        A: Handler<M>,
    {
        self.ports.get()
    }

    /// TEMPORARY: bind...
    /// TODO: we shoudl also have a default binding(?)
    pub fn bind<R: Binds<A>>(&self) -> ActorRef<R> {
        self.cell.bind(self.ports.as_ref())
    }
}

/// IntoFuture allows users to await the handle to join it. The future
/// resolves when the actor itself has stopped processing messages.
/// The future resolves to the actor's final status.
impl<A: Actor> IntoFuture for ActorHandle<A> {
    type Output = ActorStatus;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let future = async move {
            let mut status_receiver = self.cell.status().clone();
            let result = status_receiver.wait_for(ActorStatus::is_terminal).await;
            match result {
                Err(_) => ActorStatus::Unknown,
                Ok(status) => status.passthrough(),
            }
        };

        future.boxed()
    }
}

impl<A: Actor> Debug for ActorHandle<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("ActorHandle").field("cell", &"..").finish()
    }
}

impl<A: Actor> Clone for ActorHandle<A> {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
            ports: self.ports.clone(),
        }
    }
}

/// RemoteActor is a marker trait for types that can be used as
/// remote actor references. All [`Actor`]s are thus referencable;
/// but other types may also implement this in order to separately
/// specify actor interfaces.
pub trait RemoteActor: Named + Send + Sync {}

/// Binds determines how an actor's ports are bound to a specific
/// reference type.
pub trait Binds<A: Actor>: RemoteActor {
    /// Bind ports in this actor.
    fn bind(ports: &Ports<A>);
}

/// Handles is a marker trait specifying that message type [`M`]
/// is handled by a specific actor type.
pub trait RemoteHandles<M: RemoteMessage>: RemoteActor {}

/// Create a [`RemoteActor`] handling a specific set of message types.
/// This is used to create an [`ActorRef`] without having to
/// depend on the actor's implementation.
/// Currently only a single message type is supported. This
/// restriction will be lifted once we enable multi-handle [`ActorRef`]s.
#[macro_export(local_inner_macros)]
macro_rules! alias {
    ($alias:ident, $($message:ty),+) => {
        #[doc = "The generated alias struct."]
        #[derive(Debug, Named)]
        #[named(dump = false)]
        pub struct $alias;
        impl $crate::actor::RemoteActor for $alias {}

        impl<A> $crate::actor::Binds<A> for $alias
        where
            A: $crate::Actor $(+ $crate::actor::Handler<$message>)+ {
            fn bind(ports: &$crate::proc::Ports<A>) {
                $(
                    ports.bind::<$message>();
                )+
            }
        }

        $(impl $crate::actor::RemoteHandles<$message> for $alias {})+
    };
}

/// GangRefs are typed references to gangs.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct GangRef<A: RemoteActor> {
    gang_id: GangId,
    phantom: PhantomData<A>,
}

impl<A: RemoteActor> GangRef<A> {
    /// Return an ActorRef corresponding with the provided rank in
    /// this gang.  Does not check the validity of the rank, so the
    /// returned identifier is not guaranteed to refer to a valid rank.
    pub fn rank(&self, rank: Index) -> ActorRef<A> {
        let GangRef {
            gang_id: GangId(world_id, name),
            ..
        } = self;
        ActorRef::attest(ActorId(ProcId(world_id.clone(), rank), name.clone(), 0))
    }

    /// Return the gang ID.
    pub fn gang_id(&self) -> &GangId {
        &self.gang_id
    }
}

impl<A: RemoteActor> Clone for GangRef<A> {
    fn clone(&self) -> Self {
        Self {
            gang_id: self.gang_id.clone(),
            phantom: PhantomData,
        }
    }
}

// TODO: remove, replace with attest
impl<A: RemoteActor> From<GangId> for GangRef<A> {
    fn from(gang_id: GangId) -> Self {
        Self {
            gang_id,
            phantom: PhantomData,
        }
    }
}

impl<A: RemoteActor> From<GangRef<A>> for GangId {
    fn from(gang_ref: GangRef<A>) -> Self {
        gang_ref.gang_id
    }
}

impl<'a, A: RemoteActor> From<&'a GangRef<A>> for &'a GangId {
    fn from(gang_ref: &'a GangRef<A>) -> Self {
        &gang_ref.gang_id
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::Mailbox;
    use crate::OncePortHandle;
    use crate::PortRef;
    use crate::checkpoint::CheckpointError;
    use crate::checkpoint::Checkpointable;
    use crate::test_utils::pingpong::PingPongActor;
    use crate::test_utils::pingpong::PingPongActorParams;
    use crate::test_utils::pingpong::PingPongMessage;
    use crate::test_utils::proc_supervison::ProcSupervisionCoordinator;

    #[derive(Debug)]
    struct EchoActor(PortRef<u64>);

    #[async_trait]
    impl Actor for EchoActor {
        type Params = PortRef<u64>;

        async fn new(params: PortRef<u64>) -> Result<Self, anyhow::Error> {
            Ok(Self(params))
        }
    }

    #[async_trait]
    impl Handler<u64> for EchoActor {
        async fn handle(
            &mut self,
            this: &Instance<Self>,
            message: u64,
        ) -> Result<(), anyhow::Error> {
            let Self(port) = self;
            port.send(this, message)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_server_basic() {
        let proc = Proc::local();
        let client = proc.attach("client").unwrap();
        let (tx, mut rx) = client.open_port();
        let handle = proc.spawn::<EchoActor>("echo", tx.bind()).await.unwrap();
        handle.send(123u64).unwrap();
        handle.drain_and_stop().unwrap();
        handle.await;

        assert_eq!(rx.drain(), vec![123u64]);
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let proc = Proc::local();
        let client = proc.attach("client").unwrap();
        let (undeliverable_msg_tx, _) = client.open_port();

        let ping_pong_actor_params = PingPongActorParams::new(undeliverable_msg_tx.bind(), None);
        let ping_handle = proc
            .spawn::<PingPongActor>("ping", ping_pong_actor_params.clone())
            .await
            .unwrap();
        let pong_handle = proc
            .spawn::<PingPongActor>("pong", ping_pong_actor_params)
            .await
            .unwrap();

        let (local_port, local_receiver) = client.open_once_port();

        ping_handle
            .send(PingPongMessage(10, pong_handle.bind(), local_port.bind()))
            .unwrap();

        assert!(local_receiver.recv().await.unwrap());
    }

    #[tokio::test]
    async fn test_ping_pong_on_handler_error() {
        let proc = Proc::local();
        let client = proc.attach("client").unwrap();
        let (undeliverable_msg_tx, _) = client.open_port();

        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let error_ttl = 66;
        let ping_pong_actor_params =
            PingPongActorParams::new(undeliverable_msg_tx.bind(), Some(error_ttl));
        let ping_handle = proc
            .spawn::<PingPongActor>("ping", ping_pong_actor_params.clone())
            .await
            .unwrap();
        let pong_handle = proc
            .spawn::<PingPongActor>("pong", ping_pong_actor_params)
            .await
            .unwrap();

        let (local_port, local_receiver) = client.open_once_port();

        ping_handle
            .send(PingPongMessage(
                error_ttl + 1, // will encounter an error at TTL=66
                pong_handle.bind(),
                local_port.bind(),
            ))
            .unwrap();

        // TODO: Fix this receiver hanging issue in T200423722.
        let res: Result<Result<bool, MailboxError>, tokio::time::error::Elapsed> =
            timeout(Duration::from_secs(5), local_receiver.recv()).await;
        assert!(res.is_err());
    }

    #[derive(Debug)]
    struct InitActor(bool);

    #[async_trait]
    impl Actor for InitActor {
        type Params = ();

        async fn new(_params: ()) -> Result<Self, anyhow::Error> {
            Ok(Self(false))
        }

        async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
            self.0 = true;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<OncePortHandle<bool>> for InitActor {
        async fn handle(
            &mut self,
            _this: &Instance<Self>,
            port: OncePortHandle<bool>,
        ) -> Result<(), anyhow::Error> {
            port.send(self.0)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_init() {
        let proc = Proc::local();
        let handle = proc.spawn::<InitActor>("init", ()).await.unwrap();
        let client = proc.attach("client").unwrap();

        let (port, receiver) = client.open_once_port();
        handle.send(port).unwrap();
        assert!(receiver.recv().await.unwrap());

        handle.drain_and_stop().unwrap();
        handle.await;
    }

    #[derive(Debug)]
    struct CheckpointActor {
        // The actor does nothing but sum the values of messages.
        sum: u64,
        port: PortRef<u64>,
    }

    #[async_trait]
    impl Actor for CheckpointActor {
        type Params = PortRef<u64>;

        async fn new(params: PortRef<u64>) -> Result<Self, anyhow::Error> {
            Ok(Self {
                sum: 0,
                port: params,
            })
        }
    }

    #[async_trait]
    impl Handler<u64> for CheckpointActor {
        async fn handle(&mut self, this: &Instance<Self>, value: u64) -> Result<(), anyhow::Error> {
            self.sum += value;
            self.port.send(this, self.sum)?;
            Ok(())
        }
    }

    #[async_trait]
    impl Checkpointable for CheckpointActor {
        type State = (u64, PortRef<u64>);

        async fn save(&self) -> Result<Self::State, CheckpointError> {
            Ok((self.sum, self.port.clone()))
        }

        async fn load(state: Self::State) -> Result<Self, CheckpointError> {
            let (sum, port) = state;
            Ok(CheckpointActor { sum, port })
        }
    }

    type MultiValues = Arc<Mutex<(u64, String)>>;

    struct MultiValuesTest {
        proc: Proc,
        values: MultiValues,
        handle: ActorHandle<MultiActor>,
        client: Mailbox,
    }

    impl MultiValuesTest {
        async fn new() -> Self {
            let proc = Proc::local();
            let values: MultiValues = Arc::new(Mutex::new((0, "".to_string())));
            let handle = proc
                .spawn::<MultiActor>("myactor", values.clone())
                .await
                .unwrap();
            let client = proc.attach("client").unwrap();
            Self {
                proc,
                values,
                handle,
                client,
            }
        }

        fn send<M>(&self, message: M)
        where
            M: RemoteMessage,
            MultiActor: Handler<M>,
        {
            self.handle.send(message).unwrap()
        }

        async fn sync(&self) {
            let (port, done) = self.client.open_once_port::<bool>();
            self.handle.send(port).unwrap();
            assert!(done.recv().await.unwrap());
        }

        fn get_values(&self) -> (u64, String) {
            self.values.lock().unwrap().clone()
        }
    }

    impl MultiValuesTest {}

    #[derive(Debug)]
    #[hyperactor::export(u64, String)]
    struct MultiActor(MultiValues);

    #[async_trait]
    impl Actor for MultiActor {
        type Params = MultiValues;

        async fn new(init: Self::Params) -> Result<Self, anyhow::Error> {
            Ok(Self(init))
        }
    }

    #[async_trait]
    impl Handler<u64> for MultiActor {
        async fn handle(
            &mut self,
            _this: &Instance<Self>,
            message: u64,
        ) -> Result<(), anyhow::Error> {
            let mut vals = self.0.lock().unwrap();
            vals.0 = message;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<String> for MultiActor {
        async fn handle(
            &mut self,
            _this: &Instance<Self>,
            message: String,
        ) -> Result<(), anyhow::Error> {
            let mut vals = self.0.lock().unwrap();
            vals.1 = message;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<OncePortHandle<bool>> for MultiActor {
        async fn handle(
            &mut self,
            _this: &Instance<Self>,
            message: OncePortHandle<bool>,
        ) -> Result<(), anyhow::Error> {
            message.send(true).unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_multi_handler_refs() {
        let test = MultiValuesTest::new().await;

        test.send(123u64);
        test.send("foo".to_string());
        test.sync().await;
        assert_eq!(test.get_values(), (123u64, "foo".to_string()));

        let myref: ActorRef<MultiActor> = test.handle.bind();

        myref.port().send(&test.client, 321u64).unwrap();
        test.sync().await;
        assert_eq!(test.get_values(), (321u64, "foo".to_string()));

        myref.port().send(&test.client, "bar".to_string()).unwrap();
        test.sync().await;
        assert_eq!(test.get_values(), (321u64, "bar".to_string()));
    }

    #[tokio::test]
    async fn test_ref_alias() {
        let test = MultiValuesTest::new().await;

        test.send(123u64);
        test.send("foo".to_string());

        hyperactor::alias!(MyActorAlias, u64, String);

        let myref: ActorRef<MyActorAlias> = test.handle.bind();
        myref.port().send(&test.client, "biz".to_string()).unwrap();
        myref.port().send(&test.client, 999u64).unwrap();

        test.sync().await;
        assert_eq!(test.get_values(), (999u64, "biz".to_string()));
    }
}
