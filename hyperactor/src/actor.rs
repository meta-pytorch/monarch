/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)] // Allow until this is used outside of tests.

//! This module contains all the core traits required to define and manage actors.

use std::any::TypeId;
use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use futures::FutureExt;
use futures::future::BoxFuture;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use typeuri::Named;

use crate as hyperactor; // for macros
use crate::ActorRef;
use crate::Data;
use crate::Message;
use crate::RemoteMessage;
use crate::checkpoint::CheckpointError;
use crate::checkpoint::Checkpointable;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::context;
use crate::mailbox::MailboxError;
use crate::mailbox::MailboxSenderError;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::Undeliverable;
use crate::mailbox::UndeliverableMessageError;
use crate::mailbox::log::MessageLogError;
use crate::message::Castable;
use crate::message::IndexedErasedUnbound;
use crate::proc::Context;
use crate::proc::Instance;
use crate::proc::InstanceCell;
use crate::proc::Ports;
use crate::proc::Proc;
use crate::reference::ActorId;
use crate::reference::Index;
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
pub trait Actor: Sized + Send + 'static {
    /// Initialize the actor, after the runtime has been fully initialized.
    /// Init thus provides a mechanism by which an actor can reliably and always
    /// receive some initial event that can be used to kick off further
    /// (potentially delayed) processing.
    async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Default implementation: no init.
        Ok(())
    }

    /// Cleanup things used by this actor before shutting down. Notably this function
    /// is async and allows more complex cleanup. Simpler cleanup can be handled
    /// by the impl Drop for this Actor.
    /// If err is not None, it is the error that this actor is failing with. Any
    /// errors returned by this function will be logged and ignored.
    /// If err is None, any errors returned by this function will be propagated
    /// as an ActorError.
    /// This function is not called if there is a panic in the actor, as the
    /// actor may be in an indeterminate state. It is also not called if the
    /// process is killed, there is no atexit handler or signal handler.
    async fn cleanup(
        &mut self,
        _this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // Default implementation: no cleanup.
        Ok(())
    }

    /// Spawn a child actor, given a spawning capability (usually given by [`Instance`]).
    /// The spawned actor will be supervised by the parent (spawning) actor.
    fn spawn(self, cx: &impl context::Actor) -> anyhow::Result<ActorHandle<Self>> {
        cx.instance().spawn(self)
    }

    /// Spawns this actor in a detached state, handling its messages
    /// in a background task. The returned handle is used to control
    /// the actor's lifecycle and to interact with it.
    ///
    /// Actors spawned through `spawn_detached` are not attached to a supervision
    /// hierarchy, and not managed by a [`Proc`].
    fn spawn_detached(self) -> Result<ActorHandle<Self>, anyhow::Error> {
        Proc::local().spawn("anon", self)
    }

    /// This method is used by the runtime to spawn the actor server. It can be
    /// used by actors that require customized runtime setups
    /// (e.g., dedicated actor threads), or want to use a custom tokio runtime.
    #[hyperactor::instrument_infallible]
    fn spawn_server_task<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
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
        cx: &Instance<Self>,
        envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        handle_undeliverable_message(cx, envelope)
    }

    /// If overridden, we will use this name in place of the
    /// ActorId for talking about this actor in supervision error
    /// messages.
    fn display_name(&self) -> Option<String> {
        None
    }
}

/// Default implementation of [`Actor::handle_undeliverable_message`]. Defined
/// as a free function so that `Actor` implementations that override
/// [`Actor::handle_undeliverable_message`] can fallback to this default.
pub fn handle_undeliverable_message<A: Actor>(
    cx: &Instance<A>,
    Undeliverable(envelope): Undeliverable<MessageEnvelope>,
) -> Result<(), anyhow::Error> {
    assert_eq!(envelope.sender(), cx.self_id());

    anyhow::bail!(UndeliverableMessageError::DeliveryFailure { envelope });
}

/// An actor that does nothing. It is used to represent "client only" actors,
/// returned by [`Proc::instance`].
#[async_trait]
impl Actor for () {}

impl Referable for () {}

impl Binds<()> for () {
    fn bind(_ports: &Ports<Self>) {
        // Binds no ports.
    }
}

/// A Handler allows an actor to handle a specific message type.
#[async_trait]
pub trait Handler<M>: Actor {
    /// Handle the next M-typed message.
    async fn handle(&mut self, cx: &Context<Self>, message: M) -> Result<(), anyhow::Error>;
}

/// We provide this handler to indicate that actors can handle the [`Signal`] message.
/// Its actual handler is implemented by the runtime.
#[async_trait]
impl<A: Actor> Handler<Signal> for A {
    async fn handle(&mut self, _cx: &Context<Self>, _message: Signal) -> Result<(), anyhow::Error> {
        unimplemented!("signal handler should not be called directly")
    }
}

/// This handler provides a default behavior when a message sent by
/// the actor to another is returned due to delivery failure.
#[async_trait]
impl<A: Actor> Handler<Undeliverable<MessageEnvelope>> for A {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        self.handle_undeliverable_message(cx, message).await
    }
}

/// This handler enables actors to unbind the [IndexedErasedUnbound]
/// message, and forward the result to corresponding handler.
#[async_trait]
impl<A, M> Handler<IndexedErasedUnbound<M>> for A
where
    A: Handler<M>,
    M: Castable,
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: IndexedErasedUnbound<M>,
    ) -> anyhow::Result<()> {
        let message = msg.downcast()?.bind()?;
        Handler::handle(self, cx, message).await
    }
}

/// An `Actor` that can be spawned remotely.
///
/// Bounds explained:
/// - `Actor`: only actors may be remotely spawned.
/// - `Referable`: marks the type as eligible for typed remote
///   references (`ActorRef<A>`); required because remote spawn
///   ultimately hands back an `ActorId` that higher-level APIs may
///   re-type as `ActorRef<A>`.
/// - `Binds<Self>`: lets the runtime wire this actor's message ports
///   when it is spawned (the blanket impl calls `handle.bind::<Self>()`).
///
/// `gspawn` is a type-erased entry point used by the remote
/// spawn/registry machinery. It takes serialized params and returns
/// the new actor's `ActorId`; application code shouldn't call it
/// directly.
#[async_trait]
pub trait RemoteSpawn: Actor + Referable + Binds<Self> {
    /// The type of parameters used to instantiate the actor remotely.
    type Params: RemoteMessage;

    /// Creates a new actor instance given its instantiation parameters.
    /// The `environment` allows whoever is responsible for spawning this actor
    /// to pass in additional context that may be useful.
    async fn new(params: Self::Params, environment: Flattrs) -> anyhow::Result<Self>;

    /// A type-erased entry point to spawn this actor. This is
    /// primarily used by hyperactor's remote actor registration
    /// mechanism.
    // TODO: consider making this 'private' -- by moving it into a non-public trait as in [`cap`].
    fn gspawn(
        proc: &Proc,
        name: &str,
        serialized_params: Data,
        environment: Flattrs,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>> {
        let proc = proc.clone();
        let name = name.to_string();
        Box::pin(async move {
            let params = bincode::deserialize(&serialized_params)?;
            let actor = Self::new(params, environment).await?;
            let handle = proc.spawn(&name, actor)?;
            // We return only the ActorId, not a typed ActorRef.
            // Callers that hold this ID can interact with the actor
            // only via the serialized/opaque messaging path, which
            // makes it safe to export across process boundaries.
            //
            // Note: the actor itself is still `A`-typed here; we
            // merely restrict the *capability* we hand out to an
            // untyped identifier.
            //
            // This will be replaced by a proper export/registry
            // mechanism.
            Ok(handle.bind::<Self>().actor_id)
        })
    }

    /// The type ID of this actor.
    fn get_type_id() -> TypeId {
        TypeId::of::<Self>()
    }
}

/// If an actor implements Default, we use this as the
/// `RemoteSpawn` implementation, too.
#[async_trait]
impl<A: Actor + Referable + Binds<Self> + Default> RemoteSpawn for A {
    type Params = ();

    async fn new(_params: Self::Params, _environment: Flattrs) -> anyhow::Result<Self> {
        Ok(Default::default())
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
    /// The ActorId for the actor that generated this error.
    pub actor_id: Box<ActorId>,
    /// The kind of error that occurred.
    pub kind: Box<ActorErrorKind>,
}

/// The kinds of actor serving errors.
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ActorErrorKind {
    /// Generic error with a formatted message.
    #[error("{0}")]
    Generic(String),

    /// An error that occurred while trying to handle a supervision event.
    #[error("{0} while handling {1}")]
    ErrorDuringHandlingSupervision(String, Box<ActorSupervisionEvent>),

    /// The actor did not attempt to handle
    #[error("{0}")]
    UnhandledSupervisionEvent(Box<ActorSupervisionEvent>),

    /// The actor was explicitly aborted with the provided reason.
    #[error("actor explicitly aborted due to: {0}")]
    Aborted(String),
}

impl ActorErrorKind {
    /// Error while processing actor, i.e., returned by the actor's
    /// processing method.
    pub fn processing(err: anyhow::Error) -> Self {
        // Unbox err from the anyhow err. Check if it is an ActorErrorKind object.
        // If it is directly use it as the new ActorError's ActorErrorKind.
        // This lets us directly pass the ActorErrorKind::UnhandledSupervisionEvent
        // up the handling infrastructure.
        err.downcast::<ActorErrorKind>()
            .unwrap_or_else(|err| Self::Generic(err.to_string()))
    }

    /// Unwound stracktrace of a panic.
    pub fn panic(err: anyhow::Error) -> Self {
        Self::Generic(format!("panic: {}", err))
    }

    /// Error during actor initialization.
    pub fn init(err: anyhow::Error) -> Self {
        Self::Generic(format!("initialization error: {}", err))
    }

    /// Error during actor cleanup.
    pub fn cleanup(err: anyhow::Error) -> Self {
        Self::Generic(format!("cleanup error: {}", err))
    }

    /// An underlying mailbox error.
    pub fn mailbox(err: MailboxError) -> Self {
        Self::Generic(err.to_string())
    }

    /// An underlying mailbox sender error.
    pub fn mailbox_sender(err: MailboxSenderError) -> Self {
        Self::Generic(err.to_string())
    }

    /// An underlying checkpoint error.
    pub fn checkpoint(err: CheckpointError) -> Self {
        Self::Generic(format!("checkpoint error: {}", err))
    }

    /// An underlying message log error.
    pub fn message_log(err: MessageLogError) -> Self {
        Self::Generic(format!("message log error: {}", err))
    }

    /// The actor's state could not be determined.
    pub fn indeterminate_state() -> Self {
        Self::Generic("actor is in an indeterminate state".to_string())
    }
}

impl ActorError {
    /// Create a new actor server error with the provided id and kind.
    pub(crate) fn new(actor_id: &ActorId, kind: ActorErrorKind) -> Self {
        Self {
            actor_id: Box::new(actor_id.clone()),
            kind: Box::new(kind),
        }
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
        Self {
            actor_id: Box::new(inner.actor_id().clone()),
            kind: Box::new(ActorErrorKind::mailbox(inner)),
        }
    }
}

impl From<MailboxSenderError> for ActorError {
    fn from(inner: MailboxSenderError) -> Self {
        Self {
            actor_id: Box::new(inner.location().actor_id().clone()),
            kind: Box::new(ActorErrorKind::mailbox_sender(inner)),
        }
    }
}

/// A collection of signals to control the behavior of the actor.
/// Signals are internal runtime control plane messages and should not be
/// sent outside of the runtime.
///
/// These messages are not handled directly by actors; instead, the runtime
/// handles the various signals.
#[derive(Clone, Debug, Serialize, Deserialize, typeuri::Named)]
pub enum Signal {
    /// Stop the actor, after draining messages.
    DrainAndStop(String),

    /// Stop the actor immediately.
    Stop(String),

    /// The direct child with the given PID was stopped.
    ChildStopped(Index),

    /// Abort the actor. This will exit the actor loop with an error,
    /// causing a supervision event to propagate up the supervision
    /// hierarchy.
    Abort(String),
}
wirevalue::register_type!(Signal);

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Signal::DrainAndStop(reason) => write!(f, "DrainAndStop({})", reason),
            Signal::Stop(reason) => write!(f, "Stop({})", reason),
            Signal::ChildStopped(index) => write!(f, "ChildStopped({})", index),
            Signal::Abort(reason) => write!(f, "Abort({})", reason),
        }
    }
}

/// Information about a message handler being processed.
///
/// Uses `Cow<'static, str>` to avoid string copies on the hot path.
/// The typename and arm are typically static strings from `TypeInfo`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct HandlerInfo {
    /// The type name of the message being handled.
    pub typename: Cow<'static, str>,
    /// The enum arm being handled, if the message is an enum.
    pub arm: Option<Cow<'static, str>>,
}

impl HandlerInfo {
    /// Create a new `HandlerInfo` from static strings (zero-copy).
    pub fn from_static(typename: &'static str, arm: Option<&'static str>) -> Self {
        Self {
            typename: Cow::Borrowed(typename),
            arm: arm.map(Cow::Borrowed),
        }
    }

    /// Create a new `HandlerInfo` from owned strings.
    pub fn from_owned(typename: String, arm: Option<String>) -> Self {
        Self {
            typename: Cow::Owned(typename),
            arm: arm.map(Cow::Owned),
        }
    }
}

impl fmt::Display for HandlerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.arm {
            Some(arm) => write!(f, "{}.{}", self.typename, arm),
            None => write!(f, "{}", self.typename),
        }
    }
}

/// The runtime status of an actor.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Clone,
    typeuri::Named,
    EnumAsInner
)]
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
    /// instant. The message handler info is included.
    Processing(SystemTime, Option<HandlerInfo>),
    /// The actor has been saving its state.
    Saving(SystemTime),
    /// The actor has been loading its state.
    Loading(SystemTime),
    /// The actor is stopping. It is draining messages.
    Stopping,
    /// The actor is stopped with a provided reason.
    /// It is no longer processing messages.
    Stopped(String),
    /// The actor failed with the provided actor error.
    Failed(ActorErrorKind),
}

impl ActorStatus {
    /// Tells whether the status is a terminal state.
    pub fn is_terminal(&self) -> bool {
        self.is_stopped() || self.is_failed()
    }

    /// Create a generic failure status with the provided error message.
    pub fn generic_failure(message: impl Into<String>) -> Self {
        Self::Failed(ActorErrorKind::Generic(message.into()))
    }

    fn span_string(&self) -> &'static str {
        self.arm().unwrap_or_default()
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
                        .duration_since(*instant)
                        .unwrap_or_default()
                        .as_millis()
                )
            }
            Self::Processing(instant, Some(handler_info)) => {
                write!(
                    f,
                    "{}: processing for {}ms",
                    handler_info,
                    RealClock
                        .system_time_now()
                        .duration_since(*instant)
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
                        .duration_since(*instant)
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
                        .duration_since(*instant)
                        .unwrap_or_default()
                        .as_millis()
                )
            }
            Self::Stopping => write!(f, "stopping"),
            Self::Stopped(reason) => write!(f, "stopped: {}", reason),
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
    pub fn drain_and_stop(&self, reason: &str) -> Result<(), ActorError> {
        tracing::info!("ActorHandle::drain_and_stop called: {}", self.actor_id());
        self.cell.signal(Signal::DrainAndStop(reason.to_string()))
    }

    /// A watch that observes the lifecycle state of the actor.
    pub fn status(&self) -> watch::Receiver<ActorStatus> {
        self.cell.status().clone()
    }

    /// Send a message to the actor. Messages sent through the handle
    /// are always queued in process, and do not require serialization.
    pub fn send<M: Message>(
        &self,
        cx: &impl context::Actor,
        message: M,
    ) -> Result<(), MailboxSenderError>
    where
        A: Handler<M>,
    {
        self.ports.get().send(cx, message)
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
                Ok(status) => status.clone(),
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

/// `Referable` is a marker trait for types that can appear as
/// remote references across process boundaries.
///
/// It is not limited to concrete [`Actor`] implementations. For
/// example, façade types generated by [`behavior!`] implement
/// `Referable` so that you can hand out restricted or stable APIs
/// while still using the same remote messaging machinery.
///
/// Implementing this trait means the type can be identified (`Named`)
/// so the runtime knows what it is.
///
///  In contrast, [`RemoteSpawn`] is the trait that marks *actors*
/// that can actually be **spawned remotely**. A behavior may be a
/// `Referable` but is never a `RemoteSpawn`.
pub trait Referable: Named {}

/// Binds determines how an actor's ports are bound to a specific
/// reference type.
pub trait Binds<A: Actor>: Referable {
    /// Bind ports in this actor.
    fn bind(ports: &Ports<A>);
}

/// Handles is a marker trait specifying that message type [`M`]
/// is handled by a specific actor type.
pub trait RemoteHandles<M: RemoteMessage>: Referable {}

/// Check if the actor behaves-as the a given behavior (defined by [`behavior!`]).
///
/// ```
/// # use serde::Serialize;
/// # use serde::Deserialize;
/// # use typeuri::Named;
/// # use hyperactor::Actor;
///
/// // First, define a behavior, based on handling a single message type `()`.
/// hyperactor::behavior!(UnitBehavior, ());
///
/// #[derive(Debug, Default)]
/// struct MyActor;
///
/// impl Actor for MyActor {}
///
/// #[async_trait::async_trait]
/// impl hyperactor::Handler<()> for MyActor {
///     async fn handle(
///         &mut self,
///         _cx: &hyperactor::Context<Self>,
///         _message: (),
///     ) -> Result<(), anyhow::Error> {
///         // no-op
///         Ok(())
///     }
/// }
///
/// hyperactor::assert_behaves!(MyActor as UnitBehavior);
/// ```
#[macro_export]
macro_rules! assert_behaves {
    ($ty:ty as $behavior:ty) => {
        const _: fn() = || {
            fn check<B: hyperactor::actor::Binds<$ty>>() {}
            check::<$behavior>();
        };
    };
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use rand::seq::SliceRandom;
    use timed_test::async_timed_test;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;
    use crate as hyperactor;
    use crate::Actor;
    use crate::OncePortHandle;
    use crate::PortRef;
    use crate::checkpoint::CheckpointError;
    use crate::checkpoint::Checkpointable;
    use crate::config;
    use crate::context::Mailbox as _;
    use crate::id;
    use crate::mailbox::BoxableMailboxSender as _;
    use crate::mailbox::MailboxSender;
    use crate::mailbox::PortLocation;
    use crate::mailbox::monitored_return_handle;
    use crate::ordering::SEQ_INFO;
    use crate::ordering::SeqInfo;
    use crate::test_utils::pingpong::PingPongActor;
    use crate::test_utils::pingpong::PingPongMessage;
    use crate::test_utils::proc_supervison::ProcSupervisionCoordinator; // for macros

    #[derive(Debug)]
    struct EchoActor(PortRef<u64>);

    #[async_trait]
    impl Actor for EchoActor {}

    #[async_trait]
    impl Handler<u64> for EchoActor {
        async fn handle(&mut self, cx: &Context<Self>, message: u64) -> Result<(), anyhow::Error> {
            let Self(port) = self;
            port.send(cx, message)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_server_basic() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (tx, mut rx) = client.open_port();
        let actor = EchoActor(tx.bind());
        let handle = proc.spawn::<EchoActor>("echo", actor).unwrap();
        handle.send(&client, 123u64).unwrap();
        handle.drain_and_stop("test").unwrap();
        handle.await;

        assert_eq!(rx.drain(), vec![123u64]);
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (undeliverable_msg_tx, _) = client.open_port();

        let ping_actor = PingPongActor::new(Some(undeliverable_msg_tx.bind()), None, None);
        let pong_actor = PingPongActor::new(Some(undeliverable_msg_tx.bind()), None, None);
        let ping_handle = proc.spawn::<PingPongActor>("ping", ping_actor).unwrap();
        let pong_handle = proc.spawn::<PingPongActor>("pong", pong_actor).unwrap();

        let (local_port, local_receiver) = client.open_once_port();

        ping_handle
            .send(
                &client,
                PingPongMessage(10, pong_handle.bind(), local_port.bind()),
            )
            .unwrap();

        assert!(local_receiver.recv().await.unwrap());
    }

    #[tokio::test]
    async fn test_ping_pong_on_handler_error() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (undeliverable_msg_tx, _) = client.open_port();

        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let error_ttl = 66;

        let ping_actor =
            PingPongActor::new(Some(undeliverable_msg_tx.bind()), Some(error_ttl), None);
        let pong_actor =
            PingPongActor::new(Some(undeliverable_msg_tx.bind()), Some(error_ttl), None);
        let ping_handle = proc.spawn::<PingPongActor>("ping", ping_actor).unwrap();
        let pong_handle = proc.spawn::<PingPongActor>("pong", pong_actor).unwrap();

        let (local_port, local_receiver) = client.open_once_port();

        ping_handle
            .send(
                &client,
                PingPongMessage(
                    error_ttl + 1, // will encounter an error at TTL=66
                    pong_handle.bind(),
                    local_port.bind(),
                ),
            )
            .unwrap();

        // TODO: Fix this receiver hanging issue in T200423722.
        #[allow(clippy::disallowed_methods)]
        let res: Result<Result<bool, MailboxError>, tokio::time::error::Elapsed> =
            timeout(Duration::from_secs(5), local_receiver.recv()).await;
        assert!(res.is_err());
    }

    #[derive(Debug)]
    struct InitActor(bool);

    #[async_trait]
    impl Actor for InitActor {
        async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
            self.0 = true;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<OncePortHandle<bool>> for InitActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            port: OncePortHandle<bool>,
        ) -> Result<(), anyhow::Error> {
            port.send(cx, self.0)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_init() {
        let proc = Proc::local();
        let actor = InitActor(false);
        let handle = proc.spawn::<InitActor>("init", actor).unwrap();
        let (client, _) = proc.instance("client").unwrap();

        let (port, receiver) = client.open_once_port();
        handle.send(&client, port).unwrap();
        assert!(receiver.recv().await.unwrap());

        handle.drain_and_stop("test").unwrap();
        handle.await;
    }

    #[derive(Debug)]
    struct CheckpointActor {
        // The actor does nothing but sum the values of messages.
        sum: u64,
        port: PortRef<u64>,
    }

    #[async_trait]
    impl Actor for CheckpointActor {}

    #[async_trait]
    impl Handler<u64> for CheckpointActor {
        async fn handle(&mut self, cx: &Context<Self>, value: u64) -> Result<(), anyhow::Error> {
            self.sum += value;
            self.port.send(cx, self.sum)?;
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
        client: Instance<()>,
        _client_handle: ActorHandle<()>,
    }

    impl MultiValuesTest {
        async fn new() -> Self {
            let proc = Proc::local();
            let values: MultiValues = Arc::new(Mutex::new((0, "".to_string())));
            let actor = MultiActor(values.clone());
            let handle = proc.spawn::<MultiActor>("myactor", actor).unwrap();
            let (client, client_handle) = proc.instance("client").unwrap();
            Self {
                proc,
                values,
                handle,
                client,
                _client_handle: client_handle,
            }
        }

        fn send<M>(&self, message: M)
        where
            M: RemoteMessage,
            MultiActor: Handler<M>,
        {
            self.handle.send(&self.client, message).unwrap()
        }

        async fn sync(&self) {
            let (port, done) = self.client.open_once_port::<bool>();
            self.handle.send(&self.client, port).unwrap();
            assert!(done.recv().await.unwrap());
        }

        fn get_values(&self) -> (u64, String) {
            self.values.lock().unwrap().clone()
        }
    }

    #[derive(Debug)]
    #[hyperactor::export(handlers = [u64, String])]
    struct MultiActor(MultiValues);

    #[async_trait]
    impl Actor for MultiActor {}

    #[async_trait]
    impl Handler<u64> for MultiActor {
        async fn handle(&mut self, _cx: &Context<Self>, message: u64) -> Result<(), anyhow::Error> {
            let mut vals = self.0.lock().unwrap();
            vals.0 = message;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<String> for MultiActor {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
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
            cx: &Context<Self>,
            message: OncePortHandle<bool>,
        ) -> Result<(), anyhow::Error> {
            message.send(cx, true).unwrap();
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
    async fn test_ref_behavior() {
        let test = MultiValuesTest::new().await;

        test.send(123u64);
        test.send("foo".to_string());

        hyperactor::behavior!(MyActorBehavior, u64, String);

        let myref: ActorRef<MyActorBehavior> = test.handle.bind();
        myref.port().send(&test.client, "biz".to_string()).unwrap();
        myref.port().send(&test.client, 999u64).unwrap();

        test.sync().await;
        assert_eq!(test.get_values(), (999u64, "biz".to_string()));
    }

    #[tokio::test]
    async fn test_actor_handle_downcast() {
        #[derive(Debug, Default)]
        struct NothingActor;

        impl Actor for NothingActor {}

        // Just test that we can round-trip the handle through a downcast.

        let proc = Proc::local();
        let handle = proc.spawn("nothing", NothingActor).unwrap();
        let cell = handle.cell();

        // Invalid actor doesn't succeed.
        assert!(cell.downcast_handle::<EchoActor>().is_none());

        let handle = cell.downcast_handle::<NothingActor>().unwrap();
        handle.drain_and_stop("test").unwrap();
        handle.await;
    }

    // Returning the sequence number assigned to the message.
    #[derive(Debug)]
    #[hyperactor::export(handlers = [String, Callback])]
    struct GetSeqActor(PortRef<(String, SeqInfo)>);

    #[async_trait]
    impl Actor for GetSeqActor {}

    #[async_trait]
    impl Handler<String> for GetSeqActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            message: String,
        ) -> Result<(), anyhow::Error> {
            let Self(port) = self;
            let seq_info = cx.headers().get(SEQ_INFO).unwrap();
            port.send(cx, (message, seq_info.clone()))?;
            Ok(())
        }
    }

    // Unlike Handler<String>, where the sender provides the string message
    // directly, in Handler<Callback>, sender needs to provide a port, and
    // handler will reply that port with its own callback port. Then sender can
    // send the string message through this callback port.
    #[derive(Clone, Debug, Serialize, Deserialize, Named)]
    struct Callback(PortRef<PortRef<String>>);

    #[async_trait]
    impl Handler<Callback> for GetSeqActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            message: Callback,
        ) -> Result<(), anyhow::Error> {
            let (handle, mut receiver) = cx.open_port::<String>();
            let callback_ref = handle.bind();
            message.0.send(cx, callback_ref).unwrap();
            let msg = receiver.recv().await.unwrap();
            self.handle(cx, msg).await
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_sequencing_actor_handle_basic() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (tx, mut rx) = client.open_port();

        let actor_handle = proc.spawn("get_seq", GetSeqActor(tx.bind())).unwrap();

        // Verify that unbound handle can send message.
        actor_handle.send(&client, "unbound".to_string()).unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            ("unbound".to_string(), SeqInfo::Direct)
        );

        let actor_ref: ActorRef<GetSeqActor> = actor_handle.bind();

        let session_id = client.sequencer().session_id();
        let mut expected_seq = 0;
        // Interleave messages sent through the handle and the reference.
        for m in 0..10 {
            actor_handle.send(&client, format!("{m}")).unwrap();
            expected_seq += 1;
            assert_eq!(
                rx.recv().await.unwrap(),
                (
                    format!("{m}"),
                    SeqInfo::Session {
                        session_id,
                        seq: expected_seq,
                    }
                )
            );

            for n in 0..2 {
                actor_ref.port().send(&client, format!("{m}-{n}")).unwrap();
                expected_seq += 1;
                assert_eq!(
                    rx.recv().await.unwrap(),
                    (
                        format!("{m}-{n}"),
                        SeqInfo::Session {
                            session_id,
                            seq: expected_seq,
                        }
                    )
                );
            }
        }
    }

    // Test that actor ports share a sequence while non-actor ports get their own.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_sequencing_mixed_actor_and_non_actor_ports() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();

        // Port for receiving seq info from actor handler
        let (actor_tx, mut actor_rx) = client.open_port();

        // Channel for receiving seq info from non-actor port
        let (non_actor_tx, mut non_actor_rx) = mpsc::unbounded_channel::<Option<SeqInfo>>();

        let actor_handle = proc.spawn("get_seq", GetSeqActor(actor_tx.bind())).unwrap();
        let actor_ref: ActorRef<GetSeqActor> = actor_handle.bind();

        // Create a non-actor port using open_enqueue_port
        let non_actor_tx_clone = non_actor_tx.clone();
        let non_actor_port_handle = client.mailbox().open_enqueue_port(move |headers, _m: ()| {
            let seq_info = headers.get(SEQ_INFO);
            non_actor_tx_clone.send(seq_info).unwrap();
            Ok(())
        });

        // Bind the port to get a port ID
        non_actor_port_handle.bind();
        let non_actor_port_id = match non_actor_port_handle.location() {
            PortLocation::Bound(port_id) => port_id,
            _ => panic!("port_handle should be bound"),
        };
        assert!(!non_actor_port_id.is_actor_port());

        let session_id = client.sequencer().session_id();

        // Send to actor ports via ActorHandle - seq 1
        actor_handle.send(&client, "msg1".to_string()).unwrap();
        assert_eq!(
            actor_rx.recv().await.unwrap().1,
            SeqInfo::Session { session_id, seq: 1 }
        );

        // Send to actor ports via ActorRef - seq 2 (shared with ActorHandle)
        actor_ref.port().send(&client, "msg2".to_string()).unwrap();
        assert_eq!(
            actor_rx.recv().await.unwrap().1,
            SeqInfo::Session { session_id, seq: 2 }
        );

        // Send to non-actor port - has its own sequence starting at 1
        non_actor_port_handle.send(&client, ()).unwrap();
        assert_eq!(
            non_actor_rx.recv().await.unwrap(),
            Some(SeqInfo::Session { session_id, seq: 1 })
        );

        // Send more to actor ports via ActorHandle - seq continues at 3
        actor_handle.send(&client, "msg3".to_string()).unwrap();
        assert_eq!(
            actor_rx.recv().await.unwrap().1,
            SeqInfo::Session { session_id, seq: 3 }
        );

        // Send more to non-actor port - its sequence continues at 2
        non_actor_port_handle.send(&client, ()).unwrap();
        assert_eq!(
            non_actor_rx.recv().await.unwrap(),
            Some(SeqInfo::Session { session_id, seq: 2 })
        );

        // Send via ActorRef again - seq 4
        actor_ref.port().send(&client, "msg4".to_string()).unwrap();
        assert_eq!(
            actor_rx.recv().await.unwrap().1,
            SeqInfo::Session { session_id, seq: 4 }
        );

        actor_handle.drain_and_stop("test cleanup").unwrap();
        actor_handle.await;
    }

    // Test that messages from different clients get independent sequence schemes.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_sequencing_multiple_clients() {
        let proc = Proc::local();
        let (client1, _) = proc.instance("client1").unwrap();
        let (client2, _) = proc.instance("client2").unwrap();

        // Port for receiving seq info from actor handler
        let (tx, mut rx) = client1.open_port();

        let actor_handle = proc.spawn("get_seq", GetSeqActor(tx.bind())).unwrap();
        let actor_ref: ActorRef<GetSeqActor> = actor_handle.bind();

        // Each client should have a different session_id
        let session_id_1 = client1.sequencer().session_id();
        let session_id_2 = client2.sequencer().session_id();
        assert_ne!(session_id_1, session_id_2);

        // Send from client1 via ActorHandle - seq 1 for session_id_1
        actor_handle.send(&client1, "c1_msg1".to_string()).unwrap();
        assert_eq!(
            rx.recv().await.unwrap().1,
            SeqInfo::Session {
                session_id: session_id_1,
                seq: 1
            }
        );

        // Send from client2 via ActorHandle - seq 1 for session_id_2 (independent)
        actor_handle.send(&client2, "c2_msg1".to_string()).unwrap();
        assert_eq!(
            rx.recv().await.unwrap().1,
            SeqInfo::Session {
                session_id: session_id_2,
                seq: 1
            }
        );

        // Send from client1 via ActorRef - seq 2 for session_id_1
        actor_ref
            .port()
            .send(&client1, "c1_msg2".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap().1,
            SeqInfo::Session {
                session_id: session_id_1,
                seq: 2
            }
        );

        // Send from client2 via ActorRef - seq 2 for session_id_2
        actor_ref
            .port()
            .send(&client2, "c2_msg2".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap().1,
            SeqInfo::Session {
                session_id: session_id_2,
                seq: 2
            }
        );

        // Interleave more messages to further verify independence
        actor_handle.send(&client1, "c1_msg3".to_string()).unwrap();
        assert_eq!(
            rx.recv().await.unwrap().1,
            SeqInfo::Session {
                session_id: session_id_1,
                seq: 3
            }
        );

        actor_ref
            .port()
            .send(&client2, "c2_msg3".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap().1,
            SeqInfo::Session {
                session_id: session_id_2,
                seq: 3
            }
        );

        actor_handle.drain_and_stop("test cleanup").unwrap();
        actor_handle.await;
    }

    // Verify that ordering is guarranteed based on
    //   * (sender actor , client actor, port stream)
    // not
    //   * (sender actor, client actor)
    //
    // For "port stream",
    //   * actor ports of the same actor belongs to the same stream;
    //   * non-actor port has its independent stream.
    //
    // Specifically, in this test,
    //   * client sends a Callback message to dest actor's handler;
    //   * while dest actor is still processing that message, client sends
    //     another non-handler message to dest actor.
    //
    // If the ordering is based on (sender actor, client actor), this test would
    // hang, since dest actor is deadlock on waiting for the 2nd message while
    // still processing the 2nd message.
    //
    // But since port stream is also part of the ordering guarrantee, such
    // deadlock should not happen.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_sequencing_actor_handle_callback() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (tx, mut rx) = client.open_port();

        let actor_handle = proc.spawn("get_seq", GetSeqActor(tx.bind())).unwrap();
        let actor_ref: ActorRef<GetSeqActor> = actor_handle.bind();

        let (callback_tx, mut callback_rx) = client.open_port();
        // Client sends the 1st message
        actor_ref
            .send(&client, Callback(callback_tx.bind()))
            .unwrap();
        let msg_port_ref = callback_rx.recv().await.unwrap();
        // client sends the 2nd message. At this time, GetSeqActor is still
        // processing the 1st message, and waiting for the 2nd message.
        msg_port_ref.send(&client, "finally".to_string()).unwrap();

        let session_id = client.sequencer().session_id();
        // passing this assert means GetSeqActor processed the 2nd message.
        assert_eq!(
            rx.recv().await.unwrap(),
            (
                "finally".to_string(),
                SeqInfo::Session { session_id, seq: 1 }
            )
        );
    }

    // Adding a delay before sending the destination proc. Useful for tests
    // requiring latency injection.
    #[derive(Clone, Debug)]
    struct DelayedMailboxSender {
        relay_tx: mpsc::UnboundedSender<MessageEnvelope>,
    }

    impl DelayedMailboxSender {
        // Use a random latency between 0 and 1 second if the plan is empty.
        fn new(
            // The proc that hosts the dest actor. By posting envelope to this
            // proc, this proc will route that evenlope to the dest actor.
            dest_proc: Proc,
            // Vec index is the message seq - 1, value is the order this message
            // would be relayed to the dest actor. Dest actor is responsible to
            // ensure itself processes these messages in order.
            relay_orders: Vec<usize>,
        ) -> Self {
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel::<MessageEnvelope>();

            tokio::spawn(async move {
                let mut buffer = Vec::new();

                for _ in 0..relay_orders.len() {
                    let envelope = relay_rx.recv().await.unwrap();
                    buffer.push(envelope);
                }

                for m in buffer.clone() {
                    let seq = match m.headers().get(SEQ_INFO).expect("seq should be set") {
                        SeqInfo::Session { seq, .. } => seq as usize,
                        SeqInfo::Direct => panic!("expected Session variant"),
                    };
                    // seq no is one-based.
                    let order = relay_orders[seq - 1];
                    buffer[order] = m;
                }

                let dest_proc_clone = dest_proc.clone();
                for msg in buffer {
                    dest_proc_clone.post(msg, monitored_return_handle());
                }
            });

            Self { relay_tx }
        }
    }

    impl MailboxSender for DelayedMailboxSender {
        fn post_unchecked(
            &self,
            envelope: MessageEnvelope,
            _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        ) {
            self.relay_tx.send(envelope).unwrap();
        }
    }

    async fn assert_out_of_order_delivery(expected: Vec<(String, u64)>, relay_orders: Vec<usize>) {
        let local_proc: Proc = Proc::local();
        let (client, _) = local_proc.instance("local").unwrap();
        let (tx, mut rx) = client.open_port();

        let handle = local_proc.spawn("get_seq", GetSeqActor(tx.bind())).unwrap();
        let actor_ref: ActorRef<GetSeqActor> = handle.bind();

        let remote_proc = Proc::new(
            id!(remote[0]),
            DelayedMailboxSender::new(local_proc.clone(), relay_orders).boxed(),
        );
        let (remote_client, _) = remote_proc.instance("remote").unwrap();
        // Send the messages out in the order of their expected sequence numbers.
        let mut messages = expected.clone();
        messages.sort_by_key(|v| v.1);
        for (message, _seq) in messages {
            actor_ref.send(&remote_client, message).unwrap();
        }
        let session_id = remote_client.sequencer().session_id();
        for expect in expected {
            let expected = (
                expect.0,
                SeqInfo::Session {
                    session_id,
                    seq: expect.1,
                },
            );
            assert_eq!(rx.recv().await.unwrap(), expected);
        }

        handle.drain_and_stop("test cleanup").unwrap();
        handle.await;
    }

    // Send several messages, use DelayedMailboxSender and the relay orders to
    // ensure these messages will arrive at handler's workq out-of-order.
    // Then verify the actor handler will still process these messages based on
    // their sending order if reordering buffer is enabled.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_sequencing_actor_ref_known_delivery_order() {
        let config = hyperactor_config::global::lock();

        // relay order is second, third, first
        let relay_orders = vec![2, 0, 1];

        // By disabling the actor side re-ordering buffer, the mssages will
        // be processed in the same order as they sent out.
        let _guard = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, false);
        assert_out_of_order_delivery(
            vec![
                ("second".to_string(), 2),
                ("third".to_string(), 3),
                ("first".to_string(), 1),
            ],
            relay_orders.clone(),
        )
        .await;

        // By enabling the actor side re-ordering buffer, the mssages will
        // be re-ordered before being processed.
        let _guard = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        assert_out_of_order_delivery(
            vec![
                ("first".to_string(), 1),
                ("second".to_string(), 2),
                ("third".to_string(), 3),
            ],
            relay_orders.clone(),
        )
        .await;
    }

    // Send a large nubmer of messages, use DelayedMailboxSender to ensure these
    // messages will arrive at handler's workq in a random order. Then verify the
    // actor handler will still process these messages based on their sending
    // order with reordering buffer enabled.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_sequencing_actor_ref_random_delivery_order() {
        let config = hyperactor_config::global::lock();

        // By enabling the actor side re-ordering buffer, the mssages will
        // be re-ordered before being processed.
        let _guard = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        let expected = (0..10000)
            .map(|i| (format!("msg{i}"), i + 1))
            .collect::<Vec<_>>();

        let mut relay_orders: Vec<usize> = (0..10000).collect();
        relay_orders.shuffle(&mut rand::thread_rng());
        assert_out_of_order_delivery(expected, relay_orders).await;
    }
}
