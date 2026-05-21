/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mailboxes are the central message-passing mechanism in Hyperactor.
//!
//! Each actor owns a mailbox to which other actors can deliver messages.
//! An actor can open one or more typed _ports_ in the mailbox; messages
//! are in turn delivered to specific ports.
//!
//! Mailboxes are associated with an [`ActorAddr`] (given by `actor_id`
//! in the following example):
//!
//! ```
//! # use hyperactor::mailbox::Mailbox;
//! # use hyperactor::Endpoint as _;
//! # use hyperactor::Proc;
//! # use hyperactor::{ActorAddr, ProcAddr};
//! # tokio_test::block_on(async {
//! # let proc = Proc::isolated();
//! # let (client, _) = proc.client("client").unwrap();
//! # let actor_id = proc.proc_addr().actor_addr("actor");
//! let mbox = Mailbox::new(actor_id);
//! let (port, mut receiver) = mbox.open_port::<u64>();
//!
//! port.post(&client, 123);
//! assert_eq!(receiver.recv().await.unwrap(), 123u64);
//! # })
//! ```
//!
//! Mailboxes also provide a form of one-shot ports, called [`OncePort`],
//! that permits at most one message transmission:
//!
//! ```
//! # use hyperactor::mailbox::Mailbox;
//! # use hyperactor::Endpoint as _;
//! # use hyperactor::Proc;
//! # use hyperactor::{ActorAddr, ProcAddr};
//! # tokio_test::block_on(async {
//! # let proc = Proc::isolated();
//! # let (client, _) = proc.client("client").unwrap();
//! # let actor_id = proc.proc_addr().actor_addr("actor");
//! let mbox = Mailbox::new(actor_id);
//!
//! let (port, receiver) = mbox.open_once_port::<u64>();
//!
//! port.post(&client, 123u64);
//! assert_eq!(receiver.recv().await.unwrap(), 123u64);
//! # })
//! ```
//!
//! [`OncePort`]s are correspondingly used for RPC replies in the actor
//! system.
//!
//! ## Remote ports and serialization
//!
//! Mailboxes allow delivery of serialized messages to named ports:
//!
//! 1) Ports restrict message types to (serializable) [`Message`]s.
//! 2) Each [`Port`] is associated with a [`PortAddr`] which globally names the port.
//! 3) [`Mailbox`] provides interfaces to deliver serialized
//!    messages to ports named by their [`PortAddr`].
//!
//! While this complicates the interface somewhat, it allows the
//! implementation to avoid a serialization roundtrip when passing
//! messages locally.
//!
//! ## Undeliverable-message log invariants (UM-*)
//!
//! The `undelivered_message_abandoned` log at
//! `UndeliverableMailboxSender::post_unchecked` is a user-facing
//! surface: it fires when a message could not be delivered *and*
//! could not be returned to its sender. The following invariants
//! govern its shape so the log stays scannable and its downstream
//! consumers (Scuba, alerts) stay stable.
//!
//! - **UM-1 (bounded abandoned-message log).** The log must not emit
//!   unbounded `envelope.headers().to_string()` or
//!   `envelope.data().to_string()`. Payload observability is provided
//!   by `message_type` (`data.typename()`) and `data_len`
//!   (`data.len()`) — cheap, bounded, and type-safe.
//!
//! - **UM-2 (stable compatibility fields).** The `actor_name` and
//!   `actor_id` fields stay on the log with their current values and
//!   types. Readability improvements are strictly additive on this
//!   surface; renames or removals require a separate migration diff
//!   that coordinates with downstream consumers.
//!
//! - **UM-3a (destination naming).** When the envelope carries no
//!   `OPERATION_ENDPOINT`, the format string names the transport
//!   destination: `"message not delivered to <dest>"`.
//!
//! - **UM-3b (operation naming).** When the envelope carries
//!   `OPERATION_ENDPOINT`, the format string names the operation:
//!   `"abandoned message for <endpoint>"`.
//!
//!   `OPERATION_*` keys live in `hyperactor::mailbox::headers`
//!   because the readers (this log, the undeliverable formatter)
//!   live in `hyperactor` and can't depend upward on
//!   `monarch_hyperactor`. Keys whose consumers are not at this
//!   layer don't belong here.

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::ops::Bound::Excluded;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use enum_as_inner::EnumAsInner;
use futures::Sink;
use futures::Stream;
use hyperactor_config::Flattrs;
use hyperactor_telemetry::hash_to_u64;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use typeuri::Named;

use crate::ActorAddr;
use crate::Addr;
use crate::Endpoint;
use crate::EndpointLocation;
// for macros
use crate::OncePortRef;
use crate::PortAddr;
use crate::PortRef;
use crate::Proc;
use crate::ProcAddr;
use crate::accum::Accumulator;
use crate::accum::ReducerSpec;
use crate::accum::StreamingReducerOpts;
use crate::actor::ActorStatus;
use crate::actor::Signal;
use crate::actor::remote::USER_PORT_OFFSET;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::channel::SendError;
use crate::channel::TxStatus;
use crate::context;
use crate::gateway::Gateway;
use crate::id::ActorId;
use crate::metrics;
use crate::ordering::SEQ_INFO;
use crate::ordering::SeqInfo;
use crate::port::Port;

mod undeliverable;
/// For [`Undeliverable`], a message type for delivery failures.
pub use undeliverable::LostMessage;
pub use undeliverable::Undeliverable;
pub use undeliverable::UndeliverableMessageError;
pub use undeliverable::custom_monitored_return_handle;
pub use undeliverable::monitored_return_handle; // TODO: Audit
/// For [`MailboxAdminMessage`], a message type for mailbox administration.
pub mod mailbox_admin_message;
pub use mailbox_admin_message::MailboxAdminMessage;
pub use mailbox_admin_message::MailboxAdminMessageHandler;
/// For message headers and latency tracking.
pub mod headers;

/// Message collects the necessary requirements for messages that are deposited
/// into mailboxes.
pub trait Message: Send + Sync + 'static {}
impl<M: Send + Sync + 'static> Message for M {}

/// RemoteMessage extends [`Message`] by requiring that the messages
/// also be serializable, and can thus traverse process boundaries.
/// RemoteMessages must also specify a globally unique type name (a URI).
pub trait RemoteMessage: Message + Named + Serialize + DeserializeOwned {}

impl<M: Message + Named + Serialize + DeserializeOwned> RemoteMessage for M {}

/// Type alias for bytestring data used throughout the system.
pub type Data = Vec<u8>;

/// A structured delivery failure with optional metadata.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, typeuri::Named, Clone)]
#[error("{kind}")]
pub struct DeliveryFailure {
    /// The delivery failure kind.
    pub kind: DeliveryFailureKind,

    /// Additional keyed metadata for higher-level delivery features.
    pub attrs: Flattrs,
}

impl DeliveryFailure {
    /// Create a delivery failure with no additional metadata.
    pub fn new(kind: impl Into<DeliveryFailureKind>) -> Self {
        Self {
            kind: kind.into(),
            attrs: Flattrs::new(),
        }
    }

    /// Create a delivery failure with additional keyed metadata.
    pub fn with_attrs(kind: impl Into<DeliveryFailureKind>, attrs: Flattrs) -> Self {
        Self {
            kind: kind.into(),
            attrs,
        }
    }
}

/// The kind of delivery failure.
#[derive(
    thiserror::Error,
    Debug,
    Serialize,
    Deserialize,
    EnumAsInner,
    typeuri::Named,
    Clone,
    PartialEq,
    Eq
)]
pub enum DeliveryFailureKind {
    /// The destination reference does not denote a valid recipient.
    #[error("{0}")]
    InvalidReference(#[from] InvalidReference),

    /// The message could not be delivered for transport or receiver-lifecycle
    /// reasons.
    #[error("{0}")]
    Undeliverable(#[from] UndeliverableReason),

    /// The message exceeded its TTL.
    #[error("{0}")]
    Expired(#[from] ExpiredDelivery),
}

/// An invalid destination reference.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[error("invalid reference {target}: {reason}")]
pub struct InvalidReference {
    /// The invalid target.
    pub target: Addr,

    /// Why the reference is invalid.
    pub reason: InvalidReferenceReason,
}

impl InvalidReference {
    /// Create an invalid-reference failure.
    pub fn new(target: impl Into<Addr>, reason: InvalidReferenceReason) -> Self {
        Self {
            target: target.into(),
            reason,
        }
    }
}

/// Why a destination reference is invalid.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum InvalidReferenceReason {
    /// The actor does not exist.
    #[error("actor does not exist")]
    ActorNotExist,

    /// The handler port is not bound.
    #[error("handler not bound")]
    HandlerNotBound,

    /// The actor stopped before delivery.
    #[error("actor stopped")]
    ActorStopped,

    /// The actor failed before delivery.
    #[error("actor failed")]
    ActorFailed,

    /// The port was never allocated.
    #[error("port never allocated")]
    PortNeverAllocated,

    /// The message is incompatible with the destination.
    #[error("protocol mismatch")]
    ProtocolMismatch,

    /// The envelope was delivered to the wrong mailbox owner.
    #[error("wrong mailbox owner")]
    WrongMailboxOwner,
}

/// A delivery failure caused by message expiration.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[error("ttl expired for {target}")]
pub struct ExpiredDelivery {
    /// The destination whose delivery expired.
    pub target: PortAddr,
}

impl ExpiredDelivery {
    /// Create an expired-delivery failure.
    pub fn new(target: impl Into<PortAddr>) -> Self {
        Self {
            target: target.into(),
        }
    }
}

/// A non-invalid-reference delivery failure.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum UndeliverableReason {
    /// Delivery failed while carrying the message.
    #[error("{0}")]
    Transport(#[from] TransportFailure),

    /// The destination port's ordinary recipient is gone.
    #[error("{0}")]
    PortGone(#[from] PortGone),
}

impl UndeliverableReason {
    /// Whether this failure is a transport failure.
    pub fn is_transport(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}

/// A transport delivery failure.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[error("transport failure to {target}: {reason}")]
pub struct TransportFailure {
    /// The delivery target.
    pub target: Addr,

    /// Why transport failed.
    pub reason: TransportFailureReason,
}

impl TransportFailure {
    /// Create a transport failure.
    pub fn new(target: impl Into<Addr>, reason: TransportFailureReason) -> Self {
        Self {
            target: target.into(),
            reason,
        }
    }
}

/// Why transport failed.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum TransportFailureReason {
    /// The channel closed.
    #[error("channel closed: {addr}")]
    ChannelClosed {
        /// The channel address.
        addr: ChannelAddr,
    },

    /// Delivery acknowledgement timed out.
    #[error("ack timed out: {addr}")]
    AckTimedOut {
        /// The channel address.
        addr: ChannelAddr,
    },

    /// Dialing the destination failed.
    #[error("dial failed: {addr}: {error}")]
    DialFailed {
        /// The channel address.
        addr: ChannelAddr,

        /// The dial error.
        error: String,
    },

    /// The router has no route and is not authoritative for destination
    /// existence.
    #[error("no route")]
    NoRoute,

    /// A weak reference in the delivery path could not be upgraded.
    #[error("link unavailable: {0}")]
    LinkUnavailable(String),

    /// The forwarder is unavailable.
    #[error("forwarder unavailable")]
    ForwarderUnavailable,
}

/// A port whose ordinary recipient is gone.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[error("port gone: {port}")]
pub struct PortGone {
    /// The port whose recipient is gone.
    pub port: PortAddr,

    /// The message type, when known.
    pub message_type: Option<String>,
}

impl PortGone {
    /// Create a port-gone failure.
    pub fn new(port: impl Into<PortAddr>, message_type: Option<String>) -> Self {
        Self {
            port: port.into(),
            message_type,
        }
    }
}

/// Delivery errors occur during message posting.
#[derive(
    thiserror::Error,
    Debug,
    Serialize,
    Deserialize,
    typeuri::Named,
    Clone,
    PartialEq,
    Eq
)]
pub enum DeliveryError {
    /// The destination address is not reachable.
    #[error("address not routable: {0}")]
    Unroutable(String),

    /// A broken link indicates that a link in the message
    /// delivery path has failed.
    #[error("broken link: {0}")]
    BrokenLink(String),

    /// A (local) mailbox delivery error.
    #[error("mailbox error: {0}")]
    Mailbox(String),

    /// The message went through too many hops and has expired.
    #[error("ttl expired")]
    TtlExpired,
}

/// An envelope that carries a message destined to a remote actor.
/// The envelope contains a serialized message along with its destination
/// and sender.
#[derive(Debug, Serialize, Deserialize, Clone, typeuri::Named)]
pub struct MessageEnvelope {
    /// The sender of this message.
    sender: ActorAddr,

    /// The destination of the message.
    dest: PortAddr,

    /// The serialized message.
    data: wirevalue::Any,

    /// Error contains a delivery error when message delivery failed.
    errors: Vec<DeliveryError>,

    /// Structured delivery failures. The first entry is the root delivery
    /// failure; later entries record subsequent failures while returning or
    /// forwarding the same envelope.
    delivery_failures: Vec<DeliveryFailure>,

    /// Additional context for this message.
    headers: Flattrs,

    /// Decremented at every `MailboxSender` hop.
    ttl: u8,

    /// If true, undeliverable messages should be returned to sender. Else, they
    /// are dropped.
    return_undeliverable: bool,
    // TODO: add typename, source, seq, etc.
}
wirevalue::register_type!(MessageEnvelope);

impl MessageEnvelope {
    /// Create a new envelope with the provided sender, destination, and message.
    pub fn new(
        sender: impl Into<ActorAddr>,
        dest: impl Into<PortAddr>,
        data: wirevalue::Any,
        headers: Flattrs,
    ) -> Self {
        let sender = sender.into();
        let dest = dest.into();
        Self {
            sender,
            dest,
            data,
            errors: Vec::new(),
            delivery_failures: Vec::new(),
            headers,
            ttl: hyperactor_config::global::get(crate::config::MESSAGE_TTL_DEFAULT),
            // By default, all undeliverable messages should be returned to the sender.
            return_undeliverable: true,
        }
    }

    /// Create a new envelope whose sender ID is unknown.
    pub(crate) fn new_unknown(dest: impl Into<PortAddr>, data: wirevalue::Any) -> Self {
        // Create a synthetic "unknown" actor ID for messages with no known sender
        let unknown_addr = ChannelAddr::any(ChannelTransport::Local);
        let unknown_proc_ref = ProcAddr::instance(unknown_addr, "unknown");
        let unknown_actor_ref =
            ActorAddr::root(unknown_proc_ref, crate::id::Label::strip("unknown"));
        Self::new(unknown_actor_ref, dest, data, Flattrs::new())
    }

    /// Construct a new serialized value by serializing the provided T-typed value.
    pub fn serialize<T: Serialize + Named>(
        source: impl Into<ActorAddr>,
        dest: impl Into<PortAddr>,
        value: &T,
        headers: Flattrs,
    ) -> Result<Self, wirevalue::Error> {
        Ok(Self {
            headers,
            data: wirevalue::Any::serialize(value)?,
            sender: source.into(),
            dest: dest.into(),
            errors: Vec::new(),
            delivery_failures: Vec::new(),
            ttl: hyperactor_config::global::get(crate::config::MESSAGE_TTL_DEFAULT),
            // By default, all undeliverable messages should be returned to the sender.
            return_undeliverable: true,
        })
    }

    /// Returns the remaining time-to-live (TTL) for this message.
    ///
    /// The TTL is decremented at each `MailboxSender` hop. When it
    /// reaches 0, the message is considered expired and is returned
    /// to the sender as undeliverable.
    pub fn ttl(&self) -> u8 {
        self.ttl
    }

    /// Overrides the message’s time-to-live (TTL).
    ///
    /// This replaces the current TTL value (normally initialized from
    /// `config::MESSAGE_TTL_DEFAULT`) with the provided `ttl`. The
    /// updated envelope is returned for chaining.
    ///
    /// # Note
    /// The TTL is decremented at each `MailboxSender` hop, and when
    /// it reaches 0 the message will be treated as undeliverable.
    pub fn set_ttl(mut self, ttl: u8) -> Self {
        self.ttl = ttl;
        self
    }

    /// Decrements the message's TTL by one hop.
    ///
    /// Returns `Ok(())` if the TTL was greater than zero and
    /// successfully decremented. If the TTL was already zero, no
    /// decrement occurs and `Err(DeliveryError::TtlExpired)` is
    /// returned, indicating that the message has expired and should
    /// be treated as undeliverable.
    fn dec_ttl_or_err(&mut self) -> Result<(), DeliveryError> {
        if self.ttl == 0 {
            Err(DeliveryError::TtlExpired)
        } else {
            self.ttl -= 1;
            Ok(())
        }
    }

    /// Deserialize the message in the envelope to the provided type T.
    pub fn deserialized<T: DeserializeOwned + Named>(&self) -> Result<T, anyhow::Error> {
        Ok(self.data.deserialized()?)
    }

    /// The serialized message.
    pub fn data(&self) -> &wirevalue::Any {
        &self.data
    }

    /// The message sender.
    pub fn sender(&self) -> &ActorAddr {
        &self.sender
    }

    /// The destination of the message.
    pub fn dest(&self) -> &PortAddr {
        &self.dest
    }

    /// The message headers.
    pub fn headers(&self) -> &Flattrs {
        &self.headers
    }

    /// Tells whether this is a signal message.
    pub fn is_signal(&self) -> bool {
        self.dest.index() == Signal::port()
    }

    /// Set a delivery error for the message. If errors are already set, append
    /// it to the existing errors.
    pub fn set_error(&mut self, error: DeliveryError) {
        self.errors.push(error)
    }

    /// Push a structured delivery failure onto this message's failure history.
    pub fn push_delivery_failure(&mut self, failure: DeliveryFailure) {
        self.delivery_failures.push(failure)
    }

    /// Push a structured delivery failure only when this envelope does not
    /// already have a root failure.
    pub fn ensure_root_delivery_failure(&mut self, failure: impl FnOnce() -> DeliveryFailure) {
        if self.root_delivery_failure().is_none() {
            self.push_delivery_failure(failure());
        }
    }

    /// Change the sender on the envelope in case it was set incorrectly. This
    /// should only be used by CommActor since it is forwarding from another
    /// sender.
    pub fn update_sender(&mut self, sender: impl Into<ActorAddr>) {
        self.sender = sender.into();
    }

    /// Set to true if you want this message to be returned to sender if it cannot
    /// reach dest. This is the default.
    /// Set to false if you want the message to be dropped instead.
    pub fn set_return_undeliverable(&mut self, return_undeliverable: bool) {
        self.return_undeliverable = return_undeliverable;
    }

    /// The message has been determined to be undeliverable with the
    /// provided error. Mark the envelope with the error and return to
    /// sender.
    pub fn undeliverable(
        mut self,
        error: DeliveryError,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        tracing::debug!(
            name = "undelivered_message_attempt",
            sender = self.sender.to_string(),
            dest = self.dest.to_string(),
            error = error.to_string(),
            return_handle = %return_handle,
        );
        metrics::MAILBOX_UNDELIVERABLE_MESSAGES.add(
            1,
            hyperactor_telemetry::kv_pairs!(
                "sender_actor_id" => self.sender.to_string(),
                "dest_actor_id" => self.dest.to_string(),
                "message_type" => self.data.typename().unwrap_or("unknown"),
                "error_type" =>  error.to_string(),
            ),
        );

        self.set_error(error);
        undeliverable::return_undeliverable(return_handle, self);
    }

    /// Mark the message as undeliverable with both legacy and structured
    /// failure information.
    pub fn undeliverable_with_failure(
        mut self,
        error: DeliveryError,
        failure: DeliveryFailure,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        self.push_delivery_failure(failure);
        self.undeliverable(error, return_handle);
    }

    /// Get the errors of why this message was undeliverable. Empty means this
    /// message was not determined as undeliverable.
    pub fn errors(&self) -> &Vec<DeliveryError> {
        &self.errors
    }

    /// Get the structured delivery failures for this message. Empty means this
    /// message was not determined as undeliverable through the structured path.
    pub fn delivery_failures(&self) -> &[DeliveryFailure] {
        &self.delivery_failures
    }

    /// Get the root structured delivery failure for this message.
    pub fn root_delivery_failure(&self) -> Option<&DeliveryFailure> {
        self.delivery_failures.first()
    }

    /// Get the root structured delivery failure mutably.
    pub fn root_delivery_failure_mut(&mut self) -> Option<&mut DeliveryFailure> {
        self.delivery_failures.first_mut()
    }

    /// Get the string representation of the errors of this message was
    /// undeliverable. None means this message was not determined as
    /// undeliverable.
    pub fn error_msg(&self) -> Option<String> {
        if self.errors.is_empty() {
            None
        } else {
            Some(
                self.errors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        }
    }

    fn open(self) -> (MessageMetadata, wirevalue::Any) {
        let Self {
            sender,
            dest,
            data,
            errors,
            delivery_failures,
            headers,
            ttl,
            return_undeliverable,
        } = self;

        (
            MessageMetadata {
                sender,
                dest,
                errors,
                delivery_failures,
                headers,
                ttl,
                return_undeliverable,
            },
            data,
        )
    }

    fn seal(metadata: MessageMetadata, data: wirevalue::Any) -> Self {
        let MessageMetadata {
            sender,
            dest,
            errors,
            delivery_failures,
            headers,
            ttl,
            return_undeliverable,
        } = metadata;

        Self {
            sender,
            dest,
            data,
            errors,
            delivery_failures,
            headers,
            ttl,
            return_undeliverable,
        }
    }

    fn return_undeliverable(&self) -> bool {
        self.return_undeliverable
    }

    /// Set a header value on this envelope.
    pub fn set_header<T: Serialize>(&mut self, key: hyperactor_config::attrs::Key<T>, value: T) {
        self.headers.set(key, value);
    }
}

impl fmt::Display for MessageEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.error_msg() {
            None => write!(
                f,
                "{} > {}: {} {{{}}}",
                self.sender, self.dest, self.data, self.headers
            ),
            Some(err) => write!(
                f,
                "{} > {}: {} {{{}}}: delivery error: {}",
                self.sender, self.dest, self.data, self.headers, err
            ),
        }
    }
}

/// Metadata about a message sent via a MessageEnvelope.
#[derive(Clone)]
pub struct MessageMetadata {
    sender: ActorAddr,
    dest: PortAddr,
    errors: Vec<DeliveryError>,
    /// Structured delivery failures. The first entry is the root delivery
    /// failure; later entries record subsequent failures while returning or
    /// forwarding the same envelope.
    delivery_failures: Vec<DeliveryFailure>,
    headers: Flattrs,
    ttl: u8,
    return_undeliverable: bool,
}

/// Errors that occur during mailbox operations. Each error is associated
/// with the mailbox's actor id.
#[derive(Debug)]
pub struct MailboxError {
    actor_id: ActorAddr,
    kind: MailboxErrorKind,
}

/// The kinds of mailbox errors. This enum is marked non-exhaustive to
/// allow for extensibility.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum MailboxErrorKind {
    /// An operation was attempted on a closed mailbox.
    #[error("mailbox closed")]
    Closed,

    /// The port associated with an operation was invalid.
    #[error("invalid port: {0}")]
    InvalidPort(PortAddr),

    /// There was no sender associated with the port.
    #[error("no sender for port: {0}")]
    NoSenderForPort(PortAddr),

    /// There was no local sender associated with the port.
    /// Returned by operations that require a local port.
    #[error("no local sender for port: {0}")]
    NoLocalSenderForPort(PortAddr),

    /// The port was closed.
    #[error("{0}: port closed")]
    PortClosed(PortAddr),

    /// An error occured during a send operation.
    #[error("send {0}: {1}")]
    Send(PortAddr, #[source] anyhow::Error),

    /// An error occured during a receive operation.
    #[error("recv {0}: {1}")]
    Recv(PortAddr, #[source] anyhow::Error),

    /// There was a serialization failure.
    #[error("serialize: {0}")]
    Serialize(#[source] anyhow::Error),

    /// There was a deserialization failure.
    #[error("deserialize {0}: {1}")]
    Deserialize(&'static str, anyhow::Error),

    /// There was an error during a channel operation.
    #[error(transparent)]
    Channel(#[from] ChannelError),

    /// The owning actor terminated (either stopped or failed).
    #[error("owner terminated: {0}")]
    OwnerTerminated(ActorStatus),
}

impl MailboxError {
    /// Create a new mailbox error associated with the provided actor
    /// id and of the given kind.
    pub fn new(actor_id: impl Into<ActorAddr>, kind: MailboxErrorKind) -> Self {
        Self {
            actor_id: actor_id.into(),
            kind,
        }
    }

    /// The address of the mailbox producing this error.
    pub fn actor_addr(&self) -> &ActorAddr {
        &self.actor_id
    }

    /// The error's kind.
    pub fn kind(&self) -> &MailboxErrorKind {
        &self.kind
    }
}

impl fmt::Display for MailboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.actor_id)?;
        fmt::Display::fmt(&self.kind, f)
    }
}

impl std::error::Error for MailboxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

/// PortLocation describes the location of a port.
/// This is used in errors to provide a uniform data type
/// for ports that may or may not be bound.
#[derive(Debug, Clone)]
pub enum PortLocation {
    /// The port was bound: the location is its underlying bound ID.
    Bound(PortAddr),
    /// The port was not bound: we provide the actor ID and the message type.
    Unbound(ActorAddr, &'static str),
}

impl PortLocation {
    fn new_unbound<M: Message>(actor_id: ActorAddr) -> Self {
        PortLocation::Unbound(actor_id, std::any::type_name::<M>())
    }

    #[allow(dead_code)]
    fn new_unbound_type(actor_id: ActorAddr, ty: &'static str) -> Self {
        PortLocation::Unbound(actor_id, ty)
    }

    /// The actor address of the location.
    pub fn actor_addr(&self) -> ActorAddr {
        match self {
            PortLocation::Bound(port_addr) => port_addr.actor_addr(),
            PortLocation::Unbound(actor_addr, _) => actor_addr.clone(),
        }
    }
}

impl fmt::Display for PortLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortLocation::Bound(port_ref) => write!(f, "{}", port_ref),
            PortLocation::Unbound(actor_ref, name) => write!(f, "{}<{}>", actor_ref, name),
        }
    }
}

/// Errors that that occur during mailbox sending operations. Each error
/// is associated with the port ID of the operation.
#[derive(Debug)]
pub struct MailboxSenderError {
    location: Box<PortLocation>,
    kind: Box<MailboxSenderErrorKind>,
}

/// The kind of mailbox sending errors.
#[derive(thiserror::Error, Debug)]
pub enum MailboxSenderErrorKind {
    /// Error during serialization.
    #[error("serialization error: {0}")]
    Serialize(anyhow::Error),

    /// Error during deserialization.
    #[error("deserialization error for type {0}: {1}")]
    Deserialize(&'static str, anyhow::Error),

    /// A send to an invalid port.
    #[error("invalid port")]
    Invalid,

    /// A send to a closed port.
    #[error("port closed")]
    Closed,

    // The following pass through underlying errors:
    /// An underlying mailbox error.
    #[error(transparent)]
    Mailbox(#[from] MailboxError),

    /// An underlying channel error.
    #[error(transparent)]
    Channel(#[from] ChannelError),

    /// An other, uncategorized error.
    #[error("send error: {0}")]
    Other(#[from] anyhow::Error),

    /// The destination was unreachable.
    #[error("unreachable: {0}")]
    Unreachable(anyhow::Error),
}

impl MailboxSenderError {
    /// Create a new mailbox sender error to an unbound port.
    pub fn new_unbound<M>(actor_id: impl Into<ActorAddr>, kind: MailboxSenderErrorKind) -> Self {
        Self {
            location: Box::new(PortLocation::Unbound(
                actor_id.into(),
                std::any::type_name::<M>(),
            )),
            kind: Box::new(kind),
        }
    }

    /// Create a new mailbox sender, manually providing the type.
    pub fn new_unbound_type(
        actor_id: impl Into<ActorAddr>,
        kind: MailboxSenderErrorKind,
        ty: &'static str,
    ) -> Self {
        Self {
            location: Box::new(PortLocation::Unbound(actor_id.into(), ty)),
            kind: Box::new(kind),
        }
    }

    /// Create a new mailbox sender error with the provided port ID and kind.
    pub fn new_bound(port_id: impl Into<PortAddr>, kind: MailboxSenderErrorKind) -> Self {
        Self {
            location: Box::new(PortLocation::Bound(port_id.into())),
            kind: Box::new(kind),
        }
    }

    /// The location at which the error occured.
    pub fn location(&self) -> &PortLocation {
        &self.location
    }

    /// The kind associated with the error.
    pub fn kind(&self) -> &MailboxSenderErrorKind {
        &self.kind
    }
}

impl fmt::Display for MailboxSenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.location)?;
        fmt::Display::fmt(&self.kind, f)
    }
}

impl std::error::Error for MailboxSenderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

/// MailboxSenders can send messages through ports to mailboxes. It
/// provides a unified interface for message delivery in the system.
#[async_trait]
pub trait MailboxSender: Send + Sync + Any {
    /// Apply hop semantics (TTL decrement; undeliverable on 0), then
    /// delegate to transport.
    fn post(
        &self,
        mut envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if let Err(err) = envelope.dec_ttl_or_err() {
            let failure = DeliveryFailure::new(ExpiredDelivery::new(envelope.dest().clone()));
            envelope.undeliverable_with_failure(err, failure, return_handle);
            return;
        }
        self.post_unchecked(envelope, return_handle);
    }

    /// Raw transport: **no** policy.
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    );

    /// Wait until all messages previously posted through this sender
    /// have been delivered (wire-acked) or confirmed undeliverable.
    /// The default implementation is a no-op, appropriate for senders
    /// whose `post` is synchronous (e.g. local in-process delivery).
    async fn flush(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// PortSender extends [`MailboxSender`] by providing typed endpoints
/// for sending messages over ports
pub trait PortSender: MailboxSender {
    /// Deliver a message to the provided port.
    fn serialize_and_send<M: RemoteMessage>(
        &self,
        port: &PortRef<M>,
        message: M,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) -> Result<(), MailboxSenderError> {
        // TODO: convert this to a undeliverable error also
        let serialized = wirevalue::Any::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                port.port_addr().clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        })?;
        self.post(
            MessageEnvelope::new_unknown(port.port_addr().clone(), serialized),
            return_handle,
        );
        Ok(())
    }

    /// Deliver a message to a one-shot port, consuming the provided port,
    /// which is not reusable.
    fn serialize_and_send_once<M: RemoteMessage>(
        &self,
        once_port: OncePortRef<M>,
        message: M,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) -> Result<(), MailboxSenderError> {
        let serialized = wirevalue::Any::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                once_port.port_addr().clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        })?;
        self.post(
            MessageEnvelope::new_unknown(once_port.port_addr().clone(), serialized),
            return_handle,
        );
        Ok(())
    }
}

impl<T: ?Sized + MailboxSender> PortSender for T {}

/// A perpetually closed mailbox sender. Panics if any messages are posted.
/// Useful for tests, or where there is no meaningful mailbox sender
/// implementation available.
#[derive(Debug, Clone)]
pub struct PanickingMailboxSender;

#[async_trait]
impl MailboxSender for PanickingMailboxSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        panic!("panic! in the mailbox! attempted post: {}", envelope)
    }
}

/// A mailbox sender for undeliverable messages. This will simply record
/// any undelivered messages.
#[derive(Debug)]
pub struct UndeliverableMailboxSender;

#[async_trait]
impl MailboxSender for UndeliverableMailboxSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let sender_name = envelope
            .sender
            .label()
            .map_or("?".to_string(), |l| l.to_string());
        let error_str = envelope.error_msg().unwrap_or("".to_string());
        let operation_endpoint = envelope.headers().get(headers::OPERATION_ENDPOINT);
        let operation_adverb = envelope.headers().get(headers::OPERATION_ADVERB);
        // See UM-1..UM-3b in module docs.
        match &operation_endpoint {
            Some(endpoint) => tracing::error!(
                name = "undelivered_message_abandoned",
                actor_name = sender_name,
                actor_id = envelope.sender.to_string(),
                dest = envelope.dest.to_string(),
                message_type = envelope.data().typename().unwrap_or("unknown"),
                data_len = envelope.data().len(),
                endpoint = %endpoint,
                adverb = operation_adverb.as_deref().unwrap_or(""),
                error = %error_str,
                "abandoned message for {}",
                endpoint,
            ),
            None => tracing::error!(
                name = "undelivered_message_abandoned",
                actor_name = sender_name,
                actor_id = envelope.sender.to_string(),
                dest = envelope.dest.to_string(),
                message_type = envelope.data().typename().unwrap_or("unknown"),
                data_len = envelope.data().len(),
                error = %error_str,
                "message not delivered to {}",
                envelope.dest,
            ),
        }
    }
}

/// Convenience boxing implementation for MailboxSender. Most APIs
/// are parameterized on MailboxSender implementations, and it's thus
/// difficult to work with dyn values.  BoxedMailboxSender bridges this
/// gap by providing a concrete MailboxSender which dispatches using an
/// underlying (boxed) dyn.
#[derive(Clone)]
pub struct BoxedMailboxSender(Arc<dyn MailboxSender + Send + Sync + 'static>);

impl fmt::Debug for BoxedMailboxSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedMailboxSender")
            .field("sender", &"<dyn MailboxSender>")
            .finish()
    }
}

impl BoxedMailboxSender {
    /// Create a new boxed sender given the provided sender implementation.
    pub fn new(sender: impl MailboxSender + 'static) -> Self {
        Self(Arc::new(sender))
    }

    /// Attempts to downcast the inner sender to the given concrete
    /// type.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        (&*self.0 as &dyn Any).downcast_ref::<T>()
    }
}

/// Extension trait that creates a boxed clone of a MailboxSender.
pub trait BoxableMailboxSender: MailboxSender + Clone + 'static {
    /// A boxed clone of this MailboxSender.
    fn boxed(&self) -> BoxedMailboxSender;
}
impl<T: MailboxSender + Clone + 'static> BoxableMailboxSender for T {
    fn boxed(&self) -> BoxedMailboxSender {
        BoxedMailboxSender::new(self.clone())
    }
}

/// Extension trait that rehomes a MailboxSender into a BoxedMailboxSender.
pub trait IntoBoxedMailboxSender: MailboxSender {
    /// Rehome this MailboxSender into a BoxedMailboxSender.
    fn into_boxed(self) -> BoxedMailboxSender;
}
impl<T: MailboxSender + 'static> IntoBoxedMailboxSender for T {
    fn into_boxed(self) -> BoxedMailboxSender {
        BoxedMailboxSender::new(self)
    }
}

#[async_trait]
impl MailboxSender for BoxedMailboxSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        self.0.post_unchecked(envelope, return_handle);
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        self.0.flush().await
    }
}

/// Errors that occur during mailbox serving.
#[derive(thiserror::Error, Debug)]
pub enum MailboxServerError {
    /// An underlying channel error.
    #[error(transparent)]
    Channel(#[from] ChannelError),

    /// An underlying mailbox sender error.
    #[error(transparent)]
    MailboxSender(#[from] MailboxSenderError),
}

/// Represents a running [`MailboxServer`]. The handle composes a
/// ['tokio::task::JoinHandle'] and may be joined in the same manner.
#[derive(Debug)]
pub struct MailboxServerHandle {
    join_handle: JoinHandle<Result<(), MailboxServerError>>,
    stopped_tx: watch::Sender<bool>,
}

impl MailboxServerHandle {
    /// Signal the server to stop serving the mailbox. The caller should
    /// join the handle by awaiting the [`MailboxServerHandle`] future.
    ///
    /// Stop should be called at most once.
    pub fn stop(&self, reason: &str) {
        tracing::info!("stopping mailbox server; reason: {}", reason);
        self.stopped_tx.send(true).expect("stop called twice");
    }

    /// Construct a handle from an already-spawned server task and a
    /// stop signal. The task must observe `stopped_rx` (the receiver
    /// paired with `stopped_tx`) and complete once stop is requested,
    /// so callers can join the handle to confirm shutdown.
    pub fn from_parts(
        join_handle: JoinHandle<Result<(), MailboxServerError>>,
        stopped_tx: watch::Sender<bool>,
    ) -> Self {
        Self {
            join_handle,
            stopped_tx,
        }
    }
}

/// Forward future implementation to underlying handle.
impl Future for MailboxServerHandle {
    type Output = <JoinHandle<Result<(), MailboxServerError>> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: This is safe to do because self is pinned.
        let join_handle_pinned =
            unsafe { self.map_unchecked_mut(|container| &mut container.join_handle) };
        join_handle_pinned.poll(cx)
    }
}

/// Serve a port on the provided [`channel::Rx`]. This dispatches all
/// channel messages directly to the port.
pub trait MailboxServer: MailboxSender + Clone + Sized + 'static {
    /// Serve the provided port on the given channel on this sender on
    /// a background task which may be joined with the returned handle.
    /// The task fails on any send error.
    fn serve(
        self,
        mut rx: impl channel::Rx<MessageEnvelope> + Send + 'static,
    ) -> MailboxServerHandle {
        // A `MailboxServer` can receive a message that couldn't
        // reach its destination. We can use the fact that servers are
        // `MailboxSender`s to attempt to forward them back to their
        // senders.
        let (return_handle, mut undeliverable_rx) = undeliverable::new_undeliverable_port();
        let server = self.clone();
        tokio::task::spawn(async move {
            // Create a client for this task.
            static NEXT_RANK: AtomicUsize = AtomicUsize::new(0);
            let rank = NEXT_RANK.fetch_add(1, Ordering::Relaxed);
            let addr = ChannelAddr::any(ChannelTransport::Local);
            let proc_id = ProcAddr::instance(addr, format!("mailbox_server_{}", rank));
            // Use this mailbox server as the forwarder, so we can use it to
            // return message back to the sender.
            //
            // TODO(meriksen): use a global proc here when it materializes
            let proc = Proc::builder()
                .proc_id(proc_id.id().clone())
                .shared_gateway(Gateway::configured(
                    proc_id.location().clone(),
                    BoxedMailboxSender::new(server),
                ))
                .build()
                .expect("mailbox server proc builder is valid");
            let (client, _) = proc.client("undeliverable_supervisor").unwrap();
            while let Ok(undeliverable) = undeliverable_rx.recv().await {
                match undeliverable {
                    Undeliverable::Message(mut envelope) => {
                        match envelope.deserialized::<Undeliverable<MessageEnvelope>>() {
                            Ok(Undeliverable::Message(e)) => {
                                // A non-returnable undeliverable.
                                UndeliverableMailboxSender.post(e, monitored_return_handle());
                                continue;
                            }
                            Ok(Undeliverable::Lost(lost)) => {
                                tracing::error!(
                                    sender = %lost.sender,
                                    dest = %lost.dest,
                                    message_type = lost.message_type.as_deref().unwrap_or("unknown"),
                                    error = %lost.error,
                                    "lost message was undeliverable"
                                );
                                continue;
                            }
                            Err(_) => {}
                        }
                        envelope.set_error(DeliveryError::BrokenLink(
                            "message was undeliverable".to_owned(),
                        ));
                        let sender_id: ActorAddr = envelope.sender().clone();
                        let return_port =
                            PortRef::<Undeliverable<MessageEnvelope>>::attest_handler_port(
                                &sender_id,
                            );
                        return_port.post_serialized(
                            &client,
                            Flattrs::new(),
                            wirevalue::Any::serialize(&Undeliverable::Message(envelope)).unwrap(),
                        );
                    }
                    Undeliverable::Lost(lost) => {
                        tracing::error!(
                            sender = %lost.sender,
                            dest = %lost.dest,
                            message_type = lost.message_type.as_deref().unwrap_or("unknown"),
                            error = %lost.error,
                            "lost message was undeliverable"
                        );
                    }
                }
            }
        });

        let (stopped_tx, mut stopped_rx) = watch::channel(false);
        let join_handle = tokio::spawn(async move {
            let mut detached = false;

            let result = loop {
                if *stopped_rx.borrow_and_update() {
                    break Ok(());
                }

                tokio::select! {
                    message = rx.recv() => {
                        match message {
                            // Relay the message to the port directly.
                            Ok(envelope) => self.post(envelope, return_handle.clone()),

                            // Closed is a "graceful" error in this case.
                            // We simply stop serving.
                            Err(ChannelError::Closed) => break Ok(()),
                            Err(channel_err) => break Err(MailboxServerError::from(channel_err)),
                        }
                    }
                    result = stopped_rx.changed(), if !detached  => {
                        detached = result.is_err();
                        if detached {
                            tracing::debug!(
                                "the mailbox server is detached for Rx {}", rx.addr()
                            );
                        } else {
                            tracing::debug!(
                                "the mailbox server is stopped for Rx {}", rx.addr()
                            );
                        }
                    }
                }
            };

            // Join the channel receiver to ensure pending acks are
            // sent before the underlying channel server is torn down.
            rx.join().await;

            result
        });

        MailboxServerHandle {
            join_handle,
            stopped_tx,
        }
    }
}

impl<T: MailboxSender + Clone + Sized + Sync + Send + 'static> MailboxServer for T {}

struct Buffer<T: Message> {
    queue: mpsc::UnboundedSender<(T, PortHandle<Undeliverable<T>>)>,
    #[allow(dead_code)]
    processed: watch::Receiver<usize>,
    seq: AtomicUsize,
}

impl<T: Message> Buffer<T> {
    fn new<Fut>(
        process: impl Fn(T, PortHandle<Undeliverable<T>>) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (queue, mut next) = mpsc::unbounded_channel();
        let (last_processed, processed) = watch::channel(0);
        crate::init::get_runtime().spawn(async move {
            let mut seq = 0;
            while let Some((msg, return_handle)) = next.recv().await {
                process(msg, return_handle).await;
                seq += 1;
                let _ = last_processed.send(seq);
            }
        });
        Self {
            queue,
            processed,
            seq: AtomicUsize::new(0),
        }
    }

    fn send(
        &self,
        item: (T, PortHandle<Undeliverable<T>>),
    ) -> Result<(), Box<mpsc::error::SendError<(T, PortHandle<Undeliverable<T>>)>>> {
        self.seq.fetch_add(1, Ordering::SeqCst);
        self.queue.send(item).map_err(Box::new)?;
        Ok(())
    }
}

/// A mailbox server client that transmits messages on a Tx channel.
pub struct MailboxClient {
    // The channel address.
    addr: ChannelAddr,

    // The unbounded sender.
    buffer: Buffer<MessageEnvelope>,

    // To cancel monitoring tx health.
    _tx_monitoring: CancellationToken,

    // Flush tracking: counts messages successfully submitted to the buffer.
    submitted: Arc<AtomicUsize>,
    // Flush tracking: counts messages whose delivery oneshot has resolved
    // (acked or failed).
    completed: Arc<AtomicUsize>,
    // Notifies flush waiters when `completed` changes.
    completed_notify: Arc<tokio::sync::Notify>,
}

impl fmt::Debug for MailboxClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MailboxClient")
            .field("buffer", &"<Buffer>")
            .finish()
    }
}

impl MailboxClient {
    /// Create a new client that sends messages destined for a
    /// [`MailboxServer`] on the provided Tx channel.
    pub fn new(tx: impl channel::Tx<MessageEnvelope> + Send + Sync + 'static) -> Self {
        let addr = tx.addr();
        let tx = Arc::new(tx);
        let tx_status = tx.status().clone();
        let tx_monitoring = CancellationToken::new();
        let completed = Arc::new(AtomicUsize::new(0));
        let completed_notify = Arc::new(tokio::sync::Notify::new());
        let buffer = {
            let completed = completed.clone();
            let completed_notify = completed_notify.clone();
            let addr = addr.clone();
            Buffer::new(move |envelope, return_handle| {
                let tx = Arc::clone(&tx);
                let addr = addr.clone();
                let (return_channel, return_receiver) =
                    oneshot::channel::<SendError<MessageEnvelope>>();
                // Set up for delivery failure.
                let return_handle_0 = return_handle.clone();
                let completed = completed.clone();
                let completed_notify = completed_notify.clone();
                tokio::spawn(async move {
                    match return_receiver.await {
                        Ok(SendError {
                            error,
                            message,
                            reason,
                        }) => {
                            let target = message.dest().clone();
                            let failure = DeliveryFailure::new(UndeliverableReason::Transport(
                                TransportFailure::new(
                                    target,
                                    TransportFailureReason::ChannelClosed { addr },
                                ),
                            ));
                            message.undeliverable_with_failure(
                                DeliveryError::BrokenLink(format!(
                                    "failed to enqueue in MailboxClient when processing buffer: {error} with reason {reason:?}"
                                )),
                                failure,
                                return_handle_0,
                            );
                        }
                        Err(_) => {
                            // Oneshot sender was dropped — message was acked.
                        }
                    }
                    completed.fetch_add(1, Ordering::SeqCst);
                    completed_notify.notify_waiters();
                });
                // Send the message for transmission.
                tx.try_post(envelope, return_channel);
                future::ready(())
            })
        };
        let this = Self {
            addr: addr.clone(),
            buffer,
            _tx_monitoring: tx_monitoring.clone(),
            submitted: Arc::new(AtomicUsize::new(0)),
            completed,
            completed_notify,
        };
        Self::monitor_tx_health(tx_status, tx_monitoring, addr);
        this
    }

    /// Convenience constructor, to set up a mailbox client that forwards messages
    /// to the provided address.
    pub fn dial(addr: ChannelAddr) -> Result<MailboxClient, ChannelError> {
        Ok(MailboxClient::new(channel::dial(addr)?))
    }

    // Set up a watch for the tx's health.
    fn monitor_tx_health(
        mut rx: watch::Receiver<TxStatus>,
        cancel_token: CancellationToken,
        addr: ChannelAddr,
    ) {
        crate::init::get_runtime().spawn(async move {
            loop {
                tokio::select! {
                    changed = rx.changed() => {
                        if changed.is_err() || rx.borrow().is_closed() {
                            let reason = rx.borrow().as_closed().map(|r| r.to_string()).unwrap_or_else(|| "unknown".to_string());
                            tracing::warn!("connection to {} lost: {}", addr, reason);
                            // TODO: Potential for supervision event
                            // interaction here.
                            break;
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                }
            }
        });
    }
}

#[async_trait]
impl MailboxSender for MailboxClient {
    #[tracing::instrument(level = "debug", skip_all)]
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        tracing::event!(target:"messages", tracing::Level::TRACE,  "size"=envelope.data.len(), "sender"= %envelope.sender, "dest" = %envelope.dest.actor_addr(), "port"= envelope.dest.index(), "message_type" = envelope.data.typename().unwrap_or("unknown"), "send_message");
        if let Err(err) = self.buffer.send((envelope, return_handle)) {
            let mpsc::error::SendError((envelope, return_handle)) = *err;
            let err = DeliveryError::BrokenLink(
                "failed to enqueue in MailboxClient; buffer's queue is closed".to_string(),
            );
            let target = envelope.dest().clone();
            let failure =
                DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                    target,
                    TransportFailureReason::LinkUnavailable(format!(
                        "mailbox client buffer is closed for {}",
                        self.addr
                    )),
                )));

            // Failed to enqueue.
            envelope.undeliverable_with_failure(err, failure, return_handle);
        } else {
            self.submitted.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        let target = self.submitted.load(Ordering::SeqCst);
        loop {
            if self.completed.load(Ordering::SeqCst) >= target {
                return Ok(());
            }
            self.completed_notify.notified().await;
        }
    }
}

/// Wrapper to turn `PortAddr` into a `Sink`.
pub struct PortSink<C: context::Actor, M: RemoteMessage> {
    cx: C,
    port: PortRef<M>,
}

impl<C: context::Actor, M: RemoteMessage> PortSink<C, M> {
    /// Create new PortSink
    pub fn new(cx: C, port: PortRef<M>) -> Self {
        Self { cx, port }
    }
}

impl<C: context::Actor, M: RemoteMessage> Sink<M> for PortSink<C, M> {
    type Error = MailboxSenderError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
        crate::Endpoint::post(&self.port, &self.cx, item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// A mailbox coordinates message delivery to actors through typed
/// [`Port`]s associated with the mailbox.
#[derive(Clone, Debug)]
pub struct Mailbox {
    inner: Arc<State>,
}

impl Mailbox {
    /// Create a mailbox associated with the provided actor ID.
    pub fn new(actor_id: impl Into<ActorAddr>) -> Self {
        Self {
            inner: Arc::new(State::new(actor_id.into())),
        }
    }

    /// The actor address associated with this mailbox.
    pub fn actor_addr(&self) -> &ActorAddr {
        &self.inner.actor_id
    }

    /// Open a new port that accepts M-typed messages. The returned
    /// port may be freely cloned, serialized, and passed around. The
    /// returned receiver should only be retained by the actor responsible
    /// for processing the delivered messages.
    pub fn open_port<M: Message>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        let port_index = self.inner.allocate_port();
        let (sender, receiver) = mpsc::unbounded_channel::<M>();
        let port_id = self.inner.actor_id.port_addr(Port::from(port_index));
        tracing::trace!(
            name = "open_port",
            "opening port for {} at {}",
            self.inner.actor_id,
            port_id
        );
        (
            PortHandle::new(self.clone(), port_index, UnboundedPortSender::Mpsc(sender)),
            PortReceiver::new(receiver, port_id, /*coalesce=*/ false, self.clone()),
        )
    }

    /// Bind the handler port for message type `M` to this mailbox.
    /// This method is normally used:
    ///   1. when we need to intercept a message sent to a handler, and re-route
    ///      that message to the returned receiver;
    ///   2. mock this message's handler when it is not implemented for this actor
    ///      type, with the returned receiver.
    pub(crate) fn bind_handler_port<M: RemoteMessage>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        let (handle, receiver) = self.open_port();
        handle.bind_handler_port();
        (handle, receiver)
    }

    /// Open a new port with an accumulator with default reduce options.
    /// See [`open_accum_port_opts`] for more details.
    pub fn open_accum_port<A>(&self, accum: A) -> (PortHandle<A::Update>, PortReceiver<A::State>)
    where
        A: Accumulator + Send + Sync + 'static,
        A::Update: Message,
        A::State: Message + Default + Clone,
    {
        self.open_accum_port_opts(accum, StreamingReducerOpts::default())
    }

    /// Open a new port with an accumulator. This port accepts A::Update type
    /// messages, accumulate them into A::State with the given accumulator.
    /// The latest changed state can be received from the returned receiver as
    /// a single A::State message. If there is no new update, the receiver will
    /// not receive any message.
    ///
    /// If provided, reducer mode controls reduce operations.
    pub fn open_accum_port_opts<A>(
        &self,
        accum: A,
        streaming_opts: StreamingReducerOpts,
    ) -> (PortHandle<A::Update>, PortReceiver<A::State>)
    where
        A: Accumulator + Send + Sync + 'static,
        A::Update: Message,
        A::State: Message + Default + Clone,
    {
        let port_index = self.inner.allocate_port();
        let (sender, receiver) = mpsc::unbounded_channel::<A::State>();
        let port_id = self.inner.actor_id.port_addr(Port::from(port_index));
        let state = Mutex::new(A::State::default());
        let reducer_spec = accum.reducer_spec();
        let enqueue = move |_, update: A::Update| {
            let mut state = state.lock().unwrap();
            accum.accumulate(&mut state, update)?;
            let _ = sender.send(state.clone());
            Ok(())
        };
        (
            PortHandle::new_full(
                self.clone(),
                port_index,
                UnboundedPortSender::Func(Arc::new(enqueue)),
                reducer_spec,
                streaming_opts,
            ),
            PortReceiver::new(receiver, port_id, /*coalesce=*/ true, self.clone()),
        )
    }

    /// Open a port that accepts M-typed messages, using the provided function
    /// to enqueue.
    // TODO: consider making lifetime bound to Self instead.
    pub(crate) fn open_enqueue_port<M: Message>(
        &self,
        enqueue: impl Fn(Flattrs, M) -> Result<(), anyhow::Error> + Send + Sync + 'static,
    ) -> PortHandle<M> {
        PortHandle::new_full(
            self.clone(),
            self.inner.allocate_port(),
            UnboundedPortSender::Func(Arc::new(enqueue)),
            None,
            StreamingReducerOpts::default(),
        )
    }

    /// Open a runtime-dispatched handler port that accepts M-typed
    /// messages using the provided enqueue function.
    pub(crate) fn open_handler_enqueue_port<M: Message>(
        &self,
        enqueue: impl Fn(Flattrs, M) -> Result<(), anyhow::Error> + Send + Sync + 'static,
    ) -> PortHandle<M> {
        let port_index = self.inner.allocate_port();
        let enqueue = Arc::new(enqueue);
        let sender = Arc::new(HandlerPortSender::new(
            UnboundedPortSender::Func(enqueue),
            self.inner.handler_ingress.clone(),
        ));
        PortHandle::new_full(
            self.clone(),
            port_index,
            UnboundedPortSender::Handler(sender),
            None,
            StreamingReducerOpts::default(),
        )
    }

    /// Open a new one-shot port that accepts M-typed messages. The
    /// returned port may be used to send a single message; ditto the
    /// receiver may receive a single message.
    pub fn open_once_port<M: Message>(&self) -> (OncePortHandle<M>, OncePortReceiver<M>) {
        let port_index = self.inner.allocate_port();
        let port_id = self.inner.actor_id.port_addr(Port::from(port_index));
        let (sender, receiver) = oneshot::channel::<M>();
        (
            OncePortHandle {
                mailbox: self.clone(),
                port_index,
                port_id: port_id.clone(),
                sender,
                reducer_spec: None,
            },
            OncePortReceiver {
                receiver: Some(receiver),
                port_id,
                mailbox: self.clone(),
            },
        )
    }

    /// Open a new one-shot port with a reducer. This port is designed
    /// to be used with casting, where the port is split across multiple
    /// destinations and responses are accumulated using the reducer.
    /// The accumulator type must have a ReducerSpec.
    ///
    /// The returned handle can be bound and embedded in cast messages.
    /// When the message is split by CommActor, each destination receives a
    /// split port. Responses to split ports are accumulated using the
    /// accumulator's reducer, and the final accumulated result is delivered
    /// to the returned receiver.
    ///
    /// Note: For accumulators used with casting, `Update` and `State` types
    /// must be the same (e.g., `sum<u64>` where both are `u64`).
    pub fn open_reduce_port<A, T>(
        &self,
        accum: A,
    ) -> (OncePortHandle<A::State>, OncePortReceiver<A::State>)
    where
        A: Accumulator<State = T, Update = T> + Send + Sync + 'static,
        T: Message + Default + Clone,
    {
        let port_index = self.inner.allocate_port();
        let (sender, receiver) = oneshot::channel::<T>();
        let port_id = self.inner.actor_id.port_addr(Port::from(port_index));
        let reducer_spec = accum.reducer_spec();
        assert!(
            reducer_spec.is_some(),
            "cannot use a reduce port without a ReducerSpec"
        );

        (
            OncePortHandle {
                mailbox: self.clone(),
                port_index,
                port_id: port_id.clone(),
                sender,
                reducer_spec,
            },
            OncePortReceiver {
                receiver: Some(receiver),
                port_id,
                mailbox: self.clone(),
            },
        )
    }

    #[allow(dead_code)]
    fn error(&self, err: MailboxErrorKind) -> MailboxError {
        MailboxError::new(self.inner.actor_id.clone(), err)
    }

    fn lookup_sender<M: RemoteMessage>(&self) -> Option<UnboundedPortSender<M>> {
        let port_index = M::port();
        self.inner.ports.get(&port_index).and_then(|boxed| {
            boxed
                .as_any()
                .downcast_ref::<UnboundedSender<M>>()
                .map(|s| {
                    assert_eq!(
                        s.port_id,
                        self.actor_addr().port_addr(Port::from(port_index)),
                        "port_id mismatch in downcasted UnboundedSender"
                    );
                    s.sender.clone()
                })
        })
    }

    /// Retrieve the bound undeliverable handler port handle.
    pub fn bound_return_handle(&self) -> Option<PortHandle<Undeliverable<MessageEnvelope>>> {
        self.lookup_sender::<Undeliverable<MessageEnvelope>>()
            .map(|sender| PortHandle::new(self.clone(), self.inner.allocate_port(), sender))
    }

    pub(crate) fn allocate_port(&self) -> u64 {
        self.inner.allocate_port()
    }

    fn bind<M: RemoteMessage>(&self, handle: &PortHandle<M>) -> PortRef<M> {
        assert_eq!(
            handle.inner.mailbox.actor_addr(),
            self.actor_addr(),
            "port does not belong to mailbox"
        );

        // TODO: don't even allocate a port until the port is bound. Possibly
        // have handles explicitly staged (unbound, bound).
        let port_ref = self
            .actor_addr()
            .port_addr(Port::from(handle.inner.port_index));
        match self.inner.ports.entry(handle.inner.port_index) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(UnboundedSender::new(
                    handle.inner.sender.clone(),
                    port_ref.clone(),
                )));
            }
            Entry::Occupied(_entry) => {}
        }

        PortRef::attest(port_ref)
    }

    fn bind_to_handler_port<M: RemoteMessage>(&self, handle: &PortHandle<M>) {
        assert_eq!(
            handle.inner.mailbox.actor_addr(),
            self.actor_addr(),
            "port does not belong to mailbox"
        );

        let port_index = M::port();
        let port_ref = self.actor_addr().port_addr(Port::from(port_index));
        match self.inner.ports.entry(port_index) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(UnboundedSender::new(
                    handle.inner.sender.clone(),
                    port_ref,
                )));
            }
            Entry::Occupied(_entry) => panic!("port {} already bound", port_ref),
        }
    }

    fn bind_once<M: RemoteMessage>(&self, handle: OncePortHandle<M>) {
        let port_id = handle.port_addr().clone();
        match self.inner.ports.entry(handle.port_index) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(OnceSender::new(handle.sender, port_id.clone())));
            }
            Entry::Occupied(_entry) => {}
        }
    }

    pub(crate) fn bind_untyped(&self, port_id: &PortAddr, sender: UntypedUnboundedSender) {
        assert_eq!(
            port_id.actor_addr(),
            *self.actor_addr(),
            "port does not belong to mailbox"
        );

        match self.inner.ports.entry(port_id.index()) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(sender));
            }
            Entry::Occupied(_entry) => {}
        }
    }

    pub(crate) fn close(&self, status: ActorStatus) {
        let mut closed = self.inner.closed.write().unwrap();
        if closed.is_some() {
            panic!("mailbox with owner {} already closed", self.actor_addr());
        }
        let _ = closed.insert(status);
    }

    /// Start draining handler ingress for this mailbox.
    ///
    /// Draining is a mailbox lifecycle property, but it is enforced
    /// only by runtime-dispatched handler ports. New handler work is
    /// rejected at the handler-port sender. Work that already entered
    /// a handler-port sender before draining began is allowed to finish
    /// enqueueing, and this method waits for those in-flight enqueue
    /// attempts before it returns. After this method returns, no handler
    /// work can still be entering the actor queue through a handler
    /// port.
    ///
    /// Runtime/control ports and ordinary mailbox ports remain usable
    /// while draining, so shutdown can continue and already accepted
    /// work can flush.
    ///
    /// This is distinct from [`Mailbox::close`], which marks the
    /// mailbox terminal and rejects all subsequent local delivery.
    pub(crate) fn drain(&self) {
        self.inner.handler_ingress.drain();
    }
}

impl context::Mailbox for Mailbox {
    fn mailbox(&self) -> &Mailbox {
        self
    }
}

// TODO: figure out what to do with these interfaces -- possibly these caps
// do not have to be private.

/// Open a port given a capability.
pub fn open_port<M: Message>(cx: &impl context::Mailbox) -> (PortHandle<M>, PortReceiver<M>) {
    cx.mailbox().open_port()
}

/// Open a one-shot port given a capability. This is a public method primarily to
/// enable macro-generated clients.
pub fn open_once_port<M: Message>(
    cx: &impl context::Mailbox,
) -> (OncePortHandle<M>, OncePortReceiver<M>) {
    cx.mailbox().open_once_port()
}

#[async_trait]
impl MailboxSender for Mailbox {
    /// Deliver a serialized message to the provided port ID. This method fails
    /// if the message does not deserialize into the expected type.
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        metrics::MAILBOX_POSTS.add(
            1,
            hyperactor_telemetry::kv_pairs!(
                "actor_id" => envelope.sender.to_string(),
                "dest_actor_id" => envelope.dest.actor_addr().to_string(),
            ),
        );
        tracing::trace!(
            name = "post",
            actor_name = envelope.sender.label().map_or("?", |l| l.as_str()),
            actor_id = envelope.sender.to_string(),
            "posting message to {}",
            envelope.dest
        );

        if envelope.dest().actor_id() != self.inner.actor_id.id() {
            let err = DeliveryError::Mailbox(format!(
                "mailbox owner {} cannot deliver to {}",
                self.inner.actor_id,
                envelope.dest().actor_addr()
            ));
            let failure = DeliveryFailure::new(InvalidReference::new(
                envelope.dest().actor_addr(),
                InvalidReferenceReason::WrongMailboxOwner,
            ));
            return envelope.undeliverable_with_failure(err, failure, return_handle);
        }

        let port_index = envelope.dest().index();

        // Clone the Arc<dyn SerializedSender> out of the DashMap while holding
        // only a short-lived read lock, then release the lock before calling
        // send_serialized. This prevents a deadlock that occurs when
        // send_serialized itself tries to post a message to another port on the
        // same mailbox: if both ports hash to the same DashMap shard, acquiring
        // the shard's write lock a second time on the same thread deadlocks
        // (RwLock is not reentrant). DashMap uses a random per-process hasher,
        // so whether two port indices collide in the same shard varies across
        // test runs, explaining the longstanding flaky timeout failures.
        let port_sender = match self.inner.ports.get(&port_index) {
            None => {
                let (err, failure) = unbound_port_delivery_failure(
                    envelope.dest(),
                    envelope.data(),
                    self.inner.next_port.load(Ordering::SeqCst),
                );
                return envelope.undeliverable_with_failure(err, failure, return_handle);
            }
            Some(ref_) => {
                let closed = self.inner.closed.read().unwrap();
                if let Some(status) = &*closed {
                    match status {
                        ActorStatus::Stopped(reason) => {
                            let err = format!(
                                "mailbox owner {} is stopped: {}",
                                self.inner.actor_id, reason
                            );
                            let failure = DeliveryFailure::new(InvalidReference::new(
                                envelope.dest().actor_addr(),
                                InvalidReferenceReason::ActorStopped,
                            ));
                            return envelope.undeliverable_with_failure(
                                DeliveryError::Mailbox(err),
                                failure,
                                return_handle,
                            );
                        }
                        ActorStatus::Failed(actor_error) => {
                            let err = format!(
                                "mailbox owner {} failed: {}",
                                self.inner.actor_id, actor_error
                            );
                            let failure = DeliveryFailure::new(InvalidReference::new(
                                envelope.dest().actor_addr(),
                                InvalidReferenceReason::ActorFailed,
                            ));
                            return envelope.undeliverable_with_failure(
                                DeliveryError::Mailbox(err),
                                failure,
                                return_handle,
                            );
                        }
                        _ => {
                            let err = format!(
                                "mailbox owner {} closed unexpectedly: {:?}",
                                self.inner.actor_id, status
                            );
                            return envelope
                                .undeliverable(DeliveryError::Mailbox(err), return_handle);
                        }
                    }
                }
                // Clone the Arc so we can release the shard read lock before
                // calling send_serialized, which may re-enter post_unchecked.
                Arc::clone(&*ref_)
            }
        };
        // Shard read lock is released here when `ref_` is dropped.

        let (metadata, data) = envelope.open();
        let MessageMetadata {
            mut headers,
            sender,
            dest,
            errors: metadata_errors,
            delivery_failures,
            ttl,
            return_undeliverable,
        } = metadata;

        let to_actor_id = hash_to_u64(&dest);
        let message_id = hyperactor_telemetry::generate_message_id(to_actor_id);
        headers.set(crate::mailbox::headers::TELEMETRY_MESSAGE_ID, message_id);
        // Only set sender hash if not already present (cast path
        // pre-sets it with the originating actor).
        if !headers.contains_key(crate::mailbox::headers::SENDER_ACTOR_ID_HASH) {
            headers.set(
                crate::mailbox::headers::SENDER_ACTOR_ID_HASH,
                hash_to_u64(&sender),
            );
        }
        headers.set(crate::mailbox::headers::TELEMETRY_PORT_ID, dest.index());

        match port_sender.send_serialized(headers, data) {
            Ok(disposition) => {
                hyperactor_telemetry::notify_message_status(
                    hyperactor_telemetry::MessageStatusEvent {
                        timestamp: std::time::SystemTime::now(),
                        id: hyperactor_telemetry::generate_status_event_id(message_id),
                        message_id,
                        status: "queued".to_string(),
                    },
                );

                if disposition == SerializedSendDisposition::DeliveredAndExhausted {
                    self.inner.ports.remove(&port_index);
                }
            }
            Err(SerializedSendFailure::Dead { data, headers }) => {
                self.inner.ports.remove(&port_index);
                let (err, failure) = unbound_port_delivery_failure(
                    &dest,
                    &data,
                    self.inner.next_port.load(Ordering::SeqCst),
                );

                MessageEnvelope::seal(
                    MessageMetadata {
                        headers,
                        sender,
                        dest,
                        errors: metadata_errors,
                        delivery_failures,
                        ttl,
                        return_undeliverable,
                    },
                    data,
                )
                .undeliverable_with_failure(err, failure, return_handle)
            }
            Err(SerializedSendFailure::Error(SerializedSendError {
                data,
                error: sender_error,
                headers,
            })) => {
                let failure = matches!(
                    sender_error.kind(),
                    MailboxSenderErrorKind::Deserialize(_, _)
                )
                .then(|| {
                    DeliveryFailure::new(InvalidReference::new(
                        dest.clone(),
                        InvalidReferenceReason::ProtocolMismatch,
                    ))
                });
                let err = DeliveryError::Mailbox(format!("{}", sender_error));

                let envelope = MessageEnvelope::seal(
                    MessageMetadata {
                        headers,
                        sender,
                        dest,
                        errors: metadata_errors,
                        delivery_failures,
                        ttl,
                        return_undeliverable,
                    },
                    data,
                );
                if let Some(failure) = failure {
                    envelope.undeliverable_with_failure(err, failure, return_handle)
                } else {
                    envelope.undeliverable(err, return_handle)
                }
            }
        }
    }
}

fn unbound_port_delivery_failure(
    port: &PortAddr,
    data: &wirevalue::Any,
    next_port: u64,
) -> (DeliveryError, DeliveryFailure) {
    let error = DeliveryError::Unroutable(format!(
        "port not bound in mailbox; port id: {}; message type: {}",
        port.index(),
        data.typename().map_or_else(
            || format!("unregistered type hash {}", data.typehash()),
            |name| name.to_string(),
        )
    ));
    let failure = if port.is_handler_port() {
        DeliveryFailure::new(InvalidReference::new(
            port.clone(),
            InvalidReferenceReason::HandlerNotBound,
        ))
    } else if port.index() < next_port {
        DeliveryFailure::new(UndeliverableReason::PortGone(PortGone::new(
            port.clone(),
            data.typename().map(str::to_string),
        )))
    } else {
        DeliveryFailure::new(InvalidReference::new(
            port.clone(),
            InvalidReferenceReason::PortNeverAllocated,
        ))
    };

    (error, failure)
}

/// Inner state of a [`PortHandle`], shared via `Arc` to make cloning cheap
/// (single atomic refcount bump instead of cloning each field).
struct PortHandleInner<M: Message> {
    mailbox: Mailbox,
    port_index: u64,
    sender: UnboundedPortSender<M>,
    // We would like this to be a Arc<RwLock<Option<PortAddr<M>>>>, but we cannot
    // write down the type PortAddr<M> (M: Message), even though we cannot
    // legally construct such a value without M: RemoteMessage. We could consider
    // making PortAddr<M> valid for M: Message, but constructible only for
    // M: RemoteMessage, but the guarantees offered by the impossibilty of even
    // writing down the type are appealing.
    bound: Arc<RwLock<Option<PortAddr>>>,
    // Typehash of an optional reducer. When it's defined, we include it in port
    /// references to optionally enable incremental accumulation.
    reducer_spec: Option<ReducerSpec>,
    /// Streaming reducer options.
    streaming_opts: StreamingReducerOpts,
}

impl<M: Message> fmt::Debug for PortHandleInner<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortHandleInner")
            .field("mailbox", &self.mailbox)
            .field("port_index", &self.port_index)
            .field("sender", &self.sender)
            .field("bound", &self.bound)
            .field("reducer_spec", &self.reducer_spec)
            .field("streaming_opts", &self.streaming_opts)
            .finish()
    }
}

/// A port to which M-typed messages can be delivered. Ports may be
/// serialized to be sent to other actors. However, when a port is
/// deserialized, it may no longer be used to send messages directly
/// to a mailbox since it is no longer associated with a local mailbox
/// ([`Mailbox::send`] will fail). However, the runtime may accept
/// remote Ports, and arrange for these messages to be delivered
/// indirectly through inter-node message passing.
#[derive(Debug)]
pub struct PortHandle<M: Message> {
    inner: Arc<PortHandleInner<M>>,
}

impl<M: Message> PortHandle<M> {
    fn new_full(
        mailbox: Mailbox,
        port_index: u64,
        sender: UnboundedPortSender<M>,
        reducer_spec: Option<ReducerSpec>,
        streaming_opts: StreamingReducerOpts,
    ) -> Self {
        Self {
            inner: Arc::new(PortHandleInner {
                mailbox,
                port_index,
                sender,
                bound: Arc::new(RwLock::new(None)),
                reducer_spec,
                streaming_opts,
            }),
        }
    }

    fn new(mailbox: Mailbox, port_index: u64, sender: UnboundedPortSender<M>) -> Self {
        Self::new_full(
            mailbox,
            port_index,
            sender,
            None,
            StreamingReducerOpts::default(),
        )
    }

    pub(crate) fn location(&self) -> PortLocation {
        match self.inner.bound.read().unwrap().as_ref() {
            Some(port_id) => PortLocation::Bound(port_id.clone()),
            None => PortLocation::new_unbound::<M>(self.inner.mailbox.actor_addr().clone()),
        }
    }

    pub(crate) fn try_send<C>(&self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        let closed = self.inner.mailbox.inner.closed.read().unwrap();

        if let Some(status) = &*closed {
            let err = MailboxError {
                actor_id: self.inner.mailbox.actor_addr().clone(),
                kind: MailboxErrorKind::OwnerTerminated(status.clone()),
            };
            return Err(MailboxSenderError::new_unbound::<M>(
                self.inner.mailbox.actor_addr().clone(),
                MailboxSenderErrorKind::Mailbox(err),
            ));
        }
        let mut headers = Flattrs::new();

        crate::mailbox::headers::set_send_timestamp(&mut headers);
        crate::mailbox::headers::set_rust_message_type::<M>(&mut headers);
        // Hold read lock while checking and sending to prevent race with bind().
        // Message sent from handle is delivered immediately. It could race with
        // messages from refs. So we need to assign seq if the handle is bound.
        let bound_guard = self.inner.bound.read().unwrap();
        if let Some(bound_port) = bound_guard.as_ref() {
            let sequencer = cx.instance().sequencer();
            let bound_ref: PortAddr = bound_port.clone();
            let seq_info = sequencer.assign_seq(&bound_ref);
            headers.set(SEQ_INFO, seq_info);
        } else {
            // Because the port is not bound, messages can only be sent through
            // this port handle's underlying tokio channel directly. As a result,
            // we do not need to assign seq to the message. We do not need to
            // worry about race condition due to bound_guard.
            headers.set(SEQ_INFO, SeqInfo::Direct);
        }
        // Encountering error means the port is closed. So we do not need to
        // rollback the seq, because no message can be delivered to it, and
        // subsequently do not need to worry about out-of-sequence for messages
        // after this seq.
        //
        // Theoretically, we could have deadlock if
        //   1. `sender.send` attempts to hold read lock of this PortHandle's
        //      `bound` field, and in the meantime,
        //   2.  another thread is trying to bind and thus waiting for write lock.
        // But we do not expect `sender.send` to use the same PortHandle, so this
        // deadlock scenario should not happen.
        self.inner.sender.send(headers, message).map_err(|err| {
            MailboxSenderError::new_unbound::<M>(
                self.inner.mailbox.actor_addr().clone(),
                classify_sender_error(err),
            )
        })
    }
}

impl<M> Endpoint<M> for &PortHandle<M>
where
    M: Message,
{
    fn endpoint_location(&self) -> EndpointLocation {
        self.location().into()
    }

    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor,
    {
        if let Err(err) = self.try_send(cx, message) {
            cx.instance()
                .report_lost_message(LostMessage::from_send_error::<M>(
                    cx.mailbox().actor_addr().clone(),
                    self.endpoint_location(),
                    &err,
                ));
        }
    }
}

impl<M: Message> PortHandle<M> {
    /// A contravariant map: using the provided function to translate
    /// `R`-typed messages to `M`-typed ones, delivered on this port.
    pub fn contramap<R, F>(&self, unmap: F) -> PortHandle<R>
    where
        R: Message,
        F: Fn(R) -> M + Send + Sync + 'static,
    {
        let port_index = self.inner.mailbox.inner.allocate_port();
        let sender = self.inner.sender.clone();
        PortHandle::new(
            self.inner.mailbox.clone(),
            port_index,
            UnboundedPortSender::Func(Arc::new(move |headers, value: R| {
                sender.send(headers, unmap(value))
            })),
        )
    }
}

impl<M: RemoteMessage> PortHandle<M> {
    /// Bind this port, making it accessible to remote actors.
    pub fn bind(&self) -> PortRef<M> {
        let port_ref = {
            let mut guard = self.inner.bound.write().unwrap();
            guard
                .get_or_insert_with(|| self.inner.mailbox.bind(self).into_port_addr())
                .clone()
        };
        PortRef::attest_reducible(
            port_ref,
            self.inner.reducer_spec.clone(),
            self.inner.streaming_opts.clone(),
        )
    }

    /// Bind this handle to the handler port for message type `M`. This method
    /// will panic if the handle is already bound.
    ///
    /// This is used by [`actor::Binder`] implementations to bind actor refs.
    /// This is not intended for general use.
    pub(crate) fn bind_handler_port(&self) {
        let port_id = self
            .inner
            .mailbox
            .actor_addr()
            .port_addr(Port::from(M::port()));
        {
            let mut guard = self.inner.bound.write().unwrap();
            if guard.is_some() {
                panic!(
                    "could not bind port handle {} as {port_id}: already bound",
                    self.inner.port_index
                );
            }
            *guard = Some(port_id);
        }
        self.inner.mailbox.bind_to_handler_port(self);
    }
}

impl<M: Message> Clone for PortHandle<M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M: Message> fmt::Display for PortHandle<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.location(), f)
    }
}

/// A one-shot port handle to which M-typed messages can be delivered.
#[derive(Debug)]
pub struct OncePortHandle<M: Message> {
    mailbox: Mailbox,
    port_index: u64,
    port_id: PortAddr,
    sender: oneshot::Sender<M>,
    reducer_spec: Option<ReducerSpec>,
}

impl<M: Message> OncePortHandle<M> {
    /// This port's address.
    // TODO: make value
    pub fn port_addr(&self) -> &PortAddr {
        &self.port_id
    }
}

impl<M> Endpoint<M> for OncePortHandle<M>
where
    M: Message,
{
    fn endpoint_location(&self) -> EndpointLocation {
        EndpointLocation::Port(self.port_id.clone())
    }

    fn post<C>(self, _cx: &C, message: M)
    where
        C: context::Actor,
    {
        // TODO: Assign seq to the message if the port is bound to a handler port
        // in the future.
        assert!(
            !self.port_addr().is_handler_port(),
            "OncePortHandle currently does not support handler ports; a \
            prerequisite of that support is to assign seq to messages \
            if the port is a handler port."
        );

        let actor_id = self.mailbox.actor_addr().clone();
        let endpoint_location = self.endpoint_location();
        if let Err(err) = self.sender.send(message).map_err(|_| {
            // Here, the value is returned when the port is
            // closed.  We should consider having a similar
            // API for send_once, though arguably it makes less
            // sense in this context.
            MailboxSenderError::new_unbound::<M>(actor_id, MailboxSenderErrorKind::Closed)
        }) {
            _cx.instance()
                .report_lost_message(LostMessage::from_send_error::<M>(
                    _cx.mailbox().actor_addr().clone(),
                    endpoint_location,
                    &err,
                ));
        }
    }
}

impl<M: RemoteMessage> OncePortHandle<M> {
    /// Turn this handle into a ref that may be passed to
    /// a remote actor. The remote actor can then use the
    /// ref to send a message to the port. Creating a ref also
    /// binds the port, so that it is remotely writable.
    pub fn bind(self) -> OncePortRef<M> {
        let port_id: PortAddr = self.port_addr().clone();
        let reducer_spec = self.reducer_spec.clone();
        self.mailbox.clone().bind_once(self);
        OncePortRef::attest_reducible(port_id, reducer_spec)
    }
}

impl<M: Message> fmt::Display for OncePortHandle<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.port_addr(), f)
    }
}

/// A receiver of M-typed messages, used by actors to receive messages
/// on open ports.
#[derive(Debug)]
pub struct PortReceiver<M> {
    receiver: mpsc::UnboundedReceiver<M>,
    port_id: PortAddr,
    /// When multiple messages are put in channel, only receive the latest one
    /// if coalesce is true. Other messages will be discarded.
    coalesce: bool,
    /// State is used to remove the port from service when the receiver
    /// is dropped.
    mailbox: Mailbox,
}

impl<M> PortReceiver<M> {
    fn new(
        receiver: mpsc::UnboundedReceiver<M>,
        port_id: PortAddr,
        coalesce: bool,
        mailbox: Mailbox,
    ) -> Self {
        Self {
            receiver,
            port_id,
            coalesce,
            mailbox,
        }
    }

    /// Tries to receive the next value for this receiver.
    /// This function returns `Ok(None)` if the receiver is empty
    /// and returns a MailboxError if the receiver is disconnected.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `MailboxError`.
    pub fn try_recv(&mut self) -> Result<Option<M>, MailboxError> {
        let mut next = self.receiver.try_recv();
        // To coalesce, drain the mpsc queue and only keep the last one.
        if self.coalesce
            && let Some(latest) = self.drain().pop()
        {
            next = Ok(latest);
        }
        match next {
            Ok(msg) => Ok(Some(msg)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(MailboxError::new(
                self.actor_addr().clone(),
                MailboxErrorKind::Closed,
            )),
        }
    }

    /// Receive the next message from the port corresponding with this
    /// receiver.
    pub async fn recv(&mut self) -> Result<M, MailboxError> {
        let mut next = self.receiver.recv().await;
        // To coalesce, get the last message from the queue if there are
        // more on the mspc queue.
        if self.coalesce
            && let Some(latest) = self.drain().pop()
        {
            next = Some(latest);
        }
        next.ok_or(MailboxError::new(
            self.actor_addr().clone(),
            MailboxErrorKind::Closed,
        ))
    }

    /// Drains all available messages from the port.
    pub fn drain(&mut self) -> Vec<M> {
        let mut drained: Vec<M> = Vec::new();
        while let Ok(msg) = self.receiver.try_recv() {
            // To coalesce, discard the old message if there is any.
            if self.coalesce {
                drained.pop();
            }
            drained.push(msg);
        }
        drained
    }

    fn port(&self) -> u64 {
        self.port_id.index()
    }

    fn actor_addr(&self) -> ActorAddr {
        self.port_id.actor_addr()
    }
}

impl<M> Drop for PortReceiver<M> {
    fn drop(&mut self) {
        // MARIUS: do we need to tombstone these? or should we
        // error out if we have removed the receiver before serializing the port ref?
        // ("no longer live")?
        self.mailbox.inner.ports.remove(&self.port());
    }
}

impl<M> Stream for PortReceiver<M> {
    type Item = Result<M, MailboxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        std::pin::pin!(self.recv()).poll(cx).map(Some)
    }
}

/// A receiver of M-typed messages from [`OncePort`]s.
pub struct OncePortReceiver<M> {
    receiver: Option<oneshot::Receiver<M>>,
    port_id: PortAddr,

    /// Mailbox is used to remove the port from service when the receiver
    /// is dropped.
    mailbox: Mailbox,
}

impl<M> OncePortReceiver<M> {
    /// Receive message from the one-shot port associated with this
    /// receiver.  Recv consumes the receiver: it is no longer valid
    /// after this call.
    pub async fn recv(mut self) -> Result<M, MailboxError> {
        std::mem::take(&mut self.receiver)
            .unwrap()
            .await
            .map_err(|err| {
                MailboxError::new(
                    self.actor_addr().clone(),
                    MailboxErrorKind::Recv(self.port_id.clone(), err.into()),
                )
            })
    }

    fn port(&self) -> u64 {
        self.port_id.index()
    }

    fn actor_addr(&self) -> ActorAddr {
        self.port_id.actor_addr()
    }
}

impl<M> Drop for OncePortReceiver<M> {
    fn drop(&mut self) {
        // MARIUS: do we need to tombstone these? or should we
        // error out if we have removed the receiver before serializing the port ref?
        // ("no longer live")?
        self.mailbox.inner.ports.remove(&self.port());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SerializedSendDisposition {
    Delivered,
    DeliveredAndExhausted,
}

/// Error that that occur during `SerializedSender::send_serialized`.
pub(crate) struct SerializedSendError {
    /// The headers associated with the message.
    pub(crate) headers: Flattrs,
    /// The message was tried to send.
    pub(crate) data: wirevalue::Any,
    /// The mailbox sender error that occurred.
    pub(crate) error: MailboxSenderError,
}

pub(crate) enum SerializedSendFailure {
    Dead {
        headers: Flattrs,
        data: wirevalue::Any,
    },
    Error(SerializedSendError),
}

/// SerializedSender encapsulates senders:
///   - It performs type erasure (and thus it is object-safe).
///   - It abstracts over [`Port`]s and [`OncePort`]s, by dynamically tracking the
///     validity of the underlying port.
trait SerializedSender: Send + Sync {
    /// Enables downcasting from `&dyn SerializedSender` to concrete
    /// types.
    ///
    /// Used by `Mailbox::lookup_sender` to downcast to
    /// `&UnboundedSender<M>` via `Any::downcast_ref`.
    fn as_any(&self) -> &dyn Any;

    /// Send a serialized message. SerializedSender will deserialize the
    /// message (failing if it fails to deserialize), and then send the
    /// resulting message on the underlying port.
    ///
    /// The returned disposition describes successful delivery. Errors
    /// report both the failed message and whether the sender remains live.
    fn send_serialized(
        &self,
        headers: Flattrs,
        serialized: wirevalue::Any,
    ) -> Result<SerializedSendDisposition, SerializedSendFailure>;
}

#[derive(Debug, thiserror::Error)]
#[error("handler port closed")]
struct HandlerPortClosedError;

fn classify_sender_error(err: anyhow::Error) -> MailboxSenderErrorKind {
    if err.is::<HandlerPortClosedError>() {
        MailboxSenderErrorKind::Closed
    } else {
        MailboxSenderErrorKind::Other(err)
    }
}

/// A sender to an M-typed unbounded port.
enum UnboundedPortSender<M: Message> {
    /// Send directly to the mpsc queue.
    Mpsc(mpsc::UnboundedSender<M>),
    /// Use the provided function to enqueue the item.
    Func(Arc<dyn Fn(Flattrs, M) -> Result<(), anyhow::Error> + Send + Sync>),
    /// A runtime-dispatched handler port that observes mailbox drain state.
    Handler(Arc<HandlerPortSender<M>>),
}

impl<M: Message> UnboundedPortSender<M> {
    fn send(&self, headers: Flattrs, message: M) -> Result<(), anyhow::Error> {
        match self {
            Self::Mpsc(sender) => sender.send(message).map_err(anyhow::Error::from),
            Self::Func(func) => func(headers, message),
            Self::Handler(sender) => sender.send(headers, message),
        }
    }
}

// We implement Clone manually as derive(Clone) places unnecessarily
// strict bounds on the type parameter M.
impl<M: Message> Clone for UnboundedPortSender<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Mpsc(sender) => Self::Mpsc(sender.clone()),
            Self::Func(func) => Self::Func(func.clone()),
            Self::Handler(sender) => Self::Handler(sender.clone()),
        }
    }
}

impl<M: Message> Debug for UnboundedPortSender<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Mpsc(q) => f.debug_tuple("UnboundedPortSender::Mpsc").field(q).finish(),
            Self::Func(_) => f
                .debug_tuple("UnboundedPortSender::Func")
                .field(&"..")
                .finish(),
            Self::Handler(_) => f
                .debug_tuple("UnboundedPortSender::Handler")
                .field(&"..")
                .finish(),
        }
    }
}

const HANDLER_INGRESS_DRAINING: usize = 1usize << (usize::BITS as usize - 1);
const HANDLER_INGRESS_ACTIVE_MASK: usize = !HANDLER_INGRESS_DRAINING;

struct HandlerIngressGate {
    state: AtomicUsize,
    wait_lock: Mutex<()>,
    drained: Condvar,
}

struct HandlerIngressGuard {
    gate: Arc<HandlerIngressGate>,
}

impl HandlerIngressGate {
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            wait_lock: Mutex::new(()),
            drained: Condvar::new(),
        }
    }

    fn try_enter(self: &Arc<Self>) -> Result<HandlerIngressGuard, HandlerPortClosedError> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & HANDLER_INGRESS_DRAINING != 0 {
                return Err(HandlerPortClosedError);
            }

            let active = state & HANDLER_INGRESS_ACTIVE_MASK;
            assert!(
                active < HANDLER_INGRESS_ACTIVE_MASK,
                "too many active handler ingress sends"
            );

            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(HandlerIngressGuard {
                        gate: Arc::clone(self),
                    });
                }
                Err(next_state) => state = next_state,
            }
        }
    }

    fn drain(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & HANDLER_INGRESS_DRAINING != 0 {
                break;
            }
            match self.state.compare_exchange_weak(
                state,
                state | HANDLER_INGRESS_DRAINING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next_state) => state = next_state,
            }
        }

        let mut wait_guard = self.wait_lock.lock().unwrap();
        while self.state.load(Ordering::Acquire) & HANDLER_INGRESS_ACTIVE_MASK != 0 {
            wait_guard = self.drained.wait(wait_guard).unwrap();
        }
    }
}

impl Drop for HandlerIngressGuard {
    fn drop(&mut self) {
        let previous = self.gate.state.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous & HANDLER_INGRESS_ACTIVE_MASK != 0,
            "handler ingress active count underflow"
        );
        if previous & HANDLER_INGRESS_DRAINING != 0 && previous & HANDLER_INGRESS_ACTIVE_MASK == 1 {
            // Pair only the final active-count decrement during drain
            // with the drain waiter's condvar mutex. Ordinary send
            // completion stays on the atomic fast path, but the final
            // sender still cannot notify between the waiter's state
            // check and its transition to sleep.
            let _wait_guard = self.gate.wait_lock.lock().unwrap();
            self.gate.drained.notify_all();
        }
    }
}

struct HandlerPortSender<M: Message> {
    sender: UnboundedPortSender<M>,
    gate: Arc<HandlerIngressGate>,
}

impl<M: Message> HandlerPortSender<M> {
    fn new(sender: UnboundedPortSender<M>, gate: Arc<HandlerIngressGate>) -> Self {
        Self { sender, gate }
    }

    fn send(&self, headers: Flattrs, message: M) -> Result<(), anyhow::Error> {
        let _guard = self.gate.try_enter()?;
        self.sender.send(headers, message)
    }
}

struct UnboundedSender<M: Message> {
    sender: UnboundedPortSender<M>,
    port_id: PortAddr,
}

impl<M: Message> UnboundedSender<M> {
    /// Create a new UnboundedSender encapsulating the provided
    /// sender.
    fn new(sender: UnboundedPortSender<M>, port_id: PortAddr) -> Self {
        Self { sender, port_id }
    }

    #[allow(dead_code)]
    fn send(&self, headers: Flattrs, message: M) -> Result<(), MailboxSenderError> {
        self.sender.send(headers, message).map_err(|err| {
            MailboxSenderError::new_bound(self.port_id.clone(), classify_sender_error(err))
        })
    }
}

// Clone is implemented explicitly because the derive macro demands M:
// Clone directly. In this case, it isn't needed because Arc<T> can
// clone for any T.
impl<M: Message> Clone for UnboundedSender<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            port_id: self.port_id.clone(),
        }
    }
}

impl<M: RemoteMessage> SerializedSender for UnboundedSender<M> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn send_serialized(
        &self,
        headers: Flattrs,
        serialized: wirevalue::Any,
    ) -> Result<SerializedSendDisposition, SerializedSendFailure> {
        // Here, the stack ensures that this port is only instantiated for M-typed messages.
        // This does not protect against bad senders (e.g., encoding wrongly-typed messages),
        // but it is required as we have some usages that rely on representational equivalence
        // to provide type indexing, specifically in `IndexedErasedUnbound` which is used to
        // support port aggregation.
        match serialized.deserialized_unchecked() {
            Ok(message) => match self.sender.send(headers.clone(), message) {
                Ok(()) => Ok(SerializedSendDisposition::Delivered),
                Err(_) if matches!(&self.sender, UnboundedPortSender::Mpsc(_)) => {
                    Err(SerializedSendFailure::Dead {
                        data: serialized,
                        headers,
                    })
                }
                Err(err) => Err(SerializedSendFailure::Error(SerializedSendError {
                    data: serialized,
                    error: MailboxSenderError::new_bound(
                        self.port_id.clone(),
                        classify_sender_error(err),
                    ),
                    headers,
                })),
            },
            Err(err) => Err(SerializedSendFailure::Error(SerializedSendError {
                data: serialized,
                error: MailboxSenderError::new_bound(
                    self.port_id.clone(),
                    MailboxSenderErrorKind::Deserialize(M::typename(), err.into()),
                ),
                headers,
            })),
        }
    }
}

/// OnceSender encapsulates an underlying one-shot sender, dynamically
/// tracking its validity.
#[derive(Debug)]
struct OnceSender<M: Message> {
    sender: Arc<Mutex<Option<oneshot::Sender<M>>>>,
    port_id: PortAddr,
}

impl<M: Message> OnceSender<M> {
    /// Create a new OnceSender encapsulating the provided one-shot
    /// sender.
    fn new(sender: oneshot::Sender<M>, port_id: PortAddr) -> Self {
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            port_id,
        }
    }

    fn send_once(&self, message: M) -> Result<SerializedSendDisposition, MailboxSenderError> {
        // TODO: we should replace the sender on error
        match self.sender.lock().unwrap().take() {
            None => Err(MailboxSenderError::new_bound(
                self.port_id.clone(),
                MailboxSenderErrorKind::Closed,
            )),
            Some(sender) => {
                sender.send(message).map_err(|_| {
                    // Here, the value is returned when the port is
                    // closed.  We should consider having a similar
                    // API for send_once, though arguably it makes less
                    // sense in this context.
                    MailboxSenderError::new_bound(
                        self.port_id.clone(),
                        MailboxSenderErrorKind::Closed,
                    )
                })?;
                Ok(SerializedSendDisposition::DeliveredAndExhausted)
            }
        }
    }
}

// Clone is implemented explicitly because the derive macro demands M:
// Clone directly. In this case, it isn't needed because Arc<T> can
// clone for any T.
impl<M: Message> Clone for OnceSender<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            port_id: self.port_id.clone(),
        }
    }
}

impl<M: RemoteMessage> SerializedSender for OnceSender<M> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn send_serialized(
        &self,
        headers: Flattrs,
        serialized: wirevalue::Any,
    ) -> Result<SerializedSendDisposition, SerializedSendFailure> {
        match serialized.deserialized() {
            Ok(message) => self
                .send_once(message)
                .map_err(|_| SerializedSendFailure::Dead {
                    data: serialized,
                    headers,
                }),
            Err(err) => Err(SerializedSendFailure::Error(SerializedSendError {
                data: serialized,
                error: MailboxSenderError::new_bound(
                    self.port_id.clone(),
                    MailboxSenderErrorKind::Deserialize(M::typename(), err.into()),
                ),
                headers,
            })),
        }
    }
}

/// Use the provided function to send untyped messages (i.e. Any objects).
pub(crate) struct UntypedUnboundedSender {
    pub(crate) sender: Box<
        dyn Fn(Flattrs, wirevalue::Any) -> Result<SerializedSendDisposition, SerializedSendFailure>
            + Send
            + Sync,
    >,
}

impl SerializedSender for UntypedUnboundedSender {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn send_serialized(
        &self,
        headers: Flattrs,
        serialized: wirevalue::Any,
    ) -> Result<SerializedSendDisposition, SerializedSendFailure> {
        (self.sender)(headers, serialized)
    }
}

/// State is the internal state of the mailbox.
struct State {
    /// The ID of the mailbox owner.
    actor_id: ActorAddr,

    // insert if it's serializable; otherwise don't.
    /// The set of active ports in the mailbox. All currently
    /// allocated ports are
    ports: DashMap<u64, Arc<dyn SerializedSender>>,

    /// The next port ID to allocate.
    next_port: AtomicU64,

    /// If a value is present, the mailbox has been closed with the provided
    /// status, and any subsequent `Mailbox::post_unchecked` calls will fail.
    closed: RwLock<Option<ActorStatus>>,

    /// Gate that closes and drains runtime-dispatched handler ingress.
    handler_ingress: Arc<HandlerIngressGate>,
}

impl State {
    /// Create a new state with the provided owning ActorAddr.
    fn new(actor_id: ActorAddr) -> Self {
        Self {
            actor_id,
            ports: DashMap::new(),
            // The first 1024 ports are allocated to actor handlers.
            // Other port IDs are ephemeral.
            next_port: AtomicU64::new(USER_PORT_OFFSET),
            closed: RwLock::new(None),
            handler_ingress: Arc::new(HandlerIngressGate::new()),
        }
    }

    /// Allocate a fresh port.
    fn allocate_port(&self) -> u64 {
        self.next_port.fetch_add(1, Ordering::SeqCst)
    }
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("State")
            .field("actor_id", &self.actor_id)
            .field(
                "open_ports",
                &self.ports.iter().map(|e| *e.key()).collect::<Vec<_>>(),
            )
            .field("next_port", &self.next_port)
            .finish()
    }
}

// TODO: mux based on some parameterized type. (mux key).
/// An in-memory mailbox muxer. This is used to route messages to
/// different underlying senders.
#[derive(Clone)]
pub struct MailboxMuxer {
    mailboxes: Arc<DashMap<ActorId, Box<dyn MailboxSender + Send + Sync>>>,
}

impl Default for MailboxMuxer {
    fn default() -> Self {
        Self::new()
    }
}

impl MailboxMuxer {
    /// Create a new, empty, muxer.
    pub fn new() -> Self {
        Self {
            mailboxes: Arc::new(DashMap::new()),
        }
    }

    /// Route messages destined for the provided actor id to the provided
    /// sender. Returns false if there is already a sender associated
    /// with the actor. In this case, the sender is not replaced, and
    /// the caller must [`MailboxMuxer::unbind`] it first.
    pub fn bind(&self, actor_id: ActorId, sender: impl MailboxSender + 'static) -> bool {
        match self.mailboxes.entry(actor_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(Box::new(sender));
                true
            }
        }
    }

    /// Convenience function to bind a mailbox.
    pub fn bind_mailbox(&self, mailbox: Mailbox) -> bool {
        self.bind(mailbox.actor_addr().id().clone(), mailbox)
    }

    /// Unbind the sender associated with the provided actor ID. After
    /// unbinding, the muxer will no longer be able to send messages to
    /// that actor.
    #[allow(dead_code)]
    pub(crate) fn unbind(&self, actor_id: &ActorId) {
        self.mailboxes.remove(actor_id);
    }
}

#[async_trait]
impl MailboxSender for MailboxMuxer {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let dest_actor_ref = envelope.dest().actor_addr();
        match self.mailboxes.get(dest_actor_ref.id()) {
            None => {
                let err = format!(
                    "no mailbox for actor {} registered in muxer",
                    dest_actor_ref
                );
                let failure = DeliveryFailure::new(InvalidReference::new(
                    dest_actor_ref,
                    InvalidReferenceReason::ActorNotExist,
                ));
                envelope.undeliverable_with_failure(
                    DeliveryError::Unroutable(err),
                    failure,
                    return_handle,
                )
            }
            Some(sender) => sender.post(envelope, return_handle),
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        let keys: Vec<_> = self
            .mailboxes
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for key in keys {
            if let Some(sender) = self.mailboxes.get(&key) {
                sender.value().flush().await?;
            }
        }
        Ok(())
    }
}

/// MailboxRouter routes messages to the sender that is bound to its
/// nearest prefix.
#[derive(Clone)]
pub struct MailboxRouter {
    entries: Arc<RwLock<BTreeMap<Addr, Arc<dyn MailboxSender + Send + Sync>>>>,
}

impl Default for MailboxRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl MailboxRouter {
    /// Create a new, empty router.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Downgrade this router to a [`WeakMailboxRouter`].
    pub fn downgrade(&self) -> WeakMailboxRouter {
        WeakMailboxRouter(Arc::downgrade(&self.entries))
    }

    /// Returns a boxed sender that first attempts to find a route in
    /// this router's table; otherwise posts the message to the provided
    /// fallback sender.
    pub fn fallback(&self, default: BoxedMailboxSender) -> BoxedMailboxSender {
        FallbackMailboxRouter {
            router: self.clone(),
            default,
        }
        .into_boxed()
    }

    /// Bind the provided sender to the given reference. The destination
    /// is treated as a prefix to which messages can be routed, and
    /// messages are routed to their longest matching prefix.
    pub fn bind(&self, dest: impl Into<Addr>, sender: impl MailboxSender + 'static) {
        let dest = dest.into();
        let mut w = self.entries.write().unwrap();
        w.insert(dest, Arc::new(sender));
    }

    /// Remove the binding for the given reference. Only the exact
    /// point is removed; other bindings under the same prefix are
    /// unaffected.
    pub fn unbind(&self, dest: &Addr) {
        let mut w = self.entries.write().unwrap();
        w.remove(dest);
    }

    fn sender(&self, actor_ref: &ActorAddr) -> Option<Arc<dyn MailboxSender + Send + Sync>> {
        let reference = Addr::from(actor_ref.clone());
        match self
            .entries
            .read()
            .unwrap()
            .lower_bound(Excluded(&reference))
            .prev()
        {
            None => None,
            Some((key, sender)) if key.is_prefix_of(&reference) => Some(sender.clone()),
            Some(_) => None,
        }
    }
}

#[async_trait]
impl MailboxSender for MailboxRouter {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let dest_actor_ref = envelope.dest().actor_addr();
        match self.sender(&dest_actor_ref) {
            None => {
                let target = envelope.dest().clone();
                let failure = DeliveryFailure::new(UndeliverableReason::Transport(
                    TransportFailure::new(target, TransportFailureReason::NoRoute),
                ));
                envelope.undeliverable_with_failure(
                    DeliveryError::Unroutable(
                        "no destination found for actor in routing table".to_string(),
                    ),
                    failure,
                    return_handle,
                )
            }
            Some(sender) => sender.post(envelope, return_handle),
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        let senders: Vec<_> = self.entries.read().unwrap().values().cloned().collect();
        let futs: Vec<_> = senders.iter().map(|s| s.flush()).collect();
        futures::future::try_join_all(futs).await?;
        Ok(())
    }
}

/// A router that first checks a [`MailboxRouter`] for a matching
/// prefix route, falling back to a default sender when none is found.
#[derive(Clone)]
pub struct FallbackMailboxRouter {
    router: MailboxRouter,
    default: BoxedMailboxSender,
}

impl FallbackMailboxRouter {
    /// The fallback sender used when the router has no match.
    pub fn default_sender(&self) -> &BoxedMailboxSender {
        &self.default
    }
}

#[async_trait]
impl MailboxSender for FallbackMailboxRouter {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let dest_actor_ref = envelope.dest().actor_addr();
        match self.router.sender(&dest_actor_ref) {
            Some(sender) => sender.post(envelope, return_handle),
            None => self.default.post(envelope, return_handle),
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        let (r1, r2) = futures::future::join(self.router.flush(), self.default.flush()).await;
        r1?;
        r2?;
        Ok(())
    }
}

/// A version of [`MailboxRouter`] that holds a weak reference to the underlying
/// state. This allows router references to be circular: an entity holding a reference
/// to the router may also contain the router itself.
///
/// TODO: this currently holds a weak reference to the entire router. This helps
/// prevent cycle leaks, but can cause excess memory usage as the cycle is at
/// the granularity of each entry. Possibly the router should allow weak references
/// on a per-entry basis.
#[derive(Debug, Clone)]
pub struct WeakMailboxRouter(Weak<RwLock<BTreeMap<Addr, Arc<dyn MailboxSender + Send + Sync>>>>);

impl WeakMailboxRouter {
    /// Upgrade the weak router to a strong reference router.
    pub fn upgrade(&self) -> Option<MailboxRouter> {
        self.0.upgrade().map(|entries| MailboxRouter { entries })
    }
}

#[async_trait]
impl MailboxSender for WeakMailboxRouter {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match self.upgrade() {
            Some(router) => router.post(envelope, return_handle),
            None => {
                let target = envelope.dest().clone();
                let failure =
                    DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                        target,
                        TransportFailureReason::LinkUnavailable(
                            "mailbox router is gone".to_string(),
                        ),
                    )));
                envelope.undeliverable_with_failure(
                    DeliveryError::BrokenLink("failed to upgrade WeakMailboxRouter".to_string()),
                    failure,
                    return_handle,
                )
            }
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        match self.upgrade() {
            Some(router) => router.flush().await,
            None => Ok(()),
        }
    }
}

/// A dynamic mailbox router that supports remote delivery.
///
/// `DialMailboxRouter` maintains a runtime address book mapping
/// references to `ChannelAddr`s. It holds a cache of active
/// connections and forwards messages to the appropriate
/// `MailboxClient`.
///
/// If a message destination is not bound, but is a "direct mode" address
/// (i.e., its proc id contains the channel address through which the proc
/// is reachable), then DialMailboxRouter dials the proc directly.
///
/// Messages sent to unknown destinations are routed to the `default`
/// sender, if present.
#[derive(Clone)]
pub struct DialMailboxRouter {
    address_book: Arc<RwLock<BTreeMap<Addr, ChannelAddr>>>,
    sender_cache: Arc<DashMap<ChannelAddr, Arc<MailboxClient>>>,

    // The default sender, to which messages for unknown recipients
    // are sent. (This is like a default route in a routing table.)
    default: BoxedMailboxSender,

    // When true, only dial direct-addressed procs if their transport
    // type is remote. Otherwise, fall back to the default sender.
    direct_addressed_remote_only: bool,
}

impl Default for DialMailboxRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl DialMailboxRouter {
    /// Create a new [`DialMailboxRouter`] with an empty routing table.
    pub fn new() -> Self {
        Self::new_with_default(BoxedMailboxSender::new(UnroutableMailboxSender))
    }

    /// Create a new [`DialMailboxRouter`] with an empty routing table,
    /// and a default sender. Any message with an unknown destination is
    /// dispatched on this default sender, unless the destination is
    /// direct-addressed, in which case it is dialed directly.
    pub fn new_with_default(default: BoxedMailboxSender) -> Self {
        Self {
            address_book: Arc::new(RwLock::new(BTreeMap::new())),
            sender_cache: Arc::new(DashMap::new()),
            default,
            direct_addressed_remote_only: false,
        }
    }

    /// Create a new [`DialMailboxRouter`] with an empty routing table,
    /// and a default sender. Any message with an unknown destination is
    /// dispatched on this default sender, unless the destination is
    /// direct-addressed *and* has a remote channel transport type.
    pub fn new_with_default_direct_addressed_remote_only(default: BoxedMailboxSender) -> Self {
        Self {
            address_book: Arc::new(RwLock::new(BTreeMap::new())),
            sender_cache: Arc::new(DashMap::new()),
            default,
            direct_addressed_remote_only: true,
        }
    }

    /// Binds a [`Addr`] to a [`ChannelAddr`], replacing any
    /// existing binding.
    ///
    /// If the address changes, the old sender is evicted from the
    /// cache to ensure fresh routing on next use.
    pub fn bind(&self, dest: impl Into<Addr>, addr: ChannelAddr) {
        let dest = dest.into();
        if let Ok(mut w) = self.address_book.write() {
            if let Some(old_addr) = w.insert(dest.clone(), addr.clone())
                && old_addr != addr
            {
                tracing::info!("rebinding {:?} from {:?} to {:?}", dest, old_addr, addr);
                self.sender_cache.remove(&old_addr);
            }
        } else {
            tracing::error!("address book poisoned during bind of {:?}", dest);
        }
    }

    /// Removes all address mappings with the given prefix from the
    /// router.
    ///
    /// Also evicts any corresponding cached senders to prevent reuse
    /// of stale connections.
    pub fn unbind(&self, dest: &Addr) {
        if let Ok(mut w) = self.address_book.write() {
            let to_remove: Vec<(Addr, ChannelAddr)> = w
                .range(dest..)
                .take_while(|(key, _)| dest.is_prefix_of(key))
                .map(|(key, addr)| (key.clone(), addr.clone()))
                .collect();

            for (key, addr) in to_remove {
                tracing::info!("unbinding {:?} from {:?}", key, addr);
                w.remove(&key);
                self.sender_cache.remove(&addr);
            }
        } else {
            tracing::error!("address book poisoned during unbind of {:?}", dest);
        }
    }

    /// Lookup an actor's channel in the router's address bok.
    pub fn lookup_addr(&self, actor_ref: &ActorAddr) -> Option<ChannelAddr> {
        let address_book = self.address_book.read().unwrap();
        let reference = Addr::from(actor_ref.clone());
        let found = address_book.lower_bound(Excluded(&reference)).prev();

        // First try to look up the address in our address book; failing that,
        // extract the address from the ProcAddr (all procs are direct-addressed now).
        if let Some((key, addr)) = found
            && key.is_prefix_of(&reference)
        {
            Some(addr.clone())
        } else {
            let addr = actor_ref.addr().clone();
            if self.direct_addressed_remote_only {
                addr.transport().is_remote().then_some(addr)
            } else {
                Some(addr)
            }
        }
    }

    /// Return all covering prefixes of this router. That is, all references that are not
    /// prefixed by another reference in the routing table
    pub fn prefixes(&self) -> BTreeSet<Addr> {
        let addrs = self.address_book.read().unwrap();
        let mut prefixes: BTreeSet<Addr> = BTreeSet::new();
        for (reference, _) in addrs.iter() {
            match prefixes.lower_bound(Excluded(reference)).peek_prev() {
                Some(candidate) if candidate.is_prefix_of(reference) => (),
                _ => {
                    prefixes.insert(reference.clone());
                }
            }
        }

        prefixes
    }

    fn dial(
        &self,
        addr: &ChannelAddr,
        actor_ref: &ActorAddr,
    ) -> Result<Arc<MailboxClient>, MailboxSenderError> {
        // Get the sender. Create it if needed. Do not send the
        // messages inside this block so we do not hold onto the
        // reference of the dashmap entries.
        match self.sender_cache.entry(addr.clone()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let tx = channel::dial(addr.clone()).map_err(|err| {
                    MailboxSenderError::new_unbound_type(
                        actor_ref.clone(),
                        MailboxSenderErrorKind::Channel(err),
                        "unknown",
                    )
                })?;
                let sender = MailboxClient::new(tx);
                Ok(entry.insert(Arc::new(sender)).value().clone())
            }
        }
    }
}

#[async_trait]
impl MailboxSender for DialMailboxRouter {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let dest_actor_ref = envelope.dest().actor_addr();
        let Some(addr) = self.lookup_addr(&dest_actor_ref) else {
            self.default.post(envelope, return_handle);
            return;
        };

        match self.dial(&addr, &dest_actor_ref) {
            Err(err) => {
                let target = envelope.dest().clone();
                let failure =
                    DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                        target,
                        TransportFailureReason::DialFailed {
                            addr,
                            error: err.to_string(),
                        },
                    )));
                envelope.undeliverable_with_failure(
                    DeliveryError::Unroutable(format!("cannot dial destination: {err}")),
                    failure,
                    return_handle,
                )
            }
            Ok(sender) => sender.post(envelope, return_handle),
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        let senders: Vec<_> = self
            .sender_cache
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let mut futs: Vec<_> = senders.iter().map(|s| s.flush()).collect();
        futs.push(self.default.flush());
        futures::future::try_join_all(futs).await?;
        Ok(())
    }
}

/// A MailboxSender that reports any envelope as undeliverable due to
/// routing failure.
#[derive(Debug)]
pub struct UnroutableMailboxSender;

#[async_trait]
impl MailboxSender for UnroutableMailboxSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let target = envelope.dest().clone();
        let failure = DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
            target,
            TransportFailureReason::NoRoute,
        )));
        envelope.undeliverable_with_failure(
            DeliveryError::Unroutable("destination not found in routing table".to_string()),
            failure,
            return_handle,
        );
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches;
    use std::mem::drop;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use timed_test::async_timed_test;

    use super::*;
    use crate::Actor;
    use crate::ActorHandle;
    use crate::Instance;
    use crate::accum;
    use crate::accum::ReducerMode;
    use crate::channel::ChannelTransport;
    use crate::context::Mailbox as MailboxContext;
    use crate::endpoint::Endpoint as _;
    use crate::proc::Proc;
    use crate::testing::ids::test_actor_id;
    use crate::testing::ids::test_port_id;
    use crate::testing::ids::test_proc_id;

    fn test_proc_ref(name: &str) -> Addr {
        Addr::Proc(test_proc_id(name))
    }

    fn test_actor_ref(proc_name: &str, actor_name: &str) -> Addr {
        Addr::Actor(test_actor_id(proc_name, actor_name))
    }

    fn root_transport_failure(envelope: &MessageEnvelope) -> &TransportFailure {
        let root_failure = envelope
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::Undeliverable(UndeliverableReason::Transport(transport)) =
            &root_failure.kind
        else {
            panic!("expected transport failure, got {root_failure}");
        };
        transport
    }

    fn root_invalid_reference(envelope: &MessageEnvelope) -> &InvalidReference {
        let root_failure = envelope
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::InvalidReference(invalid_reference) = &root_failure.kind else {
            panic!("expected invalid reference, got {root_failure}");
        };
        invalid_reference
    }

    #[test]
    fn test_error() {
        use crate::testing::ids::test_actor_id;
        let err = MailboxError::new(
            test_actor_id("myworld_2", "myactor"),
            MailboxErrorKind::Closed,
        );
        // ActorAddr display is now "actor_uid.proc_uid@location"
        let err_str = format!("{err}");
        assert!(
            err_str.contains("mailbox closed"),
            "expected error: {}",
            err_str
        );
        assert!(
            err_str.contains("@"),
            "expected ref-style location separator in {err_str}"
        );
    }

    #[tokio::test]
    async fn test_mailbox_basic() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (port, mut receiver) = mbox.open_port::<u64>();
        let port = port.bind();

        mbox.serialize_and_send(&port, 123, monitored_return_handle())
            .unwrap();
        mbox.serialize_and_send(&port, 321, monitored_return_handle())
            .unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 123u64);
        assert_eq!(receiver.recv().await.unwrap(), 321u64);

        let serialized = wirevalue::Any::serialize(&999u64).unwrap();
        mbox.post(
            MessageEnvelope::new_unknown(port.port_addr().clone(), serialized),
            monitored_return_handle(),
        );
        assert_eq!(receiver.recv().await.unwrap(), 999u64);
    }

    #[tokio::test]
    async fn test_mailbox_rejects_messages_for_other_actors() {
        let mbox = Mailbox::new(test_actor_id("0", "owner"));
        let dest = test_actor_id("0", "other").port_addr(Port::from(1234));
        let envelope =
            MessageEnvelope::serialize(mbox.actor_addr().clone(), dest, &42u64, Flattrs::new())
                .expect("serialize");
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        mbox.post(envelope, return_handle);

        let Undeliverable::Message(undelivered) =
            tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
                .await
                .expect("timed out waiting for undeliverable")
                .expect("return port closed")
        else {
            panic!("expected returned message");
        };
        assert!(
            undelivered
                .error_msg()
                .expect("expected error")
                .contains("cannot deliver to")
        );
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::InvalidReference(invalid_reference) = &root_failure.kind else {
            panic!("expected invalid reference, got {root_failure}");
        };
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::WrongMailboxOwner
        );
    }

    #[tokio::test]
    async fn test_ttl_expiration_records_root_delivery_failure() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (port, _) = mbox.open_port::<u64>();
        let port_ref = port.bind();
        let envelope = MessageEnvelope::serialize(
            mbox.actor_addr().clone(),
            port_ref.port_addr().clone(),
            &42u64,
            Flattrs::new(),
        )
        .expect("serialize")
        .set_ttl(0);
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        mbox.post(envelope, return_handle);

        let Undeliverable(undelivered) =
            tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
                .await
                .expect("timed out waiting for undeliverable")
                .expect("return port closed");
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        assert!(
            matches!(root_failure.kind, DeliveryFailureKind::Expired(_)),
            "expected expired delivery failure, got {root_failure}"
        );
    }

    #[tokio::test]
    async fn test_missing_handler_port_records_invalid_reference() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let dest = mbox.actor_addr().port_addr(Port::handler::<TestMessage>());
        let envelope = MessageEnvelope::serialize(
            mbox.actor_addr().clone(),
            dest,
            &TestMessage,
            Flattrs::new(),
        )
        .expect("serialize");
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        mbox.post(envelope, return_handle);

        let undelivered = tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("return port closed")
            .into_message()
            .expect("expected returned envelope");
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::InvalidReference(invalid_reference) = &root_failure.kind else {
            panic!("expected invalid reference, got {root_failure}");
        };
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::HandlerNotBound
        );
    }

    #[tokio::test]
    async fn test_missing_dropped_port_records_recipient_gone() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (port, receiver) = mbox.open_port::<u64>();
        let port_ref = port.bind();
        drop(receiver);
        let envelope = MessageEnvelope::serialize(
            mbox.actor_addr().clone(),
            port_ref.port_addr().clone(),
            &42u64,
            Flattrs::new(),
        )
        .expect("serialize");
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        mbox.post(envelope, return_handle);

        let undelivered = tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("return port closed")
            .into_message()
            .expect("expected returned envelope");
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::Undeliverable(UndeliverableReason::PortGone(port_gone)) =
            &root_failure.kind
        else {
            panic!("expected port gone, got {root_failure}");
        };
        assert_eq!(port_gone.port, *port_ref.port_addr());
    }

    #[tokio::test]
    async fn test_missing_never_allocated_port_records_invalid_reference() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let dest = mbox.actor_addr().port_addr(Port::from(USER_PORT_OFFSET));
        let envelope =
            MessageEnvelope::serialize(mbox.actor_addr().clone(), dest, &42u64, Flattrs::new())
                .expect("serialize");
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        mbox.post(envelope, return_handle);

        let undelivered = tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("return port closed")
            .into_message()
            .expect("expected returned envelope");
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::InvalidReference(invalid_reference) = &root_failure.kind else {
            panic!("expected invalid reference, got {root_failure}");
        };
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::PortNeverAllocated
        );
    }

    #[tokio::test]
    async fn test_mailbox_accum() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (port, mut receiver) = client
            .mailbox()
            .open_accum_port(accum::join_semilattice::<accum::Max<i64>>());

        for i in -3..4 {
            port.post(&client, accum::Max(i));
            let received: accum::Max<i64> = receiver.recv().await.unwrap();
            let msg = received.get();
            assert_eq!(msg, &i);
        }
        // Send a smaller or same value. Should still receive the previous max.
        for i in -3..4 {
            port.post(&client, accum::Max(i));
            assert_eq!(receiver.recv().await.unwrap().get(), &3);
        }
        // send a larger value. Should receive the new max.
        port.post(&client, accum::Max(4));
        assert_eq!(receiver.recv().await.unwrap().get(), &4);

        // Send multiple updates. Should only receive the final change.
        for i in 5..10 {
            port.post(&client, accum::Max(i));
        }
        assert_eq!(receiver.recv().await.unwrap().get(), &9);
        port.post(&client, accum::Max(1));
        port.post(&client, accum::Max(3));
        port.post(&client, accum::Max(2));
        assert_eq!(receiver.recv().await.unwrap().get(), &9);
    }

    #[test]
    fn test_port_and_reducer() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        // accum port could have reducer typehash
        {
            let accumulator = accum::join_semilattice::<accum::Max<u64>>();
            let reducer_spec = accumulator.reducer_spec().unwrap();
            let (port, _) = mbox.open_accum_port(accum::join_semilattice::<accum::Max<u64>>());
            assert_eq!(port.inner.reducer_spec, Some(reducer_spec.clone()));
            let port_ref = port.bind();
            assert_eq!(port_ref.reducer_spec(), &Some(reducer_spec));
        }
        // normal port should not have reducer typehash
        {
            let (port, _) = mbox.open_port::<u64>();
            assert_eq!(port.inner.reducer_spec, None);
            let port_ref = port.bind();
            assert_eq!(port_ref.reducer_spec(), &None);
        }
    }

    #[tokio::test]
    #[ignore] // error behavior changed, but we will bring it back
    async fn test_mailbox_once() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();

        let (port, receiver) = client.open_once_port::<u64>();

        // let port_id = port.port_addr().clone();

        port.post(&client, 123u64);
        assert_eq!(receiver.recv().await.unwrap(), 123u64);

        // // The borrow checker won't let us send again on the port
        // // (good!), but we stashed the port-id and so we can try on the
        // // serialized interface.
        // let Err(err) = mbox
        //     .send_serialized(&port_id, &wirevalue::Any(Vec::new()))
        //     .await
        // else {
        //     unreachable!()
        // };
        // assert_matches!(err.kind(), MailboxSenderErrorKind::Closed);
    }

    #[tokio::test]
    #[ignore] // changed error behavior
    async fn test_mailbox_receiver_drop() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (port, mut receiver) = mbox.open_port::<u64>();
        // Make sure we go through "remote" path.
        let port = port.bind();
        mbox.serialize_and_send(&port, 123u64, monitored_return_handle())
            .unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 123u64);
        drop(receiver);
        let Err(err) = mbox.serialize_and_send(&port, 123u64, monitored_return_handle()) else {
            panic!();
        };

        assert_matches!(err.kind(), MailboxSenderErrorKind::Closed);
        assert_matches!(err.location(), PortLocation::Bound(bound) if *bound == *port.port_addr());
    }

    #[tokio::test]
    async fn test_mailbox_type_mismatch_does_not_evict_unbounded_port() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (port, mut receiver) = mbox.open_port::<u64>();
        let port = port.bind();
        let port_index = port.port_addr().index();
        let target: Addr = port.port_addr().clone().into();
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();

        let wrong_message = wirevalue::Any::serialize(&TestMessage).unwrap();
        mbox.post(
            MessageEnvelope::new_unknown(port.port_addr().clone(), wrong_message),
            return_handle.clone(),
        );

        let envelope = tokio::time::timeout(Duration::from_secs(1), return_receiver.recv())
            .await
            .expect("undeliverable mismatch should arrive")
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        assert!(
            envelope
                .error_msg()
                .is_some_and(|message| message.contains("deserialization error")),
            "expected deserialization error in {envelope}",
        );
        let invalid_reference = root_invalid_reference(&envelope);
        assert_eq!(invalid_reference.target, target);
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::ProtocolMismatch
        );
        assert!(
            mbox.inner.ports.contains_key(&port_index),
            "deserialization mismatch should not evict reusable port",
        );

        mbox.serialize_and_send(&port, 123u64, return_handle)
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("valid message should still be delivered")
                .unwrap(),
            123u64
        );
    }

    #[tokio::test]
    async fn test_mailbox_closed_unbounded_port_is_removed_after_send_failure() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let port_index = mbox.allocate_port();
        let port_id = mbox.actor_addr().port_addr(Port::from(port_index));
        let port = crate::PortRef::attest(port_id.clone());
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();
        let (sender, receiver) = mpsc::unbounded_channel::<u64>();

        drop(receiver);

        mbox.inner.ports.insert(
            port_index,
            Arc::new(UnboundedSender::new(
                UnboundedPortSender::Mpsc(sender),
                port_id,
            )),
        );

        mbox.serialize_and_send(&port, 123u64, return_handle.clone())
            .unwrap();

        let envelope = tokio::time::timeout(Duration::from_secs(1), return_receiver.recv())
            .await
            .expect("closed port should produce undeliverable")
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        let first_error = envelope.error_msg().expect("expected delivery error");
        assert!(
            first_error.contains("port not bound in mailbox"),
            "expected unbound-port error in {envelope}",
        );
        assert!(
            !mbox.inner.ports.contains_key(&port_index),
            "dead reusable port should be removed after send failure",
        );

        mbox.serialize_and_send(&port, 456u64, return_handle)
            .unwrap();
        let envelope = tokio::time::timeout(Duration::from_secs(1), return_receiver.recv())
            .await
            .expect("removed port should produce unbound undeliverable")
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        let second_error = envelope.error_msg().expect("expected delivery error");
        assert_eq!(
            first_error, second_error,
            "dead-port undeliverable should match unbound-port undeliverable exactly",
        );
    }

    #[tokio::test]
    async fn test_mailbox_once_type_mismatch_preserves_sender_until_delivery() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (port, receiver) = mbox.open_once_port::<u64>();
        let port = port.bind();
        let port_index = port.port_addr().index();
        let target: Addr = port.port_addr().clone().into();
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();

        let wrong_message = wirevalue::Any::serialize(&TestMessage).unwrap();
        mbox.post(
            MessageEnvelope::new_unknown(port.port_addr().clone(), wrong_message),
            return_handle.clone(),
        );

        let envelope = tokio::time::timeout(Duration::from_secs(1), return_receiver.recv())
            .await
            .expect("once-port mismatch should arrive")
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        assert!(
            envelope
                .error_msg()
                .is_some_and(|message| message.contains("deserialization error")),
            "expected deserialization error in {envelope}",
        );
        let invalid_reference = root_invalid_reference(&envelope);
        assert_eq!(invalid_reference.target, target);
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::ProtocolMismatch
        );
        assert!(
            mbox.inner.ports.contains_key(&port_index),
            "once port should survive deserialization mismatch before delivery",
        );

        mbox.serialize_and_send_once(port, 123u64, return_handle)
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("valid once message should still be delivered")
                .unwrap(),
            123u64
        );
        assert!(
            !mbox.inner.ports.contains_key(&port_index),
            "successful once send should remove the sender entry",
        );
    }

    #[tokio::test]
    async fn test_drain() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));

        let (port, mut receiver) = mbox.open_port();
        let port = port.bind();

        for i in 0..10 {
            mbox.serialize_and_send(&port, i, monitored_return_handle())
                .unwrap();
        }

        for i in 0..10 {
            assert_eq!(receiver.recv().await.unwrap(), i);
        }

        assert!(receiver.drain().is_empty());
    }

    #[tokio::test]
    async fn test_mailbox_muxer() {
        let muxer = MailboxMuxer::new();

        let mbox0 = Mailbox::new(test_actor_id("0", "actor1"));
        let mbox1 = Mailbox::new(test_actor_id("0", "actor2"));

        muxer.bind(mbox0.actor_addr().id().clone(), mbox0.clone());
        muxer.bind(mbox1.actor_addr().id().clone(), mbox1.clone());

        let (port, receiver) = mbox0.open_once_port::<u64>();

        let muxer_sender = muxer.clone();
        let proc = Proc::configured(test_proc_id("0"), BoxedMailboxSender::new(muxer));
        let (client, _) = proc.client("client").unwrap();

        port.post(&client, 123u64);
        assert_eq!(receiver.recv().await.unwrap(), 123u64);

        let missing_actor = test_actor_id("0", "missing_actor");
        let missing_dest = missing_actor.port_addr(Port::from(1234));
        let envelope = MessageEnvelope::serialize(
            client.self_addr().clone(),
            missing_dest,
            &456u64,
            Flattrs::new(),
        )
        .expect("serialize");
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        muxer_sender.post(envelope, return_handle);

        let undelivered = tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("return port closed")
            .into_message()
            .expect("expected returned envelope");
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::InvalidReference(invalid_reference) = &root_failure.kind else {
            panic!("expected invalid reference, got {root_failure}");
        };
        assert_eq!(invalid_reference.target, Addr::Actor(missing_actor));
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::ActorNotExist
        );

        /*
        let (tx, rx) = channel::local::new::<u64>();
        let (port, _) = mbox0.open_port::<u64>();
        let handle = muxer.clone().serve_port(port, rx).unwrap();
        muxer.unbind(mbox0.actor_addr());
        tx.send(123u64).await.unwrap();
        let Ok(Err(err)) = handle.await else { panic!() };
        assert_eq!(err.actor_addr(), &actor_id(0));
        */
    }

    #[tokio::test]
    async fn test_local_client_server() {
        let mbox = Mailbox::new(test_actor_id("0", "actor0"));
        let (tx, rx) = channel::local::new();
        let serve_handle = mbox.clone().serve(rx);
        let client = MailboxClient::new(tx);

        let (port, receiver) = mbox.open_once_port::<u64>();
        let port = port.bind();

        client
            .serialize_and_send_once(port, 123u64, monitored_return_handle())
            .unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 123u64);
        serve_handle.stop("fromt test");
        serve_handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_mailbox_client_records_channel_closed_failure() {
        let mbox = Mailbox::new(test_actor_id("0", "actor0"));
        let (tx, rx) = channel::local::new();
        drop(rx);
        let client = MailboxClient::new(tx);
        let addr = client.addr.clone();

        let (port, _receiver) = mbox.open_once_port::<u64>();
        let port = port.bind();
        let target: Addr = port.port_addr().clone().into();
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();

        client
            .serialize_and_send_once(port, 123u64, return_handle)
            .unwrap();

        let undelivered = tokio::time::timeout(Duration::from_secs(1), return_receiver.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("return port closed")
            .into_message()
            .expect("expected returned envelope");
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::Undeliverable(UndeliverableReason::Transport(transport)) =
            &root_failure.kind
        else {
            panic!("expected transport failure, got {root_failure}");
        };
        assert_eq!(transport.target, target);
        assert_eq!(
            transport.reason,
            TransportFailureReason::ChannelClosed { addr }
        );
    }

    #[tokio::test]
    async fn test_mailbox_router() {
        let mbox0 = Mailbox::new(test_actor_id("world0_0", "actor0"));
        let mbox1 = Mailbox::new(test_actor_id("world1_0", "actor0"));
        let mbox2 = Mailbox::new(test_actor_id("world1_1", "actor0"));
        let mbox3 = Mailbox::new(test_actor_id("world1_1", "actor1"));

        let comms: Vec<(OncePortRef<u64>, OncePortReceiver<u64>)> =
            [&mbox0, &mbox1, &mbox2, &mbox3]
                .into_iter()
                .map(|mbox| {
                    let (port, receiver) = mbox.open_once_port::<u64>();
                    (port.bind(), receiver)
                })
                .collect();

        let router = MailboxRouter::new();

        router.bind(test_proc_ref("world0_0"), mbox0);
        router.bind(test_proc_ref("world1_0"), mbox1);
        router.bind(test_proc_ref("world1_1"), mbox2);
        router.bind(test_actor_ref("world1_1", "actor1"), mbox3);

        for (i, (port, receiver)) in comms.into_iter().enumerate() {
            router
                .serialize_and_send_once(port, i as u64, monitored_return_handle())
                .unwrap();
            assert_eq!(receiver.recv().await.unwrap(), i as u64);
        }

        // Test undeliverable messages, and that it is delivered with the appropriate fallback.

        let mbox4 = Mailbox::new(test_actor_id("fallback_0", "actor"));

        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();
        let (port, _receiver) = mbox4.open_once_port();
        let port = port.bind();
        let target: Addr = port.port_addr().clone().into();
        router
            .serialize_and_send_once(port, 0, return_handle.clone())
            .unwrap();
        let undelivered = return_receiver
            .recv()
            .await
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        let transport = root_transport_failure(&undelivered);
        assert_eq!(transport.target, target);
        assert_eq!(transport.reason, TransportFailureReason::NoRoute);

        let router = router.fallback(mbox4.clone().into_boxed());
        let (port, receiver) = mbox4.open_once_port();
        router
            .serialize_and_send_once(port.bind(), 0, return_handle)
            .unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_weak_mailbox_router_records_link_unavailable_failure() {
        let router = MailboxRouter::new();
        let weak_router = router.downgrade();
        drop(router);

        let mbox = Mailbox::new(test_actor_id("0", "actor0"));
        let (port, _receiver) = mbox.open_once_port::<u64>();
        let port = port.bind();
        let target: Addr = port.port_addr().clone().into();
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();

        weak_router
            .serialize_and_send_once(port, 123u64, return_handle)
            .unwrap();

        let undelivered = return_receiver
            .recv()
            .await
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        let transport = root_transport_failure(&undelivered);
        assert_eq!(transport.target, target);
        assert_eq!(
            transport.reason,
            TransportFailureReason::LinkUnavailable("mailbox router is gone".to_string())
        );
    }

    #[tokio::test]
    async fn test_unroutable_mailbox_sender_records_no_route_failure() {
        let mbox = Mailbox::new(test_actor_id("0", "actor0"));
        let (port, _receiver) = mbox.open_once_port::<u64>();
        let port = port.bind();
        let target: Addr = port.port_addr().clone().into();
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();

        UnroutableMailboxSender
            .serialize_and_send_once(port, 123u64, return_handle)
            .unwrap();

        let undelivered = return_receiver
            .recv()
            .await
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        let transport = root_transport_failure(&undelivered);
        assert_eq!(transport.target, target);
        assert_eq!(transport.reason, TransportFailureReason::NoRoute);
    }

    #[tokio::test]
    async fn test_dial_mailbox_router() {
        let router = DialMailboxRouter::new();

        router.bind(test_proc_ref("world0_0"), "unix!@1".parse().unwrap());
        router.bind(test_proc_ref("world1_0"), "unix!@2".parse().unwrap());
        router.bind(test_proc_ref("world1_1"), "unix!@3".parse().unwrap());
        router.bind(
            test_actor_ref("world1_1", "actor1"),
            "unix!@4".parse().unwrap(),
        );
        // Bind a direct address -- we should use its bound address!
        // The actor must be on unix:@4 so that after unbinding, the prefix
        // route for world1_1 (unix!@3) is the fallback, not world1_1/actor1 (unix!@4).
        let direct_actor_ref: ActorAddr =
            ProcAddr::singleton("unix:@4".parse().unwrap(), "my_proc").actor_addr("my_actor");
        router.bind(
            Addr::Actor(direct_actor_ref.clone()),
            "unix:@5".parse().unwrap(),
        );

        // We should be able to lookup the ids
        router
            .lookup_addr(&test_actor_id("world0_0", "actor"))
            .unwrap();
        router
            .lookup_addr(&test_actor_id("world1_0", "actor"))
            .unwrap();

        let actor_id = direct_actor_ref;
        assert_eq!(
            router.lookup_addr(&actor_id).unwrap(),
            "unix!@5".parse().unwrap(),
        );
        router.unbind(&actor_id.clone().into());
        assert_eq!(
            router.lookup_addr(&actor_id).unwrap(),
            "unix!@4".parse().unwrap(),
        );

        // Unbind procs so lookups fall back to the proc's direct address
        // (all procs are direct-addressed now, so lookup_addr always returns
        // Some; we verify the bound address is gone by checking the returned
        // address is the local fallback, not the originally bound one).
        let fallback = ChannelAddr::any(ChannelTransport::Local);
        router.unbind(&test_proc_ref("world1_0"));
        router.unbind(&test_proc_ref("world1_1"));
        assert_eq!(
            router
                .lookup_addr(&test_actor_id("world1_0", "actor1"))
                .unwrap(),
            fallback,
        );
        assert_eq!(
            router
                .lookup_addr(&test_actor_id("world1_1", "actor1"))
                .unwrap(),
            fallback,
        );
        router
            .lookup_addr(&test_actor_id("world0_0", "actor"))
            .unwrap();
        router.unbind(&test_proc_ref("world0_0"));
        assert_eq!(
            router
                .lookup_addr(&test_actor_id("world0_0", "actor"))
                .unwrap(),
            fallback,
        );
    }

    #[tokio::test]
    async fn test_dial_mailbox_router_records_dial_failure() {
        let router = DialMailboxRouter::new();
        let addr = ChannelAddr::Local(9_876_543_210);
        let mbox = Mailbox::new(test_actor_id("world0_0", "actor0"));
        router.bind(test_proc_ref("world0_0"), addr.clone());

        let (port, _receiver) = mbox.open_once_port::<u64>();
        let port = port.bind();
        let target: Addr = port.port_addr().clone().into();
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();

        router
            .serialize_and_send_once(port, 123u64, return_handle)
            .unwrap();

        let undelivered = return_receiver
            .recv()
            .await
            .unwrap()
            .into_message()
            .expect("expected returned envelope");
        let transport = root_transport_failure(&undelivered);
        assert_eq!(transport.target, target);
        let TransportFailureReason::DialFailed {
            addr: failure_addr,
            error,
        } = &transport.reason
        else {
            panic!("expected dial failure, got {}", transport.reason);
        };
        assert_eq!(failure_addr, &addr);
        assert!(
            error.contains("channel closed"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    #[ignore] // TODO: there's a leak here, fix it
    async fn test_dial_mailbox_router_default() {
        let mbox0 = Mailbox::new(test_actor_id("world0_0", "actor0"));
        let mbox1 = Mailbox::new(test_actor_id("world1_0", "actor0"));
        let mbox2 = Mailbox::new(test_actor_id("world1_1", "actor0"));
        let mbox3 = Mailbox::new(test_actor_id("world1_1", "actor1"));

        // We don't need to dial here, since we gain direct access to the
        // underlying routers.
        let root = MailboxRouter::new();
        let world0_router = DialMailboxRouter::new_with_default(root.boxed());
        let world1_router = DialMailboxRouter::new_with_default(root.boxed());

        root.bind(test_proc_ref("world0"), world0_router.clone());
        root.bind(test_proc_ref("world1"), world1_router.clone());

        let mailboxes = [&mbox0, &mbox1, &mbox2, &mbox3];

        let mut handles = Vec::new(); // hold on to handles, or channels get closed
        for mbox in mailboxes.iter() {
            let (addr, rx) = channel::serve(ChannelAddr::any(ChannelTransport::Local)).unwrap();
            let handle = (*mbox).clone().serve(rx);
            handles.push(handle);

            eprintln!("{}: {}", mbox.actor_addr(), addr);
            if mbox
                .actor_addr()
                .proc_addr()
                .label()
                .is_some_and(|l| l.as_str().starts_with("world0"))
            {
                world0_router.bind(Addr::from(mbox.actor_addr().clone()), addr);
            } else {
                world1_router.bind(Addr::from(mbox.actor_addr().clone()), addr);
            }
        }

        // Make sure nodes are fully connected.
        for router in [root.boxed(), world0_router.boxed(), world1_router.boxed()] {
            for mbox in mailboxes.iter() {
                let (port, receiver) = mbox.open_once_port::<u64>();
                let port = port.bind();
                router
                    .serialize_and_send_once(port, 123u64, monitored_return_handle())
                    .unwrap();
                assert_eq!(receiver.recv().await.unwrap(), 123u64);
            }
        }
    }

    #[tokio::test]
    async fn test_enqueue_port() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();

        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        let port = client.mailbox().open_enqueue_port(move |_, n| {
            count_clone.fetch_add(n, Ordering::SeqCst);
            Ok(())
        });

        port.post(&client, 10);
        port.post(&client, 5);
        port.post(&client, 1);
        port.post(&client, 0);

        assert_eq!(count.load(Ordering::SeqCst), 16);
    }

    #[derive(Clone, Debug, Serialize, Deserialize, typeuri::Named)]
    struct TestMessage;

    #[derive(Clone, Debug, Serialize, Deserialize, typeuri::Named)]
    #[named(name = "some::custom::path")]
    struct TestMessage2;

    #[test]
    fn test_remote_message_macros() {
        assert_eq!(
            TestMessage::typename(),
            "hyperactor::mailbox::tests::TestMessage"
        );
        assert_eq!(TestMessage2::typename(), "some::custom::path");
    }

    #[test]
    fn test_message_envelope_display() {
        #[derive(typeuri::Named, Serialize, Deserialize)]
        struct MyTest {
            a: u64,
            b: String,
        }
        wirevalue::register_type!(MyTest);

        let envelope = MessageEnvelope::serialize(
            test_actor_id("source_0", "actor"),
            test_port_id("dest_1", "actor", 123),
            &MyTest {
                a: 123,
                b: "hello".into(),
            },
            Flattrs::new(),
        )
        .unwrap();

        // Note: display format changed from "source[0].actor" to direct format
        assert!(format!("{}", envelope).contains("MyTest{\"a\":123,\"b\":\"hello\"}"));
    }

    #[derive(Debug, Default)]
    struct Foo;

    impl Actor for Foo {}

    // Test that a message delivery failure causes the sending actor
    // to stop running.
    #[tokio::test]
    async fn test_actor_delivery_failure() {
        // This test involves making an actor fail and so we must set
        // a supervision coordinator.
        use crate::actor::ActorStatus;
        use crate::testing::proc_supervison::ProcSupervisionCoordinator;

        let proc_forwarder = BoxedMailboxSender::new(DialMailboxRouter::new_with_default(
            BoxedMailboxSender::new(PanickingMailboxSender),
        ));
        let proc_id = test_proc_id("quux_0");
        let mut proc = Proc::configured(proc_id.clone(), proc_forwarder);
        let (_reported, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();
        let (client, _) = proc.client("client").unwrap();

        let foo = proc.spawn("foo", Foo).unwrap();
        let return_handle = foo.port::<Undeliverable<MessageEnvelope>>();
        let message = MessageEnvelope::new(
            foo.actor_addr().clone(),
            test_port_id("corge_0", "bar", 9999),
            wirevalue::Any::serialize(&1u64).unwrap(),
            Flattrs::new(),
        );
        return_handle.post(&client, Undeliverable::Message(message));

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let foo_status = foo.status();
        assert!(matches!(*foo_status.borrow(), ActorStatus::Failed(_)));
        let ActorStatus::Failed(ref msg) = *foo_status.borrow() else {
            unreachable!()
        };
        let msg_str = msg.to_string();
        // UE-3 (top-line shape): with no operation context, the top
        // line names the destination — `undeliverable message to
        // {dest}`. The retired neutral `undeliverable message error`
        // wording is no longer emitted.
        assert!(
            msg_str.contains("undeliverable message to"),
            "expected destination-named top line, got:\n{msg_str}"
        );
        assert!(
            !msg_str.contains("undeliverable message error"),
            "retired neutral fallback must not appear, got:\n{msg_str}"
        );
        assert!(msg_str.contains("sender:") && msg_str.contains("quux_0"));
        assert!(msg_str.contains("dest:") && msg_str.contains("corge_0"));

        proc.destroy_and_wait(tokio::time::Duration::from_secs(1), "test cleanup")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_detached_return_handle() {
        let (return_handle, mut return_receiver) =
            crate::mailbox::undeliverable::new_undeliverable_port();
        // Simulate an undelivered message return.
        let envelope = MessageEnvelope::new(
            test_actor_id("foo_0", "bar"),
            test_port_id("baz_0", "corge", 9999),
            wirevalue::Any::serialize(&1u64).unwrap(),
            Flattrs::new(),
        );
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        return_handle.post(&client, Undeliverable::Message(envelope.clone()));
        // Check we receive the undelivered message.
        assert!(
            tokio::time::timeout(tokio::time::Duration::from_secs(1), return_receiver.recv())
                .await
                .is_ok()
        );
        // Setup a monitor for the receiver and show that if there are
        // no outstanding return handles it terminates.
        let monitor_handle = tokio::spawn(async move {
            while let Ok(Undeliverable::Message(mut envelope)) = return_receiver.recv().await {
                envelope.set_error(DeliveryError::BrokenLink(
                    "returned in unit test".to_string(),
                ));
                UndeliverableMailboxSender
                    .post(envelope, /*unused */ monitored_return_handle());
            }
        });
        drop(return_handle);
        assert!(
            tokio::time::timeout(tokio::time::Duration::from_secs(1), monitor_handle)
                .await
                .is_ok()
        );
    }

    async fn verify_receiver(coalesce: bool, drop_sender: bool) {
        fn create_receiver<M>(coalesce: bool) -> (mpsc::UnboundedSender<M>, PortReceiver<M>) {
            // Create dummy state and port_id to create PortReceiver. They are
            // not used in the test.
            let dummy_actor_ref: ActorAddr = test_actor_id("world_0", "actor");
            let dummy_state = State::new(dummy_actor_ref.clone());
            let dummy_port_id = dummy_actor_ref.port_addr(Port::from(0));
            let (sender, receiver) = mpsc::unbounded_channel::<M>();
            let receiver = PortReceiver {
                receiver,
                port_id: dummy_port_id,
                coalesce,
                mailbox: Mailbox {
                    inner: Arc::new(dummy_state),
                },
            };
            (sender, receiver)
        }

        // verify fn drain
        {
            let (sender, mut receiver) = create_receiver::<u64>(coalesce);
            assert!(receiver.drain().is_empty());

            sender.send(0).unwrap();
            sender.send(1).unwrap();
            sender.send(2).unwrap();
            sender.send(3).unwrap();
            sender.send(4).unwrap();
            sender.send(5).unwrap();
            sender.send(6).unwrap();
            sender.send(7).unwrap();

            if drop_sender {
                drop(sender);
            }

            if !coalesce {
                assert_eq!(receiver.drain(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
            } else {
                assert_eq!(receiver.drain(), vec![7]);
            }

            assert!(receiver.drain().is_empty());
            assert!(receiver.drain().is_empty());
        }

        // verify fn try_recv
        {
            let (sender, mut receiver) = create_receiver::<u64>(coalesce);
            assert!(receiver.try_recv().unwrap().is_none());

            sender.send(0).unwrap();
            sender.send(1).unwrap();
            sender.send(2).unwrap();
            sender.send(3).unwrap();

            if drop_sender {
                drop(sender);
            }

            if !coalesce {
                assert_eq!(receiver.try_recv().unwrap().unwrap(), 0);
                assert_eq!(receiver.try_recv().unwrap().unwrap(), 1);
                assert_eq!(receiver.try_recv().unwrap().unwrap(), 2);
            }
            assert_eq!(receiver.try_recv().unwrap().unwrap(), 3);
            if drop_sender {
                assert_matches!(
                    receiver.try_recv().unwrap_err().kind(),
                    MailboxErrorKind::Closed
                );
                // Still Closed error
                assert_matches!(
                    receiver.try_recv().unwrap_err().kind(),
                    MailboxErrorKind::Closed
                );
            } else {
                assert!(receiver.try_recv().unwrap().is_none());
                // Still empty
                assert!(receiver.try_recv().unwrap().is_none());
            }
        }
        // verify fn recv
        {
            let (sender, mut receiver) = create_receiver::<u64>(coalesce);
            assert!(
                tokio::time::timeout(tokio::time::Duration::from_secs(1), receiver.recv())
                    .await
                    .is_err()
            );

            sender.send(4).unwrap();
            sender.send(5).unwrap();
            sender.send(6).unwrap();
            sender.send(7).unwrap();

            if drop_sender {
                drop(sender);
            }

            if !coalesce {
                assert_eq!(receiver.recv().await.unwrap(), 4);
                assert_eq!(receiver.recv().await.unwrap(), 5);
                assert_eq!(receiver.recv().await.unwrap(), 6);
            }
            assert_eq!(receiver.recv().await.unwrap(), 7);
            if drop_sender {
                assert_matches!(
                    receiver.recv().await.unwrap_err().kind(),
                    MailboxErrorKind::Closed
                );
                // Still None
                assert_matches!(
                    receiver.recv().await.unwrap_err().kind(),
                    MailboxErrorKind::Closed
                );
            } else {
                assert!(
                    tokio::time::timeout(tokio::time::Duration::from_secs(1), receiver.recv())
                        .await
                        .is_err()
                );
            }
        }
    }

    #[tokio::test]
    async fn test_receiver_basic_default() {
        verify_receiver(/*coalesce=*/ false, /*drop_sender=*/ false).await
    }

    #[tokio::test]
    async fn test_receiver_basic_latest() {
        verify_receiver(/*coalesce=*/ true, /*drop_sender=*/ false).await
    }

    #[tokio::test]
    async fn test_receiver_after_sender_drop_default() {
        verify_receiver(/*coalesce=*/ false, /*drop_sender=*/ true).await
    }

    #[tokio::test]
    async fn test_receiver_after_sender_drop_latest() {
        verify_receiver(/*coalesce=*/ true, /*drop_sender=*/ true).await
    }

    struct Setup {
        receiver: PortReceiver<u64>,
        actor0: Instance<()>,
        actor1: Instance<()>,
        _actor0_handle: ActorHandle<()>,
        _actor1_handle: ActorHandle<()>,
        port_id: PortAddr,
        port_id1: PortAddr,
        port_id2: PortAddr,
        port_id2_1: PortAddr,
    }

    async fn setup_split_port_ids(
        reducer_spec: Option<ReducerSpec>,
        reducer_mode: ReducerMode,
    ) -> Setup {
        let proc = Proc::isolated();
        let (actor0, actor0_handle) = proc.client("actor0").unwrap();
        let (actor1, actor1_handle) = proc.client("actor1").unwrap();

        // Open a port on actor0
        let (port_handle, receiver) = actor0.open_port::<u64>();
        let port_id = port_handle.bind().port_addr().clone();

        // Split it twice on actor1
        let port_id1 = port_id
            .split(&actor1, reducer_spec.clone(), reducer_mode.clone(), true)
            .unwrap();
        let port_id2 = port_id
            .split(&actor1, reducer_spec.clone(), reducer_mode.clone(), true)
            .unwrap();

        // A split port id can also be split
        let port_id2_1 = port_id2
            .split(&actor1, reducer_spec, reducer_mode.clone(), true)
            .unwrap();

        Setup {
            receiver,
            actor0,
            actor1,
            _actor0_handle: actor0_handle,
            _actor1_handle: actor1_handle,
            port_id,
            port_id1,
            port_id2,
            port_id2_1,
        }
    }

    fn post(cx: &impl context::Actor, port_id: PortAddr, msg: u64) {
        let serialized = wirevalue::Any::serialize(&msg).unwrap();
        port_id.send(cx, serialized);
    }

    #[async_timed_test(timeout_secs = 30)]
    // TODO: OSS: this test is flaky in OSS. Need to repo and fix it.
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_split_port_id_no_reducer() {
        let Setup {
            mut receiver,
            actor0,
            actor1,
            port_id,
            port_id1,
            port_id2,
            port_id2_1,
            ..
        } = setup_split_port_ids(None, ReducerMode::default()).await;
        // Can send messages to receiver from all port handles
        post(&actor0, port_id.clone(), 1);
        assert_eq!(receiver.recv().await.unwrap(), 1);
        post(&actor1, port_id1.clone(), 2);
        assert_eq!(receiver.recv().await.unwrap(), 2);
        post(&actor1, port_id2.clone(), 3);
        assert_eq!(receiver.recv().await.unwrap(), 3);
        post(&actor1, port_id2_1.clone(), 4);
        assert_eq!(receiver.recv().await.unwrap(), 4);

        // no more messages
        tokio::time::sleep(Duration::from_secs(2)).await;
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);
    }

    async fn wait_for(
        receiver: &mut PortReceiver<u64>,
        expected_size: usize,
        timeout_duration: Duration,
    ) -> anyhow::Result<Vec<u64>> {
        let mut messeges = vec![];

        tokio::time::timeout(timeout_duration, async {
            loop {
                let msg = receiver.recv().await.unwrap();
                messeges.push(msg);
                if messeges.len() == expected_size {
                    break;
                }
            }
        })
        .await?;
        Ok(messeges)
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_split_port_id_sum_reducer() {
        let config = hyperactor_config::global::lock();
        let _config_guard = config.override_key(crate::config::SPLIT_MAX_BUFFER_SIZE, 1);

        let sum_accumulator = accum::sum::<u64>();
        let reducer_spec = sum_accumulator.reducer_spec();
        let Setup {
            mut receiver,
            actor0,
            actor1,
            port_id,
            port_id1,
            port_id2,
            port_id2_1,
            ..
        } = setup_split_port_ids(reducer_spec, ReducerMode::default()).await;
        post(&actor0, port_id.clone(), 4);
        post(&actor1, port_id1.clone(), 2);
        post(&actor1, port_id2.clone(), 3);
        post(&actor1, port_id2_1.clone(), 1);
        let mut messages = wait_for(&mut receiver, 4, Duration::from_secs(2))
            .await
            .unwrap();
        // Message might be received out of their sending out. So we sort the
        // messages here.
        messages.sort();
        assert_eq!(messages, vec![1, 2, 3, 4]);

        // no more messages
        tokio::time::sleep(Duration::from_secs(2)).await;
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);
    }

    #[async_timed_test(timeout_secs = 30)]
    // TODO: OSS: this test is flaky in OSS. Need to repo and fix it.
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_split_port_id_every_n_messages() {
        let config = hyperactor_config::global::lock();
        let _config_guard =
            config.override_key(crate::config::SPLIT_MAX_BUFFER_AGE, Duration::from_mins(10));
        let proc = Proc::isolated();
        let (actor, _actor_handle) = proc.client("actor").unwrap();
        let (port_handle, mut receiver) = actor.open_port::<u64>();
        let port_id = port_handle.bind().port_addr().clone();
        // Split it
        let reducer_spec = accum::sum::<u64>().reducer_spec();
        let split_port_id = port_id
            .split(
                &actor,
                reducer_spec,
                ReducerMode::Streaming(accum::StreamingReducerOpts {
                    max_update_interval: Some(Duration::from_mins(10)),
                    initial_update_interval: Some(Duration::from_mins(10)),
                }),
                true,
            )
            .unwrap();

        // Send 9 messages.
        for msg in [1, 5, 3, 4, 2, 91, 92, 93, 94] {
            post(&actor, split_port_id.clone(), msg);
        }
        // The first 5 should be batched and reduced once due
        // to every_n_msgs = 5.
        let messages = wait_for(&mut receiver, 1, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(messages, vec![15]);

        // the last message unfortranately will never come because they do not
        // reach batch size.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_split_port_timeout_flush() {
        let config = hyperactor_config::global::lock();
        let _config_guard = config.override_key(crate::config::SPLIT_MAX_BUFFER_SIZE, 100);

        let Setup {
            mut receiver,
            actor0: _,
            actor1,
            port_id: _,
            port_id1,
            port_id2: _,
            port_id2_1: _,
            ..
        } = setup_split_port_ids(
            Some(accum::sum::<u64>().reducer_spec().unwrap()),
            ReducerMode::Streaming(accum::StreamingReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
                initial_update_interval: Some(Duration::from_millis(50)),
            }),
        )
        .await;

        post(&actor1, port_id1.clone(), 10);
        post(&actor1, port_id1.clone(), 20);
        post(&actor1, port_id1.clone(), 30);

        // Messages should accumulate for 50ms.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);

        // Wait until we are flushed.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now we are reduced and accumulated:
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, 60); // 10 + 20 + 30

        // No further messages:
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_split_port_timeout_and_size_flush() {
        let config = hyperactor_config::global::lock();
        let _config_guard = config.override_key(crate::config::SPLIT_MAX_BUFFER_SIZE, 3);

        let Setup {
            mut receiver,
            actor0: _,
            actor1,
            port_id: _,
            port_id1,
            port_id2: _,
            port_id2_1: _,
            ..
        } = setup_split_port_ids(
            Some(accum::sum::<u64>().reducer_spec().unwrap()),
            ReducerMode::Streaming(accum::StreamingReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
                initial_update_interval: Some(Duration::from_millis(50)),
            }),
        )
        .await;

        post(&actor1, port_id1.clone(), 10);
        post(&actor1, port_id1.clone(), 20);
        post(&actor1, port_id1.clone(), 30);
        post(&actor1, port_id1.clone(), 40);

        // Should have flushed at the third message.
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, 60);

        // After 50ms, the next reduce will flush:
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, 40);

        // No further messages
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_split_port_once_mode_basic() {
        let proc = Proc::isolated();
        let (actor, _actor_handle) = proc.client("actor").unwrap();
        let (port_handle, mut receiver) = actor.open_port::<u64>();
        let port_id = port_handle.bind().port_addr().clone();

        // Split with Once(3) mode - accumulate 3 values then emit
        let reducer_spec = accum::sum::<u64>().reducer_spec();
        let split_port_id = port_id
            .split(&actor, reducer_spec, ReducerMode::Once(3), true)
            .unwrap();

        // Send 3 messages
        post(&actor, split_port_id.clone(), 10);
        post(&actor, split_port_id.clone(), 20);
        post(&actor, split_port_id.clone(), 30);

        // Should receive a single reduced message
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, 60); // 10 + 20 + 30

        // No further messages
        tokio::time::sleep(Duration::from_millis(100)).await;
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_split_port_once_mode_teardown() {
        let proc = Proc::isolated();
        let (actor, _actor_handle) = proc.client("actor").unwrap();
        let (port_handle, mut receiver) = actor.open_port::<u64>();
        let port_id = port_handle.bind().port_addr().clone();

        // Set up an undeliverable receiver to capture messages sent to torn-down ports
        let (undeliverable_handle, mut undeliverable_receiver) =
            undeliverable::new_undeliverable_port();

        // Split with Once(3) mode - accumulate 3 values then emit and tear down
        let reducer_spec = accum::sum::<u64>().reducer_spec();
        let split_port_id = port_id
            .split(&actor, reducer_spec, ReducerMode::Once(3), true)
            .unwrap();

        // Send 3 messages to trigger reduction
        post(&actor, split_port_id.clone(), 10);
        post(&actor, split_port_id.clone(), 20);
        post(&actor, split_port_id.clone(), 30);

        // Should receive a single reduced message
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, 60); // 10 + 20 + 30

        // Now send another message - it should fail because the port is torn down
        let serialized = wirevalue::Any::serialize(&100u64).unwrap();
        let envelope = MessageEnvelope::new(
            actor.mailbox().actor_addr().clone(),
            split_port_id.clone(),
            serialized,
            Flattrs::new(),
        );
        actor.mailbox().post(envelope, undeliverable_handle);

        // Verify the message was returned as undeliverable
        let undeliverable =
            tokio::time::timeout(Duration::from_secs(2), undeliverable_receiver.recv())
                .await
                .expect("should receive undeliverable message")
                .expect("receiver should not be closed");

        // Verify the undeliverable message has the correct destination
        let split_port_ref: PortAddr = split_port_id;
        assert_eq!(
            undeliverable
                .into_message()
                .expect("expected returned envelope")
                .dest(),
            &split_port_ref
        );

        // Verify no additional messages arrived at the original receiver
        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg, None);
    }

    #[test]
    fn test_dial_mailbox_router_prefixes_empty() {
        assert_eq!(DialMailboxRouter::new().prefixes().len(), 0);
    }

    #[test]
    fn test_dial_mailbox_router_prefixes_single_entry() {
        let router = DialMailboxRouter::new();
        router.bind(test_proc_ref("world0"), "unix!@1".parse().unwrap());

        let prefixes: Vec<Addr> = router.prefixes().into_iter().collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], test_proc_ref("world0"));
    }

    #[test]
    fn test_dial_mailbox_router_prefixes_no_overlap() {
        let router = DialMailboxRouter::new();
        router.bind(test_proc_ref("world0"), "unix!@1".parse().unwrap());
        router.bind(test_proc_ref("world1"), "unix!@2".parse().unwrap());
        router.bind(test_proc_ref("world2"), "unix!@3".parse().unwrap());

        let mut prefixes: Vec<Addr> = router.prefixes().into_iter().collect();
        prefixes.sort();

        let mut expected = vec![
            test_proc_ref("world0"),
            test_proc_ref("world1"),
            test_proc_ref("world2"),
        ];
        expected.sort();

        assert_eq!(prefixes, expected);
    }

    #[test]
    fn test_dial_mailbox_router_prefixes_with_overlaps() {
        let router = DialMailboxRouter::new();
        // Proc refs are all independent since they have different names.
        router.bind(test_proc_ref("world0"), "unix!@1".parse().unwrap());
        router.bind(test_proc_ref("world0_0"), "unix!@2".parse().unwrap());
        router.bind(test_proc_ref("world0_1"), "unix!@3".parse().unwrap());
        router.bind(test_proc_ref("world1"), "unix!@4".parse().unwrap());
        router.bind(test_proc_ref("world1_0"), "unix!@5".parse().unwrap());

        let mut prefixes: Vec<Addr> = router.prefixes().into_iter().collect();
        prefixes.sort();

        let mut expected = vec![
            test_proc_ref("world0"),
            test_proc_ref("world0_0"),
            test_proc_ref("world0_1"),
            test_proc_ref("world1"),
            test_proc_ref("world1_0"),
        ];
        expected.sort();

        assert_eq!(prefixes, expected);
    }

    #[test]
    fn test_dial_mailbox_router_prefixes_complex_hierarchy() {
        let router = DialMailboxRouter::new();
        // Proc refs still cover their own actors (same proc_id).
        router.bind(test_proc_ref("world0"), "unix!@1".parse().unwrap());
        router.bind(test_proc_ref("world0_0"), "unix!@2".parse().unwrap());
        router.bind(
            test_actor_ref("world0_0", "actor1"),
            "unix!@3".parse().unwrap(),
        );
        router.bind(test_proc_ref("world1_0"), "unix!@4".parse().unwrap());
        router.bind(test_proc_ref("world1_1"), "unix!@5".parse().unwrap());
        router.bind(
            test_actor_ref("world2_0", "actor0"),
            "unix!@6".parse().unwrap(),
        );

        let mut prefixes: Vec<Addr> = router.prefixes().into_iter().collect();
        prefixes.sort();

        // Covering prefixes:
        // - world0 (independent proc ref)
        // - world0_0 (covers world0_0.actor1 since same proc_id)
        // - world1_0 (not covered by anything else)
        // - world1_1 (not covered by anything else)
        // - world2_0.actor0 (not covered by anything else)
        let mut expected = vec![
            test_proc_ref("world0"),
            test_proc_ref("world0_0"),
            test_proc_ref("world1_0"),
            test_proc_ref("world1_1"),
            test_actor_ref("world2_0", "actor0"),
        ];
        expected.sort();

        assert_eq!(prefixes, expected);
    }

    #[test]
    fn test_dial_mailbox_router_prefixes_same_level() {
        let router = DialMailboxRouter::new();
        router.bind(test_proc_ref("world0_0"), "unix!@1".parse().unwrap());
        router.bind(test_proc_ref("world0_1"), "unix!@2".parse().unwrap());
        router.bind(test_proc_ref("world0_2"), "unix!@3".parse().unwrap());

        let mut prefixes: Vec<Addr> = router.prefixes().into_iter().collect();
        prefixes.sort();

        // All should be covering prefixes since none is a prefix of another
        let mut expected = vec![
            test_proc_ref("world0_0"),
            test_proc_ref("world0_1"),
            test_proc_ref("world0_2"),
        ];
        expected.sort();

        assert_eq!(prefixes, expected);
    }

    /// A forwarder that bounces messages back to the **same**
    /// mailbox, but does so on a task to avoid recursive stack
    /// growth.
    #[derive(Clone, Debug)]
    struct AsyncLoopForwarder;

    #[async_trait]
    impl MailboxSender for AsyncLoopForwarder {
        fn post_unchecked(
            &self,
            envelope: MessageEnvelope,
            return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        ) {
            let me = self.clone();
            tokio::spawn(async move {
                // Call `post` so each hop applies TTL exactly once.
                me.post(envelope, return_handle);
            });
        }
    }

    #[tokio::test]
    async fn message_ttl_expires_in_routing_loop_returns_to_sender() {
        let actor_id = test_actor_id("world_0", "ttl_actor");
        let (ret_port, mut ret_rx) = undeliverable::new_undeliverable_port();

        let remote_actor = test_actor_id("remote_world_1", "remote");
        let dest = remote_actor.port_addr(4242.into());

        // Build an envelope (TTL is seeded in `MessageEnvelope::new` /
        // `::serialize`).
        let payload = 1234_u64;
        let envelope =
            MessageEnvelope::serialize(actor_id.clone(), dest.clone(), &payload, Flattrs::new())
                .expect("serialize");

        AsyncLoopForwarder.post(envelope, ret_port.clone());

        // We expect the undeliverable to come back once TTL expires.
        let undelivered = tokio::time::timeout(Duration::from_secs(5), ret_rx.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("channel closed")
            .into_message()
            .expect("expected returned envelope");

        // Sanity: round-trip payload still deserializes.
        let got: u64 = undelivered.deserialized().expect("deserialize");
        assert_eq!(got, payload, "payload preserved");
    }

    #[tokio::test]
    async fn message_ttl_success_local_delivery() {
        let actor_id = test_actor_id("world_0", "ttl_actor");
        let mailbox = Mailbox::new(actor_id.clone());
        let (_undeliverable_tx, mut undeliverable_rx) =
            mailbox.bind_handler_port::<Undeliverable<MessageEnvelope>>();

        // Open a local user u64 port.
        let (user_port, mut user_rx) = mailbox.open_port::<u64>();

        // Build an envelope destined for this mailbox's own port.
        let payload = 0xC0FFEE_u64;
        let envelope = MessageEnvelope::serialize(
            actor_id.clone(),
            user_port.bind().port_addr().clone(),
            &payload,
            Flattrs::new(),
        )
        .expect("serialize");

        // Post the message using the mailbox (local path). TTL will
        // not expire.
        let return_handle = mailbox
            .bound_return_handle()
            .unwrap_or(monitored_return_handle());
        mailbox.post(envelope, return_handle);

        // We should receive the payload locally.
        let got = tokio::time::timeout(Duration::from_secs(1), user_rx.recv())
            .await
            .expect("timed out waiting for local delivery")
            .expect("user port closed");
        assert_eq!(got, payload);

        // There should be no undeliverables arriving.
        let no_undeliverable =
            tokio::time::timeout(Duration::from_millis(100), undeliverable_rx.recv()).await;
        assert!(
            no_undeliverable.is_err(),
            "unexpected undeliverable returned on successful local delivery"
        );
    }

    #[tokio::test]
    async fn test_port_contramap() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (handle, mut rx) = client.open_port();

        handle
            .contramap(|m| (1, m))
            .post(&client, "hello".to_string());
        assert_eq!(rx.recv().await.unwrap(), (1, "hello".to_string()));
    }

    #[test]
    #[should_panic(expected = "already bound")]
    fn test_bind_port_handle_to_handler_port_twice() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (handle, _rx) = mbox.open_port::<String>();
        handle.bind_handler_port();
        handle.bind_handler_port();
    }

    #[test]
    fn test_bind_port_handle_to_handler_port() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let default_port = mbox.actor_addr().port_addr(Port::from(String::port()));
        let (handle, _rx) = mbox.open_port::<String>();
        // Handle's port index is allocated by mailbox, not the handler port.
        assert_ne!(default_port.index(), handle.inner.port_index);
        // Bind the handle to the handler port.
        handle.bind_handler_port();
        assert_matches!(handle.location(), PortLocation::Bound(port) if port == default_port);
        // bind() can still be used, just it will not change handle's state.
        handle.bind();
        handle.bind();
        assert_matches!(handle.location(), PortLocation::Bound(port) if port == default_port);
    }

    #[test]
    #[should_panic(expected = "already bound")]
    fn test_bind_port_handle_to_handler_port_when_already_bound() {
        let mbox = Mailbox::new(test_actor_id("0", "test"));
        let (handle, _rx) = mbox.open_port::<String>();
        // Bound handle to the port allocated by mailbox.
        handle.bind();
        assert_matches!(handle.location(), PortLocation::Bound(port) if port.index() == handle.inner.port_index);
        // Since handle is already bound, call bind_to() on it will cause panic.
        handle.bind_handler_port();
    }

    #[tokio::test]
    async fn test_mailbox_post_fails_when_actor_stopped() {
        let actor_id = test_actor_id("0", "stopped_actor");

        let mailbox = Mailbox::new(actor_id.clone());

        mailbox.close(ActorStatus::Stopped("test stop".to_string()));

        let (user_port, _user_rx) = mailbox.open_port::<u64>();

        // Use a separate return mailbox since
        // the main mailbox is stopped and won't accept messages.
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        let envelope = MessageEnvelope::serialize(
            actor_id.clone(),
            user_port.bind().port_addr().clone(),
            &42u64,
            Flattrs::new(),
        )
        .expect("serialize");

        mailbox.post(envelope, return_handle);

        let undeliverable = tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("return port closed");

        let err = undeliverable
            .into_message()
            .expect("expected returned envelope")
            .error_msg()
            .expect("expected error");
        assert!(
            err.contains(&format!("owner {} is stopped", actor_id)),
            "error should indicate actor stopped: {}",
            err
        );
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::InvalidReference(invalid_reference) = &root_failure.kind else {
            panic!("expected invalid reference, got {root_failure}");
        };
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::ActorStopped
        );
    }

    #[tokio::test]
    async fn test_mailbox_post_fails_when_actor_failed() {
        use crate::actor::ActorErrorKind;

        let actor_id = test_actor_id("0", "failed_actor");

        let mailbox = Mailbox::new(actor_id.clone());

        let (user_port, _user_rx) = mailbox.open_port::<u64>();

        mailbox.close(ActorStatus::Failed(ActorErrorKind::Generic(
            "test failure".to_string(),
        )));

        // Use a separate return mailbox since
        // the main mailbox is failed and won't accept messages.
        let (return_handle, mut return_rx) = undeliverable::new_undeliverable_port();

        let envelope = MessageEnvelope::serialize(
            actor_id.clone(),
            user_port.bind().port_addr().clone(),
            &42u64,
            Flattrs::new(),
        )
        .expect("serialize");

        mailbox.post(envelope, return_handle);

        let undeliverable = tokio::time::timeout(Duration::from_secs(1), return_rx.recv())
            .await
            .expect("timed out waiting for undeliverable")
            .expect("return port closed");

        let err = undeliverable
            .into_message()
            .expect("expected returned envelope")
            .error_msg()
            .expect("expected error");
        assert!(
            err.contains(&format!("owner {} failed", actor_id)),
            "error should indicate actor failed: {}",
            err
        );
        let root_failure = undelivered
            .root_delivery_failure()
            .expect("expected root delivery failure");
        let DeliveryFailureKind::InvalidReference(invalid_reference) = &root_failure.kind else {
            panic!("expected invalid reference, got {root_failure}");
        };
        assert_eq!(
            invalid_reference.reason,
            InvalidReferenceReason::ActorFailed
        );
    }

    #[tokio::test]
    async fn test_port_handle_send_fails_when_actor_stopped() {
        let actor_id = test_actor_id("0", "stopped_actor");

        let mailbox = Mailbox::new(actor_id.clone());

        let (port_handle, _rx) = mailbox.open_port::<u64>();
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();

        mailbox.close(ActorStatus::Stopped("test stop".to_string()));

        let err = port_handle.try_send(&client, 42u64).unwrap_err();
        assert_matches!(
            err.kind(),
            MailboxSenderErrorKind::Mailbox(mailbox_err)
                if matches!(mailbox_err.kind(), MailboxErrorKind::OwnerTerminated(ActorStatus::Stopped(reason)) if reason == "test stop")
        );
    }

    #[tokio::test]
    async fn test_port_handle_send_fails_when_actor_failed() {
        use crate::actor::ActorErrorKind;

        let actor_id = test_actor_id("0", "failed_actor");

        let mailbox = Mailbox::new(actor_id.clone());

        let (port_handle, _rx) = mailbox.open_port::<u64>();
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();

        mailbox.close(ActorStatus::Failed(ActorErrorKind::Generic(
            "test failure".to_string(),
        )));

        let err = port_handle.try_send(&client, 42u64).unwrap_err();
        assert_matches!(
            err.kind(),
            MailboxSenderErrorKind::Mailbox(mailbox_err)
                if matches!(mailbox_err.kind(), MailboxErrorKind::OwnerTerminated(ActorStatus::Failed(ActorErrorKind::Generic(msg))) if msg == "test failure")
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_open_reduce_port() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();

        // Open an accumulator port with sum reducer
        let (port_handle, receiver) = client.mailbox().open_reduce_port(accum::sum::<u64>());

        // Verify the reducer_spec is set
        let port_ref = port_handle.bind();
        assert!(port_ref.reducer_spec().is_some());

        // Send a single value via the bound port
        port_ref.post(&client, 42);

        // Should receive the value
        let result = receiver.recv().await.unwrap();
        assert_eq!(result, 42);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_open_reduce_port_reducer_spec_preserved() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();

        // Test that different accumulators produce different reducer_specs
        let (sum_handle, _) = client.mailbox().open_reduce_port(accum::sum::<u64>());
        let sum_ref = sum_handle.bind();
        let sum_typehash = sum_ref.reducer_spec().as_ref().unwrap().typehash;

        let (max_handle, _) = client
            .mailbox()
            .open_reduce_port(accum::join_semilattice::<accum::Max<u64>>());
        let max_ref = max_handle.bind();
        let max_typehash = max_ref.reducer_spec().as_ref().unwrap().typehash;

        // Different accumulators should have different reducer typehashes
        assert_ne!(sum_typehash, max_typehash);
    }

    /// Test that `MailboxClient::flush()` waits until messages are wire-acked
    /// over a unix domain socket channel. We send messages, flush, and then
    /// confirm that the messages have already been delivered to the receiving
    /// mailbox.
    #[tokio::test]
    async fn test_flush_over_unix_channel() {
        let mbox = Mailbox::new(test_actor_id("0", "actor0"));

        // Serve the mailbox on a unix domain socket channel.
        let (addr, rx) = channel::serve(ChannelAddr::any(ChannelTransport::Unix)).unwrap();
        let serve_handle = mbox.clone().serve(rx);

        // Dial the unix address to get a MailboxClient.
        let client = MailboxClient::dial(addr).unwrap();

        // Open a streaming port so we can receive multiple messages.
        let (port, mut receiver) = mbox.open_port::<u64>();
        let port = port.bind();

        // Send several messages without awaiting delivery.
        for i in 0..10u64 {
            client
                .serialize_and_send(&port, i, monitored_return_handle())
                .unwrap();
        }

        // Flush: this should block until all 10 messages are wire-acked,
        // meaning they've been enqueued into the receiving mailbox.
        client.flush().await.unwrap();

        // After flush, all messages should already be available.
        for i in 0..10u64 {
            let msg = receiver
                .try_recv()
                .expect("message should be available after flush")
                .expect("receiver should not be empty after flush");
            assert_eq!(msg, i);
        }

        serve_handle.stop("test done");
        serve_handle.await.unwrap().unwrap();
    }

    #[test]
    fn test_drain_waits_for_active_handler_enqueue() {
        let mailbox = Mailbox::new(test_actor_id("drain", "actor"));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let delivered = Arc::new(AtomicUsize::new(0));

        let port = mailbox.open_handler_enqueue_port({
            let release = Arc::clone(&release);
            let delivered = Arc::clone(&delivered);
            move |_headers, _message: u64| {
                entered_tx.send(()).unwrap();
                let (lock, cvar) = &*release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cvar.wait(released).unwrap();
                }
                delivered.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });

        let sender = port.inner.sender.clone();
        let sender_thread = std::thread::spawn(move || sender.send(Flattrs::new(), 1u64).unwrap());
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (drained_tx, drained_rx) = std::sync::mpsc::channel();
        let drain_thread = std::thread::spawn({
            let mailbox = mailbox.clone();
            move || {
                mailbox.drain();
                drained_tx.send(()).unwrap();
            }
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while mailbox.inner.handler_ingress.state.load(Ordering::Acquire) & HANDLER_INGRESS_DRAINING
            == 0
        {
            assert!(std::time::Instant::now() < deadline, "drain did not start");
            std::thread::yield_now();
        }
        assert_matches!(
            drained_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );

        let (lock, cvar) = &*release;
        *lock.lock().unwrap() = true;
        cvar.notify_all();

        sender_thread.join().unwrap();
        drained_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drain_thread.join().unwrap();
        assert_eq!(delivered.load(Ordering::SeqCst), 1);

        let err = port.inner.sender.send(Flattrs::new(), 2u64).unwrap_err();
        assert!(err.is::<HandlerPortClosedError>());
    }

    /// Helper: build a `MessageEnvelope` with a recognizable payload
    /// and non-empty headers, then feed it through
    /// `UndeliverableMailboxSender::post_unchecked`. Returns the
    /// sender + destination so tests can assert against the values we
    /// know will end up on the log.
    fn drive_abandonment_log(payload_sentinel: &str) -> (crate::ActorAddr, crate::PortAddr) {
        use hyperactor_config::declare_attrs;

        declare_attrs! {
            // Any non-empty entry works; UM-1 only asserts the log
            // does not dump `headers` inline.
            attr UM_TEST_HEADER: u64;
        }

        let sender = test_actor_id("um_proc", "um_sender");
        let dest = test_port_id("um_dest_proc", "um_dest", 42);

        let mut headers = Flattrs::new();
        headers.set(UM_TEST_HEADER, 0xC0FFEEu64);

        let envelope = MessageEnvelope::new(
            sender.clone(),
            dest.clone(),
            wirevalue::Any::serialize(&payload_sentinel.to_string()).unwrap(),
            headers,
        );

        let (return_handle, _rx) = crate::mailbox::undeliverable::new_undeliverable_port();
        UndeliverableMailboxSender.post_unchecked(envelope, return_handle);
        (sender, dest)
    }

    /// UM-1: the log does not render unbounded `headers` or `data`
    /// fields, and reports bounded `message_type` + `data_len`
    /// summaries instead.
    #[tracing_test::traced_test]
    #[test]
    fn test_um1_bounded_fields() {
        let payload_sentinel = "um1_payload_sentinel_5b7a9c3d";
        let (_sender, _dest) = drive_abandonment_log(payload_sentinel);

        let buf = tracing_test::internal::global_buf().lock().unwrap();
        let logs = std::str::from_utf8(&buf).expect("logs are utf-8");

        // Bounded summaries are present.
        assert!(
            logs.contains("message_type="),
            "UM-1: expected message_type field, got:\n{logs}"
        );
        assert!(
            logs.contains("data_len="),
            "UM-1: expected data_len field, got:\n{logs}"
        );
        // The payload body must not appear (no `data=<full body>`
        // dump).
        assert!(
            !logs.contains(payload_sentinel),
            "UM-1: payload body leaked into the log:\n{logs}"
        );
        // The `headers=` field must not appear. Use a space-prefixed
        // match to avoid matching e.g. the word "headers" in free
        // prose.
        assert!(
            !logs.contains(" headers="),
            "UM-1: unbounded headers field leaked into the log:\n{logs}"
        );
    }

    /// UM-2: the `actor_name` and `actor_id` fields are preserved
    /// on the log for downstream Scuba / alert compatibility — same
    /// field names, same values, same types as the prior shape.
    #[tracing_test::traced_test]
    #[test]
    fn test_um2_compat_fields_preserved() {
        let (sender, _dest) = drive_abandonment_log("um2_payload");

        let buf = tracing_test::internal::global_buf().lock().unwrap();
        let logs = std::str::from_utf8(&buf).expect("logs are utf-8");

        // Decouple field-presence from value-presence so the test
        // does not depend on the tracing formatter's quoting rules.
        let actor_name = sender.log_name();
        let actor_id = sender.to_string();
        assert!(
            logs.contains("actor_name=") && logs.contains(actor_name),
            "UM-2: expected actor_name={actor_name} on the log, got:\n{logs}"
        );
        assert!(
            logs.contains("actor_id=") && logs.contains(&actor_id),
            "UM-2: expected actor_id={actor_id} on the log, got:\n{logs}"
        );
    }

    /// UM-3: the format string names the transport destination.
    #[tracing_test::traced_test]
    #[test]
    fn test_um3_destination_format() {
        let (_sender, dest) = drive_abandonment_log("um3_payload");

        let buf = tracing_test::internal::global_buf().lock().unwrap();
        let logs = std::str::from_utf8(&buf).expect("logs are utf-8");

        assert!(
            logs.contains(&format!("message not delivered to {}", dest)),
            "UM-3: expected destination-naming format string, got:\n{logs}"
        );
    }

    /// UM-3b: when the envelope carries operation-context headers, the
    /// format string names the user operation and the log carries
    /// the structured `endpoint` / `adverb` fields.
    #[tracing_test::traced_test]
    #[test]
    fn test_um3b_operation_format_with_operation_context() {
        let sender = test_actor_id("um_proc", "um_sender");
        let dest = test_port_id("um_dest_proc", "um_dest", 42);

        let mut headers = Flattrs::new();
        headers.set(
            headers::OPERATION_ENDPOINT,
            "training.buffer.sample()".to_string(),
        );
        headers.set(headers::OPERATION_ADVERB, "call_one".to_string());

        let envelope = MessageEnvelope::new(
            sender,
            dest,
            wirevalue::Any::serialize(&"um3b_payload".to_string()).unwrap(),
            headers,
        );

        let (return_handle, _rx) = crate::mailbox::undeliverable::new_undeliverable_port();
        UndeliverableMailboxSender.post_unchecked(envelope, return_handle);

        let buf = tracing_test::internal::global_buf().lock().unwrap();
        let logs = std::str::from_utf8(&buf).expect("logs are utf-8");

        assert!(
            logs.contains("abandoned message for training.buffer.sample()"),
            "UM-3b: expected operation-naming format string, got:\n{logs}"
        );
        assert!(
            logs.contains("endpoint=") && logs.contains("training.buffer.sample()"),
            "UM-3b: expected endpoint field with the caller's operation, got:\n{logs}"
        );
        assert!(
            logs.contains("adverb=") && logs.contains("call_one"),
            "UM-3b: expected adverb field, got:\n{logs}"
        );
        // The UM-3a destination-naming shape must not appear when
        // operation-context headers are stamped on the envelope.
        assert!(
            !logs.contains("message not delivered to"),
            "UM-3b: unexpected destination-naming format string:\n{logs}"
        );
    }
}
