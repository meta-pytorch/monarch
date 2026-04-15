/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! References for different resources in Hyperactor.
//!
//! The "Id" variants are transparent and typeless, whereas the
//! corresponding "Ref" variants are opaque and typed. The latter intended
//! to be exposed in user-facing APIs. We provide [`std::convert::From`]
//! implementations between Id and Refs where this makes sense.
//!
//! All system implementations use the same concrete reference
//! representations, as their specific layout (e.g., actor index, rank,
//! etc.) are used by the core communications algorithms throughout.
//!
//! References and ids are [`crate::Message`]s to facilitate passing
//! them between actors.

#![allow(dead_code)] // Allow until this is used outside of tests.

use std::cmp::Ord;
use std::cmp::Ordering;
use std::cmp::PartialOrd;
use std::convert::From;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::str::FromStr;

use derivative::Derivative;
use enum_as_inner::EnumAsInner;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typeuri::ACTOR_PORT_BIT;
use typeuri::Named;
use wirevalue::TypeInfo;

use crate::Actor;
use crate::ActorHandle;
use crate::RemoteHandles;
use crate::RemoteMessage;
use crate::accum::ReducerMode;
use crate::accum::ReducerSpec;
use crate::accum::StreamingReducerOpts;
use crate::actor::Referable;
use crate::channel::ChannelAddr;
use crate::context;
use crate::context::MailboxExt;
use crate::id::Label;
use crate::id::Uid;
use crate::mailbox::MailboxSenderError;
use crate::mailbox::MailboxSenderErrorKind;
use crate::mailbox::PortSink;
use crate::message::Bind;
use crate::message::Bindings;
use crate::message::Unbind;
use crate::ref_;

pub mod lex;
pub mod name;
mod parse;

use parse::Lexer;
use parse::ParseError;
use parse::Token;
pub use parse::is_valid_ident;
use parse::parse;

/// The kinds of references.
#[derive(strum::Display)]
pub enum ReferenceKind {
    /// Proc references.
    Proc,
    /// Actor references.
    Actor,
    /// Port references.
    Port,
}

/// A universal reference to hierarchical identifiers in Hyperactor.
///
/// References implement a concrete syntax which can be parsed via
/// [`FromStr`]. They are of the form:
///
/// - `addr,proc_name`,
/// - `addr,proc_name,actor_name[pid]`,
/// - `addr,proc_name,actor_name[pid][port]`
///
/// Reference also implements a total ordering, so that references are
/// ordered lexicographically with the hierarchy implied by proc,
/// actor. This allows reference ordering to be used to implement prefix
/// based routing.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    Hash,
    typeuri::Named,
    EnumAsInner
)]
pub enum Reference {
    /// A reference to a proc.
    Proc(ProcId),
    /// A reference to an actor.
    Actor(ActorId), // todo: should we only allow name references here?
    /// A reference to a port.
    Port(PortId),
}

impl Reference {
    /// Tells whether this reference is a prefix of the provided reference.
    pub fn is_prefix_of(&self, other: &Reference) -> bool {
        match self {
            Self::Proc(_) => self.proc_id() == other.proc_id(),
            Self::Actor(_) => self == other,
            Self::Port(_) => self == other,
        }
    }

    /// The proc id of the reference, if any.
    pub fn proc_id(&self) -> Option<ProcId> {
        match self {
            Self::Proc(proc_id) => Some(proc_id.clone()),
            Self::Actor(actor_id) => Some(actor_id.proc_id()),
            Self::Port(port_id) => Some(port_id.actor_id().proc_id()),
        }
    }

    /// The actor id of the reference, if any.
    pub fn actor_id(&self) -> Option<ActorId> {
        match self {
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.clone()),
            Self::Port(port_id) => Some(port_id.actor_id()),
        }
    }

    /// The actor uid of the reference, if any.
    fn actor_uid(&self) -> Option<&Uid> {
        match self {
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.uid()),
            Self::Port(port_id) => Some(port_id.0.id().actor_id().uid()),
        }
    }

    /// The port of the reference, if any.
    fn port(&self) -> Option<u64> {
        match self {
            Self::Proc(_) => None,
            Self::Actor(_) => None,
            Self::Port(port_id) => Some(port_id.index()),
        }
    }

    /// Returns the kind of the reference.
    pub fn kind(&self) -> ReferenceKind {
        match self {
            Self::Proc(_) => ReferenceKind::Proc,
            Self::Actor(_) => ReferenceKind::Actor,
            Self::Port(_) => ReferenceKind::Port,
        }
    }
}

impl PartialOrd for Reference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Reference {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by: proc id, then actor uid, then port.
        // None < Some ensures Proc < Actor < Port for same proc.
        (self.proc_id(), self.actor_uid(), self.port()).cmp(&(
            other.proc_id(),
            other.actor_uid(),
            other.port(),
        ))
    }
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proc(proc_id) => fmt::Display::fmt(proc_id, f),
            Self::Actor(actor_id) => fmt::Display::fmt(actor_id, f),
            Self::Port(port_id) => fmt::Display::fmt(port_id, f),
        }
    }
}

/// The type of error encountered while parsing references.
#[derive(thiserror::Error, Debug)]
pub enum ReferenceParsingError {
    /// The parser expected a token, but it reached the end of the token stream.
    #[error("expected token")]
    Empty,

    /// The parser encountered an unexpected token.
    #[error("unexpected token: {0}")]
    Unexpected(String),

    /// The parser encountered an error parsing an integer.
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),

    /// A parse error.
    #[error("parse: {0}")]
    Parse(#[from] ParseError),

    /// The parser encountered the wrong reference type.
    #[error("wrong reference type: expected {0}")]
    WrongType(String),

    /// An invalid channel address was encountered while parsing the reference.
    #[error("invalid channel address {0}: {1}")]
    InvalidChannelAddress(String, anyhow::Error),
}

impl FromStr for Reference {
    type Err = ReferenceParsingError;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        // References are parsed in the "direct" format:
        // The reference must contain a comma (anywhere), indicating a proc/actor/port reference.

        match addr.split_once(",") {
            Some((channel_addr, rest)) => {
                let channel_addr = channel_addr.parse().map_err(|err| {
                    ReferenceParsingError::InvalidChannelAddress(channel_addr.to_string(), err)
                })?;

                Ok(parse! {
                    Lexer::new(rest);

                    // channeladdr,proc_name
                    Token::Elem(proc_name) =>
                    Self::Proc(ProcId::with_name(channel_addr, proc_name)),

                    // channeladdr,proc_name,actor_name
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name) =>
                    Self::Actor(ActorId::new(ProcId::with_name(channel_addr, proc_name), actor_name)),

                    // channeladdr,proc_name,actor_name[port]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(port) Token::RightBracket =>
                        Self::Port(PortId::new(ActorId::new(ProcId::with_name(channel_addr, proc_name), actor_name), port as u64)),

                    // channeladdr,proc_name,actor_name[port<type>]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(port)
                            Token::LessThan Token::Elem(_type) Token::GreaterThan
                        Token::RightBracket =>
                        Self::Port(PortId::new(ActorId::new(ProcId::with_name(channel_addr, proc_name), actor_name), port as u64)),
                }?)
            }

            None => Err(ReferenceParsingError::Unexpected(format!(
                "expected a comma-separated reference, got: {}",
                addr
            ))),
        }
    }
}

impl From<ProcId> for Reference {
    fn from(proc_id: ProcId) -> Self {
        Self::Proc(proc_id)
    }
}

impl From<ActorId> for Reference {
    fn from(actor_id: ActorId) -> Self {
        Self::Actor(actor_id)
    }
}

impl From<PortId> for Reference {
    fn from(port_id: PortId) -> Self {
        Self::Port(port_id)
    }
}

/// Index is a type alias representing a value that can be used as an index
/// into a sequence.
pub type Index = usize;

/// Parse a name in ResourceId format into a (Uid, Option<Label>) pair.
///
/// Formats: `_label` (singleton), `label-hex16` (labeled instance),
/// `hex16` (unlabeled instance). Falls back to singleton from stripped name.
fn parse_resource_name(s: &str) -> (Uid, Option<Label>) {
    // Singleton: _label
    if let Some(rest) = s.strip_prefix('_') {
        if let Ok(label) = Label::new(rest) {
            return (Uid::Singleton(label.clone()), Some(label));
        }
    }

    // Labeled instance: label-hex16 (exactly 16 hex digits after the last dash
    // at position len-17).
    if s.len() >= 18 {
        let dash_pos = s.len() - 17;
        if s.as_bytes()[dash_pos] == b'-' {
            let hex_part = &s[dash_pos + 1..];
            let label_part = &s[..dash_pos];
            if hex_part.len() == 16 && hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
                if let (Ok(uid), Ok(label)) =
                    (u64::from_str_radix(hex_part, 16), Label::new(label_part))
                {
                    return (Uid::Instance(uid), Some(label));
                }
            }
        }
    }

    // Unlabeled instance: bare hex16
    if s.len() <= 16 && !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(uid) = u64::from_str_radix(s, 16) {
            return (Uid::Instance(uid), None);
        }
    }

    // Fallback: singleton from stripped name
    let label = Label::strip(s);
    (Uid::Singleton(label.clone()), Some(label))
}

/// Procs are identified by a process reference, which pairs a unique identity
/// with a network location.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    typeuri::Named
)]
#[serde(transparent)]
pub struct ProcId(ref_::ProcRef);

impl ProcId {
    /// Create a ProcId with a unique (random) uid and the given label.
    pub fn unique(addr: ChannelAddr, base_name: impl AsRef<str>) -> Self {
        let label = Label::strip(base_name.as_ref());
        Self(ref_::ProcRef::new(
            crate::id::ProcId::instance(label),
            ref_::Location::from(addr),
        ))
    }

    /// Create a ProcId by parsing a name string in ResourceId format.
    ///
    /// Recognizes three formats:
    /// - `_label` → singleton uid
    /// - `label-hex16` → labeled instance uid (hex16 = exactly 16 hex digits)
    /// - `hex16` → unlabeled instance uid
    ///
    /// Falls back to `Uid::Singleton(Label::strip(name))` if none match.
    pub fn with_name(addr: ChannelAddr, name: impl AsRef<str>) -> Self {
        let s = name.as_ref();
        let (uid, label) = parse_resource_name(s);
        Self(ref_::ProcRef::new(
            crate::id::ProcId::new(uid, label),
            ref_::Location::from(addr),
        ))
    }

    /// Wrap an existing [`ref_::ProcRef`].
    pub fn from_proc_ref(proc_ref: ref_::ProcRef) -> Self {
        Self(proc_ref)
    }

    /// Create an actor ID with the provided name within this proc.
    pub fn actor_id(&self, name: impl AsRef<str>) -> ActorId {
        ActorId::new(self.clone(), name)
    }

    /// The proc's channel address.
    pub fn addr(&self) -> &ChannelAddr {
        self.0.location().addr()
    }

    /// The underlying process identity.
    pub fn id(&self) -> &crate::id::ProcId {
        self.0.id()
    }

    /// The underlying process reference.
    pub fn proc_ref(&self) -> &ref_::ProcRef {
        &self.0
    }

    /// The proc's uid.
    pub fn uid(&self) -> &Uid {
        self.0.id().uid()
    }

    /// The proc's label.
    pub fn label(&self) -> Option<&Label> {
        self.0.id().label()
    }

    /// A human-readable name for logging, derived from the label or uid.
    pub fn log_name(&self) -> &str {
        self.0.id().label().map(|l| l.as_str()).unwrap_or("?")
    }

    /// The ResourceId text form of this proc's identity.
    ///
    /// Produces `_label` (singleton), `label-hex16` (labeled instance),
    /// or `hex16` (unlabeled instance). Suitable for filesystem paths
    /// and string-based lookups that must round-trip through
    /// `parse_resource_name`.
    pub fn resource_name(&self) -> String {
        let id = self.0.id();
        match (id.uid(), id.label()) {
            (Uid::Singleton(label), _) => format!("_{}", label),
            (Uid::Instance(uid), Some(label)) => format!("{}-{:016x}", label, uid),
            (Uid::Instance(uid), None) => format!("{:016x}", uid),
        }
    }
}

impl fmt::Display for ProcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Compat format: "addr,resource_name" where resource_name is the
        // ResourceId text form: _label | label-hex16 | hex16.
        let id = self.0.id();
        match (id.uid(), id.label()) {
            (Uid::Singleton(label), _) => write!(f, "{},_{}", self.0.location().addr(), label),
            (Uid::Instance(uid), Some(label)) => {
                write!(f, "{},{}-{:016x}", self.0.location().addr(), label, uid)
            }
            (Uid::Instance(uid), None) => {
                write!(f, "{},{:016x}", self.0.location().addr(), uid)
            }
        }
    }
}

impl FromStr for ProcId {
    type Err = ReferenceParsingError;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.parse()? {
            Reference::Proc(proc_id) => Ok(proc_id),
            _ => Err(ReferenceParsingError::WrongType("proc".into())),
        }
    }
}

/// Actors are identified by a typed actor reference pairing an identity
/// with a network location.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    typeuri::Named
)]
#[serde(transparent)]
pub struct ActorId(ref_::ActorRef);

hyperactor_config::impl_attrvalue!(ActorId);

impl ActorId {
    /// Create a new actor ID from a proc and name string.
    ///
    /// Parses the name in ResourceId format to recover the uid deterministically.
    /// Child actors with random uids are created via [`Self::unique_child_id`].
    pub fn new(proc_id: ProcId, name: impl AsRef<str>) -> Self {
        let s = name.as_ref();
        let (uid, label) = parse_resource_name(s);
        let actor_id = crate::id::ActorId::new(uid, proc_id.0.id().clone(), label);
        Self(ref_::ActorRef::new(actor_id, proc_id.0.location().clone()))
    }

    /// Create a new port ID with the provided port for this actor.
    pub fn port_id(&self, port: u64) -> PortId {
        let port_id = crate::id::PortId::new(self.0.id().clone(), crate::port::Port::from(port));
        PortId(ref_::PortRef::new(port_id, self.0.location().clone()))
    }

    /// Create a child actor ID with a random uid.
    pub fn unique_child_id(&self) -> Self {
        let child = crate::id::ActorId::instance(self.0.id().proc_id().clone());
        Self(ref_::ActorRef::new(child, self.0.location().clone()))
    }

    /// Return the root actor ID for the provided proc and label.
    pub fn root(proc_id: ProcId, label: Label) -> Self {
        let actor_id = crate::id::ActorId::singleton(label, proc_id.0.id().clone());
        Self(ref_::ActorRef::new(actor_id, proc_id.0.location().clone()))
    }

    /// Wrap an existing [`ref_::ActorRef`].
    pub fn from_actor_ref(actor_ref: ref_::ActorRef) -> Self {
        Self(actor_ref)
    }

    /// The proc ID of this actor ID.
    pub fn proc_id(&self) -> ProcId {
        ProcId(ref_::ProcRef::new(
            self.0.id().proc_id().clone(),
            self.0.location().clone(),
        ))
    }

    /// The underlying actor identity.
    pub fn id(&self) -> &crate::id::ActorId {
        self.0.id()
    }

    /// The underlying actor reference.
    pub fn actor_ref(&self) -> &ref_::ActorRef {
        &self.0
    }

    /// The actor's uid.
    pub fn uid(&self) -> &Uid {
        self.0.id().uid()
    }

    /// The actor's label.
    pub fn label(&self) -> Option<&Label> {
        self.0.id().label()
    }

    /// The actor's network address (same as proc's).
    pub fn addr(&self) -> &ChannelAddr {
        self.0.location().addr()
    }

    /// Whether this is a root (singleton) actor.
    pub fn is_root(&self) -> bool {
        matches!(self.0.id().uid(), Uid::Singleton(_))
    }

    /// A human-readable name for logging, derived from the label or uid.
    pub fn log_name(&self) -> &str {
        self.0.id().label().map(|l| l.as_str()).unwrap_or("?")
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Compat format: "proc_display,actor_resource_name"
        let id = self.0.id();
        let proc_id = self.proc_id();
        match (id.uid(), id.label()) {
            (Uid::Singleton(label), _) => write!(f, "{},_{}", proc_id, label),
            (Uid::Instance(uid), Some(label)) => {
                write!(f, "{},{}-{:016x}", proc_id, label, uid)
            }
            (Uid::Instance(uid), None) => {
                write!(f, "{},{:016x}", proc_id, uid)
            }
        }
    }
}
impl<A: Referable> From<ActorRef<A>> for ActorId {
    fn from(actor_ref: ActorRef<A>) -> Self {
        actor_ref.actor_id.clone()
    }
}

impl<'a, A: Referable> From<&'a ActorRef<A>> for &'a ActorId {
    fn from(actor_ref: &'a ActorRef<A>) -> Self {
        &actor_ref.actor_id
    }
}

impl FromStr for ActorId {
    type Err = ReferenceParsingError;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.parse()? {
            Reference::Actor(actor_id) => Ok(actor_id),
            _ => Err(ReferenceParsingError::WrongType("actor".into())),
        }
    }
}

/// ActorRefs are typed references to actors.
#[derive(typeuri::Named)]
pub struct ActorRef<A: Referable> {
    pub(crate) actor_id: ActorId,
    // fn() -> A so that the struct remains Send
    phantom: PhantomData<fn() -> A>,
}

impl<A: Referable> ActorRef<A> {
    /// Get the remote port for message type [`M`] for the referenced actor.
    pub fn port<M: RemoteMessage>(&self) -> PortRef<M>
    where
        A: RemoteHandles<M>,
    {
        PortRef::attest(self.actor_id.port_id(<M as Named>::port()))
    }

    /// Send an [`M`]-typed message to the referenced actor.
    pub fn send<M: RemoteMessage>(
        &self,
        cx: &impl context::Actor,
        message: M,
    ) -> Result<(), MailboxSenderError>
    where
        A: RemoteHandles<M>,
    {
        self.port().send(cx, message)
    }

    /// Send an [`M`]-typed message to the referenced actor, with additional context provided by
    /// headers.
    pub fn send_with_headers<M: RemoteMessage>(
        &self,
        cx: &impl context::Actor,
        headers: Flattrs,
        message: M,
    ) -> Result<(), MailboxSenderError>
    where
        A: RemoteHandles<M>,
    {
        self.port().send_with_headers(cx, headers, message)
    }

    /// The caller guarantees that the provided actor ID is also a valid,
    /// typed reference.  This is usually invoked to provide a guarantee
    /// that an externally-provided actor ID (e.g., through a command
    /// line argument) is a valid reference.
    pub fn attest(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            phantom: PhantomData,
        }
    }

    /// The actor ID corresponding with this reference.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }

    /// Convert this actor reference into its corresponding actor ID.
    pub fn into_actor_id(self) -> ActorId {
        self.actor_id
    }

    /// Attempt to downcast this reference into a (local) actor handle.
    /// This will only succeed when the referenced actor is in the same
    /// proc as the caller.
    pub fn downcast_handle(&self, cx: &impl context::Actor) -> Option<ActorHandle<A>>
    where
        A: Actor,
    {
        cx.instance().proc().resolve_actor_ref(self)
    }
}

// Implement Serialize manually, without requiring A: Serialize
impl<A: Referable> Serialize for ActorRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize only the fields that don't depend on A
        self.actor_id().serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize
impl<'de, A: Referable> Deserialize<'de> for ActorRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let actor_id = <ActorId>::deserialize(deserializer)?;
        Ok(ActorRef {
            actor_id,
            phantom: PhantomData,
        })
    }
}

// Implement Debug manually, without requiring A: Debug
impl<A: Referable> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("actor_id", &self.actor_id)
            .field("type", &std::any::type_name::<A>())
            .finish()
    }
}

impl<A: Referable> fmt::Display for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.actor_id, f)?;
        write!(f, "<{}>", std::any::type_name::<A>())
    }
}

// We implement Clone manually to avoid imposing A: Clone.
impl<A: Referable> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            actor_id: self.actor_id.clone(),
            phantom: PhantomData,
        }
    }
}

impl<A: Referable> PartialEq for ActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.actor_id == other.actor_id
    }
}

impl<A: Referable> Eq for ActorRef<A> {}

impl<A: Referable> PartialOrd for ActorRef<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Referable> Ord for ActorRef<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.actor_id.cmp(&other.actor_id)
    }
}

impl<A: Referable> Hash for ActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.actor_id.hash(state);
    }
}

/// Port ids identify [`crate::mailbox::Port`]s of an actor.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    typeuri::Named
)]
#[serde(transparent)]
pub struct PortId(ref_::PortRef);

impl PortId {
    /// Create a new port ID.
    pub fn new(actor_id: ActorId, port: u64) -> Self {
        let port_id =
            crate::id::PortId::new(actor_id.0.id().clone(), crate::port::Port::from(port));
        Self(ref_::PortRef::new(port_id, actor_id.0.location().clone()))
    }

    /// The ID of the port's owning actor.
    pub fn actor_id(&self) -> ActorId {
        ActorId(ref_::ActorRef::new(
            self.0.id().actor_id().clone(),
            self.0.location().clone(),
        ))
    }

    /// Convert this port ID into an actor ID.
    pub fn into_actor_id(self) -> ActorId {
        self.actor_id()
    }

    /// This port's index.
    pub fn index(&self) -> u64 {
        self.0.id().port().as_u64()
    }

    pub(crate) fn is_actor_port(&self) -> bool {
        self.0.id().port().is_handler()
    }

    /// Send a serialized message to this port, provided a sending capability,
    /// such as [`crate::actor::Instance`]. It is the sender's responsibility
    /// to ensure that the provided message is well-typed.
    pub fn send(&self, cx: &impl context::Actor, serialized: wirevalue::Any) {
        let mut headers = Flattrs::new();
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        cx.post(
            self.clone(),
            headers,
            serialized,
            true,
            context::SeqInfoPolicy::AssignNew,
        );
    }

    /// Send a serialized message to this port, provided a sending capability,
    /// such as [`crate::actor::Instance`], with additional context provided by headers.
    /// It is the sender's responsibility to ensure that the provided message is well-typed.
    pub fn send_with_headers(
        &self,
        cx: &impl context::Actor,
        serialized: wirevalue::Any,
        mut headers: Flattrs,
    ) {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        cx.post(
            self.clone(),
            headers,
            serialized,
            true,
            context::SeqInfoPolicy::AssignNew,
        );
    }

    /// Split this port, returning a new port that relays messages to the port
    /// through a local proxy, which may coalesce messages.
    pub fn split(
        &self,
        cx: &impl context::Actor,
        reducer_spec: Option<ReducerSpec>,
        reducer_mode: ReducerMode,
        return_undeliverable: bool,
    ) -> anyhow::Result<PortId> {
        cx.split(
            self.clone(),
            reducer_spec,
            reducer_mode,
            return_undeliverable,
        )
    }
}

impl FromStr for PortId {
    type Err = ReferenceParsingError;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.parse()? {
            Reference::Port(port_id) => Ok(port_id),
            _ => Err(ReferenceParsingError::WrongType("port".into())),
        }
    }
}

impl fmt::Display for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let actor_id = self.actor_id();
        let port = self.index();
        if self.is_actor_port() {
            let type_info = TypeInfo::get(port).or_else(|| TypeInfo::get(port & !ACTOR_PORT_BIT));
            let typename = type_info.map_or("unknown", TypeInfo::typename);
            write!(f, "{}[{}<{}>]", actor_id, port, typename)
        } else {
            write!(f, "{}[{}]", actor_id, port)
        }
    }
}

/// A reference to a remote port. All messages passed through
/// PortRefs will be serialized. PortRefs are always streaming.
#[derive(Debug, Serialize, Deserialize, Derivative, typeuri::Named)]
#[derivative(PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct PortRef<M> {
    port_id: PortId,
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    reducer_spec: Option<ReducerSpec>,
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    streaming_opts: StreamingReducerOpts,
    phantom: PhantomData<M>,
    return_undeliverable: bool,
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    unsplit: bool,
}

impl<M: RemoteMessage> PortRef<M> {
    /// The caller attests that the provided PortId can be
    /// converted to a reachable, typed port reference.
    pub fn attest(port_id: PortId) -> Self {
        Self {
            port_id,
            reducer_spec: None,
            streaming_opts: StreamingReducerOpts::default(),
            phantom: PhantomData,
            return_undeliverable: true,
            unsplit: false,
        }
    }

    /// The caller attests that the provided PortId can be
    /// converted to a reachable, typed port reference.
    pub fn attest_reducible(
        port_id: PortId,
        reducer_spec: Option<ReducerSpec>,
        streaming_opts: StreamingReducerOpts,
    ) -> Self {
        Self {
            port_id,
            reducer_spec,
            streaming_opts,
            phantom: PhantomData,
            return_undeliverable: true,
            unsplit: false,
        }
    }

    /// Prevents the port from being split.
    pub fn unsplit(mut self) -> Self {
        self.unsplit = true;
        self
    }

    /// The caller attests that the provided PortId can be
    /// converted to a reachable, typed port reference.
    pub fn attest_message_port(actor: &ActorId) -> Self {
        PortRef::<M>::attest(actor.port_id(<M as Named>::port()))
    }

    /// The typehash of this port's reducer, if any. Reducers
    /// may be used to coalesce messages sent to a port.
    pub fn reducer_spec(&self) -> &Option<ReducerSpec> {
        &self.reducer_spec
    }

    /// This port's ID.
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Convert this PortRef into its corresponding port id.
    pub fn into_port_id(self) -> PortId {
        self.port_id
    }

    /// coerce it into OncePortRef so we can send messages to this port from
    /// APIs requires OncePortRef.
    pub fn into_once(self) -> OncePortRef<M> {
        let return_undeliverable = self.return_undeliverable;
        let unsplit = self.unsplit;
        let mut once = OncePortRef::attest(self.into_port_id());
        once.return_undeliverable = return_undeliverable;
        once.unsplit = unsplit;
        once
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`].
    pub fn send(&self, cx: &impl context::Actor, message: M) -> Result<(), MailboxSenderError> {
        self.send_with_headers(cx, Flattrs::new(), message)
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`]. Additional context can be provided in the form of
    /// headers.
    pub fn send_with_headers(
        &self,
        cx: &impl context::Actor,
        headers: Flattrs,
        message: M,
    ) -> Result<(), MailboxSenderError> {
        let serialized = wirevalue::Any::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                self.port_id.clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        })?;
        self.send_serialized(cx, headers, serialized);
        Ok(())
    }

    /// Send a serialized message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`].
    pub fn send_serialized(
        &self,
        cx: &impl context::Actor,
        mut headers: Flattrs,
        message: wirevalue::Any,
    ) {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        crate::mailbox::headers::set_rust_message_type::<M>(&mut headers);
        cx.post(
            self.port_id.clone(),
            headers,
            message,
            self.return_undeliverable,
            context::SeqInfoPolicy::AssignNew,
        );
    }

    /// Convert this port into a sink that can be used to send messages using the given capability.
    pub fn into_sink<C: context::Actor>(self, cx: C) -> PortSink<C, M> {
        PortSink::new(cx, self)
    }

    /// Get whether or not messages sent to this port that are undeliverable should
    /// be returned to the sender.
    pub fn get_return_undeliverable(&self) -> bool {
        self.return_undeliverable
    }

    /// Set whether or not messages sent to this port that are undeliverable
    /// should be returned to the sender.
    pub fn return_undeliverable(&mut self, return_undeliverable: bool) {
        self.return_undeliverable = return_undeliverable;
    }
}

impl<M: RemoteMessage> Clone for PortRef<M> {
    fn clone(&self) -> Self {
        Self {
            port_id: self.port_id.clone(),
            reducer_spec: self.reducer_spec.clone(),
            streaming_opts: self.streaming_opts.clone(),
            phantom: PhantomData,
            return_undeliverable: self.return_undeliverable,
            unsplit: self.unsplit,
        }
    }
}

impl<M: RemoteMessage> fmt::Display for PortRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.port_id, f)
    }
}

/// The kind of unbound port.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Named)]
pub enum UnboundPortKind {
    /// A streaming port, which should be reduced with the provided options.
    Streaming(Option<StreamingReducerOpts>),
    /// A OncePort, which must be one-shot aggregated.
    Once,
}

/// The parameters extracted from [`PortRef`] to [`Bindings`].
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, typeuri::Named)]
pub struct UnboundPort(
    pub PortId,
    pub Option<ReducerSpec>,
    pub bool, // return_undeliverable
    pub UnboundPortKind,
    pub bool, // unsplit
);
wirevalue::register_type!(UnboundPort);

impl UnboundPort {
    /// Update the port id of this binding.
    pub fn update(&mut self, port_id: PortId) {
        self.0 = port_id;
    }
}

impl<M: RemoteMessage> From<&PortRef<M>> for UnboundPort {
    fn from(port_ref: &PortRef<M>) -> Self {
        UnboundPort(
            port_ref.port_id.clone(),
            port_ref.reducer_spec.clone(),
            port_ref.return_undeliverable,
            UnboundPortKind::Streaming(Some(port_ref.streaming_opts.clone())),
            port_ref.unsplit,
        )
    }
}

impl<M: RemoteMessage> Unbind for PortRef<M> {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        bindings.push_back(&UnboundPort::from(self))
    }
}

impl<M: RemoteMessage> Bind for PortRef<M> {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        let UnboundPort(port_id, reducer_spec, return_undeliverable, port_kind, unsplit) =
            bindings.try_pop_front::<UnboundPort>()?;
        self.port_id = port_id;
        self.reducer_spec = reducer_spec;
        self.return_undeliverable = return_undeliverable;
        self.unsplit = unsplit;
        self.streaming_opts = match port_kind {
            UnboundPortKind::Streaming(opts) => opts.unwrap_or_default(),
            UnboundPortKind::Once => {
                anyhow::bail!("OncePortRef cannot be bound to PortRef")
            }
        };
        Ok(())
    }
}

/// A remote reference to a [`OncePort`]. References are serializable
/// and may be passed to remote actors, which can then use it to send
/// a message to this port.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct OncePortRef<M> {
    port_id: PortId,
    reducer_spec: Option<ReducerSpec>,
    return_undeliverable: bool,
    unsplit: bool,
    phantom: PhantomData<M>,
}

impl<M: RemoteMessage> OncePortRef<M> {
    pub(crate) fn attest(port_id: PortId) -> Self {
        Self {
            port_id,
            reducer_spec: None,
            return_undeliverable: true,
            unsplit: false,
            phantom: PhantomData,
        }
    }

    /// The caller attests that the provided PortId can be
    /// converted to a reachable, typed once port reference.
    pub fn attest_reducible(port_id: PortId, reducer_spec: Option<ReducerSpec>) -> Self {
        Self {
            port_id,
            reducer_spec,
            return_undeliverable: true,
            unsplit: false,
            phantom: PhantomData,
        }
    }

    /// Prevents the port from being split.
    pub fn unsplit(mut self) -> Self {
        self.unsplit = true;
        self
    }

    /// The typehash of this port's reducer, if any.
    pub fn reducer_spec(&self) -> &Option<ReducerSpec> {
        &self.reducer_spec
    }

    /// This port's ID.
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Convert this PortRef into its corresponding port id.
    pub fn into_port_id(self) -> PortId {
        self.port_id
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`].
    pub fn send(self, cx: &impl context::Actor, message: M) -> Result<(), MailboxSenderError> {
        self.send_with_headers(cx, Flattrs::new(), message)
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`]. Additional context can be provided in the form of headers.
    pub fn send_with_headers(
        self,
        cx: &impl context::Actor,
        mut headers: Flattrs,
        message: M,
    ) -> Result<(), MailboxSenderError> {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        let serialized = wirevalue::Any::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                self.port_id.clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        })?;
        cx.post(
            self.port_id.clone(),
            headers,
            serialized,
            self.return_undeliverable,
            context::SeqInfoPolicy::AssignNew,
        );
        Ok(())
    }

    /// Get whether or not messages sent to this port that are undeliverable should
    /// be returned to the sender.
    pub fn get_return_undeliverable(&self) -> bool {
        self.return_undeliverable
    }

    /// Set whether or not messages sent to this port that are undeliverable
    /// should be returned to the sender.
    pub fn return_undeliverable(&mut self, return_undeliverable: bool) {
        self.return_undeliverable = return_undeliverable;
    }
}

impl<M: RemoteMessage> Clone for OncePortRef<M> {
    fn clone(&self) -> Self {
        Self {
            port_id: self.port_id.clone(),
            reducer_spec: self.reducer_spec.clone(),
            return_undeliverable: self.return_undeliverable,
            unsplit: self.unsplit,
            phantom: PhantomData,
        }
    }
}

impl<M: RemoteMessage> fmt::Display for OncePortRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.port_id, f)
    }
}

impl<M: RemoteMessage> Named for OncePortRef<M> {
    fn typename() -> &'static str {
        wirevalue::intern_typename!(Self, "hyperactor::mailbox::OncePortRef<{}>", M)
    }
}

impl<M: RemoteMessage> From<&OncePortRef<M>> for UnboundPort {
    fn from(port_ref: &OncePortRef<M>) -> Self {
        UnboundPort(
            port_ref.port_id.clone(),
            port_ref.reducer_spec.clone(),
            true, // return_undeliverable
            UnboundPortKind::Once,
            port_ref.unsplit,
        )
    }
}

impl<M: RemoteMessage> Unbind for OncePortRef<M> {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        bindings.push_back(&UnboundPort::from(self))
    }
}

impl<M: RemoteMessage> Bind for OncePortRef<M> {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        let UnboundPort(port_id, reducer_spec, _return_undeliverable, port_kind, unsplit) =
            bindings.try_pop_front::<UnboundPort>()?;
        match port_kind {
            UnboundPortKind::Once => {
                self.port_id = port_id;
                self.reducer_spec = reducer_spec;
                self.unsplit = unsplit;
                Ok(())
            }
            UnboundPortKind::Streaming(_) => {
                anyhow::bail!("PortRef cannot be bound to OncePortRef")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;
    // for macros
    use crate::Proc;
    use crate::context::Mailbox as _;
    use crate::mailbox::PortLocation;
    use crate::ordering::SEQ_INFO;
    use crate::ordering::SeqInfo;

    #[test]
    fn test_reference_parse() {
        let cases: Vec<(&str, Reference)> = vec![
            (
                "tcp:[::1]:1234,test",
                ProcId::with_name("tcp:[::1]:1234".parse::<ChannelAddr>().unwrap(), "test").into(),
            ),
            (
                "tcp:[::1]:1234,test,testactor",
                ActorId::new(
                    ProcId::with_name("tcp:[::1]:1234".parse::<ChannelAddr>().unwrap(), "test"),
                    "testactor",
                )
                .into(),
            ),
            (
                // instance actor (label-hex16 format)
                "tcp:[::1]:1234,test,myactor-00000000deadbeef",
                ActorId::new(
                    ProcId::with_name("tcp:[::1]:1234".parse::<ChannelAddr>().unwrap(), "test"),
                    "myactor-00000000deadbeef",
                )
                .into(),
            ),
            (
                // one bracket group = port (type annotations ignored)
                "tcp:[::1]:1234,test,testactor[123<my::type>]",
                PortId::new(
                    ActorId::new(
                        ProcId::with_name("tcp:[::1]:1234".parse::<ChannelAddr>().unwrap(), "test"),
                        "testactor",
                    ),
                    123,
                )
                .into(),
            ),
        ];

        for (s, expected) in cases {
            assert_eq!(s.parse::<Reference>().unwrap(), expected, "for {}", s);
        }
    }

    #[test]
    fn test_reference_parse_error() {
        let cases: Vec<&str> = vec!["(blah)", "world(1, 2, 3)", "test"];

        for s in cases {
            let result: Result<Reference, ReferenceParsingError> = s.parse();
            assert!(result.is_err(), "expected error for: {}", s);
        }
    }

    #[test]
    fn test_reference_ord() {
        let expected: Vec<Reference> = [
            "tcp:[::1]:1234,first",
            "tcp:[::1]:1234,second",
            "tcp:[::1]:1234,third",
        ]
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();

        let mut sorted = expected.to_vec();
        sorted.shuffle(&mut rand::rng());
        sorted.sort();

        assert_eq!(sorted, expected);
    }

    #[test]
    fn test_port_type_annotation() {
        #[derive(typeuri::Named, Serialize, Deserialize)]
        struct MyType;
        wirevalue::register_type!(MyType);
        let port_id = PortId::new(
            ActorId::new(
                ProcId::with_name("tcp:[::1]:1234".parse::<ChannelAddr>().unwrap(), "test"),
                "testactor",
            ),
            MyType::port(),
        );
        let s = port_id.to_string();
        assert!(
            s.starts_with("tcp:[::1]:1234,_test,_testactor["),
            "unexpected format: {}",
            s
        );
        assert!(
            s.contains("<hyperactor::reference::tests::MyType>"),
            "missing type annotation: {}",
            s
        );
    }

    #[tokio::test]
    async fn test_sequencing_from_port_handle_ref_and_id() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let port_handle = client.mailbox().open_enqueue_port(move |headers, _m: ()| {
            let seq_info = headers.get(SEQ_INFO);
            tx.send(seq_info).unwrap();
            Ok(())
        });
        port_handle.send(&client, ()).unwrap();
        // Unordered is set for unbound port handle since handler's ordered
        // channel is expecting the SEQ_INFO header to be set.
        assert_eq!(rx.try_recv().unwrap().unwrap(), SeqInfo::Direct);

        port_handle.bind_actor_port();
        let port_id = match port_handle.location() {
            PortLocation::Bound(port_id) => port_id,
            _ => panic!("port_handle should be bound"),
        };
        assert!(port_id.is_actor_port());
        let port_ref = PortRef::attest(port_id.clone());

        port_handle.send(&client, ()).unwrap();
        let SeqInfo::Session {
            session_id,
            mut seq,
        } = rx.try_recv().unwrap().unwrap()
        else {
            panic!("expected session info");
        };
        assert_eq!(session_id, client.sequencer().session_id());
        assert_eq!(seq, 1);

        fn assert_seq_info(
            rx: &mut mpsc::UnboundedReceiver<Option<SeqInfo>>,
            session_id: Uuid,
            seq: &mut u64,
        ) {
            *seq += 1;
            let SeqInfo::Session {
                session_id: rcved_session_id,
                seq: rcved_seq,
            } = rx.try_recv().unwrap().unwrap()
            else {
                panic!("expected session info");
            };
            assert_eq!(rcved_session_id, session_id);
            assert_eq!(rcved_seq, *seq);
        }

        // Interleave sends from port_handle, port_ref, and port_id
        for _ in 0..10 {
            // From port_handle
            port_handle.send(&client, ()).unwrap();
            assert_seq_info(&mut rx, session_id, &mut seq);

            // From port_ref
            for _ in 0..2 {
                port_ref.send(&client, ()).unwrap();
                assert_seq_info(&mut rx, session_id, &mut seq);
            }

            // From port_id
            for _ in 0..3 {
                port_id.send(&client, wirevalue::Any::serialize(&()).unwrap());
                assert_seq_info(&mut rx, session_id, &mut seq);
            }
        }

        assert_eq!(rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn test_sequencing_from_port_handle_bound_to_allocated_port() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let port_handle = client.mailbox().open_enqueue_port(move |headers, _m: ()| {
            let seq_info = headers.get(SEQ_INFO);
            tx.send(seq_info).unwrap();
            Ok(())
        });
        port_handle.send(&client, ()).unwrap();
        // Unordered be set for unbound port handle since handler's ordered
        // channel is expecting the SEQ_INFO header to be set.
        assert_eq!(rx.try_recv().unwrap().unwrap(), SeqInfo::Direct);

        // Bind to the allocated port.
        port_handle.bind();
        let port_id = match port_handle.location() {
            PortLocation::Bound(port_id) => port_id,
            _ => panic!("port_handle should be bound"),
        };
        // This is a non-actor port, but it still gets seq info (per-port sequence).
        assert!(!port_id.is_actor_port());

        // After binding, non-actor ports get their own sequence.
        port_handle.send(&client, ()).unwrap();
        let SeqInfo::Session {
            session_id,
            seq: seq1,
        } = rx
            .try_recv()
            .unwrap()
            .expect("non-actor port should have seq info")
        else {
            panic!("expected Session variant");
        };
        assert_eq!(seq1, 1);
        assert_eq!(session_id, client.sequencer().session_id());

        let port_ref = PortRef::attest(port_id.clone());
        port_ref.send(&client, ()).unwrap();
        let SeqInfo::Session { seq: seq2, .. } = rx
            .try_recv()
            .unwrap()
            .expect("non-actor port should have seq info")
        else {
            panic!("expected Session variant");
        };
        assert_eq!(seq2, 2);

        port_id.send(&client, wirevalue::Any::serialize(&()).unwrap());
        let SeqInfo::Session { seq: seq3, .. } = rx
            .try_recv()
            .unwrap()
            .expect("non-actor port should have seq info")
        else {
            panic!("expected Session variant");
        };
        assert_eq!(seq3, 3);
    }
}
