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
use hyperactor_config::attrs::Attrs;
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
use crate::mailbox::MailboxSenderError;
use crate::mailbox::MailboxSenderErrorKind;
use crate::mailbox::PortSink;
use crate::message::Bind;
use crate::message::Bindings;
use crate::message::Unbind;

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
    /// World references.
    World,
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
/// - `world`,
/// - `addr,proc_name`,
/// - `addr,proc_name,actor_name[pid]`,
/// - `addr,proc_name,actor_name[pid][port]`
///
/// Reference also implements a total ordering, so that references are
/// ordered lexicographically with the hierarchy implied by world, proc,
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
    /// A reference to a world.
    World(WorldId),
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
            Self::World(_) => self.world_id() == other.world_id(),
            Self::Proc(_) => self.proc_id() == other.proc_id(),
            Self::Actor(_) => self == other,
            Self::Port(_) => self == other,
        }
    }

    /// The world id of the reference (only for World variants).
    pub fn world_id(&self) -> Option<&WorldId> {
        match self {
            Self::World(world_id) => Some(world_id),
            Self::Proc(_) => None,
            Self::Actor(_) => None,
            Self::Port(_) => None,
        }
    }

    /// The proc id of the reference, if any.
    pub fn proc_id(&self) -> Option<&ProcId> {
        match self {
            Self::World(_) => None,
            Self::Proc(proc_id) => Some(proc_id),
            Self::Actor(ActorId(proc_id, _, _)) => Some(proc_id),
            Self::Port(PortId(ActorId(proc_id, _, _), _)) => Some(proc_id),
        }
    }

    /// The actor id of the reference, if any.
    pub fn actor_id(&self) -> Option<&ActorId> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id),
            Self::Port(PortId(actor_id, _)) => Some(actor_id),
        }
    }

    /// The actor name of the reference, if any.
    fn actor_name(&self) -> Option<&str> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.name()),
            Self::Port(PortId(actor_id, _)) => Some(actor_id.name()),
        }
    }

    /// The pid of the reference, if any.
    fn pid(&self) -> Option<Index> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.pid()),
            Self::Port(PortId(actor_id, _)) => Some(actor_id.pid()),
        }
    }

    /// The port of the reference, if any.
    fn port(&self) -> Option<u64> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(_) => None,
            Self::Port(port_id) => Some(port_id.index()),
        }
    }

    /// Returns the kind of the reference.
    pub fn kind(&self) -> ReferenceKind {
        match self {
            Self::World(_) => ReferenceKind::World,
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
        // Order by: world_id (if World), then proc address/name, then actor_name, then pid, then port
        (
            self.world_id(),
            self.proc_id().map(|p| (p.addr(), p.name())),
            self.actor_name(),
            self.pid(),
            self.port(),
        )
            .cmp(&(
                other.world_id(),
                other.proc_id().map(|p| (p.addr(), p.name())),
                other.actor_name(),
                other.pid(),
                other.port(),
            ))
    }
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::World(world_id) => fmt::Display::fmt(world_id, f),
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
        // 1) If the reference contains a comma (anywhere), it is a proc/actor/port reference
        // 2) Otherwise, it's a world reference (just a simple name)

        match addr.split_once(",") {
            Some((channel_addr, rest)) => {
                let channel_addr = channel_addr.parse().map_err(|err| {
                    ReferenceParsingError::InvalidChannelAddress(channel_addr.to_string(), err)
                })?;

                Ok(parse! {
                    Lexer::new(rest);

                    // channeladdr,proc_name
                    Token::Elem(proc_name) =>
                    Self::Proc(ProcId(channel_addr, proc_name.to_string())),

                    // channeladdr,proc_name,actor_name
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name) =>
                    Self::Actor(ActorId(ProcId(channel_addr, proc_name.to_string()), actor_name.to_string(), 0)),

                    // channeladdr,proc_name,actor_name[pid]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket =>
                        Self::Actor(ActorId(ProcId(channel_addr, proc_name.to_string()), actor_name.to_string(), pid)),

                    // channeladdr,proc_name,actor_name[pid][port]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket
                        Token::LeftBracket Token::Uint(index) Token::RightBracket  =>
                        Self::Port(PortId(ActorId(ProcId(channel_addr, proc_name.to_string()), actor_name.to_string(), pid), index as u64)),

                    // channeladdr,proc_name,actor_name[pid][port<type>]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket
                        Token::LeftBracket Token::Uint(index)
                            Token::LessThan Token::Elem(_type) Token::GreaterThan
                        Token::RightBracket =>
                        Self::Port(PortId(ActorId(ProcId(channel_addr, proc_name.to_string()), actor_name.to_string(), pid), index as u64)),
                }?)
            }

            // World reference (just a simple name)
            None => {
                Ok(parse! {
                    Lexer::new(addr);

                    // world
                    Token::Elem(world) => Self::World(WorldId(world.into())),
                }?)
            }
        }
    }
}

impl From<WorldId> for Reference {
    fn from(world_id: WorldId) -> Self {
        Self::World(world_id)
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

/// WorldId identifies a world within a [`crate::system::System`].
/// The index of a World uniquely identifies it within a system instance.
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
pub struct WorldId(pub String);

impl WorldId {
    /// The world name.
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorldId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let WorldId(name) = self;
        write!(f, "{}", name)
    }
}

impl FromStr for WorldId {
    type Err = ReferenceParsingError;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.parse()? {
            Reference::World(world_id) => Ok(world_id),
            _ => Err(ReferenceParsingError::WrongType("world".into())),
        }
    }
}

/// Procs are identified by a direct channel address and local name.
/// Each proc represents an actor runtime that can locally route to all of its
/// constituent actors.
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
pub struct ProcId(pub ChannelAddr, pub String);

impl ProcId {
    /// Create an actor ID with the provided name, pid within this proc.
    pub fn actor_id(&self, name: impl Into<String>, pid: Index) -> ActorId {
        ActorId(self.clone(), name.into(), pid)
    }

    /// The proc's channel address.
    pub fn addr(&self) -> &ChannelAddr {
        &self.0
    }

    /// The proc's name.
    pub fn name(&self) -> &str {
        &self.1
    }
}

impl fmt::Display for ProcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{},{}", self.0, self.1)
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

/// Actors are identified by their proc, their name, and pid.
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
pub struct ActorId(pub ProcId, pub String, pub Index);

hyperactor_config::impl_attrvalue!(ActorId);

impl ActorId {
    /// Create a new port ID with the provided port for this actor.
    pub fn port_id(&self, port: u64) -> PortId {
        PortId(self.clone(), port)
    }

    /// Create a child actor ID with the provided PID.
    pub fn child_id(&self, pid: Index) -> Self {
        Self(self.0.clone(), self.1.clone(), pid)
    }

    /// Return the root actor ID for the provided proc and name.
    pub fn root(proc_id: ProcId, name: String) -> Self {
        Self(proc_id, name, 0)
    }

    /// The proc ID of this actor ID.
    pub fn proc_id(&self) -> &ProcId {
        &self.0
    }

    /// The actor's name.
    pub fn name(&self) -> &str {
        &self.1
    }

    /// The actor's pid.
    pub fn pid(&self) -> Index {
        self.2
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ActorId(proc_id, name, pid) = self;
        write!(f, "{},{}[{}]", proc_id, name, pid)
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
        headers: Attrs,
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
///
/// TODO: consider moving [`crate::mailbox::Port`] to `PortRef` in this
/// module for consistency with actors,
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
pub struct PortId(pub ActorId, pub u64);

impl PortId {
    /// The ID of the port's owning actor.
    pub fn actor_id(&self) -> &ActorId {
        &self.0
    }

    /// Convert this port ID into an actor ID.
    pub fn into_actor_id(self) -> ActorId {
        self.0
    }

    /// This port's index.
    pub fn index(&self) -> u64 {
        self.1
    }

    pub(crate) fn is_actor_port(&self) -> bool {
        self.1 & ACTOR_PORT_BIT != 0
    }

    /// Send a serialized message to this port, provided a sending capability,
    /// such as [`crate::actor::Instance`]. It is the sender's responsibility
    /// to ensure that the provided message is well-typed.
    pub fn send(&self, cx: &impl context::Actor, serialized: wirevalue::Any) {
        let mut headers = Attrs::new();
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
        mut headers: Attrs,
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
        let PortId(actor_id, port) = self;
        if self.is_actor_port() {
            let type_info = TypeInfo::get(*port).or_else(|| TypeInfo::get(*port & !ACTOR_PORT_BIT));
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
        }
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
        let mut once = OncePortRef::attest(self.into_port_id());
        once.return_undeliverable = return_undeliverable;
        once
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`].
    pub fn send(&self, cx: &impl context::Actor, message: M) -> Result<(), MailboxSenderError> {
        self.send_with_headers(cx, Attrs::new(), message)
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`]. Additional context can be provided in the form of
    /// headers.
    pub fn send_with_headers(
        &self,
        cx: &impl context::Actor,
        headers: Attrs,
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
        mut headers: Attrs,
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
        let UnboundPort(port_id, reducer_spec, return_undeliverable, port_kind) =
            bindings.try_pop_front::<UnboundPort>()?;
        self.port_id = port_id;
        self.reducer_spec = reducer_spec;
        self.return_undeliverable = return_undeliverable;
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
    phantom: PhantomData<M>,
}

impl<M: RemoteMessage> OncePortRef<M> {
    pub(crate) fn attest(port_id: PortId) -> Self {
        Self {
            port_id,
            reducer_spec: None,
            return_undeliverable: true,
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
            phantom: PhantomData,
        }
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
        self.send_with_headers(cx, Attrs::new(), message)
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`]. Additional context can be provided in the form of headers.
    pub fn send_with_headers(
        self,
        cx: &impl context::Actor,
        mut headers: Attrs,
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
        let UnboundPort(port_id, reducer_spec, _return_undeliverable, port_kind) =
            bindings.try_pop_front::<UnboundPort>()?;
        match port_kind {
            UnboundPortKind::Once => {
                self.port_id = port_id;
                self.reducer_spec = reducer_spec;
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
    use rand::thread_rng;
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
            ("test", WorldId("test".into()).into()),
            (
                "tcp:[::1]:1234,test",
                ProcId("tcp:[::1]:1234".parse().unwrap(), "test".to_string()).into(),
            ),
            (
                "tcp:[::1]:1234,test,testactor[123]",
                ActorId(
                    ProcId("tcp:[::1]:1234".parse().unwrap(), "test".to_string()),
                    "testactor".to_string(),
                    123,
                )
                .into(),
            ),
            (
                // type annotations are ignored
                "tcp:[::1]:1234,test,testactor[0][123<my::type>]",
                PortId(
                    ActorId(
                        ProcId("tcp:[::1]:1234".parse().unwrap(), "test".to_string()),
                        "testactor".to_string(),
                        0,
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
        let cases: Vec<&str> = vec!["(blah)", "world(1, 2, 3)"];

        for s in cases {
            let result: Result<Reference, ReferenceParsingError> = s.parse();
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_reference_ord() {
        let expected: Vec<Reference> = ["first", "second", "third"]
            .into_iter()
            .map(|s| s.parse().unwrap())
            .collect();

        let mut sorted = expected.to_vec();
        sorted.shuffle(&mut thread_rng());
        sorted.sort();

        assert_eq!(sorted, expected);
    }

    #[test]
    fn test_port_type_annotation() {
        #[derive(typeuri::Named, Serialize, Deserialize)]
        struct MyType;
        wirevalue::register_type!(MyType);
        let port_id = PortId(
            ActorId(
                ProcId("tcp:[::1]:1234".parse().unwrap(), "test".to_string()),
                "testactor".into(),
                1,
            ),
            MyType::port(),
        );
        assert_eq!(
            port_id.to_string(),
            "tcp:[::1]:1234,test,testactor[1][17867850292987402005<hyperactor::reference::tests::MyType>]"
        );
    }

    #[tokio::test]
    async fn test_sequencing_from_port_handle_ref_and_id() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let port_handle = client.mailbox().open_enqueue_port(move |headers, _m: ()| {
            let seq_info = headers.get(SEQ_INFO);
            tx.send(seq_info.cloned()).unwrap();
            Ok(())
        });
        port_handle.send(&client, ()).unwrap();
        // No seq will be assigned for unbound port handle.
        assert!(rx.try_recv().unwrap().is_none());

        port_handle.bind_actor_port();
        let port_id = match port_handle.location() {
            PortLocation::Bound(port_id) => port_id,
            _ => panic!("port_handle should be bound"),
        };
        assert!(port_id.is_actor_port());
        let port_ref = PortRef::attest(port_id.clone());

        port_handle.send(&client, ()).unwrap();
        let SeqInfo {
            session_id,
            mut seq,
        } = rx.try_recv().unwrap().unwrap();
        assert_eq!(session_id, client.sequencer().session_id());
        assert_eq!(seq, 1);

        fn assert_seq_info(
            rx: &mut mpsc::UnboundedReceiver<Option<SeqInfo>>,
            session_id: Uuid,
            seq: &mut u64,
        ) {
            *seq += 1;
            let SeqInfo {
                session_id: rcved_session_id,
                seq: rcved_seq,
            } = rx.try_recv().unwrap().unwrap();
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
            tx.send(seq_info.cloned()).unwrap();
            Ok(())
        });
        port_handle.send(&client, ()).unwrap();
        // No seq will be assigned for unbound port handle.
        assert!(rx.try_recv().unwrap().is_none());

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
        let SeqInfo {
            session_id,
            seq: seq1,
        } = rx
            .try_recv()
            .unwrap()
            .expect("non-actor port should have seq info");
        assert_eq!(seq1, 1);
        assert_eq!(session_id, client.sequencer().session_id());

        let port_ref = PortRef::attest(port_id.clone());
        port_ref.send(&client, ()).unwrap();
        let SeqInfo { seq: seq2, .. } = rx
            .try_recv()
            .unwrap()
            .expect("non-actor port should have seq info");
        assert_eq!(seq2, 2);

        port_id.send(&client, wirevalue::Any::serialize(&()).unwrap());
        let SeqInfo { seq: seq3, .. } = rx
            .try_recv()
            .unwrap()
            .expect("non-actor port should have seq info");
        assert_eq!(seq3, 3);
    }
}
