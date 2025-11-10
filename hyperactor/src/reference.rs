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
use rand::Rng;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate as hyperactor;
use crate::Actor;
use crate::ActorHandle;
use crate::Named;
use crate::RemoteHandles;
use crate::RemoteMessage;
use crate::accum::ReducerOpts;
use crate::accum::ReducerSpec;
use crate::actor::Referable;
use crate::attrs::Attrs;
use crate::channel::ChannelAddr;
use crate::context;
use crate::context::MailboxExt;
use crate::data::Serialized;
use crate::data::TypeInfo;
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
use parse::parse;

/// A universal reference to hierarchical identifiers in Hyperactor.
///
/// References implement a concrete syntax which can be parsed via
/// [`FromStr`]. They are of the form:
///
/// - `world`,
/// - `world[rank]`,
/// - `world[rank].actor[pid]`,
/// - `world[rank].port[pid][port]`, or
/// - `world.actor`
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
    Named,
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
    /// A reference to a gang.
    Gang(GangId),
}

impl Reference {
    /// Tells whether this reference is a prefix of the provided reference.
    pub fn is_prefix_of(&self, other: &Reference) -> bool {
        match self {
            Self::World(_) => self.world_id() == other.world_id(),
            Self::Proc(_) => self.proc_id() == other.proc_id(),
            Self::Actor(_) => self == other,
            Self::Port(_) => self == other,
            Self::Gang(_) => self == other,
        }
    }

    /// The world id of the reference.
    pub fn world_id(&self) -> Option<&WorldId> {
        match self {
            Self::World(world_id) => Some(world_id),
            Self::Proc(proc_id) => proc_id.world_id(),
            Self::Actor(ActorId(proc_id, _, _)) => proc_id.world_id(),
            Self::Port(PortId(ActorId(proc_id, _, _), _)) => proc_id.world_id(),
            Self::Gang(GangId(world_id, _)) => Some(world_id),
        }
    }

    /// The proc id of the reference, if any.
    pub fn proc_id(&self) -> Option<&ProcId> {
        match self {
            Self::World(_) => None,
            Self::Proc(proc_id) => Some(proc_id),
            Self::Actor(ActorId(proc_id, _, _)) => Some(proc_id),
            Self::Port(PortId(ActorId(proc_id, _, _), _)) => Some(proc_id),
            Self::Gang(_) => None,
        }
    }

    /// The rank of the reference, if any.
    fn rank(&self) -> Option<Index> {
        self.proc_id().and_then(|proc_id| proc_id.rank())
    }

    /// The actor id of the reference, if any.
    pub fn actor_id(&self) -> Option<&ActorId> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id),
            Self::Port(PortId(actor_id, _)) => Some(actor_id),
            Self::Gang(_) => None,
        }
    }

    /// The actor name of the reference, if any.
    fn actor_name(&self) -> Option<&str> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.name()),
            Self::Port(PortId(actor_id, _)) => Some(actor_id.name()),
            Self::Gang(gang_id) => Some(&gang_id.1),
        }
    }

    /// The pid of the reference, if any.
    fn pid(&self) -> Option<Index> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.pid()),
            Self::Port(PortId(actor_id, _)) => Some(actor_id.pid()),
            Self::Gang(_) => None,
        }
    }

    /// The port of the reference, if any.
    fn port(&self) -> Option<u64> {
        match self {
            Self::World(_) => None,
            Self::Proc(_) => None,
            Self::Actor(_) => None,
            Self::Port(port_id) => Some(port_id.index()),
            Self::Gang(_) => None,
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
        (
            // Ranked procs precede direct procs:
            self.world_id(),
            self.rank(),
            self.proc_id().and_then(ProcId::as_direct),
            self.actor_name(),
            self.pid(),
            self.port(),
        )
            .cmp(&(
                other.world_id(),
                other.rank(),
                other.proc_id().and_then(ProcId::as_direct),
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
            Self::Gang(gang_id) => fmt::Display::fmt(gang_id, f),
        }
    }
}

/// Statically create a [`WorldId`], [`ProcId`], [`ActorId`] or [`GangId`],
/// given the concrete syntax documented in [`Reference`]:
///
/// ```
/// # use hyperactor::id;
/// # use hyperactor::reference::WorldId;
/// # use hyperactor::reference::ProcId;
/// # use hyperactor::reference::ActorId;
/// # use hyperactor::reference::GangId;
/// assert_eq!(id!(hello), WorldId("hello".into()));
/// assert_eq!(id!(hello[0]), ProcId::Ranked(WorldId("hello".into()), 0));
/// assert_eq!(
///     id!(hello[0].actor),
///     ActorId(
///         ProcId::Ranked(WorldId("hello".into()), 0),
///         "actor".into(),
///         0
///     )
/// );
/// assert_eq!(
///     id!(hello[0].actor[1]),
///     ActorId(
///         ProcId::Ranked(WorldId("hello".into()), 0),
///         "actor".into(),
///         1
///     )
/// );
/// assert_eq!(
///     id!(hello.actor),
///     GangId(WorldId("hello".into()), "actor".into())
/// );
/// ```
///
/// Prefer to use the id macro to construct identifiers in code, as it
/// guarantees static validity, and preserves and reinforces the uniform
/// concrete syntax of identifiers throughout.
#[macro_export]
macro_rules! id {
    ($world:ident) => {
        $crate::reference::WorldId(stringify!($world).to_string())
    };
    ($world:ident [$rank:expr]) => {
        $crate::reference::ProcId::Ranked(
            $crate::reference::WorldId(stringify!($world).to_string()),
            $rank,
        )
    };
    ($world:ident [$rank:expr] . $actor:ident) => {
        $crate::reference::ActorId(
            $crate::reference::ProcId::Ranked(
                $crate::reference::WorldId(stringify!($world).to_string()),
                $rank,
            ),
            stringify!($actor).to_string(),
            0,
        )
    };
    ($world:ident [$rank:expr] . $actor:ident [$pid:expr]) => {
        $crate::reference::ActorId(
            $crate::reference::ProcId::Ranked(
                $crate::reference::WorldId(stringify!($world).to_string()),
                $rank,
            ),
            stringify!($actor).to_string(),
            $pid,
        )
    };
    ($world:ident . $actor:ident) => {
        $crate::reference::GangId(
            $crate::reference::WorldId(stringify!($world).to_string()),
            stringify!($actor).to_string(),
        )
    };
    ($world:ident [$rank:expr] . $actor:ident [$pid:expr] [$port:expr]) => {
        $crate::reference::PortId(
            $crate::reference::ActorId(
                $crate::reference::ProcId::Ranked(
                    $crate::reference::WorldId(stringify!($world).to_string()),
                    $rank,
                ),
                stringify!($actor).to_string(),
                $pid,
            ),
            $port,
        )
    };
}
pub use id;

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
        // First, try to parse a "new style" reference:
        // 1) If the reference contains a comma (anywhere), it is a new style reference;
        //    commas were not a valid lexeme in the previous reference format.
        // 2) This is a bit ugly, but we bypass the tokenizer prior to this comma,
        //    try to parse a channel address, and then parse the remainder.

        match addr.split_once(",") {
            Some((channel_addr, rest)) => {
                let channel_addr = channel_addr.parse().map_err(|err| {
                    ReferenceParsingError::InvalidChannelAddress(channel_addr.to_string(), err)
                })?;

                Ok(parse! {
                    Lexer::new(rest);

                    // channeladdr,proc_name
                    Token::Elem(proc_name) =>
                    Self::Proc(ProcId::Direct(channel_addr, proc_name.to_string())),

                    // channeladdr,proc_name,actor_name
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name) =>
                    Self::Actor(ActorId(ProcId::Direct(channel_addr, proc_name.to_string()), actor_name.to_string(), 0)),

                    // channeladdr,proc_name,actor_name[rank]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(rank) Token::RightBracket =>
                        Self::Actor(ActorId(ProcId::Direct(channel_addr, proc_name.to_string()), actor_name.to_string(), rank)),

                    // channeladdr,proc_name,actor_name[rank][port]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(rank) Token::RightBracket
                        Token::LeftBracket Token::Uint(index) Token::RightBracket  =>
                        Self::Port(PortId(ActorId(ProcId::Direct(channel_addr, proc_name.to_string()), actor_name.to_string(), rank), index as u64)),

                    // channeladdr,proc_name,actor_name[rank][port<type>]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(rank) Token::RightBracket
                        Token::LeftBracket Token::Uint(index)
                            Token::LessThan Token::Elem(_type) Token::GreaterThan
                        Token::RightBracket =>
                        Self::Port(PortId(ActorId(ProcId::Direct(channel_addr, proc_name.to_string()), actor_name.to_string(), rank), index as u64)),
                }?)
            }

            // "old style" / "ranked" reference
            None => {
                Ok(parse! {
                    Lexer::new(addr);

                    // world
                    Token::Elem(world) => Self::World(WorldId(world.into())),

                    // world[rank]
                    Token::Elem(world) Token::LeftBracket Token::Uint(rank) Token::RightBracket =>
                        Self::Proc(ProcId::Ranked(WorldId(world.into()), rank)),

                    // world[rank].actor  (implied pid=0)
                    Token::Elem(world) Token::LeftBracket Token::Uint(rank) Token::RightBracket
                        Token::Dot Token::Elem(actor) =>
                        Self::Actor(ActorId(ProcId::Ranked(WorldId(world.into()), rank), actor.into(), 0)),

                    // world[rank].actor[pid]
                    Token::Elem(world) Token::LeftBracket Token::Uint(rank) Token::RightBracket
                        Token::Dot Token::Elem(actor)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket =>
                        Self::Actor(ActorId(ProcId::Ranked(WorldId(world.into()), rank), actor.into(), pid)),

                    // world[rank].actor[pid][port]
                    Token::Elem(world) Token::LeftBracket Token::Uint(rank) Token::RightBracket
                        Token::Dot Token::Elem(actor)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket
                        Token::LeftBracket Token::Uint(index) Token::RightBracket =>
                        Self::Port(PortId(ActorId(ProcId::Ranked(WorldId(world.into()), rank), actor.into(), pid), index as u64)),

                    // world[rank].actor[pid][port<type>]
                    Token::Elem(world) Token::LeftBracket Token::Uint(rank) Token::RightBracket
                        Token::Dot Token::Elem(actor)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket
                        Token::LeftBracket Token::Uint(index)
                            Token::LessThan Token::Elem(_type) Token::GreaterThan
                        Token::RightBracket =>
                        Self::Port(PortId(ActorId(ProcId::Ranked(WorldId(world.into()), rank), actor.into(), pid), index as u64)),

                    // world.actor
                    Token::Elem(world) Token::Dot Token::Elem(actor) =>
                        Self::Gang(GangId(WorldId(world.into()), actor.into())),
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

impl From<GangId> for Reference {
    fn from(gang_id: GangId) -> Self {
        Self::Gang(gang_id)
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
    Named
)]
pub struct WorldId(pub String);

impl WorldId {
    /// Create a proc ID with the provided index in this world.
    pub fn proc_id(&self, index: Index) -> ProcId {
        ProcId::Ranked(self.clone(), index)
    }

    /// The world index.
    pub fn name(&self) -> &str {
        &self.0
    }

    /// Return a randomly selected user proc in this world.
    pub fn random_user_proc(&self) -> ProcId {
        let mask = 1usize << (std::mem::size_of::<usize>() * 8 - 1);
        ProcId::Ranked(self.clone(), rand::thread_rng().r#gen::<usize>() | mask)
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

/// Procs are identified by their _rank_ within a world or by a direct channel address.
/// Each proc represents an actor runtime that can locally route to all of its
/// constituent actors.
///
/// Ranks >= 1usize << (no. bits in usize - 1) (i.e., with the high bit set) are "user"
/// ranks. These are reserved for randomly generated identifiers not
/// assigned by the system.
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
    Named,
    EnumAsInner
)]
pub enum ProcId {
    /// A ranked proc within a world
    Ranked(WorldId, Index),
    /// A proc reachable via a direct channel address, and local name.
    Direct(ChannelAddr, String),
}

impl ProcId {
    /// Create an actor ID with the provided name, pid within this proc.
    pub fn actor_id(&self, name: impl Into<String>, pid: Index) -> ActorId {
        ActorId(self.clone(), name.into(), pid)
    }

    /// The proc's world id, if this is a ranked proc.
    pub fn world_id(&self) -> Option<&WorldId> {
        match self {
            ProcId::Ranked(world_id, _) => Some(world_id),
            ProcId::Direct(_, _) => None,
        }
    }

    /// The world name, if this is a ranked proc.
    pub fn world_name(&self) -> Option<&str> {
        self.world_id().map(|world_id| world_id.name())
    }

    /// The proc's rank, if this is a ranked proc.
    pub fn rank(&self) -> Option<Index> {
        match self {
            ProcId::Ranked(_, rank) => Some(*rank),
            ProcId::Direct(_, _) => None,
        }
    }

    /// The proc's name, if this is a direct proc.
    pub fn name(&self) -> Option<&String> {
        match self {
            ProcId::Ranked(_, _) => None,
            ProcId::Direct(_, name) => Some(name),
        }
    }
}

impl fmt::Display for ProcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcId::Ranked(world_id, rank) => write!(f, "{}[{}]", world_id, rank),
            ProcId::Direct(addr, name) => write!(f, "{},{}", addr, name),
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
    Named
)]
pub struct ActorId(pub ProcId, pub String, pub Index);

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

    /// The world name. Panics if this is a direct proc.
    pub fn world_name(&self) -> &str {
        self.0
            .world_name()
            .expect("world_name() called on direct proc")
    }

    /// The actor's proc's rank. Panics if this is a direct proc.
    pub fn rank(&self) -> Index {
        self.0.rank().expect("rank() called on direct proc")
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
        match proc_id {
            ProcId::Ranked(..) => write!(f, "{}.{}[{}]", proc_id, name, pid),
            ProcId::Direct(..) => write!(f, "{},{}[{}]", proc_id, name, pid),
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
#[derive(Debug, Named)]
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
    Named
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

    /// Send a serialized message to this port, provided a sending capability,
    /// such as [`crate::actor::Instance`]. It is the sender's responsibility
    /// to ensure that the provided message is well-typed.
    pub fn send(&self, cx: &impl context::Actor, serialized: Serialized) {
        let mut headers = Attrs::new();
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        cx.post(self.clone(), headers, serialized, true);
    }

    /// Send a serialized message to this port, provided a sending capability,
    /// such as [`crate::actor::Instance`], with additional context provided by headers.
    /// It is the sender's responsibility to ensure that the provided message is well-typed.
    pub fn send_with_headers(
        &self,
        cx: &impl context::Actor,
        serialized: Serialized,
        mut headers: Attrs,
    ) {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        cx.post(self.clone(), headers, serialized, true);
    }

    /// Split this port, returning a new port that relays messages to the port
    /// through a local proxy, which may coalesce messages.
    pub fn split(
        &self,
        cx: &impl context::Actor,
        reducer_spec: Option<ReducerSpec>,
        reducer_opts: Option<ReducerOpts>,
        return_undeliverable: bool,
    ) -> anyhow::Result<PortId> {
        cx.split(
            self.clone(),
            reducer_spec,
            reducer_opts,
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
        if port & (1 << 63) != 0 {
            let type_info = TypeInfo::get(*port).or_else(|| TypeInfo::get(*port & !(1 << 63)));
            let typename = type_info.map_or("unknown", TypeInfo::typename);
            write!(f, "{}[{}<{}>]", actor_id, port, typename)
        } else {
            write!(f, "{}[{}]", actor_id, port)
        }
    }
}

/// A reference to a remote port. All messages passed through
/// PortRefs will be serialized.
#[derive(Debug, Serialize, Deserialize, Derivative, Named)]
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
    reducer_opts: Option<ReducerOpts>,
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
            reducer_opts: None,
            phantom: PhantomData,
            return_undeliverable: true,
        }
    }

    /// The caller attests that the provided PortId can be
    /// converted to a reachable, typed port reference.
    pub fn attest_reducible(port_id: PortId, reducer_spec: Option<ReducerSpec>) -> Self {
        Self {
            port_id,
            reducer_spec,
            reducer_opts: None, // TODO: provide attest_reducible_opts
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
        OncePortRef::attest(self.into_port_id())
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
        let serialized = Serialized::serialize(&message).map_err(|err| {
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
        message: Serialized,
    ) {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        crate::mailbox::headers::set_rust_message_type::<M>(&mut headers);
        cx.post(
            self.port_id.clone(),
            headers,
            message,
            self.return_undeliverable,
        );
    }

    /// Convert this port into a sink that can be used to send messages using the given capability.
    pub fn into_sink<C: context::Actor>(self, cx: C) -> PortSink<C, M> {
        PortSink::new(cx, self)
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
            reducer_opts: self.reducer_opts.clone(),
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

/// The parameters extracted from [`PortRef`] to [`Bindings`].
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Named)]
pub struct UnboundPort(
    pub PortId,
    pub Option<ReducerSpec>,
    pub Option<ReducerOpts>,
    pub bool, // return_undeliverable
);

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
            port_ref.reducer_opts.clone(),
            port_ref.return_undeliverable,
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
        let bound = bindings.try_pop_front::<UnboundPort>()?;
        self.port_id = bound.0;
        self.reducer_spec = bound.1;
        self.reducer_opts = bound.2;
        self.return_undeliverable = bound.3;
        Ok(())
    }
}

/// A remote reference to a [`OncePort`]. References are serializable
/// and may be passed to remote actors, which can then use it to send
/// a message to this port.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct OncePortRef<M> {
    port_id: PortId,
    phantom: PhantomData<M>,
}

impl<M: RemoteMessage> OncePortRef<M> {
    pub(crate) fn attest(port_id: PortId) -> Self {
        Self {
            port_id,
            phantom: PhantomData,
        }
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
        self.send_with_headers(cx, Attrs::new(), message, true)
    }

    /// Send a message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`]. Additional context can be provided in the form of headers.
    pub fn send_with_headers(
        self,
        cx: &impl context::Actor,
        mut headers: Attrs,
        message: M,
        return_undeliverable: bool,
    ) -> Result<(), MailboxSenderError> {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        let serialized = Serialized::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                self.port_id.clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        })?;
        cx.post(
            self.port_id.clone(),
            headers,
            serialized,
            return_undeliverable,
        );
        Ok(())
    }
}

impl<M: RemoteMessage> Clone for OncePortRef<M> {
    fn clone(&self) -> Self {
        Self {
            port_id: self.port_id.clone(),
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
        crate::data::intern_typename!(Self, "hyperactor::mailbox::OncePortRef<{}>", M)
    }
}

// We do not split PortRef, because it can only receive a single response, and
// there is no meaningful performance gain to make that response going through
// comm actors.
impl<M: RemoteMessage> Unbind for OncePortRef<M> {
    fn unbind(&self, _bindings: &mut Bindings) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<M: RemoteMessage> Bind for OncePortRef<M> {
    fn bind(&mut self, _bindings: &mut Bindings) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Gangs identify a gang of actors across the world.
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
    Named
)]
pub struct GangId(pub WorldId, pub String);

impl GangId {
    pub(crate) fn expand(&self, world_size: usize) -> impl Iterator<Item = ActorId> + '_ {
        (0..world_size).map(|rank| ActorId(ProcId::Ranked(self.0.clone(), rank), self.1.clone(), 0))
    }

    /// The world id of the gang.
    pub fn world_id(&self) -> &WorldId {
        &self.0
    }

    /// The name of the gang.
    pub fn name(&self) -> &str {
        &self.1
    }

    /// The gang's actor ID for the provided rank. It always returns the root
    /// actor because the root actor is the public interface of a gang.
    pub fn actor_id(&self, rank: Index) -> ActorId {
        ActorId(
            ProcId::Ranked(self.world_id().clone(), rank),
            self.name().to_string(),
            0,
        )
    }
}

impl fmt::Display for GangId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let GangId(world_id, name) = self;
        write!(f, "{}.{}", world_id, name)
    }
}

impl FromStr for GangId {
    type Err = ReferenceParsingError;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        match addr.parse()? {
            Reference::Gang(gang_id) => Ok(gang_id),
            _ => Err(ReferenceParsingError::WrongType("gang".into())),
        }
    }
}

/// Chop implements a simple lexer on a fixed set of delimiters.
fn chop<'a>(mut s: &'a str, delims: &'a [&'a str]) -> impl Iterator<Item = &'a str> + 'a {
    std::iter::from_fn(move || {
        if s.is_empty() {
            return None;
        }

        match delims
            .iter()
            .enumerate()
            .flat_map(|(index, d)| s.find(d).map(|pos| (index, pos)))
            .min_by_key(|&(_, v)| v)
        {
            Some((index, 0)) => {
                let delim = delims[index];
                s = &s[delim.len()..];
                Some(delim)
            }
            Some((_, pos)) => {
                let token = &s[..pos];
                s = &s[pos..];
                Some(token.trim())
            }
            None => {
                let token = s;
                s = "";
                Some(token.trim())
            }
        }
    })
}

/// GangRefs are typed references to gangs.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct GangRef<A: Referable> {
    gang_id: GangId,
    phantom: PhantomData<A>,
}

impl<A: Referable> GangRef<A> {
    /// Return an ActorRef corresponding with the provided rank in
    /// this gang.  Does not check the validity of the rank, so the
    /// returned identifier is not guaranteed to refer to a valid rank.
    pub fn rank(&self, rank: Index) -> ActorRef<A> {
        let GangRef {
            gang_id: GangId(world_id, name),
            ..
        } = self;
        ActorRef::attest(ActorId(
            ProcId::Ranked(world_id.clone(), rank),
            name.clone(),
            0,
        ))
    }

    /// Return the gang ID.
    pub fn gang_id(&self) -> &GangId {
        &self.gang_id
    }

    /// The caller attests that the provided GandId can be
    /// converted to a reachable, typed gang reference.
    pub fn attest(gang_id: GangId) -> Self {
        Self {
            gang_id,
            phantom: PhantomData,
        }
    }
}

impl<A: Referable> Clone for GangRef<A> {
    fn clone(&self) -> Self {
        Self {
            gang_id: self.gang_id.clone(),
            phantom: PhantomData,
        }
    }
}

impl<A: Referable> From<GangRef<A>> for GangId {
    fn from(gang_ref: GangRef<A>) -> Self {
        gang_ref.gang_id
    }
}

impl<'a, A: Referable> From<&'a GangRef<A>> for &'a GangId {
    fn from(gang_ref: &'a GangRef<A>) -> Self {
        &gang_ref.gang_id
    }
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use super::*;

    #[test]
    fn test_reference_parse() {
        let cases: Vec<(&str, Reference)> = vec![
            ("test", WorldId("test".into()).into()),
            (
                "test[234]",
                ProcId::Ranked(WorldId("test".into()), 234).into(),
            ),
            (
                "test[234].testactor[6]",
                ActorId(
                    ProcId::Ranked(WorldId("test".into()), 234),
                    "testactor".into(),
                    6,
                )
                .into(),
            ),
            (
                "test[234].testactor[6][1]",
                PortId(
                    ActorId(
                        ProcId::Ranked(WorldId("test".into()), 234),
                        "testactor".into(),
                        6,
                    ),
                    1,
                )
                .into(),
            ),
            (
                "test.testactor",
                GangId(WorldId("test".into()), "testactor".into()).into(),
            ),
            (
                "tcp:[::1]:1234,test,testactor[123]",
                ActorId(
                    ProcId::Direct("tcp:[::1]:1234".parse().unwrap(), "test".to_string()),
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
                        ProcId::Direct("tcp:[::1]:1234".parse().unwrap(), "test".to_string()),
                        "testactor".to_string(),
                        0,
                    ),
                    123,
                )
                .into(),
            ),
            (
                // References with v1::Name::Suffixed for actor names are parseable
                "test[234].testactor_12345[6]",
                ActorId(
                    ProcId::Ranked(WorldId("test".into()), 234),
                    "testactor_12345".into(),
                    6,
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
        let cases: Vec<&str> = vec![
            "(blah)",
            "world(1, 2, 3)",
            // hyphen is not allowed in actor name
            "test[234].testactor-12345[6]",
        ];

        for s in cases {
            let result: Result<Reference, ReferenceParsingError> = s.parse();
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_id_macro() {
        assert_eq!(id!(hello), WorldId("hello".into()));
        assert_eq!(id!(hello[0]), ProcId::Ranked(WorldId("hello".into()), 0));
        assert_eq!(
            id!(hello[0].actor),
            ActorId(
                ProcId::Ranked(WorldId("hello".into()), 0),
                "actor".into(),
                0
            )
        );
        assert_eq!(
            id!(hello[0].actor[1]),
            ActorId(
                ProcId::Ranked(WorldId("hello".into()), 0),
                "actor".into(),
                1
            )
        );
        assert_eq!(
            id!(hello.actor),
            GangId(WorldId("hello".into()), "actor".into())
        );
    }

    #[test]
    fn test_reference_ord() {
        let expected: Vec<Reference> = [
            "first",
            "second",
            "second.actor1",
            "second.actor2",
            "second[1]",
            "second[1].actor1",
            "second[1].actor2",
            "second[2]",
            "second[2].actor100",
            "third",
            "third.actor",
            "third[2]",
            "third[2].actor",
            "third[2].actor[1]",
        ]
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
        #[derive(Named, Serialize, Deserialize)]
        struct MyType;
        let port_id = PortId(
            ActorId(
                ProcId::Ranked(WorldId("test".into()), 234),
                "testactor".into(),
                1,
            ),
            MyType::port(),
        );
        assert_eq!(
            port_id.to_string(),
            "test[234].testactor[1][17867850292987402005<hyperactor::reference::tests::MyType>]"
        );
    }
}
