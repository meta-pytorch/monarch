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
use hyperactor_config::Attrs;
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

/// Stamp attribution-header entries carried on a ref's in-memory
/// `Attrs` onto an outbound envelope's `Flattrs` headers,
/// **overwriting** any pre-existing entry under the same key.
///
/// Thin wrapper around the shared marker-driven stamping mechanism
/// (`hyperactor_config::attrs::stamp_marked_attrs_into_flattrs`)
/// parameterized with the `ATTRIBUTION_HEADER` marker: only entries
/// whose declared attr key carries `@meta(ATTRIBUTION_HEADER = true)`
/// are copied. The substrate does not need to know the specific set
/// of declared attribution keys (they live in `hyperactor_mesh`),
/// only to pass the shared marker. AT-1 ("declared-key-only
/// transport") is enforced mechanically by the shared mechanism; this
/// call site is one of several that use it.
///
/// **Overwrite semantics matter.** `Flattrs::get` returns the first
/// match for a given key hash, so a naive append-based stamp would be
/// silently shadowed if an upstream layer had already written the
/// same key to `headers`. The ref's attribution is the authoritative
/// destination carrier for the ref's own send path (AT-3), and the
/// shared mechanism uses `set_serialized` — which replaces any
/// colliding entry rather than appending after it.
fn stamp_ref_attribution_onto(headers: &mut Flattrs, attrs: &Attrs) {
    hyperactor_config::attrs::stamp_marked_attrs_into_flattrs(
        headers,
        attrs,
        hyperactor_config::attrs::ATTRIBUTION_HEADER,
    );
}

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
    pub fn proc_id(&self) -> Option<&ProcId> {
        match self {
            Self::Proc(proc_id) => Some(proc_id),
            Self::Actor(actor_id) => Some(actor_id.proc_id()),
            Self::Port(port_id) => Some(port_id.actor_id().proc_id()),
        }
    }

    /// The actor id of the reference, if any.
    pub fn actor_id(&self) -> Option<&ActorId> {
        match self {
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id),
            Self::Port(port_id) => Some(port_id.actor_id()),
        }
    }

    /// The actor name of the reference, if any.
    fn actor_name(&self) -> Option<&str> {
        match self {
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.name()),
            Self::Port(port_id) => Some(port_id.actor_id().name()),
        }
    }

    /// The pid of the reference, if any.
    fn pid(&self) -> Option<Index> {
        match self {
            Self::Proc(_) => None,
            Self::Actor(actor_id) => Some(actor_id.pid()),
            Self::Port(port_id) => Some(port_id.actor_id().pid()),
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
        // Order by: proc address/name, then actor_name, then pid, then port
        (self.proc_id(), self.actor_name(), self.pid(), self.port()).cmp(&(
            other.proc_id(),
            other.actor_name(),
            other.pid(),
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
                    Self::Actor(ActorId::new(ProcId::with_name(channel_addr, proc_name), actor_name, 0)),

                    // channeladdr,proc_name,actor_name[pid]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket =>
                        Self::Actor(ActorId::new(ProcId::with_name(channel_addr, proc_name), actor_name, pid)),

                    // channeladdr,proc_name,actor_name[pid][port]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket
                        Token::LeftBracket Token::Uint(index) Token::RightBracket  =>
                        Self::Port(PortId::new(ActorId::new(ProcId::with_name(channel_addr, proc_name), actor_name, pid), index as u64)),

                    // channeladdr,proc_name,actor_name[pid][port<type>]
                    Token::Elem(proc_name) Token::Comma Token::Elem(actor_name)
                        Token::LeftBracket Token::Uint(pid) Token::RightBracket
                        Token::LeftBracket Token::Uint(index)
                            Token::LessThan Token::Elem(_type) Token::GreaterThan
                        Token::RightBracket =>
                        Self::Port(PortId::new(ActorId::new(ProcId::with_name(channel_addr, proc_name), actor_name, pid), index as u64)),
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
pub struct ProcId(ChannelAddr, String);

/// Compute an 8-char hex hash suffix from a [`ChannelAddr`].
fn addr_hash_suffix(addr: &ChannelAddr) -> String {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    format!("{:08x}", hasher.finish() as u32)
}

impl ProcId {
    /// Create a ProcId with a globally-unique name: `"{base_name}-{hash_of_addr}"`.
    ///
    /// Use this for well-known / fixed proc names (e.g. `"service"`, `"local"`)
    /// that are not already unique across hosts.
    pub fn unique(addr: ChannelAddr, base_name: impl Into<String>) -> Self {
        let base = base_name.into();
        let suffix = addr_hash_suffix(&addr);
        Self(addr, format!("{}-{}", base, suffix))
    }

    /// Create a ProcId with an already-unique name (no suffix added).
    ///
    /// Use this for deserialization, test helpers, and names that already
    /// contain a UUID or other uniquifying component.
    pub fn with_name(addr: ChannelAddr, name: impl Into<String>) -> Self {
        Self(addr, name.into())
    }

    /// The base name before the `-{hash}` suffix, if present.
    ///
    /// If the name ends with `-XXXXXXXX` (8 hex chars), returns the part
    /// before that suffix. Otherwise returns the full name.
    pub fn base_name(&self) -> &str {
        match self.1.rsplit_once('-') {
            Some((base, suffix))
                if suffix.len() == 8 && suffix.chars().all(|c| c.is_ascii_hexdigit()) =>
            {
                base
            }
            _ => &self.1,
        }
    }

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
pub struct ActorId(ProcId, String, Index);

hyperactor_config::impl_attrvalue!(ActorId);

impl ActorId {
    /// Create a new actor ID.
    pub fn new(proc_id: ProcId, name: impl Into<String>, pid: Index) -> Self {
        Self(proc_id, name.into(), pid)
    }

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
        write!(f, "{},{}[{}]", self.0, self.1, self.2)
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
///
/// In addition to the typed `actor_id`, an `ActorRef` may carry
/// structured transport attribution for the destination actor in an
/// `attribution: Attrs` field. Per AT-1 / AT-4 in
/// `hyperactor_mesh/src/supervision.rs`, the attribution participates
/// in neither ref identity nor hashing — it is informational
/// transport state that is copied through `port()` / `into_once()`
/// onto downstream refs and stamped onto envelope headers by the
/// shared marker-driven stamping mechanism at transport call sites
/// (cast, direct-send).
#[derive(typeuri::Named)]
pub struct ActorRef<A: Referable> {
    pub(crate) actor_id: ActorId,
    /// Structured transport attribution for the destination actor.
    /// Populated via `attest_with_attrs` on mesh-derived producer
    /// sites; `attest` defaults to an empty `Attrs`. See AT-1..AT-5
    /// in `hyperactor_mesh/src/supervision.rs`.
    pub(crate) attribution: Attrs,
    // fn() -> A so that the struct remains Send
    phantom: PhantomData<fn() -> A>,
}

impl<A: Referable> ActorRef<A> {
    /// Get the remote port for message type [`M`] for the referenced actor.
    pub fn port<M: RemoteMessage>(&self) -> PortRef<M>
    where
        A: RemoteHandles<M>,
    {
        // AT-1: carry the destination attribution from the ref onto
        // the produced PortRef unchanged, so downstream sends (e.g.
        // `PortRef::send_serialized`) stamp the same
        // attribution-header entries onto the envelope via the shared
        // marker-driven mechanism.
        PortRef::attest_with_attrs(
            self.actor_id.port_id(<M as Named>::port()),
            self.attribution.clone(),
        )
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
    ///
    /// This constructor produces a ref with empty transport
    /// attribution. Mesh-derived producer sites that have attribution
    /// in scope should prefer `attest_with_attrs`.
    pub fn attest(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            attribution: Attrs::new(),
            phantom: PhantomData,
        }
    }

    /// Like `attest`, but also attaches transport attribution to the
    /// produced ref. Used by mesh-derived producer sites (e.g.,
    /// `ActorMeshRef::get(rank)`) that have the destination
    /// attribution in scope at construction time. See AT-1..AT-5 in
    /// `hyperactor_mesh/src/supervision.rs` for the carrier contract.
    pub fn attest_with_attrs(actor_id: ActorId, attribution: Attrs) -> Self {
        Self {
            actor_id,
            attribution,
            phantom: PhantomData,
        }
    }

    /// Access the transport-attribution carrier on this ref.
    /// Informational only; does not participate in identity (AT-4).
    pub fn attribution(&self) -> &Attrs {
        &self.attribution
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

// Implement Serialize manually, without requiring A: Serialize.
//
// Wire format: `ActorRef` serializes as the tuple
// `(actor_id, attribution)`. The attribution carrier is part of the
// ref on the wire; callers that need the bare id should use
// `actor_id()`.
impl<A: Referable> Serialize for ActorRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (&self.actor_id, &self.attribution).serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize.
// See `Serialize` above for the tuple wire format.
impl<'de, A: Referable> Deserialize<'de> for ActorRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (actor_id, attribution) = <(ActorId, Attrs)>::deserialize(deserializer)?;
        Ok(ActorRef {
            actor_id,
            attribution,
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
            attribution: self.attribution.clone(),
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
pub struct PortId(ActorId, u64);

impl PortId {
    /// Create a new port ID.
    pub fn new(actor_id: ActorId, port: u64) -> Self {
        Self(actor_id, port)
    }

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

/// A reference to a remote port. All messages passed through PortRefs
/// will be serialized. PortRefs are always streaming.
///
/// Carries structured transport attribution for the destination
/// actor; see `ActorRef`'s doc for the carrier contract (AT-1..AT-5
/// in `hyperactor_mesh/src/supervision.rs`). AT-4: the attribution
/// field does not participate in identity, equality, ordering, or
/// hashing.
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
    /// Structured transport attribution for the destination actor.
    /// Populated by `ActorRef::port()` (copy-through) or
    /// `PortRef::attest_with_attrs`. Informational only; AT-4.
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    attribution: Attrs,
}

impl<M: RemoteMessage> PortRef<M> {
    /// The caller attests that the provided PortId can be
    /// converted to a reachable, typed port reference.
    ///
    /// Produces a ref with empty transport attribution. Call sites
    /// with attribution in scope should use `attest_with_attrs`.
    pub fn attest(port_id: PortId) -> Self {
        Self {
            port_id,
            reducer_spec: None,
            streaming_opts: StreamingReducerOpts::default(),
            phantom: PhantomData,
            return_undeliverable: true,
            unsplit: false,
            attribution: Attrs::new(),
        }
    }

    /// Like `attest`, but also attaches transport attribution to the
    /// produced ref. Used by `ActorRef::port()` copy-through and
    /// other mesh-derived producer sites. See AT-1..AT-5 in
    /// `hyperactor_mesh/src/supervision.rs`.
    pub fn attest_with_attrs(port_id: PortId, attribution: Attrs) -> Self {
        Self {
            port_id,
            reducer_spec: None,
            streaming_opts: StreamingReducerOpts::default(),
            phantom: PhantomData,
            return_undeliverable: true,
            unsplit: false,
            attribution,
        }
    }

    /// Access the transport-attribution carrier on this ref.
    /// Informational only; AT-4.
    pub fn attribution(&self) -> &Attrs {
        &self.attribution
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
            attribution: Attrs::new(),
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
        let attribution = self.attribution.clone();
        // AT-1: attribution copies through into the OncePortRef so the
        // same declared keys will reach the envelope headers on send.
        let mut once = OncePortRef::attest_with_attrs(self.into_port_id(), attribution);
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
        // AT-1: stamp the ref's transport attribution onto the
        // outbound envelope headers. See `stamp_ref_attribution_onto`
        // at the top of this module for the crate-boundary reasoning.
        stamp_ref_attribution_onto(&mut headers, &self.attribution);
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
            attribution: self.attribution.clone(),
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
///
/// Carries structured transport attribution for the destination
/// actor; see `ActorRef`'s doc for the carrier contract (AT-1..AT-5
/// in `hyperactor_mesh/src/supervision.rs`). AT-4: the attribution
/// field does not participate in identity or equality.
#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(PartialEq)]
pub struct OncePortRef<M> {
    port_id: PortId,
    reducer_spec: Option<ReducerSpec>,
    return_undeliverable: bool,
    unsplit: bool,
    phantom: PhantomData<M>,
    /// Structured transport attribution for the destination actor.
    /// Populated by `PortRef::into_once()` (copy-through) or
    /// `OncePortRef::attest_with_attrs`. Informational only; AT-4.
    #[derivative(PartialEq = "ignore")]
    attribution: Attrs,
}

impl<M: RemoteMessage> OncePortRef<M> {
    pub(crate) fn attest(port_id: PortId) -> Self {
        Self {
            port_id,
            reducer_spec: None,
            return_undeliverable: true,
            unsplit: false,
            phantom: PhantomData,
            attribution: Attrs::new(),
        }
    }

    /// Like `attest`, but also attaches transport attribution to the
    /// produced ref. Used by `PortRef::into_once()` copy-through.
    /// See AT-1..AT-5 in `hyperactor_mesh/src/supervision.rs`.
    pub(crate) fn attest_with_attrs(port_id: PortId, attribution: Attrs) -> Self {
        Self {
            port_id,
            reducer_spec: None,
            return_undeliverable: true,
            unsplit: false,
            phantom: PhantomData,
            attribution,
        }
    }

    /// Access the transport-attribution carrier on this ref.
    /// Informational only; AT-4.
    pub fn attribution(&self) -> &Attrs {
        &self.attribution
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
            attribution: Attrs::new(),
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
        // AT-1: stamp the ref's transport attribution onto the
        // outbound envelope headers. See `stamp_ref_attribution_onto`
        // at the top of this module for the crate-boundary reasoning.
        stamp_ref_attribution_onto(&mut headers, &self.attribution);
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
            attribution: self.attribution.clone(),
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
                "tcp:[::1]:1234,test,testactor[123]",
                ActorId::new(
                    ProcId::with_name("tcp:[::1]:1234".parse::<ChannelAddr>().unwrap(), "test"),
                    "testactor",
                    123,
                )
                .into(),
            ),
            (
                // type annotations are ignored
                "tcp:[::1]:1234,test,testactor[0][123<my::type>]",
                PortId::new(
                    ActorId::new(
                        ProcId::with_name("tcp:[::1]:1234".parse::<ChannelAddr>().unwrap(), "test"),
                        "testactor",
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

    // Attribution-transport tests for the ref types defined in this
    // module. The `AT-*` invariants these tests reference live in
    // `hyperactor_mesh::supervision`.

    use hyperactor_config::attrs::ATTRIBUTION_HEADER;
    use hyperactor_config::attrs::declare_attrs;

    declare_attrs! {
        /// Declared as an attribution-header key
        /// (`@meta(ATTRIBUTION_HEADER = true)`), so the existing AT-1
        /// copy-through and overwrite tests below continue to flow
        /// values through the shared marker-driven stamping mechanism
        /// onto outbound headers.
        @meta(ATTRIBUTION_HEADER = true)
        pub attr REF_ATTR_TEST_STRING: String;

        /// Declared as an attribution-header key; same reason as
        /// `REF_ATTR_TEST_STRING`.
        @meta(ATTRIBUTION_HEADER = true)
        pub attr REF_ATTR_TEST_U64: u64;

        /// NOT declared as an attribution-header key. Used by
        /// `ref_attr_stamp_filters_unmarked_entries` to pin AT-1:
        /// entries whose declared key lacks `@meta(ATTRIBUTION_HEADER
        /// = true)` must not be stamped onto outbound headers by the
        /// shared marker-driven mechanism, even if they are present
        /// on a ref's attribution.
        pub attr REF_ATTR_TEST_UNMARKED: String;
    }

    fn test_actor_id(name: &str) -> ActorId {
        ProcId::with_name(ChannelAddr::Local(0), "ref_attr_test_proc").actor_id(name, 0)
    }

    /// Minimal concrete type satisfying `Referable` so the tests
    /// below can construct `ActorRef<A>` without dragging in a
    /// heavier actor fixture.
    struct RefAttrTestActor;
    impl crate::actor::Referable for RefAttrTestActor {}
    impl typeuri::Named for RefAttrTestActor {
        fn typename() -> &'static str {
            "hyperactor::reference::tests::RefAttrTestActor"
        }
    }

    // AT-1 (copy-through): `ActorRef::port()` carries the ref's
    // attribution carrier onto the produced `PortRef` unchanged, so
    // downstream sends can stamp the same attribution-header entries
    // onto envelope headers via the shared marker-driven mechanism.
    #[test]
    fn ref_attr_port_copy_through() {
        let mut attrs = Attrs::new();
        attrs.set(REF_ATTR_TEST_STRING, "mesh_value".to_string());
        attrs.set(REF_ATTR_TEST_U64, 7u64);
        let actor_ref: ActorRef<RefAttrTestActor> =
            ActorRef::attest_with_attrs(test_actor_id("a"), attrs);

        // Plain Actor::port requires a RemoteHandles<M> bound; go
        // through the actor_id port_id + attest_with_attrs path
        // directly to exercise the copy-through step without needing
        // a `RemoteMessage` fixture.
        let port_ref: PortRef<()> = PortRef::attest_with_attrs(
            actor_ref.actor_id().port_id(<() as Named>::port()),
            actor_ref.attribution().clone(),
        );
        assert_eq!(
            port_ref.attribution().get(REF_ATTR_TEST_STRING),
            Some(&"mesh_value".to_string()),
        );
        assert_eq!(port_ref.attribution().get(REF_ATTR_TEST_U64), Some(&7u64),);
    }

    // AT-1 (copy-through): `PortRef::into_once()` carries attribution
    // onto the produced `OncePortRef`.
    #[test]
    fn ref_attr_into_once_copy_through() {
        let mut attrs = Attrs::new();
        attrs.set(REF_ATTR_TEST_STRING, "mesh_value".to_string());
        attrs.set(REF_ATTR_TEST_U64, 3u64);
        let port_id = PortId::new(test_actor_id("a"), 42);
        let port_ref: PortRef<()> = PortRef::attest_with_attrs(port_id, attrs);
        let once_ref: OncePortRef<()> = port_ref.into_once();
        assert_eq!(
            once_ref.attribution().get(REF_ATTR_TEST_STRING),
            Some(&"mesh_value".to_string()),
        );
        assert_eq!(once_ref.attribution().get(REF_ATTR_TEST_U64), Some(&3u64),);
    }

    // AT-4 (identity independence): two refs with identical identity
    // fields but different attribution compare equal, order equal,
    // and hash equal.
    #[test]
    fn ref_attr_identity_ignores_attribution() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher as _;

        let actor_id = test_actor_id("a");
        let bare: ActorRef<RefAttrTestActor> = ActorRef::attest(actor_id.clone());
        let mut attrs = Attrs::new();
        attrs.set(REF_ATTR_TEST_STRING, "anything".to_string());
        let decorated: ActorRef<RefAttrTestActor> =
            ActorRef::attest_with_attrs(actor_id.clone(), attrs);

        assert_eq!(bare, decorated, "PartialEq must ignore attribution");
        assert_eq!(
            bare.cmp(&decorated),
            std::cmp::Ordering::Equal,
            "Ord must ignore attribution",
        );
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        bare.hash(&mut h1);
        decorated.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish(), "Hash must ignore attribution");

        // Same check for PortRef (which uses `derivative` ignore).
        let port_id = PortId::new(actor_id, 1);
        let bare_port: PortRef<()> = PortRef::attest(port_id.clone());
        let mut attrs2 = Attrs::new();
        attrs2.set(REF_ATTR_TEST_U64, 99u64);
        let decorated_port: PortRef<()> = PortRef::attest_with_attrs(port_id, attrs2);
        assert_eq!(bare_port, decorated_port);
    }

    // AT-1: `ActorRef` wire format includes the attribution carrier,
    // so the transport-attribution keys survive serde round-trip
    // alongside the actor id.
    #[test]
    fn ref_attr_wire_round_trip_actor_ref() {
        let mut attrs = Attrs::new();
        attrs.set(REF_ATTR_TEST_STRING, "mesh_value".to_string());
        attrs.set(REF_ATTR_TEST_U64, 11u64);
        let original: ActorRef<RefAttrTestActor> =
            ActorRef::attest_with_attrs(test_actor_id("a"), attrs);

        let bytes = bincode::serde::encode_to_vec(&original, bincode::config::legacy())
            .expect("serialize ActorRef");
        let restored: ActorRef<RefAttrTestActor> =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy())
                .map(|(v, _)| v)
                .expect("deserialize ActorRef");

        assert_eq!(restored.actor_id(), original.actor_id());
        assert_eq!(
            restored.attribution().get(REF_ATTR_TEST_STRING),
            Some(&"mesh_value".to_string()),
        );
        assert_eq!(restored.attribution().get(REF_ATTR_TEST_U64), Some(&11u64),);
    }

    // AT-1: same for `PortRef` (derived serde picks up the
    // attribution field; `#[derivative(... = "ignore")]` only affects
    // equality/hash/order, not serialization).
    #[test]
    fn ref_attr_wire_round_trip_port_ref() {
        let mut attrs = Attrs::new();
        attrs.set(REF_ATTR_TEST_STRING, "mesh_value".to_string());
        let original: PortRef<()> =
            PortRef::attest_with_attrs(PortId::new(test_actor_id("a"), 7), attrs);
        let bytes = bincode::serde::encode_to_vec(&original, bincode::config::legacy())
            .expect("serialize PortRef");
        let restored: PortRef<()> =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy())
                .map(|(v, _)| v)
                .expect("deserialize PortRef");
        assert_eq!(restored.port_id(), original.port_id());
        assert_eq!(
            restored.attribution().get(REF_ATTR_TEST_STRING),
            Some(&"mesh_value".to_string()),
        );
    }

    // AT-1: same for `OncePortRef`.
    #[test]
    fn ref_attr_wire_round_trip_once_port_ref() {
        let mut attrs = Attrs::new();
        attrs.set(REF_ATTR_TEST_U64, 5u64);
        let original: OncePortRef<()> =
            OncePortRef::attest_with_attrs(PortId::new(test_actor_id("a"), 7), attrs);
        let bytes = bincode::serde::encode_to_vec(&original, bincode::config::legacy())
            .expect("serialize OncePortRef");
        let restored: OncePortRef<()> =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy())
                .map(|(v, _)| v)
                .expect("deserialize OncePortRef");
        assert_eq!(restored.attribution().get(REF_ATTR_TEST_U64), Some(&5u64),);
    }

    // AT-1 (overwrite on stamp): the ref's attribution is the
    // authoritative destination carrier for the ref's own send
    // path, so the shared marker-driven stamping mechanism must
    // overwrite any colliding pre-existing attribution-header
    // value on the outbound `Flattrs` rather than append.
    // Append-then-`Flattrs::get` (first match) would silently
    // return a stale upstream value.
    //
    // Exercises the private `stamp_ref_attribution_onto` wrapper
    // that `PortRef::send_serialized` and
    // `OncePortRef::send_with_headers` both call — one of the
    // transport call sites that use the shared mechanism.
    #[test]
    fn ref_attr_stamp_overwrites_preexisting_headers() {
        // Start with headers that already carry a colliding entry
        // under the test key.
        let mut headers = Flattrs::new();
        headers.set(REF_ATTR_TEST_STRING, "stale_upstream_value".to_string());
        headers.set(REF_ATTR_TEST_U64, 1u64);

        // Ref attribution carries different values under the same
        // keys plus an additional key.
        let mut ref_attrs = Attrs::new();
        ref_attrs.set(REF_ATTR_TEST_STRING, "ref_wins".to_string());
        ref_attrs.set(REF_ATTR_TEST_U64, 99u64);

        // Private helper: scope-local call; asserts the ref-side
        // write path overwrites (not appends).
        stamp_ref_attribution_onto(&mut headers, &ref_attrs);

        // Both collisions now show the ref value, not the stale one.
        assert_eq!(
            headers.get(REF_ATTR_TEST_STRING),
            Some("ref_wins".to_string()),
        );
        assert_eq!(headers.get(REF_ATTR_TEST_U64), Some(99u64));
    }

    // AT-1 (overwrite on stamp), different-size path. The
    // pre-existing and replacement values differ in serialized
    // length, exercising the compact-and-append branch of
    // `Flattrs::set_serialized` (distinct from the in-place
    // overwrite branch the previous test covers).
    #[test]
    fn ref_attr_stamp_overwrites_different_size() {
        let mut headers = Flattrs::new();
        headers.set(REF_ATTR_TEST_STRING, "x".to_string());

        let mut ref_attrs = Attrs::new();
        ref_attrs.set(
            REF_ATTR_TEST_STRING,
            "a considerably longer ref value".to_string(),
        );

        stamp_ref_attribution_onto(&mut headers, &ref_attrs);

        assert_eq!(
            headers.get(REF_ATTR_TEST_STRING),
            Some("a considerably longer ref value".to_string()),
        );
    }

    // AT-1 (declared-key-only transport), mechanical enforcement
    // by the shared marker-driven stamping mechanism. Ref
    // attribution carrying both a declared-with-`ATTRIBUTION_HEADER`
    // attr and a declared-without-marker attr yields outbound
    // headers with **only** the marked entry. The unmarked entry is
    // silently dropped — no opt-out producer discipline required
    // anywhere the mechanism is used.
    #[test]
    fn ref_attr_stamp_filters_unmarked_entries() {
        // Sanity: the declarations above carry / lack the marker
        // as the test expects.
        assert!(
            REF_ATTR_TEST_STRING
                .attrs()
                .get(ATTRIBUTION_HEADER)
                .is_some()
        );
        assert!(
            REF_ATTR_TEST_UNMARKED
                .attrs()
                .get(ATTRIBUTION_HEADER)
                .is_none()
        );

        let mut ref_attrs = Attrs::new();
        ref_attrs.set(REF_ATTR_TEST_STRING, "marked_value".to_string());
        ref_attrs.set(REF_ATTR_TEST_UNMARKED, "unmarked_value".to_string());

        let mut headers = Flattrs::new();
        stamp_ref_attribution_onto(&mut headers, &ref_attrs);

        assert_eq!(
            headers.get(REF_ATTR_TEST_STRING),
            Some("marked_value".to_string()),
            "marked transport key must be stamped onto outbound headers",
        );
        assert_eq!(
            headers.get::<String>(REF_ATTR_TEST_UNMARKED),
            None,
            "unmarked key must be silently filtered out by the shared marker-driven stamping mechanism",
        );
    }
}
