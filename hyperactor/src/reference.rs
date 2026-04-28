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
use std::num::ParseIntError;
use std::str::FromStr;

use enum_as_inner::EnumAsInner;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;

use crate::accum::ReducerMode;
use crate::accum::ReducerSpec;
use crate::actor::Referable;
use crate::addr;
use crate::channel::ChannelAddr;
use crate::context;
use crate::context::MailboxExt;
use crate::id::Label;
use crate::id::Uid;

pub mod lex;
pub mod name;
mod parse;

use parse::ParseError;
pub use parse::is_valid_ident;

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
/// - `proc@location`,
/// - `actor.proc@location`,
/// - `actor.proc:port@location`
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // References use the ref_:: format: "id@location".
        // Try most specific first: Port (has `:` in id), Actor (has `.`), Proc.
        if let Ok(port_ref) = s.parse::<addr::PortAddr>() {
            return Ok(Self::Port(PortId(port_ref)));
        }
        if let Ok(actor_ref) = s.parse::<addr::ActorAddr>() {
            return Ok(Self::Actor(ActorId(actor_ref)));
        }
        if let Ok(proc_ref) = s.parse::<addr::ProcAddr>() {
            return Ok(Self::Proc(ProcId(proc_ref)));
        }
        Err(ReferenceParsingError::Unexpected(format!(
            "could not parse reference: {}",
            s
        )))
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

/// Parse a proc resource name into a `(Uid, Option<Label>)` pair.
///
/// Accepts both the legacy compat forms used by `reference::ProcId::Display`
/// (`_label`, `label-uid58`, `uid58`) and the newer labeled-id forms
/// (`label`, `label<uid58>`, `<uid58>`). Falls back to a stripped singleton
/// label for non-matching input.
fn parse_resource_name(s: &str) -> (Uid, Option<Label>) {
    fn parse_wrapped_instance_uid(s: &str) -> Option<u64> {
        let wrapped = format!("<{s}>");
        match Uid::from_str(&wrapped) {
            Ok(Uid::Instance(uid)) => Some(uid),
            _ => None,
        }
    }

    if let Some(rest) = s.strip_prefix('_') {
        if let Ok(label) = Label::new(rest) {
            return (Uid::Singleton(label.clone()), Some(label));
        }
        let label = Label::strip(rest);
        return (Uid::Singleton(label.clone()), Some(label));
    }

    if let Ok(Uid::Instance(uid)) = Uid::from_str(s) {
        return (Uid::Instance(uid), None);
    }

    if let Some((label_part, uid_part)) = s.rsplit_once('-')
        && uid_part.len() >= 8
    {
        if let (Ok(label), Ok(Uid::Instance(uid))) =
            (Label::new(label_part), Uid::from_str(uid_part))
        {
            return (Uid::Instance(uid), Some(label));
        }
        if let (Ok(label), Some(uid)) =
            (Label::new(label_part), parse_wrapped_instance_uid(uid_part))
        {
            return (Uid::Instance(uid), Some(label));
        }
    }

    if let Some(inner) = s
        .strip_prefix('<')
        .and_then(|inner| inner.strip_suffix('>'))
    {
        if let Ok(Uid::Instance(uid)) = Uid::from_str(inner) {
            return (Uid::Instance(uid), None);
        }
    }

    if let Some((label_part, uid_part)) =
        s.strip_suffix('>').and_then(|inner| inner.split_once('<'))
    {
        if let (Ok(label), Ok(Uid::Instance(uid))) =
            (Label::new(label_part), Uid::from_str(uid_part))
        {
            return (Uid::Instance(uid), Some(label));
        }
    }

    if let Ok(label) = Label::new(s) {
        return (Uid::Singleton(label.clone()), Some(label));
    }

    if let Some(label_part) = s
        .strip_suffix('>')
        .and_then(|inner| inner.split_once('<'))
        .map(|(label_part, _)| label_part)
    {
        let label = Label::strip(label_part);
        return (Uid::Singleton(label.clone()), Some(label));
    }

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
pub struct ProcId(addr::ProcAddr);

impl ProcId {
    /// Create a ProcId with a unique (random) uid and the given label.
    pub fn unique(addr: ChannelAddr, base_name: impl AsRef<str>) -> Self {
        let label = Label::strip(base_name.as_ref());
        Self(addr::ProcAddr::new(
            crate::id::ProcId::instance(label),
            addr::Location::from(addr),
        ))
    }

    /// Create a ProcId by parsing a name string in ResourceId format.
    ///
    /// Recognizes both compatibility and current display forms:
    /// - `label` or `_label` → singleton uid
    /// - `label-uid58` or `label<uid58>` → labeled instance uid
    /// - `uid58` or `<uid58>` → unlabeled instance uid
    ///
    /// Falls back to `Uid::Singleton(Label::strip(name))` if none match.
    pub fn from_resource_name(addr: ChannelAddr, name: impl AsRef<str>) -> Self {
        let s = name.as_ref();
        let (uid, label) = parse_resource_name(s);
        Self(addr::ProcAddr::new(
            crate::id::ProcId::new(uid, label),
            addr::Location::from(addr),
        ))
    }

    /// Wrap an existing [`addr::ProcAddr`].
    pub fn from_proc_ref(proc_ref: addr::ProcAddr) -> Self {
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
    pub fn proc_ref(&self) -> &addr::ProcAddr {
        &self.0
    }

    /// The proc's uid.
    pub fn uid(&self) -> &Uid {
        self.0.id().uid()
    }

    /// The proc's label: the explicit metadata label for instances,
    /// or the singleton name for singletons.
    pub fn label(&self) -> Option<&Label> {
        self.0.id().label().or_else(|| match self.0.id().uid() {
            Uid::Singleton(label) => Some(label),
            _ => None,
        })
    }

    /// A human-readable name for logging, derived from the label or uid.
    pub fn log_name(&self) -> &str {
        self.label().map(|l| l.as_str()).unwrap_or("?")
    }

    /// The ResourceId text form of this proc's identity.
    ///
    /// Produces `label` (singleton), `label-uid58` (labeled instance),
    /// or `<uid58>` (unlabeled instance). Labeled instances match the
    /// `Display` output of `mesh_id::ResourceId` and are suitable for
    /// filesystem paths. All forms round-trip through `parse_resource_name`.
    pub fn resource_name(&self) -> String {
        let id = self.0.id();
        fn uid_no_brackets(uid: &Uid) -> String {
            uid.to_string()
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        }
        match (id.uid(), id.label()) {
            (Uid::Singleton(label), _) => label.to_string(),
            (uid @ Uid::Instance(_), Some(label)) => format!("{label}-{}", uid_no_brackets(uid)),
            (uid @ Uid::Instance(_), None) => uid.to_string(),
        }
    }
}

impl fmt::Display for ProcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<ProcId> for addr::ProcAddr {
    fn from(p: ProcId) -> Self {
        p.0
    }
}

impl From<addr::ProcAddr> for ProcId {
    fn from(p: addr::ProcAddr) -> Self {
        Self(p)
    }
}

impl std::ops::Deref for ProcId {
    type Target = addr::ProcAddr;
    fn deref(&self) -> &addr::ProcAddr {
        &self.0
    }
}

impl FromStr for ProcId {
    type Err = ReferenceParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((addr, proc_name)) = s.split_once(',')
            && !proc_name.contains(',')
        {
            if let Ok(channel_addr) = addr.parse::<ChannelAddr>() {
                return Ok(Self::from_resource_name(channel_addr, proc_name));
            }
        }

        let proc_ref: addr::ProcAddr = s
            .parse()
            .map_err(|e| ReferenceParsingError::Unexpected(format!("{}", e)))?;
        Ok(Self(proc_ref))
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
pub struct ActorId(addr::ActorAddr);

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
        Self(addr::ActorAddr::new(actor_id, proc_id.0.location().clone()))
    }

    /// Create a new port ID with the provided port for this actor.
    pub fn port_id(&self, port: u64) -> PortId {
        let port_id = crate::id::PortId::new(self.0.id().clone(), crate::port::Port::from(port));
        PortId(addr::PortAddr::new(port_id, self.0.location().clone()))
    }

    /// Create a child actor ID with a random uid.
    pub fn unique_child_id(&self) -> Self {
        let child = crate::id::ActorId::instance(self.0.id().proc_id().clone());
        Self(addr::ActorAddr::new(child, self.0.location().clone()))
    }

    /// Return the root actor ID for the provided proc and label.
    pub fn root(proc_id: ProcId, label: Label) -> Self {
        let actor_id = crate::id::ActorId::singleton(label, proc_id.0.id().clone());
        Self(addr::ActorAddr::new(actor_id, proc_id.0.location().clone()))
    }

    /// Wrap an existing [`addr::ActorAddr`].
    pub fn from_actor_ref(actor_ref: addr::ActorAddr) -> Self {
        Self(actor_ref)
    }

    /// The proc ID of this actor ID.
    pub fn proc_id(&self) -> ProcId {
        ProcId(addr::ProcAddr::new(
            self.0.id().proc_id().clone(),
            self.0.location().clone(),
        ))
    }

    /// The underlying actor identity.
    pub fn id(&self) -> &crate::id::ActorId {
        self.0.id()
    }

    /// The underlying actor reference.
    pub fn actor_ref(&self) -> &addr::ActorAddr {
        &self.0
    }

    /// The actor's uid.
    pub fn uid(&self) -> &Uid {
        self.0.id().uid()
    }

    /// The actor's label.
    pub fn label(&self) -> Option<&Label> {
        self.0.id().label().or_else(|| match self.0.id().uid() {
            Uid::Singleton(label) => Some(label),
            _ => None,
        })
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
        self.label().map(|l| l.as_str()).unwrap_or("?")
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let actor_ref: addr::ActorAddr = s
            .parse()
            .map_err(|e| ReferenceParsingError::Unexpected(format!("{}", e)))?;
        Ok(Self(actor_ref))
    }
}

impl From<ActorId> for addr::ActorAddr {
    fn from(a: ActorId) -> Self {
        a.0
    }
}

impl From<addr::ActorAddr> for ActorId {
    fn from(a: addr::ActorAddr) -> Self {
        Self(a)
    }
}

impl std::ops::Deref for ActorId {
    type Target = addr::ActorAddr;
    fn deref(&self) -> &addr::ActorAddr {
        &self.0
    }
}

pub use crate::ref_::ActorRef;
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
pub struct PortId(addr::PortAddr);

impl PortId {
    /// Create a new port ID.
    pub fn new(actor_id: ActorId, port: u64) -> Self {
        let port_id =
            crate::id::PortId::new(actor_id.0.id().clone(), crate::port::Port::from(port));
        Self(addr::PortAddr::new(port_id, actor_id.0.location().clone()))
    }

    /// The ID of the port's owning actor.
    pub fn actor_id(&self) -> ActorId {
        ActorId(addr::ActorAddr::new(
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
            self.clone().into(),
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
            self.clone().into(),
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
            self.clone().into(),
            reducer_spec,
            reducer_mode,
            return_undeliverable,
        )
        .map(|p| p.into())
    }
}

impl FromStr for PortId {
    type Err = ReferenceParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let port_ref: addr::PortAddr = s
            .parse()
            .map_err(|e| ReferenceParsingError::Unexpected(format!("{}", e)))?;
        Ok(Self(port_ref))
    }
}

impl fmt::Display for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<PortId> for addr::PortAddr {
    fn from(p: PortId) -> Self {
        p.0
    }
}

impl From<addr::PortAddr> for PortId {
    fn from(p: addr::PortAddr) -> Self {
        Self(p)
    }
}

impl std::ops::Deref for PortId {
    type Target = addr::PortAddr;
    fn deref(&self) -> &addr::PortAddr {
        &self.0
    }
}

// Also bridge Reference <-> addr::Address.
impl From<Reference> for addr::Address {
    fn from(r: Reference) -> Self {
        match r {
            Reference::Proc(p) => addr::Address::Proc(p.0),
            Reference::Actor(a) => addr::Address::Actor(a.0),
            Reference::Port(p) => addr::Address::Port(p.0),
        }
    }
}

impl From<addr::Address> for Reference {
    fn from(r: addr::Address) -> Self {
        match r {
            addr::Address::Proc(p) => Reference::Proc(ProcId(p)),
            addr::Address::Actor(a) => Reference::Actor(ActorId(a)),
            addr::Address::Port(p) => Reference::Port(PortId(p)),
        }
    }
}

pub use crate::ref_::OncePortRef;
pub use crate::ref_::PortRef;
pub use crate::ref_::UnboundPort;
pub use crate::ref_::UnboundPortKind;
#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;
    // for macros
    use crate::Proc;
    use crate::addr;
    use crate::context::Mailbox as _;
    use crate::id::Label;
    use crate::id::ProcId as RawProcId;
    use crate::id::Uid;
    use crate::mailbox::PortLocation;
    use crate::ordering::SEQ_INFO;
    use crate::ordering::SeqInfo;

    #[test]
    fn test_reference_parse() {
        // New format: "uid@location" for Proc, "actor.proc@loc" for Actor,
        // "actor.proc:port@loc" for Port.
        let addr: ChannelAddr = "tcp:[::1]:1234".parse().unwrap();
        let _loc = "tcp://[::1]:1234".to_string();

        let proc_id = ProcId::from_resource_name(addr.clone(), "test");
        let proc_str = proc_id.to_string();
        assert_eq!(
            proc_str.parse::<Reference>().unwrap(),
            Reference::Proc(proc_id.clone())
        );

        let actor_id = ActorId::new(proc_id.clone(), "testactor");
        let actor_str = actor_id.to_string();
        assert_eq!(
            actor_str.parse::<Reference>().unwrap(),
            Reference::Actor(actor_id.clone())
        );

        let port_id = PortId::new(actor_id.clone(), 123);
        let port_str = port_id.to_string();
        assert_eq!(
            port_str.parse::<Reference>().unwrap(),
            Reference::Port(port_id)
        );
    }

    #[test]
    fn test_reference_display_roundtrip() {
        let addr: ChannelAddr = "tcp:[::1]:1234".parse().unwrap();

        let proc_id = ProcId::from_resource_name(addr.clone(), "test");
        let proc_str = proc_id.to_string();
        assert_eq!(proc_str.parse::<ProcId>().unwrap(), proc_id);

        let actor_id = ActorId::new(proc_id.clone(), "myactor");
        let actor_str = actor_id.to_string();
        assert_eq!(actor_str.parse::<ActorId>().unwrap(), actor_id);

        let port_id = PortId::new(actor_id.clone(), 42);
        let port_str = port_id.to_string();
        assert_eq!(port_str.parse::<PortId>().unwrap(), port_id);
    }

    #[test]
    fn test_actor_label_roundtrip_preserves_singleton_names() {
        let addr: ChannelAddr = "tcp:[::1]:1234".parse().unwrap();
        let proc_id = ProcId::from_resource_name(addr, "service");
        let actor_id = ActorId::new(proc_id, "host_agent");

        let parsed: ActorId = actor_id.to_string().parse().unwrap();
        assert_eq!(
            parsed.label().map(|label| label.as_str()),
            Some("host_agent")
        );
        assert_eq!(
            parsed.proc_id().label().map(|label| label.as_str()),
            Some("service")
        );
    }

    #[test]
    fn test_reference_parse_error() {
        let cases: Vec<&str> = vec!["(blah)", "world(1, 2, 3)", "no-at-sign"];

        for s in cases {
            let result: Result<Reference, ReferenceParsingError> = s.parse();
            assert!(result.is_err(), "expected error for: {}", s);
        }
    }

    #[test]
    fn test_reference_ord() {
        let addr: ChannelAddr = "tcp:[::1]:1234".parse().unwrap();
        let expected: Vec<Reference> = ["first", "second", "third"]
            .into_iter()
            .map(|name| Reference::Proc(ProcId::from_resource_name(addr.clone(), name)))
            .collect();

        let mut sorted = expected.to_vec();
        sorted.shuffle(&mut rand::rng());
        sorted.sort();

        assert_eq!(sorted, expected);
    }

    #[test]
    fn test_port_display() {
        let addr: ChannelAddr = "tcp:[::1]:1234".parse().unwrap();
        let port_id = PortId::new(
            ActorId::new(ProcId::from_resource_name(addr, "test"), "testactor"),
            42,
        );
        let s = port_id.to_string();
        // Format: "actor_uid.proc_uid:port@location"
        assert!(s.contains("@tcp://"), "expected @ separator: {}", s);
        assert!(s.contains(":42@"), "expected port 42: {}", s);
    }

    #[test]
    fn test_proc_id_display_roundtrip_singleton() {
        let addr = "tcp:[::1]:1234".parse::<ChannelAddr>().unwrap();
        let proc_id = ProcId::from_resource_name(addr, "test");
        let s = proc_id.to_string();
        assert_eq!(s, "test@tcp://[::1]:1234");
        assert_eq!(s.parse::<ProcId>().unwrap(), proc_id);
    }

    #[test]
    fn test_proc_id_display_roundtrip_labeled_instance() {
        let addr = "tcp:[::1]:1234".parse::<ChannelAddr>().unwrap();
        let proc_id = ProcId::from_proc_ref(addr::ProcAddr::new(
            RawProcId::new(Uid::Instance(0x123), Some(Label::new("worker").unwrap())),
            addr::Location::from(addr),
        ));
        let s = proc_id.to_string();
        assert_eq!(s, "worker<62>@tcp://[::1]:1234");
        assert_eq!(s.parse::<ProcId>().unwrap(), proc_id);
    }

    #[test]
    fn test_proc_id_display_roundtrip_unlabeled_instance() {
        let addr = "tcp:[::1]:1234".parse::<ChannelAddr>().unwrap();
        let proc_id = ProcId::from_proc_ref(addr::ProcAddr::new(
            RawProcId::new(Uid::Instance(0x123), None),
            addr::Location::from(addr),
        ));
        let s = proc_id.to_string();
        assert_eq!(s, "<62>@tcp://[::1]:1234");
        assert_eq!(s.parse::<ProcId>().unwrap(), proc_id);
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
        let port_ref_val = match port_handle.location() {
            PortLocation::Bound(port_ref) => port_ref,
            _ => panic!("port_handle should be bound"),
        };
        assert!(port_ref_val.is_actor_port());
        let port_ref = PortRef::attest(port_ref_val.clone().into());

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

            // From port_ref
            let port_id_conv: PortId = port_ref_val.clone().into();
            for _ in 0..3 {
                port_id_conv.send(&client, wirevalue::Any::serialize(&()).unwrap());
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
        let port_ref_val = match port_handle.location() {
            PortLocation::Bound(port_ref) => port_ref,
            _ => panic!("port_handle should be bound"),
        };
        // This is a non-actor port, but it still gets seq info (per-port sequence).
        assert!(!port_ref_val.is_actor_port());

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

        let port_ref = PortRef::attest(port_ref_val.clone().into());
        port_ref.send(&client, ()).unwrap();
        let SeqInfo::Session { seq: seq2, .. } = rx
            .try_recv()
            .unwrap()
            .expect("non-actor port should have seq info")
        else {
            panic!("expected Session variant");
        };
        assert_eq!(seq2, 2);

        let port_id_conv: PortId = port_ref_val.clone().into();
        port_id_conv.send(&client, wirevalue::Any::serialize(&()).unwrap());
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
