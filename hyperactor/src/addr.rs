/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! References: identifiers paired with a network location.
//!
//! Concrete grammar:
//!
//! ```text
//! location     := zmq URL understood by [`ChannelAddr::from_zmq_url`]
//! proc-ref     := proc-id "@" location
//! actor-ref    := actor-id "@" location
//! port-ref     := port-id "@" location
//!
//! proc-id      := label | "<" uid58 ">" | label "<" uid58 ">"
//! actor-id     := actor-part "." proc-id
//! actor-part   := label | "<" uid58 ">" | label "<" uid58 ">"
//! ```
//!
//! Examples:
//!
//! ```text
//! local@inproc://0
//! controller<2MuAHeDjLCEd>@tcp://[::1]:2345
//! controller<2MuAHeDjLCEd>.local@inproc://0
//! <2MuAHeDjLCEd>.<NRjEZGYjYibf>:42@tcp://[::1]:2345
//! ```

use std::fmt;
use std::str::FromStr;

use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::channel::ChannelAddr;
use crate::context::MailboxExt;
use crate::id;
use crate::id::ActorId;
use crate::id::IdParseError;
use crate::id::Label;
use crate::id::PortId;
use crate::id::ProcId;
use crate::id::Uid;
use crate::parse;
use crate::port::Port;

/// A network location, wrapping a [`ChannelAddr`].
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Location(ChannelAddr);

impl Location {
    /// Returns the underlying channel address.
    pub fn addr(&self) -> &ChannelAddr {
        &self.0
    }
}

impl From<ChannelAddr> for Location {
    fn from(addr: ChannelAddr) -> Self {
        Self(addr)
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.to_zmq_url())
    }
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Location {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ChannelAddr::from_zmq_url(s).map(Self)
    }
}

/// Errors that can occur when parsing a [`ProcAddr`] or [`ActorAddr`].
#[derive(Debug, thiserror::Error)]
pub enum AddrParseError {
    /// The `@` separator between id and location is missing.
    #[error("missing '@' separator between id and location")]
    MissingSeparator,
    /// The id portion is invalid.
    #[error("invalid id: {0}")]
    InvalidId(#[from] IdParseError),
    /// The location portion is invalid.
    #[error("invalid location: {0}")]
    InvalidLocation(#[source] anyhow::Error),
}

/// A process identifier paired with a network location.
#[derive(Clone, Serialize, Deserialize, typeuri::Named)]
pub struct ProcAddr {
    id: ProcId,
    location: Location,
}

impl ProcAddr {
    /// Create a new [`ProcAddr`].
    pub fn new(id: ProcId, location: Location) -> Self {
        Self { id, location }
    }

    /// Returns the process id.
    pub fn id(&self) -> &ProcId {
        &self.id
    }

    /// Returns the location.
    pub fn location(&self) -> &Location {
        &self.location
    }

    /// The proc's channel address.
    pub fn addr(&self) -> &ChannelAddr {
        self.location.addr()
    }

    /// The proc's uid.
    pub fn uid(&self) -> &Uid {
        self.id.uid()
    }

    /// The proc's label: the explicit metadata label for instances,
    /// or the singleton name for singletons.
    pub fn label(&self) -> Option<&Label> {
        self.id.label()
    }

    /// Create a ProcAddr with an anonymous instance proc id.
    pub fn anonymous(addr: ChannelAddr) -> Self {
        Self::new(id::ProcId::anonymous(), Location::from(addr))
    }

    /// Create a ProcAddr with an instance proc id and the given display label.
    pub fn instance(addr: ChannelAddr, base_name: impl AsRef<str>) -> Self {
        let label = Label::strip(base_name.as_ref());
        Self::new(id::ProcId::instance(label), Location::from(addr))
    }

    /// Create a ProcAddr with a singleton proc id identified by the given name.
    pub fn singleton(addr: ChannelAddr, name: impl AsRef<str>) -> Self {
        Self::new(
            id::ProcId::singleton(Label::strip(name.as_ref())),
            Location::from(addr),
        )
    }

    /// Create a ProcAddr with a unique (random) uid and the given label.
    pub fn unique(addr: ChannelAddr, base_name: impl AsRef<str>) -> Self {
        Self::instance(addr, base_name)
    }

    /// Create a ProcAddr singleton with a label stripped from the given name.
    pub fn named(addr: ChannelAddr, name: impl AsRef<str>) -> Self {
        Self::singleton(addr, name)
    }

    /// Create an ActorAddr singleton with the provided name within this proc.
    pub fn actor_addr(&self, name: impl AsRef<str>) -> ActorAddr {
        let uid = Uid::singleton(Label::strip(name.as_ref()));
        self.actor_addr_uid(uid)
    }

    /// Create an ActorAddr with the provided uid within this proc.
    pub fn actor_addr_uid(&self, uid: Uid) -> ActorAddr {
        ActorAddr::new_from_uid(self.clone(), uid)
    }

    /// A human-readable name for logging.
    pub fn log_name(&self) -> &str {
        self.label().map(|l| l.as_str()).unwrap_or("?")
    }
}

impl PartialEq for ProcAddr {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.location == other.location
    }
}

impl Eq for ProcAddr {}

impl std::hash::Hash for ProcAddr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.location.hash(state);
    }
}

impl PartialOrd for ProcAddr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProcAddr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.location.cmp(&other.location))
    }
}

impl fmt::Display for ProcAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

impl fmt::Debug for ProcAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.id.label() {
            Some(label) => write!(f, "<'{}' {}@{}>", label, self.id, self.location),
            None => write!(f, "<{}@{}>", self.id, self.location),
        }
    }
}

impl FromStr for ProcAddr {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse::addr::parse_proc_addr(s).map_err(|_| legacy_parse_proc_ref(s))
    }
}

/// An actor identifier paired with a network location.
#[derive(Clone, Serialize, Deserialize, typeuri::Named)]
pub struct ActorAddr {
    id: ActorId,
    location: Location,
}

hyperactor_config::impl_attrvalue!(ActorAddr);

impl ActorAddr {
    /// Create a new [`ActorAddr`].
    pub fn new(id: ActorId, location: Location) -> Self {
        Self { id, location }
    }

    /// Create an ActorAddr from a ProcAddr and actor uid.
    pub fn new_from_uid(proc_ref: ProcAddr, uid: Uid) -> Self {
        let actor_id = id::ActorId::new(uid, proc_ref.id.clone(), None);
        Self::new(actor_id, proc_ref.location)
    }

    /// Returns the actor id.
    pub fn id(&self) -> &ActorId {
        &self.id
    }

    /// Returns the proc id that owns this actor id.
    pub fn proc_id(&self) -> &ProcId {
        self.id.proc_id()
    }

    /// Returns the location.
    pub fn location(&self) -> &Location {
        &self.location
    }

    /// The actor's channel address.
    pub fn addr(&self) -> &ChannelAddr {
        self.location.addr()
    }

    /// The actor's uid.
    pub fn uid(&self) -> &Uid {
        self.id.uid()
    }

    /// The actor's label: explicit metadata label for instances,
    /// or singleton name for singletons.
    pub fn label(&self) -> Option<&Label> {
        self.id.label()
    }

    /// Reconstruct the parent ProcAddr (with location preserved).
    pub fn proc_addr(&self) -> ProcAddr {
        ProcAddr::new(self.id.proc_id().clone(), self.location.clone())
    }

    /// Create a PortAddr for a port on this actor.
    pub fn port_addr(&self, port: Port) -> PortAddr {
        PortAddr::new(
            id::PortId::new(self.id.clone(), port),
            self.location.clone(),
        )
    }

    /// Create an ActorAddr for a root actor on a proc.
    pub fn root(proc_ref: ProcAddr, label: impl Into<Label>) -> Self {
        let label = label.into();
        let actor_id = id::ActorId::singleton(label, proc_ref.id.clone());
        Self::new(actor_id, proc_ref.location)
    }

    /// Create an ActorAddr for a child actor with a random uid.
    pub fn unique_child(&self) -> Self {
        let child_id = id::ActorId::instance(self.id.proc_id().clone());
        Self::new(child_id, self.location.clone())
    }

    /// Whether this is a root actor (singleton uid).
    pub fn is_root(&self) -> bool {
        self.id.uid().is_singleton()
    }

    /// A human-readable name for logging.
    pub fn log_name(&self) -> &str {
        self.label().map(|l| l.as_str()).unwrap_or("?")
    }
}

impl PartialEq for ActorAddr {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.location == other.location
    }
}

impl Eq for ActorAddr {}

impl std::hash::Hash for ActorAddr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.location.hash(state);
    }
}

impl PartialOrd for ActorAddr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ActorAddr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.location.cmp(&other.location))
    }
}

impl fmt::Display for ActorAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

impl fmt::Debug for ActorAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.id.label(), self.id.proc_id().label()) {
            (Some(actor_label), Some(proc_label)) => {
                write!(
                    f,
                    "<'{}.{}' {}@{}>",
                    actor_label, proc_label, self.id, self.location
                )
            }
            (Some(actor_label), None) => {
                write!(f, "<'{}' {}@{}>", actor_label, self.id, self.location)
            }
            (None, Some(proc_label)) => {
                write!(f, "<'.{}' {}@{}>", proc_label, self.id, self.location)
            }
            (None, None) => {
                write!(f, "<{}@{}>", self.id, self.location)
            }
        }
    }
}

impl FromStr for ActorAddr {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse::addr::parse_actor_addr(s).map_err(|_| legacy_parse_actor_ref(s))
    }
}

/// A port identifier paired with a network location.
#[derive(Clone, Serialize, Deserialize, typeuri::Named)]
pub struct PortAddr {
    id: PortId,
    location: Location,
}

impl PortAddr {
    /// Create a new [`PortAddr`].
    pub fn new(id: PortId, location: Location) -> Self {
        Self { id, location }
    }

    /// Returns the port id.
    pub fn id(&self) -> &PortId {
        &self.id
    }

    /// Returns the location.
    pub fn location(&self) -> &Location {
        &self.location
    }

    /// Returns the actor id (delegates to port id).
    pub fn actor_id(&self) -> &ActorId {
        self.id.actor_id()
    }

    /// Returns the proc id that owns this port id.
    pub fn proc_id(&self) -> &ProcId {
        self.id.actor_id().proc_id()
    }

    /// Whether this is a handler port.
    pub(crate) fn is_handler_port(&self) -> bool {
        self.id.port().is_handler()
    }

    /// The port index.
    pub fn index(&self) -> u64 {
        self.id.port().as_u64()
    }

    /// Reconstruct the parent ActorAddr (with location preserved).
    pub fn actor_addr(&self) -> ActorAddr {
        ActorAddr::new(self.id.actor_id().clone(), self.location.clone())
    }

    /// Reconstruct the parent ActorAddr (with location preserved).
    pub fn actor_ref(&self) -> ActorAddr {
        self.actor_addr()
    }

    /// Send a serialized message to this port, provided a sending capability.
    pub fn send(&self, cx: &impl crate::context::Actor, serialized: wirevalue::Any) {
        let mut headers = hyperactor_config::Flattrs::new();
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        cx.post(
            self.clone(),
            headers,
            serialized,
            true,
            crate::context::SeqInfoPolicy::AssignNew,
        );
    }

    /// Send a serialized message with explicit headers.
    pub fn send_with_headers(
        &self,
        cx: &impl crate::context::Actor,
        serialized: wirevalue::Any,
        mut headers: hyperactor_config::Flattrs,
    ) {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        cx.post(
            self.clone(),
            headers,
            serialized,
            true,
            crate::context::SeqInfoPolicy::AssignNew,
        );
    }

    /// Split this port through a local proxy, possibly reducing messages.
    pub fn split(
        &self,
        cx: &impl crate::context::Actor,
        reducer_spec: Option<crate::accum::ReducerSpec>,
        reducer_mode: crate::accum::ReducerMode,
        return_undeliverable: bool,
    ) -> anyhow::Result<PortAddr> {
        cx.split(
            self.clone(),
            reducer_spec,
            reducer_mode,
            return_undeliverable,
        )
    }
}

impl PartialEq for PortAddr {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.location == other.location
    }
}

impl Eq for PortAddr {}

impl std::hash::Hash for PortAddr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.location.hash(state);
    }
}

impl PartialOrd for PortAddr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PortAddr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.location.cmp(&other.location))
    }
}

impl fmt::Display for PortAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

impl fmt::Debug for PortAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (
            self.id.actor_id().label(),
            self.id.actor_id().proc_id().label(),
        ) {
            (Some(actor_label), Some(proc_label)) => {
                write!(
                    f,
                    "<'{}.{}' {}@{}>",
                    actor_label, proc_label, self.id, self.location
                )
            }
            (Some(actor_label), None) => {
                write!(f, "<'{}' {}@{}>", actor_label, self.id, self.location)
            }
            (None, Some(proc_label)) => {
                write!(f, "<'.{}' {}@{}>", proc_label, self.id, self.location)
            }
            (None, None) => {
                write!(f, "<{}@{}>", self.id, self.location)
            }
        }
    }
}

impl FromStr for PortAddr {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse::addr::parse_port_addr(s).map_err(|_| legacy_parse_port_ref(s))
    }
}

/// A polymorphic reference: proc, actor, or port.
///
/// Used for prefix-based routing in [`MailboxRouter`] and
/// [`DialMailboxRouter`]. Ordering is lexicographic by
/// (proc, actor uid, port).
#[derive(Debug, Clone, EnumAsInner, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Addr {
    /// A process reference.
    Proc(ProcAddr),
    /// An actor reference.
    Actor(ActorAddr),
    /// A port reference.
    Port(PortAddr),
}

impl Addr {
    /// Whether `self` is a prefix of `other`.
    ///
    /// - Proc is a prefix of any Actor or Port on the same proc.
    /// - Actor is a prefix of any Port on the same actor.
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Proc(p), Self::Actor(a)) => *p == a.proc_addr(),
            (Self::Proc(p), Self::Port(pt)) => *p == pt.actor_addr().proc_addr(),
            (Self::Actor(a), Self::Port(pt)) => *a == pt.actor_addr(),
            (Self::Proc(p1), Self::Proc(p2)) => p1 == p2,
            (Self::Actor(a1), Self::Actor(a2)) => a1 == a2,
            (Self::Port(p1), Self::Port(p2)) => p1 == p2,
            _ => false,
        }
    }

    /// The proc addr of this reference.
    pub fn proc_addr(&self) -> ProcAddr {
        match self {
            Self::Proc(p) => p.clone(),
            Self::Actor(a) => a.proc_addr(),
            Self::Port(p) => p.actor_addr().proc_addr(),
        }
    }
}

impl PartialOrd for Addr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Addr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by: proc, then actor uid (None < Some), then port (None < Some).
        let proc_ord = self.proc_addr().cmp(&other.proc_addr());
        if proc_ord != std::cmp::Ordering::Equal {
            return proc_ord;
        }
        let self_actor_uid = match self {
            Self::Proc(_) => None,
            Self::Actor(a) => Some(a.uid()),
            Self::Port(p) => Some(p.actor_id().uid()),
        };
        let other_actor_uid = match other {
            Self::Proc(_) => None,
            Self::Actor(a) => Some(a.uid()),
            Self::Port(p) => Some(p.actor_id().uid()),
        };
        let actor_ord = self_actor_uid.cmp(&other_actor_uid);
        if actor_ord != std::cmp::Ordering::Equal {
            return actor_ord;
        }
        let self_port = match self {
            Self::Port(p) => Some(p.id().port()),
            _ => None,
        };
        let other_port = match other {
            Self::Port(p) => Some(p.id().port()),
            _ => None,
        };
        self_port.cmp(&other_port)
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proc(p) => fmt::Display::fmt(p, f),
            Self::Actor(a) => fmt::Display::fmt(a, f),
            Self::Port(p) => fmt::Display::fmt(p, f),
        }
    }
}

impl FromStr for Addr {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse::addr::parse_addr(s).map_err(|_| legacy_parse_reference(s))
    }
}

fn split_ref_input(s: &str) -> Result<(&str, &str), AddrParseError> {
    let Some((id_text, location_text)) = s.split_once('@') else {
        return Err(AddrParseError::MissingSeparator);
    };
    Ok((id_text, location_text))
}

fn legacy_parse_proc_ref(s: &str) -> AddrParseError {
    let Ok((id_text, location_text)) = split_ref_input(s) else {
        return AddrParseError::MissingSeparator;
    };
    if let Err(err) = id_text.parse::<ProcId>() {
        return AddrParseError::InvalidId(err);
    }
    let location = if location_text.is_empty() {
        "@"
    } else {
        location_text
    };
    let err = location.parse::<Location>().unwrap_err();
    AddrParseError::InvalidLocation(err)
}

fn legacy_parse_actor_ref(s: &str) -> AddrParseError {
    let Ok((id_text, location_text)) = split_ref_input(s) else {
        return AddrParseError::MissingSeparator;
    };
    if let Err(err) = id_text.parse::<ActorId>() {
        return AddrParseError::InvalidId(err);
    }
    let location = if location_text.is_empty() {
        "@"
    } else {
        location_text
    };
    let err = location.parse::<Location>().unwrap_err();
    AddrParseError::InvalidLocation(err)
}

fn legacy_parse_port_ref(s: &str) -> AddrParseError {
    let Ok((id_text, location_text)) = split_ref_input(s) else {
        return AddrParseError::MissingSeparator;
    };
    if let Err(err) = id_text.parse::<PortId>() {
        return AddrParseError::InvalidId(err);
    }
    let location = if location_text.is_empty() {
        "@"
    } else {
        location_text
    };
    let err = location.parse::<Location>().unwrap_err();
    AddrParseError::InvalidLocation(err)
}

fn legacy_parse_reference(s: &str) -> AddrParseError {
    let Ok((id_text, location_text)) = split_ref_input(s) else {
        return AddrParseError::MissingSeparator;
    };
    let location = if location_text.is_empty() {
        "@"
    } else {
        location_text
    };
    let location_err = || {
        let err = location.parse::<Location>().unwrap_err();
        AddrParseError::InvalidLocation(err)
    };

    let port_result = id_text.parse::<PortId>();
    if port_result.is_ok() {
        return location_err();
    }
    let actor_result = id_text.parse::<ActorId>();
    if actor_result.is_ok() {
        return location_err();
    }
    let proc_result = id_text.parse::<ProcId>();
    if proc_result.is_ok() {
        return location_err();
    }

    if id_text.contains(':') {
        return AddrParseError::InvalidId(port_result.unwrap_err());
    }
    if id_text.contains('.') {
        return AddrParseError::InvalidId(actor_result.unwrap_err());
    }

    AddrParseError::InvalidId(proc_result.unwrap_err())
}

impl From<ProcAddr> for Addr {
    fn from(p: ProcAddr) -> Self {
        Self::Proc(p)
    }
}

impl From<ActorAddr> for Addr {
    fn from(a: ActorAddr) -> Self {
        Self::Actor(a)
    }
}

impl From<PortAddr> for Addr {
    fn from(p: PortAddr) -> Self {
        Self::Port(p)
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use super::*;
    use crate::id::Label;
    use crate::id::Uid;
    use crate::port::Port;

    #[test]
    fn test_location_display_fromstr_roundtrip() {
        let loc: Location = ChannelAddr::Local(42).into();
        let s = loc.to_string();
        assert_eq!(s, "inproc://42");
        let parsed: Location = s.parse().unwrap();
        assert_eq!(loc, parsed);
    }

    #[test]
    fn test_location_tcp() {
        let addr: ChannelAddr = "tcp:127.0.0.1:8080".parse().unwrap();
        let loc = Location::from(addr.clone());
        assert_eq!(loc.to_string(), "tcp://127.0.0.1:8080");
        assert_eq!(loc.addr(), &addr);
    }

    #[test]
    fn test_location_debug_same_as_display() {
        let loc: Location = ChannelAddr::Local(7).into();
        assert_eq!(format!("{:?}", loc), format!("{}", loc));
    }

    #[test]
    fn test_proc_ref_display() {
        let pid = ProcId::new(
            Uid::Instance(0xabc123, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcAddr::new(pid, loc);
        assert_eq!(pref.to_string(), format!("{}@inproc://42", pref.id()));
    }

    #[test]
    fn test_proc_addr_identity_constructors() {
        let anonymous = ProcAddr::anonymous(ChannelAddr::Local(1));
        assert!(
            matches!(anonymous.id().uid(), Uid::Instance(_, None)),
            "anonymous proc addr must have an unlabeled instance id"
        );
        assert_eq!(anonymous.label(), None);
        assert_eq!(*anonymous.location().addr(), ChannelAddr::Local(1));

        let instance = ProcAddr::instance(ChannelAddr::Local(2), "worker");
        assert!(
            matches!(
                instance.id().uid(),
                Uid::Instance(_, Some(label)) if label.as_str() == "worker"
            ),
            "instance proc addr must have a labeled instance id"
        );
        assert_eq!(instance.label().map(|label| label.as_str()), Some("worker"));
        assert_eq!(*instance.location().addr(), ChannelAddr::Local(2));

        let singleton = ProcAddr::singleton(ChannelAddr::Local(3), "controller");
        assert!(
            matches!(
                singleton.id().uid(),
                Uid::Singleton(label) if label.as_str() == "controller"
            ),
            "singleton proc addr must have a singleton id"
        );
        assert_eq!(
            singleton.label().map(|label| label.as_str()),
            Some("controller")
        );
        assert_eq!(*singleton.location().addr(), ChannelAddr::Local(3));
    }

    #[test]
    fn test_proc_ref_debug_with_label() {
        let pid = ProcId::new(
            Uid::Instance(0xabc123, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcAddr::new(pid, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'my-proc' {}@inproc://42>", pref.id())
        );
    }

    #[test]
    fn test_proc_ref_debug_without_label() {
        let pid = ProcId::new(Uid::Instance(0xabc123, None), None);
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcAddr::new(pid, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<{}@inproc://42>", pref.id())
        );
    }

    #[test]
    fn test_proc_ref_fromstr_roundtrip() {
        let pid = ProcId::new(
            Uid::Instance(0xabc123, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcAddr::new(pid, loc);
        let s = pref.to_string();
        let parsed: ProcAddr = s.parse().unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_proc_ref_fromstr_tcp() {
        let parsed: ProcAddr = format!(
            "{}@tcp://127.0.0.1:8080",
            ProcId::new(Uid::Instance(0xabc123, None), None)
        )
        .parse()
        .unwrap();
        assert_eq!(*parsed.id().uid(), Uid::Instance(0xabc123, None));
        assert_eq!(
            *parsed.location().addr(),
            "tcp:127.0.0.1:8080".parse::<ChannelAddr>().unwrap()
        );
    }

    #[test]
    fn test_proc_ref_fromstr_examples() {
        let parsed: ProcAddr = "local@inproc://0".parse().unwrap();
        assert_eq!(
            parsed.id().uid(),
            &Uid::singleton(Label::new("local").unwrap())
        );
        assert_eq!(*parsed.location().addr(), ChannelAddr::Local(0));

        let expected_uid = Uid::Instance(0xabc123, None);
        let parsed: ProcAddr = format!("controller{}@tcp://[::1]:2345", expected_uid)
            .parse()
            .unwrap();
        assert_eq!(parsed.id().uid(), &expected_uid);
        assert_eq!(
            parsed.id().label().map(|label| label.as_str()),
            Some("controller")
        );
        assert_eq!(
            *parsed.location().addr(),
            "tcp:[::1]:2345".parse::<ChannelAddr>().unwrap()
        );
    }

    #[test]
    fn test_proc_ref_fromstr_missing_separator() {
        let err = ProcId::new(Uid::Instance(0xabc123, None), None)
            .to_string()
            .parse::<ProcAddr>()
            .unwrap_err();
        assert!(matches!(err, AddrParseError::MissingSeparator));
    }

    #[test]
    fn test_proc_ref_fromstr_invalid_location() {
        let err = "local@tcp://".parse::<ProcAddr>().unwrap_err();
        assert!(matches!(err, AddrParseError::InvalidLocation(_)));
    }

    #[test]
    fn test_actor_ref_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorAddr::new(aid, loc);
        assert_eq!(aref.to_string(), format!("{}@inproc://42", aref.id()));
    }

    #[test]
    fn test_actor_and_port_proc_id_accessors() {
        let proc_id = ProcId::new(
            Uid::Instance(0xdef456, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let actor_id = ActorId::new(
            Uid::Instance(0xabc123, None),
            proc_id.clone(),
            Some(Label::new("my-actor").unwrap()),
        );
        let actor_addr = ActorAddr::new(actor_id, ChannelAddr::Local(42).into());
        let port_addr = actor_addr.port_addr(Port::from(7));

        assert_eq!(actor_addr.proc_id(), &proc_id);
        assert_eq!(port_addr.actor_id(), actor_addr.id());
        assert_eq!(port_addr.proc_id(), &proc_id);
    }

    #[test]
    fn test_actor_ref_debug_all_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorAddr::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<'my-actor.my-proc' {}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_debug_no_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            None,
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorAddr::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<{}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_debug_actor_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorAddr::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<'my-actor' {}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_debug_proc_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            None,
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorAddr::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<'.my-proc' {}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorAddr::new(aid, loc);
        let s = aref.to_string();
        let parsed: ActorAddr = s.parse().unwrap();
        assert_eq!(aref, parsed);
        assert_eq!(parsed.id.label().map(|l| l.as_str()), Some("my-actor"));
        assert_eq!(
            parsed.id.proc_id().label().map(|l| l.as_str()),
            Some("my-proc")
        );
    }

    #[test]
    fn test_actor_ref_fromstr_examples() {
        let expected_actor_uid = Uid::Instance(0xabc123, None);
        let parsed: ActorAddr = format!("controller{}.local@inproc://0", expected_actor_uid)
            .parse()
            .unwrap();
        assert_eq!(parsed.id().uid(), &expected_actor_uid);
        assert_eq!(
            parsed.id().label().map(|label| label.as_str()),
            Some("controller")
        );
        assert_eq!(
            parsed.id().proc_id().uid(),
            &Uid::singleton(Label::new("local").unwrap())
        );
        assert_eq!(*parsed.location().addr(), ChannelAddr::Local(0));
    }

    #[test]
    fn test_actor_ref_fromstr_missing_separator() {
        let err = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            None,
        )
        .to_string()
        .parse::<ActorAddr>()
        .unwrap_err();
        assert!(matches!(err, AddrParseError::MissingSeparator));
    }

    #[test]
    fn test_actor_ref_fromstr_invalid_location() {
        let err = "local.local@tcp://".parse::<ActorAddr>().unwrap_err();
        assert!(matches!(err, AddrParseError::InvalidLocation(_)));
    }

    #[test]
    fn test_proc_ref_eq_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let pid = ProcId::new(Uid::Instance(0x42, None), Some(Label::new("proc").unwrap()));
        let loc: Location = ChannelAddr::Local(1).into();
        let a = ProcAddr::new(pid.clone(), loc.clone());
        let b = ProcAddr::new(pid, loc);
        assert_eq!(a, b);

        let hash = |r: &ProcAddr| {
            let mut h = DefaultHasher::new();
            r.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_proc_ref_neq_different_location() {
        let pid = ProcId::new(Uid::Instance(0x42, None), Some(Label::new("proc").unwrap()));
        let a = ProcAddr::new(pid.clone(), ChannelAddr::Local(1).into());
        let b = ProcAddr::new(pid, ChannelAddr::Local(2).into());
        assert_ne!(a, b);
    }

    #[test]
    fn test_actor_ref_eq_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let aid = ActorId::new(
            Uid::Instance(0x42, None),
            ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(1).into();
        let a = ActorAddr::new(aid.clone(), loc.clone());
        let b = ActorAddr::new(aid, loc);
        assert_eq!(a, b);

        let hash = |r: &ActorAddr| {
            let mut h = DefaultHasher::new();
            r.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_proc_ref_singleton() {
        let pid = ProcId::new(
            Uid::singleton(Label::new("my-proc").unwrap()),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(0).into();
        let pref = ProcAddr::new(pid, loc);
        let s = pref.to_string();
        assert_eq!(s, "my-proc@inproc://0");
        let parsed: ProcAddr = s.parse().unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_reference_prefix_relationships() {
        let proc_ref = ProcAddr::singleton(ChannelAddr::Local(42), "service");
        let actor_ref = proc_ref.actor_addr("host_agent");
        let port_ref = actor_ref.port_addr(Port::from(7u64));

        assert!(Addr::Proc(proc_ref.clone()).is_prefix_of(&Addr::Actor(actor_ref.clone())));
        assert!(Addr::Proc(proc_ref.clone()).is_prefix_of(&Addr::Port(port_ref.clone())));
        assert!(Addr::Actor(actor_ref.clone()).is_prefix_of(&Addr::Port(port_ref)));
    }

    #[test]
    fn test_location_serde_roundtrip() {
        let loc: Location = ChannelAddr::Local(42).into();
        let json = serde_json::to_string(&loc).unwrap();
        let parsed: Location = serde_json::from_str(&json).unwrap();
        assert_eq!(loc, parsed);
    }

    #[test]
    fn test_proc_ref_serde_roundtrip() {
        let pid = ProcId::new(
            Uid::Instance(0xabcdef, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcAddr::new(pid, loc);
        let json = serde_json::to_string(&pref).unwrap();
        let parsed: ProcAddr = serde_json::from_str(&json).unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_actor_ref_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef, None),
            ProcId::new(
                Uid::Instance(0x123456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorAddr::new(aid, loc);
        let json = serde_json::to_string(&aref).unwrap();
        let parsed: ActorAddr = serde_json::from_str(&json).unwrap();
        assert_eq!(aref, parsed);
    }

    #[test]
    fn test_proc_ref_with_metatls_location() {
        use crate::channel::TlsAddr;

        let pid = ProcId::new(Uid::Instance(0x42, None), None);
        let loc: Location = ChannelAddr::MetaTls(TlsAddr::new("example.com", 443)).into();
        let pref = ProcAddr::new(pid, loc);
        let s = pref.to_string();
        assert_eq!(s, format!("{}@metatls://example.com:443", pref.id()));
        let parsed: ProcAddr = s.parse().unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_port_ref_construction_and_accessors() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid.clone(), Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id.clone(), loc.clone());
        assert_eq!(pref.id(), &port_id);
        assert_eq!(pref.location(), &loc);
        assert_eq!(pref.actor_id(), &aid);
    }

    #[test]
    fn test_port_ref_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id, loc);
        assert_eq!(pref.to_string(), format!("{}@inproc://7", pref.id()));
    }

    #[test]
    fn test_port_ref_debug_all_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'my-actor.my-proc' {}@inproc://7>", pref.id())
        );
    }

    #[test]
    fn test_port_ref_debug_no_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            None,
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id, loc);
        assert_eq!(format!("{:?}", pref), format!("<{}@inproc://7>", pref.id()));
    }

    #[test]
    fn test_port_ref_debug_actor_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'my-actor' {}@inproc://7>", pref.id())
        );
    }

    #[test]
    fn test_port_ref_debug_proc_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            None,
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'.my-proc' {}@inproc://7>", pref.id())
        );
    }

    #[test]
    fn test_port_ref_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id, loc);
        let s = pref.to_string();
        let parsed: PortAddr = s.parse().unwrap();
        assert_eq!(pref, parsed);
        assert_eq!(
            parsed.id.actor_id().label().map(|l| l.as_str()),
            Some("my-actor")
        );
        assert_eq!(
            parsed.id.actor_id().proc_id().label().map(|l| l.as_str()),
            Some("my-proc")
        );
    }

    #[test]
    fn test_port_ref_fromstr_examples() {
        let expected_actor_uid = Uid::Instance(0xabc123, None);
        let expected_proc_uid = Uid::Instance(0xdef456, None);
        let parsed: PortAddr = format!(
            "{}.{}:42@tcp://[::1]:2345",
            expected_actor_uid, expected_proc_uid
        )
        .parse()
        .unwrap();
        assert_eq!(parsed.id().actor_id().uid(), &expected_actor_uid);
        assert_eq!(parsed.id().proc_id().uid(), &expected_proc_uid);
        assert_eq!(parsed.id().port(), Port::from(42));
        assert_eq!(
            *parsed.location().addr(),
            "tcp:[::1]:2345".parse::<ChannelAddr>().unwrap()
        );
    }

    #[test]
    fn test_port_ref_fromstr_missing_separator() {
        let err = PortId::new(
            ActorId::new(
                Uid::Instance(0xabc123, None),
                ProcId::new(Uid::Instance(0xdef456, None), None),
                None,
            ),
            Port::from(42),
        )
        .to_string()
        .parse::<PortAddr>()
        .unwrap_err();
        assert!(matches!(err, AddrParseError::MissingSeparator));
    }

    #[test]
    fn test_port_ref_fromstr_invalid_location() {
        let err = "local.local:7@tcp://".parse::<PortAddr>().unwrap_err();
        assert!(matches!(err, AddrParseError::InvalidLocation(_)));
    }

    #[test]
    fn test_reference_fromstr_specificity() {
        let parsed: Addr = "local@inproc://0".parse().unwrap();
        assert!(parsed.is_proc());

        let parsed: Addr = "local.local@inproc://0".parse().unwrap();
        assert!(parsed.is_actor());

        let parsed: Addr = "local.local:7@inproc://0".parse().unwrap();
        assert!(parsed.is_port());
    }

    #[test]
    fn test_reference_fromstr_rejects_malformed_specific_forms() {
        assert!("local.local:not-a-port@inproc://0".parse::<Addr>().is_err());
        assert!("local.<bad!>@inproc://0".parse::<Addr>().is_err());
        assert!("local@tcp://".parse::<Addr>().is_err());
    }

    #[test]
    fn test_reference_fromstr_does_not_downcast_malformed_port_ref() {
        let err = "local.local:not-a-port@inproc://0"
            .parse::<Addr>()
            .unwrap_err();
        assert!(matches!(
            err,
            AddrParseError::InvalidId(IdParseError::InvalidPort(_))
        ));
    }

    #[test]
    fn test_reference_fromstr_does_not_downcast_malformed_actor_ref() {
        let err = "local.<bad!>@inproc://0".parse::<Addr>().unwrap_err();
        assert!(matches!(
            err,
            AddrParseError::InvalidId(IdParseError::InvalidActorProcUid(_))
        ));
    }

    #[test]
    fn test_port_ref_eq_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let aid = ActorId::new(
            Uid::Instance(0x42, None),
            ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(10));
        let loc: Location = ChannelAddr::Local(1).into();
        let a = PortAddr::new(port_id.clone(), loc.clone());
        let b = PortAddr::new(port_id, loc);
        assert_eq!(a, b);

        let hash = |r: &PortAddr| {
            let mut h = DefaultHasher::new();
            r.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_port_ref_neq_different_location() {
        let aid = ActorId::new(
            Uid::Instance(0x42, None),
            ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(10));
        let a = PortAddr::new(port_id.clone(), ChannelAddr::Local(1).into());
        let b = PortAddr::new(port_id, ChannelAddr::Local(2).into());
        assert_ne!(a, b);
    }

    #[test]
    fn test_port_ref_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef, None),
            ProcId::new(
                Uid::Instance(0x123456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortAddr::new(port_id, loc);
        let json = serde_json::to_string(&pref).unwrap();
        let parsed: PortAddr = serde_json::from_str(&json).unwrap();
        assert_eq!(pref, parsed);
    }
}
