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

use serde::Deserialize;
use serde::Serialize;

use crate::channel::ChannelAddr;
use crate::id;
use crate::id::ActorId;
use crate::id::IdParseError;
use crate::id::Label;
use crate::id::PortId;
use crate::id::ProcId;
use crate::id::Uid;
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

/// Errors that can occur when parsing a [`ProcRef`] or [`ActorRef`].
#[derive(Debug, thiserror::Error)]
pub enum RefParseError {
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
pub struct ProcRef {
    id: ProcId,
    location: Location,
}

impl ProcRef {
    /// Create a new [`ProcRef`].
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
        self.id.label().or_else(|| match self.id.uid() {
            Uid::Singleton(label) => Some(label),
            _ => None,
        })
    }

    /// Create a ProcRef with a unique (random) uid and the given label.
    pub fn unique(addr: ChannelAddr, base_name: impl AsRef<str>) -> Self {
        let label = Label::strip(base_name.as_ref());
        Self::new(id::ProcId::instance(label), Location::from(addr))
    }

    /// Create a ProcRef by parsing a name string in ResourceId format.
    ///
    /// Recognizes: `label` (singleton), `label<uid58>` (labeled instance),
    /// `<uid58>` (unlabeled instance). Falls back to singleton from stripped name.
    pub fn from_resource_name(addr: ChannelAddr, name: impl AsRef<str>) -> Self {
        let (uid, label) = parse_resource_name(name.as_ref());
        Self::new(id::ProcId::new(uid, label), Location::from(addr))
    }

    /// Create an ActorRef with the provided name within this proc.
    pub fn actor_ref(&self, name: impl AsRef<str>) -> ActorRef {
        ActorRef::new_from_name(self.clone(), name)
    }

    /// A human-readable name for logging.
    pub fn log_name(&self) -> &str {
        self.label().map(|l| l.as_str()).unwrap_or("?")
    }

    /// The ResourceId text form: `label` (singleton), `label<uid58>`
    /// (labeled instance), or `<uid58>` (unlabeled instance).
    pub fn resource_name(&self) -> String {
        match (self.id.uid(), self.id.label()) {
            (Uid::Singleton(label), _) => label.to_string(),
            (Uid::Instance(_), Some(label)) => format!("{label}{}", self.id.uid()),
            (Uid::Instance(_), None) => self.id.uid().to_string(),
        }
    }
}

/// Parse a name in ResourceId format into a (Uid, Option<Label>) pair.
///
/// Formats: `label` (singleton), `label<uid58>` (labeled instance),
/// `<uid58>` (unlabeled instance). Falls back to singleton from stripped name.
pub(crate) fn parse_resource_name(s: &str) -> (Uid, Option<Label>) {
    fn parse_wrapped_instance_uid(s: &str) -> Option<u64> {
        let wrapped = format!("<{s}>");
        match Uid::from_str(&wrapped) {
            Ok(Uid::Instance(uid)) => Some(uid),
            _ => None,
        }
    }

    if let Some(inner) = s
        .strip_prefix('<')
        .and_then(|inner| inner.strip_suffix('>'))
    {
        if let Ok(uid) = Uid::from_str(&format!("<{inner}>")) {
            return (uid, None);
        }
    }

    if let Some((label_part, uid_part)) = s.rsplit_once('-')
        && uid_part.len() >= 8
    {
        if let (Ok(label), Ok(uid)) = (
            Label::new(label_part),
            Uid::from_str(&format!("<{uid_part}>")),
        ) {
            return (uid, Some(label));
        }
        if let (Ok(label), Some(uid)) =
            (Label::new(label_part), parse_wrapped_instance_uid(uid_part))
        {
            return (Uid::Instance(uid), Some(label));
        }
    }

    if let Some(open) = s.find('<') {
        if s.ends_with('>') {
            let label_part = &s[..open];
            let uid_part = &s[open..];
            if let (Ok(uid), Ok(label)) = (Uid::from_str(uid_part), Label::new(label_part)) {
                return (uid, Some(label));
            }
        }
    }

    if let Ok(label) = Label::new(s) {
        return (Uid::Singleton(label.clone()), Some(label));
    }

    let label = Label::strip(s);
    (Uid::Singleton(label.clone()), Some(label))
}

impl PartialEq for ProcRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.location == other.location
    }
}

impl Eq for ProcRef {}

impl std::hash::Hash for ProcRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.location.hash(state);
    }
}

impl PartialOrd for ProcRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProcRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.location.cmp(&other.location))
    }
}

impl fmt::Display for ProcRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

impl fmt::Debug for ProcRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.id.label() {
            Some(label) => write!(f, "<'{}' {}@{}>", label, self.id, self.location),
            None => write!(f, "<{}@{}>", self.id, self.location),
        }
    }
}

impl FromStr for ProcRef {
    type Err = RefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let at = s.find('@').ok_or(RefParseError::MissingSeparator)?;
        let id: ProcId = s[..at].parse()?;
        let location: Location = s[at + 1..]
            .parse()
            .map_err(RefParseError::InvalidLocation)?;
        Ok(Self { id, location })
    }
}

/// An actor identifier paired with a network location.
#[derive(Clone, Serialize, Deserialize, typeuri::Named)]
pub struct ActorRef {
    id: ActorId,
    location: Location,
}

hyperactor_config::impl_attrvalue!(ActorRef);

impl PartialEq<crate::reference::ActorId> for ActorRef {
    fn eq(&self, other: &crate::reference::ActorId) -> bool {
        self == other.actor_ref()
    }
}

impl PartialEq<ActorRef> for crate::reference::ActorId {
    fn eq(&self, other: &ActorRef) -> bool {
        self.actor_ref() == other
    }
}

impl ActorRef {
    /// Create a new [`ActorRef`].
    pub fn new(id: ActorId, location: Location) -> Self {
        Self { id, location }
    }

    /// Create an ActorRef from a ProcRef and name string (parsed in ResourceId format).
    pub fn new_from_name(proc_ref: ProcRef, name: impl AsRef<str>) -> Self {
        let (uid, label) = parse_resource_name(name.as_ref());
        let actor_id = id::ActorId::new(uid, proc_ref.id.clone(), label);
        Self::new(actor_id, proc_ref.location)
    }

    /// Returns the actor id.
    pub fn id(&self) -> &ActorId {
        &self.id
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
        self.id.label().or_else(|| match self.id.uid() {
            Uid::Singleton(label) => Some(label),
            _ => None,
        })
    }

    /// Reconstruct the parent ProcRef (with location preserved).
    pub fn proc_ref(&self) -> ProcRef {
        ProcRef::new(self.id.proc_id().clone(), self.location.clone())
    }

    /// Create a PortRef for a port on this actor.
    pub fn port_ref(&self, port: Port) -> PortRef {
        PortRef::new(
            id::PortId::new(self.id.clone(), port),
            self.location.clone(),
        )
    }

    /// Create an ActorRef for a root actor on a proc.
    pub fn root(proc_ref: ProcRef, label: impl Into<Label>) -> Self {
        let label = label.into();
        let actor_id = id::ActorId::singleton(label, proc_ref.id.clone());
        Self::new(actor_id, proc_ref.location)
    }

    /// Create an ActorRef for a child actor with a random uid.
    pub fn unique_child(&self) -> Self {
        let child_id = id::ActorId::instance(self.id.proc_id().clone());
        Self::new(child_id, self.location.clone())
    }

    /// Whether this is a root actor (singleton uid).
    pub fn is_root(&self) -> bool {
        matches!(self.id.uid(), Uid::Singleton(_))
    }

    /// A human-readable name for logging.
    pub fn log_name(&self) -> &str {
        self.label().map(|l| l.as_str()).unwrap_or("?")
    }
}

impl PartialEq for ActorRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.location == other.location
    }
}

impl Eq for ActorRef {}

impl std::hash::Hash for ActorRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.location.hash(state);
    }
}

impl PartialOrd for ActorRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ActorRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.location.cmp(&other.location))
    }
}

impl fmt::Display for ActorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

impl fmt::Debug for ActorRef {
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

impl FromStr for ActorRef {
    type Err = RefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let at = s.find('@').ok_or(RefParseError::MissingSeparator)?;
        let id: ActorId = s[..at].parse()?;
        let location: Location = s[at + 1..]
            .parse()
            .map_err(RefParseError::InvalidLocation)?;
        Ok(Self { id, location })
    }
}

/// A port identifier paired with a network location.
#[derive(Clone, Serialize, Deserialize, typeuri::Named)]
pub struct PortRef {
    id: PortId,
    location: Location,
}

impl PortRef {
    /// Create a new [`PortRef`].
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

    /// Whether this is a handler (actor-level) port.
    pub(crate) fn is_actor_port(&self) -> bool {
        self.id.port().is_handler()
    }

    /// The port index.
    pub fn index(&self) -> u64 {
        self.id.port().as_u64()
    }

    /// Reconstruct the parent ActorRef (with location preserved).
    pub fn actor_ref(&self) -> ActorRef {
        ActorRef::new(self.id.actor_id().clone(), self.location.clone())
    }
}

impl PartialEq for PortRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.location == other.location
    }
}

impl Eq for PortRef {}

impl std::hash::Hash for PortRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.location.hash(state);
    }
}

impl PartialOrd for PortRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PortRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.location.cmp(&other.location))
    }
}

impl fmt::Display for PortRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

impl fmt::Debug for PortRef {
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

impl FromStr for PortRef {
    type Err = RefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let at = s.find('@').ok_or(RefParseError::MissingSeparator)?;
        let id: PortId = s[..at].parse()?;
        let location: Location = s[at + 1..]
            .parse()
            .map_err(RefParseError::InvalidLocation)?;
        Ok(Self { id, location })
    }
}

/// A polymorphic reference: proc, actor, or port.
///
/// Used for prefix-based routing in [`MailboxRouter`] and
/// [`DialMailboxRouter`]. Ordering is lexicographic by
/// (proc, actor uid, port).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Reference {
    /// A process reference.
    Proc(ProcRef),
    /// An actor reference.
    Actor(ActorRef),
    /// A port reference.
    Port(PortRef),
}

impl Reference {
    /// Whether `self` is a prefix of `other`.
    ///
    /// - Proc is a prefix of any Actor or Port on the same proc.
    /// - Actor is a prefix of any Port on the same actor.
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Proc(p), Self::Actor(a)) => *p == a.proc_ref(),
            (Self::Proc(p), Self::Port(pt)) => *p == pt.actor_ref().proc_ref(),
            (Self::Actor(a), Self::Port(pt)) => *a == pt.actor_ref(),
            (Self::Proc(p1), Self::Proc(p2)) => p1 == p2,
            (Self::Actor(a1), Self::Actor(a2)) => a1 == a2,
            (Self::Port(p1), Self::Port(p2)) => p1 == p2,
            _ => false,
        }
    }

    /// The proc ref of this reference.
    pub fn proc_ref(&self) -> ProcRef {
        match self {
            Self::Proc(p) => p.clone(),
            Self::Actor(a) => a.proc_ref(),
            Self::Port(p) => p.actor_ref().proc_ref(),
        }
    }
}

impl PartialOrd for Reference {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Reference {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by: proc, then actor uid (None < Some), then port (None < Some).
        let proc_ord = self.proc_ref().cmp(&other.proc_ref());
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

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proc(p) => fmt::Display::fmt(p, f),
            Self::Actor(a) => fmt::Display::fmt(a, f),
            Self::Port(p) => fmt::Display::fmt(p, f),
        }
    }
}

impl FromStr for Reference {
    type Err = RefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try most specific first.
        if let Ok(port_ref) = s.parse::<PortRef>() {
            return Ok(Self::Port(port_ref));
        }
        if let Ok(actor_ref) = s.parse::<ActorRef>() {
            return Ok(Self::Actor(actor_ref));
        }
        if let Ok(proc_ref) = s.parse::<ProcRef>() {
            return Ok(Self::Proc(proc_ref));
        }
        Err(RefParseError::MissingSeparator)
    }
}

impl From<ProcRef> for Reference {
    fn from(p: ProcRef) -> Self {
        Self::Proc(p)
    }
}

impl From<ActorRef> for Reference {
    fn from(a: ActorRef) -> Self {
        Self::Actor(a)
    }
}

impl From<PortRef> for Reference {
    fn from(p: PortRef) -> Self {
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
            Uid::Instance(0xabc123),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcRef::new(pid, loc);
        assert_eq!(pref.to_string(), format!("{}@inproc://42", pref.id()));
    }

    #[test]
    fn test_proc_ref_debug_with_label() {
        let pid = ProcId::new(
            Uid::Instance(0xabc123),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcRef::new(pid, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'my-proc' {}@inproc://42>", pref.id())
        );
    }

    #[test]
    fn test_proc_ref_debug_without_label() {
        let pid = ProcId::new(Uid::Instance(0xabc123), None);
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcRef::new(pid, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<{}@inproc://42>", pref.id())
        );
    }

    #[test]
    fn test_proc_ref_fromstr_roundtrip() {
        let pid = ProcId::new(
            Uid::Instance(0xabc123),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcRef::new(pid, loc);
        let s = pref.to_string();
        let parsed: ProcRef = s.parse().unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_proc_ref_fromstr_tcp() {
        let parsed: ProcRef = format!(
            "{}@tcp://127.0.0.1:8080",
            ProcId::new(Uid::Instance(0xabc123), None)
        )
        .parse()
        .unwrap();
        assert_eq!(*parsed.id().uid(), Uid::Instance(0xabc123));
        assert_eq!(
            *parsed.location().addr(),
            "tcp:127.0.0.1:8080".parse::<ChannelAddr>().unwrap()
        );
    }

    #[test]
    fn test_proc_ref_fromstr_missing_separator() {
        let err = ProcId::new(Uid::Instance(0xabc123), None)
            .to_string()
            .parse::<ProcRef>()
            .unwrap_err();
        assert!(matches!(err, RefParseError::MissingSeparator));
    }

    #[test]
    fn test_actor_ref_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorRef::new(aid, loc);
        assert_eq!(aref.to_string(), format!("{}@inproc://42", aref.id()));
    }

    #[test]
    fn test_actor_ref_debug_all_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorRef::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<'my-actor.my-proc' {}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_debug_no_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            None,
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorRef::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<{}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_debug_actor_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorRef::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<'my-actor' {}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_debug_proc_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            None,
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorRef::new(aid, loc);
        assert_eq!(
            format!("{:?}", aref),
            format!("<'.my-proc' {}@inproc://42>", aref.id())
        );
    }

    #[test]
    fn test_actor_ref_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorRef::new(aid, loc);
        let s = aref.to_string();
        let parsed: ActorRef = s.parse().unwrap();
        assert_eq!(aref, parsed);
        assert_eq!(parsed.id.label().map(|l| l.as_str()), Some("my-actor"));
        assert_eq!(
            parsed.id.proc_id().label().map(|l| l.as_str()),
            Some("my-proc")
        );
    }

    #[test]
    fn test_actor_ref_fromstr_missing_separator() {
        let err = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            None,
        )
        .to_string()
        .parse::<ActorRef>()
        .unwrap_err();
        assert!(matches!(err, RefParseError::MissingSeparator));
    }

    #[test]
    fn test_proc_ref_eq_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let pid = ProcId::new(Uid::Instance(0x42), Some(Label::new("proc").unwrap()));
        let loc: Location = ChannelAddr::Local(1).into();
        let a = ProcRef::new(pid.clone(), loc.clone());
        let b = ProcRef::new(pid, loc);
        assert_eq!(a, b);

        let hash = |r: &ProcRef| {
            let mut h = DefaultHasher::new();
            r.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_proc_ref_neq_different_location() {
        let pid = ProcId::new(Uid::Instance(0x42), Some(Label::new("proc").unwrap()));
        let a = ProcRef::new(pid.clone(), ChannelAddr::Local(1).into());
        let b = ProcRef::new(pid, ChannelAddr::Local(2).into());
        assert_ne!(a, b);
    }

    #[test]
    fn test_actor_ref_eq_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let aid = ActorId::new(
            Uid::Instance(0x42),
            ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(1).into();
        let a = ActorRef::new(aid.clone(), loc.clone());
        let b = ActorRef::new(aid, loc);
        assert_eq!(a, b);

        let hash = |r: &ActorRef| {
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
        let pref = ProcRef::new(pid, loc);
        let s = pref.to_string();
        assert_eq!(s, "my-proc@inproc://0");
        let parsed: ProcRef = s.parse().unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_parse_resource_name_formats() {
        assert_eq!(
            parse_resource_name("service"),
            (
                Uid::Singleton(Label::new("service").unwrap()),
                Some(Label::new("service").unwrap())
            )
        );
        assert_eq!(
            parse_resource_name(&format!("worker{}", Uid::Instance(0xabc123))),
            (Uid::Instance(0xabc123), Some(Label::new("worker").unwrap()))
        );
        assert_eq!(
            parse_resource_name(&Uid::Instance(0xabc123).to_string()),
            (Uid::Instance(0xabc123), None)
        );
        assert_eq!(
            parse_resource_name("dead"),
            (
                Uid::Singleton(Label::new("dead").unwrap()),
                Some(Label::new("dead").unwrap())
            )
        );
    }

    #[test]
    fn test_reference_prefix_relationships() {
        let proc_ref = ProcRef::from_resource_name(ChannelAddr::Local(42), "service");
        let actor_ref = proc_ref.actor_ref("host_agent");
        let port_ref = actor_ref.port_ref(Port::from(7u64));

        assert!(
            Reference::Proc(proc_ref.clone()).is_prefix_of(&Reference::Actor(actor_ref.clone()))
        );
        assert!(Reference::Proc(proc_ref.clone()).is_prefix_of(&Reference::Port(port_ref.clone())));
        assert!(Reference::Actor(actor_ref.clone()).is_prefix_of(&Reference::Port(port_ref)));
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
            Uid::Instance(0xabcdef),
            Some(Label::new("my-proc").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcRef::new(pid, loc);
        let json = serde_json::to_string(&pref).unwrap();
        let parsed: ProcRef = serde_json::from_str(&json).unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_actor_ref_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef),
            ProcId::new(
                Uid::Instance(0x123456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let loc: Location = ChannelAddr::Local(42).into();
        let aref = ActorRef::new(aid, loc);
        let json = serde_json::to_string(&aref).unwrap();
        let parsed: ActorRef = serde_json::from_str(&json).unwrap();
        assert_eq!(aref, parsed);
    }

    #[test]
    fn test_proc_ref_with_metatls_location() {
        use crate::channel::TlsAddr;

        let pid = ProcId::new(Uid::Instance(0x42), None);
        let loc: Location = ChannelAddr::MetaTls(TlsAddr::new("example.com", 443)).into();
        let pref = ProcRef::new(pid, loc);
        let s = pref.to_string();
        assert_eq!(s, format!("{}@metatls://example.com:443", pref.id()));
        let parsed: ProcRef = s.parse().unwrap();
        assert_eq!(pref, parsed);
    }

    #[test]
    fn test_port_ref_construction_and_accessors() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid.clone(), Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id.clone(), loc.clone());
        assert_eq!(pref.id(), &port_id);
        assert_eq!(pref.location(), &loc);
        assert_eq!(pref.actor_id(), &aid);
    }

    #[test]
    fn test_port_ref_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id, loc);
        assert_eq!(pref.to_string(), format!("{}@inproc://7", pref.id()));
    }

    #[test]
    fn test_port_ref_debug_all_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'my-actor.my-proc' {}@inproc://7>", pref.id())
        );
    }

    #[test]
    fn test_port_ref_debug_no_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            None,
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id, loc);
        assert_eq!(format!("{:?}", pref), format!("<{}@inproc://7>", pref.id()));
    }

    #[test]
    fn test_port_ref_debug_actor_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'my-actor' {}@inproc://7>", pref.id())
        );
    }

    #[test]
    fn test_port_ref_debug_proc_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            None,
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id, loc);
        assert_eq!(
            format!("{:?}", pref),
            format!("<'.my-proc' {}@inproc://7>", pref.id())
        );
    }

    #[test]
    fn test_port_ref_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id, loc);
        let s = pref.to_string();
        let parsed: PortRef = s.parse().unwrap();
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
    fn test_port_ref_fromstr_missing_separator() {
        let err = PortId::new(
            ActorId::new(
                Uid::Instance(0xabc123),
                ProcId::new(Uid::Instance(0xdef456), None),
                None,
            ),
            Port::from(42),
        )
        .to_string()
        .parse::<PortRef>()
        .unwrap_err();
        assert!(matches!(err, RefParseError::MissingSeparator));
    }

    #[test]
    fn test_port_ref_eq_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let aid = ActorId::new(
            Uid::Instance(0x42),
            ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(10));
        let loc: Location = ChannelAddr::Local(1).into();
        let a = PortRef::new(port_id.clone(), loc.clone());
        let b = PortRef::new(port_id, loc);
        assert_eq!(a, b);

        let hash = |r: &PortRef| {
            let mut h = DefaultHasher::new();
            r.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_port_ref_neq_different_location() {
        let aid = ActorId::new(
            Uid::Instance(0x42),
            ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(10));
        let a = PortRef::new(port_id.clone(), ChannelAddr::Local(1).into());
        let b = PortRef::new(port_id, ChannelAddr::Local(2).into());
        assert_ne!(a, b);
    }

    #[test]
    fn test_port_ref_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef),
            ProcId::new(
                Uid::Instance(0x123456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let port_id = PortId::new(aid, Port::from(42));
        let loc: Location = ChannelAddr::Local(7).into();
        let pref = PortRef::new(port_id, loc);
        let json = serde_json::to_string(&pref).unwrap();
        let parsed: PortRef = serde_json::from_str(&json).unwrap();
        assert_eq!(pref, parsed);
    }
}
