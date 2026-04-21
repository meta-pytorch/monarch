/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! References: identifiers paired with a network location.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::channel::ChannelAddr;
use crate::id::ActorId;
use crate::id::IdParseError;
use crate::id::PortId;
use crate::id::ProcId;

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
#[derive(Clone, Serialize, Deserialize)]
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
#[derive(Clone, Serialize, Deserialize)]
pub struct ActorRef {
    id: ActorId,
    location: Location,
}

impl ActorRef {
    /// Create a new [`ActorRef`].
    pub fn new(id: ActorId, location: Location) -> Self {
        Self { id, location }
    }

    /// Returns the actor id.
    pub fn id(&self) -> &ActorId {
        &self.id
    }

    /// Returns the location.
    pub fn location(&self) -> &Location {
        &self.location
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
#[derive(Clone, Serialize, Deserialize)]
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
        assert_eq!(pref.to_string(), "0000000000abc123@inproc://42");
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
            "<'my-proc' 0000000000abc123@inproc://42>"
        );
    }

    #[test]
    fn test_proc_ref_debug_without_label() {
        let pid = ProcId::new(Uid::Instance(0xabc123), None);
        let loc: Location = ChannelAddr::Local(42).into();
        let pref = ProcRef::new(pid, loc);
        assert_eq!(format!("{:?}", pref), "<0000000000abc123@inproc://42>");
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
        let parsed: ProcRef = "0000000000abc123@tcp://127.0.0.1:8080".parse().unwrap();
        assert_eq!(*parsed.id().uid(), Uid::Instance(0xabc123));
        assert_eq!(
            *parsed.location().addr(),
            "tcp:127.0.0.1:8080".parse::<ChannelAddr>().unwrap()
        );
    }

    #[test]
    fn test_proc_ref_fromstr_missing_separator() {
        let err = "0000000000abc123".parse::<ProcRef>().unwrap_err();
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
        assert_eq!(
            aref.to_string(),
            "0000000000abc123.0000000000def456@inproc://42"
        );
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
            "<'my-actor.my-proc' 0000000000abc123.0000000000def456@inproc://42>"
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
            "<0000000000abc123.0000000000def456@inproc://42>"
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
            "<'my-actor' 0000000000abc123.0000000000def456@inproc://42>"
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
            "<'.my-proc' 0000000000abc123.0000000000def456@inproc://42>"
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
    }

    #[test]
    fn test_actor_ref_fromstr_missing_separator() {
        let err = "0000000000abc123.0000000000def456"
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
        assert_eq!(s, "_my-proc@inproc://0");
        let parsed: ProcRef = s.parse().unwrap();
        assert_eq!(pref, parsed);
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
        assert_eq!(s, "0000000000000042@metatls://example.com:443");
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
        assert_eq!(
            pref.to_string(),
            "0000000000abc123.0000000000def456:42@inproc://7"
        );
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
            "<'my-actor.my-proc' 0000000000abc123.0000000000def456:42@inproc://7>"
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
        assert_eq!(
            format!("{:?}", pref),
            "<0000000000abc123.0000000000def456:42@inproc://7>"
        );
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
            "<'my-actor' 0000000000abc123.0000000000def456:42@inproc://7>"
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
            "<'.my-proc' 0000000000abc123.0000000000def456:42@inproc://7>"
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
    }

    #[test]
    fn test_port_ref_fromstr_missing_separator() {
        let err = "0000000000abc123.0000000000def456:42"
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
