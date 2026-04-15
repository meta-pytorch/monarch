/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Universal identifier types for the actor system.
//!
//! [`Label`] is an RFC 1035 label: up to 63 lowercase alphanumeric characters
//! plus hyphens, starting with a letter and ending with an alphanumeric.
//!
//! [`Uid`] is either a singleton (identified by label) or an instance
//! (identified by a random `u64`).

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
use smol_str::SmolStr;

use crate::port::Port;

/// Maximum length of an RFC 1035 label.
const MAX_LABEL_LEN: usize = 63;

/// An RFC 1035 label: 1–63 chars, lowercase ASCII alphanumeric plus `-`,
/// starting with a letter, ending with an alphanumeric character.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Label(SmolStr);

/// Errors that can occur when constructing a [`Label`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LabelError {
    /// The input string is empty.
    #[error("label must not be empty")]
    Empty,
    /// The input exceeds 63 characters.
    #[error("label exceeds 63 characters")]
    TooLong,
    /// The first character is not an ASCII lowercase letter.
    #[error("label must start with a lowercase letter")]
    InvalidStart,
    /// The last character is not alphanumeric.
    #[error("label must end with a lowercase letter or digit")]
    InvalidEnd,
    /// The input contains a character that is not lowercase alphanumeric or `-`.
    #[error("label contains invalid character '{0}'")]
    InvalidChar(char),
}

impl Label {
    /// Validate and construct a new [`Label`].
    pub fn new(s: &str) -> Result<Self, LabelError> {
        if s.is_empty() {
            return Err(LabelError::Empty);
        }
        if s.len() > MAX_LABEL_LEN {
            return Err(LabelError::TooLong);
        }
        let first = s.as_bytes()[0];
        if !first.is_ascii_lowercase() {
            return Err(LabelError::InvalidStart);
        }
        let last = s.as_bytes()[s.len() - 1];
        if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
            return Err(LabelError::InvalidEnd);
        }
        for ch in s.chars() {
            if !ch.is_ascii_lowercase() && !ch.is_ascii_digit() && ch != '-' && ch != '_' {
                return Err(LabelError::InvalidChar(ch));
            }
        }
        Ok(Self(SmolStr::new(s)))
    }

    /// Sanitize arbitrary input into a valid [`Label`].
    ///
    /// Lowercases, strips illegal characters, strips leading non-alpha and
    /// trailing non-alphanumeric characters, and truncates to 63 chars.
    /// Returns `"nil"` if the result would be empty.
    pub fn strip(s: &str) -> Self {
        let lowered: String = s
            .chars()
            .filter_map(|ch| {
                let ch = ch.to_ascii_lowercase();
                if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_' {
                    Some(ch)
                } else {
                    None
                }
            })
            .collect();

        // Strip leading non-alpha characters.
        let trimmed = lowered.trim_start_matches(|c: char| !c.is_ascii_lowercase());
        // Strip trailing non-alphanumeric characters.
        let trimmed =
            trimmed.trim_end_matches(|c: char| !c.is_ascii_lowercase() && !c.is_ascii_digit());

        if trimmed.is_empty() {
            return Self(SmolStr::new("nil"));
        }

        let truncated = if trimmed.len() > MAX_LABEL_LEN {
            // Re-trim trailing after truncation.
            let t = &trimmed[..MAX_LABEL_LEN];
            t.trim_end_matches(|c: char| !c.is_ascii_lowercase() && !c.is_ascii_digit())
        } else {
            trimmed
        };

        if truncated.is_empty() {
            Self(SmolStr::new("nil"))
        } else {
            Self(SmolStr::new(truncated))
        }
    }

    /// Returns the label as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Label({:?})", self.0.as_str())
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Label {
    type Err = LabelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Serialize for Label {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Label {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Label::new(&s).map_err(serde::de::Error::custom)
    }
}

/// A unique identifier: either a labeled singleton or a random instance.
#[derive(Clone)]
pub enum Uid {
    /// A singleton identified by label.
    Singleton(Label),
    /// An instance identified by a random u64.
    Instance(u64),
}

/// Errors that can occur when parsing a [`Uid`] from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UidParseError {
    /// Error parsing the label component.
    #[error("invalid label: {0}")]
    InvalidLabel(#[from] LabelError),
    /// The hex uid portion is invalid.
    #[error("invalid hex uid: {0}")]
    InvalidHex(String),
}

impl Uid {
    /// Create a fresh instance with a random uid.
    pub fn instance() -> Self {
        Uid::Instance(rand::random())
    }

    /// Create a singleton with the given label.
    pub fn singleton(label: Label) -> Self {
        Uid::Singleton(label)
    }
}

impl PartialEq for Uid {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Uid::Singleton(a), Uid::Singleton(b)) => a == b,
            (Uid::Instance(a), Uid::Instance(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Uid {}

impl Hash for Uid {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Uid::Singleton(label) => label.hash(state),
            Uid::Instance(uid) => uid.hash(state),
        }
    }
}

impl PartialOrd for Uid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Uid {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Uid::Singleton(a), Uid::Singleton(b)) => a.cmp(b),
            (Uid::Singleton(_), Uid::Instance(_)) => Ordering::Less,
            (Uid::Instance(_), Uid::Singleton(_)) => Ordering::Greater,
            (Uid::Instance(a), Uid::Instance(b)) => a.cmp(b),
        }
    }
}

/// Displays as `_label` (singleton) or `hex16` (instance), where hex16 is
/// a zero-padded 16-character lowercase hex string.
impl fmt::Debug for Uid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Uid::Singleton(label) => write!(f, "Uid(_{})", label),
            Uid::Instance(uid) => write!(f, "Uid({:016x})", uid),
        }
    }
}

impl fmt::Display for Uid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Uid::Singleton(label) => write!(f, "_{label}"),
            Uid::Instance(uid) => write!(f, "{uid:016x}"),
        }
    }
}

/// Parses `_label` as singleton, bare hex as instance.
impl FromStr for Uid {
    type Err = UidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(rest) = s.strip_prefix('_') {
            let label = Label::new(rest)?;
            return Ok(Uid::Singleton(label));
        }
        let uid = parse_hex_uid(s)?;
        Ok(Uid::Instance(uid))
    }
}

fn parse_hex_uid(s: &str) -> Result<u64, UidParseError> {
    if s.is_empty() || s.len() > 16 {
        return Err(UidParseError::InvalidHex(s.to_string()));
    }
    for ch in s.chars() {
        if !ch.is_ascii_hexdigit() {
            return Err(UidParseError::InvalidHex(s.to_string()));
        }
    }
    u64::from_str_radix(s, 16).map_err(|_| UidParseError::InvalidHex(s.to_string()))
}

impl Serialize for Uid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Uid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Uid::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur when parsing a [`ProcId`] or [`ActorId`] from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdParseError {
    /// Error parsing a [`ProcId`].
    #[error("invalid proc id: {0}")]
    InvalidProcId(#[from] UidParseError),
    /// Error parsing an [`ActorId`] (missing `.` separator).
    #[error("invalid actor id: expected format `<actor_uid>.<proc_uid>`")]
    InvalidActorIdFormat,
    /// Error parsing the actor uid component of an [`ActorId`].
    #[error("invalid actor uid: {0}")]
    InvalidActorUid(UidParseError),
    /// Error parsing the proc uid component of an [`ActorId`].
    #[error("invalid proc uid in actor id: {0}")]
    InvalidActorProcUid(UidParseError),
    /// The `<actor_id>:<port>` separator is missing.
    #[error("invalid port id: expected format `<actor_id>:<port>`")]
    InvalidPortIdFormat,
    /// The port component is invalid.
    #[error("invalid port: {0}")]
    InvalidPort(String),
}

/// Identifies a process in the actor system.
///
/// Identity (Eq, Hash, Ord) is determined solely by `uid`; `label` is
/// informational and excluded from comparisons.
#[derive(Clone, Serialize, Deserialize)]
pub struct ProcId {
    uid: Uid,
    label: Option<Label>,
}

impl ProcId {
    /// Create a new [`ProcId`].
    pub fn new(uid: Uid, label: Option<Label>) -> Self {
        Self { uid, label }
    }

    /// Returns the uid.
    pub fn uid(&self) -> &Uid {
        &self.uid
    }

    /// Returns the label.
    pub fn label(&self) -> Option<&Label> {
        self.label.as_ref()
    }
}

impl PartialEq for ProcId {
    fn eq(&self, other: &Self) -> bool {
        self.uid == other.uid
    }
}

impl Eq for ProcId {}

impl Hash for ProcId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uid.hash(state);
    }
}

impl PartialOrd for ProcId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProcId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.uid.cmp(&other.uid)
    }
}

impl fmt::Display for ProcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.uid)
    }
}

impl fmt::Debug for ProcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.label {
            Some(label) => write!(f, "<'{}' {}>", label, self.uid),
            None => write!(f, "<{}>", self.uid),
        }
    }
}

impl FromStr for ProcId {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uid: Uid = s.parse()?;
        Ok(Self { uid, label: None })
    }
}

/// Identifies an actor within a process.
///
/// Identity (Eq, Hash, Ord) is determined by `(proc_id, uid)`; `label` is
/// informational and excluded from comparisons.
#[derive(Clone, Serialize, Deserialize)]
pub struct ActorId {
    uid: Uid,
    proc_id: ProcId,
    label: Option<Label>,
}

impl ActorId {
    /// Create a new [`ActorId`].
    pub fn new(uid: Uid, proc_id: ProcId, label: Option<Label>) -> Self {
        Self {
            uid,
            proc_id,
            label,
        }
    }

    /// Returns the uid.
    pub fn uid(&self) -> &Uid {
        &self.uid
    }

    /// Returns the proc id.
    pub fn proc_id(&self) -> &ProcId {
        &self.proc_id
    }

    /// Returns the label.
    pub fn label(&self) -> Option<&Label> {
        self.label.as_ref()
    }
}

impl PartialEq for ActorId {
    fn eq(&self, other: &Self) -> bool {
        self.proc_id == other.proc_id && self.uid == other.uid
    }
}

impl Eq for ActorId {}

impl Hash for ActorId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.proc_id.hash(state);
        self.uid.hash(state);
    }
}

impl PartialOrd for ActorId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ActorId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.proc_id
            .cmp(&other.proc_id)
            .then_with(|| self.uid.cmp(&other.uid))
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.uid, self.proc_id.uid)
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.label, &self.proc_id.label) {
            (Some(actor_label), Some(proc_label)) => {
                write!(
                    f,
                    "<'{}.{}' {}.{}>",
                    actor_label, proc_label, self.uid, self.proc_id.uid
                )
            }
            (Some(actor_label), None) => {
                write!(f, "<'{}' {}.{}>", actor_label, self.uid, self.proc_id.uid)
            }
            (None, Some(proc_label)) => {
                write!(f, "<'.{}' {}.{}>", proc_label, self.uid, self.proc_id.uid)
            }
            (None, None) => {
                write!(f, "<{}.{}>", self.uid, self.proc_id.uid)
            }
        }
    }
}

impl FromStr for ActorId {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let dot = s.find('.').ok_or(IdParseError::InvalidActorIdFormat)?;
        let actor_part = &s[..dot];
        let proc_part = &s[dot + 1..];

        let actor_uid: Uid = actor_part.parse().map_err(IdParseError::InvalidActorUid)?;
        let proc_uid: Uid = proc_part
            .parse()
            .map_err(IdParseError::InvalidActorProcUid)?;

        Ok(Self {
            uid: actor_uid,
            proc_id: ProcId {
                uid: proc_uid,
                label: None,
            },
            label: None,
        })
    }
}

/// Identifies a port on an actor.
///
/// Identity (Eq, Hash, Ord) is determined by `(actor_id, port)`.
#[derive(Clone, Serialize, Deserialize)]
pub struct PortId {
    actor_id: ActorId,
    port: Port,
}

impl PortId {
    /// Create a new [`PortId`].
    pub fn new(actor_id: ActorId, port: Port) -> Self {
        Self { actor_id, port }
    }

    /// Returns the actor id.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }

    /// Returns the port.
    pub fn port(&self) -> Port {
        self.port
    }

    /// Returns the proc id (delegates to actor_id).
    pub fn proc_id(&self) -> &ProcId {
        self.actor_id.proc_id()
    }
}

impl PartialEq for PortId {
    fn eq(&self, other: &Self) -> bool {
        self.actor_id == other.actor_id && self.port == other.port
    }
}

impl Eq for PortId {}

impl Hash for PortId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.actor_id.hash(state);
        self.port.hash(state);
    }
}

impl PartialOrd for PortId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PortId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.actor_id
            .cmp(&other.actor_id)
            .then_with(|| self.port.cmp(&other.port))
    }
}

impl fmt::Display for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.actor_id, self.port)
    }
}

impl fmt::Debug for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.actor_id.label(), self.actor_id.proc_id().label()) {
            (Some(actor_label), Some(proc_label)) => {
                write!(
                    f,
                    "<'{}.{}' {}:{}>",
                    actor_label, proc_label, self.actor_id, self.port
                )
            }
            (Some(actor_label), None) => {
                write!(f, "<'{}' {}:{}>", actor_label, self.actor_id, self.port)
            }
            (None, Some(proc_label)) => {
                write!(f, "<'.{}' {}:{}>", proc_label, self.actor_id, self.port)
            }
            (None, None) => {
                write!(f, "<{}:{}>", self.actor_id, self.port)
            }
        }
    }
}

impl FromStr for PortId {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let colon = s.rfind(':').ok_or(IdParseError::InvalidPortIdFormat)?;
        let actor_part = &s[..colon];
        let port_part = &s[colon + 1..];

        let actor_id: ActorId = actor_part.parse()?;
        let port: Port = port_part
            .parse()
            .map_err(|_| IdParseError::InvalidPort(port_part.to_string()))?;

        Ok(Self { actor_id, port })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_valid() {
        assert!(Label::new("a").is_ok());
        assert!(Label::new("abc").is_ok());
        assert!(Label::new("my-service").is_ok());
        assert!(Label::new("a1").is_ok());
        assert!(Label::new("abc123").is_ok());
        assert!(Label::new("a-b-c").is_ok());
    }

    #[test]
    fn test_label_invalid_empty() {
        assert_eq!(Label::new(""), Err(LabelError::Empty));
    }

    #[test]
    fn test_label_invalid_too_long() {
        let long = "a".repeat(64);
        assert_eq!(Label::new(&long), Err(LabelError::TooLong));
        // Exactly 63 is fine.
        let exact = "a".repeat(63);
        assert!(Label::new(&exact).is_ok());
    }

    #[test]
    fn test_label_invalid_bad_start() {
        assert_eq!(Label::new("1abc"), Err(LabelError::InvalidStart));
        assert_eq!(Label::new("-abc"), Err(LabelError::InvalidStart));
        assert_eq!(Label::new("Abc"), Err(LabelError::InvalidStart));
    }

    #[test]
    fn test_label_invalid_bad_end() {
        assert_eq!(Label::new("abc-"), Err(LabelError::InvalidEnd));
    }

    #[test]
    fn test_label_invalid_char() {
        assert_eq!(Label::new("ab.c"), Err(LabelError::InvalidChar('.')));
        assert_eq!(Label::new("aBc"), Err(LabelError::InvalidChar('B')));
    }

    #[test]
    fn test_label_allows_underscores() {
        assert!(Label::new("ab_c").is_ok());
        assert!(Label::new("proc_agent").is_ok());
        assert!(Label::new("host_agent").is_ok());
    }

    #[test]
    fn test_label_strip() {
        assert_eq!(Label::strip("Hello-World").as_str(), "hello-world");
        assert_eq!(Label::strip("123abc").as_str(), "abc");
        assert_eq!(Label::strip("---abc---").as_str(), "abc");
        assert_eq!(Label::strip("").as_str(), "nil");
        assert_eq!(Label::strip("123").as_str(), "nil");
        assert_eq!(Label::strip("My_Service!").as_str(), "my_service");
    }

    #[test]
    fn test_label_strip_truncation() {
        let long = format!("a{}", "b".repeat(100));
        let stripped = Label::strip(&long);
        assert!(stripped.as_str().len() <= MAX_LABEL_LEN);
    }

    #[test]
    fn test_label_display_fromstr_roundtrip() {
        let label = Label::new("my-service").unwrap();
        let s = label.to_string();
        assert_eq!(s, "my-service");
        let parsed: Label = s.parse().unwrap();
        assert_eq!(label, parsed);
    }

    #[test]
    fn test_label_serde_roundtrip() {
        let label = Label::new("my-service").unwrap();
        let json = serde_json::to_string(&label).unwrap();
        assert_eq!(json, "\"my-service\"");
        let parsed: Label = serde_json::from_str(&json).unwrap();
        assert_eq!(label, parsed);
    }

    #[test]
    fn test_singleton_display_parse() {
        let uid = Uid::singleton(Label::new("my-actor").unwrap());
        let s = uid.to_string();
        assert_eq!(s, "_my-actor");
        let parsed: Uid = s.parse().unwrap();
        assert_eq!(uid, parsed);
    }

    #[test]
    fn test_instance_display_parse() {
        let uid = Uid::Instance(0xd5d54d7201103869);
        let s = uid.to_string();
        assert_eq!(s, "d5d54d7201103869");
        let parsed: Uid = s.parse().unwrap();
        assert_eq!(uid, parsed);
    }

    #[test]
    fn test_ordering_singleton_lt_instance() {
        let singleton = Uid::singleton(Label::new("zzz").unwrap());
        let instance = Uid::Instance(0);
        assert!(singleton < instance);
    }

    #[test]
    fn test_ordering_singletons() {
        let a = Uid::singleton(Label::new("aaa").unwrap());
        let b = Uid::singleton(Label::new("bbb").unwrap());
        assert!(a < b);
    }

    #[test]
    fn test_ordering_instances() {
        let a = Uid::Instance(1);
        let b = Uid::Instance(2);
        assert!(a < b);
    }

    #[test]
    fn test_uid_serde_roundtrip() {
        let uids = vec![
            Uid::singleton(Label::new("my-actor").unwrap()),
            Uid::Instance(0xabcdef0123456789),
            Uid::Instance(1),
        ];
        for uid in uids {
            let json = serde_json::to_string(&uid).unwrap();
            let parsed: Uid = serde_json::from_str(&json).unwrap();
            assert_eq!(uid, parsed);
        }
    }

    #[test]
    fn test_uid_parse_errors() {
        // Empty string is invalid hex.
        assert!("".parse::<Uid>().is_err());
        // Invalid singleton label.
        assert!("_".parse::<Uid>().is_err());
        assert!("_123bad".parse::<Uid>().is_err());
        // Invalid hex.
        assert!("xyz".parse::<Uid>().is_err());
        // Hex too long.
        assert!("00000000000000001".parse::<Uid>().is_err());
    }

    #[test]
    fn test_unique_uid_generation() {
        let a = Uid::instance();
        let b = Uid::instance();
        assert_ne!(a, b);
    }

    #[test]
    fn test_short_hex_parse() {
        let parsed: Uid = "1".parse().unwrap();
        assert_eq!(parsed, Uid::Instance(1));
    }

    #[test]
    fn test_proc_id_construction_and_accessors() {
        let uid = Uid::Instance(0xabc);
        let label = Label::new("my-proc").unwrap();
        let pid = ProcId::new(uid.clone(), Some(label.clone()));
        assert_eq!(pid.uid(), &uid);
        assert_eq!(pid.label(), Some(&label));
    }

    #[test]
    fn test_proc_id_eq_ignores_label() {
        let uid = Uid::Instance(0x42);
        let a = ProcId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = ProcId::new(uid, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn test_proc_id_hash_ignores_label() {
        use std::collections::hash_map::DefaultHasher;

        let uid = Uid::Instance(0x42);
        let a = ProcId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = ProcId::new(uid, Some(Label::new("beta").unwrap()));

        let hash = |pid: &ProcId| {
            let mut h = DefaultHasher::new();
            pid.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_proc_id_ord_ignores_label() {
        let a = ProcId::new(Uid::Instance(1), Some(Label::new("zzz").unwrap()));
        let b = ProcId::new(Uid::Instance(2), Some(Label::new("aaa").unwrap()));
        assert!(a < b);
    }

    #[test]
    fn test_proc_id_display() {
        let pid = ProcId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("my-proc").unwrap()),
        );
        assert_eq!(pid.to_string(), "d5d54d7201103869");

        let pid_singleton = ProcId::new(
            Uid::singleton(Label::new("my-proc").unwrap()),
            Some(Label::new("my-proc").unwrap()),
        );
        assert_eq!(pid_singleton.to_string(), "_my-proc");
    }

    #[test]
    fn test_proc_id_debug() {
        let pid = ProcId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("my-proc").unwrap()),
        );
        assert_eq!(format!("{:?}", pid), "<'my-proc' d5d54d7201103869>");

        let pid_no_label = ProcId::new(Uid::Instance(0xd5d54d7201103869), None);
        assert_eq!(format!("{:?}", pid_no_label), "<d5d54d7201103869>");
    }

    #[test]
    fn test_proc_id_fromstr_roundtrip() {
        let pid = ProcId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("my-proc").unwrap()),
        );
        let s = pid.to_string();
        let parsed: ProcId = s.parse().unwrap();
        assert_eq!(pid, parsed);
    }

    #[test]
    fn test_proc_id_fromstr_singleton() {
        let parsed: ProcId = "_my-proc".parse().unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::singleton(Label::new("my-proc").unwrap())
        );
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_proc_id_serde_roundtrip() {
        let pid = ProcId::new(
            Uid::Instance(0xabcdef),
            Some(Label::new("my-proc").unwrap()),
        );
        let json = serde_json::to_string(&pid).unwrap();
        let parsed: ProcId = serde_json::from_str(&json).unwrap();
        assert_eq!(pid, parsed);
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-proc"));

        let pid_none = ProcId::new(Uid::Instance(0xabcdef), None);
        let json_none = serde_json::to_string(&pid_none).unwrap();
        let parsed_none: ProcId = serde_json::from_str(&json_none).unwrap();
        assert_eq!(parsed_none.label(), None);
    }

    #[test]
    fn test_actor_id_construction_and_accessors() {
        let actor_uid = Uid::Instance(0xabc);
        let proc_id = ProcId::new(Uid::Instance(0xdef), Some(Label::new("my-proc").unwrap()));
        let label = Label::new("my-actor").unwrap();
        let aid = ActorId::new(actor_uid.clone(), proc_id.clone(), Some(label.clone()));
        assert_eq!(aid.uid(), &actor_uid);
        assert_eq!(aid.proc_id(), &proc_id);
        assert_eq!(aid.label(), Some(&label));
    }

    #[test]
    fn test_actor_id_eq_ignores_label() {
        let actor_uid = Uid::Instance(0x42);
        let proc_id = ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap()));
        let a = ActorId::new(
            actor_uid.clone(),
            proc_id.clone(),
            Some(Label::new("alpha").unwrap()),
        );
        let b = ActorId::new(actor_uid, proc_id, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn test_actor_id_neq_different_proc() {
        let actor_uid = Uid::Instance(0x42);
        let proc_a = ProcId::new(Uid::Instance(1), Some(Label::new("proc").unwrap()));
        let proc_b = ProcId::new(Uid::Instance(2), Some(Label::new("proc").unwrap()));
        let a = ActorId::new(
            actor_uid.clone(),
            proc_a,
            Some(Label::new("actor").unwrap()),
        );
        let b = ActorId::new(actor_uid, proc_b, Some(Label::new("actor").unwrap()));
        assert_ne!(a, b);
    }

    #[test]
    fn test_actor_id_hash_ignores_label() {
        use std::collections::hash_map::DefaultHasher;

        let actor_uid = Uid::Instance(0x42);
        let proc_id = ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap()));
        let a = ActorId::new(
            actor_uid.clone(),
            proc_id.clone(),
            Some(Label::new("alpha").unwrap()),
        );
        let b = ActorId::new(actor_uid, proc_id, Some(Label::new("beta").unwrap()));

        let hash = |aid: &ActorId| {
            let mut h = DefaultHasher::new();
            aid.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_actor_id_ord_proc_first() {
        let a = ActorId::new(
            Uid::Instance(0xff),
            ProcId::new(Uid::Instance(1), Some(Label::new("p").unwrap())),
            Some(Label::new("a").unwrap()),
        );
        let b = ActorId::new(
            Uid::Instance(0x01),
            ProcId::new(Uid::Instance(2), Some(Label::new("p").unwrap())),
            Some(Label::new("a").unwrap()),
        );
        assert!(a < b, "proc_id should be compared first");
    }

    #[test]
    fn test_actor_id_ord_then_uid() {
        let proc_id = ProcId::new(Uid::Instance(1), Some(Label::new("p").unwrap()));
        let a = ActorId::new(
            Uid::Instance(1),
            proc_id.clone(),
            Some(Label::new("a").unwrap()),
        );
        let b = ActorId::new(Uid::Instance(2), proc_id, Some(Label::new("a").unwrap()));
        assert!(a < b);
    }

    #[test]
    fn test_actor_id_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        assert_eq!(aid.to_string(), "0000000000abc123.0000000000def456");
    }

    #[test]
    fn test_actor_id_debug() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        assert_eq!(
            format!("{:?}", aid),
            "<'my-actor.my-proc' 0000000000abc123.0000000000def456>"
        );

        let aid_no_labels = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            None,
        );
        assert_eq!(
            format!("{:?}", aid_no_labels),
            "<0000000000abc123.0000000000def456>"
        );
    }

    #[test]
    fn test_actor_id_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let s = aid.to_string();
        let parsed: ActorId = s.parse().unwrap();
        assert_eq!(aid, parsed);
    }

    #[test]
    fn test_actor_id_fromstr_with_singletons() {
        let parsed: ActorId = "_my-actor._my-proc".parse().unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::singleton(Label::new("my-actor").unwrap())
        );
        assert_eq!(
            *parsed.proc_id().uid(),
            Uid::singleton(Label::new("my-proc").unwrap())
        );
    }

    #[test]
    fn test_actor_id_fromstr_errors() {
        assert!("no-dot-here".parse::<ActorId>().is_err());
        assert!(".".parse::<ActorId>().is_err());
        assert!("abc.".parse::<ActorId>().is_err());
        assert!(".abc".parse::<ActorId>().is_err());
    }

    #[test]
    fn test_actor_id_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef),
            ProcId::new(
                Uid::Instance(0x123456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let json = serde_json::to_string(&aid).unwrap();
        let parsed: ActorId = serde_json::from_str(&json).unwrap();
        assert_eq!(aid, parsed);
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-actor"));
        assert_eq!(
            parsed.proc_id().label().map(|l| l.as_str()),
            Some("my-proc")
        );
    }

    #[test]
    fn test_port_id_construction_and_accessors() {
        let actor_uid = Uid::Instance(0xabc);
        let proc_id = ProcId::new(Uid::Instance(0xdef), Some(Label::new("my-proc").unwrap()));
        let actor_id = ActorId::new(
            actor_uid,
            proc_id.clone(),
            Some(Label::new("my-actor").unwrap()),
        );
        let port = Port::from(42);
        let pid = PortId::new(actor_id.clone(), port);
        assert_eq!(pid.actor_id(), &actor_id);
        assert_eq!(pid.port(), port);
        assert_eq!(pid.proc_id(), &proc_id);
    }

    #[test]
    fn test_port_id_eq() {
        let actor_id = ActorId::new(
            Uid::Instance(0x42),
            ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let a = PortId::new(actor_id.clone(), Port::from(10));
        let b = PortId::new(actor_id, Port::from(10));
        assert_eq!(a, b);
    }

    #[test]
    fn test_port_id_neq_different_port() {
        let actor_id = ActorId::new(
            Uid::Instance(0x42),
            ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let a = PortId::new(actor_id.clone(), Port::from(10));
        let b = PortId::new(actor_id, Port::from(20));
        assert_ne!(a, b);
    }

    #[test]
    fn test_port_id_hash() {
        use std::collections::hash_map::DefaultHasher;

        let actor_id = ActorId::new(
            Uid::Instance(0x42),
            ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let a = PortId::new(actor_id.clone(), Port::from(10));
        let b = PortId::new(actor_id, Port::from(10));
        let hash = |pid: &PortId| {
            let mut h = DefaultHasher::new();
            pid.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_port_id_ord() {
        let actor_id = ActorId::new(
            Uid::Instance(0x42),
            ProcId::new(Uid::Instance(0x99), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let a = PortId::new(actor_id.clone(), Port::from(1));
        let b = PortId::new(actor_id, Port::from(2));
        assert!(a < b);
    }

    #[test]
    fn test_port_id_ord_actor_first() {
        let a = PortId::new(
            ActorId::new(
                Uid::Instance(0x01),
                ProcId::new(Uid::Instance(1), Some(Label::new("p").unwrap())),
                Some(Label::new("a").unwrap()),
            ),
            Port::from(99),
        );
        let b = PortId::new(
            ActorId::new(
                Uid::Instance(0x02),
                ProcId::new(Uid::Instance(1), Some(Label::new("p").unwrap())),
                Some(Label::new("a").unwrap()),
            ),
            Port::from(1),
        );
        assert!(a < b, "actor_id should be compared first");
    }

    #[test]
    fn test_port_id_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(pid.to_string(), "0000000000abc123.0000000000def456:42");
    }

    #[test]
    fn test_port_id_debug_all_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            "<'my-actor.my-proc' 0000000000abc123.0000000000def456:42>"
        );
    }

    #[test]
    fn test_port_id_debug_no_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            None,
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            "<0000000000abc123.0000000000def456:42>"
        );
    }

    #[test]
    fn test_port_id_debug_actor_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(Uid::Instance(0xdef456), None),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            "<'my-actor' 0000000000abc123.0000000000def456:42>"
        );
    }

    #[test]
    fn test_port_id_debug_proc_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            None,
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            "<'.my-proc' 0000000000abc123.0000000000def456:42>"
        );
    }

    #[test]
    fn test_port_id_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123),
            ProcId::new(
                Uid::Instance(0xdef456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        let s = pid.to_string();
        let parsed: PortId = s.parse().unwrap();
        assert_eq!(pid, parsed);
    }

    #[test]
    fn test_port_id_fromstr_errors() {
        // Missing colon.
        assert!(
            "0000000000abc123.0000000000def456"
                .parse::<PortId>()
                .is_err()
        );
        // Invalid port.
        assert!(
            "0000000000abc123.0000000000def456:notanumber"
                .parse::<PortId>()
                .is_err()
        );
    }

    #[test]
    fn test_port_id_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef),
            ProcId::new(
                Uid::Instance(0x123456),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        let json = serde_json::to_string(&pid).unwrap();
        let parsed: PortId = serde_json::from_str(&json).unwrap();
        assert_eq!(pid, parsed);
    }
}
