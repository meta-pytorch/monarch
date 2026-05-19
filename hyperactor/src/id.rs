/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Universal identifier types for the actor system.
//!
//! Concrete grammar:
//!
//! ```text
//! label        := lowercase letter, then lowercase letters, digits, `-`, or `_`,
//!                 ending in a lowercase letter or digit
//! uid58        := base58(u64) using the Flickr alphabet
//! uid          := label | "<" uid58 ">"
//! proc-id      := label | "<" uid58 ">" | label "<" uid58 ">"
//! actor-id     := actor-part "." proc-id
//! actor-part   := label | "<" uid58 ">" | label "<" uid58 ">"
//! port-id      := actor-id ":" decimal-port
//! ```
//!
//! Singletons are self-documenting and therefore display as bare labels.
//! Non-singleton ids display their semantic label, if any, outside the uid:
//! `label<uid58>`. Unlabeled instances display as `<uid58>`.
//!
//! [`Label`] is an RFC 1035-style label: up to 63 lowercase alphanumeric
//! characters plus `-` or `_`, starting with a letter and ending with an
//! alphanumeric.
//!
//! [`Uid`] is either a singleton (identified by label) or an instance
//! (identified by a random `u64`, with an optional label for display).

use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;
use serde::de::EnumAccess;
use serde::de::SeqAccess;
use serde::de::VariantAccess;
use serde::de::Visitor;
use serde::ser::SerializeTupleVariant;
use smol_str::SmolStr;

use crate::addr::ActorAddr;
use crate::addr::Addr;
use crate::addr::Location;
use crate::addr::PortAddr;
use crate::addr::ProcAddr;
use crate::parse::id::encode_base58_uid;
use crate::port::Port;

/// Maximum length of an RFC 1035 label.
const MAX_LABEL_LEN: usize = 63;

/// An RFC 1035-style label: 1–63 chars, lowercase ASCII alphanumeric plus `-`
/// or `_`,
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
    /// The input contains a character that is not lowercase alphanumeric, `-`,
    /// or `_`.
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

/// A unique identifier.
///
/// Singleton labels are identity. Instance labels are supplemental metadata
/// and do not participate in equality, hashing, or ordering.
#[derive(Clone, EnumAsInner)]
pub enum Uid {
    /// A singleton identified by label.
    Singleton(Label),
    /// An instance identified by a random u64, with an optional display label.
    Instance(u64, Option<Label>),
}

/// Errors that can occur when parsing a [`Uid`] from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UidParseError {
    /// Error parsing the uid syntax.
    #[error("invalid uid syntax: {0}")]
    InvalidSyntax(String),
    /// Error parsing the label component.
    #[error("invalid label: {0}")]
    InvalidLabel(#[from] LabelError),
    /// The base58 uid portion is invalid.
    #[error("invalid base58 uid: {0}")]
    InvalidBase58(String),
}

impl Uid {
    /// Create a fresh instance with a random uid and no display label.
    pub fn anonymous() -> Self {
        Uid::Instance(rand::random(), None)
    }

    /// Create a fresh instance with a random uid and display label.
    pub fn instance(label: Label) -> Self {
        Uid::Instance(rand::random(), Some(label))
    }

    /// Create a singleton with the given label.
    pub fn singleton(label: Label) -> Self {
        Uid::Singleton(label)
    }

    /// Returns the display label for this uid, if present.
    ///
    /// For singletons, the label is the identity. For instances, the label is
    /// supplemental metadata.
    pub fn label(&self) -> Option<&Label> {
        match self {
            Uid::Singleton(label) => Some(label),
            Uid::Instance(_, label) => label.as_ref(),
        }
    }

    /// Returns the raw base58 uid for instances, without display delimiters.
    pub fn instance_uid_base58(&self) -> Option<String> {
        match self {
            Uid::Singleton(_) => None,
            Uid::Instance(uid, _) => Some(encode_base58_uid(*uid)),
        }
    }

    /// Parses a raw base58 uid for instances, without display delimiters.
    pub fn parse_instance_uid_base58(s: &str) -> Result<u64, UidParseError> {
        parse_base58_uid(s)
    }

    /// Returns this uid with the provided instance label.
    ///
    /// Singleton labels are identity and are not replaced.
    pub fn with_label(self, label: Option<Label>) -> Self {
        match self {
            Uid::Singleton(label) => Uid::Singleton(label),
            Uid::Instance(uid, existing) => Uid::Instance(uid, label.or(existing)),
        }
    }
}

impl PartialEq for Uid {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Uid::Singleton(a), Uid::Singleton(b)) => a == b,
            (Uid::Instance(a, _), Uid::Instance(b, _)) => a == b,
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
            Uid::Instance(uid, _) => uid.hash(state),
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
            (Uid::Singleton(_), Uid::Instance(_, _)) => Ordering::Less,
            (Uid::Instance(_, _), Uid::Singleton(_)) => Ordering::Greater,
            (Uid::Instance(a, _), Uid::Instance(b, _)) => a.cmp(b),
        }
    }
}

/// Displays as `label` (singleton), `label<base58>` (labeled instance), or
/// `<base58>` (unlabeled instance).
impl fmt::Debug for Uid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Uid::Singleton(label) => write!(f, "Uid({})", label),
            Uid::Instance(uid, Some(label)) => {
                write!(f, "Uid({}<{}>)", label, encode_base58_uid(*uid))
            }
            Uid::Instance(uid, None) => write!(f, "Uid(<{}>)", encode_base58_uid(*uid)),
        }
    }
}

impl fmt::Display for Uid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Uid::Singleton(label) => write!(f, "{label}"),
            Uid::Instance(uid, Some(label)) => write!(f, "{}<{}>", label, encode_base58_uid(*uid)),
            Uid::Instance(uid, None) => write!(f, "<{}>", encode_base58_uid(*uid)),
        }
    }
}

/// Parses `label` as singleton, `<base58>` as an unlabeled instance, and
/// `label<base58>` as a labeled instance.
impl FromStr for Uid {
    type Err = UidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::parse::id::parse_uid_str(s)
            .map_err(|err| UidParseError::InvalidSyntax(err.to_string()))
    }
}

fn parse_base58_uid(s: &str) -> Result<u64, UidParseError> {
    crate::parse::id::decode_base58_uid(s).map_err(|_| UidParseError::InvalidBase58(s.to_string()))
}

impl Serialize for Uid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            match self {
                Uid::Singleton(label) => {
                    serializer.serialize_newtype_variant("Uid", 0, "Singleton", label)
                }
                Uid::Instance(uid, label) => {
                    let mut variant =
                        serializer.serialize_tuple_variant("Uid", 1, "Instance", 2)?;
                    variant.serialize_field(uid)?;
                    variant.serialize_field(label)?;
                    variant.end()
                }
            }
        }
    }
}

impl<'de> Deserialize<'de> for Uid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            Uid::from_str(&s).map_err(serde::de::Error::custom)
        } else {
            deserializer.deserialize_enum("Uid", &["Singleton", "Instance"], UidVisitor)
        }
    }
}

struct UidVisitor;

impl<'de> Visitor<'de> for UidVisitor {
    type Value = Uid;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a uid enum")
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: EnumAccess<'de>,
    {
        match data.variant()? {
            (UidVariant::Singleton, variant) => variant.newtype_variant().map(Uid::Singleton),
            (UidVariant::Instance, variant) => {
                let (uid, label) = variant.tuple_variant(2, UidInstanceVisitor)?;
                Ok(Uid::Instance(uid, label))
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum UidVariant {
    Singleton,
    Instance,
}

struct UidInstanceVisitor;

impl<'de> Visitor<'de> for UidInstanceVisitor {
    type Value = (u64, Option<Label>);

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a uid instance tuple")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let uid = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
        let label = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
        Ok((uid, label))
    }
}

/// Errors that can occur when parsing a [`ProcId`] or [`ActorId`] from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdParseError {
    /// Error parsing a [`ProcId`].
    #[error("invalid proc id: {0}")]
    InvalidProcId(#[from] UidParseError),
    /// Error parsing an [`ActorId`] (missing `.` separator).
    #[error("invalid actor id: expected format `<actor>.<proc>`")]
    InvalidActorIdFormat,
    /// Error parsing the actor uid component of an [`ActorId`].
    #[error("invalid actor uid: {0}")]
    InvalidActorUid(UidParseError),
    /// Error parsing the proc uid component of an [`ActorId`].
    #[error("invalid proc uid in actor id: {0}")]
    InvalidActorProcUid(UidParseError),
    /// The `<actor_id>:<port>` separator is missing.
    #[error("invalid port id: expected format `<actor>:<port>`")]
    InvalidPortIdFormat,
    /// The port component is invalid.
    #[error("invalid port: {0}")]
    InvalidPort(String),
}

/// Identifies a process in the actor system.
///
/// Identity (Eq, Hash, Ord) is determined by `uid`.
#[derive(Clone, Serialize, Deserialize)]
pub struct ProcId {
    uid: Uid,
}

impl ProcId {
    /// Create a new [`ProcId`].
    pub fn new(uid: Uid, label: Option<Label>) -> Self {
        Self {
            uid: uid.with_label(label),
        }
    }

    /// Create an anonymous instance [`ProcId`] with a random uid.
    pub fn anonymous() -> Self {
        Self {
            uid: Uid::anonymous(),
        }
    }

    /// Create a singleton [`ProcId`] identified by the given label.
    pub fn singleton(label: Label) -> Self {
        Self {
            uid: Uid::Singleton(label),
        }
    }

    /// Create an instance [`ProcId`] with a random uid and the given label.
    pub fn instance(label: Label) -> Self {
        Self {
            uid: Uid::instance(label),
        }
    }

    /// Returns the uid.
    pub fn uid(&self) -> &Uid {
        &self.uid
    }

    /// Returns the label.
    pub fn label(&self) -> Option<&Label> {
        self.uid.label()
    }

    /// Returns a unique path for this proc in the given directory. This is stable
    /// over the lifetime of the ProcId.
    ///
    /// The basename is `proc_id.pseudo_uid()` rendered in base58, which keeps the
    /// path short and host-unique. Both ends of a local link compute the same
    /// pseudo uid, so the path is consistent without coordination. The
    /// returned [`PathBuf`] is the on-disk socket path, which callers may use to
    /// pre-flight existence before dialing.
    pub fn to_path_elem(&self, base_dir: &Path) -> PathBuf {
        let pseudo_id = self.pseudo_uid();
        let tag = match pseudo_id {
            Uid::Singleton(label) => {
                panic!("pseudo uid should never be a singleton, but got: {}", label)
            }
            Uid::Instance(uid, _) => encode_base58_uid(uid).to_string(),
        };
        base_dir.join(tag)
    }

    /// A `Uid` suitable as a short, host-unique identifier — for example,
    /// as a basename in a filesystem path.
    ///
    /// For an instance proc, this is the proc's actual uid. For a singleton,
    /// it is `Uid::Instance(hash(label))`, a stable value derived from the
    /// singleton's name. Singletons are host-unique by name, so this remains
    /// host-unique. We call it "pseudo" because in the singleton case it does
    /// not match the proc's true uid.
    pub fn pseudo_uid(&self) -> Uid {
        match &self.uid {
            Uid::Instance(_, _) => self.uid.clone(),
            Uid::Singleton(label) => {
                let mut h = DefaultHasher::new();
                label.hash(&mut h);
                Uid::Instance(h.finish(), None)
            }
        }
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
        fmt::Display::fmt(&self.uid, f)
    }
}

impl fmt::Debug for ProcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.label() {
            Some(label) => write!(f, "<'{}' {}>", label, self.uid),
            None => write!(f, "<{}>", self.uid),
        }
    }
}

impl FromStr for ProcId {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::parse::id::parse_proc_id(s).map_err(|err| {
            IdParseError::InvalidProcId(UidParseError::InvalidSyntax(err.to_string()))
        })
    }
}

/// Identifies an actor within a process.
///
/// Identity (Eq, Hash, Ord) is determined by `(proc_id, uid)`.
#[derive(Clone, Serialize, Deserialize)]
pub struct ActorId {
    uid: Uid,
    proc_id: ProcId,
}

impl ActorId {
    /// Create a new [`ActorId`].
    pub fn new(uid: Uid, proc_id: ProcId, label: Option<Label>) -> Self {
        Self {
            uid: uid.with_label(label),
            proc_id,
        }
    }

    /// Create a singleton [`ActorId`] identified by the given label.
    pub fn singleton(label: Label, proc_id: ProcId) -> Self {
        Self {
            uid: Uid::Singleton(label),
            proc_id,
        }
    }

    /// Create an anonymous instance [`ActorId`] with a random uid.
    pub fn anonymous(proc_id: ProcId) -> Self {
        Self {
            uid: Uid::anonymous(),
            proc_id,
        }
    }

    /// Create an instance [`ActorId`] with a random uid and the given label.
    pub fn instance(label: Label, proc_id: ProcId) -> Self {
        Self {
            uid: Uid::instance(label),
            proc_id,
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
        self.uid.label()
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
        fmt::Display::fmt(&self.uid, f)?;
        write!(f, ".{}", self.proc_id)
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.label(), self.proc_id.label()) {
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
        crate::parse::id::parse_actor_id(s).map_err(|_| legacy_parse_actor_id(s))
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
        crate::parse::id::parse_port_id(s).map_err(|_| legacy_port_parse_error(s))
    }
}

/// A Hyperactor id.
#[derive(
    Clone,
    EnumAsInner,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum Id {
    /// A process id.
    Proc(ProcId),
    /// An actor id.
    Actor(ActorId),
    /// A port id.
    Port(PortId),
}

impl Id {
    /// Pair this id with a network location.
    pub fn addr(self, location: Location) -> Addr {
        match self {
            Self::Proc(id) => Addr::Proc(ProcAddr::new(id, location)),
            Self::Actor(id) => Addr::Actor(ActorAddr::new(id, location)),
            Self::Port(id) => Addr::Port(PortAddr::new(id, location)),
        }
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proc(id) => fmt::Display::fmt(id, f),
            Self::Actor(id) => fmt::Display::fmt(id, f),
            Self::Port(id) => fmt::Display::fmt(id, f),
        }
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proc(id) => fmt::Debug::fmt(id, f),
            Self::Actor(id) => fmt::Debug::fmt(id, f),
            Self::Port(id) => fmt::Debug::fmt(id, f),
        }
    }
}

impl FromStr for Id {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::parse::id::parse_id(s).map_err(|_| legacy_parse_id(s))
    }
}

fn legacy_parse_id_component(s: &str) -> Result<(Uid, Option<Label>), UidParseError> {
    if let Some(inner) = s
        .strip_prefix('<')
        .and_then(|inner| inner.strip_suffix('>'))
    {
        let uid = parse_base58_uid(inner)?;
        return Ok((Uid::Instance(uid, None), None));
    }

    if let Some(open) = s.find('<')
        && s.ends_with('>')
    {
        let label = Label::new(&s[..open])?;
        let uid = parse_base58_uid(&s[open + 1..s.len() - 1])?;
        return Ok((Uid::Instance(uid, Some(label.clone())), Some(label)));
    }

    let label = Label::new(s)?;
    Ok((Uid::Singleton(label.clone()), Some(label)))
}

fn legacy_parse_id(s: &str) -> IdParseError {
    if s.contains(':') {
        legacy_port_parse_error(s)
    } else if s.contains('.') {
        legacy_parse_actor_id(s)
    } else {
        legacy_parse_id_component(s)
            .err()
            .map(IdParseError::InvalidProcId)
            .unwrap_or(IdParseError::InvalidActorIdFormat)
    }
}

fn legacy_parse_actor_id(s: &str) -> IdParseError {
    let Some((actor_part, proc_part)) = s.split_once('.') else {
        return IdParseError::InvalidActorIdFormat;
    };

    if let Err(err) = legacy_parse_id_component(actor_part) {
        return IdParseError::InvalidActorUid(err);
    }

    if let Err(err) = legacy_parse_id_component(proc_part) {
        return IdParseError::InvalidActorProcUid(err);
    }

    IdParseError::InvalidActorIdFormat
}

fn legacy_port_parse_error(s: &str) -> IdParseError {
    let Some((actor_part, port_part)) = s.split_once(':') else {
        return IdParseError::InvalidPortIdFormat;
    };

    if crate::parse::id::parse_actor_id(actor_part).is_ok() {
        return IdParseError::InvalidPort(port_part.to_string());
    }

    IdParseError::InvalidPortIdFormat
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
        assert_eq!(s, "my-actor");
        let parsed: Uid = s.parse().unwrap();
        assert_eq!(uid, parsed);
    }

    #[test]
    fn test_instance_display_parse() {
        let uid = Uid::Instance(0xd5d54d7201103869, None);
        let s = uid.to_string();
        assert_eq!(s, format!("<{}>", encode_base58_uid(0xd5d54d7201103869)));
        assert_eq!(
            uid.instance_uid_base58(),
            Some(encode_base58_uid(0xd5d54d7201103869))
        );
        assert_eq!(
            Uid::parse_instance_uid_base58(&encode_base58_uid(0xd5d54d7201103869)),
            Ok(0xd5d54d7201103869)
        );
        let parsed: Uid = s.parse().unwrap();
        assert_eq!(uid, parsed);
    }

    #[test]
    fn test_singleton_has_no_instance_base58() {
        let uid = Uid::singleton(Label::new("my-actor").unwrap());
        assert_eq!(uid.instance_uid_base58(), None);
    }

    #[test]
    fn test_labeled_instance_display_parse() {
        let label = Label::new("my-actor").unwrap();
        let uid = Uid::Instance(0xd5d54d7201103869, Some(label.clone()));
        let s = uid.to_string();
        assert_eq!(
            s,
            format!("my-actor<{}>", encode_base58_uid(0xd5d54d7201103869))
        );
        let parsed: Uid = s.parse().unwrap();
        assert_eq!(parsed, uid);
        assert_eq!(parsed.label(), Some(&label));
    }

    #[test]
    fn test_labeled_instance_identity_ignores_label() {
        let a = Uid::Instance(0x42, Some(Label::new("alpha").unwrap()));
        let b = Uid::Instance(0x42, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), Ordering::Equal);

        use std::collections::hash_map::DefaultHasher;

        let hash = |uid: &Uid| {
            let mut h = DefaultHasher::new();
            uid.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_ordering_singleton_lt_instance() {
        let singleton = Uid::singleton(Label::new("zzz").unwrap());
        let instance = Uid::Instance(0, None);
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
        let a = Uid::Instance(1, None);
        let b = Uid::Instance(2, None);
        assert!(a < b);
    }

    #[test]
    fn test_uid_serde_roundtrip() {
        let uids = vec![
            Uid::singleton(Label::new("my-actor").unwrap()),
            Uid::Instance(0xabcdef0123456789, None),
            Uid::Instance(1, None),
            Uid::Instance(0xd5d54d7201103869, Some(Label::new("my-actor").unwrap())),
        ];
        for uid in uids {
            let json = serde_json::to_string(&uid).unwrap();
            assert_eq!(json, format!("\"{}\"", uid));
            let parsed: Uid = serde_json::from_str(&json).unwrap();
            assert_eq!(uid, parsed);

            let encoded = bincode::serde::encode_to_vec(&uid, bincode::config::legacy()).unwrap();
            let (parsed, len): (Uid, usize) =
                bincode::serde::decode_from_slice(&encoded, bincode::config::legacy()).unwrap();
            assert_eq!(len, encoded.len());
            assert_eq!(uid, parsed);
        }
    }

    #[test]
    fn test_uid_parse_errors() {
        // Empty string is invalid.
        assert!("".parse::<Uid>().is_err());
        // Invalid singleton label.
        assert!("123bad".parse::<Uid>().is_err());
        // Invalid base58.
        assert!("<0>".parse::<Uid>().is_err());
        // Missing closing delimiter.
        assert_eq!(
            "<abc".parse::<Uid>().unwrap_err().to_string(),
            "invalid uid syntax: expected \">\", found end of input"
        );
    }

    #[test]
    fn test_unique_uid_generation() {
        let a = Uid::anonymous();
        let b = Uid::anonymous();
        assert_ne!(a, b);
    }

    #[test]
    fn test_short_hex_parse() {
        let parsed: Uid = "<2>".parse().unwrap();
        assert_eq!(parsed, Uid::Instance(1, None));
    }

    #[test]
    fn test_proc_id_construction_and_accessors() {
        let uid = Uid::Instance(0xabc, None);
        let label = Label::new("my-proc").unwrap();
        let pid = ProcId::new(uid.clone(), Some(label.clone()));
        assert_eq!(pid.uid(), &uid);
        assert_eq!(pid.label(), Some(&label));
    }

    #[test]
    fn test_proc_id_eq_ignores_label() {
        let uid = Uid::Instance(0x42, None);
        let a = ProcId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = ProcId::new(uid, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn test_proc_id_hash_ignores_label() {
        use std::collections::hash_map::DefaultHasher;

        let uid = Uid::Instance(0x42, None);
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
        let a = ProcId::new(Uid::Instance(1, None), Some(Label::new("zzz").unwrap()));
        let b = ProcId::new(Uid::Instance(2, None), Some(Label::new("aaa").unwrap()));
        assert!(a < b);
    }

    #[test]
    fn test_proc_id_display() {
        let pid = ProcId::new(
            Uid::Instance(0xd5d54d7201103869, None),
            Some(Label::new("my-proc").unwrap()),
        );
        assert_eq!(
            pid.to_string(),
            format!("my-proc<{}>", encode_base58_uid(0xd5d54d7201103869))
        );

        let pid_singleton = ProcId::new(
            Uid::singleton(Label::new("my-proc").unwrap()),
            Some(Label::new("my-proc").unwrap()),
        );
        assert_eq!(pid_singleton.to_string(), "my-proc");
    }

    #[test]
    fn test_proc_id_debug() {
        let pid = ProcId::new(
            Uid::Instance(0xd5d54d7201103869, None),
            Some(Label::new("my-proc").unwrap()),
        );
        assert_eq!(
            format!("{:?}", pid),
            format!(
                "<'my-proc' my-proc<{}>>",
                encode_base58_uid(0xd5d54d7201103869)
            )
        );

        let pid_no_label = ProcId::new(Uid::Instance(0xd5d54d7201103869, None), None);
        assert_eq!(
            format!("{:?}", pid_no_label),
            format!("<<{}>>", encode_base58_uid(0xd5d54d7201103869))
        );
    }

    #[test]
    fn test_proc_id_fromstr_roundtrip() {
        let pid = ProcId::new(
            Uid::Instance(0xd5d54d7201103869, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let s = pid.to_string();
        let parsed: ProcId = s.parse().unwrap();
        assert_eq!(pid, parsed);
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-proc"));
    }

    #[test]
    fn test_proc_id_fromstr_singleton() {
        let parsed: ProcId = "my-proc".parse().unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::singleton(Label::new("my-proc").unwrap())
        );
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-proc"));
    }

    #[test]
    fn test_proc_id_fromstr_unlabeled_instance() {
        let expected_uid = Uid::Instance(0xabc123, None);
        let parsed: ProcId = expected_uid.to_string().parse().unwrap();
        assert_eq!(parsed.uid(), &expected_uid);
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_proc_id_fromstr_labeled_instance_with_underscore() {
        let expected_uid = Uid::Instance(0xabc123, None);
        let parsed: ProcId = format!("proc_agent{}", expected_uid).parse().unwrap();
        assert_eq!(parsed.uid(), &expected_uid);
        assert_eq!(
            parsed.label().map(|label| label.as_str()),
            Some("proc_agent")
        );
    }

    #[test]
    fn test_proc_id_fromstr_errors_are_stable() {
        assert_eq!(
            "".parse::<ProcId>().unwrap_err().to_string(),
            "invalid proc id: invalid uid syntax: expected \"label\" or \"<\", found end of input"
        );
        assert_eq!(
            "controller<2MuAHeDjLCEd"
                .parse::<ProcId>()
                .unwrap_err()
                .to_string(),
            "invalid proc id: invalid uid syntax: expected \">\", found end of input"
        );
        assert_eq!(
            "controller@tcp".parse::<ProcId>().unwrap_err().to_string(),
            "invalid proc id: invalid uid syntax: expected end of input, found \"@\""
        );
    }

    #[test]
    fn test_proc_id_serde_roundtrip() {
        let pid = ProcId::new(
            Uid::Instance(0xabcdef, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let json = serde_json::to_string(&pid).unwrap();
        let parsed: ProcId = serde_json::from_str(&json).unwrap();
        assert_eq!(pid, parsed);
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-proc"));

        let pid_none = ProcId::new(Uid::Instance(0xabcdef, None), None);
        let json_none = serde_json::to_string(&pid_none).unwrap();
        let parsed_none: ProcId = serde_json::from_str(&json_none).unwrap();
        assert_eq!(parsed_none.label(), None);
    }

    #[test]
    fn test_proc_id_singleton() {
        let label = Label::new("my-proc").unwrap();
        let pid = ProcId::singleton(label.clone());
        assert_eq!(*pid.uid(), Uid::Singleton(label.clone()));
        assert_eq!(pid.label(), Some(&label));
    }

    #[test]
    fn test_proc_id_instance() {
        let label = Label::new("my-proc").unwrap();
        let pid = ProcId::instance(label.clone());
        assert!(pid.uid().is_instance());
        assert_eq!(pid.label(), Some(&label));
        let pid2 = ProcId::instance(label);
        assert_ne!(pid, pid2);
    }

    #[test]
    fn test_proc_id_pseudo_uid_instance_returns_real_uid() {
        let uid = Uid::Instance(0xd5d54d7201103869, None);
        let pid = ProcId::new(uid.clone(), Some(Label::new("my-proc").unwrap()));
        assert_eq!(pid.pseudo_uid(), uid);
    }

    #[test]
    fn test_proc_id_pseudo_uid_singleton_is_instance_form() {
        let pid = ProcId::singleton(Label::new("my-proc").unwrap());
        assert!(matches!(pid.pseudo_uid(), Uid::Instance(_, _)));
    }

    #[test]
    fn test_proc_id_pseudo_uid_singleton_is_deterministic() {
        let a = ProcId::singleton(Label::new("my-proc").unwrap());
        let b = ProcId::singleton(Label::new("my-proc").unwrap());
        assert_eq!(a.pseudo_uid(), b.pseudo_uid());
    }

    #[test]
    fn test_proc_id_pseudo_uid_singleton_distinct_labels_differ() {
        let a = ProcId::singleton(Label::new("alpha").unwrap());
        let b = ProcId::singleton(Label::new("beta").unwrap());
        assert_ne!(a.pseudo_uid(), b.pseudo_uid());
    }

    #[test]
    fn test_proc_id_pseudo_uid_displays_as_short_base58() {
        let pid = ProcId::singleton(Label::new("my-proc").unwrap());
        let s = pid.pseudo_uid().to_string();
        assert!(s.starts_with('<') && s.ends_with('>'), "got: {s}");
        // base58 of u64 fits in 11 chars, plus the two delimiters.
        assert!(s.len() <= 13, "expected short base58 form, got: {s}");
    }

    #[test]
    fn test_actor_id_singleton() {
        let label = Label::new("my-actor").unwrap();
        let proc_id = ProcId::singleton(Label::new("my-proc").unwrap());
        let aid = ActorId::singleton(label.clone(), proc_id.clone());
        assert_eq!(*aid.uid(), Uid::Singleton(label.clone()));
        assert_eq!(aid.proc_id(), &proc_id);
        assert_eq!(aid.label(), Some(&label));
    }

    #[test]
    fn test_actor_id_anonymous() {
        let proc_id = ProcId::singleton(Label::new("my-proc").unwrap());
        let aid = ActorId::anonymous(proc_id.clone());
        assert!(aid.uid().is_instance());
        assert_eq!(aid.proc_id(), &proc_id);
        assert_eq!(aid.label(), None);
        let aid2 = ActorId::anonymous(proc_id);
        assert_ne!(aid, aid2);
    }

    #[test]
    fn test_actor_id_instance() {
        let label = Label::new("my-actor").unwrap();
        let proc_id = ProcId::singleton(Label::new("my-proc").unwrap());
        let aid = ActorId::instance(label.clone(), proc_id.clone());
        assert!(aid.uid().is_instance());
        assert_eq!(aid.proc_id(), &proc_id);
        assert_eq!(aid.label(), Some(&label));
        let aid2 = ActorId::instance(label, proc_id);
        assert_ne!(aid, aid2);
    }

    #[test]
    fn test_actor_id_construction_and_accessors() {
        let actor_uid = Uid::Instance(0xabc, None);
        let proc_id = ProcId::new(
            Uid::Instance(0xdef, None),
            Some(Label::new("my-proc").unwrap()),
        );
        let label = Label::new("my-actor").unwrap();
        let aid = ActorId::new(actor_uid.clone(), proc_id.clone(), Some(label.clone()));
        assert_eq!(aid.uid(), &actor_uid);
        assert_eq!(aid.proc_id(), &proc_id);
        assert_eq!(aid.label(), Some(&label));
    }

    #[test]
    fn test_actor_id_eq_ignores_label() {
        let actor_uid = Uid::Instance(0x42, None);
        let proc_id = ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap()));
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
        let actor_uid = Uid::Instance(0x42, None);
        let proc_a = ProcId::new(Uid::Instance(1, None), Some(Label::new("proc").unwrap()));
        let proc_b = ProcId::new(Uid::Instance(2, None), Some(Label::new("proc").unwrap()));
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

        let actor_uid = Uid::Instance(0x42, None);
        let proc_id = ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap()));
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
            Uid::Instance(0xff, None),
            ProcId::new(Uid::Instance(1, None), Some(Label::new("p").unwrap())),
            Some(Label::new("a").unwrap()),
        );
        let b = ActorId::new(
            Uid::Instance(0x01, None),
            ProcId::new(Uid::Instance(2, None), Some(Label::new("p").unwrap())),
            Some(Label::new("a").unwrap()),
        );
        assert!(a < b, "proc_id should be compared first");
    }

    #[test]
    fn test_actor_id_ord_then_uid() {
        let proc_id = ProcId::new(Uid::Instance(1, None), Some(Label::new("p").unwrap()));
        let a = ActorId::new(
            Uid::Instance(1, None),
            proc_id.clone(),
            Some(Label::new("a").unwrap()),
        );
        let b = ActorId::new(
            Uid::Instance(2, None),
            proc_id,
            Some(Label::new("a").unwrap()),
        );
        assert!(a < b);
    }

    #[test]
    fn test_actor_id_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        assert_eq!(
            aid.to_string(),
            format!(
                "my-actor<{}>.my-proc<{}>",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );
    }

    #[test]
    fn test_actor_id_debug() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        assert_eq!(
            format!("{:?}", aid),
            format!(
                "<'my-actor.my-proc' my-actor<{}>.my-proc<{}>>",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );

        let aid_no_labels = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            None,
        );
        assert_eq!(
            format!("{:?}", aid_no_labels),
            format!(
                "<<{}>.<{}>>",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );
    }

    #[test]
    fn test_actor_id_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let s = aid.to_string();
        let parsed: ActorId = s.parse().unwrap();
        assert_eq!(aid, parsed);
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-actor"));
        assert_eq!(
            parsed.proc_id().label().map(|l| l.as_str()),
            Some("my-proc")
        );
    }

    #[test]
    fn test_actor_id_fromstr_with_singletons() {
        let parsed: ActorId = "my-actor.my-proc".parse().unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::singleton(Label::new("my-actor").unwrap())
        );
        assert_eq!(
            *parsed.proc_id().uid(),
            Uid::singleton(Label::new("my-proc").unwrap())
        );
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-actor"));
        assert_eq!(
            parsed.proc_id().label().map(|l| l.as_str()),
            Some("my-proc")
        );
    }

    #[test]
    fn test_actor_id_fromstr_mixed_examples() {
        let proc_uid = Uid::Instance(0xabc123, None);
        let parsed: ActorId = format!("controller.some-proc-123{}", proc_uid)
            .parse()
            .unwrap();
        assert_eq!(
            parsed.uid(),
            &Uid::singleton(Label::new("controller").unwrap())
        );
        assert_eq!(parsed.proc_id().uid(), &proc_uid);
        assert_eq!(
            parsed.label().map(|label| label.as_str()),
            Some("controller")
        );
        assert_eq!(
            parsed.proc_id().label().map(|label| label.as_str()),
            Some("some-proc-123")
        );

        let expected_actor_uid = Uid::Instance(0xabc123, None);
        let expected_proc_uid = Uid::Instance(0xdef456, None);
        let parsed: ActorId = format!("{}.{}", expected_actor_uid, expected_proc_uid)
            .parse()
            .unwrap();
        assert_eq!(parsed.uid(), &expected_actor_uid);
        assert_eq!(parsed.proc_id().uid(), &expected_proc_uid);
        assert_eq!(parsed.label(), None);
        assert_eq!(parsed.proc_id().label(), None);

        let expected_actor_uid = Uid::Instance(0xabc123, None);
        let parsed: ActorId = format!("controller{}.local", expected_actor_uid)
            .parse()
            .unwrap();
        assert_eq!(parsed.uid(), &expected_actor_uid);
        assert_eq!(
            parsed.proc_id().uid(),
            &Uid::singleton(Label::new("local").unwrap())
        );
        assert_eq!(
            parsed.label().map(|label| label.as_str()),
            Some("controller")
        );
        assert_eq!(
            parsed.proc_id().label().map(|label| label.as_str()),
            Some("local")
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
    fn test_actor_id_fromstr_errors_are_stable() {
        assert_eq!(
            "local".parse::<ActorId>().unwrap_err().to_string(),
            "invalid actor id: expected format `<actor>.<proc>`"
        );
        assert_eq!(
            ".local".parse::<ActorId>().unwrap_err().to_string(),
            "invalid actor uid: invalid label: label must not be empty"
        );
        assert_eq!(
            "local.".parse::<ActorId>().unwrap_err().to_string(),
            "invalid proc uid in actor id: invalid label: label must not be empty"
        );
        assert_eq!(
            "local.<bad!>".parse::<ActorId>().unwrap_err().to_string(),
            "invalid proc uid in actor id: invalid base58 uid: bad!"
        );
    }

    #[test]
    fn test_actor_id_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef, None),
            ProcId::new(
                Uid::Instance(0x123456, None),
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
        let actor_uid = Uid::Instance(0xabc, None);
        let proc_id = ProcId::new(
            Uid::Instance(0xdef, None),
            Some(Label::new("my-proc").unwrap()),
        );
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
            Uid::Instance(0x42, None),
            ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap())),
            Some(Label::new("actor").unwrap()),
        );
        let a = PortId::new(actor_id.clone(), Port::from(10));
        let b = PortId::new(actor_id, Port::from(10));
        assert_eq!(a, b);
    }

    #[test]
    fn test_port_id_neq_different_port() {
        let actor_id = ActorId::new(
            Uid::Instance(0x42, None),
            ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap())),
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
            Uid::Instance(0x42, None),
            ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap())),
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
            Uid::Instance(0x42, None),
            ProcId::new(Uid::Instance(0x99, None), Some(Label::new("proc").unwrap())),
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
                Uid::Instance(0x01, None),
                ProcId::new(Uid::Instance(1, None), Some(Label::new("p").unwrap())),
                Some(Label::new("a").unwrap()),
            ),
            Port::from(99),
        );
        let b = PortId::new(
            ActorId::new(
                Uid::Instance(0x02, None),
                ProcId::new(Uid::Instance(1, None), Some(Label::new("p").unwrap())),
                Some(Label::new("a").unwrap()),
            ),
            Port::from(1),
        );
        assert!(a < b, "actor_id should be compared first");
    }

    #[test]
    fn test_port_id_display() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            pid.to_string(),
            format!(
                "my-actor<{}>.my-proc<{}>:42",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );
    }

    #[test]
    fn test_port_id_fromstr_examples() {
        let parsed: PortId = "local.local:0".parse().unwrap();
        assert_eq!(
            parsed.actor_id().uid(),
            &Uid::singleton(Label::new("local").unwrap())
        );
        assert_eq!(
            parsed.proc_id().uid(),
            &Uid::singleton(Label::new("local").unwrap())
        );
        assert_eq!(parsed.port(), Port::from(0));

        let expected_actor_uid = Uid::Instance(0xabc123, None);
        let parsed: PortId = format!("controller{}.local:42", expected_actor_uid)
            .parse()
            .unwrap();
        assert_eq!(parsed.actor_id().uid(), &expected_actor_uid);
        assert_eq!(
            parsed.actor_id().label().map(|label| label.as_str()),
            Some("controller")
        );
        assert_eq!(
            parsed.proc_id().uid(),
            &Uid::singleton(Label::new("local").unwrap())
        );
        assert_eq!(parsed.port(), Port::from(42));

        let expected_actor_uid = Uid::Instance(0xabc123, None);
        let expected_proc_uid = Uid::Instance(0xdef456, None);
        let parsed: PortId = format!("{}.{}:7", expected_actor_uid, expected_proc_uid)
            .parse()
            .unwrap();
        assert_eq!(parsed.actor_id().uid(), &expected_actor_uid);
        assert_eq!(parsed.proc_id().uid(), &expected_proc_uid);
        assert_eq!(parsed.port(), Port::from(7));
    }

    #[test]
    fn test_port_id_debug_all_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            format!(
                "<'my-actor.my-proc' my-actor<{}>.my-proc<{}>:42>",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );
    }

    #[test]
    fn test_port_id_debug_no_labels() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            None,
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            format!(
                "<<{}>.<{}>:42>",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );
    }

    #[test]
    fn test_port_id_debug_actor_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(Uid::Instance(0xdef456, None), None),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            format!(
                "<'my-actor' my-actor<{}>.<{}>:42>",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );
    }

    #[test]
    fn test_port_id_debug_proc_label_only() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            None,
        );
        let pid = PortId::new(aid, Port::from(42));
        assert_eq!(
            format!("{:?}", pid),
            format!(
                "<'.my-proc' <{}>.my-proc<{}>:42>",
                encode_base58_uid(0xabc123),
                encode_base58_uid(0xdef456)
            )
        );
    }

    #[test]
    fn test_port_id_fromstr_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabc123, None),
            ProcId::new(
                Uid::Instance(0xdef456, None),
                Some(Label::new("my-proc").unwrap()),
            ),
            Some(Label::new("my-actor").unwrap()),
        );
        let pid = PortId::new(aid, Port::from(42));
        let s = pid.to_string();
        let parsed: PortId = s.parse().unwrap();
        assert_eq!(pid, parsed);
        assert_eq!(
            parsed.actor_id().label().map(|l| l.as_str()),
            Some("my-actor")
        );
        assert_eq!(
            parsed.actor_id().proc_id().label().map(|l| l.as_str()),
            Some("my-proc")
        );
    }

    #[test]
    fn test_port_id_fromstr_errors_are_stable() {
        assert_eq!(
            "local.local".parse::<PortId>().unwrap_err().to_string(),
            "invalid port id: expected format `<actor>:<port>`"
        );
        assert_eq!(
            "local.local:".parse::<PortId>().unwrap_err().to_string(),
            "invalid port: "
        );
        assert_eq!(
            "local.local:not-a-port"
                .parse::<PortId>()
                .unwrap_err()
                .to_string(),
            "invalid port: not-a-port"
        );
        assert_eq!(
            "local.local:7@tcp://127.0.0.1:1"
                .parse::<PortId>()
                .unwrap_err()
                .to_string(),
            "invalid port: 7@tcp://127.0.0.1:1"
        );
    }

    #[test]
    fn test_port_id_fromstr_errors() {
        // Missing colon.
        assert!("<abc>.<def>".parse::<PortId>().is_err());
        // Invalid port.
        assert!("actor.proc:notanumber".parse::<PortId>().is_err());
    }

    #[test]
    fn test_port_id_serde_roundtrip() {
        let aid = ActorId::new(
            Uid::Instance(0xabcdef, None),
            ProcId::new(
                Uid::Instance(0x123456, None),
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
