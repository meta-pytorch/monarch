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

use rand::RngCore as _;
use serde::Deserialize;
use serde::Serialize;
use smol_str::SmolStr;

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
            if !ch.is_ascii_lowercase() && !ch.is_ascii_digit() && ch != '-' {
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
                if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' {
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
        let uid = rand::thread_rng().next_u64();
        Uid::Instance(uid)
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
        assert_eq!(Label::new("ab_c"), Err(LabelError::InvalidChar('_')));
        assert_eq!(Label::new("ab.c"), Err(LabelError::InvalidChar('.')));
        assert_eq!(Label::new("aBc"), Err(LabelError::InvalidChar('B')));
    }

    #[test]
    fn test_label_strip() {
        assert_eq!(Label::strip("Hello-World").as_str(), "hello-world");
        assert_eq!(Label::strip("123abc").as_str(), "abc");
        assert_eq!(Label::strip("---abc---").as_str(), "abc");
        assert_eq!(Label::strip("").as_str(), "nil");
        assert_eq!(Label::strip("123").as_str(), "nil");
        assert_eq!(Label::strip("My_Service!").as_str(), "myservice");
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
}
