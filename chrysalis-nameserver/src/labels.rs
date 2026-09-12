/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

const MAX_NAME_LEN: usize = 63;
const MAX_PREFIX_LEN: usize = 253;
const MAX_VALUE_LEN: usize = 63;

/// A validated Kubernetes-style label key.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LabelKey(String);

impl LabelKey {
    /// Returns the canonical key text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for LabelKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for LabelKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for LabelKey {
    type Err = LabelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (prefix, name) = value
            .split_once('/')
            .map_or((None, value), |(prefix, name)| (Some(prefix), name));
        validate_name(name).map_err(|kind| LabelError::InvalidKey {
            key: value.to_owned(),
            kind,
        })?;
        if let Some(prefix) = prefix {
            validate_prefix(prefix).map_err(|kind| LabelError::InvalidKey {
                key: value.to_owned(),
                kind,
            })?;
        }
        Ok(Self(value.to_owned()))
    }
}

/// A validated Kubernetes-style label value.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LabelValue(String);

impl LabelValue {
    /// Returns the label value text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for LabelValue {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for LabelValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for LabelValue {
    type Err = LabelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > MAX_VALUE_LEN {
            return Err(LabelError::ValueTooLong { len: value.len() });
        }
        if !value.is_empty() && !valid_name_text(value) {
            return Err(LabelError::InvalidValue(value.to_owned()));
        }
        Ok(Self(value.to_owned()))
    }
}

/// One immutable set of unique Kubernetes-style labels.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Labels(BTreeMap<LabelKey, LabelValue>);

impl Labels {
    /// Constructs an empty label set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates raw key/value pairs and rejects duplicate keys.
    pub fn try_from_iter<I, K, V>(pairs: I) -> Result<Self, LabelError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut labels = BTreeMap::new();
        for (key, value) in pairs {
            let key: LabelKey = key.as_ref().parse()?;
            let value: LabelValue = value.as_ref().parse()?;
            if labels.insert(key.clone(), value).is_some() {
                return Err(LabelError::DuplicateKey(key));
            }
        }
        Ok(Self(labels))
    }

    /// Returns labels in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&LabelKey, &LabelValue)> {
        self.0.iter()
    }

    /// Returns the number of labels.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether this set has no labels.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A Kubernetes-style label validation failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LabelError {
    /// A key's prefix or name violates the label-key grammar.
    #[error("invalid label key {key:?}: {kind}")]
    InvalidKey { key: String, kind: KeyError },
    /// A value exceeds Kubernetes' 63-character limit.
    #[error("label value is too long: {len}, maximum {MAX_VALUE_LEN}")]
    ValueTooLong { len: usize },
    /// A nonempty value violates the label-value grammar.
    #[error("invalid label value {0:?}")]
    InvalidValue(String),
    /// A label set contains one key more than once.
    #[error("duplicate label key {0}")]
    DuplicateKey(LabelKey),
}

/// The violated portion of a Kubernetes-style label key.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KeyError {
    /// The required name segment is empty.
    #[error("name is empty")]
    EmptyName,
    /// The name segment exceeds 63 characters.
    #[error("name exceeds 63 characters")]
    NameTooLong,
    /// The name segment contains invalid characters or endpoints.
    #[error("name has invalid syntax")]
    InvalidName,
    /// The optional DNS prefix is empty.
    #[error("prefix is empty")]
    EmptyPrefix,
    /// The optional DNS prefix exceeds 253 characters.
    #[error("prefix exceeds 253 characters")]
    PrefixTooLong,
    /// The optional prefix is not a DNS subdomain.
    #[error("prefix is not a DNS subdomain")]
    InvalidPrefix,
}

fn validate_name(name: &str) -> Result<(), KeyError> {
    if name.is_empty() {
        return Err(KeyError::EmptyName);
    }
    if name.len() > MAX_NAME_LEN {
        return Err(KeyError::NameTooLong);
    }
    if !valid_name_text(name) {
        return Err(KeyError::InvalidName);
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), KeyError> {
    if prefix.is_empty() {
        return Err(KeyError::EmptyPrefix);
    }
    if prefix.len() > MAX_PREFIX_LEN {
        return Err(KeyError::PrefixTooLong);
    }
    if !prefix.split('.').all(valid_dns_label) {
        return Err(KeyError::InvalidPrefix);
    }
    Ok(())
}

fn valid_name_text(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_dns_label(value: &str) -> bool {
    let is_endpoint = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    !value.is_empty()
        && value.len() <= MAX_NAME_LEN
        && value.as_bytes().first().is_some_and(is_endpoint)
        && value.as_bytes().last().is_some_and(is_endpoint)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_kubernetes_label_examples() {
        for key in ["release", "app.kubernetes.io/name", "example.com/a_B.c-9"] {
            assert!(key.parse::<LabelKey>().is_ok(), "accept key {key}");
        }
        for value in ["", "production", "a_B.c-9"] {
            assert!(value.parse::<LabelValue>().is_ok(), "accept value {value}");
        }
    }

    #[test]
    fn rejects_invalid_keys_and_values() {
        for key in [
            "",
            "/name",
            "prefix/",
            "UPPER.example/name",
            "a//b",
            "-name",
        ] {
            assert!(key.parse::<LabelKey>().is_err(), "reject key {key}");
        }
        for value in ["-value", "value_", "white space"] {
            assert!(value.parse::<LabelValue>().is_err(), "reject value {value}");
        }
    }

    #[test]
    fn rejects_limits_and_duplicate_keys() {
        assert!("a".repeat(64).parse::<LabelKey>().is_err());
        assert!("a".repeat(64).parse::<LabelValue>().is_err());
        assert!(Labels::try_from_iter([("a", "one"), ("a", "two")]).is_err());
    }
}
