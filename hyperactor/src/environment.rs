/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A bounded, typed environment for persistent actor context.
//!
//! An [`ActorEnvironment`] is a small set of typed attributes with a stable,
//! language-independent wire representation.
//!
//! `hyperactor` owns how this environment is carried through actor creation,
//! while higher-level crates own the values it contains. Those crates declare
//! typed [`Key`] attributes. Putting those values in a fixed struct would move
//! higher-level concepts into `hyperactor` and require core runtime and wire
//! changes for each new kind of inherited context. This is not an untyped
//! general-purpose map: the API preserves each value's type, enforces fixed
//! bounds, and requires mutable ownership for construction updates.
//!
//! # AENV invariant registry
//!
//! - **AENV-0 (value integrity):** every `ActorEnvironment` is structurally
//!   valid, bounded, unique-keyed, and updated atomically.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use hyperactor_config::AttrValue;
use hyperactor_config::Flattrs;
use hyperactor_config::FlattrsValidationError;
use hyperactor_config::Key;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de::DeserializeOwned;
use serde::de::Error as _;
use serde_multipart::Part;

const MAX_ACTOR_ENVIRONMENT_ENTRIES: usize = 32;
const MAX_ACTOR_ENVIRONMENT_BYTES: usize = 16 * 1024;

/// An error constructing or decoding an [`ActorEnvironment`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ActorEnvironmentError {
    /// One serialized value cannot fit within the environment's byte bound.
    #[error("serialized actor environment value is {bytes} bytes; maximum is {maximum}")]
    ValueTooLarge {
        /// The serialized value size in bytes.
        bytes: usize,
        /// The maximum accepted serialized value size.
        maximum: usize,
    },

    /// The encoded entry count exceeds the environment's fixed bound.
    #[error("actor environment has {entries} entries; maximum is {maximum}")]
    TooManyEntries {
        /// The number of encoded entries.
        entries: usize,
        /// The maximum accepted number of entries.
        maximum: usize,
    },

    /// The encoded environment exceeds its fixed byte bound.
    #[error("actor environment is {bytes} bytes; maximum is {maximum}")]
    TooLarge {
        /// The encoded size in bytes.
        bytes: usize,
        /// The maximum accepted encoded size.
        maximum: usize,
    },

    /// A key occurs more than once in the encoded environment.
    #[error("actor environment contains duplicate key hash {key_hash:#x}")]
    DuplicateKey {
        /// The repeated key hash.
        key_hash: u64,
    },

    /// A typed value could not be serialized into the environment.
    #[error("failed to serialize actor environment value: {0}")]
    ValueSerialization(#[from] bincode::error::EncodeError),

    /// The underlying `Flattrs` wire structure is malformed.
    #[error("invalid actor environment encoding: {0}")]
    InvalidEncoding(#[from] FlattrsValidationError),
}

/// A bounded collection of typed actor-context attributes.
///
/// The inner buffer is never exposed. Clones share the immutable buffer; a
/// subsequent builder mutation creates a bounded private copy.
#[derive(Clone)]
pub struct ActorEnvironment(Arc<Flattrs>);

impl Default for ActorEnvironment {
    fn default() -> Self {
        Self(Arc::new(Flattrs::new()))
    }
}

impl ActorEnvironment {
    /// Store `value` under `key` while enforcing the environment's entry and
    /// serialized-size bounds.
    ///
    /// This is a construction API; successful updates replace the underlying
    /// immutable buffer atomically.
    pub fn set<T: Serialize>(
        &mut self,
        key: Key<T>,
        value: T,
    ) -> Result<(), ActorEnvironmentError> {
        let serialized = bincode::serde::encode_to_vec(value, bincode::config::legacy())?;
        if serialized.len() > MAX_ACTOR_ENVIRONMENT_BYTES {
            return Err(ActorEnvironmentError::ValueTooLarge {
                bytes: serialized.len(),
                maximum: MAX_ACTOR_ENVIRONMENT_BYTES,
            });
        }
        let mut candidate = self.0.as_ref().clone();
        candidate.set_serialized(key.key_hash(), &serialized);
        validate(&candidate)?;
        self.0 = Arc::new(candidate);
        Ok(())
    }

    /// Read the value stored under `key`, if present.
    pub fn get<T: AttrValue + DeserializeOwned>(&self, key: Key<T>) -> Option<T> {
        self.0.get(key)
    }

    /// Whether the environment holds no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of entries in the environment.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

fn validate(attrs: &Flattrs) -> Result<(), ActorEnvironmentError> {
    attrs.validate_wire()?;
    let encoded_bytes = attrs.encoded_len();
    if encoded_bytes > MAX_ACTOR_ENVIRONMENT_BYTES {
        return Err(ActorEnvironmentError::TooLarge {
            bytes: encoded_bytes,
            maximum: MAX_ACTOR_ENVIRONMENT_BYTES,
        });
    }

    let declared = attrs.len();
    if declared > MAX_ACTOR_ENVIRONMENT_ENTRIES {
        return Err(ActorEnvironmentError::TooManyEntries {
            entries: declared,
            maximum: MAX_ACTOR_ENVIRONMENT_ENTRIES,
        });
    }

    let mut seen = HashSet::with_capacity(declared);
    for (key_hash, _) in attrs.iter() {
        if !seen.insert(key_hash) {
            return Err(ActorEnvironmentError::DuplicateKey { key_hash });
        }
    }
    Ok(())
}

impl Serialize for ActorEnvironment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ActorEnvironment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // This bound controls accepted instance state. The transport frame
        // limit is responsible for bounding allocation before field decoding.
        let part = Part::deserialize(deserializer)?;
        let encoded_bytes = part.len();
        if encoded_bytes > MAX_ACTOR_ENVIRONMENT_BYTES {
            return Err(D::Error::custom(ActorEnvironmentError::TooLarge {
                bytes: encoded_bytes,
                maximum: MAX_ACTOR_ENVIRONMENT_BYTES,
            }));
        }
        let attrs = Flattrs::from_part(part);
        validate(&attrs).map_err(D::Error::custom)?;
        Ok(Self(Arc::new(attrs)))
    }
}

impl std::fmt::Debug for ActorEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorEnvironment")
            .field("len", &self.0.len())
            .finish()
    }
}

/// Equality over the `(key hash, serialized bytes)` set, independent of
/// insertion order. Construction and deserialization enforce one entry per key
/// hash.
impl PartialEq for ActorEnvironment {
    fn eq(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }
        if self.0.len() != other.0.len() {
            return false;
        }
        let mine: HashMap<u64, &[u8]> = self.0.iter().collect();
        other
            .0
            .iter()
            .all(|(key_hash, value)| mine.get(&key_hash) == Some(&value))
    }
}

impl Eq for ActorEnvironment {}

#[cfg(test)]
mod tests {
    use hyperactor_config::declare_attrs;

    use super::*;

    declare_attrs! {
        attr TEST_TAG: u64;
        attr TEST_LABEL: String;
        attr TEST_BYTES: Vec<u8>;
    }

    fn decode_raw(raw: Vec<u8>) -> Result<ActorEnvironment, bincode::error::DecodeError> {
        let attrs = Flattrs::from_part(Part::from(raw));
        let encoded = bincode::serde::encode_to_vec(attrs, bincode::config::legacy())
            .expect("serialize raw Flattrs");
        bincode::serde::decode_from_slice(&encoded, bincode::config::legacy())
            .map(|(value, _)| value)
    }

    // Malformed-buffer tests must encode the private layout directly because
    // the public Flattrs builders cannot construct invalid wire shapes.
    fn append_entry(raw: &mut Vec<u8>, key_hash: u64, value: &[u8]) {
        raw.extend_from_slice(&key_hash.to_le_bytes());
        raw.extend_from_slice(&(value.len() as u32).to_le_bytes());
        raw.extend_from_slice(value);
    }

    #[test]
    fn default_accepts_first_insertion() {
        let mut env = ActorEnvironment::default();
        assert!(env.is_empty());
        env.set(TEST_TAG, 7u64).expect("insert first value");
        assert_eq!(env.get(TEST_TAG), Some(7u64));
        assert_eq!(env.len(), 1);
    }

    #[test]
    fn serde_roundtrips() {
        let mut env = ActorEnvironment::default();
        env.set(TEST_TAG, 42u64).expect("insert tag");
        env.set(TEST_LABEL, "root".to_string())
            .expect("insert label");

        let bytes =
            bincode::serde::encode_to_vec(&env, bincode::config::legacy()).expect("serialize");
        let restored: ActorEnvironment =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy())
                .map(|(value, _)| value)
                .expect("deserialize");

        assert_eq!(restored.get(TEST_TAG), Some(42u64));
        assert_eq!(restored.get(TEST_LABEL), Some("root".to_string()));
        assert_eq!(restored, env);
    }

    #[test]
    fn equality_is_order_independent() {
        let mut a = ActorEnvironment::default();
        a.set(TEST_TAG, 1u64).expect("insert tag");
        a.set(TEST_LABEL, "x".to_string()).expect("insert label");

        let mut b = ActorEnvironment::default();
        b.set(TEST_LABEL, "x".to_string()).expect("insert label");
        b.set(TEST_TAG, 1u64).expect("insert tag");

        assert_eq!(a, b);

        let mut c = ActorEnvironment::default();
        c.set(TEST_TAG, 2u64).expect("insert tag");
        c.set(TEST_LABEL, "x".to_string()).expect("insert label");
        assert_ne!(a, c);
    }

    #[test]
    fn serde_rejects_duplicate_keys() {
        let value = bincode::serde::encode_to_vec(1u64, bincode::config::legacy())
            .expect("serialize value");
        let mut raw = 2u16.to_le_bytes().to_vec();
        append_entry(&mut raw, TEST_TAG.key_hash(), &value);
        append_entry(&mut raw, TEST_TAG.key_hash(), &value);

        assert!(decode_raw(raw).is_err(), "duplicate keys must be rejected");
    }

    #[test]
    fn serde_rejects_truncated_and_trailing_buffers() {
        assert!(
            decode_raw(vec![1]).is_err(),
            "a partial count header must be rejected"
        );
        assert!(
            decode_raw(1u16.to_le_bytes().to_vec()).is_err(),
            "a declared entry without entry bytes must be rejected"
        );
        assert!(
            decode_raw(vec![0, 0, 1]).is_err(),
            "bytes after the declared entries must be rejected"
        );
    }

    #[test]
    fn set_rejects_oversized_values_without_mutating() {
        let mut env = ActorEnvironment::default();
        let err = env
            .set(TEST_BYTES, vec![0; MAX_ACTOR_ENVIRONMENT_BYTES])
            .expect_err("oversized value must be rejected");
        assert!(matches!(err, ActorEnvironmentError::ValueTooLarge { .. }));
        assert!(env.is_empty(), "a rejected insertion must be atomic");
    }

    #[test]
    fn set_rejects_entry_and_aggregate_bounds_atomically() {
        let mut raw = (MAX_ACTOR_ENVIRONMENT_ENTRIES as u16)
            .to_le_bytes()
            .to_vec();
        for offset in 1..=MAX_ACTOR_ENVIRONMENT_ENTRIES {
            append_entry(
                &mut raw,
                TEST_TAG.key_hash().wrapping_add(offset as u64),
                &[],
            );
        }
        let mut full = decode_raw(raw).expect("decode environment at the entry bound");
        let before = full.clone();
        let err = full
            .set(TEST_TAG, 1u64)
            .expect_err("a 33rd entry must be rejected");
        assert!(matches!(err, ActorEnvironmentError::TooManyEntries { .. }));
        assert_eq!(full, before, "a rejected insertion must be atomic");

        let mut large = ActorEnvironment::default();
        large
            .set(TEST_BYTES, vec![0; MAX_ACTOR_ENVIRONMENT_BYTES - 64])
            .expect("the first value fits within the aggregate bound");
        let before = large.clone();
        let err = large
            .set(TEST_LABEL, "x".repeat(128))
            .expect_err("aggregate encoded size must be enforced");
        assert!(matches!(err, ActorEnvironmentError::TooLarge { .. }));
        assert_eq!(large, before, "a rejected insertion must be atomic");
    }

    #[test]
    fn serde_rejects_policy_bounds() {
        let mut too_many = ((MAX_ACTOR_ENVIRONMENT_ENTRIES + 1) as u16)
            .to_le_bytes()
            .to_vec();
        for offset in 1..=MAX_ACTOR_ENVIRONMENT_ENTRIES + 1 {
            append_entry(
                &mut too_many,
                TEST_TAG.key_hash().wrapping_add(offset as u64),
                &[],
            );
        }
        assert!(
            decode_raw(too_many).is_err(),
            "an oversized entry set must be rejected"
        );

        let mut too_large = 1u16.to_le_bytes().to_vec();
        append_entry(
            &mut too_large,
            TEST_BYTES.key_hash(),
            &vec![0; MAX_ACTOR_ENVIRONMENT_BYTES],
        );
        let err = decode_raw(too_large).expect_err("oversized environment must fail");
        assert!(
            err.to_string().contains("maximum"),
            "a structurally valid oversized environment must hit the byte bound: {err}"
        );

        let mut boundary = ActorEnvironment::default();
        boundary
            .set(TEST_BYTES, Vec::<u8>::new())
            .expect("measure one-entry encoding overhead");
        let payload_len = MAX_ACTOR_ENVIRONMENT_BYTES - boundary.0.encoded_len();
        boundary
            .set(TEST_BYTES, vec![0; payload_len])
            .expect("the exact byte boundary must be accepted");
        assert_eq!(boundary.0.encoded_len(), MAX_ACTOR_ENVIRONMENT_BYTES);
        let encoded = bincode::serde::encode_to_vec(&boundary, bincode::config::legacy())
            .expect("serialize boundary environment");
        let restored: ActorEnvironment =
            bincode::serde::decode_from_slice(&encoded, bincode::config::legacy())
                .map(|(value, _)| value)
                .expect("deserialize boundary environment");
        assert_eq!(restored, boundary);
    }
}
