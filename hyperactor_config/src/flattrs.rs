/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Flat attribute storage for message headers.
//!
//! This module provides `Flattrs`, a type optimized for message passing scenarios
//! where headers are often forwarded without inspection. It uses a single contiguous
//! buffer with inline entry lengths for efficient zero-copy passthrough.
//!
//! # Wire Format
//!
//! ```text
//! ┌─────────────┬──────────────────────────────────────────────────┐
//! │ num_entries │ entries...                                       │
//! │ (u16)       │ (key_hash: u64, len: u32, value: [u8])...          │
//! └─────────────┴──────────────────────────────────────────────────┘
//! ```
//!
//! Each entry is self-describing with its length inline, allowing linear scan
//! without a separate index section.
//!
//! - Key IDs are FNV-1a hashes of key names (stable, computed at compile time)
//! - Uses linear search (optimal for typical small header counts of 2-5 entries)
//!
//! # Design Benefits
//!
//! - **Zero-copy passthrough**: Forward the entire buffer without parsing
//! - **Zero-copy serialization**: Uses `Part` for zero-copy through multipart codec
//! - **Simple implementation**: No mode switching, just a single buffer
//! - **Compact wire format**: u64 key IDs instead of string names
//!
//! # Example
//!
//! ```ignore
//! use hyperactor_config::flattrs::Flattrs;
//! use hyperactor_config::attrs::declare_attrs;
//!
//! declare_attrs! {
//!     pub attr TIMESTAMP: u64;
//!     pub attr REQUEST_ID: String;
//! }
//!
//! let mut headers = Flattrs::new();
//! headers.set(TIMESTAMP, 1234567890u64);
//! headers.set(REQUEST_ID, "req-123".to_string());
//!
//! // Lazy deserialization on access
//! let ts: Option<u64> = headers.get(TIMESTAMP);
//! ```

use bytes::Bytes;
use bytes::BytesMut;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_multipart::Part;

use crate::attrs::AttrValue;
use crate::attrs::Attrs;
use crate::attrs::Key;

/// Header size: num_entries as u16
const HEADER_SIZE: usize = 2;

/// Entry header size: key_hash (u64) + len (u32) = 12 bytes
const ENTRY_HEADER_SIZE: usize = 12;

/// Flat attribute storage for message headers.
///
/// Uses a single contiguous buffer with inline entry lengths.
/// Each entry is `[key_hash: u64][len: u32][value: bytes]`.
/// Linear scan is used for lookup, which is optimal for small N.
#[derive(Clone, Default)]
pub struct Flattrs {
    /// The buffer containing all entries.
    /// Format: [num_entries: u16][entries...]
    /// Each entry: [key_hash: u64][len: u32][value: bytes]
    buffer: BytesMut,
}

impl Flattrs {
    /// Create a new empty Flattrs.
    pub fn new() -> Self {
        let mut buffer = BytesMut::with_capacity(HEADER_SIZE);
        buffer.extend_from_slice(&0u16.to_le_bytes());
        Self { buffer }
    }

    /// Create from a `Part`.
    pub fn from_part(part: Part) -> Self {
        Self {
            buffer: BytesMut::from(part.into_bytes().as_ref()),
        }
    }

    /// Convert to wire format for transmission.
    ///
    /// Returns a [`Part`] for zero-copy serialization through the multipart codec.
    pub fn to_part(&self) -> Part {
        Part::from(Bytes::copy_from_slice(&self.buffer))
    }

    /// Serialize a value and store it.
    ///
    /// If the key already exists:
    /// - Same size value: overwrite in place (no shifting)
    /// - Different size: remove old entry and append new one
    pub fn set<T: Serialize>(&mut self, key: Key<T>, value: T) {
        let key_hash = key.key_hash();
        let serialized = bincode::serialize(&value).expect("serialization failed");

        // If key exists, either overwrite in place or compact + append
        if let Some((offset, old_len)) = self.find_entry_location(key_hash) {
            if serialized.len() == old_len {
                // Same size - overwrite value in place
                let value_start = offset + ENTRY_HEADER_SIZE;
                self.buffer[value_start..value_start + old_len].copy_from_slice(&serialized);
                return;
            }

            // Different size - remove old entry by shifting
            let entry_size = ENTRY_HEADER_SIZE + old_len;
            let end = offset + entry_size;

            if end < self.buffer.len() {
                self.buffer.copy_within(end.., offset);
            }
            self.buffer.truncate(self.buffer.len() - entry_size);

            // Decrement entry count since `self.append_entry` will increment it
            let count = self.len();
            self.buffer[0..2].copy_from_slice(&((count - 1) as u16).to_le_bytes());
        }

        self.append_entry(key_hash, &serialized);
    }

    /// Get a value, deserializing from the buffer.
    ///
    /// Uses linear search which is optimal for the typical small
    /// number of headers (2-5 entries).
    pub fn get<T: AttrValue + DeserializeOwned>(&self, key: Key<T>) -> Option<T> {
        let key_hash = key.key_hash();
        let value_bytes = self.find_value(key_hash)?;
        bincode::deserialize(value_bytes).ok()
    }

    /// Check if a key exists.
    #[inline]
    pub fn contains_key<T>(&self, key: Key<T>) -> bool {
        self.find_value(key.key_hash()).is_some()
    }

    /// Returns true if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        if self.buffer.len() < HEADER_SIZE {
            return 0;
        }
        u16::from_le_bytes([self.buffer[0], self.buffer[1]]) as usize
    }

    /// Convert from an existing Attrs by serializing all values.
    pub fn from_attrs(attrs: &Attrs) -> Self {
        let mut flattrs = Self::new();
        for (name, value) in attrs.iter() {
            let key_hash = crate::attrs::fnv1a_hash(name.as_bytes());
            let serialized = value.serialize_bincode();
            flattrs.append_entry(key_hash, &serialized);
        }
        flattrs
    }

    /// Find the value bytes for a given key_hash by scanning entries.
    fn find_value(&self, key_hash: u64) -> Option<&[u8]> {
        if self.buffer.len() < HEADER_SIZE {
            return None;
        }

        let num_entries = u16::from_le_bytes([self.buffer[0], self.buffer[1]]) as usize;
        let mut offset = HEADER_SIZE;

        for _ in 0..num_entries {
            if offset + ENTRY_HEADER_SIZE > self.buffer.len() {
                return None;
            }

            let entry_key_hash =
                u64::from_le_bytes(self.buffer[offset..offset + 8].try_into().unwrap_or([0; 8]));
            let entry_len = u32::from_le_bytes(
                self.buffer[offset + 8..offset + 12]
                    .try_into()
                    .unwrap_or([0; 4]),
            ) as usize;

            let value_start = offset + ENTRY_HEADER_SIZE;
            let value_end = value_start + entry_len;

            if value_end > self.buffer.len() {
                return None;
            }

            if entry_key_hash == key_hash {
                return Some(&self.buffer[value_start..value_end]);
            }

            offset = value_end;
        }

        None
    }

    /// Find the location (offset, value_len) of an entry by key_hash.
    fn find_entry_location(&self, key_hash: u64) -> Option<(usize, usize)> {
        if self.buffer.len() < HEADER_SIZE {
            return None;
        }

        let num_entries = u16::from_le_bytes([self.buffer[0], self.buffer[1]]) as usize;
        let mut offset = HEADER_SIZE;

        for _ in 0..num_entries {
            if offset + ENTRY_HEADER_SIZE > self.buffer.len() {
                return None;
            }

            let entry_key_hash =
                u64::from_le_bytes(self.buffer[offset..offset + 8].try_into().unwrap_or([0; 8]));
            let entry_len = u32::from_le_bytes(
                self.buffer[offset + 8..offset + 12]
                    .try_into()
                    .unwrap_or([0; 4]),
            ) as usize;

            if entry_key_hash == key_hash {
                return Some((offset, entry_len));
            }

            offset += ENTRY_HEADER_SIZE + entry_len;
        }

        None
    }

    /// Append a new entry to the buffer.
    fn append_entry(&mut self, key_hash: u64, value: &[u8]) {
        let len = self.len();
        self.buffer[0..2].copy_from_slice(&((len + 1) as u16).to_le_bytes());

        // Append entry: key_hash + len + value
        self.buffer.extend_from_slice(&key_hash.to_le_bytes());
        self.buffer
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(value);
    }
}

impl From<Attrs> for Flattrs {
    fn from(attrs: Attrs) -> Self {
        Self::from_attrs(&attrs)
    }
}

impl From<&Attrs> for Flattrs {
    fn from(attrs: &Attrs) -> Self {
        Self::from_attrs(attrs)
    }
}

impl std::fmt::Debug for Flattrs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flattrs").field("len", &self.len()).finish()
    }
}

impl std::fmt::Display for Flattrs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::attrs::lookup_key_info;

        let mut offset = HEADER_SIZE;
        let mut first = true;

        for _ in 0..self.len() {
            let key_hash = u64::from_le_bytes(self.buffer[offset..offset + 8].try_into().unwrap());
            let entry_len =
                u32::from_le_bytes(self.buffer[offset + 8..offset + 12].try_into().unwrap())
                    as usize;
            let value_bytes = &self.buffer[offset + ENTRY_HEADER_SIZE..][..entry_len];

            if !first {
                write!(f, ",")?;
            }
            first = false;

            let info =
                lookup_key_info(key_hash).expect("key should be registered via declare_attrs!");

            let value = (info.deserialize_bincode)(value_bytes).expect("value should deserialize");
            write!(f, "{}={}", info.name, (info.display)(value.as_ref()))?;

            offset += ENTRY_HEADER_SIZE + entry_len;
        }

        Ok(())
    }
}

impl Serialize for Flattrs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_part().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Flattrs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let part: Part = Deserialize::deserialize(deserializer)?;
        Ok(Self::from_part(part))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attrs::declare_attrs;

    declare_attrs! {
        attr TEST_U64: u64;
        attr TEST_STRING: String;
        attr TEST_BOOL: bool;
    }

    #[test]
    fn test_basic_set_get() {
        let mut attrs = Flattrs::new();

        attrs.set(TEST_U64, 42u64);
        attrs.set(TEST_STRING, "hello".to_string());
        attrs.set(TEST_BOOL, true);

        assert_eq!(attrs.get(TEST_U64), Some(42u64));
        assert_eq!(attrs.get(TEST_STRING), Some("hello".to_string()));
        assert_eq!(attrs.get(TEST_BOOL), Some(true));
    }

    #[test]
    fn test_missing_key() {
        let attrs = Flattrs::new();
        assert_eq!(attrs.get::<u64>(TEST_U64), None);
    }

    #[test]
    fn test_set_replaces_existing() {
        let mut attrs = Flattrs::new();
        attrs.set(TEST_U64, 42u64);
        attrs.set(TEST_U64, 100u64);
        assert_eq!(attrs.get(TEST_U64), Some(100u64));
        assert_eq!(attrs.len(), 1);
    }

    #[test]
    fn test_set_replaces_different_size() {
        let mut attrs = Flattrs::new();
        attrs.set(TEST_STRING, "short".to_string());
        attrs.set(TEST_STRING, "a much longer string".to_string());
        assert_eq!(
            attrs.get(TEST_STRING),
            Some("a much longer string".to_string())
        );
        assert_eq!(attrs.len(), 1);
    }

    #[test]
    fn test_contains_key() {
        let mut attrs = Flattrs::new();

        assert!(!attrs.contains_key(TEST_U64));
        attrs.set(TEST_U64, 42u64);
        assert!(attrs.contains_key(TEST_U64));
    }

    #[test]
    fn test_serde_roundtrip() {
        let mut attrs = Flattrs::new();
        attrs.set(TEST_U64, 42u64);
        attrs.set(TEST_STRING, "hello".to_string());

        let serialized = bincode::serialize(&attrs).expect("serialize");
        let deserialized: Flattrs = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(deserialized.get(TEST_U64), Some(42u64));
        assert_eq!(deserialized.get(TEST_STRING), Some("hello".to_string()));
        assert_eq!(deserialized.len(), 2);
    }

    #[test]
    fn test_wire_roundtrip() {
        let mut attrs = Flattrs::new();
        attrs.set(TEST_U64, 42u64);
        attrs.set(TEST_STRING, "hello".to_string());

        let wire = attrs.to_part();
        let received = Flattrs::from_part(wire);

        assert_eq!(received.get(TEST_U64), Some(42u64));
        assert_eq!(received.get(TEST_STRING), Some("hello".to_string()));
        assert_eq!(received.len(), 2);
    }

    #[test]
    fn test_multiple_keys() {
        let mut attrs = Flattrs::new();
        attrs.set(TEST_U64, 1u64);
        attrs.set(TEST_STRING, "two".to_string());
        attrs.set(TEST_BOOL, true);

        assert_eq!(attrs.get(TEST_U64), Some(1u64));
        assert_eq!(attrs.get(TEST_STRING), Some("two".to_string()));
        assert_eq!(attrs.get(TEST_BOOL), Some(true));
        assert_eq!(attrs.len(), 3);
    }

    #[test]
    fn test_is_empty() {
        let attrs = Flattrs::new();
        assert!(attrs.is_empty());

        let mut attrs2 = Flattrs::new();
        attrs2.set(TEST_U64, 42u64);
        assert!(!attrs2.is_empty());
    }

    #[test]
    fn test_display() {
        use crate::attrs::Attrs;

        // Empty displays as empty string
        let empty_flattrs = Flattrs::new();
        let empty_attrs = Attrs::new();
        assert_eq!(format!("{}", empty_flattrs), format!("{}", empty_attrs));
        assert_eq!(format!("{}", empty_flattrs), "");

        // Single entry - Flattrs and Attrs should display the same
        let mut single_flattrs = Flattrs::new();
        single_flattrs.set(TEST_U64, 42u64);
        let mut single_attrs = Attrs::new();
        single_attrs.set(TEST_U64, 42u64);
        assert_eq!(format!("{}", single_flattrs), format!("{}", single_attrs));
        assert_eq!(
            format!("{}", single_flattrs),
            "hyperactor_config::flattrs::tests::test_u64=42"
        );

        // Multiple entries - Flattrs maintains insertion order, Attrs uses HashMap order
        // So we only compare to the expected string for Flattrs
        let mut multi_flattrs = Flattrs::new();
        multi_flattrs.set(TEST_U64, 1u64);
        multi_flattrs.set(TEST_STRING, "hello".to_string());
        assert_eq!(
            format!("{}", multi_flattrs),
            "hyperactor_config::flattrs::tests::test_u64=1,hyperactor_config::flattrs::tests::test_string=hello"
        );
    }
}
