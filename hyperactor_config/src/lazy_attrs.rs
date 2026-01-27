/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Lazy attribute dictionary that defers deserialization until access.
//!
//! `LazyAttrs` stores attributes as raw serialized bytes on the wire,
//! only deserializing when the attributes are actually accessed. This
//! significantly reduces overhead for message passthrough scenarios
//! where intermediate hops don't need to inspect headers.

use std::fmt;
use std::sync::OnceLock;

use bytes::Bytes;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::attrs::AttrValue;
use crate::attrs::Attrs;
use crate::attrs::Key;

/// A lazy wrapper around `Attrs` that defers deserialization.
///
/// On the wire, `LazyAttrs` is serialized as length-prefixed bytes
/// containing bincode-encoded `Attrs`. When received, the bytes are
/// stored as-is until `.get()`, `.get_mut()`, or `.attrs()` is called,
/// at which point deserialization occurs.
///
/// This optimization eliminates deserialization overhead for message
/// routing scenarios where headers are not inspected.
///
/// Thread-safe: Uses `OnceLock` for lazy initialization, allowing
/// concurrent reads without data races.
pub struct LazyAttrs {
    /// Raw serialized bytes. Present when created from bytes, dropped on mutation.
    /// When present and deserialized is empty, this is used for passthrough serialization.
    raw: Option<Bytes>,

    /// Lazily deserialized attributes. Populated on first access.
    /// Thread-safe lazy initialization via OnceLock.
    deserialized: OnceLock<Attrs>,
}

impl LazyAttrs {
    /// Create a new empty lazy attributes container.
    pub fn new() -> Self {
        Self {
            raw: None,
            deserialized: OnceLock::from(Attrs::new()),
        }
    }

    /// Create from an existing `Attrs`.
    pub fn from_attrs(attrs: Attrs) -> Self {
        Self {
            raw: None,
            deserialized: OnceLock::from(attrs),
        }
    }

    /// Create from raw serialized bytes.
    /// Accepts any type that can be converted to `Bytes` (e.g., `Vec<u8>`, `Bytes`).
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        Self {
            raw: Some(bytes.into()),
            deserialized: OnceLock::new(),
        }
    }

    /// Helper to deserialize bytes into Attrs.
    #[inline]
    fn deserialize_bytes(bytes: &[u8]) -> Attrs {
        bincode::deserialize(bytes).unwrap_or_else(|e| {
            tracing::error!("failed to deserialize LazyAttrs: {}", e);
            Attrs::new()
        })
    }

    /// Ensure the state is deserialized, returning a reference to the Attrs.
    #[inline]
    fn ensure_deserialized(&self) -> &Attrs {
        self.deserialized.get_or_init(|| {
            self.raw
                .as_ref()
                .map(|bytes| Self::deserialize_bytes(bytes))
                .unwrap_or_else(Attrs::new)
        })
    }

    /// Ensure the state is deserialized for mutation.
    /// Drops raw bytes since they're now stale.
    fn ensure_deserialized_mut(&mut self) -> &mut Attrs {
        if self.deserialized.get().is_none() {
            let attrs = self
                .raw
                .as_ref()
                .map(|bytes| Self::deserialize_bytes(bytes))
                .unwrap_or_else(Attrs::new);
            let _ = self.deserialized.set(attrs);
        }
        // Drop raw bytes since mutation makes them stale
        self.raw = None;
        self.deserialized
            .get_mut()
            .expect("deserialized is populated above")
    }

    /// Get a value for the given key, deserializing if necessary.
    #[inline]
    pub fn get<T: AttrValue>(&self, key: Key<T>) -> Option<&T> {
        self.ensure_deserialized().get(key)
    }

    /// Get a mutable reference to a value, deserializing if necessary.
    pub fn get_mut<T: AttrValue>(&mut self, key: Key<T>) -> Option<&mut T> {
        self.ensure_deserialized_mut().get_mut(key)
    }

    /// Set a value for the given key, deserializing if necessary.
    pub fn set<T: AttrValue>(&mut self, key: Key<T>, value: T) {
        self.ensure_deserialized_mut().set(key, value);
    }

    /// Check if a key exists, deserializing if necessary.
    pub fn contains_key<T: AttrValue>(&self, key: Key<T>) -> bool {
        self.ensure_deserialized().contains_key(key)
    }

    /// Get the underlying Attrs, deserializing if necessary.
    #[inline]
    pub fn attrs(&self) -> &Attrs {
        self.ensure_deserialized()
    }

    /// Get a mutable reference to the underlying Attrs.
    pub fn attrs_mut(&mut self) -> &mut Attrs {
        self.ensure_deserialized_mut()
    }

    /// Convert to owned Attrs, consuming self.
    pub fn into_attrs(self) -> Attrs {
        self.deserialized
            .into_inner()
            .or_else(|| {
                self.raw
                    .as_ref()
                    .map(|bytes| Self::deserialize_bytes(bytes))
            })
            .unwrap_or_else(Attrs::new)
    }

    /// Returns true if the attrs are still in raw (unserialized) form.
    #[inline]
    pub fn is_raw(&self) -> bool {
        self.raw.is_some() && self.deserialized.get().is_none()
    }

    /// Returns the number of entries, deserializing if necessary.
    pub fn len(&self) -> usize {
        self.ensure_deserialized().len()
    }

    /// Returns true if empty, deserializing if necessary.
    pub fn is_empty(&self) -> bool {
        self.ensure_deserialized().is_empty()
    }
}

impl Default for LazyAttrs {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for LazyAttrs {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            deserialized: self
                .deserialized
                .get()
                .map(|attrs| OnceLock::from(attrs.clone()))
                .unwrap_or_default(),
        }
    }
}

impl fmt::Debug for LazyAttrs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.raw.as_ref(), self.deserialized.get()) {
            (Some(bytes), None) => f
                .debug_struct("LazyAttrs")
                .field("state", &format!("Raw({} bytes)", bytes.len()))
                .finish(),
            (_, Some(attrs)) => f
                .debug_struct("LazyAttrs")
                .field("state", &"Deserialized")
                .field("attrs", attrs)
                .finish(),
            (None, None) => f
                .debug_struct("LazyAttrs")
                .field("state", &"Empty")
                .finish(),
        }
    }
}

impl fmt::Display for LazyAttrs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ensure_deserialized())
    }
}

impl From<Attrs> for LazyAttrs {
    fn from(attrs: Attrs) -> Self {
        Self::from_attrs(attrs)
    }
}

impl Serialize for LazyAttrs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(raw) = &self.raw {
            if self.deserialized.get().is_none() {
                return serializer.serialize_bytes(raw);
            }
        }

        let attrs = self.ensure_deserialized();
        let bytes = bincode::serialize(attrs).map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for LazyAttrs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Ok(Self::from_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::attrs::declare_attrs;

    declare_attrs! {
        attr LAZY_TEST_TIMEOUT: Duration;
        attr LAZY_TEST_NAME: String;
    }

    #[test]
    fn test_lazy_attrs_passthrough() {
        // Create attrs and convert to lazy
        let mut attrs = Attrs::new();
        attrs.set(LAZY_TEST_TIMEOUT, Duration::from_secs(30));
        attrs.set(LAZY_TEST_NAME, "test".to_string());

        let lazy = LazyAttrs::from_attrs(attrs);

        // Serialize to bytes
        let bytes = bincode::serialize(&lazy).unwrap();

        // Deserialize - should be in Raw state
        let lazy2: LazyAttrs = bincode::deserialize(&bytes).unwrap();
        assert!(lazy2.is_raw());

        // Serialize again without accessing - should pass through
        let bytes2 = bincode::serialize(&lazy2).unwrap();
        assert_eq!(bytes, bytes2);

        // Now access - should deserialize
        assert_eq!(lazy2.get(LAZY_TEST_TIMEOUT), Some(&Duration::from_secs(30)));
        assert!(!lazy2.is_raw());
    }

    #[test]
    fn test_lazy_attrs_mutation() {
        let mut lazy = LazyAttrs::new();
        assert!(lazy.is_empty());

        lazy.set(LAZY_TEST_NAME, "hello".to_string());
        assert_eq!(lazy.get(LAZY_TEST_NAME), Some(&"hello".to_string()));

        // Round-trip to get into Raw state
        let bytes = bincode::serialize(&lazy).unwrap();
        let mut lazy2: LazyAttrs = bincode::deserialize(&bytes).unwrap();
        assert!(lazy2.is_raw());

        // Mutate while in Raw state - should trigger deserialization
        lazy2.set(LAZY_TEST_NAME, "world".to_string());
        assert!(!lazy2.is_raw()); // Should be deserialized now
        assert_eq!(lazy2.get(LAZY_TEST_NAME), Some(&"world".to_string()));

        // Serialize again and verify the mutation persisted
        let bytes3 = bincode::serialize(&lazy2).unwrap();
        let lazy3: LazyAttrs = bincode::deserialize(&bytes3).unwrap();
        assert_eq!(lazy3.get(LAZY_TEST_NAME), Some(&"world".to_string()));
    }

    #[test]
    fn test_lazy_attrs_clone() {
        let mut attrs = Attrs::new();
        attrs.set(LAZY_TEST_TIMEOUT, Duration::from_secs(60));
        let lazy = LazyAttrs::from_attrs(attrs);

        // Clone while deserialized
        let lazy_clone = lazy.clone();
        assert!(!lazy_clone.is_raw());
        assert_eq!(
            lazy_clone.get(LAZY_TEST_TIMEOUT),
            Some(&Duration::from_secs(60))
        );

        // Serialize, deserialize, then clone while raw
        let bytes = bincode::serialize(&lazy).unwrap();
        let lazy_raw: LazyAttrs = bincode::deserialize(&bytes).unwrap();
        assert!(lazy_raw.is_raw());

        let lazy_raw_clone = lazy_raw.clone();
        assert!(lazy_raw_clone.is_raw()); // Clone preserves raw state
        assert_eq!(
            lazy_raw_clone.get(LAZY_TEST_TIMEOUT),
            Some(&Duration::from_secs(60))
        );
    }
}
