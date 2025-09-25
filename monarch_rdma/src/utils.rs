/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::hash::Hash;
use std::hash::Hasher;

use serde::Deserialize;
use serde::Serialize;

/// A thin wrapper around a raw pointer that can be serialized/deserialized
/// by treating the pointer as a usize integer.
#[derive(Debug, Clone, Copy)]
pub struct SerializablePointer<T>(*mut T);

impl<T> PartialEq for SerializablePointer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> Eq for SerializablePointer<T> {}

impl<T> Hash for SerializablePointer<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.0 as usize).hash(state);
    }
}

impl<T> SerializablePointer<T> {
    pub fn new(ptr: *mut T) -> Self {
        Self(ptr)
    }

    pub fn as_ptr(&self) -> *mut T {
        self.0
    }

    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

impl<T> From<*mut T> for SerializablePointer<T> {
    fn from(ptr: *mut T) -> Self {
        Self::new(ptr)
    }
}

impl<T> From<SerializablePointer<T>> for *mut T {
    fn from(val: SerializablePointer<T>) -> Self {
        val.0
    }
}

impl<T> From<usize> for SerializablePointer<T> {
    fn from(addr: usize) -> Self {
        Self::new(addr as *mut T)
    }
}

impl<T> From<u64> for SerializablePointer<T> {
    fn from(addr: u64) -> Self {
        Self::new(addr as usize as *mut T)
    }
}

impl<T> Serialize for SerializablePointer<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.0 as usize as u64)
    }
}

impl<'de, T> Deserialize<'de> for SerializablePointer<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ptr_as_u64 = u64::deserialize(deserializer)?;
        Ok(Self(ptr_as_u64 as usize as *mut T))
    }
}

// SAFETY: SerializablePointer can be safely sent between threads since it only contains
// a pointer value. However, this doesn't provide any safety guarantees about the
// validity or thread-safety of the pointed-to data.
unsafe impl<T> Send for SerializablePointer<T> {}
unsafe impl<T> Sync for SerializablePointer<T> {}
