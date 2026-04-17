/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! GPU-independent types for NCCL-based communication.
//!
//! These types are used in the message protocol between tensor workers and must
//! be available even in CPU-only builds where `nccl-sys` is not compiled.

use std::fmt;
use std::fmt::Write;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeSeq;

/// Rust version of `ncclRedOp_t`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ReduceOp {
    Sum = 0,
    Prod = 1,
    Max = 2,
    Min = 3,
    Avg = 4,
}

/// Wire-compatible representation of `ncclUniqueId`.
///
/// This is a 128-byte opaque identifier used to bootstrap NCCL communicators.
/// The struct layout and serialization format match `ncclUniqueId` from `nccl-sys`
/// exactly, so that messages are wire-compatible regardless of whether the sender
/// or receiver was built with GPU support.
#[repr(C)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct NcclUniqueId {
    #[serde(
        serialize_with = "serialize_array",
        deserialize_with = "deserialize_array"
    )]
    pub internal: [::std::os::raw::c_char; 128usize],
}

fn deserialize_array<'de, D>(deserializer: D) -> Result<[::std::os::raw::c_char; 128], D::Error>
where
    D: Deserializer<'de>,
{
    let vec: Vec<::std::os::raw::c_char> = Deserialize::deserialize(deserializer)?;
    vec.try_into().map_err(|v: Vec<::std::os::raw::c_char>| {
        serde::de::Error::invalid_length(v.len(), &"expected an array of length 128")
    })
}

fn serialize_array<S>(
    array: &[::std::os::raw::c_char; 128],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(128))?;
    for element in array {
        seq.serialize_element(element)?;
    }
    seq.end()
}

/// Binding for `ncclUniqueId`.
///
/// Wraps the raw 128-byte NCCL unique identifier. On GPU builds, this can be
/// created via `nccl-sys`; on CPU builds, it can only be deserialized from a
/// message sent by a GPU-capable peer.
#[derive(Clone, Serialize, Deserialize)]
pub struct UniqueId {
    inner: NcclUniqueId,
}

impl fmt::Debug for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniqueId")
            .field(
                "inner",
                &format_args!(
                    "{}",
                    self.inner
                        .internal
                        .iter()
                        .fold(String::new(), |mut output, b| {
                            let _ = write!(output, "{:02x}", b);
                            output
                        })
                ),
            )
            .finish()
    }
}

impl UniqueId {
    /// Create a `UniqueId` from raw bytes.
    pub fn from_internal(internal: [::std::os::raw::c_char; 128]) -> Self {
        Self {
            inner: NcclUniqueId { internal },
        }
    }

    /// Access the raw bytes.
    pub fn internal(&self) -> &[::std::os::raw::c_char; 128] {
        &self.inner.internal
    }

    /// Access the inner `NcclUniqueId`.
    pub fn as_nccl_unique_id(&self) -> &NcclUniqueId {
        &self.inner
    }

    /// Consume and return the inner `NcclUniqueId`.
    pub fn into_nccl_unique_id(self) -> NcclUniqueId {
        self.inner
    }
}
