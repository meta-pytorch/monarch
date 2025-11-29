/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ops::Deref;

use bytes::Bytes;
use bytes::BytesMut;
use bytes::buf::Reader as BufReader;
use bytes::buf::Writer as BufWriter;
use serde::Deserialize;
use serde::Serialize;

use crate::UnsafeBufCellRef;
use crate::de;
use crate::ser;

/// Part represents a single part of a multipart message. Its type is simple:
/// it is just a newtype of the byte buffer [`Bytes`], which permits zero copy
/// shared ownership of the underlying buffers. Part itself provides a customized
/// serialization implementation that is specialized for the multipart codecs in
/// this crate, skipping copying the bytes whenever possible.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Part(pub(crate) Vec<Bytes>);

impl Part {
    /// Consumes the part, returning its underlying byte buffers.
    pub fn into_inner(self) -> Vec<Bytes> {
        self.0
    }

    /// Consumes the part, concatenating fragments if necessary into a single byte buffer.
    pub fn into_bytes(self) -> Bytes {
        match self.0.len() {
            0 => Bytes::new(),
            1 => self.0.into_iter().next().unwrap(),
            _ => {
                let total_len: usize = self.0.iter().map(|p| p.len()).sum();
                let mut result = BytesMut::with_capacity(total_len);
                for fragment in self.0 {
                    result.extend_from_slice(&fragment);
                }
                result.freeze()
            }
        }
    }

    /// Get bytes as a reference, concatenating fragments if necessary.
    pub fn to_bytes(&self) -> Bytes {
        match self.0.len() {
            0 => Bytes::new(),
            1 => self.0.first().unwrap().clone(),
            _ => {
                let total_len: usize = self.0.iter().map(|p| p.len()).sum();
                let mut result = BytesMut::with_capacity(total_len);
                for fragment in &self.0 {
                    result.extend_from_slice(fragment);
                }
                result.freeze()
            }
        }
    }

    /// Returns the total length in bytes.
    pub fn len(&self) -> usize {
        self.0.iter().map(|b| b.len()).sum()
    }

    /// Returns the number of fragments
    pub fn num_fragments(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the part is empty.
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|b| b.is_empty())
    }
}

impl<T: Into<Bytes>> From<T> for Part {
    fn from(bytes: T) -> Self {
        Self(vec![bytes.into()])
    }
}

impl Deref for Part {
    type Target = Vec<Bytes>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for Part {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        <Part as PartSerializer<S>>::serialize(self, s)
    }
}

impl<'de> Deserialize<'de> for Part {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        <Part as PartDeserializer<'de, D>>::deserialize(d)
    }
}

/// PartSerializer is the trait that selects serialization strategy based on the
/// the serializer's type.
pub trait PartSerializer<S: serde::Serializer> {
    fn serialize(this: &Part, s: S) -> Result<S::Ok, S::Error>;
}

/// By default, we use the underlying byte serializer, which copies the underlying bytes
/// into the serialization buffer.
impl<S: serde::Serializer> PartSerializer<S> for Part {
    default fn serialize(this: &Part, s: S) -> Result<S::Ok, S::Error> {
        // Normal serializer: concatenate into contiguous byte chunk (requires copy).
        this.to_bytes().serialize(s)
    }
}

/// The options type used by the underlying bincode codec. We capture this here to make sure
/// we consistently use the type, which is required to correctly specialize the multipart codec.
pub(crate) type BincodeOptionsType = bincode::config::WithOtherTrailing<
    bincode::config::WithOtherIntEncoding<bincode::DefaultOptions, bincode::config::FixintEncoding>,
    bincode::config::AllowTrailing,
>;

/// The serializer type used by the underlying bincode codec. We capture this here to make sure
/// we consistently use the type, which is required to correctly specialize the multipart codec.
pub(crate) type BincodeSerializer =
    ser::bincode::Serializer<BufWriter<UnsafeBufCellRef>, BincodeOptionsType>;

/// Specialized implementaiton for our multipart serializer.
impl<'a> PartSerializer<&'a mut BincodeSerializer> for Part {
    fn serialize(this: &Part, s: &'a mut BincodeSerializer) -> Result<(), bincode::Error> {
        s.serialize_part(this);
        Ok(())
    }
}

/// PartDeserializer is the trait that selects serialization strategy based on the
/// the deserializer's type.
trait PartDeserializer<'de, S: serde::Deserializer<'de>>: Sized {
    fn deserialize(this: S) -> Result<Self, S::Error>;
}

/// By default, we use the underlying byte deserializer, which copies the serialized bytes
/// into the value directly.
impl<'de, D: serde::Deserializer<'de>> PartDeserializer<'de, D> for Part {
    default fn deserialize(deserializer: D) -> Result<Self, D::Error> {
        Ok(Part(vec![Bytes::deserialize(deserializer)?]))
    }
}

/// The deserializer type used by the underlying bincode codec. We capture this here to make sure
/// we consistently use the type, which is required to correctly specialize the multipart codec.
pub(crate) type BincodeDeserializer =
    de::bincode::Deserializer<bincode::de::read::IoReader<BufReader<Bytes>>, BincodeOptionsType>;

/// Specialized implementation for our multipart deserializer.
impl<'de, 'a> PartDeserializer<'de, &'a mut BincodeDeserializer> for Part {
    fn deserialize(deserializer: &'a mut BincodeDeserializer) -> Result<Self, bincode::Error> {
        deserializer.deserialize_part()
    }
}
