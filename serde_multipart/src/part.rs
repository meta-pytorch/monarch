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
pub struct Part(pub(crate) Bytes);

impl Part {
    /// Consumes the part, returning its underlying byte buffer.
    pub fn into_inner(self) -> Bytes {
        self.0
    }

    /// Returns a reference to the underlying byte buffer.
    pub fn to_bytes(&self) -> Bytes {
        self.0.clone()
    }
}

impl<T: Into<Bytes>> From<T> for Part {
    fn from(bytes: T) -> Self {
        Self(bytes.into())
    }
}

impl Deref for Part {
    type Target = Bytes;

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
        // Normal serializer: contiguous byte chunk, but requires copy.
        this.0.serialize(s)
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
        Ok(Part(Bytes::deserialize(deserializer)?))
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

/// A logically contiguous part that may be physically fragmented or contiguous.
///
/// During serialization, parts are extracted separately (allowing zero-copy from construction).
/// During deserialization, data arrives as a single contiguous `Part`.
///
/// Use this when:
/// - Construction creates multiple Parts (e.g., multiple pickle writes to a Buffer)
/// - Consumption needs contiguous bytes (e.g., unpickling requires contiguous buffer)
/// - Network read already gives contiguous bytes (no need to split and re-concat)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentedPart {
    /// Multiple fragments that need to be concatenated when accessed
    Fragmented(Vec<Part>),
    /// Already contiguous data (typically from deserialization)
    Contiguous(Part),
}

impl Default for FragmentedPart {
    fn default() -> Self {
        Self::Contiguous(Part::default())
    }
}

impl FragmentedPart {
    pub fn new(parts: Vec<Part>) -> Self {
        if parts.len() == 1 {
            Self::Contiguous(parts.into_iter().next().unwrap())
        } else {
            Self::Fragmented(parts)
        }
    }

    pub fn into_parts(self) -> Vec<Part> {
        match self {
            Self::Fragmented(parts) => parts,
            Self::Contiguous(part) => vec![part],
        }
    }

    /// Convert into bytes, concatenating fragments if necessary.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Contiguous(part) => part.into_inner(),
            Self::Fragmented(parts) => {
                let total_len: usize = parts.iter().map(|p| p.len()).sum();
                let mut result = BytesMut::with_capacity(total_len);
                for part in parts {
                    result.extend_from_slice(&part.to_bytes());
                }
                result.freeze()
            }
        }
    }

    /// Get bytes as a reference, concatenating fragments if necessary.
    pub fn as_bytes(&self) -> Bytes {
        match self {
            Self::Contiguous(part) => part.to_bytes(),
            Self::Fragmented(parts) => {
                let total_len: usize = parts.iter().map(|p| p.len()).sum();
                let mut result = BytesMut::with_capacity(total_len);
                for part in parts {
                    result.extend_from_slice(&part.to_bytes());
                }
                result.freeze()
            }
        }
    }

    pub fn as_slice(&self) -> &[Part] {
        match self {
            Self::Fragmented(parts) => parts.as_slice(),
            Self::Contiguous(part) => std::slice::from_ref(part),
        }
    }

    /// Returns the total length in bytes of the fragmented part.
    /// For contiguous parts, this is just the part length.
    /// For fragmented parts, this is the sum of all fragment lengths.
    pub fn len(&self) -> usize {
        match self {
            Self::Contiguous(part) => part.len(),
            Self::Fragmented(parts) => parts.iter().map(|p| p.len()).sum(),
        }
    }

    /// Returns whether the fragmented part is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Serialization trait for FragmentedPart (similar to PartSerializer)
trait FragmentedPartSerializer<S: serde::Serializer> {
    fn serialize(parts: &FragmentedPart, s: S) -> Result<S::Ok, S::Error>;
}

/// Default: serialize as Vec<Part>
impl<S: serde::Serializer> FragmentedPartSerializer<S> for FragmentedPart {
    default fn serialize(part: &FragmentedPart, s: S) -> Result<S::Ok, S::Error> {
        match part {
            FragmentedPart::Fragmented(parts) => parts.serialize(s),
            FragmentedPart::Contiguous(part) => vec![part.clone()].serialize(s),
        }
    }
}

/// Specialized for our BincodeSerializer
impl<'a> FragmentedPartSerializer<&'a mut BincodeSerializer> for FragmentedPart {
    fn serialize(
        parts: &FragmentedPart,
        s: &'a mut BincodeSerializer,
    ) -> Result<(), bincode::Error> {
        // Tell the serializer to extract this as a fragmented part
        s.serialize_fragmented_part(parts);
        // Serialize as empty Vec in the body (parts are extracted)
        Vec::<Part>::new().serialize(s)
    }
}

impl Serialize for FragmentedPart {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <FragmentedPart as FragmentedPartSerializer<S>>::serialize(self, serializer)
    }
}

/// Deserialization trait for FragmentedPart
trait FragmentedPartDeserializer<'de, D: serde::Deserializer<'de>>: Sized {
    fn deserialize(d: D) -> Result<Self, D::Error>;
}

/// Default: deserialize as Vec<Part>
impl<'de, D: serde::Deserializer<'de>> FragmentedPartDeserializer<'de, D> for FragmentedPart {
    default fn deserialize(deserializer: D) -> Result<Self, D::Error> {
        let parts = Vec::<Part>::deserialize(deserializer)?;
        Ok(Self::new(parts))
    }
}

/// Specialized for our BincodeDeserializer
impl<'de, 'a> FragmentedPartDeserializer<'de, &'a mut BincodeDeserializer> for FragmentedPart {
    fn deserialize(deserializer: &'a mut BincodeDeserializer) -> Result<Self, bincode::Error> {
        // Read the Vec (should be empty from serialization)
        let _empty: Vec<Part> = Vec::deserialize(&mut *deserializer)?;
        // Pull the actual fragmented part from the deserializer
        deserializer.deserialize_fragmented_part()
    }
}

impl<'de> Deserialize<'de> for FragmentedPart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <FragmentedPart as FragmentedPartDeserializer<'de, D>>::deserialize(deserializer)
    }
}
