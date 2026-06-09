/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ops::Deref;

use bincode::Options;
use bytes::Bytes;
use bytes::BytesMut;
use bytes::buf::Reader as BufReader;
use bytes::buf::Writer as BufWriter;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use typeuri::Named;

use crate::UnsafeBufCellRef;
use crate::de;
use crate::ser;

/// Part represents a single part of a multipart message.
///
/// A part is backed by byte fragments, which permit zero-copy shared ownership
/// of the underlying buffers. It may also carry a typehash for framework-owned,
/// bincode-serialized values.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Part {
    typehash: Option<u64>,
    fragments: Vec<Bytes>,
}

impl Part {
    /// Consumes the part, returning its underlying byte fragments.
    pub fn into_fragments(self) -> Vec<Bytes> {
        self.fragments
    }

    /// Consumes the part, concatenating fragments if necessary into a single byte buffer.
    pub fn into_bytes(self) -> Bytes {
        match self.fragments.len() {
            0 => Bytes::new(),
            1 => self.fragments.into_iter().next().unwrap(),
            _ => {
                let total_len: usize = self.fragments.iter().map(|p| p.len()).sum();
                let mut result = BytesMut::with_capacity(total_len);
                for fragment in self.fragments {
                    result.extend_from_slice(&fragment);
                }
                result.freeze()
            }
        }
    }

    /// Get bytes as a reference, concatenating fragments if necessary.
    pub fn to_bytes(&self) -> Bytes {
        match self.fragments.len() {
            0 => Bytes::new(),
            1 => self.fragments.first().unwrap().clone(),
            _ => {
                let total_len: usize = self.fragments.iter().map(|p| p.len()).sum();
                let mut result = BytesMut::with_capacity(total_len);
                for fragment in &self.fragments {
                    result.extend_from_slice(fragment);
                }
                result.freeze()
            }
        }
    }

    /// Returns the total length in bytes.
    pub fn len(&self) -> usize {
        self.fragments
            .iter()
            .try_fold(0usize, |len, fragment| len.checked_add(fragment.len()))
            .expect("part length exceeds usize")
    }

    /// Returns the number of fragments
    pub fn num_fragments(&self) -> usize {
        self.fragments.len()
    }

    /// Returns whether the part is empty.
    pub fn is_empty(&self) -> bool {
        self.fragments.iter().all(|b| b.is_empty())
    }

    /// Returns the typehash carried by this part, if any.
    pub fn typehash(&self) -> Option<u64> {
        self.typehash
    }

    /// Returns whether this part contains a serialized `T`-typed value.
    pub fn is<T: Named>(&self) -> bool {
        self.typehash == Some(T::typehash())
    }

    /// Serialize a value into a typed part.
    pub fn serialize<T: Serialize + Named>(value: &T) -> crate::Result<Self> {
        Ok(Self {
            typehash: Some(T::typehash()),
            fragments: vec![Bytes::from(crate::options().serialize(value)?)],
        })
    }

    /// Deserialize this part into the provided type `T`.
    pub fn deserialized<T: DeserializeOwned + Named>(&self) -> crate::Result<T> {
        if !self.is::<T>() {
            return Err(crate::Error::TypeMismatch {
                expected: T::typename(),
                actual: self
                    .typehash
                    .map_or_else(|| "unknown".to_string(), |typehash| typehash.to_string()),
            });
        }
        self.deserialized_unchecked()
    }

    /// Deserialize this part without checking its typehash.
    pub fn deserialized_unchecked<T: DeserializeOwned>(&self) -> crate::Result<T> {
        Ok(crate::options().deserialize(&self.to_bytes())?)
    }

    /// Returns a part from byte fragments.
    pub fn from_fragments(fragments: Vec<Bytes>) -> Self {
        Self {
            typehash: None,
            fragments,
        }
    }

    /// Returns a part from an optional typehash and byte fragments.
    pub(crate) fn from_typehash_and_fragments(
        typehash: Option<u64>,
        fragments: Vec<Bytes>,
    ) -> Self {
        Self {
            typehash,
            fragments,
        }
    }
}

impl<T: Into<Bytes>> From<T> for Part {
    fn from(bytes: T) -> Self {
        Self {
            typehash: None,
            fragments: vec![bytes.into()],
        }
    }
}

impl Deref for Part {
    type Target = Vec<Bytes>;

    fn deref(&self) -> &Self::Target {
        &self.fragments
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
        (&this.typehash, &this.fragments).serialize(s)
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
        let (typehash, fragments) = <(Option<u64>, Vec<Bytes>)>::deserialize(deserializer)?;
        Ok(Self {
            typehash,
            fragments,
        })
    }
}

/// The deserializer type used by the underlying bincode codec. We capture this here to make sure
/// we consistently use the type, which is required to correctly specialize the multipart codec.
pub(crate) type BincodeDeserializer =
    de::bincode::Deserializer<bincode::de::read::IoReader<BufReader<Bytes>>, BincodeOptionsType>;

/// Specialized implementation for our multipart deserializer.
impl<'a> PartDeserializer<'_, &'a mut BincodeDeserializer> for Part {
    fn deserialize(deserializer: &'a mut BincodeDeserializer) -> Result<Self, bincode::Error> {
        deserializer.deserialize_part()
    }
}
