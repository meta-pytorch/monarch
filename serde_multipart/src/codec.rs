/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use typeuri::Named;

use crate::Part;

/// Converts a framework type to and from a typed multipart part.
pub trait PartCodec: Sized + Named {
    /// The bincode representation stored in typed parts.
    type Repr: Serialize + DeserializeOwned;

    /// Convert this value to its typed part representation.
    fn to_repr(&self) -> crate::Result<Self::Repr>;

    /// Rebuild this value from its typed part representation.
    fn from_repr(repr: Self::Repr) -> crate::Result<Self>;

    /// Convert this value to a typed part.
    fn to_part(&self) -> crate::Result<Part> {
        Part::serialize_as::<Self, _>(&self.to_repr()?)
    }

    /// Rebuild this value from a typed part.
    fn from_part(part: Part) -> crate::Result<Self> {
        let repr = part.deserialized_as::<Self, Self::Repr>()?;
        Self::from_repr(repr)
    }
}

/// Serialize a value that implements [`PartCodec`].
pub fn serialize_part_codec<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: PartCodec,
    S: serde::Serializer,
{
    <T as PartCodecSerializer<S>>::serialize(value, serializer)
}

/// Deserialize a value that implements [`PartCodec`].
pub fn deserialize_part_codec<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: PartCodec,
    D: serde::Deserializer<'de>,
{
    <T as PartCodecDeserializer<'de, D>>::deserialize(deserializer)
}

trait PartCodecSerializer<S: serde::Serializer> {
    fn serialize(this: &Self, serializer: S) -> Result<S::Ok, S::Error>;
}

impl<T, S> PartCodecSerializer<S> for T
where
    T: PartCodec,
    S: serde::Serializer,
{
    default fn serialize(this: &Self, serializer: S) -> Result<S::Ok, S::Error> {
        this.to_repr()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

trait PartCodecDeserializer<'de, D: serde::Deserializer<'de>>: Sized {
    fn deserialize(deserializer: D) -> Result<Self, D::Error>;
}

impl<'de, T, D> PartCodecDeserializer<'de, D> for T
where
    T: PartCodec,
    D: serde::Deserializer<'de>,
{
    default fn deserialize(deserializer: D) -> Result<Self, D::Error> {
        let repr = T::Repr::deserialize(deserializer)?;
        T::from_repr(repr).map_err(serde::de::Error::custom)
    }
}
