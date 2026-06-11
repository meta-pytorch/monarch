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
use crate::part::BincodeDeserializer;
use crate::part::BincodeSerializer;

/// Converts a framework type to and from a typed multipart part.
pub trait PartCodec: Sized + Named
where
    for<'a> Self::Repr: std::convert::TryFrom<&'a Self, Error = crate::Error>,
    Self: std::convert::TryFrom<Self::Repr, Error = crate::Error>,
{
    /// The bincode representation stored in typed parts.
    type Repr: Serialize + DeserializeOwned;

    /// Convert this value to its typed part representation.
    fn to_repr(&self) -> crate::Result<Self::Repr> {
        Self::Repr::try_from(self)
    }

    /// Rebuild this value from its typed part representation.
    fn from_repr(repr: Self::Repr) -> crate::Result<Self> {
        Self::try_from(repr)
    }

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

impl<'a, T> PartCodecSerializer<&'a mut BincodeSerializer> for T
where
    T: PartCodec,
{
    fn serialize(this: &Self, serializer: &'a mut BincodeSerializer) -> Result<(), bincode::Error> {
        let part = this
            .to_part()
            .map_err(<bincode::Error as serde::ser::Error>::custom)?;
        serializer.serialize_part(&part);
        Ok(())
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

impl<'a, T> PartCodecDeserializer<'_, &'a mut BincodeDeserializer> for T
where
    T: PartCodec,
{
    fn deserialize(deserializer: &'a mut BincodeDeserializer) -> Result<Self, bincode::Error> {
        let part = deserializer.deserialize_part()?;
        T::from_part(part).map_err(<bincode::Error as serde::de::Error>::custom)
    }
}

/// Implement [`PartCodec`], [`serde::Serialize`], and [`serde::Deserialize`].
#[macro_export]
macro_rules! part_codec {
    (
        impl <$($impl_generics:ident),+> $ty:ty
        {
            type Repr = $repr:ty;
        }
    ) => {
        $crate::part_codec! {
            @expand
            [<$($impl_generics),+>]
            [<'de, $($impl_generics),+>]
            [$ty]
            [$repr]
        }
    };

    (
        impl $ty:ty
        {
            type Repr = $repr:ty;
        }
    ) => {
        $crate::part_codec! {
            @expand
            []
            [<'de>]
            [$ty]
            [$repr]
        }
    };

    (
        @expand
        [$($impl_generics:tt)*]
        [$($de_impl_generics:tt)*]
        [$ty:ty]
        [$repr:ty]
    ) => {
        impl $($impl_generics)* $crate::PartCodec for $ty
        where
            $ty: typeuri::Named,
            for<'a> $repr: std::convert::TryFrom<&'a $ty, Error = $crate::Error>,
            $ty: std::convert::TryFrom<$repr, Error = $crate::Error>,
        {
            type Repr = $repr;
        }

        impl $($impl_generics)* serde::Serialize for $ty
        where
            $ty: $crate::PartCodec,
        {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                $crate::serialize_part_codec(self, serializer)
            }
        }

        impl $($de_impl_generics)* serde::Deserialize<'de> for $ty
        where
            $ty: $crate::PartCodec,
        {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                $crate::deserialize_part_codec(deserializer)
            }
        }
    };
}
