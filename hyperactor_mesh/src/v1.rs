/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A temporary holding space for APIv1 of the Hyperactor Mesh.
//! This will be moved down to the base module when we graduate
//! the APIs and fully deprecate the "v0" APIs.

pub mod host_mesh;
pub mod proc_mesh;

use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::shortuuid::ShortUuid;
use crate::v1::host_mesh::HostMeshRefParseError;

/// Errors that occur during mesh operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid mesh ref: expected {expected} ranks, but contains {actual} ranks")]
    InvalidRankCardinality { expected: usize, actual: usize },

    #[error(transparent)]
    NameParseError(#[from] NameParseError),

    #[error(transparent)]
    HostMeshRefParseError(#[from] HostMeshRefParseError),
}

/// The type of result used in `hyperactor_mesh::v1`.
pub type Result<T> = std::result::Result<T, Error>;

/// Names are used to identify objects in the system. They have a user-provided name,
/// and a unique UUID.
///
/// Names have a concrete syntax--`{name}-{uuid}`--printed by `Display` and parsed by `FromStr`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Name(pub String, pub ShortUuid);

impl Name {
    /// Create a new `Name` from a user-provided base name.
    pub fn new(name: impl Into<String>) -> Self {
        let mut name = name.into();
        if name.is_empty() {
            name = "unnamed".to_string();
        }
        let uuid = ShortUuid::generate();
        Self(name, uuid)
    }

    /// The name portion of this `Name`.
    pub fn name(&self) -> &str {
        &self.0
    }

    /// The UUID portion of this `Name`.
    pub fn uuid(&self) -> &ShortUuid {
        &self.1
    }
}

/// Errors that occur when parsing names.
#[derive(thiserror::Error, Debug)]
pub enum NameParseError {
    #[error("invalid name: missing name")]
    MissingName,

    #[error("invalid name: missing uuid")]
    MissingUuid,

    #[error(transparent)]
    InvalidUuid(#[from] <ShortUuid as FromStr>::Err),

    #[error("invalid name: missing separator")]
    MissingSeparator,
}

impl FromStr for Name {
    type Err = NameParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (name, uuid) = s.split_once('-').ok_or(NameParseError::MissingSeparator)?;
        if name.is_empty() {
            return Err(NameParseError::MissingName);
        }
        if uuid.is_empty() {
            return Err(NameParseError::MissingName);
        }

        let name = name.to_string();
        let uuid = uuid.parse()?;
        Ok(Name(name, uuid))
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-", self.name())?;
        self.uuid().format(f, true /*raw*/)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name_unique() {
        assert_ne!(Name::new("foo"), Name::new("foo"));
        let name = Name::new("foo");
        assert_eq!(name, name);
    }

    #[test]
    fn test_name_roundtrip() {
        let name = Name::new("foo");
        assert_eq!(name, Name::from_str(&name.to_string()).unwrap());
    }
}
