/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::Ord;
use std::cmp::PartialOrd;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use hyperactor_config::AttrValue;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::Name;

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    Named
)]
pub struct ProcMeshId(pub String);

/// Actor Mesh ID.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    Named,
    AttrValue
)]
pub struct ActorMeshId(pub Name);

impl fmt::Display for ActorMeshId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ActorMeshId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ActorMeshId(Name::from_str(s)?))
    }
}
