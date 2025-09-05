/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;
use std::sync::Arc;

use hyperactor::Named;
use hyperactor::channel::ChannelAddr;
use ndslice::Region;
use ndslice::view;
use ndslice::view::RegionParseError;
use serde::Deserialize;
use serde::Serialize;

use crate::v1;

/// A reference to a single host.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct HostRef(ChannelAddr);

impl std::fmt::Display for HostRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for HostRef {
    type Err = <ChannelAddr as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(HostRef(ChannelAddr::from_str(s)?))
    }
}

/// A reference to a mesh of hosts. Logically this is a data structure that
/// contains a set of ranked hosts organized into a [`Region`]. HostMeshRefs
/// can be sliced to produce new HostMeshRefs that contain a subset of the
/// hosts in the original mesh.
///
/// HostMeshRefs have a concrete syntax, implemented by its `Display` and `FromStr`
/// implementations.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct HostMeshRef {
    region: Region,
    ranks: Arc<Vec<HostRef>>,
}

impl HostMeshRef {
    /// Create a new (raw) HostMeshRef from the provided region and associated
    /// ranks, which must match in cardinality.
    fn new(region: Region, ranks: Vec<HostRef>) -> v1::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(v1::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        Ok(Self {
            region,
            ranks: Arc::new(ranks),
        })
    }
}

impl view::Ranked for HostMeshRef {
    type Item = HostRef;

    fn region(&self) -> &Region {
        &self.region
    }

    fn ranks(&self) -> &[HostRef] {
        &self.ranks
    }

    fn sliced<'a>(&self, region: Region, nodes: impl Iterator<Item = &'a HostRef>) -> Self {
        Self::new(region, nodes.cloned().collect()).unwrap()
    }
}

impl std::fmt::Display for HostMeshRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (rank, host) in self.ranks.iter().enumerate() {
            if rank > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", host)?;
        }
        write!(f, "@{}", self.region)
    }
}

/// The type of error occuring during `HostMeshRef` parsing.
#[derive(thiserror::Error, Debug)]
pub enum HostMeshRefParseError {
    #[error(transparent)]
    RegionParseError(#[from] RegionParseError),

    #[error("invalid host mesh ref: missing region")]
    MissingRegion,

    #[error(transparent)]
    InvalidHostMeshRef(#[from] Box<v1::Error>),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<v1::Error> for HostMeshRefParseError {
    fn from(err: v1::Error) -> Self {
        Self::InvalidHostMeshRef(Box::new(err))
    }
}

impl FromStr for HostMeshRef {
    type Err = HostMeshRefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hosts, region) = s
            .split_once('@')
            .ok_or(HostMeshRefParseError::MissingRegion)?;
        let hosts = hosts
            .split(',')
            .map(|host| host.trim())
            .map(|host| host.parse::<HostRef>())
            .collect::<Result<Vec<_>, _>>()?;
        let region = region.parse()?;
        Ok(HostMeshRef::new(region, hosts)?)
    }
}

#[cfg(test)]
mod tests {
    use ndslice::ViewExt;
    use ndslice::extent;

    use super::*;

    #[test]
    fn test_host_mesh_subset() {
        let hosts: HostMeshRef = "local:1,local:2,local:3,local:4@replica=2/2,host=2/1"
            .parse()
            .unwrap();
        assert_eq!(
            hosts.range("replica", 1).unwrap().to_string(),
            "local:3,local:4@2+replica=1/2,host=2/1"
        );
    }

    #[test]
    fn test_host_mesh_ref_parse_roundtrip() {
        let host_mesh_ref = HostMeshRef::new(
            extent!(replica = 2, host = 2).into(),
            vec![
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
            ],
        )
        .unwrap();

        assert_eq!(
            host_mesh_ref.to_string().parse::<HostMeshRef>().unwrap(),
            host_mesh_ref
        );
    }
}
