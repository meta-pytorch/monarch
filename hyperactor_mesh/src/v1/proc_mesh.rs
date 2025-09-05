/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use hyperactor::Named;
use hyperactor::ProcId;
use ndslice::view;
use ndslice::view::Region;
use serde::Deserialize;
use serde::Serialize;

use crate::v1;
use crate::v1::Name;

/// A reference to a single [`hyperactor::Proc`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcRef {
    proc_id: ProcId,
}

/// A reference to a ProcMesh, consisting of a set of ranked [`ProcRef`]s,
/// arranged into a region. ProcMeshes named, uniquely identifying the
/// ProcMesh from which the reference was derived.
///
/// ProcMeshes can be sliced to create new ProcMeshes with a subset of the
/// original ranks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct ProcMeshRef {
    name: Name,
    region: Region,
    ranks: Arc<Vec<ProcRef>>,
}

impl ProcMeshRef {
    /// Create a new ProcMeshRef from the given name, region, and ranks.
    fn new(name: Name, region: Region, ranks: Vec<ProcRef>) -> v1::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(v1::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        Ok(Self {
            name,
            region,
            ranks: Arc::new(ranks),
        })
    }
}

impl view::Ranked for ProcMeshRef {
    type Item = ProcRef;

    fn region(&self) -> &Region {
        &self.region
    }

    fn ranks(&self) -> &[ProcRef] {
        &self.ranks
    }

    fn sliced<'a>(&self, region: Region, nodes: impl Iterator<Item = &'a ProcRef>) -> Self {
        Self::new(self.name.clone(), region, nodes.cloned().collect()).unwrap()
    }
}
