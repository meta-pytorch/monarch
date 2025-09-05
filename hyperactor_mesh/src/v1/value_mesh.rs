/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use futures::Future;
use futures::future;
use ndslice::view;
use ndslice::view::Region;

/// A mesh of values, where each value is associated with a rank.
pub struct ValueMesh<T> {
    region: Region,
    ranks: Vec<T>,
}

impl<T> ValueMesh<T> {
    /// Creates a new value mesh with the given region and ranks.
    pub(crate) fn new(region: Region, ranks: Vec<T>) -> Self {
        Self { region, ranks }
    }
}

impl<F: Future> ValueMesh<F> {
    /// Waits for all futures to complete, producing a mesh with their results.
    pub async fn join(self) -> ValueMesh<F::Output> {
        let ValueMesh { region, ranks } = self;
        ValueMesh {
            region,
            ranks: future::join_all(ranks.into_iter()).await,
        }
    }
}

impl<T, E> ValueMesh<Result<T, E>> {
    /// Promotes a ValueMesh of results to a Result of ValueMesh.
    pub fn promote_result(self) -> Result<ValueMesh<T>, E> {
        let ValueMesh { region, ranks } = self;
        let ranks = ranks.into_iter().collect::<Result<Vec<T>, E>>()?;
        Ok(ValueMesh { region, ranks })
    }
}

impl<T: Clone + 'static> view::Ranked for ValueMesh<T> {
    type Item = T;

    fn region(&self) -> &Region {
        &self.region
    }

    fn get(&self, rank: usize) -> Option<T> {
        self.ranks.get(rank).cloned()
    }

    fn sliced(&self, region: Region, nodes: impl Iterator<Item = T>) -> Self {
        Self {
            region,
            ranks: nodes.collect(),
        }
    }
}
