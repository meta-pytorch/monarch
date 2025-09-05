/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use futures::Future;
use ndslice::view;
use ndslice::view::Region;

/// A mesh of values, where each value is associated with a rank.
///
/// # Invariant
/// The mesh is *complete*: `ranks.len()` always equals
/// `region.num_ranks()`. Every rank in the region has exactly one
/// associated value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)] // only if T implements
pub struct ValueMesh<T> {
    region: Region,
    ranks: Vec<T>,
}

impl<T> ValueMesh<T> {
    /// Creates a new `ValueMesh` for `region` with exactly one value
    /// per rank.
    ///
    /// # Invariants
    /// This constructor validates that the number of provided values
    /// (`ranks.len()`) matches the region’s cardinality
    /// (`region.num_ranks()`). A value mesh must be complete: every
    /// rank in `region` has a corresponding `T`.
    ///
    /// # Errors
    /// Returns [`Error::InvalidRankCardinality`] if `ranks.len() !=
    /// region.num_ranks()`.
    /// ```
    pub(crate) fn new(region: Region, ranks: Vec<T>) -> crate::v1::Result<Self> {
        let (actual, expected) = (ranks.len(), region.num_ranks());
        if actual != expected {
            return Err(crate::v1::Error::InvalidRankCardinality { expected, actual });
        }
        Ok(Self { region, ranks })
    }

    /// Constructs a ValueMesh without checking cardinality. Caller
    /// must ensure `ranks.len() == region.num_ranks()`.
    #[inline]
    pub(crate) fn new_unchecked(region: Region, ranks: Vec<T>) -> Self {
        debug_assert_eq!(region.num_ranks(), ranks.len());
        Self { region, ranks }
    }
}

impl<F: Future> ValueMesh<F> {
    /// Await all futures in the mesh, yielding a `ValueMesh` of their
    /// outputs.
    pub async fn join(self) -> ValueMesh<F::Output> {
        let ValueMesh { region, ranks } = self;
        ValueMesh::new_unchecked(region, futures::future::join_all(ranks).await)
    }
}

impl<T, E> ValueMesh<Result<T, E>> {
    /// Transposes a `ValueMesh<Result<T, E>>` into a
    /// `Result<ValueMesh<T>, E>`.
    pub fn transpose(self) -> Result<ValueMesh<T>, E> {
        let ValueMesh { region, ranks } = self;
        let ranks = ranks.into_iter().collect::<Result<Vec<T>, E>>()?;
        Ok(ValueMesh::new_unchecked(region, ranks))
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

#[cfg(test)]
mod tests {
    use ndslice::extent;
    use ndslice::view::Ranked;
    use ndslice::view::ViewExt;

    use super::*;

    #[test]
    fn value_mesh_new_ok() {
        let region: Region = extent!(replica = 2, gpu = 3).into();
        let mesh = ValueMesh::new(region.clone(), (0..6).collect()).expect("new should succeed");
        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(mesh.values().count(), 6);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn value_mesh_new_len_mismatch_is_error() {
        let region: Region = extent!(replica = 2, gpu = 3).into();
        let err = ValueMesh::new(region, vec![0_i32; 5]).unwrap_err();
        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
