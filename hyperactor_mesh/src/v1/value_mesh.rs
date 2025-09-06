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

    /// Constructs a `ValueMesh` without checking cardinality. Caller
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
        debug_assert!(region.is_subset(self.region()), "sliced: not a subset");
        let ranks: Vec<T> = nodes.collect();
        debug_assert_eq!(
            region.num_ranks(),
            ranks.len(),
            "sliced: cardinality mismatch"
        );
        Self::new_unchecked(region, ranks)
    }
}

// `FromIterator` cant't work for `ValueMesh`: it has no way to carry
// the required `Region`, and it can’t fail if the iterator length
// mismatches. This trait provides a "mesh-aware" collect: consume an
// iterator and a `Region`, and build a `ValueMesh<T>` with
// cardinality validated.
pub trait CollectMesh<T>: Iterator<Item = T> + Sized {
    fn collect_mesh(self, region: view::Region) -> crate::v1::Result<ValueMesh<T>>;
}

impl<I: Iterator<Item = T>, T> CollectMesh<T> for I {
    /// Collects items into a `ValueMesh` for the given `region`.
    ///
    /// Delegates to `ValueMesh::new`, so the result is only returned
    /// if the iterator yields exactly `region.num_ranks()` items.
    fn collect_mesh(self, region: view::Region) -> crate::v1::Result<ValueMesh<T>> {
        ValueMesh::new(region, self.collect())
    }
}

/// Like `CollectMesh`, but for `ExactSizeIterator`. Uses `len()` to
/// pre-check cardinality and fail fast (no allocation) if `len() !=
/// region.num_ranks()`. On success, builds a complete mesh.
pub trait CollectExactMesh<T>: ExactSizeIterator<Item = T> + Sized {
    fn collect_exact_mesh(self, region: view::Region) -> crate::v1::Result<ValueMesh<T>>;
}

impl<I: ExactSizeIterator<Item = T>, T> CollectExactMesh<T> for I {
    // Pre-check length via `len()` to fail fast before collecting.
    fn collect_exact_mesh(self, region: view::Region) -> crate::v1::Result<ValueMesh<T>> {
        let expected = region.num_ranks();
        let actual = self.len();
        if actual != expected {
            return Err(crate::v1::Error::InvalidRankCardinality { expected, actual });
        }
        Ok(ValueMesh::new_unchecked(region, self.collect()))
    }
}

pub trait TryCollectIndexedMesh<T>: Iterator<Item = (usize, T)> + Sized {
    /// Collect `(rank, value)` pairs into a `ValueMesh<T>` for
    /// `region`.
    ///
    /// Validates that:
    /// - every rank `0..region.num_ranks()` is provided exactly once
    ///   overall (coverage),
    /// - no pair has an out-of-bounds rank (`rank >= num_ranks()`).
    ///
    /// Duplicates are allowed; the **last write wins**. Missing ranks
    /// or out-of-bound ranks return `InvalidRankCardinality`.
    fn try_collect_indexed(self, region: view::Region) -> crate::v1::Result<ValueMesh<T>>;
}

impl<I: Iterator<Item = (usize, T)>, T> TryCollectIndexedMesh<T> for I {
    fn try_collect_indexed(self, region: view::Region) -> crate::v1::Result<ValueMesh<T>> {
        let n = region.num_ranks();

        // Buffer for exactly n slots; fill by rank.
        let mut buf: Vec<Option<T>> = std::iter::repeat_with(|| None).take(n).collect();
        let mut filled = 0usize;

        for (rank, value) in self {
            if rank >= n {
                // Out-of-bounds: report `expected` = n, `actual` =
                // offending index + 1; i.e. number of ranks implied
                // so far.
                return Err(crate::v1::Error::InvalidRankCardinality {
                    expected: n,
                    actual: rank + 1,
                });
            }
            if buf[rank].is_none() {
                filled += 1;
            }
            buf[rank] = Some(value); // Last write wins.
        }

        if filled != n {
            // Missing ranks: actual = number of distinct ranks seen.
            return Err(crate::v1::Error::InvalidRankCardinality {
                expected: n,
                actual: filled,
            });
        }

        // All present and in-bounds: unwrap and build unchecked.
        let ranks: Vec<T> = buf.into_iter().map(Option::unwrap).collect();
        Ok(ValueMesh::new_unchecked(region, ranks))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use futures::executor::block_on;
    use futures::future;
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

    #[test]
    fn value_mesh_transpose_ok_and_err() {
        let region: Region = extent!(x = 2).into();

        // ok case
        let ok_mesh = ValueMesh::new(region.clone(), vec![Ok::<_, Infallible>(1), Ok(2)]).unwrap();
        let ok = ok_mesh.transpose().unwrap();
        assert_eq!(ok.values().collect::<Vec<_>>(), vec![1, 2]);

        // err case: propagate user E
        #[derive(Debug, PartialEq)]
        enum E {
            Boom,
        }
        let err_mesh = ValueMesh::new(region, vec![Ok(1), Err(E::Boom)]).unwrap();
        let err = err_mesh.transpose().unwrap_err();
        assert_eq!(err, E::Boom);
    }

    #[test]
    fn value_mesh_join_preserves_region_and_values() {
        let region: Region = extent!(x = 2, y = 2).into();
        let futs = vec![
            future::ready(10),
            future::ready(11),
            future::ready(12),
            future::ready(13),
        ];
        let mesh = ValueMesh::new(region.clone(), futs).unwrap();

        let joined = block_on(mesh.join());
        assert_eq!(joined.region().num_ranks(), 4);
        assert_eq!(joined.values().collect::<Vec<_>>(), vec![10, 11, 12, 13]);
    }

    #[test]
    fn collect_mesh_ok() {
        let region: Region = extent!(x = 2, y = 3).into();
        let mesh = (0..6)
            .collect_mesh(region.clone())
            .expect("collect_mesh should succeed");

        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn collect_mesh_len_too_short_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..5).collect_mesh(region).unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_mesh_len_too_long_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..7).collect_mesh(region).unwrap_err();
        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 7);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_mesh_from_map_pipeline() {
        let region: Region = extent!(x = 2, y = 2).into();
        let mesh = (0..4).map(|i| i * 10).collect_mesh(region.clone()).unwrap();

        assert_eq!(mesh.region().num_ranks(), 4);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 10, 20, 30]);
    }

    #[test]
    fn collect_exact_mesh_ok() {
        let region: Region = extent!(x = 2, y = 3).into();
        let mesh = (0..6)
            .collect_exact_mesh(region.clone())
            .expect("collect_exact_mesh should succeed");

        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn collect_exact_mesh_len_too_short_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..5).collect_exact_mesh(region).unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_exact_mesh_len_too_long_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..7).collect_exact_mesh(region).unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 7);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_exact_mesh_from_map_pipeline() {
        let region: Region = extent!(x = 2, y = 2).into();
        let mesh = (0..4)
            .map(|i| i * 10)
            .collect_exact_mesh(region.clone())
            .unwrap();

        assert_eq!(mesh.region().num_ranks(), 4);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 10, 20, 30]);
    }

    #[test]
    fn try_collect_indexed_ok_shuffled() {
        let region: Region = extent!(x = 2, y = 3).into();
        // (rank, value) in shuffled order; values = rank * 10
        let pairs = vec![(3, 30), (0, 0), (5, 50), (2, 20), (1, 10), (4, 40)];
        let mesh = pairs
            .into_iter()
            .try_collect_indexed(region.clone())
            .unwrap();

        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(
            mesh.values().collect::<Vec<_>>(),
            vec![0, 10, 20, 30, 40, 50]
        );
    }

    #[test]
    fn try_collect_indexed_missing_rank_is_error() {
        let region: Region = extent!(x = 2, y = 2).into(); // 4
        // Missing rank 3
        let pairs = vec![(0, 100), (1, 101), (2, 102)];
        let err = pairs.into_iter().try_collect_indexed(region).unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 4);
                assert_eq!(actual, 3); // Distinct ranks seen.
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn try_collect_indexed_out_of_bounds_is_error() {
        let region: Region = extent!(x = 2, y = 2).into(); // 4 (valid ranks 0..=3)
        let pairs = vec![(0, 1), (4, 9)]; // 4 is out-of-bounds
        let err = pairs.into_iter().try_collect_indexed(region).unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 4);
                assert_eq!(actual, 5); // offending index + 1
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn try_collect_indexed_duplicate_last_write_wins() {
        let region: Region = extent!(x = 1, y = 3).into(); // 3
        // rank 1 appears twice; last value should stick
        let pairs = vec![(0, 7), (1, 8), (1, 88), (2, 9)];
        let mesh = pairs
            .into_iter()
            .try_collect_indexed(region.clone())
            .unwrap();

        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![7, 88, 9]);
    }
}
