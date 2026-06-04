/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Property-based generators for [`RankRect`] and related types.
//!
//! These strategies are test infrastructure for code that reasons about
//! rank-space geometry. They generate dense and row-major-derived affine
//! [`RankRect`]s, including selected sub-rectangles whose base ranks remain in
//! the same coordinate frame as their parent.
//!
//! The public API is intentionally small. It covers the shapes downstream
//! tiling/routing tests need today and leaves richer generators, such as
//! column-major/permuted strides or sparse [`crate::RankSpace`] values, as
//! follow-up work.
//!
//! # Example
//!
//! ```ignore
//! use proptest::prelude::*;
//! use rankspace::strategy::gen_rank_rect_strided;
//!
//! proptest! {
//!     #[test]
//!     fn my_tiler_respects_affine_frame(
//!         rect in gen_rank_rect_strided(1..=4, 4, 3, 32),
//!     ) {
//!         // assertions on rect ...
//!     }
//! }
//! ```

use std::num::NonZeroUsize;
use std::ops::RangeInclusive;

use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;

use crate::Dim;
use crate::DimRange;
use crate::Extent;
use crate::Rank;
use crate::RankRect;

/// Generates a random [`Extent`] with dimensionality in `dims` and
/// per-dimension sizes in `1..=max_size`.
///
/// Dimension names are generated as `d0, d1, ...`.
pub fn gen_extent(dims: RangeInclusive<usize>, max_size: usize) -> impl Strategy<Value = Extent> {
    prop::collection::vec(1..=max_size, dims).prop_map(|sizes| {
        Extent::new(
            sizes
                .into_iter()
                .enumerate()
                .map(|(index, size)| Dim::new(format!("d{index}"), size))
                .collect(),
        )
        .expect("generated dimensions are valid")
    })
}

/// Generates a random dense row-major [`RankRect`].
///
/// The returned rectangle has base offset zero and row-major strides. Use
/// [`gen_rank_rect_strided`] for affine rectangles with non-trivial offsets
/// or strides.
pub fn gen_rank_rect(
    dims: RangeInclusive<usize>,
    max_size: usize,
) -> impl Strategy<Value = RankRect> {
    gen_extent(dims, max_size).prop_map(|extent| {
        RankRect::new(extent).expect("dense rect built from a generated extent is valid")
    })
}

/// Generates a row-major-derived affine [`RankRect`].
///
/// The generated rectangle starts from a row-major parent with dimensionality
/// in `dims`, per-dimension sizes in `1..=max_size`, and parent base offset in
/// `0..=max_base_offset`. It then applies one selection per dimension. Each
/// selection may move the output offset and may multiply that dimension's
/// stride by a step in `1..=max_step`.
///
/// This is the rankspace replacement for `ndslice::strategy::gen_region_strided`.
/// It is the load-bearing generator for property tests that need to prove
/// affine-frame preservation — for example, that tile decomposition
/// children stay in the same base-rank frame as their parent.
///
/// TODO: column-major and permuted-stride coverage are not produced by this
/// generator. Add a dedicated `gen_rank_rect_permuted` if downstream tests
/// need those shapes.
pub fn gen_rank_rect_strided(
    dims: RangeInclusive<usize>,
    max_size: usize,
    max_step: usize,
    max_base_offset: usize,
) -> impl Strategy<Value = RankRect> {
    gen_rank_rect_with_selections(dims, max_size, max_step, max_base_offset)
        .prop_map(|(_parent, _selections, subrect)| subrect)
}

/// Generates a `(parent, subrect)` pair in that order.
///
/// `parent` is a row-major [`RankRect`] with possible non-zero base offset.
/// `subrect` is produced from `parent` by applying one [`RankRect::select`]
/// per dimension with random `(begin, step)` choices. The pair satisfies
/// `parent.contains_rect(&subrect)`, and `subrect`'s ranks are still base ranks
/// in `parent`'s coordinate frame.
///
/// Use this when the test needs both endpoints — for example, to assert
/// containment of the selected subrect within the parent. For tests that
/// only need the strided child, prefer [`gen_rank_rect_strided`].
pub fn gen_rank_rect_and_strided_subrect(
    dims: RangeInclusive<usize>,
    max_size: usize,
    max_step: usize,
    max_base_offset: usize,
) -> impl Strategy<Value = (RankRect, RankRect)> {
    gen_rank_rect_with_selections(dims, max_size, max_step, max_base_offset)
        .prop_map(|(parent, _selections, subrect)| (parent, subrect))
}

/// Internal generator that also returns the per-dim `(begin, step)` selections
/// used to derive the subrect from the parent.
///
/// Strategy tests use the selections to project subrect coordinates back into
/// parent coordinates and prove base-rank-frame preservation. The public API
/// intentionally hides them: downstream tests need generated shapes, not the
/// generator's construction trace.
fn gen_rank_rect_with_selections(
    dims: RangeInclusive<usize>,
    max_size: usize,
    max_step: usize,
    max_base_offset: usize,
) -> impl Strategy<Value = (RankRect, Vec<(usize, usize)>, RankRect)> {
    let max_step = max_step.max(1);
    gen_extent(dims, max_size).prop_flat_map(move |extent| {
        let sizes: Vec<usize> = extent.sizes().collect();
        let begins_strategy: BoxedStrategy<Vec<usize>> =
            sizes
                .iter()
                .fold(Just(Vec::<usize>::new()).boxed(), |acc, &size| {
                    let size = size.max(1);
                    (acc, 0..size)
                        .prop_map(|(mut begins, begin)| {
                            begins.push(begin);
                            begins
                        })
                        .boxed()
                });
        let steps_raw_strategy = prop::collection::vec(1..=max_step, sizes.len());
        (
            Just(extent),
            0..=max_base_offset,
            begins_strategy,
            steps_raw_strategy,
        )
            .prop_map(move |(extent, offset, begins, steps_raw)| {
                // Clamp steps so they satisfy steps[i] % steps[i+1] == 0.
                // Without this, per-dim independent stride choices on a
                // row-major source can yield NonrectangularStrides. Mirrors
                // ndslice::strategy::gen_region_strided's clamp pass.
                let steps = clamp_steps_to_divisibility_chain(steps_raw, max_step);
                let selections: Vec<(usize, usize)> = begins.into_iter().zip(steps).collect();

                let strides = row_major_strides(extent.sizes());
                let parent = if offset == 0 {
                    RankRect::new(extent.clone()).expect("dense rect is valid")
                } else {
                    RankRect::affine(extent.clone(), Rank(offset), strides)
                        .expect("row-major rect with offset is valid")
                };
                let dim_names: Vec<String> = extent
                    .dims()
                    .iter()
                    .map(|dim| dim.name().to_string())
                    .collect();
                let mut subrect = parent.clone();
                for (dim_index, &(begin, step)) in selections.iter().enumerate() {
                    let step_nonzero =
                        NonZeroUsize::new(step.max(1)).expect("step max(1) is non-zero");
                    subrect = subrect
                        .select(
                            &dim_names[dim_index],
                            DimRange::new(begin, None, step_nonzero),
                        )
                        .expect("clamped steps preserve rectangularity; select must succeed");
                }
                (parent, selections, subrect)
            })
    })
}

/// Clamp per-dim selection steps into the divisibility chain required by
/// [`RankRect::affine`].
///
/// For row-major-derived rectangles, selecting outer dimensions with arbitrary
/// independent steps can create non-rectangular stride layouts. Ensuring
/// `steps[i] % steps[i + 1] == 0` keeps the selected strides representable as a
/// single [`RankRect`].
fn clamp_steps_to_divisibility_chain(mut steps: Vec<usize>, max_step: usize) -> Vec<usize> {
    let max_step = max_step.max(1);
    if steps.is_empty() {
        return steps;
    }
    let last = steps.len() - 1;
    steps[last] = steps[last].max(1).min(max_step);
    for i in (0..last).rev() {
        let inner = steps[i + 1].max(1);
        let max_mult = (max_step / inner).max(1);
        let m = ((steps[i].max(1) - 1) % max_mult) + 1;
        steps[i] = inner.saturating_mul(m);
    }
    steps
}

fn row_major_strides(sizes: impl IntoIterator<Item = usize>) -> Vec<usize> {
    let sizes = sizes.into_iter().collect::<Vec<_>>();
    let mut strides = vec![1; sizes.len()];
    for index in (0..sizes.len().saturating_sub(1)).rev() {
        strides[index] = strides[index + 1] * sizes[index + 1];
    }
    strides
}

#[cfg(test)]
mod tests {
    use proptest::strategy::Strategy;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::Config;
    use proptest::test_runner::TestRunner;

    use super::*;
    use crate::Coord;

    fn coords_for(extent: &Extent) -> Vec<Vec<usize>> {
        (0..extent.cardinality())
            .map(|index| coord_at(index, extent.sizes()).expect("index is in bounds"))
            .collect()
    }

    fn coord_at(index: usize, sizes: impl IntoIterator<Item = usize>) -> Option<Vec<usize>> {
        let sizes = sizes.into_iter().collect::<Vec<_>>();
        let cardinality = sizes.iter().product::<usize>();
        if index >= cardinality {
            return None;
        }
        let mut rest = index;
        let mut coord = vec![0; sizes.len()];
        for (axis, size) in sizes.iter().copied().enumerate().rev() {
            coord[axis] = rest % size;
            rest /= size;
        }
        Some(coord)
    }

    proptest! {
        // Tests that the dense rectangle generator emits valid row-major
        // rectangles: every generated coordinate resolves to a rank, and that
        // rank resolves back to the original coordinate.
        #[test]
        fn prop_gen_rank_rect_emits_valid_rectangles(rect in gen_rank_rect(1..=4, 8)) {
            for coord in coords_for(rect.extent()) {
                let rank = rect.rank_of(&coord).expect("generated coord is in range");
                prop_assert_eq!(
                    rect.coord_of(rank),
                    Some(Coord::new(coord.clone())),
                    "rank/coord roundtrip failed for {:?}", coord
                );
            }
        }

        // Tests that the strided rectangle generator still emits valid affine
        // rectangles after base-offset and per-dimension selection. This guards
        // the generator itself against producing shapes `RankRect` cannot
        // round-trip through rank/coordinate conversion.
        #[test]
        fn prop_gen_rank_rect_strided_emits_valid_rectangles(
            rect in gen_rank_rect_strided(1..=4, 4, 3, 32),
        ) {
            for coord in coords_for(rect.extent()) {
                let rank = rect.rank_of(&coord).expect("generated coord is in range");
                prop_assert_eq!(
                    rect.coord_of(rank),
                    Some(Coord::new(coord.clone())),
                    "rank/coord roundtrip failed for {:?}", coord
                );
            }
        }

        // Tests the public pair-generator contract: the returned subrect is
        // contained in the returned parent, rather than being an unrelated
        // valid rectangle.
        #[test]
        fn prop_gen_rank_rect_and_strided_subrect_satisfies_containment(
            (parent, subrect) in gen_rank_rect_and_strided_subrect(1..=4, 4, 3, 32),
        ) {
            prop_assert!(parent.contains_rect(&subrect));
        }

        // Tests the load-bearing affine-frame invariant for future tiler
        // proptests: each subrect coordinate maps to the same base rank as the
        // corresponding selected parent coordinate. This catches generators
        // that accidentally rebase children into a fresh `0..n` rank frame.
        #[test]
        fn prop_gen_rank_rect_and_strided_subrect_preserves_base_rank_embedding(
            (parent, selections, subrect) in gen_rank_rect_with_selections(1..=4, 4, 3, 32),
        ) {
            for coord in coords_for(subrect.extent()) {
                let projected: Vec<usize> = coord
                    .iter()
                    .zip(&selections)
                    .map(|(c, &(begin, step))| begin + *c * step.max(1))
                    .collect();
                prop_assert_eq!(
                    subrect.rank_of(&coord),
                    parent.rank_of(&projected),
                    "subrect coord {:?} did not embed at projected parent coord {:?}",
                    coord, projected
                );
            }
        }
    }

    fn sample_n<T>(strategy: impl Strategy<Value = T>, n: usize) -> Vec<T> {
        let mut runner = TestRunner::new(Config::default());
        (0..n)
            .map(|_| strategy.new_tree(&mut runner).unwrap().current())
            .collect()
    }

    // Tests that `max_base_offset` is consumed by the generator. This inspects
    // the parent via the internal helper because a subrect's final offset can
    // also become non-zero from `select(begin > 0)`.
    #[test]
    fn gen_rank_rect_strided_can_emit_non_zero_offset() {
        let samples = sample_n(gen_rank_rect_with_selections(1..=4, 4, 3, 32), 256);
        assert!(
            samples
                .iter()
                .any(|(parent, _, _)| parent.offset().get() != 0),
            "expected at least one sample with a non-zero parent offset"
        );
    }

    // Tests that the generator can apply `step > 1` selections. Checking the
    // subrect's strides alone would be too weak because multi-dimensional
    // row-major parents naturally have strides greater than one.
    #[test]
    fn gen_rank_rect_strided_can_emit_non_unit_stride() {
        let samples = sample_n(gen_rank_rect_with_selections(1..=4, 4, 3, 32), 256);
        assert!(
            samples
                .iter()
                .any(|(_, selections, _)| selections.iter().any(|&(_, step)| step > 1)),
            "expected at least one sample with a per-dim selection step > 1"
        );
    }

    // Tests that the generator can emit different selection steps across
    // dimensions, not just a uniform stride scale. With the divisibility-chain
    // clamp, non-uniformity means at least one outer step is a proper multiple
    // of an inner step. Requires at least two dimensions.
    #[test]
    fn gen_rank_rect_strided_can_emit_non_uniform_per_dim_stride() {
        let samples = sample_n(gen_rank_rect_with_selections(2..=4, 4, 3, 32), 256);
        let non_uniform_seen = samples.iter().any(|(_, selections, _)| {
            if selections.len() < 2 {
                return false;
            }
            let first_step = selections[0].1;
            selections.iter().any(|&(_, step)| step != first_step)
        });
        assert!(
            non_uniform_seen,
            "expected at least one sample with non-uniform per-dim selection steps"
        );
    }
}
