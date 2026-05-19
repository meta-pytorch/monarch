/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeSet;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;

use proptest::prelude::*;

use crate::Coord;
use crate::Dim;
use crate::DimRange;
use crate::Extent;
use crate::Rank;
use crate::RankMask;
use crate::RankRect;
use crate::RankSpace;
use crate::view::BaseView;
use crate::view::CompactView;

fn gen_extent(dims: RangeInclusive<usize>, max_size: usize) -> impl Strategy<Value = Extent> {
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

fn gen_rect(dims: RangeInclusive<usize>, max_size: usize) -> impl Strategy<Value = RankRect> {
    gen_extent(dims, max_size)
        .prop_map(|extent| RankRect::new(extent).expect("generated extent is valid"))
}

fn gen_sparse_space(
    dims: RangeInclusive<usize>,
    max_size: usize,
) -> impl Strategy<Value = (RankSpace, BTreeSet<Rank>)> {
    gen_rect(dims, max_size)
        .prop_flat_map(|rect| {
            let ranks = rect.iter_ranks().collect::<Vec<_>>();
            let max_occlusions = ranks.len().min(16);
            let rank_strategy = prop::sample::subsequence(ranks, 0..=max_occlusions);
            (Just(rect), rank_strategy)
        })
        .prop_map(|(rect, occluded)| {
            let occluded = occluded.into_iter().collect::<BTreeSet<_>>();
            let space = RankSpace::dense(rect).without(RankMask::ranks(occluded.iter().copied()));
            (space, occluded)
        })
}

fn gen_selection(
    max_dims: usize,
    max_size: usize,
    max_step: usize,
) -> impl Strategy<Value = (RankRect, usize, usize, usize, usize)> {
    gen_rect(1..=max_dims, max_size).prop_flat_map(move |rect| {
        let dims = rect.extent().dims().to_vec();
        let dim_count = dims.len();
        (0..dim_count).prop_flat_map(move |dim_index| {
            let rect = rect.clone();
            let dim_size = dims[dim_index].size();
            (0..dim_size).prop_flat_map(move |start| {
                let rect = rect.clone();
                (start + 1..=dim_size).prop_flat_map(move |end| {
                    let rect = rect.clone();
                    (1..=max_step).prop_map(move |step| (rect.clone(), dim_index, start, end, step))
                })
            })
        })
    })
}

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
    #[test]
    fn rank_rect_ranks_are_injective(rect in gen_rect(1..=4, 8)) {
        let mut seen = HashSet::new();

        for coord in coords_for(rect.extent()) {
            let rank = rect.rank_of(&coord).expect("generated coordinate is valid");
            prop_assert!(
                seen.insert(rank),
                "duplicate rank {:?} for coordinate {:?}",
                rank,
                coord
            );
        }
    }

    #[test]
    fn rank_rect_row_major_ranks_are_monotonic(rect in gen_rect(1..=4, 8)) {
        let mut last_rank = None;

        for rank in rect.iter_ranks() {
            if let Some(prev) = last_rank {
                prop_assert!(
                    prev < rank,
                    "rank order is not monotonic: {:?} >= {:?}",
                    prev,
                    rank
                );
            }
            last_rank = Some(rank);
        }
    }

    #[test]
    fn rank_rect_ranks_stay_in_dense_bounds(rect in gen_rect(1..=4, 8)) {
        for rank in rect.iter_ranks() {
            prop_assert!(
                rank.get() < rect.cardinality(),
                "rank {:?} is outside dense cardinality {}",
                rank,
                rect.cardinality()
            );
        }
    }

    #[test]
    fn rank_rect_rank_to_coord_roundtrip(rect in gen_rect(0..=4, 8)) {
        for rank in rect.iter_ranks() {
            let coord = rect.coord_of(rank).expect("iterated rank is contained");
            prop_assert_eq!(
                rect.rank_of(coord.indices()),
                Some(rank),
                "rank/coordinate roundtrip failed for {:?}",
                rank
            );
        }
    }

    #[test]
    fn rank_rect_coord_to_rank_roundtrip(rect in gen_rect(0..=4, 8)) {
        for coord in coords_for(rect.extent()) {
            let rank = rect.rank_of(&coord).expect("generated coordinate is valid");
            prop_assert_eq!(
                rect.coord_of(rank),
                Some(Coord::new(coord.clone())),
                "coordinate/rank roundtrip failed for {:?}",
                coord
            );
        }
    }

    #[test]
    fn select_preserves_base_rank_embedding(
        (rect, dim_index, start, end, step) in gen_selection(4, 8, 4)
    ) {
        let dim_name = rect.extent().dims()[dim_index].name().to_string();
        let Ok(selected) = rect.select(
            &dim_name,
            DimRange::new(start, Some(end), NonZeroUsize::new(step).unwrap()),
        ) else {
            return Ok(());
        };

        for coord in coords_for(selected.extent()) {
            let mut base_coord = coord.clone();
            base_coord[dim_index] = start + coord[dim_index] * step;
            prop_assert!(
                base_coord[dim_index] < end,
                "selected coordinate projects outside source range"
            );
            prop_assert_eq!(
                selected.rank_of(&coord),
                rect.rank_of(&base_coord),
                "selected coordinate did not preserve base rank"
            );
        }
    }

    #[test]
    fn fix_preserves_base_rank_embedding(rect in gen_rect(1..=4, 8)) {
        for dim_index in 0..rect.extent().len() {
            let dim = rect.extent().dims()[dim_index].clone();
            for fixed_index in 0..dim.size() {
                let fixed = rect
                    .fix(dim.name(), fixed_index)
                    .expect("generated fixed index is valid");

                for coord in coords_for(fixed.extent()) {
                    let mut base_coord = coord.clone();
                    base_coord.insert(dim_index, fixed_index);
                    prop_assert_eq!(
                        fixed.rank_of(&coord),
                        rect.rank_of(&base_coord),
                        "fixed coordinate did not preserve base rank"
                    );
                }
            }
        }
    }

    #[test]
    fn sparse_space_iterates_base_ranks_minus_occlusions(
        (space, occluded) in gen_sparse_space(1..=4, 8)
    ) {
        let visible = space.iter_ranks().collect::<BTreeSet<_>>();
        let expected = space
            .base()
            .iter_ranks()
            .filter(|rank| !occluded.contains(rank))
            .collect::<BTreeSet<_>>();

        prop_assert_eq!(visible, expected);
        for rank in space.base().iter_ranks() {
            prop_assert_eq!(
                space.contains_rank(rank),
                !occluded.contains(&rank),
                "sparse membership mismatch for {:?}",
                rank
            );
        }
    }

    #[test]
    fn sparse_space_rank_local_index_roundtrip(
        (space, _) in gen_sparse_space(1..=4, 8)
    ) {
        for (index, rank) in space.iter_ranks().enumerate() {
            prop_assert_eq!(space.rank_at(index), Some(rank));
            prop_assert_eq!(space.local_index_of(rank), Some(index));
        }
        prop_assert_eq!(space.rank_at(space.cardinality()), None);
    }

    #[test]
    fn base_and_compact_views_agree_on_visible_ranks(
        (space, _) in gen_sparse_space(1..=4, 8)
    ) {
        let base_len = space
            .base()
            .iter_ranks()
            .map(Rank::get)
            .max()
            .map_or(0, |rank| rank + 1);
        let base_values = (0..base_len).collect::<Vec<_>>();
        let compact_values = space
            .iter_ranks()
            .map(Rank::get)
            .collect::<Vec<_>>();

        let base = BaseView::new(space.clone(), base_values);
        let compact = CompactView::new(space.clone(), compact_values)
            .expect("compact values match visible cardinality");

        for rank in space.iter_ranks() {
            prop_assert_eq!(base.get_rank(rank), compact.get_rank(rank));
        }
    }
}
