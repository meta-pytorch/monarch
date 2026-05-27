/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Value containers indexed by [`RankSpace`].
//!
//! The core crate models rank-space geometry. This module adds the optional
//! storage layer: it attaches user data to the visible ranks of a [`RankSpace`]
//! without changing the space itself.
//!
//! Views keep borrowing in the type of their storage. A view can own a
//! `Vec<Item>`, borrow a slice, or hold another container that implements
//! `AsRef<[Item]>`. Cloning a view is therefore a structural clone of the
//! storage handle. Use [`BaseView::cloned_all`] or [`CompactView::cloned`] when
//! you want an owned `Vec` of cloned items.
//!
//! The module has two storage layouts:
//!
//! - [`BaseView`] is addressed by base rank: `rank -> data[rank]`. It is useful
//!   when the backing data is already indexed by global rank, or when preserving
//!   unused and occluded slots is cheaper than compaction.
//! - [`CompactView`] is addressed by visible rank order:
//!   `rank -> visible_index -> data[visible_index]`. It is useful when storage
//!   should contain exactly the visible values of a sparse space.
//!
//! The [`View`] trait is the common read-only surface for code that only needs
//! to read values by visible rank or local coordinate. Layout-specific operations
//! stay on the concrete view types. For example, [`BaseView::map_all`] preserves
//! base indexing, [`BaseView::map_visible`] projects visible values into compact
//! storage, and [`CompactView::map`] preserves compact indexing.

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::Rank;
use crate::RankSpace;

/// Errors produced by view construction and transformation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ViewError {
    /// Compact storage cardinality does not match the rank space.
    #[error("invalid storage cardinality: expected {expected}, got {actual}")]
    InvalidCardinality { expected: usize, actual: usize },

    /// A visible rank does not have a corresponding base-indexed storage slot.
    #[error("rank {rank:?} is out of bounds for storage length {len}")]
    RankOutOfBounds { rank: Rank, len: usize },
}

/// A read-only value container indexed by a [`RankSpace`].
pub trait View<Item> {
    /// Returns the rank space that indexes this view.
    fn space(&self) -> &RankSpace;

    /// Returns the value addressed by a visible base rank.
    fn get_rank(&self, rank: Rank) -> Option<&Item>;

    /// Returns the value addressed by a coordinate in the view's rank space.
    fn get_coord(&self, coord: impl AsRef<[usize]>) -> Option<&Item> {
        self.get_rank(self.space().rank_of(coord)?)
    }

    /// Iterates visible ranks and their values.
    fn iter<'a>(&'a self) -> impl Iterator<Item = (Rank, &'a Item)> + 'a
    where
        Item: 'a,
    {
        self.space()
            .iter_ranks()
            .filter_map(|rank| self.get_rank(rank).map(|item| (rank, item)))
    }
}

/// A value container addressed by base rank.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BaseView<T> {
    space: RankSpace,
    data: T,
}

impl<T> BaseView<T> {
    /// Creates a base-indexed view.
    pub fn new(space: RankSpace, data: T) -> Self {
        Self { space, data }
    }

    /// Creates a base-indexed view and checks that all visible ranks are in bounds.
    pub fn new_checked<Item>(space: RankSpace, data: T) -> Result<Self, ViewError>
    where
        T: AsRef<[Item]>,
    {
        let len = data.as_ref().len();
        if let Some(rank) = space.iter_ranks().find(|rank| rank.get() >= len) {
            return Err(ViewError::RankOutOfBounds { rank, len });
        }
        Ok(Self { space, data })
    }

    /// Returns the rank space.
    pub fn space(&self) -> &RankSpace {
        &self.space
    }

    /// Returns the underlying data.
    pub fn data(&self) -> &T {
        &self.data
    }

    /// Clones all backing storage into an owned base-indexed view.
    pub fn cloned_all<Item>(&self) -> BaseView<Vec<Item>>
    where
        T: AsRef<[Item]>,
        Item: Clone,
    {
        BaseView {
            space: self.space.clone(),
            data: self.data.as_ref().to_vec(),
        }
    }

    /// Eagerly maps all backing storage and preserves base indexing.
    pub fn map_all<Item, Output>(&self, f: impl FnMut(&Item) -> Output) -> BaseView<Vec<Output>>
    where
        T: AsRef<[Item]>,
    {
        BaseView {
            space: self.space.clone(),
            data: self.data.as_ref().iter().map(f).collect(),
        }
    }

    /// Eagerly maps all backing storage and preserves base indexing.
    pub fn try_map_all<Item, Output, Error>(
        &self,
        f: impl FnMut(&Item) -> Result<Output, Error>,
    ) -> Result<BaseView<Vec<Output>>, Error>
    where
        T: AsRef<[Item]>,
    {
        Ok(BaseView {
            space: self.space.clone(),
            data: self
                .data
                .as_ref()
                .iter()
                .map(f)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    /// Eagerly maps visible ranks into compact storage.
    pub fn map_visible<Item, Output>(
        &self,
        mut f: impl FnMut(Rank, &Item) -> Output,
    ) -> Result<CompactView<Vec<Output>>, ViewError>
    where
        T: AsRef<[Item]>,
    {
        let data = self
            .space
            .iter_ranks()
            .map(|rank| {
                let item = self
                    .get_rank(rank)
                    .ok_or_else(|| ViewError::RankOutOfBounds {
                        rank,
                        len: self.data.as_ref().len(),
                    })?;
                Ok(f(rank, item))
            })
            .collect::<Result<Vec<_>, ViewError>>()?;

        Ok(CompactView {
            space: self.space.clone(),
            data,
        })
    }

    /// Eagerly maps visible ranks into compact storage.
    pub fn try_map_visible<Item, Output, Error>(
        &self,
        mut f: impl FnMut(Rank, &Item) -> Result<Output, Error>,
    ) -> Result<CompactView<Vec<Output>>, ViewMapError<Error>>
    where
        T: AsRef<[Item]>,
    {
        let data = self
            .space
            .iter_ranks()
            .map(|rank| {
                let item = self.get_rank(rank).ok_or_else(|| {
                    ViewMapError::View(ViewError::RankOutOfBounds {
                        rank,
                        len: self.data.as_ref().len(),
                    })
                })?;
                f(rank, item).map_err(ViewMapError::Map)
            })
            .collect::<Result<Vec<_>, ViewMapError<Error>>>()?;

        Ok(CompactView {
            space: self.space.clone(),
            data,
        })
    }

    /// Returns the value addressed by a visible base rank.
    pub fn get_rank<Item>(&self, rank: Rank) -> Option<&Item>
    where
        T: AsRef<[Item]>,
    {
        if !self.space.contains_rank(rank) {
            return None;
        }
        self.data.as_ref().get(rank.get())
    }

    /// Returns the value addressed by a coordinate in the rank space.
    pub fn get_coord<Item>(&self, coord: impl AsRef<[usize]>) -> Option<&Item>
    where
        T: AsRef<[Item]>,
    {
        self.get_rank(self.space.rank_of(coord)?)
    }

    /// Iterates visible ranks and their values.
    pub fn iter<'a, Item: 'a>(&'a self) -> impl Iterator<Item = (Rank, &'a Item)> + 'a
    where
        T: AsRef<[Item]>,
    {
        self.space
            .iter_ranks()
            .filter_map(|rank| self.get_rank(rank).map(|item| (rank, item)))
    }
}

impl<T, Item> View<Item> for BaseView<T>
where
    T: AsRef<[Item]>,
{
    fn space(&self) -> &RankSpace {
        &self.space
    }

    fn get_rank(&self, rank: Rank) -> Option<&Item> {
        BaseView::get_rank(self, rank)
    }
}

/// A value container addressed densely over visible ranks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompactView<T> {
    space: RankSpace,
    data: T,
}

impl<T> CompactView<T> {
    /// Creates a compact-indexed view, checking visible-rank cardinality.
    pub fn new<Item>(space: RankSpace, data: T) -> Result<Self, ViewError>
    where
        T: AsRef<[Item]>,
    {
        let expected = space.cardinality();
        let actual = data.as_ref().len();
        if expected != actual {
            return Err(ViewError::InvalidCardinality { expected, actual });
        }
        Ok(Self { space, data })
    }

    /// Returns the rank space.
    pub fn space(&self) -> &RankSpace {
        &self.space
    }

    /// Returns the underlying data.
    pub fn data(&self) -> &T {
        &self.data
    }

    /// Clones the compact storage into an owned compact view.
    pub fn cloned<Item>(&self) -> CompactView<Vec<Item>>
    where
        T: AsRef<[Item]>,
        Item: Clone,
    {
        CompactView {
            space: self.space.clone(),
            data: self.data.as_ref().to_vec(),
        }
    }

    /// Eagerly maps all visible values and preserves compact indexing.
    pub fn map<Item, Output>(&self, f: impl FnMut(&Item) -> Output) -> CompactView<Vec<Output>>
    where
        T: AsRef<[Item]>,
    {
        CompactView {
            space: self.space.clone(),
            data: self.data.as_ref().iter().map(f).collect(),
        }
    }

    /// Eagerly maps all visible values and preserves compact indexing.
    pub fn try_map<Item, Output, Error>(
        &self,
        f: impl FnMut(&Item) -> Result<Output, Error>,
    ) -> Result<CompactView<Vec<Output>>, Error>
    where
        T: AsRef<[Item]>,
    {
        Ok(CompactView {
            space: self.space.clone(),
            data: self
                .data
                .as_ref()
                .iter()
                .map(f)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    /// Returns the value addressed by a visible base rank.
    pub fn get_rank<Item>(&self, rank: Rank) -> Option<&Item>
    where
        T: AsRef<[Item]>,
    {
        let index = self.space.local_index_of(rank)?;
        self.data.as_ref().get(index)
    }

    /// Returns the value addressed by a coordinate in the rank space.
    pub fn get_coord<Item>(&self, coord: impl AsRef<[usize]>) -> Option<&Item>
    where
        T: AsRef<[Item]>,
    {
        self.get_rank(self.space.rank_of(coord)?)
    }

    /// Iterates visible ranks and values in compact order.
    pub fn iter<'a, Item: 'a>(&'a self) -> impl Iterator<Item = (Rank, &'a Item)> + 'a
    where
        T: AsRef<[Item]>,
    {
        self.space.iter_ranks().zip(self.data.as_ref().iter())
    }
}

impl<T, Item> View<Item> for CompactView<T>
where
    T: AsRef<[Item]>,
{
    fn space(&self) -> &RankSpace {
        &self.space
    }

    fn get_rank(&self, rank: Rank) -> Option<&Item> {
        CompactView::get_rank(self, rank)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (Rank, &'a Item)> + 'a
    where
        Item: 'a,
    {
        CompactView::iter(self)
    }
}

/// Error from a view transformation that can fail due to both view
/// shape and mapping logic.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ViewMapError<Error> {
    /// The view was invalid for the requested transformation.
    #[error(transparent)]
    View(#[from] ViewError),

    /// The user-provided mapping function failed.
    #[error("view map failed")]
    Map(Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Dim;
    use crate::Extent;
    use crate::RankMask;
    use crate::RankRect;

    fn host_gpu_space() -> RankSpace {
        let rect =
            RankRect::new(Extent::new(vec![Dim::new("host", 2), Dim::new("gpu", 4)]).unwrap())
                .unwrap();
        RankSpace::dense(rect).without(RankMask::ranks([Rank(5)]))
    }

    #[test]
    fn base_view_uses_base_rank_as_storage_index() {
        let space = host_gpu_space();
        let values = (0..8).map(|rank| rank * 10).collect::<Vec<_>>();
        let view = BaseView::new(space, values);

        assert_eq!(view.get_rank(Rank(4)), Some(&40));
        assert_eq!(view.get_rank(Rank(5)), None);
        assert_eq!(view.get_coord([1, 2]), Some(&60));
    }

    #[test]
    fn base_view_can_borrow_storage() {
        let space = host_gpu_space();
        let values = (0..8).map(|rank| rank * 10).collect::<Vec<_>>();
        let view = BaseView::new(space, values.as_slice());

        assert_eq!(view.get_rank(Rank(6)), Some(&60));
    }

    #[test]
    fn base_view_checked_requires_visible_ranks_to_be_in_bounds() {
        let space = host_gpu_space();
        let err = BaseView::new_checked(space, vec![0, 1, 2, 3]).unwrap_err();

        assert_eq!(
            err,
            ViewError::RankOutOfBounds {
                rank: Rank(4),
                len: 4
            }
        );
    }

    #[test]
    fn try_map_visible_wraps_user_error_as_map_variant() {
        // User-supplied fn returns `Err` for one visible rank.
        let space = host_gpu_space();
        let values = (0..8).map(|rank| rank * 10).collect::<Vec<_>>();
        let view = BaseView::new(space, values);

        let result = view.try_map_visible(|rank, _item: &usize| -> Result<usize, &'static str> {
            if rank == Rank(4) {
                Err("rank 4 not allowed")
            } else {
                Ok(rank.get())
            }
        });

        assert_eq!(result.unwrap_err(), ViewMapError::Map("rank 4 not allowed"));
    }

    #[test]
    fn try_map_visible_wraps_rank_out_of_bounds_as_view_variant() {
        // Storage too small for the visible ranks; the first OOB rank surfaces
        // as `ViewMapError::View(ViewError::RankOutOfBounds { ... })`.
        let space = host_gpu_space();
        let view = BaseView::new(space, vec![0usize, 1, 2, 3]);

        let result = view
            .try_map_visible(|_rank, item: &usize| -> Result<usize, &'static str> { Ok(*item) });

        assert_eq!(
            result.unwrap_err(),
            ViewMapError::View(ViewError::RankOutOfBounds {
                rank: Rank(4),
                len: 4,
            }),
        );
    }

    #[test]
    fn compact_view_uses_visible_rank_order() {
        let space = host_gpu_space();
        let values = vec!["r0", "r1", "r2", "r3", "r4", "r6", "r7"];
        let view = CompactView::new(space, values).unwrap();

        assert_eq!(view.get_rank(Rank(4)), Some(&"r4"));
        assert_eq!(view.get_rank(Rank(5)), None);
        assert_eq!(view.get_rank(Rank(6)), Some(&"r6"));
        assert_eq!(
            view.iter().collect::<Vec<_>>(),
            vec![
                (Rank(0), &"r0"),
                (Rank(1), &"r1"),
                (Rank(2), &"r2"),
                (Rank(3), &"r3"),
                (Rank(4), &"r4"),
                (Rank(6), &"r6"),
                (Rank(7), &"r7"),
            ]
        );
    }

    #[test]
    fn compact_view_checks_cardinality() {
        let space = host_gpu_space();
        let err = CompactView::new(space, vec![1, 2]).unwrap_err();

        assert_eq!(
            err,
            ViewError::InvalidCardinality {
                expected: 7,
                actual: 2
            }
        );
    }

    #[test]
    fn views_clone_wholesale_into_owned_storage() {
        let space = host_gpu_space();
        let values = vec!["r0", "r1", "r2", "r3", "r4", "r6", "r7"];
        let borrowed = CompactView::new(space, values.as_slice()).unwrap();
        let owned = borrowed.cloned();

        assert_eq!(owned.data(), &values);
    }

    #[test]
    fn compact_view_maps_visible_values() {
        let space = host_gpu_space();
        let values = vec![0, 1, 2, 3, 4, 6, 7];
        let view = CompactView::new(space, values).unwrap();
        let mapped = view.map(|value| value * 10);

        assert_eq!(
            mapped.iter().collect::<Vec<_>>(),
            vec![
                (Rank(0), &0),
                (Rank(1), &10),
                (Rank(2), &20),
                (Rank(3), &30),
                (Rank(4), &40),
                (Rank(6), &60),
                (Rank(7), &70),
            ]
        );
    }

    #[test]
    fn base_view_can_map_all_or_visible_storage() {
        let space = host_gpu_space();
        let values = (0..8).collect::<Vec<_>>();
        let view = BaseView::new(space, values);

        let all = view.map_all(|value| value * 10);
        assert_eq!(all.data(), &vec![0, 10, 20, 30, 40, 50, 60, 70]);

        let visible = view.map_visible(|rank, value| (rank, value * 10)).unwrap();
        assert_eq!(
            visible.iter().collect::<Vec<_>>(),
            vec![
                (Rank(0), &(Rank(0), 0)),
                (Rank(1), &(Rank(1), 10)),
                (Rank(2), &(Rank(2), 20)),
                (Rank(3), &(Rank(3), 30)),
                (Rank(4), &(Rank(4), 40)),
                (Rank(6), &(Rank(6), 60)),
                (Rank(7), &(Rank(7), 70)),
            ]
        );
    }

    #[test]
    fn generic_view_trait_reads_base_and_compact_views() {
        fn values<Item: Clone>(view: &impl View<Item>) -> Vec<(Rank, Item)> {
            view.iter()
                .map(|(rank, item)| (rank, item.clone()))
                .collect()
        }

        let base = BaseView::new(host_gpu_space(), (0..8).collect::<Vec<_>>());
        assert_eq!(
            values(&base),
            vec![
                (Rank(0), 0),
                (Rank(1), 1),
                (Rank(2), 2),
                (Rank(3), 3),
                (Rank(4), 4),
                (Rank(6), 6),
                (Rank(7), 7)
            ]
        );

        let compact = CompactView::new(host_gpu_space(), vec![0, 1, 2, 3, 4, 6, 7]).unwrap();
        assert_eq!(values(&compact), values(&base));
    }
}
