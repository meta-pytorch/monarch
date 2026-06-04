/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Affine and sparse coordinate spaces over ranks.
//!
//! A rank space is a small geometry object. It names a rectangular local
//! coordinate system, embeds that system into a flat base-rank coordinate
//! system, and can hide base ranks that are not defined.
//!
//! Intuitively, a [`Rank`] is an index into an underlying flat array.
//! `rankspace` does not own that array. It models the geometry that tells us
//! which array index a local coordinate refers to:
//!
//! ```text
//! local coordinate [host = 1, gpu = 2]
//!        |
//!        v
//! rank = 6
//!        |
//!        v
//! data[6]
//! ```
//!
//! This separation lets the same rank space act as pure geometry, as a set of
//! defined ranks, or as an indexing scheme for external storage through
//! [`view`].
//!
//! The core abstraction is an affine embedding:
//!
//! ```text
//! rank = offset + coord[0] * stride[0] + ... + coord[n - 1] * stride[n - 1]
//! ```
//!
//! This representation is compact. A large rectangular region is stored as one
//! [`Extent`], one [`Rank`] offset, and one stride per dimension. We do not need
//! to materialize every rank in the region:
//!
//! ```text
//! explicit ranks for [host: 2, gpu: 4]:
//!   [0, 1, 2, 3, 4, 5, 6, 7]
//!
//! affine representation:
//!   extent  = [host: 2, gpu: 4]
//!   offset  = 0
//!   strides = [4, 1]
//! ```
//!
//! It also makes common coordinate transforms cheap. Slicing, fixing a
//! dimension index, and re-indexing can usually be expressed by changing the
//! offset, extent, and strides while preserving the underlying base-rank
//! identity:
//!
//! ```text
//! operation                     metadata change
//! ---------                     ---------------
//! select gpu = 1.. step 2       offset += 1 * stride[gpu]
//!                               size[gpu] = 2
//!                               stride[gpu] *= 2
//!
//! fix host = 1                  offset += 1 * stride[host]
//!                               remove host from extent
//!                               remove stride[host]
//!
//! re-index a subspace           keep base ranks; change the local extent
//!                               and strides that project into them
//! ```
//!
//! [`Dim`] and [`Extent`] describe only the local coordinate shape. They name
//! dimensions and give their sizes, but they do not assign ranks:
//!
//! ```text
//! extent = [host: 2, gpu: 4]
//!
//! local coordinates:
//!           gpu
//!         0  1  2  3
//! host 0  .  .  .  .
//!      1  .  .  .  .
//! ```
//!
//! [`RankRect`] adds the affine embedding. A dense row-major rectangle with
//! `offset = 0` and `strides = [4, 1]` maps the same local coordinates to base
//! ranks:
//!
//! ```text
//! extent  = [host: 2, gpu: 4]
//! offset  = 0
//! strides = [4, 1]
//!
//!           gpu
//!         0  1  2  3
//! host 0  0  1  2  3
//!      1  4  5  6  7
//! ```
//!
//! Subspace operations preserve the meaning of base ranks. For example,
//! selecting every other `gpu` element starting at `1` produces a smaller
//! local coordinate system that still points into the original base ranks:
//!
//! ```text
//! base ranks:
//!           gpu
//!         0  1  2  3
//! host 0  0  1  2  3
//!      1  4  5  6  7
//!
//! select gpu = 1.. step 2
//!
//! extent  = [host: 2, gpu: 2]
//! offset  = 1
//! strides = [4, 2]
//!
//!           local gpu
//!             0  1
//! host 0      1  3
//!      1      5  7
//! ```
//!
//! Projection and embedding let you separate a reusable local pattern from the
//! concrete base ranks where that pattern happens to appear. [`RankRect::project`]
//! captures a rectangle as parent-local row-major indices. [`RankRect::embed`]
//! interprets such a captured pattern in a parent rectangle:
//!
//! ```text
//! project: concrete base ranks -> parent-local pattern
//! embed:   parent-local pattern -> concrete base ranks
//! ```
//!
//! This is useful for operations such as tiling. Suppose one parent maps local
//! indices to base ranks with `offset = 10` and `strides = [8, 2]`:
//!
//! ```text
//! parent A coordinates -> parent A base ranks
//!
//!           gpu
//!         0   1   2   3
//! host 0  10  12  14  16
//!      1  18  20  22  24
//! ```
//!
//! The right half of that rectangle is the concrete tile `{14, 16, 22, 24}`:
//!
//! ```text
//! tile in parent A base ranks
//!
//!           local gpu
//!           0   1
//! host 0    14  16
//!      1    22  24
//! ```
//!
//! Projecting the tile through parent A captures the shape as parent-local
//! indices `{2, 3, 6, 7}`:
//!
//! ```text
//! captured tile pattern
//!
//!           local gpu
//!           0  1
//! host 0    2  3
//!      1    6  7
//! ```
//!
//! That captured pattern can then be embedded into another compatible parent.
//! If parent B has `offset = 100` and `strides = [40, 5]`, the same tile
//! pattern resolves to different base ranks:
//!
//! ```text
//! parent B coordinates -> parent B base ranks
//!
//!           gpu
//!         0    1    2    3
//! host 0  100  105  110  115
//!      1  140  145  150  155
//!
//! captured tile pattern embedded in parent B
//!
//!           local gpu
//!           0    1
//! host 0    110  115
//!      1    150  155
//! ```
//!
//! The captured pattern is more than an extent: it includes offset and strides
//! in parent-local index space. This lets it represent affine slices, strided
//! tiles, and fixed-dimension subspaces. If a target parent cannot realize that
//! affine local-index pattern, embedding returns an error.
//!
//! [`RankMask`] is a set of ranks in base-rank coordinates. Masks are not
//! relative to a particular subspace or view. This keeps set operations about
//! ranks, while projection into local coordinates remains a view concern:
//!
//! ```text
//! mask = {5}
//!
//! base ranks:
//!           gpu
//!         0  1  2  3
//! host 0  0  1  2  3
//!      1  4  x  6  7
//! ```
//!
//! [`RankSpace`] is a visible space: a base [`RankRect`] minus base-rank
//! occlusions. A rank can be a valid array index but not a visible point in a
//! particular sparse space:
//!
//! ```text
//! base array indices:
//!   0 1 2 3 4 5 6 7
//!
//! visible ranks after masking rank 5:
//!   0 1 2 3 4   6 7
//! ```
//!
//! Visible ranks retain their base-rank identity, but they also have a compact
//! visible order:
//!
//! ```text
//! compact index: 0 1 2 3 4 5 6
//! visible rank:  0 1 2 3 4 6 7
//! ```
//!
//! Value containers are intentionally layered on top in [`view`]. The core
//! module computes ranks and visible membership; the `view` module decides how
//! those ranks address user data. A [`view::BaseView`] stores values by base
//! rank (`rank -> data[rank]`), while a [`view::CompactView`] stores values by
//! compact visible order (`rank -> visible_index -> data[visible_index]`).

pub mod strategy;
pub mod view;

#[cfg(test)]
mod proptests;

use std::collections::BTreeSet;
use std::collections::HashSet;
use std::num::NonZeroUsize;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// A flat rank in the base rank coordinate system.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
pub struct Rank(pub usize);

impl Rank {
    /// Returns the rank as a `usize`.
    pub fn get(self) -> usize {
        self.0
    }
}

impl From<usize> for Rank {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

/// A coordinate in a rank rectangle or rank space.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Coord(Vec<usize>);

impl Coord {
    /// Creates a coordinate from per-dimension indices.
    pub fn new(indices: Vec<usize>) -> Self {
        Self(indices)
    }

    /// Returns the coordinate indices.
    pub fn indices(&self) -> &[usize] {
        &self.0
    }

    /// Consumes the coordinate into its indices.
    pub fn into_indices(self) -> Vec<usize> {
        self.0
    }
}

impl From<Vec<usize>> for Coord {
    fn from(indices: Vec<usize>) -> Self {
        Self::new(indices)
    }
}

impl<const N: usize> From<[usize; N]> for Coord {
    fn from(indices: [usize; N]) -> Self {
        Self::new(indices.into())
    }
}

/// A named rank-space dimension.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dim {
    name: String,
    size: usize,
}

impl Dim {
    /// Creates a dimension with a name and size.
    pub fn new(name: impl Into<String>, size: usize) -> Self {
        Self {
            name: name.into(),
            size,
        }
    }

    /// Returns the dimension name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dimension size.
    pub fn size(&self) -> usize {
        self.size
    }
}

/// A named coordinate extent.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Extent(Vec<Dim>);

impl Extent {
    /// Creates an extent from named dimensions.
    pub fn new(dims: Vec<Dim>) -> Result<Self, RankSpaceError> {
        validate_dims(&dims)?;
        Ok(Self(dims))
    }

    /// Creates an extent from sizes and generated dimension names.
    pub fn from_sizes(sizes: impl Into<Vec<usize>>) -> Result<Self, RankSpaceError> {
        Self::new(
            sizes
                .into()
                .into_iter()
                .enumerate()
                .map(|(dim, size)| Dim::new(format!("d{dim}"), size))
                .collect(),
        )
    }

    /// Returns the dimensions.
    pub fn dims(&self) -> &[Dim] {
        &self.0
    }

    /// Consumes the extent into its dimensions.
    pub fn into_dims(self) -> Vec<Dim> {
        self.0
    }

    /// Returns the dimension sizes.
    pub fn sizes(&self) -> impl Iterator<Item = usize> + '_ {
        self.0.iter().map(Dim::size)
    }

    /// Returns the number of dimensions.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the extent has no dimensions.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of coordinate points in a dense rectangle with this extent.
    pub fn cardinality(&self) -> usize {
        self.sizes().product()
    }

    /// Returns the position of a dimension by name.
    pub fn dim_index(&self, name: &str) -> Option<usize> {
        self.0.iter().position(|dim| dim.name() == name)
    }

    fn remove_dim(&mut self, index: usize) {
        self.0.remove(index);
    }

    fn set_dim_size(&mut self, index: usize, size: usize) {
        self.0[index].size = size;
    }
}

/// A range along one dimension.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DimRange {
    start: usize,
    end: Option<usize>,
    step: NonZeroUsize,
}

impl DimRange {
    /// Creates a dimension range.
    pub fn new(start: usize, end: Option<usize>, step: NonZeroUsize) -> Self {
        Self { start, end, step }
    }

    /// Creates a dimension range with an explicit `usize` step.
    pub fn with_step(
        start: usize,
        end: Option<usize>,
        step: usize,
    ) -> Result<Self, RankSpaceError> {
        let Some(step) = NonZeroUsize::new(step) else {
            return Err(RankSpaceError::ZeroStep);
        };
        Ok(Self::new(start, end, step))
    }

    /// Returns the first selected index.
    pub fn start(&self) -> usize {
        self.start
    }

    /// Returns the exclusive end, if bounded.
    pub fn end(&self) -> Option<usize> {
        self.end
    }

    /// Returns the selection step.
    pub fn step(&self) -> NonZeroUsize {
        self.step
    }

    fn resolve(&self, dim_size: usize) -> ResolvedDimRange {
        let start = self.start.min(dim_size);
        let end = self.end.unwrap_or(dim_size).min(dim_size);
        let step = self.step.get();
        let len = if end <= start {
            0
        } else {
            (end - start).div_ceil(step)
        };
        ResolvedDimRange { start, len, step }
    }
}

impl From<usize> for DimRange {
    fn from(index: usize) -> Self {
        Self::new(
            index,
            Some(
                index
                    .checked_add(1)
                    .expect("dimension index should fit usize"),
            ),
            NonZeroUsize::MIN,
        )
    }
}

impl From<std::ops::Range<usize>> for DimRange {
    fn from(range: std::ops::Range<usize>) -> Self {
        Self::new(range.start, Some(range.end), NonZeroUsize::MIN)
    }
}

impl From<std::ops::RangeInclusive<usize>> for DimRange {
    fn from(range: std::ops::RangeInclusive<usize>) -> Self {
        Self::new(
            *range.start(),
            Some(
                range
                    .end()
                    .checked_add(1)
                    .expect("dimension range end should fit usize"),
            ),
            NonZeroUsize::MIN,
        )
    }
}

impl From<std::ops::RangeFrom<usize>> for DimRange {
    fn from(range: std::ops::RangeFrom<usize>) -> Self {
        Self::new(range.start, None, NonZeroUsize::MIN)
    }
}

#[derive(Debug, Clone, Copy)]
struct ResolvedDimRange {
    start: usize,
    len: usize,
    step: usize,
}

/// Errors produced by rank-space construction and operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RankSpaceError {
    /// The number of dimensions and strides differ.
    #[error("dimension/stride mismatch: {dims} != {strides}")]
    DimMismatch { dims: usize, strides: usize },

    /// The same dimension name was provided more than once.
    #[error("duplicate dimension name `{name}`")]
    DuplicateDim { name: String },

    /// The requested dimension does not exist.
    #[error("unknown dimension `{name}`")]
    UnknownDim { name: String },

    /// The requested dimension index is out of bounds.
    #[error("index {index} out of range for dimension `{dim}` of size {size}")]
    DimIndexOutOfRange {
        dim: String,
        index: usize,
        size: usize,
    },

    /// The affine strides do not define a rectangular rank space.
    #[error("nonrectangular strides")]
    NonrectangularStrides,

    /// Two non-unit dimensions use the same stride.
    #[error("overlapping strides")]
    OverlappingStrides,

    /// A stride overlaps the previous coordinate space.
    #[error("stride {stride} is smaller than previous coordinate space {space}")]
    StrideTooSmall { stride: usize, space: usize },

    /// Rank arithmetic overflowed.
    #[error("rank arithmetic overflow")]
    RankArithmeticOverflow,

    /// The requested embedding cannot be represented as one rank rectangle.
    #[error("incompatible embedding")]
    IncompatibleEmbedding,

    /// Dimension ranges must have nonzero steps.
    #[error("dimension range step must be nonzero")]
    ZeroStep,
}

/// A coarse half-open interval over base ranks.
///
/// `end == None` means the interval includes ranks through `usize::MAX`;
/// this is needed because one-past-`usize::MAX` cannot be represented as a
/// [`Rank`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RankBounds {
    start: Rank,
    end: Option<Rank>,
}

impl RankBounds {
    /// Creates non-empty base-rank bounds.
    pub fn new(start: Rank, end: Option<Rank>) -> Option<Self> {
        if end.is_some_and(|end| end <= start) {
            return None;
        }
        Some(Self { start, end })
    }

    /// Returns the inclusive lower bound.
    pub fn start(&self) -> Rank {
        self.start
    }

    /// Returns the exclusive upper bound, if representable.
    pub fn end(&self) -> Option<Rank> {
        self.end
    }

    /// Returns true if these bounds overlap.
    pub fn overlaps(&self, other: &Self) -> bool {
        let self_starts_before_other_ends = other.end.is_none_or(|end| self.start < end);
        let other_starts_before_self_ends = self.end.is_none_or(|end| other.start < end);
        self_starts_before_other_ends && other_starts_before_self_ends
    }

    /// Returns true if these bounds fully contain `other`.
    pub fn contains(&self, other: &Self) -> bool {
        self.start <= other.start
            && match (self.end, other.end) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(self_end), Some(other_end)) => other_end <= self_end,
            }
    }
}

/// A dense affine rectangular embedding into base rank space.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RankRect {
    extent: Extent,
    offset: Rank,
    strides: Vec<usize>,
}

impl RankRect {
    /// Creates a dense row-major rank rectangle with offset zero.
    pub fn new(extent: Extent) -> Result<Self, RankSpaceError> {
        let strides = row_major_strides(extent.sizes());
        Self::affine(extent, Rank(0), strides)
    }

    /// Creates a rank rectangle from sizes and generated dimension names.
    pub fn from_sizes(sizes: impl Into<Vec<usize>>) -> Result<Self, RankSpaceError> {
        Self::new(Extent::from_sizes(sizes)?)
    }

    /// Creates a rank rectangle with explicit offset and strides.
    pub fn affine(
        extent: Extent,
        offset: impl Into<Rank>,
        strides: Vec<usize>,
    ) -> Result<Self, RankSpaceError> {
        validate_strides(&extent, &strides)?;
        Ok(Self {
            extent,
            offset: offset.into(),
            strides,
        })
    }

    /// Returns the coordinate extent.
    pub fn extent(&self) -> &Extent {
        &self.extent
    }

    /// Returns the dimensions.
    pub fn dims(&self) -> &[Dim] {
        self.extent.dims()
    }

    /// Returns the base rank of coordinate zero.
    pub fn offset(&self) -> Rank {
        self.offset
    }

    /// Returns the affine strides.
    pub fn strides(&self) -> &[usize] {
        &self.strides
    }

    /// Returns the number of dimensions.
    pub fn ndim(&self) -> usize {
        self.extent.len()
    }

    /// Returns the number of ranks in the rectangle.
    pub fn cardinality(&self) -> usize {
        self.extent.cardinality()
    }

    /// Returns true if the rectangle contains no ranks.
    pub fn is_empty(&self) -> bool {
        self.cardinality() == 0
    }

    /// Returns the position of a dimension by name.
    pub fn dim_index(&self, name: &str) -> Option<usize> {
        self.extent.dim_index(name)
    }

    /// Converts a local coordinate into a base rank.
    ///
    /// # See also
    ///
    /// [`Self::coord_of`] is the inverse: if
    /// `self.rank_of(coord) == Some(rank)`, then `self.coord_of(rank)`
    /// returns the same coordinate.
    pub fn rank_of(&self, coord: impl AsRef<[usize]>) -> Option<Rank> {
        let coord = coord.as_ref();
        if coord.len() != self.extent.len() {
            return None;
        }

        coord
            .iter()
            .zip(self.extent.dims())
            .zip(&self.strides)
            .try_fold(self.offset.0, |rank, ((index, dim), stride)| {
                if *index >= dim.size() {
                    return None;
                }
                rank.checked_add(index.checked_mul(*stride)?)
            })
            .map(Rank)
    }

    /// Converts a base rank into a local coordinate.
    ///
    /// # See also
    ///
    /// [`Self::rank_of`] is the inverse: if
    /// `self.coord_of(rank) == Some(coord)`, then
    /// `self.rank_of(coord.indices()) == Some(rank)`.
    pub fn coord_of(&self, rank: Rank) -> Option<Coord> {
        if self.is_empty() {
            return None;
        }
        if self.extent.is_empty() {
            return (rank == self.offset).then(|| Coord::new(Vec::new()));
        }

        let mut rest = rank.0.checked_sub(self.offset.0)?;
        let mut coord = vec![0; self.extent.len()];
        let mut order = self
            .strides
            .iter()
            .copied()
            .zip(self.extent.sizes().enumerate())
            .collect::<Vec<_>>();
        order.sort_by_key(|&(stride, _)| stride);

        for (stride, (dim, size)) in order.into_iter().rev() {
            let (index, next_rest) = if size > 1 {
                (rest / stride, rest % stride)
            } else {
                (0, rest)
            };
            if index >= size {
                return None;
            }
            coord[dim] = index;
            rest = next_rest;
        }

        (rest == 0).then(|| Coord::new(coord))
    }

    /// Returns true if the rectangle contains the base rank.
    pub fn contains_rank(&self, rank: Rank) -> bool {
        self.coord_of(rank).is_some()
    }

    /// Converts a local row-major index into a base rank.
    ///
    /// # See also
    ///
    /// [`Self::local_index_of`] is the inverse: if
    /// `self.rank_at(index) == Some(rank)`, then
    /// `self.local_index_of(rank) == Some(index)`.
    pub fn rank_at(&self, local_index: usize) -> Option<Rank> {
        let coord = self.coord_at(local_index)?;
        self.rank_of(coord.indices())
    }

    /// Converts a base rank into its local row-major index.
    ///
    /// # See also
    ///
    /// [`Self::rank_at`] is the inverse: if
    /// `self.local_index_of(rank) == Some(index)`, then
    /// `self.rank_at(index) == Some(rank)`.
    pub fn local_index_of(&self, rank: Rank) -> Option<usize> {
        let coord = self.coord_of(rank)?;
        Some(row_major_index(coord.indices(), self.extent.sizes()))
    }

    /// Iterates ranks in local row-major order.
    pub fn iter_ranks(&self) -> RankRectRanks<'_> {
        RankRectRanks {
            rect: self,
            next: 0,
        }
    }

    /// Returns the coarse half-open base-rank interval covering this rectangle.
    ///
    /// Strided rectangles may not contain every rank inside these bounds.
    pub fn rank_bounds(&self) -> Option<RankBounds> {
        if self.is_empty() {
            return None;
        }

        let max_delta = self.extent.dims().iter().zip(&self.strides).try_fold(
            0usize,
            |delta, (dim, stride)| {
                let dim_delta = dim.size().checked_sub(1)?.checked_mul(*stride)?;
                delta.checked_add(dim_delta)
            },
        )?;
        let max = self.offset.0.checked_add(max_delta)?;
        RankBounds::new(self.offset, max.checked_add(1).map(Rank))
    }

    /// Returns whether this rectangle and `other` share any base rank.
    ///
    /// This is an exact set operation over the ranks produced by each affine
    /// rectangle. It uses bounds and stride-congruence pruning before splitting
    /// sparse rectangles recursively.
    pub fn intersects(&self, other: &Self) -> bool {
        fn recurse(left: &RankRect, right: &RankRect) -> bool {
            let (Some(left_bounds), Some(right_bounds)) = (left.rank_bounds(), right.rank_bounds())
            else {
                return false;
            };

            if !left_bounds.overlaps(&right_bounds) {
                return false;
            }

            if !left.congruence_may_overlap(right) {
                return false;
            }

            if left.is_contiguous() && right.is_contiguous() {
                return true;
            }

            if left.cardinality() == 1 {
                return right.contains_rank(left.offset);
            }

            if right.cardinality() == 1 {
                return left.contains_rank(right.offset);
            }

            if left.cardinality() >= right.cardinality() {
                if let Some((first, second)) = left.split_largest_dim() {
                    recurse(&first, right) || recurse(&second, right)
                } else {
                    right.contains_rank(left.offset)
                }
            } else if let Some((first, second)) = right.split_largest_dim() {
                recurse(left, &first) || recurse(left, &second)
            } else {
                left.contains_rank(right.offset)
            }
        }

        recurse(self, other)
    }

    /// Returns whether every rank in `other` is also contained in this rectangle.
    ///
    /// This is an exact set operation over the ranks produced by each affine
    /// rectangle. It avoids rank materialization in common cases, but may split
    /// `other` recursively for sparse rectangles.
    pub fn contains_rect(&self, other: &Self) -> bool {
        fn recurse(outer: &RankRect, inner: &RankRect) -> bool {
            if inner.is_empty() {
                return true;
            }
            if outer.is_empty() {
                return false;
            }

            let (Some(outer_bounds), Some(inner_bounds)) =
                (outer.rank_bounds(), inner.rank_bounds())
            else {
                return false;
            };

            if !outer_bounds.contains(&inner_bounds) {
                return false;
            }

            if outer.is_contiguous() {
                return true;
            }

            if inner.cardinality() == 1 {
                return outer.contains_rank(inner.offset);
            }

            let Some((first, second)) = inner.split_largest_dim() else {
                return outer.contains_rank(inner.offset);
            };
            recurse(outer, &first) && recurse(outer, &second)
        }

        recurse(self, other)
    }

    /// Embeds `local` through this rectangle.
    ///
    /// Ranks produced by `local` are interpreted as local row-major indices into
    /// `self`. The returned rectangle maps `local`'s coordinates directly into
    /// `self`'s base-rank coordinate system.
    ///
    /// For an affine parent rectangle, parent local indices and final base
    /// ranks are different coordinate systems:
    ///
    /// ```text
    /// parent coordinates -> parent local indices
    ///
    ///           gpu
    ///         0  1  2  3
    /// host 0  0  1  2  3
    ///      1  4  5  6  7
    ///
    /// parent coordinates -> final base ranks
    ///
    ///           gpu
    ///         0   1   2   3
    /// host 0  10  12  14  16
    ///      1  18  20  22  24
    /// ```
    ///
    /// a local rectangle can name parent local indices rather than final base
    /// ranks:
    ///
    /// ```text
    /// local coordinates -> parent local indices
    ///
    ///               local gpu
    ///               0  1
    /// local host 0  1  3
    ///            1  5  7
    /// ```
    ///
    /// Embedding composes those two mappings:
    ///
    /// ```text
    /// local coordinates -> final base ranks
    ///
    ///               local gpu
    ///               0  1
    /// local host 0  12  16
    ///            1  20  24
    /// ```
    ///
    /// For example, local coordinate `[host = 1, gpu = 0]` first selects
    /// parent local index `5`, then resolves to final base rank `20`.
    pub fn embed(&self, local: &Self) -> Result<Self, RankSpaceError> {
        if local.is_empty() {
            return Self::new(local.extent.clone());
        }

        let base_offset = self
            .rank_at(local.offset.get())
            .ok_or(RankSpaceError::IncompatibleEmbedding)?;
        let strides = local
            .extent
            .dims()
            .iter()
            .zip(&local.strides)
            .map(|(dim, stride)| {
                if dim.size() <= 1 {
                    return Ok(1);
                }
                let local_rank = local
                    .offset
                    .get()
                    .checked_add(*stride)
                    .ok_or(RankSpaceError::RankArithmeticOverflow)?;
                let base_rank = self
                    .rank_at(local_rank)
                    .ok_or(RankSpaceError::IncompatibleEmbedding)?;
                base_rank
                    .get()
                    .checked_sub(base_offset.get())
                    .ok_or(RankSpaceError::IncompatibleEmbedding)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let embedded = Self::affine(local.extent.clone(), base_offset, strides)
            .map_err(|_| RankSpaceError::IncompatibleEmbedding)?;

        if self.is_contiguous() && self.contains_local_indices(local) {
            return Ok(embedded);
        }

        for local_index in 0..local.cardinality() {
            let coord = local
                .coord_at(local_index)
                .ok_or(RankSpaceError::IncompatibleEmbedding)?;
            let local_rank = local
                .rank_of(coord.indices())
                .ok_or(RankSpaceError::IncompatibleEmbedding)?;
            let expected = self
                .rank_at(local_rank.get())
                .ok_or(RankSpaceError::IncompatibleEmbedding)?;
            if embedded.rank_of(coord.indices()) != Some(expected) {
                return Err(RankSpaceError::IncompatibleEmbedding);
            }
        }

        Ok(embedded)
    }

    /// Projects `rect` into this rectangle's local row-major index space.
    ///
    /// This is the reverse coordinate transform of [`RankRect::embed`]. Ranks
    /// produced by `rect` are interpreted as base ranks in the same coordinate
    /// system as `self`. The returned rectangle maps `rect`'s coordinates to
    /// local row-major indices into `self`.
    ///
    /// Every rank in `rect` must be contained in `self`, and the inverse image
    /// must be representable as one affine rectangle.
    ///
    /// Projection keeps `rect`'s coordinate system, but changes what its ranks
    /// mean. Starting with the same parent:
    ///
    /// ```text
    /// parent coordinates -> parent local indices
    ///
    ///           gpu
    ///         0  1  2  3
    /// host 0  0  1  2  3
    ///      1  4  5  6  7
    ///
    /// parent coordinates -> final base ranks
    ///
    ///           gpu
    ///         0   1   2   3
    /// host 0  10  12  14  16
    ///      1  18  20  22  24
    /// ```
    ///
    /// and an extracted rectangle in base-rank coordinates:
    ///
    /// ```text
    /// extracted coordinates -> base ranks
    ///
    ///           local gpu
    ///           0  1
    /// host 0    12  16
    ///      1    20  24
    /// ```
    ///
    /// projection returns a rectangle with the same extracted coordinates, but
    /// whose ranks are parent local row-major indices:
    ///
    /// ```text
    /// extracted coordinates -> parent local indices
    ///
    ///           local gpu
    ///           0  1
    /// host 0    1  3
    ///      1    5  7
    /// ```
    ///
    /// For example, extracted coordinate `[host = 1, gpu = 0]` has base rank
    /// `20`; projected through the parent, that rank becomes parent local index
    /// `5`, which is parent coordinate `[host = 1, gpu = 1]`.
    pub fn project(&self, rect: &Self) -> Result<Self, RankSpaceError> {
        if rect.is_empty() {
            return Self::new(rect.extent.clone());
        }

        let offset = Rank(
            self.local_index_of(rect.offset)
                .ok_or(RankSpaceError::IncompatibleEmbedding)?,
        );
        let strides = rect
            .extent
            .dims()
            .iter()
            .zip(&rect.strides)
            .map(|(dim, stride)| {
                if dim.size() <= 1 {
                    return Ok(1);
                }
                let rank = rect
                    .offset
                    .get()
                    .checked_add(*stride)
                    .map(Rank)
                    .ok_or(RankSpaceError::RankArithmeticOverflow)?;
                let local_index = self
                    .local_index_of(rank)
                    .ok_or(RankSpaceError::IncompatibleEmbedding)?;
                local_index
                    .checked_sub(offset.get())
                    .ok_or(RankSpaceError::IncompatibleEmbedding)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let projected = Self::affine(rect.extent.clone(), offset, strides)
            .map_err(|_| RankSpaceError::IncompatibleEmbedding)?;

        if self.is_contiguous() && self.contains_rect(rect) {
            return Ok(projected);
        }

        for local_index in 0..rect.cardinality() {
            let coord = rect
                .coord_at(local_index)
                .ok_or(RankSpaceError::IncompatibleEmbedding)?;
            let rank = rect
                .rank_of(coord.indices())
                .ok_or(RankSpaceError::IncompatibleEmbedding)?;
            let expected = self
                .local_index_of(rank)
                .ok_or(RankSpaceError::IncompatibleEmbedding)?;
            if projected.rank_of(coord.indices()) != Some(Rank(expected)) {
                return Err(RankSpaceError::IncompatibleEmbedding);
            }
        }

        Ok(projected)
    }

    /// Selects a dimension while preserving dimensionality.
    pub fn select(&self, dim: &str, range: impl Into<DimRange>) -> Result<Self, RankSpaceError> {
        let dim_index = self
            .dim_index(dim)
            .ok_or_else(|| RankSpaceError::UnknownDim {
                name: dim.to_string(),
            })?;
        let resolved = range.into().resolve(self.extent.dims()[dim_index].size());

        let mut extent = self.extent.clone();
        let mut strides = self.strides.clone();
        let offset_delta = resolved
            .start
            .checked_mul(strides[dim_index])
            .ok_or(RankSpaceError::RankArithmeticOverflow)?;
        let offset = Rank(
            self.offset
                .0
                .checked_add(offset_delta)
                .ok_or(RankSpaceError::RankArithmeticOverflow)?,
        );
        extent.set_dim_size(dim_index, resolved.len);
        strides[dim_index] = strides[dim_index]
            .checked_mul(resolved.step)
            .ok_or(RankSpaceError::RankArithmeticOverflow)?;
        Self::affine(extent, offset, strides)
    }

    /// Fixes a dimension to one index and removes it from the coordinate space.
    pub fn fix(&self, dim: &str, index: usize) -> Result<Self, RankSpaceError> {
        let dim_index = self
            .dim_index(dim)
            .ok_or_else(|| RankSpaceError::UnknownDim {
                name: dim.to_string(),
            })?;
        let dim_size = self.extent.dims()[dim_index].size();
        if index >= dim_size {
            return Err(RankSpaceError::DimIndexOutOfRange {
                dim: dim.to_string(),
                index,
                size: dim_size,
            });
        }

        let offset_delta = index
            .checked_mul(self.strides[dim_index])
            .ok_or(RankSpaceError::RankArithmeticOverflow)?;
        let offset = Rank(
            self.offset
                .0
                .checked_add(offset_delta)
                .ok_or(RankSpaceError::RankArithmeticOverflow)?,
        );
        let mut extent = self.extent.clone();
        let mut strides = self.strides.clone();
        extent.remove_dim(dim_index);
        strides.remove(dim_index);
        Self::affine(extent, offset, strides)
    }

    fn is_contiguous(&self) -> bool {
        self.strides == row_major_strides(self.extent.sizes())
    }

    fn contains_local_indices(&self, rect: &Self) -> bool {
        rect.rank_bounds()
            .and_then(|bounds| bounds.end())
            .is_some_and(|end| end.get() <= self.cardinality())
    }

    fn active_stride_gcd(&self) -> usize {
        self.extent
            .dims()
            .iter()
            .zip(&self.strides)
            .filter(|(dim, _)| dim.size() > 1)
            .map(|(_, stride)| *stride)
            .fold(0, gcd)
    }

    fn congruence_may_overlap(&self, other: &Self) -> bool {
        let modulus = gcd(self.active_stride_gcd(), other.active_stride_gcd());
        if modulus == 0 {
            return self.offset == other.offset;
        }
        self.offset
            .get()
            .abs_diff(other.offset.get())
            .is_multiple_of(modulus)
    }

    fn split_largest_dim(&self) -> Option<(Self, Self)> {
        let dim = self
            .extent
            .dims()
            .iter()
            .enumerate()
            .filter(|(_, dim)| dim.size() > 1)
            .max_by_key(|(_, dim)| dim.size())
            .map(|(_, dim)| dim)?;
        let mid = dim.size() / 2;
        let first = self
            .select(dim.name(), 0..mid)
            .expect("splitting valid rectangle should produce a valid first half");
        let second = self
            .select(dim.name(), mid..dim.size())
            .expect("splitting valid rectangle should produce a valid second half");
        Some((first, second))
    }

    fn coord_at(&self, local_index: usize) -> Option<Coord> {
        let cardinality = self.cardinality();
        if local_index >= cardinality {
            return None;
        }

        let mut rest = local_index;
        let mut coord = vec![0; self.extent.len()];
        for (index, dim) in self.extent.dims().iter().enumerate().rev() {
            coord[index] = rest % dim.size();
            rest /= dim.size();
        }
        Some(Coord::new(coord))
    }
}

impl<'a> IntoIterator for &'a RankRect {
    type Item = Rank;
    type IntoIter = RankRectRanks<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_ranks()
    }
}

/// Iterator over ranks in a [`RankRect`].
pub struct RankRectRanks<'a> {
    rect: &'a RankRect,
    next: usize,
}

impl Iterator for RankRectRanks<'_> {
    type Item = Rank;

    fn next(&mut self) -> Option<Self::Item> {
        let rank = self.rect.rank_at(self.next)?;
        self.next += 1;
        Some(rank)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rect.cardinality().saturating_sub(self.next);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RankRectRanks<'_> {}

/// A set of ranks in base-rank coordinates.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RankMask {
    /// Contains no ranks.
    Empty,
    /// Contains every rank in a dense rank rectangle.
    Rect(RankRect),
    /// Contains explicit base ranks.
    Ranks(BTreeSet<Rank>),
    /// Contains the union of all child masks.
    Union(Vec<RankMask>),
}

impl RankMask {
    /// Returns an empty mask.
    pub fn empty() -> Self {
        Self::Empty
    }

    /// Returns a mask containing one rank rectangle.
    pub fn rect(rect: RankRect) -> Self {
        Self::Rect(rect)
    }

    /// Returns a mask containing explicit ranks.
    pub fn ranks(ranks: impl IntoIterator<Item = Rank>) -> Self {
        Self::Ranks(ranks.into_iter().collect())
    }

    /// Returns a mask containing the union of child masks.
    pub fn union(masks: impl IntoIterator<Item = RankMask>) -> Self {
        let masks = masks
            .into_iter()
            .filter(|mask| !matches!(mask, RankMask::Empty))
            .collect::<Vec<_>>();
        match masks.len() {
            0 => Self::Empty,
            1 => masks.into_iter().next().expect("one mask exists"),
            _ => Self::Union(masks),
        }
    }

    /// Returns true if this mask contains the base rank.
    pub fn contains(&self, rank: Rank) -> bool {
        match self {
            RankMask::Empty => false,
            RankMask::Rect(rect) => rect.contains_rank(rank),
            RankMask::Ranks(ranks) => ranks.contains(&rank),
            RankMask::Union(masks) => masks.iter().any(|mask| mask.contains(rank)),
        }
    }

    /// Returns whether this mask and `rect` share any base rank.
    pub fn intersects_rect(&self, rect: &RankRect) -> bool {
        match self {
            RankMask::Empty => false,
            RankMask::Rect(mask_rect) => mask_rect.intersects(rect),
            RankMask::Ranks(ranks) => ranks.iter().any(|rank| rect.contains_rank(*rank)),
            RankMask::Union(masks) => masks.iter().any(|mask| mask.intersects_rect(rect)),
        }
    }

    /// Returns whether every rank in `rect` is contained in this mask.
    pub fn contains_rect(&self, rect: &RankRect) -> bool {
        if rect.is_empty() {
            return true;
        }
        match self {
            RankMask::Empty => false,
            RankMask::Rect(mask_rect) => mask_rect.contains_rect(rect),
            RankMask::Ranks(ranks) => rect.iter_ranks().all(|rank| ranks.contains(&rank)),
            RankMask::Union(masks) => {
                if masks.iter().any(|mask| mask.contains_rect(rect)) {
                    return true;
                }
                if rect.cardinality() == 1 {
                    return self.contains(rect.offset());
                }
                let Some((first, second)) = rect.split_largest_dim() else {
                    return self.contains(rect.offset());
                };
                self.contains_rect(&first) && self.contains_rect(&second)
            }
        }
    }

    /// Returns whether this mask and `other` share any base rank.
    pub fn intersects_mask(&self, other: &Self) -> bool {
        match other {
            RankMask::Empty => false,
            RankMask::Rect(rect) => self.intersects_rect(rect),
            RankMask::Ranks(ranks) => ranks.iter().any(|rank| self.contains(*rank)),
            RankMask::Union(masks) => masks.iter().any(|mask| self.intersects_mask(mask)),
        }
    }

    /// Returns whether every rank in `other` is contained in this mask.
    pub fn contains_mask(&self, other: &Self) -> bool {
        match other {
            RankMask::Empty => true,
            RankMask::Rect(rect) => self.contains_rect(rect),
            RankMask::Ranks(ranks) => ranks.iter().all(|rank| self.contains(*rank)),
            RankMask::Union(masks) => masks.iter().all(|mask| self.contains_mask(mask)),
        }
    }
}

/// A possibly sparse rank space: a dense base rectangle minus occluded ranks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RankSpace {
    base: RankRect,
    occlusion: RankMask,
}

impl RankSpace {
    /// Creates a rank space with no occlusions.
    pub fn dense(base: RankRect) -> Self {
        Self {
            base,
            occlusion: RankMask::Empty,
        }
    }

    /// Creates a rank space with base-rank occlusions.
    pub fn sparse(base: RankRect, occlusion: RankMask) -> Self {
        Self { base, occlusion }
    }

    /// Returns the dense base rectangle.
    pub fn base(&self) -> &RankRect {
        &self.base
    }

    /// Returns the base-rank occlusion mask.
    pub fn occlusion(&self) -> &RankMask {
        &self.occlusion
    }

    /// Returns a new rank space with an additional base-rank occlusion mask.
    pub fn without(mut self, mask: RankMask) -> Self {
        self.occlusion = RankMask::union([self.occlusion, mask]);
        self
    }

    /// Returns the number of visible ranks.
    pub fn cardinality(&self) -> usize {
        self.iter_ranks().count()
    }

    /// Returns true if the space contains no visible ranks.
    pub fn is_empty(&self) -> bool {
        self.iter_ranks().next().is_none()
    }

    /// Returns the coarse half-open base-rank interval covering visible ranks.
    ///
    /// This trims occluded ranks from the beginning and end of the base
    /// rectangle's rank order. The result is still coarse for strided spaces:
    /// not every rank inside the returned interval is necessarily visible.
    pub fn rank_bounds(&self) -> Option<RankBounds> {
        let bounds = self.base.rank_bounds()?;
        let start = bounds.start();
        let end_rank = bounds_end_rank(bounds);

        let min = if self.occlusion.contains(start) {
            min_visible_rank(&self.base, &self.occlusion)?
        } else {
            start
        };
        let max = if self.occlusion.contains(end_rank) {
            max_visible_rank(&self.base, &self.occlusion)?
        } else {
            end_rank
        };

        RankBounds::new(min, max.get().checked_add(1).map(Rank))
    }

    /// Returns true if the base rank is visible in this rank space.
    pub fn contains_rank(&self, rank: Rank) -> bool {
        self.base.contains_rank(rank) && !self.occlusion.contains(rank)
    }

    /// Returns whether this space and `other` share any visible base rank.
    ///
    /// This is an exact set operation over visible ranks:
    ///
    /// ```text
    /// exists rank r:
    ///   r in self.base
    ///   r in other.base
    ///   r not in self.occlusion
    ///   r not in other.occlusion
    /// ```
    ///
    /// Occlusions from both spaces are unioned before searching for a witness.
    pub fn intersects(&self, other: &Self) -> bool {
        let occlusion = RankMask::union([self.occlusion.clone(), other.occlusion.clone()]);
        rects_intersect_outside_mask(&self.base, &other.base, &occlusion)
    }

    /// Returns whether every visible rank in `other` is also visible in `self`.
    ///
    /// This is an exact set operation:
    ///
    /// ```text
    /// other.base - other.occlusion <= self.base - self.occlusion
    /// ```
    ///
    /// Equivalently, every rank in `other.base` that is not hidden by
    /// `other.occlusion` must be contained in `self.base`, and none of those
    /// ranks may be hidden by `self.occlusion`.
    pub fn contains_space(&self, other: &Self) -> bool {
        self.contains_visible_rect(&other.base, &other.occlusion)
    }

    /// Embeds `local` through this space.
    ///
    /// `local.base` is embedded through `self.base` using [`RankRect::embed`].
    /// Ranks hidden by `local.occlusion` are first interpreted as local
    /// row-major indices into `self.base`, then projected into final base-rank
    /// coordinates. The resulting occlusion is the union of those embedded
    /// local occlusions and `self.occlusion`.
    ///
    /// Extra occlusions outside the resulting base rectangle are harmless:
    /// occlusions are base-rank sets, and only ranks inside the returned base
    /// rectangle can affect visibility.
    ///
    /// Continuing the dense example from [`RankRect::embed`], suppose the
    /// parent hides base rank `20`, and the local space hides parent local
    /// index `3`:
    ///
    /// ```text
    /// parent visible base ranks
    ///
    ///           gpu
    ///         0   1   2   3
    /// host 0  10  12  14  16
    ///      1  18  x   22  24
    ///
    /// local coordinates -> parent local indices
    ///
    ///               local gpu
    ///               0  1
    /// local host 0  1  x
    ///            1  5  7
    /// ```
    ///
    /// Embedding projects the local occlusion from parent local index `3` to
    /// base rank `16`, then unions it with the parent occlusion at base rank
    /// `20`. The returned space has base rectangle ranks `{12, 16, 20, 24}`
    /// and visible ranks `{12, 24}`.
    pub fn embed(&self, local: &Self) -> Result<Self, RankSpaceError> {
        let base = self.base.embed(&local.base)?;
        let local_occlusion = embed_occlusion(&self.base, &local.base, &local.occlusion)?;
        Ok(Self::sparse(
            base,
            RankMask::union([self.occlusion.clone(), local_occlusion]),
        ))
    }

    /// Projects `space` into this space's local row-major index space.
    ///
    /// This is the reverse coordinate transform of [`RankSpace::embed`].
    /// `space.base` is expressed in `self.base`'s local row-major index space
    /// using [`RankRect::project`]. The returned space uses local row-major
    /// indices into `self.base` as its base-rank coordinate system.
    ///
    /// Occlusions from both spaces are translated into that local coordinate
    /// system. A rank is visible in the returned space exactly when the
    /// corresponding base rank is visible in both `self` and `space`.
    ///
    /// If an extracted space has base rectangle ranks `{12, 16, 20, 24}`, hides
    /// base rank `16`, and the parent hides base rank `20`, projection returns a
    /// local-index space over parent local indices `{1, 3, 5, 7}`:
    ///
    /// ```text
    /// extracted coordinates -> parent local indices
    ///
    ///           local gpu
    ///           0  1
    /// host 0    1  x    x = extracted-space occlusion, projected from base rank 16
    ///      1    x  7    x = parent occlusion, projected from base rank 20
    /// ```
    ///
    /// The visible projected ranks are `{1, 7}`. Those ranks are local indices
    /// into `self.base`, not final base-rank coordinates.
    pub fn project(&self, space: &Self) -> Result<Self, RankSpaceError> {
        let base = self.base.project(&space.base)?;
        let parent_occlusion = project_occlusion(&self.base, &base, &self.occlusion)?;
        let space_occlusion = project_occlusion(&self.base, &base, &space.occlusion)?;
        Ok(Self::sparse(
            base,
            RankMask::union([parent_occlusion, space_occlusion]),
        ))
    }

    /// Converts a coordinate into a visible base rank.
    ///
    /// # See also
    ///
    /// [`Self::coord_of`] is the inverse for visible ranks: if
    /// `self.rank_of(coord) == Some(rank)`, then `self.coord_of(rank)`
    /// returns the same coordinate.
    pub fn rank_of(&self, coord: impl AsRef<[usize]>) -> Option<Rank> {
        let rank = self.base.rank_of(coord)?;
        self.contains_rank(rank).then_some(rank)
    }

    /// Converts a visible base rank into a coordinate in this space's base rect.
    ///
    /// # See also
    ///
    /// [`Self::rank_of`] is the inverse: if
    /// `self.coord_of(rank) == Some(coord)`, then
    /// `self.rank_of(coord.indices()) == Some(rank)`.
    pub fn coord_of(&self, rank: Rank) -> Option<Coord> {
        self.contains_rank(rank)
            .then(|| self.base.coord_of(rank))
            .flatten()
    }

    /// Converts a compact visible index into a base rank.
    ///
    /// # See also
    ///
    /// [`Self::local_index_of`] is the inverse: if
    /// `self.rank_at(index) == Some(rank)`, then
    /// `self.local_index_of(rank) == Some(index)`.
    pub fn rank_at(&self, local_index: usize) -> Option<Rank> {
        self.iter_ranks().nth(local_index)
    }

    /// Converts a visible base rank into its compact visible index.
    ///
    /// # See also
    ///
    /// [`Self::rank_at`] is the inverse: if
    /// `self.local_index_of(rank) == Some(index)`, then
    /// `self.rank_at(index) == Some(rank)`.
    pub fn local_index_of(&self, rank: Rank) -> Option<usize> {
        if !self.contains_rank(rank) {
            return None;
        }
        self.iter_ranks()
            .enumerate()
            .find_map(|(index, candidate)| (candidate == rank).then_some(index))
    }

    /// Iterates visible ranks in base rectangle row-major order.
    pub fn iter_ranks(&self) -> impl Iterator<Item = Rank> + '_ {
        self.base
            .iter_ranks()
            .filter(|&rank| !self.occlusion.contains(rank))
    }

    /// Selects the base rectangle while keeping occlusions in base-rank coordinates.
    pub fn select(&self, dim: &str, range: impl Into<DimRange>) -> Result<Self, RankSpaceError> {
        Ok(Self {
            base: self.base.select(dim, range)?,
            occlusion: self.occlusion.clone(),
        })
    }

    /// Fixes one base rectangle dimension while keeping occlusions in base-rank coordinates.
    pub fn fix(&self, dim: &str, index: usize) -> Result<Self, RankSpaceError> {
        Ok(Self {
            base: self.base.fix(dim, index)?,
            occlusion: self.occlusion.clone(),
        })
    }

    fn contains_visible_rect(&self, rect: &RankRect, hidden: &RankMask) -> bool {
        if rect.is_empty() || hidden.contains_rect(rect) {
            return true;
        }

        if self.base.contains_rect(rect) && !self.occlusion.intersects_rect(rect) {
            return true;
        }

        if rect.cardinality() == 1 {
            let rank = rect.offset();
            return hidden.contains(rank) || self.contains_rank(rank);
        }

        let Some((first, second)) = rect.split_largest_dim() else {
            let rank = rect.offset();
            return hidden.contains(rank) || self.contains_rank(rank);
        };
        self.contains_visible_rect(&first, hidden) && self.contains_visible_rect(&second, hidden)
    }
}

impl From<RankRect> for RankSpace {
    fn from(rect: RankRect) -> Self {
        Self::dense(rect)
    }
}

fn rects_intersect_outside_mask(left: &RankRect, right: &RankRect, mask: &RankMask) -> bool {
    if !left.intersects(right) {
        return false;
    }

    if left.cardinality() == 1 {
        let rank = left.offset();
        return right.contains_rank(rank) && !mask.contains(rank);
    }

    if right.cardinality() == 1 {
        let rank = right.offset();
        return left.contains_rank(rank) && !mask.contains(rank);
    }

    if left.cardinality() >= right.cardinality() {
        if let Some((first, second)) = left.split_largest_dim() {
            rects_intersect_outside_mask(&first, right, mask)
                || rects_intersect_outside_mask(&second, right, mask)
        } else {
            let rank = left.offset();
            right.contains_rank(rank) && !mask.contains(rank)
        }
    } else if let Some((first, second)) = right.split_largest_dim() {
        rects_intersect_outside_mask(left, &first, mask)
            || rects_intersect_outside_mask(left, &second, mask)
    } else {
        let rank = right.offset();
        left.contains_rank(rank) && !mask.contains(rank)
    }
}

fn min_visible_rank(rect: &RankRect, mask: &RankMask) -> Option<Rank> {
    let bounds = rect.rank_bounds()?;
    if !mask.contains(bounds.start()) {
        return Some(bounds.start());
    }
    if rect.cardinality() == 1 || mask.contains_rect(rect) {
        return None;
    }

    let (first, second) = rect.split_largest_dim()?;
    min_optional_rank(
        min_visible_rank(&first, mask),
        min_visible_rank(&second, mask),
    )
}

fn max_visible_rank(rect: &RankRect, mask: &RankMask) -> Option<Rank> {
    let bounds = rect.rank_bounds()?;
    let end_rank = bounds_end_rank(bounds);
    if !mask.contains(end_rank) {
        return Some(end_rank);
    }
    if rect.cardinality() == 1 || mask.contains_rect(rect) {
        return None;
    }

    let (first, second) = rect.split_largest_dim()?;
    max_optional_rank(
        max_visible_rank(&first, mask),
        max_visible_rank(&second, mask),
    )
}

fn min_optional_rank(left: Option<Rank>, right: Option<Rank>) -> Option<Rank> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(rank), None) | (None, Some(rank)) => Some(rank),
        (None, None) => None,
    }
}

fn max_optional_rank(left: Option<Rank>, right: Option<Rank>) -> Option<Rank> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(rank), None) | (None, Some(rank)) => Some(rank),
        (None, None) => None,
    }
}

fn bounds_end_rank(bounds: RankBounds) -> Rank {
    bounds
        .end()
        .and_then(|end| end.get().checked_sub(1).map(Rank))
        .unwrap_or(Rank(usize::MAX))
}

fn embed_occlusion(
    parent: &RankRect,
    local_base: &RankRect,
    local_occlusion: &RankMask,
) -> Result<RankMask, RankSpaceError> {
    let ranks = local_base
        .iter_ranks()
        .filter(|rank| local_occlusion.contains(*rank))
        .map(|rank| {
            parent
                .rank_at(rank.get())
                .ok_or(RankSpaceError::IncompatibleEmbedding)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(RankMask::ranks(ranks))
}

fn project_occlusion(
    parent: &RankRect,
    local_base: &RankRect,
    occlusion: &RankMask,
) -> Result<RankMask, RankSpaceError> {
    let ranks = local_base
        .iter_ranks()
        .map(|local_rank| {
            let base_rank = parent
                .rank_at(local_rank.get())
                .ok_or(RankSpaceError::IncompatibleEmbedding)?;
            Ok((local_rank, base_rank))
        })
        .filter_map(|result| match result {
            Ok((local_rank, base_rank)) => occlusion.contains(base_rank).then_some(Ok(local_rank)),
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<BTreeSet<_>, RankSpaceError>>()?;
    Ok(RankMask::ranks(ranks))
}

fn gcd(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn row_major_strides(sizes: impl IntoIterator<Item = usize>) -> Vec<usize> {
    let sizes = sizes.into_iter().collect::<Vec<_>>();
    let mut strides = vec![1; sizes.len()];
    for index in (0..sizes.len().saturating_sub(1)).rev() {
        strides[index] = strides[index + 1] * sizes[index + 1];
    }
    strides
}

fn validate_dims(dims: &[Dim]) -> Result<(), RankSpaceError> {
    let mut names = HashSet::new();
    for dim in dims {
        if !names.insert(dim.name()) {
            return Err(RankSpaceError::DuplicateDim {
                name: dim.name().to_string(),
            });
        }
    }
    Ok(())
}

fn validate_strides(extent: &Extent, strides: &[usize]) -> Result<(), RankSpaceError> {
    if extent.len() != strides.len() {
        return Err(RankSpaceError::DimMismatch {
            dims: extent.len(),
            strides: strides.len(),
        });
    }

    let mut combined = strides
        .iter()
        .copied()
        .zip(extent.sizes())
        .collect::<Vec<_>>();
    combined.sort();

    let mut previous_stride = None;
    let mut previous_size = None;
    let mut coordinate_space = 1;
    for (stride, size) in combined {
        if let Some(prev_stride) = previous_stride {
            if stride % prev_stride != 0 {
                return Err(RankSpaceError::NonrectangularStrides);
            }
            if stride == prev_stride && size != 1 && previous_size.unwrap_or(1) != 1 {
                return Err(RankSpaceError::OverlappingStrides);
            }
        }

        if coordinate_space > stride {
            return Err(RankSpaceError::StrideTooSmall {
                stride,
                space: coordinate_space,
            });
        }

        coordinate_space = stride * size;
        previous_stride = Some(stride);
        previous_size = Some(size);
    }

    Ok(())
}

fn row_major_index(indices: &[usize], sizes: impl IntoIterator<Item = usize>) -> usize {
    indices
        .iter()
        .rev()
        .zip(sizes.into_iter().collect::<Vec<_>>().into_iter().rev())
        .fold((0, 1), |(rank, stride), (index, size)| {
            (rank + index * stride, stride * size)
        })
        .0
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! rankrect {
        (offset = $offset:expr; $($name:ident = $size:expr),+ $(,)?) => {{
            let extent = Extent::new(vec![$(Dim::new(stringify!($name), $size)),+]).unwrap();
            let strides = row_major_strides([$($size),+]);
            RankRect::affine(extent, Rank($offset), strides).unwrap()
        }};
        ($($name:ident = $size:expr),+ $(,)?) => {{
            RankRect::new(Extent::new(vec![$(Dim::new(stringify!($name), $size)),+]).unwrap())
                .unwrap()
        }};
    }

    fn host_gpu_rect() -> RankRect {
        rankrect!(host = 2, gpu = 4)
    }

    #[test]
    fn rank_rect_maps_coordinates_to_ranks() {
        let rect = host_gpu_rect();

        assert_eq!(rect.strides(), &[4, 1]);
        assert_eq!(rect.rank_of([0, 0]), Some(Rank(0)));
        assert_eq!(rect.rank_of([0, 3]), Some(Rank(3)));
        assert_eq!(rect.rank_of([1, 0]), Some(Rank(4)));
        assert_eq!(rect.rank_of([1, 3]), Some(Rank(7)));
        assert_eq!(rect.coord_of(Rank(6)), Some(Coord::from([1, 2])));
    }

    #[test]
    fn rank_of_returns_none_for_invalid_coords_and_overflow() {
        let rect = host_gpu_rect(); // extent [host = 2, gpu = 4]

        // Wrong-length coord (coord.len() != extent.len()).
        assert_eq!(rect.rank_of([0]), None);
        assert_eq!(rect.rank_of([0, 0, 0]), None);

        // Out-of-range coord (coord[d] >= sizes[d]).
        assert_eq!(rect.rank_of([2, 0]), None); // host out of range (size 2)
        assert_eq!(rect.rank_of([0, 4]), None); // gpu out of range (size 4)

        // Multiply overflow: coord[i] * stride[i] overflows usize.
        // Direct struct construction bypasses `validate_strides`; this mirrors
        // the existing `select_reports_rank_arithmetic_overflow` pattern.
        let overflow_stride = RankRect {
            extent: Extent::new(vec![Dim::new("host", 3)]).unwrap(),
            offset: Rank(0),
            strides: vec![usize::MAX],
        };
        assert_eq!(overflow_stride.rank_of([2]), None); // 2 * usize::MAX overflows

        // Add overflow: offset + sum overflows usize. Reachable through the public
        // constructor — `validate_strides` does not constrain offset, so a near-MAX
        // offset is admitted.
        let overflow_offset = RankRect::affine(
            Extent::new(vec![Dim::new("host", 3)]).unwrap(),
            Rank(usize::MAX),
            vec![1],
        )
        .unwrap();
        assert_eq!(overflow_offset.rank_of([1]), None); // usize::MAX + 1 overflows
    }

    #[test]
    fn coord_of_returns_none_for_ranks_outside_rect() {
        // Empty rect: `self.is_empty()` short-circuits to None.
        let empty = RankRect::affine(
            Extent::new(vec![Dim::new("host", 0)]).unwrap(),
            Rank(0),
            vec![1],
        )
        .unwrap();
        assert_eq!(empty.coord_of(Rank(0)), None);

        // Below offset: `rank.0.checked_sub(self.offset.0)` returns None.
        let offset_rect = RankRect::affine(
            Extent::new(vec![Dim::new("host", 2), Dim::new("gpu", 4)]).unwrap(),
            Rank(100),
            vec![4, 1],
        )
        .unwrap();
        assert_eq!(offset_rect.coord_of(Rank(99)), None);
        assert_eq!(offset_rect.coord_of(Rank(0)), None);

        // Above max bound on a dense rect: the largest-stride iteration produces
        // `index >= size`. `host_gpu_rect` has valid ranks 0..=7.
        let dense = host_gpu_rect();
        assert_eq!(dense.coord_of(Rank(8)), None);
        assert_eq!(dense.coord_of(Rank(100)), None);

        // Inside bounds but not on a stride boundary: the decomposition completes
        // without hitting `index >= size`, but leaves `rest != 0` at the end.
        // Requires a smallest stride > 1; extent [2] with stride [3] has valid
        // ranks {0, 3}, so ranks 1 and 2 sit between them and are unreachable.
        let sparse = RankRect::affine(
            Extent::new(vec![Dim::new("host", 2)]).unwrap(),
            Rank(0),
            vec![3],
        )
        .unwrap();
        assert_eq!(sparse.coord_of(Rank(1)), None);
        assert_eq!(sparse.coord_of(Rank(2)), None);
    }

    #[test]
    fn rank_rect_affine_rejects_invalid_strides() {
        let extent =
            |a: usize, b: usize| Extent::new(vec![Dim::new("a", a), Dim::new("b", b)]).unwrap();

        // `DimMismatch`: strides length doesn't match extent rank.
        assert_eq!(
            RankRect::affine(extent(2, 2), Rank(0), vec![2]).unwrap_err(),
            RankSpaceError::DimMismatch {
                dims: 2,
                strides: 1,
            },
        );

        // `NonrectangularStrides`: after sorting, a stride doesn't divide its predecessor.
        assert_eq!(
            RankRect::affine(extent(2, 2), Rank(0), vec![4, 3]).unwrap_err(),
            RankSpaceError::NonrectangularStrides,
        );

        // `OverlappingStrides`: two non-unit dims share a stride.
        assert_eq!(
            RankRect::affine(extent(2, 2), Rank(0), vec![2, 2]).unwrap_err(),
            RankSpaceError::OverlappingStrides,
        );

        // `StrideTooSmall`: a stride is smaller than the coordinate space below it.
        assert_eq!(
            RankRect::affine(extent(4, 4), Rank(0), vec![3, 1]).unwrap_err(),
            RankSpaceError::StrideTooSmall {
                stride: 3,
                space: 4,
            },
        );
    }

    #[test]
    fn affine_supports_column_major_strides() {
        // sizes [2, 4] with column-major strides [1, 2]: dim 0 has the smallest
        // stride, so iter (local row-major over the extent) produces ranks in
        // interleaved order rather than consecutive.
        let extent = Extent::new(vec![Dim::new("a", 2), Dim::new("b", 4)]).unwrap();
        let rect = RankRect::affine(extent, Rank(0), vec![1, 2]).unwrap();

        assert_eq!(
            rect.iter_ranks().collect::<Vec<_>>(),
            vec![
                Rank(0),
                Rank(2),
                Rank(4),
                Rank(6),
                Rank(1),
                Rank(3),
                Rank(5),
                Rank(7),
            ],
        );

        assert_eq!(rect.rank_of([1, 2]), Some(Rank(5)));
        assert_eq!(rect.coord_of(Rank(5)), Some(Coord::from([1, 2])));
        assert_eq!(rect.rank_of([0, 3]), Some(Rank(6)));
        assert_eq!(rect.coord_of(Rank(6)), Some(Coord::from([0, 3])));
    }

    #[test]
    fn affine_supports_permuted_strides() {
        // sizes [2, 3, 4] with strides [4, 8, 1]: the dim with the largest stride
        // (`b`) is in the middle position, not the outermost. Iteration is still
        // local row-major over the extent, but the rank arithmetic depends on the
        // non-canonical stride permutation. Dense (no gaps): the coord_space chain
        // is 1 -> 4 -> 8 -> 24, exactly matching 2*3*4.
        let extent =
            Extent::new(vec![Dim::new("a", 2), Dim::new("b", 3), Dim::new("c", 4)]).unwrap();
        let rect = RankRect::affine(extent, Rank(0), vec![4, 8, 1]).unwrap();

        for (coord, rank) in [
            ([0usize, 0, 0], 0usize),
            ([0, 0, 3], 3),
            ([0, 1, 0], 8),
            ([1, 0, 0], 4),
            ([1, 2, 3], 23),
        ] {
            assert_eq!(rect.rank_of(coord), Some(Rank(rank)));
            assert_eq!(rect.coord_of(Rank(rank)), Some(Coord::from(coord)));
        }

        // Iteration walks the 24 coords in local row-major over the extent; the
        // rank stream reflects the permuted strides, not the dim declaration order.
        assert_eq!(
            rect.iter_ranks().collect::<Vec<_>>(),
            vec![
                Rank(0),
                Rank(1),
                Rank(2),
                Rank(3),
                Rank(8),
                Rank(9),
                Rank(10),
                Rank(11),
                Rank(16),
                Rank(17),
                Rank(18),
                Rank(19),
                Rank(4),
                Rank(5),
                Rank(6),
                Rank(7),
                Rank(12),
                Rank(13),
                Rank(14),
                Rank(15),
                Rank(20),
                Rank(21),
                Rank(22),
                Rank(23),
            ],
        );
    }

    #[test]
    fn affine_supports_gapped_strides() {
        // sizes [2, 4] with strides [10, 1]: rows are 10 apart, leaving a gap
        // (ranks 4..=9) between row 0 and row 1 that's not part of the rect.
        let extent = Extent::new(vec![Dim::new("a", 2), Dim::new("b", 4)]).unwrap();
        let rect = RankRect::affine(extent, Rank(0), vec![10, 1]).unwrap();

        assert_eq!(
            rect.iter_ranks().collect::<Vec<_>>(),
            vec![
                Rank(0),
                Rank(1),
                Rank(2),
                Rank(3),
                Rank(10),
                Rank(11),
                Rank(12),
                Rank(13),
            ],
        );

        assert_eq!(rect.rank_of([1, 2]), Some(Rank(12)));
        assert_eq!(rect.coord_of(Rank(12)), Some(Coord::from([1, 2])));

        // Ranks inside the gap are not part of the rect.
        assert_eq!(rect.coord_of(Rank(4)), None); // immediately after row 0
        assert_eq!(rect.coord_of(Rank(5)), None);
        assert_eq!(rect.coord_of(Rank(9)), None);

        // Ranks above the last valid row also return None.
        assert_eq!(rect.coord_of(Rank(14)), None); // immediately after row 1
        assert_eq!(rect.coord_of(Rank(20)), None); // where row 2 would start
    }

    #[test]
    fn rank_rect_with_empty_extent_is_a_single_point() {
        // 0-dim `RankRect`: a "scalar" rect whose one rank is the offset.
        let rect = RankRect::affine(Extent::new(vec![]).unwrap(), Rank(42), vec![]).unwrap();

        assert!(!rect.is_empty());
        assert_eq!(rect.cardinality(), 1);
        assert_eq!(rect.iter_ranks().collect::<Vec<_>>(), vec![Rank(42)]);
        assert_eq!(rect.coord_of(Rank(42)), Some(Coord::new(vec![])));
        assert_eq!(rect.coord_of(Rank(0)), None);
        let coord = Coord::new(vec![]);
        assert_eq!(rect.rank_of(coord.indices()), Some(Rank(42)));

        // `RankRect::new` (default offset 0) takes the same scalar shape.
        let zero_rect = RankRect::new(Extent::new(vec![]).unwrap()).unwrap();
        assert_eq!(zero_rect.cardinality(), 1);
        assert_eq!(zero_rect.iter_ranks().collect::<Vec<_>>(), vec![Rank(0)]);
    }

    #[test]
    fn select_preserves_base_rank_coordinates() {
        let rect = host_gpu_rect();
        let gpus = rect
            .select("gpu", DimRange::with_step(1, None, 2).unwrap())
            .unwrap();

        assert_eq!(gpus.extent().dims()[1].size(), 2);
        assert_eq!(gpus.strides(), &[4, 2]);
        assert_eq!(
            gpus.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(1), Rank(3), Rank(5), Rank(7)]
        );
    }

    #[test]
    fn select_with_start_past_dim_yields_empty() {
        // `start >= dim_size`: clamped start equals end, len = 0.
        let rect = host_gpu_rect();
        let selected = rect
            .select("gpu", DimRange::with_step(4, None, 1).unwrap())
            .unwrap();

        assert_eq!(selected.extent().dims()[1].size(), 0);
        assert!(selected.iter_ranks().next().is_none());
    }

    #[test]
    fn select_with_end_before_start_yields_empty() {
        // `end <= start`: short-circuit to len = 0.
        let rect = host_gpu_rect();
        let selected = rect
            .select("gpu", DimRange::with_step(2, Some(2), 1).unwrap())
            .unwrap();

        assert_eq!(selected.extent().dims()[1].size(), 0);
        assert!(selected.iter_ranks().next().is_none());
    }

    #[test]
    fn select_with_step_past_range_yields_single() {
        // step > (end - start), end set explicitly: len = 1, picks up `start`.
        let rect = host_gpu_rect();
        let selected = rect
            .select("gpu", DimRange::with_step(1, Some(2), 8).unwrap())
            .unwrap();

        assert_eq!(selected.extent().dims()[1].size(), 1);
        assert_eq!(
            selected.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(1), Rank(5)],
        );
    }

    #[test]
    fn select_with_step_past_dim_yields_single() {
        // step > dim_size, end defaulted from `None`: len = 1, picks up `start = 0`.
        let rect = host_gpu_rect();
        let selected = rect
            .select("gpu", DimRange::with_step(0, None, 8).unwrap())
            .unwrap();

        assert_eq!(selected.extent().dims()[1].size(), 1);
        assert_eq!(
            selected.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(0), Rank(4)],
        );
    }

    #[test]
    fn fix_removes_a_dimension() {
        let rect = host_gpu_rect();
        let host1 = rect.fix("host", 1).unwrap();

        assert_eq!(host1.extent().len(), 1);
        assert_eq!(host1.extent().dims()[0].name(), "gpu");
        assert_eq!(
            host1.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(4), Rank(5), Rank(6), Rank(7)]
        );
    }

    #[test]
    fn sparse_space_hides_base_rank_occlusions() {
        let rect = host_gpu_rect();
        let missing_gpu = RankMask::ranks([Rank(5)]);
        let space = RankSpace::dense(rect).without(missing_gpu);

        assert!(space.contains_rank(Rank(4)));
        assert!(!space.contains_rank(Rank(5)));
        assert_eq!(
            space.iter_ranks().collect::<Vec<_>>(),
            vec![
                Rank(0),
                Rank(1),
                Rank(2),
                Rank(3),
                Rank(4),
                Rank(6),
                Rank(7)
            ]
        );
        assert_eq!(space.local_index_of(Rank(6)), Some(5));
        assert_eq!(space.rank_at(5), Some(Rank(6)));
    }

    #[test]
    fn sparse_space_with_empty_mask_matches_dense() {
        let base = host_gpu_rect(); // sizes [2, 4], valid ranks 0..=7
        let dense = RankSpace::dense(base.clone());
        let sparse = RankSpace::sparse(base.clone(), RankMask::Empty);

        // Structural equivalence: both constructors produce the same struct.
        assert_eq!(dense, sparse);

        // Behavioral equivalence: derived methods agree, and the concrete values
        // match the expected dense iteration (so a bug producing equally-broken
        // results on both sides would still be caught).
        assert_eq!(dense.cardinality(), sparse.cardinality());
        assert_eq!(dense.cardinality(), 8);
        let expected = vec![
            Rank(0),
            Rank(1),
            Rank(2),
            Rank(3),
            Rank(4),
            Rank(5),
            Rank(6),
            Rank(7),
        ];
        assert_eq!(dense.iter_ranks().collect::<Vec<_>>(), expected);
        assert_eq!(sparse.iter_ranks().collect::<Vec<_>>(), expected);
        for rank in base.iter_ranks() {
            assert_eq!(dense.contains_rank(rank), sparse.contains_rank(rank));
        }
        // Out-of-base rank: both short-circuit via `base.contains_rank`.
        assert_eq!(
            dense.contains_rank(Rank(100)),
            sparse.contains_rank(Rank(100))
        );
    }

    #[test]
    fn without_chains_accumulate_via_union() {
        let rect = host_gpu_rect(); // sizes [2, 4], valid ranks 0..=7
        let mask_a = RankMask::ranks([Rank(2)]);
        let mask_b = RankMask::ranks([Rank(5)]);

        let space = RankSpace::dense(rect)
            .without(mask_a.clone())
            .without(mask_b.clone());

        // Behavioral: both masks are applied — ranks 2 and 5 are hidden.
        assert!(!space.contains_rank(Rank(2)));
        assert!(!space.contains_rank(Rank(5)));
        assert!(space.contains_rank(Rank(0)));
        assert!(space.contains_rank(Rank(7)));
        assert!(!space.contains_rank(Rank(100))); // out-of-base
        assert_eq!(
            space.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(0), Rank(1), Rank(3), Rank(4), Rank(6), Rank(7)],
        );
        assert_eq!(space.cardinality(), 6);

        // Structural: the chained occlusion equals a direct union of the two masks.
        let expected_occlusion = RankMask::union([mask_a, mask_b]);
        assert_eq!(space.occlusion(), &expected_occlusion);
    }

    #[test]
    fn sparse_subspaces_keep_masks_in_base_rank_coordinates() {
        let rect = host_gpu_rect();
        let missing_gpu = RankMask::ranks([Rank(5)]);
        let space = RankSpace::dense(rect).without(missing_gpu);
        let host1 = space.fix("host", 1).unwrap();

        assert_eq!(
            host1.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(4), Rank(6), Rank(7)]
        );
        assert_eq!(host1.coord_of(Rank(6)), Some(Coord::from([2])));
    }

    #[test]
    fn rank_space_under_full_occlusion_is_empty() {
        let base = host_gpu_rect();
        let space = RankSpace::dense(base.clone()).without(RankMask::rect(base.clone()));

        assert!(space.is_empty());
        assert_eq!(space.cardinality(), 0);
        assert!(space.iter_ranks().next().is_none());
        assert_eq!(space.rank_at(0), None);

        // Every base rank is occluded.
        for rank in base.iter_ranks() {
            assert!(!space.contains_rank(rank));
            assert_eq!(space.local_index_of(rank), None);
        }
    }

    #[test]
    fn mask_can_be_an_affine_rect() {
        let rect = host_gpu_rect();
        let host1 = rect.fix("host", 1).unwrap();
        let space = RankSpace::dense(rect).without(RankMask::rect(host1));

        assert_eq!(
            space.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(0), Rank(1), Rank(2), Rank(3)]
        );
    }

    #[test]
    fn mask_union_all_empty_yields_empty() {
        let mask = RankMask::union([RankMask::empty(), RankMask::empty(), RankMask::empty()]);
        assert_eq!(mask, RankMask::Empty);
    }

    #[test]
    fn mask_union_single_non_empty_unwraps() {
        let ranks = RankMask::ranks([Rank(5)]);
        let mask = RankMask::union([RankMask::empty(), ranks.clone(), RankMask::empty()]);
        assert_eq!(mask, ranks);
    }

    #[test]
    fn mask_union_mixed_variants_yields_union() {
        let rect = RankMask::rect(host_gpu_rect()); // covers ranks 0..=7
        let ranks = RankMask::ranks([Rank(42)]);
        let mask = RankMask::union([rect, RankMask::empty(), ranks]);

        match &mask {
            RankMask::Union(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(children[0], RankMask::Rect(_)));
                assert!(matches!(children[1], RankMask::Ranks(_)));
            }
            other => panic!("expected Union, got {other:?}"),
        }

        assert!(mask.contains(Rank(0))); // in the rect
        assert!(mask.contains(Rank(42))); // in the explicit ranks
        assert!(!mask.contains(Rank(100))); // in neither
    }

    #[test]
    fn mask_union_preserves_nested_unions() {
        // Pins current behavior: `union` does not flatten nested `Union`s.
        // `contains` still resolves through both layers.
        let inner_a = RankMask::union([RankMask::ranks([Rank(1)]), RankMask::ranks([Rank(2)])]);
        let inner_b = RankMask::union([RankMask::ranks([Rank(3)]), RankMask::ranks([Rank(4)])]);
        let outer = RankMask::union([inner_a, inner_b]);

        match &outer {
            RankMask::Union(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(children[0], RankMask::Union(_)));
                assert!(matches!(children[1], RankMask::Union(_)));
            }
            other => panic!("expected Union, got {other:?}"),
        }

        assert!(outer.contains(Rank(1)));
        assert!(outer.contains(Rank(4)));
        assert!(!outer.contains(Rank(99)));
    }

    #[test]
    fn rank_mask_rect_relationships_are_exact() {
        let rect = host_gpu_rect();
        let host0 = rect.fix("host", 0).unwrap();
        let host1 = rect.fix("host", 1).unwrap();
        let mask = RankMask::union([
            RankMask::ranks([Rank(0), Rank(1)]),
            RankMask::ranks([Rank(2), Rank(3)]),
        ]);

        assert!(mask.contains_rect(&host0));
        assert!(!mask.contains_rect(&host1));
        assert!(mask.intersects_rect(&host0));
        assert!(!mask.intersects_rect(&host1));
        assert!(mask.contains_mask(&RankMask::rect(host0)));
        assert!(!mask.intersects_mask(&RankMask::rect(host1)));
    }

    #[test]
    fn rank_space_relationships_use_visible_rank_sets() {
        let rect = host_gpu_rect();
        let host0 = RankSpace::dense(rect.fix("host", 0).unwrap());
        let host1 = RankSpace::dense(rect.fix("host", 1).unwrap());
        let gpu2 = RankSpace::dense(rect.fix("gpu", 2).unwrap());
        let without_rank_6 = RankSpace::dense(rect).without(RankMask::ranks([Rank(6)]));

        assert!(!host0.intersects(&host1));
        assert!(host1.intersects(&gpu2));
        assert!(!without_rank_6.intersects(&RankSpace::dense(rankrect!(offset = 6; x = 1))));
        assert!(without_rank_6.contains_space(&host0));
        assert!(!without_rank_6.contains_space(&gpu2));
    }

    #[test]
    fn rank_space_bounds_trim_edge_occlusions() {
        let space = RankSpace::dense(host_gpu_rect()).without(RankMask::ranks([Rank(0), Rank(7)]));

        assert_eq!(space.rank_bounds(), RankBounds::new(Rank(1), Some(Rank(7))));
    }

    #[test]
    fn rank_space_bounds_trim_strided_edge_occlusions() {
        let rect = RankRect::affine(
            Extent::new(vec![Dim::new("host", 2), Dim::new("gpu", 4)]).unwrap(),
            Rank(10),
            vec![8, 2],
        )
        .unwrap();

        let internally_masked = RankSpace::dense(rect.clone()).without(RankMask::ranks([Rank(20)]));
        assert_eq!(
            internally_masked.rank_bounds(),
            RankBounds::new(Rank(10), Some(Rank(25)))
        );

        let edge_masked = RankSpace::dense(rect).without(RankMask::ranks([Rank(10), Rank(24)]));
        assert_eq!(
            edge_masked.rank_bounds(),
            RankBounds::new(Rank(12), Some(Rank(23)))
        );
    }

    #[test]
    fn rank_space_embed_projects_local_occlusions() {
        let parent = RankSpace::dense(host_gpu_rect()).without(RankMask::ranks([Rank(5)]));
        let local = RankSpace::dense(
            RankRect::affine(
                Extent::new(vec![Dim::new("host", 2), Dim::new("gpu", 2)]).unwrap(),
                Rank(1),
                vec![4, 2],
            )
            .unwrap(),
        )
        .without(RankMask::ranks([Rank(3)]));

        let embedded = parent.embed(&local).unwrap();

        assert_eq!(
            embedded.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(1), Rank(7)]
        );
    }

    #[test]
    fn rank_bounds_cover_rect_ranks() {
        let rect = RankRect::affine(
            Extent::new(vec![Dim::new("row", 2), Dim::new("col", 3)]).unwrap(),
            Rank(5),
            vec![10, 2],
        )
        .unwrap();

        assert_eq!(rect.rank_bounds(), RankBounds::new(Rank(5), Some(Rank(20))));

        let max_rank = rankrect!(offset = usize::MAX; scalar = 1);
        assert_eq!(
            max_rank.rank_bounds(),
            RankBounds::new(Rank(usize::MAX), None)
        );
    }

    #[test]
    fn rank_rect_relationships_match_expected_sets() {
        let base = host_gpu_rect();
        let host0 = base.fix("host", 0).unwrap();
        let host1 = base.fix("host", 1).unwrap();
        let gpu2 = base.fix("gpu", 2).unwrap();
        let even_gpus = base
            .select("gpu", DimRange::with_step(0, None, 2).unwrap())
            .unwrap();
        let odd_gpus = base
            .select("gpu", DimRange::with_step(1, None, 2).unwrap())
            .unwrap();

        assert!(base.contains_rect(&host0));
        assert!(base.contains_rect(&gpu2));
        assert!(!host0.intersects(&host1));
        assert!(host1.intersects(&gpu2));
        assert!(!even_gpus.intersects(&odd_gpus));
        assert!(
            even_gpus.contains_rect(
                &RankRect::affine(
                    Extent::new(vec![Dim::new("x", 2)]).unwrap(),
                    Rank(0),
                    vec![2]
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn embed_reindexes_local_rank_rect_into_base_ranks() {
        let base = host_gpu_rect();
        let local = RankRect::affine(
            Extent::new(vec![Dim::new("host", 2), Dim::new("gpu", 2)]).unwrap(),
            Rank(1),
            vec![4, 2],
        )
        .unwrap();
        let embedded = base.embed(&local).unwrap();

        assert_eq!(embedded.offset(), Rank(1));
        assert_eq!(embedded.strides(), &[4, 2]);
        assert_eq!(
            embedded.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(1), Rank(3), Rank(5), Rank(7)]
        );
    }

    #[test]
    fn embed_short_circuits_contiguous_parent_validation() {
        let base = RankRect::from_sizes([usize::MAX]).unwrap();
        let local = RankRect::from_sizes([usize::MAX]).unwrap();

        assert_eq!(base.embed(&local).unwrap(), local);
    }

    #[test]
    fn embed_rejects_local_indices_outside_parent() {
        let base = rankrect!(x = 4);
        let local = rankrect!(offset = 3; i = 2);

        assert_eq!(
            base.embed(&local).unwrap_err(),
            RankSpaceError::IncompatibleEmbedding
        );
    }

    #[test]
    fn project_reindexes_base_ranks_into_local_indices() {
        let base = host_gpu_rect();
        let rect = base
            .select("gpu", DimRange::with_step(1, None, 2).unwrap())
            .unwrap();
        let projected = base.project(&rect).unwrap();

        assert_eq!(projected.offset(), Rank(1));
        assert_eq!(projected.strides(), &[4, 2]);
        assert_eq!(
            projected.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(1), Rank(3), Rank(5), Rank(7)]
        );
        assert_eq!(base.embed(&projected).unwrap(), rect);
    }

    #[test]
    fn project_short_circuits_contiguous_parent_validation() {
        let base = RankRect::from_sizes([usize::MAX]).unwrap();
        let rect = RankRect::from_sizes([usize::MAX]).unwrap();

        assert_eq!(base.project(&rect).unwrap(), rect);
    }

    #[test]
    fn project_rejects_base_ranks_outside_parent() {
        let base = rankrect!(x = 4);
        let rect = rankrect!(offset = 3; i = 2);

        assert_eq!(
            base.project(&rect).unwrap_err(),
            RankSpaceError::IncompatibleEmbedding
        );
    }

    #[test]
    fn project_rejects_non_affine_inverse() {
        let column_major = RankRect::affine(
            Extent::new(vec![Dim::new("row", 2), Dim::new("col", 3)]).unwrap(),
            Rank(0),
            vec![1, 2],
        )
        .unwrap();
        let flat = RankRect::from_sizes([6]).unwrap();

        assert_eq!(
            column_major.project(&flat).unwrap_err(),
            RankSpaceError::IncompatibleEmbedding
        );
    }

    #[test]
    fn rank_space_project_projects_occlusions() {
        let parent = RankSpace::dense(host_gpu_rect()).without(RankMask::ranks([Rank(5)]));
        let extracted = RankSpace::dense(
            RankRect::affine(
                Extent::new(vec![Dim::new("host", 2), Dim::new("gpu", 2)]).unwrap(),
                Rank(1),
                vec![4, 2],
            )
            .unwrap(),
        )
        .without(RankMask::ranks([Rank(3)]));

        let projected = parent.project(&extracted).unwrap();

        assert_eq!(
            projected.iter_ranks().collect::<Vec<_>>(),
            vec![Rank(1), Rank(7)]
        );
    }

    #[test]
    fn rank_space_embed_rejects_local_indices_outside_parent() {
        let parent = RankSpace::dense(rankrect!(x = 4));
        let local = RankSpace::dense(rankrect!(offset = 3; i = 2));

        assert_eq!(
            parent.embed(&local).unwrap_err(),
            RankSpaceError::IncompatibleEmbedding
        );
    }

    #[test]
    fn rank_space_project_rejects_base_ranks_outside_parent() {
        let parent = RankSpace::dense(rankrect!(x = 4));
        let extracted = RankSpace::dense(rankrect!(offset = 3; i = 2));

        assert_eq!(
            parent.project(&extracted).unwrap_err(),
            RankSpaceError::IncompatibleEmbedding
        );
    }

    #[test]
    fn embed_rejects_non_affine_composition() {
        let column_major = RankRect::affine(
            Extent::new(vec![Dim::new("row", 2), Dim::new("col", 3)]).unwrap(),
            Rank(0),
            vec![1, 2],
        )
        .unwrap();
        let flat_local = RankRect::from_sizes([6]).unwrap();

        assert_eq!(
            column_major.embed(&flat_local).unwrap_err(),
            RankSpaceError::IncompatibleEmbedding
        );
    }

    #[test]
    fn select_reports_rank_arithmetic_overflow() {
        let rect = RankRect {
            extent: Extent::new(vec![Dim::new("host", 3)]).unwrap(),
            offset: Rank(usize::MAX),
            strides: vec![1],
        };

        assert_eq!(
            rect.select("host", 1).unwrap_err(),
            RankSpaceError::RankArithmeticOverflow
        );

        let rect = RankRect {
            extent: Extent::new(vec![Dim::new("host", 3)]).unwrap(),
            offset: Rank(0),
            strides: vec![usize::MAX],
        };

        assert_eq!(
            rect.select("host", 2).unwrap_err(),
            RankSpaceError::RankArithmeticOverflow
        );
        assert_eq!(
            rect.select("host", DimRange::with_step(0, Some(2), 2).unwrap())
                .unwrap_err(),
            RankSpaceError::RankArithmeticOverflow
        );
    }

    #[test]
    fn fix_reports_rank_arithmetic_overflow() {
        let rect = RankRect {
            extent: Extent::new(vec![Dim::new("host", 3)]).unwrap(),
            offset: Rank(usize::MAX),
            strides: vec![1],
        };

        assert_eq!(
            rect.fix("host", 1).unwrap_err(),
            RankSpaceError::RankArithmeticOverflow
        );

        let rect = RankRect {
            extent: Extent::new(vec![Dim::new("host", 3)]).unwrap(),
            offset: Rank(0),
            strides: vec![usize::MAX],
        };

        assert_eq!(
            rect.fix("host", 2).unwrap_err(),
            RankSpaceError::RankArithmeticOverflow
        );
    }

    #[test]
    fn types_serde_round_trip_via_bincode() {
        fn roundtrip<T>(value: &T) -> T
        where
            T: serde::Serialize + serde::de::DeserializeOwned,
        {
            let bytes = bincode::serde::encode_to_vec(value, bincode::config::legacy()).unwrap();
            let (decoded, len) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::legacy()).unwrap();
            assert_eq!(len, bytes.len(), "decoder left trailing bytes");
            decoded
        }

        // Rank, Coord
        let rank = Rank(7);
        assert_eq!(roundtrip(&rank), rank);
        let coord = Coord::from([1, 2]);
        assert_eq!(roundtrip(&coord), coord);

        // Dim
        let dim = Dim::new("host", 2);
        assert_eq!(roundtrip(&dim), dim);

        // DimRange
        let range = DimRange::with_step(0, Some(10), 2).unwrap();
        assert_eq!(roundtrip(&range), range);

        // Extent
        let extent = Extent::new(vec![Dim::new("host", 2), Dim::new("gpu", 4)]).unwrap();
        assert_eq!(roundtrip(&extent), extent);

        // RankRect
        let rect = host_gpu_rect();
        assert_eq!(roundtrip(&rect), rect);

        // RankMask: cover each variant.
        let mask_empty = RankMask::Empty;
        let mask_ranks = RankMask::ranks([Rank(0), Rank(3)]);
        let mask_rect = RankMask::rect(rect.clone());
        let mask_union = RankMask::union([mask_ranks.clone(), mask_rect.clone()]);
        for mask in [mask_empty, mask_ranks, mask_rect, mask_union] {
            assert_eq!(roundtrip(&mask), mask);
        }

        // RankSpace
        let space = RankSpace::sparse(rect.clone(), RankMask::ranks([Rank(5)]));
        assert_eq!(roundtrip(&space), space);

        // BaseView and CompactView (over Vec<u32>, length = cardinality 8).
        let base_view = view::BaseView::new(
            RankSpace::dense(rect.clone()),
            vec![0u32, 1, 2, 3, 4, 5, 6, 7],
        );
        assert_eq!(roundtrip(&base_view), base_view);
        let compact_view =
            view::CompactView::new(RankSpace::dense(rect), vec![0u32, 1, 2, 3, 4, 5, 6, 7])
                .unwrap();
        assert_eq!(roundtrip(&compact_view), compact_view);
    }
}
