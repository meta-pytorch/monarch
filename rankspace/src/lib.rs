/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Affine and sparse coordinate spaces over ranks.
//!
//! A rank space is a small geometry object. It names a rectangular coordinate
//! extent, embeds that extent into a flat base-rank coordinate system, and can
//! hide base ranks that are not defined. The crate keeps that geometry separate
//! from any values indexed by it.
//!
//! The core model has these layers:
//!
//! - [`Rank`] is a flat coordinate in the base rank space. Masks, set
//!   operations, and storage adapters all use this same coordinate system.
//! - [`Dim`] and [`Extent`] describe the local coordinate shape. They give names
//!   and sizes to dimensions, but they do not assign ranks.
//! - [`RankRect`] is a dense affine rectangular embedding:
//!   `coord -> offset + dot(coord, strides)`. Operations such as restriction and
//!   fixing dimensions create smaller affine rectangles without changing the
//!   meaning of the base ranks.
//! - [`RankMask`] is a set of ranks in base-rank coordinates. A mask can be
//!   empty, explicit, rectangular, or a union of masks. Masks are not relative to
//!   a view or subspace; projection into local coordinates is a view concern.
//! - [`RankSpace`] is the visible space: a base [`RankRect`] minus base-rank
//!   occlusions. It answers membership, coordinate-to-rank, rank-to-coordinate,
//!   and compact visible-index queries.
//!
//! Value containers are intentionally layered on top in [`view`]. The core
//! module computes ranks and visible membership; the `view` module decides how
//! those ranks address user data.

pub mod view;

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

    /// Dimension ranges must have nonzero steps.
    #[error("dimension range step must be nonzero")]
    ZeroStep,
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

    /// Returns true if the base rank is visible in this rank space.
    pub fn contains_rank(&self, rank: Rank) -> bool {
        self.base.contains_rank(rank) && !self.occlusion.contains(rank)
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
}

impl From<RankRect> for RankSpace {
    fn from(rect: RankRect) -> Self {
        Self::dense(rect)
    }
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
}
