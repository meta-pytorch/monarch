/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The purpose of this module is to provide data structures to efficiently
//! describe subsets of nonnegative integers, called *ranks*, represented by
//! `usize` values. Ranks are organized into multi-dimensional spaces in which
//! each discrete point is a rank, mapped in row-major order.
//!
//! These dimensions represent a space that carry semantic meaning, such that
//! ranks are usually sub-set along some dimension of the space. For example,
//! ranks may be organized into a space with dimensions "replica", "host", "gpu",
//! and we'd expect to subset along these dimensions, for example to select all
//! GPUs in a given replica.
//!
//! This alignment helps provide a simple and efficient representation, internally
//! in the form of [`crate::Slice`], comprising an offset, sizes, and strides that
//! index into the space.
//!
//! - [`Extent`]: the *shape* of the space, naming each dimension and specifying
//!               their sizes.
//! - [`Point`]: a specific coordinate in an extent, together with its linearized rank.
//! - [`Region`]: a (possibly sparse) hyper-rectangle of ranks within a larger extent.
//!               Since it is always rectangular, it also defines its own extent.
//! - [`View`]: a collection of items indexed by [`Region`]. Views provide standard
//!             manipulation operations and use ranks as an efficient indexing scheme.

use std::str::FromStr;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::Range;
use crate::Slice;
use crate::SliceError;
use crate::SliceIterator;
use crate::parse::Parser;
use crate::parse::ParserError;
use crate::slice::CartesianIterator;

/// Errors that can occur when constructing or validating an `Extent`.
#[derive(Debug, thiserror::Error)]
pub enum ExtentError {
    /// The number of labels does not match the number of sizes.
    ///
    /// This occurs when constructing an `Extent` from parallel
    /// `Vec<String>` and `Vec<usize>` inputs that are not the same
    /// length.
    #[error("label/sizes dimension mismatch: {num_labels} != {num_sizes}")]
    DimMismatch {
        /// Number of dimension labels provided.
        num_labels: usize,
        /// Number of dimension sizes provided.
        num_sizes: usize,
    },
}

/// `Extent` defines the logical shape of a multidimensional space by
/// assigning a size to each named dimension. It abstracts away memory
/// layout and focuses solely on structure — what dimensions exist and
/// how many elements each contains.
///
/// Conceptually, it corresponds to a coordinate space in the
/// mathematical sense.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Hash, Debug)]
pub struct Extent {
    inner: Arc<ExtentData>,
}

fn _assert_extent_traits()
where
    Extent: Send + Sync + 'static,
{
}

// `ExtentData` is represented as:
// - `labels`: dimension names like `"zone"`, `"host"`, `"gpu"`
// - `sizes`: number of elements in each dimension, independent of
//   stride or storage layout
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Hash, Debug)]
struct ExtentData {
    labels: Vec<String>,
    sizes: Vec<usize>,
}

impl Extent {
    /// Creates a new `Extent` from the given labels and sizes.
    pub fn new(labels: Vec<String>, sizes: Vec<usize>) -> Result<Self, ExtentError> {
        if labels.len() != sizes.len() {
            return Err(ExtentError::DimMismatch {
                num_labels: labels.len(),
                num_sizes: sizes.len(),
            });
        }

        Ok(Self {
            inner: Arc::new(ExtentData { labels, sizes }),
        })
    }

    pub fn unity() -> Extent {
        Extent::new(vec![], vec![]).unwrap()
    }

    /// Returns the ordered list of dimension labels in this extent.
    pub fn labels(&self) -> &[String] {
        &self.inner.labels
    }

    /// Returns the dimension sizes, ordered to match the labels.
    pub fn sizes(&self) -> &[usize] {
        &self.inner.sizes
    }

    /// Returns the size of the dimension with the given label, if it
    /// exists.
    pub fn size(&self, label: &str) -> Option<usize> {
        self.position(label).map(|pos| self.sizes()[pos])
    }

    /// Returns the position of the dimension with the given label, if
    /// it exists exists.
    pub fn position(&self, label: &str) -> Option<usize> {
        self.labels().iter().position(|l| l == label)
    }

    // Computes the row-major logical rank of the given coordinates
    // in this extent.
    //
    // ```text
    // Σ (coord[i] × ∏(sizes[j] for j > i))
    // ```
    //
    // where 'coord' is the point's coordinate and 'sizes' is the
    // extent's dimension sizes.
    pub fn rank_of_coords(&self, coords: &[usize]) -> Result<usize, PointError> {
        let sizes = self.sizes();
        if coords.len() != sizes.len() {
            return Err(PointError::DimMismatch {
                expected: sizes.len(),
                actual: coords.len(),
            });
        }
        let mut stride = 1;
        let mut result = 0;
        for (&c, &size) in coords.iter().rev().zip(sizes.iter().rev()) {
            if c >= size {
                return Err(PointError::OutOfRangeIndex { size, index: c });
            }
            result += c * stride;
            stride *= size;
        }
        Ok(result)
    }

    /// Creates a [`Point`] in this extent from the given coordinate
    /// vector.
    ///
    /// The coordinates are interpreted in **row-major** order against
    /// `self.sizes()`. This constructor does not store the
    /// coordinates; it computes the linear **rank** and returns a
    /// `Point` that stores `{ rank, extent }`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`PointError::DimMismatch`] if `coords.len() != self.len()`.
    /// - [`PointError::OutOfRangeIndex`] if any coordinate `coords[i]
    ///   >= self.sizes()[i]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3, z = 4);
    /// let p = ext.point(vec![1, 2, 3]).unwrap();
    /// assert_eq!(p.rank(), 1 * (3 * 4) + 2 * 4 + 3); // row-major
    /// assert_eq!(p.coords(), vec![1, 2, 3]);
    /// ```
    ///
    /// Dimension mismatch:
    /// ```
    /// use ndslice::PointError;
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3);
    /// let err = ext.point(vec![1]).unwrap_err();
    /// matches!(err, PointError::DimMismatch { .. });
    /// ```
    ///
    /// Coordinate out of range:
    /// ```
    /// use ndslice::PointError;
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3);
    /// let err = ext.point(vec![1, 3]).unwrap_err(); // y size is 3, max index is 2
    /// matches!(err, PointError::OutOfRangeIndex { .. });
    /// ```
    pub fn point(&self, coords: Vec<usize>) -> Result<Point, PointError> {
        Ok(Point {
            rank: self.rank_of_coords(&coords)?,
            extent: self.clone(),
        })
    }

    /// Returns the [`Point`] corresponding to the given linearized
    /// `rank` within this extent, using row-major order.
    ///
    /// # Errors
    ///
    /// Returns [`PointError::OutOfRangeRank`] if `rank >=
    /// self.num_ranks()`, i.e. when the requested rank lies outside
    /// the bounds of this extent.
    ///
    /// # Examples
    ///
    /// ```
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3);
    /// assert_eq!(ext.num_ranks(), 6);
    ///
    /// let p = ext.point_of_rank(4).unwrap();
    /// assert_eq!(p.coords(), vec![1, 1]); // row-major: x=1, y=1
    /// assert_eq!(p.rank(), 4);
    ///
    /// assert!(ext.point_of_rank(6).is_err()); // out of range
    /// ```
    pub fn point_of_rank(&self, rank: usize) -> Result<Point, PointError> {
        let total = self.num_ranks();
        if rank >= total {
            return Err(PointError::OutOfRangeRank { total, rank });
        }
        Ok(Point {
            rank,
            extent: self.clone(),
        })
    }

    /// Returns the number of dimensions in this extent.
    ///
    /// For example, an extent defined as `(x=2, y=3, z=4)` has
    /// dimensionality 3.
    pub fn len(&self) -> usize {
        self.sizes().len()
    }

    /// Returns true if this extent has zero dimensions.
    ///
    /// A 0-dimensional extent corresponds to the scalar case: a
    /// coordinate space with exactly one rank (the empty tuple `[]`).
    pub fn is_empty(&self) -> bool {
        self.sizes().is_empty()
    }

    /// Returns the total number of ranks (points) in this extent.
    ///
    /// This is the product of all dimension sizes, i.e. the number of
    /// distinct coordinates in row-major order.
    pub fn num_ranks(&self) -> usize {
        self.sizes().iter().product()
    }

    /// Convert this extent into its labels and sizes.
    pub fn into_inner(self) -> (Vec<String>, Vec<usize>) {
        match Arc::try_unwrap(self.inner) {
            Ok(data) => (data.labels, data.sizes),
            Err(shared) => (shared.labels.clone(), shared.sizes.clone()),
        }
    }

    /// Creates a slice representing the full extent.
    pub fn to_slice(&self) -> Slice {
        Slice::new_row_major(self.sizes())
    }

    /// Iterate over this extent's labels and sizes.
    pub fn iter(&self) -> impl Iterator<Item = (String, usize)> + use<'_> {
        self.labels()
            .iter()
            .zip(self.sizes().iter())
            .map(|(l, s)| (l.clone(), *s))
    }

    /// Iterate points in this extent.
    pub fn points(&self) -> ExtentPointsIterator<'_> {
        ExtentPointsIterator {
            extent: self,
            pos: CartesianIterator::new(self.sizes().to_vec()),
        }
    }
}

impl std::fmt::Display for Extent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.sizes().len();
        write!(f, "{{")?;
        for i in 0..n {
            write!(f, "'{}': {}", self.labels()[i], self.sizes()[i])?;
            if i != n - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "}}")?;
        Ok(())
    }
}

/// An iterator for points in an extent.
pub struct ExtentPointsIterator<'a> {
    extent: &'a Extent,
    pos: CartesianIterator,
}

impl<'a> Iterator for ExtentPointsIterator<'a> {
    type Item = Point;

    /// Advances the iterator and returns the next [`Point`] in
    /// row-major order.
    ///
    /// Internally, this takes the next coordinate tuple from the
    /// underlying [`CartesianIterator`], converts it into a
    /// linearized rank using [`Extent::rank_of_coords`], and packages
    /// both into a `Point`.
    ///
    /// If the extent has been fully traversed, or if the coordinates
    /// are invalid for the extent, `None` is returned.
    fn next(&mut self) -> Option<Self::Item> {
        let coords = self.pos.next()?;
        let rank = self.extent.rank_of_coords(&coords).ok()?;
        Some(Point {
            rank,
            extent: self.extent.clone(),
        })
    }
}

/// Errors that can occur when constructing or evaluating a `Point`.
#[derive(Debug, Error)]
pub enum PointError {
    /// The number of coordinates does not match the number of
    /// dimensions defined by the associated extent.
    ///
    /// This occurs when creating a `Point` with a coordinate vector
    /// of incorrect length relative to the dimensionality of the
    /// extent.
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimMismatch {
        /// Number of dimensions expected from the extent.
        expected: usize,
        /// Number of coordinates actually provided.
        actual: usize,
    },

    /// The provided rank is outside the valid range for the extent.
    ///
    /// Ranks are the linearized row-major indices of all points in
    /// the extent, spanning the half-open interval `[0, total)`. This
    /// error occurs when a rank greater than or equal to `total` is
    /// requested.
    #[error("out of range: total ranks {total}; does not contain rank {rank}")]
    OutOfRangeRank {
        /// The total number of valid ranks in the extent.
        total: usize,
        /// The rank that was requested but not valid.
        rank: usize,
    },

    /// A coordinate index is outside the valid range for its
    /// dimension.
    ///
    /// Each dimension of an extent has a size `size`, with valid
    /// indices spanning the half-open interval `[0, size)`. This
    /// error occurs when a coordinate `index` is greater than or
    /// equal to `size`.
    #[error("out of range: dim size {size}; does not contain index {index}")]
    OutOfRangeIndex {
        /// The size of the offending dimension.
        size: usize,
        /// The invalid coordinate index that was requested.
        index: usize,
    },
}

/// `Point` represents a specific coordinate within the
/// multi-dimensional space defined by an [`Extent`].
///
/// A `Point` can be viewed in two equivalent ways:
/// - **Coordinates**: a tuple of indices, one per dimension,
///   retrievable with [`Point::coord`] and [`Point::coords`].
/// - **Rank**: a single linearized index into the extent's row-major
///   ordering, retrievable with [`Point::rank`].
///
/// Internally, a `Point` stores:
/// - A `rank`: the row-major linearized index of this point.
/// - An `extent`: the extent that defines its dimensionality and
///   sizes.
///
/// These fields are private; use the accessor methods instead.
///
/// # Examples
///
/// ```
/// use ndslice::extent;
///
/// let ext = extent!(zone = 2, host = 4, gpu = 8);
/// let point = ext.point(vec![1, 2, 3]).unwrap();
///
/// // Coordinate-based access
/// assert_eq!(point.coord(0), 1);
/// assert_eq!(point.coord(1), 2);
/// assert_eq!(point.coord(2), 3);
///
/// // Rank-based access
/// assert_eq!(point.rank(), 1 * (4 * 8) + 2 * 8 + 3);
/// ```
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Hash, Debug)]
pub struct Point {
    rank: usize,
    extent: Extent,
}

/// An iterator over the coordinates of a [`Point`] in row-major
/// order.
///
/// Yields each coordinate component one at a time, without allocating
/// a full coordinate vector.
///
/// The iteration is deterministic: the `i`-th call to `next()`
/// returns the coordinate along axis `i`.
///
/// # Examples
/// ```
/// use ndslice::extent;
///
/// let ext = extent!(x = 2, y = 3);
/// let point = ext.point(vec![1, 2]).unwrap();
///
/// let coords: Vec<_> = point.coords_iter().collect();
/// assert_eq!(coords, vec![1, 2]);
/// ```
pub struct CoordIter<'a> {
    sizes: &'a [usize],
    rank: usize,
    stride: usize,
    axis: usize,
}

impl<'a> Iterator for CoordIter<'a> {
    type Item = usize;

    /// Computes and returns the coordinate for the current axis, then
    /// advances the iterator.
    ///
    /// Returns `None` once all dimensions have been exhausted.
    fn next(&mut self) -> Option<Self::Item> {
        if self.axis >= self.sizes.len() {
            return None;
        }
        self.stride /= self.sizes[self.axis];
        let q = self.rank / self.stride;
        self.rank %= self.stride;
        self.axis += 1;
        Some(q)
    }

    /// Returns the exact number of coordinates remaining.
    ///
    /// Since the dimensionality of the [`Point`] is known up front,
    /// this always returns `(n, Some(n))` where `n` is the number of
    /// axes not yet yielded.
    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.sizes.len().saturating_sub(self.axis);
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for CoordIter<'_> {}

impl<'a> IntoIterator for &'a Point {
    type Item = usize;
    type IntoIter = CoordIter<'a>;

    /// Iterate over the coordinate values of a [`Point`] (without
    /// allocating).
    ///
    /// This allows using a `Point` directly in a `for` loop:
    ///
    /// ```
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3);
    /// let point = ext.point(vec![1, 2]).unwrap();
    ///
    /// let coords: Vec<_> = (&point).into_iter().collect();
    /// assert_eq!(coords, vec![1, 2]);
    ///
    /// for coord in &point {
    ///     println!("{}", coord);
    /// }
    /// ```
    fn into_iter(self) -> Self::IntoIter {
        self.coords_iter()
    }
}

fn _assert_point_traits()
where
    Point: Send + Sync + 'static,
{
}

/// Extension trait for creating a `Point` from a coordinate vector
/// and an `Extent`.
///
/// This trait provides the `.in_(&extent)` method, which constructs a
/// `Point` using the caller as the coordinate vector and the given
/// extent as the shape context.
///
/// # Example
/// ```
/// use ndslice::Extent;
/// use ndslice::view::InExtent;
/// let extent = Extent::new(vec!["x".into(), "y".into()], vec![3, 4]).unwrap();
/// let point = vec![1, 2].in_(&extent).unwrap();
/// assert_eq!(point.rank(), 1 * 4 + 2);
/// ```
pub trait InExtent {
    fn in_(self, extent: &Extent) -> Result<Point, PointError>;
}

impl InExtent for Vec<usize> {
    /// Creates a `Point` with the provided coordinates in the given
    /// extent.
    ///
    /// Delegates to `Extent::point`.
    fn in_(self, extent: &Extent) -> Result<Point, PointError> {
        extent.point(self)
    }
}

impl Point {
    pub fn coords_iter(&self) -> CoordIter<'_> {
        CoordIter {
            sizes: self.extent.sizes(),
            rank: self.rank,
            stride: self.extent.sizes().iter().product(),
            axis: 0,
        }
    }

    /// Returns the coordinate of this [`Point`] along the given axis.
    ///
    /// The axis index `i` must be less than the number of dimensions
    /// in the [`Extent`], otherwise this function will panic.
    /// Computes only the `i`-th coordinate from the point's row-major
    /// `rank`, avoiding materialization of the full coordinate
    /// vector.
    ///
    /// # Examples
    /// ```
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3);
    /// let point = ext.point(vec![1, 2]).unwrap();
    /// assert_eq!(point.coord(0), 1); // x
    /// assert_eq!(point.coord(1), 2); // y
    /// ```
    pub fn coord(&self, i: usize) -> usize {
        self.coords_iter()
            .nth(i)
            .expect("coord(i): axis out of bounds")
    }

    /// Returns the full coordinate vector for this [`Point`]
    /// (allocates).
    ///
    /// The vector contains one coordinate per dimension of the
    /// [`Extent`], reconstructed from the point's row-major `rank`.
    ///
    /// # Examples
    /// ```
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3);
    /// let point = ext.point(vec![1, 2]).unwrap();
    /// assert_eq!(point.coords(), vec![1, 2]);
    /// ```
    pub fn coords(&self) -> Vec<usize> {
        self.coords_iter().collect()
    }

    /// Returns the linearized row-major rank of this [`Point`] within
    /// its [`Extent`].
    pub fn rank(&self) -> usize {
        self.rank
    }

    /// Returns the [`Extent`] that defines the coordinate space of
    /// this [`Point`].
    pub fn extent(&self) -> &Extent {
        &self.extent
    }

    /// Returns the number of dimensions in this [`Point`]'s
    /// [`Extent`].
    ///
    /// This corresponds to the dimensionality of the coordinate
    /// space, i.e. how many separate axes (labels) are present.
    ///
    /// # Examples
    /// ```
    /// use ndslice::extent;
    ///
    /// let ext = extent!(x = 2, y = 3, z = 4);
    /// let point = ext.point(vec![1, 2, 3]).unwrap();
    /// assert_eq!(point.len(), 3); // x, y, z
    /// ```
    pub fn len(&self) -> usize {
        self.extent.len()
    }

    /// Returns `true` if this [`Point`] lies in a 0-dimensional
    /// [`Extent`].
    ///
    /// A 0-D extent has no coordinate axes and exactly one valid
    /// point (the empty tuple `[]`).
    ///
    /// # Examples
    /// ```
    /// use ndslice::extent;
    ///
    /// let ext = extent!();
    /// let point = ext.point(vec![]).unwrap();
    /// assert!(point.is_empty());
    /// assert_eq!(point.len(), 0);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.extent.len() == 0
    }
}

impl std::fmt::Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let labels = self.extent.labels();
        let sizes = self.extent.sizes();
        let coords = self.coords();

        for i in 0..labels.len() {
            write!(f, "{}={}/{}", labels[i], coords[i], sizes[i])?;
            if i + 1 != labels.len() {
                write!(f, ",")?;
            }
        }
        Ok(())
    }
}

/// Errors that occur while operating on views.
#[derive(Debug, Error)]
pub enum ViewError {
    /// The provided dimension does not exist in the relevant extent.
    #[error("no such dimension: {0}")]
    InvalidDim(String),

    /// A view was attempted to be constructed from an empty (resolved) range.
    #[error("empty range: {range} for dimension {dim} of size {size}")]
    EmptyRange {
        range: Range,
        dim: String,
        size: usize,
    },

    #[error(transparent)]
    ExtentError(#[from] ExtentError),

    #[error("invalid range: selected ranks {selected} not a subset of base {base} ")]
    InvalidRange { base: Region, selected: Region },
}

/// `Region` describes a region of a possibly-larger space of ranks, organized into
/// a hyperrect.
///
/// Internally, region consist of a set of labels and a [`Slice`], as it allows for
/// a compact but useful representation of the ranks. However, this representation
/// may change in the future.
#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Region {
    labels: Vec<String>,
    slice: Slice,
}

impl Region {
    #[allow(dead_code)]
    fn empty() -> Region {
        Region {
            labels: Vec::new(),
            slice: Slice::new(0, Vec::new(), Vec::new()).unwrap(),
        }
    }

    /// The labels of the dimensions of this region.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// The slice representing this region.
    /// Note: this representation may change.
    pub fn slice(&self) -> &Slice {
        &self.slice
    }

    /// Convert this region into its constituent labels and slice.
    pub fn into_inner(self) -> (Vec<String>, Slice) {
        (self.labels, self.slice)
    }

    /// Returns the extent of the region.
    pub fn extent(&self) -> Extent {
        Extent::new(self.labels.clone(), self.slice.sizes().to_vec()).unwrap()
    }

    /// Returns `true` if this region is a subset of `other`, i.e., if `other`
    /// contains at least all of the ranks in this region.
    pub fn is_subset(&self, other: &Region) -> bool {
        let mut left = self.slice.iter().peekable();
        let mut right = other.slice.iter().peekable();

        loop {
            match (left.peek(), right.peek()) {
                (Some(l), Some(r)) => {
                    if l < r {
                        return false;
                    } else if l == r {
                        left.next();
                        right.next();
                    } else {
                        // r < l
                        right.next();
                    }
                }
                (Some(_), None) => return false,
                (None, _) => return true,
            }
        }
    }

    /// Remap the target to ranks in this region. The returned iterator iterates
    /// over each rank in `target`, providing the corresponding rank in `self`.
    /// This is useful when mapping between different subspaces.
    ///
    /// ```
    /// # use ndslice::Region;
    /// # use ndslice::ViewExt;
    /// # use ndslice::extent;
    /// let ext = extent!(replica = 8, gpu = 4);
    /// let replica1 = ext.range("replica", 1).unwrap();
    /// assert_eq!(replica1.extent(), extent!(replica = 1, gpu = 4));
    /// let replica1_gpu12 = replica1.range("gpu", 1..3).unwrap();
    /// assert_eq!(replica1_gpu12.extent(), extent!(replica = 1, gpu = 2));
    /// // The first rank in `replica1_gpu12` is the second rank in `replica1`.
    /// assert_eq!(
    ///     replica1.remap(&replica1_gpu12).unwrap().collect::<Vec<_>>(),
    ///     vec![1, 2],
    /// );
    /// ```
    pub fn remap(&self, target: &Region) -> Option<impl Iterator<Item = usize> + '_> {
        if !target.is_subset(self) {
            return None;
        }

        let mut ours = self.slice.iter().enumerate();
        let mut theirs = target.slice.iter();

        Some(std::iter::from_fn(move || {
            let needle = theirs.next()?;
            loop {
                let (index, value) = ours.next().unwrap();
                if value == needle {
                    break Some(index);
                }
            }
        }))
    }

    /// Returns the total number of ranks in the region.
    pub fn num_ranks(&self) -> usize {
        self.slice.len()
    }
}

// We would make this impl<T: Viewable> From<T> for View,
// except this conflicts with the blanket impl for From<&T> for View.
impl From<Extent> for Region {
    fn from(extent: Extent) -> Self {
        Region {
            labels: extent.labels().to_vec(),
            slice: extent.to_slice(),
        }
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.slice.offset() != 0 {
            write!(f, "{}+", self.slice.offset())?;
        }
        let n = self.labels.len();
        for i in 0..n {
            write!(
                f,
                "{}={}/{}",
                self.labels[i],
                self.slice.sizes()[i],
                self.slice.strides()[i]
            )?;
            if i != n - 1 {
                write!(f, ",")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegionParseError {
    #[error(transparent)]
    ParserError(#[from] ParserError),

    #[error(transparent)]
    SliceError(#[from] SliceError),
}

impl FromStr for Region {
    type Err = RegionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parser = Parser::new(s, &["+", "=", ",", "/"]);

        let offset: usize = if let Ok(offset) = parser.try_parse() {
            parser.expect("+")?;
            offset
        } else {
            0
        };
        let mut labels = Vec::new();
        let mut sizes = Vec::new();
        let mut strides = Vec::new();
        while !parser.is_empty() {
            // Don't allow trailing commas
            if !labels.is_empty() {
                parser.expect(",")?;
            }
            labels.push(parser.next_or_err("label")?.to_string());
            parser.expect("=")?;
            sizes.push(parser.try_parse()?);
            parser.expect("/")?;
            strides.push(parser.try_parse()?);
        }

        Ok(Region {
            labels,
            slice: Slice::new(offset, sizes, strides)?,
        })
    }
}

/// A View is a collection of items in a space indexed by a [`Region`].
pub trait View: Sized {
    /// The type of item in this view.
    type Item;

    /// The type of sub-view produced by manipulating (e.g., slicing) this view.
    type View: View;

    /// The ranks contained in this view.
    fn region(&self) -> Region;

    /// Retrieve the item corresponding to the given `rank` in the [`Region`]
    /// of this view. An implementation *MUST* return a value for all ranks
    /// defined in this view.
    fn get(&self, rank: usize) -> Option<Self::Item>;

    /// Subsets this view with the provided ranks. This is mainly used
    /// by combinators on Views themselves. The set of ranks passed in
    /// must be a subset of the ranks of the base view.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ViewError`.
    fn subset(&self, region: Region) -> Result<Self::View, ViewError>;
}

/// A [`Region`] is also a View.
impl View for Region {
    /// The type of item is the rank in the underlying space.
    type Item = usize;

    /// The type of sub-view is also a [`Region`].
    type View = Region;

    fn region(&self) -> Region {
        self.clone()
    }

    fn subset(&self, region: Region) -> Result<Region, ViewError> {
        if region.is_subset(self) {
            Ok(region)
        } else {
            Err(ViewError::InvalidRange {
                base: self.clone(),
                selected: region,
            })
        }
    }

    fn get(&self, rank: usize) -> Option<Self::Item> {
        self.slice.get(rank).ok()
    }
}

/// An [`Extent`] is also a View.
impl View for Extent {
    /// The type of item is the rank itself.
    type Item = usize;

    /// The type of sub-view can be a [`Region`], since
    /// [`Extent`] can only describe a complete space.
    type View = Region;

    fn region(&self) -> Region {
        Region {
            labels: self.labels().to_vec(),
            slice: self.to_slice(),
        }
    }

    fn subset(&self, region: Region) -> Result<Region, ViewError> {
        self.region().subset(region)
    }

    fn get(&self, rank: usize) -> Option<Self::Item> {
        if rank < self.num_ranks() {
            Some(rank)
        } else {
            None
        }
    }
}

/// An iterator over views.
pub struct ViewIterator {
    extent: Extent,     // Note that `extent` and...
    pos: SliceIterator, // ... `pos` share the same `Slice`.
}

impl Iterator for ViewIterator {
    type Item = (Point, usize);
    fn next(&mut self) -> Option<Self::Item> {
        // This is a rank in the base space.
        let rank = self.pos.next()?;
        // Here, we convert to view space.
        let coords = self.pos.slice.coordinates(rank).unwrap();
        let point = coords.in_(&self.extent).unwrap();
        Some((point, rank))
    }
}

/// Extension methods for view construction.
pub trait ViewExt: View {
    /// Construct a view comprising the range of points along the provided dimension.
    ///
    /// ## Examples
    ///
    /// ```
    /// use ndslice::Range;
    /// use ndslice::ViewExt;
    /// use ndslice::extent;
    ///
    /// let ext = extent!(zone = 4, host = 2, gpu = 8);
    ///
    /// // Subselect zone index 0.
    /// assert_eq!(ext.range("zone", 0).unwrap().iter().count(), 16);
    ///
    /// // Even GPUs within zone 0
    /// assert_eq!(
    ///     ext.range("zone", 0)
    ///         .unwrap()
    ///         .range("gpu", Range(0, None, 2))
    ///         .unwrap()
    ///         .iter()
    ///         .count(),
    ///     8
    /// );
    /// ```
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ViewError`.
    fn range<R: Into<Range>>(&self, dim: &str, range: R) -> Result<Self::View, ViewError>;

    /// Group by view on `dim`. The returned iterator enumerates all groups
    /// as views in the extent of `dim` to the last dimension of the view.
    ///
    /// ## Examples
    ///
    /// ```
    /// use ndslice::ViewExt;
    /// use ndslice::extent;
    ///
    /// let ext = extent!(zone = 4, host = 2, gpu = 8);
    ///
    /// // We generate one view for each zone.
    /// assert_eq!(ext.group_by("host").unwrap().count(), 4);
    ///
    /// let mut parts = ext.group_by("host").unwrap();
    ///
    /// let zone0 = parts.next().unwrap();
    /// let mut zone0_points = zone0.iter();
    /// assert_eq!(zone0.extent(), extent!(host = 2, gpu = 8));
    /// assert_eq!(
    ///     zone0_points.next().unwrap(),
    ///     (extent!(host = 2, gpu = 8).point(vec![0, 0]).unwrap(), 0)
    /// );
    /// assert_eq!(
    ///     zone0_points.next().unwrap(),
    ///     (extent!(host = 2, gpu = 8).point(vec![0, 1]).unwrap(), 1)
    /// );
    ///
    /// let zone1 = parts.next().unwrap();
    /// assert_eq!(zone1.extent(), extent!(host = 2, gpu = 8));
    /// assert_eq!(
    ///     zone1.iter().next().unwrap(),
    ///     (extent!(host = 2, gpu = 8).point(vec![0, 0]).unwrap(), 16)
    /// );
    /// ```
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ViewError`.
    fn group_by(&self, dim: &str) -> Result<impl Iterator<Item = Self::View>, ViewError>;

    /// The extent of this view. Every point in this space is defined.
    fn extent(&self) -> Extent;

    /// Iterate over all points in this region.
    fn iter<'a>(&'a self) -> impl Iterator<Item = (Point, Self::Item)> + 'a;

    /// Iterate over the values in the region.
    fn values<'a>(&'a self) -> impl Iterator<Item = Self::Item> + 'a;
}

impl<T: View> ViewExt for T {
    fn range<R: Into<Range>>(&self, dim: &str, range: R) -> Result<Self::View, ViewError> {
        let (labels, slice) = self.region().into_inner();
        let range = range.into();
        let dim = labels
            .iter()
            .position(|l| dim == l)
            .ok_or_else(|| ViewError::InvalidDim(dim.to_string()))?;
        let (mut offset, mut sizes, mut strides) = slice.into_inner();
        let (begin, end, step) = range.resolve(sizes[dim]);
        if end <= begin {
            return Err(ViewError::EmptyRange {
                range,
                dim: dim.to_string(),
                size: sizes[dim],
            });
        }

        offset += strides[dim] * begin;
        sizes[dim] = (end - begin).div_ceil(step);
        strides[dim] *= step;
        let slice = Slice::new(offset, sizes, strides).unwrap();

        self.subset(Region { labels, slice })
    }

    fn group_by(&self, dim: &str) -> Result<impl Iterator<Item = Self::View>, ViewError> {
        let (labels, slice) = self.region().into_inner();

        let dim = labels
            .iter()
            .position(|l| dim == l)
            .ok_or_else(|| ViewError::InvalidDim(dim.to_string()))?;

        let (offset, sizes, strides) = slice.into_inner();
        let mut ranks_iter = Slice::new(offset, sizes[..dim].to_vec(), strides[..dim].to_vec())
            .unwrap()
            .iter();

        let labels = labels[dim..].to_vec();
        let sizes = sizes[dim..].to_vec();
        let strides = strides[dim..].to_vec();

        Ok(std::iter::from_fn(move || {
            let rank = ranks_iter.next()?;
            let slice = Slice::new(rank, sizes.clone(), strides.clone()).unwrap();
            // These are always valid sub-views.
            Some(
                self.subset(Region {
                    labels: labels.clone(),
                    slice,
                })
                .unwrap(),
            )
        }))
    }

    fn extent(&self) -> Extent {
        let (labels, slice) = self.region().into_inner();
        Extent::new(labels, slice.sizes().to_vec()).unwrap()
    }

    fn iter(&self) -> impl Iterator<Item = (Point, Self::Item)> + '_ {
        let points = ViewIterator {
            extent: self.extent(),
            pos: self.region().slice().iter(),
        };

        points.map(|(point, _)| (point.clone(), self.get(point.rank()).unwrap()))
    }

    fn values(&self) -> impl Iterator<Item = Self::Item> + '_ {
        (0usize..self.extent().num_ranks()).map(|rank| self.get(rank).unwrap())
    }
}

/// Construct a new extent with the given set of dimension-size pairs.
///
/// ```
/// let s = ndslice::extent!(host = 2, gpu = 8);
/// assert_eq!(s.labels(), &["host".to_string(), "gpu".to_string()]);
/// assert_eq!(s.sizes(), &[2, 8]);
/// ```
#[macro_export]
macro_rules! extent {
    ( $( $label:ident = $size:expr ),* $(,)? ) => {
        {
            let mut labels = Vec::new();
            let mut sizes = Vec::new();

            $(
                labels.push(stringify!($label).to_string());
                sizes.push($size);
            )*

            $crate::view::Extent::new(labels, sizes).unwrap()
        }
    };
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Shape;
    use crate::shape;

    #[test]
    fn test_points_basic() {
        let extent = extent!(x = 4, y = 5, z = 6);
        let _p1 = extent.point(vec![1, 2, 3]).unwrap();
        let _p2 = vec![1, 2, 3].in_(&extent).unwrap();

        assert_eq!(extent.num_ranks(), 4 * 5 * 6);

        let p3 = extent.point_of_rank(0).unwrap();
        assert_eq!(p3.coords(), &[0, 0, 0]);
        assert_eq!(p3.rank(), 0);

        let p4 = extent.point_of_rank(1).unwrap();
        assert_eq!(p4.coords(), &[0, 0, 1]);
        assert_eq!(p4.rank(), 1);

        let p5 = extent.point_of_rank(2).unwrap();
        assert_eq!(p5.coords(), &[0, 0, 2]);
        assert_eq!(p5.rank(), 2);

        let p6 = extent.point_of_rank(6 * 5 + 1).unwrap();
        assert_eq!(p6.coords(), &[1, 0, 1]);
        assert_eq!(p6.rank(), 6 * 5 + 1);
        assert_eq!(p6.coord(0), 1);
        assert_eq!(p6.coord(1), 0);
        assert_eq!(p6.coord(2), 1);

        assert_eq!(extent.points().collect::<Vec<_>>().len(), 4 * 5 * 6);
        for (rank, point) in extent.points().enumerate() {
            let c = point.coords();
            let (x, y, z) = (c[0], c[1], c[2]);
            assert_eq!(z + y * 6 + x * 6 * 5, rank);
            assert_eq!(point.rank(), rank);
        }
    }

    macro_rules! assert_view {
        ($view:expr, $extent:expr,  $( $($coord:expr),+ => $rank:expr );* $(;)?) => {
            let view = $view;
            assert_eq!(view.extent(), $extent);
            let expected: Vec<_> = vec![$(($extent.point(vec![$($coord),+]).unwrap(), $rank)),*];
            let actual: Vec<_> = ViewExt::iter(&view).collect();
            assert_eq!(actual, expected);
        };
    }

    #[test]
    fn test_view_basic() {
        let extent = extent!(x = 4, y = 4);
        assert_view!(
            extent.range("x", 0..2).unwrap(),
            extent!(x = 2, y = 4),
            0, 0 => 0;
            0, 1 => 1;
            0, 2 => 2;
            0, 3 => 3;
            1, 0 => 4;
            1, 1 => 5;
            1, 2 => 6;
            1, 3 => 7;
        );
        assert_view!(
            extent.range("x", 1).unwrap().range("y", 2..).unwrap(),
            extent!(x = 1, y = 2),
            0, 0 => 6;
            0, 1 => 7;
        );
        assert_view!(
            extent.range("y", Range(0, None, 2)).unwrap(),
            extent!(x = 4, y = 2),
            0, 0 => 0;
            0, 1 => 2;
            1, 0 => 4;
            1, 1 => 6;
            2, 0 => 8;
            2, 1 => 10;
            3, 0 => 12;
            3, 1 => 14;
        );
        assert_view!(
            extent.range("y", Range(0, None, 2)).unwrap().range("x", 2..).unwrap(),
            extent!(x = 2, y = 2),
            0, 0 => 8;
            0, 1 => 10;
            1, 0 => 12;
            1, 1 => 14;
        );

        let extent = extent!(x = 10, y = 2);
        assert_view!(
            extent.range("x", Range(0, None, 2)).unwrap(),
            extent!(x = 5, y = 2),
            0, 0 => 0;
            0, 1 => 1;
            1, 0 => 4;
            1, 1 => 5;
            2, 0 => 8;
            2, 1 => 9;
            3, 0 => 12;
            3, 1 => 13;
            4, 0 => 16;
            4, 1 => 17;
        );
        assert_view!(
            extent.range("x", Range(0, None, 2)).unwrap().range("x", 2..).unwrap().range("y", 1).unwrap(),
            extent!(x = 3, y = 1),
            0, 0 => 9;
            1, 0 => 13;
            2, 0 => 17;
        );

        let extent = extent!(zone = 4, host = 2, gpu = 8);
        assert_view!(
            extent.range("zone", 0).unwrap().range("gpu", Range(0, None, 2)).unwrap(),
            extent!(zone = 1, host = 2, gpu = 4),
            0, 0, 0 => 0;
            0, 0, 1 => 2;
            0, 0, 2 => 4;
            0, 0, 3 => 6;
            0, 1, 0 => 8;
            0, 1, 1 => 10;
            0, 1, 2 => 12;
            0, 1, 3 => 14;
        );

        let extent = extent!(x = 3);
        assert_view!(
            extent.range("x", Range(0, None, 2)).unwrap(),
            extent!(x = 2),
            0 => 0;
            1 => 2;
        );
    }

    #[test]
    fn test_point_indexing() {
        let extent = Extent::new(vec!["x".into(), "y".into(), "z".into()], vec![4, 5, 6]).unwrap();
        let point = extent.point(vec![1, 2, 3]).unwrap();

        assert_eq!(point.coord(0), 1);
        assert_eq!(point.coord(1), 2);
        assert_eq!(point.coord(2), 3);
    }

    #[test]
    #[should_panic]
    fn test_point_indexing_out_of_bounds() {
        let extent = Extent::new(vec!["x".into(), "y".into()], vec![4, 5]).unwrap();
        let point = extent.point(vec![1, 2]).unwrap();

        let _ = point.coord(5); // Should panic
    }

    #[test]
    fn test_point_into_iter() {
        let extent = Extent::new(vec!["x".into(), "y".into(), "z".into()], vec![4, 5, 6]).unwrap();
        let point = extent.point(vec![1, 2, 3]).unwrap();

        let coords: Vec<usize> = (&point).into_iter().collect();
        assert_eq!(coords, vec![1, 2, 3]);

        let mut sum = 0;
        for coord in &point {
            sum += coord;
        }
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_extent_basic() {
        let extent = extent!(x = 10, y = 5, z = 1);
        assert_eq!(
            extent.iter().collect::<Vec<_>>(),
            vec![
                ("x".to_string(), 10),
                ("y".to_string(), 5),
                ("z".to_string(), 1)
            ]
        );
    }

    #[test]
    fn test_extent_display() {
        let extent = Extent::new(vec!["x".into(), "y".into(), "z".into()], vec![4, 5, 6]).unwrap();
        assert_eq!(format!("{}", extent), "{'x': 4, 'y': 5, 'z': 6}");

        let empty_extent = Extent::new(vec![], vec![]).unwrap();
        assert_eq!(format!("{}", empty_extent), "{}");
    }

    #[test]
    fn extent_label_helpers() {
        let e = extent!(zone = 3, host = 2, gpu = 4);
        for (i, (lbl, sz)) in e.iter().enumerate() {
            assert_eq!(e.position(&lbl), Some(i));
            assert_eq!(e.size(&lbl), Some(sz));
        }
        assert_eq!(e.position("nope"), None);
        assert_eq!(e.size("nope"), None);
    }

    #[test]
    fn test_extent_0d() {
        let e = Extent::new(vec![], vec![]).unwrap();
        assert_eq!(e.num_ranks(), 1);

        let points: Vec<_> = e.points().collect();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].coords(), &[]);
        assert_eq!(points[0].rank(), 0);

        // Iterator invariants for 0-D point.
        let mut it = (&points[0]).into_iter();
        assert_eq!(it.len(), 0);
        assert!(it.next().is_none()); // no items
        assert!(it.next().is_none()); // fused
    }

    #[test]
    fn extent_unity_equiv_to_0d() {
        let e = Extent::unity();
        assert!(e.is_empty());
        assert_eq!(e.num_ranks(), 1);
        let pts: Vec<_> = e.points().collect();
        assert_eq!(pts.len(), 1);
        assert_eq!(pts[0].rank(), 0);
        assert!(pts[0].coords().is_empty());
    }

    #[test]
    fn test_point_display() {
        let extent = Extent::new(vec!["x".into(), "y".into(), "z".into()], vec![4, 5, 6]).unwrap();
        let point = extent.point(vec![1, 2, 3]).unwrap();
        assert_eq!(format!("{}", point), "x=1/4,y=2/5,z=3/6");

        assert!(extent.point(vec![]).is_err());

        let empty_extent = Extent::new(vec![], vec![]).unwrap();
        let empty_point = empty_extent.point(vec![]).unwrap();
        assert_eq!(format!("{}", empty_point), "");
    }

    #[test]
    fn test_relative_point() {
        // Given a rank in the root shape, return the corresponding point in the
        // provided shape, which is a view of the root shape.
        pub fn relative_point(rank_on_root_mesh: usize, shape: &Shape) -> anyhow::Result<Point> {
            let coords = shape.slice().coordinates(rank_on_root_mesh)?;
            let extent = Extent::new(shape.labels().to_vec(), shape.slice().sizes().to_vec())?;
            Ok(extent.point(coords)?)
        }

        let root_shape = shape! { replicas = 4, hosts = 4, gpus = 4 };
        // rows are `hosts`, cols are gpus
        // replicas = 0
        //     0,    1,  2,    3,
        //     (4),  5,  (6),  7,
        //     8,    9,  10,   11,
        //     (12), 13, (14), 15,
        // replicas = 3, which is [replicas=0] + 48
        //     48,   49, 50,   51,
        //     (52), 53, (54), 55,
        //     56,   57, 58,   59,
        //     (60), 61, (62), 63,
        let sliced_shape = root_shape
            .select("replicas", crate::Range(0, Some(4), 3))
            .unwrap()
            .select("hosts", crate::Range(1, Some(4), 2))
            .unwrap()
            .select("gpus", crate::Range(0, Some(4), 2))
            .unwrap();
        let ranks_on_root_mesh = &[4, 6, 12, 14, 52, 54, 60, 62];
        assert_eq!(
            sliced_shape.slice().iter().collect::<Vec<_>>(),
            ranks_on_root_mesh,
        );

        let ranks_on_sliced_mesh = ranks_on_root_mesh
            .iter()
            .map(|&r| relative_point(r, &sliced_shape).unwrap().rank());
        assert_eq!(
            ranks_on_sliced_mesh.collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn test_iter_subviews() {
        let extent = extent!(zone = 4, host = 4, gpu = 8);

        assert_eq!(extent.group_by("gpu").unwrap().count(), 16);
        assert_eq!(extent.group_by("zone").unwrap().count(), 1);

        let mut parts = extent.group_by("gpu").unwrap();
        assert_view!(
            parts.next().unwrap(),
            extent!(gpu = 8),
            0 => 0;
            1 => 1;
            2 => 2;
            3 => 3;
            4 => 4;
            5 => 5;
            6 => 6;
            7 => 7;
        );
        assert_view!(
            parts.next().unwrap(),
            extent!(gpu = 8),
            0 => 8;
            1 => 9;
            2 => 10;
            3 => 11;
            4 => 12;
            5 => 13;
            6 => 14;
            7 => 15;
        );
    }

    #[test]
    fn test_view_values() {
        let extent = extent!(x = 4, y = 4);
        assert_eq!(
            extent.values().collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>()
        );
        let region = extent.range("y", 1).unwrap();
        assert_eq!(region.values().collect::<Vec<_>>(), vec![1, 5, 9, 13]);
    }

    #[test]
    fn region_is_subset_algebra() {
        let e = extent!(x = 5, y = 4);
        let a = e.range("x", 1..4).unwrap(); // 3×4
        let b = a.range("y", 1..3).unwrap(); // 3×2 (subset of a)
        let c = e.range("x", 0..2).unwrap(); // 2×4 (overlaps, not subset of a)

        assert!(b.region().is_subset(&a.region()));
        assert!(b.region().is_subset(&e.region()));
        assert!(a.region().is_subset(&e.region()));

        assert!(!c.region().is_subset(&a.region()));
        assert!(c.region().is_subset(&e.region()));
    }

    #[test]
    fn test_remap() {
        let region: Region = extent!(x = 4, y = 4).into();
        // Self-remap
        assert_eq!(
            region.remap(&region).unwrap().collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>()
        );

        let subset = region.range("x", 2..).unwrap();
        assert_eq!(subset.num_ranks(), 8);
        assert_eq!(
            region.remap(&subset).unwrap().collect::<Vec<_>>(),
            vec![8, 9, 10, 11, 12, 13, 14, 15],
        );

        let subset = subset.range("y", 1).unwrap();
        assert_eq!(subset.num_ranks(), 2);
        assert_eq!(
            region.remap(&subset).unwrap().collect::<Vec<_>>(),
            vec![9, 13],
        );

        // Test double subsetting:

        let ext = extent!(replica = 8, gpu = 4);
        let replica1 = ext.range("replica", 1).unwrap();
        assert_eq!(replica1.extent(), extent!(replica = 1, gpu = 4));
        let replica1_gpu12 = replica1.range("gpu", 1..3).unwrap();
        assert_eq!(replica1_gpu12.extent(), extent!(replica = 1, gpu = 2));
        // The first rank in `replica1_gpu12` is the second rank in `replica1`.
        assert_eq!(
            replica1.remap(&replica1_gpu12).unwrap().collect::<Vec<_>>(),
            vec![1, 2],
        );
    }

    use proptest::prelude::*;

    use crate::strategy::gen_extent;
    use crate::strategy::gen_slice;

    prop_compose! {
        fn gen_region()(slice in gen_slice(5, 1024)) -> Region {
            let labels = (0..slice.num_dim()).map(|d| format!("dim{}", d)).collect();
            Region {labels, slice}
        }
    }

    proptest! {
        #[test]
        fn test_region_parser(region in gen_region()) {
            // Roundtrip display->parse correctly and preserves equality.
            assert_eq!(
                region,
                region.to_string().parse::<Region>().unwrap(),
                "failed to roundtrip region {}", region
            );
        }
    }

    proptest! {
        // `Point.coord(i)` and `(&Point).into_iter()` must agree with
        // `coords()`.
        #[test]
        fn point_coord_and_iter_agree(extent in gen_extent(0..=4, 8)) {
            for p in extent.points() {
                let via_coords = p.coords();
                let via_into_iter: Vec<_> = (&p).into_iter().collect();
                prop_assert_eq!(via_into_iter, via_coords.clone(), "coord_iter mismatch for {}", p);

                for (i, &coord) in via_coords.iter().enumerate() {
                    prop_assert_eq!(p.coord(i), coord, "coord(i) mismatch at axis {} for {}", i, p);
                }
            }
        }

        // `points().count()` must equal `num_ranks()`.
        #[test]
        fn points_count_matches_num_ranks(extent in gen_extent(0..=4, 8)) {
            let c = extent.points().count();
            prop_assert_eq!(c, extent.num_ranks(), "count {} != num_ranks {}", c, extent.num_ranks());
        }

        // `CoordIter` must report an exact size, decreasing by one on
        // each iteration, ending at zero, and yield the same sequence
        // as `coords()`.
        #[test]
        fn coord_iter_exact_size_invariants(extent in gen_extent(0..=4, 8)) {
            for p in extent.points() {
                let mut it = (&p).into_iter();

                // Initial length matches dimensionality; size_hint
                // agrees.
                let mut remaining = p.len();
                prop_assert_eq!(it.len(), remaining);
                prop_assert_eq!(it.size_hint(), (remaining, Some(remaining)));

                // Track yielded coords to compare with p.coords()
                let mut yielded = Vec::with_capacity(remaining);

                // len() decreases by 1 per step; size_hint stays
                // consistent.
                while let Some(v) = it.next() {
                    yielded.push(v);
                    remaining -= 1;
                    prop_assert_eq!(it.len(), remaining);
                    prop_assert_eq!(it.size_hint(), (remaining, Some(remaining)));
                }

                // Exhausted: zero remaining, fused behavior (keeps
                // returning None).
                prop_assert_eq!(remaining, 0);
                prop_assert!(it.next().is_none());
                prop_assert!(it.next().is_none());

                // Sequence equals full coords() reconstruction.
                prop_assert_eq!(yielded, p.coords());
            }
        }

        // `rank_of_coords` must reject coordinate vectors of the
        // wrong length with a `PointError::DimMismatch` that reports
        // both the expected and actual dimensionality.
        #[test]
        fn rank_of_coords_dim_mismatch(extent in gen_extent(0..=4, 8)) {
            let want = extent.len();
            // Pick a wrong coords length for the extent.
            let wrong = if want == 0 { 1 } else { want - 1 };
            let bad = vec![0usize; wrong];

            match extent.rank_of_coords(&bad).unwrap_err() {
                PointError::DimMismatch { expected, actual } => {
                    prop_assert_eq!(expected, want, "expected len mismatch");
                    prop_assert_eq!(actual, wrong, "actual len mismatch");
                }
                other => prop_assert!(false, "expected DimMismatch, got {:?}", other),
            }
        }

        // `rank_of_coords` must reject coordinates with an index ≥
        // the dimension's size, producing
        // `PointError::OutOfRangeIndex` that reports both the
        // dimension size and the offending index.
        #[test]
        fn rank_of_coords_out_of_range_index(extent in gen_extent(1..=4, 8)) {
            // `extent` has at least 1 dim here.
            let sizes = extent.sizes().to_vec();
            // Start with a valid zero vector.
            let mut coords = vec![0usize; sizes.len()];
            // Bump one axis out of range.
            let axis = 0usize;
            coords[axis] = sizes[axis];

            match extent.rank_of_coords(&coords).unwrap_err() {
                PointError::OutOfRangeIndex { size, index } => {
                    prop_assert_eq!(size, sizes[axis], "reported size mismatch");
                    prop_assert_eq!(index, sizes[axis], "reported index mismatch");
                }
                other => prop_assert!(false, "expected OutOfRangeIndex, got {:?}", other),
            }
        }

        /// `point_of_rank` must reject `rank == num_ranks()` (first OOB),
        /// returning `OutOfRangeRank` with the correct fields.
        #[test]
        fn point_of_rank_out_of_range(extent in gen_extent(0..=4, 8)) {
            let total = extent.num_ranks(); // first invalid rank
            match extent.point_of_rank(total).unwrap_err() {
                PointError::OutOfRangeRank { total: t, rank: r } => {
                    prop_assert_eq!(t, total, "reported total mismatch");
                    prop_assert_eq!(r, total, "reported rank mismatch");
                }
                other => prop_assert!(false, "expected OutOfRangeRank, got {:?}", other),
            }
        }
    }
}
