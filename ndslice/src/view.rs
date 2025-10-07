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
use crate::Shape; // exclusively for `impl From<Shape> for Extent`
use crate::Slice;
use crate::SliceError;
use crate::SliceIterator;
use crate::parse::Parser;
use crate::parse::ParserError;

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

    /// An overlapping label was found.
    ///
    /// This occurs when attempting to combine extents that
    /// share one or more dimension labels, which is not allowed.
    #[error("overlapping label found: {label}")]
    OverlappingLabel {
        /// The label that appears in both extents.
        label: String,
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

impl From<&Shape> for Extent {
    fn from(s: &Shape) -> Self {
        // Safe: Shape guarantees labels.len() == sizes.len().
        Extent::new(s.labels().to_vec(), s.slice().sizes().to_vec()).unwrap()
    }
}

impl From<Shape> for Extent {
    fn from(s: Shape) -> Self {
        Extent::from(&s)
    }
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
        ExtentPointsIterator::new(self)
    }

    /// Append the dimensions of `other` to this extent, preserving order.
    ///
    /// Duplicate labels are not allowed: if any label in `other` already appears
    /// in `self`, this returns `ExtentError::OverlappingLabel`.
    ///
    /// This operation is not commutative: `a.concat(&b)` may differ from
    /// `b.concat(&a)`.
    pub fn concat(&self, other: &Extent) -> Result<Self, ExtentError> {
        use std::collections::HashSet;
        // Check for any overlapping labels in linear time using hash set
        let lhs: HashSet<&str> = self.labels().iter().map(|s| s.as_str()).collect();
        if let Some(dup) = other.labels().iter().find(|l| lhs.contains(l.as_str())) {
            return Err(ExtentError::OverlappingLabel { label: dup.clone() });
        }
        // Combine labels and sizes from both extents with pre-allocated memory
        let mut labels = self.labels().to_vec();
        let mut sizes = self.sizes().to_vec();
        labels.reserve(other.labels().len());
        sizes.reserve(other.sizes().len());
        labels.extend(other.labels().iter().cloned());
        sizes.extend(other.sizes().iter().copied());
        Extent::new(labels, sizes)
    }
}

/// Label formatting utilities shared across `Extent`, `Region`, and
/// `Point`.
///
/// - [`is_safe_ident`] determines whether a label can be printed
///   bare, i.e. consists only of `[A-Za-z0-9_]+`.
/// - [`fmt_label`] returns the label unchanged if it is safe,
///   otherwise quotes it using Rust string-literal syntax (via
///   `format!("{:?}", s)`).
///
/// This ensures a consistent, unambiguous display format across all
/// types.
mod labels {
    /// A "safe" identifier consists only of ASCII alphanumeric chars
    /// or underscores (`[A-Za-z0-9_]+`). These can be displayed
    /// without quotes.
    pub(super) fn is_safe_ident(s: &str) -> bool {
        s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Render a label according to the quoting rule:
    /// - Safe identifiers are returned as-is.
    /// - Otherwise the label is quoted using Rust string literal
    ///   syntax (via `format!("{:?}", s)`).
    pub(super) fn fmt_label(s: &str) -> String {
        if is_safe_ident(s) {
            s.to_string()
        } else {
            format!("{:?}", s)
        }
    }
}

/// Formats an `Extent` as a compact map‐literal:
/// ```text
/// {label: size, label: size, ...}
/// ```
/// # Grammar
///
/// ```text
/// Extent   := "{" [ Pair ( "," Pair )* ] "}"
/// Pair     := Label ": " Size
/// Label    := SafeIdent | Quoted
/// SafeIdent:= [A-Za-z0-9_]+
/// Quoted   := "\"" ( [^"\\] | "\\" . )* "\""
/// Size     := [0-9]+
/// ```
///
/// # Quoting rules
///
/// - Labels that are **not** `SafeIdent` are rendered using Rust
///   string literal syntax (via `format!("{:?}", label)`), e.g.
///   `"dim/0"` or `"x y"`.
/// - "Safe" means ASCII alphanumeric or underscore (`[A-Za-z0-9_]+`).
///   Everything else is quoted. This keeps common identifiers
///   unquoted and unambiguous.
///
/// # Examples
///
/// ```text
/// {x: 4, y: 5, z: 6}
/// {"dim/0": 3, "dim,1": 5}
/// {}
/// ```
///
/// Implementation note: label rendering goes through `fmt_label`,
/// which emits the label as-is for safe idents, otherwise as a Rust
/// string literal.
impl std::fmt::Display for Extent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for i in 0..self.sizes().len() {
            write!(
                f,
                "{}: {}",
                labels::fmt_label(&self.labels()[i]),
                self.sizes()[i]
            )?;
            if i + 1 != self.sizes().len() {
                write!(f, ", ")?;
            }
        }
        write!(f, "}}")
    }
}

/// An iterator for points in an extent.
pub struct ExtentPointsIterator<'a> {
    extent: &'a Extent,
    next_rank: usize,
}

impl<'a> ExtentPointsIterator<'a> {
    pub fn new(extent: &'a Extent) -> Self {
        Self {
            extent,
            next_rank: 0,
        }
    }
}

impl<'a> Iterator for ExtentPointsIterator<'a> {
    type Item = Point;

    /// Advances the iterator and returns the next [`Point`] in
    /// row-major order.
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_rank == self.extent.num_ranks() {
            return None;
        }

        let p = Point {
            rank: self.next_rank,
            extent: self.extent.clone(),
        };
        self.next_rank += 1;
        Some(p)
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

    /// Failed to parse a point from a string.
    #[error("failed to parse point: {reason}")]
    ParseError { reason: String },
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

/// Formats a `Point` as a comma-separated list of per-axis
/// coordinates against the point’s extent:
/// ```text
/// label=coord/size[,label=coord/size,...]
/// ```
///
/// # Grammar
/// ```text
/// Point    := Pair ( "," Pair )*
/// Pair     := Label "=" Coord "/" Size
/// Label    := SafeIdent | Quoted
/// SafeIdent:= [A-Za-z0-9_]+
/// Quoted   := "\"" ( [^"\\] | "\\" . )* "\""   // Rust string-literal style
/// Coord    := [0-9]+
/// Size     := [0-9]+
/// ```
///
/// # Quoting rules
/// - Labels that are **not** `SafeIdent` are rendered using Rust
///   string-literal syntax (via `labels::fmt_label`), e.g. `"dim/0"` or
///   `"x y"`.
/// - "Safe" means ASCII alphanumeric or underscore (`[A-Za-z0-9_]+`).
///   Everything else is quoted.
/// - Coordinates are shown in row-major order and each is paired with
///   that axis’s size from the point’s extent.
///
/// # Examples
/// ```text
/// x=1/4,y=2/5,z=3/6
/// "dim/0"=1/3,"dim,1"=2/5
/// ```
///
/// Implementation note: label rendering is delegated to
/// `labels::fmt_label` to keep quoting behavior consistent with
/// `Extent` and `Region`.
impl std::fmt::Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let labels = self.extent.labels();
        let sizes = self.extent.sizes();
        let coords = self.coords();

        for i in 0..labels.len() {
            write!(
                f,
                "{}={}/{}",
                labels::fmt_label(&labels[i]),
                coords[i],
                sizes[i]
            )?;
            if i + 1 != labels.len() {
                write!(f, ",")?;
            }
        }
        Ok(())
    }
}

impl FromStr for Point {
    type Err = PointError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.is_empty() {
            let empty_extent = Extent::unity();
            return empty_extent.point(vec![]);
        }

        let mut labels = Vec::new();
        let mut coords = Vec::new();
        let mut sizes = Vec::new();

        let mut chars = s.chars().peekable();

        while chars.peek().is_some() {
            while chars.peek() == Some(&' ') {
                chars.next();
            }

            if chars.peek().is_none() {
                break;
            }

            let label = if chars.peek() == Some(&'"') {
                chars.next(); // quote
                let mut label = String::new();
                let mut escaped = false;

                // Consume label until closing quote
                for ch in chars.by_ref() {
                    if escaped {
                        match ch {
                            '"' => label.push('"'),
                            '\\' => label.push('\\'),
                            _ => {
                                label.push('\\');
                                label.push(ch);
                            }
                        }
                        escaped = false;
                    } else if ch == '\\' {
                        escaped = true;
                    } else if ch == '"' {
                        break;
                    } else {
                        label.push(ch);
                    }
                }

                if label.is_empty() {
                    return Err(PointError::ParseError {
                        reason: "empty quoted label".to_string(),
                    });
                }

                label
            } else {
                let mut label = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch == '=' || ch == ' ' {
                        break;
                    }
                    label.push(chars.next().unwrap());
                }

                if label.is_empty() {
                    return Err(PointError::ParseError {
                        reason: "missing label".to_string(),
                    });
                }

                label
            };

            while chars.peek() == Some(&' ') {
                chars.next();
            }

            if chars.next() != Some('=') {
                return Err(PointError::ParseError {
                    reason: format!("expected '=' after label '{}'", label),
                });
            }

            while chars.peek() == Some(&' ') {
                chars.next();
            }

            let mut coord = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == '/' || ch == ' ' {
                    break;
                }
                coord.push(chars.next().unwrap());
            }

            if coord.is_empty() {
                return Err(PointError::ParseError {
                    reason: format!("missing coordinate for dimension '{}'", label),
                });
            }

            while chars.peek() == Some(&' ') {
                chars.next();
            }

            if chars.next() != Some('/') {
                return Err(PointError::ParseError {
                    reason: format!("expected '/' after coordinate for dimension '{}'", label),
                });
            }

            while chars.peek() == Some(&' ') {
                chars.next();
            }

            let mut size = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == ',' || ch == ' ' {
                    break;
                }
                size.push(chars.next().unwrap());
            }

            if size.is_empty() {
                return Err(PointError::ParseError {
                    reason: format!("missing size for dimension '{}'", label),
                });
            }

            let coord = coord.parse::<usize>().map_err(|e| PointError::ParseError {
                reason: format!(
                    "invalid coordinate '{}' for dimension '{}': {}",
                    coord, label, e
                ),
            })?;

            let size = size.parse::<usize>().map_err(|e| PointError::ParseError {
                reason: format!("invalid size '{}' for dimension '{}': {}", size, label, e),
            })?;

            labels.push(label);
            coords.push(coord);
            sizes.push(size);

            while chars.peek() == Some(&' ') {
                chars.next();
            }

            if chars.peek() == Some(&',') {
                chars.next(); // consume comma
                while chars.peek() == Some(&' ') {
                    chars.next();
                }
                // After consuming a comma, there must be another dimension
                if chars.peek().is_none() {
                    return Err(PointError::ParseError {
                        reason: "trailing comma".to_string(),
                    });
                }
            }
        }

        let extent = Extent::new(labels, sizes).map_err(|e| PointError::ParseError {
            reason: format!("failed to create extent: {}", e),
        })?;

        extent.point(coords)
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
    InvalidRange {
        base: Box<Region>,
        selected: Box<Region>,
    },
}

/// Errors that occur while operating on Region.
#[derive(Debug, Error)]
pub enum RegionError {
    #[error("invalid point: this point does not belong to this region: {0}")]
    InvalidPoint(String),

    #[error("out of range base rank: this base rank {0} does not belong to this region: {0}")]
    OutOfRangeBaseRank(usize, String),
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

    /// Constructor to build arbitrary regions (incl. non-contiguous /
    /// offset).
    #[allow(dead_code)]
    pub fn new(labels: Vec<String>, slice: Slice) -> Self {
        Self { labels, slice }
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

    /// Convert a rank in this region's extent into its corresponding rank in
    /// the base space defined by the region's `Slice`.
    pub fn base_rank_of_point(&self, p: Point) -> Result<usize, RegionError> {
        if p.extent() != &self.extent() {
            return Err(RegionError::InvalidPoint(
                "mismatched extent: p must be a point in this region’s extent".to_string(),
            ));
        }

        Ok(self
            .slice()
            .location(&p.coords())
            .expect("should have valid location since extent is checked"))
    }

    /// Convert a rank in the base space into the corresponding `Point` in this
    /// region's extent (if the base rank lies within the region's `Slice`).
    pub fn point_of_base_rank(&self, rank: usize) -> Result<Point, RegionError> {
        let coords = self
            .slice()
            .coordinates(rank)
            .map_err(|e| RegionError::OutOfRangeBaseRank(rank, e.to_string()))?;
        Ok(self
            .extent()
            .point(coords)
            .expect("should have valid point since coords is from this region"))
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

impl From<&Shape> for Region {
    fn from(s: &Shape) -> Self {
        Region {
            labels: s.labels().to_vec(),
            slice: s.slice().clone(),
        }
    }
}

impl From<Shape> for Region {
    fn from(s: Shape) -> Self {
        Region::from(&s)
    }
}

/// Formats a `Region` in a compact rectangular syntax:
/// ```text
/// [offset+]label=size/stride[,label=size/stride,...]
/// ```
/// # Grammar
///
/// ```text
/// Region   := [ Offset "+" ] Pair ( "," Pair )*
/// Offset   := [0-9]+
/// Pair     := Label "=" Size "/" Stride
/// Label    := SafeIdent | Quoted
/// SafeIdent:= [A-Za-z0-9_]+
/// Quoted   := "\"" ( [^"\\] | "\\" . )* "\""   // Rust string literal style
/// Size     := [0-9]+
/// Stride   := [0-9]+
/// ```
///
/// # Quoting rules
///
/// - Labels that are **not** `SafeIdent` are rendered using Rust
///   string-literal syntax (via `format!("{:?}", label)`).
/// - "Safe" means ASCII alphanumeric or underscore
///   (`[A-Za-z0-9_]+`). Everything else is quoted.
/// - The optional `Offset+` prefix appears only when the slice offset
///   is nonzero.
///
/// # Examples
///
/// ```text
/// x=2/1,y=3/2
/// 8+"dim/0"=4/1,"dim,1"=5/4
/// ```
///
/// # Notes
///
/// This format is both human-readable and machine-parsable. The
/// corresponding [`FromStr`] implementation accepts exactly this
/// grammar, including quoted labels. The quoting rule makes
/// round-trip unambiguous.
impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.slice.offset() != 0 {
            write!(f, "{}+", self.slice.offset())?;
        }
        for i in 0..self.labels.len() {
            write!(
                f,
                "{}={}/{}",
                labels::fmt_label(&self.labels[i]),
                self.slice.sizes()[i],
                self.slice.strides()[i]
            )?;
            if i + 1 != self.labels.len() {
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

/// Parses a `Region` from the textual form emitted by
/// [`Display`](Self::fmt).
///
/// The accepted syntax and quoting rules are exactly those documented
/// on `Display`: comma-separated `label=size/stride` pairs with an
/// optional `offset+` prefix, and labels that are either safe
/// identifiers or Rust string literals.
///
/// Returns a `RegionParseError` on malformed input.
///
/// # Examples
/// ```
/// use ndslice::view::Region;
///
/// let r: Region = "x=2/1,y=3/2".parse().unwrap();
/// assert_eq!(r.labels(), &["x", "y"]);
///
/// let q: Region = "8+\"dim/0\"=4/1".parse().unwrap();
/// assert_eq!(q.labels(), &["dim/0"]);
/// ```
impl std::str::FromStr for Region {
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
            if !labels.is_empty() {
                parser.expect(",")?;
            }

            // Accept either a quoted label output or a bare token.
            let label = if parser.peek_char() == Some('"') {
                parser.parse_string_literal()?
            } else {
                parser.next_or_err("label")?.to_string()
            };
            labels.push(label);

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

/// Dense builder: constructs a mesh from a complete sequence of
/// values in the canonical order for `region`.
///
/// # Semantics
/// - Input must contain exactly `region.num_ranks()` items.
/// - Values must be in the same order as `region.slice().iter()`
///   (i.e., the order observed by `ViewExt::values()`).
///
/// # Errors
/// Returns [`Self::Error`] if `values.len() != region.num_ranks()`.
///
/// See also: [`BuildFromRegionIndexed`]
pub trait BuildFromRegion<T>: Sized {
    type Error;

    /// Validates cardinality/shape and constructs a mesh.
    fn build_dense(region: Region, values: Vec<T>) -> Result<Self, Self::Error>;

    /// Caller guarantees correct cardinality/order; no validation.
    fn build_dense_unchecked(region: Region, values: Vec<T>) -> Self;
}

/// Indexed builder: constructs a mesh from sparse `(rank, value)`
/// pairs.
///
/// # Semantics
/// - Every rank in `0..region.num_ranks()` must be provided at least
///   once.
/// - Out-of-bounds ranks (`rank >= num_ranks()`) cause an error.
/// - Missing ranks cause an error.
/// - Duplicate ranks are allowed; the **last write wins**.
///
/// # Errors
/// Returns [`Self::Error`] if coverage is incomplete or a rank is out
/// of bounds.
pub trait BuildFromRegionIndexed<T>: Sized {
    type Error;

    /// Validates coverage and bounds, and constructs a mesh from
    /// `(rank, value)` pairs.
    fn build_indexed(
        region: Region,
        pairs: impl IntoIterator<Item = (usize, T)>,
    ) -> Result<Self, Self::Error>;
}

/// Mesh-aware collecting adapter.
///
/// Unlike `FromIterator`, this trait takes both an iterator *and* a
/// [`Region`] to build a mesh:
///
/// - Meshes always require a shape (`Region`) supplied externally.
/// - Cardinality must match: the iterator must yield exactly
///   `region.num_ranks()` items, or an error is returned.
/// - Construction goes through [`BuildFromRegion`], which validates
///   and builds the concrete mesh type.
///
/// In short: `collect_mesh` does for meshes what `collect` does for
/// ordinary collections, but with shape-awareness and validation.
pub trait CollectMeshExt<T>: Iterator<Item = T> + Sized {
    fn collect_mesh<M>(self, region: Region) -> Result<M, M::Error>
    where
        M: BuildFromRegion<T>;
}

/// Blanket impl: enables `.collect_mesh(region)` on any iterator of
/// `T`.
impl<I, T> CollectMeshExt<T> for I
where
    I: Iterator<Item = T> + Sized,
{
    fn collect_mesh<M>(self, region: Region) -> Result<M, M::Error>
    where
        M: BuildFromRegion<T>,
    {
        M::build_dense(region, self.collect())
    }
}

/// A canonical cardinality mismatch descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCardinality {
    pub expected: usize,
    pub actual: usize,
}

/// Exact-size, mesh-aware collecting adapter.
///
/// Like [`CollectMeshExt`], but for `ExactSizeIterator`. Performs a
/// `len()` pre-check to fail fast (no allocation) when `len() !=
/// region.num_ranks()`. On success, constructs `M` without
/// re-validating cardinality.
pub trait CollectExactMeshExt<T>: ExactSizeIterator<Item = T> + Sized {
    fn collect_exact_mesh<M>(self, region: Region) -> Result<M, M::Error>
    where
        M: BuildFromRegion<T>,
        M::Error: From<InvalidCardinality>;
}

/// Blanket impl: enables `.collect_exact_mesh(region)` on any
/// `ExactSizeIterator` of `T`.
impl<I, T> CollectExactMeshExt<T> for I
where
    I: ExactSizeIterator<Item = T> + Sized,
{
    fn collect_exact_mesh<M>(self, region: Region) -> Result<M, M::Error>
    where
        M: BuildFromRegion<T>,
        M::Error: From<InvalidCardinality>,
    {
        let expected = region.num_ranks();
        let actual = self.len();
        if actual != expected {
            return Err(M::Error::from(InvalidCardinality { expected, actual }));
        }
        Ok(M::build_dense_unchecked(region, self.collect()))
    }
}

/// Indexed collecting adapter.
///
/// Consume `(rank, value)` pairs plus a [`Region`] and build a mesh
/// via [`BuildFromRegionIndexed`].
///
/// # Semantics (recommended contract)
/// Implementations of [`BuildFromRegionIndexed`] are expected to
/// enforce:
/// - **Coverage:** every rank in `0..region.num_ranks()` is provided
///   at least once.
/// - **Bounds:** any out-of-bounds rank (`rank >= num_ranks`) is an
///   error.
/// - **Duplicates:** allowed; **last write wins**.
/// - **Missing ranks:** an error (incomplete coverage).
///
/// # Errors
/// Propagates whatever [`BuildFromRegionIndexed::build_indexed`]
/// returns (e.g., a cardinality/bounds error) from the target mesh
/// type.
///
/// See also: [`BuildFromRegionIndexed`] for the authoritative policy.
pub trait CollectIndexedMeshExt<T>: Iterator<Item = (usize, T)> + Sized {
    fn collect_indexed<M>(self, region: Region) -> Result<M, M::Error>
    where
        M: BuildFromRegionIndexed<T>;
}

/// Blanket impl: enables `.collect_indexed(region)` on any iterator
/// of `(usize, T)` pairs.
impl<I, T> CollectIndexedMeshExt<T> for I
where
    I: Iterator<Item = (usize, T)> + Sized,
{
    #[inline]
    fn collect_indexed<M>(self, region: Region) -> Result<M, M::Error>
    where
        M: BuildFromRegionIndexed<T>,
    {
        M::build_indexed(region, self)
    }
}

/// Map into any mesh `M`.
pub trait MapIntoExt: Ranked {
    fn map_into<M, U>(&self, f: impl Fn(&Self::Item) -> U) -> M
    where
        Self: Sized,
        M: BuildFromRegion<U>,
    {
        let region = self.region().clone();
        let n = region.num_ranks();
        let values: Vec<U> = (0..n).map(|i| f(self.get(i).unwrap())).collect();
        M::build_dense_unchecked(region, values)
    }

    fn try_map_into<M, U, E>(&self, f: impl Fn(&Self::Item) -> Result<U, E>) -> Result<M, E>
    where
        Self: Sized,
        M: BuildFromRegion<U>,
    {
        let region = self.region().clone();
        let n = region.num_ranks();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(f(self.get(i).unwrap())?);
        }
        Ok(M::build_dense_unchecked(region, out))
    }
}

/// Blanket impl: enables `.map_into(...)` and `.try_map_into`` on any
/// `Ranked`.
impl<T: Ranked> MapIntoExt for T {}

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
                base: Box::new(self.clone()),
                selected: Box::new(region),
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

/// Ranked is a helper trait to implement `View` on a ranked collection
/// of items.
pub trait Ranked: Sized {
    /// The type of item in this view.
    type Item: 'static;

    /// The ranks contained in this view.
    fn region(&self) -> &Region;

    /// Return the item at `rank`
    fn get(&self, rank: usize) -> Option<&Self::Item>;
}

/// Extension of [`Ranked`] for types that can materialize a new owned
/// view.
///
/// `sliced` constructs a new instance containing only the items in
/// the given `region`. Unlike [`Ranked::get`], which only provides
/// borrowed access, `sliced` requires that the implementor can
/// produce owned values for the specified sub-region.
pub trait RankedSliceable: Ranked {
    /// Construct a new Ranked containing the ranks in this view that
    /// are part of region. The caller guarantees that
    /// `region.is_subset(self.region())`.
    fn sliced(&self, region: Region) -> Self;
}

impl<T: RankedSliceable> View for T
where
    T::Item: Clone + 'static,
{
    type Item = T::Item;
    type View = Self;

    fn region(&self) -> Region {
        <Self as Ranked>::region(self).clone()
    }

    fn get(&self, rank: usize) -> Option<Self::Item> {
        <Self as Ranked>::get(self, rank).cloned()
    }

    fn subset(&self, region: Region) -> Result<Self, ViewError> {
        if !region.is_subset(self.region()) {
            return Err(ViewError::InvalidRange {
                base: Box::new(self.region().clone()),
                selected: Box::new(region.clone()),
            });
        }

        Ok(self.sliced(region))
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
            #[allow(unused_mut)]
            let mut labels = Vec::new();
            #[allow(unused_mut)]
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
    use super::labels::*;
    use super::*;
    use crate::Shape;
    use crate::shape;
    use crate::slice::CartesianIterator;

    #[test]
    fn test_is_safe_ident() {
        assert!(is_safe_ident("x"));
        assert!(is_safe_ident("gpu_0"));
        assert!(!is_safe_ident("dim/0"));
        assert!(!is_safe_ident("x y"));
        assert!(!is_safe_ident("x=y"));
    }
    #[test]
    fn test_fmt_label() {
        assert_eq!(fmt_label("x"), "x");
        assert_eq!(fmt_label("dim/0"), "\"dim/0\"");
    }

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

    #[test]
    fn points_iterates_ranks_in_row_major_order() {
        let ext = extent!(x = 2, y = 3, z = 4); // 24 ranks
        let mut it = ext.points();

        for expected_rank in 0..ext.num_ranks() {
            let p = it.next().expect("expected another Point");
            assert_eq!(
                p.rank, expected_rank,
                "ranks must be consecutive in row-major order"
            );
        }
        assert!(
            it.next().is_none(),
            "iterator must be exhausted after num_ranks items"
        );
    }

    #[test]
    fn points_iterates_single_point_for_0d_extent() {
        // 0-D extent has exactly one rank (the single point).
        let ext = extent!();
        let mut it = ext.points();

        let p = it
            .next()
            .expect("0-D extent should yield exactly one point");
        assert_eq!(p.rank, 0);
        assert_eq!(p.extent, ext);

        assert!(
            it.next().is_none(),
            "no more points after the single 0-D point"
        );
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
        assert_eq!(format!("{}", extent), "{x: 4, y: 5, z: 6}");

        let extent = Extent::new(vec!["dim/0".into(), "dim/1".into()], vec![4, 5]).unwrap();
        assert_eq!(format!("{}", extent), "{\"dim/0\": 4, \"dim/1\": 5}");

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
    fn test_extent_concat() {
        // Test basic concatenation of two extents with preserved order of labels
        let extent1 = extent!(x = 2, y = 3);
        let extent2 = extent!(z = 4, w = 5);

        let result = extent1.concat(&extent2).unwrap();
        assert_eq!(result.labels(), &["x", "y", "z", "w"]);
        assert_eq!(result.sizes(), &[2, 3, 4, 5]);
        assert_eq!(result.num_ranks(), 2 * 3 * 4 * 5);

        // Test concatenating with empty extent
        let empty = extent!();
        let result = extent1.concat(&empty).unwrap();
        assert_eq!(result.labels(), &["x", "y"]);
        assert_eq!(result.sizes(), &[2, 3]);

        let result = empty.concat(&extent1).unwrap();
        assert_eq!(result.labels(), &["x", "y"]);
        assert_eq!(result.sizes(), &[2, 3]);

        // Test concatenating two empty extents
        let result = empty.concat(&empty).unwrap();
        assert_eq!(result.labels(), &[] as &[String]);
        assert_eq!(result.sizes(), &[] as &[usize]);
        assert_eq!(result.num_ranks(), 1); // 0-dimensional extent has 1 rank

        // Test self-concatenation (overlapping labels should cause error)
        let result = extent1.concat(&extent1);
        assert!(
            result.is_err(),
            "Self-concatenation should error due to overlapping labels"
        );
        match result.unwrap_err() {
            ExtentError::OverlappingLabel { label } => {
                assert!(label == "x"); // Overlapping label should be "x"
            }
            other => panic!("Expected OverlappingLabel error, got {:?}", other),
        }

        // Test concatenation creates valid points
        let result = extent1.concat(&extent2).unwrap();
        let point = result.point(vec![1, 2, 3, 4]).unwrap();
        assert_eq!(point.coords(), vec![1, 2, 3, 4]);
        assert_eq!(point.extent(), &result);

        // Test error case: overlapping labels with same size (should error)
        let extent_a = extent!(x = 2, y = 3);
        let extent_b = extent!(y = 3, z = 4); // y overlaps with same size
        let result = extent_a.concat(&extent_b);
        assert!(
            result.is_err(),
            "Should error on overlapping labels even with same size"
        );
        match result.unwrap_err() {
            ExtentError::OverlappingLabel { label } => {
                assert_eq!(label, "y"); // the overlapping label
            }
            other => panic!("Expected OverlappingLabel error, got {:?}", other),
        }

        // Test that Extent::concat preserves order and is not commutative
        let extent_x = extent!(x = 2, y = 3);
        let extent_y = extent!(z = 4);
        assert_eq!(
            extent_x.concat(&extent_y).unwrap().labels(),
            &["x", "y", "z"]
        );
        assert_eq!(
            extent_y.concat(&extent_x).unwrap().labels(),
            &["z", "x", "y"]
        );

        // Test associativity: (a ⊕ b) ⊕ c == a ⊕ (b ⊕ c) for disjoint labels
        let extent_m = extent!(x = 2);
        let extent_n = extent!(y = 3);
        let extent_o = extent!(z = 4);

        let left_assoc = extent_m
            .concat(&extent_n)
            .unwrap()
            .concat(&extent_o)
            .unwrap();
        let right_assoc = extent_m
            .concat(&extent_n.concat(&extent_o).unwrap())
            .unwrap();

        assert_eq!(left_assoc, right_assoc);
        assert_eq!(left_assoc.labels(), &["x", "y", "z"]);
        assert_eq!(left_assoc.sizes(), &[2, 3, 4]);
        assert_eq!(left_assoc.num_ranks(), 2 * 3 * 4);
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
    fn test_point_display_with_quoted_labels() {
        // Labels include characters ("/", ",") that force quoting.
        let ext = Extent::new(vec!["dim/0".into(), "dim,1".into()], vec![3, 5]).unwrap();

        // Extent::Display should quote both labels.
        assert_eq!(format!("{}", ext), "{\"dim/0\": 3, \"dim,1\": 5}");

        // Point::Display should also quote labels consistently.
        let p = ext.point(vec![1, 2]).unwrap();
        assert_eq!(format!("{}", p), "\"dim/0\"=1/3,\"dim,1\"=2/5");
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

    #[test]
    fn test_base_local_rank_conversion() {
        fn point(rank: usize, region: &Region) -> Point {
            region.extent().point_of_rank(rank).unwrap()
        }

        let extent = extent!(replicas = 4, gpus = 2);
        let region = extent.range("replicas", 1..3).unwrap();
        // region is a 2x2 region of extent, with the ranks in the region and
        // and ranks in the extent are mapped as follows:
        //  0,        1
        // [2] -> 0, [3] -> 1
        // [4] -> 2, [5] -> 3
        //  6,        7
        // Use a point from an extent different from this region should fail:
        assert!(
            region
                .base_rank_of_point(extent.point_of_rank(0).unwrap())
                .is_err()
        );
        // Convert ranks in the extent to ranks in the region:
        assert_eq!(region.base_rank_of_point(point(0, &region)).unwrap(), 2);
        assert_eq!(region.base_rank_of_point(point(1, &region)).unwrap(), 3);
        assert_eq!(region.base_rank_of_point(point(2, &region)).unwrap(), 4);
        assert_eq!(region.base_rank_of_point(point(3, &region)).unwrap(), 5);
        // Convert ranks in the region to ranks in the extent:
        assert_eq!(region.point_of_base_rank(2).unwrap(), point(0, &region));
        assert_eq!(region.point_of_base_rank(3).unwrap(), point(1, &region));
        assert_eq!(region.point_of_base_rank(4).unwrap(), point(2, &region));
        assert_eq!(region.point_of_base_rank(5).unwrap(), point(3, &region));
        // Coverting ranks outside the region should fail:
        assert!(region.point_of_base_rank(1).is_err());
        assert!(region.point_of_base_rank(6).is_err());

        // Slice region to give a subregion:
        let subset = region
            .range("replicas", 1..2)
            .unwrap()
            .range("gpus", 1..2)
            .unwrap();
        // subset is a 1x1 region of extent, with ranksmapped as follows:
        // 0,  1
        // 2,  3
        // 4, [5] -> 0
        // 6,  7
        // Convert ranks in the extent to ranks in the subset:
        assert_eq!(subset.base_rank_of_point(point(0, &subset)).unwrap(), 5);
        assert_eq!(subset.point_of_base_rank(5).unwrap(), point(0, &subset));
        // or fail if the rank is not in the subset:
        assert!(subset.point_of_base_rank(4).is_err());
        assert!(subset.point_of_base_rank(6).is_err());
    }

    use proptest::prelude::*;

    use crate::strategy::gen_extent;
    use crate::strategy::gen_region;
    use crate::strategy::gen_region_strided;

    proptest! {
        #[test]
        fn test_region_parser(region in gen_region(1..=5, 1024)) {
            // Roundtrip display->parse correctly and preserves equality.
            assert_eq!(
                region,
                region.to_string().parse::<Region>().unwrap(),
                "failed to roundtrip region {}", region
            );
        }
    }

    // Property: `Region::Display` and `FromStr` remain a lossless
    // round-trip even when the slice has a nonzero offset.
    //
    // - Construct a region, then force its slice to have `offset =
    //   8`.
    // - Convert that region to a string via `Display`.
    // - Parse it back via `FromStr`.
    //
    // The parsed region must equal the original, showing that
    // offsets are encoded and decoded consistently in the textual
    // format.
    proptest! {
        #[test]
        fn region_parser_with_offset_roundtrips(region in gen_region(1..=4, 8)) {
            let (labels, slice) = region.clone().into_inner();
            let region_off = Region {
                labels,
                slice: Slice::new(8, slice.sizes().to_vec(), slice.strides().to_vec()).unwrap(),
            };
            let s = region_off.to_string();
            let parsed: Region = s.parse().unwrap();
            prop_assert_eq!(parsed, region_off);
        }
    }

    // Property: For any randomly generated strided `Region`,
    // converting it to a string with `Display` and parsing it back
    // with `FromStr` yields the same region.
    //
    // This ensures that:
    // - Strided layouts (with arbitrary steps and begins) are
    //   faithfully represented by the textual format.
    // - Offsets, sizes, and strides survive the round-trip without
    //   loss.
    // - Quoting rules for labels remain consistent.
    proptest! {
        #[test]
        fn region_strided_display_parse_roundtrips(
            region in gen_region_strided(1..=4, 6, 3, 16)
        ) {
            // Example: 122+"d/0"=1/30,"d/1"=5/6,"d/2"=4/1
            //
            // Decoding:
            // - `122+` is the slice offset.
            // - Each "label=size/stride" shows the post-slice size
            //   and stride.
            //   Sizes are reduced by `ceil((base_size - begin) /
            //   step)`.
            //   Strides are the base row-major strides, each
            //   multiplied by its step.
            let s = region.to_string();
            let parsed: Region = s.parse().unwrap();
            prop_assert_eq!(parsed, region);
        }
    }

    // Property: `Region::Display` faithfully reflects its underlying
    // `Slice`.
    //
    // - The offset printed as `offset+` must equal
    //   `region.slice().offset()`.
    // - Each `label=size/stride` entry must show the size and stride
    //   from the underlying slice.
    //
    // This ensures that the textual representation is consistent
    // with the region’s internal geometry.
    proptest! {
        #[test]
        fn region_strided_display_matches_slice(
            region in gen_region_strided(1..=4, 6, 3, 16)
        ) {
            let s = region.to_string();
            let slice = region.slice();

            // Check offset if present
            if slice.offset() != 0 {
                let prefix: Vec<_> = s.split('+').collect();
                prop_assert!(prefix.len() > 1, "expected offset+ form in {}", s);
                let offset_str = prefix[0];
                let offset_val: usize = offset_str.parse().unwrap();
                prop_assert_eq!(offset_val, slice.offset(), "offset mismatch in {}", s);
            } else {
                prop_assert!(!s.contains('+'), "unexpected +offset in {}", s);
            }

            // Collect all size/stride pairs from the string
            let body = s.split('+').next_back().unwrap(); // after offset if any
            let parts: Vec<_> = body.split(',').collect();
            prop_assert_eq!(parts.len(), slice.sizes().len());

            for (i, part) in parts.iter().enumerate() {
                // part looks like label=size/stride
                let rhs = part.split('=').nth(1).unwrap();
                let mut nums = rhs.split('/');
                let size_val: usize = nums.next().unwrap().parse().unwrap();
                let stride_val: usize = nums.next().unwrap().parse().unwrap();

                prop_assert_eq!(size_val, slice.sizes()[i], "size mismatch at dim {} in {}", i, s);
                prop_assert_eq!(stride_val, slice.strides()[i], "stride mismatch at dim {} in {}", i, s);
            }
        }
    }

    #[test]
    fn test_point_from_str_round_trip() {
        let points = vec![
            extent!(x = 4, y = 5, z = 6).point(vec![1, 2, 3]).unwrap(),
            extent!(host = 2, gpu = 8).point(vec![0, 7]).unwrap(),
            extent!().point(vec![]).unwrap(),
            extent!(x = 10).point(vec![5]).unwrap(),
        ];

        for point in points {
            assert_eq!(point, point.to_string().parse().unwrap());
        }
    }

    #[test]
    fn test_point_from_str_basic() {
        let cases = vec![
            ("x=1/4,y=2/5", extent!(x = 4, y = 5), vec![1, 2]),
            ("host=0/2,gpu=7/8", extent!(host = 2, gpu = 8), vec![0, 7]),
            ("z=3/6", extent!(z = 6), vec![3]),
            ("", extent!(), vec![]), // empty point
            // Test with spaces
            (" x = 1 / 4 , y = 2 / 5 ", extent!(x = 4, y = 5), vec![1, 2]),
        ];

        for (input, expected_extent, expected_coords) in cases {
            let parsed: Point = input.parse().unwrap();
            let expected = expected_extent.point(expected_coords).unwrap();
            assert_eq!(parsed, expected, "failed to parse: {}", input);
        }
    }

    #[test]
    fn test_point_from_str_quoted() {
        // Test parsing points with quoted labels
        let extent = Extent::new(vec!["dim/0".into(), "dim,1".into()], vec![3, 5]).unwrap();
        let point = extent.point(vec![1, 2]).unwrap();

        let display_str = point.to_string();
        assert_eq!(display_str, "\"dim/0\"=1/3,\"dim,1\"=2/5");

        let parsed: Point = display_str.parse().unwrap();
        assert_eq!(parsed, point);

        let parsed: Point = "\"dim/0\"=1/3,\"dim,1\"=2/5".parse().unwrap();
        assert_eq!(parsed, point);
    }

    #[test]
    fn test_point_from_str_error_cases() {
        // Test various error cases
        let error_cases = vec![
            "x=1,y=2/5",     // missing size for x
            "x=1/4,y=2",     // missing size for y
            "x=1/4,y=/5",    // missing coord for y
            "x=/4,y=2/5",    // missing coord for x
            "x=1/4,y=2/",    // missing size after /
            "x=1/,y=2/5",    // missing size after /
            "x=1/4=5,y=2/5", // extra equals
            "x=1/4/6,y=2/5", // extra slash
            "x=abc/4,y=2/5", // invalid coord
            "x=1/abc,y=2/5", // invalid size
            "=1/4,y=2/5",    // missing label
            "x=1/4,",        // trailing comma with empty part
            "x=1/4,=2/5",    // missing label after comma
            "x=1/4,y",       // incomplete dimension
            "x",             // just a label
            "x=",            // label with equals but no coord/size
            "x=1/4,y=10/5",  // coord out of bounds (y has size 5, max coord is 4)
        ];

        for input in error_cases {
            let result: Result<Point, PointError> = input.parse();
            assert!(result.is_err(), "Expected error for input: '{}'", input);
        }
    }

    #[test]
    fn test_point_from_str_coordinate_validation() {
        // Test that coordinates are validated against sizes
        let input = "x=5/4,y=2/5"; // x coord is 5 but size is 4 (max valid coord is 3)
        let result: Result<Point, PointError> = input.parse();
        assert!(
            result.is_err(),
            "Expected error for out-of-bounds coordinate"
        );

        match result.unwrap_err() {
            PointError::OutOfRangeIndex { size, index } => {
                assert_eq!(size, 4);
                assert_eq!(index, 5);
            }
            _ => panic!("Expected OutOfRangeIndex error"),
        }
    }

    #[test]
    fn test_point_from_str_consistency_validation() {
        // Test that all dimension must be consistent (same labels and sizes for given extent)
        // This is implicitly tested by the round-trip tests, but let's be explicit

        // Valid consistent point
        let input = "x=1/4,y=2/5,z=3/6";
        let parsed: Point = input.parse().unwrap();

        assert_eq!(parsed.extent().labels(), &["x", "y", "z"]);
        assert_eq!(parsed.extent().sizes(), &[4, 5, 6]);
        assert_eq!(parsed.coords(), vec![1, 2, 3]);
    }

    proptest! {
        /// `Point.coord(i)` and `(&Point).into_iter()` must agree with
        /// `coords()`.
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

        // Property test: Point display/parse round-trip should always work
        #[test]
        fn point_display_parse_round_trip(extent in gen_extent(0..=4, 8)) {
            for point in extent.points() {
                let display = point.to_string();
                let parsed: Point = display.parse().unwrap();
                prop_assert_eq!(parsed, point, "round-trip failed for point: {}", display);
            }
        }
    }

    proptest! {
        /// New rank-walk iterator must match legacy CartesianIterator
        /// + rank_of_coords.
        #[test]
        fn points_iterator_equivalent_to_legacy_cartesian(extent in gen_extent(0..=4, 8)) {
            let sizes = extent.sizes().to_vec();

            // Compare lengths without cloning the iterator.
            let legacy_len = CartesianIterator::new(sizes.clone()).count();
            prop_assert_eq!(legacy_len, extent.num_ranks());

            // Compare element-by-element: (Point from new iter) vs
            // (coords from legacy)
            let legacy_coords = CartesianIterator::new(sizes);
            for (step, (p, coords)) in extent.points().zip(legacy_coords).enumerate() {
                let old_rank = extent.rank_of_coords(&coords).expect("valid legacy coords");
                prop_assert_eq!(p.rank(), old_rank, "rank mismatch at step {} for coords {:?}", step, coords);
                prop_assert_eq!(p.coords(), coords, "coords mismatch at step {}", step);
            }
        }
    }
}
