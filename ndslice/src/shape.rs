/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::DimSliceIterator;
use crate::Region;
use crate::Slice;
use crate::SliceError;
use crate::selection::Selection;
use crate::view::Extent;

// We always retain dimensions here even if they are selected out.

#[derive(Debug, thiserror::Error)]
pub enum ShapeError {
    #[error("label slice dimension mismatch: {labels_dim} != {slice_dim}")]
    DimSliceMismatch { labels_dim: usize, slice_dim: usize },

    #[error("invalid labels `{labels:?}`")]
    InvalidLabels { labels: Vec<String> },

    #[error("empty range {range}")]
    EmptyRange { range: Range },

    #[error("out of range {range} for dimension {dim} of size {size}")]
    OutOfRange {
        range: Range,
        dim: String,
        size: usize,
    },

    #[error("selection `{expr}` exceeds dimensionality {num_dim}")]
    SelectionTooDeep { expr: Selection, num_dim: usize },

    #[error("dynamic selection `{expr}`")]
    SelectionDynamic { expr: Selection },

    #[error("{index} out of range for dimension {dim} of size {size}")]
    IndexOutOfRange {
        index: usize,
        dim: String,
        size: usize,
    },

    #[error(transparent)]
    SliceError(#[from] SliceError),
}

/// A shape is a [`Slice`] with labeled dimensions and a selection API.
#[derive(Clone, Deserialize, Serialize, PartialEq, Hash, Debug)]
pub struct Shape {
    /// The labels for each dimension in slice.
    labels: Vec<String>,
    /// The slice itself, which describes the topology of the shape.
    slice: Slice,
}

impl Shape {
    /// Creates a new shape with the provided labels, which describe the
    /// provided Slice.
    ///
    /// Shapes can also be constructed by way of the [`shape`] macro, which
    /// creates a by-construction correct slice in row-major order given a set of
    /// sized dimensions.
    pub fn new(labels: Vec<String>, slice: Slice) -> Result<Self, ShapeError> {
        if labels.len() != slice.num_dim() {
            return Err(ShapeError::DimSliceMismatch {
                labels_dim: labels.len(),
                slice_dim: slice.num_dim(),
            });
        }
        Ok(Self { labels, slice })
    }

    /// Select a single index along a named dimension, removing that
    /// dimension entirely. This reduces the dimensionality by 1. In
    /// effect it results in a cross section of the shape at the given
    /// index in the given dimension.
    pub fn at(&self, label: &str, index: usize) -> Result<Self, ShapeError> {
        let dim = self.dim(label)?;
        let slice = self.slice.at(dim, index).map_err(|err| match err {
            SliceError::IndexOutOfRange { index, total } => ShapeError::OutOfRange {
                range: Range(index, Some(index + 1), 1),
                dim: label.to_string(),
                size: total,
            },
            other => other.into(),
        })?;
        let mut labels = self.labels.clone();
        labels.remove(dim);
        Ok(Self { labels, slice })
    }

    /// Restrict this shape along a named dimension using a [`Range`].
    /// The provided range must be nonempty.
    ///
    /// `select` is composable, it can be applied repeatedly, even on
    /// the same dimension, to refine the view incrementally.
    pub fn select<R: Into<Range>>(&self, label: &str, range: R) -> Result<Self, ShapeError> {
        let dim = self.dim(label)?;
        let range = range.into();
        let (begin, end, step) = range.resolve(self.slice().sizes()[dim]);
        let slice = self
            .slice
            .select(dim, begin, end, step)
            .map_err(|err| match err {
                SliceError::EmptyRange { .. } => ShapeError::EmptyRange { range },
                SliceError::IndexOutOfRange { total, .. } => ShapeError::OutOfRange {
                    range,
                    dim: label.to_string(),
                    size: total,
                },
                other => other.into(),
            })?;
        let labels = self.labels.clone();
        Ok(Self { labels, slice })
    }

    /// Produces an iterator over subshapes by fixing the first `dims`
    /// dimensions.
    ///
    /// For a shape of rank `n`, this yields `∏ sizes[0..dims]`
    /// subshapes, each with the first `dims` dimensions restricted to
    /// size 1. The remaining dimensions are left unconstrained.
    ///
    /// This is useful for structured traversal of slices within a
    /// multidimensional shape. See [`SelectIterator`] for details and
    /// examples.
    ///
    /// # Errors
    /// Returns an error if `dims == 0` or `dims >= self.rank()`.
    pub fn select_iter(&self, dims: usize) -> Result<SelectIterator<'_>, ShapeError> {
        let num_dims = self.slice().num_dim();
        if dims == 0 || dims >= num_dims {
            return Err(ShapeError::SliceError(SliceError::IndexOutOfRange {
                index: dims,
                total: num_dims,
            }));
        }

        Ok(SelectIterator {
            shape: self,
            iter: self.slice().dim_iter(dims),
        })
    }

    /// Sub-set this shape by select a particular row of the given
    /// indices The resulting shape will no longer have dimensions for
    /// the given indices Example shape.index(vec![("gpu", 3),
    /// ("host", 0)])
    pub fn index(&self, indices: Vec<(String, usize)>) -> Result<Shape, ShapeError> {
        let mut shape = self.clone();
        for (label, index) in indices {
            shape = shape.at(&label, index)?;
        }
        Ok(shape)
    }

    /// The per-dimension labels of this shape.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// The slice describing the shape.
    pub fn slice(&self) -> &Slice {
        &self.slice
    }

    /// Return a set of labeled coordinates for the given rank.
    pub fn coordinates(&self, rank: usize) -> Result<Vec<(String, usize)>, ShapeError> {
        let coords = self.slice.coordinates(rank)?;
        Ok(coords
            .iter()
            .zip(self.labels.iter())
            .map(|(i, l)| (l.to_string(), *i))
            .collect())
    }

    pub fn dim(&self, label: &str) -> Result<usize, ShapeError> {
        self.labels
            .iter()
            .position(|l| l == label)
            .ok_or_else(|| ShapeError::InvalidLabels {
                labels: vec![label.to_string()],
            })
    }

    /// Return the 0-dimensional single element shape
    pub fn unity() -> Shape {
        Shape::new(vec![], Slice::new(0, vec![], vec![]).expect("unity")).expect("unity")
    }

    /// The extent corresponding to this shape.
    pub fn extent(&self) -> Extent {
        Extent::new(self.labels.clone(), self.slice.sizes().to_vec()).unwrap()
    }

    /// The region corresponding to this shape.
    pub fn region(&self) -> Region {
        self.into()
    }
}

impl From<&Region> for Shape {
    fn from(region: &Region) -> Self {
        Shape::new(region.labels().to_vec(), region.slice().clone())
            .expect("Shape::new should not fail because a Region by definition is a valid Shape")
    }
}

/// Iterator over subshapes obtained by fixing a prefix of dimensions.
///
/// This iterator is produced by [`Shape::select_iter(dims)`], and
/// yields one `Shape` per coordinate prefix in the first `dims`
/// dimensions.
///
/// For a shape of `n` dimensions, each yielded shape has:
/// - The first `dims` dimensions restricted to size 1 (i.e., fixed
///   via `select`)
/// - The remaining `n - dims` dimensions left unconstrained
///
/// This allows structured iteration over "slices" of the original
/// shape: for example with `n` = 3, `select_iter(1)` walks through 2D
/// planes, while `select_iter(2)` yields 1D subshapes.
///
/// # Example
/// ```ignore
/// let s = shape!(zone = 2, host = 2, gpu = 8);
/// let views: Vec<_> = s.select_iter(2).unwrap().collect();
/// assert_eq!(views.len(), 4);
/// assert_eq!(views[0].slice().sizes(), &[1, 1, 8]);
/// ```
/// The above example can be interpreted as: for each `(zone, host)`
/// pair, `select_iter(2)` yields a `Shape` describing the associated
/// row of GPUs — a view into the `[1, 1, 8]` subregion of the full
/// `[2, 2, 8]` shape.
pub struct SelectIterator<'a> {
    shape: &'a Shape,
    iter: DimSliceIterator,
}

impl<'a> Iterator for SelectIterator<'a> {
    type Item = Shape;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.iter.next()?;
        let mut shape = self.shape.clone();
        for (dim, index) in pos.iter().enumerate() {
            shape = shape.select(&self.shape.labels()[dim], *index).unwrap();
        }
        Some(shape)
    }
}

impl fmt::Display for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Just display the sizes of each dimension, for now.
        // Once we have a selection algebra, we can provide a
        // better Display implementation.
        write!(f, "{{")?;
        for dim in 0..self.labels.len() {
            write!(f, "{}={}", self.labels[dim], self.slice.sizes()[dim])?;
            if dim < self.labels.len() - 1 {
                write!(f, ",")?;
            }
        }
        write!(f, "}}")
    }
}

/// Construct a new shape with the given set of dimension-size pairs in row-major
/// order.
///
/// ```
/// let s = ndslice::shape!(host = 2, gpu = 8);
/// assert_eq!(s.labels(), &["host".to_string(), "gpu".to_string()]);
/// assert_eq!(s.slice().sizes(), &[2, 8]);
/// assert_eq!(s.slice().strides(), &[8, 1]);
/// ```
#[macro_export]
macro_rules! shape {
    ( $( $label:ident = $size:expr ),* $(,)? ) => {
        {
            let mut labels = Vec::new();
            let mut sizes = Vec::new();

            $(
                labels.push(stringify!($label).to_string());
                sizes.push($size);
            )*

            $crate::shape::Shape::new(labels, $crate::Slice::new_row_major(sizes)).unwrap()
        }
    };
}

/// Perform a sub-selection on the provided [`Shape`] object.
///
/// This macro chains `.select()` calls to apply multiple labeled
/// dimension restrictions in a fluent way.
///
/// ```
/// let s = ndslice::shape!(host = 2, gpu = 8);
/// let s = ndslice::select!(s, host = 1, gpu = 4..).unwrap();
/// assert_eq!(s.labels(), &["host".to_string(), "gpu".to_string()]);
/// assert_eq!(s.slice().sizes(), &[1, 4]);
/// ```
#[macro_export]
macro_rules! select {
    ($shape:ident, $label:ident = $range:expr) => {
        $shape.select(stringify!($label), $range)
    };

    ($shape:ident, $label:ident = $range:expr, $($labels:ident = $ranges:expr),+) => {
        $shape.select(stringify!($label), $range).and_then(|shape| $crate::select!(shape, $($labels = $ranges),+))
    };
}

/// A range of indices, with a stride. Ranges are convertible from
/// native Rust ranges.
///
/// Deriving `Eq`, `Ord` and `Hash` is sound because all fields are
/// `Ord` and comparison is purely structural over `(start, end,
/// step)`.
#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord
)]
pub struct Range(pub usize, pub Option<usize>, pub usize);

impl Range {
    pub(crate) fn resolve(&self, size: usize) -> (usize, usize, usize) {
        match self {
            Range(begin, Some(end), stride) => (*begin, std::cmp::min(size, *end), *stride),
            Range(begin, None, stride) => (*begin, size, *stride),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Range(begin, Some(end), _) if end <= begin)
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Range(begin, None, stride) => write!(f, "{}::{}", begin, stride),
            Range(begin, Some(end), stride) => write!(f, "{}:{}:{}", begin, end, stride),
        }
    }
}

impl From<std::ops::Range<usize>> for Range {
    fn from(r: std::ops::Range<usize>) -> Self {
        Self(r.start, Some(r.end), 1)
    }
}

impl From<std::ops::RangeInclusive<usize>> for Range {
    fn from(r: std::ops::RangeInclusive<usize>) -> Self {
        Self(*r.start(), Some(*r.end() + 1), 1)
    }
}

impl From<std::ops::RangeFrom<usize>> for Range {
    fn from(r: std::ops::RangeFrom<usize>) -> Self {
        Self(r.start, None, 1)
    }
}

impl From<usize> for Range {
    fn from(idx: usize) -> Self {
        Self(idx, Some(idx + 1), 1)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_basic() {
        let s = shape!(host = 2, gpu = 8);
        assert_eq!(&s.labels, &["host".to_string(), "gpu".to_string()]);
        assert_eq!(s.slice.offset(), 0);
        assert_eq!(s.slice.sizes(), &[2, 8]);
        assert_eq!(s.slice.strides(), &[8, 1]);

        assert_eq!(s.to_string(), "{host=2,gpu=8}");
    }

    #[test]
    fn test_select() {
        let s = shape!(host = 2, gpu = 8);

        assert_eq!(
            s.slice().iter().collect::<Vec<_>>(),
            &[
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                8 + 1,
                8 + 2,
                8 + 3,
                8 + 4,
                8 + 5,
                8 + 6,
                8 + 7
            ]
        );

        assert_eq!(
            select!(s, host = 1)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            &[8, 8 + 1, 8 + 2, 8 + 3, 8 + 4, 8 + 5, 8 + 6, 8 + 7]
        );

        assert_eq!(
            select!(s, gpu = 2..)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            &[2, 3, 4, 5, 6, 7, 8 + 2, 8 + 3, 8 + 4, 8 + 5, 8 + 6, 8 + 7]
        );

        assert_eq!(
            select!(s, gpu = 3..5)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            &[3, 4, 8 + 3, 8 + 4]
        );

        assert_eq!(
            select!(s, gpu = 3..5, host = 1)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            &[8 + 3, 8 + 4]
        );
    }

    #[test]
    fn test_select_iter() {
        let s = shape!(replica = 2, host = 2, gpu = 8);
        let selections: Vec<_> = s.select_iter(2).unwrap().collect();
        assert_eq!(selections[0].slice().sizes(), &[1, 1, 8]);
        assert_eq!(selections[1].slice().sizes(), &[1, 1, 8]);
        assert_eq!(selections[2].slice().sizes(), &[1, 1, 8]);
        assert_eq!(selections[3].slice().sizes(), &[1, 1, 8]);
        assert_eq!(
            selections,
            &[
                select!(s, replica = 0, host = 0).unwrap(),
                select!(s, replica = 0, host = 1).unwrap(),
                select!(s, replica = 1, host = 0).unwrap(),
                select!(s, replica = 1, host = 1).unwrap()
            ]
        );
    }

    #[test]
    fn test_coordinates() {
        let s = shape!(host = 2, gpu = 8);
        assert_eq!(
            s.coordinates(0).unwrap(),
            vec![("host".to_string(), 0), ("gpu".to_string(), 0)]
        );
        assert_eq!(
            s.coordinates(1).unwrap(),
            vec![("host".to_string(), 0), ("gpu".to_string(), 1)]
        );
        assert_eq!(
            s.coordinates(8).unwrap(),
            vec![("host".to_string(), 1), ("gpu".to_string(), 0)]
        );
        assert_eq!(
            s.coordinates(9).unwrap(),
            vec![("host".to_string(), 1), ("gpu".to_string(), 1)]
        );

        assert_matches!(
            s.coordinates(16).unwrap_err(),
            ShapeError::SliceError(SliceError::ValueNotInSlice { value: 16 })
        );
    }

    #[test]
    fn test_select_bad() {
        let s = shape!(host = 2, gpu = 8);

        assert_matches!(
            select!(s, gpu = 1..1).unwrap_err(),
            ShapeError::EmptyRange {
                range: Range(1, Some(1), 1)
            },
        );

        assert_matches!(
            select!(s, gpu = 8).unwrap_err(),
            ShapeError::OutOfRange {
                range: Range(8, Some(9), 1),
                dim,
                size: 8,
            } if dim == "gpu",
        );
    }

    #[test]
    fn test_shape_index() {
        let n_hosts = 5;
        let n_gpus = 7;

        // Index first dim
        let s = shape!(host = n_hosts, gpu = n_gpus);
        assert_eq!(
            s.index(vec![("host".to_string(), 0)]).unwrap(),
            Shape::new(
                vec!["gpu".to_string()],
                Slice::new(0, vec![n_gpus], vec![1]).unwrap()
            )
            .unwrap()
        );

        // Index last dims
        let offset = 1;
        assert_eq!(
            s.index(vec![("gpu".to_string(), offset)]).unwrap(),
            Shape::new(
                vec!["host".to_string()],
                Slice::new(offset, vec![n_hosts], vec![n_gpus]).unwrap()
            )
            .unwrap()
        );

        // Index middle dim
        let n_zone = 2;
        let s = shape!(zone = n_zone, host = n_hosts, gpu = n_gpus);
        let offset = 3;
        assert_eq!(
            s.index(vec![("host".to_string(), offset)]).unwrap(),
            Shape::new(
                vec!["zone".to_string(), "gpu".to_string()],
                Slice::new(
                    offset * n_gpus,
                    vec![n_zone, n_gpus],
                    vec![n_hosts * n_gpus, 1]
                )
                .unwrap()
            )
            .unwrap()
        );

        // Out of range
        assert!(
            shape!(gpu = n_gpus)
                .index(vec![("gpu".to_string(), n_gpus)])
                .is_err()
        );
        // Invalid dim
        assert!(
            shape!(gpu = n_gpus)
                .index(vec![("non-exist-dim".to_string(), 0)])
                .is_err()
        );
    }

    #[test]
    fn test_shape_select_stride_rounding() {
        let shape = shape!(x = 10);
        // Select x = 0..10 step 3 → expect indices [0, 3, 6, 9]
        let sub = shape.select("x", Range(0, Some(10), 3)).unwrap();
        let slice = sub.slice();
        // 10 / 3 = 3.33..., so ceil(10 / 3) = 4
        assert_eq!(
            slice,
            &Slice::new(0, vec![4], vec![3]).unwrap(),
            "Expected offset 0, size 4, stride 3"
        );
    }

    #[test]
    fn test_shape_at_removes_dimension() {
        let labels = vec![
            "batch".to_string(),
            "height".to_string(),
            "width".to_string(),
        ];
        let slice = Slice::new_row_major(vec![2, 3, 4]);
        let shape = Shape::new(labels, slice).unwrap();

        // Select index 1 from "batch" dimension
        let result = shape.at("batch", 1).unwrap();

        // Should have 2 dimensions now
        assert_eq!(result.labels(), &["height", "width"]);
        assert_eq!(result.slice().sizes(), &[3, 4]);
        assert_eq!(result.slice().offset(), 12); // 1 * 12 (batch stride)
    }

    #[test]
    fn test_shape_at_middle_dimension() {
        let labels = vec![
            "batch".to_string(),
            "height".to_string(),
            "width".to_string(),
        ];
        let slice = Slice::new_row_major(vec![2, 3, 4]);
        let shape = Shape::new(labels, slice).unwrap();

        // Select index 1 from "height" dimension (middle)
        let result = shape.at("height", 1).unwrap();

        // Should remove middle label
        assert_eq!(result.labels(), &["batch", "width"]);
        assert_eq!(result.slice().sizes(), &[2, 4]);
        assert_eq!(result.slice().offset(), 4); // 1 * 4 (height stride)
    }

    #[test]
    fn test_shape_at_invalid_label() {
        let labels = vec!["batch".to_string(), "height".to_string()];
        let slice = Slice::new_row_major(vec![2, 3]);
        let shape = Shape::new(labels, slice).unwrap();

        let result = shape.at("nonexistent", 0);
        assert!(matches!(result, Err(ShapeError::InvalidLabels { .. })));
    }

    #[test]
    fn test_shape_at_index_out_of_range() {
        let labels = vec!["batch".to_string(), "height".to_string()];
        let slice = Slice::new_row_major(vec![2, 3]);
        let shape = Shape::new(labels, slice).unwrap();

        let result = shape.at("batch", 5); // batch only has size 2
        assert!(matches!(result, Err(ShapeError::OutOfRange { .. })));
    }
}
