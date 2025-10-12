/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::iter::zip;

use serde::Deserialize;
use serde::Serialize;

/// The type of error for slice operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SliceError {
    #[error("invalid dims: expected {expected}, got {got}")]
    InvalidDims { expected: usize, got: usize },

    #[error("nonrectangular shape")]
    NonrectangularShape,

    #[error("nonunique strides")]
    NonuniqueStrides,

    #[error("stride {stride} must be larger than size of previous space {space}")]
    StrideTooSmall { stride: usize, space: usize },

    #[error("index {index} out of range {total}")]
    IndexOutOfRange { index: usize, total: usize },

    #[error("value {value} not in slice")]
    ValueNotInSlice { value: usize },

    #[error("incompatible view: {reason}")]
    IncompatibleView { reason: String },

    #[error("noncontiguous shape")]
    NonContiguous,

    #[error("empty range: {begin}..{end} (step {step})")]
    EmptyRange {
        begin: usize,
        end: usize,
        step: usize,
    },

    #[error("dimension {dim} out of range for {ndims}-dimensional slice")]
    DimensionOutOfRange { dim: usize, ndims: usize },
}

/// Slice is a compact representation of indices into the flat
/// representation of an n-dimensional array. Given an offset, sizes of
/// each dimension, and strides for each dimension, Slice can compute
/// indices into the flat array.
///
/// For example, the following describes a dense 4x4x4 array in row-major
/// order:
/// ```
/// # use ndslice::Slice;
/// let s = Slice::new(0, vec![4, 4, 4], vec![16, 4, 1]).unwrap();
/// assert!(s.iter().eq(0..(4 * 4 * 4)));
/// ```
///
/// Slices allow easy slicing by subsetting and striding. For example,
/// we can fix the index of the second dimension by dropping it and
/// adding that index (multiplied by the previous size) to the offset.
///
/// ```
/// # use ndslice::Slice;
/// let s = Slice::new(0, vec![2, 4, 2], vec![8, 2, 1]).unwrap();
/// let selected_index = 3;
/// let sub = Slice::new(2 * selected_index, vec![2, 2], vec![8, 1]).unwrap();
/// let coords = [[0, 0], [0, 1], [1, 0], [1, 1]];
/// for coord @ [x, y] in coords {
///     assert_eq!(
///         sub.location(&coord).unwrap(),
///         s.location(&[x, 3, y]).unwrap()
///     );
/// }
/// ```
// TODO: Consider representing this by arrays parameterized by the slice
// dimensionality.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Slice {
    offset: usize,
    sizes: Vec<usize>,
    strides: Vec<usize>,
}

impl Slice {
    /// Create a new Slice with the provided offset, sizes, and
    /// strides. New performs validation to ensure that sizes and strides
    /// are compatible:
    ///   - They have to be the same length (i.e., same number of dimensions)
    ///   - They have to be rectangular (i.e., stride n+1 has to evenly divide into stride n)
    ///   - Strides must be nonoverlapping (each stride has to be larger than the previous space)
    pub fn new(offset: usize, sizes: Vec<usize>, strides: Vec<usize>) -> Result<Self, SliceError> {
        if sizes.len() != strides.len() {
            return Err(SliceError::InvalidDims {
                expected: sizes.len(),
                got: strides.len(),
            });
        }
        let mut combined: Vec<(usize, usize)> =
            strides.iter().cloned().zip(sizes.iter().cloned()).collect();
        combined.sort();

        let mut prev_stride: Option<usize> = None;
        let mut prev_size: Option<usize> = None;
        let mut total: usize = 1;
        for (stride, size) in combined {
            if let Some(prev_stride) = prev_stride {
                if stride % prev_stride != 0 {
                    return Err(SliceError::NonrectangularShape);
                }
                // Strides for single element dimensions can repeat, because they are unused
                if stride == prev_stride && size != 1 && prev_size.unwrap_or(1) != 1 {
                    return Err(SliceError::NonuniqueStrides);
                }
            }
            if total > stride {
                return Err(SliceError::StrideTooSmall {
                    stride,
                    space: total,
                });
            }
            total = stride * size;
            prev_stride = Some(stride);
            prev_size = Some(size);
        }

        Ok(Slice {
            offset,
            sizes,
            strides,
        })
    }

    pub(crate) fn new_singleton(rank: usize) -> Self {
        Self {
            offset: rank,
            sizes: vec![1],
            strides: vec![1],
        }
    }

    /// Deconstruct the slice into its offset, sizes, and strides.
    pub fn into_inner(self) -> (usize, Vec<usize>, Vec<usize>) {
        let Slice {
            offset,
            sizes,
            strides,
        } = self;
        (offset, sizes, strides)
    }

    /// Create a new slice of the given sizes in row-major order.
    pub fn new_row_major(sizes: impl Into<Vec<usize>>) -> Self {
        let sizes = sizes.into();
        // "flip it and reverse it" --Missy Elliott
        let mut strides: Vec<usize> = sizes.clone();
        let _ = strides.iter_mut().rev().fold(1, |acc, n| {
            let next = *n * acc;
            *n = acc;
            next
        });
        Self {
            offset: 0,
            sizes,
            strides,
        }
    }

    /// Create one celled slice.
    pub fn new_single_multi_dim_cell(dims: usize) -> Self {
        Self {
            offset: 0,
            sizes: vec![1; dims],
            strides: vec![1; dims],
        }
    }

    /// The number of dimensions in this slice.
    pub fn num_dim(&self) -> usize {
        self.sizes.len()
    }

    /// This is the offset from which the first value in the Slice begins.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// The shape of the slice; that is, the size of each dimension.
    pub fn sizes(&self) -> &[usize] {
        &self.sizes
    }

    /// The strides of the slice; that is, the distance between each
    /// element at a given index in the underlying array.
    pub fn strides(&self) -> &[usize] {
        &self.strides
    }

    pub fn is_contiguous(&self) -> bool {
        let mut expected_stride = 1;
        for (stride, size) in zip(self.strides.iter(), self.sizes.iter()).rev() {
            if *stride != expected_stride {
                return false;
            }
            expected_stride *= *size
        }
        true
    }

    /// Select a single index along a dimension, removing that
    /// dimension entirely.
    ///
    /// This reduces the dimensionality by 1 by "fixing" one
    /// coordinate to a specific value. Think of it like taking a
    /// cross-section: selecting index 2 from the first dimension of a
    /// 3D array gives you a 2D slice, like cutting a plane from a 3D
    /// space at a fixed position.
    ///
    /// This reduces the dimensionality by 1 by "fixing" one
    /// coordinate to a specific value. The fixed coordinate's
    /// contribution (index × stride) gets absorbed into the base
    /// offset, while the remaining dimensions keep their original
    /// strides unchanged - they still describe the same memory
    /// distances between elements.
    ///
    /// # Example intuition
    /// - 3D array → select `at(dim=0, index=2)` → 2D slice (like a
    ///   plane)
    /// - 2D matrix → select `at(dim=1, index=3)` → 1D vector (like a
    ///   column)
    /// - 1D vector → select `at(dim=0, index=5)` → 0D scalar (single
    ///   element)
    ///
    /// # Arguments
    /// * `dim` - The dimension index to select from
    /// * `index` - The index within that dimension
    ///
    /// # Returns
    /// A new slice with one fewer dimension
    ///
    /// # Errors
    /// * `IndexOutOfRange` if `dim >= self.sizes.len()` or `index >=
    ///   self.sizes[dim]`
    pub fn at(&self, dim: usize, index: usize) -> Result<Self, SliceError> {
        if dim >= self.sizes.len() {
            return Err(SliceError::DimensionOutOfRange {
                dim,
                ndims: self.num_dim(),
            });
        }
        if index >= self.sizes[dim] {
            return Err(SliceError::IndexOutOfRange {
                index,
                total: self.sizes[dim],
            });
        }

        let new_offset = self.offset + index * self.strides[dim];
        let mut new_sizes = self.sizes.clone();
        let mut new_strides = self.strides.clone();
        new_sizes.remove(dim);
        new_strides.remove(dim);
        let slice = Slice::new(new_offset, new_sizes, new_strides)?;
        Ok(slice)
    }

    /// A slice defines a **strided view**; a triple (`offset,
    /// `sizes`, `strides`). Each coordinate maps to a flat memory
    /// index using the formula:
    /// ```text
    /// index = offset + ∑ iₖ × strides[k]
    /// ```
    /// where `iₖ` is the coordinate in dimension `k`.
    ///
    /// The `select(dim, range)` operation restricts the view to a
    /// subrange along a single dimension. It calculates a new slice
    /// from a base slice by updating the `offset`, `sizes[dim]`, and
    /// `strides[dim]` to describe a logically reindexed subregion:
    /// ```text
    /// offset       += begin × strides[dim]
    /// sizes[dim]    = ⎡(end - begin) / step⎤
    /// strides[dim] ×= step
    /// ```
    ///
    /// This transformation preserves the strided layout and avoids
    /// copying data. After `select`, the view behaves as if indexing
    /// starts at zero in the selected dimension, with a new length
    /// and stride. From the user's perspective, nothing changes;
    /// indexing remains zero-based, and the resulting shape can be
    /// used like any other. The transformation is internal: the
    /// view's offset and stride absorb the selection logic.
    pub fn select(
        &self,
        dim: usize,
        begin: usize,
        end: usize,
        step: usize,
    ) -> Result<Self, SliceError> {
        if dim >= self.sizes.len() {
            return Err(SliceError::IndexOutOfRange {
                index: dim,
                total: self.sizes.len(),
            });
        }
        if begin >= self.sizes[dim] {
            return Err(SliceError::IndexOutOfRange {
                index: begin,
                total: self.sizes[dim],
            });
        }
        if end <= begin {
            return Err(SliceError::EmptyRange { begin, end, step });
        }

        let mut offset = self.offset();
        let mut sizes = self.sizes().to_vec();
        let mut strides = self.strides().to_vec();

        offset += begin * strides[dim];
        // The # of elems in `begin..end` with step `step`. This is
        // ⌈(end - begin) / stride⌉ — the number of steps that fit in
        // the half-open interval.
        sizes[dim] = (end - begin).div_ceil(step);
        strides[dim] *= step;

        let slice = Slice::new(offset, sizes, strides)?;
        Ok(slice)
    }

    /// Return the location of the provided coordinates.
    pub fn location(&self, coord: &[usize]) -> Result<usize, SliceError> {
        if coord.len() != self.sizes.len() {
            return Err(SliceError::InvalidDims {
                expected: self.sizes.len(),
                got: coord.len(),
            });
        }
        Ok(self.offset
            + coord
                .iter()
                .zip(&self.strides)
                .map(|(pos, stride)| pos * stride)
                .sum::<usize>())
    }

    /// Return the coordinates of the provided value in the n-d space of this
    /// Slice.
    pub fn coordinates(&self, value: usize) -> Result<Vec<usize>, SliceError> {
        let mut pos = value
            .checked_sub(self.offset)
            .ok_or(SliceError::ValueNotInSlice { value })?;
        let mut result = vec![0; self.sizes.len()];
        let mut sorted_info: Vec<_> = self
            .strides
            .iter()
            .zip(self.sizes.iter().enumerate())
            .collect();
        sorted_info.sort_by_key(|&(stride, _)| *stride);
        for &(stride, (i, &size)) in sorted_info.iter().rev() {
            let (index, new_pos) = if size > 1 {
                (pos / stride, pos % stride)
            } else {
                (0, pos)
            };
            if index >= size {
                return Err(SliceError::ValueNotInSlice { value });
            }
            result[i] = index;
            pos = new_pos;
        }
        if pos != 0 {
            return Err(SliceError::ValueNotInSlice { value });
        }
        Ok(result)
    }

    /// Returns whether the provided rank is contained in this slice.
    pub fn contains(&self, value: usize) -> bool {
        self.coordinates(value).is_ok()
    }

    /// The total length of the slice's indices.
    pub fn len(&self) -> usize {
        self.sizes.iter().product()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterator over the slice's indices.
    pub fn iter(&self) -> SliceIterator {
        SliceIterator {
            slice: self.clone(),
            pos: CartesianIterator::new(self.sizes.clone()),
        }
    }

    /// Iterator over sub-dimensions of the slice.
    pub fn dim_iter(&self, dims: usize) -> DimSliceIterator {
        DimSliceIterator {
            pos: CartesianIterator::new(self.sizes[0..dims].to_vec()),
        }
    }

    /// The linear index formula calculates the logical rank of a
    /// multidimensional point in a row-major flattened array,
    /// assuming dense gapless storage with zero offset:
    ///
    /// ```plain
    ///     index := Σ(coordinate[i] × ∏(sizes[j] for j > i))
    /// ```
    ///
    /// For example, given a 3x2 row-major base array B:
    ///
    /// ```plain
    ///       0 1 2         1
    /// B =   3 4 5    V =  4
    ///       6 7 8         7
    /// ```
    ///
    /// Let V be the first column of B. Then,
    ///
    /// ```plain
    /// V      | loc   | index
    /// -------+-------+------
    /// (0, 0) |  1    | 0
    /// (1, 0) |  4    | 1
    /// (2, 0) |  7    | 2
    /// ```
    ///
    /// # Conditions Under Which `loc = index`
    ///
    /// The physical offset formula computes the memory location of a
    /// point `p` as:
    ///
    /// ```plain
    /// loc := offset + Σ(coordinate[i] × stride[i])
    /// ```
    ///
    /// Let the layout be dense row-major and offset = 0.
    /// Then,
    /// ```plain
    /// stride[i] := ∏(sizes[j] for j > i).
    /// ```
    /// and substituting into the physical offset formula:
    /// ```plain
    ///   loc = Σ(coordinate[i] × stride[i])
    ///       = Σ(coordinate[i] × ∏(sizes[j] for j > i))
    ///       = index.
    /// ```
    ///
    /// Thus, ∀ p = (i, j) ∈ B, loc_B(p) = index_B(p).
    ///
    /// # See also
    ///
    /// The [`get`] function performs an inverse operation: given a
    /// logical index in row-major order, it computes the physical
    /// memory offset according to the slice layout. So, if the layout
    /// is row-major then `s.get(s.index(loc)) = loc`.
    pub fn index(&self, value: usize) -> Result<usize, SliceError> {
        let coords = self.coordinates(value)?;
        let mut stride = 1;
        let mut result = 0;

        for (idx, size) in coords.iter().rev().zip(self.sizes.iter().rev()) {
            result += *idx * stride;
            stride *= size;
        }

        Ok(result)
    }

    /// Given a logical index (in row-major order), return the
    /// physical memory offset of that element according to this
    /// slice’s layout.
    ///
    /// The index is interpreted as a position in row-major traversal
    /// that is, iterating across columns within rows. This method
    /// converts logical row-major index to physical offset by:
    ///
    /// 1. Decomposing index into multidimensional coordinates
    /// 2. Computing offset = base + Σ(coordinate[i] × stride[i])
    ///
    /// For example, with shape `[3, 4]` (3 rows, 4 columns) and
    /// column-major layout:
    ///
    /// ```text
    /// sizes  = [3, 4]         // rows, cols
    /// strides = [1, 3]        // column-major: down, then right
    ///
    /// Logical matrix:
    ///   A  B  C  D
    ///   E  F  G  H
    ///   I  J  K  L
    ///
    /// Memory layout:
    /// offset 0  → [0, 0] = A
    /// offset 1  → [1, 0] = E
    /// offset 2  → [2, 0] = I
    /// offset 3  → [0, 1] = B
    /// offset 4  → [1, 1] = F
    /// offset 5  → [2, 1] = J
    /// offset 6  → [0, 2] = C
    /// offset 7  → [1, 2] = G
    /// offset 8  → [2, 2] = K
    /// offset 9  → [0, 3] = D
    /// offset 10 → [1, 3] = H
    /// offset 11 → [2, 3] = L
    ///
    /// Then:
    ///   index = 1  → coordinate [0, 1]  → offset = 0*1 + 1*3 = 3
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if `index >= product(sizes)`.
    ///
    /// # See also
    ///
    /// The [`index`] function performs an inverse operation: given a
    /// memory offset, it returns the logical position of that element
    /// in the slice's row-major iteration order.
    pub fn get(&self, index: usize) -> Result<usize, SliceError> {
        let mut val = self.offset;
        let mut rest = index;
        let mut total = 1;
        for (size, stride) in self.sizes.iter().zip(self.strides.iter()).rev() {
            total *= size;
            val += (rest % size) * stride;
            rest /= size;
        }
        if index < total {
            Ok(val)
        } else {
            Err(SliceError::IndexOutOfRange { index, total })
        }
    }

    /// The returned [`MapSlice`] is a view of this slice, with its elements
    /// mapped using the provided mapping function.
    pub fn map<T, F>(&self, mapper: F) -> MapSlice<'_, T, F>
    where
        F: Fn(usize) -> T,
    {
        MapSlice {
            slice: self,
            mapper,
        }
    }

    /// Returns a new [`Slice`] with the given shape by reinterpreting
    /// the layout of this slice.
    ///
    /// Constructs a new shape with standard row-major strides, using
    /// the same base offset. Returns an error if the reshaped view
    /// would access coordinates not valid in the original slice.
    ///
    /// # Requirements
    ///
    /// - This slice must be contiguous and have offset == 0.
    /// - The number of elements must match:
    ///   `self.sizes().iter().product() == new_sizes.iter().product()`
    /// - Each flat offset in the proposed view must be valid in `self`.
    ///
    /// # Errors
    ///
    /// Returns [`SliceError::IncompatibleView`] if:
    /// - The element count differs
    /// - The base offset is nonzero
    /// - Any offset in the view is not reachable in the original slice
    ///
    /// # Example
    ///
    /// ```rust
    /// use ndslice::Slice;
    /// let base = Slice::new_row_major(&[2, 3, 4]);
    /// let reshaped = base.view(&[6, 4]).unwrap();
    /// ```
    pub fn view(&self, new_sizes: &[usize]) -> Result<Slice, SliceError> {
        let view_elems: usize = new_sizes.iter().product();
        let base_elems: usize = self.sizes().iter().product();

        // TODO: This version of `view` requires that `self` be
        // "dense":
        //
        //   - `self.offset == 0`
        //   - `self.strides` match the row-major layout for
        //     `self.sizes`
        //   - `self.len() == self.sizes.iter().product::<usize>()`
        //
        // Future iterations of this function will aim to relax or
        // remove the "dense" requirement where possible.

        if view_elems != base_elems {
            return Err(SliceError::IncompatibleView {
                reason: format!(
                    "element count mismatch: base has {}, view wants {}",
                    base_elems, view_elems
                ),
            });
        }
        if self.offset != 0 {
            return Err(SliceError::IncompatibleView {
                reason: format!("view requires base offset = 0, but found {}", self.offset),
            });
        }
        // Compute row-major strides.
        let mut new_strides = vec![1; new_sizes.len()];
        for i in (0..new_sizes.len().saturating_sub(1)).rev() {
            new_strides[i] = new_strides[i + 1] * new_sizes[i + 1];
        }

        // Validate that every address in the new view maps to a valid
        // coordinate in base.
        for coord in CartesianIterator::new(new_sizes.to_vec()) {
            #[allow(clippy::identity_op)]
            let offset_in_view = 0 + coord
                .iter()
                .zip(&new_strides)
                .map(|(i, s)| i * s)
                .sum::<usize>();

            if self.coordinates(offset_in_view).is_err() {
                return Err(SliceError::IncompatibleView {
                    reason: format!("offset {} not reachable in base", offset_in_view),
                });
            }
        }

        Ok(Slice {
            offset: 0,
            sizes: new_sizes.to_vec(),
            strides: new_strides,
        })
    }

    /// Returns a sub-slice of `self` starting at `starts`, of size `lens`.
    pub fn subview(&self, starts: &[usize], lens: &[usize]) -> Result<Slice, SliceError> {
        if starts.len() != self.num_dim() || lens.len() != self.num_dim() {
            return Err(SliceError::InvalidDims {
                expected: self.num_dim(),
                got: starts.len().max(lens.len()),
            });
        }

        for (d, (&start, &len)) in starts.iter().zip(lens).enumerate() {
            if start + len > self.sizes[d] {
                return Err(SliceError::IndexOutOfRange {
                    index: start + len,
                    total: self.sizes[d],
                });
            }
        }

        let offset = self.location(starts)?;
        Ok(Slice {
            offset,
            sizes: lens.to_vec(),
            strides: self.strides.clone(),
        })
    }

    /// Ensures that every storage offset used by `self` is valid in
    /// `other`.
    ///
    /// That is, for all p ∈ self:
    /// `other.coordinates(self.location(p))` is defined.
    ///
    /// Returns `self` on success, enabling fluent chaining.
    ///
    /// # Examples
    ///
    /// ```
    /// use ndslice::Slice;
    ///
    /// let base = Slice::new(0, vec![4, 4], vec![4, 1]).unwrap();
    /// let view = base.subview(&[1, 1], &[2, 2]).unwrap();
    /// assert_eq!(view.enforce_embedding(&base).unwrap().len(), 4);
    ///
    /// let small = Slice::new(0, vec![2, 2], vec![2, 1]).unwrap();
    /// assert!(view.enforce_embedding(&small).is_err());
    ///  ```
    pub fn enforce_embedding<'a>(&'a self, other: &'_ Slice) -> Result<&'a Slice, SliceError> {
        self.iter()
            .try_for_each(|loc| other.coordinates(loc).map(|_| ()))?;
        Ok(self)
    }
}

impl std::fmt::Display for Slice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl IntoIterator for &Slice {
    type Item = usize;
    type IntoIter = SliceIterator;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct SliceIterator {
    pub(crate) slice: Slice,
    pos: CartesianIterator,
}

impl Iterator for SliceIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self.pos.next() {
            None => None,
            Some(pos) => Some(self.slice.location(&pos).unwrap()),
        }
    }
}

/// Iterates over the Cartesian product of a list of dimension sizes.
///
/// Given a list of dimension sizes `[d₀, d₁, ..., dₖ₋₁]`, this yields
/// all coordinate tuples `[i₀, i₁, ..., iₖ₋₁]` where each `iⱼ ∈
/// 0..dⱼ`.
///
/// Coordinates are yielded in row-major order (last dimension varies
/// fastest).
pub struct DimSliceIterator {
    pos: CartesianIterator,
}

impl Iterator for DimSliceIterator {
    type Item = Vec<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pos.next()
    }
}

/// Iterates over all coordinate tuples in an N-dimensional space.
///
/// Yields each point in row-major order for the shape defined by
/// `dims`, where each coordinate lies in `[0..dims[i])`.
/// # Example
/// ```ignore
/// let iter = CartesianIterator::new(vec![2, 3]);
/// let coords: Vec<_> = iter.collect();
/// assert_eq!(coords, vec![
///     vec![0, 0], vec![0, 1], vec![0, 2],
///     vec![1, 0], vec![1, 1], vec![1, 2],
/// ]);
/// ```
pub(crate) struct CartesianIterator {
    dims: Vec<usize>,
    index: usize,
}

impl CartesianIterator {
    pub(crate) fn new(dims: Vec<usize>) -> Self {
        CartesianIterator { dims, index: 0 }
    }
}

impl Iterator for CartesianIterator {
    type Item = Vec<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.dims.iter().product::<usize>() {
            return None;
        }

        let mut result: Vec<usize> = vec![0; self.dims.len()];
        let mut rest = self.index;
        for (i, dim) in self.dims.iter().enumerate().rev() {
            result[i] = rest % dim;
            rest /= dim;
        }
        self.index += 1;
        Some(result)
    }
}

/// MapSlice is a view of the underlying Slice that maps each rank
/// into a different type.
pub struct MapSlice<'a, T, F>
where
    F: Fn(usize) -> T,
{
    slice: &'a Slice,
    mapper: F,
}

impl<'a, T, F> MapSlice<'a, T, F>
where
    F: Fn(usize) -> T,
{
    /// The underlying slice sizes.
    pub fn sizes(&self) -> &[usize] {
        &self.slice.sizes
    }

    /// The underlying slice strides.
    pub fn strides(&self) -> &[usize] {
        &self.slice.strides
    }

    /// The mapped value at the provided coordinates. See [`Slice::location`].
    pub fn location(&self, coord: &[usize]) -> Result<T, SliceError> {
        self.slice.location(coord).map(&self.mapper)
    }

    /// The mapped value at the provided index. See [`Slice::get`].
    pub fn get(&self, index: usize) -> Result<T, SliceError> {
        self.slice.get(index).map(&self.mapper)
    }

    /// The underlying slice length.
    pub fn len(&self) -> usize {
        self.slice.len()
    }

    /// Whether the underlying slice is empty.
    pub fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::vec;

    use super::*;

    #[test]
    fn test_cartesian_iterator() {
        let dims = vec![2, 2, 2];
        let iter = CartesianIterator::new(dims);
        let products: Vec<Vec<usize>> = iter.collect();
        assert_eq!(
            products,
            vec![
                vec![0, 0, 0],
                vec![0, 0, 1],
                vec![0, 1, 0],
                vec![0, 1, 1],
                vec![1, 0, 0],
                vec![1, 0, 1],
                vec![1, 1, 0],
                vec![1, 1, 1],
            ]
        );
    }

    #[test]
    #[allow(clippy::explicit_counter_loop)]
    fn test_slice() {
        let s = Slice::new(0, vec![2, 3], vec![3, 1]).unwrap();
        for i in 0..4 {
            assert_eq!(s.get(i).unwrap(), i);
        }

        {
            // Test IntoIter
            let mut current = 0;
            for index in &s {
                assert_eq!(index, current);
                current += 1;
            }
        }

        let s = Slice::new(0, vec![3, 4, 5], vec![20, 5, 1]).unwrap();
        assert_eq!(s.get(3 * 4 + 1).unwrap(), 13);

        let s = Slice::new(0, vec![2, 2, 2], vec![4, 32, 1]).unwrap();
        assert_eq!(s.get(0).unwrap(), 0);
        assert_eq!(s.get(1).unwrap(), 1);
        assert_eq!(s.get(2).unwrap(), 32);
        assert_eq!(s.get(3).unwrap(), 33);
        assert_eq!(s.get(4).unwrap(), 4);
        assert_eq!(s.get(5).unwrap(), 5);
        assert_eq!(s.get(6).unwrap(), 36);
        assert_eq!(s.get(7).unwrap(), 37);

        let s = Slice::new(0, vec![2, 2, 2], vec![32, 4, 1]).unwrap();
        assert_eq!(s.get(0).unwrap(), 0);
        assert_eq!(s.get(1).unwrap(), 1);
        assert_eq!(s.get(2).unwrap(), 4);
        assert_eq!(s.get(4).unwrap(), 32);
    }

    #[test]
    fn test_slice_iter() {
        let s = Slice::new(0, vec![2, 3], vec![3, 1]).unwrap();
        assert!(s.iter().eq(0..6));

        let s = Slice::new(10, vec![10, 2], vec![10, 5]).unwrap();
        assert!(s.iter().eq((10..=105).step_by(5)));

        // Implementaion corresponds with Slice::get.
        assert!(s.iter().eq((0..s.len()).map(|i| s.get(i).unwrap())));
    }

    #[test]
    fn test_dim_slice_iter() {
        let s = Slice::new(0, vec![2, 3], vec![3, 1]).unwrap();
        let sub_dims: Vec<_> = s.dim_iter(1).collect();
        assert_eq!(sub_dims, vec![vec![0], vec![1]]);
    }

    #[test]
    fn test_slice_coordinates() {
        let s = Slice::new(0, vec![2, 3], vec![3, 1]).unwrap();
        assert_eq!(s.coordinates(0).unwrap(), vec![0, 0]);
        assert_eq!(s.coordinates(3).unwrap(), vec![1, 0]);
        assert_matches!(
            s.coordinates(6),
            Err(SliceError::ValueNotInSlice { value: 6 })
        );

        let s = Slice::new(10, vec![2, 3], vec![3, 1]).unwrap();
        assert_matches!(
            s.coordinates(6),
            Err(SliceError::ValueNotInSlice { value: 6 })
        );
        assert_eq!(s.coordinates(10).unwrap(), vec![0, 0]);
        assert_eq!(s.coordinates(13).unwrap(), vec![1, 0]);

        let s = Slice::new(0, vec![2, 1, 1], vec![1, 1, 1]).unwrap();
        assert_eq!(s.coordinates(1).unwrap(), vec![1, 0, 0]);
    }

    #[test]
    fn test_slice_index() {
        let s = Slice::new(0, vec![2, 3], vec![3, 1]).unwrap();
        assert_eq!(s.index(3).unwrap(), 3);
        assert!(s.index(14).is_err());

        let s = Slice::new(0, vec![2, 2], vec![4, 2]).unwrap();
        assert_eq!(s.index(2).unwrap(), 1);
    }

    #[test]
    fn test_slice_map() {
        let s = Slice::new(0, vec![2, 3], vec![3, 1]).unwrap();
        let m = s.map(|i| i * 2);
        assert_eq!(m.get(0).unwrap(), 0);
        assert_eq!(m.get(3).unwrap(), 6);
        assert_eq!(m.get(5).unwrap(), 10);
    }

    #[test]
    fn test_slice_size_one() {
        let s = Slice::new(0, vec![1, 1], vec![1, 1]).unwrap();
        assert_eq!(s.get(0).unwrap(), 0);
    }

    #[test]
    fn test_row_major() {
        let s = Slice::new_row_major(vec![4, 4, 4]);
        assert_eq!(s.offset(), 0);
        assert_eq!(s.sizes(), &[4, 4, 4]);
        assert_eq!(s.strides(), &[16, 4, 1]);
    }

    #[test]
    fn test_slice_view_smoke() {
        use crate::Slice;

        let base = Slice::new_row_major([2, 3, 4]);

        // Reshape: compatible shape and layout
        let view = base.view(&[6, 4]).unwrap();
        assert_eq!(view.sizes(), &[6, 4]);
        assert_eq!(view.offset(), 0);
        assert_eq!(view.strides(), &[4, 1]);
        assert_eq!(
            view.location(&[5, 3]).unwrap(),
            base.location(&[1, 2, 3]).unwrap()
        );

        // Reshape: identity (should succeed)
        let view = base.view(&[2, 3, 4]).unwrap();
        assert_eq!(view.sizes(), base.sizes());
        assert_eq!(view.strides(), base.strides());

        // Reshape: incompatible shape (wrong element count)
        let err = base.view(&[5, 4]);
        assert!(err.is_err());

        // Reshape: incompatible layout (simulate select)
        let selected = Slice::new(1, vec![2, 3], vec![6, 1]).unwrap(); // not offset=0
        let err = selected.view(&[3, 2]);
        assert!(err.is_err());

        // Reshape: flat 1D view
        let flat = base.view(&[24]).unwrap();
        assert_eq!(flat.sizes(), &[24]);
        assert_eq!(flat.strides(), &[1]);
        assert_eq!(
            flat.location(&[23]).unwrap(),
            base.location(&[1, 2, 3]).unwrap()
        );
    }

    #[test]
    fn test_view_of_view_when_dense() {
        // Start with a dense base: 2 × 3 × 4 = 24 elements.
        let base = Slice::new_row_major([2, 3, 4]);

        // First view: flatten to 1D.
        let flat = base.view(&[24]).unwrap();
        assert_eq!(flat.sizes(), &[24]);
        assert_eq!(flat.strides(), &[1]);
        assert_eq!(flat.offset(), 0); // Still dense.

        // Second view: reshape 1D to 6 × 4.
        let reshaped = flat.view(&[6, 4]).unwrap();
        assert_eq!(reshaped.sizes(), &[6, 4]);
        assert_eq!(reshaped.strides(), &[4, 1]);
        assert_eq!(reshaped.offset(), 0);

        // Location agreement check
        assert_eq!(
            reshaped.location(&[5, 3]).unwrap(),
            base.location(&[1, 2, 3]).unwrap()
        );
    }

    #[test]
    fn test_at_1d_to_0d() {
        let slice = Slice::new_row_major(vec![5]);
        assert_eq!(slice.num_dim(), 1);
        assert_eq!(slice.sizes(), &[5]);
        assert_eq!(slice.strides(), &[1]);

        let result = slice.at(0, 3).unwrap();
        assert_eq!(result.num_dim(), 0);
        assert_eq!(result.sizes(), &[]);
        assert_eq!(result.strides(), &[]);
        assert_eq!(result.offset(), 3);
        assert_eq!(result.location(&[]).unwrap(), 3);
    }

    #[test]
    fn test_at_2d_to_1d() {
        let slice = Slice::new_row_major(vec![3, 4]);
        assert_eq!(slice.num_dim(), 2);
        assert_eq!(slice.sizes(), &[3, 4]);
        assert_eq!(slice.strides(), &[4, 1]);

        let result = slice.at(0, 1).unwrap();
        assert_eq!(result.num_dim(), 1);
        assert_eq!(result.sizes(), &[4]);
        assert_eq!(result.strides(), &[1]);
        assert_eq!(result.offset(), 4);
    }

    #[test]
    fn test_at_3d_to_2d() {
        let slice = Slice::new_row_major(vec![2, 3, 4]);
        assert_eq!(slice.num_dim(), 3);
        assert_eq!(slice.sizes(), &[2, 3, 4]);
        assert_eq!(slice.strides(), &[12, 4, 1]);

        let result = slice.at(0, 1).unwrap();
        assert_eq!(result.num_dim(), 2);
        assert_eq!(result.sizes(), &[3, 4]);
        assert_eq!(result.strides(), &[4, 1]);
        assert_eq!(result.offset(), 12);
    }

    #[test]
    fn test_get_index_inverse_relationship() {
        // Start with a 3 x 3 dense row major matrix.
        //
        // 0 1 2
        // 3 4 5
        // 6 7 8
        let m = Slice::new_row_major([3, 3]);
        assert_eq!(m.offset, 0);
        assert_eq!(m.sizes(), &[3, 3]);
        assert_eq!(m.strides(), &[3, 1]);

        // Slice `m` is 0-offset, row-major, dense, gapless.
        for loc in m.iter() {
            // ∀ `loc` ∈ `m`, `m.index(loc) == loc`.
            assert_eq!(m.index(loc).unwrap(), loc);
            // ∀ `loc` ∈ `m`, `m.get(m.index(loc)) == loc`.
            assert_eq!(m.get(m.index(loc).unwrap()).unwrap(), loc);
        }

        // Slice out the middle column.
        //    1
        //    4
        //    7
        let c = m.select(1, 1, 2, 1).unwrap();
        assert_eq!(c.sizes(), &[3, 1]);
        assert_eq!(c.strides(), &[3, 1]);

        // Slice `c` has a non-zero offset.
        for loc in c.iter() {
            // Local rank of `loc` in `c` != loc.
            assert_ne!(c.index(loc).unwrap(), loc);
            // ∀ `loc` ∈ `c`, `c.get(c.index(loc)) == loc`.
            assert_eq!(c.get(c.index(loc).unwrap()).unwrap(), loc);
        }
    }

    #[test]
    fn embedding_succeeds_for_contained_view() {
        let base = Slice::new(0, vec![4, 4], vec![4, 1]).unwrap(); // 4×4 matrix, row-major
        let view = Slice::new(5, vec![2, 2], vec![4, 1]).unwrap(); // a 2×2 submatrix starting at (1,1)

        assert!(view.enforce_embedding(&base).is_ok());
    }

    #[test]
    fn embedding_fails_for_out_of_bounds_view() {
        let base = Slice::new(0, vec![4, 4], vec![4, 1]).unwrap(); // 4×4 matrix
        let view = Slice::new(14, vec![2, 2], vec![4, 1]).unwrap(); // starts at (3,2), accesses (4,3)

        assert!(view.enforce_embedding(&base).is_err());
    }
}
