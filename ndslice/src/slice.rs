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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Hash)]
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

    /// Create a new slice of the given sizes in row-major order.
    pub fn new_row_major(sizes: Vec<usize>) -> Self {
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

    /// Retrieve the underlying location of the provided slice index.
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
            slice: self,
            pos: CartesianIterator::new(&self.sizes),
        }
    }

    /// Iterator over sub-dimensions of the slice.
    pub fn dim_iter(&self, dims: usize) -> DimSliceIterator {
        DimSliceIterator {
            pos: CartesianIterator::new(&self.sizes[0..dims]),
        }
    }

    /// Returns the index into the flattened representation of `self` where
    /// `self[index] == value`.
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
}

impl<'a> IntoIterator for &'a Slice {
    type Item = usize;
    type IntoIter = SliceIterator<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct SliceIterator<'a> {
    slice: &'a Slice,
    pos: CartesianIterator<'a>,
}

impl<'a> Iterator for SliceIterator<'a> {
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
pub struct DimSliceIterator<'a> {
    pos: CartesianIterator<'a>,
}

impl<'a> Iterator for DimSliceIterator<'a> {
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
/// let iter = CartesianIterator::new(&[2, 3]);
/// let coords: Vec<_> = iter.collect();
/// assert_eq!(coords, vec![
///     vec![0, 0], vec![0, 1], vec![0, 2],
///     vec![1, 0], vec![1, 1], vec![1, 2],
/// ]);
/// ```
struct CartesianIterator<'a> {
    dims: &'a [usize],
    index: usize,
}

impl<'a> CartesianIterator<'a> {
    fn new(dims: &'a [usize]) -> Self {
        CartesianIterator { dims, index: 0 }
    }
}

impl<'a> Iterator for CartesianIterator<'a> {
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
        let iter = CartesianIterator::new(&dims);
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
}
