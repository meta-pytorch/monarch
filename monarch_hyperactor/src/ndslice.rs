/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyIndexError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::types::PySliceMethods;
use pyo3::types::PyTuple;

/// A wrapper around [ndslice::Slice] to expose it to python.
/// It is a compact representation of indices into the flat
/// representation of an n-dimensional array. Given an offset, sizes of
/// each dimension, and strides for each dimension, Slice can compute
/// indices into the flat array.
#[pyclass(
    name = "Slice",
    frozen,
    module = "monarch._rust_bindings.monarch_hyperactor.shape"
)]
#[derive(Clone)]
pub struct PySlice {
    inner: Arc<ndslice::Slice>,
}

#[pymethods]
impl PySlice {
    #[new]
    #[pyo3(signature = (*, offset, sizes, strides))]
    fn new(offset: usize, sizes: Vec<usize>, strides: Vec<usize>) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(
                ndslice::Slice::new(offset, sizes, strides)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?,
            ),
        })
    }

    /// Returns the number of dimensions of the slice.
    #[getter]
    fn ndim(&self) -> usize {
        self.inner.sizes().len()
    }

    /// Returns the offset of the slice.
    #[getter]
    fn offset(&self) -> usize {
        self.inner.offset()
    }

    /// Returns the sizes of each of the dimensions of the slice.
    #[getter]
    fn sizes(&self) -> Vec<usize> {
        self.inner.sizes().to_vec()
    }

    /// Returns the strides of each of the dimensions of the slice.
    #[getter]
    fn strides(&self) -> Vec<usize> {
        self.inner.strides().to_vec()
    }

    /// Returns the index of the given value in the slice or raises a `ValueError`
    /// if the value is not in the slice.
    fn index(&self, value: usize) -> PyResult<usize> {
        self.inner
            .index(value)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    /// Returns the coordinates of the given value in the slice or raises a `ValueError`
    /// if the value is not in the slice.
    fn coordinates(&self, value: usize) -> PyResult<Vec<usize>> {
        self.inner
            .coordinates(value)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    /// Returns the value at the given coordinates or raises an `IndexError` if the coordinates
    /// are out of bounds.
    fn nditem(&self, coordinates: Vec<usize>) -> PyResult<usize> {
        self.inner
            .location(&coordinates)
            .map_err(|err| PyIndexError::new_err(err.to_string()))
    }

    /// Returns the value at the given index or raises an `IndexError` if the index is out of bounds.
    fn __getitem__(&self, py: Python<'_>, range: Range<'_>) -> PyResult<Py<PyAny>> {
        match range {
            Range::Single(index) => self
                .inner
                .get(index)
                .map(|res| res.into_py_any(py))
                .map_err(|err| PyIndexError::new_err(err.to_string()))?,
            Range::Slice(slice) => {
                let indices =
                    slice.indices((self.inner.len() as std::os::raw::c_long).try_into()?)?;
                let (start, stop, step) = (indices.start, indices.stop, indices.step);
                if start < 0 || stop < 0 {
                    return Err(PyIndexError::new_err("Only positive indices are support"));
                }
                let mut result = Vec::new();
                let mut i = start;
                while if step > 0 { i < stop } else { i > stop } {
                    result.push(
                        self.inner
                            .get(i as usize)
                            .map_err(|err| PyIndexError::new_err(err.to_string()))?,
                    );
                    i += step;
                }
                PyTuple::new(py, result)?.into_py_any(py)
            }
        }
    }

    fn __iter__(&self) -> PySliceIterator {
        PySliceIterator::new(self.inner.clone())
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __getnewargs_ex__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("offset", self.inner.offset()).unwrap();
        kwargs.set_item("sizes", self.inner.sizes()).unwrap();
        kwargs.set_item("strides", self.inner.strides()).unwrap();

        PyTuple::new(
            py,
            vec![
                PyTuple::empty(py).unbind().into_any(),
                kwargs.unbind().into_any(),
            ],
        )
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(other) = other.extract::<PySlice>() {
            Ok(self.inner == other.inner)
        } else {
            Ok(false)
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.inner.hash(&mut hasher);
        hasher.finish()
    }

    /// Returns a list of slices that cover the given list of ranks.
    #[staticmethod]
    fn from_list(py: Python<'_>, ranks: Vec<usize>) -> PyResult<Py<PyAny>> {
        if ranks.is_empty() {
            return PyList::empty(py).into_py_any(py);
        }
        let mut ranks = ranks;
        ranks.sort();

        let mut result = Vec::new();
        let mut offset = ranks[0];
        let mut size = 1;
        let mut stride = 1;
        for &rank in &ranks[1..] {
            if size == 1 && rank > offset {
                stride = rank - offset;
                size += 1;
            } else if offset + size * stride == rank {
                size += 1;
            } else {
                result.push(Self::new(offset, vec![size], vec![stride])?);
                offset = rank;
                size = 1;
                stride = 1;
            }
        }
        result.push(Self::new(offset, vec![size], vec![stride])?);
        result.into_py_any(py)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.inner))
    }

    #[staticmethod]
    fn new_row_major(sizes: Vec<usize>) -> PySlice {
        ndslice::Slice::new_row_major(sizes).into()
    }

    fn get(&self, index: usize) -> PyResult<usize> {
        self.inner
            .get(index)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

impl From<&PySlice> for ndslice::Slice {
    fn from(slice: &PySlice) -> Self {
        slice.inner.as_ref().clone()
    }
}

impl From<PySlice> for ndslice::Slice {
    fn from(slice: PySlice) -> Self {
        slice.inner.as_ref().clone()
    }
}

impl From<ndslice::Slice> for PySlice {
    fn from(value: ndslice::Slice) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

#[derive(Debug, Clone, FromPyObject)]
enum Range<'s> {
    #[pyo3(transparent, annotation = "int")]
    Single(usize),
    #[pyo3(transparent, annotation = "slice")]
    Slice(Bound<'s, pyo3::types::PySlice>),
}

#[pyclass]
struct PySliceIterator {
    data: Arc<ndslice::Slice>,
    index: usize,
}

impl PySliceIterator {
    fn new(data: Arc<ndslice::Slice>) -> Self {
        Self { data, index: 0 }
    }
}

#[pymethods]
impl PySliceIterator {
    fn __iter__(self_: PyRef<'_, Self>) -> PyRef<'_, Self> {
        self_
    }

    fn __next__(&mut self) -> Option<usize> {
        let dims = self.data.sizes();
        if self.index >= dims.iter().product::<usize>() {
            return None;
        }

        let mut coords: Vec<usize> = vec![0; dims.len()];
        let mut rest = self.index;
        for (i, dim) in dims.iter().enumerate().rev() {
            coords[i] = rest % dim;
            rest /= dim;
        }
        self.index += 1;
        Some(self.data.location(&coords).unwrap())
    }
}
