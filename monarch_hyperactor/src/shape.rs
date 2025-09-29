/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use monarch_types::MapPyErr;
use ndslice::Extent;
use ndslice::Point;
use ndslice::Region;
use ndslice::Shape;
use ndslice::Slice;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3::types::PyMapping;
use serde::Deserialize;
use serde::Serialize;

use crate::ndslice::PySlice;

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(
    name = "Extent",
    module = "monarch._rust_bindings.monarch_hyperactor.shape",
    frozen
)]
pub struct PyExtent {
    inner: Extent,
}

#[pymethods]
impl PyExtent {
    #[new]
    pub fn new(labels: Vec<String>, sizes: Vec<usize>) -> PyResult<PyExtent> {
        Ok(PyExtent {
            inner: Extent::new(labels, sizes).map_pyerr()?,
        })
    }
    #[getter]
    fn nelements(&self) -> usize {
        self.inner.num_ranks()
    }
    fn __repr__(&self) -> String {
        self.inner.to_string()
    }
    #[getter]
    fn labels(&self) -> &[String] {
        self.inner.labels()
    }
    #[getter]
    fn sizes(&self) -> &[usize] {
        self.inner.sizes()
    }

    #[staticmethod]
    fn from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let extent: PyExtent = bincode::deserialize(bytes.as_bytes())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        Ok(extent)
    }

    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyBytes>,))> {
        let bytes = bincode::serialize(&*slf.borrow())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = PyBytes::new(slf.py(), &bytes);
        Ok((slf.getattr("from_bytes")?, (py_bytes,)))
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self
            .labels()
            .into_bound_py_any(py)?
            .call_method0("__iter__")?
            .into())
    }

    fn __getitem__(&self, label: &str) -> PyResult<usize> {
        self.inner.size(label).ok_or_else(|| {
            PyErr::new::<PyValueError, _>(format!("Dimension '{}' not found", label))
        })
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self.inner.labels().into_bound_py_any(py)?.into())
    }
}

impl From<Extent> for PyExtent {
    fn from(inner: Extent) -> Self {
        PyExtent { inner }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(
    name = "Region",
    module = "monarch._rust_bindings.monarch_hyperactor.shape",
    frozen
)]
pub struct PyRegion {
    pub(crate) inner: Region,
}

impl PyRegion {
    pub(crate) fn as_inner(&self) -> &Region {
        &self.inner
    }
}

#[pymethods]
impl PyRegion {
    #[new]
    fn new(labels: Vec<String>, slice: PySlice) -> PyResult<Self> {
        Ok(PyRegion {
            inner: Region::new(labels, slice.into()),
        })
    }

    fn as_shape(&self) -> PyShape {
        PyShape {
            inner: (&self.inner).into(),
        }
    }

    fn labels(&self) -> Vec<String> {
        self.inner.labels().to_vec()
    }

    fn slice(&self) -> PySlice {
        self.inner.slice().clone().into()
    }
}

impl From<Region> for PyRegion {
    fn from(inner: Region) -> Self {
        PyRegion { inner }
    }
}

#[pyclass(
    name = "Shape",
    module = "monarch._rust_bindings.monarch_hyperactor.shape",
    frozen
)]
#[derive(Clone)]
pub struct PyShape {
    pub(super) inner: Shape,
}

impl PyShape {
    pub fn get_inner(&self) -> &Shape {
        &self.inner
    }
}

#[pymethods]
impl PyShape {
    #[new]
    fn new(labels: Vec<String>, slice: PySlice) -> PyResult<Self> {
        let shape = Shape::new(labels, Slice::from(slice))
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        Ok(PyShape { inner: shape })
    }

    #[getter]
    fn ndslice(&self) -> PySlice {
        self.inner.slice().clone().into()
    }
    #[getter]
    fn labels(&self) -> Vec<String> {
        self.inner.labels().to_vec()
    }
    fn __str__(&self) -> PyResult<String> {
        Ok(self.inner.to_string())
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.inner))
    }
    fn coordinates<'py>(
        &self,
        py: Python<'py>,
        rank: usize,
    ) -> PyResult<pyo3::Bound<'py, pyo3::types::PyDict>> {
        self.inner
            .coordinates(rank)
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
            .and_then(|x| PyDict::from_sequence(&x.into_bound_py_any(py)?))
    }

    fn at(&self, label: &str, index: usize) -> PyResult<PyShape> {
        Ok(PyShape {
            inner: self
                .inner
                .at(label, index)
                .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?,
        })
    }

    #[pyo3(signature = (**kwargs))]
    fn index(&self, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<PyShape> {
        if let Some(kwargs) = kwargs {
            let mut indices: Vec<(String, usize)> = Vec::new();
            // translate kwargs into indices
            for (key, value) in kwargs.iter() {
                let key_str = key.extract::<String>()?;
                let idx = value.extract::<usize>()?;
                indices.push((key_str, idx));
            }
            Ok(PyShape {
                inner: self
                    .inner
                    .index(indices)
                    .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?,
            })
        } else {
            Ok(PyShape {
                inner: self.inner.clone(),
            })
        }
    }

    fn select(&self, label: &str, slice: &Bound<'_, pyo3::types::PySlice>) -> PyResult<PyShape> {
        let dim = self
            .inner
            .dim(label)
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let size = self.inner.slice().sizes()[dim];

        let indices = slice.indices(size as isize)?;
        let start = indices.start as usize;
        let stop = indices.stop as usize;
        let step = indices.step as usize;

        let range = ndslice::shape::Range(start, Some(stop), step);
        Ok(PyShape {
            inner: self
                .inner
                .select(label, range)
                .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?,
        })
    }

    #[staticmethod]
    fn from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let shape: Shape = bincode::deserialize(bytes.as_bytes())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        Ok(PyShape::from(shape))
    }

    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyBytes>,))> {
        let bytes = bincode::serialize(&slf.borrow().inner)
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = PyBytes::new(slf.py(), &bytes);
        Ok((slf.getattr("from_bytes")?, (py_bytes,)))
    }

    fn ranks(&self) -> Vec<usize> {
        self.inner.slice().iter().collect()
    }

    fn __len__(&self) -> usize {
        self.inner.slice().len()
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(other) = other.extract::<PyShape>() {
            Ok(self.inner == other.inner)
        } else {
            Ok(false)
        }
    }

    #[staticmethod]
    fn unity() -> PyShape {
        Shape::unity().into()
    }

    #[getter]
    fn extent(&self) -> PyExtent {
        self.inner.extent().into()
    }

    #[getter]
    fn region(&self) -> PyRegion {
        PyRegion {
            inner: self.inner.region(),
        }
    }
}

impl From<Shape> for PyShape {
    fn from(shape: Shape) -> Self {
        PyShape { inner: shape }
    }
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
#[pyclass(
    name = "Point",
    module = "monarch._rust_bindings.monarch_hyperactor.shape",
    subclass,
    frozen
)]
pub struct PyPoint {
    rank: usize,
    extent: Extent,
}

#[pymethods]
impl PyPoint {
    #[new]
    pub fn new(rank: usize, extent: PyExtent) -> Self {
        PyPoint {
            rank,
            extent: extent.inner,
        }
    }
    fn __getitem__(&self, label: &str) -> PyResult<usize> {
        let index = self.extent.position(label).ok_or_else(|| {
            PyErr::new::<PyValueError, _>(format!("Dimension '{}' not found", label))
        })?;
        let point = self.extent.point_of_rank(self.rank).map_pyerr()?;
        Ok(point.coords()[index])
    }

    fn size(&self, label: &str) -> PyResult<usize> {
        self.extent.size(label).ok_or_else(|| {
            PyErr::new::<PyValueError, _>(format!("Dimension '{}' not found", label))
        })
    }

    fn __len__(&self) -> usize {
        self.extent.len()
    }
    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self
            .extent
            .labels()
            .into_bound_py_any(py)?
            .call_method0("__iter__")?
            .into())
    }

    #[staticmethod]
    fn from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let point: PyPoint = bincode::deserialize(bytes.as_bytes())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        Ok(point)
    }

    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyBytes>,))> {
        let bytes = bincode::serialize(&*slf.borrow())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = PyBytes::new(slf.py(), &bytes);
        Ok((slf.getattr("from_bytes")?, (py_bytes,)))
    }

    #[getter]
    fn extent(&self) -> PyExtent {
        PyExtent {
            inner: self.extent.clone(),
        }
    }
    #[getter]
    fn rank(&self) -> usize {
        self.rank
    }
    fn __repr__(&self) -> PyResult<String> {
        let point = self.extent.point_of_rank(self.rank).map_pyerr()?;
        let coords = point.coords();
        let labels = self.extent.labels();
        let sizes = self.extent.sizes();
        let mut parts = Vec::new();
        for (i, label) in labels.iter().enumerate() {
            parts.push(format!("'{}': {}/{}", label, coords[i], sizes[i]));
        }

        Ok(format!("{{{}}}", parts.join(", ")))
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(other) = other.extract::<PyPoint>() {
            Ok(*self == other)
        } else {
            Ok(false)
        }
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self.extent.labels().into_bound_py_any(py)?.into())
    }
}
impl From<Point> for PyPoint {
    fn from(inner: Point) -> Self {
        PyPoint {
            rank: inner.rank(),
            extent: inner.extent().clone(),
        }
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add_class::<PyShape>()?;
    module.add_class::<PySlice>()?;
    module.add_class::<PyPoint>()?;
    PyMapping::register::<PyPoint>(py)?;
    module.add_class::<PyExtent>()?;
    PyMapping::register::<PyExtent>(py)?;
    module.add_class::<PyRegion>()?;
    Ok(())
}
