/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor_mesh::v1::ValueMesh;
use ndslice::Region;
use ndslice::view::BuildFromRegion;
use ndslice::view::Ranked;
use ndslice::view::ViewExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyList;

use crate::shape::PyShape;

#[pyclass(
    name = "ValueMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.value_mesh"
)]
pub struct PyValueMesh {
    inner: ValueMesh<Py<PyAny>>,
}

#[pymethods]
impl PyValueMesh {
    /// __init__(self, shape: Shape, values: list)
    #[new]
    fn new(_py: Python<'_>, shape: &PyShape, values: Bound<'_, PyList>) -> PyResult<Self> {
        // Convert shape to region, preserving the original Slice
        // (offset/strides) so linear rank order matches the Python
        // Shape.
        let s = shape.get_inner();
        let region = Region::new(s.labels().to_vec(), s.slice().clone());
        let vals: Vec<Py<PyAny>> = values.extract()?;

        // Build & validate cardinality against region.
        let mut inner =
            <ValueMesh<Py<PyAny>> as BuildFromRegion<Py<PyAny>>>::build_dense(region, vals)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

        // Coalesce adjacent identical Python objects (same pointer
        // identity). For Py<PyAny>, we treat equality as object
        // identity: consecutive references to the *same* object
        // pointer are merged into RLE runs. This tends to compress
        // sentinel/categorical/boolean data, but not freshly
        // allocated numerics/strings.
        inner.compress_adjacent_in_place_by(|a, b| a.as_ptr() == b.as_ptr());

        Ok(Self { inner })
    }

    /// Return number of ranks (Python: len(vm))
    fn __len__(&self) -> usize {
        self.inner.region().num_ranks()
    }

    /// Return the values in region/iteration order as a Python list.
    fn values(&self, py: Python<'_>) -> PyResult<PyObject> {
        // Clone the inner Py objects into a Python list (just bumps
        // refcounts).
        let vec: Vec<Py<PyAny>> = self.inner.values().collect();
        Ok(PyList::new(py, vec)?.into())
    }

    /// Get value by linear rank (0..num_ranks-1).
    fn get(&self, py: Python<'_>, rank: usize) -> PyResult<PyObject> {
        let n = self.inner.region().num_ranks();
        if rank >= n {
            return Err(PyValueError::new_err(format!(
                "index {} out of range (len={})",
                rank, n
            )));
        }

        // ValueMesh::get() returns &Py<PyAny>; we clone the smart
        // pointer (incrementing the Python refcount) to return an
        // owned Py<PyAny>. `unwrap` is safe because the bounds have
        // been checked.
        let v: Py<PyAny> = self.inner.get(rank).unwrap().clone_ref(py);

        Ok(v)
    }

    /// Build from (rank, value) pairs with last-write-wins semantics.
    #[staticmethod]
    fn from_indexed(
        _py: Python<'_>,
        shape: &PyShape,
        pairs: Vec<(usize, Py<PyAny>)>,
    ) -> PyResult<Self> {
        // Preserve the shape's original Slice (offset/strides).
        let s = shape.get_inner();
        let region = Region::new(s.labels().to_vec(), s.slice().clone());
        let mut inner = <ValueMesh<Py<PyAny>> as ndslice::view::BuildFromRegionIndexed<
            Py<PyAny>,
        >>::build_indexed(region, pairs)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        // Coalesce adjacent identical Python objects (same pointer
        // identity). For Py<PyAny>, we treat equality as object
        // identity: consecutive references to the *same* object
        // pointer are merged into RLE runs. This tends to compress
        // sentinel/categorical/boolean data, but not freshly
        // allocated numerics/strings.
        inner.compress_adjacent_in_place_by(|a, b| a.as_ptr() == b.as_ptr());

        Ok(Self { inner })
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyValueMesh>()?;
    Ok(())
}
