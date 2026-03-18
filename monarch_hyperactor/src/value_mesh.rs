/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Mutex;

use hyperactor_mesh::ValueMesh;
use ndslice::Extent;
use ndslice::Region;
use ndslice::view::BuildFromRegion;
use ndslice::view::Ranked;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyList;
use serde_multipart::Part;

use crate::buffers::FrozenBuffer;
use crate::pickle::unpickle;
use crate::shape::PyShape;

/// A value that is either raw pickled bytes or an already-unpickled
/// Python object. On first access, [`Pickled`] is unpickled and
/// replaced with [`Unpickled`] so subsequent accesses skip
/// deserialization.
#[derive(Clone)]
enum LazyPyObject {
    Pickled(Part),
    Unpickled(Py<PyAny>),
}

type LazyCell = Mutex<LazyPyObject>;

impl LazyPyObject {
    /// Resolve to a Python object, caching the result in place.
    /// After this call the cell will contain [`Unpickled`].
    fn resolve(cell: &LazyCell, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut guard = cell.lock().unwrap();

        match &*guard {
            LazyPyObject::Unpickled(obj) => Ok(obj.clone_ref(py)),
            LazyPyObject::Pickled(part) => {
                let py_obj = unpickle(
                    py,
                    FrozenBuffer {
                        inner: part.clone().into_bytes(),
                    },
                )?
                .unbind();

                *guard = LazyPyObject::Unpickled(py_obj.clone_ref(py));

                Ok(py_obj)
            }
        }
    }
}

fn compress(inner: &mut ValueMesh<LazyCell>) {
    inner.compress_adjacent_in_place_by(|a, b| match (&*a.lock().unwrap(), &*b.lock().unwrap()) {
        (LazyPyObject::Unpickled(a), LazyPyObject::Unpickled(b)) => a.as_ptr() == b.as_ptr(),
        (LazyPyObject::Pickled(a), LazyPyObject::Pickled(b)) => a == b,
        _ => false,
    });
}

#[pyclass(name = "ValueMesh", module = "monarch._src.actor.actor_mesh")]
pub struct PyValueMesh {
    inner: ValueMesh<LazyCell>,
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
        let vals: Vec<LazyCell> = values
            .extract::<Vec<Py<PyAny>>>()?
            .into_iter()
            .map(|v| Mutex::new(LazyPyObject::Unpickled(v)))
            .collect();

        let mut inner =
            <ValueMesh<LazyCell> as BuildFromRegion<LazyCell>>::build_dense(region, vals)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

        // Coalesce adjacent identical Python objects (same pointer
        // identity). For Py<PyAny>, we treat equality as object
        // identity: consecutive references to the *same* object
        // pointer are merged into RLE runs. This tends to compress
        // sentinel/categorical/boolean data, but not freshly
        // allocated numerics/strings.
        compress(&mut inner);

        Ok(Self { inner })
    }

    /// Return number of ranks (Python: len(vm))
    fn __len__(&self) -> usize {
        self.inner.region().num_ranks()
    }

    /// Expose the shape so Python MeshTrait methods can access labels/ndslice.
    #[getter]
    fn _shape(&self) -> PyShape {
        PyShape::from(ndslice::Shape::from(self.inner.region().clone()))
    }

    /// Return the values in region/iteration order as a Python list.
    fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let n = self.inner.region().num_ranks();
        let mut vec: Vec<Py<PyAny>> = Vec::with_capacity(n);
        for rank in 0..n {
            vec.push(LazyPyObject::resolve(self.inner.get(rank).unwrap(), py)?);
        }
        Ok(PyList::new(py, vec)?.into())
    }

    /// Get value by linear rank (0..num_ranks-1).
    fn get(&self, py: Python<'_>, rank: usize) -> PyResult<Py<PyAny>> {
        let n = self.inner.region().num_ranks();
        if rank >= n {
            return Err(PyValueError::new_err(format!(
                "index {} out of range (len={})",
                rank, n
            )));
        }

        LazyPyObject::resolve(self.inner.get(rank).unwrap(), py)
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
        let lazy_pairs: Vec<(usize, LazyCell)> = pairs
            .into_iter()
            .map(|(rank, obj)| (rank, Mutex::new(LazyPyObject::Unpickled(obj))))
            .collect();
        let mut inner = <ValueMesh<LazyCell> as ndslice::view::BuildFromRegionIndexed<
            LazyCell,
        >>::build_indexed(region, lazy_pairs)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        // Coalesce adjacent identical Python objects (same pointer
        // identity). For Py<PyAny>, we treat equality as object
        // identity: consecutive references to the *same* object
        // pointer are merged into RLE runs. This tends to compress
        // sentinel/categorical/boolean data, but not freshly
        // allocated numerics/strings.
        compress(&mut inner);

        Ok(Self { inner })
    }
}

impl PyValueMesh {
    /// Create a lazy ValueMesh from an extent and raw pickled parts.
    /// Values are unpickled on demand when accessed via `get()` or `values()`.
    pub fn build_from_parts(extent: &Extent, parts: Vec<Part>) -> PyResult<Self> {
        let lazy_values: Vec<LazyCell> = parts
            .into_iter()
            .map(|p| Mutex::new(LazyPyObject::Pickled(p)))
            .collect();
        let mut inner = <ValueMesh<LazyCell> as BuildFromRegion<LazyCell>>::build_dense(
            ndslice::View::region(extent),
            lazy_values,
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        compress(&mut inner);

        Ok(Self { inner })
    }
}

/// Test helper: create a ValueMesh entirely from Rust and return it to Python.
/// This lets us verify that Python extension methods (patched via @rust_struct)
/// are available on objects returned from Rust functions.
#[pyfunction]
fn _make_test_value_mesh(
    labels: Vec<String>,
    sizes: Vec<usize>,
    values: Bound<'_, PyList>,
) -> PyResult<PyValueMesh> {
    let strides: Vec<usize> = {
        let mut s = vec![1usize; sizes.len()];
        for i in (0..sizes.len().saturating_sub(1)).rev() {
            s[i] = s[i + 1] * sizes[i + 1];
        }
        s
    };
    let slice =
        ndslice::Slice::new(0, sizes, strides).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let region = Region::new(labels, slice);
    let vals: Vec<LazyCell> = values
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|v| Mutex::new(LazyPyObject::Unpickled(v)))
        .collect();
    let mut inner = <ValueMesh<LazyCell> as BuildFromRegion<LazyCell>>::build_dense(region, vals)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    compress(&mut inner);
    Ok(PyValueMesh { inner })
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyValueMesh>()?;
    module.add_function(wrap_pyfunction!(_make_test_value_mesh, module)?)?;
    Ok(())
}
