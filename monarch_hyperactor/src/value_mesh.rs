/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use hyperactor_mesh::v1::ValueMesh;
use ndslice::Region;
use ndslice::view::BuildFromRegion;
use ndslice::view::Ranked;
use ndslice::view::ViewExt;
use pyo3::exceptions::PyIndexError;
use pyo3::exceptions::PyKeyError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::types::PyTuple;

use crate::mesh_trait::PyMeshTrait;
use crate::ndslice::PySlice;
use crate::shape::PyExtent;
use crate::shape::PyPoint;
use crate::shape::PyShape;

/// A mesh of values returned from endpoint invocations.
///
/// ValueMesh holds the return values from all actors in a mesh, organized by
/// their coordinates. It supports iteration, coordinate-based access, and
/// mesh operations like slicing and flattening.
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
        let s = shape.get_inner();
        let region = Region::new(s.labels().to_vec(), s.slice().clone());
        Self::build(region, values.extract()?)
    }

    /// Return number of ranks (Python: len(vm))
    fn __len__(&self) -> usize {
        self.inner.region().num_ranks()
    }

    /// Return the values in region/iteration order as a Python list.
    fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Clone the inner Py objects into a Python list (just bumps
        // refcounts).
        let vec: Vec<Py<PyAny>> = self.inner.values().collect();
        Ok(PyList::new(py, vec)?.into())
    }

    #[getter]
    fn shape(&self) -> PyShape {
        PyShape::from(ndslice::Shape::from(self.inner.region().clone()))
    }

    #[getter]
    fn _ndslice(&self) -> PySlice {
        self.mesh_ndslice()
    }

    #[getter]
    fn _labels(&self) -> Vec<String> {
        self.mesh_labels()
    }

    #[getter]
    fn extent(&self) -> PyExtent {
        self.inner.region().extent().into()
    }

    #[pyo3(signature = (**kwargs))]
    fn item(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Py<PyAny>> {
        let labels = self.inner.region().labels();
        let num_elements = self.inner.region().num_ranks();

        // If no kwargs provided, the mesh must have exactly one element
        let kwargs = match kwargs {
            Some(k) if !k.is_empty() => k,
            _ => {
                // No coordinates provided - mesh must be singleton
                if num_elements != 1 {
                    return Err(PyValueError::new_err(format!(
                        "item() with no arguments requires a mesh with exactly 1 element, got {}",
                        num_elements
                    )));
                }
                return Ok(Ranked::get(&self.inner, 0).unwrap().clone_ref(py));
            }
        };

        if kwargs.len() != labels.len() {
            return Err(PyKeyError::new_err(
                "item() requires exactly one coordinate per dimension",
            ));
        }

        let coordinates = labels
            .iter()
            .map(|label| {
                kwargs
                    .get_item(label)?
                    .ok_or_else(|| PyKeyError::new_err(format!("Missing dimension '{}'", label)))?
                    .extract()
            })
            .collect::<PyResult<Vec<usize>>>()?;

        let slice = self.inner.region().slice();
        let global_rank = slice
            .location(&coordinates)
            .map_err(|e| PyIndexError::new_err(e.to_string()))?;
        let local_idx = slice
            .index(global_rank)
            .map_err(|e| PyIndexError::new_err(e.to_string()))?;

        Ok(Ranked::get(&self.inner, local_idx).unwrap().clone_ref(py))
    }

    fn items(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(PyList::new(py, self.collect_items())?.into())
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.items(py)?.call_method0(py, "__iter__")
    }

    #[pyo3(signature = (**kwargs))]
    fn slice(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        self.mesh_slice(py, kwargs)
    }

    fn flatten(&self, py: Python<'_>, name: String) -> PyResult<Self> {
        self.mesh_flatten(py, name)
    }

    #[pyo3(signature = (**kwargs))]
    fn split(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        self.mesh_split(py, kwargs)
    }

    #[pyo3(signature = (**kwargs))]
    fn rename(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        self.mesh_rename(py, kwargs)
    }

    #[pyo3(signature = (dim=None))]
    fn size(&self, dim: Option<&Bound<'_, PyAny>>) -> PyResult<usize> {
        self.mesh_size(dim)
    }

    #[getter]
    fn sizes(&self) -> HashMap<String, usize> {
        self.mesh_sizes()
    }

    fn _new_with_shape(&self, py: Python<'_>, shape: &PyShape) -> PyResult<Self> {
        self.new_with_shape(py, shape.get_inner().clone())
    }

    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (PyShape, Py<PyList>))> {
        let this = slf.borrow();
        let shape = PyShape::from(ndslice::Shape::from(this.inner.region().clone()));
        let values: Vec<Py<PyAny>> = this.inner.values().collect();
        let values_list = PyList::new(py, values)?;
        Ok((slf.get_type().into_any(), (shape, values_list.unbind())))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let extent = self.inner.region().extent();
        let items = self
            .items(py)?
            .bind(py)
            .downcast::<PyList>()?
            .iter()
            .map(|item| -> PyResult<String> {
                let tuple = item.downcast::<PyTuple>()?;
                Ok(format!(
                    "  ({}, {}),",
                    tuple.get_item(0)?.repr()?,
                    tuple.get_item(1)?.repr()?
                ))
            })
            .collect::<PyResult<Vec<_>>>()?
            .join("\n");

        Ok(format!("ValueMesh({}):\n(\n{}\n)", extent, items))
    }
}

impl PyValueMesh {
    fn build(region: Region, values: Vec<Py<PyAny>>) -> PyResult<Self> {
        let mut inner =
            <ValueMesh<Py<PyAny>> as BuildFromRegion<Py<PyAny>>>::build_dense(region, values)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        inner.compress_adjacent_in_place_by(|a, b| a.as_ptr() == b.as_ptr());
        Ok(Self { inner })
    }

    fn collect_items(&self) -> Vec<(PyPoint, Py<PyAny>)> {
        let extent = PyExtent::from(self.inner.region().extent());
        let len = self.inner.region().slice().len();
        (0..len)
            .map(|i| {
                (
                    PyPoint::new(i, extent.clone()),
                    Ranked::get(&self.inner, i).unwrap().clone(),
                )
            })
            .collect()
    }
}

impl PyMeshTrait for PyValueMesh {
    fn mesh_shape(&self) -> ndslice::Shape {
        ndslice::Shape::from(self.inner.region().clone())
    }

    fn new_with_shape(&self, py: Python<'_>, shape: ndslice::Shape) -> PyResult<Self> {
        let cur_ranks: Vec<usize> = self.inner.region().slice().iter().collect();
        let pos: HashMap<usize, usize> =
            cur_ranks.iter().enumerate().map(|(i, &g)| (g, i)).collect();
        let region = ndslice::Region::from(shape);
        let remapped = region
            .slice()
            .iter()
            .map(|g| {
                let idx = *pos.get(&g).ok_or_else(|| {
                    PyValueError::new_err(format!("rank {} not in current region", g))
                })?;
                Ok(Ranked::get(&self.inner, idx).unwrap().clone_ref(py))
            })
            .collect::<PyResult<Vec<_>>>()?;
        Self::build(region, remapped)
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyValueMesh>()?;
    Ok(())
}
