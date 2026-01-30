/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use hyperactor_mesh::v1::ValueMesh;
use ndslice::Extent;
use ndslice::Region;
use ndslice::view::BuildFromRegion;
use ndslice::view::Ranked;
use ndslice::view::ViewExt;
use pyo3::exceptions::PyIndexError;
use pyo3::exceptions::PyKeyError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyBytes;
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
    /// The underlying value storage (includes region via Ranked trait)
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
        >>::build_indexed(region.clone(), pairs)
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

    /// The shape of this ValueMesh.
    #[getter]
    fn shape(&self) -> PyShape {
        PyShape::from(ndslice::Shape::from(self.inner.region().clone()))
    }

    /// The ndslice (for MeshTrait compatibility).
    #[getter]
    fn _ndslice(&self) -> PySlice {
        self.mesh_ndslice()
    }

    /// The dimension labels (for MeshTrait compatibility).
    #[getter]
    fn _labels(&self) -> Vec<String> {
        self.mesh_labels()
    }

    /// The extent of this ValueMesh.
    #[getter]
    fn extent(&self) -> PyExtent {
        self.inner.region().extent().into()
    }

    // ========== Coordinate-based access ==========

    /// Get the value at the given coordinates.
    ///
    /// If the mesh has exactly one element, kwargs can be omitted.
    /// Otherwise, kwargs must specify all dimensions.
    ///
    /// Args:
    ///     kwargs: Coordinates to get the value at.
    ///
    /// Returns:
    ///     Value at the given coordinate.
    ///
    /// Raises:
    ///     KeyError: If invalid coordinates are provided.
    #[pyo3(signature = (**kwargs))]
    fn item(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<PyObject> {
        let labels = self.inner.region().labels();
        let n = self.inner.region().slice().len();

        // If no kwargs provided, check if mesh has exactly one element
        if kwargs.is_none() || kwargs.map(|k| k.is_empty()).unwrap_or(true) {
            if n == 1 {
                // Singleton mesh - just return the single value
                let v: Py<PyAny> = Ranked::get(&self.inner, 0).unwrap().clone_ref(py);
                return Ok(v);
            } else {
                return Err(PyKeyError::new_err(
                    "item() requires keyword arguments for each dimension",
                ));
            }
        }

        let kwargs = kwargs.unwrap();

        // Build coordinates from kwargs
        let mut coordinates: Vec<usize> = Vec::with_capacity(labels.len());
        let mut kwargs_map: HashMap<String, usize> = HashMap::new();
        for (key, value) in kwargs.iter() {
            let key_str: String = key.extract()?;
            let idx: usize = value.extract()?;
            kwargs_map.insert(key_str, idx);
        }

        for label in labels {
            let coord = kwargs_map.remove(label).ok_or_else(|| {
                PyKeyError::new_err(format!("Missing coordinate for dimension '{}'", label))
            })?;
            coordinates.push(coord);
        }

        if !kwargs_map.is_empty() {
            let extra: Vec<_> = kwargs_map.keys().collect();
            return Err(PyKeyError::new_err(format!(
                "item has extra dimensions: {:?}",
                extra
            )));
        }

        // Get global rank from coordinates
        let global_rank = self
            .inner
            .region()
            .slice()
            .location(&coordinates)
            .map_err(|e| PyIndexError::new_err(e.to_string()))?;

        // Map global rank to local index
        let ranks: Vec<usize> = self.inner.region().slice().iter().collect();
        let local_idx = ranks
            .iter()
            .position(|&r| r == global_rank)
            .ok_or_else(|| {
                PyIndexError::new_err(format!("rank {} not in current shape", global_rank))
            })?;

        let v: Py<PyAny> = Ranked::get(&self.inner, local_idx).unwrap().clone_ref(py);
        Ok(v)
    }

    /// Return all (Point, value) pairs as a list.
    ///
    /// Returns:
    ///     List of (Point, value) tuples for all elements in the mesh.
    fn items(&self, py: Python<'_>) -> PyResult<PyObject> {
        let extent = self.inner.region().extent();
        let mut result: Vec<(PyPoint, Py<PyAny>)> =
            Vec::with_capacity(self.inner.region().slice().len());

        for (i, _global_rank) in self.inner.region().slice().iter().enumerate() {
            result.push((
                PyPoint::new(i, PyExtent::from(extent.clone())),
                Ranked::get(&self.inner, i).unwrap().clone_ref(py),
            ));
        }

        Ok(PyList::new(py, result)?.into())
    }

    /// Make ValueMesh iterable (iterates over (Point, value) pairs).
    fn __iter__(&self, py: Python<'_>) -> PyResult<PyObject> {
        let items = self.items(py)?;
        items.call_method0(py, "__iter__")
    }

    // ========== MeshTrait operations ==========

    /// Select along named dimensions.
    ///
    /// Integer values remove dimensions, slice objects keep dimensions but restrict them.
    ///
    /// Examples: value_mesh.slice(batch=3, gpu=slice(2, 6))
    #[pyo3(signature = (**kwargs))]
    fn slice(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        self.mesh_slice(py, kwargs)
    }

    /// Flatten all dimensions into a single dimension with the given name.
    ///
    /// Currently only supports dense meshes (contiguous ranks).
    fn flatten(&self, py: Python<'_>, name: String) -> PyResult<Self> {
        self.mesh_flatten(py, name)
    }

    /// Split dimensions of this mesh into multiple sub-dimensions.
    ///
    /// For instance, this call splits the host dimension into dp and pp dimensions,
    /// with the size of "pp" specified and "dp" derived from it:
    ///
    /// ```python
    /// new_mesh = mesh.split(host=("dp", "pp"), gpu=("tp","cp"), pp=16, cp=2)
    /// ```
    ///
    /// Dimensions not specified will remain unchanged.
    #[pyo3(signature = (**kwargs))]
    fn split(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        self.mesh_split(py, kwargs)
    }

    /// Rename dimensions of this mesh.
    ///
    /// Returns a new mesh with some dimensions renamed.
    /// Dimensions not mentioned are retained:
    ///
    /// ```python
    /// new_mesh = mesh.rename(host="dp", gpu="tp")
    /// ```
    #[pyo3(signature = (**kwargs))]
    fn rename(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        self.mesh_rename(py, kwargs)
    }

    /// Return the size of the specified dimension(s).
    ///
    /// Args:
    ///     dim: Optional dimension name(s):
    ///         - None: Return total size (product of all dimensions)
    ///         - Single string: Return size of that dimension
    ///         - Sequence of strings: Return product of those dimensions
    ///
    /// Returns:
    ///     Number of elements in the specified dimension(s).
    #[pyo3(signature = (dim=None))]
    fn size(&self, dim: Option<&Bound<'_, PyAny>>) -> PyResult<usize> {
        self.mesh_size(dim)
    }

    /// Dictionary mapping dimension labels to their sizes.
    #[getter]
    fn sizes(&self) -> HashMap<String, usize> {
        self.mesh_sizes()
    }

    /// Create a new ValueMesh with a different shape.
    ///
    /// The new shape must be a subspace of the current shape.
    fn _new_with_shape(&self, py: Python<'_>, shape: &PyShape) -> PyResult<Self> {
        self.new_with_shape(py, shape.get_inner().clone())
    }

    /// Pickle support via __reduce__ protocol.
    ///
    /// Serialization strategy:
    /// - Region is serialized with bincode (Rust-native, fast, compact)
    /// - Values are serialized with Python pickle (required for arbitrary Python objects)
    /// - Both are combined into a single bytes object
    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyBytes>,))> {
        let py = slf.py();
        let this = slf.borrow();

        let region_bytes = bincode::serialize(this.inner.region())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let values: Vec<Py<PyAny>> = this.inner.values().collect();

        let pickle = py.import("pickle")?;
        let values_list = PyList::new(py, values)?;
        let values_bytes = pickle.call_method1("dumps", (values_list,))?;

        let combined = (region_bytes, values_bytes.extract::<Vec<u8>>()?);
        let combined_bytes =
            bincode::serialize(&combined).map_err(|e| PyValueError::new_err(e.to_string()))?;

        let py_bytes = PyBytes::new(py, &combined_bytes);
        Ok((slf.getattr("from_bytes")?, (py_bytes,)))
    }

    /// Internal method used by __reduce__ for pickle support.
    ///
    /// This is not part of the public API (intentionally omitted from .pyi stub).
    /// Users should use standard pickle.loads() to deserialize ValueMesh objects.
    #[staticmethod]
    fn from_bytes(py: Python<'_>, bytes: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let combined: (Vec<u8>, Vec<u8>) = bincode::deserialize(bytes.as_bytes())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let region: Region =
            bincode::deserialize(&combined.0).map_err(|e| PyValueError::new_err(e.to_string()))?;

        let pickle = py.import("pickle")?;
        let values_list = pickle.call_method1("loads", (PyBytes::new(py, &combined.1),))?;
        let values: Vec<Py<PyAny>> = values_list.extract()?;

        let mut inner = <ValueMesh<Py<PyAny>> as BuildFromRegion<Py<PyAny>>>::build_dense(
            region.clone(),
            values,
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        inner.compress_adjacent_in_place_by(|a, b| a.as_ptr() == b.as_ptr());

        Ok(Self { inner })
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let extent = self.inner.region().extent();
        let items = self.items(py)?;
        let items_list = items.bind(py).downcast::<PyList>()?;

        let mut items_str = String::new();
        items_str.push_str("(\n");

        for item in items_list.iter() {
            let tuple = item.downcast::<PyTuple>()?;
            let point = tuple.get_item(0)?;
            let value = tuple.get_item(1)?;
            items_str.push_str(&format!("  ({}, {}),\n", point.repr()?, value.repr()?));
        }

        items_str.push(')');
        Ok(format!("ValueMesh({}):\n{}", extent, items_str))
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(other_vm) = other.extract::<PyRef<PyValueMesh>>() {
            if self.inner.region() != other_vm.inner.region() {
                return Ok(false);
            }

            let self_values: Vec<_> = self.inner.values().collect();
            let other_values: Vec<_> = other_vm.inner.values().collect();

            if self_values.len() != other_values.len() {
                return Ok(false);
            }

            for (a, b) in self_values.iter().zip(other_values.iter()) {
                let eq: bool = a.bind(py).eq(b.bind(py))?;
                if !eq {
                    return Ok(false);
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl PyValueMesh {
    /// Remap values to a new region (internal implementation).
    ///
    /// This is the core logic for `new_with_shape` - it maps global ranks
    /// from the new region back to local indices in the current region.
    fn remap_to_region(&self, py: Python<'_>, new_region: Region) -> PyResult<Self> {
        let cur_ranks: Vec<usize> = self.inner.region().slice().iter().collect();
        let pos: HashMap<usize, usize> =
            cur_ranks.iter().enumerate().map(|(i, &g)| (g, i)).collect();

        let mut remapped: Vec<Py<PyAny>> = Vec::with_capacity(new_region.slice().len());
        for global_rank in new_region.slice().iter() {
            let local_idx = pos.get(&global_rank).ok_or_else(|| {
                PyValueError::new_err(format!("rank {} not in current region", global_rank))
            })?;
            let value = Ranked::get(&self.inner, *local_idx).unwrap().clone_ref(py);
            remapped.push(value);
        }

        let mut inner = <ValueMesh<Py<PyAny>> as BuildFromRegion<Py<PyAny>>>::build_dense(
            new_region.clone(),
            remapped,
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        inner.compress_adjacent_in_place_by(|a, b| a.as_ptr() == b.as_ptr());

        Ok(Self { inner })
    }

    /// Create a ValueMesh from an extent and a pre-populated Vec of values.
    pub fn build_dense_from_extent(extent: &Extent, values: Vec<Py<PyAny>>) -> PyResult<Self> {
        let mut inner = <ValueMesh<Py<PyAny>> as BuildFromRegion<Py<PyAny>>>::build_dense(
            ndslice::View::region(extent),
            values,
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        inner.compress_adjacent_in_place_by(|a, b| a.as_ptr() == b.as_ptr());

        Ok(Self { inner })
    }
}

impl PyMeshTrait for PyValueMesh {
    fn mesh_shape(&self) -> ndslice::Shape {
        ndslice::Shape::from(self.inner.region().clone())
    }

    fn new_with_shape(&self, py: Python<'_>, shape: ndslice::Shape) -> PyResult<Self> {
        let region = ndslice::Region::from(shape);
        self.remap_to_region(py, region)
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyValueMesh>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use ndslice::Shape;
    use ndslice::Slice;
    use ndslice::shape;
    use pyo3::prelude::*;

    use super::*;

    /// Helper to create a PyValueMesh with the given shape and string values.
    fn make_value_mesh(py: Python<'_>, shape: Shape, values: Vec<&str>) -> PyResult<PyValueMesh> {
        let py_values: Vec<Py<PyAny>> = values
            .into_iter()
            .map(|s| s.into_pyobject(py).unwrap().into_any().unbind())
            .collect();

        let region = Region::new(shape.labels().to_vec(), shape.slice().clone());
        let inner =
            <ValueMesh<Py<PyAny>> as BuildFromRegion<Py<PyAny>>>::build_dense(region, py_values)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Ok(PyValueMesh { inner })
    }

    /// Extract values from a PyValueMesh as strings.
    fn extract_values(py: Python<'_>, vm: &PyValueMesh) -> Vec<String> {
        vm.inner
            .values()
            .map(|v| v.extract::<String>(py).unwrap())
            .collect()
    }

    #[test]
    fn test_new_with_shape_identity() {
        // When the target shape has the same global ranks, values stay in order.
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let shape = shape!(rank = 4);
            let vm = make_value_mesh(py, shape.clone(), vec!["a", "b", "c", "d"]).unwrap();

            // Same shape → same values
            let vm2 = vm.new_with_shape(py, shape).unwrap();
            assert_eq!(extract_values(py, &vm2), vec!["a", "b", "c", "d"]);
        });
    }

    #[test]
    fn test_new_with_shape_permutation() {
        // A permutation of global ranks should permute the values accordingly.
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Test with 2D shape and select
            let shape_2d = shape!(x = 2, y = 2); // ranks: 0,1,2,3
            let vm2d = make_value_mesh(py, shape_2d.clone(), vec!["a", "b", "c", "d"]).unwrap();

            // Select x=1 → ranks [2, 3], values ["c", "d"]
            let target_x1 = shape_2d.at("x", 1).unwrap();
            let vm_x1 = vm2d.new_with_shape(py, target_x1).unwrap();
            assert_eq!(extract_values(py, &vm_x1), vec!["c", "d"]);

            // Select y=0 → ranks [0, 2], values ["a", "c"]
            let target_y0 = shape_2d.at("y", 0).unwrap();
            let vm_y0 = vm2d.new_with_shape(py, target_y0).unwrap();
            assert_eq!(extract_values(py, &vm_y0), vec!["a", "c"]);
        });
    }

    #[test]
    fn test_new_with_shape_noncontiguous_ranks() {
        // Non-contiguous ranks (with offset/stride) remap correctly.
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Create a 2D shape: host=2, gpu=4 → 8 ranks (0..7)
            let shape = shape!(host = 2, gpu = 4);
            let vm = make_value_mesh(
                py,
                shape.clone(),
                vec![
                    "h0g0", "h0g1", "h0g2", "h0g3", "h1g0", "h1g1", "h1g2", "h1g3",
                ],
            )
            .unwrap();

            // Select gpu=1..3 → keeps both hosts, gpus 1 and 2
            // Ranks: host=0,gpu=1 → 1; host=0,gpu=2 → 2; host=1,gpu=1 → 5; host=1,gpu=2 → 6
            let target = shape.select("gpu", 1..3).unwrap();
            let vm2 = vm.new_with_shape(py, target).unwrap();
            assert_eq!(
                extract_values(py, &vm2),
                vec!["h0g1", "h0g2", "h1g1", "h1g2"]
            );

            // Select host=1 → ranks 4,5,6,7 → values h1g0..h1g3
            let target_h1 = shape.at("host", 1).unwrap();
            let vm_h1 = vm.new_with_shape(py, target_h1).unwrap();
            assert_eq!(
                extract_values(py, &vm_h1),
                vec!["h1g0", "h1g1", "h1g2", "h1g3"]
            );
        });
    }

    #[test]
    fn test_new_with_shape_missing_rank_error() {
        // If the target shape requests a rank not in the current shape,
        // new_with_shape should return an error.
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Create a shape with only ranks 0,1,2,3
            let shape = shape!(rank = 4);
            let vm = make_value_mesh(py, shape, vec!["a", "b", "c", "d"]).unwrap();

            // Create a target shape that includes rank 10 (not in vm)
            // We do this by creating a slice with offset 10
            let target_slice = Slice::new(10, vec![2], vec![1]).unwrap();
            let target = Shape::new(vec!["rank".to_string()], target_slice).unwrap();

            let result = vm.new_with_shape(py, target);
            match result {
                Err(e) => {
                    let err_msg = e.to_string();
                    assert!(
                        err_msg.contains("not in current region"),
                        "Expected 'not in current region' error, got: {}",
                        err_msg
                    );
                }
                Ok(_) => panic!("Expected an error for missing rank"),
            }
        });
    }

    #[test]
    fn test_new_with_shape_with_offset() {
        // Test remapping when the original shape has a non-zero offset.
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Create a 2D shape and select a subset (which adds offset)
            let full_shape = shape!(host = 2, gpu = 4);
            let vm_full = make_value_mesh(
                py,
                full_shape.clone(),
                vec![
                    "h0g0", "h0g1", "h0g2", "h0g3", "h1g0", "h1g1", "h1g2", "h1g3",
                ],
            )
            .unwrap();

            // Select host=1 → creates shape with offset=4
            let sub_shape = full_shape.at("host", 1).unwrap();
            let vm_sub = vm_full.new_with_shape(py, sub_shape.clone()).unwrap();
            assert_eq!(
                extract_values(py, &vm_sub),
                vec!["h1g0", "h1g1", "h1g2", "h1g3"]
            );

            // Now from vm_sub, select gpu=1..3 → offset=5, ranks 5,6
            let sub_sub_shape = sub_shape.select("gpu", 1..3).unwrap();
            let vm_sub_sub = vm_sub.new_with_shape(py, sub_sub_shape).unwrap();
            assert_eq!(extract_values(py, &vm_sub_sub), vec!["h1g1", "h1g2"]);
        });
    }
}
