/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rust implementation of Python's `MeshTrait` for PyO3 mesh types.
//!
//! This module provides [`PyMeshTrait`], a trait that mirrors Python's
//! `MeshTrait` mixin class from `monarch._src.actor.shape`. It provides
//! labeled mesh operations (`slice`, `flatten`, `split`, `rename`, `size`,
//! `sizes`) consistently across different PyO3 mesh types.
//!
//! # Python MeshTrait
//!
//! In Python, `MeshTrait` is an abstract base class that provides mesh
//! operations based on an underlying `Shape` (labels + NDSlice). Any class
//! that implements `_ndslice`, `_labels`, and `_new_with_shape` automatically
//! gets all the mesh operations.
//!
//! # Rust PyMeshTrait
//!
//! This Rust trait follows the same pattern: implementors provide
//! `mesh_shape()` and `new_with_shape()`, and all other operations are
//! provided as default implementations.
//!
//! # Example
//!
//! ```ignore
//! use monarch_hyperactor::mesh_trait::PyMeshTrait;
//!
//! impl PyMeshTrait for PyValueMesh {
//!     fn mesh_shape(&self) -> Shape { Shape::from(self.region.clone()) }
//!     fn new_with_shape(&self, py: Python<'_>, shape: Shape) -> PyResult<Self> { ... }
//! }
//!
//! #[pymethods]
//! impl PyValueMesh {
//!     #[pyo3(signature = (**kwargs))]
//!     fn slice(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
//!         self.mesh_slice(py, kwargs)
//!     }
//!     fn flatten(&self, py: Python<'_>, name: String) -> PyResult<Self> {
//!         self.mesh_flatten(py, name)
//!     }
//!     // ... etc
//! }
//! ```

use std::collections::HashMap;

use ndslice::Shape;
use ndslice::Slice;
use ndslice::shape::Range;
use pyo3::exceptions::PyIndexError;
use pyo3::exceptions::PyKeyError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PySlice as PySliceType;
use pyo3::types::PyString;
use pyo3::types::PyTuple;

use crate::ndslice::PySlice;

/// Trait for PyO3 mesh types that support labeled mesh operations.
///
/// This is the Rust equivalent of Python's `MeshTrait` abstract base class.
/// Implementors must provide [`mesh_shape()`](Self::mesh_shape) and
/// [`new_with_shape()`](Self::new_with_shape). All other mesh operations
/// are provided as default implementations:
///
/// - [`mesh_ndslice`](Self::mesh_ndslice) - Get the underlying ndslice
/// - [`mesh_labels`](Self::mesh_labels) - Get the dimension labels
/// - [`mesh_slice`](Self::mesh_slice) - Select along named dimensions
/// - [`mesh_flatten`](Self::mesh_flatten) - Flatten all dimensions into one
/// - [`mesh_split`](Self::mesh_split) - Split dimensions into sub-dimensions
/// - [`mesh_rename`](Self::mesh_rename) - Rename dimensions
/// - [`mesh_size`](Self::mesh_size) - Get size of dimension(s)
/// - [`mesh_sizes`](Self::mesh_sizes) - Get all dimension sizes as a map
///
/// # Guarantees
///
/// The trait guarantees that calls to `new_with_shape` will only ever use
/// a shape that is a subspace of the current shape (i.e., the new shape's
/// ranks are a subset of the current shape's ranks).
pub trait PyMeshTrait: Sized {
    /// Returns the mesh's shape (labels + NDSlice).
    fn mesh_shape(&self) -> Shape;

    /// Creates a new mesh with the given shape.
    ///
    /// The new shape must be a subspace of the current shape - that is,
    /// all ranks in the new shape must exist in the current shape.
    fn new_with_shape(&self, py: Python<'_>, shape: Shape) -> PyResult<Self>;

    /// Returns the ndslice (for MeshTrait compatibility).
    fn mesh_ndslice(&self) -> PySlice {
        self.mesh_shape().slice().clone().into()
    }

    /// Returns the dimension labels (for MeshTrait compatibility).
    fn mesh_labels(&self) -> Vec<String> {
        self.mesh_shape().labels().to_vec()
    }

    /// Select along named dimensions.
    ///
    /// Integer values remove dimensions (like indexing), slice objects keep
    /// dimensions but restrict them to a range.
    ///
    /// # Examples
    ///
    /// ```python
    /// # Select batch=3 (removes batch dimension), gpu=2:6 (keeps gpu, restricts range)
    /// new_mesh = mesh.slice(batch=3, gpu=slice(2, 6))
    /// ```
    fn mesh_slice(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let kwargs = match kwargs {
            Some(k) if !k.is_empty() => k,
            _ => return self.new_with_shape(py, self.mesh_shape()),
        };
        self.new_with_shape(py, shape_slice(self.mesh_shape(), kwargs)?)
    }

    /// Returns a new mesh with all dimensions flattened into a single dimension.
    ///
    /// Currently this supports only dense meshes: that is, all ranks must be
    /// contiguous in the mesh (strides must be row-major).
    ///
    /// # Arguments
    ///
    /// * `name` - The name for the single flattened dimension
    ///
    /// # Errors
    ///
    /// Returns an error if the mesh is not dense (has non-contiguous strides).
    fn mesh_flatten(&self, py: Python<'_>, name: String) -> PyResult<Self> {
        let new_shape = self
            .mesh_shape()
            .flatten(&name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        self.new_with_shape(py, new_shape)
    }

    /// Returns a new mesh with some dimensions split into sub-dimensions.
    ///
    /// For instance, this call splits the host dimension into dp and pp dimensions.
    /// The size of "pp" is specified and "dp" is derived from it:
    ///
    /// ```python
    /// new_mesh = mesh.split(host=("dp", "pp"), gpu=("tp","cp"), pp=16, cp=2)
    /// ```
    ///
    /// Dimensions not specified will remain unchanged.
    ///
    /// # Arguments
    ///
    /// Keys matching existing dimensions map to tuples of new dimension names.
    /// Keys not matching existing dimensions are size constraints (integers).
    /// At most one new dimension per split can have its size inferred.
    fn mesh_split(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let kwargs = match kwargs {
            Some(k) if !k.is_empty() => k,
            _ => return self.new_with_shape(py, self.mesh_shape()),
        };

        let mut splits: HashMap<String, Vec<String>> = HashMap::new();
        let mut size_constraints: HashMap<String, usize> = HashMap::new();

        let shape = self.mesh_shape();

        for (key, value) in kwargs.iter() {
            let key_str: String = key.extract()?;

            if shape.labels().contains(&key_str) {
                if value.is_instance_of::<PyString>() {
                    let s: String = value.extract()?;
                    return Err(PyValueError::new_err(format!(
                        "expected a sequence of dimensions, but got '{}'",
                        s
                    )));
                }
                let new_names: Vec<String> = value
                    .try_iter()?
                    .map(|item| item?.extract::<String>())
                    .collect::<PyResult<Vec<_>>>()?;

                if new_names.is_empty() {
                    return Err(PyValueError::new_err(format!(
                        "split for dimension '{}' must have at least one target name",
                        key_str
                    )));
                }
                splits.insert(key_str, new_names);
            } else {
                // This is a size constraint
                let size: usize = value.extract().map_err(|_| {
                    PyValueError::new_err(format!(
                        "'{}' is not an existing dim. Expected an integer size constraint on a new dim.",
                        key_str
                    ))
                })?;
                size_constraints.insert(key_str, size);
            }
        }

        let mut names: Vec<String> = Vec::new();
        let mut sizes: Vec<usize> = Vec::new();
        let mut strides: Vec<usize> = Vec::new();
        let ndslice = shape.slice();

        for (name, &size, &stride) in shape
            .labels()
            .iter()
            .zip(ndslice.sizes().iter())
            .zip(ndslice.strides().iter())
            .map(|((n, s), st)| (n, s, st))
        {
            let to_names = splits
                .get(name)
                .cloned()
                .unwrap_or_else(|| vec![name.clone()]);
            let mut total_size = 1usize;
            let mut unknown_size_name: Option<String> = None;

            for to_name in &to_names {
                if let Some(&constraint_size) = size_constraints.get(to_name) {
                    total_size *= constraint_size;
                } else if unknown_size_name.is_none() {
                    unknown_size_name = Some(to_name.clone());
                } else {
                    return Err(PyValueError::new_err(format!(
                        "Cannot infer size of {:?} because both '{}' and '{}' have unknown size. \
                         Specify at least one as argument, e.g. {}=4",
                        to_names,
                        to_name,
                        unknown_size_name.as_ref().unwrap(),
                        to_name
                    )));
                }
            }

            if let Some(ref unknown_name) = unknown_size_name {
                let (inferred_size, m) = (size / total_size, size % total_size);
                if m != 0 {
                    let to_sizes: Vec<String> = to_names
                        .iter()
                        .map(|n| {
                            size_constraints
                                .get(n)
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| "?".to_string())
                        })
                        .collect();
                    return Err(PyValueError::new_err(format!(
                        "Dimension '{}' of size {} is not evenly divided by {:?} with sizes {:?}",
                        name, size, to_names, to_sizes
                    )));
                }
                size_constraints.insert(unknown_name.clone(), inferred_size);
            } else if total_size != size {
                let to_sizes: Vec<usize> = to_names
                    .iter()
                    .map(|n| *size_constraints.get(n).unwrap())
                    .collect();
                return Err(PyValueError::new_err(format!(
                    "Dimension '{}' of size {} is not evenly divided by {:?} with sizes {:?}",
                    name, size, to_names, to_sizes
                )));
            }

            let new_sizes: Vec<usize> = to_names
                .iter()
                .map(|n| {
                    size_constraints.remove(n).ok_or_else(|| {
                        PyValueError::new_err(format!("Missing size for dimension '{}'", n))
                    })
                })
                .collect::<PyResult<Vec<_>>>()?;

            // Calculate strides for split dimensions (row-major within the split).
            // Strides are computed from right to left, starting with the original stride.
            let new_strides: Vec<usize> = new_sizes
                .iter()
                .rev()
                .scan(stride, |acc, &sz| {
                    let current = *acc;
                    *acc *= sz;
                    Some(current)
                })
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();

            sizes.extend(new_sizes);
            strides.extend(new_strides);

            for to_name in &to_names {
                if names.contains(to_name) {
                    return Err(PyValueError::new_err(format!(
                        "Duplicate dimension name '{}'",
                        to_name
                    )));
                }
            }
            names.extend(to_names);
        }

        if !size_constraints.is_empty() {
            let unused: Vec<_> = size_constraints.keys().collect();
            return Err(PyValueError::new_err(format!(
                "unused size constraints: {:?}",
                unused
            )));
        }

        self.new_with_shape(
            py,
            Shape::new(
                names,
                Slice::new(ndslice.offset(), sizes, strides)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
            )
            .map_err(|e| PyValueError::new_err(e.to_string()))?,
        )
    }

    /// Returns a new mesh with some dimensions renamed.
    ///
    /// Dimensions not mentioned are retained unchanged. This is a convenience
    /// wrapper around [`mesh_split`](Self::mesh_split).
    ///
    /// # Example
    ///
    /// ```python
    /// new_mesh = mesh.rename(host="dp", gpu="tp")
    /// ```
    fn mesh_rename(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let kwargs = match kwargs {
            Some(k) if !k.is_empty() => k,
            _ => return self.new_with_shape(py, self.mesh_shape()),
        };

        let split_kwargs = PyDict::new(py);
        for (key, value) in kwargs.iter() {
            let new_name: String = value.extract()?;
            let tuple = PyTuple::new(py, [new_name])?;
            split_kwargs.set_item(key, tuple)?;
        }

        self.mesh_split(py, Some(&split_kwargs))
    }

    /// Returns the number of elements in the specified dimension(s).
    ///
    /// If `dim` is `None`, returns the total number of elements in the mesh
    /// (product of all dimension sizes).
    ///
    /// # Arguments
    ///
    /// * `dim` - Optional dimension name(s):
    ///   - `None`: Return total size (product of all dimensions)
    ///   - Single string: Return size of that dimension
    ///   - Sequence of strings: Return product of those dimensions
    ///
    /// # Errors
    ///
    /// Returns `KeyError` if a dimension name doesn't exist in the mesh.
    fn mesh_size(&self, dim: Option<&Bound<'_, PyAny>>) -> PyResult<usize> {
        let shape = self.mesh_shape();
        let labels = shape.labels();
        let sizes = shape.slice().sizes();

        match dim {
            None => Ok(sizes.iter().product()),
            Some(dim_arg) => {
                if let Ok(single_dim) = dim_arg.extract::<String>() {
                    let idx = labels
                        .iter()
                        .position(|l| l == &single_dim)
                        .ok_or_else(|| {
                            PyKeyError::new_err(format!(
                                "Shape does not have dimension '{}'",
                                single_dim
                            ))
                        })?;
                    return Ok(sizes[idx]);
                }

                let dims: Vec<String> = dim_arg.extract()?;
                let mut product = 1usize;
                for d in dims {
                    let idx = labels.iter().position(|l| l == &d).ok_or_else(|| {
                        PyKeyError::new_err(format!("Shape does not have dimension '{}'", d))
                    })?;
                    product *= sizes[idx];
                }
                Ok(product)
            }
        }
    }

    /// Returns a dictionary mapping dimension labels to their sizes.
    ///
    /// Equivalent to `dict(zip(labels, ndslice.sizes))` in Python.
    fn mesh_sizes(&self) -> HashMap<String, usize> {
        let shape = self.mesh_shape();
        shape
            .labels()
            .iter()
            .zip(shape.slice().sizes().iter())
            .map(|(label, &size)| (label.clone(), size))
            .collect()
    }
}

/// Convert a Python slice to an ndslice Range for a given dimension size.
fn py_slice_to_range(py_slice: &Bound<'_, PySliceType>, size: usize) -> PyResult<Range> {
    let indices = py_slice.indices(size as isize)?;
    Ok(Range(
        indices.start as usize,
        Some(indices.stop as usize),
        indices.step as usize,
    ))
}

/// Apply slice operations to a Shape based on Python kwargs.
///
/// This is the Rust equivalent of Python's `ShapeExt.slice(shape, **kwargs)`.
/// Integer values remove dimensions (like `at`), slice objects keep dimensions
/// but restrict them (like `select`).
fn shape_slice(shape: Shape, kwargs: &Bound<'_, PyDict>) -> PyResult<Shape> {
    let mut shape = shape;

    for (key, value) in kwargs.iter() {
        let label: String = key.extract()?;

        let dim = shape.dim(&label).map_err(|_| {
            PyKeyError::new_err(format!("Shape does not have dimension labeled '{}'", label))
        })?;
        let size = shape.slice().sizes()[dim];

        if let Ok(py_slice) = value.downcast::<PySliceType>() {
            let range = py_slice_to_range(py_slice, size)?;
            shape = shape
                .select(&label, range)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        } else {
            let index: usize = value.extract()?;
            if index >= size {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for dimension '{}' of size {}",
                    index, label, size
                )));
            }
            shape = shape
                .at(&label, index)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        }
    }

    Ok(shape)
}

#[cfg(test)]
mod tests {
    use ndslice::shape;

    use super::*;

    #[derive(Debug)]
    struct TestMesh {
        shape: Shape,
    }

    impl PyMeshTrait for TestMesh {
        fn mesh_shape(&self) -> Shape {
            self.shape.clone()
        }

        fn new_with_shape(&self, _py: Python<'_>, shape: Shape) -> PyResult<Self> {
            Ok(TestMesh { shape })
        }
    }

    #[test]
    fn test_slice_no_kwargs() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let result = mesh.mesh_slice(py, None).unwrap();
            assert_eq!(result.shape, mesh.shape);
        });
    }

    #[test]
    fn test_slice_with_integer() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let kwargs = PyDict::new(py);
            kwargs.set_item("host", 1).unwrap();

            let result = mesh.mesh_slice(py, Some(&kwargs)).unwrap();
            // host dimension removed, only gpu remains
            assert_eq!(result.shape.labels(), &["gpu"]);
            assert_eq!(result.shape.slice().sizes(), &[8]);
            // Offset should be 8 (host=1 * gpu_size=8)
            assert_eq!(result.shape.slice().offset(), 8);
        });
    }

    #[test]
    fn test_slice_with_range() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let kwargs = PyDict::new(py);
            // gpu=slice(2, 6) - select gpus 2,3,4,5
            let py_slice = pyo3::types::PySlice::new(py, 2, 6, 1);
            kwargs.set_item("gpu", py_slice).unwrap();

            let result = mesh.mesh_slice(py, Some(&kwargs)).unwrap();
            // Both dimensions remain
            assert_eq!(result.shape.labels(), &["host", "gpu"]);
            // gpu restricted to 4 elements
            assert_eq!(result.shape.slice().sizes(), &[2, 4]);
        });
    }

    #[test]
    fn test_slice_invalid_dimension() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let kwargs = PyDict::new(py);
            kwargs.set_item("nonexistent", 0).unwrap();

            let result = mesh.mesh_slice(py, Some(&kwargs));
            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("nonexistent"));
        });
    }

    #[test]
    fn test_flatten() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let result = mesh.mesh_flatten(py, "rank".to_string()).unwrap();
            assert_eq!(result.shape.labels(), &["rank"]);
            assert_eq!(result.shape.slice().sizes(), &[16]);
        });
    }

    #[test]
    fn test_split_basic() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 4, gpu = 8),
            };
            // Split host into (dp, pp) with pp=2, so dp=2
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("host", PyTuple::new(py, ["dp", "pp"]).unwrap())
                .unwrap();
            kwargs.set_item("pp", 2).unwrap();

            let result = mesh.mesh_split(py, Some(&kwargs)).unwrap();
            assert_eq!(result.shape.labels(), &["dp", "pp", "gpu"]);
            assert_eq!(result.shape.slice().sizes(), &[2, 2, 8]);
        });
    }

    #[test]
    fn test_split_preserves_strides() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 4, gpu = 8),
            };
            // Original strides: [8, 1]
            // Split host into (dp, pp) with pp=2
            // New strides should be: dp=16, pp=8, gpu=1
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("host", PyTuple::new(py, ["dp", "pp"]).unwrap())
                .unwrap();
            kwargs.set_item("pp", 2).unwrap();

            let result = mesh.mesh_split(py, Some(&kwargs)).unwrap();
            assert_eq!(result.shape.slice().strides(), &[16, 8, 1]);
        });
    }

    #[test]
    fn test_rename_basic() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let kwargs = PyDict::new(py);
            kwargs.set_item("host", "dp").unwrap();
            kwargs.set_item("gpu", "tp").unwrap();

            let result = mesh.mesh_rename(py, Some(&kwargs)).unwrap();
            assert_eq!(result.shape.labels(), &["dp", "tp"]);
            assert_eq!(result.shape.slice().sizes(), &[2, 8]);
        });
    }

    #[test]
    fn test_size_total() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|_py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let result = mesh.mesh_size(None).unwrap();
            assert_eq!(result, 16);
        });
    }

    #[test]
    fn test_size_single_dim() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let dim = PyString::new(py, "gpu");
            let result = mesh.mesh_size(Some(&dim.into_any())).unwrap();
            assert_eq!(result, 8);
        });
    }

    #[test]
    fn test_sizes_property() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|_py| {
            let mesh = TestMesh {
                shape: shape!(host = 2, gpu = 8),
            };
            let result = mesh.mesh_sizes();
            assert_eq!(result.get("host"), Some(&2));
            assert_eq!(result.get("gpu"), Some(&8));
        });
    }
}
