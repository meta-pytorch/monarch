/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::mem::take;
use std::sync::Mutex;

use pyo3::Py;
use pyo3::PyClass;
use pyo3::PyResult;
use pyo3::Python;

/// A PyCell holds a `#[pyclass]` value constructed on the Rust heap;
/// when it is first used, it is moved to the Python heap.
pub struct PyCell<T> {
    inner: Mutex<PyCellState<T>>,
}

impl<T> fmt::Debug for PyCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyCell").finish_non_exhaustive()
    }
}

#[derive(Default)]
enum PyCellState<T> {
    #[default]
    Invalid,

    Rust(T),
    Python(Py<T>),
}

impl<T> PyCell<T>
where
    T: PyClass,
{
    /// Create a new PyCell with a Rust-owned value.
    pub fn new(value: T) -> Self {
        Self {
            inner: Mutex::new(PyCellState::Rust(value)),
        }
    }

    /// Clone the PyCell, returning a reference to the Python-owned value.
    pub fn clone_ref(&self, py: Python<'_>) -> PyResult<Py<T>>
    where
        T: Into<pyo3::PyClassInitializer<T>>,
    {
        let mut inner = self.inner.lock().unwrap();

        match take(&mut *inner) {
            PyCellState::Rust(value) => {
                let py_value = Py::new(py, value)?;
                *inner = PyCellState::Python(py_value.clone_ref(py));
                Ok(py_value)
            }
            PyCellState::Python(py_value) => {
                *inner = PyCellState::Python(py_value.clone_ref(py));
                Ok(py_value)
            }
            PyCellState::Invalid => panic!("invalid state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::*;

    #[pyclass]
    struct TestClass {
        #[allow(dead_code)]
        value: i32,
    }

    #[test]
    fn test_clone_ref() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let cell = PyCell::new(TestClass { value: 42 });

            let py_obj1 = cell.clone_ref(py).unwrap();
            let py_obj2 = cell.clone_ref(py).unwrap();

            // These are the same:
            assert!(py_obj1.is(&py_obj2));
        });
    }
}
