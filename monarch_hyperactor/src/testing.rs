/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Minimal PyO3 struct for testing `@rust_struct` mixin patching.
//!
//! The `#[pyclass]` module is set to the Python file that defines
//! the `@rust_struct`-decorated class so the name-validation check passes.

use pyo3::prelude::*;

#[pyclass(name = "TestStruct", module = "monarch._src.actor.testing")]
pub struct PyTestStruct {
    value: i64,
}

#[pymethods]
impl PyTestStruct {
    #[new]
    fn new(value: i64) -> Self {
        Self { value }
    }

    fn rust_method(&self) -> i64 {
        self.value
    }

    fn shared_method(&self) -> String {
        "from_rust".to_string()
    }
}

#[pyfunction]
fn _make_test_struct(value: i64) -> PyTestStruct {
    PyTestStruct { value }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTestStruct>()?;
    module.add_function(wrap_pyfunction!(_make_test_struct, module)?)?;
    Ok(())
}
