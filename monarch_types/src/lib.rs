/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![feature(assert_matches)]

mod pyobject;
mod python;
mod pytree;

use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
pub use pyobject::PickledPyObject;
pub use python::SerializablePyErr;
pub use python::TryIntoPyObjectUnsafe;
pub use pytree::PyTree;

/// Macro to generate a Python object lookup function with caching
///
/// # Arguments
/// * `$fn_name` - Name of the Rust function to generate
/// * `$python_path` - Path to the Python object as a string (e.g., "module.submodule.function")
#[macro_export]
macro_rules! py_global {
    ($fn_name:ident, $python_module:literal, $python_class:literal) => {
        fn $fn_name<'py>(py: ::pyo3::Python<'py>) -> ::pyo3::Bound<'py, ::pyo3::PyAny> {
            static CACHE: ::pyo3::sync::GILOnceCell<::pyo3::PyObject> =
                ::pyo3::sync::GILOnceCell::new();
            CACHE
                .import(py, $python_module, $python_class)
                .unwrap()
                .clone()
        }
    };
}

/// Macro to register a function to a Python module.
#[macro_export]
macro_rules! py_module_add_function {
    ($mod:ident, $mod_name:literal, $fn:ident) => {
        let f = pyo3::wrap_pyfunction!($fn, $mod)?;
        f.setattr("__module__", $mod_name)?;
        $mod.add_function(f)?;
    };
}

pub trait MapPyErr<T> {
    fn map_pyerr(self) -> Result<T, PyErr>;
}
impl<T, E> MapPyErr<T> for Result<T, E>
where
    E: ToString,
{
    fn map_pyerr(self) -> Result<T, PyErr> {
        self.map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
    }
}
