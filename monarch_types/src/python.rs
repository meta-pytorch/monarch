/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use pyo3::Bound;
use pyo3::IntoPyObject;
use pyo3::IntoPyObjectExt;
use pyo3::PyAny;
use pyo3::PyResult;
use pyo3::Python;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyNone;
use pyo3::types::PyTuple;
use serde::Deserialize;
use serde::Serialize;

/// A variant of `pyo3::IntoPyObject` used to wrap unsafe impls and propagates the
/// unsafety to the caller.
pub trait TryIntoPyObjectUnsafe<'py, P> {
    unsafe fn try_to_object_unsafe(self, py: Python<'py>) -> PyResult<Bound<'py, P>>;
}

/// Helper impl for casting into args for python functions calls.
impl<'a, 'py, T> TryIntoPyObjectUnsafe<'py, PyTuple> for &'a Vec<T>
where
    &'a T: TryIntoPyObjectUnsafe<'py, PyAny>,
    T: 'a,
{
    unsafe fn try_to_object_unsafe(self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(
            py,
            self.iter()
                // SAFETY: Safety requirements are propagated via the `unsafe`
                // tag on this method.
                .map(|v| unsafe { v.try_to_object_unsafe(py) })
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}

/// Helper impl for casting into kwargs for python functions calls.
impl<'a, 'py, K, V> TryIntoPyObjectUnsafe<'py, PyDict> for &'a HashMap<K, V>
where
    &'a K: IntoPyObject<'py> + std::cmp::Eq + std::hash::Hash,
    &'a V: TryIntoPyObjectUnsafe<'py, PyAny>,
    K: 'a,
    V: 'a,
{
    unsafe fn try_to_object_unsafe(self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, val) in self {
            // SAFETY: Safety requirements are propagated via the `unsafe`
            // tag on this method.
            dict.set_item(key, unsafe { val.try_to_object_unsafe(py) }?)?;
        }
        Ok(dict)
    }
}

/// A wrapper around `PyErr` that contains a serialized traceback.
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::Error)]
pub struct SerializablePyErr {
    pub message: String,
}

impl SerializablePyErr {
    pub fn from(py: Python, err: &PyErr) -> Self {
        // first construct the full traceback including any python frames that were used
        // to invoke where we currently are. This is pre-pended to the traceback of the
        // currently unwinded frames (err.traceback())
        let inspect = py.import("inspect").unwrap();
        let types = py.import("types").unwrap();
        let traceback_type = types.getattr("TracebackType").unwrap();
        let traceback = py.import("traceback").unwrap();

        let mut f = inspect
            .call_method0("currentframe")
            .unwrap_or(PyNone::get(py).to_owned().into_any());
        let mut tb: Bound<'_, PyAny> = err.traceback(py).into_bound_py_any(py).unwrap();
        while !f.is_none() {
            let lasti = f.getattr("f_lasti").unwrap();
            let lineno = f.getattr("f_lineno").unwrap();
            let back = f.getattr("f_back").unwrap();
            tb = traceback_type.call1((tb, f, lasti, lineno)).unwrap();
            f = back;
        }

        let traceback_exception = traceback.getattr("TracebackException").unwrap();

        let tb = traceback_exception
            .call1((err.get_type(py), err.value(py), tb))
            .unwrap();

        let message: String = tb
            .getattr("format")
            .unwrap()
            .call0()
            .unwrap()
            .try_iter()
            .unwrap()
            .map(|x| -> String { x.unwrap().extract().unwrap() })
            .collect::<Vec<String>>()
            .join("");

        Self { message }
    }

    pub fn from_fn<'py>(py: Python<'py>) -> impl Fn(PyErr) -> Self + 'py {
        move |err| Self::from(py, &err)
    }
}

impl std::fmt::Display for SerializablePyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl<T> From<T> for SerializablePyErr
where
    T: Into<PyErr>,
{
    fn from(value: T) -> Self {
        Python::attach(|py| SerializablePyErr::from(py, &value.into()))
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use pyo3::ffi::c_str;
    use pyo3::indoc::indoc;
    use pyo3::prelude::*;
    use timed_test::async_timed_test;

    use crate::SerializablePyErr;

    #[async_timed_test(timeout_secs = 60)]
    async fn test_serializable_py_err() {
        Python::initialize();
        let _unused = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                c_str!(indoc! {r#"
                        def func1():
                            raise Exception("test")

                        def func2():
                            func1()

                        def func3():
                            func2()
                    "#}),
                c_str!("test_helpers.py"),
                c_str!("test_helpers"),
            )?;

            let err = SerializablePyErr::from(py, &module.call_method0("func3").unwrap_err());
            assert_eq!(
                err.message.as_str(),
                indoc! {r#"
                    Traceback (most recent call last):
                      File "test_helpers.py", line 8, in func3
                      File "test_helpers.py", line 5, in func2
                      File "test_helpers.py", line 2, in func1
                    Exception: test
                "#}
            );

            PyResult::Ok(())
        });
    }
}
