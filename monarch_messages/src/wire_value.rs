/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use derive_more::From;
use derive_more::TryInto;
use enum_as_inner::EnumAsInner;
use hyperactor::Named;
use monarch_types::PickledPyObject;
use monarch_types::TryIntoPyObjectUnsafe;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBool;
use pyo3::types::PyDict;
use pyo3::types::PyFloat;
use pyo3::types::PyList;
use pyo3::types::PyNone;
use pyo3::types::PyString;
use pyo3::types::PyTuple;
use serde::Deserialize;
use serde::Serialize;
use torch_sys::Device;
use torch_sys::Layout;
use torch_sys::MemoryFormat;
use torch_sys::OpaqueIValue;
use torch_sys::ScalarType;

use crate::worker::Ref;
use crate::worker::ResolvableFunction;

/// A value used as an input to CallFunction.
// TODO, this is basically the same as RValue, but with TensorIndices swapped
// out for refs. And IValue is the same as RValue, but with real tensors and
// C++ types. I wonder if there is a nicer way to express this relationship.
// TODO extend this to support other types of values, like bytes, dicts etc.
#[derive(Serialize, Deserialize, Debug, Clone, TryInto, Named, From)]
pub enum WireValue {
    // Make sure boolean goes ealier than int as bool is a subclass of int.
    // Otherwise, bool will be converted to int.
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    Ref(Ref),
    IntList(Vec<i64>),
    RefList(Vec<Ref>),
    Device(Device),
    Layout(#[serde(with = "torch_sys::LayoutDef")] Layout),
    ScalarType(#[serde(with = "torch_sys::ScalarTypeDef")] ScalarType),
    MemoryFormat(#[serde(with = "torch_sys::MemoryFormatDef")] MemoryFormat),
    // Make this wrap the unit type, as `pyo3::FromPyObject` doesn't work with
    // empty enum variants.
    None(()),
    PyObject(PickledPyObject),
    // It is ok to just have IValue without an alias tracking cell as we just use
    // WireValue as a way to serialize and send args to workers. We dont mutate the
    // IValue and use the opaque wrapper to make accessing the IValue directly
    // an unsafe op.
    IValue(torch_sys::OpaqueIValue),
}

impl FromPyObject<'_> for WireValue {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(ref_) = Ref::from_py_object(obj) {
            Ok(WireValue::Ref(ref_))
        } else if let Ok(list) = obj.downcast::<PyList>() {
            let len = list.len();
            if len == 0 {
                // TODO: This is done for now as this seems to be the most common case for empty lists
                // in torch ops but we should use the op schema to do this correctly.
                return Ok(WireValue::IntList(vec![]));
            }

            // SAFETY: We know it is within bounds
            let item = unsafe { list.get_item_unchecked(0) };
            let len = list.len();
            if let Ok(int) = item.extract::<i64>() {
                let mut int_list = Vec::with_capacity(len);
                int_list.push(int);
                for item in list.iter().skip(1) {
                    int_list.push(item.extract::<i64>().map_err(|_| {
                        PyValueError::new_err(format!(
                            "Expected homogeneous list of ints got: {:?}",
                            list
                        ))
                    })?);
                }
                return Ok(WireValue::IntList(int_list));
            }
            if let Ok(ref_) = Ref::from_py_object(&item) {
                let mut ref_list = Vec::with_capacity(len);
                ref_list.push(ref_);
                for item in list.iter().skip(1) {
                    ref_list.push(Ref::from_py_object(&item).map_err(|_| {
                        PyValueError::new_err(format!(
                            "Expected homogeneous list of ints got: {:?}",
                            list
                        ))
                    })?);
                }
                return Ok(WireValue::RefList(ref_list));
            }
            Ok(WireValue::PyObject(PickledPyObject::pickle(obj)?))
        } else if obj.is_none() {
            Ok(WireValue::None(()))
        } else if let Ok(bool_) = obj.downcast::<PyBool>() {
            Ok(WireValue::Bool(bool_.is_true()))
        } else if let Ok(int) = obj.extract::<i64>() {
            Ok(WireValue::Int(int))
        } else if let Ok(double) = obj.downcast::<PyFloat>() {
            Ok(WireValue::Double(double.value()))
        } else if let Ok(string) = obj.downcast::<PyString>() {
            Ok(WireValue::String(string.to_str()?.to_string()))
        } else if let Ok(device) = obj.extract::<Device>() {
            Ok(WireValue::Device(device))
        } else if let Ok(layout) = obj.extract::<Layout>() {
            Ok(WireValue::Layout(layout))
        } else if let Ok(scalar_type) = obj.extract::<ScalarType>() {
            Ok(WireValue::ScalarType(scalar_type))
        } else if let Ok(memory_format) = obj.extract::<MemoryFormat>() {
            Ok(WireValue::MemoryFormat(memory_format))
        } else {
            Ok(WireValue::PyObject(PickledPyObject::pickle(obj)?))
        }
    }
}

impl<'py> TryIntoPyObjectUnsafe<'py, PyAny> for WireValue {
    unsafe fn try_to_object_unsafe(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            WireValue::Ref(ref_) => ref_.into_bound_py_any(py),
            WireValue::RefList(ref_list) => ref_list.clone().into_bound_py_any(py),
            WireValue::Int(int) => int.into_bound_py_any(py),
            WireValue::IntList(int_list) => int_list.clone().into_bound_py_any(py),
            WireValue::Double(double) => double.into_bound_py_any(py),
            WireValue::Bool(bool_) => bool_.into_bound_py_any(py),
            WireValue::String(string) => string.into_bound_py_any(py),
            WireValue::Device(device) => device.into_bound_py_any(py),
            WireValue::Layout(val) => val.into_bound_py_any(py),
            WireValue::ScalarType(val) => val.into_bound_py_any(py),
            WireValue::MemoryFormat(val) => val.into_bound_py_any(py),
            WireValue::None(()) => PyNone::get(py).into_bound_py_any(py),
            WireValue::PyObject(val) => val.unpickle(py),
            // SAFETY: WireValue is only used for serde between client and worker.
            // This function is used to access the args / kwargs of a function call
            // on the client side only.
            WireValue::IValue(val) => unsafe { val.try_to_object_unsafe(py) },
        }
    }
}

impl<'py> IntoPyObject<'py> for WireValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        // SAFETY: We are going to remove this safe/unsafe distinction
        unsafe { self.try_to_object_unsafe(py) }
    }
}

impl From<PyObject> for WireValue {
    fn from(obj: PyObject) -> Self {
        Python::with_gil(|py| WireValue::PyObject(PickledPyObject::pickle(obj.bind(py)).unwrap()))
    }
}

pub fn func_call_args_to_wire_values(
    _func: Option<&ResolvableFunction>,
    args: &Bound<'_, PyTuple>,
    kwargs: &Bound<'_, PyDict>,
) -> PyResult<(Vec<WireValue>, HashMap<String, WireValue>)> {
    python_func_args_to_wire_value(args, kwargs)
}

fn python_func_args_to_wire_value(
    args: &Bound<'_, PyTuple>,
    kwargs: &Bound<'_, PyDict>,
) -> PyResult<(Vec<WireValue>, HashMap<String, WireValue>)> {
    let args = args
        .iter()
        .map(|arg| Ok(WireValue::PyObject(PickledPyObject::pickle(&arg)?)))
        .collect::<PyResult<_>>()?;
    let kwargs = kwargs
        .iter()
        .map(|(k, v)| {
            Ok((
                k.extract::<String>()?,
                WireValue::PyObject(PickledPyObject::pickle(&v)?),
            ))
        })
        .collect::<Result<HashMap<_, _>, PyErr>>()?;
    Ok((args, kwargs))
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use anyhow::Result;
    use anyhow::bail;
    use paste::paste;
    use pyo3::Python;
    use pyo3::ffi::c_str;
    use pyo3::types::PyDict;
    use torch_sys::DeviceType;
    use torch_sys::ScalarType;

    use super::*;
    use crate::worker::Ref;

    const MOCK_REFERNCABLE_MODULE: &std::ffi::CStr = c_str!(
        r#"
class Referencable:
    def __init__(self, ref: int):
        self.ref = ref

    def __monarch_ref__(self):
        return self.ref
"#
    );

    fn setup() -> Result<()> {
        pyo3::prepare_freethreaded_python();
        // We need to load torch to initialize some internal structures used by
        // the FFI funcs we use to convert ivalues to/from py objects.
        Python::with_gil(|py| py.run(c_str!("import torch"), None, None))?;
        Ok(())
    }

    fn create_py_object() -> PyObject {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("foo", "bar").unwrap();
            dict.into_any().clone().unbind()
        })
    }

    macro_rules! generate_wire_value_from_py_tests {
        ($($kind:ident, $input:expr);* $(;)?) => {
            paste! {
                $(
                    #[test]
                    fn [<test_wire_value_from_py_$kind:snake:lower>]() -> Result<()> {
                            setup()?;
                            Python::with_gil(|py| {
                                let actual = $input.into_pyobject(py)?.extract::<WireValue>()?;
                                assert_matches!(actual, WireValue::$kind(_));
                                anyhow::Ok(())
                            })
                    }
                )*

                #[test]
                fn test_wire_value_from_py_none() -> Result<()> {
                    setup()?;
                    Python::with_gil(|py| {
                        let obj = PyNone::get(py).into_pyobject(py)?;
                        let actual = obj.extract::<WireValue>()?;
                        assert_matches!(actual, WireValue::None(_));
                        anyhow::Ok(())
                    })
                }

                #[test]
                fn test_wire_value_from_py_empty_list() -> Result<()> {
                    setup()?;
                    Python::with_gil(|py| {
                        let obj: PyObject = PyList::empty(py).into_any().unbind();
                        let actual = obj.extract::<WireValue>(py)?;
                        match actual {
                            WireValue::IntList(list) if list.len() == 0 => (),
                            _ => bail!("Expected empty list to be converted to empty int list"),
                        }
                        anyhow::Ok(())
                    })
                }

                #[test]
                fn test_wire_value_from_py_referencable_class() -> Result<()> {
                    setup()?;
                    Python::with_gil(|py| {
                        let referencable = PyModule::from_code(
                            py,
                            MOCK_REFERNCABLE_MODULE,
                            c_str!("referencable.py"),
                            c_str!("referencable"),
                        )?;
                        let ref_ = referencable.getattr("Referencable")?.call1((1,))?.unbind();
                        let actual = ref_.extract::<WireValue>(py)?;
                        assert_matches!(actual, WireValue::Ref(Ref { id: 1 }));
                        anyhow::Ok(())
                    })
                }

                #[test]
                fn test_wire_value_from_py_roundtrip_was_exhaustive() {
                    let val = WireValue::Int(0);
                    match val {
                        $(WireValue::$kind(_) => (),)*
                        WireValue::None(_) => (),
                        // Can't test from py here as PyObject behaves as catch all for conversion from PY.
                        // We will manually convert torch ops args to IValue respecting the schema so its
                        // not super important to have this.
                        WireValue::IValue(_) => (),
                    }
                }
            }
        }
    }

    // Generate exhaustive roundtrip tests for all IValue kind.
    // If you got a "non-exhaustive patterns" error here, you need to add a new
    // test entry for your IValue kind!
    generate_wire_value_from_py_tests! {
        Bool, false;
        Double, 1.23f64;
        Int, 123i64;
        IntList, vec![1i64];
        Ref, Ref::from(1);
        RefList, vec![Ref::from(1), Ref::from(2)];
        String, "foobar".to_owned();
        Device, Device::new(DeviceType::CPU);
        Layout, Layout(2);
        ScalarType, ScalarType(3);
        MemoryFormat, MemoryFormat(1);
        PyObject, create_py_object();
    }
}
