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
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    TryInto,
    Named,
    From,
    EnumAsInner
)]
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

impl TryIntoPyObjectUnsafe<PyAny> for WireValue {
    unsafe fn try_to_object_unsafe(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let res = match self {
            WireValue::Ref(ref_) => ref_.into_py(py).into_bound(py),
            WireValue::RefList(ref_list) => ref_list.clone().into_py(py).into_bound(py),
            WireValue::Int(int) => int.into_py(py).into_bound(py),
            WireValue::IntList(int_list) => int_list.clone().into_py(py).into_bound(py),
            WireValue::Double(double) => double.into_py(py).into_bound(py),
            WireValue::Bool(bool_) => bool_.into_py(py).into_bound(py),
            WireValue::String(string) => string.into_py(py).into_bound(py),
            WireValue::Device(device) => device.into_py(py).into_bound(py),
            WireValue::Layout(val) => val.into_py(py).into_bound(py),
            WireValue::ScalarType(val) => val.into_py(py).into_bound(py),
            WireValue::MemoryFormat(val) => val.into_py(py).into_bound(py),
            WireValue::None(()) => PyNone::get(py).to_owned().into_any(),
            WireValue::PyObject(val) => val.unpickle(py)?,
            // SAFETY: WireValue is only used for serde between client and worker.
            // This function is used to access the args / kwargs of a function call
            // on the client side only.
            WireValue::IValue(val) => unsafe { val.try_to_object_unsafe(py)? },
        };
        Ok(res)
    }
}

impl From<PyObject> for WireValue {
    fn from(obj: PyObject) -> Self {
        Python::with_gil(|py| WireValue::PyObject(PickledPyObject::pickle(obj.bind(py)).unwrap()))
    }
}

impl WireValue {
    fn from_pyobject_with_torch_op_arg_type(
        obj: Bound<'_, PyAny>,
        type_: &torch_sys::call_op::TypePtr,
        num_elements: i32,
        allow_nums_as_tensors: bool,
    ) -> PyResult<Self> {
        if type_.is_tensor() || type_.is_optional_tensor() {
            if type_.is_optional_tensor() && obj.is_none() {
                return Ok(WireValue::None(()));
            } else if let Ok(ref_) = Ref::from_py_object(&obj) {
                return Ok(WireValue::Ref(ref_));
            }
        }
        if type_.is_tensor_list() || type_.is_optional_tensor_list() {
            if type_.is_optional_tensor_list() && obj.is_none() {
                return Ok(WireValue::None(()));
            }
            let list = obj.downcast::<PyList>()?;
            let len = list.len();
            if len == 0 {
                return Ok(WireValue::RefList(vec![]));
            }
            // SAFETY: We know it is within bounds
            let item = unsafe { list.get_item_unchecked(0) };
            if let Ok(ref_) = Ref::from_py_object(&item) {
                let mut ref_list = Vec::with_capacity(len);
                ref_list.push(ref_);
                for item in list.iter().skip(1) {
                    ref_list.push(Ref::from_py_object(&item).map_err(|_| {
                        PyValueError::new_err(format!(
                            "Expected homogeneous list of refs got: {:?}",
                            list
                        ))
                    })?);
                }
                return Ok(WireValue::RefList(ref_list));
            }
        }
        OpaqueIValue::from_py_object_with_type(obj, type_, num_elements, allow_nums_as_tensors)
            .map(WireValue::IValue)
    }
}

pub fn func_call_args_to_wire_values(
    func: Option<&ResolvableFunction>,
    args: &Bound<'_, PyTuple>,
    kwargs: &Bound<'_, PyDict>,
) -> PyResult<(Vec<WireValue>, HashMap<String, WireValue>)> {
    if let Some((op, overload)) = func.and_then(|func| func.as_torch_op()) {
        torch_op_args_to_wire_values(&op, &overload, args, kwargs)
    } else {
        python_func_args_to_wire_value(args, kwargs)
    }
}

fn torch_op_args_to_wire_values(
    op: &str,
    overload: &str,
    args: &Bound<'_, PyTuple>,
    kwargs: &Bound<'_, PyDict>,
) -> PyResult<(Vec<WireValue>, HashMap<String, WireValue>)> {
    let args_info = torch_sys::call_op::get_schema_args_info(op, overload).map_err(|err| {
        PyValueError::new_err(format!(
            "Failed to get the operator schema for {}::{}: {}",
            op, overload, err
        ))
    })?;

    let args = args
        .iter()
        .zip(&args_info)
        .map(|(arg, arg_info)| {
            WireValue::from_pyobject_with_torch_op_arg_type(
                arg,
                arg_info.type_,
                arg_info.num_elements,
                arg_info.allows_number_as_tensor,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let kwargs = kwargs
        .iter()
        .map(|(k, v)| {
            let key = k.extract::<String>()?;
            let arg_info = args_info
                .iter()
                .find(|arg_info| arg_info.name == key)
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "Torch op {}::{} does not support kwarg {}",
                        op, overload, key
                    ))
                })?;
            let val = WireValue::from_pyobject_with_torch_op_arg_type(
                v,
                arg_info.type_,
                arg_info.num_elements,
                arg_info.allows_number_as_tensor,
            )?;
            Ok((key, val))
        })
        .collect::<Result<HashMap<_, _>, PyErr>>()?;
    Ok((args, kwargs))
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
    use pyo3::IntoPy;
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
                                let actual = $input.into_py(py).into_any().extract::<WireValue>(py)?;
                                assert_matches!(actual, WireValue::$kind(_));
                                anyhow::Ok(())
                            })
                    }
                )*

                #[test]
                fn test_wire_value_from_py_none() -> Result<()> {
                    setup()?;
                    Python::with_gil(|py| {
                        let obj: PyObject = PyNone::get(py).into_py(py);
                        let actual = obj.extract::<WireValue>(py)?;
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
