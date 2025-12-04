/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use derive_more::From;
use derive_more::TryInto;
use enum_as_inner::EnumAsInner;
use hyperactor::Named;
use monarch_types::PickledPyObject;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyNone;
use serde::Deserialize;
use serde::Serialize;
use torch_sys2::Device;
use torch_sys2::Layout;
use torch_sys2::MemoryFormat;
use torch_sys2::ScalarType;

use crate::worker::Ref;

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
    Layout(#[serde(with = "torch_sys2::LayoutDef")] Layout),
    ScalarType(#[serde(with = "torch_sys2::ScalarTypeDef")] ScalarType),
    MemoryFormat(#[serde(with = "torch_sys2::MemoryFormatDef")] MemoryFormat),
    // Make this wrap the unit type, as `pyo3::FromPyObject` doesn't work with
    // empty enum variants.
    None(()),
    PyObject(PickledPyObject),
}

impl FromPyObject<'_> for WireValue {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(WireValue::PyObject(PickledPyObject::pickle(obj)?))
    }
}

impl<'py> IntoPyObject<'py> for WireValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
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
        }
    }
}

impl From<PyObject> for WireValue {
    fn from(obj: PyObject) -> Self {
        Python::with_gil(|py| WireValue::PyObject(PickledPyObject::pickle(obj.bind(py)).unwrap()))
    }
}
