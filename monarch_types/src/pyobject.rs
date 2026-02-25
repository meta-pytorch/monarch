/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct PickledPyObject {
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
    cloudpickle: bool,
}

impl PickledPyObject {
    pub fn pickle<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        Self::pickle_impl(obj, false)
    }

    pub fn cloudpickle<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        Self::pickle_impl(obj, true)
    }

    fn module(cloudpickle: bool) -> &'static str {
        if cloudpickle { "cloudpickle" } else { "pickle" }
    }

    fn pickle_impl<'py>(obj: &Bound<'py, PyAny>, cloudpickle: bool) -> PyResult<Self> {
        let module = Self::module(cloudpickle);
        let bytes = obj
            .py()
            .import(module)?
            .call_method1("dumps", (obj,))?
            .downcast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        Ok(Self { bytes, cloudpickle })
    }

    pub fn unpickle<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        py.import(Self::module(self.cloudpickle))?
            .call_method1("loads", (self.bytes.as_slice(),))
    }
}

impl TryFrom<&Bound<'_, PyAny>> for PickledPyObject {
    type Error = PyErr;
    fn try_from(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        Self::pickle(obj)
    }
}

impl TryFrom<Bound<'_, PyAny>> for PickledPyObject {
    type Error = PyErr;
    fn try_from(obj: Bound<'_, PyAny>) -> PyResult<Self> {
        Self::pickle(&obj)
    }
}

impl FromPyObject<'_> for PickledPyObject {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        PickledPyObject::pickle(obj)
    }
}

impl<'py> IntoPyObject<'py> for &PickledPyObject {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.unpickle(py)
    }
}
