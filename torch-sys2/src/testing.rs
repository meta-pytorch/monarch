/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Testing utilities

use pyo3::prelude::*;

use crate::Tensor;
use crate::torch_allclose;
use crate::torch_full;
use crate::torch_stack;

pub fn allclose(a: &Tensor, b: &Tensor) -> Result<bool, String> {
    Python::with_gil(|py| {
        let a_obj = a.inner.bind(py);
        let b_obj = b.inner.bind(py);

        torch_allclose(py)
            .call1((a_obj, b_obj))
            .map_err(|e| format!("Failed to call torch.allclose: {}", e))?
            .extract()
            .map_err(|e| format!("Failed to extract result: {}", e))
    })
}

pub fn cuda_full(size: &[i64], value: f32) -> Tensor {
    Python::with_gil(|py| {
        let size_tuple = pyo3::types::PyTuple::new(py, size).unwrap();

        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("device", "cuda").unwrap();

        let result = torch_full(py)
            .call((size_tuple, value), Some(&kwargs))
            .unwrap();

        Tensor {
            inner: result.clone().unbind(),
        }
    })
}

pub fn stack(tensors: &[Tensor]) -> Tensor {
    Python::with_gil(|py| {
        // Convert Rust tensor slice to Python list
        let tensor_list = pyo3::types::PyList::empty(py);
        for tensor in tensors {
            tensor_list.append(tensor.inner.bind(py)).unwrap();
        }

        let result = torch_stack(py).call1((tensor_list,)).unwrap();

        Tensor {
            inner: result.clone().unbind(),
        }
    })
}
