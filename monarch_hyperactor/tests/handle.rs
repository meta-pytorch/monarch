/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use monarch_hyperactor::handle::PyHandle;
use monarch_hyperactor::runtime::GilSite;
use monarch_hyperactor::runtime::get_tokio_runtime;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use pyo3::IntoPyObjectExt;

// Attests HDL-14, HDL-15.
#[test]
fn rust_value_observers_outlive_handle_and_remain_non_consuming() {
    pyo3::Python::initialize();
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = 42i64
            .into_py_any(py)
            .expect("test value should convert to Python");
        PyHandle::from_value(value)
    });

    // Each waiter owns a reference to the core, and ready construction starts no
    // producer task. Dropping the wrapper must leave both observers
    // independently able to clone the same terminal value.
    let first = handle.wait_future();
    let second = handle.wait_future();
    drop(handle);

    let (first, second) = get_tokio_runtime().block_on(async { tokio::join!(first, second) });
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert_eq!(
            first
                .expect("first observer should resolve")
                .extract::<i64>(py)
                .expect("first value should be an integer"),
            42,
            "first observer should see the ready value"
        );
        assert_eq!(
            second
                .expect("second observer should resolve")
                .extract::<i64>(py)
                .expect("second value should be an integer"),
            42,
            "second observer should see the same ready value"
        );
    });
}
