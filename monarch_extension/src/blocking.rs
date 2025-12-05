/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use libc::atexit;
use pyo3::prelude::*;
use tokio::time::Duration;

extern "C" fn exit_handler() {
    loop {
        #[allow(clippy::disallowed_methods)]
        std::thread::sleep(Duration::from_mins(1));
    }
}

/// A function that blocks when called.
/// This is used for testing stuck jobs in the Python bindings.
#[pyfunction]
pub fn blocking_function() {
    // SAFETY:
    // This is in order to simulate a process in tests that never exits.
    unsafe {
        atexit(exit_handler);
    }
}

/// Register Python bindings for the blocking module.
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(blocking_function, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.blocking",
    )?;
    module.add_function(f)?;
    Ok(())
}
