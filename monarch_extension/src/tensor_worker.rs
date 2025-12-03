/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use monarch_messages::worker::*;
use pyo3::prelude::*;
pub(crate) fn register_python_bindings(worker_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    worker_mod.add_class::<Ref>()?;
    worker_mod.add_class::<StreamRef>()?;
    worker_mod.add_class::<FunctionPath>()?;
    worker_mod.add_class::<Cloudpickle>()?;
    Ok(())
}
