/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::PathBuf;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;

#[pyfunction]
fn get_or_create_trace_id() -> String {
    hyperactor_telemetry::trace::get_or_create_trace_id()
}

#[pyfunction]
#[pyo3(signature = (telemetry_url, start_us, end_us, output = None, upload = false))]
fn export_profile(
    py: Python<'_>,
    telemetry_url: String,
    start_us: i64,
    end_us: i64,
    output: Option<String>,
    upload: bool,
) -> PyResult<String> {
    let path = py
        .detach(move || {
            monarch_perfetto_trace::profile::export_profile(
                &telemetry_url,
                start_us,
                end_us,
                output.map(PathBuf::from),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    #[cfg(fbcode_build)]
    if upload {
        return py
            .detach(move || -> anyhow::Result<String> {
                eprintln!("Uploading {} to Manifold...", path.display());
                let manifold_path = trace_upload::upload_trace_file(&path)?;
                Ok(trace_upload::perfetto_trace_url(&manifold_path))
            })
            .map_err(|error| PyRuntimeError::new_err(error.to_string()));
    }

    #[cfg(not(fbcode_build))]
    let _ = upload;

    Ok(path.to_string_lossy().into_owned())
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(get_or_create_trace_id, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.trace",
    )?;
    module.add_function(f)?;

    let f = wrap_pyfunction!(export_profile, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.trace",
    )?;
    module.add_function(f)?;
    Ok(())
}
