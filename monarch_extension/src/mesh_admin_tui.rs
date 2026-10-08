/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use clap::ValueEnum;
use hyperactor_mesh::mesh_admin::AdminHandle;
use hyperactor_mesh_admin_tui_lib::LangName;
use hyperactor_mesh_admin_tui_lib::ThemeName;
use hyperactor_mesh_admin_tui_lib::TuiConfig;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (
    addr,
    *,
    admin_port = None,
    refresh_ms = 2000,
    theme = "nord",
    lang = "en",
    tls_ca = None,
    tls_cert = None,
    tls_key = None,
    plaintext = false,
))]
fn run(
    py: Python<'_>,
    addr: String,
    admin_port: Option<u16>,
    refresh_ms: u64,
    theme: &str,
    lang: &str,
    tls_ca: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    plaintext: bool,
) -> PyResult<()> {
    if plaintext && (tls_ca.is_some() || tls_cert.is_some() || tls_key.is_some()) {
        return Err(PyValueError::new_err(
            "plaintext cannot be combined with TLS certificate options",
        ));
    }

    let theme = ThemeName::from_str(theme, false).map_err(PyValueError::new_err)?;
    let lang = LangName::from_str(lang, false).map_err(PyValueError::new_err)?;

    monarch_hyperactor::runtime::signal_safe_block_on(py, async move {
        let addr = AdminHandle::parse(&addr)
            .resolve(admin_port)
            .await
            .map_err(|error| PyRuntimeError::new_err(format!("{error:#}")))?;
        let config = TuiConfig {
            addr,
            refresh_ms,
            theme,
            lang,
            tls_ca,
            tls_cert,
            tls_key,
            plaintext,
        };
        hyperactor_mesh_admin_tui_lib::run(config)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })?
}

/// Register Python bindings for the mesh admin TUI.
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let run = wrap_pyfunction!(run, module)?;
    run.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.mesh_admin_tui",
    )?;
    module.add_function(run)?;
    Ok(())
}
