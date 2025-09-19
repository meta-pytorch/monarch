/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration for Monarch Hyperactor.
//!
//! This module provides monarch-specific configuration attributes that extend
//! the base hyperactor configuration system.

use hyperactor::attrs::declare_attrs;
use pyo3::prelude::*;

// Declare monarch-specific configuration keys
declare_attrs! {
    /// Use a single asyncio runtime for all Python actors, rather than one per actor
    pub attr SHARED_ASYNCIO_RUNTIME: bool = false;
}

/// Python API for configuration management
///
/// Reload configuration from environment variables
#[pyfunction()]
pub fn reload_config_from_env() -> PyResult<()> {
    // Reload the hyperactor global configuration from environment variables
    hyperactor::config::global::init_from_env();
    hyperactor_mesh::config::init_from_env();
    Ok(())
}

/// Register Python bindings for the config module
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let reload = wrap_pyfunction!(reload_config_from_env, module)?;
    reload.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(reload)?;
    Ok(())
}
