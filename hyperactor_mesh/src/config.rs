/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration for Hyperactor Mesh.
//!
//! This module provides hyperactor_mesh-specific configuration attributes that extend
//! the base hyperactor configuration system.

use std::env;

use hyperactor::attrs::declare_attrs;

// Declare hyperactor_mesh-specific configuration keys
declare_attrs! {
    /// The maximium for a dimension size allowed for a folded shape
    /// when reshaping during casting to limit fanout.
    /// usize::MAX means no reshaping as any shape will always be below
    /// the limit so no dimension needs to be folded.
    pub attr MAX_CAST_DIMENSION_SIZE: usize = usize::MAX;
}

pub fn init_from_env() {
    let config = hyperactor::config::global::lock();

    // Load max cast dimension size.
    if let Ok(val) = env::var("HYPERACTOR_MESH_MAX_CAST_DIMENSION_SIZE") {
        if let Ok(parsed) = val.parse::<usize>() {
            if parsed > 0 {
                tracing::info!("overriding MAX_CAST_DIMENSION_SIZE to {}", parsed);
                let _guard = config.override_key(MAX_CAST_DIMENSION_SIZE, parsed);
            }
        }
    }
}
