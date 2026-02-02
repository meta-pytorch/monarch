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

use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;

// Declare hyperactor_mesh-specific configuration keys
declare_attrs! {
    /// The maximium for a dimension size allowed for a folded shape
    /// when reshaping during casting to limit fanout.
    /// usize::MAX means no reshaping as any shape will always be below
    /// the limit so no dimension needs to be folded.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_MAX_CAST_DIMENSION_SIZE".to_string()),
        py_name: Some("max_cast_dimension_size".to_string()),
    })
    pub attr MAX_CAST_DIMENSION_SIZE: usize = usize::MAX;

    /// Which builtin process launcher backend to use.
    /// Accepted values: "native" (default), "systemd".
    /// Trimmed and lowercased before matching.
    ///
    /// **Precedence:** Python spawner (via SetProcSpawner) overrides this.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_PROC_LAUNCHER_KIND".to_string()),
        py_name: Some("proc_launcher_kind".to_string()),
    })
    pub attr MESH_PROC_LAUNCHER_KIND: String = String::new();
}
