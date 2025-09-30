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

use hyperactor::attrs::declare_attrs;
use hyperactor::config::CONFIG_ENV_VAR;

// Declare hyperactor_mesh-specific configuration keys
declare_attrs! {
    /// The maximium for a dimension size allowed for a folded shape
    /// when reshaping during casting to limit fanout.
    /// usize::MAX means no reshaping as any shape will always be below
    /// the limit so no dimension needs to be folded.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_MESH_MAX_CAST_DIMENSION_SIZE".to_string())
    pub attr MAX_CAST_DIMENSION_SIZE: usize = usize::MAX;
}
