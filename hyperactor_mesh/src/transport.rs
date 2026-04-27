/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Default transport configuration for hyperactor_mesh.

use hyperactor::channel::BindSpec;
use hyperactor::channel::ChannelTransport;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::global;

declare_attrs! {
    /// Default transport type to use across the application.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_DEFAULT_TRANSPORT".to_string()),
        Some("default_transport".to_string()),
    ))
    pub attr DEFAULT_TRANSPORT: BindSpec = BindSpec::Any(ChannelTransport::Unix);
}

/// Get the default bind spec to use across the application.
pub fn default_bind_spec() -> BindSpec {
    global::get_cloned(DEFAULT_TRANSPORT)
}
