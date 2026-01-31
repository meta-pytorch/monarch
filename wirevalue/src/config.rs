/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration keys for wirevalue.

use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;

use crate::Encoding;

declare_attrs! {
    /// The default encoding to be used.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_DEFAULT_ENCODING".to_string()),
        py_name: Some("default_encoding".to_string()),
        propagate: true,
    })
    pub attr DEFAULT_ENCODING: Encoding = Encoding::Multipart;
}
