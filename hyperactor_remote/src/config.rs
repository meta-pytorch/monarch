/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration keys for `hyperactor_remote`.

use std::time::Duration;

use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;

declare_attrs! {
    /// Grace period a child proc gets to drain and exit after a graceful stop
    /// request before the spawner force-kills the OS process.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_REMOTE_STOP_GRACE_PERIOD".to_string()),
        Some("stop_grace_period".to_string()),
    ))
    pub attr STOP_GRACE_PERIOD: Duration = Duration::from_secs(10);
}
