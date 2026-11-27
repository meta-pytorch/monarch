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

    /// The longest a connection request may ask to wait for an offer.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_REMOTE_RENDEZVOUS_MAX_WAIT".to_string()),
        Some("rendezvous_max_wait".to_string()),
    ))
    pub attr RENDEZVOUS_MAX_WAIT: Duration = Duration::from_secs(600);

    /// The most requests a rendezvous queues while no offer is posted.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_REMOTE_RENDEZVOUS_MAX_QUEUED".to_string()),
        Some("rendezvous_max_queued".to_string()),
    ))
    pub attr RENDEZVOUS_MAX_QUEUED: usize = 1024;

    /// How much longer a connector waits for the rendezvous's answer after
    /// its requested wait ends. The rendezvous starts the wait when the
    /// request arrives, so its deadline falls after the connector's
    /// unpadded one. The margin covers sending the request, the rendezvous
    /// getting to it, and the reply, so the rendezvous's answer normally
    /// decides the outcome.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_REMOTE_RENDEZVOUS_REPLY_MARGIN".to_string()),
        Some("rendezvous_reply_margin".to_string()),
    ))
    pub attr RENDEZVOUS_REPLY_MARGIN: Duration = Duration::from_secs(5);
}
