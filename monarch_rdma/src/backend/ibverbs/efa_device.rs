/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EFA backend for [`IbvDevice`].

use std::sync::Arc;

use typeuri::Named;

use super::device::IbvDeviceImpl;
use super::efa_domain::EfaDomain;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::queue_pair::legacy;
use super::queue_pair::rc_queue_pair::RCQueuePairManager;
use crate::register_ibv_device_impl;

/// AWS EFA (Elastic Fabric Adapter) backend.
#[derive(Debug, Named)]
pub struct EfaDevice;

impl IbvDeviceImpl for EfaDevice {
    type Domain = EfaDomain;
    // Temporary: EFA rides the reliable-connection manager (with the legacy
    // queue pair) until it gets its own SRD `QueuePairManager`.
    type QueuePairManager = RCQueuePairManager<EfaDevice, legacy::IbvQueuePair>;

    fn backend_name() -> &'static str {
        "efa"
    }

    fn is_instance(ctx: Arc<IbvContext>) -> bool {
        // SAFETY: `ctx.as_ptr()` is a non-null context owned by
        // the `Arc<IbvContext>` for the duration of this call.
        unsafe { rdmaxcel_sys::rdmaxcel_is_efa_dev(ctx.as_ptr()) != 0 }
    }

    fn apply_config_defaults(config: &mut IbvConfig) {
        config.max_send_sge = 1;
        config.max_recv_sge = 1;
        config.max_dest_rd_atomic = 0;
        config.max_rd_atomic = 0;
    }
}

register_ibv_device_impl!(EfaDevice);
