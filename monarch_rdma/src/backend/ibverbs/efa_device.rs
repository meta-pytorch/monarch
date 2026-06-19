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

use super::device::IbvContext;
use super::device::IbvDeviceImpl;
use super::efa_domain::EfaDomain;
use super::primitives::IbvConfig;
use crate::register_ibv_device_impl;

/// AWS EFA (Elastic Fabric Adapter) backend.
#[derive(Debug, Named)]
pub struct EfaDevice;

impl IbvDeviceImpl for EfaDevice {
    type IbvDomainImpl = EfaDomain;

    fn backend_name() -> &'static str {
        "efa"
    }

    fn is_instance(ctx: Arc<IbvContext>) -> bool {
        // SAFETY: `ctx.as_ptr()` is a non-null context owned by
        // the `Arc<IbvContext>` for the duration of this call.
        unsafe { rdmaxcel_sys::rdmaxcel_is_efa_dev(ctx.as_ptr()) != 0 }
    }

    fn apply_config_defaults(config: &mut IbvConfig) {
        config.gid_index = 0;
        config.max_send_sge = 1;
        config.max_recv_sge = 1;
        config.max_dest_rd_atomic = 0;
        config.max_rd_atomic = 0;
    }
}

register_ibv_device_impl!(EfaDevice);
