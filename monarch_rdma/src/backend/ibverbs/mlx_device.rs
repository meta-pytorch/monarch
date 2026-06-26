/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mellanox backend for [`IbvDevice`].

use std::sync::Arc;

use typeuri::Named;

use super::device::IbvContext;
use super::device::IbvDeviceImpl;
use super::mlx_domain::MlxDomain;
use super::primitives::IbvConfig;
use super::roce_gid;
use crate::register_ibv_device_impl;

/// PCI vendor ID for Mellanox Technologies.
const MELLANOX_VENDOR_ID: u32 = 0x02c9;

/// Mellanox backend.
#[derive(Debug, Named)]
pub struct MlxDevice;

impl IbvDeviceImpl for MlxDevice {
    type IbvDomainImpl = MlxDomain;

    fn backend_name() -> &'static str {
        "mellanox"
    }

    fn is_instance(ctx: Arc<IbvContext>) -> bool {
        let mut attr = rdmaxcel_sys::ibv_device_attr::default();
        // SAFETY: `ctx.as_ptr()` is a non-null context owned by
        // the `Arc<IbvContext>` for the duration of this call;
        // `&mut attr` is a writable, properly aligned
        // `ibv_device_attr`.
        let queried = unsafe { rdmaxcel_sys::ibv_query_device(ctx.as_ptr(), &mut attr) } == 0;
        queried && attr.vendor_id == MELLANOX_VENDOR_ID
    }

    fn apply_config_defaults(config: &mut IbvConfig) {
        config.gid_index = roce_gid::select_roce_v2_gid_index();
    }
}

register_ibv_device_impl!(MlxDevice);
