/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EFA (Elastic Fabric Adapter) specific RDMA operations.
//!
//! This module contains EFA-specific helpers for device detection and configuration.
//! Connect and post operations are handled by C functions in rdmaxcel.c.

use std::sync::OnceLock;

use crate::ibverbs_primitives::IbverbsConfig;

/// Cached result of EFA device check.
static EFA_DEVICE_CACHE: OnceLock<bool> = OnceLock::new();

/// Checks if any EFA device is available in the system.
///
/// Uses `efadv_query_device()` to detect EFA hardware.
/// The result is cached after the first call.
pub fn is_efa_device() -> bool {
    *EFA_DEVICE_CACHE.get_or_init(is_efa_device_impl)
}

fn is_efa_device_impl() -> bool {
    // SAFETY: We are calling C functions from libibverbs and libefa.
    unsafe {
        let mut num_devices = 0;
        let device_list = rdmaxcel_sys::ibv_get_device_list(&mut num_devices);
        if device_list.is_null() || num_devices == 0 {
            return false;
        }
        let mut found = false;
        for i in 0..num_devices {
            let device = *device_list.add(i as usize);
            if device.is_null() {
                continue;
            }
            let context = rdmaxcel_sys::ibv_open_device(device);
            if context.is_null() {
                continue;
            }
            if rdmaxcel_sys::rdmaxcel_is_efa_dev(context) != 0 {
                found = true;
                rdmaxcel_sys::ibv_close_device(context);
                break;
            }
            rdmaxcel_sys::ibv_close_device(context);
        }
        rdmaxcel_sys::ibv_free_device_list(device_list);
        found
    }
}

/// Applies EFA-specific defaults to an `IbverbsConfig`.
///
/// EFA devices have different capabilities than standard InfiniBand/RoCE devices:
/// - GID index 0 (instead of 3)
/// - Max 1 SGE per work request
/// - No RDMA atomics support
pub fn apply_efa_defaults(config: &mut IbverbsConfig) {
    config.gid_index = 0;
    config.max_send_sge = 1;
    config.max_recv_sge = 1;
    config.max_dest_rd_atomic = 0;
    config.max_rd_atomic = 0;
}

/// Returns the MR access flags appropriate for EFA devices.
///
/// EFA does not support `IBV_ACCESS_REMOTE_ATOMIC`, so this returns only
/// local write, remote write, and remote read flags.
pub fn mr_access_flags() -> rdmaxcel_sys::ibv_access_flags {
    rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
}
