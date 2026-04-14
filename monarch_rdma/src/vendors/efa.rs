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

use crate::nic_backend::NicBackend;
use std::sync::OnceLock;
use crate::backend::ibverbs::primitives::IbvConfig;

pub struct EfaBackend;

/// Cached result of EFA device check.
static DETECTED: OnceLock<bool> = OnceLock::new();



impl NicBackend for EfaBackend {
    fn name(&self) -> &'static str { "EFA" }

    fn device_prefixes(&self) -> &'static [&'static str] { &["efa"] }

    // TODO_ANDY: figure out where this comes into play and whether it's valid
    fn vendor_ids(&self) -> &'static [u32] { &[0x1d0f] }

    /// Checks if any EFA device is available in the system.
    ///
    /// Uses `efadv_query_device()` to detect EFA hardware.
    /// The result is cached after the first call.
    fn is_detected() -> bool { /// TODO_ANDY: make sure is_efa_device() isn't called elsewhere
        *DETECTED.get_or_init(is_efa_device_impl)
    }

    fn is_efa_device_impl() -> bool {
        // SAFETY: We are calling C functions from libibverbs and libefa.
        unsafe { ///TODO_ANDY: this shares a lot of logic as primitives::get_all_devices().
                 ///           can probably swap out this logic with get_all_devices somehow.
                 ///           probably how the broadcom.rs file does it
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

    fn qp_type(&self) -> u32 { rdmaxcel_sys::RDMA_QP_TYPE_EFA }


    /// Applies EFA-specific defaults to an `IbvConfig`.
    ///
    /// EFA devices have different capabilities than standard InfiniBand/RoCE devices:
    /// - GID index 0 (instead of 3)
    /// - Max 1 SGE per work request
    /// - No RDMA atomics support
    /// TODO_ANDY: make sure apply_efa_defaults isn't called elsewhere (like primitives.rs)
    pub fn apply_config_defaults(&self, config: &mut IbvConfig) {
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
    fn mr_access_flags(&self) -> rdmaxcel_sys::ibv_access_flags {
        rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
    }

    /// TODO_ANDY: figure out what to do with this
    fn priority(&self) -> u32 { 300 }

} // EfaBackend
