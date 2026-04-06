/*
* Copyright (c) Meta Platforms, Inc. and affiliates.
* All rights reserved.
*
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree.
*/
//! Broadcom (bnxt_re) specific RDMA configuration.
//!
//! Broadcom NICs use standard ibverbs without device-specific extensions.
use std::sync::OnceLock;
use crate::backend::ibverbs::primitives::IbvConfig;
use crate::backend::ibverbs::primitives::get_all_devices;
/// Cached result of Broadcom device check.
static BROADCOM_DEVICE_CACHE: OnceLock<bool> = OnceLock::new();
/// Checks if any Broadcom RDMA device is available in the system.
///
/// Detects devices with names starting with "bnxt_re" or vendor ID 0x14e4.
/// The result is cached after the first call.
pub fn is_broadcom_device() -> bool {
    *BROADCOM_DEVICE_CACHE.get_or_init(is_broadcom_device_impl)
}
fn is_broadcom_device_impl() -> bool {
    const BROADCOM_VENDOR_ID: u32 = 0x14e4;
    get_all_devices().iter().any(|dev| {
        dev.name().starts_with("bnxt_re") || dev.vendor_id() == BROADCOM_VENDOR_ID
    })
}
/// Applies Broadcom-specific defaults to an `IbvConfig`.
///
/// Broadcom NICs have similar capabilities to Mellanox but may differ in:
/// - GID index (typically 0 for RoCEv2)
/// - Some atomics support variations
pub fn apply_broadcom_defaults(config: &mut IbvConfig) {
    // Broadcom RoCE typically uses GID index 0 for RoCEv2
    config.gid_index = 0;
    // Other defaults are typically fine for Broadcom
}
/// Returns the MR access flags appropriate for Broadcom devices.
///
/// Broadcom supports standard RDMA access flags including atomics.
pub fn mr_access_flags() -> rdmaxcel_sys::ibv_access_flags {
    rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_broadcom_device() {
        // This test just verifies the function doesn't panic
        let result = is_broadcom_device();
        println!("is_broadcom_device: {}", result);
    }
}
