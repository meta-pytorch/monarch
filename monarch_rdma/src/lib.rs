/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// RDMA requires frequent unsafe code blocks
#![allow(clippy::undocumented_unsafe_blocks)]

pub mod device_selection;
mod ibverbs_primitives;
mod rdma_components;
mod rdma_manager_actor;

#[macro_use]
mod macros;

pub use ibverbs_primitives::*;
pub use rdma_components::*;
pub use rdma_manager_actor::*;
pub use test_utils::is_cuda_available;

/// Print comprehensive RDMA device information for debugging.
/// Controlled by MONARCH_DEBUG_RDMA environment variable.
pub fn print_device_info_if_debug_enabled(context: *mut rdmaxcel_sys::ibv_context) {
    if std::env::var("MONARCH_DEBUG_RDMA").is_ok() {
        unsafe {
            rdmaxcel_sys::rdmaxcel_print_device_info(context);
        }
    }
}

/// Print comprehensive RDMA device information for debugging (always prints).
pub fn print_device_info(context: *mut rdmaxcel_sys::ibv_context) {
    unsafe {
        rdmaxcel_sys::rdmaxcel_print_device_info(context);
    }
}

#[cfg(test)]
mod rdma_manager_actor_tests;
mod test_utils;
