/*
* Copyright (c) Meta Platforms, Inc. and affiliates.
* All rights reserved.
*
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree.
*/
//! Mellanox (mlx) specific RDMA configuration.
//!
//! This module contains mellanox specific helpers for device detection and configuration.
//! Connect and post operations are handled by C functions in rdmaxcel.c
use crate::nic_backend::NicBackend;
use std::sync::OnceLock;
use crate::backend::ibverbs::primitives::IbvConfig;

/// Cached result of mellanox device check.
static DETECTED: OnceLock<bool> = OnceLock::new();

pub struct MlxBackend;

impl NicBackend for MlxBackend{

    fn name(&self) -> &'static str { "Mellanox" }

    fn device_prefixes(&self) -> &'static [&'static str] { &["mlx"] }

    fn vendor_ids(&self) -> &'static [u32] { &[0x15b3] }

    /// Checks if mlx5dv (Mellanox device-specific verbs extension) is supported.
    ///
    /// This function attempts to open the first available RDMA device and check if
    /// mlx5dv extensions can be initialized. The mlx5dv extensions are required for
    /// advanced features like GPU Direct RDMA and direct queue pair manipulation.
    ///
    /// The result is cached after the first call, making subsequent calls essentially free.
    ///
    /// # Returns
    ///
    /// `true` if mlx5dv extensions are supported, `false` otherwise.
    fn is_detected(&self) -> bool {
        *DETECTED.get_or_init(mlx5dv_supported_impl)
    }

    fn mlx5dv_supported_impl(&self) -> bool {
      // SAFETY: We are calling C functions from libibverbs and libmlx5.
      unsafe {
          let mut mlx5dv_supported = false;
          let mut num_devices = 0;
          let device_list = rdmaxcel_sys::ibv_get_device_list(&mut num_devices);
          if !device_list.is_null() && num_devices > 0 {
              let device = *device_list;
              if !device.is_null() {
                  mlx5dv_supported = rdmaxcel_sys::mlx5dv_is_supported(device);
              }
              rdmaxcel_sys::ibv_free_device_list(device_list);
          }
          mlx5dv_supported
      }
    }


    /// Returns the queue-pair type for Mellanox devices.
    fn qp_type(&self) -> u32 { rdmaxcel_sys::RDMA_QP_TYPE_MLX5DV }

    /// Returns MR access flags appropriate for Mellanox devices.
    ///
    /// Mellanox supports standard RDMA access flags including atomics
    fn mr_access_flags() -> rdmaxcel_sys::ibv_access_flags {
        rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC
    }

    fn priority(&self) -> u32 { 50 } // Lowest - the "default" fallback TODO_ANDY: revisit priority

    fn supports_mlx5dv(&self) -> bool { true }

} // MlxBackend
