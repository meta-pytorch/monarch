/*
* Copyright (c) Meta Platforms, Inc. and affiliates.
* All rights reserved.
*
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree.
*/
use crate::backend::ibverbs::porimitives::IbvConfig;

pub trait NicBackend: Send + Sync + 'static {
    /// Human-readable name(e.g., "Mellanox", "EFA", "Broadcom")
    fn name(&self) -> &'static str;

    /// Device name prefixes for detection (e.g., &[mlx5_", "mlx4_"])
    fn device_prefixes(&self) -> &'static [&'static str];

    /// PCI vendor IDs for fallback detection
    fn vendor_ids(&self) -> &'static [u32] { &[] }

    /// Whether this NIC is present on current machine (cached internally)
    fn is_detected(&self) -> bool;

    /// QP type to use when IBvOPType::Auto is selected
    fn qp_type(&self) -> u32;

    /// Apply vendor-specific overrides to the default IbvConfig
    fn apply_config_defaults(&self, config: &mut IbvConfig);

    /// MR access flags for ibv_reg_mr / ibv_reg_dmabuf_mr
    fn mr_access_flags(&self) -> rdmaxcel_sys::ibv_access_flags;

    /// Whether this NIC uses mlx5dv extensions (segment scanning, loopback QP)
    fn supports_mlx5dv(&self) -> bool { false }

    /// Detection priority - higher values are checked first.
    /// EFA should be highest (it's the most constrained environment).
    fn priority(&self) -> u32 { 100 }
}
