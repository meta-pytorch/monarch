/*
* Copyright (c) Meta Platforms, Inc. and affiliates.
* All rights reserved.
*
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree.
*/
mod efa;
mod broadcom;
mod mellanox;
// Future: add more Nic backend modules here

use::std::sync::OnceLock;
use crate::nic_backend::NicBackend;
use crate::backend::ibverbs::primitives::IbvConfig;

/// Returns all known NIC backends, ordered by priority (highest first)
// Future: add new backend
fn all_nic_backends() -> Vec<&'static dyn NicBackend> {
    let mut backends: Vec<&'static dyn NicBackend> = vec![
        &efa::EfaBackend,
        &broadcom::BroadcomBackend,
        &mellanox::MellanoxBackend,
    ];
    backends.sort_by(|a, b| b.priority().cmp(&a.priority()));
    backends
}

/// Lock for current active backend
static ACTIVE: OnceLock<Option<&'static dyn NicBackend>> = OnceLock::new()

/// Returns the detected NIC backend for this machine, or None if nothing detected.
pub fn active_backend() -> Option<&'static dyn NicBackend> {
    *ACTIVE.get_or_init(|| {
        all_backends().into_iter().find(|b| b.is_detected())
    })
}


// --- Higher-Level dispatch functions ---
// Replacements for scattered if/else conditionals for detecting nic backend

/// QP type for Auto mode. Falls back to Standard if no backend detected.
pub fn resolve_auto_qp_type() -> u32 {
    active_backend()
        .map(|b| b.qp_type())
        .unwrap_or(rdmaxcel_sys::RDMA_QP_TYPE_STANDARD)
}

/// Apply vendor-specific config defaults. No-op if no backend detected
pub fn apply_vendor_defaults(config: &mut IbvConfig) {
    if let Some(backend) = active_backend() {
        backend.apply_config_defaults(config);
    }
}

/// MR access flags for the active NIC
pub fn vendor_mr_access_flags() -> rdmaxcel_sys::ibv_access_flags {
    active_backend()
        .map(|b| b.mr_access_flags())
        .unwrap_or(
            rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
              | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
              | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
              | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC,
        )
}

/// Whether the active NIC supports mlx5dv (segment scanning, loopback QP).
pub fn supports_mlx5dv() -> bool {
    active_backeend().map(|b| b.supports_mlx5dv()).unwrap_or(false)
}

/// Collected device prefixes from all registered backends.
pub fn all_known_prefixes() -> Vec<&'static str> {
    all_backends()
        .iter()
        .flat_map(|b| b.device_prefixes().iter().copied())
        .collect()
}

