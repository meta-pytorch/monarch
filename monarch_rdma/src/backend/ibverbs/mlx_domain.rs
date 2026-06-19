/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mellanox (mlx5) domain strategy for [`IbvDomainImpl`].
//!
//! Host and non-mlx5dv device memory use the default host/dmabuf
//! registration. On an mlx5dv-capable device, CUDA device memory is bound
//! to an indirect mlx5dv memory key so a whole allocator segment is
//! addressable through a single key. That segment scanning + binding
//! implementation lands in a follow-up commit; this commit introduces the
//! strategy with the mlx5dv path stubbed (callers fall back to a dmabuf
//! MR).

use std::sync::Arc;

use super::device::IbvContext;
use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::domain::register_host_or_dmabuf_mr;
use super::memory_region::IbvMemoryRegionView;
use super::primitives::IbvConfig;
use super::primitives::IbvDeviceInfo;
use crate::local_memory::KeepaliveLocalMemory;
use crate::local_memory::is_device_ptr;

/// Mellanox [`IbvDomainImpl`].
pub struct MlxDomain {
    mlx5dv_enabled: bool,
}

impl IbvDomainImpl for MlxDomain {
    unsafe fn new(context: &IbvContext, _device_info: &IbvDeviceInfo, _config: &IbvConfig) -> Self {
        let ctx = context.as_ptr();
        // A null context (e.g. a test double) has no device to query, so treat
        // it as mlx5dv-unsupported rather than dereferencing null.
        let mlx5dv_enabled = if ctx.is_null() {
            false
        } else {
            // SAFETY: `ctx` is a non-null, live `ibv_context`; its `device`
            // field is the `ibv_device` we query for mlx5dv support.
            unsafe { rdmaxcel_sys::mlx5dv_is_supported((*ctx).device) }
        };
        Self { mlx5dv_enabled }
    }

    fn mr_access_flags(&self) -> i32 {
        (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC)
            .0 as i32
    }

    unsafe fn register_mr(
        &self,
        domain: Arc<IbvDomain>,
        mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        if self.mlx5dv_enabled && is_device_ptr(mem.addr()) {
            // SAFETY: `domain.as_ptr()` is null or a live PD, per this method's
            // contract.
            match unsafe { self.register_cuda_mlx5dv_mr(domain.as_ptr(), mem) } {
                Ok(view) => return Ok(view),
                Err(e) => {
                    tracing::warn!("mlx5dv CUDA registration failed, falling back to dmabuf: {e}")
                }
            }
        }
        // SAFETY: `domain.as_ptr()` is null or a live PD (per this method's
        // contract; `register_host_or_dmabuf_mr` errors on null), and the caller
        // keeps `mem`'s backing memory valid for the MR's lifetime.
        unsafe { register_host_or_dmabuf_mr(domain, self.mr_access_flags(), mem) }
    }
}

impl MlxDomain {
    /// Bind CUDA memory via an indirect mlx5dv memory key. Stub: the real
    /// implementation lands in a follow-up commit; today this always errors
    /// so [`Self::register_mr`] falls back to a dmabuf MR.
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a live protection domain whose context
    /// outlives this call.
    unsafe fn register_cuda_mlx5dv_mr(
        &self,
        _pd: *mut rdmaxcel_sys::ibv_pd,
        _mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        anyhow::bail!("mlx5dv CUDA registration not yet implemented")
    }
}
