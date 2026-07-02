/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Registered memory regions returned by [`IbvDomainImpl::register_mr`].
//!
//! [`IbvMemoryRegionView`] is the cheap, cloneable handle peers use: the keys
//! and addresses for a slice of registered memory, plus an `Arc<dyn IbvMemoryRegionKeepalive>`
//! that keeps the backing registration's resources alive until the last clone
//! of the view drops.
//!
//! [`IbvDomainImpl::register_mr`]: super::domain::IbvDomainImpl::register_mr

use std::sync::Arc;

use super::primitives::IbvPd;

/// Guards the resources behind a registered MR, releasing them when the last
/// [`IbvMemoryRegionView`] over it drops. Each implementor frees whatever it
/// owns in its own `Drop`; the trait carries no methods and exists only to
/// type-erase the guards so a view can hold any of them behind an
/// `Arc<dyn IbvMemoryRegionKeepalive>`.
pub(super) trait IbvMemoryRegionKeepalive: std::fmt::Debug + Send + Sync {}

/// Guard for a standalone MR registered via `ibv_reg_mr` / `ibv_reg_dmabuf_mr`;
/// its `Drop` runs `ibv_dereg_mr`. Holds the PD so it outlives that call.
#[derive(Debug)]
pub(super) struct IbvMemoryRegion {
    pub(super) mr: *mut rdmaxcel_sys::ibv_mr,
    /// Keepalive for the PD `mr` was registered against; dropped only after
    /// this struct's `Drop` returns, so the PD is alive during `ibv_dereg_mr`.
    /// Never read directly.
    pub(super) _pd: Arc<IbvPd>,
}

// SAFETY: `mr` is only handed to `ibv_dereg_mr` (which libibverbs treats as
// thread-safe) and is owned exclusively by this guard; `_pd` is itself
// `Send + Sync`.
unsafe impl Send for IbvMemoryRegion {}
unsafe impl Sync for IbvMemoryRegion {}

impl IbvMemoryRegionKeepalive for IbvMemoryRegion {}

#[cfg(test)]
impl IbvMemoryRegion {
    /// The raw `ibv_mr` this guard owns. Valid until the guard drops (which
    /// deregisters it).
    pub(super) fn as_ptr(&self) -> *mut rdmaxcel_sys::ibv_mr {
        self.mr
    }
}

impl Drop for IbvMemoryRegion {
    fn drop(&mut self) {
        if self.mr.is_null() {
            return;
        }
        // SAFETY: `mr` is non-null (checked above), was returned by
        // `ibv_reg_mr` / `ibv_reg_dmabuf_mr`, and is deregistered exactly once
        // (a value's `Drop` runs once). The PD it was registered against is
        // still alive: `domain` is dropped only after this returns.
        let result = unsafe { rdmaxcel_sys::ibv_dereg_mr(self.mr) };
        if result != 0 {
            tracing::error!(
                "failed to deregister MR at {:p}: error code {}",
                self.mr,
                result
            );
        }
    }
}

/// A cloneable handle to a slice of registered memory: the keys and addresses
/// a peer needs, plus an `Arc<dyn IbvMemoryRegionKeepalive>` keepalive.
///
/// Cheap to clone; every clone shares the same guard, so the backing
/// registration stays alive (and registered) until the last clone drops.
#[derive(Debug, Clone)]
pub struct IbvMemoryRegionView {
    /// Virtual address in the local process address space.
    pub virtual_addr: usize,
    /// RDMA address, possibly offset from the region's base MR address.
    pub rdma_addr: usize,
    pub size: usize,
    pub lkey: u32,
    pub rkey: u32,
    /// Name of the RDMA device the view's protection domain is on.
    pub device_name: String,
    /// Keeps the backing registration alive for every clone of this view; the
    /// last drop releases its resources. Never read directly.
    pub(super) _guard: Arc<dyn IbvMemoryRegionKeepalive>,
}

impl IbvMemoryRegionView {
    pub(super) fn new(
        virtual_addr: usize,
        rdma_addr: usize,
        size: usize,
        lkey: u32,
        rkey: u32,
        device_name: String,
        guard: Arc<dyn IbvMemoryRegionKeepalive>,
    ) -> Self {
        Self {
            virtual_addr,
            rdma_addr,
            size,
            lkey,
            rkey,
            device_name,
            _guard: guard,
        }
    }
}
