/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Registered memory regions returned by [`IbvDomainImpl::register_mr`].
//!
//! [`IbvMemoryRegionView`] is the cheap, cloneable handle this process addresses
//! its own registered memory through: the keys and addresses for a slice of
//! registered memory, plus an `Arc<dyn IbvMemoryRegionKeepalive>` that keeps the
//! backing registration's resources alive until the last clone of the view
//! drops.
//!
//! [`IbvRemoteMemoryRegionView`] is what a peer gets instead: the same region
//! reduced to what the wire can carry and the far side can use.
//!
//! [`IbvDomainImpl::register_mr`]: super::domain::IbvDomainImpl::register_mr

use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use super::primitives::IbvMr;

/// Guards the resources behind a registered MR, releasing them when the last
/// [`IbvMemoryRegionView`] over it drops. Each implementor frees whatever it
/// owns in its own `Drop`; the trait carries no methods and exists only to
/// type-erase the guards so a view can hold any of them behind an
/// `Arc<dyn IbvMemoryRegionKeepalive>`.
pub(super) trait IbvMemoryRegionKeepalive: std::fmt::Debug + Send + Sync {}

/// A standalone [`IbvMr`] guards its own registration: its `Drop` runs
/// `ibv_dereg_mr` against the PD it owns.
impl IbvMemoryRegionKeepalive for IbvMr {}

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

    /// Fabricate a zero-sized view naming `device_name` and carrying `key` as
    /// both its `lkey` and its `rkey`, over a null MR keepalive whose `Drop`
    /// is a no-op. For tests that care only about which device a registration
    /// belongs to, and which of several registrations they are holding.
    #[cfg(test)]
    pub(crate) fn for_test(device_name: &str, key: u32) -> Self {
        Self::new(
            0,
            0,
            0,
            key,
            key,
            device_name.to_string(),
            Arc::new(IbvMr::null()),
        )
    }
}

/// What a peer needs in order to address one of our registered memory regions
/// over RDMA: the region's `rkey`, its RDMA address, its size, and the device
/// serving it.
///
/// This is the wire form of an [`IbvMemoryRegionView`]. It carries no `lkey` and
/// no keepalive: an `lkey` only means anything to the protection domain that
/// issued it, and the registration is kept alive by the views the owning side
/// holds.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct IbvRemoteMemoryRegionView {
    pub rkey: u32,
    /// RDMA address (may differ from virtual address).
    pub addr: usize,
    pub size: usize,
    /// Name of the RDMA device this region is registered on (e.g., "mlx5_0").
    pub device_name: String,
}

impl From<&IbvMemoryRegionView> for IbvRemoteMemoryRegionView {
    /// The wire transport details are fully derived from the registered MR
    /// view: the remote key, the RDMA address, the size, and the device name.
    fn from(view: &IbvMemoryRegionView) -> Self {
        Self {
            rkey: view.rkey,
            addr: view.rdma_addr,
            size: view.size,
            device_name: view.device_name.clone(),
        }
    }
}
