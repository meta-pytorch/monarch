/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EFA domain strategy for [`IbvDomainImpl`].

use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::primitives::IbvDeviceInfo;
use super::queue_pair::legacy::IbvQueuePair;

/// EFA [`IbvDomainImpl`]. Uses the default host/dmabuf MR registration;
/// EFA has no device-specific memory-key binding to add.
#[derive(Debug)]
pub struct EfaDomain;

impl IbvDomainImpl for EfaDomain {
    type QueuePair = IbvQueuePair;

    unsafe fn new(
        _context: &IbvContext,
        _device_info: &IbvDeviceInfo,
        _config: &IbvConfig,
    ) -> Self {
        EfaDomain
    }

    fn access_flags(&self) -> i32 {
        // EFA does not support `IBV_ACCESS_REMOTE_ATOMIC`.
        (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ)
            .0 as i32
    }

    fn create_queue_pair(
        domain: &IbvDomain<Self>,
        config: &IbvConfig,
    ) -> anyhow::Result<Self::QueuePair> {
        // EFA builds the legacy single-type queue pair for now; it will
        // become an EFA-specific queue pair.
        IbvQueuePair::new(domain, config.clone())
    }
}
