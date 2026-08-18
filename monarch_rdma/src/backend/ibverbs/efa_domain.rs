/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EFA domain strategy for [`IbvDomainImpl`].

use super::domain::IbvDomainImpl;
use super::efa_queue_pair::EfaQueuePair;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::primitives::IbvDeviceInfo;

/// EFA [`IbvDomainImpl`]. Uses the default host/dmabuf MR registration;
/// EFA has no device-specific memory-key binding to add.
#[derive(Debug)]
pub struct EfaDomain;

impl IbvDomainImpl for EfaDomain {
    type QueuePair = EfaQueuePair;

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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::ibverbs::domain::IbvDomain;
    use crate::backend::ibverbs::primitives::IbvCq;
    use crate::backend::ibverbs::primitives::IbvPd;

    // A domain with no protection domain cannot build a queue pair, and says so
    // rather than reaching the driver with a null handle.
    #[test]
    fn create_queue_pair_rejects_null_pd() {
        // SAFETY: `IbvPd::null()` holds a null PD (and, through it, a null
        // context) whose `Drop`s are no-ops.
        let domain = unsafe {
            IbvDomain::for_test(
                Arc::new(IbvPd::null()),
                IbvDeviceInfo::for_test_named("efa0"),
                EfaDomain,
            )
        };
        let err = domain
            .create_queue_pair(&IbvConfig::default(), Arc::new(IbvCq::null()))
            .expect_err("a null protection domain cannot back a queue pair");
        assert!(
            err.to_string().contains("null protection domain"),
            "unexpected error: {err}"
        );
    }
}
