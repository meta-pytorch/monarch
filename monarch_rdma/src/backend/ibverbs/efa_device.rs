/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EFA backend for [`IbvDevice`].

use std::ffi::CStr;
use std::sync::Arc;

use typeuri::Named;

use super::device::IbvDeviceImpl;
use super::efa_domain::EfaDomain;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use crate::register_ibv_device_impl;

/// AWS EFA (Elastic Fabric Adapter) backend.
#[derive(Debug, Named)]
pub struct EfaDevice;

impl IbvDeviceImpl for EfaDevice {
    type Domain = EfaDomain;

    fn backend_name() -> &'static str {
        "efa"
    }

    fn is_instance(ctx: Arc<IbvContext>) -> bool {
        // SAFETY: `ctx.as_ptr()` is a non-null context owned by
        // the `Arc<IbvContext>` for the duration of this call.
        if unsafe { rdmaxcel_sys::rdmaxcel_is_efa_dev(ctx.as_ptr()) } == 0 {
            return false;
        }
        // An EFA device that cannot serve RDMA read and write — p4d and
        // earlier — is left unclaimed. `IbvQueuePair` requires both
        // (`EfaQueuePair::create` rejects the device outright), so claiming
        // one here would only turn every transfer into an error; declining
        // it leaves `IbvBackend::<EfaDevice>::available()` false and lets
        // `RdmaBackends::spawn_available` fall through to the next backend.
        if !crate::efa::supports_rdma(ctx.as_ptr()) {
            // SAFETY: `ctx.as_ptr()` is a non-null open context, so its
            // `device` is the non-null device it was opened from, and
            // `ibv_get_device_name` returns a null-terminated string owned
            // by that device.
            let name = unsafe {
                CStr::from_ptr(rdmaxcel_sys::ibv_get_device_name((*ctx.as_ptr()).device))
            }
            .to_string_lossy()
            .into_owned();
            tracing::warn!(
                "EFA device {name} does not support RDMA read and write, so it \
                 cannot carry RDMA traffic; leaving it unclaimed"
            );
            return false;
        }
        true
    }

    fn apply_config_defaults(config: &mut IbvConfig) {
        config.max_send_sge = 1;
        config.max_recv_sge = 1;
        config.max_dest_rd_atomic = 0;
        config.max_rd_atomic = 0;
    }
}

register_ibv_device_impl!(EfaDevice);

#[cfg(test)]
mod tests {
    use super::*;

    // A device we cannot query for capabilities is treated as incapable, so a
    // failed query costs a transport rather than producing queue pairs that
    // reject every operation.
    #[test]
    fn supports_rdma_is_false_when_the_device_cannot_be_queried() {
        assert!(!crate::efa::supports_rdma(std::ptr::null_mut()));
    }

    // The capability check runs after the EFA check, so a non-EFA device is
    // declined for not being EFA and never reaches `supports_rdma`.
    #[test]
    fn is_instance_declines_a_non_efa_device() {
        assert!(!EfaDevice::is_instance(Arc::new(IbvContext::null())));
    }
}
