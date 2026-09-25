/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AMD Pensando (ionic) domain strategy for [`IbvDomainImpl`].

use std::sync::Arc;

use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::primitives::IbvCq;
use super::primitives::IbvDeviceInfo;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::RCQueuePair;

/// ionic [`IbvDomainImpl`]. Standard RoCE v2 RC queue pairs over plain
/// ibverbs, and the default host (`ibv_reg_mr`) / device-memory
/// (`ibv_reg_dmabuf_mr`) MR registration; ionic has no device-specific
/// memory-key binding to add (unlike mlx5dv indirect mkeys).
#[derive(Debug)]
pub struct IonicDomain;

/// Lowers `config`'s scatter/gather caps to the device's `max_sge`. A
/// non-positive `max_sge` (an unqueried device) leaves `config` unchanged.
fn fit_sge_to_device(config: &mut IbvConfig, max_sge: i32) {
    if let Ok(max_sge) = u32::try_from(max_sge)
        && max_sge > 0
    {
        config.max_send_sge = config.max_send_sge.min(max_sge);
        config.max_recv_sge = config.max_recv_sge.min(max_sge);
    }
}

impl IbvDomainImpl for IonicDomain {
    type QueuePair = RCQueuePair;

    unsafe fn new(
        _context: &IbvContext,
        _device_info: &IbvDeviceInfo,
        _config: &IbvConfig,
    ) -> Self {
        IonicDomain
    }

    fn access_flags(&self) -> i32 {
        // The device reports `ATOMIC_GLOB`, so grant remote atomics like mlx5.
        (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC)
            .0 as i32
    }

    /// Builds an [`RCQueuePair`] after fitting the scatter/gather caps to the
    /// device. `IonicDevice::apply_config_defaults` already does this, but the
    /// manager seeds those defaults only when it spawns without an explicit
    /// config, and one explicit [`IbvConfig`] is shared by every ibverbs
    /// backend. Its generic default of 30 SGEs would fail `ibv_create_qp`
    /// here (ionic `max_sge` is 8).
    unsafe fn create_queue_pair(
        domain: &IbvDomain<Self>,
        config: &IbvConfig,
        cq: Arc<IbvCq>,
    ) -> anyhow::Result<RCQueuePair> {
        if domain.as_ptr().is_null() {
            anyhow::bail!("cannot create a queue pair on a null protection domain");
        }
        let mut config = config.clone();
        fit_sge_to_device(&mut config, domain.device_info().max_sge());
        // SAFETY: `domain` holds a live PD (null was rejected above) and `cq` a
        // live queue on its context, per this method's contract, which is what
        // `IbvQueuePair::new` requires.
        unsafe { RCQueuePair::new(domain, config, Arc::clone(&cq), cq) }
    }
}
