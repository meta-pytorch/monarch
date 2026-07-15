/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EFA domain strategy for [`IbvDomainImpl`].

use std::collections::HashMap;

use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;

use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::efa_device::EfaDevice;
use super::manager_actor::IbvManagerActor;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::primitives::IbvDeviceInfo;
use super::primitives::IbvQpInfo;
use super::queue_pair::QpKey;
use super::queue_pair::QueuePairActor;
use super::queue_pair::legacy;

/// The raw queue pair EFA builds for now: the legacy single-type QP. It will
/// become an EFA-specific queue pair in a follow-up.
type EfaQueuePair = legacy::IbvQueuePair;

/// EFA [`IbvDomainImpl`]. Uses the default host/dmabuf MR registration; EFA has
/// no device-specific memory-key binding to add.
pub struct EfaDomain {
    /// Active-side QP actors spawned for peers reached through this domain.
    qp_handles:
        HashMap<QpKey, ActorHandle<QueuePairActor<IbvManagerActor<EfaDevice>, EfaQueuePair>>>,
    /// Passive mirror QPs created for peers that RDMA into this domain.
    peer_created_qps: HashMap<QpKey, EfaQueuePair>,
}

impl std::fmt::Debug for EfaDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EfaDomain").finish_non_exhaustive()
    }
}

impl EfaDomain {
    /// Build a fresh, unconnected raw EFA queue pair against `domain`'s PD.
    fn create_raw_qp(domain: &IbvDomain<EfaDomain>) -> anyhow::Result<EfaQueuePair> {
        legacy::IbvQueuePair::new(domain, domain.config().clone())
    }
}

impl IbvDomainImpl for EfaDomain {
    type Device = EfaDevice;
    type QueuePair = QueuePairActor<IbvManagerActor<EfaDevice>, EfaQueuePair>;

    unsafe fn new(
        _context: &IbvContext,
        _device_info: &IbvDeviceInfo,
        _config: &IbvConfig,
    ) -> Self {
        EfaDomain {
            qp_handles: HashMap::new(),
            peer_created_qps: HashMap::new(),
        }
    }

    fn access_flags(&self) -> i32 {
        // EFA does not support `IBV_ACCESS_REMOTE_ATOMIC`.
        (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ)
            .0 as i32
    }

    fn get_or_create_queue_pair(
        domain: &mut IbvDomain<EfaDomain>,
        cx: &hyperactor::Context<IbvManagerActor<EfaDevice>>,
        peer_ref: ActorRef<IbvManagerActor<EfaDevice>>,
        peer_device: String,
    ) -> anyhow::Result<ActorHandle<Self::QueuePair>> {
        let qp_key = QpKey {
            peer_id: peer_ref.actor_addr().id().clone(),
            peer_device,
        };
        if let Some(handle) = domain.domain_impl().qp_handles.get(&qp_key).cloned() {
            return Ok(handle);
        }
        let qp = Self::create_raw_qp(domain)
            .map_err(|e| anyhow::anyhow!("could not create IbvQueuePair for {qp_key:?}: {e}"))?;
        let local_device = domain.device_info().name().to_string();
        let local_manager: ActorRef<IbvManagerActor<EfaDevice>> = cx.bind();
        let is_loopback = local_manager.actor_addr() == peer_ref.actor_addr()
            && local_device == qp_key.peer_device;
        let actor = cx.spawn(QueuePairActor::new(
            qp_key.clone(),
            local_device,
            local_manager,
            peer_ref,
            qp,
            is_loopback,
            domain.config().max_send_wr,
            domain.config().max_rd_atomic as u32,
        ));
        domain
            .domain_impl_mut()
            .qp_handles
            .insert(qp_key, actor.clone());
        Ok(actor)
    }

    fn process_conn_request(
        domain: &mut IbvDomain<EfaDomain>,
        sender_id: ActorId,
        sender_device: String,
        sender_qp_info: &IbvQpInfo,
    ) -> anyhow::Result<IbvQpInfo> {
        let qp_key = QpKey {
            peer_id: sender_id,
            peer_device: sender_device,
        };
        if domain.domain_impl().peer_created_qps.contains_key(&qp_key) {
            anyhow::bail!("queue pair already exists for {qp_key:?}");
        }
        let mut qp = Self::create_raw_qp(domain)
            .map_err(|e| anyhow::anyhow!("could not create queue pair: {e}"))?;
        let local_info = qp
            .get_qp_info()
            .map_err(|e| anyhow::anyhow!("could not extract QP info: {e}"))?;
        qp.connect(sender_qp_info)
            .map_err(|e| anyhow::anyhow!("could not connect QP: {e}"))?;
        domain.domain_impl_mut().peer_created_qps.insert(qp_key, qp);
        Ok(local_info)
    }
}

impl Drop for EfaDomain {
    fn drop(&mut self) {
        // Drain the active-side QP actors before this domain's FFI resources
        // tear down; each finishes its in-flight ops and drops its owned QP.
        for (_key, handle) in self.qp_handles.drain() {
            let _ = handle.drain_and_stop("EfaDomain dropped");
        }
    }
}
