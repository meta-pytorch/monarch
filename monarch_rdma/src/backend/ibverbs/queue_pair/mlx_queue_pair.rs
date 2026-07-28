/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! mlx5 queue pair built on the mlx5dv extended verbs.

use std::io::Error;
use std::result::Result;

use super::IbvQueuePair;
use super::PollCompletionError;
use super::PollTarget;
use super::WorkRequestError;
use super::rc_queue_pair::RCQueuePair;
use crate::backend::ibverbs::IbvBuffer;
use crate::backend::ibverbs::domain::IbvDomain;
use crate::backend::ibverbs::domain::IbvDomainImpl;
use crate::backend::ibverbs::primitives::GidScope;
use crate::backend::ibverbs::primitives::GidType;
use crate::backend::ibverbs::primitives::IbvConfig;
use crate::backend::ibverbs::primitives::IbvCq;
use crate::backend::ibverbs::primitives::IbvQp;
use crate::backend::ibverbs::primitives::IbvQpInfo;
use crate::backend::ibverbs::primitives::IbvWc;

/// An mlx5 RC queue pair created through `mlx5dv_create_qp`, so it carries the
/// mlx5dv send-ops flags that arm a direct-WQE/doorbell data path.
///
/// Operations delegate to an inner [`RCQueuePair`], driving the QP
/// through the standard `ibv_post_send`/`ibv_poll_cq` verbs; the mlx5dv
/// send-ops flags are not exercised by that path.
#[derive(Debug)]
pub struct MlxQueuePair(RCQueuePair);

impl MlxQueuePair {
    /// Creates the `mlx5dv` RC QP and the two completion queues backing it,
    /// against `domain`'s context and PD, returned as an [`IbvQp`] that owns the
    /// CQs and PD. The QP carries the mlx5dv send-ops flags that arm its
    /// extended work-request builder. A null context or PD on `domain` yields
    /// `Err`.
    ///
    /// # Safety
    ///
    /// `domain`'s context and PD, if non-null, must be live.
    pub(in crate::backend::ibverbs) unsafe fn create_ibv_qp<I: IbvDomainImpl>(
        domain: &IbvDomain<I>,
        config: &IbvConfig,
    ) -> Result<IbvQp, anyhow::Error> {
        let context = domain.context().as_ptr();
        let pd = domain.as_ptr();
        if pd.is_null() {
            anyhow::bail!("cannot create an MlxQueuePair on a null protection domain");
        }

        // Separate send/recv completion queues, each owning a clone of the
        // device context. Each `IbvCq` destroys its queue on drop, so an early
        // return below cleans them up.
        // SAFETY: `domain`'s context is live (an `IbvDomain` holds a null-or-live
        // context); `IbvCq::create` rejects a null context.
        let send_cq = unsafe { IbvCq::create(domain.context().clone(), config.cq_entries) }?;
        // SAFETY: as for `send_cq` above.
        let recv_cq = unsafe { IbvCq::create(domain.context().clone(), config.cq_entries) }?;

        // An mlx5dv extended RC QP with the caps from `config`. The
        // `SEND_OPS_FLAGS` enable the mlx5dv extended work-request builder; the
        // standard verbs this QP runs on ignore them.
        let mut init_attr = rdmaxcel_sys::ibv_qp_init_attr_ex {
            send_cq: send_cq.as_ptr(),
            recv_cq: recv_cq.as_ptr(),
            cap: rdmaxcel_sys::ibv_qp_cap {
                max_send_wr: config.max_send_wr,
                max_recv_wr: config.max_recv_wr,
                max_send_sge: config.max_send_sge,
                max_recv_sge: config.max_recv_sge,
                max_inline_data: 0,
            },
            qp_type: rdmaxcel_sys::ibv_qp_type::IBV_QPT_RC,
            sq_sig_all: 0,
            pd,
            comp_mask: rdmaxcel_sys::IBV_QP_INIT_ATTR_PD
                | rdmaxcel_sys::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS,
            send_ops_flags: (rdmaxcel_sys::IBV_QP_EX_WITH_RDMA_WRITE
                | rdmaxcel_sys::IBV_QP_EX_WITH_RDMA_READ
                | rdmaxcel_sys::IBV_QP_EX_WITH_SEND) as u64,
            ..Default::default()
        };
        let mut mlx5dv_attr = rdmaxcel_sys::mlx5dv_qp_init_attr {
            comp_mask: rdmaxcel_sys::MLX5DV_QP_INIT_ATTR_MASK_SEND_OPS_FLAGS as u64,
            send_ops_flags: (rdmaxcel_sys::MLX5DV_QP_EX_WITH_MKEY_CONFIGURE
                | rdmaxcel_sys::MLX5DV_QP_EX_WITH_MR_LIST) as u64,
            ..Default::default()
        };
        // SAFETY: `context` and `pd` are non-null (checked above) and live (an
        // `IbvDomain` holds null-or-live pointers); both attr structs are fully
        // initialized and outlive the call, and their CQ pointers came from the
        // freshly created `send_cq`/`recv_cq`. `mlx5dv_create_qp` returns null on
        // failure.
        let qp =
            unsafe { rdmaxcel_sys::mlx5dv_create_qp(context, &mut init_attr, &mut mlx5dv_attr) };
        if qp.is_null() {
            // `send_cq`/`recv_cq` drop here, destroying the CQs.
            anyhow::bail!(
                "failed to create mlx5dv queue pair (QP): {}",
                Error::last_os_error()
            );
        }
        // SAFETY: `qp` is a live RC QP just created against `pd` with
        // `send_cq`/`recv_cq`; `IbvQp` takes ownership of all of them plus the PD
        // and destroys them in order on drop.
        Ok(unsafe { IbvQp::from_raw(qp, send_cq, recv_cq, domain.pd().clone()) })
    }
}

impl IbvQueuePair for MlxQueuePair {
    unsafe fn new<I: IbvDomainImpl<QueuePair = Self>>(
        domain: &IbvDomain<I>,
        config: IbvConfig,
    ) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating an MlxQueuePair from config {}", config);
        let gid = domain.device_info().select_gid(
            config.port_num,
            Some(GidScope::Global),
            Some(GidType::RoCEv2),
        )?;
        // SAFETY: an `IbvDomain` holds a null-or-live context and PD, which is
        // `create_ibv_qp`'s contract.
        let qp = unsafe { Self::create_ibv_qp(domain, &config) }?;
        let access_flags = domain.access_flags();
        // SAFETY: `create_ibv_qp` returns a live, fully non-null `IbvQp` (it
        // bails on any null handle).
        Ok(MlxQueuePair(unsafe {
            RCQueuePair::from_qp(qp, config, gid, access_flags)
        }))
    }

    fn connect(&mut self, info: &IbvQpInfo) -> Result<(), anyhow::Error> {
        self.0.connect(info)
    }

    fn get_qp_info(&mut self) -> Result<IbvQpInfo, anyhow::Error> {
        self.0.get_qp_info()
    }

    fn state(&mut self) -> Result<u32, anyhow::Error> {
        self.0.state()
    }

    fn put(
        &mut self,
        remote_dst: IbvBuffer,
        local_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        self.0.put(remote_dst, local_src)
    }

    fn get(
        &mut self,
        local_dst: IbvBuffer,
        remote_src: IbvBuffer,
    ) -> Result<Vec<u64>, anyhow::Error> {
        self.0.get(local_dst, remote_src)
    }

    fn poll_completion(
        &mut self,
        target: PollTarget,
    ) -> Result<Option<Result<IbvWc, WorkRequestError>>, PollCompletionError> {
        self.0.poll_completion(target)
    }
}
