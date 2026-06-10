/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! NIC transport backends for RDMA.

use std::time::Duration;

use anyhow::Result;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Instance;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::RdmaOp;
use crate::backend::RdmaBackend;
use crate::backend::ibverbs::IbvBuffer;
use crate::backend::ibverbs::device::IbvDevice;
use crate::backend::ibverbs::efa_device::EfaDevice;
use crate::backend::ibverbs::manager_actor::IbvBackend;
use crate::backend::ibverbs::manager_actor::IbvManagerActor;
use crate::backend::ibverbs::manager_actor::IbvManagerLocalMessageClient;
use crate::backend::ibverbs::manager_actor::IbvManagerMessageClient;
use crate::backend::ibverbs::mlx_device::MlxDevice;
use crate::backend::ibverbs::primitives::IbvConfig;
use crate::local_memory::KeepaliveLocalMemory;

/// Backend context carried by [`crate::RdmaRemoteBuffer`] for a
/// buffer reachable over a NIC.
#[derive(Debug, Clone, Serialize, Deserialize, Named, EnumAsInner)]
pub enum NicRemoteBackendContext {
    Mlx(ActorRef<IbvManagerActor<MlxDevice>>, IbvBuffer),
    Efa(ActorRef<IbvManagerActor<EfaDevice>>, IbvBuffer),
}
wirevalue::register_type!(NicRemoteBackendContext);

impl NicRemoteBackendContext {
    /// Whether `handle` drives the same backend as this context.
    pub(crate) fn is_compatible_with(&self, handle: &NicBackendHandle) -> bool {
        matches!(
            (self, handle),
            (NicRemoteBackendContext::Mlx(..), NicBackendHandle::Mlx(_))
                | (NicRemoteBackendContext::Efa(..), NicBackendHandle::Efa(_))
        )
    }
}

/// In-process handle to a NIC backend.
#[derive(Debug, Clone)]
pub enum NicBackendHandle {
    Mlx(IbvBackend<MlxDevice>),
    Efa(IbvBackend<EfaDevice>),
}

impl NicBackendHandle {
    /// Spawn this proc's NIC backend, if one is available. Returns
    /// `None`, after logging the reason, when no NIC backend can be
    /// spawned; returns `Err` only when an available backend fails to
    /// initialize.
    pub(crate) async fn spawn(
        this: &Instance<impl Actor>,
        params: Option<IbvConfig>,
    ) -> Result<Option<NicBackendHandle>> {
        if hyperactor_config::global::get(crate::config::RDMA_DISABLE_IBVERBS) {
            tracing::warn!("ibverbs disabled by configuration");
            return Ok(None);
        }
        if IbvDevice::<MlxDevice>::available() {
            let actor = IbvManagerActor::<MlxDevice>::new(params).await?;
            return Ok(Some(NicBackendHandle::Mlx(IbvBackend(this.spawn(actor)))));
        }
        if IbvDevice::<EfaDevice>::available() {
            let actor = IbvManagerActor::<EfaDevice>::new(params).await?;
            return Ok(Some(NicBackendHandle::Efa(IbvBackend(this.spawn(actor)))));
        }
        tracing::warn!("no RDMA NIC devices found");
        Ok(None)
    }

    /// Register `local` for remote access and return its backend context.
    pub(crate) async fn register_remote_buffer(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        remote_buf_id: usize,
        local: KeepaliveLocalMemory,
    ) -> Result<NicRemoteBackendContext> {
        match self {
            NicBackendHandle::Mlx(backend) => {
                let buf = backend
                    .register_remote_buffer(cx, remote_buf_id, local)
                    .await?
                    .map_err(|e| anyhow::anyhow!(e))?;
                Ok(NicRemoteBackendContext::Mlx(backend.bind(), buf))
            }
            NicBackendHandle::Efa(backend) => {
                let buf = backend
                    .register_remote_buffer(cx, remote_buf_id, local)
                    .await?
                    .map_err(|e| anyhow::anyhow!(e))?;
                Ok(NicRemoteBackendContext::Efa(backend.bind(), buf))
            }
        }
    }

    /// Release a buffer registration by id.
    pub(crate) async fn release_buffer(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        remote_buf_id: usize,
    ) -> Result<()> {
        match self {
            NicBackendHandle::Mlx(backend) => backend.release_buffer(cx, remote_buf_id).await,
            NicBackendHandle::Efa(backend) => backend.release_buffer(cx, remote_buf_id).await,
        }
    }

    /// Submit a batch of RDMA ops to this backend.
    pub(crate) async fn submit(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<()> {
        match self {
            NicBackendHandle::Mlx(backend) => backend.clone().submit(cx, ops, timeout).await,
            NicBackendHandle::Efa(backend) => backend.clone().submit(cx, ops, timeout).await,
        }
    }
}
