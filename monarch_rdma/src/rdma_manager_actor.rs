/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # RDMA Manager Actor
//!
//! Per-process actor that owns RDMA buffer registrations and delegates
//! transport-specific work to backend actors (currently [`IbvManagerActor`]).
//!
//! ## Responsibilities
//!
//! - Assigns a unique `remote_buf_id` to each registered local memory handle
//!   and stores the `Arc<dyn RdmaLocalMemory>` for later retrieval.
//! - Produces [`RdmaRemoteBuffer`] tokens that can be sent to remote peers so
//!   they can address this buffer over RDMA.
//! - Delegates MR registration, QP management, and data movement to the
//!   ibverbs backend ([`IbvManagerActor`]).
//! - Handles remote [`ReleaseBuffer`] requests to clean up registrations.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::context;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OnceCell;
use typeuri::Named;

use crate::RdmaLocalMemory;
use crate::backend::RdmaBackendContext;
use crate::backend::ibverbs::manager_actor::IbvManagerActor;
use crate::backend::ibverbs::manager_actor::IbvManagerMessageClient;
use crate::backend::ibverbs::primitives::IbvConfig;
use crate::rdma_components::RdmaRemoteBuffer;

/// Helper function to get detailed error messages from RDMAXCEL error codes
pub fn get_rdmaxcel_error_message(error_code: i32) -> String {
    unsafe {
        let c_str = rdmaxcel_sys::rdmaxcel_error_string(error_code);
        std::ffi::CStr::from_ptr(c_str)
            .to_string_lossy()
            .into_owned()
    }
}

/// Local-only messages for the [`RdmaManagerActor`].
///
/// These messages carry `Arc<dyn RdmaLocalMemory>` and are therefore
/// not serializable — they can only be sent within the same process.
#[derive(Handler, HandleClient, Debug)]
pub enum RdmaManagerMessage {
    /// Register a local memory handle and return a [`RdmaRemoteBuffer`] that
    /// remote peers can use to address this buffer over RDMA.
    RequestBuffer {
        local: Arc<dyn RdmaLocalMemory>,
        #[reply]
        reply: OncePortHandle<RdmaRemoteBuffer>,
    },
    /// Look up the local memory handle for a given `remote_buf_id`. Returns
    /// `None` if the id does not correspond to a registered buffer.
    RequestLocalMemory {
        remote_buf_id: usize,
        #[reply]
        reply: OncePortHandle<Option<Arc<dyn RdmaLocalMemory>>>,
    },
}

/// Serializable release message for wire transport.
///
/// Used by [`RdmaRemoteBuffer::drop_buffer`] to release a buffer
/// from a remote process.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub struct ReleaseBuffer {
    pub id: usize,
}
wirevalue::register_type!(ReleaseBuffer);

/// Serializable query for resolving the [`IbvManagerActor`] ref
/// from a remote [`RdmaManagerActor`]. Only used in testing.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub struct GetIbvActorRef {
    #[reply]
    pub reply: OncePortRef<Option<ActorRef<IbvManagerActor>>>,
}
wirevalue::register_type!(GetIbvActorRef);

#[derive(Debug)]
enum RdmaBackendActor<A: Actor> {
    Uninit,
    Created(A),
    Spawned(ActorHandle<A>),
}

impl<A: Actor> RdmaBackendActor<A> {
    fn spawn(&mut self, rdma_manager: &Instance<RdmaManagerActor>) -> anyhow::Result<()> {
        let created = std::mem::replace(self, RdmaBackendActor::Uninit);
        let actor = if let RdmaBackendActor::Created(actor) = created {
            actor
        } else {
            panic!("rdma backend actor already spawned");
        };
        let handle = rdma_manager.spawn(actor)?;
        *self = RdmaBackendActor::Spawned(handle);
        Ok(())
    }

    fn handle(&self) -> &ActorHandle<A> {
        if let RdmaBackendActor::Spawned(handle) = self {
            handle
        } else {
            panic!("cannot get handle")
        }
    }
}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        GetIbvActorRef,
        ReleaseBuffer,
    ],
)]
pub struct RdmaManagerActor {
    next_remote_buf_id: usize,
    buffers: HashMap<usize, Arc<dyn RdmaLocalMemory>>,
    ibverbs: RdmaBackendActor<IbvManagerActor>,
}

impl RdmaManagerActor {
    /// Construct an [`ActorHandle`] for the [`RdmaManagerActor`] co-located
    /// with the caller.
    pub fn local_handle(client: &impl context::Actor) -> ActorHandle<Self> {
        let proc_id = client.mailbox().actor_id().0.clone();
        let actor_ref = ActorRef::attest(ActorId(proc_id, "rdma_manager".to_string(), 0));
        actor_ref
            .downcast_handle(client)
            .expect("RdmaManagerActor is not in the local process")
    }
}

#[async_trait]
impl RemoteSpawn for RdmaManagerActor {
    type Params = Option<IbvConfig>;

    async fn new(params: Self::Params, _environment: Flattrs) -> Result<Self, anyhow::Error> {
        let ibv = RdmaBackendActor::Created(IbvManagerActor::new(params).await?);
        Ok(Self {
            next_remote_buf_id: 0,
            buffers: HashMap::new(),
            ibverbs: ibv,
        })
    }
}

#[async_trait]
impl Actor for RdmaManagerActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        self.ibverbs.spawn(this)?;
        tracing::debug!("RdmaManagerActor initialized with lazy domain/QP creation");
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        _cx: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        tracing::error!("rdmaManagerActor supervision event: {:?}", _event);
        tracing::error!("rdmaManagerActor error occurred, stop the worker process, exit code: 1");
        std::process::exit(1);
    }
}

#[async_trait]
#[hyperactor::handle(GetIbvActorRef)]
impl GetIbvActorRefHandler for RdmaManagerActor {
    async fn get_ibv_actor_ref(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<Option<ActorRef<IbvManagerActor>>, anyhow::Error> {
        Ok(Some(self.ibverbs.handle().bind()))
    }
}

#[async_trait]
#[hyperactor::handle(ReleaseBuffer)]
impl ReleaseBufferHandler for RdmaManagerActor {
    async fn release_buffer(&mut self, cx: &Context<Self>, id: usize) -> Result<(), anyhow::Error> {
        self.buffers.remove(&id);
        self.ibverbs.handle().release_buffer(cx, id).await
    }
}

#[async_trait]
#[hyperactor::handle(RdmaManagerMessage)]
impl RdmaManagerMessageHandler for RdmaManagerActor {
    async fn request_buffer(
        &mut self,
        cx: &Context<Self>,
        local: Arc<dyn RdmaLocalMemory>,
    ) -> Result<RdmaRemoteBuffer, anyhow::Error> {
        let remote_buf_id = self.next_remote_buf_id;
        self.next_remote_buf_id += 1;
        let size = local.size();

        self.buffers.insert(remote_buf_id, local);

        Ok(RdmaRemoteBuffer {
            id: remote_buf_id,
            size,
            owner: cx.bind().clone(),
            backends: vec![RdmaBackendContext::Ibverbs(
                self.ibverbs.handle().bind(),
                Arc::new(OnceCell::new()),
            )],
        })
    }

    async fn request_local_memory(
        &mut self,
        _cx: &Context<Self>,
        remote_buf_id: usize,
    ) -> Result<Option<Arc<dyn RdmaLocalMemory>>, anyhow::Error> {
        Ok(self.buffers.get(&remote_buf_id).cloned())
    }
}
