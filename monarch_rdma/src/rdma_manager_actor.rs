/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # RDMA Manager Actor
//!
//! Manages RDMA connections and operations using `hyperactor` for asynchronous messaging.
//!
//! ## Architecture
//!
//! `RdmaManagerActor` is a per-host entity that delegates to backend-specific
//! managers (currently `IbvManagerActor`) for the actual RDMA operations.
//!
//! ## Core Operations
//!
//! - Connection establishment with partner actors
//! - RDMA operations (put/write, get/read)
//! - Completion polling
//! - Memory region management
//!
//! ## Usage
//!
//! See test examples: `test_rdma_write_loopback` and `test_rdma_read_loopback`.

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::backend::ibverbs::manager_actor::IbvManagerActor;
use crate::backend::ibverbs::manager_actor::IbvManagerMessageClient;
use crate::backend::ibverbs::primitives::IbvConfig;
use crate::backend::ibverbs::primitives::IbvQpInfo;
use crate::rdma_components::RdmaBuffer;
use crate::rdma_components::RdmaQueuePair;

/// Helper function to get detailed error messages from RDMAXCEL error codes
pub fn get_rdmaxcel_error_message(error_code: i32) -> String {
    unsafe {
        let c_str = rdmaxcel_sys::rdmaxcel_error_string(error_code);
        std::ffi::CStr::from_ptr(c_str)
            .to_string_lossy()
            .into_owned()
    }
}

/// Represents a reference to a remote RDMA buffer that can be accessed via RDMA operations.
/// This struct encapsulates all the information needed to identify and access a memory region
/// on a remote host using RDMA.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum RdmaManagerMessage {
    RequestBuffer {
        addr: usize,
        size: usize,
        #[reply]
        /// `reply` - Reply channel to return the RDMA buffer handle
        reply: OncePortRef<RdmaBuffer>,
    },
    ReleaseBuffer {
        buffer: RdmaBuffer,
    },
    RequestQueuePair {
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<RdmaQueuePair>,
    },
    Connect {
        /// `other` - The ActorId of the actor to connect to
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        /// `endpoint` - Connection information needed to establish the RDMA connection
        endpoint: IbvQpInfo,
    },
    InitializeQP {
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<bool>,
    },
    ConnectionInfo {
        /// `other` - The ActorId to get connection info for
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the connection info
        reply: OncePortRef<IbvQpInfo>,
    },
    ReleaseQueuePair {
        /// `other` - The ActorId to release queue pair for
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        /// `qp` - The queue pair to return (ownership transferred back)
        qp: RdmaQueuePair,
    },
    GetQpState {
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        #[reply]
        /// `reply` - Reply channel to return the QP state
        reply: OncePortRef<u32>,
    },
}
wirevalue::register_type!(RdmaManagerMessage);

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
        RdmaManagerMessage,
    ],
)]
pub struct RdmaManagerActor {
    // Eventually, this will be an Option, but for now
    // to maintain existing behavior, we assume that
    // RdmaManagerActor only exists if ibverbs is supported.
    // This will change very soon.
    ibverbs: RdmaBackendActor<IbvManagerActor>,
}

#[async_trait]
impl RemoteSpawn for RdmaManagerActor {
    type Params = Option<IbvConfig>;

    async fn new(params: Self::Params, _environment: Flattrs) -> Result<Self, anyhow::Error> {
        let ibv = RdmaBackendActor::Created(IbvManagerActor::new(params).await?);
        Ok(Self { ibverbs: ibv })
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
#[hyperactor::handle(RdmaManagerMessage)]
impl RdmaManagerMessageHandler for RdmaManagerActor {
    async fn request_buffer(
        &mut self,
        cx: &Context<Self>,
        addr: usize,
        size: usize,
    ) -> Result<RdmaBuffer, anyhow::Error> {
        self.ibverbs
            .handle()
            .request_buffer(cx, cx.bind().clone(), addr, size)
            .await
    }

    async fn release_buffer(
        &mut self,
        cx: &Context<Self>,
        buffer: RdmaBuffer,
    ) -> Result<(), anyhow::Error> {
        self.ibverbs.handle().release_buffer(cx, buffer).await
    }

    async fn request_queue_pair(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<RdmaQueuePair, anyhow::Error> {
        self.ibverbs
            .handle()
            .request_queue_pair(cx, cx.bind().clone(), other, self_device, other_device)
            .await
    }

    async fn initialize_qp(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<bool, anyhow::Error> {
        self.ibverbs
            .handle()
            .initialize_qp(cx, other, self_device, other_device)
            .await
    }

    async fn connect(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        endpoint: IbvQpInfo,
    ) -> Result<(), anyhow::Error> {
        self.ibverbs
            .handle()
            .connect(cx, other, self_device, other_device, endpoint)
            .await
    }

    async fn connection_info(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<IbvQpInfo, anyhow::Error> {
        self.ibverbs
            .handle()
            .connection_info(cx, other, self_device, other_device)
            .await
    }

    async fn release_queue_pair(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
        qp: RdmaQueuePair,
    ) -> Result<(), anyhow::Error> {
        self.ibverbs
            .handle()
            .release_queue_pair(cx, other, self_device, other_device, qp)
            .await
    }

    async fn get_qp_state(
        &mut self,
        cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        self_device: String,
        other_device: String,
    ) -> Result<u32, anyhow::Error> {
        self.ibverbs
            .handle()
            .get_qp_state(cx, other, self_device, other_device)
            .await
    }
}
