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
//! transport-specific work to a NIC backend and [`TcpManagerActor`].
//!
//! ## Responsibilities
//!
//! - Assigns a unique `remote_buf_id` to each registered local memory handle
//!   and stores the [`KeepaliveLocalMemory`] for later retrieval.
//! - Produces [`RdmaRemoteBuffer`] tokens that can be sent to remote peers so
//!   they can address this buffer over RDMA.
//! - Delegates MR registration, QP management, and data movement to a NIC
//!   backend when available, or falls back to the TCP backend
//!   ([`TcpManagerActor`]).
//! - Handles remote [`ReleaseBuffer`] requests to clean up registrations.

use std::collections::HashMap;
use std::sync::OnceLock;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
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
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::backend::RdmaRemoteBackendContext;
use crate::backend::ibverbs::primitives::IbvConfig;
use crate::backend::tcp::manager_actor::TcpManagerActor;
use crate::local_memory::KeepaliveLocalMemory;
use crate::nic::NicBackendHandle;
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
/// These messages carry [`KeepaliveLocalMemory`] and are therefore not
/// serializable -- they can only be sent within the same process.
#[derive(Handler, HandleClient, Debug)]
pub enum RdmaManagerMessage {
    /// Register a local memory handle and return a [`RdmaRemoteBuffer`] that
    /// remote peers can use to address this buffer over RDMA.
    RequestBuffer {
        local: KeepaliveLocalMemory,
        #[reply]
        reply: OncePortHandle<RdmaRemoteBuffer>,
    },
    /// Look up the local memory handle for a given `remote_buf_id`. Returns
    /// `None` if the id does not correspond to a registered buffer.
    RequestLocalMemory {
        remote_buf_id: usize,
        #[reply]
        reply: OncePortHandle<Option<KeepaliveLocalMemory>>,
    },
    /// Return an in-process handle to the local NIC backend, if any.
    GetNicBackendHandle {
        #[reply]
        reply: OncePortHandle<Option<NicBackendHandle>>,
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

/// Serializable query for resolving the [`TcpManagerActor`] ref
/// from a remote [`RdmaManagerActor`].
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub struct GetTcpActorRef {
    #[reply]
    pub reply: OncePortRef<ActorRef<TcpManagerActor>>,
}
wirevalue::register_type!(GetTcpActorRef);

#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        GetTcpActorRef,
        ReleaseBuffer,
    ],
)]
#[hyperactor::spawnable]
pub struct RdmaManagerActor {
    next_remote_buf_id: usize,
    buffers: HashMap<usize, KeepaliveLocalMemory>,
    params: Option<IbvConfig>,
    nic: OnceLock<NicBackendHandle>,
    tcp: OnceLock<ActorHandle<TcpManagerActor>>,
}

impl RdmaManagerActor {
    /// Construct an [`ActorHandle`] for the [`RdmaManagerActor`] co-located
    /// with the caller.
    pub fn local_handle(client: &impl context::Actor) -> ActorHandle<Self> {
        let actor_ref = ActorRef::attest(
            client
                .mailbox()
                .actor_addr()
                .proc_addr()
                .actor_addr("rdma_manager"),
        );
        actor_ref
            .downcast_handle(client)
            .expect("RdmaManagerActor is not in the local process")
    }
}

#[async_trait]
impl RemoteSpawn for RdmaManagerActor {
    type Params = Option<IbvConfig>;

    async fn new(params: Self::Params, _environment: Flattrs) -> Result<Self, anyhow::Error> {
        Ok(Self {
            next_remote_buf_id: 0,
            buffers: HashMap::new(),
            params,
            nic: OnceLock::new(),
            tcp: OnceLock::new(),
        })
    }
}

#[async_trait]
impl Actor for RdmaManagerActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // A detected NIC that fails to initialize is fatal unless TCP
        // fallback is enabled, in which case we warn and fall back to TCP.
        let mut nic_init_failure = None;
        let nic = match NicBackendHandle::spawn(this, self.params.clone()).await {
            Ok(nic) => nic,
            Err(e) => {
                nic_init_failure = Some(e);
                None
            }
        };
        if let Some(nic) = nic {
            self.nic.set(nic).expect("nic set once");
        } else if !hyperactor_config::global::get(crate::config::RDMA_ALLOW_TCP_FALLBACK) {
            if let Some(e) = nic_init_failure {
                anyhow::bail!(
                    "RDMA NIC backend initialization failed and TCP fallback is disabled: {e}"
                );
            } else {
                anyhow::bail!("no RDMA NIC backend available and TCP fallback is disabled")
            }
        } else if let Some(e) = nic_init_failure {
            tracing::warn!(
                "RDMA NIC backend initialization failed, but TCP fallback is enabled: {e}"
            );
        }
        self.tcp
            .set(this.spawn(TcpManagerActor::new()))
            .expect("tcp set once");
        Ok(())
    }
}

#[async_trait]
#[hyperactor::handle(GetTcpActorRef)]
impl GetTcpActorRefHandler for RdmaManagerActor {
    async fn get_tcp_actor_ref(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<ActorRef<TcpManagerActor>, anyhow::Error> {
        Ok(self.tcp.get().expect("tcp set in init").bind())
    }
}

#[async_trait]
#[hyperactor::handle(ReleaseBuffer)]
impl ReleaseBufferHandler for RdmaManagerActor {
    async fn release_buffer(&mut self, cx: &Context<Self>, id: usize) -> Result<(), anyhow::Error> {
        self.buffers.remove(&id);
        if let Some(nic) = self.nic.get() {
            nic.release_buffer(cx, id).await?;
        }
        Ok(())
    }
}

#[async_trait]
#[hyperactor::handle(RdmaManagerMessage)]
impl RdmaManagerMessageHandler for RdmaManagerActor {
    async fn request_buffer(
        &mut self,
        cx: &Context<Self>,
        local: KeepaliveLocalMemory,
    ) -> Result<RdmaRemoteBuffer, anyhow::Error> {
        let remote_buf_id = self.next_remote_buf_id;
        self.next_remote_buf_id += 1;
        let size = local.size();

        let mut backends = Vec::new();
        if let Some(nic) = self.nic.get() {
            let ctx = nic
                .register_remote_buffer(cx, remote_buf_id, local.clone())
                .await?;
            backends.push(RdmaRemoteBackendContext::Nic(ctx));
        }
        self.buffers.insert(remote_buf_id, local);
        backends.push(RdmaRemoteBackendContext::Tcp(
            self.tcp.get().expect("tcp set in init").bind(),
        ));

        Ok(RdmaRemoteBuffer {
            id: remote_buf_id,
            size,
            owner: cx.bind().clone(),
            backends,
        })
    }

    async fn request_local_memory(
        &mut self,
        _cx: &Context<Self>,
        remote_buf_id: usize,
    ) -> Result<Option<KeepaliveLocalMemory>, anyhow::Error> {
        Ok(self.buffers.get(&remote_buf_id).cloned())
    }

    async fn get_nic_backend_handle(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<Option<NicBackendHandle>, anyhow::Error> {
        Ok(self.nic.get().cloned())
    }
}
