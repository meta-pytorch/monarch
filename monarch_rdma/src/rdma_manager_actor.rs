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
//! `RdmaManagerActor` is a per-host entity that:
//! - Manages connections to multiple remote RdmaManagerActors (i.e. across the hosts in a Monarch cluster)
//! - Handles memory registration, connection setup, and data transfer
//! - Manages all RdmaBuffers in its associated host
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
use std::collections::HashMap;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;

use crate::ibverbs_primitives::IbverbsConfig;
use crate::ibverbs_primitives::RdmaQpInfo;
use crate::rdma_components::RdmaBuffer;
use crate::rdma_components::RdmaDomain;
use crate::rdma_components::RdmaQueuePair;
use crate::validate_execution_context;

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
        remote: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<RdmaQueuePair>,
    },
    IsConnected {
        /// `other` - The ActorId of the actor to check connection with
        other: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return whether the actors have connected
        reply: OncePortRef<bool>,
    },
    Connect {
        /// `other` - The ActorId of the actor to connect to
        other: ActorRef<RdmaManagerActor>,
        /// `endpoint` - Connection information needed to establish the RDMA connection
        endpoint: RdmaQpInfo,
    },
    InitializeQP {
        remote: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return the queue pair for communication
        reply: OncePortRef<bool>,
    },
    ConnectionInfo {
        /// `other` - The ActorId to get connection info for
        other: ActorRef<RdmaManagerActor>,
        #[reply]
        /// `reply` - Reply channel to return connection information needed for the RDMA connection
        reply: OncePortRef<RdmaQpInfo>,
    },
}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        RdmaManagerMessage,
    ],
)]
pub struct RdmaManagerActor {
    // Map between ActorIds and their corresponding RdmaQueuePair
    qp_map: HashMap<ActorId, RdmaQueuePair>,

    // The RDMA domain associated with this actor.
    //
    // The domain is responsible for managing the RDMA resources and configurations
    // specific to this actor. It encapsulates the context and protection domain
    // necessary for RDMA operations, ensuring that all RDMA activities are
    // performed within a consistent and isolated environment.
    //
    // This domain is initialized during the creation of the `RdmaManagerActor`
    // and is used throughout the actor's lifecycle to manage RDMA connections
    // and operations.
    domain: RdmaDomain,
    config: IbverbsConfig,
}

#[async_trait]
impl Actor for RdmaManagerActor {
    type Params = Option<IbverbsConfig>;

    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        // Use provided config or default if none provided
        let mut config = params.unwrap_or_default();
        tracing::debug!("rdma is enabled, using device {}", config.device);

        // check config and hardware support align
        if config.use_gpu_direct {
            match validate_execution_context().await {
                Ok(_) => {
                    tracing::info!("GPU Direct RDMA execution context validated successfully");
                }
                Err(e) => {
                    tracing::warn!(
                        "GPU Direct RDMA execution context validation failed: {}. Downgrading to standard ibverbs mode.",
                        e
                    );
                    config.use_gpu_direct = false;
                }
            }
        }

        let domain = RdmaDomain::new(config.device.clone())
            .map_err(|e| anyhow::anyhow!("rdmaManagerActor could not create domain: {}", e))?;
        Ok(Self {
            qp_map: HashMap::new(),
            domain,
            config,
        })
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
#[hyperactor::forward(RdmaManagerMessage)]
impl RdmaManagerMessageHandler for RdmaManagerActor {
    /// Requests a buffer to be registered with the RDMA domain.
    ///
    /// This function registers a memory region with the RDMA domain and returns an `RdmaBuffer`
    /// that encapsulates the necessary information for RDMA operations.
    ///
    /// # Arguments
    ///
    /// * `this` - The context of the actor requesting the buffer.
    /// * `addr` - The starting address of the memory region to be registered.
    /// * `size` - The size of the memory region to be registered.
    ///
    /// # Returns
    ///
    /// * `Result<RdmaBuffer, anyhow::Error>` - On success, returns an `RdmaBuffer` containing
    ///   the registered memory region's details. On failure, returns an error.
    async fn request_buffer(
        &mut self,
        cx: &Context<Self>,
        addr: usize,
        size: usize,
    ) -> Result<RdmaBuffer, anyhow::Error> {
        let mr = self.domain.register_buffer(addr, size)?;
        Ok(RdmaBuffer {
            owner: cx.bind().clone(),
            mr_id: mr.id,
            addr: mr.addr,
            size: mr.size,
            rkey: mr.rkey,
            lkey: mr.lkey,
        })
    }

    /// Deregisters a buffer from the RDMA domain.
    ///
    /// This function removes the specified `RdmaBuffer` from the RDMA domain,
    /// effectively releasing the resources associated with it.
    ///
    /// # Arguments
    ///
    /// * `_this` - The context of the actor releasing the buffer.
    /// * `buffer` - The `RdmaBuffer` to be deregistered.
    ///
    /// # Returns
    ///
    /// * `Result<(), anyhow::Error>` - On success, returns `Ok(())`. On failure, returns an error.
    async fn release_buffer(
        &mut self,
        _cx: &Context<Self>,
        buffer: RdmaBuffer,
    ) -> Result<(), anyhow::Error> {
        self.domain
            .deregister_buffer(buffer)
            .map_err(|e| anyhow::anyhow!("could not deregister buffer: {}", e))?;
        Ok(())
    }

    /// Requests a queue pair for communication with a remote RDMA manager actor.
    ///
    /// This function checks if a connection already exists with the specified remote actor.
    /// If not, it initializes a new queue pair and establishes a connection with the remote actor.
    /// It then retrieves the queue pair associated with the remote actor for communication.
    ///
    /// # Arguments
    ///
    /// * `this` - The context of the actor requesting the queue pair.
    /// * `remote` - The ActorRef of the remote RDMA manager actor to communicate with.
    ///
    /// # Returns
    ///
    /// * `Result<RdmaQueuePair, anyhow::Error>` - On success, returns the queue pair for communication.
    ///   On failure, returns an error.
    async fn request_queue_pair(
        &mut self,
        cx: &Context<Self>,
        remote: ActorRef<RdmaManagerActor>,
    ) -> Result<RdmaQueuePair, anyhow::Error> {
        if !self.is_connected(cx, remote.clone()).await? {
            let is_loopback =
                remote.actor_id().clone() == cx.bind::<RdmaManagerActor>().actor_id().clone();

            if is_loopback {
                self.initialize_qp(cx, remote.clone()).await?;
                let endpoint = self.connection_info(cx, remote.clone()).await?;
                self.connect(cx, remote.clone(), endpoint).await?;
            } else {
                self.initialize_qp(cx, remote.clone()).await?;
                remote.initialize_qp(cx, cx.bind().clone()).await?;
                let remote_endpoint = remote.connection_info(cx, cx.bind().clone()).await?;
                self.connect(cx, remote.clone(), remote_endpoint).await?;
                let local_endpoint = self.connection_info(cx, remote.clone()).await?;
                remote
                    .connect(cx, cx.bind().clone(), local_endpoint)
                    .await?;
            }
        }

        let qp = self
            .qp_map
            .get_mut(&remote.actor_id().clone())
            .ok_or_else(|| anyhow::anyhow!("on get, no connection found for actor {}", remote))?;
        Ok(qp.clone())
    }

    /// Convenience utility to create a new RdmaQueuePair.
    ///
    /// This function initializes a new RDMA connection with another actor if one doesn't already exist.
    /// It creates a new RdmaQueuePair associated with the specified actor ID and adds it to the
    /// connection map.
    ///
    /// # Arguments
    ///
    /// * `other` - The ActorRef of the remote actor to connect with
    async fn initialize_qp(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
    ) -> Result<bool, anyhow::Error> {
        let key = other.actor_id().clone();

        if let std::collections::hash_map::Entry::Vacant(e) = self.qp_map.entry(key) {
            let qp = RdmaQueuePair::new(self.domain.context, self.domain.pd, self.config.clone())
                .map_err(|e| anyhow::anyhow!("could not create RdmaQueuePair: {}", e))?;
            e.insert(qp);
            tracing::debug!("successfully created a connection with {:?}", other);
        }
        Ok(true)
    }

    /// Checks if a connection exists with another actor.
    ///
    /// # Arguments
    /// * `other` - The ActorRef of the actor to check the connection with.
    ///
    /// # Returns
    /// * `bool` - Returns true if connected, false otherwise.
    async fn is_connected(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
    ) -> Result<bool, anyhow::Error> {
        tracing::debug!("checking if connected with {:?}", other);
        if !self.qp_map.contains_key(&other.actor_id().clone()) {
            return Ok(false);
        }
        let qp_state = self
            .qp_map
            .get_mut(&other.actor_id().clone())
            .unwrap()
            .state()?;
        Ok(qp_state == rdmaxcel_sys::ibv_qp_state::IBV_QPS_RTS)
    }

    /// Establishes a connection with another actor
    ///
    /// # Arguments
    /// * `other` - The ActorRef of the actor to connect to
    /// * `endpoint` - Connection information needed to establish the RDMA connection
    async fn connect(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
        endpoint: RdmaQpInfo,
    ) -> Result<(), anyhow::Error> {
        tracing::debug!("connecting with {:?}", other);
        let qp = self
            .qp_map
            .get_mut(&other.actor_id().clone())
            .ok_or_else(|| {
                anyhow::anyhow!("on connect, no connection found for actor {}", other)
            })?;
        qp.connect(&endpoint)
            .map_err(|e| anyhow::anyhow!("could not connect to RDMA endpoint: {}", e))?;
        Ok(())
    }

    /// Gets connection information for establishing an RDMA connection
    ///
    /// # Arguments
    /// * `other` - The ActorRef to get connection info for
    ///
    /// # Returns
    /// * `RdmaQpInfo` - Connection information needed for the RDMA connection
    async fn connection_info(
        &mut self,
        _cx: &Context<Self>,
        other: ActorRef<RdmaManagerActor>,
    ) -> Result<RdmaQpInfo, anyhow::Error> {
        tracing::debug!("getting connection info with {:?}", other);

        let connection_info = self
            .qp_map
            .get_mut(&other.actor_id().clone())
            .ok_or_else(|| anyhow::anyhow!("no connection found for actor {}", other))?
            .get_qp_info()?;
        Ok(connection_info)
    }
}
