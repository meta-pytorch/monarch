/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RDMA backend implementations.

pub mod ibverbs;
pub mod tcp;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::ActorRef;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OnceCell;

use crate::RdmaOp;
use crate::RdmaTransportLevel;

/// Backend-specific context for a remote buffer.
///
/// Each variant holds the information needed to perform RDMA operations
/// using that backend on a particular buffer.
///
/// The [`OnceCell`] is lazily populated at runtime and excluded from
/// serialization; deserializing produces an empty cell.
#[derive(Debug, Clone)]
pub enum RdmaRemoteBackendContext {
    Ibverbs(
        ActorRef<ibverbs::manager_actor::IbvManagerActor>,
        Arc<OnceCell<ibverbs::IbvBuffer>>,
    ),
    Tcp(ActorRef<tcp::manager_actor::TcpManagerActor>),
}

impl Serialize for RdmaRemoteBackendContext {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            RdmaRemoteBackendContext::Ibverbs(actor_ref, _) => serializer
                .serialize_newtype_variant("RdmaRemoteBackendContext", 0, "Ibverbs", actor_ref),
            RdmaRemoteBackendContext::Tcp(actor_ref) => serializer.serialize_newtype_variant(
                "RdmaRemoteBackendContext",
                1,
                "Tcp",
                actor_ref,
            ),
        }
    }
}

impl<'de> Deserialize<'de> for RdmaRemoteBackendContext {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "RdmaRemoteBackendContext")]
        enum Repr {
            Ibverbs(ActorRef<ibverbs::manager_actor::IbvManagerActor>),
            Tcp(ActorRef<tcp::manager_actor::TcpManagerActor>),
        }

        match Repr::deserialize(deserializer)? {
            Repr::Ibverbs(actor_ref) => Ok(RdmaRemoteBackendContext::Ibverbs(
                actor_ref,
                Arc::new(OnceCell::new()),
            )),
            Repr::Tcp(actor_ref) => Ok(RdmaRemoteBackendContext::Tcp(actor_ref)),
        }
    }
}

/// Backend for executing RDMA operations over a specific transport.
///
/// Each backend manages the transport-specific details of connection
/// management and data movement. The backend decides internally how to
/// batch and schedule submitted operations.
///
/// Current implementations:
/// - [`ibverbs::IbvManagerActor`] -- ibverbs NIC transport
/// - [`tcp::TcpManagerActor`] -- TCP fallback transport
#[async_trait]
pub trait RdmaBackend: Send + Debug {
    /// Backend-specific transport details (e.g., a cffi struct with raw
    /// ibverbs handles for GPU-initiated RDMA).
    type TransportInfo;

    /// Submit a batch of RDMA operations.
    ///
    /// The backend decides internally how to batch, schedule, and execute
    /// the operations (e.g., managing QPs and connections as needed).
    async fn submit(
        &mut self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<()>;

    /// The transport level provided by this backend.
    fn transport_level(&self) -> RdmaTransportLevel;

    /// Low-level backend-specific transport details for direct control
    /// over RDMA operations (e.g., from a GPU kernel).
    fn transport_info(&self) -> Option<Self::TransportInfo>;
}
