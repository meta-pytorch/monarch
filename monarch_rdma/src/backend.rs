/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RDMA backend implementations.

#[cfg(any(test, feature = "test-utils"))]
pub mod cuda_test_utils;
pub mod ibverbs;
pub mod tcp;

use std::fmt::Debug;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::ActorRef;
use serde::Deserialize;
use serde::Serialize;

use crate::RdmaOp;
use crate::RdmaTransportLevel;
use crate::nic::NicRemoteBackendContext;

/// Backend-specific context for a remote buffer.
///
/// Each variant holds the information needed to perform RDMA operations
/// using that backend on a particular buffer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RdmaRemoteBackendContext {
    Nic(NicRemoteBackendContext),
    Tcp(ActorRef<tcp::manager_actor::TcpManagerActor>),
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
