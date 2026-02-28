/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs backend implementation for RDMA operations.

use std::sync::Arc;

use hyperactor::ActorRef;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

pub(crate) mod domain;
pub mod manager_actor;
pub mod primitives;
pub mod queue_pair;

use manager_actor::IbvManagerActor;
pub use queue_pair::IbvQueuePair;
pub use queue_pair::PollTarget;

use crate::RdmaLocalMemory;
use crate::RdmaOpType;

/// Lazily-initialized ibverbs transport details for a registered memory
/// region. Retrieved on demand from the [`IbvManagerActor`] via
/// [`IbvManagerMessage::RequestBuffer`].
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct IbvBuffer {
    pub mr_id: usize,
    pub lkey: u32,
    pub rkey: u32,
    /// RDMA address (may differ from virtual address for CUDA memory).
    pub addr: usize,
    pub size: usize,
    /// Name of the RDMA device this buffer is associated with (e.g., "mlx5_0").
    pub device_name: String,
}

/// A single RDMA op for the [`IbvSubmit`] message.
#[derive(Debug, Clone, Named)]
pub struct IbvOp {
    pub op_type: RdmaOpType,
    pub local_memory: Arc<dyn RdmaLocalMemory>,
    pub remote_buffer: IbvBuffer,
    pub remote_manager: ActorRef<IbvManagerActor>,
}
