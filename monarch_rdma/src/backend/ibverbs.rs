/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs backend implementation for RDMA operations.

use hyperactor::ActorRef;
use hyperactor::actor::Referable;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

pub mod device;
pub mod device_selection;
pub(crate) mod domain;
pub mod efa_device;
pub mod efa_domain;
pub mod manager_actor;
pub mod memory_region;
pub mod mlx_device;
pub mod mlx_domain;
pub mod primitives;
pub mod queue_pair;

use manager_actor::IbvManagerActor;
use mlx_device::MlxDevice;
pub use queue_pair::IbvQueuePair;
pub use queue_pair::PollTarget;

#[cfg(test)]
mod doorbell_test_utils;
#[cfg(test)]
mod doorbell_tests;
#[cfg(test)]
mod mlx5dv_tests;

use crate::RdmaOpType;
use crate::local_memory::KeepaliveLocalMemory;

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

/// A single RDMA op for the [`IbvBackend`](manager_actor::IbvBackend).
///
/// Generic over the manager actor type so unit tests can swap in a
/// mock; production code uses the default `IbvOp<IbvManagerActor>`.
#[derive(Debug, Named)]
pub struct IbvOp<M: Referable = IbvManagerActor<MlxDevice>> {
    pub op_type: RdmaOpType,
    pub local_memory: KeepaliveLocalMemory,
    pub remote_buffer: IbvBuffer,
    pub remote_manager: ActorRef<M>,
}

// Hand-rolled `Clone` to avoid the `M: Clone` bound the derive macro
// would add (`ActorRef<M>` is `Clone` for all `M: Referable`).
impl<M: Referable> Clone for IbvOp<M> {
    fn clone(&self) -> Self {
        Self {
            op_type: self.op_type,
            local_memory: self.local_memory.clone(),
            remote_buffer: self.remote_buffer.clone(),
            remote_manager: self.remote_manager.clone(),
        }
    }
}
