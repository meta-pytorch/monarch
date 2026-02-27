/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TCP fallback backend for RDMA operations.
//!
//! Transfers memory over the default hyperactor channel transport by
//! chunking buffers into serializable messages. Intended as a fallback
//! when ibverbs hardware is unavailable.

pub mod manager_actor;

use std::sync::Arc;

use hyperactor::ActorRef;
use manager_actor::TcpManagerActor;

use crate::RdmaLocalMemory;
use crate::RdmaOpType;

/// A single operation for the [`TcpSubmit`] local message.
#[derive(Debug)]
pub struct TcpOp {
    pub op_type: RdmaOpType,
    pub local_memory: Arc<dyn RdmaLocalMemory>,
    pub remote_tcp_manager: ActorRef<TcpManagerActor>,
    pub remote_buf_id: usize,
}
