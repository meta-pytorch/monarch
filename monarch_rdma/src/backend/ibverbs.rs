/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs backend implementation for RDMA operations.

pub(crate) mod domain;
pub(crate) mod manager_actor;
pub mod primitives;
pub mod queue_pair;

pub use queue_pair::IbvQueuePair;
pub use queue_pair::PollTarget;
