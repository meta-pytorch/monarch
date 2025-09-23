/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// RDMA requires frequent unsafe code blocks
#![allow(clippy::undocumented_unsafe_blocks)]

pub mod device_selection;
mod ibverbs_primitives;
mod rdma_components;
mod rdma_manager_actor;

#[macro_use]
mod macros;

pub use ibverbs_primitives::*;
pub use rdma_components::*;
pub use rdma_manager_actor::*;
pub use test_utils::is_cuda_available;

#[cfg(test)]
mod rdma_manager_actor_tests;
mod test_utils;
