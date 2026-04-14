/*
* Copyright (c) Meta Platforms, Inc. and affiliates.
* All rights reserved.
*
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree.
*/
mod efa;
mod broadcom;
mod mellanox;
// Future: add more Nic backend modules here

use::std::sync::OnceLock;
use crate::nic_backend::NicBackend;
use crate::backend::ibverbs::primitives::IbvConfig;

/// Returns all known NIC backends, ordered by priority (highest first)
fn all_nic_backends() -> Vec<&'static dyn
