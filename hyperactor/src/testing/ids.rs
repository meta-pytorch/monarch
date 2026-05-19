/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Standardized test ID constructors.
//!
//! All functions prefix the name component with `test_` so that
//! test-originated IDs are distinguishable from production ones.

use crate::ActorAddr;
use crate::PortAddr;
use crate::ProcAddr;
use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;

/// Create a test `ProcAddr` with a local channel address and name `"test_{name}"`.
pub fn test_proc_id(name: &str) -> ProcAddr {
    ProcAddr::singleton(
        ChannelAddr::any(ChannelTransport::Local),
        format!("test_{name}"),
    )
}

/// Create a test `ProcAddr` with a custom address and name `"test_{name}"`.
pub fn test_proc_id_with_addr(addr: ChannelAddr, name: &str) -> ProcAddr {
    ProcAddr::singleton(addr, format!("test_{name}"))
}

/// Create a test `ActorAddr`.
pub fn test_actor_id(proc_name: &str, actor_name: &str) -> ActorAddr {
    test_proc_id(proc_name).actor_addr(actor_name)
}

/// Create a test `PortAddr`.
pub fn test_port_id(proc_name: &str, actor_name: &str, port: u64) -> PortAddr {
    test_actor_id(proc_name, actor_name).port_addr(port.into())
}
