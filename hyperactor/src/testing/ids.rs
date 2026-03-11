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

use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;
use crate::reference;

/// Create a test `ProcId` with a local channel address and name `"test_{name}"`.
pub fn test_proc_id(name: &str) -> reference::ProcId {
    reference::ProcId::with_name(
        ChannelAddr::any(ChannelTransport::Local),
        format!("test_{name}"),
    )
}

/// Create a test `ProcId` with a custom address and name `"test_{name}"`.
pub fn test_proc_id_with_addr(addr: ChannelAddr, name: &str) -> reference::ProcId {
    reference::ProcId::with_name(addr, format!("test_{name}"))
}

/// Create a test `ActorId` with pid 0.
pub fn test_actor_id(proc_name: &str, actor_name: &str) -> reference::ActorId {
    test_proc_id(proc_name).actor_id(actor_name, 0)
}

/// Create a test `ActorId` with a custom pid.
pub fn test_actor_id_with_pid(proc_name: &str, actor_name: &str, pid: usize) -> reference::ActorId {
    test_proc_id(proc_name).actor_id(actor_name, pid)
}

/// Create a test `PortId` with pid 0.
pub fn test_port_id(proc_name: &str, actor_name: &str, port: u64) -> reference::PortId {
    reference::PortId::new(test_actor_id(proc_name, actor_name), port)
}

/// Create a test `PortId` with a custom pid.
pub fn test_port_id_with_pid(
    proc_name: &str,
    actor_name: &str,
    pid: usize,
    port: u64,
) -> reference::PortId {
    reference::PortId::new(test_actor_id_with_pid(proc_name, actor_name, pid), port)
}
