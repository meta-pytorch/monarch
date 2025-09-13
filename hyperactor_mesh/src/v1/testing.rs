/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![cfg(test)]

//! This module contains common testing utilities.

use hyperactor::Instance;
use hyperactor::Proc;
use hyperactor::context;
use hyperactor::id;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use ndslice::Extent;
use tokio::process::Command;

use crate::alloc::AllocSpec;
use crate::alloc::Allocator;
use crate::alloc::LocalAllocator;
use crate::alloc::ProcessAllocator;
use crate::v1::ProcMesh;

pub fn instance() -> Instance<()> {
    let proc = Proc::new(id!(test[0]), DialMailboxRouter::new().boxed());
    let (actor, _handle) = proc.instance("testclient").unwrap();
    actor
}

pub async fn proc_meshes(cx: &impl context::Actor, extent: Extent) -> Vec<ProcMesh> {
    let mut meshes = Vec::new();

    meshes.push({
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent.clone(),
                constraints: Default::default(),
                proc_name: None,
            })
            .await
            .unwrap();

        ProcMesh::allocate(cx, alloc, "test.local").await.unwrap()
    });

    meshes.push({
        let mut allocator = ProcessAllocator::new(Command::new(
            buck_resources::get("monarch/hyperactor_mesh/bootstrap").unwrap(),
        ));
        let alloc = allocator
            .allocate(AllocSpec {
                extent,
                constraints: Default::default(),
                proc_name: None,
            })
            .await
            .unwrap();

        ProcMesh::allocate(cx, alloc, "test.process").await.unwrap()
    });

    meshes
}

/// Create a local proc mesh with the provided extent, returning the
/// mesh itself, the controller actor, and the router.
pub async fn local_proc_mesh(extent: Extent) -> (ProcMesh, Instance<()>, DialMailboxRouter) {
    let router = DialMailboxRouter::new();
    let proc = Proc::new(id!(test[0]), router.boxed());
    let (actor, _handle) = proc.instance("controller").unwrap();

    let alloc = LocalAllocator
        .allocate(AllocSpec {
            extent,
            constraints: Default::default(),
            proc_name: None,
        })
        .await
        .unwrap();
    (
        ProcMesh::allocate(&actor, alloc, "test").await.unwrap(),
        actor,
        router,
    )
}
