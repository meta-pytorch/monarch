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
use hyperactor::channel::ChannelTransport;
use hyperactor::context;
use hyperactor::id;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use ndslice::Extent;
use tokio::process::Command;
use tokio::sync::OnceCell;

use crate::alloc::Alloc;
use crate::alloc::AllocSpec;
use crate::alloc::Allocator;
use crate::alloc::LocalAllocator;
use crate::alloc::ProcessAllocator;
use crate::proc_mesh::default_transport;
use crate::v1::ProcMesh;
use crate::v1::host_mesh::HostMesh;

/// Returns a new test instance; it is initialized lazily.
pub async fn fresh_instance() -> Instance<()> {
    let proc = Proc::direct(ChannelTransport::Unix.any(), "testproc".to_string())
        .await
        .unwrap();
    let (actor, _handle) = proc.instance("testclient").unwrap();
    actor
}

/// Returns the singleton test instance; it is initialized lazily.
pub async fn instance() -> &'static Instance<()> {
    static INSTANCE: OnceCell<Instance<()>> = OnceCell::const_new();
    INSTANCE.get_or_init(fresh_instance).await
}

pub async fn proc_meshes(cx: &impl context::Actor, extent: Extent) -> Vec<ProcMesh> {
    let mut meshes = Vec::new();

    meshes.push({
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent.clone(),
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Local,
                proc_allocation_mode: Default::default(),
            })
            .await
            .unwrap();

        ProcMesh::allocate(cx, Box::new(alloc), "test.local")
            .await
            .unwrap()
    });

    meshes.push({
        let mut allocator = ProcessAllocator::new(Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        )));
        let alloc = allocator
            .allocate(AllocSpec {
                extent,
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            })
            .await
            .unwrap();

        ProcMesh::allocate(cx, Box::new(alloc), "test.process")
            .await
            .unwrap()
    });

    meshes
}

/// Return different alloc implementations with the provided extent.
pub async fn allocs(extent: Extent) -> Vec<Box<dyn Alloc + Send + Sync>> {
    let spec = AllocSpec {
        extent: extent.clone(),
        constraints: Default::default(),
        proc_name: None,
        transport: default_transport(),
        proc_allocation_mode: Default::default(),
    };

    vec![
        Box::new(LocalAllocator.allocate(spec.clone()).await.unwrap()),
        Box::new(
            ProcessAllocator::new(Command::new(crate::testresource::get(
                "monarch/hyperactor_mesh/bootstrap",
            )))
            .allocate(spec.clone())
            .await
            .unwrap(),
        ),
    ]
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
            transport: ChannelTransport::Local,
            proc_allocation_mode: Default::default(),
        })
        .await
        .unwrap();
    (
        ProcMesh::allocate(&actor, Box::new(alloc), "test")
            .await
            .unwrap(),
        actor,
        router,
    )
}

/// Create a host mesh using multiple processes running on the test machine.
pub async fn host_mesh(extent: Extent) -> HostMesh {
    let mut allocator = ProcessAllocator::new(Command::new(crate::testresource::get(
        "monarch/hyperactor_mesh/bootstrap",
    )));
    let alloc = allocator
        .allocate(AllocSpec {
            extent,
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Unix,
            proc_allocation_mode: Default::default(),
        })
        .await
        .unwrap();

    HostMesh::allocate(instance().await, Box::new(alloc), "test", None)
        .await
        .unwrap()
}
