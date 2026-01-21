/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![cfg(test)]

//! This module contains common testing utilities.

use std::sync::OnceLock;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Proc;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Signal;
use hyperactor::channel::ChannelTransport;
use hyperactor::context;
use hyperactor::id;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::PortReceiver;
use hyperactor::proc::WorkCell;
use hyperactor::supervision::ActorSupervisionEvent;
use ndslice::Extent;
use tokio::process::Command;
use tokio::sync::OnceCell;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::alloc::Alloc;
use crate::alloc::AllocSpec;
use crate::alloc::Allocator;
use crate::alloc::LocalAllocator;
use crate::alloc::ProcessAllocator;
use crate::proc_mesh::default_transport;
use crate::supervision::MeshFailure;
use crate::v1::ProcMesh;
use crate::v1::host_mesh::HostMesh;

#[derive(Debug)]
pub struct TestRootClient {
    signal_rx: PortReceiver<Signal>,
    supervision_rx: PortReceiver<ActorSupervisionEvent>,
    work_rx: mpsc::UnboundedReceiver<WorkCell<Self>>,
}

impl Actor for TestRootClient {}

#[async_trait]
impl Handler<MeshFailure> for TestRootClient {
    async fn handle(&mut self, _cx: &Context<Self>, msg: MeshFailure) -> Result<(), anyhow::Error> {
        // If a supervision failure reaches the root test client, the test has
        // failed.
        tracing::error!("got supervision event from child: {}", msg);
        panic!("got supervision event from child: {}", msg);
    }
}

impl TestRootClient {
    fn run(mut self, instance: &'static Instance<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let err = 'messages: loop {
                tokio::select! {
                    work = self.work_rx.recv() => {
                        let work = work.expect("inconsistent work queue state");
                        if let Err(err) = work.handle(&mut self, instance).await {
                            for supervision_event in self.supervision_rx.drain() {
                                if let Err(err) = instance.handle_supervision_event(&mut self, supervision_event).await {
                                    break 'messages err;
                                }
                            }
                            let kind = ActorErrorKind::processing(err);
                            break ActorError {
                                actor_id: Box::new(instance.self_id().clone()),
                                kind: Box::new(kind),
                            };
                        }
                    }
                    _ = self.signal_rx.recv() => {
                        // TODO: do we need any signal handling for the root client?
                    }
                    Ok(supervision_event) = self.supervision_rx.recv() => {
                        if let Err(err) = instance.handle_supervision_event(&mut self, supervision_event).await {
                            break err;
                        }
                    }
                };
            };
            let event = match *err.kind {
                ActorErrorKind::UnhandledSupervisionEvent(event) => *event,
                _ => {
                    let status = ActorStatus::generic_failure(err.kind.to_string());
                    ActorSupervisionEvent::new(
                        instance.self_id().clone(),
                        Some("testclient".into()),
                        status,
                        None,
                    )
                }
            };
            instance.proc().handle_unhandled_supervision_event(event);
        })
    }
}

/// Returns a new test instance; it is initialized lazily.
pub fn fresh_instance() -> &'static Instance<TestRootClient> {
    static INSTANCE: OnceLock<Instance<TestRootClient>> = OnceLock::new();
    let proc = Proc::direct(ChannelTransport::Unix.any(), "testproc".to_string()).unwrap();
    let (actor, _handle, supervision_rx, signal_rx, work_rx) =
        proc.actor_instance("testclient").unwrap();
    // Use the OnceLock to get a 'static lifetime for the instance.
    INSTANCE
        .set(actor)
        .map_err(|_| "already initialized root client instance")
        .unwrap();
    let instance = INSTANCE.get().unwrap();
    let client = TestRootClient {
        signal_rx,
        supervision_rx,
        work_rx,
    };
    client.run(instance);
    instance
}

/// Returns the singleton test instance; it is initialized lazily.
pub fn instance() -> &'static Instance<TestRootClient> {
    static INSTANCE: OnceLock<&'static Instance<TestRootClient>> = OnceLock::new();
    INSTANCE.get_or_init(fresh_instance)
}

#[cfg(fbcode_build)]
pub async fn proc_meshes<C: context::Actor>(cx: &C, extent: Extent) -> Vec<ProcMesh>
where
    C::A: Handler<MeshFailure>,
{
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

        ProcMesh::allocate(cx, Box::new(alloc), "test_local")
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

        ProcMesh::allocate(cx, Box::new(alloc), "test_process")
            .await
            .unwrap()
    });

    meshes
}

/// Return different alloc implementations with the provided extent.
#[cfg(fbcode_build)]
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

/// Create a TestRootClient, and make the router it uses available.
async fn fresh_instance_with_router() -> (
    &'static Instance<TestRootClient>,
    &'static DialMailboxRouter,
) {
    static INSTANCE: OnceLock<(Instance<TestRootClient>, DialMailboxRouter)> = OnceLock::new();
    let router = DialMailboxRouter::new();
    let proc = Proc::new(id!(test[0]), router.boxed());
    let (actor, _handle, supervision_rx, signal_rx, work_rx) =
        proc.actor_instance("testclient").unwrap();
    // Use the OnceLock to get a 'static lifetime for the instance.
    INSTANCE
        .set((actor, router))
        .map_err(|_| "already initialized root client instance")
        .unwrap();
    let (instance, router) = INSTANCE.get().unwrap();
    let client = TestRootClient {
        signal_rx,
        supervision_rx,
        work_rx,
    };
    client.run(instance);
    (instance, router)
}

/// Create a local proc mesh with the provided extent, returning the
/// mesh itself, the controller actor, and the router.
pub async fn local_proc_mesh(
    extent: Extent,
) -> (
    ProcMesh,
    &'static Instance<TestRootClient>,
    &'static DialMailboxRouter,
) {
    static INSTANCE: OnceCell<(
        &'static Instance<TestRootClient>,
        &'static DialMailboxRouter,
    )> = OnceCell::const_new();
    let (actor, router) = INSTANCE.get_or_init(fresh_instance_with_router).await;
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
        ProcMesh::allocate(actor, Box::new(alloc), "test")
            .await
            .unwrap(),
        actor,
        router,
    )
}

/// Create a host mesh using multiple processes running on the test machine.
#[cfg(fbcode_build)]
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

    HostMesh::allocate(instance(), Box::new(alloc), "test", None)
        .await
        .unwrap()
}
