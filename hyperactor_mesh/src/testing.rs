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
use hyperactor::mailbox::PortReceiver;
use hyperactor::proc::WorkCell;
use hyperactor::supervision::ActorSupervisionEvent;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::Bootstrap;
use crate::HostMeshRef;
use crate::host_mesh::HostMesh;
use crate::host_mesh::HostMeshShutdownGuard;
use crate::supervision::MeshFailure;

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
            instance
                .proc()
                .handle_unhandled_supervision_event(instance, event);
        })
    }
}

/// Returns a new test instance; it is initialized lazily.
pub fn fresh_instance() -> &'static Instance<TestRootClient> {
    static INSTANCE: OnceLock<Instance<TestRootClient>> = OnceLock::new();
    let proc = Proc::direct(ChannelTransport::Unix.any(), "testproc".to_string()).unwrap();
    let ai = proc.actor_instance("testclient").unwrap();
    // Use the OnceLock to get a 'static lifetime for the instance.
    INSTANCE
        .set(ai.instance)
        .map_err(|_| "already initialized root client instance")
        .unwrap();
    let instance = INSTANCE.get().unwrap();
    let client = TestRootClient {
        signal_rx: ai.signal,
        supervision_rx: ai.supervision,
        work_rx: ai.work,
    };
    client.run(instance);
    instance
}

/// Returns the singleton test instance; it is initialized lazily.
pub fn instance() -> &'static Instance<TestRootClient> {
    static INSTANCE: OnceLock<&'static Instance<TestRootClient>> = OnceLock::new();
    INSTANCE.get_or_init(fresh_instance)
}

/// Create a host mesh using multiple processes running on the test machine.
/// The transport used by the hosts is Unix channel.
///
/// # Examples
///
/// ```
/// let host_mesh = testing::host_mesh(4).await;
/// // spawn a process mesh on this host mesh with the name "test", abd per_host
/// // extent gpu = 8.
/// let proc_mesh = host_mesh
///     .spawn(instance, "test", extent!(gpu = 8), None)
///     .await
///     .unwrap();
/// // ... do something with the proc mesh ...
/// // shutdown the host mesh.
/// let _ = host_mesh.shutdown(&instance).await;
/// ```
#[cfg(fbcode_build)]
pub async fn host_mesh(n: usize) -> HostMeshShutdownGuard {
    use crate::Name;

    let program = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");

    let mut host_addrs = vec![];
    for _ in 0..n {
        host_addrs.push(ChannelTransport::Unix.any());
    }

    for host in host_addrs.iter() {
        let mut cmd = Command::new(program.clone());
        let boot = Bootstrap::Host {
            addr: host.clone(),
            command: None, // use current binary
            config: None,
            exit_on_shutdown: false,
        };
        boot.to_env(&mut cmd);
        cmd.kill_on_drop(false);
        // SAFETY: Ensure the child process is killed by the kernel if the
        // parent process dies, even if the parent is SIGKILL'd. This is to
        // avoid resource leak after the test exited or crashed.
        unsafe {
            cmd.pre_exec(crate::bootstrap::install_pdeathsig_kill);
        }
        cmd.spawn().unwrap();
    }

    let host_mesh = HostMeshRef::from_hosts(Name::new("test").unwrap(), host_addrs);
    HostMesh::take(host_mesh).shutdown_guard()
}
