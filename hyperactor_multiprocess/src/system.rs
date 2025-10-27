/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::future::IntoFuture;

use futures::FutureExt;
use futures::future::BoxFuture;
use hyperactor::Instance;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorHandle;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::ClockKind;
use hyperactor::context::Mailbox as _;
use hyperactor::id;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MailboxServer;
use hyperactor::mailbox::MailboxServerHandle;
use hyperactor::proc::Proc;
use system_actor::SystemActor;
use system_actor::SystemActorParams;
use system_actor::SystemMessageClient;
use tokio::join;

use crate::proc_actor::ProcMessage;
use crate::system_actor;
use crate::system_actor::ProcLifecycleMode;

/// Multiprocess system implementation.
#[derive(Debug)]
pub struct System {
    addr: ChannelAddr,
}

impl System {
    /// Spawns a system actor and serves it at the provided channel
    /// address. This becomes a well-known address with which procs
    /// can bootstrap.
    pub async fn serve(
        addr: ChannelAddr,
        supervision_update_timeout: tokio::time::Duration,
        world_eviction_timeout: tokio::time::Duration,
    ) -> Result<ServerHandle, anyhow::Error> {
        let clock = ClockKind::for_channel_addr(&addr);
        let params = SystemActorParams::new(supervision_update_timeout, world_eviction_timeout);
        let (actor_handle, system_proc) = SystemActor::bootstrap_with_clock(params, clock).await?;
        actor_handle.bind::<SystemActor>();

        let (local_addr, rx) = channel::serve(addr, "System::serve")?;
        let mailbox_handle = system_proc.clone().serve(rx);

        Ok(ServerHandle {
            actor_handle,
            mailbox_handle,
            local_addr,
        })
    }

    /// Connect to the system at the provided address.
    pub fn new(addr: ChannelAddr) -> Self {
        Self { addr }
    }

    /// A sender capable of routing all messages to actors in the system.
    async fn sender(&self) -> Result<impl MailboxSender + use<>, anyhow::Error> {
        let tx = channel::dial(self.addr.clone())?;
        Ok(MailboxClient::new(tx))
    }

    /// Join the system ephemerally. This allocates an actor id, and returns the
    /// corresponding mailbox.
    ///
    /// TODO: figure out lifecycle management: e.g., should this be
    /// alive until all ports are deallocated and the receiver is dropped?
    pub async fn attach(&mut self) -> Result<Instance<()>, anyhow::Error> {
        // TODO: just launch a proc actor here to handle the local
        // proc management.
        let world_id = id!(user);
        let proc = Proc::new(
            world_id.random_user_proc(),
            BoxedMailboxSender::new(self.sender().await?),
        );

        let (proc_addr, proc_rx) =
            channel::serve(ChannelAddr::any(self.addr.transport()), "system").unwrap();

        let _proc_serve_handle: MailboxServerHandle = proc.clone().serve(proc_rx);

        // Now, pretend we are the proc actor, and use this to join the system.
        let (instance, _handle) = proc.instance("proc")?;
        let (proc_tx, mut proc_rx) = instance.mailbox().open_port();

        system_actor::SYSTEM_ACTOR_REF
            .join(
                &instance,
                world_id,
                /*proc_id=*/ proc.proc_id().clone(),
                /*proc_message_port=*/ proc_tx.bind(),
                proc_addr,
                HashMap::new(),
                ProcLifecycleMode::Detached,
            )
            .await
            .unwrap();
        let timeout = hyperactor::config::global::get(hyperactor::config::MESSAGE_DELIVERY_TIMEOUT);
        loop {
            let result = proc.clock().timeout(timeout, proc_rx.recv()).await?;
            match result? {
                ProcMessage::Joined() => break,
                message => tracing::info!("proc message while joining: {:?}", message),
            }
        }

        proc.instance("user").map(|(instance, _)| instance)
    }
}

/// Handle for a running system server.
#[derive(Debug)]
pub struct ServerHandle {
    actor_handle: ActorHandle<SystemActor>,
    mailbox_handle: MailboxServerHandle,
    local_addr: ChannelAddr,
}

impl ServerHandle {
    /// Stop the server. The user should join the handle after calling stop.
    pub async fn stop(&self) -> Result<(), ActorError> {
        // TODO: this needn't be async
        self.actor_handle.drain_and_stop()?;
        self.mailbox_handle.stop("system server stopped");
        Ok(())
    }

    /// The local (bound) address of the server.
    pub fn local_addr(&self) -> &ChannelAddr {
        &self.local_addr
    }

    /// The system actor handle.
    pub fn system_actor_handle(&self) -> &ActorHandle<SystemActor> {
        &self.actor_handle
    }
}

/// A future implementation for actor handle used for joining. It is
/// forwarded to the underlying join handles.
impl IntoFuture for ServerHandle {
    type Output = ();
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let future = async move {
            let _ = join!(self.actor_handle.into_future(), self.mailbox_handle);
        };
        future.boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::time::Duration;

    use hyperactor::ActorRef;
    use hyperactor::ProcId;
    use hyperactor::WorldId;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::TcpMode;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor_telemetry::env::execution_id;
    use maplit::hashset;
    use timed_test::async_timed_test;

    use super::*;
    use crate::System;
    use crate::proc_actor::Environment;
    use crate::proc_actor::ProcActor;
    use crate::supervision::ProcSupervisor;
    use crate::system_actor::ProcLifecycleMode;
    use crate::system_actor::SYSTEM_ACTOR_REF;
    use crate::system_actor::Shape;
    use crate::system_actor::SystemMessageClient;
    use crate::system_actor::SystemSnapshot;
    use crate::system_actor::SystemSnapshotFilter;
    use crate::system_actor::WorldSnapshot;
    use crate::system_actor::WorldSnapshotProcInfo;
    use crate::system_actor::WorldStatus;

    #[tokio::test]
    async fn test_join() {
        for transport in ChannelTransport::all() {
            // TODO: make ChannelAddr::any work even without
            #[cfg(not(target_os = "linux"))]
            if matches!(transport, ChannelTransport::Unix) {
                continue;
            }

            let system_handle = System::serve(
                ChannelAddr::any(transport),
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .unwrap();

            let mut system = System::new(system_handle.local_addr().clone());
            let client1 = system.attach().await.unwrap();
            let client2 = system.attach().await.unwrap();

            let (port, mut port_rx) = client2.open_port();

            port.bind().send(&client1, 123u64).unwrap();
            assert_eq!(port_rx.recv().await.unwrap(), 123u64);

            system_handle.stop().await.unwrap();
            system_handle.await;
        }
    }

    #[tokio::test]
    async fn test_system_snapshot() {
        let system_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

        let mut system = System::new(system_handle.local_addr().clone());
        let client = system.attach().await.unwrap();

        let sys_actor_handle = system_handle.system_actor_handle();
        // Check the inital state.
        {
            let snapshot = sys_actor_handle
                .snapshot(&client, SystemSnapshotFilter::all())
                .await
                .unwrap();
            assert_eq!(
                snapshot,
                SystemSnapshot {
                    worlds: HashMap::new(),
                    execution_id: execution_id(),
                }
            );
        }

        // Create a world named foo, and join a non-worker proc to it.
        let foo_world = {
            let foo_world_id = WorldId("foo_world".to_string());
            sys_actor_handle
                .upsert_world(
                    &client,
                    foo_world_id.clone(),
                    Shape::Definite(vec![2]),
                    5,
                    Environment::Local,
                    HashMap::new(),
                )
                .await
                .unwrap();
            {
                let snapshot = sys_actor_handle
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .unwrap();
                let time = snapshot
                    .worlds
                    .get(&foo_world_id)
                    .unwrap()
                    .status
                    .as_unhealthy()
                    .unwrap()
                    .clone();
                assert_eq!(
                    snapshot,
                    SystemSnapshot {
                        worlds: HashMap::from([(
                            foo_world_id.clone(),
                            WorldSnapshot {
                                host_procs: HashSet::new(),
                                procs: HashMap::new(),
                                status: WorldStatus::Unhealthy(time),
                                labels: HashMap::new(),
                            }
                        ),]),
                        execution_id: execution_id(),
                    }
                );
            }

            // Join a non-worker proc to the "foo" world.
            {
                let test_labels =
                    HashMap::from([("test_name".to_string(), "test_value".to_string())]);
                let listen_addr = ChannelAddr::any(ChannelTransport::Local);
                let proc_id = ProcId::Ranked(foo_world_id.clone(), 1);
                ProcActor::try_bootstrap(
                    proc_id.clone(),
                    foo_world_id.clone(),
                    listen_addr,
                    system_handle.local_addr().clone(),
                    ActorRef::attest(proc_id.actor_id("supervision", 0)),
                    Duration::from_secs(30),
                    test_labels.clone(),
                    ProcLifecycleMode::ManagedBySystem,
                )
                .await
                .unwrap();

                let snapshot = sys_actor_handle
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .unwrap();
                let time = snapshot
                    .worlds
                    .get(&foo_world_id)
                    .unwrap()
                    .status
                    .as_unhealthy()
                    .unwrap()
                    .clone();
                let foo_world = (
                    foo_world_id.clone(),
                    WorldSnapshot {
                        host_procs: HashSet::new(),
                        procs: HashMap::from([(
                            proc_id.clone(),
                            WorldSnapshotProcInfo {
                                labels: test_labels.clone(),
                            },
                        )]),
                        status: WorldStatus::Unhealthy(time),
                        labels: HashMap::new(),
                    },
                );

                assert_eq!(
                    snapshot,
                    SystemSnapshot {
                        worlds: HashMap::from([foo_world.clone(),]),
                        execution_id: execution_id(),
                    },
                );

                // check snapshot world filters
                let snapshot = sys_actor_handle
                    .snapshot(
                        &client,
                        SystemSnapshotFilter {
                            worlds: vec![WorldId("none".to_string())],
                            world_labels: HashMap::new(),
                            proc_labels: HashMap::new(),
                        },
                    )
                    .await
                    .unwrap();
                assert!(snapshot.worlds.is_empty());
                // check actor filters
                let snapshot = sys_actor_handle
                    .snapshot(
                        &client,
                        SystemSnapshotFilter {
                            worlds: vec![],
                            world_labels: HashMap::new(),
                            proc_labels: test_labels.clone(),
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(snapshot.worlds.get(&foo_world_id).unwrap(), &foo_world.1);
                foo_world
            }
        };

        // Create a worker world from host procs.
        {
            let worker_world_id = WorldId("worker_world".to_string());
            let host_world_id = WorldId(("hostworker_world").to_string());
            let listen_addr: ChannelAddr = ChannelAddr::any(ChannelTransport::Local);
            // Join a host proc to the system first with no worker_world yet.
            let host_proc_id_1 = ProcId::Ranked(host_world_id.clone(), 1);
            ProcActor::try_bootstrap(
                host_proc_id_1.clone(),
                host_world_id.clone(),
                listen_addr.clone(),
                system_handle.local_addr().clone(),
                ActorRef::attest(host_proc_id_1.actor_id("supervision", 0)),
                Duration::from_secs(30),
                HashMap::new(),
                ProcLifecycleMode::ManagedBySystem,
            )
            .await
            .unwrap();
            {
                let snapshot = sys_actor_handle
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .unwrap();
                assert_eq!(
                    snapshot,
                    SystemSnapshot {
                        worlds: HashMap::from([
                            foo_world.clone(),
                            (
                                worker_world_id.clone(),
                                WorldSnapshot {
                                    host_procs: HashSet::from([host_proc_id_1.clone()]),
                                    procs: HashMap::new(),
                                    status: WorldStatus::AwaitingCreation,
                                    labels: HashMap::new(),
                                }
                            ),
                        ]),
                        execution_id: execution_id(),
                    },
                );
            }

            // Upsert the worker world.
            sys_actor_handle
                .upsert_world(
                    &client,
                    worker_world_id.clone(),
                    // 12 worker procs in total, 8 per host. That means one
                    // host spawn 8 procs, and another host spawn 4 procs.
                    Shape::Definite(vec![3, 4]),
                    8,
                    Environment::Local,
                    HashMap::new(),
                )
                .await
                .unwrap();
            // Wait for the worker procs being spawned.
            RealClock.sleep(Duration::from_secs(2)).await;
            {
                let snapshot = sys_actor_handle
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .unwrap();
                let time = snapshot
                    .worlds
                    .get(&worker_world_id)
                    .unwrap()
                    .status
                    .as_unhealthy()
                    .unwrap()
                    .clone();
                assert_eq!(
                    snapshot,
                    SystemSnapshot {
                        worlds: HashMap::from([
                            foo_world.clone(),
                            (
                                worker_world_id.clone(),
                                WorldSnapshot {
                                    host_procs: HashSet::from([host_proc_id_1.clone()]),
                                    procs: (8..12)
                                        .map(|i| (
                                            ProcId::Ranked(worker_world_id.clone(), i),
                                            WorldSnapshotProcInfo {
                                                labels: HashMap::new()
                                            }
                                        ))
                                        .collect(),
                                    status: WorldStatus::Unhealthy(time),
                                    labels: HashMap::new(),
                                }
                            ),
                        ]),
                        execution_id: execution_id(),
                    },
                );
            }

            let host_proc_id_0 = ProcId::Ranked(host_world_id.clone(), 0);
            ProcActor::try_bootstrap(
                host_proc_id_0.clone(),
                host_world_id.clone(),
                listen_addr,
                system_handle.local_addr().clone(),
                ActorRef::attest(host_proc_id_0.actor_id("supervision", 0)),
                Duration::from_secs(30),
                HashMap::new(),
                ProcLifecycleMode::ManagedBySystem,
            )
            .await
            .unwrap();

            // Wait for the worker procs being spawned.
            RealClock.sleep(Duration::from_secs(2)).await;
            {
                let snapshot = sys_actor_handle
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .unwrap();
                assert_eq!(
                    snapshot,
                    SystemSnapshot {
                        worlds: HashMap::from([
                            foo_world,
                            (
                                worker_world_id.clone(),
                                WorldSnapshot {
                                    host_procs: HashSet::from([host_proc_id_0, host_proc_id_1]),
                                    procs: HashMap::from_iter((0..12).map(|i| (
                                        ProcId::Ranked(worker_world_id.clone(), i),
                                        WorldSnapshotProcInfo {
                                            labels: HashMap::new()
                                        }
                                    ))),
                                    // We have 12 procs ready to serve a 3 X 4 world.
                                    status: WorldStatus::Live,
                                    labels: HashMap::new(),
                                }
                            ),
                        ]),
                        execution_id: execution_id(),
                    }
                );
            }
        }
    }

    // The test consists of 2 steps:
    // 1. spawn a system with 2 host procs, and 8 worker procs. For each worker
    //    proc, spawn a root actor with a children tree.
    // 2. Send a Stop message to system actor, and verify everything will be
    //    shut down.
    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 60)]
    async fn test_system_shutdown() {
        let system_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let system_supervision_ref: ActorRef<ProcSupervisor> =
            ActorRef::attest(SYSTEM_ACTOR_REF.actor_id().clone());

        let mut system = System::new(system_handle.local_addr().clone());
        let client = system.attach().await.unwrap();

        let sys_actor_handle = system_handle.system_actor_handle();

        // Create a worker world from host procs.
        let worker_world_id = WorldId("worker_world".to_string());
        let shape = vec![2, 2, 4];
        let host_proc_actors = {
            let host_world_id = WorldId(("hostworker_world").to_string());
            // Upsert the worker world.
            sys_actor_handle
                .upsert_world(
                    &client,
                    worker_world_id.clone(),
                    // 2 worker procs in total, 8 per host.
                    Shape::Definite(shape.clone()),
                    8,
                    Environment::Local,
                    HashMap::new(),
                )
                .await
                .unwrap();

            // Bootstrap the host procs, which will lead to work procs being spawned.
            let futs = (0..2).map(|i| {
                let host_proc_id = ProcId::Ranked(host_world_id.clone(), i);
                ProcActor::try_bootstrap(
                    host_proc_id.clone(),
                    host_world_id.clone(),
                    ChannelAddr::any(ChannelTransport::Local),
                    system_handle.local_addr().clone(),
                    system_supervision_ref.clone(),
                    Duration::from_secs(30),
                    HashMap::new(),
                    ProcLifecycleMode::ManagedBySystem,
                )
            });
            futures::future::try_join_all(futs).await.unwrap()
        };
        // Wait for the worker procs being spawned.
        RealClock.sleep(Duration::from_secs(2)).await;

        // Create a world named foo, and directly join procs to it.
        let foo_proc_actors = {
            let foo_world_id = WorldId("foo_world".to_string());
            sys_actor_handle
                .upsert_world(
                    &client,
                    foo_world_id.clone(),
                    Shape::Definite(vec![2]),
                    2,
                    Environment::Local,
                    HashMap::new(),
                )
                .await
                .unwrap();
            // Join a non-worker proc to the "foo" world.
            let foo_futs = (0..2).map(|i| {
                let listen_addr = ChannelAddr::any(ChannelTransport::Local);
                let proc_id = ProcId::Ranked(foo_world_id.clone(), i);
                ProcActor::try_bootstrap(
                    proc_id.clone(),
                    foo_world_id.clone(),
                    listen_addr,
                    system_handle.local_addr().clone(),
                    system_supervision_ref.clone(),
                    Duration::from_secs(30),
                    HashMap::new(),
                    ProcLifecycleMode::ManagedBySystem,
                )
            });
            futures::future::try_join_all(foo_futs).await.unwrap()
        };

        let (port, receiver) = client.open_once_port::<()>();
        // Kick off the shutdown.
        sys_actor_handle
            .stop(&client, None, Duration::from_secs(5), port.bind())
            .await
            .unwrap();
        receiver.recv().await.unwrap();
        RealClock.sleep(Duration::from_secs(5)).await;

        // // Verify all the host actors are stopped.
        for bootstrap in host_proc_actors {
            bootstrap.proc_actor.into_future().await;
        }

        // Verify all the foo actors are stopped.
        for bootstrap in foo_proc_actors {
            bootstrap.proc_actor.into_future().await;
        }
        // Verify the system actor is stopped.
        system_handle.actor_handle.into_future().await;

        // Since we do not have the worker actor handles, verify the worker procs
        // are stopped by checking the logs.
        for m in 0..(shape.iter().product()) {
            let proc_id = worker_world_id.proc_id(m);
            assert!(tracing_test::internal::logs_with_scope_contain(
                "hyperactor::proc",
                format!("{proc_id}: proc stopped").as_str()
            ));
        }
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_single_world_shutdown() {
        let system_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let system_supervision_ref: ActorRef<ProcSupervisor> =
            ActorRef::attest(SYSTEM_ACTOR_REF.actor_id().clone());

        let mut system = System::new(system_handle.local_addr().clone());
        let client = system.attach().await.unwrap();

        let sys_actor_handle = system_handle.system_actor_handle();

        let host_world_id = WorldId(("host_world").to_string());
        let worker_world_id = WorldId("worker_world".to_string());
        let foo_world_id = WorldId("foo_world".to_string());

        // Create a worker world from host procs.
        let shape = vec![2, 2, 4];
        let host_proc_actors = {
            // Upsert the worker world.
            sys_actor_handle
                .upsert_world(
                    &client,
                    worker_world_id.clone(),
                    // 2 worker procs in total, 8 per host.
                    Shape::Definite(shape.clone()),
                    8,
                    Environment::Local,
                    HashMap::new(),
                )
                .await
                .unwrap();

            // Bootstrap the host procs, which will lead to work procs being spawned.
            let futs = (0..2).map(|i| {
                let host_proc_id = ProcId::Ranked(host_world_id.clone(), i);
                ProcActor::try_bootstrap(
                    host_proc_id.clone(),
                    host_world_id.clone(),
                    ChannelAddr::any(ChannelTransport::Local),
                    system_handle.local_addr().clone(),
                    system_supervision_ref.clone(),
                    Duration::from_secs(30),
                    HashMap::new(),
                    ProcLifecycleMode::ManagedBySystem,
                )
            });
            futures::future::try_join_all(futs).await.unwrap()
        };
        // Wait for the worker procs being spawned.
        RealClock.sleep(Duration::from_secs(2)).await;

        // Create a world named foo, and directly join procs to it.
        let foo_proc_actors = {
            sys_actor_handle
                .upsert_world(
                    &client,
                    foo_world_id.clone(),
                    Shape::Definite(vec![2]),
                    2,
                    Environment::Local,
                    HashMap::new(),
                )
                .await
                .unwrap();
            // Join a non-worker proc to the "foo" world.
            let foo_futs = (0..2).map(|i| {
                let listen_addr = ChannelAddr::any(ChannelTransport::Local);
                let proc_id = ProcId::Ranked(foo_world_id.clone(), i);
                ProcActor::try_bootstrap(
                    proc_id.clone(),
                    foo_world_id.clone(),
                    listen_addr,
                    system_handle.local_addr().clone(),
                    system_supervision_ref.clone(),
                    Duration::from_secs(30),
                    HashMap::new(),
                    ProcLifecycleMode::ManagedBySystem,
                )
            });
            futures::future::try_join_all(foo_futs).await.unwrap()
        };

        {
            let snapshot = sys_actor_handle
                .snapshot(&client, SystemSnapshotFilter::all())
                .await
                .unwrap();
            let snapshot_world_ids: HashSet<WorldId> = snapshot.worlds.keys().cloned().collect();
            assert_eq!(
                snapshot_world_ids,
                hashset! {worker_world_id.clone(), foo_world_id.clone(), WorldId("_world".to_string())}
            );
        }

        let (port, receiver) = client.open_once_port::<()>();
        // Kick off the shutdown.
        sys_actor_handle
            .stop(
                &client,
                Some(vec![WorldId("foo_world".into())]),
                Duration::from_secs(5),
                port.bind(),
            )
            .await
            .unwrap();
        receiver.recv().await.unwrap();
        RealClock.sleep(Duration::from_secs(5)).await;

        // Verify all the foo actors are stopped.
        for bootstrap in foo_proc_actors {
            bootstrap.proc_actor.into_future().await;
        }

        // host actors should still be running.
        for bootstrap in host_proc_actors {
            match RealClock
                .timeout(Duration::from_secs(5), bootstrap.proc_actor.into_future())
                .await
            {
                Ok(_) => {
                    panic!("foo actor shouldn't be stopped");
                }
                Err(_) => {}
            }
        }

        // Verify the system actor not stopped.
        match RealClock
            .timeout(
                Duration::from_secs(3),
                system_handle.actor_handle.clone().into_future(),
            )
            .await
        {
            Ok(_) => {
                panic!("system actor shouldn't be stopped");
            }
            Err(_) => {}
        }

        {
            let snapshot = sys_actor_handle
                .snapshot(&client, SystemSnapshotFilter::all())
                .await
                .unwrap();
            let snapshot_world_ids: HashSet<WorldId> = snapshot.worlds.keys().cloned().collect();
            // foo_world_id is no longer in the snapshot.
            assert_eq!(
                snapshot_world_ids,
                hashset! {worker_world_id, WorldId("_world".to_string())}
            );
        }
    }

    // Test our understanding of when & where channel addresses are
    // dialed.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_channel_dial_count() {
        let system_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

        let system_addr = system_handle.local_addr();
        let mut system = System::new(system_addr.clone());
        // `system.attach()` calls `system.send()` which
        // `channel::dial()`s the system address for a `MailboxClient`
        // for the `EnvelopingMailboxSender` to be the forwarding
        // sender for `client1`s proc (+1 dial).
        //
        // The forwarding sender will be used to send a join message
        // to the system actor that uses the `NetTx` just dialed so no
        // new `channel::dial()` for that (+0 dial). However, the
        // system actor will respond to the join message by using the
        // proc address (given in the join message) for the new proc
        // when it sends from its `DialMailboxRouter` so we expect to
        // see a `channel::dial()` there (+1 dial).
        let client1 = system.attach().await.unwrap();

        // `system.attach()` calls `system.send()` which
        // `channel::dial()`s the system address for a `MailboxClient`
        // for the `EnvelopingMailboxSender` to be the forwarding
        // sender for `client2`s proc (+1 dial).
        //
        // The forwarding sender will be used to send a join message
        // to the system actor that uses the `NetTx` just dialed so no
        // new `channel::dial()` for that (+0 dial). However, the
        // system actor will respond to the join message by using the
        // proc address (given in the join message) for the new proc
        // when it sends from its `DialMailboxRouter` so we expect to
        // see a `channel::dial()` there (+1 dial).
        let client2 = system.attach().await.unwrap();

        // Send a message to `client2` from `client1`. This will
        // involve forwarding to the system actor using `client1`'s
        // proc's forwarder already dialied `NetTx` (+0 dial). The
        // system actor will relay to `client2`'s proc. The `NetTx` to
        // that proc was cached in the system actor's
        // `DialmailboxRouter` when responding to `client2`'s join (+0
        // dial).
        let (port, mut port_rx) = client2.open_port();
        port.bind().send(&client1, 123u64).unwrap();
        assert_eq!(port_rx.recv().await.unwrap(), 123u64);

        // In summary we expect to see 4 dials.
        logs_assert(|logs| {
            let dial_count = logs
                .iter()
                .filter(|log| log.contains("dialing channel tcp"))
                .count();
            if dial_count == 4 {
                Ok(())
            } else {
                Err(format!("unexpected tcp channel dial count: {}", dial_count))
            }
        });

        system_handle.stop().await.unwrap();
        system_handle.await;
    }
}
