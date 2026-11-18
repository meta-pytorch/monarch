/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Proc-local singleton services.
//!
//! A singleton service is one actor per (proc, service name), created on
//! first request and reused by every later requester. Each proc runs one
//! [`ServiceRegistry`], installed next to its `ActorSpawner` when the proc
//! boots. The registry spawns each service as its own child under
//! `Uid::singleton(name)`, so the service's `ActorId` is (proc id, name),
//! which is unique because the proc id is unique. Requesters receive only
//! the service's address: they cannot stop it, and stopping a requester
//! never stops it. A service lives until it stops or fails on its own, or
//! until its proc stops.
//!
//! ## Invariants (SVC-*)
//!
//! - **SVC-1 (single creator):** The registry is the only creator of
//!   services on its proc. Handlers run serially, and the registry spawns a
//!   service only when it has no entry for the name; ensures for a service
//!   that is still initializing join it as waiters.
//! - **SVC-2 (single identity):** An entry records the service's actor type
//!   and serialized parameters. An ensure with an equal specification
//!   returns the existing actor; a different specification returns
//!   [`ServiceError::ConflictingSpec`]. The first creator's environment
//!   wins and is not part of the specification.
//! - **SVC-3 (terminal absorbing):** A service that fails to construct,
//!   fails during `init()`, or later stops or fails is terminal. Later
//!   ensures return [`ServiceError::Terminal`]; the registry never
//!   recreates it. A new proc has a new registry and fresh services.
//! - **SVC-4 (ready after init):** An ensure succeeds only once the service
//!   has finished `init()`. While the service is in `init()`, the registry
//!   sends each waiter [`EnsureServiceReply::Initializing`] every third of
//!   [`ACTOR_SPAWN_MAX_IDLE`], so a requester's idle timeout bounds the
//!   registry's silence, not the length of `init()`.
//! - **SVC-5 (fail closed):** Services are the registry's children. If the
//!   registry stops, its services stop with it, and no registry remains to
//!   create a duplicate.
//! - **SVC-6 (handler resilience):** Handlers never return an error, replies
//!   are non-returning, and service failures are absorbed, so neither a
//!   request nor a service can fault the registry. The handler awaits only
//!   the service's construction (`RemoteSpawn::new`); a per-service status
//!   watch outside the handler reports the end of `init()` and the terminal
//!   status, so a service whose `init()` ensures another service on the
//!   same proc cannot deadlock the registry.

use std::collections::HashMap;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorEnvironment;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Data;
use hyperactor::Endpoint as _;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::ProcAddr;
use hyperactor::RefClient;
use hyperactor::Uid;
use hyperactor::actor::ActorStatus;
use hyperactor::context;
use hyperactor::context::Actor as _;
use hyperactor::id::Label;
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;
use typeuri::Named;

use crate::proc_mesh::ACTOR_SPAWN_MAX_IDLE;

/// Well-known singleton name of a proc's service registry.
pub const SERVICE_REGISTRY_ACTOR_NAME: &str = "service_registry";

/// The singleton uid of the service registry on a proc.
pub fn service_registry_uid() -> Uid {
    Uid::singleton(Label::strip(SERVICE_REGISTRY_ACTOR_NAME))
}

/// Attest a reference to the service registry running on `proc`.
pub fn service_registry_ref(proc: &ProcAddr) -> ActorRef<ServiceRegistry> {
    ActorRef::attest(proc.actor_addr_uid(service_registry_uid()))
}

/// A typed failure from a service ensure request.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    Named,
    thiserror::Error
)]
pub enum ServiceError {
    /// The service was already created with a different actor type or
    /// different serialized parameters (SVC-2).
    #[error("conflicting specification for service `{name}`")]
    ConflictingSpec {
        /// The service name.
        name: Label,
    },

    /// The service failed to construct or initialize, or has since stopped
    /// or failed. It is not recreated (SVC-3).
    #[error("service `{name}` is terminal: {status}")]
    Terminal {
        /// The service name.
        name: Label,
        /// The service's terminal status.
        status: ActorStatus,
    },
}

/// Ensure that the singleton service `name` exists on the registry's proc,
/// creating it from `actor_type` and `params` on first request, and reply
/// with its address once it has finished `init()`. Until then, `reply`
/// receives periodic [`EnsureServiceReply::Initializing`] updates (SVC-4).
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub struct EnsureService {
    /// The service name; the actor is created under `Uid::singleton(name)`.
    pub name: Label,
    /// The registered remote actor type name.
    pub actor_type: String,
    /// The serialized `RemoteSpawn` parameters.
    pub params: Data,
    /// The environment the service is created with, if this request
    /// creates it.
    pub environment: ActorEnvironment,
    /// Typed reply channel: zero or more `Initializing` updates, then one
    /// `Done`.
    pub reply: PortRef<EnsureServiceReply>,
}
wirevalue::register_type!(EnsureService);

/// The typed reply to [`EnsureService`].
#[derive(Debug, Serialize, Deserialize, Named)]
pub enum EnsureServiceReply {
    /// The service is still in `init()` (SVC-4).
    Initializing,
    /// The final outcome of the ensure.
    Done(Result<ActorAddr, ServiceError>),
}

/// A service finished `init()` (a non-terminal status) or became terminal.
/// Posted to the registry by the service's status watch (SVC-4, SVC-6).
#[derive(Debug, Serialize, Deserialize, Named)]
struct ServiceStatus {
    name: Label,
    status: ActorStatus,
}

/// A service is still in `init()`. Posted to the registry periodically by
/// the service's status watch, and forwarded to its waiters (SVC-4).
#[derive(Debug, Serialize, Deserialize, Named)]
struct ServiceHeartbeat {
    name: Label,
}

/// The specification a service was created with (SVC-2).
#[derive(Debug, PartialEq)]
struct ServiceSpec {
    actor_type: String,
    params: Data,
}

#[derive(Debug)]
enum ServiceState {
    Initializing {
        spec: ServiceSpec,
        addr: ActorAddr,
        waiters: Vec<PortRef<EnsureServiceReply>>,
    },
    Running {
        spec: ServiceSpec,
        addr: ActorAddr,
    },
    Terminal {
        spec: ServiceSpec,
        status: ActorStatus,
    },
}

impl ServiceState {
    fn spec(&self) -> &ServiceSpec {
        match self {
            Self::Initializing { spec, .. }
            | Self::Running { spec, .. }
            | Self::Terminal { spec, .. } => spec,
        }
    }
}

/// The per-proc creator and owner of singleton services. See the module
/// docs for the ownership model and invariants.
#[derive(Debug, Default)]
#[hyperactor::export(handlers = [EnsureService])]
pub struct ServiceRegistry {
    services: HashMap<Label, ServiceState>,
}

impl ServiceRegistry {
    /// Record that service `name` finished `init()` (a non-terminal
    /// `status`) or became terminal, replying to any joined waiters. A
    /// terminal service stays terminal (SVC-3).
    fn resolve(&mut self, cx: &impl context::Actor, name: &Label, status: ActorStatus) {
        let Some(state) = self.services.remove(name) else {
            return;
        };
        let next = match state {
            ServiceState::Initializing {
                spec,
                addr,
                waiters,
            } => {
                let (next, result) = if status.is_terminal() {
                    let result = Err(ServiceError::Terminal {
                        name: name.clone(),
                        status: status.clone(),
                    });
                    (ServiceState::Terminal { spec, status }, result)
                } else {
                    let result = Ok(addr.clone());
                    (ServiceState::Running { spec, addr }, result)
                };
                for waiter in waiters {
                    waiter.post(cx, EnsureServiceReply::Done(result.clone()));
                }
                next
            }
            ServiceState::Running { spec, .. } if status.is_terminal() => {
                tracing::info!(service = %name, %status, "service is terminal");
                ServiceState::Terminal { spec, status }
            }
            state => state,
        };
        self.services.insert(name.clone(), next);
    }
}

#[async_trait]
impl Actor for ServiceRegistry {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        this.set_system();
        Ok(())
    }

    /// Service state follows each service's status watch, so a child's
    /// supervision event is only absorbed: a service failure never faults
    /// the registry (SVC-6).
    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl Handler<EnsureService> for ServiceRegistry {
    async fn handle(&mut self, cx: &Context<Self>, message: EnsureService) -> anyhow::Result<()> {
        let EnsureService {
            name,
            actor_type,
            params,
            environment,
            mut reply,
        } = message;
        reply.return_undeliverable(false);
        let spec = ServiceSpec { actor_type, params };

        if let Some(state) = self.services.get_mut(&name) {
            if state.spec() != &spec {
                reply.post(
                    cx,
                    EnsureServiceReply::Done(Err(ServiceError::ConflictingSpec { name })),
                );
                return Ok(());
            }
            match state {
                ServiceState::Initializing { waiters, .. } => waiters.push(reply),
                ServiceState::Running { addr, .. } => {
                    reply.post(cx, EnsureServiceReply::Done(Ok(addr.clone())));
                }
                ServiceState::Terminal { status, .. } => {
                    reply.post(
                        cx,
                        EnsureServiceReply::Done(Err(ServiceError::Terminal {
                            name,
                            status: status.clone(),
                        })),
                    );
                }
            }
            return Ok(());
        }

        // SVC-1: there is no entry for `name` and handlers run serially, so
        // no other request can create this service concurrently.
        let spawned = cx
            .instance()
            .gspawn_uid_in_environment(
                &spec.actor_type,
                Uid::singleton(name.clone()),
                spec.params.clone(),
                environment,
            )
            .await;
        let handle = match spawned {
            Ok(handle) => handle,
            Err(error) => {
                let status = ActorStatus::generic_failure(format!("spawn failed: {error}"));
                reply.post(
                    cx,
                    EnsureServiceReply::Done(Err(ServiceError::Terminal {
                        name: name.clone(),
                        status: status.clone(),
                    })),
                );
                self.services
                    .insert(name, ServiceState::Terminal { spec, status });
                return Ok(());
            }
        };

        self.services.insert(
            name.clone(),
            ServiceState::Initializing {
                spec,
                addr: handle.actor_id().clone(),
                waiters: vec![reply],
            },
        );

        // Watch the service outside this handler (SVC-6): heartbeat while it
        // is in `init()` (SVC-4), report the end of `init()`, then its
        // terminal status.
        let mut status_rx = handle.status();
        let port = cx.instance().port::<ServiceStatus>();
        let heartbeat_port = cx.instance().port::<ServiceHeartbeat>();
        let (watch_cx, _) = cx.instance().child();
        let mut heartbeat =
            tokio::time::interval(hyperactor_config::global::get(ACTOR_SPAWN_MAX_IDLE) / 3);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::spawn(async move {
            let initialized = loop {
                tokio::select! {
                    status = wait_status(&mut status_rx, |status| {
                        status.is_terminal()
                            || matches!(status, ActorStatus::Idle | ActorStatus::Processing(..))
                    }) => break status,
                    _ = heartbeat.tick() => {
                        let _ = heartbeat_port.try_post(
                            &watch_cx,
                            ServiceHeartbeat { name: name.clone() },
                        );
                    }
                }
            };
            let terminal = initialized.is_terminal();
            let _ = port.try_post(
                &watch_cx,
                ServiceStatus {
                    name: name.clone(),
                    status: initialized,
                },
            );
            if !terminal {
                let status = wait_status(&mut status_rx, ActorStatus::is_terminal).await;
                let _ = port.try_post(&watch_cx, ServiceStatus { name, status });
            }
        });
        Ok(())
    }
}

/// Wait until the watched status satisfies `done`. A closed channel means
/// the actor is gone, which is reported as a failure.
async fn wait_status(
    status: &mut watch::Receiver<ActorStatus>,
    done: impl FnMut(&ActorStatus) -> bool,
) -> ActorStatus {
    match status.wait_for(done).await {
        Ok(status) => status.clone(),
        Err(_) => ActorStatus::generic_failure("service status channel closed"),
    }
}

#[async_trait]
impl Handler<ServiceStatus> for ServiceRegistry {
    async fn handle(&mut self, cx: &Context<Self>, message: ServiceStatus) -> anyhow::Result<()> {
        self.resolve(cx, &message.name, message.status);
        Ok(())
    }
}

#[async_trait]
impl Handler<ServiceHeartbeat> for ServiceRegistry {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: ServiceHeartbeat,
    ) -> anyhow::Result<()> {
        if let Some(ServiceState::Initializing { waiters, .. }) = self.services.get(&message.name) {
            for waiter in waiters {
                waiter.post(cx, EnsureServiceReply::Initializing);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::Proc;
    use hyperactor::actor::remote::Remote;
    use hyperactor::context::Mailbox as _;
    use ndslice::ViewExt as _;
    use ndslice::extent;
    use timed_test::async_timed_test;

    use super::*;
    use crate::host_mesh::HostMesh;
    use crate::testactor::GatedInitActor;
    use crate::testactor::TestActor;
    use crate::testing;

    /// Stopping the registry stops its services (SVC-5).
    #[tokio::test]
    async fn test_services_stop_with_registry() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let registry = proc
            .spawn_bound_with_uid(service_registry_uid(), ServiceRegistry::default())
            .unwrap();

        let (reply, mut replies) = client.mailbox().open_port::<EnsureServiceReply>();
        registry.post(
            &client,
            EnsureService {
                name: Label::strip("svc_child"),
                actor_type: Remote::collect()
                    .name_of::<TestActor>()
                    .unwrap()
                    .to_string(),
                params: bincode::serde::encode_to_vec((), bincode::config::legacy()).unwrap(),
                environment: ActorEnvironment::default(),
                reply: reply.bind(),
            },
        );
        let addr = loop {
            match replies.recv().await.unwrap() {
                EnsureServiceReply::Initializing => continue,
                EnsureServiceReply::Done(result) => break result.unwrap(),
            }
        };
        let mut service_status = proc
            .get_instance(&addr)
            .expect("the service should be running")
            .status()
            .clone();
        assert!(
            !service_status.borrow().is_terminal(),
            "the service should be running before the registry stops",
        );

        registry.drain_and_stop("test").unwrap();
        registry.await;
        // A closed status channel means the service's cell is gone, which
        // also means it stopped.
        let stopped = tokio::time::timeout(
            Duration::from_secs(30),
            service_status.wait_for(ActorStatus::is_terminal),
        )
        .await
        .expect("the service should stop with its registry");
        if let Ok(status) = stopped {
            assert!(
                !status.is_failed(),
                "the service should stop cleanly with its registry: {}",
                *status,
            );
        }
        assert!(
            proc.get_instance(&addr).is_none(),
            "the service should no longer be live",
        );
    }

    /// An ensure replies only after the service finishes `init()` (SVC-4):
    /// while the service's `init()` is blocked, `spawn_service` stays
    /// pending, and releasing `init()` completes it.
    #[async_timed_test(timeout_secs = 120)]
    async fn test_spawn_service_waits_for_init() {
        let instance = testing::instance();
        let mut hm = HostMesh::local_in_process().await.unwrap();
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 1), None, None)
            .await
            .unwrap();

        let (started, mut started_rx) = instance.mailbox().open_port::<PortRef<()>>();
        let started = started.bind();
        let ensure =
            proc_mesh.spawn_service::<GatedInitActor, _>(&instance, "svc_gated_init", &started);
        tokio::pin!(ensure);
        let release = tokio::select! {
            result = &mut ensure => panic!(
                "ensure completed before the service entered init(): {:?}",
                result.map(|_| ()),
            ),
            release = started_rx.recv() => release.unwrap(),
        };
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut ensure)
                .await
                .is_err(),
            "ensure must not complete while the service is in init()",
        );

        release.post(&instance, ());
        let service = ensure.await.unwrap();
        assert_eq!(service.values().count(), 1);

        let _ = hm.shutdown(&instance).await;
    }

    /// A service whose `init()` outlasts `ACTOR_SPAWN_MAX_IDLE` still
    /// ensures: the registry's heartbeats reset the requester's idle
    /// timeout while `init()` runs (SVC-4).
    #[async_timed_test(timeout_secs = 120)]
    async fn test_spawn_service_init_outlasts_max_idle() {
        let max_idle = Duration::from_secs(1);
        let config = hyperactor_config::global::lock();
        let _idle = config.override_key(ACTOR_SPAWN_MAX_IDLE, max_idle);
        let instance = testing::instance();
        let mut hm = HostMesh::local_in_process().await.unwrap();
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 1), None, None)
            .await
            .unwrap();

        let (started, mut started_rx) = instance.mailbox().open_port::<PortRef<()>>();
        let started = started.bind();
        let ensure =
            proc_mesh.spawn_service::<GatedInitActor, _>(&instance, "svc_slow_init", &started);
        tokio::pin!(ensure);
        let release = tokio::select! {
            result = &mut ensure => panic!(
                "ensure completed before the service entered init(): {:?}",
                result.map(|_| ()),
            ),
            release = started_rx.recv() => release.unwrap(),
        };
        // Keep polling the ensure while `init()` runs well past the idle
        // timeout, so a timeout would surface here.
        tokio::select! {
            result = &mut ensure => panic!(
                "ensure must stay pending while init() runs: {:?}",
                result.map(|_| ()),
            ),
            _ = tokio::time::sleep(max_idle * 4) => {}
        }

        release.post(&instance, ());
        let service = ensure.await.unwrap();
        assert_eq!(service.values().count(), 1);

        let _ = hm.shutdown(&instance).await;
    }
}
