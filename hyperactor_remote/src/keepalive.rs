/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Message-based link implementation.
//!
//! `KeepaliveWorker` initiates a stream of [`Keepalive`] messages and fails if
//! the supervisor does not acknowledge them. `KeepaliveSupervisor` responds to
//! keepalives and fails if the next expected keepalive is not delivered.

use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::Uid;
use hyperactor::Unbind;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::LinkSpec;

/// Keepalive link configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeepaliveLink {
    interval: Duration,
    timeout: Duration,
}

impl KeepaliveLink {
    /// Create a keepalive link configuration.
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self { interval, timeout }
    }

    /// Spawn the supervisor side and return the worker-side link spec.
    pub fn spawn<A: Actor>(
        self,
        this: &Instance<A>,
    ) -> anyhow::Result<(ActorHandle<KeepaliveSupervisor>, LinkSpec)> {
        self.spawn_uid(this, Uid::instance())
    }

    /// Spawn the supervisor side and return the worker-side link spec with an explicit uid.
    pub fn spawn_uid<A: Actor>(
        self,
        this: &Instance<A>,
        uid: Uid,
    ) -> anyhow::Result<(ActorHandle<KeepaliveSupervisor>, LinkSpec)> {
        let supervisor = this.spawn(KeepaliveSupervisor::new(KeepaliveSupervisorParams::new(
            self.timeout,
        )))?;
        let worker = KeepaliveWorkerParams::new(
            supervisor.port::<Keepalive>().bind(),
            self.interval,
            self.timeout,
        )
        .link_spec_uid(uid)?;
        Ok((supervisor, worker))
    }
}

/// Parameters for [`KeepaliveWorker`].
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub struct KeepaliveWorkerParams {
    /// Supervisor-side keepalive handler port.
    #[binding(include)]
    pub supervisor: PortRef<Keepalive>,
    /// Duration between keepalive sends.
    pub interval: Duration,
    /// Maximum duration to wait for each keepalive acknowledgment.
    pub timeout: Duration,
}
wirevalue::register_type!(KeepaliveWorkerParams);

impl KeepaliveWorkerParams {
    /// Create keepalive worker parameters.
    pub fn new(supervisor: PortRef<Keepalive>, interval: Duration, timeout: Duration) -> Self {
        Self {
            supervisor,
            interval,
            timeout,
        }
    }

    /// Create a link spec for a keepalive worker actor with a fresh uid.
    pub fn link_spec(self) -> anyhow::Result<LinkSpec> {
        self.link_spec_uid(Uid::instance())
    }

    /// Create a link spec for a keepalive worker actor with an explicit uid.
    pub fn link_spec_uid(self, uid: Uid) -> anyhow::Result<LinkSpec> {
        let params = bincode::serde::encode_to_vec(self, bincode::config::legacy())?;
        Ok(LinkSpec::for_actor_uid::<KeepaliveWorker>(uid, params))
    }
}

/// Parameters for [`KeepaliveSupervisor`].
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub struct KeepaliveSupervisorParams {
    /// Maximum duration between accepted keepalive messages.
    pub timeout: Duration,
}
wirevalue::register_type!(KeepaliveSupervisorParams);

impl KeepaliveSupervisorParams {
    /// Create keepalive supervisor parameters.
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

/// Keepalive request sent to a [`KeepaliveSupervisor`].
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub struct Keepalive {
    /// Supervisor-owned keepalive generation.
    pub generation: u64,
    /// Reply port that receives the keepalive acknowledgment.
    #[binding(include)]
    pub reply: OncePortRef<KeepaliveAck>,
}
wirevalue::register_type!(Keepalive);

/// Acknowledgment for an accepted keepalive.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub struct KeepaliveAck {
    /// Generation accepted by the link actor.
    pub generation: u64,
}
wirevalue::register_type!(KeepaliveAck);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct Deadline {
    generation: u64,
}
wirevalue::register_type!(Deadline);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct SendKeepalive {
    generation: u64,
}
wirevalue::register_type!(SendKeepalive);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct AckReceived {
    generation: u64,
}
wirevalue::register_type!(AckReceived);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct AckDeadline {
    generation: u64,
}
wirevalue::register_type!(AckDeadline);

/// Worker-side keepalive link actor.
#[derive(Debug)]
#[hyperactor::export]
pub struct KeepaliveWorker {
    supervisor: PortRef<Keepalive>,
    interval: Duration,
    timeout: Duration,
    generation: u64,
    acked_generation: u64,
}

#[async_trait]
impl Actor for KeepaliveWorker {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.send_keepalive(this)
    }
}

#[async_trait]
impl RemoteSpawn for KeepaliveWorker {
    type Params = KeepaliveWorkerParams;

    async fn new(params: KeepaliveWorkerParams, _environment: Flattrs) -> anyhow::Result<Self> {
        Ok(Self {
            supervisor: params.supervisor,
            interval: params.interval,
            timeout: params.timeout,
            generation: 0,
            acked_generation: 0,
        })
    }
}

#[async_trait]
impl Handler<SendKeepalive> for KeepaliveWorker {
    async fn handle(&mut self, cx: &Context<Self>, message: SendKeepalive) -> anyhow::Result<()> {
        if message.generation == self.generation {
            self.send_keepalive(cx)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<AckReceived> for KeepaliveWorker {
    async fn handle(&mut self, _cx: &Context<Self>, message: AckReceived) -> anyhow::Result<()> {
        self.acked_generation = self.acked_generation.max(message.generation);
        Ok(())
    }
}

#[async_trait]
impl Handler<AckDeadline> for KeepaliveWorker {
    async fn handle(&mut self, cx: &Context<Self>, message: AckDeadline) -> anyhow::Result<()> {
        if self.acked_generation < message.generation {
            cx.kill("keepalive acknowledgment missed")?;
        }
        Ok(())
    }
}

impl KeepaliveWorker {
    fn send_keepalive(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.generation += 1;
        let generation = self.generation;
        let (reply, ack_rx) = this.open_once_port::<KeepaliveAck>();
        self.supervisor.send(
            this,
            Keepalive {
                generation,
                reply: reply.bind(),
            },
        )?;

        let ack_port = this.port::<AckReceived>();
        tokio::spawn(async move {
            if let Ok(ack) = ack_rx.recv().await {
                let _ = ack_port.send(
                    Instance::<KeepaliveSupervisor>::self_client(),
                    AckReceived {
                        generation: ack.generation,
                    },
                );
            }
        });

        this.self_message_with_delay(SendKeepalive { generation }, self.interval)?;
        this.self_message_with_delay(AckDeadline { generation }, self.timeout)?;
        Ok(())
    }
}

/// Supervisor-side keepalive link actor.
#[derive(Debug)]
pub struct KeepaliveSupervisor {
    timeout: Duration,
    generation: u64,
}

#[async_trait]
impl Actor for KeepaliveSupervisor {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.schedule_deadline(this)
    }
}

#[async_trait]
impl Handler<Keepalive> for KeepaliveSupervisor {
    async fn handle(&mut self, cx: &Context<Self>, message: Keepalive) -> anyhow::Result<()> {
        self.generation = self.generation.max(message.generation);
        message.reply.send(
            cx,
            KeepaliveAck {
                generation: message.generation,
            },
        )?;
        self.schedule_deadline(cx)
    }
}

#[async_trait]
impl Handler<Deadline> for KeepaliveSupervisor {
    async fn handle(&mut self, cx: &Context<Self>, message: Deadline) -> anyhow::Result<()> {
        if message.generation == self.generation {
            cx.kill("keepalive missed")?;
        }
        Ok(())
    }
}

impl KeepaliveSupervisor {
    /// Create a supervisor-side keepalive actor.
    pub fn new(params: KeepaliveSupervisorParams) -> Self {
        Self {
            timeout: params.timeout,
            generation: 0,
        }
    }

    fn schedule_deadline(&self, this: &Instance<Self>) -> anyhow::Result<()> {
        this.self_message_with_delay(
            Deadline {
                generation: self.generation,
            },
            self.timeout,
        )?;
        Ok(())
    }
}

hyperactor::register_spawnable!(KeepaliveWorker);

#[cfg(test)]
mod tests {
    use hyperactor::Actor;
    use hyperactor::PortRef;
    use hyperactor::Proc;
    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::supervision::ActorSupervisionEvent;

    use super::*;

    #[derive(Debug)]
    enum ParentSpawn {
        Link(LinkSpec),
        Supervisor(KeepaliveSupervisor),
    }

    #[derive(Debug)]
    struct ParentActor {
        spawn: Option<ParentSpawn>,
        events: PortRef<ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for ParentActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            match self.spawn.take().unwrap() {
                ParentSpawn::Link(link) => {
                    link.spawn(this).await?;
                }
                ParentSpawn::Supervisor(supervisor) => {
                    this.spawn(supervisor)?;
                }
            }
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.send(this, event.clone())?;
            Ok(true)
        }
    }

    #[derive(Debug)]
    #[hyperactor::export(Keepalive)]
    struct SilentSupervisor;

    #[async_trait]
    impl Actor for SilentSupervisor {}

    #[async_trait]
    impl Handler<Keepalive> for SilentSupervisor {
        async fn handle(&mut self, _cx: &Context<Self>, _message: Keepalive) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_keepalive_supervisor_replies_to_keepalive() {
        let proc = Proc::local();
        let (parent, _parent_handle) = proc.instance("parent").unwrap();
        let supervisor = parent
            .spawn(KeepaliveSupervisor::new(KeepaliveSupervisorParams::new(
                Duration::from_secs(60),
            )))
            .unwrap();
        let (reply, ack_rx) = parent.open_once_port::<KeepaliveAck>();

        supervisor
            .send(
                &parent,
                Keepalive {
                    generation: 41,
                    reply: reply.bind(),
                },
            )
            .unwrap();

        let ack = ack_rx.recv().await.unwrap();
        assert_eq!(ack.generation, 41);

        supervisor.stop("test").unwrap();
        supervisor.await;
    }

    #[tokio::test]
    async fn test_keepalive_supervisor_failure_propagates_to_parent() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        let (events, mut event_rx) = client.open_port::<ActorSupervisionEvent>();
        let supervisor =
            KeepaliveSupervisor::new(KeepaliveSupervisorParams::new(Duration::from_millis(10)));
        let parent: ActorHandle<ParentActor> = proc
            .spawn(
                "parent",
                ParentActor {
                    spawn: Some(ParentSpawn::Supervisor(supervisor)),
                    events: events.bind(),
                },
            )
            .unwrap();

        let event = event_rx.recv().await.unwrap();

        assert!(matches!(
            event.actor_status,
            ActorStatus::Failed(ActorErrorKind::Generic(ref reason))
                if reason == "actor explicitly aborted due to: keepalive missed"
        ));
        assert!(!event.actor_id.is_root());

        parent.stop("test").unwrap();
        parent.await;
    }

    #[tokio::test]
    async fn test_keepalive_worker_sends_keepalives() {
        let proc = Proc::local();
        let (parent, _parent_handle) = proc.instance("parent").unwrap();
        let supervisor = parent
            .spawn(KeepaliveSupervisor::new(KeepaliveSupervisorParams::new(
                Duration::from_secs(60),
            )))
            .unwrap();
        let worker = KeepaliveWorkerParams::new(
            supervisor.port::<Keepalive>().bind(),
            Duration::from_millis(5),
            Duration::from_millis(50),
        )
        .link_spec()
        .unwrap()
        .spawn(&parent)
        .await
        .unwrap()
        .downcast::<KeepaliveWorker>()
        .unwrap();
        let status = worker.status();

        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(!status.borrow().is_terminal());

        worker.stop("test").unwrap();
        worker.await;
        supervisor.stop("test").unwrap();
        supervisor.await;
    }

    #[tokio::test]
    async fn test_keepalive_worker_failure_propagates_to_parent() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        let (events, mut event_rx) = client.open_port::<ActorSupervisionEvent>();
        let supervisor = proc.spawn("silent_supervisor", SilentSupervisor).unwrap();
        let uid = Uid::instance();
        let link = KeepaliveWorkerParams::new(
            supervisor.port::<Keepalive>().bind(),
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .link_spec_uid(uid.clone())
        .unwrap();
        let parent: ActorHandle<ParentActor> = proc
            .spawn(
                "parent",
                ParentActor {
                    spawn: Some(ParentSpawn::Link(link)),
                    events: events.bind(),
                },
            )
            .unwrap();

        let event = event_rx.recv().await.unwrap();

        assert_eq!(event.actor_id.uid(), &uid);
        assert!(matches!(
            event.actor_status,
            ActorStatus::Failed(ActorErrorKind::Generic(ref reason))
                if reason == "actor explicitly aborted due to: keepalive acknowledgment missed"
        ));

        parent.stop("test").unwrap();
        parent.await;
        supervisor.stop("test").unwrap();
        supervisor.await;
    }

    #[tokio::test]
    async fn test_keepalive_link_spawn_mints_shared_worker_spec() {
        let proc = Proc::local();
        let (parent, _parent_handle) = proc.instance("parent").unwrap();

        let (supervisor, worker_link) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn(&parent)
                .unwrap();
        let worker_uid = worker_link.uid().clone();
        let remote_worker = worker_link.spawn(&parent).await.unwrap();

        assert_eq!(remote_worker.actor_id().uid(), &worker_uid);

        remote_worker.stop("test").unwrap();
        remote_worker.await;
        supervisor.stop("test").unwrap();
        supervisor.await;
    }
}
