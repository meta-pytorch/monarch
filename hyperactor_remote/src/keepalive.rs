/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Message-based link implementation.
//!
//! The link has two actors. `KeepaliveWorker` runs on the worker side and owns
//! the outbound keepalive stream. `KeepaliveSupervisor` runs on the supervisor
//! side and acknowledges each keepalive. The worker sends [`Keepalive`] with a
//! reply port for the worker's [`KeepaliveAck`] handler. The supervisor answers
//! with [`KeepaliveAck`] on that reply port. Either side may fail the link: the
//! worker fails if a keepalive is not acknowledged within `timeout`, and the
//! supervisor fails if no newer keepalive is delivered within `timeout`.
//!
//! Keepalives carry monotonically increasing generation numbers. A generation
//! identifies one worker-issued keepalive and the local timers derived from it.
//! We use generations instead of canceling timers: if a later generation has
//! already been observed, a delayed timer for an older generation is stale and
//! is ignored. This relies on Hyperactor's in-order delivery guarantee for
//! messages sent along a single actor-to-actor path. In particular, the
//! supervisor observes keepalives from one worker in generation order, and each
//! actor observes its own scheduled control messages in the order in which they
//! become deliverable. The generation checks make late timers harmless, while
//! ordered delivery preserves the meaning of "no newer generation arrived before
//! this deadline."
//!
//! The worker uses two internal self messages. [`SendKeepalive`] advances the
//! stream after `interval` if its generation is still current. [`AckDeadline`]
//! checks after `timeout` whether the corresponding generation was acknowledged.
//! The supervisor uses [`Deadline`] to check whether a newer generation arrived
//! before its `timeout`.

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

/// Keepalive timing parameters.
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
pub struct KeepaliveParams {
    /// Duration between keepalive messages sent by the worker side.
    pub interval: Duration,
    /// Maximum duration to wait for keepalive delivery and acknowledgment.
    ///
    /// The worker side waits this long for each acknowledgment, and the
    /// supervisor side uses this as the maximum accepted gap between
    /// keepalives.
    pub timeout: Duration,
}
wirevalue::register_type!(KeepaliveParams);

impl KeepaliveParams {
    /// Create keepalive timing parameters.
    ///
    /// The worker sends keepalives every `interval`. Each keepalive must be
    /// acknowledged within `timeout`, and the supervisor side also uses
    /// `timeout` as the maximum accepted gap between keepalives.
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self { interval, timeout }
    }
}

/// Keepalive link configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeepaliveLink(KeepaliveParams);

impl KeepaliveLink {
    /// Create a keepalive link configuration.
    ///
    /// The worker sends keepalives every `interval`. Each keepalive must be
    /// acknowledged within `timeout`, and the supervisor side also uses
    /// `timeout` as the maximum accepted gap between keepalives.
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self(KeepaliveParams::new(interval, timeout))
    }

    /// Create a keepalive link configuration from timing parameters.
    pub fn from_params(params: KeepaliveParams) -> Self {
        Self(params)
    }

    /// Spawn the supervisor side and return the worker-side link spec.
    pub fn spawn_supervisor<A: Actor>(
        self,
        this: &Instance<A>,
    ) -> anyhow::Result<(ActorHandle<KeepaliveSupervisor>, LinkSpec)> {
        self.spawn_supervisor_uid(this, Uid::instance())
    }

    /// Spawn the supervisor side and return the worker-side link spec,
    /// used to instantiate the worker side of the link.
    ///
    /// The passed uid determines the uid of the worker side implementation
    /// actor; it is specified by the supervisor to enable efficient group-style
    /// supervision.
    pub fn spawn_supervisor_uid<A: Actor>(
        self,
        this: &Instance<A>,
        uid: Uid,
    ) -> anyhow::Result<(ActorHandle<KeepaliveSupervisor>, LinkSpec)> {
        let params = self.0;
        let supervisor = this.spawn(KeepaliveSupervisor::new(KeepaliveSupervisorParams::new(
            params.timeout,
        )))?;
        let worker = KeepaliveWorkerParams::new(supervisor.port::<Keepalive>().bind(), params)
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
    /// Keepalive timing parameters.
    pub keepalive: KeepaliveParams,
}
wirevalue::register_type!(KeepaliveWorkerParams);

impl KeepaliveWorkerParams {
    /// Create keepalive worker parameters.
    pub fn new(supervisor: PortRef<Keepalive>, keepalive: KeepaliveParams) -> Self {
        Self {
            supervisor,
            keepalive,
        }
    }

    /// Create a worker-side link spec for a keepalive worker actor with a fresh uid.
    pub fn link_spec(self) -> anyhow::Result<LinkSpec> {
        self.link_spec_uid(Uid::instance())
    }

    /// Create a worker-side link spec for a keepalive worker actor with an explicit uid.
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
    /// Worker-issued keepalive generation.
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

/// Supervisor-side deadline for receiving a newer keepalive.
///
/// `generation` is the supervisor's current generation when the deadline is
/// scheduled. If the supervisor still has the same current generation when this
/// message is handled, no newer keepalive arrived before the deadline, and the
/// supervisor fails the link. If the current generation is larger, this message
/// is stale.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct Deadline {
    generation: u64,
}
wirevalue::register_type!(Deadline);

/// Worker-side timer for sending the next keepalive.
///
/// `generation` is the keepalive generation that scheduled this timer. The
/// worker sends the next keepalive only if this generation is still current. If
/// the worker has already advanced, this message is stale.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct SendKeepalive {
    generation: u64,
}
wirevalue::register_type!(SendKeepalive);

/// Worker-side deadline for receiving a keepalive acknowledgment.
///
/// `generation` is the keepalive generation that scheduled this deadline. If
/// the worker has not recorded an acknowledgment for this generation, or any
/// later generation, when the message is handled, the worker fails the link. If
/// the acknowledged generation is equal or larger, this deadline is satisfied.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct AckDeadline {
    generation: u64,
}
wirevalue::register_type!(AckDeadline);

/// Worker-side keepalive link actor.
///
/// `KeepaliveWorker` owns the generation sequence and handles supervisor
/// [`KeepaliveAck`] messages directly. On startup, the worker sends generation
/// 1 and schedules [`SendKeepalive`] and [`AckDeadline`] for that generation.
/// Each live [`SendKeepalive`] advances to the next generation, sends a new
/// [`Keepalive`], and schedules the next pair of timers. Each [`KeepaliveAck`]
/// records progress. Each [`AckDeadline`] proves that the matching generation
/// failed to receive an acknowledgment before `timeout`, unless a same-or-newer
/// acknowledgment has already been recorded.
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
            interval: params.keepalive.interval,
            timeout: params.keepalive.timeout,
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
impl Handler<KeepaliveAck> for KeepaliveWorker {
    async fn handle(&mut self, _cx: &Context<Self>, ack: KeepaliveAck) -> anyhow::Result<()> {
        self.acked_generation = self.acked_generation.max(ack.generation);
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
        self.supervisor.send(
            this,
            Keepalive {
                generation,
                reply: this.port::<KeepaliveAck>().bind().into_once(),
            },
        )?;

        this.self_message_with_delay(SendKeepalive { generation }, self.interval)?;
        this.self_message_with_delay(AckDeadline { generation }, self.timeout)?;
        Ok(())
    }
}

/// Supervisor-side keepalive link actor.
///
/// `KeepaliveSupervisor` records the maximum keepalive generation it has
/// accepted and acknowledges every [`Keepalive`] with the same generation. Each
/// accepted keepalive schedules a [`Deadline`] for the current generation. A
/// deadline is active only while its generation remains current; if a newer
/// keepalive has arrived, the deadline is stale.
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
        // A deadline is scheduled for the generation that was current at the
        // time. Equality means no newer keepalive arrived before the deadline.
        // A smaller deadline generation is stale. A larger deadline generation
        // cannot be produced by this actor before it observes that generation.
        if message.generation == self.generation {
            let reason = format!("keepalive missed for generation {}", message.generation);
            cx.kill(&reason)?;
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
                    link.spawn_worker(this).await?;
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
        let proc = Proc::isolated();
        let (parent, _parent_handle) = proc.client("parent").unwrap();
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
        let proc = Proc::isolated();
        let (client, _client_handle) = proc.client("client").unwrap();
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
                if reason == "actor explicitly aborted due to: keepalive missed for generation 0"
        ));
        assert!(!event.actor_id.is_root());

        parent.stop("test").unwrap();
        parent.await;
    }

    #[tokio::test]
    async fn test_keepalive_worker_sends_keepalives() {
        let proc = Proc::isolated();
        let (parent, _parent_handle) = proc.client("parent").unwrap();
        let supervisor = parent
            .spawn(KeepaliveSupervisor::new(KeepaliveSupervisorParams::new(
                Duration::from_secs(60),
            )))
            .unwrap();
        let worker = KeepaliveWorkerParams::new(
            supervisor.port::<Keepalive>().bind(),
            KeepaliveParams::new(Duration::from_millis(5), Duration::from_millis(50)),
        )
        .link_spec()
        .unwrap()
        .spawn_worker(&parent)
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
        let proc = Proc::isolated();
        let (client, _client_handle) = proc.client("client").unwrap();
        let (events, mut event_rx) = client.open_port::<ActorSupervisionEvent>();
        let supervisor = proc.spawn("silent_supervisor", SilentSupervisor).unwrap();
        let uid = Uid::instance();
        let link = KeepaliveWorkerParams::new(
            supervisor.port::<Keepalive>().bind(),
            KeepaliveParams::new(Duration::from_millis(100), Duration::from_millis(10)),
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
        let proc = Proc::isolated();
        let (parent, _parent_handle) = proc.client("parent").unwrap();

        let (supervisor, worker_link) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn_supervisor(&parent)
                .unwrap();
        let worker_uid = worker_link.uid().clone();
        let remote_worker = worker_link.spawn_worker(&parent).await.unwrap();

        assert_eq!(remote_worker.actor_id().uid(), &worker_uid);

        remote_worker.stop("test").unwrap();
        remote_worker.await;
        supervisor.stop("test").unwrap();
        supervisor.await;
    }
}
