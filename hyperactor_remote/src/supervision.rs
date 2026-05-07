/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Remote supervision actors.
//!
//! [`Supervisor`] is the parent-side proxy. It links to a [`Worker`], starts
//! the configured link actor, forwards parent stop requests to the worker, and
//! re-raises worker-reported supervision events through Hyperactor's ordinary
//! `UnhandledSupervisionEvent` path.
//!
//! [`Worker`] is the worker-side container. It supervises one local child actor,
//! accepts one active remote supervision session, starts the worker-side link
//! actor, and reports child or link lifecycle events to the supervisor session.

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::AnyActorHandle;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::StopMode;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::supervision::ActorSupervisionEvent;

use crate::KeepaliveLink;
use crate::KeepaliveSupervisor;
use crate::Link;
use crate::LinkOptions;
use crate::OrphanPolicy;
use crate::RemoteActorDisposition;
use crate::SupervisedWorker;
use crate::WorkerSupervisor;

hyperactor::behavior!(WorkerLike, Link, SupervisedWorker);

#[derive(Debug)]
struct PendingStop {
    mode: StopMode,
    reason: String,
}

#[derive(Debug)]
struct SupervisorSession {
    child: ActorAddr,
    display_name: Option<String>,
}

/// Parent-side proxy for one remote supervision relationship.
#[derive(Debug)]
pub struct Supervisor {
    worker: ActorRef<WorkerLike>,
    link: Option<KeepaliveLink>,
    options: LinkOptions,
    session_id: hyperactor::Uid,
    link_handle: Option<ActorHandle<KeepaliveSupervisor>>,
    session: Option<SupervisorSession>,
    pending_stop: Option<PendingStop>,
}

impl Supervisor {
    /// Create a supervisor proxy for `worker`.
    pub fn new(worker: ActorRef<WorkerLike>, link: KeepaliveLink, options: LinkOptions) -> Self {
        Self::new_uid(worker, link, options, hyperactor::Uid::instance())
    }

    /// Create a supervisor proxy with an explicit session id.
    pub fn new_uid(
        worker: ActorRef<WorkerLike>,
        link: KeepaliveLink,
        options: LinkOptions,
        session_id: hyperactor::Uid,
    ) -> Self {
        Self {
            worker,
            link: Some(link),
            options,
            session_id,
            link_handle: None,
            session: None,
            pending_stop: None,
        }
    }
}

#[async_trait]
impl Actor for Supervisor {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let (link_handle, link) = self
            .link
            .take()
            .expect("supervisor initialized more than once")
            .spawn_supervisor(this)?;
        self.link_handle = Some(link_handle);
        self.worker.send(
            this,
            Link {
                session_id: self.session_id.clone(),
                supervisor: this.port::<WorkerSupervisor>().bind(),
                parent: this.self_addr().clone(),
                link,
                options: self.options.clone(),
            },
        )?;
        Ok(())
    }

    async fn handle_stop(
        &mut self,
        this: &Instance<Self>,
        mode: StopMode,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.pending_stop = Some(PendingStop {
            mode,
            reason: reason.to_string(),
        });
        self.send_pending_worker_stop(this)
    }

    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        if self
            .link_handle
            .as_ref()
            .is_some_and(|handle| handle.actor_addr() == &event.actor_id)
        {
            let event = self.synthesize_unreachable_event("supervision link failed");
            return self.propagate_event(event);
        }
        Ok(!event.is_error())
    }
}

#[async_trait]
impl Handler<WorkerSupervisor> for Supervisor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: WorkerSupervisor,
    ) -> anyhow::Result<()> {
        match message {
            WorkerSupervisor::Linked {
                session_id,
                child,
                display_name,
            } => {
                self.ensure_session(&session_id)?;
                self.session = Some(SupervisorSession {
                    child,
                    display_name,
                });
                self.send_pending_worker_stop(cx)?;
                Ok(())
            }
            WorkerSupervisor::LinkRejected { session_id, reason } => {
                self.ensure_session(&session_id)?;
                anyhow::bail!("remote supervision link rejected: {}", reason)
            }
            WorkerSupervisor::SupervisionEvent {
                session_id,
                event,
                disposition: _,
            } => {
                self.ensure_session(&session_id)?;
                self.propagate_event(event)
            }
            WorkerSupervisor::Unlinked { session_id, reason } => {
                self.ensure_session(&session_id)?;
                cx.exit(&reason)?;
                Ok(())
            }
        }
    }
}

impl Supervisor {
    fn ensure_session(&self, session_id: &hyperactor::Uid) -> anyhow::Result<()> {
        if session_id != &self.session_id {
            anyhow::bail!("remote supervision session id mismatch");
        }
        Ok(())
    }

    fn send_pending_worker_stop(&mut self, cx: &Instance<Self>) -> anyhow::Result<()> {
        let Some(stop) = self.pending_stop.take() else {
            return Ok(());
        };
        self.worker.send(
            cx,
            SupervisedWorker::Stop {
                session_id: self.session_id.clone(),
                mode: stop.mode,
                reason: stop.reason,
            },
        )?;
        Ok(())
    }

    fn synthesize_unreachable_event(&self, reason: &str) -> ActorSupervisionEvent {
        if let Some(session) = &self.session {
            ActorSupervisionEvent::new(
                session.child.clone(),
                session.display_name.clone(),
                ActorStatus::generic_failure(reason),
                None,
            )
        } else {
            ActorSupervisionEvent::new(
                self.worker.actor_addr().clone(),
                None,
                ActorStatus::generic_failure(reason),
                None,
            )
        }
    }

    fn propagate_event<T>(&self, event: ActorSupervisionEvent) -> anyhow::Result<T> {
        Err(anyhow::Error::new(
            ActorErrorKind::UnhandledSupervisionEvent(Box::new(event)),
        ))
    }
}

#[derive(Debug)]
struct WorkerSession {
    session_id: hyperactor::Uid,
    supervisor: PortRef<WorkerSupervisor>,
    options: LinkOptions,
    link_handle: AnyActorHandle,
}

/// Worker-side container for one remotely supervised child actor.
#[derive(Debug)]
#[hyperactor::export(Link, SupervisedWorker)]
pub struct Worker<C: Actor> {
    child: Option<C>,
    child_handle: Option<ActorHandle<C>>,
    child_display_name: Option<String>,
    session: Option<WorkerSession>,
}

impl<C: Actor> Worker<C> {
    /// Create a worker around `child`.
    pub fn new(child: C) -> Self {
        Self {
            child: Some(child),
            child_handle: None,
            child_display_name: None,
            session: None,
        }
    }
}

#[async_trait]
impl<C> Actor for Worker<C>
where
    C: Actor + Send + 'static,
{
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let child = self
            .child
            .take()
            .expect("worker initialized more than once");
        self.child_display_name = child.display_name();
        self.child_handle = Some(this.spawn(child)?);
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        if self.is_link_event(event) {
            self.handle_link_event(this, event)?;
            return Ok(true);
        }
        if self.is_child_event(event) {
            if let Some(session) = &self.session {
                let supervisor = session.supervisor.clone();
                let session_id = session.session_id.clone();
                if supervisor
                    .send(
                        this,
                        WorkerSupervisor::SupervisionEvent {
                            session_id,
                            event: event.clone(),
                            disposition: RemoteActorDisposition::Terminal,
                        },
                    )
                    .is_err()
                {
                    let _ = self.unlink_orphaned_supervisor("supervisor session undeliverable");
                }
            }
            self.child_handle = None;
            this.exit("supervised child stopped")?;
            return Ok(true);
        }
        Ok(!event.is_error())
    }

    async fn handle_undeliverable_message(
        &mut self,
        _this: &Instance<Self>,
        _envelope: Undeliverable<MessageEnvelope>,
    ) -> anyhow::Result<()> {
        self.handle_orphaned_supervisor("supervisor session undeliverable")
    }
}

#[async_trait]
impl<C> Handler<Link> for Worker<C>
where
    C: Actor + Send + 'static,
{
    async fn handle(&mut self, cx: &Context<Self>, message: Link) -> anyhow::Result<()> {
        if self.session.is_some() {
            let _ = message.supervisor.send(
                cx,
                WorkerSupervisor::LinkRejected {
                    session_id: message.session_id,
                    reason: "worker already linked".to_string(),
                },
            );
            return Ok(());
        }

        let link_handle = message.link.spawn_worker(cx).await?;
        let child_addr = self
            .child_handle
            .as_ref()
            .expect("worker child must be spawned before linking")
            .actor_addr()
            .clone();
        let supervisor = message.supervisor.clone();
        self.session = Some(WorkerSession {
            session_id: message.session_id.clone(),
            supervisor: message.supervisor,
            options: message.options,
            link_handle,
        });
        if supervisor
            .send(
                cx,
                WorkerSupervisor::Linked {
                    session_id: message.session_id.clone(),
                    child: child_addr,
                    display_name: self.child_display_name.clone(),
                },
            )
            .is_err()
        {
            self.handle_orphaned_supervisor("supervisor session undeliverable")?;
        }
        Ok(())
    }
}

#[async_trait]
impl<C> Handler<SupervisedWorker> for Worker<C>
where
    C: Actor + Send + 'static,
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: SupervisedWorker,
    ) -> anyhow::Result<()> {
        match message {
            SupervisedWorker::Stop {
                session_id,
                mode,
                reason,
            } => {
                self.ensure_session(&session_id)?;
                self.stop_child(mode, &reason)?;
            }
            SupervisedWorker::Unlink { session_id, reason } => {
                self.ensure_session(&session_id)?;
                if let Some(session) = self.session.take() {
                    let _ = session.link_handle.stop(&reason);
                    if session.options.orphan_policy == OrphanPolicy::Stop {
                        self.stop_child(StopMode::Stop, &reason)?;
                    }
                    let _ = session
                        .supervisor
                        .send(cx, WorkerSupervisor::Unlinked { session_id, reason });
                }
            }
        }
        Ok(())
    }
}

impl<C: Actor> Worker<C> {
    fn ensure_session(&self, session_id: &hyperactor::Uid) -> anyhow::Result<()> {
        let Some(session) = &self.session else {
            anyhow::bail!("worker is not linked");
        };
        if &session.session_id != session_id {
            anyhow::bail!("remote supervision session id mismatch");
        }
        Ok(())
    }

    fn stop_child(&self, mode: StopMode, reason: &str) -> anyhow::Result<()> {
        let Some(child) = &self.child_handle else {
            return Ok(());
        };
        match mode {
            StopMode::Stop => child.stop(reason)?,
            StopMode::DrainAndStop => child.drain_and_stop(reason)?,
        }
        Ok(())
    }

    fn is_child_event(&self, event: &ActorSupervisionEvent) -> bool {
        self.child_handle
            .as_ref()
            .is_some_and(|child| child.actor_addr() == &event.actor_id)
    }

    fn is_link_event(&self, event: &ActorSupervisionEvent) -> bool {
        self.session
            .as_ref()
            .is_some_and(|session| session.link_handle.actor_id() == &event.actor_id)
    }

    fn handle_orphaned_supervisor(&mut self, reason: &str) -> anyhow::Result<()> {
        if self.unlink_orphaned_supervisor(reason) == Some(OrphanPolicy::Stop) {
            self.stop_child(StopMode::Stop, reason)?;
        }
        Ok(())
    }

    fn unlink_orphaned_supervisor(&mut self, reason: &str) -> Option<OrphanPolicy> {
        let session = self.session.take()?;
        let _ = session.link_handle.stop(reason);
        Some(session.options.orphan_policy)
    }

    fn handle_link_event(
        &mut self,
        cx: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        let Some(session) = &self.session else {
            return Ok(());
        };
        let supervisor = session.supervisor.clone();
        let session_id = session.session_id.clone();
        let orphan_policy = session.options.orphan_policy;
        if let Some(child) = &self.child_handle {
            if supervisor
                .send(
                    cx,
                    WorkerSupervisor::SupervisionEvent {
                        session_id,
                        event: ActorSupervisionEvent::new(
                            child.actor_addr().clone(),
                            self.child_display_name.clone(),
                            ActorStatus::generic_failure(format!(
                                "supervision link failed: {}",
                                event
                            )),
                            None,
                        ),
                        disposition: RemoteActorDisposition::Unreachable,
                    },
                )
                .is_err()
            {
                self.handle_orphaned_supervisor("supervisor session undeliverable")?;
                return Ok(());
            }
        }
        if orphan_policy == OrphanPolicy::Stop {
            self.stop_child(StopMode::Stop, "supervision link failed")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::Bind;
    use hyperactor::PortHandle;
    use hyperactor::Proc;
    use hyperactor::Unbind;
    use serde::Deserialize;
    use serde::Serialize;
    use typeuri::Named;

    use super::*;

    #[derive(Clone, Debug, Serialize, Deserialize, Named, Bind, Unbind)]
    enum TestChildCommand {
        Fail,
    }
    wirevalue::register_type!(TestChildCommand);

    #[derive(Clone, Debug, Serialize, Deserialize, Named, Bind, Unbind)]
    struct KillParent;
    wirevalue::register_type!(KillParent);

    #[derive(Debug)]
    enum TestChildAction {
        FailAfter(Duration),
    }

    #[derive(Debug)]
    #[hyperactor::export(TestChildCommand)]
    struct TestChild {
        ready: PortHandle<ActorAddr>,
        stopped: PortHandle<String>,
        action: Option<TestChildAction>,
    }

    #[async_trait]
    impl Actor for TestChild {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            self.ready.send(this, this.self_addr().clone())?;
            if let Some(TestChildAction::FailAfter(delay)) = self.action.take() {
                this.self_message_with_delay(TestChildCommand::Fail, delay)?;
            }
            Ok(())
        }

        async fn handle_stop(
            &mut self,
            this: &Instance<Self>,
            mode: StopMode,
            reason: &str,
        ) -> anyhow::Result<()> {
            self.stopped.send(this, reason.to_string())?;
            this.close();
            match mode {
                StopMode::Stop => this.exit(reason)?,
                StopMode::DrainAndStop => this.exit_after_drain(reason)?,
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<TestChildCommand> for TestChild {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            message: TestChildCommand,
        ) -> anyhow::Result<()> {
            match message {
                TestChildCommand::Fail => cx.kill("test child failed")?,
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct Parent {
        supervisor: Option<Supervisor>,
        events: PortHandle<ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for Parent {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            this.spawn(
                self.supervisor
                    .take()
                    .expect("parent initialized more than once"),
            )?;
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
    #[hyperactor::export(KillParent)]
    struct Grandparent {
        parent: Option<Parent>,
        parent_addr: PortHandle<ActorAddr>,
        parent_handle: Option<ActorHandle<Parent>>,
        events: PortHandle<ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for Grandparent {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let parent = this.spawn(
                self.parent
                    .take()
                    .expect("grandparent initialized more than once"),
            )?;
            self.parent_addr.send(this, parent.actor_addr().clone())?;
            self.parent_handle = Some(parent);
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

    #[async_trait]
    impl Handler<KillParent> for Grandparent {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
            _message: KillParent,
        ) -> anyhow::Result<()> {
            self.parent_handle
                .as_ref()
                .expect("parent must be spawned before kill")
                .kill("test parent killed")?;
            Ok(())
        }
    }

    fn test_child(
        ready: PortHandle<ActorAddr>,
        stopped: PortHandle<String>,
        action: Option<TestChildAction>,
    ) -> TestChild {
        TestChild {
            ready,
            stopped,
            action,
        }
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: unused in this test
    // │   └── events port: receives parent-observed supervision events
    // ├── worker: Worker<TestChild>
    // │   ├── child: TestChild
    // │   │   └── self-schedules TestChildCommand::Fail
    // │   └── worker-side link actor: KeepaliveWorker
    // └── parent: Parent
    //     └── supervisor: Supervisor
    //         └── supervisor-side link actor: KeepaliveSupervisor
    //
    // TestChild fails, Worker reports the child event to Supervisor's private
    // WorkerSupervisor port, Supervisor re-raises it through
    // UnhandledSupervisionEvent, and Parent forwards the observed event to the
    // client events port.
    #[tokio::test]
    async fn test_child_failure_propagates_to_parent() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        let (ready, mut ready_rx) = client.open_port::<ActorAddr>();
        let (stopped, _stopped_rx) = client.open_port::<String>();
        let (events, mut event_rx) = client.open_port::<ActorSupervisionEvent>();
        let worker = proc
            .spawn(
                "worker",
                Worker::new(test_child(
                    ready,
                    stopped,
                    Some(TestChildAction::FailAfter(Duration::from_millis(200))),
                )),
            )
            .unwrap();
        let parent = proc
            .spawn(
                "parent",
                Parent {
                    supervisor: Some(Supervisor::new(
                        worker.bind::<WorkerLike>(),
                        KeepaliveLink::new(Duration::from_millis(5), Duration::from_secs(60)),
                        LinkOptions::default(),
                    )),
                    events,
                },
            )
            .unwrap();
        let child_addr = ready_rx.recv().await.unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(event.actor_id, child_addr);
        assert!(matches!(event.actor_status, ActorStatus::Failed(_)));

        parent.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: receives the child stop reason
    // │   └── events port: present but not expected to receive an event
    // ├── worker: Worker<TestChild>
    // │   ├── child: TestChild
    // │   └── worker-side link actor: KeepaliveWorker
    // └── parent: Parent
    //     └── supervisor: Supervisor
    //         └── supervisor-side link actor: KeepaliveSupervisor
    //
    // Stopping Parent stops Supervisor, Supervisor forwards the same stop mode
    // and reason to Worker, Worker stops TestChild, and TestChild reports the
    // reason through the client stopped port before the handles complete.
    #[tokio::test]
    async fn test_parent_stop_stops_remote_child_before_parent_finishes() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        let (ready, mut ready_rx) = client.open_port::<ActorAddr>();
        let (stopped, mut stopped_rx) = client.open_port::<String>();
        let (events, _event_rx) = client.open_port::<ActorSupervisionEvent>();
        let worker = proc
            .spawn("worker", Worker::new(test_child(ready, stopped, None)))
            .unwrap();
        let parent = proc
            .spawn(
                "parent",
                Parent {
                    supervisor: Some(Supervisor::new(
                        worker.bind::<WorkerLike>(),
                        KeepaliveLink::new(Duration::from_millis(5), Duration::from_secs(60)),
                        LinkOptions::default(),
                    )),
                    events,
                },
            )
            .unwrap();
        let _child_addr = ready_rx.recv().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        parent.stop("test parent stopping").unwrap();

        let reason = tokio::time::timeout(Duration::from_secs(5), stopped_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "parent stopping");

        tokio::time::timeout(Duration::from_secs(5), parent)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: receives the child stop reason
    // │   ├── supervisor port: receives the initial Linked message
    // │   └── supervisor-side link actor: KeepaliveSupervisor
    // └── worker: Worker<TestChild>
    //     ├── child: TestChild
    //     └── worker-side link actor: KeepaliveWorker
    //
    // Worker accepts Link and records an active supervisor session. The test
    // then injects an Undeliverable for a worker-sent message. Worker treats
    // the supervisor session as orphaned, applies OrphanPolicy::Stop, and
    // exits after the child stops.
    #[tokio::test]
    async fn test_worker_undeliverable_supervisor_session_stops_child() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        let (ready, mut ready_rx) = client.open_port::<ActorAddr>();
        let (stopped, mut stopped_rx) = client.open_port::<String>();
        let (supervisor, mut supervisor_rx) = client.open_port::<WorkerSupervisor>();
        let supervisor_ref = supervisor.bind();
        let worker = proc
            .spawn("worker", Worker::new(test_child(ready, stopped, None)))
            .unwrap();
        let (link_handle, link) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn_supervisor(&client)
                .unwrap();
        let _child_addr = ready_rx.recv().await.unwrap();
        let session_id = hyperactor::Uid::instance();

        worker
            .send(
                &client,
                Link {
                    session_id: session_id.clone(),
                    supervisor: supervisor_ref.clone(),
                    parent: client.self_addr().clone(),
                    link,
                    options: LinkOptions::default(),
                },
            )
            .unwrap();
        let linked = tokio::time::timeout(Duration::from_secs(5), supervisor_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(linked, WorkerSupervisor::Linked { .. }));

        let envelope = MessageEnvelope::serialize(
            worker.actor_addr().clone(),
            supervisor_ref.port_addr().clone(),
            &WorkerSupervisor::Unlinked {
                session_id,
                reason: "test".to_string(),
            },
            hyperactor_config::Flattrs::new(),
        )
        .unwrap();
        worker
            .port::<Undeliverable<MessageEnvelope>>()
            .send(&client, Undeliverable(envelope))
            .unwrap();

        let reason = tokio::time::timeout(Duration::from_secs(5), stopped_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "supervisor session undeliverable");

        let status = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
        assert!(matches!(status, ActorStatus::Stopped(_)));

        link_handle.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), link_handle)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: receives the child stop reason
    // │   └── events port: present but not expected to receive an event
    // ├── worker: Worker<TestChild>
    // │   ├── child: TestChild
    // │   └── worker-side link actor: KeepaliveWorker
    // └── grandparent: Grandparent
    //     └── parent: Parent
    //         └── supervisor: Supervisor
    //             └── supervisor-side link actor: KeepaliveSupervisor
    //
    // Killing Parent still tears down its local supervision subtree. Grandparent
    // handles the parent failure so the test process does not treat it as an
    // unhandled root failure. Parent stops Supervisor, Supervisor forwards the
    // stop to Worker, and Worker stops TestChild.
    #[tokio::test]
    async fn test_parent_kill_stops_remote_child() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        let (ready, mut ready_rx) = client.open_port::<ActorAddr>();
        let (stopped, mut stopped_rx) = client.open_port::<String>();
        let (events, mut event_rx) = client.open_port::<ActorSupervisionEvent>();
        let (parent_addr, mut parent_addr_rx) = client.open_port::<ActorAddr>();
        let worker = proc
            .spawn("worker", Worker::new(test_child(ready, stopped, None)))
            .unwrap();
        let grandparent = proc
            .spawn(
                "grandparent",
                Grandparent {
                    parent: Some(Parent {
                        supervisor: Some(Supervisor::new(
                            worker.bind::<WorkerLike>(),
                            KeepaliveLink::new(
                                Duration::from_millis(100),
                                Duration::from_millis(300),
                            ),
                            LinkOptions::default(),
                        )),
                        events: events.clone(),
                    }),
                    parent_addr,
                    parent_handle: None,
                    events,
                },
            )
            .unwrap();
        let _child_addr = ready_rx.recv().await.unwrap();
        let parent_addr = parent_addr_rx.recv().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        grandparent.send(&client, KillParent).unwrap();

        let reason = tokio::time::timeout(Duration::from_secs(5), stopped_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "parent stopping");

        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.actor_id, parent_addr);
        assert!(matches!(event.actor_status, ActorStatus::Failed(_)));

        grandparent.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), grandparent)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
    }
}
