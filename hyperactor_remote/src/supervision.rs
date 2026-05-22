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
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::StopMode;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::UndeliverableReason;
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionId(hyperactor::Uid);

impl SessionId {
    fn into_uid(self) -> hyperactor::Uid {
        self.0
    }
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
        Self::new_uid(worker, link, options, hyperactor::Uid::anonymous())
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
        (&self.worker).post(
            this,
            Link {
                session_id: self.session_id.clone(),
                supervisor: this.port::<WorkerSupervisor>().bind(),
                parent: this.self_addr().clone(),
                link,
                options: self.options.clone(),
            },
        );
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
        (&self.worker).post(
            cx,
            SupervisedWorker::Stop {
                session_id: self.session_id.clone(),
                mode: stop.mode,
                reason: stop.reason,
            },
        );
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
    session_id: SessionId,
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
                supervisor.post(
                    this,
                    WorkerSupervisor::SupervisionEvent {
                        session_id: session_id.into_uid(),
                        event: event.clone(),
                        disposition: RemoteActorDisposition::Terminal,
                    },
                );
            }
            self.child_handle = None;
            this.exit("supervised child stopped")?;
            return Ok(true);
        }

        debug_assert!(
            self.session.is_none(),
            "worker with active session received supervision event for non-link non-child actor: {:?}",
            event.actor_id
        );
        if self.session.is_none() {
            Ok(true)
        } else {
            Ok(!event.is_error())
        }
    }

    async fn handle_undeliverable_message(
        &mut self,
        _this: &Instance<Self>,
        _reason: UndeliverableReason,
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
            let _ = message.supervisor.post(
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
        let session_id = SessionId(message.session_id.clone());
        self.session = Some(WorkerSession {
            session_id,
            supervisor: message.supervisor,
            options: message.options,
            link_handle,
        });
        supervisor.post(
            cx,
            WorkerSupervisor::Linked {
                session_id: message.session_id.clone(),
                child: child_addr,
                display_name: self.child_display_name.clone(),
            },
        );
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
                self.accept_session_id(session_id)?;
                self.stop_child(mode, &reason)?;
            }
            SupervisedWorker::Unlink { session_id, reason } => {
                let session_id = self.accept_session_id(session_id)?;
                if let Some(session) = self.session.take() {
                    let _ = session.link_handle.stop(&reason);
                    match session.options.orphan_policy {
                        OrphanPolicy::Stop => self.stop_child(StopMode::Stop, &reason)?,
                        OrphanPolicy::Detach => (),
                    }

                    let _ = session.supervisor.post(
                        cx,
                        WorkerSupervisor::Unlinked {
                            session_id: session_id.into_uid(),
                            reason,
                        },
                    );
                }
            }
        }
        Ok(())
    }
}

impl<C: Actor> Worker<C> {
    fn accept_session_id(&self, raw: hyperactor::Uid) -> anyhow::Result<SessionId> {
        let Some(session) = &self.session else {
            anyhow::bail!("worker is not linked");
        };
        let session_id = SessionId(raw);
        if session_id != session.session_id {
            anyhow::bail!("remote supervision session id mismatch");
        }
        Ok(session_id)
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
        match self.unlink_orphaned_supervisor(reason) {
            Some(OrphanPolicy::Stop) => self.stop_child(StopMode::Stop, reason),
            Some(OrphanPolicy::Detach) | None => Ok(()),
        }
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
            supervisor.post(
                cx,
                WorkerSupervisor::SupervisionEvent {
                    session_id: session_id.into_uid(),
                    event: ActorSupervisionEvent::new(
                        child.actor_addr().clone(),
                        self.child_display_name.clone(),
                        ActorStatus::generic_failure(format!("supervision link failed: {}", event)),
                        None,
                    ),
                    disposition: RemoteActorDisposition::Unreachable,
                },
            );
        }
        let session = self
            .session
            .take()
            .expect("link event must have an active session");
        let _ = session.link_handle.stop("supervision link failed");
        if orphan_policy == OrphanPolicy::Stop {
            self.stop_child(StopMode::Stop, "supervision link failed")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use hyperactor::Bind;
    use hyperactor::PortHandle;
    use hyperactor::Proc;
    use hyperactor::Uid;
    use hyperactor::Unbind;
    use hyperactor::mailbox::PortReceiver;
    use serde::Deserialize;
    use serde::Serialize;
    use tokio::sync::Notify;
    use typeuri::Named;

    use super::*;

    #[derive(Clone, Debug, Serialize, Deserialize, Named, Bind, Unbind)]
    enum TestChildCommand {
        Fail,
        Drain(String),
    }
    wirevalue::register_type!(TestChildCommand);

    #[derive(Clone, Debug, Serialize, Deserialize, Named, Bind, Unbind)]
    struct KillParent;
    wirevalue::register_type!(KillParent);

    #[derive(Debug)]
    enum TestChildAction {
        FailAfter(Duration),
        DrainAfter(Duration, String),
    }

    #[derive(Debug)]
    #[hyperactor::export(TestChildCommand)]
    struct TestChild {
        ready: PortHandle<ActorAddr>,
        stopped: PortHandle<String>,
        action: Option<TestChildAction>,
        drain_observer: Option<PortHandle<String>>,
    }

    impl TestChild {
        fn with_drain_observer(mut self, port: PortHandle<String>) -> Self {
            self.drain_observer = Some(port);
            self
        }
    }

    #[async_trait]
    impl Actor for TestChild {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            self.ready.post(this, this.self_addr().clone());
            match self.action.take() {
                Some(TestChildAction::FailAfter(delay)) => {
                    this.post_after(this, TestChildCommand::Fail, delay);
                }
                Some(TestChildAction::DrainAfter(delay, tag)) => {
                    this.post_after(this, TestChildCommand::Drain(tag), delay);
                }
                None => {}
            }
            Ok(())
        }

        async fn handle_stop(
            &mut self,
            this: &Instance<Self>,
            mode: StopMode,
            reason: &str,
        ) -> anyhow::Result<()> {
            self.stopped.post(this, reason.to_string());
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
                TestChildCommand::Drain(tag) => {
                    if let Some(ref port) = self.drain_observer {
                        port.post(cx, tag);
                    }
                }
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
            self.events.post(this, event.clone());
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
            self.parent_addr.post(this, parent.actor_addr().clone());
            self.parent_handle = Some(parent);
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.post(this, event.clone());
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
            drain_observer: None,
        }
    }

    // Sets up the `Parent → Supervisor → Worker<TestChild>` harness
    // (not Grandparent or synthetic-link). Spawns the worker, awaits
    // the child's ready signal, then spawns the parent with
    // `Supervisor::new_uid` using the supplied session id and
    // options. Any post-spawn `Link`/`Linked` sleep is intentionally
    // omitted - tests add it themselves next to the message send that
    // depends on it.
    async fn spawn_supervised_pair(
        proc: &Proc,
        inst: &Instance<()>,
        session_id: hyperactor::Uid,
        options: LinkOptions,
        child_action: Option<TestChildAction>,
    ) -> (
        ActorAddr,                           // child address
        ActorHandle<Worker<TestChild>>,      // worker handle
        ActorHandle<Parent>,                 // parent handle
        PortReceiver<String>,                // child stop notification
        PortReceiver<ActorSupervisionEvent>, // parent's supervision events
    ) {
        // The ready receiver is consumed since no test needs more
        // than one child address.
        let (ready, mut ready_rx) = inst.open_port::<ActorAddr>();
        let (stopped, stopped_rx) = inst.open_port::<String>();
        let (events, events_rx) = inst.open_port::<ActorSupervisionEvent>();
        let worker = proc
            .spawn(
                "worker",
                Worker::new(test_child(ready, stopped, child_action)),
            )
            .unwrap();
        let child_addr = ready_rx.recv().await.unwrap();
        // Spawning Parent starts the link handshake: Parent::init
        // spawns the Supervisor whose own init sends `Link` to the
        // worker.
        let parent = proc
            .spawn(
                "parent",
                Parent {
                    supervisor: Some(Supervisor::new_uid(
                        worker.bind::<WorkerLike>(),
                        KeepaliveLink::new(Duration::from_millis(5), Duration::from_secs(60)),
                        options,
                        session_id,
                    )),
                    events,
                },
            )
            .unwrap();
        (child_addr, worker, parent, stopped_rx, events_rx)
    }

    #[derive(Debug)]
    #[hyperactor::export(Link, SupervisedWorker)]
    struct TestSlowWorker {
        link_started: PortHandle<()>,
        release_link: Arc<Notify>,
        received_stop: PortHandle<String>,
        session: Option<(hyperactor::Uid, PortRef<WorkerSupervisor>)>,
    }

    #[async_trait]
    impl Actor for TestSlowWorker {
        async fn init(&mut self, _this: &Instance<Self>) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<Link> for TestSlowWorker {
        async fn handle(&mut self, cx: &Context<Self>, message: Link) -> anyhow::Result<()> {
            self.session = Some((message.session_id, message.supervisor));
            self.link_started.post(cx, ());
            self.release_link.notified().await;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<SupervisedWorker> for TestSlowWorker {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            message: SupervisedWorker,
        ) -> anyhow::Result<()> {
            if let SupervisedWorker::Stop {
                session_id, reason, ..
            } = message
            {
                let Some((expected_session_id, supervisor)) = self.session.take() else {
                    anyhow::bail!("stop received before link");
                };
                anyhow::ensure!(session_id == expected_session_id, "unexpected session id");
                self.received_stop.post(cx, reason.clone());
                let _ = supervisor.post(cx, WorkerSupervisor::Unlinked { session_id, reason });
            }
            Ok(())
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
        let proc = Proc::isolated();
        let (client, _client_handle) = proc.client("client").unwrap();
        let session_id = Uid::anonymous();

        let (child_addr, worker, parent, _stopped_rx, mut event_rx) = spawn_supervised_pair(
            &proc,
            &client,
            session_id.clone(),
            LinkOptions::default(),
            Some(TestChildAction::FailAfter(Duration::from_millis(200))),
        )
        .await;

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
        let proc = Proc::isolated();
        let (client, _client_handle) = proc.client("client").unwrap();
        let session_id = Uid::anonymous();

        let (_child_addr, worker, parent, mut stopped_rx, _event_rx) = spawn_supervised_pair(
            &proc,
            &client,
            session_id.clone(),
            LinkOptions::default(),
            None,
        )
        .await;

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
        let proc = Proc::isolated();
        let (client, _client_handle) = proc.client("client").unwrap();
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
        let session_id = hyperactor::Uid::anonymous();

        worker.post(
            &client,
            Link {
                session_id: session_id.clone(),
                supervisor: supervisor_ref.clone(),
                parent: client.self_addr().clone(),
                link,
                options: LinkOptions::default(),
            },
        );
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
            .post(&client, Undeliverable::Returned(envelope));

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
        let proc = Proc::isolated();
        let (client, _client_handle) = proc.client("client").unwrap();
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
        grandparent.post(&client, KillParent);

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

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: not expected to fire (Detach leaves child running)
    // │   ├── supervisor port: receives the initial Linked message
    // │   └── supervisor-side link actor: KeepaliveSupervisor
    // └── worker: Worker<TestChild>
    //     ├── child: TestChild
    //     └── worker-side link actor: KeepaliveWorker
    //
    // Worker accepts Link with OrphanPolicy::Detach and records an
    // active supervisor session. The test then injects an
    // Undeliverable for a worker-sent message. Worker treats the
    // supervisor session as orphaned, stops the link actor and clears
    // the session, but does NOT stop the child. Worker remains alive
    // with no active session.
    #[tokio::test]
    async fn test_detach_orphan_policy_leaves_child_running_on_supervisor_loss() {
        let proc = Proc::isolated();
        let (inst, _client_handle) = proc.client("inst").unwrap();
        let (ready, mut ready_rx) = inst.open_port::<ActorAddr>();
        let (stopped, mut stopped_rx) = inst.open_port::<String>();
        let (supervisor, mut supervisor_rx) = inst.open_port::<WorkerSupervisor>();
        let supervisor_ref = supervisor.bind();
        let worker = proc
            .spawn("worker", Worker::new(test_child(ready, stopped, None)))
            .unwrap();
        let (keep_alive, link_spec) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn_supervisor(&inst)
                .unwrap();
        let _child_addr = ready_rx.recv().await.unwrap();
        let session_id = hyperactor::Uid::anonymous();

        worker.post(
            &inst,
            Link {
                session_id: session_id.clone(),
                supervisor: supervisor_ref.clone(),
                parent: inst.self_addr().clone(),
                link: link_spec,
                options: LinkOptions {
                    orphan_policy: OrphanPolicy::Detach,
                    ..Default::default()
                },
            },
        );
        let linked = tokio::time::timeout(Duration::from_secs(5), supervisor_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(linked, WorkerSupervisor::Linked { .. }));

        // Forge an undeliverable bounce for a worker-to-supervisor
        // message - a production trigger for
        // `handle_orphaned_supervisor` where `OrphanPolicy` is
        // applied.
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
            .post(&inst, Undeliverable::Returned(envelope));

        // Under `Detach`, the worker clears its session and stops the
        // link, but does NOT stop the child. No message shhould arrive on
        // the stopped port within a generous timeout.
        let timed_out = tokio::time::timeout(Duration::from_millis(500), stopped_rx.recv()).await;
        assert!(
            timed_out.is_err(),
            "child should not be stopped under OrphanPolicy::Detach"
        );

        // The worker itself is still alive. Cleanly stopping it should
        // succeed and yield a Stopped status.
        worker.stop("test").unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
        assert!(
            matches!(status, ActorStatus::Stopped(_)),
            "worker should accept clean stop after Detach orphan, got {:?}",
            status
        );

        keep_alive.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), keep_alive)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: receives the child stop reason
    // │   └── events port: receives parent-observed supervision events
    // ├── worker: Worker<TestChild>
    // │   ├── child: TestChild
    // │   └── worker-side link actor: KeepaliveWorker
    // └── parent: Parent
    //     └── supervisor: Supervisor (constructed with a known session id)
    //         └── supervisor-side link actor: KeepaliveSupervisor
    //
    // After the link is established the test sends
    // `SupervisedWorker::Unlink` to the worker with the matching
    // session id. Under `OrphanPolicy::Stop` the worker stops the link
    // actor, stops the child with the unlink reason, and sends
    // `Unlinked` back. The supervisor receives `Unlinked` and exits
    // cleanly via `cx.exit(reason)`; the parent observes that exit as a
    // `Stopped(reason)` supervision event. The worker then observes the
    // child's terminal event and exits as well.
    #[tokio::test]
    async fn test_supervised_worker_unlink_stops_child_under_stop_policy() {
        let proc = Proc::isolated();
        let (inst, _inst_hndl) = proc.client("inst").unwrap();
        let session_id = Uid::anonymous();

        let (_child_addr, worker, parent, mut stopped_rx, mut events_rx) = spawn_supervised_pair(
            &proc,
            &inst,
            session_id.clone(),
            LinkOptions::default(),
            None,
        )
        .await;

        // Give the Link/Linked handshake time to complete before
        // issuing Unlink.
        tokio::time::sleep(Duration::from_millis(100)).await;

        worker.post(
            &inst,
            SupervisedWorker::Unlink {
                session_id: session_id.clone(),
                reason: "test unlink".to_string(),
            },
        );

        // Under OrphanPolicy::Stop the worker stops the child with
        // the unlink reason.
        let reason = tokio::time::timeout(Duration::from_secs(5), stopped_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "test unlink");

        // Supervisor receives `Unlinked` and exits cleanly via
        // cx.exit; Parent observes that supervisor exit as a
        // `Stopped(reason)` event.
        let event = tokio::time::timeout(Duration::from_secs(5), events_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match event.actor_status {
            ActorStatus::Stopped(reason) => assert_eq!(reason, "test unlink"),
            status => panic!("expected supervisor Stopped event, got {:?}", status),
        }

        // Worker observed the child's terminal event and exited as
        // well. No `SupervisionEvent { Terminal }` is forwarded
        // because the Unlink handler already cleared the session -
        // `Unlinked` is the sole protocol message in this path.
        let status = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
        assert!(matches!(status, ActorStatus::Stopped(_)));

        parent.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: not expected to fire (Detach leaves child running)
    // │   └── events port: receives parent-observed supervision events
    // ├── worker: Worker<TestChild>
    // │   ├── child: TestChild
    // │   └── worker-side link actor: KeepaliveWorker
    // └── parent: Parent
    //     └── supervisor: Supervisor (constructed with a known session id)
    //         └── supervisor-side link actor: KeepaliveSupervisor
    //
    // After the link is established the test sends
    // `SupervisedWorker::Unlink` to the worker with the matching
    // session id. Under `OrphanPolicy::Detach` the worker stops the
    // link actor and clears the session but does NOT stop the child; it
    // then sends `Unlinked` back. The supervisor receives `Unlinked`
    // and exits cleanly via `cx.exit(reason)`; the parent observes that
    // exit as a `Stopped(reason)` supervision event. The child and the
    // worker both remain alive; the test cleanly stops the worker for
    // teardown.
    #[tokio::test]
    async fn test_supervised_worker_unlink_leaves_child_running_under_detach_policy() {
        let proc = Proc::isolated();
        let (inst, _inst_hndl) = proc.client("inst").unwrap();
        let session_id = Uid::anonymous();

        let (_child_addr, worker, parent, mut stopped_rx, mut events_rx) = spawn_supervised_pair(
            &proc,
            &inst,
            session_id.clone(),
            LinkOptions {
                orphan_policy: OrphanPolicy::Detach,
                ..Default::default()
            },
            None,
        )
        .await;

        // Give the Link/Linked handshake time to complete before
        // issuing Unlink.
        tokio::time::sleep(Duration::from_millis(100)).await;

        worker.post(
            &inst,
            SupervisedWorker::Unlink {
                session_id: session_id.clone(),
                reason: "test unlink".to_string(),
            },
        );

        // Supervisor receives `Unlinked` and exits cleanly via cx.exit;
        // Parent observes that supervisor exit as a `Stopped(reason)`
        // event. This is the positive signal that the Unlink round-trip
        // has completed; only after this can we meaningfully assert the
        // negative (no child stop).
        let event = tokio::time::timeout(Duration::from_secs(5), events_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match event.actor_status {
            ActorStatus::Stopped(reason) => assert_eq!(reason, "test unlink"),
            status => panic!("expected supervisor Stopped event, got {:?}", status),
        }

        // Under OrphanPolicy::Detach the worker does NOT stop the
        // child. The stopped port should receive no message within a
        // generous timeout window.
        let timed_out = tokio::time::timeout(Duration::from_millis(500), stopped_rx.recv()).await;
        assert!(
            timed_out.is_err(),
            "child should not be stopped under OrphanPolicy::Detach"
        );

        // The worker is still alive: Unlink left the child running, so
        // no child-terminal event cascaded the worker into
        // `this.exit("supervised child stopped")`. A clean stop
        // succeeds and yields Stopped(_).
        worker.stop("test").unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
        assert!(
            matches!(status, ActorStatus::Stopped(_)),
            "worker should accept clean stop after Detach unlink, got {:?}",
            status
        );

        parent.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address (consumed by helper)
    // │   ├── stopped port: not expected to fire
    // │   ├── events1 port: parent1's supervision events (silent — first link succeeds)
    // │   └── events2 port: parent2's supervision events (Failed with bail reason)
    // ├── worker: Worker<TestChild>
    // │   ├── child: TestChild
    // │   └── worker-side link actor: KeepaliveWorker (under session_id1)
    // ├── parent1: Parent
    // │   └── supervisor1: Supervisor (session_id1) — wins the race, receives Linked
    // └── parent2: Parent
    //     └── supervisor2: Supervisor (session_id2) — rejected, bails
    //
    // Spawn worker + parent1 via the helper, sleep 100ms so the
    // worker has `self.session = Some(...)` before parent2's Link
    // arrives, then spawn a second parent whose Supervisor targets
    // the same worker with a different session id. supervisor2 sends
    // `Link`, the worker sees `self.session.is_some()` and replies
    // with `WorkerSupervisor::LinkRejected { reason: "worker already
    // linked" }` (`supervision.rs:181`). supervisor2's
    // `Handler<WorkerSupervisor>` arm bails with "remote supervision
    // link rejected: worker already linked", supervisor2 fails,
    // parent2 observes that failure on events_rx2.
    #[tokio::test]
    async fn test_concurrent_link_rejects_second_supervisor() {
        let proc = Proc::isolated();
        let (inst, _inst_hndl) = proc.client("inst").unwrap();
        let session_id1 = Uid::anonymous();
        let (_child_addr, worker, parent1, _stopped_rx, mut events_rx1) = spawn_supervised_pair(
            &proc,
            &inst,
            session_id1.clone(),
            LinkOptions::default(),
            None,
        )
        .await;

        // Let the first Link/Linked handshake complete so the worker
        // has self.session.is_some() = true before parent2's Link
        // arrives.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Spawn a second parent whose Supervisor targets the same
        // worker with a different session id. Its Link will be
        // rejected.
        let session_id2 = Uid::anonymous();
        let (events2, mut events_rx2) = inst.open_port::<ActorSupervisionEvent>();
        let parent2 = proc
            .spawn(
                "parent2",
                Parent {
                    supervisor: Some(Supervisor::new_uid(
                        worker.bind::<WorkerLike>(),
                        KeepaliveLink::new(Duration::from_millis(5), Duration::from_secs(60)),
                        LinkOptions::default(),
                        session_id2.clone(),
                    )),
                    events: events2,
                },
            )
            .unwrap();

        // supervisor2 receives WorkerSupervisor::LinkRejected and
        // bails with "remote supervision link rejected: worker
        // already linked" (supervision.rs:181). parent2 observes that
        // failure.
        let event = tokio::time::timeout(Duration::from_secs(5), events_rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event.actor_status, ActorStatus::Failed(_)),
            "expected supervisor2 Failed event, got {:?}",
            event.actor_status
        );
        assert!(
            event
                .to_string()
                .contains("remote supervision link rejected: worker already linked"),
            "expected LinkRejected bail string in failure message, got: {}",
            event
        );

        // parent1's supervisor stays linked and healthy: its events
        // port should not receive any failure within a short window.
        let timed_out = tokio::time::timeout(Duration::from_millis(200), events_rx1.recv()).await;
        assert!(
            timed_out.is_err(),
            "first supervisor should remain linked, no failure expected"
        );

        parent1.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent1)
            .await
            .unwrap();
        // Worker exits as a downstream effect of parent1's stop
        // (cascading SupervisedWorker::Stop -> child stop -> Worker
        // exit).
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();

        parent2.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent2)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── link_started port: signals when TestSlowWorker enters Handler<Link>
    // │   ├── received_stop port: signals when Stop arrives at the worker side
    // │   └── events port: parent's supervision events (Parent absorbs)
    // ├── worker: TestSlowWorker (fake WorkerLike, never sends Linked)
    // └── parent: Parent
    //     └── supervisor: Supervisor (constructed with a known session id)
    //         └── supervisor-side link actor: KeepaliveSupervisor
    //
    // Protocol-surface test for `Supervisor::pending_stop`. The
    // fake worker records the session, signals link_started, and
    // blocks on a Notify the test holds. The test calls
    // `parent.stop` while the worker is still in Handler<Link>,
    // then releases the worker. The pending_stop machinery must
    // have already queued `SupervisedWorker::Stop` carrying the
    // parent's reason and the matching session_id. The worker
    // validates the session_id, sends the reason via
    // `received_stop`, then emits a synthetic `Unlinked` so the
    // supervisor exits cleanly via cx.exit (the Unlinked arm only
    // checks session_id; it does not require a prior Linked).
    #[tokio::test]
    async fn test_stop_before_linked_propagates_via_pending_stop() {
        let proc = Proc::isolated();
        let (inst, _inst_hndl) = proc.client("inst").unwrap();
        let (link_started, mut link_started_rx) = inst.open_port::<()>();
        let (received_stop, mut received_stop_rx) = inst.open_port::<String>();
        let (events, _events_rx) = inst.open_port::<ActorSupervisionEvent>();
        let release_link = Arc::new(Notify::new());
        let worker = proc
            .spawn(
                "worker",
                TestSlowWorker {
                    link_started,
                    release_link: Arc::clone(&release_link),
                    received_stop,
                    session: None,
                },
            )
            .unwrap();
        let session_id = Uid::anonymous();
        let parent = proc
            .spawn(
                "parent",
                Parent {
                    supervisor: Some(Supervisor::new_uid(
                        worker.bind::<WorkerLike>(),
                        KeepaliveLink::new(Duration::from_millis(5), Duration::from_secs(60)),
                        LinkOptions::default(),
                        session_id.clone(),
                    )),
                    events,
                },
            )
            .unwrap();

        // Observe Link receipt - worker is now blocked in Handler<Link>.
        tokio::time::timeout(Duration::from_secs(5), link_started_rx.recv())
            .await
            .unwrap()
            .unwrap();

        // `Supervisor::handle_stop` does not call `cx.exit` — the
        // only path that exits the supervisor is
        // `WorkerSupervisor::Unlinked`. That's why `TestSlowWorker`
        // emits `Unlinked` in its `Stop` handler.
        parent.stop("test stop").unwrap();
        release_link.notify_one();

        let stop_reason = tokio::time::timeout(Duration::from_secs(5), received_stop_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stop_reason, "parent stopping");

        tokio::time::timeout(Duration::from_secs(5), parent)
            .await
            .unwrap();
        worker.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: receives the child address
    // │   ├── stopped port: receives the child stop reason
    // │   ├── drained port: receives the child's work tag
    // │   └── events port: parent's supervision events (silent — Parent absorbs)
    // ├── worker: Worker<TestChild> (with DrainAfter(10ms, "child work"))
    // │   ├── child: TestChild
    // │   └── worker-side link actor: KeepaliveWorker
    // └── parent: Parent
    //     └── supervisor: Supervisor (constructed with a known session id)
    //         └── supervisor-side link actor: KeepaliveSupervisor
    //
    // End-to-end proxy for the worker->child DrainAndStop path:
    // verifies that `Worker::stop_child` takes the `DrainAndStop`
    // branch, `TestChild::handle_stop` takes `exit_after_drain`, and
    // the child can process ordinary work before terminating. It does
    // not force work to remain queued at the instant stop arrives;
    // that stricter property would require a blocked-handler style
    // test.
    #[tokio::test]
    async fn test_drain_and_stop_propagates_through_worker_after_child_work() {
        let proc = Proc::isolated();
        let (inst, _inst_hndl) = proc.client("inst").unwrap();
        let (ready, mut ready_rx) = inst.open_port::<ActorAddr>();
        let (stopped, mut stopped_rx) = inst.open_port::<String>();
        let (drained, mut drained_rx) = inst.open_port::<String>();
        let (events, _events_rx) = inst.open_port::<ActorSupervisionEvent>();
        let worker = proc
            .spawn(
                "worker",
                Worker::new(
                    test_child(
                        ready,
                        stopped,
                        Some(TestChildAction::DrainAfter(
                            Duration::from_millis(10),
                            "child work".to_string(),
                        )),
                    )
                    .with_drain_observer(drained),
                ),
            )
            .unwrap();
        let _child_addr = ready_rx.recv().await.unwrap();
        let session_id = Uid::anonymous();
        let parent = proc
            .spawn(
                "parent",
                Parent {
                    supervisor: Some(Supervisor::new_uid(
                        worker.bind::<WorkerLike>(),
                        KeepaliveLink::new(Duration::from_millis(5), Duration::from_secs(60)),
                        LinkOptions::default(),
                        session_id.clone(),
                    )),
                    events,
                },
            )
            .unwrap();

        // Wait for the self-scheduled child work.
        let drained_tag = tokio::time::timeout(Duration::from_secs(5), drained_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(drained_tag, "child work");

        // Now send Stop with DrainAndStop.
        worker.post(
            &inst,
            SupervisedWorker::Stop {
                session_id: session_id.clone(),
                mode: StopMode::DrainAndStop,
                reason: "drain test".to_string(),
            },
        );

        // Child reports the drain-stop reason via handle_stop.
        let reason = tokio::time::timeout(Duration::from_secs(5), stopped_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "drain test");

        // Worker exits as a downstream effect of the child stop.
        let status = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
        assert!(matches!(status, ActorStatus::Stopped(_)));

        parent.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent)
            .await
            .unwrap();
    }

    // proc
    // ├── client instance
    // │   ├── ready port: child address
    // │   ├── stopped port: child stop reasons (silent across both unlinks under Detach)
    // │   ├── events1 port: parent1's supervision events
    // │   └── events2 port: parent2's supervision events
    // ├── worker: Worker<TestChild>
    // │   └── child: TestChild (survives both unlinks under Detach)
    // ├── parent1: Parent
    // │   └── supervisor1 (session_id1, OrphanPolicy::Detach)
    // └── parent2: Parent
    //     └── supervisor2 (session_id2, OrphanPolicy::Detach)
    //
    // Verifies the worker accepts a fresh Link after Unlink clears
    // self.session. Under OrphanPolicy::Detach the child and worker
    // survive the first unlink, allowing a second supervisor with
    // session_id2 to link. A second Unlink with session_id2 confirms
    // accept_session_id installed the new session.
    #[tokio::test]
    async fn test_worker_accepts_relink_after_unlink_under_detach_policy() {
        let proc = Proc::isolated();
        let (inst, _inst_hndl) = proc.client("inst").unwrap();
        let session_id1 = Uid::anonymous();
        let (_child_addr, worker, parent1, mut stopped_rx, mut events_rx1) = spawn_supervised_pair(
            &proc,
            &inst,
            session_id1.clone(),
            LinkOptions {
                orphan_policy: OrphanPolicy::Detach,
                ..Default::default()
            },
            None,
        )
        .await;

        // Give the first Link/Linked handshake time to complete before
        // issuing Unlink.
        tokio::time::sleep(Duration::from_millis(100)).await;

        worker.post(
            &inst,
            SupervisedWorker::Unlink {
                session_id: session_id1.clone(),
                reason: "first unlink".to_string(),
            },
        );

        let event1 = tokio::time::timeout(Duration::from_secs(5), events_rx1.recv())
            .await
            .unwrap()
            .unwrap();
        match event1.actor_status {
            ActorStatus::Stopped(reason) => assert_eq!(reason, "first unlink"),
            status => panic!("expected supervisor1 Stopped event, got {:?}", status),
        }

        let timed_out = tokio::time::timeout(Duration::from_millis(500), stopped_rx.recv()).await;
        assert!(
            timed_out.is_err(),
            "child should survive unlink under OrphanPolicy::Detach"
        );

        let session_id2 = Uid::anonymous();
        let (events2, mut events_rx2) = inst.open_port::<ActorSupervisionEvent>();
        let parent2 = proc
            .spawn(
                "parent2",
                Parent {
                    supervisor: Some(Supervisor::new_uid(
                        worker.bind::<WorkerLike>(),
                        KeepaliveLink::new(Duration::from_millis(5), Duration::from_secs(60)),
                        LinkOptions {
                            orphan_policy: OrphanPolicy::Detach,
                            ..Default::default()
                        },
                        session_id2.clone(),
                    )),
                    events: events2,
                },
            )
            .unwrap();

        // Give the second Link/Linked handshake time to complete.
        tokio::time::sleep(Duration::from_millis(100)).await;

        worker.post(
            &inst,
            SupervisedWorker::Unlink {
                session_id: session_id2.clone(),
                reason: "second unlink".to_string(),
            },
        );

        let event2 = tokio::time::timeout(Duration::from_secs(5), events_rx2.recv())
            .await
            .unwrap()
            .unwrap();
        match event2.actor_status {
            ActorStatus::Stopped(reason) => assert_eq!(reason, "second unlink"),
            status => panic!("expected supervisor2 Stopped event, got {:?}", status),
        }

        parent1.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent1)
            .await
            .unwrap();
        parent2.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), parent2)
            .await
            .unwrap();
        worker.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
    }
}
