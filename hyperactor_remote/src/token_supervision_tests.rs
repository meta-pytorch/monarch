/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortHandle;
use hyperactor::Proc;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::StopMode;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::mailbox::PortReceiver;
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::KeepaliveLink;
use crate::Spawn;
use crate::SupervisionOptions;
use crate::Supervisor;
use crate::Token;
use crate::TokenOptions;
use crate::Worker;
use crate::WorkerLike;
use crate::token;

type SupervisionToken = Token<ActorAddr, ActorRef<WorkerLike>>;

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct ParentExit(StopMode);
wirevalue::register_type!(ParentExit);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
enum ParentControl {
    Exit(StopMode),
    Kill,
}
wirevalue::register_type!(ParentControl);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct WorkerControl;
wirevalue::register_type!(WorkerControl);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
enum ChildControl {
    Stop,
}
wirevalue::register_type!(ChildControl);

#[derive(Debug)]
enum ChildAction {
    StopAfter(Duration),
}

#[derive(Debug)]
struct Parent {
    token_out: PortHandle<String>,
    linked: PortHandle<ActorAddr>,
    events: PortHandle<ActorSupervisionEvent>,
}

#[async_trait]
impl Actor for Parent {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let token = token::create(
            this,
            this.self_addr().clone(),
            this.port::<token::Joined<ActorRef<WorkerLike>>>().bind(),
            TokenOptions::default(),
        )?;
        self.token_out.post(this, token.to_string());
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        self.events.post(this, event.clone());
        this.exit("remote supervision event observed")?;
        Ok(true)
    }
}

#[async_trait]
impl Handler<token::Joined<ActorRef<WorkerLike>>> for Parent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: token::Joined<ActorRef<WorkerLike>>,
    ) -> anyhow::Result<()> {
        self.linked.post(cx, message.peer.actor_addr().clone());
        cx.spawn(Supervisor::new(
            message.peer,
            KeepaliveLink::new(Duration::from_millis(100), Duration::from_millis(300)),
            SupervisionOptions::default(),
        ));
        Ok(())
    }
}

#[async_trait]
impl Handler<ParentExit> for Parent {
    async fn handle(&mut self, cx: &Context<Self>, message: ParentExit) -> anyhow::Result<()> {
        match message.0 {
            StopMode::Stop => cx.stop("parent requested stop")?,
            StopMode::DrainAndStop => cx.drain_and_stop("parent requested drain and stop")?,
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ParentRoot {
    parent: Option<Parent>,
    parent_handle: Option<hyperactor::ActorHandle<Parent>>,
}

#[async_trait]
impl Actor for ParentRoot {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let parent = this.spawn(
            self.parent
                .take()
                .expect("parent root initialized more than once"),
        );
        self.parent_handle = Some(parent);
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl Handler<ParentControl> for ParentRoot {
    async fn handle(&mut self, cx: &Context<Self>, message: ParentControl) -> anyhow::Result<()> {
        let parent = self
            .parent_handle
            .as_ref()
            .expect("parent must be spawned before control message");
        match message {
            ParentControl::Exit(stop_mode) => parent.post(cx, ParentExit(stop_mode)),
            ParentControl::Kill => parent.kill("test parent killed")?,
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SupervisedChild {
    ready: PortHandle<ActorAddr>,
    stopped: PortHandle<String>,
    action: Option<ChildAction>,
}

#[async_trait]
impl Actor for SupervisedChild {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.ready.post(this, this.self_addr().clone());
        if let Some(ChildAction::StopAfter(delay)) = self.action.take() {
            this.post_after(this, ChildControl::Stop, delay);
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
impl Handler<ChildControl> for SupervisedChild {
    async fn handle(&mut self, cx: &Context<Self>, message: ChildControl) -> anyhow::Result<()> {
        match message {
            ChildControl::Stop => cx.drain_and_stop("child finished")?,
        }
        Ok(())
    }
}

#[derive(Debug)]
struct WorkerRoot {
    child_ready: PortHandle<ActorAddr>,
    child_stopped: PortHandle<String>,
    worker_out: PortHandle<ActorRef<WorkerLike>>,
    child_action: Option<ChildAction>,
    worker: Option<hyperactor::ActorHandle<Worker>>,
}

#[async_trait]
impl Actor for WorkerRoot {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let worker = this.spawn(Worker::new());
        worker.post(
            this,
            Spawn::new(SupervisedChild {
                ready: self.child_ready.clone(),
                stopped: self.child_stopped.clone(),
                action: self.child_action.take(),
            }),
        );
        self.worker_out.post(this, worker.bind::<WorkerLike>());
        self.worker = Some(worker);
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl Handler<WorkerControl> for WorkerRoot {
    async fn handle(&mut self, _cx: &Context<Self>, _message: WorkerControl) -> anyhow::Result<()> {
        self.worker
            .as_ref()
            .expect("worker must be spawned before control message")
            .kill("test worker killed")?;
        Ok(())
    }
}

struct Harness {
    observer: hyperactor::Client,
    parent_root: hyperactor::ActorHandle<ParentRoot>,
    worker_root: hyperactor::ActorHandle<WorkerRoot>,
    child_stopped_rx: PortReceiver<String>,
    parent_events_rx: PortReceiver<ActorSupervisionEvent>,
}

impl Harness {
    async fn new() -> anyhow::Result<Self> {
        Self::new_with_child_action(None).await
    }

    async fn new_with_child_action(child_action: Option<ChildAction>) -> anyhow::Result<Self> {
        let observer_proc = local_proc("observer")?;
        let parent_proc = local_proc("parent")?;
        let worker_proc = local_proc("worker")?;
        let observer = observer_proc.client("observer");
        let (token_out, mut token_out_rx) = observer.open_port::<String>();
        let (parent_linked, mut parent_linked_rx) = observer.open_port::<ActorAddr>();
        let (parent_events, parent_events_rx) = observer.open_port::<ActorSupervisionEvent>();
        let (child_ready, mut child_ready_rx) = observer.open_port::<ActorAddr>();
        let (child_stopped, child_stopped_rx) = observer.open_port::<String>();
        let (worker_out, mut worker_out_rx) = observer.open_port::<ActorRef<WorkerLike>>();

        let parent_root = parent_proc.spawn_with_label(
            "parent_root",
            ParentRoot {
                parent: Some(Parent {
                    token_out,
                    linked: parent_linked,
                    events: parent_events,
                }),
                parent_handle: None,
            },
        );
        let token: SupervisionToken = recv::<String>(&mut token_out_rx).await?.parse()?;
        let worker_root = worker_proc.spawn_with_label(
            "worker_root",
            WorkerRoot {
                child_ready,
                child_stopped,
                worker_out,
                child_action,
                worker: None,
            },
        );
        let worker_ref: ActorRef<WorkerLike> = recv(&mut worker_out_rx).await?;
        let (join_result, mut join_result_rx) =
            observer.open_port::<token::JoinResult<ActorAddr>>();

        token.join(&observer, worker_ref, join_result.bind())?;
        match recv(&mut join_result_rx).await? {
            token::JoinResult::Joined { .. } => {}
            token::JoinResult::Rejected { reason } => {
                anyhow::bail!("token join rejected: {}", reason)
            }
        }
        let _worker_addr = recv(&mut parent_linked_rx).await?;
        let _child_addr = recv(&mut child_ready_rx).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(Self {
            observer,
            parent_root,
            worker_root,
            child_stopped_rx,
            parent_events_rx,
        })
    }

    async fn stop(self) -> anyhow::Result<()> {
        self.worker_root.stop("test")?;
        tokio::time::timeout(Duration::from_secs(5), self.worker_root).await?;
        self.parent_root.stop("test")?;
        tokio::time::timeout(Duration::from_secs(5), self.parent_root).await?;
        Ok(())
    }
}

fn local_proc(name: &str) -> anyhow::Result<Proc> {
    Ok(Proc::direct(
        ChannelAddr::any(ChannelTransport::Local),
        name.to_string(),
    )?)
}

async fn recv<T>(rx: &mut PortReceiver<T>) -> anyhow::Result<T>
where
    T: hyperactor::Message,
{
    Ok(tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??)
}

// observer proc
// └── observer instance
//     ├── receives token string from Parent
//     ├── parses Token<ActorAddr, ActorRef<WorkerLike>>
//     ├── sends token.join(observer, worker_ref, join_result)
//     ├── receives JoinResult<ActorAddr>
//     ├── receives child stop reasons
//     └── receives parent-observed supervision events
//
// parent proc
// └── ParentRoot
//     └── Parent
//         ├── creates rendezvous token
//         ├── receives Joined<ActorRef<WorkerLike>>
//         ├── rendezvous token actor
//         └── Supervisor
//             ├── sends Supervise to Worker
//             ├── receives SupervisorEvent events
//             └── KeepaliveSupervisor
//
// worker proc
// └── WorkerRoot
//     └── Worker
//         ├── SupervisedChild
//         └── KeepaliveWorker
//
// supervision relation after token join:
//
//     Parent
//       └── Supervisor  ~~ remote supervision session ~~>  Worker
//                                                          └── SupervisedChild
//
// keepalive link after token join:
//
//     KeepaliveSupervisor  <~~ keepalive messages ~~  KeepaliveWorker
//
// The token is serialized to a string by Parent, parsed by the observer, and
// then used to join the worker ref back to Parent before supervision begins.
#[tokio::test]
async fn test_token_join_parent_exit_stops_child_first() -> anyhow::Result<()> {
    let mut harness = Harness::new().await?;

    harness
        .parent_root
        .post(&harness.observer, ParentControl::Exit(StopMode::Stop));

    let reason = recv(&mut harness.child_stopped_rx).await?;
    assert_eq!(reason, "parent stopping");
    harness.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_token_join_parent_exit_drain_and_stops_child_first() -> anyhow::Result<()> {
    let mut harness = Harness::new().await?;

    harness.parent_root.post(
        &harness.observer,
        ParentControl::Exit(StopMode::DrainAndStop),
    );

    let reason = recv(&mut harness.child_stopped_rx).await?;
    assert_eq!(reason, "parent draining");
    harness.stop().await?;
    Ok(())
}

// Same topology as `test_token_join_parent_exit_stops_child_first`.
//
// Killing the parent actor is the in-process hard-stop path. The runtime kills
// the local Supervisor instead of running its graceful stop handler, so the
// worker-side orphan policy stops the child when the liveness link fails.
#[tokio::test]
async fn test_token_join_parent_kill_stops_child() -> anyhow::Result<()> {
    let mut harness = Harness::new().await?;

    harness
        .parent_root
        .post(&harness.observer, ParentControl::Kill);

    let reason = recv(&mut harness.child_stopped_rx).await?;
    assert_eq!(reason, "supervision liveness failed");
    harness.stop().await?;
    Ok(())
}

// Same topology as `test_token_join_parent_exit_stops_child_first`.
//
// Killing the worker actor is the in-process equivalent of the child-side
// process disappearing. The worker can no longer report the child lifecycle
// directly, so the parent side observes a synthesized supervision event through
// the keepalive link.
#[tokio::test]
async fn test_token_join_worker_kill_notifies_parent() -> anyhow::Result<()> {
    let mut harness = Harness::new().await?;

    harness.worker_root.post(&harness.observer, WorkerControl);

    let event = recv(&mut harness.parent_events_rx).await?;
    assert!(matches!(event.actor_status, ActorStatus::Failed(_)));
    assert!(
        event.to_string().contains("supervision liveness failed"),
        "parent event should describe the failed supervision link"
    );
    harness.stop().await?;
    Ok(())
}

// Same topology as `test_token_join_parent_exit_stops_child_first`.
//
// Here the child stops normally on its own. The worker forwards that ordinary
// lifecycle event to the supervisor, and the parent observes the same stopped
// status that local supervision would have produced.
#[tokio::test]
async fn test_token_join_child_stop_notifies_parent() -> anyhow::Result<()> {
    let mut harness =
        Harness::new_with_child_action(Some(ChildAction::StopAfter(Duration::from_millis(100))))
            .await?;

    let reason = recv(&mut harness.child_stopped_rx).await?;
    assert_eq!(reason, "child finished");

    let event = recv(&mut harness.parent_events_rx).await?;
    match event.actor_status {
        ActorStatus::Stopped(reason) => assert_eq!(reason, "child finished"),
        status => panic!("expected stopped child event, got {:?}", status),
    }
    harness.stop().await?;
    Ok(())
}
