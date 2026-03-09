/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The mesh agent actor manages procs in ProcMeshes.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Data;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::PortHandle;
use hyperactor::RefClient;
use hyperactor::Unbind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::remote::Remote;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::proc::Proc;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::Name;
use crate::resource;

/// Actor name used when spawning the proc agent on user procs.
pub const PROC_AGENT_ACTOR_NAME: &str = "proc_agent";

declare_attrs! {
    /// Whether to self kill actors, procs, and hosts whose owner is not reachable.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ORPHAN_TIMEOUT".to_string()),
        Some("mesh_orphan_timeout".to_string()),
    ))
    pub attr MESH_ORPHAN_TIMEOUT: Duration = Duration::from_secs(60);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum StopActorResult {
    Success,
    Timeout,
    NotFound,
}
wirevalue::register_type!(StopActorResult);

/// Deferred republish of introspect properties.
///
/// Sent as a zero-delay self-message from the supervision event
/// handler so it returns immediately without blocking the ProcAgent
/// message loop. Multiple rapid supervision events (e.g., 4 actors
/// failing simultaneously via broadcast) coalesce into a single
/// republish via the `introspect_dirty` flag.
///
/// Without this, calling `publish_introspect_properties` inline in
/// the supervision handler starves `GetRankStatus` polls from the
/// `ActorMeshController`, preventing `__supervise__` from firing
/// within the test timeout. See D94960791 for the root cause
/// analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
struct RepublishIntrospect;
wirevalue::register_type!(RepublishIntrospect);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Handler,
    HandleClient,
    RefClient,
    Named
)]
pub(crate) enum MeshAgentMessage {
    /// Stop actors of a specific mesh name
    StopActor {
        /// The actor to stop
        actor_id: ActorId,
        /// The timeout for waiting for the actor to stop
        timeout_ms: u64,
        /// The reason for stopping the actor
        reason: String,
        /// The result when trying to stop the actor
        #[reply]
        stopped: OncePortRef<StopActorResult>,
    },
}

/// Actor state used for v1 API.
#[derive(Debug)]
struct ActorInstanceState {
    create_rank: usize,
    spawn: Result<ActorId, anyhow::Error>,
    /// If true, the actor has been stopped. There is no way to restart it, a new
    /// actor must be spawned.
    stopped: bool,
    /// The time at which the actor should be considered expired if no further
    /// keepalive is received. `None` meaning it will never expire.
    expiry_time: Option<std::time::SystemTime>,
}

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    Named,
    Bind,
    Unbind
)]
struct SelfCheck {}

/// A mesh agent is responsible for managing procs in a [`ProcMesh`].
///
/// ## Supervision event ingestion (remote)
///
/// `ProcAgent` is the *process/rank-local* sink for
/// `ActorSupervisionEvent`s produced by the runtime (actor failures,
/// routing failures, undeliverables, etc.).
///
/// We **export** `ActorSupervisionEvent` as a handler so that other
/// procs—most importantly the process-global root client created by
/// `context()`—can forward undeliverables as supervision
/// events to the *currently active* mesh.
///
/// Without exporting this handler, `ActorSupervisionEvent` cannot be
/// addressed via `ActorRef`/`PortRef` across processes, and the
/// global-root-client undeliverable → supervision pipeline would
/// degrade to log-only behavior (events become undeliverable again or
/// are dropped).
///
/// See `global_context.rs` for the invariant and the forwarding path
/// ("last sink wins").
#[hyperactor::export(
    handlers=[
        MeshAgentMessage,
        ActorSupervisionEvent,
        resource::CreateOrUpdate<ActorSpec> { cast = true },
        resource::Stop { cast = true },
        resource::StopAll { cast = true },
        resource::GetState<ActorState> { cast = true },
        resource::KeepaliveGetState<ActorState> { cast = true },
        resource::GetRankStatus { cast = true },
        RepublishIntrospect { cast = true },
    ]
)]
pub struct ProcAgent {
    proc: Proc,
    remote: Remote,
    /// Actors created and tracked through the resource behavior.
    actor_states: HashMap<Name, ActorInstanceState>,
    /// If true, record supervision events to be reported to owning actors later.
    record_supervision_events: bool,
    /// Contains the list of all supervision events that were received.
    supervision_events: HashMap<ActorId, Vec<ActorSupervisionEvent>>,
    /// True when supervision events have arrived but introspect
    /// properties haven't been republished yet.
    introspect_dirty: bool,
    /// If set, the StopAll handler will send the exit code through this
    /// channel instead of calling process::exit directly, allowing the
    /// caller to perform graceful shutdown (e.g. draining the mailbox server).
    shutdown_tx: Option<tokio::sync::oneshot::Sender<i32>>,
    /// If set, check for expired actors whose keepalive has lapsed.
    mesh_orphan_timeout: Option<Duration>,
}

impl ProcAgent {
    pub(crate) fn boot(
        proc: Proc,
        shutdown_tx: Option<tokio::sync::oneshot::Sender<i32>>,
    ) -> Result<ActorHandle<Self>, anyhow::Error> {
        // We can't use Option<Duration> directly in config attrs because AttrValue
        // is not implemented for Option<Duration>. So we use a zero timeout to
        // indicate no timeout.
        let orphan_timeout = hyperactor_config::global::get(MESH_ORPHAN_TIMEOUT);
        let orphan_timeout = if orphan_timeout.is_zero() {
            None
        } else {
            Some(orphan_timeout)
        };
        let agent = ProcAgent {
            proc: proc.clone(),
            remote: Remote::collect(),
            actor_states: HashMap::new(),
            record_supervision_events: true,
            supervision_events: HashMap::new(),
            introspect_dirty: false,
            shutdown_tx,
            mesh_orphan_timeout: orphan_timeout,
        };
        proc.spawn::<Self>(PROC_AGENT_ACTOR_NAME, agent)
    }

    async fn destroy_and_wait_except_current<'a>(
        &mut self,
        cx: &Context<'a, Self>,
        timeout: tokio::time::Duration,
        reason: &str,
    ) -> Result<(Vec<ActorId>, Vec<ActorId>), anyhow::Error> {
        self.proc
            .destroy_and_wait_except_current::<Self>(timeout, Some(cx), true, reason)
            .await
    }

    /// Publish the current proc properties and children list for
    /// introspection.
    fn publish_introspect_properties(&self, cx: &impl hyperactor::context::Actor) {
        use hyperactor::introspect::PublishedPropertiesKind;

        // Live actors.
        let live_ids = self.proc.all_actor_ids();
        let num_live = live_ids.len();
        let mut children = Vec::with_capacity(num_live);
        let mut system_children = Vec::new();
        for id in live_ids {
            let ref_str = id.to_string();
            if self
                .proc
                .get_instance(&id)
                .is_some_and(|cell| cell.is_system())
            {
                system_children.push(ref_str.clone());
            }
            children.push(ref_str);
        }

        // Terminated actors appear as children but don't inflate
        // the actor count. Track them in stopped_children so the
        // TUI can filter/gray without per-child fetches.
        let mut stopped_children: Vec<String> = Vec::new();
        for id in self.proc.all_terminated_actor_ids() {
            let ref_str = id.to_string();
            stopped_children.push(ref_str.clone());
            // Terminated system actors must also appear in
            // system_children for correct filtering.
            if let Some(snapshot) = self.proc.terminated_snapshot(&id) {
                if matches!(
                    snapshot.properties,
                    hyperactor::introspect::NodeProperties::Actor {
                        is_system: true,
                        ..
                    }
                ) {
                    system_children.push(ref_str.clone());
                }
            }
            if !children.contains(&ref_str) {
                children.push(ref_str);
            }
        }

        let stopped_retention_cap =
            hyperactor_config::global::get(hyperactor::config::TERMINATED_SNAPSHOT_RETENTION);

        // FI-5: is_poisoned iff failed_actor_count > 0.
        let failed_actor_count = self.supervision_events.len();
        cx.instance()
            .publish_properties(PublishedPropertiesKind::Proc {
                proc_name: self.proc.proc_id().to_string(),
                num_actors: num_live,
                children,
                system_children,
                stopped_children,
                stopped_retention_cap,
                is_poisoned: failed_actor_count > 0,
                failed_actor_count,
            });
    }
}

#[async_trait]
impl Actor for ProcAgent {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        self.proc.set_supervision_coordinator(this.port())?;
        self.publish_introspect_properties(this);

        // Resolve terminated actor snapshots via QueryChild so that
        // dead actors remain directly queryable by reference.
        let proc = self.proc.clone();
        let self_id = this.self_id().clone();
        this.set_query_child_handler(move |child_ref| {
            use hyperactor::introspect::NodePayload;
            use hyperactor::introspect::NodeProperties;
            use hyperactor::introspect::PublishedPropertiesKind;
            use hyperactor::reference::Reference;

            if let Reference::Actor(id) = child_ref {
                if let Some(snapshot) = proc.terminated_snapshot(id) {
                    return snapshot;
                }
            }

            // PA-1 (ProcAgent path): proc-node children used by
            // admin/TUI must be computed from live proc state at query
            // time, not solely from cached published_properties.
            // Therefore a direct proc.spawn() actor must appear on the
            // next QueryChild(Reference::Proc) response without an
            // extra publish event. See
            // test_query_child_proc_returns_live_children.
            if let Reference::Proc(proc_id) = child_ref {
                if proc_id == proc.proc_id() {
                    let live_ids = proc.all_actor_ids();
                    let num_live = live_ids.len();
                    let mut children = Vec::with_capacity(num_live);
                    let mut system_children = Vec::new();
                    for id in live_ids {
                        let ref_str = id.to_string();
                        if proc.get_instance(&id).is_some_and(|cell| cell.is_system()) {
                            system_children.push(ref_str.clone());
                        }
                        children.push(ref_str);
                    }

                    let mut stopped_children: Vec<String> = Vec::new();
                    for id in proc.all_terminated_actor_ids() {
                        let ref_str = id.to_string();
                        stopped_children.push(ref_str.clone());
                        if let Some(snapshot) = proc.terminated_snapshot(&id) {
                            if matches!(
                                snapshot.properties,
                                hyperactor::introspect::NodeProperties::Actor {
                                    is_system: true,
                                    ..
                                }
                            ) {
                                system_children.push(ref_str.clone());
                            }
                        }
                        if !children.contains(&ref_str) {
                            children.push(ref_str);
                        }
                    }

                    let stopped_retention_cap = hyperactor_config::global::get(
                        hyperactor::config::TERMINATED_SNAPSHOT_RETENTION,
                    );

                    let (is_poisoned, failed_actor_count) = proc
                        .get_instance(&self_id)
                        .and_then(|cell| cell.published_properties())
                        .and_then(|p| match p.kind {
                            PublishedPropertiesKind::Proc {
                                is_poisoned,
                                failed_actor_count,
                                ..
                            } => Some((is_poisoned, failed_actor_count)),
                            _ => None,
                        })
                        .unwrap_or((false, 0));

                    return NodePayload {
                        identity: proc_id.to_string(),
                        properties: NodeProperties::Proc {
                            proc_name: proc_id.to_string(),
                            num_actors: num_live,
                            system_children,
                            stopped_children,
                            stopped_retention_cap,
                            is_poisoned,
                            failed_actor_count,
                        },
                        children,
                        parent: None,
                        as_of: humantime::format_rfc3339_millis(
                            hyperactor::clock::RealClock.system_time_now(),
                        )
                        .to_string(),
                    };
                }
            }

            NodePayload {
                identity: String::new(),
                properties: NodeProperties::Error {
                    code: "not_found".into(),
                    message: format!("child {} not found", child_ref),
                },
                children: Vec::new(),
                parent: None,
                as_of: humantime::format_rfc3339_millis(
                    hyperactor::clock::RealClock.system_time_now(),
                )
                .to_string(),
            }
        });

        if let Some(delay) = &self.mesh_orphan_timeout {
            this.self_message_with_delay(SelfCheck::default(), *delay)?;
        }
        Ok(())
    }
}

#[async_trait]
#[hyperactor::handle(MeshAgentMessage)]
impl MeshAgentMessageHandler for ProcAgent {
    async fn stop_actor(
        &mut self,
        cx: &Context<Self>,
        actor_id: ActorId,
        timeout_ms: u64,
        reason: String,
    ) -> Result<StopActorResult, anyhow::Error> {
        tracing::info!(
            name = "StopActor",
            %actor_id,
            actor_name = actor_id.name(),
            %reason,
        );

        let result = if let Some(mut status) = self.proc.stop_actor(&actor_id, reason) {
            match RealClock
                .timeout(
                    tokio::time::Duration::from_millis(timeout_ms),
                    status.wait_for(|state: &ActorStatus| state.is_terminal()),
                )
                .await
            {
                Ok(_) => Ok(StopActorResult::Success),
                Err(_) => Ok(StopActorResult::Timeout),
            }
        } else {
            Ok(StopActorResult::NotFound)
        };
        self.publish_introspect_properties(cx);
        result
    }
}

#[async_trait]
impl Handler<ActorSupervisionEvent> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        event: ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        if self.record_supervision_events {
            if event.is_error() {
                tracing::warn!(
                    name = "SupervisionEvent",
                    proc_id = %self.proc.proc_id(),
                    %event,
                    "recording supervision error",
                );
            } else {
                tracing::debug!(
                    name = "SupervisionEvent",
                    proc_id = %self.proc.proc_id(),
                    %event,
                    "recording non-error supervision event",
                );
            }
            self.supervision_events
                .entry(event.actor_id.clone())
                .or_default()
                .push(event.clone());
            // Defer republish so introspection picks up is_poisoned /
            // failed_actor_count without blocking the message loop.
            // Multiple rapid events coalesce into one republish.
            if !self.introspect_dirty {
                self.introspect_dirty = true;
                let _ = cx.self_message_with_delay(
                    RepublishIntrospect,
                    std::time::Duration::from_millis(100),
                );
            }
        }
        if !self.record_supervision_events {
            // If nothing is recording these, crash the whole process.
            tracing::error!(
                name = "supervision_event_transmit_failed",
                proc_id = %cx.self_id().proc_id(),
                %event,
                "could not propagate supervision event, crashing",
            );
            std::process::exit(1);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RepublishIntrospect> for ProcAgent {
    async fn handle(&mut self, cx: &Context<Self>, _: RepublishIntrospect) -> anyhow::Result<()> {
        if self.introspect_dirty {
            self.introspect_dirty = false;
            self.publish_introspect_properties(cx);
        }
        Ok(())
    }
}

// Implement the resource behavior for managing actors:

/// Actor spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct ActorSpec {
    /// registered actor type
    pub actor_type: String,
    /// serialized parameters
    pub params_data: Data,
}
wirevalue::register_type!(ActorSpec);

/// Actor state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct ActorState {
    /// The actor's ID.
    pub actor_id: ActorId,
    /// The rank of the proc that created the actor. This is before any slicing.
    pub create_rank: usize,
    // TODO status: ActorStatus,
    pub supervision_events: Vec<ActorSupervisionEvent>,
}
wirevalue::register_type!(ActorState);

#[async_trait]
impl Handler<resource::CreateOrUpdate<ActorSpec>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<ActorSpec>,
    ) -> anyhow::Result<()> {
        if self.actor_states.contains_key(&create_or_update.name) {
            // There is no update.
            return Ok(());
        }
        let create_rank = create_or_update.rank.unwrap();
        // If there have been supervision events for any actors on this proc,
        // we disallow spawning new actors on it, as this proc may be in an
        // invalid state.
        if !self.supervision_events.is_empty() {
            self.actor_states.insert(
                create_or_update.name.clone(),
                ActorInstanceState {
                    spawn: Err(anyhow::anyhow!(
                        "Cannot spawn new actors on mesh with supervision events"
                    )),
                    create_rank,
                    stopped: false,
                    expiry_time: None,
                },
            );
            return Ok(());
        }

        let ActorSpec {
            actor_type,
            params_data,
        } = create_or_update.spec;
        self.actor_states.insert(
            create_or_update.name.clone(),
            ActorInstanceState {
                create_rank,
                spawn: self
                    .remote
                    .gspawn(
                        &self.proc,
                        &actor_type,
                        &create_or_update.name.to_string(),
                        params_data,
                        cx.headers().clone(),
                    )
                    .await,
                stopped: false,
                expiry_time: None,
            },
        );

        self.publish_introspect_properties(cx);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::Stop> for ProcAgent {
    async fn handle(&mut self, cx: &Context<Self>, message: resource::Stop) -> anyhow::Result<()> {
        // We don't remove the actor from the state map, instead we just store
        // its state as Stopped.
        let actor = self.actor_states.get_mut(&message.name);
        // Have to separate stop_actor from setting "stopped" because it borrows
        // as mutable and cannot have self borrowed mutably twice.
        let actor_id = match actor {
            Some(actor_state) => {
                match &actor_state.spawn {
                    Ok(actor_id) => {
                        if actor_state.stopped {
                            None
                        } else {
                            actor_state.stopped = true;
                            Some(actor_id.clone())
                        }
                    }
                    // If the original spawn had failed, the actor is still considered
                    // successfully stopped.
                    Err(_) => None,
                }
            }
            // TODO: represent unknown rank
            None => None,
        };
        let timeout = hyperactor_config::global::get(hyperactor::config::STOP_ACTOR_TIMEOUT);
        if let Some(actor_id) = actor_id {
            // The orphaned actor SelfCheck will consult the stopped field before
            // trying to stop any actor again.
            // While this function returns a Result, it never returns an Err
            // value so we can simply expect without any failure handling.
            self.stop_actor(cx, actor_id, timeout.as_millis() as u64, message.reason)
                .await
                .expect("stop_actor cannot fail");
        }

        Ok(())
    }
}

/// Handles `StopAll` by coordinating an orderly stop of child actors and then
/// exiting the process. This handler never returns to the caller: it calls
/// `std::process::exit(0/1)` after shutdown. Any sender must *not* expect a
/// reply or send any further message, and should watch `ProcStatus` instead.
#[async_trait]
impl Handler<resource::StopAll> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: resource::StopAll,
    ) -> anyhow::Result<()> {
        let timeout = hyperactor_config::global::get(hyperactor::config::STOP_ACTOR_TIMEOUT);
        // By passing in the self context, destroy_and_wait will stop this agent
        // last, after all others are stopped.
        let stop_result = self
            .destroy_and_wait_except_current(cx, timeout, &message.reason)
            .await;
        // Exit here to cleanup all remaining resources held by the process.
        // This means ProcAgent will never run cleanup or any other code
        // from exiting its root actor. Senders of this message should never
        // send any further messages or expect a reply.
        match stop_result {
            Ok((stopped_actors, aborted_actors)) => {
                // No need to clean up any state, the process is exiting.
                tracing::info!(
                    actor = %cx.self_id(),
                    "exiting process after receiving StopAll message on ProcAgent. \
                    stopped actors = {:?}, aborted actors = {:?}",
                    stopped_actors.into_iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                    aborted_actors.into_iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                );
                if let Some(tx) = self.shutdown_tx.take() {
                    let _ = tx.send(0);
                    return Ok(());
                }
                std::process::exit(0);
            }
            Err(e) => {
                tracing::error!(actor = %cx.self_id(), "failed to stop all actors on ProcAgent: {:?}", e);
                if let Some(tx) = self.shutdown_tx.take() {
                    let _ = tx.send(1);
                    return Ok(());
                }
                std::process::exit(1);
            }
        }
    }
}

#[async_trait]
impl Handler<resource::GetRankStatus> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_rank_status: resource::GetRankStatus,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;
        use crate::resource::Status;

        let (rank, status) = match self.actor_states.get(&get_rank_status.name) {
            Some(ActorInstanceState {
                spawn: Ok(actor_id),
                create_rank,
                stopped,
                ..
            }) => {
                if *stopped {
                    (*create_rank, resource::Status::Stopped)
                } else {
                    let supervision_events = self
                        .supervision_events
                        .get(actor_id)
                        .map_or_else(Vec::new, |a| a.clone());
                    (
                        *create_rank,
                        if supervision_events.is_empty() {
                            resource::Status::Running
                        } else {
                            resource::Status::Failed(format!(
                                "because of supervision events: {:?}",
                                supervision_events
                            ))
                        },
                    )
                }
            }
            Some(ActorInstanceState {
                spawn: Err(e),
                create_rank,
                ..
            }) => (*create_rank, Status::Failed(e.to_string())),
            // TODO: represent unknown rank
            None => (usize::MAX, Status::NotExist),
        };

        // Send a sparse overlay update. If rank is unknown, emit an
        // empty overlay.
        let overlay = if rank == usize::MAX {
            StatusOverlay::new()
        } else {
            StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                .expect("valid single-run overlay")
        };
        let result = get_rank_status.reply.send(cx, overlay);
        // Ignore errors, because returning Err from here would cause the ProcAgent
        // to be stopped, which would prevent querying and spawning other actors.
        // This only means some actor that requested the state of an actor failed to receive it.
        if let Err(e) = result {
            tracing::warn!(
                actor = %cx.self_id(),
                "failed to send GetRankStatus reply to {} due to error: {}",
                get_rank_status.reply.port_id().actor_id(),
                e
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::GetState<ActorState>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_state: resource::GetState<ActorState>,
    ) -> anyhow::Result<()> {
        let state = match self.actor_states.get(&get_state.name) {
            Some(ActorInstanceState {
                create_rank,
                spawn: Ok(actor_id),
                stopped,
                ..
            }) => {
                let supervision_events = self
                    .supervision_events
                    .get(actor_id)
                    .map_or_else(Vec::new, |a| a.clone());
                let status = if *stopped {
                    resource::Status::Stopped
                } else if supervision_events.is_empty() {
                    resource::Status::Running
                } else {
                    resource::Status::Failed(format!(
                        "because of supervision events: {:?}",
                        supervision_events
                    ))
                };
                resource::State {
                    name: get_state.name.clone(),
                    status,
                    state: Some(ActorState {
                        actor_id: actor_id.clone(),
                        create_rank: *create_rank,
                        supervision_events,
                    }),
                }
            }
            Some(ActorInstanceState { spawn: Err(e), .. }) => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::Failed(e.to_string()),
                state: None,
            },
            None => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::NotExist,
                state: None,
            },
        };

        let result = get_state.reply.send(cx, state);
        // Ignore errors, because returning Err from here would cause the ProcAgent
        // to be stopped, which would prevent querying and spawning other actors.
        // This only means some actor that requested the state of an actor failed to receive it.
        if let Err(e) = result {
            tracing::warn!(
                actor = %cx.self_id(),
                "failed to send GetState reply to {} due to error: {}",
                get_state.reply.port_id().actor_id(),
                e
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::KeepaliveGetState<ActorState>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: resource::KeepaliveGetState<ActorState>,
    ) -> anyhow::Result<()> {
        // Same impl as GetState, but additionally update the expiry time on the actor.
        if let Ok(instance_state) = self
            .actor_states
            .get_mut(&message.get_state.name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "attempting to register a keepalive for an actor that doesn't exist: {}",
                    message.get_state.name
                )
            })
        {
            instance_state.expiry_time = Some(message.expires_after);
        }

        // Forward the rest of the impl to GetState.
        <Self as Handler<resource::GetState<ActorState>>>::handle(self, cx, message.get_state).await
    }
}

/// A local handler to get a new client instance on the proc.
/// This is used to create root client instances.
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct NewClientInstance {
    #[reply]
    pub client_instance: PortHandle<Instance<()>>,
}

#[async_trait]
impl Handler<NewClientInstance> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        NewClientInstance { client_instance }: NewClientInstance,
    ) -> anyhow::Result<()> {
        let (instance, _handle) = self.proc.instance("client")?;
        client_instance.send(cx, instance)?;
        Ok(())
    }
}

/// A handler to get a clone of the proc managed by this agent.
/// This is used to obtain the local proc from a host mesh.
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct GetProc {
    #[reply]
    pub proc: PortHandle<Proc>,
}

#[async_trait]
impl Handler<GetProc> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetProc { proc }: GetProc,
    ) -> anyhow::Result<()> {
        proc.send(cx, self.proc.clone())?;
        Ok(())
    }
}

#[async_trait]
impl Handler<SelfCheck> for ProcAgent {
    async fn handle(&mut self, cx: &Context<Self>, _: SelfCheck) -> anyhow::Result<()> {
        // Check each actor's expiry time. If the current time is past the expiry,
        // stop the actor. This allows automatic cleanup when a controller disappears
        // but owned resources remain. It is important that this check runs on the
        // same proc as the child actor itself, since the controller could be dead or
        // disconnected.
        let Some(duration) = &self.mesh_orphan_timeout else {
            return Ok(());
        };
        let duration = duration.clone();
        let now = RealClock.system_time_now();

        // Collect expired actors before mutating, since stop_actor borrows &mut self.
        let expired: Vec<(Name, ActorId)> = self
            .actor_states
            .iter()
            .filter_map(|(name, state)| {
                let expiry = state.expiry_time?;
                // If the actor was already stopped we don't need to stop it again.
                if now > expiry && !state.stopped {
                    if let Ok(actor_id) = &state.spawn {
                        return Some((name.clone(), actor_id.clone()));
                    }
                }
                None
            })
            .collect();

        if !expired.is_empty() {
            tracing::info!(
                "stopping {} orphaned actors past their keepalive expiry",
                expired.len(),
            );
        }

        let timeout = hyperactor_config::global::get(hyperactor::config::STOP_ACTOR_TIMEOUT);
        for (name, actor_id) in expired {
            if let Some(state) = self.actor_states.get_mut(&name) {
                state.stopped = true;
            }
            self.stop_actor(
                cx,
                actor_id,
                timeout.as_millis() as u64,
                "orphaned".to_string(),
            )
            .await
            .expect("stop_actor cannot fail");
        }

        // Reschedule.
        cx.self_message_with_delay(SelfCheck::default(), duration)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A no-op actor used to test direct proc-level spawning.
    #[derive(Debug, Default)]
    #[hyperactor::export(handlers = [])]
    struct ExtraActor;
    impl hyperactor::Actor for ExtraActor {}

    // Verifies that QueryChild(Reference::Proc) on a ProcAgent returns
    // a live NodeProperties::Proc whose children reflect actors spawned
    // directly on the proc — i.e. via proc.spawn(), which bypasses the
    // gspawn message handler and therefore never triggers
    // publish_introspect_properties.
    //
    // Invariant PA-1 (canonical):
    // For proc-node resolution used by admin/TUI, children lists
    // (children/system_children/stopped_children) must be derived from
    // live proc state at query time rather than only from the last
    // published snapshot.
    // Required behavior: direct proc.spawn() of actor X is visible on
    // the next proc resolve (no extra publish event required).
    //
    // Regression guard for the bug introduced in 9a08d559: removing
    // handle_introspect left publish_introspect_properties as the only
    // update path, which missed supervision-spawned actors (e.g. every
    // sieve actor after sieve[0]). See also
    // mesh_admin::tests::test_proc_children_reflect_directly_spawned_actors.
    #[tokio::test]
    async fn test_query_child_proc_returns_live_children() {
        use hyperactor::PortRef;
        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::introspect::IntrospectMessage;
        use hyperactor::introspect::NodePayload;
        use hyperactor::introspect::NodeProperties;
        use hyperactor::reference::Reference;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "test_proc".to_string()).unwrap();
        let agent_handle = ProcAgent::boot(proc.clone(), None).unwrap();

        // Wait for ProcAgent to finish init.
        agent_handle
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        // Client instance for opening reply ports.
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let agent_id = proc.proc_id().actor_id(PROC_AGENT_ACTOR_NAME, 0);
        let port = PortRef::<IntrospectMessage>::attest_message_port(&agent_id);

        // Helper: send QueryChild(Proc) and return the payload with a
        // timeout so a misrouted reply fails fast rather than hanging.
        let query = |client: &hyperactor::Instance<()>| {
            let (reply_port, reply_rx) = client.open_once_port::<NodePayload>();
            port.send(
                client,
                IntrospectMessage::QueryChild {
                    child_ref: Reference::Proc(proc.proc_id().clone()),
                    reply: reply_port.bind(),
                },
            )
            .unwrap();
            reply_rx
        };
        let recv = |rx: hyperactor::mailbox::OncePortReceiver<NodePayload>| async move {
            hyperactor::clock::RealClock
                .timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("QueryChild(Proc) timed out — reply never delivered")
                .expect("reply channel closed")
        };

        // Initial query: ProcAgent itself should appear in children.
        let payload = recv(query(&client)).await;
        assert!(
            matches!(payload.properties, NodeProperties::Proc { .. }),
            "expected Proc, got {:?}",
            payload.properties
        );
        assert!(
            payload
                .children
                .iter()
                .any(|c| c.contains(PROC_AGENT_ACTOR_NAME)),
            "initial children {:?} should contain proc_agent",
            payload.children
        );
        let initial_count = payload.children.len();

        // Spawn an actor directly on the proc, bypassing ProcAgent's
        // gspawn message handler. This is how supervision-spawned
        // actors (e.g. sieve children) are created.
        proc.spawn("extra_actor", ExtraActor).unwrap();

        // Second query: extra_actor must appear without any republish.
        let payload2 = recv(query(&client)).await;
        assert!(
            matches!(payload2.properties, NodeProperties::Proc { .. }),
            "expected Proc, got {:?}",
            payload2.properties
        );
        assert!(
            payload2.children.iter().any(|c| c.contains("extra_actor")),
            "after direct spawn, children {:?} should contain extra_actor",
            payload2.children
        );
        assert!(
            payload2.children.len() > initial_count,
            "expected at least {} children after direct spawn, got {:?}",
            initial_count + 1,
            payload2.children
        );
    }
}
