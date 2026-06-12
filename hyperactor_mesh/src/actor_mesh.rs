/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ## Actor mesh invariants (AM-*)
//!
//! - **AM-1 (rank-space):** `proc_mesh` and any view derived from
//!   it share the same dense rank space.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::OnceLock as OnceCell;
use std::time::Duration;

use hyperactor::ActorLocal;
use hyperactor::ActorRef;
use hyperactor::Endpoint as _;
use hyperactor::OncePortRefRepr;
use hyperactor::PortRef;
use hyperactor::PortRefRepr;
use hyperactor::RemoteEndpoint as _;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::accum::ReducerMode;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::mailbox::PortReceiver;
use hyperactor::message::Castable;
use hyperactor::message::MultipartMessage;
use hyperactor::port::Port;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use ndslice::Selection;
use ndslice::ViewExt as _;
use ndslice::view;
use ndslice::view::MapIntoExt;
use ndslice::view::Region;
use ndslice::view::View;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tokio::sync::watch;

use crate::CommActor;
use crate::Error;
use crate::ProcMeshRef;
use crate::ValueMesh;
use crate::casting;
use crate::comm::multicast;
use crate::comm::multicast::CastMessageV1;
use crate::config::V1_CAST_POINT_TO_POINT_THRESHOLD;
use crate::host_mesh::GET_PROC_STATE_MAX_IDLE;
use crate::host_mesh::mesh_to_rankedvalues_with_default;
use crate::mesh_controller::ActorMeshController;
use crate::mesh_controller::SUPERVISION_POLL_FREQUENCY;
use crate::mesh_controller::Subscribe;
use crate::mesh_controller::Unsubscribe;
use crate::mesh_id::ActorMeshId;
use crate::metrics;
use crate::proc_mesh::GET_ACTOR_STATE_MAX_IDLE;
use crate::proc_mesh::telemetry_actor_mesh_id;
use crate::resource;
use crate::supervision::MeshFailure;
use crate::supervision::Unhealthy;

declare_attrs! {
    /// Liveness watchdog for the supervision stream. If no
    /// supervision message (healthy or unhealthy) is observed within
    /// this duration, the controller is assumed to be unreachable and
    /// the mesh is treated as unhealthy. This timeout is about
    /// detecting silence, not slow messages.
    /// This value must be > poll frequency + get actor state timeout + get proc state timeout
    /// or else it is possible to declare the controller dead before it could
    /// feasibly have received a healthy reply.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_SUPERVISION_WATCHDOG_TIMEOUT".to_string()),
        Some("supervision_watchdog_timeout".to_string()),
    ))
    pub attr SUPERVISION_WATCHDOG_TIMEOUT: Duration = Duration::from_mins(2);
}

/// An ActorMesh is a collection of ranked A-typed actors.
///
/// Bound note: `A: Referable` because the mesh stores/returns
/// `ActorRef<A>`, which is only defined for `A: Referable`.
#[derive(Debug)]
pub struct ActorMesh<A: Referable> {
    proc_mesh: ProcMeshRef,
    id: ActorMeshId,
    current_ref: ActorMeshRef<A>,
    /// If present, this is the controller for the mesh. The controller ensures
    /// the mesh is stopped when the actor owning it is stopped, and can provide
    /// supervision events via subscribing.
    /// It may not be present for some types of actors, typically system actors
    /// such as ProcAgent or CommActor.
    controller: Option<ActorRef<ActorMeshController<A>>>,
}

// `A: Referable` for the same reason as the struct: the mesh holds `ActorRef<A>`.
impl<A: Referable> ActorMesh<A> {
    pub(crate) fn new(
        proc_mesh: ProcMeshRef,
        id: ActorMeshId,
        controller: Option<ActorRef<ActorMeshController<A>>>,
    ) -> Self {
        let current_ref = ActorMeshRef::with_page_size(
            id.clone(),
            proc_mesh.clone(),
            DEFAULT_PAGE,
            controller.clone(),
        );

        Self {
            proc_mesh,
            id,
            current_ref,
            controller,
        }
    }

    pub fn id(&self) -> &ActorMeshId {
        &self.id
    }

    pub(crate) fn set_controller(&mut self, controller: Option<ActorRef<ActorMeshController<A>>>) {
        self.controller = controller.clone();
        self.current_ref.set_controller(controller);
    }

    /// Stop actors on this mesh across all procs.
    pub async fn stop(&mut self, cx: &impl context::Actor, reason: String) -> crate::Result<()> {
        // Remove the controller as an optimization so all future meshes
        // created from this one (such as slices) know they are already stopped.
        // Refs and slices on other machines will still be able to query the
        // controller and will be sent a notification about this stop by the controller
        // itself.
        if let Some(controller) = self.controller.take() {
            // Run the Stop/GetState exchange. We wrap it so that, no matter
            // how it ends, we can record a single unhealthy event
            // afterwards. Taking the controller is one-way: once it is gone,
            // no future call through this handle can retry the stop, so a
            // silently-still-healthy mesh with a vanished controller would
            // hide the fact that the stop never reached (or never confirmed)
            // the actors.
            let id = self.id.resource_id().clone();
            let num_ranks = self.current_ref.region().num_ranks();
            let result: crate::Result<()> = async {
                controller.post(
                    cx,
                    resource::Stop {
                        id: id.clone(),
                        reason,
                    },
                );
                // The controller processes messages serially, and its `Stop`
                // handler already awaits the underlying ProcAgent wait, which
                // sends its own `WaitRankStatus` to the ProcAgents and
                // blocks up to `ACTOR_SPAWN_MAX_IDLE` for the actors to
                // reach `Stopped`. By the time the controller gets to this
                // `GetState`, its `health_state.statuses` already reflects
                // the outcome (Stopping, Stopped, Failed, or Timeout on
                // abort-budget exhaustion). We just need to serialize
                // behind the Stop handler and read the result.
                let (port, mut rx) = cx.mailbox().open_port();
                controller.post(
                    cx,
                    resource::GetState::<resource::mesh::State<()>> {
                        id: id.clone(),
                        reply: port.bind(),
                    },
                );
                let statuses = rx.recv().await?;
                let Some(state) = &statuses.state else {
                    return Err(Error::Other(anyhow::anyhow!(
                        "non-existent state in GetState reply from controller: {}",
                        controller.actor_addr()
                    )));
                };
                // `is_terminating` accepts Stopping, Stopped, Failed, and
                // Timeout. The controller's Stop handler has already
                // awaited (or timed out) the underlying ProcAgent wait, so
                // any rank still in Running here means the controller
                // never processed the stop for that rank — a genuine
                // error.
                let all_terminating = state.statuses.values().all(|s| s.is_terminating());
                if !all_terminating {
                    let legacy = mesh_to_rankedvalues_with_default(
                        &state.statuses,
                        resource::Status::NotExist,
                        resource::Status::is_not_exist,
                        num_ranks,
                    );
                    return Err(Error::ActorStopError { statuses: legacy });
                }
                Ok(())
            }
            .await;

            // Record the unhealthy event regardless of outcome. On success
            // the mesh is stopped; on failure the controller is gone and
            // the actors may still be running, but callers need to see the
            // mesh as unhealthy either way so they stop treating it as
            // live.
            let status = match &result {
                Ok(()) => ActorStatus::Stopped("mesh stopped".to_string()),
                Err(e) => ActorStatus::Stopped(format!("mesh stop failed: {e}")),
            };
            let mut entry = self.health_state.entry(cx).or_default();
            let health_state = entry.get_mut();
            health_state.unhealthy_event = Some(Unhealthy::StreamClosed(MeshFailure {
                actor_mesh_name: Some(self.id().to_string()),
                event: ActorSupervisionEvent::new(
                    // Use an actor id from the mesh.
                    ndslice::view::Ranked::get(&self.current_ref, 0)
                        .unwrap()
                        .actor_addr()
                        .clone(),
                    None,
                    status,
                    None,
                ),
                crashed_ranks: vec![],
            }));

            result?;
        }
        // Also take the controller from the ref, since that is used for
        // some operations.
        self.current_ref.controller.take();
        Ok(())
    }
}

impl<A: Referable> fmt::Display for ActorMesh<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.current_ref)
    }
}

impl<A: Referable> Deref for ActorMesh<A> {
    type Target = ActorMeshRef<A>;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
    }
}

/// Manual implementation of Clone because `A` doesn't need to implement Clone
/// but we still want to be able to clone the ActorMesh.
impl<A: Referable> Clone for ActorMesh<A> {
    fn clone(&self) -> Self {
        Self {
            proc_mesh: self.proc_mesh.clone(),
            id: self.id.clone(),
            current_ref: self.current_ref.clone(),
            controller: self.controller.clone(),
        }
    }
}

impl<A: Referable> Drop for ActorMesh<A> {
    fn drop(&mut self) {
        tracing::info!(
            name = "ActorMeshStatus",
            actor_name = %self.id,
            status = "Dropped",
        );
    }
}

/// Influences paging behavior for the lazy cache. Smaller pages
/// reduce over-allocation for sparse access; larger pages reduce the
/// number of heap allocations for contiguous scans.
const DEFAULT_PAGE: usize = 1024;

/// A lazily materialized page of ActorRefs.
struct Page<A: Referable> {
    slots: Box<[OnceCell<ActorRef<A>>]>,
}

impl<A: Referable> Page<A> {
    fn new(len: usize) -> Self {
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(OnceCell::new());
        }
        Self {
            slots: v.into_boxed_slice(),
        }
    }
}

#[derive(Default)]
struct HealthState {
    unhealthy_event: Option<Unhealthy>,
    crashed_ranks: HashMap<usize, ActorSupervisionEvent>,
}

impl HealthState {
    fn failure_for_region(&self, region: &Region) -> Option<MeshFailure> {
        let unhealthy = self.unhealthy_event.as_ref()?;
        let mut failure = match unhealthy {
            Unhealthy::StreamClosed(failure) | Unhealthy::Crashed(failure) => failure.clone(),
        };
        if failure.crashed_ranks.is_empty() {
            return Some(failure);
        }
        let mut crashed_ranks = self
            .crashed_ranks
            .keys()
            .copied()
            .filter(|rank| region.slice().contains(*rank))
            .collect::<Vec<_>>();
        crashed_ranks.sort_unstable();
        if crashed_ranks.is_empty() {
            return None;
        }
        failure.crashed_ranks = crashed_ranks;
        Some(failure)
    }
}

impl std::fmt::Debug for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthState")
            .field("unhealthy_event", &self.unhealthy_event)
            .field("crashed_ranks", &self.crashed_ranks)
            .finish()
    }
}

#[derive(Clone)]
enum MessageOrFailure<M: Send + Sync + Clone + Default + 'static> {
    Message(M),
    // anyhow::Error and MailboxError are not clone-able, which we need to move
    // out of a tokio watch Ref.
    Failure(String),
    Timeout,
}

impl<M: Send + Sync + Clone + Default + 'static> Default for MessageOrFailure<M> {
    fn default() -> Self {
        Self::Message(M::default())
    }
}

/// Turn the single-owner PortReceiver into a watch receiver, which can be
/// cloned and subscribed to. Requires a default message to pre-populate with.
/// Option can be used as M to provide a default of None.
fn into_watch<M: Send + Sync + Clone + Default + 'static>(
    mut rx: PortReceiver<M>,
) -> watch::Receiver<MessageOrFailure<M>> {
    let (sender, receiver) = watch::channel(MessageOrFailure::<M>::default());
    // Apply a watchdog timeout to the supervision stream. If no
    // supervision message (healthy or unhealthy) is observed within
    // this window, we assume the controller is unreachable and
    // surface a terminal failure on the watch channel. This is a
    // watchdog against indefinite silence, not a message-delivery
    // guarantee, and may conservatively treat a quiet but healthy
    // controller as failed.
    let timeout = hyperactor_config::global::get(SUPERVISION_WATCHDOG_TIMEOUT);
    let poll_frequency = hyperactor_config::global::get(SUPERVISION_POLL_FREQUENCY);
    let get_actor_state_max_idle = hyperactor_config::global::get(GET_ACTOR_STATE_MAX_IDLE);
    let get_proc_state_max_idle = hyperactor_config::global::get(GET_PROC_STATE_MAX_IDLE);
    let total_time = poll_frequency + get_actor_state_max_idle + get_proc_state_max_idle;
    if timeout < total_time {
        tracing::warn!(
            "HYPERACTOR_MESH_SUPERVISION_WATCHDOG_TIMEOUT={} is too short. It should be >= {} (SUPERVISION_POLL_FREQUENCY={} + GET_ACTOR_STATE_MAX_IDLE={} + GET_PROC_STATE_MAX_IDLE={})",
            humantime::format_duration(timeout),
            humantime::format_duration(total_time),
            humantime::format_duration(poll_frequency),
            humantime::format_duration(get_actor_state_max_idle),
            humantime::format_duration(get_proc_state_max_idle),
        );
    }
    tokio::spawn(async move {
        loop {
            let message = match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Ok(msg)) => MessageOrFailure::Message(msg),
                Ok(Err(e)) => MessageOrFailure::Failure(e.to_string()),
                Err(_) => MessageOrFailure::Timeout,
            };
            let is_failure = matches!(
                message,
                MessageOrFailure::Failure(_) | MessageOrFailure::Timeout
            );
            if sender.send(message).is_err() {
                // After a sending error, exit the task.
                break;
            }
            if is_failure {
                // No need to keep polling if we've received an error or timeout.
                break;
            }
        }
    });
    receiver
}

/// A reference to a stable snapshot of an [`ActorMesh`].
#[derive(typeuri::Named)]
pub struct ActorMeshRef<A: Referable> {
    proc_mesh: ProcMeshRef,
    id: ActorMeshId,
    /// Reference to a remote controller actor living on the proc that spawned
    /// the actors in this ref. If None, the actor mesh was already stopped, or
    /// this is a mesh ref to a "system actor" which has no controller and should
    /// not be stopped. If Some, the actor mesh may still be stopped, and the
    /// next_supervision_event function can be used to alert that the mesh has
    /// stopped.
    controller: Option<ActorRef<ActorMeshController<A>>>,

    /// Recorded health issues with the mesh, to quickly consult before sending
    /// out any casted messages. This is a locally updated copy of the authoritative
    /// state stored on the ActorMeshController.
    health_state: ActorLocal<HealthState>,
    /// Shared cloneable receiver for supervision events, used by next_supervision_event.
    /// Needs tokio mutex because it is held across an await point.
    /// Should not be shared across actors because each actor context needs its
    /// own subscriber.
    receiver: ActorLocal<
        Arc<
            tokio::sync::Mutex<(
                PortRef<Option<MeshFailure>>,
                watch::Receiver<MessageOrFailure<Option<MeshFailure>>>,
            )>,
        >,
    >,
    /// Lazily allocated collection of pages:
    /// - The outer `OnceCell` defers creating the vector until first
    ///   use.
    /// - The `Vec` holds slots for multiple pages.
    /// - Each slot is itself a `OnceCell<Box<Page<A>>>`, so that each
    ///   page can be initialized on demand.
    /// - A `Page<A>` is a boxed slice of `OnceCell<ActorRef<A>>`,
    ///   i.e. the actual storage for actor references within that
    ///   page.
    pages: OnceCell<Vec<OnceCell<Box<Page<A>>>>>,
    // Page size knob (not serialize; defaults after deserialize).
    page_size: usize,
}

impl<A: Referable> ActorMeshRef<A> {
    fn cached_failure(&self, cx: &impl context::Actor) -> Option<MeshFailure> {
        let health_state = self.health_state.entry(cx).or_default();
        health_state
            .get()
            .failure_for_region(ndslice::view::Ranked::region(self))
    }

    /// Cast a message to all the actors in this mesh
    #[allow(clippy::result_large_err)]
    pub fn cast<M>(&self, cx: &impl context::Actor, message: M) -> crate::Result<()>
    where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        self.cast_with_headers(cx, &Flattrs::new(), message)
    }

    /// Cast a message to all the actors in this mesh, merging
    /// caller-supplied `caller_headers` into the per-rank envelope
    /// headers before send. Used to propagate caller-known context
    /// (e.g. operation-context keys marked with `OPERATION_CONTEXT_HEADER`)
    /// onto the outgoing request so receivers can project it back
    /// onto replies.
    #[allow(clippy::result_large_err)]
    pub fn cast_with_headers<M>(
        &self,
        cx: &impl context::Actor,
        caller_headers: &Flattrs,
        message: M,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage + Clone,
    {
        self.check_cached_failure(cx)?;
        self.emit_sent_message_telemetry(cx, view::Ranked::region(self));

        if let Some(root_comm_actor) = self.proc_mesh.root_comm_actor() {
            if casting::v1_casting_enabled() {
                self.cast_v1(cx, message, root_comm_actor, caller_headers);
                Ok(())
            } else {
                self.cast_v0(
                    cx,
                    message,
                    ndslice::selection::dsl::true_(),
                    root_comm_actor,
                    caller_headers,
                )
            }
        } else {
            for (point, actor) in self.iter() {
                self.post_cast_direct(cx, point, &actor, message.clone(), caller_headers)?;
            }
            Ok(())
        }
    }

    /// Cast a message to one randomly chosen actor in this mesh, merging
    /// caller-supplied `caller_headers` into the outgoing envelope.
    #[allow(clippy::result_large_err)]
    pub fn cast_choose_with_headers<M>(
        &self,
        cx: &impl context::Actor,
        caller_headers: &Flattrs,
        message: M,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage + Clone,
    {
        self.check_cached_failure(cx)?;
        self.emit_sent_message_telemetry(
            cx,
            &Region::new(
                Vec::new(),
                ndslice::Slice::new(0, Vec::new(), Vec::new())
                    .expect("zero-dimensional slice is valid"),
            ),
        );

        if !casting::v1_casting_enabled()
            && let Some(root_comm_actor) = self.proc_mesh.root_comm_actor()
        {
            self.cast_v0(
                cx,
                message,
                ndslice::selection::dsl::any(ndslice::selection::dsl::true_()),
                root_comm_actor,
                caller_headers,
            )
        } else {
            self.cast_choose_direct(cx, message, caller_headers)
        }
    }

    /// Cast a message to the actors in this mesh according to the provided selection.
    /// This should *only* be used for temporary support for selections in the tensor
    /// engine. If you use this for anything else, you will be fired (you too, OSS
    /// contributor).
    #[allow(clippy::result_large_err)]
    pub fn cast_for_tensor_engine_only_do_not_use<M>(
        &self,
        cx: &impl context::Actor,
        sel: Selection,
        message: M,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        self.check_cached_failure(cx)?;
        self.emit_sent_message_telemetry(cx, view::Ranked::region(self));

        let Some(root_comm_actor) = self.proc_mesh.root_comm_actor() else {
            return Err(Error::CastingError(
                self.id.clone(),
                anyhow::anyhow!("tensor-engine selection casts require a root CommActor"),
            ));
        };

        self.cast_v0(cx, message, sel, root_comm_actor, &Flattrs::new())
    }

    #[allow(clippy::result_large_err)]
    fn check_cached_failure(&self, cx: &impl context::Actor) -> crate::Result<()> {
        // First check if the mesh is already dead before sending out any messages
        // to a possibly undeliverable actor.
        if let Some(failure) = self.cached_failure(cx) {
            tracing::debug!(
                actor_mesh = %self.id,
                crashed_ranks = ?failure.crashed_ranks,
                "rejecting cast due to cached supervision failure"
            );
            return Err(crate::Error::Supervision(Box::new(failure)));
        }

        Ok(())
    }

    fn emit_sent_message_telemetry(&self, cx: &impl context::Actor, region: &Region) {
        hyperactor_telemetry::notify_sent_message(hyperactor_telemetry::SentMessageEvent {
            timestamp: std::time::SystemTime::now(),
            sender_actor_id: hyperactor_telemetry::hash_to_u64(cx.mailbox().actor_addr().id()),
            actor_mesh_id: telemetry_actor_mesh_id(self.proc_mesh.id(), &self.id),
            view_json: serde_json::to_string(region).unwrap_or_default(),
            shape_json: {
                let shape: ndslice::Shape = region.into();
                serde_json::to_string(&shape).unwrap_or_default()
            },
        });
    }

    #[allow(clippy::result_large_err)]
    fn cast_choose_direct<M>(
        &self,
        cx: &impl context::Actor,
        message: M,
        caller_headers: &Flattrs,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage,
    {
        let region = view::Ranked::region(self);

        let num_ranks = region.num_ranks();
        if num_ranks == 0 {
            return Ok(());
        }

        let rank_index = rand::random::<u64>() as usize % num_ranks;

        let point = region
            .extent()
            .point_of_rank(rank_index)
            .map_err(|err| Error::CastingError(self.id.clone(), err.into()))?;

        let actor = view::Ranked::get(self, point.rank()).ok_or_else(|| {
            Error::CastingError(
                self.id.clone(),
                anyhow::anyhow!("missing actor for chosen rank {}", point.rank()),
            )
        })?;

        self.post_cast_direct(cx, point, actor, message, caller_headers)
    }

    #[allow(clippy::result_large_err)]
    fn post_cast_direct<M>(
        &self,
        cx: &impl context::Actor,
        point: ndslice::Point,
        actor: &ActorRef<A>,
        message: M,
        caller_headers: &Flattrs,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage,
    {
        let create_rank = point.rank();
        let mut headers = caller_headers.clone();
        multicast::set_cast_info_on_headers(&mut headers, point, cx.instance().self_addr().clone());

        // Make sure that we rewrite ranks, as these may be used for
        // bootstrapping comm actors.
        let mut data = MultipartMessage::try_from_message(message)
            .map_err(|e| Error::CastingError(self.id.clone(), e))?;
        data.visit_mut::<resource::RankRepr>(|resource::RankRepr(rank)| {
            *rank = Some(create_rank);
            Ok(())
        })
        .map_err(|e| Error::CastingError(self.id.clone(), e))?;
        let rebound_message = data
            .deserialize()
            .map_err(|e| Error::CastingError(self.id.clone(), e))?;
        actor.post_with_headers(cx, headers, rebound_message);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn cast_v0<M>(
        &self,
        cx: &impl context::Actor,
        message: M,
        sel: Selection,
        root_comm_actor: &ActorRef<CommActor>,
        caller_headers: &Flattrs,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        let cast_mesh_shape = view::Ranked::region(self).into();
        let actor_mesh_id = self.id.clone();
        match &self.proc_mesh.root_region {
            Some(root_region) => {
                let root_mesh_shape = root_region.into();
                casting::cast_to_sliced_mesh::<A, M>(
                    cx,
                    actor_mesh_id,
                    root_comm_actor,
                    &sel,
                    message,
                    &cast_mesh_shape,
                    &root_mesh_shape,
                    caller_headers,
                )
                .map_err(|e| Error::CastingError(self.id.clone(), e.into()))
            }
            None => casting::actor_mesh_cast::<A, M>(
                cx,
                actor_mesh_id,
                root_comm_actor,
                sel,
                &cast_mesh_shape,
                &cast_mesh_shape,
                message,
                caller_headers,
            )
            .map_err(|e| Error::CastingError(self.id.clone(), e.into())),
        }
    }

    fn cast_v1<M>(
        &self,
        cx: &impl context::Actor,
        message: M,
        root_comm_actor: &ActorRef<CommActor>,
        caller_headers: &Flattrs,
    ) where
        A: RemoteHandles<M>,
        M: Castable + RemoteMessage,
    {
        let _ = metrics::ACTOR_MESH_CAST_DURATION.start(hyperactor::kv_pairs!(
            "message_type" => <M as typeuri::Named>::typename(),
            "message_variant" => message.arm().unwrap_or_default(),
        ));

        let actor_ids: ValueMesh<_> = self.proc_mesh.map_into(|proc| proc.actor_addr(&self.id));

        let mut headers = caller_headers.clone();
        headers.set(
            multicast::CAST_ORIGINATING_SENDER,
            cx.instance().self_addr().clone(),
        );
        // Set CAST_ACTOR_MESH_ID temporarily to support supervision's
        // v0 transition. Should be removed once supervision is migrated
        // and ActorMeshId is deleted.
        headers.set(casting::CAST_ACTOR_MESH_ID, self.id.clone());

        let region = view::Ranked::region(self).clone();
        let num_ranks = region.num_ranks();
        let threshold = hyperactor_config::global::get(V1_CAST_POINT_TO_POINT_THRESHOLD);

        if threshold > 0 && num_ranks < threshold {
            // Point-to-point: send directly to each destination actor,
            // bypassing the comm actor tree for lower latency when fanout
            // is small.
            let sender = cx.instance().self_addr().clone();
            let dest_port = M::port();

            let mut data = MultipartMessage::try_from_message(message)
                .expect("cast message serialization should not fail");

            // Split ports for N destinations, matching the comm tree's
            // split_ports behavior.
            data.visit_mut::<PortRefRepr>(|port| {
                if port.unsplit() {
                    return Ok(());
                }
                let split = port.port_addr().split(
                    cx,
                    port.reducer_spec().clone(),
                    ReducerMode::Streaming(port.streaming_opts().clone()),
                    port.get_return_undeliverable(),
                )?;
                port.update_port_addr(split);
                Ok(())
            })
            .expect("port splitting should not fail");

            data.visit_mut::<OncePortRefRepr>(|port| {
                if port.unsplit() || port.reducer_spec().is_none() {
                    // Once ports without reducers pass through, same as the comm
                    // tree's split_ports.
                    return Ok(());
                }
                let split = port.port_addr().split(
                    cx,
                    port.reducer_spec().clone(),
                    ReducerMode::Once(num_ranks),
                    true,
                )?;
                port.update_port_addr(split);
                Ok(())
            })
            .expect("once port splitting should not fail");

            for rank in 0..num_ranks {
                let mut rank_data = data.clone();

                let cast_point = region
                    .point_of_base_rank(rank)
                    .expect("rank should be valid in region");

                rank_data
                    .visit_mut::<resource::RankRepr>(|resource::RankRepr(r)| {
                        *r = Some(cast_point.rank());
                        Ok(())
                    })
                    .expect("rank replacement should not fail");

                let mut rank_headers = headers.clone();
                multicast::set_cast_info_on_headers(&mut rank_headers, cast_point, sender.clone());

                let port_id = actor_ids
                    .get(rank)
                    .expect("mismatched actor_ids and dest_region")
                    .port_addr(Port::handler_id(dest_port, None));

                cx.instance().post(
                    port_id,
                    rank_headers,
                    rank_data.into_message().erase_encoding(),
                );
            }
        } else {
            // Tree path: route through the comm actor tree.
            // Pre-compute sequence numbers — this block is infallible so
            // rollback is not a concern.
            let sequencer = cx.instance().sequencer();
            let seqs: ValueMesh<u64> = actor_ids.map_into(|actor_id| {
                let hyperactor::ordering::SeqInfo::Session { seq, .. } =
                    sequencer.assign_seq(&actor_id.port_addr(Port::handler::<M>()))
                else {
                    unreachable!("assign_seq always returns SeqInfo::Session")
                };
                seq
            });

            let mut headers = caller_headers.clone();
            headers.set(
                multicast::CAST_ORIGINATING_SENDER,
                cx.instance().self_addr().clone(),
            );
            // Set CAST_ACTOR_MESH_ID temporarily to support supervision's
            // v0 transition. Should be removed once supervision is migrated
            // and ActorMeshId is deleted.
            headers.set(casting::CAST_ACTOR_MESH_ID, self.id.clone());
            let cast_message = CastMessageV1::new::<A, M>(
                cx.instance().self_addr().clone(),
                &self.id,
                region,
                headers.clone(),
                message,
                sequencer.session_id(),
                seqs,
            )
            .expect("infallible because CastMessage should not fail for serialization");

            // TODO: load balancing instead of always using the first comm actor
            root_comm_actor.post_with_headers(cx, headers, cast_message);
        }
    }
    pub(crate) fn new(
        id: ActorMeshId,
        proc_mesh: ProcMeshRef,
        controller: Option<ActorRef<ActorMeshController<A>>>,
    ) -> Self {
        Self::with_page_size(id, proc_mesh, DEFAULT_PAGE, controller)
    }

    pub fn id(&self) -> &ActorMeshId {
        &self.id
    }

    pub(crate) fn with_page_size(
        id: ActorMeshId,
        proc_mesh: ProcMeshRef,
        page_size: usize,
        controller: Option<ActorRef<ActorMeshController<A>>>,
    ) -> Self {
        Self {
            proc_mesh,
            id,
            controller,
            health_state: ActorLocal::new(),
            receiver: ActorLocal::new(),
            pages: OnceCell::new(),
            page_size: page_size.max(1),
        }
    }

    pub fn proc_mesh(&self) -> &ProcMeshRef {
        &self.proc_mesh
    }

    #[inline]
    fn len(&self) -> usize {
        view::Ranked::region(&self.proc_mesh).num_ranks()
    }

    pub fn controller(&self) -> &Option<ActorRef<ActorMeshController<A>>> {
        &self.controller
    }

    fn set_controller(&mut self, controller: Option<ActorRef<ActorMeshController<A>>>) {
        self.controller = controller;
    }

    fn ensure_pages(&self) -> &Vec<OnceCell<Box<Page<A>>>> {
        let n = self.len().div_ceil(self.page_size); // ⌈len / page_size⌉
        self.pages
            .get_or_init(|| (0..n).map(|_| OnceCell::new()).collect())
    }

    fn materialize(&self, rank: usize) -> Option<&ActorRef<A>> {
        let len = self.len();
        if rank >= len {
            return None;
        }
        let p = self.page_size;
        let page_ix = rank / p;
        let local_ix = rank % p;

        let pages = self.ensure_pages();
        let page = pages[page_ix].get_or_init(|| {
            // Last page may be partial.
            let base = page_ix * p;
            let remaining = len - base;
            let page_len = remaining.min(p);
            Box::new(Page::<A>::new(page_len))
        });

        Some(page.slots[local_ix].get_or_init(|| {
            // AM-1: see module doc.
            //   - ranks are contiguous [0, self.len()) with no gaps
            //     or reordering
            //   - for every rank r, `proc_mesh.get(r)` is Some(..)
            // Therefore we can index `proc_mesh` with `rank`
            // directly.
            debug_assert!(rank < self.len(), "rank must be within [0, len)");
            debug_assert!(
                ndslice::view::Ranked::get(&self.proc_mesh, rank).is_some(),
                "proc_mesh must be dense/aligned with this view"
            );
            let proc_ref =
                ndslice::view::Ranked::get(&self.proc_mesh, rank).expect("rank in-bounds");
            proc_ref.attest(&self.id)
        }))
    }

    fn init_supervision_receiver(
        controller: &ActorRef<ActorMeshController<A>>,
        cx: &impl context::Actor,
    ) -> (
        PortRef<Option<MeshFailure>>,
        watch::Receiver<MessageOrFailure<Option<MeshFailure>>>,
    ) {
        let (tx, rx) = cx.mailbox().open_port();
        let tx = tx.bind();
        controller.post(cx, Subscribe(tx.clone()));
        (tx, into_watch(rx))
    }

    /// Returns the next supervision event occurring on this mesh. Await this
    /// simultaneously with the return result of a message (such as awaiting a reply after a cast)
    /// to get back a message that indicates the actor that failed, instead of
    /// waiting forever for a reply.
    /// If there are multiple simultaneous awaits of next_supervision_event,
    /// all of them will receive the same event.
    pub async fn next_supervision_event(
        &self,
        cx: &impl context::Actor,
    ) -> Result<MeshFailure, anyhow::Error> {
        if let Some(failure) = self.cached_failure(cx) {
            tracing::debug!(
                actor_mesh = %self.id,
                crashed_ranks = ?failure.crashed_ranks,
                "returning cached supervision failure"
            );
            return Ok(failure);
        }
        let controller = if let Some(c) = self.controller() {
            c
        } else {
            return Err(anyhow::anyhow!(
                "unexpected healthy state while controller is gone"
            ));
        };
        let rx = {
            // Make sure to create only one PortReceiver per context.
            let entry = self.receiver.entry(cx).or_insert_with(|| {
                Arc::new(tokio::sync::Mutex::new(Self::init_supervision_receiver(
                    controller, cx,
                )))
            });
            // Need to clone so the lifetime is disconnected from entry, which
            // isn't Send so can't be held across an await point.
            Arc::clone(entry.get())
        };
        let message = {
            let mut rx = rx.lock().await;
            let subscriber_port = rx.0.clone();
            let message =
                rx.1.wait_for(|message| {
                    // Filter out messages that do not apply to these ranks. This
                    // is relevant for slices since we get messages back for the
                    // whole mesh.
                    if let MessageOrFailure::Message(message) = message {
                        if let Some(message) = &message {
                            let region = ndslice::view::Ranked::region(self).slice();
                            if message.crashed_ranks.is_empty() {
                                // Whole-mesh event (e.g. mesh stop).
                                true
                            } else {
                                // Accept if any crashed rank overlaps with
                                // this slice's region.
                                message.crashed_ranks.iter().any(|r| region.contains(*r))
                            }
                        } else {
                            // Filter out messages that are not failures. These are used
                            // to ensure the controller is still reachable, but are not
                            // otherwise interesting.
                            false
                        }
                    } else {
                        // either failure case is interesting
                        true
                    }
                })
                .await?;
            let message = message.clone();
            let is_failure = matches!(
                message,
                MessageOrFailure::Failure(_) | MessageOrFailure::Timeout
            );
            if is_failure {
                // In failure cases, the receiver is dropped, so we can unsubscribe
                // from the controller. The controller can detect this
                // on its own, but an explicit unsubscribe prevents error logs
                // about this receiver being unreachable.
                let mut port = controller.port();
                // We don't care if the controller is unreachable for an unsubscribe.
                port.return_undeliverable(false);
                let _ = port.post(cx, Unsubscribe(subscriber_port));
            }
            // If we successfully got a message back, we can't unsubscribe because
            // the receiver might be shared with other calls to next_supervision_event,
            // or with clones of this ActorMeshRef.
            match message {
                MessageOrFailure::Message(message) => Ok::<MeshFailure, anyhow::Error>(
                    message.expect("filter excludes any None messages"),
                ),
                MessageOrFailure::Failure(failure) => Err(anyhow::anyhow!("{}", failure)),
                MessageOrFailure::Timeout => {
                    // Treat timeout from controller as a supervision failure,
                    // the controller is unreachable.
                    Ok(MeshFailure {
                        actor_mesh_name: Some(self.id().to_string()),
                        event: ActorSupervisionEvent::new(
                            controller.actor_addr().clone(),
                            None,
                            ActorStatus::generic_failure(format!(
                                "timed out reaching controller {} for mesh {}. Assuming controller's proc is dead",
                                controller.actor_addr(),
                                self.id()
                            )),
                            None,
                        ),
                        crashed_ranks: vec![],
                    })
                }
            }?
        };
        // Update the health state now that we have received a message.
        let event = &message.event;
        // Make sure not to hold this lock across an await point.
        let mut entry = self.health_state.entry(cx).or_default();
        let health_state = entry.get_mut();
        if let ActorStatus::Failed(_) = event.actor_status {
            for &rank in &message.crashed_ranks {
                health_state.crashed_ranks.insert(rank, event.clone());
            }
        }
        health_state.unhealthy_event = match &event.actor_status {
            ActorStatus::Failed(_) => Some(Unhealthy::Crashed(message.clone())),
            ActorStatus::Stopped(_) => Some(Unhealthy::StreamClosed(message.clone())),
            _ => None,
        };
        Ok(message)
    }

    /// Same as Clone, but includes a shared supervision receiver. This copy will
    /// share the same health state and get the same supervision events.
    /// Will have a separate cache.
    pub fn clone_with_supervision_receiver(&self) -> Self {
        Self {
            proc_mesh: self.proc_mesh.clone(),
            id: self.id.clone(),
            controller: self.controller.clone(),
            health_state: self.health_state.clone(),
            receiver: self.receiver.clone(),
            // Cache does not support Clone at this time.
            pages: OnceCell::new(),
            page_size: self.page_size,
        }
    }
}

impl<A: Referable> Clone for ActorMeshRef<A> {
    fn clone(&self) -> Self {
        Self {
            proc_mesh: self.proc_mesh.clone(),
            id: self.id.clone(),
            controller: self.controller.clone(),
            // Cloning should not use the same health state or receiver, because
            // it should make a new subscriber.
            health_state: ActorLocal::new(),
            receiver: ActorLocal::new(),
            pages: OnceCell::new(), // No clone cache.
            page_size: self.page_size,
        }
    }
}

impl<A: Referable> fmt::Display for ActorMeshRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}@{}", self.id, A::typename(), self.proc_mesh)
    }
}

impl<A: Referable> PartialEq for ActorMeshRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.proc_mesh == other.proc_mesh && self.id == other.id
    }
}
impl<A: Referable> Eq for ActorMeshRef<A> {}

impl<A: Referable> Hash for ActorMeshRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.proc_mesh.hash(state);
        self.id.hash(state);
    }
}

impl<A: Referable> fmt::Debug for ActorMeshRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorMeshRef")
            .field("proc_mesh", &self.proc_mesh)
            .field("id", &self.id)
            .field("page_size", &self.page_size)
            .finish_non_exhaustive() // No print cache.
    }
}

// Implement Serialize manually, without requiring A: Serialize
impl<A: Referable> Serialize for ActorMeshRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (&self.proc_mesh, &self.id, &self.controller).serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize
impl<'de, A: Referable> Deserialize<'de> for ActorMeshRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (proc_mesh, id, controller) = <(
            ProcMeshRef,
            ActorMeshId,
            Option<ActorRef<ActorMeshController<A>>>,
        )>::deserialize(deserializer)?;
        Ok(ActorMeshRef::with_page_size(
            id,
            proc_mesh,
            DEFAULT_PAGE,
            controller,
        ))
    }
}

impl<A: Referable> view::Ranked for ActorMeshRef<A> {
    type Item = ActorRef<A>;

    #[inline]
    fn region(&self) -> &Region {
        view::Ranked::region(&self.proc_mesh)
    }

    #[inline]
    fn get(&self, rank: usize) -> Option<&Self::Item> {
        self.materialize(rank)
    }
}

impl<A: Referable> view::RankedSliceable for ActorMeshRef<A> {
    fn sliced(&self, region: Region) -> Self {
        // Slices inherit cached failures that were already observed on the parent
        // mesh ref so new sub-slices do not race the controller replay path.
        // The supervision receiver stays independent because each slice applies
        // its own region filter to future updates.
        debug_assert!(region.is_subset(view::Ranked::region(self)));
        let proc_mesh = self.proc_mesh.subset(region).unwrap();
        Self {
            proc_mesh,
            id: self.id.clone(),
            controller: self.controller.clone(),
            health_state: self.health_state.clone(),
            receiver: ActorLocal::new(),
            pages: OnceCell::new(),
            page_size: self.page_size,
        }
    }
}

#[cfg(all(test, fbcode_build))]
mod tests {

    use std::collections::HashSet;
    use std::ops::Deref;

    use hyperactor::Endpoint as _;
    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::context::Mailbox as _;
    use hyperactor::id::Label;
    use hyperactor::mailbox;
    use ndslice::Extent;
    use ndslice::Region;
    use ndslice::Slice;
    use ndslice::ViewExt;
    use ndslice::extent;
    use ndslice::view::Ranked;
    use ndslice::view::RankedSliceable;
    use timed_test::assert_no_process_leak;
    use timed_test::async_timed_test;
    use tokio::time::Duration;

    use super::ActorMesh;
    use crate::ActorMeshRef;
    use crate::ProcMesh;
    use crate::host_mesh::GET_PROC_STATE_MAX_IDLE;
    use crate::host_mesh::PROC_SPAWN_MAX_IDLE;
    use crate::mesh_controller::SUPERVISION_POLL_FREQUENCY;
    use crate::mesh_id::ActorMeshId;
    use crate::proc_mesh::ACTOR_SPAWN_MAX_IDLE;
    use crate::proc_mesh::GET_ACTOR_STATE_MAX_IDLE;
    use crate::supervision::MeshFailure;
    use crate::testactor;
    use crate::testing;

    #[test]
    fn test_actor_mesh_ref_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ActorMeshRef<()>>();
    }

    #[tokio::test]
    async fn test_actor_mesh_ref_lazy_materialization() {
        // 1) Bring up procs and spawn actors.
        let instance = testing::instance();
        // Small mesh so the test runs fast, but > page_size so we
        // cross a boundary
        let mut hm = testing::host_mesh(2).await;
        let pm: ProcMesh = hm
            .spawn(instance, "test", extent!(gpus = 2), None, None)
            .await
            .unwrap();
        let am: ActorMesh<testactor::TestActor> = pm.spawn(instance, "test", &()).await.unwrap();

        // 2) Build our ActorMeshRef with a tiny page size (2) to
        // force multiple pages:
        // page 0: ranks [0,1], page 1: [2,3], page 2: [4,5]
        let page_size = 2;
        let amr: ActorMeshRef<testactor::TestActor> =
            ActorMeshRef::with_page_size(am.id.clone(), pm.clone(), page_size, None);
        assert_eq!(amr.extent(), extent!(hosts = 2, gpus = 2));
        assert_eq!(amr.region().num_ranks(), 4);

        // 3) Within-rank pointer stability (OnceLock caches &ActorRef)
        let p0_a = amr.get(0).expect("rank 0 exists") as *const _;
        let p0_b = amr.get(0).expect("rank 0 exists") as *const _;
        assert_eq!(p0_a, p0_b, "same rank should return same cached pointer");

        // 4) Same page, different rank (both materialize fine)
        let p1_a = amr.get(1).expect("rank 1 exists") as *const _;
        let p1_b = amr.get(1).expect("rank 1 exists") as *const _;
        assert_eq!(p1_a, p1_b, "same rank should return same cached pointer");
        // They're different ranks, so the pointers are different
        // (distinct OnceLocks in the page)
        assert_ne!(p0_a, p1_a, "different ranks have different cache slots");

        // 5) Cross a page boundary (rank 2 is in a different page than rank 0/1)
        let p2_a = amr.get(2).expect("rank 2 exists") as *const _;
        let p2_b = amr.get(2).expect("rank 2 exists") as *const _;
        assert_eq!(p2_a, p2_b, "same rank should return same cached pointer");
        assert_ne!(p0_a, p2_a, "different pages have different cache slots");

        // 6) Clone should drop the cache but keep identity (actor_id)
        let amr_clone = amr.clone();
        let orig_id_0 = amr.get(0).unwrap().actor_addr().clone();
        let clone_id_0 = amr_clone.get(0).unwrap().actor_addr().clone();
        assert_eq!(orig_id_0, clone_id_0, "clone preserves identity");
        let p0_clone = amr_clone.get(0).unwrap() as *const _;
        assert_ne!(
            p0_a, p0_clone,
            "cloned ActorMeshRef has a fresh cache (different pointer)"
        );

        // 7) Slicing preserves page_size and clears cache
        // (RankedSliceable::sliced)
        let sliced = amr.range("hosts", 0..2).expect("slice should be valid"); // leaves 4 ranks
        assert_eq!(sliced.region().num_ranks(), 4);
        // First access materializes a new cache for the sliced view.
        let sp0_a = sliced.get(0).unwrap() as *const _;
        let sp0_b = sliced.get(0).unwrap() as *const _;
        assert_eq!(sp0_a, sp0_b, "sliced view has its own cache slot per rank");
        // Cross-page inside the slice too (page_size = 2 => pages are
        // [0..2), [2..4)).
        let sp2 = sliced.get(2).unwrap() as *const _;
        assert_ne!(sp0_a, sp2, "sliced view crosses its own page boundary");

        // 8) Hash/Eq ignore cache state; identical identity collapses
        // to one set entry.
        let mut set = HashSet::new();
        set.insert(amr.clone());
        set.insert(amr.clone());
        assert_eq!(set.len(), 1, "cache state must not affect Hash/Eq");

        // 9) As a sanity check, cast to ensure the refs are indeed
        // usable/live.
        let (port, mut rx) = mailbox::open_port(instance);
        // Send to rank 0 and rank 3 (extent 3x2 => at least 4 ranks
        // exist).
        amr.get(0)
            .expect("rank 0 exists")
            .post(instance, testactor::GetActorId(port.bind()));
        amr.get(3)
            .expect("rank 3 exists")
            .post(instance, testactor::GetActorId(port.bind()));
        let id_a = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for first reply")
            .expect("channel closed before first reply");
        let id_b = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for second reply")
            .expect("channel closed before second reply");
        assert_ne!(id_a, id_b, "two different ranks responded");

        let _ = hm.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 300)]
    async fn test_actor_states_with_panic() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();
        let config = hyperactor_config::global::lock();
        let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(120));
        let _actor_spawn = config.override_key(ACTOR_SPAWN_MAX_IDLE, Duration::from_secs(120));
        let _host_spawn = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(120),
        );

        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();
        let num_replicas = 1;
        let mut hm = testing::host_mesh(num_replicas).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();
        let child_name = ActorMeshId::instance(Label::new("child").unwrap());

        // Need to use a wrapper as there's no way to customize the handler for MeshFailure
        // on the client instance. The client would just panic with the message.
        let actor_mesh: ActorMesh<testactor::WrapperActor> = proc_mesh
            .spawn(
                instance,
                "wrapper",
                &(proc_mesh.deref().clone(), supervisor, child_name.clone()),
            )
            .await
            .unwrap();

        // Trigger the supervision error.
        actor_mesh
            .cast(
                instance,
                testactor::CauseSupervisionEvent {
                    kind: testactor::SupervisionEventType::Panic,
                    send_to_children: true,
                },
            )
            .unwrap();

        // The error will come back on two different pathways:
        // * on the ActorMeshRef stored in WrapperActor
        //   as an observable supervision event as a subscriber.
        // * on the owning actor (WrapperActor here) to be handled.
        // We test to ensure both have occurred.

        // First test the ActorMeshRef got the event.
        // Use a NextSupervisionFailure message to get the event from the wrapper
        // actor.
        let (failure_port, mut failure_receiver) = instance.open_port::<Option<MeshFailure>>();
        actor_mesh
            .cast(
                instance,
                testactor::NextSupervisionFailure(failure_port.bind()),
            )
            .unwrap();
        let failure = failure_receiver
            .recv()
            .await
            .unwrap()
            .expect("no supervision event found on ref from wrapper actor");
        let check_failure = move |failure: MeshFailure| {
            assert_eq!(failure.actor_mesh_name, Some(child_name.to_string()));
            assert!(
                failure
                    .event
                    .actor_id
                    .label()
                    .unwrap()
                    .as_str()
                    .starts_with(child_name.label().unwrap().as_str())
            );
            if let ActorStatus::Failed(ActorErrorKind::Generic(msg)) = &failure.event.actor_status {
                assert!(msg.contains("panic"), "{}", msg);
                assert!(msg.contains("for testing"), "{}", msg);
            } else {
                panic!("actor status is not failed: {}", failure.event.actor_status);
            }
        };
        check_failure(failure);

        // The wrapper actor should *not* have an event.

        // Wait for a supervision event to reach the wrapper actor.
        for _ in 0..num_replicas {
            let failure =
                tokio::time::timeout(Duration::from_secs(20), supervision_receiver.recv())
                    .await
                    .expect("timeout")
                    .unwrap();
            check_failure(failure);
        }

        let _ = hm.shutdown(instance).await;
    }

    #[assert_no_process_leak]
    #[async_timed_test(timeout_secs = 300)]
    async fn test_actor_states_with_process_exit() {
        hyperactor_telemetry::initialize_logging_for_test();

        let config = hyperactor_config::global::lock();
        let _poll = config.override_key(SUPERVISION_POLL_FREQUENCY, Duration::from_secs(1));
        let _guard = config.override_key(GET_ACTOR_STATE_MAX_IDLE, Duration::from_secs(1));
        let _proc_guard = config.override_key(GET_PROC_STATE_MAX_IDLE, Duration::from_secs(1));
        let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(120));
        let _host_spawn = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(120),
        );

        let instance = testing::instance();
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();
        let num_replicas = 1;
        let mut hm = testing::host_mesh(num_replicas).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();
        let mut second_hm = testing::host_mesh(num_replicas).await;
        let second_proc_mesh = second_hm
            .spawn(instance, "test2", Extent::unity(), None, None)
            .await
            .unwrap();
        let child_name = ActorMeshId::instance(Label::new("child").unwrap());

        // Need to use a wrapper as there's no way to customize the handler for MeshFailure
        // on the client instance. The client would just panic with the message.
        let actor_mesh: ActorMesh<testactor::WrapperActor> = proc_mesh
            .spawn(
                instance,
                "wrapper",
                &(
                    // Need a second set of proc meshes for the inner test actor, so the
                    // WrapperActor is still alive and gets the message.
                    second_proc_mesh.deref().clone(),
                    supervisor,
                    child_name.clone(),
                ),
            )
            .await
            .unwrap();

        actor_mesh
            .cast(
                instance,
                testactor::CauseSupervisionEvent {
                    kind: testactor::SupervisionEventType::ProcessExit(1),
                    send_to_children: true,
                },
            )
            .unwrap();

        // Same drill as for panic, except this one is for process exit.
        let (failure_port, mut failure_receiver) = instance.open_port::<Option<MeshFailure>>();
        actor_mesh
            .cast(
                instance,
                testactor::NextSupervisionFailure(failure_port.bind()),
            )
            .unwrap();
        let failure = failure_receiver
            .recv()
            .await
            .unwrap()
            .expect("no supervision event found on ref from wrapper actor");

        let check_failure = move |failure: MeshFailure| {
            assert_eq!(failure.actor_mesh_name, Some(child_name.to_string()));
            assert!(
                failure
                    .event
                    .actor_id
                    .label()
                    .unwrap()
                    .as_str()
                    .starts_with(child_name.label().unwrap().as_str())
            );
            if let ActorStatus::Failed(ActorErrorKind::Generic(msg)) = &failure.event.actor_status {
                assert!(msg.contains("exited with non-zero code 1"), "{}", msg);
            } else {
                panic!("actor status is not failed: {}", failure.event.actor_status);
            }
        };
        check_failure(failure);

        // Wait for a supervision event to occur on these actors.
        for _ in 0..num_replicas {
            let failure =
                tokio::time::timeout(Duration::from_secs(20), supervision_receiver.recv())
                    .await
                    .expect("timeout")
                    .unwrap();
            check_failure(failure);
        }

        let _ = second_hm.shutdown(instance).await;
        let _ = hm.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 300)]
    async fn test_actor_states_on_sliced_mesh() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();
        let (mut hm, _actor_mesh, sliced, sliced_replicas, child_name) = {
            let config = hyperactor_config::global::lock();
            let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(120));
            let _actor_spawn = config.override_key(ACTOR_SPAWN_MAX_IDLE, Duration::from_secs(120));
            let _host_spawn = config.override_key(
                hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
                Duration::from_secs(120),
            );
            let num_replicas = 2;
            let hm = testing::host_mesh(num_replicas).await;
            let proc_mesh = hm
                .spawn(instance, "test", Extent::unity(), None, None)
                .await
                .unwrap();
            let child_name = ActorMeshId::instance(Label::new("child").unwrap());

            // Need to use a wrapper as there's no way to customize the handler for MeshFailure
            // on the client instance. The client would just panic with the message.
            let actor_mesh: ActorMesh<testactor::WrapperActor> = proc_mesh
                .spawn(
                    instance,
                    "wrapper",
                    &(proc_mesh.deref().clone(), supervisor, child_name.clone()),
                )
                .await
                .unwrap();
            let sliced = actor_mesh
                .range("hosts", 1..2)
                .expect("slice should be valid");
            let sliced_replicas = sliced.len();
            (hm, actor_mesh, sliced, sliced_replicas, child_name)
        };

        // TODO: check that independent slice refs don't get the supervision event.
        sliced
            .cast(
                instance,
                testactor::CauseSupervisionEvent {
                    kind: testactor::SupervisionEventType::Panic,
                    send_to_children: true,
                },
            )
            .unwrap();

        for _ in 0..sliced_replicas {
            let supervision_message =
                tokio::time::timeout(Duration::from_secs(20), supervision_receiver.recv())
                    .await
                    .expect("timeout")
                    .unwrap();
            let event = supervision_message.event;
            assert!(
                event
                    .actor_id
                    .label()
                    .unwrap()
                    .as_str()
                    .starts_with(child_name.label().unwrap().as_str())
            );
            if let ActorStatus::Failed(ActorErrorKind::Generic(msg)) = &event.actor_status {
                assert!(msg.contains("panic"));
                assert!(msg.contains("for testing"));
            } else {
                panic!("actor status is not failed: {}", event.actor_status);
            }
        }

        let _ = hm.shutdown(instance).await;
    }

    async fn execute_cast(config: &hyperactor_config::global::ConfigLock) {
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);
        let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _host_spawn = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        let instance = testing::instance();
        let mut host_mesh = testing::host_mesh(2).await;
        let proc_mesh = host_mesh
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();
        let actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(instance, "test", &()).await.unwrap();

        let (cast_info, mut cast_info_rx) = instance.mailbox().open_port();
        actor_mesh
            .cast(
                instance,
                testactor::GetCastInfo {
                    cast_info: cast_info.bind(),
                },
            )
            .unwrap();

        let mut point_to_actor: HashSet<_> = actor_mesh.iter().collect();
        while !point_to_actor.is_empty() {
            let (point, origin_actor_ref, sender_actor_id) = cast_info_rx.recv().await.unwrap();
            let key = (point, origin_actor_ref);
            assert!(
                point_to_actor.remove(&key),
                "key {:?} not present or removed twice",
                key
            );
            assert_eq!(&sender_actor_id, instance.self_addr());
        }

        let _ = host_mesh.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_sliced_actor_mesh_cast_v1_reaches_slice_members() {
        use hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER;

        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);
        let _v1 = config.override_key(crate::comm::ENABLE_NATIVE_V1_CASTING, true);
        let _reorder = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _host_spawn = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        let instance = testing::instance();
        let mut host_mesh = testing::host_mesh(2).await;
        let proc_mesh = host_mesh
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();
        let root_actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(instance, "test", &()).await.unwrap();

        // Cast through a sliced mesh — `cast` still means all, but all is
        // scoped to the immutable sliced rank space.
        let actor_mesh = root_actor_mesh.sliced(Region::new(
            vec!["rank".to_string()],
            Slice::new(0, vec![1], vec![1]).unwrap(),
        ));
        let (cast_info, mut cast_info_rx) = instance.mailbox().open_port();
        actor_mesh
            .cast(
                instance,
                testactor::GetCastInfo {
                    cast_info: cast_info.bind(),
                },
            )
            .unwrap();

        let (point, _actor_ref, _sender) = cast_info_rx.recv().await.unwrap();
        let received_ranks = HashSet::from([point.rank()]);
        assert_eq!(received_ranks, HashSet::from([0]));

        // Also cast the root mesh — all ranks should be reached via V1.
        let (cast_info2, mut cast_info_rx2) = instance.mailbox().open_port();
        root_actor_mesh
            .cast(
                instance,
                testactor::GetCastInfo {
                    cast_info: cast_info2.bind(),
                },
            )
            .unwrap();

        let mut all_ranks: HashSet<usize> = HashSet::new();
        for _ in 0..2 {
            let (point, _actor_ref, _sender) = cast_info_rx2.recv().await.unwrap();
            all_ranks.insert(point.rank());
        }
        assert_eq!(all_ranks, HashSet::from([0, 1]));

        let _ = host_mesh.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_cast() {
        let config = hyperactor_config::global::lock();
        execute_cast(&config).await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_cast_p2p() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::comm::ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(
            hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER,
            true,
        );
        let _guard3 = config.override_key(crate::config::V1_CAST_POINT_TO_POINT_THRESHOLD, 1024);
        execute_cast(&config).await;
    }
    /// Test that undeliverable messages are properly returned to the
    /// sender when communication to a proc is broken.
    ///
    /// This is the V1 version of the test from
    /// hyperactor_multiprocess/src/proc_actor.rs::test_undeliverable_message_return.
    #[assert_no_process_leak]
    #[async_timed_test(timeout_secs = 60)]
    async fn test_undeliverable_message_return() {
        use hyperactor::mailbox::MessageEnvelope;
        use hyperactor::mailbox::Undeliverable;
        use hyperactor::testing::pingpong::PingPongActor;
        use hyperactor::testing::pingpong::PingPongMessage;

        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();

        // Create a proc mesh with 2 hosts.
        let (mut hm, proc_mesh) = {
            let config = hyperactor_config::global::lock();
            let _proc_spawn_guard =
                config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
            let _host_spawn_guard = config.override_key(
                hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
                Duration::from_secs(60),
            );
            let hm = testing::host_mesh(2).await;
            let proc_mesh = hm
                .spawn(instance, "test", Extent::unity(), None, None)
                .await
                .unwrap();
            (hm, proc_mesh)
        };

        // Set up undeliverable message port for collecting undeliverables
        let (undeliverable_port, mut undeliverable_rx) =
            instance.open_port::<Undeliverable<MessageEnvelope>>();

        // Spawn actors individually on each host by spawning separate actor meshes
        // with specific proc selections.
        let ping_proc_mesh = proc_mesh.range("hosts", 0..1).unwrap();
        let pong_proc_mesh = proc_mesh.range("hosts", 1..2).unwrap();

        let ping_mesh: ActorMesh<PingPongActor> = ping_proc_mesh
            .spawn(
                instance,
                "ping",
                &(Some(undeliverable_port.bind()), None, None),
            )
            .await
            .unwrap();

        let mut pong_mesh: ActorMesh<PingPongActor> = pong_proc_mesh
            .spawn(instance, "pong", &(None, None, None))
            .await
            .unwrap();

        // Get individual actor refs
        let ping_handle = ping_mesh.values().next().unwrap();
        let pong_handle = pong_mesh.values().next().unwrap();

        // Verify ping-pong works initially
        let (done_tx, done_rx) = instance.open_once_port();
        ping_handle.post(
            instance,
            PingPongMessage(2, pong_handle.clone(), done_tx.bind()),
        );
        assert!(
            done_rx.recv().await.unwrap(),
            "Initial ping-pong should work"
        );

        // Now stop the pong actor mesh to break communication
        pong_mesh
            .stop(instance, "test stop".to_string())
            .await
            .unwrap();

        // Give it a moment to fully stop
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Set message delivery timeout for faster test
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            std::time::Duration::from_secs(5),
        );

        // Send multiple messages that will all fail to be delivered
        let n = 100usize;
        for i in 1..=n {
            let ttl = 66 + i as u64; // Avoid ttl = 66 (which would cause other test behavior)
            let (once_tx, _once_rx) = instance.open_once_port();
            ping_handle.post(
                instance,
                PingPongMessage(ttl, pong_handle.clone(), once_tx.bind()),
            );
        }

        // Collect all undeliverable messages.
        // The fact that we successfully collect them proves the ping actor
        // is still running and handling undeliverables correctly (not crashing).
        let mut count = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while count < n && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_secs(1), undeliverable_rx.recv())
                .await
            {
                Ok(Ok(Undeliverable::Returned(envelope))) => {
                    let _: PingPongMessage = envelope.deserialized().unwrap();
                    count += 1;
                }
                Ok(Ok(Undeliverable::Report(_))) => break,
                Ok(Err(_)) => break, // Channel closed
                Err(_) => break,     // Timeout
            }
        }

        assert_eq!(
            count, n,
            "Expected {} undeliverable messages, got {}",
            n, count
        );

        let _ = hm.shutdown(instance).await;
    }

    /// Test that `stop()` returns bounded by `ACTOR_SPAWN_MAX_IDLE` even
    /// when actors are stuck inside a handler and never observe the
    /// `DrainAndStop` signal. The controller's `Stop` handler awaits
    /// the underlying ProcAgent wait, which waits up to `ACTOR_SPAWN_MAX_IDLE`
    /// for ProcAgents to report `Stopped`; when that idle window elapses it
    /// stamps `Status::Timeout` into the controller's health state, and the
    /// subsequent `GetState` reads that back. The actors' tokio tasks
    /// continue running in the background: no code path in the mesh layer
    /// forcibly aborts them via `JoinHandle::abort()`.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_actor_mesh_stop_timeout() {
        hyperactor_telemetry::initialize_logging_for_test();

        // `ACTOR_SPAWN_MAX_IDLE` bounds how long the controller's Stop
        // handler waits for ProcAgents to report `Stopped`. Shorten it
        // from 30s to 1s so the test finishes quickly.
        let config = hyperactor_config::global::lock();
        let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _host_spawn = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        let instance = testing::instance();

        // Create proc mesh with 2 procs
        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();

        // Spawn SleepActors across the mesh that will block longer
        // than timeout
        let mut sleep_mesh: ActorMesh<testactor::SleepActor> =
            proc_mesh.spawn(instance, "sleepers", &()).await.unwrap();
        let _guard = config.override_key(ACTOR_SPAWN_MAX_IDLE, std::time::Duration::from_secs(1));

        // Send each actor a message to sleep for 5 seconds. `Instance::run`
        // only polls the signal receiver at message boundaries, so
        // `DrainAndStop` will sit queued in the signal mailbox until this
        // handler completes. Nothing forcibly aborts it.
        for actor_ref in sleep_mesh.values() {
            actor_ref.post(instance, std::time::Duration::from_secs(5));
        }

        // Give actors time to start sleeping
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Count how many actors we spawned (for verification later)
        let expected_actors = sleep_mesh.values().count();

        // Now stop the mesh. The controller's Stop handler will give up on
        // waiting for `Stopped` after ACTOR_SPAWN_MAX_IDLE and mark the
        // ranks as `Status::Timeout`. Time this operation to confirm we
        // return on that budget rather than waiting the full 5s sleep.
        let stop_start = tokio::time::Instant::now();
        let result = sleep_mesh.stop(instance, "test stop".to_string()).await;
        let stop_duration = tokio::time::Instant::now().duration_since(stop_start);

        // `stop()` returns `Ok(())` because `is_terminating()` accepts
        // `Status::Timeout`. We still check the duration below to confirm
        // the timeout path (not a natural graceful stop) produced this.
        match result {
            Ok(_) => {
                tracing::info!(
                    "stop returned Ok for {} actors; their tokio tasks \
                     may still be running until their handler yields",
                    expected_actors
                );
            }
            Err(ref e) => {
                let err_str = format!("{:?}", e);
                assert!(
                    err_str.contains("Timeout"),
                    "Expected Timeout error, got: {:?}",
                    e
                );
            }
        }

        // Verify that stop returned on the ACTOR_SPAWN_MAX_IDLE budget
        // (~1s) rather than the full 5s sleep. This confirms we hit the
        // controller's idle timeout while querying for `Stopped` — not
        // that the actors were actually aborted; they weren't.
        assert!(
            stop_duration < std::time::Duration::from_millis(4500),
            "Stop took {:?}, expected < 4.5s (controller should have given up waiting for Stopped)",
            stop_duration
        );
        assert!(
            stop_duration >= std::time::Duration::from_millis(900),
            "Stop took {:?}, expected >= 900ms (should have waited for the 1s idle timeout)",
            stop_duration
        );

        let _ = hm.shutdown(instance).await;
    }

    /// Test that actors stop gracefully when they respond to stop
    /// signals within the timeout. Complementary to
    /// test_actor_mesh_stop_timeout which tests abort behavior. V1
    /// equivalent of
    /// hyperactor_multiprocess/src/proc_actor.rs::test_stop
    #[async_timed_test(timeout_secs = 60)]
    async fn test_actor_mesh_stop_graceful() {
        hyperactor_telemetry::initialize_logging_for_test();

        let config = hyperactor_config::global::lock();
        let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _host_spawn = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        let instance = testing::instance();

        // Create proc mesh with 2 procs
        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();

        // Spawn TestActors - these stop cleanly (no blocking
        // operations)
        let mut actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(instance, "test_actors", &()).await.unwrap();

        // Cloned mesh will still have its controller, even if the owned mesh
        // causes a stop.
        let mesh_ref = actor_mesh.deref().clone();

        let expected_actors = actor_mesh.values().count();
        assert!(expected_actors > 0, "Should have spawned some actors");

        // Time the stop operation
        let stop_start = tokio::time::Instant::now();
        let result = actor_mesh.stop(instance, "test stop".to_string()).await;
        let stop_duration = tokio::time::Instant::now().duration_since(stop_start);

        // Graceful stop should succeed (return Ok)
        assert!(
            result.is_ok(),
            "Stop should succeed for responsive actors, got: {:?}",
            result.err()
        );

        // Verify stop completed quickly (< 2 seconds). Responsive
        // actors should stop almost immediately, not wait for
        // timeout.
        assert!(
            stop_duration < std::time::Duration::from_secs(5),
            "Graceful stop took {:?}, expected < 5s (actors should stop quickly)",
            stop_duration
        );

        tracing::info!(
            "Successfully stopped {} actors in {:?}",
            expected_actors,
            stop_duration
        );

        // Check that the next returned supervision event is a Stopped event.
        // Note that Ref meshes get Stopped events, and Owned meshes do not,
        // because only the owner can stop them anyway.
        // Each owned mesh has an implicit ref mesh though, so that is what we
        // test here.
        let next_event = actor_mesh.next_supervision_event(instance).await.unwrap();
        assert_eq!(next_event.actor_mesh_name, Some(mesh_ref.id().to_string()));
        assert!(matches!(
            next_event.event.actor_status,
            ActorStatus::Stopped(_)
        ));
        // Check that a cloned Ref from earlier gets the same event. Every clone
        // should get the same event, even if it's not a subscriber.
        let next_event = mesh_ref.next_supervision_event(instance).await.unwrap();
        assert_eq!(next_event.actor_mesh_name, Some(mesh_ref.id().to_string()));
        assert!(matches!(
            next_event.event.actor_status,
            ActorStatus::Stopped(_)
        ));

        let _ = hm.shutdown(instance).await;
    }
}
