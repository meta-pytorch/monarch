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
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::mailbox::PortReceiver;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor::message::Unbound;
use hyperactor::reference as hyperactor_reference;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Attrs;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_mesh_macros::sel;
use ndslice::Selection;
use ndslice::ViewExt as _;
use ndslice::view;
use ndslice::view::Region;
use ndslice::view::View;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tokio::sync::watch;

use crate::CommActor;
use crate::Error;
use crate::Name;
use crate::ProcMeshRef;
use crate::ValueMesh;
use crate::casting;
use crate::comm::multicast;
use crate::host_mesh::GET_PROC_STATE_MAX_IDLE;
use crate::host_mesh::mesh_to_rankedvalues_with_default;
use crate::mesh_controller::ActorMeshController;
use crate::mesh_controller::SUPERVISION_POLL_FREQUENCY;
use crate::mesh_controller::Subscribe;
use crate::mesh_controller::Unsubscribe;
use crate::proc_agent::ActorState;
use crate::proc_mesh::GET_ACTOR_STATE_MAX_IDLE;
use crate::reference::ActorMeshId;
use crate::resource;
use crate::supervision::DEST_RANK;
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
/// `hyperactor_reference::ActorRef<A>`, which is only defined for `A: Referable`.
#[derive(Debug)]
pub struct ActorMesh<A: Referable> {
    proc_mesh: ProcMeshRef,
    name: Name,
    current_ref: ActorMeshRef<A>,
    /// If present, this is the controller for the mesh. The controller ensures
    /// the mesh is stopped when the actor owning it is stopped, and can provide
    /// supervision events via subscribing.
    /// It may not be present for some types of actors, typically system actors
    /// such as ProcAgent or CommActor.
    controller: Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
}

// `A: Referable` for the same reason as the struct: the mesh holds
// `hyperactor_reference::ActorRef<A>`.
impl<A: Referable> ActorMesh<A> {
    pub(crate) fn new_with_attribution(
        proc_mesh: ProcMeshRef,
        name: Name,
        controller: Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
        attribution: Attrs,
    ) -> Self {
        let current_ref = ActorMeshRef::with_page_size_and_attribution(
            name.clone(),
            proc_mesh.clone(),
            DEFAULT_PAGE,
            controller.clone(),
            attribution,
        );

        Self {
            proc_mesh,
            name,
            current_ref,
            controller,
        }
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) fn set_controller(
        &mut self,
        controller: Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
    ) {
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
            // Send a stop to the controller so it stops monitoring the actors.
            controller
                .send(
                    cx,
                    resource::Stop {
                        name: self.name.clone(),
                        reason,
                    },
                )
                .map_err(|e| {
                    crate::Error::SendingError(controller.actor_id().clone(), Box::new(e))
                })?;
            let region = ndslice::view::Ranked::region(&self.current_ref);
            let num_ranks = region.num_ranks();
            // Wait for the controller to report all actors have stopped.
            let (port, mut rx) = cx.mailbox().open_port();

            controller
                .send(
                    cx,
                    resource::GetState::<resource::mesh::State<()>> {
                        name: self.name.clone(),
                        reply: port.bind(),
                    },
                )
                .map_err(|e| {
                    crate::Error::SendingError(controller.actor_id().clone(), Box::new(e))
                })?;

            let statuses = rx.recv().await?;
            if let Some(state) = &statuses.state {
                // Check that all actors are in a terminating state (Stopping
                // or beyond). The actual wait for full cleanup (terminal)
                // happens in _drain_and_stop via the controller's status watch.
                let all_stopped = state.statuses.values().all(|s| s.is_terminating());
                if all_stopped {
                    Ok(())
                } else {
                    let legacy = mesh_to_rankedvalues_with_default(
                        &state.statuses,
                        resource::Status::NotExist,
                        resource::Status::is_not_exist,
                        num_ranks,
                    );
                    Err(Error::ActorStopError { statuses: legacy })
                }
            } else {
                Err(Error::Other(anyhow::anyhow!(
                    "non-existent state in GetState reply from controller: {}",
                    controller.actor_id()
                )))
            }?;
            // Update health state with the new statuses.
            let mut entry = self.health_state.entry(cx).or_default();
            let health_state = entry.get_mut();
            health_state.unhealthy_event = Some(Unhealthy::StreamClosed(MeshFailure {
                actor_mesh_name: Some(self.name().to_string()),
                event: ActorSupervisionEvent::new(
                    // Use an actor id from the mesh.
                    ndslice::view::Ranked::get(&self.current_ref, 0)
                        .unwrap()
                        .actor_id()
                        .clone(),
                    None,
                    ActorStatus::Stopped("mesh stopped".to_string()),
                    None,
                ),
                crashed_ranks: vec![],
            }));
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
            name: self.name.clone(),
            current_ref: self.current_ref.clone(),
            controller: self.controller.clone(),
        }
    }
}

impl<A: Referable> Drop for ActorMesh<A> {
    fn drop(&mut self) {
        tracing::info!(
            name = "ActorMeshStatus",
            actor_name = %self.name,
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
    slots: Box<[OnceCell<hyperactor_reference::ActorRef<A>>]>,
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
///
/// Carries transport-attribution state (`attribution: Attrs`) for
/// the destination actors in this mesh. Populated at spawn time
/// with `DEST_MESH_NAME` / `DEST_ACTOR_CLASS` / `DEST_ACTOR_DISPLAY_NAME`
/// and copied onto per-rank `ActorRef`s (with `DEST_RANK` added) by
/// `materialize`. Per AT-4, the attribution field does not
/// participate in identity or equality.
pub struct ActorMeshRef<A: Referable> {
    proc_mesh: ProcMeshRef,
    name: Name,
    /// Reference to a remote controller actor living on the proc that spawned
    /// the actors in this ref. If None, the actor mesh was already stopped, or
    /// this is a mesh ref to a "system actor" which has no controller and should
    /// not be stopped. If Some, the actor mesh may still be stopped, and the
    /// next_supervision_event function can be used to alert that the mesh has
    /// stopped.
    controller: Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
    /// Structured transport attribution for destination actors in
    /// this mesh. Stamped onto envelope `Flattrs` at cast and
    /// direct-send sites, and copied onto per-rank `ActorRef`s via
    /// `materialize`. See AT-1..AT-5 in `crate::supervision`.
    attribution: Attrs,

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
                hyperactor_reference::PortRef<Option<MeshFailure>>,
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
    /// - A `Page<A>` is a boxed slice of `OnceCell<hyperactor_reference::ActorRef<A>>`,
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
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        self.cast_with_selection(cx, sel!(*), message)
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
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        self.cast_with_selection(cx, sel, message)
    }

    #[allow(clippy::result_large_err)]
    fn cast_with_selection<M>(
        &self,
        cx: &impl context::Actor,
        sel: Selection,
        message: M,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        // First check if the mesh is already dead before sending out any messages
        // to a possibly undeliverable actor.
        if let Some(failure) = self.cached_failure(cx) {
            tracing::debug!(
                actor_mesh = %self.name,
                crashed_ranks = ?failure.crashed_ranks,
                "rejecting cast due to cached supervision failure"
            );
            return Err(crate::Error::Supervision(Box::new(failure)));
        }

        hyperactor_telemetry::notify_sent_message(hyperactor_telemetry::SentMessageEvent {
            timestamp: std::time::SystemTime::now(),
            sender_actor_id: hyperactor_telemetry::hash_to_u64(cx.mailbox().actor_id()),
            actor_mesh_id: hyperactor_telemetry::hash_to_u64(&self.name.to_string()),
            view_json: serde_json::to_string(view::Ranked::region(self)).unwrap_or_default(),
            shape_json: {
                let shape: ndslice::Shape = view::Ranked::region(self).into();
                serde_json::to_string(&shape).unwrap_or_default()
            },
        });

        // Now that we know these ranks are active, send out the actual messages.
        if let Some(root_comm_actor) = self.proc_mesh.root_comm_actor() {
            self.cast_v0(cx, message, sel, root_comm_actor)
        } else {
            for (point, actor) in self.iter() {
                let create_rank = point.rank();
                let mut headers = Flattrs::new();
                multicast::set_cast_info_on_headers(
                    &mut headers,
                    point,
                    cx.instance().self_id().clone(),
                );

                // Make sure that we re-bind ranks, as these may be used for
                // bootstrapping comm actors.
                let mut unbound = Unbound::try_from_message(message.clone())
                    .map_err(|e| Error::CastingError(self.name.clone(), e))?;
                unbound
                    .visit_mut::<resource::Rank>(|resource::Rank(rank)| {
                        *rank = Some(create_rank);
                        Ok(())
                    })
                    .map_err(|e| Error::CastingError(self.name.clone(), e))?;
                let rebound_message = unbound
                    .bind()
                    .map_err(|e| Error::CastingError(self.name.clone(), e))?;
                actor
                    .send_with_headers(cx, headers, rebound_message)
                    .map_err(|e| Error::SendingError(actor.actor_id().clone(), Box::new(e)))?;
            }
            Ok(())
        }
    }

    #[allow(clippy::result_large_err)]
    fn cast_v0<M>(
        &self,
        cx: &impl context::Actor,
        message: M,
        sel: Selection,
        root_comm_actor: &hyperactor_reference::ActorRef<CommActor>,
    ) -> crate::Result<()>
    where
        A: RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        let cast_mesh_shape = view::Ranked::region(self).into();
        let actor_mesh_id = ActorMeshId(self.name.clone());
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
                    &self.attribution,
                )
                .map_err(|e| Error::CastingError(self.name.clone(), e.into()))
            }
            None => casting::actor_mesh_cast::<A, M>(
                cx,
                actor_mesh_id,
                root_comm_actor,
                sel,
                &cast_mesh_shape,
                &cast_mesh_shape,
                message,
                &self.attribution,
            )
            .map_err(|e| Error::CastingError(self.name.clone(), e.into())),
        }
    }

    /// Query the state of all actors in this mesh.
    /// If keepalive is Some, use a message that indicates to the recipient
    /// that the owner of the mesh is still alive, along with the expiry time
    /// after which the actor should be considered orphaned. Else, use a normal
    /// state query.
    #[allow(clippy::result_large_err)]
    pub async fn actor_states(
        &self,
        cx: &impl context::Actor,
    ) -> crate::Result<ValueMesh<resource::State<ActorState>>> {
        self.actor_states_with_keepalive(cx, None).await
    }

    #[allow(clippy::result_large_err)]
    pub(crate) async fn actor_states_with_keepalive(
        &self,
        cx: &impl context::Actor,
        keepalive: Option<std::time::SystemTime>,
    ) -> crate::Result<ValueMesh<resource::State<ActorState>>> {
        self.proc_mesh
            .actor_states_with_keepalive(cx, self.name.clone(), keepalive)
            .await
    }

    pub(crate) fn new(
        name: Name,
        proc_mesh: ProcMeshRef,
        controller: Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
    ) -> Self {
        Self::with_page_size_and_attribution(
            name,
            proc_mesh,
            DEFAULT_PAGE,
            controller,
            Attrs::new(),
        )
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    /// Access the transport-attribution carrier on this mesh ref.
    /// Informational only; AT-4 (does not participate in identity).
    pub fn attribution(&self) -> &Attrs {
        &self.attribution
    }

    pub(crate) fn with_page_size_and_attribution(
        name: Name,
        proc_mesh: ProcMeshRef,
        page_size: usize,
        controller: Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
        attribution: Attrs,
    ) -> Self {
        Self {
            proc_mesh,
            name,
            controller,
            attribution,
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

    pub fn controller(&self) -> &Option<hyperactor_reference::ActorRef<ActorMeshController<A>>> {
        &self.controller
    }

    fn set_controller(
        &mut self,
        controller: Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
    ) {
        self.controller = controller;
    }

    fn ensure_pages(&self) -> &Vec<OnceCell<Box<Page<A>>>> {
        let n = self.len().div_ceil(self.page_size); // ⌈len / page_size⌉
        self.pages
            .get_or_init(|| (0..n).map(|_| OnceCell::new()).collect())
    }

    fn materialize(&self, rank: usize) -> Option<&hyperactor_reference::ActorRef<A>> {
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
            // AT-1 / AT-3: clone the mesh's destination attribution
            // onto the produced per-rank ref and add DEST_RANK. This
            // is the single write point for rank on the direct-send
            // path. Per AT-5, no lookup occurs here.
            let actor_id = proc_ref.actor_id(&self.name);
            let mut attribution = self.attribution.clone();
            attribution.set(DEST_RANK, rank as u64);
            hyperactor_reference::ActorRef::attest_with_attrs(actor_id, attribution)
        }))
    }

    fn init_supervision_receiver(
        controller: &hyperactor_reference::ActorRef<ActorMeshController<A>>,
        cx: &impl context::Actor,
    ) -> (
        hyperactor_reference::PortRef<Option<MeshFailure>>,
        watch::Receiver<MessageOrFailure<Option<MeshFailure>>>,
    ) {
        let (tx, rx) = cx.mailbox().open_port();
        let tx = tx.bind();
        controller
            .send(cx, Subscribe(tx.clone()))
            .expect("failed to send Subscribe");
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
                actor_mesh = %self.name,
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
                let _ = port.send(cx, Unsubscribe(subscriber_port));
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
                    //
                    // Synthesis-site attachment: project the mesh's
                    // transport attribution onto the synthesized event's
                    // structured-attribution carrier.
                    let attribution = crate::supervision::attribution_from_attrs(&self.attribution);
                    Ok(MeshFailure {
                        actor_mesh_name: Some(self.name().to_string()),
                        event: ActorSupervisionEvent::new(
                            controller.actor_id().clone(),
                            None,
                            ActorStatus::generic_failure(format!(
                                "timed out reaching controller {} for mesh {}. Assuming controller's proc is dead",
                                controller.actor_id(),
                                self.name()
                            )),
                            attribution,
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
            name: self.name.clone(),
            controller: self.controller.clone(),
            attribution: self.attribution.clone(),
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
            name: self.name.clone(),
            controller: self.controller.clone(),
            attribution: self.attribution.clone(),
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
        write!(f, "{}:{}@{}", self.name, A::typename(), self.proc_mesh)
    }
}

impl<A: Referable> PartialEq for ActorMeshRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.proc_mesh == other.proc_mesh && self.name == other.name
    }
}
impl<A: Referable> Eq for ActorMeshRef<A> {}

impl<A: Referable> Hash for ActorMeshRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.proc_mesh.hash(state);
        self.name.hash(state);
    }
}

impl<A: Referable> fmt::Debug for ActorMeshRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorMeshRef")
            .field("proc_mesh", &self.proc_mesh)
            .field("name", &self.name)
            .field("page_size", &self.page_size)
            .finish_non_exhaustive() // No print cache.
    }
}

// Implement Serialize manually, without requiring A: Serialize.
//
// Wire format: `(proc_mesh, name, controller, attribution)`. Per
// AT-1, the attribution carrier round-trips across the wire under
// the same declared keys it carries in-memory.
impl<A: Referable> Serialize for ActorMeshRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (
            &self.proc_mesh,
            &self.name,
            &self.controller,
            &self.attribution,
        )
            .serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize.
// See `Serialize` above for the tuple wire format.
impl<'de, A: Referable> Deserialize<'de> for ActorMeshRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (proc_mesh, name, controller, attribution) = <(
            ProcMeshRef,
            Name,
            Option<hyperactor_reference::ActorRef<ActorMeshController<A>>>,
            Attrs,
        )>::deserialize(deserializer)?;
        Ok(ActorMeshRef::with_page_size_and_attribution(
            name,
            proc_mesh,
            DEFAULT_PAGE,
            controller,
            attribution,
        ))
    }
}

impl<A: Referable> view::Ranked for ActorMeshRef<A> {
    type Item = hyperactor_reference::ActorRef<A>;

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
            name: self.name.clone(),
            controller: self.controller.clone(),
            attribution: self.attribution.clone(),
            health_state: self.health_state.clone(),
            receiver: ActorLocal::new(),
            pages: OnceCell::new(),
            page_size: self.page_size,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;
    use std::ops::Deref;

    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::context::Mailbox as _;
    use hyperactor::mailbox;
    use hyperactor_config::Attrs;
    use ndslice::Extent;
    use ndslice::ViewExt;
    use ndslice::extent;
    use ndslice::view::Ranked;
    use timed_test::async_timed_test;
    use tokio::time::Duration;

    use super::ActorMesh;
    use crate::ActorMeshRef;
    use crate::Name;
    use crate::ProcMesh;
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
    #[cfg(fbcode_build)]
    async fn test_actor_mesh_ref_lazy_materialization() {
        // 1) Bring up procs and spawn actors.
        let instance = testing::instance();
        // Small mesh so the test runs fast, but > page_size so we
        // cross a boundary
        let mut hm = testing::host_mesh(3).await;
        let pm: ProcMesh = hm
            .spawn(instance, "test", extent!(gpus = 2), None)
            .await
            .unwrap();
        let am: ActorMesh<testactor::TestActor> = pm.spawn(instance, "test", &()).await.unwrap();

        // 2) Build our ActorMeshRef with a tiny page size (2) to
        // force multiple pages:
        // page 0: ranks [0,1], page 1: [2,3], page 2: [4,5]
        let page_size = 2;
        let amr: ActorMeshRef<testactor::TestActor> = ActorMeshRef::with_page_size_and_attribution(
            am.name.clone(),
            pm.clone(),
            page_size,
            None,
            Attrs::new(),
        );
        assert_eq!(amr.extent(), extent!(hosts = 3, gpus = 2));
        assert_eq!(amr.region().num_ranks(), 6);

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
        let orig_id_0 = amr.get(0).unwrap().actor_id().clone();
        let clone_id_0 = amr_clone.get(0).unwrap().actor_id().clone();
        assert_eq!(orig_id_0, clone_id_0, "clone preserves identity");
        let p0_clone = amr_clone.get(0).unwrap() as *const _;
        assert_ne!(
            p0_a, p0_clone,
            "cloned ActorMeshRef has a fresh cache (different pointer)"
        );

        // 7) Slicing preserves page_size and clears cache
        // (RankedSliceable::sliced)
        let sliced = amr.range("hosts", 1..).expect("slice should be valid"); // leaves 4 ranks
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
            .send(instance, testactor::GetActorId(port.bind()))
            .expect("send to rank 0 should succeed");
        amr.get(3)
            .expect("rank 3 exists")
            .send(instance, testactor::GetActorId(port.bind()))
            .expect("send to rank 3 should succeed");
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

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_states_with_panic() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let mut hm = testing::host_mesh(num_replicas).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None)
            .await
            .unwrap();
        let child_name = Name::new("child").unwrap();

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
            assert_eq!(
                failure.event.actor_id.name(),
                child_name.clone().to_string()
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

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_states_with_process_exit() {
        hyperactor_telemetry::initialize_logging_for_test();

        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(GET_ACTOR_STATE_MAX_IDLE, Duration::from_secs(1));

        let instance = testing::instance();
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let mut hm = testing::host_mesh(num_replicas).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None)
            .await
            .unwrap();
        let mut second_hm = testing::host_mesh(num_replicas).await;
        let second_proc_mesh = second_hm
            .spawn(instance, "test2", Extent::unity(), None)
            .await
            .unwrap();
        let child_name = Name::new("child").unwrap();

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
            assert_eq!(failure.event.actor_id.name(), child_name.to_string());
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

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_states_on_sliced_mesh() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let mut hm = testing::host_mesh(num_replicas).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None)
            .await
            .unwrap();
        let child_name = Name::new("child").unwrap();

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
            .range("hosts", 1..3)
            .expect("slice should be valid");
        let sliced_replicas = sliced.len();

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
            assert_eq!(event.actor_id.name(), format!("{}", child_name.clone()));
            if let ActorStatus::Failed(ActorErrorKind::Generic(msg)) = &event.actor_status {
                assert!(msg.contains("panic"));
                assert!(msg.contains("for testing"));
            } else {
                panic!("actor status is not failed: {}", event.actor_status);
            }
        }

        let _ = hm.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_cast() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);

        let instance = testing::instance();
        let mut host_mesh = testing::host_mesh(4).await;
        let proc_mesh = host_mesh
            .spawn(instance, "test", Extent::unity(), None)
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
            assert_eq!(&sender_actor_id, instance.self_id());
        }

        let _ = host_mesh.shutdown(instance).await;
    }

    /// Test that undeliverable messages are properly returned to the
    /// sender when communication to a proc is broken.
    ///
    /// This is the V1 version of the test from
    /// hyperactor_multiprocess/src/proc_actor.rs::test_undeliverable_message_return.
    #[async_timed_test(timeout_secs = 60)]
    #[cfg(fbcode_build)]
    async fn test_undeliverable_message_return() {
        use hyperactor::mailbox::MessageEnvelope;
        use hyperactor::mailbox::Undeliverable;
        use hyperactor::testing::pingpong::PingPongActor;
        use hyperactor::testing::pingpong::PingPongMessage;

        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();

        // Create a proc mesh with 2 hosts.
        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None)
            .await
            .unwrap();

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
        ping_handle
            .send(
                instance,
                PingPongMessage(2, pong_handle.clone(), done_tx.bind()),
            )
            .unwrap();
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
            ping_handle
                .send(
                    instance,
                    PingPongMessage(ttl, pong_handle.clone(), once_tx.bind()),
                )
                .unwrap();
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
                Ok(Ok(Undeliverable(envelope))) => {
                    let _: PingPongMessage = envelope.deserialized().unwrap();
                    count += 1;
                }
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

    /// Test that actors not responding within stop timeout are
    /// forcibly aborted. This is the V1 equivalent of
    /// hyperactor_multiprocess/src/proc_actor.rs::test_stop_timeout.
    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_mesh_stop_timeout() {
        hyperactor_telemetry::initialize_logging_for_test();

        // Override ACTOR_SPAWN_MAX_IDLE to make test fast and
        // deterministic. ACTOR_SPAWN_MAX_IDLE is the maximum idle
        // time between status updates during mesh operations
        // (spawn/stop). When stop() is called, it waits for actors to
        // report they've stopped. If actors don't respond within this
        // timeout, they're forcibly aborted via JoinHandle::abort().
        // We set this to 1 second (instead of default 30s) so hung
        // actors (sleeping 5s in this test) get aborted quickly,
        // making the test fast.
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ACTOR_SPAWN_MAX_IDLE, std::time::Duration::from_secs(1));

        let instance = testing::instance();

        // Create proc mesh with 2 procs
        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None)
            .await
            .unwrap();

        // Spawn SleepActors across the mesh that will block longer
        // than timeout
        let mut sleep_mesh: ActorMesh<testactor::SleepActor> =
            proc_mesh.spawn(instance, "sleepers", &()).await.unwrap();

        // Send each actor a message to sleep for 5 seconds (longer
        // than 1-second timeout)
        for actor_ref in sleep_mesh.values() {
            actor_ref
                .send(instance, std::time::Duration::from_secs(5))
                .unwrap();
        }

        // Give actors time to start sleeping
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Count how many actors we spawned (for verification later)
        let expected_actors = sleep_mesh.values().count();

        // Now stop the mesh - actors won't respond in time, should be
        // aborted. Time this operation to verify abort behavior.
        let stop_start = tokio::time::Instant::now();
        let result = sleep_mesh.stop(instance, "test stop".to_string()).await;
        let stop_duration = tokio::time::Instant::now().duration_since(stop_start);

        // Stop will return an error because actors didn't stop within
        // the timeout. This is expected - the actors were forcibly
        // aborted, and V1 reports this as an error.
        match result {
            Ok(_) => {
                // It's possible actors stopped in time, but unlikely
                // given 5-second sleep vs 1-second timeout
                tracing::warn!("Actors stopped gracefully (unexpected but ok)");
            }
            Err(ref e) => {
                // Expected: timeout error indicating actors were aborted
                let err_str = format!("{:?}", e);
                assert!(
                    err_str.contains("Timeout"),
                    "Expected Timeout error, got: {:?}",
                    e
                );
                tracing::info!(
                    "Stop timed out as expected for {} actors, they were aborted",
                    expected_actors
                );
            }
        }

        // Verify that stop completed quickly (~1-2 seconds for
        // timeout + abort) rather than waiting the full 5 seconds for
        // actors to finish sleeping. This proves actors were aborted,
        // not waited for.
        assert!(
            stop_duration < std::time::Duration::from_secs(3),
            "Stop took {:?}, expected < 3s (actors should have been aborted, not waited for)",
            stop_duration
        );
        assert!(
            stop_duration >= std::time::Duration::from_millis(900),
            "Stop took {:?}, expected >= 900ms (should have waited for timeout)",
            stop_duration
        );

        let _ = hm.shutdown(instance).await;
    }

    /// Test that actors stop gracefully when they respond to stop
    /// signals within the timeout. Complementary to
    /// test_actor_mesh_stop_timeout which tests abort behavior. V1
    /// equivalent of
    /// hyperactor_multiprocess/src/proc_actor.rs::test_stop
    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_mesh_stop_graceful() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();

        // Create proc mesh with 2 procs
        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(instance, "test", Extent::unity(), None)
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
            stop_duration < std::time::Duration::from_secs(2),
            "Graceful stop took {:?}, expected < 2s (actors should stop quickly)",
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
        assert_eq!(
            next_event.actor_mesh_name,
            Some(mesh_ref.name().to_string())
        );
        assert!(matches!(
            next_event.event.actor_status,
            ActorStatus::Stopped(_)
        ));
        // Check that a cloned Ref from earlier gets the same event. Every clone
        // should get the same event, even if it's not a subscriber.
        let next_event = mesh_ref.next_supervision_event(instance).await.unwrap();
        assert_eq!(
            next_event.actor_mesh_name,
            Some(mesh_ref.name().to_string())
        );
        assert!(matches!(
            next_event.event.actor_status,
            ActorStatus::Stopped(_)
        ));

        let _ = hm.shutdown(instance).await;
    }

    struct AttrTestActor;
    impl hyperactor::actor::Referable for AttrTestActor {}
    impl typeuri::Named for AttrTestActor {
        fn typename() -> &'static str {
            "hyperactor_mesh::actor_mesh::tests::AttrTestActor"
        }
    }

    fn singleton_proc_mesh() -> crate::proc_mesh::ProcMeshRef {
        use hyperactor::channel::ChannelAddr;
        use hyperactor::reference as hyperactor_reference;
        let proc_id =
            hyperactor_reference::ProcId::with_name(ChannelAddr::Local(0), "attr_test_proc");
        let agent_id = proc_id.actor_id("proc_agent", 0);
        let agent: hyperactor_reference::ActorRef<crate::proc_agent::ProcAgent> =
            hyperactor_reference::ActorRef::attest(agent_id);
        let proc_ref = crate::proc_mesh::ProcRef::new(proc_id, 0, agent);
        crate::proc_mesh::ProcMeshRef::new_singleton(
            Name::new("attr_test_mesh").expect("valid name"),
            proc_ref,
        )
    }

    // Neutral tokens: these tests are preservation/round-trip
    // tests, not rendering tests, so the values only need to be
    // unique and observable round-trip.
    const MESH_NAME_TOKEN: &str = "MESH_NAME";
    const ACTOR_CLASS_TOKEN: &str = "ACTOR_CLASS";
    const ACTOR_DISPLAY_NAME_TOKEN: &str = "DISPLAY_NAME";

    fn populated_attribution() -> hyperactor_config::Attrs {
        use crate::supervision::DEST_ACTOR_CLASS;
        use crate::supervision::DEST_ACTOR_DISPLAY_NAME;
        use crate::supervision::DEST_MESH_NAME;
        let mut attrs = hyperactor_config::Attrs::new();
        attrs.set(DEST_MESH_NAME, MESH_NAME_TOKEN.to_string());
        attrs.set(DEST_ACTOR_CLASS, ACTOR_CLASS_TOKEN.to_string());
        attrs.set(
            DEST_ACTOR_DISPLAY_NAME,
            ACTOR_DISPLAY_NAME_TOKEN.to_string(),
        );
        attrs
    }

    // AT-1: `ActorMeshRef::get(rank)` is the sole write point for
    // per-rank `DEST_RANK` on the direct-send path. Mesh-level
    // attribution keys copy through to the produced `ActorRef`
    // verbatim. AT-5: no lookup occurs at materialization.
    #[test]
    fn mesh_get_rank_sets_dest_rank_and_preserves_mesh_attrs() {
        use crate::supervision::DEST_ACTOR_CLASS;
        use crate::supervision::DEST_ACTOR_DISPLAY_NAME;
        use crate::supervision::DEST_MESH_NAME;
        use crate::supervision::DEST_RANK;
        let mesh_ref: ActorMeshRef<AttrTestActor> = ActorMeshRef::with_page_size_and_attribution(
            Name::new("test_actor").unwrap(),
            singleton_proc_mesh(),
            super::DEFAULT_PAGE,
            None,
            populated_attribution(),
        );

        let rank_ref = Ranked::get(&mesh_ref, 0).expect("rank 0 exists");

        assert_eq!(rank_ref.attribution().get(DEST_RANK), Some(&0u64));
        assert_eq!(
            rank_ref.attribution().get(DEST_MESH_NAME),
            Some(&MESH_NAME_TOKEN.to_string()),
        );
        assert_eq!(
            rank_ref.attribution().get(DEST_ACTOR_CLASS),
            Some(&ACTOR_CLASS_TOKEN.to_string()),
        );
        assert_eq!(
            rank_ref.attribution().get(DEST_ACTOR_DISPLAY_NAME),
            Some(&ACTOR_DISPLAY_NAME_TOKEN.to_string()),
        );
    }

    // AT-3: `DEST_RANK` describes the produced per-rank ref, not
    // the mesh; materialization must not write it back onto the
    // mesh's own attribution carrier.
    #[test]
    fn mesh_get_rank_does_not_mutate_mesh_attribution() {
        use crate::supervision::DEST_RANK;
        let mesh_ref: ActorMeshRef<AttrTestActor> = ActorMeshRef::with_page_size_and_attribution(
            Name::new("test_actor").unwrap(),
            singleton_proc_mesh(),
            super::DEFAULT_PAGE,
            None,
            populated_attribution(),
        );
        assert_eq!(mesh_ref.attribution().get(DEST_RANK), None);
        let _ = Ranked::get(&mesh_ref, 0).expect("rank 0 exists");
        assert_eq!(mesh_ref.attribution().get(DEST_RANK), None);
    }

    // AT-1: `Clone` on `ActorMeshRef` carries the attribution
    // carrier forward unchanged, so slices / clone-with-receiver
    // produce refs that stamp the same keys.
    #[test]
    fn mesh_ref_clone_preserves_attribution() {
        use crate::supervision::DEST_ACTOR_CLASS;
        use crate::supervision::DEST_MESH_NAME;
        let mesh_ref: ActorMeshRef<AttrTestActor> = ActorMeshRef::with_page_size_and_attribution(
            Name::new("test_actor").unwrap(),
            singleton_proc_mesh(),
            super::DEFAULT_PAGE,
            None,
            populated_attribution(),
        );
        let cloned = mesh_ref.clone();
        assert_eq!(
            cloned.attribution().get(DEST_MESH_NAME),
            Some(&MESH_NAME_TOKEN.to_string()),
        );
        assert_eq!(
            cloned.attribution().get(DEST_ACTOR_CLASS),
            Some(&ACTOR_CLASS_TOKEN.to_string()),
        );
    }

    // AT-1: the attribution carrier survives bincode round-trip,
    // and (composed with the rank-materialize write) the restored
    // mesh still produces per-rank refs with `DEST_RANK` and the
    // mesh-level keys populated.
    #[test]
    fn mesh_ref_wire_round_trip_preserves_attribution() {
        use crate::supervision::DEST_ACTOR_CLASS;
        use crate::supervision::DEST_ACTOR_DISPLAY_NAME;
        use crate::supervision::DEST_MESH_NAME;
        use crate::supervision::DEST_RANK;
        let original: ActorMeshRef<AttrTestActor> = ActorMeshRef::with_page_size_and_attribution(
            Name::new("test_actor").unwrap(),
            singleton_proc_mesh(),
            super::DEFAULT_PAGE,
            None,
            populated_attribution(),
        );

        let bytes = bincode::serde::encode_to_vec(&original, bincode::config::legacy())
            .expect("serialize ActorMeshRef");
        let restored: ActorMeshRef<AttrTestActor> =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy())
                .map(|(v, _)| v)
                .expect("deserialize ActorMeshRef");

        assert_eq!(
            restored.attribution().get(DEST_MESH_NAME),
            Some(&MESH_NAME_TOKEN.to_string()),
        );
        assert_eq!(
            restored.attribution().get(DEST_ACTOR_CLASS),
            Some(&ACTOR_CLASS_TOKEN.to_string()),
        );
        assert_eq!(
            restored.attribution().get(DEST_ACTOR_DISPLAY_NAME),
            Some(&ACTOR_DISPLAY_NAME_TOKEN.to_string()),
        );
        let r0 = Ranked::get(&restored, 0).expect("rank 0");
        assert_eq!(r0.attribution().get(DEST_RANK), Some(&0u64));
        assert_eq!(
            r0.attribution().get(DEST_MESH_NAME),
            Some(&MESH_NAME_TOKEN.to_string()),
        );
    }

    // AT-4: attribution is informational carrier state, not ref
    // identity. Two meshes with the same (name, proc_mesh) but
    // different attribution compare equal and hash equal.
    #[test]
    fn mesh_ref_identity_ignores_attribution() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash as _;
        use std::hash::Hasher as _;

        let proc_mesh = singleton_proc_mesh();
        let name = Name::new("test_actor").unwrap();
        let bare: ActorMeshRef<AttrTestActor> = ActorMeshRef::with_page_size_and_attribution(
            name.clone(),
            proc_mesh.clone(),
            super::DEFAULT_PAGE,
            None,
            hyperactor_config::Attrs::new(),
        );
        let decorated: ActorMeshRef<AttrTestActor> = ActorMeshRef::with_page_size_and_attribution(
            name,
            proc_mesh,
            super::DEFAULT_PAGE,
            None,
            populated_attribution(),
        );

        assert_eq!(bare, decorated);
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        bare.hash(&mut h1);
        decorated.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }
}
