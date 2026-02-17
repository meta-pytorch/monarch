/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

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
use hyperactor::PortRef;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::config;
use hyperactor::context;
use hyperactor::mailbox::PortReceiver;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor::message::Unbound;
use hyperactor::ordering::SeqInfo;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_mesh_macros::sel;
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
use crate::actor_mesh as v0_actor_mesh;
use crate::actor_mesh::CAST_ACTOR_MESH_ID;
use crate::comm::ENABLE_NATIVE_V1_CASTING;
use crate::comm::multicast;
use crate::comm::multicast::CastMessageV1;
use crate::metrics;
use crate::proc_mesh::mesh_agent::ActorState;
use crate::reference::ActorMeshId;
use crate::resource;
use crate::supervision::MeshFailure;
use crate::supervision::Unhealthy;
use crate::v1;
use crate::v1::Error;
use crate::v1::Name;
use crate::v1::ProcMeshRef;
use crate::v1::ValueMesh;
use crate::v1::host_mesh::GET_PROC_STATE_MAX_IDLE;
use crate::v1::host_mesh::mesh_to_rankedvalues_with_default;
use crate::v1::mesh_controller::ActorMeshController;
use crate::v1::mesh_controller::SUPERVISION_POLL_FREQUENCY;
use crate::v1::mesh_controller::Subscribe;
use crate::v1::mesh_controller::Unsubscribe;
use crate::v1::proc_mesh::GET_ACTOR_STATE_MAX_IDLE;

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
    name: Name,
    current_ref: ActorMeshRef<A>,
    /// If present, this is the controller for the mesh. The controller ensures
    /// the mesh is stopped when the actor owning it is stopped, and can provide
    /// supervision events via subscribing.
    /// It may not be present for some types of actors, typically system actors
    /// such as ProcMeshAgent or CommActor.
    controller: Option<ActorRef<ActorMeshController<A>>>,
}

// `A: Referable` for the same reason as the struct: the mesh holds
// `ActorRef<A>`.
impl<A: Referable> ActorMesh<A> {
    pub(crate) fn new(
        proc_mesh: ProcMeshRef,
        name: Name,
        controller: Option<ActorRef<ActorMeshController<A>>>,
    ) -> Self {
        let current_ref = ActorMeshRef::with_page_size(
            name.clone(),
            proc_mesh.clone(),
            DEFAULT_PAGE,
            controller.clone(),
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

    /// Detach this mesh from the lifetime of `self`, and return its reference.
    pub(crate) fn detach(self) -> ActorMeshRef<A> {
        self.current_ref.clone()
    }

    pub(crate) fn set_controller(&mut self, controller: Option<ActorRef<ActorMeshController<A>>>) {
        self.controller = controller.clone();
        self.current_ref.set_controller(controller);
    }

    /// Stop actors on this mesh across all procs.
    pub async fn stop(&mut self, cx: &impl context::Actor, reason: String) -> v1::Result<()> {
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
                .map_err(|e| v1::Error::SendingError(controller.actor_id().clone(), Box::new(e)))?;
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
                .map_err(|e| v1::Error::SendingError(controller.actor_id().clone(), Box::new(e)))?;

            let statuses = rx.recv().await?;
            if let Some(state) = &statuses.state {
                // Check that all actors are in some terminal state.
                // Failed is ok, because one of these actors may have failed earlier
                // and we're trying to stop the others.
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
                rank: None,
                event: ActorSupervisionEvent::new(
                    // Use an actor id from the mesh.
                    ndslice::view::Ranked::get(&self.current_ref, 0)
                        .unwrap()
                        .actor_id()
                        .clone(),
                    None,
                    ActorStatus::Stopped("actor mesh explicitly stopped".to_string()),
                    None,
                ),
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
            let message = match RealClock.timeout(timeout, rx.recv()).await {
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
pub struct ActorMeshRef<A: Referable> {
    proc_mesh: ProcMeshRef,
    name: Name,
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
    /// Cast a message to all the actors in this mesh
    #[allow(clippy::result_large_err)]
    pub fn cast<M>(&self, cx: &impl context::Actor, message: M) -> v1::Result<()>
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
    ) -> v1::Result<()>
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
    ) -> v1::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        // First check if the mesh is already dead before sending out any messages
        // to a possibly undeliverable actor.
        {
            let health_state = self.health_state.entry(cx).or_default();
            let health_state = health_state.get();
            match &health_state.unhealthy_event {
                Some(Unhealthy::StreamClosed(failure)) => {
                    return Err(v1::Error::Supervision(Box::new(failure.clone())));
                }
                Some(Unhealthy::Crashed(failure)) => {
                    return Err(v1::Error::Supervision(Box::new(failure.clone())));
                }
                None => {
                    // If crashed ranks has any entries, then unhealthy_event should be set.
                    // This is because all slices get a distinct health state.
                    assert!(health_state.crashed_ranks.is_empty());
                }
            }
        }

        // Now that we know these ranks are active, send out the actual messages.
        if let Some(root_comm_actor) = self.proc_mesh.root_comm_actor() {
            if hyperactor_config::global::get(ENABLE_NATIVE_V1_CASTING) {
                assert!(
                    hyperactor_config::global::get(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER),
                    "native V1 casting requires ENABLE_DEST_ACTOR_REORDERING_BUFFER to be enabled",
                );
                self.cast_v1(cx, message, root_comm_actor);
                Ok(())
            } else {
                self.cast_v0(cx, message, sel, root_comm_actor)
            }
        } else {
            for (point, actor) in self.iter() {
                let create_rank = point.rank();
                let mut headers = Flattrs::new();
                headers.set(
                    multicast::CAST_ORIGINATING_SENDER,
                    cx.instance().self_id().clone(),
                );
                headers.set(multicast::CAST_POINT, point);

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
        root_comm_actor: &ActorRef<CommActor>,
    ) -> v1::Result<()>
    where
        A: RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        let cast_mesh_shape = view::Ranked::region(self).into();
        let actor_mesh_id = ActorMeshId(self.name.clone());
        match &self.proc_mesh.root_region {
            Some(root_region) => {
                let root_mesh_shape = root_region.into();
                v0_actor_mesh::cast_to_sliced_mesh::<A, M>(
                    cx,
                    actor_mesh_id,
                    root_comm_actor,
                    &sel,
                    message,
                    &cast_mesh_shape,
                    &root_mesh_shape,
                )
                .map_err(|e| Error::CastingError(self.name.clone(), e.into()))
            }
            None => v0_actor_mesh::actor_mesh_cast::<A, M>(
                cx,
                actor_mesh_id,
                root_comm_actor,
                sel,
                &cast_mesh_shape,
                &cast_mesh_shape,
                message,
            )
            .map_err(|e| Error::CastingError(self.name.clone(), e.into())),
        }
    }

    fn cast_v1<M>(
        &self,
        cx: &impl context::Actor,
        message: M,
        root_comm_actor: &ActorRef<CommActor>,
    ) where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage,
    {
        let _ = metrics::ACTOR_MESH_CAST_DURATION.start(hyperactor::kv_pairs!(
            "message_type" => M::typename(),
            "message_variant" => message.arm().unwrap_or_default(),
        ));

        let actor_ids: ValueMesh<_> = self.proc_mesh.map_into(|proc| proc.actor_id(&self.name));
        // This block is infallible so is okay to assign the sequence numbers
        // without worrying about rollback.
        {
            let sequencer = cx.instance().sequencer();
            let seqs = actor_ids.map_into(|actor_id| {
                match sequencer.assign_seq(&actor_id.port_id(M::port())) {
                    SeqInfo::Session { seq, session_id: _ } => seq,
                    _ => panic!("infallible because assign_seq always returns session"),
                }
            });

            let mut headers = Flattrs::new();
            headers.set(
                multicast::CAST_ORIGINATING_SENDER,
                cx.instance().self_id().clone(),
            );
            // Set CAST_ACTOR_MESH_ID temporarily to support supervision's
            // v0 transition. Should be removed once supervision is migrated
            // and ActorMeshId is deleted.
            let actor_mesh_id = ActorMeshId(self.name.clone());
            headers.set(CAST_ACTOR_MESH_ID, actor_mesh_id);
            let cast_message = CastMessageV1::new::<A, M>(
                cx.instance().self_id().clone(),
                &self.name,
                view::Ranked::region(self).clone(),
                headers.clone(),
                message,
                sequencer.session_id(),
                seqs,
            )
            .expect("infallible because CastMessage should not fail for serialization");

            // TODO: load balancing instead of always using the first comm actor
            root_comm_actor
                .send_with_headers(cx, headers, cast_message)
                .expect("infallible because CastMessage should not fail for serialization");
        }
    }

    #[allow(clippy::result_large_err)]
    pub async fn actor_states(
        &self,
        cx: &impl context::Actor,
    ) -> v1::Result<ValueMesh<resource::State<ActorState>>> {
        self.proc_mesh.actor_states(cx, self.name.clone()).await
    }

    pub(crate) fn new(
        name: Name,
        proc_mesh: ProcMeshRef,
        controller: Option<ActorRef<ActorMeshController<A>>>,
    ) -> Self {
        Self::with_page_size(name, proc_mesh, DEFAULT_PAGE, controller)
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) fn with_page_size(
        name: Name,
        proc_mesh: ProcMeshRef,
        page_size: usize,
        controller: Option<ActorRef<ActorMeshController<A>>>,
    ) -> Self {
        Self {
            proc_mesh,
            name,
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
            // Invariant: `proc_mesh` and this view share the same
            // dense rank space:
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
            proc_ref.attest(&self.name)
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
        let controller = if let Some(c) = self.controller() {
            c
        } else {
            let health_state = self.health_state.entry(cx).or_default();
            let health_state = health_state.get();
            return match &health_state.unhealthy_event {
                Some(Unhealthy::StreamClosed(f)) => Ok(f.clone()),
                Some(Unhealthy::Crashed(f)) => Ok(f.clone()),
                None => Err(anyhow::anyhow!(
                    "unexpected healthy state while controller is gone"
                )),
            };
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
                            if let Some(rank) = &message.rank {
                                ndslice::view::Ranked::region(self).slice().contains(*rank)
                            } else {
                                // If rank is None, it applies to the whole mesh.
                                true
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
                    Ok(MeshFailure {
                        actor_mesh_name: Some(self.name().to_string()),
                        rank: None,
                        event: ActorSupervisionEvent::new(
                            controller.actor_id().clone(),
                            None,
                            ActorStatus::generic_failure(format!(
                                "timed out reaching controller {} for mesh {}. Assuming controller's proc is dead",
                                controller.actor_id(),
                                self.name()
                            )),
                            None,
                        ),
                    })
                }
            }?
        };
        // Update the health state now that we have received a message.
        let rank = message.rank.unwrap_or_default();
        let event = &message.event;
        // Make sure not to hold this lock across an await point.
        let mut entry = self.health_state.entry(cx).or_default();
        let health_state = entry.get_mut();
        if let ActorStatus::Failed(_) = event.actor_status {
            health_state.crashed_ranks.insert(rank, event.clone());
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

// Implement Serialize manually, without requiring A: Serialize
impl<A: Referable> Serialize for ActorMeshRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize only the fields that don't depend on A
        (&self.proc_mesh, &self.name, &self.controller).serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize
impl<'de, A: Referable> Deserialize<'de> for ActorMeshRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (proc_mesh, name, controller) =
            <(ProcMeshRef, Name, Option<ActorRef<ActorMeshController<A>>>)>::deserialize(
                deserializer,
            )?;
        Ok(ActorMeshRef::with_page_size(
            name,
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
        // The sliced ref will not share the same health state or receiver.
        // TODO: share to reduce open ports and tasks?
        debug_assert!(region.is_subset(view::Ranked::region(self)));
        let proc_mesh = self.proc_mesh.subset(region).unwrap();
        Self::with_page_size(
            self.name.clone(),
            proc_mesh,
            self.page_size,
            self.controller.clone(),
        )
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;
    use std::ops::Deref;

    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::context::Mailbox as _;
    use hyperactor::mailbox;
    use ndslice::Extent;
    use ndslice::ViewExt;
    use ndslice::extent;
    use ndslice::view::Ranked;
    use timed_test::async_timed_test;
    use tokio::time::Duration;

    use super::ActorMesh;
    use crate::supervision::MeshFailure;
    use crate::v1::ActorMeshRef;
    use crate::v1::Name;
    use crate::v1::ProcMesh;
    use crate::v1::host_mesh::HostMesh;
    use crate::v1::proc_mesh::ACTOR_SPAWN_MAX_IDLE;
    use crate::v1::proc_mesh::GET_ACTOR_STATE_MAX_IDLE;
    use crate::v1::testactor;
    use crate::v1::testing;

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
        let extent = extent!(replicas = 3, hosts = 2); // 6 ranks
        let pm: ProcMesh = testing::proc_meshes(instance, extent.clone())
            .await
            .into_iter()
            .next()
            .expect("at least one proc mesh");
        let am: ActorMesh<testactor::TestActor> = pm.spawn(instance, "test", &()).await.unwrap();

        // 2) Build our ActorMeshRef with a tiny page size (2) to
        // force multiple pages:
        // page 0: ranks [0,1], page 1: [2,3], page 2: [4,5]
        let page_size = 2;
        let amr: ActorMeshRef<testactor::TestActor> =
            ActorMeshRef::with_page_size(am.name.clone(), pm.clone(), page_size, None);
        assert_eq!(amr.extent(), extent);
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
        let sliced = amr.range("replicas", 1..).expect("slice should be valid"); // leaves 4 ranks
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
        let (id_a, _) = RealClock
            .timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for first reply")
            .expect("channel closed before first reply");
        let (id_b, _) = RealClock
            .timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for second reply")
            .expect("channel closed before second reply");
        assert_ne!(id_a, id_b, "two different ranks responded");
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
        let meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
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
            let failure = RealClock
                .timeout(Duration::from_secs(20), supervision_receiver.recv())
                .await
                .expect("timeout")
                .unwrap();
            check_failure(failure);
        }
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
        let meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let second_meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
        let second_proc_mesh = &second_meshes[1];
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
            // TODO: It can't find the real actor id, so it says the agent failed.
            assert_eq!(failure.actor_mesh_name, Some(child_name.to_string()));
            assert_eq!(failure.event.actor_id.name(), "mesh");
            if let ActorStatus::Failed(ActorErrorKind::Generic(msg)) = &failure.event.actor_status {
                assert!(
                    msg.contains("timeout waiting for message from proc mesh agent"),
                    "{}",
                    msg
                );
            } else {
                panic!("actor status is not failed: {}", failure.event.actor_status);
            }
        };
        check_failure(failure);

        // Wait for a supervision event to occur on these actors.
        for _ in 0..num_replicas {
            let failure = RealClock
                .timeout(Duration::from_secs(20), supervision_receiver.recv())
                .await
                .expect("timeout")
                .unwrap();
            check_failure(failure);
        }
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
        let meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
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
            .range("replicas", 1..3)
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
            let supervision_message = RealClock
                .timeout(Duration::from_secs(20), supervision_receiver.recv())
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
    }

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_cast() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);

        let instance = testing::instance();
        let host_mesh = testing::host_mesh(4).await;
        let proc_mesh = host_mesh
            .spawn(instance, "test", Extent::unity())
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

        let _ = HostMesh::take(host_mesh).shutdown(&instance).await;
    }

    /// Test that undeliverable messages are properly returned to the
    /// sender when communication to a proc is broken.
    ///
    /// This is the V1 version of the test from
    /// hyperactor_multiprocess/src/proc_actor.rs::test_undeliverable_message_return.
    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_undeliverable_message_return() {
        use hyperactor::mailbox::MessageEnvelope;
        use hyperactor::mailbox::Undeliverable;
        use hyperactor::test_utils::pingpong::PingPongActor;
        use hyperactor::test_utils::pingpong::PingPongMessage;

        hyperactor_telemetry::initialize_logging_for_test();

        // Set message delivery timeout for faster test
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            std::time::Duration::from_secs(1),
        );

        let instance = testing::instance();

        // Create a proc mesh with 2 replicas.
        let meshes = testing::proc_meshes(instance, extent!(replicas = 2)).await;
        let proc_mesh = &meshes[1]; // Use the ProcessAllocator version

        // Set up undeliverable message port for collecting undeliverables
        let (undeliverable_port, mut undeliverable_rx) =
            instance.open_port::<Undeliverable<MessageEnvelope>>();

        // Spawn actors individually on each replica by spawning separate actor meshes
        // with specific proc selections.
        let ping_proc_mesh = proc_mesh.range("replicas", 0..1).unwrap();
        let pong_proc_mesh = proc_mesh.range("replicas", 1..2).unwrap();

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
        RealClock.sleep(std::time::Duration::from_millis(200)).await;

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
        let deadline = RealClock.now() + std::time::Duration::from_secs(10);
        while count < n && RealClock.now() < deadline {
            match RealClock
                .timeout(std::time::Duration::from_secs(1), undeliverable_rx.recv())
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

        // Create proc mesh with 2 replicas
        let meshes = testing::proc_meshes(instance, extent!(replicas = 2)).await;
        let proc_mesh = &meshes[1]; // Use ProcessAllocator version

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
        RealClock.sleep(std::time::Duration::from_millis(200)).await;

        // Count how many actors we spawned (for verification later)
        let expected_actors = sleep_mesh.values().count();

        // Now stop the mesh - actors won't respond in time, should be
        // aborted. Time this operation to verify abort behavior.
        let stop_start = RealClock.now();
        let result = sleep_mesh.stop(instance, "test stop".to_string()).await;
        let stop_duration = RealClock.now().duration_since(stop_start);

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
    }

    /// Test that actors stop gracefully when they respond to stop
    /// signals within the timeout. Complementary to
    /// test_actor_mesh_stop_timeout which tests abort behavior. V1
    /// equivalent of
    /// hyperactor_multiprocess/src/proc_actor.rs::test_stop
    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_mesh_stop_graceful() {
        use std::assert_matches::assert_matches;

        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();

        // Create proc mesh with 2 replicas
        let meshes = testing::proc_meshes(instance, extent!(replicas = 2)).await;
        let proc_mesh = &meshes[1];

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
        let stop_start = RealClock.now();
        let result = actor_mesh.stop(instance, "test stop".to_string()).await;
        let stop_duration = RealClock.now().duration_since(stop_start);

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
        assert_matches!(next_event.event.actor_status, ActorStatus::Stopped(_));
        // Check that a cloned Ref from earlier gets the same event. Every clone
        // should get the same event, even if it's not a subscriber.
        let next_event = mesh_ref.next_supervision_event(instance).await.unwrap();
        assert_eq!(
            next_event.actor_mesh_name,
            Some(mesh_ref.name().to_string())
        );
        assert_matches!(next_event.event.actor_status, ActorStatus::Stopped(_));
    }
}
