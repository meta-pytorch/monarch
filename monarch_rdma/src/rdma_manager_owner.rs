/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # RDMA Manager Owner Actor
//!
//! Controllerless-singleton coordinator that fronts the per-proc `rdma_manager`
//! service. Given a target proc mesh it get-or-spawns a mesh of
//! [`RdmaManagerActor`]s (creating or reusing the per-proc actor on each proc),
//! waits until every rank has finished `init()` and posted a [`ReadyAck`], then
//! answers the caller on a reply port with `Result<(), RdmaInitError>`.
//!
//! ## Topology
//!
//! Three distinct objects, kept separate throughout this module:
//!
//! 1. **Owner entry** — one [`MeshState`], identified by a small owner-local
//!    [`EntryId`], per requested `ProcMeshRef` view (`by_mesh` maps the view
//!    to its `EntryId`). Two overlapping `ProcMeshRef` views are *distinct*
//!    entries even when they share procs.
//! 2. **Manager mesh + controller** — the first `EnsureRdmaManager` for a view
//!    calls `spawn_service`, which materializes a new
//!    `ActorMesh<RdmaManagerActor>` view plus one `ActorMeshController`, and
//!    *creates or reuses* the per-proc actor on each proc in the view. The
//!    entry retains the mesh to cast to.
//! 3. **Per-proc service actor** — on each proc the `rdma_manager` service is a
//!    per-proc singleton (by reserved name). Overlapping views reuse the same
//!    physical actor on a shared proc; the physical actors are shared while the
//!    owner entries and mesh views stay distinct.
//!
//! ## Correlation key
//!
//! A `ProcMeshRef` carries every rank ref, so hashing or serializing one is
//! O(N) in the rank count. To keep readiness O(N) overall rather than O(N²),
//! the owner hashes the `ProcMeshRef` only on the ensure path — one lookup
//! hash per ensure, plus one insertion hash when a new view is created —
//! never on the ack path, and thereafter correlates by the small [`EntryId`]:
//! `RdmaManagerReady` and `ReadyAck` carry the `EntryId`, so the N per-rank
//! messages neither carry nor hash a full `ProcMeshRef`, and the
//! per-`EntryId` `entries` map is the hot path.
//!
//! ## Invariants (RMO-*)
//!
//! - **RMO-1** (single owner): Exactly one owner is a statically declared service
//!   of the program's root ProcAgent. Callers may live anywhere; the root owns the
//!   service lifetime, and requester exit does not tear it down.
//! - **RMO-2** (terminal-absorbing): `Ready`/`Failed` are terminal — an
//!   `EnsureRdmaManager` for a resolved view replies from cache, and every
//!   resolution path (`ReadyAck`, a live controller report, or controller-child
//!   supervision, all through [`RdmaManagerOwnerActor::transition`]) no-ops on a
//!   non-`Pending` entry. A terminal controller-child event may use its retained
//!   controller id to recover the source view, whose proc overlap then selects
//!   pending siblings, without changing the source (RMO-17).
//! - **RMO-3** (single-transition guard): [`RdmaManagerOwnerActor::transition`]
//!   is the sole `Pending -> terminal` mutator; it drains `waiters` on resolve.
//! - **RMO-4** (operational inputs never fail the owner): spawn, cast, a live
//!   controller report, direct controller-child termination, an invalid-reference
//!   return, and a departed waiter all resolve their entry or are absorbed
//!   without returning a handler error. This is not immunity from unrelated
//!   runtime faults (e.g. the default oversized-frame undeliverable path).
//! - **RMO-5** (non-returning replies): Every reply ref gets
//!   `return_undeliverable(false)` before it is posted or stashed.
//! - **RMO-6** (readiness-after-init): `Ready` is set only once every rank's
//!   post-`init()` `ReadyAck` has arrived (`remaining_acks == 0`).
//! - **RMO-7** (typed controller attribution): A live controller report — on the
//!   owner sink or the subscriber stream — is matched to its pending source only
//!   by `MeshFailure.reporting_controller` (an `ActorId`), compared against the
//!   entry's recorded `controller_id`. Event subject, headers, mesh names, and
//!   ranks are never attribution inputs. Proc identities are used only for a
//!   terminal controller, after its id has selected the source view.
//! - **RMO-8** (manager per-proc-shared): Each target proc has at most one
//!   `rdma_manager` service actor, so manager intersection is exactly
//!   `ProcMeshRef` intersection. Overlapping views reuse the per-proc actor while
//!   keeping distinct owner entries; the existing `by_mesh` membership drives
//!   cold-path overlap without a per-rank index.
//! - **RMO-9** (no artificial timer): Readiness has no timeout; a view resolves
//!   only from a spawn/cast outcome, its `ReadyAck`s, a controller report, or
//!   controller supervision. This barrier covers *delivered* terminal inputs: a
//!   live controller that stays up but stops servicing its mailbox emits no
//!   report, heartbeat, or terminal child event, so its entry can remain
//!   `Pending` indefinitely. TODO: evaluate an external actor monitor if
//!   silent-stall detection ever becomes a requirement; none is added here.
//! - **RMO-10** (bounded-spawn is the only block): `spawn_service().await` is
//!   the sole await; the ack gather is message-driven.
//! - **RMO-11** (retain-to-message, not teardown): The retained manager mesh is
//!   kept to cast to; its `Drop` is inert, so retention is not teardown.
//! - **RMO-13** (controller topology): The owner service is controllerless;
//!   every manager mesh it creates is controller-ful, with its controller owned
//!   as a direct child of the owner — which is why a controller's own terminal
//!   status arrives on the owner's `handle_supervision_event` channel.
//! - **RMO-14** (delivered terminal-input coverage): The owner resolves on every
//!   *delivered, attributable* terminal input — an error owner-sink `MeshFailure`
//!   or a subscriber `Some(MeshFailure)` (error or a published clean terminal
//!   report for `Stopped`/`NotExist`/`Timeout`) carrying a known
//!   `reporting_controller`, and
//!   any terminal controller-child supervision event for a known controller —
//!   routing each through the source (RMO-7) or overlap (RMO-17) reduction. A
//!   delivered input with no or unknown attribution is absorbed, not resolved,
//!   and no claim is made about a controller that never delivers such an input
//!   (the RMO-9 liveness boundary).
//! - **RMO-15** (registration ordering): a new view inserts `Pending`, then posts
//!   `Subscribe`, then casts readiness. The controller replays a latched report
//!   on `Subscribe` and broadcasts later reports, so no report it publishes can
//!   race registration.
//! - **RMO-16** (pending-only subscription): at most one subscription per
//!   controller while its entry is pending; a `None` heartbeat is inert; the sole
//!   `Pending -> terminal` transition posts a non-returning `Unsubscribe`, applied
//!   asynchronously by the controller (which may itself be the failed subject).
//! - **RMO-17** (controller-view overlap): the controller id is retained across
//!   `Pending`, `Ready`, and controller-backed `Failed`, associated with the
//!   existing `ProcMeshRef` key. A live report stays source-local; controller
//!   termination fails every pending view intersecting the source's full proc
//!   view. Terminal sources and disjoint pending views are left unchanged.
//! - **RMR-1** (ack-after-init): Each per-proc manager posts `ReadyAck` from
//!   its `RdmaManagerReady` handler, which runs strictly after `init()`.
//!
//! (`RMO-12`, a retired header-stamping premise, is intentionally left unused.)
//!
//! The owner implements `Handler<MeshFailure>` (the error-only owner sink
//! required by `spawn_service`) and `Handler<Option<MeshFailure>>` (the manager
//! controller's subscriber stream, which also carries published clean terminal
//! reports and `None` heartbeats). As the controller's parent, it additionally
//! receives the controller's own terminal supervision events. Together these inputs
//! cover the delivered terminal inputs of the initialization-barrier failure
//! algebra (RMO-14); a live but silent controller is out of scope (RMO-9). The
//! owner is spawned controllerless, so spawning it imposes no such bound.

use std::collections::HashMap;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::PortRef;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::context;
use hyperactor::context::Actor as _;
use hyperactor::mailbox::InvalidReference;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::ProcMeshRef;
use hyperactor_mesh::mesh_controller::Subscribe;
use hyperactor_mesh::mesh_controller::Unsubscribe;
use hyperactor_mesh::supervision::MeshFailure;
use ndslice::view::Ranked;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::errors::RdmaInitError;
use crate::rdma_manager_actor::RdmaManagerActor;

/// Owner-local handle for a `ProcMeshRef`.
///
/// A `ProcMeshRef` contains O(N) rank references. Carrying it in each of N
/// readiness acknowledgements would make correlation O(N²). The ensure path
/// hashes the `ProcMeshRef` once to obtain this id; the N per-rank
/// `RdmaManagerReady`/`ReadyAck` messages then carry only this small id, so
/// readiness correlation stays O(N).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntryId(u64);

/// Owner -> manager-mesh readiness cast, delivered to every per-proc
/// `RdmaManagerActor`. Carries the owner ref so each rank can post [`ReadyAck`]
/// back after its own `init()` (RMR-1).
#[derive(
    Handler,
    HandleClient,
    RefClient,
    Debug,
    Serialize,
    Deserialize,
    Named,
    Clone
)]
pub struct RdmaManagerReady {
    pub owner: ActorRef<RdmaManagerOwnerActor>,
    pub entry: EntryId,
}
wirevalue::register_type!(RdmaManagerReady);

/// Per-proc manager -> owner readiness acknowledgement, one per rank.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub struct ReadyAck {
    pub entry: EntryId,
}
wirevalue::register_type!(ReadyAck);

/// Caller -> owner request to ensure a proc mesh's per-proc managers are ready.
///
/// `reply` is a plain port field (not `#[reply]`) so the handler can stash
/// it on a `Pending` entry and post the result once the mesh is ready.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub struct EnsureRdmaManager {
    pub proc_mesh: ProcMeshRef,
    pub reply: OncePortRef<Result<(), RdmaInitError>>,
}
wirevalue::register_type!(EnsureRdmaManager);

/// Per-view coordination state, held in `entries` keyed by [`EntryId`].
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "Pending is the common active state and entries live behind a HashMap; boxing it to shrink the rarer terminal variants would obscure the state machine for no material gain (the owner holds a handful of entries)."
)]
enum MeshState {
    Pending {
        /// The manager-mesh view returned by `spawn_service`, containing one
        /// actor reference per target proc. Overlapping views may reference the
        /// same per-proc service actors. Retained for the readiness cast and
        /// `Ready` state; `Drop` is inert and does not control teardown
        /// (RMO-11).
        manager_mesh: ActorMesh<RdmaManagerActor>,
        /// Controller `ActorId` for this mesh view, captured at spawn.
        /// Readiness is correlated by the small `EntryId`; this id is the
        /// separate key that attributes a controller failure — a typed
        /// `MeshFailure` (RMO-7) or the controller child's own supervision event
        /// (RMO-14) — to this entry, and drives overlap invalidation (RMO-17).
        controller_id: ActorId,
        /// The exact owner `Handler<Option<MeshFailure>>` port registered with
        /// this controller's subscriber set while pending (RMO-15/RMO-16). The
        /// sole terminal transition posts `Unsubscribe` to request its removal;
        /// the controller applies that removal asynchronously when it handles the
        /// message.
        subscriber: PortRef<Option<MeshFailure>>,
        /// Outstanding post-`init()` `ReadyAck`s (RMO-6, RMR-1): decremented as
        /// each rank acks; the entry resolves to `Ready` when it reaches 0.
        remaining_acks: usize,
        /// Callers awaiting this view's result. Each reply ref is non-returning
        /// (RMO-5) and drained exactly once by `transition` on resolution (RMO-3).
        waiters: Vec<OncePortRef<Result<(), RdmaInitError>>>,
    },
    Ready {
        /// Retained after resolution so its `Drop` stays inert (RMO-11); the
        /// per-proc service actors are not torn down when the view is `Ready`.
        manager_mesh: ActorMesh<RdmaManagerActor>,
        /// Retained so a later terminal of this controller can invalidate other
        /// still-`Pending` views sharing its per-proc managers (RMO-17), without
        /// rewriting this cached success (RMO-2).
        controller_id: ActorId,
    },
    Failed {
        /// The cached terminal error, returned to every later `EnsureRdmaManager`
        /// for this view and never rewritten once set (RMO-2).
        err: RdmaInitError,
        /// `Some` for a controller-backed failure; `None` only for a
        /// pre-controller `spawn_service` failure. Retained for RMO-17 overlap.
        controller_id: Option<ActorId>,
    },
}

impl MeshState {
    /// The retained controller id for any controller-backed state; `None` only
    /// for a pre-controller `spawn_service` failure. Locates the source entry of
    /// a terminal controller event (RMO-17).
    fn controller_id(&self) -> Option<&ActorId> {
        match self {
            MeshState::Pending { controller_id, .. } | MeshState::Ready { controller_id, .. } => {
                Some(controller_id)
            }
            MeshState::Failed { controller_id, .. } => controller_id.as_ref(),
        }
    }
}

/// The logical `ProcId`s spanned by a `ProcMeshRef`. Per RMO-8 (`rdma_manager`
/// is a per-proc singleton), two views share a manager iff their proc-id sets
/// intersect, so this drives cold-path overlap without a per-rank index.
fn proc_ids(mesh: &ProcMeshRef) -> std::collections::HashSet<ProcId> {
    let n = Ranked::region(mesh).num_ranks();
    (0..n)
        .filter_map(|rank| Ranked::get(mesh, rank).map(|p| p.proc_addr().id().clone()))
        .collect()
}

/// Whether any proc in `mesh` is in `procs`. Per RMO-8 (`rdma_manager` is a
/// per-proc singleton) this is exactly shared-manager overlap. Scanning each
/// candidate view against the single materialized source set avoids allocating a
/// fresh `HashSet` per view on the cold controller-termination path.
fn view_intersects(mesh: &ProcMeshRef, procs: &std::collections::HashSet<ProcId>) -> bool {
    let n = Ranked::region(mesh).num_ranks();
    (0..n).any(|rank| Ranked::get(mesh, rank).is_some_and(|p| procs.contains(p.proc_addr().id())))
}

/// Static client-root service name for the RDMA manager owner.
pub const RDMA_MANAGER_OWNER_ACTOR_NAME: &str = "rdma_manager_owner";

#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        EnsureRdmaManager,
        ReadyAck,
        MeshFailure,
        Option<MeshFailure>,
    ],
)]
#[hyperactor::spawnable]
pub struct RdmaManagerOwnerActor {
    /// Maps each requested view to its entry id. The `ProcMeshRef` is hashed
    /// here (one lookup per ensure, plus one insertion for a new view), never
    /// on the ack path.
    by_mesh: HashMap<ProcMeshRef, EntryId>,
    /// Per-entry coordination state, keyed by the small `EntryId`.
    entries: HashMap<EntryId, MeshState>,
    /// Monotonic source of `EntryId`s. The actor runs one `&mut self` handler at
    /// a time (the borrow is held across the handler's awaits), so this counter
    /// is bumped without races and needs no atomic or lock.
    next_entry_id: u64,
}

impl RdmaManagerOwnerActor {
    fn alloc_entry_id(&mut self) -> EntryId {
        let id = EntryId(self.next_entry_id);
        self.next_entry_id = self
            .next_entry_id
            .checked_add(1)
            .expect("owner entry id space exhausted");
        id
    }

    /// The single `Pending -> terminal` mutator (RMO-3). Resolves a `Pending`
    /// entry to `Ready`/`Failed`, draining `waiters` with `result`. No-op when
    /// the entry is absent or already terminal (RMO-2).
    fn transition(
        &mut self,
        cx: &impl context::Actor<A = Self>,
        entry: EntryId,
        result: Result<(), RdmaInitError>,
    ) {
        // Act only on Pending; absent/terminal entries are ignored
        // (RMO-2/RMO-3).
        if !matches!(self.entries.get(&entry), Some(MeshState::Pending { .. })) {
            return;
        }
        let Some(MeshState::Pending {
            manager_mesh,
            controller_id,
            subscriber,
            waiters,
            ..
        }) = self.entries.remove(&entry)
        else {
            unreachable!("checked Pending immediately above");
        };

        tracing::debug!(
            entry = entry.0,
            controller = %controller_id,
            ok = result.is_ok(),
            waiters = waiters.len(),
            "RdmaManagerOwnerActor draining Pending owner entry",
        );

        // Post `Unsubscribe` for this entry's subscription (RMO-16) before
        // installing the terminal state; the controller applies the removal
        // asynchronously when it handles the message. The controller may itself
        // be the failed subject, so the post is made non-returning and cannot
        // bounce into the owner.
        if let Some(controller) = manager_mesh.controller().as_ref() {
            let mut unsub = controller.port();
            unsub.return_undeliverable(false);
            let _ = unsub.post(cx, Unsubscribe(subscriber));
        }

        // Every waiter was made non-returning before storage (RMO-5), so a
        // departed caller drops its reply instead of bouncing it to the owner.
        for reply in waiters {
            reply.post(cx, result.clone());
        }

        // Retain the controller id across the terminal state (RMO-17): a later
        // terminal of this controller can invalidate other pending views sharing
        // its managers, without rewriting this cached result (RMO-2).
        let next = match result {
            Ok(()) => MeshState::Ready {
                manager_mesh,
                controller_id,
            },
            Err(e) => MeshState::Failed {
                err: e,
                controller_id: Some(controller_id),
            },
        };
        self.entries.insert(entry, next);
        debug_assert!(
            !matches!(self.entries.get(&entry), Some(MeshState::Pending { .. })),
            "RMO-3: entry must be terminal after a transition",
        );
    }

    /// Resolve a **live** controller report (RMO-7): fail only the `Pending`
    /// entry whose recorded `controller_id` matches, through [`transition`]
    /// (RMO-3). A live report is source-local — it never fans out to overlapping
    /// views (only controller *termination* does, see
    /// [`resolve_controller_termination`]). Returns whether a `Pending` entry
    /// matched, so callers can distinguish a resolution from an unrelated, late,
    /// or already-terminal report (which they only log).
    ///
    /// [`resolve_controller_termination`]: Self::resolve_controller_termination
    fn resolve_live_report(
        &mut self,
        cx: &impl context::Actor<A = Self>,
        controller_id: &ActorId,
        err: RdmaInitError,
    ) -> bool {
        debug_assert!(
            self.entries
                .values()
                .filter(|s| matches!(
                    s,
                    MeshState::Pending { controller_id: c, .. } if c == controller_id
                ))
                .count()
                <= 1,
            "RMO-7: two pending entries must not share a controller id",
        );
        // Cold path over a small set of views: a linear scan avoids a second
        // `controller_id -> EntryId` index whose consistency would be another
        // invariant to uphold on every terminal path. Copy the matching
        // `EntryId` out before the terminal mutation so no map borrow is live
        // across `transition`.
        let matched = self.entries.iter().find_map(|(id, state)| match state {
            MeshState::Pending {
                controller_id: c, ..
            } if c == controller_id => Some(*id),
            _ => None,
        });
        match matched {
            Some(entry) => {
                self.transition(cx, entry, Err(err));
                true
            }
            None => false,
        }
    }

    /// Resolve a terminal controller event (RMO-14/RMO-17). Locate the source
    /// entry by `controller_id` across *every* controller-backed state (via
    /// [`MeshState::controller_id`]); using that source's retained `ProcMeshRef`
    /// view, fail every still-`Pending` entry whose proc view intersects it. A
    /// terminal source is left unchanged (RMO-2); disjoint pending views are
    /// untouched. By RMO-8 (per-proc singleton managers) proc-view intersection
    /// is exactly shared-manager intersection. Returns `None` when no source
    /// view can be recovered, or `Some(n)` with the number of entries
    /// transitioned.
    fn resolve_controller_termination(
        &mut self,
        cx: &impl context::Actor<A = Self>,
        controller_id: &ActorId,
        err: RdmaInitError,
    ) -> Option<usize> {
        debug_assert!(
            self.entries
                .values()
                .filter(|s| s.controller_id() == Some(controller_id))
                .count()
                <= 1,
            "RMO-17: a controller id must be unique across all entries",
        );
        // 1. Find the source entry id by controller id (any state).
        let source_id = self
            .entries
            .iter()
            .find_map(|(id, s)| (s.controller_id() == Some(controller_id)).then_some(*id))?;
        // 2. Recover the source's proc view via the existing `by_mesh` key.
        let source_procs = self
            .by_mesh
            .iter()
            .find_map(|(mesh, id)| (*id == source_id).then(|| proc_ids(mesh)))?;
        // 3. Collect every still-`Pending` entry whose view intersects the
        //    source view. Scan each candidate against the single source set (no
        //    per-view `HashSet`), and build the target list before any transition
        //    so no map borrow is live across the terminal mutation.
        let targets: Vec<EntryId> = self
            .by_mesh
            .iter()
            .filter(|(mesh, id)| {
                matches!(self.entries.get(id), Some(MeshState::Pending { .. }))
                    && view_intersects(mesh, &source_procs)
            })
            .map(|(_, id)| *id)
            .collect();
        let n = targets.len();
        for entry in targets {
            self.transition(cx, entry, Err(err.clone()));
        }
        Some(n)
    }

    /// Attribute a live controller report (owner sink or subscriber `Some`) by
    /// its typed `reporting_controller` and fail that source entry if pending
    /// (RMO-7, source-local). `None`/unknown/already-terminal reporters are
    /// logged and ignored.
    fn handle_live_report(&mut self, cx: &impl context::Actor<A = Self>, message: &MeshFailure) {
        let Some(controller_id) = message.reporting_controller.as_ref() else {
            tracing::warn!(
                ?message,
                "live report without a reporting controller; ignoring"
            );
            return;
        };
        let controller_id = controller_id.clone();
        let err = RdmaInitError::InitFailed(message.to_string());
        if self.resolve_live_report(cx, &controller_id, err) {
            return;
        }
        // No pending source matched. A report for a controller whose entry is
        // already terminal is expected — it may be a late report, or the same
        // error arriving on both the owner sink and the subscriber stream (RMO-2);
        // only a report for a wholly unknown controller is anomalous.
        if self
            .entries
            .values()
            .any(|s| s.controller_id() == Some(&controller_id))
        {
            tracing::debug!(
                %controller_id,
                "late or duplicate live report for an already-resolved controller; absorbing",
            );
        } else {
            tracing::warn!(%controller_id, "live report from an unknown controller; ignoring");
        }
    }
}

#[async_trait]
impl RemoteSpawn for RdmaManagerOwnerActor {
    type Params = ();

    async fn new(_params: (), _environment: Flattrs) -> Result<Self, anyhow::Error> {
        Ok(Self {
            by_mesh: HashMap::new(),
            entries: HashMap::new(),
            next_entry_id: 0,
        })
    }
}

#[async_trait]
impl Actor for RdmaManagerOwnerActor {
    /// The manager-mesh controller is a direct child of the owner, so any
    /// *terminal* status of the controller itself — clean `Stopped` as well as
    /// `Failed` — is delivered here (RMO-14). It is matched by `event.actor_id`
    /// on this direct parent/child channel (deliberately distinct from
    /// `MeshFailure` attribution). Because a terminating controller's cleanup can
    /// affect per-proc managers shared with other pending views, this
    /// conservatively fails every pending view intersecting the source
    /// controller's retained proc view (RMO-17), leaving a terminal source and
    /// disjoint pending views unchanged.
    /// Non-terminal events are absorbed. Every event returns `Ok(true)` so no
    /// child event terminates this singleton (RMO-1/RMO-4); the owner's only
    /// children are manager controllers.
    async fn handle_supervision_event(
        &mut self,
        this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        if event.actor_status.is_terminal() {
            let controller_id = event.actor_id.id().clone();
            let err = RdmaInitError::Supervision(event.to_string());
            match self.resolve_controller_termination(this, &controller_id, err) {
                Some(0) => {
                    // A known controller with no overlapping pending view is the
                    // expected teardown case (e.g. a `Ready` source stopping with no
                    // pending sibling to fail).
                    tracing::debug!(
                        %controller_id,
                        "terminal controller with no overlapping pending view; nothing to resolve",
                    );
                }
                None => {
                    tracing::warn!(
                        %controller_id,
                        "terminal supervision event could not be attributed to a controller view; absorbing",
                    );
                }
                Some(_) => {}
            }
        }
        Ok(true)
    }

    /// Initialization control traffic — the readiness cast to a manager, or the
    /// `Subscribe` to a controller — can return to the owner as an invalid
    /// reference if its target has already died; the default policy would `bail!`
    /// and terminate the owner. Swallow it (RMO-4). The authoritative terminal
    /// signal is the controller report (RMO-7) or the controller-child event
    /// (RMO-14), never the returned envelope — which is not parsed.
    /// `handle_undeliverable_message` is deliberately left at its default (benign
    /// except for an oversized frame).
    async fn handle_invalid_reference(
        &mut self,
        _cx: &Instance<Self>,
        invalid: InvalidReference,
        undeliverable: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        tracing::warn!(
            ?invalid,
            ?undeliverable,
            "RdmaManagerOwnerActor swallowing an invalid-reference return from initialization control traffic",
        );
        Ok(())
    }
}

// Implements the get-or-initialize barrier for one proc-mesh view. Existing
// entries reply from terminal state or join the pending waiter set. New entries
// spawn or reuse the per-proc service actors, record `Pending` before casting
// readiness, and resolve only after every rank acknowledges. Operational
// failures are returned through `reply`, never propagated from this handler.
#[async_trait]
impl Handler<EnsureRdmaManager> for RdmaManagerOwnerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: EnsureRdmaManager,
    ) -> Result<(), anyhow::Error> {
        let EnsureRdmaManager {
            proc_mesh,
            mut reply,
        } = msg;
        // RMO-5: never bounce an undeliverable reply back to the owner. Set
        // before the ref is posted or enters `waiters`.
        reply.return_undeliverable(false);

        // Hash the (O(N)) `ProcMeshRef` here (one lookup; plus one insertion
        // below for a new view) — never on the ack path. `EntryId: Copy`, so
        // this borrow of `by_mesh` ends before `entries` is touched.
        if let Some(&entry) = self.by_mesh.get(&proc_mesh) {
            match self.entries.get_mut(&entry) {
                Some(MeshState::Ready { manager_mesh, .. }) => {
                    debug_assert!(
                        manager_mesh.controller().is_some(),
                        "RMO-13: a Ready manager mesh is controller-ful",
                    );
                    reply.post(cx, Ok(()));
                }
                Some(MeshState::Failed { err, .. }) => {
                    reply.post(cx, Err(err.clone()));
                }
                Some(MeshState::Pending { waiters, .. }) => {
                    waiters.push(reply);
                }
                None => {
                    // `by_mesh` and `entries` are inserted together, so this is
                    // unreachable; reply rather than hang the caller if it ever
                    // desyncs.
                    debug_assert!(false, "by_mesh points to a missing entries slot");
                    reply.post(
                        cx,
                        Err(RdmaInitError::SpawnFailed("owner index desync".to_string())),
                    );
                }
            }
            return Ok(());
        }

        // Absent view: allocate its entry id, then materialize a
        // controller-ful manager mesh. `spawn_service` applies the singleton
        // `rdma_manager` resource id on each target proc, creating missing
        // actors and reusing existing actors from overlapping views (RMO-8).
        // This is the sole blocking await (RMO-10).
        let entry = self.alloc_entry_id();
        let mesh = match proc_mesh
            .spawn_service::<RdmaManagerActor, _>(cx, "rdma_manager", &None)
            .await
        {
            Ok(mesh) => mesh,
            Err(e) => {
                // Spawn failure is cached terminally (RMO-2): no `Pending`
                // exists yet, so insert `Failed` directly and reply inline.
                let err = RdmaInitError::SpawnFailed(e.to_string());
                self.entries.insert(
                    entry,
                    MeshState::Failed {
                        err: err.clone(),
                        controller_id: None,
                    },
                );
                self.by_mesh.insert(proc_mesh, entry);
                reply.post(cx, Err(err));
                return Ok(());
            }
        };
        // Capture the controller ref (cloned so it outlives the `mesh` move) and
        // derive its id — the attribution/overlap key (RMO-7/RMO-14/RMO-17). A
        // controller-ful `spawn_service` always sets one (RMO-13), so `expect`
        // here asserts that invariant.
        let controller = mesh
            .controller()
            .as_ref()
            .expect("controller-ful spawn_service always sets a controller")
            .clone();
        let controller_id = controller.actor_addr().id().clone();
        // The owner's shared `Handler<Option<MeshFailure>>` port; each pending
        // entry registers this same port with its own controller's subscriber
        // set (RMO-16). Reports are demultiplexed by typed `reporting_controller`.
        let subscriber = cx.instance().port::<Option<MeshFailure>>().bind();
        let remaining_acks = mesh.region().num_ranks();
        let manager_for_cast = mesh.clone();

        // Record `Pending` BEFORE subscribing and casting, so a replayed terminal
        // or a cast failure has an entry to drain terminally (RMO-2/RMO-15).
        self.entries.insert(
            entry,
            MeshState::Pending {
                manager_mesh: mesh,
                controller_id,
                subscriber: subscriber.clone(),
                remaining_acks,
                waiters: vec![reply],
            },
        );
        self.by_mesh.insert(proc_mesh, entry);

        // Subscribe to the controller's lifecycle stream before casting (RMO-15):
        // `Subscribe` replays any latched report and later reports are broadcast
        // to the now-inserted port, so no published report races registration.
        controller.post(cx, Subscribe(subscriber));

        // 0-rank mesh: no ack will ever come, so resolve immediately (the
        // transition also posts `Unsubscribe`).
        if remaining_acks == 0 {
            self.transition(cx, entry, Ok(()));
            return Ok(());
        }

        // Cast readiness to every rank; each rank acks post-`init()` (RMR-1).
        let owner = cx.bind::<RdmaManagerOwnerActor>();
        if let Err(e) = manager_for_cast.cast(cx, RdmaManagerReady { owner, entry }) {
            self.transition(cx, entry, Err(RdmaInitError::SpawnFailed(e.to_string())));
        }
        Ok(())
    }
}

// Counts the post-`init()` acknowledgement from each manager rank. Unknown or
// terminal entries absorb late acknowledgements; only the final acknowledgement
// resolves a `Pending` entry through the guarded transition path.
#[async_trait]
#[hyperactor::handle(ReadyAck)]
impl ReadyAckHandler for RdmaManagerOwnerActor {
    async fn ready_ack(&mut self, cx: &Context<Self>, entry: EntryId) -> Result<(), anyhow::Error> {
        // Decrement under a scoped borrow; resolve after it is released.
        let resolved = if let Some(MeshState::Pending { remaining_acks, .. }) =
            self.entries.get_mut(&entry)
        {
            debug_assert!(
                *remaining_acks > 0,
                "ReadyAck on a Pending entry with 0 remaining (0-rank resolves before any ack can arrive)",
            );
            *remaining_acks -= 1;
            *remaining_acks == 0
        } else {
            // Resolved (Ready/Failed) or absent -> ignore (RMO-2 late-signal
            // absorption).
            false
        };
        if resolved {
            // RMO-3/RMO-6: the final acknowledgement performs the entry's sole
            // `Pending -> Ready` transition through the centralized guard.
            self.transition(cx, entry, Ok(()));
        }
        Ok(())
    }
}

// The two live-report routes. The owner sink `Handler<MeshFailure>` (required by
// `spawn_service`) carries only *error* reports; the subscriber
// `Handler<Option<MeshFailure>>` additionally carries published *clean*
// terminal reports (`Stopped`/`NotExist`/`Timeout`) and `None` heartbeats
// (RMO-14). Both attribute by the typed `reporting_controller` and fail only the
// source pending entry (RMO-7); an error that arrives on both is absorbed by the
// terminal-absorbing transition (RMO-2). Neither returns `Err`, so a report
// cannot terminate the owner (RMO-4).
#[async_trait]
impl Handler<MeshFailure> for RdmaManagerOwnerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: MeshFailure,
    ) -> Result<(), anyhow::Error> {
        self.handle_live_report(cx, &message);
        Ok(())
    }
}

#[async_trait]
impl Handler<Option<MeshFailure>> for RdmaManagerOwnerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: Option<MeshFailure>,
    ) -> Result<(), anyhow::Error> {
        // `None` is a healthy heartbeat (RMO-16); only `Some` is a report.
        if let Some(message) = message {
            self.handle_live_report(cx, &message);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use hyperactor::ActorAddr;
    use hyperactor::ActorHandle;
    use hyperactor::ActorId;
    use hyperactor::ActorRef;
    use hyperactor::Context;
    use hyperactor::Endpoint;
    use hyperactor::Handler;
    use hyperactor::PortRef;
    use hyperactor::RemoteSpawn;
    use hyperactor::actor::ActorStatus;
    use hyperactor::context::Actor as _;
    use hyperactor::mailbox::DeliveryFailure;
    use hyperactor::mailbox::InvalidReference;
    use hyperactor::mailbox::InvalidReferenceReason;
    use hyperactor::mailbox::MessageEnvelope;
    use hyperactor::mailbox::Undeliverable;
    use hyperactor::supervision::ActorSupervisionEvent;
    use hyperactor_config::Flattrs;
    use hyperactor_mesh::ActorMesh;
    use hyperactor_mesh::ProcMeshRef;
    use hyperactor_mesh::context;
    use hyperactor_mesh::host_mesh::HostMesh;
    use hyperactor_mesh::mesh_controller::ActorMeshController;
    use hyperactor_mesh::mesh_controller::GetSubscriberCount;
    use hyperactor_mesh::mesh_controller::Subscribe;
    use hyperactor_mesh::mesh_controller::Unsubscribe;
    use hyperactor_mesh::mesh_id::ResourceId;
    use hyperactor_mesh::resource;
    use hyperactor_mesh::supervision::MeshFailure;
    use ndslice::ViewExt;
    use ndslice::view::Ranked;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::EnsureRdmaManagerClient;
    use super::EntryId;
    use super::MeshState;
    use super::RDMA_MANAGER_OWNER_ACTOR_NAME;
    use super::RdmaManagerOwnerActor;
    use super::ReadyAck;
    use crate::RdmaManagerActor;
    use crate::errors::RdmaInitError;

    /// Happy path: ensuring a manager on a two-proc mesh replies `Ok` only once
    /// every rank posts its post-`init()` `ReadyAck` (RMO-6, RMO-8, RMR-1). TCP
    /// fallback lets the manager `init()` succeed without RDMA hardware.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn ensure_resolves_ok_after_all_ranks_ack() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 2),
                None,
                None,
            )
            .await?;
        let proc_mesh_ref: ProcMeshRef = (*proc_mesh).clone();

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        let (reply, rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, proc_mesh_ref, reply.bind())
            .await?;

        let result = rx.recv().await?;
        assert_eq!(result, Ok(()));

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    // Test-only local probe messages, delivered via the owner's ActorHandle.

    #[derive(Debug, PartialEq, Eq)]
    enum EntryStateTag {
        Absent,
        Pending,
        Ready,
        Failed,
    }

    #[derive(Debug)]
    struct ProbeResult {
        state: EntryStateTag,
        entry_count: usize,
        waiter_count: usize,
        remaining_acks: Option<usize>,
        manager_ids: Vec<ActorId>,
        controller_id: Option<ActorId>,
    }

    /// Reports a view's entry state, the owner's total entry count, and (when
    /// `Pending`) its waiter and remaining-ack counts.
    #[derive(Debug)]
    struct Probe {
        key: ProcMeshRef,
        reply: oneshot::Sender<ProbeResult>,
    }

    #[async_trait]
    impl Handler<Probe> for RdmaManagerOwnerActor {
        async fn handle(&mut self, _cx: &Context<Self>, msg: Probe) -> Result<(), anyhow::Error> {
            let entry_count = self.entries.len();
            let (state, waiter_count, remaining_acks, manager_ids, controller_id) = match self
                .by_mesh
                .get(&msg.key)
                .copied()
                .and_then(|id| self.entries.get(&id))
            {
                None => (EntryStateTag::Absent, 0, None, Vec::new(), None),
                Some(MeshState::Pending {
                    waiters,
                    remaining_acks,
                    manager_mesh,
                    controller_id,
                    ..
                }) => (
                    EntryStateTag::Pending,
                    waiters.len(),
                    Some(*remaining_acks),
                    manager_mesh_ids(manager_mesh),
                    Some(controller_id.clone()),
                ),
                Some(MeshState::Ready {
                    manager_mesh,
                    controller_id,
                }) => (
                    EntryStateTag::Ready,
                    0,
                    None,
                    manager_mesh_ids(manager_mesh),
                    Some(controller_id.clone()),
                ),
                Some(MeshState::Failed { controller_id, .. }) => (
                    EntryStateTag::Failed,
                    0,
                    None,
                    Vec::new(),
                    controller_id.clone(),
                ),
            };
            let _ = msg.reply.send(ProbeResult {
                state,
                entry_count,
                waiter_count,
                remaining_acks,
                manager_ids,
                controller_id,
            });
            Ok(())
        }
    }

    async fn probe(
        handle: &ActorHandle<RdmaManagerOwnerActor>,
        cx: &impl hyperactor::context::Actor,
        key: ProcMeshRef,
    ) -> ProbeResult {
        let (tx, rx) = oneshot::channel();
        handle.post(cx, Probe { key, reply: tx });
        rx.await.expect("owner replied to Probe")
    }

    /// The member `ActorId`s of a manager mesh (one per proc in the view).
    fn manager_mesh_ids(mesh: &ActorMesh<RdmaManagerActor>) -> Vec<ActorId> {
        let ranks = Ranked::region(&**mesh).num_ranks();
        (0..ranks)
            .filter_map(|rank| {
                Ranked::get(&**mesh, rank).map(|actor| actor.actor_addr().id().clone())
            })
            .collect()
    }

    /// Once an entry is `Ready`, a duplicate ensure replies from the cached
    /// terminal state without adding another owner entry (RMO-2).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn duplicate_ensure_replies_from_ready_cache() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 2),
                None,
                None,
            )
            .await?;
        let key: ProcMeshRef = (*proc_mesh).clone();

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        let (r1, rx1) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r1.bind())
            .await?;
        assert_eq!(rx1.recv().await?, Ok(()));

        let (r2, rx2) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r2.bind())
            .await?;
        assert_eq!(rx2.recv().await?, Ok(()));

        let observed = probe(&owner_handle, client, key.clone()).await;
        assert_eq!(observed.state, EntryStateTag::Ready);
        assert_eq!(
            observed.entry_count, 1,
            "a duplicate ensure for the same view must not add an entry",
        );

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// Blocks the owner's message loop: signals `entered`, then awaits
    /// `release`. Lets a test enqueue messages behind it to control ordering.
    #[derive(Debug)]
    struct Pause {
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    }

    #[async_trait]
    impl Handler<Pause> for RdmaManagerOwnerActor {
        async fn handle(&mut self, _cx: &Context<Self>, msg: Pause) -> Result<(), anyhow::Error> {
            let _ = msg.entered.send(());
            let _ = msg.release.await;
            Ok(())
        }
    }

    /// A second ensure for the same view arriving while the first is still
    /// `Pending` joins that entry as an extra waiter rather than spawning
    /// again; both waiters then resolve `Ok` together (RMO-2, RMO-3).
    /// `Pause` + mailbox FIFO make the in-flight overlap deterministic.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn duplicate_ensure_joins_pending_entry() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 2),
                None,
                None,
            )
            .await?;
        let key: ProcMeshRef = (*proc_mesh).clone();

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        // Block the owner, then enqueue (FIFO, behind the pause) two ensures
        // for the same key and a Probe.
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: entered_tx,
                release: release_rx,
            },
        );
        entered_rx.await.expect("owner entered Pause");

        let (r1, rx1) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r1.bind())
            .await?;
        let (r2, rx2) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r2.bind())
            .await?;

        let (ptx, prx) = oneshot::channel();
        owner_handle.post(
            client,
            Probe {
                key: key.clone(),
                reply: ptx,
            },
        );

        // Release: FIFO runs ensure 1 (records `Pending` + casts), ensure 2
        // (joins as a waiter), then the Probe -- all before any `ReadyAck`.
        let _ = release_tx.send(());

        let observed = prx.await.expect("owner replied to Probe");
        assert_eq!(observed.state, EntryStateTag::Pending);
        assert_eq!(observed.entry_count, 1, "both ensures share one entry");
        assert_eq!(observed.waiter_count, 2, "second ensure joined as a waiter");
        assert_eq!(
            observed.remaining_acks,
            Some(2),
            "two-rank barrier initialized before any ack",
        );

        assert_eq!(rx1.recv().await?, Ok(()));
        assert_eq!(rx2.recv().await?, Ok(()));

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// While one view's readiness acks are outstanding, the owner keeps
    /// servicing other messages: two ensures for different views both reach
    /// `Pending` before any ack, so the gather is message-driven, not a
    /// blocking await (RMO-10).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn ensure_gather_is_non_blocking() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 2),
                None,
                None,
            )
            .await?;
        let view_a: ProcMeshRef = proc_mesh.range("procs", 0..1)?;
        let view_b: ProcMeshRef = proc_mesh.range("procs", 1..2)?;

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        // Block the owner, then enqueue (FIFO, behind the pause) ensures for
        // two distinct views and a Probe of each.
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: entered_tx,
                release: release_rx,
            },
        );
        entered_rx.await.expect("owner entered Pause");

        let (ra, rxa) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_a.clone(), ra.bind())
            .await?;
        let (rb, rxb) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_b.clone(), rb.bind())
            .await?;

        let (pta, prxa) = oneshot::channel();
        owner_handle.post(
            client,
            Probe {
                key: view_a.clone(),
                reply: pta,
            },
        );
        let (ptb, prxb) = oneshot::channel();
        owner_handle.post(
            client,
            Probe {
                key: view_b.clone(),
                reply: ptb,
            },
        );

        let _ = release_tx.send(());

        // Both views are Pending before any ack: view B was serviced without
        // blocking on view A's outstanding acks.
        let a = prxa.await.expect("probe view A");
        let b = prxb.await.expect("probe view B");
        assert_eq!(a.state, EntryStateTag::Pending);
        assert_eq!(b.state, EntryStateTag::Pending);
        assert_eq!(a.entry_count, 2, "owner tracks both views concurrently");

        assert_eq!(rxa.recv().await?, Ok(()));
        assert_eq!(rxb.recv().await?, Ok(()));

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// The owner is spawned as a controllerless, ProcAgent-managed singleton
    /// (as production does via `spawn_controllerless_service`): its mesh has no
    /// controller (RMO-13), and after the returned `ActorMesh` is dropped the
    /// owner still serves via the retained rank-0 `ActorRef` (RMO-1).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn owner_is_controllerless_singleton() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 1),
                None,
                None,
            )
            .await?;
        let target: ProcMeshRef = (*proc_mesh).clone();

        // Spawn the owner the way production does: a controllerless singleton.
        let owner_mesh = proc_mesh
            .spawn_controllerless_service::<RdmaManagerOwnerActor, _>(
                client,
                RDMA_MANAGER_OWNER_ACTOR_NAME,
                &(),
            )
            .await?;
        assert!(
            owner_mesh.controller().is_none(),
            "owner is spawned controllerless",
        );
        let owner_ref: ActorRef<RdmaManagerOwnerActor> =
            owner_mesh.values().next().expect("owner rank 0").clone();

        // Drop the returned mesh; the ProcAgent-managed singleton persists.
        drop(owner_mesh);

        // The retained ref still reaches the owner, which serves the request.
        let (r, rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_ref
            .ensure_rdma_manager(client, target, r.bind())
            .await?;
        assert_eq!(rx.recv().await?, Ok(()));

        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// Spawning the owner service twice yields the same singleton actor: the
    /// controllerless-singleton owner is deduped by its reserved name, so there
    /// is exactly one per process (RMO-1).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn owner_service_is_singleton() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 1),
                None,
                None,
            )
            .await?;

        let first = proc_mesh
            .spawn_controllerless_service::<RdmaManagerOwnerActor, _>(
                client,
                RDMA_MANAGER_OWNER_ACTOR_NAME,
                &(),
            )
            .await?;
        let second = proc_mesh
            .spawn_controllerless_service::<RdmaManagerOwnerActor, _>(
                client,
                RDMA_MANAGER_OWNER_ACTOR_NAME,
                &(),
            )
            .await?;

        let addr1 = first
            .values()
            .next()
            .expect("owner rank 0")
            .actor_addr()
            .clone();
        let addr2 = second
            .values()
            .next()
            .expect("owner rank 0")
            .actor_addr()
            .clone();
        assert_eq!(
            addr1, addr2,
            "the owner service is a singleton, deduped by name"
        );

        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// Overlapping views share the per-proc `rdma_manager`: the shared proc
    /// runs one physical actor (identical `ActorId`) referenced by both views'
    /// manager meshes, while each view keeps a distinct owner entry (RMO-8).
    /// Verified in both ensure orders (whole-first and slice-first).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn overlapping_views_share_per_proc_manager_whole_first() -> anyhow::Result<()> {
        overlapping_views_share_per_proc_manager(true).await
    }

    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn overlapping_views_share_per_proc_manager_slice_first() -> anyhow::Result<()> {
        overlapping_views_share_per_proc_manager(false).await
    }

    async fn overlapping_views_share_per_proc_manager(whole_first: bool) -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 2),
                None,
                None,
            )
            .await?;
        let whole: ProcMeshRef = (*proc_mesh).clone();
        let slice: ProcMeshRef = proc_mesh.range("procs", 1..2)?;

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        let (first, second) = if whole_first {
            (whole.clone(), slice.clone())
        } else {
            (slice.clone(), whole.clone())
        };

        // The reuse path crosses three independently-blockable stages: the
        // second `spawn_service`, the reused actor's second `ReadyAck`, and the
        // teardown of two overlapping controllers. Bound each separately so a
        // timeout names its stage rather than tripping the whole-test deadline.

        // First view resolves against fresh per-proc managers.
        let (r1, rx1) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, first, r1.bind())
            .await?;
        let first_reply = timeout(Duration::from_secs(15), rx1.recv())
            .await
            .expect("first ensure reply elapsed")?;
        assert_eq!(first_reply, Ok(()));

        // Second view overlaps the first. Probe before awaiting its reply: if
        // the owner is blocked inside the second `spawn_service`, it cannot
        // service this Probe and the wait elapses (isolates a spawn hang).
        let (r2, rx2) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, second.clone(), r2.bind())
            .await?;
        let after_spawn = timeout(
            Duration::from_secs(15),
            probe(&owner_handle, client, second.clone()),
        )
        .await
        .expect("second spawn_service blocked: probe elapsed");
        assert!(
            matches!(
                after_spawn.state,
                EntryStateTag::Pending | EntryStateTag::Ready
            ),
            "second ensure recorded no entry: {after_spawn:?}",
        );

        // spawn_service returned; a hang here is the reused actor failing to
        // deliver the second view's `ReadyAck`.
        let second_reply = timeout(Duration::from_secs(15), rx2.recv())
            .await
            .expect("second ensure reply elapsed: shared actor did not re-ack")?;
        assert_eq!(second_reply, Ok(()));

        let whole_entry = probe(&owner_handle, client, whole.clone()).await;
        let slice_entry = probe(&owner_handle, client, slice.clone()).await;

        // Distinct owner entries, one per view.
        assert_eq!(whole_entry.entry_count, 2, "each view keeps its own entry");
        assert_eq!(whole_entry.state, EntryStateTag::Ready);
        assert_eq!(slice_entry.state, EntryStateTag::Ready);

        // The shared proc runs one physical manager, referenced by both views.
        let shared = whole_entry
            .manager_ids
            .iter()
            .filter(|id| slice_entry.manager_ids.contains(id))
            .count();
        assert_eq!(
            whole_entry.manager_ids.len(),
            2,
            "whole view spans two procs"
        );
        assert_eq!(
            slice_entry.manager_ids.len(),
            1,
            "slice view spans one proc"
        );
        assert_eq!(shared, 1, "views share the one manager on the common proc");

        // Teardown drops two overlapping controllers over shared procs.
        timeout(Duration::from_secs(15), async move {
            owner_handle.stop("test complete")?;
            let _ = owner_handle.await;
            host_mesh.shutdown(client).await?;
            anyhow::Ok(())
        })
        .await
        .expect("shutdown elapsed: overlapping controller cleanup")?;
        Ok(())
    }

    // Failure-path fixtures, helpers, and tests.
    //
    // The failure tests reuse production-created entries rather than fabricating
    // `MeshState`s. `HoldPending` adds one sentinel to `remaining_acks` on a real
    // `Pending` entry so its real per-rank `ReadyAck`s cannot resolve it, keeping
    // the entry deterministically `Pending` for failure injection; it preserves
    // `by_mesh`/`entries` consistency and the entry's non-returning waiter
    // (RMO-5), and returns the real controller ref for tests that drive the
    // controller's own lifecycle.

    /// Build a `MeshFailure` the way a controller posts one: `reporting_controller`
    /// is the attribution key (RMO-7); the event subject is deliberately
    /// unrelated to the controller.
    fn make_mesh_failure(reporter: Option<ActorId>, subject: ActorAddr) -> MeshFailure {
        MeshFailure {
            actor_mesh_name: Some("rdma_manager".to_string()),
            event: ActorSupervisionEvent::new(
                subject,
                None,
                ActorStatus::generic_failure("injected manager failure"),
                None,
            ),
            crashed_ranks: vec![0],
            reporting_controller: reporter,
        }
    }

    /// Poll the entry for `key` until `remaining_acks == want`, giving the real
    /// per-rank `ReadyAck` time to land behind the held sentinel.
    async fn wait_for_remaining(
        handle: &ActorHandle<RdmaManagerOwnerActor>,
        cx: &impl hyperactor::context::Actor,
        key: ProcMeshRef,
        want: usize,
    ) {
        for _ in 0..400 {
            if probe(handle, cx, key.clone()).await.remaining_acks == Some(want) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("entry did not reach remaining_acks == {want}");
    }

    /// Hold a real `Pending` entry open (sentinel) and report its `EntryId`,
    /// recorded `controller_id`, and the real manager-mesh controller ref, so a
    /// test can drive that controller's own lifecycle via `downcast_handle` or
    /// query it with `GetSubscriberCount`.
    #[derive(Debug)]
    struct HoldPending {
        key: ProcMeshRef,
        reply: oneshot::Sender<
            Option<(
                EntryId,
                ActorId,
                ActorRef<ActorMeshController<RdmaManagerActor>>,
            )>,
        >,
    }

    #[async_trait]
    impl Handler<HoldPending> for RdmaManagerOwnerActor {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
            msg: HoldPending,
        ) -> Result<(), anyhow::Error> {
            let held = self.by_mesh.get(&msg.key).copied().and_then(|id| {
                match self.entries.get_mut(&id) {
                    Some(MeshState::Pending {
                        remaining_acks,
                        controller_id,
                        manager_mesh,
                        ..
                    }) => {
                        *remaining_acks += 1;
                        let controller = manager_mesh
                            .controller()
                            .as_ref()
                            .expect("controller-ful manager mesh")
                            .clone();
                        Some((id, controller_id.clone(), controller))
                    }
                    _ => None,
                }
            });
            let _ = msg.reply.send(held);
            Ok(())
        }
    }

    /// Poll the entry for `key` until it reaches `want`.
    async fn wait_until_state(
        handle: &ActorHandle<RdmaManagerOwnerActor>,
        cx: &impl hyperactor::context::Actor,
        key: ProcMeshRef,
        want: EntryStateTag,
    ) {
        for _ in 0..400 {
            if probe(handle, cx, key.clone()).await.state == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("entry did not reach state {want:?}");
    }

    /// Test-only owner op: remove the owner's shared `Option<MeshFailure>`
    /// subscription from `controller`, using the exact well-known handler port
    /// the owner registers. Owner-originated so a following owner-originated
    /// `CountSubscribers` is ordered behind it at the controller.
    #[derive(Debug)]
    struct UnsubscribeController {
        controller: ActorRef<ActorMeshController<RdmaManagerActor>>,
        ack: oneshot::Sender<()>,
    }

    #[async_trait]
    impl Handler<UnsubscribeController> for RdmaManagerOwnerActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: UnsubscribeController,
        ) -> Result<(), anyhow::Error> {
            let port = cx.instance().port::<Option<MeshFailure>>().bind();
            let mut unsub = msg.controller.port();
            unsub.return_undeliverable(false);
            let _ = unsub.post(cx, Unsubscribe(port));
            let _ = msg.ack.send(());
            Ok(())
        }
    }

    /// Test-only owner op: query `controller`'s subscriber count from the owner,
    /// so the query is ordered behind any prior owner-posted `Subscribe`/
    /// `Unsubscribe`/`transition` unsubscribe to that same controller (RMO-16).
    #[derive(Debug)]
    struct CountSubscribers {
        controller: ActorRef<ActorMeshController<RdmaManagerActor>>,
        count_port: PortRef<usize>,
    }

    #[async_trait]
    impl Handler<CountSubscribers> for RdmaManagerOwnerActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: CountSubscribers,
        ) -> Result<(), anyhow::Error> {
            // Owner-originated: this `GetSubscriberCount` is ordered behind any
            // prior owner post to the same controller (Subscribe / Unsubscribe /
            // the transition drop), so the reported count reflects them.
            msg.controller.post(cx, GetSubscriberCount(msg.count_port));
            Ok(())
        }
    }

    /// Post an owner-originated subscriber-count query and await the answer. The
    /// reply port lives on the caller's mailbox; the owner only forwards it, so
    /// the query is ordered behind the owner's prior posts to that controller.
    async fn owner_subscriber_count(
        owner: &ActorHandle<RdmaManagerOwnerActor>,
        cx: &impl hyperactor::context::Actor,
        controller: ActorRef<ActorMeshController<RdmaManagerActor>>,
    ) -> usize {
        let (tx, mut rx) = cx.instance().open_port::<usize>();
        owner.post(
            cx,
            CountSubscribers {
                controller,
                count_port: tx.bind(),
            },
        );
        rx.recv()
            .await
            .expect("controller replied to GetSubscriberCount")
    }

    /// Test-only owner op that drives the replay-before-insert path (RMO-15)
    /// deterministically, without any timing. All four messages are owner-posted
    /// to the same controller, so they are FIFO-ordered there: remove the owner's
    /// subscription, report the count at that midpoint (must be `0`), latch a
    /// whole-view non-error report via `resource::Stop`, then re-subscribe the
    /// same owner handler port. Only the `Subscribe` replay can then resolve the
    /// held entry, since the latched report carries the controller id it recorded.
    #[derive(Debug)]
    struct ReplaySequence {
        controller: ActorRef<ActorMeshController<RdmaManagerActor>>,
        midpoint_port: PortRef<usize>,
    }

    #[async_trait]
    impl Handler<ReplaySequence> for RdmaManagerOwnerActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: ReplaySequence,
        ) -> Result<(), anyhow::Error> {
            let port = cx.instance().port::<Option<MeshFailure>>().bind();
            msg.controller.post(cx, Unsubscribe(port.clone()));
            msg.controller
                .post(cx, GetSubscriberCount(msg.midpoint_port));
            msg.controller.post(
                cx,
                resource::Stop {
                    id: ResourceId::from_name("rdma-owner-test-stop"),
                    reason: "test: latch a whole-view stop for replay".to_string(),
                },
            );
            msg.controller.post(cx, Subscribe(port));
            Ok(())
        }
    }

    /// A manager with no available backend (ibverbs disabled, TCP fallback off)
    /// fails to initialize: the barrier terminates with a typed error, caches
    /// it, and the owner survives. The spawn-boundary race means the typed error
    /// is `SpawnFailed` if failure is visible before `spawn_service` returns, or
    /// `InitFailed` from the controller report otherwise (RMO-2/4/7).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn initialization_failure_resolves_and_is_cached() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _no_tcp = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, false);
        let _no_ib = config.override_key(crate::config::RDMA_DISABLE_IBVERBS, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 1),
                None,
                None,
            )
            .await?;
        let key: ProcMeshRef = (*proc_mesh).clone();

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        let (r1, rx1) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r1.bind())
            .await?;
        let err = rx1
            .recv()
            .await?
            .expect_err("init with no available backend must fail");
        match &err {
            // Both delivery paths carry the forced cause. If the failure is
            // visible after `Pending` is installed, the controller report renders
            // it through the `MeshFailure` display (InitFailed); if it is visible
            // during the bounded spawn-wait, it surfaces as `Status::Failed(reason)`
            // inside `ActorSpawnError`, whose display SpawnFailed captures. The
            // only failure source in this config is the missing backend, so the
            // substring must appear either way.
            RdmaInitError::InitFailed(msg) | RdmaInitError::SpawnFailed(msg) => assert!(
                msg.contains("no RDMA backend available"),
                "the forced no-backend cause must ride through on both the \
                 InitFailed (controller report) and SpawnFailed (spawn-wait) \
                 paths, got: {msg}",
            ),
            other => panic!("expected InitFailed or SpawnFailed, got {other:?}"),
        }

        // Terminal + cached: a second ensure replies from cache without
        // spawning again (RMO-2).
        let (r2, rx2) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r2.bind())
            .await?;
        let cached = rx2.recv().await?.expect_err("cached terminal error");
        assert_eq!(
            cached, err,
            "second ensure must reply the exact cached terminal error, not just \
             the same enum discriminant",
        );

        let observed = probe(&owner_handle, client, key.clone()).await;
        assert_eq!(observed.state, EntryStateTag::Failed);
        assert_eq!(observed.entry_count, 1, "a failure must not add an entry");

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// Typed source attribution (RMO-7, the linchpin) plus the `Failed`-source
    /// half of RMO-17. A live `MeshFailure` is source-local: it fails only the
    /// view whose recorded `controller_id` matches its `reporting_controller`,
    /// even though its `crashed_ranks` name a proc another pending view shares —
    /// so a stale rank-fanout implementation fails this test. `None`, missing,
    /// and unknown reporters resolve nothing (asserted BEFORE the match, so a
    /// regression cannot hide behind the later resolution). The identical report
    /// through the subscriber route is a late duplicate that cannot rewrite the
    /// cached error. Finally, killing the already-`Failed` source's real
    /// controller (a `Failed` controller terminal) invalidates the overlapping
    /// pending view by proc-view overlap, with the sibling's own subscription
    /// removed. A dropped waiter's drained reply must not bounce into the owner
    /// (RMO-5).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn mesh_failure_resolves_only_typed_source() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 3),
                None,
                None,
            )
            .await?;
        let view_a: ProcMeshRef = proc_mesh.range("procs", 0..2)?; // [0, 1], source
        let view_b: ProcMeshRef = proc_mesh.range("procs", 0..1)?; // [0], overlaps A
        let view_c: ProcMeshRef = proc_mesh.range("procs", 2..3)?; // [2], disjoint

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);
        let subject = owner_handle.actor_addr().clone();

        // Prequeue every ensure and hold behind a pause so each entry is created
        // and held before any real ack can resolve it.
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: entered_tx,
                release: release_rx,
            },
        );
        entered_rx.await.expect("owner entered Pause");

        let (ra, rxa) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_a.clone(), ra.bind())
            .await?;
        let (rb, rxb) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_b.clone(), rb.bind())
            .await?;
        let (rc, _rxc) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_c.clone(), rc.bind())
            .await?;
        let (hta, hrxa) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: view_a.clone(),
                reply: hta,
            },
        );
        let (htb, hrxb) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: view_b.clone(),
                reply: htb,
            },
        );
        let (htc, hrxc) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: view_c.clone(),
                reply: htc,
            },
        );
        let _ = release_tx.send(());

        let (_entry_a, controller_a, ctrl_a) = hrxa.await?.expect("view A held pending");
        let (_entry_b, controller_b, ctrl_b) = hrxb.await?.expect("view B held pending");
        let (_entry_c, controller_c, _ctrl_c) = hrxc.await?.expect("view C held pending");
        assert_ne!(
            controller_a, controller_b,
            "distinct views, distinct controllers"
        );
        assert_ne!(
            controller_a, controller_c,
            "distinct views, distinct controllers"
        );

        // A second waiter joins A; drop its receiver so its drained reply is
        // undeliverable and must not bounce into the owner (RMO-5).
        let (ra2, rxa2) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_a.clone(), ra2.bind())
            .await?;
        wait_for_remaining(&owner_handle, client, view_a.clone(), 1).await;
        let a_probe = probe(&owner_handle, client, view_a.clone()).await;
        assert_eq!(
            a_probe.waiter_count, 2,
            "second ensure joined A as a waiter"
        );
        assert_eq!(
            a_probe.controller_id,
            Some(controller_a.clone()),
            "probe exposes A's recorded controller id (the RMO-7 match key)",
        );
        drop(rxa2);

        // Ignore-before-match: an outer `None` heartbeat, a missing reporter, and
        // an unknown reporter resolve nothing. An owner round-trip drains the
        // queue behind them, then every view must still be pending.
        owner_handle.post(client, None::<MeshFailure>);
        owner_handle.post(client, make_mesh_failure(None, subject.clone()));
        let unknown = owner_handle.actor_addr().id().clone();
        owner_handle.post(client, make_mesh_failure(Some(unknown), subject.clone()));
        let _ = owner_subscriber_count(&owner_handle, client, ctrl_a.clone()).await;
        for v in [&view_a, &view_b, &view_c] {
            assert_eq!(
                probe(&owner_handle, client, v.clone()).await.state,
                EntryStateTag::Pending,
                "no pending view resolves on None / missing / unknown reporters",
            );
        }
        assert_eq!(
            probe(&owner_handle, client, view_a.clone())
                .await
                .waiter_count,
            2,
            "A's waiters are intact before the matching report",
        );

        // The matching report resolves ONLY A, draining both its waiters. Its
        // `crashed_ranks = [0]` name B's proc; source-local attribution must
        // leave B (and disjoint C) pending.
        owner_handle.post(
            client,
            make_mesh_failure(Some(controller_a.clone()), subject.clone()),
        );
        let a_err = rxa
            .recv()
            .await?
            .expect_err("view A resolves to a typed failure");
        assert!(
            matches!(a_err, RdmaInitError::InitFailed(_)),
            "controller report resolves as InitFailed, got {a_err:?}",
        );
        assert_eq!(
            probe(&owner_handle, client, view_a.clone()).await.state,
            EntryStateTag::Failed,
        );
        assert_eq!(
            probe(&owner_handle, client, view_b.clone()).await.state,
            EntryStateTag::Pending,
            "B (overlapping proc) is untouched by A's live report",
        );
        assert_eq!(
            probe(&owner_handle, client, view_c.clone()).await.state,
            EntryStateTag::Pending,
        );

        // The identical report through the subscriber route is a late duplicate:
        // A is terminal, so its cached error is unchanged.
        owner_handle.post(
            client,
            Some(make_mesh_failure(
                Some(controller_a.clone()),
                subject.clone(),
            )),
        );
        let (rca, rcarx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_a.clone(), rca.bind())
            .await?;
        assert_eq!(
            rcarx.recv().await?.expect_err("A cached terminal error"),
            a_err,
            "A's cached error is immutable across a late subscriber-route duplicate",
        );

        // `Failed`-source half of RMO-17: unsubscribe B so it cannot fail via its
        // own subscriber, then terminate A's real controller. The owner uses A's
        // retained controller id and A's `ProcMeshRef` to fail B by proc-view
        // overlap; disjoint C stays pending.
        let (ack_tx, ack_rx) = oneshot::channel();
        owner_handle.post(
            client,
            UnsubscribeController {
                controller: ctrl_b.clone(),
                ack: ack_tx,
            },
        );
        ack_rx.await?;
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, ctrl_b).await,
            0,
            "B unsubscribed before the overlap proof",
        );

        let ctrl_a_handle = ctrl_a.downcast_handle(client).expect("local A controller");
        ctrl_a_handle.kill("test: kill A controller")?;
        let b_err = timeout(Duration::from_secs(30), rxb.recv())
            .await
            .expect("B resolved after A-controller kill")?
            .expect_err("B fails via overlap from the Failed source");
        assert!(
            matches!(b_err, RdmaInitError::Supervision(_)),
            "B: {b_err:?}"
        );
        // A is already a `Failed` source; killing its controller drives the
        // Failed-source + Failed-controller cell of RMO-17. A's controller
        // completes `Failed`.
        let a_status = ctrl_a_handle.await;
        assert!(
            matches!(a_status, ActorStatus::Failed(_)),
            "A controller failed after kill: {a_status:?}",
        );
        assert_eq!(
            probe(&owner_handle, client, view_c.clone()).await.state,
            EntryStateTag::Pending,
            "disjoint C untouched by A-controller overlap",
        );

        // A's cached error is still exactly the original InitFailed.
        let (rca2, rca2rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_a.clone(), rca2.bind())
            .await?;
        assert_eq!(rca2rx.recv().await?.expect_err("A cached"), a_err);

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// Once a view is `Ready`, a live failure naming its controller is absorbed
    /// and the cached `Ok` is unchanged — the owner is an initialization barrier,
    /// not a health oracle (RMO-2). `Ready` retains its `controller_id` (for
    /// RMO-17 overlap), but a live report is source-local and acts only on a
    /// `Pending` entry, so it resolves nothing on either delivery route. Also
    /// pins the subscription lifetime around the success transition: exactly one
    /// subscriber while `Pending`, none once `Ready` (RMO-16).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn ready_absorbs_late_mesh_failure() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 1),
                None,
                None,
            )
            .await?;
        let key: ProcMeshRef = (*proc_mesh).clone();

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);
        let subject = owner_handle.actor_addr().clone();

        // Capture the controller id while `Pending` (it is also retained in
        // `Ready`), then let the entry resolve to `Ready`.
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: entered_tx,
                release: release_rx,
            },
        );
        entered_rx.await.expect("owner entered Pause");

        let (r, rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r.bind())
            .await?;
        let (ht, hrx) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: key.clone(),
                reply: ht,
            },
        );
        let _ = release_tx.send(());

        let (entry, controller, ctrl) = hrx.await?.expect("held pending");
        // Held `Pending`: exactly one subscriber (the owner) is registered.
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, ctrl.clone()).await,
            1,
            "a held pending entry has exactly one subscriber",
        );
        wait_for_remaining(&owner_handle, client, key.clone(), 1).await;
        owner_handle.post(client, ReadyAck { entry });
        assert_eq!(rx.recv().await?, Ok(()));
        assert_eq!(
            probe(&owner_handle, client, key.clone()).await.state,
            EntryStateTag::Ready,
        );
        // The success transition posted `Unsubscribe`; an owner-ordered count
        // query, FIFO behind it at the controller, now reports 0.
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, ctrl).await,
            0,
            "reaching Ready removes the controller subscription",
        );

        // A late failure naming the former controller is absorbed on both routes.
        owner_handle.post(
            client,
            make_mesh_failure(Some(controller.clone()), subject.clone()),
        );
        owner_handle.post(client, Some(make_mesh_failure(Some(controller), subject)));
        assert_eq!(
            probe(&owner_handle, client, key.clone()).await.state,
            EntryStateTag::Ready,
        );

        let (r2, rx2) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r2.bind())
            .await?;
        assert_eq!(rx2.recv().await?, Ok(()), "Ready is cached");

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// Subscription lifetime end to end (RMO-15/16), proved without timing. A held
    /// pending entry has one subscriber and ignores an outer `None` heartbeat.
    /// Driving `Unsubscribe -> count -> resource::Stop -> Subscribe` from the
    /// owner to the same controller latches a whole-view non-error report with no
    /// subscriber present, so only the `Subscribe` replay-before-insert resolves
    /// the waiter; the midpoint count is `0`. The resolving `transition` then
    /// removes that re-subscription (final owner-ordered count `0`). Capturing the
    /// exact latched report through a temporary test subscription lets us replay
    /// it through both handler routes as late duplicates that leave the cached
    /// error and the owner unchanged.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn subscription_lifetime_handles_heartbeat_replay_and_duplicates() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 1),
                None,
                None,
            )
            .await?;
        let key: ProcMeshRef = (*proc_mesh).clone();

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        // Prequeue ensure + hold behind a pause so the entry is created and held
        // (sentinel added) before any real `ReadyAck` can resolve it (RMO-15).
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: entered_tx,
                release: release_rx,
            },
        );
        entered_rx.await.expect("owner entered Pause");

        // Hold a real pending entry and capture its real controller.
        let (r, rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r.bind())
            .await?;
        let (ht, hrx) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: key.clone(),
                reply: ht,
            },
        );
        let _ = release_tx.send(());
        let (_entry, _controller_id, controller) = hrx.await?.expect("held pending");

        // The production ensure path registered exactly one subscriber (RMO-15/16).
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, controller.clone()).await,
            1,
            "ensure registered the owner as the sole subscriber",
        );

        // An outer `None` heartbeat resolves nothing.
        owner_handle.post(client, None::<MeshFailure>);
        assert_eq!(
            probe(&owner_handle, client, key.clone()).await.state,
            EntryStateTag::Pending,
        );

        // Replay path (no timing): only the final `Subscribe` replay can resolve
        // the held entry; the midpoint count proves the owner was unsubscribed
        // before the latch.
        let (mid_tx, mut mid_rx) = client.open_port::<usize>();
        owner_handle.post(
            client,
            ReplaySequence {
                controller: controller.clone(),
                midpoint_port: mid_tx.bind(),
            },
        );
        assert_eq!(
            mid_rx.recv().await?,
            0,
            "subscriber removed before the latch"
        );

        let err = timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("replay resolved the waiter")?
            .expect_err("replayed whole-view stop resolves the barrier");
        assert!(
            matches!(err, RdmaInitError::InitFailed(_)),
            "replay: {err:?}"
        );
        assert_eq!(
            probe(&owner_handle, client, key.clone()).await.state,
            EntryStateTag::Failed,
        );

        // `transition` posted `Unsubscribe` on resolution; the final owner-ordered
        // count is `0`, proving replay-before-insert did not leak the
        // re-subscription.
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, controller.clone()).await,
            0,
            "transition's Unsubscribe removed the replay subscription",
        );

        // Capture the exact latched report with a temporary test subscription,
        // then remove it, and prove both late-duplicate routes are absorbed.
        let (cap_tx, mut cap_rx) = client.open_port::<Option<MeshFailure>>();
        controller.post(client, Subscribe(cap_tx.bind()));
        let latched = timeout(Duration::from_secs(30), cap_rx.recv())
            .await
            .expect("controller replayed the latched report")?
            .expect("latched report is Some");
        controller.post(client, Unsubscribe(cap_tx.bind()));
        // Removal barrier: a client-originated count query is FIFO-ordered behind
        // the client's `Unsubscribe(cap_tx)` at the controller, so it observes the
        // capture port removed before the duplicates are replayed.
        let (cap_cnt_tx, mut cap_cnt_rx) = client.open_port::<usize>();
        controller.post(client, GetSubscriberCount(cap_cnt_tx.bind()));
        assert_eq!(
            cap_cnt_rx.recv().await?,
            0,
            "capture subscription removed before replaying duplicates",
        );

        owner_handle.post(client, latched.clone());
        owner_handle.post(client, Some(latched));
        let (r2, r2rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r2.bind())
            .await?;
        assert_eq!(
            r2rx.recv().await?.expect_err("cached"),
            err,
            "cached error immutable across late duplicates on both routes",
        );

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// The load-bearing RMO-17 proof, on the real manager-mesh controller and its
    /// real cleanup path. Cleanly stopping a `Ready` controller invalidates every
    /// still-`Pending` view sharing one of its per-proc managers, and only those;
    /// sibling subscriptions are removed first so the overlap cannot leak through
    /// the subscriber stream. Killing a disjoint pending view's controller then
    /// proves the error-terminal branch (RMO-14). Every terminal input keeps the
    /// owner alive (RMO-4) and leaves a terminal source and disjoint views
    /// unchanged (RMO-2).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn ready_controller_termination_resolves_only_overlapping_pending_views()
    -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 3),
                None,
                None,
            )
            .await?;
        let view_a: ProcMeshRef = proc_mesh.range("procs", 0..2)?; // [0, 1]
        let view_b: ProcMeshRef = proc_mesh.range("procs", 0..1)?; // [0]
        let view_c: ProcMeshRef = proc_mesh.range("procs", 1..2)?; // [1]
        let view_d: ProcMeshRef = proc_mesh.range("procs", 2..3)?; // [2]

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);

        // Hold A pending to capture its real controller, then release its
        // sentinel so A reaches `Ready` (which unsubscribes it). Prequeue ensure
        // + hold behind a pause so A is held before any real `ReadyAck` lands.
        let (a_entered_tx, a_entered_rx) = oneshot::channel();
        let (a_release_tx, a_release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: a_entered_tx,
                release: a_release_rx,
            },
        );
        a_entered_rx.await.expect("owner entered Pause (A)");
        let (ra, _rxa) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_a.clone(), ra.bind())
            .await?;
        let (hta, hrxa) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: view_a.clone(),
                reply: hta,
            },
        );
        let _ = a_release_tx.send(());
        let (entry_a, _ctrl_a_id, ctrl_a) = hrxa.await?.expect("A held pending");
        wait_for_remaining(&owner_handle, client, view_a.clone(), 1).await;
        owner_handle.post(client, ReadyAck { entry: entry_a });
        wait_until_state(&owner_handle, client, view_a.clone(), EntryStateTag::Ready).await;
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, ctrl_a.clone()).await,
            0,
            "a Ready controller is unsubscribed (RMO-16)",
        );
        let ctrl_a_handle = ctrl_a.downcast_handle(client).expect("local A controller");

        // Hold B[0], C[1] (both share a proc with A) and disjoint D[2] pending.
        // Prequeue their ensures + holds behind a pause so each is held before
        // any real `ReadyAck` lands.
        let (bcd_entered_tx, bcd_entered_rx) = oneshot::channel();
        let (bcd_release_tx, bcd_release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: bcd_entered_tx,
                release: bcd_release_rx,
            },
        );
        bcd_entered_rx.await.expect("owner entered Pause (B/C/D)");
        let (rb, rxb) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_b.clone(), rb.bind())
            .await?;
        let (rc, rxc) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_c.clone(), rc.bind())
            .await?;
        let (rd, rxd) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_d.clone(), rd.bind())
            .await?;
        let (htb, hrxb) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: view_b.clone(),
                reply: htb,
            },
        );
        let (htc, hrxc) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: view_c.clone(),
                reply: htc,
            },
        );
        let (htd, hrxd) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: view_d.clone(),
                reply: htd,
            },
        );
        let _ = bcd_release_tx.send(());
        let (_eb, _cb_id, ctrl_b) = hrxb.await?.expect("B held pending");
        let (_ec, _cc_id, ctrl_c) = hrxc.await?.expect("C held pending");
        let (_ed, _cd_id, ctrl_d) = hrxd.await?.expect("D held pending");

        // Remove B and C subscriptions (owner-originated) so their failure can
        // only come from A-controller overlap, not their own subscriber stream.
        for ctrl in [ctrl_b.clone(), ctrl_c.clone()] {
            let (ack_tx, ack_rx) = oneshot::channel();
            owner_handle.post(
                client,
                UnsubscribeController {
                    controller: ctrl,
                    ack: ack_tx,
                },
            );
            ack_rx.await?;
        }
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, ctrl_b.clone()).await,
            0,
            "B unsubscribed before the overlap proof",
        );
        assert_eq!(
            owner_subscriber_count(&owner_handle, client, ctrl_c).await,
            0,
            "C unsubscribed before the overlap proof",
        );

        // Cleanly stop A's real controller; its cleanup stops the managers shared
        // with B[0] and C[1]. The owner learns of A's terminal via direct child
        // supervision and fails B and C by proc-view overlap (RMO-17).
        ctrl_a_handle.stop("test: stop A controller")?;
        let b_err = timeout(Duration::from_secs(30), rxb.recv())
            .await
            .expect("B resolved after A-controller stop")?
            .expect_err("B fails via overlap");
        assert!(
            matches!(b_err, RdmaInitError::Supervision(_)),
            "B: {b_err:?}"
        );
        let c_err = timeout(Duration::from_secs(30), rxc.recv())
            .await
            .expect("C resolved after A-controller stop")?
            .expect_err("C fails via overlap");
        assert!(
            matches!(c_err, RdmaInitError::Supervision(_)),
            "C: {c_err:?}"
        );
        // A's controller completed a clean stop (the clean-terminal branch). The
        // B/C waiter errors above, not this join, are the proof that the owner
        // processed A's terminal child event.
        let a_status = ctrl_a_handle.await;
        assert!(
            matches!(a_status, ActorStatus::Stopped(_)),
            "A controller stopped cleanly: {a_status:?}",
        );

        // D is disjoint ([2]) and stays pending; A stays cached `Ready`.
        assert_eq!(
            probe(&owner_handle, client, view_d.clone()).await.state,
            EntryStateTag::Pending,
            "disjoint D untouched by A-controller overlap",
        );
        let (ra2, ra2rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, view_a.clone(), ra2.bind())
            .await?;
        assert_eq!(ra2rx.recv().await?, Ok(()), "A source stays cached Ready");

        // Self-termination path, error-terminal branch: kill D's own controller
        // so it completes as `Failed` (a crash, not a clean stop). The owner gates
        // on `is_terminal()`, so `Failed` drives the same resolution A's clean
        // `Stopped` did; D resolves directly (RMO-14).
        let ctrl_d_handle = ctrl_d.downcast_handle(client).expect("local D controller");
        ctrl_d_handle.kill("test: kill D controller")?;
        let d_err = timeout(Duration::from_secs(30), rxd.recv())
            .await
            .expect("D resolved after its controller was killed")?
            .expect_err("D fails on its own controller termination");
        assert!(
            matches!(d_err, RdmaInitError::Supervision(_)),
            "D: {d_err:?}"
        );
        // D's controller completed as `Failed` (the error-terminal branch).
        let d_status = ctrl_d_handle.await;
        assert!(
            matches!(d_status, ActorStatus::Failed(_)),
            "D controller failed after kill: {d_status:?}",
        );

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }

    /// An invalid-reference return (such as a readiness-cast bounce to a dead
    /// rank) can reach the owner; the default policy would `bail!` and kill the
    /// owner. The override swallows it (RMO-4) and does not mutate a held pending
    /// entry, which still resolves normally afterwards. Delivered through the
    /// owner's real delivery-failure dispatch, not a direct callback call.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn invalid_reference_does_not_kill_owner() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let cx = context().await;
        let client = cx.actor_instance;

        let mut host_mesh = HostMesh::local_in_process().await?;
        let proc_mesh = host_mesh
            .spawn(
                client,
                "rdma_procs",
                hyperactor_mesh::extent!(procs = 1),
                None,
                None,
            )
            .await?;
        let key: ProcMeshRef = (*proc_mesh).clone();

        let owner = RdmaManagerOwnerActor::new((), Flattrs::default()).await?;
        let owner_handle = client.spawn(owner);
        let owner_addr = owner_handle.actor_addr().clone();

        // Hold a real entry pending so the swallow is proved non-mutating.
        // Prequeue ensure + hold behind a pause so the entry is held before any
        // real `ReadyAck` lands.
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        owner_handle.post(
            client,
            Pause {
                entered: entered_tx,
                release: release_rx,
            },
        );
        entered_rx.await.expect("owner entered Pause");
        let (r, rx) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r.bind())
            .await?;
        let (ht, hrx) = oneshot::channel();
        owner_handle.post(
            client,
            HoldPending {
                key: key.clone(),
                reply: ht,
            },
        );
        let _ = release_tx.send(());
        let (entry, _controller_id, _controller) = hrx.await?.expect("held pending");

        // Build an invalid-reference return and deliver it through the owner's
        // real delivery-failure dispatch (blanket `Handler<Undeliverable>`).
        let mut env = MessageEnvelope::new(
            owner_addr.clone(),
            owner_addr.port_addr(0.into()),
            wirevalue::Any::serialize(&0u64).unwrap(),
            Flattrs::new(),
        );
        env.push_delivery_failure(DeliveryFailure::new(InvalidReference::new(
            owner_addr.clone(),
            InvalidReferenceReason::ActorNotExist,
        )));
        let undeliverable_port =
            PortRef::<Undeliverable<MessageEnvelope>>::attest_handler_port(&owner_addr);
        undeliverable_port.post(client, Undeliverable::Returned(env));

        // The swallow did not mutate the held entry: it stays pending.
        wait_for_remaining(&owner_handle, client, key.clone(), 1).await;
        assert_eq!(
            probe(&owner_handle, client, key.clone()).await.state,
            EntryStateTag::Pending,
            "an invalid-reference return must not resolve a pending entry",
        );

        // Release the sentinel; the entry resolves normally and the owner still
        // serves a cached repeat ensure.
        owner_handle.post(client, ReadyAck { entry });
        assert_eq!(
            rx.recv().await?,
            Ok(()),
            "held entry resolves normally after the swallow",
        );
        let (r2, rx2) = client.open_once_port::<Result<(), RdmaInitError>>();
        owner_handle
            .ensure_rdma_manager(client, key.clone(), r2.bind())
            .await?;
        assert_eq!(
            rx2.recv().await?,
            Ok(()),
            "owner alive and caching after an invalid-reference return",
        );

        owner_handle.stop("test complete")?;
        let _ = owner_handle.await;
        host_mesh.shutdown(client).await?;
        Ok(())
    }
}
