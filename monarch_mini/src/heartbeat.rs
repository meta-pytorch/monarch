/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Delegated heartbeating for the QUIC transport.
//!
//! At high fan-out a parent `B` cannot afford to actively heartbeat tens of
//! thousands of children. This module offloads that cost: `B` heartbeats at most
//! [`max_direct_children`] children directly; the rest are *delegated* to sibling
//! children, who prove the delegated child's liveness on `B`'s behalf and report a
//! running `+/-` diff of the connections they cover. A delegated link is silent on
//! the physical QUIC connection (no app heartbeat, QUIC keep-alive / idle-timeout
//! disabled) — liveness rides the sibling side channel instead.
//!
//! All of this lives in the transport. From the command loop's point of view an
//! actor still has N child connections and still receives the same `Establish` /
//! `PublishRoutes` / `Severed` notices; only *how liveness is proven* changes.
//!
//! ## Coroutine roles
//!
//! Every QUIC connection runs one `heartbeat_task`, whose role is fixed by the
//! [`ConnectionRef`]: a [`ConnectionRef::ChildConnection`] (role
//! [`Role::Parent`]) runs the [`parent_task`] (detect the peer child's death; may
//! delegate it); a [`ConnectionRef::ParentConnection`] (role [`Role::Child`]) runs
//! the [`child_task`] (prove liveness to the parent; may host coverage for
//! siblings). Each owns exactly one primary timeout — "did the thing I watch arrive
//! in time?" — and delegation just moves *which* task owns the timeout for a link.
//!
//! See `HEARTBEAT_DELEGATION_DESIGN.md` for the full model.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::time::Instant;

use crate::Role;
use crate::connection::ConnectionRef;
use crate::connection::sever;
use crate::ctx::Command;
use crate::ctx::Key;

/// The identity of one parent→child connection, assigned ctx-globally by the
/// Parent side at spawn. Names *which delegated link* a message concerns,
/// independent of whether actor names are known yet.
pub(crate) type ConnectionId = u64;

/// Control channel to one `heartbeat_task`.
type Handle = mpsc::UnboundedSender<HeartbeatEvent>;

// ---------------------------------------------------------------------------
// Tunables (env). See `HEARTBEAT_DELEGATION_DESIGN.md` §11.
// ---------------------------------------------------------------------------

const DEFAULT_MAX_DIRECT_CHILDREN: usize = 256;

/// How often each side emits a heartbeat, and how long a side waits for any beat
/// before declaring the connection broken. The timeout is several intervals so a
/// stray scheduling delay doesn't trip it. Both are reused for physical and
/// side-channel beats, and tunable via `MM_QUIC_HEARTBEAT_INTERVAL_MS` /
/// `MM_QUIC_HEARTBEAT_TIMEOUT_MS`: at very high fan-out the single-threaded root
/// cannot service tens of thousands of beats on a 5s cadence, so a longer interval
/// (with a proportionally longer timeout) cuts steady-state load without weakening
/// liveness. (Delegation exists to reduce that load further.)
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(20);

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

/// How often each side emits a heartbeat beat.
pub(crate) fn heartbeat_interval() -> Duration {
    std::env::var("MM_QUIC_HEARTBEAT_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL)
}

/// How long a side waits for any beat before declaring the connection broken.
pub(crate) fn heartbeat_timeout() -> Duration {
    std::env::var("MM_QUIC_HEARTBEAT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT)
}

/// The number of children a parent heartbeats directly. Any beyond this are
/// delegated to (balanced across) the direct ones, so the parent's steady-state
/// heartbeat load is bounded by this regardless of fan-out.
fn max_direct_children() -> usize {
    env_usize("MM_QUIC_MAX_DIRECT_CHILDREN", DEFAULT_MAX_DIRECT_CHILDREN)
}

/// The heartbeat tunables, resolved once and carried by [`Heartbeats`] into every
/// coroutine it spawns. Injecting them (rather than reading env per call) keeps the
/// policy in one place and lets tests drive the *real* coroutines with short
/// intervals / a low delegation threshold without touching process-global env.
#[derive(Clone, Copy)]
pub(crate) struct HeartbeatConfig {
    pub(crate) interval: Duration,
    pub(crate) timeout: Duration,
    pub(crate) max_direct_children: usize,
}

impl HeartbeatConfig {
    /// Resolve from the environment (the production path).
    fn from_env() -> Self {
        Self {
            interval: heartbeat_interval(),
            timeout: heartbeat_timeout(),
            max_direct_children: max_direct_children(),
        }
    }
}

/// Whether `ident` is side-channel addressable — i.e. it carries a non-empty
/// gateway `@tag` a sibling can dial. A served (`quic serve`) child has one; a
/// specifier-less root ident does not.
fn is_addressable(ident: &[u8]) -> bool {
    matches!(ident.iter().rposition(|&b| b == b'@'), Some(pos) if pos + 1 < ident.len())
}

/// A cheap, cloneable handle to the per-context delegated-heartbeat state. Owns the
/// shared table behind an `Rc<RefCell<>>` (single-threaded — LocalSet). The quic
/// transport holds one and hands clones to the heartbeat coroutines; it is also the
/// entry point for routing an inbound side-channel beat/ack (see [`Self::deliver`]).
#[derive(Clone)]
pub(crate) struct Heartbeats {
    shared: Rc<RefCell<HeartbeatShared>>,
    config: HeartbeatConfig,
}

impl Heartbeats {
    pub(crate) fn new() -> Self {
        Self::with_config(HeartbeatConfig::from_env())
    }

    /// Construct with explicit tunables (used by tests to drive the real coroutines
    /// with short intervals / a low delegation threshold).
    pub(crate) fn with_config(config: HeartbeatConfig) -> Self {
        Self {
            shared: Rc::new(RefCell::new(HeartbeatShared::new(
                config.max_direct_children,
            ))),
            config,
        }
    }

    /// Spawn the heartbeat coroutine for one freshly-established connection, and
    /// return the event sender its reader and writer feed (inbound beats, `Establish`
    /// snoops, reader-closed). `beats` is where the coroutine's own outbound beats go
    /// (to the writer); `send_beat` sends side-channel beats to siblings; `loop_tx`
    /// is used only to emit `Severed` on a hard failure.
    pub(crate) fn spawn<S: SendBeat>(
        &self,
        connection: ConnectionRef,
        dialed: bool,
        beats: mpsc::UnboundedSender<Heartbeat>,
        send_beat: S,
        loop_tx: mpsc::UnboundedSender<Command>,
    ) -> mpsc::UnboundedSender<HeartbeatEvent> {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        tokio::task::spawn_local(heartbeat_task(HeartbeatCtx {
            connection,
            dialed,
            beats,
            events: events_rx,
            handle: events_tx.clone(),
            shared: self.shared.clone(),
            config: self.config,
            send_beat,
            loop_tx,
        }));
        events_tx
    }

    /// Deliver an inbound sibling side-channel message to the Child-side coroutine of
    /// the connection where `recipient` is the child. Called directly by the quic
    /// side-channel reader — heartbeats never pass through ctx. Dropped if no such
    /// connection is registered yet (the next beat retries).
    pub(crate) fn deliver(
        &self,
        recipient: &[u8],
        from: Vec<u8>,
        conn_id: ConnectionId,
        kind: BeatKind,
    ) {
        let shared = self.shared.borrow();
        if let Some(handle) = shared.child_handle(recipient) {
            let _ = handle.send(HeartbeatEvent::Side(from, conn_id, kind));
        }
    }
}

// ---------------------------------------------------------------------------
// Wire types (serialized by `framing.rs`)
// ---------------------------------------------------------------------------

/// The wire body of a heartbeat probe, serialized bare onto a connection's
/// dedicated heartbeat stream (see [`crate::framing::write_heartbeat`]). `FromChild`
/// / `FromParent` are the *real*, request/response liveness beats — a Child sends
/// `FromChild`, its Parent answers with `FromParent` — and they drive delegation.
/// Riding their own stream, a beat is never delayed by a large data transfer and no
/// synthesized "still here" backstop is needed.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) enum Heartbeat {
    /// Child → parent: the running diff of connections this child covers for the
    /// parent.
    FromChild {
        cover_add: Vec<ConnectionId>,
        cover_del: Vec<ConnectionId>,
    },
    /// Parent → child: a delegation instruction, or `None` for a plain beat.
    /// `None` never revokes — a child reverts on its own ack-timeout, a parent on
    /// `ResumeHeartbeat`.
    FromParent { delegate: Option<Delegate> },
}

/// A delegation instruction carried on a `FromParent` beat: "you are connection
/// `connection_id`; beat `gateway_tag(sibling_ident)` to prove your liveness."
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Delegate {
    pub(crate) connection_id: ConnectionId,
    pub(crate) sibling_ident: Vec<u8>,
}

/// What a sibling side-channel heartbeat message is.
#[derive(Serialize, Deserialize, Clone, Copy)]
pub(crate) enum BeatKind {
    /// Delegated child → delegate: prove liveness.
    Beat,
    /// Delegate → delegated child: acknowledge a beat.
    Ack,
    /// Delegate → delegated child: I am tearing down — stop delegating to me and
    /// heartbeat your parent directly again.
    Release,
}

// ---------------------------------------------------------------------------
// Control channels
// ---------------------------------------------------------------------------

/// Events into a `heartbeat_task`. A task is either a Parent or a Child, so it only
/// ever sees the events for its role.
pub(crate) enum HeartbeatEvent {
    /// An inbound physical heartbeat from the peer (a Parent-side task receives the
    /// [`Heartbeat::FromChild`] variant, a Child-side task the
    /// [`Heartbeat::FromParent`] variant; each ignores the other).
    ReceivedHeartbeat(Heartbeat),
    /// Our outgoing `Establish` (from the writer): our own ident.
    EstablishLocal { local_ident: Vec<u8> },
    /// The peer's `Establish` (from the reader): the peer's ident.
    EstablishPeer { peer_ident: Vec<u8> },
    /// The reader hit error/EOF (hard transport close).
    ReaderClosed,

    /// To a Parent-side task: a sibling now covers this connection, so pause
    /// watching it directly (its delegate proves it live).
    PauseHeartbeat,
    /// To a Parent-side task: no sibling covers this connection any more, so resume
    /// watching it directly.
    ResumeHeartbeat,

    /// To a Child-side task: an inbound sibling side-channel message `(from, conn_id,
    /// kind)`. [`BeatKind::Beat`] is a delegated child beating us (we host its
    /// coverage); [`BeatKind::Ack`] is our delegate acking our beat;
    /// [`BeatKind::Release`] is our delegate tearing down (revert to direct).
    Side(Vec<u8>, ConnectionId, BeatKind),
}

// ---------------------------------------------------------------------------
// Shared state (one per QuicTransport / context)
// ---------------------------------------------------------------------------

/// Where one child connection sits in the delegation lifecycle, from the parent's
/// point of view. [`ParentPool`] derives its accounting from this (whether we beat the
/// child directly, and the coverage it consumes from a sibling), so no caller pokes
/// that accounting directly.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ChildStatus {
    /// Attached; its `Establish` (ident) has not arrived yet.
    NotEstablished,
    /// Established and beating us directly. Watched directly, and a valid delegate for
    /// its siblings (it may itself cover some — see `SiblingInfo::requested_covers`).
    Direct,
    /// We asked it to delegate its liveness to its `sibling`; still watched directly at
    /// the loop level (it is about to go silent), but already treated as offloaded for
    /// the budget, and not itself offered as a delegate.
    Delegating,
    /// Its `sibling` now covers it; it is silent to us (paused).
    Paused,
}

/// A parent's record of one child connection. Owned by [`ParentPool`], the single
/// source of truth for it: the keeper set and each target's coverage are *derived*
/// from these fields as statuses transition, never poked by callers.
struct SiblingInfo {
    /// The child's ident, once its `Establish` is snooped. `Some` implies the child
    /// has established; whether it is *addressable* (has a dialable gateway tag) is
    /// derived from it.
    ident: Option<Vec<u8>>,
    /// Whether the parent dialed this child (join); only a dialed+served link may
    /// serve as a delegate.
    dialed: bool,
    status: ChildStatus,
    /// The delegate (a sibling keeper) we offloaded this child to; meaningful only
    /// while `Delegating` / `Paused`.
    delegated_to: Option<ConnectionId>,
}

impl SiblingInfo {
    /// Whether this child may be offloaded to a delegate right now: it must be a
    /// live direct child, dialed by us, and addressable (a sibling needs a dialable
    /// gateway tag to reach it). A child already chosen as a delegate target
    /// (a keeper) is excluded by the caller, not here.
    fn is_delegable(&self) -> bool {
        self.status == ChildStatus::Direct
            && self.dialed
            && self.ident.as_deref().is_some_and(is_addressable)
    }
}

/// The parent's delegate targets: the (at most `max_direct`) children it keeps
/// heartbeating directly, each labelled with how many delegated siblings it
/// currently covers. Every new delegate is handed to the least-loaded target, so
/// coverage stays balanced. Capping the target count at `max_direct` is what makes
/// the parent's direct-heartbeat load respect `max_direct`: the targets are the
/// only permanently-direct children, and everything else is delegated onto them.
///
/// Internally a binary min-heap keyed by cover count: `heap[0]` is always a
/// least-loaded target. An index map (`pos`) locates any target in the array in O(1)
/// so its count can be re-heaped in place. Add, remove, and re-charge are all
/// O(log n); reading the least-loaded target is O(1). All the heap machinery is
/// contained here; callers only add/remove targets, read the least-loaded one, and
/// report a `+/-1` cover-count change.
#[derive(Default)]
struct DelegateTargets {
    /// Cap on the number of targets — i.e. on children heartbeated directly.
    max_direct: usize,
    /// Target ids in min-heap order by cover count; `heap[0]` is least loaded.
    heap: Vec<ConnectionId>,
    /// id -> its index in `heap`.
    pos: HashMap<ConnectionId, usize>,
    /// id -> its current cover count (the heap key).
    count: HashMap<ConnectionId, usize>,
}

impl DelegateTargets {
    fn new(max_direct: usize) -> Self {
        Self {
            max_direct,
            ..Default::default()
        }
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn is_full(&self) -> bool {
        self.heap.len() >= self.max_direct
    }

    fn contains(&self, id: ConnectionId) -> bool {
        self.pos.contains_key(&id)
    }

    fn load_of(&self, id: ConnectionId) -> usize {
        self.count.get(&id).copied().unwrap_or(0)
    }

    /// The least-loaded target, or `None` if there are none.
    fn least_loaded(&self) -> Option<ConnectionId> {
        self.heap.first().copied()
    }

    /// Add `id` as a target with zero cover count. Caller ensures `!is_full()` and
    /// that `id` is not already present. Push at the end and sift up (zero is the
    /// minimum key, so it bubbles to the root).
    fn add(&mut self, id: ConnectionId) {
        debug_assert!(!self.contains(id), "target added twice");
        let i = self.heap.len();
        self.heap.push(id);
        self.pos.insert(id, i);
        self.count.insert(id, 0);
        self.heap_up(i);
    }

    /// Drop a target that is gone; the children it covered fall back on their own.
    /// Move the last element into the hole and re-heap from there.
    fn remove(&mut self, id: ConnectionId) {
        let Some(i) = self.pos.remove(&id) else {
            return;
        };
        self.count.remove(&id);
        let last = self.heap.len() - 1;
        self.heap.swap(i, last);
        self.heap.pop();
        if i < self.heap.len() {
            self.pos.insert(self.heap[i], i);
            self.reheap(i);
        }
    }

    /// Report that `id`'s cover count changed by `delta` (+1 when it takes on a
    /// delegate, -1 when one leaves) and restore the heap. A no-op if `id` is not a
    /// current target: a keeper can die (its slot freed by [`Self::remove`]) while
    /// children still point at it, and those children shed their coverage only
    /// afterwards.
    fn change_count_if_present(&mut self, id: ConnectionId, delta: i64) {
        let Some(&i) = self.pos.get(&id) else {
            return;
        };
        let new = (self.count[&id] as i64 + delta).max(0) as usize;
        self.count.insert(id, new);
        self.reheap(i);
    }

    // --- heap internals ---

    /// Cover count of the target at heap index `i`.
    fn count_at(&self, i: usize) -> usize {
        self.count[&self.heap[i]]
    }

    /// Swap two heap slots and fix both index-map entries.
    fn swap(&mut self, i: usize, j: usize) {
        self.heap.swap(i, j);
        self.pos.insert(self.heap[i], i);
        self.pos.insert(self.heap[j], j);
    }

    /// Restore the heap property at `i`, which may need to move in either direction
    /// (used after a removal drops an arbitrary element into the hole).
    fn reheap(&mut self, i: usize) {
        if i > 0 && self.count_at(i) < self.count_at((i - 1) / 2) {
            self.heap_up(i);
        } else {
            self.heap_down(i);
        }
    }

    /// Sift the element at `i` toward the root while it is lighter than its parent.
    fn heap_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if self.count_at(i) >= self.count_at(parent) {
                break;
            }
            self.swap(i, parent);
            i = parent;
        }
    }

    /// Sift the element at `i` toward the leaves while a child is lighter than it.
    fn heap_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        loop {
            let mut smallest = i;
            for child in [2 * i + 1, 2 * i + 2] {
                if child < n && self.count_at(child) < self.count_at(smallest) {
                    smallest = child;
                }
            }
            if smallest == i {
                break;
            }
            self.swap(i, smallest);
            i = smallest;
        }
    }
}

/// A parent's pool of child connections and the delegation state derived from them.
/// It owns the per-child [`SiblingInfo`] records and is the *only* place eligibility
/// and the keeper set are computed: callers narrate connection events (the lifecycle
/// methods below) and never touch the derived state.
struct ParentPool {
    siblings: HashMap<ConnectionId, SiblingInfo>,
    /// The children kept direct (the delegate targets), capped at `max_direct`, with
    /// their cover counts. Every child beyond these is delegated onto the
    /// least-loaded one of them. Its size *is* the direct-heartbeat budget.
    delegate_targets: DelegateTargets,
}

impl ParentPool {
    fn new(max_direct: usize) -> Self {
        Self {
            siblings: HashMap::new(),
            delegate_targets: DelegateTargets::new(max_direct),
        }
    }

    // --- connection lifecycle (the only way callers change pool state) ---

    /// A child connection was added (not yet established). We beat it directly until
    /// it establishes and is possibly delegated.
    fn add_child(&mut self, conn_id: ConnectionId, dialed: bool) {
        self.siblings.insert(
            conn_id,
            SiblingInfo {
                ident: None,
                dialed,
                status: ChildStatus::NotEstablished,
                delegated_to: None,
            },
        );
    }

    /// The child's `Establish` arrived: record its ident and treat it as a live
    /// direct child.
    fn established(&mut self, conn_id: ConnectionId, ident: Vec<u8>) {
        if let Some(info) = self.siblings.get_mut(&conn_id) {
            info.ident = Some(ident);
        }
        self.restore_direct(conn_id);
    }

    /// Decide what to do with `conn_id`, which just beat us:
    ///
    /// - If it is a keeper (delegate target), keep heartbeating it directly.
    /// - Else if the keeper set has room, make it a keeper — so the first `max_direct`
    ///   delegable children become the direct set.
    /// - Once the keeper set is full, offload it: delegate it to the least-loaded
    ///   keeper and return the [`Delegate`] instruction. `assign_delegate` charges
    ///   that keeper's cover count, rebalancing the target order.
    ///
    /// The keeper cap *is* the budget: at most `max_direct` children are ever
    /// heartbeated directly, and every other child is delegated and balanced across
    /// them. (A non-keeper only reaches the offload path once the set is full, which
    /// is exactly when there are more direct children than the cap.)
    fn delegate(&mut self, conn_id: ConnectionId) -> Option<Delegate> {
        // A keeper is heartbeated directly forever.
        if self.delegate_targets.contains(conn_id) {
            return None;
        }
        // Only a plain, established, dialed, addressable direct child is offloaded.
        if !self
            .siblings
            .get(&conn_id)
            .is_some_and(SiblingInfo::is_delegable)
        {
            return None;
        }
        // Fill the keeper set first; only once it is full do we start offloading.
        if !self.delegate_targets.is_full() {
            self.delegate_targets.add(conn_id);
            if crate::ctx::connection_debug() {
                eprintln!(
                    "MM_DELEG keeper conn_id={conn_id} keepers={}/{}",
                    self.delegate_targets.len(),
                    self.delegate_targets.max_direct,
                );
            }
            return None;
        }
        let target = self.delegate_targets.least_loaded()?;
        let sibling_ident = self.siblings.get(&target)?.ident.clone()?;
        if crate::ctx::connection_debug() {
            eprintln!(
                "MM_DELEG pick child={conn_id} -> target={target} target_load={} keepers={}",
                self.delegate_targets.load_of(target),
                self.delegate_targets.len(),
            );
        }
        // Enter `Delegating`, pointing the child at its target and charging that
        // target's cover count.
        self.assign_delegate(conn_id, target);
        Some(Delegate {
            connection_id: conn_id,
            sibling_ident,
        })
    }

    /// This child's current status, if we still track it.
    fn status(&self, conn_id: ConnectionId) -> Option<ChildStatus> {
        self.siblings.get(&conn_id).map(|i| i.status)
    }

    // --- derived-state maintenance (internal) ---
    //
    // The only derived state is a target's cover count, and it moves solely when a
    // child *crosses the delegation boundary*: entering the delegated set charges its
    // target +1 (`assign_delegate`), leaving it charges its old target -1
    // (`restore_direct`, `remove_child`). Staying on the same side of the boundary
    // (`acknowledge_delegate`, the `Delegating` → `Paused` step) moves nothing. Every
    // record is `add_child`ed before its parent task's loop runs and dropped only by
    // the single teardown `remove_child`, so each call in between finds its record
    // present.

    /// The record for `conn_id`, which every lifecycle call after `add_child` (and
    /// before `remove_child`) is guaranteed to find (see the note above).
    fn info_mut(&mut self, conn_id: ConnectionId) -> &mut SiblingInfo {
        self.siblings
            .get_mut(&conn_id)
            .expect("no record for connection (never add_child'd, or already removed)")
    }

    /// Restore `conn_id` to a live direct child (it just established, beat us directly
    /// after being delegated, or a sibling stopped covering it). If it had been
    /// delegated, its old target sheds a unit of coverage.
    fn restore_direct(&mut self, conn_id: ConnectionId) {
        let info = self.info_mut(conn_id);
        info.status = ChildStatus::Direct;
        // Only a delegated child points at a target; clearing it uncharges that target.
        if let Some(target) = info.delegated_to.take() {
            self.delegate_targets.change_count_if_present(target, -1);
        }
    }

    /// Offload `conn_id` to `target`: it enters `Delegating` and charges `target`'s
    /// cover count +1. It is still watched directly at the loop level until its
    /// delegate acks (see `acknowledge_delegate`).
    fn assign_delegate(&mut self, conn_id: ConnectionId, target: ConnectionId) {
        let info = self.info_mut(conn_id);
        info.status = ChildStatus::Delegating;
        info.delegated_to = Some(target);
        self.delegate_targets.change_count_if_present(target, 1);
    }

    /// Acknowledge that `conn_id`'s delegate now covers it: it goes silent to us
    /// (`Delegating` → `Paused`). It keeps the same target, so no cover count moves.
    fn acknowledge_delegate(&mut self, conn_id: ConnectionId) {
        self.info_mut(conn_id).status = ChildStatus::Paused;
    }

    /// Drop `conn_id` entirely — its connection is gone. If it was delegated, its
    /// target sheds a unit of coverage; if it was itself a keeper, its slot is freed
    /// so its covered children fall back to direct.
    fn remove_child(&mut self, conn_id: ConnectionId) {
        let info = self
            .siblings
            .remove(&conn_id)
            .expect("remove_child on a connection that was never add_child'd");
        if let Some(target) = info.delegated_to {
            self.delegate_targets.change_count_if_present(target, -1);
        }
        self.delegate_targets.remove(conn_id);
    }
}

/// One per [`QuicTransport`](crate::quic_transport::QuicTransport) (per context).
/// Single-threaded → held behind `Rc<RefCell<>>`.
pub(crate) struct HeartbeatShared {
    next_conn_id: ConnectionId,
    /// For each local actor, the control handle of the Child-side task on that
    /// actor's parent connection. Keyed by the actor's own ident (an actor has one
    /// parent, so at most one entry).
    children_by_ident: HashMap<Vec<u8>, Handle>,
    /// For each local parent→child connection, the control handle of its
    /// Parent-side task. Keyed by that connection's [`ConnectionId`].
    parents_by_conn_id: HashMap<ConnectionId, Handle>,
    /// For each local actor that parents quic children, its delegate pool.
    parents: HashMap<Key, ParentPool>,
    /// Number of children each parent heartbeats directly; seeded into each
    /// [`ParentPool`] as the keeper cap.
    max_direct: usize,
}

impl HeartbeatShared {
    pub(crate) fn new(max_direct: usize) -> Self {
        Self {
            next_conn_id: 0,
            children_by_ident: HashMap::new(),
            parents_by_conn_id: HashMap::new(),
            parents: HashMap::new(),
            max_direct,
        }
    }

    /// The Child-side task handle for the connection where `ident` is the child, if
    /// registered. Used to deliver inbound side-channel beats/acks.
    pub(crate) fn child_handle(&self, ident: &[u8]) -> Option<&Handle> {
        self.children_by_ident.get(ident)
    }

    fn alloc_conn_id(&mut self) -> ConnectionId {
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        id
    }

    fn pool_mut(&mut self, key: Key) -> &mut ParentPool {
        let max_direct = self.max_direct;
        self.parents
            .entry(key)
            .or_insert_with(|| ParentPool::new(max_direct))
    }

    /// Route a delegate host's coverage diff to the covered connections' Parent-side
    /// loops: a first `cover_add(X)` pauses `X`'s loop, a `cover_del(X)` resumes it.
    /// `covered` tracks this host's contributions so its teardown can resume them.
    fn signal_coverage(
        &self,
        covered: &mut HashSet<ConnectionId>,
        cover_add: Vec<ConnectionId>,
        cover_del: Vec<ConnectionId>,
    ) {
        for x in cover_add {
            if covered.insert(x) {
                let found = self.parents_by_conn_id.contains_key(&x);
                if crate::ctx::connection_debug() {
                    eprintln!("MM_COVER root pause conn_id={x} found_parent_loop={found}");
                }
                if let Some(h) = self.parents_by_conn_id.get(&x) {
                    let _ = h.send(HeartbeatEvent::PauseHeartbeat);
                }
            }
        }
        for x in cover_del {
            if covered.remove(&x) {
                if crate::ctx::connection_debug() {
                    eprintln!("MM_COVER root RESUME conn_id={x} (coverage dropped)");
                }
                if let Some(h) = self.parents_by_conn_id.get(&x) {
                    let _ = h.send(HeartbeatEvent::ResumeHeartbeat);
                }
            }
        }
    }

    /// Resume every connection a delegate host was covering (called as the host tears
    /// down, so its covered children reclaim themselves as direct).
    fn resume_covered(&self, covered: &HashSet<ConnectionId>) {
        for x in covered {
            if crate::ctx::connection_debug() {
                eprintln!("MM_COVER root RESUME(host teardown) conn_id={x}");
            }
            if let Some(h) = self.parents_by_conn_id.get(x) {
                let _ = h.send(HeartbeatEvent::ResumeHeartbeat);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Task entry point
// ---------------------------------------------------------------------------

/// Sends one sibling side-channel message out: `(recipient, from, conn_id, kind)`.
/// The caller supplies this so the heartbeat module needs no knowledge of *how*
/// beats travel — the quic transport wires in a closure over its gateway side
/// channels, and tests can substitute their own.
pub(crate) trait SendBeat: Fn(Vec<u8>, Vec<u8>, ConnectionId, BeatKind) + 'static {}
impl<F: Fn(Vec<u8>, Vec<u8>, ConnectionId, BeatKind) + 'static> SendBeat for F {}

/// Everything a `heartbeat_task` needs, bundled to keep the spawn site readable.
/// Private — a task is only ever created through [`Heartbeats::spawn`].
struct HeartbeatCtx<S: SendBeat> {
    connection: ConnectionRef,
    dialed: bool,
    /// Physical heartbeat frames this task wants its connection's writer to send.
    beats: mpsc::UnboundedSender<Heartbeat>,
    events: mpsc::UnboundedReceiver<HeartbeatEvent>,
    /// This task's own control handle, for registration in [`HeartbeatShared`].
    handle: Handle,
    shared: Rc<RefCell<HeartbeatShared>>,
    config: HeartbeatConfig,
    /// Sends a Child-side beat/ack out over a side channel (see [`SendBeat`]). The
    /// module stays ignorant of the transport that carries it.
    send_beat: S,
    /// Only used to emit `Severed` on a hard failure — the same connection-failure
    /// signal the reader uses, not a heartbeat routing path.
    loop_tx: mpsc::UnboundedSender<Command>,
}

/// Run the heartbeat coroutine for one connection, dispatching on its role.
async fn heartbeat_task<S: SendBeat>(ctx: HeartbeatCtx<S>) {
    match ctx.connection.role() {
        Role::Parent => parent_task(ctx).await,
        Role::Child => child_task(ctx).await,
    }
}

/// Await `deadline` if set, else never resolve (a parked timeout).
async fn deadline_or_park(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

// ---------------------------------------------------------------------------
// Parent side (B watching child D)
// ---------------------------------------------------------------------------

/// How the direct (normal-connection) loop ended.
enum DirectExit {
    /// The connection closed / severed; the task is done.
    Closed,
    /// The child reported covering a sibling, so it is now a delegate host: hand off
    /// to the cover loop (which always heartbeats and is never itself delegated),
    /// carrying that first coverage diff.
    BecameCover {
        cover_add: Vec<ConnectionId>,
        cover_del: Vec<ConnectionId>,
    },
}

/// A Parent-side connection is one of two kinds, and the switch between them is
/// one-way:
///
/// - **normal** — watched directly, and possibly *delegated to* a sibling
///   (`PauseHeartbeat` parks us, `ResumeHeartbeat` un-parks us); we may also ask the
///   child to delegate. This is [`parent_direct_loop`].
/// - **delegate host** — once the child reports covering a sibling it is load-bearing
///   for that sibling's liveness, so we commit to heartbeating it directly forever
///   and never delegate it (no Pause/Resume). This is [`parent_cover_loop`].
async fn parent_task<S: SendBeat>(ctx: HeartbeatCtx<S>) {
    let HeartbeatCtx {
        connection,
        dialed,
        beats,
        mut events,
        handle,
        shared,
        config,
        // A Parent-side task never originates side-channel beats (it delegates over
        // its own writer and watches physical beats), so it does not use this.
        send_beat: _,
        loop_tx,
    } = ctx;
    let key = connection.owning_actor();

    // Assign this connection's id and register it so sibling loops can send
    // `PauseHeartbeat`/`ResumeHeartbeat` here, and seed our record in the pool.
    let conn_id = {
        let mut s = shared.borrow_mut();
        let conn_id = s.alloc_conn_id();
        s.parents_by_conn_id.insert(conn_id, handle.clone());
        s.pool_mut(key).add_child(conn_id, dialed);
        conn_id
    };

    // Start as a normal connection; the first coverage report switches us — for good
    // — to being a delegate host.
    if let DirectExit::BecameCover {
        cover_add,
        cover_del,
    } = parent_direct_loop(
        connection,
        conn_id,
        key,
        config,
        &beats,
        &mut events,
        &shared,
        &loop_tx,
    )
    .await
    {
        parent_cover_loop(
            connection,
            config,
            cover_add,
            cover_del,
            &beats,
            &mut events,
            &shared,
            &loop_tx,
        )
        .await;
    }

    // Teardown: drop our record (undoing our budget/coverage contributions) and
    // deregister. (A delegate host's covered children are resumed by
    // `parent_cover_loop` itself as it exits.)
    let mut s = shared.borrow_mut();
    s.pool_mut(key).remove_child(conn_id);
    s.parents_by_conn_id.remove(&conn_id);
}

/// The **normal-connection** loop: watch the child's beats and *answer* each one. The
/// parent never beats on its own timer — it echoes every `FromChild` with a
/// `FromParent` (carrying a `Delegate` when it decides to offload the child). Because
/// the child never sends a second beat before it hears our answer (see [`child_task`]),
/// a delegate answer is always the child's *last* direct beat: no stale beat can arrive
/// after a later `PauseHeartbeat` and un-pause us. `PauseHeartbeat` parks the connection
/// (a sibling proves it live), `ResumeHeartbeat` un-parks it. Returns when the
/// connection closes, or [`DirectExit::BecameCover`] the first time the child reports
/// covering a sibling.
#[expect(
    clippy::too_many_arguments,
    reason = "each is a distinct per-connection channel/handle; bundling adds indirection"
)]
async fn parent_direct_loop(
    connection: ConnectionRef,
    conn_id: ConnectionId,
    key: Key,
    config: HeartbeatConfig,
    beats: &mpsc::UnboundedSender<Heartbeat>,
    events: &mut mpsc::UnboundedReceiver<HeartbeatEvent>,
    shared: &Rc<RefCell<HeartbeatShared>>,
    loop_tx: &mpsc::UnboundedSender<Command>,
) -> DirectExit {
    // `Some` while watching directly (a lapse severs); `None` while paused (a sibling
    // is proving the child live).
    let mut deadline = Some(Instant::now() + config.timeout);

    loop {
        tokio::select! {
            _ = deadline_or_park(deadline) => {
                // The child has gone silent (no beat within the timeout). While paused
                // the deadline is `None`, so this never fires for a covered child.
                if crate::ctx::connection_debug() {
                    eprintln!("MM_SEVER direct-loop conn_id={conn_id}");
                }
                sever(loop_tx, connection, b"quic heartbeat timeout".to_vec());
                return DirectExit::Closed;
            }
            ev = events.recv() => {
                let Some(ev) = ev else { return DirectExit::Closed };
                match ev {
                    HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromChild { cover_add, cover_del }) => {
                        // The child is beating us directly (if it had been delegated it
                        // has fallen back): refresh the deadline and reset it to direct.
                        deadline = Some(Instant::now() + config.timeout);
                        shared
                            .borrow_mut()
                            .pool_mut(key)
                            .restore_direct(conn_id);

                        if !cover_add.is_empty() {
                            // First coverage report: it is now a delegate host. Answer this
                            // beat (so the cover host paces its next one), then hand off to
                            // the cover loop, never to return here.
                            let _ = beats.send(Heartbeat::FromParent { delegate: None });
                            return DirectExit::BecameCover { cover_add, cover_del };
                        }

                        // Answer the beat, carrying a delegate instruction if we are over
                        // budget. The child sends no further direct beat until it processes
                        // this answer, so a delegate here is its last one.
                        let delegate = shared.borrow_mut().pool_mut(key).delegate(conn_id);
                        let _ = beats.send(Heartbeat::FromParent { delegate });
                    }
                    HeartbeatEvent::PauseHeartbeat => {
                        // A sibling now covers this child: stop watching it directly — but
                        // only while we are still delegating it. If it already fell back to
                        // direct (a genuine fallback beat beat us to it), keep watching.
                        let mut s = shared.borrow_mut();
                        let pool = s.pool_mut(key);
                        if crate::ctx::connection_debug() {
                            eprintln!(
                                "MM_PAUSE recv conn_id={conn_id} status={:?}",
                                pool.status(conn_id)
                            );
                        }
                        if pool.status(conn_id) == Some(ChildStatus::Delegating) {
                            pool.acknowledge_delegate(conn_id);
                            deadline = None;
                        }
                    }
                    HeartbeatEvent::ResumeHeartbeat => {
                        // No sibling covers it any more: watch it directly again.
                        shared
                            .borrow_mut()
                            .pool_mut(key)
                            .restore_direct(conn_id);
                        deadline = Some(Instant::now() + config.timeout);
                    }
                    HeartbeatEvent::EstablishPeer { peer_ident } => {
                        shared
                            .borrow_mut()
                            .pool_mut(key)
                            .established(conn_id, peer_ident);
                    }
                    HeartbeatEvent::EstablishLocal { .. } => {}
                    HeartbeatEvent::ReaderClosed => return DirectExit::Closed,
                    // These cannot reach a Parent-side loop: our child peer only ever
                    // sends `FromChild`, and `Side` messages route via
                    // `children_by_ident` to Child-side loops only.
                    HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromParent { .. }) => {
                        unreachable!("Parent-side loop received a FromParent beat")
                    }
                    HeartbeatEvent::Side(..) => {
                        unreachable!("Parent-side loop received a side-channel message")
                    }
                }
            }
        }
    }
}

/// The **delegate-host** loop: this child covers one or more siblings, so we
/// heartbeat it directly for good and never delegate it (no Pause/Resume). Its
/// `FromChild` beats carry coverage diffs, which we forward as `PauseHeartbeat` /
/// `ResumeHeartbeat` to the covered siblings' loops. On exit every covered sibling
/// is resumed.
#[expect(
    clippy::too_many_arguments,
    reason = "each is a distinct per-connection channel/handle; bundling adds indirection"
)]
async fn parent_cover_loop(
    connection: ConnectionRef,
    config: HeartbeatConfig,
    cover_add: Vec<ConnectionId>,
    cover_del: Vec<ConnectionId>,
    beats: &mpsc::UnboundedSender<Heartbeat>,
    events: &mut mpsc::UnboundedReceiver<HeartbeatEvent>,
    shared: &Rc<RefCell<HeartbeatShared>>,
    loop_tx: &mpsc::UnboundedSender<Command>,
) {
    let mut deadline = Instant::now() + config.timeout;
    // The connections we cover; resumed if this link fails.
    let mut covered: HashSet<ConnectionId> = HashSet::new();
    shared
        .borrow()
        .signal_coverage(&mut covered, cover_add, cover_del);

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                if crate::ctx::connection_debug() {
                    eprintln!("MM_SEVER cover-loop (a cover host itself timed out)");
                }
                sever(loop_tx, connection, b"quic heartbeat timeout".to_vec());
                break;
            }
            ev = events.recv() => {
                let Some(ev) = ev else { break };
                match ev {
                    HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromChild { cover_add, cover_del }) => {
                        deadline = Instant::now() + config.timeout;
                        shared.borrow().signal_coverage(&mut covered, cover_add, cover_del);
                        // Answer so the delegate host paces its next beat.
                        let _ = beats.send(Heartbeat::FromParent { delegate: None });
                    }
                    // Establishment is a one-shot exchange that completed back in the
                    // direct loop; a stray duplicate here is harmless.
                    HeartbeatEvent::EstablishPeer { .. } | HeartbeatEvent::EstablishLocal { .. } => {}
                    HeartbeatEvent::ReaderClosed => break,
                    // A delegate host is never itself delegated, so no sibling ever
                    // covers it → no Pause/Resume. Our child peer only sends
                    // `FromChild`. `Side` routes to Child-side loops only. None reach.
                    HeartbeatEvent::PauseHeartbeat | HeartbeatEvent::ResumeHeartbeat => {
                        unreachable!("delegate host received Pause/Resume (it is never delegated)")
                    }
                    HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromParent { .. }) => {
                        unreachable!("Parent-side loop received a FromParent beat")
                    }
                    HeartbeatEvent::Side(..) => {
                        unreachable!("Parent-side loop received a side-channel message")
                    }
                }
            }
        }
    }

    // Stop covering everyone: each covered child's loop reclaims itself as direct.
    shared.borrow().resume_covered(&covered);
}

// ---------------------------------------------------------------------------
// Child side (D proving liveness to parent B; also hosts coverage as delegate C)
// ---------------------------------------------------------------------------

enum ChildState {
    /// Prove liveness to B directly; time out on B's physical `FromParent` beats.
    Direct,
    /// Prove liveness via the side channel to sibling `c`; time out on its acks.
    Delegated { x: ConnectionId, c: Vec<u8> },
}

async fn child_task<S: SendBeat>(ctx: HeartbeatCtx<S>) {
    let HeartbeatCtx {
        connection,
        dialed: _,
        beats,
        mut events,
        handle,
        shared,
        config,
        send_beat,
        loop_tx,
    } = ctx;
    let interval = config.interval;
    let timeout = config.timeout;
    // How long to wait for the *first* ack from a fresh delegate before giving up on
    // it. Half a normal timeout, so that if the delegate never answers we fall back
    // to beating our parent directly with time to spare — before the parent's own
    // (full) timeout would declare us dead. Steady-state acks use the full timeout.
    let settle = timeout / 2;

    let mut own_ident: Option<Vec<u8>> = None;
    let mut state = ChildState::Direct;
    // Coverage-diff report to the parent, drained onto the next `FromChild` beat.
    let mut cover_add: Vec<ConnectionId> = Vec::new();
    let mut cover_del: Vec<ConnectionId> = Vec::new();
    // Connections this child hosts coverage for (delegate-host duty): conn_id →
    // (beating child's ident, deadline).
    let mut coverage: HashMap<ConnectionId, (Vec<u8>, Instant)> = HashMap::new();

    // Echo model: we send one beat, then stay silent until the peer answers before
    // sending the next. `next_beat` is `Some(t)` when it is time to send again, `None`
    // while awaiting an answer. Starting at `now` sends an initial beat immediately.
    // Because we never have two un-answered beats outstanding, once our parent answers
    // with a `Delegate` we never send it another direct beat — so nothing we send can
    // race behind the sibling's coverage report and un-pause the parent.
    let mut next_beat = Some(Instant::now());
    // The single primary timeout: the parent's answer (Direct) or C's acks (Delegated).
    let mut primary = Instant::now() + timeout;

    loop {
        let cov_deadline = coverage.values().map(|(_, d)| *d).min();
        tokio::select! {
            _ = deadline_or_park(next_beat) => {
                match &state {
                    ChildState::Direct => {
                        if crate::ctx::connection_debug() && !cover_add.is_empty() {
                            eprintln!(
                                "MM_COVER pid={} report cover_add={:?}",
                                std::process::id(),
                                cover_add
                            );
                        }
                        let _ = beats.send(Heartbeat::FromChild {
                            cover_add: std::mem::take(&mut cover_add),
                            cover_del: std::mem::take(&mut cover_del),
                        });
                        // Await the parent's answer before beating again.
                        primary = Instant::now() + timeout;
                    }
                    ChildState::Delegated { x, c } => {
                        if let Some(own) = &own_ident {
                            // Beat our delegate C over the side channel.
                            if crate::ctx::connection_debug() {
                                eprintln!(
                                    "MM_CHILD pid={} beat delegate x={} c={}",
                                    std::process::id(),
                                    x,
                                    String::from_utf8_lossy(c)
                                );
                            }
                            send_beat(c.clone(), own.clone(), *x, BeatKind::Beat);
                        } else if crate::ctx::connection_debug() {
                            eprintln!("MM_CHILD pid={} delegated but own_ident=None", std::process::id());
                        }
                        // Await C's ack; `primary` was armed on entry / last ack.
                    }
                }
                next_beat = None;
            }
            _ = tokio::time::sleep_until(primary) => {
                match &state {
                    ChildState::Direct => {
                        sever(&loop_tx, connection, b"quic heartbeat timeout".to_vec());
                        break;
                    }
                    ChildState::Delegated { .. } => {
                        // Lost our delegate; beat B directly again (B re-decides).
                        state = ChildState::Direct;
                        primary = Instant::now() + timeout;
                        next_beat = Some(Instant::now());
                    }
                }
            }
            _ = deadline_or_park(cov_deadline), if cov_deadline.is_some() => {
                let now = Instant::now();
                let expired: Vec<ConnectionId> = coverage
                    .iter()
                    .filter(|(_, (_, d))| *d <= now)
                    .map(|(&x, _)| x)
                    .collect();
                for x in expired {
                    coverage.remove(&x);
                    cover_del.push(x);
                    if crate::ctx::connection_debug() {
                        eprintln!(
                            "MM_COVER pid={} EXPIRE conn_id={} (no beat within timeout)",
                            std::process::id(),
                            x
                        );
                    }
                }
            }
            ev = events.recv() => {
                let Some(ev) = ev else { break };
                match ev {
                    HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromParent { delegate }) => {
                        // Our parent's answer to our last direct beat.
                        if crate::ctx::connection_debug() {
                            if let Some(Delegate { connection_id, sibling_ident }) = &delegate {
                                eprintln!(
                                    "MM_CHILD pid={} got delegate x={} c={} own_addressable={} direct={}",
                                    std::process::id(),
                                    connection_id,
                                    String::from_utf8_lossy(sibling_ident),
                                    own_ident.as_deref().is_some_and(is_addressable),
                                    matches!(state, ChildState::Direct),
                                );
                            }
                        }
                        match delegate {
                            Some(Delegate { connection_id, sibling_ident })
                                if own_ident.as_deref().is_some_and(is_addressable)
                                    && matches!(state, ChildState::Direct) =>
                            {
                                state = ChildState::Delegated {
                                    x: connection_id,
                                    c: sibling_ident,
                                };
                                // Give the fresh delegate only the shorter settle window
                                // to answer; a full ack later extends it. Beat it now.
                                primary = Instant::now() + settle;
                                next_beat = Some(Instant::now());
                            }
                            _ if matches!(state, ChildState::Direct) => {
                                // A plain answer (or an un-actionable delegate): stay
                                // direct, refresh the deadline, and pace the next beat.
                                primary = Instant::now() + timeout;
                                next_beat = Some(Instant::now() + interval);
                            }
                            _ => {}
                        }
                    }
                    HeartbeatEvent::Side(from, conn_id, BeatKind::Beat) => {
                        // Delegate-host duty: (re)arm coverage for `conn_id` and ack.
                        if crate::ctx::connection_debug() {
                            eprintln!(
                                "MM_COVER pid={} beat-in conn_id={} from={}",
                                std::process::id(),
                                conn_id,
                                String::from_utf8_lossy(&from)
                            );
                        }
                        let is_new = !coverage.contains_key(&conn_id);
                        coverage.insert(conn_id, (from.clone(), Instant::now() + timeout));
                        if is_new {
                            cover_add.push(conn_id);
                            if crate::ctx::connection_debug() {
                                eprintln!(
                                    "MM_COVER pid={} add conn_id={} from={}",
                                    std::process::id(),
                                    conn_id,
                                    String::from_utf8_lossy(&from)
                                );
                            }
                        }
                        if let Some(own) = &own_ident {
                            // Ack the delegated child that beat us.
                            send_beat(from, own.clone(), conn_id, BeatKind::Ack);
                        }
                    }
                    HeartbeatEvent::Side(_, conn_id, BeatKind::Ack) => {
                        if let ChildState::Delegated { x, .. } = &state {
                            if *x == conn_id {
                                // C is alive: extend the deadline and pace the next beat.
                                primary = Instant::now() + timeout;
                                next_beat = Some(Instant::now() + interval);
                            }
                        }
                    }
                    HeartbeatEvent::Side(_, conn_id, BeatKind::Release) => {
                        // Our delegate is tearing down: revert to beating our parent
                        // directly again (no need to wait for the ack to lapse).
                        if let ChildState::Delegated { x, .. } = &state {
                            if *x == conn_id {
                                state = ChildState::Direct;
                                primary = Instant::now() + timeout;
                                next_beat = Some(Instant::now());
                            }
                        }
                    }
                    HeartbeatEvent::EstablishLocal { local_ident } => {
                        own_ident = Some(local_ident.clone());
                        shared
                            .borrow_mut()
                            .children_by_ident
                            .insert(local_ident, handle.clone());
                    }
                    HeartbeatEvent::EstablishPeer { .. } => {}
                    HeartbeatEvent::ReaderClosed => {
                        // The reader already severed with the detailed close reason;
                        // this just stops the coroutine (its own `handle` clones keep
                        // the event channel open, so it can't rely on that closing) so
                        // the teardown below runs. Do *not* sever again here.
                        break;
                    }
                    // These cannot reach a Child-side loop: our parent peer only ever
                    // sends `FromParent`, and Pause/Resume route via
                    // `parents_by_conn_id` to Parent-side loops only.
                    HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromChild { .. }) => {
                        unreachable!("Child-side loop received a FromChild beat")
                    }
                    HeartbeatEvent::PauseHeartbeat | HeartbeatEvent::ResumeHeartbeat => {
                        unreachable!("Child-side loop received Pause/Resume")
                    }
                }
            }
        }
    }

    if let Some(own) = &own_ident {
        // We are tearing down: tell every delegated child we were covering to stop
        // delegating to us and heartbeat its parent directly again, so it re-homes
        // immediately rather than waiting for our acks to lapse.
        for (&conn_id, (child_ident, _)) in &coverage {
            send_beat(child_ident.clone(), own.clone(), conn_id, BeatKind::Release);
        }
        shared.borrow_mut().children_by_ident.remove(own);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// These drive the *real* heartbeat coroutines ([`Heartbeats::spawn`]) but replace
// the QUIC transport with a manual pump, so the order of messages into the
// `Heartbeats` object is fully controlled:
//
//   * A connection's outgoing physical beats land in its `beats` receiver; a test
//     forwards a peer's beats into this connection's `events` as `ReceivedHeartbeat`
//     (this is what the reader would do). Withholding that forward simulates a
//     silent / dead peer.
//   * Sibling side-channel beats/acks/releases go through the real
//     [`Heartbeats::deliver`] (the same call the side-channel reader makes), routed
//     by `children_by_ident` — so one shared `Heartbeats` models the B/C/D fabric.
//   * A `Severed` shows up on the connection's `loop_tx` receiver.
//
// Time is real but short (see [`test_config`]) so the tests run in well under a
// second while exercising the genuine timers.
#[cfg(test)]
mod tests {
    use slotmap::SlotMap;
    use tokio::sync::mpsc;

    use super::*;
    use crate::connection::ConnectionCommand;
    use crate::ctx::ChildConnectionKey;

    const INTERVAL_MS: u64 = 20;
    const TIMEOUT_MS: u64 = 200;

    fn test_config() -> HeartbeatConfig {
        HeartbeatConfig {
            interval: Duration::from_millis(INTERVAL_MS),
            timeout: Duration::from_millis(TIMEOUT_MS),
            // A parent may keep one child direct; a second established child is over
            // budget and gets delegated onto the first.
            max_direct_children: 1,
        }
    }

    /// One spawned coroutine's test-side handles.
    struct Conn {
        events: mpsc::UnboundedSender<HeartbeatEvent>,
        beats: mpsc::UnboundedReceiver<Heartbeat>,
        severs: mpsc::UnboundedReceiver<Command>,
    }

    /// Spawn a Parent-side coroutine (watches a child `slot` of actor `parent`).
    /// `dialed` = we dialed the child (required for delegation).
    fn spawn_parent(hb: &Heartbeats, parent: Key, slot: ChildConnectionKey, dialed: bool) -> Conn {
        let (beats_tx, beats) = mpsc::unbounded_channel();
        let (sever_tx, severs) = mpsc::unbounded_channel();
        let connection = ConnectionRef::ChildConnection {
            ofactor: parent,
            slot,
        };
        // A parent never originates side-channel beats.
        let events = hb.spawn(connection, dialed, beats_tx, |_, _, _, _| {}, sever_tx);
        Conn {
            events,
            beats,
            severs,
        }
    }

    /// Spawn a Child-side coroutine for actor `actor`. Its side-channel beats/acks
    /// are delivered through the same `Heartbeats` (the shared fabric).
    fn spawn_child(hb: &Heartbeats, actor: Key) -> Conn {
        let (beats_tx, beats) = mpsc::unbounded_channel();
        let (sever_tx, severs) = mpsc::unbounded_channel();
        let connection = ConnectionRef::ParentConnection { ofactor: actor };
        let fabric = hb.clone();
        let send_beat = move |recipient: Vec<u8>, from: Vec<u8>, conn_id: ConnectionId, kind| {
            fabric.deliver(&recipient, from, conn_id, kind);
        };
        // `dialed` is irrelevant on the child side.
        let events = hb.spawn(connection, false, beats_tx, send_beat, sever_tx);
        Conn {
            events,
            beats,
            severs,
        }
    }

    fn send(c: &Conn, ev: HeartbeatEvent) {
        let _ = c.events.send(ev);
    }

    fn establish_child(c: &Conn, own: &[u8], parent: &[u8]) {
        send(
            c,
            HeartbeatEvent::EstablishLocal {
                local_ident: own.to_vec(),
            },
        );
        send(
            c,
            HeartbeatEvent::EstablishPeer {
                peer_ident: parent.to_vec(),
            },
        );
    }

    fn establish_parent(c: &Conn, peer: &[u8]) {
        send(
            c,
            HeartbeatEvent::EstablishPeer {
                peer_ident: peer.to_vec(),
            },
        );
    }

    /// Forward every physical beat `from` has emitted into `to` (what a live reader
    /// would do). Withholding this is how a test simulates a silent peer.
    fn pump(from: &mut Conn, to: &Conn) {
        while let Ok(hb) = from.beats.try_recv() {
            let _ = to.events.send(HeartbeatEvent::ReceivedHeartbeat(hb));
        }
    }

    fn drain_beats(c: &mut Conn) {
        while c.beats.try_recv().is_ok() {}
    }

    fn sever_reason(c: &mut Conn) -> Option<Vec<u8>> {
        match c.severs.try_recv() {
            Ok(Command::ConnectionAction {
                action: ConnectionCommand::Severed { reason },
                ..
            }) => Some(reason),
            _ => None,
        }
    }

    async fn nap(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    /// `DelegateTargets` hands each new delegate to the least-loaded target and keeps
    /// coverage balanced as counts move by one, staying correctly sorted throughout.
    #[test]
    fn delegate_targets_balances_least_loaded() {
        let mut t = DelegateTargets::new(3);
        t.add(1);
        t.add(2);
        t.add(3); // three targets, all cover count 0

        // Assign six delegates, each to the least-loaded target (charge +1 after each
        // pick, as the pool does when a child enters `Delegating`).
        for _ in 0..6 {
            let id = t.least_loaded().expect("a target exists");
            t.change_count_if_present(id, 1);
        }
        // Balanced: 6 delegates across 3 targets => 2 each.
        for id in [1, 2, 3] {
            assert_eq!(t.load_of(id), 2, "target {id} balanced to 2");
        }
        assert!(t.is_full(), "three targets fills a cap of three");

        // Discharging one makes it strictly least loaded, so it is picked next.
        t.change_count_if_present(2, -1);
        assert_eq!(t.load_of(2), 1, "target 2 now covers one");
        assert_eq!(
            t.least_loaded(),
            Some(2),
            "the discharged target is least loaded"
        );

        // Removing a target drops it entirely; order among the rest is preserved.
        t.remove(1);
        assert!(!t.contains(1), "removed target is gone");
        assert_eq!(t.len(), 2);
    }

    /// Stress the min-heap internals: interleave add/remove/charge with a
    /// deterministic pseudo-random schedule and check, after every mutation, that the
    /// heap invariant holds, the index map is consistent, and `least_loaded` really is
    /// a minimum-count target.
    #[test]
    fn delegate_targets_heap_invariant_under_churn() {
        // Assert every structural invariant of `t`.
        fn check(t: &DelegateTargets) {
            assert_eq!(t.heap.len(), t.pos.len(), "pos covers exactly the heap");
            assert_eq!(t.heap.len(), t.count.len(), "count covers exactly the heap");
            for (i, &id) in t.heap.iter().enumerate() {
                assert_eq!(t.pos[&id], i, "pos[{id}] tracks its heap slot");
                if i > 0 {
                    let parent = (i - 1) / 2;
                    assert!(
                        t.count_at(parent) <= t.count_at(i),
                        "min-heap: parent at {parent} <= child at {i}"
                    );
                }
            }
            if let Some(top) = t.least_loaded() {
                let min = t.heap.iter().map(|&id| t.load_of(id)).min().unwrap();
                assert_eq!(t.load_of(top), min, "least_loaded is a true minimum");
            }
        }

        // A tiny deterministic LCG so the test is reproducible without an rng dep.
        let mut seed: u64 = 0x9e3779b97f4a7c15;
        let mut rng = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 33) as usize
        };

        // Big cap so `add` is never rejected; ids cycle through a small space so
        // adds, removes, and re-adds all collide and exercise every path.
        let mut t = DelegateTargets::new(1_000);
        for _ in 0..5_000 {
            let id = (rng() % 32) as ConnectionId;
            match rng() % 3 {
                0 if !t.contains(id) => t.add(id),
                1 => t.remove(id), // no-op if absent
                _ if t.contains(id) => {
                    // Move the count by +/-1, as the pool does.
                    let delta = if rng() % 2 == 0 { 1 } else { -1 };
                    t.change_count_if_present(id, delta);
                }
                _ => {}
            }
            check(&t);
        }
    }

    /// The pool keeps at most `max_direct` children direct (the keepers) and delegates
    /// every other established child onto them, balanced — so the number of direct
    /// children settles at exactly `max_direct`, never above it.
    #[test]
    fn delegate_respects_max_direct_and_balances() {
        let mut pool = ParentPool::new(2); // keep at most two children direct
        for id in 0..6 {
            pool.add_child(id, true);
            pool.established(id, format!("c{id}@x").into_bytes());
        }

        // Each child beats once: the first two become keepers, the rest are delegated
        // onto them.
        for id in 0..6 {
            let _ = pool.delegate(id);
        }

        let direct = pool
            .siblings
            .values()
            .filter(|s| s.status == ChildStatus::Direct)
            .count();
        assert_eq!(direct, 2, "exactly max_direct children remain direct");
        assert_eq!(pool.delegate_targets.len(), 2, "the two keepers");
        // The four delegated children are balanced two-per-keeper.
        let mut loads: Vec<usize> = pool
            .delegate_targets
            .heap
            .iter()
            .map(|&k| pool.delegate_targets.load_of(k))
            .collect();
        loads.sort_unstable();
        assert_eq!(
            loads,
            vec![2, 2],
            "four delegates balanced across two keepers"
        );
    }

    /// B watching D directly: stable while both are pumped; B severs once D goes
    /// silent.
    #[test]
    fn direct_liveness_and_parent_timeout() {
        let rt = runtime();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let hb = Heartbeats::with_config(test_config());
            let mut keys: SlotMap<Key, ()> = SlotMap::with_key();
            let mut slots: SlotMap<ChildConnectionKey, ()> = SlotMap::with_key();
            let b = keys.insert(());
            let d = keys.insert(());
            let d_slot = slots.insert(());

            let mut bd = spawn_parent(&hb, b, d_slot, true);
            let mut d = spawn_child(&hb, d);
            establish_parent(&bd, b"D@d");
            establish_child(&d, b"D@d", b"B@b");

            // Steady state: both beat each other for several intervals — no sever.
            for _ in 0..8 {
                nap(INTERVAL_MS).await;
                pump(&mut bd, &d);
                pump(&mut d, &bd);
            }
            assert!(
                sever_reason(&mut bd).is_none(),
                "B should not sever while D beats"
            );
            assert!(
                sever_reason(&mut d).is_none(),
                "D should not sever while B beats"
            );

            // D goes silent (its beats no longer reach B). In the echo model B answers
            // only beats it hears, so with D silent B never answers either — B's watch
            // of D lapses and it severs. (Symmetrically D, hearing no answer, would also
            // time out; here we only assert B's side.)
            let mut reason = None;
            for _ in 0..((TIMEOUT_MS / INTERVAL_MS) + 4) {
                nap(INTERVAL_MS).await;
                drain_beats(&mut d);
                if let Some(r) = sever_reason(&mut bd) {
                    reason = Some(r);
                    break;
                }
            }
            assert_eq!(
                reason.as_deref(),
                Some(b"quic heartbeat timeout".as_slice()),
                "B severs D once D stops beating"
            );
        });
    }

    /// A child severs when its parent goes silent.
    #[test]
    fn child_times_out_when_parent_silent() {
        let rt = runtime();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let hb = Heartbeats::with_config(test_config());
            let mut keys: SlotMap<Key, ()> = SlotMap::with_key();
            let mut slots: SlotMap<ChildConnectionKey, ()> = SlotMap::with_key();
            let b = keys.insert(());
            let d = keys.insert(());
            let d_slot = slots.insert(());

            let mut bd = spawn_parent(&hb, b, d_slot, true);
            let mut d = spawn_child(&hb, d);
            establish_parent(&bd, b"D@d");
            establish_child(&d, b"D@d", b"B@b");

            for _ in 0..6 {
                nap(INTERVAL_MS).await;
                pump(&mut bd, &d);
                pump(&mut d, &bd);
            }
            assert!(sever_reason(&mut d).is_none());

            // B goes silent; keep forwarding D's beats to B so only D's watch lapses.
            let mut reason = None;
            for _ in 0..((TIMEOUT_MS / INTERVAL_MS) + 4) {
                nap(INTERVAL_MS).await;
                pump(&mut d, &bd);
                drain_beats(&mut bd);
                if let Some(r) = sever_reason(&mut d) {
                    reason = Some(r);
                    break;
                }
            }
            assert_eq!(
                reason.as_deref(),
                Some(b"quic heartbeat timeout".as_slice()),
                "D severs once its parent stops beating"
            );
        });
    }

    /// A delegated child whose delegate never acks reverts to beating its parent
    /// directly (ack-timeout fallback), rather than severing.
    #[test]
    fn delegated_child_reverts_when_delegate_never_acks() {
        let rt = runtime();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let hb = Heartbeats::with_config(test_config());
            let mut keys: SlotMap<Key, ()> = SlotMap::with_key();
            let d = keys.insert(());
            let mut d = spawn_child(&hb, d);
            establish_child(&d, b"D@d", b"B@b");
            nap(INTERVAL_MS).await;

            // Pretend B delegated us to sibling C@c (which does not exist here, so no
            // acks ever come back).
            send(
                &d,
                HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromParent {
                    delegate: Some(Delegate {
                        connection_id: 777,
                        sibling_ident: b"C@c".to_vec(),
                    }),
                }),
            );

            // While delegated it beats the (absent) delegate over the side channel,
            // not its parent — so no FromChild beats come out for a bit.
            nap(INTERVAL_MS).await;
            drain_beats(&mut d);

            // After the ack timeout it must revert to Direct and beat its parent again
            // (a FromChild beat), and must NOT sever.
            let mut saw_from_child = false;
            for _ in 0..((TIMEOUT_MS / INTERVAL_MS) + 6) {
                nap(INTERVAL_MS).await;
                while let Ok(hb) = d.beats.try_recv() {
                    if matches!(hb, Heartbeat::FromChild { .. }) {
                        saw_from_child = true;
                    }
                }
                if saw_from_child {
                    break;
                }
            }
            assert!(
                saw_from_child,
                "delegated child reverts to beating its parent"
            );
            assert!(
                sever_reason(&mut d).is_none(),
                "reverting child does not sever"
            );
        });
    }

    /// B delegates D to a sibling C that is *about to* die: C is still eligible (has
    /// not yet failed its own heartbeat with B) but never acks D. D must revert
    /// within the settle window — before B's full timeout — so D is never severed
    /// during that gap. Then C fails its timeout with B: B severs it and drops it
    /// from the pool, so it is no longer an eligible delegate and D settles as a
    /// direct child. (A dead C never recovers — idents are not reused.)
    #[test]
    fn delegate_to_dying_c_never_severs_d() {
        let rt = runtime();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let hb = Heartbeats::with_config(test_config());
            let mut keys: SlotMap<Key, ()> = SlotMap::with_key();
            let mut slots: SlotMap<ChildConnectionKey, ()> = SlotMap::with_key();
            let b = keys.insert(());
            let d = keys.insert(());
            let c_slot = slots.insert(());
            let d_slot = slots.insert(());

            // B's watch of C exists so C is an eligible delegate, but there is no live
            // C coroutine: it never acks D's side-channel beats, and (since we never
            // forward beats to `bc`) it will fail its own heartbeat with B.
            let mut bc = spawn_parent(&hb, b, c_slot, true);
            let mut bd = spawn_parent(&hb, b, d_slot, true);
            let mut d = spawn_child(&hb, d);
            establish_child(&d, b"D@d", b"B@b");
            establish_parent(&bc, b"C@c");
            establish_parent(&bd, b"D@d"); // 2 direct children > max_direct = 1
            nap(INTERVAL_MS).await;

            // One beat from C promotes it to the sole keeper (max_direct = 1); then it
            // goes silent forever (we never pump `bc` again), so it is the doomed
            // delegate. Beating B–D next delegates D onto C.
            send(
                &bc,
                HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromChild {
                    cover_add: vec![],
                    cover_del: vec![],
                }),
            );
            nap(INTERVAL_MS).await;
            // Trigger B–D to delegate D to the doomed C.
            send(
                &bd,
                HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromChild {
                    cover_add: vec![],
                    cover_del: vec![],
                }),
            );

            // Until C is reaped, B may (re-)hand D to it; D reverts within the settle
            // window each time and beats B — so B never severs D. Once C fails its own
            // timeout, B severs it and it leaves the eligible pool.
            let mut d_severed = false;
            let mut bd_severed = false;
            let mut c_severed = false;
            for _ in 0..(TIMEOUT_MS * 4 / INTERVAL_MS) {
                nap(INTERVAL_MS).await;
                drain_beats(&mut bc); // C never hears B and never answers
                pump(&mut bd, &d); // carries B's Delegate instructions and beats
                pump(&mut d, &bd); // D's direct beats after each revert
                if sever_reason(&mut d).is_some() {
                    d_severed = true;
                }
                if sever_reason(&mut bd).is_some() {
                    bd_severed = true;
                }
                if sever_reason(&mut bc).is_some() {
                    c_severed = true;
                }
            }
            assert!(!d_severed, "D is never severed while its delegate is dying");
            assert!(!bd_severed, "B never severs D");
            assert!(
                c_severed,
                "B severs the dead delegate C (it failed its timeout)"
            );

            // C is gone from the pool, so B can no longer delegate D to it; D is a
            // plain direct child now — B keeps beating it and it survives.
            drain_beats(&mut bd);
            for _ in 0..8 {
                nap(INTERVAL_MS).await;
                pump(&mut bd, &d);
                pump(&mut d, &bd);
            }
            assert!(
                sever_reason(&mut d).is_none(),
                "D remains a live direct child of B"
            );
            assert!(sever_reason(&mut bd).is_none());
        });
    }

    /// Full B/C/D: D is delegated to sibling C, C covers D (so B pauses its watch of
    /// D), then C tears down — D must survive by re-homing to B.
    #[test]
    fn delegation_then_c_dies_d_survives() {
        let rt = runtime();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let hb = Heartbeats::with_config(test_config());
            let mut keys: SlotMap<Key, ()> = SlotMap::with_key();
            let mut slots: SlotMap<ChildConnectionKey, ()> = SlotMap::with_key();
            let b = keys.insert(());
            let c = keys.insert(());
            let d = keys.insert(());
            let c_slot = slots.insert(());
            let d_slot = slots.insert(());

            // B's two parent loops, and C's and D's child loops.
            let mut bc = spawn_parent(&hb, b, c_slot, true);
            let mut bd = spawn_parent(&hb, b, d_slot, true);
            let mut c = spawn_child(&hb, c);
            let mut d = spawn_child(&hb, d);

            establish_child(&c, b"C@c", b"B@b");
            establish_child(&d, b"D@d", b"B@b");
            establish_parent(&bc, b"C@c");
            establish_parent(&bd, b"D@d"); // now 2 direct children > max_direct = 1
            nap(INTERVAL_MS).await;

            // With max_direct = 1 the first over-budget beat promotes a keeper and the
            // next delegates onto it. Beat B–C first so C becomes the sole keeper...
            send(
                &bc,
                HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromChild {
                    cover_add: vec![],
                    cover_del: vec![],
                }),
            );
            nap(INTERVAL_MS).await;
            // ...then beat B–D so D is delegated to C.
            send(
                &bd,
                HeartbeatEvent::ReceivedHeartbeat(Heartbeat::FromChild {
                    cover_add: vec![],
                    cover_del: vec![],
                }),
            );
            nap(INTERVAL_MS).await;
            // Deliver B–D's Delegate answer to D (and any prior liveness beats).
            pump(&mut bd, &d);

            // D now beats C over the side channel (via deliver); C acks and queues
            // coverage, reporting it to B on C's next beat. Pump B–C both ways so C keeps
            // beating (echo model) and its report reaches B, flipping B–C to a delegate
            // host that pauses B–D. C already covers D (requested_covers=1), so B answers
            // C plainly and never delegates C itself. Detect the pause via B's pool.
            let mut b_c_became_cover = false;
            for _ in 0..16 {
                nap(INTERVAL_MS).await;
                drain_beats(&mut d); // D's side-channel beats go via deliver, not here
                pump(&mut c, &bc); // C's beats (incl. its coverage report) → B
                pump(&mut bc, &c); // B's answers → C, so C keeps beating
                let paused =
                    hb.shared.borrow().parents.get(&b).is_some_and(|p| {
                        p.siblings.values().any(|s| s.status == ChildStatus::Paused)
                    });
                if paused {
                    b_c_became_cover = true;
                    break;
                }
            }
            assert!(b_c_became_cover, "C reported covering D to B (B paused D)");

            // While paused, B does not sever the delegated D even past a full timeout.
            // Keep pumping B–C both ways so the cover host C stays alive and keeps
            // covering D (D beats C via deliver, so that link needs no pump).
            for _ in 0..((TIMEOUT_MS / INTERVAL_MS) + 2) {
                nap(INTERVAL_MS).await;
                drain_beats(&mut d);
                pump(&mut c, &bc);
                pump(&mut bc, &c);
            }
            assert!(sever_reason(&mut bd).is_none(), "delegated D not severed");
            assert!(
                sever_reason(&mut d).is_none(),
                "D not severed while delegated"
            );

            // --- C dies ---
            // First, B's watch of C sees the transport close: B–C's cover loop tears
            // down, resuming its coverage of D (so B reclaims D) and dropping C from
            // the eligible-delegate set — before D re-homes, so D is never
            // re-delegated to the dying C.
            send(&bc, HeartbeatEvent::ReaderClosed);
            nap(INTERVAL_MS).await;
            // Then C's own coroutine tears down, releasing D over the side channel so
            // it reverts to beating its parent directly.
            send(&c, HeartbeatEvent::ReaderClosed);
            nap(INTERVAL_MS).await;

            // C is gone: side-channel beats addressed to it are now dropped.
            assert!(
                hb.shared.borrow().child_handle(b"C@c").is_none(),
                "C is deregistered once it tears down"
            );

            // D re-homed to B; pump the B–D link and confirm both survive and B answers
            // D's direct beats again (un-paused). In the echo model B emits a beat only
            // in reply to D, so seeing any B beat proves it is heartbeating D again.
            let mut b_answers_d = false;
            for _ in 0..8 {
                nap(INTERVAL_MS).await;
                // D's revert beats → B.
                while let Ok(hb) = d.beats.try_recv() {
                    let _ = bd.events.send(HeartbeatEvent::ReceivedHeartbeat(hb));
                }
                // B's answers → D (so D keeps beating), noting that B answered.
                while let Ok(hb) = bd.beats.try_recv() {
                    b_answers_d = true;
                    let _ = d.events.send(HeartbeatEvent::ReceivedHeartbeat(hb));
                }
            }
            assert!(
                sever_reason(&mut d).is_none(),
                "D survives C's death by re-homing to B"
            );
            assert!(
                sever_reason(&mut bd).is_none(),
                "B keeps D alive after reclaiming it"
            );
            assert!(
                b_answers_d,
                "B resumes directly heartbeating D after reclaiming it"
            );
        });
    }
}
