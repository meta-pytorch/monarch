/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! [`Proc`] is an addressable actor-runtime boundary.
//!
//! It owns actor lifecycle (spawn, run, terminate), routes messages
//! to local actors, forwards messages for remote destinations, and
//! hosts supervision state.
//!
//! It also stores bounded snapshots of terminated actors for
//! post-mortem introspection.
//!
//! ## Client instance invariants (CI-*)
//!
//! - **CI-1 (client status):** `IntrospectMessage::Query` on an
//!   introspectable instance returns `status: "client"` and
//!   `actor_type: "()"` in attrs.
//! - **CI-2 (snapshot on drop):** Dropping the returned `Instance<()>`
//!   transitions its status to terminal, causing the introspect task
//!   to store a terminated snapshot.
//!
//! ## Actor identity invariants (AI-*)
//!
//! - **AI-1 (named-child uid):** Each child gets a globally unique
//!   random uid. Named children carry a label for display purposes.
//! - **AI-3 (controller ActorAddr uniqueness):** Each named child gets
//!   a unique uid; the label is informational only.
//!
//! ## Flight recorder span invariants (FR-*)
//!
//! - **FR-1 (recording-span route equivalence):**
//!   `Instance::recording_span()` returns a span bound to the same
//!   actor-local `Recording` consumed by handler instrumentation and
//!   introspection. Events emitted under that span land in the same
//!   flight-recorder ring buffer returned by `introspect_payload()`.
//! - **FR-2 (recording-span rootness):** Every span returned by
//!   `Instance::recording_span()` is a fresh root span (`parent:
//!   None`). Ambient tracing context does not cause events emitted
//!   under that span to route into a parent actor's flight recorder.
//! - **FR-3 (fresh-handle, stable-destination):** Repeated calls to
//!   `Instance::recording_span()` return distinct span handles, but
//!   all target the same underlying actor recording.
//!
//! ## Queue depth accounting invariants (PD-5*)
//!
//! - **PD-5a:** Per-actor queue depth counts accepted handler work
//!   not yet dequeued by the actor loop. Accounting increments in
//!   `HandlerPorts::get`'s enqueue closure *before* the reorder-buffer
//!   decision, so this counter includes both in-order messages waiting
//!   in `work_rx` AND out-of-order messages held in the
//!   `OrderedSender` reorder buffer. (Reflected at the introspection
//!   layer in IO-3 of the `introspect` module doc.)
//! - **PD-5b:** Queue depth is incremented exactly once per accepted
//!   message in the enqueue closure of `HandlerPorts::get`, before
//!   the in-order / out-of-order branch.
//! - **PD-5c:** Queue depth is decremented exactly once on every
//!   dequeue from `work_rx` (in the actor `run` loop).
//! - **PD-5d:** Queue depth is intended to be non-negative; tests
//!   must cover ordered/buffered delivery paths to validate the
//!   accounting.
//! - **PD-5e:** `queue_depth` and the OTel `ACTOR_MESSAGE_QUEUE_SIZE`
//!   counter are two consumers of one accounting path. The
//!   `account_enqueue` / `account_dequeue` helpers update both
//!   together so they cannot drift.
//!
//! ## Retained queue-pressure invariants (PD-6 through PD-9)
//!
//! `ProcQueueStats` holds proc-level retained evidence of queue
//! pressure. These are runtime-driven (not publish-time sampled)
//! so they capture between-publish bursts.
//!
//! - **PD-6:** `high_water_mark >= running_total` eventually.
//!   Because `running_total` is incremented before `high_water_mark`
//!   is updated, a concurrent reader may transiently observe
//!   `total > high_water_mark`. This is a sampling artifact, not
//!   an accounting error.
//! - **PD-7:** `last_nonzero_age_ms() == None` iff proc queue
//!   depth has never been non-zero since startup. The timestamp
//!   is updated on enqueue and on dequeue when the queue remains
//!   non-zero, so it reflects the last observed non-zero state.
//! - **PD-8:** transient bursts that drain before publish still
//!   update both the high-water mark and the last-nonzero state.
//! - **PD-9:** `last_nonzero_age_ms()` is expected to be
//!   non-decreasing during quiet periods, but this is not a hard
//!   guarantee — the implementation uses `SystemTime` (wall clock),
//!   which can move backward on NTP adjustments. Callers should
//!   treat the age as best-effort telemetry, not a monotonic
//!   invariant.

use std::any::Any;
use std::any::TypeId;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::panic::Location as PanicLocation;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::DashSet;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::multiple::RefMulti;
use futures::FutureExt;
use hyperactor_config::Flattrs;
use hyperactor_telemetry::ActorStatusEvent;
use hyperactor_telemetry::generate_actor_status_event_id;
use hyperactor_telemetry::hash_to_u64;
use hyperactor_telemetry::notify_actor_status_changed;
use hyperactor_telemetry::notify_message;
use hyperactor_telemetry::notify_message_status;
use hyperactor_telemetry::recorder::Recording;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::Span;
use typeuri::Named;
use uuid::Uuid;
use wirevalue::TypeInfo;

use crate as hyperactor;
use crate::Actor;
use crate::ActorAddr;
use crate::ActorRef;
use crate::Addr;
use crate::Data;
use crate::Handler;
use crate::Location;
use crate::Message;
use crate::PortAddr;
use crate::ProcAddr;
use crate::ProcId;
use crate::RemoteMessage;
use crate::actor::ActorError;
use crate::actor::ActorErrorKind;
use crate::actor::ActorHandle;
use crate::actor::ActorStatus;
use crate::actor::AnyActorHandle;
use crate::actor::Binds;
use crate::actor::HandlerInfo;
use crate::actor::Referable;
use crate::actor::RemoteHandles;
use crate::actor::Signal;
use crate::actor::StopMode;
use crate::actor_local::ActorLocalStorage;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
#[cfg(test)]
use crate::channel::ChannelTransport;
use crate::client::Client;
use crate::client::ClientActor;
use crate::config;
use crate::context;
use crate::context::Mailbox as _;
use crate::endpoint::Endpoint as _;
use crate::gateway::Gateway;
use crate::id::ActorId;
use crate::id::Label;
use crate::id::Uid;
use crate::introspect::IntrospectMessage;
use crate::introspect::IntrospectResult;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DeliveryError;
use crate::mailbox::DeliveryFailure;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::Mailbox;
use crate::mailbox::MailboxClient;
use crate::mailbox::MailboxMuxer;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::OncePortHandle;
use crate::mailbox::OncePortReceiver;
#[cfg(test)]
use crate::mailbox::PanickingMailboxSender;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::mailbox::TransportFailure;
use crate::mailbox::TransportFailureReason;
use crate::mailbox::Undeliverable;
use crate::mailbox::UndeliverableReason;
use crate::metrics::ACTOR_MESSAGE_HANDLER_DURATION;
use crate::metrics::ACTOR_MESSAGE_QUEUE_SIZE;
use crate::metrics::ACTOR_MESSAGES_RECEIVED;
use crate::subject::AsSubject as _;

tokio::task_local! {
    static CURRENT_TASK_PROC: Proc;
}

/// Legacy singleton proc name used for host-local client actors.
///
/// This is not a true singleton: every host may have a `local` proc, so local
/// delivery must compare both proc id and location for this id.
pub const LEGACY_LOCAL_PROC_NAME: &str = "local";

/// Legacy singleton proc name used for host system actors.
///
/// This is not a true singleton: every host may have a `service` proc, so
/// local delivery must compare both proc id and location for this id.
pub const LEGACY_SERVICE_PROC_NAME: &str = "service";

/// Returns current epoch-millis from wall clock. Used by
/// `ProcQueueStats` for timestamp recording. In tests, override
/// via `ProcQueueStats::with_clock` to get deterministic behavior.
fn wall_clock_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Proc-level retained queue-pressure state (PD-6 through PD-9).
///
/// Runtime-driven and updated from the enqueue/dequeue accounting
/// path, not from publish-time sampling. These metrics preserve
/// between-publish queue-pressure evidence that instantaneous
/// sampling misses.
pub(crate) struct ProcQueueStats {
    /// Proc-wide running total of queued work items. Incremented on
    /// enqueue, decremented on dequeue. O(1) alternative to iterating
    /// per-actor depths.
    running_total: AtomicU64,
    /// Maximum proc-wide queue depth observed since startup (PD-6).
    high_water_mark: AtomicU64,
    /// Epoch-millis of the most recent moment when proc-wide queue
    /// depth was observed non-zero (PD-7). Sentinel 0 means never.
    /// Updated on enqueue and on dequeue when the queue remains
    /// non-zero, so the age reflects the last observed non-zero
    /// state rather than merely the last enqueue.
    last_nonzero_epoch_ms: AtomicU64,
    /// Clock function for timestamps. Defaults to `wall_clock_epoch_ms`.
    /// Tests can override via `with_clock` for deterministic behavior.
    clock: fn() -> u64,
}

impl ProcQueueStats {
    fn new() -> Self {
        Self {
            running_total: AtomicU64::new(0),
            high_water_mark: AtomicU64::new(0),
            last_nonzero_epoch_ms: AtomicU64::new(0),
            clock: wall_clock_epoch_ms,
        }
    }

    /// Create with a custom clock for testing.
    #[cfg(test)]
    fn with_clock(clock: fn() -> u64) -> Self {
        Self {
            running_total: AtomicU64::new(0),
            high_water_mark: AtomicU64::new(0),
            last_nonzero_epoch_ms: AtomicU64::new(0),
            clock,
        }
    }

    /// Current epoch-millis from this instance's clock.
    fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    /// Current proc-wide running total.
    pub(crate) fn running_total(&self) -> u64 {
        self.running_total.load(Ordering::Relaxed)
    }

    /// Maximum proc-wide queue depth since startup (PD-6).
    pub(crate) fn high_water_mark(&self) -> u64 {
        self.high_water_mark.load(Ordering::Relaxed)
    }

    /// How long ago proc-wide queue depth was last observed non-zero
    /// (PD-7). `None` means no counted actor work has traversed the
    /// queue accounting path since startup. Uses the configured clock
    /// (wall clock in production, injectable in tests).
    pub(crate) fn last_nonzero_age_ms(&self) -> Option<u64> {
        let ts = self.last_nonzero_epoch_ms.load(Ordering::Relaxed);
        if ts == 0 {
            return None;
        }
        Some(self.now_ms().saturating_sub(ts))
    }
}

/// Single accounting path for actor work-queue enqueue.
///
/// Updates three consumers together: per-actor `queue_depth`,
/// proc-level retained queue-pressure state (`ProcQueueStats`),
/// and OTel `ACTOR_MESSAGE_QUEUE_SIZE`. Unifying the update
/// here ensures they cannot drift.
fn account_enqueue(queue_depth: &AtomicU64, proc_stats: &ProcQueueStats, actor_id: &str) {
    queue_depth.fetch_add(1, Ordering::Relaxed);
    let new_total = proc_stats.running_total.fetch_add(1, Ordering::Relaxed) + 1;
    // PD-6: update high-water mark.
    proc_stats
        .high_water_mark
        .fetch_max(new_total, Ordering::Relaxed);
    // PD-7: record that the proc is non-zero right now.
    proc_stats
        .last_nonzero_epoch_ms
        .store(proc_stats.now_ms(), Ordering::Relaxed);
    ACTOR_MESSAGE_QUEUE_SIZE.add(
        1,
        hyperactor_telemetry::kv_pairs!("actor_id" => actor_id.to_owned()),
    );
}

/// Single accounting path for actor work-queue dequeue.
///
/// Updates per-actor `queue_depth`, proc-level running total,
/// OTel `ACTOR_MESSAGE_QUEUE_SIZE`, and the last-nonzero
/// timestamp when the proc-wide queue remains non-zero after
/// this dequeue.
fn account_dequeue(queue_depth: &AtomicU64, proc_stats: &ProcQueueStats, actor_id: &str) {
    queue_depth.fetch_sub(1, Ordering::Relaxed);
    let prev_total = proc_stats.running_total.fetch_sub(1, Ordering::Relaxed);
    // PD-7: if the queue is still non-zero after this dequeue,
    // update the timestamp so last_nonzero_age_ms reflects
    // "last observed non-zero state," not just "last enqueue."
    if prev_total > 1 {
        proc_stats
            .last_nonzero_epoch_ms
            .store(proc_stats.now_ms(), Ordering::Relaxed);
    }
    ACTOR_MESSAGE_QUEUE_SIZE.add(
        -1,
        hyperactor_telemetry::kv_pairs!("actor_id" => actor_id.to_owned()),
    );
}

/// Roll back an accounted enqueue when the underlying send fails.
///
/// Must be paired with a prior `account_enqueue` that has not yet
/// been balanced by `account_dequeue`. Decrements per-actor
/// `queue_depth`, proc-level `running_total`, and OTel
/// `ACTOR_MESSAGE_QUEUE_SIZE` symmetrically. Leaves
/// `high_water_mark` alone (monotonic by design) and does not
/// touch `last_nonzero_epoch_ms` (best-effort observational
/// timestamp; brief overcount on failed sends is acceptable).
fn account_cancel_enqueue(queue_depth: &AtomicU64, proc_stats: &ProcQueueStats, actor_id: &str) {
    queue_depth.fetch_sub(1, Ordering::Relaxed);
    proc_stats.running_total.fetch_sub(1, Ordering::Relaxed);
    ACTOR_MESSAGE_QUEUE_SIZE.add(
        -1,
        hyperactor_telemetry::kv_pairs!("actor_id" => actor_id.to_owned()),
    );
}
use crate::ordering::OrderedSender;
use crate::ordering::OrderedSenderError;
use crate::ordering::SEQ_INFO;
use crate::ordering::SeqInfo;
use crate::ordering::Sequencer;
use crate::ordering::ordered_channel;
use crate::panic_handler;
use crate::supervision::ActorSupervisionEvent;

/// Identity assignment sent by a host as the first message on a duplex
/// attach connection. The child reads this to learn its [`ProcAddr`].
#[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub struct BootstrapAssignment {
    /// The assigned proc identity.
    pub proc_id: ProcAddr,
}
wirevalue::register_type!(BootstrapAssignment);

/// Sentinel message sent by an attach client as its first
/// [`MessageEnvelope`]. Hosts use this to distinguish attach requests
/// from regular inbound [`MessageEnvelope`] connections.
#[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub struct AttachRequest;
wirevalue::register_type!(AttachRequest);

/// Wire protocol for the host -> client direction on a duplex attach
/// connection.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
#[expect(
    clippy::large_enum_variant,
    reason = "wire-protocol enum; boxing Envelope ripples through all channel/networking construction and destructure sites and needs a wire-compatibility review — separate diff"
)]
pub enum Host2Client {
    /// First message: identity assignment from the host.
    Bootstrap(BootstrapAssignment),
    /// Subsequent messages: routed envelopes.
    Envelope(MessageEnvelope),
}
wirevalue::register_type!(Host2Client);

/// [`Rx<MessageEnvelope>`](channel::Rx) adapter that unwraps
/// [`Host2Client::Envelope`] from a duplex receiver.
pub struct AttachRx(pub channel::duplex::DuplexRx<Host2Client>);

#[async_trait]
impl channel::Rx<MessageEnvelope> for AttachRx {
    async fn recv(&mut self) -> Result<MessageEnvelope, ChannelError> {
        match self.0.recv().await? {
            Host2Client::Envelope(envelope) => Ok(envelope),
            Host2Client::Bootstrap(_) => Err(ChannelError::Other(anyhow::anyhow!(
                "unexpected bootstrap message after handshake"
            ))),
        }
    }

    fn addr(&self) -> ChannelAddr {
        self.0.addr()
    }

    async fn join(self) {
        self.0.join().await
    }
}

/// A proc instance is the runtime managing a single proc in Hyperactor.
/// It is responsible for spawning actors in the proc, multiplexing messages
/// to/within actors in the proc, and providing fallback routing to external
/// procs.
///
/// Procs are also responsible for maintaining the local supervision hierarchy.
#[derive(Clone)]
pub struct Proc {
    inner: Arc<ProcState>,
}

impl fmt::Debug for Proc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Proc")
            .field("proc_id", &self.inner.proc_id)
            .finish()
    }
}

struct ProcState {
    /// The proc's runtime identity. This should be globally unique,
    /// but is not (yet) for local-only procs.
    proc_id: ProcId,

    /// Shared ingress, egress, and advertised reachability state.
    gateway: Gateway,

    /// A muxer instance that has entries for every actor managed by
    /// the proc.
    proc_muxer: MailboxMuxer,

    /// Reserved root actor uids. Prevents races between concurrent
    /// `allocate_root_id` callers — insert returns false if the uid
    /// was already reserved.
    reserved_roots: DashSet<crate::id::Uid>,

    /// Reserved explicit child actor uids. Prevents races between concurrent
    /// `gspawn_uid` callers with the same uid.
    reserved_child_uids: DashSet<crate::id::Uid>,

    /// All actor instances in this proc.
    instances: DashMap<ActorId, WeakInstanceCell>,

    /// Root actor ids in this proc, tracked independently from uid shape.
    root_actors: DashSet<ActorId>,

    /// Proc-level queue-pressure accounting (PD-6 through PD-9).
    /// Runtime-driven — updated from `account_enqueue` /
    /// `account_dequeue`, not from publish-time sampling.
    /// `Arc`-wrapped so `HandlerPorts<A>` enqueue closures can share it.
    queue_stats: Arc<ProcQueueStats>,

    /// Snapshots of terminated actors for post-mortem introspection.
    /// Populated by the introspect task just before it exits on
    /// terminal status. Bounded by
    /// [`config::TERMINATED_SNAPSHOT_RETENTION`].
    terminated_snapshots: DashMap<ActorId, TerminatedSnapshot>,

    /// Used by root actors to send events to the actor coordinating
    /// supervision of root actors in this proc.
    supervision_coordinator_port: OnceLock<PortHandle<ActorSupervisionEvent>>,

    /// The actor ID of the supervision coordinator, if it lives on this proc.
    /// Used to ensure the coordinator is shut down last during proc teardown.
    supervision_coordinator_actor_id: OnceLock<ActorAddr>,

    /// Handle to the mailbox server task, if this proc was created with
    /// `Proc::direct()` or had `serve()` called on it. Used to
    /// gracefully stop the server and join it (flushing receive-side
    /// acks) during shutdown.
    mailbox_server_handle: std::sync::Mutex<Option<crate::mailbox::MailboxServerHandle>>,
}

struct TerminatedSnapshot {
    actor_addr: ActorAddr,
    payload: crate::introspect::IntrospectResult,
}

impl Drop for ProcState {
    fn drop(&mut self) {
        // We only want log ProcStatus::Dropped when ProcState is dropped,
        // rather than Proc is dropped. This is because we need to wait for
        // Proc::inner's ref count becomes 0.
        let proc_addr = self.proc_addr();
        tracing::info!(
            subject = %proc_addr.subject(),
            name = "ProcStatus",
            status = "Dropped"
        );
    }
}

impl ProcState {
    fn default_location(&self) -> Location {
        self.gateway.default_location()
    }

    fn set_default_location(&self, location: Location) {
        self.gateway.set_default_location(location)
    }

    fn proc_addr(&self) -> ProcAddr {
        self.gateway.proc_addr(&self.proc_id)
    }
}

/// Structured return type for [`Proc::actor_instance`].
///
/// Groups the instance, handle, and per-channel receivers that an
/// "inverted" actor caller needs to drive the actor manually.
pub struct ActorInstance<A: Actor> {
    /// The actor instance (used for sending/receiving messages, spawning children, etc.).
    pub instance: Instance<A>,
    /// Handle to the actor (used for lifecycle control and port access).
    pub handle: ActorHandle<A>,
    /// Supervision events delivered to this actor.
    pub supervision: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
    /// Control signals for the actor.
    pub signal: mpsc::UnboundedReceiver<Signal>,
    /// Primary work queue for handler dispatch.
    pub work: mpsc::UnboundedReceiver<WorkCell<A>>,
}

/// Builder for constructing a [`Proc`] with explicit identity and connectivity.
pub struct Builder<State = GlobalGateway> {
    proc_id: Option<ProcId>,
    state: State,
}

/// Builder state that attaches the proc to the process-wide global gateway.
pub struct GlobalGateway;

/// Builder state that attaches the proc to a shared gateway.
pub struct SharedGateway {
    gateway: Gateway,
}

/// Builder state that creates a private gateway with a custom forwarder.
pub struct PrivateGateway {
    forwarder: BoxedMailboxSender,
}

impl Builder<GlobalGateway> {
    /// Create a new proc builder.
    pub fn new() -> Self {
        Self {
            proc_id: None,
            state: GlobalGateway,
        }
    }

    /// Attach the proc to a shared gateway.
    pub fn shared_gateway(self, gateway: Gateway) -> Builder<SharedGateway> {
        Builder {
            proc_id: self.proc_id,
            state: SharedGateway { gateway },
        }
    }

    /// Use a private gateway with the provided forwarder.
    pub fn private_gateway(self, forwarder: BoxedMailboxSender) -> Builder<PrivateGateway> {
        Builder {
            proc_id: self.proc_id,
            state: PrivateGateway { forwarder },
        }
    }

    /// Build the proc.
    pub fn build(self) -> Result<Proc, anyhow::Error> {
        self.build_with_gateway(Gateway::global().clone())
    }
}

impl<State> Builder<State> {
    /// Set the proc identity.
    pub fn proc_id(mut self, proc_id: ProcId) -> Self {
        self.proc_id = Some(proc_id);
        self
    }

    fn build_with_gateway(self, gateway: Gateway) -> Result<Proc, anyhow::Error> {
        Self::build_proc(self.proc_id, gateway)
    }

    fn build_proc(proc_id: Option<ProcId>, gateway: Gateway) -> Result<Proc, anyhow::Error> {
        let proc_id = proc_id.unwrap_or_else(ProcId::anonymous);
        if is_legacy_pseudo_singleton_proc_id(&proc_id) {
            anyhow::bail!(
                "legacy pseudo-singleton proc id '{}' must be constructed with a dedicated Proc constructor",
                proc_id
            );
        }
        Ok(Proc::from_parts_unchecked(proc_id, gateway))
    }
}

impl Builder<SharedGateway> {
    /// Build the proc.
    pub fn build(self) -> Result<Proc, anyhow::Error> {
        let Builder {
            proc_id,
            state: SharedGateway { gateway },
        } = self;
        Self::build_proc(proc_id, gateway)
    }
}

impl Builder<PrivateGateway> {
    /// Build the proc.
    pub fn build(self) -> Result<Proc, anyhow::Error> {
        let Builder {
            proc_id,
            state: PrivateGateway { forwarder },
        } = self;
        let gateway = Gateway::configured(channel::reserve_local_addr().into(), forwarder);
        Self::build_proc(proc_id, gateway)
    }
}

impl Proc {
    fn from_parts_unchecked(proc_id: ProcId, gateway: Gateway) -> Self {
        let proc_addr = gateway.proc_addr(&proc_id);
        tracing::info!(
            subject = %proc_addr.subject(),
            name = "ProcStatus",
            status = "Created"
        );

        let proc = Self {
            inner: Arc::new(ProcState {
                proc_id: proc_id.clone(),
                gateway: gateway.clone(),
                proc_muxer: MailboxMuxer::new(),
                reserved_roots: DashSet::new(),
                reserved_child_uids: DashSet::new(),
                instances: DashMap::new(),
                root_actors: DashSet::new(),
                queue_stats: Arc::new(ProcQueueStats::new()),
                terminated_snapshots: DashMap::new(),
                supervision_coordinator_port: OnceLock::new(),
                supervision_coordinator_actor_id: OnceLock::new(),
                mailbox_server_handle: std::sync::Mutex::new(None),
            }),
        };
        gateway.attach(&proc);
        proc
    }

    fn from_parts(proc_id: ProcId, gateway: Gateway) -> Self {
        assert_not_legacy_pseudo_singleton_proc_id(&proc_id);
        Self::from_parts_unchecked(proc_id, gateway)
    }

    /// Create the legacy host-local client proc pseudo-singleton.
    pub fn legacy_local_pseudo_singleton(addr: ChannelAddr, forwarder: BoxedMailboxSender) -> Self {
        Self::legacy_pseudo_singleton(addr, LEGACY_LOCAL_PROC_NAME, forwarder)
    }

    /// Create the legacy host system proc pseudo-singleton.
    pub fn legacy_service_pseudo_singleton(
        addr: ChannelAddr,
        forwarder: BoxedMailboxSender,
    ) -> Self {
        Self::legacy_pseudo_singleton(addr, LEGACY_SERVICE_PROC_NAME, forwarder)
    }

    fn legacy_pseudo_singleton(
        addr: ChannelAddr,
        name: &'static str,
        forwarder: BoxedMailboxSender,
    ) -> Self {
        let proc_addr = ProcAddr::singleton(addr, name);
        Self::from_parts_unchecked(
            proc_addr.id().clone(),
            Gateway::configured(proc_addr.location().clone(), forwarder),
        )
    }

    /// Create a proc with an anonymous instance id on the default gateway.
    pub fn anonymous() -> Self {
        Self::builder()
            .build()
            .expect("anonymous proc builder is valid")
    }

    /// Create a proc with an instance id and display label on the default gateway.
    pub fn instance(label: impl AsRef<str>) -> Self {
        Self::builder()
            .proc_id(ProcId::instance(Label::strip(label.as_ref())))
            .build()
            .expect("instance proc builder is valid")
    }

    /// Create a proc with a singleton id on the default gateway.
    pub fn singleton(name: impl AsRef<str>) -> Self {
        Self::builder()
            .proc_id(ProcId::singleton(Label::strip(name.as_ref())))
            .build()
            .expect("singleton proc builder is valid")
    }

    /// Create a proc with a random id on a fresh local-only gateway.
    pub fn isolated() -> Self {
        Self::builder()
            .shared_gateway(Gateway::isolated())
            .build()
            .expect("isolated proc builder is valid")
    }

    /// Create a proc builder.
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Create a pre-configured proc with the given proc id and forwarder.
    pub fn configured(proc_id: impl Into<ProcAddr>, forwarder: BoxedMailboxSender) -> Self {
        let proc_addr = proc_id.into();
        Self::from_parts(
            proc_addr.id().clone(),
            Gateway::configured(proc_addr.location().clone(), forwarder),
        )
    }

    /// Create a new direct-addressed proc.
    ///
    /// The provided name is a display label. Direct procs are otherwise
    /// independent instances, so each one receives a unique proc id.
    pub fn direct(addr: ChannelAddr, name: String) -> Result<Self, ChannelError> {
        let (addr, rx) = channel::serve(addr)?;
        let proc_id = ProcAddr::instance(addr, name);
        let proc = Self::builder()
            .proc_id(proc_id.id().clone())
            .shared_gateway(Gateway::configured(
                proc_id.location().clone(),
                DialMailboxRouter::new().into_boxed(),
            ))
            .build()
            .expect("direct proc builder is valid");
        let handle = proc.gateway().serve_rx(rx);
        *proc.inner.mailbox_server_handle.lock().unwrap() = Some(handle);
        Ok(proc)
    }

    /// Connect to a host's duplex server and return a [`Proc`] whose
    /// identity is assigned by the host. Outbound messages are forwarded
    /// over the duplex channel; inbound messages are served into the
    /// proc's muxer. Mirrors [`Proc::direct`] but the identity and
    /// routing are managed by the remote host.
    pub async fn attach_to_host(addr: ChannelAddr) -> Result<Self, anyhow::Error> {
        use crate::channel::Rx;
        use crate::channel::Tx;
        let mut duplex_client = channel::duplex::dial::<MessageEnvelope, Host2Client>(addr)?;
        let duplex_tx = duplex_client.tx();
        let mut duplex_rx = duplex_client
            .take_rx()
            .expect("dial returns a fresh DuplexClient with rx present");
        // Send an AttachRequest envelope to signal attach intent.
        // The host deserializes the first message and enters the
        // attach protocol when it finds an AttachRequest. The
        // sender/dest ids are placeholders — on the happy path the
        // host consumes the envelope without routing it. Clearing
        // `return_undeliverable` closes the hazard path in case the
        // envelope ever escapes into the forwarder: it should be
        // dropped, not bounced to the fake sender.
        let signal_actor_id = ActorAddr::root(
            ProcAddr::singleton(ChannelAddr::any(channel::ChannelTransport::Local), "attach"),
            crate::id::Label::strip("attach"),
        );
        let signal_port = signal_actor_id.port_addr(crate::port::Port::from(0u64));
        let mut envelope = MessageEnvelope::serialize(
            signal_actor_id,
            signal_port,
            &AttachRequest,
            Default::default(),
        )?;
        envelope.set_return_undeliverable(false);
        duplex_tx.post(envelope);
        // Wait for the host to assign an identity.
        let assignment = match duplex_rx.recv().await? {
            Host2Client::Bootstrap(a) => a,
            Host2Client::Envelope(_) => {
                anyhow::bail!("expected bootstrap assignment as first message")
            }
        };
        let proc = Self::builder()
            .proc_id(assignment.proc_id.id().clone())
            .shared_gateway(Gateway::configured(
                assignment.proc_id.location().clone(),
                MailboxClient::new(duplex_tx).into_boxed(),
            ))
            .build()
            .expect("attached proc builder is valid");
        // Wrap the inner mailbox server handle so that stopping/
        // joining the outer handle also joins the dial-side
        // `DuplexClient`.
        let inner_handle = proc.gateway().serve_rx(AttachRx(duplex_rx));
        let (stopped_tx, mut stopped_rx) = tokio::sync::watch::channel(false);
        let wrapped_join = tokio::spawn(async move {
            let _ = stopped_rx.wait_for(|stopped| *stopped).await;
            inner_handle.stop("proc shutting down");
            let _ = inner_handle.await;
            duplex_client.join().await;
            Ok(())
        });
        let handle = crate::mailbox::MailboxServerHandle::from_parts(wrapped_join, stopped_tx);
        *proc.inner.mailbox_server_handle.lock().unwrap() = Some(handle);
        Ok(proc)
    }

    /// Set the supervision coordinator's port for this proc. Return Err if it is
    /// already set.
    pub fn set_supervision_coordinator(
        &self,
        port: PortHandle<ActorSupervisionEvent>,
    ) -> Result<(), anyhow::Error> {
        let actor_ref: ActorAddr = port.location().actor_addr();
        self.state()
            .supervision_coordinator_port
            .set(port)
            .map_err(|existing| anyhow::anyhow!("coordinator port is already set to {existing}"))?;
        let _ = self.state().supervision_coordinator_actor_id.set(actor_ref);
        Ok(())
    }

    /// The actor address of the supervision coordinator, if one is set and
    /// lives on this proc.
    pub fn supervision_coordinator_actor_addr(&self) -> Option<&ActorAddr> {
        self.state().supervision_coordinator_actor_id.get()
    }

    /// Handle a supervision event received by the proc. Attempt to forward it to the
    /// supervision coordinator port if one is set, otherwise crash the process.
    pub fn handle_unhandled_supervision_event(
        &self,
        cx: &impl context::Actor,
        event: ActorSupervisionEvent,
    ) {
        let result = match self.state().supervision_coordinator_port.get() {
            Some(port) => {
                port.post(cx, event.clone());
                Ok(())
            }
            None => {
                if !event.is_error() {
                    // Normal lifecycle events (e.g. clean stop) without a coordinator
                    // are silently dropped.
                    return;
                }
                Err(anyhow::anyhow!(
                    "coordinator port is not set for proc {}",
                    self.proc_addr(),
                ))
            }
        };
        if let Err(err) = result {
            if !event.is_error() {
                // Normal lifecycle events that fail to send (e.g. coordinator
                // mailbox already closed during shutdown) are silently dropped.
                tracing::debug!(
                    subject = %self.proc_addr().subject(),
                    "dropping non-error supervision event {}: {:?}",
                    event,
                    err
                );
                return;
            }
            tracing::error!(
                subject = %self.proc_addr().subject(),
                "could not propagate supervision event {} due to error: {:?}: crashing",
                event,
                err
            );

            std::process::exit(1);
        }
    }

    /// The proc's runtime identity.
    pub fn proc_id(&self) -> &ProcId {
        &self.state().proc_id
    }

    /// The proc's default advertised location.
    pub fn default_location(&self) -> Location {
        self.state().default_location()
    }

    /// Set the proc's default advertised location.
    pub fn set_default_location(&self, location: Location) {
        self.state().set_default_location(location)
    }

    /// The proc's routeable address using its default advertised location.
    pub fn proc_addr(&self) -> ProcAddr {
        self.state().proc_addr()
    }

    /// The proc's connectivity boundary.
    pub fn gateway(&self) -> Gateway {
        self.state().gateway.clone()
    }

    /// Return the process-global proc.
    pub fn global() -> Self {
        static GLOBAL_PROC: OnceLock<Proc> = OnceLock::new();
        GLOBAL_PROC
            .get_or_init(|| {
                let label = global_proc_label();
                Proc::instance(label.as_str())
            })
            .clone()
    }

    /// Return the proc for the current execution context.
    ///
    /// Actor callbacks run with their owning proc installed as the current
    /// proc. Outside an actor callback, this returns the process-global proc.
    pub fn current() -> Self {
        CURRENT_TASK_PROC
            .try_with(Clone::clone)
            .unwrap_or_else(|_| Self::global())
    }

    async fn with_current<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        CURRENT_TASK_PROC.scope(self.clone(), future).await
    }

    /// Shared sender used by the proc to forward messages to remote
    /// destinations.
    pub fn forwarder(&self) -> &BoxedMailboxSender {
        self.state().gateway.forwarder()
    }

    /// The proc's mailbox muxer, which routes messages to actors
    /// registered on this proc.
    pub fn muxer(&self) -> &MailboxMuxer {
        &self.inner.proc_muxer
    }

    /// Convenience accessor for state.
    fn state(&self) -> &ProcState {
        self.inner.as_ref()
    }

    /// Attach a mailbox to the proc with the provided root name.
    pub fn attach(&self, name: &str) -> Result<Mailbox, anyhow::Error> {
        let actor_id: ActorAddr = self.allocate_root_id(name)?;
        Ok(self.bind_mailbox(actor_id))
    }

    /// Attach a mailbox to the proc as a child actor.
    pub fn attach_child(&self, parent_id: &ActorAddr) -> Result<Mailbox, anyhow::Error> {
        let actor_id: ActorAddr = self.allocate_anonymous_child_id(parent_id);
        Ok(self.bind_mailbox(actor_id))
    }

    /// Bind a mailbox to the proc.
    fn bind_mailbox(&self, actor_id: ActorAddr) -> Mailbox {
        let mbox = Mailbox::new(actor_id);

        // TODO: T210748165 tie the muxer entry to the lifecycle of the mailbox held
        // by the caller. This will likely require a weak reference.
        self.state().proc_muxer.bind_mailbox(mbox.clone());
        mbox
    }

    /// Attach a mailbox to the proc with the provided root name, and bind an [`ActorAddr`].
    /// This is intended only for testing, and will be replaced by simpled utilities.
    pub fn attach_actor<R, M>(
        &self,
        name: &str,
    ) -> Result<(Client, ActorRef<R>, PortReceiver<M>), anyhow::Error>
    where
        M: RemoteMessage,
        R: Referable + RemoteHandles<M>,
    {
        let client = self.client(name);
        let (_handle, rx) = client.bind_handler_port::<M>();
        let actor_ref = ActorRef::attest(client.self_addr().clone());
        Ok((client, actor_ref, rx))
    }

    /// Spawn a root actor with a fresh uid labeled from the actor type.
    pub fn spawn<A: Actor>(&self, actor: A) -> ActorHandle<A> {
        let actor_id: ActorAddr = self.allocate_root_type::<A>();
        self.spawn_inner(actor_id, actor, None)
    }

    /// Spawn a root actor with a fresh uid carrying a display label.
    ///
    /// The label is descriptive only and does not participate in actor
    /// identity.
    pub fn spawn_with_label<A: Actor>(&self, label: &str, actor: A) -> ActorHandle<A> {
        let actor_id: ActorAddr = self.allocate_root_label(label);
        self.spawn_inner(actor_id, actor, None)
    }

    /// Spawn a root actor on this proc using an explicit uid.
    ///
    /// This is the explicit identity API, and the only root spawn API that
    /// permits singleton actor identity. The uid must be unique among root
    /// actors on this proc. Instance labels, if present, are descriptive only
    /// and do not affect uniqueness.
    pub fn spawn_with_uid<A: Actor>(
        &self,
        uid: Uid,
        actor: A,
    ) -> Result<ActorHandle<A>, anyhow::Error> {
        let actor_id: ActorAddr = self.allocate_root_uid(uid)?;
        Ok(self.spawn_inner(actor_id, actor, None))
    }

    /// Common spawn logic for both root and child actors.
    fn spawn_inner<A: Actor>(
        &self,
        actor_id: ActorAddr,
        actor: A,
        parent: Option<InstanceCell>,
    ) -> ActorHandle<A> {
        let (instance, receivers) = Instance::new(self.clone(), actor_id, false, parent);
        instance.start(actor, receivers)
    }

    /// Create a lightweight client instance (no actor loop, no
    /// introspect task).  This is safe to call outside a Tokio
    /// runtime — unlike [`actor_instance`], it never calls
    /// `tokio::spawn`.
    pub fn client(&self, label: &str) -> Client {
        let actor_id = self.allocate_client_id(label);
        let (instance, _receivers) =
            Instance::<ClientActor>::new(self.clone(), actor_id, false, None);
        instance.change_status(ActorStatus::Client);
        Client::new(instance)
    }

    /// Create a lightweight client instance that handles
    /// [`IntrospectMessage`].
    ///
    /// Like [`client`](Self::client), this creates a client-mode
    /// instance with no actor message loop. Unlike `client`, it
    /// spawns a dedicated introspect task, so the instance responds
    /// to `IntrospectMessage::Query` and is visible and navigable in
    /// admin tooling such as the mesh TUI.
    ///
    /// See CI-1, CI-2 in module doc.
    ///
    /// Requires an active Tokio runtime (calls `tokio::spawn`).
    pub fn introspectable_instance(
        &self,
        name: &str,
    ) -> Result<(Instance<()>, ActorHandle<()>), anyhow::Error> {
        let actor_id: ActorAddr = self.allocate_root_id(name)?;
        let (instance, receivers) = Instance::new(self.clone(), actor_id, false, None);
        let handle = ActorHandle::new(instance.inner.cell.clone(), instance.inner.ports.clone());
        instance.change_status(ActorStatus::Client);
        tokio::spawn(crate::introspect::serve_introspect(
            instance.inner.cell.clone(),
            receivers.introspect,
        ));
        Ok((instance, handle))
    }

    /// Create and return an actor instance, its handle, and its
    /// receivers. This allows actors to be "inverted": the caller can
    /// use the returned [`Instance`] to send and receive messages,
    /// launch child actors, etc. The actor itself does not handle any
    /// messages unless driven by the caller.
    pub fn actor_instance<A: Actor>(&self, name: &str) -> Result<ActorInstance<A>, anyhow::Error> {
        let actor_id: ActorAddr = self.allocate_root_id(name)?;
        let span = tracing::debug_span!(
            "actor_instance",
            subject = %actor_id.subject(),
        );
        let _guard = span.enter();
        let (instance, receivers) = Instance::new(self.clone(), actor_id.clone(), false, None);
        let handle = ActorHandle::new(instance.inner.cell.clone(), instance.inner.ports.clone());
        instance.change_status(ActorStatus::Client);

        tokio::spawn(crate::introspect::serve_introspect(
            instance.inner.cell.clone(),
            receivers.introspect,
        ));

        let (signal_rx, supervision_rx) = receivers.actor_loop.unwrap();
        Ok(ActorInstance {
            instance,
            handle,
            supervision: supervision_rx,
            signal: signal_rx,
            work: receivers.work,
        })
    }

    /// Traverse all actor trees in this proc, starting from root actors.
    pub fn traverse<F>(&self, f: &mut F)
    where
        F: FnMut(&InstanceCell, usize),
    {
        for entry in self.state().root_actors.iter() {
            if let Some(cell) = self.get_instance_by_id(entry.key()) {
                cell.traverse(f);
            }
        }
    }

    /// Proc-wide running total of queued work items.
    pub fn queue_depth_total(&self) -> u64 {
        self.state().queue_stats.running_total()
    }

    /// Maximum proc-wide queue depth observed since startup (PD-6).
    pub fn queue_depth_high_water_mark(&self) -> u64 {
        self.state().queue_stats.high_water_mark()
    }

    /// How long ago proc-wide queue depth was last non-zero (PD-7).
    pub fn last_nonzero_queue_depth_age_ms(&self) -> Option<u64> {
        self.state().queue_stats.last_nonzero_age_ms()
    }

    /// Look up an instance by ActorAddr.
    pub fn get_instance(&self, actor_id: &ActorAddr) -> Option<InstanceCell> {
        self.get_instance_by_id(actor_id.id())
    }

    /// Look up an instance by ActorId.
    pub fn get_instance_by_id(&self, actor_id: &ActorId) -> Option<InstanceCell> {
        self.state()
            .instances
            .get(actor_id)
            .and_then(|cell| cell.upgrade())
    }

    /// Returns the ActorAddrs of all root actors in this proc.
    pub fn root_actor_ids(&self) -> Vec<ActorAddr> {
        self.state()
            .root_actors
            .iter()
            .filter_map(|entry| {
                self.get_instance_by_id(entry.key())
                    .map(|cell| cell.actor_addr().clone())
            })
            .collect()
    }

    /// Returns the ActorAddrs of all live actors in this proc, including
    /// dynamically spawned children.
    ///
    /// An actor is considered live if its weak reference is
    /// upgradeable and its status is not terminal. This excludes
    /// actors whose `InstanceCell` has been dropped and actors that
    /// have stopped or failed but whose Arc is still held (e.g. by
    /// the introspect task during teardown).
    pub fn all_actor_ids(&self) -> Vec<ActorAddr> {
        self.state()
            .instances
            .iter()
            .filter_map(|entry| {
                let cell = entry.value().upgrade()?;
                (!cell.status().borrow().is_terminal()).then(|| cell.actor_addr().clone())
            })
            .collect()
    }

    /// Snapshot all instance ids from the DashMap without inspecting
    /// values. Each shard read lock is held only long enough to clone
    /// the id — no `Weak::upgrade()`, no `watch::borrow()`, no
    /// `is_terminal()` check. This minimises shard lock hold time to
    /// avoid convoy starvation with concurrent `insert`/`remove`
    /// operations during rapid actor churn.
    ///
    /// The returned list may include actors that are terminal or whose
    /// `WeakInstanceCell` no longer upgrades. Callers should tolerate stale
    /// ids (e.g. by handling "not found" on subsequent per-actor lookups).
    pub fn all_instance_keys(&self) -> Vec<ActorId> {
        self.state()
            .instances
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Look up a terminated actor's snapshot by ID.
    pub fn terminated_snapshot(
        &self,
        actor_id: &ActorAddr,
    ) -> Option<crate::introspect::IntrospectResult> {
        self.state()
            .terminated_snapshots
            .get(actor_id.id())
            .map(|entry| entry.value().payload.clone())
    }

    /// Return all terminated actor IDs currently retained.
    pub fn all_terminated_actor_ids(&self) -> Vec<ActorAddr> {
        self.state()
            .terminated_snapshots
            .iter()
            .map(|entry| entry.value().actor_addr.clone())
            .collect()
    }

    /// Create a child instance. Called from `Instance`.
    fn child_instance(&self, parent: InstanceCell) -> (Instance<()>, ActorHandle<()>) {
        let actor_id = self.allocate_anonymous_child_id(parent.actor_addr());
        let _ = tracing::debug_span!(
            "child_actor_instance",
            subject = %actor_id.subject(),
        );

        let (instance, _receivers) = Instance::new(self.clone(), actor_id, false, Some(parent));
        // Client-mode instance: no actor loop, no introspect task.
        // Receivers are intentionally dropped.
        let handle = ActorHandle::new(instance.inner.cell.clone(), instance.inner.ports.clone());
        instance.change_status(ActorStatus::Client);
        (instance, handle)
    }

    /// Spawn a child actor from the provided parent on this proc. The parent actor
    /// must already belong to this proc, a fact which is asserted in code.
    ///
    /// When spawn_child returns, the child has an associated cell and is linked
    /// with its parent.
    pub(crate) fn spawn_child<A: Actor>(&self, parent: InstanceCell, actor: A) -> ActorHandle<A> {
        let actor_id = self.allocate_child_id::<A>(parent.actor_addr());
        self.spawn_inner(actor_id, actor, Some(parent))
    }

    /// Spawn a child actor from the provided parent using an explicit uid.
    pub(crate) fn spawn_child_with_uid<A: Actor>(
        &self,
        parent: InstanceCell,
        uid: Uid,
        actor: A,
    ) -> Result<ActorHandle<A>, anyhow::Error> {
        let actor_id = self.ensure_child_uid(parent.actor_addr(), uid)?;
        Ok(self.spawn_inner(actor_id, actor, Some(parent)))
    }

    /// Spawn a named child actor. Same as `spawn_child` but the child
    /// gets a descriptive name instead of inheriting the parent's.
    /// Supervision linkage to parent is preserved.
    pub(crate) fn spawn_named_child<A: Actor>(
        &self,
        parent: InstanceCell,
        name: &str,
        actor: A,
    ) -> ActorHandle<A> {
        let actor_id = self.allocate_named_child_id(parent.actor_addr(), name);
        self.spawn_inner(actor_id, actor, Some(parent))
    }

    /// Call `abort` on the `JoinHandle` associated with the given
    /// root actor. If successful return `Some(root.clone())` else
    /// `None`.
    pub fn abort_root_actor(&self, root: &ActorId) -> Option<impl Future<Output = ActorAddr>> {
        self.state()
            .instances
            .get(root)
            .into_iter()
            .flat_map(|entry| entry.value().upgrade())
            .map(|cell| {
                let actor_addr = cell.actor_addr().clone();
                let r1 = actor_addr.clone();
                let r2 = actor_addr;
                // `Instance::start()` is infallible and should
                // complete quickly, so calling `wait()` on `actor_task_handle`
                // should be safe (i.e., not hang forever).
                async move {
                    tokio::task::spawn_blocking(move || {
                        let h = cell.inner.actor_task_handle.wait();
                        tracing::debug!("{}: aborting {:?}", r1, h);
                        h.abort();
                    })
                    .await
                    .unwrap();
                    r2
                }
            })
            .next()
    }

    /// Signals to a root actor to stop,
    /// returning a status observer if successful.
    pub fn stop_actor(
        &self,
        actor_id: &ActorId,
        reason: String,
    ) -> Option<watch::Receiver<ActorStatus>> {
        // Upgrade the weak ref and immediately drop the DashMap entry (read
        // guard) before doing anything with `cell`. InstanceCellState::drop
        // calls instances.remove(), which needs a write lock on the same shard.
        // Holding the read guard while cell drops would self-deadlock.
        let cell = match self.state().instances.get(actor_id) {
            None => {
                tracing::error!(subject = %self.proc_addr().subject(), "no actor {} found", actor_id);
                return None;
            }
            Some(entry) => entry.value().upgrade(),
        }; // entry (shard read lock) dropped here
        match cell {
            None => None, // the actor's cell has been dropped
            Some(cell) => {
                tracing::info!("sending stop signal to {}", cell.actor_addr());
                if let Err(err) = cell.signal(Signal::DrainAndStop(reason)) {
                    tracing::error!(
                        "failed to send stop signal to uid {}: {:?}",
                        cell.uid(),
                        err
                    );
                    None
                } else {
                    Some(cell.status().clone())
                }
            }
        }
    }

    /// Stop the proc. Returns a pair of:
    /// - the actors observed to stop;
    /// - the actors not observed to stop when timeout.
    #[hyperactor::instrument(fields(subject = self.proc_addr().subject().to_string()))]
    pub async fn destroy_and_wait(
        &mut self,
        timeout: Duration,
        reason: &str,
    ) -> Result<(Vec<ActorAddr>, Vec<ActorAddr>), anyhow::Error> {
        tracing::debug!("proc stopping");

        let coordinator_id = self.supervision_coordinator_actor_addr().cloned();

        // Phase 1: stop all root actors except the supervision coordinator
        // (which must stay alive to receive stop events from the others).
        let mut statuses = HashMap::new();
        for actor_id in self
            .state()
            .root_actors
            .iter()
            .filter_map(|entry| self.get_instance_by_id(entry.key()))
            .filter(|cell| !matches!(*cell.status().borrow(), ActorStatus::Client))
            .map(|cell| cell.actor_addr().clone())
            .collect::<Vec<_>>()
        {
            if coordinator_id.as_ref() == Some(&actor_id) {
                continue;
            }
            if let Some(status) = self.stop_actor(actor_id.id(), reason.to_string()) {
                statuses.insert(actor_id, status);
            }
        }
        tracing::debug!("non-coordinator actors stopped");

        let waits: Vec<_> = statuses
            .iter_mut()
            .map(|(actor_id, root)| {
                let actor_id = actor_id.clone();
                async move {
                    tokio::time::timeout(
                        timeout,
                        root.wait_for(|state: &ActorStatus| state.is_terminal()),
                    )
                    .await
                    .ok()
                    .map(|_| actor_id)
                }
            })
            .collect();

        let results = futures::future::join_all(waits).await;
        let mut stopped_actors: Vec<_> = results
            .iter()
            .filter_map(|actor_id| actor_id.as_ref())
            .cloned()
            .collect();
        let aborted_actors: Vec<_> = statuses
            .iter()
            .filter(|(actor_id, _)| !stopped_actors.contains(actor_id))
            .map(|(actor_id, _)| {
                let f = self.abort_root_actor(actor_id.id());
                async move {
                    let _ = if let Some(f) = f { Some(f.await) } else { None };
                    // If `is_none(&_)` then the associated actor's
                    // instance cell was already dropped when we went
                    // to call `abort()` on the cell's task handle.

                    actor_id.clone()
                }
            })
            .collect();
        let mut aborted_actors = futures::future::join_all(aborted_actors).await;

        // Phase 2: now that all other actors have stopped, request the
        // supervision coordinator to stop. Their terminal supervision
        // events have already been enqueued by this point, and the
        // coordinator's DrainAndStop path drains queued supervision
        // events before exiting.
        if let Some(ref coord_id) = coordinator_id
            && let Some(mut status) = self.stop_actor(coord_id.id(), reason.to_string())
        {
            let stopped =
                tokio::time::timeout(timeout, status.wait_for(|s: &ActorStatus| s.is_terminal()))
                    .await
                    .is_ok();
            if stopped {
                stopped_actors.push(coord_id.clone());
            } else {
                if let Some(f) = self.abort_root_actor(coord_id.id()) {
                    f.await;
                }
                aborted_actors.push(coord_id.clone());
            }
        }

        // Flush the gateway so that any messages posted during
        // teardown (e.g. supervision events) are wire-delivered
        // before we tear down the proc's networking. The flush is
        // best-effort: if the remote side has already torn down its
        // networking, acks may never arrive and flush would hang
        // indefinitely, so we bound it with a configurable timeout.
        let flush_timeout = hyperactor_config::global::get(crate::config::FORWARDER_FLUSH_TIMEOUT);
        let gateway = self.gateway();
        match tokio::time::timeout(flush_timeout, gateway.flush()).await {
            Ok(Err(err)) => {
                tracing::warn!("gateway flush failed during proc exit: {:?}", err);
            }
            Err(_elapsed) => {
                tracing::warn!("gateway flush timed out during proc exit");
            }
            Ok(Ok(())) => {}
        }

        tracing::info!(
            "destroy_and_wait: {} actors stopped, {} actors aborted",
            stopped_actors.len(),
            aborted_actors.len()
        );
        Ok((stopped_actors, aborted_actors))
    }

    /// Resolve an actor reference to a **live** actor on this proc.
    ///
    /// Returns `None` if:
    /// - the actor was never spawned here,
    /// - the actor's `InstanceCell` has been dropped, or
    /// - the actor's status is terminal (stopped or failed).
    ///
    /// The terminal-status check guards a race window: the introspect
    /// task (`serve_introspect`) holds a strong `InstanceCell` Arc
    /// and drops it only after observing terminal status. Between the
    /// actor reaching terminal and the introspect task reacting,
    /// `upgrade()` on the weak ref succeeds even though the actor is
    /// dead. The `is_terminal()` check closes that window. Once the
    /// introspect task exits, the Arc is dropped and `upgrade()`
    /// returns `None` on its own.
    ///
    /// Bounds:
    /// - `R: Actor` — must be a real actor that can live in this
    ///   proc.
    /// - `R: Referable` — required because the input is an
    ///   `ActorRef<R>`.
    pub fn resolve_actor_ref<R: Actor + Referable>(
        &self,
        actor_ref: &ActorRef<R>,
    ) -> Option<ActorHandle<R>> {
        let cell = self
            .inner
            .instances
            .get(actor_ref.actor_addr().id())?
            .upgrade()?;
        // An actor whose status is terminal has stopped processing
        // messages even if its InstanceCell Arc is still alive (e.g.
        // held by the introspect task during teardown).
        if cell.status().borrow().is_terminal() {
            return None;
        }
        cell.downcast_handle()
    }

    /// Create a root allocation in the proc.
    ///
    /// Uses `reserved_roots` to prevent races between concurrent callers.
    fn allocate_root_id(&self, name: &str) -> Result<ActorAddr, anyhow::Error> {
        self.reserve_root(Uid::singleton(Label::strip(name)))
    }

    /// Create a root allocation with fresh identity and the actor type label.
    fn allocate_root_type<A: Actor>(&self) -> ActorAddr {
        self.root_addr(Uid::instance(default_actor_label::<A>()))
    }

    /// Create a root allocation with a display label and fresh identity.
    fn allocate_root_label(&self, label: &str) -> ActorAddr {
        self.root_addr(Uid::instance(Label::strip(label)))
    }

    /// Create a root allocation in the proc from an explicit uid.
    fn allocate_root_uid(&self, uid: Uid) -> Result<ActorAddr, anyhow::Error> {
        self.reserve_root(uid)
    }

    fn allocate_client_id(&self, label: &str) -> ActorAddr {
        let actor_id = if label.is_empty() {
            ActorId::anonymous(self.proc_id().clone())
        } else {
            ActorId::instance(Label::strip(label), self.proc_id().clone())
        };
        ActorAddr::new(actor_id, self.default_location())
    }

    fn reserve_root(&self, uid: Uid) -> Result<ActorAddr, anyhow::Error> {
        let actor_id = ActorId::new(uid.clone(), self.proc_id().clone(), None);
        if !self.state().reserved_roots.insert(uid) {
            anyhow::bail!("an actor with id '{}' has already been spawned", actor_id)
        }
        Ok(ActorAddr::new(actor_id, self.default_location()))
    }

    fn root_addr(&self, uid: Uid) -> ActorAddr {
        ActorAddr::new(
            ActorId::new(uid, self.proc_id().clone(), None),
            self.default_location(),
        )
    }

    /// Create a child allocation in the proc.
    pub(crate) fn allocate_anonymous_child_id(&self, parent_id: &ActorAddr) -> ActorAddr {
        assert_eq!(parent_id.proc_id(), self.proc_id());
        ActorAddr::new(
            ActorId::anonymous(self.proc_id().clone()),
            self.default_location(),
        )
    }

    /// Create a child allocation in the proc using the actor type label.
    pub(crate) fn allocate_child_id<A: Actor>(&self, parent_id: &ActorAddr) -> ActorAddr {
        assert_eq!(parent_id.proc_id(), self.proc_id());
        let actor_id = ActorId::instance(default_actor_label::<A>(), self.proc_id().clone());
        ActorAddr::new(actor_id, self.default_location())
    }

    /// Ensure that the requested child uid is available in this proc.
    fn ensure_child_uid(
        &self,
        parent_id: &ActorAddr,
        uid: Uid,
    ) -> Result<ActorAddr, anyhow::Error> {
        assert_eq!(parent_id.proc_id(), self.proc_id());
        let actor_id = ActorId::new(uid.clone(), self.proc_id().clone(), None);
        let actor_addr = ActorAddr::new(actor_id, self.default_location());
        if !self.state().reserved_child_uids.insert(uid) {
            anyhow::bail!("an actor with id {} has already been spawned", actor_addr);
        }
        Ok(actor_addr)
    }

    /// Allocate an actor ID with a custom name on this proc.
    pub(crate) fn allocate_named_child_id(&self, parent_id: &ActorAddr, name: &str) -> ActorAddr {
        assert_eq!(parent_id.proc_id(), self.proc_id());
        let proc_id = self.proc_id().clone();
        let actor_id = crate::id::ActorId::instance(crate::id::Label::strip(name), proc_id);
        ActorAddr::new(actor_id, self.default_location())
    }

    /// Downgrade to a weak reference that doesn't prevent the proc from being dropped.
    pub fn downgrade(&self) -> WeakProc {
        WeakProc::new(self)
    }

    /// Flush the gateway so that any buffered messages are
    /// wire-delivered before the proc's networking is torn down.
    pub async fn flush(&self) -> Result<(), anyhow::Error> {
        self.gateway().flush().await
    }

    /// Stop and join the mailbox server, flushing receive-side acks.
    ///
    /// This stops the `MailboxServer::serve` loop and awaits its
    /// completion, which runs `Rx::join()` to flush any pending
    /// transport-level acks before the channel is torn down.
    ///
    /// No-op if no mailbox server handle is stored (e.g. for
    /// `Proc::configured` or `Proc::isolated` procs that don't serve).
    pub async fn join_mailbox_server(&self) {
        let handle = self.inner.mailbox_server_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.stop("proc shutting down");
            let _ = handle.await;
        }
    }

    pub(crate) fn is_local_delivery_target(&self, dest_proc: &ProcAddr) -> bool {
        let local_proc_id = self.proc_id();
        if requires_location_for_local_delivery_identity(dest_proc.id()) {
            // TODO: check all bound addresses for this proc, not only
            // the current default advertised location.
            return dest_proc.id() == local_proc_id
                && dest_proc.location() == &self.default_location();
        }

        dest_proc.id() == local_proc_id
    }
}

fn requires_location_for_local_delivery_identity(proc_id: &ProcId) -> bool {
    // Temporary hyperactor_mesh compatibility hack: host bootstrap
    // still creates a `service` proc and a `local` proc in every host
    // process, so those proc ids are not globally unique. Until those
    // construction paths are assigned instance ids, local delivery for
    // those two ids must keep the old full-address comparison.
    is_legacy_pseudo_singleton_proc_id(proc_id)
}

fn default_actor_label<A>() -> Label {
    let type_name = std::any::type_name::<A>();
    let type_name = type_name
        .split_once('<')
        .map_or(type_name, |(base, _)| base);
    Label::strip(type_name.rsplit("::").next().unwrap_or(type_name))
}

fn global_proc_label() -> Label {
    let hostname = hostname::get().expect("hostname should be available");
    global_proc_label_from(&hostname.to_string_lossy(), std::process::id())
}

fn global_proc_label_from(hostname: &str, pid: u32) -> Label {
    let short_hostname = hostname
        .split_once('.')
        .map_or(hostname, |(short, _)| short);
    Label::strip(&format!("{}-{}", short_hostname, pid))
}

fn assert_not_legacy_pseudo_singleton_proc_id(proc_id: &ProcId) {
    if is_legacy_pseudo_singleton_proc_id(proc_id) {
        panic!(
            "legacy pseudo-singleton proc id '{}' must be constructed with a dedicated Proc constructor",
            proc_id
        );
    }
}

fn is_legacy_pseudo_singleton_proc_id(proc_id: &ProcId) -> bool {
    matches!(
        proc_id.uid(),
        Uid::Singleton(label) if is_legacy_pseudo_singleton_label(label)
    )
}

fn is_legacy_pseudo_singleton_label(label: &Label) -> bool {
    matches!(
        label.as_str(),
        LEGACY_SERVICE_PROC_NAME | LEGACY_LOCAL_PROC_NAME
    )
}

#[async_trait]
impl MailboxSender for Proc {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let dest_proc = envelope.dest().actor_addr().proc_addr();
        if self.is_local_delivery_target(&dest_proc) {
            self.state().proc_muxer.post(envelope, return_handle)
        } else {
            self.state().gateway.post(envelope, return_handle)
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        self.gateway().flush().await
    }
}

/// A weak reference to a Proc that doesn't prevent it from being dropped.
#[derive(Clone, Debug)]
pub struct WeakProc(Weak<ProcState>);

impl WeakProc {
    fn new(proc: &Proc) -> Self {
        Self(Arc::downgrade(&proc.inner))
    }

    /// Upgrade to a strong Proc reference, if the proc is still alive.
    pub fn upgrade(&self) -> Option<Proc> {
        self.0.upgrade().map(|inner| Proc { inner })
    }
}

#[async_trait]
impl MailboxSender for WeakProc {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match self.upgrade() {
            Some(proc) => proc.post(envelope, return_handle),
            None => {
                let target = envelope.dest().clone();
                let failure =
                    DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                        target,
                        TransportFailureReason::LinkUnavailable("proc is gone".to_string()),
                    )));
                envelope.undeliverable_with_failure(
                    DeliveryError::BrokenLink("fail to upgrade WeakProc".to_string()),
                    failure,
                    return_handle,
                )
            }
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        match self.upgrade() {
            Some(proc) => proc.flush().await,
            None => Ok(()),
        }
    }
}

/// Represents a single work item used by the instance to dispatch to
/// actor handles. Specifically, this enables handler polymorphism.
pub struct WorkCell<A: Actor + Send>(
    Box<
        dyn for<'a> FnOnce(
                &'a mut A,
                &'a Instance<A>,
            )
                -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + 'a + Send>>
            + Send
            + Sync,
    >,
);

impl<A: Actor + Send> WorkCell<A> {
    /// Create a new WorkCell from a concrete function (closure).
    fn new(
        f: impl for<'a> FnOnce(
            &'a mut A,
            &'a Instance<A>,
        )
            -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + 'a + Send>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self(Box::new(f))
    }

    /// Handle the message represented by this work cell.
    pub fn handle<'a>(
        self,
        actor: &'a mut A,
        instance: &'a Instance<A>,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'a>> {
        (self.0)(actor, instance)
    }
}

/// Context for a message currently being handled by an Instance.
pub struct Context<'a, A: Actor> {
    instance: &'a Instance<A>,
    headers: Flattrs,
}

impl<'a, A: Actor> Context<'a, A> {
    /// Construct a new Context.
    pub fn new(instance: &'a Instance<A>, headers: Flattrs) -> Self {
        Self { instance, headers }
    }

    /// Get a reference to the message headers.
    pub fn headers(&self) -> &Flattrs {
        &self.headers
    }
}

impl<A: Actor> Deref for Context<'_, A> {
    type Target = Instance<A>;

    fn deref(&self) -> &Self::Target {
        self.instance
    }
}

/// An actor instance. This is responsible for managing a running actor, including
/// its full lifecycle, supervision, signal management, etc. Instances can represent
/// a managed actor or a "client" actor that has joined the proc.
pub struct Instance<A: Actor> {
    inner: Arc<InstanceState<A>>,
}

impl<A: Actor> fmt::Debug for Instance<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Instance").field("inner", &"..").finish()
    }
}

struct InstanceState<A: Actor> {
    /// The proc that owns this instance.
    proc: Proc,

    /// The instance cell that manages instance hierarchy.
    cell: InstanceCell,

    /// The mailbox associated with the actor.
    mailbox: Mailbox,

    ports: Arc<HandlerPorts<A>>,

    /// Runtime-owned delayed-post scheduler.
    delayed_posts: DelayedPosts<A>,

    /// A watch for communicating the actor's state.
    status_tx: watch::Sender<ActorStatus>,

    /// This instance's globally unique ID.
    id: Uuid,

    /// Used to assign sequence numbers for messages sent from this actor.
    sequencer: Sequencer,

    /// Per-instance local storage.
    instance_locals: ActorLocalStorage,
}

type DelayedPost<A> = Box<dyn FnOnce(&Instance<A>) + Send>;

trait PostAfterEndpoint<A: Actor, M: Message>: Send {
    fn endpoint_location(&self) -> crate::EndpointLocation;

    fn into_delayed_post(self, message: M) -> DelayedPost<A>;
}

impl<A, M> PostAfterEndpoint<A, M> for &Instance<A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::EndpointLocation::Actor(self.self_addr().clone())
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        let dest = self.clone_for_py();
        Box::new(move |this| crate::Endpoint::post(&dest, this, message))
    }
}

impl<A, M> PostAfterEndpoint<A, M> for &Context<'_, A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::EndpointLocation::Actor(self.self_addr().clone())
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        let dest = self.clone_for_py();
        Box::new(move |this| crate::Endpoint::post(&dest, this, message))
    }
}

impl<A, M> PostAfterEndpoint<A, M> for Instance<A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::EndpointLocation::Actor(self.self_addr().clone())
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        Box::new(move |this| crate::Endpoint::post(&self, this, message))
    }
}

impl<A, B, M> PostAfterEndpoint<A, M> for ActorHandle<B>
where
    A: Actor,
    B: Actor + Handler<M>,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::Endpoint::endpoint_location(&self)
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        Box::new(move |this| crate::Endpoint::post(&self, this, message))
    }
}

impl<A, M> PostAfterEndpoint<A, M> for PortHandle<M>
where
    A: Actor,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::Endpoint::endpoint_location(&self)
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        Box::new(move |this| crate::Endpoint::post(&self, this, message))
    }
}

impl<A, M> PostAfterEndpoint<A, M> for OncePortHandle<M>
where
    A: Actor,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::Endpoint::endpoint_location(self)
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        Box::new(move |this| crate::Endpoint::post(self, this, message))
    }
}

impl<A, B, M> PostAfterEndpoint<A, M> for ActorRef<B>
where
    A: Actor,
    B: Referable + RemoteHandles<M>,
    M: RemoteMessage,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::Endpoint::endpoint_location(&self)
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        Box::new(move |this| crate::Endpoint::post(&self, this, message))
    }
}

impl<A, M> PostAfterEndpoint<A, M> for crate::PortRef<M>
where
    A: Actor,
    M: RemoteMessage,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::Endpoint::endpoint_location(&self)
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        Box::new(move |this| crate::Endpoint::post(&self, this, message))
    }
}

impl<A, M> PostAfterEndpoint<A, M> for crate::OncePortRef<M>
where
    A: Actor,
    M: RemoteMessage,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::Endpoint::endpoint_location(self)
    }

    fn into_delayed_post(self, message: M) -> DelayedPost<A> {
        Box::new(move |this| crate::Endpoint::post(self, this, message))
    }
}

struct DelayedPosts<A: Actor> {
    ingress: Arc<DelayedPostIngressGate>,
    state: Mutex<DelayedPostState<A>>,
    notify: Notify,
}

struct DelayedPostState<A: Actor> {
    queue: BTreeMap<(tokio::time::Instant, u64), DelayedPost<A>>,
    next_order: u64,
}

impl<A: Actor> DelayedPosts<A> {
    fn new() -> Self {
        Self {
            ingress: Arc::new(DelayedPostIngressGate::new()),
            state: Mutex::new(DelayedPostState {
                queue: BTreeMap::new(),
                next_order: 0,
            }),
            notify: Notify::new(),
        }
    }

    fn push(&self, deadline: tokio::time::Instant, post: DelayedPost<A>) {
        let mut state = self.state.lock().unwrap();
        let order = state.next_order;
        state.next_order = state.next_order.wrapping_add(1);
        state.queue.insert((deadline, order), post);
        drop(state);
        self.notify.notify_one();
    }

    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        self.state
            .lock()
            .unwrap()
            .queue
            .keys()
            .next()
            .map(|(deadline, _)| *deadline)
    }

    fn pop_due(&self, now: tokio::time::Instant) -> Vec<DelayedPost<A>> {
        let mut posts = Vec::new();
        let mut state = self.state.lock().unwrap();
        while let Some((&(deadline, _), _)) = state.queue.first_key_value() {
            if deadline > now {
                break;
            }
            let (_, post) = state.queue.pop_first().expect("delayed post should exist");
            posts.push(post);
        }
        posts
    }

    fn drain(&self) {
        self.ingress.drain();
    }

    fn is_draining(&self) -> bool {
        self.ingress.is_draining()
    }
}

const DELAYED_POST_INGRESS_DRAINING: usize = 1usize << (usize::BITS as usize - 1);
const DELAYED_POST_INGRESS_ACTIVE_MASK: usize = !DELAYED_POST_INGRESS_DRAINING;

struct DelayedPostIngressGate {
    state: AtomicUsize,
    wait_lock: Mutex<()>,
    drained: Condvar,
}

struct DelayedPostIngressGuard {
    gate: Arc<DelayedPostIngressGate>,
}

impl DelayedPostIngressGate {
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            wait_lock: Mutex::new(()),
            drained: Condvar::new(),
        }
    }

    fn try_enter(self: &Arc<Self>) -> Result<DelayedPostIngressGuard, ()> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & DELAYED_POST_INGRESS_DRAINING != 0 {
                return Err(());
            }

            let active = state & DELAYED_POST_INGRESS_ACTIVE_MASK;
            assert!(
                active < DELAYED_POST_INGRESS_ACTIVE_MASK,
                "too many active delayed post sends"
            );

            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(DelayedPostIngressGuard {
                        gate: Arc::clone(self),
                    });
                }
                Err(next_state) => state = next_state,
            }
        }
    }

    fn drain(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & DELAYED_POST_INGRESS_DRAINING != 0 {
                break;
            }
            match self.state.compare_exchange_weak(
                state,
                state | DELAYED_POST_INGRESS_DRAINING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next_state) => state = next_state,
            }
        }

        let mut wait_guard = self.wait_lock.lock().unwrap();
        while self.state.load(Ordering::Acquire) & DELAYED_POST_INGRESS_ACTIVE_MASK != 0 {
            wait_guard = self.drained.wait(wait_guard).unwrap();
        }
    }

    fn is_draining(&self) -> bool {
        self.state.load(Ordering::Acquire) & DELAYED_POST_INGRESS_DRAINING != 0
    }
}

impl Drop for DelayedPostIngressGuard {
    fn drop(&mut self) {
        let previous = self.gate.state.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous & DELAYED_POST_INGRESS_ACTIVE_MASK != 0,
            "delayed post ingress active count underflow"
        );
        if previous & DELAYED_POST_INGRESS_DRAINING != 0
            && previous & DELAYED_POST_INGRESS_ACTIVE_MASK == 1
        {
            let _wait_guard = self.gate.wait_lock.lock().unwrap();
            self.gate.drained.notify_all();
        }
    }
}

impl<A: Actor> InstanceState<A> {
    fn self_addr(&self) -> &ActorAddr {
        self.mailbox.actor_addr()
    }
}

impl<A: Actor> Drop for InstanceState<A> {
    fn drop(&mut self) {
        self.status_tx.send_if_modified(|status| {
            if status.is_terminal() {
                false
            } else {
                tracing::info!(
                    name = "ActorStatus",
                    actor_id = %self.self_addr(),
                    actor_name = self.self_addr().log_name(),
                    status = "Stopped",
                    prev_status = status.arm().unwrap_or("unknown"),
                    "instance is dropped",
                );
                *status = ActorStatus::Stopped("instance is dropped".into());
                true
            }
        });
    }
}

/// Receivers created by [`Instance::new`] that must be threaded to
/// their respective consumers (actor loop, introspect task, etc.).
///
/// # Invariant
///
/// See S10 in `introspect` module doc.
pub struct InstanceReceivers<A: Actor> {
    /// Signal and supervision receivers for the actor loop. `None`
    /// for detached/client instances that don't run an actor loop.
    actor_loop: Option<(
        mpsc::UnboundedReceiver<Signal>,
        mpsc::UnboundedReceiver<ActorSupervisionEvent>,
    )>,
    /// Work queue for dispatching messages to actor handlers.
    work: mpsc::UnboundedReceiver<WorkCell<A>>,
    /// Introspect message receiver for the dedicated introspect task.
    introspect: PortReceiver<IntrospectMessage>,
}

impl<A: Actor> Instance<A> {
    /// Create a new actor instance in Created state.
    fn new(
        proc: Proc,
        actor_id: ActorAddr,
        detached: bool,
        parent: Option<InstanceCell>,
    ) -> (Self, InstanceReceivers<A>) {
        // Set up messaging
        let mailbox = Mailbox::new(actor_id.clone());
        let (work_tx, work_rx) = ordered_channel(
            actor_id.to_string(),
            hyperactor_config::global::get(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER),
        );
        let queue_depth = Arc::new(AtomicU64::new(0));
        let proc_stats = Arc::clone(&proc.state().queue_stats);
        let ports: Arc<HandlerPorts<A>> = Arc::new(HandlerPorts::new(
            mailbox.clone(),
            work_tx,
            Arc::clone(&queue_depth),
            proc_stats,
        ));
        proc.state().proc_muxer.bind_mailbox(mailbox.clone());
        let (status_tx, status_rx) = watch::channel(ActorStatus::Created);

        let actor_type = match TypeInfo::of::<A>() {
            Some(info) => ActorType::Named(info),
            None => ActorType::Anonymous(std::any::type_name::<A>()),
        };
        let actor_loop_ports = if detached {
            None
        } else {
            let (signal_tx, signal_receiver) = mpsc::unbounded_channel::<Signal>();
            let (supervision_tx, supervision_receiver) =
                mpsc::unbounded_channel::<ActorSupervisionEvent>();
            Some((
                (signal_tx, supervision_tx),
                (signal_receiver, supervision_receiver),
            ))
        };

        let (actor_loop, actor_loop_receivers) = actor_loop_ports.unzip();

        // Introspect port: a separate channel handled by a dedicated
        // tokio task (not the actor's message loop). bind_handler_port()
        // registers in the mailbox
        // dispatch table at IntrospectMessage::port().
        //
        // Exercises S3, S4, S9 (see introspect module doc).
        let (introspect_port, introspect_receiver) = mailbox.open_port::<IntrospectMessage>();
        introspect_port.bind_handler_port();

        let instance_id = Uuid::now_v7();

        // Type-erased snapshot callback: captures `Arc<HandlerPorts<A>>::clone()`
        // (the typed Arc), which lets `InstanceCellState` invoke
        // `OrderedSender::snapshot` from non-generic code. Only the typed
        // ports are captured; nothing cyclic (no Instance, no InstanceCell).
        let workq_ports = ports.clone();
        let inbound_ordering_snapshot: Option<
            Box<dyn Fn() -> crate::ordering::OrderingSnapshot + Send + Sync>,
        > = Some(Box::new(move || workq_ports.workq.snapshot()));

        let cell = InstanceCell::new(
            actor_id,
            instance_id,
            actor_type,
            proc.clone(),
            actor_loop,
            status_rx,
            parent,
            ports.clone(),
            queue_depth,
            inbound_ordering_snapshot,
        );
        let inner = Arc::new(InstanceState {
            proc,
            cell,
            mailbox,
            ports,
            delayed_posts: DelayedPosts::new(),
            status_tx,
            sequencer: Sequencer::new(instance_id),
            id: instance_id,
            instance_locals: ActorLocalStorage::new(),
        });
        (
            Self { inner },
            InstanceReceivers {
                actor_loop: actor_loop_receivers,
                work: work_rx,
                introspect: introspect_receiver,
            },
        )
    }

    /// Notify subscribers of a change in the actors status and bump counters with the duration which
    /// the last status was active for.
    #[track_caller]
    pub fn change_status(&self, new: ActorStatus) {
        let old = self.inner.status_tx.send_replace(new.clone());
        // 2 cases are allowed:
        // * non-terminal -> non-terminal
        // * non-terminal -> terminal
        // terminal -> terminal is not allowed unless it is the same status (no-op).
        // terminal -> non-terminal is never allowed.
        assert!(
            !old.is_terminal() && !new.is_terminal()
                || !old.is_terminal() && new.is_terminal()
                || old == new,
            "actor changing status illegally, only allow non-terminal -> non-terminal \
            and non-terminal -> terminal statuses. actor_id={}, prev_status={}, status={}",
            self.self_addr(),
            old,
            new
        );
        // Actor status changes between Idle and Processing when handling every
        // message. It creates too many logs if we want to log these 2 states.
        // Also, sometimes the actor transitions from Processing -> Processing.
        // Therefore we skip the status changes between them.
        if !((old.is_idle() && new.is_processing())
            || (old.is_processing() && new.is_idle())
            || old == new)
        {
            let new_status = new.arm().unwrap_or("unknown");
            let change_reason = match &new {
                ActorStatus::Failed(reason) => reason.to_string(),
                ActorStatus::Stopped(reason) => reason.clone(),
                _ => "".to_string(),
            };
            tracing::info!(
                name = "ActorStatus",
                actor_id = %self.self_addr(),
                actor_name = self.self_addr().log_name(),
                status = new_status,
                prev_status = old.arm().unwrap_or("unknown"),
                caller = %PanicLocation::caller(),
                change_reason,
            );
            let actor_id = hash_to_u64(self.self_addr());
            notify_actor_status_changed(ActorStatusEvent {
                id: generate_actor_status_event_id(actor_id),
                timestamp: std::time::SystemTime::now(),
                actor_id,
                new_status: new_status.to_string(),
                reason: if change_reason.is_empty() {
                    None
                } else {
                    Some(change_reason)
                },
            });
        }
    }

    fn is_terminal(&self) -> bool {
        self.inner.status_tx.borrow().is_terminal()
    }

    fn is_stopping(&self) -> bool {
        self.inner.status_tx.borrow().is_stopping()
    }

    /// This instance's actor address.
    pub fn self_addr(&self) -> &ActorAddr {
        self.inner.self_addr()
    }

    /// Report a message that could not be delivered and could not be returned.
    pub(crate) fn report_lost_message(&self, lost: crate::mailbox::LostMessage) {
        static REPORT_LOST_WARNED_MAILBOXES: OnceLock<DashSet<ActorAddr>> = OnceLock::new();

        let mailbox = &self.inner.mailbox;
        let return_handle = mailbox.bound_return_handle().unwrap_or_else(|| {
            let actor_id = mailbox.actor_addr();
            if REPORT_LOST_WARNED_MAILBOXES
                .get_or_init(DashSet::new)
                .insert(actor_id.clone())
            {
                let bt = std::backtrace::Backtrace::force_capture();
                tracing::warn!(
                    actor_id = ?actor_id,
                    backtrace = ?bt,
                    "actor attempted to report a lost message without binding Undeliverable<MessageEnvelope>"
                );
            }
            crate::mailbox::monitored_return_handle()
        });

        if let Err(error) =
            return_handle.try_post(self, crate::mailbox::Undeliverable::lost(lost.clone()))
        {
            tracing::error!(
                sender = %lost.sender,
                dest = %lost.dest,
                message_type = lost.message_type.as_deref().unwrap_or("unknown"),
                error = %lost.error,
                return_error = %error,
                "lost message could not be reported"
            );
        }
    }

    /// Snapshot of this actor's introspection payload.
    ///
    /// Returns an [`IntrospectResult`] built from live [`InstanceCell`]
    /// state, without going through the actor message loop. This is
    /// safe to call from within a handler on the same actor (no
    /// self-send deadlock).
    ///
    /// The snapshot is best-effort: it reflects framework-owned state
    /// (status, message count, flight recorder, supervision children)
    /// at the instant of the call. `parent` is left as `None` —
    /// callers are responsible for setting topology context.
    ///
    /// Note: this acquires a write lock on the flight recorder spool
    /// and clones its contents. Suitable for occasional introspection
    /// requests, not for hot paths.
    pub fn introspect_payload(&self) -> crate::introspect::IntrospectResult {
        crate::introspect::live_actor_payload(&self.inner.cell)
    }

    /// Return a fresh tracing span bound to this actor's flight
    /// recorder, with this actor as the subject. See FR-1, FR-2, FR-3
    /// in module doc.
    pub fn recording_span(&self) -> tracing::Span {
        use crate::subject::AsSubject;
        self.inner
            .cell
            .recording()
            .span(&self.self_addr().subject().to_string())
    }

    /// Publish domain-specific properties for introspection.
    ///
    /// Publish a complete Attrs bag for introspection. Replaces any
    /// previously published attrs.
    ///
    /// Debug builds assert that every key in the bag is tagged with
    /// the `INTROSPECT` meta-attribute.
    pub fn publish_attrs(&self, attrs: hyperactor_config::Attrs) {
        #[cfg(debug_assertions)]
        {
            use std::collections::HashSet;
            use std::sync::OnceLock;

            use hyperactor_config::attrs::AttrKeyInfo;

            static INTROSPECT_KEYS: OnceLock<HashSet<&'static str>> = OnceLock::new();
            let allowed = INTROSPECT_KEYS.get_or_init(|| {
                inventory::iter::<AttrKeyInfo>()
                    .filter(|info| info.meta.get(hyperactor_config::INTROSPECT).is_some())
                    .map(|info| info.name)
                    .collect()
            });
            for (name, _) in attrs.iter() {
                debug_assert!(
                    allowed.contains(name),
                    "publish_attrs: key {:?} is not tagged with INTROSPECT",
                    name
                );
            }
        }
        self.inner.cell.set_published_attrs(attrs);
    }

    /// Publish a single attr key-value pair for introspection. Merges
    /// into existing published attrs (insert or overwrite).
    ///
    /// Debug builds assert that the key is tagged with the
    /// `INTROSPECT` meta-attribute.
    pub fn publish_attr<T: hyperactor_config::AttrValue>(
        &self,
        key: hyperactor_config::Key<T>,
        value: T,
    ) {
        debug_assert!(
            key.attrs().get(hyperactor_config::INTROSPECT).is_some(),
            "publish_attr called with non-introspection key: {}",
            key.name()
        );
        self.inner.cell.merge_published_attr(key, value);
    }

    /// Mark this actor as system/infrastructure. System actors are
    /// hidden by default in the TUI (toggled via `s`).
    pub fn set_system(&self) {
        self.inner
            .cell
            .inner
            .is_system
            .store(true, Ordering::Relaxed);
    }

    /// Register a callback for resolving non-addressable children.
    ///
    /// The callback runs on the actor's introspect task (not the
    /// actor loop), so it must be `Send + Sync` and must not access
    /// actor-mutable state. Capture cloned `Proc` references.
    ///
    /// Only `HostAgent` uses this today — for resolving system
    /// procs that have no independent `ProcAgent`.
    pub fn set_query_child_handler(
        &self,
        handler: impl (Fn(&Addr) -> IntrospectResult) + Send + Sync + 'static,
    ) {
        self.inner.cell.set_query_child_handler(handler);
    }

    /// Signal the actor to stop.
    pub fn stop(&self, reason: &str) -> Result<(), ActorError> {
        tracing::info!(
            actor_id = %self.inner.cell.actor_addr(),
            reason,
            "instance stop called",
        );
        self.inner.cell.signal(Signal::Stop(reason.to_string()))
    }

    /// Signal the actor to drain current ordinary work and then stop.
    pub fn drain_and_stop(&self, reason: &str) -> Result<(), ActorError> {
        tracing::info!(
            actor_id = %self.inner.cell.actor_addr(),
            reason,
            "instance drain_and_stop called",
        );
        self.inner
            .cell
            .signal(Signal::DrainAndStop(reason.to_string()))
    }

    /// Signal the actor to terminate immediately with a provided reason.
    pub fn kill(&self, reason: &str) -> Result<(), ActorError> {
        tracing::info!(
            actor_id = %self.inner.cell.actor_addr(),
            reason,
            "instance kill called",
        );
        self.inner.cell.signal(Signal::Kill(reason.to_string()))
    }

    /// Backward-compatible alias for `kill()`.
    pub fn abort(&self, reason: &str) -> Result<(), ActorError> {
        tracing::info!(
            actor_id = %self.inner.cell.actor_addr(),
            reason,
            "instance abort called",
        );
        self.kill(reason)
    }

    /// Close handler ingress for this actor.
    pub fn close(&self) {
        self.inner.delayed_posts.drain();
        self.inner.mailbox.drain();
    }

    pub(crate) fn status(&self) -> watch::Receiver<ActorStatus> {
        self.inner.status_tx.subscribe()
    }

    pub(crate) fn close_client(&self, reason: &str) {
        let status = ActorStatus::Stopped(reason.to_string());
        self.inner.mailbox.close(status.clone());
        self.change_status(status);
    }

    /// Request immediate actor exit with the provided stop reason.
    pub fn exit(&self, reason: &str) -> Result<(), ActorError> {
        self.inner
            .cell
            .signal(Signal::ExitRequested(reason.to_string()))
    }

    /// Queue an internal exit request after already accepted handler work.
    ///
    /// This is intentionally a small runtime special case for now.
    /// The long-term goal is to make "exit after drain" fall out of
    /// ordinary self-messaging semantics rather than requiring a
    /// dedicated internal path here.
    pub fn exit_after_drain(&self, reason: &str) -> Result<(), ActorError> {
        let this = self.clone_for_py();
        let reason = reason.to_string();
        let work = WorkCell::new(move |_actor: &mut A, _instance: &Instance<A>| {
            Box::pin(async move {
                this.exit(&reason).map_err(anyhow::Error::from)?;
                Ok(())
            })
        });
        self.enqueue_runtime_work(work)
    }

    /// Open a new port that accepts M-typed messages. The returned
    /// port may be freely cloned, serialized, and passed around. The
    /// returned receiver should only be retained by the actor responsible
    /// for processing the delivered messages.
    pub fn open_port<M: Message>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        self.inner.mailbox.open_port()
    }

    /// Open a new one-shot port that accepts M-typed messages. The
    /// returned port may be used to send a single message; ditto the
    /// receiver may receive a single message.
    pub fn open_once_port<M: Message>(&self) -> (OncePortHandle<M>, OncePortReceiver<M>) {
        self.inner.mailbox.open_once_port()
    }

    /// Return this actor's runtime signal sender.
    #[doc(hidden)]
    pub fn signal_sender(&self) -> mpsc::UnboundedSender<Signal> {
        self.inner.cell.signal_sender()
    }

    /// Get the per-instance local storage.
    pub fn locals(&self) -> &ActorLocalStorage {
        &self.inner.instance_locals
    }

    /// Send a message to the actor running on the proc.
    pub fn post(&self, port_id: impl Into<PortAddr>, headers: Flattrs, message: wirevalue::Any) {
        let port_id: PortAddr = port_id.into();
        <Self as context::MailboxExt>::post(
            self,
            port_id,
            headers,
            message,
            true,
            context::SeqInfoPolicy::AssignNew,
        )
    }

    /// Post a message with pre-set SEQ_INFO. Only for internal use by CommActor.
    ///
    /// # Warning
    /// This method bypasses the SEQ_INFO assertion. Do not use unless you are
    /// implementing mesh-level message routing (CommActor).
    #[doc(hidden)]
    pub fn post_with_external_seq_info(
        &self,
        port_id: impl Into<PortAddr>,
        headers: Flattrs,
        message: wirevalue::Any,
    ) {
        <Self as context::MailboxExt>::post(
            self,
            port_id.into(),
            headers,
            message,
            true,
            context::SeqInfoPolicy::AllowExternal,
        )
    }

    fn enqueue_runtime_work(&self, work: WorkCell<A>) -> Result<(), ActorError> {
        let actor_id_str = self.self_addr().to_string();
        account_enqueue(
            &self.inner.cell.inner.queue_depth,
            &self.inner.proc.state().queue_stats,
            &actor_id_str,
        );
        let result = self
            .inner
            .ports
            .workq
            .direct_send(work)
            .map_err(anyhow::Error::from);
        if result.is_err() {
            account_cancel_enqueue(
                &self.inner.cell.inner.queue_depth,
                &self.inner.proc.state().queue_stats,
                &actor_id_str,
            );
        }
        result.map_err(|err| ActorError::new(self.self_addr(), ActorErrorKind::processing(err)))
    }

    /// Return a static client instance that can be used to send
    /// messages to port handles from outside an actor context
    /// (e.g. from background tokio tasks).
    // TODO: replace with a proper mechanism for sending to port
    // handles without an actor context.
    pub fn self_client() -> &'static Client {
        static CLIENT: OnceLock<Client> = OnceLock::new();
        CLIENT.get_or_init(|| Proc::global().client("self_message_client"))
    }

    /// Post `message` to `dest` after `delay`.
    ///
    /// Delayed posts are owned by the actor runtime. They are best-effort:
    /// messages are posted no earlier than `delay`, and any delayed posts that
    /// have not fired when the actor shuts down are discarded.
    #[allow(private_bounds)]
    pub fn post_after<D, M>(&self, dest: D, message: M, delay: Duration)
    where
        M: Message,
        D: PostAfterEndpoint<A, M>,
    {
        let dest_location = dest.endpoint_location();
        if matches!(*self.inner.status_tx.borrow(), ActorStatus::Client) {
            self.report_lost_message(crate::mailbox::LostMessage {
                sender: self.mailbox().actor_addr().clone(),
                dest: dest_location,
                message_type: Some(std::any::type_name::<M>().to_string()),
                error: "delayed posts require an actor runtime".to_string(),
            });
            return;
        }
        let Ok(_guard) = self.inner.delayed_posts.ingress.try_enter() else {
            self.report_lost_message(crate::mailbox::LostMessage {
                sender: self.mailbox().actor_addr().clone(),
                dest: dest_location,
                message_type: Some(std::any::type_name::<M>().to_string()),
                error: "actor runtime is stopping".to_string(),
            });
            return;
        };
        if self.is_stopping() || self.is_terminal() {
            self.report_lost_message(crate::mailbox::LostMessage {
                sender: self.mailbox().actor_addr().clone(),
                dest: dest_location,
                message_type: Some(std::any::type_name::<M>().to_string()),
                error: "actor runtime is stopping".to_string(),
            });
            return;
        }

        self.inner.delayed_posts.push(
            tokio::time::Instant::now() + delay,
            dest.into_delayed_post(message),
        );
    }

    /// Start an A-typed actor onto this instance with the provided params. When spawn returns,
    /// the actor has been linked with its parent, if it has one.
    fn start(self, actor: A, receivers: InstanceReceivers<A>) -> ActorHandle<A> {
        let instance_cell = self.inner.cell.clone();
        let actor_id = self.inner.cell.actor_addr().clone();
        let actor_handle = ActorHandle::new(self.inner.cell.clone(), self.inner.ports.clone());

        // Spawn the introspect task — a separate tokio task that
        // reads InstanceCell directly and replies through the owning Proc. The
        // actor loop never sees IntrospectMessage.
        tokio::spawn(crate::introspect::serve_introspect(
            self.inner.cell.clone(),
            receivers.introspect,
        ));

        let actor_loop_receivers = receivers
            .actor_loop
            .expect("non-detached instance must have actor loop receivers");
        let actor_task_handle = A::spawn_server_task(
            panic_handler::with_backtrace_tracking(self.serve(
                actor,
                actor_loop_receivers,
                receivers.work,
            ))
            .instrument(Span::current()),
        );
        tracing::debug!("{}: spawned with {:?}", actor_id, actor_task_handle);
        instance_cell
            .inner
            .actor_task_handle
            .set(actor_task_handle)
            .unwrap_or_else(|_| panic!("{}: task handle store failed", actor_id));

        actor_handle
    }

    async fn serve(
        mut self,
        mut actor: A,
        actor_loop_receivers: (
            mpsc::UnboundedReceiver<Signal>,
            mpsc::UnboundedReceiver<ActorSupervisionEvent>,
        ),
        mut work_rx: mpsc::UnboundedReceiver<WorkCell<A>>,
    ) {
        let result = self
            .run_actor_tree(&mut actor, actor_loop_receivers, &mut work_rx)
            .await;

        assert!(self.is_stopping());
        // Compute the terminal status and supervision event, but defer
        // change_status until AFTER the event is delivered. If we flip
        // the status to terminal first, a concurrent destroy_and_wait
        // observer can release Phase 1 and stop the coordinator before
        // the event lands in its mailbox — dropping the event.
        let (terminal_status, event) = match result {
            Ok(stop_reason) => {
                let status = ActorStatus::Stopped(stop_reason);
                let event = ActorSupervisionEvent::new(
                    self.inner.cell.actor_addr().clone(),
                    actor.display_name(),
                    status.clone(),
                    None,
                );
                (status, Some(event))
            }
            Err(err) => match *err.kind {
                ActorErrorKind::UnhandledSupervisionEvent(box event) => {
                    // We use the event's actor_status as this actor's terminal status.
                    assert!(event.actor_status.is_terminal());
                    let status = event.actor_status.clone();
                    (status, Some(event))
                }
                _ => {
                    let error_kind = ActorErrorKind::Generic(err.kind.to_string());
                    let status = ActorStatus::Failed(error_kind);
                    let event = ActorSupervisionEvent::new(
                        self.inner.cell.actor_addr().clone(),
                        actor.display_name(),
                        status.clone(),
                        None,
                    );
                    (status, Some(event))
                }
            },
        };

        self.mailbox().close(terminal_status.clone());
        // FI-1: store supervision_event BEFORE change_status.
        if let Some(event) = &event {
            *self.inner.cell.inner.supervision_event.lock().unwrap() = Some(event.clone());
        }

        // Deliver the supervision event to the parent/proc BEFORE
        // change_status so that any observer waiting for this actor's
        // terminal state can only see it once the event has been
        // enqueued at its destination.
        if let Some(parent) = self.inner.cell.maybe_unlink_parent() {
            if let Some(event) = event {
                // Parent exists, failure should be propagated to the parent.
                parent.send_supervision_event_or_crash(event);
            }
            // TODO: we should get rid of this signal, and use *only* supervision events for
            // the purpose of conveying lifecycle changes
            if let Err(err) = parent.signal(Signal::ChildStopped(self.inner.cell.uid().clone())) {
                tracing::error!(
                    "{}: failed to send stop message to parent uid {}: {:?}",
                    self.self_addr(),
                    parent.uid(),
                    err
                );
            }
        } else {
            // Failure happened to the root actor or orphaned child actors.
            // In either case, the failure should be propagated to proc.
            //
            // Note that orphaned actor is unexpected and would only happen if
            // there is a bug.
            if let Some(event) = event {
                self.inner
                    .proc
                    .handle_unhandled_supervision_event(&self, event);
            }
        }

        self.change_status(terminal_status);
    }

    /// Runs the actor, and manages its supervision tree. When the function returns,
    /// the whole tree rooted at this actor has stopped. On success, returns the reason
    /// why the actor stopped. On failure, returns the error that caused the failure.
    async fn run_actor_tree(
        &mut self,
        actor: &mut A,
        mut actor_loop_receivers: (
            mpsc::UnboundedReceiver<Signal>,
            mpsc::UnboundedReceiver<ActorSupervisionEvent>,
        ),
        work_rx: &mut mpsc::UnboundedReceiver<WorkCell<A>>,
    ) -> Result<String, ActorError> {
        // It is okay to catch all panics here, because we are in a tokio task,
        // and tokio will catch the panic anyway:
        // https://docs.rs/tokio/latest/tokio/task/struct.JoinError.html#method.is_panic
        // What we do here is just to catch it early so we can handle it.

        let mut did_panic = false;
        let result = match AssertUnwindSafe(self.run(actor, &mut actor_loop_receivers, work_rx))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(_) => {
                did_panic = true;
                let panic_info = panic_handler::take_panic_info()
                    .map(|info| info.to_string())
                    .unwrap_or_else(|e| format!("Cannot take backtrace due to: {:?}", e));
                Err(ActorError::new(
                    self.self_addr(),
                    ActorErrorKind::panic(anyhow::anyhow!(panic_info)),
                ))
            }
        };

        assert!(!self.is_terminal());
        self.change_status(ActorStatus::Stopping);
        if let Err(err) = &result {
            tracing::error!("{}: actor failure: {}", self.self_addr(), err);
        }

        // After this point, we know we won't spawn any more children,
        // so we can safely read the current child keys.
        let mut to_unlink = Vec::new();
        for child in self.inner.cell.child_iter() {
            if let Err(err) = child
                .value()
                .signal(Signal::Stop("parent stopping".to_string()))
            {
                tracing::error!(
                    "{}: failed to send stop signal to child pid {}: {:?}",
                    self.self_addr(),
                    child.key(),
                    err
                );
                to_unlink.push(child.value().clone());
            }
        }
        // Manually unlink children that have already been stopped.
        for child in to_unlink {
            self.inner.cell.unlink(&child);
        }

        let (mut signal_receiver, _) = actor_loop_receivers;
        while self.inner.cell.child_count() > 0 {
            match tokio::time::timeout(Duration::from_millis(500), signal_receiver.recv()).await {
                Ok(Some(Signal::ChildStopped(uid))) => {
                    assert!(self.inner.cell.get_child(&uid).is_none());
                }
                // Drain only tracks child termination; other signals are
                // intentionally swallowed here.
                Ok(Some(_)) => {}
                Ok(None) => {
                    // Signal channel closed: no further ChildStopped will
                    // arrive, so we can no longer track child termination.
                    // Drop remaining links and exit the drain loop, mirroring
                    // the timeout branch below.
                    self.inner.cell.unlink_all();
                    break;
                }
                Err(_) => {
                    tracing::warn!(
                        "timeout waiting for ChildStopped signal from child on actor: {}, ignoring",
                        self.self_addr()
                    );
                    // No more waiting to receive messages. Unlink all remaining
                    // children.
                    self.inner.cell.unlink_all();
                    break;
                }
            }
        }
        // Run the actor cleanup function before the actor stops to delete
        // resources. If it times out, continue with stopping the actor.
        // Don't call it if there was a panic, because the actor may
        // be in an invalid state and unable to access anything, for example
        // the GIL.
        let cleanup_result = if !did_panic {
            let cleanup_timeout = hyperactor_config::global::get(config::CLEANUP_TIMEOUT);
            match tokio::time::timeout(
                cleanup_timeout,
                self.inner
                    .proc
                    .with_current(actor.cleanup(self, result.as_ref().err())),
            )
            .await
            {
                Ok(Ok(x)) => Ok(x),
                Ok(Err(e)) => Err(ActorError::new(
                    self.self_addr(),
                    ActorErrorKind::cleanup(e),
                )),
                Err(e) => Err(ActorError::new(
                    self.self_addr(),
                    ActorErrorKind::cleanup(e.into()),
                )),
            }
        } else {
            Ok(())
        };
        if let Err(ref actor_err) = result {
            // The original result error takes precedence over the cleanup error,
            // so make sure the cleanup error is still logged in that case.
            if let Err(ref err) = cleanup_result {
                tracing::warn!(
                    cleanup_err = %err,
                    %actor_err,
                    "ignoring cleanup error after actor error",
                );
            }
        }
        // If the original exit was not an error, let cleanup errors be
        // surfaced.
        result.and_then(|reason| cleanup_result.map(|_| reason))
    }

    /// Initialize and run the actor until it fails or is stopped. On success,
    /// returns the reason why the actor stopped. On failure, returns the error
    /// that caused the failure.
    async fn run(
        &mut self,
        actor: &mut A,
        actor_loop_receivers: &mut (
            mpsc::UnboundedReceiver<Signal>,
            mpsc::UnboundedReceiver<ActorSupervisionEvent>,
        ),
        work_rx: &mut mpsc::UnboundedReceiver<WorkCell<A>>,
    ) -> Result<String, ActorError> {
        let (signal_receiver, supervision_event_receiver) = actor_loop_receivers;

        self.change_status(ActorStatus::Initializing);
        self.inner
            .proc
            .with_current(actor.init(self))
            .await
            .map_err(|err| ActorError::new(self.self_addr(), ActorErrorKind::init(err)))?;
        let actor_id_str = self.self_addr().to_string();
        let stop_reason = 'messages: loop {
            if !self.is_stopping() {
                self.change_status(ActorStatus::Idle);
            }
            let next_delayed_deadline = self.inner.delayed_posts.next_deadline();
            let metric_pairs = hyperactor_telemetry::kv_pairs!("actor_id" => actor_id_str.clone());
            tokio::select! {
                biased;
                signal = signal_receiver.recv() => {
                    let signal = signal.ok_or_else(|| {
                        ActorError::new(self.self_addr(), ActorErrorKind::SignalChannelClosed)
                    })?;
                    tracing::debug!("received signal {signal:?}");
                    match signal {
                        Signal::Stop(reason) => {
                            self.change_status(ActorStatus::Stopping);
                            self.inner
                                .proc
                                .with_current(actor.handle_stop(self, StopMode::Stop, &reason))
                                .await
                                .map_err(|err| ActorError::new(self.self_addr(), ActorErrorKind::processing(err)))?;
                        },
                        Signal::DrainAndStop(reason) => {
                            self.change_status(ActorStatus::Stopping);
                            self.inner
                                .proc
                                .with_current(actor.handle_stop(self, StopMode::DrainAndStop, &reason))
                                .await
                                .map_err(|err| ActorError::new(self.self_addr(), ActorErrorKind::processing(err)))?;
                        },
                        Signal::ChildStopped(uid) => {
                            assert!(self.inner.cell.get_child(&uid).is_none());
                        },
                        Signal::ExitRequested(reason) => {
                            break 'messages reason;
                        }
                        Signal::Kill(reason) => {
                            return Err(ActorError { actor_id: Box::new(self.self_addr().clone()), kind: Box::new(ActorErrorKind::Aborted(reason)) });
                        }
                    }
                }
                work = work_rx.recv() => {
                    ACTOR_MESSAGES_RECEIVED.add(1, metric_pairs);
                    account_dequeue(&self.inner.cell.inner.queue_depth, &self.inner.proc.state().queue_stats, &actor_id_str);
                    let _ = ACTOR_MESSAGE_HANDLER_DURATION.start(metric_pairs);
                    let work = work.expect("inconsistent work queue state");
                    if let Err(err) = work.handle(actor, self).await {
                        while let Ok(supervision_event) = supervision_event_receiver.try_recv() {
                            self.handle_supervision_event(actor, supervision_event).await?;
                        }
                        let kind = ActorErrorKind::processing(err);
                        return Err(ActorError {
                            actor_id: Box::new(self.self_addr().clone()),
                            kind: Box::new(kind),
                        });
                    }
                }
                _ = self.inner.delayed_posts.notify.notified(), if !self.is_stopping() && !self.inner.delayed_posts.is_draining() => {
                }
                _ = async {
                    match next_delayed_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if !self.is_stopping() && !self.inner.delayed_posts.is_draining() && next_delayed_deadline.is_some() => {
                    let now = tokio::time::Instant::now();
                    if let Ok(_guard) = self.inner.delayed_posts.ingress.try_enter() {
                        for post in self.inner.delayed_posts.pop_due(now) {
                            post(self);
                        }
                    }
                }
                Some(supervision_event) = supervision_event_receiver.recv() => {
                    self.handle_supervision_event(actor, supervision_event).await?;
                }
            }
            self.inner
                .cell
                .inner
                .num_processed_messages
                .fetch_add(1, Ordering::SeqCst);
        };
        tracing::debug!(
            actor_id = %self.self_addr(),
            reason = stop_reason,
            "exited actor loop",
        );
        Ok(stop_reason)
    }

    /// Handle a supervision event using the provided actor.
    pub async fn handle_supervision_event(
        &self,
        actor: &mut A,
        supervision_event: ActorSupervisionEvent,
    ) -> Result<(), ActorError> {
        // Handle the supervision event with the current actor.
        match self
            .inner
            .proc
            .with_current(actor.handle_supervision_event(self, &supervision_event))
            .await
        {
            Ok(true) => {
                // The supervision event was handled by this actor, nothing more to do.
                Ok(())
            }
            Ok(false) => {
                let kind = ActorErrorKind::UnhandledSupervisionEvent(Box::new(supervision_event));
                Err(ActorError::new(self.self_addr(), kind))
            }
            Err(err) => {
                // The actor failed to handle the supervision event, it should die.
                // Create a new supervision event for this failure and propagate it.
                let kind = ActorErrorKind::ErrorDuringHandlingSupervision(
                    err.to_string(),
                    Box::new(supervision_event),
                );
                Err(ActorError::new(self.self_addr(), kind))
            }
        }
    }

    async unsafe fn handle_message<M: Message>(
        &self,
        actor: &mut A,
        type_info: Option<&'static TypeInfo>,
        headers: Flattrs,
        message: M,
    ) -> Result<(), anyhow::Error>
    where
        A: Handler<M>,
    {
        // Build HandlerInfo from TypeInfo (zero-copy) or fall back to type_name.
        let handler_info = match type_info {
            Some(info) => {
                // SAFETY: The caller promises to pass the correct type info.
                let arm = unsafe { info.arm_unchecked(&message as *const M as *const ()) };
                HandlerInfo::from_static(info.typename(), arm)
            }
            None => {
                // Fall back to std::any::type_name (also static, zero-copy).
                HandlerInfo::from_static(std::any::type_name::<M>(), None)
            }
        };

        let endpoint = type_info.and_then(|info| {
            // SAFETY: The caller promises to pass the correct type info.
            unsafe { info.endpoint_name(&message as *const M as *const ()) }
        });

        // Use a helper function for a better instrument log.
        self.handle_message_with_handler_info(actor, handler_info, headers, message, endpoint)
            .await
    }

    #[tracing::instrument(level = "debug", name = "handle_message", skip_all, fields(message_type = %handler_info))]
    async fn handle_message_with_handler_info<M: Message>(
        &self,
        actor: &mut A,
        handler_info: HandlerInfo,
        headers: Flattrs,
        message: M,
        endpoint: Option<String>,
    ) -> Result<(), anyhow::Error>
    where
        A: Handler<M>,
    {
        let now = std::time::SystemTime::now();
        let handler_info = Some(handler_info);
        self.change_status(ActorStatus::Processing(now, handler_info.clone()));
        crate::mailbox::headers::log_message_latency_if_sampling(
            &headers,
            self.self_addr().to_string(),
        );

        let message_id = headers.get(crate::mailbox::headers::TELEMETRY_MESSAGE_ID);

        if let Some(message_id) = message_id {
            let from_actor_id = headers
                .get(crate::mailbox::headers::SENDER_ACTOR_ID_HASH)
                .unwrap_or(0);
            let to_actor_id = hash_to_u64(self.self_addr());
            let port_id = headers.get(crate::mailbox::headers::TELEMETRY_PORT_ID);

            notify_message(hyperactor_telemetry::MessageEvent {
                timestamp: now,
                id: message_id,
                from_actor_id,
                to_actor_id,
                endpoint,
                port_id,
            });

            notify_message_status(hyperactor_telemetry::MessageStatusEvent {
                timestamp: now,
                id: hyperactor_telemetry::generate_status_event_id(message_id),
                message_id,
                status: "active".to_string(),
            });
        }

        // Record the message handler being invoked.
        *self.inner.cell.inner.last_message_handler.write().unwrap() = handler_info;

        let context = Context::new(self, headers);
        // Pass a reference to the context to the handler, so that deref
        // coercion allows the `this` argument to be treated exactly like
        // &Instance<A>.
        let start = Instant::now();
        let subject_str = self.self_addr().subject().to_string();
        let result = self
            .inner
            .proc
            .with_current(actor.handle(&context, message))
            .instrument(self.inner.cell.inner.recording.span(&subject_str))
            .await;
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.inner
            .cell
            .inner
            .total_processing_time_us
            .fetch_add(elapsed_us, Ordering::SeqCst);

        if let Some(message_id) = message_id {
            notify_message_status(hyperactor_telemetry::MessageStatusEvent {
                timestamp: std::time::SystemTime::now(),
                id: hyperactor_telemetry::generate_status_event_id(message_id),
                message_id,
                status: "complete".to_string(),
            });
        }

        result
    }

    /// Spawn a child actor with a fresh uid labeled from the actor type.
    pub fn spawn<C: Actor>(&self, actor: C) -> ActorHandle<C> {
        self.inner.proc.spawn_child(self.inner.cell.clone(), actor)
    }

    /// Spawn a named child actor on this instance. The child gets a
    /// descriptive name in its ActorId instead of inheriting this
    /// instance's name. Supervision linkage is preserved.
    pub fn spawn_with_name<C: Actor>(&self, name: &str, actor: C) -> ActorHandle<C> {
        self.inner
            .proc
            .spawn_named_child(self.inner.cell.clone(), name, actor)
    }

    /// Spawn a child actor with a fresh uid carrying a display label.
    ///
    /// The label is descriptive only and does not participate in actor
    /// identity. Supervision linkage to this instance is preserved.
    pub fn spawn_with_label<C: Actor>(&self, label: &str, actor: C) -> ActorHandle<C> {
        self.inner
            .proc
            .spawn_named_child(self.inner.cell.clone(), label, actor)
    }

    /// Spawn a child actor on this instance using an explicit uid.
    ///
    /// This is the explicit identity API, and the only child spawn API that
    /// permits singleton actor identity. Instance labels, if present, are
    /// descriptive only and do not affect uniqueness.
    pub fn spawn_with_uid<C: Actor>(&self, uid: Uid, actor: C) -> anyhow::Result<ActorHandle<C>> {
        self.inner
            .proc
            .spawn_child_with_uid(self.inner.cell.clone(), uid, actor)
    }

    /// Create a new direct child instance.
    pub fn child(&self) -> (Instance<()>, ActorHandle<()>) {
        self.inner.proc.child_instance(self.inner.cell.clone())
    }

    /// Spawn a registered actor as this instance's child.
    ///
    /// The actor type is resolved through the remote spawn registry. The child
    /// receives an empty environment.
    pub async fn gspawn(&self, actor_type: &str, params: Data) -> anyhow::Result<AnyActorHandle> {
        self.gspawn_uid(actor_type, crate::id::Uid::anonymous(), params)
            .await
    }

    /// Spawn a registered actor as this instance's child using an explicit uid.
    ///
    /// The actor type is resolved through the remote spawn registry. The child
    /// receives an empty environment.
    pub async fn gspawn_uid(
        &self,
        actor_type: &str,
        uid: crate::id::Uid,
        params: Data,
    ) -> anyhow::Result<AnyActorHandle> {
        crate::actor::remote::Remote::global()
            .gspawn_child(
                &self.inner.proc,
                self.inner.cell.clone(),
                actor_type,
                uid,
                params,
                Flattrs::default(),
            )
            .await
    }

    /// Return a handler port handle representing the actor's message
    /// handler for M-typed messages.
    pub fn port<M: Message>(&self) -> PortHandle<M>
    where
        A: Handler<M>,
    {
        self.inner.ports.get()
    }

    /// The [`ActorHandle`] corresponding to this instance.
    pub fn handle(&self) -> ActorHandle<A> {
        ActorHandle::new(self.inner.cell.clone(), Arc::clone(&self.inner.ports))
    }

    /// The owning actor ref.
    pub fn bind<R: Binds<A>>(&self) -> ActorRef<R> {
        self.inner.cell.bind(self.inner.ports.as_ref())
    }

    // Temporary in order to support python bindings.
    #[doc(hidden)]
    pub fn mailbox_for_py(&self) -> &Mailbox {
        &self.inner.mailbox
    }

    /// The owning proc.
    pub fn proc(&self) -> &Proc {
        &self.inner.proc
    }

    /// Clone this Instance to get an owned struct that can be
    /// plumbed through python. This should really only be called
    /// for the explicit purpose of being passed into python
    #[doc(hidden)]
    pub fn clone_for_py(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Get the join handle associated with this actor.
    fn actor_task_handle(&self) -> Option<&JoinHandle<()>> {
        self.inner.cell.inner.actor_task_handle.get()
    }

    /// Return this instance's sequencer.
    pub fn sequencer(&self) -> &Sequencer {
        &self.inner.sequencer
    }

    /// Reserve (consume) the next `count` ordering sequence numbers for
    /// the given destination without posting any messages. Subsequent
    /// normal sends to this destination pick up at `last_reserved + 1`,
    /// creating a deterministic gap from the receiver's perspective.
    ///
    /// Test/demo only. Production code should not call this; misuse will
    /// produce stalled receivers. Marked `#[doc(hidden)]`; review
    /// discipline is the misuse defense.
    #[doc(hidden)]
    pub fn debug_skip_next_ordering_seq(&self, dest: &PortAddr, count: u64) {
        let sequencer = self.sequencer();
        for _ in 0..count {
            let _ = sequencer.assign_seq(dest);
        }
    }

    /// Return this instance's ID.
    pub fn instance_id(&self) -> Uuid {
        self.inner.id
    }

    /// Return a handle to this instance's parent actor, if it has one.
    pub fn parent_handle<P: Actor>(&self) -> Option<ActorHandle<P>> {
        let parent_cell = self.inner.cell.inner.parent.upgrade()?;
        let ports = if let Ok(ports) = parent_cell.inner.ports.clone().downcast() {
            ports
        } else {
            return None;
        };
        Some(ActorHandle::new(parent_cell, ports))
    }
}

impl Instance<ClientActor> {
    pub(crate) fn child_client(&self) -> Client {
        let actor_id = self
            .inner
            .proc
            .allocate_anonymous_child_id(self.inner.cell.actor_addr());
        let (instance, _receivers) = Instance::new(
            self.inner.proc.clone(),
            actor_id,
            false,
            Some(self.inner.cell.clone()),
        );
        instance.change_status(ActorStatus::Client);
        Client::new(instance)
    }
}

impl<A: Actor> context::Mailbox for Instance<A> {
    fn mailbox(&self) -> &Mailbox {
        &self.inner.mailbox
    }
}

impl<A: Actor> context::Mailbox for Context<'_, A> {
    fn mailbox(&self) -> &Mailbox {
        &self.instance.inner.mailbox
    }
}

impl<A: Actor> context::Mailbox for &Instance<A> {
    fn mailbox(&self) -> &Mailbox {
        &self.inner.mailbox
    }
}

impl<A: Actor> context::Mailbox for &Context<'_, A> {
    fn mailbox(&self) -> &Mailbox {
        &self.instance.inner.mailbox
    }
}

impl<A: Actor> context::Actor for Instance<A> {
    type A = A;
    fn instance(&self) -> &Instance<A> {
        self
    }
}

impl<A: Actor> context::Actor for Context<'_, A> {
    type A = A;
    fn instance(&self) -> &Instance<A> {
        self
    }

    fn headers(&self) -> &Flattrs {
        Context::headers(self)
    }
}

impl<A: Actor> context::Actor for &Instance<A> {
    type A = A;
    fn instance(&self) -> &Instance<A> {
        self
    }
}

impl<A: Actor> context::Actor for &Context<'_, A> {
    type A = A;
    fn instance(&self) -> &Instance<A> {
        self
    }

    fn headers(&self) -> &Flattrs {
        Context::headers(self)
    }
}

impl<A, M> crate::Endpoint<M> for &Instance<A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::EndpointLocation::Actor(self.self_addr().clone())
    }

    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor,
    {
        let port = self.port();
        crate::Endpoint::post(&port, cx, message)
    }
}

impl<A, M> crate::Endpoint<M> for &Context<'_, A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::EndpointLocation::Actor(self.self_addr().clone())
    }

    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor,
    {
        crate::Endpoint::post(self.instance, cx, message)
    }
}

impl<A, M> crate::Endpoint<M> for Instance<A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn endpoint_location(&self) -> crate::EndpointLocation {
        crate::EndpointLocation::Actor(self.self_addr().clone())
    }

    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor,
    {
        crate::Endpoint::post(&self, cx, message)
    }
}

impl Instance<()> {
    /// See [Mailbox::bind_handler_port] for details.
    pub fn bind_handler_port<M: RemoteMessage>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        assert!(
            self.actor_task_handle().is_none(),
            "can only bind handler port on instance with no running actor task"
        );
        self.inner.mailbox.bind_handler_port()
    }
}

#[derive(Debug)]
enum ActorType {
    Named(&'static TypeInfo),
    Anonymous(&'static str),
}

impl ActorType {
    fn type_name(&self) -> &str {
        match self {
            ActorType::Named(info) => info.typename(),
            ActorType::Anonymous(name) => name,
        }
    }
}

/// InstanceCell contains all of the type-erased, shareable state of an instance.
/// Specifically, InstanceCells form a supervision tree, and is used by ActorHandle
/// to access the underlying instance.
///
/// InstanceCell is reference counted and cloneable.
#[derive(Clone)]
pub struct InstanceCell {
    inner: Arc<InstanceCellState>,
}

impl fmt::Debug for InstanceCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstanceCell")
            .field("actor_id", &self.inner.actor_id)
            .field("actor_type", &self.inner.actor_type)
            .finish()
    }
}

struct InstanceCellState {
    /// The actor's id.
    actor_id: ActorAddr,

    /// The actor instance's `Uuid::now_v7()` identity. Stable for the
    /// lifetime of this instance; surfaced via `InstanceCell::instance_id`
    /// and the `INSTANCE_ID` introspection attr.
    instance_id: Uuid,

    /// Actor info contains the actor's type information.
    actor_type: ActorType,

    /// The proc in which the actor is running.
    proc: Proc,

    /// Control plane message senders to the actor loop, if one is running.
    actor_loop: Option<(
        mpsc::UnboundedSender<Signal>,
        mpsc::UnboundedSender<ActorSupervisionEvent>,
    )>,

    /// An observer that stores the current status of the actor.
    status: watch::Receiver<ActorStatus>,

    /// A weak reference to this instance's parent.
    parent: WeakInstanceCell,

    /// This instance's children by their uids.
    children: DashMap<crate::id::Uid, InstanceCell>,

    /// Access to the spawned actor's join handle.
    actor_task_handle: OnceLock<JoinHandle<()>>,

    /// The set of named ports that are exported by this actor.
    exported_named_ports: DashMap<u64, &'static str>,

    /// The number of messages processed by this actor.
    num_processed_messages: AtomicU64,

    /// When this actor was created.
    created_at: SystemTime,

    /// Name of the last message handler invoked.
    last_message_handler: RwLock<Option<HandlerInfo>>,

    /// Total time spent processing messages, in microseconds.
    total_processing_time_us: AtomicU64,

    /// Current actor work-queue depth.
    ///
    /// Two consumers of one accounting path (PD-5e): this field is
    /// the introspection-readable state; the OTel
    /// `ACTOR_MESSAGE_QUEUE_SIZE` counter is the telemetry export.
    /// Both are updated together by `account_enqueue` /
    /// `account_dequeue`.
    ///
    /// Shared with `HandlerPorts<A>`: incremented at enqueue in the send
    /// path, decremented when the actor loop receives from `work_rx`.
    queue_depth: Arc<AtomicU64>,

    /// The log recording associated with this actor. It is used to
    /// store a 'flight record' of events while the actor is running.
    recording: Recording,

    /// Attrs-based introspection data published by the actor. Written
    /// by the actor via `Instance::publish_attrs()` /
    /// `Instance::publish_attr()`, and read by the introspection
    /// runtime handler when building node payloads.
    ///
    /// This bag may contain both mesh-level keys (`node_type`,
    /// `addr`, `num_procs`, ...) and actor-runtime keys (`status`,
    /// `messages_processed`, ...).
    published_attrs: RwLock<Option<hyperactor_config::Attrs>>,

    /// Optional callback for resolving non-addressable children
    /// (e.g., system procs). Registered by infrastructure actors
    /// like `HostAgent` in `Actor::init`. Invoked by the
    /// introspection runtime handler for `QueryChild` messages.
    /// `None` means `QueryChild` returns a "not_found" error.
    ///
    /// See S7 in `introspect` module doc.
    query_child_handler: RwLock<Option<Box<dyn (Fn(&Addr) -> IntrospectResult) + Send + Sync>>>,

    /// The supervision event for this actor's failure, if any.
    /// See FI-1, FI-2 in `introspect` module doc.
    supervision_event: std::sync::Mutex<Option<crate::supervision::ActorSupervisionEvent>>,

    /// Whether this actor is infrastructure/system (hidden by default
    /// in the TUI `s` toggle). Set by spawning code via
    /// `Instance::set_system()`.
    is_system: AtomicBool,

    /// A type-erased reference to HandlerPorts<A>, which allows us to
    /// recover an ActorHandle<A> by downcasting.
    ports: Arc<dyn Any + Send + Sync>,

    /// Type-erased snapshot callback for inbound ordering state.
    /// Captured at `Instance::new` from the typed `Arc<HandlerPorts<A>>`
    /// (where `A` is in scope), erased to `dyn Fn() -> OrderingSnapshot`
    /// so non-generic code in `InstanceCellState` can invoke it.
    ///
    /// Hygiene: the closure captures ONLY `Arc<HandlerPorts<A>>::clone()`
    /// — never `Instance<A>` or `InstanceCell` — to avoid cyclic refs
    /// back to the cell that holds this callback. Body is a single
    /// `workq.snapshot()` call; bounded work, `try_lock`-based, never
    /// blocks. See IO-1/IO-2 in `introspect` module doc.
    ///
    /// `None` only for hand-built fixtures or future code paths that
    /// construct an `InstanceCellState` without going through
    /// `Instance::new`; production live actors always install Some.
    inbound_ordering_snapshot:
        Option<Box<dyn Fn() -> crate::ordering::OrderingSnapshot + Send + Sync>>,
}

impl InstanceCellState {
    /// Unlink this instance from its parent, if it has one. If it was unlinked,
    /// the parent is returned.
    fn maybe_unlink_parent(&self) -> Option<InstanceCell> {
        self.parent
            .upgrade()
            .filter(|parent| parent.inner.unlink(self))
    }

    /// Unlink this instance from a child.
    fn unlink(&self, child: &InstanceCellState) -> bool {
        assert_eq!(self.actor_id.proc_id(), child.actor_id.proc_id());
        self.children.remove(child.actor_id.uid()).is_some()
    }
}

/// Select which terminated snapshots to evict when the retention cap
/// is exceeded.
///
/// Each entry is `(actor_id, Option<occurred_at>)` where `Some` means
/// the actor has `failure_info` (i.e. it failed), and `None` means a
/// clean stop.
///
/// Eviction priority:
/// 1. Cleanly-stopped actors are evicted first (arbitrary order).
/// 2. If more evictions are needed, failed actors are evicted
///    newest-first (descending `occurred_at`), preserving the
///    earliest failures which are closest to the root cause.
fn select_eviction_candidates(
    entries: &[(ActorAddr, Option<String>)],
    excess: usize,
) -> Vec<ActorAddr> {
    let mut clean: Vec<&ActorAddr> = Vec::new();
    let mut failed: Vec<(&ActorAddr, &str)> = Vec::new();
    for (id, occurred_at) in entries {
        match occurred_at {
            Some(ts) => failed.push((id, ts.as_str())),
            None => clean.push(id),
        }
    }

    let mut to_remove: Vec<ActorAddr> = Vec::new();
    let mut remaining = excess;

    // Evict cleanly-stopped first.
    for id in clean {
        if remaining == 0 {
            break;
        }
        to_remove.push(id.clone());
        remaining -= 1;
    }

    // If still over cap, evict most-recent failures first.
    if remaining > 0 {
        failed.sort_by(|a, b| b.1.cmp(a.1));
        for (id, _) in failed.into_iter().take(remaining) {
            to_remove.push(id.clone());
        }
    }

    to_remove
}

impl InstanceCell {
    /// Creates a new instance cell with the provided internal state. If a parent
    /// is provided, it is linked to this cell.
    #[allow(clippy::too_many_arguments)]
    fn new(
        actor_id: ActorAddr,
        instance_id: Uuid,
        actor_type: ActorType,
        proc: Proc,
        actor_loop: Option<(
            mpsc::UnboundedSender<Signal>,
            mpsc::UnboundedSender<ActorSupervisionEvent>,
        )>,
        status: watch::Receiver<ActorStatus>,
        parent: Option<InstanceCell>,
        ports: Arc<dyn Any + Send + Sync>,
        queue_depth: Arc<AtomicU64>,
        inbound_ordering_snapshot: Option<
            Box<dyn Fn() -> crate::ordering::OrderingSnapshot + Send + Sync>,
        >,
    ) -> Self {
        let is_root = parent.is_none();
        let _ais = actor_id.to_string();
        let cell = Self {
            inner: Arc::new(InstanceCellState {
                actor_id: actor_id.clone(),
                instance_id,
                actor_type,
                proc: proc.clone(),
                actor_loop,
                status,
                parent: parent.map_or_else(WeakInstanceCell::new, |cell| cell.downgrade()),
                children: DashMap::new(),
                actor_task_handle: OnceLock::new(),
                exported_named_ports: DashMap::new(),
                num_processed_messages: AtomicU64::new(0),
                created_at: std::time::SystemTime::now(),
                last_message_handler: RwLock::new(None),
                total_processing_time_us: AtomicU64::new(0),
                queue_depth,
                recording: hyperactor_telemetry::recorder().record(64),
                published_attrs: RwLock::new(None),
                query_child_handler: RwLock::new(None),
                supervision_event: std::sync::Mutex::new(None),
                is_system: AtomicBool::new(false),
                ports,
                inbound_ordering_snapshot,
            }),
        };
        cell.maybe_link_parent();
        proc.inner
            .instances
            .insert(actor_id.id().clone(), cell.downgrade());
        if is_root {
            proc.inner.root_actors.insert(actor_id.id().clone());
        }
        cell
    }

    fn wrap(inner: Arc<InstanceCellState>) -> Self {
        Self { inner }
    }

    /// The actor's address.
    pub fn actor_addr(&self) -> &ActorAddr {
        &self.inner.actor_id
    }

    /// The proc in which this actor is running.
    pub(crate) fn proc(&self) -> &Proc {
        &self.inner.proc
    }

    /// The actor's uid.
    pub(crate) fn uid(&self) -> &crate::id::Uid {
        self.inner.actor_id.uid()
    }

    /// The actor's join handle.
    #[allow(dead_code)]
    pub(crate) fn actor_task_handle(&self) -> Option<&JoinHandle<()>> {
        self.inner.actor_task_handle.get()
    }

    /// The instance's status observer.
    pub fn status(&self) -> &watch::Receiver<ActorStatus> {
        &self.inner.status
    }

    /// The supervision event stored when this actor failed.
    /// `None` for actors that stopped cleanly or are still running.
    pub fn supervision_event(&self) -> Option<crate::supervision::ActorSupervisionEvent> {
        self.inner.supervision_event.lock().unwrap().clone()
    }

    fn signal_sender(&self) -> mpsc::UnboundedSender<Signal> {
        self.inner
            .actor_loop
            .as_ref()
            .map(|(signal_tx, _)| signal_tx.clone())
            .unwrap_or_else(|| panic!("{} has no runtime signal sender", self.actor_addr()))
    }

    /// Send a signal to the actor.
    pub fn signal(&self, signal: Signal) -> Result<(), ActorError> {
        if let Some((signal_tx, _)) = &self.inner.actor_loop {
            signal_tx.send(signal).map_err(|_| {
                ActorError::new(self.actor_addr(), ActorErrorKind::SignalChannelClosed)
            })
        } else {
            tracing::warn!(
                "{}: attempted to send signal {} to detached actor",
                self.inner.actor_id,
                signal
            );
            Ok(())
        }
    }

    /// Used by this actor's children to send a supervision event to this actor.
    /// When it fails to send, we will crash the process. As part of the crash,
    /// all the procs and actors running on this process will be terminated
    /// forcefully.
    ///
    /// Note that "let it crash" is the default behavior when a supervision event
    /// cannot be delivered upstream. It is the upstream's responsibility to
    /// detect and handle crashes.
    pub fn send_supervision_event_or_crash(&self, event: ActorSupervisionEvent) {
        match &self.inner.actor_loop {
            Some((_, supervision_tx)) => {
                if let Err(err) = supervision_tx.send(event.clone()) {
                    if !event.is_error() {
                        // Normal lifecycle events (e.g. clean stop) that fail to
                        // send are silently dropped. This happens when a child
                        // stops after the parent's mailbox has been closed or its
                        // supervision port receiver has been dropped (e.g. client
                        // instances created via Proc::client()).
                        tracing::debug!(
                            "{}: dropping non-error supervision event {}: {:?}",
                            self.actor_addr(),
                            event,
                            err
                        );
                        return;
                    }
                    tracing::error!(
                        "{}: failed to send supervision event to actor: {:?}. Crash the process.",
                        self.actor_addr(),
                        err
                    );
                    std::process::exit(1);
                }
            }
            None => {
                if !event.is_error() {
                    tracing::debug!(
                        "{}: dropping non-error supervision event {} to detached actor",
                        self.actor_addr(),
                        event,
                    );
                    return;
                }
                tracing::error!(
                    "{}: failed: {}: cannot send supervision event to detached actor: crashing",
                    self.actor_addr(),
                    event,
                );
                std::process::exit(1);
            }
        }
    }

    /// Downgrade this InstanceCell to a weak reference.
    pub fn downgrade(&self) -> WeakInstanceCell {
        WeakInstanceCell {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Link this instance to a new child.
    fn link(&self, child: InstanceCell) {
        assert_eq!(self.actor_addr().proc_id(), child.actor_addr().proc_id());
        self.inner.children.insert(child.uid().clone(), child);
    }

    /// Unlink this instance from a child.
    fn unlink(&self, child: &InstanceCell) {
        assert_eq!(self.actor_addr().proc_id(), child.actor_addr().proc_id());
        self.inner.children.remove(child.uid());
    }

    /// Unlink this instance from all children.
    fn unlink_all(&self) {
        self.inner.children.clear();
    }

    /// Link this instance to its parent, if it has one.
    fn maybe_link_parent(&self) {
        if let Some(parent) = self.inner.parent.upgrade() {
            parent.link(self.clone());
        }
    }

    /// Unlink this instance from its parent, if it has one. If it was unlinked,
    /// the parent is returned.
    fn maybe_unlink_parent(&self) -> Option<InstanceCell> {
        self.inner.maybe_unlink_parent()
    }

    /// Return an iterator over this instance's children. This may deadlock if the
    /// caller already holds a reference to any item in map.
    fn child_iter(&self) -> impl Iterator<Item = RefMulti<'_, crate::id::Uid, InstanceCell>> {
        self.inner.children.iter()
    }

    /// The number of children this instance has.
    pub fn child_count(&self) -> usize {
        self.inner.children.len()
    }

    /// Returns the ActorAddrs of this instance's direct children.
    pub fn child_actor_ids(&self) -> Vec<ActorAddr> {
        self.inner
            .children
            .iter()
            .map(|entry| entry.value().actor_addr().clone())
            .collect()
    }

    /// Get a child by its uid.
    fn get_child(&self, uid: &crate::id::Uid) -> Option<InstanceCell> {
        self.inner.children.get(uid).map(|child| child.clone())
    }

    /// Access the flight recorder for this actor.
    pub fn recording(&self) -> &Recording {
        &self.inner.recording
    }

    /// When this actor was created.
    pub fn created_at(&self) -> SystemTime {
        self.inner.created_at
    }

    /// The number of messages processed by this actor.
    pub fn num_processed_messages(&self) -> u64 {
        self.inner.num_processed_messages.load(Ordering::SeqCst)
    }

    /// The last message handler invoked by this actor.
    pub fn last_message_handler(&self) -> Option<HandlerInfo> {
        self.inner.last_message_handler.read().unwrap().clone()
    }

    /// Total time spent processing messages, in microseconds.
    pub fn total_processing_time_us(&self) -> u64 {
        self.inner.total_processing_time_us.load(Ordering::SeqCst)
    }

    /// Current actor work-queue depth (PD-5).
    pub fn queue_depth(&self) -> u64 {
        self.inner.queue_depth.load(Ordering::Relaxed)
    }

    /// Stable per-instance identifier (`Uuid::now_v7`) assigned at
    /// `Instance::new` and threaded through to the cell at construction.
    pub fn instance_id(&self) -> Uuid {
        self.inner.instance_id
    }

    /// Out-of-band inbound ordering snapshot. Returns `None` when no
    /// snapshot callback was installed (see IO-1 in `introspect` module
    /// doc). The callback uses `OrderedSender::snapshot` (`try_lock`,
    /// non-blocking) and never perturbs ordering state.
    pub fn inbound_ordering_snapshot(&self) -> Option<crate::ordering::OrderingSnapshot> {
        self.inner.inbound_ordering_snapshot.as_ref().map(|f| f())
    }

    /// Get parent instance cell, if it exists.
    pub fn parent(&self) -> Option<InstanceCell> {
        self.inner.parent.upgrade()
    }

    /// The actor's type name.
    pub fn actor_type_name(&self) -> &str {
        self.inner.actor_type.type_name()
    }

    /// Replace the published introspection attrs with a new bag.
    pub fn set_published_attrs(&self, attrs: hyperactor_config::Attrs) {
        *self.inner.published_attrs.write().unwrap() = Some(attrs);
    }

    /// Set a single introspection attr, merging into the existing bag
    /// (or creating one if none exists).
    pub fn merge_published_attr<T: hyperactor_config::AttrValue>(
        &self,
        key: hyperactor_config::Key<T>,
        value: T,
    ) {
        self.inner
            .published_attrs
            .write()
            .unwrap()
            .get_or_insert_with(hyperactor_config::Attrs::new)
            .set(key, value);
    }

    /// Read the published introspection attrs, if any.
    pub fn published_attrs(&self) -> Option<hyperactor_config::Attrs> {
        self.inner.published_attrs.read().unwrap().clone()
    }

    /// Register a callback for resolving non-addressable children
    /// via `IntrospectMessage::QueryChild`.
    ///
    /// The callback runs on the actor's introspect task (a separate
    /// tokio task, not the actor's message loop), so it must be
    /// `Send + Sync` and must not access actor-mutable state.
    /// Capture cloned `Proc` references, not `&mut self`.
    pub fn set_query_child_handler(
        &self,
        handler: impl (Fn(&Addr) -> IntrospectResult) + Send + Sync + 'static,
    ) {
        *self.inner.query_child_handler.write().unwrap() = Some(Box::new(handler));
    }

    /// Invoke the registered QueryChild handler, if any.
    pub fn query_child(&self, child_ref: &Addr) -> Option<IntrospectResult> {
        let guard = self.inner.query_child_handler.read().unwrap();
        guard.as_ref().map(|handler| handler(child_ref))
    }

    /// Whether this actor is infrastructure/system.
    pub fn is_system(&self) -> bool {
        self.inner.is_system.load(Ordering::Relaxed)
    }

    /// Store a post-mortem snapshot for this actor in the proc's
    /// `terminated_snapshots` map. Called by the introspect task
    /// just before exiting on terminal status.
    ///
    /// Eviction policy when the retention cap is exceeded:
    /// 1. Evict cleanly-stopped actors first (no `failure_info`).
    /// 2. When only failed actors remain, evict the most recent
    ///    (by `occurred_at`), preserving the earliest failures
    ///    which are closest to the root cause.
    pub fn store_terminated_snapshot(&self, payload: crate::introspect::IntrospectResult) {
        let snapshots = &self.inner.proc.inner.terminated_snapshots;
        snapshots.insert(
            self.actor_addr().id().clone(),
            TerminatedSnapshot {
                actor_addr: self.actor_addr().clone(),
                payload,
            },
        );
        let max = hyperactor_config::global::get(crate::config::TERMINATED_SNAPSHOT_RETENTION);
        let excess = snapshots.len().saturating_sub(max);
        if excess > 0 {
            // Build entries for the eviction selector.
            let entries: Vec<_> = snapshots
                .iter()
                .map(|entry| {
                    let occurred_at = serde_json::from_str::<hyperactor_config::Attrs>(
                        &entry.value().payload.attrs,
                    )
                    .ok()
                    .and_then(|attrs| {
                        // Presence of FAILURE_ERROR_MESSAGE means the actor failed.
                        attrs
                            .get(crate::introspect::FAILURE_ERROR_MESSAGE)
                            .cloned()?;
                        // Extract occurred_at timestamp for sorting.
                        attrs
                            .get(crate::introspect::FAILURE_OCCURRED_AT)
                            .map(|t| humantime::format_rfc3339(*t).to_string())
                    });
                    (entry.value().actor_addr.clone(), occurred_at)
                })
                .collect();

            for key in select_eviction_candidates(&entries, excess) {
                snapshots.remove(key.id());
            }
        }
    }

    /// This is temporary so that we can share binding code between handle and instance.
    /// We should find some (better) way to consolidate the two.
    pub(crate) fn bind<A: Actor, R: Binds<A>>(&self, ports: &HandlerPorts<A>) -> ActorRef<R> {
        <R as Binds<A>>::bind(ports);
        // Undeliverable: dispatched through the work queue to the
        // actor's Handler<Undeliverable<MessageEnvelope>>.
        //
        // IntrospectMessage: registered directly in Instance::new()
        // and handled by a dedicated introspect task.
        ports.bind::<Undeliverable<MessageEnvelope>>();
        // TODO: consider sharing `ports.bound` directly.
        for entry in ports.bound.iter() {
            self.inner
                .exported_named_ports
                .insert(*entry.key(), entry.value());
        }
        ActorRef::attest(ActorAddr::new(
            self.actor_addr().id().clone(),
            self.inner.proc.default_location(),
        ))
    }

    /// Attempt to downcast this cell to a concrete actor handle.
    pub(crate) fn downcast_handle<A: Actor>(&self) -> Option<ActorHandle<A>> {
        let ports = Arc::clone(&self.inner.ports)
            .downcast::<HandlerPorts<A>>()
            .ok()?;
        Some(ActorHandle::new(self.clone(), ports))
    }

    /// Traverse the subtree rooted at this instance in pre-order.
    /// The callback receives each InstanceCell and its depth (root = 0).
    /// Children are visited in pid order for deterministic traversal.
    pub fn traverse<F>(&self, f: &mut F)
    where
        F: FnMut(&InstanceCell, usize),
    {
        self.traverse_inner(0, f);
    }

    fn traverse_inner<F>(&self, depth: usize, f: &mut F)
    where
        F: FnMut(&InstanceCell, usize),
    {
        f(self, depth);
        // Collect and sort children by uid for deterministic traversal order
        let mut children: Vec<_> = self.child_iter().map(|r| r.value().clone()).collect();
        children.sort_by_key(|c| c.uid().clone());
        for child in children {
            child.traverse_inner(depth + 1, f);
        }
    }
}

impl Drop for InstanceCellState {
    fn drop(&mut self) {
        if let Some(parent) = self.maybe_unlink_parent() {
            tracing::debug!(
                "instance {} was dropped with parent {} still linked",
                self.actor_id,
                parent.actor_addr()
            );
        }
        if self
            .proc
            .inner
            .instances
            .remove(self.actor_id.id())
            .is_none()
        {
            tracing::error!("instance {} was dropped but not in proc", self.actor_id);
        }
        self.proc.inner.root_actors.remove(self.actor_id.id());
    }
}

/// A weak version of the InstanceCell. This is used to provide cyclical
/// linkage between actors without creating a strong reference cycle.
#[derive(Debug, Clone)]
pub struct WeakInstanceCell {
    inner: Weak<InstanceCellState>,
}

impl Default for WeakInstanceCell {
    fn default() -> Self {
        Self::new()
    }
}

impl WeakInstanceCell {
    /// Create a new weak instance cell that is never upgradeable.
    pub fn new() -> Self {
        Self { inner: Weak::new() }
    }

    /// Upgrade this weak instance cell to a strong reference, if possible.
    pub fn upgrade(&self) -> Option<InstanceCell> {
        self.inner.upgrade().map(InstanceCell::wrap)
    }
}

/// A polymorphic dictionary that stores runtime-dispatched handler ports.
/// The interface memoizes the ports so that they are reused. We do not
/// (yet) support stable identifiers across multiple instances of the same
/// actor.
pub struct HandlerPorts<A: Actor> {
    ports: DashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>,
    bound: DashMap<u64, &'static str>,
    mailbox: Mailbox,
    workq: OrderedSender<WorkCell<A>>,
    /// Per-actor queue depth (PD-5). Shared with `InstanceCellState`.
    queue_depth: Arc<AtomicU64>,
    /// Proc-level queue-pressure stats (PD-6 through PD-9).
    proc_stats: Arc<ProcQueueStats>,
}

impl<A: Actor> HandlerPorts<A> {
    fn new(
        mailbox: Mailbox,
        workq: OrderedSender<WorkCell<A>>,
        queue_depth: Arc<AtomicU64>,
        proc_stats: Arc<ProcQueueStats>,
    ) -> Self {
        Self {
            ports: DashMap::new(),
            bound: DashMap::new(),
            mailbox,
            workq,
            queue_depth,
            proc_stats,
        }
    }

    /// Get a port for the Handler<M> of actor A.
    pub(crate) fn get<M: Message>(&self) -> PortHandle<M>
    where
        A: Handler<M>,
    {
        let key = TypeId::of::<M>();
        match self.ports.entry(key) {
            Entry::Vacant(entry) => {
                // Runtime control-plane ports are provisioned directly, not
                // through HandlerPorts, nor wired to the work queue. So they
                // should never hit this code path.
                assert!(
                    !crate::ordering::is_bypass_workq_type_id(key),
                    "cannot provision bypass-workq port {} through `Ports::get`; \
                     it must be pre-registered via `open_message_port` in `Instance::new`",
                    std::any::type_name::<M>()
                );

                let type_info = TypeInfo::get_by_typeid(key);
                let workq = self.workq.clone();
                let actor_id = self.mailbox.actor_addr().to_string();
                let enqueue_depth = Arc::clone(&self.queue_depth);
                let enqueue_proc_stats = Arc::clone(&self.proc_stats);
                // Handler-port draining holds an ingress guard while this
                // closure runs. Therefore, the drain guarantee depends on this
                // closure synchronously finishing all work that it admits into
                // the actor work queue before it returns. That includes the
                // ordered path: `OrderedSender::send` delivers the current item
                // and synchronously flushes any consecutive buffered items that
                // the current item unblocks. Messages already held in the
                // reorder buffer but still waiting on a future sequence are not
                // considered drainable accepted work; after draining begins,
                // that missing future sequence is rejected.
                let enqueue = move |headers: Flattrs, msg: M| {
                    // Extract values from headers BEFORE they're moved into
                    // WorkCell — Flattrs::get returns owned typed values, so
                    // these bindings don't borrow from `headers` and `headers`
                    // can be moved into WorkCell freely.
                    let seq_info = headers.get(SEQ_INFO);
                    let sender = headers.get(crate::mailbox::headers::SENDER_ACTOR_ID);

                    let work = WorkCell::new(move |actor: &mut A, instance: &Instance<A>| {
                        Box::pin(async move {
                            // SAFETY: we guarantee that the passed type_info is for type M.
                            unsafe {
                                instance
                                    .handle_message(actor, type_info, headers, msg)
                                    .await
                            }
                        })
                    });
                    // PD-5b: account the enqueue BEFORE handing the work
                    // to the queue. Otherwise the consumer can race and
                    // call `account_dequeue` before this thread accounts
                    // the enqueue, underflowing `running_total`. On send
                    // failure, `account_cancel_enqueue` rolls back the
                    // counters so `queue_depth` does not drift.
                    account_enqueue(&enqueue_depth, &enqueue_proc_stats, &actor_id);
                    let result = if workq.enable_buffering {
                        match seq_info {
                            Some(SeqInfo::Session { session_id, seq }) => {
                                // TODO: return the message contained in the error instead of dropping them when converting
                                // to anyhow::Error. In that way, the message can be picked up by mailbox and returned to sender.
                                workq.send(session_id, seq, sender, work).map_err(|e| match e {
                                    OrderedSenderError::InvalidZeroSeq(_) => {
                                        let error_msg = format!(
                                            "in enqueue func for {}, got seq 0 for message type {}",
                                            actor_id,
                                            std::any::type_name::<M>(),
                                        );
                                        tracing::error!(error_msg);
                                        anyhow::anyhow!(error_msg)
                                    }
                                    OrderedSenderError::SendError(e) => anyhow::Error::from(e),
                                    OrderedSenderError::FlushError(e) => e,
                                })
                            }
                            Some(SeqInfo::Direct) => {
                                workq.direct_send(work).map_err(anyhow::Error::from)
                            }
                            None => {
                                let error_msg = format!(
                                    "in enqueue func for {}, buffering is enabled, but SEQ_INFO is not set for message type {}",
                                    actor_id,
                                    std::any::type_name::<M>(),
                                );
                                tracing::error!(error_msg);
                                Err(anyhow::anyhow!(error_msg))
                            }
                        }
                    } else {
                        workq.direct_send(work).map_err(anyhow::Error::from)
                    };
                    if result.is_err() {
                        account_cancel_enqueue(&enqueue_depth, &enqueue_proc_stats, &actor_id);
                    }
                    result
                };
                let port = self.mailbox.open_handler_enqueue_port(enqueue);
                entry.insert(Box::new(port.clone()));
                port
            }
            Entry::Occupied(entry) => {
                let port = entry.get();
                port.downcast_ref::<PortHandle<M>>().unwrap().clone()
            }
        }
    }

    /// Bind the given message type to its handler port.
    pub fn bind<M: RemoteMessage>(&self)
    where
        A: Handler<M>,
    {
        let port_index = M::port();
        match self.bound.entry(port_index) {
            Entry::Vacant(entry) => {
                self.get::<M>().bind_handler_port();
                entry.insert(M::typename());
            }
            Entry::Occupied(entry) => {
                assert_eq!(
                    *entry.get(),
                    M::typename(),
                    "bind {}: port index {} already bound to type {}",
                    M::typename(),
                    port_index,
                    entry.get(),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::atomic::AtomicBool;

    use hyperactor_macros::export;
    use serde_json::json;
    use timed_test::async_timed_test;
    use tokio::sync::Barrier;
    use tokio::sync::oneshot;
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_test::internal::logs_with_scope_contain;

    use super::*;
    // needed for in-crate macro expansion
    use crate as hyperactor;
    use crate::HandleClient;
    use crate::Handler;
    use crate::OncePortRef;
    use crate::PortRef;
    use crate::port::Port;
    use crate::testing::proc_supervison::ProcSupervisionCoordinator;
    use crate::testing::process_assertion::assert_termination;

    #[derive(Debug, Default)]
    #[export]
    struct TestActor;

    impl Actor for TestActor {}

    #[derive(Debug)]
    struct ChildLabelActor;

    impl Actor for ChildLabelActor {}

    #[derive(Debug)]
    struct DelayedSelfActor {
        ready: Option<OncePortRef<()>>,
        fired: Option<OncePortRef<()>>,
        delay: Duration,
    }

    #[derive(Debug)]
    struct DelayedSelfTick;

    #[async_trait]
    impl Actor for DelayedSelfActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            if let Some(ready) = self.ready.take() {
                ready.post(this, ());
            }
            this.post_after(this, DelayedSelfTick, self.delay);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<DelayedSelfTick> for DelayedSelfActor {
        async fn handle(
            &mut self,
            cx: &crate::Context<Self>,
            _message: DelayedSelfTick,
        ) -> anyhow::Result<()> {
            if let Some(fired) = self.fired.take() {
                fired.post(cx, ());
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct DelayedPortActor {
        reply: Option<PortRef<u64>>,
        delay: Duration,
    }

    #[async_trait]
    impl Actor for DelayedPortActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            this.post_after(
                self.reply.take().expect("reply port should be present"),
                123u64,
                self.delay,
            );
            Ok(())
        }
    }

    #[derive(Handler, HandleClient, Debug)]
    enum TestActorMessage {
        Reply(oneshot::Sender<()>),
        Wait(oneshot::Sender<()>, oneshot::Receiver<()>),
        Forward(ActorHandle<TestActor>, Box<TestActorMessage>),
        Noop(),
        Fail(anyhow::Error),
        Panic(String),
        Spawn(oneshot::Sender<ActorHandle<TestActor>>),
    }

    impl TestActor {
        async fn spawn_child(
            cx: &impl context::Actor,
            parent: &ActorHandle<TestActor>,
        ) -> ActorHandle<TestActor> {
            let (tx, rx) = oneshot::channel();
            parent.post(cx, TestActorMessage::Spawn(tx));
            rx.await.unwrap()
        }
    }

    #[test]
    fn test_proc_identity_constructors() {
        let anonymous = Proc::anonymous();
        assert!(
            matches!(anonymous.proc_id().uid(), crate::id::Uid::Instance(_, None)),
            "anonymous proc must have an unlabeled instance id"
        );
        assert_eq!(anonymous.proc_id().label(), None);

        let instance = Proc::instance("worker");
        assert!(
            matches!(
                instance.proc_id().uid(),
                crate::id::Uid::Instance(_, Some(label)) if label.as_str() == "worker"
            ),
            "instance proc must have a labeled instance id"
        );
        assert_eq!(
            instance.proc_id().label().map(|label| label.as_str()),
            Some("worker")
        );

        let singleton = Proc::singleton("controller");
        assert!(
            matches!(
                singleton.proc_id().uid(),
                crate::id::Uid::Singleton(label) if label.as_str() == "controller"
            ),
            "singleton proc must have a singleton id"
        );
        assert_eq!(
            singleton.proc_id().label().map(|label| label.as_str()),
            Some("controller")
        );
    }

    #[test]
    fn test_default_actor_label_uses_label_compatible_type_basename() {
        assert_eq!(default_actor_label::<TestActor>().as_str(), "testactor");
        assert_eq!(
            default_actor_label::<std::collections::HashMap<String, u64>>().as_str(),
            "hashmap"
        );
        assert_eq!(default_actor_label::<()>().as_str(), "nil");
    }

    #[test]
    fn test_global_proc_label_uses_short_hostname_and_pid() {
        assert_eq!(
            global_proc_label_from("devvm34959.nha0.facebook.com", 123555).as_str(),
            "devvm34959-123555"
        );
        assert_eq!(
            global_proc_label_from("DevVM34959.nha0.facebook.com", 7).as_str(),
            "devvm34959-7"
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_uses_actor_type_label_for_root_actor() {
        let proc = Proc::isolated();
        let handle = proc.spawn(TestActor);

        assert_eq!(
            handle.actor_addr().label().map(Label::as_str),
            Some("testactor")
        );
        assert!(matches!(
            handle.actor_addr().uid(),
            Uid::Instance(_, Some(label)) if label.as_str() == "testactor"
        ));

        handle.drain_and_stop("test").unwrap();
        handle.await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_uses_actor_type_label_for_child_actor() {
        let proc = Proc::isolated();
        let parent = proc.spawn(TestActor);
        let child = proc.spawn_child(parent.cell().clone(), ChildLabelActor);

        assert!(!child.actor_addr().is_root());
        assert_eq!(
            child.actor_addr().label().map(Label::as_str),
            Some("childlabelactor")
        );
        assert!(matches!(
            child.actor_addr().uid(),
            Uid::Instance(_, Some(label)) if label.as_str() == "childlabelactor"
        ));

        child.drain_and_stop("test").unwrap();
        parent.drain_and_stop("test").unwrap();
        child.await;
        parent.await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_root_tracking_does_not_depend_on_singleton_uids() {
        let proc = Proc::isolated();
        let parent = proc.spawn(TestActor);
        let child = proc.spawn_child(parent.cell().clone(), TestActor);

        assert!(parent.actor_addr().uid().is_instance());
        let roots = proc.root_actor_ids();
        assert!(
            roots
                .iter()
                .any(|root| root.id() == parent.actor_addr().id())
        );
        assert!(
            !roots
                .iter()
                .any(|root| root.id() == child.actor_addr().id())
        );

        let mut traversed = Vec::new();
        proc.traverse(&mut |cell, _depth| {
            traversed.push(cell.actor_addr().id().clone());
        });
        assert!(traversed.contains(parent.actor_addr().id()));
        assert!(traversed.contains(child.actor_addr().id()));

        child.drain_and_stop("test").unwrap();
        parent.drain_and_stop("test").unwrap();
        child.await;
        parent.await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_client_spawn_api_labels_and_explicit_uid() {
        let proc = Proc::isolated();
        let client = proc.client("client");

        let spawned = client.spawn(TestActor);
        assert_eq!(
            spawned.actor_addr().label().map(Label::as_str),
            Some("testactor")
        );

        let labeled = client.spawn_with_label("custom", TestActor);
        assert_eq!(
            labeled.actor_addr().label().map(Label::as_str),
            Some("custom")
        );

        let uid = Uid::instance(Label::new("explicit").unwrap());
        let explicit = client.spawn_with_uid(uid.clone(), TestActor).unwrap();
        assert_eq!(explicit.actor_addr().uid(), &uid);

        let child = client.child();
        assert!(!child.self_addr().is_root());
        assert!(matches!(child.self_addr().uid(), Uid::Instance(_, None)));
        assert_eq!(child.self_addr().label(), None);
        let child_spawned = child.spawn(TestActor);
        assert_eq!(
            child_spawned.actor_addr().label().map(Label::as_str),
            Some("testactor")
        );

        spawned.drain_and_stop("test").unwrap();
        labeled.drain_and_stop("test").unwrap();
        explicit.drain_and_stop("test").unwrap();
        child_spawned.drain_and_stop("test").unwrap();
        spawned.await;
        labeled.await;
        explicit.await;
        child_spawned.await;
    }

    #[test]
    fn test_current_proc_uses_stable_global_proc_outside_actor_context() {
        let first = Proc::current();
        let second = Proc::current();

        assert_eq!(first.proc_id(), second.proc_id());
        assert_eq!(
            Gateway::current().proc_addr(first.proc_id()),
            first.proc_addr()
        );
    }

    #[async_trait]
    #[crate::handle(TestActorMessage)]
    impl TestActorMessageHandler for TestActor {
        async fn reply(
            &mut self,
            _cx: &crate::Context<Self>,
            sender: oneshot::Sender<()>,
        ) -> Result<(), anyhow::Error> {
            sender.send(()).unwrap();
            Ok(())
        }

        async fn wait(
            &mut self,
            _cx: &crate::Context<Self>,
            sender: oneshot::Sender<()>,
            receiver: oneshot::Receiver<()>,
        ) -> Result<(), anyhow::Error> {
            sender.send(()).unwrap();
            receiver.await.unwrap();
            Ok(())
        }

        async fn forward(
            &mut self,
            cx: &crate::Context<Self>,
            destination: ActorHandle<TestActor>,
            message: Box<TestActorMessage>,
        ) -> Result<(), anyhow::Error> {
            // TODO: this needn't be async
            destination.post(cx, *message);
            Ok(())
        }

        async fn noop(&mut self, _cx: &crate::Context<Self>) -> Result<(), anyhow::Error> {
            Ok(())
        }

        async fn fail(
            &mut self,
            _cx: &crate::Context<Self>,
            err: anyhow::Error,
        ) -> Result<(), anyhow::Error> {
            Err(err)
        }

        async fn panic(
            &mut self,
            _cx: &crate::Context<Self>,
            err_msg: String,
        ) -> Result<(), anyhow::Error> {
            panic!("{}", err_msg);
        }

        async fn spawn(
            &mut self,
            cx: &crate::Context<Self>,
            reply: oneshot::Sender<ActorHandle<TestActor>>,
        ) -> Result<(), anyhow::Error> {
            let handle = cx.spawn(TestActor);
            reply.send(handle).unwrap();
            Ok(())
        }
    }

    #[derive(Debug)]
    struct CurrentProcActor;

    impl Actor for CurrentProcActor {}

    #[derive(Handler, Debug)]
    enum CurrentProcMessage {
        Check(oneshot::Sender<CurrentProcSnapshot>),
    }

    #[derive(Debug)]
    struct CurrentProcSnapshot {
        current_proc_id: ProcId,
        current_gateway_proc_addr: ProcAddr,
        spawned_handle: ActorHandle<TestActor>,
        client_proc_id: ProcId,
    }

    #[async_trait]
    #[crate::handle(CurrentProcMessage)]
    impl CurrentProcMessageHandler for CurrentProcActor {
        async fn check(
            &mut self,
            _cx: &crate::Context<Self>,
            reply: oneshot::Sender<CurrentProcSnapshot>,
        ) -> Result<(), anyhow::Error> {
            let current = Proc::current();
            let spawned_handle = crate::spawn(TestActor);
            let client = crate::client("current_client");
            reply
                .send(CurrentProcSnapshot {
                    current_proc_id: current.proc_id().clone(),
                    current_gateway_proc_addr: Gateway::current().proc_addr(current.proc_id()),
                    spawned_handle,
                    client_proc_id: client.self_addr().proc_id().clone(),
                })
                .unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_current_proc_tracks_actor_context() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let actor = proc.spawn(CurrentProcActor);
        let (tx, rx) = oneshot::channel();

        crate::Endpoint::post(&actor, &client, CurrentProcMessage::Check(tx));
        let snapshot = rx.await.unwrap();

        assert_eq!(&snapshot.current_proc_id, proc.proc_id());
        assert_eq!(snapshot.current_gateway_proc_addr, proc.proc_addr());
        assert_eq!(
            snapshot.spawned_handle.actor_addr().proc_id(),
            proc.proc_id()
        );
        assert_eq!(&snapshot.client_proc_id, proc.proc_id());

        snapshot
            .spawned_handle
            .drain_and_stop("test complete")
            .unwrap();
        snapshot.spawned_handle.await;
        actor.drain_and_stop("test complete").unwrap();
        actor.await;
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "tracing_test::traced_test macro expansion holds tracing::span::Entered across awaits; can't be fixed in our code"
    )]
    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_actor() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn(TestActor);

        // Check on the join handle.
        assert!(logs_contain(
            format!(
                "{}: spawned with {:?}",
                handle.actor_addr(),
                handle.cell().actor_task_handle().unwrap(),
            )
            .as_str()
        ));

        let mut state = handle.status().clone();

        // Send a ping-pong to the actor. Wait for the actor to become idle.

        let (tx, rx) = oneshot::channel::<()>();
        handle.post(&client, TestActorMessage::Reply(tx));
        rx.await.unwrap();

        state
            .wait_for(|state: &ActorStatus| matches!(*state, ActorStatus::Idle))
            .await
            .unwrap();

        // Make sure we enter processing state while the actor is handling a message.
        let (enter_tx, enter_rx) = oneshot::channel::<()>();
        let (exit_tx, exit_rx) = oneshot::channel::<()>();

        handle.post(&client, TestActorMessage::Wait(enter_tx, exit_rx));
        enter_rx.await.unwrap();
        assert_matches!(*state.borrow(), ActorStatus::Processing(instant, _) if instant <= std::time::SystemTime::now());
        exit_tx.send(()).unwrap();

        state
            .wait_for(|state| matches!(*state, ActorStatus::Idle))
            .await
            .unwrap();

        handle.drain_and_stop("test").unwrap();
        handle.await;
        assert_matches!(&*state.borrow(), ActorStatus::Stopped(reason) if reason == "test");
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_proc_actors_messaging() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let first = proc.spawn_with_label::<TestActor>("first", TestActor);
        let second = proc.spawn_with_label::<TestActor>("second", TestActor);
        let (tx, rx) = oneshot::channel::<()>();
        let reply_message = TestActorMessage::Reply(tx);
        first.post(
            &client,
            TestActorMessage::Forward(second, Box::new(reply_message)),
        );
        rx.await.unwrap();
    }

    /// Proc ownership is based on `ProcId`, not the routeable
    /// `ProcAddr`. A proc may be reached through multiple locations,
    /// but a different proc id must still forward even when the
    /// location matches.
    #[tokio::test]
    async fn test_post_routes_by_proc_id() {
        use crate::mailbox::monitored_return_handle;
        use crate::testing::ids::test_actor_id;

        #[derive(Clone)]
        struct CountingSender(Arc<AtomicUsize>);

        #[async_trait]
        impl MailboxSender for CountingSender {
            fn post_unchecked(
                &self,
                _envelope: MessageEnvelope,
                _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
            ) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        // Distinct in-process local addresses; `ChannelAddr::any` would
        // hand out the same `Local(0)` sentinel both times.
        let local_addr = ChannelAddr::Local(1);
        let remote_addr = ChannelAddr::Local(2);

        let proc_local = ProcAddr::instance(local_addr.clone(), "shared");
        let proc_same_id_other_location =
            ProcAddr::new(proc_local.id().clone(), remote_addr.into());
        let proc_other_id_same_location = ProcAddr::instance(local_addr, "other");
        assert_eq!(
            proc_local.id(),
            proc_same_id_other_location.id(),
            "test setup: both procs must share a ProcId"
        );
        assert_ne!(
            proc_local.id(),
            proc_other_id_same_location.id(),
            "test setup: the remote proc must have a distinct ProcId"
        );

        let forwarded = Arc::new(AtomicUsize::new(0));
        let proc = Proc::configured(
            proc_local.clone(),
            BoxedMailboxSender::new(CountingSender(forwarded.clone())),
        );
        let sender = test_actor_id("sender", "client");

        // Same ProcId, same location: route locally; the forwarder must not see it.
        let local_dest = proc_local.actor_addr("worker").port_addr(Port::from(1234));
        proc.post(
            MessageEnvelope::new(
                sender.clone(),
                local_dest,
                wirevalue::Any::serialize(&1u64).unwrap(),
                Flattrs::new(),
            ),
            monitored_return_handle(),
        );
        assert_eq!(forwarded.load(Ordering::SeqCst), 0);

        // Same instance ProcId, different location: still local ownership.
        let same_id_other_location_dest = proc_same_id_other_location
            .actor_addr("worker")
            .port_addr(Port::from(1234));
        proc.post(
            MessageEnvelope::new(
                sender.clone(),
                same_id_other_location_dest,
                wirevalue::Any::serialize(&1u64).unwrap(),
                Flattrs::new(),
            ),
            monitored_return_handle(),
        );
        assert_eq!(forwarded.load(Ordering::SeqCst), 0);

        // Different ProcId, same location: forward.
        let other_id_same_location_dest = proc_other_id_same_location
            .actor_addr("worker")
            .port_addr(Port::from(1234));
        proc.post(
            MessageEnvelope::new(
                sender,
                other_id_same_location_dest,
                wirevalue::Any::serialize(&1u64).unwrap(),
                Flattrs::new(),
            ),
            monitored_return_handle(),
        );
        assert_eq!(forwarded.load(Ordering::SeqCst), 1);
    }

    /// `Instance::post` (-> `MailboxExt::post`) must stamp `SENDER_ACTOR_ID`
    /// alongside the `SEQ_INFO` it assigns when the destination is a handler
    /// port. Verified by forwarding to a different `ProcId` and capturing the
    /// outbound envelope.
    #[tokio::test]
    async fn test_mailbox_ext_post_stamps_sender_actor_id() {
        use typeuri::Named;

        use crate::mailbox::headers::SENDER_ACTOR_ID;

        #[derive(typeuri::Named)]
        struct DestHandlerMsg;

        #[derive(Clone, Default)]
        struct CapturingSender(Arc<Mutex<Vec<MessageEnvelope>>>);

        #[async_trait]
        impl MailboxSender for CapturingSender {
            fn post_unchecked(
                &self,
                envelope: MessageEnvelope,
                _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
            ) {
                self.0.lock().unwrap().push(envelope);
            }
        }

        let proc_addr = ProcAddr::instance(ChannelAddr::Local(1), "stamping_test");
        let captured: Arc<Mutex<Vec<MessageEnvelope>>> = Arc::new(Mutex::new(Vec::new()));
        let proc = Proc::configured(
            proc_addr,
            BoxedMailboxSender::new(CapturingSender(captured.clone())),
        );

        let client = proc.client("client");
        let client_addr = client.mailbox().actor_addr().clone();

        // Distinct ProcId so the envelope routes through the configured
        // forwarder (CapturingSender), where we can inspect the headers.
        let remote_dest = ProcAddr::instance(ChannelAddr::Local(2), "remote")
            .actor_addr("worker")
            .port_addr(Port::from(DestHandlerMsg::port()));

        // UFCS to select MailboxExt::post over Endpoint::post (also in
        // scope at module level via `use ... as _`). Client implements
        // `context::Actor` so the MailboxExt blanket impl applies.
        <Client as context::MailboxExt>::post(
            &client,
            remote_dest,
            Flattrs::new(),
            wirevalue::Any::serialize(&1u64).unwrap(),
            false,
            context::SeqInfoPolicy::AssignNew,
        );

        let captured = captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "exactly one envelope should be forwarded"
        );
        assert_eq!(
            captured[0].headers().get(SENDER_ACTOR_ID),
            Some(client_addr),
            "MailboxExt::post must stamp SENDER_ACTOR_ID with the client's actor_addr"
        );
    }

    #[test]
    fn test_local_delivery_service_and_local_compare_full_proc_addr() {
        for name in [LEGACY_SERVICE_PROC_NAME, LEGACY_LOCAL_PROC_NAME] {
            let local = ProcAddr::singleton(ChannelAddr::Local(1), name);
            let same_id_other_location = ProcAddr::singleton(ChannelAddr::Local(2), name);
            let proc = match name {
                LEGACY_SERVICE_PROC_NAME => Proc::legacy_service_pseudo_singleton(
                    ChannelAddr::Local(1),
                    BoxedMailboxSender::new(PanickingMailboxSender),
                ),
                LEGACY_LOCAL_PROC_NAME => Proc::legacy_local_pseudo_singleton(
                    ChannelAddr::Local(1),
                    BoxedMailboxSender::new(PanickingMailboxSender),
                ),
                _ => unreachable!("test only covers legacy pseudo-singletons"),
            };

            assert_eq!(local.id(), same_id_other_location.id());
            assert!(proc.is_local_delivery_target(&local));
            assert!(!proc.is_local_delivery_target(&same_id_other_location));
        }

        let shared = ProcAddr::singleton(ChannelAddr::Local(1), "shared");
        let shared_other_location = ProcAddr::singleton(ChannelAddr::Local(2), "shared");
        let proc = Proc::configured(
            shared.clone(),
            BoxedMailboxSender::new(PanickingMailboxSender),
        );
        assert!(proc.is_local_delivery_target(&shared_other_location));

        let service_instance = ProcAddr::instance(ChannelAddr::Local(1), "service");
        let service_instance_other_location =
            ProcAddr::new(service_instance.id().clone(), ChannelAddr::Local(2).into());
        let proc = Proc::configured(
            service_instance,
            BoxedMailboxSender::new(PanickingMailboxSender),
        );
        assert!(proc.is_local_delivery_target(&service_instance_other_location));
    }

    #[test]
    fn test_legacy_pseudo_singletons_use_dedicated_constructors() {
        for name in [LEGACY_SERVICE_PROC_NAME, LEGACY_LOCAL_PROC_NAME] {
            let result = std::panic::catch_unwind(|| {
                Proc::configured(
                    ProcAddr::singleton(ChannelAddr::Local(1), name),
                    BoxedMailboxSender::new(PanickingMailboxSender),
                );
            });
            assert!(result.is_err());
        }

        let service = Proc::legacy_service_pseudo_singleton(
            ChannelAddr::Local(1),
            BoxedMailboxSender::new(PanickingMailboxSender),
        );
        assert_eq!(
            service.proc_addr().id().uid().to_string(),
            LEGACY_SERVICE_PROC_NAME
        );

        let local = Proc::legacy_local_pseudo_singleton(
            ChannelAddr::Local(2),
            BoxedMailboxSender::new(PanickingMailboxSender),
        );
        assert_eq!(
            local.proc_addr().id().uid().to_string(),
            LEGACY_LOCAL_PROC_NAME
        );
    }

    #[tokio::test]
    async fn test_mailbox_muxer_delivers_by_actor_id() {
        use crate::mailbox::PortLocation;
        use crate::mailbox::monitored_return_handle;
        use crate::testing::ids::test_actor_id;

        let proc = Proc::isolated();
        let instance = proc.client("worker");
        let (port, mut receiver) = instance.bind_handler_port::<u64>();

        let PortLocation::Bound(default_dest) = port.location() else {
            panic!("actor port must be bound");
        };
        let alternate_dest =
            PortAddr::new(default_dest.id().clone(), ChannelAddr::Local(9876).into());

        proc.post(
            MessageEnvelope::serialize(
                test_actor_id("sender", "client"),
                alternate_dest,
                &123u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        assert_eq!(receiver.recv().await.unwrap(), 123);
    }

    #[test]
    fn test_default_location_changes_new_bindings_not_lookup() {
        let proc = Proc::isolated();
        let gateway = proc.gateway();
        let client = proc.client("worker");

        let first_ref: ActorRef<()> = client.bind();
        let new_location = ChannelAddr::Local(9876).into();
        gateway.set_default_location(new_location);
        let second_ref: ActorRef<()> = client.bind();

        assert_eq!(first_ref.actor_addr().id(), second_ref.actor_addr().id());
        assert_ne!(
            first_ref.actor_addr().location(),
            second_ref.actor_addr().location()
        );
        assert_eq!(second_ref.actor_addr().location(), &proc.default_location());
        assert_eq!(proc.default_location(), gateway.default_location());
        assert_eq!(proc.proc_addr(), gateway.proc_addr(proc.proc_id()));
        assert!(proc.get_instance(second_ref.actor_addr()).is_some());
    }

    /// Concurrent `set_default_location` and `handle.bind()` must not
    /// corrupt the bindings. Every bound ref carries one of the racing
    /// locations, and every bound ref is still resolvable via
    /// `get_instance` (which keys on identity, not location).
    #[async_timed_test(timeout_secs = 10)]
    async fn test_default_location_concurrent_with_bind() {
        let proc = Proc::isolated();
        let gateway = proc.gateway();
        let handle = proc.client("worker");

        let loc_a: Location = ChannelAddr::Local(40001).into();
        let loc_b: Location = ChannelAddr::Local(40002).into();

        // Pre-set to loc_a so binds never observe the initial default
        // location. Without this, a bind that runs before the setter's
        // first write could see the initial location and fail the
        // "one of two locations" assertion.
        gateway.set_default_location(loc_a.clone());

        let barrier = std::sync::Arc::new(Barrier::new(2));

        let setter = {
            let gateway = gateway.clone();
            let loc_a = loc_a.clone();
            let loc_b = loc_b.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                for i in 0..100 {
                    let loc = if i % 2 == 0 {
                        loc_a.clone()
                    } else {
                        loc_b.clone()
                    };
                    gateway.set_default_location(loc);
                    tokio::task::yield_now().await;
                }
            })
        };

        let binder = {
            // Clone `handle` into the binder so the outer `handle` stays
            // alive after the spawned task finishes. Without this, the
            // only strong reference to the client's instance drops when
            // the binder task ends and the `proc.get_instance(...)` checks
            // below return None.
            let handle = handle.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                let mut refs = Vec::with_capacity(100);
                for _ in 0..100 {
                    refs.push(handle.bind::<()>());
                    tokio::task::yield_now().await;
                }
                refs
            })
        };

        setter.await.unwrap();
        let refs = binder.await.unwrap();

        // Every ref carries one of the two racing locations.
        for r in &refs {
            let loc = r.actor_addr().location();
            assert!(
                loc == &loc_a || loc == &loc_b,
                "ref location {loc:?} is neither {loc_a:?} nor {loc_b:?}",
            );
        }

        // Every ref is still resolvable via get_instance (identity-based).
        for r in &refs {
            assert!(
                proc.get_instance(r.actor_addr()).is_some(),
                "ref {:?} no longer resolves",
                r.actor_addr(),
            );
        }
    }

    #[test]
    fn test_builder_procs_can_share_gateway_with_distinct_ids() {
        let gateway = Gateway::new();
        let first = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("first")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let second = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("second")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        assert_ne!(first.proc_id(), second.proc_id());
        assert_eq!(first.default_location(), second.default_location());

        let new_location = ChannelAddr::Local(9876).into();
        gateway.set_default_location(new_location);

        assert_eq!(first.default_location(), gateway.default_location());
        assert_eq!(second.default_location(), gateway.default_location());
        assert_eq!(first.proc_addr(), gateway.proc_addr(first.proc_id()));
        assert_eq!(second.proc_addr(), gateway.proc_addr(second.proc_id()));
    }

    #[test]
    fn test_isolated_procs_use_distinct_gateways() {
        let first = Proc::isolated();
        let second = Proc::isolated();
        let second_location = second.default_location();

        first
            .gateway()
            .set_default_location(ChannelAddr::Local(9876).into());

        assert_ne!(first.proc_id(), second.proc_id());
        assert_ne!(first.default_location(), second_location);
        assert_eq!(second.default_location(), second_location);
    }

    #[tokio::test]
    async fn test_gateway_serve_updates_location_and_stops() {
        use crate::mailbox::PortLocation;
        use crate::mailbox::monitored_return_handle;
        use crate::testing::ids::test_actor_id;

        let proc = Proc::isolated();
        let gateway = proc.gateway();
        let initial_location = proc.default_location();
        let client = proc.client("client");
        let (port, mut receiver) = client.bind_handler_port::<u64>();
        let PortLocation::Bound(default_dest) = port.location() else {
            panic!("handler port must be bound");
        };

        async fn send_to_location(
            location: Location,
            default_dest: &PortAddr,
            value: u64,
            receiver: &mut PortReceiver<u64>,
        ) {
            let dest = PortAddr::new(default_dest.id().clone(), location.clone());
            let sender = MailboxClient::dial(location.addr().clone()).unwrap();
            sender.post(
                MessageEnvelope::serialize(
                    test_actor_id("sender", "client"),
                    dest,
                    &value,
                    Flattrs::new(),
                )
                .unwrap(),
                monitored_return_handle(),
            );
            sender.flush().await.unwrap();
            let received = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(received, value);
        }

        let server = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();

        assert_eq!(proc.default_location(), initial_location);
        assert_eq!(proc.default_location(), gateway.default_location());
        assert_eq!(proc.proc_addr(), gateway.proc_addr(proc.proc_id()));
        send_to_location(initial_location.clone(), &default_dest, 1, &mut receiver).await;

        let next_server =
            Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let next_location = proc.default_location();

        assert_ne!(proc.default_location(), initial_location);
        assert_eq!(proc.default_location(), gateway.default_location());
        assert_eq!(proc.proc_addr(), gateway.proc_addr(proc.proc_id()));
        send_to_location(next_location.clone(), &default_dest, 2, &mut receiver).await;
        send_to_location(initial_location.clone(), &default_dest, 3, &mut receiver).await;

        next_server.stop("test complete");
        next_server.await.unwrap().unwrap();

        assert_eq!(proc.default_location(), initial_location);
        assert_eq!(proc.default_location(), gateway.default_location());
        assert!(MailboxClient::dial(next_location.addr().clone()).is_err());
        send_to_location(initial_location.clone(), &default_dest, 4, &mut receiver).await;

        server.stop("test complete");
        server.await.unwrap().unwrap();

        assert_eq!(proc.default_location(), initial_location);
        assert_eq!(proc.default_location(), gateway.default_location());
        assert!(MailboxClient::dial(initial_location.addr().clone()).is_err());
    }

    #[tokio::test]
    async fn test_direct_proc_server_stops_via_join_mailbox_server() {
        let proc = Proc::direct(
            ChannelAddr::any(ChannelTransport::Local),
            "direct".to_string(),
        )
        .unwrap();

        assert_eq!(proc.proc_addr(), proc.gateway().proc_addr(proc.proc_id()));

        proc.join_mailbox_server().await;
    }

    #[tokio::test]
    async fn test_local_only_gateway_returns_undeliverable_messages() {
        use crate::testing::ids::test_actor_id;

        let proc = Proc::isolated();
        let client = proc.client("client");
        let (return_handle, mut undeliverable_rx) =
            client.open_port::<Undeliverable<MessageEnvelope>>();
        let remote_proc = ProcAddr::instance(ChannelAddr::Local(1234), "remote");
        let remote_dest = remote_proc.actor_addr("worker").port_addr(Port::from(0));

        proc.post(
            MessageEnvelope::serialize(
                test_actor_id("sender", "client"),
                remote_dest.clone(),
                &123u64,
                Flattrs::new(),
            )
            .unwrap(),
            return_handle,
        );

        let Undeliverable::Message(envelope) = undeliverable_rx.recv().await.unwrap() else {
            panic!("expected returned message");
        };
        assert_eq!(envelope.dest(), &remote_dest);
    }

    #[derive(Debug, Default)]
    #[export]
    struct LookupTestActor;

    impl Actor for LookupTestActor {}

    #[derive(Handler, HandleClient, Debug)]
    enum LookupTestMessage {
        ActorExists(ActorRef<TestActor>, #[reply] OncePortRef<bool>),
    }

    #[async_trait]
    #[crate::handle(LookupTestMessage)]
    impl LookupTestMessageHandler for LookupTestActor {
        async fn actor_exists(
            &mut self,
            cx: &crate::Context<Self>,
            actor_ref: ActorRef<TestActor>,
        ) -> Result<bool, anyhow::Error> {
            Ok(actor_ref.downcast_handle(cx).is_some())
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_actor_lookup() {
        let proc = Proc::isolated();
        let client = proc.client("client");

        let target_actor = proc.spawn(TestActor);
        let target_actor_ref = target_actor.bind();
        let lookup_actor = proc.spawn(LookupTestActor);

        assert!(
            lookup_actor
                .actor_exists(&client, target_actor_ref.clone())
                .await
                .unwrap()
        );

        // Make up a child actor. It shouldn't exist.
        assert!(
            !lookup_actor
                .actor_exists(
                    &client,
                    ActorRef::attest(target_actor.actor_addr().anonymous_child())
                )
                .await
                .unwrap()
        );
        // A wrongly-typed actor ref should also not obtain.
        assert!(
            !lookup_actor
                .actor_exists(&client, ActorRef::attest(lookup_actor.actor_addr().clone()))
                .await
                .unwrap()
        );

        target_actor.drain_and_stop("test").unwrap();
        target_actor.await;

        assert!(
            !lookup_actor
                .actor_exists(&client, target_actor_ref)
                .await
                .unwrap()
        );

        lookup_actor.drain_and_stop("test").unwrap();
        lookup_actor.await;
    }

    fn validate_link(child: &InstanceCell, parent: &InstanceCell) {
        assert_eq!(
            child.actor_addr().proc_addr(),
            parent.actor_addr().proc_addr()
        );
        assert_eq!(
            child.inner.parent.upgrade().unwrap().actor_addr(),
            parent.actor_addr()
        );
        assert_matches!(
            parent.inner.children.get(child.uid()),
            Some(node) if node.actor_addr() == child.actor_addr()
        );
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "tracing_test::traced_test macro expansion holds tracing::span::Entered across awaits; can't be fixed in our code"
    )]
    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_child() {
        let proc = Proc::isolated();
        let client = proc.client("client");

        let first = proc.spawn_with_label::<TestActor>("first", TestActor);
        let second = TestActor::spawn_child(&client, &first).await;
        let third = TestActor::spawn_child(&client, &second).await;

        // Check we've got the join handles.
        assert!(logs_with_scope_contain(
            "hyperactor::proc",
            format!(
                "{}: spawned with {:?}",
                first.actor_addr(),
                first.cell().actor_task_handle().unwrap()
            )
            .as_str()
        ));
        assert!(logs_with_scope_contain(
            "hyperactor::proc",
            format!(
                "{}: spawned with {:?}",
                second.actor_addr(),
                second.cell().actor_task_handle().unwrap()
            )
            .as_str()
        ));
        assert!(logs_with_scope_contain(
            "hyperactor::proc",
            format!(
                "{}: spawned with {:?}",
                third.actor_addr(),
                third.cell().actor_task_handle().unwrap()
            )
            .as_str()
        ));

        // All actors are in the same proc:
        assert_eq!(first.actor_addr().proc_addr(), proc.proc_addr());
        assert_eq!(second.actor_addr().proc_addr(), proc.proc_addr());
        assert_eq!(third.actor_addr().proc_addr(), proc.proc_addr());

        // Supervision tree is constructed correctly.
        validate_link(third.cell(), second.cell());
        validate_link(second.cell(), first.cell());
        assert!(first.cell().inner.parent.upgrade().is_none());

        // Supervision tree is torn down correctly.
        // Once each actor is stopped, it should have no linked children.
        let third_cell = third.cell().clone();
        third.drain_and_stop("test").unwrap();
        third.await;
        assert!(third_cell.inner.children.is_empty());
        drop(third_cell);
        validate_link(second.cell(), first.cell());

        let second_cell = second.cell().clone();
        second.drain_and_stop("test").unwrap();
        second.await;
        assert!(second_cell.inner.children.is_empty());
        drop(second_cell);

        let first_cell = first.cell().clone();
        first.drain_and_stop("test").unwrap();
        first.await;
        assert!(first_cell.inner.children.is_empty());
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_child_lifecycle() {
        let proc = Proc::isolated();
        let client = proc.client("client");

        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let root_1 = TestActor::spawn_child(&client, &root).await;
        let root_2 = TestActor::spawn_child(&client, &root).await;
        let root_2_1 = TestActor::spawn_child(&client, &root_2).await;

        root.drain_and_stop("test").unwrap();
        root.await;

        for actor in [root_1, root_2, root_2_1] {
            assert!(
                actor
                    .port::<TestActorMessage>()
                    .try_post(&client, TestActorMessage::Noop())
                    .is_err()
            );
            assert_matches!(actor.await, ActorStatus::Stopped(reason) if reason == "parent stopping");
        }
    }

    #[derive(Debug)]
    struct DeferredStopActor {
        stop_started: Arc<tokio::sync::Notify>,
        release_stop: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Actor for DeferredStopActor {
        async fn handle_stop(
            &mut self,
            this: &Instance<Self>,
            mode: StopMode,
            reason: &str,
        ) -> Result<(), anyhow::Error> {
            let this = this.clone_for_py();
            let release_stop = Arc::clone(&self.release_stop);
            let reason = reason.to_string();
            this.close();
            self.stop_started.notify_one();
            tokio::spawn(async move {
                release_stop.notified().await;
                match mode {
                    StopMode::Stop => this.exit(&reason).unwrap(),
                    StopMode::DrainAndStop => this.exit_after_drain(&reason).unwrap(),
                }
            });
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<()> for DeferredStopActor {
        async fn handle(&mut self, _cx: &crate::Context<Self>, _message: ()) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_handle_stop_can_defer_exit() {
        let proc = Proc::isolated();
        let stop_started = Arc::new(tokio::sync::Notify::new());
        let release_stop = Arc::new(tokio::sync::Notify::new());
        let handle = proc.spawn(DeferredStopActor {
            stop_started: Arc::clone(&stop_started),
            release_stop: Arc::clone(&release_stop),
        });

        let mut status = handle.status();
        handle.stop("test").unwrap();
        stop_started.notified().await;
        status
            .wait_for(|state| matches!(state, ActorStatus::Stopping))
            .await
            .unwrap();

        release_stop.notify_one();
        assert_matches!(handle.await, ActorStatus::Stopped(reason) if reason == "test");
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_drain_and_stop_closes_handler_ingress() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let stop_started = Arc::new(tokio::sync::Notify::new());
        let release_stop = Arc::new(tokio::sync::Notify::new());
        let handle = proc.spawn(DeferredStopActor {
            stop_started: Arc::clone(&stop_started),
            release_stop: Arc::clone(&release_stop),
        });

        handle.drain_and_stop("test").unwrap();
        stop_started.notified().await;

        // Drain closes runtime-dispatched handler ingress, so new
        // sends to the actor's handler port are rejected.
        let err = handle.port::<()>().try_post(&client, ()).unwrap_err();
        assert_matches!(err.kind(), crate::mailbox::MailboxSenderErrorKind::Closed);

        release_stop.notify_one();
        assert_matches!(handle.await, ActorStatus::Stopped(reason) if reason == "test");
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_parent_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        let (_reported, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let root_1 = TestActor::spawn_child(&client, &root).await;
        let root_2 = TestActor::spawn_child(&client, &root).await;
        let root_2_1 = TestActor::spawn_child(&client, &root_2).await;

        root_2.post(
            &client,
            TestActorMessage::Fail(anyhow::anyhow!("some random failure")),
        );
        let _root_2_actor_id = root_2.actor_addr().clone();
        assert_matches!(
            root_2.await,
            ActorStatus::Failed(err) if err.to_string() == "some random failure"
        );

        // TODO: should we provide finer-grained stop reasons, e.g., to indicate it was
        // stopped by a parent failure?
        // Currently the parent fails with an error related to the child's failure.
        assert_matches!(
            root.await,
            ActorStatus::Failed(err) if err.to_string().contains("some random failure")
        );
        assert_matches!(root_2_1.await, ActorStatus::Stopped(_));
        assert_matches!(root_1.await, ActorStatus::Stopped(_));
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_multi_handler() {
        // TEMPORARY: This test is currently a bit awkward since we don't yet expose
        // public interfaces to multi-handlers. This will be fixed shortly.

        #[derive(Debug)]
        struct TestActor(Arc<AtomicUsize>);

        #[async_trait]
        impl Actor for TestActor {}

        #[async_trait]
        impl Handler<OncePortHandle<PortHandle<usize>>> for TestActor {
            async fn handle(
                &mut self,
                cx: &crate::Context<Self>,
                message: OncePortHandle<PortHandle<usize>>,
            ) -> anyhow::Result<()> {
                message.post(cx, cx.port());
                Ok(())
            }
        }

        #[async_trait]
        impl Handler<usize> for TestActor {
            async fn handle(
                &mut self,
                _cx: &crate::Context<Self>,
                message: usize,
            ) -> anyhow::Result<()> {
                self.0.fetch_add(message, Ordering::SeqCst);
                Ok(())
            }
        }

        let proc = Proc::isolated();
        let state = Arc::new(AtomicUsize::new(0));
        let actor = TestActor(state.clone());
        let handle = proc.spawn(actor);
        let client = proc.client("client");
        let (tx, rx) = client.open_once_port();
        handle.post(&client, tx);
        let usize_handle = rx.recv().await.unwrap();
        usize_handle.post(&client, 123);

        handle.drain_and_stop("test").unwrap();
        handle.await;

        assert_eq!(state.load(Ordering::SeqCst), 123);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_post_after_self_message() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let (ready, ready_rx) = client.open_once_port();
        let (fired, fired_rx) = client.open_once_port();
        let delay = Duration::from_millis(50);
        let start = tokio::time::Instant::now();
        let handle = proc.spawn(DelayedSelfActor {
            ready: Some(ready.bind()),
            fired: Some(fired.bind()),
            delay,
        });

        ready_rx.recv().await.unwrap();
        fired_rx.recv().await.unwrap();

        assert!(start.elapsed() >= delay);
        handle.drain_and_stop("test").unwrap();
        handle.await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_post_after_port_ref() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let (reply, mut reply_rx) = client.open_port();
        let delay = Duration::from_millis(50);
        let start = tokio::time::Instant::now();
        let handle = proc.spawn(DelayedPortActor {
            reply: Some(reply.bind()),
            delay,
        });

        assert_eq!(reply_rx.recv().await.unwrap(), 123);
        assert!(start.elapsed() >= delay);
        handle.drain_and_stop("test").unwrap();
        handle.await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_post_after_discards_pending_messages_on_shutdown() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let (ready, ready_rx) = client.open_once_port();
        let (fired, fired_rx) = client.open_once_port();
        let handle = proc.spawn(DelayedSelfActor {
            ready: Some(ready.bind()),
            fired: Some(fired.bind()),
            delay: Duration::from_secs(60),
        });

        ready_rx.recv().await.unwrap();
        handle.drain_and_stop("test").unwrap();
        assert_matches!(handle.await, ActorStatus::Stopped(reason) if reason == "test");

        let result = tokio::time::timeout(Duration::from_millis(100), fired_rx.recv()).await;
        assert!(!matches!(result, Ok(Ok(()))));
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_actor_panic() {
        // Need this custom hook to store panic backtrace in task_local.
        panic_handler::set_panic_hook();

        let proc = Proc::isolated();
        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        let (_reported, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let client = proc.client("client");
        let actor_handle = proc.spawn(TestActor);
        actor_handle
            .panic(&client, "some random failure".to_string())
            .await
            .unwrap();
        let actor_status = actor_handle.await;

        // Note: even when the test passes, the panic stacktrace will still be
        // printed to stderr because that is the behavior controlled by the panic
        // hook.
        assert_matches!(actor_status, ActorStatus::Failed(_));
        if let ActorStatus::Failed(err) = actor_status {
            let error_msg = err.to_string();
            // Verify panic message is captured
            assert!(error_msg.contains("some random failure"));
            // Verify backtrace is captured. Note the backtrace message might
            // change in the future. If that happens, we need to update this
            // statement with something up-to-date.
            assert!(error_msg.contains("library/std/src/panicking.rs"));
        }
    }

    #[cfg_attr(not(target_os = "linux"), ignore = "linux-only")]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_local_supervision_propagation() {
        hyperactor_telemetry::initialize_logging_for_test();

        #[derive(Debug)]
        struct TestActor {
            handled: Arc<AtomicBool>,
            notify: Arc<tokio::sync::Notify>,
            should_handle: bool,
        }

        #[async_trait]
        impl Actor for TestActor {
            async fn handle_supervision_event(
                &mut self,
                _this: &Instance<Self>,
                _event: &ActorSupervisionEvent,
            ) -> Result<bool, anyhow::Error> {
                if !self.should_handle {
                    return Ok(false);
                }

                tracing::error!(
                    "{}: supervision event received: {:?}",
                    _this.self_addr(),
                    _event
                );
                self.handled.store(true, Ordering::SeqCst);
                self.notify.notify_one();
                Ok(true)
            }
        }

        #[async_trait]
        impl Handler<String> for TestActor {
            async fn handle(
                &mut self,
                cx: &crate::Context<Self>,
                message: String,
            ) -> anyhow::Result<()> {
                tracing::info!("{} received message: {}", cx.self_addr(), message);
                Err(anyhow::anyhow!(message))
            }
        }

        let make_actor = |handled: &Arc<AtomicBool>, should_handle: bool| TestActor {
            handled: handled.clone(),
            notify: Arc::new(tokio::sync::Notify::new()),
            should_handle,
        };

        let proc = Proc::isolated();
        let client = proc.client("client");
        let (mut reported_event, _coordinator) =
            ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let root_state = Arc::new(AtomicBool::new(false));
        let root_1_state = Arc::new(AtomicBool::new(false));
        let root_1_notify = Arc::new(tokio::sync::Notify::new());
        let root_1_1_state = Arc::new(AtomicBool::new(false));
        let root_1_1_1_state = Arc::new(AtomicBool::new(false));
        let root_2_state = Arc::new(AtomicBool::new(false));
        let root_2_1_state = Arc::new(AtomicBool::new(false));

        let root = proc.spawn_with_label::<TestActor>("root", make_actor(&root_state, false));
        let root_1 = proc.spawn_child::<TestActor>(
            root.cell().clone(),
            TestActor {
                handled: root_1_state.clone(),
                notify: root_1_notify.clone(),
                should_handle: true, // children's event stops here
            },
        );
        let root_1_1 = proc
            .spawn_child::<TestActor>(root_1.cell().clone(), make_actor(&root_1_1_state, false));
        let root_1_1_1 = proc.spawn_child::<TestActor>(
            root_1_1.cell().clone(),
            make_actor(&root_1_1_1_state, false),
        );
        let root_2 =
            proc.spawn_child::<TestActor>(root.cell().clone(), make_actor(&root_2_state, false));
        let root_2_1 = proc
            .spawn_child::<TestActor>(root_2.cell().clone(), make_actor(&root_2_1_state, false));

        // fail `root_1_1_1`, the supervision msg should be propagated to
        // `root_1` because `root_1` has set `true` to `handle_supervision_event`.
        root_1_1_1.post(&client, "some random failure".to_string());

        // fail `root_2_1`, the supervision msg should be propagated to
        // ProcSupervisionCoordinator.
        let root_2_1_id = root_2_1.actor_addr().clone();
        root_2_1.post(&client, "some random failure".to_string());

        // Wait for root_1 to handle the supervision event from the
        // root_1_1_1 -> root_1_1 -> root_1 chain. The Notify provides
        // a deterministic signal — no polling or timing needed.
        root_1_notify.notified().await;

        // Wait for the supervision event from root_2_1's failure to
        // reach the ProcSupervisionCoordinator.
        let event = reported_event.recv().await;
        assert_eq!(event.actor_id, root_2_1_id);

        assert!(!root_state.load(Ordering::SeqCst));
        assert!(root_1_state.load(Ordering::SeqCst));
        assert!(!root_1_1_state.load(Ordering::SeqCst));
        assert!(!root_1_1_1_state.load(Ordering::SeqCst));
        assert!(!root_2_state.load(Ordering::SeqCst));
        assert!(!root_2_1_state.load(Ordering::SeqCst));
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_instance() {
        #[derive(Debug, Default)]
        struct TestActor;

        impl Actor for TestActor {}

        #[async_trait]
        impl Handler<(String, PortRef<String>)> for TestActor {
            async fn handle(
                &mut self,
                cx: &crate::Context<Self>,
                (message, port): (String, PortRef<String>),
            ) -> anyhow::Result<()> {
                port.post(cx, message);
                Ok(())
            }
        }

        let proc = Proc::isolated();

        let client = proc.client("my_test_actor");
        let status = client.status();

        let child_actor = client.spawn(TestActor);

        let (port, mut receiver) = client.open_port();
        child_actor.post(&client, ("hello".to_string(), port.bind()));

        let message = receiver.recv().await.unwrap();
        assert_eq!(message, "hello");

        child_actor.drain_and_stop("test").unwrap();
        child_actor.await;

        assert_eq!(*status.borrow(), ActorStatus::Client);
        drop(client);
        assert_matches!(*status.borrow(), ActorStatus::Stopped(_));
    }

    // Tokio's I/O driver is not fork-safe on macOS, and this test intentionally
    // validates process termination by forking without a coordinator.
    #[cfg_attr(target_os = "macos", ignore = "tokio runtime fork assertion on macOS")]
    #[tokio::test]
    async fn test_proc_terminate_without_coordinator() {
        if std::env::var("CARGO_TEST").is_ok() {
            eprintln!("test skipped as it hangs when run by cargo in sandcastle");
            return;
        }

        let process = async {
            let proc = Proc::isolated();
            // Intentionally not setting a proc supervison coordinator. This
            // should cause the process to terminate.
            // ProcSupervisionCoordinator::set(&proc).await.unwrap();
            let root = proc.spawn_with_label("root", TestActor);
            let client = proc.client("client");
            root.fail(&client, anyhow::anyhow!("some random failure"))
                .await
                .unwrap();
            // It is okay to sleep a long time here, because we expect this
            // process to be terminated way before the sleep ends due to the
            // missing proc supervison coordinator.
            tokio::time::sleep(Duration::from_secs(30)).await;
        };

        assert_termination(|| process, 1).await.unwrap();
    }

    fn trace_and_block(fut: impl Future) {
        tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(hyperactor_telemetry::recorder().layer()),
            || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(fut)
            },
        );
    }

    #[ignore = "until trace recording is turned back on"]
    #[test]
    fn test_handler_logging() {
        #[derive(Debug, Default)]
        struct LoggingActor;

        impl Actor for LoggingActor {}

        impl LoggingActor {
            async fn wait(cx: &impl context::Actor, handle: &ActorHandle<Self>) {
                let barrier = Arc::new(Barrier::new(2));
                handle.post(cx, barrier.clone());
                barrier.wait().await;
            }
        }

        #[async_trait]
        impl Handler<String> for LoggingActor {
            async fn handle(
                &mut self,
                _cx: &crate::Context<Self>,
                message: String,
            ) -> anyhow::Result<()> {
                tracing::info!("{}", message);
                Ok(())
            }
        }

        #[async_trait]
        impl Handler<u64> for LoggingActor {
            async fn handle(
                &mut self,
                _cx: &crate::Context<Self>,
                message: u64,
            ) -> anyhow::Result<()> {
                tracing::event!(Level::INFO, number = message);
                Ok(())
            }
        }

        #[async_trait]
        impl Handler<Arc<Barrier>> for LoggingActor {
            async fn handle(
                &mut self,
                _cx: &crate::Context<Self>,
                message: Arc<Barrier>,
            ) -> anyhow::Result<()> {
                message.wait().await;
                Ok(())
            }
        }

        #[async_trait]
        impl Handler<Arc<(Barrier, Barrier)>> for LoggingActor {
            #[expect(
                clippy::await_holding_invalid_type,
                reason = "tracing_test::traced_test macro expansion holds tracing::span::Entered across awaits; can't be fixed in our code"
            )]
            async fn handle(
                &mut self,
                _cx: &crate::Context<Self>,
                barriers: Arc<(Barrier, Barrier)>,
            ) -> anyhow::Result<()> {
                let inner = tracing::span!(Level::INFO, "child_span");
                let _inner_guard = inner.enter();
                barriers.0.wait().await;
                barriers.1.wait().await;
                Ok(())
            }
        }

        trace_and_block(async {
            let proc = Proc::isolated();
            let client = proc.client("client");
            let handle = hyperactor::spawn(LoggingActor).into_guard();
            handle.post(&client, "hello world".to_string());
            handle.post(&client, "hello world again".to_string());
            handle.post(&client, 123u64);

            LoggingActor::wait(&client, &handle).await;

            let events = handle.cell().inner.recording.tail();
            assert_eq!(events.len(), 3);
            assert_eq!(events[0].json_value(), json!({ "message": "hello world" }));
            assert_eq!(
                events[1].json_value(),
                json!({ "message": "hello world again" })
            );
            assert_eq!(events[2].json_value(), json!({ "number": 123 }));

            let stacks = {
                let barriers = Arc::new((Barrier::new(2), Barrier::new(2)));
                handle.post(&client, Arc::clone(&barriers));
                barriers.0.wait().await;
                let stacks = handle.cell().inner.recording.stacks();
                barriers.1.wait().await;
                stacks
            };
            assert_eq!(stacks.len(), 1);
            assert_eq!(stacks[0].len(), 1);
            assert_eq!(stacks[0][0].name(), "child_span");
        })
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_mailbox_closed_with_owner_stopped_reason() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let actor_handle = proc.spawn(TestActor);

        // Clone the handle before awaiting since await consumes the handle
        let handle_for_send = actor_handle.clone();

        // Stop the actor gracefully
        actor_handle.drain_and_stop("healthy shutdown").unwrap();
        actor_handle.await;

        // Try to send a message to the stopped actor
        let result = handle_for_send
            .port::<TestActorMessage>()
            .try_post(&client, TestActorMessage::Noop());

        assert!(result.is_err(), "send should fail when actor is stopped");
        let err = result.unwrap_err();
        assert_matches!(
            err.kind(),
            crate::mailbox::MailboxSenderErrorKind::Mailbox(mailbox_err)
                if matches!(
                    mailbox_err.kind(),
                    crate::mailbox::MailboxErrorKind::OwnerTerminated(ActorStatus::Stopped(reason)) if reason == "healthy shutdown"
                )
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_mailbox_closed_with_owner_failed_reason() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        let (_reported, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let actor_handle = proc.spawn(TestActor);

        // Clone the handle before awaiting since await consumes the handle
        let handle_for_send = actor_handle.clone();

        // Cause the actor to fail
        actor_handle.post(
            &client,
            TestActorMessage::Fail(anyhow::anyhow!("intentional failure")),
        );
        actor_handle.await;

        // Try to send a message to the failed actor
        let result = handle_for_send
            .port::<TestActorMessage>()
            .try_post(&client, TestActorMessage::Noop());

        assert!(result.is_err(), "send should fail when actor has failed");
        let err = result.unwrap_err();
        assert_matches!(
            err.kind(),
            crate::mailbox::MailboxSenderErrorKind::Mailbox(mailbox_err)
                if matches!(
                    mailbox_err.kind(),
                    crate::mailbox::MailboxErrorKind::OwnerTerminated(ActorStatus::Failed(ActorErrorKind::Generic(msg)))
                        if msg.contains("intentional failure")
                )
        );
    }

    /// Wait for a terminated snapshot to appear for the given actor.
    /// The introspect task runs in a separate tokio task and may not
    /// have stored the snapshot by the time `handle.await` returns.
    async fn wait_for_terminated_snapshot(
        proc: &Proc,
        actor_id: &ActorAddr,
    ) -> crate::introspect::IntrospectResult {
        // Yield to let the introspect task run, then poll. Use a
        // combination of yields (for fast paths) and sleeps (to
        // avoid busy-spinning if the scheduler is loaded).
        for i in 0..1000 {
            if let Some(snapshot) = proc.terminated_snapshot(actor_id) {
                return snapshot;
            }
            if i < 50 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        panic!("timed out waiting for terminated snapshot for {}", actor_id);
    }

    // Verifies that when an actor is stopped, the proc eventually
    // records a "terminated snapshot" for it (written by the
    // introspect task, which runs asynchronously). The test asserts
    // the snapshot is absent while the actor is live, then stops the
    // actor, waits for the introspect task to observe the terminal
    // state, and confirms:
    //   - the stored snapshot reports a `stopped:*` actor_status, and
    //   - the actor id moves from the live set to the terminated set.
    #[async_timed_test(timeout_secs = 60)]
    async fn test_terminated_snapshot_stored_on_stop() {
        let proc = Proc::isolated();
        let _client = proc.client("client");

        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();

        // Actor is live — no terminated snapshot yet.
        assert!(proc.terminated_snapshot(&actor_id).is_none());
        assert!(!proc.all_terminated_actor_ids().contains(&actor_id));

        // Stop the actor and wait for it to fully terminate.
        handle.drain_and_stop("test").unwrap();
        handle.await;

        // The introspect task runs in a separate tokio task; wait for
        // it to observe the terminal status and store the snapshot.
        let snapshot = wait_for_terminated_snapshot(&proc, &actor_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("snapshot attrs must be valid");
        let status = attrs
            .get(crate::introspect::STATUS)
            .expect("must have status");
        assert!(
            status.starts_with("stopped"),
            "expected stopped status, got: {}",
            status
        );

        // Actor should appear in terminated IDs but not in live IDs.
        assert!(proc.all_terminated_actor_ids().contains(&actor_id));
        assert!(
            !proc.all_actor_ids().contains(&actor_id),
            "stopped actor should not appear in live actor IDs"
        );
    }

    // Verifies that an actor failure results in a terminated snapshot
    // being stored. The test installs a ProcSupervisionCoordinator
    // (required for failure handling), spawns an actor, triggers a
    // failure via a message, waits for the actor to terminate, then
    // waits for the introspect task to persist the terminal snapshot
    // and asserts the snapshot reports a `failed:*` actor_status.
    #[async_timed_test(timeout_secs = 60)]
    async fn test_terminated_snapshot_stored_on_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        // Supervision coordinator required for actor failure handling.
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();

        // Trigger a failure.
        handle.post(&client, TestActorMessage::Fail(anyhow::anyhow!("boom")));
        handle.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &actor_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("snapshot attrs must be valid");
        let status = attrs
            .get(crate::introspect::STATUS)
            .expect("must have status");
        assert!(
            status.starts_with("failed"),
            "expected failed status, got: {}",
            status
        );
    }

    // Exercises FI-1/FI-2 (see introspect.rs module-scope comment).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_supervision_event_stored_on_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();
        let cell = handle.cell().clone();

        handle.post(&client, TestActorMessage::Fail(anyhow::anyhow!("boom")));
        handle.await;

        let event = cell
            .supervision_event()
            .expect("failed actor must have supervision_event");
        assert_eq!(event.actor_id, actor_id);
        assert!(event.actor_status.is_failed());
        // Originated here, not propagated.
        assert_eq!(event.actually_failing_actor().unwrap().actor_id, actor_id);
    }

    // Exercises FI-2 (see introspect.rs module-scope comment).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_supervision_event_on_clean_stop() {
        let proc = Proc::isolated();
        let _client = proc.client("client");

        let handle = proc.spawn(TestActor);
        let cell = handle.cell().clone();

        handle.drain_and_stop("test").unwrap();
        handle.await;

        let event = cell
            .supervision_event()
            .expect("clean stop must store supervision event");
        assert!(
            matches!(event.actor_status, ActorStatus::Stopped(_)),
            "expected Stopped status, got {:?}",
            event.actor_status
        );
        assert!(!event.is_error());
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_supervision_coordinator_receives_clean_stop() {
        let proc = Proc::isolated();
        let _client = proc.client("client");
        let (mut reported_event, _coordinator_handle) =
            ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();

        handle.drain_and_stop("test").unwrap();
        handle.await;

        let event = reported_event.recv().await;
        assert_eq!(event.actor_id, actor_id);
        assert!(
            matches!(event.actor_status, ActorStatus::Stopped(_)),
            "expected Stopped status, got {:?}",
            event.actor_status
        );
        assert!(!event.is_error());
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_coordinator_shuts_down_last_during_destroy() {
        let mut proc = Proc::isolated();
        let _client = proc.client("client");
        let (mut reported_event, _coordinator_handle) =
            ProcSupervisionCoordinator::set(&proc).await.unwrap();

        // Spawn several actors that will all stop during destroy_and_wait.
        let mut actor_ids = Vec::new();
        for i in 0..3 {
            let handle = proc.spawn_with_label::<TestActor>(&format!("actor_{i}"), TestActor);
            actor_ids.push(handle.actor_addr().clone());
        }

        // destroy_and_wait stops all actors. If the coordinator were stopped
        // simultaneously, supervision event delivery would fail and crash
        // the process. The fact that this completes without crashing proves
        // the coordinator outlived the other actors.
        proc.destroy_and_wait(Duration::from_secs(5), "test")
            .await
            .unwrap();

        // Verify the coordinator received stop events from all three actors.
        let mut received_ids = Vec::new();
        for _ in 0..actor_ids.len() {
            let event = reported_event.recv().await;
            assert!(
                matches!(event.actor_status, ActorStatus::Stopped(_)),
                "expected Stopped, got {:?}",
                event.actor_status
            );
            received_ids.push(event.actor_id);
        }
        received_ids.sort();
        actor_ids.sort();
        assert_eq!(received_ids, actor_ids);
    }

    // Exercises FI-4 (see introspect.rs module-scope comment).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_supervision_event_on_propagated_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let parent = proc.spawn_with_label::<TestActor>("parent", TestActor);
        let parent_cell = parent.cell().clone();
        // Spawn child under parent.
        let (tx, rx) = oneshot::channel();
        parent.post(&client, TestActorMessage::Spawn(tx));
        let child = rx.await.unwrap();
        let child_id = child.actor_addr().clone();

        // Fail the child — parent doesn't handle supervision, so it
        // propagates and terminates too.
        child.post(
            &client,
            TestActorMessage::Fail(anyhow::anyhow!("child boom")),
        );
        parent.await;

        let event = parent_cell.supervision_event();
        assert!(
            event.is_some(),
            "parent must have supervision_event from propagated failure"
        );
        let event = event.unwrap();
        // Root cause is the child, not the parent.
        assert_eq!(event.actually_failing_actor().unwrap().actor_id, child_id);
    }

    // Exercises S11 (see introspect.rs module doc).
    //
    // A live actor is resolvable. After drain_and_stop + await, the
    // actor's status is terminal and resolve_actor_ref must return
    // None — even though the introspect task may still hold a strong
    // InstanceCell Arc (it drops the Arc only after observing
    // terminal status asynchronously). The is_terminal() check in
    // resolve_actor_ref closes that race window.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_resolve_actor_ref_none_for_terminal_actor() {
        let proc = Proc::isolated();
        let _client = proc.client("client");

        let handle = proc.spawn(TestActor);
        let actor_ref: ActorRef<TestActor> = handle.bind();

        // Actor is live — resolve should succeed.
        assert!(
            proc.resolve_actor_ref(&actor_ref).is_some(),
            "live actor should be resolvable"
        );

        handle.drain_and_stop("test").unwrap();
        handle.await;

        // Actor is terminal — resolve must return None regardless of
        // whether the introspect task has dropped its Arc yet.
        assert!(
            proc.resolve_actor_ref(&actor_ref).is_none(),
            "terminal actor must not be resolvable"
        );
    }

    // Exercises FI-3 (see introspect module doc).
    #[async_timed_test(timeout_secs = 60)]
    async fn test_terminated_snapshot_has_failure_info() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();

        handle.post(&client, TestActorMessage::Fail(anyhow::anyhow!("kaboom")));
        handle.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &actor_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("snapshot attrs must be valid");
        let status = attrs
            .get(crate::introspect::STATUS)
            .expect("must have status");
        assert!(
            status.starts_with("failed"),
            "expected failed status, got: {}",
            status
        );
        let err_msg = attrs
            .get(crate::introspect::FAILURE_ERROR_MESSAGE)
            .expect("failed actor must have failure_error_message");
        assert!(!err_msg.is_empty());
        let root_cause = attrs
            .get(crate::introspect::FAILURE_ROOT_CAUSE_ACTOR)
            .expect("must have root_cause_actor");
        assert_eq!(root_cause, &actor_id);
        assert_eq!(
            attrs.get(crate::introspect::FAILURE_IS_PROPAGATED),
            Some(&false)
        );
        assert!(
            attrs.get(crate::introspect::FAILURE_OCCURRED_AT).is_some(),
            "failed actor must have occurred_at"
        );
    }

    // Exercises FI-4 (see introspect module doc).
    #[async_timed_test(timeout_secs = 60)]
    async fn test_propagated_failure_info() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let parent = proc.spawn_with_label::<TestActor>("parent", TestActor);
        let parent_id = parent.actor_addr().clone();

        let (tx, rx) = oneshot::channel();
        parent.post(&client, TestActorMessage::Spawn(tx));
        let child = rx.await.unwrap();
        let child_id = child.actor_addr().clone();

        child.post(
            &client,
            TestActorMessage::Fail(anyhow::anyhow!("child fail")),
        );
        parent.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &parent_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("snapshot attrs must be valid");
        let root_cause = attrs
            .get(crate::introspect::FAILURE_ROOT_CAUSE_ACTOR)
            .expect("propagated failure must have root_cause_actor");
        assert_eq!(root_cause, &child_id);
        assert_eq!(
            attrs.get(crate::introspect::FAILURE_IS_PROPAGATED),
            Some(&true)
        );
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_creates_descriptive_name() {
        let proc = Proc::isolated();
        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let handle = proc.spawn_named_child(root.cell().clone(), "my_controller", TestActor);
        assert_eq!(
            handle.actor_addr().label().unwrap().as_str(),
            "my_controller"
        );
        assert!(!handle.actor_addr().is_root());
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_increments_index() {
        let proc = Proc::isolated();
        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let first = proc.spawn_named_child(root.cell().clone(), "my_controller", TestActor);
        let second = proc.spawn_named_child(root.cell().clone(), "my_controller", TestActor);
        assert_ne!(first.actor_addr().uid(), second.actor_addr().uid());
    }

    /// Exercises AI-1 (see module doc).
    /// spawn_named_child passes Some(parent) to spawn_inner.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_preserves_supervision() {
        let proc = Proc::isolated();
        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let child = proc.spawn_named_child(root.cell().clone(), "supervised_child", TestActor);
        let child_cell = child.cell();
        let parent = child_cell.parent().expect("child must have parent");
        assert_eq!(parent.actor_addr(), root.actor_addr());
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_unchanged() {
        let proc = Proc::isolated();
        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let child = proc.spawn_child(root.cell().clone(), TestActor);
        assert!(!child.actor_addr().is_root());
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_different_names_different_pids() {
        let proc = Proc::isolated();
        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let a = proc.spawn_named_child(root.cell().clone(), "controller_a", TestActor);
        let b = proc.spawn_named_child(root.cell().clone(), "controller_b", TestActor);
        assert_ne!(a.actor_addr().uid(), b.actor_addr().uid());
        assert_eq!(a.actor_addr().label().unwrap().as_str(), "controller_a");
        assert_eq!(b.actor_addr().label().unwrap().as_str(), "controller_b");
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_no_child_overwrite() {
        let proc = Proc::isolated();
        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let _a = proc.spawn_named_child(root.cell().clone(), "ctrl", TestActor);
        let _b = proc.spawn_named_child(root.cell().clone(), "ctrl", TestActor);
        let _c = proc.spawn_child(root.cell().clone(), TestActor);
        assert_eq!(root.cell().child_count(), 3);
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_does_not_pollute_roots() {
        let proc = Proc::isolated();
        let root = proc.spawn_with_label::<TestActor>("root", TestActor);
        let _child = proc.spawn_named_child(root.cell().clone(), "foo", TestActor);
        // "foo" was used as a named child name but should NOT
        // prevent spawning a root actor with that name.
        let _root = proc.spawn_with_label::<TestActor>("foo", TestActor);
    }

    /// Exercises AI-3 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ai3_controller_actor_ids_unique_across_parents_same_proc() {
        let proc = Proc::isolated();
        let parent_a = proc.spawn_with_label::<TestActor>("parent_a", TestActor);
        let parent_b = proc.spawn_with_label::<TestActor>("parent_b", TestActor);

        // Simulate the correct pattern: include mesh identity in name.
        let ctrl_a =
            proc.spawn_named_child(parent_a.cell().clone(), "controller_mesh_a", TestActor);
        let ctrl_b =
            proc.spawn_named_child(parent_b.cell().clone(), "controller_mesh_b", TestActor);

        assert_ne!(
            ctrl_a.actor_addr(),
            ctrl_b.actor_addr(),
            "controller ActorAddrs must be unique across parents"
        );
    }

    /// Exercises AI-3 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ai3_no_controller_overwrite_in_parent_or_proc_maps() {
        let proc = Proc::isolated();
        let parent_a = proc.spawn_with_label::<TestActor>("parent_a", TestActor);
        let parent_b = proc.spawn_with_label::<TestActor>("parent_b", TestActor);

        let ctrl_a =
            proc.spawn_named_child(parent_a.cell().clone(), "controller_mesh_a", TestActor);
        let ctrl_b =
            proc.spawn_named_child(parent_b.cell().clone(), "controller_mesh_b", TestActor);

        // Both must be independently resolvable via the proc's instances.
        assert!(
            proc.get_instance(ctrl_a.actor_addr()).is_some(),
            "ctrl_a must be resolvable"
        );
        assert!(
            proc.get_instance(ctrl_b.actor_addr()).is_some(),
            "ctrl_b must be resolvable"
        );
        // Parents each see exactly one child.
        assert_eq!(parent_a.cell().child_count(), 1);
        assert_eq!(parent_b.cell().child_count(), 1);
    }

    // Exercises FI-6 (see introspect module doc).
    #[async_timed_test(timeout_secs = 60)]
    async fn test_stopped_snapshot_has_no_failure_info() {
        let proc = Proc::isolated();
        let _client = proc.client("client");

        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();

        handle.drain_and_stop("test").unwrap();
        handle.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &actor_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("snapshot attrs must be valid");
        let status = attrs
            .get(crate::introspect::STATUS)
            .expect("must have status");
        assert!(
            status.starts_with("stopped"),
            "expected stopped, got: {}",
            status
        );
        assert!(
            attrs
                .get(crate::introspect::FAILURE_ERROR_MESSAGE)
                .is_none(),
            "stopped actor must not have failure attrs"
        );
    }

    // ── PD-5: queue depth accounting ────────────────────────────

    // PD-5b/PD-5c: queue depth increments on enqueue, decrements on
    // dequeue, and returns to zero after the message is handled. This
    // tests that the introspection-readable queue_depth is aligned
    // with the existing OTel ACTOR_MESSAGE_QUEUE_SIZE accounting.
    #[async_timed_test(timeout_secs = 10)]
    async fn test_queue_depth_increment_decrement() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn_with_label("qd_test", TestActor);
        let actor_ref: crate::ActorRef<TestActor> = handle.bind();
        let actor_id = actor_ref.actor_addr().clone();

        // Before any message: queue depth should be 0.
        let cell = proc.get_instance(&actor_id).expect("actor must exist");
        assert_eq!(cell.queue_depth(), 0, "initial queue depth should be 0");

        // Send a message that blocks until we signal it. This lets
        // us observe queue depth > 0 while the actor is busy.
        let (reply_tx, reply_rx) = oneshot::channel();
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        handle.wait(&client, reply_tx, gate_rx).await.unwrap();

        // Wait for the actor to start processing (it sends reply_tx).
        reply_rx.await.unwrap();

        // Now send a second message — it should be queued.
        let (reply2_tx, reply2_rx) = oneshot::channel();
        handle.reply(&client, reply2_tx).await.unwrap();

        // Give the enqueue a moment to propagate.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Queue depth should be >= 1 (the Reply message is queued).
        let depth = cell.queue_depth();
        assert!(
            depth >= 1,
            "expected queue depth >= 1 while actor is busy, got {depth}"
        );

        // Unblock the first message.
        let _ = gate_tx.send(());

        // Wait for the second message to be handled.
        reply2_rx.await.unwrap();

        // Give the dequeue a moment to propagate.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Queue depth should return to 0.
        let depth = cell.queue_depth();
        assert_eq!(
            depth, 0,
            "queue depth should return to 0 after all messages handled"
        );
    }

    // Test-only Named message + dedicated actor with explicit handler
    // export, so the integration test can bind the BufferTestMsg
    // handler port (`handle.bind()`) and drive ordered traffic through
    // `OrderedSender`. Without the bind, `PortHandle::try_post` stamps
    // `SeqInfo::Direct` and the enqueue closure takes the `direct_send`
    // branch -- which never populates `OrderedSender::states`, leaving
    // the snapshot empty.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, typeuri::Named)]
    struct BufferTestMsg;

    #[derive(Debug, Default)]
    #[hyperactor::export(handlers = [BufferTestMsg])]
    struct BufferTestActor;

    #[async_trait]
    impl Actor for BufferTestActor {}

    #[async_trait]
    impl Handler<BufferTestMsg> for BufferTestActor {
        async fn handle(
            &mut self,
            _cx: &crate::Context<Self>,
            _msg: BufferTestMsg,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Exercises IO-1 ("active" branch: snapshot is `Some({enabled:
    /// true, ...})`), IO-2 (publish-time state via `try_lock` populates
    /// the session's buffered fields), and IO-3 (asserts `queue_depth`
    /// is a propagated `u64` but makes NO arithmetic claim relating it
    /// to `buffered_count`). End-to-end wiring proof: `Instance::new`
    /// installs the snapshot callback, `InstanceCell::inbound_ordering_snapshot()`
    /// invokes it, the resulting snapshot includes the buffered session
    /// with its sender, `expected_next_seq`, and `buffered_count`, AND
    /// `build_actor_attrs` (via `live_actor_payload`) publishes
    /// `INBOUND_ORDERING` so it round-trips through `ActorAttrsView`.
    /// The attrs-publish assertion catches accidental removal of the
    /// `attrs.set(INBOUND_ORDERING, snapshot)` call site. Drives a
    /// deterministic gap via `debug_skip_next_ordering_seq` so the test
    /// is not timing-sensitive.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_inbound_ordering_snapshot_callback_publishes_session() {
        use typeuri::Named;

        // Pin reorder buffering ON regardless of any global config
        // overrides set by other tests in the same binary. Without this
        // guard the snapshot would observe `enabled = false` and the
        // assertions below would fail when tests run in interleaved
        // order.
        let config = hyperactor_config::global::lock();
        let _g = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn_with_label("a", BufferTestActor);
        let actor_id = handle.actor_addr().clone();

        // Bind the handler ports so `handle.post` uses `SeqInfo::Session`
        // (not `SeqInfo::Direct`). This is the equivalent of asking
        // for a typed actor reference; for our purposes we only need
        // the side effect of binding handler ports.
        let _actor_ref: crate::ActorRef<BufferTestActor> = handle.bind();

        // Reserve seq 1 on the actor's BufferTestMsg handler port so
        // subsequent client posts get seqs 2..=N which then buffer
        // (waiting for seq 1, which will never arrive).
        let handler_port = actor_id.port_addr(Port::from(BufferTestMsg::port()));
        // Reserve one seq directly on the client's sequencer. Equivalent
        // to `Instance::debug_skip_next_ordering_seq(dest, 1)`, but
        // inlined here so the test does not depend on a Client-side
        // convenience method.
        let _ = client.sequencer().assign_seq(&handler_port);

        // Post three messages. These flow through MailboxExt::post ->
        // HandlerPorts enqueue closure -> OrderedSender::send, where
        // they buffer (out of order from seq 1's perspective).
        for _ in 0..3 {
            handle.post(&client, BufferTestMsg);
        }
        // Let the enqueues propagate to the buffer.
        tokio::task::yield_now().await;

        // Direct accessor: cell.inbound_ordering_snapshot() invokes the
        // type-erased callback installed at Instance::new.
        let cell = proc.get_instance(&actor_id).expect("actor exists");
        let snapshot = cell
            .inbound_ordering_snapshot()
            .expect("snapshot callback should be installed for live actors");

        assert!(snapshot.enabled, "buffering enabled by override");
        assert_eq!(snapshot.sessions.len(), 1, "one client session expected");
        let session = &snapshot.sessions[0];
        assert_eq!(session.expected_next_seq, 1);
        assert_eq!(session.buffered_count, 3);
        assert_eq!(session.oldest_buffered_seq, Some(2));
        assert_eq!(session.newest_buffered_seq, Some(4));
        assert_eq!(
            session.sender.as_ref(),
            Some(client.mailbox().actor_addr()),
            "session owner should be the posting client",
        );

        // Attrs-publish wiring: build_actor_attrs (via live_actor_payload)
        // must call attrs.set(INBOUND_ORDERING, snapshot). Round-trip
        // through ActorAttrsView and assert the same session.
        let payload = crate::introspect::live_actor_payload(&cell);
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&payload.attrs).expect("payload.attrs is well-formed JSON");
        let view = crate::introspect::ActorAttrsView::from_attrs(&attrs)
            .expect("attrs decode through ActorAttrsView");
        let view_snapshot = view
            .inbound_ordering
            .as_ref()
            .expect("INBOUND_ORDERING attr should be set by build_actor_attrs");
        assert_eq!(view_snapshot, &snapshot);

        // queue_depth scalar is propagated (no arithmetic relation to
        // buffered_count asserted; IO-3 in introspect module doc).
        let _: u64 = cell.queue_depth();
        let _: u64 = view.queue_depth;
    }

    // PD-4/PD-5: proc-level queue pressure aggregation reports
    // non-zero under induced load. Queue depth is an instantaneous
    // snapshot of currently queued work, not backlog history.
    #[async_timed_test(timeout_secs = 10)]
    async fn test_proc_queue_depth_aggregation_under_pressure() {
        let proc = Proc::isolated();
        let client = proc.client("client");

        // Spawn two actors.
        let h1 = proc.spawn_with_label("a1", TestActor);
        let h2 = proc.spawn_with_label("a2", TestActor);

        // Block both actors with a Wait message.
        let (reply1, rx1) = oneshot::channel();
        let (gate1, grx1) = oneshot::channel::<()>();
        h1.wait(&client, reply1, grx1).await.unwrap();
        rx1.await.unwrap();

        let (reply2, rx2) = oneshot::channel();
        let (gate2, grx2) = oneshot::channel::<()>();
        h2.wait(&client, reply2, grx2).await.unwrap();
        rx2.await.unwrap();

        // Queue additional messages while actors are blocked.
        h1.noop(&client).await.unwrap();
        h1.noop(&client).await.unwrap();
        h2.noop(&client).await.unwrap();

        // Poll until aggregated queue depth reaches the expected
        // level, with a bounded timeout to avoid flakes.
        let aggregate = || -> (u64, u64) {
            let mut total: u64 = 0;
            let mut max: u64 = 0;
            for actor_id in proc.all_instance_keys() {
                if let Some(cell) = proc.get_instance_by_id(&actor_id) {
                    let depth = cell.queue_depth();
                    total = total.saturating_add(depth);
                    max = max.max(depth);
                }
            }
            (total, max)
        };

        // Same aggregation logic used by
        // ProcAgent::publish_introspect_properties.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let (total, max) = aggregate();
            if total >= 3 {
                assert!(max >= 1, "expected max >= 1, got {max}");
                assert!(max <= total, "PD-1: max ({max}) <= total ({total})");
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for queue depth >= 3, got {total}",
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Unblock both actors.
        let _ = gate1.send(());
        let _ = gate2.send(());

        // Poll until aggregated depth returns to 0.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let (total, _) = aggregate();
            if total == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for queue depth to return to 0, got {total}",
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // ── PD-6 through PD-9: retained queue-pressure evidence ───

    // PD-7: cold start — no queue traffic means last-nonzero is None
    // and watermark is 0.
    #[async_timed_test(timeout_secs = 5)]
    async fn test_retained_queue_stats_cold_start() {
        let proc = Proc::isolated();
        assert_eq!(proc.queue_depth_total(), 0);
        assert_eq!(proc.queue_depth_high_water_mark(), 0);
        assert_eq!(proc.last_nonzero_queue_depth_age_ms(), None);
    }

    // PD-6/PD-8: after induced pressure drains, high-water mark
    // retains the peak and last-nonzero is Some.
    #[async_timed_test(timeout_secs = 10)]
    async fn test_retained_queue_stats_burst_then_drain() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let h = proc.spawn_with_label("ret_test", TestActor);

        // Block the actor.
        let (ready_tx, ready_rx) = oneshot::channel();
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        h.wait(&client, ready_tx, gate_rx).await.unwrap();
        ready_rx.await.unwrap();

        // Queue work behind it.
        h.noop(&client).await.unwrap();
        h.noop(&client).await.unwrap();

        // Poll until watermark is updated.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let hwm = proc.queue_depth_high_water_mark();
            if hwm >= 2 {
                // PD-6: watermark >= current total.
                assert!(hwm >= proc.queue_depth_total());
                // Active pressure: last-nonzero should be near zero.
                let age = proc.last_nonzero_queue_depth_age_ms();
                assert!(
                    age.is_some(),
                    "last-nonzero should be Some while pressure is active"
                );
                assert!(age.unwrap() < 2000, "last-nonzero age should be near zero");
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for watermark >= 2",
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Unblock and drain.
        let _ = gate_tx.send(());
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if proc.queue_depth_total() == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for total to drain",
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // PD-8: watermark retained after drain.
        assert!(
            proc.queue_depth_high_water_mark() >= 2,
            "watermark should retain the peak after drain",
        );

        // PD-7: last-nonzero is Some (not None) after pressure.
        let age = proc.last_nonzero_queue_depth_age_ms();
        assert!(age.is_some(), "last-nonzero should be Some after pressure");
    }

    // PD-7: deterministic test of dequeue-side timestamp refresh
    // using a fake clock. Proves "last observed non-zero" semantics
    // without timing-dependent sleeps.
    #[test]
    fn test_last_nonzero_refreshed_on_dequeue_deterministic() {
        use std::sync::atomic::AtomicU64;

        static FAKE_NOW: AtomicU64 = AtomicU64::new(0);
        fn fake_clock() -> u64 {
            FAKE_NOW.load(Ordering::Relaxed)
        }

        let stats = ProcQueueStats::with_clock(fake_clock);
        let depth = Arc::new(AtomicU64::new(0));

        // Cold start: no activity.
        assert_eq!(stats.last_nonzero_age_ms(), None);

        // t=1000: enqueue two items.
        FAKE_NOW.store(1000, Ordering::Relaxed);
        account_enqueue(&depth, &stats, "a");
        account_enqueue(&depth, &stats, "a");
        assert_eq!(stats.running_total(), 2);
        assert_eq!(stats.high_water_mark(), 2);

        // t=2000: read age — should be 1000ms since last nonzero.
        FAKE_NOW.store(2000, Ordering::Relaxed);
        assert_eq!(stats.last_nonzero_age_ms(), Some(1000));

        // t=3000: dequeue one item. Queue still non-zero (1 left).
        // This should refresh the timestamp to 3000.
        FAKE_NOW.store(3000, Ordering::Relaxed);
        account_dequeue(&depth, &stats, "a");
        assert_eq!(stats.running_total(), 1);

        // t=4000: read age — should be 1000ms (4000 - 3000), not
        // 3000ms (4000 - 1000). This proves the dequeue refreshed
        // the timestamp.
        FAKE_NOW.store(4000, Ordering::Relaxed);
        assert_eq!(stats.last_nonzero_age_ms(), Some(1000));

        // t=5000: dequeue last item. Queue is now zero.
        // prev_total was 1, so prev_total > 1 is false — timestamp
        // is NOT refreshed. It stays at 3000.
        FAKE_NOW.store(5000, Ordering::Relaxed);
        account_dequeue(&depth, &stats, "a");
        assert_eq!(stats.running_total(), 0);

        // t=6000: age should be 3000ms (6000 - 3000).
        FAKE_NOW.store(6000, Ordering::Relaxed);
        assert_eq!(stats.last_nonzero_age_ms(), Some(3000));

        // Watermark retained.
        assert_eq!(stats.high_water_mark(), 2);
    }

    // account_cancel_enqueue must symmetrically reverse
    // account_enqueue on queue_depth and running_total so that a
    // send failure after accounting cannot leave the proc-wide
    // counter at u64::MAX (which would panic the next enqueue via
    // the `fetch_add(1) + 1` path).
    #[test]
    fn test_account_cancel_enqueue_restores_counters() {
        let stats = ProcQueueStats::new();
        let depth = Arc::new(AtomicU64::new(0));

        account_enqueue(&depth, &stats, "a");
        assert_eq!(stats.running_total(), 1);
        assert_eq!(depth.load(Ordering::Relaxed), 1);

        account_cancel_enqueue(&depth, &stats, "a");
        assert_eq!(
            stats.running_total(),
            0,
            "cancel must restore running_total"
        );
        assert_eq!(
            depth.load(Ordering::Relaxed),
            0,
            "cancel must restore queue_depth"
        );

        // high_water_mark is monotonic by design; cancel does not reset it.
        assert_eq!(stats.high_water_mark(), 1);

        // A subsequent enqueue must not observe underflow: fetch_add(1) + 1
        // would panic in debug builds if running_total had wrapped to u64::MAX.
        account_enqueue(&depth, &stats, "a");
        assert_eq!(stats.running_total(), 1);
    }
}
