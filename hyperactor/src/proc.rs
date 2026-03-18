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
//! - **AI-1 (named-child pid):** The pid of a named child must
//!   remain in the parent's sibling pid domain. The name is
//!   presentation only; the numeric pid is allocated from the
//!   parent's counter, preserving supervision linkage.
//! - **AI-3 (controller ActorId uniqueness):** Callers must ensure
//!   the name is unique proc-wide. Two children with the same name
//!   under different parents get distinct pids but the same name
//!   prefix.

use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::panic::Location;
use std::pin::Pin;
use std::sync::Arc;
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
use crate::Handler;
use crate::Message;
use crate::RemoteMessage;
use crate::actor::ActorError;
use crate::actor::ActorErrorKind;
use crate::actor::ActorHandle;
use crate::actor::ActorStatus;
use crate::actor::Binds;
use crate::actor::HandlerInfo;
use crate::actor::Referable;
use crate::actor::RemoteHandles;
use crate::actor::Signal;
use crate::actor_local::ActorLocalStorage;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::config;
use crate::context;
use crate::context::Mailbox as _;
use crate::introspect::IntrospectMessage;
use crate::introspect::IntrospectResult;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DeliveryError;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::Mailbox;
use crate::mailbox::MailboxMuxer;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxServer as _;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::OncePortHandle;
use crate::mailbox::OncePortReceiver;
use crate::mailbox::PanickingMailboxSender;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::mailbox::Undeliverable;
use crate::metrics::ACTOR_MESSAGE_HANDLER_DURATION;
use crate::metrics::ACTOR_MESSAGE_QUEUE_SIZE;
use crate::metrics::ACTOR_MESSAGES_RECEIVED;
use crate::ordering::OrderedSender;
use crate::ordering::OrderedSenderError;
use crate::ordering::SEQ_INFO;
use crate::ordering::SeqInfo;
use crate::ordering::Sequencer;
use crate::ordering::ordered_channel;
use crate::panic_handler;
use crate::reference;
use crate::supervision::ActorSupervisionEvent;

/// This is used to mint new local ranks for [`Proc::local`].
static NEXT_LOCAL_RANK: AtomicUsize = AtomicUsize::new(0);

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
    /// The proc's id. This should be globally unique, but is not (yet)
    /// for local-only procs.
    proc_id: reference::ProcId,

    /// A muxer instance that has entries for every actor managed by
    /// the proc.
    proc_muxer: MailboxMuxer,

    /// Sender used to forward messages outside of the proc.
    forwarder: BoxedMailboxSender,

    /// Per-name atomic index allocator. Used by `allocate_root_id`
    /// (index 0, counter starts at 1) and `allocate_child_id`
    /// (increments the parent's counter). Each root name gets its
    /// own independent counter.
    roots: DashMap<String, AtomicUsize>,

    /// All actor instances in this proc.
    instances: DashMap<reference::ActorId, WeakInstanceCell>,

    /// Snapshots of terminated actors for post-mortem introspection.
    /// Populated by the introspect task just before it exits on
    /// terminal status. Bounded by
    /// [`config::TERMINATED_SNAPSHOT_RETENTION`].
    terminated_snapshots: DashMap<reference::ActorId, crate::introspect::IntrospectResult>,

    /// Used by root actors to send events to the actor coordinating
    /// supervision of root actors in this proc.
    supervision_coordinator_port: OnceLock<PortHandle<ActorSupervisionEvent>>,
}

impl Drop for ProcState {
    fn drop(&mut self) {
        // We only want log ProcStatus::Dropped when ProcState is dropped,
        // rather than Proc is dropped. This is because we need to wait for
        // Proc::inner's ref count becomes 0.
        tracing::info!(
            proc_id = %self.proc_id,
            name = "ProcStatus",
            status = "Dropped"
        );
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
    pub supervision: PortReceiver<ActorSupervisionEvent>,
    /// Control signals for the actor.
    pub signal: PortReceiver<Signal>,
    /// Primary work queue for handler dispatch.
    pub work: mpsc::UnboundedReceiver<WorkCell<A>>,
}

impl Proc {
    /// Create a pre-configured proc with the given proc id and forwarder.
    pub fn configured(proc_id: reference::ProcId, forwarder: BoxedMailboxSender) -> Self {
        tracing::info!(
            proc_id = %proc_id,
            name = "ProcStatus",
            status = "Created"
        );

        Self {
            inner: Arc::new(ProcState {
                proc_id,
                proc_muxer: MailboxMuxer::new(),
                forwarder,
                roots: DashMap::new(),
                instances: DashMap::new(),
                terminated_snapshots: DashMap::new(),
                supervision_coordinator_port: OnceLock::new(),
            }),
        }
    }

    /// Create a new direct-addressed proc.
    pub fn direct(addr: ChannelAddr, name: String) -> Result<Self, ChannelError> {
        let (addr, rx) = channel::serve(addr)?;
        let proc_id = reference::ProcId::with_name(addr, name);
        let proc = Self::configured(proc_id, DialMailboxRouter::new().into_boxed());
        proc.clone().serve(rx);
        Ok(proc)
    }

    /// Set the supervision coordinator's port for this proc. Return Err if it is
    /// already set.
    pub fn set_supervision_coordinator(
        &self,
        port: PortHandle<ActorSupervisionEvent>,
    ) -> Result<(), anyhow::Error> {
        self.state()
            .supervision_coordinator_port
            .set(port)
            .map_err(|existing| anyhow::anyhow!("coordinator port is already set to {existing}"))
    }

    /// Handle a supervision event received by the proc. Attempt to forward it to the
    /// supervision coordinator port if one is set, otherwise crash the process.
    pub fn handle_unhandled_supervision_event(
        &self,
        cx: &impl context::Actor,
        event: ActorSupervisionEvent,
    ) {
        let result = match self.state().supervision_coordinator_port.get() {
            Some(port) => port.send(cx, event.clone()).map_err(anyhow::Error::from),
            None => {
                if !event.is_error() {
                    // Normal lifecycle events (e.g. clean stop) without a coordinator
                    // are silently dropped.
                    return;
                }
                Err(anyhow::anyhow!(
                    "coordinator port is not set for proc {}",
                    self.proc_id(),
                ))
            }
        };
        if let Err(err) = result {
            if !event.is_error() {
                // Normal lifecycle events that fail to send (e.g. coordinator
                // mailbox already closed during shutdown) are silently dropped.
                tracing::debug!(
                    "proc {}: dropping non-error supervision event {}: {:?}",
                    self.proc_id(),
                    event,
                    err
                );
                return;
            }
            tracing::error!(
                "proc {}: could not propagate supervision event {} due to error: {:?}: crashing",
                self.proc_id(),
                event,
                err
            );

            std::process::exit(1);
        }
    }

    /// Create a new local-only proc. This proc is not allowed to forward messages
    /// outside of the proc itself.
    pub fn local() -> Self {
        let rank = NEXT_LOCAL_RANK.fetch_add(1, Ordering::Relaxed);
        let addr = ChannelAddr::any(ChannelTransport::Local);
        let proc_id = reference::ProcId::unique(addr, format!("local_{}", rank));
        Proc::configured(proc_id, BoxedMailboxSender::new(PanickingMailboxSender))
    }

    /// The proc's ID.
    pub fn proc_id(&self) -> &reference::ProcId {
        &self.state().proc_id
    }

    /// Shared sender used by the proc to forward messages to remote
    /// destinations.
    pub fn forwarder(&self) -> &BoxedMailboxSender {
        &self.inner.forwarder
    }

    /// Convenience accessor for state.
    fn state(&self) -> &ProcState {
        self.inner.as_ref()
    }

    /// A global runtime proc used by this crate.
    pub(crate) fn runtime() -> &'static Proc {
        static RUNTIME_PROC: OnceLock<Proc> = OnceLock::new();
        RUNTIME_PROC.get_or_init(|| {
            let addr = ChannelAddr::any(ChannelTransport::Local);
            let proc_id = reference::ProcId::unique(addr, "hyperactor_runtime");
            Proc::configured(proc_id, BoxedMailboxSender::new(PanickingMailboxSender))
        })
    }

    /// Attach a mailbox to the proc with the provided root name.
    pub fn attach(&self, name: &str) -> Result<Mailbox, anyhow::Error> {
        let actor_id: reference::ActorId = self.allocate_root_id(name)?;
        Ok(self.bind_mailbox(actor_id))
    }

    /// Attach a mailbox to the proc as a child actor.
    pub fn attach_child(&self, parent_id: &reference::ActorId) -> Result<Mailbox, anyhow::Error> {
        let actor_id: reference::ActorId = self.allocate_child_id(parent_id)?;
        Ok(self.bind_mailbox(actor_id))
    }

    /// Bind a mailbox to the proc.
    fn bind_mailbox(&self, actor_id: reference::ActorId) -> Mailbox {
        let mbox = Mailbox::new(actor_id, BoxedMailboxSender::new(self.downgrade()));

        // TODO: T210748165 tie the muxer entry to the lifecycle of the mailbox held
        // by the caller. This will likely require a weak reference.
        self.state().proc_muxer.bind_mailbox(mbox.clone());
        mbox
    }

    /// Attach a mailbox to the proc with the provided root name, and bind an [`ActorRef`].
    /// This is intended only for testing, and will be replaced by simpled utilities.
    pub fn attach_actor<R, M>(
        &self,
        name: &str,
    ) -> Result<(Instance<()>, reference::ActorRef<R>, PortReceiver<M>), anyhow::Error>
    where
        M: RemoteMessage,
        R: Referable + RemoteHandles<M>,
    {
        let (instance, _handle) = self.instance(name)?;
        let (_handle, rx) = instance.bind_actor_port::<M>();
        let actor_ref = reference::ActorRef::attest(instance.self_id().clone());
        Ok((instance, actor_ref, rx))
    }

    /// Spawn a named (root) actor on this proc. The name of the actor must be
    /// unique.
    pub fn spawn<A: Actor>(&self, name: &str, actor: A) -> Result<ActorHandle<A>, anyhow::Error> {
        let actor_id = self.allocate_root_id(name)?;
        self.spawn_inner(actor_id, actor, None)
    }

    /// Common spawn logic for both root and child actors.
    /// Creates a tracing span with the correct actor_id before starting the actor.
    #[hyperactor::instrument(fields(actor_id = actor_id.to_string(), actor_name = actor_id.name(), actor_type = std::any::type_name::<A>()))]
    fn spawn_inner<A: Actor>(
        &self,
        actor_id: reference::ActorId,
        actor: A,
        parent: Option<InstanceCell>,
    ) -> Result<ActorHandle<A>, anyhow::Error> {
        let (instance, receivers) = Instance::new(self.clone(), actor_id, false, parent);
        Ok(instance.start(actor, receivers))
    }

    /// Create a lightweight client instance (no actor loop, no
    /// introspect task).  This is safe to call outside a Tokio
    /// runtime — unlike [`actor_instance`], it never calls
    /// `tokio::spawn`.
    pub fn instance(&self, name: &str) -> Result<(Instance<()>, ActorHandle<()>), anyhow::Error> {
        let actor_id = self.allocate_root_id(name)?;
        let (instance, _receivers) = Instance::new(self.clone(), actor_id, false, None);
        let handle = ActorHandle::new(instance.inner.cell.clone(), instance.inner.ports.clone());
        instance.change_status(ActorStatus::Client);
        Ok((instance, handle))
    }

    /// Create a lightweight client instance that handles
    /// [`IntrospectMessage`].
    ///
    /// Like [`instance`](Self::instance), this creates a client-mode
    /// instance with no actor message loop. Unlike `instance`, it
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
        let actor_id = self.allocate_root_id(name)?;
        let (instance, receivers) = Instance::new(self.clone(), actor_id, false, None);
        let handle = ActorHandle::new(instance.inner.cell.clone(), instance.inner.ports.clone());
        instance.change_status(ActorStatus::Client);
        tokio::spawn(crate::introspect::serve_introspect(
            instance.inner.cell.clone(),
            instance.inner.mailbox.clone(),
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
        let actor_id = self.allocate_root_id(name)?;
        let span = tracing::debug_span!(
            "actor_instance",
            actor_name = name,
            actor_type = std::any::type_name::<A>(),
            actor_id = actor_id.to_string(),
        );
        let _guard = span.enter();
        let (instance, receivers) = Instance::new(self.clone(), actor_id.clone(), false, None);
        let handle = ActorHandle::new(instance.inner.cell.clone(), instance.inner.ports.clone());
        instance.change_status(ActorStatus::Client);

        let introspect_cell = instance.inner.cell.clone();
        let introspect_mailbox = instance.inner.mailbox.clone();
        tokio::spawn(crate::introspect::serve_introspect(
            introspect_cell,
            introspect_mailbox,
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

    /// Traverse all actor trees in this proc, starting from root actors (pid=0).
    ///
    /// **Caution:** This holds DashMap shard read locks while doing
    /// `Weak::upgrade()` and recursively walking the actor tree per
    /// entry. Under rapid actor churn, this causes convoy starvation
    /// with concurrent `insert`/`remove` operations. Prefer
    /// `all_instance_keys()` with point lookups if you only need
    /// actor IDs. Currently unused in production code.
    pub fn traverse<F>(&self, f: &mut F)
    where
        F: FnMut(&InstanceCell, usize),
    {
        for entry in self.state().instances.iter() {
            if entry.key().pid() == 0 {
                if let Some(cell) = entry.value().upgrade() {
                    cell.traverse(f);
                }
            }
        }
    }

    /// Look up an instance by ActorId.
    pub fn get_instance(&self, actor_id: &reference::ActorId) -> Option<InstanceCell> {
        self.state()
            .instances
            .get(actor_id)
            .and_then(|weak| weak.upgrade())
    }

    /// Returns the ActorIds of all root actors (pid=0) in this proc.
    ///
    /// **Caution:** This iterates the full DashMap under shard read
    /// locks. The per-entry work is lightweight (key filter + clone),
    /// but under very rapid churn the iteration can still contend
    /// with concurrent writes. Prefer `all_instance_keys()` with a
    /// post-filter if this becomes a hot path. Currently unused in
    /// production code.
    pub fn root_actor_ids(&self) -> Vec<reference::ActorId> {
        self.state()
            .instances
            .iter()
            .filter(|entry| entry.key().pid() == 0)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Returns the ActorIds of all live actors in this proc, including
    /// dynamically spawned children.
    ///
    /// An actor is considered live if its weak reference is
    /// upgradeable and its status is not terminal. This excludes
    /// actors whose `InstanceCell` has been dropped and actors that
    /// have stopped or failed but whose Arc is still held (e.g. by
    /// the introspect task during teardown).
    pub fn all_actor_ids(&self) -> Vec<reference::ActorId> {
        self.state()
            .instances
            .iter()
            .filter(|entry| {
                entry
                    .value()
                    .upgrade()
                    .is_some_and(|cell| !cell.status().borrow().is_terminal())
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Snapshot all instance keys from the DashMap without inspecting
    /// values. Each shard read lock is held only long enough to clone
    /// the key — no `Weak::upgrade()`, no `watch::borrow()`, no
    /// `is_terminal()` check. This minimises shard lock hold time to
    /// avoid convoy starvation with concurrent `insert`/`remove`
    /// operations during rapid actor churn.
    ///
    /// The returned list may include actors that are terminal or
    /// whose `WeakInstanceCell` no longer upgrades. Callers should
    /// tolerate stale entries (e.g. by handling "not found" on
    /// subsequent per-actor lookups).
    pub fn all_instance_keys(&self) -> Vec<reference::ActorId> {
        self.state()
            .instances
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Look up a terminated actor's snapshot by ID.
    pub fn terminated_snapshot(
        &self,
        actor_id: &reference::ActorId,
    ) -> Option<crate::introspect::IntrospectResult> {
        self.state()
            .terminated_snapshots
            .get(actor_id)
            .map(|e| e.value().clone())
    }

    /// Return all terminated actor IDs currently retained.
    pub fn all_terminated_actor_ids(&self) -> Vec<reference::ActorId> {
        self.state()
            .terminated_snapshots
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }

    /// Create a child instance. Called from `Instance`.
    fn child_instance(
        &self,
        parent: InstanceCell,
    ) -> Result<(Instance<()>, ActorHandle<()>), anyhow::Error> {
        let actor_id = self.allocate_child_id(parent.actor_id())?;
        let _ = tracing::debug_span!(
            "child_actor_instance",
            parent_actor_id = %parent.actor_id(),
            actor_type = std::any::type_name::<()>(),
            actor_id = %actor_id,
        );

        let (instance, _receivers) = Instance::new(self.clone(), actor_id, false, Some(parent));
        // Client-mode instance: no actor loop, no introspect task.
        // Receivers are intentionally dropped.
        let handle = ActorHandle::new(instance.inner.cell.clone(), instance.inner.ports.clone());
        instance.change_status(ActorStatus::Client);
        Ok((instance, handle))
    }

    /// Spawn a child actor from the provided parent on this proc. The parent actor
    /// must already belong to this proc, a fact which is asserted in code.
    ///
    /// When spawn_child returns, the child has an associated cell and is linked
    /// with its parent.
    pub(crate) fn spawn_child<A: Actor>(
        &self,
        parent: InstanceCell,
        actor: A,
    ) -> Result<ActorHandle<A>, anyhow::Error> {
        let actor_id = self.allocate_child_id(parent.actor_id())?;
        self.spawn_inner(actor_id, actor, Some(parent))
    }

    /// Spawn a named child actor. Same as `spawn_child` but the child
    /// gets a descriptive name instead of inheriting the parent's.
    /// Supervision linkage to parent is preserved.
    pub(crate) fn spawn_named_child<A: Actor>(
        &self,
        parent: InstanceCell,
        name: &str,
        actor: A,
    ) -> Result<ActorHandle<A>, anyhow::Error> {
        let actor_id = self.allocate_named_child_id(parent.actor_id(), name)?;
        self.spawn_inner(actor_id, actor, Some(parent))
    }

    /// Call `abort` on the `JoinHandle` associated with the given
    /// root actor. If successful return `Some(root.clone())` else
    /// `None`.
    pub fn abort_root_actor(
        &self,
        root: &reference::ActorId,
        this_handle: Option<&JoinHandle<()>>,
    ) -> Option<impl Future<Output = reference::ActorId>> {
        self.state()
            .instances
            .get(root)
            .into_iter()
            .flat_map(|e| e.upgrade())
            .map(|cell| {
                let r1 = root.clone();
                let r2 = root.clone();
                // If abort_root_actor was called from inside an actor task, we don't want to abort that actor's task yet.
                let skip_abort = this_handle.is_some_and(|this_h| {
                    cell.inner
                        .actor_task_handle
                        .get()
                        .is_some_and(|other_h| std::ptr::eq(this_h, other_h))
                });
                // `Instance::start()` is infallible and should
                // complete quickly, so calling `wait()` on `actor_task_handle`
                // should be safe (i.e., not hang forever).
                async move {
                    tokio::task::spawn_blocking(move || {
                        if !skip_abort {
                            let h = cell.inner.actor_task_handle.wait();
                            tracing::debug!("{}: aborting {:?}", r1, h);
                            h.abort();
                        }
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
        actor_id: &reference::ActorId,
        reason: String,
    ) -> Option<watch::Receiver<ActorStatus>> {
        if let Some(entry) = self.state().instances.get(actor_id) {
            match entry.value().upgrade() {
                None => None, // the actor's cell has been dropped
                Some(cell) => {
                    tracing::info!("sending stop signal to {}", cell.actor_id());
                    if let Err(err) = cell.signal(Signal::DrainAndStop(reason)) {
                        tracing::error!(
                            "{}: failed to send stop signal to pid {}: {:?}",
                            self.proc_id(),
                            cell.pid(),
                            err
                        );
                        None
                    } else {
                        Some(cell.status().clone())
                    }
                }
            }
        } else {
            tracing::error!("no actor {} found in {}", actor_id, self.proc_id());
            None
        }
    }

    /// Stop the proc. Returns a pair of:
    /// - the actors observed to stop;
    /// - the actors not observed to stop when timeout.
    ///
    /// If `cx` is specified, it means this method was called from inside an actor
    /// in which case we shouldn't wait for it to stop and need to delay aborting
    /// its task.
    pub async fn destroy_and_wait<A: Actor>(
        &mut self,
        timeout: Duration,
        cx: Option<&Context<'_, A>>,
        reason: &str,
    ) -> Result<(Vec<reference::ActorId>, Vec<reference::ActorId>), anyhow::Error> {
        self.destroy_and_wait_except_current::<A>(timeout, cx, false, reason)
            .await
    }

    /// Stop the proc. Returns a pair of:
    /// - the actors observed to stop;
    /// - the actors not observed to stop when timeout.
    ///
    /// If `cx` is specified, it means this method was called from inside an actor
    /// in which case we shouldn't wait for it to stop and need to delay aborting
    /// its task.
    /// If except_current is true, don't stop the actor represented by "cx" at
    /// all.
    #[hyperactor::instrument]
    pub async fn destroy_and_wait_except_current<A: Actor>(
        &mut self,
        timeout: Duration,
        cx: Option<&Context<'_, A>>,
        except_current: bool,
        reason: &str,
    ) -> Result<(Vec<reference::ActorId>, Vec<reference::ActorId>), anyhow::Error> {
        tracing::debug!("{}: proc stopping", self.proc_id());

        let (this_handle, this_actor_id) = cx.map_or((None, None), |cx| {
            (
                Some(cx.actor_task_handle().expect("cannot call destroy_and_wait from inside an actor unless actor has finished starting")),
                Some(cx.self_id())
            )
        });

        let mut statuses = HashMap::new();
        for actor_id in self
            .state()
            .instances
            .iter()
            .filter(|entry| entry.key().pid() == 0)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
        {
            if let Some(status) = self.stop_actor(&actor_id, reason.to_string()) {
                statuses.insert(actor_id, status);
            }
        }
        tracing::debug!("{}: proc stopped", self.proc_id());

        let waits: Vec<_> = statuses
            .iter_mut()
            .filter(|(actor_id, _)| Some(*actor_id) != this_actor_id)
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
        let stopped_actors: Vec<_> = results
            .iter()
            .filter_map(|actor_id| actor_id.as_ref())
            .cloned()
            .collect();
        let aborted_actors: Vec<_> = statuses
            .iter()
            .filter(|(actor_id, _)| !stopped_actors.contains(actor_id))
            .map(|(actor_id, _)| {
                let f = self.abort_root_actor(actor_id, this_handle);
                async move {
                    let _ = if let Some(f) = f { Some(f.await) } else { None };
                    // If `is_none(&_)` then the associated actor's
                    // instance cell was already dropped when we went
                    // to call `abort()` on the cell's task handle.

                    actor_id.clone()
                }
            })
            .collect();
        let aborted_actors = futures::future::join_all(aborted_actors).await;

        if let Some(this_handle) = this_handle
            && let Some(this_actor_id) = this_actor_id
            && !except_current
        {
            tracing::debug!("{}: aborting (delayed) {:?}", this_actor_id, this_handle);
            this_handle.abort()
        };

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
        actor_ref: &reference::ActorRef<R>,
    ) -> Option<ActorHandle<R>> {
        let cell = self.inner.instances.get(actor_ref.actor_id())?.upgrade()?;
        // An actor whose status is terminal has stopped processing
        // messages even if its InstanceCell Arc is still alive (e.g.
        // held by the introspect task during teardown).
        if cell.status().borrow().is_terminal() {
            return None;
        }
        cell.downcast_handle()
    }

    /// Create a root allocation in the proc.
    fn allocate_root_id(&self, name: &str) -> Result<reference::ActorId, anyhow::Error> {
        let name = name.to_string();
        match self.state().roots.entry(name.to_string()) {
            Entry::Vacant(entry) => {
                entry.insert(AtomicUsize::new(1));
            }
            Entry::Occupied(_) => {
                anyhow::bail!("an actor with name '{}' has already been spawned", name)
            }
        }
        Ok(reference::ActorId::new(
            self.state().proc_id.clone(),
            name.to_string(),
            0,
        ))
    }

    /// Create a child allocation in the proc.
    #[hyperactor::instrument(fields(actor_name=parent_id.name()))]
    pub(crate) fn allocate_child_id(
        &self,
        parent_id: &reference::ActorId,
    ) -> Result<reference::ActorId, anyhow::Error> {
        assert_eq!(*parent_id.proc_id(), self.state().proc_id);
        let pid = match self.state().roots.get(parent_id.name()) {
            None => anyhow::bail!(
                "no actor named {} in proc {}",
                parent_id.name(),
                self.state().proc_id
            ),
            Some(next_pid) => next_pid.fetch_add(1, Ordering::Relaxed),
        };
        Ok(parent_id.child_id(pid))
    }

    /// Allocate an actor ID with a custom name on this proc.
    ///
    /// See AI-1 (named-child pid) and AI-3 (controller ActorId
    /// uniqueness) in module doc.
    pub(crate) fn allocate_named_child_id(
        &self,
        parent_id: &reference::ActorId,
        name: &str,
    ) -> Result<reference::ActorId, anyhow::Error> {
        let inherited = self.allocate_child_id(parent_id)?;
        Ok(reference::ActorId::new(
            inherited.proc_id().clone(),
            name,
            inherited.pid(),
        ))
    }

    /// Downgrade to a weak reference that doesn't prevent the proc from being dropped.
    pub fn downgrade(&self) -> WeakProc {
        WeakProc::new(self)
    }
}

#[async_trait]
impl MailboxSender for Proc {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if envelope.dest().actor_id().proc_id() == &self.state().proc_id {
            self.state().proc_muxer.post(envelope, return_handle)
        } else {
            self.state().forwarder.post(envelope, return_handle)
        }
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
            None => envelope.undeliverable(
                DeliveryError::BrokenLink("fail to upgrade WeakProc".to_string()),
                return_handle,
            ),
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

    ports: Arc<Ports<A>>,

    /// A watch for communicating the actor's state.
    status_tx: watch::Sender<ActorStatus>,

    /// This instance's globally unique ID.
    id: Uuid,

    /// Used to assign sequence numbers for messages sent from this actor.
    sequencer: Sequencer,

    /// Per-instance local storage.
    instance_locals: ActorLocalStorage,
}

impl<A: Actor> InstanceState<A> {
    fn self_id(&self) -> &reference::ActorId {
        self.mailbox.actor_id()
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
                    actor_id = %self.self_id(),
                    actor_name = self.self_id().name(),
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
    actor_loop: Option<(PortReceiver<Signal>, PortReceiver<ActorSupervisionEvent>)>,
    /// Work queue for dispatching messages to actor handlers.
    work: mpsc::UnboundedReceiver<WorkCell<A>>,
    /// Introspect message receiver for the dedicated introspect task.
    introspect: PortReceiver<IntrospectMessage>,
}

impl<A: Actor> Instance<A> {
    /// Create a new actor instance in Created state.
    fn new(
        proc: Proc,
        actor_id: reference::ActorId,
        detached: bool,
        parent: Option<InstanceCell>,
    ) -> (Self, InstanceReceivers<A>) {
        // Set up messaging
        let mailbox = Mailbox::new(actor_id.clone(), BoxedMailboxSender::new(proc.downgrade()));
        let (work_tx, work_rx) = ordered_channel(
            actor_id.to_string(),
            hyperactor_config::global::get(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER),
        );
        let ports: Arc<Ports<A>> = Arc::new(Ports::new(mailbox.clone(), work_tx));
        proc.state().proc_muxer.bind_mailbox(mailbox.clone());
        let (status_tx, status_rx) = watch::channel(ActorStatus::Created);

        let actor_type = match TypeInfo::of::<A>() {
            Some(info) => ActorType::Named(info),
            None => ActorType::Anonymous(std::any::type_name::<A>()),
        };
        let actor_loop_ports = if detached {
            None
        } else {
            let (signal_port, signal_receiver) = ports.open_message_port().unwrap();
            let (supervision_port, supervision_receiver) = mailbox.open_port();
            Some((
                (signal_port, supervision_port),
                (signal_receiver, supervision_receiver),
            ))
        };

        let (actor_loop, actor_loop_receivers) = actor_loop_ports.unzip();

        // Introspect port: a separate channel handled by a dedicated
        // tokio task (not the actor's message loop). Pre-registered
        // so Ports::get finds the Occupied entry and skips WorkCell
        // creation. bind_actor_port() registers in the mailbox
        // dispatch table at IntrospectMessage::port().
        //
        // Exercises S3, S4, S9 (see introspect module doc).
        let (introspect_port, introspect_receiver) =
            ports.open_message_port::<IntrospectMessage>().unwrap();
        introspect_port.bind_actor_port();

        let cell = InstanceCell::new(
            actor_id,
            actor_type,
            proc.clone(),
            actor_loop,
            status_rx,
            parent,
            ports.clone(),
        );
        let instance_id = Uuid::now_v7();
        let inner = Arc::new(InstanceState {
            proc,
            cell,
            mailbox,
            ports,
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
    fn change_status(&self, new: ActorStatus) {
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
            self.self_id(),
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
            let change_reason = match new {
                ActorStatus::Failed(reason) => reason.to_string(),
                _ => "".to_string(),
            };
            tracing::info!(
                name = "ActorStatus",
                actor_id = %self.self_id(),
                actor_name = self.self_id().name(),
                status = new_status,
                prev_status = old.arm().unwrap_or("unknown"),
                caller = %Location::caller(),
                change_reason,
            );
            let actor_id = hash_to_u64(self.self_id());
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

    /// This instance's actor ID.
    pub fn self_id(&self) -> &reference::ActorId {
        self.inner.self_id()
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
        handler: impl (Fn(&crate::reference::Reference) -> IntrospectResult) + Send + Sync + 'static,
    ) {
        self.inner.cell.set_query_child_handler(handler);
    }

    /// Signal the actor to stop.
    pub fn stop(&self, reason: &str) -> Result<(), ActorError> {
        tracing::info!(
            actor_id = %self.inner.cell.actor_id(),
            reason,
            "Instance::stop called",
        );
        self.inner
            .cell
            .signal(Signal::DrainAndStop(reason.to_string()))
    }

    /// Signal the actor to abort with a provided reason.
    pub fn abort(&self, reason: &str) -> Result<(), ActorError> {
        tracing::info!(
            actor_id = %self.inner.cell.actor_id(),
            reason,
            "Instance::abort called",
        );
        self.inner.cell.signal(Signal::Abort(reason.to_string()))
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

    /// Get the per-instance local storage.
    pub fn locals(&self) -> &ActorLocalStorage {
        &self.inner.instance_locals
    }

    /// Send a message to the actor running on the proc.
    pub fn post(&self, port_id: reference::PortId, headers: Flattrs, message: wirevalue::Any) {
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
        port_id: reference::PortId,
        headers: Flattrs,
        message: wirevalue::Any,
    ) {
        <Self as context::MailboxExt>::post(
            self,
            port_id,
            headers,
            message,
            true,
            context::SeqInfoPolicy::AllowExternal,
        )
    }

    /// Send a message to the actor itself with a delay usually to trigger some event.
    pub fn self_message_with_delay<M>(&self, message: M, delay: Duration) -> Result<(), ActorError>
    where
        M: Message,
        A: Handler<M>,
    {
        // A global client to send self message.
        static CLIENT: OnceLock<(Instance<()>, ActorHandle<()>)> = OnceLock::new();
        let client = &CLIENT
            .get_or_init(|| Proc::runtime().instance("self_message_client").unwrap())
            .0;
        let port = self.port();
        let self_id = self.self_id().clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Err(e) = port.send(&client, message) {
                // TODO: this is a fire-n-forget thread. We need to
                // handle errors in a better way.
                tracing::info!("{}: error sending delayed message: {}", self_id, e);
            }
        });
        Ok(())
    }

    /// Start an A-typed actor onto this instance with the provided params. When spawn returns,
    /// the actor has been linked with its parent, if it has one.
    fn start(self, actor: A, receivers: InstanceReceivers<A>) -> ActorHandle<A> {
        let instance_cell = self.inner.cell.clone();
        let actor_id = self.inner.cell.actor_id().clone();
        let actor_handle = ActorHandle::new(self.inner.cell.clone(), self.inner.ports.clone());

        // Spawn the introspect task — a separate tokio task that
        // reads InstanceCell directly and replies via the actor's
        // Mailbox. The actor loop never sees IntrospectMessage.
        let introspect_cell = self.inner.cell.clone();
        let introspect_mailbox = self.inner.mailbox.clone();
        tokio::spawn(crate::introspect::serve_introspect(
            introspect_cell,
            introspect_mailbox,
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
        actor_loop_receivers: (PortReceiver<Signal>, PortReceiver<ActorSupervisionEvent>),
        mut work_rx: mpsc::UnboundedReceiver<WorkCell<A>>,
    ) {
        let result = self
            .run_actor_tree(&mut actor, actor_loop_receivers, &mut work_rx)
            .await;

        assert!(self.is_stopping());
        let event = match result {
            Ok(stop_reason) => {
                let status = ActorStatus::Stopped(stop_reason);
                self.mailbox().close(status.clone());
                let event = ActorSupervisionEvent::new(
                    self.inner.cell.actor_id().clone(),
                    actor.display_name(),
                    status.clone(),
                    None,
                );
                // FI-1: store supervision_event BEFORE change_status.
                *self.inner.cell.inner.supervision_event.lock().unwrap() = Some(event.clone());
                self.change_status(status);
                Some(event)
            }
            Err(err) => {
                match *err.kind {
                    ActorErrorKind::UnhandledSupervisionEvent(box event) => {
                        // We use the event's actor_status as this actor's terminal status.
                        assert!(event.actor_status.is_terminal());
                        self.mailbox().close(event.actor_status.clone());
                        // FI-1: store supervision_event BEFORE change_status.
                        *self.inner.cell.inner.supervision_event.lock().unwrap() =
                            Some(event.clone());
                        self.change_status(event.actor_status.clone());
                        Some(event)
                    }
                    _ => {
                        let error_kind = ActorErrorKind::Generic(err.kind.to_string());
                        let status = ActorStatus::Failed(error_kind);
                        self.mailbox().close(status.clone());
                        let event = ActorSupervisionEvent::new(
                            self.inner.cell.actor_id().clone(),
                            actor.display_name(),
                            status.clone(),
                            None,
                        );
                        // FI-1: store supervision_event BEFORE change_status.
                        *self.inner.cell.inner.supervision_event.lock().unwrap() =
                            Some(event.clone());
                        self.change_status(status);
                        Some(event)
                    }
                }
            }
        };

        if let Some(parent) = self.inner.cell.maybe_unlink_parent() {
            if let Some(event) = event {
                // Parent exists, failure should be propagated to the parent.
                parent.send_supervision_event_or_crash(&self, event);
            }
            // TODO: we should get rid of this signal, and use *only* supervision events for
            // the purpose of conveying lifecycle changes
            if let Err(err) = parent.signal(Signal::ChildStopped(self.inner.cell.pid())) {
                tracing::error!(
                    "{}: failed to send stop message to parent pid {}: {:?}",
                    self.self_id(),
                    parent.pid(),
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
    }

    /// Runs the actor, and manages its supervision tree. When the function returns,
    /// the whole tree rooted at this actor has stopped. On success, returns the reason
    /// why the actor stopped. On failure, returns the error that caused the failure.
    async fn run_actor_tree(
        &mut self,
        actor: &mut A,
        mut actor_loop_receivers: (PortReceiver<Signal>, PortReceiver<ActorSupervisionEvent>),
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
                    self.self_id(),
                    ActorErrorKind::panic(anyhow::anyhow!(panic_info)),
                ))
            }
        };

        assert!(!self.is_terminal());
        self.change_status(ActorStatus::Stopping);
        if let Err(err) = &result {
            tracing::error!("{}: actor failure: {}", self.self_id(), err);
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
                    self.self_id(),
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
                Ok(signal) => {
                    if let Signal::ChildStopped(pid) = signal? {
                        assert!(self.inner.cell.get_child(pid).is_none());
                    }
                }
                Err(_) => {
                    tracing::warn!(
                        "timeout waiting for ChildStopped signal from child on actor: {}, ignoring",
                        self.self_id()
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
            match tokio::time::timeout(cleanup_timeout, actor.cleanup(self, result.as_ref().err()))
                .await
            {
                Ok(Ok(x)) => Ok(x),
                Ok(Err(e)) => Err(ActorError::new(self.self_id(), ActorErrorKind::cleanup(e))),
                Err(e) => Err(ActorError::new(
                    self.self_id(),
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
        actor_loop_receivers: &mut (PortReceiver<Signal>, PortReceiver<ActorSupervisionEvent>),
        work_rx: &mut mpsc::UnboundedReceiver<WorkCell<A>>,
    ) -> Result<String, ActorError> {
        let (signal_receiver, supervision_event_receiver) = actor_loop_receivers;

        self.change_status(ActorStatus::Initializing);
        actor
            .init(self)
            .await
            .map_err(|err| ActorError::new(self.self_id(), ActorErrorKind::init(err)))?;
        let need_drain;
        let stop_reason;
        'messages: loop {
            self.change_status(ActorStatus::Idle);
            let metric_pairs =
                hyperactor_telemetry::kv_pairs!("actor_id" => self.self_id().to_string());
            tokio::select! {
                work = work_rx.recv() => {
                    ACTOR_MESSAGES_RECEIVED.add(1, metric_pairs);
                    ACTOR_MESSAGE_QUEUE_SIZE.add(-1, metric_pairs);
                    let _ = ACTOR_MESSAGE_HANDLER_DURATION.start(metric_pairs);
                    let work = work.expect("inconsistent work queue state");
                    if let Err(err) = work.handle(actor, self).await {
                        for supervision_event in supervision_event_receiver.drain() {
                            self.handle_supervision_event(actor, supervision_event).await?;
                        }
                        let kind = ActorErrorKind::processing(err);
                        return Err(ActorError {
                            actor_id: Box::new(self.self_id().clone()),
                            kind: Box::new(kind),
                        });
                    }
                }
                signal = signal_receiver.recv() => {
                    let signal = signal.map_err(ActorError::from);
                    tracing::debug!("Received signal {signal:?}");
                    match signal? {
                        Signal::Stop(reason) => {
                            need_drain = false;
                            stop_reason = reason;
                            break 'messages;
                        },
                        Signal::DrainAndStop(reason) => {
                            need_drain = true;
                            stop_reason = reason;
                            break 'messages;
                        },
                        Signal::ChildStopped(pid) => {
                            assert!(self.inner.cell.get_child(pid).is_none());
                        },
                        Signal::Abort(reason) => {
                            return Err(ActorError { actor_id: Box::new(self.self_id().clone()), kind: Box::new(ActorErrorKind::Aborted(reason)) });
                        }
                    }
                }
                Ok(supervision_event) = supervision_event_receiver.recv() => {
                    self.handle_supervision_event(actor, supervision_event).await?;
                }
            }
            self.inner
                .cell
                .inner
                .num_processed_messages
                .fetch_add(1, Ordering::SeqCst);
        }

        if need_drain {
            let mut n = 0;
            while let Ok(work) = work_rx.try_recv() {
                if let Err(err) = work.handle(actor, self).await {
                    return Err(ActorError::new(
                        self.self_id(),
                        ActorErrorKind::processing(err),
                    ));
                }
                n += 1;
            }
            tracing::debug!("drained {} messages", n);
        }
        tracing::debug!(
            actor_id = %self.self_id(),
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
        match actor
            .handle_supervision_event(self, &supervision_event)
            .await
        {
            Ok(true) => {
                // The supervision event was handled by this actor, nothing more to do.
                Ok(())
            }
            Ok(false) => {
                let kind = ActorErrorKind::UnhandledSupervisionEvent(Box::new(supervision_event));
                Err(ActorError::new(self.self_id(), kind))
            }
            Err(err) => {
                // The actor failed to handle the supervision event, it should die.
                // Create a new supervision event for this failure and propagate it.
                let kind = ActorErrorKind::ErrorDuringHandlingSupervision(
                    err.to_string(),
                    Box::new(supervision_event),
                );
                Err(ActorError::new(self.self_id(), kind))
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
        // Use a helper function for a better instrument log.
        self.handle_message_with_handler_info(actor, handler_info, headers, message)
            .await
    }

    // Skip serializing all fields except HandlerInfo which includes the typename.
    #[tracing::instrument(level = "debug", name = "handle_message", skip_all, fields(actor_id = %self.self_id(), message_type = %handler_info))]
    async fn handle_message_with_handler_info<M: Message>(
        &self,
        actor: &mut A,
        handler_info: HandlerInfo,
        headers: Flattrs,
        message: M,
    ) -> Result<(), anyhow::Error>
    where
        A: Handler<M>,
    {
        let now = std::time::SystemTime::now();
        let handler_info = Some(handler_info);
        self.change_status(ActorStatus::Processing(now, handler_info.clone()));
        crate::mailbox::headers::log_message_latency_if_sampling(
            &headers,
            self.self_id().to_string(),
        );

        let message_id = headers.get(crate::mailbox::headers::TELEMETRY_MESSAGE_ID);

        if let Some(message_id) = message_id {
            let from_actor_id = headers
                .get(crate::mailbox::headers::SENDER_ACTOR_ID_HASH)
                .unwrap_or(0);
            let to_actor_id = hash_to_u64(self.self_id());
            let port_id = headers.get(crate::mailbox::headers::TELEMETRY_PORT_ID);

            notify_message(hyperactor_telemetry::MessageEvent {
                timestamp: now,
                id: message_id,
                from_actor_id,
                to_actor_id,
                // TODO: populate endpoint
                endpoint: None,
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
        let result = actor
            .handle(&context, message)
            .instrument(self.inner.cell.inner.recording.span())
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

    /// Spawn on child on this instance.
    pub fn spawn<C: Actor>(&self, actor: C) -> anyhow::Result<ActorHandle<C>> {
        self.inner.proc.spawn_child(self.inner.cell.clone(), actor)
    }

    /// Spawn a named child actor on this instance. The child gets a
    /// descriptive name in its ActorId instead of inheriting this
    /// instance's name. Supervision linkage is preserved.
    pub fn spawn_with_name<C: Actor>(
        &self,
        name: &str,
        actor: C,
    ) -> anyhow::Result<ActorHandle<C>> {
        self.inner
            .proc
            .spawn_named_child(self.inner.cell.clone(), name, actor)
    }

    /// Create a new direct child instance.
    pub fn child(&self) -> anyhow::Result<(Instance<()>, ActorHandle<()>)> {
        self.inner.proc.child_instance(self.inner.cell.clone())
    }

    /// Return a handle port handle representing the actor's message
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
    pub fn bind<R: Binds<A>>(&self) -> reference::ActorRef<R> {
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
}

impl Instance<()> {
    /// See [Mailbox::bind_actor_port] for details.
    pub fn bind_actor_port<M: RemoteMessage>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        assert!(
            self.actor_task_handle().is_none(),
            "can only bind actor port on instance with no running actor task"
        );
        self.inner.mailbox.bind_actor_port()
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
    actor_id: reference::ActorId,

    /// Actor info contains the actor's type information.
    actor_type: ActorType,

    /// The proc in which the actor is running.
    proc: Proc,

    /// Control port handles to the actor loop, if one is running.
    actor_loop: Option<(PortHandle<Signal>, PortHandle<ActorSupervisionEvent>)>,

    /// An observer that stores the current status of the actor.
    status: watch::Receiver<ActorStatus>,

    /// A weak reference to this instance's parent.
    parent: WeakInstanceCell,

    /// This instance's children by their PIDs.
    children: DashMap<reference::Index, InstanceCell>,

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
    query_child_handler: RwLock<
        Option<Box<dyn (Fn(&crate::reference::Reference) -> IntrospectResult) + Send + Sync>>,
    >,

    /// The supervision event for this actor's failure, if any.
    /// See FI-1, FI-2 in `introspect` module doc.
    supervision_event: std::sync::Mutex<Option<crate::supervision::ActorSupervisionEvent>>,

    /// Whether this actor is infrastructure/system (hidden by default
    /// in the TUI `s` toggle). Set by spawning code via
    /// `Instance::set_system()`.
    is_system: AtomicBool,

    /// A type-erased reference to Ports<A>, which allows us to recover
    /// an ActorHandle<A> by downcasting.
    ports: Arc<dyn Any + Send + Sync>,
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
        self.children.remove(&child.actor_id.pid()).is_some()
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
    entries: &[(reference::ActorId, Option<String>)],
    excess: usize,
) -> Vec<reference::ActorId> {
    let mut clean: Vec<&reference::ActorId> = Vec::new();
    let mut failed: Vec<(&reference::ActorId, &str)> = Vec::new();
    for (id, occurred_at) in entries {
        match occurred_at {
            Some(ts) => failed.push((id, ts.as_str())),
            None => clean.push(id),
        }
    }

    let mut to_remove: Vec<reference::ActorId> = Vec::new();
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
    fn new(
        actor_id: reference::ActorId,
        actor_type: ActorType,
        proc: Proc,
        actor_loop: Option<(PortHandle<Signal>, PortHandle<ActorSupervisionEvent>)>,
        status: watch::Receiver<ActorStatus>,
        parent: Option<InstanceCell>,
        ports: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        let _ais = actor_id.to_string();
        let cell = Self {
            inner: Arc::new(InstanceCellState {
                actor_id: actor_id.clone(),
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
                recording: hyperactor_telemetry::recorder().record(64),
                published_attrs: RwLock::new(None),
                query_child_handler: RwLock::new(None),
                supervision_event: std::sync::Mutex::new(None),
                is_system: AtomicBool::new(false),
                ports,
            }),
        };
        cell.maybe_link_parent();
        proc.inner
            .instances
            .insert(actor_id.clone(), cell.downgrade());
        cell
    }

    fn wrap(inner: Arc<InstanceCellState>) -> Self {
        Self { inner }
    }

    /// The actor's ID.
    pub fn actor_id(&self) -> &reference::ActorId {
        &self.inner.actor_id
    }

    /// The actor's PID.
    pub(crate) fn pid(&self) -> reference::Index {
        self.inner.actor_id.pid()
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

    /// Send a signal to the actor.
    pub fn signal(&self, signal: Signal) -> Result<(), ActorError> {
        if let Some((signal_port, _)) = &self.inner.actor_loop {
            // A global signal client is used to send signals to the actor.
            static CLIENT: OnceLock<(Instance<()>, ActorHandle<()>)> = OnceLock::new();
            let client = &CLIENT
                .get_or_init(|| Proc::runtime().instance("global_signal_client").unwrap())
                .0;
            signal_port.send(&client, signal).map_err(ActorError::from)
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
    pub fn send_supervision_event_or_crash(
        &self,
        child_cx: &impl context::Actor, // context of the child who sends the event.
        event: ActorSupervisionEvent,
    ) {
        match &self.inner.actor_loop {
            Some((_, supervision_port)) => {
                if let Err(err) = supervision_port.send(child_cx, event.clone()) {
                    if !event.is_error() {
                        tracing::debug!(
                            "{}: dropping non-error supervision event {}: {:?}",
                            self.actor_id(),
                            event,
                            err
                        );
                        return;
                    }
                    tracing::error!(
                        "{}: failed to send supervision event to actor: {:?}. Crash the process.",
                        self.actor_id(),
                        err
                    );
                    std::process::exit(1);
                }
            }
            None => {
                if !event.is_error() {
                    tracing::debug!(
                        "{}: dropping non-error supervision event to detached actor: {}",
                        self.actor_id(),
                        event,
                    );
                    return;
                }
                tracing::error!(
                    "{}: failed: {}: cannot send supervision event to detached actor: crashing",
                    self.actor_id(),
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
        assert_eq!(self.actor_id().proc_id(), child.actor_id().proc_id());
        self.inner.children.insert(child.pid(), child);
    }

    /// Unlink this instance from a child.
    fn unlink(&self, child: &InstanceCell) {
        assert_eq!(self.actor_id().proc_id(), child.actor_id().proc_id());
        self.inner.children.remove(&child.pid());
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
    fn child_iter(&self) -> impl Iterator<Item = RefMulti<'_, reference::Index, InstanceCell>> {
        self.inner.children.iter()
    }

    /// The number of children this instance has.
    pub fn child_count(&self) -> usize {
        self.inner.children.len()
    }

    /// Returns the ActorIds of this instance's direct children.
    pub fn child_actor_ids(&self) -> Vec<reference::ActorId> {
        self.inner
            .children
            .iter()
            .map(|entry| entry.value().actor_id().clone())
            .collect()
    }

    /// Get a child by its PID.
    fn get_child(&self, pid: reference::Index) -> Option<InstanceCell> {
        self.inner.children.get(&pid).map(|child| child.clone())
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
        handler: impl (Fn(&crate::reference::Reference) -> IntrospectResult) + Send + Sync + 'static,
    ) {
        *self.inner.query_child_handler.write().unwrap() = Some(Box::new(handler));
    }

    /// Invoke the registered QueryChild handler, if any.
    pub fn query_child(&self, child_ref: &crate::reference::Reference) -> Option<IntrospectResult> {
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
        snapshots.insert(self.actor_id().clone(), payload);
        let max = hyperactor_config::global::get(crate::config::TERMINATED_SNAPSHOT_RETENTION);
        let excess = snapshots.len().saturating_sub(max);
        if excess > 0 {
            // Build entries for the eviction selector.
            let entries: Vec<_> = snapshots
                .iter()
                .map(|entry| {
                    let occurred_at =
                        serde_json::from_str::<hyperactor_config::Attrs>(&entry.value().attrs)
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
                    (entry.key().clone(), occurred_at)
                })
                .collect();

            for key in select_eviction_candidates(&entries, excess) {
                snapshots.remove(&key);
            }
        }
    }

    /// This is temporary so that we can share binding code between handle and instance.
    /// We should find some (better) way to consolidate the two.
    pub(crate) fn bind<A: Actor, R: Binds<A>>(&self, ports: &Ports<A>) -> reference::ActorRef<R> {
        <R as Binds<A>>::bind(ports);
        // Signal: pre-registered via open_message_port() in
        // Instance::new(), handled by the actor loop's select!.
        // Ports::bind() here reuses the existing handle.
        //
        // Undeliverable: dispatched through the work queue to the
        // actor's Handler<Undeliverable<MessageEnvelope>>.
        //
        // IntrospectMessage: pre-registered via open_message_port()
        // in Instance::new(), handled by a dedicated introspect task.
        // NOT bound here — its port is registered via
        // bind_actor_port() directly.
        ports.bind::<Signal>();
        ports.bind::<Undeliverable<MessageEnvelope>>();
        // TODO: consider sharing `ports.bound` directly.
        for entry in ports.bound.iter() {
            self.inner
                .exported_named_ports
                .insert(*entry.key(), entry.value());
        }
        reference::ActorRef::attest(self.actor_id().clone())
    }

    /// Attempt to downcast this cell to a concrete actor handle.
    pub(crate) fn downcast_handle<A: Actor>(&self) -> Option<ActorHandle<A>> {
        let ports = Arc::clone(&self.inner.ports).downcast::<Ports<A>>().ok()?;
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
        // Collect and sort children by pid for deterministic traversal order
        let mut children: Vec<_> = self.child_iter().map(|r| r.value().clone()).collect();
        children.sort_by_key(|c| c.pid());
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
                parent.actor_id()
            );
        }
        if self.proc.inner.instances.remove(&self.actor_id).is_none() {
            tracing::error!("instance {} was dropped but not in proc", self.actor_id);
        }
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

/// A polymorphic dictionary that stores ports for an actor's handlers.
/// The interface memoizes the ports so that they are reused. We do not
/// (yet) support stable identifiers across multiple instances of the same
/// actor.
pub struct Ports<A: Actor> {
    ports: DashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>,
    bound: DashMap<u64, &'static str>,
    mailbox: Mailbox,
    workq: OrderedSender<WorkCell<A>>,
}

impl<A: Actor> Ports<A> {
    fn new(mailbox: Mailbox, workq: OrderedSender<WorkCell<A>>) -> Self {
        Self {
            ports: DashMap::new(),
            bound: DashMap::new(),
            mailbox,
            workq,
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
                // Some special case hackery, but it keeps the rest of the code (relatively) simple.
                assert_ne!(
                    key,
                    TypeId::of::<Signal>(),
                    "cannot provision Signal port through `Ports::get`"
                );
                assert_ne!(
                    key,
                    TypeId::of::<IntrospectMessage>(),
                    "cannot provision IntrospectMessage port through `Ports::get`"
                );

                let type_info = TypeInfo::get_by_typeid(key);
                let workq = self.workq.clone();
                let actor_id = self.mailbox.actor_id().to_string();
                let port = self.mailbox.open_enqueue_port(move |headers, msg: M| {
                    let seq_info = headers.get(SEQ_INFO);

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
                    ACTOR_MESSAGE_QUEUE_SIZE.add(
                        1,
                        hyperactor_telemetry::kv_pairs!("actor_id" => actor_id.clone()),
                    );
                    if workq.enable_buffering {
                        match seq_info {
                            Some(SeqInfo::Session { session_id, seq }) => {
                                // TODO: return the message contained in the error instead of dropping them when converting
                                // to anyhow::Error. In that way, the message can be picked up by mailbox and returned to sender.
                                workq.send(session_id, seq, work).map_err(|e| match e {
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
                                anyhow::bail!(error_msg);
                            }
                        }
                    } else {
                        workq.direct_send(work).map_err(anyhow::Error::from)
                    }
                });
                entry.insert(Box::new(port.clone()));
                port
            }
            Entry::Occupied(entry) => {
                let port = entry.get();
                port.downcast_ref::<PortHandle<M>>().unwrap().clone()
            }
        }
    }

    /// Open a (typed) message port as in [`get`], but return a port receiver instead of dispatching
    /// the underlying handler.
    pub(crate) fn open_message_port<M: Message>(&self) -> Option<(PortHandle<M>, PortReceiver<M>)> {
        match self.ports.entry(TypeId::of::<M>()) {
            Entry::Vacant(entry) => {
                let (port, receiver) = self.mailbox.open_port();
                entry.insert(Box::new(port.clone()));
                Some((port, receiver))
            }
            Entry::Occupied(_) => None,
        }
    }

    /// Bind the given message type to its actor port.
    pub fn bind<M: RemoteMessage>(&self)
    where
        A: Handler<M>,
    {
        let port_index = M::port();
        match self.bound.entry(port_index) {
            Entry::Vacant(entry) => {
                self.get::<M>().bind_actor_port();
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
    use std::assert_matches::assert_matches;
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
    use crate::testing::proc_supervison::ProcSupervisionCoordinator;
    use crate::testing::process_assertion::assert_termination;

    #[derive(Debug, Default)]
    #[export]
    struct TestActor;

    impl Actor for TestActor {}

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
            parent.send(cx, TestActorMessage::Spawn(tx)).unwrap();
            rx.await.unwrap()
        }
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
            destination.send(cx, *message)?;
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
            let handle = TestActor.spawn(cx)?;
            reply.send(handle).unwrap();
            Ok(())
        }
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_actor() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let handle = proc.spawn("test", TestActor).unwrap();

        // Check on the join handle.
        assert!(logs_contain(
            format!(
                "{}: spawned with {:?}",
                handle.actor_id(),
                handle.cell().actor_task_handle().unwrap(),
            )
            .as_str()
        ));

        let mut state = handle.status().clone();

        // Send a ping-pong to the actor. Wait for the actor to become idle.

        let (tx, rx) = oneshot::channel::<()>();
        handle.send(&client, TestActorMessage::Reply(tx)).unwrap();
        rx.await.unwrap();

        state
            .wait_for(|state: &ActorStatus| matches!(*state, ActorStatus::Idle))
            .await
            .unwrap();

        // Make sure we enter processing state while the actor is handling a message.
        let (enter_tx, enter_rx) = oneshot::channel::<()>();
        let (exit_tx, exit_rx) = oneshot::channel::<()>();

        handle
            .send(&client, TestActorMessage::Wait(enter_tx, exit_rx))
            .unwrap();
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
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let first = proc.spawn::<TestActor>("first", TestActor).unwrap();
        let second = proc.spawn::<TestActor>("second", TestActor).unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let reply_message = TestActorMessage::Reply(tx);
        first
            .send(
                &client,
                TestActorMessage::Forward(second, Box::new(reply_message)),
            )
            .unwrap();
        rx.await.unwrap();
    }

    #[derive(Debug, Default)]
    #[export]
    struct LookupTestActor;

    impl Actor for LookupTestActor {}

    #[derive(Handler, HandleClient, Debug)]
    enum LookupTestMessage {
        ActorExists(
            reference::ActorRef<TestActor>,
            #[reply] reference::OncePortRef<bool>,
        ),
    }

    #[async_trait]
    #[crate::handle(LookupTestMessage)]
    impl LookupTestMessageHandler for LookupTestActor {
        async fn actor_exists(
            &mut self,
            cx: &crate::Context<Self>,
            actor_ref: reference::ActorRef<TestActor>,
        ) -> Result<bool, anyhow::Error> {
            Ok(actor_ref.downcast_handle(cx).is_some())
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_actor_lookup() {
        let proc = Proc::local();
        let (client, _handle) = proc.instance("client").unwrap();

        let target_actor = proc.spawn::<TestActor>("target", TestActor).unwrap();
        let target_actor_ref = target_actor.bind();
        let lookup_actor = proc
            .spawn::<LookupTestActor>("lookup", LookupTestActor)
            .unwrap();

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
                    reference::ActorRef::attest(target_actor.actor_id().child_id(123).clone())
                )
                .await
                .unwrap()
        );
        // A wrongly-typed actor ref should also not obtain.
        assert!(
            !lookup_actor
                .actor_exists(
                    &client,
                    reference::ActorRef::attest(lookup_actor.actor_id().clone())
                )
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
        assert_eq!(child.actor_id().proc_id(), parent.actor_id().proc_id());
        assert_eq!(
            child.inner.parent.upgrade().unwrap().actor_id(),
            parent.actor_id()
        );
        assert_matches!(
            parent.inner.children.get(&child.pid()),
            Some(node) if node.actor_id() == child.actor_id()
        );
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_child() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();

        let first = proc.spawn::<TestActor>("first", TestActor).unwrap();
        let second = TestActor::spawn_child(&client, &first).await;
        let third = TestActor::spawn_child(&client, &second).await;

        // Check we've got the join handles.
        assert!(logs_with_scope_contain(
            "hyperactor::proc",
            format!(
                "{}: spawned with {:?}",
                first.actor_id(),
                first.cell().actor_task_handle().unwrap()
            )
            .as_str()
        ));
        assert!(logs_with_scope_contain(
            "hyperactor::proc",
            format!(
                "{}: spawned with {:?}",
                second.actor_id(),
                second.cell().actor_task_handle().unwrap()
            )
            .as_str()
        ));
        assert!(logs_with_scope_contain(
            "hyperactor::proc",
            format!(
                "{}: spawned with {:?}",
                third.actor_id(),
                third.cell().actor_task_handle().unwrap()
            )
            .as_str()
        ));

        // These are allocated in sequence:
        assert_eq!(first.actor_id().proc_id(), proc.proc_id());
        assert_eq!(second.actor_id(), &first.actor_id().child_id(1));
        assert_eq!(third.actor_id(), &first.actor_id().child_id(2));

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
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();

        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let root_1 = TestActor::spawn_child(&client, &root).await;
        let root_2 = TestActor::spawn_child(&client, &root).await;
        let root_2_1 = TestActor::spawn_child(&client, &root_2).await;

        root.drain_and_stop("test").unwrap();
        root.await;

        for actor in [root_1, root_2, root_2_1] {
            assert!(actor.send(&client, TestActorMessage::Noop()).is_err());
            assert_matches!(actor.await, ActorStatus::Stopped(reason) if reason == "parent stopping");
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_parent_failure() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        let (_reported, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let root_1 = TestActor::spawn_child(&client, &root).await;
        let root_2 = TestActor::spawn_child(&client, &root).await;
        let root_2_1 = TestActor::spawn_child(&client, &root_2).await;

        root_2
            .send(
                &client,
                TestActorMessage::Fail(anyhow::anyhow!("some random failure")),
            )
            .unwrap();
        let _root_2_actor_id = root_2.actor_id().clone();
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
                message.send(cx, cx.port())?;
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

        let proc = Proc::local();
        let state = Arc::new(AtomicUsize::new(0));
        let actor = TestActor(state.clone());
        let handle = proc.spawn::<TestActor>("test", actor).unwrap();
        let (client, _) = proc.instance("client").unwrap();
        let (tx, rx) = client.open_once_port();
        handle.send(&client, tx).unwrap();
        let usize_handle = rx.recv().await.unwrap();
        usize_handle.send(&client, 123).unwrap();

        handle.drain_and_stop("test").unwrap();
        handle.await;

        assert_eq!(state.load(Ordering::SeqCst), 123);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_actor_panic() {
        // Need this custom hook to store panic backtrace in task_local.
        panic_handler::set_panic_hook();

        let proc = Proc::local();
        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        let (_reported, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let (client, _handle) = proc.instance("client").unwrap();
        let actor_handle = proc.spawn("test", TestActor).unwrap();
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

    #[async_timed_test(timeout_secs = 30)]
    async fn test_local_supervision_propagation() {
        hyperactor_telemetry::initialize_logging_for_test();

        #[derive(Debug)]
        struct TestActor(Arc<AtomicBool>, bool);

        #[async_trait]
        impl Actor for TestActor {
            async fn handle_supervision_event(
                &mut self,
                _this: &Instance<Self>,
                _event: &ActorSupervisionEvent,
            ) -> Result<bool, anyhow::Error> {
                if !self.1 {
                    return Ok(false);
                }

                tracing::error!(
                    "{}: supervision event received: {:?}",
                    _this.self_id(),
                    _event
                );
                self.0.store(true, Ordering::SeqCst);
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
                tracing::info!("{} received message: {}", cx.self_id(), message);
                Err(anyhow::anyhow!(message))
            }
        }

        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let (reported_event, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let root_state = Arc::new(AtomicBool::new(false));
        let root_1_state = Arc::new(AtomicBool::new(false));
        let root_1_1_state = Arc::new(AtomicBool::new(false));
        let root_1_1_1_state = Arc::new(AtomicBool::new(false));
        let root_2_state = Arc::new(AtomicBool::new(false));
        let root_2_1_state = Arc::new(AtomicBool::new(false));

        let root = proc
            .spawn::<TestActor>("root", TestActor(root_state.clone(), false))
            .unwrap();
        let root_1 = proc
            .spawn_child::<TestActor>(
                root.cell().clone(),
                TestActor(
                    root_1_state.clone(),
                    true, /* set true so children's event stops here */
                ),
            )
            .unwrap();
        let root_1_1 = proc
            .spawn_child::<TestActor>(
                root_1.cell().clone(),
                TestActor(root_1_1_state.clone(), false),
            )
            .unwrap();
        let root_1_1_1 = proc
            .spawn_child::<TestActor>(
                root_1_1.cell().clone(),
                TestActor(root_1_1_1_state.clone(), false),
            )
            .unwrap();
        let root_2 = proc
            .spawn_child::<TestActor>(root.cell().clone(), TestActor(root_2_state.clone(), false))
            .unwrap();
        let root_2_1 = proc
            .spawn_child::<TestActor>(
                root_2.cell().clone(),
                TestActor(root_2_1_state.clone(), false),
            )
            .unwrap();

        // fail `root_1_1_1`, the supervision msg should be propagated to
        // `root_1` because `root_1` has set `true` to `handle_supervision_event`.
        root_1_1_1
            .send::<String>(&client, "some random failure".into())
            .unwrap();

        // fail `root_2_1`, the supervision msg should be propagated to
        // ProcSupervisionCoordinator.
        root_2_1
            .send::<String>(&client, "some random failure".into())
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(!root_state.load(Ordering::SeqCst));
        assert!(root_1_state.load(Ordering::SeqCst));
        assert!(!root_1_1_state.load(Ordering::SeqCst));
        assert!(!root_1_1_1_state.load(Ordering::SeqCst));
        assert!(!root_2_state.load(Ordering::SeqCst));
        assert!(!root_2_1_state.load(Ordering::SeqCst));
        assert_eq!(
            reported_event.event().map(|e| e.actor_id.clone()),
            Some(root_2_1.actor_id().clone())
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_instance() {
        #[derive(Debug, Default)]
        struct TestActor;

        impl Actor for TestActor {}

        #[async_trait]
        impl Handler<(String, reference::PortRef<String>)> for TestActor {
            async fn handle(
                &mut self,
                cx: &crate::Context<Self>,
                (message, port): (String, reference::PortRef<String>),
            ) -> anyhow::Result<()> {
                port.send(cx, message)?;
                Ok(())
            }
        }

        let proc = Proc::local();

        let (instance, handle) = proc.instance("my_test_actor").unwrap();

        let child_actor = TestActor.spawn(&instance).unwrap();

        let (port, mut receiver) = instance.open_port();
        child_actor
            .send(&instance, ("hello".to_string(), port.bind()))
            .unwrap();

        let message = receiver.recv().await.unwrap();
        assert_eq!(message, "hello");

        child_actor.drain_and_stop("test").unwrap();
        child_actor.await;

        assert_eq!(*handle.status().borrow(), ActorStatus::Client);
        drop(instance);
        assert_matches!(*handle.status().borrow(), ActorStatus::Stopped(_));
        handle.await;
    }

    #[tokio::test]
    async fn test_proc_terminate_without_coordinator() {
        if std::env::var("CARGO_TEST").is_ok() {
            eprintln!("test skipped as it hangs when run by cargo in sandcastle");
            return;
        }

        let process = async {
            let proc = Proc::local();
            // Intentionally not setting a proc supervison coordinator. This
            // should cause the process to terminate.
            // ProcSupervisionCoordinator::set(&proc).await.unwrap();
            let root = proc.spawn("root", TestActor).unwrap();
            let (client, _handle) = proc.instance("client").unwrap();
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
                handle.send(cx, barrier.clone()).unwrap();
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
            let proc = Proc::local();
            let (client, _) = proc.instance("client").unwrap();
            let handle = LoggingActor.spawn_detached().unwrap();
            handle.send(&client, "hello world".to_string()).unwrap();
            handle
                .send(&client, "hello world again".to_string())
                .unwrap();
            handle.send(&client, 123u64).unwrap();

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
                handle.send(&client, Arc::clone(&barriers)).unwrap();
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
        use crate::actor::ActorStatus;
        use crate::mailbox::MailboxErrorKind;
        use crate::mailbox::MailboxSenderErrorKind;

        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let actor_handle = proc.spawn("test", TestActor).unwrap();

        // Clone the handle before awaiting since await consumes the handle
        let handle_for_send = actor_handle.clone();

        // Stop the actor gracefully
        actor_handle.drain_and_stop("healthy shutdown").unwrap();
        actor_handle.await;

        // Try to send a message to the stopped actor
        let result = handle_for_send.send(&client, TestActorMessage::Noop());

        assert!(result.is_err(), "send should fail when actor is stopped");
        let err = result.unwrap_err();
        assert_matches!(
            err.kind(),
            MailboxSenderErrorKind::Mailbox(mailbox_err)
                if matches!(
                    mailbox_err.kind(),
                    MailboxErrorKind::OwnerTerminated(ActorStatus::Stopped(reason)) if reason == "healthy shutdown"
                )
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_mailbox_closed_with_owner_failed_reason() {
        use crate::actor::ActorErrorKind;
        use crate::actor::ActorStatus;
        use crate::mailbox::MailboxErrorKind;
        use crate::mailbox::MailboxSenderErrorKind;

        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        // Need to set a supervison coordinator for this Proc because there will
        // be actor failure(s) in this test which trigger supervision.
        let (_reported, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let actor_handle = proc.spawn("test", TestActor).unwrap();

        // Clone the handle before awaiting since await consumes the handle
        let handle_for_send = actor_handle.clone();

        // Cause the actor to fail
        actor_handle
            .send(
                &client,
                TestActorMessage::Fail(anyhow::anyhow!("intentional failure")),
            )
            .unwrap();
        actor_handle.await;

        // Try to send a message to the failed actor
        let result = handle_for_send.send(&client, TestActorMessage::Noop());

        assert!(result.is_err(), "send should fail when actor has failed");
        let err = result.unwrap_err();
        assert_matches!(
            err.kind(),
            MailboxSenderErrorKind::Mailbox(mailbox_err)
                if matches!(
                    mailbox_err.kind(),
                    MailboxErrorKind::OwnerTerminated(ActorStatus::Failed(ActorErrorKind::Generic(msg)))
                        if msg.contains("intentional failure")
                )
        );
    }

    /// Wait for a terminated snapshot to appear for the given actor.
    /// The introspect task runs in a separate tokio task and may not
    /// have stored the snapshot by the time `handle.await` returns.
    async fn wait_for_terminated_snapshot(
        proc: &Proc,
        actor_id: &reference::ActorId,
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
    #[async_timed_test(timeout_secs = 30)]
    async fn test_terminated_snapshot_stored_on_stop() {
        let proc = Proc::local();
        let (_client, _client_handle) = proc.instance("client").unwrap();

        let handle = proc.spawn::<TestActor>("actor", TestActor).unwrap();
        let actor_id = handle.actor_id().clone();

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
            serde_json::from_str(&snapshot.attrs).expect("attrs must be valid JSON");
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
    #[async_timed_test(timeout_secs = 30)]
    async fn test_terminated_snapshot_stored_on_failure() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        // Supervision coordinator required for actor failure handling.
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn::<TestActor>("fail_actor", TestActor).unwrap();
        let actor_id = handle.actor_id().clone();

        // Trigger a failure.
        handle
            .send(&client, TestActorMessage::Fail(anyhow::anyhow!("boom")))
            .unwrap();
        handle.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &actor_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("attrs must be valid JSON");
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
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn::<TestActor>("fail_actor", TestActor).unwrap();
        let actor_id = handle.actor_id().clone();
        let cell = handle.cell().clone();

        handle
            .send(&client, TestActorMessage::Fail(anyhow::anyhow!("boom")))
            .unwrap();
        handle.await;

        let event = cell.supervision_event();
        assert!(event.is_some(), "failed actor must have supervision_event");
        let event = event.unwrap();
        assert_eq!(event.actor_id, actor_id);
        assert!(event.actor_status.is_failed());
        // Originated here, not propagated.
        assert_eq!(event.actually_failing_actor().actor_id, actor_id);
    }

    // Exercises FI-2 (see introspect.rs module-scope comment).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_supervision_event_on_clean_stop() {
        let proc = Proc::local();
        let (_client, _client_handle) = proc.instance("client").unwrap();

        let handle = proc.spawn::<TestActor>("stop_actor", TestActor).unwrap();
        let cell = handle.cell().clone();

        handle.drain_and_stop("test").unwrap();
        handle.await;

        let event = cell
            .supervision_event()
            .expect("cleanly stopped actor must have a supervision_event");
        assert!(
            matches!(event.actor_status, ActorStatus::Stopped(_)),
            "expected Stopped status, got {:?}",
            event.actor_status
        );
        assert!(!event.is_error());
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_supervision_coordinator_receives_clean_stop() {
        let proc = Proc::local();
        let (_client, _client_handle) = proc.instance("client").unwrap();
        let (mut reported_event, _coordinator_handle) =
            ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn::<TestActor>("stop_actor", TestActor).unwrap();
        let actor_id = handle.actor_id().clone();

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

    // Exercises FI-4 (see introspect.rs module-scope comment).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_supervision_event_on_propagated_failure() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let parent = proc.spawn::<TestActor>("parent", TestActor).unwrap();
        let parent_cell = parent.cell().clone();
        // Spawn child under parent.
        let (tx, rx) = oneshot::channel();
        parent.send(&client, TestActorMessage::Spawn(tx)).unwrap();
        let child = rx.await.unwrap();
        let child_id = child.actor_id().clone();

        // Fail the child — parent doesn't handle supervision, so it
        // propagates and terminates too.
        child
            .send(
                &client,
                TestActorMessage::Fail(anyhow::anyhow!("child boom")),
            )
            .unwrap();
        parent.await;

        let event = parent_cell.supervision_event();
        assert!(
            event.is_some(),
            "parent must have supervision_event from propagated failure"
        );
        let event = event.unwrap();
        // Root cause is the child, not the parent.
        assert_eq!(event.actually_failing_actor().actor_id, child_id);
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
        let proc = Proc::local();
        let (_client, _client_handle) = proc.instance("client").unwrap();

        let handle = proc.spawn::<TestActor>("target", TestActor).unwrap();
        let actor_ref: reference::ActorRef<TestActor> = handle.bind();

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
    #[async_timed_test(timeout_secs = 30)]
    async fn test_terminated_snapshot_has_failure_info() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let handle = proc.spawn::<TestActor>("fail_actor", TestActor).unwrap();
        let actor_id = handle.actor_id().clone();

        handle
            .send(&client, TestActorMessage::Fail(anyhow::anyhow!("kaboom")))
            .unwrap();
        handle.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &actor_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("attrs must be valid JSON");
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
        assert_eq!(root_cause, &actor_id.to_string());
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
    #[async_timed_test(timeout_secs = 30)]
    async fn test_propagated_failure_info() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        ProcSupervisionCoordinator::set(&proc).await.unwrap();

        let parent = proc.spawn::<TestActor>("parent", TestActor).unwrap();
        let parent_id = parent.actor_id().clone();

        let (tx, rx) = oneshot::channel();
        parent.send(&client, TestActorMessage::Spawn(tx)).unwrap();
        let child = rx.await.unwrap();
        let child_id = child.actor_id().clone();

        child
            .send(
                &client,
                TestActorMessage::Fail(anyhow::anyhow!("child fail")),
            )
            .unwrap();
        parent.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &parent_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("attrs must be valid JSON");
        let root_cause = attrs
            .get(crate::introspect::FAILURE_ROOT_CAUSE_ACTOR)
            .expect("propagated failure must have root_cause_actor");
        assert_eq!(root_cause, &child_id.to_string());
        assert_eq!(
            attrs.get(crate::introspect::FAILURE_IS_PROPAGATED),
            Some(&true)
        );
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_creates_descriptive_name() {
        let proc = Proc::local();
        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let handle = proc
            .spawn_named_child(root.cell().clone(), "my_controller", TestActor)
            .unwrap();
        assert_eq!(handle.actor_id().name(), "my_controller");
        assert_eq!(handle.actor_id().pid(), 1);
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_increments_index() {
        let proc = Proc::local();
        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let first = proc
            .spawn_named_child(root.cell().clone(), "my_controller", TestActor)
            .unwrap();
        let second = proc
            .spawn_named_child(root.cell().clone(), "my_controller", TestActor)
            .unwrap();
        assert_eq!(first.actor_id().pid(), 1);
        assert_eq!(second.actor_id().pid(), 2);
    }

    /// Exercises AI-1 (see module doc).
    /// spawn_named_child passes Some(parent) to spawn_inner.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_preserves_supervision() {
        let proc = Proc::local();
        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let child = proc
            .spawn_named_child(root.cell().clone(), "supervised_child", TestActor)
            .unwrap();
        let child_cell = child.cell();
        let parent = child_cell.parent().expect("named child must have a parent");
        assert_eq!(parent.actor_id(), root.actor_id());
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_unchanged() {
        let proc = Proc::local();
        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let child = proc.spawn_child(root.cell().clone(), TestActor).unwrap();
        assert_eq!(child.actor_id().name(), root.actor_id().name());
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_different_names_different_pids() {
        let proc = Proc::local();
        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let a = proc
            .spawn_named_child(root.cell().clone(), "controller_a", TestActor)
            .unwrap();
        let b = proc
            .spawn_named_child(root.cell().clone(), "controller_b", TestActor)
            .unwrap();
        assert_ne!(a.actor_id().pid(), b.actor_id().pid());
        assert_eq!(a.actor_id().name(), "controller_a");
        assert_eq!(b.actor_id().name(), "controller_b");
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_no_child_overwrite() {
        let proc = Proc::local();
        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let _a = proc
            .spawn_named_child(root.cell().clone(), "ctrl", TestActor)
            .unwrap();
        let _b = proc
            .spawn_named_child(root.cell().clone(), "ctrl", TestActor)
            .unwrap();
        let _c = proc.spawn_child(root.cell().clone(), TestActor).unwrap();
        assert_eq!(root.cell().child_count(), 3);
    }

    /// Exercises AI-1 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_with_name_does_not_pollute_roots() {
        let proc = Proc::local();
        let root = proc.spawn::<TestActor>("root", TestActor).unwrap();
        let _child = proc
            .spawn_named_child(root.cell().clone(), "foo", TestActor)
            .unwrap();
        // "foo" was used as a named child name but should NOT
        // prevent spawning a root actor with that name.
        let result = proc.spawn::<TestActor>("foo", TestActor);
        assert!(result.is_ok(), "named child should not pollute roots");
    }

    /// Exercises AI-3 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ai3_controller_actor_ids_unique_across_parents_same_proc() {
        let proc = Proc::local();
        let parent_a = proc.spawn::<TestActor>("parent_a", TestActor).unwrap();
        let parent_b = proc.spawn::<TestActor>("parent_b", TestActor).unwrap();

        // Simulate the correct pattern: include mesh identity in name.
        let ctrl_a = proc
            .spawn_named_child(parent_a.cell().clone(), "controller_mesh_a", TestActor)
            .unwrap();
        let ctrl_b = proc
            .spawn_named_child(parent_b.cell().clone(), "controller_mesh_b", TestActor)
            .unwrap();

        assert_ne!(
            ctrl_a.actor_id(),
            ctrl_b.actor_id(),
            "controller ActorIds must be unique across parents"
        );
    }

    /// Exercises AI-3 (see module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_ai3_no_controller_overwrite_in_parent_or_proc_maps() {
        let proc = Proc::local();
        let parent_a = proc.spawn::<TestActor>("parent_a", TestActor).unwrap();
        let parent_b = proc.spawn::<TestActor>("parent_b", TestActor).unwrap();

        let ctrl_a = proc
            .spawn_named_child(parent_a.cell().clone(), "controller_mesh_a", TestActor)
            .unwrap();
        let ctrl_b = proc
            .spawn_named_child(parent_b.cell().clone(), "controller_mesh_b", TestActor)
            .unwrap();

        // Both must be independently resolvable via the proc's instances.
        assert!(
            proc.get_instance(ctrl_a.actor_id()).is_some(),
            "ctrl_a must be resolvable"
        );
        assert!(
            proc.get_instance(ctrl_b.actor_id()).is_some(),
            "ctrl_b must be resolvable"
        );
        // Parents each see exactly one child.
        assert_eq!(parent_a.cell().child_count(), 1);
        assert_eq!(parent_b.cell().child_count(), 1);
    }

    // Exercises FI-6 (see introspect module doc).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_stopped_snapshot_has_no_failure_info() {
        let proc = Proc::local();
        let (_client, _client_handle) = proc.instance("client").unwrap();

        let handle = proc.spawn::<TestActor>("stop_actor", TestActor).unwrap();
        let actor_id = handle.actor_id().clone();

        handle.drain_and_stop("test").unwrap();
        handle.await;

        let snapshot = wait_for_terminated_snapshot(&proc, &actor_id).await;
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&snapshot.attrs).expect("attrs must be valid JSON");
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
}
