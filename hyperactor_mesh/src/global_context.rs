/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Process-global context, root client actor, and supervision bridge.
//!
//! This module provides the Rust equivalent of Python's `context()`,
//! `this_host()`, and `this_proc()`. A singleton [`Host`] is lazily
//! created with the [`GlobalClientActor`] on its `local_proc`:
//!
//! ```rust,ignore
//! let cx = context().await;
//! cx.actor_instance    // c.f. Python: context().actor_instance
//! this_host().await    // c.f. Python: this_host()
//! this_proc().await    // c.f Python: this_proc()
//! ```
//!
//! ## Undeliverables → supervision
//!
//! When the runtime detects that a message cannot be delivered, it
//! produces an [`Undeliverable<MessageEnvelope>`]. The global root
//! client observes these failures, converts them into
//! [`ActorSupervisionEvent`]s, and forwards them to the currently
//! active mesh supervision sink.
//!
//! **Invariant:** Any `Undeliverable<MessageEnvelope>` observed by
//! the global root client must be reported as an
//! [`ActorSupervisionEvent`] to the active `ProcMesh`, and handling
//! that failure must never crash the global client. The root client
//! acts as a monitor, not a participant: routing failures are treated
//! as signals to be reported, not fatal errors.
//!
//! ## Multiple ProcMeshes
//!
//! A process may allocate more than one `ProcMesh` (e.g.
//! internal/controller meshes plus an application mesh). The root
//! client is a process-wide singleton, so its supervision sink is
//! also process-global.
//!
//! The active mesh is defined using **last-sink-wins** semantics:
//! each newly allocated `ProcMesh` installs its sink, overriding the
//! previous one.
//!
//! If no sink has been installed yet (early/late binding),
//! undeliverables are logged and dropped, preserving forward progress
//! until a mesh becomes available.

use std::sync::OnceLock;
use std::sync::RwLock;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Signal;
use hyperactor::host::Host;
use hyperactor::host::LocalProcManager;
use hyperactor::mailbox::DeliveryError;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::PortReceiver;
use hyperactor::mailbox::Undeliverable;
use hyperactor::proc::Proc;
use hyperactor::proc::WorkCell;
use hyperactor::reference::PortRef;
use hyperactor::supervision::ActorSupervisionEvent;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::HostMeshRef;
use crate::Name;
use crate::host_mesh::host_agent::GetLocalProcClient;
use crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
use crate::host_mesh::host_agent::HostAgent;
use crate::host_mesh::host_agent::HostAgentMode;
use crate::host_mesh::host_agent::ProcManagerSpawnFn;
use crate::proc_agent::GetProcClient;
use crate::proc_agent::ProcAgent;
use crate::proc_mesh::ProcMeshRef;
use crate::proc_mesh::ProcRef;
use crate::supervision::MeshFailure;
use crate::transport::default_bind_spec;

/// Single, process-wide supervision sink storage.
///
/// Routes undeliverables observed by the process-global root client
/// (c.f. [`context()`]) to the *currently active* `ProcMesh`'s
/// agent. Newer meshes override older ones ("last sink wins").
///
/// Uses `PortRef` (not `PortHandle`) because the sink target
/// (`ProcAgent`) runs in a remote worker process.
static GLOBAL_SUPERVISION_SINK: OnceLock<RwLock<Option<PortRef<ActorSupervisionEvent>>>> =
    OnceLock::new();

/// Returns the lazily-initialized container that holds the current
/// process-global supervision sink.
fn sink_cell() -> &'static RwLock<Option<PortRef<ActorSupervisionEvent>>> {
    GLOBAL_SUPERVISION_SINK.get_or_init(|| RwLock::new(None))
}

/// Install (or replace) the process-global supervision sink used by
/// the [`context()`] undeliverable → supervision bridge.
///
/// This uses **last-sink-wins** semantics: if multiple `ProcMesh`
/// instances are created in the same process (e.g. controller meshes
/// plus an application mesh), the most recently installed sink
/// becomes the active destination for forwarded
/// [`ActorSupervisionEvent`]s.
///
/// Returns the previously installed sink, if any, to allow callers to
/// log/inspect overrides.
///
/// Note: the sink is a [`PortRef`] (not a `PortHandle`) because the
/// destination [`ProcAgent`] may live in a different
/// process/rank.
pub(crate) fn set_global_supervision_sink(
    sink: PortRef<ActorSupervisionEvent>,
) -> Option<PortRef<ActorSupervisionEvent>> {
    let cell = sink_cell();
    let mut guard = cell.write().unwrap();
    let prev = guard.take();
    *guard = Some(sink);
    prev
}

/// Get the current process-global supervision sink used by the
/// [`context()`] undeliverable → supervision bridge.
///
/// Returns `None` until some mesh creation path installs a sink
/// (early/late binding). Callers should treat this as "no active mesh
/// yet": log and drop undeliverables rather than crashing the global
/// root client.
///
/// Cloning a [`PortRef`] is cheap.
///
/// Used only by the process-global root client.
fn get_global_supervision_sink() -> Option<PortRef<ActorSupervisionEvent>> {
    sink_cell().read().unwrap().clone()
}

/// Process-global "root client" actor.
///
/// This actor lives on the `local_proc` of the singleton [`Host`]
/// created by [`context()`], symmetric with Python's
/// `RootClientActor` on `bootstrap_host()`'s local proc.
///
/// It acts as a *monitor* for routing failures observed at the
/// process boundary: undeliverable messages are treated as signals to
/// be reported via mesh supervision (when a sink is installed), not
/// as fatal errors.
///
/// The actor is driven by `run()`, which `select!`s over:
/// - `work_rx`: the primary dispatch queue for bound handler work
///   items (including `Undeliverable<MessageEnvelope>` and
///   `MeshFailure>`),
/// - `supervision_rx`: supervision events delivered to this actor,
///   and
/// - `signal_rx`: control signals (currently minimal handling).
#[derive(Debug)]
#[hyperactor::export(handlers = [MeshFailure])]
pub struct GlobalClientActor {
    /// Control signals for the actor's proc (shutdown, etc.).
    signal_rx: PortReceiver<Signal>,
    /// Supervision events delivered to this actor instance.
    ///
    /// The root client is a monitor, so it should process these
    /// events without crashing on routine routing/delivery failures
    /// it observes.
    supervision_rx: PortReceiver<ActorSupervisionEvent>,
    /// Primary work queue for handler dispatch.
    ///
    /// Any bound handler message (e.g. `MeshFailure`,
    /// `Undeliverable<MessageEnvelope>`, introspection, etc.) is
    /// received here and executed via `WorkCell::handle`.
    work_rx: mpsc::UnboundedReceiver<WorkCell<Self>>,
}

impl GlobalClientActor {
    fn run(mut self, instance: &'static Instance<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            #[allow(unused_labels)]
            let err = 'messages: loop {
                tokio::select! {
                    work = self.work_rx.recv() => {
                        let work = work.expect("inconsistent work queue state");
                        if let Err(err) = work.handle(&mut self, instance).await {
                            for supervision_event in self.supervision_rx.drain() {
                                instance.handle_supervision_event(&mut self, supervision_event).await
                                    .expect("GlobalClientActor::handle_supervision_event is infallible");
                            }
                            let kind = ActorErrorKind::processing(err);
                            break ActorError {
                                actor_id: Box::new(instance.self_id().clone()),
                                kind: Box::new(kind),
                            };
                        }
                    }
                    _ = self.signal_rx.recv() => {
                        // TODO: do we need any signal handling for the root client?
                    }
                    Ok(supervision_event) = self.supervision_rx.recv() => {
                        instance.handle_supervision_event(&mut self, supervision_event).await
                            .expect("GlobalClientActor::handle_supervision_event is infallible");
                    }
                };
            };
            let event = match *err.kind {
                ActorErrorKind::UnhandledSupervisionEvent(event) => *event,
                _ => {
                    let status = ActorStatus::generic_failure(err.kind.to_string());
                    ActorSupervisionEvent::new(
                        instance.self_id().clone(),
                        Some("testclient".into()),
                        status,
                        None,
                    )
                }
            };
            instance
                .proc()
                .handle_unhandled_supervision_event(instance, event);
        })
    }
}

/// Handle a returned (undeliverable) message observed by the
/// process-global root client.
///
/// The global root client is a **monitor**, not a participant: it
/// must not crash or propagate failures just because a routed message
/// could not be delivered.
///
/// Instead, we translate the undeliverable into an
/// `ActorSupervisionEvent` and forward it to the **active**
/// `ProcMesh` via the process-global supervision sink ("last sink
/// wins"). If no sink has been installed yet (e.g., before the first
/// `ProcMesh` allocation completes), we log and drop the event.
#[async_trait]
impl Actor for GlobalClientActor {
    /// The global root client is the root of the supervision tree:
    /// there is no parent to escalate to. Child-actor failures (e.g.
    /// ActorMeshControllers detecting dead procs after mesh teardown)
    /// are expected and must not crash the process.
    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        tracing::warn!(
            %event,
            "global root client absorbed child supervision event",
        );
        Ok(true)
    }

    async fn handle_undeliverable_message(
        &mut self,
        cx: &Instance<Self>,
        Undeliverable(mut env): Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        env.set_error(DeliveryError::BrokenLink(
            "message returned to global root client".to_string(),
        ));
        let actor_id = env.dest().actor_id().clone();
        let headers = env.headers().clone();
        let event = ActorSupervisionEvent::new(
            actor_id.clone(),
            None,
            ActorStatus::generic_failure(format!("message not delivered: {}", env)),
            Some(headers),
        );

        match get_global_supervision_sink() {
            Some(sink) => {
                if let Err(e) = sink.send(cx, event) {
                    tracing::warn!(
                        %e,
                        actor=%actor_id,
                        "failed to forward supervision event from undeliverable"
                    );
                }
            }
            None => {
                tracing::warn!(
                    actor=%actor_id,
                    error=?env.errors(),
                    "no supervision sink; undeliverable message logged but not forwarded"
                );
            }
        }
        Ok(())
    }
}

/// `MeshFailure` is a terminal supervision signal for an `ActorMesh`.
///
/// The process-global root client should never be a consumer of
/// mesh-level supervision failures during normal operation: those
/// events are expected to be observed and handled by the owning
/// mesh/controller, not by the global client.
///
/// In processes that create and destroy multiple meshes (e.g.,
/// benchmarks), `MeshFailure` events may arrive here during or after
/// mesh teardown. Log loudly but do not crash — the global client is
/// a monitor and must preserve forward progress.
#[async_trait]
impl Handler<MeshFailure> for GlobalClientActor {
    async fn handle(&mut self, _cx: &Context<Self>, message: MeshFailure) -> anyhow::Result<()> {
        tracing::error!("supervision failure reached global client: {}", message);
        Ok(())
    }
}

struct GlobalState {
    actor_instance: &'static Instance<GlobalClientActor>,
    host_mesh: HostMeshRef,
    proc_mesh: ProcMeshRef,
}

/// Process-global, lazily-initialized Monarch context.
///
/// Backed by a `tokio::sync::OnceCell` so initialization is async and
/// runs at most once per process. The first caller bootstraps the
/// singleton host and root client actor (mirroring Python's
/// `bootstrap_host()` / `context()`), and subsequent callers reuse
/// the same `GlobalState`.
///
/// This provides a stable root `actor_instance` plus `this_host()` /
/// `this_proc()` accessors.
static GLOBAL_CONTEXT: tokio::sync::OnceCell<GlobalState> = tokio::sync::OnceCell::const_new();

/// Bootstrap the singleton Host and GlobalClientActor. Mirrors
/// Python's `bootstrap_host()` (monarch_hyperactor/src/host_mesh.rs).
async fn bootstrap_host() -> GlobalState {
    // 1. Create Host with LocalProcManager. The spawn closure is the
    // ProcAgent boot function, called by HostAgent on GetLocalProc.
    let spawn: ProcManagerSpawnFn =
        Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
    let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
    let host = Host::new(manager, default_bind_spec().binding_addr())
        .await
        .expect("failed to create global host");

    // 2. Extract system_proc before moving Host into HostAgent.
    let system_proc = host.system_proc().clone();

    // 3. Spawn HostAgent on system_proc (takes ownership of Host).
    let host_agent = system_proc
        .spawn(
            HOST_MESH_AGENT_ACTOR_NAME,
            HostAgent::new(HostAgentMode::Local(host)),
        )
        .expect("failed to spawn host agent");

    // 4. Build HostMeshRef.
    let host_mesh =
        HostMeshRef::from_host_agent(Name::new_reserved("local").unwrap(), host_agent.bind())
            .expect("failed to create host mesh ref");

    // 5. Get local_proc via HostAgent (lazily boots ProcAgent).
    //
    // We use a throwaway Proc::local() for the bootstrap request-reply
    // calls, matching Python's bootstrap_host() (host_mesh.rs:330-333).
    // This creates a temporary in-process-only proc context during init
    // — intentionally acceptable for cross-language symmetry and easier
    // reasoning about the bootstrap sequence.
    let temp_proc = Proc::local();
    let (bootstrap_cx, _guard) = temp_proc
        .instance("bootstrap")
        .expect("failed to create bootstrap instance");
    let local_proc_agent: ActorHandle<ProcAgent> = host_agent
        .get_local_proc(&bootstrap_cx)
        .await
        .expect("failed to get local proc agent");

    // 6. Get the actual Proc object.
    let local_proc = local_proc_agent
        .get_proc(&bootstrap_cx)
        .await
        .expect("failed to get local proc");

    // 7. Build ProcMeshRef.
    let proc_mesh = ProcMeshRef::new_singleton(
        Name::new_reserved("local").unwrap(),
        ProcRef::new(
            local_proc_agent.actor_id().proc_id().clone(),
            0,
            local_proc_agent.bind(),
        ),
    );
    let actor_instance = local_proc
        .actor_instance::<GlobalClientActor>("client")
        .expect("failed to create root client instance");

    let hyperactor::proc::ActorInstance {
        instance: client_instance,
        handle,
        supervision,
        signal,
        work,
    } = actor_instance;

    // GlobalClientActor uses a custom run loop that bypasses
    // Actor::init, so set_system() must be called explicitly.
    client_instance.set_system();
    handle.bind::<GlobalClientActor>();

    // Use a static OnceLock to get 'static lifetime for the instance.
    static INSTANCE: OnceLock<(Instance<GlobalClientActor>, ActorHandle<GlobalClientActor>)> =
        OnceLock::new();
    INSTANCE
        .set((client_instance, handle))
        .map_err(|_| "already initialized root client instance")
        .unwrap();
    let (instance, _handle) = INSTANCE.get().unwrap();

    let client = GlobalClientActor {
        signal_rx: signal,
        supervision_rx: supervision,
        work_rx: work,
    };
    client.run(instance);

    GlobalState {
        actor_instance: instance,
        host_mesh,
        proc_mesh,
    }
}

/// Process-global Monarch context for Rust programs. Symmetric with
/// Python's `context()`.
pub struct GlobalContext {
    /// Consistent with Python's `context().actor_instance`
    pub actor_instance: &'static Instance<GlobalClientActor>,
    /// The singleton HostMesh. See also [`this_host()`].
    pub host_mesh: &'static HostMeshRef,
    /// The local ProcMesh. See also [`this_proc()`].
    pub proc_mesh: &'static ProcMeshRef,
}

/// Returns the process-global Monarch context, lazily initialized.
///
/// On first call, creates a singleton [`Host`] and bootstraps
/// [`GlobalClientActor`] on its `local_proc` — symmetric with
/// Python's `bootstrap_host()`. Subsequent calls return immediately.
///
/// ```rust,ignore
/// let cx = context().await;
/// cx.actor_instance    // c.f. Python: context().actor_instance
/// ```
///
/// **Python programs do not use this.** Python has its own root
/// client actor bootstrapped separately.
pub async fn context() -> GlobalContext {
    let state = GLOBAL_CONTEXT.get_or_init(bootstrap_host).await;
    GlobalContext {
        actor_instance: state.actor_instance,
        host_mesh: &state.host_mesh,
        proc_mesh: &state.proc_mesh,
    }
}

/// Returns the singleton HostMesh c.f. Python's `this_host()`.
pub async fn this_host() -> &'static HostMeshRef {
    &GLOBAL_CONTEXT.get_or_init(bootstrap_host).await.host_mesh
}

/// Returns the local ProcMesh c.f Python's `this_proc()`.
pub async fn this_proc() -> &'static ProcMeshRef {
    &GLOBAL_CONTEXT.get_or_init(bootstrap_host).await.proc_mesh
}

/// Separate storage for client host registered by non-Rust runtimes
/// (e.g. Python's `bootstrap_host()`). Checked by `try_this_host()`
/// alongside `GLOBAL_CONTEXT`.
static REGISTERED_CLIENT_HOST: std::sync::OnceLock<HostMeshRef> = std::sync::OnceLock::new();

/// Register the client host mesh from an external runtime (Python).
/// Called by Python's `bootstrap_host()` so that `try_this_host()`
/// can discover C for the A/C invariant.
pub fn register_client_host(host_mesh: HostMeshRef) {
    let _ = REGISTERED_CLIENT_HOST.set(host_mesh);
}

/// Returns the client host mesh if available, without triggering
/// lazy bootstrap. Checks both the Rust global context and the
/// external registration (Python). Used by `MeshAdminAgent` to
/// discover C at query time (A/C invariant).
pub fn try_this_host() -> Option<&'static HostMeshRef> {
    GLOBAL_CONTEXT
        .get()
        .map(|state| &state.host_mesh)
        .or_else(|| REGISTERED_CLIENT_HOST.get())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::PortId;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::testing::ids::test_actor_id;
    use hyperactor_config::Flattrs;
    use ndslice::extent;

    use super::*;
    use crate::testing;

    /// Helper: send an `Undeliverable<MessageEnvelope>` to the global
    /// root client's well-known undeliverable port via the runtime's
    /// routing/dispatch path.
    ///
    /// This exercises the full integration boundary: serialisation →
    /// routing → work_rx dispatch → `handle_undeliverable_message`.
    ///
    /// Uses the provided `dest_actor` so callers can distinguish
    /// events from different injections (important because the global
    /// sink is shared across tests running in the same process).
    fn inject_undeliverable(
        client: &'static Instance<GlobalClientActor>,
        dest_actor: hyperactor::ActorId,
    ) {
        let env = MessageEnvelope::new(
            client.self_id().clone(),
            PortId::new(dest_actor, 0),
            wirevalue::Any::serialize(&0u64).unwrap(),
            Flattrs::new(),
        );
        // Target the global root client's well-known Undeliverable port.
        let undeliverable_port =
            PortRef::<Undeliverable<MessageEnvelope>>::attest_message_port(client.self_id());
        undeliverable_port
            .send(client, Undeliverable(env))
            .expect("inject_undeliverable: send failed");
    }

    /// Verifies that creating a `ProcMesh` installs the
    /// process-global supervision sink used by the global root
    /// client.
    #[tokio::test]
    async fn test_sink_installed_after_mesh_creation() {
        let (_mesh, _actor, _router) = testing::local_proc_mesh(extent!(replica = 2)).await;
        assert!(
            get_global_supervision_sink().is_some(),
            "supervision sink must be set after ProcMesh creation"
        );
    }

    /// Proves the full forwarding pipeline:
    ///
    ///   Undeliverable<MessageEnvelope>
    ///       → GlobalClientActor::handle_undeliverable_message
    ///       → GLOBAL_SUPERVISION_SINK (PortRef)
    ///       → ActorSupervisionEvent delivered
    ///
    /// Installs a local port as the sink and verifies that the
    /// `ActorSupervisionEvent` arrives there.
    #[tokio::test]
    async fn test_undeliverable_forwarded_to_sink() {
        let cx = context().await;
        let client = cx.actor_instance;

        // Install a test sink we control.
        let (sink_handle, mut sink_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_handle.bind());

        let marker = test_actor_id("fwd_test", "marker_actor");
        inject_undeliverable(client, marker.clone());

        // The handler runs asynchronously via work_rx; wait for the
        // forwarded event with our marker.
        let event = RealClock
            .timeout(Duration::from_secs(5), async {
                loop {
                    let ev = sink_rx.recv().await.expect("sink channel closed");
                    if ev.actor_id == marker {
                        return ev;
                    }
                    // Discard stale events from other tests sharing the
                    // global sink.
                }
            })
            .await
            .expect("timed out waiting for supervision event");

        assert_eq!(
            event.actor_id, marker,
            "forwarded event must reference the undeliverable's destination actor"
        );
    }

    /// Proves last-sink-wins: when two sinks are installed in
    /// sequence, only the second receives the forwarded event.
    #[tokio::test]
    async fn test_last_sink_wins() {
        let cx = context().await;
        let client = cx.actor_instance;

        // Install sink A.
        let (sink_a_handle, _sink_a_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_a_handle.bind());

        // Install sink B (overrides A).
        let (sink_b_handle, mut sink_b_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_b_handle.bind());

        let marker = test_actor_id("last_wins", "marker_actor");
        inject_undeliverable(client, marker.clone());

        // B should receive our marked event.
        let event = RealClock
            .timeout(Duration::from_secs(5), async {
                loop {
                    let ev = sink_b_rx.recv().await.expect("sink B channel closed");
                    if ev.actor_id == marker {
                        return ev;
                    }
                }
            })
            .await
            .expect("timed out waiting for supervision event on sink B");
        assert_eq!(event.actor_id, marker);
    }

    /// Proves the global client does not crash when no sink is
    /// installed (early/late binding). The handler must log and
    /// drop gracefully, and the client must remain usable
    /// afterward.
    #[tokio::test]
    async fn test_no_crash_without_sink() {
        let cx = context().await;
        let client = cx.actor_instance;

        // Clear any previously installed sink.
        *sink_cell().write().unwrap() = None;

        // Inject an undeliverable — should not panic.
        inject_undeliverable(client, test_actor_id("no_sink", "marker_actor"));

        // Give the async handler time to run.
        RealClock.sleep(Duration::from_millis(100)).await;

        // The global client must still be alive and usable.
        // Verify by installing a new sink and sending another
        // undeliverable that arrives correctly.
        let (sink_handle, mut sink_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_handle.bind());

        let marker = test_actor_id("no_sink_recovery", "marker_actor");
        inject_undeliverable(client, marker.clone());

        let event = RealClock
            .timeout(Duration::from_secs(5), async {
                loop {
                    let ev = sink_rx.recv().await.expect("sink channel closed");
                    if ev.actor_id == marker {
                        return ev;
                    }
                }
            })
            .await
            .expect("timed out: global client crashed or stopped processing");
        assert_eq!(event.actor_id, marker);
    }
}
