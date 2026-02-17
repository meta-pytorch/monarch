/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Process-global root client actor and supervision bridge.
//!
//! This module defines the *global root client*: a single,
//! lazily-initialized actor used by driver processes (e.g. Python
//! entrypoints) to inject messages into meshes. It runs in its own
//! proc and can be used before any specific [`ProcMesh`] is fully
//! constructed.
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
//! [`global_root_client()`] must be reported as an
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
use hyperactor::mailbox::BoxableMailboxSender;
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

use crate::router;
use crate::supervision::MeshFailure;
use crate::transport::default_bind_spec;

/// Single, process-wide supervision sink storage.
///
/// Routes undeliverables observed by the process-global root client
/// (c.f. [`global_root_client`]) to the *currently active* `ProcMesh`'s
/// agent. Newer meshes override older ones ("last sink wins").
///
/// Uses `PortRef` (not `PortHandle`) because the sink target
/// (`ProcMeshAgent`) runs in a remote worker process.
static GLOBAL_SUPERVISION_SINK: OnceLock<RwLock<Option<PortRef<ActorSupervisionEvent>>>> =
    OnceLock::new();

/// Returns the lazily-initialized container that holds the current
/// process-global supervision sink.
fn sink_cell() -> &'static RwLock<Option<PortRef<ActorSupervisionEvent>>> {
    GLOBAL_SUPERVISION_SINK.get_or_init(|| RwLock::new(None))
}

/// Install (or replace) the process-global supervision sink used by
/// the [`global_root_client()`] undeliverable → supervision bridge.
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
/// destination [`ProcMeshAgent`] may live in a different
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
/// [`global_root_client()`] undeliverable → supervision bridge.
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
/// This actor lives in a dedicated, lazily-initialized proc and is
/// used by driver code (e.g. Python entrypoints) to inject messages
/// into meshes before a specific [`ProcMesh`] is fully constructed.
///
/// It also acts as a *monitor* for routing failures observed at the
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
    /// Control signals for the actor’s proc (shutdown, etc.).
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
            let err = 'messages: loop {
                tokio::select! {
                    work = self.work_rx.recv() => {
                        let work = work.expect("inconsistent work queue state");
                        if let Err(err) = work.handle(&mut self, instance).await {
                            for supervision_event in self.supervision_rx.drain() {
                                tracing::warn!(
                                    %supervision_event,
                                    "global root client absorbed child supervision event (during error drain)",
                                );
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
                        // The global root client is the root of the
                        // supervision tree: there is no parent to
                        // escalate to.  Child-actor failures (e.g.
                        // ActorMeshControllers detecting dead procs
                        // after mesh teardown) are expected and must
                        // not crash the process.
                        tracing::warn!(
                            %supervision_event,
                            "global root client absorbed child supervision event",
                        );
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

/// Lazily create (and start) the process-global root client actor
/// instance.
///
/// This initializes the dedicated `mesh_root_client_proc`, creates
/// the `GlobalClientActor` instance within it, binds the actor's
/// well-known ports, and spawns the actor's run loop. The returned
/// references are `'static` via a `OnceLock`, ensuring there is
/// exactly one global root client per process.
///
/// The global root client is a monitor/entrypoint: it must not crash
/// due to routing or delivery failures it observes. Undeliverables
/// are handled non-fatally and (when a global supervision sink is
/// installed) are converted into `ActorSupervisionEvent`s and
/// forwarded to the active `ProcMesh`.
fn fresh_instance() -> (
    &'static Instance<GlobalClientActor>,
    &'static ActorHandle<GlobalClientActor>,
) {
    static INSTANCE: OnceLock<(Instance<GlobalClientActor>, ActorHandle<GlobalClientActor>)> =
        OnceLock::new();
    let client_proc = Proc::direct_with_default(
        default_bind_spec().binding_addr(),
        "mesh_root_client_proc".into(),
        router::global().clone().boxed(),
    )
    .unwrap();

    // Make this proc reachable through the global router, so that we can use the
    // same client in both direct-addressed and ranked-addressed modes.
    router::global().bind(client_proc.proc_id().clone().into(), client_proc.clone());

    // Drive the global root client actor.
    //
    // `work_rx` is the primary dispatch queue: messages received on
    // bound ports are enqueued as work items and executed via
    // `work.handle(..)`.
    //
    // `supervision_rx` carries supervision events delivered to this
    // actor; we process them via
    // `Instance::handle_supervision_event`. The global root client is
    // a monitor and must not crash due to routing / delivery failures
    // it observes; undeliverables are handled non-fatally.
    let (client, handle, supervision_rx, signal_rx, work_rx) = client_proc
        .actor_instance::<GlobalClientActor>("client")
        .expect("root instance create");

    // Bind the actor's well-known ports (Signal,
    // Undeliverable<MessageEnvelope>, IntrospectMessage, and the
    // MeshFailure handler). Undeliverable messages are routed to
    // GlobalClientActor::handle_undeliverable_message, which converts
    // them to ActorSupervisionEvents and forwards to the global
    // supervision sink.
    handle.bind::<GlobalClientActor>();

    // Use the OnceLock to get a 'static lifetime for the instance.
    INSTANCE
        .set((client, handle))
        .map_err(|_| "already initialized root client instance")
        .unwrap();
    let (instance, handle) = INSTANCE.get().unwrap();
    let client = GlobalClientActor {
        signal_rx,
        supervision_rx,
        work_rx,
    };
    client.run(instance);
    (instance, handle)
}

/// Context use by root client to send messages.
/// This mailbox allows us to open ports before we know which proc the
/// messages will be sent to.
pub fn global_root_client() -> &'static Instance<GlobalClientActor> {
    static GLOBAL_INSTANCE: OnceLock<(
        &'static Instance<GlobalClientActor>,
        &'static ActorHandle<GlobalClientActor>,
    )> = OnceLock::new();
    GLOBAL_INSTANCE.get_or_init(fresh_instance).0
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::PortId;
    use hyperactor::id;
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
            PortId(dest_actor, 0),
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
        let client = global_root_client();

        // Install a test sink we control.
        let (sink_handle, mut sink_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_handle.bind());

        let marker = id!(fwd_test[0].marker_actor);
        inject_undeliverable(client, marker.clone());

        // The handler runs asynchronously via work_rx; wait for the
        // forwarded event with our marker.
        let event = tokio::time::timeout(Duration::from_secs(5), async {
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
        let client = global_root_client();

        // Install sink A.
        let (sink_a_handle, _sink_a_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_a_handle.bind());

        // Install sink B (overrides A).
        let (sink_b_handle, mut sink_b_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_b_handle.bind());

        let marker = id!(last_wins[0].marker_actor);
        inject_undeliverable(client, marker.clone());

        // B should receive our marked event.
        let event = tokio::time::timeout(Duration::from_secs(5), async {
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
        let client = global_root_client();

        // Clear any previously installed sink.
        *sink_cell().write().unwrap() = None;

        // Inject an undeliverable — should not panic.
        inject_undeliverable(client, id!(no_sink[0].marker_actor));

        // Give the async handler time to run.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The global client must still be alive and usable.
        // Verify by installing a new sink and sending another
        // undeliverable that arrives correctly.
        let (sink_handle, mut sink_rx) = client.open_port::<ActorSupervisionEvent>();
        set_global_supervision_sink(sink_handle.bind());

        let marker = id!(no_sink_recovery[0].marker_actor);
        inject_undeliverable(client, marker.clone());

        let event = tokio::time::timeout(Duration::from_secs(5), async {
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
