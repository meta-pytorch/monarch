/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Global client actor and supervision sink management.

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
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::PortHandle;
use hyperactor::mailbox::PortReceiver;
use hyperactor::mailbox::Undeliverable;
use hyperactor::proc::Proc;
use hyperactor::proc::WorkCell;
use hyperactor::supervision::ActorSupervisionEvent;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::router;
use crate::supervision::MeshFailure;
use crate::transport::default_bind_spec;

/// Single, process-wide supervision sink storage.
///
/// This is a pragmatic "good enough for now" global used to route
/// undeliverables observed by the process-global root client (c.f.
/// [`global_root_client`])to the *currently active* `ProcMesh`. Newer
/// meshes override older ones ("last sink wins").
static GLOBAL_SUPERVISION_SINK: OnceLock<RwLock<Option<PortHandle<ActorSupervisionEvent>>>> =
    OnceLock::new();

/// Returns the lazily-initialized container that holds the current
/// process-global supervision sink.
///
/// Internal helper: callers should use `set_global_supervision_sink`
/// and `get_global_supervision_sink` instead.
fn sink_cell() -> &'static RwLock<Option<PortHandle<ActorSupervisionEvent>>> {
    GLOBAL_SUPERVISION_SINK.get_or_init(|| RwLock::new(None))
}

/// Install (or replace) the process-global supervision sink.
///
/// This function enforces "last sink wins" semantics: if a sink was
/// already installed, it is replaced and the previous sink is
/// returned. Called from `ProcMesh::allocate_boxed`, after creating
/// the mesh's supervision port.
///
/// Returns:
/// - `Some(prev)` if a prior sink was installed, allowing the caller
///   to log/inspect it if desired;
/// - `None` if this is the first sink.
///
/// Thread-safety: takes a write lock briefly to swap the handle.
pub(crate) fn set_global_supervision_sink(
    sink: PortHandle<ActorSupervisionEvent>,
) -> Option<PortHandle<ActorSupervisionEvent>> {
    let cell = sink_cell();
    let mut guard = cell.write().unwrap();
    let prev = guard.take();
    *guard = Some(sink);
    prev
}

/// Get a clone of the current process-global supervision sink, if
/// any.
///
/// This is used by the process-global root client [c.f.
/// `global_root_client`] to forward undeliverables once a mesh has
/// installed its sink. If no sink has been installed yet, returns
/// `None` and callers should defer/ignore forwarding until one
/// appears.
///
/// Thread-safety: takes a read lock briefly; cloning the `PortHandle`
/// is cheap.
pub(crate) fn get_global_supervision_sink() -> Option<PortHandle<ActorSupervisionEvent>> {
    sink_cell().read().unwrap().clone()
}

#[derive(Debug)]
pub struct GlobalClientActor {
    signal_rx: PortReceiver<Signal>,
    supervision_rx: PortReceiver<ActorSupervisionEvent>,
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
                                if let Err(err) = instance.handle_supervision_event(&mut self, supervision_event).await {
                                    break 'messages err;
                                }
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
                        if let Err(err) = instance.handle_supervision_event(&mut self, supervision_event).await {
                            break err;
                        }
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

impl Actor for GlobalClientActor {}

#[async_trait]
impl Handler<MeshFailure> for GlobalClientActor {
    async fn handle(&mut self, _cx: &Context<Self>, message: MeshFailure) -> anyhow::Result<()> {
        tracing::error!("supervision failure reached global client: {}", message);
        panic!("supervision failure reached global client: {}", message);
    }
}

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

    // The work_rx messages loop is ignored. v0 will support Handler<MeshFailure>,
    // but it doesn't actually handle the messages.
    // This is fine because v0 doesn't use this supervision mechanism anyway.
    let (client, handle, supervision_rx, signal_rx, work_rx) = client_proc
        .actor_instance::<GlobalClientActor>("client")
        .expect("root instance create");

    // Bind the global root client's undeliverable port and
    // forward any undeliverable messages to the currently active
    // supervision sink.
    //
    // The resolver (`get_global_supervision_sink`) is passed as a
    // function pointer, so each time an undeliverable is
    // processed, we look up the *latest* sink. This allows the
    // root client to seamlessly track whichever ProcMesh most
    // recently installed a supervision sink (e.g., the
    // application mesh instead of an internal controller mesh).
    //
    // The hook logs each undeliverable, along with whether a sink
    // was present at the time of receipt, which helps diagnose
    // lost or misrouted events.
    let (_undeliverable_tx, undeliverable_rx) =
        client.open_port::<Undeliverable<MessageEnvelope>>();
    hyperactor::mailbox::supervise_undeliverable_messages_with(
        undeliverable_rx,
        get_global_supervision_sink,
        |env| {
            let sink_present = get_global_supervision_sink().is_some();
            tracing::info!(
                actor_id = %env.dest().actor_id(),
                "global root client undeliverable observed with headers {:?} {}", env.headers(), sink_present
            );
        },
    );

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
