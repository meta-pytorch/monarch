/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::sync::OnceLock;
use std::sync::RwLock;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Signal;
use hyperactor::channel::BindSpec;
use hyperactor::channel::ChannelTransport;
use hyperactor::context;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::PortHandle;
use hyperactor::mailbox::PortReceiver;
use hyperactor::mailbox::Undeliverable;
use hyperactor::proc::Proc;
use hyperactor::proc::WorkCell;
use hyperactor::reference::ProcId;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::global;
use ndslice::Range;
use ndslice::Shape;
use ndslice::ShapeError;
use ndslice::ViewExt;
use ndslice::view::Ranked;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::CommActor;
use crate::Mesh;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::reference::ProcMeshId;
use crate::router;
use crate::supervision::MeshFailure;
use crate::v1;
use crate::v1::Name;

pub mod mesh_agent;

declare_attrs! {
    /// Default transport type to use across the application.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_DEFAULT_TRANSPORT".to_string()),
        Some("default_transport".to_string()),
    ))
    pub attr DEFAULT_TRANSPORT: BindSpec = BindSpec::Any(ChannelTransport::Unix);
}

/// Temporary: used to support the legacy allocator-based V1 bootstrap. Should
/// be removed once we fully migrate to simple bootstrap.
///
/// Get the default transport to use across the application. Panic if BindSpec::Addr
/// is set as default transport. Since we expect BindSpec::Addr to be used only
/// with simple bootstrap, we should not see this panic in production.
pub fn default_transport() -> ChannelTransport {
    match default_bind_spec() {
        BindSpec::Any(transport) => transport,
        BindSpec::Addr(addr) => panic!("default_bind_spec() returned BindSpec::Addr({addr})"),
    }
}

/// Get the default bind spec to use across the application.
pub fn default_bind_spec() -> BindSpec {
    global::get_cloned(DEFAULT_TRANSPORT)
}

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
        crate::proc_mesh::get_global_supervision_sink,
        |env| {
            let sink_present = crate::proc_mesh::get_global_supervision_sink().is_some();
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

/// A ProcMesh maintains a mesh of procs whose lifecycles are managed by
/// an allocator.
pub struct ProcMesh {
    inner: v1::ProcMeshRef,
    shape: OnceLock<Shape>,
}

impl From<v1::ProcMeshRef> for ProcMesh {
    fn from(proc_mesh: v1::ProcMeshRef) -> Self {
        ProcMesh {
            inner: proc_mesh,
            shape: OnceLock::new(),
        }
    }
}

impl ProcMesh {
    fn agents(&self) -> Box<dyn Iterator<Item = ActorRef<ProcMeshAgent>> + '_ + Send> {
        Box::new(
            self.inner
                .agent_mesh()
                .iter()
                .map(|(_point, agent)| agent.clone())
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    /// Return the comm actor to which casts should be forwarded.
    pub(crate) fn comm_actor(&self) -> &ActorRef<CommActor> {
        self.inner.root_comm_actor().unwrap()
    }

    pub fn shape(&self) -> &Shape {
        self.shape
            .get_or_init(|| Ranked::region(&self.inner).into())
    }

    /// Send stop actors message to all mesh agents for a specific mesh name
    #[hyperactor::observe_result("ProcMesh")]
    pub async fn stop_actor_by_name(
        &self,
        cx: &impl context::Actor,
        mesh_name: &str,
        reason: &str,
    ) -> Result<(), anyhow::Error> {
        self.inner
            .stop_actor_by_name(cx, Name::new_reserved(mesh_name)?, reason.to_string())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Mesh for ProcMesh {
    type Node = ProcId;
    type Id = ProcMeshId;
    type Sliced<'a> = SlicedProcMesh<'a>;

    fn shape(&self) -> &Shape {
        ProcMesh::shape(self)
    }

    fn select<R: Into<Range>>(
        &self,
        label: &str,
        range: R,
    ) -> Result<Self::Sliced<'_>, ShapeError> {
        Ok(SlicedProcMesh(self, self.shape().select(label, range)?))
    }

    fn get(&self, rank: usize) -> Option<ProcId> {
        Ranked::get(&self.inner, rank).map(|proc| proc.proc_id().clone())
    }

    fn id(&self) -> Self::Id {
        ProcMeshId(self.inner.name().to_string())
    }
}

impl fmt::Display for ProcMesh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ shape: {} }}", self.shape())
    }
}

impl fmt::Debug for ProcMesh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

pub struct SlicedProcMesh<'a>(&'a ProcMesh, Shape);

#[async_trait]
impl Mesh for SlicedProcMesh<'_> {
    type Node = ProcId;
    type Id = ProcMeshId;
    type Sliced<'b>
        = SlicedProcMesh<'b>
    where
        Self: 'b;

    fn shape(&self) -> &Shape {
        &self.1
    }

    fn select<R: Into<Range>>(
        &self,
        label: &str,
        range: R,
    ) -> Result<Self::Sliced<'_>, ShapeError> {
        Ok(Self(self.0, self.1.select(label, range)?))
    }

    fn get(&self, _index: usize) -> Option<ProcId> {
        unimplemented!()
    }

    fn id(&self) -> Self::Id {
        self.0.id()
    }
}
