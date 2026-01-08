/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)] // until used publically

use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::OnceLock;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::GangId;
use hyperactor::GangRef;
use hyperactor::Message;
use hyperactor::PortHandle;
use hyperactor::ProcId;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::Unbind;
use hyperactor::WorldId;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::mailbox::MailboxSenderError;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::PortReceiver;
use hyperactor::mailbox::Undeliverable;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::attrs::Attrs;
use hyperactor_config::attrs::declare_attrs;
use ndslice::Range;
use ndslice::Selection;
use ndslice::Shape;
use ndslice::ShapeError;
use ndslice::SliceError;
use ndslice::View;
use ndslice::reshape::Limit;
use ndslice::reshape::ReshapeError;
use ndslice::reshape::ReshapeSliceExt;
use ndslice::reshape::reshape_selection;
use ndslice::selection;
use ndslice::selection::EvalOpts;
use ndslice::selection::ReifySlice;
use ndslice::selection::normal;
use ndslice::view::ViewExt;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use tokio::sync::mpsc;
use typeuri::Named;

use crate::CommActor;
use crate::Mesh;
use crate::comm::multicast::CAST_ORIGINATING_SENDER;
use crate::comm::multicast::CastMessage;
use crate::comm::multicast::CastMessageEnvelope;
use crate::comm::multicast::Uslice;
use crate::config::MAX_CAST_DIMENSION_SIZE;
use crate::metrics;
use crate::proc_mesh::ProcMesh;
use crate::reference::ActorMeshId;
use crate::reference::ActorMeshRef;
use crate::v1;

declare_attrs! {
    /// Which mesh this message was cast to. Used for undeliverable message
    /// handling, where the CastMessageEnvelope is serialized, and its content
    /// cannot be inspected.
    pub attr CAST_ACTOR_MESH_ID: ActorMeshId;
}

/// An undeliverable might have its sender address set as the comm actor instead
/// of the original sender. Update it based on the headers present in the message
/// so it matches the sender.
pub fn update_undeliverable_envelope_for_casting(
    mut envelope: Undeliverable<MessageEnvelope>,
) -> Undeliverable<MessageEnvelope> {
    let old_actor = envelope.0.sender().clone();
    // v1 casting
    if let Some(actor_id) = envelope.0.headers().get(CAST_ORIGINATING_SENDER).cloned() {
        tracing::debug!(
            actor_id = %old_actor,
            "remapped comm-actor id to id from CAST_ORIGINATING_SENDER {}", actor_id
        );
        envelope.0.update_sender(actor_id);
    // v0 casting
    } else if let Some(actor_mesh_id) = envelope.0.headers().get(CAST_ACTOR_MESH_ID) {
        match actor_mesh_id {
            ActorMeshId::V0(proc_mesh_id, actor_name) => {
                let actor_id = ActorId(
                    ProcId::Ranked(WorldId(proc_mesh_id.0.clone()), 0),
                    actor_name.clone(),
                    0,
                );
                tracing::debug!(
                    actor_id = %old_actor,
                    "remapped comm-actor id to mesh id from CAST_ACTOR_MESH_ID {}", actor_id
                );
                envelope.0.update_sender(actor_id);
            }
            ActorMeshId::V1(_) => {
                tracing::debug!("headers present but V1 ActorMeshId; leaving actor_id unchanged");
            }
        }
    } else {
        // Do nothing, it wasn't from a comm actor.
    }
    envelope
}

/// Common implementation for `ActorMesh`s and `ActorMeshRef`s to cast
/// an `M`-typed message
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
#[hyperactor::instrument]
pub(crate) fn actor_mesh_cast<A, M>(
    cx: &impl context::Actor,
    actor_mesh_id: ActorMeshId,
    comm_actor_ref: &ActorRef<CommActor>,
    selection_of_root: Selection,
    root_mesh_shape: &Shape,
    cast_mesh_shape: &Shape,
    message: M,
) -> Result<(), CastError>
where
    A: Referable + RemoteHandles<IndexedErasedUnbound<M>>,
    M: Castable + RemoteMessage,
{
    let _ = metrics::ACTOR_MESH_CAST_DURATION.start(hyperactor::kv_pairs!(
        "message_type" => M::typename(),
        "message_variant" => message.arm().unwrap_or_default(),
    ));

    let message = CastMessageEnvelope::new::<A, M>(
        actor_mesh_id.clone(),
        cx.mailbox().actor_id().clone(),
        cast_mesh_shape.clone(),
        message,
    )?;

    // Mesh's shape might have large extents on some dimensions. Those
    // dimensions would cause large fanout in our comm actor
    // implementation. To avoid that, we reshape it by increasing
    // dimensionality and limiting the extent of each dimension. Note
    // the reshape is only visible to the internal algorithm. The
    // shape that user sees maintains intact.
    //
    // For example, a typical shape is [hosts=1024, gpus=8]. By using
    // limit 8, it becomes [8, 8, 8, 2, 8] during casting. In other
    // words, it adds 3 extra layers to the comm actor tree, while
    // keeping the fanout in each layer per dimension at 8 or smaller.
    //
    // An important note here is that max dimension size != max fanout.
    // Rank 0 must send a message to all ranks at index 0 for every dimension.
    // If our reshaped shape is [8, 8, 8, 2, 8], rank 0 must send
    // 7 + 7 + 7 + 1 + 7 = 21 messages.

    let slice_of_root = root_mesh_shape.slice();

    let max_cast_dimension_size = hyperactor_config::global::get(MAX_CAST_DIMENSION_SIZE);

    let slice_of_cast = slice_of_root.reshape_with_limit(Limit::from(max_cast_dimension_size));

    let selection_of_cast =
        reshape_selection(selection_of_root, root_mesh_shape.slice(), &slice_of_cast)?;

    let cast_message = CastMessage {
        dest: Uslice {
            slice: slice_of_cast,
            selection: selection_of_cast,
        },
        message,
    };

    let mut headers = Attrs::new();
    headers.set(CAST_ACTOR_MESH_ID, actor_mesh_id);

    comm_actor_ref
        .port()
        .send_with_headers(cx, headers, cast_message)?;

    Ok(())
}

#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
pub(crate) fn cast_to_sliced_mesh<A, M>(
    cx: &impl context::Actor,
    actor_mesh_id: ActorMeshId,
    comm_actor_ref: &ActorRef<CommActor>,
    sel_of_sliced: &Selection,
    message: M,
    sliced_shape: &Shape,
    root_mesh_shape: &Shape,
) -> Result<(), CastError>
where
    A: Referable + RemoteHandles<IndexedErasedUnbound<M>>,
    M: Castable + RemoteMessage,
{
    let root_slice = root_mesh_shape.slice();

    // Casting to `*`?
    let sel_of_root = if selection::normalize(sel_of_sliced) == normal::NormalizedSelection::True {
        // Reify this view into base.
        root_slice.reify_slice(sliced_shape.slice())?
    } else {
        // No, fall back on `of_ranks`.
        let ranks = sel_of_sliced
            .eval(&EvalOpts::strict(), sliced_shape.slice())?
            .collect::<BTreeSet<_>>();
        Selection::of_ranks(root_slice, &ranks)?
    };

    // Cast.
    actor_mesh_cast::<A, M>(
        cx,
        actor_mesh_id,
        comm_actor_ref,
        sel_of_root,
        root_mesh_shape,
        sliced_shape,
        message,
    )
}

/// A mesh of actors, all of which reside on the same [`ProcMesh`].
#[async_trait]
pub trait ActorMesh: Mesh<Id = ActorMeshId> {
    /// The type of actor in the mesh.
    type Actor: Referable;

    /// Cast an `M`-typed message to the ranks selected by `sel` in
    /// this ActorMesh.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
    fn cast<M>(
        &self,
        cx: &impl context::Actor,
        selection: Selection,
        message: M,
    ) -> Result<(), CastError>
    where
        Self::Actor: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone,
    {
        if let Some(v1) = self.v1() {
            return v1
                .cast_for_tensor_engine_only_do_not_use(cx, selection, message)
                .map_err(anyhow::Error::from)
                .map_err(CastError::from);
        }
        actor_mesh_cast::<Self::Actor, M>(
            cx,                            // actor context
            self.id(),                     // actor mesh id (destination mesh)
            self.proc_mesh().comm_actor(), // comm actor
            selection,                     // the selected actors
            self.shape(),                  // root mesh shape
            self.shape(),                  // cast mesh shape
            message,                       // the message
        )
    }

    /// The ProcMesh on top of which this actor mesh is spawned.
    fn proc_mesh(&self) -> &ProcMesh;

    /// The name given to the actors in this mesh.
    fn name(&self) -> &str;

    fn world_id(&self) -> &WorldId {
        self.proc_mesh().world_id()
    }

    /// Iterate over all `ActorRef<Self::Actor>` in this mesh.
    fn iter_actor_refs(&self) -> Box<dyn Iterator<Item = ActorRef<Self::Actor>>> {
        if let Some(v1) = self.v1() {
            // We collect() here to ensure that the data are owned. Since this is a short-lived
            // shim, we'll live with it.
            return Box::new(
                v1.iter()
                    .map(|(_point, actor_ref)| actor_ref.clone())
                    .collect::<Vec<_>>()
                    .into_iter(),
            );
        }
        let gang: GangRef<Self::Actor> = GangRef::attest(GangId(
            self.proc_mesh().world_id().clone(),
            self.name().to_string(),
        ));
        Box::new(self.shape().slice().iter().map(move |rank| gang.rank(rank)))
    }

    async fn stop(&self, cx: &impl context::Actor) -> Result<(), anyhow::Error> {
        self.proc_mesh().stop_actor_by_name(cx, self.name()).await
    }

    /// Get a serializeable reference to this mesh similar to ActorHandle::bind
    fn bind(&self) -> ActorMeshRef<Self::Actor> {
        ActorMeshRef::attest(
            self.id(),
            self.shape().clone(),
            self.proc_mesh().comm_actor().clone(),
        )
    }

    /// Retrieves the v1 mesh for this v0 ActorMesh, if it is available.
    fn v1(&self) -> Option<v1::ActorMeshRef<Self::Actor>>;
}

/// Abstracts over shared and borrowed references to a [`ProcMesh`].
/// Given a shared ProcMesh, we can obtain a [`ActorMesh<'static, _>`]
/// for it, useful when lifetime must be managed dynamically.
enum ProcMeshRef<'a> {
    /// The reference is shared without requiring a reference.
    Shared(Box<dyn Deref<Target = ProcMesh> + Sync + Send>),
    /// The reference is borrowed with a parameterized
    /// lifetime.
    Borrowed(&'a ProcMesh),
}

impl Deref for ProcMeshRef<'_> {
    type Target = ProcMesh;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Shared(p) => p,
            Self::Borrowed(p) => p, // p: &ProcMesh
        }
    }
}

/// A mesh of actor instances. ActorMeshes are obtained by spawning an
/// actor on a [`ProcMesh`].
///
/// Generic bound: `A: Referable` — this type hands out typed
/// `ActorRef<A>` handles (see `ranks`), and `ActorRef` is only
/// defined for `A: Referable`.
pub struct RootActorMesh<'a, A: Referable> {
    inner: ActorMeshKind<'a, A>,
    shape: OnceLock<Shape>,
    proc_mesh: OnceLock<ProcMesh>,
    name: OnceLock<String>,
}

enum ActorMeshKind<'a, A: Referable> {
    V0 {
        proc_mesh: ProcMeshRef<'a>,
        name: String,
        ranks: Vec<ActorRef<A>>, // temporary until we remove `ArcActorMesh`.
        // The receiver of supervision events. It is None if it has been transferred to
        // an actor event observer.
        actor_supervision_rx: Option<mpsc::UnboundedReceiver<ActorSupervisionEvent>>,
    },

    V1(v1::ActorMeshRef<A>),
}

impl<'a, A: Referable> From<v1::ActorMeshRef<A>> for RootActorMesh<'a, A> {
    fn from(actor_mesh: v1::ActorMeshRef<A>) -> Self {
        Self {
            inner: ActorMeshKind::V1(actor_mesh),
            shape: OnceLock::new(),
            proc_mesh: OnceLock::new(),
            name: OnceLock::new(),
        }
    }
}

impl<'a, A: Referable> From<v1::ActorMesh<A>> for RootActorMesh<'a, A> {
    fn from(actor_mesh: v1::ActorMesh<A>) -> Self {
        actor_mesh.detach().into()
    }
}

impl<'a, A: Referable> RootActorMesh<'a, A> {
    pub(crate) fn new(
        proc_mesh: &'a ProcMesh,
        name: String,
        actor_supervision_rx: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
        ranks: Vec<ActorRef<A>>,
    ) -> Self {
        Self {
            inner: ActorMeshKind::V0 {
                proc_mesh: ProcMeshRef::Borrowed(proc_mesh),
                name,
                ranks,
                actor_supervision_rx: Some(actor_supervision_rx),
            },
            shape: OnceLock::new(),
            proc_mesh: OnceLock::new(),
            name: OnceLock::new(),
        }
    }

    pub fn new_v1(actor_mesh: v1::ActorMeshRef<A>) -> Self {
        Self {
            inner: ActorMeshKind::V1(actor_mesh),
            shape: OnceLock::new(),
            proc_mesh: OnceLock::new(),
            name: OnceLock::new(),
        }
    }

    pub(crate) fn new_shared<D: Deref<Target = ProcMesh> + Send + Sync + 'static>(
        proc_mesh: D,
        name: String,
        actor_supervision_rx: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
        ranks: Vec<ActorRef<A>>,
    ) -> Self {
        Self {
            inner: ActorMeshKind::V0 {
                proc_mesh: ProcMeshRef::Shared(Box::new(proc_mesh)),
                name,
                ranks,
                actor_supervision_rx: Some(actor_supervision_rx),
            },
            shape: OnceLock::new(),
            proc_mesh: OnceLock::new(),
            name: OnceLock::new(),
        }
    }

    /// Open a port on this ActorMesh.
    pub fn open_port<M: Message>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        match &self.inner {
            ActorMeshKind::V0 { proc_mesh, .. } => proc_mesh.client().open_port(),
            ActorMeshKind::V1(_actor_mesh) => unimplemented!("unsupported operation"),
        }
    }

    /// An event stream of actor events. Each RootActorMesh can produce only one such
    /// stream, returning None after the first call.
    pub fn events(&mut self) -> Option<ActorSupervisionEvents> {
        match &mut self.inner {
            ActorMeshKind::V0 {
                actor_supervision_rx,
                ..
            } => actor_supervision_rx
                .take()
                .map(|actor_supervision_rx| ActorSupervisionEvents {
                    actor_supervision_rx,
                    mesh_id: self.id(),
                }),
            ActorMeshKind::V1(_actor_mesh) => unimplemented!("unsupported operation"),
        }
    }

    /// Access the ranks field (temporary until we remove `ArcActorMesh`).
    #[cfg(test)]
    pub(crate) fn ranks(&self) -> &Vec<ActorRef<A>> {
        match &self.inner {
            ActorMeshKind::V0 { ranks, .. } => ranks,
            ActorMeshKind::V1(_actor_mesh) => unimplemented!("unsupported operation"),
        }
    }
}

/// Supervision event stream for actor mesh. It emits actor supervision events.
pub struct ActorSupervisionEvents {
    // The receiver of supervision events from proc mesh.
    actor_supervision_rx: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
    // The name of the actor mesh.
    mesh_id: ActorMeshId,
}

impl ActorSupervisionEvents {
    pub async fn next(&mut self) -> Option<ActorSupervisionEvent> {
        let result = self.actor_supervision_rx.recv().await;
        if result.is_none() {
            tracing::info!(
                "supervision stream for actor mesh {:?} was closed!",
                self.mesh_id
            );
        }
        result
    }
}

#[async_trait]
impl<'a, A: Referable> Mesh for RootActorMesh<'a, A> {
    type Node = ActorRef<A>;
    type Id = ActorMeshId;
    type Sliced<'b>
        = SlicedActorMesh<'b, A>
    where
        'a: 'b;

    fn shape(&self) -> &Shape {
        self.shape.get_or_init(|| match &self.inner {
            ActorMeshKind::V0 { proc_mesh, .. } => proc_mesh.shape().clone(),
            ActorMeshKind::V1(actor_mesh) => actor_mesh.region().into(),
        })
    }

    fn select<R: Into<Range>>(
        &self,
        label: &str,
        range: R,
    ) -> Result<Self::Sliced<'_>, ShapeError> {
        Ok(SlicedActorMesh(self, self.shape().select(label, range)?))
    }

    fn get(&self, rank: usize) -> Option<ActorRef<A>> {
        match &self.inner {
            ActorMeshKind::V0 { ranks, .. } => ranks.get(rank).cloned(),
            ActorMeshKind::V1(actor_mesh) => actor_mesh.get(rank),
        }
    }

    fn id(&self) -> Self::Id {
        match &self.inner {
            ActorMeshKind::V0 {
                proc_mesh, name, ..
            } => ActorMeshId::V0(proc_mesh.id(), name.clone()),
            ActorMeshKind::V1(actor_mesh) => ActorMeshId::V1(actor_mesh.name().clone()),
        }
    }
}

impl<A: Referable> ActorMesh for RootActorMesh<'_, A> {
    type Actor = A;

    fn proc_mesh(&self) -> &ProcMesh {
        match &self.inner {
            ActorMeshKind::V0 { proc_mesh, .. } => proc_mesh,
            ActorMeshKind::V1(actor_mesh) => self
                .proc_mesh
                .get_or_init(|| actor_mesh.proc_mesh().clone().into()),
        }
    }

    fn name(&self) -> &str {
        match &self.inner {
            ActorMeshKind::V0 { name, .. } => name,
            ActorMeshKind::V1(actor_mesh) => {
                self.name.get_or_init(|| actor_mesh.name().to_string())
            }
        }
    }

    fn v1(&self) -> Option<v1::ActorMeshRef<Self::Actor>> {
        match &self.inner {
            ActorMeshKind::V0 { .. } => None,
            ActorMeshKind::V1(actor_mesh) => Some(actor_mesh.clone()),
        }
    }
}

pub struct SlicedActorMesh<'a, A: Referable>(&'a RootActorMesh<'a, A>, Shape);

impl<'a, A: Referable> SlicedActorMesh<'a, A> {
    pub fn new(actor_mesh: &'a RootActorMesh<'a, A>, shape: Shape) -> Self {
        Self(actor_mesh, shape)
    }

    pub fn shape(&self) -> &Shape {
        &self.1
    }
}

#[async_trait]
impl<A: Referable> Mesh for SlicedActorMesh<'_, A> {
    type Node = ActorRef<A>;
    type Id = ActorMeshId;
    type Sliced<'b>
        = SlicedActorMesh<'b, A>
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

    fn get(&self, _index: usize) -> Option<ActorRef<A>> {
        unimplemented!()
    }

    fn id(&self) -> Self::Id {
        self.0.id()
    }
}

impl<A: Referable> ActorMesh for SlicedActorMesh<'_, A> {
    type Actor = A;

    fn proc_mesh(&self) -> &ProcMesh {
        self.0.proc_mesh()
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
    fn cast<M>(&self, cx: &impl context::Actor, sel: Selection, message: M) -> Result<(), CastError>
    where
        Self::Actor: RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage,
    {
        cast_to_sliced_mesh::<A, M>(
            /*cx=*/ cx,
            /*actor_mesh_id=*/ self.id(),
            /*comm_actor_ref*/ self.proc_mesh().comm_actor(),
            /*sel_of_sliced=*/ &sel,
            /*message=*/ message,
            /*sliced_shape=*/ self.shape(),
            /*root_mesh_shape=*/ self.0.shape(),
        )
    }

    fn v1(&self) -> Option<v1::ActorMeshRef<Self::Actor>> {
        self.0
            .v1()
            .map(|actor_mesh| actor_mesh.subset(self.shape().into()).unwrap())
    }
}

/// The type of error of casting operations.
#[derive(Debug, thiserror::Error)]
pub enum CastError {
    #[error("invalid selection {0}: {1}")]
    InvalidSelection(Selection, ShapeError),

    #[error("send on rank {0}: {1}")]
    MailboxSenderError(usize, MailboxSenderError),

    #[error("unsupported selection: {0}")]
    SelectionNotSupported(String),

    #[error(transparent)]
    RootMailboxSenderError(#[from] MailboxSenderError),

    #[error(transparent)]
    ShapeError(#[from] ShapeError),

    #[error(transparent)]
    SliceError(#[from] SliceError),

    #[error(transparent)]
    SerializationError(#[from] bincode::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error(transparent)]
    ReshapeError(#[from] ReshapeError),
}

// This has to be compiled outside of test mode because the bootstrap binary
// is not built in test mode, and requires access to TestActor.
pub(crate) mod test_util {
    use std::collections::VecDeque;
    use std::fmt;
    use std::fmt::Debug;
    use std::sync::Arc;

    use anyhow::ensure;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::Instance;
    use hyperactor::PortRef;
    use hyperactor::RemoteSpawn;
    use ndslice::extent;

    use super::*;
    use crate::comm::multicast::CastInfo;
    use crate::supervision::SupervisionFailureMessage;

    // This can't be defined under a `#[cfg(test)]` because there needs to
    // be an entry in the spawnable actor registry in the executable
    // 'hyperactor_mesh_test_bootstrap' for the `tests::process` actor
    // mesh test suite.
    #[derive(Debug, Default)]
    #[hyperactor::export(
        spawn = true,
        handlers = [
            Echo { cast = true },
            Payload { cast = true },
            GetRank { cast = true },
            Error { cast = true },
            Relay,
        ],
    )]
    pub struct TestActor;

    impl Actor for TestActor {}

    /// Request message to retrieve the actor's rank.
    ///
    /// The `bool` in the tuple controls the outcome of the handler:
    /// - If `true`, the handler will send the rank and return
    ///   `Ok(())`.
    /// - If `false`, the handler will still send the rank, but return
    ///   an error (`Err(...)`).
    ///
    /// This is useful for testing both successful and failing
    /// responses from a single message type.
    #[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
    pub struct GetRank(pub bool, #[binding(include)] pub PortRef<usize>);

    #[async_trait]
    impl Handler<GetRank> for TestActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            GetRank(ok, reply): GetRank,
        ) -> Result<(), anyhow::Error> {
            let point = cx.cast_point();
            reply.send(cx, point.rank())?;
            anyhow::ensure!(ok, "intentional error!"); // If `!ok` exit with `Err()`.
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
    pub struct Echo(pub String, #[binding(include)] pub PortRef<String>);

    #[async_trait]
    impl Handler<Echo> for TestActor {
        async fn handle(&mut self, cx: &Context<Self>, message: Echo) -> Result<(), anyhow::Error> {
            let Echo(message, reply_port) = message;
            reply_port.send(cx, message)?;
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
    pub struct Payload {
        pub part: Part,
        #[binding(include)]
        pub reply_port: PortRef<()>,
    }

    #[async_trait]
    impl Handler<Payload> for TestActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            message: Payload,
        ) -> Result<(), anyhow::Error> {
            let Payload { reply_port, .. } = message;
            reply_port.send(cx, ())?;
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
    pub struct Error(pub String);

    #[async_trait]
    impl Handler<Error> for TestActor {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
            Error(error): Error,
        ) -> Result<(), anyhow::Error> {
            Err(anyhow::anyhow!("{}", error))
        }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone)]
    pub struct Relay(pub usize, pub VecDeque<PortRef<Relay>>);

    #[async_trait]
    impl Handler<Relay> for TestActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            Relay(count, mut hops): Relay,
        ) -> Result<(), anyhow::Error> {
            ensure!(!hops.is_empty(), "relay must have at least one hop");
            let next = hops.pop_front().unwrap();
            next.send(cx, Relay(count + 1, hops))?;
            Ok(())
        }
    }

    // -- ProxyActor

    #[hyperactor::export(
        spawn = true,
        handlers = [
            Echo,
        ],
    )]
    pub struct ProxyActor {
        proc_mesh: &'static Arc<ProcMesh>,
        actor_mesh: Option<RootActorMesh<'static, TestActor>>,
    }

    impl fmt::Debug for ProxyActor {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ProxyActor")
                .field("proc_mesh", &"...")
                .field("actor_mesh", &"...")
                .finish()
        }
    }

    #[async_trait]
    impl Actor for ProxyActor {
        async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
            self.actor_mesh = Some(self.proc_mesh.spawn(this, "echo", &()).await?);
            Ok(())
        }
    }

    #[async_trait]
    impl RemoteSpawn for ProxyActor {
        type Params = ();

        async fn new(_params: Self::Params) -> Result<Self, anyhow::Error> {
            // The actor creates a mesh.
            use std::sync::Arc;

            use hyperactor::channel::ChannelTransport;

            use crate::alloc::AllocSpec;
            use crate::alloc::Allocator;
            use crate::alloc::LocalAllocator;

            let alloc = LocalAllocator
                .allocate(AllocSpec {
                    extent: extent! { replica = 1 },
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Local,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();

            let proc_mesh = Arc::new(ProcMesh::allocate(alloc).await.unwrap());
            let leaked: &'static Arc<ProcMesh> = Box::leak(Box::new(proc_mesh));
            Ok(Self {
                proc_mesh: leaked,
                actor_mesh: None,
            })
        }
    }

    #[async_trait]
    impl Handler<Echo> for ProxyActor {
        async fn handle(&mut self, cx: &Context<Self>, message: Echo) -> Result<(), anyhow::Error> {
            if std::env::var("HYPERACTOR_MESH_ROUTER_NO_GLOBAL_FALLBACK").is_err() {
                // test_proxy_mesh

                let actor = self.actor_mesh.as_ref().unwrap().get(0).unwrap();

                // For now, we reply directly to the client.
                // We will support directly wiring up the meshes later.
                let (tx, mut rx) = cx.open_port();

                actor.send(cx, Echo(message.0, tx.bind()))?;
                message.1.send(cx, rx.recv().await.unwrap())?;

                Ok(())
            } else {
                // test_router_undeliverable_return

                let actor: ActorRef<_> = self.actor_mesh.as_ref().unwrap().get(0).unwrap();
                let (tx, mut rx) = cx.open_port::<String>();
                actor.send(cx, Echo(message.0, tx.bind()))?;

                use tokio::time::Duration;
                use tokio::time::timeout;
                #[allow(clippy::disallowed_methods)]
                if timeout(Duration::from_secs(1), rx.recv()).await.is_ok() {
                    message
                        .1
                        .send(cx, "the impossible happened".to_owned())
                        .unwrap()
                }

                Ok(())
            }
        }
    }
    #[async_trait]
    impl Handler<SupervisionFailureMessage> for ProxyActor {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
            message: SupervisionFailureMessage,
        ) -> Result<(), anyhow::Error> {
            panic!("unhandled supervision failure: {}", message);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hyperactor::ActorId;
    use hyperactor::PortRef;
    use hyperactor::ProcId;
    use hyperactor::WorldId;
    use hyperactor_config::attrs::Attrs;
    use timed_test::async_timed_test;
    use wirevalue::Encoding;

    use super::*;
    use crate::proc_mesh::ProcEvent;

    // These tests are parametric over allocators.
    #[macro_export]
    macro_rules! actor_mesh_test_suite {
        ($allocator:expr) => {
            use std::assert_matches::assert_matches;

            use ndslice::extent;
            use $crate::alloc::AllocSpec;
            use $crate::alloc::Allocator;
            use $crate::assign::Ranks;
            use $crate::sel_from_shape;
            use $crate::sel;
            use $crate::comm::multicast::set_cast_info_on_headers;
            use $crate::proc_mesh::SharedSpawnable;
            use std::collections::VecDeque;
            use $crate::proc_mesh::default_transport;

            use super::*;
            use super::test_util::*;

            #[tokio::test]
            async fn test_proxy_mesh() {
                use super::test_util::*;
                use $crate::alloc::AllocSpec;
                use $crate::alloc::Allocator;

                use ndslice::extent;

                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent: extent! { replica = 1 },
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();
                let instance = $crate::v1::testing::instance();
                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let actor_mesh: RootActorMesh<'_, ProxyActor> = proc_mesh.spawn(&instance, "proxy", &()).await.unwrap();
                let proxy_actor = actor_mesh.get(0).unwrap();
                let (tx, mut rx) = actor_mesh.open_port::<String>();
                proxy_actor.send(proc_mesh.client(), Echo("hello!".to_owned(), tx.bind())).unwrap();

                #[allow(clippy::disallowed_methods)]
                match tokio::time::timeout(tokio::time::Duration::from_secs(3), rx.recv()).await {
                    Ok(msg) => assert_eq!(&msg.unwrap(), "hello!"),
                    Err(_) =>  assert!(false),
                }
            }

            #[tokio::test]
            async fn test_basic() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent: extent!(replica = 4),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let instance = $crate::v1::testing::instance();
                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let actor_mesh: RootActorMesh<TestActor> = proc_mesh.spawn(&instance, "echo", &()).await.unwrap();
                let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
                actor_mesh
                    .cast(proc_mesh.client(), sel!(*), Echo("Hello".to_string(), reply_handle.bind()))
                    .unwrap();
                for _ in 0..4 {
                    assert_eq!(&reply_receiver.recv().await.unwrap(), "Hello");
                }
            }

            #[tokio::test]
            async fn test_ping_pong() {
                use hyperactor::test_utils::pingpong::PingPongActor;
                use hyperactor::test_utils::pingpong::PingPongMessage;

                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent: extent!(replica = 2),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();
                let instance = $crate::v1::testing::instance();
                let mesh = ProcMesh::allocate(alloc).await.unwrap();

                let (undeliverable_msg_tx, _) = mesh.client().open_port();
                let actor_mesh: RootActorMesh<PingPongActor> = mesh
                    .spawn(&instance, "ping-pong", &(Some(undeliverable_msg_tx.bind()), None, None))
                    .await
                    .unwrap();

                let ping: ActorRef<PingPongActor> = actor_mesh.get(0).unwrap();
                let pong: ActorRef<PingPongActor> = actor_mesh.get(1).unwrap();
                let (done_tx, done_rx) = mesh.client().open_once_port();
                ping.send(mesh.client(), PingPongMessage(4, pong.clone(), done_tx.bind())).unwrap();

                assert!(done_rx.recv().await.unwrap());
            }

            #[tokio::test]
            async fn test_pingpong_full_mesh() {
                use hyperactor::test_utils::pingpong::PingPongActor;
                use hyperactor::test_utils::pingpong::PingPongMessage;

                use futures::future::join_all;

                const X: usize = 3;
                const Y: usize = 3;
                const Z: usize = 3;
                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent: extent!(x = X, y = Y, z = Z),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let instance = $crate::v1::testing::instance();
                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let (undeliverable_tx, _undeliverable_rx) = proc_mesh.client().open_port();
                let actor_mesh: RootActorMesh<PingPongActor> = proc_mesh.spawn(&instance, "pingpong", &(Some(undeliverable_tx.bind()), None, None)).await.unwrap();
                let slice = actor_mesh.shape().slice();

                let mut futures = Vec::new();
                for rank in slice.iter() {
                    let actor = actor_mesh.get(rank).unwrap();
                    let coords = (&slice.coordinates(rank).unwrap()[..]).try_into().unwrap();
                    let sizes = (&slice.sizes())[..].try_into().unwrap();
                    let neighbors = ndslice::utils::stencil::moore_neighbors::<3>();
                    for neighbor_coords in ndslice::utils::apply_stencil(&coords, sizes, &neighbors) {
                        if let Ok(neighbor_rank) = slice.location(&neighbor_coords) {
                            let neighbor = actor_mesh.get(neighbor_rank).unwrap();
                            let (done_tx, done_rx) = proc_mesh.client().open_once_port();
                            actor
                                .send(
                                    proc_mesh.client(),
                                    PingPongMessage(4, neighbor.clone(), done_tx.bind()),
                                )
                                .unwrap();
                            futures.push(done_rx.recv());
                        }
                    }
                }
                let results = join_all(futures).await;
                assert_eq!(results.len(), 316); // 5180 messages
                for result in results {
                    assert_eq!(result.unwrap(), true);
                }
            }

            #[tokio::test]
            async fn test_cast() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent: extent!(replica = 2, host = 2, gpu = 8),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let instance = $crate::v1::testing::instance();
                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let actor_mesh: RootActorMesh<TestActor> = proc_mesh.spawn(&instance, "echo", &()).await.unwrap();
                let dont_simulate_error = true;
                let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
                actor_mesh
                    .cast(proc_mesh.client(), sel!(*), GetRank(dont_simulate_error, reply_handle.bind()))
                    .unwrap();
                let mut ranks = Ranks::new(actor_mesh.shape().slice().len());
                while !ranks.is_full() {
                    let rank = reply_receiver.recv().await.unwrap();
                    assert!(ranks.insert(rank, rank).is_none(), "duplicate rank {rank}");
                }
                // Retrieve all GPUs on replica=0, host=0
                let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
                actor_mesh
                    .cast(
                        proc_mesh.client(),
                        sel_from_shape!(actor_mesh.shape(), replica = 0, host = 0),
                        GetRank(dont_simulate_error, reply_handle.bind()),
                    )
                    .unwrap();
                let mut ranks = Ranks::new(8);
                while !ranks.is_full() {
                    let rank = reply_receiver.recv().await.unwrap();
                    assert!(ranks.insert(rank, rank).is_none(), "duplicate rank {rank}");
                }
            }

            #[tokio::test]
            async fn test_inter_actor_comms() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        // Sizes intentionally small to keep the time
                        // required for this test in the process case
                        // reasonable (< 60s).
                        extent: extent!(replica = 2, host = 2, gpu = 8),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let instance = $crate::v1::testing::instance();
                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let actor_mesh: RootActorMesh<TestActor> = proc_mesh.spawn(&instance, "echo", &()).await.unwrap();

                // Bounce the message through all actors and return it to the sender (us).
                let mut hops: VecDeque<_> = actor_mesh.iter().map(|actor| actor.port()).collect();
                let (handle, mut rx) = proc_mesh.client().open_port();
                hops.push_back(handle.bind());
                hops.pop_front()
                    .unwrap()
                    .send(proc_mesh.client(), Relay(0, hops))
                    .unwrap();
                assert_matches!(
                    rx.recv().await.unwrap(),
                    Relay(count, hops)
                        if count == actor_mesh.shape().slice().len()
                        && hops.is_empty());
            }

            #[tokio::test]
            async fn test_inter_proc_mesh_comms() {
                let mut meshes = Vec::new();
                let instance = $crate::v1::testing::instance();
                for _ in 0..2 {
                    let alloc = $allocator
                        .allocate(AllocSpec {
                            extent: extent!(replica = 1),
                            constraints: Default::default(),
                            proc_name: None,
                            transport: default_transport(),
                            proc_allocation_mode: Default::default(),
                        })
                        .await
                        .unwrap();

                    let proc_mesh = Arc::new(ProcMesh::allocate(alloc).await.unwrap());
                    let proc_mesh_clone = Arc::clone(&proc_mesh);
                    let actor_mesh : RootActorMesh<TestActor> = proc_mesh_clone.spawn(&instance, "echo", &()).await.unwrap();
                    meshes.push((proc_mesh, actor_mesh));
                }

                let mut hops: VecDeque<_> = meshes
                    .iter()
                    .flat_map(|(_proc_mesh, actor_mesh)| actor_mesh.iter())
                    .map(|actor| actor.port())
                    .collect();
                let num_hops = hops.len();

                let client = meshes[0].0.client();
                let (handle, mut rx) = client.open_port();
                hops.push_back(handle.bind());
                hops.pop_front()
                    .unwrap()
                    .send(client, Relay(0, hops))
                    .unwrap();
                assert_matches!(
                    rx.recv().await.unwrap(),
                    Relay(count, hops)
                        if count == num_hops
                        && hops.is_empty());
            }

            #[async_timed_test(timeout_secs = 60)]
            async fn test_actor_mesh_cast() {
                // Verify a full broadcast in the mesh. Send a message
                // to every actor and check each actor receives it.

                use $crate::sel;
                use $crate::comm::test_utils::TestActor as CastTestActor;
                use $crate::comm::test_utils::TestActorParams as CastTestActorParams;
                use $crate::comm::test_utils::TestMessage as CastTestMessage;

                let extent = extent!(replica = 4, host = 4, gpu = 4);
                let num_actors = extent.len();
                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent,
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let instance = $crate::v1::testing::instance();
                let mut proc_mesh = ProcMesh::allocate(alloc).await.unwrap();

                let (tx, mut rx) = hyperactor::mailbox::open_port(proc_mesh.client());
                let params = CastTestActorParams{ forward_port: tx.bind() };
                let actor_mesh: RootActorMesh<CastTestActor> = proc_mesh.spawn(&instance, "actor", &params).await.unwrap();

                actor_mesh.cast(proc_mesh.client(), sel!(*), CastTestMessage::Forward("abc".to_string())).unwrap();

                for _ in 0..num_actors {
                    assert_eq!(rx.recv().await.unwrap(), CastTestMessage::Forward("abc".to_string()));
                }

                // Attempt to avoid this intermittent fatal error.
                // ⚠ Fatal: monarch/hyperactor_mesh:hyperactor_mesh-unittest - \
                //            actor_mesh::tests::sim::test_actor_mesh_cast (2.5s)
                // Test appears to have passed but the binary exited with a non-zero exit code.
                proc_mesh.events().unwrap().into_alloc().stop_and_wait().await.unwrap();
            }

            #[tokio::test]
            async fn test_delivery_failure() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent: extent!(replica = 1 ),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let name = alloc.name().to_string();
                let mut mesh = ProcMesh::allocate(alloc).await.unwrap();
                let mut events = mesh.events().unwrap();

                // Send a message to a non-existent actor (the proc however exists).
                let unmonitored_reply_to = mesh.client().open_port::<usize>().0.bind();
                let bad_actor = ActorRef::<TestActor>::attest(ActorId(ProcId::Ranked(WorldId(name.clone()), 0), "foo".into(), 0));
                bad_actor.send(mesh.client(), GetRank(true, unmonitored_reply_to)).unwrap();

                // The message will be returned!
                assert_matches!(
                    events.next().await.unwrap(),
                    ProcEvent::Crashed(0, reason) if reason.contains("message not delivered")
                );

                // TODO: Stop the proc.
            }

            #[tokio::test]
            async fn test_send_with_headers() {
                let extent = extent!(replica = 3);
                let alloc = $allocator
                    .allocate(AllocSpec {
                        extent: extent.clone(),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: default_transport(),
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let instance = $crate::v1::testing::instance();
                let mesh = ProcMesh::allocate(alloc).await.unwrap();
                let (reply_port_handle, mut reply_port_receiver) = mesh.client().open_port::<usize>();
                let reply_port = reply_port_handle.bind();

                let actor_mesh: RootActorMesh<TestActor> = mesh.spawn(&instance, "test", &()).await.unwrap();
                let actor_ref = actor_mesh.get(0).unwrap();
                let mut headers = Attrs::new();
                set_cast_info_on_headers(&mut headers, extent.point_of_rank(0).unwrap(), mesh.client().self_id().clone());
                actor_ref.send_with_headers(mesh.client(), headers.clone(), GetRank(true, reply_port.clone())).unwrap();
                assert_eq!(0, reply_port_receiver.recv().await.unwrap());

                set_cast_info_on_headers(&mut headers, extent.point_of_rank(1).unwrap(), mesh.client().self_id().clone());
                actor_ref.port()
                    .send_with_headers(mesh.client(), headers.clone(), GetRank(true, reply_port.clone()))
                    .unwrap();
                assert_eq!(1, reply_port_receiver.recv().await.unwrap());

                set_cast_info_on_headers(&mut headers, extent.point_of_rank(2).unwrap(), mesh.client().self_id().clone());
                actor_ref.actor_id()
                    .port_id(GetRank::port())
                    .send_with_headers(
                        mesh.client(),
                        wirevalue::Any::serialize(&GetRank(true, reply_port)).unwrap(),
                        headers
                    );
                assert_eq!(2, reply_port_receiver.recv().await.unwrap());
                // TODO: Stop the proc.
            }
        }
    }

    mod local {
        use hyperactor::channel::ChannelTransport;

        use crate::alloc::local::LocalAllocator;

        actor_mesh_test_suite!(LocalAllocator);

        #[tokio::test]
        async fn test_send_failure() {
            hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

            use hyperactor::test_utils::pingpong::PingPongActor;
            use hyperactor::test_utils::pingpong::PingPongMessage;

            use crate::alloc::ProcStopReason;
            use crate::proc_mesh::ProcEvent;

            let config = hyperactor_config::global::lock();
            let _guard = config.override_key(
                hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
                tokio::time::Duration::from_secs(1),
            );

            let alloc = LocalAllocator
                .allocate(AllocSpec {
                    extent: extent!(replica = 2),
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Local,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();
            let instance = crate::v1::testing::instance();
            let monkey = alloc.chaos_monkey();
            let mut mesh = ProcMesh::allocate(alloc).await.unwrap();
            let mut events = mesh.events().unwrap();

            let actor_mesh: RootActorMesh<PingPongActor> = mesh
                .spawn(
                    &instance,
                    "ping-pong",
                    &(
                        Some(PortRef::attest_message_port(mesh.client().self_id())),
                        None,
                        None,
                    ),
                )
                .await
                .unwrap();

            let ping: ActorRef<PingPongActor> = actor_mesh.get(0).unwrap();
            let pong: ActorRef<PingPongActor> = actor_mesh.get(1).unwrap();

            // Kill ping.
            monkey(0, ProcStopReason::Killed(0, false));
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Stopped(0, ProcStopReason::Killed(0, false))
            );

            // Try to send a message to 'ping'. Since 'ping's mailbox
            // is stopped, the send will timeout and fail.
            let (unmonitored_done_tx, _) = mesh.client().open_once_port();
            ping.send(
                mesh.client(),
                PingPongMessage(1, pong.clone(), unmonitored_done_tx.bind()),
            )
            .unwrap();

            // The message will be returned!
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("message not delivered")
            );

            // Get 'pong' to send 'ping' a message. Since 'ping's
            // mailbox is stopped, the send will timeout and fail.
            let (unmonitored_done_tx, _) = mesh.client().open_once_port();
            pong.send(
                mesh.client(),
                PingPongMessage(1, ping.clone(), unmonitored_done_tx.bind()),
            )
            .unwrap();

            // The message will be returned!
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("message not delivered")
            );
        }

        #[tokio::test]
        async fn test_cast_failure() {
            use crate::alloc::ProcStopReason;
            use crate::proc_mesh::ProcEvent;
            use crate::sel;

            let alloc = LocalAllocator
                .allocate(AllocSpec {
                    extent: extent!(replica = 1),
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Local,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();
            let instance = crate::v1::testing::instance();

            let stop = alloc.stopper();
            let mut mesh = ProcMesh::allocate(alloc).await.unwrap();
            let mut events = mesh.events().unwrap();

            let actor_mesh: RootActorMesh<TestActor> =
                mesh.spawn(&instance, "reply-then-fail", &()).await.unwrap();

            // `GetRank` with `false` means exit with error after
            // replying with rank.
            let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
            actor_mesh
                .cast(mesh.client(), sel!(*), GetRank(false, reply_handle.bind()))
                .unwrap();
            let rank = reply_receiver.recv().await.unwrap();
            assert_eq!(rank, 0);

            // The above is expected to trigger a proc crash.
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("intentional error!")
            );

            // Cast the message.
            let (reply_handle, _) = actor_mesh.open_port();
            actor_mesh
                .cast(mesh.client(), sel!(*), GetRank(false, reply_handle.bind()))
                .unwrap();

            // The message will be returned!
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("message not delivered")
            );

            // Stop the mesh.
            stop();
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Stopped(0, ProcStopReason::Stopped),
            );
            assert!(events.next().await.is_none());
        }

        #[tracing_test::traced_test]
        #[tokio::test]
        async fn test_stop_actor_mesh() {
            use hyperactor::test_utils::pingpong::PingPongActor;
            use hyperactor::test_utils::pingpong::PingPongMessage;

            let config = hyperactor_config::global::lock();
            let _guard = config.override_key(
                hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
                tokio::time::Duration::from_secs(1),
            );

            let alloc = LocalAllocator
                .allocate(AllocSpec {
                    extent: extent!(replica = 2),
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Local,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();
            let instance = crate::v1::testing::instance();
            let mesh = ProcMesh::allocate(alloc).await.unwrap();

            let mesh_one: RootActorMesh<PingPongActor> = mesh
                .spawn(
                    &instance,
                    "mesh_one",
                    &(
                        Some(PortRef::attest_message_port(mesh.client().self_id())),
                        None,
                        None,
                    ),
                )
                .await
                .unwrap();

            let mesh_two: RootActorMesh<PingPongActor> = mesh
                .spawn(
                    &instance,
                    "mesh_two",
                    &(
                        Some(PortRef::attest_message_port(mesh.client().self_id())),
                        None,
                        None,
                    ),
                )
                .await
                .unwrap();

            mesh_two.stop(&instance).await.unwrap();

            let ping_two: ActorRef<PingPongActor> = mesh_two.get(0).unwrap();
            let pong_two: ActorRef<PingPongActor> = mesh_two.get(1).unwrap();

            assert!(logs_contain(&format!(
                "stopped actor {}",
                ping_two.actor_id()
            )));
            assert!(logs_contain(&format!(
                "stopped actor {}",
                pong_two.actor_id()
            )));

            // Other actor meshes on this proc mesh should still be up and running
            let ping_one: ActorRef<PingPongActor> = mesh_one.get(0).unwrap();
            let pong_one: ActorRef<PingPongActor> = mesh_one.get(1).unwrap();
            let (done_tx, done_rx) = mesh.client().open_once_port();
            pong_one
                .send(
                    mesh.client(),
                    PingPongMessage(1, ping_one.clone(), done_tx.bind()),
                )
                .unwrap();
            assert!(done_rx.recv().await.is_ok());
        }
    } // mod local

    mod process {

        use bytes::Bytes;
        use hyperactor::PortId;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::clock::Clock;
        use hyperactor::clock::RealClock;
        use hyperactor::mailbox::MessageEnvelope;
        use rand::Rng;
        use tokio::process::Command;

        use crate::alloc::process::ProcessAllocator;

        #[cfg(fbcode_build)]
        fn process_allocator() -> ProcessAllocator {
            ProcessAllocator::new(Command::new(crate::testresource::get(
                "monarch/hyperactor_mesh/bootstrap",
            )))
        }

        #[cfg(fbcode_build)] // we use an external binary, produced by buck
        actor_mesh_test_suite!(process_allocator());

        // This test is concerned with correctly reporting failures
        // when message sizes exceed configured limits.
        #[cfg(fbcode_build)]
        //#[tracing_test::traced_test]
        #[async_timed_test(timeout_secs = 30)]
        async fn test_oversized_frames() {
            // Reproduced from 'net.rs'.
            #[derive(Debug, Serialize, Deserialize, PartialEq)]
            enum Frame<M> {
                Init(u64),
                Message(u64, M),
            }
            // Calculate the frame length for the given message.
            fn frame_length(src: &ActorId, dst: &PortId, pay: &Payload) -> usize {
                let serialized = wirevalue::Any::serialize(pay).unwrap();
                let mut headers = Attrs::new();
                hyperactor::mailbox::headers::set_send_timestamp(&mut headers);
                hyperactor::mailbox::headers::set_rust_message_type::<Payload>(&mut headers);
                let envelope = MessageEnvelope::new(src.clone(), dst.clone(), serialized, headers);
                let frame = Frame::Message(0u64, envelope);
                let message = serde_multipart::serialize_bincode(&frame).unwrap();
                message.frame_len()
            }

            // This process: short delivery timeout.
            let config = hyperactor_config::global::lock();
            // This process (write): max frame len for frame writes.
            let _guard2 =
                config.override_key(hyperactor::config::CODEC_MAX_FRAME_LENGTH, 1024usize);
            // Remote process (read): max frame len for frame reads.
            // SAFETY: Ok here but not safe for concurrent access.
            unsafe {
                std::env::set_var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH", "1024");
            };
            let _guard3 =
                config.override_key(wirevalue::config::DEFAULT_ENCODING, Encoding::Bincode);

            let alloc = process_allocator()
                .allocate(AllocSpec {
                    extent: extent!(replica = 1),
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Unix,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();
            let instance = crate::v1::testing::instance();
            let mut proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
            let mut proc_events = proc_mesh.events().unwrap();
            let actor_mesh: RootActorMesh<TestActor> =
                proc_mesh.spawn(&instance, "ingest", &()).await.unwrap();
            let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
            let dest = actor_mesh.get(0).unwrap();

            // Message sized to exactly max frame length.
            let payload = Payload {
                part: Part::from(Bytes::from(vec![0u8; 586])),
                reply_port: reply_handle.bind(),
            };
            let frame_len = frame_length(
                proc_mesh.client().self_id(),
                dest.port::<Payload>().port_id(),
                &payload,
            );
            assert_eq!(frame_len, 1024);

            // Send direct. A cast message is > 1024 bytes.
            dest.send(proc_mesh.client(), payload).unwrap();
            #[allow(clippy::disallowed_methods)]
            let result = RealClock
                .timeout(Duration::from_secs(2), reply_receiver.recv())
                .await;
            assert!(result.is_ok(), "Operation should not time out");

            // Message sized to max frame length + 1.
            let payload = Payload {
                part: Part::from(Bytes::from(vec![0u8; 587])),
                reply_port: reply_handle.bind(),
            };
            let frame_len = frame_length(
                proc_mesh.client().self_id(),
                dest.port::<Payload>().port_id(),
                &payload,
            );
            assert_eq!(frame_len, 1025); // over the max frame len

            // Send direct or cast. Either are guaranteed over the
            // limit and will fail.
            if rand::thread_rng().gen_bool(0.5) {
                dest.send(proc_mesh.client(), payload).unwrap();
            } else {
                actor_mesh
                    .cast(proc_mesh.client(), sel!(*), payload)
                    .unwrap();
            }

            // The undeliverable supervision event that happens next
            // does not depend on a timeout.
            {
                let event = proc_events.next().await.unwrap();
                assert_matches!(
                    event,
                    ProcEvent::Crashed(_, _),
                    "Should have received crash event"
                );
            }
        }

        // Set this test only for `mod process` because it relies on a
        // trick to emulate router failure that only works when using
        // non-local allocators.
        #[cfg(fbcode_build)]
        #[tokio::test]
        async fn test_router_undeliverable_return() {
            // Test that an undeliverable message received by a
            // router results in actor mesh supervision events.
            use ndslice::extent;

            use super::test_util::*;
            use crate::alloc::AllocSpec;
            use crate::alloc::Allocator;

            let alloc = process_allocator()
                .allocate(AllocSpec {
                    extent: extent! { replica = 1 },
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Unix,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();

            // SAFETY: Not multithread safe.
            unsafe { std::env::set_var("HYPERACTOR_MESH_ROUTER_NO_GLOBAL_FALLBACK", "1") };

            let instance = crate::v1::testing::instance();
            let mut proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
            let mut proc_events = proc_mesh.events().unwrap();
            let mut actor_mesh: RootActorMesh<'_, ProxyActor> =
                { proc_mesh.spawn(&instance, "proxy", &()).await.unwrap() };
            let mut actor_events = actor_mesh.events().unwrap();

            let proxy_actor = actor_mesh.get(0).unwrap();
            let (tx, mut rx) = actor_mesh.open_port::<String>();
            proxy_actor
                .send(proc_mesh.client(), Echo("hello!".to_owned(), tx.bind()))
                .unwrap();

            #[allow(clippy::disallowed_methods)]
            match tokio::time::timeout(tokio::time::Duration::from_secs(3), rx.recv()).await {
                Ok(_) => panic!("the impossible happened"),
                Err(_) => {
                    assert_matches!(
                        proc_events.next().await.unwrap(),
                        ProcEvent::Crashed(0, reason) if reason.contains("undeliverable")
                    );
                    assert_eq!(
                        actor_events.next().await.unwrap().actor_id.name(),
                        actor_mesh.name(),
                    );
                }
            }

            // SAFETY: Not multithread safe.
            unsafe { std::env::remove_var("HYPERACTOR_MESH_ROUTER_NO_GLOBAL_FALLBACK") };
        }
    }

    mod sim {
        use crate::alloc::sim::SimAllocator;

        actor_mesh_test_suite!(SimAllocator::new_and_start_simnet());
    }

    mod reshape_cast {
        use async_trait::async_trait;
        use hyperactor::Actor;
        use hyperactor::Context;
        use hyperactor::Handler;
        use hyperactor::RemoteSpawn;
        use hyperactor::channel::ChannelAddr;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::channel::ChannelTx;
        use hyperactor::channel::Rx;
        use hyperactor::channel::Tx;
        use hyperactor::channel::dial;
        use hyperactor::channel::serve;
        use hyperactor::clock::Clock;
        use hyperactor::clock::RealClock;
        use ndslice::Extent;
        use ndslice::Selection;

        use crate::Mesh;
        use crate::ProcMesh;
        use crate::RootActorMesh;
        use crate::actor_mesh::ActorMesh;
        use crate::alloc::AllocSpec;
        use crate::alloc::Allocator;
        use crate::alloc::LocalAllocator;
        use crate::config::MAX_CAST_DIMENSION_SIZE;

        #[derive(Debug)]
        #[hyperactor::export(
            spawn = true,
            handlers = [() { cast = true }],
        )]
        struct EchoActor(ChannelTx<usize>);

        #[async_trait]
        impl Actor for EchoActor {}

        #[async_trait]
        impl RemoteSpawn for EchoActor {
            type Params = ChannelAddr;

            async fn new(params: ChannelAddr) -> Result<Self, anyhow::Error> {
                Ok(Self(dial::<usize>(params)?))
            }
        }

        #[async_trait]
        impl Handler<()> for EchoActor {
            async fn handle(
                &mut self,
                cx: &Context<Self>,
                _message: (),
            ) -> Result<(), anyhow::Error> {
                let Self(port) = self;
                port.post(cx.self_id().rank());
                Ok(())
            }
        }

        async fn validate_cast<A>(
            actor_mesh: &A,
            caps: &impl hyperactor::context::Actor,
            addr: ChannelAddr,
            selection: Selection,
        ) where
            A: ActorMesh<Actor = EchoActor>,
        {
            let config = hyperactor_config::global::lock();
            let _guard = config.override_key(MAX_CAST_DIMENSION_SIZE, 2);

            let (_, mut rx) = serve::<usize>(addr).unwrap();

            let expected_ranks = selection
                .eval(
                    &ndslice::selection::EvalOpts::strict(),
                    actor_mesh.shape().slice(),
                )
                .unwrap()
                .collect::<std::collections::BTreeSet<_>>();

            actor_mesh.cast(caps, selection, ()).unwrap();

            let mut received = std::collections::BTreeSet::new();

            for _ in 0..(expected_ranks.len()) {
                received.insert(
                    RealClock
                        .timeout(tokio::time::Duration::from_secs(1), rx.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }

            assert_eq!(received, expected_ranks);
        }

        use ndslice::strategy::gen_extent;
        use ndslice::strategy::gen_selection;
        use proptest::prelude::*;
        use proptest::test_runner::TestRunner;

        fn make_tokio_runtime() -> tokio::runtime::Runtime {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .build()
                .unwrap()
        }

        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 8, ..ProptestConfig::default()
            })]
            #[test]
            fn test_reshaped_actor_mesh_cast(extent in gen_extent(1..=4, 8)) {
                let runtime = make_tokio_runtime();
                async fn inner(extent: Extent) {
                    let alloc = LocalAllocator
                        .allocate(AllocSpec {
                            extent,
                            constraints: Default::default(),
                            proc_name: None,
                            transport: ChannelTransport::Local,
                            proc_allocation_mode: Default::default(),
                        }).await
                        .unwrap();
                    let instance = crate::v1::testing::instance();
                    let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                    let addr = ChannelAddr::any(ChannelTransport::Unix);
                    let actor_mesh: RootActorMesh<EchoActor> =
                        proc_mesh.spawn(&instance, "echo", &addr).await.unwrap();
                    let mut runner = TestRunner::default();
                    let selection = gen_selection(4, actor_mesh.shape().slice().sizes().to_vec(), 0)
                        .new_tree(&mut runner)
                        .unwrap()
                        .current();
                    validate_cast(&actor_mesh, actor_mesh.proc_mesh().client(), addr, selection).await;
                }
                runtime.block_on(inner(extent));
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 8, ..ProptestConfig::default()
            })]
            #[test]
            fn test_reshaped_actor_mesh_slice_cast(extent in gen_extent(1..=4, 8)) {
                let runtime = make_tokio_runtime();
                async fn inner(extent: Extent) {
                    let alloc = LocalAllocator
                        .allocate(AllocSpec {
                            extent: extent.clone(),
                            constraints: Default::default(),
                            proc_name: None,
                            transport: ChannelTransport::Local,
                            proc_allocation_mode: Default::default(),
                        }).await
                        .unwrap();
                    let instance = crate::v1::testing::instance();
                    let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();

                    let addr = ChannelAddr::any(ChannelTransport::Unix);

                    let actor_mesh: RootActorMesh<EchoActor> =
                        proc_mesh.spawn(&instance, "echo", &addr).await.unwrap();


                    let first_label = extent.labels().first().unwrap();
                    let slice = actor_mesh.select(first_label, 0..extent.size(first_label).unwrap()).unwrap();

                    // Unfortunately we must do things this way due to borrow checker reasons
                    let slice = if extent.len() >= 2 {
                        let label = &extent.labels()[1];
                        let size = extent.size(label).unwrap();
                        let start = if size > 1 { 1 } else { 0 };
                        let end = (if size > 1 { size - 1 } else { 1 }).max(start + 1);
                        slice.select(label, start..end).unwrap()
                    } else {
                        slice
                    };

                    let slice = if extent.len() >= 3 {
                        let label = &extent.labels()[2];
                        let size = extent.size(label).unwrap();
                        let start = if size > 1 { 1 } else { 0 };
                        let end = (if size > 1 { size - 1 } else { 1 }).max(start + 1);
                        slice.select(label, start..end).unwrap()
                    } else {
                        slice
                    };

                    let slice = if extent.len() >= 4 {
                        let label = &extent.labels()[3];
                        let size = extent.size(label).unwrap();
                        let start = if size > 1 { 1 } else { 0 };
                        let end = (if size > 1 { size - 1 } else { 1 }).max(start + 1);
                        slice.select(label, start..end).unwrap()
                    } else {
                        slice
                    };


                    let mut runner = TestRunner::default();
                    let selection = gen_selection(4, slice.shape().slice().sizes().to_vec(), 0)
                        .new_tree(&mut runner)
                        .unwrap()
                        .current();

                    validate_cast(
                        &slice,
                        actor_mesh.proc_mesh().client(),
                        addr,
                        selection
                    ).await;
                }
                runtime.block_on(inner(extent));
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 8, ..ProptestConfig::default()
            })]
             #[test]
             fn test_reshaped_actor_mesh_cast_with_selection(extent in gen_extent(1..=4, 8)) {
                let runtime = make_tokio_runtime();
                async fn inner(extent: Extent) {
                    let alloc = LocalAllocator
                        .allocate(AllocSpec {
                            extent,
                            constraints: Default::default(),
                            proc_name: None,
                            transport: ChannelTransport::Local,
                            proc_allocation_mode: Default::default(),
                        }).await
                        .unwrap();
                    let instance = crate::v1::testing::instance();
                    let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();

                    let addr = ChannelAddr::any(ChannelTransport::Unix);

                    let actor_mesh: RootActorMesh<EchoActor> =
                        proc_mesh.spawn(&instance, "echo", &addr).await.unwrap();

                    let mut runner = TestRunner::default();
                    let selection = gen_selection(4, actor_mesh.shape().slice().sizes().to_vec(), 0)
                        .new_tree(&mut runner)
                        .unwrap()
                        .current();

                    validate_cast(
                        &actor_mesh,
                        actor_mesh.proc_mesh().client(),
                        addr,
                        selection
                    ).await;
                }
                runtime.block_on(inner(extent));
            }
        }
    }

    mod shim {
        use std::collections::HashSet;

        use hyperactor::context::Mailbox;
        use ndslice::Extent;
        use ndslice::extent;

        use super::*;
        use crate::sel;

        #[tokio::test]
        #[cfg(fbcode_build)]
        async fn test_basic() {
            let instance = v1::testing::instance();
            let host_mesh = v1::testing::host_mesh(extent!(host = 4)).await;
            let proc_mesh = host_mesh
                .spawn(instance, "test", Extent::unity())
                .await
                .unwrap();
            let actor_mesh: v1::ActorMesh<v1::testactor::TestActor> =
                proc_mesh.spawn(instance, "test", &()).await.unwrap();

            let actor_mesh_v0: RootActorMesh<'_, _> = actor_mesh.clone().into();

            let (cast_info, mut cast_info_rx) = instance.mailbox().open_port();
            actor_mesh_v0
                .cast(
                    instance,
                    sel!(*),
                    v1::testactor::GetCastInfo {
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
        }
    }
}
