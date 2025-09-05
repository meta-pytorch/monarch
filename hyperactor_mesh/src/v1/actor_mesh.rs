use std::marker::PhantomData;

use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::RemoteActor;
use hyperactor::cap;
use hyperactor::message::Castable;
use ndslice::view;
use ndslice::view::Region;
use ndslice::view::View;
use ndslice::view::ViewExt;
use serde::Deserialize;
use serde::Serialize;

use crate::v1;
use crate::v1::Error;
use crate::v1::Name;
use crate::v1::ProcMeshRef;

/// An ActorMesh is a collection of ranked A-typed actors.
#[derive(Debug)]
pub struct ActorMesh<A> {
    proc_mesh: ProcMeshRef,
    name: Name,
    _phantom: PhantomData<A>,
}

impl<A> ActorMesh<A> {
    pub(crate) fn new(proc_mesh: ProcMeshRef, name: Name) -> Self {
        Self {
            proc_mesh,
            name,
            _phantom: PhantomData,
        }
    }

    /// Freeze this actor mesh in its current state, returning a stable
    /// reference that may be serialized.
    pub fn freeze(&self) -> ActorMeshRef<A> {
        ActorMeshRef {
            proc_mesh: self.proc_mesh.clone(),
            name: self.name.clone(),
            _phantom: PhantomData,
        }
    }
}

/// A reference to a stable snapshot of an [`ActorMesh`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorMeshRef<A> {
    proc_mesh: ProcMeshRef,
    name: Name,
    _phantom: PhantomData<A>,
}

impl<A: Actor + RemoteActor> ActorMeshRef<A> {
    /// Cast a message to all actors in this mesh.
    pub fn cast<M>(&self, caps: &impl cap::CanSend, message: M) -> v1::Result<()>
    where
        M: Castable + RemoteMessage + Clone,
        A: RemoteHandles<M>,
    {
        // todo: headers, binding/unbinding/accumulation
        for actor_ref in self.values() {
            actor_ref
                .send(caps, message.clone())
                .map_err(|e| Error::SendingError(actor_ref.actor_id().clone(), e))?;
        }
        Ok(())
    }
}

impl<A: RemoteActor> view::Ranked for ActorMeshRef<A> {
    type Item = ActorRef<A>;

    fn region(&self) -> &Region {
        view::Ranked::region(&self.proc_mesh)
    }

    fn get(&self, rank: usize) -> Option<ActorRef<A>> {
        let proc_ref = view::Ranked::get(&self.proc_mesh, rank)?;
        Some(proc_ref.attest(&self.name.clone()))
    }

    // TODO: adjust this to include a compaction hint, rather than the nodes themselves,
    // as the current interface forces refs like ActorRef, which does not materialize its
    // ranks, to needlessly materialize.
    fn sliced(&self, region: Region, _nodes: impl Iterator<Item = ActorRef<A>>) -> Self {
        Self {
            // This is safe because by the time `sliced` has been called, the subsetting
            // has been validated.
            proc_mesh: self.proc_mesh.subset(region).unwrap(),
            name: self.name.clone(),
            _phantom: PhantomData,
        }
    }
}
