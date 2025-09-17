/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::marker::PhantomData;

use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::RemoteActor;
use hyperactor::context;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh_macros::sel;
use ndslice::Selection;
use ndslice::Shape;
use ndslice::ViewExt;
use ndslice::view;
use ndslice::view::Region;
use ndslice::view::View;
use serde::Deserialize;
use serde::Serialize;

use crate::CommActor;
use crate::actor_mesh as v0_actor_mesh;
use crate::reference::ActorMeshId;
use crate::v1;
use crate::v1::Error;
use crate::v1::Name;
use crate::v1::ProcMeshRef;
use crate::v1::ValueMesh;

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
}

impl<A: RemoteActor> ActorMesh<A> {
    /// Freeze this actor mesh in its current state, returning a stable
    /// reference that may be serialized.
    pub fn freeze(&self) -> ActorMeshRef<A> {
        let actor_refs = self
            .proc_mesh
            .values()
            .map(|p| p.attest(&self.name))
            .collect();
        ActorMeshRef::new(self.name.clone(), self.proc_mesh.clone(), actor_refs)
    }
}

/// A reference to a stable snapshot of an [`ActorMesh`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorMeshRef<A: RemoteActor> {
    proc_mesh: ProcMeshRef,
    name: Name,
    actor_refs: Vec<ActorRef<A>>, // Ranked::get() -> &ActorRef<A>
    _phantom: PhantomData<A>,
}

impl<A: RemoteActor> ActorMeshRef<A> {
    pub(crate) fn new(name: Name, proc_mesh: ProcMeshRef, actor_refs: Vec<ActorRef<A>>) -> Self {
        Self {
            proc_mesh,
            name,
            actor_refs,
            _phantom: PhantomData,
        }
    }
}

impl<A: Actor + RemoteActor> ActorMeshRef<A> {
    /// Cast a message to all actors in this mesh.
    pub fn cast<M>(&self, cx: &impl context::Actor, message: M) -> v1::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage,
    {
        let cast_mesh_shape = to_shape(view::Ranked::region(self));
        let comm_actor_ref = self
            .proc_mesh
            .root_mesh_rank_0
            .attest::<CommActor>(self.proc_mesh.comm_actor_name());
        let actor_mesh_id = ActorMeshId::V1(self.name.clone());
        match &self.proc_mesh.root_region {
            Some(root_region) => {
                let root_mesh_shape = to_shape(root_region);
                v0_actor_mesh::cast_to_sliced_mesh::<A, M>(
                    cx,
                    actor_mesh_id,
                    &comm_actor_ref,
                    &sel!(*),
                    message,
                    &cast_mesh_shape,
                    &root_mesh_shape,
                )
                .map_err(|e| Error::CastingError(self.name.clone(), e.into()))
            }
            None => v0_actor_mesh::actor_mesh_cast::<A, M>(
                cx,
                actor_mesh_id,
                &comm_actor_ref,
                sel!(*),
                &cast_mesh_shape,
                &cast_mesh_shape,
                message,
            )
            .map_err(|e| Error::CastingError(self.name.clone(), e.into())),
        }
    }

    pub async fn supervision_events(
        &self,
        cx: &impl context::Actor,
        name: Name,
    ) -> v1::Result<ValueMesh<Vec<ActorSupervisionEvent>>> {
        self.proc_mesh.supervision_events(cx, name).await
    }
}

impl<A: RemoteActor> view::Ranked for ActorMeshRef<A> {
    type Item = ActorRef<A>;

    fn region(&self) -> &Region {
        view::Ranked::region(&self.proc_mesh)
    }

    fn get(&self, rank: usize) -> Option<&Self::Item> {
        self.actor_refs.get(rank)
    }
}

impl<A: RemoteActor> view::RankedSliceable for ActorMeshRef<A> {
    fn sliced(&self, region: Region) -> Self {
        debug_assert!(region.is_subset(view::Ranked::region(self)));
        let proc_mesh = self.proc_mesh.subset(region.clone()).unwrap();
        let actor_refs = self
            .region()
            .remap(&region)
            .unwrap()
            .map(|index| self.actor_refs[index].clone())
            .collect();
        ActorMeshRef::new(self.name.clone(), proc_mesh, actor_refs)
    }
}

fn to_shape(region: &Region) -> Shape {
    Shape::new(region.labels().to_vec(), region.slice().clone())
        .expect("Shape::new should not fail because a Region by definition is a valid Shape")
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;

    use hyperactor::actor::ActorStatus;
    use hyperactor::supervision::ActorSupervisionEvent;
    use ndslice::ViewExt;
    use ndslice::extent;
    use timed_test::async_timed_test;

    use crate::v1::Name;
    use crate::v1::testactor;
    use crate::v1::testing;

    #[async_timed_test(timeout_secs = 30)]
    async fn test_status() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) =
            instance.open_port::<ActorSupervisionEvent>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let meshes = testing::proc_meshes(&instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
        let child_name = Name::new("child");

        let actor_mesh = proc_mesh
            .freeze()
            .spawn_with_name::<testactor::TestActor>(&instance, child_name.clone(), &())
            .await
            .unwrap();

        actor_mesh
            .freeze()
            .cast(
                &instance,
                testactor::CauseSupervisionEvent(testactor::SupervisionEventType::Panic),
            )
            .unwrap();

        // Wait for the casted message to cause a panic on all actors.
        // We can't use a reply port because the handler for the message will
        // by definition not complete and send a reply.
        #[allow(clippy::disallowed_methods)]
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Now that all ranks have completed, set up a continuous poll of the
        // status such that when a process switches to unhealthy it sets a
        // supervision event.
        let actor_mesh_ref = actor_mesh.freeze();
        let child_name_clone = child_name.clone();
        let supervision_task = tokio::spawn(async move {
            match actor_mesh_ref
                .supervision_events(&instance, child_name_clone)
                .await
            {
                Ok(events) => {
                    for event_list in events.values() {
                        assert!(!event_list.is_empty());
                        for event in event_list {
                            supervisor.send(&instance, event).unwrap();
                        }
                    }
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            };
        });
        // Make sure the task completes first without a panic.
        supervision_task.await.unwrap();

        for _ in 0..num_replicas {
            match supervision_receiver.recv().await {
                Ok(event) => {
                    println!("receiving event: {:?}", event);
                    assert_eq!(event.actor_id.name(), format!("{}", child_name.clone()));
                    assert_matches!(event.actor_status, ActorStatus::Failed(_));
                }
                Err(e) => {
                    panic!("error: {:?}", e);
                }
            }
        }
    }
}
