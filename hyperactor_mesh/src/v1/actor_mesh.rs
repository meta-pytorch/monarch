/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::OnceLock as OnceCell;

use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::RemoteActor;
use hyperactor::attrs::Attrs;
use hyperactor::context;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor_mesh_macros::sel;
use ndslice::Selection;
use ndslice::ViewExt as _;
use ndslice::view;
use ndslice::view::Region;
use ndslice::view::View;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::CommActor;
use crate::actor_mesh as v0_actor_mesh;
use crate::comm::multicast;
use crate::proc_mesh::mesh_agent::ActorState;
use crate::reference::ActorMeshId;
use crate::resource;
use crate::v1;
use crate::v1::Error;
use crate::v1::Name;
use crate::v1::ProcMeshRef;
use crate::v1::ValueMesh;

/// An ActorMesh is a collection of ranked A-typed actors.
#[derive(Debug)]
pub struct ActorMesh<A: RemoteActor> {
    proc_mesh: ProcMeshRef,
    name: Name,
    current_ref: ActorMeshRef<A>,
}

impl<A: RemoteActor> ActorMesh<A> {
    pub(crate) fn new(proc_mesh: ProcMeshRef, name: Name) -> Self {
        let current_ref =
            ActorMeshRef::with_page_size(name.clone(), proc_mesh.clone(), DEFAULT_PAGE);

        Self {
            proc_mesh,
            name,
            current_ref,
        }
    }
}

impl<A: RemoteActor> Deref for ActorMesh<A> {
    type Target = ActorMeshRef<A>;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
    }
}

/// Manual implementation of Clone because `A` doesn't need to implement Clone
/// but we still want to be able to clone the ActorMesh.
impl<A: RemoteActor> Clone for ActorMesh<A> {
    fn clone(&self) -> Self {
        Self {
            proc_mesh: self.proc_mesh.clone(),
            name: self.name.clone(),
            current_ref: self.current_ref.clone(),
        }
    }
}

/// Influences paging behavior for the lazy cache. Smaller pages
/// reduce over-allocation for sparse access; larger pages reduce the
/// number of heap allocations for contiguous scans.
const DEFAULT_PAGE: usize = 1024;

/// A lazily materialized page of ActorRefs.
struct Page<A: RemoteActor> {
    slots: Box<[OnceCell<ActorRef<A>>]>,
}

impl<A: RemoteActor> Page<A> {
    fn new(len: usize) -> Self {
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(OnceCell::new());
        }
        Self {
            slots: v.into_boxed_slice(),
        }
    }
}

/// A reference to a stable snapshot of an [`ActorMesh`].
pub struct ActorMeshRef<A: RemoteActor> {
    proc_mesh: ProcMeshRef,
    name: Name,

    /// Lazily allocated collection of pages:
    /// - The outer `OnceCell` defers creating the vector until first
    ///   use.
    /// - The `Vec` holds slots for multiple pages.
    /// - Each slot is itself a `OnceCell<Box<Page<A>>>`, so that each
    ///   page can be initialized on demand.
    /// - A `Page<A>` is a boxed slice of `OnceCell<ActorRef<A>>`,
    ///   i.e. the actual storage for actor references within that
    ///   page.
    pages: OnceCell<Vec<OnceCell<Box<Page<A>>>>>,
    // Page size knob (not serialize; defaults after deserialize).
    page_size: usize,

    _phantom: PhantomData<A>,
}

impl<A: Actor + RemoteActor> ActorMeshRef<A> {
    /// Cast a message to all actors in this mesh.
    pub fn cast<M>(&self, cx: &impl context::Actor, message: M) -> v1::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        if let Some(root_comm_actor) = self.proc_mesh.root_comm_actor() {
            self.cast_v0(cx, message, root_comm_actor)
        } else {
            for (point, actor) in self.iter() {
                let mut headers = Attrs::new();
                headers.set(
                    multicast::CAST_ORIGINATING_SENDER,
                    cx.instance().self_id().clone(),
                );
                headers.set(multicast::CAST_POINT, point);

                actor
                    .send_with_headers(cx, headers, message.clone())
                    .map_err(|e| Error::SendingError(actor.actor_id().clone(), Box::new(e)))?;
            }
            Ok(())
        }
    }

    fn cast_v0<M>(
        &self,
        cx: &impl context::Actor,
        message: M,
        root_comm_actor: &ActorRef<CommActor>,
    ) -> v1::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        let cast_mesh_shape = view::Ranked::region(self).into();
        let actor_mesh_id = ActorMeshId::V1(self.name.clone());
        match &self.proc_mesh.root_region {
            Some(root_region) => {
                let root_mesh_shape = root_region.into();
                v0_actor_mesh::cast_to_sliced_mesh::<A, M>(
                    cx,
                    actor_mesh_id,
                    root_comm_actor,
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
                root_comm_actor,
                sel!(*),
                &cast_mesh_shape,
                &cast_mesh_shape,
                message,
            )
            .map_err(|e| Error::CastingError(self.name.clone(), e.into())),
        }
    }

    pub async fn actor_states(
        &self,
        cx: &impl context::Actor,
    ) -> v1::Result<ValueMesh<resource::State<ActorState>>> {
        self.proc_mesh.actor_states(cx, self.name.clone()).await
    }
}

impl<A: RemoteActor> ActorMeshRef<A> {
    pub(crate) fn new(name: Name, proc_mesh: ProcMeshRef) -> Self {
        Self::with_page_size(name, proc_mesh, DEFAULT_PAGE)
    }

    pub(crate) fn with_page_size(name: Name, proc_mesh: ProcMeshRef, page_size: usize) -> Self {
        Self {
            proc_mesh,
            name,
            pages: OnceCell::new(),
            page_size: page_size.max(1),
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        view::Ranked::region(&self.proc_mesh).num_ranks()
    }

    fn ensure_pages(&self) -> &Vec<OnceCell<Box<Page<A>>>> {
        let n = self.len().div_ceil(self.page_size); // ⌈len / page_size⌉
        self.pages
            .get_or_init(|| (0..n).map(|_| OnceCell::new()).collect())
    }

    fn materialize(&self, rank: usize) -> Option<&ActorRef<A>> {
        let len = self.len();
        if rank >= len {
            return None;
        }
        let p = self.page_size;
        let page_ix = rank / p;
        let local_ix = rank % p;

        let pages = self.ensure_pages();
        let page = pages[page_ix].get_or_init(|| {
            // Last page may be partial.
            let base = page_ix * p;
            let remaining = len - base;
            let page_len = remaining.min(p);
            Box::new(Page::<A>::new(page_len))
        });

        Some(page.slots[local_ix].get_or_init(|| {
            // Invariant: `proc_mesh` and this view share the same
            // dense rank space:
            //   - ranks are contiguous [0, self.len()) with no gaps
            //     or reordering
            //   - for every rank r, `proc_mesh.get(r)` is Some(..)
            // Therefore we can index `proc_mesh` with `rank`
            // directly.
            debug_assert!(rank < self.len(), "rank must be within [0, len)");
            debug_assert!(
                self.proc_mesh.get(rank).is_some(),
                "proc_mesh must be dense/aligned with this view"
            );
            let proc_ref = self.proc_mesh.get(rank).expect("rank in-bounds");
            proc_ref.attest(&self.name)
        }))
    }
}

impl<A: RemoteActor> Clone for ActorMeshRef<A> {
    fn clone(&self) -> Self {
        Self {
            proc_mesh: self.proc_mesh.clone(),
            name: self.name.clone(),
            pages: OnceCell::new(), // No clone cache.
            page_size: self.page_size,
            _phantom: PhantomData,
        }
    }
}

impl<A: RemoteActor> PartialEq for ActorMeshRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.proc_mesh == other.proc_mesh && self.name == other.name
    }
}
impl<A: RemoteActor> Eq for ActorMeshRef<A> {}

impl<A: RemoteActor> Hash for ActorMeshRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.proc_mesh.hash(state);
        self.name.hash(state);
    }
}

impl<A: RemoteActor> fmt::Debug for ActorMeshRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorMeshRef")
            .field("proc_mesh", &self.proc_mesh)
            .field("name", &self.name)
            .field("page_size", &self.page_size)
            .finish_non_exhaustive() // No print cache.
    }
}

// Implement Serialize manually, without requiring A: Serialize
impl<A: RemoteActor> Serialize for ActorMeshRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize only the fields that don't depend on A
        (&self.proc_mesh, &self.name).serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize
impl<'de, A: RemoteActor> Deserialize<'de> for ActorMeshRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (proc_mesh, name) = <(ProcMeshRef, Name)>::deserialize(deserializer)?;
        Ok(ActorMeshRef::with_page_size(name, proc_mesh, DEFAULT_PAGE))
    }
}

impl<A: RemoteActor> view::Ranked for ActorMeshRef<A> {
    type Item = ActorRef<A>;

    #[inline]
    fn region(&self) -> &Region {
        view::Ranked::region(&self.proc_mesh)
    }

    #[inline]
    fn get(&self, rank: usize) -> Option<&Self::Item> {
        self.materialize(rank)
    }
}

impl<A: RemoteActor> view::RankedSliceable for ActorMeshRef<A> {
    fn sliced(&self, region: Region) -> Self {
        debug_assert!(region.is_subset(view::Ranked::region(self)));
        let proc_mesh = self.proc_mesh.subset(region).unwrap();
        Self::with_page_size(self.name.clone(), proc_mesh, self.page_size)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashSet;

    use hyperactor::actor::ActorStatus;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::context::Mailbox as _;
    use hyperactor::mailbox;
    use ndslice::Extent;
    use ndslice::ViewExt;
    use ndslice::extent;
    use ndslice::view::Ranked;
    use timed_test::async_timed_test;
    use tokio::time::Duration;

    use super::ActorMesh;
    use crate::proc_mesh::mesh_agent::ActorState;
    use crate::resource;
    use crate::v1::ActorMeshRef;
    use crate::v1::Name;
    use crate::v1::ProcMesh;
    use crate::v1::testactor;
    use crate::v1::testing;

    #[tokio::test]
    async fn test_actor_mesh_ref_lazy_materialization() {
        // 1) Bring up procs and spawn actors.
        let instance = testing::instance().await;
        // Small mesh so the test runs fast, but > page_size so we
        // cross a boundary
        let extent = extent!(replicas = 3, hosts = 2); // 6 ranks
        let pm: ProcMesh = testing::proc_meshes(instance, extent.clone())
            .await
            .into_iter()
            .next()
            .expect("at least one proc mesh");
        let am: ActorMesh<testactor::TestActor> = pm.spawn(instance, "test", &()).await.unwrap();

        // 2) Build our ActorMeshRef with a tiny page size (2) to
        // force multiple pages:
        // page 0: ranks [0,1], page 1: [2,3], page 2: [4,5]
        let page_size = 2;
        let amr: ActorMeshRef<testactor::TestActor> =
            ActorMeshRef::with_page_size(am.name.clone(), pm.clone(), page_size);
        assert_eq!(amr.extent(), extent);
        assert_eq!(amr.region().num_ranks(), 6);

        // 3) Within-rank pointer stability (OnceLock caches &ActorRef)
        let p0_a = amr.get(0).expect("rank 0 exists") as *const _;
        let p0_b = amr.get(0).expect("rank 0 exists") as *const _;
        assert_eq!(p0_a, p0_b, "same rank should return same cached pointer");

        // 4) Same page, different rank (both materialize fine)
        let p1_a = amr.get(1).expect("rank 1 exists") as *const _;
        let p1_b = amr.get(1).expect("rank 1 exists") as *const _;
        assert_eq!(p1_a, p1_b, "same rank should return same cached pointer");
        // They're different ranks, so the pointers are different
        // (distinct OnceLocks in the page)
        assert_ne!(p0_a, p1_a, "different ranks have different cache slots");

        // 5) Cross a page boundary (rank 2 is in a different page than rank 0/1)
        let p2_a = amr.get(2).expect("rank 2 exists") as *const _;
        let p2_b = amr.get(2).expect("rank 2 exists") as *const _;
        assert_eq!(p2_a, p2_b, "same rank should return same cached pointer");
        assert_ne!(p0_a, p2_a, "different pages have different cache slots");

        // 6) Clone should drop the cache but keep identity (actor_id)
        let amr_clone = amr.clone();
        let orig_id_0 = amr.get(0).unwrap().actor_id().clone();
        let clone_id_0 = amr_clone.get(0).unwrap().actor_id().clone();
        assert_eq!(orig_id_0, clone_id_0, "clone preserves identity");
        let p0_clone = amr_clone.get(0).unwrap() as *const _;
        assert_ne!(
            p0_a, p0_clone,
            "cloned ActorMeshRef has a fresh cache (different pointer)"
        );

        // 7) Slicing preserves page_size and clears cache
        // (RankedSliceable::sliced)
        let sliced = amr.range("replicas", 1..).expect("slice should be valid"); // leaves 4 ranks
        assert_eq!(sliced.region().num_ranks(), 4);
        // First access materializes a new cache for the sliced view.
        let sp0_a = sliced.get(0).unwrap() as *const _;
        let sp0_b = sliced.get(0).unwrap() as *const _;
        assert_eq!(sp0_a, sp0_b, "sliced view has its own cache slot per rank");
        // Cross-page inside the slice too (page_size = 2 => pages are
        // [0..2), [2..4)).
        let sp2 = sliced.get(2).unwrap() as *const _;
        assert_ne!(sp0_a, sp2, "sliced view crosses its own page boundary");

        // 8) Hash/Eq ignore cache state; identical identity collapses
        // to one set entry.
        let mut set = HashSet::new();
        set.insert(amr.clone());
        set.insert(amr.clone());
        assert_eq!(set.len(), 1, "cache state must not affect Hash/Eq");

        // 9) As a sanity check, cast to ensure the refs are indeed
        // usable/live.
        let (port, mut rx) = mailbox::open_port(instance);
        // Send to rank 0 and rank 3 (extent 3x2 => at least 4 ranks
        // exist).
        amr.get(0)
            .expect("rank 0 exists")
            .send(instance, testactor::GetActorId(port.bind()))
            .expect("send to rank 0 should succeed");
        amr.get(3)
            .expect("rank 3 exists")
            .send(instance, testactor::GetActorId(port.bind()))
            .expect("send to rank 3 should succeed");
        let id_a = RealClock
            .timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for first reply")
            .expect("channel closed before first reply");
        let id_b = RealClock
            .timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for second reply")
            .expect("channel closed before second reply");
        assert_ne!(id_a, id_b, "two different ranks responded");
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_actor_states() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance().await;
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) =
            instance.open_port::<resource::State<ActorState>>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
        let child_name = Name::new("child");

        let actor_mesh = proc_mesh
            .spawn_with_name::<testactor::TestActor>(instance, child_name.clone(), &())
            .await
            .unwrap();

        actor_mesh
            .cast(
                instance,
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
        let supervision_task = tokio::spawn(async move {
            match actor_mesh.actor_states(&instance).await {
                Ok(events) => {
                    for state in events.values() {
                        supervisor.send(instance, state.clone()).unwrap();
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
            let state = supervision_receiver.recv().await.unwrap();
            if let resource::Status::Failed(s) = state.status {
                assert!(s.contains("supervision events"));
            } else {
                panic!("Not failed: {:?}", state.status);
            }
            if let Some(ref inner) = state.state {
                assert!(!inner.supervision_events.is_empty());
                for event in &inner.supervision_events {
                    println!("receiving event: {:?}", event);
                    assert_eq!(event.actor_id.name(), format!("{}", child_name.clone()));
                    assert_matches!(event.actor_status, ActorStatus::Failed(_));
                }
            }
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_cast() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);

        let instance = testing::instance().await;
        let host_mesh = testing::host_mesh(extent!(host = 4)).await;
        let proc_mesh = host_mesh
            .spawn(instance, "test", Extent::unity())
            .await
            .unwrap();
        let actor_mesh = proc_mesh
            .spawn::<testactor::TestActor>(instance, "test", &())
            .await
            .unwrap();

        let (cast_info, mut cast_info_rx) = instance.mailbox().open_port();
        actor_mesh
            .cast(
                instance,
                testactor::GetCastInfo {
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

        let _ = host_mesh.shutdown(&instance).await;
    }
}
