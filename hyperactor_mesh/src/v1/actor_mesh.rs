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

use hyperactor::ActorRef;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor::message::Unbound;
use hyperactor_config::attrs::Attrs;
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
///
/// Bound note: `A: Referable` because the mesh stores/returns
/// `ActorRef<A>`, which is only defined for `A: Referable`.
#[derive(Debug)]
pub struct ActorMesh<A: Referable> {
    proc_mesh: ProcMeshRef,
    name: Name,
    current_ref: ActorMeshRef<A>,
}

// `A: Referable` for the same reason as the struct: the mesh holds
// `ActorRef<A>`.
impl<A: Referable> ActorMesh<A> {
    pub(crate) fn new(proc_mesh: ProcMeshRef, name: Name) -> Self {
        let current_ref =
            ActorMeshRef::with_page_size(name.clone(), proc_mesh.clone(), DEFAULT_PAGE);

        Self {
            proc_mesh,
            name,
            current_ref,
        }
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    /// Detach this mesh from the lifetime of `self`, and return its reference.
    pub(crate) fn detach(self) -> ActorMeshRef<A> {
        self.current_ref.clone()
    }

    /// Stop actors on this mesh across all procs.
    pub async fn stop(&self, cx: &impl context::Actor) -> v1::Result<()> {
        self.proc_mesh()
            .stop_actor_by_name(cx, self.name.clone())
            .await
    }
}

impl<A: Referable> fmt::Display for ActorMesh<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.current_ref)
    }
}

impl<A: Referable> Deref for ActorMesh<A> {
    type Target = ActorMeshRef<A>;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
    }
}

/// Manual implementation of Clone because `A` doesn't need to implement Clone
/// but we still want to be able to clone the ActorMesh.
impl<A: Referable> Clone for ActorMesh<A> {
    fn clone(&self) -> Self {
        Self {
            proc_mesh: self.proc_mesh.clone(),
            name: self.name.clone(),
            current_ref: self.current_ref.clone(),
        }
    }
}

impl<A: Referable> Drop for ActorMesh<A> {
    fn drop(&mut self) {
        tracing::info!(
            name = "ActorMeshStatus",
            actor_name = %self.name,
            status = "Dropped",
        );
    }
}

/// Influences paging behavior for the lazy cache. Smaller pages
/// reduce over-allocation for sparse access; larger pages reduce the
/// number of heap allocations for contiguous scans.
const DEFAULT_PAGE: usize = 1024;

/// A lazily materialized page of ActorRefs.
struct Page<A: Referable> {
    slots: Box<[OnceCell<ActorRef<A>>]>,
}

impl<A: Referable> Page<A> {
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
pub struct ActorMeshRef<A: Referable> {
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

impl<A: Referable> ActorMeshRef<A> {
    /// Cast a message to all the actors in this mesh
    #[allow(clippy::result_large_err)]
    pub fn cast<M>(&self, cx: &impl context::Actor, message: M) -> v1::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        self.cast_with_selection(cx, sel!(*), message)
    }

    /// Cast a message to the actors in this mesh according to the provided selection.
    /// This should *only* be used for temporary support for selections in the tensor
    /// engine. If you use this for anything else, you will be fired (you too, OSS
    /// contributor).
    #[allow(clippy::result_large_err)]
    pub(crate) fn cast_for_tensor_engine_only_do_not_use<M>(
        &self,
        cx: &impl context::Actor,
        sel: Selection,
        message: M,
    ) -> v1::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        self.cast_with_selection(cx, sel, message)
    }

    #[allow(clippy::result_large_err)]
    fn cast_with_selection<M>(
        &self,
        cx: &impl context::Actor,
        sel: Selection,
        message: M,
    ) -> v1::Result<()>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage + Clone, // Clone is required until we are fully onto comm actor
    {
        if let Some(root_comm_actor) = self.proc_mesh.root_comm_actor() {
            self.cast_v0(cx, message, sel, root_comm_actor)
        } else {
            for (point, actor) in self.iter() {
                let create_rank = point.rank();
                let mut headers = Attrs::new();
                headers.set(
                    multicast::CAST_ORIGINATING_SENDER,
                    cx.instance().self_id().clone(),
                );
                headers.set(multicast::CAST_POINT, point);

                // Make sure that we re-bind ranks, as these may be used for
                // bootstrapping comm actors.
                let mut unbound = Unbound::try_from_message(message.clone())
                    .map_err(|e| Error::CastingError(self.name.clone(), e))?;
                unbound
                    .visit_mut::<resource::Rank>(|resource::Rank(rank)| {
                        *rank = Some(create_rank);
                        Ok(())
                    })
                    .map_err(|e| Error::CastingError(self.name.clone(), e))?;
                let rebound_message = unbound
                    .bind()
                    .map_err(|e| Error::CastingError(self.name.clone(), e))?;
                actor
                    .send_with_headers(cx, headers, rebound_message)
                    .map_err(|e| Error::SendingError(actor.actor_id().clone(), Box::new(e)))?;
            }
            Ok(())
        }
    }

    #[allow(clippy::result_large_err)]
    fn cast_v0<M>(
        &self,
        cx: &impl context::Actor,
        message: M,
        sel: Selection,
        root_comm_actor: &ActorRef<CommActor>,
    ) -> v1::Result<()>
    where
        A: RemoteHandles<IndexedErasedUnbound<M>>,
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
                    &sel,
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
                sel,
                &cast_mesh_shape,
                &cast_mesh_shape,
                message,
            )
            .map_err(|e| Error::CastingError(self.name.clone(), e.into())),
        }
    }

    #[allow(clippy::result_large_err)]
    pub async fn actor_states(
        &self,
        cx: &impl context::Actor,
    ) -> v1::Result<ValueMesh<resource::State<ActorState>>> {
        self.proc_mesh.actor_states(cx, self.name.clone()).await
    }
}

impl<A: Referable> ActorMeshRef<A> {
    pub(crate) fn new(name: Name, proc_mesh: ProcMeshRef) -> Self {
        Self::with_page_size(name, proc_mesh, DEFAULT_PAGE)
    }

    pub fn name(&self) -> &Name {
        &self.name
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

    pub fn proc_mesh(&self) -> &ProcMeshRef {
        &self.proc_mesh
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

impl<A: Referable> Clone for ActorMeshRef<A> {
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

impl<A: Referable> fmt::Display for ActorMeshRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}@{}", self.name, A::typename(), self.proc_mesh)
    }
}

impl<A: Referable> PartialEq for ActorMeshRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.proc_mesh == other.proc_mesh && self.name == other.name
    }
}
impl<A: Referable> Eq for ActorMeshRef<A> {}

impl<A: Referable> Hash for ActorMeshRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.proc_mesh.hash(state);
        self.name.hash(state);
    }
}

impl<A: Referable> fmt::Debug for ActorMeshRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorMeshRef")
            .field("proc_mesh", &self.proc_mesh)
            .field("name", &self.name)
            .field("page_size", &self.page_size)
            .finish_non_exhaustive() // No print cache.
    }
}

// Implement Serialize manually, without requiring A: Serialize
impl<A: Referable> Serialize for ActorMeshRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize only the fields that don't depend on A
        (&self.proc_mesh, &self.name).serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize
impl<'de, A: Referable> Deserialize<'de> for ActorMeshRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (proc_mesh, name) = <(ProcMeshRef, Name)>::deserialize(deserializer)?;
        Ok(ActorMeshRef::with_page_size(name, proc_mesh, DEFAULT_PAGE))
    }
}

impl<A: Referable> view::Ranked for ActorMeshRef<A> {
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

impl<A: Referable> view::RankedSliceable for ActorMeshRef<A> {
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
    use crate::v1::proc_mesh::ACTOR_SPAWN_MAX_IDLE;
    use crate::v1::proc_mesh::GET_ACTOR_STATE_MAX_IDLE;
    use crate::v1::testactor;
    use crate::v1::testing;

    #[tokio::test]
    #[cfg(fbcode_build)]
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
    #[cfg(fbcode_build)]
    async fn test_actor_states_with_panic() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance().await;
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) =
            instance.open_port::<resource::State<ActorState>>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
        let child_name = Name::new("child").unwrap();

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
            let events = actor_mesh.actor_states(&instance).await.unwrap();
            for state in events.values() {
                supervisor.send(instance, state.clone()).unwrap();
            }
        });
        // Make sure the task completes first without a panic.
        supervision_task.await.unwrap();

        for _ in 0..num_replicas {
            let state = RealClock
                .timeout(Duration::from_secs(10), supervision_receiver.recv())
                .await
                .expect("timeout")
                .unwrap();
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
    #[cfg(fbcode_build)]
    async fn test_actor_states_with_process_exit() {
        hyperactor_telemetry::initialize_logging_for_test();

        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(GET_ACTOR_STATE_MAX_IDLE, Duration::from_secs(1));

        let instance = testing::instance().await;
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) =
            instance.open_port::<resource::State<ActorState>>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
        let child_name = Name::new("child").unwrap();

        let actor_mesh = proc_mesh
            .spawn_with_name::<testactor::TestActor>(instance, child_name.clone(), &())
            .await
            .unwrap();

        actor_mesh
            .cast(
                instance,
                testactor::CauseSupervisionEvent(testactor::SupervisionEventType::ProcessExit(1)),
            )
            .unwrap();

        // Wait for the casted message to cause a process exit on all actors.
        // We can't use a reply port because the handler for the message will
        // by definition not complete and send a reply.
        #[allow(clippy::disallowed_methods)]
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Now that all ranks have completed, set up a continuous poll of the
        // status such that when a process switches to unhealthy it sets a
        // supervision event.
        let supervision_task = tokio::spawn(async move {
            let events = actor_mesh.actor_states(&instance).await.unwrap();
            for state in events.values() {
                supervisor.send(instance, state.clone()).unwrap();
            }
        });
        // Make sure the task completes first without a panic.
        RealClock
            .timeout(Duration::from_secs(10), supervision_task)
            .await
            .expect("timeout")
            .unwrap();

        for _ in 0..num_replicas {
            let state = RealClock
                .timeout(Duration::from_secs(10), supervision_receiver.recv())
                .await
                .expect("timeout")
                .unwrap();
            assert_matches!(state.status, resource::Status::Timeout(_));
            let events = state
                .state
                .expect("state should be present")
                .supervision_events;
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].actor_status, ActorStatus::Stopped);
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_states_on_sliced_mesh() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance().await;
        // Listen for supervision events sent to the parent instance.
        let (supervision_port, mut supervision_receiver) =
            instance.open_port::<resource::State<ActorState>>();
        let supervisor = supervision_port.bind();
        let num_replicas = 4;
        let meshes = testing::proc_meshes(instance, extent!(replicas = num_replicas)).await;
        let proc_mesh = &meshes[1];
        let child_name = Name::new("child").unwrap();

        let actor_mesh = proc_mesh
            .spawn_with_name::<testactor::TestActor>(instance, child_name.clone(), &())
            .await
            .unwrap();
        let sliced = actor_mesh
            .range("replicas", 1..3)
            .expect("slice should be valid");
        let sliced_replicas = sliced.len();

        sliced
            .cast(
                instance,
                testactor::CauseSupervisionEvent(testactor::SupervisionEventType::Panic),
            )
            .unwrap();

        // Wait for the casted message to cause a process exit on all actors.
        // We can't use a reply port because the handler for the message will
        // by definition not complete and send a reply.
        #[allow(clippy::disallowed_methods)]
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Now that all ranks have completed, set up a continuous poll of the
        // status such that when a process switches to unhealthy it sets a
        // supervision event.
        let supervision_task = tokio::spawn(async move {
            let events = sliced.actor_states(&instance).await.unwrap();
            for state in events.values() {
                supervisor.send(instance, state.clone()).unwrap();
            }
        });
        // Make sure the task completes first without a panic.
        RealClock
            .timeout(Duration::from_secs(10), supervision_task)
            .await
            .expect("timeout")
            .unwrap();

        for _ in 0..sliced_replicas {
            let state = RealClock
                .timeout(Duration::from_secs(10), supervision_receiver.recv())
                .await
                .expect("timeout")
                .unwrap();
            if let resource::Status::Failed(s) = state.status {
                assert!(s.contains("supervision events"));
            } else {
                panic!("Not failed: {:?}", state.status);
            }
            if let Some(ref inner) = state.state {
                assert!(!inner.supervision_events.is_empty());
                for event in &inner.supervision_events {
                    assert_eq!(event.actor_id.name(), format!("{}", child_name.clone()));
                    assert_matches!(event.actor_status, ActorStatus::Failed(_));
                }
            }
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_cast() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::bootstrap::MESH_BOOTSTRAP_ENABLE_PDEATHSIG, false);

        let instance = testing::instance().await;
        let mut host_mesh = testing::host_mesh(extent!(host = 4)).await;
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

    /// Test that undeliverable messages are properly returned to the
    /// sender when communication to a proc is broken.
    ///
    /// This is the V1 version of the test from
    /// hyperactor_multiprocess/src/proc_actor.rs::test_undeliverable_message_return.
    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_undeliverable_message_return() {
        use hyperactor::mailbox::MessageEnvelope;
        use hyperactor::mailbox::Undeliverable;
        use hyperactor::test_utils::pingpong::PingPongActor;
        use hyperactor::test_utils::pingpong::PingPongActorParams;
        use hyperactor::test_utils::pingpong::PingPongMessage;

        hyperactor_telemetry::initialize_logging_for_test();

        // Set message delivery timeout for faster test
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            std::time::Duration::from_secs(1),
        );

        let instance = testing::instance().await;

        // Create a proc mesh with 2 replicas.
        let meshes = testing::proc_meshes(instance, extent!(replicas = 2)).await;
        let proc_mesh = &meshes[1]; // Use the ProcessAllocator version

        // Set up undeliverable message port for collecting undeliverables
        let (undeliverable_port, mut undeliverable_rx) =
            instance.open_port::<Undeliverable<MessageEnvelope>>();

        // Spawn PingPongActors across both replicas.
        // Only actors on replica 0 will forward undeliverable messages.
        let ping_params = PingPongActorParams::new(Some(undeliverable_port.bind()), None);
        let pong_params = PingPongActorParams::new(None, None);

        // Spawn actors individually on each replica by spawning separate actor meshes
        // with specific proc selections.
        let ping_proc_mesh = proc_mesh.range("replicas", 0..1).unwrap();
        let pong_proc_mesh = proc_mesh.range("replicas", 1..2).unwrap();

        let ping_mesh = ping_proc_mesh
            .spawn::<PingPongActor>(instance, "ping", &ping_params)
            .await
            .unwrap();

        let pong_mesh = pong_proc_mesh
            .spawn::<PingPongActor>(instance, "pong", &pong_params)
            .await
            .unwrap();

        // Get individual actor refs
        let ping_handle = ping_mesh.values().next().unwrap();
        let pong_handle = pong_mesh.values().next().unwrap();

        // Verify ping-pong works initially
        let (done_tx, done_rx) = instance.open_once_port();
        ping_handle
            .send(
                instance,
                PingPongMessage(2, pong_handle.clone(), done_tx.bind()),
            )
            .unwrap();
        assert!(
            done_rx.recv().await.unwrap(),
            "Initial ping-pong should work"
        );

        // Now stop the pong actor mesh to break communication
        pong_mesh.stop(instance).await.unwrap();

        // Give it a moment to fully stop
        RealClock.sleep(std::time::Duration::from_millis(200)).await;

        // Send multiple messages that will all fail to be delivered
        let n = 100usize;
        for i in 1..=n {
            let ttl = 66 + i as u64; // Avoid ttl = 66 (which would cause other test behavior)
            let (once_tx, _once_rx) = instance.open_once_port();
            ping_handle
                .send(
                    instance,
                    PingPongMessage(ttl, pong_handle.clone(), once_tx.bind()),
                )
                .unwrap();
        }

        // Collect all undeliverable messages.
        // The fact that we successfully collect them proves the ping actor
        // is still running and handling undeliverables correctly (not crashing).
        let mut count = 0;
        let deadline = RealClock.now() + std::time::Duration::from_secs(5);
        while count < n && RealClock.now() < deadline {
            match RealClock
                .timeout(std::time::Duration::from_secs(1), undeliverable_rx.recv())
                .await
            {
                Ok(Ok(Undeliverable(envelope))) => {
                    let _: PingPongMessage = envelope.deserialized().unwrap();
                    count += 1;
                }
                Ok(Err(_)) => break, // Channel closed
                Err(_) => break,     // Timeout
            }
        }

        assert_eq!(
            count, n,
            "Expected {} undeliverable messages, got {}",
            n, count
        );
    }

    /// Test that actors not responding within stop timeout are
    /// forcibly aborted. This is the V1 equivalent of
    /// hyperactor_multiprocess/src/proc_actor.rs::test_stop_timeout.
    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_mesh_stop_timeout() {
        hyperactor_telemetry::initialize_logging_for_test();

        // Override ACTOR_SPAWN_MAX_IDLE to make test fast and
        // deterministic. ACTOR_SPAWN_MAX_IDLE is the maximum idle
        // time between status updates during mesh operations
        // (spawn/stop). When stop() is called, it waits for actors to
        // report they've stopped. If actors don't respond within this
        // timeout, they're forcibly aborted via JoinHandle::abort().
        // We set this to 1 second (instead of default 30s) so hung
        // actors (sleeping 5s in this test) get aborted quickly,
        // making the test fast.
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ACTOR_SPAWN_MAX_IDLE, std::time::Duration::from_secs(1));

        let instance = testing::instance().await;

        // Create proc mesh with 2 replicas
        let meshes = testing::proc_meshes(instance, extent!(replicas = 2)).await;
        let proc_mesh = &meshes[1]; // Use ProcessAllocator version

        // Spawn SleepActors across the mesh that will block longer
        // than timeout
        let sleep_mesh = proc_mesh
            .spawn::<testactor::SleepActor>(instance, "sleepers", &())
            .await
            .unwrap();

        // Send each actor a message to sleep for 5 seconds (longer
        // than 1-second timeout)
        for actor_ref in sleep_mesh.values() {
            actor_ref
                .send(instance, std::time::Duration::from_secs(5))
                .unwrap();
        }

        // Give actors time to start sleeping
        RealClock.sleep(std::time::Duration::from_millis(200)).await;

        // Count how many actors we spawned (for verification later)
        let expected_actors = sleep_mesh.values().count();

        // Now stop the mesh - actors won't respond in time, should be
        // aborted. Time this operation to verify abort behavior.
        let stop_start = RealClock.now();
        let result = sleep_mesh.stop(instance).await;
        let stop_duration = RealClock.now().duration_since(stop_start);

        // Stop will return an error because actors didn't stop within
        // the timeout. This is expected - the actors were forcibly
        // aborted, and V1 reports this as an error.
        match result {
            Ok(_) => {
                // It's possible actors stopped in time, but unlikely
                // given 5-second sleep vs 1-second timeout
                tracing::warn!("Actors stopped gracefully (unexpected but ok)");
            }
            Err(ref e) => {
                // Expected: timeout error indicating actors were aborted
                let err_str = format!("{:?}", e);
                assert!(
                    err_str.contains("Timeout"),
                    "Expected Timeout error, got: {:?}",
                    e
                );
                tracing::info!(
                    "Stop timed out as expected for {} actors, they were aborted",
                    expected_actors
                );
            }
        }

        // Verify that stop completed quickly (~1-2 seconds for
        // timeout + abort) rather than waiting the full 5 seconds for
        // actors to finish sleeping. This proves actors were aborted,
        // not waited for.
        assert!(
            stop_duration < std::time::Duration::from_secs(3),
            "Stop took {:?}, expected < 3s (actors should have been aborted, not waited for)",
            stop_duration
        );
        assert!(
            stop_duration >= std::time::Duration::from_millis(900),
            "Stop took {:?}, expected >= 900ms (should have waited for timeout)",
            stop_duration
        );
    }

    /// Test that actors stop gracefully when they respond to stop
    /// signals within the timeout. Complementary to
    /// test_actor_mesh_stop_timeout which tests abort behavior. V1
    /// equivalent of
    /// hyperactor_multiprocess/src/proc_actor.rs::test_stop
    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_actor_mesh_stop_graceful() {
        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance().await;

        // Create proc mesh with 2 replicas
        let meshes = testing::proc_meshes(instance, extent!(replicas = 2)).await;
        let proc_mesh = &meshes[1];

        // Spawn TestActors - these stop cleanly (no blocking
        // operations)
        let actor_mesh = proc_mesh
            .spawn::<testactor::TestActor>(instance, "test_actors", &())
            .await
            .unwrap();

        let expected_actors = actor_mesh.values().count();
        assert!(expected_actors > 0, "Should have spawned some actors");

        // Time the stop operation
        let stop_start = RealClock.now();
        let result = actor_mesh.stop(instance).await;
        let stop_duration = RealClock.now().duration_since(stop_start);

        // Graceful stop should succeed (return Ok)
        assert!(
            result.is_ok(),
            "Stop should succeed for responsive actors, got: {:?}",
            result.err()
        );

        // Verify stop completed quickly (< 2 seconds). Responsive
        // actors should stop almost immediately, not wait for
        // timeout.
        assert!(
            stop_duration < std::time::Duration::from_secs(2),
            "Graceful stop took {:?}, expected < 2s (actors should stop quickly)",
            stop_duration
        );

        tracing::info!(
            "Successfully stopped {} actors in {:?}",
            expected_actors,
            stop_duration
        );
    }
}
