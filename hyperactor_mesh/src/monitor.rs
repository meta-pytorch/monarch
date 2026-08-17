/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Multicast liveness monitoring.
//!
//! [`MeshMonitor`] mirrors [`hyperactor::ActorMonitor`] but observes a set of
//! ranked actors rather than a single one. It composes: one `ActorMonitor` per
//! rank, fanning each per-rank [`MonitorFailure`] out into a mesh-level
//! [`MeshFailure`] that names the failed rank. Multicast monitoring is, for now,
//! many unicast monitors; a later diff can optimize the fan-out with casting.

use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

use futures::future::select_all;
use hyperactor::ActorAddr;
use hyperactor::ActorMonitor;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::mailbox::Message;
use hyperactor::mailbox::PortReceiver;
use hyperactor::monitor::MonitorFailure;
use hyperactor::monitor::MonitorStatus;
use hyperactor::supervision::ActorSupervisionEvent;
use ndslice::ViewExt as _;
use ndslice::view::Ranked as _;
use ndslice::view::Region;

use crate::ActorMeshRef;
use crate::ValueMesh;
use crate::supervision::MeshFailure;

/// A liveness monitor over a set of ranked actors.
///
/// A `MeshMonitor` logically monitors one actor per rank in a region. Monitoring
/// runs until the `MeshMonitor` is dropped. Awaiting a `&MeshMonitor` resolves
/// with the first rank failure, reported as a [`MeshFailure`] that names the
/// rank; use [`MeshMonitor::guard`] to run a future until it completes or a rank
/// fails.
pub struct MeshMonitor {
    /// One monitor per rank, keyed by the mesh's region.
    monitors: ValueMesh<ActorMonitor>,
}

impl MeshMonitor {
    /// Spawn one [`ActorMonitor`] per rank as a child of `cx`.
    ///
    /// `actors` holds the actor addresses in the mesh's region, so the resulting
    /// monitors share that region: each rank reports failures under the same rank
    /// it holds in the mesh.
    pub fn spawn(cx: &impl context::Actor, actors: ValueMesh<ActorAddr>) -> Self {
        let region = actors.region().clone();
        let monitors = actors
            .values()
            .map(|actor| ActorMonitor::spawn(cx, actor.clone()))
            .collect();
        Self {
            monitors: ValueMesh::new(region, monitors)
                .expect("actor monitors preserve actor mesh cardinality"),
        }
    }

    /// Return the monitored region.
    pub fn region(&self) -> &Region {
        self.monitors.region()
    }

    /// Return the latest observed status for `rank`.
    pub fn status(&self, rank: usize) -> Option<ActorStatus> {
        self.monitors
            .get(rank)
            .map(|monitor| match monitor.status() {
                MonitorStatus::Checking => ActorStatus::Unknown,
                MonitorStatus::Alive(status) => status,
                MonitorStatus::Failed(failure) => monitor_failure_to_actor_status(&failure),
            })
    }

    /// Wait for the first monitored rank to fail, reported as a [`MeshFailure`].
    ///
    /// Internal helper behind the [`IntoFuture`] impl for `&MeshMonitor` and
    /// [`Self::guard`], the public ways to observe failures. An empty monitor set
    /// never resolves: a mesh with no ranks has no rank that can fail.
    async fn wait_for_failure(&self) -> MeshFailure {
        if self.region().num_ranks() == 0 {
            return std::future::pending().await;
        }
        let waits = (0..self.region().num_ranks()).map(|rank| {
            let monitor = self
                .monitors
                .get(rank)
                .expect("dense actor monitor mesh has one value per rank");
            Box::pin(async move {
                // Guarding a never-completing future resolves only when the
                // monitor reports a failure.
                let failure = monitor
                    .guard(std::future::pending::<()>())
                    .await
                    .expect_err("pending future never completes");
                (rank, failure)
            })
        });
        let ((rank, failure), _index, _rest) = select_all(waits).await;
        monitor_failure_to_mesh_failure(rank, &failure)
    }

    /// Run `fut` until it completes or any monitored rank fails.
    pub async fn guard<F>(&self, fut: F) -> Result<F::Output, MeshFailure>
    where
        F: Future,
    {
        tokio::pin!(fut);
        tokio::select! {
            result = fut => Ok(result),
            failure = self.wait_for_failure() => Err(failure),
        }
    }

    /// Cast `message` and collect one response per rank unless a rank fails.
    ///
    /// `receiver` must receive exactly one response from each actor in
    /// `actor_mesh`. Responses are returned in arrival order. The outer result
    /// reports cast or mailbox errors; the inner result distinguishes all-rank
    /// completion from a monitored rank failure. This monitor must have been
    /// created from `actor_mesh`.
    pub async fn cast_and_collect<A, M, R>(
        &self,
        actor_mesh: &ActorMeshRef<A>,
        cx: &impl context::Actor,
        message: M,
        receiver: &mut PortReceiver<R>,
    ) -> crate::Result<Result<Vec<R>, MeshFailure>>
    where
        A: Referable + RemoteHandles<M>,
        M: RemoteMessage + Clone,
        R: Message,
    {
        actor_mesh.cast(cx, message)?;
        let num_ranks = actor_mesh.region().num_ranks();
        let replies = async {
            let mut replies = Vec::with_capacity(num_ranks);
            for _ in 0..num_ranks {
                replies.push(receiver.recv().await?);
            }
            Ok::<_, hyperactor::mailbox::MailboxError>(replies)
        };

        match self.guard(replies).await {
            Ok(Ok(replies)) => Ok(Ok(replies)),
            Ok(Err(error)) => Err(Box::new(error).into()),
            Err(failure) => Ok(Err(failure)),
        }
    }
}

/// Awaiting a `&MeshMonitor` resolves with the first rank failure. The borrow
/// keeps the monitors alive for the duration of the await.
impl<'a> IntoFuture for &'a MeshMonitor {
    type Output = MeshFailure;
    type IntoFuture = Pin<Box<dyn Future<Output = MeshFailure> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait_for_failure())
    }
}

fn monitor_failure_to_mesh_failure(rank: usize, failure: &MonitorFailure) -> MeshFailure {
    let actor_id = failure.actor_id().clone();
    let actor_status = monitor_failure_to_actor_status(failure);
    // The failed actor is identified by `event.actor_id` and the rank; the mesh
    // itself has no name to carry.
    MeshFailure {
        actor_mesh_name: None,
        event: ActorSupervisionEvent::new(actor_id, None, actor_status, None),
        crashed_ranks: vec![rank],
        // Synthesized locally from direct per-rank monitoring, not a
        // controller report.
        reporting_controller: None,
    }
}

fn monitor_failure_to_actor_status(failure: &MonitorFailure) -> ActorStatus {
    match failure {
        MonitorFailure::ActorStopped { status, .. } => status.clone(),
        MonitorFailure::ActorFailed { status, .. } if status.is_failed() => status.clone(),
        failure => ActorStatus::generic_failure(failure.to_string()),
    }
}

#[cfg(all(test, fbcode_build))]
mod tests {
    use std::collections::HashSet;

    use hyperactor::ActorRef;
    use hyperactor::Proc;
    use ndslice::Region;
    use ndslice::extent;
    use tokio::time::Duration;

    use super::*;
    use crate::testactor;

    #[tokio::test]
    async fn test_mesh_monitor_reports_rank_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let target = client.spawn_with_label("rank0", testactor::TestActor);
        let region: Region = extent!(replicas = 1).into();
        let actors = ValueMesh::new(region, vec![target.actor_addr().clone()]).unwrap();
        let monitor = MeshMonitor::spawn(&client, actors);
        assert_eq!(monitor.status(0), Some(ActorStatus::Unknown));

        // No failure should be observed while the actor is alive.
        let mut wait = (&monitor).into_future();
        tokio::select! {
            biased;
            failure = &mut wait => panic!("unexpected failure before stop: {failure:?}"),
            _ = tokio::task::yield_now() => {}
        }

        target
            .drain_and_stop("rank complete")
            .expect("target should accept stop");

        let failure = tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("timed out waiting for mesh monitor failure");
        assert_eq!(failure.crashed_ranks, vec![0]);
        assert!(matches!(
            failure.event.actor_status,
            ActorStatus::Stopped(ref reason) if reason == "rank complete"
        ));
        assert!(matches!(
            monitor.status(0).expect("rank 0 should remain monitored"),
            ActorStatus::Stopped(ref reason) if reason == "rank complete"
        ));

        tokio::time::timeout(Duration::from_secs(5), target)
            .await
            .expect("timed out waiting for target to stop");
    }

    #[tokio::test]
    async fn test_cast_and_collect_returns_all_replies_or_rank_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let rank0 = client.spawn_with_label("rank0", testactor::TestActor);
        let rank1 = client.spawn_with_label("rank1", testactor::TestActor);
        let actor_refs: Vec<ActorRef<testactor::TestActor>> = vec![rank0.bind(), rank1.bind()];
        let expected_actor_addrs: HashSet<_> = actor_refs
            .iter()
            .map(|actor_ref| actor_ref.actor_addr().clone())
            .collect();
        let region: Region = extent!(replicas = 2).into();
        let actor_mesh = ActorMeshRef::try_new_data(region, actor_refs)
            .expect("data actor mesh should be valid");
        let monitor = actor_mesh.monitor(&client);

        let (port, mut receiver) = client.open_port();
        let replies = tokio::time::timeout(
            Duration::from_secs(10),
            monitor.cast_and_collect(
                &actor_mesh,
                &client,
                testactor::GetActorId(port.bind()),
                &mut receiver,
            ),
        )
        .await
        .expect("timed out waiting for all rank replies")
        .expect("cast and receive should succeed")
        .expect("all ranks should remain alive");
        let actor_addrs: HashSet<_> = replies
            .into_iter()
            .map(|(actor_addr, _seq_info)| actor_addr)
            .collect();
        assert_eq!(actor_addrs, expected_actor_addrs);

        let (_port, mut receiver): (_, PortReceiver<()>) = client.open_port();
        let mut pending =
            std::pin::pin!(monitor.cast_and_collect(&actor_mesh, &client, (), &mut receiver,));
        tokio::select! {
            biased;
            result = &mut pending => panic!("cast unexpectedly completed: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        rank1
            .drain_and_stop("rank complete")
            .expect("rank 1 should accept stop");
        let failure = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .expect("timed out waiting for a rank failure")
            .expect("cast should initiate")
            .expect_err("rank failure should win before all replies arrive");
        assert_eq!(failure.crashed_ranks, vec![1]);
        assert!(matches!(
            failure.event.actor_status,
            ActorStatus::Stopped(ref reason) if reason == "rank complete"
        ));

        rank0
            .drain_and_stop("test complete")
            .expect("rank 0 should accept stop");
        tokio::time::timeout(Duration::from_secs(5), rank0)
            .await
            .expect("timed out waiting for rank 0 to stop");
        tokio::time::timeout(Duration::from_secs(5), rank1)
            .await
            .expect("timed out waiting for rank 1 to stop");
    }
}
