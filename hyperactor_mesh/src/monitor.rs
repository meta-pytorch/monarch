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
use hyperactor::actor::ActorStatus;
use hyperactor::context;
use hyperactor::monitor::MonitorFailure;
use hyperactor::supervision::ActorSupervisionEvent;
use rankspace::view::CompactView;

use crate::supervision::MeshFailure;

/// A liveness monitor over a set of ranked actors.
///
/// A `MeshMonitor` owns one [`ActorMonitor`] per rank, keyed by the same
/// [`RankSpace`](rankspace::RankSpace) as the mesh it observes. The monitors run
/// until the `MeshMonitor` is dropped. Awaiting a `&MeshMonitor` resolves with the
/// first rank failure, reported as a [`MeshFailure`] that names the rank; use
/// [`MeshMonitor::guard`] to run a future until it completes or a rank fails.
pub struct MeshMonitor {
    /// One monitor per visible rank, keyed by the mesh's rank space.
    monitors: CompactView<Vec<ActorMonitor>>,
}

impl MeshMonitor {
    /// Spawn one [`ActorMonitor`] per visible rank as a child of `cx`.
    ///
    /// `actors` holds the actor addresses in the mesh's rank space, so the
    /// resulting monitors share that space: each rank reports failures under the
    /// same rank it holds in the mesh.
    pub fn spawn(cx: &impl context::Actor, actors: CompactView<Vec<ActorAddr>>) -> Self {
        let monitors = actors.map(|actor| ActorMonitor::spawn(cx, actor.clone()));
        Self { monitors }
    }

    /// Wait for the first monitored rank to fail, reported as a [`MeshFailure`].
    ///
    /// Internal helper behind the [`IntoFuture`] impl for `&MeshMonitor` and
    /// [`Self::guard`], the public ways to observe failures. An empty monitor set
    /// never resolves: a mesh with no ranks has no rank that can fail.
    async fn wait_for_failure(&self) -> MeshFailure {
        if self.monitors.data().is_empty() {
            return std::future::pending().await;
        }
        let waits = self.monitors.iter().map(|(rank, monitor)| {
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
        monitor_failure_to_mesh_failure(rank.get(), failure)
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

fn monitor_failure_to_mesh_failure(rank: usize, failure: MonitorFailure) -> MeshFailure {
    let actor_id = failure.actor_id().clone();
    let actor_status = match failure {
        MonitorFailure::ActorStopped { status, .. } => status,
        MonitorFailure::ActorFailed { status, .. } if status.is_failed() => status,
        failure => ActorStatus::generic_failure(failure.to_string()),
    };
    // The failed actor is identified by `event.actor_id` and the rank; the mesh
    // itself has no name to carry (data meshes are identified by their members).
    MeshFailure {
        actor_mesh_name: None,
        event: ActorSupervisionEvent::new(actor_id, None, actor_status, None),
        crashed_ranks: vec![rank],
        // Synthesized locally from direct per-rank monitoring, not a
        // controller report.
        reporting_controller: None,
    }
}

#[cfg(all(test, fbcode_build))]
mod tests {
    use hyperactor::Proc;
    use ndslice::Region;
    use ndslice::extent;
    use rankspace::RankSpace;
    use tokio::time::Duration;

    use super::*;
    use crate::testactor;

    #[tokio::test]
    async fn test_mesh_monitor_reports_rank_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let target = client.spawn_with_label("rank0", testactor::TestActor);
        let region: Region = extent!(replicas = 1).into();
        let space = RankSpace::from(region);
        let actors = CompactView::new(space, vec![target.actor_addr().clone()]).unwrap();
        let monitor = MeshMonitor::spawn(&client, actors);

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

        tokio::time::timeout(Duration::from_secs(5), target)
            .await
            .expect("timed out waiting for target to stop");
    }
}
