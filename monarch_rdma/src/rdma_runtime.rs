/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A shared, dedicated Tokio runtime for the RDMA manager actors.
//!
//! RDMA buffer registration scans `torch.cuda` segments under the GIL
//! (`backend::ibverbs::mlx_domain` -> `pytorch_cuda_segments`), so the RDMA actor
//! loops run here rather than on the shared control-plane runtime.
//!
//! # Invariants (RR-*)
//!
//! - **RR-1 (process-singleton):** one shared rdma runtime exists per process
//!   (built once, lazily), serving all RDMA actors rather than one per actor.
//!   Sharing is safe because RDMA keeps no per-thread state that would pin an
//!   actor to a worker, and the `QueuePairActor`s are cooperative (each re-arms a
//!   `Tick` and yields rather than busy-polling), so a small worker pool serves
//!   many without starving.
//! - **RR-2 (shared-runtime-routing):** the RDMA actors (`RdmaManagerActor`,
//!   `IbvManagerActor`, `QueuePairActor`) route `spawn_server_task` through
//!   [`spawn_on_rdma_runtime`], not ad hoc `tokio::spawn`.
//! - **RR-3 (rdma-is-data-plane):** [`spawn_on_rdma_runtime`] runs its future on
//!   a `DataPlane("rdma")` thread.
//! - **RR-4 (registration-off-control-plane):** consequently, the GIL taken
//!   during buffer registration lands on a `DataPlane("rdma")` thread, never the
//!   shared control-plane runtime (where a blocked GIL-holder would stall actor
//!   dispatch, supervision, and networking).
//! - **RR-5 (cross-runtime-joinhandle):** the `JoinHandle` from
//!   [`spawn_on_rdma_runtime`] is awaitable from the caller's runtime, so it is
//!   returned directly rather than bridged through a relay task.

use std::sync::OnceLock;

use hyperactor::runtime_identity::build_data_plane_runtime;

/// Worker-thread count for the shared rdma runtime. >1 so concurrent
/// `QueuePairActor` ticks make progress in parallel.
const RDMA_RUNTIME_WORKER_THREADS: usize = 4;

/// The process-wide rdma data-plane runtime, built on first use and tagged
/// `DataPlane("rdma")`.
fn rdma_runtime() -> &'static tokio::runtime::Handle {
    static RT: OnceLock<tokio::runtime::Handle> = OnceLock::new();
    RT.get_or_init(|| build_data_plane_runtime("rdma", RDMA_RUNTIME_WORKER_THREADS))
}

/// Spawn an actor server loop onto the shared rdma runtime and return its
/// `JoinHandle`. Used as `Actor::spawn_server_task` by the RDMA actors. The
/// rdma-runtime handle is returned directly: a tokio `JoinHandle` can be awaited
/// from any runtime, including the caller's.
///
/// Must not be called after `shutdown_data_plane_runtimes` has torn the rdma
/// runtime down: the cached handle would then point at a dropped runtime and
/// `spawn` would panic. Safe in practice because the RDMA actors are drained
/// before that atexit teardown runs.
pub(crate) fn spawn_on_rdma_runtime<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    rdma_runtime().spawn(future)
}

#[cfg(test)]
mod tests {
    use hyperactor::Actor;
    use hyperactor::runtime_identity::RuntimeKind;
    use hyperactor::runtime_identity::current_runtime_kind;

    use super::*;
    use crate::RdmaManagerActor;

    // RR invariant coverage:
    // RR-1 (process-singleton): structural (built once via OnceLock).
    // RR-2 (shared-runtime-routing): rdma_manager_routes_spawn_server_task_to_rdma_runtime;
    //   the generic actors share the identical override (structural).
    // RR-3 (rdma-is-data-plane): spawn_on_rdma_runtime_runs_on_data_plane.
    // RR-4 (registration-off-control-plane): structural (RR-2 + RR-3); the full
    //   end-to-end assertion is the Diff-4 fitness test (GPU/IB-gated).
    // RR-5 (cross-runtime-joinhandle): handle_awaitable_from_caller_runtime.

    // RR-3: a future routed through spawn_on_rdma_runtime runs on a DataPlane("rdma") thread.
    #[test]
    fn spawn_on_rdma_runtime_runs_on_data_plane() {
        let (tx, rx) = std::sync::mpsc::channel();
        let _join = spawn_on_rdma_runtime(async move {
            let _ = tx.send(current_runtime_kind());
        });
        assert_eq!(rx.recv().unwrap(), Some(RuntimeKind::DataPlane("rdma")));
    }

    // RR-2: RdmaManagerActor's spawn_server_task override routes onto the rdma
    // runtime (the default tokio::spawn would have no runtime here and panic).
    #[test]
    fn rdma_manager_routes_spawn_server_task_to_rdma_runtime() {
        let (tx, rx) = std::sync::mpsc::channel();
        let _join = <RdmaManagerActor as Actor>::spawn_server_task(async move {
            let _ = tx.send(current_runtime_kind());
        });
        assert_eq!(rx.recv().unwrap(), Some(RuntimeKind::DataPlane("rdma")));
    }

    // RR-5: the rdma-runtime JoinHandle is awaitable from a different runtime.
    #[tokio::test]
    async fn handle_awaitable_from_caller_runtime() {
        let out = spawn_on_rdma_runtime(async { 7 }).await.unwrap();
        assert_eq!(out, 7);
    }
}
