/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime-identity tagging: which Tokio runtime a thread belongs to.
//!
//! Monarch runs Python on more than one Tokio runtime: the shared control-plane
//! runtime that drives `PythonActor` dispatch, and separate data-plane runtimes
//! where GIL work runs safely, off the control plane (the tensor streams; the
//! RDMA managers, during buffer registration). The
//! hazard is contention on the shared runtime, where a thread holding the GIL
//! while blocked stalls every actor loop, supervisor, and network task on it.
//! So data-plane work runs on its own runtime, and control-plane GIL use is
//! kept to brief, sanctioned sites. This module records a thread's
//! [`RuntimeKind`] (stamped at thread start via `on_thread_start`, read via
//! [`current_runtime_kind`]) so GIL-entry sites can tell the two apart. An
//! unstamped thread is not owned by a Monarch runtime and reads as `None`.
//!
//! # Invariants (RI-*)
//!
//! - **RI-1 (unstamped-is-none):** [`current_runtime_kind`] returns `None` on
//!   any thread no runtime builder has tagged.
//! - **RI-2 (tagging-is-thread-local-and-once):** [`tag_current_thread`] sets
//!   only the current OS thread's marker, and only once.
//! - **RI-3 (data-plane-workers-tagged):** [`build_data_plane_runtime`] stamps
//!   every worker thread of the runtime it returns `DataPlane(label)`, via the
//!   builder's `on_thread_start`.
//! - **RI-4 (tagging-does-not-leak):** spawning work onto a tagged runtime does
//!   not change the spawning thread's observed kind.
//! - **RI-5 (process-lifetime-runtime):** the handle from
//!   [`build_data_plane_runtime`] is kept alive by the `DATA_PLANE_RUNTIMES`
//!   registry and stays valid until the runtime is torn down by
//!   [`shutdown_data_plane_runtimes`] at process teardown; callers must not spawn
//!   onto it afterward.
//! - **RI-6 (block-on-host-tagged):** a data-plane runtime hosted on a manually
//!   spawned thread that drives its work via `rt.block_on` must tag that hosting
//!   thread explicitly; `on_thread_start` stamps only the runtime's own worker
//!   threads, not the `block_on` caller.
//! - **RI-7 (teardown-registered):** every runtime from
//!   [`build_data_plane_runtime`] is registered in `DATA_PLANE_RUNTIMES` and shut
//!   down by [`shutdown_data_plane_runtimes`]. The pyo3 layer calls that at
//!   Python teardown before `Py_Finalize`; this crate stays Python-free.

use std::cell::OnceCell;
use std::sync::Mutex;
use std::time::Duration;

/// Which kind of Tokio runtime a thread belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RuntimeKind {
    /// The shared control-plane runtime that drives actor dispatch.
    /// Control-plane GIL use is kept to brief, sanctioned sites.
    ControlPlane,
    /// A runtime off the control plane. GIL work here is safe because it does
    /// not drive actor dispatch (the tensor streams; the RDMA managers during
    /// buffer registration).
    DataPlane(&'static str),
}

thread_local! {
    // A thread is unstamped until a runtime builder's `on_thread_start` tags it,
    // so non-runtime threads (e.g. Python-owned asyncio threads) read as `None`.
    static KIND: OnceCell<RuntimeKind> = const { OnceCell::new() };
}

/// Stamp the current thread with `kind`. Call from a runtime builder's
/// `on_thread_start` so every worker of that runtime carries the marker.
pub fn tag_current_thread(kind: RuntimeKind) {
    KIND.with(|k| k.set(kind).expect("runtime kind already tagged"));
}

/// The [`RuntimeKind`] of the current thread, if it is stamped.
pub fn current_runtime_kind() -> Option<RuntimeKind> {
    KIND.with(|k| k.get().copied())
}

/// Data-plane runtimes built by [`build_data_plane_runtime`], retained so they
/// stay alive and can be torn down at process teardown via
/// [`shutdown_data_plane_runtimes`].
static DATA_PLANE_RUNTIMES: Mutex<Vec<tokio::runtime::Runtime>> = Mutex::new(Vec::new());

/// Build a dedicated data-plane runtime on its own OS thread, tagged
/// `DataPlane(label)`, and return a handle for spawning onto it.
///
/// The runtime runs on a standalone `std::thread` rather than a Tokio
/// `spawn_blocking` thread: a runtime-managed blocking thread is awaited at
/// teardown, so uninterruptible FFI (CUDA, NCCL, ibverbs) running on it would
/// hang shutdown forever. A standalone thread is not awaited. Its worker threads
/// are stamped `DataPlane(label)` via `on_thread_start`. The returned
/// [`Handle`](tokio::runtime::Handle) stays valid until the runtime (retained in
/// `DATA_PLANE_RUNTIMES`) is torn down by [`shutdown_data_plane_runtimes`] at
/// process teardown.
pub fn build_data_plane_runtime(
    label: &'static str,
    worker_threads: usize,
) -> tokio::runtime::Handle {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(label.to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .on_thread_start(move || tag_current_thread(RuntimeKind::DataPlane(label)))
                .enable_all()
                .build()
                .expect("failed to build data-plane runtime");
            let handle = rt.handle().clone();
            // Stash the owned runtime so it stays alive (its worker threads keep
            // running) and can be torn down at process teardown via
            // shutdown_data_plane_runtimes; the builder thread then exits.
            DATA_PLANE_RUNTIMES
                .lock()
                .expect("DATA_PLANE_RUNTIMES poisoned")
                .push(rt);
            tx.send(handle)
                .expect("failed to hand back data-plane runtime handle");
        })
        .expect("failed to spawn data-plane runtime thread");
    rx.recv()
        .expect("failed to receive data-plane runtime handle")
}

/// Shut down every data-plane runtime built by [`build_data_plane_runtime`],
/// waiting up to `timeout` for each. Idempotent: a second call finds an empty
/// registry and does nothing. Pure Rust (no Python); the pyo3 layer must call
/// this at Python teardown, before `Py_Finalize`, so data-plane workers stop
/// before the interpreter is finalized.
pub fn shutdown_data_plane_runtimes(timeout: Duration) {
    // Drain under the lock, then shut down outside it (shutdown_timeout blocks).
    let runtimes: Vec<tokio::runtime::Runtime> = DATA_PLANE_RUNTIMES
        .lock()
        .expect("DATA_PLANE_RUNTIMES poisoned")
        .drain(..)
        .collect();
    shutdown_runtimes(runtimes, timeout);
}

fn shutdown_runtimes(runtimes: Vec<tokio::runtime::Runtime>, timeout: Duration) {
    for rt in runtimes {
        rt.shutdown_timeout(timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // RI invariant coverage:
    // RI-1 (unstamped-is-none): tag_and_read_round_trip; build_data_plane_runtime_tags_workers.
    // RI-2 (tagging-is-thread-local-and-once): tag_and_read_round_trip;
    //   tag_current_thread_panics_when_retagged.
    // RI-3 (data-plane-workers-tagged): build_data_plane_runtime_tags_workers.
    // RI-4 (tagging-does-not-leak): build_data_plane_runtime_tags_workers.
    // RI-5 (process-lifetime-runtime): structural (the registry keeps it alive;
    //   the tests rely on the runtime staying alive).
    // RI-6 (block-on-host-tagged): on_thread_start_does_not_tag_the_block_on_thread.
    // RI-7 (teardown-registered): shutdown_aborts_in_flight_task;
    //   shutdown_runtimes_empty_is_noop. (the global registry drain is a thin
    //   Vec::drain wrapper, exercised structurally.)

    // RI-1/RI-2: a fresh thread is unstamped; tagging then reads back on that thread.
    #[test]
    fn tag_and_read_round_trip() {
        // A fresh thread is unstamped.
        assert_eq!(current_runtime_kind(), None);
        tag_current_thread(RuntimeKind::ControlPlane);
        assert_eq!(current_runtime_kind(), Some(RuntimeKind::ControlPlane));
    }

    #[test]
    #[should_panic(expected = "runtime kind already tagged")]
    fn tag_current_thread_panics_when_retagged() {
        tag_current_thread(RuntimeKind::ControlPlane);
        tag_current_thread(RuntimeKind::DataPlane("test-dp"));
    }

    // RI-3/RI-4: workers are stamped DataPlane(label) while the caller stays unstamped.
    #[test]
    fn build_data_plane_runtime_tags_workers() {
        // The calling (test) thread is unstamped.
        assert_eq!(current_runtime_kind(), None);

        let handle = build_data_plane_runtime("test-dp", 1);
        let (tx, rx) = std::sync::mpsc::channel();
        handle.spawn(async move {
            let _ = tx.send(current_runtime_kind());
        });

        // A task on the data-plane runtime observes its DataPlane stamp,
        assert_eq!(rx.recv().unwrap(), Some(RuntimeKind::DataPlane("test-dp")));
        // while the stamp stays confined to that runtime's worker threads.
        assert_eq!(current_runtime_kind(), None);
    }

    // RI-6 (block-on-host-tagged): `rt.block_on` drives its future on the CALLING
    // thread, which is not a runtime worker, so `on_thread_start` does not cover
    // it. A thread that runs work via `block_on` (the StreamActor pattern) must
    // tag itself explicitly; the `on_thread_start` tag alone leaves it unstamped.
    #[test]
    fn on_thread_start_does_not_tag_the_block_on_thread() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .on_thread_start(|| tag_current_thread(RuntimeKind::DataPlane("x")))
                .enable_all()
                .build()
                .unwrap();
            // block_on runs on this (the calling) thread, which on_thread_start
            // does not tag -> still unstamped.
            let on_caller = rt.block_on(async { current_runtime_kind() });
            // a spawned task runs on a worker -> DataPlane.
            let on_worker = rt.block_on(async {
                tokio::spawn(async { current_runtime_kind() })
                    .await
                    .unwrap()
            });
            let _ = tx.send((on_caller, on_worker));
        });
        let (on_caller, on_worker) = rx.recv().unwrap();
        assert_eq!(on_caller, None);
        assert_eq!(on_worker, Some(RuntimeKind::DataPlane("x")));
    }

    // RI-7: shut down a runtime with an in-flight task, and confirm the task is
    // aborted. Built locally (not via the shared registry) so the test is
    // isolated. Race-safe: wait for the task to signal it is live and pending
    // before shutting down.
    #[test]
    fn shutdown_aborts_in_flight_task() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (live_tx, live_rx) = std::sync::mpsc::channel();
        let reached_end = Arc::new(AtomicBool::new(false));
        let reached_end_task = reached_end.clone();
        rt.spawn(async move {
            let _ = live_tx.send(()); // signal live before the long await
            tokio::time::sleep(Duration::from_secs(30)).await;
            reached_end_task.store(true, Ordering::SeqCst); // only if not aborted
        });
        live_rx.recv().unwrap(); // task is live and pending at the await

        shutdown_runtimes(vec![rt], Duration::from_millis(200));

        assert!(!reached_end.load(Ordering::SeqCst));
    }

    // RI-7: shutting down an empty set is a no-op (the idempotent second call).
    #[test]
    fn shutdown_runtimes_empty_is_noop() {
        shutdown_runtimes(vec![], Duration::from_secs(1));
    }
}
