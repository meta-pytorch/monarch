/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![deny(clippy::disallowed_methods)]

//! Control-plane GIL accounting.
//!
//! The GIL wrappers (`monarch_with_gil` / `monarch_with_gil_blocking`) route every
//! Python acquisition through `check_gil_site`, which flags any acquisition on the
//! shared control-plane runtime whose `GilSite` is not sanctioned
//! (`is_control_plane_allowed`). This makes "no unsanctioned Python on the
//! control-plane runtime" a counted, debug-assertable invariant. Builds on the
//! `RuntimeKind` tagging in `hyperactor::runtime_identity`.
//!
//! # Invariants (GIL-*)
//!
//! - **GIL-1 (outermost-entry-accountable):** `check_gil_site` acts only at the
//!   outermost logical GIL entry (when `is_reentrant()` is false); a re-entrant
//!   acquisition under a sanctioned entry is attributed to that entry, not checked
//!   on its own.
//! - **GIL-2 (off-control-plane-exempt):** `check_gil_site` is a no-op on
//!   `DataPlane`/`Foreign` threads; only `RuntimeKind::ControlPlane` is policed.
//! - **GIL-3 (allowlist-exhaustive):** `is_control_plane_allowed` is a wildcard-free
//!   `match` over `GilSite`, so a new variant cannot compile until it is classified,
//!   and `GilSite` has no catch-all variant.
//! - **GIL-4 (unsanctioned-accounted):** an unsanctioned control-plane acquisition
//!   increments `GIL_ON_CONTROL_PLANE`, logs the caller location, and trips a
//!   `debug_assert` in debug builds.

use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use hyperactor::runtime_identity::RuntimeKind;
use hyperactor::runtime_identity::current_runtime_kind;
use pyo3::Python;
use pyo3::prelude::*;

/// Global lock to serialize GIL acquisition from Rust threads in async contexts.
///
/// Under high concurrency, many async tasks can simultaneously try to acquire the GIL.
/// Each call blocks the current tokio worker thread, which can cause runtime starvation
/// and apparent deadlocks (nothing else gets polled).
///
/// This wrapper serializes GIL acquisition among callers that opt in, so at most one
/// tokio task is blocked in `Python::attach` at a time, improving fairness under
/// contention.
///
/// Note: this does not globally prevent other sync code from calling `Python::attach`
/// directly. Use `monarch_with_gil` or `monarch_with_gil_blocking` for Python interaction
/// that occurs on async hot paths.
static GIL_LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

// Thread-local depth counter for re-entrant GIL acquisition.
//
// This tracks when we're already inside a `monarch_with_gil` or `monarch_with_gil_blocking`
// call. On re-entry (e.g., when Python calls back into Rust while we're already executing
// under `Python::attach`), we bypass the `GIL_LOCK` to avoid deadlocks.
//
// Without this, the following scenario would deadlock:
// 1. Rust async code calls `monarch_with_gil`, acquires `GIL_LOCK`
// 2. Inside the closure, Python code is executed
// 3. Python code calls back into Rust (e.g., via a PyO3 callback)
// 4. The callback tries to call `monarch_with_gil` again
// 5. DEADLOCK: waiting for `GIL_LOCK` which is held by the same logical call chain
thread_local! {
    static GIL_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard that decrements the GIL depth counter when dropped.
struct GilDepthGuard {
    prev_depth: u32,
}

impl Drop for GilDepthGuard {
    fn drop(&mut self) {
        GIL_DEPTH.with(|d| d.set(self.prev_depth));
    }
}

/// Increments the GIL depth counter and returns a guard that restores it on drop.
fn increment_gil_depth() -> GilDepthGuard {
    let prev_depth = GIL_DEPTH.with(|d| {
        let current = d.get();
        d.set(current + 1);
        current
    });
    GilDepthGuard { prev_depth }
}

/// Returns true if we're already inside a `monarch_with_gil` call (re-entrant).
fn is_reentrant() -> bool {
    GIL_DEPTH.with(|d| d.get() > 0)
}

/// A sanctioned GIL acquisition operation, identifying *why* a
/// `monarch_with_gil`/`monarch_with_gil_blocking` site takes the GIL. Each
/// variant names one distinct operation so `is_control_plane_allowed` can
/// classify it. There is deliberately no catch-all: a new GIL site must add a
/// variant and classify it, or the code will not compile.
///
/// This sanctioned set is transitional and meant to shrink: the goal is to move
/// Python off the control-plane runtime entirely, retiring these operations over
/// time, so the allow-list (and this enum) trends toward empty. New variants
/// should be rare and short-lived.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GilSite {
    /// Dispatch a message to a Python endpoint (direct dispatch).
    EndpointDispatch,
    /// Build a queued message for queue-dispatch mode.
    QueueDispatch,
    /// Start the Python dispatch loop during actor init.
    DispatchInit,
    /// Run an actor's `__cleanup__`.
    EndpointCleanup,
    /// Run `__supervise__`, or inspect a supervision result.
    Supervise,
    /// Render an actor's display name.
    DisplayName,
    /// Construct a `PythonActor` (unpickle the actor type).
    ActorConstruct,
    /// Bootstrap a client actor, or convert bootstrap addresses.
    Bootstrap,
    /// Create or clone a `TaskLocals` (asyncio event loop).
    TaskLocals,
    /// Convert a port reply into a Python value.
    ReplyConvert,
    /// Run a comm reducer.
    Reducer,
    /// Run an accumulator.
    Accumulate,
    /// Convert a Rust value into a Python value (or clone a Python value).
    Convert,
    /// Capture, format, or clone a Python traceback or exception.
    Traceback,
    /// Drive a Python coroutine or blocking task on the bridge.
    AwaitDrive,
    /// Cancel pending tasks, stop an event loop, or interrupt the client.
    Stop,
    /// Interactive debugger read/write.
    Debugger,
    /// Get or set a Python logger.
    Logging,
    /// Run the code-sync auto-reloader.
    CodeSync,
    /// Register or operate RDMA buffers (runs on the data plane).
    Rdma,
    /// Worker-process startup: torch import, env init. Runs on the
    /// control-plane runtime (the `WorkerActor` has no data-plane override).
    WorkerInit,
    /// Tensor/Python work on the `StreamActor` data-plane runtime.
    StreamCompute,
    /// Test-only GIL use.
    Test,
}

/// Counts GIL acquisitions for unsanctioned operations on the control-plane
/// runtime. Exposed to Python via `get_gil_on_control_plane` for tests.
static GIL_ON_CONTROL_PLANE: AtomicU64 = AtomicU64::new(0);

/// Whether `site` is allowed to take the GIL on the control-plane runtime.
///
/// This `match` has no wildcard arm on purpose: adding a `GilSite` variant must
/// fail to compile until it is classified here. `true` is the transitional
/// sanctioned set of operations that run on the control plane today; `false`
/// marks operations that must run elsewhere (the data plane).
fn is_control_plane_allowed(site: GilSite) -> bool {
    match site {
        // Dispatch and reply.
        GilSite::EndpointDispatch
        | GilSite::QueueDispatch
        | GilSite::DispatchInit
        | GilSite::EndpointCleanup
        | GilSite::ReplyConvert => true,
        // Supervision and lifecycle.
        GilSite::Supervise
        | GilSite::DisplayName
        | GilSite::ActorConstruct
        | GilSite::Bootstrap
        | GilSite::Stop => true,
        // Accumulation and reduction.
        GilSite::Reducer | GilSite::Accumulate => true,
        // Coroutine driving and value conversion.
        GilSite::TaskLocals | GilSite::Convert | GilSite::Traceback | GilSite::AwaitDrive => true,
        // Auxiliary control-plane services.
        GilSite::Debugger | GilSite::Logging | GilSite::CodeSync => true,
        // Tests run their GIL work on control-plane-tagged threads.
        GilSite::Test => true,
        // Worker-process startup runs on the control-plane runtime (the
        // WorkerActor has no data-plane override); sanctioned one-time setup.
        GilSite::WorkerInit => true,
        // Data-plane subsystems own a runtime and must never take the GIL on
        // the control plane; a control-plane grab here is the regression this
        // net catches.
        GilSite::Rdma | GilSite::StreamCompute => false,
    }
}

/// Account for a GIL acquisition at `site`. On the control-plane runtime, an
/// unsanctioned `site` (per `is_control_plane_allowed`) bumps
/// `GIL_ON_CONTROL_PLANE`, logs a warning, and trips a `debug_assert`. Only the
/// outermost logical entry is checked; re-entrant acquisitions are skipped.
#[track_caller]
fn check_gil_site(site: GilSite) {
    if is_reentrant() {
        return;
    }
    if current_runtime_kind() != RuntimeKind::ControlPlane {
        return;
    }
    if is_control_plane_allowed(site) {
        return;
    }
    GIL_ON_CONTROL_PLANE.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        site = ?site,
        caller = %std::panic::Location::caller(),
        "unsanctioned GIL on control-plane runtime"
    );
    debug_assert!(
        false,
        "unsanctioned GIL on control-plane: {:?} at {}",
        site,
        std::panic::Location::caller()
    );
}

/// Async wrapper around `Python::attach` intended for async call sites.
///
/// Why: under high concurrency, many async tasks can simultaneously
/// try to acquire the GIL. Each call blocks the current tokio worker
/// thread, which can cause runtime starvation / apparent deadlocks
/// (nothing else gets polled).
///
/// This wrapper serializes GIL acquisition among async callers so at most one tokio
/// task is blocked in `Python::attach` at a time, preventing runtime starvation
/// under GIL contention.
///
/// Note: this does not globally prevent other sync code from calling
/// `Python::attach` directly, so the control-plane GIL accounting
/// (`check_gil_site`) covers only acquisitions that route through these
/// wrappers; a raw `Python::attach` elsewhere bypasses it. Use this wrapper
/// for Python interaction that occurs on async hot paths.
///
/// # Re-entrancy Safety
///
/// This function is re-entrant safe. If called while already inside a `monarch_with_gil`
/// or `monarch_with_gil_blocking` call (e.g., from a Python→Rust callback), it bypasses
/// the `GIL_LOCK` to avoid deadlocks.
///
/// # Example
/// ```ignore
/// let result = monarch_with_gil(GilSite::Convert, |py| {
///     // Do work with Python GIL
///     Ok(42)
/// })
/// .await?;
/// ```
// No `#[track_caller]` (unlike the blocking wrapper): it does not propagate through
// `async fn`, so the logged `GilSite` is the diagnostic for async sites.
#[allow(clippy::disallowed_methods)]
pub async fn monarch_with_gil<F, R>(site: GilSite, f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R + Send,
{
    check_gil_site(site);

    // If we're already inside a monarch_with_gil call (re-entrant), skip the lock
    // to avoid deadlock from Python→Rust callbacks
    if is_reentrant() {
        let _depth_guard = increment_gil_depth();
        return Python::attach(f);
    }

    // Not re-entrant: acquire the serialization lock
    let _lock_guard = GIL_LOCK.lock().await;
    let _depth_guard = increment_gil_depth();
    Python::attach(f)
}

/// Blocking wrapper around `Python::with_gil` for use in synchronous contexts.
///
/// Unlike `monarch_with_gil`, this function does NOT use the `GIL_LOCK` async mutex.
/// Since it is blocking call, it simply acquires the GIL and releases it when the
/// closure returns.
///
/// Note: like `monarch_with_gil`, the control-plane GIL accounting
/// (`check_gil_site`) covers only acquisitions that route through these wrappers;
/// a raw `Python::attach` elsewhere bypasses it.
///
/// # Example
/// ```ignore
/// let result = monarch_with_gil_blocking(GilSite::Convert, |py| {
///     // Do work with Python GIL
///     Ok(42)
/// })?;
/// ```
#[track_caller]
#[allow(clippy::disallowed_methods)]
pub fn monarch_with_gil_blocking<F, R>(site: GilSite, f: F) -> R
where
    // No `Send` bound (unlike `monarch_with_gil`): the closure runs on the
    // current thread and never crosses a thread boundary.
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    check_gil_site(site);

    let _depth_guard = increment_gil_depth();
    Python::attach(f)
}

/// Number of unsanctioned GIL acquisitions seen on the control-plane runtime.
/// For tests; see `GIL_ON_CONTROL_PLANE`.
#[pyfunction]
#[pyo3(name = "_get_gil_on_control_plane")]
pub fn get_gil_on_control_plane() -> u64 {
    GIL_ON_CONTROL_PLANE.load(Ordering::Relaxed)
}

/// Reset the unsanctioned control-plane GIL counter to zero. For tests.
#[pyfunction]
#[pyo3(name = "_reset_gil_on_control_plane")]
pub fn reset_gil_on_control_plane() {
    GIL_ON_CONTROL_PLANE.store(0, Ordering::Relaxed);
}

/// Force one unsanctioned control-plane GIL acquisition and count it, for the
/// negative fitness test. Runs on a freshly `ControlPlane`-tagged thread and
/// routes through `check_gil_site` so the real gating is exercised; the
/// debug-build `debug_assert` panic is swallowed so the increment sticks (in
/// release builds there is no assert and the counter bumps directly).
#[pyfunction]
#[pyo3(name = "_force_unsanctioned_gil_on_control_plane")]
pub fn force_unsanctioned_gil_on_control_plane() {
    std::thread::spawn(|| {
        hyperactor::runtime_identity::tag_current_thread(RuntimeKind::ControlPlane);
        let _ = std::panic::catch_unwind(|| check_gil_site(GilSite::Rdma));
    })
    .join()
    .unwrap();
}

#[cfg(test)]
mod tests {
    use hyperactor::runtime_identity::RuntimeKind;
    use hyperactor::runtime_identity::tag_current_thread;

    use super::*;

    // GIL invariant coverage:
    // GIL-1 (outermost-entry-accountable): reentrant_unsanctioned_is_skipped.
    // GIL-2 (off-control-plane-exempt): unsanctioned_site_off_control_plane_is_silent.
    // GIL-3 (allowlist-exhaustive): allowed_classification; structural (wildcard-free match, no catch-all).
    // GIL-4 (unsanctioned-accounted): unsanctioned_site_on_control_plane_panics; allowed_site_on_control_plane_is_silent.

    // The classification is the source of truth for the net: dispatch is
    // sanctioned; Rdma is not (it must run on the data plane).
    // GIL-3:
    #[test]
    fn allowed_classification() {
        assert!(is_control_plane_allowed(GilSite::EndpointDispatch));
        assert!(is_control_plane_allowed(GilSite::Test));
        assert!(!is_control_plane_allowed(GilSite::Rdma));
    }

    // An allowlisted site on a control-plane thread is a no-op: the counter does
    // not move and `check_gil_site` does not trip the debug_assert. Run on a
    // freshly tagged thread (the test thread itself is Foreign) and assert a
    // delta so concurrent tests on the global counter do not interfere.
    // GIL-4:
    #[test]
    fn allowed_site_on_control_plane_is_silent() {
        let before = GIL_ON_CONTROL_PLANE.load(Ordering::Relaxed);
        std::thread::spawn(|| {
            tag_current_thread(RuntimeKind::ControlPlane);
            check_gil_site(GilSite::EndpointDispatch);
        })
        .join()
        .unwrap();
        let after = GIL_ON_CONTROL_PLANE.load(Ordering::Relaxed);
        assert_eq!(after, before, "allowed site must not bump the counter");
    }

    // An unsanctioned site (Rdma) on a control-plane thread trips the
    // debug_assert (which fires in test builds).
    // GIL-4:
    #[test]
    fn unsanctioned_site_on_control_plane_panics() {
        // Run on a throwaway thread so the ControlPlane tag never leaks into a
        // sibling test; the debug_assert fires inside it, so join() returns Err.
        let res = std::thread::spawn(|| {
            tag_current_thread(RuntimeKind::ControlPlane);
            check_gil_site(GilSite::Rdma);
        })
        .join();
        assert!(
            res.is_err(),
            "unsanctioned control-plane GIL must trip the debug_assert"
        );
    }

    // Off the control plane the runtime-kind gate returns first, so even an
    // unsanctioned site neither panics nor moves the counter.
    // GIL-2:
    #[test]
    fn unsanctioned_site_off_control_plane_is_silent() {
        let before = GIL_ON_CONTROL_PLANE.load(Ordering::Relaxed);

        // The test thread is Foreign.
        check_gil_site(GilSite::Rdma);

        // A DataPlane-tagged thread is likewise exempt.
        std::thread::spawn(|| {
            tag_current_thread(RuntimeKind::DataPlane("test-dp"));
            check_gil_site(GilSite::Rdma);
        })
        .join()
        .unwrap();

        let after = GIL_ON_CONTROL_PLANE.load(Ordering::Relaxed);
        assert_eq!(after, before, "off control plane must not bump the counter");
    }

    // The reentrancy gate fires before the runtime-kind and allowlist checks: a
    // re-entrant acquisition under a sanctioned entry is attributed to that
    // entry, so an unsanctioned site under it is skipped, even on the control
    // plane. Run on a freshly tagged thread so the depth guard is the only
    // reason the check is skipped.
    // GIL-1:
    #[test]
    fn reentrant_unsanctioned_is_skipped() {
        std::thread::spawn(|| {
            tag_current_thread(RuntimeKind::ControlPlane);
            let _g = increment_gil_depth();
            let before = GIL_ON_CONTROL_PLANE.load(Ordering::Relaxed);
            check_gil_site(GilSite::Rdma);
            let after = GIL_ON_CONTROL_PLANE.load(Ordering::Relaxed);
            assert_eq!(after, before, "reentrant acquisition must not be checked");
        })
        .join()
        .unwrap();
    }
}
