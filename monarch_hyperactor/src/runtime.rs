/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::OnceCell as UnsyncOnceCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use hyperactor::runtime_identity::RuntimeKind;
use hyperactor::runtime_identity::shutdown_data_plane_runtimes;
use hyperactor::runtime_identity::tag_current_thread;
pub use monarch_gil::GilSite;
pub use monarch_gil::force_unsanctioned_gil_on_control_plane;
pub use monarch_gil::get_gil_on_control_plane;
pub use monarch_gil::monarch_with_gil;
pub use monarch_gil::monarch_with_gil_blocking;
pub use monarch_gil::reset_gil_on_control_plane;
use pyo3::PyResult;
use pyo3::Python;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3_async_runtimes::TaskLocals;
use tokio::runtime::Handle;
use tokio::task;

use crate::config::TOKIO_WORKER_THREADS;

/// Attribute holding an actor's event loop on the `threading.Thread` running
/// it. `threading` already tracks every live thread, so this needs no second
/// registry and takes no lock on the actor-spawn path.
const ACTOR_EVENT_LOOP_ATTRIBUTE: &str = "_monarch_actor_event_loop";
const ACTOR_EVENT_LOOP_POLL_INTERVAL: Duration = Duration::from_millis(1);
const ACTOR_EVENT_LOOP_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);

/// Global tokio runtime container.
///
/// `handle` is cheap to clone and is what callers receive from
/// `get_tokio_runtime()`. Holding a `Handle` does not lock anything, so
/// concurrent block_on calls from different threads do not contend.
///
/// `runtime` exists only so the atexit handler can take ownership and
/// call `shutdown_timeout`. Under normal operation nothing locks it; the
/// mutex is uncontended at shutdown.
struct GlobalRuntime {
    handle: Handle,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
}

static INSTANCE: OnceLock<GlobalRuntime> = OnceLock::new();

fn global_runtime() -> &'static GlobalRuntime {
    INSTANCE.get_or_init(|| {
        let worker_threads = hyperactor_config::global::get(TOKIO_WORKER_THREADS);
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = worker_threads {
            builder.worker_threads(worker_threads.get());
        }
        let runtime = builder
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("monarch-pytokio-worker-{}", id)
            })
            // The shared control-plane runtime: stamp its workers (and
            // blocking-pool threads) so GIL-entry sites can tell they are on the
            // control plane. See `hyperactor::runtime_identity`.
            .on_thread_start(|| tag_current_thread(RuntimeKind::ControlPlane))
            .enable_all()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        GlobalRuntime {
            handle,
            runtime: Mutex::new(Some(runtime)),
        }
    })
}

pub fn get_tokio_runtime() -> Handle {
    global_runtime().handle.clone()
}

/// Record an actor's event loop on the `threading.Thread` running it, so the
/// interpreter-exit reaper can find it again.
///
/// This does not keep the loop alive any longer than it already lives:
/// `TaskLocals` holds it for the life of the `PythonActor` regardless. What it
/// buys is that no separate process-lifetime registry exists.
pub(crate) fn mark_actor_event_loop_thread(
    thread: &Bound<'_, PyAny>,
    event_loop: &Bound<'_, PyAny>,
) -> PyResult<()> {
    thread.setattr(ACTOR_EVENT_LOOP_ATTRIBUTE, event_loop)
}

fn actor_event_loops(py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
    let threading = py.import("threading")?;
    let mut loops = Vec::new();
    for thread in threading.call_method0("enumerate")?.try_iter()? {
        let thread = thread?;
        let Ok(event_loop) = thread.getattr(ACTOR_EVENT_LOOP_ATTRIBUTE) else {
            continue;
        };
        loops.push(event_loop.unbind());
    }
    Ok(loops)
}

/// Stop every marked actor loop, polling until they exit or `timeout` elapses.
/// Returns how many were still enumerable when the pass ended.
///
/// Scope: this covers loops enumerable *during* the pass. The first empty
/// snapshot returns, so a loop created after that observation is missed.
/// That residual is deliberate -- closing it would mean proving actor-loop
/// creation had quiesced first, which is a process-wide gate this guard
/// intentionally does not have. Actor-owned teardown is what will make late
/// creation safe; this is only the interpreter-exit backstop.
///
/// Latency: the deadline is checked once per pass, after a full enumeration
/// and one `call_soon_threadsafe` per loop, so the bound is `timeout` plus one
/// pass rather than exactly `timeout`.
///
/// Every wait is bounded and there are no thread joins: the pass only sleeps.
/// `Thread.join()` and `Thread.is_alive()` are both avoided because on CPython
/// 3.14 `is_alive()` can enter an unbounded OS-thread join once
/// `thread_is_exiting` is set.
fn stop_and_wait_actor_event_loops(py: Python<'_>, timeout: Duration) -> PyResult<usize> {
    let deadline = Instant::now() + timeout;
    loop {
        let loops = actor_event_loops(py)?;
        if loops.is_empty() {
            return Ok(0);
        }
        for event_loop in &loops {
            let event_loop = event_loop.bind(py);
            // `stop` must run on the loop's own thread, so schedule it rather
            // than call it. A closed loop raises, which is the normal state
            // for an actor that already drained.
            let stop_result = event_loop
                .getattr("stop")
                .and_then(|stop| event_loop.call_method1("call_soon_threadsafe", (stop,)));
            if let Err(err) = stop_result {
                tracing::warn!(error = %err, "failed to stop actor event loop at interpreter exit");
            }
        }
        if Instant::now() >= deadline {
            // Re-enumerate: threads may have exited while this pass issued its
            // stops, and the survivor count is what the caller reports.
            return Ok(actor_event_loops(py)?.len());
        }

        // Releasing the GIL is what lets the loop threads run the scheduled stop.
        let sleep_for =
            ACTOR_EVENT_LOOP_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now()));
        py.detach(move || thread::sleep(sleep_for));
    }
}

/// Reap actor event-loop threads before the interpreter finalizes.
///
/// `Py_FinalizeEx` runs Python `atexit` handlers *before* it publishes the
/// finalizing thread state, so while this runs every loop thread can still
/// take the GIL, run the scheduled `stop`, return from `run_forever` and exit.
/// After that point a surviving daemon thread that asks for the GIL is
/// force-exited with `pthread_exit`, and that unwind through pyo3's
/// `catch_unwind` aborts the process.
///
/// This is an interpreter-exit guard, not the lifecycle repair: actor cleanup
/// still returns before its thread terminates, and the root client still
/// publishes `Stopped` without running cleanup.
fn shutdown_actor_event_loops(py: Python<'_>, timeout: Duration) {
    let remaining = match stop_and_wait_actor_event_loops(py, timeout) {
        Ok(remaining) => remaining,
        Err(err) => {
            tracing::warn!(error = %err, "failed to stop actor event-loop threads at interpreter exit");
            return;
        }
    };
    if remaining > 0 {
        // Only reachable if a callback on that loop never yields, which is a
        // bug of its own. Naming it turns a bare SIGABRT into something
        // diagnosable rather than hanging the exit path.
        tracing::warn!(
            remaining,
            "actor event-loop threads survived interpreter-exit shutdown"
        );
    }
}

/// atexit handler that tears down the data-plane runtimes and the global Tokio runtime.
///
/// Callers obtain a cloned `Handle` from `get_tokio_runtime()` rather
/// than a guard, so the `runtime` mutex is uncontended at shutdown. We
/// can take ownership of the `Runtime` and call `shutdown_timeout`
/// directly. If a worker thread is still inside `Handle::block_on` on a
/// future that never resolves (e.g. a non-main thread that cannot
/// observe SIGINT), `shutdown_timeout` aborts spawned tasks and returns
/// after at most one second; the stuck worker is then a daemon thread
/// that CPython kills on interpreter exit.
#[pyfunction]
pub fn shutdown_tokio_runtime(py: Python<'_>) {
    // Called from Python's atexit, which holds the GIL. Release it so tokio
    // worker threads can acquire it to complete their Python work.
    py.detach(|| {
        // Tear down the data-plane runtimes (e.g. rdma) first, while the
        // control-plane runtime is still intact, so their GIL-taking workers
        // stop before Py_Finalize.
        shutdown_data_plane_runtimes(Duration::from_secs(1));
        let Some(global) = INSTANCE.get() else {
            return;
        };
        let Some(rt) = global.runtime.lock().unwrap().take() else {
            return;
        };
        rt.shutdown_timeout(Duration::from_secs(1));
    });

    // Reaping runs unconditionally. The field intervention that took this
    // failure from 6/60 to 0/60 was an unconditional stop-and-join, and the
    // bounded deadline caps the worst case.
    shutdown_actor_event_loops(py, ACTOR_EVENT_LOOP_SHUTDOWN_TIMEOUT);
}

/// Stores the native thread ID of the main Python thread.
/// This is lazily initialized on first call to `is_main_thread`.
static MAIN_THREAD_NATIVE_ID: OnceLock<i64> = OnceLock::new();

/// Returns the native thread ID of the main Python thread.
/// On first call, looks it up via `threading.main_thread().native_id`.
fn get_main_thread_native_id() -> i64 {
    *MAIN_THREAD_NATIVE_ID.get_or_init(|| {
        monarch_with_gil_blocking(GilSite::Bootstrap, |py| {
            let threading = py.import("threading").expect("failed to import threading");
            let main_thread = threading
                .call_method0("main_thread")
                .expect("failed to get main_thread");
            main_thread
                .getattr("native_id")
                .expect("failed to get native_id")
                .extract::<i64>()
                .expect("native_id is not an i64")
        })
    })
}

/// Returns the current thread's native ID in a cross-platform way.
#[cfg(target_os = "linux")]
fn get_current_thread_id() -> i64 {
    nix::unistd::gettid().as_raw() as i64
}

/// Returns the current thread's native ID in a cross-platform way.
#[cfg(target_os = "macos")]
fn get_current_thread_id() -> i64 {
    let mut tid: u64 = 0;
    // pthread_threadid_np with thread=0 (null pthread_t) gets the current thread's ID.
    unsafe {
        let ret = libc::pthread_threadid_np(0, &mut tid);
        debug_assert_eq!(
            ret, 0,
            "pthread_threadid_np failed with error code: {}",
            ret
        );
    }
    // macOS thread IDs are u64 so we need to convert to i64.
    debug_assert!(tid <= i64::MAX as u64, "thread ID {} exceeds i64::MAX", tid);
    tid as i64
}

/// Returns the current thread's native ID in a cross-platform way.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("get_current_thread_id is only implemented for Linux and macOS");

/// Returns true if the current thread is the main Python thread.
/// Compares the current thread's native ID against the main Python thread's native ID.
pub fn is_main_thread() -> bool {
    let current_tid = get_current_thread_id();
    current_tid == get_main_thread_native_id()
}

pub fn initialize(py: Python) -> Result<()> {
    // Eagerly initialize the main thread ID while we're on the main thread
    // with the GIL held. If this were lazily initialized on a background
    // tokio thread during shutdown, the `py.import("threading")` call inside
    // get_main_thread_native_id() would trigger module_from_spec on a
    // partially-finalized interpreter, causing a segfault.
    let _ = get_main_thread_native_id();

    let atexit = py.import("atexit")?;
    let shutdown_fn = wrap_pyfunction!(shutdown_tokio_runtime, py)?;
    atexit.call_method1("register", (shutdown_fn,))?;
    Ok(())
}

/// Block the current thread on a future, but make sure to check for signals
/// originating from the Python signal handler.
///
/// Python's signal handler just sets a flag that it expects the Python
/// interpreter to handle later via a call to `PyErr_CheckSignals`. When we
/// enter into potentially long-running native code, we need to make sure to be
/// checking for signals frequently, otherwise we will ignore them. This will
/// manifest as `ctrl-C` not doing anything.
///
/// One additional wrinkle is that `PyErr_CheckSignals` only works on the main
/// Python thread; if it's called on any other thread it silently does nothing.
/// So, we check if we're on the main thread by comparing native thread IDs.
pub fn signal_safe_block_on<F>(py: Python, future: F) -> PyResult<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime = get_tokio_runtime();
    // Release the GIL, otherwise the work in `future` that tries to acquire the
    // GIL on another thread may deadlock.
    py.detach(|| {
        if is_main_thread() {
            // Spawn the future onto the tokio runtime
            let handle = runtime.spawn(future);
            // Block the current thread on waiting for *either* the future to
            // complete or a signal.
            runtime.block_on(async {
                tokio::select! {
                    result = handle => result.map_err(|e| PyRuntimeError::new_err(format!("JoinErr: {:?}", e))),
                    signal = async {
                        let sleep_for = std::time::Duration::from_millis(100);
                        loop {
                            // Acquiring the GIL in a loop is sad, hopefully once
                            // every 100ms is fine.
                            monarch_with_gil_blocking(GilSite::AwaitDrive, |py| py.check_signals())?;
                            tokio::time::sleep(sleep_for).await;
                        }
                    } => signal
                }
            })
        } else {
            // If we're not on the main thread, we can just block it. We've
            // released the GIL, so the Python main thread will continue on, and
            // `PyErr_CheckSignals` doesn't do anything anyway.
            Ok(runtime.block_on(future))
        }
    })
}

/// A test function that sleeps indefinitely in a loop.
/// This is used for testing signal handling in signal_safe_block_on.
/// The function will sleep forever until interrupted by a signal.
#[pyfunction]
pub fn sleep_indefinitely_for_unit_tests(py: Python) -> PyResult<()> {
    // Create a future that sleeps indefinitely
    let future = async {
        loop {
            tracing::info!("idef sleeping for 100ms");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    // Use signal_safe_block_on to run the future, which should make it
    // interruptible by signals like SIGINT
    signal_safe_block_on(py, future)
}

/// Test hook for the interpreter-exit regression test.
///
/// `threading.Event.wait()` releases the GIL, so keeping this PyO3 frame on
/// the stack while it waits is what puts a `catch_unwind` trampoline in the
/// path of CPython's finalization unwind. Test-only; not part of any API.
#[pyfunction]
#[pyo3(name = "_wait_on_event_for_exit_test")]
fn wait_on_event_for_exit_test(
    entered: &Bound<'_, PyAny>,
    release: &Bound<'_, PyAny>,
) -> PyResult<()> {
    entered.call_method0("set")?;
    release.call_method0("wait")?;
    Ok(())
}

/// Initialize the runtime module and expose Python functions
pub fn register_python_bindings(runtime_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    let sleep_indefinitely_fn =
        wrap_pyfunction!(sleep_indefinitely_for_unit_tests, runtime_mod.py())?;
    sleep_indefinitely_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.runtime",
    )?;
    runtime_mod.add_function(sleep_indefinitely_fn)?;

    let wait_on_event_for_exit_test_fn =
        wrap_pyfunction!(wait_on_event_for_exit_test, runtime_mod.py())?;
    wait_on_event_for_exit_test_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.runtime",
    )?;
    runtime_mod.add_function(wait_on_event_for_exit_test_fn)?;

    let get_gil_on_control_plane_fn = wrap_pyfunction!(get_gil_on_control_plane, runtime_mod.py())?;
    get_gil_on_control_plane_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.runtime",
    )?;
    runtime_mod.add_function(get_gil_on_control_plane_fn)?;

    let reset_gil_on_control_plane_fn =
        wrap_pyfunction!(reset_gil_on_control_plane, runtime_mod.py())?;
    reset_gil_on_control_plane_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.runtime",
    )?;
    runtime_mod.add_function(reset_gil_on_control_plane_fn)?;

    let force_unsanctioned_gil_on_control_plane_fn =
        wrap_pyfunction!(force_unsanctioned_gil_on_control_plane, runtime_mod.py())?;
    force_unsanctioned_gil_on_control_plane_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.runtime",
    )?;
    runtime_mod.add_function(force_unsanctioned_gil_on_control_plane_fn)?;

    Ok(())
}

struct SimpleRuntime;

impl pyo3_async_runtimes::generic::Runtime for SimpleRuntime {
    type JoinError = task::JoinError;
    type JoinHandle = task::JoinHandle<()>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        get_tokio_runtime().spawn(async move {
            fut.await;
        })
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        get_tokio_runtime().spawn_blocking(f)
    }
}

tokio::task_local! {
    static TASK_LOCALS: UnsyncOnceCell<TaskLocals>;
}

impl pyo3_async_runtimes::generic::ContextExt for SimpleRuntime {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        let cell = UnsyncOnceCell::new();
        cell.set(locals).unwrap();

        Box::pin(TASK_LOCALS.scope(cell, fut))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        TASK_LOCALS
            .try_with(|c| c.get().cloned())
            .unwrap_or_default()
    }
}

pub fn future_into_py<F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    pyo3_async_runtimes::generic::future_into_py::<SimpleRuntime, F, T>(py, fut)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use hyperactor::runtime_identity::RuntimeKind;
    use hyperactor::runtime_identity::current_runtime_kind;
    use pyo3::types::PyDict;

    use super::*;

    // The reaper is process-wide over `threading.enumerate()`, so two of these
    // running concurrently would see each other's threads. A test-only static
    // is fine; the no-new-global-state rule is about production code.
    static ACTOR_EVENT_LOOP_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn start_marked_thread(
        py: Python<'_>,
        target: &Bound<'_, PyAny>,
        event_loop: &Bound<'_, PyAny>,
    ) {
        let kwargs = PyDict::new(py);
        kwargs
            .set_item("target", target)
            .expect("thread target should be set");
        kwargs
            .set_item("daemon", true)
            .expect("thread should be made a daemon");
        let thread = py
            .import("threading")
            .expect("threading should import")
            .call_method("Thread", (), Some(&kwargs))
            .expect("thread should be created");
        mark_actor_event_loop_thread(&thread, event_loop)
            .expect("actor event loop should attach to its owning thread");
        thread.call_method0("start").expect("thread should start");
    }

    /// A marked thread actually running `run_forever`, so a scheduled `stop`
    /// reaches it and the thread exits.
    fn new_actor_event_loop(py: Python<'_>) -> Py<PyAny> {
        let event_loop = py
            .import("asyncio")
            .expect("asyncio should import")
            .call_method0("new_event_loop")
            .expect("event loop should be created");
        let target = event_loop
            .getattr("run_forever")
            .expect("event loop should have run_forever");
        start_marked_thread(py, &target, &event_loop);
        event_loop.unbind()
    }

    /// A marked thread wedged on `Event.wait()`, carrying a loop that never
    /// runs, so the scheduled `stop` can never execute and the thread outlives
    /// any deadline.
    fn new_wedged_actor_event_loop(py: Python<'_>) -> (Py<PyAny>, Py<PyAny>) {
        let event_loop = py
            .import("asyncio")
            .expect("asyncio should import")
            .call_method0("new_event_loop")
            .expect("event loop should be created");
        let release = py
            .import("threading")
            .expect("threading should import")
            .call_method0("Event")
            .expect("release event should be created");
        let target = release.getattr("wait").expect("event should have wait");
        start_marked_thread(py, &target, &event_loop);
        (event_loop.unbind(), release.unbind())
    }

    fn marked_loop_count(py: Python<'_>) -> usize {
        actor_event_loops(py)
            .expect("actor event-loop threads should enumerate")
            .len()
    }

    #[test]
    fn stop_and_wait_actor_event_loops_reaps_every_thread() {
        let _guard = ACTOR_EVENT_LOOP_TEST_LOCK
            .lock()
            .expect("actor event-loop tests should not poison their lock");
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let loops = [new_actor_event_loop(py), new_actor_event_loop(py)];

            assert_eq!(
                stop_and_wait_actor_event_loops(py, Duration::from_secs(1))
                    .expect("actor event-loop shutdown should succeed"),
                0,
                "all marked actor event-loop threads should exit before the deadline"
            );
            assert_eq!(
                marked_loop_count(py),
                0,
                "stopped actor event-loop threads should leave threading.enumerate()"
            );
            for event_loop in loops {
                event_loop
                    .bind(py)
                    .call_method0("close")
                    .expect("stopped event loop should close");
            }
        });
    }

    #[test]
    fn stop_and_wait_actor_event_loops_returns_at_deadline() {
        let _guard = ACTOR_EVENT_LOOP_TEST_LOCK
            .lock()
            .expect("actor event-loop tests should not poison their lock");
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let (event_loop, release) = new_wedged_actor_event_loop(py);
            let started = Instant::now();

            assert_eq!(
                stop_and_wait_actor_event_loops(py, Duration::from_millis(10))
                    .expect("actor event-loop timeout should be observed"),
                1,
                "a wedged marked thread should survive the shutdown deadline"
            );
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "actor event-loop shutdown should return near its deadline"
            );

            release
                .bind(py)
                .call_method0("set")
                .expect("wedged thread should be released");
            assert_eq!(
                stop_and_wait_actor_event_loops(py, Duration::from_secs(1))
                    .expect("released actor event-loop thread should be observed"),
                0,
                "released marked thread should exit"
            );
            event_loop
                .bind(py)
                .call_method0("close")
                .expect("unused event loop should close");
        });
    }

    // The documented bound is `timeout + one pass`. Measure the one-pass term
    // with many loops so it is a behaviour rather than a claim, and so a
    // regression to per-thread timeouts (N * timeout) would be caught.
    #[test]
    fn stop_and_wait_actor_event_loops_pass_cost_is_bounded_with_many_loops() {
        const LOOPS: usize = 32;
        let _guard = ACTOR_EVENT_LOOP_TEST_LOCK
            .lock()
            .expect("actor event-loop tests should not poison their lock");
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let wedged: Vec<_> = (0..LOOPS)
                .map(|_| new_wedged_actor_event_loop(py))
                .collect();
            let started = Instant::now();

            assert_eq!(
                stop_and_wait_actor_event_loops(py, Duration::from_millis(50))
                    .expect("actor event-loop timeout should be observed"),
                LOOPS,
                "every wedged marked thread should survive the shutdown deadline"
            );
            // Per-thread timeouts would cost LOOPS * 50ms = 1.6s here.
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "one pass over {LOOPS} loops should be small next to the deadline, got {:?}",
                started.elapsed()
            );

            for (_, release) in &wedged {
                release
                    .bind(py)
                    .call_method0("set")
                    .expect("wedged thread should be released");
            }
            assert_eq!(
                stop_and_wait_actor_event_loops(py, Duration::from_secs(5))
                    .expect("released actor event-loop threads should be observed"),
                0,
                "released marked threads should exit"
            );
            for (event_loop, _) in wedged {
                event_loop
                    .bind(py)
                    .call_method0("close")
                    .expect("unused event loop should close");
            }
        });
    }

    // The shared control-plane runtime stamps its worker threads ControlPlane.
    #[test]
    fn global_runtime_workers_are_control_plane() {
        let kind = get_tokio_runtime().block_on(async {
            tokio::spawn(async { current_runtime_kind() })
                .await
                .unwrap()
        });
        assert_eq!(kind, Some(RuntimeKind::ControlPlane));
    }

    // on_thread_start also reaches the blocking pool, so GIL work on a
    // spawn_blocking thread is still seen as control-plane.
    #[test]
    fn global_runtime_blocking_pool_is_control_plane() {
        let kind = get_tokio_runtime().block_on(async {
            tokio::task::spawn_blocking(current_runtime_kind)
                .await
                .unwrap()
        });
        assert_eq!(kind, Some(RuntimeKind::ControlPlane));
    }
}
