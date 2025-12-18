/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use once_cell::unsync::OnceCell as UnsyncOnceCell;
use pyo3::PyResult;
use pyo3::Python;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3_async_runtimes::TaskLocals;
use tokio::task;

// this must be a RwLock and only return a guard for reading the runtime.
// Otherwise multiple threads can deadlock fighting for the Runtime object if they hold it
// while blocking on something.
static INSTANCE: std::sync::LazyLock<RwLock<Option<tokio::runtime::Runtime>>> =
    std::sync::LazyLock::new(|| RwLock::new(None));

pub fn get_tokio_runtime<'l>() -> std::sync::MappedRwLockReadGuard<'l, tokio::runtime::Runtime> {
    // First try to get a read lock and check if runtime exists
    {
        let read_guard = INSTANCE.read().unwrap();
        if read_guard.is_some() {
            return RwLockReadGuard::map(read_guard, |lock: &Option<tokio::runtime::Runtime>| {
                lock.as_ref().unwrap()
            });
        }
        // Drop the read lock by letting it go out of scope
    }

    // Runtime doesn't exist, upgrade to write lock to initialize
    let mut write_guard = INSTANCE.write().unwrap();
    if write_guard.is_none() {
        *write_guard = Some(
            tokio::runtime::Builder::new_multi_thread()
                .thread_name_fn(|| {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("monarch-pytokio-worker-{}", id)
                })
                .enable_all()
                .build()
                .unwrap(),
        );
    }

    // Downgrade write lock to read lock and return the reference
    let read_guard = std::sync::RwLockWriteGuard::downgrade(write_guard);
    RwLockReadGuard::map(read_guard, |lock: &Option<tokio::runtime::Runtime>| {
        lock.as_ref().unwrap()
    })
}

#[pyfunction]
pub fn shutdown_tokio_runtime() {
    // It is important to not hold the GIL while calling this function.
    // Other runtime threads may be waiting to acquire it and we will never get to shutdown.
    if let Some(x) = INSTANCE.write().unwrap().take() {
        x.shutdown_timeout(Duration::from_secs(1));
    }
}

/// Stores the native thread ID of the main Python thread.
/// This is lazily initialized on first call to `is_main_thread`.
static MAIN_THREAD_NATIVE_ID: OnceLock<i64> = OnceLock::new();

/// Returns the native thread ID of the main Python thread.
/// On first call, looks it up via `threading.main_thread().native_id`.
fn get_main_thread_native_id() -> i64 {
    *MAIN_THREAD_NATIVE_ID.get_or_init(|| {
        Python::with_gil(|py| {
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
    Python::allow_threads(py, || {
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
                            Python::with_gil(|py| {py.check_signals()})?;
                            #[allow(clippy::disallowed_methods)]
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
            #[allow(clippy::disallowed_methods)]
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    // Use signal_safe_block_on to run the future, which should make it
    // interruptible by signals like SIGINT
    signal_safe_block_on(py, future)
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
            .try_with(|c| {
                c.get()
                    .map(|locals| Python::with_gil(|py| locals.clone_ref(py)))
            })
            .unwrap_or_default()
    }
}

pub fn future_into_py<F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    pyo3_async_runtimes::generic::future_into_py::<SimpleRuntime, F, T>(py, fut)
}
