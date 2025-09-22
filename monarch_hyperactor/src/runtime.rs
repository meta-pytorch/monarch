/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::time::Duration;

use anyhow::Result;
use anyhow::ensure;
use once_cell::unsync::OnceCell as UnsyncOnceCell;
use pyo3::PyResult;
use pyo3::Python;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyCFunction;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
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

pub fn shutdown_tokio_runtime() {
    if let Some(x) = INSTANCE.write().unwrap().take() {
        x.shutdown_timeout(Duration::from_secs(1));
    }
}

thread_local! {
    static IS_MAIN_THREAD: Cell<bool> = const { Cell::new(false) };
}

pub fn initialize(py: Python) -> Result<()> {
    // Initialize thread local state to identify the main Python thread.
    let threading = Python::import(py, "threading")?;
    let main_thread = threading.call_method0("main_thread")?;
    let current_thread = threading.getattr("current_thread")?.call0()?;
    ensure!(
        current_thread.is(&main_thread),
        "initialize called not on the main Python thread"
    );
    IS_MAIN_THREAD.set(true);

    let closure = PyCFunction::new_closure(
        py,
        None,
        None,
        |_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| {
            shutdown_tokio_runtime();
        },
    )
    .unwrap();
    let atexit = py.import("atexit").unwrap();
    atexit.call_method1("register", (closure,)).unwrap();
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
/// So, we check a thread-local to ensure we are on the main thread.
pub fn signal_safe_block_on<F>(py: Python, future: F) -> PyResult<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime = get_tokio_runtime();
    // Release the GIL, otherwise the work in `future` that tries to acquire the
    // GIL on another thread may deadlock.
    Python::allow_threads(py, || {
        if IS_MAIN_THREAD.get() {
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
