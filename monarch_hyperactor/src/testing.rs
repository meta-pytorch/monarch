/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Private test-support bindings.
//!
//! Three unrelated pieces live here:
//!
//! 1. `TestStruct`, a minimal PyO3 struct for testing `@rust_struct` mixin
//!    patching. The `#[pyclass]` module is set to the Python file that defines
//!    the `@rust_struct`-decorated class so the name-validation check passes.
//! 2. `_HandleProbe`, a closed probe that mints a real Rust-produced `Handle`
//!    for the `Future`/`Handle` contract suite.
//! 3. `_make_delayed_handle`, a `Handle` that publishes after registering its
//!    first waiter, for the `returns_future` overhead benchmark.
//!
//! Everything here is private support: nothing is re-exported from `monarch`
//! and no production Python imports it.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::handle::HandleCore;
use crate::handle::PyHandle;
use crate::handle::send_result;
use crate::runtime::get_tokio_runtime;
use crate::runtime::signal_safe_block_on;

/// The value a successful probe publishes. Fixed, because the probe accepts no
/// caller-supplied producer or value.
pub(crate) const PROBE_SUCCESS_VALUE: i64 = 4242;

/// The closed set of terminal outcomes the probe can produce.
///
/// Deliberately not a caller-supplied coroutine, awaitable, callable, future or
/// producer function: the probe exists to put a real `PyHandle` into one of
/// three reviewed terminal states, not to drive arbitrary work.
#[derive(Clone, Copy)]
enum ProbeOutcome {
    Success,
    Exception,
    BaseException,
}

impl ProbeOutcome {
    fn parse(name: &str) -> PyResult<Self> {
        match name {
            "success" => Ok(Self::Success),
            "exception" => Ok(Self::Exception),
            "base_exception" => Ok(Self::BaseException),
            other => Err(PyValueError::new_err(format!(
                "unknown probe outcome {other:?}; expected one of \
                 'success', 'exception', 'base_exception'"
            ))),
        }
    }

    fn into_result(self) -> PyResult<i64> {
        match self {
            Self::Success => Ok(PROBE_SUCCESS_VALUE),
            Self::Exception => Err(PyValueError::new_err("probe failure")),
            Self::BaseException => Err(PyKeyboardInterrupt::new_err("probe base failure")),
        }
    }
}

/// A closed control object over one Rust-produced `Handle`.
///
/// The handle comes from the real [`PyHandle::spawn`] path (HDL-13), never from
/// `PythonTask.spawn_handle()`, so the contract suite observes a genuine direct
/// Rust producer. The producer parks on a release gate, so a test decides
/// exactly when the terminal state becomes observable without sleeping.
///
/// Two limits are load-bearing, not stylistic:
///
/// * It must never accept arbitrary Python work -- no coroutine, awaitable,
///   callable, future or producer function. Widening it to take one turns a
///   contract fixture into a general async escape hatch, and every caller then
///   depends on this module staying compiled into production.
/// * It must never be cited as a real producer's oracle. Its timing is a
///   release gate and its outcomes are three fixed cases, so it says nothing
///   about when a real producer starts work or whether that work is abandoned
///   when nobody observes it. Those belong to the producer's own tests.
#[pyclass(
    name = "_HandleProbe",
    module = "monarch._rust_bindings.monarch_hyperactor.testing"
)]
pub struct PyHandleProbe {
    handle: Py<PyHandle>,
    release: Mutex<Option<oneshot::Sender<()>>>,
    started: Arc<AtomicBool>,
    completed: Arc<AtomicBool>,
}

#[pymethods]
impl PyHandleProbe {
    /// The observe-only `Handle` under test. Cloning the reference does not
    /// consume it, so repeated lookups and observations are safe.
    #[getter]
    fn _handle(&self, py: Python<'_>) -> Py<PyHandle> {
        self.handle.clone_ref(py)
    }

    /// Whether the producer body has begun.
    ///
    /// This can be true immediately after construction: the producer is eager,
    /// so it enters its body and sets this *before* waiting on `_release`. Only
    /// completion is gated, so `_completed` is the witness a test can assert a
    /// negative on.
    #[getter]
    fn _started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Whether the producer body has finished.
    ///
    /// Set just before the terminal value is published, so it is a witness for
    /// assertions, not a synchronization primitive; use `_wait_completed` to
    /// wait for the value to be observable.
    #[getter]
    fn _completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    /// Open the release gate so the producer can run to its terminal state.
    ///
    /// Idempotent: a second call is a no-op, so a test may release
    /// unconditionally in a fixture teardown.
    fn _release(&self) {
        if let Some(tx) = self.release.lock().expect("probe mutex poisoned").take() {
            let _ = tx.send(());
        }
    }

    /// Block until the terminal state is published, discarding it.
    ///
    /// This is how a test reaches a genuinely *ready* `Handle` deterministically
    /// -- the alternative is sleeping, which the contract suite forbids. The
    /// terminal value is dropped here; the test observes it through the handle.
    /// Must be called off a Tokio thread, like any other blocking observation.
    fn _wait_completed(&self, py: Python<'_>) -> PyResult<()> {
        let waiter = self.handle.bind(py).borrow().wait_completion();
        // The probe waits for publication; whether the outcome was success or
        // error is the test's business, observed through the handle.
        signal_safe_block_on(py, async move {
            let _ = waiter.await;
        })
    }
}

/// Build a probe whose handle will reach `outcome` once released.
#[pyfunction]
fn _make_handle_probe(py: Python<'_>, outcome: &str) -> PyResult<PyHandleProbe> {
    let outcome = ProbeOutcome::parse(outcome)?;
    let (tx, rx) = oneshot::channel::<()>();
    let started = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let started_producer = Arc::clone(&started);
    let completed_producer = Arc::clone(&completed);

    let handle = PyHandle::spawn(async move {
        started_producer.store(true, Ordering::SeqCst);
        // A dropped sender releases the gate too, so the producer cannot wedge
        // if the probe is garbage collected mid-test.
        let _ = rx.await;
        let result = outcome.into_result();
        completed_producer.store(true, Ordering::SeqCst);
        result
    });

    Ok(PyHandleProbe {
        handle: Py::new(py, handle)?,
        release: Mutex::new(Some(tx)),
        started,
        completed,
    })
}

/// A `Handle` that publishes `value` after registering its first waiter.
///
/// The producer publishes through `send_result` rather than
/// [`PyHandle::spawn`], which takes the GIL to convert its value on
/// completion, so completion takes no GIL. The core holds one receiver, and
/// `get()` or `await` clones another; `poll()` does not. Once the producer
/// detects the first clone, it waits at least `delay` before publishing. A
/// later observer waits only for the remaining time. The producer yields
/// between checks, so it remains runnable on Tokio until the first waiter is
/// registered, and ends if the Handle is dropped unobserved. The time from its
/// first poll until it detects that waiter is added to the gate totals read by
/// `_delayed_handle_gate_stats`.
fn delayed_handle(delay: Duration, value: Py<PyAny>) -> PyHandle {
    let (tx, rx) = watch::channel(None);
    get_tokio_runtime().spawn(async move {
        let first_poll = Instant::now();
        while tx.receiver_count() < 2 {
            if tx.is_closed() {
                return;
            }
            tokio::task::yield_now().await;
        }
        let gate = u64::try_from(first_poll.elapsed().as_nanos()).unwrap_or(u64::MAX);
        {
            let mut stats = GATE_STATS
                .lock()
                .expect("should not be poisoned: no holder can panic");
            stats.0 += 1;
            stats.1 = stats.1.saturating_add(gate);
        }
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        send_result(tx, Ok(value), None);
    });
    PyHandle::from_core(HandleCore::new(rx, None))
}

/// Build a `Handle` that publishes `None` no sooner than `delay` seconds after
/// detecting its first registered waiter.
#[pyfunction]
fn _make_delayed_handle(py: Python<'_>, delay: f64) -> PyResult<PyHandle> {
    let delay = Duration::try_from_secs_f64(delay)
        .map_err(|e| PyValueError::new_err(format!("invalid delay {delay}: {e}")))?;
    Ok(delayed_handle(delay, py.None()))
}

/// Delayed Handles whose producer has detected its first waiter, and their
/// total gate time in nanoseconds, since the last `_delayed_handle_gate_stats`.
/// One lock guards both, so a read is a snapshot.
static GATE_STATS: Mutex<(u64, u64)> = Mutex::new((0, 0));

/// Return the number of delayed Handles whose gate opened, and their total
/// gate time in nanoseconds, since the last call; then reset both.
///
/// Gate time is elapsed time, including any time other tasks ran between the
/// producer's checks, so it is an upper bound on the gate's CPU.
#[pyfunction]
fn _delayed_handle_gate_stats() -> (u64, u64) {
    std::mem::take(
        &mut *GATE_STATS
            .lock()
            .expect("should not be poisoned: no holder can panic"),
    )
}

#[pyclass(name = "TestStruct", module = "monarch._src.actor.testing")]
pub struct PyTestStruct {
    value: i64,
}

#[pymethods]
impl PyTestStruct {
    #[new]
    fn new(value: i64) -> Self {
        Self { value }
    }

    fn rust_method(&self) -> i64 {
        self.value
    }

    fn shared_method(&self) -> String {
        "from_rust".to_string()
    }
}

#[pyfunction]
fn _make_test_struct(value: i64) -> PyTestStruct {
    PyTestStruct { value }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTestStruct>()?;
    module.add_function(wrap_pyfunction!(_make_test_struct, module)?)?;
    module.add_class::<PyHandleProbe>()?;
    module.add_function(wrap_pyfunction!(_make_handle_probe, module)?)?;
    module.add("_PROBE_SUCCESS_VALUE", PROBE_SUCCESS_VALUE)?;
    module.add_function(wrap_pyfunction!(_make_delayed_handle, module)?)?;
    module.add_function(wrap_pyfunction!(_delayed_handle_gate_stats, module)?)?;
    crate::actor::register_testing_python_bindings(module)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::GilSite;
    use crate::runtime::ensure_python;
    use crate::runtime::monarch_with_gil_blocking;

    const DELAYS: [Duration; 2] = [Duration::ZERO, Duration::from_millis(10)];

    fn none() -> Py<PyAny> {
        monarch_with_gil_blocking(GilSite::Test, |py| py.None())
    }

    // With no observer, the producer never starts its delay.
    #[test]
    fn delayed_handle_waits_for_an_observer() {
        ensure_python();
        for delay in DELAYS {
            let handle = delayed_handle(delay, none());
            std::thread::sleep(Duration::from_millis(30));
            assert!(
                handle.poll_ready().unwrap().is_none(),
                "an unobserved Handle with delay {delay:?} should still be pending"
            );
        }
    }

    // Once observed, it publishes `None`, no sooner than the delay.
    #[test]
    fn delayed_handle_publishes_after_its_delay() {
        ensure_python();
        for delay in DELAYS {
            let handle = delayed_handle(delay, none());
            let observed = Instant::now();
            let wait = handle.wait_future();
            // Bounded, so a broken gate fails here rather than at the target's
            // timeout.
            let value = get_tokio_runtime()
                .block_on(async { tokio::time::timeout(Duration::from_secs(5), wait).await })
                .expect("the Handle should publish within 5 s")
                .unwrap();
            assert!(
                observed.elapsed() >= delay,
                "the Handle published before its delay of {delay:?}"
            );
            monarch_with_gil_blocking(GilSite::Test, |py| {
                assert!(value.is_none(py), "the Handle should publish None");
            });
        }
    }
}
