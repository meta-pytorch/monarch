/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use monarch_types::py_global;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyCFunction;

use super::PyHandle;
use super::core::Core;
use super::core::Observed;
use super::core::wait_arrival;
use super::outcome::ReconstructionFailurePolicy;
use super::outcome::clone_shared_outcome;
use super::outcome::handle_producer_ended_error;
use crate::handle_wake::Notifier;
use crate::runtime::get_tokio_runtime;

// Held so the completion callback can catch and swallow it: publishing a result
// onto an asyncio.Future that's already settled (the awaiter cancelled it, or it
// resolved) raises InvalidStateError, which is benign here.
py_global!(invalid_state_error, "asyncio", "InvalidStateError");

/// Start a Tokio task that wakes the asyncio loop when this Handle can be read.
///
/// The task waits until the Handle has a final outcome, loses its producer, or
/// has a pickled reply available to claim. It then queues this await's token
/// and writes the loop's wake pipe.
///
/// The task owns the core and native notifier, but no Python object, so waiting
/// and waking require no GIL. The wake reader also uses this helper to re-arm
/// an await when another observer is reconstructing the reply.
pub(super) fn spawn_arrival_wake(core: Arc<Core>, loop_notifier: Notifier) {
    get_tokio_runtime().spawn(async move {
        wait_arrival(core).await;
        loop_notifier.notify();
    });
}

/// What the asyncio wake reader finds when an await's token arrives (HDL-17).
pub(crate) enum LoopReadOutcome {
    /// The shared value or error, to set on the await's future.
    Ready(PyResult<Py<PyAny>>),
    /// Another observer holds the raw reply's reconstruction claim. The await
    /// waits for that observer's outcome or release.
    Claimed,
    /// KeyboardInterrupt or SystemExit interrupted the rebuild on the loop.
    /// The reply stays Raw and unclaimed for a retry (HDL-27).
    Interrupted(PyErr),
}

impl PyHandle {
    /// Read the result for the asyncio wake reader when an await's token
    /// arrives, on the loop thread with its GIL (HDL-17).
    ///
    /// Claim and rebuild an unclaimed raw reply here, under the loop's failure
    /// policy: only KeyboardInterrupt or SystemExit leaves it retryable
    /// (HDL-27). The stored result remains available to other observers.
    pub(crate) fn read_on_loop(&self, py: Python<'_>) -> LoopReadOutcome {
        match self.core.observe() {
            Observed::Done(outcome) => LoopReadOutcome::Ready(clone_shared_outcome(py, &outcome)),
            Observed::Lost => LoopReadOutcome::Ready(Err(handle_producer_ended_error())),
            // A token follows an outcome or an unclaimed reply, and Raw never
            // returns to Running, so a pending core has another observer's claim.
            Observed::Pending => LoopReadOutcome::Claimed,
            Observed::Reconstruct(claim) => {
                match claim
                    .reconstruct(py, ReconstructionFailurePolicy::KeepReplyAfterLoopInterrupt)
                {
                    Ok(outcome) => LoopReadOutcome::Ready(clone_shared_outcome(py, &outcome)),
                    Err(interrupt) => LoopReadOutcome::Interrupted(interrupt),
                }
            }
        }
    }

    /// Re-arm a pending asyncio await while another observer reconstructs the reply.
    ///
    /// Start a native waiter for this Handle's next readable state. When the other
    /// observer stores an outcome or releases its claim, `loop_notifier` queues the
    /// same token and wakes the loop again.
    pub(crate) fn notify_on_arrival(&self, loop_notifier: Notifier) {
        spawn_arrival_wake(Arc::clone(&self.core), loop_notifier);
    }
}

/// The cached `complete_asyncio_future` callable.
///
/// The function is stateless, so it is wrapped once and reused for every
/// completion rather than rebuilt (and leaked) on each `call_soon_threadsafe`.
fn cached_completer(py: Python<'_>) -> PyResult<Bound<'_, PyCFunction>> {
    static COMPLETER: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
    COMPLETER
        .get_or_try_init(py, || {
            Ok(wrap_pyfunction!(complete_asyncio_future, py)?.unbind())
        })
        .map(|f| f.bind(py).clone())
}

/// Schedule the handle's completion onto `event_loop` from the Tokio observer.
///
/// Runs under the GIL. The caller has already cloned the success value; an error
/// is turned into its Python exception object here, so no Rust `PyErr` crosses
/// into the loop callback. `complete_asyncio_future` is then scheduled via
/// `call_soon_threadsafe`, whose error (e.g. a closed loop) is returned so the
/// caller can log rather than panic.
pub(super) fn schedule_completion(
    py: Python<'_>,
    event_loop: &Bound<'_, PyAny>,
    fut: &Bound<'_, PyAny>,
    result: PyResult<Py<PyAny>>,
) -> PyResult<()> {
    let completer = cached_completer(py)?;
    let (is_exc, value) = match result {
        Ok(v) => (false, v),
        Err(e) => (true, e.into_value(py).into_any()),
    };
    event_loop.call_method1("call_soon_threadsafe", (completer, fut, is_exc, value))?;
    Ok(())
}

/// Complete an `asyncio.Future` on its loop thread (HDL-7).
///
/// The future may already be settled when this runs: a cancelled future is left
/// alone, and an already-completed one is swallowed. Any other error propagates.
#[pyfunction]
pub(crate) fn complete_asyncio_future(
    fut: &Bound<'_, PyAny>,
    is_exc: bool,
    value: Py<PyAny>,
) -> PyResult<()> {
    if fut.call_method0("cancelled")?.is_truthy()? {
        return Ok(());
    }
    let py = fut.py();
    let (method, value) = if is_exc {
        // asyncio.Future.set_exception rejects a StopIteration (PEP 479) with a
        // TypeError; running as a call_soon_threadsafe callback it would be
        // logged and dropped, hanging the awaiter. Wrap any StopIteration
        // (including subclasses) in a RuntimeError -- how `raise StopIteration`
        // already surfaces out of a coroutine -- so the future can always settle.
        let value = if value.bind(py).is_instance_of::<PyStopIteration>() {
            PyRuntimeError::new_err("coroutine raised StopIteration")
                .into_value(py)
                .into_any()
        } else {
            value
        };
        ("set_exception", value)
    } else {
        ("set_result", value)
    };
    // HDL-7: publish only to a non-cancelled future (the guard above), and
    // swallow InvalidStateError from one already settled. Any other error is a
    // real failure and propagates.
    match fut.call_method1(method, (value,)) {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.is_instance(py, &invalid_state_error(py)) {
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}
