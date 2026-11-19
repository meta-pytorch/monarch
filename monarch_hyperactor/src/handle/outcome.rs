/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::error::Error;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;

use monarch_types::SerializablePyErr;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PySystemExit;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;

use super::core::Observed;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil_blocking;

pyo3::create_exception!(
    handle,
    WouldBlockRuntime,
    pyo3::exceptions::PyRuntimeError,
    "raised when a synchronous API refuses to enter or block on Tokio from \
     an existing Tokio runtime context -- Handle.get(), fresh root-client \
     bootstrap, or a blocking worker-loop wrapper -- or when a returns_future \
     .get() would block a running event loop"
);

/// Convert a Rust error into the generic Python `ValueError` used by Monarch's
/// native bindings.
pub fn to_py_error<T>(error: T) -> PyErr
where
    T: Error,
{
    PyErr::new::<PyValueError, _>(error.to_string())
}

/// Report that a Handle lost its producer before publishing a terminal value.
pub(super) fn handle_producer_ended_error() -> PyErr {
    PyRuntimeError::new_err("Handle producer ended without publishing a result")
}

/// Format a captured traceback (from `traceback.extract_stack()`) as a single
/// string suitable for logging.
fn format_traceback(py: Python<'_>, traceback: &Py<PyAny>) -> PyResult<String> {
    let tb = py
        .import("traceback")?
        .call_method1("format_list", (traceback,))?;
    PyString::new(py, "")
        .call_method1("join", (tb,))?
        .extract::<String>()
}

/// Report a failed operation whose result no longer has any observers.
///
/// Without a Handle or waiter left to receive the error, it would otherwise
/// go unreported (HDL-13). Log the exception and, when captured, the Python
/// stack showing where the operation was created.
pub(super) fn log_unobserved(outcome: &PyResult<Py<PyAny>>, traceback: Option<Py<PyAny>>) {
    let Err(pyerr) = outcome else {
        return;
    };
    monarch_with_gil_blocking(GilSite::Traceback, |py| {
        let tb = if let Some(tb) = traceback {
            format_traceback(py, &tb).unwrap()
        } else {
            // No creation traceback was captured: either the env var is
            // unset (the default), or this producer captures no traceback.
            "creation traceback unavailable (endpoint calls, stream items and port receives \
             capture one when `MONARCH_HYPERACTOR_ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK=1` \
             is set)\n"
                .into()
        };
        tracing::error!(
            target: "monarch_hyperactor::handle",
            "a background task errored but is not being awaited; this will not crash your \
            program, but indicates that something went wrong.\n{}\nTraceback where the task \
            was created (most recent call last):\n{}",
            SerializablePyErr::from(py, pyerr),
            tb
        );
    });
}

/// Clone the Handle's shared outcome for one observer (HDL-3).
///
/// Call with the GIL held, but never with the Handle's state lock held.
/// Cloning a Python exception may temporarily release the GIL. Another thread
/// could then take the GIL and wait for the state lock, leaving each thread
/// waiting for the other (HDL-9).
pub(super) fn clone_shared_outcome(
    py: Python<'_>,
    outcome: &PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    match outcome {
        Ok(value) => Ok(value.clone_ref(py)),
        Err(error) => Err(error.clone_ref(py)),
    }
}

/// Rebuild a successful reply without making Handle depend on
/// endpoint-specific storage.
///
/// The endpoint implementation retains the pickled bytes, mesh references and
/// calling actor's instance. Handle coordinates observers; this trait supplies
/// the operation that turns those retained components into a Python value.
///
/// Each attempt must use fresh unpickling state; an interrupted attempt
/// may be retried.
pub(crate) trait Reconstruct: Send + Sync {
    /// Rebuild the Python value with the GIL held and no core state lock held.
    fn reconstruct(&self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

/// Decide whether an interrupted or panicking unpickle attempt is retryable.
///
/// Ordinary Python exceptions become shared Handle errors under every policy.
#[derive(Clone, Copy)]
pub(super) enum ReconstructionFailurePolicy {
    /// Deliver a signal-like BaseException or Rust unwind to this observer.
    /// Keep the pickled reply so another observer can retry unpickling it.
    KeepReplyAfterInterrupt,
    /// On the asyncio loop, let KeyboardInterrupt and SystemExit stop this
    /// read and leave the pickled reply available for another attempt.
    /// Store every other failure, including other BaseException subclasses
    /// and Rust panics, as the Handle's shared error.
    KeepReplyAfterLoopInterrupt,
    /// Store every failure as the Handle's shared error before the asyncio
    /// fallback schedules completion. Its callback requires a final value or
    /// error, not a retryable reply.
    StoreEveryFailureInHandle,
}

impl ReconstructionFailurePolicy {
    /// Return whether this failure belongs only to the current read.
    /// An interrupted read leaves the pickled reply available for another
    /// attempt; otherwise the failure becomes the Handle's shared outcome.
    pub(super) fn interrupts(self, py: Python<'_>, error: &PyErr) -> bool {
        match self {
            // Outside the loop reader, control-flow exceptions such as
            // KeyboardInterrupt interrupt this caller's read rather than the
            // shared operation.
            Self::KeepReplyAfterInterrupt => !error.is_instance_of::<PyException>(py),
            // While the loop handles the wake pipe, KeyboardInterrupt and
            // SystemExit escape the current run. Leave the pickled reply
            // available so a later loop turn can retry it; store every other
            // failure as the shared outcome.
            Self::KeepReplyAfterLoopInterrupt => {
                error.is_instance_of::<PyKeyboardInterrupt>(py)
                    || error.is_instance_of::<PySystemExit>(py)
            }
            // The fallback reconstructs off-loop and has no read to interrupt,
            // so every failure must be stored before completion is scheduled.
            Self::StoreEveryFailureInHandle => false,
        }
    }
}

/// Run one reconstruction attempt without letting a Rust panic escape.
/// Convert the panic payload to a Python RuntimeError so the caller can store
/// it through the same path as any other Python failure.
pub(super) fn reconstruct_catching_panic(
    reply: &dyn Reconstruct,
    py: Python<'_>,
) -> PyResult<Py<PyAny>> {
    match catch_unwind(AssertUnwindSafe(|| reply.reconstruct(py))) {
        Ok(result) => result,
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("Rust unwind while reconstructing an endpoint reply");
            Err(PyRuntimeError::new_err(message.to_owned()))
        }
    }
}

/// Turn a core observation into the result of a non-blocking Handle read.
/// Return a ready value, an error, or `Ok(None)` when no result is ready.
///
/// This starts no work and consumes no stored result; other observers can
/// still read it.
///
/// # Preconditions
///
/// - The caller must already have acquired the GIL.
/// - No Handle state mutex may be held (HDL-9). Cloning a stored Python
///   exception can release and reacquire the GIL; doing so while holding
///   such a mutex can deadlock another observer.
#[inline]
pub(super) fn read_observed(py: Python<'_>, observed: Observed) -> PyResult<Option<Py<PyAny>>> {
    match observed {
        // Clone the stored value or exception. map(Some) wraps only the
        // successful value; an error remains Err.
        Observed::Done(outcome) => clone_shared_outcome(py, &outcome).map(Some),
        // No producer remains to supply a result. Construct RuntimeError
        // "Handle producer ended without publishing a result" rather than
        // report Pending and leave the observer waiting for work that ended.
        Observed::Lost => Err(handle_producer_ended_error()),
        // Unstarted or still running: report not ready without waiting or
        // taking ownership of the receive. This is Rust None, not Python None.
        Observed::Pending => Ok(None),
        Observed::Reconstruct(claim) => {
            let outcome =
                claim.reconstruct(py, ReconstructionFailurePolicy::KeepReplyAfterInterrupt)?;
            clone_shared_outcome(py, &outcome).map(Some)
        }
    }
}

/// Obtain the shared Python outcome from a terminal snapshot or a raw claim.
/// Precondition: hold the GIL, not Core.state's mutex. Reconstruction and result
/// reading can share this acquisition; an interrupted observer leaves Raw intact.
pub(super) fn reconstruct_observed(
    py: Python<'_>,
    observed: Observed,
    failure_policy: ReconstructionFailurePolicy,
) -> PyResult<Arc<PyResult<Py<PyAny>>>> {
    match observed {
        Observed::Done(outcome) => Ok(outcome),
        Observed::Lost => Err(handle_producer_ended_error()),
        Observed::Reconstruct(claim) => claim.reconstruct(py, failure_policy),
        Observed::Pending => unreachable!("wait for a pending observation before reading"),
    }
}
