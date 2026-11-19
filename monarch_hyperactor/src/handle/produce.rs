/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;

use pyo3::exceptions::PyBaseException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use tokio::sync::futures::OwnedNotified;

use super::PyHandle;
use super::core::Publisher;
use crate::runtime::get_tokio_runtime;

/// What a spawned receive does when its last Handle or active waiter disappears.
/// This policy applies after start; an unstarted receive is dropped with its core.
#[derive(Clone, Copy)]
pub(crate) enum ObserverLoss {
    /// Stop the unfinished receive and release its captured resources (ports).
    Stop,
    /// Keep the work running even without observers (stream items).
    Continue,
}

/// Whether a spawned producer stops when its last observer disappears.
pub(super) enum ObserverStop {
    /// Keep running without observers; no observer-loss stop signal is needed.
    Never,
    /// Stop unfinished work when the core signals that every observer is gone.
    /// Create this notification before scheduling the task, so it also covers
    /// observer loss before the task's first poll (HDL-23).
    WhenUnobserved(OwnedNotified),
}

/// Run the supplied Rust future as a Tokio task and publish its value or error
/// through the supplied Publisher (HDL-1).
///
/// ObserverStop::Never keeps the work running even if every observer disappears
/// (HDL-4). Port receives use ObserverStop::WhenUnobserved: when the last observer
/// drops the core, the task is notified to drop the unfinished receive (HDL-23).
/// This takes effect when the task polls the signal, not synchronously when
/// the core drops.
pub(super) fn spawn_producer<F>(future: F, publisher: Publisher, observer_stop: ObserverStop)
where
    F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
{
    // The task owns the future and publisher before its first poll. If Tokio
    // drops the task without a result, Publisher::Drop marks any surviving core
    // Lost so its observers do not wait forever (HDL-10).
    get_tokio_runtime().spawn(async move {
        let result = match observer_stop {
            ObserverStop::WhenUnobserved(stop) => tokio::select! {
                // Check the stop signal first. If both branches are ready,
                // stop rather than poll the receive and take a message.
                biased;
                // Returning drops the unfinished future and its captured
                // resources, along with the publisher. No result is published.
                _ = stop => return,
                result = future => result,
            },
            // Ordinary producers and started stream items ignore observer loss.
            ObserverStop::Never => future.await,
        };
        // Supply Done(result) to a surviving core and wake its waiters, or log
        // a failed result if no observers remain. This consumes the publisher.
        publisher.publish(result);
    });
}

/// Let Python supply the value or exception for a paired Handle.
///
/// For Python-owned work, `_new_handle_pair()` returns a Handle and this
/// completer. The Handle starts in Running; this object neither starts nor
/// schedules the work. Python calls set_result() or set_exception() when it
/// has the outcome.
///
/// The first valid completion consumes the sole publisher; later completion
/// attempts raise. Dropping the completer unresolved marks the Handle Lost,
/// so observers receive producer loss rather than wait forever (HDL-10/HDL-11).
/// This class has no Python constructor.
#[pyclass(
    name = "_HandleCompleter",
    module = "monarch._rust_bindings.monarch_hyperactor.handle"
)]
pub(crate) struct PyHandleCompleter {
    /// The paired Handle's sole publisher. Some until set_result() or a valid
    /// set_exception() takes it; None thereafter. An invalid exception is
    /// rejected before taking it, leaving the Handle available for completion.
    publisher: Option<Publisher>,
}

impl PyHandleCompleter {
    /// How many observers hold the paired Handle's core, for the private
    /// pending-`get()` test probe. A fresh pair has one; a pending `get()` adds
    /// another before it releases the GIL; zero after completion.
    pub(crate) fn observer_count_for_test(&self) -> usize {
        self.publisher.as_ref().map_or(0, Publisher::observer_count)
    }

    /// Publish the terminal result exactly once.
    fn complete(&mut self, result: PyResult<Py<PyAny>>) -> PyResult<()> {
        let publisher = self
            .publisher
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Handle has already been completed"))?;
        // Change a surviving core from Running to Done(result) and wake its
        // waiters. If no observers remain, log failures instead. This consumes
        // the publisher; later completion attempts find self.publisher empty.
        publisher.publish(result);
        Ok(())
    }
}

#[pymethods]
impl PyHandleCompleter {
    /// Resolve the paired Handle successfully.
    pub(super) fn set_result(&mut self, value: Py<PyAny>) -> PyResult<()> {
        self.complete(Ok(value))
    }

    /// Resolve the paired Handle with a Python exception.
    pub(super) fn set_exception(&mut self, error: Bound<'_, PyAny>) -> PyResult<()> {
        if !error.is_instance_of::<PyBaseException>() {
            return Err(PyTypeError::new_err("error must be a BaseException"));
        }
        self.complete(Err(PyErr::from_value(error)))
    }
}

/// Create a Handle and its only completer for work owned and scheduled by Python.
///
/// The Handle's core starts in State::Running, with no result. This factory
/// starts no work; Python supplies its value or exception through the completer.
/// Dropping the completer before supplying a result marks a surviving core Lost.
///
/// Return both together so Python cannot obtain a newly pending Handle without
/// its completion authority (HDL-10/HDL-11).
#[pyfunction]
pub(super) fn _new_handle_pair() -> (PyHandle, PyHandleCompleter) {
    // Create one Running core. The Handle holds it strongly; its sole Publisher
    // holds it weakly and can complete it exactly once.
    let (publisher, handle) = PyHandle::pending();
    // Return the observer alongside a Python-facing completer that owns the
    // publisher until set_result(), valid set_exception(), or destruction.
    (
        handle,
        PyHandleCompleter {
            publisher: Some(publisher),
        },
    )
}

/// Give Rust a result to await for work that must run on a Python actor's driver.
///
/// Sync actor cleanup starts in Rust, but the driver owns the actor's Python
/// state and must run its user __cleanup__ hook. Rust creates this pair, keeps
/// the Handle, and sends the completer to the driver in PendingSyncCleanup.
/// The driver wraps it in a Python _HandleCompleter and supplies the hook's
/// outcome. Rust's wait then finishes, allowing cleanup to join the driver.
///
/// This is the crate-visible Rust entry point to the private _new_handle_pair()
/// factory. It returns native Rust values without acquiring the GIL or allocating
/// Python objects, so the pair can travel through the GIL-free control queue.
/// Python allocation happens later on the driver thread. Dropping an unresolved
/// completer reports producer loss instead of leaving Rust waiting (HDL-10/HDL-11).
pub(crate) fn handle_pair() -> (PyHandle, PyHandleCompleter) {
    _new_handle_pair()
}
