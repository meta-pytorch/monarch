/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `Handle`: an observe-only handle to a background operation.
//!
//! A `Handle` wraps a `watch` channel that a producer fulfills exactly once with
//! the operation's `PyResult<Py<PyAny>>`. You observe its eventual result:
//! synchronously via `get()`, on an `asyncio` loop via `as_asyncio()`/`await`, or
//! without blocking via `poll()`. The channel is multi-observer, so a resolved
//! value stays observable by any number of later observers.
//!
//! The watch-channel mechanics live in [`HandleCore`].
//!
//! ## Handle invariants (HDL-*)
//!
//! Code that relies on or enforces an invariant is tagged `// HDL-N`, and each
//! `#[cfg(test)]` test that attests one names it; read the matching entry below
//! before changing a tagged site.
//!
//! Assumed of the shared core and producer (not enforced here):
//! - **HDL-1 (single terminal completion).** The watch value goes `None` ->
//!   `Some(_)` exactly once and is never reset (`Some` -> `Some`, `Some` ->
//!   `None`). Relied on by [`HandleCore::wait_ready`] (wait loop),
//!   [`HandleCore::wait_future`] and `get()` (`expect` after that wait),
//!   [`HandleCore::poll`] (re-borrow across the GIL), and
//!   [`PyHandle::wait_completion`] (discriminant check then GIL re-borrow). Held
//!   by convention: every producer sends once, always `Some(_)` (including the
//!   direct [`PyHandle::spawn`] producer). A send-once sender (a newtype
//!   consuming the `watch::Sender`) could make it structural, hardening `poll`'s
//!   re-borrow.
//! - **HDL-2 (value encoding).** `None` is pending, `Some(Ok(_))` success,
//!   `Some(Err(_))` a producer error.
//!
//! Guaranteed by this module:
//! - **HDL-3 (non-consuming reads).** No read moves the value out: a
//!   value-extracting read clones it (`clone_ref`), and a discriminant-only read
//!   (`wait_completion`'s success fast path) borrows just the `Ok`/`Err` tag and
//!   clones nothing. Either way any number of observers see the same result.
//!   Enforced by construction: no read path moves the value.
//! - **HDL-4 (drop does not cancel; drop never panics).** Dropping a `Handle`
//!   never cancels its producer; a `Handle` observes, it does not own the
//!   producer's lifecycle. Enforced by construction: the core holds only a
//!   result receiver and optional traceback, with no producer task handle or
//!   abort-on-drop implementation.
//! - **HDL-5 (GIL discipline).** No Python object is touched without the GIL,
//!   and every acquisition goes through `monarch_with_gil{,_blocking}`. Enforced
//!   by the crate's `#![deny(clippy::disallowed_methods)]` ban on raw
//!   `Python::with_gil`/`attach` -- a hard compile error, not a `debug_assert`.
//! - **HDL-6 (`WouldBlockRuntime` is `get()`'s alone *among the observers*).**
//!   Of the `Handle` observers only `get()` raises it, and only in a Tokio
//!   runtime context; `as_asyncio()`/`__await__` off a loop raise the native
//!   `RuntimeError` instead. The exception type itself is not private to this
//!   module: it marks synchronous APIs that refuse to enter or block on Tokio
//!   from an existing Tokio runtime context. The Python layer also raises it for
//!   fresh root-client bootstrap (`monarch._src.actor.actor_mesh`), the
//!   blocking worker-loop wrappers (`monarch._src.actor.bootstrap`), and a
//!   `returns_future` `.get()` that would block a running event loop
//!   (`monarch._src.actor.returns_future`, RF-7).
//! - **HDL-7 (`as_asyncio` publish).** The observer waits borrow-first (via
//!   `wait_ready`, never `changed()`-first). The result is published on the
//!   loop thread through `complete_asyncio_future`, by the wake reader
//!   (HDL-17) or by the fallback's `call_soon_threadsafe` callback (HDL-21),
//!   or, for a Handle already complete, by `as_asyncio` itself (HDL-22).
//!   It sets a result only on a non-cancelled future, swallowing
//!   `InvalidStateError`; any `StopIteration` (including subclasses) is wrapped
//!   in `RuntimeError` first, since `set_exception` rejects `StopIteration`
//!   (PEP 479). If the loop has closed, the fallback logs rather than panics
//!   and the wake reader never runs. A wake-reader failure, including a
//!   future whose `done()` raises, is reported through the future's loop
//!   exception handler, as asyncio reports an exception from a callback.
//! - **HDL-8 (`get()` loop-warning is call-pattern, not outcome).** `get()` on a
//!   running asyncio loop warns regardless of whether the value is ready: it
//!   flags the blocking-call-from-a-loop anti-pattern, not whether this call
//!   happens to block. A ready value returns after warning; under a
//!   warnings-as-errors filter the warning escalates to an error.
//! - **HDL-9 (GIL/watch borrow discipline).** A `watch` borrow is never held
//!   across a GIL acquisition or an `.await`. `poll` and `wait_completion`'s error
//!   path drop their read guard before taking the GIL and re-borrow under it
//!   (GIL-then-watch order, never the inverse against the producer's write-lock
//!   `send`); the `wait_ready`/
//!   `wait_future` loops drop the temporary `Ref` before each `.await`; and their
//!   returned futures own a cloned receiver (`Send + 'static`, borrowing nothing
//!   from `&self`), which is what lets `get()` `drop(slf)` + release the GIL
//!   before blocking and lets `as_asyncio` `spawn` the observer. `as_asyncio`'s
//!   fallback observer clones the value under the one GIL acquisition it makes
//!   to schedule; its wake observer never reads the value (HDL-17). `get()`
//!   reads only after its calling thread has reacquired the GIL. Enforced
//!   partly by the compiler (`Send` bounds, the borrow checker on `drop(slf)`)
//!   and partly by construction; exercised by the multi-observer/blocking
//!   tests.
//! - **HDL-10 (dropped producer surfaces, never hangs).** If every producer drops
//!   its sender without sending, the wait loop's `changed().await?` yields a
//!   `RecvError` converted to `RuntimeError("Handle producer ended without
//!   publishing a result")` on the non-blocking (`poll`), sync
//!   (`get`/`wait_future`), async (`as_asyncio`), and `wait_completion` paths,
//!   so every observer receives the same producer-lifecycle failure rather than
//!   waiting forever.
//! - **HDL-11 (Python construction is paired).** Neither `Handle` nor
//!   `_HandleCompleter` has `#[new]`. Internal Python may call the private
//!   `_new_handle_pair()`, which atomically creates a pending `Handle` and its
//!   sole producer. The factory initializes the completer with `Some(sender)`.
//!   `set_exception()` rejects a non-`BaseException` before taking it; the first
//!   `set_result()` or valid `set_exception()` takes it, producing the only
//!   state transition to `None`, and any later completion attempt raises. No
//!   sender clone escapes. Dropping the completer unresolved closes the channel
//!   and surfaces through HDL-10. Python therefore cannot create a pending
//!   `Handle` without its producer. Native producers remain the default; this
//!   private pair is reserved for operations whose semantics and scheduling are
//!   owned by Python.
//! - **HDL-12 (`get()` timeout contract).** `get()` validates the timeout up
//!   front (rejecting negative/NaN/non-finite as `ValueError` via
//!   `try_from_secs_f64`, never panicking) and before the ready fast path, so an
//!   invalid timeout raises deterministically even for a ready handle. A timeout
//!   is non-cancelling: it raises `TimeoutError` while leaving the producer and
//!   every observer untouched, so a later `poll()`/`get()`/`await` still observes
//!   completion.
//! - **HDL-13 (direct Rust producer).** A Rust caller can eagerly produce a
//!   `Handle` from a `Send + 'static` future without a `PyPythonTask`.
//!   [`PyHandle::spawn`] wraps the future with conversion of a successful Rust
//!   value to `Py<PyAny>` under `monarch_with_gil` and delegates to
//!   [`PyHandle::spawn_pyobject`]. The latter starts one producer and publishes
//!   its Python value or error once through
//!   `send_result`, without a result-conversion GIL acquisition (same one-shot
//!   core and unobserved-error policy as pytokio's producers, HDL-1/HDL-2).
//!   Dropping the result receiver does not cancel the producer (HDL-4).
//!   [`PyHandle::wait_completion`] is the matching observer: it projects the
//!   terminal value to `PyResult<()>`, touching neither the GIL nor the Python
//!   value on the ready-success fast path (a discriminant-only read, HDL-3) and
//!   cloning the `PyErr` under the HDL-9 borrow order only on the error path. Both
//!   constructors and this observer are `pub` Rust-only methods outside
//!   `#[pymethods]` (HDL-11).
//! - **HDL-14 (Rust value observation is owned and non-consuming).**
//!   [`PyHandle::wait_future`] returns a `Send + 'static` future over a cloned
//!   watch receiver. The waiter borrows nothing from the `PyHandle`, survives
//!   the wrapper being dropped, and leaves the terminal value available to
//!   every other observer.
//! - **HDL-15 (Rust ready construction starts no work).**
//!   [`PyHandle::from_value`] constructs an already-complete, non-aborting core
//!   without spawning a task. It is public to Rust callers but remains outside
//!   `#[pymethods]`, preserving HDL-11's direct-construction restriction.
//! - **HDL-16 (permanent Python identity).** `Handle` and `WouldBlockRuntime`
//!   have the canonical module `monarch._rust_bindings.monarch_hyperactor.handle`.
//!   The legacy `pytokio` module does not export `Handle`.
//! - **HDL-17 (GIL-free completion wake).** For a Handle still pending when
//!   `as_asyncio` is called, on a loop that accepts `add_reader`, the observer
//!   task holds a `Notifier` (a token,
//!   an mpsc sender and an `Arc<Waker>`) and the `wait_ready` future. It
//!   publishes by pushing the token and writing the loop's pipe, never reading
//!   the value and taking no GIL; the loop thread reads the value from the
//!   `Handle` and sets it (see [`crate::handle_wake`]). Enforced by
//!   construction: `Notifier` has no `Py<_>`, and the task body makes no
//!   `monarch_with_gil` call.
//! - **HDL-18 (notification ordering under supported pipe outcomes).** A
//!   notifier pushes its token before writing the pipe, and the reader drains
//!   the pipe before the queue, never returning between the two; both retry
//!   `EINTR`. A write that succeeds or returns `EAGAIN` leaves a byte pending,
//!   so a token pushed after a queue drain is delivered by a later wake.
//!   `EPIPE` or a disconnected queue means the channel is gone and the token is
//!   dropped. Any other errno is logged as a broken wake channel, not claimed
//!   as delivered.
//! - **HDL-19 (loop not retained).** The wake registry holds each loop as a
//!   weak key and its channel through a weak reference; the loop's selector,
//!   through the channel's reader callback, is the channel's only strong
//!   owner. A channel holds no reference to its loop. It holds each pending
//!   await's asyncio future strongly until delivery, cancellation or loop
//!   close, because asyncio holds tasks weakly and that reference is the
//!   waiting task's only owner outside the task's own cycle; `__traverse__`
//!   exposes each entry's future and `Handle` object to the garbage collector.
//!   Closing a loop therefore releases the channel-owned graph (the channel,
//!   its read end, its entries, their futures and `Handle`s). The write end
//!   stays open until every `Notifier` is dropped, so a loop closed while an
//!   await waits on a `Handle` that never completes keeps that descriptor
//!   (HDL-20). A loop dropped
//!   without being closed while an await on a pending `Handle` is registered
//!   is still freed by the collector. Not freed: a loop dropped without being
//!   closed while an undelivered entry's `Handle` value references it, and any
//!   independent cycle through a `Handle`, such as a value that references its
//!   own `Handle`. Both run through `PyHandle`, which the garbage collector
//!   does not traverse.
//! - **HDL-20 (late wake is harmless).** A notify after its channel is gone is
//!   dropped without error or panic: the send fails, or the write gets `EPIPE`.
//!   This requires `SIGPIPE` to be ignored, as Rust's runtime and CPython set
//!   it by default; in a process that restores `SIG_DFL`, a completion that
//!   races loop teardown can kill the process. Every `Notifier` owns the write
//!   end through its `Arc<Waker>` until its `Handle` completes, so no write
//!   reaches a reused descriptor.
//! - **HDL-21 (fallback).** For a Handle still pending, a loop whose
//!   `add_reader` raises `NotImplementedError` or that cannot be weakly
//!   referenced uses the `call_soon_threadsafe` path; HDL-7 applies there
//!   unchanged.
//! - **HDL-22 (ready shortcut).** `as_asyncio()` on a Handle that already holds
//!   its outcome, a value, a stored exception or producer loss (HDL-10),
//!   returns a loop future already completed with it through
//!   `complete_asyncio_future`, with no observer task and no wake-channel
//!   registration. Awaiting it returns without giving the loop a turn, as for
//!   any finished asyncio future.

use std::error::Error;
use std::future::Future;

use monarch_types::SerializablePyErr;
use monarch_types::py_global;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyBaseException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyUserWarning;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyCFunction;
use pyo3::types::PyString;
use pyo3::types::PyType;
use tokio::sync::watch;

use crate::handle_wake::LoopWake;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::is_in_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on_inline;

// Held so the completion callback can catch and swallow it: publishing a result
// onto an asyncio.Future that's already settled (the awaiter cancelled it, or it
// resolved) raises InvalidStateError, which is benign here.
py_global!(invalid_state_error, "asyncio", "InvalidStateError");

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

/// Report that a Handle lost its producer before receiving a terminal value.
fn handle_producer_ended_error(_: watch::error::RecvError) -> PyErr {
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

/// Publish a completed background-task result to the shared watch channel.
///
/// Shared by `PyPythonTask` spawns, the direct [`PyHandle::spawn_pyobject`] producer
/// (HDL-13), and the private Python completion pair (HDL-11). If the receiver
/// has already been dropped, `watch::Sender::send` returns the unsent value as
/// `SendError`: no consumer can ever observe it. An unobserved error is logged
/// with its producer-creation traceback when one was captured.
pub(crate) fn send_result(
    tx: watch::Sender<Option<PyResult<Py<PyAny>>>>,
    result: PyResult<Py<PyAny>>,
    traceback: Option<Py<PyAny>>,
) {
    if let Err(watch::error::SendError(Some(Err(pyerr)))) = tx.send(Some(result)) {
        monarch_with_gil_blocking(GilSite::Traceback, |py| {
            let tb = if let Some(tb) = traceback {
                format_traceback(py, &tb).unwrap()
            } else {
                // No creation traceback was captured: either a capture-disabled
                // PythonTask (the default when the env var is unset) or a direct
                // Handle producer, which never captures one.
                "creation traceback unavailable (PythonTask producers can set \
                 `MONARCH_HYPERACTOR_ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK=1` to capture one)\n"
                    .into()
            };
            tracing::error!(
                "a background task errored but is not being awaited; this will not crash your \
                program, but indicates that something went wrong.\n{}\nTraceback where the task \
                was created (most recent call last):\n{}",
                SerializablePyErr::from(py, &pyerr),
                tb
            );
        });
    }
}

/// The watch-channel mechanics behind a `Handle`.
///
/// The channel starts as `None` and is fulfilled once with `Some(Ok(obj))` or
/// `Some(Err(pyerr))`. Cloning the receiver lets multiple observers see the same
/// completion, so observation is non-consuming.
pub(crate) struct HandleCore {
    /// One-shot result channel. `None` until the producer completes.
    rx: watch::Receiver<Option<PyResult<Py<PyAny>>>>,

    /// Optional creation-site traceback, used when logging un-awaited errors.
    traceback: Option<Py<PyAny>>,
}

impl HandleCore {
    /// Construct a core from its parts.
    pub(crate) fn new(
        rx: watch::Receiver<Option<PyResult<Py<PyAny>>>>,
        traceback: Option<Py<PyAny>>,
    ) -> Self {
        Self { rx, traceback }
    }

    /// Construct a core that is already resolved with `value`.
    ///
    /// There is no producer task, so the core never aborts on drop and carries
    /// no traceback.
    pub(crate) fn from_value(value: Py<PyAny>) -> PyResult<Self> {
        let (tx, rx) = watch::channel(None);
        tx.send(Some(Ok(value))).map_err(to_py_error)?;
        Ok(Self {
            rx,
            traceback: None,
        })
    }

    /// Non-blocking, non-consuming check for completion.
    ///
    /// Returns `Ok(None)` while pending, `Ok(Some(obj))` on success, or
    /// `Err(pyerr)` if the producer completed with an exception or disappeared
    /// before completion.
    pub(crate) fn poll(&self) -> PyResult<Option<Py<PyAny>>> {
        // HDL-9: release the watch read guard before taking the GIL; holding it
        // across the GIL acquisition would invert lock order against the
        // producer's write-lock send. Re-borrowing under the GIL is safe by HDL-1
        // (one-shot: once `Some`, never reset or replaced).
        if self.rx.borrow().is_none() {
            #[cfg(test)]
            if let Some(between) = POLL_BETWEEN_CHECKS.with(|hook| hook.borrow_mut().take()) {
                between();
            }
            // HDL-10: a closed channel with no value is terminal producer loss,
            // not a still-pending Handle. `has_changed` also reports a channel
            // closed just after a send, so read the value again after it.
            let closed = self.rx.has_changed().err();
            if self.rx.borrow().is_none() {
                return match closed {
                    Some(err) => Err(handle_producer_ended_error(err)),
                    None => Ok(None),
                };
            }
        }
        monarch_with_gil_blocking(GilSite::Convert, |py| {
            match self
                .rx
                .borrow()
                .as_ref()
                .expect("HDL-1: value is Some after the is_none check")
            {
                Ok(v) => Ok(Some(v.clone_ref(py))),
                Err(err) => Err(err.clone_ref(py)),
            }
        })
    }

    /// Return a future that observes this core's completion.
    ///
    /// The receiver is cloned, so this is non-consuming: any number of waiters
    /// can observe the same final value. The current value is checked before
    /// awaiting `changed()`, so a core that already holds its value resolves
    /// immediately.
    pub(crate) fn wait_future(
        &self,
    ) -> impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static + use<> {
        // Reuse wait_ready's HDL-1 wait loop (single source of truth), then clone
        // the value out under the GIL.
        let ready = self.wait_ready();
        async move {
            let rx = ready.await?;
            monarch_with_gil(GilSite::Convert, |py| {
                match rx
                    .borrow()
                    .as_ref()
                    .expect("HDL-1: value is Some after wait_ready")
                {
                    Ok(v) => Ok(v.clone_ref(py)),
                    Err(err) => Err(err.clone_ref(py)),
                }
            })
            .await
        }
    }

    /// Await this core's completion without touching the GIL.
    ///
    /// Yields the cloned receiver positioned at the completed value, or a
    /// `RuntimeError` if every producer dropped its sender without sending. Unlike
    /// `wait_future`, which clones the value out under its own GIL acquisition,
    /// the caller decides where the value is read. `get()` reads it after its
    /// calling thread reacquires the GIL. `as_asyncio`'s fallback clones it under
    /// the GIL it takes to schedule, and its wake path reads it on the loop
    /// thread instead (HDL-17).
    pub(crate) fn wait_ready(
        &self,
    ) -> impl Future<Output = PyResult<watch::Receiver<Option<PyResult<Py<PyAny>>>>>>
    + Send
    + 'static
    + use<> {
        // HDL-9: the returned future owns a cloned receiver (Send + 'static,
        // borrowing nothing from &self), so a caller can drop its PyRef / release
        // the GIL before awaiting and can spawn this future.
        let mut rx = self.rx.clone();
        async move {
            // HDL-1: loop until the value is actually `Some` (one-shot). HDL-9:
            // the temporary borrow is dropped before each `.await`, never held
            // across it. HDL-10: a dropped producer makes `changed()` yield a
            // RecvError, which becomes the producer-lifecycle error rather than
            // leaking the watch-channel implementation or hanging.
            while rx.borrow().is_none() {
                rx.changed().await.map_err(handle_producer_ended_error)?;
            }
            Ok(rx)
        }
    }

    /// Clone the creation-site traceback under the GIL.
    pub(crate) fn traceback_clone(&self) -> Option<Py<PyAny>> {
        self.traceback
            .as_ref()
            .map(|t| monarch_with_gil_blocking(GilSite::Traceback, |py| t.clone_ref(py)))
    }
}

/// Private one-shot producer paired with a Python-visible `Handle`.
///
/// The class has no Python constructor. `_new_handle_pair()` creates it with
/// the only sender for its Handle; completion consumes that sender before
/// publishing, and dropping it unresolved closes the channel (HDL-10/HDL-11).
#[pyclass(
    name = "_HandleCompleter",
    module = "monarch._rust_bindings.monarch_hyperactor.handle"
)]
pub(crate) struct PyHandleCompleter {
    // `Some` from `_new_handle_pair()` until the first valid completion;
    // `None` thereafter.
    tx: Option<watch::Sender<Option<PyResult<Py<PyAny>>>>>,
}

impl PyHandleCompleter {
    /// Count live result receivers for the private pending-`get()` test probe.
    /// The paired Handle owns one; a blocking wait owns an additional clone.
    pub(crate) fn receiver_count_for_test(&self) -> usize {
        self.tx.as_ref().map_or(0, watch::Sender::receiver_count)
    }

    /// Publish the terminal result exactly once.
    fn complete(&mut self, result: PyResult<Py<PyAny>>) -> PyResult<()> {
        let tx = self
            .tx
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Handle has already been completed"))?;
        send_result(tx, result, None);
        Ok(())
    }
}

#[pymethods]
impl PyHandleCompleter {
    /// Resolve the paired Handle successfully.
    fn set_result(&mut self, value: Py<PyAny>) -> PyResult<()> {
        self.complete(Ok(value))
    }

    /// Resolve the paired Handle with a Python exception.
    fn set_exception(&mut self, error: Bound<'_, PyAny>) -> PyResult<()> {
        if !error.is_instance_of::<PyBaseException>() {
            return Err(PyTypeError::new_err("error must be a BaseException"));
        }
        self.complete(Err(PyErr::from_value(error)))
    }
}

/// The observe-only handle to a background Tokio task or Python-owned operation.
///
/// Exposed to Python as
/// `monarch._rust_bindings.monarch_hyperactor.handle.Handle`. HDL-11: Python
/// cannot construct one directly. Internal Python may obtain one only from the
/// private `_new_handle_pair()` factory together with its sole producer.
#[pyclass(
    name = "Handle",
    module = "monarch._rust_bindings.monarch_hyperactor.handle"
)]
pub struct PyHandle {
    core: HandleCore,
}

/// Atomically create a pending Handle and its sole Python completion endpoint.
///
/// Native producers remain the default. This private pair is for operations
/// whose semantics and scheduling are owned by Python (HDL-11).
#[pyfunction]
fn _new_handle_pair() -> (PyHandle, PyHandleCompleter) {
    let (tx, rx) = watch::channel(None);
    (
        PyHandle::from_core(HandleCore::new(rx, None)),
        PyHandleCompleter { tx: Some(tx) },
    )
}

/// Create a pending Handle and its sole completer without acquiring the GIL.
///
/// This function returns ordinary Rust values and performs no Python
/// allocation. A caller can retain the Handle for observation and move the
/// completer with pending work to a Python-owned execution thread.
/// Under the GIL, that thread allocates a private `_HandleCompleter` Python
/// object around the Rust completer. Python settles it when the operation
/// finishes; dropping it unresolved fails the Handle (HDL-10/HDL-11).
///
/// For example, sync actor cleanup carries the completer in
/// `PendingSyncCleanup`; the driver includes it in `SyncCleanup` and settles it
/// after running `__cleanup__`. Rust cleanup awaits the paired Handle; settling
/// the completer wakes that future so cleanup can resume and join the driver.
pub(crate) fn handle_pair() -> (PyHandle, PyHandleCompleter) {
    _new_handle_pair()
}

impl PyHandle {
    /// Wrap an already-built `HandleCore` in a `PyHandle`.
    ///
    /// Native producers build the core and hand back an observe-only `Handle`
    /// through this; `PyHandle`'s field is private to this module.
    pub(crate) fn from_core(core: HandleCore) -> Self {
        Self { core }
    }

    /// Construct an already-complete `Handle` from `value` without spawning a
    /// producer task (HDL-15).
    ///
    /// Rust-only and outside `#[pymethods]`, so making this method public does
    /// not make `Handle` directly constructible from Python (HDL-11).
    pub fn from_value(value: Py<PyAny>) -> PyResult<Self> {
        Ok(Self {
            core: HandleCore::from_value(value)?,
        })
    }

    /// Return an owned, non-consuming Rust observer for this Handle's value
    /// (HDL-14).
    ///
    /// The returned future owns a cloned watch receiver, so it borrows nothing
    /// from `self`, survives the `PyHandle` being dropped, and can coexist with
    /// any number of other observers.
    pub fn wait_future(
        &self,
    ) -> impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static + use<> {
        self.core.wait_future()
    }

    /// Non-blocking, non-consuming read of this Handle's outcome, for the
    /// loop-thread wake reader (HDL-17). Rust-only and outside `#[pymethods]`
    /// (HDL-11).
    pub(crate) fn poll_ready(&self) -> PyResult<Option<Py<PyAny>>> {
        self.core.poll()
    }

    /// Eagerly drive a Rust future to completion and return an observe-only
    /// `Handle` over its result (HDL-13).
    ///
    /// The producer spawns immediately on the shared Tokio runtime; the returned
    /// `Handle` observes already-running work rather than starting it lazily on
    /// first observation. A successful value is converted to `Py<PyAny>` under
    /// `monarch_with_gil`; a producer or conversion error is stored unchanged.
    /// [`Self::spawn_pyobject`] starts the task and publishes its result
    /// (HDL-1/HDL-2). Dropping the `Handle` never cancels the producer (HDL-4).
    /// Rust-only and outside `#[pymethods]` (HDL-11).
    pub fn spawn<F, T>(fut: F) -> Self
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send,
    {
        Self::spawn_pyobject(async move {
            let value = fut.await?;
            monarch_with_gil(GilSite::Convert, |py| value.into_py_any(py)).await
        })
    }

    /// Start a Rust future whose result is already a Python object, and return
    /// a Handle for its result (HDL-13).
    ///
    /// The future runs as one task on the shared Tokio runtime. Its Python value
    /// or error is stored without conversion or a result-conversion GIL
    /// acquisition. `send_result` still takes the GIL to log an error if every
    /// observer has gone. Dropping the Handle does not cancel the task (HDL-4).
    /// Rust-only and outside `#[pymethods]` (HDL-11).
    pub fn spawn_pyobject<F>(fut: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        let (tx, rx) = watch::channel(None);
        get_tokio_runtime().spawn(async move {
            // HDL-1/HDL-2/HDL-13: publish exactly once via the shared helper; a
            // direct Rust producer carries no creation traceback.
            send_result(tx, fut.await, None);
        });
        // HDL-4: a `Handle`'s core never aborts its producer on drop.
        PyHandle::from_core(HandleCore::new(rx, None))
    }

    /// Observe whether this handle completed successfully, projecting the terminal
    /// value to `PyResult<()>` (HDL-13).
    ///
    /// Returns an owned `Send + 'static` future over a cloned receiver (HDL-9),
    /// so multiple Rust callers can observe the same terminal state and dropping
    /// the `Handle` after taking an observer does not abort the producer (HDL-4).
    /// The ready-success fast path touches neither the GIL nor the Python value
    /// (a discriminant-only read, HDL-3); only the error path clones the stored
    /// `PyErr`. This is the readiness surface for callers that need only the
    /// success/error discriminant, not the value.
    pub fn wait_completion(&self) -> impl Future<Output = PyResult<()>> + Send + 'static + use<> {
        let ready = self.core.wait_ready();
        async move {
            let rx = ready.await?;
            // Read only the success/error discriminant under a watch borrow that
            // holds no GIL, then drop it. HDL-1: the value is `Some` and, once
            // set, never changes.
            let is_err = rx
                .borrow()
                .as_ref()
                .expect("HDL-1: value is Some after wait_ready")
                .is_err();
            if !is_err {
                return Ok(());
            }
            // Error path only. HDL-9: acquire the GIL first, then re-borrow the
            // watch value (GIL-then-watch order, never the inverse); the
            // discriminant borrow above is already dropped, so the two are never
            // held together, and HDL-1 guarantees the same terminal `Err` is still
            // there to clone.
            monarch_with_gil(GilSite::Convert, move |py| -> PyResult<()> {
                Err(rx
                    .borrow()
                    .as_ref()
                    .expect("HDL-1: value is Some after wait_ready")
                    .as_ref()
                    .expect_err("HDL-1: error branch, discriminant already checked")
                    .clone_ref(py))
            })
            .await
        }
    }
}

/// Run `operation` only after `ready` resolves `Ok`, returning the readiness
/// error unchanged if it does not.
///
/// Generic over the result and error so the short-circuit law is unit-testable
/// without pyo3; the RDMA bindings instantiate `E = PyErr` and this helper never
/// inspects or remaps it. `operation` is a closure, not an already-started
/// future, so no effect begins before readiness. The typical `ready` input is
/// [`PyHandle::wait_completion`].
pub async fn after_ready<T, E, RF, Op, OF>(ready: RF, operation: Op) -> Result<T, E>
where
    RF: Future<Output = Result<(), E>>,
    Op: FnOnce() -> OF,
    OF: Future<Output = Result<T, E>>,
{
    ready.await?;
    operation().await
}

#[cfg(test)]
mod after_ready_tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::after_ready;

    #[derive(Debug, PartialEq)]
    struct SentinelErr(&'static str);

    /// After readiness resolves `Ok`, the operation closure runs exactly once
    /// and its result is returned.
    #[tokio::test]
    async fn after_ready_runs_operation_once_on_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let out: Result<u8, SentinelErr> =
            after_ready(async { Ok::<(), SentinelErr>(()) }, move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(7u8)
                }
            })
            .await;
        assert_eq!(out, Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// A readiness error is returned unchanged and the operation closure is
    /// never invoked.
    #[tokio::test]
    async fn after_ready_short_circuits_on_readiness_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let out: Result<u8, SentinelErr> = after_ready(
            async { Err::<(), SentinelErr>(SentinelErr("boom")) },
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(7u8)
                }
            },
        )
        .await;
        assert_eq!(out, Err(SentinelErr("boom")));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// While readiness is pending the composed future cannot complete and the
    /// operation closure is never invoked.
    #[test]
    fn after_ready_does_not_run_operation_while_pending() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let pending_ready = std::future::pending::<Result<(), SentinelErr>>();
        let mut fut = Box::pin(after_ready(pending_ready, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(7u8)
            }
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[pymethods]
impl PyHandle {
    /// Block the calling thread until the handle resolves and return its value.
    ///
    /// Behavior is keyed to the calling context, not to whether the value
    /// happens to be ready:
    ///   - Tokio runtime context: raise `WouldBlockRuntime` unconditionally
    ///     (blocking there would panic the runtime); use `poll()` or
    ///     `as_asyncio()` instead.
    ///   - running `asyncio` loop: warns (get() is a blocking call from a loop),
    ///     then a ready value returns via the `poll()` fast path and a pending
    ///     one blocks. Under a warnings-as-errors filter the warning is an error.
    ///   - sync thread: a ready value fast-paths, a pending one blocks
    ///     (Ctrl-C-safe on main).
    ///
    /// `get()` is non-cancelling. On timeout it raises `TimeoutError`, leaving
    /// the producer and every observer untouched, so a later
    /// `poll()`/`get()`/`await` still observes completion.
    /// A pending wait releases the GIL; once it finishes, the calling thread
    /// reacquires the GIL and reads the stored value or error itself.
    #[pyo3(signature = (timeout = None))]
    fn get(slf: PyRef<'_, Self>, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        // HDL-6: among the observers, get() is the sole WouldBlockRuntime
        // raiser. It is the blocking API, and in a Tokio runtime context
        // blocking would panic the runtime, so refuse unconditionally -- even
        // a ready value -- keying the outcome to context, not producer timing.
        if is_in_tokio_runtime() {
            return Err(WouldBlockRuntime::new_err(
                "get() cannot be called from a Tokio runtime context; use poll() or as_asyncio()",
            ));
        }

        // HDL-12: validate the timeout up front, so an invalid value raises
        // ValueError deterministically rather than only when the handle happens
        // to be pending. Reject negative/NaN/non-finite rather than panicking in
        // Duration::from_secs_f64.
        let duration = timeout
            .map(|seconds| {
                std::time::Duration::try_from_secs_f64(seconds)
                    .map_err(|e| PyValueError::new_err(format!("invalid timeout {seconds}: {e}")))
            })
            .transpose()?;

        // Warn regardless of readiness: get() is a blocking call, and calling it
        // from a running asyncio loop is the anti-pattern being flagged whether or
        // not the value happens to be ready this time (a later call may block and
        // freeze the loop). Under a warnings-as-errors filter this escalates to an
        // error -- the caller's chosen strictness for the anti-pattern.
        if pyo3_async_runtimes::get_running_loop(py).is_ok() {
            let category = py.get_type::<PyUserWarning>();
            PyErr::warn(
                py,
                &category,
                c"Blocking get() was called from a running asyncio event loop. It is a synchronous, blocking call that can freeze the loop and deadlock; use as_asyncio() (or await) instead.",
                // Python code reaches get() through a wrapper such as
                // `Future.get()`; level 2 names the wrapper's caller.
                2,
            )?;
        }

        // A ready value returns without blocking, in any non-Tokio context.
        if let Some(value) = slf.core.poll()? {
            return Ok(value);
        }

        let wait = slf.core.wait_ready();
        // HDL-9: drop the PyRef before releasing the GIL in `signal_safe_block_on_inline`
        // (which blocks with the GIL released), so a concurrent mutable borrow of
        // this handle does not throw during the blocking window.
        drop(slf);

        // HDL-12: the timeout drops only the local `wait` future, leaving the
        // producer and the watch channel untouched, so a timed-out get() stays
        // observable (non-cancelling).
        let rx = match duration {
            Some(duration) => signal_safe_block_on_inline(py, async move {
                tokio::time::timeout(duration, wait)
                    .await
                    .map_err(|_| PyTimeoutError::new_err(()))?
            })?,
            None => signal_safe_block_on_inline(py, wait)?,
        }?;

        // The wait has finished and this thread holds the GIL again.
        // Only now take the channel's read lock (HDL-9). Clone the stored
        // value or error without removing it, so other observers can read it (HDL-3).
        match rx
            .borrow()
            .as_ref()
            .expect("HDL-1: value is Some after wait_ready")
        {
            Ok(value) => Ok(value.clone_ref(py)),
            Err(error) => Err(error.clone_ref(py)),
        }
    }

    /// Non-blocking, non-consuming check for completion.
    ///
    /// Returns `None` while pending, the value on success, or raises the stored
    /// exception. If the producer disappears before completion, raises the
    /// producer-lifecycle `RuntimeError`. A ready value stays observable by
    /// later observers.
    fn poll(&self) -> PyResult<Option<Py<PyAny>>> {
        self.core.poll()
    }

    /// Return a standard `asyncio.Future` that resolves when the handle does.
    ///
    /// Requires a running loop; off a loop this raises the native `RuntimeError`
    /// from `asyncio.get_running_loop()`. A Tokio observer of the watch channel
    /// waits for completion; it never drives a Python coroutine. It then wakes
    /// the loop through the loop's [`LoopWake`] channel without the GIL, and the
    /// loop thread reads the value and sets the result (HDL-17). It instead
    /// takes the GIL and publishes via `loop.call_soon_threadsafe(...)` when the
    /// loop's `add_reader` raises `NotImplementedError`, or when the loop cannot
    /// be weakly referenced (HDL-21). Any other failure to install the channel
    /// or register the future is raised.
    ///
    /// A Handle that already holds its outcome returns a future already
    /// completed with it, and starts no observer (HDL-22). For a pending
    /// Handle, the observer borrows the current watch value first, so a core
    /// that completes before the observer starts resolves rather than hangs.
    /// The result is set on the loop thread. A cancel can race that: a
    /// cancelled future is left alone and the handle continues. If the loop has
    /// closed by then, the result is never set, and nothing panics.
    ///
    /// Each call on a pending Handle spawns its own observer; cancelling the
    /// returned future does not stop the observer, which exits when the handle
    /// resolves.
    fn as_asyncio<'py>(slf: PyRef<'_, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // HDL-6: off a loop this yields the native RuntimeError, not WouldBlockRuntime.
        let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
        let fut = event_loop.call_method0("create_future")?;

        // HDL-22: `poll()` registers no waiter. A value that arrives after it
        // is found by the observer's borrow-first wait below.
        match slf.core.poll() {
            Ok(Some(value)) => {
                complete_asyncio_future(&fut, false, value)?;
                return Ok(fut);
            }
            Err(err) => {
                complete_asyncio_future(&fut, true, err.into_value(py).into_any())?;
                return Ok(fut);
            }
            Ok(None) => {}
        }
        let wait = slf.core.wait_ready();

        if let Some(channel) = LoopWake::for_loop(py, &event_loop)? {
            let notifier = LoopWake::register(&channel, &fut, slf.into())?;
            get_tokio_runtime().spawn(async move {
                // HDL-17: no GIL on this task. Dropping the receiver before the
                // pipe write means a readable pipe proves the task got past it.
                drop(wait.await);
                notifier.notify();
            });
            return Ok(fut);
        }

        // HDL-21: the fallback, for a loop without a channel.
        let loop_handle = event_loop.clone().unbind();
        let fut_handle = fut.clone().unbind();

        get_tokio_runtime().spawn(async move {
            let recv = wait.await;
            let scheduled = monarch_with_gil(GilSite::Convert, move |py| {
                // HDL-9: one GIL section that borrows the watch value under the
                // already-held GIL (GIL-then-watch order), clones it out (or turns
                // a dropped-producer RecvError into a PyErr), and schedules it --
                // completion touches the GIL exactly once.
                let result: PyResult<Py<PyAny>> = match recv {
                    Ok(rx) => match rx
                        .borrow()
                        .as_ref()
                        .expect("HDL-1: value is Some after wait_ready")
                    {
                        Ok(v) => Ok(v.clone_ref(py)),
                        Err(err) => Err(err.clone_ref(py)),
                    },
                    // HDL-10: a dropped producer surfaces as a set_exception.
                    Err(e) => Err(e),
                };
                schedule_completion(py, loop_handle.bind(py), fut_handle.bind(py), result)
            })
            .await;
            if let Err(err) = scheduled {
                tracing::warn!("as_asyncio: failed to schedule completion on the loop: {err}");
            }
        });

        Ok(fut)
    }

    /// Implement Python's `await` protocol by delegating to `as_asyncio()`.
    fn __await__<'py>(slf: PyRef<'_, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Self::as_asyncio(slf, py)?.call_method0("__await__")
    }

    /// Support `Handle[T]` type syntax on the Python side (no runtime effect).
    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _arg: Py<PyAny>) -> Py<PyAny> {
        cls.clone().unbind().into()
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
fn schedule_completion(
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

#[cfg(test)]
thread_local! {
    /// Runs once inside `HandleCore::poll`, after it finds no value and before
    /// it checks whether the channel is closed.
    static POLL_BETWEEN_CHECKS: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
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

/// Register the permanent Handle Python bindings.
pub fn register_python_bindings(handle_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    // HDL-16: this is Handle's sole class registration and Python home.
    handle_mod.add_class::<PyHandle>()?;
    handle_mod.add_class::<PyHandleCompleter>()?;
    handle_mod.add_function(wrap_pyfunction!(_new_handle_pair, handle_mod)?)?;
    let would_block = handle_mod.py().get_type::<WouldBlockRuntime>();
    would_block.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.handle",
    )?;
    handle_mod.add("WouldBlockRuntime", would_block)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::thread;

    use pyo3::IntoPyObjectExt;
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::exceptions::PyValueError;
    use pyo3::types::PyModule;
    use pyo3::types::PyTuple;

    use super::*;
    use crate::runtime::ensure_python;

    // Build a `Handle` over a controlled, still-pending watch channel and return
    // the sender so the test drives completion explicitly.
    fn pending_handle() -> (watch::Sender<Option<PyResult<Py<PyAny>>>>, PyHandle) {
        let (tx, rx) = watch::channel(None);
        let handle = PyHandle {
            core: HandleCore::new(rx, None),
        };
        (tx, handle)
    }

    fn assert_producer_ended_error(py: Python<'_>, error: &PyErr) {
        assert!(
            error.is_instance_of::<PyRuntimeError>(py),
            "a dropped producer should surface a RuntimeError"
        );
        assert_eq!(
            error.to_string(),
            "RuntimeError: Handle producer ended without publishing a result"
        );
    }

    // A Python helper module providing loop drivers for the `asyncio` tests.
    fn loop_helper(py: Python<'_>) -> Bound<'_, PyModule> {
        let code = cr#"
import asyncio
import warnings

async def _await(h):
    return await h

def run_await(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_await(h))
    finally:
        loop.close()

async def _get_on_loop(h):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = h.get()
    return (value, any(issubclass(w.category, UserWarning) for w in caught))

def run_get_on_loop(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_get_on_loop(h))
    finally:
        loop.close()

async def _get_on_loop_strict(h):
    # get() under a warnings-as-errors filter: a ready value must still return
    # (no-warn fast path), not raise the UserWarning as an error.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        return h.get()

def run_get_on_loop_strict(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_get_on_loop_strict(h))
    finally:
        loop.close()

async def _cancel(h):
    f = h.as_asyncio()
    was_done = f.done()
    accepted = f.cancel()
    await asyncio.sleep(0.05)
    return (was_done, accepted, f.cancelled(), h.poll())

def run_cancel(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_cancel(h))
    finally:
        loop.close()

async def _cancel_then_await(h):
    # cancel the first observer, then observe again while a live producer runs
    f1 = h.as_asyncio()
    f1.cancel()
    result = await h.as_asyncio()
    # let the cancelled observer's completion callback drain (it no-ops)
    await asyncio.sleep(0.05)
    return (f1.cancelled(), result, h.poll())

def run_cancel_then_await(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_cancel_then_await(h))
    finally:
        loop.close()

async def _two(h):
    a = h.as_asyncio()
    b = h.as_asyncio()
    # gather returns a list; the Rust side downcasts a tuple
    return tuple(await asyncio.gather(a, b))

def run_two(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_two(h))
    finally:
        loop.close()

class CountingLoop(asyncio.SelectorEventLoop):
    # Counts only the public add_reader, which the wake channel uses; the
    # loop's own self-pipe goes through _add_reader.
    def __init__(self):
        super().__init__()
        self.add_reader_calls = 0

    def add_reader(self, fd, callback, *args):
        self.add_reader_calls += 1
        return super().add_reader(fd, callback, *args)

def observe_ready(h):
    # (done when returned, the outcome or the exception, add_reader calls)
    loop = CountingLoop()

    async def observe():
        fut = h.as_asyncio()
        done = fut.done()
        try:
            outcome = await fut
        except BaseException as e:
            outcome = e
        return (done, outcome, loop.add_reader_calls)

    try:
        return loop.run_until_complete(observe())
    finally:
        loop.close()

def ready_await_order(h):
    order = []

    async def a():
        order.append("a1")
        await h
        order.append("a2")

    async def b():
        order.append("b")

    async def main():
        await asyncio.gather(a(), b())

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    return order
"#;
        PyModule::from_code(py, code, c"handle_test_helper.py", c"handle_test_helper").unwrap()
    }

    // A ready value is returned via the poll fast path.
    // Attests HDL-2.
    #[test]
    fn get_returns_ready_value() {
        ensure_python();
        let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
            let value = 42i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(value).unwrap();
            let r = Py::new(py, handle).unwrap();
            let out = PyHandle::get(r.borrow(py), py, None).unwrap();
            out.extract::<i64>(py).unwrap()
        });
        assert_eq!(got, 42, "get() should return the resolved value");
    }

    // A pending handle blocks get() on a sync thread until a producer completes.
    // Attests HDL-1, HDL-2.
    #[test]
    fn get_blocks_then_returns() {
        ensure_python();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 7i64.into_py_any(py).unwrap());
        let (tx, handle) = pending_handle();
        get_tokio_runtime().spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let _ = tx.send(Some(Ok(value)));
        });
        let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
            let r = Py::new(py, handle).unwrap();
            let out = PyHandle::get(r.borrow(py), py, None).unwrap();
            out.extract::<i64>(py).unwrap()
        });
        assert_eq!(got, 7, "get() should block then return the produced value");
    }

    // poll() transitions from None to the value once the producer completes.
    // Attests HDL-1, HDL-2.
    #[test]
    fn poll_transitions_pending_to_ready() {
        ensure_python();
        let (tx, rx) = watch::channel(None);
        let core = HandleCore::new(rx, None);
        assert!(
            core.poll().unwrap().is_none(),
            "poll() should be None while pending"
        );
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 1i64.into_py_any(py).unwrap());
        tx.send(Some(Ok(value))).unwrap();
        let got = core.poll().unwrap();
        let extracted =
            monarch_with_gil_blocking(GilSite::Test, |py| got.unwrap().extract::<i64>(py).unwrap());
        assert_eq!(extracted, 1, "poll() should observe the produced value");
    }

    // poll() is non-consuming: after it returns ready, get() and poll() still
    // observe the same value.
    // Attests HDL-1, HDL-3.
    #[test]
    fn repeated_observation_after_poll() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = 5i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(value).unwrap();
            let r = Py::new(py, handle).unwrap();

            let first = r.borrow(py).poll().unwrap().unwrap();
            assert_eq!(first.extract::<i64>(py).unwrap(), 5);

            let via_get = PyHandle::get(r.borrow(py), py, None).unwrap();
            assert_eq!(via_get.extract::<i64>(py).unwrap(), 5);

            let again = r.borrow(py).poll().unwrap().unwrap();
            assert_eq!(again.extract::<i64>(py).unwrap(), 5);
        });
    }

    // Two independent observers of one still-pending core both resolve; cloning
    // the receiver never consumes it.
    // Attests HDL-1, HDL-3.
    #[test]
    fn multiple_observers_resolve() {
        ensure_python();
        let (tx, rx) = watch::channel(None);
        let core = HandleCore::new(rx, None);
        let f1 = core.wait_future();
        let f2 = core.wait_future();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 3i64.into_py_any(py).unwrap());
        tx.send(Some(Ok(value))).unwrap();
        let (r1, r2) = get_tokio_runtime().block_on(async move { tokio::join!(f1, f2) });
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_eq!(r1.unwrap().extract::<i64>(py).unwrap(), 3);
            assert_eq!(r2.unwrap().extract::<i64>(py).unwrap(), 3);
        });
    }

    // A still-pending get() in a Tokio runtime context raises WouldBlockRuntime.
    // Attests HDL-6.
    #[test]
    fn get_pending_in_tokio_raises_would_block() {
        ensure_python();
        let (_tx, handle) = pending_handle();
        get_tokio_runtime().block_on(async {
            monarch_with_gil(GilSite::Test, |py| {
                let r = Py::new(py, handle).unwrap();
                let err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
                assert!(
                    err.is_instance_of::<WouldBlockRuntime>(py),
                    "pending get() in a tokio context should raise WouldBlockRuntime"
                );
            })
            .await
        });
    }

    // A ready value in a Tokio runtime context still raises: get() is refused by
    // context, not by whether the value happens to be available (HDL-6).
    // Attests HDL-6.
    #[test]
    fn get_ready_in_tokio_also_raises() {
        ensure_python();
        get_tokio_runtime().block_on(async {
            monarch_with_gil(GilSite::Test, |py| {
                let value = 9i64.into_py_any(py).unwrap();
                let handle = PyHandle::from_value(value).unwrap();
                let r = Py::new(py, handle).unwrap();
                let err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
                assert!(
                    err.is_instance_of::<WouldBlockRuntime>(py),
                    "get() in a tokio context must raise even for a ready value"
                );
            })
            .await
        });
    }

    // __await__ in a Tokio runtime context finds no asyncio loop to bridge to,
    // so it surfaces asyncio's native "no running event loop" RuntimeError --
    // NOT WouldBlockRuntime, which among the observers stays specific to get()
    // (HDL-6). The refusal consumes nothing: the same handle still observes its
    // value afterwards. Lives here rather than in the Python suite so the Handle
    // contract does not depend on a PythonTask-driven test helper.
    // Attests HDL-6, HDL-7.
    #[test]
    fn await_in_tokio_raises_native_and_leaves_handle_observable() {
        ensure_python();
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = 11i64.into_py_any(py).unwrap();
            Py::new(py, PyHandle::from_value(value).unwrap()).unwrap()
        });
        let in_tokio = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));

        // `block_on` would poll the root future on *this* thread under an
        // entered runtime -- `is_in_tokio_runtime()` is true there, but it is not a
        // worker and nested blocking is tolerated. Spawn instead, so the
        // assertion genuinely runs on a runtime worker, and join from here.
        let joined = get_tokio_runtime().spawn(async move {
            assert!(
                is_in_tokio_runtime(),
                "the oracle must observe from a runtime worker"
            );
            monarch_with_gil(GilSite::Test, |py| {
                let err = PyHandle::__await__(in_tokio.borrow(py), py).unwrap_err();
                assert!(
                    !err.is_instance_of::<WouldBlockRuntime>(py),
                    "__await__ must not raise WouldBlockRuntime; among the observers that is get()'s alone"
                );
                assert!(
                    err.is_instance_of::<PyRuntimeError>(py),
                    "__await__ off an asyncio loop should raise the native RuntimeError"
                );
                assert!(
                    err.to_string().contains("no running event loop"),
                    "expected asyncio's native message, got {err}"
                );
            })
            .await
        });
        get_tokio_runtime()
            .block_on(joined)
            .expect("the spawned oracle panicked");

        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = PyHandle::get(handle.borrow(py), py, None).unwrap();
            assert_eq!(
                value.extract::<i64>(py).unwrap(),
                11,
                "a refused __await__ must not consume the handle"
            );
        });
    }

    // get(timeout) raises TimeoutError and leaves the handle pending, so a later
    // observation still sees completion.
    // Attests HDL-12.
    #[test]
    fn get_timeout_raises_and_leaves_pending() {
        ensure_python();
        let (tx, handle) = pending_handle();
        let r = monarch_with_gil_blocking(GilSite::Test, |py| Py::new(py, handle).unwrap());

        let is_timeout = monarch_with_gil_blocking(GilSite::Test, |py| {
            let err = PyHandle::get(r.borrow(py), py, Some(0.05)).unwrap_err();
            err.is_instance_of::<PyTimeoutError>(py)
        });
        assert!(is_timeout, "get(timeout) should raise TimeoutError");

        let still_pending =
            monarch_with_gil_blocking(GilSite::Test, |py| r.borrow(py).poll().unwrap().is_none());
        assert!(
            still_pending,
            "a timed-out get() must not resolve the handle"
        );

        let value = monarch_with_gil_blocking(GilSite::Test, |py| 4i64.into_py_any(py).unwrap());
        tx.send(Some(Ok(value))).unwrap();
        let observed = monarch_with_gil_blocking(GilSite::Test, |py| {
            r.borrow(py)
                .poll()
                .unwrap()
                .unwrap()
                .extract::<i64>(py)
                .unwrap()
        });
        assert_eq!(
            observed, 4,
            "completion is still observable after a timeout"
        );
    }

    // as_asyncio() off a loop raises the native RuntimeError, not
    // WouldBlockRuntime.
    // Attests HDL-6.
    #[test]
    fn as_asyncio_off_loop_raises_runtime_error() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let handle = PyHandle::from_value(py.None()).unwrap();
            let r = Py::new(py, handle).unwrap();
            let err = PyHandle::as_asyncio(r.borrow(py), py).unwrap_err();
            assert!(
                err.is_instance_of::<PyRuntimeError>(py),
                "off a loop as_asyncio() should raise RuntimeError"
            );
            assert!(
                !err.is_instance_of::<WouldBlockRuntime>(py),
                "the off-loop error must be the native RuntimeError, not WouldBlockRuntime"
            );
        });
    }

    // __await__ off a loop surfaces the native RuntimeError from as_asyncio().
    // Attests HDL-6.
    #[test]
    fn await_off_loop_raises_runtime_error() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let handle = PyHandle::from_value(py.None()).unwrap();
            let r = Py::new(py, handle).unwrap();
            let err = PyHandle::__await__(r.borrow(py), py).unwrap_err();
            assert!(
                err.is_instance_of::<PyRuntimeError>(py),
                "off a loop __await__ should raise RuntimeError"
            );
            assert!(
                !err.is_instance_of::<WouldBlockRuntime>(py),
                "__await__ must not raise WouldBlockRuntime"
            );
        });
    }

    // await on a pre-resolved handle resolves on a real loop and yields the
    // value.
    // Attests HDL-2, HDL-7.
    #[test]
    fn await_resolves_ok_on_loop() {
        ensure_python();
        let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
            let value = 42i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(value).unwrap();
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            helper
                .getattr("run_await")
                .unwrap()
                .call1((h,))
                .unwrap()
                .extract::<i64>()
                .unwrap()
        });
        assert_eq!(got, 42, "await should resolve to the value on a real loop");
    }

    // await surfaces an error sent while it waits as a Python exception on a
    // real loop.
    // Attests HDL-2, HDL-7.
    #[test]
    fn await_resolves_err_on_loop() {
        ensure_python();
        let (tx, handle) = pending_handle();
        let waited = Arc::new(AtomicBool::new(false));
        let waited_in_producer = Arc::clone(&waited);
        get_tokio_runtime().spawn(async move {
            // The Handle holds one receiver and `wait_ready()` clones a second,
            // so the error is sent only once the observer is on the pending path.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.receiver_count() < 2 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            waited_in_producer.store(tx.receiver_count() >= 2, Ordering::SeqCst);
            let _ = tx.send(Some(Err(PyValueError::new_err("boom"))));
        });
        let is_value_error = monarch_with_gil_blocking(GilSite::Test, |py| {
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            let err = helper
                .getattr("run_await")
                .unwrap()
                .call1((h,))
                .unwrap_err();
            err.is_instance_of::<PyValueError>(py)
        });
        assert!(
            waited.load(Ordering::SeqCst),
            "the error should be sent only after the observer waited"
        );
        assert!(is_value_error, "await should raise the stored exception");
    }

    // await resolves a still-pending handle once a producer completes it while
    // the loop runs.
    // Attests HDL-1, HDL-7.
    #[test]
    fn await_resolves_pending_on_loop() {
        ensure_python();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 11i64.into_py_any(py).unwrap());
        let (tx, handle) = pending_handle();
        get_tokio_runtime().spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            let _ = tx.send(Some(Ok(value)));
        });
        let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            helper
                .getattr("run_await")
                .unwrap()
                .call1((h,))
                .unwrap()
                .extract::<i64>()
                .unwrap()
        });
        assert_eq!(got, 11, "await should resolve once the producer completes");
    }

    // Cancelling an as_asyncio() future does not cancel the handle. Here the
    // handle is pre-resolved, so its observer is already done: cancel() is
    // refused and the value stays observable via poll(). The live-producer
    // case -- one observer's cancel leaves the producer running and other
    // observers still resolving -- is
    // `cancel_asyncio_future_does_not_stop_producer_or_observers`.
    // Attests HDL-3, HDL-22.
    #[test]
    fn cancel_asyncio_future_does_not_cancel_handle() {
        ensure_python();
        let (was_done, accepted, cancelled, handle_resolved) =
            monarch_with_gil_blocking(GilSite::Test, |py| -> (bool, bool, bool, bool) {
                let value = 42i64.into_py_any(py).unwrap();
                let handle = PyHandle::from_value(value).unwrap();
                let h = Py::new(py, handle).unwrap();
                let helper = loop_helper(py);
                let res = helper.getattr("run_cancel").unwrap().call1((h,)).unwrap();
                let tup = res.cast::<PyTuple>().unwrap();
                let flag = |i| tup.get_item(i).unwrap().extract::<bool>().unwrap();
                (
                    flag(0),
                    flag(1),
                    flag(2),
                    !tup.get_item(3).unwrap().is_none(),
                )
            });
        assert!(was_done, "a ready handle's observer should already be done");
        assert!(!accepted, "cancel() on a done observer should return false");
        assert!(!cancelled, "a done observer should not become cancelled");
        assert!(
            handle_resolved,
            "cancelling the future must not cancel the handle"
        );
    }

    // Run `observe_ready` on `h`: whether the observer was done when returned,
    // its outcome (a value or an exception object), and the add_reader calls.
    fn observe_ready(py: Python<'_>, h: Py<PyHandle>) -> (bool, Bound<'_, PyAny>, i64) {
        let res = loop_helper(py)
            .getattr("observe_ready")
            .unwrap()
            .call1((h,))
            .unwrap();
        let tup = res.cast::<PyTuple>().unwrap();
        (
            tup.get_item(0).unwrap().extract().unwrap(),
            tup.get_item(1).unwrap(),
            tup.get_item(2).unwrap().extract().unwrap(),
        )
    }

    fn errored_handle(err: PyErr) -> PyHandle {
        let (tx, rx) = watch::channel(None);
        tx.send(Some(Err(err))).unwrap();
        PyHandle {
            core: HandleCore::new(rx, None),
        }
    }

    // A ready value comes back in an already-done observer, and no wake channel
    // is installed.
    // Attests HDL-22.
    #[test]
    fn as_asyncio_ready_value_is_done_without_wake() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let handle = PyHandle::from_value(42i64.into_py_any(py).unwrap()).unwrap();
            let (done, outcome, add_reader_calls) = observe_ready(py, Py::new(py, handle).unwrap());
            assert!(done, "a ready handle's observer should already be done");
            assert_eq!(outcome.extract::<i64>().unwrap(), 42);
            assert_eq!(add_reader_calls, 0, "no wake channel should be installed");
        });
    }

    // A ready error comes back in an already-done observer; a stored
    // StopIteration is wrapped in RuntimeError as on the pending path.
    // Attests HDL-7, HDL-22.
    #[test]
    fn as_asyncio_ready_error_is_done_without_wake() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let h = Py::new(py, errored_handle(PyValueError::new_err("boom"))).unwrap();
            let (done, outcome, add_reader_calls) = observe_ready(py, h);
            assert!(done, "a ready handle's observer should already be done");
            assert!(outcome.is_instance_of::<PyValueError>());
            assert_eq!(add_reader_calls, 0, "no wake channel should be installed");

            let h = Py::new(py, errored_handle(PyStopIteration::new_err(()))).unwrap();
            let (done, outcome, _) = observe_ready(py, h);
            assert!(done);
            assert!(
                outcome.is_instance_of::<PyRuntimeError>()
                    && !outcome.is_instance_of::<PyStopIteration>(),
                "a stored StopIteration should surface as RuntimeError"
            );
        });
    }

    // A producer that ended without publishing is a ready outcome too.
    // Attests HDL-10, HDL-22.
    #[test]
    fn as_asyncio_producer_ended_is_done_without_wake() {
        ensure_python();
        let (tx, handle) = pending_handle();
        drop(tx);
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let (done, outcome, add_reader_calls) = observe_ready(py, Py::new(py, handle).unwrap());
            assert!(done, "a ready handle's observer should already be done");
            assert_producer_ended_error(py, &PyErr::from_value(outcome));
            assert_eq!(add_reader_calls, 0, "no wake channel should be installed");
        });
    }

    // A producer that publishes and drops its sender between poll()'s empty
    // read and its closed check: the observer gets what was published, not
    // producer loss.
    // Attests HDL-10, HDL-22.
    #[test]
    fn as_asyncio_reads_an_outcome_sent_just_before_close() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let outcomes = [
                Ok(42i64.into_py_any(py).unwrap()),
                Err(PyValueError::new_err("boom")),
            ];
            for outcome in outcomes {
                let expect_value = outcome.is_ok();
                let (tx, handle) = pending_handle();
                POLL_BETWEEN_CHECKS.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        tx.send(Some(outcome)).unwrap();
                        drop(tx);
                    }));
                });
                let (done, got, _) = observe_ready(py, Py::new(py, handle).unwrap());
                assert!(done, "the published outcome is ready");
                if expect_value {
                    assert_eq!(got.extract::<i64>().unwrap(), 42);
                } else {
                    assert!(
                        got.is_instance_of::<PyValueError>(),
                        "the stored exception should surface, not producer loss: {got}"
                    );
                }
            }
        });
    }

    // Awaiting a ready handle does not give the loop a turn: the awaiting task
    // runs on before a task scheduled after it, so the order is a1, a2, b.
    // Attests HDL-22.
    #[test]
    fn await_ready_handle_takes_no_loop_turn() {
        ensure_python();
        let order = monarch_with_gil_blocking(GilSite::Test, |py| -> Vec<String> {
            let handle = PyHandle::from_value(1i64.into_py_any(py).unwrap()).unwrap();
            loop_helper(py)
                .getattr("ready_await_order")
                .unwrap()
                .call1((Py::new(py, handle).unwrap(),))
                .unwrap()
                .extract()
                .unwrap()
        });
        assert_eq!(order, ["a1", "a2", "b"]);
    }

    // The live-producer companion to `cancel_asyncio_future_does_not_cancel_handle`:
    // with a still-pending handle and a real producer, cancelling one observer
    // leaves the producer running and other observers still resolving. A second
    // as_asyncio() future resolves to the value and the handle stays pollable; the
    // cancelled observer still runs to completion, and the `complete_asyncio_future`
    // it posts to the loop no-ops on the cancelled future.
    // Attests HDL-3, HDL-7.
    #[test]
    fn cancel_asyncio_future_does_not_stop_producer_or_observers() {
        ensure_python();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 42i64.into_py_any(py).unwrap());
        let (tx, handle) = pending_handle();
        let both_waited = Arc::new(AtomicBool::new(false));
        let both_waited_in_producer = Arc::clone(&both_waited);
        get_tokio_runtime().spawn(async move {
            // Publish only once both observers wait: the Handle holds one
            // receiver and each pending `as_asyncio()` clones another. A value
            // published first would leave the first observer already done, and
            // its cancel refused (HDL-22).
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.receiver_count() < 3 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            both_waited_in_producer.store(tx.receiver_count() >= 3, Ordering::SeqCst);
            let _ = tx.send(Some(Ok(value)));
        });
        let (cancelled, awaited, polled) =
            monarch_with_gil_blocking(GilSite::Test, |py| -> (bool, i64, i64) {
                let h = Py::new(py, handle).unwrap();
                let helper = loop_helper(py);
                let res = helper
                    .getattr("run_cancel_then_await")
                    .unwrap()
                    .call1((h,))
                    .unwrap();
                let tup = res.cast::<PyTuple>().unwrap();
                (
                    tup.get_item(0).unwrap().extract::<bool>().unwrap(),
                    tup.get_item(1).unwrap().extract::<i64>().unwrap(),
                    tup.get_item(2).unwrap().extract::<i64>().unwrap(),
                )
            });
        assert!(
            both_waited.load(Ordering::SeqCst),
            "the producer should publish only after both observers waited"
        );
        assert!(cancelled, "the cancelled future should stay cancelled");
        assert_eq!(
            awaited, 42,
            "a second observer resolves even after the first future is cancelled"
        );
        assert_eq!(
            polled, 42,
            "the handle stays observable after one observer is cancelled"
        );
    }

    // Two as_asyncio() futures waiting on one handle both resolve on a real
    // loop.
    // Attests HDL-3, HDL-7.
    #[test]
    fn two_asyncio_observers_resolve_on_loop() {
        ensure_python();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 8i64.into_py_any(py).unwrap());
        let (tx, handle) = pending_handle();
        let both_waited = Arc::new(AtomicBool::new(false));
        let both_waited_in_producer = Arc::clone(&both_waited);
        get_tokio_runtime().spawn(async move {
            // The Handle holds one receiver and each pending `as_asyncio()`
            // clones another.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.receiver_count() < 3 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            both_waited_in_producer.store(tx.receiver_count() >= 3, Ordering::SeqCst);
            let _ = tx.send(Some(Ok(value)));
        });
        let (a, b) = monarch_with_gil_blocking(GilSite::Test, |py| -> (i64, i64) {
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            let res = helper.getattr("run_two").unwrap().call1((h,)).unwrap();
            let tup = res.cast::<PyTuple>().unwrap();
            (
                tup.get_item(0).unwrap().extract::<i64>().unwrap(),
                tup.get_item(1).unwrap().extract::<i64>().unwrap(),
            )
        });
        assert!(
            both_waited.load(Ordering::SeqCst),
            "the value should be sent only after both observers waited"
        );
        assert_eq!((a, b), (8, 8), "both observers should resolve to the value");
    }

    // Scheduling completion onto a closed loop returns an error rather than
    // panicking; the observer swallows this and logs.
    // Attests HDL-7.
    #[test]
    fn schedule_on_closed_loop_errs_not_panics() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let asyncio = py.import("asyncio").unwrap();
            let event_loop = asyncio.call_method0("new_event_loop").unwrap();
            let fut = event_loop.call_method0("create_future").unwrap();
            event_loop.call_method0("close").unwrap();
            let result = schedule_completion(py, &event_loop, &fut, Ok(py.None()));
            assert!(
                result.is_err(),
                "call_soon_threadsafe on a closed loop should error, not panic"
            );
        });
    }

    // A negative, NaN, or non-finite timeout is rejected with ValueError rather
    // than panicking in `Duration::from_secs_f64`.
    // Attests HDL-12.
    #[test]
    fn get_invalid_timeout_raises_value_error() {
        ensure_python();
        let (_tx, handle) = pending_handle();
        let r = monarch_with_gil_blocking(GilSite::Test, |py| Py::new(py, handle).unwrap());
        monarch_with_gil_blocking(GilSite::Test, |py| {
            for bad in [-1.0, f64::NAN, f64::INFINITY] {
                let err = PyHandle::get(r.borrow(py), py, Some(bad)).unwrap_err();
                assert!(
                    err.is_instance_of::<PyValueError>(py),
                    "get(timeout={bad}) should raise ValueError, not panic"
                );
            }
        });
    }

    // On a running asyncio loop, get() warns (UserWarning) regardless of whether
    // the value is ready -- calling the blocking get() from a loop is the
    // anti-pattern being flagged. A ready value still returns after warning.
    // Attests HDL-8.
    #[test]
    fn get_on_asyncio_loop_warns() {
        ensure_python();
        let (value, warned) = monarch_with_gil_blocking(GilSite::Test, |py| -> (i64, bool) {
            let v = 5i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(v).unwrap();
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            let res = helper
                .getattr("run_get_on_loop")
                .unwrap()
                .call1((h,))
                .unwrap();
            let tup = res.cast::<PyTuple>().unwrap();
            (
                tup.get_item(0).unwrap().extract::<i64>().unwrap(),
                tup.get_item(1).unwrap().extract::<bool>().unwrap(),
            )
        });
        assert_eq!(value, 5, "a ready value still returns after warning");
        assert!(
            warned,
            "get() on a running asyncio loop warns regardless of readiness"
        );
    }

    // A producer error surfaces through the direct read paths: poll() returns it
    // and get() raises it (the primary sync API path, previously observed only
    // through await/as_asyncio).
    // Attests HDL-2.
    #[test]
    fn poll_and_get_surface_producer_error() {
        ensure_python();
        let (tx, handle) = pending_handle();
        tx.send(Some(Err(PyValueError::new_err("boom")))).unwrap();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let r = Py::new(py, handle).unwrap();
            let poll_err = r.borrow(py).poll().unwrap_err();
            assert!(
                poll_err.is_instance_of::<PyValueError>(py),
                "poll() should surface the producer error"
            );
            let get_err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
            assert!(
                get_err.is_instance_of::<PyValueError>(py),
                "get() should raise the producer error"
            );
        });
    }

    // A producer already gone when poll() or get() is called surfaces a Python
    // error through the ready check rather than being mistaken for pending work.
    // Attests HDL-10.
    #[test]
    fn dropped_producer_surfaces_error() {
        ensure_python();
        let (tx, handle) = pending_handle();
        drop(tx);
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let r = Py::new(py, handle).unwrap();
            let poll_err = r.borrow(py).poll().unwrap_err();
            assert_producer_ended_error(py, &poll_err);
            let err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
            assert_producer_ended_error(py, &err);
        });
    }

    // HDL-10: losing the producer after get() enters its pending branch raises
    // the producer-lifecycle error, not TimeoutError or a watch-channel error.
    #[test]
    fn get_pending_surfaces_dropped_producer() {
        ensure_python();
        let (producer, result) = monarch_with_gil_blocking(GilSite::Test, |py| {
            let (tx, handle) = pending_handle();
            let handle = Py::new(py, handle).unwrap();
            assert!(handle.borrow(py).poll().unwrap().is_none());
            // Load asyncio before starting the producer: importing it can
            // release the GIL, but the ready check must run while we hold it.
            assert!(pyo3_async_runtimes::get_running_loop(py).is_err());

            // This thread cannot drop the sender until get() releases the GIL.
            // The sender therefore stays connected through get()'s ready check,
            // forcing the pending branch without a scheduling sleep.
            let producer = thread::spawn(move || {
                monarch_with_gil_blocking(GilSite::Test, |_py| drop(tx));
            });
            let result = PyHandle::get(handle.borrow(py), py, Some(5.0));
            (producer, result)
        });
        producer.join().expect("producer thread should finish");
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let error = result.expect_err("get() should report the dropped producer");
            assert_producer_ended_error(py, &error);
        });
    }

    // A producer that publishes and drops its sender between poll()'s empty
    // read and its closed check: poll() and get(), whose ready fast path is
    // poll(), return what was published, not producer loss.
    // Attests HDL-2, HDL-10.
    #[test]
    fn poll_and_get_read_an_outcome_sent_just_before_close() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let publish_then_close = |outcome: PyResult<Py<PyAny>>| {
                let (tx, handle) = pending_handle();
                POLL_BETWEEN_CHECKS.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        tx.send(Some(outcome)).unwrap();
                        drop(tx);
                    }));
                });
                Py::new(py, handle).unwrap()
            };

            let h = publish_then_close(Ok(42i64.into_py_any(py).unwrap()));
            let polled = h.borrow(py).poll().unwrap().expect("the published value");
            assert_eq!(polled.extract::<i64>(py).unwrap(), 42);

            let h = publish_then_close(Err(PyValueError::new_err("boom")));
            let err = h.borrow(py).poll().unwrap_err();
            assert!(
                err.is_instance_of::<PyValueError>(py),
                "poll() should surface the stored exception, not producer loss: {err}"
            );

            let h = publish_then_close(Ok(7i64.into_py_any(py).unwrap()));
            let got = PyHandle::get(h.borrow(py), py, None).unwrap();
            assert_eq!(got.extract::<i64>(py).unwrap(), 7);

            let h = publish_then_close(Err(PyValueError::new_err("boom")));
            let err = PyHandle::get(h.borrow(py), py, None).unwrap_err();
            assert!(
                err.is_instance_of::<PyValueError>(py),
                "get() should raise the stored exception, not producer loss: {err}"
            );
        });
    }

    // The blocking future path reports producer loss even without poll()/get().
    // Attests HDL-10.
    #[test]
    fn wait_future_dropped_producer_surfaces_runtime_error() {
        ensure_python();
        let (tx, handle) = pending_handle();
        drop(tx);

        let err = get_tokio_runtime()
            .block_on(handle.core.wait_future())
            .unwrap_err();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_producer_ended_error(py, &err);
        });
    }

    // The Rust-only completion observer reports the same producer-lifecycle
    // failure as Python's synchronous and asyncio observers.
    // Attests HDL-10.
    #[test]
    fn wait_completion_dropped_producer_surfaces_runtime_error() {
        ensure_python();
        let (tx, handle) = pending_handle();
        drop(tx);

        let err = get_tokio_runtime()
            .block_on(handle.wait_completion())
            .unwrap_err();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_producer_ended_error(py, &err);
        });
    }

    // complete_asyncio_future swallows InvalidStateError from an already-settled
    // (non-cancelled) future, and propagates any other setter error.
    // Attests HDL-7.
    #[test]
    fn complete_asyncio_future_swallows_and_propagates() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let event_loop = py
                .import("asyncio")
                .unwrap()
                .call_method0("new_event_loop")
                .unwrap();
            let settled = event_loop.call_method0("create_future").unwrap();
            settled.call_method1("set_result", (1i64,)).unwrap();
            assert!(
                complete_asyncio_future(&settled, false, py.None()).is_ok(),
                "InvalidStateError from a settled future should be swallowed"
            );
            event_loop.call_method0("close").unwrap();

            // A fake future whose setter raises a non-InvalidStateError: it must
            // propagate rather than be swallowed.
            let helper = PyModule::from_code(
                py,
                cr#"
class RaisingFuture:
    def cancelled(self):
        return False

    def set_result(self, value):
        raise ValueError("nope")
"#,
                c"raising_future.py",
                c"raising_future",
            )
            .unwrap();
            let fake = helper.getattr("RaisingFuture").unwrap().call0().unwrap();
            let err = complete_asyncio_future(&fake, false, py.None()).unwrap_err();
            assert!(
                err.is_instance_of::<PyValueError>(py),
                "a non-InvalidStateError setter error should propagate"
            );
        });
    }

    // get(timeout) whose producer resolves before the deadline returns the value
    // (the Ok branch of tokio::time::timeout, distinct from the timeout-fires and
    // bad-float cases).
    // Attests HDL-2, HDL-12.
    #[test]
    fn get_timeout_returns_value_before_deadline() {
        ensure_python();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 13i64.into_py_any(py).unwrap());
        let (tx, handle) = pending_handle();
        get_tokio_runtime().spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let _ = tx.send(Some(Ok(value)));
        });
        let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
            let r = Py::new(py, handle).unwrap();
            let out = PyHandle::get(r.borrow(py), py, Some(5.0)).unwrap();
            out.extract::<i64>(py).unwrap()
        });
        assert_eq!(got, 13, "a generous timeout returns the produced value");
    }

    // Under a warnings-as-errors filter, a get() on a running loop escalates the
    // anti-pattern warning to an error -- even for a ready value -- rather than
    // silently returning. Warn-regardless means the strict caller sees it.
    // Attests HDL-8.
    #[test]
    fn get_on_loop_under_warnings_as_errors_raises() {
        ensure_python();
        let raised = monarch_with_gil_blocking(GilSite::Test, |py| -> bool {
            let v = 5i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(v).unwrap();
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            helper
                .getattr("run_get_on_loop_strict")
                .unwrap()
                .call1((h,))
                .is_err()
        });
        assert!(
            raised,
            "get() on a loop under warnings-as-errors should raise the escalated warning"
        );
    }

    // The timeout is validated before the ready fast path, so an invalid timeout
    // raises ValueError even when the handle is already resolved (where get()
    // would otherwise return without blocking).
    // Attests HDL-12.
    #[test]
    fn get_invalid_timeout_on_ready_raises() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = 7i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(value).unwrap();
            let r = Py::new(py, handle).unwrap();
            let err = PyHandle::get(r.borrow(py), py, Some(f64::NAN)).unwrap_err();
            assert!(
                err.is_instance_of::<PyValueError>(py),
                "an invalid timeout must raise ValueError even for a ready handle"
            );
        });
    }

    // A producer that drops its sender without sending, once the observer is
    // waiting, surfaces through as_asyncio/await (wait_ready's RecvError ->
    // set_exception) as a raised exception, not a hang -- the async mirror of
    // dropped_producer_surfaces_error. The already-ended case is
    // `as_asyncio_producer_ended_is_done_without_wake`.
    // Attests HDL-10.
    #[test]
    fn as_asyncio_dropped_producer_raises_on_loop() {
        ensure_python();
        let (tx, handle) = pending_handle();
        let observed = Arc::new(AtomicBool::new(false));
        let gate = observed.clone();
        get_tokio_runtime().spawn(async move {
            // The Handle holds one receiver and `wait_ready()` clones a second,
            // so the producer drops only once the observer is on the pending path.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.receiver_count() < 2 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            gate.store(tx.receiver_count() >= 2, Ordering::SeqCst);
            drop(tx);
        });
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            let err = helper
                .getattr("run_await")
                .unwrap()
                .call1((h,))
                .unwrap_err();
            assert_producer_ended_error(py, &err);
        });
        assert!(
            observed.load(Ordering::SeqCst),
            "the producer should drop only after the observer is waiting"
        );
    }

    // A StopIteration producer error is wrapped in RuntimeError before
    // set_exception, since asyncio.Future.set_exception rejects a StopIteration
    // (PEP 479) with a TypeError that would hang the awaiter.
    // Attests HDL-7.
    #[test]
    fn complete_asyncio_future_wraps_stop_iteration() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let event_loop = py
                .import("asyncio")
                .unwrap()
                .call_method0("new_event_loop")
                .unwrap();
            let fut = event_loop.call_method0("create_future").unwrap();
            let stop_iteration = py.get_type::<PyStopIteration>().call0().unwrap().unbind();
            // Must not raise (a raw set_exception(StopIteration) would TypeError).
            complete_asyncio_future(&fut, true, stop_iteration).unwrap();
            let exc = fut.call_method0("exception").unwrap();
            assert!(
                exc.is_instance_of::<PyRuntimeError>(),
                "a StopIteration producer error should be wrapped in RuntimeError"
            );
            assert!(
                !exc.is_instance_of::<PyStopIteration>(),
                "the wrapped error must not remain a StopIteration"
            );
            event_loop.call_method0("close").unwrap();
        });
    }

    // A StopIteration SUBCLASS is also wrapped in RuntimeError: we wrap any
    // StopIteration, not just the exact type, so the awaiter never hits the
    // set_exception TypeError hang.
    // Attests HDL-7.
    #[test]
    fn complete_asyncio_future_wraps_stop_iteration_subclass() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let event_loop = py
                .import("asyncio")
                .unwrap()
                .call_method0("new_event_loop")
                .unwrap();
            let fut = event_loop.call_method0("create_future").unwrap();
            let helper = PyModule::from_code(
                py,
                cr#"
class MyStop(StopIteration):
    pass
"#,
                c"my_stop.py",
                c"my_stop",
            )
            .unwrap();
            let subclass_exc = helper.getattr("MyStop").unwrap().call0().unwrap().unbind();
            complete_asyncio_future(&fut, true, subclass_exc).unwrap();
            let exc = fut.call_method0("exception").unwrap();
            assert!(
                exc.is_instance_of::<PyRuntimeError>(),
                "a StopIteration subclass should be wrapped in RuntimeError"
            );
            assert!(
                !exc.is_instance_of::<PyStopIteration>(),
                "the wrapped error must not remain a StopIteration"
            );
            event_loop.call_method0("close").unwrap();
        });
    }

    // Neither side of the private completion pair can be constructed directly
    // from Python -- both pyclasses have no #[new].
    // Attests HDL-11.
    #[test]
    fn handle_and_completer_not_constructible_from_python() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let handle_type = py.get_type::<PyHandle>();
            let handle_error = handle_type.call0().unwrap_err();
            assert!(
                handle_error.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
                "constructing Handle() from Python should raise TypeError (no __new__)"
            );
            let completer_type = py.get_type::<PyHandleCompleter>();
            let completer_error = completer_type.call0().unwrap_err();
            assert!(
                completer_error.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
                "constructing _HandleCompleter() from Python should raise TypeError (no __new__)"
            );
        });
    }

    // A pending Handle releases the GIL while blocking, allowing the paired
    // producer on another thread to acquire it, publish, and wake the observer.
    // The zero-capacity channel ensures the worker has reached the gate
    // immediately before its GIL acquisition when observation starts.
    // Attests HDL-1, HDL-2, HDL-9, HDL-11.
    #[test]
    fn python_pair_resolves_pending_handle_across_threads() {
        use std::sync::mpsc::sync_channel;
        use std::time::Duration;

        ensure_python();
        let (worker, observed) = monarch_with_gil_blocking(GilSite::Test, |py| {
            let (handle, completer) = _new_handle_pair();
            let handle = Py::new(py, handle).unwrap();
            let completer = Py::new(py, completer).unwrap();
            let (ready_tx, ready_rx) = sync_channel(0);
            let worker = std::thread::spawn(move || {
                ready_tx.send(()).unwrap();
                monarch_with_gil_blocking(GilSite::Test, |py| {
                    completer
                        .borrow_mut(py)
                        .set_result(42i64.into_py_any(py).unwrap())
                        .unwrap();
                });
            });

            ready_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("completion worker should reach its pre-GIL gate");
            assert!(
                PyHandle::poll(&handle.borrow(py)).unwrap().is_none(),
                "the Handle should still be pending before the worker acquires the GIL"
            );
            let value = PyHandle::get(handle.borrow(py), py, Some(2.0))
                .unwrap()
                .extract::<i64>(py)
                .unwrap();
            (worker, value)
        });

        worker.join().expect("completion worker should exit");
        assert_eq!(
            observed, 42,
            "the pending observer should receive the value"
        );
    }

    // Failure publication follows the same pending cross-thread path and wakes
    // the observer with the original Python error.
    // Attests HDL-1, HDL-2, HDL-9, HDL-11.
    #[test]
    fn python_pair_publishes_exception_to_pending_handle_across_threads() {
        use std::sync::mpsc::sync_channel;
        use std::time::Duration;

        ensure_python();
        let worker = monarch_with_gil_blocking(GilSite::Test, |py| {
            let (handle, completer) = _new_handle_pair();
            let handle = Py::new(py, handle).unwrap();
            let completer = Py::new(py, completer).unwrap();
            let (ready_tx, ready_rx) = sync_channel(0);
            let worker = std::thread::spawn(move || {
                ready_tx.send(()).unwrap();
                monarch_with_gil_blocking(GilSite::Test, |py| {
                    let error = PyRuntimeError::new_err("cross-thread failure")
                        .into_value(py)
                        .into_bound(py)
                        .into_any();
                    completer.borrow_mut(py).set_exception(error).unwrap();
                });
            });

            ready_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("completion worker should reach its pre-GIL gate");
            assert!(
                PyHandle::poll(&handle.borrow(py)).unwrap().is_none(),
                "the Handle should still be pending before the worker acquires the GIL"
            );
            let error = PyHandle::get(handle.borrow(py), py, Some(2.0)).unwrap_err();
            assert!(
                error.is_instance_of::<PyRuntimeError>(py),
                "the pending observer should receive the worker's exception"
            );
            assert_eq!(error.to_string(), "RuntimeError: cross-thread failure");
            worker
        });

        worker.join().expect("completion worker should exit");
    }

    // spawn() eagerly runs its producer -- before any observer exists -- and
    // resolves the converted value; no PyPythonTask is involved.
    // Attests HDL-1, HDL-2, HDL-13.
    #[test]
    fn rust_spawn_eagerly_resolves_handle() {
        ensure_python();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = PyHandle::spawn(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
            Ok::<i64, PyErr>(42)
        });
        // Eager: the producer starts before we create any observer.
        get_tokio_runtime()
            .block_on(started_rx)
            .expect("spawn() must start its producer eagerly, before observation");
        let _ = release_tx.send(());
        let out = get_tokio_runtime().block_on(handle.core.wait_future());
        let got =
            monarch_with_gil_blocking(GilSite::Test, |py| out.unwrap().extract::<i64>(py).unwrap());
        assert_eq!(
            got, 42,
            "spawn() should resolve the converted producer value"
        );
    }

    // Two wait_completion() observers of one handle both see the same terminal
    // outcome; observation is non-consuming, and success projects to Ok(()).
    // Attests HDL-3, HDL-13.
    #[test]
    fn rust_wait_completion_is_non_consuming() {
        ensure_python();

        // Success: two observers of one pending handle both resolve to Ok(()).
        let (tx, handle) = pending_handle();
        let ok1 = handle.wait_completion();
        let ok2 = handle.wait_completion();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 1i64.into_py_any(py).unwrap());
        tx.send(Some(Ok(value))).unwrap();
        let (r1, r2) = get_tokio_runtime().block_on(async move { tokio::join!(ok1, ok2) });
        assert!(
            r1.is_ok() && r2.is_ok(),
            "both completion observers should project success to Ok(())"
        );

        // Error: two observers of one failed handle both surface the stored error.
        let (etx, ehandle) = pending_handle();
        let err1 = ehandle.wait_completion();
        let err2 = ehandle.wait_completion();
        etx.send(Some(Err(PyValueError::new_err("boom")))).unwrap();
        let (e1, e2) = get_tokio_runtime().block_on(async move { tokio::join!(err1, err2) });
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(
                e1.unwrap_err().is_instance_of::<PyValueError>(py),
                "the error projection should clone the stored PyErr"
            );
            assert!(e2.unwrap_err().is_instance_of::<PyValueError>(py));
        });
    }

    // Dropping the returned Handle before the producer completes does not cancel
    // it (HDL-4: the core has no producer task handle to abort).
    // Attests HDL-4, HDL-13.
    #[test]
    fn rust_spawn_survives_handle_drop() {
        ensure_python();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = PyHandle::spawn(async move {
            let _ = release_rx.await;
            let _ = done_tx.send(());
            Ok::<i64, PyErr>(7)
        });
        // Drop the observer before releasing the producer.
        drop(handle);
        let _ = release_tx.send(());
        // The producer still runs to completion despite the dropped Handle.
        get_tokio_runtime()
            .block_on(done_rx)
            .expect("dropping the Handle must not cancel the spawn() producer");
    }

    // The real spawn() -> wait_completion() pair: a spawn()-produced future that
    // returns Err surfaces the exact exception (type + message) through
    // wait_completion, and observing twice sequentially returns the same cached
    // error -- proving non-consuming error projection over a real producer.
    // Attests HDL-1, HDL-2, HDL-3, HDL-13.
    #[test]
    fn rust_spawn_error_observed_by_wait_completion_twice() {
        ensure_python();
        let handle =
            PyHandle::spawn(async move { Err::<(), PyErr>(PyValueError::new_err("boom")) });
        // Observe the same terminal error twice, sequentially, over the real
        // spawn()-produced core.
        let first = get_tokio_runtime().block_on(handle.wait_completion());
        let second = get_tokio_runtime().block_on(handle.wait_completion());
        monarch_with_gil_blocking(GilSite::Test, |py| {
            for (label, res) in [("first", first), ("second", second)] {
                let err = res.expect_err("wait_completion should surface the producer error");
                assert!(
                    err.is_instance_of::<PyValueError>(py),
                    "{label}: expected PyValueError"
                );
                let msg = err.value(py).to_string();
                assert!(
                    msg.contains("boom"),
                    "{label}: cached error message should be preserved, got {msg}"
                );
            }
        });
    }
}
