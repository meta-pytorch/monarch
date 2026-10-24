/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `Handle`: an observe-only handle to a background operation.
//!
//! A `Handle` observes one operation through a shared [`Core`]. The operation's
//! producer publishes its `PyResult<Py<PyAny>>` there exactly once, and every
//! observer reads it: synchronously via `get()`, on an `asyncio` loop via
//! `as_asyncio()`/`await`, or without blocking via `poll()`. A published result
//! stays observable by any number of later observers.
//!
//! Most producers start when their Handle is created. A deferred producer, a
//! stream item's or a port receive's, starts on the Handle's first observation
//! (HDL-24).
//!
//! ## Handle invariants (HDL-*)
//!
//! Code that relies on or enforces an invariant is tagged `// HDL-N`, and each
//! `#[cfg(test)]` test that attests one names it; read the matching entry below
//! before changing a tagged site.
//!
//! - **HDL-1 (single terminal state).** A core's state moves from pending
//!   (`Unstarted` or `Running`) to `Done` or `Lost` once and never leaves it.
//!   Enforced by construction: `Publisher::publish` consumes the publisher, its
//!   drop publishes `Lost` only if it never published, and `Core::finish`
//!   panics on a transition from any state but `Running`.
//! - **HDL-2 (outcome encoding).** `Done(Ok(_))` is success, `Done(Err(_))` a
//!   producer error, and `Lost` producer loss (HDL-10).
//! - **HDL-3 (non-consuming reads).** No read moves the outcome out. Under the
//!   state lock an observer clones only the outcome's `Arc`; after unlocking it
//!   clones the value or error (`clone_ref`), or, on `wait_completion`'s
//!   success path, reads just the `Ok`/`Err` tag. Any number of observers see
//!   the same result.
//! - **HDL-4 (drop does not cancel; drop never panics).** Dropping a `Handle`
//!   never cancels a running producer; a `Handle` observes, it does not own the
//!   producer's lifecycle. Producers hold the core weakly and publish through
//!   it. An unstarted deferred producer is dropped with the last observer,
//!   never polled. A producer with `ObserverLoss::Stop` (a port receive)
//!   instead stops when the last observer drops (HDL-23).
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
//! - **HDL-7 (`as_asyncio` publish).** The observer waits through
//!   `wait_terminal`, which checks the state before it registers a wake
//!   (HDL-25). The result is published on the loop thread through
//!   `complete_asyncio_future`, by the wake reader (HDL-17) or by the
//!   fallback's `call_soon_threadsafe` callback (HDL-21), or, for a Handle
//!   already complete, by `as_asyncio` itself (HDL-22).
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
//!   warnings-as-errors filter the warning escalates to an error. The check
//!   uses `asyncio._get_running_loop()`, which returns `None` off a loop where
//!   `asyncio.get_running_loop()` builds and raises an exception.
//! - **HDL-9 (state lock discipline).** The state lock is a leaf: while it is
//!   held, no code runs Python, acquires or releases the GIL, awaits, notifies,
//!   or drops a Python object. Lock order is GIL, then state lock. An observer
//!   clones an `Arc` under the lock and reads the value after unlocking,
//!   because `PyErr::clone_ref` can release and reacquire the GIL while it
//!   normalizes a lazy error. A transition moves the retired state out under
//!   the lock and drops it after unlocking, because dropping a `Py` on an
//!   attached thread can run a finalizer that re-enters the Handle. Enforced by
//!   construction; exercised by the lazy-error and re-entrant-destruction
//!   tests.
//! - **HDL-10 (dropped producer surfaces, never hangs).** A `Publisher` dropped
//!   without publishing publishes `Lost`: a task dropped at runtime shutdown,
//!   before or after its first poll, an inline drive that unwinds, or a
//!   `_HandleCompleter` dropped unresolved. Every observer, on the
//!   non-blocking (`poll`), sync (`get`/`wait_future`), async (`as_asyncio`) and
//!   `wait_completion` paths, then receives the same producer-lifecycle failure,
//!   `RuntimeError("Handle producer ended without publishing a result")`, rather
//!   than waiting forever.
//! - **HDL-11 (Python construction is paired).** Neither `Handle` nor
//!   `_HandleCompleter` has `#[new]`. Internal Python may call the private
//!   `_new_handle_pair()`, which atomically creates a pending `Handle` and its
//!   sole producer. The factory initializes the completer with
//!   `Some(publisher)`. `set_exception()` rejects a non-`BaseException` before
//!   taking it; the first `set_result()` or valid `set_exception()` takes it,
//!   producing the only state transition to `None`, and any later completion
//!   attempt raises. Dropping the completer unresolved publishes `Lost`
//!   (HDL-10). Python therefore cannot create a pending `Handle` without its
//!   producer. Native producers remain the default; this private pair is
//!   reserved for operations whose semantics and scheduling are owned by
//!   Python.
//! - **HDL-12 (`get()` timeout contract).** `get()` validates the timeout up
//!   front (rejecting negative/NaN/non-finite as `ValueError` via
//!   `try_from_secs_f64`, never panicking) and before the ready fast path, so an
//!   invalid timeout raises deterministically even for a ready handle. A timeout
//!   is non-cancelling: it raises `TimeoutError` while leaving the producer and
//!   every observer untouched, so a later `poll()`/`get()`/`await` still
//!   observes completion. A `get()` that was driving its producer hands it to
//!   Tokio first (HDL-24).
//! - **HDL-13 (direct Rust producer).** A Rust caller can eagerly produce a
//!   `Handle` from a `Send + 'static` future.
//!   [`PyHandle::spawn`] wraps the future with conversion of a successful Rust
//!   value to `Py<PyAny>` under `monarch_with_gil` and delegates to
//!   [`PyHandle::spawn_pyobject`]. The latter starts one producer and publishes
//!   its Python value or error once through its `Publisher`, without a
//!   result-conversion GIL acquisition (HDL-1/HDL-2). A `Publisher` whose
//!   observers have all gone logs an error result instead, with the producer's
//!   own copy of the creation traceback when one was captured.
//!   Dropping the Handle does not cancel the producer (HDL-4).
//!   [`PyHandle::spawn_pyobject_with_traceback`] does the same, and also keeps
//!   the producer's creation traceback for that log; the endpoint adverbs use
//!   it. [`PyHandle::wait_completion`] is the matching observer: it projects the
//!   terminal value to `PyResult<()>`, touching neither the GIL nor the Python
//!   value on the success path (a tag-only read, HDL-3) and cloning the error
//!   after unlocking only on the error path (HDL-9). The three constructors and
//!   this observer are `pub` Rust-only methods outside `#[pymethods]`
//!   (HDL-11).
//! - **HDL-14 (Rust value observation is owned and non-consuming).**
//!   [`PyHandle::wait_future`] returns a `Send + 'static` future that owns a
//!   strong reference to the core. The waiter borrows nothing from the
//!   `PyHandle`, survives the wrapper being dropped, and leaves the terminal
//!   value available to every other observer.
//! - **HDL-15 (Rust ready construction starts no work).**
//!   [`PyHandle::from_value`] constructs a core already `Done`, with no producer
//!   and no wake. It is public to Rust callers but remains outside
//!   `#[pymethods]`, preserving HDL-11's direct-construction restriction.
//! - **HDL-16 (permanent Python identity).** `Handle` and `WouldBlockRuntime`
//!   have the canonical module `monarch._rust_bindings.monarch_hyperactor.handle`.
//! - **HDL-17 (GIL-free completion wake).** For a Handle still pending when
//!   `as_asyncio` is called, on a loop that accepts `add_reader`, the observer
//!   task holds a `Notifier` (a token,
//!   an mpsc sender and an `Arc<Waker>`) and the `wait_terminal` future. It
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
//! - **HDL-23 (stop when nobody can observe the result).** A `Handle`, its
//!   clones, a blocked `get()`, a Rust waiter and the native waiter behind
//!   `await`/`as_asyncio()` each hold the core strongly; producers hold it
//!   weakly. When the last strong reference drops, the core's `Drop` stops a
//!   producer with `ObserverLoss::Stop` (a port receive). Its stop future is
//!   created before its task is scheduled, so a loss before the first poll
//!   still stops it. The stop is not immediate: the task must notice it, and
//!   it can still take a message before then. Other producers keep running
//!   when their observers are dropped (HDL-4), and so does an unpickling retry
//!   after an interrupted decode (HDL-26), whose message is already taken.
//! - **HDL-24 (deferred start).** A deferred producer starts on the first
//!   observation that passes its own checks. `get()` refuses on Tokio,
//!   validates its timeout and warns on a running loop before it touches the
//!   state, and `as_asyncio()` checks for a running loop first, so a rejected
//!   observation starts nothing. Start authority lives in the core, so any
//!   observer can start the producer, including a clone. `get()` drives the
//!   producer inline, on the calling thread with the GIL released; every other
//!   observation spawns it on Tokio. If Ctrl-C (on Python's main thread) or the
//!   timeout ends a driving `get()` first, it hands the unfinished producer to
//!   Tokio before it raises, so the producer's result still reaches the core
//!   and stays readable. Ctrl-C during decoding is HDL-26's case.
//! - **HDL-25 (on-demand wake).** A core creates its wake only when an observer
//!   must wait. A waiter clones the wake and creates its `OwnedNotified` under
//!   the state lock; a publisher moves the wake out under the lock and calls
//!   `notify_waiters()` after unlocking. Tokio delivers `notify_waiters()` to
//!   every `OwnedNotified` created before the call, polled or not, and the lock
//!   orders each registration against the publisher's transition, so no
//!   wake-up is lost. Reading a finished value and driving a producer inline
//!   create no wake.
//! - **HDL-26 (interrupted decoding).** A `get()` driving a producer on
//!   Python's main thread also runs its decoding there, where a signal handler
//!   can raise mid-decode. Deferred producers decode through
//!   `ReceiveOutcome::decode`, which takes a decoding failure raised there
//!   that is not an `Exception`, such as `KeyboardInterrupt`, as the caller's
//!   interrupt, not the outcome. `get()` raises it and decodes the same input
//!   again on Tokio, so the outcome still reaches the core and stays readable.
//!   A failure that is an `Exception` is the outcome, and so is an exception
//!   the message itself carries, whatever its type. The retry unpickles from
//!   the start: side effects of the interrupted attempt, such as a user
//!   `__setstate__`'s, are not undone.

use std::error::Error;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::Weak;

use monarch_types::SerializablePyErr;
use monarch_types::py_global;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyBaseException;
use pyo3::exceptions::PyException;
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
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

use crate::handle_wake::LoopWake;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::is_in_tokio_runtime;
use crate::runtime::is_main_thread;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on_inline;

// Held so the completion callback can catch and swallow it: publishing a result
// onto an asyncio.Future that's already settled (the awaiter cancelled it, or it
// resolved) raises InvalidStateError, which is benign here.
py_global!(invalid_state_error, "asyncio", "InvalidStateError");

// HDL-8: unlike `get_running_loop`, returns `None` off a loop instead of raising.
py_global!(running_loop_or_none, "asyncio", "_get_running_loop");

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
fn handle_producer_ended_error() -> PyErr {
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
fn log_unobserved(outcome: &PyResult<Py<PyAny>>, traceback: Option<Py<PyAny>>) {
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
            "a background task errored but is not being awaited; this will not crash your \
            program, but indicates that something went wrong.\n{}\nTraceback where the task \
            was created (most recent call last):\n{}",
            SerializablePyErr::from(py, pyerr),
            tb
        );
    });
}

/// Return this observer's reference to the shared value or exception (HDL-3).
///
/// Hold the GIL, but not the Handle's state lock: cloning a Python error can
/// release and reacquire the GIL, risking deadlock with another observer
/// that holds the GIL and is waiting for that lock (HDL-9).
fn read(py: Python<'_>, outcome: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match outcome {
        Ok(value) => Ok(value.clone_ref(py)),
        Err(error) => Err(error.clone_ref(py)),
    }
}

/// Finish unpickling a received message into a Python value or exception.
/// Used after an interrupt requires unpickling to retry on another thread.
type UnpickleFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>;

/// Take one message from a port or ValueStream reply queue and unpickle it.
///
/// Wait for a message if necessary. This work starts on the Handle's first
/// observation (HDL-24). `ReceiveOutcome` reports either its outcome or an
/// interrupt that leaves unpickling unfinished.
type ReceiveFuture = Pin<Box<dyn Future<Output = ReceiveOutcome> + Send>>;

/// The outcome of receiving and unpickling a port message or stream reply.
///
/// A signal may interrupt main-thread unpickling after the message has been
/// taken from its queue. Raise the interrupt in the caller's `get()`, but
/// retain the message and retry unpickling elsewhere so the Handle can
/// still supply its result (HDL-26).
pub(crate) enum ReceiveOutcome {
    /// The message's Python value or exception, or a receive/unpickle error.
    Outcome(PyResult<Py<PyAny>>),

    /// The message was received, but the caller was interrupted while unpickling.
    Interrupted {
        /// The signal-handler exception to raise in the interrupted `get()`.
        interrupt: PyErr,
        /// Retry unpickling the retained message, not receiving another one,
        /// off Python's main thread.
        finish: UnpickleFuture,
    },
}

impl ReceiveOutcome {
    /// Unpickle a received port message or stream reply without losing it if
    /// the caller is interrupted (HDL-26). The GIL must be held.
    ///
    /// `input` holds the received message and any associated references.
    /// The supplied `decode` function returns its Python value or remote
    /// exception; a failure while unpickling is handled separately.
    ///
    /// On Python's main thread, unpickle a clone and retain the original.
    /// If unpickling raises an interrupt such as KeyboardInterrupt, return
    /// that interrupt together with work that retries the original message
    /// on Tokio. Ordinary unpickling errors become the Handle's error.
    ///
    /// On other threads, unpickle the input directly, without retaining a
    /// copy for retry. Retrying unpickling never receives another message.
    pub(crate) fn decode<T, D>(py: Python<'_>, site: GilSite, input: T, decode: D) -> Self
    where
        T: Clone + Send + 'static,
        D: Fn(Python<'_>, T) -> PyResult<PyResult<Py<PyAny>>> + Send + 'static,
    {
        // Python runs signal handlers only on its main thread. Elsewhere,
        // consume the message once. Flatten an unpickling failure into the
        // receive's error, just like an exception carried by the message.
        if !on_main_thread() {
            return Self::Outcome(decode(py, input).unwrap_or_else(Err));
        }
        // Unpickle a clone so the original message survives an interrupted attempt.
        let decoded = decode(py, input.clone());
        match decoded {
            // This includes a remote exception: it is the receive's outcome,
            // not an interruption of unpickling.
            Ok(outcome) => Self::Outcome(outcome),
            // Treat a BaseException outside Exception, such as KeyboardInterrupt,
            // as an interrupt. get() will schedule the retry and raise the interrupt.
            Err(interrupt) if !interrupt.is_instance_of::<PyException>(py) => Self::Interrupted {
                interrupt,
                // Retry unpickling the original message on Tokio; receive no new message.
                finish: Box::pin(monarch_with_gil(site, move |py| decode(py, input)?)),
            },
            // An ordinary unpickling failure completes the receive with that error.
            Err(error) => Self::Outcome(Err(error)),
        }
    }
}

/// Whether this is Python's main thread, computed once per thread.
fn on_main_thread() -> bool {
    thread_local! {
        static ON_MAIN_THREAD: bool = is_main_thread();
    }
    ON_MAIN_THREAD.with(|on_main| *on_main)
}

/// Finish receiving and unpickling a port message or stream reply on Tokio.
///
/// Return its value or error. If unpickling reports an interrupt, await
/// the retry instead: the interrupt belongs to the calling observer,
/// not to the Handle's shared result (HDL-26).
async fn settle(future: ReceiveFuture) -> PyResult<Py<PyAny>> {
    match future.await {
        ReceiveOutcome::Outcome(result) => result,
        ReceiveOutcome::Interrupted { finish, .. } => finish.await,
    }
}

/// What a spawned receive does when its last Handle or active waiter disappears.
/// This policy applies after start; an unstarted receive is dropped with its core.
#[derive(Clone, Copy)]
pub(crate) enum ObserverLoss {
    /// Stop the unfinished receive and release its captured resources (ports).
    Stop,
    /// Keep the work running even without observers (stream items).
    Continue,
}

/// Hold a port or stream-reply receive until its Handle is first observed
/// (HDL-24). Creating this state takes no message from the queue.
struct DeferredReceive {
    /// Unstarted work that takes the next message, waits if necessary, and
    /// unpickles it. The first observer takes this future and runs it inline
    /// or starts it as a Tokio task.
    future: ReceiveFuture,
    /// Optional Python stack captured when the receive or stream item was
    /// created. When the work starts, this copy goes with the producer so it
    /// can log a failure even after every observer and the core's copy of
    /// the traceback are gone (HDL-13).
    traceback: Option<Py<PyAny>>,
    /// Whether a spawned receive stops or continues after its last observer goes.
    /// Ports use ObserverLoss::Stop to release their receiver (HDL-23); stream
    /// items use ObserverLoss::Continue to finish work already started.
    observer_loss: ObserverLoss,
}

/// Whether the shared Handle has unstarted receive work, is awaiting a result,
/// or has finished. Every observer consults the same state under the core's lock.
enum State {
    /// A port or stream-reply receive waiting for its first observer.
    /// No message has been taken from the queue (HDL-24).
    Unstarted(DeferredReceive),
    /// No result yet. A Tokio task, an observer running the work inline, or
    /// a Python completer is responsible for supplying it.
    Running {
        /// Wake observers waiting for the result. Created only when an observer
        /// first needs to wait. Completion changes the state before notifying
        /// the waiters; the notification carries no result itself (HDL-25).
        wake: Option<Arc<Notify>>,
        /// Tell a spawned receive to stop when the last Handle or active waiter
        /// drops the core. Present only for work with ObserverLoss::Stop;
        /// ordinary producers continue without observers (HDL-23).
        stop: Option<Arc<Notify>>,
    },
    /// The final value or error, never replaced. Every observer shares this
    /// outcome, and reading it does not consume it (HDL-1, HDL-2, HDL-3).
    Done(Arc<PyResult<Py<PyAny>>>),
    /// The producer ended without supplying a result. Observers raise the
    /// producer-ended error rather than waiting indefinitely (HDL-10).
    Lost,
}

/// The result of checking whether a Handle's outcome is available.
/// This snapshot can be used after releasing the core's state lock.
enum Observed {
    /// The stored value or error, kept alive by the Arc after unlocking.
    /// Cloning its Python value or exception requires the GIL and must
    /// happen outside the core's state lock.
    Done(Arc<PyResult<Py<PyAny>>>),
    /// The producer ended without a result; report the producer-ended error
    /// to the observer (HDL-10).
    Lost,
    /// No result is available yet. The observer must wait, or return None
    /// if it is only polling.
    Pending,
}

/// Where to run a port or stream-reply receive if this observer starts it.
#[derive(Clone, Copy)]
enum ReceiveExecution {
    /// observe() starts the receive as a Tokio task.
    Tokio,
    /// get() takes the receive to poll on the calling thread.
    CallingThread,
}

/// Whether an observer checks the shared result or takes an unstarted receive
/// to run on the calling thread.
///
/// Only one observer can take the port or stream-reply receive. Other observers
/// read or wait for its shared result (HDL-24).
enum Observation {
    /// A snapshot of the shared result: ready, producer lost, or still pending.
    /// This observer takes no ownership of the receive future.
    Observed(Observed),
    /// This observer claimed the unstarted receive to run on its own thread.
    /// It owns both the future and the publisher that supplies the shared result
    /// (HDL-24).
    Drive(Drive),
}

/// A port or stream-reply receive claimed by a blocking `get()`.
///
/// The caller runs the receive on its own thread and publishes the result for
/// every observer. If it stops waiting before completion, these owned values
/// can be handed to Tokio so the receive continues (HDL-24).
struct Drive {
    /// The claimed future: take the next queued message, wait if necessary,
    /// and unpickle it into a Python value or exception.
    future: ReceiveFuture,
    /// Store the outcome in the shared core and wake its waiters. Dropping this
    /// without publishing tells observers that the producer ended without a
    /// result, rather than leaving them waiting indefinitely (HDL-10).
    publisher: Publisher,
    /// The observer-loss policy to apply if this receive is handed to Tokio.
    /// Ports use ObserverLoss::Stop; stream items use ObserverLoss::Continue
    /// so work already started keeps running without observers (HDL-23).
    observer_loss: ObserverLoss,
}

/// Store one operation's result and coordinate every observer waiting for it.
///
/// Handles and active waiters share an `Arc<Core>`. The producer's `Publisher`
/// holds only a `Weak<Core>`, so running work does not keep an abandoned core
/// alive (HDL-23). Once stored, the value or error remains available to every
/// observer; reading it does not consume it (HDL-3).
pub(crate) struct Core {
    /// The operation's progress, shared outcome and optional wake/stop signals.
    /// The mutex serializes taking an unstarted receive, registering waiters
    /// and publishing completion. It is a leaf lock: no Python calls, GIL
    /// changes, awaiting, notification or Python-object drops while held (HDL-9).
    state: Mutex<State>,
    /// Optional Python stack captured when the operation was created.
    /// The producer keeps a separate copy so it can log an unobserved failure
    /// even after the last observer and this core have gone (HDL-13).
    traceback: Option<Py<PyAny>>,
}

impl Core {
    /// Create a shared core in the supplied state, retaining any creation stack.
    /// This allocates the core; it starts no work and creates no wake signal.
    fn new(state: State, traceback: Option<Py<PyAny>>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(state),
            traceback,
        })
    }

    /// Lock the shared state until the returned guard is dropped.
    /// While holding it, do not call Python, change the GIL, await, notify,
    /// or drop Python objects. Code under this leaf lock must not panic,
    /// so a poisoned mutex indicates a violated invariant (HDL-9).
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("should not be poisoned: nothing panics under the state lock (HDL-9)")
    }

    /// Check whether the shared result is available without starting a receive.
    ///
    /// Return the stored outcome, producer loss, or Pending for both unstarted
    /// and running work. Clone only the outcome's Arc under the lock, so the
    /// caller can read its Python value or error after unlocking (HDL-9).
    fn snapshot(&self) -> Observed {
        match &*self.lock() {
            State::Done(outcome) => Observed::Done(outcome.clone()),
            State::Lost => Observed::Lost,
            State::Unstarted(_) | State::Running { .. } => Observed::Pending,
        }
    }

    /// Check for a result, starting an unstarted port or stream-reply receive
    /// as a Tokio task (HDL-24).
    ///
    /// Unlike snapshot(), this can start work; it never runs the receive on
    /// the calling thread. Return the stored outcome, producer loss, or Pending
    /// without waiting for completion or consuming the result.
    fn observe(self: &Arc<Self>) -> Observed {
        // Request Tokio execution, so any unstarted receive is spawned there.
        // Only ReceiveExecution::CallingThread returns Observation::Drive.
        // get() chooses that mode to run the receive itself; this Tokio request
        // cannot return that variant.
        let Observation::Observed(observed) = self.observe_or_drive(ReceiveExecution::Tokio) else {
            unreachable!("only a driving observation receives the producer");
        };
        observed
    }

    /// Check the shared result, choosing how to start a port or stream-reply
    /// receive that has not started. The state lock ensures only one observer
    /// takes its future (HDL-24).
    ///
    /// The caller explicitly chooses `execution`; no thread is detected here:
    ///
    /// - `get()` passes `ReceiveExecution::CallingThread`: return the receive
    ///   future and its publisher as `Observation::Drive`. The caller polls
    ///   that future on its own thread; this function does not poll it.
    /// - `observe()` passes `ReceiveExecution::Tokio`: spawn the receive as a
    ///   Tokio task and return `Observation::Observed(Observed::Pending)`.
    ///
    /// If the operation is already running or finished, ignore `execution` and
    /// return its snapshot as `Observation::Observed`. Start no new receive
    /// and transfer no future to the caller.
    fn observe_or_drive(self: &Arc<Self>, execution: ReceiveExecution) -> Observation {
        // Keep the lock through the start decision and the move to Running,
        // so another observer cannot take the same receive future.
        let mut state = self.lock();

        // Finished or already started: return its snapshot. Otherwise borrow
        // the unstarted receive to decide how it will be started below.
        let deferred = match &*state {
            State::Done(outcome) => return Observation::Observed(Observed::Done(outcome.clone())),
            State::Lost => return Observation::Observed(Observed::Lost),
            State::Running { .. } => return Observation::Observed(Observed::Pending),
            State::Unstarted(deferred) => deferred,
        };

        // Create a signal only for a Tokio receive that must stop when its last
        // observer disappears. An inline get() keeps the core alive; if it
        // later hands the receive to Tokio, hand_off() installs the signal then.
        let stop = match execution {
            ReceiveExecution::CallingThread => None,
            ReceiveExecution::Tokio if matches!(deferred.observer_loss, ObserverLoss::Stop) => {
                Some(Arc::new(Notify::new()))
            }
            ReceiveExecution::Tokio => None,
        };
        // Register for Core::Drop's stop notification before scheduling the
        // task. Otherwise the last observer could drop before registration,
        // leaving the receive running with nobody to read its result (HDL-23).
        let observer_stop = match &stop {
            Some(stop) => ObserverStop::WhenUnobserved(Arc::clone(stop).notified_owned()),
            None => ObserverStop::Never,
        };
        // mem::replace installs Running in the core and returns the old state.
        // Extract its Unstarted receive: deferred now owns that receive,
        // rather than borrowing it as above. Other observers see Running
        // and wait for the result instead of starting another receive.
        // No wake signal is needed unless an observer actually waits (HDL-25).
        let State::Unstarted(deferred) =
            mem::replace(&mut *state, State::Running { wake: None, stop })
        else {
            unreachable!("the state was Unstarted under this lock");
        };
        // Drop the mutex guard, not the core. This releases the state lock
        // before scheduling work or dropping Python objects in the receive (HDL-9).
        drop(state);

        // Give the receive its sole publisher before scheduling it. If a task
        // is dropped before its first poll, the publisher still drops and marks
        // the core Lost. A weak reference does not keep observers alive (HDL-10).
        let publisher = Publisher::new(Arc::downgrade(self), deferred.traceback);
        match execution {
            // get() takes both the future and publisher, then polls the future
            // on its own thread. Nothing is spawned here.
            ReceiveExecution::CallingThread => Observation::Drive(Drive {
                future: deferred.future,
                publisher,
                observer_loss: deferred.observer_loss,
            }),
            ReceiveExecution::Tokio => {
                // The task owns the future and publisher. This caller gets a
                // Pending snapshot without waiting; completion can race its return.
                spawn_producer(settle(deferred.future), publisher, observer_stop);
                Observation::Observed(Observed::Pending)
            }
        }
    }

    /// Continue a receive on Tokio after the get() running it stops waiting.
    ///
    /// A timeout or interrupt ends that get(), not the receive. Move its
    /// unfinished future and publisher into a Tokio task so its value or error
    /// still becomes the Handle's shared result (HDL-24).
    ///
    /// For a port receive, install the stop signal now: the task must stop if
    /// every Handle and active waiter disappears. Started stream items instead
    /// continue without observers (HDL-23).
    fn hand_off(&self, drive: Drive) {
        // Only ObserverLoss::Stop needs a stop-notification future. Stream
        // items use ObserverLoss::Continue and need no stop signal.
        let observer_stop = match drive.observer_loss {
            ObserverLoss::Stop => {
                let stop = Arc::new(Notify::new());
                // Register before scheduling the task, so Core::Drop's notification
                // reaches it even if the last observer goes before its first poll.
                let notified = Arc::clone(&stop).notified_owned();
                // The inline get() already marked this core Running. Store the
                // signal there; Core::Drop notifies it when the last observer goes.
                match &mut *self.lock() {
                    State::Running { stop: slot, .. } => *slot = Some(stop),
                    _ => unreachable!("HDL-24: a driven producer keeps the core running"),
                }
                // The core holds the Notify; the task awaits this OwnedNotified.
                ObserverStop::WhenUnobserved(notified)
            }
            ObserverLoss::Continue => ObserverStop::Never,
        };
        // Resume the same receive, not a new one. The task owns its future and
        // publisher; settle() also finishes any interrupted unpickling.
        spawn_producer(settle(drive.future), drive.publisher, observer_stop);
    }

    /// Give the producer its own Python reference to the captured creation stack.
    /// Its publisher retains this reference to log a failure even after all
    /// observers and this core have gone (HDL-13).
    ///
    /// Clone the Python reference under the GIL if a stack was captured.
    /// Otherwise return None without acquiring the GIL.
    fn producer_traceback(&self) -> Option<Py<PyAny>> {
        self.traceback
            .as_ref()
            .map(|t| monarch_with_gil_blocking(GilSite::Traceback, |py| t.clone_ref(py)))
    }

    /// Ensure an unstarted port or stream-reply receive is started on Tokio.
    /// Rust waiters call this before waiting for the shared result. If the
    /// operation is already running or finished, start nothing (HDL-24).
    /// This neither waits for completion nor consumes the stored result.
    fn start(self: &Arc<Self>) {
        // observe() starts an unstarted receive and returns a result snapshot.
        // Discard only that snapshot, not the core, receive or stored outcome.
        drop(self.observe());
    }

    /// Store the operation's final value/error (Done) or producer loss (Lost),
    /// then wake every observer waiting for it (HDL-1, HDL-25).
    ///
    /// Only a Running core may finish; a second completion violates HDL-1.
    /// Replace the state under the lock, but release it before notifying or
    /// dropping the old state's fields (HDL-9). Woken observers read the
    /// terminal state; the notification itself carries no result.
    fn finish(&self, terminal: State) {
        // Install the terminal state and keep ownership of the old state.
        // The temporary mutex guard drops at this semicolon, releasing the lock.
        let previous = mem::replace(&mut *self.lock(), terminal);
        // Extract the wake signal outside the lock. Any state other than
        // Running means completion was attempted without a running producer.
        let State::Running { wake, .. } = previous else {
            panic!("HDL-1: only a running producer publishes");
        };
        // Tests can pause here: the final state is visible, but waiters have
        // not yet been notified.
        #[cfg(test)]
        run_test_hook(&BEFORE_NOTIFY);
        // The result is already stored. Notify all registered waiters if a
        // wake signal was created; finishing alone never allocates one (HDL-25).
        if let Some(wake) = wake {
            wake.notify_waiters();
        }
    }
}

impl Drop for Core {
    /// Destroy the core after the last Handle or active waiter is gone.
    /// Signal a running port receive to stop (HDL-23); ordinary producers and
    /// started stream items continue without this core. An unstarted receive
    /// is discarded with the core without ever being polled (HDL-24).
    fn drop(&mut self) {
        // Drop has exclusive access through &mut self. self.state is a Mutex;
        // get_mut() returns &mut State, not a guard, without taking the lock.
        let operation_state = self
            .state
            .get_mut()
            .expect("should not be poisoned: nothing panics under the state lock (HDL-9)");
        // A stop signal exists only for a spawned receive that must stop when
        // unobserved. Other states and ordinary producers need no notification.
        if let State::Running {
            stop: Some(stop), ..
        } = operation_state
        {
            // Wake the task waiting for this signal. It stops the receive when
            // it next polls; this does not synchronously cancel the Tokio task.
            stop.notify_waiters();
        }
    }
}

/// Wait for the shared value or error, returning an Arc to it without consuming
/// it. If the producer ended without publishing, return the producer-loss error
/// instead (HDL-10).
///
/// The core must already be Running, Done or Lost. The caller must start any
/// unstarted receive first; this function does not start it (HDL-24).
/// Its Arc keeps the core alive, so a running port receive cannot stop for lack
/// of observers while this future waits (HDL-23).
///
/// Check the result and register for completion under the same lock. Completion
/// then either has already happened or will notify this waiter, even before its
/// first poll of the notification future (HDL-25). Waiting takes no GIL.
async fn wait_terminal(core: Arc<Core>) -> PyResult<Arc<PyResult<Py<PyAny>>>> {
    loop {
        // The temporary guard lasts through this match, then releases the lock
        // before awaiting. Checking and registering together prevents completion
        // from slipping between the check and registration (HDL-25).
        let notified = match &mut *core.lock() {
            // Clone only the Arc, not the Python value or error. The caller
            // reads that value after unlocking, with the GIL if needed (HDL-9).
            State::Done(outcome) => return Ok(outcome.clone()),
            State::Lost => return Err(handle_producer_ended_error()),
            // Reuse the shared wake signal, allocating it only for the first
            // waiter. Each waiter creates its own OwnedNotified under this lock.
            State::Running { wake, .. } => Some(
                wake.get_or_insert_with(|| Arc::new(Notify::new()))
                    .clone()
                    .notified_owned(),
            ),
            // Invalid here: the observation must start the receive before waiting.
            State::Unstarted(_) => None,
        };
        // Tests can pause after registration but before awaiting, then publish
        // a result to check that the notification is not lost.
        #[cfg(test)]
        run_test_hook(&AFTER_REGISTER);
        // No lock is held while waiting. After waking, loop back to read the
        // terminal state: the notification itself carries no value or error.
        notified
            .expect("HDL-24: an observer starts the producer before it waits")
            .await;
    }
}

#[cfg(test)]
thread_local! {
    /// Runs once in `wait_terminal`, after a waiter registers under the lock
    /// and before it awaits (HDL-25).
    static AFTER_REGISTER: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Runs once in `Core::finish`, after the transition and before the notify
    /// (HDL-25).
    static BEFORE_NOTIFY: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn run_test_hook(
    hook: &'static std::thread::LocalKey<std::cell::RefCell<Option<Box<dyn FnOnce()>>>>,
) {
    if let Some(hook) = hook.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
}

/// The sole authority to store one operation's value or error in its shared core.
///
/// The producer holds this alongside its work. Publishing consumes it, so the
/// result can be supplied only once (HDL-1). If dropped before publication,
/// mark any surviving core Lost so observers do not wait forever (HDL-10).
/// Its weak reference never keeps the core or its observers alive (HDL-23).
pub(crate) struct Publisher {
    /// Weak reference to the core awaiting this result. Taken on publication,
    /// leaving None so Drop cannot mark the completed operation Lost.
    /// If the last observer has gone, the weak reference cannot reach a core.
    core: Option<Weak<Core>>,
    /// Optional Python reference to the stack captured when the operation was
    /// created. Retained independently of the core, so a failed result can be
    /// logged with its creation stack after every observer has gone (HDL-13).
    traceback: Option<Py<PyAny>>,
}

impl Publisher {
    /// Retain the core's weak reference and any creation stack for one publication.
    /// This starts no work and does not keep the core alive.
    fn new(core: Weak<Core>, traceback: Option<Py<PyAny>>) -> Self {
        Self {
            core: Some(core),
            traceback,
        }
    }

    /// Store the operation's value or error for every observer, consuming this
    /// publisher so it cannot publish again (HDL-1). Wrap the result in an Arc
    /// so observers share it without consuming it.
    /// This returns unit: observers obtain the result from the core, not from
    /// the return value of this call.
    ///
    /// If no core remains, log an error result; discard a successful result
    /// that nobody can observe (HDL-13).
    pub(crate) fn publish(self, result: PyResult<Py<PyAny>>) {
        // If the core remains, it stores this Arc in State::Done. Observers
        // share the stored result; this call does not return it to the producer.
        self.publish_shared(Arc::new(result));
    }

    /// Publish a result already held in an Arc. An inline get() uses this to
    /// publish and read the same outcome without allocating a second wrapper.
    fn publish_shared(mut self, outcome: Arc<PyResult<Py<PyAny>>>) {
        // Take the weak reference, leaving self.core as None. When this publisher
        // drops, None prevents it from marking the completed operation Lost.
        let core = self.core.take().expect("HDL-1: a Publisher publishes once");
        match core.upgrade() {
            // finish() replaces Running with Done(outcome), releases the state
            // lock, then wakes observers waiting for the stored result.
            // This upgraded Arc keeps the core alive throughout publication.
            Some(core) => core.finish(State::Done(outcome)),
            // All observers have gone. Retain no result for them; log only
            // failures, using the publisher's independently retained stack.
            None => log_unobserved(&outcome, self.traceback.take()),
        }
    }

    /// How many observers hold the core (HDL-23), counted without adding one:
    /// the Handle, its clones and its waiters. Zero once published, or once
    /// every observer has gone.
    pub(crate) fn observer_count(&self) -> usize {
        self.core.as_ref().map_or(0, Weak::strong_count)
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        // HDL-10: a producer that never published surfaces as producer loss.
        if let Some(core) = self.core.take().and_then(|core| core.upgrade()) {
            // Transition Running -> Lost and wake observers to report producer loss.
            core.finish(State::Lost);
        }
    }
}

/// Whether a spawned producer stops when its last observer disappears.
enum ObserverStop {
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
fn spawn_producer<F>(future: F, publisher: Publisher, observer_stop: ObserverStop)
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

/// The Rust implementation of Python's Handle, not a wrapper around another
/// Rust Handle type. Each instance holds a strong reference to the shared core
/// for one native or Python-owned operation.
///
/// Exposed to Python as
/// `monarch._rust_bindings.monarch_hyperactor.handle.Handle`. Observers read
/// the core's shared value or error without consuming it (HDL-3).
///
/// There is no Python constructor. Native bindings return Handles for their
/// operations; internal Python can create a Handle for Python-owned work through
/// `_new_handle_pair()`, which also returns its sole completer (HDL-11).
#[pyclass(
    name = "Handle",
    module = "monarch._rust_bindings.monarch_hyperactor.handle"
)]
pub struct PyHandle {
    /// Strong reference to the operation's progress and shared result. Cloned
    /// Handles and active waiters hold their own Arc to this same core, so it
    /// survives until the last observer lets it go (HDL-23).
    core: Arc<Core>,
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
fn _new_handle_pair() -> (PyHandle, PyHandleCompleter) {
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

impl PyHandle {
    /// Return another Rust-owned PyHandle sharing the same operation's core.
    ///
    /// Pending mesh serialization needs an observer that outlives the binding
    /// call's borrow of the original Handle. Cloning its Arc keeps the core
    /// alive independently of that original Handle (HDL-23).
    ///
    /// Cloning starts no work and allocates no Python object. A later observation
    /// through the clone can start an unstarted receive, just as observation
    /// through the original Handle can (HDL-24).
    pub(crate) fn clone_ref(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }

    /// Create the sole Publisher and the pending Handle it will complete.
    pub(crate) fn pending() -> (Publisher, Self) {
        let core = Core::new(
            State::Running {
                wake: None,
                stop: None,
            },
            None,
        );
        (Publisher::new(Arc::downgrade(&core), None), Self { core })
    }

    /// Defer taking and unpickling a port message or stream reply until the
    /// Handle is observed. Messages may already be queued; creating this Handle
    /// leaves them there and starts no work.
    ///
    /// Store the receive future in State::Unstarted. The first accepted
    /// observation starts it exactly once (HDL-24): get() polls it on the calling
    /// thread; poll(), asyncio observation and Rust waiters start it on Tokio.
    ///
    /// Port receives use ObserverLoss::Stop: a spawned receive stops when its
    /// last observer disappears (HDL-23). Stream items use ObserverLoss::Continue:
    /// once started, they continue without observers. An unstarted receive is
    /// simply dropped with its core, without ever taking a message.
    ///
    /// The future must unpickle through [`ReceiveOutcome::decode`] so an
    /// interrupted main-thread get() retains the received message for retry
    /// rather than losing it (HDL-26).
    pub(crate) fn deferred<F>(
        py: Python<'_>,
        future: F,
        traceback: Option<Py<PyAny>>,
        observer_loss: ObserverLoss,
    ) -> Self
    where
        F: Future<Output = ReceiveOutcome> + Send + 'static,
    {
        let deferred = DeferredReceive {
            future: Box::pin(future),
            traceback: traceback.as_ref().map(|traceback| traceback.clone_ref(py)),
            observer_loss,
        };
        Self {
            core: Core::new(State::Unstarted(deferred), traceback),
        }
    }

    /// Create a Handle for a Python value that Rust already has.
    ///
    /// Every observer can read the value immediately. No producer task or
    /// waiter notification is created (HDL-15).
    ///
    /// This is a Rust constructor, not a Python method; Python still cannot
    /// construct Handles directly (HDL-11).
    pub fn from_value(value: Py<PyAny>) -> Self {
        Self {
            core: Core::new(State::Done(Arc::new(Ok(value))), None),
        }
    }

    /// Return a Rust future that waits for this Handle's value or error without
    /// consuming the result (HDL-14).
    ///
    /// If a port or stream-reply receive is unstarted, this call starts it on
    /// Tokio before returning, even if the returned future is never polled
    /// (HDL-24). Already-running or completed work is left unchanged.
    ///
    /// The future owns a reference to the shared core, not a borrow of `self`.
    /// It can outlive this PyHandle and keeps a port receive observed while it
    /// exists (HDL-23). Other observers can still read the same result (HDL-3).
    pub fn wait_future(
        &self,
    ) -> impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static + use<> {
        // Start any deferred receive now, rather than on this future's first poll.
        self.core.start();
        // Move an independent observer into the future; retain no borrow of self.
        let core = self.core.clone();
        async move {
            // Wait for completion before requesting the GIL. Producer loss fails
            // this future here, without trying to read a Python value.
            let outcome = wait_terminal(core).await?;
            // Under the GIL, read() returns Ok with another reference to the same
            // Python value, or Err with a clone of the stored Python exception.
            // It does not unpickle again or consume the stored outcome; other
            // observers can still read it.
            monarch_with_gil(GilSite::Convert, move |py| read(py, &outcome)).await
        }
    }

    /// Read the current result without waiting or starting a deferred receive.
    /// The asyncio wake callback uses this on the loop thread to settle its
    /// Python future after notification (HDL-17).
    ///
    /// Hold the GIL. The stored result remains available to other observers.
    /// This is an internal Rust method, not exposed to Python.
    pub(crate) fn poll_ready(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        // snapshot() releases the state lock before read_observed() touches Python.
        // read_observed() returns Ok(Some(reference to the stored Python value)),
        // Err(a cloned stored exception), or Ok(None) if no result is ready.
        // Producer loss returns Err rather than remaining pending. No result is
        // unpickled again or consumed.
        read_observed(py, self.core.snapshot())
    }

    /// Start Rust work on Tokio and return a Handle for its Python result (HDL-13).
    ///
    /// Use this when the future returns a Rust value convertible to Python.
    /// After it succeeds, convert the value under the GIL. If the work or
    /// conversion fails, the Handle stores that exception instead.
    /// [`Self::spawn_pyobject`] owns the task and publishes the result; this
    /// wrapper adds only the conversion (HDL-1/HDL-2).
    ///
    /// Calling this starts the task without waiting for anyone to observe the
    /// Handle. Dropping the Handle does not cancel the work (HDL-4).
    /// This is a Rust constructor, not a Python method (HDL-11).
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

    /// Start Rust work on Tokio and return a Handle for its existing Python
    /// value or exception (HDL-13).
    ///
    /// Use this when the future already returns `Py<PyAny>`, rather than a Rust
    /// value that [`Self::spawn`] must convert. Store the result directly, with
    /// no conversion or GIL acquisition for that conversion. The future may
    /// still need the GIL to do its work; logging an unobserved failure does too.
    ///
    /// Calling this starts one task on the shared Tokio runtime. Dropping the
    /// Handle does not cancel it (HDL-4). No creation traceback is captured.
    /// This is a Rust constructor, not a Python method (HDL-11).
    pub fn spawn_pyobject<F>(fut: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self::spawn_pyobject_with_traceback(fut, None)
    }

    /// Start Rust work as in [`Self::spawn_pyobject`], retaining a caller-supplied
    /// creation traceback for logging failures that nobody observes (HDL-13).
    ///
    /// This does not capture a stack itself; `None` means none was supplied.
    /// The publisher keeps its own reference to the traceback, so it can report
    /// where the operation was created even after every Handle and waiter is gone.
    /// Dropping the Handle does not cancel the work (HDL-4).
    /// This is a Rust constructor, not a Python method (HDL-11).
    pub fn spawn_pyobject_with_traceback<F>(fut: F, traceback: Option<Py<PyAny>>) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        // A producer will supply the result. Allocate no waiter notification yet,
        // and install no observer-loss stop signal for this ordinary producer.
        let core = Core::new(
            State::Running {
                wake: None,
                stop: None,
            },
            traceback,
        );
        // The task publishes through a weak reference; it does not keep the core
        // alive as an observer. Its separate traceback reference survives the core.
        spawn_producer(
            fut,
            Publisher::new(Arc::downgrade(&core), core.producer_traceback()),
            // Keep running if every observer disappears (HDL-4).
            ObserverStop::Never,
        );
        // Return an observer that owns the core, not the task running the work.
        Self { core }
    }

    /// Whether this Handle kept a creation traceback. For tests.
    #[cfg(test)]
    pub(crate) fn has_traceback_for_test(&self) -> bool {
        self.core.traceback.is_some()
    }

    /// How many observers hold this Handle's core: Handles, clones and waiters
    /// (HDL-23). For tests.
    #[cfg(test)]
    pub(crate) fn observer_count_for_test(&self) -> usize {
        Arc::strong_count(&self.core)
    }

    /// Return a Rust future that waits for success or failure without reading
    /// the Python value. Use this when a caller needs readiness, not the result.
    ///
    /// Calling this starts any unstarted port or stream-reply receive on Tokio,
    /// even before the returned future is polled (HDL-24). The future retains
    /// the core independently of this Handle. While waiting for completion, it
    /// counts as an observer even if nobody is polling it (HDL-23).
    ///
    /// Success returns `Ok(())` without acquiring the GIL or touching the Python
    /// value. A stored failure returns a cloned Python exception, under the GIL
    /// and outside the state lock (HDL-9). Neither consumes the shared result;
    /// other observers can still read the value or error (HDL-3).
    pub fn wait_completion(&self) -> impl Future<Output = PyResult<()>> + Send + 'static + use<> {
        // Start any deferred receive now; this observer will not run it inline.
        self.core.start();
        // The returned future retains the core without borrowing this Handle.
        let core = self.core.clone();
        async move {
            // Wait for the shared outcome. Producer loss returns an error here.
            let outcome = wait_terminal(core).await?;
            // Check only whether the Rust Result is Ok. The Python value is
            // neither borrowed nor cloned, so success needs no GIL acquisition.
            if outcome.is_ok() {
                return Ok(());
            }
            // wait_terminal() has released the mutex protecting the core's shared
            // result (Core.state). Clone the exception under the GIL, outside that
            // mutex: error cloning can release and reacquire the GIL (HDL-9).
            monarch_with_gil(GilSite::Convert, move |py| -> PyResult<()> {
                // First borrow the Result inside its Arc, then borrow its error
                // rather than moving it. The shared outcome remains unchanged.
                Err(outcome
                    .as_ref()
                    .as_ref()
                    .expect_err("HDL-2: the error branch, discriminant already checked")
                    .clone_ref(py))
            })
            .await
        }
    }
}

/// Run a port or stream-reply receive claimed by a blocking Handle.get() on
/// the calling thread, without spawning a Tokio task (HDL-24). Release the
/// GIL while waiting; unpickling acquires it as needed.
///
/// Store a completed value or error in the shared core and return it to this
/// caller. If the wait times out or Ctrl-C interrupts it on Python's main
/// thread, continue the same unfinished receive on Tokio and raise in this
/// caller. The wait failed, not the operation; other observations can still
/// obtain its result (HDL-12).
///
/// A signal interrupting unpickling is different: the message has already been
/// taken from the queue. Retry unpickling that retained message on Tokio,
/// rather than receiving another one, and raise the interrupt here (HDL-26).
fn drive_inline(
    py: Python<'_>,
    core: &Arc<Core>,
    mut drive: Drive,
    duration: Option<std::time::Duration>,
) -> PyResult<Py<PyAny>> {
    // Drive owns the receive future and its sole publisher. Borrow the future
    // for this wait, so dropping the timeout or signal-check wrapper does not
    // drop the receive itself. Unfinished work remains in drive for hand_off().
    let future = &mut drive.future;
    // Poll the borrowed receive on this thread, alongside main-thread signal
    // checks, with the GIL released. The helper reacquires the GIL before returning.
    let driven = signal_safe_block_on_inline(py, async move {
        match duration {
            // An elapsed deadline becomes None. A receive or unpickling error
            // remains inside ReceiveOutcome; it is not mistaken for a timeout.
            Some(duration) => tokio::time::timeout(duration, future).await.ok(),
            None => Some(future.await),
        }
    });
    match driven {
        Ok(Some(ReceiveOutcome::Outcome(result))) => {
            // The operation finished, successfully or with an error. Share one
            // outcome between the core's observers and this get() call.
            let outcome = Arc::new(result);
            // publish_shared() consumes the sole Publisher and stores this same
            // Arc in Core.state, changing Running to Done(outcome). It releases
            // that mutex, then wakes every registered waiter to read the result.
            drive.publisher.publish_shared(Arc::clone(&outcome));
            // The blocking helper has reacquired the GIL on this thread. read()
            // returns Ok with another reference to the same Python value, or Err
            // with a clone of the stored exception. It does not unpickle again
            // or consume the shared result, and runs outside Core.state's mutex.
            read(py, &outcome)
        }
        Ok(Some(ReceiveOutcome::Interrupted { interrupt, finish })) => {
            // Handle.get() called drive_inline() to run this receive on its thread.
            // A signal interrupted unpickling after the message left the queue.
            // finish is an owned Rust future (UnpickleFuture) that retains that
            // message and retries unpickling under the GIL; it receives no new one.
            // Run the retry on Tokio with the same Publisher. It stores the final
            // value or error in the core, so a later get() can read that result.
            // Never means finish keeps running even if every Handle and waiter
            // disappears before the retry completes.
            spawn_producer(finish, drive.publisher, ObserverStop::Never);
            // Return the signal-handler exception through drive_inline() to the
            // current Handle.get() caller, not as the Handle's stored result.
            Err(interrupt)
        }
        Ok(None) => {
            // Only this wait timed out. Move the unfinished receive and publisher
            // to Tokio; do not store the timeout as the operation's result.
            core.hand_off(drive);
            Err(PyTimeoutError::new_err(()))
        }
        Err(interrupt) => {
            // Signal checking interrupted the wait, not unpickling. Continue the
            // same unfinished receive on Tokio and raise in this get() only.
            core.hand_off(drive);
            Err(interrupt)
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
fn read_observed(py: Python<'_>, observed: Observed) -> PyResult<Option<Py<PyAny>>> {
    match observed {
        // read() returns Ok with another reference to the same Python value,
        // or Err with a clone of the stored exception. map(Some) wraps only
        // the successful value; an error remains Err.
        Observed::Done(outcome) => read(py, &outcome).map(Some),
        // No producer remains to supply a result. Construct RuntimeError
        // "Handle producer ended without publishing a result" rather than
        // report Pending and leave the observer waiting for work that ended.
        Observed::Lost => Err(handle_producer_ended_error()),
        // Unstarted or still running: report not ready without waiting or
        // taking ownership of the receive. This is Rust None, not Python None.
        Observed::Pending => Ok(None),
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

/// Return a new Handle that reports `result`'s value after all prerequisites
/// succeed. Proc initialization uses this to wait for logging and setup before
/// making the proc mesh available.
///
/// Await the Handles in `ready` in list order. If one fails, observing the
/// returned Handle raises that error without waiting for later prerequisites
/// or `result`. Otherwise, report `result`'s value or error.
///
/// The input operations may already be running: this orders observation, not
/// their start. The new waiting task starts immediately; dropping its Handle
/// does not cancel it or the input operations.
#[pyfunction]
fn _after(ready: Vec<PyRef<'_, PyHandle>>, result: &PyHandle) -> PyHandle {
    // Capture owned observer futures so the task retains no Python borrows.
    // Prerequisites need only succeed; their values are not read.
    let ready: Vec<_> = ready
        .iter()
        .map(|handle| handle.wait_completion())
        .collect();
    // Retain an observer of the value to return after the prerequisites succeed.
    let result = result.wait_future();
    // Run the waits on Tokio. `after_ready` observes `result` only if this
    // prerequisite future completes successfully.
    PyHandle::spawn_pyobject(after_ready(
        async move {
            for completion in ready {
                // An error ends this future here and becomes the new Handle's
                // error, even if `result` has already failed differently.
                completion.await?;
            }
            Ok(())
        },
        // Give `after_ready` the result observer when it reaches that step.
        move || result,
    ))
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
    ///     then a ready value returns at once and a pending one blocks. Under a
    ///     warnings-as-errors filter the warning is an error.
    ///   - sync thread: a ready value returns at once, a pending one blocks
    ///     (Ctrl-C-safe on main).
    ///
    /// `get()` runs an unstarted deferred producer itself, on the calling
    /// thread. If Ctrl-C or the timeout ends the wait first, the producer
    /// continues on Tokio and its result stays observable (HDL-24).
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
        if !running_loop_or_none(py).call0()?.is_none() {
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

        // Context, timeout and warning checks have all succeeded. A rejected
        // get() returns above without starting any receive (HDL-24).
        // Retain the shared core independently of the Python borrow of this Handle.
        let core = slf.core.clone();
        // Release that PyRef borrow, not the core. No Python borrow remains
        // across the GIL release used by the blocking wait below (HDL-9).
        drop(slf);

        // Under Core.state's mutex, either claim an unstarted port/stream receive
        // exactly once, or obtain a snapshot of work already started or finished.
        // CallingThread means this get() will poll a claimed receive itself.
        let observed = match core.observe_or_drive(ReceiveExecution::CallingThread) {
            // No receive was transferred: use the existing operation's snapshot.
            Observation::Observed(observed) => observed,
            // Drive owns the claimed receive future and its sole Publisher.
            // drive_inline() runs it here, publishes its result and returns this
            // get()'s value or error. A timeout or interrupt hands work to Tokio.
            Observation::Drive(drive) => return drive_inline(py, &core, drive, duration),
        };
        let outcome = match observed {
            // Already complete: retain its shared outcome without waiting.
            Observed::Done(outcome) => outcome,
            // The producer ended without supplying a result. Raise RuntimeError
            // rather than wait for work that can no longer complete.
            Observed::Lost => return Err(handle_producer_ended_error()),
            Observed::Pending => {
                // Work is already running elsewhere. This future only waits for
                // its shared result; it neither starts nor polls the producer.
                // Moving core into it keeps this get() counted as an observer.
                let wait = wait_terminal(core);
                // Poll the waiter here with the GIL released, without spawning a
                // waiter task. Timeout or Ctrl-C drops this waiter, not a running
                // producer; no observation error is stored as its result (HDL-12).
                match duration {
                    // Limit this wait. An elapsed deadline becomes TimeoutError;
                    // producer-loss errors from wait_terminal() pass through.
                    Some(duration) => signal_safe_block_on_inline(py, async move {
                        tokio::time::timeout(duration, wait)
                            .await
                            .map_err(|_| PyTimeoutError::new_err(()))?
                    })?,
                    // No deadline; Python's main thread still checks for signals.
                    None => signal_safe_block_on_inline(py, wait)?,
                }?
            }
        };
        // Core.state's mutex is no longer held. This thread has the GIL; the
        // blocking helper reacquired it if we waited. read() returns Ok with
        // another reference to the stored Python value, or Err with a cloned
        // exception, leaving the shared result available to other observers.
        read(py, &outcome)
    }

    /// Non-blocking, non-consuming check for completion.
    ///
    /// Returns `None` while pending, the value on success, or raises the stored
    /// exception. If the producer disappears before completion, raises the
    /// producer-lifecycle `RuntimeError`. A ready value stays observable by
    /// later observers. Starts an unstarted deferred producer on Tokio
    /// (HDL-24).
    pub(crate) fn poll(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        read_observed(py, self.core.observe())
    }

    /// Return a standard `asyncio.Future` that resolves when the handle does.
    ///
    /// Requires a running loop; off a loop this raises the native `RuntimeError`
    /// from `asyncio.get_running_loop()`, before it starts anything (HDL-24). A
    /// Tokio observer of the core waits for completion; it never drives a
    /// Python coroutine. It then wakes the loop through the loop's [`LoopWake`]
    /// channel without the GIL, and the loop thread reads the value and sets
    /// the result (HDL-17). It instead takes the GIL and publishes via
    /// `loop.call_soon_threadsafe(...)` when the loop's `add_reader` raises
    /// `NotImplementedError`, or when the loop cannot be weakly referenced
    /// (HDL-21). Any other failure to install the channel or register the
    /// future is raised.
    ///
    /// A Handle that already holds its outcome returns a future already
    /// completed with it, and starts no observer (HDL-22). For a pending
    /// Handle, the observer checks the state before it waits, so a core that
    /// completes before the observer starts resolves rather than hangs.
    /// The result is set on the loop thread. A cancel can race that: a
    /// cancelled future is left alone and the handle continues. If the loop has
    /// closed by then, the result is never set, and nothing panics.
    ///
    /// Each call on a pending Handle spawns its own observer; cancelling the
    /// returned future does not stop the observer, which exits when the handle
    /// resolves.
    fn as_asyncio<'py>(slf: PyRef<'_, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // HDL-6: off a loop this yields the native RuntimeError, not
        // WouldBlockRuntime. HDL-24: this check runs before anything starts.
        let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
        let fut = event_loop.call_method0("create_future")?;

        // A running loop was confirmed above. observe() may now start an
        // unstarted port/stream receive on Tokio (HDL-24). It releases
        // Core.state's mutex before we read or publish any Python result here.
        match slf.core.observe() {
            Observed::Done(outcome) => {
                // The Handle already has a value or error. Under the GIL, read()
                // returns another reference to that value or a cloned exception,
                // without consuming the core's result.
                match read(py, &outcome) {
                    // false selects asyncio.Future.set_result(value).
                    Ok(value) => complete_asyncio_future(&fut, false, value)?,
                    // true selects set_exception. into_value() gives the helper
                    // the Python exception object represented by this PyErr.
                    Err(err) => complete_asyncio_future(&fut, true, err.into_value(py).into_any())?,
                }
                // Return the already-completed asyncio future. No native waiter
                // task or loop wake-channel registration is needed (HDL-22).
                return Ok(fut);
            }
            Observed::Lost => {
                // The producer ended without publishing. Complete the asyncio
                // future with that RuntimeError; awaiting it raises the error,
                // rather than waiting for a result that will never arrive.
                let err = handle_producer_ended_error();
                complete_asyncio_future(&fut, true, err.into_value(py).into_any())?;
                return Ok(fut);
            }
            // Not ready: continue below to register a loop wakeup and start a
            // native waiter for the result. Any deferred receive was started by
            // observe(), not by this waiter.
            Observed::Pending => {}
        }
        // Retain a native observer of the core. It waits for completion without
        // reading the Python value or acquiring the GIL, and keeps a port receive
        // observed until it finishes (HDL-23).
        let wait = wait_terminal(slf.core.clone());

        // Reuse or install this loop's shared pipe-based wake channel. None means
        // the loop lacks add_reader support or cannot be weakly referenced; other
        // installation failures propagate instead of silently falling back.
        if let Some(channel) = LoopWake::for_loop(py, &event_loop)? {
            // Associate this asyncio future with its Handle in the channel's table.
            // slf.into() gives the entry an owned Python reference to the Handle.
            // The notifier carries only a registration token and native wake state.
            let notifier = LoopWake::register(&channel, &fut, slf.into())?;
            get_tokio_runtime().spawn(async move {
                // Wait only for readiness, then discard this task's copy of the
                // outcome. The loop callback reads the registered Handle's stored
                // value or error; this task never acquires the GIL (HDL-17).
                drop(wait.await);
                // Queue the registration token, then make the pipe readable.
                // The loop thread handles that token and completes fut; no Python
                // value or exception is passed through the pipe.
                notifier.notify();
            });
            // Return now; the loop callback will complete the asyncio future.
            return Ok(fut);
        }

        // HDL-21: without a pipe channel, the Tokio waiter must acquire the GIL
        // and use call_soon_threadsafe to queue completion on the loop thread.
        // Retain owned references to the same loop and future, not borrowed Bounds,
        // so the task can use them after this binding call returns.
        let loop_handle = event_loop.clone().unbind();
        let fut_handle = fut.clone().unbind();

        get_tokio_runtime().spawn(async move {
            // Finish the GIL-free wait first. outcome is either the shared value/
            // error or the RuntimeError for a producer that ended without a result.
            let outcome = wait.await;
            let scheduled = monarch_with_gil(GilSite::Convert, move |py| {
                // wait_terminal() has released Core.state's mutex. Under the GIL,
                // read() returns a reference to the stored value or a cloned error.
                // and_then leaves a producer-loss error unchanged (HDL-9/HDL-10).
                let result = outcome.and_then(|outcome| read(py, &outcome));
                // Queue complete_asyncio_future with this value or exception via
                // loop.call_soon_threadsafe. This schedules the setter; it does
                // not change the asyncio future from the Tokio worker thread.
                schedule_completion(py, loop_handle.bind(py), fut_handle.bind(py), result)
            })
            .await;
            if let Err(err) = scheduled {
                // Scheduling can fail, for example if the loop closed meanwhile.
                // Report that failure rather than panic in the observer task.
                tracing::warn!("as_asyncio: failed to schedule completion on the loop: {err}");
            }
        });

        // The caller observes this future; completion happens later on its loop.
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
    handle_mod.add_function(wrap_pyfunction!(_after, handle_mod)?)?;
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use pyo3::IntoPyObjectExt;
    use pyo3::exceptions::PyKeyboardInterrupt;
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::exceptions::PyValueError;
    use pyo3::types::PyList;
    use pyo3::types::PyModule;
    use pyo3::types::PyTuple;

    use super::*;
    use crate::runtime::ensure_python;

    // Build a still-pending `Handle` and return its publisher so the test
    // drives completion explicitly.
    fn pending_handle() -> (Publisher, PyHandle) {
        PyHandle::pending()
    }

    fn assert_producer_ended_error(py: Python<'_>, error: &PyErr) {
        assert!(
            error.is_instance_of::<PyRuntimeError>(py),
            "a dropped producer should surface a RuntimeError, got {error}"
        );
        assert_eq!(
            error.to_string(),
            "RuntimeError: Handle producer ended without publishing a result"
        );
    }

    // HDL-23: losing the last observer stops a started stop-when-unobserved
    // producer. The drop acknowledgement proves destruction, not merely that a
    // stop was requested.
    #[tokio::test]
    async fn stop_when_unobserved_ends_when_last_observer_drops() {
        ensure_python();
        struct AcknowledgeDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for AcknowledgeDrop {
            fn drop(&mut self) {
                let _ = self.0.take().expect("only Drop takes the sender").send(());
            }
        }
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let guard = AcknowledgeDrop(Some(dropped_tx));
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let handle = PyHandle::deferred(
                py,
                async move {
                    let _guard = guard;
                    started_tx
                        .send(())
                        .expect("the test waits for the first poll");
                    std::future::pending::<ReceiveOutcome>().await
                },
                None,
                ObserverLoss::Stop,
            );
            // HDL-24: an observation starts it.
            assert!(handle.poll(py).unwrap().is_none());
            handle
        });
        tokio::time::timeout(Duration::from_secs(30), started_rx)
            .await
            .expect("the started producer should be polled")
            .expect("the producer should acknowledge its first poll");
        drop(handle);
        tokio::time::timeout(Duration::from_secs(30), dropped_rx)
            .await
            .expect("loss of the last observer should destroy the producer")
            .expect("the producer's Drop should acknowledge destruction");
    }

    // HDL-23: a native waiter keeps a stop-when-unobserved producer running
    // after its Handle drops. Stopping when the Handle drops would lose this
    // waiter's result.
    #[tokio::test]
    async fn stop_when_unobserved_keeps_running_for_a_waiting_observer() {
        ensure_python();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            PyHandle::deferred(
                py,
                async move { ReceiveOutcome::Outcome(rx.await.expect("the test supplies the value")) },
                None,
                ObserverLoss::Stop,
            )
        });
        let waiter = handle.wait_future();
        drop(handle);
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 37.into_py_any(py))
            .expect("the value should convert");
        tx.send(Ok(value))
            .expect("the producer should still be waiting");
        let ready = tokio::time::timeout(Duration::from_secs(30), waiter)
            .await
            .expect("the native waiter should complete")
            .expect("the producer should publish its value");
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_eq!(ready.extract::<i64>(py).unwrap(), 37);
        });
    }

    // HDL-4, HDL-24: a deferred producer that nobody observes is dropped with
    // its last Handle, never polled.
    #[tokio::test]
    async fn unobserved_deferred_producer_is_dropped_unpolled() {
        ensure_python();
        struct AcknowledgeDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for AcknowledgeDrop {
            fn drop(&mut self) {
                let _ = self.0.take().expect("only Drop takes the sender").send(());
            }
        }
        let polled = Arc::new(AtomicBool::new(false));
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let guard = AcknowledgeDrop(Some(dropped_tx));
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let polled = polled.clone();
            PyHandle::deferred(
                py,
                async move {
                    let _guard = guard;
                    polled.store(true, Ordering::SeqCst);
                    std::future::pending::<ReceiveOutcome>().await
                },
                None,
                ObserverLoss::Stop,
            )
        });
        drop(handle);
        tokio::time::timeout(Duration::from_secs(30), dropped_rx)
            .await
            .expect("the unobserved producer should be dropped with its Handle")
            .expect("the producer's Drop should acknowledge destruction");
        assert!(
            !polled.load(Ordering::SeqCst),
            "an unobserved deferred producer must never be polled"
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
            let handle = PyHandle::from_value(value);
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
            tx.publish(Ok(value));
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
        let (tx, handle) = pending_handle();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(
                handle.poll(py).unwrap().is_none(),
                "poll() should be None while pending"
            );
            tx.publish(Ok(1i64.into_py_any(py).unwrap()));
            let got = handle.poll(py).unwrap().unwrap();
            assert_eq!(
                got.extract::<i64>(py).unwrap(),
                1,
                "poll() should observe the produced value"
            );
        });
    }

    // poll() is non-consuming: after it returns ready, get() and poll() still
    // observe the same value.
    // Attests HDL-1, HDL-3.
    #[test]
    fn repeated_observation_after_poll() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = 5i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(value);
            let r = Py::new(py, handle).unwrap();

            let first = r.borrow(py).poll(py).unwrap().unwrap();
            assert_eq!(first.extract::<i64>(py).unwrap(), 5);

            let via_get = PyHandle::get(r.borrow(py), py, None).unwrap();
            assert_eq!(via_get.extract::<i64>(py).unwrap(), 5);

            let again = r.borrow(py).poll(py).unwrap().unwrap();
            assert_eq!(again.extract::<i64>(py).unwrap(), 5);
        });
    }

    // Two independent observers of one still-pending core both resolve;
    // waiting never consumes the outcome.
    // Attests HDL-1, HDL-3.
    #[test]
    fn multiple_observers_resolve() {
        ensure_python();
        let (tx, handle) = pending_handle();
        let f1 = handle.wait_future();
        let f2 = handle.wait_future();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 3i64.into_py_any(py).unwrap());
        tx.publish(Ok(value));
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
                let handle = PyHandle::from_value(value);
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
    // contract is tested directly through its Rust binding.
    // Attests HDL-6, HDL-7.
    #[test]
    fn await_in_tokio_raises_native_and_leaves_handle_observable() {
        ensure_python();
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = 11i64.into_py_any(py).unwrap();
            Py::new(py, PyHandle::from_value(value)).unwrap()
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
            monarch_with_gil_blocking(GilSite::Test, |py| r.borrow(py).poll(py).unwrap().is_none());
        assert!(
            still_pending,
            "a timed-out get() must not resolve the handle"
        );

        let value = monarch_with_gil_blocking(GilSite::Test, |py| 4i64.into_py_any(py).unwrap());
        tx.publish(Ok(value));
        let observed = monarch_with_gil_blocking(GilSite::Test, |py| {
            r.borrow(py)
                .poll(py)
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
            let handle = PyHandle::from_value(py.None());
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
            let handle = PyHandle::from_value(py.None());
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
            let handle = PyHandle::from_value(value);
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
            // The Handle is one observer and the pending observer adds a second,
            // so the error is sent only once the observer is on the pending path.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.observer_count() < 2 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            waited_in_producer.store(tx.observer_count() >= 2, Ordering::SeqCst);
            tx.publish(Err(PyValueError::new_err("boom")));
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
            tx.publish(Ok(value));
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
                let handle = PyHandle::from_value(value);
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
        let (tx, handle) = pending_handle();
        tx.publish(Err(err));
        handle
    }

    // A ready value comes back in an already-done observer, and no wake channel
    // is installed.
    // Attests HDL-22.
    #[test]
    fn as_asyncio_ready_value_is_done_without_wake() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let handle = PyHandle::from_value(42i64.into_py_any(py).unwrap());
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

    // A producer that publishes and is then gone: the observer gets what was
    // published, not producer loss.
    // Attests HDL-1, HDL-10, HDL-22.
    #[test]
    fn as_asyncio_reads_an_outcome_published_by_a_departed_producer() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let outcomes = [
                Ok(42i64.into_py_any(py).unwrap()),
                Err(PyValueError::new_err("boom")),
            ];
            for outcome in outcomes {
                let expect_value = outcome.is_ok();
                let (tx, handle) = pending_handle();
                tx.publish(outcome);
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
            let handle = PyHandle::from_value(1i64.into_py_any(py).unwrap());
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
            // Publish only once both observers wait: the Handle is one
            // observer and each pending `as_asyncio()` adds another. A value
            // published first would leave the first observer already done, and
            // its cancel refused (HDL-22).
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.observer_count() < 3 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            both_waited_in_producer.store(tx.observer_count() >= 3, Ordering::SeqCst);
            tx.publish(Ok(value));
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
            // The Handle is one observer and each pending `as_asyncio()` adds
            // another.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.observer_count() < 3 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            both_waited_in_producer.store(tx.observer_count() >= 3, Ordering::SeqCst);
            tx.publish(Ok(value));
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
            let handle = PyHandle::from_value(v);
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
        tx.publish(Err(PyValueError::new_err("boom")));
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let r = Py::new(py, handle).unwrap();
            let poll_err = r.borrow(py).poll(py).unwrap_err();
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
            let poll_err = r.borrow(py).poll(py).unwrap_err();
            assert_producer_ended_error(py, &poll_err);
            let err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
            assert_producer_ended_error(py, &err);
        });
    }

    // HDL-10: losing the producer after get() enters its pending branch raises
    // the producer-lifecycle error, not TimeoutError.
    #[test]
    fn get_pending_surfaces_dropped_producer() {
        ensure_python();
        let (producer, result) = monarch_with_gil_blocking(GilSite::Test, |py| {
            let (tx, handle) = pending_handle();
            let handle = Py::new(py, handle).unwrap();
            assert!(handle.borrow(py).poll(py).unwrap().is_none());
            // Load asyncio before starting the producer: importing it can
            // release the GIL, but the ready check must run while we hold it.
            assert!(pyo3_async_runtimes::get_running_loop(py).is_err());

            // This thread cannot drop the publisher until get() releases the GIL.
            // The producer therefore stays pending through get()'s ready check,
            // forcing the pending branch without a scheduling sleep.
            let producer = thread::spawn(move || {
                monarch_with_gil_blocking(GilSite::Test, |_py| drop(tx));
            });
            let result = PyHandle::get(handle.borrow(py), py, Some(60.0));
            (producer, result)
        });
        producer.join().expect("producer thread should finish");
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let error = result.expect_err("get() should report the dropped producer");
            assert_producer_ended_error(py, &error);
        });
    }

    // A producer that publishes and is then gone: poll() and get() return what
    // was published, not producer loss.
    // Attests HDL-1, HDL-2, HDL-10.
    #[test]
    fn poll_and_get_read_an_outcome_published_by_a_departed_producer() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let publish_then_close = |outcome: PyResult<Py<PyAny>>| {
                let (tx, handle) = pending_handle();
                tx.publish(outcome);
                Py::new(py, handle).unwrap()
            };

            let h = publish_then_close(Ok(42i64.into_py_any(py).unwrap()));
            let polled = h.borrow(py).poll(py).unwrap().expect("the published value");
            assert_eq!(polled.extract::<i64>(py).unwrap(), 42);

            let h = publish_then_close(Err(PyValueError::new_err("boom")));
            let err = h.borrow(py).poll(py).unwrap_err();
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
            .block_on(handle.wait_future())
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
            tx.publish(Ok(value));
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
            let handle = PyHandle::from_value(v);
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
            let handle = PyHandle::from_value(value);
            let r = Py::new(py, handle).unwrap();
            let err = PyHandle::get(r.borrow(py), py, Some(f64::NAN)).unwrap_err();
            assert!(
                err.is_instance_of::<PyValueError>(py),
                "an invalid timeout must raise ValueError even for a ready handle"
            );
        });
    }

    // A producer that drops its publisher without publishing, once the observer
    // is waiting, surfaces through as_asyncio/await (producer loss ->
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
            // The Handle is one observer and the pending observer adds a second,
            // so the producer drops only once the observer is on the pending path.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while tx.observer_count() < 2 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            gate.store(tx.observer_count() >= 2, Ordering::SeqCst);
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
                PyHandle::poll(&handle.borrow(py), py).unwrap().is_none(),
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
                PyHandle::poll(&handle.borrow(py), py).unwrap().is_none(),
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
    // resolves the converted value.
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
        let out = get_tokio_runtime().block_on(handle.wait_future());
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
        tx.publish(Ok(value));
        let (r1, r2) = get_tokio_runtime().block_on(async move { tokio::join!(ok1, ok2) });
        assert!(
            r1.is_ok() && r2.is_ok(),
            "both completion observers should project success to Ok(())"
        );

        // Error: two observers of one failed handle both surface the stored error.
        let (etx, ehandle) = pending_handle();
        let err1 = ehandle.wait_completion();
        let err2 = ehandle.wait_completion();
        etx.publish(Err(PyValueError::new_err("boom")));
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

    // A ready read and an inline drive create no wake; waiters share one.
    // Attests HDL-25.
    #[test]
    fn wake_exists_only_while_an_observer_waits() {
        ensure_python();
        let ready = monarch_with_gil_blocking(GilSite::Test, |py| PyHandle::from_value(py.None()));
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(
                ready.poll(py).unwrap().is_some(),
                "the ready Handle reads at once"
            );
        });
        assert!(matches!(*ready.core.lock(), State::Done(_)));

        let saw_wake = Arc::new(AtomicBool::new(true));
        let slot: Arc<std::sync::Mutex<Option<Weak<Core>>>> = Arc::default();
        let driven = monarch_with_gil_blocking(GilSite::Test, |py| {
            let (saw_wake, producer_slot) = (saw_wake.clone(), slot.clone());
            let handle = PyHandle::deferred(
                py,
                async move {
                    let core = producer_slot
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .upgrade()
                        .unwrap();
                    let has_wake = matches!(*core.lock(), State::Running { wake: Some(_), .. });
                    saw_wake.store(has_wake, Ordering::SeqCst);
                    drop(core);
                    ReceiveOutcome::Outcome(
                        monarch_with_gil(GilSite::Test, |py| 1i64.into_py_any(py)).await,
                    )
                },
                None,
                ObserverLoss::Continue,
            );
            *slot.lock().unwrap() = Some(Arc::downgrade(&handle.core));
            Py::new(py, handle).unwrap()
        });
        let reader = monarch_with_gil_blocking(GilSite::Test, |py| driven.clone_ref(py));
        thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                PyHandle::get(reader.borrow(py), py, None).unwrap();
            });
        })
        .join()
        .unwrap();
        assert!(
            !saw_wake.load(Ordering::SeqCst),
            "an inline drive with no waiter should create no wake"
        );

        let (tx, handle) = pending_handle();
        let waiters: Vec<_> = (0..2)
            .map(|_| get_tokio_runtime().spawn(handle.wait_completion()))
            .collect();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let registered = match &*handle.core.lock() {
                State::Running {
                    wake: Some(wake), ..
                } => Arc::strong_count(wake) - 1,
                _ => 0,
            };
            if registered == 2 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "both waiters should register one shared wake"
            );
            thread::sleep(Duration::from_millis(1));
        }
        tx.publish(Ok(monarch_with_gil_blocking(GilSite::Test, |py| py.None())));
        get_tokio_runtime().block_on(async {
            for waiter in waiters {
                waiter.await.unwrap().unwrap();
            }
        });
    }

    // A publish between a waiter's registration and its first poll still wakes
    // it: its `OwnedNotified` existed before the notify.
    // Attests HDL-25.
    #[test]
    fn publish_between_registration_and_await_wakes_the_waiter() {
        ensure_python();
        let (tx, handle) = pending_handle();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| 9i64.into_py_any(py).unwrap());
        AFTER_REGISTER.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || tx.publish(Ok(value))));
        });
        let outcome = get_tokio_runtime()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(5), wait_terminal(handle.core.clone()))
                    .await
            })
            .expect("the registered waiter should wake")
            .expect("the producer published a value");
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = outcome.as_ref().as_ref().unwrap();
            assert_eq!(value.extract::<i64>(py).unwrap(), 9);
        });
    }

    // Between the publisher's transition and its notify, a new observer sees
    // the outcome without waiting.
    // Attests HDL-25.
    #[test]
    fn observer_between_transition_and_notify_sees_the_outcome() {
        ensure_python();
        let (tx, handle) = pending_handle();
        let core = handle.core.clone();
        let seen = Arc::new(AtomicBool::new(false));
        let seen_in_hook = seen.clone();
        BEFORE_NOTIFY.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let done = matches!(core.snapshot(), Observed::Done(_));
                seen_in_hook.store(done, Ordering::SeqCst);
            }));
        });
        tx.publish(Ok(monarch_with_gil_blocking(GilSite::Test, |py| py.None())));
        assert!(
            seen.load(Ordering::SeqCst),
            "a new observer should see the outcome before the notify"
        );
        drop(handle);
    }

    // Many waiters racing one publisher all wake.
    // Attests HDL-25.
    #[test]
    fn many_waiters_racing_a_publisher_all_wake() {
        ensure_python();
        for _ in 0..200 {
            let (tx, handle) = pending_handle();
            let waiters: Vec<_> = (0..8)
                .map(|_| get_tokio_runtime().spawn(handle.wait_completion()))
                .collect();
            let value = monarch_with_gil_blocking(GilSite::Test, |py| py.None());
            get_tokio_runtime().spawn(async move {
                tokio::task::yield_now().await;
                tx.publish(Ok(value));
            });
            get_tokio_runtime().block_on(async {
                for waiter in waiters {
                    tokio::time::timeout(Duration::from_secs(5), waiter)
                        .await
                        .expect("every waiter should wake")
                        .expect("the waiter task should not panic")
                        .expect("the outcome is a value");
                }
            });
        }
    }

    // A clone observed first starts a deferred producer, once, and the
    // original reads the same object.
    // Attests HDL-23, HDL-24.
    #[test]
    fn clone_observed_first_starts_the_producer_once() {
        ensure_python();
        let polls = Arc::new(AtomicUsize::new(0));
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let polls = polls.clone();
            let value = PyList::empty(py).into_any().unbind();
            PyHandle::deferred(
                py,
                async move {
                    polls.fetch_add(1, Ordering::SeqCst);
                    ReceiveOutcome::Outcome(Ok(value))
                },
                None,
                ObserverLoss::Continue,
            )
        });
        let clone = handle.clone_ref();
        let from_clone = get_tokio_runtime().block_on(clone.wait_future()).unwrap();
        let from_original = get_tokio_runtime().block_on(handle.wait_future()).unwrap();
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "the producer should start once"
        );
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(from_clone.bind(py).is(from_original.bind(py)));
        });
    }

    // Each rejected observation leaves a deferred producer unstarted; a later
    // valid observation starts it once.
    // Attests HDL-6, HDL-8, HDL-12, HDL-24.
    #[test]
    fn rejected_observations_start_nothing() {
        ensure_python();
        let polls = Arc::new(AtomicUsize::new(0));
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let polls = polls.clone();
            let deferred = PyHandle::deferred(
                py,
                async move {
                    polls.fetch_add(1, Ordering::SeqCst);
                    ReceiveOutcome::Outcome(
                        monarch_with_gil(GilSite::Test, |py| 5i64.into_py_any(py)).await,
                    )
                },
                None,
                ObserverLoss::Continue,
            );
            Py::new(py, deferred).unwrap()
        });
        let in_tokio = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
        get_tokio_runtime().block_on(async move {
            monarch_with_gil(GilSite::Test, move |py| {
                let err = PyHandle::get(in_tokio.borrow(py), py, None).unwrap_err();
                assert!(err.is_instance_of::<WouldBlockRuntime>(py));
            })
            .await
        });
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let err = PyHandle::get(handle.borrow(py), py, Some(-1.0)).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
            let err = PyHandle::as_asyncio(handle.borrow(py), py).unwrap_err();
            assert!(err.is_instance_of::<PyRuntimeError>(py));
            let strict = loop_helper(py).getattr("run_get_on_loop_strict").unwrap();
            assert!(
                strict.call1((handle.clone_ref(py),)).is_err(),
                "a warning raised as an error rejects the observation"
            );
        });
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "no rejected observation may start the producer"
        );
        let wait = monarch_with_gil_blocking(GilSite::Test, |py| handle.borrow(py).wait_future());
        let value = get_tokio_runtime().block_on(wait).unwrap();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_eq!(value.extract::<i64>(py).unwrap(), 5);
        });
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "a valid observation starts it once"
        );
    }

    // During an inline drive, a second get() on another thread waits for the
    // shared result instead of failing.
    // Attests HDL-3, HDL-24.
    #[test]
    fn second_reader_waits_for_an_inline_drive() {
        ensure_python();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let driver: Arc<std::sync::Mutex<Option<thread::ThreadId>>> = Arc::default();
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let driver = driver.clone();
            let value = PyList::empty(py).into_any().unbind();
            let deferred = PyHandle::deferred(
                py,
                async move {
                    *driver.lock().unwrap() = Some(thread::current().id());
                    gate_rx.await.expect("the test opens the gate");
                    ReceiveOutcome::Outcome(Ok(value))
                },
                None,
                ObserverLoss::Continue,
            );
            Py::new(py, deferred).unwrap()
        });
        let first_handle = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
        let first = thread::spawn(move || {
            let value = monarch_with_gil_blocking(GilSite::Test, |py| {
                PyHandle::get(first_handle.borrow(py), py, None).unwrap()
            });
            (thread::current().id(), value)
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while driver.lock().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "the first get() should start driving"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let second_handle = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
        let second = thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                PyHandle::get(second_handle.borrow(py), py, Some(10.0)).unwrap()
            })
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while monarch_with_gil_blocking(GilSite::Test, |py| {
            handle.borrow(py).observer_count_for_test()
        }) < 3
        {
            assert!(Instant::now() < deadline, "the second get() should wait");
            thread::sleep(Duration::from_millis(1));
        }
        gate_tx.send(()).unwrap();
        let (first_thread, first_value) = first.join().unwrap();
        let second_value = second.join().unwrap();
        assert_eq!(
            *driver.lock().unwrap(),
            Some(first_thread),
            "the first get() drives the producer on its own thread"
        );
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(first_value.bind(py).is(second_value.bind(py)));
        });
    }

    // A producer task that panics publishes producer loss.
    // Attests HDL-10.
    #[test]
    fn panicking_producer_surfaces_producer_loss() {
        ensure_python();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| py.None());
        let panics = true;
        let handle = PyHandle::spawn_pyobject(async move {
            if panics {
                panic!("the producer panics on purpose");
            }
            Ok(value)
        });
        let err = get_tokio_runtime()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(5), handle.wait_future()).await
            })
            .expect("the waiter should not hang")
            .unwrap_err();
        monarch_with_gil_blocking(GilSite::Test, |py| assert_producer_ended_error(py, &err));
    }

    // Reading a lazy error normalizes it, which releases and reacquires the
    // GIL. A concurrent observer that holds the GIL and observes the same core
    // must not deadlock.
    // Attests HDL-9.
    #[test]
    fn lazy_error_read_does_not_deadlock_a_concurrent_observer() {
        ensure_python();
        for _ in 0..50 {
            let (tx, handle) = pending_handle();
            tx.publish(Err(PyValueError::new_err("lazy")));
            let (reader_handle, poller_handle) = monarch_with_gil_blocking(GilSite::Test, |py| {
                let handle = Py::new(py, handle).unwrap();
                (handle.clone_ref(py), handle)
            });
            let (done_tx, done_rx) = mpsc::channel();
            let reader_done = done_tx.clone();
            let reader = thread::spawn(move || {
                monarch_with_gil_blocking(GilSite::Test, |py| {
                    let _ = PyHandle::get(reader_handle.borrow(py), py, None);
                });
                reader_done.send(()).unwrap();
            });
            let poller = thread::spawn(move || {
                monarch_with_gil_blocking(GilSite::Test, |py| {
                    for _ in 0..100 {
                        let _ = poller_handle.borrow(py).poll(py);
                    }
                });
                done_tx.send(()).unwrap();
            });
            for _ in 0..2 {
                done_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("neither observer may deadlock");
            }
            reader.join().unwrap();
            poller.join().unwrap();
        }
    }

    // A Python finalizer that observes the Handle while its producer runs
    // neither deadlocks nor panics: no Python runs under the state lock.
    // Attests HDL-9.
    #[test]
    fn finalizer_observing_the_handle_during_a_drive_does_not_deadlock() {
        ensure_python();
        let slot: Arc<std::sync::Mutex<Option<Py<PyAny>>>> = Arc::default();
        let (handle, seen) = monarch_with_gil_blocking(GilSite::Test, |py| {
            let module = PyModule::from_code(
                py,
                cr#"
class PollOnDel:
    def __init__(self, handle, seen):
        self.handle = handle
        self.seen = seen

    def __del__(self):
        self.seen.append(self.handle.poll())
"#,
                c"poll_on_del.py",
                c"poll_on_del",
            )
            .unwrap();
            let in_producer = slot.clone();
            let deferred = PyHandle::deferred(
                py,
                async move {
                    ReceiveOutcome::Outcome(
                        monarch_with_gil(GilSite::Test, move |py| {
                            // Run the finalizer while the producer is still running.
                            drop(in_producer.lock().unwrap().take());
                            3i64.into_py_any(py)
                        })
                        .await,
                    )
                },
                None,
                ObserverLoss::Continue,
            );
            let handle = Py::new(py, deferred).unwrap();
            let seen = PyList::empty(py).unbind();
            let observer = module
                .getattr("PollOnDel")
                .unwrap()
                .call1((handle.clone_ref(py), seen.clone_ref(py)))
                .unwrap()
                .unbind();
            *slot.lock().unwrap() = Some(observer);
            (handle, seen)
        });
        let reader = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let value = monarch_with_gil_blocking(GilSite::Test, |py| {
                let value = PyHandle::get(reader.borrow(py), py, None).unwrap();
                value.extract::<i64>(py).unwrap()
            });
            done_tx.send(value).unwrap();
        });
        let value = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the finalizer must not deadlock the drive");
        assert_eq!(value, 3);
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let seen = seen.bind(py);
            assert_eq!(seen.len(), 1, "the finalizer ran once");
            assert!(
                seen.get_item(0).unwrap().is_none(),
                "the finalizer observed the producer still running"
            );
        });
    }

    // A timed get() that times out while driving hands the producer to Tokio:
    // the producer finishes there and a later get() reads its result.
    // Attests HDL-12, HDL-24.
    #[test]
    fn timed_out_drive_hands_off_and_its_result_survives() {
        ensure_python();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = PyList::empty(py).into_any().unbind();
            let deferred = PyHandle::deferred(
                py,
                async move {
                    gate_rx.await.expect("the test opens the gate");
                    ReceiveOutcome::Outcome(Ok(value))
                },
                None,
                ObserverLoss::Continue,
            );
            Py::new(py, deferred).unwrap()
        });
        let driver = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
        let timed_out = thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                let err = PyHandle::get(driver.borrow(py), py, Some(0.05)).unwrap_err();
                err.is_instance_of::<PyTimeoutError>(py)
            })
        })
        .join()
        .unwrap();
        assert!(timed_out, "the driving get() should time out");
        gate_tx.send(()).unwrap();
        let (first, second) = monarch_with_gil_blocking(GilSite::Test, |py| {
            let first = PyHandle::get(handle.borrow(py), py, Some(5.0)).unwrap();
            let second = PyHandle::get(handle.borrow(py), py, None).unwrap();
            (first, second)
        });
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(first.bind(py).is(second.bind(py)));
        });
    }

    // A stop-when-unobserved producer handed to Tokio by a timed-out get()
    // still stops once its last observer drops.
    // Attests HDL-23, HDL-24.
    #[test]
    fn handed_off_producer_stops_when_unobserved() {
        ensure_python();
        struct AcknowledgeDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for AcknowledgeDrop {
            fn drop(&mut self) {
                let _ = self.0.take().expect("only Drop takes the sender").send(());
            }
        }
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let guard = AcknowledgeDrop(Some(dropped_tx));
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            let deferred = PyHandle::deferred(
                py,
                async move {
                    let _guard = guard;
                    std::future::pending::<ReceiveOutcome>().await
                },
                None,
                ObserverLoss::Stop,
            );
            Py::new(py, deferred).unwrap()
        });
        let driver = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
        thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                PyHandle::get(driver.borrow(py), py, Some(0.05)).unwrap_err();
            })
        })
        .join()
        .unwrap();
        monarch_with_gil_blocking(GilSite::Test, |_py| drop(handle));
        get_tokio_runtime()
            .block_on(async { tokio::time::timeout(Duration::from_secs(30), dropped_rx).await })
            .expect("loss of the last observer should destroy the handed-off producer")
            .expect("the producer's Drop should acknowledge destruction");
    }

    /// A deferred producer whose decoding reports an interrupt, with a
    /// finished decoding that yields `value`.
    fn interrupted_producer(py: Python<'_>, value: Py<PyAny>) -> PyHandle {
        PyHandle::deferred(
            py,
            async move {
                ReceiveOutcome::Interrupted {
                    interrupt: PyKeyboardInterrupt::new_err(()),
                    finish: Box::pin(async move { Ok(value) }),
                }
            },
            None,
            ObserverLoss::Continue,
        )
    }

    // A driving get() whose decoding reports an interrupt raises it, and a
    // later get() reads the result the finished decoding publishes.
    // Attests HDL-26.
    #[test]
    fn interrupted_decode_raises_and_its_finished_result_survives() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = PyList::empty(py).into_any().unbind();
            let handle = Py::new(py, interrupted_producer(py, value)).unwrap();
            let err = PyHandle::get(handle.borrow(py), py, None).unwrap_err();
            assert!(
                err.is_instance_of::<PyKeyboardInterrupt>(py),
                "the driving get() should raise the interrupt"
            );
            let first = PyHandle::get(handle.borrow(py), py, Some(5.0)).unwrap();
            let second = PyHandle::get(handle.borrow(py), py, None).unwrap();
            assert!(first.bind(py).is_instance_of::<PyList>());
            assert!(first.bind(py).is(second.bind(py)));
        });
    }

    // A spawned producer whose decoding reports an interrupt publishes the
    // finished decoding's result.
    // Attests HDL-26.
    #[test]
    fn spawned_producer_finishes_an_interrupted_decode() {
        ensure_python();
        let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
            interrupted_producer(py, 7i64.into_py_any(py).unwrap())
        });
        let value = get_tokio_runtime()
            .block_on(handle.wait_future())
            .expect("the finished decoding should publish its value");
        let value = monarch_with_gil_blocking(GilSite::Test, |py| value.extract::<i64>(py));
        assert_eq!(value.unwrap(), 7);
    }

    // Off Python's main thread no signal handler runs, so even a decoding
    // failure that is not an `Exception` is the outcome, and it decodes once.
    // Attests HDL-26.
    #[test]
    fn decode_off_the_main_thread_takes_every_error_as_the_outcome() {
        ensure_python();
        // Under Python 3.12 the first thread to import `threading` becomes
        // its main thread; settle that before starting the thread below.
        is_main_thread();
        let decodes = Arc::new(AtomicUsize::new(0));
        let counted = decodes.clone();
        let outcome_is_the_error = thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, move |py| {
                let produced = ReceiveOutcome::decode(py, GilSite::Test, (), move |_py, ()| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Err(PyKeyboardInterrupt::new_err(()))
                });
                matches!(
                    produced,
                    ReceiveOutcome::Outcome(Err(err)) if err.is_instance_of::<PyKeyboardInterrupt>(py)
                )
            })
        })
        .join()
        .unwrap();
        assert!(outcome_is_the_error, "the error should be the outcome");
        assert_eq!(
            decodes.load(Ordering::SeqCst),
            1,
            "the decoding should run once"
        );
    }
}
