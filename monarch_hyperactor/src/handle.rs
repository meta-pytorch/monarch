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
//! producer supplies a Python outcome or a successful reply still pickled, and every
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
//! - **HDL-1 (single terminal state).** A core moves from `Unstarted`, `Running`
//!   or `Raw` to `Done` or `Lost` once and never leaves it. Publication consumes
//!   the Publisher; only the owner of a raw reconstruction claim may finish Raw.
//! - **HDL-2 (outcome encoding).** `Done(Ok(_))` is success, `Done(Err(_))` a
//!   producer/reconstruction error, and `Lost` producer loss (HDL-10). Raw bytes
//!   are not a completed Python outcome or successful readiness.
//! - **HDL-3 (non-consuming reads).** No read moves the outcome out. Under the
//!   state lock an observer clones only the outcome's `Arc`; after unlocking it
//!   clones the value or error (`clone_ref`), or, on `wait_completion`'s
//!   success path, reads just the `Ok`/`Err` tag. Any number of observers see
//!   the same result.
//!   Raw replies are reconstructed once before ordinary shared reads (HDL-27).
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
//! - **HDL-7 (`as_asyncio` publish).** The pipe observer waits through
//!   `wait_arrival`, the fallback observer through `wait_terminal`; each
//!   checks the state before it registers a wake (HDL-25). The result is
//!   published on the loop thread through
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
//!   The timeout bounds waiting for arrival or another reconstructor, not
//!   synchronous Python unpickling performed by this observer.
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
//!   `call()`, `call_one()` and `choose()` use [`PyHandle::spawn_raw`], which
//!   retains the supplied creation traceback, publishes successful bytes and
//!   leaves reconstruction to an observer; errors keep the same logging path.
//!   [`PyHandle::wait_completion`] is the matching observer: it projects the
//!   terminal value to `PyResult<()>`, touching neither the GIL nor the Python
//!   value on the decoded success path (a tag-only read, HDL-3) and cloning the error
//!   after unlocking only on the error path (HDL-9). These are Rust-only
//!   methods outside `#[pymethods]` (HDL-11).
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
//!   task holds a `Notifier` (a token, an mpsc sender and an `Arc<Waker>`) and
//!   the `wait_arrival` future. Without the GIL or a claim, it waits until the
//!   outcome is Done or Lost or a raw reply is unclaimed, then pushes the token
//!   and writes the loop's pipe. The loop thread reads the value from the
//!   `Handle`, claiming and rebuilding a raw reply there, and sets it (see
//!   [`crate::handle_wake`]). A token that finds the reply claimed by another
//!   observer re-arms its entry under the same token. Enforced by
//!   construction: `Notifier` has no `Py<_>`, and a token follows a terminal
//!   outcome, raw arrival or claim release.
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
//!   An unclaimed Raw reply is reconstructed on the loop thread first; a
//!   claimed reply follows the pending wake path without blocking that thread.
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
//!   Raw arrival retains the same wake for claim release and settlement; all
//!   three transitions notify only after releasing the state lock.
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
//! - **HDL-27 (one raw reconstruction claim).** One observer claims a successful
//!   raw reply under the state lock, then unpickles outside it. Success or an
//!   ordinary Exception stores one shared outcome. An observer-side BaseException,
//!   unwind or cancelled GIL wait releases the claim, preserves the original
//!   payload and wakes waiters. The asyncio wake reader follows asyncio's
//!   callback dispatch instead: only KeyboardInterrupt and SystemExit release
//!   the claim, and the reader re-arms its entry and re-queues its batch's
//!   undelivered tokens before it raises out of the loop. Every other failure,
//!   including another BaseException or a caught unwind, is the shared outcome.
//!   The fallback worker settles every reconstruction failure before it
//!   schedules completion. Both retain the claim through error settlement; no
//!   competitor can claim between unwind and publication.

mod asyncio;
mod core;
mod outcome;
mod produce;
mod receive;

use std::future::Future;
use std::sync::Arc;

use monarch_types::py_global;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyUserWarning;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;

pub(crate) use self::asyncio::LoopReadOutcome;
pub(crate) use self::asyncio::complete_asyncio_future;
use self::asyncio::complete_ready;
use self::asyncio::register_pending;
use self::core::Core;
use self::core::Observation;
pub(crate) use self::core::Publisher;
use self::core::Running;
use self::core::State;
use self::core::WaitOutcome;
use self::core::wait_observed;
pub(crate) use self::outcome::Reconstruct;
use self::outcome::ReconstructionFailurePolicy;
pub use self::outcome::WouldBlockRuntime;
use self::outcome::clone_shared_outcome;
use self::outcome::handle_producer_ended_error;
use self::outcome::read_observed;
use self::outcome::reconstruct_observed;
pub use self::outcome::to_py_error;
use self::produce::_new_handle_pair;
pub(crate) use self::produce::ObserverLoss;
use self::produce::ObserverStop;
pub(crate) use self::produce::PyHandleCompleter;
pub(crate) use self::produce::handle_pair;
use self::produce::spawn_producer;
use self::receive::DeferredReceive;
pub(crate) use self::receive::ReceiveOutcome;
use self::receive::drive_inline;
#[cfg(doc)]
use crate::handle_wake::LoopWake;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::is_in_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::signal_safe_block_on_inline;

// HDL-8: unlike `get_running_loop`, returns `None` off a loop instead of raising.
py_global!(running_loop_or_none, "asyncio", "_get_running_loop");

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
        let core = Core::new(State::Running(Running::default()), None);
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
    /// An available raw endpoint reply is claimed only when this future is
    /// polled, then unpickled on its Rust driver under the same GIL acquisition
    /// used to read the result. Dropping a queued GIL wait releases that claim.
    pub fn wait_future(
        &self,
    ) -> impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static + use<> {
        // Start any deferred receive now, rather than on this future's first poll.
        self.core.start();
        // Move an independent observer into the future; retain no borrow of self.
        let core = self.core.clone();
        async move {
            // Wait for a stored outcome or claim an available raw reply.
            // Producer loss fails here without acquiring the GIL.
            let observed = wait_observed(core).await;
            if matches!(observed, WaitOutcome::Lost) {
                return Err(handle_producer_ended_error());
            }
            // Reconstruct a claimed reply and read its shared outcome in one
            // GIL acquisition. Already-decoded results only clone the shared
            // Python value or exception without consuming it.
            monarch_with_gil(GilSite::Convert, move |py| {
                let outcome = reconstruct_observed(
                    py,
                    observed,
                    ReconstructionFailurePolicy::KeepReplyAfterInterrupt,
                )?;
                clone_shared_outcome(py, &outcome)
            })
            .await
        }
    }

    /// Inspect arrival without claiming or reconstructing, for endpoint witnesses.
    #[cfg(test)]
    pub(crate) fn received_raw_for_test(&self) -> Option<bool> {
        self.core.received_raw_for_test()
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
        // A producer will supply the result. Allocate no waiter notification yet,
        // and install no observer-loss stop signal for this ordinary producer.
        let core = Core::new(State::Running(Running::default()), None);
        // The task publishes through a weak reference; it does not keep the core
        // alive as an observer. This producer carries no creation traceback.
        spawn_producer(
            fut,
            Publisher::new(Arc::downgrade(&core), None),
            // Keep running if every observer disappears (HDL-4).
            ObserverStop::Never,
        );
        // Return an observer that owns the core, not the task running the work.
        Self { core }
    }

    /// Receive an endpoint reply eagerly on Tokio, retaining success as pickled bytes.
    /// The first valid observer reconstructs it; ignored successes need no
    /// reconstruction or result-conversion GIL acquisition.
    /// Errors are already Python exceptions and retain ordinary unobserved logging.
    pub(crate) fn spawn_raw<F, R>(future: F, traceback: Option<Py<PyAny>>) -> Self
    where
        F: Future<Output = PyResult<R>> + Send + 'static,
        R: Reconstruct + 'static,
    {
        let core = Core::new(State::Running(Running::default()), traceback);
        let publisher = Publisher::new(Arc::downgrade(&core), core.producer_traceback());
        get_tokio_runtime().spawn(async move {
            match future.await {
                Ok(reply) => publisher.publish_raw(Arc::new(reply)),
                Err(error) => publisher.publish(Err(error)),
            }
        });
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
    /// Already-decoded success returns `Ok(())` without acquiring the GIL or
    /// touching the Python value. Raw success must first be reconstructed on
    /// this future's driver: arrival alone is not readiness (HDL-27).
    /// A stored failure returns a cloned Python exception, under the GIL
    /// and outside the state lock (HDL-9). Neither consumes the shared result;
    /// other observers can still read the value or error (HDL-3).
    pub fn wait_completion(&self) -> impl Future<Output = PyResult<()>> + Send + 'static + use<> {
        // Start any deferred receive now; this observer will not run it inline.
        self.core.start();
        // The returned future retains the core without borrowing this Handle.
        let core = self.core.clone();
        async move {
            // Wait for the shared outcome. Producer loss returns an error here.
            let observed = wait_observed(core).await;
            if matches!(observed, WaitOutcome::Lost) {
                return Err(handle_producer_ended_error());
            }
            // Check only whether the Rust Result is Ok. The Python value is
            // neither borrowed nor cloned, so success needs no GIL acquisition.
            if let WaitOutcome::Done(outcome) = &observed
                && outcome.is_ok()
            {
                return Ok(());
            }
            // wait_observed() has released the mutex protecting the core's shared
            // result (Core.state). Clone the exception under the GIL, outside that
            // mutex: error cloning can release and reacquire the GIL (HDL-9).
            monarch_with_gil(GilSite::Convert, move |py| -> PyResult<()> {
                // Reconstruct and inspect under the same acquisition. Raw arrival
                // is not readiness, and a failed reconstruction must fail _after.
                let outcome = reconstruct_observed(
                    py,
                    observed,
                    ReconstructionFailurePolicy::KeepReplyAfterInterrupt,
                )?;
                match outcome.as_ref() {
                    Ok(_) => Ok(()),
                    Err(error) => Err(error.clone_ref(py)),
                }
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
    ///
    /// If successful endpoint bytes are available, this get() claims and
    /// unpickles them on the calling thread. The timeout bounds waiting for
    /// arrival or another observer's claim, not that synchronous unpickling.
    /// An interrupted reconstruction releases the claim for a later attempt;
    /// an ordinary unpickle exception becomes the shared result (HDL-27).
    #[pyo3(signature = (timeout = None))]
    fn get(slf: PyRef<'_, Self>, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        // Reject Tokio calls, validate the timeout, and warn on a running asyncio loop.
        let duration = check_blocking_get(py, timeout)?;

        // Context, timeout and warning checks have all succeeded. A rejected
        // get() returns above without starting any receive (HDL-24).
        // Retain the shared core independently of the Python borrow of this Handle.
        let core = slf.core.clone();
        // Release that PyRef borrow, not the core. No Python borrow remains
        // across the GIL release used by the blocking wait below (HDL-9).
        drop(slf);

        // Under Core.state's mutex, either claim an unstarted port/stream receive
        // exactly once, or obtain a snapshot of work already started or finished.
        // This get() will poll a claimed receive itself.
        let observed = match core.observe_or_drive() {
            // No receive was transferred: use the existing operation's snapshot.
            Observation::Observed(observed) => observed,
            // Drive owns the claimed receive future and its sole Publisher.
            // drive_inline() runs it here, publishes its result and returns this
            // get()'s value or error. A timeout or interrupt hands work to Tokio.
            Observation::Drive(drive) => return drive_inline(py, &core, drive, duration),
        };
        // Use a ready result; otherwise wait here with the GIL released.
        let observed = match observed.into_wait_outcome() {
            None => wait_inline(py, core, duration)?,
            Some(ready) => ready,
        };
        // Core.state's mutex is no longer held. This thread has the GIL; the
        // blocking helper reacquired it if we waited. Clone the stored value or
        // exception, leaving the shared result available to other observers.
        let outcome = reconstruct_observed(
            py,
            observed,
            ReconstructionFailurePolicy::KeepReplyAfterInterrupt,
        )?;
        clone_shared_outcome(py, &outcome)
    }

    /// Non-blocking, non-consuming check for completion.
    ///
    /// Returns `None` while pending, the value on success, or raises the stored
    /// exception. If the producer disappears before completion, raises the
    /// producer-lifecycle `RuntimeError`. A ready value stays observable by
    /// later observers. Starts an unstarted deferred producer on Tokio
    /// (HDL-24).
    ///
    /// An available raw endpoint reply is unpickled here under the caller's
    /// GIL. Return None if another observer holds its reconstruction claim.
    /// This never waits for arrival, but unpickle hooks may run Python code.
    pub(crate) fn poll(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        read_observed(py, self.core.observe())
    }

    /// Return a standard `asyncio.Future` that resolves when the handle does.
    ///
    /// Requires a running loop; off a loop this raises the native `RuntimeError`
    /// from `asyncio.get_running_loop()`, before it starts anything (HDL-24). A
    /// Tokio observer of the core waits for completion, or for a raw reply's
    /// arrival; it never drives a Python coroutine. It then wakes the loop
    /// through the loop's [`LoopWake`] channel without the GIL, and the loop
    /// thread reads the value and sets the result (HDL-17). It instead takes
    /// the GIL and publishes via `loop.call_soon_threadsafe(...)` when the
    /// loop's `add_reader` raises `NotImplementedError`, or when the loop
    /// cannot be weakly referenced (HDL-21). Any other failure to install the
    /// channel or register the future is raised.
    ///
    /// A raw endpoint reply is unpickled on the loop thread: immediately if it
    /// is available, otherwise by the wake reader after it arrives. If another
    /// observer holds its claim, the await settles from that observer's
    /// outcome, or the reader claims the reply after a release. A
    /// KeyboardInterrupt or SystemExit raised while the reader unpickles
    /// propagates out of the loop and leaves the reply for a later turn; every
    /// other failure becomes the shared result (HDL-27). On the fallback, a
    /// Tokio waiter unpickles before scheduling completion, and every failure
    /// there becomes the shared result.
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
    /// resolves or, on the pipe path, when its raw reply arrives. An await
    /// cancelled, or whose loop closes, before the reply arrives does not
    /// unpickle it; another observer can.
    fn as_asyncio<'py>(slf: PyRef<'_, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // HDL-6: off a loop this yields the native RuntimeError, not
        // WouldBlockRuntime. HDL-24: this check runs before anything starts.
        let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
        let fut = event_loop.call_method0("create_future")?;

        // A running loop was confirmed above. observe() may now start an
        // unstarted port/stream receive on Tokio (HDL-24). It releases
        // Core.state's mutex before we read or publish any Python result here.
        match slf.core.observe().into_wait_outcome() {
            // Return the already-completed asyncio future. No native waiter
            // task or loop wake-channel registration is needed (HDL-22).
            Some(ready) => complete_ready(py, &fut, ready)?,
            // Not ready: register a loop wakeup and start a
            // native waiter for the result. Any deferred receive was started by
            // observe(), not by this waiter.
            None => register_pending(py, slf, &event_loop, &fut)?,
        }
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

/// Check that a blocking `get()` may run in this calling context, and parse its
/// timeout (`None` for no limit). The timeout's clock starts only when `get()`
/// begins to wait or to drive a receive. `get()` runs these checks before it
/// touches the Handle's state, so a rejected call starts nothing.
fn check_blocking_get(
    py: Python<'_>,
    timeout: Option<f64>,
) -> PyResult<Option<std::time::Duration>> {
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

    Ok(duration)
}

/// Wait on the calling thread for work already running elsewhere, and return
/// its outcome or this observer's claim on a raw reply.
fn wait_inline(
    py: Python<'_>,
    core: Arc<Core>,
    duration: Option<std::time::Duration>,
) -> PyResult<WaitOutcome> {
    // Work is already running elsewhere. This future only waits for
    // its shared result; it neither starts nor polls the producer.
    // Moving core into it keeps this get() counted as an observer.
    // Return a raw claim to this thread rather than reconstructing
    // while detached. This get() uses the GIL it reacquires afterwards.
    let wait = wait_observed(core);
    // Poll the waiter here with the GIL released, without spawning a
    // waiter task. Timeout or Ctrl-C drops this waiter, not a running
    // producer; no observation error is stored as its result (HDL-12).
    Ok(match duration {
        // Limit this wait. An elapsed deadline becomes TimeoutError;
        // producer loss is returned as an observation.
        Some(duration) => signal_safe_block_on_inline(py, async move {
            tokio::time::timeout(duration, wait)
                .await
                .map_err(|_| PyTimeoutError::new_err(()))
        })??,
        // No deadline; Python's main thread still checks for signals.
        None => signal_safe_block_on_inline(py, wait)?,
    })
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
mod tests;
