/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! GIL-free completion wake for `Handle.as_asyncio` (HDL-17 to HDL-21 in
//! [`crate::handle`]).
//!
//! Each asyncio loop that can watch a file descriptor (`loop.add_reader`,
//! which every selector-based loop, including all of Monarch's, provides) gets
//! one [`LoopWake`]: a queue of `u64` tokens and a non-blocking pipe whose read
//! end the loop watches. A loop without `add_reader` keeps the
//! `call_soon_threadsafe` path (HDL-21).
//!
//! `waker` and `read_fd` are the two ends of the pipe; `tx` and `rx` are the
//! two ends of the token queue. When a `Handle` completes, or its endpoint
//! reply arrives still pickled, its Tokio observer pushes its token and writes
//! one byte to the pipe ([`Notifier::notify`]), without the GIL. The byte only
//! wakes the loop; the queue says which awaits can settle. The loop's reader
//! drains the pipe, then the queue, and settles the future of each await,
//! unpickling a raw reply on the loop thread (HDL-17, HDL-27).
//!
//! The loop's selector is the channel's only strong owner, through its
//! `on_readable` callback. The registry holds the loop as a weak key and the
//! channel through a weak reference. Each entry holds its pending await's
//! future strongly, because that reference is the waiting task's only owner
//! outside the task's own cycle; asyncio holds tasks weakly. Closing the loop
//! therefore releases the channel, its read end, its entries and their
//! futures; the write end lasts until every notifier is done (HDL-20).
//! `__traverse__` lets the garbage collector free an unclosed loop (HDL-19,
//! which also records the case that is not freed):
//!
//! ```text
//! registry:         weak(loop) ──▶ weak(LoopWake)
//! loop's selector:  read_fd    ──▶ LoopWake.on_readable
//!
//! LoopWake
//!   ├─ tx / rx    queue of u64 tokens
//!   ├─ waker      pipe write end, shared with every Notifier
//!   ├─ read_fd    pipe read end, registered with `add_reader`
//!   └─ entries    token → (asyncio future, Handle)
//! ```

use std::collections::HashMap;
use std::convert::Infallible;
use std::iter;
#[cfg(test)]
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
#[cfg(test)]
use std::time::Duration;

use monarch_types::MapPyErr;
use nix::errno::Errno;
#[cfg(test)]
use nix::poll::PollFd;
#[cfg(test)]
use nix::poll::PollFlags;
#[cfg(test)]
use nix::poll::poll;
use nix::unistd::read;
use pyo3::PyTraverseError;
use pyo3::PyVisit;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use pyo3::types::PyWeakrefReference;
use tokio::sync::oneshot;

use super::PyHandle;
use super::asyncio::LoopReadOutcome;
use super::asyncio::complete_asyncio_future;
use crate::pywaker;
use crate::pywaker::Waker;
use crate::pywaker::retry_eintr;

/// The process-wide `weakref.WeakKeyDictionary` from each asyncio loop to its
/// wake state:
///
/// - no entry: no `Handle` has been awaited on the loop yet;
/// - a weak reference to a [`LoopWake`]: the loop's channel, installed by its
///   first await and owned by the loop's selector;
/// - `None`: the loop's `add_reader` raised `NotImplementedError`, so its
///   awaits use the `call_soon_threadsafe` fallback (HDL-21).
///
/// Keys are weak and channels are held weakly, so the registry keeps neither a
/// loop nor anything its channel holds alive (HDL-19). A Rust map keyed by the
/// loop's `id` would outlive the loop, and a later loop could reuse that `id`.
fn registry(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static REGISTRY: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    REGISTRY
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(py
                .import("weakref")?
                .getattr("WeakKeyDictionary")?
                .call0()?
                .unbind())
        })
        .map(|registry| registry.bind(py).clone())
}

/// The lookup default that tells "no entry" apart from a cached `None`.
fn absent(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static ABSENT: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    ABSENT
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(py.import("builtins")?.getattr("object")?.call0()?.unbind())
        })
        .map(|absent| absent.bind(py).clone())
}

/// One-shot hooks run at the reader's two drain boundaries (HDL-18).
#[cfg(test)]
#[derive(Default)]
struct DrainHooks {
    after_pipe_drain: Option<Box<dyn FnOnce(&LoopWake) + Send>>,
    after_queue_drain: Option<Box<dyn FnOnce(&LoopWake) + Send>>,
}

/// A pending await: the `asyncio.Future` it is suspended on, and the `Handle`
/// it is waiting for.
struct Entry {
    /// The awaiting `asyncio.Future`, held strongly until delivery,
    /// cancellation or loop close. A suspended task is referenced only through
    /// the future it awaits, so without this reference a task whose other
    /// references form a cycle would be collected while it waits.
    /// `LoopWake.__traverse__` exposes it to the garbage collector (HDL-19).
    fut: Py<PyAny>,
    /// The awaited `Handle`. Its value is read when the entry's token arrives,
    /// and set as the result of `fut`.
    handle: Py<PyHandle>,
    /// Dropped with the entry, whichever path removes it, which releases this
    /// await's native waiter (HDL-17, HDL-19). Nothing is ever sent on it.
    release: oneshot::Sender<Infallible>,
}

impl Entry {
    /// Return whether the wake reader should keep processing this entry.
    ///
    /// `deliver()` checks before reading the Handle and again before re-inserting
    /// an entry whose reply is claimed or interrupted. The second check matters
    /// because reconstruction can run Python code that completes or cancels the
    /// future.
    ///
    /// A finished future needs no delivery. If `done()` itself raises, report that
    /// error to the loop and drop the entry rather than retain a broken await.
    fn waiting(&self, py: Python<'_>) -> bool {
        let fut = self.fut.bind(py);
        match fut.call_method0("done").and_then(|done| done.is_truthy()) {
            Ok(done) => !done,
            Err(err) => {
                report(fut, "handle wake: done() failed; the await is dropped", err);
                false
            }
        }
    }
}

/// The completion-wake channel of one asyncio loop: a token queue, and a pipe
/// the loop watches.
///
/// [`LoopWake::for_loop`] creates it on the loop's first `Handle` await and
/// registers [`on_readable`](LoopWake::on_readable) with the loop's
/// `add_reader`. From then on the loop's selector, which holds that callback,
/// is its only strong owner, so it lives until the loop is closed or freed.
/// The registry refers to it weakly (HDL-19).
///
/// Two sides use it:
/// - the loop thread, holding the GIL: [`LoopWake::register`] adds an entry
///   for each await, and `on_readable` delivers the completed ones;
/// - Tokio, without the GIL: each [`Notifier`] holds clones of `tx` and
///   `waker`, and nothing else.
///
/// The `pyclass` attributes:
/// - `pyclass`: `add_reader` needs a Python callable, the bound
///   `on_readable`, and the registry is a Python dictionary;
/// - `frozen`: nothing needs `&mut self`. Every field is reached through
///   `&self`, from whichever thread holds the GIL, so the fields that change
///   are a `Mutex` or an atomic, and pyo3 keeps no runtime borrow flag;
/// - `weakref`: a `TokenReaper` refers to its channel weakly, so no reference
///   cycle runs through Rust fields the garbage collector cannot see (HDL-19).
#[pyclass(frozen, weakref)]
pub(super) struct LoopWake {
    /// The sending half of the token queue, cloned into each [`Notifier`].
    tx: mpsc::Sender<u64>,
    /// The receiving half of the token queue. Only `on_readable` drains it, on
    /// the loop thread, so the `Mutex` is never contended; it is there because
    /// `frozen` requires the struct to be `Sync`.
    rx: Mutex<mpsc::Receiver<u64>>,
    /// The pipe's write end, shared through the `Arc` with every [`Notifier`].
    /// It stays open while any notifier lives, so a late write can never reach
    /// a closed or reused descriptor (HDL-20).
    waker: Arc<Waker>,
    /// The pipe's read end, watched by the loop through `add_reader`. It is
    /// closed when the channel is dropped, after which a write gets `EPIPE`
    /// (HDL-20).
    read_fd: OwnedFd,
    /// The next token. Tokens are never reused, so a token that arrives after
    /// its entry was removed finds nothing, instead of resolving a later
    /// await.
    next_token: AtomicU64,
    /// Maps each token to its pending await. The Tokio side holds only the
    /// token, never the entry's Python objects (HDL-17).
    entries: Mutex<HashMap<u64, Entry>>,
    /// Hooks that tests run inside `on_readable`; see [`DrainHooks`].
    #[cfg(test)]
    drain_hooks: Mutex<DrainHooks>,
}

impl LoopWake {
    /// Create a channel with an empty token queue, a new non-blocking pipe, and
    /// no entries. It is not yet watched by any loop; [`LoopWake::install`]
    /// registers it. Fails only if the pipe cannot be created or made
    /// non-blocking, for example when the process is out of file descriptors.
    fn try_new() -> Result<Self, nix::Error> {
        let (waker, read_fd) = pywaker::pipe()?;
        let (tx, rx) = mpsc::channel();
        Ok(Self {
            tx,
            rx: Mutex::new(rx),
            waker: Arc::new(waker),
            read_fd,
            next_token: AtomicU64::new(0),
            entries: Mutex::new(HashMap::new()),
            #[cfg(test)]
            drain_hooks: Mutex::new(DrainHooks::default()),
        })
    }

    /// Return `event_loop`'s channel, installing one on first use, or `None`
    /// when the loop uses the `call_soon_threadsafe` fallback (HDL-21).
    ///
    /// Call on the thread running `event_loop`: installing calls `add_reader`.
    pub(super) fn for_loop<'py>(
        py: Python<'py>,
        event_loop: &Bound<'py, PyAny>,
    ) -> PyResult<Option<Bound<'py, LoopWake>>> {
        let registry = registry(py)?;
        let absent = absent(py)?;
        // `registry.get(loop, absent)`: the loop's state, or `absent` if it has
        // no entry.
        let current = match registry.call_method1("get", (event_loop, &absent)) {
            Ok(current) => current,
            // HDL-21: `get` builds `weakref.ref(loop)`, which raises
            // `TypeError` for a loop that cannot be weakly referenced.
            Err(err) if err.is_instance_of::<PyTypeError>(py) => return Ok(None),
            Err(err) => return Err(err),
        };
        // No entry: this is the loop's first `Handle` await.
        if current.is(&absent) {
            return Self::install(py, &registry, event_loop);
        }
        // Cached as unsupported (HDL-21).
        if current.is_none() {
            return Ok(None);
        }
        // A weak reference to the loop's channel. It is dead only once the
        // loop's selector has dropped the channel, when the loop was closed.
        match current.cast_into::<PyWeakrefReference>()?.upgrade() {
            Some(channel) => Ok(Some(channel.cast_into::<LoopWake>()?)),
            None => Self::install(py, &registry, event_loop),
        }
    }

    /// Install a channel for `event_loop` as one transaction: every
    /// `add_reader` failure removes it, and only `NotImplementedError` is
    /// cached (HDL-21).
    fn install<'py>(
        py: Python<'py>,
        registry: &Bound<'py, PyAny>,
        event_loop: &Bound<'py, PyAny>,
    ) -> PyResult<Option<Bound<'py, LoopWake>>> {
        let channel = Bound::new(py, Self::try_new().map_pyerr()?)?;
        // Provisional: inserted before `add_reader` so that every failure below
        // can be undone, leaving no reader registered for a channel the
        // registry does not know. Weak, so the registry never keeps the
        // channel, or anything it holds, alive (HDL-19).
        registry.set_item(event_loop, PyWeakrefReference::new(channel.as_any())?)?;
        // The bound method holds the channel. Once the loop's selector holds
        // the method, it is the channel's only strong owner (HDL-19).
        let reader = channel.getattr("on_readable")?;
        // The channel keeps ownership of the descriptor; the loop only watches
        // it.
        let read_fd = channel.get().read_fd.as_raw_fd();
        match event_loop.call_method1("add_reader", (read_fd, reader)) {
            Ok(_) => Ok(Some(channel)),
            // HDL-21: the loop cannot watch descriptors. Replacing the entry
            // lets the channel and its pipe be freed, and later awaits skip
            // `install`.
            Err(err) if err.is_instance_of::<PyNotImplementedError>(py) => {
                registry.set_item(event_loop, py.None())?;
                Ok(None)
            }
            // Any other failure: undo the insert and propagate, so a later
            // await tries again.
            Err(err) => {
                registry.del_item(event_loop)?;
                Err(err)
            }
        }
    }

    /// Record a pending await, `fut` waiting on `handle`, under a new token,
    /// and return the [`Notifier`] that its Tokio observer uses to report that
    /// `handle` completed, with the [`Release`] that completes when the
    /// entry's sender is dropped (HDL-17).
    ///
    /// Takes the bound `channel` rather than `&self` because the reaper needs a
    /// weak reference to the Python object the registry holds, and a `&self`
    /// cannot provide one (HDL-19). On error, nothing stays registered.
    pub(super) fn register(
        channel: &Bound<'_, LoopWake>,
        fut: &Bound<'_, PyAny>,
        handle: Py<PyHandle>,
    ) -> PyResult<(Notifier, Release)> {
        let py = channel.py();
        let this = channel.get();
        // Checked, so a token is never reused.
        let token = this
            .next_token
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
                token.checked_add(1)
            })
            .map_err(|_| PyRuntimeError::new_err("handle wake tokens exhausted"))?;
        // Removes this entry once `fut` is cancelled or settled.
        let reaper = Bound::new(
            py,
            TokenReaper {
                channel: PyWeakrefReference::new(channel.as_any())?.unbind(),
                token,
            },
        )?;
        let (release, released) = release_pair();
        this.insert(
            token,
            Entry {
                fut: fut.clone().unbind(),
                handle,
                release,
            },
        );
        // Runs the reaper when `fut` is cancelled or completed. If the future
        // rejects the callback, undo the insert. Removal is idempotent, since a
        // custom future may already have run the reaper.
        if let Err(err) = fut.call_method1("add_done_callback", (&reaper,)) {
            drop(this.take(token));
            return Err(err);
        }
        Ok((this.notifier(token), released))
    }

    /// All that the Tokio side gets for `token`: clones of the queue's sender
    /// and the pipe's write end, and the token (HDL-17).
    fn notifier(&self, token: u64) -> Notifier {
        Notifier {
            tx: self.tx.clone(),
            waker: Arc::clone(&self.waker),
            token,
        }
    }

    /// Add `entry` under `token`. Tokens are never reused, so the slot is
    /// always empty. Any replaced entry would be dropped after the lock is
    /// released, as [`LoopWake::take`] requires.
    fn insert(&self, token: u64, entry: Entry) {
        let previous = self
            .entries
            .lock()
            .expect("entries lock should not be poisoned")
            .insert(token, entry);
        debug_assert!(previous.is_none(), "tokens are never reused");
    }

    /// Remove `token`'s entry. The caller drops or uses it after the lock is
    /// released: a decref under the lock can run a finalizer that calls
    /// `register`, which takes the same lock.
    fn take(&self, token: u64) -> Option<Entry> {
        self.entries
            .lock()
            .expect("entries lock should not be poisoned")
            .remove(&token)
    }

    /// Read and discard every byte in the pipe, so the loop stops seeing it as
    /// readable, and return once the pipe is empty. The bytes carry no
    /// information: each only says "wake up", and the queue says which awaits
    /// completed. [`LoopWake::on_readable`] drains the queue next whatever
    /// happens here, so a token pushed after this drain still has its byte
    /// pending (HDL-18).
    fn drain_pipe(&self) {
        // Discarded; its size only sets how many bytes one `read` takes.
        let mut buf = [0u8; 64];
        loop {
            match retry_eintr(|| read(&self.read_fd, &mut buf)) {
                // `EAGAIN`: the pipe is empty. `Ok(0)`: end of file, which
                // needs every write end closed and cannot happen while the
                // channel holds its own `waker`.
                Ok(0) | Err(Errno::EAGAIN) => return,
                // More may remain.
                Ok(_) => {}
                // Unexpected; `EINTR` was already retried. Stop, and let the
                // caller drain the queue.
                Err(err) => {
                    tracing::error!(error = %err, "handle wake: pipe read failed");
                    return;
                }
            }
        }
    }

    /// Settle the pending await named by `token`: read its `Handle`'s value or
    /// error, unpickling a raw reply on this loop thread, and set it as the
    /// result of its `asyncio.Future` (HDL-7, HDL-17).
    ///
    /// Does nothing when there is no longer anyone to settle: the entry is
    /// gone or the future is already done. If another observer holds the raw
    /// reply's claim, re-insert the entry under the same token and wait for
    /// that observer's outcome or release. Other failures are reported through
    /// the future's loop, as asyncio reports an exception from a callback.
    ///
    /// Raises only a KeyboardInterrupt or SystemExit that interrupted the
    /// rebuild, after re-inserting the entry if its future still waits. The
    /// caller re-queues the token (HDL-27).
    fn deliver(&self, py: Python<'_>, token: u64) -> PyResult<()> {
        // A stale token: the reaper removed the entry because the await was
        // cancelled or settled. The entry is dropped after `take` releases
        // the lock.
        let Some(mut entry) = self.take(token) else {
            return Ok(());
        };
        // Already settled, for example cancelled but not yet reaped.
        if !entry.waiting(py) {
            return Ok(());
        }
        // The entry holds the Future to finish and the Handle that supplies its
        // result. Reading the Handle may run Python unpickle hooks, which can
        // finish or cancel the Future. Ready consumes the entry; Claimed and
        // Interrupted must check the Future again before restoring it.
        let read = entry.handle.bind(py).borrow().read_on_loop(py);
        match read {
            LoopReadOutcome::Ready(result) => {
                // A final shared value or error: finish the await and leave its
                // entry removed.
                let fut = entry.fut.bind(py);
                let published = match result {
                    Ok(value) => complete_asyncio_future(fut, false, value),
                    // Includes HDL-10's "producer ended without publishing a result".
                    Err(err) => complete_asyncio_future(fut, true, err.into_value(py).into_any()),
                };
                if let Err(err) = published {
                    // Cancellation and an already-finished future are handled
                    // by the completion helper. Report any other setter error.
                    report(
                        fut,
                        "handle wake: publishing to an asyncio future failed",
                        err,
                    );
                }
                Ok(())
            }
            LoopReadOutcome::Claimed => {
                // Another observer is reconstructing the reply. If this await
                // still waits, start a new native wait for that claim to settle
                // or release, then restore the entry under the same token.
                if entry.waiting(py) {
                    // The new waiter gets its own release. Replacing the
                    // sender drops the old one, whose waiter already returned
                    // after notifying, so nothing else is released.
                    let (release, released) = release_pair();
                    entry.release = release;
                    entry
                        .handle
                        .bind(py)
                        .borrow()
                        .notify_on_arrival(self.notifier(token), released);
                    // The new waiter may queue its token immediately, but the
                    // loop cannot consume it until this callback returns, after
                    // the entry has been restored.
                    self.insert(token, entry);
                }
                Ok(())
            }
            LoopReadOutcome::Interrupted(interrupt) => {
                // Reconstruction released its claim and left the bytes for a
                // retry. Preserve a waiting entry; on_readable re-queues its
                // token and the rest of the batch before raising the interrupt.
                if entry.waiting(py) {
                    self.insert(token, entry);
                }
                Err(interrupt)
            }
        }
    }

    /// Preserve a drained batch when delivery is interrupted.
    ///
    /// `on_readable()` removes every queued token before it starts delivering
    /// them. If one delivery raises KeyboardInterrupt or SystemExit, pass that
    /// token and the undelivered remainder here. Put them back in order and wake
    /// the pipe so a later loop turn retries them instead of losing their awaits.
    ///
    /// This channel owns the queue's receiver, so these sends cannot fail.
    fn requeue(&self, tokens: impl Iterator<Item = u64>) {
        tokens.for_each(|token| {
            self.tx
                .send(token)
                .expect("the channel holds its own receiver")
        });
        if let Err(err) = self.waker.wake() {
            tracing::error!(error = %err, "handle wake: pipe write failed");
        }
    }
}

/// Report a reader failure to `fut`'s loop through `call_exception_handler`,
/// the way asyncio reports an exception raised by a callback. Falls back to
/// logging if the loop cannot be reached.
fn report(fut: &Bound<'_, PyAny>, message: &str, err: PyErr) {
    let py = fut.py();
    let reported = (|| -> PyResult<()> {
        let context = PyDict::new(py);
        context.set_item("message", message)?;
        context.set_item("exception", err.value(py))?;
        context.set_item("future", fut)?;
        fut.call_method0("get_loop")?
            .call_method1("call_exception_handler", (context,))?;
        Ok(())
    })();
    if let Err(report_err) = reported {
        tracing::error!(error = %err, report_error = %report_err, "{message}");
    }
}

#[cfg(test)]
impl LoopWake {
    fn run_hook(
        &self,
        take: impl FnOnce(&mut DrainHooks) -> Option<Box<dyn FnOnce(&LoopWake) + Send>>,
    ) {
        let hook = take(
            &mut self
                .drain_hooks
                .lock()
                .expect("drain hooks lock should not be poisoned"),
        );
        if let Some(hook) = hook {
            hook(self);
        }
    }

    pub(super) fn entry_count(&self) -> usize {
        self.entries
            .lock()
            .expect("entries lock should not be poisoned")
            .len()
    }

    /// Whether the pipe becomes readable within `timeout`, without draining it.
    pub(super) fn readable_within(&self, timeout: Duration) -> bool {
        let mut fds = [PollFd::new(self.read_fd.as_fd(), PollFlags::POLLIN)];
        let millis = u16::try_from(timeout.as_millis()).unwrap_or(u16::MAX);
        poll(&mut fds, millis).unwrap() == 1
    }

    /// The pipe's write end. Its strong count falls as each notifier exits.
    pub(super) fn waker_for_test(&self) -> Arc<Waker> {
        Arc::clone(&self.waker)
    }
}

#[pymethods]
impl LoopWake {
    /// Expose each entry's future and `Handle` object to the garbage collector,
    /// so a loop dropped without being closed while an await is registered is
    /// still freed (HDL-19). Every holder of `entries` holds the GIL and
    /// allocates no Python object while holding it, so the collector never runs
    /// inside it and `try_lock` succeeds here. A lock missed in only one of the
    /// collector's passes could free a live future, so no holder may break that
    /// rule.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Ok(entries) = self.entries.try_lock() {
            for entry in entries.values() {
                visit.call(&entry.fut)?;
                visit.call(&entry.handle)?;
            }
        }
        Ok(())
    }

    /// The `add_reader` callback: drain the pipe, then the queue, then deliver
    /// (HDL-18).
    ///
    /// Raises only a KeyboardInterrupt or SystemExit from a rebuild, which
    /// asyncio re-raises out of the loop. First it re-queues the interrupted
    /// token and the batch's undelivered ones, so a later turn of the loop
    /// delivers them (HDL-27).
    fn on_readable(&self, py: Python<'_>) -> PyResult<()> {
        self.drain_pipe();
        #[cfg(test)]
        self.run_hook(|hooks| hooks.after_pipe_drain.take());
        // Collected so `rx` is released before any Python runs.
        let tokens: Vec<u64> = {
            let rx = self.rx.lock().expect("rx lock should not be poisoned");
            iter::from_fn(|| rx.try_recv().ok()).collect()
        };
        #[cfg(test)]
        self.run_hook(|hooks| hooks.after_queue_drain.take());
        let mut tokens = tokens.into_iter();
        tokens
            .by_ref()
            .try_for_each(|token| {
                self.deliver(py, token)
                    .map_err(|interrupt| (token, interrupt))
            })
            .map_err(|(token, interrupt)| {
                self.requeue(iter::once(token).chain(tokens));
                interrupt
            })
    }
}

/// Removes a pending await's entry once its future is cancelled or settled, so
/// an abandoned await does not keep its future and `Handle` alive.
///
/// [`LoopWake::register`] attaches it as the future's done callback. It may run
/// after delivery has already removed the entry, so removal is idempotent. A
/// `pyclass` because it is a Python callback; `frozen` because it never
/// changes.
#[pyclass(frozen)]
struct TokenReaper {
    /// A weak reference to the channel holding the entry. The future's done
    /// callbacks hold this reaper and the entry holds the future, so a strong
    /// reference would close a cycle through the reaper, which the garbage
    /// collector cannot traverse (HDL-19).
    channel: Py<PyWeakrefReference>,
    /// The entry to remove.
    token: u64,
}

#[pymethods]
impl TokenReaper {
    /// Remove the entry, if the channel still exists. Accepts any arguments; a
    /// done callback is passed the future.
    #[pyo3(signature = (*_args))]
    fn __call__(&self, py: Python<'_>, _args: &Bound<'_, PyTuple>) {
        // No channel means its loop, and every entry with it, is already gone.
        if let Some(channel) = self.channel.bind(py).upgrade()
            && let Ok(channel) = channel.cast::<LoopWake>()
        {
            // Dropped after `take` releases the lock.
            drop(channel.get().take(self.token));
        }
    }
}

/// Everything a `Handle`'s Tokio observer needs to report that one pending
/// await's `Handle` completed: the await's token, a sender into the channel's
/// token queue, and the pipe's write end. It is plain Rust, with no Python
/// object, so the observer uses it without the GIL (HDL-17).
pub(super) struct Notifier {
    /// A clone of the channel's `tx`. Sending fails once the channel is freed.
    tx: mpsc::Sender<u64>,
    /// The pipe's write end, shared with the channel and every other notifier.
    /// It stays open while this notifier lives (HDL-20).
    waker: Arc<Waker>,
    /// The token of the await this notifier reports.
    token: u64,
}

impl Notifier {
    /// Push the token, then write the pipe (HDL-18). A channel that is gone
    /// drops the token (HDL-20).
    pub(super) fn notify(self) {
        if self.tx.send(self.token).is_err() {
            return;
        }
        if let Err(err) = self.waker.wake() {
            tracing::error!(error = %err, "handle wake: pipe write failed");
        }
    }
}

/// Ends a pending await's native wait once nobody can read its result.
///
/// The paired sender is held by whatever ends the await: the await's
/// [`Entry`] on the pipe path, or the future's done callback on the fallback
/// (HDL-17, HDL-21). Dropping the sender completes [`Release::released`]. It
/// carries no value, and nothing is ever sent.
pub(super) struct Release(oneshot::Receiver<Infallible>);

impl Release {
    /// Complete once the paired sender has been dropped.
    pub(super) async fn released(self) {
        // Nothing can be sent, so the receive ends only with the sender's
        // drop, as an error.
        let _ = self.0.await;
    }
}

/// A sender and the [`Release`] it completes when dropped.
pub(super) fn release_pair() -> (oneshot::Sender<Infallible>, Release) {
    let (tx, rx) = oneshot::channel();
    (tx, Release(rx))
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::time::Instant;

    use pyo3::IntoPyObjectExt;
    use pyo3::exceptions::PyOSError;
    use pyo3::types::IntoPyDict;
    use pyo3::types::PyModule;

    use super::*;
    use crate::handle::core::Publisher;
    use crate::runtime::GilSite;
    use crate::runtime::ensure_python;
    use crate::runtime::monarch_with_gil_blocking;

    const HELPER: &CStr = cr#"
import asyncio


class Probe:
    pass


def start_observer(loop, h):
    async def start():
        return h.as_asyncio()

    return loop.run_until_complete(start())


def run_until_done(loop, fut):
    return loop.run_until_complete(asyncio.wait_for(fut, 5))


def run_await(loop, h):
    async def wait():
        return await asyncio.wait_for(h.as_asyncio(), 5)

    return loop.run_until_complete(wait())


def try_observe(loop, h):
    async def start():
        try:
            h.as_asyncio()
        except Exception as e:
            return e
        return None

    return loop.run_until_complete(start())


def await_in_task(loop, h):
    async def waiter():
        await h

    task = loop.create_task(waiter())
    loop.run_until_complete(asyncio.sleep(0))
    return task


async def _await(observer):
    await observer


def await_observer_in_task(loop, h):
    # The task holds only the observer future, not `h`: a task that held `h`
    # would, once published as `h`'s value, form a cycle through the Handle.
    async def start():
        return loop.create_task(_await(h.as_asyncio()))

    task = loop.run_until_complete(start())
    loop.run_until_complete(asyncio.sleep(0))
    return task


def start_and_cancel(loop, h):
    errors = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    async def start():
        h.as_asyncio().cancel()

    loop.run_until_complete(start())
    return errors


def observe_after_cancel(loop, h):
    async def observe():
        value = await asyncio.wait_for(h.as_asyncio(), 5)
        await asyncio.sleep(0.05)
        return value

    return loop.run_until_complete(observe())


class NoReaderLoop(asyncio.SelectorEventLoop):
    def __init__(self):
        super().__init__()
        self.add_reader_calls = 0

    def add_reader(self, fd, callback, *args):
        self.add_reader_calls += 1
        raise NotImplementedError


class FlakyReaderLoop(asyncio.SelectorEventLoop):
    def __init__(self):
        super().__init__()
        self.add_reader_calls = 0

    def add_reader(self, fd, callback, *args):
        self.add_reader_calls += 1
        if self.add_reader_calls == 1:
            raise OSError("transient add_reader failure")
        return super().add_reader(fd, callback, *args)


class RejectingFuture(asyncio.Future):
    def add_done_callback(self, fn, *, context=None):
        raise RuntimeError("add_done_callback rejected")


class RejectingLoop(asyncio.SelectorEventLoop):
    def create_future(self):
        return RejectingFuture(loop=self)


class TypeRejectingFuture(asyncio.Future):
    def add_done_callback(self, fn, *, context=None):
        raise TypeError("add_done_callback rejected")


class TypeRejectingLoop(asyncio.SelectorEventLoop):
    def create_future(self):
        return TypeRejectingFuture(loop=self)


class NoWeakFuture:
    __slots__ = ("_inner", "_asyncio_future_blocking")

    def __init__(self, loop):
        self._inner = asyncio.Future(loop=loop)
        self._asyncio_future_blocking = False

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def __await__(self):
        return self._inner.__await__()

    def add_done_callback(self, fn, *, context=None):
        self._inner.add_done_callback(lambda _: fn(self), context=context)


class NoWeakLoop(asyncio.SelectorEventLoop):
    def create_future(self):
        return NoWeakFuture(self)


class BrokenDoneFuture(asyncio.Future):
    def done(self):
        raise RuntimeError("done() is broken")


def capture_errors(loop):
    errors = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    return errors


def await_unreferenced(loop, h):
    # The task is not kept: only the future it awaits can keep it alive.
    results = []

    async def waiter():
        results.append(await h)

    loop.create_task(waiter())
    loop.run_until_complete(asyncio.sleep(0))
    return results


def run_until_nonempty(loop, items):
    async def wait():
        while not items:
            await asyncio.sleep(0.001)

    loop.run_until_complete(asyncio.wait_for(wait(), 5))


def base_done(fut):
    return asyncio.Future.done(fut)
"#;

    fn helper(py: Python<'_>) -> Bound<'_, PyModule> {
        PyModule::from_code(
            py,
            HELPER,
            c"handle_wake_test_helper.py",
            c"handle_wake_test_helper",
        )
        .unwrap()
    }

    fn new_loop(py: Python<'_>) -> Bound<'_, PyAny> {
        py.import("asyncio")
            .unwrap()
            .call_method0("new_event_loop")
            .unwrap()
    }

    fn pending_handle(py: Python<'_>) -> (Publisher, Py<PyHandle>) {
        let (publisher, handle) = PyHandle::pending();
        (publisher, Py::new(py, handle).unwrap())
    }

    fn ready_handle(py: Python<'_>, value: i64) -> Py<PyHandle> {
        Py::new(py, PyHandle::from_value(value.into_py_any(py).unwrap())).unwrap()
    }

    fn collect_garbage(py: Python<'_>) {
        py.import("gc").unwrap().call_method0("collect").unwrap();
    }

    fn is_done(fut: &Bound<'_, PyAny>) -> bool {
        fut.call_method0("done").unwrap().is_truthy().unwrap()
    }

    // HDL-17: Tokio writes the wake pipe while this thread holds the GIL.
    #[test]
    fn wake_is_written_while_test_holds_gil() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = new_loop(py);
            let (tx, handle) = pending_handle(py);
            let fut = helper
                .call_method1("start_observer", (&event_loop, &handle))
                .unwrap();
            let channel = LoopWake::for_loop(py, &event_loop)
                .unwrap()
                .expect("a selector loop should get a wake channel");

            tx.publish(Ok(42i64.into_py_any(py).unwrap()));
            assert!(
                channel.get().readable_within(Duration::from_secs(5)),
                "the observer should write the pipe without the GIL, which this thread holds"
            );

            let value: i64 = helper
                .call_method1("run_until_done", (&event_loop, &fut))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(value, 42, "the reader should set the Handle's value");
            event_loop.call_method0("close").unwrap();
        });
    }

    // HDL-19: a dropped observer's future stays registered until its loop
    // closes, and closing the loop releases the entry, its Handle and the
    // published value.
    #[test]
    fn dropped_observer_is_retained_until_loop_close() {
        ensure_python();
        let value_ref = monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = new_loop(py);
            let (tx, handle) = pending_handle(py);
            let fut = helper
                .call_method1("start_observer", (&event_loop, &handle))
                .unwrap();
            let channel = LoopWake::for_loop(py, &event_loop).unwrap().unwrap();
            drop(fut);
            drop(handle);
            collect_garbage(py);
            assert_eq!(
                channel.get().entry_count(),
                1,
                "the entry should hold the dropped observer's future"
            );

            let value = helper.getattr("Probe").unwrap().call0().unwrap();
            let value_ref = PyWeakrefReference::new(&value).unwrap().unbind();
            tx.publish(Ok(value.unbind()));
            assert!(
                channel.get().readable_within(Duration::from_secs(5)),
                "the observer should notify the channel"
            );
            event_loop.call_method0("close").unwrap();
            value_ref
        });
        monarch_with_gil_blocking(GilSite::Test, |py| {
            collect_garbage(py);
            assert!(
                value_ref.bind(py).upgrade().is_none(),
                "closing the loop should release the entry, its Handle and the value"
            );
        });
    }

    // HDL-19: a task that nothing else references survives a collection while
    // it awaits a pending Handle, because its entry holds the future.
    #[test]
    fn unreferenced_waiting_task_survives_collection() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = new_loop(py);
            let errors = helper
                .call_method1("capture_errors", (&event_loop,))
                .unwrap();
            let (tx, handle) = pending_handle(py);
            let results = helper
                .call_method1("await_unreferenced", (&event_loop, &handle))
                .unwrap();
            drop(handle);
            collect_garbage(py);
            tx.publish(Ok(7i64.into_py_any(py).unwrap()));
            helper
                .call_method1("run_until_nonempty", (&event_loop, &results))
                .unwrap();
            assert_eq!(results.extract::<Vec<i64>>().unwrap(), vec![7]);
            assert_eq!(
                errors.len().unwrap(),
                0,
                "no pending task should have been destroyed"
            );
            event_loop.call_method0("close").unwrap();
        });
    }

    // HDL-19: a loop dropped without being closed, while an await on a pending
    // Handle with no value is registered, is freed by a collection. Freeing
    // its channel releases the await's waiter, which drops the core and the
    // write end before anything is published (HDL-17, HDL-20).
    #[test]
    fn unclosed_loop_with_a_pending_await_is_collected() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = new_loop(py);
            let (tx, handle) = pending_handle(py);
            let task = helper
                .call_method1("await_in_task", (&event_loop, &handle))
                .unwrap();
            let waker = LoopWake::for_loop(py, &event_loop)
                .unwrap()
                .expect("a selector loop uses the pipe")
                .get()
                .waker_for_test();
            let loop_ref = PyWeakrefReference::new(&event_loop).unwrap();
            drop((task, event_loop, handle));
            collect_garbage(py);
            assert!(
                loop_ref.upgrade().is_none(),
                "the collector should free the unclosed loop"
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while Arc::strong_count(&waker) > 1 || tx.observer_count() > 0 {
                assert!(
                    Instant::now() < deadline,
                    "the released waiter should drop its notifier and the core"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            drop(tx);
        });
    }

    // HDL-20: a notify after the channel is gone is dropped.
    #[test]
    fn late_notify_after_channel_dropped_is_ignored() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let channel = Bound::new(py, LoopWake::try_new().unwrap()).unwrap();
            let notifier = Notifier {
                tx: channel.get().tx.clone(),
                waker: Arc::clone(&channel.get().waker),
                token: 0,
            };
            let waker = Arc::clone(&notifier.waker);
            drop(channel);

            notifier.notify();
            assert!(
                !waker.wake().unwrap(),
                "a write after the read end closed should report EPIPE"
            );
        });
    }

    // HDL-19: closing a loop frees it and its channel, even with an await
    // pending.
    #[test]
    fn channel_does_not_retain_loop() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = new_loop(py);
            let (tx, handle) = pending_handle(py);
            let task = helper
                .call_method1("await_in_task", (&event_loop, &handle))
                .unwrap();
            let channel = LoopWake::for_loop(py, &event_loop).unwrap().unwrap();
            assert_eq!(
                channel.get().entry_count(),
                1,
                "the awaiting task should be registered"
            );
            let loop_ref = PyWeakrefReference::new(&event_loop).unwrap();
            let channel_ref = PyWeakrefReference::new(channel.as_any()).unwrap();

            event_loop.call_method0("close").unwrap();
            drop((task, channel, event_loop));
            collect_garbage(py);
            assert!(loop_ref.upgrade().is_none(), "the loop should be freed");
            assert!(
                channel_ref.upgrade().is_none(),
                "the loop's channel, with its token, should be freed with it"
            );

            tx.publish(Ok(py.None()));
        });
    }

    // HDL-19: closing the loop releases the channel-owned graph, so a
    // completed, undelivered Handle value that references the loop no longer
    // keeps it alive. The value is the observer future itself, or a task that
    // awaits that future without holding the Handle.
    #[test]
    fn closing_loop_releases_values_that_reference_it() {
        ensure_python();
        for variant in ["observer future", "awaiting task"] {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                let helper = helper(py);
                let event_loop = new_loop(py);
                let (tx, handle) = pending_handle(py);
                let fixture = match variant {
                    "observer future" => "start_observer",
                    _ => "await_observer_in_task",
                };
                let value = helper
                    .call_method1(fixture, (&event_loop, &handle))
                    .unwrap();
                let channel = LoopWake::for_loop(py, &event_loop).unwrap().unwrap();
                let waker = Arc::clone(&channel.get().waker);
                let loop_ref = PyWeakrefReference::new(&event_loop).unwrap();
                let value_ref = PyWeakrefReference::new(&value).unwrap();
                let channel_ref = PyWeakrefReference::new(channel.as_any()).unwrap();

                // The loop never runs again, so the value is never delivered.
                tx.publish(Ok(value.clone().unbind()));
                assert!(
                    channel.get().readable_within(Duration::from_secs(5)),
                    "{variant}: the observer should finish, releasing its receiver"
                );

                event_loop.call_method0("close").unwrap();
                drop((value, handle, channel, event_loop));
                collect_garbage(py);
                assert!(
                    loop_ref.upgrade().is_none(),
                    "{variant}: the loop should be freed"
                );
                assert!(
                    value_ref.upgrade().is_none(),
                    "{variant}: the Handle value should be freed"
                );
                assert!(
                    channel_ref.upgrade().is_none(),
                    "{variant}: the channel, with its entry, should be freed"
                );
                assert!(
                    !waker.wake().unwrap(),
                    "{variant}: the pipe's read end should be closed"
                );
            });
        }
    }

    // HDL-7: a future whose `done()` raises is reported to its loop, and the
    // other token in the same drain is still delivered.
    #[test]
    fn done_failure_is_reported_without_collateral_damage() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = new_loop(py);
            let errors = helper
                .call_method1("capture_errors", (&event_loop,))
                .unwrap();
            let channel = Bound::new(py, LoopWake::try_new().unwrap()).unwrap();
            let broken = helper
                .getattr("BrokenDoneFuture")
                .unwrap()
                .call((), Some(&[("loop", &event_loop)].into_py_dict(py).unwrap()))
                .unwrap();
            let healthy = event_loop.call_method0("create_future").unwrap();
            for (fut, value) in [(&broken, 1i64), (&healthy, 2)] {
                let (notifier, _release) =
                    LoopWake::register(&channel, fut, ready_handle(py, value)).unwrap();
                notifier.notify();
            }

            channel.get().on_readable(py).unwrap();
            assert!(is_done(&healthy), "the healthy token should be delivered");
            let base_done: bool = helper
                .call_method1("base_done", (&broken,))
                .unwrap()
                .extract()
                .unwrap();
            assert!(!base_done, "the broken future should be left pending");
            assert_eq!(
                channel.get().entry_count(),
                0,
                "both entries should be released"
            );
            assert_eq!(
                errors.len().unwrap(),
                1,
                "one failure should be reported: {errors}"
            );
            let context = errors.get_item(0).unwrap();
            let message: String = context.get_item("message").unwrap().extract().unwrap();
            assert!(
                message.contains("done() failed"),
                "unexpected message: {message}"
            );
            assert!(
                context
                    .get_item("exception")
                    .unwrap()
                    .is_instance_of::<PyRuntimeError>(),
                "the report should carry done()'s exception"
            );
            event_loop.call_method0("close").unwrap();
        });
    }

    // HDL-7: completing after a cancelled observer raises nothing into the loop.
    #[test]
    fn cancelled_observer_then_completion_is_silent() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = new_loop(py);
            let (tx, handle) = pending_handle(py);
            let errors = helper
                .call_method1("start_and_cancel", (&event_loop, &handle))
                .unwrap();

            tx.publish(Ok(7i64.into_py_any(py).unwrap()));
            let value: i64 = helper
                .call_method1("observe_after_cancel", (&event_loop, &handle))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(value, 7, "a later observer should resolve");
            assert_eq!(
                errors.len().unwrap(),
                0,
                "the reader should raise nothing into the loop: {errors}"
            );
            event_loop.call_method0("close").unwrap();
        });
    }

    // HDL-18: a token pushed at either drain boundary is delivered.
    #[test]
    fn token_pushed_between_drains_is_delivered() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let event_loop = new_loop(py);
            let channel = Bound::new(py, LoopWake::try_new().unwrap()).unwrap();
            let register_ready = |value: i64| {
                let fut = event_loop.call_method0("create_future").unwrap();
                let (notifier, _release) =
                    LoopWake::register(&channel, &fut, ready_handle(py, value)).unwrap();
                (fut, notifier)
            };

            let queued: Vec<_> = (0..8)
                .map(|value| {
                    let (fut, notifier) = register_ready(value);
                    notifier.notify();
                    fut
                })
                .collect();
            let (between, notifier) = register_ready(100);
            channel.get().drain_hooks.lock().unwrap().after_pipe_drain =
                Some(Box::new(move |_| notifier.notify()));
            channel.get().on_readable(py).unwrap();
            assert!(
                queued.iter().all(is_done),
                "queued tokens should be delivered"
            );
            assert!(
                is_done(&between),
                "a token pushed between the drains should be delivered in the same callback"
            );

            let (after, notifier) = register_ready(200);
            channel.get().drain_hooks.lock().unwrap().after_queue_drain =
                Some(Box::new(move |_| notifier.notify()));
            channel.get().on_readable(py).unwrap();
            assert!(
                !is_done(&after),
                "a token pushed after the queue drain should wait for the next callback"
            );
            assert!(
                channel.get().readable_within(Duration::ZERO),
                "its pipe byte should still be pending"
            );
            channel.get().on_readable(py).unwrap();
            assert!(is_done(&after), "the pending byte should deliver it");
            event_loop.call_method0("close").unwrap();
        });
    }

    // HDL-18: `EINTR` is retried, and any other result returns at once.
    #[test]
    fn retry_eintr_repeats_until_a_non_eintr_result() {
        let mut calls = 0;
        let result = retry_eintr(|| {
            calls += 1;
            if calls < 3 { Err(Errno::EINTR) } else { Ok(1) }
        });
        assert_eq!(
            (result, calls),
            (Ok(1), 3),
            "two EINTRs should be retried before the success"
        );

        let mut calls = 0;
        let result: nix::Result<i32> = retry_eintr(|| {
            calls += 1;
            Err(Errno::EAGAIN)
        });
        assert_eq!(
            (result, calls),
            (Err(Errno::EAGAIN), 1),
            "EAGAIN should return without a retry"
        );
    }

    // HDL-21: a loop without `add_reader` falls back and is not retried.
    #[test]
    fn loop_without_add_reader_falls_back() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = helper.getattr("NoReaderLoop").unwrap().call0().unwrap();
            for value in [1i64, 2] {
                // Pending, so the observation reaches wake-channel installation.
                let (tx, handle) = pending_handle(py);
                let fut = helper
                    .call_method1("start_observer", (&event_loop, &handle))
                    .unwrap();
                tx.publish(Ok(value.into_py_any(py).unwrap()));
                let got: i64 = helper
                    .call_method1("run_until_done", (&event_loop, &fut))
                    .unwrap()
                    .extract()
                    .unwrap();
                assert_eq!(got, value, "the fallback should resolve the await");
            }
            let calls: i64 = event_loop
                .getattr("add_reader_calls")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(
                calls, 1,
                "a loop cached as unsupported should not be retried"
            );
            assert!(LoopWake::for_loop(py, &event_loop).unwrap().is_none());
            event_loop.call_method0("close").unwrap();
        });
    }

    // HDL-21: a failed `add_reader` is not cached, and a later await installs.
    #[test]
    fn failed_add_reader_does_not_poison_registry() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = helper.getattr("FlakyReaderLoop").unwrap().call0().unwrap();
            // Pending, with the producer held, so each observation reaches
            // wake-channel installation.
            let (_first_tx, first) = pending_handle(py);
            let err = helper
                .call_method1("try_observe", (&event_loop, &first))
                .unwrap();
            assert!(
                err.is_instance_of::<PyOSError>(),
                "the first as_asyncio should raise the add_reader failure: {err}"
            );

            let (tx, second) = pending_handle(py);
            let fut = helper
                .call_method1("start_observer", (&event_loop, &second))
                .unwrap();
            tx.publish(Ok(2i64.into_py_any(py).unwrap()));
            let got: i64 = helper
                .call_method1("run_until_done", (&event_loop, &fut))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(got, 2, "the second await should resolve through the wake");
            let calls: i64 = event_loop
                .getattr("add_reader_calls")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(calls, 2, "the failed install should have been retried");
            assert!(LoopWake::for_loop(py, &event_loop).unwrap().is_some());
            event_loop.call_method0("close").unwrap();
        });
    }

    // HDL-19: a failed registration leaves no entry holding its Handle.
    #[test]
    fn failed_registration_leaves_no_entry() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = helper.getattr("RejectingLoop").unwrap().call0().unwrap();
            // Pending, with the producer held, so the observation reaches
            // `LoopWake::register`.
            let (_tx, handle) = pending_handle(py);
            let err = helper
                .call_method1("try_observe", (&event_loop, &handle))
                .unwrap();
            assert!(
                err.is_instance_of::<PyRuntimeError>(),
                "as_asyncio should raise the add_done_callback failure: {err}"
            );

            let channel = LoopWake::for_loop(py, &event_loop)
                .unwrap()
                .expect("the channel is installed before registration");
            assert_eq!(
                channel.get().entry_count(),
                0,
                "the failed registration should remove its entry"
            );
            event_loop.call_method0("close").unwrap();
        });
    }

    // A future that cannot be weakly referenced uses the channel.
    #[test]
    fn future_without_weakref_uses_the_channel() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = helper.getattr("NoWeakLoop").unwrap().call0().unwrap();
            let (tx, handle) = pending_handle(py);
            let fut = helper
                .call_method1("start_observer", (&event_loop, handle))
                .unwrap();
            let channel = LoopWake::for_loop(py, &event_loop)
                .unwrap()
                .expect("the loop supports the channel");
            assert_eq!(
                channel.get().entry_count(),
                1,
                "the await should be registered with the channel"
            );
            tx.publish(Ok(7i64.into_py_any(py).unwrap()));
            let got: i64 = helper
                .call_method1("run_until_done", (&event_loop, &fut))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(got, 7, "the channel should resolve the await");
            assert_eq!(
                channel.get().entry_count(),
                0,
                "the delivered await should leave no entry"
            );
            event_loop.call_method0("close").unwrap();
        });
    }

    // A `TypeError` from the future's own `add_done_callback` propagates.
    #[test]
    fn add_done_callback_type_error_propagates() {
        ensure_python();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = helper(py);
            let event_loop = helper
                .getattr("TypeRejectingLoop")
                .unwrap()
                .call0()
                .unwrap();
            // Pending, with the producer held, so the observation reaches
            // `LoopWake::register`.
            let (_tx, handle) = pending_handle(py);
            let err = helper
                .call_method1("try_observe", (&event_loop, &handle))
                .unwrap();
            assert!(
                err.is_instance_of::<PyTypeError>(),
                "as_asyncio should raise the add_done_callback TypeError, not fall back: {err}"
            );
            let channel = LoopWake::for_loop(py, &event_loop)
                .unwrap()
                .expect("the channel is installed before registration");
            assert_eq!(
                channel.get().entry_count(),
                0,
                "the failed registration should remove its entry"
            );
            event_loop.call_method0("close").unwrap();
        });
    }
}
