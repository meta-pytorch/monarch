/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use pyo3::exceptions::PyException;
use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;

use super::core::Core;
use super::core::Publisher;
use super::outcome::clone_shared_outcome;
use super::produce::ObserverLoss;
use super::produce::ObserverStop;
use super::produce::spawn_producer;
use crate::runtime::GilSite;
use crate::runtime::is_main_thread;
use crate::runtime::monarch_with_gil;
use crate::runtime::signal_safe_block_on_inline;

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
pub(super) async fn settle(future: ReceiveFuture) -> PyResult<Py<PyAny>> {
    match future.await {
        ReceiveOutcome::Outcome(result) => result,
        ReceiveOutcome::Interrupted { finish, .. } => finish.await,
    }
}

/// Hold a port or stream-reply receive until its Handle is first observed
/// (HDL-24). Creating this state takes no message from the queue.
pub(super) struct DeferredReceive {
    /// Unstarted work that takes the next message, waits if necessary, and
    /// unpickles it. The first observer takes this future and runs it inline
    /// or starts it as a Tokio task.
    pub(super) future: ReceiveFuture,
    /// Optional Python stack captured when the receive or stream item was
    /// created. When the work starts, this copy goes with the producer so it
    /// can log a failure even after every observer and the core's copy of
    /// the traceback are gone (HDL-13).
    pub(super) traceback: Option<Py<PyAny>>,
    /// Whether a spawned receive stops or continues after its last observer goes.
    /// Ports use ObserverLoss::Stop to release their receiver (HDL-23); stream
    /// items use ObserverLoss::Continue to finish work already started.
    pub(super) observer_loss: ObserverLoss,
}

/// A port or stream-reply receive claimed by a blocking `get()`.
///
/// The caller runs the receive on its own thread and publishes the result for
/// every observer. If it stops waiting before completion, these owned values
/// can be handed to Tokio so the receive continues (HDL-24).
pub(super) struct Drive {
    /// The claimed future: take the next queued message, wait if necessary,
    /// and unpickle it into a Python value or exception.
    pub(super) future: ReceiveFuture,
    /// Store the outcome in the shared core and wake its waiters. Dropping this
    /// without publishing tells observers that the producer ended without a
    /// result, rather than leaving them waiting indefinitely (HDL-10).
    pub(super) publisher: Publisher,
    /// The observer-loss policy to apply if this receive is handed to Tokio.
    /// Ports use ObserverLoss::Stop; stream items use ObserverLoss::Continue
    /// so work already started keeps running without observers (HDL-23).
    pub(super) observer_loss: ObserverLoss,
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
#[inline]
pub(super) fn drive_inline(
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
            // The blocking helper has reacquired the GIL on this thread. Clone
            // the stored value or exception without unpickling again or
            // consuming the shared result, outside Core.state's mutex.
            clone_shared_outcome(py, &outcome)
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
