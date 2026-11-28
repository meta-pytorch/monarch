/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::Weak;

use pyo3::prelude::*;
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

use super::outcome::Reconstruct;
use super::outcome::ReconstructionFailurePolicy;
use super::outcome::handle_producer_ended_error;
use super::outcome::log_unobserved;
use super::outcome::reconstruct_catching_panic;
use super::produce::ObserverLoss;
use super::produce::ObserverStop;
use super::produce::spawn_producer;
use super::receive::DeferredReceive;
use super::receive::Drive;
use super::receive::settle;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;

/// Whether the shared Handle has unstarted receive work, is awaiting a result,
/// or has finished. Every observer consults the same state under the core's lock.
pub(super) enum State {
    /// A port or stream-reply receive waiting for its first observer.
    /// No message has been taken from the queue (HDL-24).
    Unstarted(DeferredReceive),
    /// No result yet. A Tokio task, an observer running the work inline, or
    /// a Python completer is responsible for supplying it.
    Running(Running),
    /// A successful endpoint reply has arrived, but is still pickled.
    Raw(Raw),
    /// The final value or error, never replaced. Every observer shares this
    /// outcome, and reading it does not consume it (HDL-1, HDL-2, HDL-3).
    Done(Arc<PyResult<Py<PyAny>>>),
    /// The producer ended without supplying a result. Observers raise the
    /// producer-ended error rather than waiting indefinitely (HDL-10).
    Lost,
}

/// The state while the result is still being produced.
#[derive(Default)]
pub(super) struct Running {
    /// Wake observers waiting for the result. Created only when an observer
    /// first needs to wait. Completion changes the state before notifying
    /// the waiters; the notification carries no result itself (HDL-25).
    pub(super) wake: WakeSlot,
    /// Tell a spawned receive to stop when the last Handle or active waiter
    /// drops the core. Present only for work with ObserverLoss::Stop;
    /// ordinary producers continue without observers (HDL-23).
    stop: Option<Arc<Notify>>,
}

/// The state while a successful reply waits, still pickled, for the observer
/// that reconstructs it.
pub(super) struct Raw {
    /// Retained independently of an attempt, so an interrupted observer
    /// cannot destroy the bytes needed by the next observer.
    reply: Arc<dyn Reconstruct>,
    /// One observer has the sole right to reconstruct and publish (HDL-27).
    pub(super) claimed: bool,
    /// Wake observers waiting for reconstruction to finish (HDL-25).
    /// Moved from Running if waiters already exist; otherwise created only
    /// when an observer needs to wait. Releasing the reconstruction claim
    /// or storing the final value or error wakes them to recheck the state.
    /// The notification carries no value or error itself.
    wake: WakeSlot,
}

impl Raw {
    /// Claim the sole right to reconstruct this reply (HDL-27), unless another
    /// observer holds it. Call with Core.state's mutex held, in the same
    /// critical section as the state check that found this Raw.
    fn try_claim(&mut self, core: &Arc<Core>) -> Option<ReconstructionClaim> {
        // The reply has arrived as pickled bytes, not a Python value.
        // If another observer owns the claim, leave it alone.
        if self.claimed {
            return None;
        }
        // Take the exclusive right to unpickle while Core.state's
        // mutex is held, so no second observer can claim the same reply.
        self.claimed = true;
        // Return the claim, not a decoded value. The caller unpickles
        // after it releases the mutex. Dropping an unfinished
        // claim releases that right and wakes waiting observers.
        Some(ReconstructionClaim {
            // Keep the shared core alive throughout this attempt.
            core: Some(Arc::clone(core)),
            // Retain the original reply; no byte buffers are copied here.
            reply: Arc::clone(&self.reply),
        })
    }
}

/// The signal that wakes observers waiting for the state to change (HDL-25).
///
/// The Notify is created only when an observer must wait, never by a
/// transition alone. A waiter registers under the state lock, in the same
/// critical section as its state check, so a transition cannot slip between
/// them. Transitions notify after releasing the lock; woken observers recheck
/// the state, because the notification carries no result.
#[derive(Clone, Default)]
pub(super) struct WakeSlot(Option<Arc<Notify>>);

impl WakeSlot {
    /// Register one waiter, creating the shared Notify for the first.
    /// Call with Core.state's mutex held.
    fn register(&mut self) -> OwnedNotified {
        self.0
            .get_or_insert_with(|| Arc::new(Notify::new()))
            .clone()
            .notified_owned()
    }

    /// Wake every registered waiter, if any observer created the Notify.
    /// Call after releasing Core.state's mutex.
    fn notify(&self) {
        if let Some(wake) = &self.0 {
            wake.notify_waiters();
        }
    }

    /// How many waiters hold the Notify, or `None` if none was created. For tests.
    #[cfg(test)]
    pub(super) fn waiters_for_test(&self) -> Option<usize> {
        self.0.as_ref().map(|wake| Arc::strong_count(wake) - 1)
    }
}

/// The result of checking whether a Handle's outcome is available.
/// This snapshot can be used after releasing the core's state lock.
pub(super) enum Observed {
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
    /// This observer has the sole right to reconstruct a pickled reply.
    /// The guard releases that right if the observation is interrupted (HDL-27).
    Reconstruct(ReconstructionClaim),
}

impl Observed {
    /// Convert this observation to the outcome a wait would return, or return
    /// None if it is Pending: only a pending observation needs to wait.
    /// The conversion takes no lock and cannot fail.
    pub(super) fn into_wait_outcome(self) -> Option<WaitOutcome> {
        match self {
            Self::Done(outcome) => Some(WaitOutcome::Done(outcome)),
            Self::Lost => Some(WaitOutcome::Lost),
            Self::Reconstruct(claim) => Some(WaitOutcome::Reconstruct(claim)),
            Self::Pending => None,
        }
    }
}

/// What a wait for the Handle's outcome ends in. Unlike Observed, never Pending.
///
/// A reconstruction claim is not readiness: the observer holding it must
/// reconstruct the reply before the Handle has its outcome (HDL-27).
pub(super) enum WaitOutcome {
    /// The stored value or error, kept alive by the Arc after unlocking.
    Done(Arc<PyResult<Py<PyAny>>>),
    /// The producer ended without a result (HDL-10).
    Lost,
    /// This observer has the sole right to reconstruct a pickled reply (HDL-27).
    Reconstruct(ReconstructionClaim),
}

/// Give one observer the right to unpickle a successful endpoint reply.
///
/// Other observers cannot unpickle it at the same time. Python runs without
/// holding Core.state's mutex. Once this observer stores a value or error,
/// every observer shares that outcome.
///
/// If this claim is dropped before storing an outcome, release it and wake
/// waiting observers. The retained reply remains available for another attempt.
pub(super) struct ReconstructionClaim {
    /// Keep the core alive throughout this attempt.
    /// Some means this observer still owns the reconstruction claim. Taken
    /// before storing the outcome so Drop does not release an already-settled
    /// claim.
    core: Option<Arc<Core>>,
    /// The retained pickled reply. Each attempt builds a new PicklingState,
    /// sharing the original byte buffers and copying the mesh-reference table.
    /// Unpickling consumes that attempt's state, not this retained reply.
    /// If interrupted, another attempt can start from the beginning using
    /// the same bytes and references.
    reply: Arc<dyn Reconstruct>,
}

impl ReconstructionClaim {
    /// Unpickle this successful endpoint reply and store its value or error
    /// for every observer. This claim prevents another observer from unpickling
    /// the reply at the same time (HDL-27).
    ///
    /// The caller must hold the GIL, but not Core.state's mutex.
    /// Ordinary Python exceptions become the Handle's shared error. The
    /// failure policy determines what happens to other failures:
    ///
    /// - KeepReplyAfterInterrupt returns a BaseException outside Exception,
    ///   such as KeyboardInterrupt, to this observer only. A Rust panic also
    ///   escapes. Both release the claim and leave Raw available for a retry.
    /// - KeepReplyAfterLoopInterrupt returns only KeyboardInterrupt or
    ///   SystemExit, releasing the claim. It stores every other Python
    ///   exception and converts a Rust panic to a shared RuntimeError, so
    ///   nothing unwinds through the asyncio wake reader.
    /// - StoreEveryFailureInHandle stores every Python exception and converts
    ///   a Rust panic to a shared RuntimeError. The asyncio fallback worker
    ///   uses this because its callback needs a final value or error.
    ///
    /// Ok contains the shared outcome, which may itself be an error. Err means
    /// this observation was interrupted; the retained reply remains retryable.
    pub(super) fn reconstruct(
        mut self,
        py: Python<'_>,
        failure_policy: ReconstructionFailurePolicy,
    ) -> PyResult<Arc<PyResult<Py<PyAny>>>> {
        let result = match failure_policy {
            ReconstructionFailurePolicy::KeepReplyAfterInterrupt => {
                // Preserve ordinary caller behavior: let a Rust panic unwind.
                // Dropping this claim leaves the pickled reply available for retry.
                self.reply.reconstruct(py)
            }
            ReconstructionFailurePolicy::KeepReplyAfterLoopInterrupt
            | ReconstructionFailurePolicy::StoreEveryFailureInHandle => {
                // Both asyncio paths turn a Rust panic into a shared RuntimeError.
                // `interrupts()` below decides which Python exceptions remain
                // retryable.
                reconstruct_catching_panic(self.reply.as_ref(), py)
            }
        };
        // If the policy takes the failure as an interrupt of this observation,
        // return it to this observer without storing a shared error. Another
        // observer can then retry the retained reply. Every other failure
        // continues below to become the Handle's shared outcome.
        if let Err(error) = &result
            && failure_policy.interrupts(py, error)
        {
            // Returning drops this claim: its Drop clears Raw.claimed and
            // wakes waiting observers. Nothing is stored in Done.
            return Err(result.expect_err("the result was checked as an interrupt"));
        }
        // Retryable interruptions returned above. What remains is final: either
        // the reconstructed value or an exception every observer will share.
        let outcome = Arc::new(result);
        // Tests can pause with the claim still held, proving a competing
        // observer cannot claim the reply before this outcome is stored.
        #[cfg(test)]
        run_test_hook(&BEFORE_RECONSTRUCTION_SETTLE);
        // Disarm Drop before storing the outcome. It must not try to release
        // the reconstruction claim after Raw has become Done.
        let core = self.core.take().expect("HDL-27: a claim settles once");
        // Replace Raw with Done under the core's mutex, then wake its waiters
        // after unlocking. Every later observer reads this same outcome.
        core.finish(State::Done(Arc::clone(&outcome)));
        // The outer Ok says this claim settled the Handle. `outcome` itself
        // still contains either the value or the shared exception.
        Ok(outcome)
    }
}

impl Drop for ReconstructionClaim {
    /// Release an unfinished reconstruction claim, allowing another attempt.
    /// An interrupt or Rust panic can drop this guard before an outcome is
    /// stored. Keep the reply in Raw, clear its claimed flag under the core's
    /// mutex, then wake waiting observers after unlocking (HDL-27, HDL-25).
    /// This prevents a departed observer from leaving the reply permanently
    /// claimed. It does not retry unpickling itself or guarantee that another
    /// attempt succeeds; a deterministic panic can recur.
    ///
    /// If reconstruct() already stored an outcome, it took this guard's core
    /// first. In that case, do nothing: the shared outcome must remain intact.
    fn drop(&mut self) {
        // None means reconstruct() disarmed this guard before storing Done.
        // Otherwise this guard still owns the unfinished reconstruction claim.
        let Some(core) = self.core.take() else {
            return;
        };
        // Release the claim under the mutex and retain the optional wake signal
        // for use after unlocking. No unpickling or Python calls happen here.
        let wake = {
            let mut state = core.lock();
            match &mut *state {
                State::Raw(raw) => {
                    // Only release exclusive access. Leave the original reply
                    // in Raw so another observer can reconstruct it from scratch.
                    raw.claimed = false;
                    raw.wake.clone()
                }
                _ => unreachable!("HDL-27: an unfinished claim retains Raw"),
            }
        };
        // The block released Core.state's mutex. Wake observers so they recheck
        // Raw and one can claim the reply; this path stores no value or error.
        wake.notify();
    }
}

/// Whether an observer checks the shared result or takes an unstarted receive
/// to run on the calling thread.
///
/// Only one observer can take the port or stream-reply receive. Other observers
/// read or wait for its shared result (HDL-24).
pub(super) enum Observation {
    /// A snapshot of the shared result: ready, producer lost, or still pending.
    /// This observer takes no ownership of the receive future.
    Observed(Observed),
    /// This observer claimed the unstarted receive to run on its own thread.
    /// It owns both the future and the publisher that supplies the shared result
    /// (HDL-24).
    Drive(Drive),
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
    pub(super) traceback: Option<Py<PyAny>>,
}

impl Core {
    /// Create a shared core in the supplied state, retaining any creation stack.
    /// This allocates the core; it starts no work and creates no wake signal.
    pub(super) fn new(state: State, traceback: Option<Py<PyAny>>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(state),
            traceback,
        })
    }

    /// Lock the shared state until the returned guard is dropped.
    /// While holding it, do not call Python, change the GIL, await, notify,
    /// or drop Python objects. Code under this leaf lock must not panic,
    /// so a poisoned mutex indicates a violated invariant (HDL-9).
    pub(super) fn lock(&self) -> MutexGuard<'_, State> {
        #[cfg(test)]
        STATE_LOCK_COUNT.with(|count| count.set(count.get() + 1));
        self.state
            .lock()
            .expect("should not be poisoned: nothing panics under the state lock (HDL-9)")
    }

    /// Check for a result, starting an unstarted port or stream-reply receive
    /// as a Tokio task (HDL-24).
    ///
    /// This can start receive work or claim an available raw reply for
    /// reconstruction. It never polls a receive or runs Python here.
    /// Return the shared outcome, loss, Pending or that exclusive claim.
    #[inline]
    pub(super) fn observe(self: &Arc<Self>) -> Observed {
        // Keep the lock through the start decision and the move to Running,
        // so another observer cannot take the same receive future.
        let mut state = self.lock();

        // Finished or already started: return its snapshot. Otherwise read
        // the unstarted receive's observer-loss policy to start it below.
        let observer_loss = match &mut *state {
            State::Done(outcome) => return Observed::Done(outcome.clone()),
            State::Lost => return Observed::Lost,
            State::Running(_) => return Observed::Pending,
            State::Raw(raw) => {
                return raw
                    .try_claim(self)
                    .map_or(Observed::Pending, Observed::Reconstruct);
            }
            // A deferred port receive or stream item has no observer that has
            // started its future yet. A message may already be queued, but this
            // future has not taken or unpickled it.
            State::Unstarted(deferred) => deferred.observer_loss,
        };
        // The task owns the future and publisher. This caller gets a
        // Pending snapshot without waiting; completion can race its return.
        self.spawn_unstarted(state, observer_loss);
        Observed::Pending
    }

    /// Check the shared result, or take a port or stream-reply
    /// receive that has not started. The state lock ensures only one observer
    /// takes its future (HDL-24).
    ///
    /// Only `get()` calls this. For an unstarted receive, return the receive
    /// future and its publisher as `Observation::Drive`. The caller polls
    /// that future on its own thread; this function does not poll it.
    ///
    /// If a pickled reply is available and nobody has claimed it, give the
    /// caller the exclusive right to unpickle it.
    ///
    /// Return Pending while work is running or another observer is unpickling.
    /// For a finished operation, return the stored outcome or producer-loss
    /// status.
    #[inline]
    pub(super) fn observe_or_drive(self: &Arc<Self>) -> Observation {
        let mut state = self.lock();
        let observed = match &mut *state {
            State::Done(outcome) => Observed::Done(outcome.clone()),
            State::Lost => Observed::Lost,
            State::Running(_) => Observed::Pending,
            State::Raw(raw) => raw
                .try_claim(self)
                .map_or(Observed::Pending, Observed::Reconstruct),
            // get() takes both the future and publisher, then polls the future
            // on its own thread. Nothing is spawned here.
            //
            // get() runs the receive itself and keeps the core alive, so no
            // stop signal is needed now. hand_off() installs one if needed
            // when a timeout or interrupt moves the receive to Tokio.
            State::Unstarted(_) => return Observation::Drive(self.take_unstarted(state, None)),
        };
        Observation::Observed(observed)
    }

    /// Ensure an unstarted port or stream-reply receive is started on Tokio.
    /// Rust waiters call this before waiting for the shared result. If the
    /// operation is already running or finished, start nothing (HDL-24).
    /// This neither waits for completion nor consumes the stored result.
    ///
    /// Leave an already-arrived pickled reply unclaimed. Creating a Rust
    /// observer future does not claim it; polling that future may do so.
    pub(super) fn start(self: &Arc<Self>) {
        // Start deferred receive work, but never claim raw bytes for a Rust
        // observer before its returned future is polled.
        let state = self.lock();
        let State::Unstarted(deferred) = &*state else {
            return;
        };
        let observer_loss = deferred.observer_loss;
        self.spawn_unstarted(state, observer_loss);
    }

    /// Start an unstarted receive as a Tokio task (HDL-24). `state` is the
    /// guard of the check that found it unstarted; the lock is released before
    /// the task is scheduled (HDL-9).
    fn spawn_unstarted(
        self: &Arc<Self>,
        state: MutexGuard<'_, State>,
        observer_loss: ObserverLoss,
    ) {
        // Create a stop signal only for a receive that will run on Tokio and
        // must stop when its last observer disappears. An inline get() keeps
        // the core alive. If it times out or is interrupted, hand_off() moves
        // the unfinished receive to Tokio so work continues without that get().
        // A later get() can wait for the result; it does not run the receive.
        // hand_off() also installs a stop signal if this receive needs one.
        let stop = match observer_loss {
            // This receive will run on Tokio and must stop if nobody can
            // observe its result. Create the signal that Core::drop() sends
            // when the last Handle or active waiter releases the core.
            ObserverLoss::Stop => Some(Arc::new(Notify::new())),
            // A started stream-item receive continues without observers.
            // Its task needs no stop signal.
            ObserverLoss::Continue => None,
        };
        // Register for Core::Drop's stop notification before scheduling the
        // task. Otherwise the last observer could drop before registration,
        // leaving the receive running with nobody to read its result (HDL-23).
        let observer_stop = match &stop {
            Some(stop) => ObserverStop::WhenUnobserved(Arc::clone(stop).notified_owned()),
            None => ObserverStop::Never,
        };
        let drive = self.take_unstarted(state, stop);
        spawn_producer(settle(drive.future), drive.publisher, observer_stop);
    }

    /// Take an unstarted receive with its sole publisher, leaving the core
    /// Running with `stop`. `state` is the guard of the check that found the
    /// receive unstarted; this releases the lock before returning.
    fn take_unstarted(
        self: &Arc<Self>,
        mut state: MutexGuard<'_, State>,
        stop: Option<Arc<Notify>>,
    ) -> Drive {
        // mem::replace installs Running in the core and returns the old state.
        // Extract its Unstarted receive: deferred now owns that receive.
        // Other observers see Running and wait for the result instead of
        // starting another receive.
        // No wake signal is needed unless an observer actually waits (HDL-25).
        let State::Unstarted(deferred) = mem::replace(
            &mut *state,
            State::Running(Running {
                wake: WakeSlot::default(),
                stop,
            }),
        ) else {
            unreachable!("the state was Unstarted under this lock");
        };
        // Drop the mutex guard, not the core. This releases the state lock
        // before scheduling work or dropping Python objects in the receive (HDL-9).
        drop(state);

        // Give the receive its sole publisher before scheduling it. If a task
        // is dropped before its first poll, the publisher still drops and marks
        // the core Lost. A weak reference does not keep observers alive (HDL-10).
        let publisher = Publisher::new(Arc::downgrade(self), deferred.traceback);
        Drive {
            future: deferred.future,
            publisher,
            observer_loss: deferred.observer_loss,
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
    pub(super) fn hand_off(&self, drive: Drive) {
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
                    State::Running(running) => running.stop = Some(stop),
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
    pub(super) fn producer_traceback(&self) -> Option<Py<PyAny>> {
        self.traceback
            .as_ref()
            .map(|t| monarch_with_gil_blocking(GilSite::Traceback, |py| t.clone_ref(py)))
    }

    /// Store a producer's raw reply, final value/error, or producer loss,
    /// then wake observers waiting for progress (HDL-1, HDL-25, HDL-27).
    ///
    /// While the core is Running, only its producer may publish.
    /// Once a pickled reply is stored in Raw, only the observer holding its
    /// reconstruction claim may store the Python value or error. Done and
    /// Lost must never be replaced by another outcome (HDL-1, HDL-27).
    ///
    /// Moving from Running to Raw keeps the same wake signal, so existing
    /// waiters can also be notified when reconstruction finishes (HDL-25).
    ///
    /// Replace the state under the lock, but release it before notifying or
    /// dropping the old state's fields (HDL-9). Woken observers read the
    /// new state; the notification itself carries no result.
    fn finish(&self, mut next_state: State) {
        // Install the new state under the mutex, retaining the old state for
        // notification below. This block releases the mutex before returning.
        let previous_state = {
            let mut state_guard = self.lock();
            // The incoming Raw state starts with an empty WakeSlot. The stored
            // Running state may already have a Notify because an observer
            // registered to wait. Copy that notification into Raw before the
            // swap below, so it remains available for claim release and
            // completed unpickling as well as this reply's arrival.
            if let State::Raw(raw) = &mut next_state
                && let State::Running(running) = &*state_guard
            {
                // Some gives Raw another reference to the existing Notify.
                // None leaves Raw without a wake signal. No new Notify is
                // allocated in either case.
                raw.wake = running.wake.clone();
            }
            // Store next_state in the core and return its previous state.
            // Retaining that state lets us notify its waiters after unlocking.
            mem::replace(&mut *state_guard, next_state)
        };
        // Extract the wake signal outside the lock. Publication requires a
        // running producer or the observer holding the sole raw claim.
        let wake = match &previous_state {
            State::Running(Running { wake, .. })
            | State::Raw(Raw {
                claimed: true,
                wake,
                ..
            }) => wake,
            _ => panic!("HDL-1: only a producer or reconstruction claim publishes"),
        };
        // Tests can pause here: the new state is visible, but waiters have
        // not yet been notified.
        #[cfg(test)]
        run_test_hook(&BEFORE_NOTIFY);
        // The new state is already stored. Notify all registered waiters if a
        // wake signal was created; finishing alone never allocates one (HDL-25).
        wake.notify();
    }

    /// Whether a raw reply has arrived (`Some(true)`), the operation has
    /// settled (`Some(false)`), or neither (`None`), without claiming. For tests.
    #[cfg(test)]
    pub(super) fn received_raw_for_test(&self) -> Option<bool> {
        match &*self.lock() {
            State::Raw(_) => Some(true),
            State::Done(_) | State::Lost => Some(false),
            State::Unstarted(_) | State::Running(_) => None,
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
        if let State::Running(Running {
            stop: Some(stop), ..
        }) = operation_state
        {
            // Wake the task waiting for this signal. It stops the receive when
            // it next polls; this does not synchronously cancel the Tokio task.
            stop.notify_waiters();
        }
    }
}

/// Wait for a shared Python outcome, producer loss or a claim on available bytes.
/// Raw arrival wakes this waiter but is not itself completed readiness (HDL-27).
///
/// The core must already be Running, Raw, Done or Lost. The caller must start any
/// unstarted receive first; this function does not start it (HDL-24).
/// Its Arc keeps the core alive, so a running port receive cannot stop for lack
/// of observers while this future waits (HDL-23).
///
/// Check the result and register for completion under the same lock. Completion
/// then either has already happened or will notify this waiter, even before its
/// first poll of the notification future (HDL-25). Waiting takes no GIL.
pub(super) async fn wait_observed(core: Arc<Core>) -> WaitOutcome {
    loop {
        // The temporary guard lasts through this match, then releases the lock
        // before awaiting. Checking and registering together prevents completion
        // from slipping between the check and registration (HDL-25).
        let notified = match &mut *core.lock() {
            // Clone only the Arc, not the Python value or error. The caller
            // reads that value after unlocking, with the GIL if needed (HDL-9).
            State::Done(outcome) => return WaitOutcome::Done(Arc::clone(outcome)),
            State::Lost => return WaitOutcome::Lost,
            // Claim inside this same check/register lock, not in a separate
            // probe. Ready and running non-raw observers retain their cost.
            State::Raw(raw) => match raw.try_claim(&core) {
                Some(claim) => return WaitOutcome::Reconstruct(claim),
                None => Some(raw.wake.register()),
            },
            // Reuse the shared wake signal, allocating it only for the first
            // waiter. Each waiter creates its own OwnedNotified under this lock.
            State::Running(running) => Some(running.wake.register()),
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

/// Wait until the loop reader can make progress on this Handle.
///
/// Return when the Handle has a final outcome, its producer is lost, or a
/// pickled reply is available to claim. While the operation is still running,
/// or another observer holds the reconstruction claim, wait for the next
/// state change.
///
/// This function neither claims nor reconstructs the reply and never takes
/// the GIL. The asyncio pipe waiter uses it only to decide when to wake the
/// loop. The caller must start any deferred receive before calling it.
///
/// Check the state and register the wake under the same lock, so arrival,
/// claim release, and settlement cannot occur between those operations.
pub(super) async fn wait_arrival(core: Arc<Core>) {
    loop {
        let state_change = match &mut *core.lock() {
            State::Done(_) | State::Lost | State::Raw(Raw { claimed: false, .. }) => return,
            // Nothing is ready for the loop reader: either the producer is
            // still running or another observer is reconstructing the reply.
            // Reuse the state's shared Notify, creating it lazily for the first
            // waiter. Create this waiter's owned notification future while the
            // state lock is held; after it fires, loop back and check the state
            // again.
            State::Running(Running { wake, .. }) | State::Raw(Raw { wake, .. }) => {
                Some(wake.register())
            }
            State::Unstarted(_) => None,
        };
        #[cfg(test)]
        run_test_hook(&AFTER_REGISTER);
        state_change
            .expect("HDL-24: an observer starts the producer before it waits")
            .await;
    }
}

/// Wait for the Handle's final outcome when the event loop cannot use `add_reader`.
///
/// If a pickled reply arrives, claim and reconstruct it on this Tokio worker,
/// storing its value or error before returning. A result already stored, or
/// producer loss, requires no GIL while waiting. The caller later schedules
/// this outcome onto the loop with `call_soon_threadsafe`.
///
/// The fallback observer runs these two steps separately, so a release can end
/// its wait for an observation (HDL-21). Tests use this composition.
#[cfg(test)]
pub(super) async fn wait_terminal(core: Arc<Core>) -> PyResult<Arc<PyResult<Py<PyAny>>>> {
    settle_observed(wait_observed(core).await).await
}

/// Turn an observation into the Handle's final outcome: a stored result as
/// is, producer loss as its error, and a claimed raw reply reconstructed on
/// this Tokio worker. The fallback observer calls this after its releasable
/// wait for an observation (HDL-21).
pub(super) async fn settle_observed(observed: WaitOutcome) -> PyResult<Arc<PyResult<Py<PyAny>>>> {
    match observed {
        WaitOutcome::Done(outcome) => Ok(outcome),
        WaitOutcome::Lost => Err(handle_producer_ended_error()),
        WaitOutcome::Reconstruct(claim) => {
            monarch_with_gil(GilSite::ReplyConvert, move |py| {
                claim.reconstruct(py, ReconstructionFailurePolicy::StoreEveryFailureInHandle)
            })
            .await
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only per-thread count; no code or storage exists in production builds.
    pub(super) static STATE_LOCK_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Pause with the claim still held, after reconstruction and before Done.
    pub(super) static BEFORE_RECONSTRUCTION_SETTLE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Runs once in `wait_observed`, after a waiter registers under the lock
    /// and before it awaits (HDL-25).
    pub(super) static AFTER_REGISTER: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Runs once in `Core::finish`, after the transition and before the notify
    /// (HDL-25).
    pub(super) static BEFORE_NOTIFY: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
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
    pub(super) fn new(core: Weak<Core>, traceback: Option<Py<PyAny>>) -> Self {
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

    /// Supply successful bytes without reconstructing a Python value.
    /// Disarm publication before handing them to Raw; Drop must not publish Lost.
    pub(super) fn publish_raw(mut self, reply: Arc<dyn Reconstruct>) {
        let core = self.core.take().expect("HDL-1: a Publisher publishes once");
        if let Some(core) = core.upgrade() {
            core.finish(State::Raw(Raw {
                reply,
                claimed: false,
                wake: WakeSlot::default(),
            }));
        }
        // With no observers, drop the native payload without unpickling or a GIL.
    }

    /// Publish a result already held in an Arc. An inline get() uses this to
    /// publish and read the same outcome without allocating a second wrapper.
    pub(super) fn publish_shared(mut self, outcome: Arc<PyResult<Py<PyAny>>>) {
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
