/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Bounded, nonblocking MPSC queues for transport submissions and completions.
//!
//! This module combines lifecycle and notification semantics that standard
//! channels do not expose together. A failed push returns ownership of its
//! value, the receiver can close admission while retaining accepted values for
//! draining, and `Sender::try_push_and_close` atomically accepts a terminal
//! value and closes further admission. The pluggable `Notifier` and monotonic
//! sequence counter let eventfd-, runtime-, or callback-driven consumers detect
//! work without coupling this crate to a blocking or async runtime. The queue
//! can also provide one FIFO ordering domain to otherwise distinct producer
//! APIs.
//!
//! `VecDeque` is a circular buffer and is preallocated for the queue's fixed
//! logical capacity, so steady-state operations do not allocate. The mutex
//! provides straightforward MPSC linearization, exact FIFO order, and simple
//! close semantics. Notifications happen after releasing the mutex and are
//! coalesced on empty-to-nonempty transitions.
//!
//! The trade-off is that every producer and the single consumer contend on one
//! lock, while repeated single-item pops reacquire it. If profiling identifies
//! this queue as a bottleneck, batched draining is the simplest first
//! optimization. A lock-free bounded MPSC ring could reduce contention further,
//! but would have to preserve publication order, atomic terminal close,
//! producer lifetime tracking, safe destruction of owned values, and
//! lost-wakeup prevention.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::Notifier;

pub(crate) enum TryPushError<T> {
    Full(T),
    Closed(T),
}

pub(crate) struct Sender<T> {
    inner: Arc<Inner<T>>,
}

pub(crate) struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    capacity: usize,
    state: Mutex<State<T>>,
    sequence: AtomicU64,
    notifier: Arc<dyn Notifier>,
}

struct State<T> {
    values: VecDeque<T>,
    senders: usize,
    receiver_open: bool,
}

pub(crate) fn bounded<T>(
    capacity: NonZeroUsize,
    notifier: Arc<dyn Notifier>,
) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        capacity: capacity.get(),
        state: Mutex::new(State {
            values: VecDeque::with_capacity(capacity.get()),
            senders: 1,
            receiver_open: true,
        }),
        sequence: AtomicU64::new(0),
        notifier,
    });
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    pub(crate) fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned");
        if !state.receiver_open {
            return Err(TryPushError::Closed(value));
        }
        if state.values.len() == self.inner.capacity {
            return Err(TryPushError::Full(value));
        }

        let notify = state.values.is_empty();
        state.values.push_back(value);
        self.inner.sequence.fetch_add(1, Ordering::Release);
        drop(state);
        if notify {
            self.inner.notifier.notify();
        }
        Ok(())
    }

    pub(crate) fn try_push_and_close(&self, value: T) -> Result<(), TryPushError<T>> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned");
        if !state.receiver_open {
            return Err(TryPushError::Closed(value));
        }
        if state.values.len() == self.inner.capacity {
            return Err(TryPushError::Full(value));
        }

        state.values.push_back(value);
        state.receiver_open = false;
        self.inner.sequence.fetch_add(1, Ordering::Release);
        drop(state);
        self.inner.notifier.notify();
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> bool {
        !self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned")
            .receiver_open
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned");
        state.senders = state
            .senders
            .checked_add(1)
            .expect("queue sender count should not overflow");
        drop(state);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned");
        state.senders = state
            .senders
            .checked_sub(1)
            .expect("queue sender count should remain positive while a sender exists");
        let notify = state.senders == 0;
        if notify {
            self.inner.sequence.fetch_add(1, Ordering::Release);
        }
        drop(state);
        if notify {
            self.inner.notifier.notify();
        }
    }
}

impl<T> Receiver<T> {
    pub(crate) fn close(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned");
        if !state.receiver_open {
            return;
        }
        state.receiver_open = false;
        self.inner.sequence.fetch_add(1, Ordering::Release);
        drop(state);
        self.inner.notifier.notify();
    }

    pub(crate) fn try_pop(&self) -> Option<T> {
        self.inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned")
            .values
            .pop_front()
    }

    pub(crate) fn len(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned")
            .values
            .len()
    }

    pub(crate) fn is_closed(&self) -> bool {
        let state = self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned");
        state.senders == 0 && state.values.is_empty()
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.inner.sequence.load(Ordering::Acquire)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("queue mutex should not be poisoned");
        let notify = state.receiver_open;
        state.receiver_open = false;
        state.values.clear();
        if notify {
            self.inner.sequence.fetch_add(1, Ordering::Release);
        }
        drop(state);
        if notify {
            self.inner.notifier.notify();
        }
    }
}
