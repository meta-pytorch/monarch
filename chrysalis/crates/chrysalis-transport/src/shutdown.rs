/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::task::Waker;

use tokio::sync::Notify;

const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const TERMINATED: u8 = 2;

/// Shared state for idempotent shutdown and multi-waiter join.
#[derive(Debug, Default)]
pub(crate) struct ShutdownState {
    state: AtomicU8,
    shutdown: Notify,
    terminated: Notify,
    wakers: Mutex<Vec<Waker>>,
}

impl ShutdownState {
    pub(crate) fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == RUNNING
    }

    pub(crate) fn shutdown(&self) -> bool {
        let changed = self
            .state
            .compare_exchange(RUNNING, SHUTTING_DOWN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if changed {
            self.shutdown.notify_waiters();
            self.wake_all();
        }
        changed
    }

    pub(crate) async fn cancelled(&self) {
        loop {
            let notified = self.shutdown.notified();
            if !self.is_running() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn terminate(&self) {
        if self.state.swap(TERMINATED, Ordering::AcqRel) != TERMINATED {
            self.shutdown.notify_waiters();
            self.terminated.notify_waiters();
            self.wake_all();
        }
    }

    pub(crate) async fn join(&self) {
        loop {
            let notified = self.terminated.notified();
            if self.state.load(Ordering::Acquire) == TERMINATED {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn register_waker(&self, waker: &Waker) {
        let mut registered = self
            .wakers
            .lock()
            .expect("shutdown state waker lock poisoned");
        if !registered.iter().any(|current| current.will_wake(waker)) {
            registered.push(waker.clone());
        }
    }

    fn wake_all(&self) {
        let wakers = std::mem::take(
            &mut *self
                .wakers
                .lock()
                .expect("shutdown state waker lock poisoned"),
        );
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Marks shutdown complete when its owning task exits or is aborted.
#[derive(Debug)]
pub(crate) struct CompletionGuard<'a> {
    shutdown_state: &'a ShutdownState,
}

impl<'a> CompletionGuard<'a> {
    pub(crate) fn new(shutdown_state: &'a ShutdownState) -> Self {
        Self { shutdown_state }
    }
}

impl Drop for CompletionGuard<'_> {
    fn drop(&mut self) {
        self.shutdown_state.terminate();
    }
}
