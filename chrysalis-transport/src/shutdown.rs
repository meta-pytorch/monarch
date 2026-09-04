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
    terminated: Notify,
    wakers: Mutex<ShutdownWakers>,
}

#[derive(Debug, Default)]
struct ShutdownWakers {
    send: Option<Waker>,
    receive: Option<Waker>,
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
            self.wake_all();
        }
        changed
    }

    pub(crate) fn terminate(&self) {
        if self.state.swap(TERMINATED, Ordering::AcqRel) != TERMINATED {
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

    pub(crate) fn register_send_waker(&self, waker: &Waker) {
        let mut registered = self
            .wakers
            .lock()
            .expect("shutdown state waker lock poisoned");
        if registered
            .send
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            registered.send = Some(waker.clone());
        }
    }

    pub(crate) fn register_receive_waker(&self, waker: &Waker) {
        let mut registered = self
            .wakers
            .lock()
            .expect("shutdown state waker lock poisoned");
        if registered
            .receive
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            registered.receive = Some(waker.clone());
        }
    }

    fn wake_all(&self) {
        let wakers = std::mem::take(
            &mut *self
                .wakers
                .lock()
                .expect("shutdown state waker lock poisoned"),
        );
        for waker in [wakers.send, wakers.receive].into_iter().flatten() {
            waker.wake();
        }
    }
}
