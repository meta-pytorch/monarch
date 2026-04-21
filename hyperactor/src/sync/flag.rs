/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A simple flagging mechanism to coordinate between tasks.
//!
//! ```
//! # use hyperactor::sync::flag;
//!
//! # tokio_test::block_on(async {
//! let (flag, guard) = flag::guarded();
//! assert!(!flag.signalled());
//! let (flag1, guard1) = flag::guarded();
//! tokio::spawn(async move {
//!     let _guard = guard;
//!     flag1.await;
//! });
//! drop(guard1);
//! flag.await
//! # })
//! ```

use std::future::Future;
use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;

/// Create a new guarded flag. The flag obtains when the guard is dropped.
pub fn guarded() -> (Flag, Guard) {
    let state = Arc::new(Default::default());
    let flag = Flag(Arc::clone(&state));
    let guard = Guard(state);
    (flag, guard)
}

#[derive(Debug, Default)]
struct State {
    flagged: AtomicBool,
    notify: Notify,
}

impl State {
    fn set(&self) {
        self.flagged.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    fn get(&self) -> bool {
        self.flagged.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        if !self.flagged.load(Ordering::SeqCst) {
            self.notify.notified().await;
        }
    }
}

/// A flag indicating that an event occured. Flags can be queried and awaited.
#[derive(Debug)]
pub struct Flag(Arc<State>);

impl Flag {
    /// Returns true if the flag has been set.
    pub fn signalled(&self) -> bool {
        self.0.get()
    }
}

impl IntoFuture for Flag {
    type Output = ();
    type IntoFuture = impl Future<Output = Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        async move { self.0.wait().await }
    }
}

/// A guard that sets the flag when dropped.
#[derive(Debug)]
pub struct Guard(Arc<State>);

impl Guard {
    /// Sets the flag. This is equivalent to `drop(guard)`, but
    /// conveys the intent more clearly.
    pub fn signal(self) {}
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.set();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let (flag, guard) = guarded();
        assert!(!flag.signalled());

        guard.signal();
        assert!(flag.signalled());

        flag.await;
    }

    #[tokio::test]
    async fn test_basic_running_await() {
        let (flag, guard) = guarded();

        let handle = tokio::spawn(async move {
            flag.await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        guard.signal();
        handle.await.unwrap();
    }
}
