/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module contains an implementation of the MVar synchronization
//! primitive.

use std::mem::take;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tokio::sync::watch;

/// An MVar is a primitive that combines synchronization and the exchange
/// of a value. Its semantics are analogous to a synchronous channel of
/// size 1: if the MVar is full, then `put` blocks until it is emptied;
/// if the MVar is empty, then `take` blocks until it is filled.
///
/// MVars, first introduced in "[Concurrent Haskell](https://www.microsoft.com/en-us/research/wp-content/uploads/1996/01/concurrent-haskell.pdf)"
/// are surprisingly versatile in use. They can be used as:
/// - a communication channel (with `put` and `take` corresponding to `send` and `recv`);
/// - a semaphore (with `put` and `take` corresponding to `signal` and `wait`);
/// - a mutex (with `put` and `take` corresponding to `lock` and `unlock`);
#[derive(Clone, Debug)]
pub struct MVar<T> {
    seq: watch::Sender<usize>,
    value: Arc<Mutex<Option<T>>>,
}

impl<T> MVar<T> {
    /// Create a new MVar with an optional initial value; if no value is
    /// provided the MVar starts empty.
    fn new(init: Option<T>) -> Self {
        let (seq, _) = watch::channel(0);
        Self {
            seq,
            value: Arc::new(Mutex::new(init)),
        }
    }

    /// Create a new full MVar with the provided value.
    pub fn full(value: T) -> Self {
        Self::new(Some(value))
    }

    /// Create a new empty MVar.
    pub fn empty() -> Self {
        Self::new(None)
    }

    async fn waitseq(&self, seq: usize) -> (MutexGuard<'_, Option<T>>, usize) {
        let mut sub = self.seq.subscribe();
        while *sub.borrow_and_update() < seq {
            sub.changed().await.unwrap();
        }
        let locked = self.value.lock().await;
        let seq = *sub.borrow_and_update();
        (locked, seq + 1)
    }

    fn notify(&self, seq: usize) {
        self.seq.send_replace(seq);
    }

    /// Wait until the MVar is full and take its value.
    /// This method is cancellation safe.
    pub async fn take(&self) -> T {
        let mut seq = 0;
        loop {
            let mut value;
            (value, seq) = self.waitseq(seq).await;

            if let Some(current_value) = take(&mut *value) {
                self.notify(seq);
                break current_value;
            }
            drop(value);
        }
    }

    /// Wait until the MVar is empty and put a new value.
    /// This method is cancellation safe.
    pub async fn put(&self, new_value: T) {
        let mut seq = 0;
        loop {
            let mut value;
            (value, seq) = self.waitseq(seq).await;

            if value.is_none() {
                *value = Some(new_value);
                self.notify(seq);
                break;
            }
            drop(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mvar() {
        let mv0 = MVar::full(0);
        let mv1 = MVar::empty();

        assert_eq!(mv0.take().await, 0);

        tokio::spawn({
            let mv0 = mv0.clone();
            let mv1 = mv1.clone();
            async move { mv1.put(mv0.take().await).await }
        });

        mv0.put(1).await;
        assert_eq!(mv1.take().await, 1);
    }
}
