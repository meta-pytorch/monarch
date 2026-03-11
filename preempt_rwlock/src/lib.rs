/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![feature(future_join)]

use std::sync::Arc;

use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use tokio::sync::TryLockError;
use tokio::sync::watch;

pub struct PreemptibleRwLockReadGuard<'a, T: Sized> {
    preemptor: &'a watch::Receiver<usize>,
    guard: RwLockReadGuard<'a, T>,
}

impl<'a, T: Sized> PreemptibleRwLockReadGuard<'a, T> {
    pub async fn preempted(&self) {
        let mut preemptor = self.preemptor.clone();
        // Wait for pending writers.
        preemptor.wait_for(|&v| v > 0).await.expect("wait_for fail");
    }
}

impl<T: Sized> std::ops::Deref for PreemptibleRwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.deref()
    }
}

pub struct OwnedPreemptibleRwLockReadGuard<T: ?Sized, U: ?Sized = T> {
    lock: Arc<PreemptibleRwLock<T>>,
    preemptor: watch::Receiver<usize>,
    guard: OwnedRwLockReadGuard<T, U>,
}

impl<T: ?Sized, U: ?Sized> OwnedPreemptibleRwLockReadGuard<T, U> {
    pub async fn preempted(&self) {
        let mut preemptor = self.preemptor.clone();
        // Wait for pending writers.
        preemptor.wait_for(|&v| v > 0).await.expect("wait_for fail");
    }

    /// Maps the data guarded by this lock with a function.
    ///
    /// This is similar to the `map` method on `OwnedRwLockReadGuard`, but preserves
    /// the preemption capability.
    pub fn map<F, V>(self, f: F) -> OwnedPreemptibleRwLockReadGuard<T, V>
    where
        F: FnOnce(&U) -> &V,
    {
        OwnedPreemptibleRwLockReadGuard {
            lock: self.lock,
            preemptor: self.preemptor,
            guard: OwnedRwLockReadGuard::map(self.guard, f),
        }
    }
}

impl<T: ?Sized, U: ?Sized> std::ops::Deref for OwnedPreemptibleRwLockReadGuard<T, U> {
    type Target = U;
    fn deref(&self) -> &U {
        self.guard.deref()
    }
}

pub struct PreemptibleRwLockWriteGuard<'a, T: Sized> {
    preemptor: &'a watch::Sender<usize>,
    guard: RwLockWriteGuard<'a, T>,
    preempt_readers: bool,
}

impl<'a, T: Sized> Drop for PreemptibleRwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        if self.preempt_readers {
            self.preemptor.send_if_modified(|v| {
                *v -= 1;
                // No need to send a change event when decrementing.
                false
            });
        }
    }
}

impl<T: Sized> std::ops::Deref for PreemptibleRwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.deref()
    }
}

impl<T: Sized> std::ops::DerefMut for PreemptibleRwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

/// A RW-lock which also supports a way for pending writers to request that
/// readers get preempted, via `preempted()` method on the read guard that
/// readers can `tokio::select!` on.
#[derive(Debug)]
pub struct PreemptibleRwLock<T: ?Sized> {
    lock: Arc<RwLock<T>>,
    preemptor_lock: RwLock<()>,
    // Used to track the number of writers waiting to acquire the lock and
    // allows readers to `await` on updates to this value to support preemption.
    preemptor: (watch::Sender<usize>, watch::Receiver<usize>),
}

impl<T: Sized> PreemptibleRwLock<T> {
    pub fn new(item: T) -> Self {
        PreemptibleRwLock {
            lock: Arc::new(RwLock::new(item)),
            preemptor_lock: RwLock::new(()),
            preemptor: watch::channel(0),
        }
    }

    pub async fn read<'a>(&'a self) -> PreemptibleRwLockReadGuard<'a, T> {
        let _guard = self.preemptor_lock.read().await;
        PreemptibleRwLockReadGuard {
            preemptor: &self.preemptor.1,
            guard: self.lock.read().await,
        }
    }

    pub async fn read_owned(self: Arc<Self>) -> OwnedPreemptibleRwLockReadGuard<T> {
        let _guard = self.preemptor_lock.read().await;
        OwnedPreemptibleRwLockReadGuard {
            lock: self.clone(),
            preemptor: self.preemptor.1.clone(),
            guard: self.lock.clone().read_owned().await,
        }
    }

    pub fn try_read_owned(
        self: Arc<Self>,
    ) -> Result<OwnedPreemptibleRwLockReadGuard<T>, TryLockError> {
        let _guard = self.preemptor_lock.try_read()?;
        Ok(OwnedPreemptibleRwLockReadGuard {
            lock: self.clone(),
            preemptor: self.preemptor.1.clone(),
            guard: self.lock.clone().try_read_owned()?,
        })
    }

    pub async fn write<'a>(&'a self, preempt_readers: bool) -> PreemptibleRwLockWriteGuard<'a, T> {
        let _guard = self.preemptor_lock.write().await;
        if preempt_readers {
            self.preemptor.0.send_if_modified(|v| {
                *v += 1;
                // Only send a change event if we're the first pending writer.
                *v == 1
            });
        }
        PreemptibleRwLockWriteGuard {
            preemptor: &self.preemptor.0,
            guard: self.lock.write().await,
            preempt_readers,
        }
    }
    pub fn try_write<'a>(
        &'a self,
        preempt_readers: bool,
    ) -> Result<PreemptibleRwLockWriteGuard<'a, T>, TryLockError> {
        let _guard = self.preemptor_lock.try_write()?;
        if preempt_readers {
            self.preemptor.0.send_if_modified(|v| {
                *v += 1;
                // Only send a change event if we're the first pending writer.
                *v == 1
            });
        }
        Ok(PreemptibleRwLockWriteGuard {
            preemptor: &self.preemptor.0,
            guard: self.lock.try_write()?,
            preempt_readers,
        })
    }

    pub fn blocking_write<'a>(
        &'a self,
        preempt_readers: bool,
    ) -> PreemptibleRwLockWriteGuard<'a, T> {
        let _guard = self.preemptor_lock.blocking_write();
        if preempt_readers {
            self.preemptor.0.send_if_modified(|v| {
                *v += 1;
                // Only send a change event if we're the first pending writer.
                *v == 1
            });
        }
        PreemptibleRwLockWriteGuard {
            preemptor: &self.preemptor.0,
            guard: self.lock.blocking_write(),
            preempt_readers,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::join;
    use std::time::Duration;

    use anyhow::Result;

    use super::*;

    #[tokio::test]
    async fn test_preempt_reader() -> Result<()> {
        let lock = PreemptibleRwLock::new(());

        let reader = lock.read().await;

        join!(
            async move {
                loop {
                    tokio::select!(
                        _ = reader.preempted() => break,
                        _ = tokio::time::sleep(Duration::from_secs(100)) => (),
                    )
                }
            },
            lock.write(/* preempt_readers */ true),
        )
        .await;

        Ok(())
    }

    #[tokio::test]
    async fn test_preempt_owned_reader_after_lock_dropped() -> Result<()> {
        let lock = Arc::new(PreemptibleRwLock::new(42));

        // Get an owned read guard
        let reader = lock.clone().read_owned().await;

        // Drop the original Arc<PreemptibleRwLock>
        drop(lock);

        // Verify that preempted() still works even after the original Arc is dropped
        tokio::select! {
            biased;  // Make sure peempted is checked first.
            _ = reader.preempted() => assert!(false),
            _ = futures::future::ready(()) => (),
        }

        Ok(())
    }
}
