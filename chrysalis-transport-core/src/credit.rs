/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::Notifier;

/// Shared admission budget for completions retained by one driver.
#[derive(Clone)]
pub struct CompletionCredits {
    inner: Arc<Inner>,
}

struct Inner {
    capacity: usize,
    used: AtomicUsize,
    notifier: Arc<dyn Notifier>,
}

/// One reserved terminal-completion slot.
pub struct CompletionPermit {
    inner: Arc<Inner>,
}

impl fmt::Debug for CompletionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CompletionPermit").finish()
    }
}

impl CompletionCredits {
    /// Constructs a completion-credit budget that wakes `notifier` on release.
    pub fn new(capacity: NonZeroUsize, notifier: Arc<dyn Notifier>) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity: capacity.get(),
                used: AtomicUsize::new(0),
                notifier,
            }),
        }
    }

    /// Reserves one completion slot without blocking.
    pub fn try_acquire(&self) -> Option<CompletionPermit> {
        let mut used = self.inner.used.load(Ordering::Acquire);
        loop {
            if used == self.inner.capacity {
                return None;
            }
            match self.inner.used.compare_exchange_weak(
                used,
                used + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(CompletionPermit {
                        inner: self.inner.clone(),
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }

    /// Returns the number of occupied completion slots.
    pub fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    /// Returns the configured completion limit.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl Drop for CompletionPermit {
    fn drop(&mut self) {
        let previous = self.inner.used.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous > 0,
            "completion credit release should not underflow"
        );
        self.inner.notifier.notify();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::NoopNotifier;

    #[test]
    fn permits_bound_and_release_completion_ownership() {
        let credits = CompletionCredits::new(NonZeroUsize::new(2).unwrap(), Arc::new(NoopNotifier));
        let first = credits.try_acquire().unwrap();
        let second = credits.try_acquire().unwrap();
        assert!(credits.try_acquire().is_none());
        assert_eq!(credits.used(), 2);

        drop(first);
        assert_eq!(credits.used(), 1);
        assert!(credits.try_acquire().is_some());
        drop(second);
    }
}
