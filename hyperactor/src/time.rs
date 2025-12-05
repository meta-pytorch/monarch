/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module contains various utilities for dealing with time.
//! (This probably belongs in a separate crate.)

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tokio::time::sleep_until;

use crate::clock::Clock;
use crate::clock::RealClock;

/// An alarm that can be Armed to fire at some future time.
///
/// Alarm is itself owned, and may have multiple sleepers attached
/// to it. Each sleeper is awoken at most once for each alarm that has
/// been set.
///
/// When instances of `Alarm` are dropped, sleepers are awoken,
/// returning `false`, indicating that the alarm is defunct.
pub struct Alarm {
    status: Arc<Mutex<AlarmStatus>>,
    notify: Arc<Notify>,
    version: usize,
}
enum AlarmStatus {
    Unarmed,
    Armed { version: usize, deadline: Instant },
    Dropped,
}

impl Alarm {
    /// Create a new, unset alarm.
    pub fn new() -> Self {
        Self {
            status: Arc::new(Mutex::new(AlarmStatus::Unarmed)),
            notify: Arc::new(Notify::new()),
            version: 0,
        }
    }

    /// Arm the alarm to fire after the provided duration.
    pub fn arm(&mut self, duration: Duration) {
        let mut status = self.status.lock().unwrap();
        *status = AlarmStatus::Armed {
            version: self.version,
            deadline: RealClock.now() + duration,
        };
        drop(status);
        self.notify.notify_waiters();
        self.version += 1;
    }

    /// Disarm the alarm, canceling any pending alarms.
    pub fn disarm(&mut self) {
        let mut status = self.status.lock().unwrap();
        *status = AlarmStatus::Unarmed;
        drop(status);
        // Not technically needed (sleepers will still converge),
        // but this clears up the timers:
        self.notify.notify_waiters();
    }

    /// Fire the alarm immediately.
    pub fn fire(&mut self) {
        self.arm(Duration::from_millis(0))
    }

    /// Create a new sleeper for this alarm. Many sleepers can wait for the alarm
    /// to fire at any given time.
    pub fn sleeper(&self) -> AlarmSleeper {
        AlarmSleeper {
            status: Arc::clone(&self.status),
            notify: Arc::clone(&self.notify),
            min_version: 0,
        }
    }
}

impl Drop for Alarm {
    fn drop(&mut self) {
        let mut status = self.status.lock().unwrap();
        *status = AlarmStatus::Dropped;
        drop(status);
        self.notify.notify_waiters();
    }
}

impl Default for Alarm {
    fn default() -> Self {
        Self::new()
    }
}

/// A single alarm sleeper.
pub struct AlarmSleeper {
    status: Arc<Mutex<AlarmStatus>>,
    notify: Arc<Notify>,
    min_version: usize,
}

impl AlarmSleeper {
    /// Sleep until the alarm fires. Returns true if the alarm fired,
    /// and false if the alarm has been dropped.
    ///
    /// Sleep will fire (return true) at most once for each time the
    /// alarm is set.
    pub async fn sleep(&mut self) -> bool {
        loop {
            // Obtain a notifier before checking the state, to avoid the unlock-notify race.
            let notified = self.notify.notified();
            let deadline = match *self.status.lock().unwrap() {
                AlarmStatus::Dropped => return false,
                AlarmStatus::Unarmed => None,
                AlarmStatus::Armed { version, .. } if version < self.min_version => None,
                AlarmStatus::Armed { version, deadline } if RealClock.now() >= deadline => {
                    self.min_version = version + 1;
                    return true;
                }
                AlarmStatus::Armed {
                    version: _,
                    deadline,
                } => Some(deadline),
            };

            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = sleep_until(deadline) => (),
                    _ = notified => (),
                }
            } else {
                notified.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio_test::assert_pending;
    use tokio_test::task;

    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let mut alarm = Alarm::new();
        let mut sleeper = alarm.sleeper();
        let handle = tokio::spawn(async move { sleeper.sleep().await });
        assert!(!handle.is_finished()); // not super meaningful..

        alarm.fire();

        assert!(handle.await.unwrap());

        let mut sleeper = alarm.sleeper();
        alarm.arm(Duration::from_mins(10));
        let handle = tokio::spawn(async move { sleeper.sleep().await });
        drop(alarm);
        // Dropped:
        assert!(!handle.await.unwrap());
    }

    #[tokio::test]
    async fn test_sleep_once() {
        let mut alarm = Alarm::new();
        alarm.fire();
        let mut sleeper = alarm.sleeper();
        assert!(sleeper.sleep().await);

        // Don't wake up again:
        assert_pending!(task::spawn(sleeper.sleep()).poll());
        alarm.fire();
        assert!(sleeper.sleep().await);
        // Don't wake up again:
        assert_pending!(task::spawn(sleeper.sleep()).poll());
        drop(alarm);
        assert!(!sleeper.sleep().await);
    }

    #[tokio::test]
    async fn test_reset() {
        let mut alarm = Alarm::new();
        alarm.arm(Duration::from_mins(10));
        let mut sleeper = alarm.sleeper();
        assert_pending!(task::spawn(sleeper.sleep()).poll());
        // Should reset after setting to an earlier time:
        alarm.arm(Duration::from_millis(10));
        assert!(sleeper.sleep().await);
    }

    #[tokio::test]
    async fn test_disarm() {
        let mut alarm = Alarm::new();
        alarm.arm(Duration::from_mins(10));
        let mut sleeper = alarm.sleeper();
        assert_pending!(task::spawn(sleeper.sleep()).poll());
        alarm.disarm();
        assert_pending!(task::spawn(sleeper.sleep()).poll());
        alarm.arm(Duration::from_millis(10));
        assert!(sleeper.sleep().await);
    }
}
