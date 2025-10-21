/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module provides a simple watchdog, designed to monitor the progress
//! of asynchronous code.

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio::time::sleep_until;
use tracing::Span;

use crate::clock::Clock;
use crate::clock::RealClock;

/// A watchdog keeps track of caller check-ins, failing when
/// the caller has not checked in for a configurable period of time.
pub struct Watchdog {
    interval: tokio::time::Interval,
    timeout: std::time::Duration,
    tx: watch::Sender<(tokio::time::Instant, Span)>,
}

impl Watchdog {
    /// Spawn a new watchdog which times out after the given timeout value.
    /// The user must call `send` (or use `tick`) periodically to keep the watchdog
    /// alive.
    ///
    /// Calling `spawn` is considered the first check-in to the watch dog.
    ///
    /// When the watchdog times out, it logs an error message to the span captured at the
    /// last check-in.
    pub fn spawn(timeout: std::time::Duration) -> Self {
        let now = RealClock.now();
        let (tx, rx) = watch::channel((now, Span::current()));

        tokio::spawn(Self::watcher(timeout, rx));
        let mut interval = tokio::time::interval(timeout / 2);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            interval,
            timeout,
            tx,
        }
    }

    /// Tick checks in with the watchdog, and then returns when it is
    /// time to check in again. The intent of `tick` is to use it in a loop
    /// (usually as a branch of a [`tokio::select`]).
    pub async fn tick(&mut self) {
        self.send();
        self.interval.tick().await;
    }

    /// Check in with the watchdog. Returns true if the watchdog was
    /// considered alive prior to the update.
    pub fn send(&mut self) -> bool {
        let was_ok = self.ok();
        self.tx.send((RealClock.now(), Span::current())).unwrap();
        !was_ok
    }

    async fn watcher(
        timeout: std::time::Duration,
        mut rx: watch::Receiver<(tokio::time::Instant, Span)>,
    ) {
        let mut ok = true;

        loop {
            let now = RealClock.now();
            ok = {
                let time_and_span = rx.borrow_and_update();

                let since_last_alive = now.duration_since(time_and_span.0);
                let is_timed_out = since_last_alive > timeout;

                // This is a timed out state.
                if ok && is_timed_out {
                    tracing::error!(parent: &time_and_span.1, ?since_last_alive, "watchdog timed out");
                } else if !ok && !is_timed_out {
                    tracing::error!(parent: &time_and_span.1, "watchdog recovered");
                }

                !is_timed_out
            };

            tokio::select! {
                // We only need to check again if we're currently okay;
                // otherwise we wait for an update.
                _ = sleep_until(RealClock.now() + timeout), if ok => (),
                changed = rx.changed() => {
                    if changed.is_err() {
                        break
                    }
                }
            }
        }
    }
}

impl Watchdog {
    /// Returns the time of the last check-in.
    pub fn last_alive(&self) -> tokio::time::Instant {
        self.tx.borrow().0
    }

    /// Returns whether the watchdog is considered to be in a
    /// healthy state.
    pub fn ok(&self) -> bool {
        self.last_alive().elapsed() <= self.timeout
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tracing::Level;
    use tracing::span;
    use tracing_test::traced_test;

    use super::*;

    #[traced_test]
    #[tokio::test(start_paused = true)]
    async fn test_basic() {
        let span = span!(Level::INFO, "a test", test = true);
        let _guard = span.enter();

        let mut w = Watchdog::spawn(Duration::from_secs(5));

        w.send();
        assert!(w.ok());
        // tokio::time::sleep(Duration::from_secs(6)).await;
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(!w.ok());
        w.send();
        // tokio::time::sleep(Duration::from_secs(1)).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(w.ok());

        // Give the loop some time to catch up again.
        // tokio::time::sleep(Duration::from_secs(5)).await;
        tokio::time::advance(Duration::from_secs(5)).await;

        // TODO: not sure why these log assertions don't work;
        // I have verified manually that the test emits them.
        // May have to do with using paused time.
        assert!(logs_contain("watchdog timed out"));
        assert!(logs_contain("watchdog recovered"));
    }

    #[tokio::test(start_paused = true)]
    async fn test_tick_then_failure() {
        let timeout = Duration::from_secs(2);
        let start = RealClock.now();
        let mut w = Watchdog::spawn(timeout);
        w.tick().await; // first tick completes instantly
        assert_eq!(RealClock.now(), start);
        w.tick().await;
        assert_eq!(RealClock.now(), start + timeout / 2);
        assert_eq!(w.last_alive(), start);

        tokio::time::advance(timeout / 4).await;
        // We are now overdue for a tick, but not timed out.
        assert!(w.ok());
        assert_eq!(w.last_alive(), start);

        w.tick().await;
        assert_eq!(RealClock.now(), start + timeout);

        // It is also okay to completely skip an interval.
        // We'll then catch up again.
        tokio::time::advance(timeout * 2).await;
        assert!(!w.ok());
        assert_eq!(RealClock.now(), start + timeout * 3);
        // Recovery resets the ticks:
        w.tick().await;
        assert!(w.ok());
        assert_eq!(RealClock.now(), start + timeout * 3);
        w.tick().await;
        assert_eq!(RealClock.now(), start + timeout * 3 + timeout / 2);
    }
}
