/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The clock allows us to control the behaviour of all time dependent events in both real and simulated time throughout the system

use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;

use crate::Mailbox;
use crate::channel::ChannelAddr;
use crate::id;
use crate::simnet::SleepEvent;
use crate::simnet::simnet_handle;

struct SimTime {
    start: tokio::time::Instant,
    now: Mutex<tokio::time::Instant>,
    system_start: SystemTime,
}

#[allow(clippy::disallowed_methods)]
static SIM_TIME: LazyLock<SimTime> = LazyLock::new(|| {
    let now = tokio::time::Instant::now();
    SimTime {
        start: now.clone(),
        now: Mutex::new(now),
        system_start: SystemTime::now(),
    }
});

/// The Sleeps trait allows different implementations to control the behavior of sleep.
pub trait Clock {
    /// Initiates a sleep for the specified duration
    fn sleep(
        &self,
        duration: tokio::time::Duration,
    ) -> impl std::future::Future<Output = ()> + Send + Sync;
    /// Get the current time according to the clock
    fn now(&self) -> tokio::time::Instant;
    /// Sleep until the specified deadline.
    fn sleep_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> impl std::future::Future<Output = ()> + Send + Sync;
    /// Get the current system time according to the clock
    fn system_time_now(&self) -> SystemTime;
}

/// An adapter that allows us to control the behaviour of sleep between performing a real sleep
/// and a sleep on the simnet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClockKind {
    /// Simulates a clock that uses the simnet's current time as the source of truth
    Sim(SimClock),
    /// Represents a real clock using tokio's sleep functionality for production use.
    Real(RealClock),
}

impl Clock for ClockKind {
    async fn sleep(&self, duration: tokio::time::Duration) {
        match self {
            Self::Sim(clock) => clock.sleep(duration).await,
            Self::Real(clock) => clock.sleep(duration).await,
        }
    }
    async fn sleep_until(&self, deadline: tokio::time::Instant) {
        match self {
            Self::Sim(clock) => clock.sleep_until(deadline).await,
            Self::Real(clock) => clock.sleep_until(deadline).await,
        }
    }
    fn now(&self) -> tokio::time::Instant {
        match self {
            Self::Sim(clock) => clock.now(),
            Self::Real(clock) => clock.now(),
        }
    }
    fn system_time_now(&self) -> SystemTime {
        match self {
            Self::Sim(clock) => clock.system_time_now(),
            Self::Real(clock) => clock.system_time_now(),
        }
    }
}

impl Default for ClockKind {
    fn default() -> Self {
        Self::Real(RealClock)
    }
}

impl ClockKind {
    /// Returns the appropriate clock given the channel address kind
    /// a proc is being served on
    pub fn for_channel_addr(channel_addr: &ChannelAddr) -> Self {
        match channel_addr {
            ChannelAddr::Sim(_) => Self::Sim(SimClock),
            _ => Self::Real(RealClock),
        }
    }
}

/// Clock to be used in simulator runs that allows the simnet to create a scheduled event for.
/// When the wakeup event becomes the next earliest scheduled event, the simnet will advance it's
/// time to the wakeup time and use the transmitter to wake up this green thread
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimClock;

impl Clock for SimClock {
    /// Tell the simnet to wake up this green thread after the specified duration has pass on the simnet
    async fn sleep(&self, duration: tokio::time::Duration) {
        let mailbox = Mailbox::new_detached(id!(proc[0].proc).clone());
        let (tx, rx) = mailbox.open_once_port::<()>();

        simnet_handle()
            .unwrap()
            .send_event(SleepEvent::new(
                tx.bind(),
                mailbox,
                duration.as_millis() as u64,
            ))
            .unwrap();
        rx.recv().await.unwrap();
    }
    async fn sleep_until(&self, deadline: tokio::time::Instant) {
        let now = self.now();
        if deadline <= now {
            return;
        }
        self.sleep(deadline - now).await;
    }
    /// Get the current time according to the simnet
    fn now(&self) -> tokio::time::Instant {
        SIM_TIME.now.lock().unwrap().clone()
    }

    fn system_time_now(&self) -> SystemTime {
        SIM_TIME.system_start.clone() + self.now().duration_since(SIM_TIME.start)
    }
}

impl SimClock {
    /// Advance the sumulator's time to the specified instant
    pub fn advance_to(&self, millis: u64) {
        let mut guard = SIM_TIME.now.lock().unwrap();
        *guard = SIM_TIME.start + tokio::time::Duration::from_millis(millis);
    }

    /// Get the number of milliseconds elapsed since the start of the simulation
    pub fn millis_since_start(&self, instant: tokio::time::Instant) -> u64 {
        instant.duration_since(SIM_TIME.start).as_millis() as u64
    }
}

/// An adapter for tokio::time::sleep to be used in production
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealClock;

impl Clock for RealClock {
    #[allow(clippy::disallowed_methods)]
    async fn sleep(&self, duration: tokio::time::Duration) {
        tokio::time::sleep(duration).await;
    }
    #[allow(clippy::disallowed_methods)]
    async fn sleep_until(&self, deadline: tokio::time::Instant) {
        tokio::time::sleep_until(deadline).await;
    }
    /// Get the current time using tokio::time::Instant
    #[allow(clippy::disallowed_methods)]
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }
    #[allow(clippy::disallowed_methods)]
    fn system_time_now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[cfg(test)]
mod tests {
    use crate::clock::Clock;
    use crate::clock::SimClock;

    #[tokio::test]
    async fn test_sim_clock_simple() {
        let start = SimClock.now();
        assert_eq!(SimClock.millis_since_start(start), 0);
        SimClock.advance_to(10000);
        let end = SimClock.now();
        assert_eq!(SimClock.millis_since_start(end), 10000);
        assert_eq!(
            end.duration_since(start),
            tokio::time::Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn test_sim_clock_system_time() {
        let start = SimClock.system_time_now();
        SimClock.advance_to(10000);
        let end = SimClock.system_time_now();
        assert_eq!(
            end.duration_since(start).unwrap(),
            tokio::time::Duration::from_secs(10)
        );
    }
}
