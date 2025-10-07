/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The clock allows us to control the behaviour of all time dependent events in both real and simulated time throughout the system

use std::error::Error;
use std::fmt;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::SystemTime;

use futures::pin_mut;
use hyperactor_telemetry::TelemetryClock;
use serde::Deserialize;
use serde::Serialize;

use crate::Mailbox;
use crate::channel::ChannelAddr;
use crate::id;
use crate::mailbox::DeliveryError;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::Undeliverable;
use crate::mailbox::UndeliverableMailboxSender;
use crate::mailbox::monitored_return_handle;
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
        start: now,
        now: Mutex::new(now),
        system_start: SystemTime::now(),
    }
});

#[derive(Debug)]
/// Errors returned by `Timeout`.
///
/// This error is returned when a timeout expires before the function was able
/// to finish.
pub struct TimeoutError;

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "deadline has elapsed")
    }
}

impl Error for TimeoutError {}

/// The Sleeps trait allows different implementations to control the behavior of sleep.
pub trait Clock {
    /// Initiates a sleep for the specified duration
    fn sleep(
        &self,
        duration: tokio::time::Duration,
    ) -> impl std::future::Future<Output = ()> + Send + Sync;
    /// Initiates a sleep for the specified duration
    fn non_advancing_sleep(
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
    /// Require a future to complete within the specified duration
    ///
    /// if the future completes before the duration has elapsed, then the completed value is returned.
    /// Otherwise, an error is returned and the future is canceled.
    fn timeout<F, T>(
        &self,
        duration: tokio::time::Duration,
        f: F,
    ) -> impl std::future::Future<Output = Result<T, TimeoutError>> + Send
    where
        F: std::future::Future<Output = T> + Send;
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
    async fn non_advancing_sleep(&self, duration: tokio::time::Duration) {
        match self {
            Self::Sim(clock) => clock.non_advancing_sleep(duration).await,
            Self::Real(clock) => clock.non_advancing_sleep(duration).await,
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
    async fn timeout<F, T>(&self, duration: tokio::time::Duration, f: F) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = T> + Send,
    {
        match self {
            Self::Sim(clock) => clock.timeout(duration, f).await,
            Self::Real(clock) => clock.timeout(duration, f).await,
        }
    }
}

impl TelemetryClock for ClockKind {
    fn now(&self) -> tokio::time::Instant {
        match self {
            Self::Sim(clock) => clock.now(),
            Self::Real(clock) => clock.now(),
        }
    }

    fn system_time_now(&self) -> std::time::SystemTime {
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
        let mailbox = SimClock::mailbox().clone();
        let (tx, rx) = mailbox.open_once_port::<()>();

        simnet_handle()
            .unwrap()
            .send_event(SleepEvent::new(tx.bind(), mailbox, duration))
            .unwrap();
        rx.recv().await.unwrap();
    }

    async fn non_advancing_sleep(&self, duration: tokio::time::Duration) {
        let mailbox = SimClock::mailbox().clone();
        let (tx, rx) = mailbox.open_once_port::<()>();

        simnet_handle()
            .unwrap()
            .send_nonadvanceable_event(SleepEvent::new(tx.bind(), mailbox, duration))
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
        *SIM_TIME.now.lock().unwrap()
    }

    fn system_time_now(&self) -> SystemTime {
        SIM_TIME.system_start + self.now().duration_since(SIM_TIME.start)
    }

    #[allow(clippy::disallowed_methods)]
    async fn timeout<F, T>(&self, duration: tokio::time::Duration, f: F) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = T>,
    {
        let mailbox = SimClock::mailbox().clone();
        let (tx, deadline_rx) = mailbox.open_once_port::<()>();

        simnet_handle()
            .unwrap()
            .send_event(SleepEvent::new(tx.bind(), mailbox, duration))
            .unwrap();

        let fut = f;
        pin_mut!(fut);

        tokio::select! {
            _ = deadline_rx.recv() => {
                Err(TimeoutError)
            }
            res = &mut fut => Ok(res)
        }
    }
}

impl SimClock {
    // TODO (SF, 2025-07-11): Remove this global, thread through a mailbox
    // from upstack and handle undeliverable messages properly.
    fn mailbox() -> &'static Mailbox {
        static SIMCLOCK_MAILBOX: OnceLock<Mailbox> = OnceLock::new();
        SIMCLOCK_MAILBOX.get_or_init(|| {
            let mailbox = Mailbox::new_detached(id!(proc[0].proc).clone());
            let (_undeliverable_messages, mut rx) =
                mailbox.bind_actor_port::<Undeliverable<MessageEnvelope>>();
            tokio::spawn(async move {
                while let Ok(Undeliverable(mut envelope)) = rx.recv().await {
                    envelope.set_error(DeliveryError::BrokenLink(
                        "message returned to undeliverable port".to_string(),
                    ));
                    UndeliverableMailboxSender
                        .post(envelope, /*unused */ monitored_return_handle())
                }
            });
            mailbox
        })
    }

    /// Advance the sumulator's time to the specified instant
    pub fn advance_to(&self, time: tokio::time::Instant) {
        let mut guard = SIM_TIME.now.lock().unwrap();
        *guard = time;
    }

    /// Get the number of milliseconds elapsed since the start of the simulation
    pub fn duration_since_start(&self, instant: tokio::time::Instant) -> tokio::time::Duration {
        instant.duration_since(SIM_TIME.start)
    }

    /// Instant marking the start of the simulation
    pub fn start(&self) -> tokio::time::Instant {
        SIM_TIME.start
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
    async fn non_advancing_sleep(&self, duration: tokio::time::Duration) {
        Self::sleep(self, duration).await;
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
    #[allow(clippy::disallowed_methods)]
    async fn timeout<F, T>(&self, duration: tokio::time::Duration, f: F) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = T>,
    {
        tokio::time::timeout(duration, f)
            .await
            .map_err(|_| TimeoutError)
    }
}

#[cfg(test)]
mod tests {

    use crate::clock::Clock;
    use crate::clock::SimClock;
    use crate::simnet;

    #[tokio::test]
    async fn test_sim_clock_simple() {
        let start = SimClock.now();
        assert_eq!(
            SimClock.duration_since_start(start),
            tokio::time::Duration::ZERO
        );
        SimClock.advance_to(SimClock.start() + tokio::time::Duration::from_millis(10000));
        let end = SimClock.now();
        assert_eq!(
            SimClock.duration_since_start(end),
            tokio::time::Duration::from_millis(10000)
        );
        assert_eq!(
            end.duration_since(start),
            tokio::time::Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn test_sim_clock_system_time() {
        let start = SimClock.system_time_now();
        SimClock.advance_to(SimClock.start() + tokio::time::Duration::from_millis(10000));
        let end = SimClock.system_time_now();
        assert_eq!(
            end.duration_since(start).unwrap(),
            tokio::time::Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn test_sim_timeout() {
        simnet::start();
        let res = SimClock
            .timeout(tokio::time::Duration::from_secs(10), async {
                SimClock.sleep(tokio::time::Duration::from_secs(5)).await;
                5
            })
            .await;
        assert_eq!(res.unwrap(), 5);

        let res = SimClock
            .timeout(tokio::time::Duration::from_secs(10), async {
                SimClock.sleep(tokio::time::Duration::from_secs(15)).await;
                5
            })
            .await;
        assert!(res.is_err());
    }
}
