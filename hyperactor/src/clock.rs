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
use std::time::SystemTime;

use hyperactor_telemetry::TelemetryClock;
use serde::Deserialize;
use serde::Serialize;

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

/// An adapter that allows us to control the behaviour of sleep
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClockKind {
    /// Represents a real clock using tokio's sleep functionality for production use.
    Real(RealClock),
}

impl Clock for ClockKind {
    async fn sleep(&self, duration: tokio::time::Duration) {
        match self {
            Self::Real(clock) => clock.sleep(duration).await,
        }
    }
    async fn non_advancing_sleep(&self, duration: tokio::time::Duration) {
        match self {
            Self::Real(clock) => clock.non_advancing_sleep(duration).await,
        }
    }
    async fn sleep_until(&self, deadline: tokio::time::Instant) {
        match self {
            Self::Real(clock) => clock.sleep_until(deadline).await,
        }
    }
    fn now(&self) -> tokio::time::Instant {
        match self {
            Self::Real(clock) => clock.now(),
        }
    }
    fn system_time_now(&self) -> SystemTime {
        match self {
            Self::Real(clock) => clock.system_time_now(),
        }
    }
    async fn timeout<F, T>(&self, duration: tokio::time::Duration, f: F) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = T> + Send,
    {
        match self {
            Self::Real(clock) => clock.timeout(duration, f).await,
        }
    }
}

impl TelemetryClock for ClockKind {
    fn now(&self) -> tokio::time::Instant {
        match self {
            Self::Real(clock) => clock.now(),
        }
    }

    fn system_time_now(&self) -> std::time::SystemTime {
        match self {
            Self::Real(clock) => clock.system_time_now(),
        }
    }
}

impl Default for ClockKind {
    fn default() -> Self {
        Self::Real(RealClock)
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
