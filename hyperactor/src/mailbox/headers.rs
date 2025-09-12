/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Message headers and latency tracking functionality for the mailbox system.
//!
//! This module provides header attributes and utilities for message metadata,
//! including latency tracking timestamps used to measure message processing times.

use crate::attrs::Attrs;
use crate::attrs::declare_attrs;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::config::global;

declare_attrs! {
    /// Send timestamp for message latency tracking (in microseconds since UNIX_EPOCH)
    pub attr SEND_TIMESTAMP: u64;
}

/// Set the send timestamp for latency tracking if sampling is enabled and timestamp not already set.
/// Only sets the timestamp if not already present in headers.
pub fn set_send_timestamp_if_sampling(headers: &mut Attrs) {
    if !headers.contains_key(SEND_TIMESTAMP)
        && fastrand::u32(..) % 100 <= global::get(crate::config::LATENCY_SAMPLING_RATE)
    {
        if let Ok(duration) = RealClock
            .system_time_now()
            .duration_since(std::time::UNIX_EPOCH)
        {
            headers.set(SEND_TIMESTAMP, duration.as_micros() as u64);
        }
    }
}
