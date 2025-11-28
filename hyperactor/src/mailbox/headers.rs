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

use std::any::type_name;
use std::time::SystemTime;

use hyperactor_config::attrs::Attrs;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::global;

use crate::clock::Clock;
use crate::clock::RealClock;
use crate::metrics::MESSAGE_LATENCY_MICROS;

declare_attrs! {
    /// Send timestamp for message latency tracking
    pub attr SEND_TIMESTAMP: SystemTime;

    /// The rust type of the message.
    pub attr RUST_MESSAGE_TYPE: String;
}

/// Set the send timestamp for latency tracking if timestamp not already set.
pub fn set_send_timestamp(headers: &mut Attrs) {
    if !headers.contains_key(SEND_TIMESTAMP) {
        let time = RealClock.system_time_now();
        headers.set(SEND_TIMESTAMP, time);
    }
}

/// Set the send timestamp for latency tracking if timestamp not already set.
pub fn set_rust_message_type<M>(headers: &mut Attrs) {
    headers.set(RUST_MESSAGE_TYPE, type_name::<M>().to_string());
}

/// This function checks the configured sampling rate and, if the random sample passes,
/// calculates the latency between the send timestamp and the current time, then records
/// the latency metric with the associated actor ID.
pub fn log_message_latency_if_sampling(headers: &Attrs, actor_id: String) {
    if fastrand::f32() > global::get(crate::config::MESSAGE_LATENCY_SAMPLING_RATE) {
        return;
    }

    if !headers.contains_key(SEND_TIMESTAMP) {
        tracing::debug!(
            actor_id = actor_id,
            "SEND_TIMESTAMP missing from message headers, cannot measure latency"
        );
        return;
    }

    let metric_pairs = hyperactor_telemetry::kv_pairs!(
        "actor_id" => actor_id
    );
    let Some(send_timestamp) = headers.get(SEND_TIMESTAMP) else {
        return;
    };
    let now = RealClock.system_time_now();
    let latency = now.duration_since(*send_timestamp).unwrap_or_default();
    MESSAGE_LATENCY_MICROS.record(latency.as_micros() as f64, metric_pairs);
}
