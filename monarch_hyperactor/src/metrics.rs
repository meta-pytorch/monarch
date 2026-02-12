/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Metrics for Python actor endpoints.
//!
//! This module contains metrics definitions for tracking Python actor endpoint performance.

use hyperactor_telemetry::declare_static_counter;
use hyperactor_telemetry::declare_static_histogram;

// ENDPOINT METRICS
// Tracks latency of endpoint calls in microseconds
declare_static_histogram!(
    ENDPOINT_ACTOR_LATENCY_US_HISTOGRAM,
    "endpoint_actor_latency_us_histogram"
);
// Tracks the total number of endpoint calls
declare_static_counter!(ENDPOINT_ACTOR_COUNT, "endpoint_actor_count");
// Tracks errors that occur during endpoint execution
declare_static_counter!(ENDPOINT_ACTOR_ERROR, "endpoint_actor_error");
// Tracks panics that occur during endpoint execution
declare_static_counter!(ENDPOINT_ACTOR_PANIC, "endpoint_actor_panic");
// Tracks latency of endpoint calls from the caller's perspective in microseconds
declare_static_histogram!(
    ENDPOINT_CALL_LATENCY_US_HISTOGRAM,
    "endpoint_call_latency_us_histogram"
);
// Tracks latency of endpoint call_one operations in microseconds
declare_static_histogram!(
    ENDPOINT_CALL_ONE_LATENCY_US_HISTOGRAM,
    "endpoint_call_one_latency_us_histogram"
);
// Tracks latency of endpoint choose operations in microseconds
declare_static_histogram!(
    ENDPOINT_CHOOSE_LATENCY_US_HISTOGRAM,
    "endpoint_choose_latency_us_histogram"
);
// Tracks errors that occur during endpoint calls from the caller's perspective
declare_static_counter!(ENDPOINT_CALL_ERROR, "endpoint_call_error");
// Tracks errors that occur during endpoint call_one operations
declare_static_counter!(ENDPOINT_CALL_ONE_ERROR, "endpoint_call_one_error");
// Tracks errors that occur during endpoint choose operations
declare_static_counter!(ENDPOINT_CHOOSE_ERROR, "endpoint_choose_error");
// Tracks throughput of endpoint call operations
declare_static_counter!(ENDPOINT_CALL_THROUGHPUT, "endpoint_call_throughput");
// Tracks throughput of endpoint call_one operations
declare_static_counter!(ENDPOINT_CALL_ONE_THROUGHPUT, "endpoint_call_one_throughput");
// Tracks throughput of endpoint choose operations
declare_static_counter!(ENDPOINT_CHOOSE_THROUGHPUT, "endpoint_choose_throughput");
