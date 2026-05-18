/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::env;

use crate::env::execution_id;

const MONARCH_CLIENT_TRACE_ID: &str = "MONARCH_CLIENT_TRACE_ID";

/// Returns the current trace ID if it exists, or None if it doesn't.
/// This function does not create a new trace ID if one doesn't exist.
/// Todo: Eventually use Message Headers to relay this traceid instead of
/// env vars
pub fn get_trace_id() -> Option<String> {
    if let Ok(env_id) = env::var(MONARCH_CLIENT_TRACE_ID)
        && !env_id.is_empty()
    {
        return Some(env_id);
    }

    // No trace ID found
    None
}

/// Returns the current trace ID, if none exists, set the current execution id as the trace id.
/// This ensures that the client trace id and execution id is the same.
/// The trace ID remains the same as long as it is in the same process.
/// Use this method only on the client side.
pub fn get_or_create_trace_id() -> String {
    if let Ok(existing_trace_id) = std::env::var(MONARCH_CLIENT_TRACE_ID)
        && !existing_trace_id.is_empty()
    {
        return existing_trace_id;
    }

    let trace_id = execution_id().clone();
    // Safety: Can be unsound if there are multiple threads
    // reading and writing the environment.
    unsafe {
        std::env::set_var(MONARCH_CLIENT_TRACE_ID, trace_id.clone());
    }

    trace_id
}
