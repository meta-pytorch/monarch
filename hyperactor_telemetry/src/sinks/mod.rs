/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Sinks for the unified telemetry layer.
//! Each sink implements `TraceEventSink` and writes events to a specific
//! backend (SQLite, Perfetto, glog, etc.).

pub mod glog;
pub mod perfetto;
