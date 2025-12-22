/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Exporters for the unified telemetry layer.
//! Each exporter implements the TraceExporter trait and handles
//! writing events to a specific backend (SQLite, Scuba, glog, etc).

pub mod glog;
pub mod perfetto;
pub mod sqlite;
