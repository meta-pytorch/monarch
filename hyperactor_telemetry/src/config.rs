/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration keys for hyperactor telemetry.
//!
//! This module defines configuration attributes for telemetry features including
//! OpenTelemetry tracing/metrics, recorder output, SQLite tracing, and file logging.

use std::time::Duration;

use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;

declare_attrs! {
    /// Enable the OpenTelemetry tracing layer.
    /// When true (default), OpenTelemetry tracing is enabled.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("ENABLE_OTEL_TRACING".to_string()),
        py_name: Some("enable_otel_tracing".to_string()),
    })
    pub attr ENABLE_OTEL_TRACING: bool = true;

    /// Enable the OpenTelemetry metrics layer.
    /// When true (default), OpenTelemetry metrics are enabled.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("ENABLE_OTEL_METRICS".to_string()),
        py_name: Some("enable_otel_metrics".to_string()),
    })
    pub attr ENABLE_OTEL_METRICS: bool = true;

    /// Enable the recorder tracing layer.
    /// When true (default), recorder output is enabled.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("ENABLE_RECORDER_TRACING".to_string()),
        py_name: Some("enable_recorder_tracing".to_string()),
    })
    pub attr ENABLE_RECORDER_TRACING: bool = true;

    /// Enable the SQLite tracing layer.
    /// When true, SQLite tracing is enabled.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("ENABLE_SQLITE_TRACING".to_string()),
        py_name: Some("enable_sqlite_tracing".to_string()),
    })
    pub attr ENABLE_SQLITE_TRACING: bool = false;

    /// Log level for Monarch file logging.
    /// Valid values: "debug", "info", "warn", "error"
    /// Defaults to "info" when not set via environment variable.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("MONARCH_FILE_LOG".to_string()),
        py_name: Some("file_log_level".to_string()),
    })
    pub attr MONARCH_FILE_LOG_LEVEL: String = String::new();

    /// OpenTelemetry metric export interval.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("OTEL_METRIC_EXPORT_INTERVAL".to_string()),
        py_name: Some("otel_metric_export_interval".to_string()),
    })
    pub attr OTEL_METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(10);

    /// Enable logging of span enter/exit events to Scuba.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("SCUBA_LOG_ENTER_EXIT".to_string()),
        py_name: Some("scuba_log_enter_exit".to_string()),
    })
    pub attr SCUBA_LOG_ENTER_EXIT: bool = false;

    /// Enable the unified tracing layer.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("USE_UNIFIED_LAYER".to_string()),
        py_name: Some("use_unified_layer".to_string()),
    })
    pub attr USE_UNIFIED_LAYER: bool = false;

    // Suffix to append to log filenames for test isolation
    @meta(CONFIG = ConfigAttr {
        env_name: Some("MONARCH_LOG_SUFFIX".to_string()),
        py_name: Some("monarch_log_suffix".to_string()),
    })
    pub attr MONARCH_LOG_SUFFIX: String = String::new();
}
