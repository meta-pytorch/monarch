/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! SQLite-backed tracing for `hyperactor_telemetry`.
//!
//! Split into its own crate so the `libsqlite3-sys` C build (a ~66s
//! single-TU amalgamation compile in OSS builds) is not on the
//! compile critical path of `hyperactor` and everything above it:
//! consumers that never enable SQLite tracing don't build it at
//! all, and consumers that do build it in parallel slack.
//!
//! Two pieces live here:
//! - [`sink::SqliteSink`], the `TraceEventSink` used when
//!   `ENABLE_SQLITE_TRACING` is set. Processes that want it must
//!   call [`register()`] before `hyperactor_telemetry::
//!   initialize_logging` runs.
//! - [`sqlite::SqliteLayer`] / [`sqlite::SqliteTracing`], the older
//!   tracing-layer API (used by tests and the Python
//!   `PySqliteTracing` class), plus the shared table definitions
//!   both pieces write to.

use anyhow::Result;
use hyperactor_telemetry::trace_dispatcher::TraceEventSink;

pub mod sink;
pub mod sqlite;

/// Register the SQLite sink factory with `hyperactor_telemetry`.
///
/// After registration, `initialize_logging` creates a
/// [`sink::SqliteSink`] whenever `ENABLE_SQLITE_TRACING` is set.
/// Without registration that config logs a warning and is ignored.
/// Idempotent: only the first call takes effect.
pub fn register() {
    hyperactor_telemetry::register_sqlite_sink_factory(create_sink);
}

fn create_sink(db_file: &str) -> Result<Box<dyn TraceEventSink>> {
    Ok(Box::new(sink::SqliteSink::new_with_file(db_file, 100)?))
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use hyperactor_telemetry::trace_dispatcher::FieldValue;
    use hyperactor_telemetry::trace_dispatcher::TraceEvent;

    use super::*;

    #[test]
    fn sink_consumes_event_and_flushes() -> Result<()> {
        let mut sink = sink::SqliteSink::new(1)?;
        let event = TraceEvent::Event {
            name: "test_event",
            target: "log_events",
            level: tracing::Level::INFO,
            fields: [("message", FieldValue::Str("hello".to_string()))]
                .into_iter()
                .collect(),
            timestamp: SystemTime::now(),
            parent_span: None,
            thread_id: "test",
            thread_name: "test",
            module_path: None,
            file: None,
            line: None,
        };
        sink.consume(&event)?;
        sink.flush()?;
        assert_eq!(sink.name(), "SqliteSink");
        Ok(())
    }
}
