/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! OTLP export for OSS observability.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, this module provides:
//! - A `SdkMeterProvider` that exports metrics via OTLP/HTTP+protobuf
//! - An `OtlpLogSink` that exports log events via OTLP/HTTP+protobuf
//!
//! When the env var is unset, both functions return `None`, preserving
//! the current no-op behavior for OSS builds.

use opentelemetry::logs::AnyValue;
use opentelemetry::logs::LogRecord;
use opentelemetry::logs::Logger;
use opentelemetry::logs::LoggerProvider;
use opentelemetry::logs::Severity;
use opentelemetry_sdk::logs::BatchLogProcessor;
use opentelemetry_sdk::logs::SdkLogger;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tracing_subscriber::filter::Targets;

use crate::config::OTEL_METRIC_EXPORT_INTERVAL;
use crate::trace_dispatcher::FieldValue;
use crate::trace_dispatcher::TraceEvent;
use crate::trace_dispatcher::TraceEventSink;

#[allow(dead_code)]
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Build an OTLP-backed `SdkMeterProvider` if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
///
/// The `opentelemetry-otlp` crate automatically reads standard OTel env vars
/// (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`,
/// `OTEL_EXPORTER_OTLP_TIMEOUT`, etc.), so callers only need to set those.
///
/// Returns `None` if the endpoint is not configured, leaving the global
/// meter provider as the default no-op.
#[allow(dead_code)]
pub fn otlp_meter_provider() -> Option<SdkMeterProvider> {
    if std::env::var(OTLP_ENDPOINT_ENV).is_err() {
        return None;
    }

    let exporter = match opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[telemetry] failed to build OTLP metric exporter: {}", e);
            return None;
        }
    };

    let interval = hyperactor_config::global::get(OTEL_METRIC_EXPORT_INTERVAL);

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build();

    let provider = SdkMeterProvider::builder().with_reader(reader).build();

    Some(provider)
}

#[allow(dead_code)]
fn level_to_severity(level: &tracing::Level) -> Severity {
    match *level {
        tracing::Level::TRACE => Severity::Trace,
        tracing::Level::DEBUG => Severity::Debug,
        tracing::Level::INFO => Severity::Info,
        tracing::Level::WARN => Severity::Warn,
        tracing::Level::ERROR => Severity::Error,
    }
}

#[allow(dead_code)]
fn field_value_to_any_value(v: &FieldValue) -> AnyValue {
    match v {
        FieldValue::Bool(b) => AnyValue::Boolean(*b),
        FieldValue::I64(i) => AnyValue::Int(*i),
        FieldValue::U64(u) => AnyValue::Int(*u as i64),
        FieldValue::F64(f) => AnyValue::Double(*f),
        FieldValue::Str(s) => AnyValue::String(s.clone().into()),
        FieldValue::Debug(s) => AnyValue::String(s.clone().into()),
    }
}

/// Log sink that exports tracing events as OTLP log records.
///
/// Only consumes `TraceEvent::Event` (i.e., `tracing::info!()` and similar
/// log macros); all other trace event variants are ignored.
///
/// Uses the SDK's `BatchLogProcessor` for non-blocking background export.
#[allow(dead_code)]
pub(crate) struct OtlpLogSink {
    provider: SdkLoggerProvider,
    logger: SdkLogger,
    targets: Targets,
}

impl OtlpLogSink {
    #[allow(dead_code)]
    pub(crate) fn new(exporter: opentelemetry_otlp::LogExporter) -> Self {
        let processor = BatchLogProcessor::builder(exporter).build();
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(processor)
            .build();
        let logger = provider.logger("monarch");

        Self {
            provider,
            logger,
            targets: crate::config::get_tracing_targets(),
        }
    }
}

impl Drop for OtlpLogSink {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            eprintln!("[telemetry] otlp log provider shutdown failed: {e:?}");
        }
    }
}

impl TraceEventSink for OtlpLogSink {
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        let TraceEvent::Event {
            name,
            target,
            level,
            fields,
            timestamp,
            parent_span,
            thread_id,
            thread_name,
            module_path,
            file,
            line,
        } = event
        else {
            return Ok(());
        };

        let mut record = self.logger.create_log_record();

        let body = fields
            .iter()
            .find(|(k, _)| *k == "message")
            .map(|(_, v)| match v {
                FieldValue::Str(s) => s.clone(),
                FieldValue::Debug(s) => s.clone(),
                other => format!("{:?}", other),
            })
            .unwrap_or_else(|| (*name).to_string());

        record.set_timestamp(*timestamp);
        record.set_severity_number(level_to_severity(level));
        record.set_body(AnyValue::String(body.into()));
        record.add_attribute("event_type", AnyValue::String("instant_event".into()));
        record.add_attribute("name", AnyValue::String((*name).into()));
        record.add_attribute("level", AnyValue::String(level.as_str().into()));
        record.add_attribute("target", AnyValue::String((*target).into()));
        record.add_attribute("thread_id", AnyValue::String((*thread_id).into()));
        record.add_attribute("thread_name", AnyValue::String((*thread_name).into()));

        if let Some(pid) = parent_span {
            record.add_attribute("parent_span_id", AnyValue::Int(*pid as i64));
        }
        if let Some(mp) = module_path {
            record.add_attribute("module_path", AnyValue::String((*mp).into()));
        }
        if let Some(f) = file {
            record.add_attribute("file", AnyValue::String((*f).into()));
        }
        if let Some(l) = line {
            record.add_attribute("line", AnyValue::Int(*l as i64));
        }
        for (k, v) in fields.iter() {
            if *k != "message" {
                record.add_attribute(*k, field_value_to_any_value(v));
            }
        }

        self.logger.emit(record);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        if let Err(e) = self.provider.force_flush() {
            eprintln!("[telemetry] otlp log flush failed: {e:?}");
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "OtlpLogSink"
    }

    fn target_filter(&self) -> Option<&Targets> {
        Some(&self.targets)
    }
}

/// Build an `OtlpLogSink` if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
///
/// Returns `None` when the endpoint is not configured.
#[allow(dead_code)]
pub(crate) fn otlp_log_sink() -> Option<Box<dyn TraceEventSink>> {
    if std::env::var(OTLP_ENDPOINT_ENV).is_err() {
        return None;
    }

    let exporter = match opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[telemetry] failed to build OTLP log exporter: {}", e);
            return None;
        }
    };

    Some(Box::new(OtlpLogSink::new(exporter)))
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use smallvec::smallvec;

    use super::*;
    use crate::trace_dispatcher::TraceEvent;

    #[test]
    fn test_otlp_meter_provider_returns_none_without_endpoint() {
        // Safety: test-only; no other threads read this env var concurrently.
        unsafe { std::env::remove_var(OTLP_ENDPOINT_ENV) };
        assert!(otlp_meter_provider().is_none());
    }

    fn make_test_log_sink() -> OtlpLogSink {
        let exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .build()
            .expect("test log exporter");
        OtlpLogSink::new(exporter)
    }

    #[test]
    fn test_otlp_log_sink_returns_none_without_endpoint() {
        // Safety: test-only; no other threads read this env var concurrently.
        unsafe { std::env::remove_var(OTLP_ENDPOINT_ENV) };
        assert!(otlp_log_sink().is_none());
    }

    #[test]
    fn test_log_sink_event_produces_log_record() {
        let mut sink = make_test_log_sink();

        let result = sink.consume(&TraceEvent::Event {
            name: "my_event",
            target: "test",
            level: tracing::Level::INFO,
            fields: smallvec![("message", FieldValue::Str("hello world".to_string()))],
            timestamp: SystemTime::now(),
            parent_span: None,
            thread_id: "1",
            thread_name: "test-thread",
            module_path: None,
            file: None,
            line: None,
        });

        assert!(result.is_ok(), "consuming an Event should succeed");
    }

    #[test]
    fn test_log_sink_ignores_non_event_variants() {
        let mut sink = make_test_log_sink();

        let result = sink.consume(&TraceEvent::NewSpan {
            id: 1,
            name: "my_span",
            target: "test",
            level: tracing::Level::INFO,
            fields: smallvec![],
            timestamp: SystemTime::now(),
            parent_id: None,
            thread_name: "test-thread",
            file: None,
            line: None,
        });
        assert!(result.is_ok(), "NewSpan should be silently ignored");

        let result = sink.consume(&TraceEvent::SpanClose {
            id: 1,
            timestamp: SystemTime::now(),
        });
        assert!(result.is_ok(), "SpanClose should be silently ignored");
    }
}
