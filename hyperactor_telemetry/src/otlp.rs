/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! OTLP export over HTTP+protobuf.
//!
//! Endpoints follow the standard OTLP env vars: a per-signal var such as
//! `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` is used as is, otherwise
//! `OTEL_EXPORTER_OTLP_ENDPOINT` plus the signal path (e.g., `/v1/logs`). An
//! empty `OTEL_EXPORTER_OTLP_ENDPOINT` disables export; when no var is set,
//! export falls back to a build-specific default (none in OSS builds). All
//! signals share the resource built by `resource()`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use opentelemetry::Context;
use opentelemetry::KeyValue;
use opentelemetry::logs::AnyValue;
use opentelemetry::logs::LogRecord;
use opentelemetry::logs::Logger;
use opentelemetry::logs::LoggerProvider;
use opentelemetry::logs::Severity;
use opentelemetry::trace::Span as _;
use opentelemetry::trace::SpanContext;
use opentelemetry::trace::TraceContextExt as _;
use opentelemetry::trace::Tracer as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT;
use opentelemetry_otlp::OTEL_EXPORTER_OTLP_LOGS_ENDPOINT;
use opentelemetry_otlp::OTEL_EXPORTER_OTLP_METRICS_ENDPOINT;
use opentelemetry_otlp::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::BatchLogProcessor;
use opentelemetry_sdk::logs::SdkLogger;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::trace::SdkTracer;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Targets;

use crate::env::Env;
use crate::sinks::perfetto::USER_TELEMETRY_PREFIX;
use crate::trace_dispatcher::FieldValue;
use crate::trace_dispatcher::TraceEvent;
use crate::trace_dispatcher::TraceEventSink;

/// `service.name` resource attribute on every OTLP signal.
const SERVICE_NAME: &str = "monarch/monarch";

/// Module of the shim in `monarch_hyperactor` that forwards Python log records
/// into `tracing`.
const PYTHON_LOG_FORWARDER_MODULE: &str = "monarch_hyperactor::telemetry";

/// Endpoint used when no OTLP endpoint env var is set: the host collector
/// on Meta builds outside tests, otherwise none.
pub(crate) fn default_endpoint(env: &Env) -> Option<&'static str> {
    #[cfg(all(fbcode_build, target_os = "linux"))]
    if *env != Env::Test {
        return Some(crate::meta::otlp::DEFAULT_ENDPOINT);
    }
    let _ = env;
    None
}

/// Where an exporter sends its signal.
#[derive(Debug, PartialEq)]
enum Destination {
    /// The OTLP crate resolves the URL from the standard env vars.
    FromEnv,
    /// Full signal URL built from the default endpoint.
    Default(String),
}

/// A non-empty per-signal var wins, then `OTEL_EXPORTER_OTLP_ENDPOINT` (empty
/// disables export), then `default`. Returns `None` when export is disabled.
fn resolve_destination(
    signal_env: Option<String>,
    base_env: Option<String>,
    default: Option<&str>,
    path: &str,
) -> Option<Destination> {
    if signal_env.is_some_and(|endpoint| !endpoint.is_empty()) {
        return Some(Destination::FromEnv);
    }
    match base_env {
        Some(endpoint) => (!endpoint.is_empty()).then_some(Destination::FromEnv),
        None => default.map(|endpoint| {
            Destination::Default(format!("{}/{path}", endpoint.trim_end_matches('/')))
        }),
    }
}

fn destination(signal_env: &str, path: &str) -> Option<Destination> {
    resolve_destination(
        std::env::var(signal_env).ok(),
        std::env::var(OTEL_EXPORTER_OTLP_ENDPOINT).ok(),
        default_endpoint(&Env::current()),
        path,
    )
}

fn with_destination<B: WithExportConfig>(builder: B, destination: Destination) -> B {
    match destination {
        Destination::FromEnv => builder,
        Destination::Default(url) => builder.with_endpoint(url),
    }
}

/// Resource attached to every OTLP signal.
pub(crate) fn resource() -> Resource {
    let mut builder = Resource::builder()
        .with_service_name(SERVICE_NAME)
        .with_attribute(KeyValue::new("execution_id", crate::env::execution_id()))
        .with_attribute(KeyValue::new("user", crate::username()))
        .with_attribute(KeyValue::new("environment", Env::current().to_string()));
    if let Ok(hostname) = whoami::hostname() {
        builder = builder.with_attribute(KeyValue::new("hostname", hostname));
    }
    if let Some(trace_id) = crate::trace::get_trace_id() {
        builder = builder.with_attribute(KeyValue::new("monarch_client_trace_id", trace_id));
    }
    #[cfg(all(fbcode_build, target_os = "linux"))]
    {
        builder = builder.with_attributes(crate::meta::otlp::resource_attributes());
    }
    builder.build()
}

/// Build an OTLP metric exporter when an endpoint is configured.
pub(crate) fn otlp_metric_exporter() -> Option<impl PushMetricExporter> {
    let destination = destination(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, "v1/metrics")?;
    let builder = with_destination(
        opentelemetry_otlp::MetricExporter::builder().with_http(),
        destination,
    );
    #[cfg(all(fbcode_build, target_os = "linux"))]
    let builder = builder.with_temporality(crate::meta::otlp::TEMPORALITY);
    match builder.build() {
        Ok(exporter) => Some(exporter),
        Err(e) => {
            eprintln!("[telemetry] failed to build OTLP metric exporter: {}", e);
            None
        }
    }
}

fn log_targets() -> Targets {
    crate::config::get_tracing_targets().with_target("execution", LevelFilter::INFO)
}

fn level_to_severity(level: &tracing::Level) -> Severity {
    match *level {
        tracing::Level::TRACE => Severity::Trace,
        tracing::Level::DEBUG => Severity::Debug,
        tracing::Level::INFO => Severity::Info,
        tracing::Level::WARN => Severity::Warn,
        tracing::Level::ERROR => Severity::Error,
    }
}

/// Converts a trace field to a log (`AnyValue`) or span (`Value`) attribute value.
fn field_value<T>(v: &FieldValue) -> T
where
    T: From<bool> + From<i64> + From<f64> + From<String>,
{
    match v {
        FieldValue::Bool(b) => T::from(*b),
        FieldValue::I64(i) => T::from(*i),
        FieldValue::U64(u) => T::from(*u as i64),
        FieldValue::F64(f) => T::from(*f),
        FieldValue::Str(s) | FieldValue::Debug(s) => T::from(s.clone()),
    }
}

/// Spans opened by `OtlpSpanSink`, keyed by dispatcher span ID. `OtlpLogSink`
/// reads it to attach the enclosing span's trace context to log records.
#[derive(Clone, Default)]
struct OpenSpans(Arc<Mutex<HashMap<u64, opentelemetry_sdk::trace::Span>>>);

impl OpenSpans {
    fn insert(&self, id: u64, span: opentelemetry_sdk::trace::Span) {
        self.0
            .lock()
            .expect("should not be poisoned")
            .insert(id, span);
    }

    fn remove(&self, id: u64) -> Option<opentelemetry_sdk::trace::Span> {
        self.0.lock().expect("should not be poisoned").remove(&id)
    }

    fn context(&self, id: u64) -> Option<SpanContext> {
        self.0
            .lock()
            .expect("should not be poisoned")
            .get(&id)
            .map(|span| span.span_context().clone())
    }
}

/// Log sink that exports tracing events as OTLP log records.
///
/// Only consumes `TraceEvent::Event` (i.e., `tracing::info!()` and similar
/// log macros); all other trace event variants are ignored.
///
/// Uses the SDK's `BatchLogProcessor` for non-blocking background export.
struct OtlpLogSink {
    provider: SdkLoggerProvider,
    logger: SdkLogger,
    targets: Targets,
    spans: OpenSpans,
}

impl OtlpLogSink {
    fn new(provider: SdkLoggerProvider, spans: OpenSpans) -> Self {
        let logger = provider.logger("monarch");

        Self {
            provider,
            logger,
            targets: log_targets(),
            spans,
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
        record.add_attribute("level", AnyValue::String(level.as_str().into()));
        record.add_attribute("target", AnyValue::String((*target).into()));
        record.add_attribute("thread_id", AnyValue::String((*thread_id).into()));
        record.add_attribute("thread_name", AnyValue::String((*thread_name).into()));

        if let Some(parent) = parent_span.and_then(|id| self.spans.context(id)) {
            record.set_trace_context(
                parent.trace_id(),
                parent.span_id(),
                Some(parent.trace_flags()),
            );
            // Scuba log ingestion drops the trace context; this column joins
            // to `span_id` in the traces table.
            #[cfg(all(fbcode_build, target_os = "linux"))]
            record.add_attribute("span_id", parent.span_id().to_string());
        }
        // A log forwarded from Python has the forwarding shim as its Rust call
        // site; its own `file` and `lineno` arrive as fields. For other events,
        // a field replaces the call-site attribute with the same key.
        let forwarded_from_python = *module_path == Some(PYTHON_LOG_FORWARDER_MODULE);
        let has_field = |key: &str| fields.iter().any(|(k, _)| *k == key);
        if !forwarded_from_python
            && !has_field("module_path")
            && let Some(mp) = module_path
        {
            record.add_attribute("module_path", AnyValue::String((*mp).into()));
        }
        if !forwarded_from_python
            && !has_field("file")
            && let Some(f) = file
        {
            record.add_attribute("file", AnyValue::String((*f).into()));
        }
        if !forwarded_from_python
            && !has_field("line")
            && let Some(l) = line
        {
            record.add_attribute("line", AnyValue::Int(*l as i64));
        }
        for (k, v) in fields.iter() {
            if *k != "message" {
                record.add_attribute(*k, field_value::<AnyValue>(v));
            }
        }

        self.logger.emit(record);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        if let Err(e) = self.provider.force_flush() {
            tracing::debug!("otlp log flush failed: {e:?}");
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

fn otlp_log_sink(spans: OpenSpans) -> Option<Box<dyn TraceEventSink>> {
    let destination = destination(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, "v1/logs")?;
    match with_destination(
        opentelemetry_otlp::LogExporter::builder().with_http(),
        destination,
    )
    .build()
    {
        Ok(exporter) => Some(Box::new(OtlpLogSink::new(
            SdkLoggerProvider::builder()
                .with_log_processor(BatchLogProcessor::builder(exporter).build())
                .with_resource(resource())
                .build(),
            spans,
        ))),
        Err(e) => {
            eprintln!("[telemetry] failed to build OTLP log exporter: {}", e);
            None
        }
    }
}

/// Exports dispatcher spans, including synthetic user spans, as OTLP spans.
struct OtlpSpanSink {
    provider: SdkTracerProvider,
    tracer: SdkTracer,
    spans: OpenSpans,
    targets: Targets,
}

impl OtlpSpanSink {
    fn new(provider: SdkTracerProvider, spans: OpenSpans) -> Self {
        let tracer = provider.tracer("monarch");
        Self {
            provider,
            tracer,
            spans,
            targets: crate::config::get_tracing_targets(),
        }
    }
}

impl TraceEventSink for OtlpSpanSink {
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        match event {
            TraceEvent::NewSpan {
                id,
                name,
                target,
                level,
                fields,
                timestamp,
                parent_id,
                thread_name,
                file,
                line,
            } => {
                // The parent's context has `is_remote == false`, so the SDK
                // records a local parent despite the method name.
                let parent = parent_id
                    .and_then(|parent_id| self.spans.context(parent_id))
                    .map(|parent| Context::new().with_remote_span_context(parent))
                    .unwrap_or_default();
                // Python user spans share one static name; the user's name is
                // in the `name` field, as in the Perfetto sink.
                let span_name: Cow<'static, str> = match fields.iter().find(|(k, _)| *k == "name") {
                    Some((_, FieldValue::Str(user_name))) if *target == USER_TELEMETRY_PREFIX => {
                        user_name.clone().into()
                    }
                    _ => (*name).into(),
                };
                let mut attributes = Vec::with_capacity(fields.len() + 5);
                attributes.push(KeyValue::new("target", *target));
                attributes.push(KeyValue::new("level", level.as_str()));
                attributes.push(KeyValue::new("thread_name", *thread_name));
                if let Some(file) = file {
                    attributes.push(KeyValue::new("file", *file));
                }
                if let Some(line) = line {
                    attributes.push(KeyValue::new("line", i64::from(*line)));
                }
                attributes.extend(
                    fields
                        .iter()
                        .map(|(k, v)| KeyValue::new(*k, field_value::<opentelemetry::Value>(v))),
                );
                let span = self
                    .tracer
                    .span_builder(span_name)
                    .with_start_time(*timestamp)
                    .with_attributes(attributes)
                    .start_with_context(&self.tracer, &parent);
                self.spans.insert(*id, span);
            }
            TraceEvent::SpanClose { id, timestamp } => {
                if let Some(mut span) = self.spans.remove(*id) {
                    span.end_with_timestamp(*timestamp);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        if let Err(e) = self.provider.force_flush() {
            tracing::debug!("otlp trace flush failed: {e:?}");
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "OtlpSpanSink"
    }

    fn target_filter(&self) -> Option<&Targets> {
        Some(&self.targets)
    }
}

fn otlp_span_sink(spans: OpenSpans) -> Option<Box<dyn TraceEventSink>> {
    let destination = destination(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, "v1/traces")?;
    match with_destination(
        opentelemetry_otlp::SpanExporter::builder().with_http(),
        destination,
    )
    .build()
    {
        Ok(exporter) => Some(Box::new(OtlpSpanSink::new(
            SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource())
                .build(),
            spans,
        ))),
        Err(e) => {
            eprintln!("[telemetry] failed to build OTLP trace exporter: {}", e);
            None
        }
    }
}

/// Build the OTLP log and span sinks for each signal with an endpoint. They
/// share the open spans so log records carry their enclosing span's context.
pub(crate) fn otlp_sinks() -> Vec<Box<dyn TraceEventSink>> {
    let spans = OpenSpans::default();
    [otlp_log_sink(spans.clone()), otlp_span_sink(spans)]
        .into_iter()
        .flatten()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::SystemTime;

    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::logs::LogBatch;
    use opentelemetry_sdk::logs::LogExporter;
    use opentelemetry_sdk::logs::SdkLogRecord;
    use opentelemetry_sdk::logs::SimpleLogProcessor;
    use opentelemetry_sdk::trace::SpanData;
    use opentelemetry_sdk::trace::SpanExporter;
    use smallvec::smallvec;

    use super::*;
    use crate::trace_dispatcher::TraceEvent;

    #[derive(Clone, Debug, Default)]
    struct RecordingLogExporter {
        records: Arc<Mutex<Vec<SdkLogRecord>>>,
    }

    impl LogExporter for RecordingLogExporter {
        async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
            self.records
                .lock()
                .unwrap()
                .extend(batch.iter().map(|(record, _)| record.clone()));
            Ok(())
        }
    }

    fn recording_log_sink(spans: OpenSpans) -> (OtlpLogSink, Arc<Mutex<Vec<SdkLogRecord>>>) {
        let exporter = RecordingLogExporter::default();
        let records = Arc::clone(&exporter.records);
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(SimpleLogProcessor::new(exporter))
            .build();
        (OtlpLogSink::new(provider, spans), records)
    }

    #[derive(Clone, Debug, Default)]
    struct RecordingSpanExporter {
        spans: Arc<Mutex<Vec<SpanData>>>,
    }

    impl SpanExporter for RecordingSpanExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.spans.lock().unwrap().extend(batch);
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }
    }

    fn recording_span_sink(spans: OpenSpans) -> (OtlpSpanSink, Arc<Mutex<Vec<SpanData>>>) {
        let exporter = RecordingSpanExporter::default();
        let exported = Arc::clone(&exporter.spans);
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        (OtlpSpanSink::new(provider, spans), exported)
    }

    fn new_span(id: u64, parent_id: Option<u64>, at: SystemTime) -> TraceEvent {
        TraceEvent::NewSpan {
            id,
            name: "span",
            target: "test",
            level: tracing::Level::INFO,
            fields: smallvec![("probe", FieldValue::Str("x".to_string()))],
            timestamp: at,
            parent_id,
            thread_name: "test-thread",
            file: None,
            line: None,
        }
    }

    fn close(id: u64, at: SystemTime) -> TraceEvent {
        TraceEvent::SpanClose { id, timestamp: at }
    }

    #[test]
    fn span_uses_event_timestamps_and_fields() {
        let (mut sink, spans) = recording_span_sink(OpenSpans::default());
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let end = SystemTime::UNIX_EPOCH + Duration::from_secs(20);

        sink.consume(&new_span(1, None, start)).unwrap();
        sink.consume(&close(1, end)).unwrap();

        let spans = spans.lock().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "span");
        assert_eq!(spans[0].start_time, start);
        assert_eq!(spans[0].end_time, end);
        assert!(
            spans[0]
                .attributes
                .contains(&KeyValue::new("target", "test"))
        );
        assert!(spans[0].attributes.contains(&KeyValue::new("probe", "x")));
        assert!(
            spans[0]
                .attributes
                .contains(&KeyValue::new("level", "INFO"))
        );
        assert!(
            spans[0]
                .attributes
                .contains(&KeyValue::new("thread_name", "test-thread"))
        );
    }

    #[test]
    fn child_links_to_parent() {
        let (mut sink, spans) = recording_span_sink(OpenSpans::default());
        let now = SystemTime::now();

        sink.consume(&new_span(1, None, now)).unwrap();
        sink.consume(&new_span(2, Some(1), now)).unwrap();
        sink.consume(&close(2, now)).unwrap();
        sink.consume(&close(1, now)).unwrap();

        let spans = spans.lock().unwrap();
        let (child, parent) = (&spans[0], &spans[1]);
        assert_eq!(
            child.span_context.trace_id(),
            parent.span_context.trace_id()
        );
        assert_eq!(child.parent_span_id, parent.span_context.span_id());
        assert!(!child.parent_span_is_remote);
    }

    #[test]
    fn unknown_parent_starts_new_trace() {
        let (mut sink, spans) = recording_span_sink(OpenSpans::default());
        let now = SystemTime::now();

        sink.consume(&new_span(1, Some(99), now)).unwrap();
        sink.consume(&close(1, now)).unwrap();

        let spans = spans.lock().unwrap();
        assert_eq!(
            spans[0].parent_span_id,
            opentelemetry::trace::SpanId::INVALID
        );
    }

    #[test]
    fn synthetic_span_exports_like_native() {
        let (mut sink, spans) = recording_span_sink(OpenSpans::default());
        let id = (1 << 63) + 1;
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let end = SystemTime::UNIX_EPOCH + Duration::from_secs(20);

        sink.consume(&new_span(id, None, start)).unwrap();
        sink.consume(&TraceEvent::SpanEnter {
            id,
            timestamp: start,
            thread_name: "python",
        })
        .unwrap();
        sink.consume(&TraceEvent::SpanExit {
            id,
            timestamp: end,
            thread_name: "python",
        })
        .unwrap();
        sink.consume(&close(id, end)).unwrap();

        let spans = spans.lock().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start_time, start);
        assert_eq!(spans[0].end_time, end);
    }

    #[test]
    fn log_sink_exports_events_only() {
        let (mut sink, records) = recording_log_sink(OpenSpans::default());
        let now = SystemTime::now();

        sink.consume(&log_event(None)).unwrap();
        sink.consume(&new_span(1, None, now)).unwrap();
        sink.consume(&close(1, now)).unwrap();

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 1, "only `Event` becomes a log record");
        assert_eq!(
            records[0].body(),
            Some(&AnyValue::from("hello".to_string()))
        );
        assert_eq!(records[0].severity_number(), Some(Severity::Info));
    }

    #[test]
    fn log_location_attributes() {
        let (mut sink, records) = recording_log_sink(OpenSpans::default());
        let event = |module_path, fields| TraceEvent::Event {
            name: "event",
            target: "test",
            level: tracing::Level::INFO,
            fields,
            timestamp: SystemTime::now(),
            parent_span: None,
            thread_id: "1",
            thread_name: "test-thread",
            module_path: Some(module_path),
            file: Some("src/io.rs"),
            line: Some(42),
        };
        let message = |m: &str| ("message", FieldValue::Str(m.to_string()));

        sink.consume(&event(
            PYTHON_LOG_FORWARDER_MODULE,
            smallvec![
                message("from python"),
                ("file", FieldValue::Str("train.py".to_string())),
                ("lineno", FieldValue::I64(3)),
            ],
        ))
        .unwrap();
        sink.consume(&event(
            "my_crate::io",
            smallvec![
                message("opened"),
                ("file", FieldValue::Str("data.bin".to_string())),
            ],
        ))
        .unwrap();
        sink.consume(&event("my_crate::io", smallvec![message("plain")]))
            .unwrap();

        let records = records.lock().unwrap();
        let attrs = |i: usize| -> Vec<(String, AnyValue)> {
            records[i]
                .attributes_iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        };
        let values = |attrs: &[(String, AnyValue)], key: &str| -> Vec<AnyValue> {
            attrs
                .iter()
                .filter(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .collect()
        };
        let text = |s: &str| AnyValue::from(s.to_string());

        let python = attrs(0);
        assert_eq!(values(&python, "file"), vec![text("train.py")]);
        assert_eq!(values(&python, "lineno"), vec![AnyValue::Int(3)]);
        assert!(
            values(&python, "line").is_empty() && values(&python, "module_path").is_empty(),
            "a forwarded Python log drops the shim's call site"
        );

        let rust_with_file_field = attrs(1);
        assert_eq!(
            values(&rust_with_file_field, "file"),
            vec![text("data.bin")],
            "the event's own `file` field replaces only the call-site `file`"
        );
        assert_eq!(
            values(&rust_with_file_field, "line"),
            vec![AnyValue::Int(42)]
        );
        assert_eq!(
            values(&rust_with_file_field, "module_path"),
            vec![text("my_crate::io")]
        );

        let rust = attrs(2);
        assert_eq!(values(&rust, "file"), vec![text("src/io.rs")]);
        assert_eq!(values(&rust, "line"), vec![AnyValue::Int(42)]);
        assert!(
            !rust.iter().any(|(k, _)| k == "event_type" || k == "name"),
            "legacy Scuba attributes are not exported"
        );
    }

    #[test]
    fn resolves_destination() {
        let set = |v: &str| Some(v.to_string());
        let default = Some("http://default/");
        assert_eq!(
            resolve_destination(set("http://signal"), set(""), default, "v1/logs"),
            Some(Destination::FromEnv),
            "a per-signal endpoint wins even when the base var disables export"
        );
        assert_eq!(
            resolve_destination(set(""), set("http://base"), default, "v1/logs"),
            Some(Destination::FromEnv),
            "an empty per-signal var falls through to the base var"
        );
        assert_eq!(
            resolve_destination(None, set(""), default, "v1/logs"),
            None,
            "an empty base var disables export"
        );
        assert_eq!(
            resolve_destination(None, None, default, "v1/logs"),
            Some(Destination::Default("http://default/v1/logs".to_string()))
        );
        assert_eq!(resolve_destination(None, None, None, "v1/logs"), None);
    }

    fn log_event(parent_span: Option<u64>) -> TraceEvent {
        TraceEvent::Event {
            name: "my_event",
            target: "test",
            level: tracing::Level::INFO,
            fields: smallvec![("message", FieldValue::Str("hello".to_string()))],
            timestamp: SystemTime::now(),
            parent_span,
            thread_id: "1",
            thread_name: "test-thread",
            module_path: None,
            file: None,
            line: None,
        }
    }

    #[test]
    fn log_carries_enclosing_span_context() {
        let spans = OpenSpans::default();
        let (mut span_sink, exported) = recording_span_sink(spans.clone());
        let (mut log_sink, records) = recording_log_sink(spans);
        let now = SystemTime::now();

        span_sink.consume(&new_span(1, None, now)).unwrap();
        log_sink.consume(&log_event(Some(1))).unwrap();
        log_sink.consume(&log_event(Some(99))).unwrap();
        span_sink.consume(&close(1, now)).unwrap();

        let span = &exported.lock().unwrap()[0];
        let records = records.lock().unwrap();
        let context = records[0]
            .trace_context()
            .expect("log inside a span should carry its trace context");
        assert_eq!(context.trace_id, span.span_context.trace_id());
        assert_eq!(context.span_id, span.span_context.span_id());
        #[cfg(all(fbcode_build, target_os = "linux"))]
        assert!(
            records[0]
                .attributes_iter()
                .any(|(k, v)| k.as_str() == "span_id"
                    && *v == AnyValue::from(span.span_context.span_id().to_string())),
            "Meta builds copy the span ID into a `span_id` attribute"
        );
        assert!(
            records[1].trace_context().is_none(),
            "log with an unknown parent span should have no trace context"
        );
    }

    #[test]
    fn python_span_named_after_user_name() {
        let (mut sink, spans) = recording_span_sink(OpenSpans::default());
        let now = SystemTime::now();
        let span = |id, target| TraceEvent::NewSpan {
            id,
            name: "python_user_span",
            target,
            level: tracing::Level::INFO,
            fields: smallvec![("name", FieldValue::Str("train_step".to_string()))],
            timestamp: now,
            parent_id: None,
            thread_name: "python",
            file: None,
            line: None,
        };

        sink.consume(&span(1, USER_TELEMETRY_PREFIX)).unwrap();
        sink.consume(&close(1, now)).unwrap();
        sink.consume(&span(2, "other")).unwrap();
        sink.consume(&close(2, now)).unwrap();

        let spans = spans.lock().unwrap();
        assert_eq!(spans[0].name, "train_step");
        assert_eq!(
            spans[1].name, "python_user_span",
            "only the Python telemetry target is renamed"
        );
    }

    #[test]
    fn resource_has_execution_attributes() {
        let resource = resource();
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str("service.name")),
            Some(opentelemetry::Value::from(SERVICE_NAME))
        );
        for key in ["execution_id", "user", "hostname", "environment"] {
            assert!(
                resource
                    .get(&opentelemetry::Key::from_static_str(key))
                    .is_some(),
                "missing {key}"
            );
        }
    }

    #[test]
    fn log_targets_enable_execution() {
        assert!(log_targets().would_enable("execution", &tracing::Level::INFO));
    }
}
