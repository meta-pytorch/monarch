/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(internal_features)]
#![allow(clippy::disallowed_methods)] // hyperactor_telemetry can't use hyperactor::clock::Clock (circular dependency)
#![feature(assert_matches)]
#![feature(sync_unsafe_cell)]
#![feature(mpmc_channel)]
#![feature(cfg_version)]
#![feature(formatting_options)]
#![recursion_limit = "256"]

// Environment variable for job name (used for environment detection)
pub const MAST_HPC_JOB_NAME_ENV: &str = "MAST_HPC_JOB_NAME";

// Log level constants
const LOG_LEVEL_INFO: &str = "info";
const LOG_LEVEL_DEBUG: &str = "debug";

// Span field constants
const SPAN_FIELD_RECORDING: &str = "recording";
#[allow(dead_code)]
const SPAN_FIELD_RECORDER: &str = "recorder";

// Environment value constants
const ENV_VALUE_LOCAL: &str = "local";
const ENV_VALUE_MAST_EMULATOR: &str = "mast_emulator";
const ENV_VALUE_MAST: &str = "mast";
const ENV_VALUE_TEST: &str = "test";
#[allow(dead_code)]
const ENV_VALUE_LOCAL_MAST_SIMULATOR: &str = "local_mast_simulator";

/// A marker field used to indicate that a span should not be recorded as
/// individual start/end span events; rather the span is purely used to
/// provide context for child events.
///
/// Note that the mechanism for skipping span recording uses the precise
/// name "skip_record", thus it must be used as a naked identifier:
/// ```ignore
/// use hyperactor_telemetry::skip_record;
///
/// tracing::span!(..., skip_record);
/// ```
#[allow(non_upper_case_globals)]
// pub const skip_record: tracing::field::Empty = tracing::field::Empty;
pub const skip_record: bool = true;

mod config;
pub mod in_memory_reader;
#[cfg(fbcode_build)]
mod meta;
mod otel;
mod pool;
mod rate_limit;
pub mod recorder;
pub mod sinks;
mod spool;
pub mod sqlite;
pub mod task;
pub mod trace;
pub mod trace_dispatcher;

// Re-export key types for external sink implementations
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Instant;

use lazy_static::lazy_static;
pub use opentelemetry;
pub use opentelemetry::Key;
pub use opentelemetry::KeyValue;
pub use opentelemetry::Value;
pub use opentelemetry::global::meter;
pub use trace_dispatcher::DispatcherControl;
pub use trace_dispatcher::FieldValue;
pub use trace_dispatcher::TraceEvent;
pub use trace_dispatcher::TraceEventSink;
pub use tracing;
pub use tracing::Level;
use tracing_appender::non_blocking::NonBlocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_glog::Glog;
use tracing_glog::GlogFields;
use tracing_glog::LocalTime;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::registry::LookupSpan;

use crate::config::ENABLE_OTEL_METRICS;
use crate::config::ENABLE_OTEL_TRACING;
use crate::config::ENABLE_RECORDER_TRACING;
use crate::config::ENABLE_SQLITE_TRACING;
use crate::config::MONARCH_FILE_LOG_LEVEL;
use crate::config::MONARCH_LOG_SUFFIX;
use crate::config::USE_UNIFIED_LAYER;
use crate::recorder::Recorder;
use crate::sqlite::get_reloadable_sqlite_layer;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TelemetrySample {
    fields: Vec<(String, String)>,
}

impl TelemetrySample {
    pub fn get_string(&self, key: &str) -> Option<&str> {
        for (k, v) in &self.fields {
            if k == key {
                return Some(v.as_str());
            }
        }
        None
    }
}

#[cfg(fbcode_build)]
impl From<crate::meta::sample_buffer::Sample> for TelemetrySample {
    fn from(sample: crate::meta::sample_buffer::Sample) -> Self {
        let mut fields = Vec::new();
        for (key, value) in sample.0 {
            if let crate::meta::sample_buffer::SampleValue::String(s) = value {
                fields.push((key.to_string(), s.to_string()));
            }
        }
        Self { fields }
    }
}

#[cfg(not(fbcode_build))]
impl TelemetrySample {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }
}

pub trait TelemetryTestHandle {
    fn get_tracing_samples(&self) -> Vec<TelemetrySample>;
}

#[cfg(fbcode_build)]
struct MockScubaHandle {
    tracing_client: crate::meta::scuba_utils::MockScubaClient,
}

#[cfg(fbcode_build)]
impl TelemetryTestHandle for MockScubaHandle {
    fn get_tracing_samples(&self) -> Vec<TelemetrySample> {
        self.tracing_client
            .get_samples()
            .into_iter()
            .map(TelemetrySample::from)
            .collect()
    }
}

struct EmptyTestHandle;

impl TelemetryTestHandle for EmptyTestHandle {
    fn get_tracing_samples(&self) -> Vec<TelemetrySample> {
        vec![]
    }
}

pub trait TelemetryClock {
    fn now(&self) -> tokio::time::Instant;
    fn system_time_now(&self) -> std::time::SystemTime;
}

pub struct DefaultTelemetryClock {}

impl TelemetryClock for DefaultTelemetryClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }

    fn system_time_now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
}

pub fn username() -> String {
    let env = env::Env::current();
    match env {
        env::Env::Mast => {
            std::env::var("MAST_JOB_OWNER_UNIXNAME").unwrap_or_else(|_| "mast_owner".to_string())
        }
        _ => whoami::username(),
    }
}

// Given an environment, determine the log file path to write to.
// If a suffix is provided, it will be prepended with "_" and then appended to file name
pub fn log_file_path(
    env: env::Env,
    suffix: Option<&str>,
) -> Result<(String, String), anyhow::Error> {
    let suffix = suffix
        .map(|s| {
            if s.is_empty() {
                String::new()
            } else {
                format!("_{}", s)
            }
        })
        .unwrap_or_default();
    match env {
        env::Env::Local | env::Env::MastEmulator => {
            let username = if whoami::username().is_empty() {
                "monarch".to_string()
            } else {
                whoami::username()
            };
            Ok((
                format!("/tmp/{}", username),
                format!("monarch_log{}", suffix),
            ))
        }
        env::Env::Mast => Ok((
            "/logs/".to_string(),
            format!("dedicated_log_monarch{}", suffix),
        )),
        _ => Err(anyhow::anyhow!(
            "file writer unsupported for environment {}",
            env
        )),
    }
}

fn try_create_appender(
    path: &str,
    filename: &str,
    create_dir: bool,
) -> Result<RollingFileAppender, Box<dyn std::error::Error>> {
    if create_dir {
        std::fs::create_dir_all(path)?;
    }
    Ok(RollingFileAppender::builder()
        .filename_prefix(filename)
        .filename_suffix("log")
        .build(path)?)
}

fn writer() -> Box<dyn Write + Send> {
    match env::Env::current() {
        env::Env::Test => Box::new(std::io::stderr()),
        env::Env::Local | env::Env::MastEmulator | env::Env::Mast => {
            let suffix = hyperactor_config::global::try_get_cloned(MONARCH_LOG_SUFFIX);
            let (path, filename) = log_file_path(env::Env::current(), suffix.as_deref()).unwrap();
            match try_create_appender(&path, &filename, true) {
                Ok(file_appender) => Box::new(file_appender),
                Err(e) => {
                    eprintln!(
                        "unable to create log file in {}: {}. Falling back to stderr",
                        path, e
                    );
                    Box::new(std::io::stderr())
                }
            }
        }
    }
}

lazy_static! {
    static ref TELEMETRY_CLOCK: Arc<Mutex<Box<dyn TelemetryClock + Send>>> =
        Arc::new(Mutex::new(Box::new(DefaultTelemetryClock {})));
    /// Global control channel for sink registration.
    /// Created upfront so sinks can be registered at any time (before or after telemetry init).
    /// The receiver is taken once when the TraceEventDispatcher is created.
    static ref SINK_CONTROL_CHANNEL: (
        mpsc::Sender<DispatcherControl>,
        Mutex<Option<mpsc::Receiver<DispatcherControl>>>
    ) = {
        let (sender, receiver) = mpsc::channel();
        (sender, Mutex::new(Some(receiver)))
    };
}

/// Register a sink to receive trace events.
/// This can be called at any time - before or after telemetry initialization.
/// The sink will receive all trace events on the background worker thread.
///
/// # Example
/// ```ignore
/// use hyperactor_telemetry::{register_sink, TraceEventSink, TraceEvent};
///
/// struct MySink;
/// impl TraceEventSink for MySink {
///     fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> { Ok(()) }
///     fn flush(&mut self) -> Result<(), anyhow::Error> { Ok(()) }
/// }
///
/// register_sink(Box::new(MySink));
/// ```
pub fn register_sink(sink: Box<dyn TraceEventSink>) {
    let sender = &SINK_CONTROL_CHANNEL.0;
    if let Err(e) = sender.send(DispatcherControl::AddSink(sink)) {
        eprintln!("[telemetry] failed to register sink: {}", e);
    }
}

/// Take the control receiver for use by the TraceEventDispatcher.
/// This can only be called once; subsequent calls return None.
pub(crate) fn take_sink_control_receiver() -> Option<mpsc::Receiver<DispatcherControl>> {
    SINK_CONTROL_CHANNEL.1.lock().unwrap().take()
}

/// The recorder singleton that is configured as a layer in the the default tracing
/// subscriber, as configured by `initialize_logging`.
pub fn recorder() -> &'static Recorder {
    static RECORDER: std::sync::OnceLock<Recorder> = std::sync::OnceLock::new();
    RECORDER.get_or_init(Recorder::new)
}

/// Hotswap the telemetry clock at runtime. This allows changing the clock implementation
/// after initialization, which is useful for testing or switching between real and simulated time.
pub fn swap_telemetry_clock(clock: impl TelemetryClock + Send + 'static) {
    *TELEMETRY_CLOCK.lock().unwrap() = Box::new(clock);
}

/// Create key value pairs for use in opentelemetry. These pairs can be stored and used multiple
/// times. Opentelemetry adds key value attributes when you bump counters and histograms.
/// so MY_COUNTER.add(42, &[key_value!("key", "value")])  and MY_COUNTER.add(42, &[key_value!("key", "other_value")]) will actually bump two separete counters.
#[macro_export]
macro_rules! key_value {
    ($key:expr, $val:expr) => {
        $crate::opentelemetry::KeyValue::new(
            $crate::opentelemetry::Key::new($key),
            $crate::opentelemetry::Value::from($val),
        )
    };
}
/// Construct the key value attribute slice using mapping syntax.
/// Example:
/// ```
/// # #[macro_use] extern crate hyperactor_telemetry;
/// # fn main() {
/// assert_eq!(
///     kv_pairs!("1" => "1", "2" => 2, "3" => 3.0),
///     &[
///         key_value!("1", "1"),
///         key_value!("2", 2),
///         key_value!("3", 3.0),
///     ],
/// );
/// # }
/// ```
#[macro_export]
macro_rules! kv_pairs {
    ($($k:expr => $v:expr),* $(,)?) => {
        &[$($crate::key_value!($k, $v),)*]
    };
}

#[derive(Debug, Clone, Copy)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

impl TimeUnit {
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeUnit::Millis => "ms",
            TimeUnit::Micros => "us",
            TimeUnit::Nanos => "ns",
        }
    }
}
pub struct Timer(opentelemetry::metrics::Histogram<u64>, TimeUnit);

impl<'a> Timer {
    pub fn new(data: opentelemetry::metrics::Histogram<u64>, unit: TimeUnit) -> Self {
        Timer(data, unit)
    }
    pub fn start(&'static self, pairs: &'a [opentelemetry::KeyValue]) -> TimerGuard<'a> {
        TimerGuard {
            data: self,
            pairs,
            start: Instant::now(),
        }
    }

    pub fn record(&'static self, dur: std::time::Duration, pairs: &'a [opentelemetry::KeyValue]) {
        let dur = match self.1 {
            TimeUnit::Millis => dur.as_millis(),
            TimeUnit::Micros => dur.as_micros(),
            TimeUnit::Nanos => dur.as_nanos(),
        } as u64;

        self.0.record(dur, pairs);
    }
}
pub struct TimerGuard<'a> {
    data: &'static Timer,
    pairs: &'a [opentelemetry::KeyValue],
    start: Instant,
}

impl<'a> Drop for TimerGuard<'a> {
    fn drop(&mut self) {
        let now = Instant::now();
        let dur = now.duration_since(self.start);
        self.data.record(dur, self.pairs);
    }
}

/// Create a thread safe static timer that can be used to measure durations.
/// This macro creates a histogram with predefined boundaries appropriate for the specified time unit.
/// Supported units are "ms" (milliseconds), "us" (microseconds), and "ns" (nanoseconds).
///
/// Example:
/// ```
/// # #[macro_use] extern crate hyperactor_telemetry;
/// # fn main() {
/// declare_static_timer!(REQUEST_TIMER, "request_processing_time", hyperactor_telemetry::TimeUnit::Millis);
///
/// {
///     let _ = REQUEST_TIMER.start(kv_pairs!("endpoint" => "/api/users", "method" => "GET"));
///     // do something expensive
/// }
/// # }
/// ```
#[macro_export]
macro_rules! declare_static_timer {
    ($name:ident, $key:expr, $unit:path) => {
        #[doc = "a global histogram timer named: "]
        #[doc = $key]
        pub static $name: std::sync::LazyLock<$crate::Timer> = std::sync::LazyLock::new(|| {
            $crate::Timer::new(
                $crate::meter(module_path!())
                    .u64_histogram(format!("{}.{}", $key, $unit.as_str()))
                    .with_unit($unit.as_str())
                    .build(),
                $unit,
            )
        });
    };
}

/// Create a thread safe static counter that can be incremeneted or decremented.
/// This is useful to avoid creating temporary counters.
/// You can safely create counters with the same name. They will be joined by the underlying
/// runtime and are thread safe.
///
/// Example:
/// ```
/// struct Url {
///     pub path: String,
///     pub proto: String,
/// }
///
/// # #[macro_use] extern crate hyperactor_telemetry;
/// # fn main() {
/// # let url = Url{path: "/request/1".into(), proto: "https".into()};
/// declare_static_counter!(REQUESTS_RECEIVED, "requests_received");
///
/// REQUESTS_RECEIVED.add(40, kv_pairs!("path" => url.path, "proto" => url.proto))
///
/// # }
/// ```
#[macro_export]
macro_rules! declare_static_counter {
    ($name:ident, $key:expr) => {
        #[doc = "a global counter named: "]
        #[doc = $key]
        pub static $name: std::sync::LazyLock<opentelemetry::metrics::Counter<u64>> =
            std::sync::LazyLock::new(|| $crate::meter(module_path!()).u64_counter($key).build());
    };
}

/// Create a thread safe static counter that can be incremeneted or decremented.
/// This is useful to avoid creating temporary counters.
/// You can safely create counters with the same name. They will be joined by the underlying
/// runtime and are thread safe.
///
/// Example:
/// ```
/// struct Url {
///     pub path: String,
///     pub proto: String,
/// }
///
/// # #[macro_use] extern crate hyperactor_telemetry;
/// # fn main() {
/// # let url = Url{path: "/request/1".into(), proto: "https".into()};
/// declare_static_counter!(REQUESTS_RECEIVED, "requests_received");
///
/// REQUESTS_RECEIVED.add(40, kv_pairs!("path" => url.path, "proto" => url.proto))
///
/// # }
/// ```
#[macro_export]
macro_rules! declare_static_up_down_counter {
    ($name:ident, $key:expr) => {
        #[doc = "a global up down counter named: "]
        #[doc = $key]
        pub static $name: std::sync::LazyLock<opentelemetry::metrics::UpDownCounter<i64>> =
            std::sync::LazyLock::new(|| {
                $crate::meter(module_path!())
                    .i64_up_down_counter($key)
                    .build()
            });
    };
}

/// Create a thread safe static gauge that can be set to a specific value.
/// This is useful to avoid creating temporary gauges.
/// You can safely create gauges with the same name. They will be joined by the underlying
/// runtime and are thread safe.
///
/// Example:
/// ```
/// struct System {
///     pub memory_usage: f64,
///     pub cpu_usage: f64,
/// }
///
/// # #[macro_use] extern crate hyperactor_telemetry;
/// # fn main() {
/// # let system = System{memory_usage: 512.5, cpu_usage: 25.0};
/// declare_static_gauge!(MEMORY_USAGE, "memory_usage");
///
/// MEMORY_USAGE.record(system.memory_usage, kv_pairs!("unit" => "MB", "process" => "hyperactor"))
///
/// # }
/// ```
#[macro_export]
macro_rules! declare_static_gauge {
    ($name:ident, $key:expr) => {
        #[doc = "a global gauge named: "]
        #[doc = $key]
        pub static $name: std::sync::LazyLock<opentelemetry::metrics::Gauge<f64>> =
            std::sync::LazyLock::new(|| $crate::meter(module_path!()).f64_gauge($key).build());
    };
}
/// Create a thread safe static observable gauge that can be set to a specific value based on the provided callback.
/// This is useful for metrics that need to be calculated or retrieved dynamically.
/// The callback will be executed whenever the gauge is observed by the metrics system.
///
/// Example:
/// ```
/// # #[macro_use] extern crate hyperactor_telemetry;
///
/// # fn main() {
/// declare_observable_gauge!(MEMORY_USAGE_GAUGE, "memory_usage", |observer| {
///     // Simulate getting memory usage - this could be any complex operation
///     observer.observe(512.0, &[]);
/// });
///
/// // The gauge will be automatically updated when observed
/// # }
/// ```
#[macro_export]
macro_rules! declare_observable_gauge {
    ($name:ident, $key:expr, $cb:expr) => {
        #[doc = "a global gauge named: "]
        #[doc = $key]
        pub static $name: std::sync::LazyLock<opentelemetry::metrics::ObservableGauge<f64>> =
            std::sync::LazyLock::new(|| {
                $crate::meter(module_path!())
                    .f64_observable_gauge($key)
                    .with_callback($cb)
                    .build()
            });
    };
}
/// Create a thread safe static histogram that can be incremeneted or decremented.
/// This is useful to avoid creating temporary histograms.
/// You can safely create histograms with the same name. They will be joined by the underlying
/// runtime and are thread safe.
///
/// Example:
/// ```
/// struct Url {
///     pub path: String,
///     pub proto: String,
/// }
///
/// # #[macro_use] extern crate hyperactor_telemetry;
/// # fn main() {
/// # let url = Url{path: "/request/1".into(), proto: "https".into()};
/// declare_static_histogram!(REQUEST_LATENCY, "request_latency");
///
/// REQUEST_LATENCY.record(40.0, kv_pairs!("path" => url.path, "proto" => url.proto))
///
/// # }
/// ```
#[macro_export]
macro_rules! declare_static_histogram {
    ($name:ident, $key:expr) => {
        #[doc = "a global histogram named: "]
        #[doc = $key]
        pub static $name: std::sync::LazyLock<opentelemetry::metrics::Histogram<f64>> =
            std::sync::LazyLock::new(|| {
                hyperactor_telemetry::meter(module_path!())
                    .f64_histogram($key)
                    .build()
            });
    };
}

static FILE_WRITER_GUARD: std::sync::OnceLock<Arc<(NonBlocking, WorkerGuard)>> =
    std::sync::OnceLock::new();

/// A custom formatter that prepends prefix from env_var to log messages.
struct PrefixedFormatter {
    formatter: Glog<LocalTime>,
    prefix_env_var: Option<String>,
}

impl PrefixedFormatter {
    fn new(prefix_env_var: Option<String>) -> Self {
        let formatter = Glog::default().with_timer(LocalTime::default());
        Self {
            formatter,
            prefix_env_var,
        }
    }
}

impl<S, N> FormatEvent<S, N> for PrefixedFormatter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let prefix: String = if self.prefix_env_var.is_some() {
            std::env::var(self.prefix_env_var.clone().unwrap()).unwrap_or_default()
        } else {
            "".to_string()
        };

        if prefix.is_empty() {
            write!(writer, "[-]")?;
        } else {
            write!(writer, "[{}]", prefix)?;
        }

        self.formatter.format_event(ctx, writer, event)
    }
}

/// Set up logging based on the given execution environment. We specialize logging based on how the
/// logs are consumed. The destination scuba table is specialized based on the execution environment.
/// mast -> monarch_tracing/prod
/// devserver -> monarch_tracing/local
/// unit test  -> monarch_tracing/test
/// scuba logging won't normally be enabled for a unit test unless we are specifically testing logging, so
/// you don't need to worry about your tests being flakey due to scuba logging. You have to manually call initialize_logging()
/// to get this behavior.
pub fn initialize_logging(clock: impl TelemetryClock + Send + 'static) {
    initialize_logging_with_log_prefix(clock, None);
}

/// testing
pub fn initialize_logging_for_test() {
    initialize_logging(DefaultTelemetryClock {});
}

/// Set up logging based on the given execution environment. We specialize logging based on how the
/// logs are consumed. The destination scuba table is specialized based on the execution environment.
/// mast -> monarch_tracing/prod
/// devserver -> monarch_tracing/local
/// unit test  -> monarch_tracing/test
/// scuba logging won't normally be enabled for a unit test unless we are specifically testing logging, so
/// you don't need to worry about your tests being flakey due to scuba logging. You have to manually call initialize_logging()
/// to get this behavior.
///
/// tracing logs will be prefixed with the given prefix and routed to:
/// test -> stderr
/// local -> /tmp/monarch_log.log
/// mast -> /logs/dedicated_monarch_logs.log
/// Additionally, is MONARCH_STDERR_LOG sets logs level, then logs will be routed to stderr as well.
pub fn initialize_logging_with_log_prefix(
    clock: impl TelemetryClock + Send + 'static,
    prefix_env_var: Option<String>,
) {
    let _ = initialize_logging_with_log_prefix_impl(clock, prefix_env_var, false);
}

pub fn initialize_logging_with_log_prefix_mock_scuba(
    clock: impl TelemetryClock + Send + 'static,
    prefix_env_var: Option<String>,
) -> Box<dyn TelemetryTestHandle> {
    initialize_logging_with_log_prefix_impl(clock, prefix_env_var, true)
}

fn initialize_logging_with_log_prefix_impl(
    clock: impl TelemetryClock + Send + 'static,
    prefix_env_var: Option<String>,
    mock_scuba: bool,
) -> Box<dyn TelemetryTestHandle> {
    let use_unified = hyperactor_config::global::get(USE_UNIFIED_LAYER);

    swap_telemetry_clock(clock);
    let file_log_level = match env::Env::current() {
        env::Env::Local => LOG_LEVEL_INFO,
        env::Env::MastEmulator => LOG_LEVEL_INFO,
        env::Env::Mast => LOG_LEVEL_INFO,
        env::Env::Test => LOG_LEVEL_DEBUG,
    };

    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[cfg(fbcode_build)]
    {
        let mut mock_scuba_client: Option<crate::meta::scuba_utils::MockScubaClient> = None;

        if use_unified {
            let mut sinks: Vec<Box<dyn trace_dispatcher::TraceEventSink>> = Vec::new();
            sinks.push(Box::new(sinks::glog::GlogSink::new(
                writer(),
                prefix_env_var.clone(),
                file_log_level,
            )));

            let sqlite_enabled = hyperactor_config::global::get(ENABLE_SQLITE_TRACING);

            if sqlite_enabled {
                match create_sqlite_sink() {
                    Ok(sink) => {
                        sinks.push(Box::new(sink));
                    }
                    Err(e) => {
                        tracing::warn!("failed to create SqliteSink: {}", e);
                    }
                }
            }

            if hyperactor_config::global::get(sinks::perfetto::PERFETTO_TRACE_MODE)
                != sinks::perfetto::PerfettoTraceMode::Off
            {
                let exec_id = env::execution_id();
                let process_name = std::env::var("HYPERACTOR_PROCESS_NAME")
                    .unwrap_or_else(|_| "client".to_string());
                match sinks::perfetto::PerfettoFileSink::new(
                    sinks::perfetto::default_trace_dir(),
                    &exec_id,
                    &process_name,
                ) {
                    Ok(sink) => {
                        sinks.push(Box::new(sink));
                    }
                    Err(e) => {
                        tracing::warn!("failed to create PerfettoFileSink: {}", e);
                    }
                }
            }

            {
                if hyperactor_config::global::get(ENABLE_OTEL_TRACING) {
                    use crate::meta;
                    use crate::meta::get_tracing_targets;
                    use crate::meta::scuba_utils::LOG_ENTER_EXIT;

                    if mock_scuba {
                        let tracing_client = meta::scuba_utils::MockScubaClient::new();

                        sinks.push(Box::new(
                            meta::scuba_sink::ScubaSink::with_client(
                                tracing_client.clone(),
                                match meta::tracing_resource().get(&LOG_ENTER_EXIT) {
                                    Some(Value::Bool(enabled)) => enabled,
                                    _ => false,
                                },
                            )
                            .with_target_filter(get_tracing_targets()),
                        ));

                        mock_scuba_client = Some(tracing_client);
                    } else {
                        sinks.push(Box::new(
                            meta::scuba_sink::ScubaSink::new(meta::tracing_resource())
                                .with_target_filter(get_tracing_targets()),
                        ));
                    }
                }
            }

            let dispatcher = trace_dispatcher::TraceEventDispatcher::new(sinks);

            if let Err(err) = Registry::default()
                .with(if hyperactor_config::global::get(ENABLE_RECORDER_TRACING) {
                    Some(recorder().layer())
                } else {
                    None
                })
                .with(dispatcher)
                .try_init()
            {
                tracing::debug!("logging already initialized for this process: {}", err);
            }
        } else {
            // For file_layer, use NonBlocking
            let (non_blocking, guard) =
                tracing_appender::non_blocking::NonBlockingBuilder::default()
                    .lossy(false)
                    .finish(writer());
            let writer_guard = Arc::new((non_blocking, guard));
            let _ = FILE_WRITER_GUARD.set(writer_guard.clone());

            let file_layer = fmt::Layer::default()
                .with_writer(writer_guard.0.clone())
                .event_format(PrefixedFormatter::new(prefix_env_var.clone()))
                .fmt_fields(GlogFields::default().compact())
                .with_ansi(false)
                .with_filter(
                    Targets::new()
                        .with_default(LevelFilter::from_level({
                            let log_level_str =
                                hyperactor_config::global::try_get_cloned(MONARCH_FILE_LOG_LEVEL)
                                    .unwrap_or_else(|| file_log_level.to_string());
                            tracing::Level::from_str(&log_level_str).unwrap_or_else(|_| {
                                tracing::Level::from_str(file_log_level)
                                    .expect("Invalid default log level")
                            })
                        }))
                        .with_target("opentelemetry", LevelFilter::OFF), // otel has some log span under debug that we don't care about
                );

            let registry = Registry::default()
                .with(if hyperactor_config::global::get(ENABLE_SQLITE_TRACING) {
                    // TODO: get_reloadable_sqlite_layer currently still returns None,
                    // and some additional work is required to make it work.
                    Some(get_reloadable_sqlite_layer().expect("failed to create sqlite layer"))
                } else {
                    None
                })
                .with(file_layer)
                .with(if hyperactor_config::global::get(ENABLE_RECORDER_TRACING) {
                    Some(recorder().layer())
                } else {
                    None
                });

            if mock_scuba {
                let tracing_client = crate::meta::scuba_utils::MockScubaClient::new();

                let scuba_layer = crate::meta::tracing_layer_with_client(tracing_client.clone());

                if let Err(err) = registry.with(scuba_layer).try_init() {
                    tracing::debug!("logging already initialized for this process: {}", err);
                }

                mock_scuba_client = Some(tracing_client);
            } else if let Err(err) = registry
                .with(if hyperactor_config::global::get(ENABLE_OTEL_TRACING) {
                    Some(otel::tracing_layer())
                } else {
                    None
                })
                .try_init()
            {
                tracing::debug!("logging already initialized for this process: {}", err);
            }
        }
        let exec_id = env::execution_id();
        let process_name =
            std::env::var("HYPERACTOR_PROCESS_NAME").unwrap_or_else(|_| "client".to_string());

        // setting target to "execution" will prevent the monarch_tracing scuba client from logging this
        tracing::info!(
            target: "execution",
            execution_id = exec_id,
            environment = %env::Env::current(),
            args = ?std::env::args(),
            build_mode = build_info::BuildInfo::get_build_mode(),
            compiler = build_info::BuildInfo::get_compiler(),
            compiler_version = build_info::BuildInfo::get_compiler_version(),
            buck_rule = build_info::BuildInfo::get_rule(),
            package_name = build_info::BuildInfo::get_package_name(),
            package_release = build_info::BuildInfo::get_package_release(),
            upstream_revision = build_info::BuildInfo::get_upstream_revision(),
            revision = build_info::BuildInfo::get_revision(),
            process_name = process_name,
            "logging_initialized",
        );
        // here we have the monarch_executions scuba client log
        meta::log_execution_event(
            &exec_id,
            &env::Env::current().to_string(),
            std::env::args().collect(),
            build_info::BuildInfo::get_build_mode(),
            build_info::BuildInfo::get_compiler(),
            build_info::BuildInfo::get_compiler_version(),
            build_info::BuildInfo::get_rule(),
            build_info::BuildInfo::get_package_name(),
            build_info::BuildInfo::get_package_release(),
            build_info::BuildInfo::get_upstream_revision(),
            build_info::BuildInfo::get_revision(),
            &process_name,
        );

        if hyperactor_config::global::get(ENABLE_OTEL_METRICS) {
            otel::init_metrics();
        }

        if let Some(tracing_client) = mock_scuba_client {
            Box::new(MockScubaHandle { tracing_client })
        } else {
            Box::new(EmptyTestHandle)
        }
    }
    #[cfg(not(fbcode_build))]
    {
        let registry =
            Registry::default().with(if hyperactor_config::global::get(ENABLE_RECORDER_TRACING) {
                Some(recorder().layer())
            } else {
                None
            });

        if use_unified {
            let mut sinks: Vec<Box<dyn trace_dispatcher::TraceEventSink>> = Vec::new();

            let sqlite_enabled = hyperactor_config::global::get(ENABLE_SQLITE_TRACING);

            if sqlite_enabled {
                match create_sqlite_sink() {
                    Ok(sink) => {
                        sinks.push(Box::new(sink));
                    }
                    Err(e) => {
                        tracing::warn!("failed to create SqliteSink: {}", e);
                    }
                }
            }

            sinks.push(Box::new(sinks::glog::GlogSink::new(
                writer(),
                prefix_env_var.clone(),
                file_log_level,
            )));

            let dispatcher = trace_dispatcher::TraceEventDispatcher::new(sinks);

            if let Err(err) = registry.with(dispatcher).try_init() {
                tracing::debug!("logging already initialized for this process: {}", err);
            }
        } else {
            let (non_blocking, guard) =
                tracing_appender::non_blocking::NonBlockingBuilder::default()
                    .lossy(false)
                    .finish(writer());
            let writer_guard = Arc::new((non_blocking, guard));
            let _ = FILE_WRITER_GUARD.set(writer_guard.clone());

            let file_layer = fmt::Layer::default()
                .with_writer(writer_guard.0.clone())
                .event_format(PrefixedFormatter::new(prefix_env_var.clone()))
                .fmt_fields(GlogFields::default().compact())
                .with_ansi(false)
                .with_filter(
                    Targets::new()
                        .with_default(LevelFilter::from_level({
                            let log_level_str =
                                hyperactor_config::global::try_get_cloned(MONARCH_FILE_LOG_LEVEL)
                                    .unwrap_or_else(|| file_log_level.to_string());
                            tracing::Level::from_str(&log_level_str).unwrap_or_else(|_| {
                                tracing::Level::from_str(file_log_level)
                                    .expect("Invalid default log level")
                            })
                        }))
                        .with_target("opentelemetry", LevelFilter::OFF), // otel has some log span under debug that we don't care about
                );

            if let Err(err) = registry.with(file_layer).try_init() {
                tracing::debug!("logging already initialized for this process: {}", err);
            }
        }

        Box::new(EmptyTestHandle)
    }
}

fn create_sqlite_sink() -> anyhow::Result<sinks::sqlite::SqliteSink> {
    let (db_path, _) = log_file_path(env::Env::current(), Some("traces"))
        .expect("failed to determine trace db path");
    let db_file = format!("{}/hyperactor_trace_{}.db", db_path, std::process::id());

    sinks::sqlite::SqliteSink::new_with_file(&db_file, 100)
}

/// Create a context span at ERROR level with skip_record enabled.
/// This is intended to create spans whose only purpose it is to add context
/// to child events; the span itself is never independently recorded.
///
/// Example:
/// ```ignore
/// use hyperactor_telemetry::context_span;
///
/// let span = context_span!("my_context", field1 = value1, field2 = value2);
/// let _guard = span.enter();
/// // ... do work that will be logged with this context
/// ```
#[macro_export]
macro_rules! context_span {
    (target: $target:expr, parent: $parent:expr, $name:expr, $($field:tt)*) => {
        ::tracing::error_span!(
            target: $target,
            parent: $parent,
            $name,
            skip_record = $crate::skip_record,
            $($field)*
        )
    };
    (target: $target:expr, parent: $parent:expr, $name:expr) => {
        ::tracing::error_span!(
            target: $target,
            parent: $parent,
            $name,
            skip_record = $crate::skip_record,
        )
    };
    (parent: $parent:expr, $name:expr, $($field:tt)*) => {
        ::tracing::error_span!(
            target: module_path!(),
            parent: $parent,
            $name,
            skip_record = $crate::skip_record,
            $($field)*
        )
    };
    (parent: $parent:expr, $name:expr) => {
        ::tracing::error_span!(
            parent: $parent,
            $name,
            skip_record = $crate::skip_record,
        )
    };
    (target: $target:expr, $name:expr, $($field:tt)*) => {
        ::tracing::error_span!(
            target: $target,
            $name,
            skip_record = $crate::skip_record,
            $($field)*
        )
    };
    (target: $target:expr, $name:expr) => {
        ::tracing::error_span!(
            target: $target,
            $name,
            skip_record = $crate::skip_record,
        )
    };
    ($name:expr, $($field:tt)*) => {
        ::tracing::error_span!(
            target: module_path!(),
            $name,
            skip_record = $crate::skip_record,
            $($field)*
        )
    };
    ($name:expr) => {
        ::tracing::error_span!(
            $name,
            skip_record = $crate::skip_record,
        )
    };
}

pub mod env {
    use rand::RngCore;

    /// Env var name set when monarch launches subprocesses to forward the execution context
    pub const HYPERACTOR_EXECUTION_ID_ENV: &str = "HYPERACTOR_EXECUTION_ID";
    pub const OTEL_EXPORTER: &str = "HYPERACTOR_OTEL_EXPORTER";
    pub const MAST_ENVIRONMENT: &str = "MAST_ENVIRONMENT";

    /// Forward or generate a uuid for this execution. When running in production on mast, this is provided to
    /// us via the MAST_HPC_JOB_NAME env var. Subprocesses should either forward the MAST_HPC_JOB_NAME
    /// variable, or set the "MONARCH_EXECUTION_ID" var for subprocesses launched by this process.
    /// We keep these env vars separate so that other applications that depend on the MAST_HPC_JOB_NAME existing
    /// to understand their environment do not get confused and think they are running on mast when we are doing
    ///  local testing.
    pub fn execution_id() -> String {
        let id = std::env::var(HYPERACTOR_EXECUTION_ID_ENV).unwrap_or_else(|_| {
            // not able to find an existing id so generate a unique one: username + current_time + random number.
            let username = crate::username();
            let now = {
                let now = std::time::SystemTime::now();
                let datetime: chrono::DateTime<chrono::Local> = now.into();
                datetime.format("%b-%d_%H:%M").to_string()
            };
            let random_number: u16 = (rand::rng().next_u32() % 1000) as u16;
            let execution_id = format!("{}_{}_{}", username, now, random_number);
            execution_id
        });
        // Safety: Can be unsound if there are multiple threads
        // reading and writing the environment.
        unsafe {
            std::env::set_var(HYPERACTOR_EXECUTION_ID_ENV, id.clone());
        }
        id
    }

    /// Returns a URL for the execution trace, if available.
    #[cfg(fbcode_build)]
    pub async fn execution_url() -> anyhow::Result<Option<String>> {
        Ok(Some(
            crate::meta::scuba_tracing::url::get_samples_shorturl(&execution_id()).await?,
        ))
    }
    #[cfg(not(fbcode_build))]
    pub async fn execution_url() -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    #[derive(PartialEq)]
    pub enum Env {
        Local,
        Mast,
        MastEmulator,
        Test,
    }

    impl std::fmt::Display for Env {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{}",
                match self {
                    Self::Local => crate::ENV_VALUE_LOCAL,
                    Self::MastEmulator => crate::ENV_VALUE_MAST_EMULATOR,
                    Self::Mast => crate::ENV_VALUE_MAST,
                    Self::Test => crate::ENV_VALUE_TEST,
                }
            )
        }
    }

    impl Env {
        #[cfg(test)]
        pub fn current() -> Self {
            Self::Test
        }

        #[cfg(not(test))]
        pub fn current() -> Self {
            match std::env::var(MAST_ENVIRONMENT).unwrap_or_default().as_str() {
                // Constant from https://fburl.com/fhysd3fd
                crate::ENV_VALUE_LOCAL_MAST_SIMULATOR => Self::MastEmulator,
                _ => match std::env::var(crate::MAST_HPC_JOB_NAME_ENV).is_ok() {
                    true => Self::Mast,
                    false => Self::Local,
                },
            }
        }
    }
}

#[cfg(test)]
mod test {
    use opentelemetry::*;
    extern crate self as hyperactor_telemetry;
    use super::*;

    #[test]
    fn infer_kv_pair_types() {
        assert_eq!(
            key_value!("str", "str"),
            KeyValue::new(Key::new("str"), Value::String("str".into()))
        );
        assert_eq!(
            key_value!("str", 25),
            KeyValue::new(Key::new("str"), Value::I64(25))
        );
        assert_eq!(
            key_value!("str", 1.1),
            KeyValue::new(Key::new("str"), Value::F64(1.1))
        );
    }
    #[test]
    fn kv_pair_slices() {
        assert_eq!(
            kv_pairs!("1" => "1", "2" => 2, "3" => 3.0),
            &[
                key_value!("1", "1"),
                key_value!("2", 2),
                key_value!("3", 3.0),
            ],
        );
    }

    #[test]
    fn test_static_gauge() {
        // Create a static gauge using the macro
        declare_static_gauge!(TEST_GAUGE, "test_gauge");
        declare_static_gauge!(MEMORY_GAUGE, "memory_usage");

        // Set values to the gauge with different attributes
        // This shouldn't actually log to scribe/scuba in test environment
        TEST_GAUGE.record(42.5, kv_pairs!("component" => "test", "unit" => "MB"));
        MEMORY_GAUGE.record(512.0, kv_pairs!("type" => "heap", "process" => "test"));

        // Test with empty attributes
        TEST_GAUGE.record(50.0, &[]);
    }
}
