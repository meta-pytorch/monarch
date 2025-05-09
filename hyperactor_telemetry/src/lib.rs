#![allow(internal_features)]
#![feature(assert_matches)]
#![feature(sync_unsafe_cell)]
#![feature(mpmc_channel)]
#![feature(cfg_version)]
#![feature(formatting_options)]

// TODO:ehedeman Remove or replace with better config once telemetry perf issues are solved
/// Environment variable to disable the glog logging layer.
/// Set to "1" to disable glog logging output.
pub const DISABLE_GLOG_TRACING: &str = "DISABLE_GLOG_TRACING";

/// Environment variable to disable the OpenTelemetry logging layer.
/// Set to "1" to disable OpenTelemetry  tracing.
pub const DISABLE_OTEL_TRACING: &str = "DISABLE_OTEL_TRACING";

/// Environment variable to disable the OpenTelemetry logging layer.
/// Set to "1" to disable OpenTelemetry metrics.
pub const DISABLE_OTEL_METRICS: &str = "DISABLE_OTEL_METRICS";

/// Environment variable to disable the recorder logging layer.
/// Set to "1" to disable the recorder output.
pub const DISABLE_RECORDER_TRACING: &str = "DISABLE_RECORDER_TRACING";

#[cfg(fbcode_build)]
mod meta;
mod otel;
mod pool;
pub mod recorder;
mod spool;
use std::io::IsTerminal;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use lazy_static::lazy_static;
pub use opentelemetry;
pub use opentelemetry::Key;
pub use opentelemetry::KeyValue;
pub use opentelemetry::Value;
pub use opentelemetry::global::meter;
pub use tracing::Level;
use tracing_appender::non_blocking::NonBlocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_glog::Glog;
use tracing_glog::GlogFields;
use tracing_glog::LocalTime;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt;

use crate::env::Env;
use crate::recorder::Recorder;

// Need to keep this around so that the tracing subscriber doesn't drop the writer.
lazy_static! {
    static ref WRITER_GUARD: Arc<(NonBlocking, WorkerGuard)> = {
        let writer: Box<dyn Write + Send> = match env::Env::current() {
            env::Env::Local | env::Env::Test | env::Env::MastEmulator => {
                Box::new(std::io::stderr())
            }
            env::Env::Mast => match RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .filename_prefix("dedicated_log_monarch")
                .filename_suffix("log")
                .build("/logs/")
            {
                Ok(file) => Box::new(file),
                Err(e) => {
                    tracing::warn!("unable to create custom log file: {}", e);
                    Box::new(std::io::stderr())
                }
            },
        };
        return Arc::new(
            tracing_appender::non_blocking::NonBlockingBuilder::default()
                .lossy(false)
                .finish(writer),
        );
    };
}

/// The recorder singleton that is configured as a layer in the the default tracing
/// subscriber, as configured by `initialize_logging`.
pub fn recorder() -> &'static Recorder {
    static RECORDER: std::sync::OnceLock<Recorder> = std::sync::OnceLock::new();
    RECORDER.get_or_init(Recorder::new)
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
    ($($k:expr => $v:expr),* $(,)?) => {{
        &[$($crate::key_value!($k, $v),)*]
    }};
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
            data: &self.0,
            pairs,
            start: Instant::now(),
            unit: self.1,
        }
    }
}
pub struct TimerGuard<'a> {
    data: &'a opentelemetry::metrics::Histogram<u64>,
    pairs: &'a [opentelemetry::KeyValue],
    start: Instant,
    unit: TimeUnit,
}

impl<'a> Drop for TimerGuard<'a> {
    fn drop(&mut self) {
        let now = Instant::now();
        let dur = now.duration_since(self.start);
        let dur = match self.unit {
            TimeUnit::Millis => dur.as_millis(),
            TimeUnit::Micros => dur.as_micros(),
            TimeUnit::Nanos => dur.as_nanos(),
        } as u64;

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
            std::sync::LazyLock::new(|| {
                hyperactor_telemetry::meter(module_path!())
                    .u64_counter($key)
                    .build()
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
macro_rules! declare_static_up_down_counter {
    ($name:ident, $key:expr) => {
        #[doc = "a global up down counter named: "]
        #[doc = $key]
        pub static $name: std::sync::LazyLock<opentelemetry::metrics::UpDownCounter<i64>> =
            std::sync::LazyLock::new(|| {
                hyperactor_telemetry::meter(module_path!())
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
            std::sync::LazyLock::new(|| {
                hyperactor_telemetry::meter(module_path!())
                    .f64_gauge($key)
                    .build()
            });
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
                hyperactor_telemetry::meter(module_path!())
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

/// Set up logging based on the given execution environment. We specialize logging based on how the
/// logs are consumed. The destination scuba table is specialized based on the execution environment.
/// mast -> monarch_tracing/prod
/// devserver -> monarch_tracing/local
/// unit test  -> monarch_tracing/test
/// scuba logging won't normally be enabled for a unit test unless we are specifically testing logging, so
/// you don't need to worry about your tests being flakey due to scuba logging. You have to manually call initialize_logging()
/// to get this behavior.
pub fn initialize_logging() {
    let glog_level = match env::Env::current() {
        env::Env::Local => "info",
        env::Env::MastEmulator => "info",
        env::Env::Mast => "info",
        env::Env::Test => "debug",
    };

    let writer: &NonBlocking = &WRITER_GUARD.0;
    let glog = fmt::Layer::default()
        .with_writer(writer.clone())
        .event_format(Glog::default().with_timer(LocalTime::default()))
        .fmt_fields(GlogFields::default().compact())
        .with_ansi(std::io::stderr().is_terminal())
        .with_filter(LevelFilter::from_level(
            tracing::Level::from_str(&std::env::var("RUST_LOG").unwrap_or(glog_level.to_string()))
                .expect("Invalid log level"),
        ));

    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[cfg(fbcode_build)]
    {
        fn is_layer_enabled(env_var: &str) -> bool {
            std::env::var(env_var).unwrap_or_default() != "1"
        }
        if let Err(err) = Registry::default()
            .with(if is_layer_enabled(DISABLE_OTEL_TRACING) {
                Some(otel::tracing_layer())
            } else {
                None
            })
            .with(if is_layer_enabled(DISABLE_GLOG_TRACING) {
                Some(glog)
            } else {
                None
            })
            .with(if is_layer_enabled(DISABLE_RECORDER_TRACING) {
                Some(recorder().layer())
            } else {
                None
            })
            .try_init()
        {
            tracing::debug!("logging already initialized for this process: {}", err);
        }
        let exec_id = env::execution_id();
        tracing::info!(
            target: "execution",
            execution_id = exec_id,
            environment = %Env::current(),
            args = ?std::env::args(),
            build_mode = build_info::BuildInfo::get_build_mode(),
            compiler = build_info::BuildInfo::get_compiler(),
            compiler_version = build_info::BuildInfo::get_compiler_version(),
            buck_rule = build_info::BuildInfo::get_rule(),
            package_name = build_info::BuildInfo::get_package_name(),
            package_release = build_info::BuildInfo::get_package_release(),
            upstream_revision = build_info::BuildInfo::get_revision(),
            "logging_initialized"
        );

        if is_layer_enabled(DISABLE_OTEL_METRICS) {
            otel::init_metrics();
        }
    }
    #[cfg(not(fbcode_build))]
    {
        if let Err(err) = Registry::default()
            .with(
                if std::env::var(DISABLE_GLOG_TRACING).unwrap_or_default() != "1" {
                    Some(glog)
                } else {
                    None
                },
            )
            .with(
                if std::env::var(DISABLE_RECORDER_TRACING).unwrap_or_default() != "1" {
                    Some(recorder().layer())
                } else {
                    None
                },
            )
            .try_init()
        {
            tracing::debug!("logging already initialized for this process: {}", err);
        }
    }
}

pub mod env {
    use rand::Rng;
    use rand::distributions::Alphanumeric;

    /// Env var name set when monarch launches subprocesses to forward the execution context
    pub const HYPERACTOR_EXECUTION_ID_ENV: &str = "HYPERACTOR_EXECUTION_ID";
    pub const MAST_HPC_JOB_NAME_ENV: &str = "MAST_HPC_JOB_NAME";
    pub const OTEL_EXPORTER: &str = "HYPERACTOR_OTEL_EXPORTER";
    const MAST_ENVIRONMENT: &str = "MAST_ENVIRONMENT";

    /// Forward or generate a uuid for this execution. When running in production on mast, this is provided to
    /// us via the MAST_HPC_JOB_NAME env var. Subprocesses should either forward the MAST_HPC_JOB_NAME
    /// variable, or set the "MONARCH_EXECUTION_ID" var for subprocesses launched by this process.
    /// We keep these env vars separate so that other applications that depend on the MAST_HPC_JOB_NAME existing
    /// to understand their environment do not get confused and think they are running on mast when we are doing
    ///  local testing.
    pub fn execution_id() -> String {
        let id = std::env::var(HYPERACTOR_EXECUTION_ID_ENV)
            .or(std::env::var(MAST_HPC_JOB_NAME_ENV))
            .ok()
            .unwrap_or_else(|| {
                // not able to find an existing id so generate a random one. 24 bytes should be sufficient.
                let random_string: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(24)
                    .map(char::from)
                    .collect::<String>();
                random_string
            });
        std::env::set_var(HYPERACTOR_EXECUTION_ID_ENV, id.clone());
        id
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
                    Self::Local => "local",
                    Self::MastEmulator => "mast_emulator",
                    Self::Mast => "mast",
                    Self::Test => "test",
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
                "local_mast_simulator" => Self::MastEmulator,
                _ => match std::env::var("MAST_HPC_JOB_NAME").is_ok() {
                    true => Self::Mast,
                    false => Self::Local,
                },
            }
        }
    }

    pub fn exporter_name() -> String {
        std::env::var(OTEL_EXPORTER).unwrap_or("scribe_cat".into())
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
