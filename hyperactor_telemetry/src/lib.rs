/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(internal_features)]
#![feature(sync_unsafe_cell)]
#![feature(mpmc_channel)]
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

/// Well-known tracing field name for the log subject.
/// Spans carrying this field identify the entity (actor, proc, etc.)
/// that log events within the span pertain to.
pub const SUBJECT_KEY: &str = "subject";

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
#[cfg(all(fbcode_build, target_os = "linux"))]
mod meta;
mod otel;
pub(crate) mod otlp;
mod pool;
mod rate_limit;
pub mod recorder;
pub mod sinks;
mod spool;
pub mod sqlite;
pub mod task;
pub mod trace;
pub mod trace_dispatcher;
mod unix_sink;

// Re-export key types for external sink implementations
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Write;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Instant;
use std::time::SystemTime;

pub use opentelemetry;
pub use opentelemetry::Key;
pub use opentelemetry::KeyValue;
pub use opentelemetry::Value;
pub use opentelemetry::global::meter;
pub use trace_dispatcher::DispatcherControl;
pub use trace_dispatcher::FieldValue;
pub use trace_dispatcher::TraceEvent;
pub use trace_dispatcher::TraceEventSink;
use trace_dispatcher::TraceFields;
pub use tracing;
pub use tracing::Level;
use tracing_appender::rolling::RollingFileAppender;
pub use unix_sink::set_unix_socket_sink_path;
pub use unix_sink::unix_socket_sink_dropped_frames;
pub use unix_sink::unix_socket_sink_is_active;

#[cfg(all(fbcode_build, target_os = "linux"))]
use crate::config::ENABLE_OTEL_METRICS;
#[cfg(all(fbcode_build, target_os = "linux"))]
use crate::config::ENABLE_OTEL_TRACING;
use crate::config::ENABLE_RECORDER_TRACING;
use crate::config::ENABLE_SQLITE_TRACING;
use crate::config::MONARCH_LOG_SUFFIX;
use crate::recorder::Recorder;

/// Hash any hashable value to a u64 using DefaultHasher.
pub fn hash_to_u64(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
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

static TELEMETRY_CLOCK: LazyLock<Arc<Mutex<Box<dyn TelemetryClock + Send>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(Box::new(DefaultTelemetryClock {}))));

/// Global sender into the active `TraceEventDispatcher` queue.
///
/// This is for `TraceEvent`s synthesized outside normal `tracing` subscriber callbacks,
/// such as Python user spans. Once telemetry initializes and constructs the dispatcher,
/// we stash its sender here so those synthetic events flow through the same sink fan-out
/// path as native Rust tracing events.
static SYNTHETIC_TRACE_EVENT_SENDER: Mutex<Option<mpsc::SyncSender<TraceEvent>>> = Mutex::new(None);

/// Global control channel for sink registration.
/// Created upfront so sinks can be registered at any time (before or after telemetry init).
/// The receiver is taken once when the TraceEventDispatcher is created.
static SINK_CONTROL_CHANNEL: LazyLock<(
    mpsc::Sender<DispatcherControl>,
    Mutex<Option<mpsc::Receiver<DispatcherControl>>>,
)> = LazyLock::new(|| {
    let (sender, receiver) = mpsc::channel();
    (sender, Mutex::new(Some(receiver)))
});

const SYNTHETIC_USER_SPAN_ID_BASE: u64 = 1 << 63;
static USER_SPAN_SEQ: AtomicU64 = AtomicU64::new(SYNTHETIC_USER_SPAN_ID_BASE);
const SYNTHETIC_USER_SPAN_TRACK_NAME: &str = "python";

/// Install the sender for the active dispatcher so synthesized events can join the
/// same pipeline as events captured from native `tracing` callbacks.
pub(crate) fn set_synthetic_trace_event_sender(sender: mpsc::SyncSender<TraceEvent>) {
    *SYNTHETIC_TRACE_EVENT_SENDER
        .lock()
        .expect("SYNTHETIC_TRACE_EVENT_SENDER mutex should not be poisoned") = Some(sender);
}

/// Sends a synthesized trace event to the active dispatcher queue.
/// Returns `true` if sent successfully.
pub(crate) fn emit_trace_event(event: TraceEvent) -> bool {
    match synthetic_trace_event_sender() {
        Some(sender) => sender.try_send(event).is_ok(),
        None => false,
    }
}

/// Begins a user-defined span and returns its id. Returns 0 if the dispatcher is not initialized.
pub fn start_user_span(
    name: &'static str,
    target: &'static str,
    fields: impl IntoIterator<Item = (&'static str, FieldValue)>,
) -> u64 {
    if SYNTHETIC_TRACE_EVENT_SENDER
        .lock()
        .expect("SYNTHETIC_TRACE_EVENT_SENDER mutex should not be poisoned")
        .is_none()
    {
        return 0;
    }

    let id = USER_SPAN_SEQ.fetch_add(1, Ordering::Relaxed);

    let fields = fields.into_iter().collect::<TraceFields>();

    let _ = emit_trace_event(TraceEvent::NewSpan {
        id,
        name,
        target,
        level: tracing::Level::INFO,
        fields,
        timestamp: SystemTime::now(),
        parent_id: None,
        thread_name: SYNTHETIC_USER_SPAN_TRACK_NAME,
        file: None,
        line: None,
    });

    let _ = emit_trace_event(TraceEvent::SpanEnter {
        id,
        timestamp: SystemTime::now(),
        thread_name: SYNTHETIC_USER_SPAN_TRACK_NAME,
    });

    id
}

/// Ends a user-defined span previously started with [`start_user_span`].
pub fn end_user_span(id: u64) {
    if id == 0 {
        return;
    }

    let _ = emit_trace_event(TraceEvent::SpanExit {
        id,
        timestamp: SystemTime::now(),
        thread_name: SYNTHETIC_USER_SPAN_TRACK_NAME,
    });

    let _ = emit_trace_event(TraceEvent::SpanClose {
        id,
        timestamp: SystemTime::now(),
    });
}

/// Event data for actor creation.
#[derive(Debug, Clone)]
pub struct ActorEvent {
    /// Unique identifier for this actor, hashed from ActorId.
    pub id: u64,
    /// Timestamp when the actor was created
    pub timestamp: SystemTime,
    /// ID of the mesh this actor belongs to, matching `MeshEvent.id`.
    pub mesh_id: u64,
    /// Rank index into the mesh shape
    pub rank: u64,
    /// Full hierarchical name of this actor
    pub full_name: String,
    /// User-facing name for this actor
    pub display_name: Option<String>,
}

/// Notify telemetry that an actor was created.
pub fn notify_actor_created(event: ActorEvent) {
    emit_entity_event(EntityEvent::Actor(event));
}

/// Event data for mesh creation.
#[derive(Debug, Clone)]
pub struct MeshEvent {
    /// Unique identifier for this mesh (hashed)
    pub id: u64,
    /// Timestamp when the mesh was created
    pub timestamp: SystemTime,
    /// Mesh class (e.g., "Proc", "Host", "Python<SomeUserDefinedActor>")
    pub class: String,
    /// User-provided name for this mesh
    pub given_name: String,
    /// Full hierarchical name as it appears in supervision events
    pub full_name: String,
    /// Shape of the mesh, serialized from ndslice::Extent
    pub shape_json: String,
    /// Parent mesh ID (None for root meshes)
    pub parent_mesh_id: Option<u64>,
    /// Region over which the parent spawned this mesh, serialized from ndslice::Region
    pub parent_view_json: Option<String>,
}

/// Notify telemetry that a mesh was created.
pub fn notify_mesh_created(event: MeshEvent) {
    emit_entity_event(EntityEvent::Mesh(event));
}

/// Event data for actor status changes.
#[derive(Debug, Clone)]
pub struct ActorStatusEvent {
    /// Unique identifier for this event
    pub id: u64,
    /// Timestamp when the status change occurred
    pub timestamp: SystemTime,
    /// ID of the actor whose status changed
    pub actor_id: u64,
    /// New status value (e.g. "Created", "Idle", "Failed")
    pub new_status: String,
    /// Reason for the status change (e.g. error details for Failed)
    pub reason: Option<String>,
}

/// Notify telemetry that an actor changed status.
pub fn notify_actor_status_changed(event: ActorStatusEvent) {
    emit_entity_event(EntityEvent::ActorStatus(event));
}

/// Event fired when a message is sent to an actor mesh.
///
/// Emitted from `cast_all_or_choose` in `actor_mesh.rs`, which is the common
/// path for all Python send methods: `call`, `call_one`, `broadcast`, and `choose`.
#[derive(Debug, Clone)]
pub struct SentMessageEvent {
    pub timestamp: SystemTime,
    /// Hash of the sending actor's ActorId.
    pub sender_actor_id: u64,
    /// Hash of the target actor mesh's `(ProcMeshId, ActorMeshId)`.
    pub actor_mesh_id: u64,
    /// The view (slice) of the actor mesh that was targeted, serialized from
    /// [`ndslice::Region`]. For full-mesh sends (call, broadcast) this covers
    /// all dimensions; for sliced sends (call_one) collapsed dimensions are
    /// absent; for choose this is a scalar (0-dim) Region.
    pub view_json: String,
    /// The shape of the view, serialized from [`ndslice::Shape`] (converted
    /// from the view Region via `Region::into::<Shape>`).
    pub shape_json: String,
}

/// Notify telemetry that a message was sent.
pub fn notify_sent_message(event: SentMessageEvent) {
    emit_entity_event(EntityEvent::SentMessage(event));
}

/// Event fired when a message is received (from receiver's perspective).
#[derive(Debug, Clone)]
pub struct MessageEvent {
    pub timestamp: SystemTime,
    /// Unique identifier for this received message.
    pub id: u64,
    /// Hash of sender's ActorId.
    pub from_actor_id: u64,
    /// Hash of receiver's ActorId.
    pub to_actor_id: u64,
    /// Endpoint name if this message targets a specific actor endpoint
    pub endpoint: Option<String>,
    /// Destination port index, scoped by `to_actor_id`.
    pub port_index: Option<u64>,
}

/// Notify telemetry that a message was received.
pub fn notify_message(event: MessageEvent) {
    emit_entity_event(EntityEvent::Message(event));
}

/// Event fired when a received message changes status.
#[derive(Debug, Clone)]
pub struct MessageStatusEvent {
    pub timestamp: SystemTime,
    /// Unique identifier for this status event.
    pub id: u64,
    /// The message whose status changed (FK to MessageEvent.id).
    pub message_id: u64,
    /// New status: "queued", "active", or "complete".
    pub status: String,
}

/// Notify telemetry that a message changed status.
pub fn notify_message_status(event: MessageStatusEvent) {
    emit_entity_event(EntityEvent::MessageStatus(event));
}

static ACTOR_STATUS_SEQ: AtomicU64 = AtomicU64::new(1);

/// Generate a globally unique ActorStatusEvent ID.
///
/// Combines the actor's unique ID with a process-local sequence number,
/// then hashes the pair to produce an ID that is unique across processes.
pub fn generate_actor_status_event_id(actor_id: u64) -> u64 {
    let seq = ACTOR_STATUS_SEQ.fetch_add(1, Ordering::Relaxed);
    hash_to_u64(&(actor_id, seq))
}

static SEND_SEQ: AtomicU64 = AtomicU64::new(1);

/// Generate a globally unique SentMessage ID.
pub fn generate_sent_message_id(sender_actor_id: u64) -> u64 {
    let seq = SEND_SEQ.fetch_add(1, Ordering::Relaxed);
    hash_to_u64(&(sender_actor_id, seq))
}

static RECV_MSG_SEQ: AtomicU64 = AtomicU64::new(1);

/// Generate a unique received-message ID (cross-process unique).
///
/// Hashes (to_actor_id, seq) following the same pattern as
/// `generate_sent_message_id`.
pub fn generate_message_id(to_actor_id: u64) -> u64 {
    let seq = RECV_MSG_SEQ.fetch_add(1, Ordering::Relaxed);
    hash_to_u64(&(to_actor_id, seq))
}

static STATUS_EVENT_SEQ: AtomicU64 = AtomicU64::new(1);

/// Generate a unique message-status-event ID (cross-process unique).
///
/// Hashes (message_id, seq) following the same pattern as
/// `generate_sent_message_id`.
pub fn generate_status_event_id(message_id: u64) -> u64 {
    let seq = STATUS_EVENT_SEQ.fetch_add(1, Ordering::Relaxed);
    hash_to_u64(&(message_id, seq))
}

/// Unified event enum for all entity lifecycle events.
///
/// This enum wraps all entity events (actors, meshes, and future event types)
/// into a single type. This enables a single sink to handle all entity events,
/// simplifying the registration and notification infrastructure.
#[derive(Debug, Clone)]
pub enum EntityEvent {
    /// An actor was created.
    Actor(ActorEvent),
    /// A mesh was created.
    Mesh(MeshEvent),
    /// An actor changed status.
    ActorStatus(ActorStatusEvent),
    /// A message was sent.
    SentMessage(SentMessageEvent),
    /// A message was received.
    Message(MessageEvent),
    /// A received message changed status.
    MessageStatus(MessageStatusEvent),
}

/// Emit an entity event through the unified trace dispatcher queue.
fn emit_entity_event(event: EntityEvent) {
    if let Some(sender) = synthetic_trace_event_sender() {
        let _ = sender.try_send(TraceEvent::Entity(event));
    }
}

fn synthetic_trace_event_sender() -> Option<mpsc::SyncSender<TraceEvent>> {
    SYNTHETIC_TRACE_EVENT_SENDER
        .lock()
        .expect("SYNTHETIC_TRACE_EVENT_SENDER mutex should not be poisoned")
        .clone()
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

impl Drop for TimerGuard<'_> {
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
    let should_install_subscriber = !tracing::dispatcher::has_been_set();

    swap_telemetry_clock(clock);
    if !should_install_subscriber {
        tracing::debug!("logging already initialized for this process");
    }
    let file_log_level = match env::Env::current() {
        env::Env::Local => LOG_LEVEL_INFO,
        env::Env::MastEmulator => LOG_LEVEL_INFO,
        env::Env::Mast => LOG_LEVEL_INFO,
        env::Env::Test => LOG_LEVEL_DEBUG,
    };

    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[cfg(all(fbcode_build, target_os = "linux"))]
    {
        if should_install_subscriber {
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

            if hyperactor_config::global::get(ENABLE_OTEL_TRACING) {
                use crate::meta;

                sinks.push(Box::new(
                    meta::scuba_sink::ScubaSink::new(meta::tracing_resource())
                        .with_target_filter(crate::config::get_tracing_targets()),
                ));
            }

            sinks.push(unix_sink::install_unix_socket_sink_inactive());

            let dispatcher = trace_dispatcher::TraceEventDispatcher::new(sinks);
            let synthetic_sender = dispatcher.sender();

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
            } else {
                set_synthetic_trace_event_sender(synthetic_sender);
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
    }
    #[cfg(not(all(fbcode_build, target_os = "linux")))]
    {
        let registry =
            Registry::default().with(if hyperactor_config::global::get(ENABLE_RECORDER_TRACING) {
                Some(recorder().layer())
            } else {
                None
            });

        if should_install_subscriber {
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

            if let Some(log_sink) = otlp::otlp_log_sink() {
                sinks.push(log_sink);
            }

            sinks.push(unix_sink::install_unix_socket_sink_inactive());

            let dispatcher = trace_dispatcher::TraceEventDispatcher::new(sinks);
            let synthetic_sender = dispatcher.sender();

            if let Err(err) = registry.with(dispatcher).try_init() {
                tracing::debug!("logging already initialized for this process: {}", err);
            } else {
                set_synthetic_trace_event_sender(synthetic_sender);
            }
        }

        otel::init_metrics();
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
            let random_number: u16 = (rand::random::<u32>() % 1000) as u16;
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
