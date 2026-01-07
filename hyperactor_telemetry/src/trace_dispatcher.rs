/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Unified telemetry layer that captures trace events once and fans out to multiple exporters
//! on a background thread, eliminating redundant capture and moving work off the application
//! thread.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::SystemTime;

use indexmap::IndexMap;
use tracing::Id;
use tracing::Subscriber;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::register_shutdown_hook;

const QUEUE_CAPACITY: usize = 100_000;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Unified representation of a trace event captured from the tracing layer.
/// This is captured once on the application thread, then sent to the background
/// worker for fan-out to multiple exporters.
#[derive(Debug, Clone)]
pub(crate) enum TraceEvent {
    /// A new span was created (on_new_span)
    NewSpan {
        id: u64,
        name: &'static str,
        target: &'static str,
        level: tracing::Level,
        fields: IndexMap<String, FieldValue>,
        timestamp: SystemTime,
        parent_id: Option<u64>,
        thread_name: String,
        file: Option<&'static str>,
        line: Option<u32>,
    },
    /// A span was entered (on_enter)
    SpanEnter { id: u64, timestamp: SystemTime },
    /// A span was exited (on_exit)
    SpanExit { id: u64, timestamp: SystemTime },
    /// A span was closed (dropped)
    SpanClose { id: u64, timestamp: SystemTime },
    /// A tracing event occurred (e.g., tracing::info!())
    Event {
        name: &'static str,
        target: &'static str,
        level: tracing::Level,
        fields: IndexMap<String, FieldValue>,
        timestamp: SystemTime,
        parent_span: Option<u64>,
        thread_id: String,
        thread_name: String,
        module_path: Option<&'static str>,
        file: Option<&'static str>,
        line: Option<u32>,
    },
}

/// Simplified field value representation for trace events
#[derive(Debug, Clone)]
pub(crate) enum FieldValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
    Debug(String),
}

/// Trait for sinks that receive trace events from the dispatcher.
/// Implementations run on the background worker thread and can perform
/// expensive I/O operations without blocking the application.
pub(crate) trait TraceEventSink: Send + 'static {
    /// Consume a single event. Called on background thread.
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error>;

    /// Optional target/level filter for this sink.
    ///
    /// The worker loop automatically applies this filter before calling `consume()`,
    /// so sinks don't need to check target/level in their consume implementation.
    /// Only `NewSpan` and `Event` are filtered by target/level; other event types
    /// are always passed through.
    ///
    /// # Returns
    /// - `None` - No filtering, all events are consumed (default)
    /// - `Some(Targets)` - Only consume events matching the target/level filter
    ///
    /// # Example
    /// ```ignore
    /// fn target_filter(&self) -> Option<&Targets> {
    ///     Some(Targets::new()
    ///         .with_target("opentelemetry", LevelFilter::OFF)
    ///         .with_default(LevelFilter::DEBUG))
    /// }
    /// ```
    fn target_filter(&self) -> Option<&Targets> {
        None
    }

    /// Flush any buffered events to the backend.
    /// Called periodically and on shutdown.
    fn flush(&mut self) -> Result<(), anyhow::Error>;

    /// Optional: return name for debugging/logging
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

/// The trace event dispatcher that captures events once and dispatches to multiple sinks
/// on a background thread.
pub struct TraceEventDispatcher {
    sender: mpsc::SyncSender<TraceEvent>,
    /// Separate channel so we are always notified of when the main queue is full and events are being dropped.
    dropped_sender: mpsc::Sender<TraceEvent>,
    max_level: Option<tracing::level_filters::LevelFilter>,
    dropped_events: Arc<AtomicU64>,
}

impl TraceEventDispatcher {
    /// Create a new trace event dispatcher with the given sinks.
    /// Uses a bounded channel (capacity QUEUE_CAPACITY) to ensure telemetry never blocks
    /// the application. Events are dropped with a warning if the queue is full.
    /// A separate unbounded priority channel guarantees delivery of critical events
    /// like drop notifications (safe because drop events are rate-limited).
    ///
    /// # Arguments
    /// * `sinks` - List of sinks to dispatch events to.
    /// * `max_level` - Maximum level filter hint (None for no filtering)
    pub(crate) fn new(
        sinks: Vec<Box<dyn TraceEventSink>>,
        max_level: Option<tracing::level_filters::LevelFilter>,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let (dropped_sender, dropped_receiver) = mpsc::channel();
        let dropped_events = Arc::new(AtomicU64::new(0));
        let dropped_events_worker = Arc::clone(&dropped_events);

        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_worker = Arc::clone(&shutdown_flag);

        let (done_tx, done_rx) = mpsc::sync_channel::<()>(1);

        let worker_handle = std::thread::Builder::new()
            .name("telemetry-worker".into())
            .spawn(move || {
                worker_loop(
                    receiver,
                    dropped_receiver,
                    sinks,
                    dropped_events_worker,
                    shutdown_flag_worker,
                );
                let _ = done_tx.send(());
            })
            .expect("failed to spawn telemetry worker thread");

        register_shutdown_hook(move || {
            shutdown_flag.store(true, Ordering::Release);

            if done_rx.recv_timeout(SHUTDOWN_TIMEOUT).is_err() {
                eprintln!(
                    "[telemetry] WARNING: worker thread did not exit within {:?}, continuing shutdown",
                    SHUTDOWN_TIMEOUT
                );
                return;
            }

            // This join should be instant since done_rx was received
            if let Err(e) = worker_handle.join() {
                eprintln!(
                    "[telemetry] worker thread panicked during shutdown: {:?}",
                    e
                );
            }
        });

        Self {
            sender,
            dropped_sender,
            max_level,
            dropped_events,
        }
    }

    fn send_event(&self, event: TraceEvent) {
        if let Err(mpsc::TrySendError::Full(_)) = self.sender.try_send(event) {
            let dropped = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

            if dropped == 1 || dropped.is_multiple_of(1000) {
                eprintln!(
                    "[telemetry]: {} events and log lines dropped due to full queue (capacity: {})",
                    dropped, QUEUE_CAPACITY
                );
                self.send_drop_event(dropped);
            }
        }
    }

    fn send_drop_event(&self, total_dropped: u64) {
        #[cfg(target_os = "linux")]
        let thread_id_num = {
            // SAFETY: syscall(SYS_gettid) is always safe to call
            unsafe { libc::syscall(libc::SYS_gettid) as u64 }
        };
        #[cfg(not(target_os = "linux"))]
        let thread_id_num = {
            let tid = std::thread::current().id();
            // SAFETY: ThreadId transmute for non-Linux platforms
            unsafe { std::mem::transmute::<std::thread::ThreadId, u64>(tid) }
        };

        let mut fields = IndexMap::new();
        fields.insert(
            "message".to_string(),
            FieldValue::Str(format!(
                "Telemetry events and log lines dropped due to full queue (capacity: {}). Worker may be falling behind.",
                QUEUE_CAPACITY
            )),
        );
        fields.insert("dropped_count".to_string(), FieldValue::U64(total_dropped));

        // We want to just directly construct and send a `TraceEvent::Event` here so we don't need to
        // reason very hard about whether or not we are creating a DoS loop
        let drop_event = TraceEvent::Event {
            name: "dropped events",
            target: module_path!(),
            level: tracing::Level::ERROR,
            fields,
            timestamp: SystemTime::now(),
            parent_span: None,
            thread_id: thread_id_num.to_string(),
            thread_name: std::thread::current()
                .name()
                .unwrap_or_default()
                .to_string(),
            module_path: Some(module_path!()),
            file: Some(file!()),
            line: Some(line!()),
        };

        if self.dropped_sender.send(drop_event).is_err() {
            // Last resort
            eprintln!(
                "[telemetry] CRITICAL: {} events and log lines dropped and unable to log to telemetry \
                 (worker thread may have died). Telemetry system offline.",
                total_dropped
            );
        }
    }
}

impl<S> Layer<S> for TraceEventDispatcher
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();
        let mut fields = IndexMap::new();

        let mut visitor = FieldVisitor(&mut fields);
        attrs.record(&mut visitor);

        let parent_id = if let Some(parent) = attrs.parent() {
            Some(parent.into_u64())
        } else {
            ctx.current_span().id().map(|id| id.into_u64())
        };

        let thread_name = std::thread::current()
            .name()
            .unwrap_or_default()
            .to_string();

        let event = TraceEvent::NewSpan {
            id: id.into_u64(),
            name: metadata.name(),
            target: metadata.target(),
            level: *metadata.level(),
            fields,
            timestamp: SystemTime::now(),
            parent_id,
            thread_name,
            file: metadata.file(),
            line: metadata.line(),
        };

        self.send_event(event);
    }

    fn on_enter(&self, id: &Id, _ctx: Context<'_, S>) {
        let event = TraceEvent::SpanEnter {
            id: id.into_u64(),
            timestamp: SystemTime::now(),
        };

        self.send_event(event);
    }

    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        let event = TraceEvent::SpanExit {
            id: id.into_u64(),
            timestamp: SystemTime::now(),
        };

        self.send_event(event);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut fields = IndexMap::new();
        let mut visitor = FieldVisitor(&mut fields);
        event.record(&mut visitor);

        let parent_span = ctx.event_span(event).map(|span| span.id().into_u64());

        #[cfg(target_os = "linux")]
        let thread_id_num = {
            // SAFETY: syscall(SYS_gettid) is always safe to call - it's a read-only
            // syscall that returns the current thread's kernel thread ID (TID).
            // The cast to u64 is safe because gettid() returns a positive pid_t.
            unsafe { libc::syscall(libc::SYS_gettid) as u64 }
        };
        #[cfg(not(target_os = "linux"))]
        let thread_id_num = {
            let tid = std::thread::current().id();
            // SAFETY: ThreadId is a newtype wrapper around a u64 counter.
            // This transmute relies on the internal representation of ThreadId,
            // which is stable in practice but not guaranteed by Rust's API.
            // On non-Linux platforms this is a best-effort approximation.
            // See: https://doc.rust-lang.org/std/thread/struct.ThreadId.html
            unsafe { std::mem::transmute::<std::thread::ThreadId, u64>(tid) }
        };
        let thread_id_str = thread_id_num.to_string();

        let thread_name = std::thread::current()
            .name()
            .unwrap_or_default()
            .to_string();

        let trace_event = TraceEvent::Event {
            name: metadata.name(),
            target: metadata.target(),
            level: *metadata.level(),
            fields,
            timestamp: SystemTime::now(),
            parent_span,
            thread_id: thread_id_str,
            thread_name,
            module_path: metadata.module_path(),
            file: metadata.file(),
            line: metadata.line(),
        };

        self.send_event(trace_event);
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        let event = TraceEvent::SpanClose {
            id: id.into_u64(),
            timestamp: SystemTime::now(),
        };

        self.send_event(event);
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        self.max_level
    }
}

struct FieldVisitor<'a>(&'a mut IndexMap<String, FieldValue>);

impl<'a> tracing::field::Visit for FieldVisitor<'a> {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0
            .insert(field.name().to_string(), FieldValue::Bool(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0
            .insert(field.name().to_string(), FieldValue::I64(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0
            .insert(field.name().to_string(), FieldValue::U64(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0
            .insert(field.name().to_string(), FieldValue::F64(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0
            .insert(field.name().to_string(), FieldValue::Str(value.to_string()));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            FieldValue::Debug(format!("{:?}", value)),
        );
    }
}

/// Background worker loop that receives events from both regular and priority channels,
/// and dispatches them to sinks. Priority events are processed first.
/// Runs until shutdown_flag is set or senders are dropped.
fn worker_loop(
    receiver: mpsc::Receiver<TraceEvent>,
    dropped_receiver: mpsc::Receiver<TraceEvent>,
    mut sinks: Vec<Box<dyn TraceEventSink>>,
    dropped_events: Arc<AtomicU64>,
    shutdown_flag: Arc<AtomicBool>,
) {
    const FLUSH_INTERVAL: Duration = Duration::from_millis(100);
    const FLUSH_EVENT_COUNT: usize = 1000;
    let mut last_flush = std::time::Instant::now();
    let mut events_since_flush = 0;

    fn flush_sinks(sinks: &mut [Box<dyn TraceEventSink>]) {
        for sink in sinks {
            if let Err(e) = sink.flush() {
                eprintln!("[telemetry] sink {} failed to flush: {}", sink.name(), e);
            }
        }
    }

    fn dispatch_to_sinks(sinks: &mut [Box<dyn TraceEventSink>], event: TraceEvent) {
        for sink in sinks {
            if match &event {
                TraceEvent::NewSpan { target, level, .. }
                | TraceEvent::Event { target, level, .. } => match sink.target_filter() {
                    Some(targets) => targets.would_enable(target, level),
                    None => true,
                },
                _ => true,
            } {
                if let Err(e) = sink.consume(&event) {
                    eprintln!(
                        "[telemetry] sink {} failed to consume event: {}",
                        sink.name(),
                        e
                    );
                }
            }
        }
    }

    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            break;
        }

        while let Ok(event) = dropped_receiver.try_recv() {
            dispatch_to_sinks(&mut sinks, event);
            events_since_flush += 1;
        }

        match receiver.recv_timeout(FLUSH_INTERVAL) {
            Ok(event) => {
                dispatch_to_sinks(&mut sinks, event);
                events_since_flush += 1;

                if events_since_flush >= FLUSH_EVENT_COUNT || last_flush.elapsed() >= FLUSH_INTERVAL
                {
                    flush_sinks(&mut sinks);
                    last_flush = std::time::Instant::now();
                    events_since_flush = 0;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                flush_sinks(&mut sinks);
                last_flush = std::time::Instant::now();
                events_since_flush = 0;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    while let Ok(event) = dropped_receiver.try_recv() {
        dispatch_to_sinks(&mut sinks, event);
    }
    while let Ok(event) = receiver.try_recv() {
        dispatch_to_sinks(&mut sinks, event);
    }

    flush_sinks(&mut sinks);

    let total_dropped = dropped_events.load(Ordering::Relaxed);
    if total_dropped > 0 {
        eprintln!(
            "[telemetry] Telemetry worker shutting down. Total events dropped during session: {}",
            total_dropped
        );
    }
}
