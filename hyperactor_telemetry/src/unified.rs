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
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::SystemTime;

use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use crossbeam_channel::unbounded;
use indexmap::IndexMap;
use tracing::Id;
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

/// Unified representation of a trace event captured from the tracing layer.
/// This is captured once on the application thread, then sent to the background
/// worker for fan-out to multiple exporters.
#[derive(Debug, Clone)]
pub(crate) enum TraceEvent {
    /// A span was entered
    SpanEnter {
        id: u64,
        name: &'static str,
        target: &'static str,
        level: tracing::Level,
        fields: IndexMap<String, FieldValue>,
        timestamp: SystemTime,
        parent_id: Option<u64>,
        module_path: Option<&'static str>,
        file: Option<&'static str>,
        line: Option<u32>,
    },
    /// A span was entered at runtime (on_enter hook - async_enter in Scuba)
    SpanEnterRuntime { id: u64, timestamp: SystemTime },
    /// A span was exited at runtime (on_exit hook - async_exit in Scuba)
    SpanExitRuntime { id: u64, timestamp: SystemTime },
    /// Fields were recorded on an existing span
    SpanRecord {
        id: u64,
        fields: IndexMap<String, FieldValue>,
    },
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

/// Trait for exporting trace events to different backends.
/// Implementations run on the background worker thread and can perform
/// expensive I/O operations without blocking the application.
pub(crate) trait TraceExporter: Send + 'static {
    /// Export a single event. Called on background thread.
    fn export(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error>;

    /// Flush any buffered events to the backend.
    /// Called periodically and on shutdown.
    fn flush(&mut self) -> Result<(), anyhow::Error>;

    /// Optional: return name for debugging/logging
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Optional: check if this exporter should export the given event.
    /// This is called before `export()` to allow exporters to filter events
    /// based on target, level, or other criteria.
    ///
    /// Default implementation exports everything.
    fn should_export(&self, _event: &TraceEvent) -> bool {
        true
    }
}

/// The unified layer that captures events once and sends to background thread.
pub struct UnifiedLayer {
    sender: Sender<TraceEvent>,
    _worker_handle: Arc<WorkerHandle>,
    max_level: Option<tracing::level_filters::LevelFilter>,
}

struct WorkerHandle {
    join_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl UnifiedLayer {
    /// Create a new unified layer with the given exporters.
    /// Uses an unbounded channel to ensure no events are ever dropped.
    ///
    /// # Arguments
    /// * `exporters` - List of exporters to fan out events to.
    /// * `max_level` - Maximum level filter hint (None for no filtering)
    pub(crate) fn new(
        exporters: Vec<Box<dyn TraceExporter>>,
        max_level: Option<tracing::level_filters::LevelFilter>,
    ) -> Self {
        let (sender, receiver) = unbounded();
        let shutdown = Arc::new(AtomicBool::new(false));

        let worker_handle = {
            let shutdown = Arc::clone(&shutdown);
            std::thread::Builder::new()
                .name("telemetry-worker".into())
                .spawn(move || {
                    worker_loop(receiver, exporters, shutdown);
                })
                .expect("failed to spawn telemetry worker thread")
        };

        Self {
            sender,
            _worker_handle: Arc::new(WorkerHandle {
                join_handle: Some(worker_handle),
                shutdown,
            }),
            max_level,
        }
    }

    fn send_event(&self, event: TraceEvent) {
        let _ = self.sender.send(event);
    }
}

impl<S> Layer<S> for UnifiedLayer
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

        let event = TraceEvent::SpanEnter {
            id: id.into_u64(),
            name: metadata.name(),
            target: metadata.target(),
            level: *metadata.level(),
            fields,
            timestamp: SystemTime::now(),
            parent_id,
            module_path: metadata.module_path(),
            file: metadata.file(),
            line: metadata.line(),
        };

        self.send_event(event);
    }

    fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, _ctx: Context<'_, S>) {
        let mut fields = IndexMap::new();
        let mut visitor = FieldVisitor(&mut fields);
        values.record(&mut visitor);

        let event = TraceEvent::SpanRecord {
            id: id.into_u64(),
            fields,
        };

        self.send_event(event);
    }

    fn on_enter(&self, id: &Id, _ctx: Context<'_, S>) {
        let event = TraceEvent::SpanEnterRuntime {
            id: id.into_u64(),
            timestamp: SystemTime::now(),
        };

        self.send_event(event);
    }

    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        let event = TraceEvent::SpanExitRuntime {
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

        let trace_event = TraceEvent::Event {
            name: metadata.name(),
            target: metadata.target(),
            level: *metadata.level(),
            fields,
            timestamp: SystemTime::now(),
            parent_span,
            thread_id: thread_id_str,
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

/// Background worker loop that receives events and fans them out to exporters.
/// Runs until the sender is dropped or shutdown is signaled.
fn worker_loop(
    receiver: Receiver<TraceEvent>,
    mut exporters: Vec<Box<dyn TraceExporter>>,
    shutdown: Arc<AtomicBool>,
) {
    const FLUSH_INTERVAL: Duration = Duration::from_millis(100);
    let mut last_flush = std::time::Instant::now();

    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => {
                for exporter in &mut exporters {
                    if exporter.should_export(&event) {
                        if let Err(e) = exporter.export(&event) {
                            tracing::warn!(
                                exporter = exporter.name(),
                                error = %e,
                                "exporter failed to export event"
                            );
                        }
                    }
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                break;
            }
        }

        if last_flush.elapsed() >= FLUSH_INTERVAL {
            for exporter in &mut exporters {
                if let Err(e) = exporter.flush() {
                    tracing::warn!(
                        exporter = exporter.name(),
                        error = %e,
                        "exporter failed to flush"
                    );
                }
            }
            last_flush = std::time::Instant::now();
        }
    }

    while let Ok(event) = receiver.try_recv() {
        for exporter in &mut exporters {
            if exporter.should_export(&event) {
                let _ = exporter.export(&event);
            }
        }
    }

    for exporter in &mut exporters {
        if let Err(e) = exporter.flush() {
            tracing::warn!(
                exporter = exporter.name(),
                error = %e,
                "exporter failed final flush"
            );
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.join_handle.take() {
            if let Err(e) = handle.join() {
                tracing::warn!("telemetry worker thread panicked: {:?}", e);
            }
        }
    }
}
