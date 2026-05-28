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

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::SystemTime;

use smallvec::SmallVec;
use tracing::Id;
use tracing::Subscriber;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::EntityEvent;

const QUEUE_CAPACITY: usize = 100_000;

/// Type alias for trace event fields
/// We expect that most trace events have fewer than 4 fields.
pub(crate) type TraceFields = SmallVec<[(&'static str, FieldValue); 4]>;

#[inline]
pub(crate) fn get_field<'a>(fields: &'a TraceFields, key: &str) -> Option<&'a FieldValue> {
    fields.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
}

/// Unified representation of a trace event captured from the tracing layer.
/// This is captured once on the application thread, then sent to the background
/// worker for fan-out to multiple exporters.
#[derive(Debug, Clone)]
pub enum TraceEvent {
    /// A new span was created (on_new_span)
    NewSpan {
        id: u64,
        name: &'static str,
        target: &'static str,
        level: tracing::Level,
        fields: TraceFields,
        timestamp: SystemTime,
        parent_id: Option<u64>,
        thread_name: &'static str,
        file: Option<&'static str>,
        line: Option<u32>,
    },
    /// A span was entered (on_enter)
    SpanEnter {
        id: u64,
        timestamp: SystemTime,
        thread_name: &'static str,
    },
    /// A span was exited (on_exit)
    SpanExit {
        id: u64,
        timestamp: SystemTime,
        thread_name: &'static str,
    },
    /// A span was closed (dropped)
    SpanClose { id: u64, timestamp: SystemTime },
    /// A tracing event occurred (e.g., tracing::info!())
    Event {
        name: &'static str,
        target: &'static str,
        level: tracing::Level,
        fields: TraceFields,
        timestamp: SystemTime,
        parent_span: Option<u64>,
        thread_id: &'static str,
        thread_name: &'static str,
        module_path: Option<&'static str>,
        file: Option<&'static str>,
        line: Option<u32>,
    },
    /// An entity lifecycle event emitted outside the tracing subscriber.
    Entity(EntityEvent),
}

/// Simplified field value representation for trace events
#[derive(Debug, Clone)]
pub enum FieldValue {
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
pub trait TraceEventSink: Send + 'static {
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

thread_local! {
    /// Cached thread info (thread_name, thread_id) for minimal overhead.
    /// Strings are leaked once per thread to get &'static str - threads are long-lived so this is fine.
    /// Uses Cell since (&'static str, &'static str) is Copy.
    static CACHED_THREAD_INFO: Cell<Option<(&'static str, &'static str)>> = const { Cell::new(None) };
}

#[inline(always)]
fn get_thread_info() -> (&'static str, &'static str) {
    CACHED_THREAD_INFO.with(|cache| {
        if let Some(info) = cache.get() {
            return info;
        }

        let thread_name: &'static str = Box::leak(
            std::thread::current()
                .name()
                .unwrap_or("")
                .to_string()
                .into_boxed_str(),
        );

        #[cfg(target_os = "linux")]
        let thread_id: &'static str = {
            // SAFETY: syscall(SYS_gettid) is always safe to call - it's a read-only
            // syscall that returns the current thread's kernel thread ID (TID).
            // The cast to u64 is safe because gettid() returns a positive pid_t.
            let tid = unsafe { libc::syscall(libc::SYS_gettid) as u64 };
            Box::leak(tid.to_string().into_boxed_str())
        };
        #[cfg(not(target_os = "linux"))]
        let thread_id: &'static str = {
            let tid_num = std::thread::current().id().as_u64().get();
            Box::leak(tid_num.to_string().into_boxed_str())
        };

        cache.set(Some((thread_name, thread_id)));
        (thread_name, thread_id)
    })
}

/// Control messages for the dispatcher (e.g., adding sinks dynamically)
pub enum DispatcherControl {
    /// Add a new sink to receive events
    AddSink(Box<dyn TraceEventSink>),
}

/// The trace event dispatcher that captures events once and dispatches to multiple sinks
/// on a background thread.
pub struct TraceEventDispatcher {
    sender: Option<mpsc::SyncSender<TraceEvent>>,
    /// Separate channel so we are always notified of when the main queue is full and events are being dropped.
    dropped_sender: Option<mpsc::Sender<TraceEvent>>,
    _worker_handle: WorkerHandle,
    max_level: Option<LevelFilter>,
    dropped_events: Arc<AtomicU64>,
}

struct WorkerHandle {
    join_handle: Option<JoinHandle<()>>,
}

thread_local! {
    static IN_SEND: Cell<bool> = const { Cell::new(false) };
}

struct InSendGuard;

impl Drop for InSendGuard {
    fn drop(&mut self) {
        IN_SEND.with(|f| f.set(false));
    }
}

impl TraceEventDispatcher {
    /// Create a new trace event dispatcher with the given sinks.
    /// Uses a bounded channel (capacity QUEUE_CAPACITY) to ensure telemetry never blocks
    /// the application. Events are dropped with a warning if the queue is full.
    /// A separate unbounded priority channel guarantees delivery of critical events
    /// like drop notifications (safe because drop events are rate-limited).
    ///
    /// Takes the global control receiver for dynamic sink registration. Sinks registered
    /// via `register_sink()` before or after this call will be added to the dispatcher.
    ///
    /// # Arguments
    /// * `sinks` - List of sinks to dispatch events to.
    pub(crate) fn new(sinks: Vec<Box<dyn TraceEventSink>>) -> Self {
        let max_level = Self::derive_max_level(&sinks);

        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let (dropped_sender, dropped_receiver) = mpsc::channel();
        // Take the global control receiver - sinks registered via register_sink() will be received here
        let control_receiver = crate::take_sink_control_receiver();
        let dropped_events = Arc::new(AtomicU64::new(0));
        let dropped_events_worker = Arc::clone(&dropped_events);

        let worker_handle = std::thread::Builder::new()
            .name("telemetry-worker".into())
            .spawn(move || {
                worker_loop(
                    receiver,
                    dropped_receiver,
                    control_receiver,
                    sinks,
                    dropped_events_worker,
                );
            })
            .expect("failed to spawn telemetry worker thread");

        Self {
            sender: Some(sender),
            dropped_sender: Some(dropped_sender),
            _worker_handle: WorkerHandle {
                join_handle: Some(worker_handle),
            },
            max_level,
            dropped_events,
        }
    }

    fn derive_max_level(sinks: &[Box<dyn TraceEventSink>]) -> Option<LevelFilter> {
        let mut max_level: Option<LevelFilter> = None;

        for sink in sinks {
            let sink_max = match sink.target_filter() {
                None => LevelFilter::TRACE,
                Some(targets) => {
                    let levels = [
                        (tracing::Level::TRACE, LevelFilter::TRACE),
                        (tracing::Level::DEBUG, LevelFilter::DEBUG),
                        (tracing::Level::INFO, LevelFilter::INFO),
                        (tracing::Level::WARN, LevelFilter::WARN),
                        (tracing::Level::ERROR, LevelFilter::ERROR),
                    ];
                    let mut result = LevelFilter::OFF;
                    for (level, filter) in levels {
                        if targets.would_enable("", &level) {
                            result = filter;
                            break;
                        }
                    }
                    result
                }
            };

            max_level = Some(match max_level {
                None => sink_max,
                Some(current) => std::cmp::max(current, sink_max),
            });
        }

        max_level
    }

    fn send_event(&self, event: TraceEvent) {
        // Re-entrancy guard. A `Layer` callback may emit `tracing` events
        // through code it touches—notably std's mpmc channel, which is itself
        // instrumented—and those events loop back through this subscriber.
        // Without this guard, the recursion exhausts the stack and SIGSEGVs.
        if IN_SEND.with(|f| f.replace(true)) {
            return;
        }
        let _reset = InSendGuard;

        if let Some(sender) = &self.sender
            && let Err(mpsc::TrySendError::Full(_)) = sender.try_send(event)
        {
            let dropped = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

            if dropped == 1 || dropped.is_multiple_of(1000) {
                eprintln!(
                    "[telemetry]: {}  events and log lines dropped que to full queue (capacity: {})",
                    dropped, QUEUE_CAPACITY
                );
                self.send_drop_event(dropped);
            }
        }
    }

    pub(crate) fn sender(&self) -> mpsc::SyncSender<TraceEvent> {
        self.sender
            .as_ref()
            .expect("trace event dispatcher sender should exist during initialization")
            .clone()
    }

    fn send_drop_event(&self, total_dropped: u64) {
        if let Some(dropped_sender) = &self.dropped_sender {
            let (thread_name, thread_id) = get_thread_info();

            let mut fields = TraceFields::new();
            fields.push((
                "message",
                FieldValue::Str(format!(
                    "Telemetry events and log lines dropped due to full queue (capacity: {}). Worker may be falling behind.",
                    QUEUE_CAPACITY
                )),
            ));
            fields.push(("dropped_count", FieldValue::U64(total_dropped)));

            // We want to just directly construct and send a `TraceEvent::Event` here so we don't need to
            // reason very hard about whether or not we are creating a DoS loop
            let drop_event = TraceEvent::Event {
                name: "dropped events",
                target: module_path!(),
                level: tracing::Level::ERROR,
                fields,
                timestamp: SystemTime::now(),
                parent_span: None,
                thread_id,
                thread_name,
                module_path: Some(module_path!()),
                file: Some(file!()),
                line: Some(line!()),
            };

            if dropped_sender.send(drop_event).is_err() {
                // Last resort
                eprintln!(
                    "[telemetry] CRITICAL: {} events and log lines dropped and unable to log to telemetry \
                     (worker thread may have died). Telemetry system offline.",
                    total_dropped
                );
            }
        }
    }
}

impl Drop for TraceEventDispatcher {
    fn drop(&mut self) {
        // Explicitly drop both senders to close the channels.
        // The next field to be dropped is `worker_handle` which
        // will run its own drop impl to join the thread and flush
        drop(self.sender.take());
        drop(self.dropped_sender.take());
    }
}

impl<S> Layer<S> for TraceEventDispatcher
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();
        let mut fields = TraceFields::new();

        let mut visitor = FieldVisitor(&mut fields);
        attrs.record(&mut visitor);

        let parent_id = if let Some(parent) = attrs.parent() {
            Some(parent.into_u64())
        } else {
            ctx.current_span().id().map(|id| id.into_u64())
        };

        let (thread_name, _) = get_thread_info();

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
        let (thread_name, _) = get_thread_info();
        let event = TraceEvent::SpanEnter {
            id: id.into_u64(),
            timestamp: SystemTime::now(),
            thread_name,
        };

        self.send_event(event);
    }

    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        let (thread_name, _) = get_thread_info();
        let event = TraceEvent::SpanExit {
            id: id.into_u64(),
            timestamp: SystemTime::now(),
            thread_name,
        };

        self.send_event(event);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut fields = TraceFields::new();
        let mut visitor = FieldVisitor(&mut fields);
        event.record(&mut visitor);

        let parent_span = ctx.event_span(event).map(|span| span.id().into_u64());

        let (thread_name, thread_id) = get_thread_info();

        let trace_event = TraceEvent::Event {
            name: metadata.name(),
            target: metadata.target(),
            level: *metadata.level(),
            fields,
            timestamp: SystemTime::now(),
            parent_span,
            thread_id,
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

    fn max_level_hint(&self) -> Option<LevelFilter> {
        self.max_level
    }
}

struct FieldVisitor<'a>(&'a mut TraceFields);

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.push((field.name(), FieldValue::Bool(value)));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.push((field.name(), FieldValue::I64(value)));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.push((field.name(), FieldValue::U64(value)));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.push((field.name(), FieldValue::F64(value)));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0
            .push((field.name(), FieldValue::Str(value.to_string())));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name(), FieldValue::Debug(format!("{:?}", value))));
    }
}

/// Background worker loop that receives events from both regular and priority channels,
/// and dispatches them to sinks. Priority events are processed first.
/// Runs until both senders are dropped.
fn worker_loop(
    receiver: mpsc::Receiver<TraceEvent>,
    dropped_receiver: mpsc::Receiver<TraceEvent>,
    control_receiver: Option<mpsc::Receiver<DispatcherControl>>,
    mut sinks: Vec<Box<dyn TraceEventSink>>,
    dropped_events: Arc<AtomicU64>,
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

    fn process_control_messages(
        control_receiver: Option<&mpsc::Receiver<DispatcherControl>>,
        sinks: &mut Vec<Box<dyn TraceEventSink>>,
    ) {
        if let Some(ctrl_rx) = control_receiver {
            while let Ok(control) = ctrl_rx.try_recv() {
                match control {
                    DispatcherControl::AddSink(sink) => {
                        sinks.push(sink);
                    }
                }
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
                // Target filters are tracing/log filters. Variants without
                // target/level metadata, including semantic entity table rows,
                // must reach sinks so each sink can decide whether to consume
                // or ignore them.
                _ => true,
            } && let Err(e) = sink.consume(&event)
            {
                eprintln!(
                    "[telemetry] sink {} failed to consume event: {}",
                    sink.name(),
                    e
                );
            }
        }
    }

    loop {
        while let Ok(event) = dropped_receiver.try_recv() {
            dispatch_to_sinks(&mut sinks, event);
            events_since_flush += 1;
        }

        match receiver.recv_timeout(FLUSH_INTERVAL) {
            Ok(event) => {
                // A control message may have arrived while we were blocked in
                // `recv_timeout`. Drain it before dispatching the event that
                // woke us so dynamically registered sinks see subsequent
                // replayed events.
                process_control_messages(control_receiver.as_ref(), &mut sinks);
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
                flush_sinks(&mut sinks);
                last_flush = std::time::Instant::now();
                events_since_flush = 0;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    // The event queues are closing, but the control queue is independent.
    // Apply any sink registrations already queued before draining telemetry
    // events so shutdown delivery follows the same ordering as the live loop.
    process_control_messages(control_receiver.as_ref(), &mut sinks);

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

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take()
            && let Err(e) = handle.join()
        {
            eprintln!("[telemetry] worker thread panicked: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::os::unix::net::UnixListener;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;

    use super::*;

    static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    #[derive(Default)]
    struct RecordingSink {
        events: Arc<Mutex<Vec<TraceEvent>>>,
    }

    impl TraceEventSink for RecordingSink {
        fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        fn flush(&mut self) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    struct CountingSink {
        entity_events: Arc<AtomicU64>,
        target_filter: Option<Targets>,
    }

    impl TraceEventSink for CountingSink {
        fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
            if matches!(event, TraceEvent::Entity(_)) {
                self.entity_events.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }

        fn target_filter(&self) -> Option<&Targets> {
            self.target_filter.as_ref()
        }

        fn flush(&mut self) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    fn span_close(id: u64) -> TraceEvent {
        TraceEvent::SpanClose {
            id,
            timestamp: SystemTime::now(),
        }
    }

    fn event() -> TraceEvent {
        TraceEvent::Event {
            name: "test_event",
            target: "test",
            level: tracing::Level::INFO,
            fields: TraceFields::new(),
            timestamp: SystemTime::now(),
            parent_span: None,
            thread_id: "1",
            thread_name: "test",
            module_path: Some("test"),
            file: Some("test.rs"),
            line: Some(1),
        }
    }

    fn entity_event() -> TraceEvent {
        TraceEvent::Entity(EntityEvent::Mesh(crate::MeshEvent {
            id: 11,
            timestamp: SystemTime::now(),
            class: "Host".to_string(),
            given_name: "test_mesh".to_string(),
            full_name: "test_mesh".to_string(),
            shape_json: "{}".to_string(),
            parent_mesh_id: None,
            parent_view_json: None,
        }))
    }

    fn socket_path(name: &str) -> std::path::PathBuf {
        let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "monarch_trace_dispatcher_{}_{}",
            std::process::id(),
            seq
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn read_frame_table(listener: UnixListener) -> String {
        let (mut stream, _addr) = listener.accept().unwrap();
        let mut name_len_bytes = [0; 2];
        stream.read_exact(&mut name_len_bytes).unwrap();
        let name_len = u16::from_be_bytes(name_len_bytes) as usize;
        let mut name_bytes = vec![0; name_len];
        stream.read_exact(&mut name_bytes).unwrap();
        String::from_utf8(name_bytes).unwrap()
    }

    #[test]
    fn send_event_delivers_repeatedly() {
        let sink = RecordingSink::default();
        let recorded = Arc::clone(&sink.events);
        let dispatcher = TraceEventDispatcher::new(vec![Box::new(sink)]);

        dispatcher.send_event(span_close(1));
        dispatcher.send_event(span_close(2));
        dispatcher.send_event(span_close(3));

        drop(dispatcher);
        assert_eq!(recorded.lock().unwrap().len(), 3);
    }

    #[test]
    fn send_event_drops_on_reentrance() {
        let sink = RecordingSink::default();
        let recorded = Arc::clone(&sink.events);
        let dispatcher = TraceEventDispatcher::new(vec![Box::new(sink)]);

        // Simulate that this thread is already inside `send_event`. The nested
        // call must short-circuit; otherwise a `Layer` callback that re-enters
        // the subscriber would recurse without bound.
        IN_SEND.with(|f| f.set(true));
        dispatcher.send_event(span_close(1));
        IN_SEND.with(|f| f.set(false));

        drop(dispatcher);
        assert!(recorded.lock().unwrap().is_empty());
    }

    #[test]
    fn entity_event_bypasses_target_filter_and_reaches_sink() {
        let entity_events = Arc::new(AtomicU64::new(0));
        let sink = CountingSink {
            entity_events: Arc::clone(&entity_events),
            target_filter: Some(Targets::new().with_default(LevelFilter::OFF)),
        };
        let dispatcher = TraceEventDispatcher::new(vec![Box::new(sink)]);

        dispatcher.send_event(entity_event());

        drop(dispatcher);
        assert_eq!(entity_events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn shutdown_drain_processes_pending_sink_registration() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (dropped_sender, dropped_receiver) = mpsc::channel();
        let (control_sender, control_receiver) = mpsc::channel();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let dropped_events = Arc::new(AtomicU64::new(0));

        let worker = std::thread::spawn(move || {
            worker_loop(
                receiver,
                dropped_receiver,
                Some(control_receiver),
                Vec::new(),
                dropped_events,
            );
        });

        // Let the worker pass its top-of-loop dropped-event drain and block on
        // the main event receiver. The queued dropped event below then reaches
        // the shutdown drain path, where pending sink registrations must be
        // applied first.
        std::thread::sleep(Duration::from_millis(200));

        control_sender
            .send(DispatcherControl::AddSink(Box::new(RecordingSink {
                events: Arc::clone(&recorded),
            })))
            .unwrap();
        dropped_sender.send(span_close(7)).unwrap();

        drop(sender);
        drop(dropped_sender);
        drop(control_sender);
        worker.join().unwrap();

        assert_eq!(recorded.lock().unwrap().len(), 1);
    }

    #[test]
    fn unix_socket_sink_receives_dispatched_event() {
        let path = socket_path("telemetry.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (sender, receiver) = mpsc::channel();
        let read_handle = std::thread::spawn(move || {
            sender.send(read_frame_table(listener)).unwrap();
        });

        let sink = Arc::new(crate::unix_sink::UnixSocketSink::new());
        sink.set_path(path).unwrap();
        let dispatcher =
            TraceEventDispatcher::new(vec![crate::unix_sink::adapter_for_test(Arc::clone(&sink))]);

        dispatcher.send_event(event());

        drop(dispatcher);
        let table_name = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        read_handle.join().unwrap();
        assert_eq!(table_name, monarch_telemetry_schema::trace_tables::EVENTS);
    }
}
