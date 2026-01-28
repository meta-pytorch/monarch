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
//!
//! Uses thread-local SPSC ring buffers for minimal latency and jitter on the hot path.

use std::cell::Cell;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
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

/// Capacity for each thread-local ring buffer.
const THREAD_RING_CAPACITY: usize = 8192;

/// Number of producers to pre-allocate and pre-touch during initialization.
/// initialization can take around a millisecond per producer and we want to avoid doing this
/// on the first application thread that hits some on_ hook
const PREALLOC_PRODUCER_COUNT: usize = 32;

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
pub(crate) enum TraceEvent {
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
        fields: TraceFields,
        timestamp: SystemTime,
        parent_span: Option<u64>,
        thread_id: &'static str,
        thread_name: &'static str,
        module_path: Option<&'static str>,
        file: Option<&'static str>,
        line: Option<u32>,
    },
}

impl TraceEvent {
    fn timestamp(&self) -> SystemTime {
        match self {
            TraceEvent::NewSpan { timestamp, .. } => *timestamp,
            TraceEvent::SpanEnter { timestamp, .. } => *timestamp,
            TraceEvent::SpanExit { timestamp, .. } => *timestamp,
            TraceEvent::SpanClose { timestamp, .. } => *timestamp,
            TraceEvent::Event { timestamp, .. } => *timestamp,
        }
    }
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

// This owns all the consumers.
// Whenever a a new producer/consumer pair is created it will be sent here
// The worker thread will own one instance struct to pop events from
struct ConsumerRegistry {
    /// Channel for receiving new consumers when a producer/consumer pair is created.
    new_consumer_rx: mpsc::Receiver<rtrb::Consumer<TraceEvent>>,
    consumers: Vec<rtrb::Consumer<TraceEvent>>,
}

impl ConsumerRegistry {
    fn new(rx: mpsc::Receiver<rtrb::Consumer<TraceEvent>>) -> Self {
        Self {
            new_consumer_rx: rx,
            consumers: Vec::new(),
        }
    }

    /// Reads events from consumers into `buffer` in timestamp order.
    /// Drains all events then sorts - O(n log n)
    /// Benchmarks show that this actually performs the same as a O(n log k) k-way merge
    /// as popping dominates
    ///
    /// Returns the number of events read
    fn drain_into(&mut self, buffer: &mut Vec<TraceEvent>) -> usize {
        while let Ok(consumer) = self.new_consumer_rx.try_recv() {
            self.consumers.push(consumer);
        }

        let mut count = 0;
        for consumer in &mut self.consumers {
            while let Ok(event) = consumer.pop() {
                buffer.push(event);
                count += 1;
            }
        }

        buffer.sort_by_key(|e| e.timestamp());

        count
    }
}

/// Handle for registering new thread-local producers.
/// Cloned into thread-local storage to register new ring buffers.
#[derive(Clone)]
struct ProducerRegistration {
    /// Channel to send new consumers to the worker.
    sender: mpsc::Sender<rtrb::Consumer<TraceEvent>>,
    alive: Arc<AtomicBool>,
    /// Pool of pre-allocated and pre-touched producers.
    /// Threads take from here first to avoid initialization latency.
    producer_pool: Arc<Mutex<Vec<rtrb::Producer<TraceEvent>>>>,
}

impl ProducerRegistration {
    /// Create a new producer for this thread and register its consumer with the worker.
    /// First tries to take from the pre-allocated pool, then creates new if empty.
    fn create_producer(&self) -> Option<rtrb::Producer<TraceEvent>> {
        if !self.alive.load(Ordering::Relaxed) {
            return None;
        }

        if let Ok(mut pool) = self.producer_pool.lock() {
            if let Some(producer) = pool.pop() {
                return Some(producer);
            }
        }

        let (producer, consumer) = rtrb::RingBuffer::new(THREAD_RING_CAPACITY);

        let _ = self.sender.send(consumer);

        Some(producer)
    }
}

/// Thread-local state for the trace producer.
/// Uses UnsafeCell + MaybeUninit for zero-overhead access on the hot path.
/// The `initialized` flag guards access to the uninitialized producer.
struct ThreadLocalProducer {
    /// The SPSC producer. Only valid to access when `initialized` is true.
    producer: UnsafeCell<MaybeUninit<rtrb::Producer<TraceEvent>>>,
    dropped_count: UnsafeCell<u64>,
    /// Whether this thread-local has been initialized.
    /// This is the only branch on the hot path.
    initialized: UnsafeCell<bool>,
}

impl ThreadLocalProducer {
    const fn new() -> Self {
        Self {
            producer: UnsafeCell::new(MaybeUninit::uninit()),
            dropped_count: UnsafeCell::new(0),
            initialized: UnsafeCell::new(false),
        }
    }

    /// Only call when initialized
    /// Returns true if pushed successfully, false if buffer full.
    #[inline(always)]
    unsafe fn push_unchecked(&self, event: TraceEvent) -> bool {
        // SAFETY: Caller guarantees we're initialized
        unsafe {
            let producer = (*self.producer.get()).assume_init_mut();
            producer.push(event).is_ok()
        }
    }

    #[inline(always)]
    fn is_initialized(&self) -> bool {
        // SAFETY: Single-threaded access
        unsafe { *self.initialized.get() }
    }

    fn initialize(&self, producer_registration: &ProducerRegistration) {
        // SAFETY: Single-threaded access
        unsafe {
            if !*self.initialized.get() {
                if let Some(producer) = producer_registration.create_producer() {
                    (*self.producer.get()).write(producer);
                    *self.initialized.get() = true;
                }
            }
        }
    }

    fn increment_dropped(&self) {
        // SAFETY: Single-threaded access
        unsafe {
            let count = self.dropped_count.get();
            *count += 1;
            if *count == 1 || (*count).is_multiple_of(1000) {
                let (thread_name, _) = get_thread_info();
                eprintln!(
                    "thread '{}' dropped {} telemetry events (buffer full)",
                    thread_name, *count
                );
            }
        }
    }
}

thread_local! {
    /// Cached thread info (thread_name, thread_id) for minimal overhead.
    /// Strings are leaked once per thread to get &'static str - threads are long-lived so this is fine.
    /// Uses Cell since (&'static str, &'static str) is Copy.
    static CACHED_THREAD_INFO: Cell<Option<(&'static str, &'static str)>> = const { Cell::new(None) };
    /// Each thread's SPSC producer. Uses UnsafeCell for minimal overhead.
    static THREAD_PRODUCER: ThreadLocalProducer = const { ThreadLocalProducer::new() };
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
            let tid = std::thread::current().id();
            // SAFETY: ThreadId is a newtype wrapper around a u64 counter.
            // This transmute relies on the internal representation of ThreadId,
            // which is stable in practice but not guaranteed by Rust's API.
            // On non-Linux platforms this is a best-effort approximation.
            // See: https://doc.rust-lang.org/std/thread/struct.ThreadId.html
            let tid_num = unsafe { std::mem::transmute::<std::thread::ThreadId, u64>(tid) };
            Box::leak(tid_num.to_string().into_boxed_str())
        };

        cache.set(Some((thread_name, thread_id)));
        (thread_name, thread_id)
    })
}

/// The trace event dispatcher that captures events once and dispatches to multiple sinks
/// on a background thread.
pub struct TraceEventDispatcher {
    /// Registration handle for creating new thread-local producers.
    producer_registration: ProducerRegistration,
    /// Separate channel so we are always notified of when the main queue is full and events are being dropped.
    dropped_sender: Option<mpsc::Sender<TraceEvent>>,
    _worker_handle: WorkerHandle,
    max_level: Option<LevelFilter>,
    dropped_events: Arc<AtomicU64>,
}

struct WorkerHandle {
    join_handle: Option<JoinHandle<()>>,
}

impl TraceEventDispatcher {
    /// Create a new trace event dispatcher with the given sinks.
    /// Uses thread-local SPSC ring buffers to ensure low-latency, low-jitter
    /// event capture. Events are dropped if the thread's ring buffer is full.
    ///
    /// # Arguments
    /// * `sinks` - List of sinks to dispatch events to.
    pub(crate) fn new(sinks: Vec<Box<dyn TraceEventSink>>) -> Self {
        let max_level = Self::derive_max_level(&sinks);

        let (consumer_tx, consumer_rx) = mpsc::channel();
        let (dropped_sender, dropped_receiver) = mpsc::channel();
        let dropped_events = Arc::new(AtomicU64::new(0));
        let dropped_events_worker = Arc::clone(&dropped_events);
        let alive = Arc::new(AtomicBool::new(true));
        let alive_worker = Arc::clone(&alive);

        let mut producer_pool = Vec::with_capacity(PREALLOC_PRODUCER_COUNT);
        // It's better that we take a few ms to warm once at startup than randomly take a ms to warmup the first time
        // a thread needs a producer, or to have a cold start
        for _ in 0..PREALLOC_PRODUCER_COUNT {
            let (mut producer, consumer) = rtrb::RingBuffer::new(THREAD_RING_CAPACITY);
            for _ in 0..THREAD_RING_CAPACITY - 1 {
                let _ = producer.push(TraceEvent::SpanClose {
                    id: 0,
                    timestamp: SystemTime::UNIX_EPOCH,
                });
            }
            let _ = consumer_tx.send(consumer);
            producer_pool.push(producer);
        }
        let producer_pool = Arc::new(Mutex::new(producer_pool));

        let worker_handle = std::thread::Builder::new()
            .name("telemetry-worker".into())
            .spawn(move || {
                worker_loop(
                    ConsumerRegistry::new(consumer_rx),
                    dropped_receiver,
                    sinks,
                    dropped_events_worker,
                    alive_worker,
                );
            })
            .expect("failed to spawn telemetry worker thread");

        let producer_registration = ProducerRegistration {
            sender: consumer_tx,
            alive,
            producer_pool,
        };

        Self {
            producer_registration,
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

    #[inline(always)]
    fn send_event(&self, event: TraceEvent) {
        THREAD_PRODUCER.with(|producer| {
            if producer.is_initialized() {
                // SAFETY: is_initialized() returned true
                if unsafe { !producer.push_unchecked(event) } {
                    self.drop_event(producer);
                }
            } else {
                self.send_event_uninitialized(producer, event);
            }
        });
    }

    #[cold]
    #[inline(never)]
    fn send_event_uninitialized(&self, producer: &ThreadLocalProducer, event: TraceEvent) {
        producer.initialize(&self.producer_registration);

        if producer.is_initialized() {
            // SAFETY: just initialized
            unsafe {
                producer.push_unchecked(event);
            }
        } else {
            self.drop_event(producer);
        }
    }

    #[cold]
    #[inline(never)]
    fn drop_event(&self, producer: &ThreadLocalProducer) {
        producer.increment_dropped();
        let dropped = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

        if dropped == 1 || dropped.is_multiple_of(1000) {
            eprintln!(
                "[telemetry]: {} events and log lines dropped que to full buffer (capacity: {})",
                dropped, THREAD_RING_CAPACITY
            );
            self.send_drop_event(dropped);
        }
    }

    fn send_drop_event(&self, total_dropped: u64) {
        if let Some(dropped_sender) = &self.dropped_sender {
            let (thread_name, thread_id) = get_thread_info();

            let mut fields = TraceFields::new();
            fields.push((
                "message",
                FieldValue::Str(format!(
                    "Telemetry events and log lines dropped due to full buffer (capacity: {}). Worker may be falling behind.",
                    THREAD_RING_CAPACITY
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
        self.producer_registration
            .alive
            .store(false, Ordering::Release);
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

impl<'a> tracing::field::Visit for FieldVisitor<'a> {
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

/// Background worker loop drains events from all rtrb::Consumers and priority channels,
/// and dispatches them to sinks. Priority events are processed first.
/// Runs until the alive flag is set to false and all buffers are drained.
fn worker_loop(
    mut consumers: ConsumerRegistry,
    dropped_receiver: mpsc::Receiver<TraceEvent>,
    mut sinks: Vec<Box<dyn TraceEventSink>>,
    dropped_events: Arc<AtomicU64>,
    alive: Arc<AtomicBool>,
) {
    const FLUSH_INTERVAL: Duration = Duration::from_millis(100);
    const FLUSH_EVENT_COUNT: usize = 1000;
    let mut last_flush = std::time::Instant::now();
    let mut events_since_flush = 0;
    let mut buffer = Vec::with_capacity(256);

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
        while let Ok(event) = dropped_receiver.try_recv() {
            dispatch_to_sinks(&mut sinks, event);
            events_since_flush += 1;
        }

        buffer.clear();
        let event_count = consumers.drain_into(&mut buffer);

        if event_count > 0 {
            for event in buffer.drain(..) {
                dispatch_to_sinks(&mut sinks, event);
            }
            events_since_flush += event_count;

            if events_since_flush >= FLUSH_EVENT_COUNT || last_flush.elapsed() >= FLUSH_INTERVAL {
                flush_sinks(&mut sinks);
                last_flush = std::time::Instant::now();
                events_since_flush = 0;
            }
        } else {
            if last_flush.elapsed() >= FLUSH_INTERVAL {
                flush_sinks(&mut sinks);
                last_flush = std::time::Instant::now();
                events_since_flush = 0;
            }

            if !alive.load(Ordering::Acquire) {
                break;
            }
        }
    }

    while let Ok(event) = dropped_receiver.try_recv() {
        dispatch_to_sinks(&mut sinks, event);
    }

    buffer.clear();
    consumers.drain_into(&mut buffer);
    for event in buffer.drain(..) {
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
        if let Some(handle) = self.join_handle.take() {
            if let Err(e) = handle.join() {
                eprintln!("[telemetry] worker thread panicked: {:?}", e);
            }
        }
    }
}
