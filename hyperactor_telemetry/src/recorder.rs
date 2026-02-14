/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::mem::take;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use tracing::Level;
use tracing::Metadata;
use tracing::Span;
use tracing::Subscriber;
use tracing::field::Field;
use tracing::field::Visit;
use tracing::span;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::registry::Scope;

use crate::SPAN_FIELD_RECORDING;
use crate::pool::Pool;
use crate::spool::Spool;

/// Key is provides a guaranteed unique [`KeyRef`] for as long as
/// the [`Key`] is alive.
#[derive(Debug)]
struct Key(
    // We have to use a u8 (as opposed to, e.g., ()) here, as
    // Rust gets clever about zero size objects, and allocates
    // them all to 0x1.
    Box<u8>,
);

impl Key {
    /// Create a new unique key.
    fn new() -> Self {
        Self(Box::new(0u8))
    }

    /// Produce a reference to this key. The reference is guaranteed to be
    /// unique for the lifetime of this key. The user must be careful to
    /// ensure that there are no outstanding [`KeyRef`]s after its corresponding
    /// [`Key`] is dropped.
    fn as_key_ref(&self) -> KeyRef {
        KeyRef(&*self.0 as *const _)
    }
}

/// A reference to a [`Key`]. KeyRefs may be used as keys in a map,
/// and are guaranteed to be unique for the lifetime of the [`Key`].
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct KeyRef(*const u8);

// SAFETY: We only ever pass these around as values. They are never
// dereferenced.
unsafe impl Send for KeyRef {}
// SAFETY: We only ever pass these around as values. They are never
// dereferenced.
unsafe impl Sync for KeyRef {}

impl Copy for KeyRef {}

impl From<Key> for KeyRef {
    fn from(value: Key) -> Self {
        Self(&*value.0 as *const _)
    }
}

impl From<u64> for KeyRef {
    fn from(value: u64) -> Self {
        Self(value as *const _)
    }
}

impl From<KeyRef> for u64 {
    fn from(value: KeyRef) -> Self {
        value.0 as u64
    }
}

/// A value that can appear in an entry. This custom
/// representation serves two purposes: 1) internally, to
/// to manage string buffer reuse; and 2) as a serialization
/// format that may be used with bincode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    /// A String.
    String(String),
    /// An i64.
    I64(i64),
    /// A u64.
    U64(u64),
    /// An f64.
    F64(f64),
    /// A bool.
    Bool(bool),
}

impl Value {
    /// Convert this Value to a JSON value
    pub fn to_json(&self) -> serde_json::Value {
        match &self {
            Self::String(s) => serde_json::Value::String(s.clone()),
            Self::I64(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            Self::U64(u) => serde_json::Value::Number(serde_json::Number::from(*u)),
            Self::F64(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap()),
            Self::Bool(b) => serde_json::Value::Bool(*b),
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Bool(false)
    }
}

#[derive(Debug, Default, Clone)]
struct Entry {
    name: &'static str,
    value: Value,
    buffer: Option<String>,
}

impl Entry {
    fn reset(&mut self) {
        if let Value::String(mut s) = take(&mut self.value) {
            s.clear();
            self.buffer = Some(s);
        }
    }

    fn set_str(&mut self, name: &'static str, value: &str) {
        self.reset();
        let mut buf = self.buffer.take().unwrap_or_else(String::new);
        buf.clear();
        buf.push_str(value);
        self.name = name;
        self.value = Value::String(buf);
    }

    fn set_error(&mut self, name: &'static str, value: &(dyn std::error::Error + 'static)) {
        self.reset();
        let mut buf = self.buffer.take().unwrap_or_else(String::new);

        let mut formatter =
            core::fmt::Formatter::new(&mut buf, core::fmt::FormattingOptions::new());

        Display::fmt(value, &mut formatter)
            .expect("a Display implementation returned an error unexpectedly");

        self.name = name;
        self.value = Value::String(buf);
    }

    fn set_debug(&mut self, name: &'static str, value: &dyn std::fmt::Debug) {
        self.reset();
        let mut buf = self.buffer.take().unwrap_or_else(String::new);

        let mut formatter =
            core::fmt::Formatter::new(&mut buf, core::fmt::FormattingOptions::new());

        Debug::fmt(value, &mut formatter)
            .expect("a Debug implementation returned an error unexpectedly");

        self.name = name;
        self.value = Value::String(buf);
    }

    fn set_i64(&mut self, name: &'static str, value: i64) {
        self.reset();
        self.name = name;
        self.value = Value::I64(value);
    }

    fn set_u64(&mut self, name: &'static str, value: u64) {
        self.reset();
        self.name = name;
        self.value = Value::U64(value);
    }

    fn set_f64(&mut self, name: &'static str, value: f64) {
        self.reset();
        self.name = name;
        self.value = Value::F64(value);
    }

    fn set_bool(&mut self, name: &'static str, value: bool) {
        self.reset();
        self.name = name;
        self.value = Value::Bool(value);
    }
}

/// An event that has been recorded by a [`Recorder`].
#[derive(Debug, Clone)]
pub struct Event {
    /// The time at which the event was recorded.
    pub time: SystemTime,

    /// The metadata for the event.
    pub metadata: &'static Metadata<'static>,

    /// All other (structured) fields defined in the event.
    fields: Vec<Entry>,

    /// The number of fields defined.
    num_fields: usize,

    /// A monotonically increasing sequence number.
    pub seq: usize,
}

impl Event {
    fn reset(&mut self, time: SystemTime, metadata: &'static Metadata<'static>, seq: usize) {
        self.time = time;
        self.metadata = metadata;
        self.num_fields = 0;
        self.seq = seq;
    }

    fn next_field(&mut self) -> &mut Entry {
        while self.fields.len() <= self.num_fields {
            self.fields.push(Default::default());
        }
        let field = self.fields.get_mut(self.num_fields).unwrap();
        field.reset();
        self.num_fields += 1;
        field
    }

    /// The fields of this event, structed as a JSON value.
    pub fn json_value(&self) -> serde_json::Value {
        serde_json::Value::Object(self.json_fields())
    }

    /// The fields of this event, structured as a JSON map.
    pub fn json_fields(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::new();
        for field in &self.fields {
            map.insert(field.name.to_string(), field.value.to_json());
        }
        map
    }

    /// The fields of this event, using the [`Value`] representation.
    pub fn fields(&self) -> Vec<(String, Value)> {
        self.fields
            .iter()
            .map(|field| (field.name.to_string(), field.value.clone()))
            .collect()
    }
}

impl Default for Event {
    fn default() -> Self {
        static __CALLSITE: tracing::__macro_support::MacroCallsite = tracing::callsite2! {
            name: "",
            kind: tracing::metadata::Kind::SPAN,
            level: Level::DEBUG,
            fields:
        };
        static DEFAULT_METADATA: Metadata<'static> = tracing::metadata! {
            name: "",
            target: "",
            level: Level::DEBUG,
            fields: &[],
            callsite: &__CALLSITE,
            kind: tracing::metadata::Kind::SPAN,
        };
        Self {
            time: SystemTime::now(),
            metadata: &DEFAULT_METADATA,
            fields: Vec::new(),
            num_fields: 0,
            seq: 0,
        }
    }
}

/// Visitor to populate an [`Event`] from a [`tracing::Event`].
impl Visit for Event {
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.next_field().set_i64(field.name(), value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.next_field().set_u64(field.name(), value);
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.next_field().set_f64(field.name(), value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.next_field().set_bool(field.name(), value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.next_field().set_str(field.name(), value);
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.next_field().set_error(field.name(), value);
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.next_field().set_debug(field.name(), value);
    }
}

/// A recording of events from a [`Recorder`].
#[derive(Debug)]
pub struct Recording {
    state: Arc<RecordingState>,
    recorder_state: Arc<RecorderState>,
}

impl Recording {
    fn new(cap: usize, recorder_state: Arc<RecorderState>) -> Self {
        assert!(cap > 1, "capacity must be > 1");

        let state = Arc::new(RecordingState {
            key: Key::new(),
            active: Mutex::new(HashMap::new()),
            spool: Spool::new(cap),
            seq: AtomicUsize::new(0),
        });
        assert!(
            recorder_state
                .recordings
                .insert(state.key.as_key_ref(), Arc::clone(&state))
                .is_none(),
            "non-unique key"
        );

        Self {
            state,
            recorder_state,
        }
    }

    /// Return a span, which will record events to this recording when entered.
    ///
    /// Uses `parent: None` so that this span is always a root span. Without this,
    /// contextual parentage would cause a spawned actor's recording span to become
    /// a child of the spawning actor's recording span (via `Instance::start()`'s
    /// `.instrument(Span::current())`), causing events to leak into the parent
    /// actor's flight recorder.
    pub fn span(&self) -> Span {
        span!(
            parent: None,
            Level::INFO,
            SPAN_FIELD_RECORDING,
            recording = self.recording_key(),
            recorder = self.recorder_key(),
        )
    }

    /// Retrieve the tail of the recorded event log,
    /// up to the capacity of the recording.
    pub fn tail(&self) -> Vec<Event> {
        self.state.spool.tail()
    }

    /// Return the set of currently active span stacks. The metadata has
    /// enough information to recover a sparse stack trace for the trace.
    pub fn stacks(&self) -> Vec<Vec<&'static Metadata<'static>>> {
        let snapshot = self.state.active.lock().unwrap().clone();

        let parents: HashSet<Id> = snapshot
            .iter()
            .filter_map(|(_, (_, parent))| parent.clone())
            .collect();

        snapshot
            .iter()
            .filter_map(|(id, _)| {
                if parents.contains(id) {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .map(|id| {
                let mut stack = Vec::new();
                let mut parent: Option<Id> = Some(id);
                while let Some(id) = parent {
                    let Some((meta, next_parent)) = snapshot.get(&id) else {
                        break;
                    };
                    stack.push(*meta);
                    parent = next_parent.clone();
                }
                stack
            })
            .collect()
    }

    fn recording_key_ref(&self) -> KeyRef {
        self.state.key.as_key_ref()
    }

    fn recording_key(&self) -> u64 {
        self.recording_key_ref().into()
    }

    fn recorder_key(&self) -> u64 {
        self.recorder_state.key.as_key_ref().into()
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        assert!(
            self.recorder_state
                .recordings
                .remove(&self.state.key.as_key_ref())
                .is_some(),
            "missing recording"
        );
    }
}

#[derive(Debug)]
struct RecordingState {
    active: Mutex<HashMap<Id, (&'static Metadata<'static>, Option<Id>)>>,
    key: Key,
    seq: AtomicUsize,
    spool: Spool<Event>,
}

/// A recorder captures events from a [`tracing::span`] and records them
/// to a [`mpsc::UnboundedSender`]. In order to record events, the recorder's
/// layer ([`Recorder::layer`]) must be installed into the relevant tracing
/// subscriber.
pub struct Recorder {
    state: Arc<RecorderState>,
}

#[derive(Debug)]
struct RecorderState {
    key: Key,
    recordings: Arc<DashMap<KeyRef, Arc<RecordingState>>>,
    pool: Pool<Event>,
}

impl Recorder {
    /// Create a new recorder.
    pub fn new() -> Self {
        let state = Arc::new(RecorderState {
            key: Key::new(),
            recordings: Arc::new(DashMap::new()),
            pool: Pool::new(1024),
        });
        Self { state }
    }

    /// Create a new recording that can be used to selective capture
    /// events and span traces.
    pub fn record(&self, cap: usize) -> Recording {
        Recording::new(cap, Arc::clone(&self.state))
    }

    /// The layer associated with this recorder. This layer must be
    /// installed into the relevant tracing subscriber in order to
    /// record events.
    pub fn layer(&self) -> RecorderLayer {
        RecorderLayer {
            state: Arc::clone(&self.state),
        }
    }
}

/// The type of layer used by [`Recorder`].
pub struct RecorderLayer {
    state: Arc<RecorderState>,
}

impl RecorderLayer {
    /// Return an iterator over all the [`KeyRef`]s for the given scope.
    fn iter_recordings<'a, S>(
        &'a self,
        scope: Scope<'a, S>,
    ) -> impl Iterator<Item = dashmap::mapref::one::Ref<'a, KeyRef, Arc<RecordingState>>> + 'a
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        // We can provide deadlock freedom here because we always iterate in the same order:
        // from root to leaf; thus there can be no cycles.
        scope
            .from_root()
            .filter_map(|span| match span.extensions().get::<(KeyRef, KeyRef)>() {
                Some((recording_key_ref, recorder_key_ref))
                    if recorder_key_ref == &self.state.key.as_key_ref() =>
                {
                    Some(*recording_key_ref)
                }
                _ => None,
            })
            // Spans can outlive recording, which may no longer exist.
            .filter_map(|key_ref| self.state.recordings.get(&key_ref))
    }
}

impl<S: Subscriber> Layer<S> for RecorderLayer
where
    S: for<'span> LookupSpan<'span>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut visitor = RecordingKeysVisitor::new();

        attrs.record(&mut visitor);

        if let Some(keys) = visitor.keys() {
            if let Some(span) = ctx.span(id) {
                let mut extensions: tracing_subscriber::registry::ExtensionsMut<'_> =
                    span.extensions_mut();
                extensions.insert(keys);
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let Some(scope) = ctx.event_scope(event) else {
            return;
        };
        for state in self.iter_recordings(scope) {
            let mut recorded = self.state.pool.get();
            let seq = state.seq.fetch_add(1, Ordering::Relaxed);
            recorded.reset(SystemTime::now(), event.metadata(), seq);
            event.record(&mut recorded);
            state.spool.push(recorded);
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };

        for state in self.iter_recordings(span.scope()) {
            let mut active = state.active.lock().unwrap();
            active.insert(id.clone(), (span.metadata(), span.parent().map(|o| o.id())));
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };

        for state in self.iter_recordings(span.scope()) {
            let mut active = state.active.lock().unwrap();
            active.remove(id);
        }
    }
}

/// A visitor to pick out the recording key from spans.
struct RecordingKeysVisitor {
    recording_key: Option<KeyRef>,
    recorder_key: Option<KeyRef>,
}

impl RecordingKeysVisitor {
    fn new() -> Self {
        Self {
            recording_key: None,
            recorder_key: None,
        }
    }

    fn keys(&self) -> Option<(KeyRef, KeyRef)> {
        match (self.recording_key, self.recorder_key) {
            (Some(recording_key), Some(recorder_key)) => Some((recording_key, recorder_key)),
            _ => None,
        }
    }
}

impl Visit for RecordingKeysVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "recording" {
            self.recording_key = Some(value.into());
        } else if field.name() == "recorder" {
            self.recorder_key = Some(value.into());
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tracing::Level;
    use tracing::info;
    use tracing::span;
    use tracing_subscriber::Registry;
    use tracing_subscriber::prelude::*;

    use super::*;

    #[test]
    fn test_key() {
        let key = Key::new();
        assert_eq!(key.as_key_ref(), key.as_key_ref());

        assert_ne!(Key::new().as_key_ref(), Key::new().as_key_ref());
    }

    #[test]
    fn test_events_are_recorded() {
        let recorder = Recorder::new();
        let recording = recorder.record(10);
        tracing::subscriber::with_default(Registry::default().with(recorder.layer()), || {
            let span = recording.span();
            let _guard = span.enter();
            info!("This event should be recorded");
            info!("another event");
        });

        let events = recording.tail();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].json_value(),
            json!({
                "message": "This event should be recorded"
            })
        );
        assert_eq!(
            events[1].json_value(),
            json!({
                "message": "another event"
            })
        );
    }

    #[test]
    fn test_last_n_entries() {
        let recorder = Recorder::new();
        let recording = recorder.record(5);
        tracing::subscriber::with_default(Registry::default().with(recorder.layer()), || {
            let span = recording.span();
            let _guard = span.enter();
            for i in 0..10 {
                info!("event {}", i);
            }
        });

        let events = recording.tail();
        assert_eq!(events.len(), 5);
        for (i, event) in events.into_iter().enumerate() {
            assert_eq!(
                event.json_value(),
                json!({
                    "message": format!("event {}", i + 5)
                })
            );
            assert_eq!(event.seq, i + 5);
        }
    }

    #[test]
    fn test_events_outside_span_are_not_recorded() {
        let recorder = Recorder::new();
        let recording = recorder.record(10);
        tracing::subscriber::with_default(Registry::default().with(recorder.layer()), || {
            let _span = recording.span(); // not entered
            info!("This event should NOT be recorded");
        });
        assert_eq!(recording.tail().len(), 0);
    }

    #[test]
    fn test_child_span_inherits_recorder() {
        let recorder = Recorder::new();
        let recording = recorder.record(10);

        tracing::subscriber::with_default(Registry::default().with(recorder.layer()), || {
            let outer = recording.span();
            let _outer_guard = outer.enter();

            {
                let inner = span!(Level::INFO, "child_span");
                let _inner_guard = inner.enter();

                info!("Event from inner span");
            }
        });

        let events = recording.tail();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].json_value(),
            json!({
                "message": "Event from inner span"
            })
        );
    }

    // Ensures that nested tracing recordings remain isolated: events
    // emitted while span A is active are captured only by recording
    // A, and events emitted inside nested span B are captured only by
    // recording B. This verifies that the recorder correctly
    // attributes events to the currently entered span without
    // cross-contamination.
    #[test]
    fn test_recording_spans_are_isolated_across_nested_recordings() {
        let recorder = Recorder::new();
        let recording_a = recorder.record(10);
        let recording_b = recorder.record(10);

        tracing::subscriber::with_default(Registry::default().with(recorder.layer()), || {
            let span_a = recording_a.span();
            let _guard_a = span_a.enter();

            info!("event_from_a");

            {
                let span_b = recording_b.span();
                let _guard_b = span_b.enter();

                info!("event_from_b");
            }
        });

        let events_a: Vec<String> = recording_a
            .tail()
            .iter()
            .map(|e| e.json_value().to_string())
            .collect();
        let events_b: Vec<String> = recording_b
            .tail()
            .iter()
            .map(|e| e.json_value().to_string())
            .collect();

        let a_joined = events_a.join(" ");
        let b_joined = events_b.join(" ");

        assert!(
            a_joined.contains("event_from_a"),
            "recording A should contain event_from_a"
        );
        assert!(
            !a_joined.contains("event_from_b"),
            "recording A should NOT contain event_from_b"
        );
        assert!(
            b_joined.contains("event_from_b"),
            "recording B should contain event_from_b"
        );
        assert!(
            !b_joined.contains("event_from_a"),
            "recording B should NOT contain event_from_a"
        );
    }

    #[test]
    fn test_active_spans() {
        let recorder = Recorder::new();
        let recording = recorder.record(10);

        tracing::subscriber::with_default(Registry::default().with(recorder.layer()), || {
            let outer = recording.span();
            let _outer_guard = outer.enter();

            {
                let inner = span!(Level::INFO, "child_span");
                let _inner_guard = inner.enter();

                assert_eq!(recording.stacks().len(), 1);
                // TODO: possibly we should remove the 'recording' span from here.
                assert_eq!(recording.stacks()[0][0].name(), "child_span");
                assert_eq!(recording.stacks()[0][1].name(), "recording");
            }
        });
    }
}
