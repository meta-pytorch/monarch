/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tracing_perfetto_sdk_schema::CounterDescriptor;
use tracing_perfetto_sdk_schema::DebugAnnotation;
use tracing_perfetto_sdk_schema::DebugAnnotationName;
use tracing_perfetto_sdk_schema::EventCategory;
use tracing_perfetto_sdk_schema::EventName;
use tracing_perfetto_sdk_schema::InternedData;
use tracing_perfetto_sdk_schema::InternedString;
use tracing_perfetto_sdk_schema::ProcessDescriptor;
use tracing_perfetto_sdk_schema::ThreadDescriptor;
use tracing_perfetto_sdk_schema::TracePacket;
use tracing_perfetto_sdk_schema::TrackDescriptor;
use tracing_perfetto_sdk_schema::TrackEvent;
use tracing_perfetto_sdk_schema::debug_annotation::NameField;
use tracing_perfetto_sdk_schema::debug_annotation::Value as DBGValue;
use tracing_perfetto_sdk_schema::trace_packet::Data;
use tracing_perfetto_sdk_schema::trace_packet::OptionalTrustedPacketSequenceId;
use tracing_perfetto_sdk_schema::track_descriptor::StaticOrDynamicName;
use tracing_perfetto_sdk_schema::track_event;
use tracing_perfetto_sdk_schema::track_event::CounterValueField;
use tracing_perfetto_sdk_schema::track_event::Timestamp;
use tracing_perfetto_sdk_schema::track_event::Type as TrackEventType;

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct RawEvent {
    pub time: u64,
    pub time_us: u64,
    pub span_id: u64,
    pub parent_span_id: Option<u64>,
    pub name: Option<String>,
    pub event_type: String,
    pub message: Option<String>,
    pub actor_id: Option<String>,
    pub file: Option<String>,
    pub lineno: u32,
    pub os_pid: i32,
    pub tokio_task_id: i64,
    pub target: String,
    pub err: Option<String>,
    pub stacktrace: Option<String>,
    pub thread_name: Option<String>,
    #[serde(flatten)]
    #[serde(default)]
    pub extra_fields: HashMap<String, serde_json::Value>,
}

impl PartialEq for RawEvent {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
            && self.time_us == other.time_us
            && self.span_id == other.span_id
            && self.parent_span_id == other.parent_span_id
            && self.name == other.name
            && self.event_type == other.event_type
            && self.message == other.message
            && self.actor_id == other.actor_id
            && self.file == other.file
            && self.lineno == other.lineno
            && self.os_pid == other.os_pid
            && self.tokio_task_id == other.tokio_task_id
            && self.target == other.target
            && self.err == other.err
            && self.stacktrace == other.stacktrace
            && self.thread_name == other.thread_name
    }
}

impl Eq for RawEvent {}

impl PartialOrd for RawEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RawEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.time_us == other.time_us {
            return match (self.event_type.as_str(), other.event_type.as_str()) {
                ("start_span", "start_span") => std::cmp::Ordering::Equal,
                ("start_span", _) => std::cmp::Ordering::Less,
                (_, "start_span") => std::cmp::Ordering::Greater,
                ("async_enter", ot) if ot != "async_enter" => std::cmp::Ordering::Less,
                _ => std::cmp::Ordering::Equal,
            };
        }
        self.time_us.cmp(&other.time_us)
    }
}

impl From<&RawEvent> for DebugAnnotation {
    fn from(_value: &RawEvent) -> Self {
        // Note: DebugAnnotation structure needs to be updated based on actual prost generated code
        // For now, just return a default instance
        Self::default()
    }
}

#[derive(Default)]
struct Strings {
    next: u64,
    flushed: u64,
    data: HashMap<String, u64>,
}
impl Strings {
    fn intern<'a, T: Into<&'a str>>(&mut self, val: T) -> u64 {
        let val = val.into();
        if let Some(id) = self.data.get(val) {
            return *id;
        }
        self.next += 1;
        let id = self.next;
        self.data.insert(val.to_string(), id);
        id
    }

    fn needs_flush(&self) -> bool {
        self.flushed < self.next
    }

    fn flush(&mut self) {
        self.flushed = self.next;
    }

    fn to_flush(&self) -> impl Iterator<Item = (&String, &u64)> {
        self.data.iter().filter(|item| *item.1 >= self.flushed)
    }
}

pub trait Sink {
    fn consume(&mut self, packet: TracePacket);
}
pub struct CounterTrack<'a, T: Sink> {
    desc: TrackDescriptor,
    ctx: &'a mut Ctx<T>,
}

impl<'a, T: Sink> CounterTrack<'a, T> {
    pub fn name(mut self, name: &str) -> Self {
        self.desc.static_or_dynamic_name = Some(StaticOrDynamicName::StaticName(name.to_owned()));
        self
    }
    pub fn parent(mut self, id: u64) -> Self {
        self.desc.parent_uuid = Some(id);
        self
    }

    pub fn process(mut self, pid: i32) -> Self {
        let pd = ProcessDescriptor {
            pid: Some(pid),
            ..Default::default()
        };
        self.desc.process = Some(pd);
        self
    }

    pub fn consume(self) -> u64 {
        let id = self.desc.uuid.unwrap_or_default();
        let mut desc = self.desc;
        desc.counter = Some(CounterDescriptor::default());
        let tp = TracePacket {
            data: Some(Data::TrackDescriptor(desc)),
            ..Default::default()
        };
        self.ctx.consume(tp);
        id
    }
}

pub struct Track<'a, T: Sink> {
    desc: TrackDescriptor,
    ctx: &'a mut Ctx<T>,
}

impl<'a, T: Sink> Track<'a, T> {
    pub fn name(mut self, name: &str) -> Self {
        self.desc.static_or_dynamic_name = Some(StaticOrDynamicName::StaticName(name.to_owned()));
        self
    }
    pub fn parent(mut self, id: u64) -> Self {
        self.desc.parent_uuid = Some(id);
        self
    }

    pub fn process(mut self, pid: i32) -> Self {
        let pd = ProcessDescriptor {
            pid: Some(pid),
            ..Default::default()
        };
        self.desc.process = Some(pd);
        self
    }

    pub fn consume(self) -> u64 {
        let id = self.desc.uuid.unwrap_or_default();
        let tp = TracePacket {
            data: Some(Data::TrackDescriptor(self.desc)),
            ..Default::default()
        };
        self.ctx.consume(tp);
        id
    }
}

pub struct TimeUs(u64);

impl From<TimeUs> for Timestamp {
    fn from(value: TimeUs) -> Self {
        Timestamp::TimestampAbsoluteUs((value.0 * 1000) as i64)
    }
}

impl From<TimeUs> for u64 {
    fn from(value: TimeUs) -> Self {
        value.0 * 1000
    }
}
impl From<TimeUs> for Option<u64> {
    fn from(value: TimeUs) -> Self {
        Some(value.0 * 1000)
    }
}

/// Trait for types that can be named
pub trait Nameable {
    fn name(self, name: &str) -> Self;
}

/// Trait for types that can have annotations added
pub trait Annotable {
    fn add_annotation(self, key: &str, value: &serde_json::Value) -> Self;
}

pub struct Instant<'a, T: Sink> {
    event: TrackEvent,
    ts: TimeUs,
    ctx: &'a mut Ctx<T>,
}

impl<'a, T: Sink> Instant<'a, T> {
    pub fn name(mut self, name: &str) -> Self {
        let id = self.ctx.event_names.intern(name);
        self.event.name_field = Some(track_event::NameField::NameIid(id));
        self
    }

    pub fn debug(mut self, values: &serde_json::Value) -> Self {
        self.event
            .debug_annotations
            .push(self.ctx.debug_annotation("debug", values));
        self
    }

    pub fn add_annotation(mut self, name: &str, value: &serde_json::Value) -> Self {
        self.event
            .debug_annotations
            .push(self.ctx.debug_annotation(name, value));
        self
    }

    pub fn consume(self) {
        let event = self.event;
        let tp = TracePacket {
            timestamp: self.ts.into(),
            data: Some(Data::TrackEvent(event)),
            ..Default::default()
        };
        self.ctx.consume(tp);
    }
}

impl<'a, T: Sink> Nameable for Instant<'a, T> {
    fn name(self, name: &str) -> Self {
        self.name(name)
    }
}

impl<'a, T: Sink> Annotable for Instant<'a, T> {
    fn add_annotation(self, key: &str, value: &serde_json::Value) -> Self {
        self.add_annotation(key, value)
    }
}

pub struct Counter<'a, T: Sink> {
    event: TrackEvent,
    ts: TimeUs,
    ctx: &'a mut Ctx<T>,
}

impl<'a, T: Sink> Counter<'a, T> {
    pub fn name(mut self, name: &str) -> Self {
        let id = self.ctx.event_names.intern(name);
        self.event.name_field = Some(track_event::NameField::NameIid(id));
        self
    }

    pub fn track(mut self, id: u64) -> Self {
        self.event.track_uuid = Some(id);
        self
    }

    pub fn int(mut self, v: i64) -> Self {
        self.event.counter_value_field = Some(CounterValueField::CounterValue(v));
        self
    }

    pub fn float(mut self, v: f64) -> Self {
        self.event.counter_value_field = Some(CounterValueField::DoubleCounterValue(v));
        self
    }

    pub fn consume(self) {
        let mut event = self.event;
        event.r#type = Some(TrackEventType::Counter as i32);
        let tp = TracePacket {
            timestamp: self.ts.into(),
            data: Some(Data::TrackEvent(event)),
            ..Default::default()
        };
        self.ctx.consume(tp);
    }
}

pub struct StartSlice<'a, T: Sink> {
    event: TrackEvent,
    ts: TimeUs,
    ctx: &'a mut Ctx<T>,
}

impl<'a, T: Sink> StartSlice<'a, T> {
    pub fn name(mut self, name: &str) -> Self {
        let id = self.ctx.event_names.intern(name);
        self.event.name_field = Some(track_event::NameField::NameIid(id));
        self
    }

    pub fn get_name(&self) -> &str {
        // Note: This method may need to be updated based on how names are stored in prost
        ""
    }

    pub fn debug(mut self, values: &serde_json::Value) -> Self {
        self.event
            .debug_annotations
            .push(self.ctx.debug_annotation("debug", values));
        self
    }

    pub fn add_annotation(mut self, name: &str, value: &serde_json::Value) -> Self {
        self.event
            .debug_annotations
            .push(self.ctx.debug_annotation(name, value));
        self
    }

    pub fn consume(self) {
        let mut event = self.event;
        event.r#type = Some(TrackEventType::SliceBegin as i32);
        let tp = TracePacket {
            timestamp: self.ts.into(),
            data: Some(Data::TrackEvent(event)),
            ..Default::default()
        };
        self.ctx.consume(tp);
    }
}

impl<'a, T: Sink> Nameable for StartSlice<'a, T> {
    fn name(self, name: &str) -> Self {
        self.name(name)
    }
}

impl<'a, T: Sink> Annotable for StartSlice<'a, T> {
    fn add_annotation(self, key: &str, value: &serde_json::Value) -> Self {
        self.add_annotation(key, value)
    }
}

pub struct EndSlice<'a, T: Sink> {
    event: TrackEvent,
    ts: TimeUs,
    ctx: &'a mut Ctx<T>,
}

impl<'a, T: Sink> EndSlice<'a, T> {
    pub fn consume(self) {
        let mut event = self.event;
        event.r#type = Some(TrackEventType::SliceEnd as i32);
        let tp = TracePacket {
            timestamp: self.ts.into(),
            data: Some(Data::TrackEvent(event)),
            ..Default::default()
        };
        self.ctx.consume(tp);
    }
}

pub struct Ctx<T: Sink> {
    next_uuid: AtomicU64,
    seq: u32,

    // useful for translating from other frameworks
    remappped_ids: HashMap<u64, u64>,

    // interned data
    event_names: Strings,
    event_categories: Strings,
    debug_annotation_names: Strings,
    debug_annotation_strings: Strings,

    sink: T,
}

impl<T: Sink> Ctx<T> {
    pub fn remap(&mut self, old: u64) -> u64 {
        let next = self.next_uuid();
        *self.remappped_ids.entry(old).or_insert(next)
    }

    pub fn new(sink: T) -> Self {
        let seq = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        let mut ctx = Self {
            sink,
            seq,
            remappped_ids: Default::default(),
            next_uuid: Default::default(),
            event_names: Default::default(),
            event_categories: Default::default(),
            debug_annotation_names: Default::default(),
            debug_annotation_strings: Default::default(),
        };
        ctx.next_uuid();
        ctx.init_trace();
        ctx
    }

    pub fn init_trace(&mut self) {
        self.seq += 1;
        // Note: trusted_packet_sequence_id field doesn't exist, need to find correct field name
        let id = TracePacket {
            incremental_state_cleared: Some(true),
            first_packet_on_sequence: Some(true),
            sequence_flags: Some(3),
            ..Default::default()
        };
        self.consume(id);
    }

    pub fn instant(&mut self, track: u64, time_us: u64) -> Instant<'_, T> {
        let te = TrackEvent {
            track_uuid: Some(track),
            ..Default::default()
        };
        Instant {
            event: te,
            ts: TimeUs(time_us),
            ctx: self,
        }
    }

    pub fn start_slice(&mut self, track: u64, time_us: u64) -> StartSlice<'_, T> {
        let te = TrackEvent {
            track_uuid: Some(track),
            ..Default::default()
        };
        StartSlice {
            event: te,
            ts: TimeUs(time_us),
            ctx: self,
        }
    }

    pub fn counter(&mut self, time_us: u64) -> Counter<'_, T> {
        let te = TrackEvent::default();
        Counter {
            event: te,
            ts: TimeUs(time_us),
            ctx: self,
        }
    }

    pub fn end_slice(&mut self, track: u64, time_us: u64) -> EndSlice<'_, T> {
        let te = TrackEvent {
            track_uuid: Some(track),
            ..Default::default()
        };
        // Note: timestamp_absolute_us field doesn't exist, timestamp is set at TracePacket level
        EndSlice {
            event: te,
            ts: TimeUs(time_us),
            ctx: self,
        }
    }

    pub fn new_process(&mut self, pid: i32) -> u64 {
        let id = self.next_uuid();
        let pd = ProcessDescriptor {
            pid: Some(pid),
            ..Default::default()
        };
        let td = TrackDescriptor {
            uuid: Some(id),
            process: Some(pd),
            ..Default::default()
        };
        let tp = TracePacket {
            data: Some(Data::TrackDescriptor(td)),
            ..Default::default()
        };
        self.sink.consume(tp);
        id
    }

    pub fn new_thread(&mut self, pid: i32, tid: i32, name: String) -> u64 {
        let id = self.next_uuid();
        let thd = ThreadDescriptor {
            pid: Some(pid),
            tid: Some(tid),
            thread_name: Some(name),
            ..Default::default()
        };
        let td = TrackDescriptor {
            uuid: Some(id),
            thread: Some(thd),
            ..Default::default()
        };
        let tp = TracePacket {
            data: Some(Data::TrackDescriptor(td)),
            ..Default::default()
        };
        self.sink.consume(tp);
        id
    }

    pub fn new_track(&mut self, id: u64) -> Track<'_, T> {
        let td = TrackDescriptor {
            uuid: Some(id),
            ..Default::default()
        };
        Track {
            ctx: self,
            desc: td,
        }
    }
    pub fn new_counter_track(&mut self, id: u64) -> CounterTrack<'_, T> {
        let td = TrackDescriptor {
            uuid: Some(id),
            ..Default::default()
        };
        CounterTrack {
            ctx: self,
            desc: td,
        }
    }

    pub fn next_uuid(&self) -> u64 {
        self.next_uuid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn consume(&mut self, mut packet: TracePacket) {
        if self.needs_flush() {
            self.flush_interned_data();
        }
        packet.optional_trusted_packet_sequence_id = Some(
            OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.seq),
        );
        packet.sequence_flags = Some(2); // needs interned data

        self.sink.consume(packet);
    }

    fn needs_flush(&self) -> bool {
        self.debug_annotation_names.needs_flush()
            | self.event_names.needs_flush()
            | self.event_categories.needs_flush()
            | self.debug_annotation_strings.needs_flush()
    }

    fn flush_interned_data(&mut self) {
        let id = TracePacket {
            interned_data: Some(self.interned_data()),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.seq),
            ),
            sequence_flags: Some(2),
            ..Default::default()
        };
        self.sink.consume(id);
        self.event_categories.flush();
        self.event_names.flush();
        self.debug_annotation_names.flush();
        self.debug_annotation_strings.flush();
    }

    fn interned_data(&self) -> InternedData {
        // Populate interned_data with the interned strings from the HashMaps
        let mut interned_data = InternedData::default();

        for (category, id) in self.event_categories.to_flush() {
            let event_category = EventCategory {
                iid: Some(*id),
                name: Some(category.clone()),
                ..Default::default()
            };
            interned_data.event_categories.push(event_category);
        }

        for (name, id) in self.event_names.to_flush() {
            let event_name = EventName {
                iid: Some(*id),
                name: Some(name.clone()),
                ..Default::default()
            };
            interned_data.event_names.push(event_name);
        }

        for (name, id) in self.debug_annotation_names.to_flush() {
            let debug_annotation_name = DebugAnnotationName {
                iid: Some(*id),
                name: Some(name.clone()),
                ..Default::default()
            };
            interned_data
                .debug_annotation_names
                .push(debug_annotation_name);
        }
        for (val, id) in self.debug_annotation_strings.to_flush() {
            let ii = InternedString {
                iid: Some(*id),
                str: Some(val.clone().into_bytes().into()),
                ..Default::default()
            };
            interned_data.debug_annotation_string_values.push(ii);
        }

        interned_data
    }

    pub fn sink(self) -> T {
        self.sink
    }

    fn debug_annotation_value(&mut self, value: &serde_json::Value) -> DebugAnnotation {
        match value {
            Value::Null => DebugAnnotation::default(),
            Value::Bool(b) => DebugAnnotation {
                value: Some(DBGValue::BoolValue(*b)),
                ..Default::default()
            },
            Value::Number(number) => {
                if number.is_i64() {
                    DebugAnnotation {
                        value: Some(DBGValue::IntValue(number.as_i64().unwrap())),
                        ..Default::default()
                    }
                } else {
                    DebugAnnotation {
                        value: Some(DBGValue::DoubleValue(number.as_f64().unwrap())),
                        ..Default::default()
                    }
                }
            }
            Value::String(s) => DebugAnnotation {
                value: Some(DBGValue::StringValueIid(
                    self.debug_annotation_strings.intern(s.as_str()),
                )),
                ..Default::default()
            },
            Value::Array(values) => DebugAnnotation {
                array_values: values
                    .iter()
                    .map(|v| self.debug_annotation_value(v))
                    .collect(),
                ..Default::default()
            },
            Value::Object(map) => DebugAnnotation {
                dict_entries: map
                    .iter()
                    .map(|(k, v)| self.debug_annotation(k.as_str(), v))
                    .collect(),
                ..Default::default()
            },
        }
    }

    fn debug_annotation(&mut self, name: &str, value: &serde_json::Value) -> DebugAnnotation {
        let mut dbg = self.debug_annotation_value(value);
        dbg.name_field = Some(NameField::NameIid(self.debug_annotation_names.intern(name)));
        dbg
    }
}
