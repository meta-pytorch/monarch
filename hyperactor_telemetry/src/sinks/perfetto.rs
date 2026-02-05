/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Perfetto sink that writes trace events directly to .pftrace files on disk.
//!
//! This provides an alternative to Scuba-based tracing that:
//! - Has no row limits (vs 400K Scuba limit)
//! - Has no ingestion latency (immediate file writes)
//! - Uses native Perfetto protobuf format (no conversion needed)
//! - Supports distributed file systems like OILFS
//!
//! ## Directory Layout
//!
//! ```text
//! {trace_dir}/
//! ├── executions/
//! │   ├── {execution_id}/
//! │   │   ├── {process_name}.pftrace
//! │   │   └── ...
//! │   └── latest -> {execution_id}/   # symlink to most recent
//! ```
//!
//! ## Default Trace Directory
//!
//! If not specified, traces are written to `/tmp/{username}/monarch_traces/`

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Result;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::AttrValue;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::typeuri::Named;
use prost::Message;
use serde::Deserialize;
use serde::Serialize;
use tracing_core::LevelFilter;
use tracing_perfetto_sdk_schema::DebugAnnotation;
use tracing_perfetto_sdk_schema::DebugAnnotationName;
use tracing_perfetto_sdk_schema::EventName;
use tracing_perfetto_sdk_schema::InternedData;
use tracing_perfetto_sdk_schema::ProcessDescriptor;
use tracing_perfetto_sdk_schema::ThreadDescriptor;
use tracing_perfetto_sdk_schema::Trace;
use tracing_perfetto_sdk_schema::TracePacket;
use tracing_perfetto_sdk_schema::TrackDescriptor;
use tracing_perfetto_sdk_schema::TrackEvent;
use tracing_perfetto_sdk_schema::debug_annotation::NameField;
use tracing_perfetto_sdk_schema::debug_annotation::Value as DbgValue;
use tracing_perfetto_sdk_schema::trace_packet::Data;
use tracing_perfetto_sdk_schema::trace_packet::OptionalTrustedPacketSequenceId;
use tracing_perfetto_sdk_schema::track_descriptor::StaticOrDynamicName;
use tracing_perfetto_sdk_schema::track_event::NameField as EventNameField;
use tracing_perfetto_sdk_schema::track_event::Type as TrackEventType;
use tracing_subscriber::filter::Targets;

use crate::config::MONARCH_FILE_LOG_LEVEL;
use crate::trace_dispatcher::FieldValue;
use crate::trace_dispatcher::TraceEvent;
use crate::trace_dispatcher::TraceEventSink;
use crate::trace_dispatcher::TraceFields;
use crate::trace_dispatcher::get_field;

/// The target prefix for user-facing telemetry spans.
pub const USER_TELEMETRY_PREFIX: &str = "monarch_hyperactor::telemetry";

/// Controls what events are captured in Perfetto traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PerfettoTraceMode {
    /// Tracing is disabled - no events are written.
    Off,
    /// Only user-facing telemetry events (target starts with `monarch_hyperactor::telemetry`).
    #[default]
    User,
    /// All events (for debugging/development).
    Dev,
}

impl std::fmt::Display for PerfettoTraceMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PerfettoTraceMode::Off => write!(f, "off"),
            PerfettoTraceMode::User => write!(f, "user"),
            PerfettoTraceMode::Dev => write!(f, "dev"),
        }
    }
}

impl std::str::FromStr for PerfettoTraceMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "off" | "false" | "0" | "none" => Ok(PerfettoTraceMode::Off),
            "dev" | "all" | "debug" => Ok(PerfettoTraceMode::Dev),
            "user" | "true" | "1" => Ok(PerfettoTraceMode::User),
            _ => Err(anyhow::anyhow!("unknown trace mode: {}", s)),
        }
    }
}

impl Named for PerfettoTraceMode {
    fn typename() -> &'static str {
        "hyperactor_telemetry::sinks::perfetto::PerfettoTraceMode"
    }
}

impl AttrValue for PerfettoTraceMode {
    fn display(&self) -> String {
        self.to_string()
    }

    fn parse(s: &str) -> Result<Self, anyhow::Error> {
        s.parse()
    }
}

impl PerfettoTraceMode {
    /// Returns true if the given target should be included in the trace.
    pub fn should_include(&self, target: &str) -> bool {
        match self {
            PerfettoTraceMode::Off => false,
            PerfettoTraceMode::User => target.starts_with(USER_TELEMETRY_PREFIX),
            PerfettoTraceMode::Dev => true,
        }
    }
}

declare_attrs! {
    /// Perfetto trace mode controlling which events are captured.
    /// Valid values: "off", "user" (default), "dev"
    /// - "off": Tracing is disabled
    /// - "user": Only user-facing telemetry events (monarch_hyperactor::telemetry::*)
    /// - "dev": All events (for debugging)
    @meta(CONFIG = ConfigAttr::new(
        Some("PERFETTO_TRACE_MODE".to_string()),
        Some("perfetto_trace_mode".to_string()),
    ))
    pub attr PERFETTO_TRACE_MODE: PerfettoTraceMode = PerfettoTraceMode::User;
}

/// Environment variable to override the default trace directory.
pub const MONARCH_TRACE_DIR_ENV: &str = "MONARCH_TRACE_DIR";

/// Returns the default trace directory.
///
/// Uses `$MONARCH_TRACE_DIR` if set, otherwise `/tmp/{username}/monarch_traces/`.
pub fn default_trace_dir() -> PathBuf {
    if let Ok(dir) = std::env::var(MONARCH_TRACE_DIR_ENV) {
        return PathBuf::from(dir);
    }
    let username = whoami::username();
    PathBuf::from(format!("/tmp/{}/monarch_traces", username))
}

/// Metadata stored for each span, used when Enter/Exit events occur.
struct SpanInfo {
    /// Fully qualified name: {target}::{name}
    fq_name: String,
    fields: TraceFields,
    file: Option<&'static str>,
    line: Option<u32>,
}

/// String interning for Perfetto trace compression.
#[derive(Default)]
struct InternedStrings {
    next_iid: u64,
    strings: HashMap<String, u64>,
    pending: Vec<(String, u64)>,
}

impl InternedStrings {
    fn intern(&mut self, s: &str) -> u64 {
        if let Some(&iid) = self.strings.get(s) {
            return iid;
        }
        self.next_iid += 1;
        let iid = self.next_iid;
        self.strings.insert(s.to_string(), iid);
        self.pending.push((s.to_string(), iid));
        iid
    }

    fn take_pending(&mut self) -> Vec<(String, u64)> {
        std::mem::take(&mut self.pending)
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

/// File-based Perfetto sink that writes .pftrace files.
pub struct PerfettoFileSink {
    writer: BufWriter<File>,
    pending_packets: Vec<TracePacket>,
    next_track_id: AtomicU64,
    /// Trusted packet sequence ID
    sequence_id: u32,
    event_names: InternedStrings,
    annotation_names: InternedStrings,
    span_info: HashMap<u64, SpanInfo>,
    /// Maps thread names to track ids
    thread_tracks: HashMap<String, u64>,
    /// track_id of this process
    process_track: u64,
    pid: i32,
    process_name: String,
    target_filter: Targets,
    trace_mode: PerfettoTraceMode,
}

impl PerfettoFileSink {
    /// Create a new Perfetto file sink.
    ///
    /// # Arguments
    /// * `trace_dir` - Base directory for trace files (use `default_trace_dir()` for default)
    /// * `execution_id` - Unique identifier for this execution/run
    /// * `process_name` - Name of this process (used in directory layout)
    ///
    /// Creates the directory structure and updates the `latest` symlink.
    pub fn new(
        trace_dir: impl AsRef<Path>,
        execution_id: &str,
        process_name: &str,
    ) -> Result<Self> {
        let trace_dir = trace_dir.as_ref().to_path_buf();
        let pid = std::process::id() as i32;

        let executions_dir = trace_dir.join("executions");
        let execution_dir = executions_dir.join(execution_id);
        fs::create_dir_all(&execution_dir)?;

        // Update the `latest` symlink
        let latest_link = executions_dir.join("latest");
        let _ = fs::remove_file(&latest_link);
        if let Err(e) = symlink(execution_id, &latest_link) {
            tracing::debug!("Failed to create latest symlink: {}", e);
        }

        let path = execution_dir.join(format!("{}.pftrace", process_name));
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);

        let sequence_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        let mut sink = Self {
            writer,
            pending_packets: Vec::new(),
            next_track_id: AtomicU64::new(1), // Start at 1, 0 is reserved
            sequence_id,
            event_names: InternedStrings::default(),
            annotation_names: InternedStrings::default(),
            span_info: HashMap::new(),
            thread_tracks: HashMap::new(),
            process_track: 0,
            pid,
            process_name: process_name.to_string(),
            target_filter: Targets::new().with_default({
                let log_level_str =
                    hyperactor_config::global::try_get_cloned(MONARCH_FILE_LOG_LEVEL)
                        .unwrap_or_else(|| "info".to_string());
                let level =
                    tracing::Level::from_str(&log_level_str).unwrap_or(tracing::Level::INFO);
                LevelFilter::from_level(level)
            }),
            trace_mode: hyperactor_config::global::get(PERFETTO_TRACE_MODE),
        };

        sink.write_sequence_header();

        sink.process_track = sink.create_process_track();

        Ok(sink)
    }

    fn next_track_id(&self) -> u64 {
        self.next_track_id.fetch_add(1, Ordering::Relaxed)
    }

    fn write_sequence_header(&mut self) {
        let packet = TracePacket {
            incremental_state_cleared: Some(true),
            first_packet_on_sequence: Some(true),
            sequence_flags: Some(3),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.sequence_id),
            ),
            ..Default::default()
        };

        self.write_packet(&packet);
    }

    fn create_process_track(&mut self) -> u64 {
        let track_id = self.next_track_id();

        let packet = TracePacket {
            data: Some(Data::TrackDescriptor(TrackDescriptor {
                uuid: Some(track_id),
                process: Some(ProcessDescriptor {
                    pid: Some(self.pid),
                    process_name: Some(self.process_name.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.sequence_id),
            ),
            ..Default::default()
        };

        self.write_packet(&packet);

        track_id
    }

    fn get_or_create_thread_track(&mut self, thread_name: &str) -> u64 {
        if let Some(&track) = self.thread_tracks.get(thread_name) {
            return track;
        }

        let track_id = self.next_track_id();
        let tid = self.thread_tracks.len() as i32 + 1;

        let packet = TracePacket {
            data: Some(Data::TrackDescriptor(TrackDescriptor {
                uuid: Some(track_id),
                parent_uuid: Some(self.process_track),
                static_or_dynamic_name: Some(StaticOrDynamicName::Name(thread_name.to_string())),
                thread: Some(ThreadDescriptor {
                    pid: Some(self.pid),
                    tid: Some(tid),
                    thread_name: Some(thread_name.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.sequence_id),
            ),
            ..Default::default()
        };
        self.write_packet(&packet);

        self.thread_tracks.insert(thread_name.to_string(), track_id);

        track_id
    }

    fn write_packet(&mut self, packet: &TracePacket) {
        self.pending_packets.push(packet.clone());
    }

    fn write_pending_packets(&mut self) -> Result<()> {
        if self.pending_packets.is_empty() {
            return Ok(());
        }

        let trace = Trace {
            packet: std::mem::take(&mut self.pending_packets),
        };

        let bytes = trace.encode_to_vec();
        self.writer.write_all(&bytes)?;
        Ok(())
    }

    fn flush_interned_data(&mut self) {
        if !self.event_names.has_pending() && !self.annotation_names.has_pending() {
            return;
        }

        let mut interned_data = InternedData::default();

        for (name, iid) in self.event_names.take_pending() {
            interned_data.event_names.push(EventName {
                iid: Some(iid),
                name: Some(name),
                ..Default::default()
            });
        }

        for (name, iid) in self.annotation_names.take_pending() {
            interned_data
                .debug_annotation_names
                .push(DebugAnnotationName {
                    iid: Some(iid),
                    name: Some(name),
                    ..Default::default()
                });
        }

        let packet = TracePacket {
            interned_data: Some(interned_data),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.sequence_id),
            ),
            sequence_flags: Some(2), // SEQ_NEEDS_INCREMENTAL_STATE
            ..Default::default()
        };
        self.write_packet(&packet);
    }

    fn timestamp_ns(ts: SystemTime) -> u64 {
        ts.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
    }

    fn field_to_debug_annotation(&mut self, key: &str, value: &FieldValue) -> DebugAnnotation {
        let name_iid = self.annotation_names.intern(key);
        let dbg_value = match value {
            FieldValue::Bool(b) => Some(DbgValue::BoolValue(*b)),
            FieldValue::I64(i) => Some(DbgValue::IntValue(*i)),
            FieldValue::U64(u) => Some(DbgValue::IntValue(*u as i64)),
            FieldValue::F64(f) => Some(DbgValue::DoubleValue(*f)),
            FieldValue::Str(s) => Some(DbgValue::StringValue(s.clone())),
            FieldValue::Debug(d) => Some(DbgValue::StringValue(d.clone())),
        };

        DebugAnnotation {
            name_field: Some(NameField::NameIid(name_iid)),
            value: dbg_value,
            ..Default::default()
        }
    }

    fn write_slice_begin(
        &mut self,
        track: u64,
        timestamp: SystemTime,
        name: &str,
        fields: &TraceFields,
        file: Option<&str>,
        line: Option<u32>,
    ) {
        self.flush_interned_data();

        let name_iid = self.event_names.intern(name);

        let mut debug_annotations = Vec::new();
        for (key, value) in fields {
            debug_annotations.push(self.field_to_debug_annotation(key, value));
        }
        if let Some(f) = file {
            debug_annotations
                .push(self.field_to_debug_annotation("file", &FieldValue::Str(f.to_string())));
        }
        if let Some(l) = line {
            debug_annotations
                .push(self.field_to_debug_annotation("line", &FieldValue::U64(l as u64)));
        }

        self.flush_interned_data();

        let packet = TracePacket {
            timestamp: Some(Self::timestamp_ns(timestamp)),
            data: Some(Data::TrackEvent(TrackEvent {
                track_uuid: Some(track),
                r#type: Some(TrackEventType::SliceBegin as i32),
                name_field: Some(EventNameField::NameIid(name_iid)),
                debug_annotations,
                ..Default::default()
            })),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.sequence_id),
            ),
            sequence_flags: Some(2),
            ..Default::default()
        };
        self.write_packet(&packet);
    }

    fn write_slice_end(&mut self, track: u64, timestamp: SystemTime) {
        let packet = TracePacket {
            timestamp: Some(Self::timestamp_ns(timestamp)),
            data: Some(Data::TrackEvent(TrackEvent {
                track_uuid: Some(track),
                r#type: Some(TrackEventType::SliceEnd as i32),
                ..Default::default()
            })),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.sequence_id),
            ),
            sequence_flags: Some(2),
            ..Default::default()
        };
        self.write_packet(&packet);
    }

    fn write_instant(
        &mut self,
        track: u64,
        timestamp: SystemTime,
        name: &str,
        fields: &TraceFields,
    ) {
        self.flush_interned_data();

        let name_iid = self.event_names.intern(name);

        let mut debug_annotations = Vec::new();
        for (key, value) in fields {
            debug_annotations.push(self.field_to_debug_annotation(key, value));
        }

        self.flush_interned_data();

        let packet = TracePacket {
            timestamp: Some(Self::timestamp_ns(timestamp)),
            data: Some(Data::TrackEvent(TrackEvent {
                track_uuid: Some(track),
                r#type: Some(TrackEventType::Instant as i32),
                name_field: Some(EventNameField::NameIid(name_iid)),
                debug_annotations,
                ..Default::default()
            })),
            optional_trusted_packet_sequence_id: Some(
                OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(self.sequence_id),
            ),
            sequence_flags: Some(2),
            ..Default::default()
        };
        self.write_packet(&packet);
    }
}

impl TraceEventSink for PerfettoFileSink {
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        match event {
            TraceEvent::NewSpan {
                id,
                name,
                target,
                fields,
                file,
                line,
                ..
            } => {
                if !self.trace_mode.should_include(target) {
                    return Ok(());
                }

                // In user mode, prefer the "name" field if present for display
                // In dev mode, use the fully qualified name
                let display_name = if self.trace_mode == PerfettoTraceMode::User {
                    if let Some(FieldValue::Str(n)) = get_field(fields, "name") {
                        n.clone()
                    } else {
                        name.to_string()
                    }
                } else {
                    format!("{}::{}", target, name)
                };

                self.span_info.insert(
                    *id,
                    SpanInfo {
                        fq_name: display_name,
                        fields: fields.clone(),
                        file: *file,
                        line: *line,
                    },
                );
            }

            TraceEvent::SpanEnter {
                id,
                timestamp,
                thread_name,
            } => {
                if let Some(info) = self.span_info.get(id) {
                    let fq_name = info.fq_name.clone();
                    let fields = info.fields.clone();
                    let file = info.file;
                    let line = info.line;
                    let track = self.get_or_create_thread_track(thread_name);
                    self.write_slice_begin(track, *timestamp, &fq_name, &fields, file, line);
                }
            }

            TraceEvent::SpanExit {
                id,
                timestamp,
                thread_name,
            } => {
                if self.span_info.contains_key(id) {
                    let track = self.get_or_create_thread_track(thread_name);
                    self.write_slice_end(track, *timestamp);
                }
            }

            TraceEvent::SpanClose { id, .. } => {
                self.span_info.remove(id);
            }

            TraceEvent::Event {
                name,
                target,
                fields,
                timestamp,
                thread_name,
                ..
            } => {
                if !self.trace_mode.should_include(target) {
                    return Ok(());
                }

                // In user mode, prefer the "message" field if present for display
                // In dev mode, use the fully qualified name
                let display_name = if self.trace_mode == PerfettoTraceMode::User {
                    if let Some(FieldValue::Str(msg)) = get_field(fields, "message") {
                        msg.clone()
                    } else {
                        name.to_string()
                    }
                } else {
                    format!("{}::{}", target, name)
                };

                let track = self.get_or_create_thread_track(thread_name);
                self.write_instant(track, *timestamp, &display_name, fields);
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        self.flush_interned_data();
        self.write_pending_packets()?;
        self.writer.flush()?;
        Ok(())
    }

    fn name(&self) -> &str {
        "PerfettoFileSink"
    }

    fn target_filter(&self) -> Option<&Targets> {
        Some(&self.target_filter)
    }
}
