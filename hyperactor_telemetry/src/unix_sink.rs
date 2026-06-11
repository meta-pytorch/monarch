/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Producer-side Unix-socket sink for telemetry sidecars.
//!
//! This module is the producer-side transport for telemetry. It installs
//! an inactive process-global `UnixSocketSink`, buffers trace and entity events into
//! schema-specific `RecordBatchBuffer`s, and, once activated with a socket
//! path, forwards flushed batches to the sidecar over a Unix socket.
//!
//! The dispatcher thread only does cheap row buffering and bounded channel
//! sends. Arrow IPC serialization and socket I/O run on a dedicated writer
//! thread so slow or missing sidecars do not block application tracing.
//!
//! Frame layout matches `socket_ingest.rs`: table-name length, UTF-8 table
//! name, Arrow IPC payload length, and one non-empty `RecordBatch` payload.
//! Frames are lossy by design: if the queue is full, serialization fails, or a
//! socket write fails, we increment `dropped` and keep the tracing path moving.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::SystemTime;

use monarch_record_batch::RecordBatchBuffer;
use monarch_telemetry_schema::MAX_FRAME_LEN;
use monarch_telemetry_schema::entity_tables::ACTOR_STATUS_EVENTS;
use monarch_telemetry_schema::entity_tables::ACTORS;
use monarch_telemetry_schema::entity_tables::Actor;
use monarch_telemetry_schema::entity_tables::ActorBuffer;
use monarch_telemetry_schema::entity_tables::ActorStatusEvent as ActorStatusEventRow;
use monarch_telemetry_schema::entity_tables::ActorStatusEventBuffer;
use monarch_telemetry_schema::entity_tables::MESHES;
use monarch_telemetry_schema::entity_tables::MESSAGE_STATUS_EVENTS;
use monarch_telemetry_schema::entity_tables::MESSAGES;
use monarch_telemetry_schema::entity_tables::Mesh;
use monarch_telemetry_schema::entity_tables::MeshBuffer;
use monarch_telemetry_schema::entity_tables::Message;
use monarch_telemetry_schema::entity_tables::MessageBuffer;
use monarch_telemetry_schema::entity_tables::MessageStatusEvent as MessageStatusEventRow;
use monarch_telemetry_schema::entity_tables::MessageStatusEventBuffer;
use monarch_telemetry_schema::entity_tables::SENT_MESSAGES;
use monarch_telemetry_schema::entity_tables::SentMessage;
use monarch_telemetry_schema::entity_tables::SentMessageBuffer;
use monarch_telemetry_schema::trace_tables::EVENTS;
use monarch_telemetry_schema::trace_tables::Event;
use monarch_telemetry_schema::trace_tables::EventBuffer;
use monarch_telemetry_schema::trace_tables::SPAN_EVENTS;
use monarch_telemetry_schema::trace_tables::SPANS;
use monarch_telemetry_schema::trace_tables::Span;
use monarch_telemetry_schema::trace_tables::SpanBuffer;
use monarch_telemetry_schema::trace_tables::SpanEvent;
use monarch_telemetry_schema::trace_tables::SpanEventBuffer;
use monarch_telemetry_schema::write_frame_header;
use tracing_subscriber::filter::Targets;

use crate::EntityEvent;
use crate::FieldValue;
use crate::TraceEvent;
use crate::TraceEventSink;
use crate::config::get_tracing_targets;
use crate::generate_sent_message_id;

/// Maximum queued table batches before the tracing path starts dropping frames.
const WORKER_QUEUE_CAPACITY: usize = 10_000;

/// Process-global sink installed with the tracing subscriber and activated later.
static UNIX_SOCKET_SINK: OnceLock<Arc<UnixSocketSink>> = OnceLock::new();

/// Producer-side sink that forwards telemetry batches to a sidecar socket.
pub struct UnixSocketSink {
    // Shared between the dispatcher adapter and the global activation handle.
    // The mutex covers buffered rows plus the path/worker activation state.
    inner: Mutex<UnixSocketSinkInner>,
    // The writer thread increments this for asynchronous serialization and
    // socket failures, so callers can observe producer-side frame loss.
    dropped: Arc<AtomicU64>,
}

struct UnixSocketSinkInner {
    spans_buffer: SpanBuffer,
    span_events_buffer: SpanEventBuffer,
    events_buffer: EventBuffer,
    actors_buffer: ActorBuffer,
    meshes_buffer: MeshBuffer,
    actor_status_events_buffer: ActorStatusEventBuffer,
    sent_messages_buffer: SentMessageBuffer,
    messages_buffer: MessageBuffer,
    message_status_events_buffer: MessageStatusEventBuffer,
    path: Option<PathBuf>,
    worker: Option<WorkerHandle>,
}

struct WorkerHandle {
    sender: mpsc::SyncSender<TelemetryTableBuffer>,
    _join_handle: std::thread::JoinHandle<()>,
}

enum TelemetryTableBuffer {
    Spans(SpanBuffer),
    SpanEvents(SpanEventBuffer),
    Events(EventBuffer),
    Actors(ActorBuffer),
    Meshes(MeshBuffer),
    ActorStatusEvents(ActorStatusEventBuffer),
    SentMessages(SentMessageBuffer),
    Messages(MessageBuffer),
    MessageStatusEvents(MessageStatusEventBuffer),
}

struct UnixSocketSinkAdapter {
    // The dispatcher owns this adapter, while `UNIX_SOCKET_SINK` keeps another
    // handle so callers can supply the socket path after logging initialization.
    sink: Arc<UnixSocketSink>,
    target_filter: Targets,
}

impl UnixSocketSink {
    /// Create a sink with no socket path and no writer thread.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(UnixSocketSinkInner {
                spans_buffer: SpanBuffer::default(),
                span_events_buffer: SpanEventBuffer::default(),
                events_buffer: EventBuffer::default(),
                actors_buffer: ActorBuffer::default(),
                meshes_buffer: MeshBuffer::default(),
                actor_status_events_buffer: ActorStatusEventBuffer::default(),
                sent_messages_buffer: SentMessageBuffer::default(),
                messages_buffer: MessageBuffer::default(),
                message_status_events_buffer: MessageStatusEventBuffer::default(),
                path: None,
                worker: None,
            }),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Activate the sink and lazily spawn a writer thread.
    ///
    /// Reapplying the same path is a no-op; changing paths is rejected because
    /// the process-global sink should not be retargeted after activation.
    pub fn set_path(&self, path: PathBuf) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;

        if inner.path.as_ref() == Some(&path) {
            return Ok(());
        }
        if let Some(existing) = &inner.path {
            anyhow::bail!(
                "unix socket sink already activated with {}",
                existing.display()
            );
        }

        // The tracing subscriber is installed before the sidecar path is
        // available. Start the writer only after activation so the inactive
        // sink is cheap and does not repeatedly try to connect to nowhere.
        let (sender, receiver) = mpsc::sync_channel(WORKER_QUEUE_CAPACITY);
        let dropped = Arc::clone(&self.dropped);
        let writer_path = path.clone();
        let join_handle = std::thread::Builder::new()
            .name("monarch-telemetry-unix-sink".into())
            .spawn(move || writer_loop(writer_path, receiver, dropped))
            .map_err(|error| anyhow::anyhow!("failed to spawn unix socket sink: {error}"))?;

        inner.path = Some(path);
        inner.worker = Some(WorkerHandle {
            sender,
            _join_handle: join_handle,
        });
        Ok(())
    }

    /// Return whether this sink has an active socket path.
    pub fn is_active(&self) -> bool {
        self.inner
            .lock()
            .map(|inner| inner.worker.is_some())
            .unwrap_or(false)
    }

    /// Return the cumulative number of dropped socket frames.
    pub fn dropped_frames(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Convert one dispatcher event into its table row and buffer it locally.
    fn consume_shared(&self, event: &TraceEvent) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;

        match event {
            TraceEvent::NewSpan {
                id,
                name,
                target,
                level,
                fields,
                timestamp,
                parent_id,
                thread_name,
                file,
                line,
            } => {
                inner.spans_buffer.insert(Span {
                    id: *id,
                    name: name.to_string(),
                    target: target.to_string(),
                    level: level.to_string(),
                    fields_json: fields_to_json(fields),
                    timestamp_us: timestamp_to_micros(timestamp),
                    parent_id: *parent_id,
                    thread_name: thread_name.to_string(),
                    file: file.map(|s| s.to_string()),
                    line: *line,
                });
            }
            TraceEvent::SpanEnter { id, timestamp, .. } => {
                inner.span_events_buffer.insert(SpanEvent {
                    id: *id,
                    timestamp_us: timestamp_to_micros(timestamp),
                    event_type: "enter".to_string(),
                });
            }
            TraceEvent::SpanExit { id, timestamp, .. } => {
                inner.span_events_buffer.insert(SpanEvent {
                    id: *id,
                    timestamp_us: timestamp_to_micros(timestamp),
                    event_type: "exit".to_string(),
                });
            }
            TraceEvent::SpanClose { id, timestamp } => {
                inner.span_events_buffer.insert(SpanEvent {
                    id: *id,
                    timestamp_us: timestamp_to_micros(timestamp),
                    event_type: "close".to_string(),
                });
            }
            TraceEvent::Event {
                name,
                target,
                level,
                fields,
                timestamp,
                parent_span,
                thread_id,
                thread_name,
                module_path,
                file,
                line,
            } => {
                inner.events_buffer.insert(Event {
                    name: name.to_string(),
                    target: target.to_string(),
                    level: level.to_string(),
                    fields_json: fields_to_json(fields),
                    timestamp_us: timestamp_to_micros(timestamp),
                    parent_span: *parent_span,
                    thread_id: thread_id.to_string(),
                    thread_name: thread_name.to_string(),
                    module_path: module_path.map(|s| s.to_string()),
                    file: file.map(|s| s.to_string()),
                    line: *line,
                });
            }
            TraceEvent::Entity(event) => buffer_entity_event(&mut inner, event),
        }
        Ok(())
    }

    /// Move non-empty table buffers to the writer queue, or drop them while inactive.
    fn flush_shared(&self) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;

        if inner.worker.is_none() {
            // The buffers provide only a small pre-activation window: rows
            // consumed before `set_path` can be sent if activation wins the
            // race with the next dispatcher flush. Otherwise, drop them here
            // so startup telemetry does not accumulate without a socket path.
            inner.spans_buffer = SpanBuffer::default();
            inner.span_events_buffer = SpanEventBuffer::default();
            inner.events_buffer = EventBuffer::default();
            inner.actors_buffer = ActorBuffer::default();
            inner.meshes_buffer = MeshBuffer::default();
            inner.actor_status_events_buffer = ActorStatusEventBuffer::default();
            inner.sent_messages_buffer = SentMessageBuffer::default();
            inner.messages_buffer = MessageBuffer::default();
            inner.message_status_events_buffer = MessageStatusEventBuffer::default();
            return Ok(());
        }

        let sender = inner
            .worker
            .as_ref()
            .expect("worker should exist")
            .sender
            .clone();

        flush_buffer(
            &mut inner.spans_buffer,
            TelemetryTableBuffer::Spans,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.span_events_buffer,
            TelemetryTableBuffer::SpanEvents,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.events_buffer,
            TelemetryTableBuffer::Events,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.actors_buffer,
            TelemetryTableBuffer::Actors,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.meshes_buffer,
            TelemetryTableBuffer::Meshes,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.actor_status_events_buffer,
            TelemetryTableBuffer::ActorStatusEvents,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.sent_messages_buffer,
            TelemetryTableBuffer::SentMessages,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.messages_buffer,
            TelemetryTableBuffer::Messages,
            &sender,
            &self.dropped,
        );
        flush_buffer(
            &mut inner.message_status_events_buffer,
            TelemetryTableBuffer::MessageStatusEvents,
            &sender,
            &self.dropped,
        );
        Ok(())
    }
}

impl TraceEventSink for UnixSocketSinkAdapter {
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        self.sink.consume_shared(event)
    }

    fn target_filter(&self) -> Option<&Targets> {
        Some(&self.target_filter)
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        self.sink.flush_shared()
    }

    fn name(&self) -> &str {
        "UnixSocketSink"
    }
}

/// Install the inactive process-global Unix socket sink.
pub(crate) fn install_unix_socket_sink_inactive() -> Box<dyn TraceEventSink> {
    let sink = Arc::new(UnixSocketSink::new());
    let _ = UNIX_SOCKET_SINK.set(Arc::clone(&sink));
    Box::new(UnixSocketSinkAdapter {
        sink,
        target_filter: get_tracing_targets(),
    })
}

/// Activate the process-global Unix socket sink against a socket path.
pub fn set_unix_socket_sink_path(path: impl Into<PathBuf>) -> anyhow::Result<()> {
    let sink = UNIX_SOCKET_SINK
        .get()
        .ok_or_else(|| anyhow::anyhow!("unix socket sink is not installed"))?;
    sink.set_path(path.into())
}

/// Return whether the process-global Unix socket sink is active.
pub fn unix_socket_sink_is_active() -> bool {
    UNIX_SOCKET_SINK
        .get()
        .map(|sink| sink.is_active())
        .unwrap_or(false)
}

/// Return the process-global Unix socket sink's cumulative dropped frames.
pub fn unix_socket_sink_dropped_frames() -> Option<u64> {
    UNIX_SOCKET_SINK.get().map(|sink| sink.dropped_frames())
}

fn flush_buffer<B>(
    buffer: &mut B,
    table_buffer: impl FnOnce(B) -> TelemetryTableBuffer,
    sender: &mpsc::SyncSender<TelemetryTableBuffer>,
    dropped: &AtomicU64,
) where
    B: RecordBatchBuffer + Default,
{
    if buffer.is_empty() {
        return;
    }

    let batch = table_buffer(std::mem::take(buffer));
    match sender.try_send(batch) {
        Ok(()) => {}
        Err(mpsc::TrySendError::Full(_)) | Err(mpsc::TrySendError::Disconnected(_)) => {
            // Socket delivery must never backpressure the tracing dispatcher.
            // Drop the whole table batch when the worker cannot accept it.
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn buffer_entity_event(inner: &mut UnixSocketSinkInner, event: &EntityEvent) {
    match event {
        EntityEvent::Actor(event) => {
            inner.actors_buffer.insert(Actor {
                id: event.id,
                timestamp_us: timestamp_to_micros(&event.timestamp),
                mesh_id: event.mesh_id,
                rank: event.rank,
                full_name: event.full_name.clone(),
                display_name: event.display_name.clone(),
            });
        }
        EntityEvent::Mesh(event) => {
            inner.meshes_buffer.insert(Mesh {
                id: event.id,
                timestamp_us: timestamp_to_micros(&event.timestamp),
                class: event.class.clone(),
                given_name: event.given_name.clone(),
                full_name: event.full_name.clone(),
                shape_json: event.shape_json.clone(),
                parent_mesh_id: event.parent_mesh_id,
                parent_view_json: event.parent_view_json.clone(),
            });
        }
        EntityEvent::ActorStatus(event) => {
            inner
                .actor_status_events_buffer
                .insert(ActorStatusEventRow {
                    id: event.id,
                    timestamp_us: timestamp_to_micros(&event.timestamp),
                    actor_id: event.actor_id,
                    new_status: event.new_status.clone(),
                    reason: event.reason.clone(),
                });
        }
        EntityEvent::SentMessage(event) => {
            inner.sent_messages_buffer.insert(SentMessage {
                id: generate_sent_message_id(event.sender_actor_id),
                timestamp_us: timestamp_to_micros(&event.timestamp),
                sender_actor_id: event.sender_actor_id,
                actor_mesh_id: event.actor_mesh_id,
                view_json: event.view_json.clone(),
                shape_json: event.shape_json.clone(),
            });
        }
        EntityEvent::Message(event) => {
            inner.messages_buffer.insert(Message {
                id: event.id,
                timestamp_us: timestamp_to_micros(&event.timestamp),
                from_actor_id: event.from_actor_id,
                to_actor_id: event.to_actor_id,
                endpoint: event.endpoint.clone(),
                port_index: event.port_index,
            });
        }
        EntityEvent::MessageStatus(event) => {
            inner
                .message_status_events_buffer
                .insert(MessageStatusEventRow {
                    id: event.id,
                    timestamp_us: timestamp_to_micros(&event.timestamp),
                    message_id: event.message_id,
                    status: event.status.clone(),
                });
        }
    }
}

/// Serialize drained table buffers and write framed Arrow IPC payloads to the sidecar.
fn writer_loop(
    path: PathBuf,
    receiver: mpsc::Receiver<TelemetryTableBuffer>,
    dropped: Arc<AtomicU64>,
) {
    let mut stream = None;

    while let Ok(mut buffer) = receiver.recv() {
        // Convert buffered rows into the same one-table, one-batch frame shape
        // that the ingest server validates. Failures here are producer-side
        // frame drops; the writer keeps processing later batches.
        //
        // Keep the table-name selection next to the concrete buffer variant so
        // adding a socket table requires an explicit writer-loop update.
        let (table_name, batch) = match &mut buffer {
            TelemetryTableBuffer::Spans(buffer) => (SPANS, buffer.drain_to_record_batch()),
            TelemetryTableBuffer::SpanEvents(buffer) => {
                (SPAN_EVENTS, buffer.drain_to_record_batch())
            }
            TelemetryTableBuffer::Events(buffer) => (EVENTS, buffer.drain_to_record_batch()),
            TelemetryTableBuffer::Actors(buffer) => (ACTORS, buffer.drain_to_record_batch()),
            TelemetryTableBuffer::Meshes(buffer) => (MESHES, buffer.drain_to_record_batch()),
            TelemetryTableBuffer::ActorStatusEvents(buffer) => {
                (ACTOR_STATUS_EVENTS, buffer.drain_to_record_batch())
            }
            TelemetryTableBuffer::SentMessages(buffer) => {
                (SENT_MESSAGES, buffer.drain_to_record_batch())
            }
            TelemetryTableBuffer::Messages(buffer) => (MESSAGES, buffer.drain_to_record_batch()),
            TelemetryTableBuffer::MessageStatusEvents(buffer) => {
                (MESSAGE_STATUS_EVENTS, buffer.drain_to_record_batch())
            }
        };
        let Ok(batch) = batch else {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if batch.num_rows() == 0 {
            continue;
        }

        let Ok(payload) = monarch_telemetry_schema::serialize_batch(&batch) else {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if payload.len() > MAX_FRAME_LEN {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        // Exact frame size: u16 table-name length, table-name bytes, u32
        // payload length, then the Arrow IPC payload bytes.
        let mut frame = Vec::with_capacity(2 + table_name.len() + 4 + payload.len());
        if write_frame_header(&mut frame, table_name, payload.len()).is_err() {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        frame.extend_from_slice(&payload);

        if write_frame(&path, &mut stream, &frame).is_err() {
            dropped.fetch_add(1, Ordering::Relaxed);
            stream = None;
        }
    }
}

fn write_frame(
    path: &PathBuf,
    stream: &mut Option<UnixStream>,
    frame: &[u8],
) -> std::io::Result<()> {
    if stream.is_none() {
        *stream = Some(UnixStream::connect(path)?);
    }

    // Reuse a connected stream across frames. On any write error, discard the
    // stream so the next frame attempts a fresh connect to a restarted sidecar.
    match stream
        .as_mut()
        .expect("stream should exist")
        .write_all(frame)
    {
        Ok(()) => Ok(()),
        Err(error) => {
            *stream = None;
            Err(error)
        }
    }
}

fn fields_to_json(fields: &[(&str, FieldValue)]) -> String {
    monarch_telemetry_schema::fields_to_json(fields.iter().map(|(key, value)| {
        let json_value = match value {
            FieldValue::Bool(b) => serde_json::Value::Bool(*b),
            FieldValue::I64(i) => serde_json::Value::Number((*i).into()),
            FieldValue::U64(u) => serde_json::Value::Number((*u).into()),
            FieldValue::F64(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            FieldValue::Str(s) => serde_json::Value::String(s.clone()),
            FieldValue::Debug(d) => serde_json::Value::String(d.clone()),
        };
        (*key, json_value)
    }))
}

fn timestamp_to_micros(timestamp: &SystemTime) -> i64 {
    timestamp
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

#[cfg(test)]
pub(crate) fn adapter_for_test(sink: Arc<UnixSocketSink>) -> Box<dyn TraceEventSink> {
    Box::new(UnixSocketSinkAdapter {
        sink,
        target_filter: get_tracing_targets(),
    })
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::io::Read;
    use std::os::unix::net::UnixListener;
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::SystemTime;

    use monarch_record_batch::RecordBatchBuffer;

    use super::*;
    use crate::ActorEvent;
    use crate::ActorStatusEvent;
    use crate::MeshEvent;
    use crate::MessageEvent;
    use crate::MessageStatusEvent;
    use crate::SentMessageEvent;

    static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    struct Frame {
        table_name: String,
        payload: Vec<u8>,
    }

    fn socket_path(name: &str) -> PathBuf {
        let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "monarch_unix_socket_sink_{}_{}",
            std::process::id(),
            seq
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn timestamp() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_micros(123)
    }

    fn fields() -> crate::trace_dispatcher::TraceFields {
        let mut fields = crate::trace_dispatcher::TraceFields::new();
        fields.push(("count", FieldValue::U64(3)));
        fields
    }

    fn span() -> TraceEvent {
        TraceEvent::NewSpan {
            id: 7,
            name: "test_span",
            target: "test_target",
            level: tracing::Level::INFO,
            fields: fields(),
            timestamp: timestamp(),
            parent_id: None,
            thread_name: "test_thread",
            file: Some("test.rs"),
            line: Some(42),
        }
    }

    fn span_enter() -> TraceEvent {
        TraceEvent::SpanEnter {
            id: 7,
            timestamp: timestamp(),
            thread_name: "test_thread",
        }
    }

    fn event() -> TraceEvent {
        TraceEvent::Event {
            name: "test_event",
            target: "test_target",
            level: tracing::Level::INFO,
            fields: fields(),
            timestamp: timestamp(),
            parent_span: Some(7),
            thread_id: "99",
            thread_name: "test_thread",
            module_path: Some("test_module"),
            file: Some("test.rs"),
            line: Some(43),
        }
    }

    fn entity_events() -> Vec<TraceEvent> {
        vec![
            TraceEvent::Entity(EntityEvent::Actor(ActorEvent {
                id: 1,
                timestamp: timestamp(),
                mesh_id: 2,
                rank: 3,
                full_name: "actor/full".to_string(),
                display_name: Some("actor".to_string()),
            })),
            TraceEvent::Entity(EntityEvent::Mesh(MeshEvent {
                id: 2,
                timestamp: timestamp(),
                class: "Host".to_string(),
                given_name: "hosts".to_string(),
                full_name: "hosts/full".to_string(),
                shape_json: r#"{"dims":[1]}"#.to_string(),
                parent_mesh_id: None,
                parent_view_json: None,
            })),
            TraceEvent::Entity(EntityEvent::ActorStatus(ActorStatusEvent {
                id: 3,
                timestamp: timestamp(),
                actor_id: 1,
                new_status: "Running".to_string(),
                reason: Some("test".to_string()),
            })),
            TraceEvent::Entity(EntityEvent::SentMessage(SentMessageEvent {
                timestamp: timestamp(),
                sender_actor_id: 1,
                actor_mesh_id: 2,
                view_json: r#"{"rank":0}"#.to_string(),
                shape_json: r#"{"dims":[1]}"#.to_string(),
            })),
            TraceEvent::Entity(EntityEvent::Message(MessageEvent {
                timestamp: timestamp(),
                id: 4,
                from_actor_id: 1,
                to_actor_id: 5,
                endpoint: Some("endpoint".to_string()),
                port_index: Some(6),
            })),
            TraceEvent::Entity(EntityEvent::MessageStatus(MessageStatusEvent {
                timestamp: timestamp(),
                id: 7,
                message_id: 4,
                status: "complete".to_string(),
            })),
        ]
    }

    fn read_frame(stream: &mut UnixStream) -> Frame {
        let mut name_len_bytes = [0; 2];
        stream.read_exact(&mut name_len_bytes).unwrap();
        let name_len = u16::from_be_bytes(name_len_bytes) as usize;

        let mut name_bytes = vec![0; name_len];
        stream.read_exact(&mut name_bytes).unwrap();
        let table_name = String::from_utf8(name_bytes).unwrap();

        let mut payload_len_bytes = [0; 4];
        stream.read_exact(&mut payload_len_bytes).unwrap();
        let payload_len = u32::from_be_bytes(payload_len_bytes) as usize;
        assert!((1..=MAX_FRAME_LEN).contains(&payload_len));

        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).unwrap();
        Frame {
            table_name,
            payload,
        }
    }

    fn read_frame_tables(stream: UnixStream, count: usize) -> Vec<String> {
        read_frames(stream, count)
            .into_iter()
            .map(|frame| frame.table_name)
            .collect()
    }

    fn read_frames(mut stream: UnixStream, count: usize) -> Vec<Frame> {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        (0..count).map(|_| read_frame(&mut stream)).collect()
    }

    fn assert_no_frame_available(stream: &mut UnixStream) {
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut name_len_bytes = [0; 2];
        match stream.read_exact(&mut name_len_bytes) {
            Ok(()) => panic!("unexpected extra telemetry frame"),
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => panic!("unexpected read error: {error}"),
        }
    }

    fn wait_for_dropped_frames(sink: &UnixSocketSink, expected: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if sink.dropped_frames() == expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(sink.dropped_frames(), expected);
    }

    fn assert_frame_batch<B>(frame: &Frame)
    where
        B: RecordBatchBuffer + Default,
    {
        let batch = monarch_telemetry_schema::deserialize_one_batch(&frame.payload).unwrap();
        let expected = B::default().drain_to_record_batch().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), expected.schema());
    }

    fn assert_entity_frame_schema(frame: &Frame) {
        match frame.table_name.as_str() {
            ACTORS => assert_frame_batch::<ActorBuffer>(frame),
            MESHES => assert_frame_batch::<MeshBuffer>(frame),
            ACTOR_STATUS_EVENTS => assert_frame_batch::<ActorStatusEventBuffer>(frame),
            SENT_MESSAGES => assert_frame_batch::<SentMessageBuffer>(frame),
            MESSAGES => assert_frame_batch::<MessageBuffer>(frame),
            MESSAGE_STATUS_EVENTS => assert_frame_batch::<MessageStatusEventBuffer>(frame),
            other => panic!("unexpected entity table frame: {other}"),
        }
    }

    #[test]
    fn inactive_flush_discards_buffered_events_without_counting_drops() {
        let sink = UnixSocketSink::new();

        sink.consume_shared(&span()).unwrap();
        sink.consume_shared(&span_enter()).unwrap();
        sink.consume_shared(&event()).unwrap();
        for event in entity_events() {
            sink.consume_shared(&event).unwrap();
        }

        {
            let inner = sink.inner.lock().unwrap();
            assert_eq!(inner.spans_buffer.len(), 1);
            assert_eq!(inner.span_events_buffer.len(), 1);
            assert_eq!(inner.events_buffer.len(), 1);
            assert_eq!(inner.actors_buffer.len(), 1);
            assert_eq!(inner.meshes_buffer.len(), 1);
            assert_eq!(inner.actor_status_events_buffer.len(), 1);
            assert_eq!(inner.sent_messages_buffer.len(), 1);
            assert_eq!(inner.messages_buffer.len(), 1);
            assert_eq!(inner.message_status_events_buffer.len(), 1);
        }

        sink.flush_shared().unwrap();

        {
            let inner = sink.inner.lock().unwrap();
            assert_eq!(inner.spans_buffer.len(), 0);
            assert_eq!(inner.span_events_buffer.len(), 0);
            assert_eq!(inner.events_buffer.len(), 0);
            assert_eq!(inner.actors_buffer.len(), 0);
            assert_eq!(inner.meshes_buffer.len(), 0);
            assert_eq!(inner.actor_status_events_buffer.len(), 0);
            assert_eq!(inner.sent_messages_buffer.len(), 0);
            assert_eq!(inner.messages_buffer.len(), 0);
            assert_eq!(inner.message_status_events_buffer.len(), 0);
        }
        assert!(!sink.is_active());
        assert_eq!(sink.dropped_frames(), 0);
    }

    #[test]
    fn set_path_is_idempotent_for_same_path() {
        let path = socket_path("same.sock");
        let sink = UnixSocketSink::new();

        sink.set_path(path.clone()).unwrap();
        sink.set_path(path).unwrap();
    }

    #[test]
    fn set_path_rejects_different_path_after_activation() {
        let sink = UnixSocketSink::new();

        sink.set_path(socket_path("first.sock")).unwrap();

        assert!(sink.set_path(socket_path("second.sock")).is_err());
    }

    #[test]
    fn active_flush_sends_one_frame_per_non_empty_trace_table() {
        let path = socket_path("telemetry.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (sender, receiver) = mpsc::channel();
        let read_handle = std::thread::spawn(move || {
            let (stream, _addr) = listener.accept().unwrap();
            sender.send(read_frame_tables(stream, 3)).unwrap();
        });

        let sink = UnixSocketSink::new();
        sink.set_path(path).unwrap();
        sink.consume_shared(&span()).unwrap();
        sink.consume_shared(&span_enter()).unwrap();
        sink.consume_shared(&event()).unwrap();

        sink.flush_shared().unwrap();

        let tables = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        read_handle.join().unwrap();
        assert_eq!(
            tables,
            vec![
                SPANS.to_string(),
                SPAN_EVENTS.to_string(),
                EVENTS.to_string()
            ]
        );
        assert_eq!(sink.dropped_frames(), 0);
    }

    #[test]
    fn active_flush_sends_one_frame_per_non_empty_entity_table() {
        let path = socket_path("entities.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (sender, receiver) = mpsc::channel();
        let read_handle = std::thread::spawn(move || {
            let (stream, _addr) = listener.accept().unwrap();
            sender.send(read_frames(stream, 6)).unwrap();
        });

        let sink = UnixSocketSink::new();
        sink.set_path(path).unwrap();
        for event in entity_events() {
            sink.consume_shared(&event).unwrap();
        }

        sink.flush_shared().unwrap();

        let frames = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        read_handle.join().unwrap();
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.table_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                ACTORS,
                MESHES,
                ACTOR_STATUS_EVENTS,
                SENT_MESSAGES,
                MESSAGES,
                MESSAGE_STATUS_EVENTS,
            ]
        );
        frames.iter().for_each(assert_entity_frame_schema);
        assert_eq!(sink.dropped_frames(), 0);
    }

    #[test]
    fn active_flush_omits_empty_trace_tables() {
        let path = socket_path("events_only.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (sender, receiver) = mpsc::channel();
        let read_handle = std::thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let table_name = read_frame(&mut stream).table_name;
            assert_no_frame_available(&mut stream);
            sender.send(table_name).unwrap();
        });

        let sink = UnixSocketSink::new();
        sink.set_path(path).unwrap();
        sink.consume_shared(&event()).unwrap();

        sink.flush_shared().unwrap();

        let table = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        read_handle.join().unwrap();
        assert_eq!(table, EVENTS);
        assert_eq!(sink.dropped_frames(), 0);
    }

    #[test]
    fn flush_counts_missing_sidecar_connection_as_dropped_frame() {
        let sink = UnixSocketSink::new();
        sink.set_path(socket_path("missing.sock")).unwrap();
        sink.consume_shared(&event()).unwrap();

        sink.flush_shared().unwrap();

        wait_for_dropped_frames(&sink, 1);
    }
}
