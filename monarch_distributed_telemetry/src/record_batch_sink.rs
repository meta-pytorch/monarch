/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RecordBatchSink - TraceEventSink that produces Arrow RecordBatches
//!
//! Produces three tables:
//! - `spans`: Information about span creation (NewSpan events)
//! - `span_events`: Enter/exit/close events for spans
//! - `events`: Tracing events (e.g., tracing::info!())

use std::time::SystemTime;

use datafusion::arrow::record_batch::RecordBatch;
use hyperactor_telemetry::FieldValue;
use hyperactor_telemetry::TraceEvent;
use hyperactor_telemetry::TraceEventSink;
use record_batch_derive::RecordBatchRow;

/// Helper to convert SystemTime to microseconds since Unix epoch.
fn timestamp_to_micros(timestamp: &SystemTime) -> i64 {
    timestamp
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Helper to convert FieldValue slice to JSON string.
fn fields_to_json(fields: &[(&str, FieldValue)]) -> String {
    let mut map = serde_json::Map::new();
    for (key, value) in fields {
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
        map.insert((*key).to_string(), json_value);
    }
    serde_json::Value::Object(map).to_string()
}

/// Row data for the spans table.
#[derive(RecordBatchRow)]
pub struct Span {
    pub id: u64,
    pub name: String,
    pub target: String,
    pub level: String,
    pub fields_json: String,
    pub timestamp_us: i64,
    pub parent_id: Option<u64>,
    pub thread_name: String,
    pub file: Option<String>,
    pub line: Option<u32>,
}

/// Row data for the span_events table.
#[derive(RecordBatchRow)]
pub struct SpanEvent {
    pub id: u64,
    pub timestamp_us: i64,
    pub event_type: String,
}

/// Row data for the events table.
#[derive(RecordBatchRow)]
pub struct Event {
    pub name: String,
    pub target: String,
    pub level: String,
    pub fields_json: String,
    pub timestamp_us: i64,
    pub parent_span: Option<u64>,
    pub thread_id: String,
    pub thread_name: String,
    pub module_path: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
}

use std::sync::Arc;
use std::sync::Mutex;

/// Callback function type for flushing RecordBatches.
/// Takes ownership of the RecordBatch. The callback should handle empty batches
/// by creating the table with the schema but not appending the empty data.
pub type FlushCallback = Box<dyn Fn(&str, RecordBatch) + Send>;

/// Trait for buffer types that can produce RecordBatches.
/// Auto-implemented by the RecordBatchRow derive macro.
pub trait RecordBatchBuffer {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn to_record_batch(&mut self) -> anyhow::Result<RecordBatch>;
}

/// Inner state of RecordBatchSink.
struct RecordBatchSinkInner {
    spans_buffer: SpanBuffer,
    span_events_buffer: SpanEventBuffer,
    events_buffer: EventBuffer,
    batch_size: usize,
    flush_callback: FlushCallback,
}

impl RecordBatchSinkInner {
    fn flush_buffer<B: RecordBatchBuffer>(
        buffer: &mut B,
        table_name: &str,
        callback: &FlushCallback,
    ) -> anyhow::Result<()> {
        // Always produce a batch (even if empty) - the callback handles empty batches
        // by creating the table with the schema but not appending empty data
        let batch = buffer.to_record_batch()?;
        callback(table_name, batch);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        Self::flush_buffer(&mut self.spans_buffer, "spans", &self.flush_callback)?;
        Self::flush_buffer(
            &mut self.span_events_buffer,
            "span_events",
            &self.flush_callback,
        )?;
        Self::flush_buffer(&mut self.events_buffer, "events", &self.flush_callback)?;
        Ok(())
    }

    fn flush_spans_if_full(&mut self) -> anyhow::Result<()> {
        if self.spans_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.spans_buffer, "spans", &self.flush_callback)?;
        }
        Ok(())
    }

    fn flush_span_events_if_full(&mut self) -> anyhow::Result<()> {
        if self.span_events_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.span_events_buffer,
                "span_events",
                &self.flush_callback,
            )?;
        }
        Ok(())
    }

    fn flush_events_if_full(&mut self) -> anyhow::Result<()> {
        if self.events_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.events_buffer, "events", &self.flush_callback)?;
        }
        Ok(())
    }
}

/// A TraceEventSink that buffers trace events and produces Arrow RecordBatches.
///
/// This type can be cloned to get a handle for flushing from outside the
/// telemetry system. Clone it before registering with telemetry if you need
/// to call flush() later.
///
/// Produces three tables:
/// - `spans`: Information about span creation (NewSpan events)
/// - `span_events`: Enter/exit/close events for spans
/// - `events`: Tracing events (e.g., tracing::info!())
#[derive(Clone)]
pub struct RecordBatchSink {
    inner: Arc<Mutex<RecordBatchSinkInner>>,
}

impl RecordBatchSink {
    /// Create a new RecordBatchSink with the specified batch size and flush callback.
    ///
    /// The callback receives (table_name, record_batch) when a batch is ready.
    /// The callback should handle empty batches by creating the table with the
    /// schema but not appending the empty data.
    ///
    /// # Arguments
    /// * `batch_size` - Number of rows to buffer before flushing each table
    /// * `flush_callback` - Called with (table_name, record_batch) when a batch is ready
    pub fn new(batch_size: usize, flush_callback: FlushCallback) -> Self {
        let inner = Arc::new(Mutex::new(RecordBatchSinkInner {
            spans_buffer: SpanBuffer::default(),
            span_events_buffer: SpanEventBuffer::default(),
            events_buffer: EventBuffer::default(),
            batch_size,
            flush_callback,
        }));
        Self { inner }
    }

    /// Flush all buffers, emitting batches for all tables.
    ///
    /// This always emits batches for all three tables (spans, span_events, events),
    /// even if they are empty. The callback is expected to handle empty batches
    /// by creating the table with the correct schema but not appending empty data.
    pub fn flush(&self) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        inner.flush()
    }

    /// Create a new RecordBatchSink that prints batches to stdout.
    pub fn new_printing(batch_size: usize) -> Self {
        Self::new(
            batch_size,
            Box::new(|table_name, batch| {
                println!(
                    "[RecordBatchSink] Table: {}, rows: {}, schema: {:?}",
                    table_name,
                    batch.num_rows(),
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                );
                let schema = batch.schema();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let field = schema.field(col_idx);
                    println!("  {}: {:?}", field.name(), col);
                }
            }),
        )
    }
}

impl TraceEventSink for RecordBatchSink {
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
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
                inner.flush_spans_if_full()?;
            }
            TraceEvent::SpanEnter { id, timestamp } => {
                inner.span_events_buffer.insert(SpanEvent {
                    id: *id,
                    timestamp_us: timestamp_to_micros(timestamp),
                    event_type: "enter".to_string(),
                });
                inner.flush_span_events_if_full()?;
            }
            TraceEvent::SpanExit { id, timestamp } => {
                inner.span_events_buffer.insert(SpanEvent {
                    id: *id,
                    timestamp_us: timestamp_to_micros(timestamp),
                    event_type: "exit".to_string(),
                });
                inner.flush_span_events_if_full()?;
            }
            TraceEvent::SpanClose { id, timestamp } => {
                inner.span_events_buffer.insert(SpanEvent {
                    id: *id,
                    timestamp_us: timestamp_to_micros(timestamp),
                    event_type: "close".to_string(),
                });
                inner.flush_span_events_if_full()?;
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
                inner.flush_events_if_full()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        // Use the shared flush implementation
        RecordBatchSink::flush(self)
    }

    fn name(&self) -> &str {
        "RecordBatchSink"
    }
}
