/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared telemetry table schemas and Arrow IPC helpers.

use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use monarch_record_batch::RecordBatchRow;

/// Convert pre-translated fields to a stable JSON object string.
pub fn fields_to_json<'a>(
    fields: impl IntoIterator<Item = (&'a str, serde_json::Value)>,
) -> String {
    serde_json::Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
    .to_string()
}

/// Serialise a single Arrow record batch as an IPC stream.
pub fn serialize_batch(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Trace table row schemas.
pub mod trace_tables {
    use super::*;

    /// Table containing span creation events.
    pub const SPANS: &str = "spans";
    /// Table containing span enter, exit, and close events.
    pub const SPAN_EVENTS: &str = "span_events";
    /// Table containing tracing log events.
    pub const EVENTS: &str = "events";

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

    /// Row data for the span events table.
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
}

/// Entity table row schemas.
pub mod entity_tables {
    use super::*;

    /// Table containing actor creation events.
    pub const ACTORS: &str = "actors";
    /// Table containing mesh creation events.
    pub const MESHES: &str = "meshes";
    /// Table containing actor status transitions.
    pub const ACTOR_STATUS_EVENTS: &str = "actor_status_events";
    /// Table containing sent-message events.
    pub const SENT_MESSAGES: &str = "sent_messages";
    /// Table containing received-message events.
    pub const MESSAGES: &str = "messages";
    /// Table containing received-message status transitions.
    pub const MESSAGE_STATUS_EVENTS: &str = "message_status_events";

    /// Row data for the actors table.
    #[derive(RecordBatchRow)]
    pub struct Actor {
        pub id: u64,
        pub timestamp_us: i64,
        pub mesh_id: u64,
        pub rank: u64,
        pub full_name: String,
        pub display_name: Option<String>,
    }

    /// Row data for the meshes table.
    #[derive(RecordBatchRow)]
    pub struct Mesh {
        pub id: u64,
        pub timestamp_us: i64,
        pub class: String,
        pub given_name: String,
        pub full_name: String,
        pub shape_json: String,
        pub parent_mesh_id: Option<u64>,
        pub parent_view_json: Option<String>,
    }

    /// Row data for the actor status events table.
    #[derive(RecordBatchRow)]
    pub struct ActorStatusEvent {
        pub id: u64,
        pub timestamp_us: i64,
        pub actor_id: u64,
        pub new_status: String,
        pub reason: Option<String>,
    }

    /// Row data for the sent messages table.
    #[derive(RecordBatchRow)]
    pub struct SentMessage {
        pub id: u64,
        pub timestamp_us: i64,
        pub sender_actor_id: u64,
        pub actor_mesh_id: u64,
        pub view_json: String,
        pub shape_json: String,
    }

    /// Row data for the messages table.
    #[derive(RecordBatchRow)]
    pub struct Message {
        pub id: u64,
        pub timestamp_us: i64,
        pub from_actor_id: u64,
        pub to_actor_id: u64,
        pub endpoint: Option<String>,
        pub port_id: Option<u64>,
    }

    /// Row data for the message status events table.
    #[derive(RecordBatchRow)]
    pub struct MessageStatusEvent {
        pub id: u64,
        pub timestamp_us: i64,
        pub message_id: u64,
        pub status: String,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::ipc::reader::StreamReader;
    use monarch_record_batch::RecordBatchBuffer;
    use serde_json::json;

    use super::*;
    use crate::trace_tables::Span;
    use crate::trace_tables::SpanBuffer;

    #[test]
    fn fields_to_json_preserves_field_values() {
        let encoded = fields_to_json([
            ("count", json!(3)),
            ("enabled", json!(true)),
            ("name", json!("worker")),
        ]);

        let decoded: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded["count"], json!(3));
        assert_eq!(decoded["enabled"], json!(true));
        assert_eq!(decoded["name"], json!("worker"));
    }

    #[test]
    fn serialize_batch_round_trips_one_record_batch() {
        let mut buffer = SpanBuffer::default();
        buffer.insert(Span {
            id: 7,
            name: "span".to_string(),
            target: "target".to_string(),
            level: "INFO".to_string(),
            fields_json: "{}".to_string(),
            timestamp_us: 123,
            parent_id: None,
            thread_name: "main".to_string(),
            file: Some("lib.rs".to_string()),
            line: Some(42),
        });

        let data = serialize_batch(&buffer.drain_to_record_batch().unwrap()).unwrap();
        let mut reader = StreamReader::try_new(Cursor::new(data), None).unwrap();
        let batch = reader.next().transpose().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 7);
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "span");
        assert!(reader.next().transpose().unwrap().is_none());
    }
}
