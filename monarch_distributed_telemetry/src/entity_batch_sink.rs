/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EntityBatchSink - Collects entity lifecycle events as Arrow RecordBatches
//!
//! Produces tables:
//! - `actors`: Actor creation events
//! - `meshes`: Mesh creation events
//! - `actor_status_events`: Actor status change events
//! - `sent_messages`: Sent message events
//! - `messages`: Received message events
//! - `message_status_events`: Received message status transitions

use std::sync::Arc;
use std::sync::Mutex;

use hyperactor_telemetry::EntityEvent;
use hyperactor_telemetry::TraceEvent;
use hyperactor_telemetry::TraceEventSink;
use monarch_record_batch::RecordBatchBuffer;
use monarch_telemetry_schema::entity_tables::ACTOR_STATUS_EVENTS;
use monarch_telemetry_schema::entity_tables::ACTORS;
pub use monarch_telemetry_schema::entity_tables::Actor;
pub use monarch_telemetry_schema::entity_tables::ActorBuffer;
pub use monarch_telemetry_schema::entity_tables::ActorStatusEvent;
pub use monarch_telemetry_schema::entity_tables::ActorStatusEventBuffer;
use monarch_telemetry_schema::entity_tables::MESHES;
use monarch_telemetry_schema::entity_tables::MESSAGE_STATUS_EVENTS;
use monarch_telemetry_schema::entity_tables::MESSAGES;
pub use monarch_telemetry_schema::entity_tables::Mesh;
pub use monarch_telemetry_schema::entity_tables::MeshBuffer;
pub use monarch_telemetry_schema::entity_tables::Message;
pub use monarch_telemetry_schema::entity_tables::MessageBuffer;
pub use monarch_telemetry_schema::entity_tables::MessageStatusEvent;
pub use monarch_telemetry_schema::entity_tables::MessageStatusEventBuffer;
use monarch_telemetry_schema::entity_tables::SENT_MESSAGES;
pub use monarch_telemetry_schema::entity_tables::SentMessage;
pub use monarch_telemetry_schema::entity_tables::SentMessageBuffer;

use crate::record_batch_sink::FlushCallback;
use crate::timestamp_to_micros;

/// Inner state of EntityBatchSink.
struct EntityBatchSinkInner {
    actors_buffer: ActorBuffer,
    meshes_buffer: MeshBuffer,
    actor_status_events_buffer: ActorStatusEventBuffer,
    sent_messages_buffer: SentMessageBuffer,
    messages_buffer: MessageBuffer,
    message_status_events_buffer: MessageStatusEventBuffer,
    batch_size: usize,
    flush_callback: FlushCallback,
}

impl EntityBatchSinkInner {
    fn flush_buffer<B: RecordBatchBuffer>(
        buffer: &mut B,
        table_name: &str,
        callback: &FlushCallback,
    ) -> anyhow::Result<()> {
        let batch = buffer.drain_to_record_batch()?;
        callback(table_name, batch);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        Self::flush_buffer(&mut self.actors_buffer, ACTORS, &self.flush_callback)?;
        Self::flush_buffer(&mut self.meshes_buffer, MESHES, &self.flush_callback)?;
        Self::flush_buffer(
            &mut self.actor_status_events_buffer,
            ACTOR_STATUS_EVENTS,
            &self.flush_callback,
        )?;
        Self::flush_buffer(
            &mut self.sent_messages_buffer,
            SENT_MESSAGES,
            &self.flush_callback,
        )?;
        Self::flush_buffer(&mut self.messages_buffer, MESSAGES, &self.flush_callback)?;
        Self::flush_buffer(
            &mut self.message_status_events_buffer,
            MESSAGE_STATUS_EVENTS,
            &self.flush_callback,
        )?;
        Ok(())
    }

    fn flush_actors_if_full(&mut self) -> anyhow::Result<()> {
        if self.actors_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.actors_buffer, ACTORS, &self.flush_callback)?;
        }
        Ok(())
    }

    fn flush_meshes_if_full(&mut self) -> anyhow::Result<()> {
        if self.meshes_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.meshes_buffer, MESHES, &self.flush_callback)?;
        }
        Ok(())
    }

    fn flush_actor_status_events_if_full(&mut self) -> anyhow::Result<()> {
        if self.actor_status_events_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.actor_status_events_buffer,
                ACTOR_STATUS_EVENTS,
                &self.flush_callback,
            )?;
        }
        Ok(())
    }

    fn flush_sent_messages_if_full(&mut self) -> anyhow::Result<()> {
        if self.sent_messages_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.sent_messages_buffer,
                SENT_MESSAGES,
                &self.flush_callback,
            )?;
        }
        Ok(())
    }

    fn flush_messages_if_full(&mut self) -> anyhow::Result<()> {
        if self.messages_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.messages_buffer, MESSAGES, &self.flush_callback)?;
        }
        Ok(())
    }

    fn flush_message_status_events_if_full(&mut self) -> anyhow::Result<()> {
        if self.message_status_events_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.message_status_events_buffer,
                MESSAGE_STATUS_EVENTS,
                &self.flush_callback,
            )?;
        }
        Ok(())
    }
}

/// Collects entity lifecycle events into Arrow RecordBatches.
///
/// This is the current in-process materializer for `TraceEvent::Entity`. It
/// keeps today's `DatabaseScanner` query path working while entity production
/// moves onto the unified trace dispatcher queue.
#[derive(Clone)]
pub struct EntityBatchSink {
    inner: Arc<Mutex<EntityBatchSinkInner>>,
}

impl EntityBatchSink {
    /// Create a new EntityBatchSink with the specified batch size and flush callback.
    ///
    /// The callback receives (table_name, record_batch) when a batch is ready.
    /// The callback should handle empty batches by creating the table with the
    /// schema but not appending the empty data.
    ///
    /// # Arguments
    /// * `batch_size` - Number of rows to buffer before flushing each table
    /// * `flush_callback` - Called with (table_name, record_batch) when a batch is ready
    pub fn new(batch_size: usize, flush_callback: FlushCallback) -> Self {
        let inner = Arc::new(Mutex::new(EntityBatchSinkInner {
            actors_buffer: ActorBuffer::default(),
            meshes_buffer: MeshBuffer::default(),
            actor_status_events_buffer: ActorStatusEventBuffer::default(),
            sent_messages_buffer: SentMessageBuffer::default(),
            messages_buffer: MessageBuffer::default(),
            message_status_events_buffer: MessageStatusEventBuffer::default(),
            batch_size,
            flush_callback,
        }));
        Self { inner }
    }

    /// Flush all entity table buffers.
    ///
    /// This always emits batches for all entity tables, even if they are empty.
    /// The callback is expected to handle empty batches by creating the table
    /// with the correct schema but not appending empty data.
    pub fn flush(&self) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        inner.flush()
    }
}

impl EntityBatchSink {
    fn consume_entity(&self, event: EntityEvent) -> Result<(), anyhow::Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;

        match event {
            EntityEvent::Actor(actor_event) => {
                inner.actors_buffer.insert(Actor {
                    id: actor_event.id,
                    timestamp_us: timestamp_to_micros(&actor_event.timestamp),
                    mesh_id: actor_event.mesh_id,
                    rank: actor_event.rank,
                    full_name: actor_event.full_name,
                    display_name: actor_event.display_name,
                });
                inner.flush_actors_if_full()?;
            }
            EntityEvent::Mesh(mesh_event) => {
                inner.meshes_buffer.insert(Mesh {
                    id: mesh_event.id,
                    timestamp_us: timestamp_to_micros(&mesh_event.timestamp),
                    class: mesh_event.class,
                    given_name: mesh_event.given_name,
                    full_name: mesh_event.full_name,
                    shape_json: mesh_event.shape_json,
                    parent_mesh_id: mesh_event.parent_mesh_id,
                    parent_view_json: mesh_event.parent_view_json,
                });
                inner.flush_meshes_if_full()?;
            }
            EntityEvent::ActorStatus(status_event) => {
                inner.actor_status_events_buffer.insert(ActorStatusEvent {
                    id: status_event.id,
                    timestamp_us: timestamp_to_micros(&status_event.timestamp),
                    actor_id: status_event.actor_id,
                    new_status: status_event.new_status,
                    reason: status_event.reason,
                });
                inner.flush_actor_status_events_if_full()?;
            }
            EntityEvent::SentMessage(event) => {
                inner.sent_messages_buffer.insert(SentMessage {
                    id: hyperactor_telemetry::generate_sent_message_id(event.sender_actor_id),
                    timestamp_us: timestamp_to_micros(&event.timestamp),
                    sender_actor_id: event.sender_actor_id,
                    actor_mesh_id: event.actor_mesh_id,
                    view_json: event.view_json,
                    shape_json: event.shape_json,
                });
                inner.flush_sent_messages_if_full()?;
            }
            EntityEvent::Message(event) => {
                inner.messages_buffer.insert(Message {
                    id: event.id,
                    timestamp_us: timestamp_to_micros(&event.timestamp),
                    from_actor_id: event.from_actor_id,
                    to_actor_id: event.to_actor_id,
                    endpoint: event.endpoint,
                    port_index: event.port_index,
                });
                inner.flush_messages_if_full()?;
            }
            EntityEvent::MessageStatus(event) => {
                inner
                    .message_status_events_buffer
                    .insert(MessageStatusEvent {
                        id: event.id,
                        timestamp_us: timestamp_to_micros(&event.timestamp),
                        message_id: event.message_id,
                        status: event.status,
                    });
                inner.flush_message_status_events_if_full()?;
            }
        }
        Ok(())
    }
}

impl TraceEventSink for EntityBatchSink {
    fn consume(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        if let TraceEvent::Entity(event) = event {
            self.consume_entity(event.clone())?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        EntityBatchSink::flush(self)
    }

    fn name(&self) -> &str {
        "EntityBatchSink"
    }
}
