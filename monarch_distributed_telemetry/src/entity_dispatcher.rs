/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EntityDispatcher - Dispatches entity lifecycle events to Arrow RecordBatches
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
use hyperactor_telemetry::EntityEventDispatcher;
use record_batch_derive::RecordBatchRow;

use crate::record_batch_sink::FlushCallback;
use crate::record_batch_sink::RecordBatchBuffer;
use crate::timestamp_to_micros;

/// Row data for the actors table.
/// Logged when actors are created.
#[derive(RecordBatchRow)]
pub struct Actor {
    /// Unique identifier for this actor
    pub id: u64,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_us: i64,
    /// ID of the mesh this actor belongs to
    pub mesh_id: u64,
    /// Rank index into the mesh shape
    pub rank: u64,
    /// Full hierarchical name of this actor
    pub full_name: String,
    /// User-facing name for this actor
    pub display_name: Option<String>,
}

/// Row data for the meshes table.
/// Logged when meshes are created.
#[derive(RecordBatchRow)]
pub struct Mesh {
    /// Unique identifier for this mesh
    pub id: u64,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_us: i64,
    /// mesh class (e.g., "Proc", "Host", "Python<SomeUserDefinedActor>")
    pub class: String,
    /// User-provided name for this mesh
    pub given_name: String,
    /// Full hierarchical name as it appears in supervision events
    pub full_name: String,
    /// Shape of the mesh, serialized from ndslice::Extent
    pub shape_json: String,
    /// Parent mesh ID (None for root meshes)
    pub parent_mesh_id: Option<u64>,
    /// Region over which the parent spawned this mesh, serialized from ndslice::Region
    pub parent_view_json: Option<String>,
}

/// Row data for the actor_status_events table.
/// Logged when actors change status.
#[derive(RecordBatchRow)]
pub struct ActorStatusEvent {
    /// Unique identifier for this event
    pub id: u64,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_us: i64,
    /// ID of the actor whose status changed
    pub actor_id: u64,
    /// New status value (e.g. "Created", "Idle", "Failed")
    pub new_status: String,
    /// Reason for the status change (e.g. error message for Failed)
    pub reason: Option<String>,
}

/// Row data for the sent_messages table.
///
/// Tracks messages from the perspective of the sending actor.
#[derive(RecordBatchRow)]
pub struct SentMessage {
    /// Unique identifier for this sent message record
    pub id: u64,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_us: i64,
    /// ID of the sending actor
    pub sender_actor_id: u64,
    /// ID of the actor mesh over which the message was sent (0 for point-to-point)
    pub actor_mesh_id: u64,
    /// Region over which the message was sent, serialized from ndslice::Region
    pub view_json: String,
    /// Shape of the message, serialized from ndslice::Shape
    pub shape_json: String,
}

/// Row data for the messages table.
///
/// Tracks messages from the receiver's perspective.
#[derive(RecordBatchRow)]
pub struct Message {
    /// Unique identifier for this received message
    pub id: u64,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_us: i64,
    /// Hash of sender's ActorId
    pub from_actor_id: u64,
    /// Hash of receiver's ActorId
    pub to_actor_id: u64,
    /// Message handler type name
    pub endpoint: Option<String>,
    /// Port identifier (reserved)
    pub port_id: Option<u64>,
}

/// Row data for the message_status_events table.
///
/// Logs status transitions for received messages.
#[derive(RecordBatchRow)]
pub struct MessageStatusEvent {
    /// Unique identifier for this status event
    pub id: u64,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_us: i64,
    /// FK to messages.id
    pub message_id: u64,
    /// Status value: "queued", "active", or "complete"
    pub status: String,
}

/// Inner state of EntityDispatcher.
struct EntityDispatcherInner {
    actors_buffer: ActorBuffer,
    meshes_buffer: MeshBuffer,
    actor_status_events_buffer: ActorStatusEventBuffer,
    sent_messages_buffer: SentMessageBuffer,
    messages_buffer: MessageBuffer,
    message_status_events_buffer: MessageStatusEventBuffer,
    batch_size: usize,
    flush_callback: FlushCallback,
}

impl EntityDispatcherInner {
    fn flush_buffer<B: RecordBatchBuffer>(
        buffer: &mut B,
        table_name: &str,
        callback: &FlushCallback,
    ) -> anyhow::Result<()> {
        let batch = buffer.to_record_batch()?;
        callback(table_name, batch);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        Self::flush_buffer(&mut self.actors_buffer, "actors", &self.flush_callback)?;
        Self::flush_buffer(&mut self.meshes_buffer, "meshes", &self.flush_callback)?;
        Self::flush_buffer(
            &mut self.actor_status_events_buffer,
            "actor_status_events",
            &self.flush_callback,
        )?;
        Self::flush_buffer(
            &mut self.sent_messages_buffer,
            "sent_messages",
            &self.flush_callback,
        )?;
        Self::flush_buffer(&mut self.messages_buffer, "messages", &self.flush_callback)?;
        Self::flush_buffer(
            &mut self.message_status_events_buffer,
            "message_status_events",
            &self.flush_callback,
        )?;
        Ok(())
    }

    fn flush_actors_if_full(&mut self) -> anyhow::Result<()> {
        if self.actors_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.actors_buffer, "actors", &self.flush_callback)?;
        }
        Ok(())
    }

    fn flush_meshes_if_full(&mut self) -> anyhow::Result<()> {
        if self.meshes_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.meshes_buffer, "meshes", &self.flush_callback)?;
        }
        Ok(())
    }

    fn flush_actor_status_events_if_full(&mut self) -> anyhow::Result<()> {
        if self.actor_status_events_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.actor_status_events_buffer,
                "actor_status_events",
                &self.flush_callback,
            )?;
        }
        Ok(())
    }

    fn flush_sent_messages_if_full(&mut self) -> anyhow::Result<()> {
        if self.sent_messages_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.sent_messages_buffer,
                "sent_messages",
                &self.flush_callback,
            )?;
        }
        Ok(())
    }

    fn flush_messages_if_full(&mut self) -> anyhow::Result<()> {
        if self.messages_buffer.len() >= self.batch_size {
            Self::flush_buffer(&mut self.messages_buffer, "messages", &self.flush_callback)?;
        }
        Ok(())
    }

    fn flush_message_status_events_if_full(&mut self) -> anyhow::Result<()> {
        if self.message_status_events_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.message_status_events_buffer,
                "message_status_events",
                &self.flush_callback,
            )?;
        }
        Ok(())
    }
}

/// Dispatches entity lifecycle events to Arrow RecordBatches.
///
/// This is separate from RecordBatchSink which handles tracing events (spans, events).
/// Both use the same FlushCallback pattern to push batches to the database scanner's tables.
#[derive(Clone)]
pub struct EntityDispatcher {
    inner: Arc<Mutex<EntityDispatcherInner>>,
}

impl EntityDispatcher {
    /// Create a new EntityDispatcher with the specified batch size and flush callback.
    ///
    /// The callback receives (table_name, record_batch) when a batch is ready.
    /// The callback should handle empty batches by creating the table with the
    /// schema but not appending the empty data.
    ///
    /// # Arguments
    /// * `batch_size` - Number of rows to buffer before flushing each table
    /// * `flush_callback` - Called with (table_name, record_batch) when a batch is ready
    pub fn new(batch_size: usize, flush_callback: FlushCallback) -> Self {
        let inner = Arc::new(Mutex::new(EntityDispatcherInner {
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

    /// Flush all buffers, emitting batches for actors and meshes tables.
    ///
    /// This always emits batches for both tables, even if they are empty.
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

impl EntityEventDispatcher for EntityDispatcher {
    fn dispatch(&self, event: EntityEvent) -> Result<(), anyhow::Error> {
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
                    port_id: event.port_id,
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
