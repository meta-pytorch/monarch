/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! EntityDispatcher - Dispatches entity lifecycle events to Arrow RecordBatches
//!
//! Handles actor and actor mesh creation events, buffering them and flushing
//! to tables when the buffer reaches the configured batch size.
//!
//! Produces two tables:
//! - `actors`: Actor creation events
//! - `actor_meshes`: Actor mesh creation events

use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

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
}

/// Row data for the actor_meshes table.
/// Logged when actor meshes are created.
#[derive(RecordBatchRow)]
pub struct ActorMesh {
    /// Unique identifier for this actor mesh
    pub id: u64,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_us: i64,
    /// Actor mesh class (e.g., "Proc", "Host", "Python<SomeUserDefinedActor>")
    pub class: String,
    /// User-provided name for this actor mesh
    pub given_name: String,
    /// Full hierarchical name as it appears in supervision events
    pub full_name: String,
    /// Shape of the actor mesh, serialized from ndslice::Shape (labels + slice topology)
    pub shape_json: String,
    /// Parent actor mesh ID (None for root meshes)
    pub parent_mesh_id: Option<u64>,
    /// Region over which the parent spawned this actor mesh, serialized from ndslice::Region
    pub parent_view_json: Option<String>,
}

/// Inner state of EntityDispatcher.
struct EntityDispatcherInner {
    actors_buffer: ActorBuffer,
    actor_meshes_buffer: ActorMeshBuffer,
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
        Self::flush_buffer(
            &mut self.actor_meshes_buffer,
            "actor_meshes",
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

    fn flush_actor_meshes_if_full(&mut self) -> anyhow::Result<()> {
        if self.actor_meshes_buffer.len() >= self.batch_size {
            Self::flush_buffer(
                &mut self.actor_meshes_buffer,
                "actor_meshes",
                &self.flush_callback,
            )?;
        }
        Ok(())
    }
}

/// Dispatches entity lifecycle events (actors, actor meshes) to Arrow RecordBatches.
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
            actor_meshes_buffer: ActorMeshBuffer::default(),
            batch_size,
            flush_callback,
        }));
        Self { inner }
    }

    /// Flush all buffers, emitting batches for actors and actor_meshes tables.
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
    fn dispatch(&self, event: &EntityEvent) -> Result<(), anyhow::Error> {
        match event {
            EntityEvent::Actor(actor_event) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
                inner.actors_buffer.insert(Actor {
                    id: actor_event.id,
                    timestamp_us: timestamp_to_micros(&actor_event.timestamp),
                    mesh_id: actor_event.mesh_id,
                    rank: actor_event.rank,
                    full_name: actor_event.full_name.clone(),
                });
                inner.flush_actors_if_full()?;
            }
            EntityEvent::ActorMesh(mesh_event) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
                inner.actor_meshes_buffer.insert(ActorMesh {
                    id: mesh_event.id,
                    timestamp_us: timestamp_to_micros(&mesh_event.timestamp),
                    class: mesh_event.class.clone(),
                    given_name: mesh_event.given_name.clone(),
                    full_name: mesh_event.full_name.clone(),
                    shape_json: mesh_event.shape_json.clone(),
                    parent_mesh_id: mesh_event.parent_mesh_id,
                    parent_view_json: mesh_event.parent_view_json.clone(),
                });
                inner.flush_actor_meshes_if_full()?;
            }
        }
        Ok(())
    }
}
