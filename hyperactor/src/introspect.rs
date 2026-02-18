/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Blanket introspection protocol for hyperactor actors.
//!
//! Every actor automatically handles [`IntrospectMessage`] via a
//! blanket `Handler` implementation. The default response is
//! *structural*: it reports only framework-owned state (identity,
//! type, status, supervision parent/children) and is intended to be
//! cheap and safe to call on any actor at any time.
//!
//! Callers navigate topology by fetching a [`NodePayload`] and
//! following its `children` references. Children come in two forms:
//!
//! - **Addressable children** are actors with their own `ActorId` and
//!   mailbox. They can be introspected directly by sending
//!   [`IntrospectMessage::Query`] to that actor. The default handler
//!   reports these automatically.
//! - **Non-addressable children** are nodes a parent chooses to
//!   expose in `children` but which are not independently messageable
//!   (no mailbox / `ActorId`). These must be described indirectly via
//!   [`IntrospectMessage::QueryChild`], where the parent answers on
//!   the child's behalf (for example, a host exposing system procs
//!   that are not independently addressable actors).
//!
//! Actors that own non-addressable children, or that want to publish
//! domain-specific [`NodeProperties`] (e.g. `Host` or `Proc`),
//! override [`Actor::handle_introspect`].
//!
//! # Pre-dispatch snapshot (one-behind invariant)
//!
//! [`default_actor_payload`] reports `actor_status` and
//! `last_message_handler` from a *pre-dispatch snapshot* — the values
//! captured immediately before the current handler began executing.
//! The running handler therefore never sees itself in the payload.
//!
//! This is a purely mechanical "one behind" guarantee: consecutive
//! introspect queries *will* reveal each other (the second query
//! reports the first introspect handler), and introspection is not
//! otherwise hidden from the history. For a freshly spawned actor
//! with no prior messages, the snapshot reports post-initialization
//! state (`idle`) and no last handler.

use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::InstanceCell;
use crate::OncePortRef;
use crate::reference::Reference;

/// Node-type-specific metadata for a single entity in the mesh
/// topology (root, host, proc, actor, or error sentinel).
///
/// Kept "wire-friendly" (no `serde_json::Value`) so it can be encoded
/// via wirevalue's bincode path, while the HTTP layer can still
/// expose structured JSON via `Serialize`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum NodeProperties {
    /// Synthetic mesh root node (not a real actor/proc).
    Root {
        /// Number of hosts registered with the mesh admin agent.
        num_hosts: usize,
        /// When the mesh was started (ISO-8601 timestamp).
        started_at: String,
        /// Username who started the mesh.
        started_by: String,
    },

    /// A host in the mesh, represented by its `HostMeshAgent`.
    Host {
        /// Host address (e.g. `127.0.0.1:12345`).
        addr: String,
        /// Number of procs currently reported on this host.
        num_procs: usize,
    },

    /// Properties describing a proc running on a host.
    Proc {
        /// Human-readable proc identifier.
        proc_name: String,
        /// Number of actors currently hosted by this proc.
        num_actors: usize,
        /// Whether this proc is infrastructure-owned rather than
        /// user-created.
        #[serde(default)]
        is_system: bool,
    },

    /// Runtime metadata for a single actor instance.
    Actor {
        /// Current lifecycle/status of the actor (e.g. "Running",
        /// "Stopped").
        actor_status: String,
        /// Concrete actor type name.
        actor_type: String,
        /// Total number of messages processed by this actor so far.
        messages_processed: u64,
        /// Actor creation time, as an ISO-8601 timestamp string.
        created_at: String,
        /// Name of the most recent message handler run by the actor,
        /// if known.
        last_message_handler: Option<String>,
        /// Cumulative time spent processing messages, in
        /// microseconds.
        total_processing_time_us: u64,
        /// Serialized flight-recorder events for the actor, if
        /// enabled/available.
        flight_recorder: Option<String>,
    },

    /// Error sentinel returned when a child reference cannot be
    /// resolved.
    Error {
        /// Machine-readable error code (e.g. "not_found").
        code: String,
        /// Human-readable error message.
        message: String,
    },
}
wirevalue::register_type!(NodeProperties);

/// Uniform response for any node in the mesh topology.
///
/// Every addressable entity (root, host, proc, actor) is represented
/// as a `NodePayload`. The client navigates the mesh by fetching a
/// node and following its `children` references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct NodePayload {
    /// Canonical reference string for this node.
    pub identity: String,
    /// Node-specific metadata (type, status, metrics, etc.).
    pub properties: NodeProperties,
    /// Reference strings the client can GET next to descend the
    /// tree.
    pub children: Vec<String>,
    /// Parent node reference for upward navigation.
    pub parent: Option<String>,
}
wirevalue::register_type!(NodePayload);

/// Context for introspection query - what aspect of the actor to
/// describe.
///
/// Infrastructure actors (e.g., ProcMeshAgent, HostMeshAgent)
/// have dual nature: they manage entities (Proc, Host) while also
/// being actors themselves. IntrospectView allows callers to
/// specify which aspect to query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Named)]
pub enum IntrospectView {
    /// Return managed-entity properties (Proc, Host, etc.) for
    /// infrastructure actors.
    Entity,
    /// Return standard actor properties (status, messages_processed,
    /// flight_recorder).
    Actor,
}
wirevalue::register_type!(IntrospectView);

/// Introspection query sent to any actor.
///
/// `Query` asks the actor to describe itself. `QueryChild` asks the
/// actor to describe one of its non-addressable children — an entity
/// that appears in the navigation tree but has no mailbox of its own
/// (e.g. a system proc owned by a host). The parent actor answers on
/// the child's behalf.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum IntrospectMessage {
    /// "Describe yourself."
    Query {
        /// View context - Entity or Actor.
        view: IntrospectView,
        /// Reply port receiving the actor's self-description.
        reply: OncePortRef<NodePayload>,
    },
    /// "Describe one of your children."
    QueryChild {
        /// Reference identifying the child to describe.
        child_ref: Reference,
        /// Reply port receiving the child's description.
        reply: OncePortRef<NodePayload>,
    },
}
wirevalue::register_type!(IntrospectMessage);

/// Structured tracing event from the actor-local flight recorder.
///
/// Deserialization target for the `flight_recorder` JSON string in
/// [`NodeProperties::Actor`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedEvent {
    /// ISO 8601 timestamp of the event.
    pub timestamp: String,
    /// Monotonic sequence number for ordering.
    #[serde(default)]
    pub seq: usize,
    /// Event level (INFO, DEBUG, etc.).
    pub level: String,
    /// Event target (module path).
    #[serde(default)]
    pub target: String,
    /// Event name.
    pub name: String,
    /// Event fields as JSON.
    pub fields: serde_json::Value,
}

/// Structural projection of an [`InstanceCell`] into a
/// [`NodePayload`] with [`NodeProperties::Actor`].
///
/// This is the default introspection response for any actor — it
/// reports only framework-owned state. Used by
/// [`default_handle_introspect`](crate::actor::default_handle_introspect).
///
/// Status and last-handler are taken from the *previous* values
/// (before the current handler started), so the introspect query
/// doesn't report itself. This avoids the Heisenberg problem where
/// observing the actor always shows "processing IntrospectMessage".
pub fn default_actor_payload(cell: &InstanceCell) -> NodePayload {
    let actor_id = cell.actor_id();
    // Use prev_status / prev_last_message_handler so the introspect
    // handler doesn't report itself.
    let status = cell.prev_status();
    let last_handler = cell.prev_last_message_handler();

    let children: Vec<String> = cell
        .child_actor_ids()
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    let events = cell.recording().tail();
    let flight_recorder_events: Vec<RecordedEvent> = events
        .into_iter()
        .map(|event| RecordedEvent {
            timestamp: format_timestamp(event.time),
            seq: event.seq,
            level: event.metadata.level().to_string(),
            target: event.metadata.target().to_string(),
            name: event.metadata.name().to_string(),
            fields: event.json_value(),
        })
        .collect();

    let flight_recorder = if flight_recorder_events.is_empty() {
        None
    } else {
        // Pre-serialize to JSON string for wire transport.
        serde_json::to_string(&flight_recorder_events).ok()
    };

    let supervisor = cell.parent().map(|p| p.actor_id().to_string());

    NodePayload {
        identity: actor_id.to_string(),
        properties: NodeProperties::Actor {
            actor_status: status.to_string(),
            actor_type: cell.actor_type_name().to_string(),
            messages_processed: cell.num_processed_messages(),
            created_at: format_timestamp(cell.created_at()),
            last_message_handler: last_handler.map(|info| info.to_string()),
            total_processing_time_us: cell.total_processing_time_us(),
            flight_recorder,
        },
        children,
        parent: supervisor,
    }
}

/// Format a [`SystemTime`] as an ISO 8601 timestamp with millisecond
/// precision.
fn format_timestamp(time: SystemTime) -> String {
    humantime::format_rfc3339_millis(time).to_string()
}
