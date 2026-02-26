/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Introspection protocol for hyperactor actors.
//!
//! Every actor has a dedicated introspect task that handles
//! [`IntrospectMessage`] by reading [`InstanceCell`] state directly,
//! without going through the actor's message loop. This means:
//!
//! - Stuck actors can be introspected (the task runs independently).
//! - Introspection does not perturb observed state (no Heisenberg).
//! - Live status is reported accurately.
//!
//! Infrastructure actors publish domain-specific metadata via
//! [`PublishedProperties`], which the introspect task reads for
//! Entity-view queries. Non-addressable children (e.g., system procs)
//! are resolved via a callback registered on [`InstanceCell`].
//!
//! Callers navigate topology by fetching a [`NodePayload`] and
//! following its `children` references.
//!
//! # Design Invariants
//!
//! The introspection subsystem maintains ten invariants (S1--S10).
//! Each is documented at the code site that enforces it.
//!
//! - **S1.** Introspection must not depend on actor responsiveness --
//!   a wedged actor can still be introspected (runtime task, not
//!   actor loop).
//! - **S2.** Introspection must not perturb observed state -- reading
//!   `InstanceCell` never sets `last_message_handler` to
//!   `IntrospectMessage`.
//! - **S3.** Sender routing is unchanged -- senders target the same
//!   `PortId` (`IntrospectMessage::port()`) across processes.
//! - **S4.** `IntrospectMessage` never produces a `WorkCell` --
//!   pre-registration via `open_message_port` gives the introspect
//!   port its own channel, independent of the actor's work queue.
//! - **S5.** Replies never use `PanickingMailboxSender` -- the
//!   introspect task replies via `Mailbox::serialize_and_send_once`.
//! - **S6.** View semantics are stable -- Actor view uses live
//!   structural state + supervision children; Entity view uses
//!   published properties + domain children.
//! - **S7.** `QueryChild` must work without actor handlers -- system
//!   procs are resolved via a per-actor callback on `InstanceCell`.
//! - **S8.** Published properties are constrained -- actors cannot
//!   publish `Root` or `Error` payloads (only `Host` and `Proc`
//!   variants).
//! - **S9.** Port binding is single source of truth -- the introspect
//!   port is bound exactly once via `bind_actor_port()` in
//!   `Instance::new()`.
//! - **S10.** Introspect receiver lifecycle -- created in
//!   `Instance::new()`, spawned in `start()`, dropped in
//!   `child_instance()`.

use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::InstanceCell;
use crate::OncePortRef;
use crate::clock::Clock;
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
        /// Whether this actor is infrastructure-owned (e.g.
        /// ProcMeshAgent, HostMeshAgent) rather than user-created.
        is_system: bool,
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
    /// ISO 8601 timestamp indicating when this data was captured.
    pub as_of: String,
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

/// Domain-specific properties an actor may publish for introspection.
///
/// Infrastructure actors (HostMeshAgent, ProcMeshAgent) push these to
/// make their managed-entity metadata available to the introspection
/// runtime without going through the actor's message handler. The
/// runtime handler reads the last-published value and merges it into
/// the [`NodePayload`] response for Entity-view queries.
///
/// Values may be arbitrarily stale for stuck actors — they reflect
/// whatever the actor last published before it stopped making
/// progress. The `published_at` timestamp makes staleness visible to
/// tooling.
#[derive(Debug, Clone)]
pub struct PublishedProperties {
    /// When these properties were last published.
    pub published_at: SystemTime,
    /// Domain-specific metadata.
    pub kind: PublishedPropertiesKind,
}

/// The domain-specific metadata variants that an actor may publish.
///
/// Only `Host` and `Proc` variants are available — actors cannot
/// publish `Root` or `Error` payloads.
#[derive(Debug, Clone)]
pub enum PublishedPropertiesKind {
    /// A host in the mesh.
    Host {
        /// Host address (e.g. `127.0.0.1:12345`).
        addr: String,
        /// Number of procs currently reported on this host.
        num_procs: usize,
        /// Custom children list (system procs + user procs).
        children: Vec<String>,
    },
    /// A proc running on a host.
    Proc {
        /// Human-readable proc identifier.
        proc_name: String,
        /// Number of actors currently hosted by this proc.
        num_actors: usize,
        /// Whether this proc is infrastructure-owned.
        is_system: bool,
        /// Custom children list (all actors in the proc).
        children: Vec<String>,
    },
}

/// Format a [`SystemTime`] as an ISO 8601 timestamp with millisecond
/// precision.
pub fn format_timestamp(time: SystemTime) -> String {
    humantime::format_rfc3339_millis(time).to_string()
}

/// Build a [`NodePayload`] from live [`InstanceCell`] state.
///
/// Reads the current live status and last handler directly from
/// the cell. Used by the introspect task (which runs outside
/// the actor's message loop) and by `Instance::introspect_payload`.
pub fn live_actor_payload(cell: &InstanceCell) -> NodePayload {
    let actor_id = cell.actor_id();
    let status = cell.status().borrow().clone();
    let last_handler = cell.last_message_handler();

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
            is_system: cell.published_properties().is_some_and(|p| {
                matches!(
                    p.kind,
                    PublishedPropertiesKind::Host { .. } | PublishedPropertiesKind::Proc { .. }
                )
            }),
        },
        children,
        parent: supervisor,
        as_of: format_timestamp(crate::clock::RealClock.system_time_now()),
    }
}

/// Introspect task: runs on a dedicated tokio task per actor,
/// handling [`IntrospectMessage`] by reading [`InstanceCell`]
/// directly and replying via the actor's [`Mailbox`].
///
/// The actor's message loop never sees these messages.
///
/// # Invariants
///
/// - **S1:** Introspection does not depend on actor responsiveness --
///   this task runs independently; a wedged actor is still introspectable.
/// - **S2:** Introspection does not perturb observed state -- reads
///   `InstanceCell` directly, never sets `last_message_handler`.
/// - **S4:** `IntrospectMessage` never produces a `WorkCell` -- the
///   introspect port has its own channel, separate from the work queue.
/// - **S5:** Replies never use `PanickingMailboxSender` -- replies go
///   through `Mailbox::serialize_and_send_once`.
/// - **S6:** View semantics -- Actor view uses live structural state +
///   supervision children; Entity view uses published properties +
///   domain children.
pub async fn serve_introspect(
    cell: InstanceCell,
    mailbox: crate::mailbox::Mailbox,
    mut receiver: crate::mailbox::PortReceiver<IntrospectMessage>,
) {
    use crate::actor::ActorStatus;
    use crate::mailbox::PortSender as _;

    // Watch for terminal status so we can break the reference cycle:
    // InstanceCellState → Ports → introspect sender → keeps receiver
    // open → this task holds InstanceCell → InstanceCellState.
    // Without this, a stopped actor's InstanceCellState is never
    // dropped and the actor lingers in the proc's instances map.
    let mut status = cell.status().clone();

    loop {
        let msg = tokio::select! {
            msg = receiver.recv() => {
                match msg {
                    Ok(msg) => msg,
                    Err(_) => break,
                }
            }
            _ = status.wait_for(ActorStatus::is_terminal) => break,
        };

        let result = match msg {
            IntrospectMessage::Query { view, reply } => {
                let payload = match view {
                    IntrospectView::Entity => match cell.published_properties() {
                        Some(props) => {
                            let published_at = props.published_at;
                            let children = match &props.kind {
                                PublishedPropertiesKind::Host { children, .. } => children.clone(),
                                PublishedPropertiesKind::Proc { children, .. } => children.clone(),
                            };
                            let properties = match props.kind {
                                PublishedPropertiesKind::Host {
                                    addr, num_procs, ..
                                } => NodeProperties::Host { addr, num_procs },
                                PublishedPropertiesKind::Proc {
                                    proc_name,
                                    num_actors,
                                    is_system,
                                    ..
                                } => NodeProperties::Proc {
                                    proc_name,
                                    num_actors,
                                    is_system,
                                },
                            };
                            NodePayload {
                                identity: cell.actor_id().to_string(),
                                properties,
                                children,
                                parent: cell.parent().map(|p| p.actor_id().to_string()),
                                as_of: format_timestamp(published_at),
                            }
                        }
                        None => live_actor_payload(&cell),
                    },
                    IntrospectView::Actor => live_actor_payload(&cell),
                };
                mailbox.serialize_and_send_once(
                    reply,
                    payload,
                    crate::mailbox::monitored_return_handle(),
                )
            }
            IntrospectMessage::QueryChild { child_ref, reply } => {
                let payload = cell.query_child(&child_ref).unwrap_or_else(|| NodePayload {
                    identity: String::new(),
                    properties: NodeProperties::Error {
                        code: "not_found".into(),
                        message: format!("child {} not found (no callback registered)", child_ref),
                    },
                    children: Vec::new(),
                    parent: None,
                    as_of: humantime::format_rfc3339_millis(
                        crate::clock::RealClock.system_time_now(),
                    )
                    .to_string(),
                });
                mailbox.serialize_and_send_once(
                    reply,
                    payload,
                    crate::mailbox::monitored_return_handle(),
                )
            }
        };
        if let Err(e) = result {
            tracing::debug!("introspect reply failed: {e}");
        }
    }
    tracing::debug!(
        actor_id = %cell.actor_id(),
        "introspect task exiting"
    );
}
