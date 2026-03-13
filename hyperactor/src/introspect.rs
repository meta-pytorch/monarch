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
//! `publish_attrs()`, which the introspect task reads for
//! Entity-view queries. Non-addressable children (e.g., system procs)
//! are resolved via a callback registered on [`InstanceCell`].
//!
//! Callers navigate topology by fetching an [`IntrospectResult`]
//! and following its `children` references.
//!
//! # Design Invariants
//!
//! The introspection subsystem maintains eleven invariants (S1--S11).
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
//! - **S11.** Terminated snapshots do not keep actors resolvable --
//!   `store_terminated_snapshot` writes to the proc's snapshot map,
//!   not the instances map. `resolve_actor_ref` checks terminal
//!   status independently and is unaffected by snapshot storage.
//! - **S12.** Introspection must not impair actor liveness --
//!   introspection queries (including DashMap reads for actor
//!   enumeration) must not cause convoy starvation or scheduling
//!   delays that stall concurrent actor spawn/stop operations.
//!
//! ## Introspection key invariants (IK-*)
//!
//! - **IK-1 (metadata completeness):** Every actor-runtime
//!   introspection key must carry `@meta(INTROSPECT = ...)` with
//!   non-empty `name` and `desc`.
//! - **IK-2 (short-name uniqueness):** No two introspection keys
//!   may share the same `IntrospectAttr.name`. Duplicates would break
//!   the FQ-to-short HTTP remap and schema output.
//!
//! ## Failure introspection invariants (FI-*)
//!
//! The FailureInfo presentation type lives in
//! `hyperactor_mesh::introspect`; these invariants are documented
//! here because the enforcement sites are in hyperactor
//! (`proc.rs` `serve()`, `live_actor_payload`).
//!
//! - **FI-1 (event-before-status):** All `InstanceCell` state that
//!   `live_actor_payload` reads must be written BEFORE
//!   `change_status()` transitions to terminal.
//! - **FI-2 (write-once):** `InstanceCellState::supervision_event`
//!   is written at most once per actor lifetime.
//! - **FI-3 (failure attrs <-> status):** Failure attrs are present
//!   iff status is `"failed"`.
//! - **FI-4 (is_propagated <-> root_cause_actor):**
//!   `failure_is_propagated == true` iff
//!   `failure_root_cause_actor != this_actor_id`.
//! - **FI-5 (is_poisoned <-> failed_actor_count):**
//!   `is_poisoned == true` iff `failed_actor_count > 0`.
//! - **FI-6 (clean stop = no artifacts):** When an actor stops
//!   cleanly, `supervision_event` is `None`, failure attrs are
//!   absent, and the actor does not contribute to
//!   `failed_actor_count`.

use std::time::SystemTime;

use hyperactor_config::INTROSPECT;
use hyperactor_config::IntrospectAttr;
use hyperactor_config::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::InstanceCell;
use crate::reference;

// Introspection attr keys — actor-runtime concepts.
//
// These keys are populated by the introspect handler from
// InstanceCell data. Mesh-topology keys (node_type, addr, num_procs,
// etc.) are declared in hyperactor_mesh::introspect.
//
// Naming convention:
//
// - Attr names are node-type-agnostic. The `node_type` attr (from the
//   mesh layer) identifies what kind of node it is; individual attr
//   names don't repeat that. So `status`, not `actor_status`.
// - Related attrs share a prefix to form a group. The `failure_*`
//   keys decompose failure info into flat attrs — the `failure_`
//   prefix groups them semantically.
// - `actor_type` is an exception: the `actor_` prefix disambiguates
//   it from `node_type` (mesh-layer concept). `actor_type` is the
//   Rust actor type name; `node_type` is the topology role.
// - Use real types where possible (e.g. SystemTime for timestamps),
//   not String. Serialization format is a presentation concern.
// - Internal key names are fully-qualified by `declare_attrs!`
//   (module_path + attr constant), e.g.
//   `hyperactor::introspect::status`.
// - HTTP/schema public key names come from `@meta(INTROSPECT =
//   IntrospectAttr { name, desc })`. Keep `name` explicit so API
//   stability is decoupled from internal refactors.
//
// See IK-1 (metadata completeness) and IK-2 (short-name uniqueness)
// in module doc.
declare_attrs! {
    /// Actor lifecycle status: "running", "stopped", "failed".
    ///
    /// Together with `STATUS_REASON`, these two attrs replace the
    /// former `actor_status` prefix protocol (`"stopped:reason"`,
    /// `"failed:reason"`) with structured fields, eliminating string
    /// prefix parsing in consumers.
    @meta(INTROSPECT = IntrospectAttr {
        name: "status".into(),
        desc: "Actor lifecycle status: running, stopped, failed".into(),
    })
    pub attr STATUS: String;

    /// Reason for stop/failure (absent when running).
    @meta(INTROSPECT = IntrospectAttr {
        name: "status_reason".into(),
        desc: "Reason for stop/failure (absent when running)".into(),
    })
    pub attr STATUS_REASON: String;

    /// Fully-qualified actor type name.
    @meta(INTROSPECT = IntrospectAttr {
        name: "actor_type".into(),
        desc: "Fully-qualified actor type name".into(),
    })
    pub attr ACTOR_TYPE: String;

    /// Number of messages processed by this actor.
    @meta(INTROSPECT = IntrospectAttr {
        name: "messages_processed".into(),
        desc: "Number of messages processed by this actor".into(),
    })
    pub attr MESSAGES_PROCESSED: u64 = 0;

    /// Timestamp when this actor was created.
    @meta(INTROSPECT = IntrospectAttr {
        name: "created_at".into(),
        desc: "Timestamp when this actor was created".into(),
    })
    pub attr CREATED_AT: SystemTime;

    /// Name of the last message handler invoked.
    @meta(INTROSPECT = IntrospectAttr {
        name: "last_handler".into(),
        desc: "Name of the last message handler invoked".into(),
    })
    pub attr LAST_HANDLER: String;

    /// Total CPU time in message handlers (microseconds).
    @meta(INTROSPECT = IntrospectAttr {
        name: "total_processing_time_us".into(),
        desc: "Total CPU time in message handlers (microseconds)".into(),
    })
    pub attr TOTAL_PROCESSING_TIME_US: u64 = 0;

    /// Flight recorder JSON (recent trace events).
    @meta(INTROSPECT = IntrospectAttr {
        name: "flight_recorder".into(),
        desc: "Flight recorder JSON (recent trace events)".into(),
    })
    pub attr FLIGHT_RECORDER: String;

    /// Whether this actor is infrastructure/system.
    @meta(INTROSPECT = IntrospectAttr {
        name: "is_system".into(),
        desc: "Whether this actor is infrastructure/system".into(),
    })
    pub attr IS_SYSTEM: bool = false;

    /// Child reference strings for tree navigation. Published by
    /// infrastructure actors (HostMeshAgent, ProcAgent) so the
    /// Entity view can return children without parsing mesh-layer keys.
    @meta(INTROSPECT = IntrospectAttr {
        name: "children".into(),
        desc: "Child reference strings for tree navigation".into(),
    })
    pub attr CHILDREN: Vec<String>;

    /// Machine-readable error code for error nodes.
    @meta(INTROSPECT = IntrospectAttr {
        name: "error_code".into(),
        desc: "Machine-readable error code (e.g. not_found)".into(),
    })
    pub attr ERROR_CODE: String;

    /// Human-readable error message for error nodes.
    @meta(INTROSPECT = IntrospectAttr {
        name: "error_message".into(),
        desc: "Human-readable error message".into(),
    })
    pub attr ERROR_MESSAGE: String;

    // Failure attrs — decomposition of FailureInfo into flat attrs.
    //
    // - **FI-A1 (presence):** failure_* attrs are present iff
    //   status == "failed"; absent otherwise. (Attr-level restatement
    //   of FI-3.)
    // - **FI-A2 (propagation):** failure_is_propagated == true iff
    //   failure_root_cause_actor != this actor's id. (Attr-level
    //   restatement of FI-4.)
    // FI-1, FI-2 (write ordering) are enforced in proc.rs serve()
    // and are unaffected by the representation change.
    // FI-5, FI-6 are proc/mesh-level and unaffected.

    /// Failure error message.
    @meta(INTROSPECT = IntrospectAttr {
        name: "failure_error_message".into(),
        desc: "Failure error message".into(),
    })
    pub attr FAILURE_ERROR_MESSAGE: String;

    /// Actor that caused the failure (root cause).
    @meta(INTROSPECT = IntrospectAttr {
        name: "failure_root_cause_actor".into(),
        desc: "Actor that caused the failure (root cause)".into(),
    })
    pub attr FAILURE_ROOT_CAUSE_ACTOR: String;

    /// Name of root cause actor.
    @meta(INTROSPECT = IntrospectAttr {
        name: "failure_root_cause_name".into(),
        desc: "Name of root cause actor".into(),
    })
    pub attr FAILURE_ROOT_CAUSE_NAME: String;

    /// Timestamp when failure occurred.
    @meta(INTROSPECT = IntrospectAttr {
        name: "failure_occurred_at".into(),
        desc: "Timestamp when failure occurred".into(),
    })
    pub attr FAILURE_OCCURRED_AT: SystemTime;

    /// Whether the failure was propagated from a child.
    @meta(INTROSPECT = IntrospectAttr {
        name: "failure_is_propagated".into(),
        desc: "Whether the failure was propagated from a child".into(),
    })
    pub attr FAILURE_IS_PROPAGATED: bool = false;
}

// See FI-1 through FI-6 in module doc.

/// Internal introspection result. Carries attrs as a JSON string.
/// The mesh layer constructs the API-facing `NodePayload` (with
/// `properties`) from this via `derive_properties`.
///
/// This is the internal wire type — it travels over actor ports
/// via `IntrospectMessage`. The presentation-layer `NodePayload`
/// (with `NodeProperties`) lives in `hyperactor_mesh::introspect`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct IntrospectResult {
    /// Canonical reference string for this node.
    pub identity: String,
    /// JSON-serialized `Attrs` bag containing introspection attributes.
    pub attrs: String,
    /// Reference strings the client can GET next to descend the tree.
    pub children: Vec<String>,
    /// Parent node reference for upward navigation.
    pub parent: Option<String>,
    /// ISO 8601 timestamp indicating when this data was captured.
    pub as_of: String,
}
wirevalue::register_type!(IntrospectResult);

/// Context for introspection query - what aspect of the actor to
/// describe.
///
/// Infrastructure actors (e.g., ProcAgent, HostAgent)
/// have dual nature: they manage entities (Proc, Host) while also
/// being actors themselves. IntrospectView allows callers to
/// specify which aspect to query.
// TODO(monarch-introspection): IntrospectView currently uses
// Entity/Actor naming. Consider renaming to runtime-neutral query
// modes (e.g. Published/Runtime) to avoid mesh-domain wording in
// hyperactor while preserving behavior and wire compatibility.
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
        reply: reference::OncePortRef<IntrospectResult>,
    },
    /// "Describe one of your children."
    QueryChild {
        /// Reference identifying the child to describe.
        child_ref: reference::Reference,
        /// Reply port receiving the child's description.
        reply: reference::OncePortRef<IntrospectResult>,
    },
}
wirevalue::register_type!(IntrospectMessage);

/// Structured tracing event from the actor-local flight recorder.
///
/// Deserialization target for the `FLIGHT_RECORDER` attrs JSON string.
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

/// Format a [`SystemTime`] as an ISO 8601 timestamp with millisecond
/// precision.
pub fn format_timestamp(time: SystemTime) -> String {
    humantime::format_rfc3339_millis(time).to_string()
}

/// Build a JSON-serialized `Attrs` string from values already
/// computed by `live_actor_payload`. Reuses the same data — no
/// redundant reads from `InstanceCell`.
///
/// Populates actor-runtime keys (STATUS, ACTOR_TYPE, etc.),
/// decomposes the status prefix protocol into STATUS + STATUS_REASON,
/// and decomposes failure fields into individual FAILURE_* attrs.
///
/// Starts from a fresh `Attrs` bag — published attrs (node_type,
/// addr, etc.) are NOT included. This ensures the Actor view
/// produces actor-only data; the Entity view handles published
/// attrs separately.
/// Failure fields extracted from a supervision event.
struct FailureSnapshot {
    error_message: String,
    root_cause_actor: String,
    root_cause_name: Option<String>,
    occurred_at: String,
    is_propagated: bool,
}

/// Pre-computed actor state for building the attrs JSON string.
/// Avoids redundant InstanceCell reads — `live_actor_payload`
/// computes these once and passes them in.
struct ActorSnapshot {
    status_str: String,
    is_system: bool,
    last_handler: Option<String>,
    flight_recorder: Option<String>,
    failure: Option<FailureSnapshot>,
}

fn build_actor_attrs(cell: &crate::InstanceCell, snap: &ActorSnapshot) -> String {
    // Actor view builds a clean attrs bag with only actor-runtime
    // keys. Published attrs (node_type, addr, etc.) belong to the
    // Entity view — they are NOT merged here. This ensures that
    // e.g. a HostMeshAgent resolved via Actor view produces Actor
    // properties, not Host properties.
    let mut attrs = hyperactor_config::Attrs::new();

    // IA-3: status_reason present iff status carries a reason.
    if let Some(reason) = snap.status_str.strip_prefix("stopped:") {
        attrs.set(STATUS, "stopped".to_string());
        attrs.set(STATUS_REASON, reason.trim().to_string());
    } else if let Some(reason) = snap.status_str.strip_prefix("failed:") {
        attrs.set(STATUS, "failed".to_string());
        attrs.set(STATUS_REASON, reason.trim().to_string());
    } else {
        attrs.set(STATUS, snap.status_str.clone());
        // IA-3: no status_reason for non-terminal states —
        // guaranteed by fresh Attrs bag.
    }

    attrs.set(ACTOR_TYPE, cell.actor_type_name().to_string());
    attrs.set(MESSAGES_PROCESSED, cell.num_processed_messages());
    attrs.set(CREATED_AT, cell.created_at());
    attrs.set(TOTAL_PROCESSING_TIME_US, cell.total_processing_time_us());
    attrs.set(IS_SYSTEM, snap.is_system);

    if let Some(handler) = &snap.last_handler {
        attrs.set(LAST_HANDLER, handler.clone());
    }
    if let Some(fr) = &snap.flight_recorder {
        attrs.set(FLIGHT_RECORDER, fr.clone());
    }

    // IA-4 / FI-A1: failure attrs present iff status == "failed".
    if let Some(fi) = &snap.failure {
        attrs.set(FAILURE_ERROR_MESSAGE, fi.error_message.clone());
        attrs.set(FAILURE_ROOT_CAUSE_ACTOR, fi.root_cause_actor.clone());
        if let Some(name) = &fi.root_cause_name {
            attrs.set(FAILURE_ROOT_CAUSE_NAME, name.clone());
        }
        if let Ok(t) = humantime::parse_rfc3339(&fi.occurred_at) {
            attrs.set(FAILURE_OCCURRED_AT, t);
        }
        attrs.set(FAILURE_IS_PROPAGATED, fi.is_propagated);
    }
    // IA-4: failure attrs absent when not failed — guaranteed by
    // starting from a fresh Attrs bag (no stale keys possible).

    serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string())
}

/// Build an [`IntrospectResult`] from live [`InstanceCell`] state.
///
/// Reads the current live status and last handler directly from
/// the cell. Used by the introspect task (which runs outside
/// the actor's message loop) and by `Instance::introspect_payload`.
pub fn live_actor_payload(cell: &InstanceCell) -> IntrospectResult {
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

    // FI-3: failure_info is computed from the same status value as
    // actor_status, ensuring they agree on whether the actor failed.
    let failure = if status.is_failed() {
        cell.supervision_event().map(|event| {
            let root = event.actually_failing_actor();
            FailureSnapshot {
                error_message: event.actor_status.to_string(),
                root_cause_actor: root.actor_id.to_string(),
                root_cause_name: root.display_name.clone(),
                occurred_at: format_timestamp(event.occurred_at),
                is_propagated: root.actor_id != *actor_id,
            }
        })
    } else {
        None
    };

    let snap = ActorSnapshot {
        status_str: status.to_string(),
        is_system: cell.is_system(),
        last_handler: last_handler.map(|info| info.to_string()),
        flight_recorder,
        failure,
    };

    let attrs = build_actor_attrs(cell, &snap);

    IntrospectResult {
        identity: actor_id.to_string(),
        attrs,
        children,
        parent: supervisor,
        as_of: format_timestamp(std::time::SystemTime::now()),
    }
}

/// Introspect task: runs on a dedicated tokio task per actor,
/// handling [`IntrospectMessage`] by reading [`InstanceCell`]
/// directly and replying via the actor's [`Mailbox`].
///
/// The actor's message loop never sees these messages.
///
/// # Invariants exercised
///
/// Exercises S1, S2, S4, S5, S6, S11 (see module doc).
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
                    Err(_) => {
                        // Channel closed. If the actor reached a
                        // terminal state, snapshot it before exiting
                        // so it remains queryable post-mortem.
                        if cell.status().borrow().is_terminal() {
                            let snapshot = live_actor_payload(&cell);
                            cell.store_terminated_snapshot(snapshot);
                        }
                        break;
                    }
                }
            }
            _ = status.wait_for(ActorStatus::is_terminal) => {
                // Snapshot for post-mortem introspection before
                // dropping our InstanceCell reference.
                let snapshot = live_actor_payload(&cell);
                cell.store_terminated_snapshot(snapshot);
                break;
            }
        };

        let result = match msg {
            IntrospectMessage::Query { view, reply } => {
                let payload = match view {
                    IntrospectView::Entity => match cell.published_attrs() {
                        Some(published) => {
                            let attrs_json =
                                serde_json::to_string(&published).unwrap_or_else(|_| "{}".into());
                            let children: Vec<String> =
                                published.get(CHILDREN).cloned().unwrap_or_default();
                            IntrospectResult {
                                identity: cell.actor_id().to_string(),
                                attrs: attrs_json,
                                children,
                                parent: cell.parent().map(|p| p.actor_id().to_string()),
                                as_of: format_timestamp(std::time::SystemTime::now()),
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
                let payload = cell.query_child(&child_ref).unwrap_or_else(|| {
                    let mut error_attrs = hyperactor_config::Attrs::new();
                    error_attrs.set(ERROR_CODE, "not_found".to_string());
                    error_attrs.set(
                        ERROR_MESSAGE,
                        format!("child {} not found (no callback registered)", child_ref),
                    );
                    IntrospectResult {
                        identity: String::new(),
                        attrs: serde_json::to_string(&error_attrs)
                            .unwrap_or_else(|_| "{}".to_string()),
                        children: Vec::new(),
                        parent: None,
                        as_of: humantime::format_rfc3339_millis(std::time::SystemTime::now())
                            .to_string(),
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Exercises IK-1 (see module doc).
    #[test]
    fn test_introspect_keys_are_tagged() {
        let cases = vec![
            ("status", STATUS.attrs()),
            ("status_reason", STATUS_REASON.attrs()),
            ("actor_type", ACTOR_TYPE.attrs()),
            ("messages_processed", MESSAGES_PROCESSED.attrs()),
            ("created_at", CREATED_AT.attrs()),
            ("last_handler", LAST_HANDLER.attrs()),
            ("total_processing_time_us", TOTAL_PROCESSING_TIME_US.attrs()),
            ("flight_recorder", FLIGHT_RECORDER.attrs()),
            ("is_system", IS_SYSTEM.attrs()),
            ("children", CHILDREN.attrs()),
            ("error_code", ERROR_CODE.attrs()),
            ("error_message", ERROR_MESSAGE.attrs()),
            ("failure_error_message", FAILURE_ERROR_MESSAGE.attrs()),
            ("failure_root_cause_actor", FAILURE_ROOT_CAUSE_ACTOR.attrs()),
            ("failure_root_cause_name", FAILURE_ROOT_CAUSE_NAME.attrs()),
            ("failure_occurred_at", FAILURE_OCCURRED_AT.attrs()),
            ("failure_is_propagated", FAILURE_IS_PROPAGATED.attrs()),
        ];

        for (expected_name, meta) in &cases {
            // IK-1: see module doc.
            let introspect = meta
                .get(INTROSPECT)
                .unwrap_or_else(|| panic!("{expected_name}: missing INTROSPECT meta-attr"));
            assert_eq!(
                introspect.name, *expected_name,
                "short name mismatch for {expected_name}"
            );
            assert!(
                !introspect.desc.is_empty(),
                "{expected_name}: desc should not be empty"
            );
        }

        // Exhaustiveness: verify cases covers all INTROSPECT-tagged
        // keys declared in this module.
        use hyperactor_config::attrs::AttrKeyInfo;
        let registry_count = inventory::iter::<AttrKeyInfo>()
            .filter(|info| {
                info.name.starts_with("hyperactor::introspect::")
                    && info.meta.get(INTROSPECT).is_some()
            })
            .count();
        assert_eq!(
            cases.len(),
            registry_count,
            "test must cover all INTROSPECT-tagged keys in this module"
        );
    }

    /// Exercises IK-2 (see module doc).
    #[test]
    fn test_introspect_short_names_are_globally_unique() {
        use hyperactor_config::attrs::AttrKeyInfo;

        let mut seen = std::collections::HashMap::new();
        for info in inventory::iter::<AttrKeyInfo>() {
            let Some(introspect) = info.meta.get(INTROSPECT) else {
                continue;
            };
            // Metadata quality: every tagged key must have
            // non-empty name and desc.
            assert!(
                !introspect.name.is_empty(),
                "INTROSPECT key {:?} has empty name",
                info.name
            );
            assert!(
                !introspect.desc.is_empty(),
                "INTROSPECT key {:?} has empty desc",
                info.name
            );
            if let Some(prev_fq) = seen.insert(introspect.name.clone(), info.name) {
                panic!(
                    "IK-2 violation: duplicate short name {:?} declared by both {:?} and {:?}",
                    introspect.name, prev_fq, info.name
                );
            }
        }
    }

    // IA-1, IA-3, IA-4 tests require spawning actors and live in
    // actor.rs where #[hyperactor::export] and test infrastructure
    // are available.
}
