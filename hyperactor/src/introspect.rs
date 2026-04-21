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
//! `publish_attrs()`, which the introspect task reads for Entity-view
//! queries. Non-addressable children (e.g., system procs) are
//! resolved via a callback registered on [`InstanceCell`].
//!
//! Callers navigate topology by fetching an [`IntrospectResult`] and
//! following its `children` references.
//!
//! # Design Invariants
//!
//! The introspection subsystem maintains twelve invariants (S1--S12).
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
//! - **IK-2 (short-name uniqueness):** No two introspection keys may
//!   share the same `IntrospectAttr.name`. Duplicates would break the
//!   FQ-to-short HTTP remap and schema output.
//!
//! ## Failure introspection invariants (FI-*)
//!
//! The FailureInfo presentation type lives in
//! `hyperactor_mesh::introspect`; these invariants are documented
//! here because the enforcement sites are in hyperactor (`proc.rs`
//! `serve()`, `live_actor_payload`).
//!
//! - **FI-1 (event-before-status):** All `InstanceCell` state that
//!   `live_actor_payload` reads must be written BEFORE
//!   `change_status()` transitions to terminal.
//! - **FI-2 (write-once):** `InstanceCellState::supervision_event` is
//!   written at most once per actor lifetime.
//! - **FI-3 (failure attrs <-> status):** Failure attrs are present
//!   iff status is `"failed"`.
//! - **FI-4 (is_propagated <-> root_cause_actor):**
//!   `failure_is_propagated == true` iff `failure_root_cause_actor !=
//!   this_actor_id`.
//! - **FI-5 (is_poisoned <-> failed_actor_count):** `is_poisoned ==
//!   true` iff `failed_actor_count > 0`.
//! - **FI-6 (clean stop = no artifacts):** When an actor stops
//!   cleanly, `supervision_event` is `None`, failure attrs are
//!   absent, and the actor does not contribute to
//!   `failed_actor_count`.
//! - **FI-7 (propagated-stopped-root-cause):** When a failed actor's
//!   supervision chain bottoms out in a `Stopped` child event,
//!   structured failure metadata must still name the stopped child as
//!   `failure_root_cause_actor`.
//! - **FI-8 (propagation-classification):** `failure_is_propagated`
//!   is derived from root-cause actor identity; a parent that failed
//!   due to a child's event must report `failure_is_propagated ==
//!   true`.
//!
//! ## Attrs view invariants (AV-*)
//!
//! These govern the typed view layer (`ActorAttrsView`). The full
//! AV-* / DP-* family is documented in `hyperactor_mesh::introspect`;
//! the subset relevant to this crate:
//!
//! - **AV-1 (view-roundtrip):** For each view V,
//!   `V::from_attrs(&v.to_attrs()) == Ok(v)`.
//! - **AV-2 (required-key-strictness):** `from_attrs` fails iff
//!   required keys for that view are missing.
//! - **AV-3 (unknown-key-tolerance):** Unknown attrs keys must not
//!   affect successful decode outcome.

use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;

use hyperactor_config::Attrs;
use hyperactor_config::INTROSPECT;
use hyperactor_config::IntrospectAttr;
use hyperactor_config::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::InstanceCell;
use crate::reference;

/// Typed reference to an introspectable entity.
///
/// This is the generic hyperactor layer — it knows about procs and
/// actors, not mesh-specific concepts like root or host.
///
/// Port references are intentionally excluded — introspection
/// does not address individual ports.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Named)]
pub enum IntrospectRef {
    /// A proc reference.
    Proc(reference::ProcId),
    /// An actor reference.
    Actor(reference::ActorId),
}
hyperactor_config::impl_attrvalue!(IntrospectRef);

impl fmt::Display for IntrospectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proc(id) => fmt::Display::fmt(id, f),
            Self::Actor(id) => fmt::Display::fmt(id, f),
        }
    }
}

impl FromStr for IntrospectRef {
    type Err = reference::ReferenceParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let r: reference::Reference = s.parse()?;
        match r {
            reference::Reference::Proc(id) => Ok(Self::Proc(id)),
            reference::Reference::Actor(id) => Ok(Self::Actor(id)),
            reference::Reference::Port(_) => Err(reference::ReferenceParsingError::WrongType(
                "port references are not valid introspection references".to_string(),
            )),
        }
    }
}

impl From<reference::ProcId> for IntrospectRef {
    fn from(id: reference::ProcId) -> Self {
        Self::Proc(id)
    }
}

impl From<reference::ActorId> for IntrospectRef {
    fn from(id: reference::ActorId) -> Self {
        Self::Actor(id)
    }
}

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

    /// Child references for tree navigation. Published by
    /// infrastructure actors (HostMeshAgent, ProcAgent) so the
    /// Entity view can return children without parsing mesh-layer keys.
    @meta(INTROSPECT = IntrospectAttr {
        name: "children".into(),
        desc: "Child references for tree navigation".into(),
    })
    pub attr CHILDREN: Vec<IntrospectRef>;

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
    pub attr FAILURE_ROOT_CAUSE_ACTOR: reference::ActorId;

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

// See FI-1 through FI-8 in module doc.

/// Error from decoding an `Attrs` bag into a typed view.
#[derive(Debug, Clone, PartialEq)]
pub enum AttrsViewError {
    /// A required key was absent (and has no default).
    MissingKey {
        /// The attr key that was absent.
        key: &'static str,
    },
    /// A cross-field coherence check failed.
    InvariantViolation {
        /// Invariant label (e.g. "IA-4").
        label: &'static str,
        /// Human-readable description of the violation.
        detail: String,
    },
}

impl fmt::Display for AttrsViewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKey { key } => write!(f, "missing required key: {key}"),
            Self::InvariantViolation { label, detail } => {
                write!(f, "invariant {label} violated: {detail}")
            }
        }
    }
}

impl std::error::Error for AttrsViewError {}

impl AttrsViewError {
    /// Convenience constructor for a missing required key.
    pub fn missing(key: &'static str) -> Self {
        Self::MissingKey { key }
    }

    /// Convenience constructor for an invariant violation.
    pub fn invariant(label: &'static str, detail: String) -> Self {
        Self::InvariantViolation { label, detail }
    }
}

/// Structured failure fields decoded from `FAILURE_*` attrs.
#[derive(Debug, Clone, PartialEq)]
pub struct FailureAttrs {
    /// Error message describing the failure.
    pub error_message: String,
    /// Actor that caused the failure (root cause).
    pub root_cause_actor: reference::ActorId,
    /// Display name of the root-cause actor, if available.
    pub root_cause_name: Option<String>,
    /// When the failure occurred.
    pub occurred_at: SystemTime,
    /// Whether this failure was propagated from a child.
    pub is_propagated: bool,
}

/// Typed view over attrs for an actor node.
#[derive(Debug, Clone, PartialEq)]
pub struct ActorAttrsView {
    /// Lifecycle status: "running", "stopped", "failed".
    pub status: String,
    /// Reason for stop/failure, if any.
    pub status_reason: Option<String>,
    /// Fully-qualified actor type name.
    pub actor_type: String,
    /// Number of messages processed.
    pub messages_processed: u64,
    /// When this actor was created.
    pub created_at: Option<SystemTime>,
    /// Name of the last message handler invoked.
    pub last_handler: Option<String>,
    /// Total CPU time in message handlers (microseconds).
    pub total_processing_time_us: u64,
    /// Flight recorder JSON, if available.
    pub flight_recorder: Option<String>,
    /// Whether this is a system/infrastructure actor.
    pub is_system: bool,
    /// Failure details, present iff status == "failed".
    pub failure: Option<FailureAttrs>,
}

impl ActorAttrsView {
    /// Decode from an `Attrs` bag (AV-2, AV-3). Requires `STATUS`
    /// and `ACTOR_TYPE`. Enforces IA-3 (status_reason must not be
    /// present for non-terminal status), IA-4 (failure attrs iff
    /// failed), and failure completeness (if any required failure
    /// key is present, all three required keys must be).
    pub fn from_attrs(attrs: &Attrs) -> Result<Self, AttrsViewError> {
        let status = attrs
            .get(STATUS)
            .ok_or_else(|| AttrsViewError::missing("status"))?
            .clone();
        let status_reason = attrs.get(STATUS_REASON).cloned();
        let actor_type = attrs
            .get(ACTOR_TYPE)
            .ok_or_else(|| AttrsViewError::missing("actor_type"))?
            .clone();
        let messages_processed = *attrs.get(MESSAGES_PROCESSED).unwrap_or(&0);
        let created_at = attrs.get(CREATED_AT).copied();
        let last_handler = attrs.get(LAST_HANDLER).cloned();
        let total_processing_time_us = *attrs.get(TOTAL_PROCESSING_TIME_US).unwrap_or(&0);
        let flight_recorder = attrs.get(FLIGHT_RECORDER).cloned();
        let is_system = *attrs.get(IS_SYSTEM).unwrap_or(&false);

        // IA-3 (one-sided): status_reason must not be present for
        // non-terminal status. The converse is not enforced —
        // terminal status without a reason is valid (clean shutdown).
        let is_terminal = status == "stopped" || status == "failed";
        if status_reason.is_some() && !is_terminal {
            return Err(AttrsViewError::invariant(
                "IA-3",
                format!(
                    "status_reason present but status is '{status}' (expected stopped or failed)"
                ),
            ));
        }

        // Decode failure attrs. If any of the three required
        // failure keys is present, require all three.
        // FAILURE_IS_PROPAGATED has a declare_attrs! default of
        // false, so it always resolves via attrs.get() and needs
        // no explicit presence check. FAILURE_ROOT_CAUSE_NAME is
        // genuinely optional.
        let has_any_failure = attrs.get(FAILURE_ERROR_MESSAGE).is_some()
            || attrs.get(FAILURE_ROOT_CAUSE_ACTOR).is_some()
            || attrs.get(FAILURE_OCCURRED_AT).is_some();

        let failure = if has_any_failure {
            let error_message = attrs
                .get(FAILURE_ERROR_MESSAGE)
                .ok_or_else(|| AttrsViewError::missing("failure_error_message"))?
                .clone();
            let root_cause_actor = attrs
                .get(FAILURE_ROOT_CAUSE_ACTOR)
                .ok_or_else(|| AttrsViewError::missing("failure_root_cause_actor"))?
                .clone();
            let root_cause_name = attrs.get(FAILURE_ROOT_CAUSE_NAME).cloned();
            let occurred_at = *attrs
                .get(FAILURE_OCCURRED_AT)
                .ok_or_else(|| AttrsViewError::missing("failure_occurred_at"))?;
            // Default false: failure originated at this actor.
            let is_propagated = *attrs.get(FAILURE_IS_PROPAGATED).unwrap_or(&false);
            Some(FailureAttrs {
                error_message,
                root_cause_actor,
                root_cause_name,
                occurred_at,
                is_propagated,
            })
        } else {
            None
        };

        // IA-4: failure attrs present iff status == "failed".
        if status == "failed" && failure.is_none() {
            return Err(AttrsViewError::invariant(
                "IA-4",
                "status is 'failed' but no failure_* attrs present".to_string(),
            ));
        }
        if status != "failed" && failure.is_some() {
            return Err(AttrsViewError::invariant(
                "IA-4",
                format!("status is '{status}' but failure_* attrs are present"),
            ));
        }

        Ok(Self {
            status,
            status_reason,
            actor_type,
            messages_processed,
            created_at,
            last_handler,
            total_processing_time_us,
            flight_recorder,
            is_system,
            failure,
        })
    }

    /// Encode into an `Attrs` bag (AV-1 round-trip producer).
    pub fn to_attrs(&self) -> Attrs {
        let mut attrs = Attrs::new();
        attrs.set(STATUS, self.status.clone());
        if let Some(reason) = &self.status_reason {
            attrs.set(STATUS_REASON, reason.clone());
        }
        attrs.set(ACTOR_TYPE, self.actor_type.clone());
        attrs.set(MESSAGES_PROCESSED, self.messages_processed);
        if let Some(t) = self.created_at {
            attrs.set(CREATED_AT, t);
        }
        if let Some(handler) = &self.last_handler {
            attrs.set(LAST_HANDLER, handler.clone());
        }
        attrs.set(TOTAL_PROCESSING_TIME_US, self.total_processing_time_us);
        if let Some(fr) = &self.flight_recorder {
            attrs.set(FLIGHT_RECORDER, fr.clone());
        }
        attrs.set(IS_SYSTEM, self.is_system);
        if let Some(fi) = &self.failure {
            attrs.set(FAILURE_ERROR_MESSAGE, fi.error_message.clone());
            attrs.set(FAILURE_ROOT_CAUSE_ACTOR, fi.root_cause_actor.clone());
            if let Some(name) = &fi.root_cause_name {
                attrs.set(FAILURE_ROOT_CAUSE_NAME, name.clone());
            }
            attrs.set(FAILURE_OCCURRED_AT, fi.occurred_at);
            attrs.set(FAILURE_IS_PROPAGATED, fi.is_propagated);
        }
        attrs
    }
}

/// Internal introspection result. Carries attrs as a JSON string.
/// The mesh layer constructs the API-facing `NodePayload` (with
/// `properties`) from this via `derive_properties`.
///
/// This is the internal wire type — it travels over actor ports
/// via `IntrospectMessage`. The presentation-layer `NodePayload`
/// (with `NodeProperties`) lives in `hyperactor_mesh::introspect`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct IntrospectResult {
    /// Reference identifying this node.
    pub identity: IntrospectRef,
    /// JSON-serialized `Attrs` bag containing introspection attributes.
    pub attrs: String,
    /// Child references the client can follow to descend the tree.
    pub children: Vec<IntrospectRef>,
    /// Parent reference for upward navigation.
    pub parent: Option<IntrospectRef>,
    /// When this data was captured.
    pub as_of: SystemTime,
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
    root_cause_actor: reference::ActorId,
    root_cause_name: Option<String>,
    occurred_at: SystemTime,
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
        attrs.set(FAILURE_OCCURRED_AT, fi.occurred_at);
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

    let children: Vec<IntrospectRef> = cell
        .child_actor_ids()
        .into_iter()
        .map(IntrospectRef::Actor)
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

    let supervisor = cell
        .parent()
        .map(|p| IntrospectRef::Actor(p.actor_id().clone()));

    // FI-3: failure_info is computed from the same status value as
    // actor_status, ensuring they agree on whether the actor failed.
    let failure = if status.is_failed() {
        cell.supervision_event().and_then(|event| {
            let root = event.actually_failing_actor()?;
            Some(FailureSnapshot {
                error_message: event.actor_status.to_string(),
                root_cause_actor: root.actor_id.clone(),
                root_cause_name: root.display_name.clone(),
                occurred_at: event.occurred_at,
                is_propagated: root.actor_id != *actor_id,
            })
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
        identity: IntrospectRef::Actor(actor_id.clone()),
        attrs,
        children,
        parent: supervisor,
        as_of: SystemTime::now(),
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
pub(crate) async fn serve_introspect(
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
                            let children: Vec<IntrospectRef> =
                                published.get(CHILDREN).cloned().unwrap_or_default();
                            IntrospectResult {
                                identity: IntrospectRef::Actor(cell.actor_id().clone()),
                                attrs: attrs_json,
                                children,
                                parent: cell
                                    .parent()
                                    .map(|p| IntrospectRef::Actor(p.actor_id().clone())),
                                as_of: SystemTime::now(),
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
                    // Use the queried child_ref as identity for the error node.
                    let identity = match &child_ref {
                        reference::Reference::Proc(id) => IntrospectRef::Proc(id.clone()),
                        reference::Reference::Actor(id) => IntrospectRef::Actor(id.clone()),
                        reference::Reference::Port(id) => {
                            IntrospectRef::Actor(id.actor_id().clone())
                        }
                    };
                    IntrospectResult {
                        identity,
                        attrs: serde_json::to_string(&error_attrs)
                            .unwrap_or_else(|_| "{}".to_string()),
                        children: Vec::new(),
                        parent: None,
                        as_of: SystemTime::now(),
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
    use crate::actor::ActorErrorKind;
    use crate::actor::ActorStatus;
    use crate::channel::ChannelAddr;
    use crate::reference::ProcId;
    use crate::supervision::ActorSupervisionEvent;

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

    // IA-1 tests require spawning actors and live in actor.rs
    // where #[hyperactor::export] and test infrastructure are
    // available. IA-3 and IA-4 are tested below at the view level.

    fn running_actor_attrs() -> Attrs {
        let mut attrs = Attrs::new();
        attrs.set(STATUS, "running".to_string());
        attrs.set(ACTOR_TYPE, "MyActor".to_string());
        attrs.set(MESSAGES_PROCESSED, 42u64);
        attrs.set(CREATED_AT, SystemTime::UNIX_EPOCH);
        attrs.set(IS_SYSTEM, false);
        attrs
    }

    fn test_actor_id(proc_name: &str, actor_name: &str, pid: usize) -> crate::reference::ActorId {
        ProcId::with_name(ChannelAddr::Local(0), proc_name).actor_id(actor_name, pid)
    }

    fn failed_actor_attrs() -> Attrs {
        let mut attrs = running_actor_attrs();
        attrs.set(STATUS, "failed".to_string());
        attrs.set(STATUS_REASON, "something broke".to_string());
        attrs.set(FAILURE_ERROR_MESSAGE, "boom".to_string());
        attrs.set(FAILURE_ROOT_CAUSE_ACTOR, test_actor_id("proc", "other", 0));
        attrs.set(FAILURE_ROOT_CAUSE_NAME, "OtherActor".to_string());
        attrs.set(FAILURE_OCCURRED_AT, SystemTime::UNIX_EPOCH);
        attrs.set(FAILURE_IS_PROPAGATED, true);
        attrs
    }

    /// AV-1: from_attrs(to_attrs(v)) == v.
    #[test]
    fn test_actor_view_round_trip_running() {
        let view = ActorAttrsView::from_attrs(&running_actor_attrs()).unwrap();
        assert_eq!(view.status, "running");
        assert_eq!(view.actor_type, "MyActor");
        assert_eq!(view.messages_processed, 42);
        assert!(view.failure.is_none());

        let round_tripped = ActorAttrsView::from_attrs(&view.to_attrs()).unwrap();
        assert_eq!(round_tripped, view);
    }

    /// AV-1.
    #[test]
    fn test_actor_view_round_trip_failed() {
        let view = ActorAttrsView::from_attrs(&failed_actor_attrs()).unwrap();
        assert_eq!(view.status, "failed");
        let fi = view.failure.as_ref().unwrap();
        assert_eq!(fi.error_message, "boom");
        assert!(fi.is_propagated);

        let round_tripped = ActorAttrsView::from_attrs(&view.to_attrs()).unwrap();
        assert_eq!(round_tripped, view);
    }

    /// AV-2: missing required key rejected.
    #[test]
    fn test_actor_view_missing_status() {
        let mut attrs = Attrs::new();
        attrs.set(ACTOR_TYPE, "X".to_string());
        let err = ActorAttrsView::from_attrs(&attrs).unwrap_err();
        assert_eq!(err, AttrsViewError::MissingKey { key: "status" });
    }

    /// AV-2.
    #[test]
    fn test_actor_view_missing_actor_type() {
        let mut attrs = Attrs::new();
        attrs.set(STATUS, "running".to_string());
        let err = ActorAttrsView::from_attrs(&attrs).unwrap_err();
        assert_eq!(err, AttrsViewError::MissingKey { key: "actor_type" });
    }

    #[test]
    fn test_actor_view_ia3_rejects_reason_on_running() {
        let mut attrs = running_actor_attrs();
        attrs.set(STATUS_REASON, "should not be here".to_string());
        let err = ActorAttrsView::from_attrs(&attrs).unwrap_err();
        assert!(matches!(
            err,
            AttrsViewError::InvariantViolation { label: "IA-3", .. }
        ));
    }

    #[test]
    fn test_actor_view_ia3_allows_terminal_without_reason() {
        let mut attrs = running_actor_attrs();
        attrs.set(STATUS, "stopped".to_string());
        // No status_reason — should be fine.
        let view = ActorAttrsView::from_attrs(&attrs).unwrap();
        assert_eq!(view.status, "stopped");
        assert!(view.status_reason.is_none());
    }

    #[test]
    fn test_actor_view_ia4_rejects_failed_without_failure_attrs() {
        let mut attrs = running_actor_attrs();
        attrs.set(STATUS, "failed".to_string());
        // No failure_* keys.
        let err = ActorAttrsView::from_attrs(&attrs).unwrap_err();
        assert!(matches!(
            err,
            AttrsViewError::InvariantViolation { label: "IA-4", .. }
        ));
    }

    #[test]
    fn test_actor_view_ia4_rejects_failure_attrs_on_running() {
        let mut attrs = running_actor_attrs();
        attrs.set(FAILURE_ERROR_MESSAGE, "boom".to_string());
        attrs.set(FAILURE_ROOT_CAUSE_ACTOR, test_actor_id("proc", "x", 0));
        attrs.set(FAILURE_OCCURRED_AT, SystemTime::UNIX_EPOCH);
        let err = ActorAttrsView::from_attrs(&attrs).unwrap_err();
        assert!(matches!(
            err,
            AttrsViewError::InvariantViolation { label: "IA-4", .. }
        ));
    }

    /// AV-2: partial failure set → missing key.
    #[test]
    fn test_actor_view_partial_failure_attrs_rejected() {
        let mut attrs = running_actor_attrs();
        attrs.set(STATUS, "failed".to_string());
        // Only one of the three required failure keys.
        attrs.set(FAILURE_ERROR_MESSAGE, "boom".to_string());
        let err = ActorAttrsView::from_attrs(&attrs).unwrap_err();
        assert_eq!(
            err,
            AttrsViewError::MissingKey {
                key: "failure_root_cause_actor"
            }
        );
    }

    /// Exercises FI-7 and FI-8 (see module doc): when a parent fails
    /// due to an unhandled Stopped child event, structured failure
    /// attrs must name the stopped child as
    /// `failure_root_cause_actor` (FI-7) and report
    /// `failure_is_propagated == true` (FI-8).
    ///
    /// Partially white-box: re-creates `FailureSnapshot` construction
    /// from `live_actor_payload` because that function requires an
    /// `InstanceCell`. This test will fail if
    /// `actually_failing_actor()` regresses, because that helper is
    /// the shared decision point for root-cause attribution. See
    /// `test_propagated_failure_info` in `proc.rs` for end-to-end
    /// integration coverage.
    #[test]
    fn test_fi7_fi8_propagated_stopped_child() {
        let proc_id = ProcId::with_name(ChannelAddr::Local(0), "test_proc");
        let child_id = proc_id.actor_id("proc_agent", 0);
        let parent_id = proc_id.actor_id("mesh_actor", 0);

        let child_event = ActorSupervisionEvent::new(
            child_id.clone(),
            Some("proc_agent".into()),
            ActorStatus::Stopped("host died".into()),
            None,
        );
        let parent_event = ActorSupervisionEvent::new(
            parent_id.clone(),
            Some("mesh_actor".into()),
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(
                child_event,
            ))),
            None,
        );

        // -- reproduce FailureSnapshot construction (same logic as
        // live_actor_payload) --
        let root = parent_event
            .actually_failing_actor()
            .expect("parent_event is a failure");
        let snap = FailureSnapshot {
            error_message: parent_event.actor_status.to_string(),
            root_cause_actor: root.actor_id.clone(),
            root_cause_name: root.display_name.clone(),
            occurred_at: parent_event.occurred_at,
            is_propagated: root.actor_id != parent_id,
        };

        // FI-7: failure_root_cause_actor is the stopped child.
        assert_eq!(snap.root_cause_actor, child_id);
        // FI-8: failure_is_propagated is true.
        assert!(snap.is_propagated);
        // root_cause_name pinned before round-trip.
        assert_eq!(snap.root_cause_name.as_deref(), Some("proc_agent"));

        // -- attrs round-trip through ActorAttrsView --
        let mut attrs = failed_actor_attrs();
        attrs.set(FAILURE_ERROR_MESSAGE, snap.error_message);
        attrs.set(FAILURE_ROOT_CAUSE_ACTOR, snap.root_cause_actor.clone());
        if let Some(name) = &snap.root_cause_name {
            attrs.set(FAILURE_ROOT_CAUSE_NAME, name.clone());
        }
        attrs.set(FAILURE_OCCURRED_AT, snap.occurred_at);
        attrs.set(FAILURE_IS_PROPAGATED, snap.is_propagated);

        let view = ActorAttrsView::from_attrs(&attrs).unwrap();
        assert_eq!(view.status, "failed");
        let fi = view.failure.as_ref().expect("failure_info must be present");
        // FI-7: failure_root_cause_actor survives attrs round-trip.
        assert_eq!(fi.root_cause_actor, child_id);
        // FI-8: failure_is_propagated survives attrs round-trip.
        assert!(fi.is_propagated);
        // root_cause_name also survives.
        assert_eq!(fi.root_cause_name.as_deref(), Some("proc_agent"));
    }
}
