/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Introspection attr keys — mesh-topology concepts.
//!
//! These keys are published by `HostMeshAgent`, `ProcAgent`, and
//! `MeshAdminAgent` to describe mesh topology (hosts, procs, root).
//! Actor-runtime keys (status, actor_type, messages_processed, etc.)
//! are declared in `hyperactor::introspect`.
//!
//! See `hyperactor::introspect` for naming convention, invariant
//! labels, and the `IntrospectAttr` meta-attribute pattern.
//!
//! ## Mesh key invariants (MK-*)
//!
//! - **MK-1 (metadata completeness):** Every mesh-topology
//!   introspection key must carry `@meta(INTROSPECT = ...)` with
//!   non-empty `name` and `desc`.
//! - **MK-2 (short-name uniqueness):** Covered by
//!   `test_introspect_short_names_are_globally_unique` in
//!   `hyperactor::introspect` (cross-crate).
//!
//! ## Attrs invariants (IA-*)
//!
//! These govern how `IntrospectResult.attrs` is built in
//! `hyperactor::introspect` and how `properties` is derived via
//! `derive_properties`.
//!
//! - **IA-1 (attrs-json):** `IntrospectResult.attrs` is always a
//!   valid JSON object string.
//! - **IA-2 (runtime-precedence):** Runtime-owned introspection keys
//!   override any same-named keys in published attrs.
//! - **IA-3 (status-shape):** `status_reason` is present in attrs
//!   iff the status string carries a reason.
//! - **IA-4 (failure-shape):** `failure_*` attrs are present iff
//!   effective status is `failed`.
//! - **IA-5 (payload-totality):** Every `IntrospectResult` sets
//!   `attrs` -- never omitted, never null.

use hyperactor_config::INTROSPECT;
use hyperactor_config::IntrospectAttr;
use hyperactor_config::declare_attrs;

// See MK-1, MK-2, IA-1..IA-5 in module doc.
declare_attrs! {
    /// Topology role of this node: "root", "host", "proc", "error".
    @meta(INTROSPECT = IntrospectAttr {
        name: "node_type".into(),
        desc: "Topology role: root, host, proc, error".into(),
    })
    pub attr NODE_TYPE: String;

    /// Host network address (e.g. "10.0.0.1:8080").
    @meta(INTROSPECT = IntrospectAttr {
        name: "addr".into(),
        desc: "Host network address".into(),
    })
    pub attr ADDR: String;

    /// Number of procs on a host.
    @meta(INTROSPECT = IntrospectAttr {
        name: "num_procs".into(),
        desc: "Number of procs on a host".into(),
    })
    pub attr NUM_PROCS: usize = 0;

    /// Human-readable proc name.
    @meta(INTROSPECT = IntrospectAttr {
        name: "proc_name".into(),
        desc: "Human-readable proc name".into(),
    })
    pub attr PROC_NAME: String;

    /// Number of actors in a proc.
    @meta(INTROSPECT = IntrospectAttr {
        name: "num_actors".into(),
        desc: "Number of actors in a proc".into(),
    })
    pub attr NUM_ACTORS: usize = 0;

    /// References of system/infrastructure children.
    @meta(INTROSPECT = IntrospectAttr {
        name: "system_children".into(),
        desc: "References of system/infrastructure children".into(),
    })
    pub attr SYSTEM_CHILDREN: Vec<String>;

    /// References of stopped children (proc only).
    @meta(INTROSPECT = IntrospectAttr {
        name: "stopped_children".into(),
        desc: "References of stopped children".into(),
    })
    pub attr STOPPED_CHILDREN: Vec<String>;

    /// Cap on stopped children retention.
    @meta(INTROSPECT = IntrospectAttr {
        name: "stopped_retention_cap".into(),
        desc: "Maximum number of stopped children retained".into(),
    })
    pub attr STOPPED_RETENTION_CAP: usize = 0;

    /// Whether this proc is refusing new spawns due to actor
    /// failures.
    @meta(INTROSPECT = IntrospectAttr {
        name: "is_poisoned".into(),
        desc: "Whether this proc is poisoned (refusing new spawns)".into(),
    })
    pub attr IS_POISONED: bool = false;

    /// Count of failed actors in a proc.
    @meta(INTROSPECT = IntrospectAttr {
        name: "failed_actor_count".into(),
        desc: "Number of failed actors in this proc".into(),
    })
    pub attr FAILED_ACTOR_COUNT: usize = 0;

    /// Timestamp when the mesh was started.
    @meta(INTROSPECT = IntrospectAttr {
        name: "started_at".into(),
        desc: "Timestamp when the mesh was started".into(),
    })
    pub attr STARTED_AT: std::time::SystemTime;

    /// Username who started the mesh.
    @meta(INTROSPECT = IntrospectAttr {
        name: "started_by".into(),
        desc: "Username who started the mesh".into(),
    })
    pub attr STARTED_BY: String;

    /// Number of hosts in the mesh (root only).
    @meta(INTROSPECT = IntrospectAttr {
        name: "num_hosts".into(),
        desc: "Number of hosts in the mesh".into(),
    })
    pub attr NUM_HOSTS: usize = 0;

}

// --- API / presentation types ---
//
// These types define the HTTP response shape for
// GET /v1/{reference}. Derived from internal attrs at the
// mesh boundary. Shared with the TUI.

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Uniform response for any node in the mesh topology.
///
/// Every addressable entity (root, host, proc, actor) is represented
/// as a `NodePayload`. The client navigates the mesh by fetching a
/// node and following its `children` references.
///
/// See IA-1..IA-5 in module doc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct NodePayload {
    /// Canonical reference string for this node.
    pub identity: String,
    /// Node-specific metadata (type, status, metrics, etc.).
    pub properties: NodeProperties,
    /// Reference strings the client can GET next to descend the tree.
    pub children: Vec<String>,
    /// Parent node reference for upward navigation.
    pub parent: Option<String>,
    /// ISO 8601 timestamp indicating when this data was captured.
    pub as_of: String,
}
wirevalue::register_type!(NodePayload);

/// Node-specific metadata. Externally-tagged enum — the JSON
/// key is the variant name (Root, Host, Proc, Actor, Error).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum NodeProperties {
    /// Synthetic mesh root node (not a real actor/proc).
    Root {
        num_hosts: usize,
        started_at: String,
        started_by: String,
        system_children: Vec<String>,
    },
    /// A host in the mesh, represented by its `HostAgent`.
    Host {
        addr: String,
        num_procs: usize,
        system_children: Vec<String>,
    },
    /// Properties describing a proc running on a host.
    Proc {
        proc_name: String,
        num_actors: usize,
        system_children: Vec<String>,
        stopped_children: Vec<String>,
        stopped_retention_cap: usize,
        is_poisoned: bool,
        failed_actor_count: usize,
    },
    /// Runtime metadata for a single actor instance.
    Actor {
        actor_status: String,
        actor_type: String,
        messages_processed: u64,
        created_at: String,
        last_message_handler: Option<String>,
        total_processing_time_us: u64,
        flight_recorder: Option<String>,
        is_system: bool,
        failure_info: Option<FailureInfo>,
    },
    /// Error sentinel returned when a child reference cannot be resolved.
    Error { code: String, message: String },
}
wirevalue::register_type!(NodeProperties);

/// Structured failure information for failed actors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct FailureInfo {
    pub error_message: String,
    pub root_cause_actor: String,
    pub root_cause_name: Option<String>,
    pub occurred_at: String,
    pub is_propagated: bool,
}
wirevalue::register_type!(FailureInfo);

/// Derive `NodeProperties` from a JSON-serialized attrs string.
///
/// Detection precedence (DP-1):
/// 1. `node_type` = "root" / "host" / "proc" → corresponding variant
/// 2. `error_code` present → Error
/// 3. `STATUS` key present → Actor
/// 4. none of the above → Error("unknown_node_type")
pub fn derive_properties(attrs_json: &str) -> NodeProperties {
    let attrs: hyperactor_config::Attrs = match serde_json::from_str(attrs_json) {
        Ok(a) => a,
        Err(_) => {
            return NodeProperties::Error {
                code: "parse_error".into(),
                message: "failed to parse attrs JSON".into(),
            };
        }
    };

    let node_type = attrs.get(NODE_TYPE).cloned().unwrap_or_default();

    match node_type.as_str() {
        "root" => NodeProperties::Root {
            num_hosts: attrs.get(NUM_HOSTS).copied().unwrap_or(0),
            started_at: attrs
                .get(STARTED_AT)
                .map(|t| humantime::format_rfc3339_millis(*t).to_string())
                .unwrap_or_default(),
            started_by: attrs.get(STARTED_BY).cloned().unwrap_or_default(),
            system_children: attrs.get(SYSTEM_CHILDREN).cloned().unwrap_or_default(),
        },
        "host" => NodeProperties::Host {
            addr: attrs.get(ADDR).cloned().unwrap_or_default(),
            num_procs: attrs.get(NUM_PROCS).copied().unwrap_or(0),
            system_children: attrs.get(SYSTEM_CHILDREN).cloned().unwrap_or_default(),
        },
        "proc" => NodeProperties::Proc {
            proc_name: attrs.get(PROC_NAME).cloned().unwrap_or_default(),
            num_actors: attrs.get(NUM_ACTORS).copied().unwrap_or(0),
            system_children: attrs.get(SYSTEM_CHILDREN).cloned().unwrap_or_default(),
            stopped_children: attrs.get(STOPPED_CHILDREN).cloned().unwrap_or_default(),
            stopped_retention_cap: attrs.get(STOPPED_RETENTION_CAP).copied().unwrap_or(0),
            is_poisoned: attrs.get(IS_POISONED).copied().unwrap_or(false),
            failed_actor_count: attrs.get(FAILED_ACTOR_COUNT).copied().unwrap_or(0),
        },
        _ => {
            // DP-1: error_code → Error, STATUS present → Actor,
            // else → Error("unknown_node_type")
            use hyperactor::introspect::ERROR_CODE;
            use hyperactor::introspect::ERROR_MESSAGE;
            if let Some(code) = attrs.get(ERROR_CODE) {
                return NodeProperties::Error {
                    code: code.clone(),
                    message: attrs.get(ERROR_MESSAGE).cloned().unwrap_or_default(),
                };
            }

            use hyperactor::introspect::ACTOR_TYPE;
            use hyperactor::introspect::CREATED_AT;
            use hyperactor::introspect::FAILURE_ERROR_MESSAGE;
            use hyperactor::introspect::FAILURE_IS_PROPAGATED;
            use hyperactor::introspect::FAILURE_OCCURRED_AT;
            use hyperactor::introspect::FAILURE_ROOT_CAUSE_ACTOR;
            use hyperactor::introspect::FAILURE_ROOT_CAUSE_NAME;
            use hyperactor::introspect::FLIGHT_RECORDER;
            use hyperactor::introspect::IS_SYSTEM;
            use hyperactor::introspect::LAST_HANDLER;
            use hyperactor::introspect::MESSAGES_PROCESSED;
            use hyperactor::introspect::STATUS;
            use hyperactor::introspect::STATUS_REASON;
            use hyperactor::introspect::TOTAL_PROCESSING_TIME_US;

            if attrs.get(STATUS).is_none() {
                return NodeProperties::Error {
                    code: "unknown_node_type".into(),
                    message: format!("unrecognized node_type: {:?}", node_type),
                };
            }

            // Reconstruct actor_status from status + status_reason.
            let status = attrs.get(STATUS).cloned().unwrap_or_default();
            let actor_status = match attrs.get(STATUS_REASON) {
                Some(reason) => format!("{}: {}", status, reason),
                None => status,
            };

            let failure_info = attrs.get(FAILURE_ERROR_MESSAGE).map(|err_msg| FailureInfo {
                error_message: err_msg.clone(),
                root_cause_actor: attrs
                    .get(FAILURE_ROOT_CAUSE_ACTOR)
                    .cloned()
                    .unwrap_or_default(),
                root_cause_name: attrs.get(FAILURE_ROOT_CAUSE_NAME).cloned(),
                occurred_at: attrs
                    .get(FAILURE_OCCURRED_AT)
                    .map(|t| humantime::format_rfc3339_millis(*t).to_string())
                    .unwrap_or_default(),
                is_propagated: attrs.get(FAILURE_IS_PROPAGATED).copied().unwrap_or(false),
            });

            NodeProperties::Actor {
                actor_status,
                actor_type: attrs.get(ACTOR_TYPE).cloned().unwrap_or_default(),
                messages_processed: attrs.get(MESSAGES_PROCESSED).copied().unwrap_or(0),
                created_at: attrs
                    .get(CREATED_AT)
                    .map(|t| humantime::format_rfc3339_millis(*t).to_string())
                    .unwrap_or_default(),
                last_message_handler: attrs.get(LAST_HANDLER).cloned(),
                total_processing_time_us: attrs.get(TOTAL_PROCESSING_TIME_US).copied().unwrap_or(0),
                flight_recorder: attrs.get(FLIGHT_RECORDER).cloned(),
                is_system: attrs.get(IS_SYSTEM).copied().unwrap_or(false),
                failure_info,
            }
        }
    }
}

/// Convert an internal `IntrospectResult` into an API-facing
/// `NodePayload` by deriving `properties` from attrs.
/// Convert an `IntrospectResult` to a presentation `NodePayload`
/// with `properties` derived from attrs.
pub fn to_node_payload(result: hyperactor::introspect::IntrospectResult) -> NodePayload {
    NodePayload {
        identity: result.identity,
        properties: derive_properties(&result.attrs),
        children: result.children,
        parent: result.parent,
        as_of: result.as_of,
    }
}

/// Convert an `IntrospectResult` to a `NodePayload`, overriding
/// identity and parent for correct tree navigation.
pub fn to_node_payload_with(
    result: hyperactor::introspect::IntrospectResult,
    identity: String,
    parent: Option<String>,
) -> NodePayload {
    NodePayload {
        identity,
        properties: derive_properties(&result.attrs),
        children: result.children,
        parent,
        as_of: result.as_of,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Enforces MK-1 (metadata completeness) for all mesh-topology
    /// introspection keys.
    #[test]
    fn test_mesh_introspect_keys_are_tagged() {
        let cases = vec![
            ("node_type", NODE_TYPE.attrs()),
            ("addr", ADDR.attrs()),
            ("num_procs", NUM_PROCS.attrs()),
            ("proc_name", PROC_NAME.attrs()),
            ("num_actors", NUM_ACTORS.attrs()),
            ("system_children", SYSTEM_CHILDREN.attrs()),
            ("stopped_children", STOPPED_CHILDREN.attrs()),
            ("stopped_retention_cap", STOPPED_RETENTION_CAP.attrs()),
            ("is_poisoned", IS_POISONED.attrs()),
            ("failed_actor_count", FAILED_ACTOR_COUNT.attrs()),
            ("started_at", STARTED_AT.attrs()),
            ("started_by", STARTED_BY.attrs()),
            ("num_hosts", NUM_HOSTS.attrs()),
        ];

        for (expected_name, meta) in &cases {
            // MK-1: every key must have INTROSPECT with non-empty
            // name and desc.
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
                info.name.starts_with("hyperactor_mesh::introspect::")
                    && info.meta.get(INTROSPECT).is_some()
            })
            .count();
        assert_eq!(
            cases.len(),
            registry_count,
            "test must cover all INTROSPECT-tagged keys in this module"
        );
    }
}
