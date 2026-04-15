/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP boundary DTO types for mesh-admin introspection.
//!
//! These types own the HTTP JSON wire contract. Domain types
//! (`NodePayload`, `NodeProperties`, `FailureInfo`) stay clean of
//! HTTP serialization concerns; conversion happens at the boundary
//! via `From` / `TryFrom` impls defined here.
//!
//! ## Invariants
//!
//! - **HB-1 (typed-internal, string-external):** `NodeRef`, `ActorId`,
//!   `ProcId`, and `SystemTime` are encoded as canonical strings in the
//!   DTO types.
//! - **HB-2 (round-trip):** `NodePayload → NodePayloadDto → NodePayload`
//!   is lossless for values representable in the wire format.
//!   Timestamps are formatted at millisecond precision
//!   (`humantime::format_rfc3339_millis`), matching the established
//!   HTTP contract; sub-millisecond precision is truncated at the
//!   boundary.
//! - **HB-3 (schema-honesty):** Schema/OpenAPI are generated from these
//!   DTO types, so the published schema reflects the actual wire format.

use std::time::SystemTime;

use anyhow::Context;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use super::FailureInfo;
use super::NodePayload;
use super::NodeProperties;
use super::NodeRef;

// DTO struct definitions

/// Uniform response for any node in the mesh topology.
///
/// Every addressable entity (root, host, proc, actor) is represented
/// as a `NodePayload`. The client navigates the mesh by fetching a
/// node and following its `children` references.
///
/// `identity`, `children`, and `parent` are plain reference strings.
/// `as_of` is an ISO 8601 timestamp string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(title = "NodePayload")]
pub struct NodePayloadDto {
    /// Canonical node reference identifying this node.
    pub identity: String,
    /// Node-specific metadata (type, status, metrics, etc.).
    pub properties: NodePropertiesDto,
    /// Child node reference strings the client can URL-encode and
    /// fetch via `GET /v1/{reference}`.
    pub children: Vec<String>,
    /// Parent node reference for upward navigation.
    pub parent: Option<String>,
    /// When this payload was captured (ISO 8601 timestamp string).
    pub as_of: String,
}

/// Memory stats of the hosting OS process (DTO mirror of
/// `ProcessMemoryStats`).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    JsonSchema
)]
#[schemars(rename = "ProcessMemoryStats")]
pub struct ProcessMemoryStatsDto {
    /// RSS of the hosting OS process (bytes).
    pub process_rss_bytes: Option<u64>,
    /// Virtual memory size of the hosting OS process (bytes).
    pub process_vm_size_bytes: Option<u64>,
}

/// Proc-level debug/operational stats (DTO mirror of
/// `ProcDebugStats`).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    JsonSchema
)]
#[schemars(rename = "ProcDebugStats")]
pub struct ProcDebugStatsDto {
    /// Hosting-process memory.
    pub memory: ProcessMemoryStatsDto,
    /// Sum of per-actor queue depths (live actors only).
    pub actor_work_queue_depth_total: u64,
    /// Max per-actor queue depth (live actors only).
    pub actor_work_queue_depth_max: u64,
}

/// Node-specific metadata. Externally-tagged enum — the JSON
/// key is the variant name (Root, Host, Proc, Actor, Error).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "NodeProperties")]
pub enum NodePropertiesDto {
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
        /// Hosting-process memory stats.
        memory: ProcessMemoryStatsDto,
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
        /// Runtime debug/operational stats.
        debug: ProcDebugStatsDto,
    },
    /// Runtime metadata for a single actor instance.
    Actor {
        actor_status: String,
        actor_type: String,
        messages_processed: u64,
        created_at: Option<String>,
        last_message_handler: Option<String>,
        total_processing_time_us: u64,
        flight_recorder: Option<String>,
        is_system: bool,
        failure_info: Option<FailureInfoDto>,
    },
    /// Error sentinel returned when a child reference cannot be resolved.
    Error { code: String, message: String },
}

/// Structured failure information for failed actors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FailureInfo")]
pub struct FailureInfoDto {
    /// Error message describing the failure.
    pub error_message: String,
    /// Actor that caused the failure (root cause).
    pub root_cause_actor: String,
    /// Display name of the root-cause actor, if available.
    pub root_cause_name: Option<String>,
    /// When the failure occurred (ISO 8601 timestamp string).
    pub occurred_at: String,
    /// Whether this failure was propagated from a child.
    pub is_propagated: bool,
}

// Helpers

fn format_time(t: &SystemTime) -> String {
    humantime::format_rfc3339_millis(*t).to_string()
}

fn refs_to_strings(refs: &[NodeRef]) -> Vec<String> {
    refs.iter().map(|r| r.to_string()).collect()
}

fn parse_refs(field: &str, strings: &[String]) -> anyhow::Result<Vec<NodeRef>> {
    strings
        .iter()
        .enumerate()
        .map(|(i, s)| {
            s.parse()
                .with_context(|| format!("failed to parse {field}[{i}]: {s:?}"))
        })
        .collect()
}

// Domain → DTO conversions (infallible)

impl From<NodePayload> for NodePayloadDto {
    fn from(p: NodePayload) -> Self {
        Self {
            identity: p.identity.to_string(),
            properties: p.properties.into(),
            children: refs_to_strings(&p.children),
            parent: p.parent.as_ref().map(|r| r.to_string()),
            as_of: format_time(&p.as_of),
        }
    }
}

impl From<NodeProperties> for NodePropertiesDto {
    fn from(p: NodeProperties) -> Self {
        match p {
            NodeProperties::Root {
                num_hosts,
                started_at,
                started_by,
                system_children,
            } => Self::Root {
                num_hosts,
                started_at: format_time(&started_at),
                started_by,
                system_children: refs_to_strings(&system_children),
            },
            NodeProperties::Host {
                addr,
                num_procs,
                system_children,
                memory,
            } => Self::Host {
                addr,
                num_procs,
                system_children: refs_to_strings(&system_children),
                memory: ProcessMemoryStatsDto {
                    process_rss_bytes: memory.process_rss_bytes,
                    process_vm_size_bytes: memory.process_vm_size_bytes,
                },
            },
            NodeProperties::Proc {
                proc_name,
                num_actors,
                system_children,
                stopped_children,
                stopped_retention_cap,
                is_poisoned,
                failed_actor_count,
                debug,
            } => Self::Proc {
                proc_name,
                num_actors,
                system_children: refs_to_strings(&system_children),
                stopped_children: refs_to_strings(&stopped_children),
                stopped_retention_cap,
                is_poisoned,
                failed_actor_count,
                debug: ProcDebugStatsDto {
                    memory: ProcessMemoryStatsDto {
                        process_rss_bytes: debug.memory.process_rss_bytes,
                        process_vm_size_bytes: debug.memory.process_vm_size_bytes,
                    },
                    actor_work_queue_depth_total: debug.actor_work_queue_depth_total,
                    actor_work_queue_depth_max: debug.actor_work_queue_depth_max,
                },
            },
            NodeProperties::Actor {
                actor_status,
                actor_type,
                messages_processed,
                created_at,
                last_message_handler,
                total_processing_time_us,
                flight_recorder,
                is_system,
                failure_info,
            } => Self::Actor {
                actor_status,
                actor_type,
                messages_processed,
                created_at: created_at.as_ref().map(format_time),
                last_message_handler,
                total_processing_time_us,
                flight_recorder,
                is_system,
                failure_info: failure_info.map(Into::into),
            },
            NodeProperties::Error { code, message } => Self::Error { code, message },
        }
    }
}

impl From<FailureInfo> for FailureInfoDto {
    fn from(f: FailureInfo) -> Self {
        Self {
            error_message: f.error_message,
            root_cause_actor: f.root_cause_actor.to_string(),
            root_cause_name: f.root_cause_name,
            occurred_at: format_time(&f.occurred_at),
            is_propagated: f.is_propagated,
        }
    }
}

// DTO → Domain conversions (fallible)

impl TryFrom<NodePayloadDto> for NodePayload {
    type Error = anyhow::Error;

    fn try_from(dto: NodePayloadDto) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: dto
                .identity
                .parse()
                .with_context(|| format!("failed to parse identity: {:?}", dto.identity))?,
            properties: dto
                .properties
                .try_into()
                .context("failed to parse properties")?,
            children: parse_refs("children", &dto.children)?,
            parent: dto
                .parent
                .map(|s| {
                    s.parse()
                        .with_context(|| format!("failed to parse parent: {s:?}"))
                })
                .transpose()?,
            as_of: humantime::parse_rfc3339(&dto.as_of)
                .with_context(|| format!("failed to parse as_of: {:?}", dto.as_of))?,
        })
    }
}

impl TryFrom<NodePropertiesDto> for NodeProperties {
    type Error = anyhow::Error;

    fn try_from(
        dto: NodePropertiesDto,
    ) -> Result<Self, <Self as TryFrom<NodePropertiesDto>>::Error> {
        Ok(match dto {
            NodePropertiesDto::Root {
                num_hosts,
                started_at,
                started_by,
                system_children,
            } => Self::Root {
                num_hosts,
                started_at: humantime::parse_rfc3339(&started_at)
                    .context("failed to parse Root.started_at")?,
                started_by,
                system_children: parse_refs("Root.system_children", &system_children)?,
            },
            NodePropertiesDto::Host {
                addr,
                num_procs,
                system_children,
                memory,
            } => Self::Host {
                addr,
                num_procs,
                system_children: parse_refs("Host.system_children", &system_children)?,
                memory: super::ProcessMemoryStats {
                    process_rss_bytes: memory.process_rss_bytes,
                    process_vm_size_bytes: memory.process_vm_size_bytes,
                },
            },
            NodePropertiesDto::Proc {
                proc_name,
                num_actors,
                system_children,
                stopped_children,
                stopped_retention_cap,
                is_poisoned,
                failed_actor_count,
                debug,
            } => Self::Proc {
                proc_name,
                num_actors,
                system_children: parse_refs("Proc.system_children", &system_children)?,
                stopped_children: parse_refs("Proc.stopped_children", &stopped_children)?,
                stopped_retention_cap,
                is_poisoned,
                failed_actor_count,
                debug: super::ProcDebugStats {
                    memory: super::ProcessMemoryStats {
                        process_rss_bytes: debug.memory.process_rss_bytes,
                        process_vm_size_bytes: debug.memory.process_vm_size_bytes,
                    },
                    actor_work_queue_depth_total: debug.actor_work_queue_depth_total,
                    actor_work_queue_depth_max: debug.actor_work_queue_depth_max,
                },
            },
            NodePropertiesDto::Actor {
                actor_status,
                actor_type,
                messages_processed,
                created_at,
                last_message_handler,
                total_processing_time_us,
                flight_recorder,
                is_system,
                failure_info,
            } => Self::Actor {
                actor_status,
                actor_type,
                messages_processed,
                created_at: created_at
                    .map(|s| {
                        humantime::parse_rfc3339(&s)
                            .with_context(|| format!("failed to parse Actor.created_at: {s:?}"))
                    })
                    .transpose()?,
                last_message_handler,
                total_processing_time_us,
                flight_recorder,
                is_system,
                failure_info: failure_info
                    .map(TryInto::try_into)
                    .transpose()
                    .context("failed to parse Actor.failure_info")?,
            },
            NodePropertiesDto::Error { code, message } => Self::Error { code, message },
        })
    }
}

impl TryFrom<FailureInfoDto> for FailureInfo {
    type Error = anyhow::Error;

    fn try_from(dto: FailureInfoDto) -> Result<Self, Self::Error> {
        Ok(Self {
            error_message: dto.error_message,
            root_cause_actor: dto.root_cause_actor.parse().with_context(|| {
                format!(
                    "failed to parse FailureInfo.root_cause_actor: {:?}",
                    dto.root_cause_actor
                )
            })?,
            root_cause_name: dto.root_cause_name,
            occurred_at: humantime::parse_rfc3339(&dto.occurred_at).with_context(|| {
                format!(
                    "failed to parse FailureInfo.occurred_at: {:?}",
                    dto.occurred_at
                )
            })?,
            is_propagated: dto.is_propagated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test fixtures

    fn test_proc_id() -> hyperactor::reference::ProcId {
        hyperactor::reference::ProcId::with_name(
            hyperactor::channel::ChannelAddr::Local(0),
            "worker",
        )
    }

    fn test_actor_id() -> hyperactor::reference::ActorId {
        test_proc_id().actor_id("actor", 0)
    }

    fn test_host_actor_id() -> hyperactor::reference::ActorId {
        test_proc_id().actor_id("host_agent", 0)
    }

    fn test_time() -> SystemTime {
        humantime::parse_rfc3339("2025-01-15T10:30:00.123Z").unwrap()
    }

    fn test_time_2() -> SystemTime {
        humantime::parse_rfc3339("2025-01-15T11:00:00.456Z").unwrap()
    }

    fn make_root_payload() -> NodePayload {
        NodePayload {
            identity: NodeRef::Root,
            properties: NodeProperties::Root {
                num_hosts: 2,
                started_at: test_time(),
                started_by: "test_user".to_string(),
                system_children: vec![],
            },
            children: vec![NodeRef::Host(test_host_actor_id())],
            parent: None,
            as_of: test_time(),
        }
    }

    fn make_host_payload() -> NodePayload {
        NodePayload {
            identity: NodeRef::Host(test_host_actor_id()),
            properties: NodeProperties::Host {
                addr: "127.0.0.1:8080".to_string(),
                num_procs: 1,
                system_children: vec![],
                memory: Default::default(),
            },
            children: vec![NodeRef::Proc(test_proc_id())],
            parent: Some(NodeRef::Root),
            as_of: test_time(),
        }
    }

    fn make_proc_payload() -> NodePayload {
        NodePayload {
            identity: NodeRef::Proc(test_proc_id()),
            properties: NodeProperties::Proc {
                proc_name: "worker".to_string(),
                num_actors: 3,
                system_children: vec![NodeRef::Actor(test_actor_id())],
                stopped_children: vec![],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
                debug: Default::default(),
            },
            children: vec![NodeRef::Actor(test_actor_id())],
            parent: Some(NodeRef::Host(test_host_actor_id())),
            as_of: test_time(),
        }
    }

    fn make_actor_payload_no_failure() -> NodePayload {
        NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Actor {
                actor_status: "running".to_string(),
                actor_type: "MyActor".to_string(),
                messages_processed: 42,
                created_at: Some(test_time()),
                last_message_handler: Some("handle_msg".to_string()),
                total_processing_time_us: 1500,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: Some(NodeRef::Proc(test_proc_id())),
            as_of: test_time(),
        }
    }

    fn make_actor_payload_with_failure() -> NodePayload {
        NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Actor {
                actor_status: "failed".to_string(),
                actor_type: "MyActor".to_string(),
                messages_processed: 10,
                created_at: Some(test_time()),
                last_message_handler: None,
                total_processing_time_us: 500,
                flight_recorder: Some("trace-abc".to_string()),
                is_system: true,
                failure_info: Some(FailureInfo {
                    error_message: "boom".to_string(),
                    root_cause_actor: test_actor_id(),
                    root_cause_name: Some("root_actor".to_string()),
                    occurred_at: test_time_2(),
                    is_propagated: true,
                }),
            },
            children: vec![],
            parent: Some(NodeRef::Proc(test_proc_id())),
            as_of: test_time(),
        }
    }

    fn make_actor_payload_minimal() -> NodePayload {
        NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Actor {
                actor_status: "idle".to_string(),
                actor_type: "MinimalActor".to_string(),
                messages_processed: 0,
                created_at: None,
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: Some(NodeRef::Proc(test_proc_id())),
            as_of: test_time(),
        }
    }

    fn make_error_payload() -> NodePayload {
        NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Error {
                code: "not_found".to_string(),
                message: "actor not found".to_string(),
            },
            children: vec![],
            parent: None,
            as_of: test_time(),
        }
    }

    // HB-2 (round-trip): NodePayload → NodePayloadDto → NodePayload is
    // lossless for values representable in the wire format.

    fn assert_round_trip(payload: &NodePayload) {
        let dto: NodePayloadDto = payload.clone().into();
        let back: NodePayload = dto.try_into().expect("round-trip conversion");
        assert_eq!(payload, &back);
    }

    /// HB-2: Root variant round-trips.
    #[test]
    fn test_round_trip_root() {
        assert_round_trip(&make_root_payload());
    }

    /// HB-2: Host variant round-trips.
    #[test]
    fn test_round_trip_host() {
        assert_round_trip(&make_host_payload());
    }

    /// HB-2: Proc variant round-trips.
    #[test]
    fn test_round_trip_proc() {
        assert_round_trip(&make_proc_payload());
    }

    /// HB-2: Actor variant without failure round-trips.
    #[test]
    fn test_round_trip_actor_no_failure() {
        assert_round_trip(&make_actor_payload_no_failure());
    }

    /// HB-2: Actor variant with failure round-trips.
    #[test]
    fn test_round_trip_actor_with_failure() {
        assert_round_trip(&make_actor_payload_with_failure());
    }

    /// HB-2: Actor variant with all optional fields absent round-trips.
    #[test]
    fn test_round_trip_actor_minimal() {
        assert_round_trip(&make_actor_payload_minimal());
    }

    /// HB-2: Error variant round-trips.
    #[test]
    fn test_round_trip_error() {
        assert_round_trip(&make_error_payload());
    }

    // HB-1 (typed-internal, string-external): typed Rust values serialize
    // as canonical strings in the DTO JSON output.

    /// HB-1: Root identity, children, parent, and timestamps serialize
    /// as strings; externally-tagged enum key is "Root".
    #[test]
    fn test_json_shape_root() {
        let dto: NodePayloadDto = make_root_payload().into();
        let json = serde_json::to_value(&dto).unwrap();

        assert_eq!(json["identity"], "root");
        assert!(json["parent"].is_null());
        assert_eq!(json["as_of"], "2025-01-15T10:30:00.123Z");

        let children = json["children"].as_array().unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0], format!("host:{}", test_host_actor_id()));

        let root = &json["properties"]["Root"];
        assert_eq!(root["num_hosts"], 2);
        assert_eq!(root["started_at"], "2025-01-15T10:30:00.123Z");
        assert_eq!(root["started_by"], "test_user");
        assert!(root["system_children"].as_array().unwrap().is_empty());
    }

    /// HB-1: Actor variant with failure — ActorId, SystemTime, and
    /// nested FailureInfo fields all serialize as strings.
    #[test]
    fn test_json_shape_actor_with_failure() {
        let dto: NodePayloadDto = make_actor_payload_with_failure().into();
        let json = serde_json::to_value(&dto).unwrap();

        assert_eq!(json["identity"], test_actor_id().to_string());
        assert_eq!(json["parent"], test_proc_id().to_string());

        let actor = &json["properties"]["Actor"];
        assert_eq!(actor["actor_status"], "failed");
        assert_eq!(actor["messages_processed"], 10);
        assert_eq!(actor["created_at"], "2025-01-15T10:30:00.123Z");
        assert!(actor["last_message_handler"].is_null());
        assert_eq!(actor["flight_recorder"], "trace-abc");
        assert_eq!(actor["is_system"], true);

        let fi = &actor["failure_info"];
        assert_eq!(fi["error_message"], "boom");
        assert_eq!(fi["root_cause_actor"], test_actor_id().to_string());
        assert_eq!(fi["root_cause_name"], "root_actor");
        assert_eq!(fi["occurred_at"], "2025-01-15T11:00:00.456Z");
        assert_eq!(fi["is_propagated"], true);
    }

    /// HB-1: Option fields serialize as JSON null when absent.
    #[test]
    fn test_json_shape_optional_none_fields() {
        let dto: NodePayloadDto = make_actor_payload_minimal().into();
        let json = serde_json::to_value(&dto).unwrap();

        let actor = &json["properties"]["Actor"];
        assert!(actor["created_at"].is_null());
        assert!(actor["last_message_handler"].is_null());
        assert!(actor["flight_recorder"].is_null());
        assert!(actor["failure_info"].is_null());
    }

    /// HB-1: Error variant preserves code/message as plain strings.
    #[test]
    fn test_json_shape_error() {
        let dto: NodePayloadDto = make_error_payload().into();
        let json = serde_json::to_value(&dto).unwrap();

        let err = &json["properties"]["Error"];
        assert_eq!(err["code"], "not_found");
        assert_eq!(err["message"], "actor not found");
    }

    /// HB-1: Empty children vec serializes as `[]`.
    #[test]
    fn test_json_shape_empty_children() {
        let dto: NodePayloadDto = make_actor_payload_no_failure().into();
        let json = serde_json::to_value(&dto).unwrap();
        assert!(json["children"].as_array().unwrap().is_empty());
    }

    // HB-3 (schema-honesty): published schema reflects the actual wire
    // format. The schemars(rename/title) attributes must produce $defs
    // keys and title matching the domain type names, not the Dto suffixes.

    /// HB-3: $defs keys are "NodeProperties" and "FailureInfo", not
    /// "NodePropertiesDto" / "FailureInfoDto".
    #[test]
    fn test_schema_defs_keys() {
        let schema = schemars::schema_for!(NodePayloadDto);
        let json = serde_json::to_value(&schema).unwrap();
        let defs = json["$defs"].as_object().unwrap();
        assert!(
            defs.contains_key("NodeProperties"),
            "$defs must contain 'NodeProperties', got: {:?}",
            defs.keys().collect::<Vec<_>>()
        );
        assert!(
            defs.contains_key("FailureInfo"),
            "$defs must contain 'FailureInfo', got: {:?}",
            defs.keys().collect::<Vec<_>>()
        );
    }

    /// HB-3: Top-level schema title is "NodePayload", not
    /// "NodePayloadDto".
    #[test]
    fn test_schema_title() {
        let schema = schemars::schema_for!(NodePayloadDto);
        let json = serde_json::to_value(&schema).unwrap();
        assert_eq!(json["title"], "NodePayload");
    }
}
