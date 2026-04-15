/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Conversion from typed mesh-admin domain types to relational row
//! definitions.
//!
//! The entry point is [`convert_node`], which takes a single
//! [`NodePayload`] and produces a [`ConvertedNode`] — a typed
//! per-node projection that the BFS capture layer can fold into a
//! full snapshot.
//!
//! # Conversion invariants (CV-*)
//!
//! - **CV-1 (exactly one node row):** Each `NodePayload` converts to
//!   exactly one [`NodeRow`].
//! - **CV-2 (exactly one subtype row):** Each `NodePayload` converts
//!   to exactly one [`NodeKindRow`] variant matching
//!   `NodeRow.node_kind`. Enforced by the type system.
//! - **CV-3 (failure iff actor with failure_info):** `actor_failure`
//!   is `Some` iff `NodeProperties::Actor { failure_info: Some(_) }`.
//! - **CV-4 (child sort key = enumeration order):** `ChildRow` at
//!   position `i` in `payload.children` has `child_sort_key = i`.
//! - **CV-5 (child classification from parent sets):** `is_system`
//!   and `is_stopped` are derived solely from the parent's typed
//!   `system_children` / `stopped_children` sets via `HashSet`
//!   lookup.
//! - **CV-6 (canonical boundary crossing):** Typed refs cross the SQL
//!   boundary only via `.to_string()`. Times cross only via
//!   [`to_micros`]. No ad-hoc formatting.
//! - **CV-7 (parent not materialized):** `convert_node` does not read
//!   or persist `NodePayload.parent`. Parenthood in the snapshot
//!   schema is represented only through [`ChildRow`] edges.

use std::collections::HashSet;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use hyperactor_mesh::introspect::FailureInfo;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;

use crate::schema::ActorFailureRow;
use crate::schema::ActorNodeRow;
use crate::schema::ChildRow;
use crate::schema::HostNodeRow;
use crate::schema::NodeRow;
use crate::schema::ProcNodeRow;
use crate::schema::ResolutionErrorRow;
use crate::schema::RootNodeRow;

// Conversion algebra

/// Typed per-node projection. Encodes the exact-one-subtype invariant
/// (CV-2) in the type system: `kind_row` is an enum with one variant
/// per node kind, and `node.node_kind` is derived from the same
/// match.
#[derive(Debug, Clone, PartialEq)]
pub struct ConvertedNode {
    /// Base row present for every node (CV-1).
    pub node: NodeRow,
    /// Exactly one kind-specific row, matching `node.node_kind` (CV-2).
    pub kind_row: NodeKindRow,
    /// Failure detail, present only for actor nodes with
    /// `failure_info: Some(…)` (CV-3).
    pub actor_failure: Option<ActorFailureRow>,
    /// One [`ChildRow`] per entry in `payload.children`, in
    /// enumeration order (CV-4).
    pub children: Vec<ChildRow>,
}

/// Exactly one subtype row per converted node.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeKindRow {
    /// Root node properties.
    Root(RootNodeRow),
    /// Host node properties.
    Host(HostNodeRow),
    /// Proc node properties.
    Proc(ProcNodeRow),
    /// Actor node properties.
    Actor(ActorNodeRow),
    /// Resolution error properties.
    ResolutionError(ResolutionErrorRow),
}

impl NodeKindRow {
    /// The `node_kind` discriminator string stored in [`NodeRow`].
    /// Derived from the same match that produces the variant, so the
    /// string and the variant can never disagree.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::Root(_) => "root",
            Self::Host(_) => "host",
            Self::Proc(_) => "proc",
            Self::Actor(_) => "actor",
            Self::ResolutionError(_) => "error",
        }
    }
}

// Boundary helpers (CV-6)

/// Convert a `SystemTime` to microseconds since epoch.
///
/// Fallible: pre-epoch times and post-2554 overflow produce errors
/// rather than silent truncation.
pub(crate) fn to_micros(t: SystemTime) -> anyhow::Result<i64> {
    let micros = t
        .duration_since(UNIX_EPOCH)
        .context("SystemTime before UNIX epoch")?
        .as_micros();
    i64::try_from(micros).context("SystemTime microseconds overflow i64")
}

fn to_opt_micros(t: Option<SystemTime>) -> anyhow::Result<Option<i64>> {
    t.map(to_micros).transpose()
}

// Child classification (CV-5)

/// Link-level classification sets extracted from a parent's
/// `NodeProperties`. Built once per payload, then queried per child.
struct ChildClasses<'a> {
    system: HashSet<&'a NodeRef>,
    stopped: HashSet<&'a NodeRef>,
}

impl<'a> ChildClasses<'a> {
    fn from_properties(properties: &'a NodeProperties) -> Self {
        match properties {
            NodeProperties::Root {
                system_children, ..
            }
            | NodeProperties::Host {
                system_children, ..
            } => Self {
                system: system_children.iter().collect(),
                stopped: HashSet::new(),
            },
            NodeProperties::Proc {
                system_children,
                stopped_children,
                ..
            } => Self {
                system: system_children.iter().collect(),
                stopped: stopped_children.iter().collect(),
            },
            NodeProperties::Actor { .. } | NodeProperties::Error { .. } => Self {
                system: HashSet::new(),
                stopped: HashSet::new(),
            },
        }
    }

    fn classify(&self, child: &NodeRef) -> (bool, bool) {
        (self.system.contains(child), self.stopped.contains(child))
    }
}

fn build_child_rows(
    snapshot_id: &str,
    parent_id: &str,
    children: &[NodeRef],
    classes: &ChildClasses<'_>,
) -> anyhow::Result<Vec<ChildRow>> {
    children
        .iter()
        .enumerate()
        .map(|(i, child)| {
            let (is_system, is_stopped) = classes.classify(child);
            Ok(ChildRow {
                snapshot_id: snapshot_id.to_owned(),
                parent_id: parent_id.to_owned(),
                child_id: child.to_string(),
                child_sort_key: i64::try_from(i).context("child index overflow i64")?,
                is_system,
                is_stopped,
            })
        })
        .collect()
}

// Entry point (CV-1, CV-2, CV-3, CV-4, CV-5, CV-6)

/// Convert a single [`NodePayload`] into its relational row
/// projection.
///
/// `payload.parent` is intentionally not stored — the snapshot schema
/// represents parenthood through [`ChildRow`] edges, not a column on
/// the node. Parent reconstruction is query-side and contextual
/// (join through `ChildRow`) rather than a stored fact.
pub fn convert_node(snapshot_id: &str, payload: &NodePayload) -> anyhow::Result<ConvertedNode> {
    let node_id = payload.identity.to_string();

    // Subtype row + optional failure. Done first because NodeRow.node_kind
    // is derived from the same match via kind_row.kind_str().
    let (kind_row, actor_failure) = match &payload.properties {
        NodeProperties::Root {
            num_hosts,
            started_at,
            started_by,
            ..
        } => {
            let row = RootNodeRow {
                snapshot_id: snapshot_id.to_owned(),
                node_id: node_id.clone(),
                num_hosts: i64::try_from(*num_hosts).context("num_hosts overflow i64")?,
                started_at: to_micros(*started_at)?,
                started_by: started_by.clone(),
            };
            (NodeKindRow::Root(row), None)
        }
        NodeProperties::Host {
            addr, num_procs, ..
        } => {
            let row = HostNodeRow {
                snapshot_id: snapshot_id.to_owned(),
                node_id: node_id.clone(),
                addr: addr.clone(),
                host_num_procs: i64::try_from(*num_procs).context("num_procs overflow i64")?,
            };
            (NodeKindRow::Host(row), None)
        }
        NodeProperties::Proc {
            proc_name,
            num_actors,
            stopped_retention_cap,
            is_poisoned,
            failed_actor_count,
            ..
        } => {
            let row = ProcNodeRow {
                snapshot_id: snapshot_id.to_owned(),
                node_id: node_id.clone(),
                proc_name: proc_name.clone(),
                num_actors: i64::try_from(*num_actors).context("num_actors overflow i64")?,
                stopped_retention_cap: i64::try_from(*stopped_retention_cap)
                    .context("stopped_retention_cap overflow i64")?,
                is_poisoned: *is_poisoned,
                failed_actor_count: i64::try_from(*failed_actor_count)
                    .context("failed_actor_count overflow i64")?,
            };
            (NodeKindRow::Proc(row), None)
        }
        NodeProperties::Actor {
            actor_status,
            actor_type,
            messages_processed,
            created_at,
            last_message_handler,
            total_processing_time_us,
            is_system,
            failure_info,
            ..
        } => {
            let actor_row = ActorNodeRow {
                snapshot_id: snapshot_id.to_owned(),
                node_id: node_id.clone(),
                actor_status: actor_status.clone(),
                actor_type: actor_type.clone(),
                messages_processed: i64::try_from(*messages_processed)
                    .context("messages_processed overflow i64")?,
                created_at: to_opt_micros(*created_at)?,
                last_message_handler: last_message_handler.clone(),
                total_processing_time_us: i64::try_from(*total_processing_time_us)
                    .context("total_processing_time_us overflow i64")?,
                is_system: *is_system,
            };
            let failure = failure_info
                .as_ref()
                .map(|fi| convert_failure(snapshot_id, &node_id, fi))
                .transpose()?;
            (NodeKindRow::Actor(actor_row), failure)
        }
        NodeProperties::Error { code, message } => {
            let row = ResolutionErrorRow {
                snapshot_id: snapshot_id.to_owned(),
                node_id: node_id.clone(),
                error_code: code.clone(),
                error_message: message.clone(),
            };
            (NodeKindRow::ResolutionError(row), None)
        }
    };

    let node = NodeRow {
        snapshot_id: snapshot_id.to_owned(),
        node_id: node_id.clone(),
        node_kind: kind_row.kind_str().to_owned(),
        as_of: to_micros(payload.as_of)?,
    };

    // Child edges with classification from parent's typed sets.
    let classes = ChildClasses::from_properties(&payload.properties);
    let children = build_child_rows(snapshot_id, &node_id, &payload.children, &classes)?;

    Ok(ConvertedNode {
        node,
        kind_row,
        actor_failure,
        children,
    })
}

fn convert_failure(
    snapshot_id: &str,
    node_id: &str,
    fi: &FailureInfo,
) -> anyhow::Result<ActorFailureRow> {
    Ok(ActorFailureRow {
        snapshot_id: snapshot_id.to_owned(),
        node_id: node_id.to_owned(),
        failure_error_message: fi.error_message.clone(),
        failure_root_cause_actor: fi.root_cause_actor.to_string(),
        failure_root_cause_name: fi.root_cause_name.clone(),
        failure_occurred_at: to_micros(fi.occurred_at)?,
        failure_is_propagated: fi.is_propagated,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::channel::ChannelAddr;
    use hyperactor::reference::ProcId;

    use super::*;

    // Test fixtures

    fn test_proc_id() -> ProcId {
        ProcId::with_name(ChannelAddr::Local(0), "worker")
    }

    fn test_actor_id() -> hyperactor::reference::ActorId {
        test_proc_id().actor_id("actor", 0)
    }

    fn test_host_actor_id() -> hyperactor::reference::ActorId {
        test_proc_id().actor_id("host_agent", 0)
    }

    fn test_time() -> SystemTime {
        UNIX_EPOCH + Duration::from_micros(1_700_000_000_000_000)
    }

    fn test_time_2() -> SystemTime {
        UNIX_EPOCH + Duration::from_micros(1_700_000_001_000_000)
    }

    // CV-1, CV-2, CV-6: Root variant produces correct NodeRow and
    // RootNodeRow.
    #[test]
    fn test_convert_root() {
        let payload = NodePayload {
            identity: NodeRef::Root,
            properties: NodeProperties::Root {
                num_hosts: 2,
                started_at: test_time(),
                started_by: "test_user".to_owned(),
                system_children: vec![],
            },
            children: vec![NodeRef::Host(test_host_actor_id())],
            parent: None,
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();

        assert_eq!(result.node.node_id, "root");
        assert_eq!(result.node.node_kind, "root");
        assert_eq!(result.node.snapshot_id, "snap-1");
        assert_eq!(result.node.as_of, 1_700_000_000_000_000);

        let NodeKindRow::Root(root) = &result.kind_row else {
            panic!("expected Root variant");
        };
        assert_eq!(root.num_hosts, 2);
        assert_eq!(root.started_at, 1_700_000_000_000_000);
        assert_eq!(root.started_by, "test_user");

        assert!(result.actor_failure.is_none());
        assert_eq!(result.children.len(), 1);
        assert_eq!(
            result.children[0].child_id,
            format!("host:{}", test_host_actor_id())
        );
    }

    // CV-1, CV-2, CV-6: Host variant.
    #[test]
    fn test_convert_host() {
        let payload = NodePayload {
            identity: NodeRef::Host(test_host_actor_id()),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8080".to_owned(),
                num_procs: 3,
                system_children: vec![],
                memory: Default::default(),
            },
            children: vec![NodeRef::Proc(test_proc_id())],
            parent: Some(NodeRef::Root),
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();

        assert_eq!(result.node.snapshot_id, "snap-1");
        assert_eq!(
            result.node.node_id,
            format!("host:{}", test_host_actor_id())
        );
        assert_eq!(result.node.node_kind, "host");
        let NodeKindRow::Host(host) = &result.kind_row else {
            panic!("expected Host variant");
        };
        assert_eq!(host.snapshot_id, "snap-1");
        assert_eq!(host.node_id, result.node.node_id);
        assert_eq!(host.addr, "10.0.0.1:8080");
        assert_eq!(host.host_num_procs, 3);
        assert!(result.actor_failure.is_none());
        assert_eq!(result.children.len(), 1);
    }

    // CV-1, CV-2, CV-6: Proc variant.
    #[test]
    fn test_convert_proc() {
        let payload = NodePayload {
            identity: NodeRef::Proc(test_proc_id()),
            properties: NodeProperties::Proc {
                proc_name: "worker".to_owned(),
                num_actors: 5,
                system_children: vec![],
                stopped_children: vec![],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 1,
                debug: Default::default(),
            },
            children: vec![NodeRef::Actor(test_actor_id())],
            parent: Some(NodeRef::Host(test_host_actor_id())),
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();

        assert_eq!(result.node.snapshot_id, "snap-1");
        assert_eq!(result.node.node_id, test_proc_id().to_string());
        assert_eq!(result.node.node_kind, "proc");
        let NodeKindRow::Proc(proc_row) = &result.kind_row else {
            panic!("expected Proc variant");
        };
        assert_eq!(proc_row.snapshot_id, "snap-1");
        assert_eq!(proc_row.node_id, result.node.node_id);
        assert_eq!(proc_row.proc_name, "worker");
        assert_eq!(proc_row.num_actors, 5);
        assert_eq!(proc_row.stopped_retention_cap, 100);
        assert!(!proc_row.is_poisoned);
        assert_eq!(proc_row.failed_actor_count, 1);
        assert!(result.actor_failure.is_none());
    }

    // CV-1, CV-2, CV-3 (None case), CV-6: Actor without failure.
    #[test]
    fn test_convert_actor_no_failure() {
        let payload = NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Actor {
                actor_status: "running".to_owned(),
                actor_type: "MyActor".to_owned(),
                messages_processed: 42,
                created_at: Some(test_time()),
                last_message_handler: Some("handle_msg".to_owned()),
                total_processing_time_us: 1500,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: Some(NodeRef::Proc(test_proc_id())),
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();

        assert_eq!(result.node.node_kind, "actor");
        let NodeKindRow::Actor(actor) = &result.kind_row else {
            panic!("expected Actor variant");
        };
        assert_eq!(actor.actor_status, "running");
        assert_eq!(actor.messages_processed, 42);
        assert_eq!(actor.created_at, Some(1_700_000_000_000_000));
        assert_eq!(actor.last_message_handler.as_deref(), Some("handle_msg"));
        assert_eq!(actor.total_processing_time_us, 1500);
        assert!(!actor.is_system);
        assert!(result.actor_failure.is_none());
    }

    // CV-1, CV-2, CV-3 (Some case), CV-6: Actor with failure.
    #[test]
    fn test_convert_actor_with_failure() {
        let payload = NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Actor {
                actor_status: "failed".to_owned(),
                actor_type: "MyActor".to_owned(),
                messages_processed: 10,
                created_at: None,
                last_message_handler: None,
                total_processing_time_us: 500,
                flight_recorder: Some("trace-abc".to_owned()),
                is_system: true,
                failure_info: Some(FailureInfo {
                    error_message: "boom".to_owned(),
                    root_cause_actor: test_actor_id(),
                    root_cause_name: Some("root_actor".to_owned()),
                    occurred_at: test_time_2(),
                    is_propagated: true,
                }),
            },
            children: vec![],
            parent: Some(NodeRef::Proc(test_proc_id())),
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();

        assert_eq!(result.node.node_kind, "actor");
        let failure = result.actor_failure.as_ref().expect("expected failure row");
        assert_eq!(failure.failure_error_message, "boom");
        assert_eq!(
            failure.failure_root_cause_actor,
            test_actor_id().to_string()
        );
        assert_eq!(
            failure.failure_root_cause_name.as_deref(),
            Some("root_actor")
        );
        assert_eq!(failure.failure_occurred_at, 1_700_000_001_000_000);
        assert!(failure.failure_is_propagated);
    }

    // CV-1, CV-2: Error variant.
    #[test]
    fn test_convert_error() {
        let payload = NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Error {
                code: "not_found".to_owned(),
                message: "actor not found".to_owned(),
            },
            children: vec![],
            parent: None,
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();

        assert_eq!(result.node.node_kind, "error");
        let NodeKindRow::ResolutionError(err) = &result.kind_row else {
            panic!("expected ResolutionError variant");
        };
        assert_eq!(err.error_code, "not_found");
        assert_eq!(err.error_message, "actor not found");
        assert!(result.actor_failure.is_none());
    }

    // CV-5: Proc with mixed system/stopped/regular children.
    #[test]
    fn test_child_classification_mixed() {
        let regular = test_proc_id().actor_id("regular", 0);
        let sys_only = test_proc_id().actor_id("sys_actor", 0);
        let stopped_only = test_proc_id().actor_id("stopped_actor", 0);
        let sys_and_stopped = test_proc_id().actor_id("both", 0);

        let children = vec![
            NodeRef::Actor(regular.clone()),
            NodeRef::Actor(sys_only.clone()),
            NodeRef::Actor(stopped_only.clone()),
            NodeRef::Actor(sys_and_stopped.clone()),
        ];

        let payload = NodePayload {
            identity: NodeRef::Proc(test_proc_id()),
            properties: NodeProperties::Proc {
                proc_name: "worker".to_owned(),
                num_actors: 4,
                system_children: vec![
                    NodeRef::Actor(sys_only),
                    NodeRef::Actor(sys_and_stopped.clone()),
                ],
                stopped_children: vec![
                    NodeRef::Actor(stopped_only),
                    NodeRef::Actor(sys_and_stopped),
                ],
                stopped_retention_cap: 10,
                is_poisoned: false,
                failed_actor_count: 0,
                debug: Default::default(),
            },
            children,
            parent: Some(NodeRef::Host(test_host_actor_id())),
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();
        assert_eq!(result.children.len(), 4);

        // regular: neither system nor stopped
        assert!(!result.children[0].is_system);
        assert!(!result.children[0].is_stopped);

        // sys_only: system but not stopped
        assert!(result.children[1].is_system);
        assert!(!result.children[1].is_stopped);

        // stopped_only: stopped but not system
        assert!(!result.children[2].is_system);
        assert!(result.children[2].is_stopped);

        // sys_and_stopped: both
        assert!(result.children[3].is_system);
        assert!(result.children[3].is_stopped);
    }

    // CV-4: child_sort_key is enumeration order.
    #[test]
    fn test_child_sort_key_is_enumeration_order() {
        let a0 = test_proc_id().actor_id("a", 0);
        let a1 = test_proc_id().actor_id("b", 0);
        let a2 = test_proc_id().actor_id("c", 0);

        let payload = NodePayload {
            identity: NodeRef::Proc(test_proc_id()),
            properties: NodeProperties::Proc {
                proc_name: "w".to_owned(),
                num_actors: 3,
                system_children: vec![],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
                debug: Default::default(),
            },
            children: vec![NodeRef::Actor(a0), NodeRef::Actor(a1), NodeRef::Actor(a2)],
            parent: None,
            as_of: test_time(),
        };

        let result = convert_node("snap-1", &payload).unwrap();
        assert_eq!(result.children[0].child_sort_key, 0);
        assert_eq!(result.children[1].child_sort_key, 1);
        assert_eq!(result.children[2].child_sort_key, 2);
    }

    // CV-6: to_micros produces correct microseconds.
    #[test]
    fn test_to_micros_known_epoch() {
        let t = UNIX_EPOCH + Duration::from_micros(1_234_567_890);
        assert_eq!(to_micros(t).unwrap(), 1_234_567_890);
    }

    // CV-6: to_micros rejects pre-epoch SystemTime.
    #[test]
    fn test_to_micros_pre_epoch_errors() {
        let t = UNIX_EPOCH - Duration::from_secs(1);
        let err = to_micros(t).unwrap_err();
        assert!(
            format!("{err:#}").contains("before UNIX epoch"),
            "expected pre-epoch error, got: {err:#}"
        );
    }

    // CV-2: node_kind string matches kind_row variant for every type.
    #[test]
    fn test_node_kind_derived_from_match() {
        let payloads = [
            (
                "root",
                NodeProperties::Root {
                    num_hosts: 0,
                    started_at: test_time(),
                    started_by: String::new(),
                    system_children: vec![],
                },
            ),
            (
                "host",
                NodeProperties::Host {
                    addr: String::new(),
                    num_procs: 0,
                    system_children: vec![],
                    memory: Default::default(),
                },
            ),
            (
                "proc",
                NodeProperties::Proc {
                    proc_name: String::new(),
                    num_actors: 0,
                    system_children: vec![],
                    stopped_children: vec![],
                    stopped_retention_cap: 0,
                    is_poisoned: false,
                    failed_actor_count: 0,
                    debug: Default::default(),
                },
            ),
            (
                "actor",
                NodeProperties::Actor {
                    actor_status: String::new(),
                    actor_type: String::new(),
                    messages_processed: 0,
                    created_at: None,
                    last_message_handler: None,
                    total_processing_time_us: 0,
                    flight_recorder: None,
                    is_system: false,
                    failure_info: None,
                },
            ),
            (
                "error",
                NodeProperties::Error {
                    code: String::new(),
                    message: String::new(),
                },
            ),
        ];

        for (expected_kind, props) in payloads {
            let payload = NodePayload {
                identity: NodeRef::Root,
                properties: props,
                children: vec![],
                parent: None,
                as_of: test_time(),
            };
            let result = convert_node("s", &payload).unwrap();
            assert_eq!(
                result.node.node_kind,
                result.kind_row.kind_str(),
                "node_kind and kind_row disagree for {expected_kind}"
            );
            assert_eq!(result.node.node_kind, expected_kind);
        }
    }

    // CV-7: parenthood is represented only through ChildRow edges.
    #[test]
    fn test_parent_field_ignored() {
        let make = |parent: Option<NodeRef>| NodePayload {
            identity: NodeRef::Actor(test_actor_id()),
            properties: NodeProperties::Actor {
                actor_status: "running".to_owned(),
                actor_type: "A".to_owned(),
                messages_processed: 0,
                created_at: None,
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent,
            as_of: test_time(),
        };

        let a = convert_node("s", &make(None)).unwrap();
        let b = convert_node("s", &make(Some(NodeRef::Proc(test_proc_id())))).unwrap();

        assert_eq!(a, b);
    }

    // CV-5: empty classification sets produce false link flags.
    #[test]
    fn test_empty_classification_sets_produce_false_flags() {
        let child = NodeRef::Host(test_host_actor_id());

        // Root with empty system_children.
        let root_payload = NodePayload {
            identity: NodeRef::Root,
            properties: NodeProperties::Root {
                num_hosts: 1,
                started_at: test_time(),
                started_by: "u".to_owned(),
                system_children: vec![],
            },
            children: vec![child.clone()],
            parent: None,
            as_of: test_time(),
        };
        let root_result = convert_node("s", &root_payload).unwrap();
        assert!(!root_result.children[0].is_system);
        assert!(!root_result.children[0].is_stopped);

        // Host with empty system_children.
        let proc_child = NodeRef::Proc(test_proc_id());
        let host_payload = NodePayload {
            identity: NodeRef::Host(test_host_actor_id()),
            properties: NodeProperties::Host {
                addr: "addr".to_owned(),
                num_procs: 1,
                system_children: vec![],
                memory: Default::default(),
            },
            children: vec![proc_child],
            parent: Some(NodeRef::Root),
            as_of: test_time(),
        };
        let host_result = convert_node("s", &host_payload).unwrap();
        assert!(!host_result.children[0].is_system);
        assert!(!host_result.children[0].is_stopped);
    }
}
