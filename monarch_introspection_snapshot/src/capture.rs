/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! BFS capture of a mesh topology into [`SnapshotData`].
//!
//! The entry point is [`capture_snapshot`], which walks the mesh from
//! root, resolving each node exactly once, and folds the results into
//! a relational row set ready for ingestion.
//!
//! # Capture invariants (CS-*)
//!
//! - **CS-1 (snapshot-row-once):** Each successful `capture_snapshot`
//!   emits exactly one [`SnapshotRow`].
//! - **CS-2 (resolve-once-per-node):** Each distinct `NodeRef` is
//!   resolved at most once per capture.
//! - **CS-3 (edge-per-parent-payload):** Every [`ChildRow`] emitted
//!   comes from a successfully resolved parent payload's `children`,
//!   in parent enumeration order.
//! - **CS-4 (duplicate-node-single-row):** If the same `NodeRef` is
//!   reachable from multiple parents, [`SnapshotData`] contains one
//!   node projection but one [`ChildRow`] per observed parent→child
//!   edge.
//! - **CS-5 (fold-homomorphism):** Folding a
//!   [`ConvertedNode`](crate::convert::ConvertedNode) via
//!   [`SnapshotData::push_converted`] appends exactly one
//!   [`NodeRow`], exactly one subtype-table row matching `kind_row`,
//!   optionally one [`ActorFailureRow`], and all of its
//!   [`ChildRow`]s.
//! - **CS-6 (resolution-error-boundary):** Resolver transport/query
//!   failure aborts capture with `Err`. Only successfully resolved
//!   payloads with `NodeProperties::Error` populate
//!   `resolution_errors`.
//! - **CS-7 (typed-visited-key):** Traversal dedup uses typed
//!   `NodeRef` identity, not stringified IDs.
//! - **CS-8 (snapshot-ts-capture-start):** `snapshot.snapshot_ts` is
//!   captured once at traversal start and is independent of per-node
//!   `as_of`.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::time::SystemTime;

use anyhow::Context;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeRef;

use crate::convert::ConvertedNode;
use crate::convert::NodeKindRow;
use crate::convert::convert_node;
use crate::convert::to_micros;
use crate::schema::ActorFailureRow;
use crate::schema::ActorNodeRow;
use crate::schema::ChildRow;
use crate::schema::HostNodeRow;
use crate::schema::NodeRow;
use crate::schema::ProcNodeRow;
use crate::schema::ResolutionErrorRow;
use crate::schema::RootNodeRow;
use crate::schema::SnapshotRow;

/// All row vectors produced by a single snapshot capture.
#[derive(Debug)]
pub struct SnapshotData {
    /// Capture metadata (CS-1: exactly one per successful capture).
    pub snapshot: SnapshotRow,
    /// One row per resolved node (CS-2: each `NodeRef` resolved
    /// once).
    pub nodes: Vec<NodeRow>,
    /// One row per parent→child edge (CS-3, CS-4: edges emitted
    /// per-parent, so a multiply-reachable node has multiple edges).
    pub children: Vec<ChildRow>,
    /// Singleton on success — CS-2 applied to the single root entry
    /// point means root is resolved exactly once.
    pub root_nodes: Vec<RootNodeRow>,
    /// One row per resolved host node.
    pub host_nodes: Vec<HostNodeRow>,
    /// One row per resolved proc node.
    pub proc_nodes: Vec<ProcNodeRow>,
    /// One row per resolved actor node.
    pub actor_nodes: Vec<ActorNodeRow>,
    /// One row per actor with `failure_info: Some(…)` (CV-3).
    pub actor_failures: Vec<ActorFailureRow>,
    /// One row per successfully resolved `NodeProperties::Error`
    /// (CS-6: distinct from resolver transport failures).
    pub resolution_errors: Vec<ResolutionErrorRow>,
}

impl SnapshotData {
    /// Fold a single converted node into the accumulator (CS-5).
    ///
    /// This is the only place that branches on [`NodeKindRow`]. BFS
    /// calls this and nothing else.
    pub fn push_converted(&mut self, converted: ConvertedNode) {
        self.nodes.push(converted.node);
        self.children.extend(converted.children);
        if let Some(f) = converted.actor_failure {
            self.actor_failures.push(f);
        }
        match converted.kind_row {
            NodeKindRow::Root(r) => self.root_nodes.push(r),
            NodeKindRow::Host(h) => self.host_nodes.push(h),
            NodeKindRow::Proc(p) => self.proc_nodes.push(p),
            NodeKindRow::Actor(a) => self.actor_nodes.push(a),
            NodeKindRow::ResolutionError(e) => self.resolution_errors.push(e),
        }
    }
}

/// Capture a full mesh snapshot by BFS from root.
///
/// The `resolve` closure is called once per distinct `NodeRef`
/// (CS-2). Resolver transport failures abort capture immediately
/// (CS-6). Successfully resolved `NodeProperties::Error` payloads are
/// valid rows, not capture failures.
pub async fn capture_snapshot<F, Fut>(snapshot_id: &str, resolve: F) -> anyhow::Result<SnapshotData>
where
    F: Fn(&NodeRef) -> Fut,
    Fut: Future<Output = anyhow::Result<NodePayload>>,
{
    // CS-1, CS-8: one snapshot row, timestamp at capture start.
    let snapshot = SnapshotRow {
        snapshot_id: snapshot_id.to_owned(),
        snapshot_ts: to_micros(SystemTime::now())
            .context("failed to compute snapshot timestamp")?,
    };

    let mut data = SnapshotData {
        snapshot,
        nodes: Vec::new(),
        children: Vec::new(),
        root_nodes: Vec::new(),
        host_nodes: Vec::new(),
        proc_nodes: Vec::new(),
        actor_nodes: Vec::new(),
        actor_failures: Vec::new(),
        resolution_errors: Vec::new(),
    };

    // CS-7: typed visited key.
    let mut visited: HashSet<NodeRef> = HashSet::new();
    let mut queue: VecDeque<NodeRef> = VecDeque::new();
    queue.push_back(NodeRef::Root);

    while let Some(node_ref) = queue.pop_front() {
        // Dedup on dequeue — queue may contain already-visited refs.
        if !visited.insert(node_ref.clone()) {
            continue;
        }

        // CS-6: resolver failure is an immediate capture error.
        let payload = resolve(&node_ref)
            .await
            .with_context(|| format!("failed to resolve {}", node_ref))?;

        // Enqueue all children unconditionally; dedup happens on
        // dequeue.
        for child_ref in &payload.children {
            queue.push_back(child_ref.clone());
        }

        // Project and fold.
        let converted = convert_node(snapshot_id, &payload)
            .with_context(|| format!("failed to convert {}", node_ref))?;
        data.push_converted(converted);
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use hyperactor::channel::ChannelAddr;
    use hyperactor::reference::ProcId;
    use hyperactor_mesh::introspect::NodeProperties;

    use super::*;

    // Test fixtures

    fn test_proc_id() -> ProcId {
        ProcId::with_name(ChannelAddr::Local(0), "worker")
    }

    fn test_actor_id(name: &str) -> hyperactor::reference::ActorId {
        test_proc_id().actor_id(name)
    }

    fn test_host_actor_id() -> hyperactor::reference::ActorId {
        test_proc_id().actor_id("host_agent")
    }

    fn test_time() -> SystemTime {
        UNIX_EPOCH + Duration::from_micros(1_700_000_000_000_000)
    }

    fn make_payload(
        identity: NodeRef,
        properties: NodeProperties,
        children: Vec<NodeRef>,
    ) -> NodePayload {
        NodePayload {
            identity,
            properties,
            children,
            parent: None,
            as_of: test_time(),
        }
    }

    /// Stub resolver backed by a map. Returns the payload for known
    /// refs, error for unknown.
    fn stub_resolver(
        map: HashMap<NodeRef, NodePayload>,
    ) -> impl Fn(&NodeRef) -> std::future::Ready<anyhow::Result<NodePayload>> {
        move |node_ref: &NodeRef| {
            let result = map
                .get(node_ref)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown ref: {}", node_ref));
            std::future::ready(result)
        }
    }

    /// Stub resolver that also records every NodeRef requested.
    fn recording_stub_resolver(
        map: HashMap<NodeRef, NodePayload>,
        log: Arc<Mutex<Vec<NodeRef>>>,
    ) -> impl Fn(&NodeRef) -> std::future::Ready<anyhow::Result<NodePayload>> {
        move |node_ref: &NodeRef| {
            log.lock().unwrap().push(node_ref.clone());
            let result = map
                .get(node_ref)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown ref: {}", node_ref));
            std::future::ready(result)
        }
    }

    // CS-1, CS-8: one snapshot row, timestamp at capture start.
    #[tokio::test]
    async fn test_capture_emits_one_snapshot_row() {
        let map: HashMap<NodeRef, NodePayload> = [(
            NodeRef::Root,
            make_payload(
                NodeRef::Root,
                NodeProperties::Root {
                    num_hosts: 0,
                    started_at: test_time(),
                    started_by: "test".to_owned(),
                    system_children: vec![],
                },
                vec![],
            ),
        )]
        .into();

        let data = capture_snapshot("snap-1", stub_resolver(map))
            .await
            .unwrap();

        assert_eq!(data.snapshot.snapshot_id, "snap-1");
        // CS-8: snapshot_ts should be recent (within last 5 seconds),
        // and distinct from node as_of.
        let now_micros = to_micros(SystemTime::now()).unwrap();
        assert!(
            data.snapshot.snapshot_ts > now_micros - 5_000_000,
            "CS-8: snapshot_ts should be recent"
        );
        assert_ne!(
            data.snapshot.snapshot_ts,
            to_micros(test_time()).unwrap(),
            "CS-8: snapshot_ts should differ from node as_of"
        );
        // CS-1: exactly one node (root only in this topology).
        assert_eq!(data.nodes.len(), 1);
        // Root is always singleton on success.
        assert_eq!(data.root_nodes.len(), 1);
    }

    // CS-2, CS-7: each NodeRef resolved at most once.
    #[tokio::test]
    async fn test_capture_resolves_each_node_once() {
        let actor_b = NodeRef::Actor(test_actor_id("b"));
        let actor_a = NodeRef::Actor(test_actor_id("a"));
        let proc_ref = NodeRef::Proc(test_proc_id());
        let host_ref = NodeRef::Host(test_host_actor_id());

        // actor_b reachable from both proc and actor_a.
        let map: HashMap<NodeRef, NodePayload> = [
            (
                NodeRef::Root,
                make_payload(
                    NodeRef::Root,
                    NodeProperties::Root {
                        num_hosts: 1,
                        started_at: test_time(),
                        started_by: "test".to_owned(),
                        system_children: vec![],
                    },
                    vec![host_ref.clone()],
                ),
            ),
            (
                host_ref.clone(),
                make_payload(
                    host_ref.clone(),
                    NodeProperties::Host {
                        addr: "addr".to_owned(),
                        num_procs: 1,
                        system_children: vec![],
                        memory: Default::default(),
                    },
                    vec![proc_ref.clone()],
                ),
            ),
            (
                proc_ref.clone(),
                make_payload(
                    proc_ref.clone(),
                    NodeProperties::Proc {
                        proc_name: "w".to_owned(),
                        num_actors: 2,
                        system_children: vec![],
                        stopped_children: vec![],
                        stopped_retention_cap: 0,
                        is_poisoned: false,
                        failed_actor_count: 0,
                        debug: Default::default(),
                    },
                    vec![actor_a.clone(), actor_b.clone()],
                ),
            ),
            (
                actor_a.clone(),
                make_payload(
                    actor_a.clone(),
                    NodeProperties::Actor {
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
                    vec![actor_b.clone()],
                ),
            ),
            (
                actor_b.clone(),
                make_payload(
                    actor_b.clone(),
                    NodeProperties::Actor {
                        actor_status: "running".to_owned(),
                        actor_type: "B".to_owned(),
                        messages_processed: 0,
                        created_at: None,
                        last_message_handler: None,
                        total_processing_time_us: 0,
                        flight_recorder: None,
                        is_system: false,
                        failure_info: None,
                    },
                    vec![],
                ),
            ),
        ]
        .into();

        let log = Arc::new(Mutex::new(Vec::new()));
        let data = capture_snapshot("s", recording_stub_resolver(map, log.clone()))
            .await
            .unwrap();

        // CS-2: actor_b resolved exactly once even though reachable
        // from both proc and actor_a.
        let resolved = log.lock().unwrap();
        let b_count = resolved.iter().filter(|r| **r == actor_b).count();
        assert_eq!(b_count, 1, "CS-2: actor_b should be resolved once");

        // CS-7: visited key is typed NodeRef (structural — if it
        // used strings, the dedup would fail for refs with different
        // typed representations but same Display form).
        assert_eq!(data.nodes.len(), 5);
    }

    // CS-3: child edges emitted in parent enumeration order.
    #[tokio::test]
    async fn test_capture_emits_edges_from_each_parent() {
        let a0 = NodeRef::Actor(test_actor_id("a"));
        let a1 = NodeRef::Actor(test_actor_id("b"));
        let a2 = NodeRef::Actor(test_actor_id("c"));
        let proc_ref = NodeRef::Proc(test_proc_id());
        let host_ref = NodeRef::Host(test_host_actor_id());

        let map: HashMap<NodeRef, NodePayload> = [
            (
                NodeRef::Root,
                make_payload(
                    NodeRef::Root,
                    NodeProperties::Root {
                        num_hosts: 1,
                        started_at: test_time(),
                        started_by: "t".to_owned(),
                        system_children: vec![],
                    },
                    vec![host_ref.clone()],
                ),
            ),
            (
                host_ref.clone(),
                make_payload(
                    host_ref.clone(),
                    NodeProperties::Host {
                        addr: "a".to_owned(),
                        num_procs: 1,
                        system_children: vec![],
                        memory: Default::default(),
                    },
                    vec![proc_ref.clone()],
                ),
            ),
            (
                proc_ref.clone(),
                make_payload(
                    proc_ref.clone(),
                    NodeProperties::Proc {
                        proc_name: "w".to_owned(),
                        num_actors: 3,
                        system_children: vec![],
                        stopped_children: vec![],
                        stopped_retention_cap: 0,
                        is_poisoned: false,
                        failed_actor_count: 0,
                        debug: Default::default(),
                    },
                    vec![a0.clone(), a1.clone(), a2.clone()],
                ),
            ),
            (
                a0.clone(),
                make_payload(
                    a0.clone(),
                    NodeProperties::Actor {
                        actor_status: "r".to_owned(),
                        actor_type: "A".to_owned(),
                        messages_processed: 0,
                        created_at: None,
                        last_message_handler: None,
                        total_processing_time_us: 0,
                        flight_recorder: None,
                        is_system: false,
                        failure_info: None,
                    },
                    vec![],
                ),
            ),
            (
                a1.clone(),
                make_payload(
                    a1.clone(),
                    NodeProperties::Actor {
                        actor_status: "r".to_owned(),
                        actor_type: "A".to_owned(),
                        messages_processed: 0,
                        created_at: None,
                        last_message_handler: None,
                        total_processing_time_us: 0,
                        flight_recorder: None,
                        is_system: false,
                        failure_info: None,
                    },
                    vec![],
                ),
            ),
            (
                a2.clone(),
                make_payload(
                    a2.clone(),
                    NodeProperties::Actor {
                        actor_status: "r".to_owned(),
                        actor_type: "A".to_owned(),
                        messages_processed: 0,
                        created_at: None,
                        last_message_handler: None,
                        total_processing_time_us: 0,
                        flight_recorder: None,
                        is_system: false,
                        failure_info: None,
                    },
                    vec![],
                ),
            ),
        ]
        .into();

        let data = capture_snapshot("s", stub_resolver(map)).await.unwrap();

        // CS-3: proc's children should appear with correct sort keys.
        let proc_id_str = proc_ref.to_string();
        let proc_children: Vec<_> = data
            .children
            .iter()
            .filter(|c| c.parent_id == proc_id_str)
            .collect();
        assert_eq!(proc_children.len(), 3);
        assert_eq!(proc_children[0].child_sort_key, 0);
        assert_eq!(proc_children[0].child_id, a0.to_string());
        assert_eq!(proc_children[1].child_sort_key, 1);
        assert_eq!(proc_children[1].child_id, a1.to_string());
        assert_eq!(proc_children[2].child_sort_key, 2);
        assert_eq!(proc_children[2].child_id, a2.to_string());
    }

    // CS-2, CS-4: one node row for a multiply-reachable node, but
    // one ChildRow per parent→child edge.
    #[tokio::test]
    async fn test_capture_dedupes_nodes_not_edges() {
        let actor_b = NodeRef::Actor(test_actor_id("b"));
        let actor_a = NodeRef::Actor(test_actor_id("a"));
        let proc_ref = NodeRef::Proc(test_proc_id());
        let host_ref = NodeRef::Host(test_host_actor_id());

        // actor_b reachable from both proc and actor_a.
        let map: HashMap<NodeRef, NodePayload> = [
            (
                NodeRef::Root,
                make_payload(
                    NodeRef::Root,
                    NodeProperties::Root {
                        num_hosts: 1,
                        started_at: test_time(),
                        started_by: "t".to_owned(),
                        system_children: vec![],
                    },
                    vec![host_ref.clone()],
                ),
            ),
            (
                host_ref.clone(),
                make_payload(
                    host_ref.clone(),
                    NodeProperties::Host {
                        addr: "a".to_owned(),
                        num_procs: 1,
                        system_children: vec![],
                        memory: Default::default(),
                    },
                    vec![proc_ref.clone()],
                ),
            ),
            (
                proc_ref.clone(),
                make_payload(
                    proc_ref.clone(),
                    NodeProperties::Proc {
                        proc_name: "w".to_owned(),
                        num_actors: 2,
                        system_children: vec![],
                        stopped_children: vec![],
                        stopped_retention_cap: 0,
                        is_poisoned: false,
                        failed_actor_count: 0,
                        debug: Default::default(),
                    },
                    vec![actor_a.clone(), actor_b.clone()],
                ),
            ),
            (
                actor_a.clone(),
                make_payload(
                    actor_a.clone(),
                    NodeProperties::Actor {
                        actor_status: "r".to_owned(),
                        actor_type: "A".to_owned(),
                        messages_processed: 0,
                        created_at: None,
                        last_message_handler: None,
                        total_processing_time_us: 0,
                        flight_recorder: None,
                        is_system: false,
                        failure_info: None,
                    },
                    vec![actor_b.clone()],
                ),
            ),
            (
                actor_b.clone(),
                make_payload(
                    actor_b.clone(),
                    NodeProperties::Actor {
                        actor_status: "r".to_owned(),
                        actor_type: "B".to_owned(),
                        messages_processed: 0,
                        created_at: None,
                        last_message_handler: None,
                        total_processing_time_us: 0,
                        flight_recorder: None,
                        is_system: false,
                        failure_info: None,
                    },
                    vec![],
                ),
            ),
        ]
        .into();

        let data = capture_snapshot("s", stub_resolver(map)).await.unwrap();

        // CS-4: one NodeRow for actor_b.
        let b_id = actor_b.to_string();
        let b_nodes: Vec<_> = data.nodes.iter().filter(|n| n.node_id == b_id).collect();
        assert_eq!(b_nodes.len(), 1, "CS-4: one NodeRow for actor_b");

        // CS-4: two ChildRows pointing to actor_b from different parents.
        let b_edges: Vec<_> = data
            .children
            .iter()
            .filter(|c| c.child_id == b_id)
            .collect();
        assert_eq!(b_edges.len(), 2, "CS-4: two ChildRows for actor_b");

        let parents: HashSet<&str> = b_edges.iter().map(|c| c.parent_id.as_str()).collect();
        assert!(
            parents.contains(proc_ref.to_string().as_str()),
            "CS-4: proc should be a parent of actor_b"
        );
        assert!(
            parents.contains(actor_a.to_string().as_str()),
            "CS-4: actor_a should be a parent of actor_b"
        );
    }

    // CS-5: push_converted routes each NodeKindRow variant to the
    // correct subtype vec. Table-driven across all five variants.
    #[test]
    fn test_push_converted_matches_kind_row() {
        use crate::schema::*;

        fn node(id: &str, kind: &str) -> NodeRow {
            NodeRow {
                snapshot_id: "s".to_owned(),
                node_id: id.to_owned(),
                node_kind: kind.to_owned(),
                as_of: 0,
            }
        }

        let cases: Vec<(ConvertedNode, &str)> = vec![
            (
                ConvertedNode {
                    node: node("root", "root"),
                    kind_row: NodeKindRow::Root(RootNodeRow {
                        snapshot_id: "s".to_owned(),
                        node_id: "root".to_owned(),
                        num_hosts: 0,
                        started_at: 0,
                        started_by: "t".to_owned(),
                    }),
                    actor_failure: None,
                    children: vec![ChildRow {
                        snapshot_id: "s".to_owned(),
                        parent_id: "root".to_owned(),
                        child_id: "h".to_owned(),
                        child_sort_key: 0,
                        is_system: false,
                        is_stopped: false,
                    }],
                },
                "Root",
            ),
            (
                ConvertedNode {
                    node: node("h", "host"),
                    kind_row: NodeKindRow::Host(HostNodeRow {
                        snapshot_id: "s".to_owned(),
                        node_id: "h".to_owned(),
                        addr: "a".to_owned(),
                        host_num_procs: 0,
                    }),
                    actor_failure: None,
                    children: vec![],
                },
                "Host",
            ),
            (
                ConvertedNode {
                    node: node("p", "proc"),
                    kind_row: NodeKindRow::Proc(ProcNodeRow {
                        snapshot_id: "s".to_owned(),
                        node_id: "p".to_owned(),
                        proc_name: "w".to_owned(),
                        num_actors: 0,
                        stopped_retention_cap: 0,
                        is_poisoned: false,
                        failed_actor_count: 0,
                    }),
                    actor_failure: None,
                    children: vec![],
                },
                "Proc",
            ),
            (
                ConvertedNode {
                    node: node("a", "actor"),
                    kind_row: NodeKindRow::Actor(ActorNodeRow {
                        snapshot_id: "s".to_owned(),
                        node_id: "a".to_owned(),
                        actor_status: "failed".to_owned(),
                        actor_type: "A".to_owned(),
                        messages_processed: 0,
                        created_at: None,
                        last_message_handler: None,
                        total_processing_time_us: 0,
                        is_system: false,
                    }),
                    actor_failure: Some(ActorFailureRow {
                        snapshot_id: "s".to_owned(),
                        node_id: "a".to_owned(),
                        failure_error_message: "boom".to_owned(),
                        failure_root_cause_actor: "a".to_owned(),
                        failure_root_cause_name: None,
                        failure_occurred_at: 0,
                        failure_is_propagated: false,
                    }),
                    children: vec![],
                },
                "Actor",
            ),
            (
                ConvertedNode {
                    node: node("e", "error"),
                    kind_row: NodeKindRow::ResolutionError(ResolutionErrorRow {
                        snapshot_id: "s".to_owned(),
                        node_id: "e".to_owned(),
                        error_code: "not_found".to_owned(),
                        error_message: "gone".to_owned(),
                    }),
                    actor_failure: None,
                    children: vec![],
                },
                "ResolutionError",
            ),
        ];

        let mut data = SnapshotData {
            snapshot: SnapshotRow {
                snapshot_id: "s".to_owned(),
                snapshot_ts: 0,
            },
            nodes: Vec::new(),
            children: Vec::new(),
            root_nodes: Vec::new(),
            host_nodes: Vec::new(),
            proc_nodes: Vec::new(),
            actor_nodes: Vec::new(),
            actor_failures: Vec::new(),
            resolution_errors: Vec::new(),
        };

        for (converted, label) in cases {
            data.push_converted(converted);
            let subtype_count = match label {
                "Root" => data.root_nodes.len(),
                "Host" => data.host_nodes.len(),
                "Proc" => data.proc_nodes.len(),
                "Actor" => data.actor_nodes.len(),
                "ResolutionError" => data.resolution_errors.len(),
                _ => unreachable!(),
            };
            assert_eq!(
                subtype_count, 1,
                "CS-5: {label} variant should route to its subtype vec"
            );
        }

        assert_eq!(data.nodes.len(), 5);
        assert_eq!(data.children.len(), 1); // from root
        assert_eq!(data.actor_failures.len(), 1); // from actor
    }

    // CS-6: resolver failure aborts capture.
    #[tokio::test]
    async fn test_capture_aborts_on_resolver_error() {
        let host_ref = NodeRef::Host(test_host_actor_id());

        // Root has a child, but the child resolver fails.
        let map: HashMap<NodeRef, NodePayload> = [(
            NodeRef::Root,
            make_payload(
                NodeRef::Root,
                NodeProperties::Root {
                    num_hosts: 1,
                    started_at: test_time(),
                    started_by: "t".to_owned(),
                    system_children: vec![],
                },
                vec![host_ref],
            ),
        )]
        .into();

        let result = capture_snapshot("s", stub_resolver(map)).await;
        assert!(
            result.is_err(),
            "CS-6: capture should fail when resolver returns Err"
        );
    }

    // CS-6: successfully resolved NodeProperties::Error is a valid
    // row.
    #[tokio::test]
    async fn test_capture_keeps_domain_error_payloads() {
        let error_ref = NodeRef::Actor(test_actor_id("err"));
        let host_ref = NodeRef::Host(test_host_actor_id());
        let proc_ref = NodeRef::Proc(test_proc_id());

        let map: HashMap<NodeRef, NodePayload> = [
            (
                NodeRef::Root,
                make_payload(
                    NodeRef::Root,
                    NodeProperties::Root {
                        num_hosts: 1,
                        started_at: test_time(),
                        started_by: "t".to_owned(),
                        system_children: vec![],
                    },
                    vec![host_ref.clone()],
                ),
            ),
            (
                host_ref.clone(),
                make_payload(
                    host_ref.clone(),
                    NodeProperties::Host {
                        addr: "a".to_owned(),
                        num_procs: 1,
                        system_children: vec![],
                        memory: Default::default(),
                    },
                    vec![proc_ref.clone()],
                ),
            ),
            (
                proc_ref.clone(),
                make_payload(
                    proc_ref.clone(),
                    NodeProperties::Proc {
                        proc_name: "w".to_owned(),
                        num_actors: 1,
                        system_children: vec![],
                        stopped_children: vec![],
                        stopped_retention_cap: 0,
                        is_poisoned: false,
                        failed_actor_count: 0,
                        debug: Default::default(),
                    },
                    vec![error_ref.clone()],
                ),
            ),
            (
                error_ref.clone(),
                make_payload(
                    error_ref.clone(),
                    NodeProperties::Error {
                        code: "not_found".to_owned(),
                        message: "child not found".to_owned(),
                    },
                    vec![],
                ),
            ),
        ]
        .into();

        let data = capture_snapshot("s", stub_resolver(map)).await.unwrap();

        // CS-6: capture succeeds despite an Error node.
        assert_eq!(data.nodes.len(), 4);
        assert_eq!(
            data.resolution_errors.len(),
            1,
            "CS-6: domain error should produce ResolutionErrorRow"
        );
        assert_eq!(data.resolution_errors[0].error_code, "not_found");
    }
}
