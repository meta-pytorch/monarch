/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Relational row definitions for mesh introspection snapshots.
//!
//! Each struct maps to one Arrow table. `#[derive(RecordBatchRow)]`
//! generates a `FooBuffer` type with `insert()`, `schema()`, `len()`,
//! and `drain_to_record_batch()`.
//!
//! # Conversion principle
//!
//! Typed in Rust, canonical string IDs in SQL, numeric time in SQL.
//! ID columns (`node_id`, `parent_id`, `child_id`,
//! `failure_root_cause_actor`) are opaque strings derived from typed
//! refs (`NodeRef`, `ActorId`, `ProcId`) at the conversion boundary.
//! Timestamps are `i64` microseconds since epoch. Queries should
//! treat ID columns as opaque join keys — do not parse them in SQL.
//!
//! # How to read this schema
//!
//! This schema stores one mesh-admin snapshot as a small relational
//! graph.
//!
//! - [`SnapshotRow`] is the capture itself: one row per snapshot.
//! - [`NodeRow`] is the base table of all nodes seen in that snapshot.
//!   Every node has a stable opaque ID string (`node_id`) and a
//!   `node_kind` telling you which subtype table holds its properties.
//! - The subtype tables ([`RootNodeRow`], [`HostNodeRow`],
//!   [`ProcNodeRow`], [`ActorNodeRow`], [`ResolutionErrorRow`]) store
//!   the kind-specific columns for each node.
//! - [`ChildRow`] stores the edges between nodes: each row says
//!   "within this snapshot, parent P has child C at position K", plus
//!   link-level classifications such as `is_system` and `is_stopped`.
//! - [`ActorFailureRow`] stores optional failure detail for actor nodes
//!   that were failed at snapshot time.
//!
//! The important modeling choice is that SQL IDs are opaque strings even
//! though the in-memory model is typed. Conversion into these string IDs
//! happens at the boundary when rows are produced.
//!
//! Another important choice is that [`ChildRow`] models a rooted DAG, not
//! a strict tree: a node may legitimately appear under more than one
//! parent. Queries should therefore treat `children` as an edge table,
//! not assume a unique parent pointer for every non-root node.
//!
//! # Cardinality invariants
//!
//! - One snapshot has many nodes.
//! - One node can have many outgoing child links.
//! - Each node has exactly one kind-specific payload row, chosen by
//!   `node_kind`.
//! - An actor node has zero or one failure-detail row.
//! - Root never appears as a child.
//! - Non-root nodes may have more than one parent link, so the graph is
//!   a rooted DAG rather than a strict tree.

use monarch_record_batch::RecordBatchRow;

// Schema-row invariants (SR-*)
//
// These invariants are local to the Arrow schema defined in this
// module. They describe guarantees enforced by the row definitions
// and verified by the tests below.
//
// SR-1: Generated Arrow field names match the SQL-facing column names
//       (the Rust field name IS the column name).
// SR-2: ID columns (`snapshot_id`, `node_id`, `parent_id`, `child_id`,
//       `failure_root_cause_actor`) are Arrow `Utf8`.
// SR-3: Time and count columns (`snapshot_ts`, `as_of`, `started_at`,
//       `created_at`, `failure_occurred_at`, `num_hosts`,
//       `host_num_procs`, `num_actors`, `stopped_retention_cap`,
//       `failed_actor_count`, `messages_processed`,
//       `total_processing_time_us`, `child_sort_key`) are Arrow `Int64`.
// SR-4: Flag columns (`is_system`, `is_stopped`, `is_poisoned`,
//       `failure_is_propagated`) are Arrow `Boolean`.
// SR-5: Optional source fields map to nullable Arrow columns; required
//       fields are non-nullable. The optional fields are:
//       `ActorNodeRow.created_at`, `ActorNodeRow.last_message_handler`,
//       `ActorFailureRow.failure_root_cause_name`.
// SR-6: `drain_to_record_batch()` preserves row count and empties the
//       buffer (zero rows remain after drain).

/// One row per snapshot capture.
#[derive(RecordBatchRow)]
pub struct SnapshotRow {
    /// PK. Opaque UUID generated at capture time.
    pub snapshot_id: String,
    /// Capture timestamp, microseconds since epoch.
    pub snapshot_ts: i64,
}

/// Base table for all resolved nodes in a snapshot.
///
/// Tree structure is expressed solely through [`ChildRow`] — there
/// is no parent column here.
#[derive(RecordBatchRow)]
pub struct NodeRow {
    /// PK component. FK → [`SnapshotRow::snapshot_id`].
    pub snapshot_id: String,
    /// PK component. Canonical opaque node key from
    /// `NodeRef::to_string()` — do not parse in SQL.
    pub node_id: String,
    /// Discriminator: `"root"` | `"host"` | `"proc"` | `"actor"` |
    /// `"error"`. Determines which subtype table holds this node's
    /// properties.
    pub node_kind: String,
    /// Payload freshness, microseconds since epoch. From
    /// `NodePayload.as_of`.
    pub as_of: i64,
}

/// Canonical parent→child relation. The model is a rooted DAG, not a
/// strict tree: the same logical node may legitimately appear under
/// more than one parent in the admin navigation model. This happens
/// when one node is shown through multiple structural views rather
/// than because of a cycle. The TUI already accounts for this by
/// disambiguating appearances by `(reference, depth)` and rejecting
/// only true ancestor cycles.
#[derive(RecordBatchRow)]
pub struct ChildRow {
    /// PK component. FK → [`SnapshotRow::snapshot_id`].
    pub snapshot_id: String,
    /// PK component. FK → `Node(snapshot_id, node_id)`.
    pub parent_id: String,
    /// PK component. FK → `Node(snapshot_id, node_id)`.
    pub child_id: String,
    /// Sibling ordering within one parent.
    pub child_sort_key: i64,
    /// Link classification: child appears in parent's
    /// `system_children` set (Root/Host/Proc).
    pub is_system: bool,
    /// Link classification: child appears in parent's
    /// `stopped_children` set (Proc only).
    pub is_stopped: bool,
}

/// Subtype table for root nodes. PK: `(snapshot_id, node_id)`.
#[derive(RecordBatchRow)]
pub struct RootNodeRow {
    /// PK component. FK → `Node(snapshot_id, node_id)`.
    pub snapshot_id: String,
    /// PK component. Opaque node key — do not parse in SQL.
    pub node_id: String,
    /// Number of host children.
    pub num_hosts: i64,
    /// Mesh start time, microseconds since epoch.
    pub started_at: i64,
    /// Identity of the user or system that started the mesh.
    pub started_by: String,
}

/// Subtype table for host nodes. PK: `(snapshot_id, node_id)`.
#[derive(RecordBatchRow)]
pub struct HostNodeRow {
    /// PK component. FK → `Node(snapshot_id, node_id)`.
    pub snapshot_id: String,
    /// PK component. Opaque node key — do not parse in SQL.
    pub node_id: String,
    /// Network address of the host.
    pub addr: String,
    /// Number of proc children on this host.
    pub host_num_procs: i64,
}

/// Subtype table for proc nodes. PK: `(snapshot_id, node_id)`.
#[derive(RecordBatchRow)]
pub struct ProcNodeRow {
    /// PK component. FK → `Node(snapshot_id, node_id)`.
    pub snapshot_id: String,
    /// PK component. Opaque node key — do not parse in SQL.
    pub node_id: String,
    /// Proc name (e.g. `"worker"`, `"service"`).
    pub proc_name: String,
    /// Number of live (non-stopped) actor children.
    pub num_actors: i64,
    /// Maximum number of stopped actors retained.
    pub stopped_retention_cap: i64,
    /// Whether the proc is poisoned (unrecoverable error state).
    pub is_poisoned: bool,
    /// Number of failed actor children.
    pub failed_actor_count: i64,
}

/// Subtype table for actor nodes. PK: `(snapshot_id, node_id)`.
///
/// `is_system` is the actor's intrinsic classification from
/// `NodeProperties::Actor::is_system` — whether the actor was spawned
/// as a system actor. Distinct from [`ChildRow::is_system`], which
/// records whether the parent classifies the child link as system.
#[derive(RecordBatchRow)]
pub struct ActorNodeRow {
    /// PK component. FK → `Node(snapshot_id, node_id)`.
    pub snapshot_id: String,
    /// PK component. Opaque node key — do not parse in SQL.
    pub node_id: String,
    /// Actor status string (e.g. `"running"`, `"stopped:…"`,
    /// `"failed:…"`).
    pub actor_status: String,
    /// Actor type name.
    pub actor_type: String,
    /// Cumulative messages processed.
    pub messages_processed: i64,
    /// Actor creation time, microseconds since epoch. Nullable —
    /// `Option<SystemTime>` in source model.
    pub created_at: Option<i64>,
    /// Name of the last message handler invoked. Nullable —
    /// `Option<String>` in source model.
    pub last_message_handler: Option<String>,
    /// Cumulative message-processing wall time in microseconds.
    pub total_processing_time_us: i64,
    /// Intrinsic system-actor classification from the actor itself.
    pub is_system: bool,
}

/// Snapshot-time failure projection for a failed actor. PK:
/// `(snapshot_id, node_id)`, FK → `ActorNode(snapshot_id, node_id)`.
///
/// Captures the failure state as observed at snapshot time only — not
/// intended to model failure history.
#[derive(RecordBatchRow)]
pub struct ActorFailureRow {
    /// PK component. FK → `ActorNode(snapshot_id, node_id)`.
    pub snapshot_id: String,
    /// PK component. FK → `ActorNode(snapshot_id, node_id)`.
    pub node_id: String,
    /// Human-readable error message.
    pub failure_error_message: String,
    /// Opaque `ActorId.to_string()` of the root-cause actor — do not
    /// parse in SQL.
    pub failure_root_cause_actor: String,
    /// Display name of the root-cause actor. Nullable —
    /// `Option<String>` in source model.
    pub failure_root_cause_name: Option<String>,
    /// When the failure occurred, microseconds since epoch.
    pub failure_occurred_at: i64,
    /// Whether this failure was propagated from another actor.
    pub failure_is_propagated: bool,
}

/// Subtype table for resolution error nodes. PK: `(snapshot_id,
/// node_id)`.
#[derive(RecordBatchRow)]
pub struct ResolutionErrorRow {
    /// PK component. FK → `Node(snapshot_id, node_id)`.
    pub snapshot_id: String,
    /// PK component. Opaque node key — do not parse in SQL.
    pub node_id: String,
    /// Machine-readable error code.
    pub error_code: String,
    /// Human-readable error description.
    pub error_message: String,
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::DataType;
    use monarch_record_batch::RecordBatchBuffer;

    use super::*;

    // Schema-shape assertions (SR-1, SR-2, SR-3, SR-4, SR-5)

    /// Helper: assert a schema field has the expected name, type, and
    /// nullability.
    fn assert_field(
        schema: &datafusion::arrow::datatypes::Schema,
        index: usize,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) {
        let field = schema.field(index);
        assert_eq!(field.name(), name, "field {} name mismatch", index);
        assert_eq!(
            field.data_type(),
            &data_type,
            "field {} ({}) type mismatch",
            index,
            name
        );
        assert_eq!(
            field.is_nullable(),
            nullable,
            "field {} ({}) nullable mismatch",
            index,
            name
        );
    }

    // Verifies SR-1, SR-2, SR-3, SR-5.
    #[test]
    fn test_snapshot_row_schema() {
        let schema = SnapshotRowBuffer::schema();
        assert_eq!(schema.fields().len(), 2);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "snapshot_ts", DataType::Int64, false);
    }

    // Verifies SR-1, SR-2, SR-3, SR-5.
    #[test]
    fn test_node_row_schema() {
        let schema = NodeRowBuffer::schema();
        assert_eq!(schema.fields().len(), 4);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "node_id", DataType::Utf8, false);
        assert_field(&schema, 2, "node_kind", DataType::Utf8, false);
        assert_field(&schema, 3, "as_of", DataType::Int64, false);
    }

    // Verifies SR-1, SR-2, SR-3, SR-4, SR-5.
    #[test]
    fn test_child_row_schema() {
        let schema = ChildRowBuffer::schema();
        assert_eq!(schema.fields().len(), 6);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "parent_id", DataType::Utf8, false);
        assert_field(&schema, 2, "child_id", DataType::Utf8, false);
        assert_field(&schema, 3, "child_sort_key", DataType::Int64, false);
        assert_field(&schema, 4, "is_system", DataType::Boolean, false);
        assert_field(&schema, 5, "is_stopped", DataType::Boolean, false);
    }

    // Verifies SR-1, SR-2, SR-3, SR-5.
    #[test]
    fn test_root_node_row_schema() {
        let schema = RootNodeRowBuffer::schema();
        assert_eq!(schema.fields().len(), 5);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "node_id", DataType::Utf8, false);
        assert_field(&schema, 2, "num_hosts", DataType::Int64, false);
        assert_field(&schema, 3, "started_at", DataType::Int64, false);
        assert_field(&schema, 4, "started_by", DataType::Utf8, false);
    }

    // Verifies SR-1, SR-2, SR-3, SR-5.
    #[test]
    fn test_host_node_row_schema() {
        let schema = HostNodeRowBuffer::schema();
        assert_eq!(schema.fields().len(), 4);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "node_id", DataType::Utf8, false);
        assert_field(&schema, 2, "addr", DataType::Utf8, false);
        assert_field(&schema, 3, "host_num_procs", DataType::Int64, false);
    }

    // Verifies SR-1, SR-2, SR-3, SR-4, SR-5.
    #[test]
    fn test_proc_node_row_schema() {
        let schema = ProcNodeRowBuffer::schema();
        assert_eq!(schema.fields().len(), 7);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "node_id", DataType::Utf8, false);
        assert_field(&schema, 2, "proc_name", DataType::Utf8, false);
        assert_field(&schema, 3, "num_actors", DataType::Int64, false);
        assert_field(&schema, 4, "stopped_retention_cap", DataType::Int64, false);
        assert_field(&schema, 5, "is_poisoned", DataType::Boolean, false);
        assert_field(&schema, 6, "failed_actor_count", DataType::Int64, false);
    }

    // Verifies SR-1, SR-2, SR-3, SR-4, SR-5.
    #[test]
    fn test_actor_node_row_schema() {
        let schema = ActorNodeRowBuffer::schema();
        assert_eq!(schema.fields().len(), 9);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "node_id", DataType::Utf8, false);
        assert_field(&schema, 2, "actor_status", DataType::Utf8, false);
        assert_field(&schema, 3, "actor_type", DataType::Utf8, false);
        assert_field(&schema, 4, "messages_processed", DataType::Int64, false);
        // Genuinely optional source fields → nullable
        assert_field(&schema, 5, "created_at", DataType::Int64, true);
        assert_field(&schema, 6, "last_message_handler", DataType::Utf8, true);
        assert_field(
            &schema,
            7,
            "total_processing_time_us",
            DataType::Int64,
            false,
        );
        assert_field(&schema, 8, "is_system", DataType::Boolean, false);
    }

    // Verifies SR-1, SR-2, SR-3, SR-4, SR-5.
    #[test]
    fn test_actor_failure_row_schema() {
        let schema = ActorFailureRowBuffer::schema();
        assert_eq!(schema.fields().len(), 7);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "node_id", DataType::Utf8, false);
        assert_field(&schema, 2, "failure_error_message", DataType::Utf8, false);
        assert_field(
            &schema,
            3,
            "failure_root_cause_actor",
            DataType::Utf8,
            false,
        );
        // Genuinely optional source field → nullable
        assert_field(&schema, 4, "failure_root_cause_name", DataType::Utf8, true);
        assert_field(&schema, 5, "failure_occurred_at", DataType::Int64, false);
        assert_field(
            &schema,
            6,
            "failure_is_propagated",
            DataType::Boolean,
            false,
        );
    }

    // Verifies SR-1, SR-2, SR-5.
    #[test]
    fn test_resolution_error_row_schema() {
        let schema = ResolutionErrorRowBuffer::schema();
        assert_eq!(schema.fields().len(), 4);
        assert_field(&schema, 0, "snapshot_id", DataType::Utf8, false);
        assert_field(&schema, 1, "node_id", DataType::Utf8, false);
        assert_field(&schema, 2, "error_code", DataType::Utf8, false);
        assert_field(&schema, 3, "error_message", DataType::Utf8, false);
    }

    // Drain round-trip tests (SR-5, SR-6)

    // Verifies SR-6.
    #[test]
    fn test_drain_snapshot_row() {
        let mut buf = SnapshotRowBuffer::default();
        buf.insert(SnapshotRow {
            snapshot_id: "snap-1".into(),
            snapshot_ts: 1_000_000,
        });
        buf.insert(SnapshotRow {
            snapshot_id: "snap-2".into(),
            snapshot_ts: 2_000_000,
        });
        assert_eq!(buf.len(), 2);

        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(buf.is_empty());

        let ids = batch
            .column_by_name("snapshot_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "snap-1");
        assert_eq!(ids.value(1), "snap-2");

        let ts = batch
            .column_by_name("snapshot_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 1_000_000);
        assert_eq!(ts.value(1), 2_000_000);
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_node_row() {
        let mut buf = NodeRowBuffer::default();
        buf.insert(NodeRow {
            snapshot_id: "s1".into(),
            node_id: "root".into(),
            node_kind: "root".into(),
            as_of: 100,
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let kind = batch
            .column_by_name("node_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(kind.value(0), "root");
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_child_row() {
        let mut buf = ChildRowBuffer::default();
        buf.insert(ChildRow {
            snapshot_id: "s1".into(),
            parent_id: "root".into(),
            child_id: "host:abc".into(),
            child_sort_key: 0,
            is_system: false,
            is_stopped: false,
        });
        buf.insert(ChildRow {
            snapshot_id: "s1".into(),
            parent_id: "root".into(),
            child_id: "host:def".into(),
            child_sort_key: 1,
            is_system: true,
            is_stopped: false,
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let sys = batch
            .column_by_name("is_system")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(!sys.value(0));
        assert!(sys.value(1));
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_root_node_row() {
        let mut buf = RootNodeRowBuffer::default();
        buf.insert(RootNodeRow {
            snapshot_id: "s1".into(),
            node_id: "root".into(),
            num_hosts: 3,
            started_at: 500_000,
            started_by: "user@meta.com".into(),
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let hosts = batch
            .column_by_name("num_hosts")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(hosts.value(0), 3);
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_host_node_row() {
        let mut buf = HostNodeRowBuffer::default();
        buf.insert(HostNodeRow {
            snapshot_id: "s1".into(),
            node_id: "host:abc".into(),
            addr: "10.0.0.1:8080".into(),
            host_num_procs: 2,
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let addr = batch
            .column_by_name("addr")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(addr.value(0), "10.0.0.1:8080");
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_proc_node_row() {
        let mut buf = ProcNodeRowBuffer::default();
        buf.insert(ProcNodeRow {
            snapshot_id: "s1".into(),
            node_id: "proc-001".into(),
            proc_name: "worker".into(),
            num_actors: 10,
            stopped_retention_cap: 100,
            is_poisoned: false,
            failed_actor_count: 1,
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let poisoned = batch
            .column_by_name("is_poisoned")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(!poisoned.value(0));
    }

    // Verifies SR-5, SR-6.
    #[test]
    fn test_drain_actor_node_row() {
        let mut buf = ActorNodeRowBuffer::default();
        // Row with optional fields present
        buf.insert(ActorNodeRow {
            snapshot_id: "s1".into(),
            node_id: "actor-1".into(),
            actor_status: "running".into(),
            actor_type: "Philosopher".into(),
            messages_processed: 42,
            created_at: Some(999_000),
            last_message_handler: Some("grant_chopstick".into()),
            total_processing_time_us: 5000,
            is_system: false,
        });
        // Row with optional fields absent
        buf.insert(ActorNodeRow {
            snapshot_id: "s1".into(),
            node_id: "actor-2".into(),
            actor_status: "idle".into(),
            actor_type: "Waiter".into(),
            messages_processed: 0,
            created_at: None,
            last_message_handler: None,
            total_processing_time_us: 0,
            is_system: true,
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Check optional i64 field
        let created = batch
            .column_by_name("created_at")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert!(!created.is_null(0));
        assert_eq!(created.value(0), 999_000);
        assert!(created.is_null(1));

        // Check optional String field
        let handler = batch
            .column_by_name("last_message_handler")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert!(!handler.is_null(0));
        assert_eq!(handler.value(0), "grant_chopstick");
        assert!(handler.is_null(1));

        // Check non-optional bool
        let sys = batch
            .column_by_name("is_system")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(!sys.value(0));
        assert!(sys.value(1));
    }

    // Verifies SR-5, SR-6.
    #[test]
    fn test_drain_actor_failure_row() {
        let mut buf = ActorFailureRowBuffer::default();
        // With optional root_cause_name present
        buf.insert(ActorFailureRow {
            snapshot_id: "s1".into(),
            node_id: "actor-1".into(),
            failure_error_message: "panicked at division by zero".into(),
            failure_root_cause_actor: "actor-1".into(),
            failure_root_cause_name: Some("Philosopher".into()),
            failure_occurred_at: 888_000,
            failure_is_propagated: false,
        });
        // With optional root_cause_name absent
        buf.insert(ActorFailureRow {
            snapshot_id: "s1".into(),
            node_id: "actor-2".into(),
            failure_error_message: "propagated failure".into(),
            failure_root_cause_actor: "actor-1".into(),
            failure_root_cause_name: None,
            failure_occurred_at: 889_000,
            failure_is_propagated: true,
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let name = batch
            .column_by_name("failure_root_cause_name")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert!(!name.is_null(0));
        assert_eq!(name.value(0), "Philosopher");
        assert!(name.is_null(1));

        let propagated = batch
            .column_by_name("failure_is_propagated")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(!propagated.value(0));
        assert!(propagated.value(1));
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_resolution_error_row() {
        let mut buf = ResolutionErrorRowBuffer::default();
        buf.insert(ResolutionErrorRow {
            snapshot_id: "s1".into(),
            node_id: "err-1".into(),
            error_code: "NOT_FOUND".into(),
            error_message: "node not found".into(),
        });
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let code = batch
            .column_by_name("error_code")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(code.value(0), "NOT_FOUND");
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_empties_buffer() {
        let mut buf = NodeRowBuffer::default();
        buf.insert(NodeRow {
            snapshot_id: "s1".into(),
            node_id: "root".into(),
            node_kind: "root".into(),
            as_of: 100,
        });
        assert_eq!(buf.len(), 1);
        let _ = buf.drain_to_record_batch().unwrap();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    // Verifies SR-6.
    #[test]
    fn test_drain_empty_buffer() {
        let mut buf = ChildRowBuffer::default();
        let batch = buf.drain_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
