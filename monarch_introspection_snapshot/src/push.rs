/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Drain [`SnapshotData`] into [`TableStore`] tables.
//!
//! The shared infrastructure is [`drain_to_batches`], which converts
//! a [`SnapshotData`] into 9 named `RecordBatch` pairs. The public
//! entry point [`push_snapshot`] uses this to ingest all tables into
//! a [`TableStore`].
//!
//! # Push-snapshot invariants (PS-*)
//!
//! - **PS-1 (full table coverage):** Every `push_snapshot` call
//!   ingests all nine logical tables, including empty ones.
//! - **PS-2 (canonical routing):** Each row family goes only to its
//!   canonical table.
//! - **PS-3 (append-preserving counts):** Each table's row count
//!   increases by exactly the number of source rows for that family.
//! - **PS-4 (empty-table queryability):** Empty row families still
//!   become registered, queryable tables through
//!   [`TableStore::table_provider()`].
//! - **PS-5 (snapshot anchor):** The [`SnapshotRow`] is always
//!   persisted to `snapshots`, including when all per-node vectors
//!   are empty.
//! - **PS-6 (query handoff):** After push, all registered tables can
//!   be wired into a DataFusion `SessionContext` without extra
//!   adaptation.
//! - **PS-7 (error propagation):** Drain/ingest failure returns `Err`
//!   and does not silently skip tables.

use datafusion::arrow::record_batch::RecordBatch;
use monarch_distributed_telemetry::database_scanner::TableStore;
use monarch_record_batch::RecordBatchBuffer;

use crate::capture::SnapshotData;
use crate::schema::ActorFailureRowBuffer;
use crate::schema::ActorNodeRowBuffer;
use crate::schema::ChildRowBuffer;
use crate::schema::HostNodeRowBuffer;
use crate::schema::NodeRowBuffer;
use crate::schema::ProcNodeRowBuffer;
use crate::schema::ResolutionErrorRowBuffer;
use crate::schema::RootNodeRowBuffer;
use crate::schema::SnapshotRowBuffer;

/// Canonical table names in sorted order.
///
/// Every snapshot contains exactly these 9 tables. Both
/// [`drain_to_batches`] and [`push_snapshot`] use this ordering.
pub const SNAPSHOT_TABLE_NAMES: &[&str] = &[
    "actor_failures",
    "actor_nodes",
    "children",
    "host_nodes",
    "nodes",
    "proc_nodes",
    "resolution_errors",
    "root_nodes",
    "snapshots",
];

/// A table name paired with its drained `RecordBatch`.
pub type NamedBatch = (&'static str, RecordBatch);

/// Drain all row families in `data` to named `RecordBatch` pairs.
///
/// Returns exactly 9 pairs, one per table, in
/// [`SNAPSHOT_TABLE_NAMES`] order. Empty families produce zero-row
/// batches with correct schemas (PS-4).
pub fn drain_to_batches(data: SnapshotData) -> anyhow::Result<Vec<NamedBatch>> {
    // Each block: create buffer, insert rows, drain to RecordBatch.
    // Order matches SNAPSHOT_TABLE_NAMES (sorted).

    let mut failure_buf = ActorFailureRowBuffer::default();
    for row in data.actor_failures {
        failure_buf.insert(row);
    }

    let mut actor_buf = ActorNodeRowBuffer::default();
    for row in data.actor_nodes {
        actor_buf.insert(row);
    }

    let mut child_buf = ChildRowBuffer::default();
    for row in data.children {
        child_buf.insert(row);
    }

    let mut host_buf = HostNodeRowBuffer::default();
    for row in data.host_nodes {
        host_buf.insert(row);
    }

    let mut node_buf = NodeRowBuffer::default();
    for row in data.nodes {
        node_buf.insert(row);
    }

    let mut proc_buf = ProcNodeRowBuffer::default();
    for row in data.proc_nodes {
        proc_buf.insert(row);
    }

    let mut error_buf = ResolutionErrorRowBuffer::default();
    for row in data.resolution_errors {
        error_buf.insert(row);
    }

    // Logically singleton per snapshot (CS-2: root resolved exactly
    // once), but modeled as a row family like the other subtype
    // tables.
    let mut root_buf = RootNodeRowBuffer::default();
    for row in data.root_nodes {
        root_buf.insert(row);
    }

    // PS-5: snapshot anchor is always persisted.
    let mut snapshot_buf = SnapshotRowBuffer::default();
    snapshot_buf.insert(data.snapshot);

    Ok(vec![
        ("actor_failures", failure_buf.drain_to_record_batch()?),
        ("actor_nodes", actor_buf.drain_to_record_batch()?),
        ("children", child_buf.drain_to_record_batch()?),
        ("host_nodes", host_buf.drain_to_record_batch()?),
        ("nodes", node_buf.drain_to_record_batch()?),
        ("proc_nodes", proc_buf.drain_to_record_batch()?),
        ("resolution_errors", error_buf.drain_to_record_batch()?),
        ("root_nodes", root_buf.drain_to_record_batch()?),
        ("snapshots", snapshot_buf.drain_to_record_batch()?),
    ])
}

/// Drain a captured snapshot into table storage.
///
/// Calls [`drain_to_batches`] and ingests each batch into its
/// canonical table via [`TableStore::ingest_batch`]. Empty families
/// produce zero-row batches that still register the table schema
/// (PS-4).
pub async fn push_snapshot(table_store: &TableStore, data: SnapshotData) -> anyhow::Result<()> {
    let batches = drain_to_batches(data)?;
    for (name, batch) in batches {
        table_store.ingest_batch(name, batch).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::reference::ProcId;
    use hyperactor_mesh::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
    use hyperactor_mesh::introspect::NodeRef;

    use super::*;
    use crate::schema::*;

    // Canonical ID fixtures — same pattern as convert.rs /
    // capture.rs.

    const PROC_NAME: &str = "worker";
    const ACTOR_NAME: &str = "test_actor";

    fn test_proc_id() -> ProcId {
        ProcId::with_name(ChannelAddr::Local(0), PROC_NAME)
    }

    fn test_host_ref() -> NodeRef {
        NodeRef::Host(test_proc_id().actor_id(HOST_MESH_AGENT_ACTOR_NAME))
    }

    fn test_proc_ref() -> NodeRef {
        NodeRef::Proc(test_proc_id())
    }

    fn test_actor_ref() -> NodeRef {
        NodeRef::Actor(test_proc_id().actor_id(ACTOR_NAME))
    }

    /// Expected table names in sorted order (PS-1). Alias for the
    /// public constant — existing tests reference this name.
    const ALL_TABLE_NAMES: &[&str] = SNAPSHOT_TABLE_NAMES;

    /// Register all tables from a `TableStore` into a
    /// `SessionContext`.
    async fn register_all(store: &TableStore, ctx: &SessionContext) -> anyhow::Result<()> {
        for name in store.table_names()? {
            if let Some(provider) = store.table_provider(&name)? {
                ctx.register_table(&name, provider)?;
            }
        }
        Ok(())
    }

    /// Query row count for a table in a `SessionContext`.
    async fn query_row_count(ctx: &SessionContext, table: &str) -> anyhow::Result<i64> {
        let df = ctx
            .sql(&format!("SELECT COUNT(*) AS cnt FROM {table}"))
            .await?;
        let batches = df.collect().await?;
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column_by_name("cnt")
            .expect("cnt column missing");
        let arr = col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("cnt not Int64");
        Ok(arr.value(0))
    }

    /// Build a minimal `SnapshotData` with only a snapshot row (all
    /// node vectors empty).
    fn minimal_snapshot(id: &str) -> SnapshotData {
        SnapshotData {
            snapshot: SnapshotRow {
                snapshot_id: id.to_owned(),
                snapshot_ts: 1_000_000,
            },
            nodes: vec![],
            children: vec![],
            root_nodes: vec![],
            host_nodes: vec![],
            proc_nodes: vec![],
            actor_nodes: vec![],
            actor_failures: vec![],
            resolution_errors: vec![],
        }
    }

    /// Build a representative populated snapshot for count tests.
    ///
    /// Uses canonical IDs from typed refs (CV-6) — the same boundary
    /// crossing that `convert_node` applies in production.
    fn populated_snapshot(id: &str) -> SnapshotData {
        let host_id = test_host_ref().to_string();
        let proc_id = test_proc_ref().to_string();
        let actor_id = test_actor_ref().to_string();
        let failed_actor_id = NodeRef::Actor(test_proc_id().actor_id("failed_actor")).to_string();
        let error_id = NodeRef::Actor(test_proc_id().actor_id("missing")).to_string();

        SnapshotData {
            snapshot: SnapshotRow {
                snapshot_id: id.to_owned(),
                snapshot_ts: 1_000_000,
            },
            nodes: vec![
                NodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: "root".to_owned(),
                    node_kind: "root".to_owned(),
                    as_of: 1_000_000,
                },
                NodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: host_id.clone(),
                    node_kind: "host".to_owned(),
                    as_of: 1_000_000,
                },
                NodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: proc_id.clone(),
                    node_kind: "proc".to_owned(),
                    as_of: 1_000_000,
                },
                NodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: actor_id.clone(),
                    node_kind: "actor".to_owned(),
                    as_of: 1_000_000,
                },
                NodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: failed_actor_id.clone(),
                    node_kind: "actor".to_owned(),
                    as_of: 1_000_000,
                },
                NodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: error_id.clone(),
                    node_kind: "error".to_owned(),
                    as_of: 1_000_000,
                },
            ],
            children: vec![
                ChildRow {
                    snapshot_id: id.to_owned(),
                    parent_id: "root".to_owned(),
                    child_id: host_id.clone(),
                    child_sort_key: 0,
                    is_system: false,
                    is_stopped: false,
                },
                ChildRow {
                    snapshot_id: id.to_owned(),
                    parent_id: host_id.clone(),
                    child_id: proc_id.clone(),
                    child_sort_key: 0,
                    is_system: false,
                    is_stopped: false,
                },
                ChildRow {
                    snapshot_id: id.to_owned(),
                    parent_id: proc_id.clone(),
                    child_id: actor_id.clone(),
                    child_sort_key: 0,
                    is_system: false,
                    is_stopped: false,
                },
                ChildRow {
                    snapshot_id: id.to_owned(),
                    parent_id: proc_id.clone(),
                    child_id: failed_actor_id.clone(),
                    child_sort_key: 1,
                    is_system: false,
                    is_stopped: false,
                },
                ChildRow {
                    snapshot_id: id.to_owned(),
                    parent_id: proc_id.clone(),
                    child_id: error_id.clone(),
                    child_sort_key: 2,
                    is_system: false,
                    is_stopped: false,
                },
            ],
            root_nodes: vec![RootNodeRow {
                snapshot_id: id.to_owned(),
                node_id: "root".to_owned(),
                num_hosts: 1,
                started_at: 1_000_000,
                started_by: "test".to_owned(),
            }],
            host_nodes: vec![HostNodeRow {
                snapshot_id: id.to_owned(),
                node_id: host_id,
                addr: "10.0.0.1".to_owned(),
                host_num_procs: 1,
            }],
            proc_nodes: vec![ProcNodeRow {
                snapshot_id: id.to_owned(),
                node_id: proc_id,
                proc_name: PROC_NAME.to_owned(),
                num_actors: 2,
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 1,
            }],
            actor_nodes: vec![
                ActorNodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: actor_id,
                    actor_status: "running".to_owned(),
                    actor_type: ACTOR_NAME.to_owned(),
                    messages_processed: 42,
                    created_at: Some(900_000),
                    last_message_handler: Some("handle_msg".to_owned()),
                    total_processing_time_us: 5000,
                    is_system: false,
                },
                ActorNodeRow {
                    snapshot_id: id.to_owned(),
                    node_id: failed_actor_id.clone(),
                    actor_status: "failed".to_owned(),
                    actor_type: "failed_actor".to_owned(),
                    messages_processed: 1,
                    created_at: Some(800_000),
                    last_message_handler: None,
                    total_processing_time_us: 100,
                    is_system: false,
                },
            ],
            actor_failures: vec![ActorFailureRow {
                snapshot_id: id.to_owned(),
                node_id: failed_actor_id,
                failure_error_message: "division by zero".to_owned(),
                failure_root_cause_actor: "self".to_owned(),
                failure_root_cause_name: None,
                failure_occurred_at: 850_000,
                failure_is_propagated: false,
            }],
            resolution_errors: vec![ResolutionErrorRow {
                snapshot_id: id.to_owned(),
                node_id: error_id,
                error_code: "not_found".to_owned(),
                error_message: "node not found".to_owned(),
            }],
        }
    }

    // PS-1: every push ingests all nine tables, including empty ones.
    #[tokio::test]
    async fn test_push_snapshot_registers_all_tables() {
        let store = TableStore::new_empty();
        push_snapshot(&store, minimal_snapshot("s1")).await.unwrap();

        let names = store.table_names().unwrap();
        assert_eq!(
            names,
            ALL_TABLE_NAMES
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            "PS-1: all nine tables should be registered"
        );
    }

    // PS-3, PS-5: row counts match source vector lengths on a fresh
    // store.
    #[tokio::test]
    async fn test_push_snapshot_preserves_counts_on_fresh_store() {
        let store = TableStore::new_empty();
        push_snapshot(&store, populated_snapshot("s1"))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        register_all(&store, &ctx).await.unwrap();

        assert_eq!(query_row_count(&ctx, "snapshots").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "nodes").await.unwrap(), 6);
        assert_eq!(query_row_count(&ctx, "children").await.unwrap(), 5);
        assert_eq!(query_row_count(&ctx, "root_nodes").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "host_nodes").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "proc_nodes").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "actor_nodes").await.unwrap(), 2);
        assert_eq!(query_row_count(&ctx, "actor_failures").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "resolution_errors").await.unwrap(), 1);
    }

    // PS-3: cumulative counts after two pushes into the same store.
    #[tokio::test]
    async fn test_push_snapshot_appends_on_existing_store() {
        let store = TableStore::new_empty();
        push_snapshot(&store, populated_snapshot("s1"))
            .await
            .unwrap();
        push_snapshot(&store, populated_snapshot("s2"))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        register_all(&store, &ctx).await.unwrap();

        // Each push contributes the same counts; cumulative = 2×.
        assert_eq!(query_row_count(&ctx, "snapshots").await.unwrap(), 2);
        assert_eq!(query_row_count(&ctx, "nodes").await.unwrap(), 12);
        assert_eq!(query_row_count(&ctx, "children").await.unwrap(), 10);
        assert_eq!(query_row_count(&ctx, "root_nodes").await.unwrap(), 2);
        assert_eq!(query_row_count(&ctx, "host_nodes").await.unwrap(), 2);
        assert_eq!(query_row_count(&ctx, "proc_nodes").await.unwrap(), 2);
        assert_eq!(query_row_count(&ctx, "actor_nodes").await.unwrap(), 4);
        assert_eq!(query_row_count(&ctx, "actor_failures").await.unwrap(), 2);
        assert_eq!(query_row_count(&ctx, "resolution_errors").await.unwrap(), 2);
    }

    // PS-4, PS-6: empty subtype tables are still queryable.
    #[tokio::test]
    async fn test_push_snapshot_keeps_empty_tables_queryable() {
        let store = TableStore::new_empty();
        // minimal_snapshot has all per-node vectors empty.
        push_snapshot(&store, minimal_snapshot("s1")).await.unwrap();

        let ctx = SessionContext::new();
        register_all(&store, &ctx).await.unwrap();

        // PS-4: tables exist and are queryable even with zero rows.
        assert_eq!(query_row_count(&ctx, "nodes").await.unwrap(), 0);
        assert_eq!(query_row_count(&ctx, "actor_nodes").await.unwrap(), 0);
        assert_eq!(query_row_count(&ctx, "actor_failures").await.unwrap(), 0);
        assert_eq!(query_row_count(&ctx, "resolution_errors").await.unwrap(), 0);
    }

    // PS-1, PS-6: every table_provider() is usable in SessionContext.
    #[tokio::test]
    async fn test_push_snapshot_table_provider_query_smoke() {
        let store = TableStore::new_empty();
        push_snapshot(&store, populated_snapshot("s1"))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        register_all(&store, &ctx).await.unwrap();

        // Query every table — no errors.
        for name in ALL_TABLE_NAMES {
            let count = query_row_count(&ctx, name).await;
            assert!(
                count.is_ok(),
                "PS-6: query on table '{}' should succeed, got {:?}",
                name,
                count.err()
            );
        }
    }

    /// Helper: run a SQL query and return the first row's value for
    /// the named column as a String.
    async fn query_string_scalar(
        ctx: &SessionContext,
        sql: &str,
        column: &str,
    ) -> anyhow::Result<String> {
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;
        assert_eq!(batches.len(), 1);
        assert!(batches[0].num_rows() >= 1);
        let col = batches[0]
            .column_by_name(column)
            .ok_or_else(|| anyhow::anyhow!("column '{}' not found", column))?;
        let arr = col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("column '{}' not Utf8", column))?;
        Ok(arr.value(0).to_owned())
    }

    // PS-2: each row family lands in its canonical table, not a
    // different one. Count-based tests cannot catch swapped table
    // names when two tables have the same row count.
    #[tokio::test]
    async fn test_push_snapshot_canonical_routing() {
        let store = TableStore::new_empty();
        push_snapshot(&store, populated_snapshot("s1"))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        register_all(&store, &ctx).await.unwrap();

        // snapshots — distinguishing column: snapshot_id.
        let snap_id = query_string_scalar(
            &ctx,
            "SELECT snapshot_id FROM snapshots LIMIT 1",
            "snapshot_id",
        )
        .await
        .unwrap();
        assert_eq!(snap_id, "s1", "PS-2: snapshots table");

        // root_nodes — distinguishing column: started_by.
        let started_by = query_string_scalar(
            &ctx,
            "SELECT started_by FROM root_nodes LIMIT 1",
            "started_by",
        )
        .await
        .unwrap();
        assert_eq!(started_by, "test", "PS-2: root_nodes table");

        // host_nodes — distinguishing column: addr.
        let addr = query_string_scalar(&ctx, "SELECT addr FROM host_nodes LIMIT 1", "addr")
            .await
            .unwrap();
        assert_eq!(addr, "10.0.0.1", "PS-2: host_nodes table");

        // proc_nodes — distinguishing column: proc_name.
        let proc_name = query_string_scalar(
            &ctx,
            "SELECT proc_name FROM proc_nodes LIMIT 1",
            "proc_name",
        )
        .await
        .unwrap();
        assert_eq!(proc_name, PROC_NAME, "PS-2: proc_nodes table");

        // actor_nodes — distinguishing column: actor_type.
        let actor_type = query_string_scalar(
            &ctx,
            "SELECT actor_type FROM actor_nodes \
             WHERE actor_status = 'running' LIMIT 1",
            "actor_type",
        )
        .await
        .unwrap();
        assert_eq!(actor_type, ACTOR_NAME, "PS-2: actor_nodes table");

        // actor_failures — distinguishing column: failure_error_message.
        let err_msg = query_string_scalar(
            &ctx,
            "SELECT failure_error_message FROM actor_failures LIMIT 1",
            "failure_error_message",
        )
        .await
        .unwrap();
        assert_eq!(err_msg, "division by zero", "PS-2: actor_failures table");

        // resolution_errors — distinguishing column: error_code.
        let error_code = query_string_scalar(
            &ctx,
            "SELECT error_code FROM resolution_errors LIMIT 1",
            "error_code",
        )
        .await
        .unwrap();
        assert_eq!(error_code, "not_found", "PS-2: resolution_errors table");
    }

    // --- drain_to_batches tests ---

    // PS-1, PS-2: exactly 9 pairs in canonical order.
    #[test]
    fn test_drain_to_batches_produces_nine_pairs() {
        let batches = drain_to_batches(populated_snapshot("d1")).unwrap();
        let names: Vec<&str> = batches.iter().map(|(n, _)| *n).collect();
        assert_eq!(names.len(), 9);
        assert_eq!(names, SNAPSHOT_TABLE_NAMES.to_vec(),);
    }

    // PS-4, PS-5: empty families produce valid batches; snapshot
    // anchor is always present.
    #[test]
    fn test_drain_to_batches_empty_families() {
        let batches = drain_to_batches(minimal_snapshot("d2")).unwrap();
        assert_eq!(batches.len(), 9);
        // All tables except "snapshots" should have 0 rows.
        for (name, batch) in &batches {
            if *name == "snapshots" {
                assert_eq!(batch.num_rows(), 1, "snapshots should have 1 row");
            } else {
                assert_eq!(batch.num_rows(), 0, "{} should have 0 rows", name);
            }
            // Schema should always be present (non-zero column count).
            assert!(
                batch.num_columns() > 0,
                "{} should have a valid schema",
                name,
            );
        }
    }

    // PS-3: row counts in drained batches match source vector
    // lengths.
    #[test]
    fn test_drain_to_batches_row_counts() {
        let batches = drain_to_batches(populated_snapshot("d3")).unwrap();
        let counts: std::collections::HashMap<&str, usize> =
            batches.iter().map(|(n, b)| (*n, b.num_rows())).collect();
        assert_eq!(counts["snapshots"], 1);
        assert_eq!(counts["nodes"], 6);
        assert_eq!(counts["children"], 5);
        assert_eq!(counts["root_nodes"], 1);
        assert_eq!(counts["host_nodes"], 1);
        assert_eq!(counts["proc_nodes"], 1);
        assert_eq!(counts["actor_nodes"], 2);
        assert_eq!(counts["actor_failures"], 1);
        assert_eq!(counts["resolution_errors"], 1);
    }
}
