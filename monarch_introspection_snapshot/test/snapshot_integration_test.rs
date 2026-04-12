/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Bridge-layer integration test: capture → push → SQL.
//!
//! Proves the full Rust-side path from live mesh capture through
//! `push_snapshot` ingestion to DataFusion SQL queries over the
//! normalized snapshot tables.
//!
//! Uses only public `hyperactor_mesh` APIs. The fixture is a
//! single-host multiprocess mesh via `HostMesh::local()` — closer to
//! production than the in-process variant. The `mesh_admin.rs`
//! white-box tests use `pub(crate)` shortcuts not available here.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor_mesh::global_context::context;
use hyperactor_mesh::host_mesh::HostMesh;
use hyperactor_mesh::host_mesh::spawn_admin;
use hyperactor_mesh::introspect::NodeRef;
use hyperactor_mesh::mesh_admin::ResolveReferenceMessageClient;
use monarch_distributed_telemetry::database_scanner::TableStore;
use monarch_introspection_snapshot::capture::capture_snapshot;
use monarch_introspection_snapshot::integration::register_snapshot_schemas;
use monarch_introspection_snapshot::integration::start_periodic_snapshots;
use monarch_introspection_snapshot::push::push_snapshot;
use ndslice::extent;
use ndslice::view::Ranked;

// -- Trivial test actor for the fixture --
//
// Minimal RemoteSpawn actor: Default gives us the blanket RemoteSpawn
// impl with Params = (). The () handler satisfies the export
// requirement for at least one handler.

#[derive(Default, Debug)]
#[hyperactor::export(spawn = true, handlers = [()])]
struct SnapshotTestActor;

impl Actor for SnapshotTestActor {}

#[async_trait]
impl Handler<()> for SnapshotTestActor {
    async fn handle(&mut self, _cx: &Context<Self>, _: ()) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

// -- Test helpers --

/// Register all tables from a `TableStore` into a `SessionContext`.
async fn register_all(store: &TableStore, ctx: &SessionContext) -> Result<()> {
    for name in store.table_names()? {
        if let Some(provider) = store.table_provider(&name)? {
            ctx.register_table(&name, provider)?;
        }
    }
    Ok(())
}

/// Run a SQL query and return the result as a single concatenated
/// `RecordBatch`. Handles the case where DataFusion returns multiple
/// output batches by merging them.
async fn query_batch(ctx: &SessionContext, sql: &str) -> Result<RecordBatch> {
    use datafusion::arrow::compute::concat_batches;
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    if batches.is_empty() {
        anyhow::bail!("query returned no batches");
    }
    let schema = batches[0].schema();
    Ok(concat_batches(&schema, &batches)?)
}

fn col_str(batch: &RecordBatch, name: &str, row: usize) -> String {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column '{name}' not Utf8"))
        .value(row)
        .to_owned()
}

fn col_i64(batch: &RecordBatch, name: &str, row: usize) -> i64 {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column '{name}' not Int64"))
        .value(row)
}

fn col_bool(batch: &RecordBatch, name: &str, row: usize) -> bool {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("column '{name}' not Boolean"))
        .value(row)
}

fn is_null(batch: &RecordBatch, col: &str, row: usize) -> bool {
    batch
        .column_by_name(col)
        .unwrap_or_else(|| panic!("column '{col}' not found"))
        .is_null(row)
}

// -- The integration test --

#[tokio::test]
async fn test_snapshot_sql_queries() -> Result<()> {
    // Step 1: Global context and mesh.
    let cx = context().await;
    let instance = cx.actor_instance;
    let host_mesh = HostMesh::local().await?;

    // Step 2: Spawn two worker procs, each with one test actor.
    let proc_mesh = host_mesh
        .spawn(&instance, "worker", extent!(replica = 2), None)
        .await?;
    let actor_mesh = proc_mesh
        .spawn::<SnapshotTestActor, _>(&instance, "test_actor", &())
        .await?;

    // Step 3: Spawn admin on the caller-local proc.
    let admin_ref = spawn_admin([&host_mesh], &instance, Some("[::]:0".parse()?), None).await?;

    // Capture deterministic fixture-owned IDs via typed refs.
    let proc_0_ref = proc_mesh.get(0).expect("proc at rank 0");
    let proc_0_id = NodeRef::Proc(proc_0_ref.proc_id().clone()).to_string();

    let actor_0_ref = actor_mesh.get(0).expect("actor at rank 0");
    let actor_0_id = NodeRef::Actor(actor_0_ref.actor_id().clone()).to_string();

    // Step 4: Build the resolver closure.
    let resolve = |node_ref: &NodeRef| {
        let admin_ref = admin_ref.clone();
        let ref_string = node_ref.to_string();
        async move {
            let resp = admin_ref.resolve(instance, ref_string).await?;
            resp.0.map_err(|e| anyhow::anyhow!("{}", e))
        }
    };

    // Step 5: Capture and push.
    let data = capture_snapshot("test_snap", resolve).await?;
    let table_store = TableStore::new_empty();
    push_snapshot(&table_store, data).await?;

    // Step 6: Register all tables in DataFusion.
    let ctx = SessionContext::new();
    register_all(&table_store, &ctx).await?;

    // PS-1: all nine tables registered.
    assert_eq!(
        table_store.table_names()?.len(),
        9,
        "PS-1: all nine tables should be registered"
    );

    // PS-5: exactly one snapshot row.
    let snap = query_batch(&ctx, "SELECT 1 FROM snapshots").await?;
    assert_eq!(snap.num_rows(), 1, "PS-5: exactly one snapshot row");

    // Query A: actor detail view for one actor in one snapshot.
    // Joins the base node row to actor-specific columns, and LEFT
    // JOINs failure state so healthy actors yield NULL failure
    // fields.
    let a = query_batch(
        &ctx,
        &format!(
            r#"
        SELECT n.node_id, a.actor_type, a.actor_status,
               a.messages_processed, a.is_system,
               f.failure_error_message
        FROM nodes n
        JOIN actor_nodes a
          ON a.snapshot_id = n.snapshot_id AND a.node_id = n.node_id
        LEFT JOIN actor_failures f
          ON f.snapshot_id = a.snapshot_id AND f.node_id = a.node_id
        WHERE n.snapshot_id = 'test_snap'
          AND n.node_id = '{actor_0_id}'
    "#
        ),
    )
    .await?;
    assert_eq!(a.num_rows(), 1, "Query A: exactly 1 row");
    assert!(
        !is_null(&a, "actor_type", 0),
        "Query A: actor_type non-null"
    );
    assert!(
        !is_null(&a, "actor_status", 0),
        "Query A: actor_status non-null"
    );
    assert!(
        is_null(&a, "failure_error_message", 0),
        "Query A: healthy actor, failure null"
    );

    // Query B: fetch the proc's direct children in stored order.
    // Reads edge metadata from `children` and joins to `nodes` to learn
    // what kind of node each child is.
    let b = query_batch(
        &ctx,
        &format!(
            r#"
        SELECT ch.child_sort_key, c.node_id, c.node_kind,
               ch.is_system, ch.is_stopped
        FROM children ch
        JOIN nodes c
          ON c.snapshot_id = ch.snapshot_id AND c.node_id = ch.child_id
        WHERE ch.snapshot_id = 'test_snap'
          AND ch.parent_id = '{proc_0_id}'
        ORDER BY ch.child_sort_key
    "#
        ),
    )
    .await?;
    assert!(b.num_rows() >= 1, "Query B: proc should have children");
    let mut found_user_actor = false;
    let mut has_system_child = false;
    for row in 0..b.num_rows() {
        assert_eq!(
            col_str(&b, "node_kind", row),
            "actor",
            "Query B: all proc children are actors"
        );
        assert!(!col_bool(&b, "is_stopped", row), "Query B: not stopped");
        if col_str(&b, "node_id", row) == actor_0_id {
            found_user_actor = true;
            assert!(
                !col_bool(&b, "is_system", row),
                "Query B: user actor is not system"
            );
        }
        if col_bool(&b, "is_system", row) {
            has_system_child = true;
        }
    }
    assert!(
        found_user_actor,
        "Query B: user actor should appear as proc child"
    );
    assert!(
        has_system_child,
        "Query B: proc should have at least one system child"
    );

    // Query C: proc page view.
    // Returns one row per direct child, repeating the proc summary
    // fields and joining actor-specific columns for each child actor.
    let c = query_batch(
        &ctx,
        &format!(
            r#"
        SELECT p.proc_name, p.num_actors, p.is_poisoned,
               c.node_id AS child_id, a.actor_type, ch.child_sort_key
        FROM proc_nodes p
        LEFT JOIN children ch
          ON ch.snapshot_id = p.snapshot_id AND ch.parent_id = p.node_id
        LEFT JOIN nodes c
          ON c.snapshot_id = ch.snapshot_id AND c.node_id = ch.child_id
        LEFT JOIN actor_nodes a
          ON a.snapshot_id = c.snapshot_id AND a.node_id = c.node_id
        WHERE p.snapshot_id = 'test_snap'
          AND p.node_id = '{proc_0_id}'
        ORDER BY ch.child_sort_key
    "#
        ),
    )
    .await?;
    assert!(c.num_rows() >= 1, "Query C: proc should have children");
    let mut found_user_actor_c = false;
    for row in 0..c.num_rows() {
        let proc_name = col_str(&c, "proc_name", row);
        assert!(
            proc_name.contains("worker"),
            "Query C: proc_name contains 'worker', got '{proc_name}'"
        );
        assert!(!col_bool(&c, "is_poisoned", row), "Query C: not poisoned");
        assert!(
            !is_null(&c, "actor_type", row),
            "Query C: all children are actors"
        );
        if !is_null(&c, "child_id", row) && col_str(&c, "child_id", row) == actor_0_id {
            found_user_actor_c = true;
        }
    }
    assert!(
        found_user_actor_c,
        "Query C: user actor should appear in proc child rows"
    );

    // Query D: ancestry breadcrumb for one actor.
    // Seeds the CTE with the actor itself, then recursively walks
    // upward through `children` from child -> parent and returns the
    // path ordered by depth.
    let d = query_batch(
        &ctx,
        &format!(
            r#"
        WITH RECURSIVE ancestry AS (
            SELECT n.snapshot_id, n.node_id, n.node_kind, 0 AS depth
            FROM nodes n
            WHERE n.snapshot_id = 'test_snap'
              AND n.node_id = '{actor_0_id}'
            UNION ALL
            SELECT p.snapshot_id, p.node_id, p.node_kind, a.depth + 1
            FROM children ch
            JOIN ancestry a
              ON ch.snapshot_id = a.snapshot_id AND ch.child_id = a.node_id
            JOIN nodes p
              ON p.snapshot_id = ch.snapshot_id AND p.node_id = ch.parent_id
        )
        SELECT node_id, node_kind, depth
        FROM ancestry
        ORDER BY depth
    "#
        ),
    )
    .await?;
    assert_eq!(
        d.num_rows(),
        4,
        "Query D: 4 ancestors (actor -> proc -> host -> root)"
    );
    assert_eq!(col_str(&d, "node_kind", 0), "actor");
    assert_eq!(col_i64(&d, "depth", 0), 0);
    assert_eq!(col_str(&d, "node_kind", 1), "proc");
    assert_eq!(col_i64(&d, "depth", 1), 1);
    assert_eq!(col_str(&d, "node_kind", 2), "host");
    assert_eq!(col_i64(&d, "depth", 2), 2);
    assert_eq!(col_str(&d, "node_kind", 3), "root");
    assert_eq!(col_i64(&d, "depth", 3), 3);

    // Cleanup: shutdown the mesh.
    let mut host_mesh = host_mesh;
    host_mesh.shutdown(&instance).await?;

    Ok(())
}

/// PT-1: zero interval rejected at the `start_periodic_snapshots`
/// boundary.
#[tokio::test]
async fn test_pt1_rejects_zero_interval() -> Result<()> {
    let cx = context().await;
    let instance = cx.actor_instance;
    let host_mesh = HostMesh::local().await?;
    let admin_ref = spawn_admin([&host_mesh], &instance, Some("[::]:0".parse()?), None).await?;
    let table_store = TableStore::new_empty();

    let err = start_periodic_snapshots(&instance, table_store, admin_ref.clone(), Duration::ZERO);
    assert!(err.is_err(), "PT-1: zero interval must be rejected");
    assert!(
        err.unwrap_err().to_string().contains("non-zero"),
        "PT-1: error must mention non-zero",
    );

    let mut host_mesh = host_mesh;
    host_mesh.shutdown(&instance).await?;
    Ok(())
}

/// PT-3: first capture fires at spawn time (immediate, not delayed).
#[tokio::test]
async fn test_pt3_immediate_first_capture() -> Result<()> {
    let cx = context().await;
    let instance = cx.actor_instance;
    let host_mesh = HostMesh::local().await?;
    let admin_ref = spawn_admin([&host_mesh], &instance, Some("[::]:0".parse()?), None).await?;

    let table_store = TableStore::new_empty();
    register_snapshot_schemas(&table_store).await?;

    // Use a long interval so only the initial immediate capture fires.
    start_periodic_snapshots(
        &instance,
        table_store.clone(),
        admin_ref.clone(),
        Duration::from_secs(600),
    )?;

    // Give the immediate capture time to complete.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let ctx = SessionContext::new();
    register_all(&table_store, &ctx).await?;
    let batch = query_batch(&ctx, "SELECT COUNT(*) AS cnt FROM snapshots").await?;
    let count = col_i64(&batch, "cnt", 0);
    assert!(
        count >= 1,
        "PT-3: at least one capture should fire immediately, got {}",
        count,
    );

    // Stop the actor and clean up.
    let actor_id = instance.proc().proc_id().actor_id("snapshot_capture", 0);
    instance
        .proc()
        .stop_actor(&actor_id, "PT-3 test cleanup".to_string());

    let mut host_mesh = host_mesh;
    host_mesh.shutdown(&instance).await?;
    Ok(())
}

/// PT-5: after proc shutdown, snapshot count stabilizes. The actor
/// may complete one in-flight or drained capture during DrainAndStop,
/// but does not reschedule indefinitely.
#[tokio::test]
async fn test_pt5_drain_halts_future_captures() -> Result<()> {
    let cx = context().await;
    let instance = cx.actor_instance;
    let host_mesh = HostMesh::local().await?;
    let admin_ref = spawn_admin([&host_mesh], &instance, Some("[::]:0".parse()?), None).await?;

    let table_store = TableStore::new_empty();
    register_snapshot_schemas(&table_store).await?;

    // Start periodic capture with a short interval.
    start_periodic_snapshots(
        &instance,
        table_store.clone(),
        admin_ref.clone(),
        Duration::from_millis(200),
    )?;

    // Let a few captures run.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify captures actually ran before stopping.
    let count_before_stop = {
        let ctx = SessionContext::new();
        register_all(&table_store, &ctx).await?;
        let batch = query_batch(&ctx, "SELECT COUNT(*) AS cnt FROM snapshots").await?;
        col_i64(&batch, "cnt", 0)
    };
    assert!(
        count_before_stop > 0,
        "PT-5: expected positive snapshot count before stop, got {}",
        count_before_stop,
    );

    // Stop the snapshot actor directly. In production, job teardown
    // stops the proc which stops all actors on it.
    let actor_id = instance.proc().proc_id().actor_id("snapshot_capture", 0);
    let status_rx = instance
        .proc()
        .stop_actor(&actor_id, "PT-5 test shutdown".to_string());
    if let Some(mut rx) = status_rx {
        // Wait for the actor to reach a terminal state.
        while !rx.borrow().is_terminal() {
            rx.changed().await.ok();
        }
    }
    // Small headroom for any async cleanup.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Record snapshot count after actor stop.
    let count_at_shutdown = {
        let ctx = SessionContext::new();
        register_all(&table_store, &ctx).await?;
        let batch = query_batch(&ctx, "SELECT COUNT(*) AS cnt FROM snapshots").await?;
        col_i64(&batch, "cnt", 0)
    };

    // Wait to verify no further captures fire.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let count_after_wait = {
        let ctx = SessionContext::new();
        register_all(&table_store, &ctx).await?;
        let batch = query_batch(&ctx, "SELECT COUNT(*) AS cnt FROM snapshots").await?;
        col_i64(&batch, "cnt", 0)
    };

    // PT-5: snapshot count must not keep increasing after shutdown.
    assert_eq!(
        count_at_shutdown, count_after_wait,
        "PT-5: snapshot count should stabilize after shutdown \
         (got {} at shutdown, {} after 2s wait)",
        count_at_shutdown, count_after_wait,
    );

    Ok(())
}
