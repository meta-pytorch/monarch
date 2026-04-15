/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Durable snapshot bundle export and import.
//!
//! A snapshot bundle is a directory containing 9 Arrow IPC files (one
//! per table) and a JSON manifest. It represents exactly one snapshot
//! and is the persistence mechanism for historical TUI debugging.
//!
//! # Bundle invariants (BN-*)
//!
//! - **BN-1 (one snapshot per bundle):** One bundle directory
//!   represents exactly one `snapshot_id`. `snapshots.arrow` contains
//!   exactly one row.
//! - **BN-2 (one batch per table file):** Each `{table}.arrow` file
//!   contains exactly one `RecordBatch`. `write_bundle` writes one
//!   batch; `import_snapshot_bundle` reads one batch.
//! - **BN-3 (portable manifest):** `manifest.json` contains no
//!   path-dependent fields. The bundle remains valid if the directory
//!   is moved or copied.
//! - **BN-4 (canonical timestamp):** `snapshot_ts` is the only
//!   persisted timestamp in the manifest. No `created_at`.
//! - **BN-5 (manifest written last):** `manifest.json` is written
//!   only after all table files are successfully written. An
//!   incomplete bundle has no manifest and is rejected by import.
//! - **BN-6 (import rejects invalid bundles):**
//!   `import_snapshot_bundle` errors on: missing `manifest.json`,
//!   unparseable manifest, unknown `version`, `tables` not matching
//!   [`SNAPSHOT_TABLE_NAMES`] exactly (same entries, same order), or
//!   missing `.arrow` file for any listed table.
//! - **BN-7 (path derivation ownership):** The service generates
//!   `snapshot_id` and derives
//!   `{export_root}/snapshot-{snapshot_id}/`. The directory name and
//!   the snapshot ID cannot diverge.
//! - **BN-8 (round-trip fidelity):** For any `SnapshotData`,
//!   `capture(export_root=some) → import → query` produces identical
//!   SQL results to `capture(table_store=some) → query`.

use std::fs;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::Path;

use anyhow::Context;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int64Type;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use monarch_distributed_telemetry::database_scanner::TableStore;
use serde::Deserialize;
use serde::Serialize;

use crate::push::NamedBatch;
use crate::push::SNAPSHOT_TABLE_NAMES;
use crate::service::NodeCounts;

/// Current bundle format version.
const BUNDLE_VERSION: u32 = 1;

/// Portable metadata for a snapshot bundle.
///
/// Persisted as `manifest.json` inside the bundle directory. Contains
/// no path-dependent fields (BN-3) and no `created_at` (BN-4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleManifest {
    /// Format version. Currently always `1`.
    pub version: u32,
    /// Unique identifier for the snapshot (UUID v4).
    pub snapshot_id: String,
    /// Capture timestamp in microseconds since epoch (BN-4).
    pub snapshot_ts: i64,
    /// Summary counts of captured entities.
    pub node_counts: NodeCounts,
    /// Table names in canonical order, matching
    /// [`SNAPSHOT_TABLE_NAMES`].
    pub tables: Vec<String>,
}

/// Extract `snapshot_id` and `snapshot_ts` from the snapshots batch.
///
/// The snapshots batch must have exactly one row (BN-1) with columns
/// `snapshot_id` (Utf8) and `snapshot_ts` (Int64).
fn extract_snapshot_row(batch: &RecordBatch) -> anyhow::Result<(String, i64)> {
    anyhow::ensure!(
        batch.num_rows() == 1,
        "snapshots batch must have exactly 1 row, found {}",
        batch.num_rows(),
    );
    let id_col = batch
        .column_by_name("snapshot_id")
        .context("snapshots batch missing snapshot_id column")?;
    let ts_col = batch
        .column_by_name("snapshot_ts")
        .context("snapshots batch missing snapshot_ts column")?;
    let id = id_col.as_string::<i32>().value(0).to_owned();
    let ts = ts_col.as_primitive::<Int64Type>().value(0);
    Ok((id, ts))
}

/// Derive [`NodeCounts`] from batch row counts (write path).
fn counts_from_batches(batches: &[NamedBatch]) -> NodeCounts {
    let row_count = |name: &str| -> usize {
        batches
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, b)| b.num_rows())
            .unwrap_or(0)
    };
    NodeCounts {
        nodes: row_count("nodes"),
        children: row_count("children"),
        root_nodes: row_count("root_nodes"),
        host_nodes: row_count("host_nodes"),
        proc_nodes: row_count("proc_nodes"),
        actor_nodes: row_count("actor_nodes"),
        actor_failures: row_count("actor_failures"),
        resolution_errors: row_count("resolution_errors"),
    }
}

/// Derive [`NodeCounts`] from loaded table name/row-count pairs
/// (import path).
fn counts_from_loaded(loaded: &[(&str, usize)]) -> NodeCounts {
    let row_count = |name: &str| -> usize {
        loaded
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, c)| *c)
            .unwrap_or(0)
    };
    NodeCounts {
        nodes: row_count("nodes"),
        children: row_count("children"),
        root_nodes: row_count("root_nodes"),
        host_nodes: row_count("host_nodes"),
        proc_nodes: row_count("proc_nodes"),
        actor_nodes: row_count("actor_nodes"),
        actor_failures: row_count("actor_failures"),
        resolution_errors: row_count("resolution_errors"),
    }
}

/// Write a snapshot bundle to `dir`.
///
/// All manifest metadata is derived from the batches themselves —
/// `snapshot_id`, `snapshot_ts`, and `node_counts` are read from the
/// data, not supplied separately. This makes it impossible to write
/// a bundle whose manifest disagrees with its table files.
///
/// Creates `dir` (errors if it already exists), writes one `.arrow`
/// file per table, then writes `manifest.json` last (BN-5).
///
/// On failure, the partially written directory is left in place — no
/// cleanup attempt. Incomplete bundles lack `manifest.json` and are
/// rejected by [`import_snapshot_bundle`].
pub fn write_bundle(dir: &Path, batches: &[NamedBatch]) -> anyhow::Result<()> {
    // Validate that batches matches SNAPSHOT_TABLE_NAMES exactly
    // before writing anything.
    let batch_names: Vec<&str> = batches.iter().map(|(n, _)| *n).collect();
    let expected_names: Vec<&str> = SNAPSHOT_TABLE_NAMES.to_vec();
    anyhow::ensure!(
        batch_names == expected_names,
        "batches {:?} do not match expected {:?}",
        batch_names,
        expected_names,
    );

    // Derive manifest metadata from the batches.
    let snapshots_batch = &batches
        .iter()
        .find(|(n, _)| *n == "snapshots")
        .context("missing snapshots batch")?
        .1;
    let (snapshot_id, snapshot_ts) = extract_snapshot_row(snapshots_batch)?;
    let node_counts = counts_from_batches(batches);

    // BN-5: create dir first; error if exists (UUID collision = bug).
    fs::create_dir(dir).with_context(|| format!("failed to create bundle directory {:?}", dir))?;

    // Write table files before manifest (BN-5).
    for (name, batch) in batches {
        let path = dir.join(format!("{}.arrow", name));
        let file =
            fs::File::create(&path).with_context(|| format!("failed to create {:?}", path))?;
        let writer = BufWriter::new(file);
        let mut ipc_writer = FileWriter::try_new(writer, &batch.schema())
            .with_context(|| format!("failed to create IPC writer for {:?}", path))?;
        ipc_writer
            .write(batch)
            .with_context(|| format!("failed to write batch to {:?}", path))?;
        ipc_writer
            .finish()
            .with_context(|| format!("failed to finish IPC file {:?}", path))?;
    }

    // BN-5: manifest written last, derived from batch data.
    let manifest = BundleManifest {
        version: BUNDLE_VERSION,
        snapshot_id,
        snapshot_ts,
        node_counts,
        tables: SNAPSHOT_TABLE_NAMES.iter().map(|s| s.to_string()).collect(),
    };
    let manifest_path = dir.join("manifest.json");
    let manifest_file = fs::File::create(&manifest_path)
        .with_context(|| format!("failed to create {:?}", manifest_path))?;
    serde_json::to_writer_pretty(BufWriter::new(manifest_file), &manifest)
        .context("failed to write manifest.json")?;

    Ok(())
}

/// Import a snapshot bundle from `dir`.
///
/// Validates the manifest (BN-6), reads one `RecordBatch` per table
/// file (BN-2), and ingests into a fresh [`TableStore`].
pub async fn import_snapshot_bundle(dir: &Path) -> anyhow::Result<(TableStore, BundleManifest)> {
    // Read and validate manifest.
    let manifest_path = dir.join("manifest.json");
    let manifest_file = fs::File::open(&manifest_path)
        .with_context(|| format!("failed to open {:?}", manifest_path))?;
    let manifest: BundleManifest = serde_json::from_reader(BufReader::new(manifest_file))
        .with_context(|| format!("failed to parse {:?}", manifest_path))?;

    // BN-6: validate version.
    anyhow::ensure!(
        manifest.version == BUNDLE_VERSION,
        "unsupported bundle version {} (expected {})",
        manifest.version,
        BUNDLE_VERSION,
    );

    // BN-6: validate tables match SNAPSHOT_TABLE_NAMES exactly.
    let expected: Vec<String> = SNAPSHOT_TABLE_NAMES.iter().map(|s| s.to_string()).collect();
    anyhow::ensure!(
        manifest.tables == expected,
        "manifest tables {:?} do not match expected {:?}",
        manifest.tables,
        expected,
    );

    // Load each table file and ingest into a fresh store.
    // Track row counts per table for node_counts cross-check.
    let store = TableStore::new_empty();
    let mut loaded_batches: Vec<(&str, usize)> = Vec::new();
    for table_name in &manifest.tables {
        let path = dir.join(format!("{}.arrow", table_name));
        let file = fs::File::open(&path).with_context(|| format!("failed to open {:?}", path))?;
        let reader = FileReader::try_new(BufReader::new(file), None)
            .with_context(|| format!("failed to create IPC reader for {:?}", path))?;

        // BN-2: read exactly one batch.
        let mut batch_count = 0;
        let mut row_count = 0;
        for batch_result in reader {
            let batch =
                batch_result.with_context(|| format!("failed to read batch from {:?}", path))?;

            // BN-1: verify snapshots table has one row and matches
            // the manifest.
            if table_name == "snapshots" {
                let (row_id, row_ts) = extract_snapshot_row(&batch)
                    .context("snapshots.arrow failed BN-1 validation")?;
                anyhow::ensure!(
                    row_id == manifest.snapshot_id,
                    "snapshots.arrow snapshot_id {:?} does not match \
                     manifest {:?}",
                    row_id,
                    manifest.snapshot_id,
                );
                anyhow::ensure!(
                    row_ts == manifest.snapshot_ts,
                    "snapshots.arrow snapshot_ts {} does not match \
                     manifest {}",
                    row_ts,
                    manifest.snapshot_ts,
                );
            }

            row_count += batch.num_rows();
            store.ingest_batch(table_name, batch).await?;
            batch_count += 1;
        }
        anyhow::ensure!(
            batch_count == 1,
            "expected exactly 1 batch in {:?}, found {}",
            path,
            batch_count,
        );
        loaded_batches.push((table_name, row_count));
    }

    // Cross-check manifest.node_counts against actual file row
    // counts. The manifest was derived from the batches on the write
    // side, so a mismatch here indicates tampering or corruption.
    let actual_counts = counts_from_loaded(&loaded_batches);
    anyhow::ensure!(
        actual_counts == manifest.node_counts,
        "manifest node_counts {:?} do not match actual file row \
         counts {:?}",
        manifest.node_counts,
        actual_counts,
    );

    Ok((store, manifest))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::reference::ProcId;
    use hyperactor_mesh::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
    use hyperactor_mesh::introspect::NodeRef;

    use super::*;
    use crate::capture::SnapshotData;
    use crate::push::drain_to_batches;
    use crate::schema::*;

    // --- Fixtures ---

    const PROC_NAME: &str = "worker";
    const ACTOR_TYPE: &str = "test_actor";

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
        NodeRef::Actor(test_proc_id().actor_id(ACTOR_TYPE))
    }

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

    fn populated_snapshot(id: &str) -> SnapshotData {
        let host_id = test_host_ref().to_string();
        let proc_id = test_proc_ref().to_string();
        let actor_id = test_actor_ref().to_string();

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
                num_actors: 1,
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            }],
            actor_nodes: vec![ActorNodeRow {
                snapshot_id: id.to_owned(),
                node_id: actor_id,
                actor_status: "running".to_owned(),
                actor_type: ACTOR_TYPE.to_owned(),
                messages_processed: 42,
                created_at: Some(900_000),
                last_message_handler: Some("handle_msg".to_owned()),
                total_processing_time_us: 5000,
                is_system: false,
            }],
            actor_failures: vec![],
            resolution_errors: vec![],
        }
    }

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

    // --- BN-1, BN-2: one snapshot per bundle, one batch per file ---

    // BN-1: snapshots.arrow contains exactly one row.
    // BN-2: each table file has exactly one batch.
    #[tokio::test]
    async fn test_bundle_one_snapshot_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-test1");
        let data = populated_snapshot("test1");
        let batches = drain_to_batches(data).unwrap();

        write_bundle(&bundle_dir, &batches).unwrap();

        // Re-read each file and verify exactly one batch.
        for table_name in SNAPSHOT_TABLE_NAMES {
            let path = bundle_dir.join(format!("{}.arrow", table_name));
            let file = fs::File::open(&path).unwrap();
            let reader = FileReader::try_new(BufReader::new(file), None).unwrap();
            let file_batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
            assert_eq!(
                file_batches.len(),
                1,
                "BN-2: {} should have exactly 1 batch",
                table_name,
            );
        }

        // BN-1: snapshots table has exactly 1 row.
        let snapshots_path = bundle_dir.join("snapshots.arrow");
        let file = fs::File::open(&snapshots_path).unwrap();
        let reader = FileReader::try_new(BufReader::new(file), None).unwrap();
        let snap_batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(snap_batches[0].num_rows(), 1, "BN-1: one snapshot row");
    }

    // --- BN-3, BN-4: portable manifest ---

    // BN-3: manifest contains no path-dependent fields.
    // BN-4: snapshot_ts is the only timestamp.
    #[test]
    fn test_bundle_manifest_portable() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-mfst");
        let data = populated_snapshot("mfst");
        let batches = drain_to_batches(data).unwrap();

        write_bundle(&bundle_dir, &batches).unwrap();

        // Read raw JSON to verify no extra fields.
        let raw = fs::read_to_string(bundle_dir.join("manifest.json")).unwrap();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let obj = value.as_object().unwrap();

        // BN-3: no bundle_path in manifest.
        assert!(!obj.contains_key("bundle_path"), "BN-3: no bundle_path");
        // BN-4: no created_at in manifest.
        assert!(!obj.contains_key("created_at"), "BN-4: no created_at");

        // Only expected keys.
        let expected_keys: Vec<&str> = vec![
            "version",
            "snapshot_id",
            "snapshot_ts",
            "node_counts",
            "tables",
        ];
        for key in &expected_keys {
            assert!(obj.contains_key(*key), "missing expected key: {}", key);
        }
        assert_eq!(obj.len(), expected_keys.len(), "unexpected extra fields");

        // Verify values.
        assert_eq!(obj["version"], 1);
        assert_eq!(obj["snapshot_id"], "mfst");
        assert_eq!(obj["snapshot_ts"], 1_000_000);
    }

    // --- BN-5: manifest written last / write_bundle errors on
    // existing dir ---

    // BN-5: write_bundle errors if directory already exists.
    #[test]
    fn test_bundle_write_errors_on_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-dup");
        fs::create_dir(&bundle_dir).unwrap();

        let data = minimal_snapshot("dup");
        let batches = drain_to_batches(data).unwrap();

        let err = write_bundle(&bundle_dir, &batches);
        assert!(err.is_err(), "BN-5: should error on existing dir");
    }

    // --- BN-6: import rejects invalid bundles ---

    // BN-6: missing manifest.json.
    #[tokio::test]
    async fn test_import_rejects_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let err = import_snapshot_bundle(dir.path()).await;
        assert!(err.is_err(), "BN-6: should reject missing manifest");
    }

    // BN-6: unparseable manifest.json.
    #[tokio::test]
    async fn test_import_rejects_malformed_manifest() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("manifest.json"), "not valid json {{{").unwrap();
        let err = import_snapshot_bundle(dir.path()).await;
        assert!(err.is_err(), "BN-6: should reject malformed manifest");
    }

    // BN-6: unknown version.
    #[tokio::test]
    async fn test_import_rejects_unknown_version() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-badver");
        let data = minimal_snapshot("badver");
        let batches = drain_to_batches(data).unwrap();
        write_bundle(&bundle_dir, &batches).unwrap();

        // Tamper with version.
        let manifest_path = bundle_dir.join("manifest.json");
        let mut manifest: BundleManifest =
            serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        manifest.version = 99;
        fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        let result = import_snapshot_bundle(&bundle_dir).await;
        let Err(e) = result else {
            panic!("BN-6: should reject unknown version");
        };
        assert!(
            e.to_string().contains("unsupported bundle version"),
            "BN-6: error should mention version, got: {}",
            e,
        );
    }

    // BN-6: wrong table order in manifest.
    #[tokio::test]
    async fn test_import_rejects_wrong_table_order() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-badord");
        let data = minimal_snapshot("badord");
        let batches = drain_to_batches(data).unwrap();
        write_bundle(&bundle_dir, &batches).unwrap();

        // Tamper: reverse table order.
        let manifest_path = bundle_dir.join("manifest.json");
        let mut manifest: BundleManifest =
            serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        manifest.tables.reverse();
        fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        let err = import_snapshot_bundle(&bundle_dir).await;
        assert!(err.is_err(), "BN-6: should reject wrong table order");
    }

    // BN-6: missing .arrow file.
    #[tokio::test]
    async fn test_import_rejects_missing_arrow_file() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-nofile");
        let data = minimal_snapshot("nofile");
        let batches = drain_to_batches(data).unwrap();
        write_bundle(&bundle_dir, &batches).unwrap();

        // Delete one .arrow file.
        fs::remove_file(bundle_dir.join("nodes.arrow")).unwrap();

        let err = import_snapshot_bundle(&bundle_dir).await;
        assert!(err.is_err(), "BN-6: should reject missing arrow file");
    }

    // --- BN-8: round-trip fidelity ---

    // BN-8: export → import → query produces same row counts as
    // direct drain_to_batches → TableStore ingest.
    #[tokio::test]
    async fn test_bundle_round_trip_row_counts() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-rt");
        let data = populated_snapshot("rt");
        let batches = drain_to_batches(data).unwrap();

        write_bundle(&bundle_dir, &batches).unwrap();
        let (store, manifest) = import_snapshot_bundle(&bundle_dir).await.unwrap();

        // Verify manifest.
        assert_eq!(manifest.snapshot_id, "rt");
        assert_eq!(manifest.snapshot_ts, 1_000_000);
        assert_eq!(manifest.version, BUNDLE_VERSION);

        // Register and query.
        let ctx = SessionContext::new();
        register_all(&store, &ctx).await.unwrap();

        assert_eq!(query_row_count(&ctx, "snapshots").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "nodes").await.unwrap(), 4);
        assert_eq!(query_row_count(&ctx, "children").await.unwrap(), 3);
        assert_eq!(query_row_count(&ctx, "root_nodes").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "host_nodes").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "proc_nodes").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "actor_nodes").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "actor_failures").await.unwrap(), 0);
        assert_eq!(query_row_count(&ctx, "resolution_errors").await.unwrap(), 0);
    }

    // BN-8: empty-table families produce valid .arrow files that
    // import correctly.
    #[tokio::test]
    async fn test_bundle_round_trip_empty_tables() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-empty");
        let data = minimal_snapshot("empty");
        let batches = drain_to_batches(data).unwrap();

        write_bundle(&bundle_dir, &batches).unwrap();
        let (store, _manifest) = import_snapshot_bundle(&bundle_dir).await.unwrap();

        let ctx = SessionContext::new();
        register_all(&store, &ctx).await.unwrap();

        // All tables registered and queryable.
        assert_eq!(query_row_count(&ctx, "snapshots").await.unwrap(), 1);
        assert_eq!(query_row_count(&ctx, "nodes").await.unwrap(), 0);
        assert_eq!(query_row_count(&ctx, "actor_failures").await.unwrap(), 0);
        assert_eq!(query_row_count(&ctx, "resolution_errors").await.unwrap(), 0);
    }

    // BN-8: query equivalence — import-path and push-path produce
    // identical results for a representative query.
    #[tokio::test]
    async fn test_bundle_query_equivalence_with_push() {
        // Push path.
        let push_store = TableStore::new_empty();
        let push_data = populated_snapshot("equiv");
        let push_batches = drain_to_batches(push_data).unwrap();
        for (name, batch) in &push_batches {
            push_store.ingest_batch(name, batch.clone()).await.unwrap();
        }

        // Import path.
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-equiv");
        let import_data = populated_snapshot("equiv");
        let import_batches = drain_to_batches(import_data).unwrap();
        write_bundle(&bundle_dir, &import_batches).unwrap();
        let (import_store, _) = import_snapshot_bundle(&bundle_dir).await.unwrap();

        // Query both paths.
        let push_ctx = SessionContext::new();
        register_all(&push_store, &push_ctx).await.unwrap();
        let import_ctx = SessionContext::new();
        register_all(&import_store, &import_ctx).await.unwrap();

        // Compare row counts for every table.
        for table in SNAPSHOT_TABLE_NAMES {
            let push_count = query_row_count(&push_ctx, table).await.unwrap();
            let import_count = query_row_count(&import_ctx, table).await.unwrap();
            assert_eq!(
                push_count, import_count,
                "BN-8: row count mismatch for table {}",
                table,
            );
        }

        // Spot-check a representative value.
        let push_df = push_ctx
            .sql("SELECT proc_name FROM proc_nodes LIMIT 1")
            .await
            .unwrap();
        let push_result = push_df.collect().await.unwrap();
        let import_df = import_ctx
            .sql("SELECT proc_name FROM proc_nodes LIMIT 1")
            .await
            .unwrap();
        let import_result = import_df.collect().await.unwrap();
        assert_eq!(push_result, import_result, "BN-8: value mismatch");
    }

    // BN-6: manifest with missing table entry.
    #[tokio::test]
    async fn test_import_rejects_missing_table_entry() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-misstbl");
        let data = minimal_snapshot("misstbl");
        let batches = drain_to_batches(data).unwrap();
        write_bundle(&bundle_dir, &batches).unwrap();

        // Tamper: remove a table entry from manifest.
        let manifest_path = bundle_dir.join("manifest.json");
        let mut manifest: BundleManifest =
            serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        manifest.tables.pop(); // Remove last table.
        fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        let err = import_snapshot_bundle(&bundle_dir).await;
        assert!(err.is_err(), "BN-6: should reject missing table entry");
    }

    // BN-3: manifest tables match SNAPSHOT_TABLE_NAMES exactly.
    #[test]
    fn test_bundle_manifest_tables_canonical_order() {
        let dir = tempfile::tempdir().unwrap();
        let bundle_dir = dir.path().join("snapshot-order");
        let data = minimal_snapshot("order");
        let batches = drain_to_batches(data).unwrap();

        write_bundle(&bundle_dir, &batches).unwrap();

        let raw = fs::read_to_string(bundle_dir.join("manifest.json")).unwrap();
        let manifest: BundleManifest = serde_json::from_str(&raw).unwrap();

        let expected: Vec<String> = SNAPSHOT_TABLE_NAMES.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            manifest.tables, expected,
            "BN-3: tables must match SNAPSHOT_TABLE_NAMES exactly",
        );
    }
}
