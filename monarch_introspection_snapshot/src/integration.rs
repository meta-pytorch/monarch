/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Integration helpers for wiring snapshot capture into the live
//! telemetry system.
//!
//! - [`register_snapshot_schemas`] pre-registers empty snapshot
//!   tables so the `QueryEngine` discovers them at setup time.
//! - [`start_periodic_snapshots`] spawns a
//!   [`SnapshotCaptureActor`](crate::service::SnapshotCaptureActor)
//!   on the given proc.
//!
//! # Snapshot integration invariants (SI-*)
//!
//! - **SI-1 (snapshot tables discoverable):** After
//!   `register_snapshot_schemas`, the 9 snapshot table names appear
//!   in `DatabaseScanner.table_names()` and are discoverable by
//!   `QueryEngine.setup_tables()`.
//! - **SI-2 (snapshot tables queryable):** After periodic capture
//!   fires, snapshot rows are queryable through the live telemetry
//!   query path.
//! - **SI-3 (shared storage):** The `TableStore` handle used by
//!   `SnapshotService` shares the same underlying storage as the
//!   `DatabaseScanner`. Snapshot ingestion is visible to telemetry
//!   queries immediately.
//! - **SI-4 (startup ordering):** Schema pre-registration happens
//!   after `DatabaseScanner` creation but before `QueryEngine`
//!   construction. Periodic snapshots start only after both telemetry
//!   and admin are running.
//! - **SI-5 (shutdown):** The snapshot capture actor is stopped by
//!   framework lifecycle (proc teardown via `DrainAndStop`). The
//!   framework guarantees the current handler runs to completion.
//!   After stop, snapshot count stabilizes. No Python code calls stop
//!   explicitly.
//! - **SI-6 (unconditional schemas):** Schema pre-registration runs
//!   whenever telemetry starts, regardless of whether periodic
//!   capture is enabled. The query schema does not depend on config.
//! - **SI-7 (resolver provenance):** The resolver's
//!   `ActorRef<MeshAdminAgent>` comes directly from `spawn_admin`'s
//!   typed return value. It crosses the Python boundary only as an
//!   opaque capability token (`PyMeshAdminRef`). It is not
//!   reconstructed from actor identity.

use std::time::Duration;

use hyperactor_mesh::mesh_admin::MeshAdminAgent;
use monarch_distributed_telemetry::database_scanner::TableStore;
use monarch_record_batch::RecordBatchBuffer;

use crate::schema::ActorFailureRowBuffer;
use crate::schema::ActorNodeRowBuffer;
use crate::schema::ChildRowBuffer;
use crate::schema::HostNodeRowBuffer;
use crate::schema::NodeRowBuffer;
use crate::schema::ProcNodeRowBuffer;
use crate::schema::ResolutionErrorRowBuffer;
use crate::schema::RootNodeRowBuffer;
use crate::schema::SnapshotRowBuffer;
use crate::service::CaptureSnapshot;
use crate::service::SnapshotCaptureActor;

/// Pre-register the 9 snapshot table schemas into `table_store`.
///
/// Each table is registered with a zero-row `RecordBatch` carrying
/// the correct Arrow schema. This must be called before the
/// `QueryEngine` constructs its `SessionContext`, because table
/// discovery is static (one-shot at construction time).
///
/// Uses the same pattern as pyspy table pre-registration in
/// `DatabaseScanner::new()` (`database_scanner.rs:251`).
pub async fn register_snapshot_schemas(table_store: &TableStore) -> anyhow::Result<()> {
    // Order matches SNAPSHOT_TABLE_NAMES (sorted).
    let batches = [
        (
            "actor_failures",
            ActorFailureRowBuffer::default().drain_to_record_batch()?,
        ),
        (
            "actor_nodes",
            ActorNodeRowBuffer::default().drain_to_record_batch()?,
        ),
        (
            "children",
            ChildRowBuffer::default().drain_to_record_batch()?,
        ),
        (
            "host_nodes",
            HostNodeRowBuffer::default().drain_to_record_batch()?,
        ),
        ("nodes", NodeRowBuffer::default().drain_to_record_batch()?),
        (
            "proc_nodes",
            ProcNodeRowBuffer::default().drain_to_record_batch()?,
        ),
        (
            "resolution_errors",
            ResolutionErrorRowBuffer::default().drain_to_record_batch()?,
        ),
        (
            "root_nodes",
            RootNodeRowBuffer::default().drain_to_record_batch()?,
        ),
        (
            "snapshots",
            SnapshotRowBuffer::default().drain_to_record_batch()?,
        ),
    ];

    for (name, batch) in batches {
        table_store.ingest_batch(name, batch).await?;
    }

    Ok(())
}

/// Spawn periodic snapshot capture as a `SnapshotCaptureActor`.
///
/// The actor is spawned on the given proc (same proc as the mesh
/// admin). Lifecycle is framework-managed: proc teardown stops the
/// actor via `DrainAndStop`. Fire-and-forget — returns `()`.
///
/// `cx` is any actor context for sending the initial
/// `CaptureSnapshot` message to the spawned actor.
pub fn start_periodic_snapshots(
    cx: &impl hyperactor::context::Actor,
    table_store: TableStore,
    admin_ref: hyperactor::reference::ActorRef<MeshAdminAgent>,
    interval: Duration,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !interval.is_zero(),
        "periodic capture interval must be non-zero"
    );
    let proc = cx.instance().proc();
    let actor = SnapshotCaptureActor::new(table_store, admin_ref, interval);
    let handle = proc.spawn("snapshot_capture", actor)?;
    // PT-3: first capture fires at spawn time.
    handle.send(cx, CaptureSnapshot)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::push::SNAPSHOT_TABLE_NAMES;

    // SI-1: register_snapshot_schemas populates a TableStore with 9
    // table names matching SNAPSHOT_TABLE_NAMES.
    #[tokio::test]
    async fn test_register_snapshot_schemas() {
        let store = TableStore::new_empty();
        register_snapshot_schemas(&store).await.unwrap();

        let names = store.table_names().unwrap();
        assert_eq!(names.len(), 9);

        let expected: Vec<String> = SNAPSHOT_TABLE_NAMES.iter().map(|s| s.to_string()).collect();
        assert_eq!(names, expected);
    }

    // SI-1: each pre-registered table has zero rows but a valid
    // schema (non-zero columns).
    #[tokio::test]
    async fn test_register_snapshot_schemas_empty_but_valid() {
        let store = TableStore::new_empty();
        register_snapshot_schemas(&store).await.unwrap();

        for name in SNAPSHOT_TABLE_NAMES {
            let provider = store.table_provider(name).unwrap();
            assert!(
                provider.is_some(),
                "table '{}' should have a provider",
                name,
            );
        }
    }
}
