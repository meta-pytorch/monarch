/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! pyo3 wrappers for snapshot telemetry integration.
//!
//! Thin wrappers around the pure Rust helpers in
//! `monarch_introspection_snapshot::integration`. Called from Python
//! during telemetry/admin startup.

use std::time::Duration;

use monarch_distributed_telemetry::database_scanner::DatabaseScanner;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::host_mesh::PyMeshAdminRef;
use monarch_introspection_snapshot::integration::register_snapshot_schemas;
use monarch_introspection_snapshot::integration::start_periodic_snapshots;
use pyo3::prelude::*;

/// Pre-register the 9 snapshot table schemas in a `DatabaseScanner`.
///
/// Must be called after `DatabaseScanner` creation and before
/// `QueryEngine` construction, because table discovery is static.
/// Called unconditionally whenever telemetry starts (SI-6).
#[pyfunction]
#[pyo3(name = "_pre_register_snapshot_schemas")]
fn pre_register_snapshot_schemas_py(py: Python<'_>, scanner: &DatabaseScanner) -> PyResult<()> {
    let table_store = scanner.table_store();
    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { register_snapshot_schemas(&table_store).await })
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{:#}", e)))
    })
}

/// Spawn periodic snapshot capture as a `SnapshotCaptureActor`.
///
/// Fire-and-forget: the actor is spawned on the same proc as the
/// mesh admin. Framework lifecycle (proc teardown) stops it.
/// Returns nothing (SI-5).
#[pyfunction]
#[pyo3(name = "_start_periodic_snapshots")]
fn start_periodic_snapshots_py(
    scanner: &DatabaseScanner,
    admin_ref: &PyMeshAdminRef,
    instance: &PyInstance,
    interval_secs: f64,
) -> PyResult<()> {
    let table_store = scanner.table_store();
    let admin_ref = admin_ref.actor_ref();

    if interval_secs <= 0.0 || !interval_secs.is_finite() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "interval_secs must be a positive finite number, got {}",
            interval_secs,
        )));
    }
    let interval = Duration::from_secs_f64(interval_secs);

    let _guard = pyo3_async_runtimes::tokio::get_runtime().enter();
    start_periodic_snapshots(&**instance, table_store, admin_ref, interval)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{:#}", e)))
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(pre_register_snapshot_schemas_py, module)?)?;
    module.add_function(wrap_pyfunction!(start_periodic_snapshots_py, module)?)?;
    Ok(())
}
