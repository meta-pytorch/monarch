/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `DiningScenario` — eager, total construction over a
//! `dining_philosophers` workload.
//!
//! See MIT-2 (scoped-cleanup), MIT-3 (no-shared-fixtures), MIT-7
//! (proc-classification).

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use hyperactor_mesh::config_dump::ConfigDumpResult;

use crate::harness;
use crate::harness::WorkloadFixture;

const SERVICE_CONFIG_READY_ATTEMPTS: usize = 30;
const SERVICE_CONFIG_READY_SLEEP: Duration = Duration::from_secs(4);

/// A fully-initialized dining_philosophers scenario.
///
/// Construction is eager and total: `start()` returns only when the
/// fixture is live and both proc types have been classified. You
/// cannot hold a `DiningScenario` without valid proc refs.
pub(crate) struct DiningScenario {
    pub(crate) fixture: WorkloadFixture,
    pub(crate) service: String,
    pub(crate) worker: String,
}

impl DiningScenario {
    /// Start a fresh dining_philosophers workload and classify its
    /// procs.
    pub(crate) async fn start(bin: &Path) -> Self {
        let fixture = harness::start_workload(bin, &[], Duration::from_secs(60))
            .await
            .expect("failed to start dining_philosophers");
        let procs = fixture
            .classify_procs()
            .await
            .expect("failed to classify dining procs");

        // Startup convergence: wait for the service config path to be
        // usable.
        //
        // Product finding: hyperactor_mesh advertises the admin URL
        // when MeshAdminAgent::init binds the TCP listener
        // (spawn_admin / GetAdminAddr). That precedes the service
        // HostAgent being able to serve /v1/config/{service} — the
        // HostAgent is still processing startup messages
        // (CreateOrUpdate, ProcStatusChanged for each child proc),
        // and ConfigDump queues behind them. Under stress-runs 5 this
        // unreadiness window exceeds 30s.
        //
        // This loop is NOT the fix for that product readiness gap. It
        // is a startup convergence budget so the test waits long
        // enough for the existing product behavior to settle.
        // Endpoint assertions (config.rs, tree.rs) remain single-shot
        // and honest.
        //
        // Test readiness preconditions:
        //   1. Admin URL available          (sentinel in start_workload)
        //   2. Topology visible             (classify_procs above)
        //   3. Service config responsive    (poll below)
        let encoded = urlencoding::encode(&procs.service);
        let config_path = format!("/v1/config/{encoded}");
        // Budget: 30 attempts × (up to 5s bridge timeout + 4s sleep)
        // ≈ 270s.
        for attempt in 1..=SERVICE_CONFIG_READY_ATTEMPTS {
            match fixture.get_json::<ConfigDumpResult>(&config_path).await {
                Ok(_) => break,
                Err(e) if attempt == SERVICE_CONFIG_READY_ATTEMPTS => {
                    panic!(
                        "service config not ready after {attempt} attempts: {e}\n\
                         path: {config_path}",
                    );
                }
                Err(_) => {
                    tokio::time::sleep(SERVICE_CONFIG_READY_SLEEP).await;
                }
            }
        }

        DiningScenario {
            fixture,
            service: procs.service,
            worker: procs.worker,
        }
    }

    /// Run a test closure with a fresh scenario.
    ///
    /// **Structural cleanup guarantee (MIT-2):** The fixture is shut
    /// down before this function returns, even if the closure panics.
    /// This is implemented via a Drop guard — `WorkloadFixture::Drop`
    /// calls `start_kill()` synchronously on the panic path.
    ///
    /// Call sites use `|s| Box::pin(async move { ... })` to satisfy
    /// the lifetime bound — the boxed future borrows from the
    /// scenario reference.
    async fn run<F>(bin: &Path, f: F)
    where
        F: for<'a> FnOnce(&'a DiningScenario) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    {
        let scenario = Self::start(bin).await;
        let guard = ShutdownGuard(&scenario.fixture);
        f(&scenario).await;
        // Normal path: explicit async shutdown before guard drops.
        guard.disarm();
        scenario.fixture.shutdown().await;
    }
}

/// Drop guard that ensures the child process is killed even on panic.
///
/// On the normal path, `disarm()` is called before drop, and the
/// caller does an explicit async shutdown. On the panic path, `Drop`
/// logs a message and lets `WorkloadFixture::Drop` handle the
/// synchronous kill.
struct ShutdownGuard<'a>(#[allow(dead_code)] &'a WorkloadFixture);

impl ShutdownGuard<'_> {
    /// Disarm the guard (normal exit path). The caller will do async
    /// shutdown.
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for ShutdownGuard<'_> {
    fn drop(&mut self) {
        // Panic path: WorkloadFixture::Drop will call start_kill()
        // synchronously.
    }
}

/// All dining-based endpoint assertions against a single binary.
async fn check_dining_endpoints(bin: &Path) {
    DiningScenario::run(bin, |s| {
        Box::pin(async move {
            crate::admin::assert_admin_info(s).await;
            crate::admin::assert_admin_schema(s).await;
            crate::config::check(s).await;
            crate::tree::check(s).await;
        })
    })
    .await;
}

/// MIT-9, MIT-10, MIT-11, MIT-12, MIT-13, MIT-14, MIT-15:
/// dining-based endpoint assertions — Rust binary.
pub async fn run_dining_endpoints_rust() {
    let bin = harness::dining_philosophers_rust_binary();
    check_dining_endpoints(&bin).await;
}

/// MIT-9, MIT-10, MIT-11, MIT-12, MIT-13, MIT-14, MIT-15:
/// dining-based endpoint assertions — Python binary.
pub async fn run_dining_endpoints_python() {
    let bin = harness::dining_philosophers_python_binary();
    check_dining_endpoints(&bin).await;
}

// --- traversal family ---

/// MIT-20, MIT-21, MIT-22, MIT-23, MIT-24: /v1/{ref} topology
/// traversal — Rust binary.
pub async fn run_ref_traversal_rust() {
    let bin = harness::dining_philosophers_rust_binary();
    DiningScenario::run(&bin, |s| {
        Box::pin(async move {
            crate::ref_check::check(s).await;
        })
    })
    .await;
}

/// MIT-20, MIT-21, MIT-22, MIT-23, MIT-24: /v1/{ref} topology
/// traversal — Python binary.
pub async fn run_ref_traversal_python() {
    let bin = harness::dining_philosophers_python_binary();
    DiningScenario::run(&bin, |s| {
        Box::pin(async move {
            crate::ref_check::check(s).await;
        })
    })
    .await;
}

// --- malformed-ref family ---

/// MIT-25, MIT-26, MIT-27, MIT-28, MIT-29, MIT-30, MIT-31:
/// malformed/encoded reference edge cases — Rust binary.
pub async fn run_ref_edge_cases_rust() {
    let bin = harness::dining_philosophers_rust_binary();
    DiningScenario::run(&bin, |s| {
        Box::pin(async move {
            crate::ref_edge::check(s).await;
        })
    })
    .await;
}

/// MIT-25, MIT-26, MIT-27, MIT-28, MIT-29, MIT-30, MIT-31:
/// malformed/encoded reference edge cases — Python binary.
pub async fn run_ref_edge_cases_python() {
    let bin = harness::dining_philosophers_python_binary();
    DiningScenario::run(&bin, |s| {
        Box::pin(async move {
            crate::ref_edge::check(s).await;
        })
    })
    .await;
}

// --- openapi conformance family ---

/// MIT-37 through MIT-52: OpenAPI conformance — Rust binary.
pub async fn run_openapi_conformance_rust() {
    let bin = harness::dining_philosophers_rust_binary();
    DiningScenario::run(&bin, |s| {
        Box::pin(async move {
            crate::openapi::check(s).await;
        })
    })
    .await;
}

// --- auth family ---

/// MIT-32, MIT-33, MIT-34, MIT-35, MIT-36: auth failure coverage —
/// Rust binary.
pub async fn run_auth_failures_rust() {
    let bin = harness::dining_philosophers_rust_binary();
    DiningScenario::run(&bin, |s| {
        Box::pin(async move {
            crate::auth::check(s).await;
        })
    })
    .await;
}
