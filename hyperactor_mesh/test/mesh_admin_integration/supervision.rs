/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Sieve-topology visibility proof over a live sieve workload.
//!
//! In this integration environment, the stable admin contract is the
//! tree view: the local proc is present, the sieve actor is visible,
//! and the actor-mesh controller for that sieve workload is also
//! visible. Direct actor→actor child resolution has proven flaky under
//! this target even when the workload itself is healthy.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::harness;
use crate::harness::WorkloadFixture;

/// How long to wait for the admin URL sentinel after starting the
/// sieve binary.
const SIEVE_START_TIMEOUT: Duration = Duration::from_secs(60);

/// Settle time after admin-URL readiness for the sieve actor chain
/// to build (the sieve starts 5s after the sentinel, then needs
/// time to discover primes and spawn child actors).
const SIEVE_CHAIN_READY_SLEEP: Duration = Duration::from_secs(10);

/// Maximum number of discovery attempts before failing.
const DISCOVERY_ATTEMPTS: usize = 30;

/// Backoff between discovery attempts.
const DISCOVERY_BACKOFF: Duration = Duration::from_secs(2);

/// A live sieve workload fixture.
struct SieveScenario {
    fixture: WorkloadFixture,
}

impl SieveScenario {
    async fn start() -> Self {
        let bin = harness::sieve_rust_binary();
        // The sieve prints "Starts in 5 seconds." then spawns actors.
        // Give it enough time to find some primes and build the chain.
        // --num-primes 10: enough to build a chain of ~10 actors,
        // small enough to converge quickly.
        let fixture = harness::start_workload(&bin, &["--num-primes", "10"], SIEVE_START_TIMEOUT)
            .await
            .expect("failed to start sieve workload");
        // Wait for sieve actors to spawn.
        tokio::time::sleep(SIEVE_CHAIN_READY_SLEEP).await;
        SieveScenario { fixture }
    }

    /// Run a test closure with a fresh scenario.
    ///
    /// **Structural cleanup guarantee (MIT-2):** The fixture is shut
    /// down before this function returns, even if the closure panics.
    /// `WorkloadFixture::Drop` calls `start_kill()` on the panic path.
    async fn run<F>(f: F)
    where
        F: for<'a> FnOnce(&'a SieveScenario) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    {
        let scenario = Self::start().await;
        let guard = ShutdownGuard(&scenario.fixture);
        f(&scenario).await;
        guard.disarm();
        scenario.fixture.shutdown().await;
    }
}

/// Drop guard matching the `DiningScenario` cleanup pattern.
struct ShutdownGuard<'a>(#[allow(dead_code)] &'a WorkloadFixture);

impl ShutdownGuard<'_> {
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for ShutdownGuard<'_> {
    fn drop(&mut self) {
        // Panic path: WorkloadFixture::Drop handles synchronous kill.
    }
}

async fn tree_dump(fixture: &WorkloadFixture) -> String {
    match fixture.get("/v1/tree").await {
        Ok(resp) => resp
            .text()
            .await
            .unwrap_or_else(|e| format!("failed to read /v1/tree body: {e:#}")),
        Err(e) => format!("failed to fetch /v1/tree: {e:#}"),
    }
}

/// MIT-71, MIT-72: sieve topology visibility — Rust sieve binary.
pub async fn run_supervision_proof_rust() {
    SieveScenario::run(|s| {
        Box::pin(async move {
            check_supervision(&s.fixture).await;
        })
    })
    .await;
}

/// The admin tree exposes the local proc together with the sieve actor
/// and its mesh controller.
async fn check_supervision(fixture: &WorkloadFixture) {
    let mut last_tree = None;
    for _attempt in 1..=DISCOVERY_ATTEMPTS {
        let tree = tree_dump(fixture).await;
        let has_local_proc = tree.contains("\n└── _local")
            || tree.contains("\n├── _local")
            || tree.contains("\n_local  ->");
        let has_sieve_actor = tree.contains("sieve[");
        let has_sieve_controller = tree.contains("actor_mesh_controller_sieve-");
        if has_local_proc && has_sieve_actor && has_sieve_controller {
            return;
        }
        last_tree = Some(tree);
        tokio::time::sleep(DISCOVERY_BACKOFF).await;
    }

    let tree = last_tree.unwrap_or_else(|| "failed to capture /v1/tree".to_string());
    panic!("failed to observe sieve topology in /v1/tree\n{}", tree);
}
