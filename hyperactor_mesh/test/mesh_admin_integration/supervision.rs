/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! NI-2/NI-3 integration proof over a live sieve topology.
//!
//! The sieve example creates a chain of actors where each actor
//! spawns the next as a supervision child:
//!
//!   proc → sieve[0] → sieve[1] → sieve[2] → …
//!
//! All sieve actors also appear as direct children of the proc
//! (via `all_instance_keys()`). This test proves that:
//!
//! - NI-2: an actor's `parent` is its containment proc, not
//!   the supervising actor that spawned it.
//! - NI-3: actor→actor edges in `children` coexist with
//!   proc→actor membership edges.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;

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

fn enc(r: &NodeRef) -> String {
    urlencoding::encode(&r.to_string()).into_owned()
}

/// Find the first actor under a proc that itself has actor children
/// (i.e., the first sieve actor in the chain that has spawned a
/// successor).
async fn find_actor_with_child(
    fixture: &WorkloadFixture,
) -> Option<(NodeRef, NodePayload, NodePayload)> {
    // Walk: root → host → proc → actor. Transient fetch failures
    // skip the node and continue rather than aborting the search.
    let root = match fixture.get_node_payload("/v1/root").await {
        Ok(r) => r,
        Err(_) => return None,
    };

    for host_ref in &root.children {
        let host = match fixture
            .get_node_payload(&format!("/v1/{}", enc(host_ref)))
            .await
        {
            Ok(h) => h,
            Err(_) => continue,
        };

        for proc_ref in &host.children {
            let proc_node = match fixture
                .get_node_payload(&format!("/v1/{}", enc(proc_ref)))
                .await
            {
                Ok(p) => p,
                Err(_) => continue,
            };

            if !matches!(proc_node.properties, NodeProperties::Proc { .. }) {
                continue;
            }

            for actor_ref in &proc_node.children {
                let actor = match fixture
                    .get_node_payload(&format!("/v1/{}", enc(actor_ref)))
                    .await
                {
                    Ok(a) => a,
                    Err(_) => continue,
                };

                // Must be a sieve actor (not an infrastructure actor
                // that happens to have children) with at least one
                // supervision child.
                let is_sieve = matches!(
                    &actor.properties,
                    NodeProperties::Actor { actor_type, .. }
                        if actor_type.contains("SieveActor")
                );
                if is_sieve && !actor.children.is_empty() {
                    return Some((proc_ref.clone(), proc_node, actor));
                }
            }
        }
    }
    None
}

/// MIT-71, MIT-72: NI-2/NI-3 supervision proof — Rust sieve binary.
pub async fn run_supervision_proof_rust() {
    SieveScenario::run(|s| {
        Box::pin(async move {
            check_supervision(&s.fixture).await;
        })
    })
    .await;
}

/// NI-2, NI-3: actor supervision children coexist with proc
/// membership, and parent always points to the proc.
async fn check_supervision(fixture: &WorkloadFixture) {
    // Retry discovery — actors may still be spawning.
    let (proc_ref, proc_node, actor_a) = {
        let mut result = None;
        for _attempt in 1..=DISCOVERY_ATTEMPTS {
            if let Some(found) = find_actor_with_child(fixture).await {
                result = Some(found);
                break;
            }
            tokio::time::sleep(DISCOVERY_BACKOFF).await;
        }
        result.expect("failed to find an actor with supervision children in the sieve topology")
    };

    // actor_a is the supervisor (e.g. sieve[0]).
    // Select the first Actor child explicitly — not just children[0].
    let actor_b_ref = actor_a
        .children
        .iter()
        .find(|r| matches!(r, NodeRef::Actor(_)))
        .expect("NI-3: supervising actor A must have at least one Actor child");

    // (2) NI-3: A.children contains B — actor→actor navigation edge.
    assert!(
        actor_a.children.contains(actor_b_ref),
        "NI-3: supervising actor A must list child actor B in children"
    );

    // (3) NI-3: the containing proc's children also contains B —
    // proc→actor membership edge coexists with actor→actor edge.
    assert!(
        proc_node.children.contains(actor_b_ref),
        "NI-3: proc must also list actor B in children; \
         proc children: {:?}, looking for: {:?}",
        proc_node.children,
        actor_b_ref
    );

    // (4) NI-2: resolve B and assert parent = proc, not actor A.
    let actor_b = fixture
        .get_node_payload(&format!("/v1/{}", enc(actor_b_ref)))
        .await
        .expect("failed to resolve actor B");

    assert_eq!(
        actor_b.parent,
        Some(proc_ref),
        "NI-2: actor B's parent must be the containing proc, not the supervising actor; \
         got: {:?}",
        actor_b.parent
    );
}
