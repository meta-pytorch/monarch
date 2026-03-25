/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tree and root assertion helpers.
//!
//! These are assertion functions, not tests. They are called from
//! `dining::test_dining_endpoints` so that all dining-based assertions
//! share one scenario.
//!
//! See MIT-9, MIT-13, MIT-14 in `main` module doc.

use std::time::Duration;

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;

use crate::dining::DiningScenario;

const TREE_READY_ATTEMPTS: usize = 30;
const TREE_READY_SLEEP: Duration = Duration::from_secs(2);
const TOPOLOGY_READY_ATTEMPTS: usize = 45;
const TOPOLOGY_READY_SLEEP: Duration = Duration::from_secs(2);

fn actor_name(reference: &str) -> &str {
    reference.rsplit(',').next().unwrap_or(reference)
}

async fn topology_has_dining_actors(s: &DiningScenario) -> bool {
    let root: NodePayload = match s.fixture.get_json("/v1/root").await {
        Ok(root) => root,
        Err(_) => return false,
    };

    for host_ref in &root.children {
        let encoded = urlencoding::encode(host_ref);
        let host: NodePayload = match s.fixture.get_json(&format!("/v1/{encoded}")).await {
            Ok(host) => host,
            Err(_) => continue,
        };

        for proc_ref in &host.children {
            let encoded = urlencoding::encode(proc_ref);
            let proc_node: NodePayload = match s.fixture.get_json(&format!("/v1/{encoded}")).await {
                Ok(proc) => proc,
                Err(_) => continue,
            };

            if proc_node.children.iter().any(|actor_ref| {
                let name = actor_name(actor_ref);
                name.starts_with("philosopher") || name.starts_with("waiter")
            }) {
                return true;
            }
        }
    }

    false
}

/// MIT-9, MIT-13, MIT-14: Tree and root assertions.
pub(crate) async fn check(s: &DiningScenario) {
    // --- MIT-13: /v1/root contract ---
    let root: NodePayload = s
        .fixture
        .get_json("/v1/root")
        .await
        .unwrap_or_else(|e| panic!("MIT-13: /v1/root failed: {e:#}"));
    assert_eq!(root.identity, "root", "MIT-13: expected identity 'root'");
    match &root.properties {
        NodeProperties::Root { num_hosts, .. } => {
            assert!(
                *num_hosts >= 1,
                "MIT-13: expected at least 1 host, got {num_hosts}"
            );
        }
        other => panic!("MIT-13: expected Root variant, got {other:?}"),
    }
    assert!(
        !root.children.is_empty(),
        "MIT-13: expected at least 1 child"
    );

    // Wait for the underlying topology to expose dining actors before
    // asserting on the rendered tree output. `/v1/tree` resolves each proc
    // subtree opportunistically and can transiently omit actor children under
    // heavy load even when the proc itself is already visible.
    let mut topology_ready = false;
    for _attempt in 1..=TOPOLOGY_READY_ATTEMPTS {
        if topology_has_dining_actors(s).await {
            topology_ready = true;
            break;
        }
        tokio::time::sleep(TOPOLOGY_READY_SLEEP).await;
    }
    assert!(
        topology_ready,
        "MIT-14: dining actor topology did not converge before tree check"
    );

    // --- MIT-14: /v1/tree format ---
    //
    // Poll until philosopher actors appear in the rendered tree after the
    // underlying topology has converged.
    let mut tree = String::new();
    for _attempt in 1..=TREE_READY_ATTEMPTS {
        let resp = match s.fixture.get("/v1/tree").await {
            Ok(r) => r,
            Err(_) => {
                tokio::time::sleep(TREE_READY_SLEEP).await;
                continue;
            }
        };
        tree = resp.text().await.unwrap();
        if tree.contains("philosopher") {
            break;
        }
        tokio::time::sleep(TREE_READY_SLEEP).await;
    }
    assert!(
        tree.contains("\u{251c}\u{2500}\u{2500} ") || tree.contains("\u{2514}\u{2500}\u{2500} "),
        "MIT-14: /v1/tree missing box-drawing connectors: {tree}"
    );
    assert!(
        tree.contains("http://") || tree.contains("https://"),
        "MIT-14: /v1/tree missing clickable URLs: {tree}"
    );
    assert!(
        tree.contains("philosopher"),
        "MIT-14: /v1/tree missing philosopher procs: {tree}",
    );
}
