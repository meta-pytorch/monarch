/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tree and root assertion helpers.
//!
//! These are assertion functions, not tests. Called from
//! `dining::check_dining_endpoints` as part of the combined
//! dining scenario.
//!
//! See MIT-9, MIT-13, MIT-14 in `main` module doc.

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;

use crate::dining::DiningScenario;

/// MIT-9, MIT-13, MIT-14: Tree and root assertions.
pub(crate) async fn check(s: &DiningScenario) {
    // --- MIT-13: /v1/root contract ---
    let root: NodePayload = s
        .fixture
        .get_node_payload("/v1/root")
        .await
        .unwrap_or_else(|e| panic!("MIT-13: /v1/root failed: {e:#}"));
    assert_eq!(
        root.identity,
        NodeRef::Root,
        "MIT-13: expected identity Root"
    );
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

    // --- MIT-14: /v1/tree availability ---
    //
    // /v1/tree is a human-facing rendering surface, not a machine
    // contract. We only smoke-test that it responds and returns
    // non-empty content. Content assertions belong in the typed
    // /v1/{ref} traversal tests, not here.
    let resp = s
        .fixture
        .get("/v1/tree")
        .await
        .unwrap_or_else(|e| panic!("MIT-14: /v1/tree failed: {e:#}"));
    let tree = resp.text().await.unwrap();
    assert!(!tree.is_empty(), "MIT-14: /v1/tree returned empty body");
}
