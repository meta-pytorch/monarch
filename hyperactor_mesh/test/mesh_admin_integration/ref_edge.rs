/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Malformed/encoded reference edge case assertion helpers.
//!
//! Purely syntactic — no actor interaction. Tests the
//! `resolve_reference_bridge` handler's input validation.
//!
//! See MIT-25, MIT-26, MIT-27, MIT-29, MIT-30, MIT-31 in
//! `main` module doc.

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::mesh_admin::ApiErrorEnvelope;

use crate::dining::DiningScenario;

fn enc(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// MIT-25, MIT-26, MIT-27, MIT-29, MIT-30, MIT-31:
/// malformed/encoded reference edge cases.
pub(crate) async fn check(s: &DiningScenario) {
    // --- MIT-25: empty ref (trailing slash only) ---
    // Axum's catch-all `{*reference}` does not match when the wildcard
    // segment is empty, so `/v1/` hits the router's built-in 404 before
    // `resolve_reference_bridge` runs.  Assert non-success only.
    let resp = s.fixture.get("/v1/").await.unwrap();
    assert!(
        !resp.status().is_success(),
        "MIT-25: empty ref should not succeed (got {})",
        resp.status()
    );

    // --- MIT-26: garbage refs ---
    for garbage in ["xyzzy", "not-a-reference", "12345", "null", "true"] {
        let resp = s.fixture.get(&format!("/v1/{garbage}")).await.unwrap();
        assert!(
            !resp.status().is_success(),
            "MIT-26: {garbage} should not succeed"
        );
        let body = resp.text().await.unwrap();
        let envelope: ApiErrorEnvelope = serde_json::from_str(&body)
            .unwrap_or_else(|e| panic!("MIT-26: {garbage}: not an error envelope: {e}: {body}"));
        assert_eq!(
            envelope.error.code, "not_found",
            "MIT-26: {garbage}: expected not_found, got {}",
            envelope.error.code
        );
    }

    // --- MIT-27: truncated ref ---
    let truncated = &s.worker[..s.worker.len() / 2];
    let resp = s
        .fixture
        .get(&format!("/v1/{}", enc(truncated)))
        .await
        .unwrap();
    assert!(
        !resp.status().is_success(),
        "MIT-27: truncated ref should not succeed"
    );

    // --- MIT-29: extremely long ref ---
    let long_ref = "a".repeat(10_000);
    let resp = s.fixture.get(&format!("/v1/{long_ref}")).await.unwrap();
    assert!(
        !resp.status().is_success(),
        "MIT-29: 10KB ref should not succeed"
    );

    // --- MIT-30: unreachable socket ref ---
    let bogus_socket = "unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff";
    let resp = s
        .fixture
        .get(&format!("/v1/{}", enc(bogus_socket)))
        .await
        .unwrap();
    assert!(
        !resp.status().is_success(),
        "MIT-30: unreachable socket ref should not succeed"
    );

    // --- MIT-31: valid ref round-trips through URL encoding ---
    let encoded = urlencoding::encode(&s.worker);
    let node: NodePayload = s.fixture.get_json(&format!("/v1/{encoded}")).await.unwrap();
    assert_eq!(node.identity, s.worker, "MIT-31");
}
