/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `/v1/admin` and `/v1/schema/admin` route assertions.
//!
//! See AI-1..AI-3 in the `mesh_admin` module-level invariant docs.
//! AI-4 (`host` derives from `url`) is a constructor guarantee of
//! `AdminInfo::new()` — not a live invariant tested here.

use hyperactor_mesh::mesh_admin::AdminInfo;

use crate::dining::DiningScenario;

/// AI-1 through AI-3: `/v1/admin` returns correct `AdminInfo`.
/// AI-4 holds by construction via `AdminInfo::new()`.
pub(crate) async fn assert_admin_info(s: &DiningScenario) {
    let info: AdminInfo = s
        .fixture
        .get_json("/v1/admin")
        .await
        .expect("GET /v1/admin must succeed");

    // AI-1: live identity fields are populated.
    assert!(
        !info.actor_id.is_empty(),
        "AI-1: actor_id must be non-empty"
    );

    // AI-2: proc_id is populated and well-formed. Placement equality
    // is proved by the SA-5 unit test; here we validate the HTTP layer.
    assert!(!info.proc_id.is_empty(), "AI-2: proc_id must be non-empty");
    // The proc_id should look like a hyperactor ProcId — at minimum
    // it contains a transport address component.
    assert!(
        info.proc_id.contains("unix:") || info.proc_id.contains("tcp:"),
        "AI-2: proc_id '{}' does not look like a valid ProcId",
        info.proc_id
    );

    // AI-3: url matches the admin URL the fixture discovered at startup.
    assert_eq!(
        info.url, s.fixture.admin_url,
        "AI-3: url must match the fixture's admin URL"
    );

    // AI-4 (constructor guarantee): host derives from url via
    // AdminInfo::new() strict URL parsing — no live assertion needed.
    // We still verify host is non-empty as a sanity check.
    assert!(
        !info.host.is_empty(),
        "AI-4 (sanity): host must be non-empty"
    );
}

/// `/v1/schema/admin` returns a valid JSON Schema document.
pub(crate) async fn assert_admin_schema(s: &DiningScenario) {
    let schema: serde_json::Value = s
        .fixture
        .get_json("/v1/schema/admin")
        .await
        .expect("GET /v1/schema/admin must succeed");

    // Must be a JSON object with a "type" or "$id" field (basic
    // schema structure check).
    assert!(
        schema.is_object(),
        "/v1/schema/admin must return a JSON object"
    );
    assert!(
        schema.get("$id").is_some() || schema.get("type").is_some(),
        "/v1/schema/admin must contain $id or type"
    );
}
