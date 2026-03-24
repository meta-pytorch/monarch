/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Config endpoint assertion helpers.
//!
//! These are assertion functions, not tests. They are called from
//! `dining::test_dining_endpoints` so that all dining-based assertions
//! share one scenario.
//!
//! See MIT-7, MIT-9, MIT-10, MIT-11, MIT-12, MIT-15 in `main` module doc.

use hyperactor_mesh::config_dump::ConfigDumpResult;
use hyperactor_mesh::mesh_admin::ApiErrorEnvelope;

use crate::dining::DiningScenario;

/// MIT-7, MIT-9, MIT-10, MIT-11, MIT-12, MIT-15: All config assertions.
///
/// Order: worker → service → bogus.
/// The bogus case eats the full bridge timeout (5s) since probe_actor
/// was removed, so we run the live-proc assertions first.
pub(crate) async fn check(s: &DiningScenario) {
    // --- MIT-12, MIT-15: worker then service (table-driven) ---
    for (label, proc_ref) in [
        ("worker", s.worker.as_str()),
        ("service", s.service.as_str()),
    ] {
        let encoded = urlencoding::encode(proc_ref);
        let result: ConfigDumpResult = s
            .fixture
            .get_json(&format!("/v1/config/{encoded}"))
            .await
            .unwrap_or_else(|e| panic!("MIT-15: {label}: {e}"));

        // MIT-15: entries non-empty.
        assert!(
            !result.entries.is_empty(),
            "MIT-15: {label}: expected at least one config entry"
        );

        // MIT-15: sorted by name.
        let names: Vec<&str> = result.entries.iter().map(|e| e.name.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(
            names, sorted,
            "MIT-15: {label}: config entries not sorted by name"
        );
    }

    // --- MIT-10, MIT-11: bogus proc error envelope ---
    //
    // With probe_actor removed from config_bridge, a bogus proc reference
    // goes straight to ConfigDump send + bridge timeout. PortRef::send()
    // serializes and posts without validating the destination, so the send
    // succeeds and the reply never arrives — gateway_timeout after the
    // bridge timeout (MESH_ADMIN_CONFIG_DUMP_BRIDGE_TIMEOUT, 5s).
    let bogus = "unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff";
    let encoded = urlencoding::encode(bogus);
    let resp = s
        .fixture
        .get(&format!("/v1/config/{encoded}"))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "MIT-10/MIT-11: config bogus-ref transport failure (expected HTTP error envelope, got: {e:#})"
            )
        });
    let status = resp.status();
    assert!(
        !status.is_success(),
        "MIT-11: expected non-200 for bogus proc, got {status}"
    );
    let body = resp.text().await.unwrap();
    let envelope: ApiErrorEnvelope =
        serde_json::from_str(&body).expect("MIT-10: response should be ApiErrorEnvelope");
    assert_eq!(
        envelope.error.code, "gateway_timeout",
        "MIT-10: expected gateway_timeout for bogus proc, got: {}",
        envelope.error.code
    );
}
