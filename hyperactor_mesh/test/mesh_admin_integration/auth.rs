/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Auth failure assertion helpers.
//!
//! TLS-level assertions executed against a real dining fixture for
//! endpoint parity and harness reuse.
//!
//! See MIT-32, MIT-33, MIT-34, MIT-35, MIT-36 in `main` module doc.

use crate::dining::DiningScenario;
use crate::harness;

/// MIT-32, MIT-33, MIT-34, MIT-35, MIT-36: auth failure edge cases.
pub(crate) async fn check(s: &DiningScenario) {
    // --- MIT-32: no client cert — parity across all 4 endpoints ---
    let no_cert = s.fixture.build_unauthenticated_client().unwrap();
    let worker_encoded = urlencoding::encode(&s.worker);
    let endpoints = [
        "/v1/root".to_string(),
        "/v1/tree".to_string(),
        format!("/v1/config/{worker_encoded}"),
        format!("/v1/pyspy/{worker_encoded}"),
    ];
    for ep in &endpoints {
        let result = no_cert
            .get(format!("{}{ep}", s.fixture.admin_url))
            .send()
            .await;
        assert!(
            result.is_err(),
            "MIT-32: {ep} should reject unauthenticated client"
        );
    }

    // --- MIT-33: wrong CA cert — client presents cert signed by foreign CA ---
    let foreign_dir = tempfile::tempdir().unwrap();
    let foreign_pki = harness::generate_pki(foreign_dir.path()).unwrap();
    let wrong_ca = harness::build_tls_client(
        s.fixture.ca_pem(),
        Some(&foreign_pki.cert_pem),
        Some(&foreign_pki.key_pem),
    )
    .unwrap();
    let result = wrong_ca
        .get(format!("{}/v1/root", s.fixture.admin_url))
        .send()
        .await;
    assert!(
        result.is_err(),
        "MIT-33: foreign-CA cert should be rejected"
    );

    // --- MIT-34: client trusts wrong CA — TLS handshake fails client-side ---
    let wrong_trust = harness::build_tls_client(
        &foreign_pki.ca_pem,
        Some(&foreign_pki.cert_pem),
        Some(&foreign_pki.key_pem),
    )
    .unwrap();
    let result = wrong_trust
        .get(format!("{}/v1/root", s.fixture.admin_url))
        .send()
        .await;
    assert!(
        result.is_err(),
        "MIT-34: wrong-CA trust should fail handshake"
    );

    // --- MIT-35: corrupt PEM — TLS setup rejects invalid CA PEM ---
    let (_, ok) = hyperactor_mesh::mesh_admin_client::add_tls(
        reqwest::Client::builder(),
        b"not-valid-pem",
        None,
        None,
    );
    assert!(!ok, "MIT-35: add_tls must reject corrupt CA PEM");

    // --- MIT-36: auth failure is TLS-level, not HTTP-level ---
    // Demonstrated by MIT-32: result.is_err() means the request never
    // reached the server. If it were HTTP 401/403, result would be Ok.
}
