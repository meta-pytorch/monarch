/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! /v1/{ref} topology traversal assertion helpers.
//!
//! Single-shot assertions, no retry loops. All refs discovered from
//! the tree, never constructed.
//!
//! See MIT-20, MIT-21, MIT-22, MIT-23, MIT-24 in `main` module doc.

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;

use crate::dining::DiningScenario;
use crate::harness::WorkloadFixture;

/// Extract the base actor name from a full actor reference string.
/// Matches the harness's `classify_procs` parsing: take the last
/// comma-separated segment, strip the `[index]` suffix.
fn actor_base_name(actor_ref: &str) -> &str {
    let name = actor_ref.rsplit(',').next().unwrap_or(actor_ref);
    name.split('[').next().unwrap_or(name)
}

fn enc(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// MIT-20, MIT-21, MIT-22, MIT-23, MIT-24: /v1/{ref} topology
/// traversal (V1 scope).
pub(crate) async fn check(s: &DiningScenario) {
    // MIT-20, MIT-21: root fetchable, typed correctly.
    let root: NodePayload = s.fixture.get_json("/v1/root").await.unwrap();
    assert!(
        matches!(root.properties, NodeProperties::Root { .. }),
        "MIT-21: expected Root variant"
    );
    assert_eq!(root.identity, "root", "MIT-22");

    // MIT-20, MIT-21: first host fetchable.
    let host_ref = root
        .children
        .first()
        .expect("root should have at least one host child");
    let host: NodePayload = s
        .fixture
        .get_json(&format!("/v1/{}", enc(host_ref)))
        .await
        .unwrap();
    assert!(
        matches!(host.properties, NodeProperties::Host { .. }),
        "MIT-21"
    );
    assert_eq!(host.identity, *host_ref, "MIT-22");

    // MIT-23 V1: classified service and worker procs fetchable.
    let service: NodePayload = s
        .fixture
        .get_json(&format!("/v1/{}", enc(&s.service)))
        .await
        .unwrap();
    assert!(
        matches!(service.properties, NodeProperties::Proc { .. }),
        "MIT-21"
    );
    assert_eq!(service.identity, s.service, "MIT-22");

    let worker: NodePayload = s
        .fixture
        .get_json(&format!("/v1/{}", enc(&s.worker)))
        .await
        .unwrap();
    assert!(
        matches!(worker.properties, NodeProperties::Proc { .. }),
        "MIT-21"
    );
    assert_eq!(worker.identity, s.worker, "MIT-22");

    // MIT-24: service proc must expose host_agent, worker proc must
    // expose proc_agent.
    check_known_actors(&s.fixture, &service, "service", "host_agent").await;
    check_known_actors(&s.fixture, &worker, "worker", "proc_agent").await;
}

async fn check_known_actors(
    fixture: &WorkloadFixture,
    proc_node: &NodePayload,
    label: &str,
    expected_agent: &str,
) {
    let mut found = false;
    for actor_ref in &proc_node.children {
        let actor: NodePayload = fixture
            .get_json(&format!("/v1/{}", enc(actor_ref)))
            .await
            .unwrap();
        assert!(
            matches!(actor.properties, NodeProperties::Actor { .. }),
            "MIT-21"
        );
        assert_eq!(actor.identity, *actor_ref, "MIT-22");

        if actor_base_name(actor_ref) == expected_agent {
            found = true;
        }
    }
    assert!(
        found,
        "MIT-24: {label} proc should contain {expected_agent}",
    );
}
