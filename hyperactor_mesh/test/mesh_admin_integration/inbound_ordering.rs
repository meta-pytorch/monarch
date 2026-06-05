/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Exercises MIT-77 (inbound-ordering API exposure) by walking the
//! dining workload topology to a known live actor and asserting the
//! presentation invariants IO-4, IO-5, IO-6, IO-7 over `/v1/{actor}`.
//! See `test/mesh_admin_integration/main.rs` for MIT-77 and
//! `src/introspect.rs` for IO-4..IO-7.
//!
//! Scope (per the Diff 5 plan): HTTP transport + DTO schema only.
//! Deterministic stalled-session evidence (manufactured gaps) belongs
//! with the later workload/verifier diff where the workload binary can
//! own the debug-tap call site.

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;

use crate::dining::DiningScenario;

fn enc(r: &NodeRef) -> String {
    urlencoding::encode(&r.to_string()).into_owned()
}

fn enc_str(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// MIT-77 (inbound-ordering API exposure):
/// /v1/{actor} returns instance_id, queue_depth, inbound_ordering for a
/// live worker actor, and the InboundOrdering presentation contract
/// (IO-4, IO-5, IO-6, IO-7) holds.
pub(crate) async fn check(s: &DiningScenario) {
    // Walk: root -> first host -> worker proc -> first running actor.
    let worker: NodePayload = s
        .fixture
        .get_node_payload(&format!("/v1/{}", enc_str(&s.worker)))
        .await
        .unwrap_or_else(|e| panic!("MIT-77: GET /v1/{} failed: {e:#}", s.worker));
    let worker_children = match &worker.properties {
        NodeProperties::Proc { .. } => &worker.children,
        other => panic!("MIT-77: expected worker to be Proc variant, got {other:?}"),
    };

    // Pick the first non-terminal actor. ActorStatus::Display strings
    // for live actors include "idle", "initializing", "created",
    // "processing for Xms", etc.; we exclude only "stopped" / "failed"
    // (terminal) and "unknown" (transport error).
    let is_terminal_status =
        |s: &str| s.starts_with("stopped") || s.starts_with("failed") || s.starts_with("unknown");

    let mut chosen: Option<(NodeRef, NodePayload)> = None;
    let mut observed_statuses: Vec<String> = Vec::new();
    for actor_ref in worker_children {
        let actor: NodePayload = s
            .fixture
            .get_node_payload(&format!("/v1/{}", enc(actor_ref)))
            .await
            .unwrap_or_else(|e| panic!("MIT-77: GET /v1/{actor_ref} failed: {e:#}"));
        if let NodeProperties::Actor { actor_status, .. } = &actor.properties {
            observed_statuses.push(actor_status.clone());
            if !is_terminal_status(actor_status) {
                chosen = Some((actor_ref.clone(), actor));
                break;
            }
        }
    }
    let (actor_ref, actor) = chosen.unwrap_or_else(|| {
        panic!(
            "MIT-77: dining worker proc must expose at least one live (non-terminal) actor; \
             observed statuses: {observed_statuses:?}",
        )
    });

    let NodeProperties::Actor {
        instance_id,
        queue_depth,
        inbound_ordering,
        ..
    } = actor.properties
    else {
        panic!("MIT-77: expected Actor variant for {actor_ref}");
    };

    // Three new fields are present and decode cleanly.
    assert!(
        !instance_id.is_empty(),
        "MIT-77: instance_id must be non-empty for live actor {actor_ref}",
    );
    // Structural UUID check without pulling uuid as a dep: 36 chars,
    // hyphens at the canonical positions. Format-only sanity; the DTO
    // round-trip tests verify exact parse/format fidelity.
    assert_eq!(
        instance_id.len(),
        36,
        "MIT-77: instance_id must be 36 chars (UUID canonical); got {instance_id:?}",
    );
    for (i, c) in instance_id.chars().enumerate() {
        let expect_hyphen = matches!(i, 8 | 13 | 18 | 23);
        if expect_hyphen {
            assert_eq!(c, '-', "MIT-77: instance_id char {i} must be '-'");
        } else {
            assert!(
                c.is_ascii_hexdigit(),
                "MIT-77: instance_id char {i} must be a hex digit; got {c:?}",
            );
        }
    }
    let _: u64 = queue_depth; // exists; no arithmetic check (IO-3).

    // IO-7: live actor built through `Instance::new` MUST expose Some(...).
    // None here would silently mask a regression in the publish path
    // (parent diff's `attrs.set(INBOUND_ORDERING, ...)` or this diff's
    // IntoNodeProperties / DTO threading); missing Option<T> JSON fields
    // deserialize as None, so equality on `is_some()` is the only honest
    // assertion.
    let io = inbound_ordering.expect(
        "MIT-77 / IO-7: inbound_ordering must be Some for a live worker actor; \
         None here indicates a regression in the publish path",
    );

    // IO-4: snapshot_complete is derived from skipped_session_count.
    assert_eq!(
        io.snapshot_complete,
        io.skipped_session_count == 0,
        "MIT-77 / IO-4: snapshot_complete must equal (skipped_session_count == 0); \
         got snapshot_complete={}, skipped_session_count={}",
        io.snapshot_complete,
        io.skipped_session_count,
    );

    // IO-5: known_session_count totals returned + skipped (equality, not >=).
    assert_eq!(
        io.known_session_count,
        io.sessions.len() + io.skipped_session_count,
        "MIT-77 / IO-5: known_session_count must equal sessions.len() + skipped_session_count; \
         got known={}, sessions.len()={}, skipped={}",
        io.known_session_count,
        io.sessions.len(),
        io.skipped_session_count,
    );

    // IO-6: returned_* rollups are computed over RETURNED sessions only.
    assert_eq!(
        io.returned_buffered_session_count,
        io.sessions.iter().filter(|s| s.buffered_count > 0).count(),
        "MIT-77 / IO-6: returned_buffered_session_count must match the count of \
         returned sessions with buffered_count > 0",
    );
    assert_eq!(
        io.returned_buffered_message_count,
        io.sessions.iter().map(|s| s.buffered_count).sum::<usize>(),
        "MIT-77 / IO-6: returned_buffered_message_count must equal sum of \
         buffered_count over returned sessions",
    );
    assert_eq!(
        io.returned_max_buffered_count,
        io.sessions
            .iter()
            .map(|s| s.buffered_count)
            .max()
            .unwrap_or(0),
        "MIT-77 / IO-6: returned_max_buffered_count must equal max of \
         buffered_count over returned sessions (or 0 when empty)",
    );

    // Inherited-shape sanity (parent's OrderingSessionSnapshot invariants
    // surviving the DTO wire): per-session consistency checks. Not new
    // IO-* invariants, but fast regression signal for DTO shape drift.
    for session in &io.sessions {
        assert_eq!(
            session.expected_next_seq,
            session.last_released_seq.saturating_add(1),
            "MIT-77: session {} expected_next_seq must equal last_released_seq.saturating_add(1)",
            session.session_id,
        );
        assert_eq!(
            session.oldest_buffered_seq.is_some(),
            session.buffered_count > 0,
            "MIT-77: session {} oldest_buffered_seq.is_some() must match (buffered_count > 0)",
            session.session_id,
        );
        assert_eq!(
            session.newest_buffered_seq.is_some(),
            session.buffered_count > 0,
            "MIT-77: session {} newest_buffered_seq.is_some() must match (buffered_count > 0)",
            session.session_id,
        );
    }
}
