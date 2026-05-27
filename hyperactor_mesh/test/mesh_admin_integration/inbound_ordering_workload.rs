/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Exercises MIT-78 (deterministic stalled inbound ordering over the
//! mesh-admin API) by spawning the `inbound_ordering_workload` Python
//! binary, waiting for it to print its admin URL, then polling
//! `/v1/{stalled_receiver}` until the expected `InboundOrdering`
//! shape appears (bounded retry — fire-and-forget `.broadcast()`
//! returning is not proof the receiver's snapshot has caught up).
//!
//! Sister test to MIT-77 (`inbound_ordering.rs`), which asserts the
//! HTTP transport against the dining workload's happy-path actors.
//! MIT-78 asserts the same transport against a manufactured stall, so
//! a regression that drops the publish path (`attrs.set` on the
//! actor's INBOUND_ORDERING key, or the `IntoNodeProperties` / DTO
//! threading in `hyperactor_mesh::introspect`) cannot pass.
//!
//! Assertions are against the typed `NodePayload` /
//! `NodeProperties::Actor { inbound_ordering: Some(io), .. }` returned
//! by `WorkloadFixture::get_node_payload()` (mirrors MIT-77). Exact
//! wire field-name coverage lives in the DTO round-trip / schema
//! snapshot tests and in `verify_inbound_ordering.py`; this test
//! intentionally stays on the typed domain layer.

use std::cmp::Reverse;
use std::time::Duration;
use std::time::Instant;

use hyperactor_mesh::introspect::InboundOrdering;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;

use crate::harness;
use crate::harness::WorkloadFixture;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const EXPECTED_TOTAL_BUFFERED: usize = 8;

fn enc(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// Walk the topology (root -> hosts -> procs -> actors) and return
/// the ref of the first actor whose label is `stalled_receiver`.
async fn find_stalled_receiver(fixture: &WorkloadFixture) -> String {
    let root: NodePayload = fixture
        .get_node_payload("/v1/root")
        .await
        .expect("MIT-78: GET /v1/root failed");
    for host_ref in &root.children {
        let host: NodePayload = fixture
            .get_node_payload(&format!("/v1/{}", enc(&host_ref.to_string())))
            .await
            .unwrap_or_else(|e| panic!("MIT-78: GET /v1/{host_ref} failed: {e:#}"));
        for proc_ref in &host.children {
            let proc: NodePayload = fixture
                .get_node_payload(&format!("/v1/{}", enc(&proc_ref.to_string())))
                .await
                .unwrap_or_else(|e| panic!("MIT-78: GET /v1/{proc_ref} failed: {e:#}"));
            for actor_ref in &proc.children {
                let s = actor_ref.to_string();
                // ActorAddr.to_string() is `{actor_uid}.{proc_id}@{location}`
                // where actor_uid is `label<base58>`. Match the label
                // exactly -- substring would also match the controller
                // (`actor_mesh_controller_stalled_receiver<...>`),
                // which is a different actor with a different session
                // table.
                let label = s.split('<').next().unwrap_or("");
                if label == "stalled_receiver" {
                    return s;
                }
            }
        }
    }
    panic!("MIT-78: stalled_receiver actor not found in topology");
}

fn matches_expected(io: &InboundOrdering) -> bool {
    io.enabled
        && io.snapshot_complete
        && io.returned_buffered_message_count == EXPECTED_TOTAL_BUFFERED
}

/// Poll until the receiver publishes the stalled shape or the poll
/// budget elapses. Returns the final observed payload and the last
/// observed `InboundOrdering` (whether matching or not) for use in
/// the assertion failure message.
async fn poll_until_stalled(
    fixture: &WorkloadFixture,
    receiver_ref: &str,
) -> (NodePayload, Option<Box<InboundOrdering>>) {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let path = format!("/v1/{}", enc(receiver_ref));
    loop {
        let actor: NodePayload = fixture
            .get_node_payload(&path)
            .await
            .unwrap_or_else(|e| panic!("MIT-78: GET {path} failed: {e:#}"));
        let last_io: Option<Box<InboundOrdering>> = match &actor.properties {
            NodeProperties::Actor {
                inbound_ordering, ..
            } => inbound_ordering.clone(),
            other => panic!("MIT-78: expected Actor variant for receiver, got {other:?}"),
        };
        if let Some(io) = &last_io
            && matches_expected(io)
        {
            return (actor, last_io);
        }
        if Instant::now() >= deadline {
            return (actor, last_io);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn run_inner(fixture: &WorkloadFixture) {
    let receiver_ref = find_stalled_receiver(fixture).await;
    let (actor, last_io) = poll_until_stalled(fixture, &receiver_ref).await;

    let NodeProperties::Actor {
        inbound_ordering, ..
    } = actor.properties
    else {
        panic!("MIT-78: expected Actor variant on converged payload; last observed: {last_io:?}");
    };
    let io = inbound_ordering.unwrap_or_else(|| {
        panic!(
            "MIT-78 / IO-7: inbound_ordering must be Some for a live receiver actor; \
             last observed: {last_io:?}"
        )
    });

    // IO-4: snapshot_complete derived from skipped_session_count.
    assert_eq!(
        io.snapshot_complete,
        io.skipped_session_count == 0,
        "MIT-78 / IO-4: snapshot_complete must equal (skipped_session_count == 0); \
         got snapshot_complete={}, skipped={}; last observed: {io:?}",
        io.snapshot_complete,
        io.skipped_session_count,
    );
    assert!(
        io.snapshot_complete,
        "MIT-78 / IO-4 (deterministic case): snapshot_complete must be true; \
         last observed: {io:?}",
    );

    // IO-5: known_session_count totals returned + skipped.
    assert_eq!(
        io.known_session_count,
        io.sessions.len() + io.skipped_session_count,
        "MIT-78 / IO-5: known_session_count must equal sessions.len() + skipped_session_count; \
         got known={}, sessions.len()={}, skipped={}; last observed: {io:?}",
        io.known_session_count,
        io.sessions.len(),
        io.skipped_session_count,
    );
    // 3 sessions, not 2: the workload's root client opens a session
    // on the receiver via the `whoami` bootstrap call (used to extract
    // the receiver's `ActorAddr` for `_debug_skip_next_ordering_seq`).
    // That session is idle (buffered_count == 0), so it shows up in
    // `known_session_count` but NOT in the `returned_*` rollups below.
    assert_eq!(
        io.known_session_count, 3,
        "MIT-78 / IO-5 (deterministic case): known_session_count must be 3 \
         (sender_a + sender_b + workload bootstrap client); last observed: {io:?}",
    );

    // IO-6: returned_* rollups equal recomputation over returned sessions.
    assert_eq!(
        io.returned_buffered_session_count,
        io.sessions.iter().filter(|s| s.buffered_count > 0).count(),
        "MIT-78 / IO-6: returned_buffered_session_count must equal count of returned \
         sessions with buffered_count > 0; last observed: {io:?}",
    );
    assert_eq!(
        io.returned_buffered_message_count,
        io.sessions.iter().map(|s| s.buffered_count).sum::<usize>(),
        "MIT-78 / IO-6: returned_buffered_message_count must equal sum of buffered_count \
         over returned sessions; last observed: {io:?}",
    );
    assert_eq!(
        io.returned_max_buffered_count,
        io.sessions
            .iter()
            .map(|s| s.buffered_count)
            .max()
            .unwrap_or(0),
        "MIT-78 / IO-6: returned_max_buffered_count must equal max of buffered_count \
         over returned sessions; last observed: {io:?}",
    );

    // Workload content totals (deterministic).
    assert_eq!(
        io.returned_buffered_session_count, 2,
        "MIT-78 workload shape: returned_buffered_session_count expected 2; \
         last observed: {io:?}",
    );
    assert_eq!(
        io.returned_buffered_message_count, EXPECTED_TOTAL_BUFFERED,
        "MIT-78 workload shape: returned_buffered_message_count expected {}; \
         last observed: {io:?}",
        EXPECTED_TOTAL_BUFFERED,
    );
    assert_eq!(
        io.returned_max_buffered_count, 5,
        "MIT-78 workload shape: returned_max_buffered_count expected 5; \
         last observed: {io:?}",
    );

    // Per-session shape, sessions sorted by buffered_count desc.
    let mut sessions: Vec<&_> = io.sessions.iter().collect();
    sessions.sort_by_key(|s| Reverse(s.buffered_count));
    assert!(
        sessions.len() >= 2,
        "MIT-78: expected at least 2 returned sessions; last observed: {io:?}",
    );
    let sa = sessions[0];
    let sb = sessions[1];

    assert_eq!(
        sa.buffered_count, 5,
        "MIT-78: session A buffered_count expected 5; got {}; last observed: {io:?}",
        sa.buffered_count,
    );
    assert_eq!(
        sb.buffered_count, 3,
        "MIT-78: session B buffered_count expected 3; got {}; last observed: {io:?}",
        sb.buffered_count,
    );
    for s in [sa, sb] {
        assert_eq!(
            s.expected_next_seq, 1,
            "MIT-78: session expected_next_seq expected 1; got {}; last observed: {io:?}",
            s.expected_next_seq,
        );
        assert_eq!(
            s.oldest_buffered_seq,
            Some(2),
            "MIT-78: session oldest_buffered_seq expected Some(2); got {:?}; \
             last observed: {io:?}",
            s.oldest_buffered_seq,
        );
        assert!(
            s.sender.is_some(),
            "MIT-78: session sender must be Some for a stalled session; last observed: {io:?}",
        );
    }
    assert_eq!(
        sa.newest_buffered_seq,
        Some(6),
        "MIT-78: session A newest_buffered_seq expected Some(6); got {:?}; \
         last observed: {io:?}",
        sa.newest_buffered_seq,
    );
    assert_eq!(
        sb.newest_buffered_seq,
        Some(4),
        "MIT-78: session B newest_buffered_seq expected Some(4); got {:?}; \
         last observed: {io:?}",
        sb.newest_buffered_seq,
    );

    // Two distinct session owners.
    assert_ne!(
        sa.sender, sb.sender,
        "MIT-78: two stalled sessions must have distinct senders; \
         both report {:?}; last observed: {io:?}",
        sa.sender,
    );
}

/// Drop guard for structural cleanup. See `dining::ShutdownGuard`.
struct ShutdownGuard<'a>(#[allow(dead_code)] &'a WorkloadFixture);

impl ShutdownGuard<'_> {
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for ShutdownGuard<'_> {
    fn drop(&mut self) {
        // Panic path: WorkloadFixture::Drop will call start_kill()
        // synchronously.
    }
}

/// MIT-78 entry point invoked from `main.rs`.
pub async fn run_inbound_ordering_workload() {
    let bin = harness::inbound_ordering_workload_binary();
    let fixture = harness::start_workload(&bin, &[], Duration::from_secs(60))
        .await
        .expect("MIT-78: failed to start inbound_ordering_workload");
    let guard = ShutdownGuard(&fixture);
    run_inner(&fixture).await;
    guard.disarm();
    fixture.shutdown().await;
}
