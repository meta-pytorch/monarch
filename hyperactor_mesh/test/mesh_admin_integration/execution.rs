/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Exercises the execution surface (real Python hooks end-to-end) by
//! spawning the `execution_workload` Python binary and steering it
//! through the stdin-command / stdout-sentinel handshake
//! (`WorkloadFixture::send_command` / `wait_for_stdout`). NO sleeps:
//! every poll happens only after the relevant `EXEC_ENTERED` /
//! `EXEC_ACK` sentinel, so it proves mesh-admin truth rather than
//! winning a timing race.
//!
//! Unlike the `ExecutionTracker` unit tests (aggregation, oldest-first
//! ordering, truncation, the `0` sentinel, idempotent `finish`) and the
//! DTO json-shape tests, this proves the *real* `_Actor.handle` bracket
//! increments the `execution` surface on entry and decrements on exit
//! (completion and exception), across direct dispatch, queue dispatch,
//! and `@concurrent_endpoint`, visible end-to-end through
//! `GET /v1/{actor}` and the typed actor node payload.
//!
//! Cancellation is actor-fatal in the current runtime (`CancelledError`
//! is a `BaseException` -> the `except BaseException` arm re-raises -> a
//! kill signal), so it cannot be observed as `active_count -> 0` on a
//! surviving actor; the framework `finally` still decrements first (no
//! leak -- the cell is torn down with the actor anyway), and that
//! decrement path is covered by the `ExecutionTracker` unit tests.
//!
//! Coverage:
//!   - direct dispatch: one held invocation -> `active_count` 1, populated row.
//!   - same-name aggregation: two held `hold`s -> `active_count` 2, ONE row
//!     at row `active_count == 2`.
//!   - completion decrement: release each -> count drops; after the last
//!     release the supported-but-idle shape returns (`Some`, count 0).
//!   - exception decrement: raise a held invocation (via `call_one`, so
//!     the handler error returns as `ActorError` and the actor survives)
//!     -> count drops back to the idle shape.
//!   - queue dispatch: one held invocation -> `active_count` 1 (the
//!     bracket fires in queue mode too).
//!   - concurrent endpoint under queue dispatch: two held `hold`s overlap,
//!     aggregate to one row with `active_count == 2`, and can be released by
//!     a later control message on the same actor without deadlocking.
//!   - queue-dispatch deadlock: a blocking plain `@endpoint` monopolizes the
//!     Python dispatch loop, so a later release can never run; the API surfaces
//!     the wedge as `execution.active_count` pinned at 1 on the same invocation
//!     (the stuck release is not introspectable -- `queue_depth` is the Rust
//!     work queue, which drains before the handler runs).
//!   - idle sibling: `Some` with `active_count == 0` (EX-1: a supported
//!     actor that is idle is `Some`, not `None`).

use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use hyperactor_mesh::introspect::Execution;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;

use crate::harness;
use crate::harness::WorkloadFixture;

/// Per-sentinel wait budget. Generous: the workload only needs to spawn
/// a task and reach the first handler line.
const SENTINEL_TIMEOUT: Duration = Duration::from_secs(30);
/// Budget for the async `finally` decrement to become visible after the
/// `control` reply returns. The Python `finally` runs after the reply,
/// so we poll rather than assume immediate.
const DECREMENT_POLL_TIMEOUT: Duration = Duration::from_secs(15);
const DECREMENT_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// A wedged queue-dispatch handler has no positive "still stuck" sentinel, so
/// the deadlock proof bounds a negative window: for this whole span the joint
/// snapshot must stay wedged and the release ACK must never arrive.
const DEADLOCK_NEGATIVE_TIMEOUT: Duration = Duration::from_secs(5);
const DEADLOCK_POLL_INTERVAL: Duration = Duration::from_millis(100);
const ACTOR_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

fn enc(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// Fetch the typed `Execution` for `actor_ref`. A live Python actor
/// installs its read-side callback eagerly in `init` (PE-1), so its
/// `execution` field is always `Some` (EX-1: `None` would mean the actor
/// is unsupported); unwrap it here and fail loudly on `None`.
async fn execution_of(fixture: &WorkloadFixture, actor_ref: &str) -> Execution {
    let path = format!("/v1/{}", enc(actor_ref));
    let actor: NodePayload = fixture
        .get_node_payload(&path)
        .await
        .unwrap_or_else(|e| panic!("execution: GET {path} failed: {e:#}"));
    match actor.properties {
        NodeProperties::Actor { execution, .. } => *execution.unwrap_or_else(|| {
            panic!(
                "execution / EX-1: a live Python actor must report Some(execution); \
                 got None for {actor_ref}"
            )
        }),
        other => panic!("execution: expected Actor variant for {actor_ref}, got {other:?}"),
    }
}

/// Poll `/v1/{actor}` until `pred(execution)` holds or the budget
/// elapses. Bounded retries, never sleep-then-assume: the async Python
/// `finally` decrement is observed, not waited out blindly. Returns the
/// last observed snapshot (matching or not) for the failure message.
async fn poll_until(
    fixture: &WorkloadFixture,
    actor_ref: &str,
    pred: impl Fn(&Execution) -> bool,
) -> Execution {
    let deadline = Instant::now() + DECREMENT_POLL_TIMEOUT;
    loop {
        let exec = execution_of(fixture, actor_ref).await;
        if pred(&exec) || Instant::now() >= deadline {
            return exec;
        }
        tokio::time::sleep(DECREMENT_POLL_INTERVAL).await;
    }
}

/// Assert the wedged handler stays in-flight for the whole
/// `DEADLOCK_NEGATIVE_TIMEOUT` window: every snapshot must show exactly one
/// in-flight `handler` row (`active_count == 1`) that is the SAME invocation
/// (`oldest_since == started` -- it was never released and re-held). A plain
/// poll-until-wedged would pass on the first match and prove nothing about the
/// handler *never completing*; the deadlock proof is that no snapshot in the
/// window ever recovers. `queue_depth` is deliberately NOT checked: it is the
/// Rust work queue, drained before the handler runs, so a Python-side dispatch
/// wedge leaves it at 0 (see the deadlock scenario).
async fn assert_stays_wedged(
    fixture: &WorkloadFixture,
    actor_ref: &str,
    handler: &str,
    started: SystemTime,
) {
    let deadline = Instant::now() + DEADLOCK_NEGATIVE_TIMEOUT;
    loop {
        let exec = execution_of(fixture, actor_ref).await;
        assert_eq!(
            exec.active_count, 1,
            "deadlock: the wedged handler must stay in-flight (active_count == 1) for the whole \
             window; completion would drop it. got {exec:?}",
        );
        assert_eq!(
            exec.active_handlers.len(),
            1,
            "deadlock: exactly one stuck handler row expected; got {exec:?}",
        );
        assert_eq!(
            exec.active_handlers[0].name, handler,
            "deadlock: the stuck row must be `{handler}`; got {exec:?}",
        );
        assert_eq!(
            exec.active_handlers[0].oldest_since, started,
            "deadlock: the SAME invocation must stay stuck (oldest_since unchanged) -- a release \
             would have dropped it, and a new hold would restart the clock; got {exec:?}",
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(DEADLOCK_POLL_INTERVAL).await;
    }
}

/// Assert the supported-but-idle shape: `Some` with no in-flight work.
fn assert_idle_shape(exec: &Execution, ctx: &str) {
    assert_eq!(
        exec.active_count, 0,
        "execution ({ctx}): active_count must be 0; got {exec:?}",
    );
    assert!(
        exec.active_handlers.is_empty(),
        "execution ({ctx}): active_handlers must be empty; got {exec:?}",
    );
    assert!(
        !exec.truncated,
        "execution ({ctx}): truncated must be false; got {exec:?}",
    );
    assert!(
        exec.complete,
        "execution ({ctx}) / EX-2: an uncontended read must be complete; got {exec:?}",
    );
}

async fn run_inner(fixture: &WorkloadFixture) {
    let busy = fixture
        .wait_for_actor_by_label("busy_actor", ACTOR_DISCOVERY_TIMEOUT)
        .await
        .expect("execution: busy actor was not initialized");
    let idle = fixture
        .wait_for_actor_by_label("idle_actor", ACTOR_DISCOVERY_TIMEOUT)
        .await
        .expect("execution: idle actor was not initialized");
    let queue = fixture
        .wait_for_actor_by_label("queue_actor", ACTOR_DISCOVERY_TIMEOUT)
        .await
        .expect("execution: queue actor was not initialized");
    let concurrent = fixture
        .wait_for_actor_by_label("concurrent_actor", ACTOR_DISCOVERY_TIMEOUT)
        .await
        .expect("execution: concurrent actor was not initialized");
    let deadlock = fixture
        .wait_for_actor_by_label("deadlock_actor", ACTOR_DISCOVERY_TIMEOUT)
        .await
        .expect("execution: deadlock actor was not initialized");

    // --- Direct dispatch: one held invocation -> count 1. ---
    fixture
        .send_command("HOLD busy a")
        .await
        .expect("execution: send HOLD busy a");
    fixture
        .wait_for_stdout("EXEC_ENTERED a", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ENTERED a");

    let exec = execution_of(fixture, &busy).await;
    assert_eq!(
        exec.active_count, 1,
        "execution: one held invocation must report active_count == 1; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers.len(),
        1,
        "execution: one held invocation must report exactly one active_handlers row; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].name, "hold",
        "execution: the active row name must be the Python method name `hold`; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].active_count, 1,
        "execution: the active row active_count must be 1; got {exec:?}",
    );
    assert!(
        exec.complete,
        "execution / EX-2: an uncontended read must be complete; got {exec:?}",
    );

    // --- Same-name aggregation: two held `hold`s -> count 2, ONE row. ---
    fixture
        .send_command("HOLD busy b")
        .await
        .expect("execution: send HOLD busy b");
    fixture
        .wait_for_stdout("EXEC_ENTERED b", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ENTERED b");

    let exec = execution_of(fixture, &busy).await;
    assert_eq!(
        exec.active_count, 2,
        "execution: two held invocations must report active_count == 2; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers.len(),
        1,
        "execution: two invocations of the SAME endpoint must aggregate into ONE row \
         (a one-row-per-invocation bug fails here); got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].active_count, 2,
        "execution: the aggregated row active_count must be 2; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].name, "hold",
        "execution: the aggregated row name must remain `hold`; got {exec:?}",
    );

    // --- Completion decrement: release a -> count drops to 1. ---
    fixture
        .send_command("RELEASE busy a")
        .await
        .expect("execution: send RELEASE busy a");
    fixture
        .wait_for_stdout("EXEC_ACK release a", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ACK release a");

    let exec = poll_until(fixture, &busy, |e| e.active_count == 1).await;
    assert_eq!(
        exec.active_count, 1,
        "execution: after releasing one of two, count must drop to 1 (the finally ran); got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers.len(),
        1,
        "execution: one invocation should remain in a single row; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].active_count, 1,
        "execution: the remaining row active_count must be 1; got {exec:?}",
    );

    // --- Post-activity clear: release b -> idle shape returns. ---
    fixture
        .send_command("RELEASE busy b")
        .await
        .expect("execution: send RELEASE busy b");
    fixture
        .wait_for_stdout("EXEC_ACK release b", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ACK release b");

    let exec = poll_until(fixture, &busy, |e| e.active_count == 0).await;
    assert_idle_shape(&exec, "post-activity clear (completion)");

    // --- Exception decrement: raise a held invocation -> count drops to 0. ---
    fixture
        .send_command("HOLD busy c")
        .await
        .expect("execution: send HOLD busy c");
    fixture
        .wait_for_stdout("EXEC_ENTERED c", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ENTERED c");
    let exec = execution_of(fixture, &busy).await;
    assert_eq!(
        exec.active_count, 1,
        "execution: held invocation `c` must report count 1 before raise; got {exec:?}",
    );

    fixture
        .send_command("RAISE busy c")
        .await
        .expect("execution: send RAISE busy c");
    fixture
        .wait_for_stdout("EXEC_ACK raise c", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ACK raise c");
    let exec = poll_until(fixture, &busy, |e| e.active_count == 0).await;
    assert_idle_shape(&exec, "exception (finally ran on exception)");

    // The raise is delivered via call_one, so the actor must SURVIVE the
    // exception (not be supervision-killed) -- otherwise "exception -> 0 on a
    // live actor" is hollow. Prove survival the strong way: the actor still
    // accepts and runs a NEW invocation after the exception.
    fixture
        .send_command("HOLD busy e")
        .await
        .expect("execution: send HOLD busy e");
    fixture
        .wait_for_stdout("EXEC_ENTERED e", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ENTERED e");
    let exec = execution_of(fixture, &busy).await;
    assert_eq!(
        exec.active_count, 1,
        "execution: busy actor must still accept a new invocation after the \
         exception (count 1); got {exec:?}",
    );
    fixture
        .send_command("RELEASE busy e")
        .await
        .expect("execution: send RELEASE busy e");
    fixture
        .wait_for_stdout("EXEC_ACK release e", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ACK release e");
    let exec = poll_until(fixture, &busy, |e| e.active_count == 0).await;
    assert_idle_shape(&exec, "after post-exception interaction");

    // Cancellation is deliberately NOT exercised here: it is actor-fatal
    // in the current runtime (see the module doc), so `count -> 0` is not
    // observable on a surviving actor. The decrement-on-exit guarantee is
    // covered by the `ExecutionTracker` unit tests.

    // --- Queue dispatch: one held invocation -> count 1 (bracket fires). ---
    // Queue dispatch is serialized: we hold a single invocation and only
    // assert count == 1, never releasing it through a second endpoint
    // call (that would deadlock the dispatch loop). The proc tears down
    // at fixture shutdown.
    fixture
        .send_command("HOLD queue q")
        .await
        .expect("execution: send HOLD queue q");
    fixture
        .wait_for_stdout("EXEC_ENTERED q", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ENTERED q");
    let exec = execution_of(fixture, &queue).await;
    assert_eq!(
        exec.active_count, 1,
        "execution: queue-dispatch held invocation must report count 1 \
         (the bracket fires in queue mode too); got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers.len(),
        1,
        "execution: queue-dispatch held invocation must report one row; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].name, "hold",
        "execution: queue-dispatch active row name must be `hold`; got {exec:?}",
    );

    // --- @concurrent_endpoint under queue dispatch: overlap + release. ---
    // This is the queue-compatible resource-manager pattern: a held endpoint
    // does not monopolize the actor dispatch loop, so a later control message
    // can run on the same actor and release it.
    fixture
        .send_command("HOLD concurrent ca")
        .await
        .expect("execution: send HOLD concurrent ca");
    fixture
        .wait_for_stdout("EXEC_ENTERED ca", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ENTERED ca");
    fixture
        .send_command("HOLD concurrent cb")
        .await
        .expect("execution: send HOLD concurrent cb");
    fixture
        .wait_for_stdout("EXEC_ENTERED cb", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ENTERED cb");

    let exec = execution_of(fixture, &concurrent).await;
    assert_eq!(
        exec.active_count, 2,
        "execution: two held @concurrent_endpoint invocations must report \
         active_count == 2; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers.len(),
        1,
        "execution: two @concurrent_endpoint invocations of the SAME endpoint \
         must aggregate into ONE row; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].name, "hold",
        "execution: the @concurrent_endpoint active row name must be `hold`; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].active_count, 2,
        "execution: the @concurrent_endpoint aggregated row active_count must be 2; got {exec:?}",
    );

    fixture
        .send_command("RELEASE concurrent ca")
        .await
        .expect("execution: send RELEASE concurrent ca");
    fixture
        .wait_for_stdout("EXEC_ACK release ca", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ACK release ca");
    let exec = poll_until(fixture, &concurrent, |e| e.active_count == 1).await;
    assert_eq!(
        exec.active_count, 1,
        "execution: after releasing one @concurrent_endpoint invocation, \
         count must drop to 1; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers.len(),
        1,
        "execution: one @concurrent_endpoint invocation should remain in one row; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].active_count, 1,
        "execution: the remaining @concurrent_endpoint row active_count must be 1; got {exec:?}",
    );

    fixture
        .send_command("RELEASE concurrent cb")
        .await
        .expect("execution: send RELEASE concurrent cb");
    fixture
        .wait_for_stdout("EXEC_ACK release cb", SENTINEL_TIMEOUT)
        .await
        .expect("execution: wait EXEC_ACK release cb");
    let exec = poll_until(fixture, &concurrent, |e| e.active_count == 0).await;
    assert_idle_shape(&exec, "@concurrent_endpoint post-release clear");

    // --- Queue-dispatch DEADLOCK: the API surfaces a wedged actor. ---
    // A plain @endpoint under queue dispatch monopolizes the Python dispatch
    // loop: `hold` blocks, so the later `control` release is stuck behind it in
    // the per-actor dispatch queue and can never run. This is the failure the
    // airport turnaround demo hits under the default (queue) dispatch.
    //
    // The API-visible signal is `execution.active_count`: the wedged `hold`
    // never completes, so its bracket never decrements and active_count stays
    // pinned at 1 on the same invocation -- even after a release that would free
    // a healthy actor. `queue_depth` does NOT show this: it is the Rust actor
    // work queue, drained before the handler runs (in queue mode the message is
    // handed to a Python-side channel and the Rust loop returns), so it sits at
    // 0; the stuck release waits in the Python dispatch queue, which
    // introspection does not expose. (Contrast: `concurrent_actor` above, same
    // queue dispatch, releases cleanly and drops to 0 -- so the signal is the
    // wedge, not queue dispatch per se.)
    //
    // Last driven block: the command loop wedges on the inline `control.call_one`
    // after the release. The admin server is Rust-served, so `/v1` stays live.
    fixture
        .send_command("HOLD deadlock d")
        .await
        .expect("deadlock: send HOLD deadlock d");
    fixture
        .wait_for_stdout("EXEC_ENTERED d", SENTINEL_TIMEOUT)
        .await
        .expect("deadlock: wait EXEC_ENTERED d");

    let exec = execution_of(fixture, &deadlock).await;
    assert_eq!(
        exec.active_count, 1,
        "deadlock: held queue-dispatch invocation must report active_count == 1; got {exec:?}",
    );
    assert_eq!(
        exec.active_handlers[0].name, "hold",
        "deadlock: the stuck row name must be `hold`; got {exec:?}",
    );
    // Pin the exact invocation so the sustained check proves the SAME hold stays
    // stuck (not released-and-re-held).
    let started = exec.active_handlers[0].oldest_since;

    // Send the release and positively confirm the workload dispatched it: the
    // command loop prints `EXEC_SENDING release d` immediately before the
    // (wedging) `control.call_one`, so a missing ACK later cannot be explained
    // by "the release was never sent".
    fixture
        .send_command("RELEASE deadlock d")
        .await
        .expect("deadlock: send RELEASE deadlock d");
    fixture
        .wait_for_stdout("EXEC_SENDING release d", SENTINEL_TIMEOUT)
        .await
        .expect("deadlock: wait EXEC_SENDING release d (release must be dispatched)");

    // The proof: across the whole window the same `hold` invocation stays
    // in-flight (active_count == 1, oldest_since unchanged) despite the release.
    assert_stays_wedged(fixture, &deadlock, "hold", started).await;

    // Corroboration: the release ACK never arrives within the window -- the
    // command loop is wedged in `control.call_one` -- and it is a *timeout*, not
    // the workload dying (which would close stdout and surface a different error).
    let acked = fixture
        .wait_for_stdout("EXEC_ACK release d", DEADLOCK_NEGATIVE_TIMEOUT)
        .await;
    let err = acked.expect_err("deadlock: release ACK must NOT arrive (the loop is wedged)");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains("stdout closed"),
        "deadlock: the release-ACK wait must time out (loop wedged), not fail from workload \
         death; got: {msg}",
    );

    // --- Idle sibling: Some with count 0 (never invoked). ---
    let exec = execution_of(fixture, &idle).await;
    assert_idle_shape(&exec, "idle sibling");
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
        // Panic path: WorkloadFixture::Drop calls start_kill()
        // synchronously.
    }
}

/// Execution-surface integration entry point invoked from `main.rs`.
pub async fn run_execution_workload() {
    let bin = harness::execution_workload_binary();
    let fixture = harness::start_workload(&bin, &[], Duration::from_secs(60))
        .await
        .expect("execution: failed to start execution_workload");
    let guard = ShutdownGuard(&fixture);
    run_inner(&fixture).await;
    guard.disarm();
    fixture.shutdown().await;
}
