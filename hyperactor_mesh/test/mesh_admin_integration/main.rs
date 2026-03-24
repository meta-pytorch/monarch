/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mesh-admin integration test suite.
//!
//! Typed Rust tests over a shared harness.
//!
//! ## Mesh-admin integration test invariants (MIT-*)
//!
//! ### Fixture lifecycle
//!
//! - **MIT-1 (fixture-readiness):** A fixture is not considered live
//!   until the admin URL sentinel (`"Mesh admin server listening on
//!   "`) is observed from workload stdout within a timeout.
//!   `start_workload` fails if the sentinel does not appear.
//! - **MIT-2 (scoped-cleanup):** Scenarios own their fixture
//!   lifetime. `WorkloadFixture::shutdown()` takes the `Child` from
//!   its `Mutex`, calls `start_kill()`, and `wait().await`s for reap.
//!   `Drop` provides a synchronous best-effort `start_kill()`
//!   fallback. The scenario `run` helpers use a Drop guard to
//!   guarantee shutdown even on assertion failure.
//! - **MIT-3 (no-shared-fixtures):** We do NOT use shared static
//!   fixtures or `prctl(PR_SET_PDEATHSIG)`. Each test (or test group)
//!   starts its own scenario and shuts it down. This eliminates
//!   cross-test fixture contention and the thread-death signal bug.
//! - **MIT-4 (ephemeral-pki):** All fixtures generate ephemeral PKI
//!   via `rcgen` with CA + server cert SANs (localhost, hostname,
//!   127.0.0.1, ::1). No `openssl` CLI dependency.
//!
//! ### Authentication
//!
//! - **MIT-5 (mtls-required):** All harness HTTP clients validate the
//!   server's certificate chain against the ephemeral test CA and
//!   present a client cert. Full TLS verification is on - no
//!   `danger_accept_invalid_certs`. Both the server URL and the cert
//!   SANs derive from `hostname::get()`, so hostname verification
//!   succeeds. Uses `hyperactor_mesh::mesh_admin_client::add_tls` for
//!   the fbcode/OSS native-tls vs rustls identity split.
//! - **MIT-6 (mtls-rejection):** Unauthenticated clients (no client
//!   cert) are rejected by the mesh admin server. This is tested
//!   explicitly, not assumed.
//!
//! ### Discovery
//!
//! - **MIT-7 (proc-classification):** Service proc (contains
//!   `host_agent` actor) and worker proc (contains `proc_agent`
//!   actor) are classified by `WorkloadFixture::classify_procs() ->
//!   ClassifiedProcs` in the harness module. Retries with backoff.
//!   Fails explicitly if classification cannot be established within
//!   the retry budget.
//! - **MIT-8 (pyspy-worker-discovery):** Py-spy integration tests
//!   discover workload procs by filtering for actors whose names
//!   start with `pyspy_worker`. Retries until count >= expected
//!   concurrency.
//!
//! ### Endpoint contracts
//!
//! - **MIT-9 (success-typing):** Successful endpoint responses
//!   deserialize into the documented Rust types (`NodePayload`,
//!   `ConfigDumpResult`, `PySpyResult`), not loose JSON.
//! - **MIT-10 (error-envelope):** Failing endpoint calls return the
//!   standard `ApiErrorEnvelope` with a stable `error.code` field
//!   appropriate to the endpoint and failure mode (`gateway_timeout`,
//!   `not_found`, `internal_error`).
//! - **MIT-11 (bogus-ref-behavior):** Requests targeting
//!   unreachable/nonexistent proc references return a structured HTTP
//!   error envelope. The config path currently waits out its bridge
//!   timeout and returns `gateway_timeout`; py-spy bogus-ref checks
//!   accept the current non-success envelope behavior.
//! - **MIT-12 (both-proc-types):** Endpoints that accept a proc
//!   reference (`/v1/config`, `/v1/pyspy`) are tested on both worker
//!   (ProcAgent path) and service (HostAgent path) procs.
//!
//! ### Tree endpoint
//!
//! - **MIT-13 (root-contract):** `/v1/root` returns `Root` variant
//!   with `identity == "root"`, `num_hosts >= 1`, non-empty
//!   `children`.
//! - **MIT-14 (tree-format):** `/v1/tree` contains box-drawing
//!   characters (`├──`/`└──`), clickable URLs, and workload-specific
//!   actor names.
//!
//! ### Config endpoint
//!
//! - **MIT-15 (config-structure):** `/v1/config/{proc}` returns
//!   `ConfigDumpResult` with non-empty, name-sorted entries. Each
//!   entry has `name`, `value`, `source`.
//!
//! ### Py-spy endpoint
//!
//! - **MIT-16 (pyspy-result-contract):** `/v1/pyspy/{proc}`
//!   eventually yields a successful typed response with one of `Ok`,
//!   `BinaryNotFound`, or `Failed` variants once transient
//!   gateway/overload responses are retried.
//! - **MIT-17 (evidence-frames):** Py-spy integration success
//!   requires mode-specific evidence frames in at least one `Ok`
//!   sample across all worker procs for the evidence-checking modes
//!   (`cpu`, `block`) - not just 200 responses. The hard gate is
//!   workload-level.
//! - **MIT-18 (pyspy-provisioned):** Py-spy integration targets first
//!   fetch `fb-py-spy:prod` via `fbpkg fetch` into a temp dir and
//!   export that path as `PYSPY_BIN`, matching the old shell tests.
//!   If the fetch fails, workloads fall back to `py-spy` on `PATH`.
//!   `BinaryNotFound` under these tests is therefore unexpected and
//!   treated as failure signal rather than an environmental skip.
//! - **MIT-19 (quality-thresholds):** For the evidence-checking
//!   modes, per-mode quality thresholds (cpu 70%, block 80%) are
//!   logged as warnings, not failures. Mixed mode is a
//!   startup/topology smoke test and does not enforce evidence or
//!   threshold checks.

mod config;
mod dining;
mod harness;
mod pyspy;
mod tree;

#[tokio::test]
async fn test_dining_endpoints_rust() {
    dining::run_dining_endpoints_rust().await;
}

#[tokio::test]
async fn test_dining_endpoints_python() {
    dining::run_dining_endpoints_python().await;
}

#[tokio::test]
async fn test_pyspy_integration_cpu() {
    pyspy::run_pyspy_integration_cpu().await;
}

#[tokio::test]
async fn test_pyspy_integration_block() {
    pyspy::run_pyspy_integration_block().await;
}

#[tokio::test]
async fn test_pyspy_integration_mixed() {
    pyspy::run_pyspy_integration_mixed().await;
}
