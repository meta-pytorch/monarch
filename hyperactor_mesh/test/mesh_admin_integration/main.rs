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
//!
//! ### /v1/{ref} traversal
//!
//! - **MIT-20 (round-trip):** Child references returned by a parent
//!   node are fetchable via `/v1/{ref}`.
//! - **MIT-21 (node-kind-typing):** Root returns `Root`, host returns
//!   `Host`, proc returns `Proc`, actor returns `Actor` variant.
//! - **MIT-22 (identity-consistency):** `node.identity` matches the
//!   reference string used to fetch it.
//! - **MIT-23 (child-link-consistency):** Root, first host, classified
//!   service proc, classified worker proc, and known actor refs are
//!   fetchable.
//! - **MIT-24 (known-actors):** Among each proc node's children,
//!   actors named `host_agent` or `proc_agent` are present and
//!   fetchable, returning `Actor` variant.
//!
//! ### Malformed/encoded ref edge cases
//!
//! - **MIT-25 (empty-ref):** `/v1/` returns a non-success status.
//! - **MIT-26 (garbage-ref):** Random garbage strings return
//!   `not_found` error envelope.
//! - **MIT-27 (truncated-ref):** Prefix of a valid reference returns
//!   a non-success status.
//! - **MIT-29 (long-ref):** A 10KB string returns a structured error,
//!   not a crash or timeout.
//! - **MIT-30 (unreachable-socket-ref):** Valid transport address with
//!   nonexistent socket returns `not_found` or `gateway_timeout`.
//! - **MIT-31 (url-encoded-round-trip):** A valid reference containing
//!   commas/brackets (which all hyperactor refs do) round-trips
//!   correctly through URL encoding.
//!
//! ### Auth failure cases
//!
//! - **MIT-32 (no-client-cert):** Unauthenticated client is rejected
//!   across all 4 endpoints (`/v1/root`, `/v1/tree`,
//!   `/v1/config/{ref}`, `/v1/pyspy/{ref}`).
//! - **MIT-33 (wrong-ca-cert):** Client presents a cert signed by an
//!   independent CA → TLS-level failure.
//! - **MIT-34 (wrong-client-ca):** Client trusts a different CA than
//!   the server's → TLS-level failure.
//! - **MIT-35 (corrupt-pem):** Passing corrupt PEM material to
//!   `add_tls` → TLS setup rejects invalid CA PEM deterministically
//!   (returns `ok == false`).
//! - **MIT-36 (auth-vs-app-error):** Auth failures are TLS-level
//!   (`result.is_err()`), not HTTP-level 4xx/5xx.
//!
//! ### OpenAPI conformance
//!
//! #### OpenAPI document — static
//!
//! - **MIT-37 (openapi-valid):** `/v1/openapi.json` returns parseable
//!   JSON with `openapi: "3.1.0"`, `info`, `paths`,
//!   `components.schemas`.
//! - **MIT-38 (openapi-resolvable):** All `$ref` targets resolve to
//!   definitions that exist within the document.
//! - **MIT-39 (openapi-routes-covered):** Every client-facing route
//!   (`/v1/root`, `/v1/{reference}`, `/v1/config/{proc_reference}`,
//!   `/v1/tree`, `/v1/schema`, `/v1/schema/error`) is present in
//!   `paths`. The spec-serving endpoint `/v1/openapi.json` is also
//!   verified to be live, though it is not required to appear in
//!   `paths` since it is a meta-endpoint.
//! - **MIT-40 (schemas-compile):** Every `components/schemas` entry
//!   compiles via `jsonschema::JSONSchema::compile` when embedded in
//!   a synthetic document that includes the full component set as
//!   `$defs`.
//!
//! #### Success responses
//!
//! - **MIT-41 (success-status-documented):** Success responses return
//!   a status code declared by the operation.
//! - **MIT-42 (success-body-validates):** Success bodies validate
//!   against the operation's response schema. Covers `/v1/root`
//!   (Root), `/v1/{ref}` (Host, Proc, Actor), `/v1/config/{proc}`.
//! - **MIT-45 (no-undocumented-fields):** Enforced where the
//!   published schemas forbid additional properties (the
//!   NodeProperties variant wrappers do); otherwise subsumed by
//!   MIT-42.
//! - **MIT-51 (success-content-type):** Successful responses use a
//!   media type matching the declared type (`application/json` or
//!   `text/plain`). Matching is by media type, not exact header
//!   string.
//!
//! #### Error responses
//!
//! - **MIT-43 (error-status-documented):** Error responses return a
//!   status code declared by the operation.
//! - **MIT-44 (error-body-validates):** Error bodies validate against
//!   the `ApiErrorEnvelope` schema.
//! - **MIT-46 (error-envelope-shape):** `error.code` (string) and
//!   `error.message` (string) present per schema.
//! - **MIT-47 (error-code-enum-conformance):** Vacuously true today
//!   (schema doesn't constrain `error.code` to an enum). Automatic if
//!   the schema adds one later.
//! - **MIT-52 (error-content-type):** Error responses use a media
//!   type matching `application/json`.
//!
//! #### Parameters and encoding
//!
//! - **MIT-48 (path-param-conformance):** Parameters match the
//!   declared contract (type: string, required: true).
//! - **MIT-49 (encoded-param-conformance):** URL-encoded references
//!   produce responses conforming to the same schema.
//! - **MIT-50 (invalid-param-error-conformance):** Invalid params
//!   fail with documented status codes and error body shapes.
//!
//! ### Py-spy OpenAPI conformance
//!
//! #### Py-spy route in spec — static
//!
//! - **MIT-53 (pyspy-route-documented):**
//!   `/v1/pyspy/{proc_reference}` is present in `paths`.
//! - **MIT-54 (pyspy-schemas-compile):** `PySpyResult` (and its
//!   transitive types) in `components/schemas` compile via
//!   `jsonschema::JSONSchema::compile`.
//!
//! #### Py-spy success responses
//!
//! - **MIT-55 (pyspy-success-status-documented):** Success responses
//!   return a status code declared by the operation.
//! - **MIT-56 (pyspy-success-body-validates):** Success body
//!   validates against the operation's `PySpyResult` response schema.
//!
//! #### Py-spy error responses
//!
//! - **MIT-57 (pyspy-error-status-documented):** Error responses
//!   return a status code declared by the operation.
//! - **MIT-58 (pyspy-error-body-validates):** Error body validates
//!   against `ApiErrorEnvelope` schema.
//! - **MIT-59 (pyspy-error-envelope-shape):** `error.code` and
//!   `error.message` present.
//!
//! #### Py-spy parameters
//!
//! - **MIT-60 (pyspy-path-param-conformance):** `proc_reference`
//!   parameter matches contract (type: string, required: true).
//! - **MIT-61 (pyspy-invalid-param-error-conformance):** Bogus proc
//!   ref fails with documented status code and error body shape.
//!
//! #### Py-spy content-type
//!
//! - **MIT-62 (pyspy-content-type):** Both success and error
//!   responses use `application/json` media type.

mod auth;
mod config;
mod dining;
mod harness;
mod openapi;
mod pyspy;
mod ref_check;
mod ref_edge;
mod tree;

// --- dining family ---

/// MIT-9, MIT-10, MIT-11, MIT-12, MIT-13, MIT-14, MIT-15: dining-based
/// endpoint assertions — Rust binary.
#[tokio::test]
async fn test_dining_endpoints_rust() {
    dining::run_dining_endpoints_rust().await;
}

/// MIT-9, MIT-10, MIT-11, MIT-12, MIT-13, MIT-14, MIT-15: dining-based
/// endpoint assertions — Python binary.
#[tokio::test]
async fn test_dining_endpoints_python() {
    dining::run_dining_endpoints_python().await;
}

// --- pyspy family ---

/// MIT-16, MIT-17, MIT-18, MIT-19: py-spy integration — cpu mode.
#[tokio::test]
async fn test_pyspy_integration_cpu() {
    pyspy::run_pyspy_integration_cpu().await;
}

/// MIT-16, MIT-17, MIT-18, MIT-19: py-spy integration — block mode.
#[tokio::test]
async fn test_pyspy_integration_block() {
    pyspy::run_pyspy_integration_block().await;
}

/// MIT-16, MIT-18, MIT-19: py-spy integration — mixed mode (smoke
/// test, no evidence check).
#[tokio::test]
async fn test_pyspy_integration_mixed() {
    pyspy::run_pyspy_integration_mixed().await;
}

// --- traversal family ---

/// MIT-20, MIT-21, MIT-22, MIT-23, MIT-24: /v1/{ref} topology
/// traversal — Rust binary.
#[tokio::test]
async fn test_ref_traversal_rust() {
    dining::run_ref_traversal_rust().await;
}

/// MIT-20, MIT-21, MIT-22, MIT-23, MIT-24: /v1/{ref} topology
/// traversal — Python binary.
#[tokio::test]
async fn test_ref_traversal_python() {
    dining::run_ref_traversal_python().await;
}

// --- malformed-ref family ---

/// MIT-25, MIT-26, MIT-27, MIT-28, MIT-29, MIT-30, MIT-31:
/// malformed/encoded reference edge cases — Rust binary.
#[tokio::test]
async fn test_ref_edge_cases_rust() {
    dining::run_ref_edge_cases_rust().await;
}

/// MIT-25, MIT-26, MIT-27, MIT-28, MIT-29, MIT-30, MIT-31:
/// malformed/encoded reference edge cases — Python binary.
#[tokio::test]
async fn test_ref_edge_cases_python() {
    dining::run_ref_edge_cases_python().await;
}

// --- auth family ---

/// MIT-32, MIT-33, MIT-34, MIT-35, MIT-36: auth failure coverage —
/// Rust binary.
#[tokio::test]
async fn test_auth_failures_rust() {
    dining::run_auth_failures_rust().await;
}

// --- openapi conformance family ---

/// MIT-37, MIT-38, MIT-39, MIT-40, MIT-41, MIT-42, MIT-43, MIT-44,
/// MIT-45, MIT-46, MIT-47, MIT-48, MIT-49, MIT-50, MIT-51, MIT-52,
/// MIT-53, MIT-54, MIT-55, MIT-56, MIT-57, MIT-58, MIT-59, MIT-60,
/// MIT-61, MIT-62: OpenAPI conformance — Rust binary.
#[tokio::test]
async fn test_openapi_conformance_rust() {
    dining::run_openapi_conformance_rust().await;
}
